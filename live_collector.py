#!/usr/bin/env python3
"""
Live Supplementary Collector — Order Books, Trades, Outcomes, Market Discovery

Runs alongside the existing collector.py. Captures high-frequency data to CSV:
  • book_snapshots.csv   — order book snapshots every 2s for active 5m/15m markets
  • live_trades.csv      — real-time trades via WebSocket
  • market_outcomes.csv  — resolution data for expired markets
  • active_markets.csv   — discovered active markets log

All CSVs are stored in the data/ directory in append mode.
"""

import csv
import json
import logging
import os
import re
import sys
import threading
import time
from datetime import datetime, timezone
from pathlib import Path

import requests

try:
    import websocket  # websocket-client library
except ImportError:
    websocket = None

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[logging.StreamHandler(sys.stdout)],
)
log = logging.getLogger("live_collector")

DATA_DIR = Path(__file__).parent / "data"
DATA_DIR.mkdir(parents=True, exist_ok=True)

# ── CSV file paths ────────────────────────────────────────────────────────────
BOOK_CSV    = DATA_DIR / "book_snapshots.csv"
TRADES_CSV  = DATA_DIR / "live_trades.csv"
OUTCOMES_CSV = DATA_DIR / "market_outcomes.csv"
MARKETS_CSV = DATA_DIR / "active_markets.csv"

# ── APIs ──────────────────────────────────────────────────────────────────────
CLOB_API   = "https://clob.polymarket.com"
GAMMA_API  = "https://gamma-api.polymarket.com"
WS_URL     = "wss://ws-subscriptions-clob.polymarket.com/ws/market"

# ── Settings ──────────────────────────────────────────────────────────────────
BOOK_INTERVAL        = 2      # seconds between book snapshots
DISCOVERY_INTERVAL   = 60     # seconds between market discovery
OUTCOME_POLL         = 10     # seconds between outcome checks
OUTCOME_LOOKBACK     = 300    # check markets that ended within last 5 min
CSV_FLUSH_INTERVAL   = 30     # seconds between CSV flushes
BOOK_LEVELS          = 5      # top N bid/ask levels to store

# ── Coin keywords for market discovery ────────────────────────────────────────
COIN_TAGS = {
    "bitcoin":  "BTC",
    "ethereum": "ETH",
    "solana":   "SOL",
}

# ═════════════════════════════════════════════════════════════════════════════
#  Shared state
# ═════════════════════════════════════════════════════════════════════════════

# Active markets: {condition_id: market_info_dict}
active_markets: dict[str, dict] = {}
active_markets_lock = threading.Lock()

# Recent book snapshots for trade enrichment: {token_id: (ts_ms, best_bid, best_ask, mid)}
recent_books: dict[str, tuple] = {}
recent_books_lock = threading.Lock()

# Resolved markets to avoid re-checking
resolved_set: set[str] = set()


# ═════════════════════════════════════════════════════════════════════════════
#  HTTP helper
# ═════════════════════════════════════════════════════════════════════════════

def _req(url: str, params: dict | None = None, timeout: int = 10):
    for attempt in range(3):
        try:
            resp = requests.get(url, params=params or {}, timeout=timeout)
            if resp.status_code == 429:
                wait = int(resp.headers.get("Retry-After", 10))
                log.warning(f"Rate limited — waiting {wait}s")
                time.sleep(wait)
                continue
            resp.raise_for_status()
            return resp.json()
        except requests.RequestException as e:
            log.warning(f"Request {url} attempt {attempt+1}: {e}")
            time.sleep(2 ** attempt)
    return None


# ═════════════════════════════════════════════════════════════════════════════
#  CSV writers (thread-safe, append mode with periodic flush)
# ═════════════════════════════════════════════════════════════════════════════

class CSVAppender:
    """Thread-safe CSV appender with periodic flush."""

    def __init__(self, path: Path, columns: list[str]):
        self.path = path
        self.columns = columns
        self.lock = threading.Lock()
        self._last_flush = time.time()

        # Write header if file doesn't exist or is empty
        needs_header = not path.exists() or path.stat().st_size == 0
        self._file = open(path, "a", newline="", buffering=1)
        self._writer = csv.writer(self._file)
        if needs_header:
            self._writer.writerow(columns)
            self._file.flush()

    def write_row(self, row: list):
        with self.lock:
            self._writer.writerow(row)
            now = time.time()
            if now - self._last_flush >= CSV_FLUSH_INTERVAL:
                self._file.flush()
                self._last_flush = now

    def write_rows(self, rows: list[list]):
        with self.lock:
            for row in rows:
                self._writer.writerow(row)
            now = time.time()
            if now - self._last_flush >= CSV_FLUSH_INTERVAL:
                self._file.flush()
                self._last_flush = now

    def flush(self):
        with self.lock:
            self._file.flush()


# Initialize CSV writers
book_csv = None
trades_csv = None
outcomes_csv = None
markets_csv = None


def init_csv_writers():
    global book_csv, trades_csv, outcomes_csv, markets_csv

    book_csv = CSVAppender(BOOK_CSV, [
        "timestamp_ms", "condition_id", "token_id", "outcome",
        "best_bid", "best_ask", "bid_depth_3", "ask_depth_3",
        "mid_price", "spread",
        "bid_1_price", "bid_1_size", "bid_2_price", "bid_2_size",
        "bid_3_price", "bid_3_size", "bid_4_price", "bid_4_size",
        "bid_5_price", "bid_5_size",
        "ask_1_price", "ask_1_size", "ask_2_price", "ask_2_size",
        "ask_3_price", "ask_3_size", "ask_4_price", "ask_4_size",
        "ask_5_price", "ask_5_size",
        "coin", "timeframe", "market_slug",
    ])

    trades_csv = CSVAppender(TRADES_CSV, [
        "timestamp_ms", "condition_id", "token_id", "outcome",
        "side", "price", "size", "trade_id",
        "maker_address", "taker_address",
        "book_best_bid", "book_best_ask", "book_mid",
        "coin", "timeframe", "market_slug",
    ])

    outcomes_csv = CSVAppender(OUTCOMES_CSV, [
        "condition_id", "market_slug", "token_id_up", "token_id_down",
        "market_end_ts_ms", "resolution_outcome", "resolution_timestamp_ms",
        "final_up_price", "final_down_price",
        "coin", "timeframe", "question",
    ])

    markets_csv = CSVAppender(MARKETS_CSV, [
        "discovered_ts_ms", "condition_id", "market_slug",
        "coin", "timeframe", "question",
        "token_id_up", "token_id_down",
        "end_date", "end_ts_ms",
    ])


# ═════════════════════════════════════════════════════════════════════════════
#  Market Discovery
# ═════════════════════════════════════════════════════════════════════════════

_TIMEFRAME_RE = re.compile(
    r'(\d+)\s*(?:minute|min|m)|(\d+)\s*(?:hour|hr|h)|(\d+)\s*(?:day|d)',
    re.IGNORECASE,
)
_DURATION_SECS = {"5m": 300, "15m": 900, "1h": 3600, "1d": 86400}


def _parse_timeframe(question: str) -> str | None:
    """Extract timeframe from question text."""
    q = question.lower()
    if "5 min" in q or "5m" in q or "five min" in q:
        return "5m"
    if "15 min" in q or "15m" in q or "fifteen min" in q:
        return "15m"
    if "1 hour" in q or "1h" in q or "one hour" in q or "60 min" in q:
        return "1h"
    if "1 day" in q or "1d" in q or "24 hour" in q or "24h" in q:
        return "1d"
    return None


def _parse_end_date_ms(end_date_str: str) -> int:
    if not end_date_str:
        return 0
    for fmt in ("%Y-%m-%dT%H:%M:%SZ", "%Y-%m-%dT%H:%M:%S", "%Y-%m-%d %H:%M:%S", "%Y-%m-%d"):
        try:
            dt = datetime.strptime(end_date_str, fmt).replace(tzinfo=timezone.utc)
            return int(dt.timestamp() * 1000)
        except ValueError:
            continue
    return 0


def discover_markets():
    """Find active BTC/ETH/SOL Up/Down 5m and 15m markets via Gamma API."""
    now_ms = int(time.time() * 1000)
    new_count = 0

    for tag, coin in COIN_TAGS.items():
        data = _req(f"{GAMMA_API}/markets", {
            "tag": tag,
            "closed": "false",
            "limit": 50,
        })
        if not isinstance(data, list):
            continue

        for mkt in data:
            question = (mkt.get("question") or "").lower()
            if "up" not in question or "down" not in question:
                continue

            condition_id = mkt.get("conditionId") or ""
            if not condition_id:
                continue

            tf = _parse_timeframe(mkt.get("question") or "")
            if tf not in ("5m", "15m"):
                continue

            end_date = mkt.get("endDate") or ""
            end_ms = _parse_end_date_ms(end_date)

            # Skip already expired markets
            if end_ms and end_ms < now_ms:
                continue

            # Parse token IDs
            outcomes = json.loads(mkt.get("outcomes") or "[]")
            clob_ids = json.loads(mkt.get("clobTokenIds") or "[]")
            if len(outcomes) < 2 or len(clob_ids) < 2:
                continue

            # Map outcomes to token IDs
            token_up = None
            token_down = None
            for i, o in enumerate(outcomes):
                ol = o.lower()
                if ol in ("up", "yes"):
                    token_up = clob_ids[i]
                elif ol in ("down", "no"):
                    token_down = clob_ids[i]
            if not token_up or not token_down:
                continue

            slug = mkt.get("slug") or mkt.get("conditionId") or ""

            info = {
                "condition_id": condition_id,
                "coin": coin,
                "timeframe": tf,
                "question": mkt.get("question") or "",
                "slug": slug,
                "token_up": token_up,
                "token_down": token_down,
                "end_date": end_date,
                "end_ts_ms": end_ms,
            }

            with active_markets_lock:
                if condition_id not in active_markets:
                    active_markets[condition_id] = info
                    new_count += 1
                    # Log to CSV
                    markets_csv.write_row([
                        now_ms, condition_id, slug,
                        coin, tf, mkt.get("question") or "",
                        token_up, token_down,
                        end_date, end_ms,
                    ])
                else:
                    # Update end time if changed
                    active_markets[condition_id].update(info)

    if new_count:
        log.info(f"[discovery] +{new_count} new markets (total active: {len(active_markets)})")


def expire_old_markets():
    """Remove markets past their end time from active set."""
    now_ms = int(time.time() * 1000)
    to_remove = []
    with active_markets_lock:
        for cid, info in active_markets.items():
            end = info.get("end_ts_ms", 0)
            # Keep for OUTCOME_LOOKBACK after expiry for outcome polling
            if end and end + OUTCOME_LOOKBACK * 1000 < now_ms:
                to_remove.append(cid)
        for cid in to_remove:
            del active_markets[cid]
    if to_remove:
        log.info(f"[discovery] Expired {len(to_remove)} old markets")


# ═════════════════════════════════════════════════════════════════════════════
#  Order Book Snapshots
# ═════════════════════════════════════════════════════════════════════════════

def _parse_book(data: dict) -> tuple[list, list]:
    """Parse order book into sorted bid/ask lists of (price, size)."""
    bids = []
    asks = []
    for entry in data.get("bids", []):
        try:
            bids.append((float(entry["price"]), float(entry["size"])))
        except (KeyError, ValueError):
            pass
    for entry in data.get("asks", []):
        try:
            asks.append((float(entry["price"]), float(entry["size"])))
        except (KeyError, ValueError):
            pass
    bids.sort(key=lambda x: -x[0])  # highest first
    asks.sort(key=lambda x: x[0])   # lowest first
    return bids, asks


def snapshot_books():
    """Take a snapshot of order books for all active markets."""
    with active_markets_lock:
        markets = list(active_markets.values())

    if not markets:
        return

    ts = int(time.time() * 1000)
    rows = []

    for mkt in markets:
        # Skip expired
        end = mkt.get("end_ts_ms", 0)
        if end and end < ts:
            continue

        for outcome, token_id in [("Up", mkt["token_up"]), ("Down", mkt["token_down"])]:
            data = _req(f"{CLOB_API}/book", {"token_id": token_id}, timeout=5)
            if not data:
                continue

            bids, asks = _parse_book(data)

            best_bid = bids[0][0] if bids else 0
            best_ask = asks[0][0] if asks else 0
            bid_depth_3 = sum(s for _, s in bids[:3])
            ask_depth_3 = sum(s for _, s in asks[:3])
            mid = (best_bid + best_ask) / 2 if best_bid and best_ask else 0
            spread = best_ask - best_bid if best_bid and best_ask else 0

            # Store for trade enrichment
            with recent_books_lock:
                recent_books[token_id] = (ts, best_bid, best_ask, mid)

            # Build row with top 5 levels
            row = [
                ts, mkt["condition_id"], token_id, outcome,
                best_bid, best_ask, bid_depth_3, ask_depth_3,
                mid, spread,
            ]
            for i in range(BOOK_LEVELS):
                if i < len(bids):
                    row.extend([bids[i][0], bids[i][1]])
                else:
                    row.extend([0, 0])
            for i in range(BOOK_LEVELS):
                if i < len(asks):
                    row.extend([asks[i][0], asks[i][1]])
                else:
                    row.extend([0, 0])
            row.extend([mkt["coin"], mkt["timeframe"], mkt["slug"]])
            rows.append(row)

    if rows:
        book_csv.write_rows(rows)


# ═════════════════════════════════════════════════════════════════════════════
#  Market Outcomes
# ═════════════════════════════════════════════════════════════════════════════

def check_outcomes():
    """Poll recently expired markets for resolution."""
    now_ms = int(time.time() * 1000)

    with active_markets_lock:
        candidates = [
            m for m in active_markets.values()
            if m.get("end_ts_ms")
            and m["end_ts_ms"] < now_ms
            and m["end_ts_ms"] > now_ms - OUTCOME_LOOKBACK * 1000
            and m["condition_id"] not in resolved_set
        ]

    for mkt in candidates:
        cid = mkt["condition_id"]
        data = _req(f"{CLOB_API}/markets/{cid}", timeout=5)
        if not data:
            continue

        # Check for resolution
        tokens = data.get("tokens", [])
        if not tokens:
            continue

        # A market is resolved when one token price is ~1.0 and other is ~0.0
        up_price = None
        down_price = None
        resolution = None

        for tok in tokens:
            outcome_label = tok.get("outcome", "").lower()
            price = float(tok.get("price", 0))
            winner = tok.get("winner", False)

            if outcome_label in ("up", "yes"):
                up_price = price
                if winner:
                    resolution = "Up"
            elif outcome_label in ("down", "no"):
                down_price = price
                if winner:
                    resolution = "Down"

        # Also detect resolution from extreme prices
        if resolution is None:
            if up_price is not None and up_price > 0.95:
                resolution = "Up"
            elif down_price is not None and down_price > 0.95:
                resolution = "Down"

        if resolution:
            resolved_set.add(cid)
            outcomes_csv.write_row([
                cid, mkt["slug"],
                mkt["token_up"], mkt["token_down"],
                mkt["end_ts_ms"], resolution, now_ms,
                up_price, down_price,
                mkt["coin"], mkt["timeframe"], mkt["question"],
            ])
            outcomes_csv.flush()
            log.info(f"[outcome] {mkt['coin']} {mkt['timeframe']} → {resolution} ({cid[:12]}...)")


# ═════════════════════════════════════════════════════════════════════════════
#  Live Trades (WebSocket)
# ═════════════════════════════════════════════════════════════════════════════

def _get_book_at_trade(token_id: str) -> tuple:
    """Get the most recent book snapshot for a token."""
    with recent_books_lock:
        snap = recent_books.get(token_id)
    if snap:
        return snap[1], snap[2], snap[3]  # best_bid, best_ask, mid
    return None, None, None


def _token_to_market(token_id: str) -> dict | None:
    """Look up market info for a token ID."""
    with active_markets_lock:
        for mkt in active_markets.values():
            if mkt["token_up"] == token_id or mkt["token_down"] == token_id:
                return mkt
    return None


def _token_outcome(token_id: str, mkt: dict) -> str:
    if token_id == mkt["token_up"]:
        return "Up"
    elif token_id == mkt["token_down"]:
        return "Down"
    return "Unknown"


def _get_all_token_ids() -> list[str]:
    """Get all active token IDs for WebSocket subscription."""
    with active_markets_lock:
        ids = []
        for mkt in active_markets.values():
            ids.append(mkt["token_up"])
            ids.append(mkt["token_down"])
        return ids


def ws_trade_listener():
    """WebSocket listener for live trades. Reconnects on failure."""
    if websocket is None:
        log.warning("[ws] websocket-client not installed — skipping live trades")
        return

    while True:
        token_ids = _get_all_token_ids()
        if not token_ids:
            log.info("[ws] No active markets yet — waiting 10s...")
            time.sleep(10)
            continue

        try:
            log.info(f"[ws] Connecting to WebSocket with {len(token_ids)} tokens...")
            ws = websocket.create_connection(WS_URL, timeout=30)

            # Subscribe to trade events
            sub_msg = json.dumps({
                "type": "market",
                "assets_id": token_ids,
            })
            ws.send(sub_msg)
            log.info(f"[ws] Subscribed to {len(token_ids)} token channels")

            last_resub = time.time()

            while True:
                # Periodically resubscribe with updated token list
                if time.time() - last_resub > 60:
                    new_ids = _get_all_token_ids()
                    if set(new_ids) != set(token_ids):
                        token_ids = new_ids
                        ws.send(json.dumps({
                            "type": "market",
                            "assets_id": token_ids,
                        }))
                        log.info(f"[ws] Resubscribed with {len(token_ids)} tokens")
                    last_resub = time.time()

                try:
                    raw = ws.recv()
                except websocket.WebSocketTimeoutException:
                    continue

                if not raw:
                    continue

                try:
                    msgs = json.loads(raw)
                except json.JSONDecodeError:
                    continue

                # Handle both single message and array
                if isinstance(msgs, dict):
                    msgs = [msgs]
                if not isinstance(msgs, list):
                    continue

                for msg in msgs:
                    event_type = msg.get("event_type") or msg.get("type") or ""
                    if event_type not in ("trade", "last_trade_price"):
                        continue

                    token_id = msg.get("asset_id") or msg.get("token_id") or ""
                    if not token_id:
                        continue

                    mkt = _token_to_market(token_id)
                    if not mkt:
                        continue

                    outcome = _token_outcome(token_id, mkt)
                    ts = int(float(msg.get("timestamp", 0)) * 1000) or int(time.time() * 1000)
                    side = msg.get("side", "").upper()
                    price = float(msg.get("price", 0))
                    size = float(msg.get("size", 0))
                    trade_id = msg.get("id") or msg.get("trade_id") or ""
                    maker = msg.get("maker_address") or ""
                    taker = msg.get("taker_address") or ""

                    # Enrich with book state
                    bb, ba, bm = _get_book_at_trade(token_id)

                    trades_csv.write_row([
                        ts, mkt["condition_id"], token_id, outcome,
                        side, price, size, trade_id,
                        maker, taker,
                        bb, ba, bm,
                        mkt["coin"], mkt["timeframe"], mkt["slug"],
                    ])

        except Exception as e:
            log.error(f"[ws] WebSocket error: {e}")
            time.sleep(5)


# ═════════════════════════════════════════════════════════════════════════════
#  Polling threads
# ═════════════════════════════════════════════════════════════════════════════

def book_snapshot_loop():
    """Continuously snapshot order books every BOOK_INTERVAL seconds."""
    while True:
        try:
            snapshot_books()
        except Exception as e:
            log.error(f"[book] Error: {e}")
        time.sleep(BOOK_INTERVAL)


def discovery_loop():
    """Discover new markets and expire old ones."""
    while True:
        try:
            discover_markets()
            expire_old_markets()
        except Exception as e:
            log.error(f"[discovery] Error: {e}")
        time.sleep(DISCOVERY_INTERVAL)


def outcome_loop():
    """Check for market resolutions."""
    while True:
        try:
            check_outcomes()
        except Exception as e:
            log.error(f"[outcome] Error: {e}")
        time.sleep(OUTCOME_POLL)


def flush_loop():
    """Periodically flush all CSV writers."""
    while True:
        time.sleep(CSV_FLUSH_INTERVAL)
        try:
            book_csv.flush()
            trades_csv.flush()
            outcomes_csv.flush()
            markets_csv.flush()
        except Exception:
            pass


# ═════════════════════════════════════════════════════════════════════════════
#  Main
# ═════════════════════════════════════════════════════════════════════════════

def run_live_collector():
    log.info("=" * 60)
    log.info("  Live Supplementary Collector")
    log.info("  Books · Trades · Outcomes · Discovery")
    log.info("=" * 60)

    init_csv_writers()

    # Initial market discovery
    log.info("[main] Running initial market discovery...")
    discover_markets()
    with active_markets_lock:
        log.info(f"[main] Found {len(active_markets)} active markets")

    # Start threads
    threads = [
        ("book-snapshots", book_snapshot_loop),
        ("discovery", discovery_loop),
        ("outcomes", outcome_loop),
        ("csv-flusher", flush_loop),
        ("ws-trades", ws_trade_listener),
    ]

    for name, target in threads:
        t = threading.Thread(target=target, name=name, daemon=True)
        t.start()
        log.info(f"[main] Started thread: {name}")

    # Keep main thread alive
    try:
        while True:
            time.sleep(60)
            with active_markets_lock:
                n = len(active_markets)
            log.info(
                f"[main] Active: {n} markets | "
                f"Resolved: {len(resolved_set)} | "
                f"Books: {BOOK_CSV.stat().st_size // 1024}KB | "
                f"Trades: {TRADES_CSV.stat().st_size // 1024 if TRADES_CSV.exists() else 0}KB"
            )
    except KeyboardInterrupt:
        log.info("[main] Shutting down...")
        # Flush all CSVs
        for w in (book_csv, trades_csv, outcomes_csv, markets_csv):
            if w:
                w.flush()


if __name__ == "__main__":
    run_live_collector()
