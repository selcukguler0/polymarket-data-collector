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
import ssl
import sys
import threading
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
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

# ── Coin config for market discovery (matches collector.py approach) ───────────
COINS = {
    "BTC": {"gamma_tag": "bitcoin", "prefix": "bitcoin up or down"},
    "ETH": {"gamma_tag": "ethereum", "prefix": "ethereum up or down"},
    "SOL": {"gamma_tag": "solana",  "prefix": "solana up or down"},
}
GAMMA_EVENTS_URL = f"{GAMMA_API}/events"

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

# Global counters for status line
_stats_lock = threading.Lock()
_stats = {
    "books_this_cycle": 0,
    "book_cycle_time": 0.0,
    "ws_connected": False,
    "trades_captured": 0,
    "errors": 0,
}

# Token→market reverse index for fast WS lookup
_token_index: dict[str, str] = {}  # token_id → condition_id
_token_index_lock = threading.Lock()


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


def _parse_duration_minutes(question: str) -> int | None:
    """Parse duration from market question text (same logic as collector.py)."""
    q = question.lower()
    m = re.search(r'(\d+)\s*m(?:in)?(?:ute)?s?\b', q)
    if m:
        return int(m.group(1))
    m = re.search(r'(\d+)\s*h(?:our)?s?\b', q)
    if m:
        return int(m.group(1)) * 60
    m = re.search(r'(\d+)\s*d(?:ay)?s?\b', q)
    if m:
        return int(m.group(1)) * 1440
    # Time range like "3:00 PM to 3:05 PM" → 5 mins
    range_pat = r'(\d{1,2}):(\d{2})\s*(am|pm)\s*(?:to|-)\s*(\d{1,2}):(\d{2})\s*(am|pm)'
    m = re.search(range_pat, q, re.IGNORECASE)
    if m:
        h1, m1, ap1 = int(m.group(1)), int(m.group(2)), m.group(3).lower()
        h2, m2, ap2 = int(m.group(4)), int(m.group(5)), m.group(6).lower()
        def to_min(h, mn, ap):
            if ap == "pm" and h != 12: h += 12
            elif ap == "am" and h == 12: h = 0
            return h * 60 + mn
        t1, t2 = to_min(h1, m1, ap1), to_min(h2, m2, ap2)
        diff = t2 - t1 if t2 > t1 else t2 + 1440 - t1
        return diff
    return None


_DURATION_TO_TF = {5: "5m", 15: "15m", 60: "1h", 1440: "1d"}


def discover_markets():
    """Find active Up/Down 5m and 15m markets via Gamma Events API.

    Uses the same approach as collector.py: query /events with tag_slug,
    iterate nested markets, filter by question prefix.
    """
    now_ms = int(time.time() * 1000)
    now_utc = datetime.now(timezone.utc)
    new_count = 0

    for coin, info in COINS.items():
        try:
            events = _req(GAMMA_EVENTS_URL, {
                "tag_slug": info["gamma_tag"],
                "active": "true",
                "closed": "false",
                "limit": 200,
            })
            if not isinstance(events, list):
                log.warning(f"[discovery] No events for {coin}")
                continue

            for event in events:
                event_markets = event.get("markets") or []
                for mkt in event_markets:
                    question = mkt.get("question", "")
                    if not question.lower().startswith(info["prefix"]):
                        continue
                    if not mkt.get("active", False):
                        continue

                    # Skip expired
                    end_date_str = mkt.get("endDate", "")
                    if end_date_str:
                        try:
                            end_dt = datetime.fromisoformat(
                                end_date_str.replace("Z", "+00:00")
                            )
                            if end_dt < now_utc:
                                continue
                        except ValueError:
                            pass

                    # Parse clobTokenIds (may be JSON string or list)
                    raw_ids = mkt.get("clobTokenIds", "[]")
                    if isinstance(raw_ids, str):
                        try:
                            token_ids = json.loads(raw_ids)
                        except json.JSONDecodeError:
                            continue
                    else:
                        token_ids = raw_ids
                    if not token_ids or len(token_ids) < 2:
                        continue

                    condition_id = mkt.get("conditionId", "")
                    if not condition_id:
                        continue

                    # Parse timeframe from question
                    duration_mins = _parse_duration_minutes(question)
                    tf = _DURATION_TO_TF.get(duration_mins)
                    if tf not in ("5m", "15m"):
                        continue

                    # Parse outcomes (may be JSON string or list)
                    raw_outcomes = mkt.get("outcomes", "[]")
                    if isinstance(raw_outcomes, str):
                        try:
                            outcomes = json.loads(raw_outcomes)
                        except json.JSONDecodeError:
                            outcomes = []
                    else:
                        outcomes = raw_outcomes

                    # Map outcomes to token IDs
                    token_up = token_ids[0]   # First is Up by convention
                    token_down = token_ids[1]  # Second is Down
                    # But verify if outcome labels exist
                    if len(outcomes) >= 2:
                        for i, o in enumerate(outcomes):
                            ol = o.lower()
                            if ol in ("up", "yes") and i < len(token_ids):
                                token_up = token_ids[i]
                            elif ol in ("down", "no") and i < len(token_ids):
                                token_down = token_ids[i]

                    end_ms = _parse_end_date_ms(end_date_str)
                    slug = mkt.get("slug") or condition_id

                    mkt_info = {
                        "condition_id": condition_id,
                        "coin": coin,
                        "timeframe": tf,
                        "question": question,
                        "slug": slug,
                        "token_up": token_up,
                        "token_down": token_down,
                        "end_date": end_date_str,
                        "end_ts_ms": end_ms,
                    }

                    with active_markets_lock:
                        if condition_id not in active_markets:
                            active_markets[condition_id] = mkt_info
                            new_count += 1
                            markets_csv.write_row([
                                now_ms, condition_id, slug,
                                coin, tf, question,
                                token_up, token_down,
                                end_date_str, end_ms,
                            ])
                        else:
                            active_markets[condition_id].update(mkt_info)

        except Exception as e:
            log.error(f"[discovery] Error for {coin}: {e}")

    if new_count:
        _rebuild_token_index()
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


def _fetch_one_book(token_id: str, outcome: str, mkt: dict, ts: int) -> list | None:
    """Fetch and parse one order book. Returns CSV row or None."""
    try:
        resp = requests.get(
            f"{CLOB_API}/book", params={"token_id": token_id}, timeout=5
        )
        if resp.status_code == 429:
            with _stats_lock:
                _stats["errors"] += 1
            return None
        resp.raise_for_status()
        data = resp.json()
    except Exception:
        return None

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
    return row


# Track last snapshot time per condition_id for tiered intervals
_last_book_snap: dict[str, float] = {}


def snapshot_books():
    """Take concurrent snapshots of order books with tiered intervals.

    STE < 120s  → every 2s  (active trading)
    STE 120-240s → every 5s
    STE 240-300s → every 15s
    STE > 300s   → every 30s (far from expiry, still ~50/50)
    """
    with active_markets_lock:
        markets = list(active_markets.values())

    if not markets:
        return

    ts = int(time.time() * 1000)
    now_s = ts / 1000

    # Determine which markets need a snapshot this cycle
    # Priority: close-to-expiry markets get fetched every cycle
    # Far markets get spread across cycles (max ~40 tokens per cycle from this tier)
    urgent = []     # STE < 120s — every 2s
    medium = []     # STE 120-300s — every 5-15s
    background = [] # STE > 300s — batched across cycles

    for mkt in markets:
        end = mkt.get("end_ts_ms", 0)
        if end and end < ts:
            continue

        ste = (end - ts) / 1000 if end else 9999
        cid = mkt["condition_id"]
        last = _last_book_snap.get(cid, 0)
        elapsed = now_s - last

        if ste < 120:
            if elapsed >= BOOK_INTERVAL:
                urgent.append(mkt)
                _last_book_snap[cid] = now_s
        elif ste < 300:
            interval = 5 if ste < 240 else 15
            if elapsed >= interval:
                medium.append(mkt)
                _last_book_snap[cid] = now_s
        else:
            if elapsed >= 30:
                background.append(mkt)

    # Limit background batch to ~20 markets (40 tokens) per cycle
    MAX_BG_BATCH = 20
    bg_batch = background[:MAX_BG_BATCH]
    for mkt in bg_batch:
        _last_book_snap[mkt["condition_id"]] = now_s

    to_fetch = []
    for mkt in urgent + medium + bg_batch:
        to_fetch.append(("Up", mkt["token_up"], mkt))
        to_fetch.append(("Down", mkt["token_down"], mkt))

    if not to_fetch:
        return

    # Concurrent fetching with bounded parallelism
    rows = []
    with ThreadPoolExecutor(max_workers=50) as pool:
        futures = {
            pool.submit(_fetch_one_book, token_id, outcome, mkt, ts): (outcome, mkt)
            for outcome, token_id, mkt in to_fetch
        }
        for fut in as_completed(futures):
            row = fut.result()
            if row:
                rows.append(row)

    if rows:
        book_csv.write_rows(rows)

    with _stats_lock:
        _stats["books_this_cycle"] = len(rows)


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
    """Look up market info for a token ID via reverse index."""
    with _token_index_lock:
        cid = _token_index.get(token_id)
    if cid:
        with active_markets_lock:
            return active_markets.get(cid)
    return None


def _rebuild_token_index():
    """Rebuild token→condition_id reverse index."""
    with active_markets_lock:
        new_idx = {}
        for cid, mkt in active_markets.items():
            new_idx[mkt["token_up"]] = cid
            new_idx[mkt["token_down"]] = cid
    with _token_index_lock:
        _token_index.clear()
        _token_index.update(new_idx)


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
            log.info(f"[ws] Connecting to {WS_URL} with {len(token_ids)} tokens...")
            ws = websocket.create_connection(
                WS_URL, timeout=30,
                sslopt={"cert_reqs": ssl.CERT_NONE},
            )
            with _stats_lock:
                _stats["ws_connected"] = True
            log.info("[ws] Connected OK")

            # Correct subscription format: assets_ids (plural!)
            sub_msg = json.dumps({
                "assets_ids": token_ids,
                "type": "market",
                "custom_feature_enabled": True,
            })
            ws.send(sub_msg)
            log.info(f"[ws] Subscribed to {len(token_ids)} tokens")

            last_resub = time.time()

            while True:
                # Periodically resubscribe with updated token list
                if time.time() - last_resub > 60:
                    new_ids = _get_all_token_ids()
                    if set(new_ids) != set(token_ids):
                        token_ids = new_ids
                        ws.send(json.dumps({
                            "assets_ids": token_ids,
                            "type": "market",
                            "custom_feature_enabled": True,
                        }))
                        log.info(f"[ws] Resubscribed with {len(token_ids)} tokens")
                    last_resub = time.time()

                try:
                    ws.settimeout(5)
                    raw = ws.recv()
                except websocket.WebSocketTimeoutException:
                    continue

                if not raw:
                    continue

                try:
                    data = json.loads(raw)
                except json.JSONDecodeError:
                    continue

                # Messages come as arrays or single dicts
                if isinstance(data, dict):
                    items = [data]
                elif isinstance(data, list):
                    items = data
                else:
                    continue

                for msg in items:
                    if not isinstance(msg, dict):
                        continue
                    event_type = msg.get("event_type", "")
                    if event_type != "last_trade_price":
                        continue

                    token_id = msg.get("asset_id", "")
                    if not token_id:
                        continue

                    mkt = _token_to_market(token_id)
                    if not mkt:
                        continue

                    outcome = _token_outcome(token_id, mkt)
                    # timestamp is already in milliseconds
                    ts = int(msg.get("timestamp", 0)) or int(time.time() * 1000)
                    side = msg.get("side", "").upper()
                    price = float(msg.get("price", 0))
                    size = float(msg.get("size", 0))
                    trade_id = msg.get("transaction_hash", "")

                    # Enrich with book state
                    bb, ba, bm = _get_book_at_trade(token_id)
                    # Compute spread and seconds since last book snap
                    book_spread = round(ba - bb, 6) if bb and ba else None
                    with recent_books_lock:
                        snap = recent_books.get(token_id)
                    snap_age = round((ts - snap[0]) / 1000, 1) if snap else None

                    trades_csv.write_row([
                        ts, mkt["condition_id"], token_id, outcome,
                        side, price, size, trade_id,
                        "", "",  # maker/taker not available in public channel
                        bb, ba, bm,
                        mkt["coin"], mkt["timeframe"], mkt["slug"],
                    ])
                    trades_csv.flush()

                    with _stats_lock:
                        _stats["trades_captured"] += 1

        except Exception as e:
            log.error(f"[ws] WebSocket error: {e}")
            with _stats_lock:
                _stats["ws_connected"] = False
                _stats["errors"] += 1
            time.sleep(5)


# ═════════════════════════════════════════════════════════════════════════════
#  Polling threads
# ═════════════════════════════════════════════════════════════════════════════

def book_snapshot_loop():
    """Continuously snapshot order books, accounting for fetch time."""
    while True:
        start = time.time()
        try:
            snapshot_books()
        except Exception as e:
            log.error(f"[book] Error: {e}")
            with _stats_lock:
                _stats["errors"] += 1
        elapsed = time.time() - start
        with _stats_lock:
            _stats["book_cycle_time"] = elapsed
        sleep_time = max(0, BOOK_INTERVAL - elapsed)
        time.sleep(sleep_time)


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

    # Keep main thread alive — status line every 10s
    try:
        while True:
            time.sleep(10)
            with active_markets_lock:
                n = len(active_markets)
            with _stats_lock:
                books = _stats["books_this_cycle"]
                cycle = _stats["book_cycle_time"]
                ws_ok = _stats["ws_connected"]
                trades = _stats["trades_captured"]
                errs = _stats["errors"]
            log.info(
                f"[STATUS] active_mkts={n} | "
                f"books_this_cycle={books} (avg {cycle:.1f}s) | "
                f"ws_connected={ws_ok} | "
                f"trades_captured={trades} | "
                f"errors={errs}"
            )
    except KeyboardInterrupt:
        log.info("[main] Shutting down...")
        for w in (book_csv, trades_csv, outcomes_csv, markets_csv):
            if w:
                w.flush()


if __name__ == "__main__":
    run_live_collector()
