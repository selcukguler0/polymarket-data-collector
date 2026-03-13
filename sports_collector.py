#!/usr/bin/env python3
"""
Sports Collector — Order Books, Trades, Outcomes, Market Discovery

Collects data for all sports markets on Polymarket:
  • sports_book_snapshots.csv  — order book snapshots for active sports markets
  • sports_live_trades.csv     — real-time trades via WebSocket
  • sports_outcomes.csv        — resolution data for settled markets
  • sports_markets.csv         — discovered active markets log

All CSVs are stored in the data/ directory in append mode.
"""

import collections
import csv
import json
import logging
import os
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
log = logging.getLogger("sports_collector")

DATA_DIR = Path(__file__).parent / "data"
DATA_DIR.mkdir(parents=True, exist_ok=True)

# ── CSV file paths ────────────────────────────────────────────────────────────
BOOK_CSV     = DATA_DIR / "sports_book_snapshots.csv"
TRADES_CSV   = DATA_DIR / "sports_live_trades.csv"
OUTCOMES_CSV = DATA_DIR / "sports_outcomes.csv"
MARKETS_CSV  = DATA_DIR / "sports_markets.csv"

# ── APIs ──────────────────────────────────────────────────────────────────────
CLOB_API   = "https://clob.polymarket.com"
GAMMA_API  = "https://gamma-api.polymarket.com"
WS_URL     = "wss://ws-subscriptions-clob.polymarket.com/ws/market"

# ── Polygon RPC for on-chain enrichment ──────────────────────────────────────
POLYGON_RPCS = [
    "https://rpc-mainnet.matic.quiknode.pro",
    "https://polygon.llamarpc.com",
    "https://polygon.drpc.org",
    "https://1rpc.io/matic",
]
EXCHANGE_CONTRACTS = [
    "0x4bFb41d5B3570DeFd03C39a9A4D8dE6Bd8B8982E",  # NegRiskCTFExchange
    "0xC5d563A36AE78145C45a50134d48A1215220f80a",  # CTFExchange
]
ORDER_FILLED_TOPIC = "0xd0a08e8c493f9c94f29311604c9de1b4e8c8d4c06bd0c789af57f2d65bfec0f6"

# ── Settings ──────────────────────────────────────────────────────────────────
BOOK_INTERVAL        = 5      # seconds between book snapshot cycles
DISCOVERY_INTERVAL   = 120    # seconds between market discovery (slower — many sports)
OUTCOME_POLL         = 30     # seconds between outcome checks
OUTCOME_LOOKBACK     = 600    # check markets settled within last 10 min
CSV_FLUSH_INTERVAL   = 30     # seconds between CSV flushes
BOOK_LEVELS          = 5      # top N bid/ask levels to store
CHAIN_POLL_INTERVAL  = 5      # seconds between on-chain event polls
TRADE_BUFFER_TTL     = 30     # max seconds to hold a trade before flushing unenriched
CHAIN_RPC_RATE       = 0.2    # min seconds between RPC calls (5/sec)
MAX_BOOK_TOKENS      = 200    # max tokens to snapshot per cycle (rate limit safety)

# ── Sport tags to collect ─────────────────────────────────────────────────────
# Use the most specific tags to avoid overlap.
# basketball⊃nba, hockey⊃nhl, baseball⊃mlb — use parent tags.
SPORT_TAGS = [
    "nba", "ncaab", "nfl", "nhl", "mlb", "mls",
    "soccer", "ucl",
    "tennis", "golf", "cricket", "rugby",
    "ufc", "mma", "boxing",
    "esports", "table-tennis",
    "football",  # American football (non-NFL)
    "pickleball", "lacrosse",
    "formula-1", "chess",
]

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

# Pending trades buffer for on-chain enrichment
_pending_trades: list[dict] = []
_pending_trades_lock = threading.Lock()

# On-chain fills cache
_chain_fills: dict[str, collections.deque] = {}
_chain_fills_lock = threading.Lock()
_chain_last_block: int = 0
_chain_rpc_idx: int = 0


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
        "sport", "event_title", "team_player", "market_slug",
    ])

    trades_csv = CSVAppender(TRADES_CSV, [
        "timestamp_ms", "condition_id", "token_id", "outcome",
        "side", "price", "size", "trade_id",
        "maker_address", "taker_address",
        "book_best_bid", "book_best_ask", "book_mid",
        "sport", "event_title", "team_player", "market_slug",
    ])

    outcomes_csv = CSVAppender(OUTCOMES_CSV, [
        "condition_id", "market_slug", "token_yes", "token_no",
        "end_date", "resolution_outcome", "resolution_timestamp_ms",
        "final_yes_price", "final_no_price",
        "sport", "event_title", "team_player", "question",
    ])

    markets_csv = CSVAppender(MARKETS_CSV, [
        "discovered_ts_ms", "condition_id", "market_slug",
        "sport", "event_title", "team_player", "question",
        "token_yes", "token_no",
        "outcomes_raw", "end_date",
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
    # Try ISO format with timezone
    try:
        dt = datetime.fromisoformat(end_date_str.replace("Z", "+00:00"))
        return int(dt.timestamp() * 1000)
    except ValueError:
        pass
    return 0


def discover_markets():
    """Find active sports markets across all sport tags via Gamma Events API."""
    now_ms = int(time.time() * 1000)
    now_utc = datetime.now(timezone.utc)
    new_count = 0

    for tag in SPORT_TAGS:
        try:
            # Paginate: Gamma API returns max ~500 events per call
            offset = 0
            page_size = 200
            while True:
                events = _req(GAMMA_EVENTS_URL, {
                    "tag_slug": tag,
                    "active": "true",
                    "closed": "false",
                    "limit": page_size,
                    "offset": offset,
                })
                if not isinstance(events, list) or not events:
                    break

                for event in events:
                    event_title = event.get("title", "")
                    event_markets = event.get("markets") or []

                    for mkt in event_markets:
                        if not mkt.get("active", False):
                            continue

                        condition_id = mkt.get("conditionId", "")
                        if not condition_id:
                            continue

                        # Skip if already tracked (dedup across overlapping tags)
                        with active_markets_lock:
                            if condition_id in active_markets:
                                continue

                        # Parse clobTokenIds
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

                        # Parse outcomes
                        raw_outcomes = mkt.get("outcomes", "[]")
                        if isinstance(raw_outcomes, str):
                            try:
                                outcomes = json.loads(raw_outcomes)
                            except json.JSONDecodeError:
                                outcomes = ["Yes", "No"]
                        else:
                            outcomes = raw_outcomes
                        if not outcomes or len(outcomes) < 2:
                            outcomes = ["Yes", "No"]

                        question = mkt.get("question", "")
                        slug = mkt.get("slug") or condition_id
                        end_date_str = mkt.get("endDate", "")
                        team_player = mkt.get("groupItemTitle", "") or outcomes[0]

                        # Token mapping: first token = first outcome, second = second outcome
                        token_yes = token_ids[0]
                        token_no = token_ids[1]

                        mkt_info = {
                            "condition_id": condition_id,
                            "sport": tag,
                            "event_title": event_title,
                            "team_player": team_player,
                            "question": question,
                            "slug": slug,
                            "token_yes": token_yes,
                            "token_no": token_no,
                            "outcomes": outcomes,
                            "end_date": end_date_str,
                            "end_ts_ms": _parse_end_date_ms(end_date_str),
                        }

                        with active_markets_lock:
                            if condition_id not in active_markets:
                                active_markets[condition_id] = mkt_info
                                new_count += 1
                                markets_csv.write_row([
                                    now_ms, condition_id, slug,
                                    tag, event_title, team_player, question,
                                    token_yes, token_no,
                                    json.dumps(outcomes), end_date_str,
                                ])

                # Next page
                if len(events) < page_size:
                    break
                offset += page_size
                time.sleep(0.5)  # Rate limit between pages

        except Exception as e:
            log.error(f"[discovery] Error for tag '{tag}': {e}")

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
    bids.sort(key=lambda x: -x[0])
    asks.sort(key=lambda x: x[0])
    return bids, asks


def _fetch_one_book(token_id: str, outcome: str, mkt: dict, ts: int) -> list | None:
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
    row.extend([mkt["sport"], mkt["event_title"], mkt["team_player"], mkt["slug"]])
    return row


def snapshot_books():
    """Take concurrent snapshots of order books.

    Sports markets can be thousands — we sample a batch each cycle,
    prioritizing markets with upcoming end dates.
    """
    with active_markets_lock:
        markets = list(active_markets.values())

    if not markets:
        return

    ts = int(time.time() * 1000)

    # Sort by end date (soonest first), then take a batch
    markets.sort(key=lambda m: m.get("end_ts_ms", 0) or float("inf"))

    # Each market = 2 tokens (yes/no), limit total tokens per cycle
    batch = markets[:MAX_BOOK_TOKENS // 2]

    to_fetch = []
    for mkt in batch:
        outcomes = mkt.get("outcomes", ["Yes", "No"])
        to_fetch.append((outcomes[0] if outcomes else "Yes", mkt["token_yes"], mkt))
        to_fetch.append((outcomes[1] if len(outcomes) > 1 else "No", mkt["token_no"], mkt))

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

        tokens = data.get("tokens", [])
        if not tokens:
            continue

        yes_price = None
        no_price = None
        resolution = None

        for tok in tokens:
            outcome_label = tok.get("outcome", "").lower()
            price = float(tok.get("price", 0))
            winner = tok.get("winner", False)

            if outcome_label in ("yes",) or tok.get("token_id") == mkt["token_yes"]:
                yes_price = price
                if winner:
                    resolution = mkt.get("outcomes", ["Yes", "No"])[0]
            elif outcome_label in ("no",) or tok.get("token_id") == mkt["token_no"]:
                no_price = price
                if winner:
                    resolution = mkt.get("outcomes", ["Yes", "No"])[1]

        if resolution is None:
            if yes_price is not None and yes_price > 0.95:
                resolution = mkt.get("outcomes", ["Yes", "No"])[0]
            elif no_price is not None and no_price > 0.95:
                resolution = mkt.get("outcomes", ["Yes", "No"])[1]

        if resolution:
            resolved_set.add(cid)
            outcomes_csv.write_row([
                cid, mkt["slug"],
                mkt["token_yes"], mkt["token_no"],
                mkt["end_date"], resolution, now_ms,
                yes_price, no_price,
                mkt["sport"], mkt["event_title"], mkt["team_player"], mkt["question"],
            ])
            outcomes_csv.flush()
            log.info(f"[outcome] {mkt['sport']} {mkt['team_player']} → {resolution} ({cid[:12]}...)")


# ═════════════════════════════════════════════════════════════════════════════
#  Live Trades (WebSocket)
# ═════════════════════════════════════════════════════════════════════════════

def _get_book_at_trade(token_id: str) -> tuple:
    with recent_books_lock:
        snap = recent_books.get(token_id)
    if snap:
        return snap[1], snap[2], snap[3]
    return None, None, None


def _token_to_market(token_id: str) -> dict | None:
    with _token_index_lock:
        cid = _token_index.get(token_id)
    if cid:
        with active_markets_lock:
            return active_markets.get(cid)
    return None


def _rebuild_token_index():
    with active_markets_lock:
        new_idx = {}
        for cid, mkt in active_markets.items():
            new_idx[mkt["token_yes"]] = cid
            new_idx[mkt["token_no"]] = cid
    with _token_index_lock:
        _token_index.clear()
        _token_index.update(new_idx)


def _token_outcome(token_id: str, mkt: dict) -> str:
    outcomes = mkt.get("outcomes", ["Yes", "No"])
    if token_id == mkt["token_yes"]:
        return outcomes[0] if outcomes else "Yes"
    elif token_id == mkt["token_no"]:
        return outcomes[1] if len(outcomes) > 1 else "No"
    return "Unknown"


def _get_all_token_ids() -> list[str]:
    with active_markets_lock:
        ids = []
        for mkt in active_markets.values():
            ids.append(mkt["token_yes"])
            ids.append(mkt["token_no"])
        return ids


def ws_trade_listener():
    """WebSocket listener for live trades. Reconnects on failure.

    Polymarket WS has a limit on subscription size, so we batch tokens
    and rotate subscriptions across cycles for very large token sets.
    """
    if websocket is None:
        log.warning("[ws] websocket-client not installed — skipping live trades")
        return

    WS_MAX_TOKENS = 5000  # Max tokens per WS connection

    while True:
        all_token_ids = _get_all_token_ids()
        if not all_token_ids:
            log.info("[ws] No active markets yet — waiting 10s...")
            time.sleep(10)
            continue

        # If too many tokens, take most recent / soonest-expiring
        token_ids = all_token_ids[:WS_MAX_TOKENS]

        try:
            log.info(f"[ws] Connecting with {len(token_ids)} tokens ({len(all_token_ids)} total)...")
            ws = websocket.create_connection(
                WS_URL, timeout=30,
                sslopt={"cert_reqs": ssl.CERT_NONE},
            )
            with _stats_lock:
                _stats["ws_connected"] = True
            log.info("[ws] Connected OK")

            sub_msg = json.dumps({
                "assets_ids": token_ids,
                "type": "market",
                "custom_feature_enabled": True,
            })
            ws.send(sub_msg)
            log.info(f"[ws] Subscribed to {len(token_ids)} tokens")

            last_resub = time.time()

            while True:
                if time.time() - last_resub > 120:
                    new_ids = _get_all_token_ids()[:WS_MAX_TOKENS]
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
                    ts = int(msg.get("timestamp", 0)) or int(time.time() * 1000)
                    side = msg.get("side", "").upper()
                    price = float(msg.get("price", 0))
                    size = float(msg.get("size", 0))
                    trade_id = msg.get("transaction_hash", "")

                    bb, ba, bm = _get_book_at_trade(token_id)

                    row = [
                        ts, mkt["condition_id"], token_id, outcome,
                        side, price, size, trade_id,
                        "", "",  # maker/taker — enriched from chain
                        bb, ba, bm,
                        mkt["sport"], mkt["event_title"], mkt["team_player"], mkt["slug"],
                    ]
                    with _pending_trades_lock:
                        _pending_trades.append({
                            "row": row,
                            "token_id": token_id,
                            "price": price,
                            "size": size,
                            "ts_ms": ts,
                            "buffered_at": time.time(),
                            "maker": "",
                            "taker": "",
                        })

                    with _stats_lock:
                        _stats["trades_captured"] += 1

        except Exception as e:
            log.error(f"[ws] WebSocket error: {e}")
            with _stats_lock:
                _stats["ws_connected"] = False
                _stats["errors"] += 1
            time.sleep(5)


# ═════════════════════════════════════════════════════════════════════════════
#  On-chain enrichment (maker/taker from Polygon OrderFilled events)
# ═════════════════════════════════════════════════════════════════════════════

def _rpc_call(method: str, params: list, timeout: int = 15) -> dict | None:
    global _chain_rpc_idx
    time.sleep(CHAIN_RPC_RATE)
    for attempt in range(len(POLYGON_RPCS)):
        rpc_url = POLYGON_RPCS[_chain_rpc_idx % len(POLYGON_RPCS)]
        try:
            resp = requests.post(rpc_url, json={
                "jsonrpc": "2.0", "method": method, "params": params, "id": 1,
            }, timeout=timeout)
            data = resp.json()
            if "result" in data:
                return data
            err = data.get("error", {})
            log.warning(f"[chain] RPC error from {rpc_url}: {err.get('message', err)}")
        except Exception as e:
            log.warning(f"[chain] RPC {rpc_url} failed: {e}")
        _chain_rpc_idx += 1
    return None


def _decode_order_filled(log_entry: dict) -> dict | None:
    topics = log_entry.get("topics", [])
    data = log_entry.get("data", "0x")
    if len(topics) < 4 or len(data) < 322:
        return None

    maker = "0x" + topics[2][26:]
    taker = "0x" + topics[3][26:]

    d = data[2:]
    maker_asset_id = int(d[0:64], 16)
    taker_asset_id = int(d[64:128], 16)
    maker_amount = int(d[128:192], 16)
    taker_amount = int(d[192:256], 16)

    if maker_asset_id != 0 and taker_asset_id == 0:
        token_id = str(maker_asset_id)
        token_amount = maker_amount / 1e6
        usdc_amount = taker_amount / 1e6
    elif taker_asset_id != 0 and maker_asset_id == 0:
        token_id = str(taker_asset_id)
        token_amount = taker_amount / 1e6
        usdc_amount = maker_amount / 1e6
    else:
        return None

    if token_amount <= 0:
        return None

    price = usdc_amount / token_amount

    return {
        "token_id": token_id,
        "maker": maker,
        "taker": taker,
        "price": round(price, 6),
        "size": round(token_amount, 6),
        "block_number": int(log_entry.get("blockNumber", "0x0"), 16),
    }


def _poll_chain_fills():
    global _chain_last_block

    resp = _rpc_call("eth_blockNumber", [])
    if not resp:
        return
    latest = int(resp["result"], 16)

    if _chain_last_block == 0:
        _chain_last_block = latest - 5

    if latest <= _chain_last_block:
        return

    from_block = hex(_chain_last_block + 1)
    to_block = hex(latest)

    with _token_index_lock:
        known_tokens = set(_token_index.keys())

    new_fills = 0
    for contract_addr in EXCHANGE_CONTRACTS:
        resp = _rpc_call("eth_getLogs", [{
            "fromBlock": from_block,
            "toBlock": to_block,
            "address": contract_addr,
            "topics": [ORDER_FILLED_TOPIC],
        }], timeout=30)
        if not resp:
            continue

        for log_entry in resp.get("result", []):
            fill = _decode_order_filled(log_entry)
            if not fill:
                continue
            if fill["token_id"] not in known_tokens:
                continue

            with _chain_fills_lock:
                if fill["token_id"] not in _chain_fills:
                    _chain_fills[fill["token_id"]] = collections.deque(maxlen=200)
                _chain_fills[fill["token_id"]].append(fill)
                new_fills += 1

    _chain_last_block = latest

    if new_fills:
        with _stats_lock:
            _stats["chain_fills"] = _stats.get("chain_fills", 0) + new_fills


def _match_fill(token_id: str, price: float, size: float) -> tuple[str, str]:
    with _chain_fills_lock:
        fills = _chain_fills.get(token_id)
        if not fills:
            return "", ""

        best_idx = -1
        best_score = float("inf")

        for i, fill in enumerate(fills):
            price_diff = abs(fill["price"] - price) / max(price, 0.001)
            size_diff = abs(fill["size"] - size) / max(size, 0.001)
            if price_diff < 0.01 and size_diff < 0.10:
                score = price_diff + size_diff
                if score < best_score:
                    best_score = score
                    best_idx = i

        if best_idx >= 0:
            fill = fills[best_idx]
            del fills[best_idx]
            return fill["maker"], fill["taker"]

    return "", ""


def _flush_pending_trades():
    now = time.time()
    to_write = []

    with _pending_trades_lock:
        still_pending = []
        for trade in _pending_trades:
            age = now - trade["buffered_at"]
            if not trade["maker"]:
                maker, taker = _match_fill(trade["token_id"], trade["price"], trade["size"])
                if maker:
                    trade["maker"] = maker
                    trade["taker"] = taker
                    with _stats_lock:
                        _stats["enriched_trades"] = _stats.get("enriched_trades", 0) + 1

            if trade["maker"] or age >= TRADE_BUFFER_TTL:
                row = trade["row"]
                row[8] = trade["maker"]
                row[9] = trade["taker"]
                to_write.append(row)
            else:
                still_pending.append(trade)
        _pending_trades[:] = still_pending

    if to_write:
        trades_csv.write_rows(to_write)
        trades_csv.flush()


def chain_enrichment_loop():
    time.sleep(15)
    log.info("[chain] Starting on-chain enrichment (polling OrderFilled events)")

    while True:
        try:
            _poll_chain_fills()
            _flush_pending_trades()
            with _chain_fills_lock:
                for token_id in list(_chain_fills.keys()):
                    dq = _chain_fills[token_id]
                    if not dq:
                        del _chain_fills[token_id]
        except Exception as e:
            log.error(f"[chain] Error: {e}")
            with _stats_lock:
                _stats["errors"] += 1
        time.sleep(CHAIN_POLL_INTERVAL)


# ═════════════════════════════════════════════════════════════════════════════
#  Polling threads
# ═════════════════════════════════════════════════════════════════════════════

def book_snapshot_loop():
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
    while True:
        try:
            discover_markets()
            expire_old_markets()
        except Exception as e:
            log.error(f"[discovery] Error: {e}")
        time.sleep(DISCOVERY_INTERVAL)


def outcome_loop():
    while True:
        try:
            check_outcomes()
        except Exception as e:
            log.error(f"[outcome] Error: {e}")
        time.sleep(OUTCOME_POLL)


def flush_loop():
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

def run_sports_collector():
    log.info("=" * 60)
    log.info("  Sports Collector")
    log.info("  Books · Trades · Outcomes · Discovery")
    log.info(f"  Tags: {', '.join(SPORT_TAGS)}")
    log.info("=" * 60)

    init_csv_writers()

    # Initial market discovery
    log.info("[main] Running initial market discovery (this may take a moment)...")
    discover_markets()
    with active_markets_lock:
        # Count per sport
        by_sport: dict[str, int] = {}
        for m in active_markets.values():
            by_sport[m["sport"]] = by_sport.get(m["sport"], 0) + 1
        log.info(f"[main] Found {len(active_markets)} active sports markets")
        for sport, count in sorted(by_sport.items(), key=lambda x: -x[1]):
            log.info(f"  {sport}: {count}")

    # Start threads
    threads = [
        ("book-snapshots", book_snapshot_loop),
        ("discovery", discovery_loop),
        ("outcomes", outcome_loop),
        ("csv-flusher", flush_loop),
        ("ws-trades", ws_trade_listener),
        ("chain-enrichment", chain_enrichment_loop),
    ]

    for name, target in threads:
        t = threading.Thread(target=target, name=name, daemon=True)
        t.start()
        log.info(f"[main] Started thread: {name}")

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
                chain_fills = _stats.get("chain_fills", 0)
                enriched = _stats.get("enriched_trades", 0)
            with _pending_trades_lock:
                pending = len(_pending_trades)
            log.info(
                f"[STATUS] active_mkts={n} | "
                f"books_this_cycle={books} ({cycle:.1f}s) | "
                f"ws={ws_ok} | "
                f"trades={trades} enriched={enriched} pending={pending} | "
                f"chain_fills={chain_fills} | "
                f"errors={errs}"
            )
    except KeyboardInterrupt:
        log.info("[main] Shutting down...")
        for w in (book_csv, trades_csv, outcomes_csv, markets_csv):
            if w:
                w.flush()


if __name__ == "__main__":
    run_sports_collector()
