#!/usr/bin/env python3
"""
Backtest data collector — Binance OHLCV + Polymarket Up/Down book snapshots.

Collects for BTC, ETH, XRP, SOL across 5m, 15m, 1h, 1d timeframes.
Stores in SQLite. Backfills Binance history on first run, then polls continuously.
Polymarket books are snapshotted every ~5s for active markets.

Extra fields collected:
  - Binance: funding rate, open interest (futures), mark price
  - Polymarket: full book depth (top 10 levels), spread, volume from CLOB
"""

import sqlite3
import time
import json
import logging
import re
import sys
import threading
from datetime import datetime, timezone
from pathlib import Path

import requests

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[logging.StreamHandler(sys.stdout)],
)
log = logging.getLogger(__name__)

DB_PATH = Path(__file__).parent / "data" / "candles.db"

# ── Binance endpoints ──
BINANCE_KLINES = "https://api.binance.com/api/v3/klines"
BINANCE_TICKER = "https://api.binance.com/api/v3/ticker/24hr"
BINANCE_FUNDING = "https://fapi.binance.com/fapi/v1/fundingRate"
BINANCE_OI = "https://fapi.binance.com/fapi/v1/openInterest"
BINANCE_MARK = "https://fapi.binance.com/fapi/v1/premiumIndex"

# ── Polymarket endpoints ──
CLOB_BOOK_URL = "https://clob.polymarket.com/book"
GAMMA_EVENTS_URL = "https://gamma-api.polymarket.com/events"

# ── Coins ──
COINS = {
    "BTC": {"spot": "BTCUSDT", "futures": "BTCUSDT", "gamma_tag": "bitcoin", "prefix": "bitcoin up or down"},
    "ETH": {"spot": "ETHUSDT", "futures": "ETHUSDT", "gamma_tag": "ethereum", "prefix": "ethereum up or down"},
    "XRP": {"spot": "XRPUSDT", "futures": "XRPUSDT", "gamma_tag": "xrp", "prefix": "xrp up or down"},
    "SOL": {"spot": "SOLUSDT", "futures": "SOLUSDT", "gamma_tag": "solana", "prefix": "solana up or down"},
}

TIMEFRAMES = {
    "5m":  {"binance": "5m",  "seconds": 300,   "backfill_days": 30},
    "15m": {"binance": "15m", "seconds": 900,   "backfill_days": 60},
    "1h":  {"binance": "1h",  "seconds": 3600,  "backfill_days": 180},
    "1d":  {"binance": "1d",  "seconds": 86400, "backfill_days": 365 * 2},
}

POLL_INTERVALS = {"5m": 60, "15m": 120, "1h": 300, "1d": 3600}

# Polymarket snapshot interval (seconds)
PM_SNAPSHOT_INTERVAL = 5
# Market discovery interval (seconds)
PM_DISCOVERY_INTERVAL = 30


# ═══════════════════════════════════════════════════════════════
#  Database setup
# ═══════════════════════════════════════════════════════════════

def get_db() -> sqlite3.Connection:
    DB_PATH.parent.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(str(DB_PATH), timeout=120)
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA synchronous=NORMAL")
    conn.execute("PRAGMA busy_timeout=120000")

    # Binance OHLCV candles
    conn.execute("""
        CREATE TABLE IF NOT EXISTS candles (
            coin            TEXT NOT NULL,
            timeframe       TEXT NOT NULL,
            open_time       INTEGER NOT NULL,
            open            REAL NOT NULL,
            high            REAL NOT NULL,
            low             REAL NOT NULL,
            close           REAL NOT NULL,
            volume          REAL NOT NULL,
            close_time      INTEGER NOT NULL,
            quote_volume    REAL NOT NULL,
            trades          INTEGER NOT NULL,
            taker_buy_volume    REAL NOT NULL,
            taker_buy_quote     REAL NOT NULL,
            PRIMARY KEY (coin, timeframe, open_time)
        )
    """)

    # Binance funding rate snapshots
    conn.execute("""
        CREATE TABLE IF NOT EXISTS funding_rates (
            coin            TEXT NOT NULL,
            timestamp       INTEGER NOT NULL,
            funding_rate    REAL NOT NULL,
            mark_price      REAL,
            PRIMARY KEY (coin, timestamp)
        )
    """)

    # Binance open interest snapshots
    conn.execute("""
        CREATE TABLE IF NOT EXISTS open_interest (
            coin            TEXT NOT NULL,
            timestamp       INTEGER NOT NULL,
            open_interest   REAL NOT NULL,
            open_interest_usdt  REAL,
            PRIMARY KEY (coin, timestamp)
        )
    """)

    # Binance 24h ticker snapshots (volatility, volume context)
    conn.execute("""
        CREATE TABLE IF NOT EXISTS ticker_snapshots (
            coin            TEXT NOT NULL,
            timestamp       INTEGER NOT NULL,
            price_change_pct    REAL,
            high_24h        REAL,
            low_24h         REAL,
            volume_24h      REAL,
            quote_volume_24h    REAL,
            weighted_avg_price  REAL,
            last_price      REAL,
            PRIMARY KEY (coin, timestamp)
        )
    """)

    # Polymarket book snapshots (Up and Down tokens)
    conn.execute("""
        CREATE TABLE IF NOT EXISTS pm_book_snapshots (
            id              INTEGER PRIMARY KEY AUTOINCREMENT,
            coin            TEXT NOT NULL,
            timeframe       TEXT NOT NULL,
            timestamp       INTEGER NOT NULL,
            condition_id    TEXT NOT NULL,
            question        TEXT,
            end_date        TEXT,
            up_best_bid     REAL,
            up_best_ask     REAL,
            up_mid          REAL,
            up_spread       REAL,
            up_bid_depth    REAL,
            up_ask_depth    REAL,
            up_book_json    TEXT,
            down_best_bid   REAL,
            down_best_ask   REAL,
            down_mid        REAL,
            down_spread     REAL,
            down_bid_depth  REAL,
            down_ask_depth  REAL,
            down_book_json  TEXT,
            combined_mid    REAL,
            implied_edge    REAL
        )
    """)
    conn.execute("""
        CREATE INDEX IF NOT EXISTS idx_pm_book_coin_tf_ts
        ON pm_book_snapshots (coin, timeframe, timestamp)
    """)

    # Polymarket resolved market outcomes (for backtest ground truth)
    conn.execute("""
        CREATE TABLE IF NOT EXISTS pm_market_outcomes (
            condition_id    TEXT PRIMARY KEY,
            coin            TEXT NOT NULL,
            timeframe       TEXT NOT NULL,
            question        TEXT,
            start_date      TEXT,
            end_date        TEXT,
            outcome         TEXT,
            up_final_price  REAL,
            down_final_price REAL
        )
    """)

    conn.execute("""
        CREATE INDEX IF NOT EXISTS idx_candles_lookup
        ON candles (coin, timeframe, open_time)
    """)
    conn.commit()
    return conn


# ═══════════════════════════════════════════════════════════════
#  Binance data collection
# ═══════════════════════════════════════════════════════════════

def _request(url: str, params: dict, timeout: int = 15) -> dict | list | None:
    for attempt in range(5):
        try:
            resp = requests.get(url, params=params, timeout=timeout)
            if resp.status_code == 429:
                wait = int(resp.headers.get("Retry-After", 30))
                log.warning(f"Rate limited, waiting {wait}s")
                time.sleep(wait)
                continue
            resp.raise_for_status()
            return resp.json()
        except requests.RequestException as e:
            log.warning(f"Request {url} failed (attempt {attempt+1}): {e}")
            time.sleep(2 ** attempt)
    return None


def fetch_candles(symbol: str, interval: str, start_ms: int | None = None, limit: int = 1000) -> list:
    params = {"symbol": symbol, "interval": interval, "limit": limit}
    if start_ms is not None:
        params["startTime"] = start_ms
    return _request(BINANCE_KLINES, params) or []


def insert_candles(conn: sqlite3.Connection, coin: str, timeframe: str, raw: list) -> int:
    if not raw:
        return 0
    rows = [
        (coin, timeframe, int(c[0]), float(c[1]), float(c[2]), float(c[3]), float(c[4]),
         float(c[5]), int(c[6]), float(c[7]), int(c[8]), float(c[9]), float(c[10]))
        for c in raw
    ]
    conn.executemany("""
        INSERT OR REPLACE INTO candles
        (coin, timeframe, open_time, open, high, low, close, volume,
         close_time, quote_volume, trades, taker_buy_volume, taker_buy_quote)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
    """, rows)
    conn.commit()
    return len(rows)


def get_latest_time(conn: sqlite3.Connection, coin: str, timeframe: str) -> int | None:
    row = conn.execute(
        "SELECT MAX(open_time) FROM candles WHERE coin = ? AND timeframe = ?",
        (coin, timeframe),
    ).fetchone()
    return row[0] if row and row[0] else None


def backfill(conn: sqlite3.Connection, coin: str, timeframe: str):
    tf_config = TIMEFRAMES[timeframe]
    symbol = COINS[coin]["spot"]

    latest = get_latest_time(conn, coin, timeframe)
    if latest:
        start_ms = latest + 1
        log.info(f"[{coin}/{timeframe}] Resuming backfill from {datetime.fromtimestamp(start_ms/1000, tz=timezone.utc)}")
    else:
        days = tf_config["backfill_days"]
        start_ms = int((time.time() - days * 86400) * 1000)
        log.info(f"[{coin}/{timeframe}] Fresh backfill: {days} days of history")

    total = 0
    while True:
        candles = fetch_candles(symbol, tf_config["binance"], start_ms=start_ms, limit=1000)
        if not candles:
            break
        count = insert_candles(conn, coin, timeframe, candles)
        total += count
        last_open = int(candles[-1][0])
        if last_open <= start_ms or len(candles) < 1000:
            break
        start_ms = last_open + 1
        time.sleep(0.2)
    log.info(f"[{coin}/{timeframe}] Backfill complete: {total} candles")


def poll_binance(conn: sqlite3.Connection, coin: str, timeframe: str):
    tf_config = TIMEFRAMES[timeframe]
    symbol = COINS[coin]["spot"]
    latest = get_latest_time(conn, coin, timeframe)
    if latest is None:
        backfill(conn, coin, timeframe)
        return
    start_ms = latest + 1
    candles = fetch_candles(symbol, tf_config["binance"], start_ms=start_ms, limit=100)
    count = insert_candles(conn, coin, timeframe, candles)
    if count > 0:
        log.info(f"[{coin}/{timeframe}] +{count} new candles")


def collect_funding_rates(conn: sqlite3.Connection):
    """Snapshot funding rates for all coins."""
    ts = int(time.time() * 1000)
    for coin, info in COINS.items():
        data = _request(BINANCE_MARK, {"symbol": info["futures"]})
        if data:
            try:
                conn.execute(
                    "INSERT OR REPLACE INTO funding_rates (coin, timestamp, funding_rate, mark_price) VALUES (?, ?, ?, ?)",
                    (coin, ts, float(data.get("lastFundingRate", 0)), float(data.get("markPrice", 0))),
                )
            except Exception as e:
                log.warning(f"Funding rate insert error for {coin}: {e}")
    conn.commit()


def collect_open_interest(conn: sqlite3.Connection):
    """Snapshot open interest for all coins."""
    ts = int(time.time() * 1000)
    for coin, info in COINS.items():
        data = _request(BINANCE_OI, {"symbol": info["futures"]})
        if data:
            try:
                oi = float(data.get("openInterest", 0))
                mark_data = _request(BINANCE_MARK, {"symbol": info["futures"]})
                mark_price = float(mark_data.get("markPrice", 0)) if mark_data else 0
                conn.execute(
                    "INSERT OR REPLACE INTO open_interest (coin, timestamp, open_interest, open_interest_usdt) VALUES (?, ?, ?, ?)",
                    (coin, ts, oi, oi * mark_price),
                )
            except Exception as e:
                log.warning(f"OI insert error for {coin}: {e}")
    conn.commit()


def collect_ticker_snapshots(conn: sqlite3.Connection):
    """Snapshot 24h ticker stats for all coins."""
    ts = int(time.time() * 1000)
    for coin, info in COINS.items():
        data = _request(BINANCE_TICKER, {"symbol": info["spot"]})
        if data:
            try:
                conn.execute("""
                    INSERT OR REPLACE INTO ticker_snapshots
                    (coin, timestamp, price_change_pct, high_24h, low_24h, volume_24h,
                     quote_volume_24h, weighted_avg_price, last_price)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                """, (
                    coin, ts,
                    float(data.get("priceChangePercent", 0)),
                    float(data.get("highPrice", 0)),
                    float(data.get("lowPrice", 0)),
                    float(data.get("volume", 0)),
                    float(data.get("quoteVolume", 0)),
                    float(data.get("weightedAvgPrice", 0)),
                    float(data.get("lastPrice", 0)),
                ))
            except Exception as e:
                log.warning(f"Ticker insert error for {coin}: {e}")
    conn.commit()


# ═══════════════════════════════════════════════════════════════
#  Polymarket data collection
# ═══════════════════════════════════════════════════════════════

def parse_duration_minutes(question: str) -> int | None:
    """Parse duration from market question, e.g. '5m', '15m', '1h', '1 day'."""
    q = question.lower()
    # "5m", "15m" etc
    m = re.search(r'(\d+)\s*m(?:in)?(?:ute)?s?\b', q)
    if m:
        return int(m.group(1))
    # "1h", "1 hour"
    m = re.search(r'(\d+)\s*h(?:our)?s?\b', q)
    if m:
        return int(m.group(1)) * 60
    # "1 day"
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
            if ap == "pm" and h != 12:
                h += 12
            elif ap == "am" and h == 12:
                h = 0
            return h * 60 + mn
        t1, t2 = to_min(h1, m1, ap1), to_min(h2, m2, ap2)
        diff = t2 - t1 if t2 > t1 else t2 + 1440 - t1
        return diff
    return None


DURATION_TO_TF = {5: "5m", 15: "15m", 60: "1h", 1440: "1d"}


def discover_polymarket_markets() -> list[dict]:
    """Query Gamma API for all active Up/Down binary markets across all coins."""
    markets = []
    now_utc = datetime.now(timezone.utc)

    for coin, info in COINS.items():
        try:
            params = {
                "tag_slug": info["gamma_tag"],
                "active": "true",
                "closed": "false",
                "limit": 200,
            }
            resp = requests.get(GAMMA_EVENTS_URL, params=params, timeout=15)
            resp.raise_for_status()
            events = resp.json()

            for event in events:
                event_markets = event.get("markets") or []
                neg_risk = event.get("negRisk", False)

                for mkt in event_markets:
                    question = mkt.get("question", "")
                    if not question.lower().startswith(info["prefix"]):
                        continue
                    if not mkt.get("active", False):
                        continue

                    # Skip already-ended markets
                    end_date_str = mkt.get("endDate", "")
                    if end_date_str:
                        try:
                            end_dt = datetime.fromisoformat(end_date_str.replace("Z", "+00:00"))
                            if end_dt < now_utc:
                                continue
                        except ValueError:
                            pass

                    # clobTokenIds is a JSON string like '["123...", "456..."]'
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

                    duration_mins = parse_duration_minutes(question)
                    tf = DURATION_TO_TF.get(duration_mins)
                    if not tf:
                        continue

                    markets.append({
                        "coin": coin,
                        "timeframe": tf,
                        "condition_id": condition_id,
                        "token_id_up": token_ids[0],
                        "token_id_down": token_ids[1],
                        "question": question,
                        "end_date": end_date_str,
                        "neg_risk": neg_risk,
                        "tick_size": float(mkt.get("orderPriceMinTickSize", 0.01)),
                    })

        except Exception as e:
            log.warning(f"[gamma] Discovery error for {coin}: {e}")

    return markets


def fetch_book(token_id: str) -> dict | None:
    """Fetch order book for a single token from Polymarket CLOB."""
    try:
        resp = requests.get(CLOB_BOOK_URL, params={"token_id": token_id}, timeout=10)
        resp.raise_for_status()
        return resp.json()
    except Exception as e:
        log.warning(f"Book fetch error for {token_id[:20]}...: {e}")
        return None


def parse_book_side(levels: list[dict]) -> list[tuple[float, float]]:
    """Parse book levels into [(price, size), ...]."""
    result = []
    for lvl in levels:
        try:
            result.append((float(lvl["price"]), float(lvl["size"])))
        except (KeyError, ValueError):
            continue
    return result


def snapshot_polymarket_books(conn: sqlite3.Connection, active_markets: list[dict]):
    """Take a book snapshot for all active Polymarket markets."""
    ts = int(time.time() * 1000)
    rows = []

    for mkt in active_markets:
        try:
            up_book = fetch_book(mkt["token_id_up"])
            down_book = fetch_book(mkt["token_id_down"])
            if not up_book or not down_book:
                continue

            up_bids = parse_book_side(up_book.get("bids", []))
            up_asks = parse_book_side(up_book.get("asks", []))
            down_bids = parse_book_side(down_book.get("bids", []))
            down_asks = parse_book_side(down_book.get("asks", []))

            up_best_bid = up_bids[0][0] if up_bids else None
            up_best_ask = up_asks[0][0] if up_asks else None
            up_mid = (up_best_bid + up_best_ask) / 2 if up_best_bid and up_best_ask else None
            up_spread = (up_best_ask - up_best_bid) if up_best_bid and up_best_ask else None
            up_bid_depth = sum(p * s for p, s in up_bids[:10])
            up_ask_depth = sum(p * s for p, s in up_asks[:10])

            down_best_bid = down_bids[0][0] if down_bids else None
            down_best_ask = down_asks[0][0] if down_asks else None
            down_mid = (down_best_bid + down_best_ask) / 2 if down_best_bid and down_best_ask else None
            down_spread = (down_best_ask - down_best_bid) if down_best_bid and down_best_ask else None
            down_bid_depth = sum(p * s for p, s in down_bids[:10])
            down_ask_depth = sum(p * s for p, s in down_asks[:10])

            combined_mid = (up_mid + down_mid) if up_mid and down_mid else None
            implied_edge = (1.0 - combined_mid) if combined_mid else None

            # Store top 10 levels as JSON for deep analysis
            up_book_json = json.dumps({"bids": up_bids[:10], "asks": up_asks[:10]})
            down_book_json = json.dumps({"bids": down_bids[:10], "asks": down_asks[:10]})

            rows.append((
                mkt["coin"], mkt["timeframe"], ts, mkt["condition_id"], mkt["question"], mkt["end_date"],
                up_best_bid, up_best_ask, up_mid, up_spread, up_bid_depth, up_ask_depth, up_book_json,
                down_best_bid, down_best_ask, down_mid, down_spread, down_bid_depth, down_ask_depth, down_book_json,
                combined_mid, implied_edge,
            ))
        except Exception as e:
            log.warning(f"[pm] Snapshot error for {mkt['coin']}/{mkt['timeframe']}: {e}")

    if rows:
        conn.executemany("""
            INSERT INTO pm_book_snapshots
            (coin, timeframe, timestamp, condition_id, question, end_date,
             up_best_bid, up_best_ask, up_mid, up_spread, up_bid_depth, up_ask_depth, up_book_json,
             down_best_bid, down_best_ask, down_mid, down_spread, down_bid_depth, down_ask_depth, down_book_json,
             combined_mid, implied_edge)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """, rows)
        conn.commit()


def check_resolved_markets(conn: sqlite3.Connection, seen_conditions: set[str]):
    """Check if any tracked markets have resolved, record outcome."""
    rows = conn.execute("""
        SELECT DISTINCT condition_id, coin, timeframe, question, end_date
        FROM pm_book_snapshots
        WHERE condition_id NOT IN (SELECT condition_id FROM pm_market_outcomes)
        GROUP BY condition_id
    """).fetchall()

    for row in rows:
        cid, coin, tf, question, end_date = row
        if cid in seen_conditions:
            continue
        try:
            resp = requests.get(f"https://clob.polymarket.com/market/{cid}", timeout=10)
            if resp.status_code == 200:
                data = resp.json()
                # Check if market has resolved
                if data.get("closed") or data.get("resolved"):
                    tokens = data.get("tokens", [])
                    up_price = None
                    down_price = None
                    outcome = None
                    for t in tokens:
                        if t.get("outcome") in ("Yes", "Up"):
                            up_price = float(t.get("price", 0))
                        elif t.get("outcome") in ("No", "Down"):
                            down_price = float(t.get("price", 0))

                    if up_price is not None and up_price > 0.9:
                        outcome = "Up"
                    elif down_price is not None and down_price > 0.9:
                        outcome = "Down"

                    if outcome:
                        conn.execute("""
                            INSERT OR REPLACE INTO pm_market_outcomes
                            (condition_id, coin, timeframe, question, end_date, outcome, up_final_price, down_final_price)
                            VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                        """, (cid, coin, tf, question, end_date, outcome, up_price, down_price))
                        conn.commit()
                        seen_conditions.add(cid)
                        log.info(f"[pm] Resolved: {question} → {outcome}")
        except Exception:
            pass


# ═══════════════════════════════════════════════════════════════
#  Polymarket collector thread
# ═══════════════════════════════════════════════════════════════

def polymarket_collector_loop():
    """Runs in its own thread — discovers markets and snapshots books."""
    log.info("[pm] Polymarket collector starting...")
    conn = get_db()
    active_markets: list[dict] = []
    seen_conditions: set[str] = set()
    last_discovery = 0.0
    last_resolution_check = 0.0

    while True:
        now = time.time()

        # Discover new markets periodically
        if now - last_discovery >= PM_DISCOVERY_INTERVAL:
            try:
                new_markets = discover_polymarket_markets()
                if new_markets:
                    active_markets = new_markets
                    coins_found = set(m["coin"] for m in active_markets)
                    tfs_found = set(m["timeframe"] for m in active_markets)
                    log.info(f"[pm] Tracking {len(active_markets)} markets: coins={coins_found}, tfs={tfs_found}")
                else:
                    log.info("[pm] No active markets found (may be off-hours)")
            except Exception as e:
                log.error(f"[pm] Discovery error: {e}")
            last_discovery = now

        # Snapshot books
        if active_markets:
            snapshot_polymarket_books(conn, active_markets)

        # Check for resolved markets every 2 minutes
        if now - last_resolution_check >= 120:
            try:
                check_resolved_markets(conn, seen_conditions)
            except Exception as e:
                log.warning(f"[pm] Resolution check error: {e}")
            last_resolution_check = now

        time.sleep(PM_SNAPSHOT_INTERVAL)


# ═══════════════════════════════════════════════════════════════
#  Main collector
# ═══════════════════════════════════════════════════════════════

def run_collector():
    log.info("=" * 60)
    log.info("  Backtest Data Collector")
    log.info("  Binance OHLCV + Funding + OI + Polymarket Books")
    log.info("=" * 60)

    conn = get_db()

    # Phase 1: Backfill Binance candles (before starting PM thread to avoid lock contention)
    log.info("=== BINANCE BACKFILL ===")
    for coin in COINS:
        for tf in TIMEFRAMES:
            backfill(conn, coin, tf)

    # Collect initial snapshots of extra data
    log.info("=== INITIAL EXTRA DATA ===")
    collect_funding_rates(conn)
    collect_open_interest(conn)
    collect_ticker_snapshots(conn)

    # Start Polymarket collector after backfill to avoid write contention
    pm_thread = threading.Thread(target=polymarket_collector_loop, daemon=True)
    pm_thread.start()
    log.info("[main] Polymarket collector thread started")

    # Phase 2: Continuous polling
    log.info("=== POLLING PHASE ===")
    last_poll = {tf: 0.0 for tf in TIMEFRAMES}
    last_extra = 0.0

    while True:
        now = time.time()

        # Poll Binance candles
        for tf, interval in POLL_INTERVALS.items():
            if now - last_poll[tf] >= interval:
                for coin in COINS:
                    try:
                        poll_binance(conn, coin, tf)
                    except Exception as e:
                        log.error(f"Error polling {coin}/{tf}: {e}")
                last_poll[tf] = now

        # Collect extra data every 5 minutes
        if now - last_extra >= 300:
            try:
                collect_funding_rates(conn)
                collect_open_interest(conn)
                collect_ticker_snapshots(conn)
            except Exception as e:
                log.error(f"Error collecting extra data: {e}")
            last_extra = now

        time.sleep(10)


if __name__ == "__main__":
    run_collector()
