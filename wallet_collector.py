#!/usr/bin/env python3
"""
Wallet & Leaderboard Collector — Strategy Intelligence

Tracks the top-20 Polymarket crypto traders (weekly profit leaderboard) plus
any seed wallets.  Designed to collect *everything* needed to reverse-engineer
and copy trading strategies.

Per-wallet data collected
─────────────────────────
  • Full trade history (backfilled on first encounter, then incremental)
  • Open + closed position snapshots
  • Daily P&L snapshots

Per-trade enrichment
────────────────────
  • Binance spot price at collection time
  • Market Up / Down prices at collection time
  • Market timeframe (5m / 15m / 1h / 1d)
  • Seconds until market expiry when trade was placed
  • Trade count for this wallet × market (position sizing pattern)

Leaderboard
───────────
  • Hourly snapshot of the top-20 ranking (rank, P&L, volume, username)

All data is stored in the shared candles.db SQLite file.
"""

import json
import re
import sqlite3
import sys
import time
import logging
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

# ── APIs ─────────────────────────────────────────────────────────────────────
DATA_API         = "https://data-api.polymarket.com"
GAMMA_API        = "https://gamma-api.polymarket.com"
BINANCE_PRICE    = "https://api.binance.com/api/v3/ticker/price"
LEADERBOARD_URL  = "https://polymarket.com/leaderboard/crypto/weekly/profit"

# ── Always-tracked wallets (in addition to leaderboard) ──────────────────────
SEED_WALLETS: list[str] = [
    # add any extra addresses here
]

# ── Settings ─────────────────────────────────────────────────────────────────
LEADERBOARD_TOP_N    = 20
LEADERBOARD_REFRESH  = 3600   # seconds — refresh leaderboard ranking
ACTIVITY_POLL        = 60     # seconds — poll for new trades
POSITION_POLL        = 300    # seconds — snapshot positions
PNL_SNAPSHOT_INTERVAL= 3600   # seconds — daily P&L snapshot
BACKFILL_PAGE        = 500    # trades per page when backfilling
INTER_WALLET_SLEEP   = 0.4    # seconds between wallets to respect rate limits

# ── Coin keyword → (short name, Binance symbol) ──────────────────────────────
COIN_MAP = {
    "bitcoin":  ("BTC", "BTCUSDT"),
    "ethereum": ("ETH", "ETHUSDT"),
    "xrp":      ("XRP", "XRPUSDT"),
    "solana":   ("SOL", "SOLUSDT"),
}


# ═══════════════════════════════════════════════════════════════════════════════
#  Database
# ═══════════════════════════════════════════════════════════════════════════════

def get_db() -> sqlite3.Connection:
    DB_PATH.parent.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(str(DB_PATH), timeout=120)
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA synchronous=NORMAL")
    conn.execute("PRAGMA busy_timeout=120000")

    # ── Schema migrations for tracked_wallets (older installs) ───────────────
    conn.execute("""
        CREATE TABLE IF NOT EXISTS tracked_wallets (
            wallet_address  TEXT PRIMARY KEY,
            username        TEXT,
            source          TEXT,
            first_seen_ts   INTEGER,
            last_seen_ts    INTEGER,
            backfill_done   INTEGER DEFAULT 0,
            total_trades    INTEGER DEFAULT 0,
            last_trade_ts   INTEGER DEFAULT 0
        )
    """)
    existing_tw = {r[1] for r in conn.execute("PRAGMA table_info(tracked_wallets)")}
    for col, defn in [
        ("backfill_done", "INTEGER DEFAULT 0"),
        ("total_trades",  "INTEGER DEFAULT 0"),
        ("last_trade_ts", "INTEGER DEFAULT 0"),
    ]:
        if col not in existing_tw:
            conn.execute(f"ALTER TABLE tracked_wallets ADD COLUMN {col} {defn}")

    # ── Leaderboard snapshots ────────────────────────────────────────────────
    conn.execute("""
        CREATE TABLE IF NOT EXISTS leaderboard_snapshots (
            id             INTEGER PRIMARY KEY AUTOINCREMENT,
            snapshot_ts    INTEGER NOT NULL,
            rank           INTEGER NOT NULL,
            wallet_address TEXT NOT NULL,
            username       TEXT,
            pnl            REAL,
            volume         REAL,
            realized       REAL,
            unrealized     REAL
        )
    """)
    conn.execute("CREATE INDEX IF NOT EXISTS idx_lb_ts ON leaderboard_snapshots(snapshot_ts)")

    # tracked_wallets already created above with migration

    # ── Individual trades ────────────────────────────────────────────────────
    conn.execute("""
        CREATE TABLE IF NOT EXISTS wallet_trades (
            tx_hash           TEXT PRIMARY KEY,
            wallet_address    TEXT NOT NULL,
            trade_ts          INTEGER NOT NULL,    -- Unix ms (from Polymarket API)
            condition_id      TEXT NOT NULL,
            market_title      TEXT,
            coin              TEXT,                -- BTC / ETH / XRP / SOL
            timeframe         TEXT,                -- 5m / 15m / 1h / 1d
            outcome           TEXT,                -- Up | Down
            side              TEXT,                -- BUY | SELL
            price             REAL,                -- outcome token price 0-1
            size              REAL,                -- outcome tokens bought/sold
            usdc_size         REAL,                -- USDC amount
            outcome_index     INTEGER,             -- 0=Up/Yes  1=Down/No
            asset_id          TEXT,                -- token asset ID
            market_start_ts   INTEGER,             -- market start Unix ms (from slug)
            market_end_ts     INTEGER,             -- market expiry Unix ms (start + duration)
            secs_to_expiry    INTEGER,             -- seconds from trade to expiry
            wallet_trade_seq  INTEGER,             -- this wallet's Nth trade in this market
            -- enrichment at collection time
            collected_ts      INTEGER,
            binance_price     REAL,
            up_price          REAL,
            down_price        REAL
        )
    """)
    conn.execute("CREATE INDEX IF NOT EXISTS idx_wt_wallet_ts  ON wallet_trades(wallet_address, trade_ts)")
    conn.execute("CREATE INDEX IF NOT EXISTS idx_wt_condition  ON wallet_trades(condition_id)")
    conn.execute("CREATE INDEX IF NOT EXISTS idx_wt_outcome    ON wallet_trades(wallet_address, outcome, side)")
    # Add new columns to wallet_trades if upgrading from older schema
    existing = {r[1] for r in conn.execute("PRAGMA table_info(wallet_trades)")}
    for col, defn in [
        ("timeframe",        "TEXT"),
        ("market_start_ts",  "INTEGER"),
        ("market_end_ts",    "INTEGER"),
        ("secs_to_expiry",   "INTEGER"),
        ("wallet_trade_seq", "INTEGER"),
    ]:
        if col not in existing:
            conn.execute(f"ALTER TABLE wallet_trades ADD COLUMN {col} {defn}")
    conn.execute("CREATE INDEX IF NOT EXISTS idx_wt_coin_tf ON wallet_trades(coin, timeframe, trade_ts)")

    # ── Position snapshots ───────────────────────────────────────────────────
    conn.execute("""
        CREATE TABLE IF NOT EXISTS wallet_positions (
            id              INTEGER PRIMARY KEY AUTOINCREMENT,
            snapshot_ts     INTEGER NOT NULL,
            wallet_address  TEXT NOT NULL,
            condition_id    TEXT NOT NULL,
            market_title    TEXT,
            coin            TEXT,
            timeframe       TEXT,
            outcome         TEXT,
            size            REAL,
            avg_price       REAL,
            cur_price       REAL,
            cash_pnl        REAL,
            percent_pnl     REAL,
            initial_value   REAL,
            current_value   REAL,
            total_bought    REAL,
            realized_pnl    REAL,
            end_date        TEXT,
            market_start_ts INTEGER,
            market_end_ts   INTEGER,
            secs_to_expiry  INTEGER,               -- seconds until this market closes
            binance_price   REAL,
            up_price        REAL,
            down_price      REAL
        )
    """)
    conn.execute("CREATE INDEX IF NOT EXISTS idx_wp_wallet_ts ON wallet_positions(wallet_address, snapshot_ts)")
    conn.execute("CREATE INDEX IF NOT EXISTS idx_wp_condition ON wallet_positions(condition_id)")
    existing_wp = {r[1] for r in conn.execute("PRAGMA table_info(wallet_positions)")}
    for col, defn in [
        ("timeframe",       "TEXT"),
        ("market_start_ts", "INTEGER"),
        ("market_end_ts",   "INTEGER"),
        ("secs_to_expiry",  "INTEGER"),
    ]:
        if col not in existing_wp:
            conn.execute(f"ALTER TABLE wallet_positions ADD COLUMN {col} {defn}")

    # ── Per-wallet P&L snapshots (hourly) ────────────────────────────────────
    conn.execute("""
        CREATE TABLE IF NOT EXISTS wallet_pnl_snapshots (
            id              INTEGER PRIMARY KEY AUTOINCREMENT,
            snapshot_ts     INTEGER NOT NULL,
            wallet_address  TEXT NOT NULL,
            username        TEXT,
            total_value     REAL,    -- sum of currentValue across all positions
            total_cash_pnl  REAL,    -- sum of cashPnl
            open_positions  INTEGER, -- number of open markets
            total_size      REAL     -- total outcome tokens held
        )
    """)
    conn.execute("CREATE INDEX IF NOT EXISTS idx_pnl_wallet_ts ON wallet_pnl_snapshots(wallet_address, snapshot_ts)")

    conn.commit()
    return conn


# ═══════════════════════════════════════════════════════════════════════════════
#  HTTP helper
# ═══════════════════════════════════════════════════════════════════════════════

def _req(url: str, params: dict | None = None, timeout: int = 15):
    for attempt in range(4):
        try:
            resp = requests.get(url, params=params or {}, timeout=timeout)
            if resp.status_code == 429:
                wait = int(resp.headers.get("Retry-After", 30))
                log.warning(f"Rate limited — waiting {wait}s")
                time.sleep(wait)
                continue
            resp.raise_for_status()
            return resp.json()
        except requests.RequestException as e:
            log.warning(f"Request {url} failed (attempt {attempt + 1}): {e}")
            time.sleep(2 ** attempt)
    return None


# ═══════════════════════════════════════════════════════════════════════════════
#  Price / enrichment helpers
# ═══════════════════════════════════════════════════════════════════════════════

# Module-level price caches to avoid redundant API calls within one poll cycle
_binance_cache: dict[str, tuple[float, float]] = {}   # symbol → (price, ts)
_market_cache:  dict[str, tuple]               = {}   # slug → (up, down, ts)
_CACHE_TTL = 30  # seconds


def get_binance_price(symbol: str) -> float | None:
    now = time.time()
    cached = _binance_cache.get(symbol)
    if cached and now - cached[1] < _CACHE_TTL:
        return cached[0]
    data = _req(BINANCE_PRICE, {"symbol": symbol})
    if data:
        try:
            price = float(data["price"])
            _binance_cache[symbol] = (price, now)
            return price
        except (KeyError, ValueError):
            pass
    return None


def get_market_prices(slug: str) -> tuple[float | None, float | None]:
    """Return (up_price, down_price) from the Gamma API outcomePrices field."""
    now = time.time()
    cached = _market_cache.get(slug)
    if cached and now - cached[2] < _CACHE_TTL:
        return cached[0], cached[1]
    data = _req(f"{GAMMA_API}/markets", {"slug": slug})
    if not isinstance(data, list) or not data:
        return None, None
    mkt = data[0]
    try:
        prices   = json.loads(mkt.get("outcomePrices") or "[]")
        outcomes = json.loads(mkt.get("outcomes")      or "[]")
        if not prices or not outcomes:
            return None, None
        pm = {o.lower(): float(p) for o, p in zip(outcomes, prices)}
        up   = pm.get("up")   or pm.get("yes")
        down = pm.get("down") or pm.get("no")
        _market_cache[slug] = (up, down, now)
        return up, down
    except Exception:
        return None, None


def detect_coin(title: str) -> tuple[str | None, str | None]:
    t = title.lower()
    for kw, (coin, sym) in COIN_MAP.items():
        if kw in t:
            return coin, sym
    return None, None


_DURATION_SECS = {"5m": 300, "15m": 900, "1h": 3600, "1d": 86400}


def extract_slug_meta(slug: str) -> tuple[str | None, int | None, int | None]:
    """
    Parse slug like 'btc-updown-5m-1773060900'.
    The timestamp in the slug is the market START time, not end.
    Returns (timeframe, start_ts_ms, end_ts_ms).
    """
    tf    = None
    start = None
    end   = None
    m = re.search(r'-(5m|15m|1h|1d)-', slug)
    if m:
        tf = m.group(1)
    m = re.search(r'-(\d{10})$', slug)
    if m:
        start = int(m.group(1)) * 1000
    if start is not None and tf is not None:
        end = start + _DURATION_SECS[tf] * 1000
    return tf, start, end


# ═══════════════════════════════════════════════════════════════════════════════
#  Leaderboard
# ═══════════════════════════════════════════════════════════════════════════════

def fetch_leaderboard() -> list[dict]:
    """Fetch top-N crypto traders from Polymarket's SSG leaderboard page."""
    try:
        resp = requests.get(LEADERBOARD_URL, timeout=20)
        resp.raise_for_status()
        m = re.search(r'"buildId":"([^"]+)"', resp.text)
        if not m:
            log.warning("[lb] buildId not found in leaderboard page")
            return []
        build_id = m.group(1)
        ssg_url = (
            f"https://polymarket.com/_next/data/{build_id}"
            "/en/leaderboard/crypto/weekly/profit.json"
        )
        data = _req(ssg_url, timeout=20)
        if not data:
            return []
        for q in data.get("pageProps", {}).get("dehydratedState", {}).get("queries", []):
            entries = q.get("state", {}).get("data", [])
            if isinstance(entries, list) and entries and "proxyWallet" in entries[0]:
                return entries[:LEADERBOARD_TOP_N]
        log.warning("[lb] Leaderboard entries not found in SSG data")
        return []
    except Exception as e:
        log.error(f"[lb] Fetch error: {e}")
        return []


def refresh_leaderboard(conn: sqlite3.Connection, tracked: dict) -> dict:
    """
    Fetch leaderboard, save snapshot, upsert tracked_wallets.
    Returns updated {wallet_address: username} dict.
    """
    log.info("[lb] Refreshing top-20 leaderboard...")
    entries = fetch_leaderboard()
    if not entries:
        log.warning("[lb] No entries — keeping existing tracked list")
        return tracked

    ts = int(time.time() * 1000)

    conn.executemany("""
        INSERT INTO leaderboard_snapshots
        (snapshot_ts, rank, wallet_address, username, pnl, volume, realized, unrealized)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?)
    """, [
        (
            ts,
            e.get("rank"),
            (e.get("proxyWallet") or "").lower(),
            e.get("name") or e.get("pseudonym"),
            float(e.get("pnl") or 0),
            float(e.get("volume") or e.get("amount") or 0),
            float(e.get("realized") or 0),
            float(e.get("unrealized") or 0),
        )
        for e in entries if e.get("proxyWallet")
    ])

    for e in entries:
        wallet   = (e.get("proxyWallet") or "").lower()
        username = e.get("name") or e.get("pseudonym")
        if not wallet:
            continue
        conn.execute("""
            INSERT INTO tracked_wallets
                (wallet_address, username, source, first_seen_ts, last_seen_ts)
            VALUES (?, ?, 'leaderboard', ?, ?)
            ON CONFLICT(wallet_address) DO UPDATE SET
                username     = excluded.username,
                last_seen_ts = excluded.last_seen_ts
        """, (wallet, username, ts, ts))
        tracked[wallet] = username

    for wallet in SEED_WALLETS:
        w = wallet.lower()
        if w not in tracked:
            conn.execute("""
                INSERT INTO tracked_wallets
                    (wallet_address, username, source, first_seen_ts, last_seen_ts)
                VALUES (?, NULL, 'seed', ?, ?)
                ON CONFLICT(wallet_address) DO UPDATE SET last_seen_ts = excluded.last_seen_ts
            """, (w, ts, ts))
            tracked[w] = None

    conn.commit()
    log.info(f"[lb] {len(tracked)} wallets tracked")
    return tracked


# ═══════════════════════════════════════════════════════════════════════════════
#  Trade helpers
# ═══════════════════════════════════════════════════════════════════════════════

def _enrich_and_build_rows(raw_trades: list[dict], wallet: str) -> list[tuple]:
    """
    Convert raw API trade dicts into DB row tuples with full enrichment.
    Batches Binance / market-price calls per unique (symbol, slug).
    """
    collected_ts = int(time.time() * 1000)
    price_cache: dict[tuple, tuple] = {}
    # market_trade_counts[condition_id] → count so far (for sequence numbering)
    seq_counter: dict[str, int] = {}
    rows = []

    # Prices from Binance/Gamma are only meaningful for real-time collection.
    # For backfilled trades (older than 5 min), these would be current prices,
    # not prices at trade time — so we set them to None.
    REALTIME_THRESHOLD_MS = 5 * 60 * 1000

    for t in raw_trades:
        tx = t.get("transactionHash") or ""
        if not tx or t.get("type") != "TRADE":
            continue

        title        = t.get("title") or ""
        condition_id = t.get("conditionId") or ""
        slug         = t.get("slug") or t.get("eventSlug") or ""
        coin, symbol = detect_coin(title)
        tf, start_ts, end_ts = extract_slug_meta(slug)
        trade_ts     = int(t.get("timestamp") or 0) * 1000
        secs_to_exp  = int((end_ts - trade_ts) / 1000) if end_ts and trade_ts else None

        is_realtime = (collected_ts - trade_ts) < REALTIME_THRESHOLD_MS if trade_ts else False

        if is_realtime:
            key = (symbol, slug)
            if key not in price_cache:
                bp   = get_binance_price(symbol) if symbol else None
                u, d = get_market_prices(slug)   if slug   else (None, None)
                price_cache[key] = (bp, u, d)
            bp, up, down = price_cache[key]
        else:
            # Historical trade — current prices are not meaningful
            bp, up, down = None, None, None

        seq_counter[condition_id] = seq_counter.get(condition_id, 0) + 1

        rows.append((
            tx,
            wallet,
            trade_ts,
            condition_id,
            title,
            coin,
            tf,
            t.get("outcome"),
            t.get("side"),
            float(t.get("price")    or 0),
            float(t.get("size")     or 0),
            float(t.get("usdcSize") or 0),
            t.get("outcomeIndex"),
            t.get("asset"),
            start_ts,
            end_ts,
            secs_to_exp,
            seq_counter[condition_id],
            collected_ts,
            bp,
            up,
            down,
        ))

    return rows


def _insert_trades(conn: sqlite3.Connection, rows: list[tuple]) -> int:
    if not rows:
        return 0
    conn.executemany("""
        INSERT OR IGNORE INTO wallet_trades
        (tx_hash, wallet_address, trade_ts, condition_id, market_title, coin,
         timeframe, outcome, side, price, size, usdc_size, outcome_index, asset_id,
         market_start_ts, market_end_ts, secs_to_expiry, wallet_trade_seq,
         collected_ts, binance_price, up_price, down_price)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
    """, rows)
    conn.commit()
    return len(rows)


# ═══════════════════════════════════════════════════════════════════════════════
#  Full history backfill (runs once per wallet)
# ═══════════════════════════════════════════════════════════════════════════════

def backfill_wallet(conn: sqlite3.Connection, wallet: str, username: str | None):
    """
    Paginate through ALL historical activity for wallet and store every trade.
    Called once for each new wallet; sets backfill_done=1 when complete.
    """
    label  = username or wallet[:14] + "..."
    log.info(f"[backfill] {label}: starting full history backfill...")
    offset = 0
    total  = 0

    while True:
        raw = _req(
            f"{DATA_API}/activity",
            {"user": wallet, "limit": BACKFILL_PAGE, "offset": offset},
            timeout=20,
        )
        if not isinstance(raw, list) or not raw:
            break

        trades = [t for t in raw if t.get("type") == "TRADE"]
        if trades:
            rows    = _enrich_and_build_rows(trades, wallet)
            inserted = _insert_trades(conn, rows)
            total   += inserted

        if len(raw) < BACKFILL_PAGE:
            break
        offset += BACKFILL_PAGE
        time.sleep(0.3)

    conn.execute("""
        UPDATE tracked_wallets
        SET backfill_done = 1, total_trades = ?
        WHERE wallet_address = ?
    """, (total, wallet))
    conn.commit()
    log.info(f"[backfill] {label}: complete — {total} trades stored")
    return total


# ═══════════════════════════════════════════════════════════════════════════════
#  Incremental trade polling
# ═══════════════════════════════════════════════════════════════════════════════

def poll_new_trades(conn: sqlite3.Connection, wallet: str) -> int:
    """
    Fetch trades newer than the latest stored trade_ts for this wallet.
    Returns number of new rows inserted.
    """
    row = conn.execute(
        "SELECT MAX(trade_ts) FROM wallet_trades WHERE wallet_address = ?",
        (wallet,),
    ).fetchone()
    since_ms = row[0] or 0

    raw = _req(f"{DATA_API}/activity", {"user": wallet, "limit": 500})
    if not isinstance(raw, list):
        return 0

    new = [
        t for t in raw
        if t.get("type") == "TRADE"
        and int(t.get("timestamp") or 0) * 1000 > since_ms
        and t.get("transactionHash")
    ]
    if not new:
        return 0

    rows     = _enrich_and_build_rows(new, wallet)
    inserted = _insert_trades(conn, rows)

    if inserted:
        conn.execute("""
            UPDATE tracked_wallets
            SET total_trades  = total_trades + ?,
                last_trade_ts = MAX(last_trade_ts, ?)
            WHERE wallet_address = ?
        """, (inserted, max(r[2] for r in rows), wallet))
        conn.commit()

    return inserted


# ═══════════════════════════════════════════════════════════════════════════════
#  Position snapshots
# ═══════════════════════════════════════════════════════════════════════════════

def snapshot_positions(conn: sqlite3.Connection, wallet: str) -> int:
    """Snapshot all open positions, with enrichment. Returns row count."""
    raw = _req(f"{DATA_API}/positions", {"user": wallet, "limit": 500})
    if not isinstance(raw, list) or not raw:
        return 0

    ts    = int(time.time() * 1000)
    now_s = ts / 1000
    price_cache: dict[tuple, tuple] = {}
    rows  = []

    for pos in raw:
        title        = pos.get("title") or ""
        condition_id = pos.get("conditionId") or ""
        slug         = pos.get("slug") or pos.get("eventSlug") or ""
        coin, symbol = detect_coin(title)
        tf, start_ts, end_ts = extract_slug_meta(slug)
        secs_left    = int(end_ts / 1000 - now_s) if end_ts else None

        key = (symbol, slug)
        if key not in price_cache:
            bp   = get_binance_price(symbol) if symbol else None
            u, d = get_market_prices(slug)   if slug   else (None, None)
            price_cache[key] = (bp, u, d)
        bp, up, down = price_cache[key]

        rows.append((
            ts, wallet,
            condition_id, title, coin, tf,
            pos.get("outcome"),
            float(pos.get("size")          or 0),
            float(pos.get("avgPrice")      or 0),
            float(pos.get("curPrice")      or 0),
            float(pos.get("cashPnl")       or 0),
            float(pos.get("percentPnl")    or 0),
            float(pos.get("initialValue")  or 0),
            float(pos.get("currentValue")  or 0),
            float(pos.get("totalBought")   or 0),
            float(pos.get("realizedPnl")   or 0),
            pos.get("endDate"),
            start_ts,
            end_ts,
            secs_left,
            bp, up, down,
        ))

    if rows:
        conn.executemany("""
            INSERT INTO wallet_positions
            (snapshot_ts, wallet_address, condition_id, market_title, coin, timeframe,
             outcome, size, avg_price, cur_price, cash_pnl, percent_pnl,
             initial_value, current_value, total_bought, realized_pnl, end_date,
             market_start_ts, market_end_ts, secs_to_expiry,
             binance_price, up_price, down_price)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """, rows)
        conn.commit()

    return len(rows)


# ═══════════════════════════════════════════════════════════════════════════════
#  Hourly P&L snapshot
# ═══════════════════════════════════════════════════════════════════════════════

def snapshot_pnl(conn: sqlite3.Connection, wallet: str, username: str | None):
    """Aggregate open positions into a single P&L snapshot row."""
    raw = _req(f"{DATA_API}/positions", {"user": wallet, "limit": 500})
    if not isinstance(raw, list):
        return

    ts          = int(time.time() * 1000)
    total_val   = sum(float(p.get("currentValue") or 0) for p in raw)
    total_pnl   = sum(float(p.get("cashPnl")      or 0) for p in raw)
    total_size  = sum(float(p.get("size")          or 0) for p in raw)
    open_count  = len(raw)

    conn.execute("""
        INSERT INTO wallet_pnl_snapshots
        (snapshot_ts, wallet_address, username, total_value, total_cash_pnl,
         open_positions, total_size)
        VALUES (?, ?, ?, ?, ?, ?, ?)
    """, (ts, wallet, username, total_val, total_pnl, open_count, total_size))
    conn.commit()


# ═══════════════════════════════════════════════════════════════════════════════
#  Backfill worker thread (runs backfills without blocking poll cycle)
# ═══════════════════════════════════════════════════════════════════════════════

_backfill_queue: list[tuple[str, str | None]] = []
_backfill_lock  = threading.Lock()


def backfill_worker(conn_factory):
    """Background thread: drain the backfill queue one wallet at a time."""
    while True:
        with _backfill_lock:
            item = _backfill_queue.pop(0) if _backfill_queue else None
        if item:
            wallet, username = item
            try:
                conn = conn_factory()
                backfill_wallet(conn, wallet, username)
            except Exception as e:
                log.error(f"[backfill] Error for {wallet[:14]}...: {e}")
        else:
            time.sleep(2)


def queue_backfill(wallet: str, username: str | None):
    with _backfill_lock:
        if not any(w == wallet for w, _ in _backfill_queue):
            _backfill_queue.append((wallet, username))


# ═══════════════════════════════════════════════════════════════════════════════
#  Main loop
# ═══════════════════════════════════════════════════════════════════════════════

def run_wallet_collector():
    log.info("=" * 60)
    log.info("  Wallet & Leaderboard Collector — Strategy Intelligence")
    log.info("=" * 60)

    conn    = get_db()
    tracked: dict[str, str | None] = {}  # wallet → username

    # ── Step 1: load leaderboard immediately ─────────────────────────────────
    tracked = refresh_leaderboard(conn, tracked)

    # ── Step 2: queue full-history backfills for wallets not yet done ─────────
    for wallet, username in list(tracked.items()):
        row = conn.execute(
            "SELECT backfill_done FROM tracked_wallets WHERE wallet_address = ?",
            (wallet,),
        ).fetchone()
        if not row or not row[0]:
            queue_backfill(wallet, username)

    # ── Step 3: start background backfill thread ──────────────────────────────
    bf_thread = threading.Thread(
        target=backfill_worker,
        args=(get_db,),
        daemon=True,
    )
    bf_thread.start()
    log.info("[wc] Background backfill thread started")

    last_leaderboard = time.time()
    last_positions   = 0.0
    last_pnl         = 0.0

    # ── Step 4: main poll loop ────────────────────────────────────────────────
    while True:
        now = time.time()

        # Leaderboard refresh
        if now - last_leaderboard >= LEADERBOARD_REFRESH:
            try:
                old_wallets = set(tracked.keys())
                tracked     = refresh_leaderboard(conn, tracked)
                for w, u in tracked.items():
                    if w not in old_wallets:
                        log.info(f"[wc] New wallet on leaderboard: {u or w[:14]}...")
                        queue_backfill(w, u)
            except Exception as e:
                log.error(f"[wc] Leaderboard error: {e}")
            last_leaderboard = now

        # New trades — all wallets
        new_total = 0
        for wallet, username in list(tracked.items()):
            label = username or wallet[:14] + "..."
            try:
                n = poll_new_trades(conn, wallet)
                if n:
                    log.info(f"[trade] {label}: +{n} new trades")
                    new_total += n
            except Exception as e:
                log.warning(f"[trade] Error for {label}: {e}")
            time.sleep(INTER_WALLET_SLEEP)

        # Position snapshots every 5 min
        if now - last_positions >= POSITION_POLL:
            for wallet, username in list(tracked.items()):
                label = username or wallet[:14] + "..."
                try:
                    n = snapshot_positions(conn, wallet)
                    if n:
                        log.info(f"[pos] {label}: {n} positions snapshotted")
                except Exception as e:
                    log.warning(f"[pos] Error for {label}: {e}")
                time.sleep(INTER_WALLET_SLEEP)
            last_positions = now

        # Hourly P&L snapshots
        if now - last_pnl >= PNL_SNAPSHOT_INTERVAL:
            for wallet, username in list(tracked.items()):
                try:
                    snapshot_pnl(conn, wallet, username)
                except Exception as e:
                    log.warning(f"[pnl] Error for {wallet[:14]}...: {e}")
                time.sleep(INTER_WALLET_SLEEP)
            log.info(f"[pnl] Snapshots done for {len(tracked)} wallets")
            last_pnl = now

        time.sleep(ACTIVITY_POLL)


if __name__ == "__main__":
    run_wallet_collector()
