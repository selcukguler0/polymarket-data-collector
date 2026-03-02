#!/usr/bin/env python3
"""
Dashboard for browsing and downloading collected backtest data.
Serves Binance candles, funding rates, OI, and Polymarket book snapshots.
Run: python dashboard.py
"""

import bisect
import csv
import io
import json
import math
import sqlite3
import zipfile
from datetime import datetime, timezone
from pathlib import Path

import numpy as np

from flask import Flask, Response, render_template, request, jsonify

DB_PATH = Path(__file__).parent / "data" / "candles.db"

app = Flask(__name__)


def get_db() -> sqlite3.Connection:
    conn = sqlite3.connect(str(DB_PATH), timeout=60)
    conn.execute("PRAGMA busy_timeout=60000")
    conn.row_factory = sqlite3.Row
    return conn


# ── Backtest Export Constants ──

TIMEFRAME_SECONDS = {"5m": 300, "15m": 900, "1h": 3600, "1d": 86400}
SIGMA_WINDOW_CANDLES = 48  # rolling window of 5m candles for volatility
# 105192 = number of 5-minute intervals per year (365.25 * 24 * 12)
ANNUALIZATION_FACTOR = 105192


# ── Backtest Export Helpers ──

def _parse_end_date_ms(end_date_str: str) -> int:
    """Convert ISO 8601 end_date string to epoch ms."""
    if not end_date_str:
        return 0
    # Handle various ISO formats
    for fmt in ("%Y-%m-%dT%H:%M:%SZ", "%Y-%m-%dT%H:%M:%S", "%Y-%m-%d %H:%M:%S", "%Y-%m-%d"):
        try:
            dt = datetime.strptime(end_date_str, fmt).replace(tzinfo=timezone.utc)
            return int(dt.timestamp() * 1000)
        except ValueError:
            continue
    return 0


def _derive_start_ms(conn: sqlite3.Connection, condition_id: str,
                     end_date_str: str, timeframe: str) -> int:
    """Compute market start time: fallback to earliest snapshot."""
    end_ms = _parse_end_date_ms(end_date_str)
    if end_ms > 0:
        earliest = conn.execute(
            "SELECT MIN(timestamp) FROM pm_book_snapshots WHERE condition_id = ?",
            (condition_id,)
        ).fetchone()[0]
        if earliest:
            return earliest
    # Fallback
    row = conn.execute(
        "SELECT MIN(timestamp) FROM pm_book_snapshots WHERE condition_id = ?",
        (condition_id,)
    ).fetchone()
    return row[0] if row and row[0] else 0


def _precompute_binance_prices(conn: sqlite3.Connection, coin: str,
                                start_ms: int, end_ms: int) -> list[tuple[int, float]]:
    """Load all 5m candle closes as sorted [(open_time, close)] for bisect lookups."""
    rows = conn.execute(
        """SELECT open_time, close FROM candles
           WHERE coin = ? AND timeframe = '5m' AND open_time >= ? AND open_time <= ?
           ORDER BY open_time ASC""",
        (coin, start_ms - 300_000, end_ms)  # pad start by one candle
    ).fetchall()
    return [(r[0], r[1]) for r in rows]


def _lookup_price(candle_series: list[tuple[int, float]], ts_ms: int) -> float | None:
    """Binary search for the candle close at or before timestamp."""
    if not candle_series:
        return None
    # bisect_right finds insertion point; we want the element at or before ts_ms
    idx = bisect.bisect_right(candle_series, (ts_ms,)) - 1
    if idx < 0:
        return candle_series[0][1] if candle_series else None
    return candle_series[idx][1]


def _compute_sigma_series(conn: sqlite3.Connection, coin: str,
                          start_ms: int, end_ms: int) -> list[tuple[int, float]]:
    """Rolling annualized realized vol from 5m candle log returns.

    sigma = stdev(last 48 log_returns) * sqrt(ANNUALIZATION_FACTOR)
    Returns [(open_time, sigma)].
    """
    rows = conn.execute(
        """SELECT open_time, close FROM candles
           WHERE coin = ? AND timeframe = '5m' AND open_time >= ? AND open_time <= ?
           ORDER BY open_time ASC""",
        (coin, start_ms - SIGMA_WINDOW_CANDLES * 300_000, end_ms)
    ).fetchall()

    if len(rows) < 2:
        return []

    closes = np.array([r[1] for r in rows], dtype=np.float64)
    times = [r[0] for r in rows]

    # Compute log returns
    log_returns = np.diff(np.log(closes))

    result = []
    for i in range(SIGMA_WINDOW_CANDLES, len(log_returns) + 1):
        window = log_returns[i - SIGMA_WINDOW_CANDLES:i]
        if len(window) < SIGMA_WINDOW_CANDLES:
            continue
        sigma = float(np.std(window, ddof=1) * math.sqrt(ANNUALIZATION_FACTOR))
        # The sigma corresponds to the candle at index i (after log_returns, so times[i])
        if i < len(times):
            result.append((times[i], sigma))

    return result


def _is_degenerate_book(row: sqlite3.Row) -> bool:
    """Filter snapshots with null or extreme bids/asks."""
    up_bid = row["up_best_bid"]
    up_ask = row["up_best_ask"]
    down_bid = row["down_best_bid"]
    down_ask = row["down_best_ask"]

    if up_bid is None or up_ask is None or down_bid is None or down_ask is None:
        return True
    if up_bid <= 0.02 or up_ask >= 0.98:
        return True
    if down_bid <= 0.02 or down_ask >= 0.98:
        return True
    return False


def _rows_to_csv(columns: list[str], rows: list[list]) -> str:
    """Serialize rows to CSV string."""
    output = io.StringIO()
    writer = csv.writer(output)
    writer.writerow(columns)
    writer.writerows(rows)
    return output.getvalue()


def _rows_to_parquet(columns: list[str], rows: list[list]) -> bytes:
    """Serialize rows to Parquet bytes via pyarrow."""
    import pyarrow as pa
    import pyarrow.parquet as pq

    # Transpose rows to columns
    if not rows:
        arrays = [pa.array([], type=pa.float64()) for _ in columns]
    else:
        col_data = list(zip(*rows)) if rows else [[] for _ in columns]
        arrays = []
        for i, col_name in enumerate(columns):
            data = list(col_data[i]) if i < len(col_data) else []
            # Infer type from first non-None value
            sample = next((v for v in data if v is not None), None)
            if isinstance(sample, int):
                arrays.append(pa.array(data, type=pa.int64()))
            elif isinstance(sample, float):
                arrays.append(pa.array(data, type=pa.float64()))
            else:
                arrays.append(pa.array([str(v) if v is not None else None for v in data], type=pa.string()))

    table = pa.table(dict(zip(columns, arrays)))
    buf = io.BytesIO()
    pq.write_table(table, buf)
    return buf.getvalue()


def _build_prices_file(conn: sqlite3.Connection, condition_id: str, coin: str,
                       timeframe: str, end_date_str: str, fmt: str) -> tuple[str | bytes, str]:
    """Build the prices file for a single market period.

    Returns (content, extension) where content is str for csv or bytes for parquet.
    """
    start_ms = _derive_start_ms(conn, condition_id, end_date_str, timeframe)
    end_ms = _parse_end_date_ms(end_date_str)
    if end_ms == 0:
        end_ms = conn.execute(
            "SELECT MAX(timestamp) FROM pm_book_snapshots WHERE condition_id = ?",
            (condition_id,)
        ).fetchone()[0] or 0

    # Query PM snapshots
    snapshots = conn.execute(
        """SELECT * FROM pm_book_snapshots
           WHERE condition_id = ? ORDER BY timestamp ASC""",
        (condition_id,)
    ).fetchall()

    if not snapshots:
        raise ValueError(f"No snapshots for condition_id={condition_id}")

    # Precompute Binance prices and sigma
    candle_series = _precompute_binance_prices(conn, coin, start_ms, end_ms)
    sigma_series = _compute_sigma_series(conn, coin, start_ms, end_ms)

    # price_open: Binance price at market start
    price_open = _lookup_price(candle_series, start_ms)

    columns = [
        "timestamp", "binance_price", "price_open",
        "fv_up", "fv_down", "sigma", "remaining_secs",
        "best_bid_up", "best_ask_up", "best_bid_down", "best_ask_down",
        "up_bid_depth", "up_ask_depth", "down_bid_depth", "down_ask_depth",
        "up_book_json", "down_book_json",
        "combined_mid", "up_mid", "down_mid",
        "condition_id", "question", "end_date", "timeframe", "asset",
    ]

    rows = []
    last_ts = -1
    for snap in snapshots:
        # Filter degenerate books
        if _is_degenerate_book(snap):
            continue

        ts = snap["timestamp"]
        # Enforce monotonic timestamps
        if ts <= last_ts:
            continue
        last_ts = ts

        binance_price = _lookup_price(candle_series, ts)
        sigma = _lookup_price(sigma_series, ts)  # same bisect logic works
        remaining_secs = (end_ms - ts) / 1000.0 if end_ms > 0 else 0

        rows.append([
            ts,
            binance_price,
            price_open,
            snap["up_mid"],       # fv_up
            snap["down_mid"],     # fv_down
            sigma,
            remaining_secs,
            snap["up_best_bid"],  # best_bid_up
            snap["up_best_ask"],  # best_ask_up
            snap["down_best_bid"],  # best_bid_down
            snap["down_best_ask"],  # best_ask_down
            snap["up_bid_depth"],
            snap["up_ask_depth"],
            snap["down_bid_depth"],
            snap["down_ask_depth"],
            snap["up_book_json"],
            snap["down_book_json"],
            snap["combined_mid"],
            snap["up_mid"],
            snap["down_mid"],
            condition_id,
            snap["question"],
            end_date_str,
            timeframe,
            coin,
        ])

    if not rows:
        raise ValueError(f"No valid rows after filtering for condition_id={condition_id}")

    if fmt == "parquet":
        return _rows_to_parquet(columns, rows), "parquet"
    return _rows_to_csv(columns, rows), "csv"


def _build_period_result_file(condition_id: str, coin: str, timeframe: str,
                              outcome: str, price_open: float | None,
                              start_ms: int, end_ms: int, fmt: str) -> tuple[str | bytes, str]:
    """Build period_result file: single-row with period metadata."""
    duration_mins = (end_ms - start_ms) / 60_000.0 if end_ms > start_ms else 0
    period_name = f"{coin}_{timeframe}_{condition_id[:8]}"

    columns = ["period_name", "condition_id", "duration_mins", "price_open", "result"]
    rows = [[period_name, condition_id, round(duration_mins, 2), price_open, outcome]]

    if fmt == "parquet":
        return _rows_to_parquet(columns, rows), "parquet"
    return _rows_to_csv(columns, rows), "csv"


# ── Pages ──

@app.route("/")
def index():
    return render_template("index.html")


# ── API: Stats ──

@app.route("/api/stats")
def stats():
    conn = get_db()

    # Binance candle stats
    candle_rows = conn.execute("""
        SELECT coin, timeframe, COUNT(*) as count,
               MIN(open_time) as earliest, MAX(open_time) as latest
        FROM candles GROUP BY coin, timeframe ORDER BY coin, timeframe
    """).fetchall()

    # Polymarket snapshot stats
    pm_rows = conn.execute("""
        SELECT coin, timeframe, COUNT(*) as count,
               MIN(timestamp) as earliest, MAX(timestamp) as latest,
               COUNT(DISTINCT condition_id) as markets
        FROM pm_book_snapshots GROUP BY coin, timeframe ORDER BY coin, timeframe
    """).fetchall()

    # Resolved markets count
    resolved = conn.execute("SELECT COUNT(*) FROM pm_market_outcomes").fetchone()[0]

    # Extra data counts
    funding_count = conn.execute("SELECT COUNT(*) FROM funding_rates").fetchone()[0]
    oi_count = conn.execute("SELECT COUNT(*) FROM open_interest").fetchone()[0]
    ticker_count = conn.execute("SELECT COUNT(*) FROM ticker_snapshots").fetchone()[0]

    conn.close()

    def fmt_ts(ms):
        return datetime.fromtimestamp(ms / 1000, tz=timezone.utc).strftime("%Y-%m-%d %H:%M")

    candles = [{
        "coin": r["coin"], "timeframe": r["timeframe"], "count": r["count"],
        "earliest": fmt_ts(r["earliest"]), "latest": fmt_ts(r["latest"]),
    } for r in candle_rows]

    pm = [{
        "coin": r["coin"], "timeframe": r["timeframe"], "count": r["count"],
        "markets": r["markets"],
        "earliest": fmt_ts(r["earliest"]), "latest": fmt_ts(r["latest"]),
    } for r in pm_rows]

    return jsonify({
        "candles": candles,
        "polymarket": pm,
        "resolved_markets": resolved,
        "funding_snapshots": funding_count,
        "oi_snapshots": oi_count,
        "ticker_snapshots": ticker_count,
    })


# ── API: Preview ──

@app.route("/api/preview")
def preview():
    dataset = request.args.get("dataset", "candles")
    coin = request.args.get("coin", "BTC")
    timeframe = request.args.get("timeframe", "5m")
    start = request.args.get("start")
    end = request.args.get("end")

    conn = get_db()

    if dataset == "candles":
        query = "SELECT * FROM candles WHERE coin = ? AND timeframe = ?"
        params: list = [coin, timeframe]
        ts_col = "open_time"
    elif dataset == "polymarket":
        query = "SELECT * FROM pm_book_snapshots WHERE coin = ? AND timeframe = ?"
        params = [coin, timeframe]
        ts_col = "timestamp"
    elif dataset == "funding":
        query = "SELECT * FROM funding_rates WHERE coin = ?"
        params = [coin]
        ts_col = "timestamp"
    elif dataset == "oi":
        query = "SELECT * FROM open_interest WHERE coin = ?"
        params = [coin]
        ts_col = "timestamp"
    elif dataset == "ticker":
        query = "SELECT * FROM ticker_snapshots WHERE coin = ?"
        params = [coin]
        ts_col = "timestamp"
    elif dataset == "outcomes":
        query = "SELECT * FROM pm_market_outcomes WHERE coin = ?"
        params = [coin]
        ts_col = None
    else:
        return jsonify([])

    if ts_col and start:
        start_ms = int(datetime.strptime(start, "%Y-%m-%d").replace(tzinfo=timezone.utc).timestamp() * 1000)
        query += f" AND {ts_col} >= ?"
        params.append(start_ms)
    if ts_col and end:
        end_ms = int(datetime.strptime(end, "%Y-%m-%d").replace(tzinfo=timezone.utc).timestamp() * 1000)
        query += f" AND {ts_col} <= ?"
        params.append(end_ms)

    if ts_col:
        query += f" ORDER BY {ts_col} DESC LIMIT 50"
    else:
        query += " LIMIT 50"

    rows = conn.execute(query, params).fetchall()
    conn.close()

    result = [dict(r) for r in rows]

    # Format timestamps for display
    for r in result:
        for key in ("open_time", "close_time", "timestamp"):
            if key in r and r[key] and isinstance(r[key], (int, float)):
                r[key + "_fmt"] = datetime.fromtimestamp(r[key] / 1000, tz=timezone.utc).strftime("%Y-%m-%d %H:%M:%S")

    return jsonify(result)


# ── Downloads ──

def _build_query(table: str, coin: str | None, timeframe: str | None, start: str | None, end: str | None, ts_col: str):
    query = f"SELECT * FROM {table} WHERE 1=1"
    params: list = []
    if coin:
        query += " AND coin = ?"
        params.append(coin)
    if timeframe and timeframe != "all":
        query += " AND timeframe = ?"
        params.append(timeframe)
    if start:
        start_ms = int(datetime.strptime(start, "%Y-%m-%d").replace(tzinfo=timezone.utc).timestamp() * 1000)
        query += f" AND {ts_col} >= ?"
        params.append(start_ms)
    if end:
        end_ms = int(datetime.strptime(end, "%Y-%m-%d").replace(tzinfo=timezone.utc).timestamp() * 1000)
        query += f" AND {ts_col} <= ?"
        params.append(end_ms)
    query += f" ORDER BY {'' if not coin else 'coin, '}{ts_col} ASC"
    return query, params


@app.route("/download")
def download():
    dataset = request.args.get("dataset", "candles")
    coin = request.args.get("coin")
    timeframe = request.args.get("timeframe")
    start = request.args.get("start")
    end = request.args.get("end")
    all_coins = request.args.get("all_coins") == "true"

    conn = get_db()

    if dataset == "candles":
        ts_col = "open_time"
        query, params = _build_query("candles", None if all_coins else coin, timeframe, start, end, ts_col)
        rows = conn.execute(query, params).fetchall()
        conn.close()

        output = io.StringIO()
        writer = csv.writer(output)
        writer.writerow([
            "coin", "timestamp_utc", "open_time_ms", "open", "high", "low", "close",
            "volume", "close_time_ms", "quote_volume", "trades",
            "taker_buy_volume", "taker_buy_quote_volume",
        ])
        for r in rows:
            writer.writerow([
                r["coin"],
                datetime.fromtimestamp(r["open_time"] / 1000, tz=timezone.utc).strftime("%Y-%m-%d %H:%M:%S"),
                r["open_time"], r["open"], r["high"], r["low"], r["close"],
                r["volume"], r["close_time"], r["quote_volume"], r["trades"],
                r["taker_buy_volume"], r["taker_buy_quote"],
            ])

    elif dataset == "polymarket":
        ts_col = "timestamp"
        query, params = _build_query("pm_book_snapshots", None if all_coins else coin, timeframe, start, end, ts_col)
        rows = conn.execute(query, params).fetchall()
        conn.close()

        output = io.StringIO()
        writer = csv.writer(output)
        writer.writerow([
            "coin", "timeframe", "timestamp_utc", "timestamp_ms", "condition_id", "question", "end_date",
            "up_best_bid", "up_best_ask", "up_mid", "up_spread", "up_bid_depth", "up_ask_depth",
            "down_best_bid", "down_best_ask", "down_mid", "down_spread", "down_bid_depth", "down_ask_depth",
            "combined_mid", "implied_edge",
            "up_book_json", "down_book_json",
        ])
        for r in rows:
            writer.writerow([
                r["coin"], r["timeframe"],
                datetime.fromtimestamp(r["timestamp"] / 1000, tz=timezone.utc).strftime("%Y-%m-%d %H:%M:%S"),
                r["timestamp"], r["condition_id"], r["question"], r["end_date"],
                r["up_best_bid"], r["up_best_ask"], r["up_mid"], r["up_spread"], r["up_bid_depth"], r["up_ask_depth"],
                r["down_best_bid"], r["down_best_ask"], r["down_mid"], r["down_spread"], r["down_bid_depth"], r["down_ask_depth"],
                r["combined_mid"], r["implied_edge"],
                r["up_book_json"], r["down_book_json"],
            ])

    elif dataset == "funding":
        query, params = _build_query("funding_rates", None if all_coins else coin, None, start, end, "timestamp")
        rows = conn.execute(query, params).fetchall()
        conn.close()

        output = io.StringIO()
        writer = csv.writer(output)
        writer.writerow(["coin", "timestamp_utc", "timestamp_ms", "funding_rate", "mark_price"])
        for r in rows:
            writer.writerow([
                r["coin"],
                datetime.fromtimestamp(r["timestamp"] / 1000, tz=timezone.utc).strftime("%Y-%m-%d %H:%M:%S"),
                r["timestamp"], r["funding_rate"], r["mark_price"],
            ])

    elif dataset == "oi":
        query, params = _build_query("open_interest", None if all_coins else coin, None, start, end, "timestamp")
        rows = conn.execute(query, params).fetchall()
        conn.close()

        output = io.StringIO()
        writer = csv.writer(output)
        writer.writerow(["coin", "timestamp_utc", "timestamp_ms", "open_interest", "open_interest_usdt"])
        for r in rows:
            writer.writerow([
                r["coin"],
                datetime.fromtimestamp(r["timestamp"] / 1000, tz=timezone.utc).strftime("%Y-%m-%d %H:%M:%S"),
                r["timestamp"], r["open_interest"], r["open_interest_usdt"],
            ])

    elif dataset == "ticker":
        query, params = _build_query("ticker_snapshots", None if all_coins else coin, None, start, end, "timestamp")
        rows = conn.execute(query, params).fetchall()
        conn.close()

        output = io.StringIO()
        writer = csv.writer(output)
        writer.writerow([
            "coin", "timestamp_utc", "timestamp_ms", "price_change_pct",
            "high_24h", "low_24h", "volume_24h", "quote_volume_24h",
            "weighted_avg_price", "last_price",
        ])
        for r in rows:
            writer.writerow([
                r["coin"],
                datetime.fromtimestamp(r["timestamp"] / 1000, tz=timezone.utc).strftime("%Y-%m-%d %H:%M:%S"),
                r["timestamp"], r["price_change_pct"],
                r["high_24h"], r["low_24h"], r["volume_24h"], r["quote_volume_24h"],
                r["weighted_avg_price"], r["last_price"],
            ])

    elif dataset == "outcomes":
        query = "SELECT * FROM pm_market_outcomes"
        params = []
        if coin and not all_coins:
            query += " WHERE coin = ?"
            params.append(coin)
        query += " ORDER BY end_date ASC"
        rows = conn.execute(query, params).fetchall()
        conn.close()

        output = io.StringIO()
        writer = csv.writer(output)
        writer.writerow([
            "condition_id", "coin", "timeframe", "question",
            "start_date", "end_date", "outcome", "up_final_price", "down_final_price",
        ])
        for r in rows:
            writer.writerow([
                r["condition_id"], r["coin"], r["timeframe"], r["question"],
                r["start_date"], r["end_date"], r["outcome"], r["up_final_price"], r["down_final_price"],
            ])
    else:
        return "Unknown dataset", 400

    c = coin or "ALL"
    tf = timeframe or "all"
    filename = f"{dataset}_{c}_{tf}"
    if start:
        filename += f"_from{start}"
    if end:
        filename += f"_to{end}"
    filename += ".csv"

    return Response(
        output.getvalue(),
        mimetype="text/csv",
        headers={"Content-Disposition": f"attachment; filename={filename}"},
    )


# ── Backtest Export Routes ──

@app.route("/api/backtest/markets")
def backtest_markets():
    """List resolved markets with snapshot counts for UI selection."""
    conn = get_db()
    rows = conn.execute("""
        SELECT o.condition_id, o.coin, o.timeframe, o.question,
               o.start_date, o.end_date, o.outcome,
               o.up_final_price, o.down_final_price,
               COUNT(s.id) as snapshot_count
        FROM pm_market_outcomes o
        LEFT JOIN pm_book_snapshots s ON o.condition_id = s.condition_id
        GROUP BY o.condition_id
        ORDER BY o.end_date DESC
    """).fetchall()
    conn.close()

    return jsonify([{
        "condition_id": r["condition_id"],
        "coin": r["coin"],
        "timeframe": r["timeframe"],
        "question": r["question"],
        "start_date": r["start_date"],
        "end_date": r["end_date"],
        "outcome": r["outcome"],
        "up_final_price": r["up_final_price"],
        "down_final_price": r["down_final_price"],
        "snapshot_count": r["snapshot_count"],
    } for r in rows])


@app.route("/export/backtest")
def export_backtest():
    """Generate ZIP with backtest-compatible folder structure.

    Query params:
      format: csv (default) or parquet
      condition_ids: comma-separated list (optional, defaults to all resolved)
    """
    fmt = request.args.get("format", "csv")
    selected_ids = request.args.get("condition_ids", "")

    conn = get_db()

    # Get resolved markets
    if selected_ids:
        id_list = [cid.strip() for cid in selected_ids.split(",") if cid.strip()]
        placeholders = ",".join("?" for _ in id_list)
        markets = conn.execute(
            f"SELECT * FROM pm_market_outcomes WHERE condition_id IN ({placeholders})",
            id_list
        ).fetchall()
    else:
        markets = conn.execute("SELECT * FROM pm_market_outcomes").fetchall()

    if not markets:
        conn.close()
        return jsonify({"error": "No resolved markets found"}), 404

    # Build ZIP in memory
    zip_buf = io.BytesIO()
    ext = "parquet" if fmt == "parquet" else "csv"
    errors = []
    outcomes_columns = ["condition_id", "start_ms", "end_ms", "outcome", "source"]
    outcomes_rows = []

    with zipfile.ZipFile(zip_buf, "w", zipfile.ZIP_DEFLATED) as zf:
        for market in markets:
            cid = market["condition_id"]
            coin = market["coin"]
            timeframe = market["timeframe"]
            end_date_str = market["end_date"] or ""
            outcome = market["outcome"] or ""

            try:
                # Build prices file
                prices_content, _ = _build_prices_file(
                    conn, cid, coin, timeframe, end_date_str, fmt
                )

                # Compute start/end ms for period_result
                start_ms = _derive_start_ms(conn, cid, end_date_str, timeframe)
                end_ms = _parse_end_date_ms(end_date_str)
                if end_ms == 0:
                    end_ms = conn.execute(
                        "SELECT MAX(timestamp) FROM pm_book_snapshots WHERE condition_id = ?",
                        (cid,)
                    ).fetchone()[0] or 0

                # Get price_open from Binance
                candle_series = _precompute_binance_prices(conn, coin, start_ms, end_ms)
                price_open = _lookup_price(candle_series, start_ms)

                # Build period_result file
                period_content, _ = _build_period_result_file(
                    cid, coin, timeframe, outcome, price_open, start_ms, end_ms, fmt
                )

                # Write to ZIP
                folder = cid
                if isinstance(prices_content, bytes):
                    zf.writestr(f"{folder}/prices.{ext}", prices_content)
                else:
                    zf.writestr(f"{folder}/prices.{ext}", prices_content)

                if isinstance(period_content, bytes):
                    zf.writestr(f"{folder}/period_result.{ext}", period_content)
                else:
                    zf.writestr(f"{folder}/period_result.{ext}", period_content)

                # Collect for outcomes
                outcomes_rows.append([
                    cid, start_ms, end_ms, outcome, "polymarket"
                ])

            except Exception as e:
                errors.append(f"{cid}: {e}")

        # Write global outcomes file
        if fmt == "parquet":
            outcomes_data = _rows_to_parquet(outcomes_columns, outcomes_rows)
            zf.writestr(f"outcomes.{ext}", outcomes_data)
        else:
            outcomes_data = _rows_to_csv(outcomes_columns, outcomes_rows)
            zf.writestr(f"outcomes.{ext}", outcomes_data)

        # Write errors file if any
        if errors:
            zf.writestr("_export_errors.txt", "\n".join(errors))

    conn.close()

    timestamp = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")
    filename = f"backtest_export_{timestamp}.zip"

    zip_buf.seek(0)
    return Response(
        zip_buf.getvalue(),
        mimetype="application/zip",
        headers={"Content-Disposition": f"attachment; filename={filename}"},
    )


if __name__ == "__main__":
    print(f"DB path: {DB_PATH}")
    print("Dashboard running at http://0.0.0.0:8050")
    app.run(host="0.0.0.0", port=8050, debug=False)
