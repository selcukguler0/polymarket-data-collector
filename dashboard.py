#!/usr/bin/env python3
"""
Dashboard for browsing and downloading collected backtest data.
Serves Binance candles, funding rates, OI, and Polymarket book snapshots.
Run: python dashboard.py
"""

import csv
import io
import json
import sqlite3
from datetime import datetime, timezone
from pathlib import Path

from flask import Flask, Response, render_template, request, jsonify

DB_PATH = Path(__file__).parent / "data" / "candles.db"

app = Flask(__name__)


def get_db() -> sqlite3.Connection:
    conn = sqlite3.connect(str(DB_PATH), timeout=60)
    conn.execute("PRAGMA busy_timeout=60000")
    conn.row_factory = sqlite3.Row
    return conn


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


if __name__ == "__main__":
    print(f"DB path: {DB_PATH}")
    print("Dashboard running at http://0.0.0.0:8050")
    app.run(host="0.0.0.0", port=8050, debug=False)
