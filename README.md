# Polymarket Data Collector

Backtest data collector that continuously gathers **Binance OHLCV candles** and **Polymarket Up/Down order book snapshots** for BTC, ETH, XRP, and SOL. All data is stored in a local SQLite database and served through a web dashboard with CSV export.

## Data Collected

### Binance
- **OHLCV candles** across 5m, 15m, 1h, 1d timeframes (with historical backfill)
- **Funding rates** and mark prices (futures)
- **Open interest** snapshots
- **24h ticker** stats (price change, volume, highs/lows)

### Polymarket
- **Order book snapshots** (~5s interval) for active Up/Down binary markets
- Top 10 bid/ask levels, spread, depth, mid prices, implied edge
- **Resolved market outcomes** for backtest ground truth

## Quick Start

### Local

```bash
# Install dependencies
pip install -r requirements.txt

# Run collector + dashboard
./run.sh
```

The dashboard will be available at `http://localhost:8050`.

### Docker

```bash
docker compose up -d
```

Data persists in the `./data/` directory via volume mount.

## Project Structure

```
collector.py          # Data collection engine (Binance + Polymarket)
dashboard.py          # Flask web dashboard with API and CSV downloads
templates/index.html  # Dashboard frontend
run.sh                # Launch script (collector + dashboard)
data/candles.db       # SQLite database (created at runtime)
Dockerfile            # Container image
docker-compose.yml    # Docker Compose config
requirements.txt      # Python dependencies
```

## Dashboard

The web dashboard at `:8050` provides:

- Live stats for all collected data (candle counts, snapshot counts, resolved markets)
- Data preview with filtering by coin, timeframe, and date range
- CSV downloads for any dataset (single coin or all coins)

### API Endpoints

| Endpoint        | Description                          |
|-----------------|--------------------------------------|
| `GET /api/stats`   | Collection statistics summary     |
| `GET /api/preview` | Preview rows (50 most recent)     |
| `GET /download`    | Download filtered data as CSV     |

## Database Schema

| Table                | Description                              |
|----------------------|------------------------------------------|
| `candles`            | Binance OHLCV candles                    |
| `funding_rates`      | Binance futures funding rate snapshots   |
| `open_interest`      | Binance futures open interest snapshots  |
| `ticker_snapshots`   | Binance 24h ticker stats                 |
| `pm_book_snapshots`  | Polymarket order book snapshots          |
| `pm_market_outcomes` | Resolved Polymarket market results       |

## Configuration

Key parameters in `collector.py`:

| Parameter              | Default | Description                        |
|------------------------|---------|------------------------------------|
| `PM_SNAPSHOT_INTERVAL` | 5s      | Polymarket book snapshot frequency |
| `PM_DISCOVERY_INTERVAL`| 30s     | Market discovery polling interval  |
| `POLL_INTERVALS`       | varies  | Binance candle poll intervals      |
| Backfill days          | 30-730  | Historical data depth per timeframe|

## Requirements

- Python 3.12+
- No API keys required (uses public Binance and Polymarket endpoints)
