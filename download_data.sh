#!/bin/bash
# Download live_collector CSV files from remote Docker container to local machine
# Usage: ./download_data.sh [local_output_dir]

set -e

# ── Remote server config ──
SERVER="69.62.119.76"
USER="root"
CONTAINER="a5f7cbf840a2"

# ── Paths ──
LOCAL_DIR="${1:-./downloaded_data}"
REMOTE_TMP="/tmp/polymarket_csv_export"

# ── CSV files to download ──
CSV_FILES=(
    "book_snapshots.csv"
    "live_trades.csv"
    "market_outcomes.csv"
    "active_markets.csv"
    "sports_book_snapshots.csv"
    "sports_live_trades.csv"
    "sports_outcomes.csv"
    "sports_markets.csv"
)

mkdir -p "$LOCAL_DIR"

echo "=== Polymarket Data Downloader ==="
echo "Server: $USER@$SERVER"
echo "Container: $CONTAINER"
echo "Local dir: $LOCAL_DIR"
echo ""

# Find the data directory inside the container
echo "[1/3] Finding data directory inside container..."
DATA_PATH=$(ssh "$USER@$SERVER" "docker exec $CONTAINER find / -maxdepth 4 -type d -name data 2>/dev/null | head -1")

if [ -z "$DATA_PATH" ]; then
    echo "ERROR: Could not find data/ directory in container"
    exit 1
fi
echo "  Found: $DATA_PATH"

# Copy files from container to server /tmp
echo "[2/3] Copying from container to server..."
ssh "$USER@$SERVER" "mkdir -p $REMOTE_TMP && \
    for f in ${CSV_FILES[*]}; do \
        docker cp $CONTAINER:$DATA_PATH/\$f $REMOTE_TMP/\$f 2>/dev/null && \
        echo \"  Copied \$f\" || echo \"  Skipped \$f (not found)\"; \
    done"

# rsync from server to local (resumes on failure, compresses transfer)
echo "[3/3] Downloading to local machine..."
rsync -avz --progress "$USER@$SERVER:$REMOTE_TMP/" "$LOCAL_DIR/"

# Cleanup server tmp
ssh "$USER@$SERVER" "rm -rf $REMOTE_TMP"

echo ""
echo "=== Done ==="
ls -lh "$LOCAL_DIR"/*.csv 2>/dev/null
