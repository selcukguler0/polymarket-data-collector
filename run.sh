#!/bin/bash
# Run both collector and dashboard
# Usage: ./run.sh

DIR="$(cd "$(dirname "$0")" && pwd)"
cd "$DIR"

pip install -r requirements.txt -q

echo "Starting collector in background..."
python collector.py &
COLLECTOR_PID=$!

echo "Starting dashboard on :8050..."
python dashboard.py &
DASHBOARD_PID=$!

echo ""
echo "  Collector PID: $COLLECTOR_PID"
echo "  Dashboard PID: $DASHBOARD_PID"
echo "  Dashboard URL: http://localhost:8050"
echo ""
echo "Press Ctrl+C to stop both"

trap "kill $COLLECTOR_PID $DASHBOARD_PID 2>/dev/null; exit" INT TERM
wait
