#!/usr/bin/env bash

set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT_DIR"

FORECAST_OUTPUT_ROOT="${FORECAST_OUTPUT_ROOT:-data/marts/forecast_distribution_manual}"
RUN_DATE="${RUN_DATE:-$(date +%F)}"
RUN_DIR="$FORECAST_OUTPUT_ROOT/$RUN_DATE"
DECISION_TIME_LOCAL="${DECISION_TIME_LOCAL:-10:00}"
CONFIG_PATH="${CONFIG_PATH:-configs/cities.yml}"
FORECAST_SNAPSHOTS_PATH="${FORECAST_SNAPSHOTS_PATH:-data/staging/nws_forecast_hourly_snapshots.parquet}"

echo "Starting manual forecast-distribution research run"
echo "Repo root: $ROOT_DIR"
echo "Run dir: $ROOT_DIR/$RUN_DIR"
echo "Decision time local: $DECISION_TIME_LOCAL"
echo "Config path: $CONFIG_PATH"
echo "Forecast snapshots path: $FORECAST_SNAPSHOTS_PATH"
echo

python3 -m kwb research run-forecast-distribution \
  --decision-time-local "$DECISION_TIME_LOCAL" \
  --config-path "$CONFIG_PATH" \
  --forecast-snapshots-path "$FORECAST_SNAPSHOTS_PATH" \
  --output-dir "$RUN_DIR" \
  --overwrite

echo
echo "Forecast-distribution research run finished."
echo "Main comparison report: $ROOT_DIR/$RUN_DIR/backtest_report_forecast_distribution.md"
echo "Coverage report: $ROOT_DIR/$RUN_DIR/forecast_snapshot_coverage.md"
