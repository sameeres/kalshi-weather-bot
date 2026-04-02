#!/usr/bin/env bash

set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT_DIR"

FORECAST_OUTPUT_ROOT="${FORECAST_OUTPUT_ROOT:-data/marts/forecast_distribution_manual}"
FULL_ROOT="$ROOT_DIR/$FORECAST_OUTPUT_ROOT"
SNAPSHOT_ARCHIVE="$ROOT_DIR/data/staging/nws_forecast_hourly_snapshots.parquet"

echo "Forecast-distribution output root: $FULL_ROOT"
echo "Snapshot archive: $SNAPSHOT_ARCHIVE"
echo

if [[ ! -d "$FORECAST_OUTPUT_ROOT" ]]; then
  echo "No forecast-distribution output root found yet."
  exit 0
fi

LATEST_RUN="$(find "$FORECAST_OUTPUT_ROOT" -mindepth 1 -maxdepth 1 -type d -exec basename {} \; | sort | tail -n 1)"

if [[ -z "$LATEST_RUN" ]]; then
  echo "No dated forecast-distribution directories found yet."
  exit 0
fi

RUN_DIR="$FULL_ROOT/$LATEST_RUN"
echo "Latest forecast-distribution run: $LATEST_RUN"
echo "Run dir: $RUN_DIR"
echo

for FILE in \
  research_manifest_forecast_distribution.json \
  forecast_snapshot_coverage.md \
  forecast_snapshot_coverage.json \
  backtest_report_forecast_distribution.md \
  backtest_summary_forecast_distribution.json \
  backtest_trades_forecast_distribution.parquet \
  backtest_scored_forecast_distribution.parquet \
  backtest_scored_climatology.parquet
do
  if [[ -f "$RUN_DIR/$FILE" ]]; then
    echo "Found: $RUN_DIR/$FILE"
  else
    echo "Missing: $RUN_DIR/$FILE"
  fi
done
