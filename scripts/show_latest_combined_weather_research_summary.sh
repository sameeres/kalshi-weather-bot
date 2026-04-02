#!/usr/bin/env bash

set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT_DIR"

PAPER_ROOT="${PAPER_ROOT:-data/marts/paper_trading}"
FORECAST_ROOT="${FORECAST_ROOT:-data/marts/forecast_distribution_manual}"
OUTPUT_PATH="${OUTPUT_PATH:-data/marts/combined_weather_research_summary_latest.md}"

echo "Building combined weather research summary"
echo "Repo root: $ROOT_DIR"
echo "Paper root: $ROOT_DIR/$PAPER_ROOT"
echo "Forecast root: $ROOT_DIR/$FORECAST_ROOT"
echo "Output path: $ROOT_DIR/$OUTPUT_PATH"
echo

python3 -m kwb research build-combined-weather-summary \
  --paper-root "$PAPER_ROOT" \
  --forecast-root "$FORECAST_ROOT" \
  --output-path "$OUTPUT_PATH"

echo
echo "Combined summary ready: $ROOT_DIR/$OUTPUT_PATH"
