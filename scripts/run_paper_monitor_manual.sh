#!/usr/bin/env bash

set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT_DIR"

PAPER_OUTPUT_ROOT="${PAPER_OUTPUT_ROOT:-data/marts/paper_trading}"
PAPER_CONFIG_PATH="${PAPER_CONFIG_PATH:-configs/paper_trading.yml}"
MICROSTRUCTURE_DIR="${MICROSTRUCTURE_DIR:-data/staging}"
ITERATIONS="${ITERATIONS:-120}"
POLL_INTERVAL_SECONDS="${POLL_INTERVAL_SECONDS:-60}"

echo "Starting bounded paper-only climatology monitor"
echo "Repo root: $ROOT_DIR"
echo "Paper config: $PAPER_CONFIG_PATH"
echo "Paper output root: $PAPER_OUTPUT_ROOT"
echo "Microstructure dir: $MICROSTRUCTURE_DIR"
echo "Iterations: $ITERATIONS"
echo "Poll interval seconds: $POLL_INTERVAL_SECONDS"
echo "Expected session coverage: about $((ITERATIONS * POLL_INTERVAL_SECONDS / 60)) minutes"
echo
echo "Outputs will appear under: $ROOT_DIR/$PAPER_OUTPUT_ROOT/YYYY-MM-DD/"
echo

python3 -m kwb research paper-monitor-climatology \
  --paper-config-path "$PAPER_CONFIG_PATH" \
  --output-root "$PAPER_OUTPUT_ROOT" \
  --microstructure-dir "$MICROSTRUCTURE_DIR" \
  --iterations "$ITERATIONS" \
  --poll-interval-seconds "$POLL_INTERVAL_SECONDS"

echo
echo "Paper monitor finished successfully."
echo "Use scripts/show_latest_paper_reports.sh to see the latest report paths."
