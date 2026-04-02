#!/usr/bin/env bash

set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT_DIR"

PAPER_OUTPUT_ROOT="${PAPER_OUTPUT_ROOT:-data/marts/paper_trading}"
HISTORY_PATH="${HISTORY_PATH:-}"
TRADE_DATE="${1:-}"

echo "Starting paper climatology reconciliation"
echo "Repo root: $ROOT_DIR"
echo "Paper output root: $PAPER_OUTPUT_ROOT"
if [[ -n "$TRADE_DATE" ]]; then
  echo "Trade date override: $TRADE_DATE"
else
  echo "Trade date override: latest available date"
fi
if [[ -n "$HISTORY_PATH" ]]; then
  echo "Weather history override: $HISTORY_PATH"
fi
echo

CMD=(
  python3 -m kwb research reconcile-paper-climatology
  --paper-output-root "$PAPER_OUTPUT_ROOT"
)

if [[ -n "$TRADE_DATE" ]]; then
  CMD+=(--trade-date "$TRADE_DATE")
fi

if [[ -n "$HISTORY_PATH" ]]; then
  CMD+=(--history-path "$HISTORY_PATH")
fi

"${CMD[@]}"

echo
echo "Reconciliation finished successfully."
echo "Use scripts/show_latest_paper_reports.sh to see the latest reconciliation and cumulative report paths."
