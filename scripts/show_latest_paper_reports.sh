#!/usr/bin/env bash

set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT_DIR"

PAPER_OUTPUT_ROOT="${PAPER_OUTPUT_ROOT:-data/marts/paper_trading}"
FULL_ROOT="$ROOT_DIR/$PAPER_OUTPUT_ROOT"

echo "Paper output root: $FULL_ROOT"

if [[ ! -d "$PAPER_OUTPUT_ROOT" ]]; then
  echo "No paper output root found yet."
  exit 0
fi

LATEST_DATE="$(find "$PAPER_OUTPUT_ROOT" -mindepth 1 -maxdepth 1 -type d -exec basename {} \; | sort | tail -n 1)"

if [[ -z "$LATEST_DATE" ]]; then
  echo "No dated paper-trading directories found yet."
  exit 0
fi

DAILY_DIR="$FULL_ROOT/$LATEST_DATE"
echo "Latest paper-trading date: $LATEST_DATE"
echo "Daily dir: $DAILY_DIR"
echo

for FILE in \
  paper_climatology_report.md \
  paper_climatology_summary.json \
  paper_climatology_trades.parquet \
  paper_climatology_reconciliation_report.md \
  paper_climatology_reconciliation_summary.json \
  paper_climatology_reconciled_trades.parquet
do
  if [[ -f "$DAILY_DIR/$FILE" ]]; then
    echo "Found: $DAILY_DIR/$FILE"
  else
    echo "Missing: $DAILY_DIR/$FILE"
  fi
done

echo

for FILE in \
  paper_climatology_cumulative_report.md \
  paper_climatology_cumulative_summary.json \
  paper_climatology_cumulative_scoreboard.csv
do
  if [[ -f "$FULL_ROOT/$FILE" ]]; then
    echo "Found: $FULL_ROOT/$FILE"
  else
    echo "Missing: $FULL_ROOT/$FILE"
  fi
done
