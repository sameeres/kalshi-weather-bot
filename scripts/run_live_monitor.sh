#!/usr/bin/env bash
# scripts/run_live_monitor.sh
#
# Runs the climatology monitor for the 10:00 AM ET decision window.
# When live_trading: true is set in configs/paper_trading.yml, this will
# place real Kalshi orders for qualified markets.
#
# Called automatically by the com.kwb.daily-live-monitor launchd agent at 9:50 AM ET.
# Can also be run manually for testing.
#
# Environment overrides:
#   PAPER_CONFIG_PATH      — path to paper_trading.yml (default: configs/paper_trading.yml)
#   PAPER_OUTPUT_ROOT      — root for daily output dirs (default: data/marts/paper_trading)
#   MICROSTRUCTURE_DIR     — staging dir for Kalshi snapshots (default: data/staging)
#   ITERATIONS             — number of 1-minute polls (default: 30, covers 10:00–10:30 AM)
#   POLL_INTERVAL_SECONDS  — seconds between polls (default: 60)

set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT_DIR"

# Load .env so KALSHI_API_KEY_ID and KALSHI_PRIVATE_KEY_PATH are available.
if [[ -f "$ROOT_DIR/.env" ]]; then
  set -o allexport
  source "$ROOT_DIR/.env"
  set +o allexport
fi

PAPER_CONFIG_PATH="${PAPER_CONFIG_PATH:-configs/paper_trading.yml}"
PAPER_OUTPUT_ROOT="${PAPER_OUTPUT_ROOT:-data/marts/paper_trading}"
MICROSTRUCTURE_DIR="${MICROSTRUCTURE_DIR:-data/staging}"
ITERATIONS="${ITERATIONS:-30}"
POLL_INTERVAL_SECONDS="${POLL_INTERVAL_SECONDS:-60}"

LIVE_TRADING=$(python3 -c "
import yaml, sys
with open('$PAPER_CONFIG_PATH') as f:
    cfg = yaml.safe_load(f)
print(str(cfg.get('paper_climatology_monitor', {}).get('live_trading', False)).lower())
" 2>/dev/null || echo "false")

echo "=== kwb live monitor ==="
echo "Repo root:        $ROOT_DIR"
echo "Config:           $PAPER_CONFIG_PATH"
echo "Output root:      $PAPER_OUTPUT_ROOT"
echo "Iterations:       $ITERATIONS"
echo "Poll interval:    ${POLL_INTERVAL_SECONDS}s"
echo "Expected window:  ~$((ITERATIONS * POLL_INTERVAL_SECONDS / 60)) minutes"
echo "Live trading:     $LIVE_TRADING"
if [[ "$LIVE_TRADING" == "true" ]]; then
  echo "*** LIVE MODE: real Kalshi orders will be placed ***"
fi
echo "Started at:       $(date -u '+%Y-%m-%dT%H:%M:%SZ')"
echo

python3 -m kwb research paper-monitor-climatology \
  --paper-config-path "$PAPER_CONFIG_PATH" \
  --output-root "$PAPER_OUTPUT_ROOT" \
  --microstructure-dir "$MICROSTRUCTURE_DIR" \
  --iterations "$ITERATIONS" \
  --poll-interval-seconds "$POLL_INTERVAL_SECONDS"

echo
echo "Monitor finished at: $(date -u '+%Y-%m-%dT%H:%M:%SZ')"
echo "Use scripts/show_latest_paper_reports.sh to inspect outputs."
