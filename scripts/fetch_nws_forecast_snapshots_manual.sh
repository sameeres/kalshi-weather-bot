#!/usr/bin/env bash

set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT_DIR"

CONFIG_PATH="${CONFIG_PATH:-configs/cities.yml}"
STAGING_DIR="${STAGING_DIR:-data/staging}"
ITERATIONS="${ITERATIONS:-1}"
POLL_INTERVAL_SECONDS="${POLL_INTERVAL_SECONDS:-900}"

echo "Starting bounded NWS forecast snapshot collection"
echo "Repo root: $ROOT_DIR"
echo "Config path: $CONFIG_PATH"
echo "Staging dir: $STAGING_DIR"
echo "Iterations: $ITERATIONS"
echo "Poll interval seconds: $POLL_INTERVAL_SECONDS"
echo

for ((i=1; i<=ITERATIONS; i++)); do
  echo "Snapshot iteration $i of $ITERATIONS"
  python3 -m kwb data fetch-nws-forecast-snapshots \
    --config-path "$CONFIG_PATH" \
    --output-dir "$STAGING_DIR"

  if [[ "$i" -lt "$ITERATIONS" ]]; then
    echo "Sleeping $POLL_INTERVAL_SECONDS seconds before the next snapshot..."
    sleep "$POLL_INTERVAL_SECONDS"
  fi
done

echo
echo "Forecast snapshot collection finished."
echo "Staged snapshot archive: $ROOT_DIR/$STAGING_DIR/nws_forecast_hourly_snapshots.parquet"
