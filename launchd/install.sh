#!/usr/bin/env bash
# launchd/install.sh
#
# Installs the three daily kwb launchd agents into ~/Library/LaunchAgents/.
# Run once from the repo root (or from anywhere — the script resolves paths itself).
#
# Usage:
#   bash launchd/install.sh           # install all three agents
#   bash launchd/install.sh --unload  # unload agents (keeps plist files)
#   bash launchd/install.sh --remove  # unload and delete plist files

set -euo pipefail

LAUNCHD_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "$LAUNCHD_DIR/.." && pwd)"
LAUNCH_AGENTS_DIR="$HOME/Library/LaunchAgents"
LOGS_DIR="$REPO_ROOT/logs"

AGENTS=(
  "com.kwb.daily-nws-snapshot"
  "com.kwb.daily-live-monitor"
  "com.kwb.daily-reconcile"
)

# ---- argument parsing -------------------------------------------------------

MODE="install"
if [[ "${1:-}" == "--unload" ]]; then
  MODE="unload"
elif [[ "${1:-}" == "--remove" ]]; then
  MODE="remove"
fi

# ---- helpers ----------------------------------------------------------------

stamp_and_copy() {
  local label="$1"
  local src="$LAUNCHD_DIR/${label}.plist"
  local dst="$LAUNCH_AGENTS_DIR/${label}.plist"

  if [[ ! -f "$src" ]]; then
    echo "ERROR: source plist not found: $src" >&2
    return 1
  fi

  # Replace the KWB_REPO_ROOT placeholder with the actual path.
  sed "s|KWB_REPO_ROOT|${REPO_ROOT}|g" "$src" > "$dst"
  echo "  Copied $label.plist → $dst"
}

unload_agent() {
  local label="$1"
  local dst="$LAUNCH_AGENTS_DIR/${label}.plist"
  if launchctl list "$label" &>/dev/null; then
    launchctl unload "$dst" 2>/dev/null && echo "  Unloaded $label" || echo "  Warning: could not unload $label"
  else
    echo "  $label not currently loaded"
  fi
}

load_agent() {
  local label="$1"
  local dst="$LAUNCH_AGENTS_DIR/${label}.plist"
  launchctl load "$dst"
  echo "  Loaded $label"
}

# ---- main -------------------------------------------------------------------

echo "kwb launchd agent manager"
echo "Repo root: $REPO_ROOT"
echo "LaunchAgents dir: $LAUNCH_AGENTS_DIR"
echo "Mode: $MODE"
echo

if [[ "$MODE" == "install" ]]; then
  mkdir -p "$LAUNCH_AGENTS_DIR"
  mkdir -p "$LOGS_DIR"

  echo "Checking .env..."
  if [[ ! -f "$REPO_ROOT/.env" ]]; then
    echo "  WARNING: $REPO_ROOT/.env not found."
    echo "  Copy .env.example to .env and fill in KALSHI_API_KEY_ID and KALSHI_PRIVATE_KEY_PATH"
    echo "  before live_trading can be enabled."
  else
    echo "  .env found."
  fi
  echo

  echo "Installing agents..."
  for LABEL in "${AGENTS[@]}"; do
    # Unload existing version first if loaded.
    if launchctl list "$LABEL" &>/dev/null; then
      launchctl unload "$LAUNCH_AGENTS_DIR/${LABEL}.plist" 2>/dev/null || true
    fi
    stamp_and_copy "$LABEL"
    load_agent "$LABEL"
  done

  echo
  echo "All agents installed and loaded."
  echo
  echo "Schedule (times are UTC, your machine clock must be UTC-aware):"
  echo "  14:15 UTC (~9:15 AM ET) — NWS forecast snapshot"
  echo "  14:50 UTC (~9:50 AM ET) — Live monitor / order placement"
  echo "  12:30 UTC (~7:30 AM ET) — Daily reconciliation"
  echo
  echo "Logs:"
  echo "  $LOGS_DIR/nws_snapshot.log"
  echo "  $LOGS_DIR/live_monitor.log"
  echo "  $LOGS_DIR/reconcile.log"
  echo
  echo "To verify agents are loaded:"
  echo "  launchctl list | grep kwb"
  echo
  echo "To unload later:"
  echo "  bash launchd/install.sh --unload"

elif [[ "$MODE" == "unload" ]]; then
  echo "Unloading agents..."
  for LABEL in "${AGENTS[@]}"; do
    unload_agent "$LABEL"
  done
  echo "Done. Plist files remain in $LAUNCH_AGENTS_DIR."

elif [[ "$MODE" == "remove" ]]; then
  echo "Unloading and removing agents..."
  for LABEL in "${AGENTS[@]}"; do
    unload_agent "$LABEL"
    dst="$LAUNCH_AGENTS_DIR/${LABEL}.plist"
    if [[ -f "$dst" ]]; then
      rm "$dst"
      echo "  Removed $dst"
    fi
  done
  echo "Done."
fi
