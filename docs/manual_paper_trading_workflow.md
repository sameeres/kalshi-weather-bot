# Manual Paper Trading Workflow

## What this does

This workflow runs the existing paper-only climatology monitor for the current narrow slice:

- cities: NYC + Chicago
- contract type: `or_below`
- side: `YES`
- entry price gate: `0-25` cents
- fee-aware decision logic
- `10:00 local` baseline

It uses the existing repo logic to:

- capture current Kalshi weather-market quote context
- score eligible markets with the existing climatology model
- log all evaluations and paper trades
- reconcile realized paper performance the next morning

## What this does not do

- no live orders
- no VPS or scheduler infrastructure
- manual run only
- no strategy broadening beyond the validated gate

## Daily workflow from Amsterdam

The current baseline is `10:00 local` for each city:

- NYC `10:00 local` is usually about `16:00` Amsterdam
- Chicago `10:00 local` is usually about `17:00` Amsterdam

Daylight saving time can shift these conversions, so verify occasionally.

## Afternoon monitor run

Run:

```bash
./scripts/run_paper_monitor_manual.sh
```

This is a bounded session, not an infinite loop.

Default session settings:

- `ITERATIONS=120`
- `POLL_INTERVAL_SECONDS=60`

That covers about `120` minutes.

Outputs are written under:

```text
data/marts/paper_trading/YYYY-MM-DD/
```

Key files:

- `paper_climatology_report.md`
- `paper_climatology_summary.json`
- `paper_climatology_evaluations.parquet`
- `paper_climatology_trades.parquet`

### If you want to tweak the session length

You can override the shell variables for one run:

```bash
ITERATIONS=90 POLL_INTERVAL_SECONDS=60 ./scripts/run_paper_monitor_manual.sh
```

## Next-morning reconciliation

Run:

```bash
./scripts/reconcile_paper_monitor_latest.sh
```

This reconciles the latest available paper-monitor date by default.

If you want a specific date:

```bash
./scripts/reconcile_paper_monitor_latest.sh 2026-04-01
```

Reconciliation outputs are written into the same daily directory plus cumulative files at the paper root.

Daily reconciliation files:

- `paper_climatology_reconciled_trades.parquet`
- `paper_climatology_reconciliation_summary.json`
- `paper_climatology_reconciliation_report.md`

Cumulative files:

- `paper_climatology_cumulative_scoreboard.csv`
- `paper_climatology_cumulative_summary.json`
- `paper_climatology_cumulative_report.md`

## Quick status helper

Run:

```bash
./scripts/show_latest_paper_reports.sh
```

This prints:

- the latest available paper-trading date
- the latest daily report paths
- whether reconciliation files exist for that date
- the cumulative scoreboard/report paths

## What to inspect each morning

Look at:

- latest daily monitor report
- latest reconciliation report
- cumulative scoreboard and cumulative report
- unresolved trades
- rejection reasons
- city-level breakdown

Useful order:

1. `paper_climatology_reconciliation_report.md`
2. `paper_climatology_cumulative_report.md`
3. `paper_climatology_report.md`
4. `paper_climatology_reconciliation_summary.json`

## Common failure modes

### Missing outputs

Possible causes:

- command failed before writing artifacts
- wrong working directory
- environment or package path issue

Check:

- you are in the repo root
- the virtual environment is activated if needed
- `python3 -m kwb --help` works

### No trades that day

This is allowed.

The monitor still logs evaluations, rejection reasons, spreads, and fee-aware edge calculations. A no-trade day is still useful forward evidence.

### Unresolved trade the next morning

This usually means the settlement-aligned weather observation is not yet available in staged history for that event date.

The reconciliation command will mark those trades as unresolved and exclude them from finalized realized PnL until they can be resolved cleanly.

### Command path or environment issues

If a script fails immediately, try:

```bash
python3 -m kwb research paper-monitor-climatology --help
python3 -m kwb research reconcile-paper-climatology --help
```

## Daily checklist

1. Before the afternoon session, confirm you can spare about two hours around the Amsterdam equivalents of the baseline window.
2. Start the bounded monitor with `./scripts/run_paper_monitor_manual.sh`.
3. After the run completes, inspect `paper_climatology_report.md`.
4. The next morning, run `./scripts/reconcile_paper_monitor_latest.sh`.
5. Read `paper_climatology_reconciliation_report.md`.
6. Check `paper_climatology_cumulative_report.md` for the running scoreboard.
7. Use `./scripts/show_latest_paper_reports.sh` anytime you want the current artifact paths.

## Remaining manual prerequisite

Before the first real forward paper-trading day, make sure:

- the repo environment is installed and `python3 -m kwb` runs successfully
- you have working network access for the live Kalshi data path from your laptop
- staged settlement-aligned weather data remains current enough for next-morning reconciliation
