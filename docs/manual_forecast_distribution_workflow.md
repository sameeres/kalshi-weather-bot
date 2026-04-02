# Manual Forecast Distribution Workflow

## What this does

This workflow adds a paper/research support layer for the forecast-distribution sidecar:

- cities: NYC + Chicago only
- decision framing: `10:00 local`
- NWS forecast snapshots from `api.weather.gov`
- research only
- no live trading logic
- no change to the existing climatology paper monitor flow

It is meant to help you:

- collect forecast snapshots manually from Amsterdam
- run the forecast-distribution research bundle with the currently staged data
- inspect the latest comparison and coverage reports quickly
- judge whether snapshot coverage is good enough to keep accumulating forward evidence

## What this does not do

- no live orders
- no VPS or scheduler
- no strategy broadening beyond the current daily-high forecast-distribution sidecar
- no replacement of the existing climatology workflow

## Timing from Amsterdam

The baseline remains `10:00 local` for each city:

- NYC `10:00 local` is usually about `16:00` Amsterdam
- Chicago `10:00 local` is usually about `17:00` Amsterdam

For manual forward accumulation, the practical goal is to capture at least one forecast snapshot before each city's `10:00 local` decision time.

## Step 1: collect forecast snapshots

Run:

```bash
./scripts/fetch_nws_forecast_snapshots_manual.sh
```

By default this does a single bounded fetch and appends into:

```text
data/staging/nws_forecast_hourly_snapshots.parquet
```

If you want a denser archive around the afternoon Amsterdam window, you can run a short bounded loop:

```bash
ITERATIONS=4 POLL_INTERVAL_SECONDS=900 ./scripts/fetch_nws_forecast_snapshots_manual.sh
```

That captures four snapshots 15 minutes apart.

## Step 2: run the forecast-distribution research bundle

Run:

```bash
./scripts/run_forecast_distribution_manual.sh
```

By default it writes into:

```text
data/marts/forecast_distribution_manual/YYYY-MM-DD/
```

Key files:

- `backtest_report_forecast_distribution.md`
- `backtest_summary_forecast_distribution.json`
- `forecast_snapshot_coverage.md`
- `forecast_snapshot_coverage.json`
- `research_manifest_forecast_distribution.json`

## Step 3: inspect the latest outputs

Run:

```bash
./scripts/show_latest_forecast_distribution_reports.sh
```

Useful order:

1. `forecast_snapshot_coverage.md`
2. `backtest_report_forecast_distribution.md`
3. `backtest_summary_forecast_distribution.json`
4. `research_manifest_forecast_distribution.json`

## What good-enough coverage roughly means

This should stay skeptical and operational:

- both NYC and Chicago should appear regularly in the snapshot archive
- you should usually have at least one pre-decision snapshot for each city on forward dates you care about
- matched-share in the coverage report should trend toward something like `0.80` or better before you lean much on the results
- if matched-share is much lower, treat the forecast-sidecar evidence as thin and keep accumulating

Single-day results are not enough. The point is to build a usable archive and then compare climatology-only, forecast-only, and overlap signals over time.

## Quick checklist

1. In the Amsterdam afternoon window, run `./scripts/fetch_nws_forecast_snapshots_manual.sh`.
2. After snapshots are staged, run `./scripts/run_forecast_distribution_manual.sh`.
3. Inspect `forecast_snapshot_coverage.md` first.
4. Then inspect `backtest_report_forecast_distribution.md`.
5. Use `./scripts/show_latest_forecast_distribution_reports.sh` anytime for the latest paths.

## Reminder

This remains research/paper support only. It does not add live execution, automation infrastructure, or any broader strategy expansion.
