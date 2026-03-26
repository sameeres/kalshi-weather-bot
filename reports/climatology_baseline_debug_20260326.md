# Climatology Baseline Debug Report

Date: 2026-03-26

## Environment verification

- `git status --short`: clean working tree
- Current shell `NCEI_API_TOKEN`: not set
- Sandbox DNS to Kalshi/NOAA: failed
- Unrestricted DNS to Kalshi/NOAA: succeeded

## Staging rebuild attempt

Command run:

```bash
python3 -m kwb data build-staging \
  --start-date 2025-11-01 \
  --end-date 2026-01-30 \
  --weather-start-date 2015-01-01 \
  --weather-end-date 2026-01-30 \
  --overwrite
```

Result:

- `weather_daily` failed: `NCEI_API_TOKEN is not set`
- `weather_normals_daily` failed: `NCEI_API_TOKEN is not set`
- `kalshi_market_history` failed in sandbox on DNS resolution to `api.elections.kalshi.com`

This means the attempted rebuild did not refresh the staged NOAA datasets.

## On-disk staging state inspected after the failed rebuild

### `data/staging/weather_daily.parquet`

- Exists: yes
- Columns: `station_id`, `city_key`, `obs_date`, `tmax_c`, `tmin_c`, `tmax_f`, `tmin_f`, `source_dataset`, `ingested_at`
- Row count: `91`
- Date coverage: `2025-11-01` through `2026-01-30`
- Station IDs present: `KLGA`

### `data/staging/weather_normals_daily.parquet`

- Station IDs present: `KLGA`

### Config vs staged station mapping

- Configured NYC settlement station: `KNYC`
- Configured NYC settlement station name: `Central Park`
- Staged weather station: `KLGA`
- Staged normals station: `KLGA`

Conclusion: the on-disk weather staging is stale and predates the Central Park settlement-aligned mapping change.

## Validation and baseline results

- `python3 -m kwb data validate-staging`: passed
- `python3 -m kwb research run-climatology-baseline --validate-staging --fail-fast-on-unready-staging`: completed with zero scored rows
- Latest run dir: `data/marts/research_runs/climatology_baseline_20260326T145933Z`

Reported outputs:

- `rows_scored=0`
- `decision-price trades_taken=0`
- `executable trades_taken=0`

## Targeted bottleneck counts

Using the latest backtest dataset and staged weather history:

- Backtest rows before weather join: `546`
- Rows with weather match: `546`
- Rows with normals match: `546`
- Rows surviving backtest dataset filters: `546`
- Unique event dates in backtest dataset: `91`
- Markets per event date: `6`
- Weather history rows available: `91`
- Weather history coverage: `2025-11-01` through `2026-01-30`

This shows the weather join is not the bottleneck. All rows survive the join.

## Climatology lookback diagnostics

For `day_window=1` and `min_lookback_samples=30`:

- Qualifying event dates: `0`
- Surviving scored rows: `0`
- Rows failing insufficient history: `546`

For wider seasonal windows, using the same stale history:

- `day_window=3`: qualifying event dates `0`, surviving scored rows `0`
- `day_window=7`: qualifying event dates `0`, surviving scored rows `0`

Observed day-window-1 lookback sizes by event date:

- First date `2025-11-01`: `0`
- Typical later dates: `1`

Conclusion: `day_window=1` is not the real blocker here. The blocker is that the history file contains only the same 91 dates being backtested, so there is effectively no prior-year climatology sample to score against.

## Exact next blocker

The next blocker is missing fresh NOAA rebuild inputs:

1. `NCEI_API_TOKEN` is not configured in the current shell, so weather and normals cannot be rebuilt.
2. Until those NOAA datasets are rebuilt, the baseline continues to use stale `KLGA`-based short-history files instead of the intended Central Park-aligned long-history files.

## Recommended next step

Set `NCEI_API_TOKEN` in the shell that runs `kwb`, then rerun the exact staging build with unrestricted network access. After that, re-check:

- weather schema matches the intended TMAX-only layout
- weather date coverage starts at or near `2015-01-01`
- staged station IDs switch from `KLGA` to the intended NOAA proxy for Central Park alignment
- baseline scored rows become non-zero
