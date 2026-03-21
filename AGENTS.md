# AGENTS.md

This repository is for building a **Kalshi weather-market research and trading system**.

Read this file before making changes.

## Project objective

Build an algorithm that:
1. pulls Kalshi daily temperature-high markets for selected cities,
2. maps each market to its exact settlement source and station,
3. compares market-implied probabilities with model-implied probabilities derived from historical weather data plus current forecast information,
4. identifies potentially positive-EV opportunities,
5. backtests the strategy conservatively before any live trading.

## Current stage

This project is in the **research / MVP** stage.

We are **not** building a production auto-trader yet.
We are **not** placing live orders yet.
We are **not** optimizing across many cities yet.

The immediate focus is:
- 3 cities maximum
- daily high temperature markets only
- one decision time per day
- backtesting first
- exact settlement mapping first

## Non-negotiable rules

1. **Settlement accuracy comes first.**
   Weather contracts must be tied to the exact Kalshi settlement source and the exact weather station used for settlement.
   Never substitute a generic weather app or a nearby station without making that explicit.

2. **Backtests must be point-in-time honest.**
   Do not use data that would not have been known at the decision timestamp.
   Do not use final revised observations or future forecasts as if they were available earlier.

3. **Use conservative execution assumptions.**
   Prefer executable bid/ask logic over midpoint assumptions.
   Include spreads, fees, and slippage buffers.

4. **Keep the MVP narrow.**
   Do not add live trading, portfolio optimization, or excessive infrastructure until the baseline research works.

5. **Make small, reviewable changes.**
   Prefer incremental edits over large rewrites.
   If touching multiple modules, preserve a clean separation of concerns.

## Technical preferences

- Language: Python
- Workspace: VSCode
- Storage: DuckDB + Parquet for research and backtesting
- Package layout: `src/kwb/...`
- Config-driven city setup in `configs/`
- Keep raw / staging / marts separation in `data/`
- Write code that is easy to inspect and test

## Modeling philosophy

The baseline idea of “compare current line to historical highs” is **only a starting point**.

The real target is to estimate the **distribution** of the exact settlement temperature.
That means:
- climatology is a prior,
- forecast information shifts or sharpens that prior,
- contract bucket probabilities come from the modeled distribution,
- trades are only valid if edge survives fees and fill assumptions.

Start with simple models before doing anything fancy.

### Model ladder

1. **Baseline A: Climatology only**
   Same station, same part of year, historical realized highs.

2. **Baseline B: Forecast-shifted climatology**
   Shift historical distribution using current forecast anomaly.

3. **Baseline C: Forecast error calibration**
   Model residual uncertainty around the forecast.

Do not jump to ML-heavy approaches before these work.

## Data priorities

When choosing what to implement next, follow this order:

1. settlement-source discovery and storage
2. city-to-station mapping
3. historical realized weather ingestion
4. Kalshi historical event / market / quote ingestion
5. baseline probability model
6. conservative backtest engine
7. only later, execution logic

## File-specific guidance

### `configs/cities.yml`
This is the source of truth for enabled cities.
Keep it explicit and human-readable.

### `src/kwb/clients/`
API clients should stay thin and focused on transport / response parsing.
Do not bury business logic here.

### `src/kwb/ingestion/`
Ingestion modules should fetch, normalize, and save data.
Avoid mixing ingestion with modeling.

### `src/kwb/mapping/`
Put settlement-source extraction and station-mapping logic here.
This layer is critical.

### `src/kwb/models/`
Models should output probabilities or distribution parameters in a way that is easy to test.

### `src/kwb/backtest/`
Backtest code should be conservative, explicit, and reproducible.

## Coding standards

- Use type hints where practical.
- Prefer readable code over clever code.
- Add docstrings on non-trivial public functions.
- Avoid hidden magic and global state.
- Log important ingestion steps.
- Fail loudly on schema mismatches.

## Testing expectations

At minimum, maintain tests for:
- bucket probability logic
- settlement-window/date logic
- fill logic / trade accounting

If you change logic in those areas, update tests in the same edit.

## How to work in this repo

Before editing:
1. read `AGENTS.md`
2. read `README.md`
3. read `docs/PROJECT_CONTEXT.md`
4. inspect the relevant module before changing it

When responding with a plan or a commit summary:
- say what you changed,
- say why it matters,
- note any assumptions or open questions,
- avoid unnecessary verbosity.

## Immediate next task

Unless the user says otherwise, the highest-priority task is:

**Implement `src/kwb/ingestion/kalshi_events.py` end-to-end and wire settlement-source fields into `configs/cities.yml` / staging outputs so the repo can discover and store authoritative settlement metadata for enabled cities.**

