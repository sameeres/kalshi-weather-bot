# Kalshi Weather Bot

A VSCode-ready starter repo for researching weather-based Kalshi prediction market strategies.

## MVP

- 3 cities max to start
- daily high temperature markets only
- backtest before any live execution
- exact settlement-source mapping first

## What is included

- Python package scaffold under `src/kwb`
- config-driven city setup
- Kalshi public metadata client
- NOAA/NWS client stubs
- DuckDB/Parquet-oriented project layout
- starter ingestion and backtest skeletons
- VSCode launch/tasks/settings files

## Quick start

1. Create and activate a virtual environment.
2. Install the package in editable mode:

```bash
pip install -e .
```

3. Copy `.env.example` to `.env` and update values if needed.
4. Run the starter CLI:

```bash
python -m kwb --help
python -m kwb cities list
```

## Suggested first build order

1. Fill out `configs/cities.yml`
2. Run `python -m kwb cities list`
3. Implement and test Kalshi event metadata ingestion
4. Build settlement-source mapping
5. Add historical weather ingestion
6. Build the first baseline backtest

## Notes

This repo intentionally starts with **research and backtesting only**. Do not add automated order placement until the settlement mapping, pricing logic, and fill assumptions are validated.
