# Climatology Friction Stress Test

- Source run: `/Users/faisalessa/kalshi-weather-bot/data/marts/research_runs/climatology_baseline_20260326T165904Z`
- Walk-forward profile: `research_short`
- Weather-series fee override found: `False`

## Scenario Summary

| Scenario | Trades | Win Rate | Total PnL | Avg PnL/Trade | Avg Net Edge | Avg Gross Edge | Total Fees |
| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| A Executable No Fees | 50 | 0.7000 | 32.6400 | 0.6528 | 0.6656 | 0.6656 | 0.0000 |
| B Executable + Standard Taker Fees | 49 | 0.7143 | 32.1200 | 0.6555 | 0.6680 | 0.6788 | 0.5300 |
| C Taker Fees + Stricter Edge | 46 | 0.7609 | 32.2300 | 0.7007 | 0.7087 | 0.7195 | 0.5000 |
| D Taker Fees + Stricter Edge + Tight Spread | 42 | 0.7857 | 30.9900 | 0.7379 | 0.7256 | 0.7361 | 0.4400 |

## Strict-Edge Filtered Trades

- Trades removed when moving from Scenario B to Scenario C: `3`
