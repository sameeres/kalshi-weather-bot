# Climatology Friction Stress Test

- Source run: `/Users/faisalessa/kalshi-weather-bot/data/marts/research_runs/climatology_baseline_20260326T192819Z`
- Walk-forward profile: `research_short`
- Weather-series fee override found: `False`

## Scenario Summary

| Scenario | Trades | Win Rate | Total PnL | Avg PnL/Trade | Avg Net Edge | Avg Gross Edge | Total Fees |
| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| A Executable No Fees | 144 | 0.0347 | -0.2200 | -0.0015 | 0.4743 | 0.4743 | 0.0000 |
| B Executable + Standard Taker Fees | 140 | 0.0357 | -1.5600 | -0.0111 | 0.4759 | 0.4863 | 1.4600 |
| C Taker Fees + Stricter Edge | 140 | 0.0357 | -1.5600 | -0.0111 | 0.4759 | 0.4863 | 1.4600 |
| D Taker Fees + Stricter Edge + Tight Spread | 130 | 0.0385 | -0.2100 | -0.0016 | 0.4835 | 0.4939 | 1.3500 |

## Strict-Edge Filtered Trades

- Trades removed when moving from Scenario B to Scenario C: `0`
