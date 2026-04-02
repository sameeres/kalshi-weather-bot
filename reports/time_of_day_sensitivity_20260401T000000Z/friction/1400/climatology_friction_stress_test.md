# Climatology Friction Stress Test

- Source run: `/Users/faisalessa/kalshi-weather-bot/reports/time_of_day_sensitivity_20260401T000000Z/runs/1400`
- Walk-forward profile: `research_short`
- Weather-series fee override found: `False`

## Scenario Summary

| Scenario | Trades | Win Rate | Total PnL | Avg PnL/Trade | Avg Net Edge | Avg Gross Edge | Total Fees |
| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| A Executable No Fees | 162 | 0.0309 | 1.5200 | 0.0094 | 0.4408 | 0.4408 | 0.0000 |
| B Executable + Standard Taker Fees | 148 | 0.0338 | 0.1900 | 0.0013 | 0.4671 | 0.4773 | 1.5100 |
| C Taker Fees + Stricter Edge | 148 | 0.0338 | 0.1900 | 0.0013 | 0.4671 | 0.4773 | 1.5100 |
| D Taker Fees + Stricter Edge + Tight Spread | 141 | 0.0284 | -0.1400 | -0.0010 | 0.4675 | 0.4777 | 1.4300 |

## Strict-Edge Filtered Trades

- Trades removed when moving from Scenario B to Scenario C: `0`
