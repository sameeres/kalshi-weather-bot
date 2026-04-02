# Climatology Friction Stress Test

- Source run: `/Users/faisalessa/kalshi-weather-bot/reports/time_of_day_sensitivity_20260401T000000Z/runs/1100`
- Walk-forward profile: `research_short`
- Weather-series fee override found: `False`

## Scenario Summary

| Scenario | Trades | Win Rate | Total PnL | Avg PnL/Trade | Avg Net Edge | Avg Gross Edge | Total Fees |
| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| A Executable No Fees | 148 | 0.0270 | -0.0000 | -0.0000 | 0.4687 | 0.4687 | 0.0000 |
| B Executable + Standard Taker Fees | 141 | 0.0284 | -1.3500 | -0.0096 | 0.4790 | 0.4893 | 1.4400 |
| C Taker Fees + Stricter Edge | 141 | 0.0284 | -1.3500 | -0.0096 | 0.4790 | 0.4893 | 1.4400 |
| D Taker Fees + Stricter Edge + Tight Spread | 134 | 0.0224 | -1.3000 | -0.0097 | 0.4773 | 0.4873 | 1.3500 |

## Strict-Edge Filtered Trades

- Trades removed when moving from Scenario B to Scenario C: `0`
