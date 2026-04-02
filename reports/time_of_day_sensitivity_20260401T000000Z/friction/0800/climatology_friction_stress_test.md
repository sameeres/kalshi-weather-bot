# Climatology Friction Stress Test

- Source run: `/Users/faisalessa/kalshi-weather-bot/reports/time_of_day_sensitivity_20260401T000000Z/runs/0800`
- Walk-forward profile: `research_short`
- Weather-series fee override found: `False`

## Scenario Summary

| Scenario | Trades | Win Rate | Total PnL | Avg PnL/Trade | Avg Net Edge | Avg Gross Edge | Total Fees |
| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| A Executable No Fees | 142 | 0.0352 | -1.4400 | -0.0101 | 0.4682 | 0.4682 | 0.0000 |
| B Executable + Standard Taker Fees | 137 | 0.0365 | -2.6000 | -0.0190 | 0.4728 | 0.4833 | 1.4300 |
| C Taker Fees + Stricter Edge | 137 | 0.0365 | -2.6000 | -0.0190 | 0.4728 | 0.4833 | 1.4300 |
| D Taker Fees + Stricter Edge + Tight Spread | 118 | 0.0339 | -1.5600 | -0.0132 | 0.4694 | 0.4797 | 1.2200 |

## Strict-Edge Filtered Trades

- Trades removed when moving from Scenario B to Scenario C: `0`
