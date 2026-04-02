# Climatology Friction Stress Test

- Source run: `/Users/faisalessa/kalshi-weather-bot/reports/time_of_day_sensitivity_20260401T000000Z/runs/0900`
- Walk-forward profile: `research_short`
- Weather-series fee override found: `False`

## Scenario Summary

| Scenario | Trades | Win Rate | Total PnL | Avg PnL/Trade | Avg Net Edge | Avg Gross Edge | Total Fees |
| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| A Executable No Fees | 144 | 0.0278 | -1.3700 | -0.0095 | 0.4713 | 0.4713 | 0.0000 |
| B Executable + Standard Taker Fees | 138 | 0.0290 | -2.4600 | -0.0178 | 0.4790 | 0.4894 | 1.4300 |
| C Taker Fees + Stricter Edge | 138 | 0.0290 | -2.4600 | -0.0178 | 0.4790 | 0.4894 | 1.4300 |
| D Taker Fees + Stricter Edge + Tight Spread | 130 | 0.0231 | -2.1200 | -0.0163 | 0.4878 | 0.4980 | 1.3300 |

## Strict-Edge Filtered Trades

- Trades removed when moving from Scenario B to Scenario C: `0`
