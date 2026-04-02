# Climatology Friction Stress Test

- Source run: `/Users/faisalessa/kalshi-weather-bot/reports/time_of_day_sensitivity_20260401T000000Z/runs/1300`
- Walk-forward profile: `research_short`
- Weather-series fee override found: `False`

## Scenario Summary

| Scenario | Trades | Win Rate | Total PnL | Avg PnL/Trade | Avg Net Edge | Avg Gross Edge | Total Fees |
| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| A Executable No Fees | 156 | 0.0256 | 0.3900 | 0.0025 | 0.4492 | 0.4492 | 0.0000 |
| B Executable + Standard Taker Fees | 142 | 0.0282 | -0.8600 | -0.0061 | 0.4779 | 0.4881 | 1.4400 |
| C Taker Fees + Stricter Edge | 142 | 0.0282 | -0.8600 | -0.0061 | 0.4779 | 0.4881 | 1.4400 |
| D Taker Fees + Stricter Edge + Tight Spread | 134 | 0.0299 | 0.2300 | 0.0017 | 0.4734 | 0.4835 | 1.3500 |

## Strict-Edge Filtered Trades

- Trades removed when moving from Scenario B to Scenario C: `0`
