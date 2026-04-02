# Climatology Friction Stress Test

- Source run: `/Users/faisalessa/kalshi-weather-bot/reports/time_of_day_sensitivity_20260401T000000Z/runs/1200`
- Walk-forward profile: `research_short`
- Weather-series fee override found: `False`

## Scenario Summary

| Scenario | Trades | Win Rate | Total PnL | Avg PnL/Trade | Avg Net Edge | Avg Gross Edge | Total Fees |
| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| A Executable No Fees | 157 | 0.0255 | -0.4900 | -0.0031 | 0.4443 | 0.4443 | 0.0000 |
| B Executable + Standard Taker Fees | 143 | 0.0280 | -1.5200 | -0.0106 | 0.4721 | 0.4823 | 1.4600 |
| C Taker Fees + Stricter Edge | 143 | 0.0280 | -1.5200 | -0.0106 | 0.4721 | 0.4823 | 1.4600 |
| D Taker Fees + Stricter Edge + Tight Spread | 134 | 0.0224 | -1.0400 | -0.0078 | 0.4741 | 0.4842 | 1.3500 |

## Strict-Edge Filtered Trades

- Trades removed when moving from Scenario B to Scenario C: `0`
