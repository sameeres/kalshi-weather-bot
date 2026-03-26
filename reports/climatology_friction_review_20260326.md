## Climatology Friction Review

Source run bundle: `data/marts/research_runs/climatology_baseline_20260326T165904Z`

Stress-test bundle: `reports/climatology_friction_stress_20260326T175501Z`

### Modeled assumptions

- Official and directly modeled:
  - Kalshi standard taker fee per trade: `ceil_to_cent(0.07 * C * P * (1 - P))`
  - Executable buys at ask using binary reciprocity from stored quotes
  - No settlement fee
- Inferred from available market data:
  - Entry spread filter using stored executable spread
- Unknown or not modelable from available data:
  - No stored orderbook depth to walk the book
  - No verified weather-series fee override in repo/data
  - No fill-probability model for resting orders
  - No deeper market-impact estimate beyond top-of-book sensitivity

### Scenario summary

| Scenario | Trades | Win rate | Total net PnL | Avg net PnL/trade | Avg gross edge | Avg net edge | Avg fee |
| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| A: Executable no fees | 50 | 70.00% | 32.64 | 0.6528 | 0.6656 | 0.6656 | 0.0000 |
| B: + standard taker fees | 49 | 71.43% | 32.12 | 0.6555 | 0.6788 | 0.6680 | 0.0108 |
| C: + taker fees + stricter edge | 46 | 76.09% | 32.23 | 0.7007 | 0.7195 | 0.7087 | 0.0109 |
| D: + taker fees + stricter edge + tight spread | 42 | 78.57% | 30.99 | 0.7379 | 0.7361 | 0.7256 | 0.0105 |

### Main takeaways

- Official taker fees reduce, but do not eliminate, the observed edge.
- Fees alone removed about `0.53` total PnL versus the no-fee executable case and one tiny-edge trade no longer cleared the threshold.
- The edge remains narrow:
  - Scenario B `or_below`: `29` trades, `25.87` PnL
  - Scenario B `between`: `12` trades, `0.75` PnL
- Tightening the edge filter helps quality more than quantity:
  - Scenario C keeps similar total PnL to B with fewer trades and higher win rate
  - Scenario D improves average trade quality further but trims total PnL

### Trades removed by stricter edge

- `KXHIGHNY-25DEC31-T31` (`30° or below`), gross edge `0.0525`, net edge after fee `0.0425`
- `KXHIGHNY-26JAN01-B30.5` (`30° to 31°`), gross edge `0.0506`, net edge after fee `0.0406`
- `KXHIGHNY-26JAN04-B31.5` (`31° to 32°`), gross edge `0.0576`, net edge after fee `0.0476`

### Conclusion

This baseline still has positive out-of-sample executable PnL after official Kalshi taker fees, so the signal is not fully fee-arbitraged away. But it remains concentrated in cheap `YES` tail trades, especially `or_below`, and `between` contracts still look weak enough that they are probably not worth trusting yet.
