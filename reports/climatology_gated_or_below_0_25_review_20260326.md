## Gated Climatology Review

Run bundle:
- `data/marts/research_runs/climatology_baseline_20260326T192819Z`

Friction bundle:
- `reports/climatology_friction_stress_20260326T193321Z`

Gate applied to executable walk-forward trades:
- `contract_type == "or_below"`
- `chosen_side == "yes"`
- `entry_price_bucket == "0-25"`

Eligible out-of-sample rows:

| Fold | Chicago | NYC |
| --- | ---: | ---: |
| 1 | 12 | 15 |
| 2 | 15 | 15 |
| 3 | 14 | 14 |
| Total | 41 | 44 |

Scenario B, taker fees:

| City | Trades | Total PnL | Avg PnL | Notes |
| --- | ---: | ---: | ---: | --- |
| Chicago | 30 | 1.51 | 0.0503 | Positive overall, but fold 2 is negative |
| NYC | 32 | 0.93 | 0.0291 | Positive overall, but fold 3 is negative |
| Pooled | 62 | 2.44 | 0.0394 | Positive after fees |

Scenario D, taker fees + strict edge + tight spread:

| City | Trades | Total PnL | Avg PnL | Notes |
| --- | ---: | ---: | ---: | --- |
| Chicago | 28 | 1.70 | 0.0607 | Best filtered result, still not positive every fold |
| NYC | 30 | 1.04 | 0.0347 | Positive overall, still one losing fold |
| Pooled | 58 | 2.74 | 0.0472 | Positive after the tightest modeled filter |

Interpretation:
- The broad two-city strategy is weak, but this narrow gated slice is materially better.
- Both cities remain positive after standard taker fees on this gate.
- Neither city is positive fold by fold, so this is still a narrow, somewhat unstable edge.
- Trade frequency remains modest but usable for paper trading: about `0.62` to `0.71` trades per test day per city in the historical sample.
- This is strong enough for paper trading, but still not strong enough for confident live deployment on the current executable proxy alone.

Recommendation:
- Near-term pilot: `paper-only`
- Watchlist cities: `NYC` and `Chicago`
- Live trading: wait for either more forward evidence or better execution realism
