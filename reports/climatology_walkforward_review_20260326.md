# Climatology Walk-Forward Review

Run reviewed: `data/marts/research_runs/climatology_baseline_20260326T165904Z`

## What changed

- Added an explicit walk-forward window profile system to the existing climatology walk-forward engine.
- Added a small-sample `research_short` expanding-window profile:
  - train: `30` unique event dates
  - validation: `15`
  - test: `15`
  - step: `15`
- Wired the profile through the standalone walk-forward CLI and the full climatology research runner.
- Added tests covering profile selection and runner propagation.

## Why this profile

The previous default walk-forward windows (`60/30/30`) could not score any folds on a `91`-day sample.

This profile keeps the evaluation honest:

- folds are based on ordered trade dates
- training stays strictly before validation
- validation stays strictly before test
- training is expanding, never rolling in future information

## Folds scored

Total folds scored: `3` in both pricing modes.

### Fold ranges

Fold 1:

- Train: `2025-11-01` to `2025-11-30`
- Validation: `2025-12-01` to `2025-12-15`
- Test: `2025-12-16` to `2025-12-30`

Fold 2:

- Train: `2025-11-01` to `2025-12-15`
- Validation: `2025-12-16` to `2025-12-30`
- Test: `2025-12-31` to `2026-01-14`

Fold 3:

- Train: `2025-11-01` to `2025-12-30`
- Validation: `2025-12-31` to `2026-01-14`
- Test: `2026-01-15` to `2026-01-29`

## Fold-by-fold results

### Decision-price

| Fold | Test Range | Rows Scored | Trades | Win Rate | Avg Edge | Total PnL | Avg PnL/Trade |
| --- | --- | ---: | ---: | ---: | ---: | ---: | ---: |
| 1 | 2025-12-16 to 2025-12-30 | 90 | 16 | 68.75% | 0.7053 | 10.155 | 0.6347 |
| 2 | 2025-12-31 to 2026-01-14 | 90 | 17 | 64.71% | 0.6089 | 10.100 | 0.5941 |
| 3 | 2026-01-15 to 2026-01-29 | 90 | 19 | 73.68% | 0.6241 | 12.975 | 0.6829 |

Selected thresholds:

- Fold 1: `min_edge=0.00`
- Fold 2: `min_edge=0.05`
- Fold 3: `min_edge=0.05`

### Executable / candle_proxy

| Fold | Test Range | Rows Scored | Trades | Win Rate | Avg Edge | Total PnL | Avg PnL/Trade |
| --- | --- | ---: | ---: | ---: | ---: | ---: | ---: |
| 1 | 2025-12-16 to 2025-12-30 | 90 | 15 | 73.33% | 0.7456 | 10.120 | 0.6747 |
| 2 | 2025-12-31 to 2026-01-14 | 90 | 17 | 64.71% | 0.6061 | 10.490 | 0.6171 |
| 3 | 2026-01-15 to 2026-01-29 | 90 | 18 | 72.22% | 0.6553 | 12.030 | 0.6683 |

Selected thresholds:

- Fold 1: `min_edge=0.02`, `max_spread=5.0`
- Fold 2: `min_edge=0.05`
- Fold 3: `min_edge=0.05`

## Aggregated out-of-sample metrics

### Decision-price

- Rows evaluated: `270`
- Trades taken: `52`
- Win rate: `69.23%`
- Total PnL: `33.23`
- Average PnL/trade: `0.6390`
- Average edge at entry: `0.6441`
- Average model probability on traded rows: `0.6973`

### Executable

- Rows evaluated: `270`
- Trades taken: `50`
- Win rate: `70.00%`
- Total PnL: `32.64`
- Average PnL/trade: `0.6528`
- Average edge at entry: `0.6656`
- Average model probability on traded rows: `0.7128`

## Consistency vs concentration

This is better than the earlier one-shot-only picture because:

- all three folds are positive in both pricing modes
- no single fold accounts for all of the PnL
- executable results remain close to decision-price results

But the edge is still narrow.

### Cheap YES bias

Out-of-sample entry-price mix:

- Decision-price: `50 / 52` trades in the `0-25` cent bucket
- Executable: `49 / 50` trades in the `0-25` cent bucket

The few `25-50` cent trades were bad:

- Decision-price: `2` trades, `-0.83` total PnL
- Executable: `1` trade, `-0.38` total PnL

### Bucket-type concentration

Decision-price:

- `or_below`: `28` trades, `26.235` total PnL, `96.43%` win rate
- `or_above`: `9` trades, `5.515` total PnL, `66.67%` win rate
- `between`: `15` trades, `1.480` total PnL, `20.00%` win rate

Executable:

- `or_below`: `29` trades, `26.170` total PnL, `93.10%` win rate
- `or_above`: `8` trades, `5.590` total PnL, `75.00%` win rate
- `between`: `13` trades, `0.880` total PnL, `15.38%` win rate

This means the cheap cold-tail YES story still explains most of the profit. The “between” buckets remain weak and close to noise.

## Conclusion

This baseline looks stronger than before, but not fully robust yet.

What improved:

- the edge survives honest out-of-sample slicing across `3` folds
- results stay positive under executable candle-proxy pricing
- performance is not dominated by a single test fold

What still worries me:

- the signal is still mostly “buy very cheap YES in tail buckets”
- there is no meaningful NO-side evidence
- the “between” buckets are still poor
- this is still only NYC and one winter slice

Bottom line:

The baseline no longer looks like pure one-shot luck. There is probably some real signal here.

But it still looks fragile and highly concentrated, not broad or production-ready.

## Recommended next step

Keep the climatology model fixed and make the evaluation harsher before changing the model.

Highest-value next step:

- run the same walk-forward with non-zero fees and a stricter executable filter set
- then compare performance separately for `or_below`, `or_above`, and `between` buckets

If the positive out-of-sample result survives fees and still holds outside the ultra-cheap tail contracts, the case for a real edge gets much stronger.
