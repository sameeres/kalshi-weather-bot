# Climatology Baseline Run Review

Run reviewed: `data/marts/research_runs/climatology_baseline_20260326T151436Z`

## Files inspected

- `baseline_report_climatology.json`
- `baseline_report_climatology.md`
- `backtest_summary_climatology.json`
- `backtest_summary_climatology_executable.json`
- `backtest_comparison_climatology_pricing.json`
- `backtest_dataset.parquet`
- `backtest_scored_climatology.parquet`
- `backtest_trades_climatology.parquet`
- `backtest_trades_climatology_executable.parquet`
- `research_manifest_climatology.json`

## Headline take

The first one-shot baseline run is encouraging as a smoke test, but it is not yet evidence of durable signal.

Why:

- one-shot PnL is strongly positive in both pricing modes
- but the report still marks the baseline as `weak`
- there are no scored walk-forward folds yet
- the sample is only one city and one season fragment (`2025-11-01` through `2026-01-30`)
- almost all profit comes from buying very cheap YES on extreme cold-tail buckets that mostly resolved true in November

This looks more like a promising research starting point than a validated edge.

## Key metrics

### Decision-price

- Rows scored: `546`
- Trades taken: `108`
- Win rate: `73.15%`
- Total PnL: `73.39`
- Average PnL per trade: `0.6795`
- Average edge at entry: `0.6934`
- Average `model_prob_yes` on traded rows: `0.7453`
- Average `fair_yes` on traded rows: `0.7453`

### Executable

- Rows scored: `546`
- Trades taken: `107`
- Win rate: `73.83%`
- Total PnL: `73.04`
- Average PnL per trade: `0.6826`
- Average edge at entry: `0.6963`
- Average `model_prob_yes` on traded rows: `0.7520`
- Average `fair_yes` on traded rows: `0.7520`

## Breakdown

### By month

Decision-price:

- `2025-11`: `30` trades, `100%` win rate, `29.245` total PnL
- `2025-12`: `35` trades, `65.71%` win rate, `21.190` total PnL
- `2026-01`: `43` trades, `60.47%` win rate, `22.955` total PnL

Executable:

- `2025-11`: `30` trades, `100%` win rate, `29.14` total PnL
- `2025-12`: `35` trades, `65.71%` win rate, `21.08` total PnL
- `2026-01`: `42` trades, `61.90%` win rate, `22.82` total PnL

### By side

Both modes only took `YES` trades.

- Decision-price: `108 YES`, `0 NO`
- Executable: `107 YES`, `0 NO`

That means the current setup is not testing whether the baseline can profitably express the opposite side.

### By bucket

The strongest buckets were extreme low-tail contracts, especially "`X° or below`" buckets priced near zero that resolved true.

Largest positive bucket groups in decision-price mode:

- `39° or below`: `6` trades, `5.89` total PnL
- `45° or below`: `5` trades, `4.96`
- `40° or below`: `5` trades, `4.955`
- `33° or below`: `5` trades, `4.82`

Weakest bucket groups in decision-price mode:

- `33° to 34°`: `10` trades, `-2.15`
- `34° to 35°`: `5` trades, `-0.24`
- `33° or above`: `1` trade, `-0.07`

Executable mode tells the same story:

- best bucket group: `39° or below`, `5.87`
- worst bucket group: `33° to 34°`, `-2.26`

So the current edge is not broad-based. It is strongest in obvious low-price tail contracts and weak in mid-range “between” buckets.

## Why 108 trades became 107

The only trade present in decision-price but absent in executable mode was:

- `2026-01-03`
- market: `KXHIGHNY-26JAN03-B35.5`
- bucket: `35° to 36°`

Decision-price logic traded it because:

- close price was `2` cents
- model probability was `2.9412%`
- decision-price edge was `+0.009412`

Executable logic dropped it because:

- conservative YES entry uses the ask, not the close
- `yes_ask` was `3` cents
- executable edge became `0.029412 - 0.03 = -0.000588`

So the extra trade disappears because a tiny positive edge at the close is not actually executable once you pay the ask.

## Best and worst trades

### Top 5 decision-price trades

- `2025-11-01` `KXHIGHNY-25NOV01-T53` `52° or below`: entry `0.5`, model prob `1.0`, actual `34.592F`, PnL `+0.995`
- `2025-11-03` `KXHIGHNY-25NOV03-T57` `56° or below`: entry `0.5`, model prob `1.0`, actual `34.700F`, PnL `+0.995`
- `2025-11-05` `KXHIGHNY-25NOV05-T60` `59° or below`: entry `0.5`, model prob `1.0`, actual `35.402F`, PnL `+0.995`
- `2025-11-07` `KXHIGHNY-25NOV07-T55` `54° or below`: entry `0.5`, model prob `1.0`, actual `34.898F`, PnL `+0.995`
- `2025-11-10` `KXHIGHNY-25NOV10-T55` `54° or below`: entry `0.5`, model prob `1.0`, actual `34.700F`, PnL `+0.995`

### Worst 5 decision-price trades

- `2026-01-04` `KXHIGHNY-26JAN04-B33.5` `33° to 34°`: entry `45`, model prob `0.5000`, actual `32.306F`, PnL `-0.45`
- `2026-01-03` `KXHIGHNY-26JAN03-B33.5` `33° to 34°`: entry `44`, model prob `0.4706`, actual `31.802F`, PnL `-0.44`
- `2026-01-16` `KXHIGHNY-26JAN16-B33.5` `33° to 34°`: entry `38`, model prob `0.4706`, actual `32.198F`, PnL `-0.38`
- `2026-01-21` `KXHIGHNY-26JAN21-B33.5` `33° to 34°`: entry `34`, model prob `0.3824`, actual `32.792F`, PnL `-0.34`
- `2026-01-02` `KXHIGHNY-26JAN02-B32.5` `32° to 33°`: entry `25`, model prob `0.2941`, actual `31.802F`, PnL `-0.25`

### Worst 5 executable trades

- `2026-01-04` `KXHIGHNY-26JAN04-B33.5`: ask entry `48`, fair prob `0.5000`, actual `32.306F`, PnL `-0.48`
- `2026-01-03` `KXHIGHNY-26JAN03-B33.5`: ask entry `47`, fair prob `0.4706`, actual `31.802F`, PnL `-0.47`
- `2026-01-16` `KXHIGHNY-26JAN16-B33.5`: ask entry `38`, fair prob `0.4706`, actual `32.198F`, PnL `-0.38`
- `2026-01-21` `KXHIGHNY-26JAN21-B33.5`: ask entry `34`, fair prob `0.3824`, actual `32.792F`, PnL `-0.34`
- `2026-01-02` `KXHIGHNY-26JAN02-B32.5`: ask entry `27`, fair prob `0.2941`, actual `31.802F`, PnL `-0.27`

## Assessment

This baseline does not yet look proven or robust enough to call promising in a trading sense.

It does show:

- the pipeline is functioning end to end
- the model can score real rows and generate coherent trades
- the positive one-shot result survives a conservative executable proxy with only minor degradation

But it does not yet show:

- out-of-sample stability
- multi-fold walk-forward performance
- side diversity
- robustness outside one city and one cold-season window
- resilience once fees and stricter spread filters matter

## Next highest-value improvement

Implement a true walk-forward-ready evaluation window that can score at least a few folds, then rerun the same baseline before changing the model class.

If the current fold configuration is too long for the available sample, the next best step is to add a shorter research-only walk-forward configuration so we can test whether this apparent edge survives even basic out-of-sample slicing.
