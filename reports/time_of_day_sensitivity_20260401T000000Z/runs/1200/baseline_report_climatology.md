# Climatology Baseline Report

- Model: `baseline_climatology_v1`
- Run timestamp (UTC): `2026-03-31T22:57:50.665449+00:00`
- Baseline status: `weak`
- Status note: candle_proxy walk-forward total net PnL was not positive.
- Staging ready: `None`

## Data Coverage

- Rows scored: `1092`
- Date range: `2025-11-01` to `2026-01-30`
- Cities covered: `chicago, nyc`

## One-Shot Evaluation

### candle_proxy

- Trades taken: `447`
- Hit rate: `0.013423`
- Average edge at entry: `0.319047`
- Total net PnL: `-3.27`

## Walk-Forward Evaluation

### candle_proxy

- Folds scored: `3`
- Trades taken: `157`
- Hit rate: `0.025478`
- Average net PnL per trade: `-0.003121`
- Total net PnL: `-0.49`
