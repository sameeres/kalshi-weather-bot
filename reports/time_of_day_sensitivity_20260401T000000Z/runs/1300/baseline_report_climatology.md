# Climatology Baseline Report

- Model: `baseline_climatology_v1`
- Run timestamp (UTC): `2026-03-31T22:58:25.696274+00:00`
- Baseline status: `inconclusive`
- Status note: candle_proxy stayed positive, but evidence is still thin for a strong claim.
- Staging ready: `None`

## Data Coverage

- Rows scored: `1092`
- Date range: `2025-11-01` to `2026-01-30`
- Cities covered: `chicago, nyc`

## One-Shot Evaluation

### candle_proxy

- Trades taken: `469`
- Hit rate: `0.012793`
- Average edge at entry: `0.307797`
- Total net PnL: `-2.44`

## Walk-Forward Evaluation

### candle_proxy

- Folds scored: `3`
- Trades taken: `156`
- Hit rate: `0.025641`
- Average net PnL per trade: `0.0025`
- Total net PnL: `0.39`
