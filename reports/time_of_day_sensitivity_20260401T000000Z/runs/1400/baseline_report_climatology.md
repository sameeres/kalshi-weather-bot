# Climatology Baseline Report

- Model: `baseline_climatology_v1`
- Run timestamp (UTC): `2026-03-31T22:59:00.623829+00:00`
- Baseline status: `inconclusive`
- Status note: candle_proxy stayed positive, but evidence is still thin for a strong claim.
- Staging ready: `None`

## Data Coverage

- Rows scored: `1092`
- Date range: `2025-11-01` to `2026-01-30`
- Cities covered: `chicago, nyc`

## One-Shot Evaluation

### candle_proxy

- Trades taken: `494`
- Hit rate: `0.012146`
- Average edge at entry: `0.296647`
- Total net PnL: `-2.18`

## Walk-Forward Evaluation

### candle_proxy

- Folds scored: `3`
- Trades taken: `162`
- Hit rate: `0.030864`
- Average net PnL per trade: `0.009383`
- Total net PnL: `1.52`
