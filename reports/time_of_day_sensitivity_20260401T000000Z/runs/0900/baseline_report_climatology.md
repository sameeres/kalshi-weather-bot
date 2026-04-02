# Climatology Baseline Report

- Model: `baseline_climatology_v1`
- Run timestamp (UTC): `2026-03-31T22:55:35.891331+00:00`
- Baseline status: `weak`
- Status note: candle_proxy walk-forward total net PnL was not positive.
- Staging ready: `None`

## Data Coverage

- Rows scored: `1092`
- Date range: `2025-11-01` to `2026-01-30`
- Cities covered: `chicago, nyc`

## One-Shot Evaluation

### candle_proxy

- Trades taken: `417`
- Hit rate: `0.019185`
- Average edge at entry: `0.330834`
- Total net PnL: `-4.95`

## Walk-Forward Evaluation

### candle_proxy

- Folds scored: `3`
- Trades taken: `144`
- Hit rate: `0.027778`
- Average net PnL per trade: `-0.009514`
- Total net PnL: `-1.37`
