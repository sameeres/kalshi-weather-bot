# Climatology Baseline Report

- Model: `baseline_climatology_v1`
- Run timestamp (UTC): `2026-03-31T22:56:13.316183+00:00`
- Baseline status: `weak`
- Status note: candle_proxy walk-forward total net PnL was not positive.
- Staging ready: `None`

## Data Coverage

- Rows scored: `1092`
- Date range: `2025-11-01` to `2026-01-30`
- Cities covered: `chicago, nyc`

## One-Shot Evaluation

### candle_proxy

- Trades taken: `426`
- Hit rate: `0.023474`
- Average edge at entry: `0.327316`
- Total net PnL: `-2.25`

## Walk-Forward Evaluation

### candle_proxy

- Folds scored: `3`
- Trades taken: `144`
- Hit rate: `0.034722`
- Average net PnL per trade: `-0.001528`
- Total net PnL: `-0.22`
