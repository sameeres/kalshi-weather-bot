# Climatology Baseline Report

- Model: `baseline_climatology_v1`
- Run timestamp (UTC): `2026-03-26T19:28:19.190396+00:00`
- Baseline status: `weak`
- Status note: candle_proxy walk-forward total net PnL was not positive.
- Staging ready: `True`

## Data Coverage

- Rows scored: `1092`
- Date range: `2025-11-01` to `2026-01-30`
- Cities covered: `chicago, nyc`

## One-Shot Evaluation

### decision_price

- Trades taken: `434`
- Hit rate: `0.023041`
- Average edge at entry: `0.32579`
- Total net PnL: `-0.825`

### candle_proxy

- Trades taken: `426`
- Hit rate: `0.023474`
- Average edge at entry: `0.327316`
- Total net PnL: `-2.25`

## Walk-Forward Evaluation

### decision_price

- Folds scored: `3`
- Trades taken: `150`
- Hit rate: `0.033333`
- Average net PnL per trade: `0.0009`
- Total net PnL: `0.135`

### candle_proxy

- Folds scored: `3`
- Trades taken: `144`
- Hit rate: `0.034722`
- Average net PnL per trade: `-0.001528`
- Total net PnL: `-0.22`
