# Climatology Baseline Report

- Model: `baseline_climatology_v1`
- Run timestamp (UTC): `2026-03-31T22:54:59.703958+00:00`
- Baseline status: `weak`
- Status note: candle_proxy walk-forward total net PnL was not positive.
- Staging ready: `None`

## Data Coverage

- Rows scored: `1092`
- Date range: `2025-11-01` to `2026-01-30`
- Cities covered: `chicago, nyc`

## One-Shot Evaluation

### candle_proxy

- Trades taken: `404`
- Hit rate: `0.022277`
- Average edge at entry: `0.336414`
- Total net PnL: `-5.67`

## Walk-Forward Evaluation

### candle_proxy

- Folds scored: `3`
- Trades taken: `142`
- Hit rate: `0.035211`
- Average net PnL per trade: `-0.010141`
- Total net PnL: `-1.44`
