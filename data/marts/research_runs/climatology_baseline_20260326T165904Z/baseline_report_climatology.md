# Climatology Baseline Report

- Model: `baseline_climatology_v1`
- Run timestamp (UTC): `2026-03-26T16:59:04.348700+00:00`
- Baseline status: `promising`
- Status note: candle_proxy stayed positive out of sample with enough folds and trade count.
- Staging ready: `True`

## Data Coverage

- Rows scored: `546`
- Date range: `2025-11-01` to `2026-01-30`
- Cities covered: `nyc`

## One-Shot Evaluation

### decision_price

- Trades taken: `108`
- Hit rate: `0.731481`
- Average edge at entry: `0.693354`
- Total net PnL: `73.39`

### candle_proxy

- Trades taken: `107`
- Hit rate: `0.738318`
- Average edge at entry: `0.696288`
- Total net PnL: `73.04`

## Walk-Forward Evaluation

### decision_price

- Folds scored: `3`
- Trades taken: `52`
- Hit rate: `0.692308`
- Average net PnL per trade: `0.639038`
- Total net PnL: `33.23`

### candle_proxy

- Folds scored: `3`
- Trades taken: `50`
- Hit rate: `0.7`
- Average net PnL per trade: `0.6528`
- Total net PnL: `32.64`
