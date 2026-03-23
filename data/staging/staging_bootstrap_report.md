# Staging Bootstrap Report

- Checked at (UTC): `2026-03-22T23:41:18.222128+00:00`
- Ready for baseline runner: `False`
- Config path: `/Users/faisalessa/kalshi-weather-bot/configs/cities.yml`
- Staging dir: `data/staging`
- Station mapping ready: `True`

## Datasets

### weather_daily

- File: `data/staging/weather_daily.parquet`
- Exists: `True`
- Row count: `91`
- Date coverage: `2025-11-01` to `2026-01-30`
- Source of truth: Enabled-city settlement station mapping from configs/cities.yml, then NOAA NCEI daily station observations for the mapped settlement station.
- Builder: `kwb.ingestion.weather_history.ingest_weather_history_for_enabled_cities`

### weather_normals_daily

- File: `data/staging/weather_normals_daily.parquet`
- Exists: `True`
- Row count: `365`
- Date coverage: `01-01` to `12-31`
- Source of truth: Enabled-city settlement station mapping from configs/cities.yml, then NOAA NCEI daily climate normals for the mapped settlement station.
- Builder: `kwb.ingestion.climate_normals.ingest_climate_normals_for_enabled_cities`

### kalshi_markets

- File: `data/staging/kalshi_markets.parquet`
- Exists: `True`
- Row count: `8512`
- Date coverage: `None` to `None`
- Source of truth: Enabled Kalshi series tickers from configs/cities.yml, then Kalshi series event and market metadata retrieved through the public API.
- Builder: `kwb.ingestion.kalshi_market_history.ingest_kalshi_market_history_for_enabled_cities`
- Errors: `Column 'strike_date' contains invalid datetime values.`

### kalshi_candles

- File: `data/staging/kalshi_candles.parquet`
- Exists: `True`
- Row count: `32321`
- Date coverage: `2025-11-01T00:00:00+0000` to `2026-03-22T23:00:00+0000`
- Source of truth: Enabled Kalshi series tickers from configs/cities.yml, then Kalshi candlestick history for each discovered market ticker.
- Builder: `kwb.ingestion.kalshi_market_history.ingest_kalshi_market_history_for_enabled_cities`

## Cross-Dataset Checks

- `ok` Enabled cities in config: nyc.
- `error` Candles are missing for 7648 staged market tickers.

## Kalshi Chunk Progress

- Status: `completed`
- Manifest: `data/staging/kalshi_history_manifest.json`
- Chunk dir: `data/staging/kalshi_history_chunks`
- Completed market chunks: `1`
- Completed candle chunks: `8512`
- Failed candle chunks: `0`
- Retries used: `839`

## Environment

- NCEI_API_TOKEN configured: `True`

## Local Raw Inputs

- Raw files discovered under `data/raw`: `0`
- Files: `none`

## Station Mapping

- `enabled city station mapping validation passed`

## Next Step

- Fix the staged schema/integrity issues reported above, then rerun: kwb data validate-staging
