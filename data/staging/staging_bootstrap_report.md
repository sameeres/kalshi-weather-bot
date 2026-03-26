# Staging Bootstrap Report

- Checked at (UTC): `2026-03-26T19:16:29.628203+00:00`
- Ready for baseline runner: `True`
- Config path: `/Users/faisalessa/kalshi-weather-bot/configs/cities.yml`
- Staging dir: `/Users/faisalessa/kalshi-weather-bot/data/staging`
- Station mapping ready: `True`

## Datasets

### weather_daily

- File: `/Users/faisalessa/kalshi-weather-bot/data/staging/weather_daily.parquet`
- Exists: `True`
- Row count: `8093`
- Date coverage: `2015-01-01` to `2026-01-30`
- Source of truth: Enabled-city settlement station mapping from configs/cities.yml, then NOAA NCEI daily station observations for the mapped settlement station.
- Builder: `kwb.ingestion.weather_history.ingest_weather_history_for_enabled_cities`

### weather_normals_daily

- File: `/Users/faisalessa/kalshi-weather-bot/data/staging/weather_normals_daily.parquet`
- Exists: `True`
- Row count: `732`
- Date coverage: `01-01` to `12-31`
- Source of truth: Enabled-city settlement station mapping from configs/cities.yml, then NOAA NCEI daily climate normals for the mapped settlement station.
- Builder: `kwb.ingestion.climate_normals.ingest_climate_normals_for_enabled_cities`

### kalshi_markets

- File: `/Users/faisalessa/kalshi-weather-bot/data/staging/kalshi_markets.parquet`
- Exists: `True`
- Row count: `1092`
- Date coverage: `2025-11-01T00:00:00+0000` to `2026-01-30T00:00:00+0000`
- Source of truth: Enabled Kalshi series tickers from configs/cities.yml, then Kalshi series event and market metadata retrieved through the public API.
- Builder: `kwb.ingestion.kalshi_market_history.ingest_kalshi_market_history_for_enabled_cities`

### kalshi_candles

- File: `/Users/faisalessa/kalshi-weather-bot/data/staging/kalshi_candles.parquet`
- Exists: `True`
- Row count: `41493`
- Date coverage: `2025-11-01T00:00:00+0000` to `2026-01-30T23:00:00+0000`
- Source of truth: Enabled Kalshi series tickers from configs/cities.yml, then Kalshi candlestick history for each discovered market ticker.
- Builder: `kwb.ingestion.kalshi_market_history.ingest_kalshi_market_history_for_enabled_cities`

## Cross-Dataset Checks

- `ok` Enabled cities in config: chicago, nyc.
- `ok` Every staged market_ticker has at least one staged candle row.

## Kalshi Chunk Progress

- Status: `completed`
- Manifest: `/Users/faisalessa/kalshi-weather-bot/data/staging/kalshi_history_manifest.json`
- Chunk dir: `/Users/faisalessa/kalshi-weather-bot/data/staging/kalshi_history_chunks`
- Completed market chunks: `2`
- Completed candle chunks: `1092`
- Failed candle chunks: `0`
- Retries used: `127`

## Environment

- NCEI_API_TOKEN configured: `False`

## Local Raw Inputs

- Raw files discovered under `data/raw`: `0`
- Files: `none`

## Station Mapping

- `enabled city station mapping validation passed`

## Next Step

- Baseline inputs look ready. Run: kwb research run-climatology-baseline
