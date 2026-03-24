# Staging Bootstrap Report

- Checked at (UTC): `2026-03-24T11:29:20.624412+00:00`
- Ready for baseline runner: `True`
- Config path: `/Users/faisalessa/kalshi-weather-bot/configs/cities.yml`
- Staging dir: `/Users/faisalessa/kalshi-weather-bot/data/staging`
- Station mapping ready: `True`

## Build Errors

- `weather_daily build failed: NCEI_API_TOKEN is not set. NOAA Climate Data Online requests require this token.`
- `weather_normals_daily build failed: NCEI_API_TOKEN is not set. NOAA Climate Data Online requests require this token.`
- `kalshi_market_history build failed: HTTPSConnectionPool(host='api.elections.kalshi.com', port=443): Max retries exceeded with url: /trade-api/v2/events?limit=200&series_ticker=KXHIGHNY (Caused by NameResolutionError("HTTPSConnection(host='api.elections.kalshi.com', port=443): Failed to resolve 'api.elections.kalshi.com' ([Errno 8] nodename nor servname provided, or not known)"))`

## Datasets

### weather_daily

- File: `/Users/faisalessa/kalshi-weather-bot/data/staging/weather_daily.parquet`
- Exists: `True`
- Row count: `91`
- Date coverage: `2025-11-01` to `2026-01-30`
- Source of truth: Enabled-city settlement station mapping from configs/cities.yml, then NOAA NCEI daily station observations for the mapped settlement station.
- Builder: `kwb.ingestion.weather_history.ingest_weather_history_for_enabled_cities`

### weather_normals_daily

- File: `/Users/faisalessa/kalshi-weather-bot/data/staging/weather_normals_daily.parquet`
- Exists: `True`
- Row count: `365`
- Date coverage: `01-01` to `12-31`
- Source of truth: Enabled-city settlement station mapping from configs/cities.yml, then NOAA NCEI daily climate normals for the mapped settlement station.
- Builder: `kwb.ingestion.climate_normals.ingest_climate_normals_for_enabled_cities`

### kalshi_markets

- File: `/Users/faisalessa/kalshi-weather-bot/data/staging/kalshi_markets.parquet`
- Exists: `True`
- Row count: `546`
- Date coverage: `2025-11-01T00:00:00+0000` to `2026-01-30T00:00:00+0000`
- Source of truth: Enabled Kalshi series tickers from configs/cities.yml, then Kalshi series event and market metadata retrieved through the public API.
- Builder: `kwb.ingestion.kalshi_market_history.ingest_kalshi_market_history_for_enabled_cities`

### kalshi_candles

- File: `/Users/faisalessa/kalshi-weather-bot/data/staging/kalshi_candles.parquet`
- Exists: `True`
- Row count: `20544`
- Date coverage: `2025-11-01T00:00:00+0000` to `2026-01-30T23:00:00+0000`
- Source of truth: Enabled Kalshi series tickers from configs/cities.yml, then Kalshi candlestick history for each discovered market ticker.
- Builder: `kwb.ingestion.kalshi_market_history.ingest_kalshi_market_history_for_enabled_cities`

## Cross-Dataset Checks

- `ok` Enabled cities in config: nyc.
- `ok` Every staged market_ticker has at least one staged candle row.

## Kalshi Chunk Progress

- Status: `failed`
- Manifest: `/Users/faisalessa/kalshi-weather-bot/data/staging/kalshi_history_manifest.json`
- Chunk dir: `/Users/faisalessa/kalshi-weather-bot/data/staging/kalshi_history_chunks`
- Completed market chunks: `0`
- Completed candle chunks: `0`
- Failed candle chunks: `0`
- Retries used: `4`
- Last error: `HTTPSConnectionPool(host='api.elections.kalshi.com', port=443): Max retries exceeded with url: /trade-api/v2/events?limit=200&series_ticker=KXHIGHNY (Caused by NameResolutionError("HTTPSConnection(host='api.elections.kalshi.com', port=443): Failed to resolve 'api.elections.kalshi.com' ([Errno 8] nodename nor servname provided, or not known)"))`
- Resume next with `kwb data build-staging --resume --dataset kalshi_markets --dataset kalshi_candles ...`

## Environment

- NCEI_API_TOKEN configured: `False`

## Local Raw Inputs

- Raw files discovered under `data/raw`: `0`
- Files: `none`

## Station Mapping

- `enabled city station mapping validation passed`

## Next Step

- Set NCEI_API_TOKEN, then rerun: kwb data build-staging --start-date YYYY-MM-DD --end-date YYYY-MM-DD
