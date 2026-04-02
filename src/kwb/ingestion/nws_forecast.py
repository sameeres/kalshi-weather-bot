from __future__ import annotations

from datetime import datetime, timezone
from pathlib import Path
from typing import TYPE_CHECKING, Any

import pandas as pd

from kwb.ingestion.kalshi_events import DEFAULT_CITIES_CONFIG_PATH, load_enabled_cities
from kwb.settings import STAGING_DIR
from kwb.utils.logging import get_logger

if TYPE_CHECKING:
    from kwb.clients.nws import NWSClient

logger = get_logger(__name__)

DEFAULT_FORECAST_SNAPSHOTS_FILENAME = "nws_forecast_hourly_snapshots.parquet"


class NWSForecastIngestionError(ValueError):
    """Raised when NWS forecast snapshot ingestion cannot complete safely."""


def fetch_nws_forecast_snapshots(
    config_path: Path = DEFAULT_CITIES_CONFIG_PATH,
    output_dir: Path | None = None,
    client: "NWSClient | None" = None,
    append: bool = True,
    snapshot_ts: datetime | None = None,
) -> Path:
    cities = load_enabled_cities(config_path)
    if not cities:
        raise NWSForecastIngestionError(f"No enabled cities with Kalshi series tickers found in {config_path}.")

    client = client or _build_nws_client()
    output_dir = output_dir or STAGING_DIR
    output_dir.mkdir(parents=True, exist_ok=True)

    normalized_snapshot_ts = (snapshot_ts or datetime.now(timezone.utc)).astimezone(timezone.utc)
    rows: list[dict[str, Any]] = []
    for city in cities:
        rows.extend(_fetch_city_snapshot_rows(client=client, city=city, snapshot_ts=normalized_snapshot_ts))

    fresh_df = _build_forecast_snapshot_frame(rows)
    outpath = output_dir / DEFAULT_FORECAST_SNAPSHOTS_FILENAME

    if append and outpath.exists():
        existing_df = pd.read_parquet(outpath)
        df = _merge_snapshot_frames(existing_df=existing_df, fresh_df=fresh_df)
    else:
        df = fresh_df

    df.to_parquet(outpath, index=False)
    logger.info(
        "Saved %s NWS hourly forecast snapshot rows across %s cities to %s",
        len(df),
        df["city_key"].nunique() if not df.empty else 0,
        outpath,
    )
    return outpath


def _fetch_city_snapshot_rows(
    client: "NWSClient",
    city: dict[str, Any],
    snapshot_ts: datetime,
) -> list[dict[str, Any]]:
    latitude = city.get("station_lat")
    longitude = city.get("station_lon")
    if latitude is None or longitude is None:
        raise NWSForecastIngestionError(
            f"City {city.get('city_key')} is missing station_lat/station_lon required for api.weather.gov points lookup."
        )

    points_payload = client.get_points(latitude=float(latitude), longitude=float(longitude))
    points_properties = points_payload.get("properties", {})
    forecast_hourly_url = points_properties.get("forecastHourly")
    if not isinstance(forecast_hourly_url, str) or not forecast_hourly_url:
        raise NWSForecastIngestionError(
            f"NWS points payload did not include forecastHourly URL for city {city.get('city_key')}."
        )

    forecast_payload = client.get_json_url(forecast_hourly_url)
    forecast_properties = forecast_payload.get("properties", {})
    periods = forecast_properties.get("periods", [])
    if not isinstance(periods, list):
        raise NWSForecastIngestionError(
            f"NWS hourly forecast periods payload was not a list for city {city.get('city_key')}."
        )

    rows: list[dict[str, Any]] = []
    for period in periods:
        if not isinstance(period, dict):
            continue
        start_ts = _parse_timestamp(period.get("startTime"))
        end_ts = _parse_timestamp(period.get("endTime"))
        if start_ts is None or end_ts is None:
            continue

        temperature = period.get("temperature")
        temperature_unit = str(period.get("temperatureUnit") or "F").upper()
        temperature_f = _normalize_temperature_f(temperature=temperature, temperature_unit=temperature_unit)
        if temperature_f is None:
            continue

        rows.append(
            {
                "snapshot_ts": snapshot_ts.isoformat(),
                "city_key": city.get("city_key"),
                "city_name": city.get("city_name"),
                "timezone": city.get("timezone"),
                "series_ticker": city.get("kalshi_series_ticker"),
                "settlement_station_id": city.get("settlement_station_id"),
                "settlement_station_name": city.get("settlement_station_name"),
                "forecast_points_url": points_payload.get("@id"),
                "forecast_hourly_url": forecast_hourly_url,
                "forecast_updated_at": forecast_properties.get("updated"),
                "forecast_generated_at": forecast_properties.get("generatedAt"),
                "period_number": period.get("number"),
                "period_name": period.get("name"),
                "period_start_ts": start_ts.isoformat(),
                "period_end_ts": end_ts.isoformat(),
                "period_date_local": start_ts.strftime("%Y-%m-%d"),
                "lead_hours": round((start_ts - snapshot_ts).total_seconds() / 3600.0, 3),
                "temperature_f": temperature_f,
                "temperature_unit": temperature_unit,
                "is_daytime": bool(period.get("isDaytime")),
                "short_forecast": period.get("shortForecast"),
            }
        )

    return rows


def _build_forecast_snapshot_frame(rows: list[dict[str, Any]]) -> pd.DataFrame:
    columns = [
        "snapshot_ts",
        "city_key",
        "city_name",
        "timezone",
        "series_ticker",
        "settlement_station_id",
        "settlement_station_name",
        "forecast_points_url",
        "forecast_hourly_url",
        "forecast_updated_at",
        "forecast_generated_at",
        "period_number",
        "period_name",
        "period_start_ts",
        "period_end_ts",
        "period_date_local",
        "lead_hours",
        "temperature_f",
        "temperature_unit",
        "is_daytime",
        "short_forecast",
    ]
    df = pd.DataFrame(rows, columns=columns)
    if df.empty:
        return df
    return (
        df.drop_duplicates(subset=["snapshot_ts", "city_key", "period_start_ts"], keep="last")
        .sort_values(["snapshot_ts", "city_key", "period_start_ts"], kind="stable")
        .reset_index(drop=True)
    )


def _merge_snapshot_frames(existing_df: pd.DataFrame, fresh_df: pd.DataFrame) -> pd.DataFrame:
    combined = pd.concat([existing_df, fresh_df], ignore_index=True, sort=False)
    return (
        combined.drop_duplicates(subset=["snapshot_ts", "city_key", "period_start_ts"], keep="last")
        .sort_values(["snapshot_ts", "city_key", "period_start_ts"], kind="stable")
        .reset_index(drop=True)
    )


def _parse_timestamp(value: Any) -> datetime | None:
    if value is None:
        return None
    parsed = pd.to_datetime(value, utc=True, errors="coerce")
    if pd.isna(parsed):
        return None
    return parsed.to_pydatetime()


def _normalize_temperature_f(temperature: Any, temperature_unit: str) -> float | None:
    if temperature is None or pd.isna(temperature):
        return None
    value = float(temperature)
    if temperature_unit == "F":
        return round(value, 3)
    if temperature_unit == "C":
        return round((value * 9.0 / 5.0) + 32.0, 3)
    raise NWSForecastIngestionError(f"Unsupported NWS forecast temperature unit {temperature_unit!r}")


def _build_nws_client() -> "NWSClient":
    from kwb.clients.nws import NWSClient

    return NWSClient()
