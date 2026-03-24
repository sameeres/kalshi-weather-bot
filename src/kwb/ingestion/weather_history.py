from __future__ import annotations

from datetime import date, datetime, timedelta, timezone
from pathlib import Path
from typing import TYPE_CHECKING, Any

import pandas as pd

from kwb.mapping.station_candidates import resolve_ncei_station_id
from kwb.mapping.station_mapping import validate_enabled_city_mappings
from kwb.settings import STAGING_DIR
from kwb.utils.logging import get_logger

if TYPE_CHECKING:
    from kwb.clients.ncei import NCEIClient

logger = get_logger(__name__)

DEFAULT_WEATHER_HISTORY_FILENAME = "weather_daily.parquet"
DEFAULT_SOURCE_DATASET = "GHCND"
DEFAULT_OBSERVATION_DATATYPES = ("TMAX",)


def ingest_weather_history_for_enabled_cities(
    start_date: str,
    end_date: str,
    config_path: Path,
    output_dir: Path | None = None,
    events_path: Path | None = None,
    client: "NCEIClient | None" = None,
) -> Path:
    """Fetch daily historical weather for validated enabled settlement stations."""
    validated_cities = validate_enabled_city_mappings(config_path=config_path, events_path=events_path)
    client = client or _build_ncei_client()
    output_dir = output_dir or STAGING_DIR
    output_dir.mkdir(parents=True, exist_ok=True)

    ingested_at = datetime.now(timezone.utc).isoformat()
    rows: list[dict[str, Any]] = []

    for city in validated_cities:
        station_rows = _fetch_city_weather_rows(
            client=client,
            city=city,
            start_date=start_date,
            end_date=end_date,
            ingested_at=ingested_at,
        )
        rows.extend(station_rows)

    df = _build_weather_daily_frame(rows)
    outpath = output_dir / DEFAULT_WEATHER_HISTORY_FILENAME
    df.to_parquet(outpath, index=False)
    logger.info(
        "Saved %s weather rows covering %s to %s across %s stations to %s",
        len(df),
        start_date,
        end_date,
        df["station_id"].nunique() if not df.empty else 0,
        outpath,
    )
    return outpath


def _fetch_city_weather_rows(
    client: "NCEIClient",
    city: dict[str, Any],
    start_date: str,
    end_date: str,
    ingested_at: str,
) -> list[dict[str, Any]]:
    station_id = city["settlement_station_id"]
    ncei_station_id = resolve_ncei_station_id(station_id)
    observations = _fetch_station_observations(
        client=client,
        station_id=ncei_station_id,
        start_date=start_date,
        end_date=end_date,
        datasetid=DEFAULT_SOURCE_DATASET,
    )

    grouped: dict[tuple[str, str], dict[str, Any]] = {}
    for observation in observations:
        if not isinstance(observation, dict):
            continue
        row_key = (city["city_key"], station_id, observation.get("date"))
        if row_key not in grouped:
            grouped[row_key] = {
                "station_id": station_id,
                "city_key": city["city_key"],
                "obs_date": _normalize_obs_date(observation.get("date")),
                "tmax_c": None,
                "source_dataset": observation.get("datasetid") or DEFAULT_SOURCE_DATASET,
                "ingested_at": ingested_at,
            }

        datatype = observation.get("datatype")
        normalized_value = _normalize_temperature_value(observation.get("value"))
        if datatype == "TMAX":
            grouped[row_key]["tmax_c"] = normalized_value

    rows: list[dict[str, Any]] = []
    for row in grouped.values():
        row["tmax_f"] = _celsius_to_fahrenheit(row["tmax_c"])
        rows.append(row)

    return rows


def _fetch_station_observations(
    client: "NCEIClient",
    station_id: str,
    start_date: str,
    end_date: str,
    datasetid: str,
    limit: int = 1000,
) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []

    for chunk_start_date, chunk_end_date in _iter_observation_request_windows(
        start_date=start_date,
        end_date=end_date,
        datasetid=datasetid,
    ):
        offset = 1
        while True:
            payload = client.get_daily_station_observations(
                station_id=station_id,
                start_date=chunk_start_date,
                end_date=chunk_end_date,
                datasetid=datasetid,
                datatypeids=list(DEFAULT_OBSERVATION_DATATYPES),
                limit=limit,
                offset=offset,
            )
            observations = payload.get("results", [])
            if not isinstance(observations, list):
                raise TypeError(f"Expected observations list for station {station_id}, got {type(observations)}")
            rows.extend(observations)
            if len(observations) < limit:
                break
            offset += limit

    return _dedupe_observations(rows)


def _iter_observation_request_windows(
    start_date: str,
    end_date: str,
    datasetid: str,
) -> list[tuple[str, str]]:
    if datasetid != DEFAULT_SOURCE_DATASET:
        return [(start_date, end_date)]

    request_start = date.fromisoformat(start_date)
    request_end = date.fromisoformat(end_date)
    if request_end < request_start:
        raise ValueError(f"end_date {end_date} must be on or after start_date {start_date}")

    windows: list[tuple[str, str]] = []
    chunk_start = request_start
    while chunk_start <= request_end:
        chunk_end = min(_inclusive_one_year_window_end(chunk_start), request_end)
        windows.append((chunk_start.isoformat(), chunk_end.isoformat()))
        chunk_start = chunk_end + timedelta(days=1)

    return windows


def _inclusive_one_year_window_end(chunk_start: date) -> date:
    return _same_day_next_year(chunk_start) - timedelta(days=1)


def _same_day_next_year(value: date) -> date:
    try:
        return value.replace(year=value.year + 1)
    except ValueError:
        # Feb 29 rolls to Feb 28 in the following year.
        return value.replace(year=value.year + 1, day=28)


def _dedupe_observations(observations: list[dict[str, Any]]) -> list[dict[str, Any]]:
    deduped: list[dict[str, Any]] = []
    seen: set[tuple[Any, ...]] = set()
    for observation in observations:
        if not isinstance(observation, dict):
            continue
        dedupe_key = (
            observation.get("station"),
            observation.get("date"),
            observation.get("datatype"),
            observation.get("value"),
            observation.get("attributes"),
            observation.get("datasetid"),
        )
        if dedupe_key in seen:
            continue
        seen.add(dedupe_key)
        deduped.append(observation)
    return deduped


def _build_weather_daily_frame(rows: list[dict[str, Any]]) -> pd.DataFrame:
    columns = [
        "station_id",
        "city_key",
        "obs_date",
        "tmax_c",
        "tmax_f",
        "source_dataset",
        "ingested_at",
    ]
    df = pd.DataFrame(rows, columns=columns)
    if df.empty:
        return df

    df = df.drop_duplicates(subset=["city_key", "station_id", "obs_date"], keep="last")
    df = df.sort_values(["city_key", "obs_date"], kind="stable").reset_index(drop=True)
    return df


def _normalize_obs_date(value: Any) -> str | None:
    if not isinstance(value, str) or not value:
        return None
    return value[:10]


def _normalize_temperature_value(value: Any) -> float | None:
    if value is None:
        return None
    if not isinstance(value, (int, float)):
        raise TypeError(f"Expected numeric temperature value, got {value!r}")
    return round(float(value) / 10.0, 3)


def _celsius_to_fahrenheit(value_c: float | None) -> float | None:
    if value_c is None:
        return None
    return round((value_c * 9.0 / 5.0) + 32.0, 3)


def _build_ncei_client() -> "NCEIClient":
    from kwb.clients.ncei import NCEIClient

    return NCEIClient()
