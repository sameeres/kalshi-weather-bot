from __future__ import annotations

from datetime import datetime, timezone
from pathlib import Path
from typing import TYPE_CHECKING, Any

import pandas as pd

from kwb.mapping.station_mapping import validate_enabled_city_mappings
from kwb.settings import STAGING_DIR
from kwb.utils.logging import get_logger

if TYPE_CHECKING:
    from kwb.clients.ncei import NCEIClient

logger = get_logger(__name__)

DEFAULT_CLIMATE_NORMALS_FILENAME = "weather_normals_daily.parquet"
DEFAULT_NORMALS_DATASET = "NORMAL_DLY"
DEFAULT_NORMALS_START_DATE = "2010-01-01"
DEFAULT_NORMALS_END_DATE = "2010-12-31"
DEFAULT_NORMALS_PERIOD = "1991-2020"
DEFAULT_NORMALS_DATATYPES = {
    "DLY-TMAX-NORMAL": "normal_tmax_c",
    "DLY-TMIN-NORMAL": "normal_tmin_c",
}


def ingest_climate_normals_for_enabled_cities(
    config_path: Path,
    output_dir: Path | None = None,
    events_path: Path | None = None,
    client: "NCEIClient | None" = None,
) -> tuple[Path, int, int]:
    """Fetch daily climate normals for validated settlement stations only."""
    validated_cities = validate_enabled_city_mappings(config_path=config_path, events_path=events_path)
    client = client or _build_ncei_client()
    output_dir = output_dir or STAGING_DIR
    output_dir.mkdir(parents=True, exist_ok=True)

    ingested_at = datetime.now(timezone.utc).isoformat()
    rows: list[dict[str, Any]] = []

    for city in validated_cities:
        station_rows = _fetch_city_climate_normal_rows(
            client=client,
            city=city,
            ingested_at=ingested_at,
        )
        rows.extend(station_rows)

    df = _build_climate_normals_frame(rows)
    outpath = output_dir / DEFAULT_CLIMATE_NORMALS_FILENAME
    df.to_parquet(outpath, index=False)
    row_count = len(df)
    station_count = df["station_id"].nunique() if not df.empty else 0
    logger.info(
        "Saved %s climate normals rows across %s stations to %s",
        row_count,
        station_count,
        outpath,
    )
    return outpath, row_count, station_count


def _fetch_city_climate_normal_rows(
    client: "NCEIClient",
    city: dict[str, Any],
    ingested_at: str,
) -> list[dict[str, Any]]:
    station_id = city["settlement_station_id"]
    payload = client.get_daily_climate_normals(
        station_id=station_id,
        start_date=DEFAULT_NORMALS_START_DATE,
        end_date=DEFAULT_NORMALS_END_DATE,
        datasetid=DEFAULT_NORMALS_DATASET,
        datatypeids=list(DEFAULT_NORMALS_DATATYPES),
    )
    observations = payload.get("results", [])
    if not isinstance(observations, list):
        raise TypeError(f"Expected climate normals list for station {station_id}, got {type(observations)}")

    grouped: dict[tuple[str, str, str], dict[str, Any]] = {}
    for observation in observations:
        if not isinstance(observation, dict):
            continue

        datatype = observation.get("datatype")
        if datatype not in DEFAULT_NORMALS_DATATYPES:
            continue

        month_day = _normalize_month_day(observation.get("date"))
        row_key = (city["city_key"], station_id, month_day)
        if row_key not in grouped:
            grouped[row_key] = {
                "station_id": station_id,
                "city_key": city["city_key"],
                "month_day": month_day,
                "normal_tmax_c": None,
                "normal_tmin_c": None,
                "normal_tmax_f": None,
                "normal_tmin_f": None,
                # Assumption: NORMAL_DLY responses map to NOAA daily climate normals for the 1991-2020 period.
                "normals_period": DEFAULT_NORMALS_PERIOD,
                "normals_source": observation.get("datasetid") or DEFAULT_NORMALS_DATASET,
                "ingested_at": ingested_at,
            }

        grouped[row_key][DEFAULT_NORMALS_DATATYPES[datatype]] = _normalize_temperature_value(observation.get("value"))

    rows: list[dict[str, Any]] = []
    for row in grouped.values():
        row["normal_tmax_f"] = _celsius_to_fahrenheit(row["normal_tmax_c"])
        row["normal_tmin_f"] = _celsius_to_fahrenheit(row["normal_tmin_c"])
        rows.append(row)

    return rows


def _build_climate_normals_frame(rows: list[dict[str, Any]]) -> pd.DataFrame:
    columns = [
        "station_id",
        "city_key",
        "month_day",
        "normal_tmax_c",
        "normal_tmin_c",
        "normal_tmax_f",
        "normal_tmin_f",
        "normals_period",
        "normals_source",
        "ingested_at",
    ]
    df = pd.DataFrame(rows, columns=columns)
    if df.empty:
        return df

    df = df.drop_duplicates(subset=["city_key", "station_id", "month_day"], keep="last")
    df = df.sort_values(["city_key", "month_day"], kind="stable").reset_index(drop=True)
    return df


def _normalize_month_day(value: Any) -> str:
    if not isinstance(value, str) or not value:
        raise TypeError(f"Expected normals date string, got {value!r}")
    normalized = value[5:10]
    if len(normalized) != 5 or normalized[2] != "-":
        raise ValueError(f"Could not normalize month_day from {value!r}")
    return normalized


def _normalize_temperature_value(value: Any) -> float | None:
    if value is None:
        raise ValueError("Expected numeric normals temperature value, got None")
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
