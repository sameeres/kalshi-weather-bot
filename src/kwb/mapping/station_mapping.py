from __future__ import annotations

from pathlib import Path
from typing import Any
from urllib.parse import urlparse

import pandas as pd

from kwb.ingestion.kalshi_events import DEFAULT_CITIES_CONFIG_PATH, DEFAULT_EVENTS_FILENAME
from kwb.settings import STAGING_DIR
from kwb.utils.io import read_yaml


class StationMappingValidationError(ValueError):
    """Raised when enabled-city settlement station mapping is incomplete or ambiguous."""


def load_enabled_city_mappings(config_path: Path = DEFAULT_CITIES_CONFIG_PATH) -> list[dict[str, Any]]:
    """Load enabled city rows from the checked-in config."""
    payload = read_yaml(config_path)
    cities = payload.get("cities", [])
    if not isinstance(cities, list):
        raise TypeError(f"Expected 'cities' list in {config_path}.")
    return [city for city in cities if isinstance(city, dict) and city.get("enabled")]


def validate_enabled_city_mappings(
    config_path: Path = DEFAULT_CITIES_CONFIG_PATH,
    events_path: Path | None = None,
) -> list[dict[str, Any]]:
    """Validate that each enabled city has an explicit, authoritative station mapping."""
    cities = load_enabled_city_mappings(config_path)
    issues = collect_station_mapping_issues(config_path=config_path, events_path=events_path, cities=cities)
    if issues:
        raise StationMappingValidationError("\n".join(issues))
    return cities


def collect_station_mapping_issues(
    config_path: Path = DEFAULT_CITIES_CONFIG_PATH,
    events_path: Path | None = None,
    cities: list[dict[str, Any]] | None = None,
) -> list[str]:
    """Collect station-mapping validation issues without raising."""
    cities = cities if cities is not None else load_enabled_city_mappings(config_path)
    issues: list[str] = []

    if len(cities) > 3:
        issues.append(f"Enabled city count exceeds MVP limit: found {len(cities)} enabled cities.")

    enabled_city_keys = [city.get("city_key") for city in cities]
    duplicate_keys = sorted({key for key in enabled_city_keys if key and enabled_city_keys.count(key) > 1})
    for city_key in duplicate_keys:
        issues.append(f"Duplicate enabled city_key found: {city_key}")

    staged_events = _load_events_frame(events_path) if _events_path_exists(events_path) else None

    for city in cities:
        city_key = city.get("city_key") or "<missing-city-key>"
        issues.extend(_collect_city_issues(city))
        if staged_events is not None:
            issues.extend(_collect_staged_source_issues(city=city, staged_events=staged_events))

        if not city.get("city_key"):
            issues.append("Enabled city row is missing city_key.")
        if not city.get("kalshi_series_ticker"):
            issues.append(f"Enabled city {city_key} is missing kalshi_series_ticker.")

    return issues


def _collect_city_issues(city: dict[str, Any]) -> list[str]:
    city_key = city.get("city_key") or "<missing-city-key>"
    issues: list[str] = []
    required_fields = [
        "settlement_source_name",
        "settlement_source_url",
        "settlement_station_id",
        "settlement_station_name",
        "station_lat",
        "station_lon",
    ]

    for field in required_fields:
        value = city.get(field)
        if value in (None, ""):
            issues.append(f"Enabled city {city_key} is missing {field}.")

    for field in ("station_lat", "station_lon"):
        value = city.get(field)
        if value in (None, ""):
            continue
        if not isinstance(value, (int, float)):
            issues.append(f"Enabled city {city_key} has non-numeric {field}: {value!r}")
            continue
        if field == "station_lat" and not (-90.0 <= float(value) <= 90.0):
            issues.append(f"Enabled city {city_key} has out-of-range station_lat: {value!r}")
        if field == "station_lon" and not (-180.0 <= float(value) <= 180.0):
            issues.append(f"Enabled city {city_key} has out-of-range station_lon: {value!r}")

    station_id = city.get("settlement_station_id")
    if station_id not in (None, ""):
        if not isinstance(station_id, str) or len(station_id.strip()) < 3:
            issues.append(f"Enabled city {city_key} has implausible settlement_station_id: {station_id!r}")

    station_name = city.get("settlement_station_name")
    if station_name not in (None, "") and (not isinstance(station_name, str) or not station_name.strip()):
        issues.append(f"Enabled city {city_key} has empty settlement_station_name.")

    source_name = city.get("settlement_source_name")
    if source_name not in (None, "") and (not isinstance(source_name, str) or not source_name.strip()):
        issues.append(f"Enabled city {city_key} has empty settlement_source_name.")

    source_url = city.get("settlement_source_url")
    if source_url not in (None, ""):
        if not isinstance(source_url, str):
            issues.append(f"Enabled city {city_key} has non-string settlement_source_url: {source_url!r}")
        else:
            parsed = urlparse(source_url)
            if parsed.scheme not in {"http", "https"} or not parsed.netloc:
                issues.append(f"Enabled city {city_key} has implausible settlement_source_url: {source_url!r}")

    return issues


def _collect_staged_source_issues(city: dict[str, Any], staged_events: pd.DataFrame) -> list[str]:
    city_key = city.get("city_key") or "<missing-city-key>"
    city_rows = staged_events.loc[staged_events["city_key"] == city_key]
    if city_rows.empty:
        return [f"Enabled city {city_key} has no staged Kalshi event rows for source validation."]

    source_pairs = {
        (row["settlement_source_name"], row["settlement_source_url"])
        for row in city_rows[["settlement_source_name", "settlement_source_url"]].to_dict("records")
        if row["settlement_source_name"] or row["settlement_source_url"]
    }
    if not source_pairs:
        return [f"Enabled city {city_key} has no staged settlement source metadata."]
    if len(source_pairs) > 1:
        return [f"Enabled city {city_key} has ambiguous staged settlement sources: {sorted(source_pairs)!r}"]

    staged_name, staged_url = next(iter(source_pairs))
    issues: list[str] = []
    config_url = city.get("settlement_source_url")
    config_name = city.get("settlement_source_name")

    if config_url and staged_url and config_url != staged_url:
        issues.append(
            f"Enabled city {city_key} settlement_source_url does not match staged metadata: "
            f"{config_url!r} != {staged_url!r}"
        )
    if config_name and staged_name and config_name != staged_name:
        issues.append(
            f"Enabled city {city_key} settlement_source_name does not match staged metadata: "
            f"{config_name!r} != {staged_name!r}"
        )

    return issues


def _events_path_exists(events_path: Path | None) -> bool:
    path = _resolve_events_path(events_path)
    return path.exists()


def _load_events_frame(events_path: Path | None) -> pd.DataFrame:
    path = _resolve_events_path(events_path)
    return pd.read_parquet(path)


def _resolve_events_path(events_path: Path | None) -> Path:
    return events_path or (STAGING_DIR / DEFAULT_EVENTS_FILENAME)
