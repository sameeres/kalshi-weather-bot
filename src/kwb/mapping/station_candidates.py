from __future__ import annotations

import json
from pathlib import Path
from typing import Any

import pandas as pd

from kwb.ingestion.kalshi_events import DEFAULT_CITIES_CONFIG_PATH, DEFAULT_EVENTS_FILENAME
from kwb.mapping.station_mapping import load_enabled_city_mappings
from kwb.settings import STAGING_DIR

DEFAULT_STATION_REPORT_FILENAME = "station_mapping_report.csv"

REQUIRED_STATION_FIELDS = [
    "settlement_source_url",
    "settlement_station_id",
    "settlement_station_name",
    "station_lat",
    "station_lon",
]


def build_station_mapping_report(
    config_path: Path = DEFAULT_CITIES_CONFIG_PATH,
    events_path: Path | None = None,
) -> pd.DataFrame:
    """Build a human-readable report of missing station mapping fields for enabled cities."""
    cities = load_enabled_city_mappings(config_path)
    staged_summary = _load_staged_source_summary(events_path) if _events_path(events_path).exists() else {}

    rows = [_build_report_row(city, staged_summary.get(city.get("city_key"))) for city in cities]
    columns = [
        "city_key",
        "city_name",
        "kalshi_series_ticker",
        "settlement_source_name",
        "settlement_source_url",
        "staged_settlement_source_name",
        "staged_settlement_source_url",
        "staged_source_status",
        "staged_source_pairs_json",
        "settlement_station_id",
        "settlement_station_name",
        "station_lat",
        "station_lon",
        "missing_fields",
        "mapping_complete",
        "validation_ready",
        "notes",
    ]
    df = pd.DataFrame(rows, columns=columns)
    if not df.empty:
        df = df.sort_values(["city_key"], kind="stable").reset_index(drop=True)
    return df


def write_station_mapping_report(
    config_path: Path = DEFAULT_CITIES_CONFIG_PATH,
    output_path: Path | None = None,
    events_path: Path | None = None,
) -> Path:
    """Write the station-mapping helper report to CSV without modifying config."""
    report = build_station_mapping_report(config_path=config_path, events_path=events_path)
    path = output_path or (STAGING_DIR / DEFAULT_STATION_REPORT_FILENAME)
    path.parent.mkdir(parents=True, exist_ok=True)
    report.to_csv(path, index=False)
    return path


def _build_report_row(
    city: dict[str, Any],
    staged_summary: dict[str, Any] | None,
) -> dict[str, Any]:
    missing_fields = [field for field in REQUIRED_STATION_FIELDS if city.get(field) in (None, "")]
    staged_status = staged_summary["staged_source_status"] if staged_summary else "not_checked"
    notes = _build_notes(missing_fields=missing_fields, staged_status=staged_status)

    mapping_complete = not missing_fields
    validation_ready = mapping_complete and staged_status not in {"ambiguous", "missing_rows", "missing_source"}

    return {
        "city_key": city.get("city_key"),
        "city_name": city.get("city_name"),
        "kalshi_series_ticker": city.get("kalshi_series_ticker"),
        "settlement_source_name": city.get("settlement_source_name"),
        "settlement_source_url": city.get("settlement_source_url"),
        "staged_settlement_source_name": staged_summary.get("staged_settlement_source_name")
        if staged_summary
        else None,
        "staged_settlement_source_url": staged_summary.get("staged_settlement_source_url")
        if staged_summary
        else None,
        "staged_source_status": staged_status,
        "staged_source_pairs_json": staged_summary.get("staged_source_pairs_json") if staged_summary else None,
        "settlement_station_id": city.get("settlement_station_id"),
        "settlement_station_name": city.get("settlement_station_name"),
        "station_lat": city.get("station_lat"),
        "station_lon": city.get("station_lon"),
        "missing_fields": ",".join(missing_fields),
        "mapping_complete": mapping_complete,
        "validation_ready": validation_ready,
        "notes": notes,
    }


def _build_notes(missing_fields: list[str], staged_status: str) -> str:
    notes: list[str] = []
    if missing_fields:
        notes.append(f"manual config update required for: {', '.join(missing_fields)}")
    if staged_status == "ambiguous":
        notes.append("staged settlement source is ambiguous; do not map station until resolved")
    elif staged_status == "missing_rows":
        notes.append("no staged Kalshi event rows found for enabled city")
    elif staged_status == "missing_source":
        notes.append("staged Kalshi events found but settlement source metadata is missing")
    elif staged_status == "not_checked":
        notes.append("staged Kalshi events not available; source context not cross-checked")
    return "; ".join(notes)


def _load_staged_source_summary(events_path: Path | None) -> dict[str, dict[str, Any]]:
    events_df = pd.read_parquet(_events_path(events_path))
    summary: dict[str, dict[str, Any]] = {}

    if events_df.empty:
        return summary

    for city_key in sorted(set(events_df["city_key"])):
        city_rows = events_df.loc[events_df["city_key"] == city_key]
        source_pairs = {
            (row["settlement_source_name"], row["settlement_source_url"])
            for row in city_rows[["settlement_source_name", "settlement_source_url"]].to_dict("records")
            if row["settlement_source_name"] or row["settlement_source_url"]
        }

        if city_rows.empty:
            summary[city_key] = {
                "staged_source_status": "missing_rows",
                "staged_source_pairs_json": "[]",
            }
            continue

        if not source_pairs:
            summary[city_key] = {
                "staged_source_status": "missing_source",
                "staged_source_pairs_json": "[]",
            }
            continue

        if len(source_pairs) > 1:
            summary[city_key] = {
                "staged_source_status": "ambiguous",
                "staged_source_pairs_json": json.dumps(sorted(source_pairs)),
            }
            continue

        staged_name, staged_url = next(iter(source_pairs))
        summary[city_key] = {
            "staged_source_status": "unique",
            "staged_settlement_source_name": staged_name,
            "staged_settlement_source_url": staged_url,
            "staged_source_pairs_json": json.dumps(sorted(source_pairs)),
        }

    return summary


def _events_path(events_path: Path | None) -> Path:
    return events_path or (STAGING_DIR / DEFAULT_EVENTS_FILENAME)
