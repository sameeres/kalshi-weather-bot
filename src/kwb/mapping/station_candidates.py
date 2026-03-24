from __future__ import annotations

import json
import re
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import pandas as pd

from kwb.ingestion.kalshi_events import DEFAULT_CITIES_CONFIG_PATH, DEFAULT_EVENTS_FILENAME
from kwb.mapping.station_mapping import load_enabled_city_mappings
from kwb.settings import STAGING_DIR
from kwb.utils.io import read_yaml, write_yaml

DEFAULT_STATION_REPORT_FILENAME = "station_mapping_report.csv"
DEFAULT_STATION_CANDIDATES_FILENAME = "station_mapping_candidates.json"
DEFAULT_STATION_RECOMMENDATIONS_FILENAME = "station_mapping_recommendations.md"

REQUIRED_STATION_FIELDS = [
    "settlement_source_url",
    "settlement_station_id",
    "settlement_station_name",
    "station_lat",
    "station_lon",
]

OBHISTORY_STATION_PATTERN = re.compile(r"/obhistory/(?P<station>[A-Z0-9]{3,8})\.html", re.IGNORECASE)
STATION_ID_PATTERN = re.compile(r"^[A-Z0-9:-]{3,24}$")

# Curated metadata for settlement stations already supported by the repo's MVP scope.
# Provenance is recorded in recommendation outputs so this does not silently masquerade as
# dynamically discovered upstream metadata.
KNOWN_STATION_METADATA: dict[str, dict[str, Any]] = {
    "KNYC": {
        "station_name": "Central Park",
        "station_lat": 40.7789,
        "station_lon": -73.9692,
        "ncei_station_id": "GHCND:USW00094728",
        "metadata_provenance": "curated_station_registry",
    },
    "KLGA": {
        "station_name": "LaGuardia Airport",
        "station_lat": 40.7769,
        "station_lon": -73.8740,
        "ncei_station_id": "GHCND:USW00014732",
        "metadata_provenance": "curated_station_registry",
    }
}

# MVP series/source hint registry used only when staged Kalshi event metadata is unavailable locally.
SERIES_SOURCE_HINTS: dict[str, dict[str, Any]] = {
    "KXHIGHNY": {
        "settlement_source_name": "National Weather Service",
        "settlement_source_url": "https://forecast.weather.gov/data/obhistory/KLGA.html",
        "source_provenance": "curated_series_source_hint",
    }
}

# Explicit settlement overrides must take precedence over generic source-url parsing or nearest-airport
# heuristics. Kalshi settlement alignment is more important than geographic convenience.
EXPLICIT_SETTLEMENT_OVERRIDES: dict[tuple[str, str], dict[str, Any]] = {
    ("nyc", "KXHIGHNY"): {
        "settlement_source_name": "National Weather Service",
        "settlement_source_url": "https://forecast.weather.gov/data/obhistory/KNYC.html",
        "settlement_station_id": "KNYC",
        "selection_reason": "explicit settlement-alignment override for Kalshi Central Park temperature markets",
        "override_provenance": "explicit_settlement_override",
    }
}


def build_station_mapping_report(
    config_path: Path = DEFAULT_CITIES_CONFIG_PATH,
    events_path: Path | None = None,
) -> pd.DataFrame:
    """Build a human-readable report of missing station mapping fields for enabled cities."""
    resolution = resolve_enabled_city_station_candidates(config_path=config_path, events_path=events_path)
    rows = [_build_report_row(result) for result in resolution["results"]]
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
        "recommended_station_id",
        "recommended_station_name",
        "recommended_station_lat",
        "recommended_station_lon",
        "recommendation_confidence",
        "selected_automatically",
        "recommendation_provenance",
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


def resolve_enabled_city_station_candidates(
    config_path: Path = DEFAULT_CITIES_CONFIG_PATH,
    events_path: Path | None = None,
    city_key: str | None = None,
    min_confidence: float = 0.85,
) -> dict[str, Any]:
    """Resolve ranked station candidates for enabled cities using staged settlement metadata plus curated hints."""
    cities = load_enabled_city_mappings(config_path)
    if city_key:
        cities = [city for city in cities if city.get("city_key") == city_key]

    staged_summary = _load_staged_source_summary(events_path) if _events_path(events_path).exists() else {}
    checked_at = datetime.now(timezone.utc).isoformat()
    results = [_resolve_city_station_mapping(city=city, staged_summary=staged_summary.get(city.get("city_key")), min_confidence=min_confidence) for city in cities]

    return {
        "checked_at_utc": checked_at,
        "config_path": str(config_path),
        "events_path": str(_events_path(events_path)) if _events_path(events_path).exists() else None,
        "min_confidence": min_confidence,
        "city_count": len(results),
        "results": results,
    }


def write_station_mapping_recommendations(
    config_path: Path = DEFAULT_CITIES_CONFIG_PATH,
    events_path: Path | None = None,
    city_key: str | None = None,
    output_dir: Path | None = None,
    min_confidence: float = 0.85,
) -> tuple[Path, Path, dict[str, Any]]:
    """Write machine-readable and markdown recommendation artifacts for station completion."""
    output_dir = output_dir or STAGING_DIR
    output_dir.mkdir(parents=True, exist_ok=True)

    resolution = resolve_enabled_city_station_candidates(
        config_path=config_path,
        events_path=events_path,
        city_key=city_key,
        min_confidence=min_confidence,
    )
    json_path = output_dir / DEFAULT_STATION_CANDIDATES_FILENAME
    markdown_path = output_dir / DEFAULT_STATION_RECOMMENDATIONS_FILENAME
    json_path.write_text(json.dumps(resolution, indent=2, sort_keys=True), encoding="utf-8")
    markdown_path.write_text(render_station_mapping_recommendations_markdown(resolution), encoding="utf-8")
    return json_path, markdown_path, resolution


def apply_station_mapping_recommendations(
    config_path: Path = DEFAULT_CITIES_CONFIG_PATH,
    events_path: Path | None = None,
    city_key: str | None = None,
    min_confidence: float = 0.85,
) -> tuple[Path, list[dict[str, Any]], dict[str, Any]]:
    """Opt-in config writer for high-confidence station recommendations only."""
    resolution = resolve_enabled_city_station_candidates(
        config_path=config_path,
        events_path=events_path,
        city_key=city_key,
        min_confidence=min_confidence,
    )
    payload = read_yaml(config_path)
    cities = payload.get("cities", [])
    if not isinstance(cities, list):
        raise TypeError(f"Expected 'cities' list in {config_path}.")

    updates: list[dict[str, Any]] = []
    resolution_by_city = {result["city_key"]: result for result in resolution["results"]}

    changed = False
    for city in cities:
        if not isinstance(city, dict):
            continue
        result = resolution_by_city.get(city.get("city_key"))
        if result is None or not result["selected_automatically"]:
            continue

        recommendation = result["selected_candidate"]
        if recommendation is None:
            continue

        changed_fields: dict[str, Any] = {}
        for field in REQUIRED_STATION_FIELDS + ["settlement_source_name"]:
            recommended_value = recommendation.get(field)
            if recommended_value in (None, ""):
                continue
            if city.get(field) != recommended_value:
                city[field] = recommended_value
                changed_fields[field] = recommended_value
                changed = True

        if changed_fields:
            updates.append(
                {
                    "city_key": result["city_key"],
                    "changed_fields": changed_fields,
                    "confidence": recommendation["confidence_score"],
                    "provenance": recommendation["provenance"],
                }
            )

    if changed:
        write_yaml(config_path, payload)
    return config_path, updates, resolution


def render_station_mapping_recommendations_markdown(resolution: dict[str, Any]) -> str:
    """Render a compact human-readable recommendation report with copy-pasteable YAML snippets."""
    lines = [
        "# Station Mapping Recommendations",
        "",
        f"- Checked at (UTC): `{resolution['checked_at_utc']}`",
        f"- Config path: `{resolution['config_path']}`",
        f"- Min confidence for auto-selection: `{resolution['min_confidence']}`",
        "",
    ]

    for result in resolution["results"]:
        lines.extend(
            [
                f"## {result['city_key']}",
                "",
                f"- City: `{result['city_name']}`",
                f"- Series: `{result['kalshi_series_ticker']}`",
                f"- Current mapping complete: `{result['mapping_complete']}`",
                f"- Selected automatically: `{result['selected_automatically']}`",
                f"- Resolution status: `{result['resolution_status']}`",
            ]
        )
        if result["selected_candidate"] is not None:
            candidate = result["selected_candidate"]
            lines.extend(
                [
                    f"- Recommended station: `{candidate['settlement_station_id']}` `{candidate['settlement_station_name']}`",
                    f"- Coordinates: `{candidate['station_lat']}`, `{candidate['station_lon']}`",
                    f"- Settlement source URL: `{candidate['settlement_source_url']}`",
                    f"- Confidence: `{candidate['confidence_bucket']}` ({candidate['confidence_score']})",
                    f"- Provenance: `{candidate['provenance']}`",
                    "",
                    "```yaml",
                    f"settlement_source_name: {candidate['settlement_source_name']}",
                    f"settlement_source_url: {candidate['settlement_source_url']}",
                    f"settlement_station_id: {candidate['settlement_station_id']}",
                    f"settlement_station_name: {candidate['settlement_station_name']}",
                    f"station_lat: {candidate['station_lat']}",
                    f"station_lon: {candidate['station_lon']}",
                    "```",
                ]
            )
        if result["notes"]:
            lines.append(f"- Notes: {'; '.join(result['notes'])}")
        lines.append("")

    return "\n".join(lines)


def _resolve_city_station_mapping(
    city: dict[str, Any],
    staged_summary: dict[str, Any] | None,
    min_confidence: float,
) -> dict[str, Any]:
    source_context = _build_source_context(city=city, staged_summary=staged_summary)
    candidates = _generate_candidates_for_city(city=city, source_context=source_context)
    selected = candidates[0] if candidates else None
    selected_automatically = bool(selected and selected["confidence_score"] >= min_confidence and selected["is_complete"])
    missing_fields = [field for field in REQUIRED_STATION_FIELDS if city.get(field) in (None, "")]
    mapping_complete = not missing_fields

    notes = list(source_context["notes"])
    if selected is None:
        notes.append("no high-signal station candidate could be derived from current source evidence")
    elif not selected["is_complete"]:
        notes.append("candidate exists but required station metadata is incomplete")
    elif not selected_automatically:
        notes.append("candidate exists but confidence is below the auto-selection threshold")

    if mapping_complete:
        resolution_status = "configured"
    elif selected_automatically:
        resolution_status = "recommended_high_confidence"
    elif candidates:
        resolution_status = "recommended_manual_review"
    else:
        resolution_status = "unresolved"

    return {
        "city_key": city.get("city_key"),
        "city_name": city.get("city_name"),
        "kalshi_series_ticker": city.get("kalshi_series_ticker"),
        "current_config": {
            "settlement_source_name": city.get("settlement_source_name"),
            "settlement_source_url": city.get("settlement_source_url"),
            "settlement_station_id": city.get("settlement_station_id"),
            "settlement_station_name": city.get("settlement_station_name"),
            "station_lat": city.get("station_lat"),
            "station_lon": city.get("station_lon"),
        },
        "staged_source_status": source_context["staged_source_status"],
        "staged_source_name": source_context["staged_settlement_source_name"],
        "staged_source_url": source_context["staged_settlement_source_url"],
        "staged_source_pairs_json": source_context["staged_source_pairs_json"],
        "source_evidence": source_context["evidence"],
        "candidate_count": len(candidates),
        "candidates": candidates,
        "selected_candidate": selected,
        "selected_automatically": selected_automatically,
        "missing_fields": missing_fields,
        "mapping_complete": mapping_complete,
        "resolution_status": resolution_status,
        "notes": notes,
    }


def _generate_candidates_for_city(city: dict[str, Any], source_context: dict[str, Any]) -> list[dict[str, Any]]:
    override_candidate = _candidate_from_explicit_override(city=city, source_context=source_context)
    if override_candidate is not None:
        return [override_candidate]
    candidate = _candidate_from_source_context(city=city, source_context=source_context)
    if candidate is None:
        return []
    return [candidate]


def _candidate_from_explicit_override(city: dict[str, Any], source_context: dict[str, Any]) -> dict[str, Any] | None:
    override = EXPLICIT_SETTLEMENT_OVERRIDES.get(
        (
            str(city.get("city_key") or ""),
            str(city.get("kalshi_series_ticker") or ""),
        )
    )
    if override is None:
        return None

    station_id = str(override["settlement_station_id"]).upper()
    metadata = KNOWN_STATION_METADATA.get(station_id, {})
    evidence = list(source_context["evidence"])
    evidence.append("applied explicit settlement override before generic station heuristics")
    if source_context["effective_source_url"]:
        evidence.append(
            "override takes precedence because Kalshi settlement alignment outranks nearest-airport/source-url heuristics"
        )

    provenance = [override["override_provenance"]]
    if metadata.get("metadata_provenance"):
        provenance.append(metadata["metadata_provenance"])

    return {
        "city_key": city.get("city_key"),
        "settlement_source_name": override["settlement_source_name"],
        "settlement_source_url": override["settlement_source_url"],
        "settlement_station_id": station_id,
        "settlement_station_name": metadata.get("station_name"),
        "station_lat": metadata.get("station_lat"),
        "station_lon": metadata.get("station_lon"),
        "confidence_score": 0.99,
        "confidence_bucket": "high",
        "selection_reason": override["selection_reason"],
        "source_evidence": evidence,
        "provenance": provenance,
        "is_complete": all(
            value not in (None, "")
            for value in [
                override["settlement_source_url"],
                station_id,
                metadata.get("station_name"),
                metadata.get("station_lat"),
                metadata.get("station_lon"),
            ]
        ),
    }


def _candidate_from_source_context(city: dict[str, Any], source_context: dict[str, Any]) -> dict[str, Any] | None:
    source_url = source_context["effective_source_url"]
    source_name = source_context["effective_source_name"]
    if not source_url:
        return None

    match = OBHISTORY_STATION_PATTERN.search(source_url)
    if match is None:
        return None

    station_id = match.group("station").upper()
    metadata = KNOWN_STATION_METADATA.get(station_id, {})
    confidence = 0.6
    evidence = list(source_context["evidence"])
    evidence.append(f"parsed station id {station_id} from settlement_source_url")

    if source_context["source_rank"] == "staged_unique":
        confidence += 0.2
    elif source_context["source_rank"] == "config":
        confidence += 0.15
    elif source_context["source_rank"] == "series_hint":
        confidence += 0.1

    if metadata:
        confidence += 0.15
        evidence.append(f"matched station metadata registry for {station_id}")

    confidence = min(confidence, 0.99)
    confidence_bucket = _confidence_bucket(confidence)
    provenance = [source_context["source_provenance"]]
    if metadata.get("metadata_provenance"):
        provenance.append(metadata["metadata_provenance"])

    candidate = {
        "city_key": city.get("city_key"),
        "settlement_source_name": source_name,
        "settlement_source_url": source_url,
        "settlement_station_id": station_id,
        "settlement_station_name": metadata.get("station_name"),
        "station_lat": metadata.get("station_lat"),
        "station_lon": metadata.get("station_lon"),
        "confidence_score": round(confidence, 3),
        "confidence_bucket": confidence_bucket,
        "selection_reason": "derived from authoritative settlement source URL",
        "source_evidence": evidence,
        "provenance": provenance,
        "is_complete": all(
            value not in (None, "")
            for value in [
                source_url,
                station_id,
                metadata.get("station_name"),
                metadata.get("station_lat"),
                metadata.get("station_lon"),
            ]
        ),
    }
    return candidate


def _build_source_context(city: dict[str, Any], staged_summary: dict[str, Any] | None) -> dict[str, Any]:
    evidence: list[str] = []
    notes: list[str] = []
    config_source_name = city.get("settlement_source_name")
    config_source_url = city.get("settlement_source_url")
    staged_source_name = staged_summary.get("staged_settlement_source_name") if staged_summary else None
    staged_source_url = staged_summary.get("staged_settlement_source_url") if staged_summary else None
    staged_source_status = staged_summary["staged_source_status"] if staged_summary else "not_checked"

    if staged_source_status == "unique" and staged_source_url:
        evidence.append("using unique staged Kalshi settlement source metadata")
        effective_source_name = staged_source_name
        effective_source_url = staged_source_url
        source_rank = "staged_unique"
        source_provenance = "staged_kalshi_events"
    elif config_source_url:
        evidence.append("using settlement source already present in config")
        effective_source_name = config_source_name
        effective_source_url = config_source_url
        source_rank = "config"
        source_provenance = "config"
    else:
        hint = SERIES_SOURCE_HINTS.get(str(city.get("kalshi_series_ticker") or ""))
        if hint is not None:
            evidence.append("using curated series source hint because staged Kalshi events are unavailable locally")
            effective_source_name = hint["settlement_source_name"]
            effective_source_url = hint["settlement_source_url"]
            source_rank = "series_hint"
            source_provenance = hint["source_provenance"]
        else:
            effective_source_name = None
            effective_source_url = None
            source_rank = "none"
            source_provenance = "none"

    if staged_source_status == "ambiguous":
        notes.append("staged settlement source metadata is ambiguous")
    elif staged_source_status == "missing_rows":
        notes.append("no staged Kalshi event rows found for enabled city")
    elif staged_source_status == "missing_source":
        notes.append("staged Kalshi event rows exist but settlement source metadata is missing")
    elif staged_source_status == "not_checked":
        notes.append("staged Kalshi events not available locally")

    return {
        "effective_source_name": effective_source_name,
        "effective_source_url": effective_source_url,
        "source_rank": source_rank,
        "source_provenance": source_provenance,
        "staged_source_status": staged_source_status,
        "staged_settlement_source_name": staged_source_name,
        "staged_settlement_source_url": staged_source_url,
        "staged_source_pairs_json": staged_summary.get("staged_source_pairs_json") if staged_summary else None,
        "evidence": evidence,
        "notes": notes,
    }


def _build_report_row(result: dict[str, Any]) -> dict[str, Any]:
    current = result["current_config"]
    selected = result["selected_candidate"] or {}
    notes = list(result["notes"])
    if selected:
        notes.append(
            "recommended fields can be copied directly into configs/cities.yml"
        )

    return {
        "city_key": result["city_key"],
        "city_name": result["city_name"],
        "kalshi_series_ticker": result["kalshi_series_ticker"],
        "settlement_source_name": current.get("settlement_source_name"),
        "settlement_source_url": current.get("settlement_source_url"),
        "staged_settlement_source_name": result["staged_source_name"],
        "staged_settlement_source_url": result["staged_source_url"],
        "staged_source_status": result["staged_source_status"],
        "staged_source_pairs_json": result["staged_source_pairs_json"],
        "settlement_station_id": current.get("settlement_station_id"),
        "settlement_station_name": current.get("settlement_station_name"),
        "station_lat": current.get("station_lat"),
        "station_lon": current.get("station_lon"),
        "recommended_station_id": selected.get("settlement_station_id"),
        "recommended_station_name": selected.get("settlement_station_name"),
        "recommended_station_lat": selected.get("station_lat"),
        "recommended_station_lon": selected.get("station_lon"),
        "recommendation_confidence": selected.get("confidence_score"),
        "selected_automatically": result["selected_automatically"],
        "recommendation_provenance": json.dumps(selected.get("provenance", [])),
        "missing_fields": ",".join(result["missing_fields"]),
        "mapping_complete": result["mapping_complete"],
        "validation_ready": result["mapping_complete"] or result["selected_automatically"],
        "notes": "; ".join(notes),
    }


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


def _confidence_bucket(confidence: float) -> str:
    if confidence >= 0.9:
        return "high"
    if confidence >= 0.75:
        return "medium"
    return "low"


def _events_path(events_path: Path | None) -> Path:
    return events_path or (STAGING_DIR / DEFAULT_EVENTS_FILENAME)


def resolve_ncei_station_id(settlement_station_id: str) -> str:
    """Translate a settlement station identifier into the NCEI station identifier used for NOAA pulls."""
    metadata = KNOWN_STATION_METADATA.get(settlement_station_id.upper())
    if metadata and metadata.get("ncei_station_id"):
        return str(metadata["ncei_station_id"])
    return settlement_station_id
