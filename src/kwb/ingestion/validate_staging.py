from __future__ import annotations

import json
import os
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import pandas as pd

from kwb.ingestion.kalshi_events import DEFAULT_CITIES_CONFIG_PATH, load_enabled_cities
from kwb.ingestion.kalshi_market_history import summarize_kalshi_history_manifest
from kwb.ingestion.kalshi_microstructure import (
    DEFAULT_MICROSTRUCTURE_SNAPSHOTS_FILENAME,
    DEFAULT_ORDERBOOK_LEVELS_FILENAME,
    REQUIRED_MICROSTRUCTURE_SNAPSHOT_COLUMNS,
    REQUIRED_ORDERBOOK_LEVEL_COLUMNS,
)
from kwb.mapping.station_candidates import resolve_enabled_city_station_candidates
from kwb.mapping.station_mapping import collect_station_mapping_issues
from kwb.marts.backtest_dataset import (
    DEFAULT_STAGED_CANDLES_PATH,
    DEFAULT_STAGED_MARKETS_PATH,
    DEFAULT_STAGED_NORMALS_PATH,
    DEFAULT_STAGED_WEATHER_PATH,
    REQUIRED_CANDLES_COLUMNS,
    REQUIRED_MARKETS_COLUMNS,
    REQUIRED_NORMALS_COLUMNS,
    REQUIRED_WEATHER_COLUMNS,
)
from kwb.settings import RAW_DIR, STAGING_DIR
from kwb.utils.logging import get_logger

logger = get_logger(__name__)

DEFAULT_STAGING_VALIDATION_SUMMARY_FILENAME = "staging_validation_summary.json"
DEFAULT_STAGING_BOOTSTRAP_REPORT_FILENAME = "staging_bootstrap_report.md"
REQUIRED_STAGING_DATASETS = ("weather_daily", "weather_normals_daily", "kalshi_markets", "kalshi_candles")

DATASET_SPECS: dict[str, dict[str, Any]] = {
    "weather_daily": {
        "filename": DEFAULT_STAGED_WEATHER_PATH.name,
        "required_columns": REQUIRED_WEATHER_COLUMNS,
        "date_column": "obs_date",
        "date_kind": "date",
        "unique_keys": ["city_key", "station_id", "obs_date"],
        "city_column": "city_key",
        "upstream_dependency": "NOAA NCEI daily observations API (GHCND)",
        "builder_name": "kwb.ingestion.weather_history.ingest_weather_history_for_enabled_cities",
        "local_raw_supported": False,
        "requires_date_range": True,
        "source_of_truth": (
            "Enabled-city settlement station mapping from configs/cities.yml, then NOAA NCEI daily station "
            "observations for the mapped settlement station."
        ),
    },
    "weather_normals_daily": {
        "filename": DEFAULT_STAGED_NORMALS_PATH.name,
        "required_columns": REQUIRED_NORMALS_COLUMNS,
        "date_column": "month_day",
        "date_kind": "month_day",
        "unique_keys": ["city_key", "station_id", "month_day"],
        "city_column": "city_key",
        "upstream_dependency": "NOAA NCEI daily climate normals API (NORMAL_DLY)",
        "builder_name": "kwb.ingestion.climate_normals.ingest_climate_normals_for_enabled_cities",
        "local_raw_supported": False,
        "requires_date_range": False,
        "source_of_truth": (
            "Enabled-city settlement station mapping from configs/cities.yml, then NOAA NCEI daily climate "
            "normals for the mapped settlement station."
        ),
    },
    "kalshi_markets": {
        "filename": DEFAULT_STAGED_MARKETS_PATH.name,
        "required_columns": REQUIRED_MARKETS_COLUMNS,
        "date_column": "strike_date",
        "date_kind": "timestamp",
        "unique_keys": ["city_key", "market_ticker"],
        "city_column": "city_key",
        "upstream_dependency": "Kalshi public events/markets API",
        "builder_name": "kwb.ingestion.kalshi_market_history.ingest_kalshi_market_history_for_enabled_cities",
        "local_raw_supported": False,
        "requires_date_range": True,
        "source_of_truth": (
            "Enabled Kalshi series tickers from configs/cities.yml, then Kalshi series event and market metadata "
            "retrieved through the public API."
        ),
    },
    "kalshi_candles": {
        "filename": DEFAULT_STAGED_CANDLES_PATH.name,
        "required_columns": REQUIRED_CANDLES_COLUMNS,
        "date_column": "candle_ts",
        "date_kind": "timestamp",
        "unique_keys": ["market_ticker", "candle_ts", "interval"],
        "city_column": "city_key",
        "upstream_dependency": "Kalshi public candlestick API",
        "builder_name": "kwb.ingestion.kalshi_market_history.ingest_kalshi_market_history_for_enabled_cities",
        "local_raw_supported": False,
        "requires_date_range": True,
        "source_of_truth": (
            "Enabled Kalshi series tickers from configs/cities.yml, then Kalshi candlestick history for each "
            "discovered market ticker."
        ),
    },
    "kalshi_market_microstructure_snapshots": {
        "filename": DEFAULT_MICROSTRUCTURE_SNAPSHOTS_FILENAME,
        "required_columns": REQUIRED_MICROSTRUCTURE_SNAPSHOT_COLUMNS,
        "date_column": "snapshot_ts",
        "date_kind": "timestamp",
        "unique_keys": ["snapshot_ts", "market_ticker"],
        "city_column": "city_key",
        "upstream_dependency": "Kalshi live/public markets endpoint plus optional orderbook endpoint",
        "builder_name": "kwb.ingestion.kalshi_microstructure.capture_kalshi_microstructure_for_enabled_cities",
        "local_raw_supported": False,
        "requires_date_range": False,
        "require_enabled_city_coverage": False,
        "allow_empty": True,
        "source_of_truth": (
            "Forward-captured Kalshi market microstructure snapshots for enabled weather series, using current "
            "top-of-book fields from the markets endpoint and orderbook depth when available."
        ),
    },
    "kalshi_orderbook_levels": {
        "filename": DEFAULT_ORDERBOOK_LEVELS_FILENAME,
        "required_columns": REQUIRED_ORDERBOOK_LEVEL_COLUMNS,
        "date_column": "snapshot_ts",
        "date_kind": "timestamp",
        "unique_keys": ["snapshot_ts", "market_ticker", "side", "level_rank"],
        "city_column": "city_key",
        "upstream_dependency": "Kalshi market orderbook endpoint",
        "builder_name": "kwb.ingestion.kalshi_microstructure.capture_kalshi_microstructure_for_enabled_cities",
        "local_raw_supported": False,
        "requires_date_range": False,
        "require_enabled_city_coverage": False,
        "allow_empty": True,
        "source_of_truth": (
            "Forward-captured Kalshi orderbook depth for enabled weather series. One row per side/level at each "
            "snapshot timestamp."
        ),
    },
}


def validate_staging_datasets(
    datasets: tuple[str, ...] = REQUIRED_STAGING_DATASETS,
    staging_dir: Path | None = None,
    config_path: Path = DEFAULT_CITIES_CONFIG_PATH,
    summary_output_path: Path | None = None,
) -> dict[str, Any]:
    """Validate staged research inputs required by the climatology baseline."""
    staging_dir = staging_dir or STAGING_DIR
    enabled_cities = load_enabled_cities(config_path)
    enabled_city_keys = sorted(
        city_key for city_key in (str(city.get("city_key")) for city in enabled_cities) if city_key
    )
    raw_files = _discover_local_raw_files()
    station_mapping_issues = collect_station_mapping_issues(config_path=config_path)
    station_resolution = resolve_enabled_city_station_candidates(config_path=config_path)

    summary: dict[str, Any] = {
        "checked_at_utc": datetime.now(timezone.utc).isoformat(),
        "config_path": str(config_path),
        "staging_dir": str(staging_dir),
        "required_datasets": list(datasets),
        "enabled_city_keys": enabled_city_keys,
        "local_raw_files_discovered": [str(path) for path in raw_files],
        "datasets": {},
        "cross_dataset_checks": [],
        "station_mapping": {
            "ready": not station_mapping_issues,
            "issues": station_mapping_issues,
            "recommended_city_keys": [
                result["city_key"] for result in station_resolution["results"] if result["selected_candidate"] is not None
            ],
            "auto_selectable_city_keys": [
                result["city_key"] for result in station_resolution["results"] if result["selected_automatically"]
            ],
        },
        "upstream_environment": {
            "ncei_api_token_configured": bool(os.getenv("NCEI_API_TOKEN")),
        },
        "kalshi_history_progress": summarize_kalshi_history_manifest(output_dir=staging_dir),
        "ready": False,
        "missing_datasets": [],
        "invalid_datasets": [],
        "recommendation": "",
    }

    frames: dict[str, pd.DataFrame] = {}
    for dataset_name in datasets:
        dataset_summary, frame = _validate_single_dataset(
            dataset_name=dataset_name,
            staging_dir=staging_dir,
            enabled_city_keys=enabled_city_keys,
        )
        summary["datasets"][dataset_name] = dataset_summary
        if frame is not None:
            frames[dataset_name] = frame
        if not dataset_summary["exists"]:
            summary["missing_datasets"].append(dataset_name)
        if dataset_summary["errors"]:
            summary["invalid_datasets"].append(dataset_name)

    summary["cross_dataset_checks"] = _run_cross_dataset_checks(
        frames=frames,
        enabled_city_keys=enabled_city_keys,
        datasets=datasets,
    )
    if any(check["status"] == "error" for check in summary["cross_dataset_checks"]):
        summary["invalid_datasets"].extend(
            sorted(
                {
                    dataset_name
                    for check in summary["cross_dataset_checks"]
                    for dataset_name in check.get("datasets", [])
                }
            )
        )

    summary["invalid_datasets"] = sorted(set(summary["invalid_datasets"]))
    summary["ready"] = not summary["missing_datasets"] and not summary["invalid_datasets"]
    summary["recommendation"] = _build_validation_recommendation(summary)

    output_path = summary_output_path or (staging_dir / DEFAULT_STAGING_VALIDATION_SUMMARY_FILENAME)
    output_path.parent.mkdir(parents=True, exist_ok=True)
    _write_json(output_path, summary)
    summary["summary_output_path"] = str(output_path)
    return summary


def check_climatology_baseline_readiness(
    staging_dir: Path | None = None,
    config_path: Path = DEFAULT_CITIES_CONFIG_PATH,
    summary_output_path: Path | None = None,
) -> dict[str, Any]:
    """Return a compact readiness answer for the climatology baseline runner."""
    validation = validate_staging_datasets(
        datasets=REQUIRED_STAGING_DATASETS,
        staging_dir=staging_dir,
        config_path=config_path,
        summary_output_path=summary_output_path,
    )
    return {
        "ready": validation["ready"],
        "config_path": validation["config_path"],
        "staging_dir": validation["staging_dir"],
        "required_datasets": validation["required_datasets"],
        "enabled_city_keys": validation["enabled_city_keys"],
        "station_mapping": validation["station_mapping"],
        "missing_datasets": validation["missing_datasets"],
        "invalid_datasets": validation["invalid_datasets"],
        "validation_summary_path": validation["summary_output_path"],
        "datasets": {
            name: {
                "path": payload["path"],
                "exists": payload["exists"],
                "valid": not payload["errors"],
                "row_count": payload["row_count"],
                "date_coverage": payload["date_coverage"],
            }
            for name, payload in validation["datasets"].items()
        },
        "recommendation": validation["recommendation"],
        "kalshi_history_progress": validation.get("kalshi_history_progress"),
    }


def render_staging_bootstrap_report(summary: dict[str, Any]) -> str:
    """Render a compact human-readable staging/bootstrap report."""
    lines = [
        "# Staging Bootstrap Report",
        "",
        f"- Checked at (UTC): `{summary.get('checked_at_utc') or summary.get('built_at_utc')}`",
        f"- Ready for baseline runner: `{summary.get('ready')}`",
        f"- Config path: `{summary.get('config_path')}`",
        f"- Staging dir: `{summary.get('staging_dir')}`",
        f"- Station mapping ready: `{summary.get('station_mapping', {}).get('ready')}`",
        "",
    ]

    if summary.get("errors"):
        lines.extend(["## Build Errors", ""])
        for error in summary["errors"]:
            lines.append(f"- `{error}`")
        lines.append("")

    lines.extend(["## Datasets", ""])

    datasets = summary.get("datasets", {})
    for dataset_name in REQUIRED_STAGING_DATASETS:
        payload = datasets.get(dataset_name)
        if not payload:
            continue
        lines.extend(
            [
                f"### {dataset_name}",
                "",
                f"- File: `{payload.get('path')}`",
                f"- Exists: `{payload.get('exists')}`",
                f"- Row count: `{payload.get('row_count')}`",
                (
                    f"- Date coverage: `{payload.get('date_coverage', {}).get('min')}` to "
                    f"`{payload.get('date_coverage', {}).get('max')}`"
                ),
                f"- Source of truth: {payload.get('source_of_truth')}",
                f"- Builder: `{payload.get('builder_name')}`",
            ]
        )
        if payload.get("errors"):
            lines.append(f"- Errors: `{' | '.join(payload['errors'])}`")
        if payload.get("warnings"):
            lines.append(f"- Warnings: `{' | '.join(payload['warnings'])}`")
        lines.append("")

    if summary.get("cross_dataset_checks"):
        lines.extend(["## Cross-Dataset Checks", ""])
        for check in summary["cross_dataset_checks"]:
            lines.append(f"- `{check['status']}` {check['message']}")
        lines.append("")

    kalshi_history_progress = summary.get("kalshi_history_progress")
    if kalshi_history_progress:
        lines.extend(
            [
                "## Kalshi Chunk Progress",
                "",
                f"- Status: `{kalshi_history_progress.get('status')}`",
                f"- Manifest: `{kalshi_history_progress.get('manifest_path')}`",
                f"- Chunk dir: `{kalshi_history_progress.get('chunk_dir')}`",
                f"- Completed market chunks: `{kalshi_history_progress.get('completed_market_chunks')}`",
                f"- Completed candle chunks: `{kalshi_history_progress.get('completed_candle_chunks')}`",
                f"- Failed candle chunks: `{kalshi_history_progress.get('failed_candle_chunks')}`",
                f"- Retries used: `{kalshi_history_progress.get('retry_summary', {}).get('total_retries')}`",
            ]
        )
        if kalshi_history_progress.get("last_error"):
            lines.append(f"- Last error: `{kalshi_history_progress.get('last_error')}`")
        if kalshi_history_progress.get("resume_recommended"):
            lines.append("- Resume next with `kwb data build-staging --resume --dataset kalshi_markets --dataset kalshi_candles ...`")
        lines.append("")

    raw_files = summary.get("local_raw_files_discovered", [])
    lines.extend(
        [
            "## Environment",
            "",
            f"- NCEI_API_TOKEN configured: `{summary.get('upstream_environment', {}).get('ncei_api_token_configured')}`",
            "",
            "## Local Raw Inputs",
            "",
            f"- Raw files discovered under `data/raw`: `{len(raw_files)}`",
        ]
    )
    if raw_files:
        lines.append(f"- Files: `{', '.join(raw_files)}`")
    else:
        lines.append("- Files: `none`")

    lines.extend(
        [
            "",
            "## Station Mapping",
            "",
        ]
    )
    station_mapping = summary.get("station_mapping", {})
    if station_mapping.get("issues"):
        for issue in station_mapping["issues"]:
            lines.append(f"- `{issue}`")
    else:
        lines.append("- `enabled city station mapping validation passed`")

    lines.extend(
        [
            "",
            "## Next Step",
            "",
            f"- {summary.get('recommendation')}",
            "",
        ]
    )
    return "\n".join(lines)


def _validate_single_dataset(
    dataset_name: str,
    staging_dir: Path,
    enabled_city_keys: list[str],
) -> tuple[dict[str, Any], pd.DataFrame | None]:
    if dataset_name not in DATASET_SPECS:
        raise ValueError(f"Unsupported staging dataset {dataset_name!r}. Expected one of {sorted(DATASET_SPECS)}.")

    spec = DATASET_SPECS[dataset_name]
    path = staging_dir / spec["filename"]
    summary = {
        "dataset": dataset_name,
        "path": str(path),
        "filename": spec["filename"],
        "exists": path.exists(),
        "row_count": 0,
        "columns": [],
        "date_coverage": {"min": None, "max": None},
        "duplicate_rows": 0,
        "city_keys_present": [],
        "errors": [],
        "warnings": [],
        "source_of_truth": spec["source_of_truth"],
        "builder_name": spec["builder_name"],
        "upstream_dependency": spec["upstream_dependency"],
        "local_raw_supported": spec["local_raw_supported"],
        "requires_date_range": spec["requires_date_range"],
    }
    if not path.exists():
        summary["errors"].append(
            f"Missing staged dataset {dataset_name}: expected file at {path}."
        )
        return summary, None

    try:
        frame = pd.read_parquet(path)
    except Exception as exc:  # pragma: no cover - defensive
        summary["errors"].append(f"Failed to read {path}: {exc}")
        return summary, None

    summary["row_count"] = int(len(frame))
    summary["columns"] = sorted(str(column) for column in frame.columns)
    if frame.empty:
        if spec.get("allow_empty", False):
            summary["warnings"].append(f"Dataset {dataset_name} is empty.")
            return summary, frame
        summary["errors"].append(f"Dataset {dataset_name} is empty.")
        return summary, frame

    missing_columns = sorted(spec["required_columns"] - set(frame.columns))
    if missing_columns:
        summary["errors"].append(
            f"Dataset {dataset_name} is missing required columns: {', '.join(missing_columns)}."
        )
        return summary, frame

    duplicate_count = int(frame.duplicated(subset=spec["unique_keys"]).sum())
    summary["duplicate_rows"] = duplicate_count
    if duplicate_count:
        summary["errors"].append(
            f"Dataset {dataset_name} has {duplicate_count} duplicate rows on keys {spec['unique_keys']}."
        )

    summary["city_keys_present"] = _sorted_unique_strings(frame[spec["city_column"]])
    if spec.get("require_enabled_city_coverage", True):
        missing_cities = sorted(set(enabled_city_keys) - set(summary["city_keys_present"]))
        if missing_cities:
            summary["errors"].append(
                f"Dataset {dataset_name} is missing enabled city coverage for: {', '.join(missing_cities)}."
            )

    date_coverage, date_errors = _compute_date_coverage(frame, spec["date_column"], spec["date_kind"])
    summary["date_coverage"] = date_coverage
    summary["errors"].extend(date_errors)

    if dataset_name == "kalshi_candles":
        candle_errors = _validate_candle_integrity(frame)
        summary["errors"].extend(candle_errors)
    if dataset_name == "kalshi_orderbook_levels":
        level_errors = _validate_orderbook_level_integrity(frame)
        summary["errors"].extend(level_errors)

    return summary, frame


def _compute_date_coverage(frame: pd.DataFrame, column: str, date_kind: str) -> tuple[dict[str, str | None], list[str]]:
    if column not in frame.columns:
        return {"min": None, "max": None}, [f"Date coverage column {column!r} is missing."]

    series = frame[column]
    if date_kind == "month_day":
        if series.isna().any():
            return {"min": None, "max": None}, [f"Column {column!r} contains null month_day values."]
        invalid = [str(value) for value in series.astype(str) if len(str(value)) != 5 or str(value)[2] != "-"]
        if invalid:
            return {"min": None, "max": None}, [f"Column {column!r} contains invalid month_day values."]
        values = sorted(series.astype(str).tolist())
        return {"min": values[0], "max": values[-1]}, []

    parsed = pd.to_datetime(series, utc=(date_kind == "timestamp"), errors="coerce")
    if parsed.isna().any():
        return {"min": None, "max": None}, [f"Column {column!r} contains invalid datetime values."]
    if date_kind == "date":
        normalized = parsed.dt.date.astype(str)
    else:
        normalized = parsed.dt.strftime("%Y-%m-%dT%H:%M:%S%z")
    values = normalized.tolist()
    return {"min": min(values), "max": max(values)}, []


def _validate_candle_integrity(frame: pd.DataFrame) -> list[str]:
    errors: list[str] = []
    parsed = pd.to_datetime(frame["candle_ts"], utc=True, errors="coerce")
    if parsed.isna().any():
        errors.append("Dataset kalshi_candles contains invalid candle_ts values.")
        return errors

    for market_ticker, market_df in frame.assign(_parsed_ts=parsed).groupby("market_ticker", sort=False):
        if market_df["_parsed_ts"].is_monotonic_increasing:
            continue
        errors.append(f"Dataset kalshi_candles has non-monotonic candle_ts ordering for market_ticker={market_ticker}.")
        break
    return errors


def _run_cross_dataset_checks(
    frames: dict[str, pd.DataFrame],
    enabled_city_keys: list[str],
    datasets: tuple[str, ...],
) -> list[dict[str, Any]]:
    checks: list[dict[str, Any]] = []
    if enabled_city_keys:
        checks.append(
            {
                "status": "ok",
                "datasets": [],
                "message": f"Enabled cities in config: {', '.join(enabled_city_keys)}.",
            }
        )

    if "kalshi_markets" in datasets and "kalshi_candles" in datasets:
        markets_df = frames.get("kalshi_markets")
        candles_df = frames.get("kalshi_candles")
        if markets_df is None or candles_df is None:
            checks.append(
                {
                    "status": "error",
                    "datasets": ["kalshi_markets", "kalshi_candles"],
                    "message": "Cannot validate market/candle ticker overlap because one or both datasets are unavailable.",
                }
            )
        else:
            market_tickers = set(markets_df["market_ticker"].dropna().astype(str))
            candle_tickers = set(candles_df["market_ticker"].dropna().astype(str))
            missing_candles = sorted(market_tickers - candle_tickers)
            orphan_candles = sorted(candle_tickers - market_tickers)
            if missing_candles:
                checks.append(
                    {
                        "status": "error",
                        "datasets": ["kalshi_markets", "kalshi_candles"],
                        "message": f"Candles are missing for {len(missing_candles)} staged market tickers.",
                    }
                )
            else:
                checks.append(
                    {
                        "status": "ok",
                        "datasets": ["kalshi_markets", "kalshi_candles"],
                        "message": "Every staged market_ticker has at least one staged candle row.",
                    }
                )
            if orphan_candles:
                checks.append(
                    {
                        "status": "error",
                        "datasets": ["kalshi_markets", "kalshi_candles"],
                        "message": f"Candle data contains {len(orphan_candles)} market_tickers absent from kalshi_markets.",
                    }
                )

    if "kalshi_market_microstructure_snapshots" in datasets and "kalshi_orderbook_levels" in datasets:
        snapshots_df = frames.get("kalshi_market_microstructure_snapshots")
        levels_df = frames.get("kalshi_orderbook_levels")
        if snapshots_df is None or levels_df is None:
            checks.append(
                {
                    "status": "error",
                    "datasets": ["kalshi_market_microstructure_snapshots", "kalshi_orderbook_levels"],
                    "message": "Cannot validate snapshot/level alignment because one or both datasets are unavailable.",
                }
            )
        elif levels_df.empty:
            checks.append(
                {
                    "status": "ok",
                    "datasets": ["kalshi_market_microstructure_snapshots", "kalshi_orderbook_levels"],
                    "message": "No orderbook level rows are staged yet; snapshot dataset exists without depth rows.",
                }
            )
        else:
            snapshot_keys = set(
                zip(
                    snapshots_df["snapshot_ts"].astype(str),
                    snapshots_df["market_ticker"].astype(str),
                    strict=False,
                )
            )
            level_keys = set(
                zip(
                    levels_df["snapshot_ts"].astype(str),
                    levels_df["market_ticker"].astype(str),
                    strict=False,
                )
            )
            orphan_levels = sorted(level_keys - snapshot_keys)
            if orphan_levels:
                checks.append(
                    {
                        "status": "error",
                        "datasets": ["kalshi_market_microstructure_snapshots", "kalshi_orderbook_levels"],
                        "message": (
                            "Orderbook levels contain snapshot_ts/market_ticker pairs absent from the snapshot dataset."
                        ),
                    }
                )
            else:
                checks.append(
                    {
                        "status": "ok",
                        "datasets": ["kalshi_market_microstructure_snapshots", "kalshi_orderbook_levels"],
                        "message": "Every staged orderbook level row maps to a staged microstructure snapshot row.",
                    }
                )

    return checks


def _validate_orderbook_level_integrity(frame: pd.DataFrame) -> list[str]:
    errors: list[str] = []
    for (_, _, side), side_df in frame.groupby(["snapshot_ts", "market_ticker", "side"], sort=False):
        if not side_df["level_rank"].is_monotonic_increasing:
            errors.append("Dataset kalshi_orderbook_levels has non-monotonic level_rank ordering within a side.")
            break
        if not side_df["price_cents"].is_monotonic_decreasing:
            errors.append("Dataset kalshi_orderbook_levels has bid levels that are not sorted from best to worse price.")
            break
    return errors


def _build_validation_recommendation(summary: dict[str, Any]) -> str:
    station_mapping = summary.get("station_mapping", {})
    if station_mapping and not station_mapping.get("ready", True):
        if station_mapping.get("auto_selectable_city_keys"):
            return (
                "Station mapping is incomplete but high-confidence recommendations exist. "
                "Run: kwb station recommend --write-config"
            )
        return "Station mapping is incomplete. Run: kwb station recommend"

    if summary["ready"]:
        return "Baseline inputs look ready. Run: kwb research run-climatology-baseline"

    build_errors = summary.get("errors", [])
    if build_errors:
        first_error = str(build_errors[0])
        if "429" in first_error:
            return "Kalshi API rate-limited the staging build. Resume with: kwb data build-staging --resume --start-date YYYY-MM-DD --end-date YYYY-MM-DD"
        if "400 Client Error" in first_error and "ncei.noaa.gov" in first_error:
            return "NOAA staging pull failed upstream. Check the NCEI station mapping and rerun: kwb data build-staging"
        if "ConnectionError" in first_error or "NameResolutionError" in first_error:
            return "Upstream network access failed during staging build. Confirm external access to NOAA and Kalshi, then rerun: kwb data build-staging"

    missing = summary["missing_datasets"]
    if missing:
        needs_dates = any(DATASET_SPECS[name]["requires_date_range"] for name in missing)
        if needs_dates:
            return (
                "Build the missing staging datasets with explicit dates, for example: "
                "kwb data build-staging --start-date YYYY-MM-DD --end-date YYYY-MM-DD"
            )
        return "Build the missing staging datasets with: kwb data build-staging"

    return "Fix the staged schema/integrity issues reported above, then rerun: kwb data validate-staging"


def _sorted_unique_strings(series: pd.Series) -> list[str]:
    return sorted(str(value) for value in series.dropna().astype(str).unique().tolist())


def _discover_local_raw_files() -> list[Path]:
    if not RAW_DIR.exists():
        return []
    return sorted(path for path in RAW_DIR.rglob("*") if path.is_file())


def _write_json(path: Path, payload: dict[str, Any]) -> None:
    path.write_text(json.dumps(payload, indent=2, sort_keys=True), encoding="utf-8")
