from __future__ import annotations

from datetime import datetime, timezone
from pathlib import Path
from typing import TYPE_CHECKING, Any

import pandas as pd

from kwb.ingestion.climate_normals import ingest_climate_normals_for_enabled_cities
from kwb.ingestion.kalshi_events import DEFAULT_CITIES_CONFIG_PATH
from kwb.ingestion.kalshi_market_history import (
    DEFAULT_CANDLES_FILENAME,
    KalshiHistoryIngestionError,
    DEFAULT_MARKETS_FILENAME,
    ingest_kalshi_market_history_for_enabled_cities,
    summarize_kalshi_history_manifest,
)
from kwb.ingestion.validate_staging import (
    DATASET_SPECS,
    DEFAULT_STAGING_BOOTSTRAP_REPORT_FILENAME,
    DEFAULT_STAGING_VALIDATION_SUMMARY_FILENAME,
    REQUIRED_STAGING_DATASETS,
    render_staging_bootstrap_report,
    validate_staging_datasets,
)
from kwb.ingestion.weather_history import ingest_weather_history_for_enabled_cities
from kwb.settings import STAGING_DIR
from kwb.utils.logging import get_logger

if TYPE_CHECKING:
    from kwb.clients.kalshi import KalshiClient
    from kwb.clients.ncei import NCEIClient

logger = get_logger(__name__)


def build_staging_datasets(
    datasets: tuple[str, ...] = REQUIRED_STAGING_DATASETS,
    config_path: Path = DEFAULT_CITIES_CONFIG_PATH,
    staging_dir: Path | None = None,
    summary_output_path: Path | None = None,
    report_output_path: Path | None = None,
    start_date: str | None = None,
    end_date: str | None = None,
    interval: str = "1h",
    overwrite: bool = False,
    resume: bool = False,
    max_retries: int = 4,
    initial_backoff_seconds: float = 1.0,
    max_backoff_seconds: float = 30.0,
    events_path: Path | None = None,
    ncei_client: "NCEIClient | None" = None,
    kalshi_client: "KalshiClient | None" = None,
) -> dict[str, Any]:
    """Build the required staged datasets through the repo's existing ingestion paths."""
    staging_dir = staging_dir or STAGING_DIR
    staging_dir.mkdir(parents=True, exist_ok=True)
    summary_output_path = summary_output_path or (staging_dir / DEFAULT_STAGING_VALIDATION_SUMMARY_FILENAME)
    report_output_path = report_output_path or (staging_dir / DEFAULT_STAGING_BOOTSTRAP_REPORT_FILENAME)

    _validate_dataset_selection(datasets)

    summary: dict[str, Any] = {
        "built_at_utc": datetime.now(timezone.utc).isoformat(),
        "config_path": str(config_path),
        "staging_dir": str(staging_dir),
        "requested_datasets": list(datasets),
        "start_date": start_date,
        "end_date": end_date,
        "interval": interval,
        "overwrite": overwrite,
        "resume": resume,
        "datasets": {},
        "success": False,
        "errors": [],
        "validation_summary_path": str(summary_output_path),
        "bootstrap_report_path": str(report_output_path),
    }

    if {"weather_daily", "kalshi_markets", "kalshi_candles"} & set(datasets):
        if not start_date or not end_date:
            raise ValueError(
                "Building weather_daily, kalshi_markets, or kalshi_candles requires both start_date and end_date."
            )

    build_groups = _build_groups_for_selection(datasets)
    for group_name in build_groups:
        try:
            if group_name == "weather_daily":
                outpath = staging_dir / DATASET_SPECS["weather_daily"]["filename"]
                _maybe_fail_on_existing(outpath, overwrite)
                built_path = ingest_weather_history_for_enabled_cities(
                    start_date=start_date or "",
                    end_date=end_date or "",
                    config_path=config_path,
                    output_dir=staging_dir,
                    events_path=events_path,
                    client=ncei_client,
                )
                summary["datasets"]["weather_daily"] = {"status": "built", "path": str(built_path)}
            elif group_name == "weather_normals_daily":
                outpath = staging_dir / DATASET_SPECS["weather_normals_daily"]["filename"]
                _maybe_fail_on_existing(outpath, overwrite)
                built_path, row_count, station_count = ingest_climate_normals_for_enabled_cities(
                    config_path=config_path,
                    output_dir=staging_dir,
                    events_path=events_path,
                    client=ncei_client,
                )
                summary["datasets"]["weather_normals_daily"] = {
                    "status": "built",
                    "path": str(built_path),
                    "row_count": row_count,
                    "station_count": station_count,
                }
            elif group_name == "kalshi_market_history":
                market_path = staging_dir / DEFAULT_MARKETS_FILENAME
                candle_path = staging_dir / DEFAULT_CANDLES_FILENAME
                _maybe_fail_on_existing(market_path, overwrite)
                _maybe_fail_on_existing(candle_path, overwrite)
                built_market_path, built_candle_path, kalshi_details = ingest_kalshi_market_history_for_enabled_cities(
                    start_date=start_date or "",
                    end_date=end_date or "",
                    interval=interval,
                    config_path=config_path,
                    output_dir=staging_dir,
                    client=kalshi_client,
                    resume=resume,
                    max_retries=max_retries,
                    initial_backoff_seconds=initial_backoff_seconds,
                    max_backoff_seconds=max_backoff_seconds,
                    return_details=True,
                )
                summary["datasets"]["kalshi_markets"] = {"status": "built", "path": str(built_market_path)}
                summary["datasets"]["kalshi_candles"] = {"status": "built", "path": str(built_candle_path)}
                summary["kalshi_history_progress"] = kalshi_details
        except KalshiHistoryIngestionError as exc:
            summary["kalshi_history_progress"] = exc.details
            affected = _datasets_for_group(group_name)
            message = f"{group_name} build failed: {exc.__cause__ or exc}"
            summary["errors"].append(message)
            for dataset_name in affected:
                summary["datasets"].setdefault(dataset_name, {"status": "failed", "path": None})
                summary["datasets"][dataset_name]["status"] = "failed"
                summary["datasets"][dataset_name]["error"] = str(exc.__cause__ or exc)
        except Exception as exc:
            affected = _datasets_for_group(group_name)
            message = f"{group_name} build failed: {exc}"
            summary["errors"].append(message)
            for dataset_name in affected:
                summary["datasets"].setdefault(dataset_name, {"status": "failed", "path": None})
                summary["datasets"][dataset_name]["status"] = "failed"
                summary["datasets"][dataset_name]["error"] = str(exc)

    validation = validate_staging_datasets(
        datasets=datasets,
        staging_dir=staging_dir,
        config_path=config_path,
        summary_output_path=summary_output_path,
    )
    summary["datasets"] = {
        dataset_name: {
            **summary["datasets"].get(dataset_name, {}),
            **validation["datasets"][dataset_name],
        }
        for dataset_name in datasets
    }
    summary["success"] = validation["ready"] and not summary["errors"]
    summary["ready"] = validation["ready"]
    summary["missing_datasets"] = validation["missing_datasets"]
    summary["invalid_datasets"] = validation["invalid_datasets"]
    summary["recommendation"] = _build_staging_recommendation(
        build_errors=summary["errors"],
        validation_recommendation=validation["recommendation"],
    )
    summary["local_raw_files_discovered"] = validation["local_raw_files_discovered"]
    summary["cross_dataset_checks"] = validation["cross_dataset_checks"]
    summary["station_mapping"] = validation["station_mapping"]
    summary["upstream_environment"] = validation.get("upstream_environment", {})
    if "kalshi_history_progress" not in summary:
        summary["kalshi_history_progress"] = summarize_kalshi_history_manifest(staging_dir)

    report_output_path.write_text(render_staging_bootstrap_report(summary), encoding="utf-8")
    logger.info(
        "Completed staging build with success=%s for datasets=%s",
        summary["success"],
        ",".join(datasets),
    )
    return summary


def _build_groups_for_selection(datasets: tuple[str, ...]) -> list[str]:
    groups: list[str] = []
    if "weather_daily" in datasets:
        groups.append("weather_daily")
    if "weather_normals_daily" in datasets:
        groups.append("weather_normals_daily")
    if {"kalshi_markets", "kalshi_candles"} & set(datasets):
        groups.append("kalshi_market_history")
    return groups


def _datasets_for_group(group_name: str) -> tuple[str, ...]:
    if group_name == "weather_daily":
        return ("weather_daily",)
    if group_name == "weather_normals_daily":
        return ("weather_normals_daily",)
    if group_name == "kalshi_market_history":
        return ("kalshi_markets", "kalshi_candles")
    raise ValueError(f"Unsupported build group {group_name!r}.")


def _validate_dataset_selection(datasets: tuple[str, ...]) -> None:
    unknown = sorted(set(datasets) - set(REQUIRED_STAGING_DATASETS))
    if unknown:
        raise ValueError(f"Unsupported dataset selection: {', '.join(unknown)}")


def _maybe_fail_on_existing(path: Path, overwrite: bool) -> None:
    if path.exists() and not overwrite:
        raise ValueError(f"Refusing to overwrite existing staged file without overwrite=True: {path}")


def _build_staging_recommendation(build_errors: list[str], validation_recommendation: str) -> str:
    if not build_errors:
        return validation_recommendation

    first_error = build_errors[0]
    if "NCEI_API_TOKEN is not set" in first_error:
        return "Set NCEI_API_TOKEN, then rerun: kwb data build-staging --start-date YYYY-MM-DD --end-date YYYY-MM-DD"
    if "429" in first_error:
        return "Kalshi API rate-limited the build. Resume with: kwb data build-staging --resume --start-date YYYY-MM-DD --end-date YYYY-MM-DD"
    return validation_recommendation
