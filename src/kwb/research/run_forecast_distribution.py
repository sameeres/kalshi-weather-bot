from __future__ import annotations

import json
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import pandas as pd

from kwb.backtest.evaluate_forecast_distribution import evaluate_forecast_distribution_signals
from kwb.marts.backtest_dataset import build_backtest_dataset
from kwb.models.baseline_climatology import score_climatology_baseline
from kwb.models.forecast_distribution import DEFAULT_FORECAST_SNAPSHOTS_PATH, score_forecast_distribution
from kwb.settings import CONFIG_DIR, MARTS_DIR
from kwb.utils.logging import get_logger

logger = get_logger(__name__)

DEFAULT_RESEARCH_RUNS_DIR = MARTS_DIR / "forecast_distribution_runs"
DEFAULT_MANIFEST_FILENAME = "research_manifest_forecast_distribution.json"
DEFAULT_COVERAGE_JSON_FILENAME = "forecast_snapshot_coverage.json"
DEFAULT_COVERAGE_MARKDOWN_FILENAME = "forecast_snapshot_coverage.md"


class ForecastDistributionResearchRunError(ValueError):
    """Raised when the forecast-distribution research runner cannot complete safely."""


def run_forecast_distribution_research(
    decision_time_local: str = "10:00",
    output_dir: Path | None = None,
    overwrite: bool = False,
    config_path: Path | None = None,
    weather_path: Path | None = None,
    normals_path: Path | None = None,
    markets_path: Path | None = None,
    candles_path: Path | None = None,
    history_path: Path | None = None,
    forecast_snapshots_path: Path | None = None,
    day_window: int = 1,
    min_lookback_samples: int = 30,
    min_edge: float = 0.05,
    min_samples: int = 30,
    min_price: float = 0.0,
    max_price: float = 25.0,
    contracts: int = 1,
    fee_per_contract: float = 0.01,
    allow_no: bool = False,
    fold_count: int = 3,
) -> tuple[Path, Path, dict[str, Any]]:
    run_started_at = datetime.now(timezone.utc)
    run_dir = _prepare_run_directory(output_dir=output_dir, overwrite=overwrite, run_started_at=run_started_at)
    config_path = config_path or (CONFIG_DIR / "cities.yml")
    forecast_snapshots_path = forecast_snapshots_path or DEFAULT_FORECAST_SNAPSHOTS_PATH

    backtest_path, backtest_stats = build_backtest_dataset(
        decision_time_local=decision_time_local,
        config_path=config_path,
        weather_path=weather_path,
        normals_path=normals_path,
        markets_path=markets_path,
        candles_path=candles_path,
        output_dir=run_dir,
    )
    coverage_json_path = run_dir / DEFAULT_COVERAGE_JSON_FILENAME
    coverage_markdown_path = run_dir / DEFAULT_COVERAGE_MARKDOWN_FILENAME
    coverage_summary = build_forecast_snapshot_coverage_summary(
        backtest_dataset_path=backtest_path,
        forecast_snapshots_path=forecast_snapshots_path,
    )
    coverage_json_path.write_text(json.dumps(coverage_summary, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    coverage_markdown_path.write_text(render_forecast_snapshot_coverage_markdown(coverage_summary), encoding="utf-8")

    climatology_scored_path, climatology_summary = score_climatology_baseline(
        backtest_dataset_path=backtest_path,
        history_path=history_path,
        output_dir=run_dir,
        day_window=day_window,
        min_lookback_samples=min_lookback_samples,
    )
    forecast_scored_path, forecast_summary = score_forecast_distribution(
        backtest_dataset_path=backtest_path,
        history_path=history_path,
        forecast_snapshots_path=forecast_snapshots_path,
        output_dir=run_dir,
        day_window=day_window,
        min_lookback_samples=min_lookback_samples,
    )
    trades_path, comparison_summary_path, comparison_report_path, comparison_summary = evaluate_forecast_distribution_signals(
        climatology_scored_path=climatology_scored_path,
        forecast_scored_path=forecast_scored_path,
        output_dir=run_dir,
        min_edge=min_edge,
        min_samples=min_samples,
        min_price=min_price,
        max_price=max_price,
        contracts=contracts,
        fee_per_contract=fee_per_contract,
        allow_no=allow_no,
        fold_count=fold_count,
    )

    manifest = {
        "run_timestamp_utc": run_started_at.isoformat(),
        "run_directory": str(run_dir),
        "decision_time_local": decision_time_local,
        "input_paths": {
            "config_path": str(config_path),
            "forecast_snapshots_path": str(forecast_snapshots_path),
        },
        "output_paths": {
            "backtest_dataset": str(backtest_path),
            "forecast_coverage_json": str(coverage_json_path),
            "forecast_coverage_report": str(coverage_markdown_path),
            "climatology_scored": str(climatology_scored_path),
            "forecast_scored": str(forecast_scored_path),
            "comparison_trades": str(trades_path),
            "comparison_summary": str(comparison_summary_path),
            "comparison_report": str(comparison_report_path),
        },
        "row_counts": {
            "backtest_rows": int(backtest_stats.get("rows_written", 0)),
            "climatology_rows_scored": int(climatology_summary.get("rows_scored", 0)),
            "forecast_rows_scored": int(forecast_summary.get("rows_scored", 0)),
        },
        "forecast_coverage": coverage_summary,
        "parameters": {
            "day_window": day_window,
            "min_lookback_samples": min_lookback_samples,
            "min_edge": min_edge,
            "min_samples": min_samples,
            "min_price": min_price,
            "max_price": max_price,
            "contracts": contracts,
            "fee_per_contract": fee_per_contract,
            "allow_no": allow_no,
            "fold_count": fold_count,
        },
        "comparison_summary": comparison_summary,
    }

    manifest_path = run_dir / DEFAULT_MANIFEST_FILENAME
    manifest_path.write_text(json.dumps(manifest, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    logger.info("Saved forecast-distribution research manifest to %s", manifest_path)
    return run_dir, manifest_path, manifest


def build_forecast_snapshot_coverage_summary(
    backtest_dataset_path: Path,
    forecast_snapshots_path: Path,
    max_snapshot_age_hours: float = 18.0,
) -> dict[str, Any]:
    backtest_df = pd.read_parquet(backtest_dataset_path).copy()
    forecast_df = pd.read_parquet(forecast_snapshots_path).copy()
    return _build_forecast_snapshot_coverage_summary_from_frames(
        backtest_df=backtest_df,
        forecast_df=forecast_df,
        max_snapshot_age_hours=max_snapshot_age_hours,
    )


def _build_forecast_snapshot_coverage_summary_from_frames(
    backtest_df: pd.DataFrame,
    forecast_df: pd.DataFrame,
    max_snapshot_age_hours: float,
) -> dict[str, Any]:
    prepared_backtest = _prepare_backtest_coverage_frame(backtest_df)
    prepared_forecast = _prepare_forecast_coverage_frame(forecast_df)

    counts_by_city_date = []
    earliest_latest_by_city = []
    if not prepared_forecast.empty:
        counts = (
            prepared_forecast.groupby(["city_key", "snapshot_date_local"], sort=True)
            .agg(
                snapshots=("snapshot_ts", "nunique"),
                periods=("period_start_ts", "count"),
            )
            .reset_index()
        )
        counts_by_city_date = counts.to_dict("records")

        bounds = (
            prepared_forecast.groupby("city_key", sort=True)
            .agg(
                earliest_snapshot_ts=("snapshot_ts", "min"),
                latest_snapshot_ts=("snapshot_ts", "max"),
                distinct_snapshot_count=("snapshot_ts", "nunique"),
            )
            .reset_index()
        )
        earliest_latest_by_city = [
            {
                "city_key": str(row["city_key"]),
                "earliest_snapshot_ts": row["earliest_snapshot_ts"].isoformat() if pd.notna(row["earliest_snapshot_ts"]) else None,
                "latest_snapshot_ts": row["latest_snapshot_ts"].isoformat() if pd.notna(row["latest_snapshot_ts"]) else None,
                "distinct_snapshot_count": int(row["distinct_snapshot_count"]),
            }
            for row in bounds.to_dict("records")
        ]

    matched_city_counts: dict[str, int] = {}
    eligible_city_counts: dict[str, int] = {}
    matched_rows = 0
    for row in prepared_backtest.to_dict("records"):
        city_key = str(row["city_key"])
        eligible_city_counts[city_key] = eligible_city_counts.get(city_key, 0) + 1
        matched = _row_has_matching_forecast_snapshot(
            forecast_df=prepared_forecast.loc[prepared_forecast["city_key"] == city_key].copy(),
            decision_ts=row["decision_ts"],
            event_date=row["event_date"],
            max_snapshot_age_hours=max_snapshot_age_hours,
        )
        if matched:
            matched_rows += 1
            matched_city_counts[city_key] = matched_city_counts.get(city_key, 0) + 1

    eligible_rows = int(len(prepared_backtest))
    matched_share = round(float(matched_rows / eligible_rows), 6) if eligible_rows else 0.0
    warnings = _coverage_warnings(
        eligible_rows=eligible_rows,
        matched_rows=matched_rows,
        matched_share=matched_share,
        earliest_latest_by_city=earliest_latest_by_city,
        eligible_city_counts=eligible_city_counts,
        matched_city_counts=matched_city_counts,
    )

    overall_earliest = None
    overall_latest = None
    if not prepared_forecast.empty:
        overall_earliest = prepared_forecast["snapshot_ts"].min()
        overall_latest = prepared_forecast["snapshot_ts"].max()

    return {
        "generated_at_utc": datetime.now(timezone.utc).isoformat(),
        "max_snapshot_age_hours": max_snapshot_age_hours,
        "snapshot_archive": {
            "rows": int(len(prepared_forecast)),
            "distinct_snapshots": int(prepared_forecast["snapshot_ts"].nunique()) if not prepared_forecast.empty else 0,
            "cities_covered": sorted(prepared_forecast["city_key"].dropna().astype(str).unique().tolist()),
            "earliest_snapshot_ts": None if overall_earliest is None else overall_earliest.isoformat(),
            "latest_snapshot_ts": None if overall_latest is None else overall_latest.isoformat(),
            "by_city": earliest_latest_by_city,
            "by_city_date": counts_by_city_date,
        },
        "matching_coverage": {
            "backtest_rows_eligible": eligible_rows,
            "backtest_rows_matched": matched_rows,
            "matched_share": matched_share,
            "by_city": [
                {
                    "city_key": city_key,
                    "eligible_rows": int(eligible_city_counts.get(city_key, 0)),
                    "matched_rows": int(matched_city_counts.get(city_key, 0)),
                    "matched_share": round(
                        float(matched_city_counts.get(city_key, 0) / eligible_city_counts.get(city_key, 1)),
                        6,
                    ) if eligible_city_counts.get(city_key, 0) else 0.0,
                }
                for city_key in sorted(eligible_city_counts)
            ],
        },
        "warnings": warnings,
    }


def render_forecast_snapshot_coverage_markdown(summary: dict[str, Any]) -> str:
    archive = summary["snapshot_archive"]
    matching = summary["matching_coverage"]
    lines = [
        "# Forecast Snapshot Coverage",
        "",
        f"- Generated at (UTC): `{summary['generated_at_utc']}`",
        f"- Max snapshot age (hours): `{summary['max_snapshot_age_hours']}`",
        f"- Snapshot rows: `{archive['rows']}`",
        f"- Distinct snapshots: `{archive['distinct_snapshots']}`",
        f"- Archive range: `{archive['earliest_snapshot_ts']}` to `{archive['latest_snapshot_ts']}`",
        f"- Backtest rows eligible: `{matching['backtest_rows_eligible']}`",
        f"- Backtest rows matched: `{matching['backtest_rows_matched']}`",
        f"- Matched share: `{matching['matched_share']}`",
        "",
        "## By City",
        "",
    ]
    for row in matching["by_city"]:
        lines.append(
            f"- `{row['city_key']}` eligible_rows={row['eligible_rows']} matched_rows={row['matched_rows']} matched_share={row['matched_share']}"
        )
    lines.append("")

    lines.extend(["## Snapshot Counts By City/Date", ""])
    if not archive["by_city_date"]:
        lines.append("- None")
    else:
        for row in archive["by_city_date"]:
            lines.append(
                f"- `{row['city_key']}` `{row['snapshot_date_local']}` snapshots={row['snapshots']} periods={row['periods']}"
            )
    lines.append("")

    lines.extend(["## Warnings", ""])
    if not summary["warnings"]:
        lines.append("- None")
    else:
        for warning in summary["warnings"]:
            lines.append(f"- {warning}")
    lines.append("")
    return "\n".join(lines)


def _prepare_run_directory(output_dir: Path | None, overwrite: bool, run_started_at: datetime) -> Path:
    if output_dir is not None:
        run_dir = output_dir
    else:
        run_dir = DEFAULT_RESEARCH_RUNS_DIR / run_started_at.strftime("%Y%m%dT%H%M%SZ")

    if run_dir.exists() and any(run_dir.iterdir()) and not overwrite:
        raise ForecastDistributionResearchRunError(
            f"Forecast-distribution run directory already exists and is not empty: {run_dir}"
        )
    run_dir.mkdir(parents=True, exist_ok=True)
    return run_dir


def _prepare_backtest_coverage_frame(df: pd.DataFrame) -> pd.DataFrame:
    prepared = df.copy()
    prepared["event_date"] = pd.to_datetime(prepared["event_date"], errors="coerce")
    prepared["decision_ts"] = pd.to_datetime(prepared["decision_ts"], utc=True, errors="coerce")
    prepared = prepared.loc[prepared["city_key"].notna() & prepared["event_date"].notna() & prepared["decision_ts"].notna()].copy()
    return prepared


def _prepare_forecast_coverage_frame(df: pd.DataFrame) -> pd.DataFrame:
    prepared = df.copy()
    if prepared.empty:
        return prepared
    prepared["snapshot_ts"] = pd.to_datetime(prepared["snapshot_ts"], utc=True, errors="coerce")
    prepared["period_start_ts"] = pd.to_datetime(prepared["period_start_ts"], utc=True, errors="coerce")
    prepared["period_end_ts"] = pd.to_datetime(prepared["period_end_ts"], utc=True, errors="coerce")
    prepared = prepared.loc[
        prepared["city_key"].notna()
        & prepared["snapshot_ts"].notna()
        & prepared["period_start_ts"].notna()
        & prepared["period_end_ts"].notna()
        & prepared["period_date_local"].notna()
    ].copy()
    prepared["snapshot_date_local"] = prepared["snapshot_ts"].dt.strftime("%Y-%m-%d")
    return prepared


def _row_has_matching_forecast_snapshot(
    forecast_df: pd.DataFrame,
    decision_ts: pd.Timestamp,
    event_date: pd.Timestamp,
    max_snapshot_age_hours: float,
) -> bool:
    if forecast_df.empty:
        return False
    candidate_rows = forecast_df.loc[
        (forecast_df["snapshot_ts"] <= decision_ts)
        & (forecast_df["snapshot_ts"] >= decision_ts - pd.Timedelta(hours=max_snapshot_age_hours))
        & (forecast_df["period_date_local"] == event_date.strftime("%Y-%m-%d"))
        & (forecast_df["period_end_ts"] > forecast_df["snapshot_ts"])
    ]
    return not candidate_rows.empty


def _coverage_warnings(
    eligible_rows: int,
    matched_rows: int,
    matched_share: float,
    earliest_latest_by_city: list[dict[str, Any]],
    eligible_city_counts: dict[str, int],
    matched_city_counts: dict[str, int],
) -> list[str]:
    warnings: list[str] = []
    if eligible_rows == 0:
        warnings.append("No backtest rows were available for forecast matching.")
    if matched_rows == 0 and eligible_rows > 0:
        warnings.append("No backtest rows matched a point-in-time forecast snapshot.")
    if eligible_rows > 0 and matched_share < 0.8:
        warnings.append("Matched-share is below 0.80; forward evaluation will accumulate slowly and may be biased by missing dates.")
    if not earliest_latest_by_city:
        warnings.append("Forecast snapshot archive is empty.")
    for city_key, eligible in sorted(eligible_city_counts.items()):
        matched = matched_city_counts.get(city_key, 0)
        if eligible > 0 and matched == 0:
            warnings.append(f"{city_key} has eligible backtest rows but no matched forecast snapshots.")
    return warnings
