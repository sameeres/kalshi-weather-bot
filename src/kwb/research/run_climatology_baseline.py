from __future__ import annotations

import importlib.util
import json
from datetime import datetime, timezone
from pathlib import Path
from statistics import median
from typing import Any

import pandas as pd

from kwb.backtest.compare_climatology_pricing import (
    ClimatologyPricingComparisonError,
    compare_climatology_pricing_modes,
)
from kwb.backtest.evaluate_climatology import (
    ClimatologyEvaluationError,
    evaluate_climatology_strategy,
)
from kwb.backtest.evaluate_climatology_executable import (
    ClimatologyExecutableEvaluationError,
    DEFAULT_EXECUTABLE_SUMMARY_FILENAME,
    DEFAULT_EXECUTABLE_TRADES_FILENAME,
    evaluate_climatology_executable_strategy,
)
from kwb.backtest.walkforward_climatology import (
    WalkforwardClimatologyError,
    run_walkforward_climatology,
)
from kwb.marts.backtest_dataset import BacktestDatasetBuildError, build_backtest_dataset
from kwb.models.baseline_climatology import (
    ClimatologyModelError,
    DEFAULT_HISTORY_PATH,
    DEFAULT_MODEL_NAME,
    score_climatology_baseline,
)
from kwb.settings import CONFIG_DIR, MARTS_DIR
from kwb.utils.logging import get_logger

logger = get_logger(__name__)

DEFAULT_RESEARCH_RUNS_DIR = MARTS_DIR / "research_runs"
DEFAULT_MANIFEST_FILENAME = "research_manifest_climatology.json"
DEFAULT_REPORT_JSON_FILENAME = "baseline_report_climatology.json"
DEFAULT_REPORT_MARKDOWN_FILENAME = "baseline_report_climatology.md"
DEFAULT_THRESHOLD_STABILITY_FILENAME = "threshold_stability_climatology.json"


class ClimatologyResearchRunError(ValueError):
    """Raised when the reproducible climatology baseline runner cannot complete safely."""


def run_climatology_baseline_research(
    decision_time_local: str = "10:00",
    output_dir: Path | None = None,
    pricing_mode: str = "both",
    overwrite: bool = False,
    config_path: Path | None = None,
    weather_path: Path | None = None,
    normals_path: Path | None = None,
    markets_path: Path | None = None,
    candles_path: Path | None = None,
    history_path: Path | None = None,
    day_window: int = 0,
    min_lookback_samples: int = 1,
    min_edge: float = 0.0,
    min_samples: int = 1,
    min_price: float = 0.0,
    max_price: float = 100.0,
    allow_no: bool = False,
    contracts: int = 1,
    fee_per_contract: float = 0.0,
    max_spread: float | None = None,
    train_window: int = 60,
    validation_window: int = 30,
    test_window: int = 30,
    step_window: int | None = None,
    min_trades_for_selection: int = 1,
    min_edge_grid: tuple[float, ...] = (0.0, 0.02, 0.05),
    min_samples_grid: tuple[int, ...] = (1, 5),
    min_price_grid: tuple[float, ...] = (0.0,),
    max_price_grid: tuple[float, ...] = (100.0,),
    max_spread_grid: tuple[float | None, ...] = (None, 5.0),
    allow_no_grid: tuple[bool, ...] = (False,),
    expanding: bool = True,
    selection_metric: str = "total_net_pnl",
) -> tuple[Path, Path, Path, Path, dict[str, Any]]:
    """Run the full local climatology baseline research bundle and write compact reports."""
    run_started_at = datetime.now(timezone.utc)
    run_dir = _prepare_run_directory(output_dir=output_dir, overwrite=overwrite, run_started_at=run_started_at)
    config_path = config_path or (CONFIG_DIR / "cities.yml")
    history_path = history_path or weather_path or DEFAULT_HISTORY_PATH

    manifest: dict[str, Any] = {
        "model_name": DEFAULT_MODEL_NAME,
        "run_timestamp_utc": run_started_at.isoformat(),
        "run_directory": str(run_dir),
        "decision_time_local": decision_time_local,
        "pricing_modes_requested": _requested_modes(pricing_mode),
        "input_paths": {
            "config_path": str(config_path),
            "weather_path": "" if weather_path is None else str(weather_path),
            "normals_path": "" if normals_path is None else str(normals_path),
            "markets_path": "" if markets_path is None else str(markets_path),
            "candles_path": "" if candles_path is None else str(candles_path),
            "history_path": str(history_path),
        },
        "output_paths": {},
        "row_counts": {},
        "steps": [],
        "skipped_steps": [],
        "real_local_data_available": _local_inputs_available(
            config_path=config_path,
            weather_path=weather_path,
            normals_path=normals_path,
            markets_path=markets_path,
            candles_path=candles_path,
            history_path=history_path,
        ),
        "parquet_engine_available": _parquet_engine_available(),
        "parquet_engine_limitations_affected_execution": False,
        "walkforward_config": {
            "pricing_mode": pricing_mode,
            "train_window": train_window,
            "validation_window": validation_window,
            "test_window": test_window,
            "step_window": step_window or test_window,
            "expanding": expanding,
            "selection_metric": selection_metric,
            "min_trades_for_selection": min_trades_for_selection,
        },
        "threshold_grid": {
            "min_edge_grid": list(min_edge_grid),
            "min_samples_grid": list(min_samples_grid),
            "min_price_grid": list(min_price_grid),
            "max_price_grid": list(max_price_grid),
            "max_spread_grid": list(max_spread_grid),
            "allow_no_grid": list(allow_no_grid),
        },
        "one_shot_params": {
            "min_edge": min_edge,
            "min_samples": min_samples,
            "min_price": min_price,
            "max_price": max_price,
            "allow_no": allow_no,
            "contracts": contracts,
            "fee_per_contract": fee_per_contract,
            "max_spread": max_spread,
        },
    }

    try:
        backtest_path, backtest_stats = build_backtest_dataset(
            decision_time_local=decision_time_local,
            config_path=config_path,
            weather_path=weather_path,
            normals_path=normals_path,
            markets_path=markets_path,
            candles_path=candles_path,
            output_dir=run_dir,
        )
        manifest["output_paths"]["backtest_dataset"] = str(backtest_path)
        manifest["row_counts"]["backtest_dataset_rows"] = int(backtest_stats.get("rows_written", 0))
        _record_completed_step(manifest, "build_backtest_dataset", rows_written=backtest_stats.get("rows_written", 0))

        scored_path, scored_summary = score_climatology_baseline(
            backtest_dataset_path=backtest_path,
            history_path=history_path,
            output_dir=run_dir,
            day_window=day_window,
            min_lookback_samples=min_lookback_samples,
        )
        manifest["output_paths"]["scored_dataset"] = str(scored_path)
        manifest["row_counts"]["scored_rows"] = int(scored_summary.get("rows_scored", 0))
        _record_completed_step(manifest, "score_climatology_baseline", rows_scored=scored_summary.get("rows_scored", 0))

        scored_df = pd.read_parquet(scored_path)
        data_coverage = _build_data_coverage(scored_df)

        simple_summary: dict[str, Any] | None = None
        executable_summary: dict[str, Any] | None = None
        comparison: dict[str, Any] | None = None
        walkforward_summary: dict[str, Any] | None = None
        walkforward_results_df = pd.DataFrame()
        walkforward_diagnostics_df = pd.DataFrame()

        if pricing_mode in {"decision_price", "both"}:
            simple_trades_path, simple_summary_path, simple_summary = evaluate_climatology_strategy(
                scored_dataset_path=scored_path,
                output_dir=run_dir,
                min_edge=min_edge,
                min_samples=min_samples,
                min_price=min_price,
                max_price=max_price,
                contracts=contracts,
                fee_per_contract=fee_per_contract,
                allow_no=allow_no,
            )
            manifest["output_paths"]["simple_trades"] = str(simple_trades_path)
            manifest["output_paths"]["simple_summary"] = str(simple_summary_path)
            _record_completed_step(manifest, "evaluate_climatology", trades_taken=simple_summary.get("trades_taken", 0))
        else:
            _record_skipped_step(manifest, "evaluate_climatology", "pricing_mode_excludes_decision_price")

        if pricing_mode in {"candle_proxy", "both"}:
            executable_trades_path, executable_summary_path, executable_summary = evaluate_climatology_executable_strategy(
                scored_dataset_path=scored_path,
                output_path=run_dir / DEFAULT_EXECUTABLE_TRADES_FILENAME,
                summary_output_path=run_dir / DEFAULT_EXECUTABLE_SUMMARY_FILENAME,
                min_edge=min_edge,
                min_samples=min_samples,
                min_price=min_price,
                max_price=max_price,
                contracts=contracts,
                fee_per_contract=fee_per_contract,
                allow_no=allow_no,
                max_spread=max_spread,
            )
            manifest["output_paths"]["executable_trades"] = str(executable_trades_path)
            manifest["output_paths"]["executable_summary"] = str(executable_summary_path)
            _record_completed_step(
                manifest,
                "evaluate_climatology_executable",
                trades_taken=executable_summary.get("trades_taken", 0),
            )
        else:
            _record_skipped_step(manifest, "evaluate_climatology_executable", "pricing_mode_excludes_candle_proxy")

        if pricing_mode == "both":
            comparison_json_path, comparison_csv_path, comparison = compare_climatology_pricing_modes(
                scored_dataset_path=scored_path,
                output_dir=run_dir,
                min_edge=min_edge,
                min_samples=min_samples,
                min_price=min_price,
                max_price=max_price,
                contracts=contracts,
                fee_per_contract=fee_per_contract,
                allow_no=allow_no,
                max_spread=max_spread,
            )
            manifest["output_paths"]["pricing_comparison_json"] = str(comparison_json_path)
            manifest["output_paths"]["pricing_comparison_csv"] = str(comparison_csv_path)
            _record_completed_step(manifest, "compare_climatology_pricing", modes_compared=len(comparison.get("modes", [])))
        else:
            _record_skipped_step(manifest, "compare_climatology_pricing", "requires_both_pricing_modes")

        try:
            walkforward_results_path, walkforward_summary_path, walkforward_diagnostics_path, walkforward_summary = (
                run_walkforward_climatology(
                    scored_dataset_path=scored_path,
                    output_dir=run_dir,
                    pricing_mode=pricing_mode,
                    train_window=train_window,
                    validation_window=validation_window,
                    test_window=test_window,
                    step_window=step_window,
                    min_trades_for_selection=min_trades_for_selection,
                    min_edge_grid=min_edge_grid,
                    min_samples_grid=min_samples_grid,
                    min_price_grid=min_price_grid,
                    max_price_grid=max_price_grid,
                    max_spread_grid=max_spread_grid,
                    allow_no_grid=allow_no_grid,
                    expanding=expanding,
                    selection_metric=selection_metric,
                )
            )
            manifest["output_paths"]["walkforward_results"] = str(walkforward_results_path)
            manifest["output_paths"]["walkforward_summary"] = str(walkforward_summary_path)
            manifest["output_paths"]["walkforward_diagnostics"] = str(walkforward_diagnostics_path)
            walkforward_results_df = pd.read_csv(walkforward_results_path)
            walkforward_diagnostics_df = pd.read_csv(walkforward_diagnostics_path)
            _record_completed_step(manifest, "walkforward_climatology", fold_count=walkforward_summary.get("fold_count", 0))
        except WalkforwardClimatologyError as exc:
            _record_skipped_step(manifest, "walkforward_climatology", str(exc))

    except (
        BacktestDatasetBuildError,
        ClimatologyModelError,
        ClimatologyEvaluationError,
        ClimatologyExecutableEvaluationError,
        ClimatologyPricingComparisonError,
    ) as exc:
        if _looks_like_parquet_engine_error(exc):
            manifest["parquet_engine_limitations_affected_execution"] = True
            raise ClimatologyResearchRunError(_parquet_engine_failure_message(exc)) from exc
        raise ClimatologyResearchRunError(str(exc)) from exc
    except (ImportError, ModuleNotFoundError, ValueError) as exc:
        if _looks_like_parquet_engine_error(exc):
            manifest["parquet_engine_limitations_affected_execution"] = True
            raise ClimatologyResearchRunError(_parquet_engine_failure_message(exc)) from exc
        raise

    threshold_stability = _build_threshold_stability_summary(walkforward_summary)
    threshold_stability_path = run_dir / DEFAULT_THRESHOLD_STABILITY_FILENAME
    _write_json(threshold_stability_path, threshold_stability)
    manifest["output_paths"]["threshold_stability"] = str(threshold_stability_path)

    report = _build_research_report(
        manifest=manifest,
        data_coverage=data_coverage,
        simple_summary=simple_summary,
        executable_summary=executable_summary,
        comparison=comparison,
        walkforward_summary=walkforward_summary,
        walkforward_results_df=walkforward_results_df,
        walkforward_diagnostics_df=walkforward_diagnostics_df,
        threshold_stability=threshold_stability,
    )

    report_json_path = run_dir / DEFAULT_REPORT_JSON_FILENAME
    report_markdown_path = run_dir / DEFAULT_REPORT_MARKDOWN_FILENAME
    manifest_path = run_dir / DEFAULT_MANIFEST_FILENAME
    _write_json(report_json_path, report)
    report_markdown_path.write_text(_render_markdown_report(report), encoding="utf-8")
    manifest["output_paths"]["report_json"] = str(report_json_path)
    manifest["output_paths"]["report_markdown"] = str(report_markdown_path)
    manifest["output_paths"]["manifest"] = str(manifest_path)
    _write_json(manifest_path, manifest)

    logger.info("Saved climatology baseline research bundle to %s", run_dir)
    return run_dir, manifest_path, report_json_path, report_markdown_path, manifest


def _prepare_run_directory(output_dir: Path | None, overwrite: bool, run_started_at: datetime) -> Path:
    if output_dir is None:
        output_dir = DEFAULT_RESEARCH_RUNS_DIR / f"climatology_baseline_{run_started_at.strftime('%Y%m%dT%H%M%SZ')}"
    if output_dir.exists():
        if any(output_dir.iterdir()) and not overwrite:
            raise ClimatologyResearchRunError(
                f"Output directory already exists and is not empty: {output_dir}. Pass overwrite=True to reuse it."
            )
    else:
        output_dir.mkdir(parents=True, exist_ok=True)
    return output_dir


def _requested_modes(pricing_mode: str) -> list[str]:
    return ["decision_price", "candle_proxy"] if pricing_mode == "both" else [pricing_mode]


def _local_inputs_available(
    config_path: Path,
    weather_path: Path | None,
    normals_path: Path | None,
    markets_path: Path | None,
    candles_path: Path | None,
    history_path: Path,
) -> bool:
    required = [config_path, history_path]
    for path in [weather_path, normals_path, markets_path, candles_path]:
        if path is not None:
            required.append(path)
    return all(path.exists() for path in required)


def _record_completed_step(manifest: dict[str, Any], step: str, **details: Any) -> None:
    manifest["steps"].append({"step": step, "status": "completed", **details})


def _record_skipped_step(manifest: dict[str, Any], step: str, reason: str) -> None:
    payload = {"step": step, "status": "skipped", "reason": reason}
    manifest["steps"].append(payload)
    manifest["skipped_steps"].append(payload)


def _build_data_coverage(scored_df: pd.DataFrame) -> dict[str, Any]:
    if scored_df.empty:
        return {
            "rows_scored": 0,
            "date_range": {"start": None, "end": None},
            "cities_covered": [],
            "quote_coverage": {},
            "spread_coverage": {},
        }

    event_dates = pd.to_datetime(scored_df["event_date"], errors="coerce")
    yes_spread = (scored_df["yes_ask"] - scored_df["yes_bid"]).where(
        scored_df["yes_ask"].notna() & scored_df["yes_bid"].notna()
    )
    no_spread = (scored_df["no_ask"] - scored_df["no_bid"]).where(
        scored_df["no_ask"].notna() & scored_df["no_bid"].notna()
    )
    return {
        "rows_scored": int(len(scored_df)),
        "date_range": {
            "start": None if event_dates.isna().all() else event_dates.min().date().isoformat(),
            "end": None if event_dates.isna().all() else event_dates.max().date().isoformat(),
        },
        "cities_covered": sorted(scored_df["city_key"].dropna().astype(str).unique().tolist()),
        "quote_coverage": {
            "yes_bid": _coverage(scored_df["yes_bid"]),
            "yes_ask": _coverage(scored_df["yes_ask"]),
            "no_bid": _coverage(scored_df["no_bid"]),
            "no_ask": _coverage(scored_df["no_ask"]),
        },
        "spread_coverage": {
            "yes_spread_available": _coverage(yes_spread),
            "no_spread_available": _coverage(no_spread),
            "average_yes_spread": _rounded_mean(yes_spread),
            "average_no_spread": _rounded_mean(no_spread),
        },
    }


def _build_threshold_stability_summary(walkforward_summary: dict[str, Any] | None) -> dict[str, Any]:
    if not walkforward_summary:
        return {"available": False, "results_by_pricing_mode": {}}

    by_mode: dict[str, Any] = {}
    for pricing_mode, mode_summary in walkforward_summary.get("results_by_pricing_mode", {}).items():
        selected = [
            row
            for row in mode_summary.get("selected_thresholds_per_fold", [])
            if row.get("selected_min_edge") is not None
        ]
        by_mode[str(pricing_mode)] = {
            "folds_scored": int(mode_summary.get("folds_scored", 0)),
            "selected_min_edge_frequency": _frequency_map(selected, "selected_min_edge"),
            "selected_min_samples_frequency": _frequency_map(selected, "selected_min_samples"),
            "selected_allow_no_frequency": _frequency_map(selected, "selected_allow_no"),
            "selected_max_spread_frequency": _frequency_map(selected, "selected_max_spread"),
            "average_selected_min_edge": _average_numeric(selected, "selected_min_edge"),
            "median_selected_min_edge": _median_numeric(selected, "selected_min_edge"),
            "average_selected_min_samples": _average_numeric(selected, "selected_min_samples"),
            "median_selected_min_samples": _median_numeric(selected, "selected_min_samples"),
            "allow_no_selected_rate": _selected_true_rate(selected, "selected_allow_no"),
            "max_spread_active_rate": _selected_non_null_rate(selected, "selected_max_spread"),
        }
    return {"available": True, "results_by_pricing_mode": by_mode}


def _build_research_report(
    manifest: dict[str, Any],
    data_coverage: dict[str, Any],
    simple_summary: dict[str, Any] | None,
    executable_summary: dict[str, Any] | None,
    comparison: dict[str, Any] | None,
    walkforward_summary: dict[str, Any] | None,
    walkforward_results_df: pd.DataFrame,
    walkforward_diagnostics_df: pd.DataFrame,
    threshold_stability: dict[str, Any],
) -> dict[str, Any]:
    walkforward_aggregate = {}
    if walkforward_summary is not None:
        walkforward_aggregate = walkforward_summary.get("results_by_pricing_mode", {})

    baseline_status, baseline_reason, baseline_criteria = _classify_baseline_status(
        requested_modes=manifest["pricing_modes_requested"],
        walkforward_aggregate=walkforward_aggregate,
        executable_summary=executable_summary,
    )
    return {
        "model_name": DEFAULT_MODEL_NAME,
        "run_timestamp_utc": manifest["run_timestamp_utc"],
        "pricing_modes_run": manifest["pricing_modes_requested"],
        "baseline_status": baseline_status,
        "baseline_status_reason": baseline_reason,
        "baseline_status_criteria": baseline_criteria,
        "data_coverage": data_coverage,
        "one_shot_evaluation": {
            "decision_price": simple_summary,
            "candle_proxy": executable_summary,
            "comparison": comparison,
        },
        "walkforward_evaluation": {
            "summary": walkforward_summary,
            "aggregate_by_pricing_mode": walkforward_aggregate,
            "threshold_stability": threshold_stability,
            "diagnostic_highlights": _build_diagnostics_highlights(walkforward_diagnostics_df),
            "folds_preview": walkforward_results_df.head(10).to_dict("records") if not walkforward_results_df.empty else [],
        },
        "skipped_steps": manifest["skipped_steps"],
    }


def _classify_baseline_status(
    requested_modes: list[str],
    walkforward_aggregate: dict[str, Any],
    executable_summary: dict[str, Any] | None,
) -> tuple[str, str, dict[str, Any]]:
    primary_mode = "candle_proxy" if "candle_proxy" in requested_modes else requested_modes[0]
    primary = walkforward_aggregate.get(primary_mode, {})
    trades_taken = int(primary.get("trades_taken", 0))
    folds_scored = int(primary.get("folds_scored", 0))
    total_net_pnl = float(primary.get("total_net_pnl", 0.0))
    average_net_pnl = float(primary.get("average_net_pnl_per_trade", 0.0))
    hit_rate = float(primary.get("hit_rate", 0.0))
    yes_quote_coverage = None if executable_summary is None else executable_summary.get("yes_quote_coverage")

    criteria = {
        "primary_mode": primary_mode,
        "promising_requires": {
            "folds_scored_at_least": 2,
            "trades_taken_at_least": 10,
            "total_net_pnl_positive": True,
            "average_net_pnl_per_trade_positive": True,
            "hit_rate_at_least": 0.5,
            "yes_quote_coverage_at_least_for_candle_proxy": 0.8,
        },
        "weak_if_any": ["no_scored_folds", "no_trades", "non_positive_total_net_pnl"],
    }

    if folds_scored == 0:
        return "weak", f"{primary_mode} walk-forward produced no scored folds.", criteria
    if trades_taken == 0:
        return "weak", f"{primary_mode} walk-forward produced no out-of-sample trades.", criteria
    if total_net_pnl <= 0:
        return "weak", f"{primary_mode} walk-forward total net PnL was not positive.", criteria

    promising = (
        folds_scored >= 2
        and trades_taken >= 10
        and total_net_pnl > 0
        and average_net_pnl > 0
        and hit_rate >= 0.5
    )
    if primary_mode == "candle_proxy":
        promising = promising and yes_quote_coverage is not None and float(yes_quote_coverage) >= 0.8

    if promising:
        return "promising", f"{primary_mode} stayed positive out of sample with enough folds and trade count.", criteria
    return "inconclusive", f"{primary_mode} stayed positive, but evidence is still thin for a strong claim.", criteria


def _build_diagnostics_highlights(diagnostics_df: pd.DataFrame) -> dict[str, Any]:
    if diagnostics_df.empty:
        return {}

    highlights: dict[str, Any] = {}
    for pricing_mode, mode_df in diagnostics_df.groupby("pricing_mode", sort=False):
        mode_highlights: dict[str, Any] = {}
        for breakdown in ["city_key", "season_bucket", "entry_price_bucket", "sample_size_bucket", "spread_bucket"]:
            subset = mode_df.loc[mode_df["breakdown"] == breakdown].copy()
            if subset.empty:
                continue
            top = subset.sort_values(["total_net_pnl", "trades_taken"], ascending=[False, False], kind="stable").iloc[0]
            bottom = subset.sort_values(["total_net_pnl", "trades_taken"], ascending=[True, False], kind="stable").iloc[0]
            mode_highlights[breakdown] = {
                "top_bucket": _series_to_plain_dict(top),
                "bottom_bucket": _series_to_plain_dict(bottom),
            }
        highlights[str(pricing_mode)] = mode_highlights
    return highlights


def _render_markdown_report(report: dict[str, Any]) -> str:
    lines = [
        "# Climatology Baseline Report",
        "",
        f"- Model: `{report['model_name']}`",
        f"- Run timestamp (UTC): `{report['run_timestamp_utc']}`",
        f"- Baseline status: `{report['baseline_status']}`",
        f"- Status note: {report['baseline_status_reason']}",
        "",
        "## Data Coverage",
        "",
    ]
    coverage = report["data_coverage"]
    lines.extend(
        [
            f"- Rows scored: `{coverage.get('rows_scored', 0)}`",
            f"- Date range: `{coverage.get('date_range', {}).get('start')}` to `{coverage.get('date_range', {}).get('end')}`",
            f"- Cities covered: `{', '.join(coverage.get('cities_covered', []))}`",
            "",
            "## One-Shot Evaluation",
            "",
        ]
    )
    for mode in ["decision_price", "candle_proxy"]:
        summary = report["one_shot_evaluation"].get(mode)
        if not summary:
            continue
        lines.extend(
            [
                f"### {mode}",
                "",
                f"- Trades taken: `{summary.get('trades_taken', 0)}`",
                f"- Hit rate: `{summary.get('hit_rate', 0.0)}`",
                f"- Average edge at entry: `{summary.get('average_edge_at_entry', 0.0)}`",
                f"- Total net PnL: `{summary.get('total_net_pnl', 0.0)}`",
                "",
            ]
        )
    lines.extend(["## Walk-Forward Evaluation", ""])
    for mode, summary in report["walkforward_evaluation"].get("aggregate_by_pricing_mode", {}).items():
        lines.extend(
            [
                f"### {mode}",
                "",
                f"- Folds scored: `{summary.get('folds_scored', 0)}`",
                f"- Trades taken: `{summary.get('trades_taken', 0)}`",
                f"- Hit rate: `{summary.get('hit_rate', 0.0)}`",
                f"- Average net PnL per trade: `{summary.get('average_net_pnl_per_trade', 0.0)}`",
                f"- Total net PnL: `{summary.get('total_net_pnl', 0.0)}`",
                "",
            ]
        )
    return "\n".join(lines).strip() + "\n"


def _coverage(series: pd.Series) -> float:
    if len(series) == 0:
        return 0.0
    return round(float(series.notna().mean()), 6)


def _rounded_mean(series: pd.Series) -> float | None:
    valid = series.dropna()
    if valid.empty:
        return None
    return round(float(valid.mean()), 6)


def _frequency_map(rows: list[dict[str, Any]], key: str) -> dict[str, int]:
    counts: dict[str, int] = {}
    for row in rows:
        value = row.get(key)
        label = "null" if value is None else str(value)
        counts[label] = counts.get(label, 0) + 1
    return counts


def _average_numeric(rows: list[dict[str, Any]], key: str) -> float | None:
    values = [float(row[key]) for row in rows if row.get(key) is not None]
    if not values:
        return None
    return round(sum(values) / len(values), 6)


def _median_numeric(rows: list[dict[str, Any]], key: str) -> float | None:
    values = [float(row[key]) for row in rows if row.get(key) is not None]
    if not values:
        return None
    return round(float(median(values)), 6)


def _selected_true_rate(rows: list[dict[str, Any]], key: str) -> float | None:
    values = [bool(row[key]) for row in rows if row.get(key) is not None]
    if not values:
        return None
    return round(sum(values) / len(values), 6)


def _selected_non_null_rate(rows: list[dict[str, Any]], key: str) -> float | None:
    if not rows:
        return None
    active = [row for row in rows if row.get(key) is not None]
    return round(len(active) / len(rows), 6)


def _series_to_plain_dict(series: pd.Series) -> dict[str, Any]:
    return {str(key): _plain_json_value(value) for key, value in series.to_dict().items()}


def _plain_json_value(value: Any) -> Any:
    if pd.isna(value):
        return None
    if isinstance(value, (pd.Timestamp, datetime)):
        return value.isoformat()
    if hasattr(value, "item"):
        try:
            return value.item()
        except ValueError:
            return str(value)
    return value


def _write_json(path: Path, payload: dict[str, Any]) -> None:
    path.write_text(json.dumps(payload, indent=2, sort_keys=True) + "\n", encoding="utf-8")


def _parquet_engine_available() -> bool:
    return bool(importlib.util.find_spec("pyarrow") or importlib.util.find_spec("fastparquet"))


def _looks_like_parquet_engine_error(exc: BaseException) -> bool:
    message = str(exc).lower()
    return "pyarrow" in message or "fastparquet" in message or ("parquet" in message and "engine" in message)


def _parquet_engine_failure_message(exc: BaseException) -> str:
    return (
        "Parquet support is required for the baseline research runner, but no usable parquet engine was available. "
        f"Original error: {exc}"
    )
