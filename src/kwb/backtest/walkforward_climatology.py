from __future__ import annotations

import json
from dataclasses import dataclass
from itertools import product
from pathlib import Path
from typing import Any

import pandas as pd

from kwb.backtest import evaluate_climatology as decision_eval
from kwb.backtest import evaluate_climatology_executable as executable_eval
from kwb.models.baseline_climatology import DEFAULT_SCORED_FILENAME, evaluate_scored_climatology
from kwb.settings import MARTS_DIR
from kwb.utils.logging import get_logger

logger = get_logger(__name__)

DEFAULT_WALKFORWARD_RESULTS_FILENAME = "walkforward_results_climatology.csv"
DEFAULT_WALKFORWARD_SUMMARY_FILENAME = "walkforward_summary_climatology.json"
DEFAULT_WALKFORWARD_DIAGNOSTICS_FILENAME = "walkforward_diagnostics_climatology.csv"

DEFAULT_MIN_EDGE_GRID = (0.0, 0.02, 0.05)
DEFAULT_MIN_SAMPLES_GRID = (1, 5)
DEFAULT_MIN_PRICE_GRID = (0.0,)
DEFAULT_MAX_PRICE_GRID = (100.0,)
DEFAULT_MAX_SPREAD_GRID = (None, 5.0)
DEFAULT_ALLOW_NO_GRID = (False,)
DEFAULT_WALKFORWARD_WINDOW_PROFILE = "custom"

WINDOW_PROFILE_CONFIGS: dict[str, dict[str, int]] = {
    "standard": {
        "train_window": 60,
        "validation_window": 30,
        "test_window": 30,
        "step_window": 30,
    },
    "research_short": {
        "train_window": 30,
        "validation_window": 15,
        "test_window": 15,
        "step_window": 15,
    },
}

VALID_PRICING_MODES = {"decision_price", "candle_proxy", "both"}
VALID_SELECTION_METRICS = {"total_net_pnl", "average_net_pnl_per_trade"}
VALID_WINDOW_PROFILES = {"custom", "standard", "research_short", "auto"}


class WalkforwardClimatologyError(ValueError):
    """Raised when walk-forward climatology evaluation cannot be completed safely."""


@dataclass(frozen=True)
class ThresholdParams:
    min_edge: float
    min_samples: int
    min_price: float
    max_price: float
    allow_no: bool
    max_spread: float | None


def run_walkforward_climatology(
    scored_dataset_path: Path | None = None,
    output_dir: Path | None = None,
    pricing_mode: str = "both",
    window_profile: str = DEFAULT_WALKFORWARD_WINDOW_PROFILE,
    train_window: int = 60,
    validation_window: int = 30,
    test_window: int = 30,
    step_window: int | None = None,
    min_trades_for_selection: int = 1,
    min_edge_grid: tuple[float, ...] = DEFAULT_MIN_EDGE_GRID,
    min_samples_grid: tuple[int, ...] = DEFAULT_MIN_SAMPLES_GRID,
    min_price_grid: tuple[float, ...] = DEFAULT_MIN_PRICE_GRID,
    max_price_grid: tuple[float, ...] = DEFAULT_MAX_PRICE_GRID,
    executable_fee_model: str = "flat_per_contract",
    executable_fee_per_contract: float = 0.0,
    max_spread_grid: tuple[float | None, ...] = DEFAULT_MAX_SPREAD_GRID,
    allow_no_grid: tuple[bool, ...] = DEFAULT_ALLOW_NO_GRID,
    expanding: bool = True,
    selection_metric: str = "total_net_pnl",
) -> tuple[Path, Path, Path, dict[str, Any]]:
    """Run a temporal walk-forward evaluation for the baseline climatology strategy."""
    scored_dataset_path = scored_dataset_path or (MARTS_DIR / DEFAULT_SCORED_FILENAME)
    output_dir = output_dir or MARTS_DIR
    output_dir.mkdir(parents=True, exist_ok=True)

    _validate_walkforward_parameters(
        pricing_mode=pricing_mode,
        window_profile=window_profile,
        train_window=train_window,
        validation_window=validation_window,
        test_window=test_window,
        step_window=step_window,
        min_trades_for_selection=min_trades_for_selection,
        selection_metric=selection_metric,
    )
    _ensure_inputs_exist([scored_dataset_path])

    scored_df = pd.read_parquet(scored_dataset_path).copy()
    _prepare_scored_frame(scored_df)

    (
        resolved_profile,
        train_window,
        validation_window,
        test_window,
        resolved_step_window,
    ) = _resolve_window_configuration(
        scored_df=scored_df,
        window_profile=window_profile,
        train_window=train_window,
        validation_window=validation_window,
        test_window=test_window,
        step_window=step_window or test_window,
        expanding=expanding,
    )

    modes = ["decision_price", "candle_proxy"] if pricing_mode == "both" else [pricing_mode]
    folds = _build_temporal_folds(
        scored_df=scored_df,
        train_window=train_window,
        validation_window=validation_window,
        test_window=test_window,
        step_window=resolved_step_window,
        expanding=expanding,
    )
    if not folds:
        raise WalkforwardClimatologyError(
            "No valid walk-forward folds could be created from the scored dataset and requested window sizes."
        )

    grid = _build_threshold_grid(
        min_edge_grid=min_edge_grid,
        min_samples_grid=min_samples_grid,
        min_price_grid=min_price_grid,
        max_price_grid=max_price_grid,
        max_spread_grid=max_spread_grid,
        allow_no_grid=allow_no_grid,
    )

    fold_rows: list[dict[str, Any]] = []
    all_test_rows: list[pd.DataFrame] = []
    all_trade_rows: list[pd.DataFrame] = []

    for mode in modes:
        for fold_number, fold in enumerate(folds, start=1):
            result = _evaluate_fold(
                fold=fold,
                fold_number=fold_number,
                pricing_mode=mode,
                grid=grid,
                executable_fee_model=executable_fee_model,
                executable_fee_per_contract=executable_fee_per_contract,
                min_trades_for_selection=min_trades_for_selection,
                selection_metric=selection_metric,
            )
            fold_rows.append(result["fold_row"])
            if result["test_rows"] is not None and not result["test_rows"].empty:
                all_test_rows.append(result["test_rows"])
            if result["trades_df"] is not None and not result["trades_df"].empty:
                all_trade_rows.append(result["trades_df"])

    results_df = pd.DataFrame(fold_rows)
    diagnostics_df = _build_diagnostics_frame(
        trades_df=pd.concat(all_trade_rows, ignore_index=True) if all_trade_rows else pd.DataFrame(),
    )
    summary = _build_walkforward_summary(
        results_df=results_df,
        all_test_rows=pd.concat(all_test_rows, ignore_index=True) if all_test_rows else pd.DataFrame(),
        all_trade_rows=pd.concat(all_trade_rows, ignore_index=True) if all_trade_rows else pd.DataFrame(),
        pricing_mode=pricing_mode,
        fold_count=len(folds),
        train_window=train_window,
        validation_window=validation_window,
        test_window=test_window,
        step_window=resolved_step_window,
        expanding=expanding,
        min_trades_for_selection=min_trades_for_selection,
        selection_metric=selection_metric,
        window_profile=resolved_profile,
    )

    results_path = output_dir / DEFAULT_WALKFORWARD_RESULTS_FILENAME
    summary_path = output_dir / DEFAULT_WALKFORWARD_SUMMARY_FILENAME
    diagnostics_path = output_dir / DEFAULT_WALKFORWARD_DIAGNOSTICS_FILENAME
    results_df.to_csv(results_path, index=False)
    diagnostics_df.to_csv(diagnostics_path, index=False)
    summary_path.write_text(json.dumps(summary, indent=2, sort_keys=True) + "\n", encoding="utf-8")

    logger.info(
        "Saved walk-forward climatology results to %s, %s, and %s",
        results_path,
        summary_path,
        diagnostics_path,
    )
    return results_path, summary_path, diagnostics_path, summary


def _prepare_scored_frame(scored_df: pd.DataFrame) -> None:
    required = decision_eval.REQUIRED_SCORED_COLUMNS | {
        "actual_tmax_f",
        "normal_tmax_f",
        "tmax_anomaly_f",
        "fair_yes",
        "fair_no",
    }
    missing = sorted(required - set(scored_df.columns))
    if missing:
        raise WalkforwardClimatologyError(
            f"Scored dataset is missing required walk-forward columns: {', '.join(missing)}"
        )

    scored_df["event_date"] = pd.to_datetime(scored_df["event_date"], errors="coerce")
    if scored_df["event_date"].isna().any():
        raise WalkforwardClimatologyError("Scored dataset contains invalid event_date values.")
    scored_df["decision_ts"] = pd.to_datetime(scored_df["decision_ts"], utc=True, errors="coerce")
    if scored_df["decision_ts"].isna().any():
        raise WalkforwardClimatologyError("Scored dataset contains invalid decision_ts values.")


def _build_temporal_folds(
    scored_df: pd.DataFrame,
    train_window: int,
    validation_window: int,
    test_window: int,
    step_window: int,
    expanding: bool,
) -> list[dict[str, pd.DataFrame | pd.Timestamp]]:
    unique_dates = sorted(scored_df["event_date"].dt.normalize().drop_duplicates().tolist())
    folds: list[dict[str, pd.DataFrame | pd.Timestamp]] = []
    cursor = 0

    while True:
        train_start = 0 if expanding else cursor
        train_end = (train_window + cursor) if expanding else (train_start + train_window)
        validation_end = train_end + validation_window
        test_end = validation_end + test_window
        if test_end > len(unique_dates):
            break

        train_dates = set(unique_dates[train_start:train_end])
        validation_dates = set(unique_dates[train_end:validation_end])
        test_dates = set(unique_dates[validation_end:test_end])

        folds.append(
            {
                "train_df": scored_df.loc[scored_df["event_date"].dt.normalize().isin(train_dates)].copy(),
                "validation_df": scored_df.loc[scored_df["event_date"].dt.normalize().isin(validation_dates)].copy(),
                "test_df": scored_df.loc[scored_df["event_date"].dt.normalize().isin(test_dates)].copy(),
                "train_start": unique_dates[train_start],
                "train_end": unique_dates[train_end - 1],
                "validation_start": unique_dates[train_end],
                "validation_end": unique_dates[validation_end - 1],
                "test_start": unique_dates[validation_end],
                "test_end": unique_dates[test_end - 1],
            }
        )
        cursor += step_window

    return folds


def _build_threshold_grid(
    min_edge_grid: tuple[float, ...],
    min_samples_grid: tuple[int, ...],
    min_price_grid: tuple[float, ...],
    max_price_grid: tuple[float, ...],
    max_spread_grid: tuple[float | None, ...],
    allow_no_grid: tuple[bool, ...],
) -> list[ThresholdParams]:
    params: list[ThresholdParams] = []
    for min_edge, min_samples, min_price, max_price, allow_no, max_spread in product(
        min_edge_grid,
        min_samples_grid,
        min_price_grid,
        max_price_grid,
        allow_no_grid,
        max_spread_grid,
    ):
        if min_price > max_price:
            continue
        params.append(
            ThresholdParams(
                min_edge=float(min_edge),
                min_samples=int(min_samples),
                min_price=float(min_price),
                max_price=float(max_price),
                allow_no=bool(allow_no),
                max_spread=None if max_spread is None else float(max_spread),
            )
        )
    if not params:
        raise WalkforwardClimatologyError("Threshold grid is empty after validation.")
    return params


def _evaluate_fold(
    fold: dict[str, pd.DataFrame | pd.Timestamp],
    fold_number: int,
    pricing_mode: str,
    grid: list[ThresholdParams],
    executable_fee_model: str,
    executable_fee_per_contract: float,
    min_trades_for_selection: int,
    selection_metric: str,
) -> dict[str, Any]:
    validation_df = fold["validation_df"]
    test_df = fold["test_df"]
    assert isinstance(validation_df, pd.DataFrame)
    assert isinstance(test_df, pd.DataFrame)

    candidates: list[dict[str, Any]] = []
    for params in grid:
        validation_trades, validation_summary = _evaluate_mode_frame(
            validation_df,
            pricing_mode,
            params,
            executable_fee_model=executable_fee_model,
            executable_fee_per_contract=executable_fee_per_contract,
        )
        if int(validation_summary["trades_taken"]) < min_trades_for_selection:
            continue
        candidates.append(
            {
                "params": params,
                "validation_trades": validation_trades,
                "validation_summary": validation_summary,
                "selection_tuple": _selection_tuple(validation_summary, selection_metric),
            }
        )

    if not candidates:
        return {
            "fold_row": _build_fold_row(
                fold_number=fold_number,
                pricing_mode=pricing_mode,
                fold=fold,
                chosen_params=None,
                validation_summary=None,
                test_summary=None,
                skip_reason="no_validation_candidate_met_min_trades",
            ),
            "test_rows": None,
            "trades_df": None,
        }

    chosen = max(candidates, key=lambda item: item["selection_tuple"])
    test_trades, test_summary = _evaluate_mode_frame(
        test_df,
        pricing_mode,
        chosen["params"],
        executable_fee_model=executable_fee_model,
        executable_fee_per_contract=executable_fee_per_contract,
    )

    annotated_test_rows = test_df.copy()
    annotated_test_rows["pricing_mode"] = pricing_mode
    annotated_test_rows["fold_number"] = fold_number

    return {
        "fold_row": _build_fold_row(
            fold_number=fold_number,
            pricing_mode=pricing_mode,
            fold=fold,
            chosen_params=chosen["params"],
            validation_summary=chosen["validation_summary"],
            test_summary=test_summary,
            skip_reason=None,
        ),
        "test_rows": annotated_test_rows,
        "trades_df": test_trades.assign(fold_number=fold_number, pricing_mode=pricing_mode),
    }


def _evaluate_mode_frame(
    df: pd.DataFrame,
    pricing_mode: str,
    params: ThresholdParams,
    executable_fee_model: str = "flat_per_contract",
    executable_fee_per_contract: float = 0.0,
) -> tuple[pd.DataFrame, dict[str, Any]]:
    frame = df.copy().reset_index(drop=True)
    if pricing_mode == "decision_price":
        trades_df, summary = decision_eval._evaluate_frame(  # noqa: SLF001
            scored_df=frame,
            min_edge=params.min_edge,
            min_samples=params.min_samples,
            min_price=params.min_price,
            max_price=params.max_price,
            contracts=1,
            fee_per_contract=0.0,
            allow_no=params.allow_no,
        )
        summary = _normalize_mode_summary(summary, pricing_mode)
        return trades_df, summary

    if pricing_mode == "candle_proxy":
        missing = sorted(executable_eval.REQUIRED_EXECUTABLE_COLUMNS - set(frame.columns))
        if missing:
            raise WalkforwardClimatologyError(
                f"Scored dataset is missing executable pricing columns required for candle_proxy: {', '.join(missing)}"
            )
        trades_df, summary = executable_eval._evaluate_frame(  # noqa: SLF001
            scored_df=frame,
            min_edge=params.min_edge,
            min_samples=params.min_samples,
            min_price=params.min_price,
            max_price=params.max_price,
            contracts=1,
            fee_model=executable_fee_model,
            fee_per_contract=executable_fee_per_contract,
            allow_no=params.allow_no,
            max_spread=params.max_spread,
        )
        summary = _normalize_mode_summary(summary, pricing_mode)
        return trades_df, summary

    raise WalkforwardClimatologyError(f"Unsupported pricing_mode {pricing_mode!r}")


def _normalize_mode_summary(summary: dict[str, Any], pricing_mode: str) -> dict[str, Any]:
    normalized = dict(summary)
    normalized["pricing_mode"] = pricing_mode
    normalized["average_net_pnl_per_trade"] = float(
        summary.get("average_net_pnl_per_trade", summary.get("average_pnl_per_trade", 0.0))
    )
    normalized["average_gross_pnl_per_trade"] = float(
        summary.get("average_gross_pnl_per_trade", summary.get("average_pnl_per_trade", 0.0))
    )
    return normalized


def _selection_tuple(summary: dict[str, Any], selection_metric: str) -> tuple[float, float, float]:
    primary = float(summary.get(selection_metric, 0.0))
    secondary = float(summary.get("average_net_pnl_per_trade", 0.0))
    turnover_penalty = -float(summary.get("trades_taken", 0))
    return primary, secondary, turnover_penalty


def _build_fold_row(
    fold_number: int,
    pricing_mode: str,
    fold: dict[str, pd.DataFrame | pd.Timestamp],
    chosen_params: ThresholdParams | None,
    validation_summary: dict[str, Any] | None,
    test_summary: dict[str, Any] | None,
    skip_reason: str | None,
) -> dict[str, Any]:
    return {
        "fold_number": fold_number,
        "pricing_mode": pricing_mode,
        "train_start": _date_str(fold["train_start"]),
        "train_end": _date_str(fold["train_end"]),
        "validation_start": _date_str(fold["validation_start"]),
        "validation_end": _date_str(fold["validation_end"]),
        "test_start": _date_str(fold["test_start"]),
        "test_end": _date_str(fold["test_end"]),
        "selected_min_edge": None if chosen_params is None else chosen_params.min_edge,
        "selected_min_samples": None if chosen_params is None else chosen_params.min_samples,
        "selected_min_price": None if chosen_params is None else chosen_params.min_price,
        "selected_max_price": None if chosen_params is None else chosen_params.max_price,
        "selected_allow_no": None if chosen_params is None else chosen_params.allow_no,
        "selected_max_spread": None if chosen_params is None else chosen_params.max_spread,
        "validation_trades": 0 if validation_summary is None else int(validation_summary["trades_taken"]),
        "validation_total_net_pnl": 0.0 if validation_summary is None else float(validation_summary["total_net_pnl"]),
        "validation_average_net_pnl_per_trade": (
            0.0 if validation_summary is None else float(validation_summary["average_net_pnl_per_trade"])
        ),
        "test_rows_evaluated": len(fold["test_df"]),
        "test_trades": 0 if test_summary is None else int(test_summary["trades_taken"]),
        "test_yes_trades": 0 if test_summary is None else int(test_summary.get("yes_trades_taken", 0)),
        "test_no_trades": 0 if test_summary is None else int(test_summary.get("no_trades_taken", 0)),
        "test_hit_rate": 0.0 if test_summary is None else float(test_summary["hit_rate"]),
        "test_average_edge_at_entry": 0.0 if test_summary is None else float(test_summary["average_edge_at_entry"]),
        "test_total_gross_pnl": 0.0 if test_summary is None else float(test_summary["total_gross_pnl"]),
        "test_total_net_pnl": 0.0 if test_summary is None else float(test_summary["total_net_pnl"]),
        "test_brier_score": 0.0 if test_summary is None else float(test_summary["brier_score"]),
        "skip_reason": skip_reason or "",
    }


def _build_walkforward_summary(
    results_df: pd.DataFrame,
    all_test_rows: pd.DataFrame,
    all_trade_rows: pd.DataFrame,
    pricing_mode: str,
    fold_count: int,
    train_window: int,
    validation_window: int,
    test_window: int,
    step_window: int,
    expanding: bool,
    min_trades_for_selection: int,
    selection_metric: str,
    window_profile: str,
) -> dict[str, Any]:
    mode_summaries: dict[str, Any] = {}
    for mode, mode_results in results_df.groupby("pricing_mode", sort=False):
        mode_test_rows = all_test_rows.loc[all_test_rows["pricing_mode"] == mode].copy() if not all_test_rows.empty else pd.DataFrame()
        mode_trade_rows = all_trade_rows.loc[all_trade_rows["pricing_mode"] == mode].copy() if not all_trade_rows.empty else pd.DataFrame()
        scored_summary = evaluate_scored_climatology(mode_test_rows) if not mode_test_rows.empty else evaluate_scored_climatology(pd.DataFrame())
        mode_summaries[str(mode)] = {
            "pricing_mode": str(mode),
            "folds_attempted": int(len(mode_results)),
            "folds_scored": int((mode_results["skip_reason"] == "").sum()),
            "rows_evaluated": int(len(mode_test_rows)),
            "trades_taken": int(len(mode_trade_rows)),
            "yes_trades_taken": _count_values(mode_trade_rows, "chosen_side", "yes"),
            "no_trades_taken": _count_values(mode_trade_rows, "chosen_side", "no"),
            "hit_rate": _trade_metric(mode_trade_rows, "hit_rate"),
            "average_edge_at_entry": _trade_metric(mode_trade_rows, "edge_at_entry"),
            "average_gross_pnl_per_trade": _trade_metric(mode_trade_rows, "gross_pnl"),
            "average_net_pnl_per_trade": _trade_metric(mode_trade_rows, "net_pnl"),
            "total_gross_pnl": round(float(mode_trade_rows["gross_pnl"].sum()), 6) if not mode_trade_rows.empty else 0.0,
            "total_net_pnl": round(float(mode_trade_rows["net_pnl"].sum()), 6) if not mode_trade_rows.empty else 0.0,
            "brier_score": float(scored_summary["brier_score"]),
            "selected_thresholds_per_fold": mode_results[
                [
                    "fold_number",
                    "selected_min_edge",
                    "selected_min_samples",
                    "selected_min_price",
                    "selected_max_price",
                    "selected_allow_no",
                    "selected_max_spread",
                ]
            ].to_dict("records"),
        }

    return {
        "pricing_mode": pricing_mode,
        "window_config": {
            "window_profile": window_profile,
            "train_window": train_window,
            "validation_window": validation_window,
            "test_window": test_window,
            "step_window": step_window,
            "expanding": expanding,
        },
        "selection_config": {
            "min_trades_for_selection": min_trades_for_selection,
            "selection_metric": selection_metric,
        },
        "fold_count": fold_count,
        "rows_available": int(len(all_test_rows)),
        "results_by_pricing_mode": mode_summaries,
        "fold_results_preview": results_df.to_dict("records"),
    }


def _build_diagnostics_frame(trades_df: pd.DataFrame) -> pd.DataFrame:
    columns = [
        "pricing_mode",
        "subset",
        "breakdown",
        "bucket",
        "trades_taken",
        "yes_trades_taken",
        "no_trades_taken",
        "hit_rate",
        "average_edge_at_entry",
        "average_net_pnl_per_trade",
        "total_net_pnl",
    ]
    if trades_df.empty:
        return pd.DataFrame(columns=columns)

    df = trades_df.copy()
    df["event_date"] = pd.to_datetime(df["event_date"], errors="coerce")
    df["month_bucket"] = df["event_date"].dt.strftime("%Y-%m")
    df["season_bucket"] = df["event_date"].dt.month.map(_month_to_season)
    df["entry_price_bucket"] = pd.cut(
        df["entry_price"].astype(float),
        bins=[0, 25, 50, 75, 100],
        labels=["0-25", "25-50", "50-75", "75-100"],
        include_lowest=True,
        right=True,
    ).astype(str)
    df["sample_size_bucket"] = pd.cut(
        df["lookback_sample_size"].astype(float),
        bins=[0, 5, 10, 25, 10_000],
        labels=["1-5", "6-10", "11-25", "26+"],
        include_lowest=True,
        right=True,
    ).astype(str)
    if "quote_spread" in df.columns:
        df["spread_bucket"] = pd.cut(
            df["quote_spread"].astype(float),
            bins=[0, 2, 5, 10, 100],
            labels=["0-2", "2-5", "5-10", "10+"],
            include_lowest=True,
            right=True,
        ).astype(str)
    else:
        df["spread_bucket"] = "not_available"

    diagnostics: list[dict[str, Any]] = []
    for pricing_mode, mode_df in df.groupby("pricing_mode", sort=False):
        for breakdown in ["city_key", "month_bucket", "season_bucket", "entry_price_bucket", "sample_size_bucket"]:
            diagnostics.extend(_group_trade_diagnostics(mode_df, str(pricing_mode), breakdown))
        if str(pricing_mode) == "candle_proxy":
            diagnostics.extend(_group_trade_diagnostics(mode_df, str(pricing_mode), "spread_bucket"))

    return pd.DataFrame(diagnostics, columns=columns).sort_values(
        ["pricing_mode", "breakdown", "bucket"], kind="stable"
    ).reset_index(drop=True)


def _group_trade_diagnostics(df: pd.DataFrame, pricing_mode: str, breakdown: str) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    for bucket, bucket_df in df.groupby(breakdown, dropna=False, sort=False):
        hits = bucket_df.apply(
            lambda row: bool(row["resolved_yes"]) if row["chosen_side"] == "yes" else not bool(row["resolved_yes"]),
            axis=1,
        )
        rows.append(
            {
                "pricing_mode": pricing_mode,
                "subset": "selected_trades",
                "breakdown": breakdown,
                "bucket": str(bucket),
                "trades_taken": int(len(bucket_df)),
                "yes_trades_taken": _count_values(bucket_df, "chosen_side", "yes"),
                "no_trades_taken": _count_values(bucket_df, "chosen_side", "no"),
                "hit_rate": round(float(hits.mean()), 6) if len(bucket_df) else 0.0,
                "average_edge_at_entry": round(float(bucket_df["edge_at_entry"].mean()), 6),
                "average_net_pnl_per_trade": round(float(bucket_df["net_pnl"].mean()), 6),
                "total_net_pnl": round(float(bucket_df["net_pnl"].sum()), 6),
            }
        )
    return rows


def _count_values(df: pd.DataFrame, column: str, value: str) -> int:
    if df.empty or column not in df.columns:
        return 0
    return int((df[column].astype(str) == value).sum())


def _trade_metric(df: pd.DataFrame, column: str) -> float:
    if df.empty:
        return 0.0
    if column == "hit_rate":
        hits = df.apply(
            lambda row: bool(row["resolved_yes"]) if row["chosen_side"] == "yes" else not bool(row["resolved_yes"]),
            axis=1,
        )
        return round(float(hits.mean()), 6)
    return round(float(df[column].mean()), 6)


def _month_to_season(month: int) -> str:
    if month in {12, 1, 2}:
        return "winter"
    if month in {3, 4, 5}:
        return "spring"
    if month in {6, 7, 8}:
        return "summer"
    return "fall"


def _date_str(value: Any) -> str:
    if isinstance(value, pd.Timestamp):
        return value.date().isoformat()
    return str(value)


def _ensure_inputs_exist(paths: list[Path]) -> None:
    missing = [str(path) for path in paths if not path.exists()]
    if missing:
        raise WalkforwardClimatologyError(
            "Required local input parquet files are missing:\n" + "\n".join(missing)
        )


def _validate_walkforward_parameters(
    pricing_mode: str,
    window_profile: str,
    train_window: int,
    validation_window: int,
    test_window: int,
    step_window: int | None,
    min_trades_for_selection: int,
    selection_metric: str,
) -> None:
    if pricing_mode not in VALID_PRICING_MODES:
        raise WalkforwardClimatologyError(
            f"Unsupported pricing_mode {pricing_mode!r}. Expected one of {sorted(VALID_PRICING_MODES)}."
        )
    if window_profile not in VALID_WINDOW_PROFILES:
        raise WalkforwardClimatologyError(
            f"Unsupported window_profile {window_profile!r}. Expected one of {sorted(VALID_WINDOW_PROFILES)}."
        )
    if train_window < 1 or validation_window < 1 or test_window < 1:
        raise WalkforwardClimatologyError("train_window, validation_window, and test_window must each be at least 1.")
    if step_window is not None and step_window < 1:
        raise WalkforwardClimatologyError("step_window must be at least 1 when provided.")
    if min_trades_for_selection < 0:
        raise WalkforwardClimatologyError("min_trades_for_selection must be non-negative.")
    if selection_metric not in VALID_SELECTION_METRICS:
        raise WalkforwardClimatologyError(
            f"Unsupported selection_metric {selection_metric!r}. Expected one of {sorted(VALID_SELECTION_METRICS)}."
        )


def _resolve_window_configuration(
    scored_df: pd.DataFrame,
    window_profile: str,
    train_window: int,
    validation_window: int,
    test_window: int,
    step_window: int,
    expanding: bool,
) -> tuple[str, int, int, int, int]:
    unique_dates_count = int(scored_df["event_date"].dt.normalize().nunique())

    if window_profile == "custom":
        return "custom", train_window, validation_window, test_window, step_window

    if window_profile in WINDOW_PROFILE_CONFIGS:
        profile = WINDOW_PROFILE_CONFIGS[window_profile]
        return (
            window_profile,
            profile["train_window"],
            profile["validation_window"],
            profile["test_window"],
            profile["step_window"],
        )

    if window_profile == "auto":
        for candidate in ["standard", "research_short"]:
            profile = WINDOW_PROFILE_CONFIGS[candidate]
            possible_folds = _count_possible_folds(
                unique_dates_count=unique_dates_count,
                train_window=profile["train_window"],
                validation_window=profile["validation_window"],
                test_window=profile["test_window"],
                step_window=profile["step_window"],
                expanding=expanding,
            )
            if possible_folds >= 2:
                return (
                    candidate,
                    profile["train_window"],
                    profile["validation_window"],
                    profile["test_window"],
                    profile["step_window"],
                )
        profile = WINDOW_PROFILE_CONFIGS["research_short"]
        return (
            "research_short",
            profile["train_window"],
            profile["validation_window"],
            profile["test_window"],
            profile["step_window"],
        )

    raise WalkforwardClimatologyError(
        f"Unsupported window_profile {window_profile!r}. Expected one of {sorted(VALID_WINDOW_PROFILES)}."
    )


def _count_possible_folds(
    unique_dates_count: int,
    train_window: int,
    validation_window: int,
    test_window: int,
    step_window: int,
    expanding: bool,
) -> int:
    cursor = 0
    folds = 0
    while True:
        train_start = 0 if expanding else cursor
        train_end = (train_window + cursor) if expanding else (train_start + train_window)
        validation_end = train_end + validation_window
        test_end = validation_end + test_window
        if test_end > unique_dates_count:
            break
        folds += 1
        cursor += step_window
    return folds
