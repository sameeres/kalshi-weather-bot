from __future__ import annotations

import json
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import pandas as pd

from kwb.backtest.walkforward_climatology import (
    ThresholdParams,
    _build_temporal_folds,
    _evaluate_mode_frame,
    _prepare_scored_frame,
    run_walkforward_climatology,
)
from kwb.utils.logging import get_logger

logger = get_logger(__name__)

DEFAULT_REPORT_JSON_FILENAME = "climatology_friction_stress_test.json"
DEFAULT_REPORT_CSV_FILENAME = "climatology_friction_stress_test_summary.csv"
DEFAULT_REPORT_MARKDOWN_FILENAME = "climatology_friction_stress_test.md"
DEFAULT_FILTERED_TRADES_FILENAME = "climatology_friction_filtered_trades_strict_edge.csv"


@dataclass(frozen=True)
class FrictionScenario:
    code: str
    label: str
    description: str
    fee_model: str
    fee_per_contract: float
    min_edge_grid: tuple[float, ...]
    max_spread_grid: tuple[float | None, ...]
    assumption_type: str


SCENARIOS = [
    FrictionScenario(
        code="A",
        label="Executable No Fees",
        description="Current executable baseline with no modeled fees.",
        fee_model="flat_per_contract",
        fee_per_contract=0.0,
        min_edge_grid=(0.0, 0.02, 0.05),
        max_spread_grid=(None, 5.0),
        assumption_type="baseline",
    ),
    FrictionScenario(
        code="B",
        label="Executable + Standard Taker Fees",
        description="Executable pricing plus official Kalshi standard taker fees on immediate matches.",
        fee_model="kalshi_standard_taker",
        fee_per_contract=0.0,
        min_edge_grid=(0.0, 0.02, 0.05),
        max_spread_grid=(None, 5.0),
        assumption_type="official_directly_modeled",
    ),
    FrictionScenario(
        code="C",
        label="Taker Fees + Stricter Edge",
        description="Official taker fees plus a stricter minimum executable edge threshold.",
        fee_model="kalshi_standard_taker",
        fee_per_contract=0.0,
        min_edge_grid=(0.05,),
        max_spread_grid=(None, 5.0),
        assumption_type="official_directly_modeled",
    ),
    FrictionScenario(
        code="D",
        label="Taker Fees + Stricter Edge + Tight Spread",
        description="Official taker fees, stricter edge, and a 2-cent max spread filter supported by stored quote data.",
        fee_model="kalshi_standard_taker",
        fee_per_contract=0.0,
        min_edge_grid=(0.05,),
        max_spread_grid=(2.0,),
        assumption_type="inferred_from_available_market_data",
    ),
]


class ClimatologyFrictionStressTestError(ValueError):
    """Raised when the executable friction stress test cannot complete safely."""


def stress_test_climatology_frictions(
    run_dir: Path,
    output_dir: Path | None = None,
    walkforward_profile: str = "research_short",
    selection_metric: str = "total_net_pnl",
    min_trades_for_selection: int = 1,
) -> tuple[Path, Path, Path, dict[str, Any]]:
    run_dir = run_dir.expanduser().resolve()
    scored_path = run_dir / "backtest_scored_climatology.parquet"
    backtest_path = run_dir / "backtest_dataset.parquet"
    if not scored_path.exists() or not backtest_path.exists():
        raise ClimatologyFrictionStressTestError(
            f"Run directory is missing required scored/backtest files: {run_dir}"
        )

    if output_dir is None:
        timestamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
        output_dir = Path("reports") / f"climatology_friction_stress_{timestamp}"
    output_dir.mkdir(parents=True, exist_ok=True)

    scenario_reports: list[dict[str, Any]] = []
    summary_rows: list[dict[str, Any]] = []

    scored_df = pd.read_parquet(scored_path).copy()
    backtest_df = pd.read_parquet(backtest_path).copy()
    _prepare_scored_frame(scored_df)

    for scenario in SCENARIOS:
        scenario_dir = output_dir / f"scenario_{scenario.code.lower()}"
        scenario_dir.mkdir(parents=True, exist_ok=True)
        results_path, summary_path, diagnostics_path, walkforward_summary = run_walkforward_climatology(
            scored_dataset_path=scored_path,
            output_dir=scenario_dir,
            pricing_mode="candle_proxy",
            window_profile=walkforward_profile,
            min_trades_for_selection=min_trades_for_selection,
            min_edge_grid=scenario.min_edge_grid,
            min_samples_grid=(1, 5),
            min_price_grid=(0.0,),
            max_price_grid=(100.0,),
            executable_fee_model=scenario.fee_model,
            executable_fee_per_contract=scenario.fee_per_contract,
            max_spread_grid=scenario.max_spread_grid,
            allow_no_grid=(False,),
            expanding=True,
            selection_metric=selection_metric,
        )
        results_df = pd.read_csv(results_path)
        diagnostics_df = pd.read_csv(diagnostics_path)
        trade_rows = _rebuild_walkforward_trades(
            scored_df=scored_df,
            backtest_df=backtest_df,
            results_df=results_df,
            fee_model=scenario.fee_model,
            fee_per_contract=scenario.fee_per_contract,
            walkforward_profile=walkforward_profile,
        )
        rebuilt_trades_path = scenario_dir / "scenario_trades_rebuilt.csv"
        trade_rows.to_csv(rebuilt_trades_path, index=False)
        aggregate = _scenario_aggregate(trade_rows, walkforward_summary)
        breakdowns = _scenario_breakdowns(trade_rows)

        scenario_report = {
            "scenario_code": scenario.code,
            "scenario_label": scenario.label,
            "description": scenario.description,
            "assumption_type": scenario.assumption_type,
            "walkforward_summary": walkforward_summary,
            "aggregate": aggregate,
            "breakdowns": breakdowns,
            "diagnostics_highlights": _diagnostic_highlights(diagnostics_df),
            "output_paths": {
                "results_csv": str(results_path),
                "summary_json": str(summary_path),
                "diagnostics_csv": str(diagnostics_path),
                "rebuilt_trades_csv": str(rebuilt_trades_path),
            },
        }
        scenario_reports.append(scenario_report)
        summary_rows.append(
            {
                "scenario_code": scenario.code,
                "scenario_label": scenario.label,
                "fee_model": scenario.fee_model,
                "min_edge_grid": ",".join(str(value) for value in scenario.min_edge_grid),
                "max_spread_grid": ",".join("none" if value is None else str(value) for value in scenario.max_spread_grid),
                **aggregate,
            }
        )

    filtered_by_strict_edge = _filtered_trades_between_scenarios(scenario_reports, source_code="B", target_code="C")
    filtered_path = output_dir / DEFAULT_FILTERED_TRADES_FILENAME
    filtered_by_strict_edge.to_csv(filtered_path, index=False)

    report = {
        "generated_at_utc": datetime.now(timezone.utc).isoformat(),
        "source_run_dir": str(run_dir),
        "walkforward_profile": walkforward_profile,
        "weather_series_fee_override_found": False,
        "weather_series_fee_override_basis": (
            "No weather-series-specific fee override fields were found in the staged backtest/scored datasets, "
            "and no fee-override logic was found in the repo's executable evaluation path."
        ),
        "modeled_assumptions": {
            "official_and_directly_modeled": [
                "Immediate executable buys use the staged ask-side proxy.",
                "Kalshi standard taker fee formula: ceil_to_cent(0.07 * C * P * (1 - P)).",
                "No settlement fee or membership fee modeled.",
                "No maker-fill assumptions used.",
            ],
            "inferred_from_available_market_data": [
                "Scenario D adds a 2-cent max spread filter using stored quote spread data.",
            ],
            "unknown_or_not_modelable": [
                "No staged orderbook depth beyond top-of-book proxies, so walking the book cannot be modeled.",
                "No verified market-specific tick-size metadata was found, so one-tick adverse-entry sensitivity was not modeled.",
                "Market impact beyond the best quoted price remains unmodeled.",
            ],
        },
        "scenario_reports": scenario_reports,
        "strict_edge_filtered_trades": filtered_by_strict_edge.to_dict("records"),
        "best_scenario": _best_scenario(summary_rows),
        "worst_scenario": _worst_scenario(summary_rows),
    }

    json_path = output_dir / DEFAULT_REPORT_JSON_FILENAME
    csv_path = output_dir / DEFAULT_REPORT_CSV_FILENAME
    markdown_path = output_dir / DEFAULT_REPORT_MARKDOWN_FILENAME
    json_path.write_text(json.dumps(report, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    pd.DataFrame(summary_rows).to_csv(csv_path, index=False)
    markdown_path.write_text(_render_markdown_report(report), encoding="utf-8")

    logger.info("Saved climatology friction stress test artifacts to %s", output_dir)
    return json_path, csv_path, markdown_path, report


def _rebuild_walkforward_trades(
    scored_df: pd.DataFrame,
    backtest_df: pd.DataFrame,
    results_df: pd.DataFrame,
    fee_model: str,
    fee_per_contract: float,
    walkforward_profile: str,
) -> pd.DataFrame:
    window_config = {
        "research_short": {"train_window": 30, "validation_window": 15, "test_window": 15, "step_window": 15},
        "standard": {"train_window": 60, "validation_window": 30, "test_window": 30, "step_window": 30},
    }.get(walkforward_profile)
    if window_config is None:
        raise ClimatologyFrictionStressTestError(
            f"Trade reconstruction only supports explicit standard/research_short profiles, got {walkforward_profile!r}."
        )

    folds = _build_temporal_folds(
        scored_df=scored_df,
        train_window=window_config["train_window"],
        validation_window=window_config["validation_window"],
        test_window=window_config["test_window"],
        step_window=window_config["step_window"],
        expanding=True,
    )

    meta = backtest_df[
        [
            "market_ticker",
            "event_date",
            "market_subtitle",
            "strike_type",
            "floor_strike",
            "cap_strike",
        ]
    ].drop_duplicates()
    meta["event_date"] = meta["event_date"].astype(str)

    rows: list[pd.DataFrame] = []
    for result_row in results_df.to_dict("records"):
        params = ThresholdParams(
            min_edge=float(result_row["selected_min_edge"]),
            min_samples=int(result_row["selected_min_samples"]),
            min_price=float(result_row["selected_min_price"]),
            max_price=float(result_row["selected_max_price"]),
            allow_no=bool(result_row["selected_allow_no"]),
            max_spread=None if pd.isna(result_row["selected_max_spread"]) else float(result_row["selected_max_spread"]),
        )
        fold = folds[int(result_row["fold_number"]) - 1]
        trades_df, _ = _evaluate_mode_frame(
            fold["test_df"],
            "candle_proxy",
            params,
            executable_fee_model=fee_model,
            executable_fee_per_contract=fee_per_contract,
        )
        if trades_df.empty:
            continue
        trades_df = trades_df.copy()
        trades_df["event_date"] = trades_df["event_date"].astype(str)
        trades_df["fold_number"] = int(result_row["fold_number"])
        trades_df = trades_df.merge(meta, on=["market_ticker", "event_date"], how="left")
        trades_df["contract_type"] = trades_df["market_subtitle"].map(_contract_type)
        trades_df["entry_price_bucket"] = pd.cut(
            trades_df["entry_price"].astype(float),
            bins=[0, 25, 50, 75, 100],
            labels=["0-25", "25-50", "50-75", "75-100"],
            include_lowest=True,
            right=True,
        ).astype(str)
        trades_df["won"] = trades_df.apply(
            lambda row: bool(row["resolved_yes"]) if row["chosen_side"] == "yes" else not bool(row["resolved_yes"]),
            axis=1,
        )
        rows.append(trades_df)

    if not rows:
        return pd.DataFrame()
    return pd.concat(rows, ignore_index=True)


def _scenario_aggregate(trades_df: pd.DataFrame, walkforward_summary: dict[str, Any]) -> dict[str, Any]:
    mode_summary = walkforward_summary["results_by_pricing_mode"]["candle_proxy"]
    average_gross_edge = round(float(trades_df["gross_edge_at_entry"].mean()), 6) if not trades_df.empty else 0.0
    average_net_edge = round(float(trades_df["edge_at_entry"].mean()), 6) if not trades_df.empty else 0.0
    return {
        "folds_scored": int(mode_summary["folds_scored"]),
        "rows_evaluated": int(mode_summary["rows_evaluated"]),
        "trades_taken": int(mode_summary["trades_taken"]),
        "win_rate": float(mode_summary["hit_rate"]),
        "total_net_pnl": float(mode_summary["total_net_pnl"]),
        "average_net_pnl_per_trade": float(mode_summary["average_net_pnl_per_trade"]),
        "average_edge_at_entry": average_net_edge,
        "average_gross_edge_at_entry": average_gross_edge,
        "average_fee_per_trade": round(float(trades_df["fees"].mean()), 6) if not trades_df.empty else 0.0,
        "total_fees": round(float(trades_df["fees"].sum()), 6) if not trades_df.empty else 0.0,
        "edge_lost_to_fees_per_trade": round(average_gross_edge - average_net_edge, 6),
    }


def _scenario_breakdowns(trades_df: pd.DataFrame) -> dict[str, Any]:
    if trades_df.empty:
        return {"contract_type": [], "entry_price_bucket": []}
    contract_type = (
        trades_df.groupby("contract_type", dropna=False)
        .agg(
            trades=("market_ticker", "size"),
            win_rate=("won", "mean"),
            total_net_pnl=("net_pnl", "sum"),
            average_net_pnl_per_trade=("net_pnl", "mean"),
            average_fee_per_trade=("fees", "mean"),
        )
        .round(6)
        .reset_index()
        .to_dict("records")
    )
    entry_price_bucket = (
        trades_df.groupby("entry_price_bucket", dropna=False)
        .agg(
            trades=("market_ticker", "size"),
            win_rate=("won", "mean"),
            total_net_pnl=("net_pnl", "sum"),
            average_net_pnl_per_trade=("net_pnl", "mean"),
            average_fee_per_trade=("fees", "mean"),
        )
        .round(6)
        .reset_index()
        .to_dict("records")
    )
    return {"contract_type": contract_type, "entry_price_bucket": entry_price_bucket}


def _filtered_trades_between_scenarios(
    scenario_reports: list[dict[str, Any]],
    source_code: str,
    target_code: str,
) -> pd.DataFrame:
    source = next(report for report in scenario_reports if report["scenario_code"] == source_code)
    target = next(report for report in scenario_reports if report["scenario_code"] == target_code)

    source_dir = Path(source["output_paths"]["results_csv"]).parent
    target_dir = Path(target["output_paths"]["results_csv"]).parent
    source_trades = _load_rebuilt_trades(source_dir)
    target_trades = _load_rebuilt_trades(target_dir)

    if source_trades.empty:
        return pd.DataFrame()
    if target_trades.empty:
        return source_trades.copy()

    join_keys = ["fold_number", "market_ticker", "event_date", "decision_ts", "chosen_side"]
    merged = source_trades.merge(
        target_trades[join_keys],
        on=join_keys,
        how="left",
        indicator=True,
    )
    filtered = merged.loc[merged["_merge"] == "left_only"].drop(columns=["_merge"])
    keep_columns = [
        "fold_number",
        "market_ticker",
        "event_date",
        "chosen_side",
        "entry_price",
        "gross_edge_at_entry",
        "edge_at_entry",
        "fees",
        "market_subtitle",
        "contract_type",
        "entry_price_bucket",
    ]
    return filtered.reindex(columns=keep_columns).sort_values(
        ["fold_number", "event_date", "market_ticker"], kind="stable"
    ).reset_index(drop=True)


def _load_rebuilt_trades(scenario_dir: Path) -> pd.DataFrame:
    path = scenario_dir / "scenario_trades_rebuilt.csv"
    if path.exists():
        return pd.read_csv(path)
    return pd.DataFrame()


def _contract_type(subtitle: Any) -> str:
    text = str(subtitle or "").lower()
    if "or below" in text:
        return "or_below"
    if "or above" in text:
        return "or_above"
    if " to " in text:
        return "between"
    return "unknown"


def _diagnostic_highlights(diagnostics_df: pd.DataFrame) -> dict[str, Any]:
    highlights: dict[str, Any] = {}
    for breakdown in ["entry_price_bucket", "month_bucket", "spread_bucket"]:
        subset = diagnostics_df.loc[diagnostics_df["breakdown"] == breakdown].copy()
        if subset.empty:
            continue
        top = subset.sort_values(["total_net_pnl", "trades_taken"], ascending=[False, False], kind="stable").iloc[0]
        bottom = subset.sort_values(["total_net_pnl", "trades_taken"], ascending=[True, False], kind="stable").iloc[0]
        highlights[breakdown] = {
            "top_bucket": top.to_dict(),
            "bottom_bucket": bottom.to_dict(),
        }
    return highlights


def _best_scenario(summary_rows: list[dict[str, Any]]) -> dict[str, Any]:
    return max(summary_rows, key=lambda row: (float(row["total_net_pnl"]), float(row["average_net_pnl_per_trade"])))


def _worst_scenario(summary_rows: list[dict[str, Any]]) -> dict[str, Any]:
    return min(summary_rows, key=lambda row: (float(row["total_net_pnl"]), float(row["average_net_pnl_per_trade"])))


def _render_markdown_report(report: dict[str, Any]) -> str:
    lines = [
        "# Climatology Friction Stress Test",
        "",
        f"- Source run: `{report['source_run_dir']}`",
        f"- Walk-forward profile: `{report['walkforward_profile']}`",
        f"- Weather-series fee override found: `{report['weather_series_fee_override_found']}`",
        "",
        "## Scenario Summary",
        "",
        "| Scenario | Trades | Win Rate | Total PnL | Avg PnL/Trade | Avg Net Edge | Avg Gross Edge | Total Fees |",
        "| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: |",
    ]
    for scenario in report["scenario_reports"]:
        agg = scenario["aggregate"]
        lines.append(
            "| "
            + f"{scenario['scenario_code']} {scenario['scenario_label']} | {agg['trades_taken']} | {agg['win_rate']:.4f} | "
            + f"{agg['total_net_pnl']:.4f} | {agg['average_net_pnl_per_trade']:.4f} | {agg['average_edge_at_entry']:.4f} | "
            + f"{agg['average_gross_edge_at_entry']:.4f} | {agg['total_fees']:.4f} |"
        )
    lines.extend(
        [
            "",
            "## Strict-Edge Filtered Trades",
            "",
            f"- Trades removed when moving from Scenario B to Scenario C: `{len(report['strict_edge_filtered_trades'])}`",
            "",
        ]
    )
    return "\n".join(lines).strip() + "\n"
