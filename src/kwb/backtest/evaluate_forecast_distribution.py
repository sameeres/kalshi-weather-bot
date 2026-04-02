from __future__ import annotations

import json
from pathlib import Path
from typing import Any

import numpy as np
import pandas as pd

from kwb.backtest.evaluate_climatology import _compute_trade_pnl, select_trade
from kwb.models.baseline_climatology import DEFAULT_SCORED_FILENAME as DEFAULT_CLIMATOLOGY_SCORED_FILENAME
from kwb.models.forecast_distribution import DEFAULT_SCORED_FILENAME as DEFAULT_FORECAST_SCORED_FILENAME
from kwb.settings import MARTS_DIR
from kwb.utils.logging import get_logger

logger = get_logger(__name__)

DEFAULT_CLIMATOLOGY_SCORED_PATH = MARTS_DIR / DEFAULT_CLIMATOLOGY_SCORED_FILENAME
DEFAULT_FORECAST_SCORED_PATH = MARTS_DIR / DEFAULT_FORECAST_SCORED_FILENAME
DEFAULT_TRADES_FILENAME = "backtest_trades_forecast_distribution.parquet"
DEFAULT_SUMMARY_FILENAME = "backtest_summary_forecast_distribution.json"
DEFAULT_REPORT_FILENAME = "backtest_report_forecast_distribution.md"

JOIN_KEYS = ["city_key", "market_ticker", "event_date", "decision_ts"]


class ForecastDistributionEvaluationError(ValueError):
    """Raised when the forecast distribution evaluation cannot complete safely."""


def evaluate_forecast_distribution_signals(
    climatology_scored_path: Path | None = None,
    forecast_scored_path: Path | None = None,
    output_dir: Path | None = None,
    min_edge: float = 0.05,
    min_samples: int = 30,
    min_price: float = 0.0,
    max_price: float = 25.0,
    contracts: int = 1,
    fee_per_contract: float = 0.01,
    allow_no: bool = False,
    fold_count: int = 3,
) -> tuple[Path, Path, Path, dict[str, Any]]:
    climatology_scored_path = climatology_scored_path or DEFAULT_CLIMATOLOGY_SCORED_PATH
    forecast_scored_path = forecast_scored_path or DEFAULT_FORECAST_SCORED_PATH
    output_dir = output_dir or MARTS_DIR
    output_dir.mkdir(parents=True, exist_ok=True)

    climatology_df = pd.read_parquet(climatology_scored_path).copy()
    forecast_df = pd.read_parquet(forecast_scored_path).copy()
    joined_df = _build_joined_frame(climatology_df=climatology_df, forecast_df=forecast_df)
    trades_df, summary = _evaluate_joined_frame(
        joined_df=joined_df,
        min_edge=min_edge,
        min_samples=min_samples,
        min_price=min_price,
        max_price=max_price,
        contracts=contracts,
        fee_per_contract=fee_per_contract,
        allow_no=allow_no,
        fold_count=fold_count,
    )

    trades_path = output_dir / DEFAULT_TRADES_FILENAME
    summary_path = output_dir / DEFAULT_SUMMARY_FILENAME
    report_path = output_dir / DEFAULT_REPORT_FILENAME
    trades_df.to_parquet(trades_path, index=False)
    summary_path.write_text(json.dumps(summary, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    report_path.write_text(_render_report(summary=summary), encoding="utf-8")
    return trades_path, summary_path, report_path, summary


def _build_joined_frame(climatology_df: pd.DataFrame, forecast_df: pd.DataFrame) -> pd.DataFrame:
    joined = climatology_df.merge(
        forecast_df,
        on=JOIN_KEYS,
        how="inner",
        suffixes=("_climatology", "_forecast"),
    )
    if joined.empty:
        return joined
    joined["event_date"] = pd.to_datetime(joined["event_date"], errors="coerce").dt.strftime("%Y-%m-%d")
    return joined.sort_values(["event_date", "city_key", "market_ticker"], kind="stable").reset_index(drop=True)


def _evaluate_joined_frame(
    joined_df: pd.DataFrame,
    min_edge: float,
    min_samples: int,
    min_price: float,
    max_price: float,
    contracts: int,
    fee_per_contract: float,
    allow_no: bool,
    fold_count: int,
) -> tuple[pd.DataFrame, dict[str, Any]]:
    strategies = ("climatology_only", "forecast_only", "intersection")
    rows: list[dict[str, Any]] = []

    for row in joined_df.to_dict("records"):
        climatology_selection = select_trade(
            row={
                "lookback_sample_size": row["lookback_sample_size_climatology"],
                "decision_price": row["decision_price_climatology"],
                "edge_yes": row["edge_yes_climatology"],
                "model_prob_no": row["model_prob_no_climatology"],
            },
            min_edge=min_edge,
            min_samples=min_samples,
            min_price=min_price,
            max_price=max_price,
            allow_no=allow_no,
        )
        forecast_selection = select_trade(
            row={
                "lookback_sample_size": row["lookback_sample_size_forecast"],
                "decision_price": row["decision_price_forecast"],
                "edge_yes": row["edge_yes_forecast"],
                "model_prob_no": row["model_prob_no_forecast"],
            },
            min_edge=min_edge,
            min_samples=min_samples,
            min_price=min_price,
            max_price=max_price,
            allow_no=allow_no,
        )

        selections = {
            "climatology_only": climatology_selection,
            "forecast_only": forecast_selection,
            "intersection": _intersection_selection(climatology_selection, forecast_selection),
        }

        for strategy_name in strategies:
            selection = selections[strategy_name]
            if selection is None:
                continue
            gross_pnl, net_pnl = _compute_trade_pnl(
                chosen_side=str(selection["chosen_side"]),
                decision_price=float(row["decision_price_forecast"]),
                resolved_yes=bool(row["resolved_yes_climatology"]),
                contracts=contracts,
                fee_per_contract=fee_per_contract,
            )
            rows.append(
                {
                    "strategy_name": strategy_name,
                    "city_key": row["city_key"],
                    "market_ticker": row["market_ticker"],
                    "event_date": row["event_date"],
                    "decision_ts": row["decision_ts"],
                    "chosen_side": selection["chosen_side"],
                    "entry_price": selection["entry_price"],
                    "edge_at_entry": selection["edge_at_entry"],
                    "resolved_yes": bool(row["resolved_yes_climatology"]),
                    "gross_pnl": gross_pnl,
                    "net_pnl": net_pnl,
                }
            )

    trades_df = pd.DataFrame(rows)
    if not trades_df.empty:
        trades_df = trades_df.sort_values(["strategy_name", "event_date", "city_key", "market_ticker"], kind="stable").reset_index(drop=True)

    return trades_df, {
        "rows_with_both_models": int(len(joined_df)),
        "parameters": {
            "min_edge": min_edge,
            "min_samples": min_samples,
            "min_price": min_price,
            "max_price": max_price,
            "contracts": contracts,
            "fee_per_contract": fee_per_contract,
            "allow_no": allow_no,
            "fold_count": fold_count,
        },
        "strategies": {
            strategy_name: _summarize_strategy(
                trades_df=trades_df.loc[trades_df["strategy_name"] == strategy_name].copy() if not trades_df.empty else pd.DataFrame(),
                fold_count=fold_count,
            )
            for strategy_name in strategies
        },
    }


def _intersection_selection(
    climatology_selection: dict[str, float | str] | None,
    forecast_selection: dict[str, float | str] | None,
) -> dict[str, float | str] | None:
    if climatology_selection is None or forecast_selection is None:
        return None
    if climatology_selection["chosen_side"] != forecast_selection["chosen_side"]:
        return None
    return {
        "chosen_side": str(forecast_selection["chosen_side"]),
        "entry_price": float(forecast_selection["entry_price"]),
        "edge_at_entry": min(
            float(climatology_selection["edge_at_entry"]),
            float(forecast_selection["edge_at_entry"]),
        ),
    }


def _summarize_strategy(trades_df: pd.DataFrame, fold_count: int) -> dict[str, Any]:
    if trades_df.empty:
        return {
            "trade_count": 0,
            "hit_rate": 0.0,
            "average_edge_at_entry": 0.0,
            "total_gross_pnl": 0.0,
            "total_net_pnl": 0.0,
            "average_net_pnl_per_trade": 0.0,
            "fold_stability": {"folds": [], "net_pnl_mean": 0.0, "net_pnl_std": 0.0, "positive_fold_fraction": 0.0},
        }

    hit_rate = float(
        trades_df.apply(
            lambda row: bool(row["resolved_yes"]) if row["chosen_side"] == "yes" else not bool(row["resolved_yes"]),
            axis=1,
        ).mean()
    )
    fold_rows = _build_fold_rows(trades_df=trades_df, fold_count=fold_count)
    fold_net_pnls = [row["total_net_pnl"] for row in fold_rows]
    net_pnl_std = float(pd.Series(fold_net_pnls, dtype=float).std(ddof=0)) if fold_net_pnls else 0.0
    positive_fold_fraction = float(pd.Series([value > 0 for value in fold_net_pnls], dtype=float).mean()) if fold_net_pnls else 0.0

    return {
        "trade_count": int(len(trades_df)),
        "hit_rate": round(hit_rate, 6),
        "average_edge_at_entry": round(float(trades_df["edge_at_entry"].mean()), 6),
        "total_gross_pnl": round(float(trades_df["gross_pnl"].sum()), 6),
        "total_net_pnl": round(float(trades_df["net_pnl"].sum()), 6),
        "average_net_pnl_per_trade": round(float(trades_df["net_pnl"].mean()), 6),
        "fold_stability": {
            "folds": fold_rows,
            "net_pnl_mean": round(float(pd.Series(fold_net_pnls, dtype=float).mean()) if fold_net_pnls else 0.0, 6),
            "net_pnl_std": round(net_pnl_std, 6),
            "positive_fold_fraction": round(positive_fold_fraction, 6),
        },
    }


def _build_fold_rows(trades_df: pd.DataFrame, fold_count: int) -> list[dict[str, Any]]:
    unique_dates = sorted(str(value) for value in trades_df["event_date"].dropna().unique())
    if not unique_dates:
        return []
    fold_count = max(1, min(fold_count, len(unique_dates)))
    date_chunks = [chunk for chunk in np.array_split(unique_dates, fold_count) if len(chunk) > 0]
    rows: list[dict[str, Any]] = []
    for index, chunk in enumerate(date_chunks, start=1):
        chunk_values = [str(value) for value in chunk.tolist()]
        fold_df = trades_df.loc[trades_df["event_date"].astype(str).isin(chunk_values)].copy()
        rows.append(
            {
                "fold_number": index,
                "start_date": min(chunk_values),
                "end_date": max(chunk_values),
                "trade_count": int(len(fold_df)),
                "total_net_pnl": round(float(fold_df["net_pnl"].sum()) if not fold_df.empty else 0.0, 6),
            }
        )
    return rows


def _render_report(summary: dict[str, Any]) -> str:
    lines = [
        "# Forecast Distribution Research Comparison",
        "",
        f"- Rows with both models: `{summary['rows_with_both_models']}`",
        f"- Min edge: `{summary['parameters']['min_edge']}`",
        f"- Min samples: `{summary['parameters']['min_samples']}`",
        f"- Price window: `{summary['parameters']['min_price']}` to `{summary['parameters']['max_price']}` cents",
        f"- Fee per contract: `{summary['parameters']['fee_per_contract']}`",
        "",
        "## Strategies",
        "",
    ]
    for strategy_name, strategy_summary in summary["strategies"].items():
        fold_stability = strategy_summary["fold_stability"]
        lines.extend(
            [
                f"### {strategy_name}",
                "",
                f"- Trades: `{strategy_summary['trade_count']}`",
                f"- Hit rate: `{strategy_summary['hit_rate']}`",
                f"- Total net PnL: `{strategy_summary['total_net_pnl']}`",
                f"- Avg net PnL / trade: `{strategy_summary['average_net_pnl_per_trade']}`",
                f"- Fold net PnL mean/std: `{fold_stability['net_pnl_mean']}` / `{fold_stability['net_pnl_std']}`",
                f"- Positive fold fraction: `{fold_stability['positive_fold_fraction']}`",
                "",
            ]
        )
    return "\n".join(lines)
