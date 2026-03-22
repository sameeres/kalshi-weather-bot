from __future__ import annotations

import json
from pathlib import Path
from typing import Any

import pandas as pd

from kwb.backtest.pnl import contract_pnl
from kwb.models.baseline_climatology import (
    DEFAULT_MODEL_NAME,
    DEFAULT_SCORED_FILENAME,
    ClimatologyModelError,
    evaluate_scored_climatology,
)
from kwb.settings import MARTS_DIR
from kwb.utils.logging import get_logger

logger = get_logger(__name__)

DEFAULT_SCORED_PATH = MARTS_DIR / DEFAULT_SCORED_FILENAME
DEFAULT_TRADES_FILENAME = "backtest_trades_climatology.parquet"
DEFAULT_SUMMARY_FILENAME = "backtest_summary_climatology.json"
PRICING_MODE = "decision_price"
QUOTE_SOURCE = "decision_price_close"

REQUIRED_SCORED_COLUMNS = {
    "city_key",
    "market_ticker",
    "event_date",
    "decision_ts",
    "decision_price",
    "resolved_yes",
    "model_prob_yes",
    "model_prob_no",
    "edge_yes",
    "lookback_sample_size",
    "model_name",
}


class ClimatologyEvaluationError(ValueError):
    """Raised when climatology backtest evaluation cannot be completed safely."""


def evaluate_climatology_strategy(
    scored_dataset_path: Path | None = None,
    output_dir: Path | None = None,
    min_edge: float = 0.0,
    min_samples: int = 1,
    min_price: float = 0.0,
    max_price: float = 100.0,
    contracts: int = 1,
    fee_per_contract: float = 0.0,
    allow_no: bool = False,
) -> tuple[Path, Path, dict[str, int | float | bool]]:
    """Evaluate a simple threshold-based climatology paper strategy."""
    scored_dataset_path = scored_dataset_path or DEFAULT_SCORED_PATH
    output_dir = output_dir or MARTS_DIR
    output_dir.mkdir(parents=True, exist_ok=True)

    _validate_parameters(
        min_edge=min_edge,
        min_samples=min_samples,
        min_price=min_price,
        max_price=max_price,
        contracts=contracts,
        fee_per_contract=fee_per_contract,
    )
    _ensure_inputs_exist([scored_dataset_path])
    scored_df = _load_scored_frame(scored_dataset_path)

    trades_df, summary = _evaluate_frame(
        scored_df=scored_df,
        min_edge=min_edge,
        min_samples=min_samples,
        min_price=min_price,
        max_price=max_price,
        contracts=contracts,
        fee_per_contract=fee_per_contract,
        allow_no=allow_no,
    )

    trades_path = output_dir / DEFAULT_TRADES_FILENAME
    summary_path = output_dir / DEFAULT_SUMMARY_FILENAME
    trades_df.to_parquet(trades_path, index=False)
    summary_path.write_text(json.dumps(summary, indent=2, sort_keys=True) + "\n", encoding="utf-8")

    logger.info(
        "Saved %s climatology backtest trades to %s and summary to %s",
        len(trades_df),
        trades_path,
        summary_path,
    )
    return trades_path, summary_path, summary


def _evaluate_frame(
    scored_df: pd.DataFrame,
    min_edge: float,
    min_samples: int,
    min_price: float,
    max_price: float,
    contracts: int,
    fee_per_contract: float,
    allow_no: bool,
) -> tuple[pd.DataFrame, dict[str, int | float | bool]]:
    rows_available = len(scored_df)
    rows_scored = int(scored_df["model_prob_yes"].notna().sum())
    scored_summary = evaluate_scored_climatology(scored_df)

    trade_rows: list[dict[str, Any]] = []
    for row in scored_df.to_dict("records"):
        selection = select_trade(
            row=row,
            min_edge=min_edge,
            min_samples=min_samples,
            min_price=min_price,
            max_price=max_price,
            allow_no=allow_no,
        )
        if selection is None:
            continue

        gross_pnl, net_pnl = _compute_trade_pnl(
            chosen_side=selection["chosen_side"],
            decision_price=float(row["decision_price"]),
            resolved_yes=bool(row["resolved_yes"]),
            contracts=contracts,
            fee_per_contract=fee_per_contract,
        )
        trade_rows.append(
            {
                "city_key": row["city_key"],
                "market_ticker": row["market_ticker"],
                "event_date": row["event_date"],
                "decision_ts": row["decision_ts"],
                "decision_price": float(row["decision_price"]),
                "resolved_yes": bool(row["resolved_yes"]),
                "model_prob_yes": float(row["model_prob_yes"]),
                "model_prob_no": float(row["model_prob_no"]),
                "edge_yes": float(row["edge_yes"]),
                "chosen_side": selection["chosen_side"],
                "entry_price": selection["entry_price"],
                "edge_at_entry": selection["edge_at_entry"],
                "pricing_mode": PRICING_MODE,
                "contracts": contracts,
                "gross_pnl": gross_pnl,
                "net_pnl": net_pnl,
                "lookback_sample_size": int(row["lookback_sample_size"]),
                "model_name": row["model_name"],
            }
        )

    trades_df = _build_trades_frame(trade_rows)
    summary = _summarize_trades(
        trades_df=trades_df,
        rows_available=rows_available,
        rows_scored=rows_scored,
        scored_summary=scored_summary,
        allow_no=allow_no,
        min_edge=min_edge,
        min_samples=min_samples,
        min_price=min_price,
        max_price=max_price,
        contracts=contracts,
        fee_per_contract=fee_per_contract,
    )
    return trades_df, summary


def select_trade(
    row: dict[str, Any],
    min_edge: float,
    min_samples: int,
    min_price: float,
    max_price: float,
    allow_no: bool,
) -> dict[str, float | str] | None:
    if int(row["lookback_sample_size"]) < min_samples:
        return None

    yes_price = float(row["decision_price"])
    yes_edge = float(row["edge_yes"])
    candidates: list[dict[str, float | str]] = []

    if min_price <= yes_price <= max_price and yes_edge >= min_edge:
        candidates.append(
            {
                "chosen_side": "yes",
                "entry_price": yes_price,
                "edge_at_entry": yes_edge,
            }
        )

    if allow_no:
        no_price = round(100.0 - yes_price, 6)
        no_edge = round(float(row["model_prob_no"]) - (no_price / 100.0), 6)
        if min_price <= no_price <= max_price and no_edge >= min_edge:
            candidates.append(
                {
                    "chosen_side": "no",
                    "entry_price": no_price,
                    "edge_at_entry": no_edge,
                }
            )

    if not candidates:
        return None

    return max(
        candidates,
        key=lambda candidate: (float(candidate["edge_at_entry"]), candidate["chosen_side"] == "yes"),
    )


def _compute_trade_pnl(
    chosen_side: str,
    decision_price: float,
    resolved_yes: bool,
    contracts: int,
    fee_per_contract: float,
) -> tuple[float, float]:
    yes_price_dollars = decision_price / 100.0
    no_price_dollars = (100.0 - decision_price) / 100.0

    if chosen_side == "yes":
        gross_pnl = contract_pnl(
            fill_price=yes_price_dollars,
            resolved_value=1.0 if resolved_yes else 0.0,
            contracts=contracts,
            fee_per_contract=0.0,
        )
        net_pnl = contract_pnl(
            fill_price=yes_price_dollars,
            resolved_value=1.0 if resolved_yes else 0.0,
            contracts=contracts,
            fee_per_contract=fee_per_contract,
        )
        return round(gross_pnl, 6), round(net_pnl, 6)

    if chosen_side == "no":
        gross_pnl = contract_pnl(
            fill_price=no_price_dollars,
            resolved_value=0.0 if resolved_yes else 1.0,
            contracts=contracts,
            fee_per_contract=0.0,
        )
        net_pnl = contract_pnl(
            fill_price=no_price_dollars,
            resolved_value=0.0 if resolved_yes else 1.0,
            contracts=contracts,
            fee_per_contract=fee_per_contract,
        )
        return round(gross_pnl, 6), round(net_pnl, 6)

    raise ClimatologyEvaluationError(f"Unsupported chosen_side {chosen_side!r}")


def _build_trades_frame(rows: list[dict[str, Any]]) -> pd.DataFrame:
    columns = [
        "city_key",
        "market_ticker",
        "event_date",
        "decision_ts",
        "decision_price",
        "resolved_yes",
        "model_prob_yes",
        "model_prob_no",
        "edge_yes",
        "chosen_side",
        "entry_price",
        "edge_at_entry",
        "pricing_mode",
        "contracts",
        "gross_pnl",
        "net_pnl",
        "lookback_sample_size",
        "model_name",
    ]
    df = pd.DataFrame(rows, columns=columns)
    if df.empty:
        return df
    return df.sort_values(["event_date", "city_key", "market_ticker"], kind="stable").reset_index(drop=True)


def _summarize_trades(
    trades_df: pd.DataFrame,
    rows_available: int,
    rows_scored: int,
    scored_summary: dict[str, int | float],
    allow_no: bool,
    min_edge: float,
    min_samples: int,
    min_price: float,
    max_price: float,
    contracts: int,
    fee_per_contract: float,
) -> dict[str, int | float | bool]:
    if trades_df.empty:
        hit_rate = 0.0
        average_edge_at_entry = 0.0
        average_pnl_per_trade = 0.0
        total_gross_pnl = 0.0
        total_net_pnl = 0.0
    else:
        hit_series = trades_df.apply(
            lambda row: bool(row["resolved_yes"]) if row["chosen_side"] == "yes" else not bool(row["resolved_yes"]),
            axis=1,
        )
        hit_rate = round(float(hit_series.mean()), 6)
        average_edge_at_entry = round(float(trades_df["edge_at_entry"].mean()), 6)
        average_pnl_per_trade = round(float(trades_df["net_pnl"].mean()), 6)
        total_gross_pnl = round(float(trades_df["gross_pnl"].sum()), 6)
        total_net_pnl = round(float(trades_df["net_pnl"].sum()), 6)

    side_counts = _count_by_column(trades_df, "chosen_side")
    price_bucket_counts = _bucket_counts(
        trades_df["entry_price"] if "entry_price" in trades_df else pd.Series(dtype=float),
        bins=[0, 25, 50, 75, 100],
        labels=["0-25", "25-50", "50-75", "75-100"],
    )

    return {
        "pricing_mode": PRICING_MODE,
        "quote_source": QUOTE_SOURCE,
        "uses_true_quotes": False,
        "rows_available": rows_available,
        "rows_scored": rows_scored,
        "trades_taken": int(len(trades_df)),
        "yes_trades_taken": side_counts.get("yes", 0),
        "no_trades_taken": side_counts.get("no", 0),
        "hit_rate": hit_rate,
        "average_edge_at_entry": average_edge_at_entry,
        "average_pnl_per_trade": average_pnl_per_trade,
        "total_gross_pnl": total_gross_pnl,
        "total_net_pnl": total_net_pnl,
        "brier_score": float(scored_summary["brier_score"]),
        "average_lookback_sample_size": float(scored_summary["average_lookback_sample_size"]),
        "entry_price_bucket_counts": price_bucket_counts,
        "allow_no": allow_no,
        "min_edge": min_edge,
        "min_samples": min_samples,
        "min_price": min_price,
        "max_price": max_price,
        "contracts": contracts,
        "fee_per_contract": fee_per_contract,
    }


def _validate_parameters(
    min_edge: float,
    min_samples: int,
    min_price: float,
    max_price: float,
    contracts: int,
    fee_per_contract: float,
) -> None:
    if min_edge < 0:
        raise ClimatologyEvaluationError(f"min_edge must be non-negative, got {min_edge}")
    if min_samples < 1:
        raise ClimatologyEvaluationError(f"min_samples must be at least 1, got {min_samples}")
    if contracts < 1:
        raise ClimatologyEvaluationError(f"contracts must be at least 1, got {contracts}")
    if fee_per_contract < 0:
        raise ClimatologyEvaluationError(f"fee_per_contract must be non-negative, got {fee_per_contract}")
    if min_price < 0 or max_price > 100 or min_price > max_price:
        raise ClimatologyEvaluationError(
            f"Price filter must satisfy 0 <= min_price <= max_price <= 100, got {min_price}, {max_price}"
        )


def _ensure_inputs_exist(paths: list[Path]) -> None:
    missing = [str(path) for path in paths if not path.exists()]
    if missing:
        raise ClimatologyEvaluationError(
            "Required local input parquet files are missing:\n" + "\n".join(missing)
        )


def _load_scored_frame(path: Path) -> pd.DataFrame:
    try:
        frame = pd.read_parquet(path)
    except ClimatologyModelError as exc:  # pragma: no cover - defensive, not expected from pandas
        raise ClimatologyEvaluationError(str(exc)) from exc

    missing = sorted(REQUIRED_SCORED_COLUMNS - set(frame.columns))
    if missing:
        raise ClimatologyEvaluationError(f"Required columns are missing from {path}: {', '.join(missing)}")
    return frame.copy()


def _count_by_column(df: pd.DataFrame, column: str) -> dict[str, int]:
    if df.empty or column not in df.columns:
        return {}
    return {str(key): int(value) for key, value in df[column].value_counts(dropna=False).to_dict().items()}


def _bucket_counts(series: pd.Series, bins: list[float], labels: list[str]) -> dict[str, int]:
    if series.empty:
        return {label: 0 for label in labels}
    bucketed = pd.cut(series.astype(float), bins=bins, labels=labels, include_lowest=True, right=True)
    counts = bucketed.value_counts(sort=False, dropna=False)
    return {label: int(counts.get(label, 0)) for label in labels}
