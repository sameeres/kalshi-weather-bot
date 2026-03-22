from __future__ import annotations

import json
from pathlib import Path
from typing import Any

import pandas as pd

from kwb.backtest.fills import conservative_fill
from kwb.backtest.pnl import contract_pnl
from kwb.features.market_microstructure import spread
from kwb.models.baseline_climatology import (
    DEFAULT_SCORED_FILENAME,
    evaluate_scored_climatology,
)
from kwb.settings import MARTS_DIR
from kwb.utils.logging import get_logger

logger = get_logger(__name__)

DEFAULT_SCORED_PATH = MARTS_DIR / DEFAULT_SCORED_FILENAME
DEFAULT_EXECUTABLE_TRADES_FILENAME = "backtest_trades_climatology_executable.parquet"
DEFAULT_EXECUTABLE_SUMMARY_FILENAME = "backtest_summary_climatology_executable.json"
PRICING_MODE = "candle_proxy"
QUOTE_SOURCE = "decision_candle_ohlc_bounds"

REQUIRED_EXECUTABLE_COLUMNS = {
    "city_key",
    "market_ticker",
    "event_date",
    "decision_ts",
    "decision_price",
    "resolved_yes",
    "model_prob_yes",
    "model_prob_no",
    "fair_yes",
    "fair_no",
    "lookback_sample_size",
    "model_name",
    "yes_bid",
    "yes_ask",
    "no_bid",
    "no_ask",
}


class ClimatologyExecutableEvaluationError(ValueError):
    """Raised when executable climatology evaluation cannot be completed safely."""


def evaluate_climatology_executable_strategy(
    scored_dataset_path: Path | None = None,
    output_path: Path | None = None,
    summary_output_path: Path | None = None,
    min_edge: float = 0.0,
    min_samples: int = 1,
    min_price: float = 0.0,
    max_price: float = 100.0,
    contracts: int = 1,
    fee_per_contract: float = 0.0,
    allow_no: bool = False,
    max_spread: float | None = None,
) -> tuple[Path, Path, dict[str, int | float | bool]]:
    """Evaluate a conservative executable-quote paper strategy on the scored climatology dataset."""
    scored_dataset_path = scored_dataset_path or DEFAULT_SCORED_PATH
    output_path = output_path or (MARTS_DIR / DEFAULT_EXECUTABLE_TRADES_FILENAME)
    summary_output_path = summary_output_path or (MARTS_DIR / DEFAULT_EXECUTABLE_SUMMARY_FILENAME)
    output_path.parent.mkdir(parents=True, exist_ok=True)
    summary_output_path.parent.mkdir(parents=True, exist_ok=True)

    _validate_parameters(
        min_edge=min_edge,
        min_samples=min_samples,
        min_price=min_price,
        max_price=max_price,
        contracts=contracts,
        fee_per_contract=fee_per_contract,
        max_spread=max_spread,
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
        max_spread=max_spread,
    )

    trades_df.to_parquet(output_path, index=False)
    summary_output_path.write_text(json.dumps(summary, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    logger.info(
        "Saved %s executable climatology trades to %s and summary to %s",
        len(trades_df),
        output_path,
        summary_output_path,
    )
    return output_path, summary_output_path, summary


def _evaluate_frame(
    scored_df: pd.DataFrame,
    min_edge: float,
    min_samples: int,
    min_price: float,
    max_price: float,
    contracts: int,
    fee_per_contract: float,
    allow_no: bool,
    max_spread: float | None,
) -> tuple[pd.DataFrame, dict[str, int | float | bool]]:
    rows_available = len(scored_df)
    rows_scored = int(scored_df["model_prob_yes"].notna().sum())
    yes_quote_rows = int(scored_df["yes_ask"].notna().sum())
    no_quote_rows = int(scored_df["no_ask"].notna().sum())
    both_sides_quote_rows = int(
        scored_df["yes_ask"].notna()
        .mul(scored_df["yes_bid"].notna())
        .mul(scored_df["no_ask"].notna())
        .mul(scored_df["no_bid"].notna())
        .sum()
    )
    scored_summary = evaluate_scored_climatology(scored_df)
    yes_spread_mean = _mean_spread(scored_df["yes_bid"], scored_df["yes_ask"])
    no_spread_mean = _mean_spread(scored_df["no_bid"], scored_df["no_ask"])

    trade_rows: list[dict[str, Any]] = []
    yes_trades_taken = 0
    no_trades_taken = 0

    for row in scored_df.to_dict("records"):
        selection = select_executable_trade(
            row=row,
            min_edge=min_edge,
            min_samples=min_samples,
            min_price=min_price,
            max_price=max_price,
            allow_no=allow_no,
            max_spread=max_spread,
        )
        if selection is None:
            continue

        gross_pnl, net_pnl = _compute_trade_pnl(
            chosen_side=selection["chosen_side"],
            entry_price=float(selection["entry_price"]),
            resolved_yes=bool(row["resolved_yes"]),
            contracts=contracts,
            fee_per_contract=fee_per_contract,
        )
        if selection["chosen_side"] == "yes":
            yes_trades_taken += 1
        else:
            no_trades_taken += 1

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
                "fair_yes": float(row["fair_yes"]),
                "fair_no": float(row["fair_no"]),
                "yes_bid": _optional_float(row.get("yes_bid")),
                "yes_ask": _optional_float(row.get("yes_ask")),
                "no_bid": _optional_float(row.get("no_bid")),
                "no_ask": _optional_float(row.get("no_ask")),
                "chosen_side": selection["chosen_side"],
                "entry_price": float(selection["entry_price"]),
                "entry_price_source": selection["entry_price_source"],
                "pricing_mode": PRICING_MODE,
                "quote_source": QUOTE_SOURCE,
                "uses_true_quotes": False,
                "quote_spread": _optional_float(selection["quote_spread"]),
                "exec_edge_yes": _optional_float(selection["exec_edge_yes"]),
                "exec_edge_no": _optional_float(selection["exec_edge_no"]),
                "edge_at_entry": float(selection["edge_at_entry"]),
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
        yes_quote_rows=yes_quote_rows,
        no_quote_rows=no_quote_rows,
        both_sides_quote_rows=both_sides_quote_rows,
        yes_trades_taken=yes_trades_taken,
        no_trades_taken=no_trades_taken,
        scored_summary=scored_summary,
        yes_spread_mean=yes_spread_mean,
        no_spread_mean=no_spread_mean,
        allow_no=allow_no,
        min_edge=min_edge,
        min_samples=min_samples,
        min_price=min_price,
        max_price=max_price,
        contracts=contracts,
        fee_per_contract=fee_per_contract,
        max_spread=max_spread,
    )
    return trades_df, summary


def select_executable_trade(
    row: dict[str, Any],
    min_edge: float,
    min_samples: int,
    min_price: float,
    max_price: float,
    allow_no: bool,
    max_spread: float | None,
) -> dict[str, float | str | None] | None:
    if int(row["lookback_sample_size"]) < min_samples:
        return None

    yes_spread = spread(_optional_float(row.get("yes_bid")), _optional_float(row.get("yes_ask")))
    no_spread = spread(_optional_float(row.get("no_bid")), _optional_float(row.get("no_ask")))

    candidates: list[dict[str, float | str | None]] = []

    yes_entry = conservative_fill(
        ask=_normalize_cents_quote(row.get("yes_ask"), "yes_ask"),
        bid=_normalize_cents_quote(row.get("yes_bid"), "yes_bid"),
        side="yes",
    )
    if yes_entry is not None:
        exec_edge_yes = round(float(row["fair_yes"]) - (yes_entry / 100.0), 6)
        if _passes_execution_filters(
            entry_price=yes_entry,
            exec_edge=exec_edge_yes,
            quote_spread=yes_spread,
            min_edge=min_edge,
            min_price=min_price,
            max_price=max_price,
            max_spread=max_spread,
        ):
            candidates.append(
                {
                    "chosen_side": "yes",
                    "entry_price": yes_entry,
                    "entry_price_source": "yes_ask",
                    "quote_spread": yes_spread,
                    "exec_edge_yes": exec_edge_yes,
                    "exec_edge_no": None,
                    "edge_at_entry": exec_edge_yes,
                }
            )

    if allow_no:
        no_entry = conservative_fill(
            ask=_normalize_cents_quote(row.get("no_ask"), "no_ask"),
            bid=_normalize_cents_quote(row.get("no_bid"), "no_bid"),
            side="no",
        )
        if no_entry is not None:
            exec_edge_no = round(float(row["fair_no"]) - (no_entry / 100.0), 6)
            if _passes_execution_filters(
                entry_price=no_entry,
                exec_edge=exec_edge_no,
                quote_spread=no_spread,
                min_edge=min_edge,
                min_price=min_price,
                max_price=max_price,
                max_spread=max_spread,
            ):
                candidates.append(
                    {
                        "chosen_side": "no",
                        "entry_price": no_entry,
                        "entry_price_source": "no_ask",
                        "quote_spread": no_spread,
                        "exec_edge_yes": None,
                        "exec_edge_no": exec_edge_no,
                        "edge_at_entry": exec_edge_no,
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
    entry_price: float,
    resolved_yes: bool,
    contracts: int,
    fee_per_contract: float,
) -> tuple[float, float]:
    fill_price = entry_price / 100.0
    if chosen_side == "yes":
        resolved_value = 1.0 if resolved_yes else 0.0
    elif chosen_side == "no":
        resolved_value = 0.0 if resolved_yes else 1.0
    else:
        raise ClimatologyExecutableEvaluationError(f"Unsupported chosen_side {chosen_side!r}")

    gross_pnl = contract_pnl(fill_price=fill_price, resolved_value=resolved_value, contracts=contracts, fee_per_contract=0.0)
    net_pnl = contract_pnl(
        fill_price=fill_price,
        resolved_value=resolved_value,
        contracts=contracts,
        fee_per_contract=fee_per_contract,
    )
    return round(gross_pnl, 6), round(net_pnl, 6)


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
        "fair_yes",
        "fair_no",
        "yes_bid",
        "yes_ask",
        "no_bid",
        "no_ask",
        "chosen_side",
        "entry_price",
        "entry_price_source",
        "pricing_mode",
        "quote_source",
        "uses_true_quotes",
        "quote_spread",
        "exec_edge_yes",
        "exec_edge_no",
        "edge_at_entry",
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
    yes_quote_rows: int,
    no_quote_rows: int,
    both_sides_quote_rows: int,
    yes_trades_taken: int,
    no_trades_taken: int,
    scored_summary: dict[str, int | float],
    yes_spread_mean: float | None,
    no_spread_mean: float | None,
    allow_no: bool,
    min_edge: float,
    min_samples: int,
    min_price: float,
    max_price: float,
    contracts: int,
    fee_per_contract: float,
    max_spread: float | None,
) -> dict[str, int | float | bool]:
    if trades_df.empty:
        hit_rate = 0.0
        average_edge_at_entry = 0.0
        average_gross_pnl_per_trade = 0.0
        average_net_pnl_per_trade = 0.0
        total_gross_pnl = 0.0
        total_net_pnl = 0.0
    else:
        hits = trades_df.apply(
            lambda row: bool(row["resolved_yes"]) if row["chosen_side"] == "yes" else not bool(row["resolved_yes"]),
            axis=1,
        )
        hit_rate = round(float(hits.mean()), 6)
        average_edge_at_entry = round(float(trades_df["edge_at_entry"].mean()), 6)
        average_gross_pnl_per_trade = round(float(trades_df["gross_pnl"].mean()), 6)
        average_net_pnl_per_trade = round(float(trades_df["net_pnl"].mean()), 6)
        total_gross_pnl = round(float(trades_df["gross_pnl"].sum()), 6)
        total_net_pnl = round(float(trades_df["net_pnl"].sum()), 6)

    side_counts = _count_by_column(trades_df, "chosen_side")
    price_bucket_counts = _bucket_counts(
        trades_df["entry_price"] if "entry_price" in trades_df else pd.Series(dtype=float),
        bins=[0, 25, 50, 75, 100],
        labels=["0-25", "25-50", "50-75", "75-100"],
    )
    spread_bucket_counts = _bucket_counts(
        trades_df["quote_spread"] if "quote_spread" in trades_df else pd.Series(dtype=float),
        bins=[0, 2, 5, 10, 100],
        labels=["0-2", "2-5", "5-10", "10+"],
    )

    return {
        "pricing_mode": PRICING_MODE,
        "quote_source": QUOTE_SOURCE,
        "uses_true_quotes": False,
        "rows_available": rows_available,
        "rows_scored": rows_scored,
        "rows_with_executable_yes_quote": yes_quote_rows,
        "rows_with_executable_no_quote": no_quote_rows,
        "rows_with_both_sides_executable_quotes": both_sides_quote_rows,
        "rows_missing_executable_yes_quote": rows_available - yes_quote_rows,
        "rows_missing_executable_no_quote": rows_available - no_quote_rows,
        "yes_quote_coverage": _coverage_ratio(yes_quote_rows, rows_available),
        "no_quote_coverage": _coverage_ratio(no_quote_rows, rows_available),
        "both_sides_quote_coverage": _coverage_ratio(both_sides_quote_rows, rows_available),
        "trades_taken": int(len(trades_df)),
        "yes_trades_taken": yes_trades_taken or side_counts.get("yes", 0),
        "no_trades_taken": no_trades_taken or side_counts.get("no", 0),
        "hit_rate": hit_rate,
        "average_edge_at_entry": average_edge_at_entry,
        "average_gross_pnl_per_trade": average_gross_pnl_per_trade,
        "average_net_pnl_per_trade": average_net_pnl_per_trade,
        "total_gross_pnl": total_gross_pnl,
        "total_net_pnl": total_net_pnl,
        "brier_score": float(scored_summary["brier_score"]),
        "average_lookback_sample_size": float(scored_summary["average_lookback_sample_size"]),
        "average_yes_spread": yes_spread_mean,
        "average_no_spread": no_spread_mean,
        "entry_price_bucket_counts": price_bucket_counts,
        "spread_bucket_counts": spread_bucket_counts,
        "allow_no": allow_no,
        "min_edge": min_edge,
        "min_samples": min_samples,
        "min_price": min_price,
        "max_price": max_price,
        "contracts": contracts,
        "fee_per_contract": fee_per_contract,
        "max_spread": max_spread,
    }


def _passes_execution_filters(
    entry_price: float,
    exec_edge: float,
    quote_spread: float | None,
    min_edge: float,
    min_price: float,
    max_price: float,
    max_spread: float | None,
) -> bool:
    if not (min_price <= entry_price <= max_price):
        return False
    if exec_edge < min_edge:
        return False
    if max_spread is not None:
        if quote_spread is None or quote_spread > max_spread:
            return False
    return True


def _normalize_cents_quote(value: Any, field_name: str) -> float | None:
    if value is None or pd.isna(value):
        return None
    if not isinstance(value, (int, float)):
        raise ClimatologyExecutableEvaluationError(f"Expected numeric quote for {field_name}, got {value!r}")
    return float(value)


def _optional_float(value: Any) -> float | None:
    if value is None or pd.isna(value):
        return None
    return float(value)


def _coverage_ratio(numerator: int, denominator: int) -> float:
    if denominator <= 0:
        return 0.0
    return round(float(numerator) / float(denominator), 6)


def _mean_spread(bids: pd.Series, asks: pd.Series) -> float | None:
    spreads = [
        spread(_optional_float(bid), _optional_float(ask))
        for bid, ask in zip(bids.tolist(), asks.tolist(), strict=False)
    ]
    valid = [value for value in spreads if value is not None]
    if not valid:
        return None
    return round(float(sum(valid) / len(valid)), 6)


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


def _validate_parameters(
    min_edge: float,
    min_samples: int,
    min_price: float,
    max_price: float,
    contracts: int,
    fee_per_contract: float,
    max_spread: float | None,
) -> None:
    if min_edge < 0:
        raise ClimatologyExecutableEvaluationError(f"min_edge must be non-negative, got {min_edge}")
    if min_samples < 1:
        raise ClimatologyExecutableEvaluationError(f"min_samples must be at least 1, got {min_samples}")
    if contracts < 1:
        raise ClimatologyExecutableEvaluationError(f"contracts must be at least 1, got {contracts}")
    if fee_per_contract < 0:
        raise ClimatologyExecutableEvaluationError(f"fee_per_contract must be non-negative, got {fee_per_contract}")
    if min_price < 0 or max_price > 100 or min_price > max_price:
        raise ClimatologyExecutableEvaluationError(
            f"Price filter must satisfy 0 <= min_price <= max_price <= 100, got {min_price}, {max_price}"
        )
    if max_spread is not None and max_spread < 0:
        raise ClimatologyExecutableEvaluationError(f"max_spread must be non-negative when provided, got {max_spread}")


def _ensure_inputs_exist(paths: list[Path]) -> None:
    missing = [str(path) for path in paths if not path.exists()]
    if missing:
        raise ClimatologyExecutableEvaluationError(
            "Required local input parquet files are missing:\n" + "\n".join(missing)
        )


def _load_scored_frame(path: Path) -> pd.DataFrame:
    frame = pd.read_parquet(path)
    missing = sorted(REQUIRED_EXECUTABLE_COLUMNS - set(frame.columns))
    if missing:
        raise ClimatologyExecutableEvaluationError(
            f"Required executable quote columns are missing from {path}: {', '.join(missing)}"
        )
    return frame.copy()
