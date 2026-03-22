from __future__ import annotations

import json
from pathlib import Path
from typing import Any

import pandas as pd

from kwb.backtest.evaluate_climatology import evaluate_climatology_strategy
from kwb.backtest.evaluate_climatology_executable import evaluate_climatology_executable_strategy
from kwb.ingestion.kalshi_market_history import describe_local_quote_history_capabilities
from kwb.models.baseline_climatology import DEFAULT_SCORED_FILENAME
from kwb.settings import MARTS_DIR
from kwb.utils.logging import get_logger

logger = get_logger(__name__)

DEFAULT_COMPARISON_JSON_FILENAME = "backtest_comparison_climatology_pricing.json"
DEFAULT_COMPARISON_CSV_FILENAME = "backtest_comparison_climatology_pricing.csv"

COMPARISON_METRICS = [
    "rows_available",
    "rows_scored",
    "rows_with_executable_yes_quote",
    "rows_with_executable_no_quote",
    "yes_quote_coverage",
    "no_quote_coverage",
    "trades_taken",
    "yes_trades_taken",
    "no_trades_taken",
    "hit_rate",
    "average_edge_at_entry",
    "average_pnl_per_trade",
    "average_gross_pnl_per_trade",
    "average_net_pnl_per_trade",
    "total_gross_pnl",
    "total_net_pnl",
    "brier_score",
    "average_yes_spread",
    "average_no_spread",
]


class ClimatologyPricingComparisonError(ValueError):
    """Raised when pricing-mode comparison cannot be completed safely."""


def compare_climatology_pricing_modes(
    scored_dataset_path: Path | None = None,
    output_dir: Path | None = None,
    min_edge: float = 0.0,
    min_samples: int = 1,
    min_price: float = 0.0,
    max_price: float = 100.0,
    contracts: int = 1,
    fee_per_contract: float = 0.0,
    allow_no: bool = False,
    max_spread: float | None = None,
) -> tuple[Path, Path, dict[str, Any]]:
    """Compare baseline results across locally supported pricing assumptions."""
    scored_dataset_path = scored_dataset_path or (MARTS_DIR / DEFAULT_SCORED_FILENAME)
    output_dir = output_dir or MARTS_DIR
    output_dir.mkdir(parents=True, exist_ok=True)

    simple_summary = evaluate_climatology_strategy(
        scored_dataset_path=scored_dataset_path,
        output_dir=output_dir,
        min_edge=min_edge,
        min_samples=min_samples,
        min_price=min_price,
        max_price=max_price,
        contracts=contracts,
        fee_per_contract=fee_per_contract,
        allow_no=allow_no,
    )[2]

    executable_summary = evaluate_climatology_executable_strategy(
        scored_dataset_path=scored_dataset_path,
        output_path=output_dir / "backtest_trades_climatology_executable.parquet",
        summary_output_path=output_dir / "backtest_summary_climatology_executable.json",
        min_edge=min_edge,
        min_samples=min_samples,
        min_price=min_price,
        max_price=max_price,
        contracts=contracts,
        fee_per_contract=fee_per_contract,
        allow_no=allow_no,
        max_spread=max_spread,
    )[2]

    mode_rows = [_flatten_summary(simple_summary), _flatten_summary(executable_summary)]
    comparison = {
        "quote_audit": describe_local_quote_history_capabilities(),
        "modes": [simple_summary, executable_summary],
        "delta_executable_minus_decision_price": _compute_metric_deltas(
            decision_price_summary=simple_summary,
            executable_summary=executable_summary,
        ),
    }

    json_path = output_dir / DEFAULT_COMPARISON_JSON_FILENAME
    csv_path = output_dir / DEFAULT_COMPARISON_CSV_FILENAME
    json_path.write_text(json.dumps(comparison, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    pd.DataFrame(mode_rows).to_csv(csv_path, index=False)

    logger.info("Saved climatology pricing comparison to %s and %s", json_path, csv_path)
    return json_path, csv_path, comparison


def _compute_metric_deltas(
    decision_price_summary: dict[str, Any],
    executable_summary: dict[str, Any],
) -> dict[str, float]:
    deltas: dict[str, float] = {}
    for metric in COMPARISON_METRICS:
        left = decision_price_summary.get(metric)
        right = executable_summary.get(metric)
        if isinstance(left, (int, float)) and isinstance(right, (int, float)):
            deltas[f"{metric}_delta"] = round(float(right) - float(left), 6)
    return deltas


def _flatten_summary(summary: dict[str, Any]) -> dict[str, Any]:
    flattened: dict[str, Any] = {}
    for key, value in summary.items():
        if isinstance(value, (dict, list)):
            flattened[key] = json.dumps(value, sort_keys=True)
        else:
            flattened[key] = value
    return flattened
