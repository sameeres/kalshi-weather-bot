from pathlib import Path

import pandas as pd
from typer.testing import CliRunner

from kwb.cli import app
from kwb.research.time_of_day_sensitivity import (
    _apply_validated_gate,
    _baseline_comparison,
    _rank_hours,
)


def test_apply_validated_gate_keeps_only_validated_slice() -> None:
    df = pd.DataFrame(
        [
            {
                "city_key": "nyc",
                "market_ticker": "A",
                "contract_type": "or_below",
                "chosen_side": "yes",
                "entry_price_bucket": "0-25",
                "net_pnl": 0.5,
                "fold_number": 1,
                "event_date": "2026-01-01",
                "entry_price": 20.0,
                "quote_spread": 1.0,
                "edge_at_entry": 0.2,
            },
            {
                "city_key": "nyc",
                "market_ticker": "B",
                "contract_type": "or_above",
                "chosen_side": "yes",
                "entry_price_bucket": "0-25",
                "net_pnl": 0.4,
                "fold_number": 1,
                "event_date": "2026-01-01",
                "entry_price": 18.0,
                "quote_spread": 1.0,
                "edge_at_entry": 0.2,
            },
            {
                "city_key": "chicago",
                "market_ticker": "C",
                "contract_type": "or_below",
                "chosen_side": "no",
                "entry_price_bucket": "0-25",
                "net_pnl": 0.3,
                "fold_number": 2,
                "event_date": "2026-01-02",
                "entry_price": 17.0,
                "quote_spread": 1.0,
                "edge_at_entry": 0.2,
            },
        ]
    )

    gated = _apply_validated_gate(df)

    assert list(gated["market_ticker"]) == ["A"]


def test_rank_hours_prefers_more_credible_hour_not_just_any_row() -> None:
    summary = pd.DataFrame(
        [
            {"decision_time_local": "10:00", "scenario_code": "B", "city_scope": "pooled", "total_net_pnl": 1.0, "trades_taken": 10, "positive_fold_count": 2, "all_folds_positive": False},
            {"decision_time_local": "10:00", "scenario_code": "D", "city_scope": "pooled", "total_net_pnl": 0.9, "trades_taken": 9, "positive_fold_count": 2, "all_folds_positive": False},
            {"decision_time_local": "10:00", "scenario_code": "D", "city_scope": "nyc", "total_net_pnl": 0.4, "trades_taken": 5, "positive_fold_count": 2, "all_folds_positive": False},
            {"decision_time_local": "10:00", "scenario_code": "D", "city_scope": "chicago", "total_net_pnl": 0.5, "trades_taken": 4, "positive_fold_count": 2, "all_folds_positive": False},
            {"decision_time_local": "11:00", "scenario_code": "B", "city_scope": "pooled", "total_net_pnl": 0.8, "trades_taken": 11, "positive_fold_count": 1, "all_folds_positive": False},
            {"decision_time_local": "11:00", "scenario_code": "D", "city_scope": "pooled", "total_net_pnl": 1.2, "trades_taken": 10, "positive_fold_count": 3, "all_folds_positive": True},
            {"decision_time_local": "11:00", "scenario_code": "D", "city_scope": "nyc", "total_net_pnl": 0.6, "trades_taken": 5, "positive_fold_count": 3, "all_folds_positive": True},
            {"decision_time_local": "11:00", "scenario_code": "D", "city_scope": "chicago", "total_net_pnl": 0.6, "trades_taken": 5, "positive_fold_count": 3, "all_folds_positive": True},
        ]
    )

    ranked = _rank_hours(summary)

    assert ranked.iloc[0]["decision_time_local"] == "11:00"
    comparison = _baseline_comparison(summary)
    row = comparison.loc[
        (comparison["decision_time_local"] == "11:00")
        & (comparison["scenario_code"] == "D")
        & (comparison["city_scope"] == "pooled")
    ].iloc[0]
    assert row["delta_vs_1000_pnl"] == 0.3


def test_time_of_day_sensitivity_cli_smoke(tmp_path: Path, monkeypatch) -> None:
    runner = CliRunner()

    def fake_run(**kwargs):
        json_path = tmp_path / "time_of_day_sensitivity.json"
        csv_path = tmp_path / "time_of_day_sensitivity_summary.csv"
        fold_path = tmp_path / "time_of_day_sensitivity_fold_metrics.csv"
        report_path = tmp_path / "time_of_day_sensitivity_20260401.md"
        json_path.write_text("{}", encoding="utf-8")
        pd.DataFrame([{"decision_time_local": "10:00"}]).to_csv(csv_path, index=False)
        pd.DataFrame([{"decision_time_local": "10:00", "fold_number": 1}]).to_csv(fold_path, index=False)
        report_path.write_text("# report\n", encoding="utf-8")
        return json_path, csv_path, fold_path, report_path, {"times_tested": ["10:00", "11:00"]}

    monkeypatch.setattr("kwb.cli.run_time_of_day_sensitivity_study", fake_run)

    result = runner.invoke(app, ["research", "time-of-day-sensitivity-climatology"])

    assert result.exit_code == 0
    assert "Saved time-of-day sensitivity study" in result.output
