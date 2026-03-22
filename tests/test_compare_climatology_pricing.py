import json
from pathlib import Path

import pandas as pd
import pytest
from typer.testing import CliRunner

import kwb.cli as cli_module
from kwb.backtest.compare_climatology_pricing import compare_climatology_pricing_modes
from kwb.cli import app


def test_compare_climatology_pricing_writes_mode_comparison_outputs(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    scored_path = tmp_path / "backtest_scored_climatology.parquet"
    scored_path.write_text("placeholder", encoding="utf-8")
    captured: dict[str, str | pd.DataFrame] = {}

    def fake_simple(**kwargs):
        assert kwargs["scored_dataset_path"] == scored_path
        return (
            tmp_path / "backtest_trades_climatology.parquet",
            tmp_path / "backtest_summary_climatology.json",
            {
                "pricing_mode": "decision_price",
                "quote_source": "decision_price_close",
                "uses_true_quotes": False,
                "rows_available": 10,
                "rows_scored": 8,
                "trades_taken": 4,
                "yes_trades_taken": 4,
                "no_trades_taken": 0,
                "hit_rate": 0.5,
                "average_edge_at_entry": 0.04,
                "average_pnl_per_trade": 0.01,
                "total_gross_pnl": 0.12,
                "total_net_pnl": 0.04,
                "brier_score": 0.21,
            },
        )

    def fake_exec(**kwargs):
        assert kwargs["scored_dataset_path"] == scored_path
        return (
            tmp_path / "backtest_trades_climatology_executable.parquet",
            tmp_path / "backtest_summary_climatology_executable.json",
            {
                "pricing_mode": "candle_proxy",
                "quote_source": "decision_candle_ohlc_bounds",
                "uses_true_quotes": False,
                "rows_available": 10,
                "rows_scored": 8,
                "rows_with_executable_yes_quote": 7,
                "rows_with_executable_no_quote": 7,
                "yes_quote_coverage": 0.7,
                "no_quote_coverage": 0.7,
                "trades_taken": 2,
                "yes_trades_taken": 2,
                "no_trades_taken": 0,
                "hit_rate": 0.5,
                "average_edge_at_entry": 0.02,
                "average_gross_pnl_per_trade": 0.0,
                "average_net_pnl_per_trade": -0.01,
                "total_gross_pnl": 0.0,
                "total_net_pnl": -0.02,
                "brier_score": 0.21,
                "average_yes_spread": 4.0,
                "average_no_spread": 4.0,
            },
        )

    def fake_write_text(self: Path, text: str, encoding: str = "utf-8") -> int:
        captured["json"] = text
        return len(text)

    monkeypatch.setattr(
        "kwb.backtest.compare_climatology_pricing.evaluate_climatology_strategy",
        fake_simple,
    )
    monkeypatch.setattr(
        "kwb.backtest.compare_climatology_pricing.evaluate_climatology_executable_strategy",
        fake_exec,
    )
    monkeypatch.setattr(Path, "write_text", fake_write_text)
    monkeypatch.setattr(pd.DataFrame, "to_csv", lambda self, path, index=False: captured.setdefault("csv", self.copy()))

    json_path, csv_path, comparison = compare_climatology_pricing_modes(
        scored_dataset_path=scored_path,
        output_dir=tmp_path,
    )

    assert json_path.name == "backtest_comparison_climatology_pricing.json"
    assert csv_path.name == "backtest_comparison_climatology_pricing.csv"
    assert comparison["quote_audit"]["has_true_historical_best_bid_ask"] is False
    assert comparison["delta_executable_minus_decision_price"]["trades_taken_delta"] == -2.0
    csv_df = captured["csv"]
    assert list(csv_df["pricing_mode"]) == ["decision_price", "candle_proxy"]
    payload = json.loads(captured["json"])
    assert payload["modes"][0]["pricing_mode"] == "decision_price"
    assert payload["modes"][1]["pricing_mode"] == "candle_proxy"


def test_compare_climatology_pricing_cli_smoke(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    def fake_compare(**kwargs):
        return (
            tmp_path / "backtest_comparison_climatology_pricing.json",
            tmp_path / "backtest_comparison_climatology_pricing.csv",
            {
                "modes": [{}, {}],
                "delta_executable_minus_decision_price": {
                    "trades_taken_delta": -1.0,
                    "total_net_pnl_delta": -0.15,
                },
            },
        )

    monkeypatch.setattr(cli_module, "compare_climatology_pricing_modes", fake_compare)

    runner = CliRunner()
    result = runner.invoke(app, ["backtest", "compare-climatology-pricing"])

    assert result.exit_code == 0
    assert "Saved pricing comparison" in result.stdout
    assert "trade delta exec-vs-decision: -1.0" in result.stdout
