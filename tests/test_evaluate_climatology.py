import json
from pathlib import Path

import pandas as pd
import pytest
from typer.testing import CliRunner

import kwb.cli as cli_module
from kwb.cli import app
from kwb.backtest.evaluate_climatology import (
    ClimatologyEvaluationError,
    evaluate_climatology_strategy,
)


def test_trade_selection_respects_min_edge(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    scored_path = _write_placeholder(tmp_path)
    captured: dict[str, Path | pd.DataFrame | str] = {}
    frames = _base_scored_frames()

    monkeypatch.setattr(pd, "read_parquet", lambda path: frames[Path(path).name].copy())
    monkeypatch.setattr(
        pd.DataFrame,
        "to_parquet",
        lambda self, path, index=False: captured.update({"trades_path": Path(path), "trades_df": self.copy()}),
    )
    monkeypatch.setattr(
        Path,
        "write_text",
        lambda self, text, encoding="utf-8": captured.update({"summary_path": self, "summary_text": text}),
    )

    _, _, summary = evaluate_climatology_strategy(
        scored_dataset_path=scored_path,
        output_dir=tmp_path,
        min_edge=0.05,
    )

    assert summary["trades_taken"] == 1
    trades_df = captured["trades_df"]
    assert list(trades_df["market_ticker"]) == ["MKT-YES-1"]


def test_yes_trade_pnl_is_computed_correctly(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    scored_path = _write_placeholder(tmp_path)
    captured: dict[str, pd.DataFrame] = {}
    frames = {
        "backtest_scored_climatology.parquet": pd.DataFrame(
            [
                {
                    "city_key": "nyc",
                    "market_ticker": "MKT-YES-WIN",
                    "event_date": "2026-03-20",
                    "decision_ts": "2026-03-20T14:00:00+00:00",
                    "decision_price": 40.0,
                    "actual_tmax_f": 68.0,
                    "normal_tmax_f": 64.0,
                    "tmax_anomaly_f": 4.0,
                    "resolved_yes": True,
                    "model_prob_yes": 0.6,
                    "model_prob_no": 0.4,
                    "fair_yes": 0.6,
                    "fair_no": 0.4,
                    "edge_yes": 0.2,
                    "lookback_sample_size": 10,
                    "model_name": "baseline_climatology_v1",
                }
            ]
        )
    }

    monkeypatch.setattr(pd, "read_parquet", lambda path: frames[Path(path).name].copy())
    monkeypatch.setattr(
        pd.DataFrame,
        "to_parquet",
        lambda self, path, index=False: captured.setdefault("df", self.copy()),
    )
    monkeypatch.setattr(Path, "write_text", lambda self, text, encoding="utf-8": None)

    evaluate_climatology_strategy(
        scored_dataset_path=scored_path,
        output_dir=tmp_path,
        contracts=1,
        fee_per_contract=0.02,
    )

    df = captured["df"]
    assert df.loc[0, "chosen_side"] == "yes"
    assert df.loc[0, "gross_pnl"] == 0.6
    assert df.loc[0, "net_pnl"] == 0.58


def test_no_side_logic_is_correct_when_enabled(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    scored_path = _write_placeholder(tmp_path)
    captured: dict[str, pd.DataFrame] = {}
    frames = {
        "backtest_scored_climatology.parquet": pd.DataFrame(
            [
                {
                    "city_key": "nyc",
                    "market_ticker": "MKT-NO-WIN",
                    "event_date": "2026-03-20",
                    "decision_ts": "2026-03-20T14:00:00+00:00",
                    "decision_price": 70.0,
                    "actual_tmax_f": 62.0,
                    "normal_tmax_f": 64.0,
                    "tmax_anomaly_f": -2.0,
                    "resolved_yes": False,
                    "model_prob_yes": 0.2,
                    "model_prob_no": 0.8,
                    "fair_yes": 0.2,
                    "fair_no": 0.8,
                    "edge_yes": -0.5,
                    "lookback_sample_size": 10,
                    "model_name": "baseline_climatology_v1",
                }
            ]
        )
    }

    monkeypatch.setattr(pd, "read_parquet", lambda path: frames[Path(path).name].copy())
    monkeypatch.setattr(
        pd.DataFrame,
        "to_parquet",
        lambda self, path, index=False: captured.setdefault("df", self.copy()),
    )
    monkeypatch.setattr(Path, "write_text", lambda self, text, encoding="utf-8": None)

    evaluate_climatology_strategy(
        scored_dataset_path=scored_path,
        output_dir=tmp_path,
        min_edge=0.1,
        allow_no=True,
    )

    df = captured["df"]
    assert df.loc[0, "chosen_side"] == "no"
    assert df.loc[0, "entry_price"] == 30.0
    assert df.loc[0, "gross_pnl"] == 0.7


def test_insufficient_lookback_rows_are_excluded(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    scored_path = _write_placeholder(tmp_path)
    captured: dict[str, pd.DataFrame] = {}
    frames = {
        "backtest_scored_climatology.parquet": pd.DataFrame(
            [
                {
                    "city_key": "nyc",
                    "market_ticker": "MKT-LOW-SAMPLE",
                    "event_date": "2026-03-20",
                    "decision_ts": "2026-03-20T14:00:00+00:00",
                    "decision_price": 40.0,
                    "actual_tmax_f": 68.0,
                    "normal_tmax_f": 64.0,
                    "tmax_anomaly_f": 4.0,
                    "resolved_yes": True,
                    "model_prob_yes": 0.6,
                    "model_prob_no": 0.4,
                    "fair_yes": 0.6,
                    "fair_no": 0.4,
                    "edge_yes": 0.2,
                    "lookback_sample_size": 1,
                    "model_name": "baseline_climatology_v1",
                }
            ]
        )
    }

    monkeypatch.setattr(pd, "read_parquet", lambda path: frames[Path(path).name].copy())
    monkeypatch.setattr(
        pd.DataFrame,
        "to_parquet",
        lambda self, path, index=False: captured.setdefault("df", self.copy()),
    )
    monkeypatch.setattr(Path, "write_text", lambda self, text, encoding="utf-8": None)

    _, _, summary = evaluate_climatology_strategy(
        scored_dataset_path=scored_path,
        output_dir=tmp_path,
        min_samples=2,
    )

    assert summary["trades_taken"] == 0
    assert captured["df"].empty


def test_trade_output_schema_contains_required_columns(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    scored_path = _write_placeholder(tmp_path)
    captured: dict[str, pd.DataFrame] = {}
    frames = _base_scored_frames()

    monkeypatch.setattr(pd, "read_parquet", lambda path: frames[Path(path).name].copy())
    monkeypatch.setattr(
        pd.DataFrame,
        "to_parquet",
        lambda self, path, index=False: captured.setdefault("df", self.copy()),
    )
    monkeypatch.setattr(Path, "write_text", lambda self, text, encoding="utf-8": None)

    evaluate_climatology_strategy(
        scored_dataset_path=scored_path,
        output_dir=tmp_path,
    )

    df = captured["df"]
    assert list(df.columns) == [
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


def test_climatology_evaluation_cli_smoke(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    def fake_evaluate(**kwargs):
        assert kwargs["min_edge"] == 0.0
        assert kwargs["allow_no"] is False
        return (
            tmp_path / "backtest_trades_climatology.parquet",
            tmp_path / "backtest_summary_climatology.json",
            {
                "trades_taken": 3,
                "hit_rate": 0.666667,
                "average_pnl_per_trade": 0.12,
                "total_net_pnl": 0.36,
            },
        )

    monkeypatch.setattr(cli_module, "evaluate_climatology_strategy", fake_evaluate)

    runner = CliRunner()
    result = runner.invoke(app, ["backtest", "evaluate-climatology"])

    assert result.exit_code == 0
    assert "Saved climatology evaluation" in result.stdout
    assert "trades: 3" in result.stdout
    assert "total net pnl: 0.36" in result.stdout


def test_climatology_evaluation_missing_input_fails_clearly(tmp_path: Path) -> None:
    with pytest.raises(ClimatologyEvaluationError, match="Required local input parquet files are missing"):
        evaluate_climatology_strategy(
            scored_dataset_path=tmp_path / "missing_scored.parquet",
            output_dir=tmp_path,
        )


def test_summary_json_is_written(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    scored_path = _write_placeholder(tmp_path)
    captured: dict[str, str] = {}
    frames = _base_scored_frames()

    monkeypatch.setattr(pd, "read_parquet", lambda path: frames[Path(path).name].copy())
    monkeypatch.setattr(pd.DataFrame, "to_parquet", lambda self, path, index=False: None)

    def fake_write_text(self: Path, text: str, encoding: str = "utf-8") -> int:
        captured["text"] = text
        return len(text)

    monkeypatch.setattr(Path, "write_text", fake_write_text)

    evaluate_climatology_strategy(
        scored_dataset_path=scored_path,
        output_dir=tmp_path,
    )

    payload = json.loads(captured["text"])
    assert payload["trades_taken"] == 2
    assert payload["pricing_mode"] == "decision_price"
    assert payload["quote_source"] == "decision_price_close"
    assert payload["uses_true_quotes"] is False
    assert payload["yes_trades_taken"] == 2
    assert "brier_score" in payload


def _base_scored_frames() -> dict[str, pd.DataFrame]:
    return {
        "backtest_scored_climatology.parquet": pd.DataFrame(
            [
                {
                    "city_key": "nyc",
                    "market_ticker": "MKT-YES-1",
                    "event_date": "2026-03-20",
                    "decision_ts": "2026-03-20T14:00:00+00:00",
                    "decision_price": 40.0,
                    "actual_tmax_f": 68.0,
                    "normal_tmax_f": 64.0,
                    "tmax_anomaly_f": 4.0,
                    "resolved_yes": True,
                    "model_prob_yes": 0.5,
                    "model_prob_no": 0.5,
                    "fair_yes": 0.5,
                    "fair_no": 0.5,
                    "edge_yes": 0.1,
                    "lookback_sample_size": 8,
                    "model_name": "baseline_climatology_v1",
                },
                {
                    "city_key": "nyc",
                    "market_ticker": "MKT-YES-2",
                    "event_date": "2026-03-21",
                    "decision_ts": "2026-03-21T14:00:00+00:00",
                    "decision_price": 55.0,
                    "actual_tmax_f": 71.0,
                    "normal_tmax_f": 64.0,
                    "tmax_anomaly_f": 7.0,
                    "resolved_yes": False,
                    "model_prob_yes": 0.57,
                    "model_prob_no": 0.43,
                    "fair_yes": 0.57,
                    "fair_no": 0.43,
                    "edge_yes": 0.02,
                    "lookback_sample_size": 9,
                    "model_name": "baseline_climatology_v1",
                },
            ]
        )
    }


def _write_placeholder(tmp_path: Path) -> Path:
    scored_path = tmp_path / "backtest_scored_climatology.parquet"
    scored_path.write_text("placeholder", encoding="utf-8")
    return scored_path
