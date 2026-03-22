import json
from pathlib import Path

import pandas as pd
import pytest
from typer.testing import CliRunner

import kwb.cli as cli_module
from kwb.cli import app
from kwb.research.run_climatology_baseline import (
    ClimatologyResearchRunError,
    run_climatology_baseline_research,
)


def test_research_runner_executes_expected_stages_and_writes_bundle(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    calls: list[str] = []
    scored_df = _scored_frame()

    def fake_build(**kwargs):
        calls.append("build")
        path = tmp_path / "backtest_dataset.parquet"
        path.write_text("placeholder", encoding="utf-8")
        return path, {"rows_written": 3}

    def fake_score(**kwargs):
        calls.append("score")
        path = tmp_path / "backtest_scored_climatology.parquet"
        path.write_text("placeholder", encoding="utf-8")
        return path, {"rows_scored": 3, "average_lookback_sample_size": 8.0, "brier_score": 0.2, "average_edge_yes": 0.03}

    def fake_simple(**kwargs):
        calls.append("simple")
        trades = tmp_path / "backtest_trades_climatology.parquet"
        summary = tmp_path / "backtest_summary_climatology.json"
        trades.write_text("placeholder", encoding="utf-8")
        summary.write_text("{}", encoding="utf-8")
        return trades, summary, {
            "pricing_mode": "decision_price",
            "trades_taken": 2,
            "hit_rate": 0.5,
            "average_edge_at_entry": 0.04,
            "total_net_pnl": 0.1,
        }

    def fake_exec(**kwargs):
        calls.append("exec")
        trades = tmp_path / "backtest_trades_climatology_executable.parquet"
        summary = tmp_path / "backtest_summary_climatology_executable.json"
        trades.write_text("placeholder", encoding="utf-8")
        summary.write_text("{}", encoding="utf-8")
        return trades, summary, {
            "pricing_mode": "candle_proxy",
            "trades_taken": 1,
            "hit_rate": 1.0,
            "average_edge_at_entry": 0.03,
            "total_net_pnl": 0.05,
            "yes_quote_coverage": 1.0,
        }

    def fake_compare(**kwargs):
        calls.append("compare")
        json_path = tmp_path / "backtest_comparison_climatology_pricing.json"
        csv_path = tmp_path / "backtest_comparison_climatology_pricing.csv"
        json_path.write_text("{}", encoding="utf-8")
        pd.DataFrame([{"pricing_mode": "decision_price"}, {"pricing_mode": "candle_proxy"}]).to_csv(csv_path, index=False)
        return json_path, csv_path, {
            "modes": [{"pricing_mode": "decision_price"}, {"pricing_mode": "candle_proxy"}],
            "delta_executable_minus_decision_price": {"total_net_pnl_delta": -0.05},
        }

    def fake_walkforward(**kwargs):
        calls.append("walkforward")
        results_path = tmp_path / "walkforward_results_climatology.csv"
        summary_path = tmp_path / "walkforward_summary_climatology.json"
        diagnostics_path = tmp_path / "walkforward_diagnostics_climatology.csv"
        pd.DataFrame(
            [
                {
                    "fold_number": 1,
                    "pricing_mode": "decision_price",
                    "selected_min_edge": 0.02,
                    "selected_min_samples": 5,
                    "selected_allow_no": False,
                    "selected_max_spread": None,
                },
                {
                    "fold_number": 1,
                    "pricing_mode": "candle_proxy",
                    "selected_min_edge": 0.05,
                    "selected_min_samples": 5,
                    "selected_allow_no": False,
                    "selected_max_spread": 5.0,
                },
            ]
        ).to_csv(results_path, index=False)
        pd.DataFrame(
            [
                {
                    "pricing_mode": "candle_proxy",
                    "subset": "selected_trades",
                    "breakdown": "city_key",
                    "bucket": "nyc",
                    "trades_taken": 1,
                    "yes_trades_taken": 1,
                    "no_trades_taken": 0,
                    "hit_rate": 1.0,
                    "average_edge_at_entry": 0.03,
                    "average_net_pnl_per_trade": 0.05,
                    "total_net_pnl": 0.05,
                }
            ]
        ).to_csv(diagnostics_path, index=False)
        summary = {
            "pricing_mode": "both",
            "fold_count": 1,
            "results_by_pricing_mode": {
                "decision_price": {
                    "folds_scored": 1,
                    "trades_taken": 2,
                    "hit_rate": 0.5,
                    "average_net_pnl_per_trade": 0.05,
                    "total_net_pnl": 0.1,
                    "selected_thresholds_per_fold": [
                        {
                            "fold_number": 1,
                            "selected_min_edge": 0.02,
                            "selected_min_samples": 5,
                            "selected_allow_no": False,
                            "selected_max_spread": None,
                        }
                    ],
                },
                "candle_proxy": {
                    "folds_scored": 1,
                    "trades_taken": 1,
                    "hit_rate": 1.0,
                    "average_net_pnl_per_trade": 0.05,
                    "total_net_pnl": 0.05,
                    "selected_thresholds_per_fold": [
                        {
                            "fold_number": 1,
                            "selected_min_edge": 0.05,
                            "selected_min_samples": 5,
                            "selected_allow_no": False,
                            "selected_max_spread": 5.0,
                        }
                    ],
                },
            },
        }
        summary_path.write_text(json.dumps(summary), encoding="utf-8")
        return results_path, summary_path, diagnostics_path, summary

    monkeypatch.setattr("kwb.research.run_climatology_baseline.build_backtest_dataset", fake_build)
    monkeypatch.setattr("kwb.research.run_climatology_baseline.score_climatology_baseline", fake_score)
    monkeypatch.setattr("kwb.research.run_climatology_baseline.evaluate_climatology_strategy", fake_simple)
    monkeypatch.setattr("kwb.research.run_climatology_baseline.evaluate_climatology_executable_strategy", fake_exec)
    monkeypatch.setattr("kwb.research.run_climatology_baseline.compare_climatology_pricing_modes", fake_compare)
    monkeypatch.setattr("kwb.research.run_climatology_baseline.run_walkforward_climatology", fake_walkforward)
    monkeypatch.setattr(pd, "read_parquet", lambda path: scored_df.copy())

    run_dir, manifest_path, report_json_path, report_markdown_path, manifest = run_climatology_baseline_research(
        output_dir=tmp_path,
        overwrite=True,
        pricing_mode="both",
    )

    assert run_dir == tmp_path
    assert calls == ["build", "score", "simple", "exec", "compare", "walkforward"]
    assert manifest_path.exists()
    assert report_json_path.exists()
    assert report_markdown_path.exists()
    assert manifest["output_paths"]["threshold_stability"].endswith("threshold_stability_climatology.json")

    payload = json.loads(report_json_path.read_text(encoding="utf-8"))
    assert payload["one_shot_evaluation"]["decision_price"]["pricing_mode"] == "decision_price"
    assert payload["walkforward_evaluation"]["threshold_stability"]["results_by_pricing_mode"]["candle_proxy"][
        "selected_max_spread_frequency"
    ] == {"5.0": 1}
    assert payload["baseline_status"] in {"inconclusive", "promising"}


def test_research_runner_records_skipped_walkforward_state(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    scored_df = _scored_frame()

    monkeypatch.setattr(
        "kwb.research.run_climatology_baseline.build_backtest_dataset",
        lambda **kwargs: (tmp_path / "backtest_dataset.parquet", {"rows_written": 3}),
    )
    monkeypatch.setattr(
        "kwb.research.run_climatology_baseline.score_climatology_baseline",
        lambda **kwargs: (tmp_path / "backtest_scored_climatology.parquet", {"rows_scored": 3}),
    )
    monkeypatch.setattr(
        "kwb.research.run_climatology_baseline.evaluate_climatology_strategy",
        lambda **kwargs: (
            tmp_path / "backtest_trades_climatology.parquet",
            tmp_path / "backtest_summary_climatology.json",
            {"pricing_mode": "decision_price", "trades_taken": 1, "hit_rate": 1.0, "average_edge_at_entry": 0.04, "total_net_pnl": 0.1},
        ),
    )
    monkeypatch.setattr(
        "kwb.research.run_climatology_baseline.evaluate_climatology_executable_strategy",
        lambda **kwargs: (
            tmp_path / "backtest_trades_climatology_executable.parquet",
            tmp_path / "backtest_summary_climatology_executable.json",
            {"pricing_mode": "candle_proxy", "trades_taken": 1, "hit_rate": 1.0, "average_edge_at_entry": 0.03, "total_net_pnl": 0.05, "yes_quote_coverage": 1.0},
        ),
    )
    monkeypatch.setattr(
        "kwb.research.run_climatology_baseline.compare_climatology_pricing_modes",
        lambda **kwargs: (
            tmp_path / "backtest_comparison_climatology_pricing.json",
            tmp_path / "backtest_comparison_climatology_pricing.csv",
            {"modes": [{}, {}]},
        ),
    )

    def fake_walkforward(**kwargs):
        raise cli_module.WalkforwardClimatologyError("No valid walk-forward folds could be created.")

    monkeypatch.setattr("kwb.research.run_climatology_baseline.run_walkforward_climatology", fake_walkforward)
    monkeypatch.setattr(pd, "read_parquet", lambda path: scored_df.copy())
    (tmp_path / "backtest_dataset.parquet").write_text("placeholder", encoding="utf-8")
    (tmp_path / "backtest_scored_climatology.parquet").write_text("placeholder", encoding="utf-8")

    _, manifest_path, report_json_path, _, manifest = run_climatology_baseline_research(
        output_dir=tmp_path,
        overwrite=True,
        pricing_mode="both",
    )

    assert any(step["step"] == "walkforward_climatology" and step["status"] == "skipped" for step in manifest["steps"])
    report = json.loads(report_json_path.read_text(encoding="utf-8"))
    assert report["walkforward_evaluation"]["threshold_stability"]["available"] is False
    assert report["baseline_status"] == "weak"
    manifest_payload = json.loads(manifest_path.read_text(encoding="utf-8"))
    assert manifest_payload["skipped_steps"][0]["step"] == "walkforward_climatology"


def test_research_runner_cli_smoke(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    def fake_run(**kwargs):
        return (
            tmp_path,
            tmp_path / "research_manifest_climatology.json",
            tmp_path / "baseline_report_climatology.json",
            tmp_path / "baseline_report_climatology.md",
            {"skipped_steps": []},
        )

    monkeypatch.setattr(cli_module, "run_climatology_baseline_research", fake_run)

    runner = CliRunner()
    result = runner.invoke(app, ["research", "run-climatology-baseline"])

    assert result.exit_code == 0
    assert "Saved climatology baseline research bundle" in result.stdout


def _scored_frame() -> pd.DataFrame:
    return pd.DataFrame(
        [
            {
                "city_key": "nyc",
                "market_ticker": "MKT-1",
                "event_date": "2026-01-01",
                "decision_ts": "2026-01-01T15:00:00+00:00",
                "decision_price": 40.0,
                "yes_bid": 39.0,
                "yes_ask": 41.0,
                "no_bid": 59.0,
                "no_ask": 61.0,
                "actual_tmax_f": 70.0,
                "normal_tmax_f": 64.0,
                "tmax_anomaly_f": 6.0,
                "resolved_yes": True,
                "model_prob_yes": 0.45,
                "model_prob_no": 0.55,
                "fair_yes": 0.45,
                "fair_no": 0.55,
                "edge_yes": 0.05,
                "lookback_sample_size": 8,
                "model_name": "baseline_climatology_v1",
            },
            {
                "city_key": "chi",
                "market_ticker": "MKT-2",
                "event_date": "2026-01-02",
                "decision_ts": "2026-01-02T15:00:00+00:00",
                "decision_price": 50.0,
                "yes_bid": 48.0,
                "yes_ask": 52.0,
                "no_bid": 48.0,
                "no_ask": 52.0,
                "actual_tmax_f": 55.0,
                "normal_tmax_f": 50.0,
                "tmax_anomaly_f": 5.0,
                "resolved_yes": False,
                "model_prob_yes": 0.48,
                "model_prob_no": 0.52,
                "fair_yes": 0.48,
                "fair_no": 0.52,
                "edge_yes": -0.02,
                "lookback_sample_size": 9,
                "model_name": "baseline_climatology_v1",
            },
        ]
    )
