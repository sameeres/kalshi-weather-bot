import json
from pathlib import Path

import pandas as pd
import pytest
from typer.testing import CliRunner

import kwb.cli as cli_module
from kwb.backtest.walkforward_climatology import (
    WalkforwardClimatologyError,
    _build_temporal_folds,
    run_walkforward_climatology,
)
from kwb.cli import app


def test_temporal_folds_are_strictly_ordered_with_no_leakage() -> None:
    df = _scored_frame_with_quotes(days=8)
    df["event_date"] = pd.to_datetime(df["event_date"])

    folds = _build_temporal_folds(
        scored_df=df,
        train_window=3,
        validation_window=2,
        test_window=1,
        step_window=1,
        expanding=True,
    )

    assert len(folds) == 3
    first = folds[0]
    assert max(first["train_df"]["event_date"]) < min(first["validation_df"]["event_date"])
    assert max(first["validation_df"]["event_date"]) < min(first["test_df"]["event_date"])


def test_threshold_selection_uses_validation_only(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    scored_path = _write_scored_pickle(tmp_path, _validation_selection_frame())
    monkeypatch.setattr(pd, "read_parquet", lambda path: pd.read_pickle(path))

    results_path, summary_path, diagnostics_path, summary = run_walkforward_climatology(
        scored_dataset_path=scored_path,
        output_dir=tmp_path,
        pricing_mode="decision_price",
        train_window=2,
        validation_window=2,
        test_window=1,
        step_window=1,
        min_trades_for_selection=1,
        min_edge_grid=(0.0, 0.05),
        min_samples_grid=(1,),
        min_price_grid=(0.0,),
        max_price_grid=(100.0,),
        allow_no_grid=(False,),
    )

    assert results_path.exists()
    assert summary_path.exists()
    assert diagnostics_path.exists()
    results_df = pd.read_csv(results_path)
    assert results_df.loc[0, "selected_min_edge"] == 0.05
    assert results_df.loc[0, "validation_trades"] == 1
    assert results_df.loc[0, "test_trades"] == 0
    assert summary["results_by_pricing_mode"]["decision_price"]["trades_taken"] == 0


def test_both_pricing_modes_produce_fold_results_and_diagnostics(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    scored_path = _write_scored_pickle(tmp_path, _scored_frame_with_quotes(days=8))
    monkeypatch.setattr(pd, "read_parquet", lambda path: pd.read_pickle(path))

    results_path, summary_path, diagnostics_path, summary = run_walkforward_climatology(
        scored_dataset_path=scored_path,
        output_dir=tmp_path,
        pricing_mode="both",
        train_window=3,
        validation_window=2,
        test_window=1,
        step_window=1,
        min_trades_for_selection=0,
        min_edge_grid=(0.0,),
        min_samples_grid=(1,),
        min_price_grid=(0.0,),
        max_price_grid=(100.0,),
        max_spread_grid=(None, 5.0),
        allow_no_grid=(False,),
    )

    results_df = pd.read_csv(results_path)
    diagnostics_df = pd.read_csv(diagnostics_path)
    payload = json.loads(summary_path.read_text(encoding="utf-8"))
    assert set(results_df["pricing_mode"]) == {"decision_price", "candle_proxy"}
    assert set(payload["results_by_pricing_mode"]) == {"decision_price", "candle_proxy"}
    assert {"city_key", "month_bucket", "season_bucket", "entry_price_bucket", "sample_size_bucket"} <= set(
        diagnostics_df["breakdown"]
    )


def test_insufficient_data_fails_clearly(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    scored_path = _write_scored_pickle(tmp_path, _scored_frame_with_quotes(days=3))
    monkeypatch.setattr(pd, "read_parquet", lambda path: pd.read_pickle(path))

    with pytest.raises(WalkforwardClimatologyError, match="No valid walk-forward folds"):
        run_walkforward_climatology(
            scored_dataset_path=scored_path,
            output_dir=tmp_path,
            pricing_mode="decision_price",
            train_window=2,
            validation_window=1,
            test_window=1,
        )


def test_cli_smoke_for_walkforward(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    def fake_run(**kwargs):
        assert kwargs["pricing_mode"] == "both"
        return (
            tmp_path / "walkforward_results_climatology.csv",
            tmp_path / "walkforward_summary_climatology.json",
            tmp_path / "walkforward_diagnostics_climatology.csv",
            {
                "fold_count": 3,
                "pricing_mode": "both",
            },
        )

    monkeypatch.setattr(cli_module, "run_walkforward_climatology", fake_run)

    runner = CliRunner()
    result = runner.invoke(app, ["backtest", "walkforward-climatology"])

    assert result.exit_code == 0
    assert "Saved walk-forward climatology" in result.stdout
    assert "folds: 3" in result.stdout


def _write_scored_pickle(tmp_path: Path, df: pd.DataFrame) -> Path:
    path = tmp_path / "backtest_scored_climatology.parquet"
    df.to_pickle(path)
    return path


def _validation_selection_frame() -> pd.DataFrame:
    return pd.DataFrame(
        [
            _row("2026-01-01", 40.0, 0.05, True, 42.0, 44.0),
            _row("2026-01-02", 40.0, 0.05, False, 42.0, 44.0),
            _row("2026-01-03", 40.0, 0.30, True, 42.0, 44.0),
            _row("2026-01-04", 40.0, 0.03, False, 42.0, 44.0),
            _row("2026-01-05", 40.0, 0.04, True, 42.0, 44.0),
        ]
    )


def _scored_frame_with_quotes(days: int) -> pd.DataFrame:
    rows = []
    for offset in range(days):
        event_date = pd.Timestamp("2026-01-01") + pd.Timedelta(days=offset)
        decision_price = 40.0 + (offset % 3)
        edge = 0.08 if offset % 2 == 0 else 0.03
        rows.append(
            _row(
                event_date.strftime("%Y-%m-%d"),
                decision_price,
                edge,
                offset % 3 == 0,
                42.0,
                44.0,
            )
        )
    return pd.DataFrame(rows)


def _row(
    event_date: str,
    decision_price: float,
    edge_yes: float,
    resolved_yes: bool,
    yes_bid: float,
    yes_ask: float,
) -> dict[str, object]:
    model_prob_yes = round((decision_price / 100.0) + edge_yes, 6)
    model_prob_no = round(1.0 - model_prob_yes, 6)
    return {
        "city_key": "nyc",
        "market_ticker": f"MKT-{event_date}",
        "event_date": event_date,
        "decision_ts": f"{event_date}T14:00:00+00:00",
        "decision_price": decision_price,
        "yes_bid": yes_bid,
        "yes_ask": yes_ask,
        "no_bid": round(100.0 - yes_ask, 6),
        "no_ask": round(100.0 - yes_bid, 6),
        "actual_tmax_f": 70.0,
        "normal_tmax_f": 64.0,
        "tmax_anomaly_f": 6.0,
        "resolved_yes": resolved_yes,
        "model_prob_yes": model_prob_yes,
        "model_prob_no": model_prob_no,
        "fair_yes": model_prob_yes,
        "fair_no": model_prob_no,
        "edge_yes": edge_yes,
        "lookback_sample_size": 8,
        "model_name": "baseline_climatology_v1",
    }
