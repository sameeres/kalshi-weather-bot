from pathlib import Path

import pandas as pd
import pytest
from typer.testing import CliRunner

import kwb.cli as cli_module
from kwb.cli import app
from kwb.models.baseline_climatology import (
    ClimatologyModelError,
    estimate_climatology_prob_yes,
    evaluate_scored_climatology,
    score_climatology_baseline,
)


def test_climatology_baseline_uses_only_prior_observations(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    backtest_path, history_path = _write_placeholders(tmp_path)
    captured: dict[str, pd.DataFrame] = {}
    frames = _base_frames()
    frames["weather_daily.parquet"] = pd.DataFrame(
        [
            {"city_key": "nyc", "obs_date": "2024-03-20", "tmax_f": 68.0},
            {"city_key": "nyc", "obs_date": "2025-03-20", "tmax_f": 72.0},
            {"city_key": "nyc", "obs_date": "2026-03-19", "tmax_f": 71.0},
            {"city_key": "nyc", "obs_date": "2026-03-21", "tmax_f": 90.0},
        ]
    )

    monkeypatch.setattr(pd, "read_parquet", lambda path: frames[Path(path).name].copy())
    monkeypatch.setattr(
        pd.DataFrame,
        "to_parquet",
        lambda self, path, index=False: captured.setdefault("df", self.copy()),
    )

    _, summary = score_climatology_baseline(
        backtest_dataset_path=backtest_path,
        history_path=history_path,
        output_dir=tmp_path,
        day_window=1,
    )

    df = captured["df"]
    assert summary["rows_scored"] == 1
    assert df.loc[0, "lookback_sample_size"] == 3
    assert df.loc[0, "model_prob_yes"] == pytest.approx(1 / 3, rel=1e-6)


def test_climatology_baseline_same_city_history_is_used_correctly(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    backtest_path, history_path = _write_placeholders(tmp_path)
    captured: dict[str, pd.DataFrame] = {}
    frames = _base_frames()
    frames["weather_daily.parquet"] = pd.DataFrame(
        [
            {"city_key": "nyc", "obs_date": "2025-03-20", "tmax_f": 67.0},
            {"city_key": "nyc", "obs_date": "2024-03-20", "tmax_f": 70.0},
            {"city_key": "chicago", "obs_date": "2025-03-20", "tmax_f": 99.0},
        ]
    )

    monkeypatch.setattr(pd, "read_parquet", lambda path: frames[Path(path).name].copy())
    monkeypatch.setattr(
        pd.DataFrame,
        "to_parquet",
        lambda self, path, index=False: captured.setdefault("df", self.copy()),
    )

    score_climatology_baseline(
        backtest_dataset_path=backtest_path,
        history_path=history_path,
        output_dir=tmp_path,
    )

    df = captured["df"]
    assert df.loc[0, "lookback_sample_size"] == 2
    assert df.loc[0, "model_prob_yes"] == 0.5


def test_climatology_market_probability_logic_is_correct() -> None:
    history_df = pd.DataFrame({"tmax_f": [64.0, 66.0, 67.0, 70.0]})

    prob = estimate_climatology_prob_yes(
        history_df=history_df,
        floor_strike=65,
        cap_strike=69,
        strike_type="between",
    )

    assert prob == 0.5


def test_climatology_output_schema_contains_required_columns(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    backtest_path, history_path = _write_placeholders(tmp_path)
    captured: dict[str, pd.DataFrame] = {}
    frames = _base_frames()

    monkeypatch.setattr(pd, "read_parquet", lambda path: frames[Path(path).name].copy())
    monkeypatch.setattr(
        pd.DataFrame,
        "to_parquet",
        lambda self, path, index=False: captured.setdefault("df", self.copy()),
    )

    score_climatology_baseline(
        backtest_dataset_path=backtest_path,
        history_path=history_path,
        output_dir=tmp_path,
    )

    df = captured["df"]
    assert list(df.columns) == [
        "city_key",
        "market_ticker",
        "event_date",
        "decision_ts",
        "decision_price",
        "yes_bid",
        "yes_ask",
        "no_bid",
        "no_ask",
        "actual_tmax_f",
        "normal_tmax_f",
        "tmax_anomaly_f",
        "resolved_yes",
        "model_prob_yes",
        "model_prob_no",
        "fair_yes",
        "fair_no",
        "edge_yes",
        "lookback_sample_size",
        "model_name",
    ]


def test_climatology_preserves_quote_columns_unchanged(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    backtest_path, history_path = _write_placeholders(tmp_path)
    captured: dict[str, pd.DataFrame] = {}
    frames = _base_frames()

    monkeypatch.setattr(pd, "read_parquet", lambda path: frames[Path(path).name].copy())
    monkeypatch.setattr(
        pd.DataFrame,
        "to_parquet",
        lambda self, path, index=False: captured.setdefault("df", self.copy()),
    )

    score_climatology_baseline(
        backtest_dataset_path=backtest_path,
        history_path=history_path,
        output_dir=tmp_path,
    )

    df = captured["df"]
    assert df.loc[0, "decision_price"] == 44.0
    assert df.loc[0, "yes_bid"] == 41.0
    assert df.loc[0, "yes_ask"] == 45.0
    assert df.loc[0, "no_bid"] == 55.0
    assert df.loc[0, "no_ask"] == 59.0


def test_climatology_insufficient_lookback_is_handled_explicitly(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    backtest_path, history_path = _write_placeholders(tmp_path)
    captured: dict[str, pd.DataFrame] = {}
    frames = _base_frames()
    frames["weather_daily.parquet"] = pd.DataFrame(
        [
            {"city_key": "nyc", "obs_date": "2025-03-20", "tmax_f": 67.0},
        ]
    )

    monkeypatch.setattr(pd, "read_parquet", lambda path: frames[Path(path).name].copy())
    monkeypatch.setattr(
        pd.DataFrame,
        "to_parquet",
        lambda self, path, index=False: captured.setdefault("df", self.copy()),
    )

    _, summary = score_climatology_baseline(
        backtest_dataset_path=backtest_path,
        history_path=history_path,
        output_dir=tmp_path,
        min_lookback_samples=2,
    )

    assert summary["rows_scored"] == 0
    assert summary["unscored_insufficient_history_rows"] == 1
    assert captured["df"].empty


def test_climatology_evaluation_helper_computes_brier_score() -> None:
    scored_df = pd.DataFrame(
        [
            {"model_prob_yes": 0.2, "resolved_yes": False, "lookback_sample_size": 5, "edge_yes": -0.1},
            {"model_prob_yes": 0.8, "resolved_yes": True, "lookback_sample_size": 7, "edge_yes": 0.05},
        ]
    )

    summary = evaluate_scored_climatology(scored_df)

    assert summary["rows_scored"] == 2
    assert summary["average_lookback_sample_size"] == 6.0
    assert summary["brier_score"] == 0.04


def test_climatology_cli_smoke(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    def fake_score(**kwargs):
        assert kwargs["day_window"] == 0
        assert kwargs["min_lookback_samples"] == 1
        return tmp_path / "backtest_scored_climatology.parquet", {
            "rows_scored": 8,
            "average_lookback_sample_size": 12.5,
            "brier_score": 0.123456,
            "average_edge_yes": 0.01,
        }

    monkeypatch.setattr(cli_module, "score_climatology_baseline", fake_score)

    runner = CliRunner()
    result = runner.invoke(app, ["model", "climatology-baseline"])

    assert result.exit_code == 0
    assert "Saved climatology baseline" in result.stdout
    assert "8" in result.stdout
    assert "Brier: 0.123456" in result.stdout


def test_climatology_missing_input_fails_clearly(tmp_path: Path) -> None:
    with pytest.raises(ClimatologyModelError, match="Required local input parquet files are missing"):
        score_climatology_baseline(
            backtest_dataset_path=tmp_path / "missing_backtest.parquet",
            history_path=tmp_path / "missing_history.parquet",
            output_dir=tmp_path,
        )


def _base_frames() -> dict[str, pd.DataFrame]:
    return {
        "backtest_dataset.parquet": pd.DataFrame(
            [
                {
                    "city_key": "nyc",
                    "market_ticker": "KXHIGHNY-26MAR20-B65",
                    "event_date": "2026-03-20",
                    "decision_ts": "2026-03-20T14:00:00+00:00",
                    "decision_price": 44.0,
                    "yes_bid": 41.0,
                    "yes_ask": 45.0,
                    "no_bid": 55.0,
                    "no_ask": 59.0,
                    "actual_tmax_f": 70.0,
                    "normal_tmax_f": 64.0,
                    "tmax_anomaly_f": 6.0,
                    "resolved_yes": False,
                    "floor_strike": 65,
                    "cap_strike": 69,
                    "strike_type": "between",
                    "month_day": "03-20",
                }
            ]
        ),
        "weather_daily.parquet": pd.DataFrame(
            [
                {"city_key": "nyc", "obs_date": "2024-03-20", "tmax_f": 67.0},
                {"city_key": "nyc", "obs_date": "2025-03-20", "tmax_f": 70.0},
            ]
        ),
    }


def _write_placeholders(tmp_path: Path) -> tuple[Path, Path]:
    backtest_path = tmp_path / "backtest_dataset.parquet"
    history_path = tmp_path / "weather_daily.parquet"
    backtest_path.write_text("placeholder", encoding="utf-8")
    history_path.write_text("placeholder", encoding="utf-8")
    return backtest_path, history_path
