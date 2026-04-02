from __future__ import annotations

from pathlib import Path

import pandas as pd
import pytest

from kwb.models.forecast_distribution import estimate_forecast_prob_yes, score_forecast_distribution


def test_estimate_forecast_prob_yes_increases_with_hotter_forecast() -> None:
    history_samples = pd.Series([60.0, 61.0, 62.0, 63.0]).to_numpy()
    cool_prob = estimate_forecast_prob_yes(
        history_samples=history_samples,
        forecast_max_temp_f=61.0,
        normal_tmax_f=61.0,
        floor_strike=62.0,
        cap_strike=None,
        strike_type="above",
        distribution_sigma_f=2.0,
    )
    hot_prob = estimate_forecast_prob_yes(
        history_samples=history_samples,
        forecast_max_temp_f=69.0,
        normal_tmax_f=61.0,
        floor_strike=62.0,
        cap_strike=None,
        strike_type="above",
        distribution_sigma_f=2.0,
    )
    assert hot_prob > cool_prob


def test_score_forecast_distribution_uses_latest_eligible_snapshot(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    backtest = pd.DataFrame(
        [
            {
                "city_key": "nyc",
                "market_ticker": "KXHIGHNY-26APR02-T70",
                "event_date": "2026-04-02",
                "month_day": "04-02",
                "decision_ts": "2026-04-02T14:00:00+00:00",
                "decision_price": 20.0,
                "yes_bid": 19.0,
                "yes_ask": 21.0,
                "no_bid": 79.0,
                "no_ask": 81.0,
                "actual_tmax_f": 72.0,
                "normal_tmax_f": 60.0,
                "tmax_anomaly_f": 12.0,
                "resolved_yes": True,
                "floor_strike": 70.0,
                "cap_strike": None,
                "strike_type": "above",
            }
        ]
    )
    history = pd.DataFrame(
        [
            {"city_key": "nyc", "obs_date": "2025-04-01", "tmax_f": 58.0},
            {"city_key": "nyc", "obs_date": "2025-04-02", "tmax_f": 60.0},
            {"city_key": "nyc", "obs_date": "2025-04-03", "tmax_f": 61.0},
            {"city_key": "nyc", "obs_date": "2024-04-01", "tmax_f": 59.0},
            {"city_key": "nyc", "obs_date": "2024-04-02", "tmax_f": 62.0},
            {"city_key": "nyc", "obs_date": "2024-04-03", "tmax_f": 63.0},
        ]
    )
    forecast = pd.DataFrame(
        [
            {
                "snapshot_ts": "2026-04-02T13:30:00+00:00",
                "city_key": "nyc",
                "period_start_ts": "2026-04-02T15:00:00+00:00",
                "period_end_ts": "2026-04-02T16:00:00+00:00",
                "period_date_local": "2026-04-02",
                "temperature_f": 71.0,
            },
            {
                "snapshot_ts": "2026-04-02T13:30:00+00:00",
                "city_key": "nyc",
                "period_start_ts": "2026-04-02T16:00:00+00:00",
                "period_end_ts": "2026-04-02T17:00:00+00:00",
                "period_date_local": "2026-04-02",
                "temperature_f": 73.0,
            },
        ]
    )
    frames = {
        "backtest.parquet": backtest,
        "history.parquet": history,
        "forecast.parquet": forecast,
    }
    captured: dict[str, pd.DataFrame] = {}

    monkeypatch.setattr(pd, "read_parquet", lambda path: frames[Path(path).name].copy())
    monkeypatch.setattr(pd.DataFrame, "to_parquet", lambda self, path, index=False: captured.setdefault("df", self.copy()))
    (tmp_path / "backtest.parquet").write_text("placeholder", encoding="utf-8")
    (tmp_path / "history.parquet").write_text("placeholder", encoding="utf-8")
    (tmp_path / "forecast.parquet").write_text("placeholder", encoding="utf-8")

    outpath, summary = score_forecast_distribution(
        backtest_dataset_path=tmp_path / "backtest.parquet",
        history_path=tmp_path / "history.parquet",
        forecast_snapshots_path=tmp_path / "forecast.parquet",
        output_dir=tmp_path,
        day_window=1,
        min_lookback_samples=2,
    )

    assert outpath == tmp_path / "backtest_scored_forecast_distribution.parquet"
    assert summary["rows_scored"] == 1
    assert captured["df"].loc[0, "forecast_max_temp_f"] == 73.0
    assert captured["df"].loc[0, "forecast_snapshot_ts"] == "2026-04-02T13:30:00+00:00"
