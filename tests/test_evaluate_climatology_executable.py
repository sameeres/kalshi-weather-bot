import json
from pathlib import Path

import pandas as pd
import pytest
from typer.testing import CliRunner

import kwb.cli as cli_module
from kwb.cli import app
from kwb.backtest.evaluate_climatology_executable import (
    ClimatologyExecutableEvaluationError,
    evaluate_climatology_executable_strategy,
)
from kwb.marts.backtest_dataset import build_backtest_dataset
from kwb.models.baseline_climatology import score_climatology_baseline


def test_yes_trade_selection_uses_executable_edge_not_decision_price_edge(
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
                    "market_ticker": "MKT-YES-STRICT",
                    "event_date": "2026-03-20",
                    "decision_ts": "2026-03-20T14:00:00+00:00",
                    "decision_price": 30.0,
                    "resolved_yes": True,
                    "model_prob_yes": 0.5,
                    "model_prob_no": 0.5,
                    "fair_yes": 0.5,
                    "fair_no": 0.5,
                    "edge_yes": 0.2,
                    "lookback_sample_size": 8,
                    "model_name": "baseline_climatology_v1",
                    "yes_bid": 40.0,
                    "yes_ask": 55.0,
                    "no_bid": 45.0,
                    "no_ask": 60.0,
                }
            ]
        )
    }

    monkeypatch.setattr(pd, "read_parquet", lambda path: frames[Path(path).name].copy())
    monkeypatch.setattr(pd.DataFrame, "to_parquet", lambda self, path, index=False: captured.setdefault("df", self.copy()))
    monkeypatch.setattr(Path, "write_text", lambda self, text, encoding="utf-8": None)

    _, _, summary = evaluate_climatology_executable_strategy(
        scored_dataset_path=scored_path,
        output_path=tmp_path / "trades.parquet",
        summary_output_path=tmp_path / "summary.json",
        min_edge=0.0,
    )

    assert summary["trades_taken"] == 0
    assert captured["df"].empty


def test_no_trade_selection_uses_executable_edge(
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
                    "market_ticker": "MKT-NO-STRICT",
                    "event_date": "2026-03-20",
                    "decision_ts": "2026-03-20T14:00:00+00:00",
                    "decision_price": 70.0,
                    "resolved_yes": False,
                    "model_prob_yes": 0.25,
                    "model_prob_no": 0.75,
                    "fair_yes": 0.25,
                    "fair_no": 0.75,
                    "edge_yes": -0.45,
                    "lookback_sample_size": 8,
                    "model_name": "baseline_climatology_v1",
                    "yes_bid": 68.0,
                    "yes_ask": 80.0,
                    "no_bid": 18.0,
                    "no_ask": 22.0,
                }
            ]
        )
    }

    monkeypatch.setattr(pd, "read_parquet", lambda path: frames[Path(path).name].copy())
    monkeypatch.setattr(pd.DataFrame, "to_parquet", lambda self, path, index=False: captured.setdefault("df", self.copy()))
    monkeypatch.setattr(Path, "write_text", lambda self, text, encoding="utf-8": None)

    evaluate_climatology_executable_strategy(
        scored_dataset_path=scored_path,
        output_path=tmp_path / "trades.parquet",
        summary_output_path=tmp_path / "summary.json",
        allow_no=True,
        min_edge=0.1,
    )

    df = captured["df"]
    assert df.loc[0, "chosen_side"] == "no"
    assert df.loc[0, "entry_price"] == 22.0
    assert df.loc[0, "exec_edge_no"] == 0.53


def test_better_side_is_chosen_when_both_qualify(
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
                    "market_ticker": "MKT-BOTH",
                    "event_date": "2026-03-20",
                    "decision_ts": "2026-03-20T14:00:00+00:00",
                    "decision_price": 48.0,
                    "resolved_yes": True,
                    "model_prob_yes": 0.62,
                    "model_prob_no": 0.38,
                    "fair_yes": 0.62,
                    "fair_no": 0.38,
                    "edge_yes": 0.14,
                    "lookback_sample_size": 10,
                    "model_name": "baseline_climatology_v1",
                    "yes_bid": 45.0,
                    "yes_ask": 50.0,
                    "no_bid": 50.0,
                    "no_ask": 55.0,
                }
            ]
        )
    }

    monkeypatch.setattr(pd, "read_parquet", lambda path: frames[Path(path).name].copy())
    monkeypatch.setattr(pd.DataFrame, "to_parquet", lambda self, path, index=False: captured.setdefault("df", self.copy()))
    monkeypatch.setattr(Path, "write_text", lambda self, text, encoding="utf-8": None)

    evaluate_climatology_executable_strategy(
        scored_dataset_path=scored_path,
        output_path=tmp_path / "trades.parquet",
        summary_output_path=tmp_path / "summary.json",
        allow_no=True,
        min_edge=0.01,
    )

    df = captured["df"]
    assert df.loc[0, "chosen_side"] == "yes"
    assert df.loc[0, "edge_at_entry"] == 0.12


def test_rows_with_missing_executable_quotes_are_filtered_out(
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
                    "market_ticker": "MKT-MISSING-ASK",
                    "event_date": "2026-03-20",
                    "decision_ts": "2026-03-20T14:00:00+00:00",
                    "decision_price": 48.0,
                    "resolved_yes": True,
                    "model_prob_yes": 0.62,
                    "model_prob_no": 0.38,
                    "fair_yes": 0.62,
                    "fair_no": 0.38,
                    "edge_yes": 0.14,
                    "lookback_sample_size": 10,
                    "model_name": "baseline_climatology_v1",
                    "yes_bid": 45.0,
                    "yes_ask": None,
                    "no_bid": 50.0,
                    "no_ask": 55.0,
                }
            ]
        )
    }

    monkeypatch.setattr(pd, "read_parquet", lambda path: frames[Path(path).name].copy())
    monkeypatch.setattr(pd.DataFrame, "to_parquet", lambda self, path, index=False: captured.setdefault("df", self.copy()))
    monkeypatch.setattr(Path, "write_text", lambda self, text, encoding="utf-8": None)

    _, _, summary = evaluate_climatology_executable_strategy(
        scored_dataset_path=scored_path,
        output_path=tmp_path / "trades.parquet",
        summary_output_path=tmp_path / "summary.json",
    )

    assert summary["rows_with_executable_yes_quote"] == 0
    assert summary["trades_taken"] == 0
    assert captured["df"].empty


def test_price_bounds_apply_to_executable_entry_price(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    scored_path = _write_placeholder(tmp_path)
    captured: dict[str, pd.DataFrame] = {}
    frames = _base_scored_frames()

    monkeypatch.setattr(pd, "read_parquet", lambda path: frames[Path(path).name].copy())
    monkeypatch.setattr(pd.DataFrame, "to_parquet", lambda self, path, index=False: captured.setdefault("df", self.copy()))
    monkeypatch.setattr(Path, "write_text", lambda self, text, encoding="utf-8": None)

    _, _, summary = evaluate_climatology_executable_strategy(
        scored_dataset_path=scored_path,
        output_path=tmp_path / "trades.parquet",
        summary_output_path=tmp_path / "summary.json",
        max_price=45.0,
    )

    assert summary["trades_taken"] == 1
    assert list(captured["df"]["market_ticker"]) == ["MKT-EXEC-1"]


def test_min_samples_filter_is_applied(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    scored_path = _write_placeholder(tmp_path)
    captured: dict[str, pd.DataFrame] = {}
    frames = _base_scored_frames()

    monkeypatch.setattr(pd, "read_parquet", lambda path: frames[Path(path).name].copy())
    monkeypatch.setattr(pd.DataFrame, "to_parquet", lambda self, path, index=False: captured.setdefault("df", self.copy()))
    monkeypatch.setattr(Path, "write_text", lambda self, text, encoding="utf-8": None)

    _, _, summary = evaluate_climatology_executable_strategy(
        scored_dataset_path=scored_path,
        output_path=tmp_path / "trades.parquet",
        summary_output_path=tmp_path / "summary.json",
        min_samples=10,
    )

    assert summary["trades_taken"] == 1
    assert list(captured["df"]["market_ticker"]) == ["MKT-EXEC-2"]


def test_gross_and_net_pnl_use_executable_entry_price(
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
                    "market_ticker": "MKT-PNL",
                    "event_date": "2026-03-20",
                    "decision_ts": "2026-03-20T14:00:00+00:00",
                    "decision_price": 35.0,
                    "resolved_yes": True,
                    "model_prob_yes": 0.7,
                    "model_prob_no": 0.3,
                    "fair_yes": 0.7,
                    "fair_no": 0.3,
                    "edge_yes": 0.35,
                    "lookback_sample_size": 8,
                    "model_name": "baseline_climatology_v1",
                    "yes_bid": 42.0,
                    "yes_ask": 45.0,
                    "no_bid": 55.0,
                    "no_ask": 60.0,
                }
            ]
        )
    }

    monkeypatch.setattr(pd, "read_parquet", lambda path: frames[Path(path).name].copy())
    monkeypatch.setattr(pd.DataFrame, "to_parquet", lambda self, path, index=False: captured.setdefault("df", self.copy()))
    monkeypatch.setattr(Path, "write_text", lambda self, text, encoding="utf-8": None)

    evaluate_climatology_executable_strategy(
        scored_dataset_path=scored_path,
        output_path=tmp_path / "trades.parquet",
        summary_output_path=tmp_path / "summary.json",
        fee_per_contract=0.03,
    )

    df = captured["df"]
    assert df.loc[0, "entry_price"] == 45.0
    assert df.loc[0, "gross_pnl"] == 0.55
    assert df.loc[0, "net_pnl"] == 0.52


def test_kalshi_standard_taker_fee_filters_tiny_edge_trade(
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
                    "market_ticker": "MKT-TAKER-FEE",
                    "event_date": "2026-03-20",
                    "decision_ts": "2026-03-20T14:00:00+00:00",
                    "decision_price": 49.0,
                    "resolved_yes": True,
                    "model_prob_yes": 0.51,
                    "model_prob_no": 0.49,
                    "fair_yes": 0.51,
                    "fair_no": 0.49,
                    "edge_yes": 0.02,
                    "lookback_sample_size": 8,
                    "model_name": "baseline_climatology_v1",
                    "yes_bid": 48.0,
                    "yes_ask": 50.0,
                    "no_bid": 50.0,
                    "no_ask": 52.0,
                }
            ]
        )
    }

    monkeypatch.setattr(pd, "read_parquet", lambda path: frames[Path(path).name].copy())
    monkeypatch.setattr(pd.DataFrame, "to_parquet", lambda self, path, index=False: captured.setdefault("df", self.copy()))
    monkeypatch.setattr(Path, "write_text", lambda self, text, encoding="utf-8": None)

    _, _, summary = evaluate_climatology_executable_strategy(
        scored_dataset_path=scored_path,
        output_path=tmp_path / "trades.parquet",
        summary_output_path=tmp_path / "summary.json",
        min_edge=0.0,
        fee_model="kalshi_standard_taker",
    )

    assert summary["trades_taken"] == 0
    assert summary["total_fees"] == 0.0
    assert captured["df"].empty


def test_output_schema_contains_required_columns(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    scored_path = _write_placeholder(tmp_path)
    captured: dict[str, pd.DataFrame] = {}
    frames = _base_scored_frames()

    monkeypatch.setattr(pd, "read_parquet", lambda path: frames[Path(path).name].copy())
    monkeypatch.setattr(pd.DataFrame, "to_parquet", lambda self, path, index=False: captured.setdefault("df", self.copy()))
    monkeypatch.setattr(Path, "write_text", lambda self, text, encoding="utf-8": None)

    evaluate_climatology_executable_strategy(
        scored_dataset_path=scored_path,
        output_path=tmp_path / "trades.parquet",
        summary_output_path=tmp_path / "summary.json",
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
        "gross_edge_at_entry",
        "contracts",
        "fees",
        "gross_pnl",
        "net_pnl",
        "lookback_sample_size",
        "model_name",
    ]


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

    evaluate_climatology_executable_strategy(
        scored_dataset_path=scored_path,
        output_path=tmp_path / "trades.parquet",
        summary_output_path=tmp_path / "summary.json",
    )

    payload = json.loads(captured["text"])
    assert payload["trades_taken"] == 2
    assert payload["pricing_mode"] == "candle_proxy"
    assert payload["quote_source"] == "decision_candle_ohlc_bounds"
    assert payload["uses_true_quotes"] is False
    assert payload["rows_with_executable_yes_quote"] == 2
    assert payload["rows_with_both_sides_executable_quotes"] == 2
    assert payload["yes_quote_coverage"] == 1.0
    assert payload["average_yes_spread"] == 2.0
    assert payload["spread_bucket_counts"]["0-2"] == 2
    assert payload["fee_model"] == "flat_per_contract"
    assert payload["total_fees"] == 0.0
    assert "brier_score" in payload


def test_cli_smoke_for_executable_evaluator(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    def fake_evaluate(**kwargs):
        assert kwargs["allow_no"] is False
        return (
            tmp_path / "backtest_trades_climatology_executable.parquet",
            tmp_path / "backtest_summary_climatology_executable.json",
            {
                "trades_taken": 2,
                "yes_trades_taken": 2,
                "no_trades_taken": 0,
                "total_net_pnl": 0.42,
            },
        )

    monkeypatch.setattr(cli_module, "evaluate_climatology_executable_strategy", fake_evaluate)

    runner = CliRunner()
    result = runner.invoke(app, ["backtest", "evaluate-climatology-executable"])

    assert result.exit_code == 0
    assert "Saved executable climatology evaluation" in result.stdout
    assert "trades: 2" in result.stdout
    assert "total net pnl: 0.42" in result.stdout


def test_missing_required_quote_columns_fail_cleanly(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    scored_path = _write_placeholder(tmp_path)
    frames = {
        "backtest_scored_climatology.parquet": pd.DataFrame(
            [
                {
                    "city_key": "nyc",
                    "market_ticker": "MKT-MISSING-COLS",
                    "event_date": "2026-03-20",
                    "decision_ts": "2026-03-20T14:00:00+00:00",
                    "decision_price": 40.0,
                    "resolved_yes": True,
                    "model_prob_yes": 0.6,
                    "model_prob_no": 0.4,
                    "fair_yes": 0.6,
                    "fair_no": 0.4,
                    "lookback_sample_size": 8,
                    "model_name": "baseline_climatology_v1",
                }
            ]
        )
    }

    monkeypatch.setattr(pd, "read_parquet", lambda path: frames[Path(path).name].copy())

    with pytest.raises(ClimatologyExecutableEvaluationError, match="Required executable quote columns are missing"):
        evaluate_climatology_executable_strategy(
            scored_dataset_path=scored_path,
            output_path=tmp_path / "trades.parquet",
            summary_output_path=tmp_path / "summary.json",
        )


def test_strict_executable_evaluator_succeeds_on_normal_pipeline_output(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(pd.DataFrame, "to_parquet", lambda self, path, index=False: self.to_pickle(path))
    monkeypatch.setattr(pd, "read_parquet", lambda path: pd.read_pickle(path))
    config_path = tmp_path / "cities.yml"
    config_path.write_text(
        "\n".join(
            [
                "cities:",
                "  - city_key: nyc",
                "    city_name: New York City",
                "    timezone: America/New_York",
                "    kalshi_series_ticker: KXHIGHNY",
                "    enabled: true",
            ]
        )
        + "\n",
        encoding="utf-8",
    )

    staging_dir = tmp_path / "staging"
    staging_dir.mkdir(parents=True, exist_ok=True)

    pd.DataFrame(
        [
            {
                "station_id": "KLGA",
                "city_key": "nyc",
                "obs_date": "2026-03-20",
                "tmax_f": 70.0,
            },
            {
                "station_id": "KLGA",
                "city_key": "nyc",
                "obs_date": "2025-03-20",
                "tmax_f": 67.0,
            },
            {
                "station_id": "KLGA",
                "city_key": "nyc",
                "obs_date": "2024-03-20",
                "tmax_f": 70.0,
            },
        ]
    ).to_parquet(staging_dir / "weather_daily.parquet", index=False)
    pd.DataFrame(
        [
            {
                "station_id": "KLGA",
                "city_key": "nyc",
                "month_day": "03-20",
                "normal_tmax_f": 64.0,
                "normals_period": "1991-2020",
                "normals_source": "NORMAL_DLY",
            }
        ]
    ).to_parquet(staging_dir / "weather_normals_daily.parquet", index=False)
    pd.DataFrame(
        [
            {
                "city_key": "nyc",
                "series_ticker": "KXHIGHNY",
                "event_ticker": "KXHIGHNY-26MAR20",
                "market_ticker": "KXHIGHNY-26MAR20-B65",
                "strike_date": "2026-03-20T00:00:00Z",
                "market_title": "65F to 69F",
                "market_subtitle": "Daily high bucket",
                "status": "settled",
                "floor_strike": 65,
                "cap_strike": 69,
                "strike_type": "between",
            }
        ]
    ).to_parquet(staging_dir / "kalshi_markets.parquet", index=False)
    pd.DataFrame(
        [
            {
                "market_ticker": "KXHIGHNY-26MAR20-B65",
                "city_key": "nyc",
                "candle_ts": "2026-03-20T14:00:00+00:00",
                "open": 42.0,
                "high": 45.0,
                "low": 41.0,
                "close": 44.0,
                "volume": 11,
                "interval": "1h",
            }
        ]
    ).to_parquet(staging_dir / "kalshi_candles.parquet", index=False)

    marts_dir = tmp_path / "marts"
    mart_path, _ = build_backtest_dataset(
        decision_time_local="10:00",
        config_path=config_path,
        weather_path=staging_dir / "weather_daily.parquet",
        normals_path=staging_dir / "weather_normals_daily.parquet",
        markets_path=staging_dir / "kalshi_markets.parquet",
        candles_path=staging_dir / "kalshi_candles.parquet",
        output_dir=marts_dir,
    )
    scored_path, _ = score_climatology_baseline(
        backtest_dataset_path=mart_path,
        history_path=staging_dir / "weather_daily.parquet",
        output_dir=marts_dir,
    )
    trades_path, summary_path, summary = evaluate_climatology_executable_strategy(
        scored_dataset_path=scored_path,
        output_path=marts_dir / "backtest_trades_climatology_executable.parquet",
        summary_output_path=marts_dir / "backtest_summary_climatology_executable.json",
        min_edge=0.01,
    )

    assert trades_path.exists()
    assert summary_path.exists()
    trades_df = pd.read_parquet(trades_path)
    scored_df = pd.read_parquet(scored_path)
    assert list(scored_df[["yes_bid", "yes_ask", "no_bid", "no_ask"]].iloc[0]) == [41.0, 45.0, 55.0, 59.0]
    assert trades_df.loc[0, "chosen_side"] == "yes"
    assert trades_df.loc[0, "entry_price"] == 45.0
    assert summary["trades_taken"] == 1


def _base_scored_frames() -> dict[str, pd.DataFrame]:
    return {
        "backtest_scored_climatology.parquet": pd.DataFrame(
            [
                {
                    "city_key": "nyc",
                    "market_ticker": "MKT-EXEC-1",
                    "event_date": "2026-03-20",
                    "decision_ts": "2026-03-20T14:00:00+00:00",
                    "decision_price": 40.0,
                    "resolved_yes": True,
                    "model_prob_yes": 0.6,
                    "model_prob_no": 0.4,
                    "fair_yes": 0.6,
                    "fair_no": 0.4,
                    "edge_yes": 0.2,
                    "lookback_sample_size": 8,
                    "model_name": "baseline_climatology_v1",
                    "yes_bid": 42.0,
                    "yes_ask": 44.0,
                    "no_bid": 56.0,
                    "no_ask": 60.0,
                },
                {
                    "city_key": "nyc",
                    "market_ticker": "MKT-EXEC-2",
                    "event_date": "2026-03-21",
                    "decision_ts": "2026-03-21T14:00:00+00:00",
                    "decision_price": 55.0,
                    "resolved_yes": False,
                    "model_prob_yes": 0.57,
                    "model_prob_no": 0.43,
                    "fair_yes": 0.57,
                    "fair_no": 0.43,
                    "edge_yes": 0.02,
                    "lookback_sample_size": 12,
                    "model_name": "baseline_climatology_v1",
                    "yes_bid": 52.0,
                    "yes_ask": 54.0,
                    "no_bid": 46.0,
                    "no_ask": 48.0,
                },
            ]
        )
    }


def _write_placeholder(tmp_path: Path) -> Path:
    scored_path = tmp_path / "backtest_scored_climatology.parquet"
    scored_path.write_text("placeholder", encoding="utf-8")
    return scored_path
