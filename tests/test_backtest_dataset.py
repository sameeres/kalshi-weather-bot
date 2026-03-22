from pathlib import Path

import pandas as pd
import pytest
from typer.testing import CliRunner

import kwb.cli as cli_module
from kwb.cli import app
from kwb.marts.backtest_dataset import (
    BacktestDatasetBuildError,
    build_backtest_dataset,
    resolve_bucket,
)


def test_backtest_dataset_selects_latest_candle_at_or_before_decision_time(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    config_path = _write_cities_config(
        tmp_path,
        [
            {
                "city_key": "nyc",
                "city_name": "New York City",
                "timezone": "America/New_York",
                "kalshi_series_ticker": "KXHIGHNY",
                "enabled": True,
            }
        ],
    )
    staging_dir = _write_staged_placeholders(tmp_path)
    captured: dict[str, Path | pd.DataFrame] = {}
    frames = _base_frames()
    frames["kalshi_candles.parquet"] = pd.DataFrame(
        [
            {
                "market_ticker": "KXHIGHNY-26MAR20-B65",
                "city_key": "nyc",
                "candle_ts": "2026-03-20T13:30:00+00:00",
                "open": 40,
                "high": 44,
                "low": 39,
                "close": 43,
                "volume": 10,
                "interval": "1h",
                "ingested_at": "2026-03-20T14:00:00+00:00",
            },
            {
                "market_ticker": "KXHIGHNY-26MAR20-B65",
                "city_key": "nyc",
                "candle_ts": "2026-03-20T14:00:00+00:00",
                "open": 42,
                "high": 45,
                "low": 41,
                "close": 44,
                "volume": 11,
                "interval": "1h",
                "ingested_at": "2026-03-20T14:00:00+00:00",
            },
        ]
    )

    monkeypatch.setattr(pd, "read_parquet", lambda path: frames[Path(path).name].copy())
    monkeypatch.setattr(
        pd.DataFrame,
        "to_parquet",
        lambda self, path, index=False: captured.update({"path": Path(path), "df": self.copy()}),
    )

    outpath, stats = build_backtest_dataset(
        decision_time_local="10:00",
        config_path=config_path,
        weather_path=staging_dir / "weather_daily.parquet",
        normals_path=staging_dir / "weather_normals_daily.parquet",
        markets_path=staging_dir / "kalshi_markets.parquet",
        candles_path=staging_dir / "kalshi_candles.parquet",
        output_dir=tmp_path,
    )

    assert outpath == tmp_path / "backtest_dataset.parquet"
    assert stats["rows_written"] == 1
    df = captured["df"]
    assert df.loc[0, "decision_candle_ts"] == "2026-03-20T14:00:00+00:00"
    assert df.loc[0, "decision_price"] == 44
    assert df.loc[0, "yes_bid"] == 41
    assert df.loc[0, "yes_ask"] == 45
    assert df.loc[0, "no_bid"] == 55
    assert df.loc[0, "no_ask"] == 59


def test_backtest_dataset_excludes_candles_after_decision_time(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    config_path = _write_single_city_config(tmp_path)
    staging_dir = _write_staged_placeholders(tmp_path)
    captured: dict[str, pd.DataFrame] = {}
    frames = _base_frames()
    frames["kalshi_candles.parquet"] = pd.DataFrame(
        [
            {
                "market_ticker": "KXHIGHNY-26MAR20-B65",
                "city_key": "nyc",
                "candle_ts": "2026-03-20T13:59:00+00:00",
                "open": 40,
                "high": 44,
                "low": 39,
                "close": 43,
                "volume": 10,
                "interval": "1m",
                "ingested_at": "2026-03-20T14:00:00+00:00",
            },
            {
                "market_ticker": "KXHIGHNY-26MAR20-B65",
                "city_key": "nyc",
                "candle_ts": "2026-03-20T14:01:00+00:00",
                "open": 45,
                "high": 46,
                "low": 44,
                "close": 46,
                "volume": 12,
                "interval": "1m",
                "ingested_at": "2026-03-20T14:02:00+00:00",
            },
        ]
    )

    monkeypatch.setattr(pd, "read_parquet", lambda path: frames[Path(path).name].copy())
    monkeypatch.setattr(
        pd.DataFrame,
        "to_parquet",
        lambda self, path, index=False: captured.setdefault("df", self.copy()),
    )

    build_backtest_dataset(
        decision_time_local="10:00",
        config_path=config_path,
        weather_path=staging_dir / "weather_daily.parquet",
        normals_path=staging_dir / "weather_normals_daily.parquet",
        markets_path=staging_dir / "kalshi_markets.parquet",
        candles_path=staging_dir / "kalshi_candles.parquet",
        output_dir=tmp_path,
    )

    df = captured["df"]
    assert df.loc[0, "decision_candle_ts"] == "2026-03-20T13:59:00+00:00"
    assert df.loc[0, "decision_price"] == 43


def test_backtest_dataset_joins_weather_and_normals_correctly(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    config_path = _write_single_city_config(tmp_path)
    staging_dir = _write_staged_placeholders(tmp_path)
    captured: dict[str, pd.DataFrame] = {}
    frames = _base_frames()

    monkeypatch.setattr(pd, "read_parquet", lambda path: frames[Path(path).name].copy())
    monkeypatch.setattr(
        pd.DataFrame,
        "to_parquet",
        lambda self, path, index=False: captured.setdefault("df", self.copy()),
    )

    build_backtest_dataset(
        decision_time_local="10:00",
        config_path=config_path,
        weather_path=staging_dir / "weather_daily.parquet",
        normals_path=staging_dir / "weather_normals_daily.parquet",
        markets_path=staging_dir / "kalshi_markets.parquet",
        candles_path=staging_dir / "kalshi_candles.parquet",
        output_dir=tmp_path,
    )

    df = captured["df"]
    assert df.loc[0, "actual_tmax_f"] == 70.0
    assert df.loc[0, "normal_tmax_f"] == 64.0
    assert df.loc[0, "tmax_anomaly_f"] == 6.0
    assert df.loc[0, "weather_station_id"] == "KLGA"
    assert df.loc[0, "normals_station_id"] == "KLGA"


def test_backtest_dataset_derives_conservative_quotes_from_decision_candle(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    config_path = _write_single_city_config(tmp_path)
    staging_dir = _write_staged_placeholders(tmp_path)
    captured: dict[str, pd.DataFrame] = {}
    frames = _base_frames()
    frames["kalshi_candles.parquet"] = pd.DataFrame(
        [
            {
                "market_ticker": "KXHIGHNY-26MAR20-B65",
                "city_key": "nyc",
                "candle_ts": "2026-03-20T13:00:00+00:00",
                "open": 38,
                "high": 42,
                "low": 37,
                "close": 40,
                "volume": 9,
                "interval": "1h",
                "ingested_at": "2026-03-20T13:00:00+00:00",
            },
            {
                "market_ticker": "KXHIGHNY-26MAR20-B65",
                "city_key": "nyc",
                "candle_ts": "2026-03-20T14:00:00+00:00",
                "open": 42,
                "high": 47,
                "low": 41,
                "close": 44,
                "volume": 11,
                "interval": "1h",
                "ingested_at": "2026-03-20T14:00:00+00:00",
            },
        ]
    )

    monkeypatch.setattr(pd, "read_parquet", lambda path: frames[Path(path).name].copy())
    monkeypatch.setattr(
        pd.DataFrame,
        "to_parquet",
        lambda self, path, index=False: captured.setdefault("df", self.copy()),
    )

    build_backtest_dataset(
        decision_time_local="10:00",
        config_path=config_path,
        weather_path=staging_dir / "weather_daily.parquet",
        normals_path=staging_dir / "weather_normals_daily.parquet",
        markets_path=staging_dir / "kalshi_markets.parquet",
        candles_path=staging_dir / "kalshi_candles.parquet",
        output_dir=tmp_path,
    )

    df = captured["df"]
    assert df.loc[0, "decision_price"] == 44
    assert df.loc[0, "yes_bid"] == 41
    assert df.loc[0, "yes_ask"] == 47
    assert df.loc[0, "no_bid"] == 53
    assert df.loc[0, "no_ask"] == 59


def test_backtest_dataset_output_schema_contains_required_columns(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    config_path = _write_single_city_config(tmp_path)
    staging_dir = _write_staged_placeholders(tmp_path)
    captured: dict[str, pd.DataFrame] = {}
    frames = _base_frames()

    monkeypatch.setattr(pd, "read_parquet", lambda path: frames[Path(path).name].copy())
    monkeypatch.setattr(
        pd.DataFrame,
        "to_parquet",
        lambda self, path, index=False: captured.setdefault("df", self.copy()),
    )

    build_backtest_dataset(
        decision_time_local="10:00",
        config_path=config_path,
        weather_path=staging_dir / "weather_daily.parquet",
        normals_path=staging_dir / "weather_normals_daily.parquet",
        markets_path=staging_dir / "kalshi_markets.parquet",
        candles_path=staging_dir / "kalshi_candles.parquet",
        output_dir=tmp_path,
    )

    df = captured["df"]
    assert list(df.columns) == [
        "city_key",
        "city_name",
        "timezone",
        "series_ticker",
        "event_ticker",
        "market_ticker",
        "strike_date",
        "event_date",
        "month_day",
        "market_title",
        "market_subtitle",
        "status",
        "floor_strike",
        "cap_strike",
        "strike_type",
        "decision_time_local",
        "decision_ts",
        "decision_candle_ts",
        "decision_price",
        "yes_bid",
        "yes_ask",
        "no_bid",
        "no_ask",
        "candle_interval",
        "actual_tmax_f",
        "normal_tmax_f",
        "tmax_anomaly_f",
        "weather_station_id",
        "normals_station_id",
        "normals_period",
        "normals_source",
        "resolved_yes",
    ]


def test_backtest_dataset_missing_staged_input_fails_clearly(tmp_path: Path) -> None:
    config_path = _write_single_city_config(tmp_path)
    staging_dir = tmp_path / "staging"
    staging_dir.mkdir(parents=True, exist_ok=True)
    (staging_dir / "weather_daily.parquet").write_text("placeholder", encoding="utf-8")

    with pytest.raises(BacktestDatasetBuildError, match="Required staged input parquet files are missing"):
        build_backtest_dataset(
            decision_time_local="10:00",
            config_path=config_path,
            weather_path=staging_dir / "weather_daily.parquet",
            normals_path=staging_dir / "weather_normals_daily.parquet",
            markets_path=staging_dir / "kalshi_markets.parquet",
            candles_path=staging_dir / "kalshi_candles.parquet",
            output_dir=tmp_path,
        )


def test_backtest_dataset_city_timezone_handling_changes_decision_ts(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    config_path = _write_cities_config(
        tmp_path,
        [
            {
                "city_key": "chicago",
                "city_name": "Chicago",
                "timezone": "America/Chicago",
                "kalshi_series_ticker": "KXHIGHCHI",
                "enabled": True,
            }
        ],
    )
    staging_dir = _write_staged_placeholders(tmp_path)
    captured: dict[str, pd.DataFrame] = {}
    frames = {
        "weather_daily.parquet": pd.DataFrame(
            [
                {
                    "station_id": "KORD",
                    "city_key": "chicago",
                    "obs_date": "2026-03-20",
                    "tmax_c": 21.0,
                    "tmin_c": 10.0,
                    "tmax_f": 70.0,
                    "tmin_f": 50.0,
                    "source_dataset": "GHCND",
                    "ingested_at": "2026-03-20T00:00:00+00:00",
                }
            ]
        ),
        "weather_normals_daily.parquet": pd.DataFrame(
            [
                {
                    "station_id": "KORD",
                    "city_key": "chicago",
                    "month_day": "03-20",
                    "normal_tmax_c": 16.0,
                    "normal_tmin_c": 5.0,
                    "normal_tmax_f": 61.0,
                    "normal_tmin_f": 41.0,
                    "normals_period": "1991-2020",
                    "normals_source": "NORMAL_DLY",
                    "ingested_at": "2026-03-20T00:00:00+00:00",
                }
            ]
        ),
        "kalshi_markets.parquet": pd.DataFrame(
            [
                {
                    "city_key": "chicago",
                    "series_ticker": "KXHIGHCHI",
                    "event_ticker": "KXHIGHCHI-26MAR20",
                    "market_ticker": "KXHIGHCHI-26MAR20-B65",
                    "strike_date": "2026-03-20T00:00:00Z",
                    "market_title": "65F to 69F",
                    "market_subtitle": "Daily high bucket",
                    "status": "settled",
                    "floor_strike": 65,
                    "cap_strike": 69,
                    "strike_type": "between",
                    "expiration_ts": "2026-03-20T23:59:00Z",
                    "close_time": "2026-03-20T23:00:00Z",
                    "ingested_at": "2026-03-20T00:00:00+00:00",
                }
            ]
        ),
        "kalshi_candles.parquet": pd.DataFrame(
            [
                {
                    "market_ticker": "KXHIGHCHI-26MAR20-B65",
                    "city_key": "chicago",
                    "candle_ts": "2026-03-20T14:59:00+00:00",
                    "open": 40,
                    "high": 43,
                    "low": 39,
                    "close": 42,
                    "volume": 10,
                    "interval": "1m",
                    "ingested_at": "2026-03-20T15:00:00+00:00",
                }
            ]
        ),
    }

    monkeypatch.setattr(pd, "read_parquet", lambda path: frames[Path(path).name].copy())
    monkeypatch.setattr(
        pd.DataFrame,
        "to_parquet",
        lambda self, path, index=False: captured.setdefault("df", self.copy()),
    )

    build_backtest_dataset(
        decision_time_local="10:00",
        config_path=config_path,
        weather_path=staging_dir / "weather_daily.parquet",
        normals_path=staging_dir / "weather_normals_daily.parquet",
        markets_path=staging_dir / "kalshi_markets.parquet",
        candles_path=staging_dir / "kalshi_candles.parquet",
        output_dir=tmp_path,
    )

    df = captured["df"]
    assert df.loc[0, "decision_ts"] == "2026-03-20T15:00:00+00:00"
    assert df.loc[0, "decision_candle_ts"] == "2026-03-20T14:59:00+00:00"


def test_resolve_bucket_logic_is_explicit_and_correct() -> None:
    assert resolve_bucket(actual_tmax_f=67.0, floor_strike=65, cap_strike=69, strike_type="between") is True
    assert resolve_bucket(actual_tmax_f=70.0, floor_strike=65, cap_strike=69, strike_type="between") is False
    assert resolve_bucket(actual_tmax_f=70.0, floor_strike=70, cap_strike=None, strike_type="above") is True
    assert resolve_bucket(actual_tmax_f=64.0, floor_strike=None, cap_strike=64, strike_type="below") is True


def test_backtest_dataset_cli_smoke(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    config_path = _write_single_city_config(tmp_path)

    def fake_build(**kwargs):
        assert kwargs["decision_time_local"] == "10:00"
        assert kwargs["config_path"] == config_path
        return tmp_path / "backtest_dataset.parquet", {
            "rows_written": 12,
            "cities_covered": 1,
            "decision_time_local": "10:00",
        }

    monkeypatch.setattr(cli_module, "build_backtest_dataset", fake_build)

    runner = CliRunner()
    result = runner.invoke(
        app,
        ["mart", "backtest-dataset", "--decision-time-local", "10:00", "--config-path", str(config_path)],
    )

    assert result.exit_code == 0
    assert "Saved backtest dataset" in result.stdout
    assert "rows: 12" in result.stdout
    assert "decision time local: 10:00" in result.stdout


def _base_frames() -> dict[str, pd.DataFrame]:
    return {
        "weather_daily.parquet": pd.DataFrame(
            [
                {
                    "station_id": "KLGA",
                    "city_key": "nyc",
                    "obs_date": "2026-03-20",
                    "tmax_c": 21.111,
                    "tmin_c": 10.0,
                    "tmax_f": 70.0,
                    "tmin_f": 50.0,
                    "source_dataset": "GHCND",
                    "ingested_at": "2026-03-20T00:00:00+00:00",
                }
            ]
        ),
        "weather_normals_daily.parquet": pd.DataFrame(
            [
                {
                    "station_id": "KLGA",
                    "city_key": "nyc",
                    "month_day": "03-20",
                    "normal_tmax_c": 17.778,
                    "normal_tmin_c": 7.0,
                    "normal_tmax_f": 64.0,
                    "normal_tmin_f": 44.6,
                    "normals_period": "1991-2020",
                    "normals_source": "NORMAL_DLY",
                    "ingested_at": "2026-03-20T00:00:00+00:00",
                }
            ]
        ),
        "kalshi_markets.parquet": pd.DataFrame(
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
                    "expiration_ts": "2026-03-20T23:59:00Z",
                    "close_time": "2026-03-20T23:00:00Z",
                    "ingested_at": "2026-03-20T00:00:00+00:00",
                }
            ]
        ),
        "kalshi_candles.parquet": pd.DataFrame(
            [
                {
                    "market_ticker": "KXHIGHNY-26MAR20-B65",
                    "city_key": "nyc",
                    "candle_ts": "2026-03-20T14:00:00+00:00",
                    "open": 42,
                    "high": 45,
                    "low": 41,
                    "close": 44,
                    "volume": 11,
                    "interval": "1h",
                    "ingested_at": "2026-03-20T14:00:00+00:00",
                }
            ]
        ),
    }


def _write_single_city_config(tmp_path: Path) -> Path:
    return _write_cities_config(
        tmp_path,
        [
            {
                "city_key": "nyc",
                "city_name": "New York City",
                "timezone": "America/New_York",
                "kalshi_series_ticker": "KXHIGHNY",
                "enabled": True,
            }
        ],
    )


def _write_cities_config(tmp_path: Path, cities: list[dict]) -> Path:
    lines = ["cities:"]
    ordered_fields = [
        "city_key",
        "city_name",
        "timezone",
        "kalshi_series_ticker",
        "settlement_source_name",
        "settlement_source_url",
        "settlement_station_id",
        "settlement_station_name",
        "station_lat",
        "station_lon",
        "enabled",
    ]
    for city in cities:
        lines.append(f"  - city_key: {_yaml_scalar(city['city_key'])}")
        for field in ordered_fields[1:]:
            lines.append(f"    {field}: {_yaml_scalar(city.get(field))}")

    config_path = tmp_path / "cities.yml"
    config_path.write_text("\n".join(lines) + "\n", encoding="utf-8")
    return config_path


def _write_staged_placeholders(tmp_path: Path) -> Path:
    staging_dir = tmp_path / "staging"
    staging_dir.mkdir(parents=True, exist_ok=True)
    for filename in [
        "weather_daily.parquet",
        "weather_normals_daily.parquet",
        "kalshi_markets.parquet",
        "kalshi_candles.parquet",
    ]:
        (staging_dir / filename).write_text("placeholder", encoding="utf-8")
    return staging_dir


def _yaml_scalar(value: object) -> str:
    if value is None:
        return "null"
    if isinstance(value, bool):
        return "true" if value else "false"
    return str(value)
