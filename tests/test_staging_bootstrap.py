from pathlib import Path

import pandas as pd
from typer.testing import CliRunner

import kwb.cli as cli_module
from kwb.cli import app
from kwb.ingestion.build_staging import build_staging_datasets
from kwb.ingestion.validate_staging import (
    check_climatology_baseline_readiness,
    validate_staging_datasets,
)


class FakeNCEIClient:
    def get_daily_station_observations(
        self,
        station_id: str,
        start_date: str,
        end_date: str,
        datasetid: str = "GHCND",
        units: str = "metric",
        limit: int = 1000,
        offset: int = 1,
    ) -> dict:
        assert station_id == "GHCND:USW00014732"
        return {
            "results": [
                {"date": "2026-03-20T00:00:00", "datatype": "TMAX", "value": 210, "datasetid": datasetid},
                {"date": "2026-03-20T00:00:00", "datatype": "TMIN", "value": 100, "datasetid": datasetid},
            ]
        }

    def get_daily_climate_normals(
        self,
        station_id: str,
        start_date: str = "2010-01-01",
        end_date: str = "2010-12-31",
        datasetid: str = "NORMAL_DLY",
        datatypeids: list[str] | None = None,
        units: str = "metric",
        limit: int = 1000,
        offset: int = 1,
    ) -> dict:
        assert station_id == "GHCND:USW00014732"
        return {
            "results": [
                {"date": "2010-03-20T00:00:00", "datatype": "DLY-TMAX-NORMAL", "value": 180, "datasetid": datasetid},
                {"date": "2010-03-20T00:00:00", "datatype": "DLY-TMIN-NORMAL", "value": 80, "datasetid": datasetid},
            ]
        }


class FakeKalshiClient:
    def get_events(
        self,
        series_ticker: str | None = None,
        limit: int = 200,
        cursor: str | None = None,
        with_nested_markets: bool = False,
    ) -> dict:
        return {
            "events": [
                {"event_ticker": "KXHIGHNY-26MAR20", "strike_date": "2026-03-20T00:00:00Z"},
            ]
        }

    def list_markets(
        self,
        series_ticker: str | None = None,
        event_ticker: str | None = None,
        limit: int = 200,
        cursor: str | None = None,
    ) -> dict:
        return {
            "markets": [
                {
                    "event_ticker": "KXHIGHNY-26MAR20",
                    "ticker": "KXHIGHNY-26MAR20-B65",
                    "title": "65F to 69F",
                    "subtitle": "Daily high bucket",
                    "status": "active",
                    "floor_strike": 65,
                    "cap_strike": 69,
                    "strike_type": "between",
                    "expiration_ts": "2026-03-20T23:59:00Z",
                    "close_time": "2026-03-20T23:00:00Z",
                }
            ]
        }

    def get_market_candlesticks(
        self,
        series_ticker: str,
        market_ticker: str,
        start_ts: int,
        end_ts: int,
        period_interval: int,
        include_latest_before_start: bool = False,
    ) -> dict:
        return {
            "candlesticks": [
                {
                    "end_period_ts": 1774015200,
                    "open": 41,
                    "high": 45,
                    "low": 40,
                    "close": 44,
                    "volume": 120,
                }
            ]
        }


def test_build_staging_datasets_succeeds_from_minimal_inputs(tmp_path: Path) -> None:
    config_path = _write_complete_city_config(tmp_path)
    staging_dir = tmp_path / "staging"
    summary = build_staging_datasets(
        config_path=config_path,
        staging_dir=staging_dir,
        start_date="2026-03-20",
        end_date="2026-03-20",
        ncei_client=FakeNCEIClient(),
        kalshi_client=FakeKalshiClient(),
    )

    assert summary["success"] is True
    assert (staging_dir / "weather_daily.parquet").exists()
    assert (staging_dir / "weather_normals_daily.parquet").exists()
    assert (staging_dir / "kalshi_markets.parquet").exists()
    assert (staging_dir / "kalshi_candles.parquet").exists()
    assert (staging_dir / "staging_validation_summary.json").exists()
    assert (staging_dir / "staging_bootstrap_report.md").exists()


def test_validate_staging_passes_for_good_datasets(tmp_path: Path) -> None:
    config_path = _write_complete_city_config(tmp_path)
    staging_dir = _write_valid_staging_dir(tmp_path)
    summary = validate_staging_datasets(config_path=config_path, staging_dir=staging_dir)

    assert summary["ready"] is True
    assert summary["missing_datasets"] == []
    assert summary["invalid_datasets"] == []


def test_validate_staging_fails_for_missing_dataset(tmp_path: Path) -> None:
    config_path = _write_complete_city_config(tmp_path)
    staging_dir = tmp_path / "staging"
    staging_dir.mkdir(parents=True, exist_ok=True)

    summary = validate_staging_datasets(config_path=config_path, staging_dir=staging_dir)

    assert summary["ready"] is False
    assert "weather_daily" in summary["missing_datasets"]


def test_validate_staging_fails_for_malformed_schema(tmp_path: Path) -> None:
    config_path = _write_complete_city_config(tmp_path)
    staging_dir = tmp_path / "staging"
    staging_dir.mkdir(parents=True, exist_ok=True)
    pd.DataFrame([{"city_key": "nyc"}]).to_parquet(staging_dir / "weather_daily.parquet", index=False)
    _write_good_normals(staging_dir)
    _write_good_markets(staging_dir)
    _write_good_candles(staging_dir)
    summary = validate_staging_datasets(config_path=config_path, staging_dir=staging_dir)

    assert summary["ready"] is False
    assert "weather_daily" in summary["invalid_datasets"]


def test_readiness_check_reports_success_when_validation_passes(tmp_path: Path) -> None:
    config_path = _write_complete_city_config(tmp_path)
    staging_dir = _write_valid_staging_dir(tmp_path)
    readiness = check_climatology_baseline_readiness(config_path=config_path, staging_dir=staging_dir)

    assert readiness["ready"] is True
    assert readiness["missing_datasets"] == []
    assert readiness["invalid_datasets"] == []
    assert readiness["station_mapping"]["ready"] is True


def test_build_validate_and_readiness_cli_smoke(tmp_path: Path, monkeypatch) -> None:
    config_path = _write_complete_city_config(tmp_path)
    staging_dir = tmp_path / "staging"

    monkeypatch.setattr(
        cli_module,
        "build_staging_datasets",
        lambda **kwargs: {
            "success": True,
            "validation_summary_path": str(staging_dir / "staging_validation_summary.json"),
            "bootstrap_report_path": str(staging_dir / "staging_bootstrap_report.md"),
            "recommendation": "Run: kwb research run-climatology-baseline",
        },
    )
    monkeypatch.setattr(
        cli_module,
        "validate_staging_datasets",
        lambda **kwargs: {
            "ready": True,
            "station_mapping": {"ready": True},
            "upstream_environment": {"ncei_api_token_configured": True},
            "missing_datasets": [],
            "invalid_datasets": [],
            "summary_output_path": str(staging_dir / "staging_validation_summary.json"),
        },
    )
    monkeypatch.setattr(
        cli_module,
        "check_climatology_baseline_readiness",
        lambda **kwargs: {
            "ready": True,
            "station_mapping": {"ready": True},
            "upstream_environment": {"ncei_api_token_configured": True},
            "missing_datasets": [],
            "invalid_datasets": [],
            "validation_summary_path": str(staging_dir / "staging_validation_summary.json"),
            "recommendation": "Run: kwb research run-climatology-baseline",
        },
    )

    runner = CliRunner()
    result_build = runner.invoke(
        app,
        [
            "data",
            "build-staging",
            "--config-path",
            str(config_path),
            "--start-date",
            "2026-03-20",
            "--end-date",
            "2026-03-20",
            "--output-dir",
            str(staging_dir),
        ],
    )
    result_validate = runner.invoke(
        app,
        ["data", "validate-staging", "--config-path", str(config_path), "--staging-dir", str(staging_dir)],
    )
    result_ready = runner.invoke(
        app,
        [
            "research",
            "check-baseline-readiness",
            "--config-path",
            str(config_path),
            "--staging-dir",
            str(staging_dir),
        ],
    )

    assert result_build.exit_code == 0
    assert result_validate.exit_code == 0
    assert result_ready.exit_code == 0
    assert "Built staging datasets successfully" in result_build.stdout
    assert "Staging validation: ready" in result_validate.stdout
    assert "Baseline readiness: ready" in result_ready.stdout


def _write_valid_staging_dir(tmp_path: Path) -> Path:
    staging_dir = tmp_path / "staging"
    staging_dir.mkdir(parents=True, exist_ok=True)
    _write_good_weather(staging_dir)
    _write_good_normals(staging_dir)
    _write_good_markets(staging_dir)
    _write_good_candles(staging_dir)
    return staging_dir


def _write_good_weather(staging_dir: Path) -> None:
    pd.DataFrame(
        [
            {
                "station_id": "KLGA",
                "city_key": "nyc",
                "obs_date": "2026-03-20",
                "tmax_c": 21.0,
                "tmin_c": 10.0,
                "tmax_f": 69.8,
                "tmin_f": 50.0,
                "source_dataset": "GHCND",
                "ingested_at": "2026-03-20T00:00:00+00:00",
            }
        ]
    ).to_parquet(staging_dir / "weather_daily.parquet", index=False)


def _write_good_normals(staging_dir: Path) -> None:
    pd.DataFrame(
        [
            {
                "station_id": "KLGA",
                "city_key": "nyc",
                "month_day": "03-20",
                "normal_tmax_c": 18.0,
                "normal_tmin_c": 8.0,
                "normal_tmax_f": 64.4,
                "normal_tmin_f": 46.4,
                "normals_period": "1991-2020",
                "normals_source": "NORMAL_DLY",
                "ingested_at": "2026-03-20T00:00:00+00:00",
            }
        ]
    ).to_parquet(staging_dir / "weather_normals_daily.parquet", index=False)


def _write_good_markets(staging_dir: Path) -> None:
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
                "status": "active",
                "floor_strike": 65,
                "cap_strike": 69,
                "strike_type": "between",
                "expiration_ts": "2026-03-20T23:59:00Z",
                "close_time": "2026-03-20T23:00:00Z",
                "ingested_at": "2026-03-20T00:00:00+00:00",
            }
        ]
    ).to_parquet(staging_dir / "kalshi_markets.parquet", index=False)


def _write_good_candles(staging_dir: Path) -> None:
    pd.DataFrame(
        [
            {
                "market_ticker": "KXHIGHNY-26MAR20-B65",
                "city_key": "nyc",
                "candle_ts": "2026-03-20T14:00:00+00:00",
                "open": 41,
                "high": 45,
                "low": 40,
                "close": 44,
                "volume": 120,
                "interval": "1h",
                "ingested_at": "2026-03-20T14:00:00+00:00",
            }
        ]
    ).to_parquet(staging_dir / "kalshi_candles.parquet", index=False)


def _write_complete_city_config(tmp_path: Path) -> Path:
    path = tmp_path / "cities.yml"
    path.write_text(
        "\n".join(
            [
                "cities:",
                "  - city_key: nyc",
                "    city_name: New York City",
                "    timezone: America/New_York",
                "    kalshi_series_ticker: KXHIGHNY",
                "    settlement_source_name: National Weather Service",
                "    settlement_station_id: KLGA",
                "    settlement_station_name: LaGuardia Airport",
                "    settlement_source_url: https://forecast.weather.gov/data/obhistory/KLGA.html",
                "    station_lat: 40.7769",
                "    station_lon: -73.8740",
                "    enabled: true",
            ]
        ),
        encoding="utf-8",
    )
    return path

