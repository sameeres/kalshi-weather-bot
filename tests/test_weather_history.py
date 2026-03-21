from pathlib import Path

import pandas as pd
import pytest
from typer.testing import CliRunner

from kwb.cli import app
from kwb.ingestion.weather_history import ingest_weather_history_for_enabled_cities
from kwb.mapping.station_mapping import StationMappingValidationError


class FakeNCEIClient:
    def __init__(self, responses: dict[str, list[dict]]) -> None:
        self.responses = responses
        self.calls: list[tuple[str, str, str, str]] = []

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
        self.calls.append((station_id, start_date, end_date, datasetid))
        return {"results": self.responses.get(station_id, [])}


def test_weather_history_ingestion_succeeds_for_validated_city(
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
                "settlement_source_name": "National Weather Service",
                "settlement_source_url": "https://forecast.weather.gov/data/obhistory/KLGA.html",
                "settlement_station_id": "KLGA",
                "settlement_station_name": "LaGuardia Airport",
                "station_lat": 40.7769,
                "station_lon": -73.874,
                "enabled": True,
            }
        ],
    )
    client = FakeNCEIClient(
        {
            "KLGA": [
                {"date": "2026-03-20T00:00:00", "datatype": "TMAX", "value": 170, "datasetid": "GHCND"},
                {"date": "2026-03-20T00:00:00", "datatype": "TMIN", "value": 50, "datasetid": "GHCND"},
            ]
        }
    )
    output_dir = tmp_path / "staging"
    captured: dict[str, pd.DataFrame] = {}

    def fake_to_parquet(self: pd.DataFrame, path: Path, index: bool = False) -> None:
        captured["path"] = Path(path)
        captured["df"] = self.copy()

    monkeypatch.setattr(pd.DataFrame, "to_parquet", fake_to_parquet)

    outpath = ingest_weather_history_for_enabled_cities(
        start_date="2026-03-20",
        end_date="2026-03-20",
        config_path=config_path,
        output_dir=output_dir,
        client=client,
    )

    assert outpath == output_dir / "weather_daily.parquet"
    assert captured["path"] == outpath
    df = captured["df"]
    assert list(df.columns) == [
        "station_id",
        "city_key",
        "obs_date",
        "tmax_c",
        "tmin_c",
        "tmax_f",
        "tmin_f",
        "source_dataset",
        "ingested_at",
    ]
    assert df.loc[0, "station_id"] == "KLGA"
    assert df.loc[0, "city_key"] == "nyc"
    assert df.loc[0, "obs_date"] == "2026-03-20"


def test_weather_history_ingestion_fails_when_station_validation_fails(tmp_path: Path) -> None:
    config_path = _write_cities_config(
        tmp_path,
        [
            {
                "city_key": "nyc",
                "city_name": "New York City",
                "timezone": "America/New_York",
                "kalshi_series_ticker": "KXHIGHNY",
                "settlement_source_name": None,
                "settlement_source_url": None,
                "settlement_station_id": None,
                "settlement_station_name": None,
                "station_lat": None,
                "station_lon": None,
                "enabled": True,
            }
        ],
    )

    with pytest.raises(StationMappingValidationError):
        ingest_weather_history_for_enabled_cities(
            start_date="2026-03-20",
            end_date="2026-03-20",
            config_path=config_path,
            client=FakeNCEIClient({}),
        )


def test_weather_history_temperature_conversion_is_correct(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    config_path = _write_complete_city_config(tmp_path)
    client = FakeNCEIClient(
        {
            "KLGA": [
                {"date": "2026-03-20T00:00:00", "datatype": "TMAX", "value": 250, "datasetid": "GHCND"},
                {"date": "2026-03-20T00:00:00", "datatype": "TMIN", "value": 0, "datasetid": "GHCND"},
            ]
        }
    )
    captured: dict[str, pd.DataFrame] = {}

    monkeypatch.setattr(
        pd.DataFrame,
        "to_parquet",
        lambda self, path, index=False: captured.setdefault("df", self.copy()),
    )

    ingest_weather_history_for_enabled_cities(
        start_date="2026-03-20",
        end_date="2026-03-20",
        config_path=config_path,
        output_dir=tmp_path,
        client=client,
    )

    df = captured["df"]
    assert df.loc[0, "tmax_c"] == 25.0
    assert df.loc[0, "tmax_f"] == 77.0
    assert df.loc[0, "tmin_c"] == 0.0
    assert df.loc[0, "tmin_f"] == 32.0


def test_weather_history_duplicate_station_date_rows_are_collapsed(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    config_path = _write_complete_city_config(tmp_path)
    client = FakeNCEIClient(
        {
            "KLGA": [
                {"date": "2026-03-20T00:00:00", "datatype": "TMAX", "value": 200, "datasetid": "GHCND"},
                {"date": "2026-03-20T00:00:00", "datatype": "TMAX", "value": 210, "datasetid": "GHCND"},
                {"date": "2026-03-20T00:00:00", "datatype": "TMIN", "value": 100, "datasetid": "GHCND"},
            ]
        }
    )
    captured: dict[str, pd.DataFrame] = {}

    monkeypatch.setattr(
        pd.DataFrame,
        "to_parquet",
        lambda self, path, index=False: captured.setdefault("df", self.copy()),
    )

    ingest_weather_history_for_enabled_cities(
        start_date="2026-03-20",
        end_date="2026-03-20",
        config_path=config_path,
        output_dir=tmp_path,
        client=client,
    )

    df = captured["df"]
    assert len(df) == 1
    assert df.loc[0, "tmax_c"] == 21.0


def test_weather_history_cli_fails_cleanly_when_mapping_incomplete(tmp_path: Path) -> None:
    config_path = _write_cities_config(
        tmp_path,
        [
            {
                "city_key": "nyc",
                "city_name": "New York City",
                "timezone": "America/New_York",
                "kalshi_series_ticker": "KXHIGHNY",
                "settlement_source_name": None,
                "settlement_source_url": None,
                "settlement_station_id": None,
                "settlement_station_name": None,
                "station_lat": None,
                "station_lon": None,
                "enabled": True,
            }
        ],
    )

    runner = CliRunner()
    result = runner.invoke(
        app,
        [
            "weather",
            "history",
            "--start-date",
            "2026-03-20",
            "--end-date",
            "2026-03-20",
            "--config-path",
            str(config_path),
        ],
    )

    assert result.exit_code == 1
    assert "Weather history ingestion failed" in result.stdout


def _write_complete_city_config(tmp_path: Path) -> Path:
    return _write_cities_config(
        tmp_path,
        [
            {
                "city_key": "nyc",
                "city_name": "New York City",
                "timezone": "America/New_York",
                "kalshi_series_ticker": "KXHIGHNY",
                "settlement_source_name": "National Weather Service",
                "settlement_source_url": "https://forecast.weather.gov/data/obhistory/KLGA.html",
                "settlement_station_id": "KLGA",
                "settlement_station_name": "LaGuardia Airport",
                "station_lat": 40.7769,
                "station_lon": -73.874,
                "enabled": True,
            }
        ],
    )


def _write_cities_config(tmp_path: Path, cities: list[dict]) -> Path:
    payload = pd.DataFrame(cities).to_dict(orient="records")
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
    for city in payload:
        lines.append(f"  - city_key: {_yaml_scalar(city['city_key'])}")
        for field in ordered_fields[1:]:
            lines.append(f"    {field}: {_yaml_scalar(city.get(field))}")

    config_path = tmp_path / "cities.yml"
    config_path.write_text("\n".join(lines) + "\n", encoding="utf-8")
    return config_path


def _yaml_scalar(value: object) -> str:
    if value is None:
        return "null"
    if isinstance(value, bool):
        return "true" if value else "false"
    return str(value)
