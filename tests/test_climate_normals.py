from pathlib import Path

import pandas as pd
import pytest
from typer.testing import CliRunner

import kwb.cli as cli_module
from kwb.cli import app
from kwb.ingestion.climate_normals import ingest_climate_normals_for_enabled_cities
from kwb.mapping.station_mapping import StationMappingValidationError


class FakeNCEINormalsClient:
    def __init__(self, responses: dict[str, list[dict]]) -> None:
        self.responses = responses
        self.calls: list[tuple[str, str, str, str, tuple[str, ...]]] = []

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
        self.calls.append((station_id, start_date, end_date, datasetid, tuple(datatypeids or [])))
        return {"results": self.responses.get(station_id, [])}


def test_climate_normals_ingestion_succeeds_for_validated_city(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    config_path = _write_complete_city_config(tmp_path)
    client = FakeNCEINormalsClient(
        {
            "GHCND:USW00014732": [
                {"date": "2010-03-20T00:00:00", "datatype": "DLY-TMAX-NORMAL", "value": 111, "datasetid": "NORMAL_DLY"},
                {"date": "2010-03-20T00:00:00", "datatype": "DLY-TMIN-NORMAL", "value": 22, "datasetid": "NORMAL_DLY"},
            ]
        }
    )
    captured: dict[str, Path | pd.DataFrame] = {}

    def fake_to_parquet(self: pd.DataFrame, path: Path, index: bool = False) -> None:
        captured["path"] = Path(path)
        captured["df"] = self.copy()

    monkeypatch.setattr(pd.DataFrame, "to_parquet", fake_to_parquet)

    outpath, row_count, station_count = ingest_climate_normals_for_enabled_cities(
        config_path=config_path,
        output_dir=tmp_path,
        client=client,
    )

    assert outpath == tmp_path / "weather_normals_daily.parquet"
    assert row_count == 1
    assert station_count == 1
    assert captured["path"] == outpath
    assert client.calls == [
        ("GHCND:USW00014732", "2010-01-01", "2010-12-31", "NORMAL_DLY", ("DLY-TMAX-NORMAL", "DLY-TMIN-NORMAL"))
    ]

    df = captured["df"]
    assert list(df.columns) == [
        "station_id",
        "city_key",
        "month_day",
        "normal_tmax_c",
        "normal_tmin_c",
        "normal_tmax_f",
        "normal_tmin_f",
        "normals_period",
        "normals_source",
        "ingested_at",
    ]
    assert df.loc[0, "station_id"] == "KLGA"
    assert df.loc[0, "city_key"] == "nyc"
    assert df.loc[0, "month_day"] == "03-20"


def test_climate_normals_ingestion_fails_when_station_validation_fails(tmp_path: Path) -> None:
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
        ingest_climate_normals_for_enabled_cities(
            config_path=config_path,
            client=FakeNCEINormalsClient({}),
        )


def test_climate_normals_temperature_conversion_is_correct(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    config_path = _write_complete_city_config(tmp_path)
    client = FakeNCEINormalsClient(
        {
            "GHCND:USW00014732": [
                {"date": "2010-03-20T00:00:00", "datatype": "DLY-TMAX-NORMAL", "value": 250, "datasetid": "NORMAL_DLY"},
                {"date": "2010-03-20T00:00:00", "datatype": "DLY-TMIN-NORMAL", "value": 0, "datasetid": "NORMAL_DLY"},
            ]
        }
    )
    captured: dict[str, pd.DataFrame] = {}
    monkeypatch.setattr(
        pd.DataFrame,
        "to_parquet",
        lambda self, path, index=False: captured.setdefault("df", self.copy()),
    )

    ingest_climate_normals_for_enabled_cities(
        config_path=config_path,
        output_dir=tmp_path,
        client=client,
    )

    df = captured["df"]
    assert df.loc[0, "normal_tmax_c"] == 25.0
    assert df.loc[0, "normal_tmax_f"] == 77.0
    assert df.loc[0, "normal_tmin_c"] == 0.0
    assert df.loc[0, "normal_tmin_f"] == 32.0


def test_climate_normals_duplicate_station_month_day_rows_are_collapsed(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    config_path = _write_complete_city_config(tmp_path)
    client = FakeNCEINormalsClient(
        {
            "GHCND:USW00014732": [
                {"date": "2010-03-20T00:00:00", "datatype": "DLY-TMAX-NORMAL", "value": 200, "datasetid": "NORMAL_DLY"},
                {"date": "2010-03-20T00:00:00", "datatype": "DLY-TMAX-NORMAL", "value": 210, "datasetid": "NORMAL_DLY"},
                {"date": "2010-03-20T00:00:00", "datatype": "DLY-TMIN-NORMAL", "value": 100, "datasetid": "NORMAL_DLY"},
            ]
        }
    )
    captured: dict[str, pd.DataFrame] = {}
    monkeypatch.setattr(
        pd.DataFrame,
        "to_parquet",
        lambda self, path, index=False: captured.setdefault("df", self.copy()),
    )

    ingest_climate_normals_for_enabled_cities(
        config_path=config_path,
        output_dir=tmp_path,
        client=client,
    )

    df = captured["df"]
    assert len(df) == 1
    assert df.loc[0, "normal_tmax_c"] == 21.0


def test_climate_normals_month_day_normalizes_correctly(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    config_path = _write_complete_city_config(tmp_path)
    client = FakeNCEINormalsClient(
        {
            "GHCND:USW00014732": [
                {"date": "2010-01-05T00:00:00", "datatype": "DLY-TMAX-NORMAL", "value": 50, "datasetid": "NORMAL_DLY"},
                {"date": "2010-01-05T00:00:00", "datatype": "DLY-TMIN-NORMAL", "value": -10, "datasetid": "NORMAL_DLY"},
            ]
        }
    )
    captured: dict[str, pd.DataFrame] = {}
    monkeypatch.setattr(
        pd.DataFrame,
        "to_parquet",
        lambda self, path, index=False: captured.setdefault("df", self.copy()),
    )

    ingest_climate_normals_for_enabled_cities(
        config_path=config_path,
        output_dir=tmp_path,
        client=client,
    )

    df = captured["df"]
    assert df.loc[0, "month_day"] == "01-05"


def test_climate_normals_cli_fails_cleanly_when_mapping_incomplete(tmp_path: Path) -> None:
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
    result = runner.invoke(app, ["weather", "normals", "--config-path", str(config_path)])

    assert result.exit_code == 1
    assert "Climate normals ingestion failed" in result.stdout


def test_climate_normals_cli_smoke(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    config_path = _write_complete_city_config(tmp_path)

    def fake_ingest(**kwargs):
        assert kwargs["config_path"] == config_path
        return tmp_path / "weather_normals_daily.parquet", 366, 1

    monkeypatch.setattr(cli_module, "ingest_climate_normals_for_enabled_cities", fake_ingest)

    runner = CliRunner()
    result = runner.invoke(app, ["weather", "normals", "--config-path", str(config_path)])

    assert result.exit_code == 0
    assert "Saved climate normals" in result.stdout
    assert "366" in result.stdout
    assert "stations: 1" in result.stdout


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


def _yaml_scalar(value: object) -> str:
    if value is None:
        return "null"
    if isinstance(value, bool):
        return "true" if value else "false"
    return str(value)
