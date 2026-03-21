from pathlib import Path

import pandas as pd
import pytest
from typer.testing import CliRunner

from kwb.cli import app
from kwb.mapping.station_mapping import (
    StationMappingValidationError,
    validate_enabled_city_mappings,
)


def test_validate_enabled_city_mappings_passes_for_complete_city(tmp_path: Path) -> None:
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

    cities = validate_enabled_city_mappings(config_path=config_path)

    assert len(cities) == 1
    assert cities[0]["city_key"] == "nyc"


def test_validate_enabled_city_mappings_fails_when_station_id_missing(tmp_path: Path) -> None:
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
                "settlement_station_id": None,
                "settlement_station_name": "LaGuardia Airport",
                "station_lat": 40.7769,
                "station_lon": -73.874,
                "enabled": True,
            }
        ],
    )

    with pytest.raises(StationMappingValidationError, match="settlement_station_id"):
        validate_enabled_city_mappings(config_path=config_path)


def test_validate_enabled_city_mappings_fails_when_coordinates_missing(tmp_path: Path) -> None:
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
                "station_lat": None,
                "station_lon": None,
                "enabled": True,
            }
        ],
    )

    with pytest.raises(StationMappingValidationError, match="station_lat"):
        validate_enabled_city_mappings(config_path=config_path)


def test_validate_enabled_city_mappings_fails_for_duplicate_enabled_city_keys(tmp_path: Path) -> None:
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
            },
            {
                "city_key": "nyc",
                "city_name": "New York City Duplicate",
                "timezone": "America/New_York",
                "kalshi_series_ticker": "KXHIGHNY2",
                "settlement_source_name": "National Weather Service",
                "settlement_source_url": "https://forecast.weather.gov/data/obhistory/KLGA.html",
                "settlement_station_id": "KLGA",
                "settlement_station_name": "LaGuardia Airport",
                "station_lat": 40.7769,
                "station_lon": -73.874,
                "enabled": True,
            },
        ],
    )

    with pytest.raises(StationMappingValidationError, match="Duplicate enabled city_key"):
        validate_enabled_city_mappings(config_path=config_path)


def test_validate_enabled_city_mappings_fails_for_ambiguous_staged_sources(
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
    events_path = tmp_path / "kalshi_events.parquet"
    events_path.write_text("placeholder", encoding="utf-8")

    monkeypatch.setattr(
        pd,
        "read_parquet",
        lambda _: pd.DataFrame(
            [
                {
                    "city_key": "nyc",
                    "settlement_source_name": "National Weather Service",
                    "settlement_source_url": "https://forecast.weather.gov/data/obhistory/KLGA.html",
                },
                {
                    "city_key": "nyc",
                    "settlement_source_name": "NOAA",
                    "settlement_source_url": "https://example.com/noaa",
                },
            ]
        ),
    )

    with pytest.raises(StationMappingValidationError, match="ambiguous staged settlement sources"):
        validate_enabled_city_mappings(config_path=config_path, events_path=events_path)


def test_station_validate_cli_passes_for_complete_city(tmp_path: Path) -> None:
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

    runner = CliRunner()
    result = runner.invoke(app, ["station", "validate", "--config-path", str(config_path)])

    assert result.exit_code == 0
    assert "Station mapping validation passed" in result.stdout


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
