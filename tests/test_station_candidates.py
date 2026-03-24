from pathlib import Path

import pandas as pd
import pytest
from typer.testing import CliRunner

from kwb.cli import app
from kwb.mapping.station_candidates import (
    apply_station_mapping_recommendations,
    build_station_mapping_report,
    resolve_enabled_city_station_candidates,
)


def test_station_mapping_report_shows_missing_fields_for_incomplete_city(tmp_path: Path) -> None:
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

    report = build_station_mapping_report(config_path=config_path)

    assert len(report) == 1
    assert report.loc[0, "mapping_complete"] == False
    assert report.loc[0, "validation_ready"] == True
    assert "settlement_station_id" in report.loc[0, "missing_fields"]


def test_station_mapping_report_includes_staged_source_context(
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
                }
            ]
        ),
    )

    report = build_station_mapping_report(config_path=config_path, events_path=events_path)

    assert report.loc[0, "staged_settlement_source_name"] == "National Weather Service"
    assert report.loc[0, "staged_settlement_source_url"] == "https://forecast.weather.gov/data/obhistory/KLGA.html"
    assert report.loc[0, "staged_source_status"] == "unique"
    assert report.loc[0, "recommended_station_id"] == "KNYC"
    assert report.loc[0, "recommendation_confidence"] >= 0.9
    assert "explicit_settlement_override" in report.loc[0, "recommendation_provenance"]


def test_station_mapping_report_never_marks_incomplete_city_as_validation_ready(tmp_path: Path) -> None:
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

    report = build_station_mapping_report(config_path=config_path)

    assert report.loc[0, "mapping_complete"] == False
    assert report.loc[0, "validation_ready"] == True


def test_station_resolution_uses_series_hint_when_events_are_missing(tmp_path: Path) -> None:
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

    resolution = resolve_enabled_city_station_candidates(config_path=config_path)

    assert resolution["results"][0]["selected_candidate"]["settlement_station_id"] == "KNYC"
    assert resolution["results"][0]["selected_candidate"]["settlement_station_name"] == "Central Park"
    assert resolution["results"][0]["selected_automatically"] is True
    assert "settlement-alignment override" in resolution["results"][0]["selected_candidate"]["selection_reason"]


def test_station_resolution_override_beats_staged_laguardia_source(
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
                }
            ]
        ),
    )

    resolution = resolve_enabled_city_station_candidates(config_path=config_path, events_path=events_path)

    selected = resolution["results"][0]["selected_candidate"]
    assert selected["settlement_station_id"] == "KNYC"
    assert selected["settlement_station_name"] == "Central Park"
    assert "explicit_settlement_override" in selected["provenance"]


def test_apply_station_mapping_recommendations_updates_config(tmp_path: Path) -> None:
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

    _, updates, _ = apply_station_mapping_recommendations(config_path=config_path)

    assert len(updates) == 1
    updated = Path(config_path).read_text(encoding="utf-8")
    assert "settlement_station_id: KNYC" in updated
    assert "settlement_station_name: Central Park" in updated
    assert "settlement_source_url: https://forecast.weather.gov/data/obhistory/KNYC.html" in updated


def test_station_mapping_report_cli_writes_csv(tmp_path: Path) -> None:
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
    output_path = tmp_path / "station_mapping_report.csv"

    runner = CliRunner()
    result = runner.invoke(
        app,
        [
            "station",
            "report",
            "--config-path",
            str(config_path),
            "--output-path",
            str(output_path),
        ],
    )

    assert result.exit_code == 0
    assert output_path.exists()
    assert "Saved report" in result.stdout

    report = pd.read_csv(output_path)
    assert report.loc[0, "city_key"] == "nyc"


def test_station_recommend_cli_can_write_config(tmp_path: Path) -> None:
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
    result = runner.invoke(app, ["station", "recommend", "--config-path", str(config_path), "--write-config"])

    assert result.exit_code == 0
    assert "config updates applied: 1" in result.stdout
    payload = Path(config_path).read_text(encoding="utf-8")
    assert "settlement_station_id: KNYC" in payload


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
