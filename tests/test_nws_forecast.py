from __future__ import annotations

from datetime import datetime, timezone
from pathlib import Path

import pandas as pd

from kwb.ingestion.nws_forecast import fetch_nws_forecast_snapshots


class _FakeNWSClient:
    def get_points(self, latitude: float, longitude: float) -> dict:
        return {
            "@id": f"https://api.weather.gov/points/{latitude},{longitude}",
            "properties": {
                "forecastHourly": f"https://api.weather.gov/gridpoints/TEST/{latitude},{longitude}/forecast/hourly"
            },
        }

    def get_json_url(self, url: str) -> dict:
        return {
            "properties": {
                "updated": "2026-04-02T08:00:00+00:00",
                "generatedAt": "2026-04-02T07:55:00+00:00",
                "periods": [
                    {
                        "number": 1,
                        "name": "This Morning",
                        "startTime": "2026-04-02T10:00:00-04:00",
                        "endTime": "2026-04-02T11:00:00-04:00",
                        "temperature": 68,
                        "temperatureUnit": "F",
                        "isDaytime": True,
                        "shortForecast": "Sunny",
                    },
                    {
                        "number": 2,
                        "name": "Late Morning",
                        "startTime": "2026-04-02T11:00:00-04:00",
                        "endTime": "2026-04-02T12:00:00-04:00",
                        "temperature": 20,
                        "temperatureUnit": "C",
                        "isDaytime": True,
                        "shortForecast": "Warm",
                    },
                ],
            }
        }


def test_fetch_nws_forecast_snapshots_normalizes_hourly_rows(tmp_path: Path) -> None:
    config_path = tmp_path / "cities.yml"
    config_path.write_text(
        """
cities:
  - city_key: nyc
    city_name: New York City
    timezone: America/New_York
    kalshi_series_ticker: KXHIGHNY
    settlement_station_id: KNYC
    settlement_station_name: Central Park
    station_lat: 40.7789
    station_lon: -73.9692
    enabled: true
""".strip()
        + "\n",
        encoding="utf-8",
    )

    outpath = fetch_nws_forecast_snapshots(
        config_path=config_path,
        output_dir=tmp_path,
        client=_FakeNWSClient(),
        append=False,
        snapshot_ts=datetime(2026, 4, 2, 14, 0, tzinfo=timezone.utc),
    )

    df = pd.read_parquet(outpath)
    assert len(df) == 2
    assert list(df["period_date_local"]) == ["2026-04-02", "2026-04-02"]
    assert list(df["temperature_f"]) == [68.0, 68.0]
    assert set(df["settlement_station_id"]) == {"KNYC"}
