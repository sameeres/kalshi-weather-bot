import json
from pathlib import Path

import pandas as pd
import pytest

from kwb.ingestion.kalshi_events import ingest_enabled_city_events
from kwb.mapping.settlement_sources import extract_settlement_sources
from kwb.utils.io import read_yaml


class FakeKalshiClient:
    def __init__(self) -> None:
        self.event_calls: list[tuple[str | None, int, str | None, bool]] = []

    def get_events(
        self,
        series_ticker: str | None = None,
        limit: int = 200,
        cursor: str | None = None,
        with_nested_markets: bool = False,
    ) -> dict:
        self.event_calls.append((series_ticker, limit, cursor, with_nested_markets))
        if cursor is None:
            return {
                "events": [
                    {
                        "event_ticker": "KXHIGHNY-26MAR21",
                        "series_ticker": series_ticker,
                        "title": "NYC High on Mar 21",
                        "sub_title": "Temperature high",
                        "strike_date": "2026-03-21T00:00:00Z",
                        "strike_period": "day",
                    }
                ],
                "cursor": "page-2",
            }
        return {
            "events": [
                {
                    "event_ticker": "KXHIGHNY-26MAR22",
                    "series_ticker": series_ticker,
                    "title": "NYC High on Mar 22",
                    "sub_title": "Temperature high",
                    "strike_date": "2026-03-22T00:00:00Z",
                    "strike_period": "day",
                }
            ]
        }

    def get_event(self, event_ticker: str) -> dict:
        return {
            "event": {
                "event_ticker": event_ticker,
                "title": f"Detail {event_ticker}",
                "sub_title": "Detailed subtitle",
                "strike_date": "2026-03-21T00:00:00Z" if event_ticker.endswith("21") else "2026-03-22T00:00:00Z",
                "strike_period": "day",
                "last_updated_ts": "2026-03-20T12:00:00Z",
            }
        }

    def get_event_metadata(self, event_ticker: str) -> dict:
        return {
            "settlement_sources": [
                {
                    "name": "National Weather Service",
                    "url": "https://forecast.weather.gov/data/obhistory/KLGA.html",
                }
            ]
        }


def test_extract_settlement_sources_supports_metadata_payload() -> None:
    payload = {
        "metadata": {
            "settlement_sources": [
                {"name": "NOAA", "url": "https://example.com/noaa"},
            ]
        }
    }

    assert extract_settlement_sources(payload) == [
        {"name": "NOAA", "url": "https://example.com/noaa"}
    ]


def test_ingest_enabled_city_events_writes_staging_and_updates_config(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    config_path = tmp_path / "cities.yml"
    config_path.write_text(
        "\n".join(
            [
                "cities:",
                "  - city_key: nyc",
                "    city_name: New York City",
                "    timezone: America/New_York",
                "    kalshi_series_ticker: KXHIGHNY",
                "    settlement_source_name: null",
                "    settlement_station_id: null",
                "    settlement_station_name: null",
                "    settlement_source_url: null",
                "    station_lat: null",
                "    station_lon: null",
                "    enabled: true",
                "  - city_key: chicago",
                "    city_name: Chicago",
                "    timezone: America/Chicago",
                "    kalshi_series_ticker: KXHIGHCHI",
                "    settlement_source_name: null",
                "    settlement_station_id: null",
                "    settlement_station_name: null",
                "    settlement_source_url: null",
                "    station_lat: null",
                "    station_lon: null",
                "    enabled: false",
            ]
        ),
        encoding="utf-8",
    )
    output_dir = tmp_path / "staging"
    captured: dict[str, pd.DataFrame] = {}

    def fake_to_parquet(self: pd.DataFrame, path: Path, index: bool = False) -> None:
        captured["path"] = Path(path)
        captured["df"] = self.copy()

    monkeypatch.setattr(pd.DataFrame, "to_parquet", fake_to_parquet)

    outpath = ingest_enabled_city_events(
        config_path=config_path,
        output_dir=output_dir,
        client=FakeKalshiClient(),
        update_city_config=True,
    )

    assert outpath == output_dir / "kalshi_events.parquet"
    assert captured["path"] == outpath
    df = captured["df"]
    assert list(df["event_ticker"]) == ["KXHIGHNY-26MAR21", "KXHIGHNY-26MAR22"]
    assert set(df["settlement_source_name"]) == {"National Weather Service"}
    assert set(df["settlement_source_url"]) == {"https://forecast.weather.gov/data/obhistory/KLGA.html"}

    decoded_names = json.loads(df.loc[0, "settlement_source_names"])
    assert decoded_names == ["National Weather Service"]

    updated_config = read_yaml(config_path)
    nyc = updated_config["cities"][0]
    assert nyc["settlement_source_name"] == "National Weather Service"
    assert nyc["settlement_source_url"] == "https://forecast.weather.gov/data/obhistory/KLGA.html"
