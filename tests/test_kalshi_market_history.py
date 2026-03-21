from pathlib import Path

import pandas as pd
from typer.testing import CliRunner

import kwb.cli as cli_module
from kwb.cli import app
from kwb.ingestion.kalshi_market_history import ingest_kalshi_market_history_for_enabled_cities


class FakeKalshiHistoryClient:
    def __init__(self) -> None:
        self.event_calls: list[tuple[str | None, str | None]] = []
        self.market_calls: list[tuple[str | None, str | None]] = []
        self.candle_calls: list[tuple[str, str, int]] = []

    def get_events(
        self,
        series_ticker: str | None = None,
        limit: int = 200,
        cursor: str | None = None,
        with_nested_markets: bool = False,
    ) -> dict:
        self.event_calls.append((series_ticker, cursor))
        if series_ticker == "KXHIGHNY" and cursor is None:
            return {
                "events": [
                    {"event_ticker": "KXHIGHNY-26MAR20", "strike_date": "2026-03-20T00:00:00Z"},
                ],
                "cursor": "event-page-2",
            }
        if series_ticker == "KXHIGHNY" and cursor == "event-page-2":
            return {
                "events": [
                    {"event_ticker": "KXHIGHNY-26MAR21", "strike_date": "2026-03-21T00:00:00Z"},
                ]
            }
        return {"events": []}

    def list_markets(
        self,
        series_ticker: str | None = None,
        event_ticker: str | None = None,
        limit: int = 200,
        cursor: str | None = None,
    ) -> dict:
        self.market_calls.append((series_ticker, cursor))
        if series_ticker == "KXHIGHNY" and cursor is None:
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
                ],
                "cursor": "market-page-2",
            }
        if series_ticker == "KXHIGHNY" and cursor == "market-page-2":
            return {
                "markets": [
                    {
                        "event_ticker": "KXHIGHNY-26MAR21",
                        "ticker": "KXHIGHNY-26MAR21-B70",
                        "title": "70F to 74F",
                        "subtitle": "Daily high bucket",
                        "status": "settled",
                        "floor_strike": 70,
                        "cap_strike": 74,
                        "strike_type": "between",
                        "expiration_ts": "2026-03-21T23:59:00Z",
                        "close_time": "2026-03-21T23:00:00Z",
                    }
                ]
            }
        return {"markets": []}

    def get_market_candlesticks(
        self,
        series_ticker: str,
        market_ticker: str,
        start_ts: int,
        end_ts: int,
        period_interval: int,
        include_latest_before_start: bool = False,
    ) -> dict:
        self.candle_calls.append((series_ticker, market_ticker, period_interval))
        return {
            "candlesticks": [
                {
                    "end_period_ts": 1773968400,
                    "open": 41,
                    "high": 45,
                    "low": 40,
                    "close": 44,
                    "volume": 120,
                },
                {
                    "end_period_ts": 1773968400,
                    "open": 42,
                    "high": 46,
                    "low": 41,
                    "close": 45,
                    "volume": 121,
                },
            ]
        }


def test_kalshi_market_history_ingestion_succeeds_for_enabled_series(
    tmp_path: Path,
    monkeypatch,
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
            },
            {
                "city_key": "chicago",
                "city_name": "Chicago",
                "timezone": "America/Chicago",
                "kalshi_series_ticker": "KXHIGHCHI",
                "enabled": False,
            },
        ],
    )
    client = FakeKalshiHistoryClient()
    captured: dict[str, pd.DataFrame] = {}

    def fake_to_parquet(self: pd.DataFrame, path: Path, index: bool = False) -> None:
        captured[Path(path).name] = self.copy()

    monkeypatch.setattr(pd.DataFrame, "to_parquet", fake_to_parquet)

    markets_path, candles_path = ingest_kalshi_market_history_for_enabled_cities(
        start_date="2026-03-20",
        end_date="2026-03-21",
        interval="1h",
        config_path=config_path,
        output_dir=tmp_path,
        client=client,
    )

    assert markets_path.name == "kalshi_markets.parquet"
    assert candles_path.name == "kalshi_candles.parquet"
    assert set(captured) == {"kalshi_markets.parquet", "kalshi_candles.parquet"}
    assert {call[0] for call in client.event_calls} == {"KXHIGHNY"}
    assert {call[0] for call in client.market_calls} == {"KXHIGHNY"}


def test_kalshi_market_history_required_columns_are_present(
    tmp_path: Path,
    monkeypatch,
) -> None:
    config_path = _write_single_enabled_city(tmp_path)
    client = FakeKalshiHistoryClient()
    captured: dict[str, pd.DataFrame] = {}
    monkeypatch.setattr(pd.DataFrame, "to_parquet", lambda self, path, index=False: captured.setdefault(Path(path).name, self.copy()))

    ingest_kalshi_market_history_for_enabled_cities(
        start_date="2026-03-20",
        end_date="2026-03-21",
        interval="1h",
        config_path=config_path,
        output_dir=tmp_path,
        client=client,
    )

    markets_df = captured["kalshi_markets.parquet"]
    candles_df = captured["kalshi_candles.parquet"]
    assert list(markets_df.columns) == [
        "city_key",
        "series_ticker",
        "event_ticker",
        "market_ticker",
        "strike_date",
        "market_title",
        "market_subtitle",
        "status",
        "floor_strike",
        "cap_strike",
        "strike_type",
        "expiration_ts",
        "close_time",
        "ingested_at",
    ]
    assert list(candles_df.columns) == [
        "market_ticker",
        "city_key",
        "candle_ts",
        "open",
        "high",
        "low",
        "close",
        "volume",
        "interval",
        "ingested_at",
    ]


def test_kalshi_market_history_candle_rows_normalize_correctly(
    tmp_path: Path,
    monkeypatch,
) -> None:
    config_path = _write_single_enabled_city(tmp_path)
    client = FakeKalshiHistoryClient()
    captured: dict[str, pd.DataFrame] = {}
    monkeypatch.setattr(pd.DataFrame, "to_parquet", lambda self, path, index=False: captured.setdefault(Path(path).name, self.copy()))

    ingest_kalshi_market_history_for_enabled_cities(
        start_date="2026-03-20",
        end_date="2026-03-21",
        interval="1h",
        config_path=config_path,
        output_dir=tmp_path,
        client=client,
    )

    candles_df = captured["kalshi_candles.parquet"]
    assert len(candles_df) == 2
    assert candles_df.loc[0, "candle_ts"].endswith("+00:00")
    assert candles_df.loc[0, "interval"] == "1h"


def test_kalshi_market_history_paginates_events_and_markets(
    tmp_path: Path,
    monkeypatch,
) -> None:
    config_path = _write_single_enabled_city(tmp_path)
    client = FakeKalshiHistoryClient()
    captured: dict[str, pd.DataFrame] = {}
    monkeypatch.setattr(pd.DataFrame, "to_parquet", lambda self, path, index=False: captured.setdefault(Path(path).name, self.copy()))

    ingest_kalshi_market_history_for_enabled_cities(
        start_date="2026-03-20",
        end_date="2026-03-21",
        interval="1h",
        config_path=config_path,
        output_dir=tmp_path,
        client=client,
    )

    assert ("KXHIGHNY", None) in client.event_calls
    assert ("KXHIGHNY", "event-page-2") in client.event_calls
    assert ("KXHIGHNY", None) in client.market_calls
    assert ("KXHIGHNY", "market-page-2") in client.market_calls
    markets_df = captured["kalshi_markets.parquet"]
    assert len(markets_df) == 2


def test_kalshi_market_history_duplicate_candles_are_handled_sensibly(
    tmp_path: Path,
    monkeypatch,
) -> None:
    config_path = _write_single_enabled_city(tmp_path)
    client = FakeKalshiHistoryClient()
    captured: dict[str, pd.DataFrame] = {}
    monkeypatch.setattr(pd.DataFrame, "to_parquet", lambda self, path, index=False: captured.setdefault(Path(path).name, self.copy()))

    ingest_kalshi_market_history_for_enabled_cities(
        start_date="2026-03-20",
        end_date="2026-03-21",
        interval="1h",
        config_path=config_path,
        output_dir=tmp_path,
        client=client,
    )

    candles_df = captured["kalshi_candles.parquet"]
    market_slice = candles_df.loc[candles_df["market_ticker"] == "KXHIGHNY-26MAR20-B65"]
    assert len(market_slice) == 1
    assert market_slice.iloc[0]["close"] == 45


def test_kalshi_history_cli_smoke(tmp_path: Path, monkeypatch) -> None:
    config_path = _write_single_enabled_city(tmp_path)

    def fake_ingest(**kwargs):
        assert kwargs["start_date"] == "2026-03-20"
        assert kwargs["end_date"] == "2026-03-21"
        assert kwargs["interval"] == "1h"
        return tmp_path / "kalshi_markets.parquet", tmp_path / "kalshi_candles.parquet"

    monkeypatch.setattr(cli_module, "ingest_kalshi_market_history_for_enabled_cities", fake_ingest)

    runner = CliRunner()
    result = runner.invoke(
        app,
        [
            "kalshi",
            "history",
            "--start-date",
            "2026-03-20",
            "--end-date",
            "2026-03-21",
            "--config-path",
            str(config_path),
        ],
    )

    assert result.exit_code == 0
    assert "Saved Kalshi history" in result.stdout


def _write_single_enabled_city(tmp_path: Path) -> Path:
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
        "settlement_station_id",
        "settlement_station_name",
        "settlement_source_url",
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
