import json
from pathlib import Path

import pandas as pd
import requests
from typer.testing import CliRunner

import kwb.cli as cli_module
from kwb.cli import app
from kwb.clients.kalshi import KalshiClient
from kwb.ingestion.kalshi_market_history import (
    DEFAULT_KALSHI_HISTORY_MANIFEST_FILENAME,
    KalshiHistoryIngestionError,
    describe_local_quote_history_capabilities,
    ingest_kalshi_market_history_for_enabled_cities,
    summarize_kalshi_history_manifest,
)


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


class FlakyKalshiHistoryClient(FakeKalshiHistoryClient):
    def __init__(self) -> None:
        super().__init__()
        self.failed_once = False

    def get_market_candlesticks(
        self,
        series_ticker: str,
        market_ticker: str,
        start_ts: int,
        end_ts: int,
        period_interval: int,
        include_latest_before_start: bool = False,
    ) -> dict:
        if not self.failed_once and market_ticker == "KXHIGHNY-26MAR21-B70":
            self.failed_once = True
            response = requests.Response()
            response.status_code = 429
            raise requests.HTTPError("429 Client Error: Too Many Requests", response=response)
        return super().get_market_candlesticks(
            series_ticker=series_ticker,
            market_ticker=market_ticker,
            start_ts=start_ts,
            end_ts=end_ts,
            period_interval=period_interval,
            include_latest_before_start=include_latest_before_start,
        )


class MixedUniverseKalshiHistoryClient(FakeKalshiHistoryClient):
    def get_events(
        self,
        series_ticker: str | None = None,
        limit: int = 200,
        cursor: str | None = None,
        with_nested_markets: bool = False,
    ) -> dict:
        self.event_calls.append((series_ticker, cursor))
        return {
            "events": [
                {"event_ticker": "HIGHNY-24AUG15"},
                {"event_ticker": "KXHIGHNY-25DEC01"},
                {"event_ticker": "KXHIGHNY-26APR01"},
            ]
        }

    def list_markets(
        self,
        series_ticker: str | None = None,
        event_ticker: str | None = None,
        limit: int = 200,
        cursor: str | None = None,
    ) -> dict:
        self.market_calls.append((series_ticker, cursor))
        return {
            "markets": [
                {
                    "event_ticker": "HIGHNY-24AUG15",
                    "ticker": "HIGHNY-24AUG15-T80",
                    "title": "legacy",
                    "subtitle": ">80",
                    "status": "settled",
                    "strike_type": "greater",
                    "close_time": "2024-08-16T03:59:00Z",
                },
                {
                    "event_ticker": "KXHIGHNY-25DEC01",
                    "ticker": "KXHIGHNY-25DEC01-B40.5",
                    "title": "in window",
                    "subtitle": "40 to 41",
                    "status": "settled",
                    "strike_type": "between",
                    "close_time": "2025-12-02T04:59:00Z",
                },
                {
                    "event_ticker": "KXHIGHNY-26APR01",
                    "ticker": "KXHIGHNY-26APR01-B60.5",
                    "title": "out of window",
                    "subtitle": "60 to 61",
                    "status": "active",
                    "strike_type": "between",
                    "close_time": "2026-04-02T03:59:00Z",
                },
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
        self.candle_calls.append((series_ticker, market_ticker, period_interval))
        return {
            "candlesticks": [
                {
                    "end_period_ts": 1764637200,
                    "open": 40,
                    "high": 42,
                    "low": 39,
                    "close": 41,
                    "volume": 12,
                }
            ]
        }


class EmptyCandleKalshiHistoryClient(FakeKalshiHistoryClient):
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
        return {"candlesticks": []}


class NestedPriceKalshiHistoryClient(FakeKalshiHistoryClient):
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
                    "price": {
                        "open_dollars": "0.4200",
                        "close_dollars": "0.4400",
                        "high_dollars": "0.4700",
                        "low_dollars": "0.4100",
                    },
                    "yes_bid": {
                        "open_dollars": "0.3900",
                        "close_dollars": "0.4100",
                        "high_dollars": "0.4100",
                        "low_dollars": "0.3900",
                    },
                    "yes_ask": {
                        "open_dollars": "0.4500",
                        "close_dollars": "0.4700",
                        "high_dollars": "0.4700",
                        "low_dollars": "0.4500",
                    },
                    "volume_fp": "11.00",
                }
            ]
        }


def test_kalshi_market_history_ingestion_succeeds_for_enabled_series(tmp_path: Path) -> None:
    config_path = _write_single_enabled_city(tmp_path)
    client = FakeKalshiHistoryClient()

    markets_path, candles_path, details = ingest_kalshi_market_history_for_enabled_cities(
        start_date="2026-03-20",
        end_date="2026-03-21",
        interval="1h",
        config_path=config_path,
        output_dir=tmp_path,
        client=client,
        return_details=True,
    )

    markets_df = pd.read_parquet(markets_path)
    candles_df = pd.read_parquet(candles_path)
    assert details["final_outputs_written"] is True
    assert markets_path.name == "kalshi_markets.parquet"
    assert candles_path.name == "kalshi_candles.parquet"
    assert {call[0] for call in client.event_calls} == {"KXHIGHNY"}
    assert {call[0] for call in client.market_calls} == {"KXHIGHNY"}
    assert len(markets_df) == 2
    assert len(candles_df) == 2


def test_kalshi_market_history_required_columns_are_present(tmp_path: Path) -> None:
    config_path = _write_single_enabled_city(tmp_path)
    client = FakeKalshiHistoryClient()

    markets_path, candles_path = ingest_kalshi_market_history_for_enabled_cities(
        start_date="2026-03-20",
        end_date="2026-03-21",
        interval="1h",
        config_path=config_path,
        output_dir=tmp_path,
        client=client,
    )

    markets_df = pd.read_parquet(markets_path)
    candles_df = pd.read_parquet(candles_path)
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


def test_kalshi_market_history_candle_rows_normalize_correctly(tmp_path: Path) -> None:
    config_path = _write_single_enabled_city(tmp_path)
    client = FakeKalshiHistoryClient()

    _, candles_path = ingest_kalshi_market_history_for_enabled_cities(
        start_date="2026-03-20",
        end_date="2026-03-21",
        interval="1h",
        config_path=config_path,
        output_dir=tmp_path,
        client=client,
    )

    candles_df = pd.read_parquet(candles_path)
    assert len(candles_df) == 2
    assert candles_df.loc[0, "candle_ts"].endswith("+00:00")
    assert candles_df.loc[0, "interval"] == "1h"


def test_local_quote_history_capabilities_are_explicit() -> None:
    capabilities = describe_local_quote_history_capabilities()

    assert capabilities["has_true_historical_best_bid_ask"] is False
    assert capabilities["has_candle_history"] is True
    assert capabilities["best_local_quote_source"] == "candlestick_ohlcv"


def test_kalshi_market_history_paginates_events_and_markets(tmp_path: Path) -> None:
    config_path = _write_single_enabled_city(tmp_path)
    client = FakeKalshiHistoryClient()

    markets_path, _ = ingest_kalshi_market_history_for_enabled_cities(
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
    markets_df = pd.read_parquet(markets_path)
    assert len(markets_df) == 2


def test_kalshi_market_history_duplicate_candles_are_handled_sensibly(tmp_path: Path) -> None:
    config_path = _write_single_enabled_city(tmp_path)
    client = FakeKalshiHistoryClient()

    _, candles_path = ingest_kalshi_market_history_for_enabled_cities(
        start_date="2026-03-20",
        end_date="2026-03-21",
        interval="1h",
        config_path=config_path,
        output_dir=tmp_path,
        client=client,
    )

    candles_df = pd.read_parquet(candles_path)
    market_slice = candles_df.loc[candles_df["market_ticker"] == "KXHIGHNY-26MAR20-B65"]
    assert len(market_slice) == 1
    assert market_slice.iloc[0]["close"] == 45


def test_kalshi_market_history_persists_partial_progress_and_resumes(tmp_path: Path) -> None:
    config_path = _write_single_enabled_city(tmp_path)
    flaky_client = FlakyKalshiHistoryClient()

    try:
        ingest_kalshi_market_history_for_enabled_cities(
            start_date="2026-03-20",
            end_date="2026-03-21",
            interval="1h",
            config_path=config_path,
            output_dir=tmp_path,
            client=flaky_client,
        )
    except KalshiHistoryIngestionError as exc:
        details = exc.details
    else:  # pragma: no cover
        raise AssertionError("Expected KalshiHistoryIngestionError")

    assert details["resume_recommended"] is True
    assert details["completed_market_chunks"] == 1
    assert details["completed_candle_chunks"] == 1
    assert details["failed_candle_chunks"] == 1
    assert not (tmp_path / "kalshi_markets.parquet").exists()
    assert not (tmp_path / "kalshi_candles.parquet").exists()
    assert (tmp_path / DEFAULT_KALSHI_HISTORY_MANIFEST_FILENAME).exists()

    resumed_client = FakeKalshiHistoryClient()
    markets_path, candles_path, resumed_details = ingest_kalshi_market_history_for_enabled_cities(
        start_date="2026-03-20",
        end_date="2026-03-21",
        interval="1h",
        config_path=config_path,
        output_dir=tmp_path,
        client=resumed_client,
        resume=True,
        return_details=True,
    )

    assert resumed_details["final_outputs_written"] is True
    assert sum(call[1] == "KXHIGHNY-26MAR20-B65" for call in resumed_client.candle_calls) == 0
    assert sum(call[1] == "KXHIGHNY-26MAR21-B70" for call in resumed_client.candle_calls) == 1
    assert pd.read_parquet(markets_path).shape[0] == 2
    assert pd.read_parquet(candles_path).shape[0] == 2


def test_kalshi_market_history_filters_to_supported_ticker_era_and_date_window(tmp_path: Path) -> None:
    config_path = _write_single_enabled_city(tmp_path)
    client = MixedUniverseKalshiHistoryClient()

    markets_path, candles_path = ingest_kalshi_market_history_for_enabled_cities(
        start_date="2025-12-01",
        end_date="2025-12-01",
        interval="1h",
        config_path=config_path,
        output_dir=tmp_path,
        client=client,
    )

    markets_df = pd.read_parquet(markets_path)
    candles_df = pd.read_parquet(candles_path)
    assert list(markets_df["market_ticker"]) == ["KXHIGHNY-25DEC01-B40.5"]
    assert list(candles_df["market_ticker"].unique()) == ["KXHIGHNY-25DEC01-B40.5"]
    assert markets_df.loc[0, "strike_date"] == "2025-12-01T00:00:00Z"


def test_kalshi_market_history_uses_close_time_to_normalize_event_date(tmp_path: Path) -> None:
    config_path = _write_single_enabled_city(tmp_path)
    client = MixedUniverseKalshiHistoryClient()

    markets_path, _ = ingest_kalshi_market_history_for_enabled_cities(
        start_date="2025-12-01",
        end_date="2025-12-01",
        interval="1h",
        config_path=config_path,
        output_dir=tmp_path,
        client=client,
    )

    markets_df = pd.read_parquet(markets_path)
    assert markets_df.loc[0, "strike_date"] == "2025-12-01T00:00:00Z"


def test_zero_row_candle_chunks_are_marked_failed_not_complete(tmp_path: Path) -> None:
    config_path = _write_single_enabled_city(tmp_path)
    client = EmptyCandleKalshiHistoryClient()

    try:
        ingest_kalshi_market_history_for_enabled_cities(
            start_date="2026-03-20",
            end_date="2026-03-21",
            interval="1h",
            config_path=config_path,
            output_dir=tmp_path,
            client=client,
        )
    except KalshiHistoryIngestionError as exc:
        details = exc.details
    else:  # pragma: no cover
        raise AssertionError("Expected KalshiHistoryIngestionError")

    manifest = json.loads((tmp_path / DEFAULT_KALSHI_HISTORY_MANIFEST_FILENAME).read_text(encoding="utf-8"))
    assert details["failed_candle_chunks"] == 1
    assert details["completed_candle_chunks"] == 0
    assert manifest["candle_chunks"]["KXHIGHNY-26MAR20-B65"]["status"] == "failed"


def test_kalshi_market_history_parses_nested_live_candle_fields(tmp_path: Path) -> None:
    config_path = _write_single_enabled_city(tmp_path)
    client = NestedPriceKalshiHistoryClient()

    _, candles_path = ingest_kalshi_market_history_for_enabled_cities(
        start_date="2026-03-20",
        end_date="2026-03-21",
        interval="1h",
        config_path=config_path,
        output_dir=tmp_path,
        client=client,
    )

    candles_df = pd.read_parquet(candles_path)
    assert candles_df.loc[0, "open"] == 42.0
    assert candles_df.loc[0, "close"] == 44.0
    assert candles_df.loc[0, "low"] == 41.0
    assert candles_df.loc[0, "high"] == 47.0
    assert candles_df.loc[0, "volume"] == 11.0


def test_kalshi_client_retries_429_then_succeeds(monkeypatch) -> None:
    client = KalshiClient(max_retries=2, initial_backoff_seconds=0.1, max_backoff_seconds=0.2)
    calls = {"count": 0}
    sleeps: list[float] = []

    class _Response:
        def __init__(self, status_code: int, payload: dict) -> None:
            self.status_code = status_code
            self._payload = payload
            self.headers = {"Retry-After": "0"}

        def raise_for_status(self) -> None:
            if self.status_code >= 400:
                raise requests.HTTPError(f"{self.status_code} error", response=self)

        def json(self) -> dict:
            return self._payload

    def fake_get(url: str, params=None, timeout: int = 20):
        calls["count"] += 1
        if calls["count"] == 1:
            return _Response(429, {})
        return _Response(200, {"events": []})

    monkeypatch.setattr(client.session, "get", fake_get)
    monkeypatch.setattr("kwb.clients.kalshi.time.sleep", lambda seconds: sleeps.append(seconds))

    payload = client.get_events(series_ticker="KXHIGHNY")

    assert payload == {"events": []}
    assert calls["count"] == 2
    assert len(sleeps) == 1
    assert client.retry_summary()["total_retries"] == 1


def test_kalshi_client_exhausts_retries_on_persistent_429(monkeypatch) -> None:
    client = KalshiClient(max_retries=2, initial_backoff_seconds=0.1, max_backoff_seconds=0.2)
    sleeps: list[float] = []

    class _Response:
        status_code = 429
        headers = {"Retry-After": "0"}

        def raise_for_status(self) -> None:
            raise requests.HTTPError("429 error", response=self)

        def json(self) -> dict:
            return {}

    monkeypatch.setattr(client.session, "get", lambda url, params=None, timeout=20: _Response())
    monkeypatch.setattr("kwb.clients.kalshi.time.sleep", lambda seconds: sleeps.append(seconds))

    try:
        client.get_events(series_ticker="KXHIGHNY")
    except requests.HTTPError:
        pass
    else:  # pragma: no cover
        raise AssertionError("Expected HTTPError after retry exhaustion")

    assert len(sleeps) == 2
    assert client.retry_summary()["total_retries"] == 2


def test_kalshi_history_cli_smoke(tmp_path: Path, monkeypatch) -> None:
    config_path = _write_single_enabled_city(tmp_path)

    def fake_ingest(**kwargs):
        assert kwargs["start_date"] == "2026-03-20"
        assert kwargs["end_date"] == "2026-03-21"
        assert kwargs["interval"] == "1h"
        assert kwargs["resume"] is True
        return (
            tmp_path / "kalshi_markets.parquet",
            tmp_path / "kalshi_candles.parquet",
            {"retry_summary": {"total_retries": 2}, "resume_supported": True},
        )

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
            "--resume",
        ],
    )

    assert result.exit_code == 0
    assert "Saved Kalshi history" in result.stdout
    assert "retries=2" in result.stdout


def test_manifest_summary_reports_resume_state(tmp_path: Path) -> None:
    config_path = _write_single_enabled_city(tmp_path)
    flaky_client = FlakyKalshiHistoryClient()

    try:
        ingest_kalshi_market_history_for_enabled_cities(
            start_date="2026-03-20",
            end_date="2026-03-21",
            interval="1h",
            config_path=config_path,
            output_dir=tmp_path,
            client=flaky_client,
        )
    except KalshiHistoryIngestionError:
        pass

    summary = summarize_kalshi_history_manifest(tmp_path)
    assert summary is not None
    assert summary["status"] == "failed"
    assert summary["resume_recommended"] is True


def test_resume_rejects_mismatched_date_range(tmp_path: Path) -> None:
    config_path = _write_single_enabled_city(tmp_path)
    client = FakeKalshiHistoryClient()

    ingest_kalshi_market_history_for_enabled_cities(
        start_date="2026-03-20",
        end_date="2026-03-21",
        interval="1h",
        config_path=config_path,
        output_dir=tmp_path,
        client=client,
    )

    try:
        ingest_kalshi_market_history_for_enabled_cities(
            start_date="2026-03-19",
            end_date="2026-03-21",
            interval="1h",
            config_path=config_path,
            output_dir=tmp_path,
            client=client,
            resume=True,
        )
    except ValueError as exc:
        assert "different parameters" in str(exc)
    else:  # pragma: no cover
        raise AssertionError("Expected resume parameter mismatch to fail")


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
