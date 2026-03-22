from __future__ import annotations

from datetime import datetime, time, timezone
from pathlib import Path
from typing import TYPE_CHECKING, Any

import pandas as pd

from kwb.ingestion.kalshi_events import DEFAULT_CITIES_CONFIG_PATH, load_enabled_cities
from kwb.settings import STAGING_DIR
from kwb.utils.logging import get_logger

if TYPE_CHECKING:
    from kwb.clients.kalshi import KalshiClient

logger = get_logger(__name__)

DEFAULT_MARKETS_FILENAME = "kalshi_markets.parquet"
DEFAULT_CANDLES_FILENAME = "kalshi_candles.parquet"
INTERVAL_TO_MINUTES = {"1m": 1, "5m": 5, "15m": 15, "1h": 60, "1d": 1440}


def describe_local_quote_history_capabilities() -> dict[str, Any]:
    """Describe the best point-in-time market microstructure available locally.

    The current research pipeline stages:
    - market metadata (`kalshi_markets.parquet`)
    - OHLCV candlesticks (`kalshi_candles.parquet`)

    It does not currently stage:
    - historical best-bid / best-ask snapshots
    - order book depth history
    - quote update or tick-by-tick trade history
    """
    return {
        "has_true_historical_best_bid_ask": False,
        "has_orderbook_history": False,
        "has_trade_history": False,
        "has_candle_history": True,
        "best_local_quote_source": "candlestick_ohlcv",
        "local_quote_limitations": [
            "No historical best-bid/best-ask snapshots are staged locally.",
            "No order-book depth history is staged locally.",
            "Executable backtests must use candle-derived quote proxies unless richer local data is added.",
        ],
    }


def ingest_kalshi_market_history_for_enabled_cities(
    start_date: str,
    end_date: str,
    interval: str,
    config_path: Path = DEFAULT_CITIES_CONFIG_PATH,
    output_dir: Path | None = None,
    client: "KalshiClient | None" = None,
) -> tuple[Path, Path]:
    """Ingest market definitions and candles for enabled Kalshi weather series."""
    cities = load_enabled_cities(config_path)
    if not cities:
        raise ValueError(f"No enabled cities with Kalshi series tickers found in {config_path}.")
    if interval not in INTERVAL_TO_MINUTES:
        raise ValueError(f"Unsupported candle interval {interval!r}. Expected one of {sorted(INTERVAL_TO_MINUTES)}.")

    client = client or _build_kalshi_client()
    output_dir = output_dir or STAGING_DIR
    output_dir.mkdir(parents=True, exist_ok=True)
    ingested_at = datetime.now(timezone.utc).isoformat()

    all_market_rows: list[dict[str, Any]] = []
    all_candle_rows: list[dict[str, Any]] = []

    for city in cities:
        event_index = _fetch_event_index_for_series(client=client, city=city)
        city_market_rows = _fetch_market_rows_for_series(
            client=client,
            city=city,
            event_index=event_index,
            ingested_at=ingested_at,
        )
        all_market_rows.extend(city_market_rows)

        for market_row in city_market_rows:
            candle_rows = _fetch_candle_rows_for_market(
                client=client,
                city=city,
                market_row=market_row,
                start_date=start_date,
                end_date=end_date,
                interval=interval,
                ingested_at=ingested_at,
            )
            all_candle_rows.extend(candle_rows)

    markets_df = _build_markets_frame(all_market_rows)
    candles_df = _build_candles_frame(all_candle_rows)

    markets_path = output_dir / DEFAULT_MARKETS_FILENAME
    candles_path = output_dir / DEFAULT_CANDLES_FILENAME
    markets_df.to_parquet(markets_path, index=False)
    candles_df.to_parquet(candles_path, index=False)

    logger.info(
        "Saved %s Kalshi markets and %s candles for %s enabled cities to %s and %s",
        len(markets_df),
        len(candles_df),
        len(cities),
        markets_path,
        candles_path,
    )
    return markets_path, candles_path


def _fetch_event_index_for_series(
    client: "KalshiClient",
    city: dict[str, Any],
) -> dict[str, dict[str, Any]]:
    events: dict[str, dict[str, Any]] = {}
    cursor: str | None = None

    while True:
        payload = client.get_events(
            series_ticker=city["kalshi_series_ticker"],
            limit=200,
            cursor=cursor,
            with_nested_markets=False,
        )
        page_events = payload.get("events", [])
        if not isinstance(page_events, list):
            raise TypeError(
                f"Expected list of events for series {city['kalshi_series_ticker']}, got {type(page_events)}"
            )
        for event in page_events:
            if not isinstance(event, dict):
                continue
            event_ticker = event.get("event_ticker") or event.get("ticker")
            if event_ticker:
                events[event_ticker] = event

        cursor = payload.get("cursor")
        if not cursor:
            break

    return events


def _fetch_market_rows_for_series(
    client: "KalshiClient",
    city: dict[str, Any],
    event_index: dict[str, dict[str, Any]],
    ingested_at: str,
) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    cursor: str | None = None

    while True:
        payload = client.list_markets(
            series_ticker=city["kalshi_series_ticker"],
            limit=200,
            cursor=cursor,
        )
        page_markets = payload.get("markets", [])
        if not isinstance(page_markets, list):
            raise TypeError(
                f"Expected list of markets for series {city['kalshi_series_ticker']}, got {type(page_markets)}"
            )

        for market in page_markets:
            if not isinstance(market, dict):
                continue
            rows.append(_build_market_row(city=city, market=market, event_index=event_index, ingested_at=ingested_at))

        cursor = payload.get("cursor")
        if not cursor:
            break

    return rows


def _build_market_row(
    city: dict[str, Any],
    market: dict[str, Any],
    event_index: dict[str, dict[str, Any]],
    ingested_at: str,
) -> dict[str, Any]:
    event_ticker = market.get("event_ticker")
    event_summary = event_index.get(event_ticker or "", {})
    return {
        "city_key": city.get("city_key"),
        "series_ticker": city.get("kalshi_series_ticker"),
        "event_ticker": event_ticker,
        "market_ticker": market.get("ticker") or market.get("market_ticker"),
        "strike_date": event_summary.get("strike_date") or market.get("strike_date"),
        "market_title": market.get("title"),
        "market_subtitle": market.get("subtitle") or market.get("sub_title"),
        "status": market.get("status"),
        "floor_strike": market.get("floor_strike"),
        "cap_strike": market.get("cap_strike"),
        "strike_type": market.get("strike_type"),
        "expiration_ts": market.get("expiration_ts"),
        "close_time": market.get("close_time") or market.get("close_ts"),
        "ingested_at": ingested_at,
    }


def _fetch_candle_rows_for_market(
    client: "KalshiClient",
    city: dict[str, Any],
    market_row: dict[str, Any],
    start_date: str,
    end_date: str,
    interval: str,
    ingested_at: str,
) -> list[dict[str, Any]]:
    market_ticker = market_row["market_ticker"]
    if not market_ticker:
        return []

    start_ts, end_ts = _date_range_to_unix_bounds(start_date, end_date)
    payload = client.get_market_candlesticks(
        series_ticker=city["kalshi_series_ticker"],
        market_ticker=market_ticker,
        start_ts=start_ts,
        end_ts=end_ts,
        period_interval=INTERVAL_TO_MINUTES[interval],
    )
    candles = payload.get("candlesticks") or payload.get("candles") or []
    if not isinstance(candles, list):
        raise TypeError(f"Expected list of candles for market {market_ticker}, got {type(candles)}")

    rows: list[dict[str, Any]] = []
    for candle in candles:
        if not isinstance(candle, dict):
            continue
        rows.append(
            {
                "market_ticker": market_ticker,
                "city_key": city.get("city_key"),
                "candle_ts": _normalize_candle_ts(candle),
                "open": candle.get("open"),
                "high": candle.get("high"),
                "low": candle.get("low"),
                "close": candle.get("close"),
                "volume": candle.get("volume"),
                "interval": interval,
                "ingested_at": ingested_at,
            }
        )
    return rows


def _build_markets_frame(rows: list[dict[str, Any]]) -> pd.DataFrame:
    columns = [
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
    df = pd.DataFrame(rows, columns=columns)
    if df.empty:
        return df
    df = df.drop_duplicates(subset=["city_key", "market_ticker"], keep="last")
    return df.sort_values(["city_key", "strike_date", "market_ticker"], kind="stable").reset_index(drop=True)


def _build_candles_frame(rows: list[dict[str, Any]]) -> pd.DataFrame:
    columns = [
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
    df = pd.DataFrame(rows, columns=columns)
    if df.empty:
        return df
    df = df.drop_duplicates(subset=["market_ticker", "candle_ts", "interval"], keep="last")
    return df.sort_values(["city_key", "market_ticker", "candle_ts"], kind="stable").reset_index(drop=True)


def _normalize_candle_ts(candle: dict[str, Any]) -> str | None:
    timestamp = (
        candle.get("end_period_ts")
        or candle.get("end_ts")
        or candle.get("start_period_ts")
        or candle.get("start_ts")
        or candle.get("time")
    )
    if timestamp is None:
        return None
    if not isinstance(timestamp, (int, float)):
        raise TypeError(f"Expected numeric candle timestamp, got {timestamp!r}")
    return datetime.fromtimestamp(timestamp, tz=timezone.utc).isoformat()


def _date_range_to_unix_bounds(start_date: str, end_date: str) -> tuple[int, int]:
    start_dt = datetime.combine(datetime.fromisoformat(start_date).date(), time.min, tzinfo=timezone.utc)
    end_dt = datetime.combine(datetime.fromisoformat(end_date).date(), time.max, tzinfo=timezone.utc)
    return int(start_dt.timestamp()), int(end_dt.timestamp())


def _build_kalshi_client() -> "KalshiClient":
    from kwb.clients.kalshi import KalshiClient

    return KalshiClient()
