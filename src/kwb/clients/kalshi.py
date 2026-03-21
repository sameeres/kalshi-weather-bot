from __future__ import annotations

from datetime import datetime, timezone
from typing import Any

import requests

from kwb.settings import KALSHI_BASE_URL


class KalshiClient:
    def __init__(self, base_url: str = KALSHI_BASE_URL, timeout: int = 20) -> None:
        self.base_url = base_url.rstrip("/")
        self.timeout = timeout
        self.session = requests.Session()

    def _get(self, path: str, params: dict[str, Any] | None = None) -> dict[str, Any]:
        url = f"{self.base_url}{path}"
        response = self.session.get(url, params=params, timeout=self.timeout)
        response.raise_for_status()
        return response.json()

    def get_events(
        self,
        series_ticker: str | None = None,
        limit: int = 200,
        cursor: str | None = None,
        with_nested_markets: bool = False,
    ) -> dict[str, Any]:
        params: dict[str, Any] = {"limit": limit}
        if series_ticker:
            params["series_ticker"] = series_ticker
        if cursor:
            params["cursor"] = cursor
        if with_nested_markets:
            params["with_nested_markets"] = with_nested_markets
        return self._get("/events", params=params)

    def get_event(self, event_ticker: str) -> dict[str, Any]:
        return self._get(f"/events/{event_ticker}")

    def get_event_metadata(self, event_ticker: str) -> dict[str, Any]:
        return self._get(f"/events/{event_ticker}/metadata")

    def get_markets(self, event_ticker: str | None = None, limit: int = 100) -> dict[str, Any]:
        params: dict[str, Any] = {"limit": limit}
        if event_ticker:
            params["event_ticker"] = event_ticker
        return self._get("/markets", params=params)

    def list_markets(
        self,
        series_ticker: str | None = None,
        event_ticker: str | None = None,
        limit: int = 200,
        cursor: str | None = None,
    ) -> dict[str, Any]:
        params: dict[str, Any] = {"limit": limit}
        if series_ticker:
            params["series_ticker"] = series_ticker
        if event_ticker:
            params["event_ticker"] = event_ticker
        if cursor:
            params["cursor"] = cursor
        return self._get("/markets", params=params)

    def get_market_candlesticks(
        self,
        series_ticker: str,
        market_ticker: str,
        start_ts: int,
        end_ts: int,
        period_interval: int,
        include_latest_before_start: bool = False,
    ) -> dict[str, Any]:
        params = {
            "start_ts": start_ts,
            "end_ts": end_ts,
            "period_interval": period_interval,
            "include_latest_before_start": str(include_latest_before_start).lower(),
        }
        return self._get(
            f"/series/{series_ticker}/markets/{market_ticker}/candlesticks",
            params=params,
        )


def to_unix_ts(dt: datetime) -> int:
    return int(dt.replace(tzinfo=timezone.utc).timestamp()) if dt.tzinfo is None else int(dt.timestamp())
