from __future__ import annotations

import random
import time
from datetime import datetime, timezone
from typing import Any

import requests

from kwb.settings import KALSHI_BASE_URL
from kwb.utils.logging import get_logger

logger = get_logger(__name__)

RETRYABLE_STATUS_CODES = {429, 500, 502, 503, 504}


class KalshiClient:
    def __init__(
        self,
        base_url: str = KALSHI_BASE_URL,
        timeout: int = 20,
        max_retries: int = 4,
        initial_backoff_seconds: float = 1.0,
        max_backoff_seconds: float = 30.0,
    ) -> None:
        self.base_url = base_url.rstrip("/")
        self.timeout = timeout
        self.max_retries = max_retries
        self.initial_backoff_seconds = initial_backoff_seconds
        self.max_backoff_seconds = max_backoff_seconds
        self.session = requests.Session()
        self.retry_events: list[dict[str, Any]] = []

    def _get(self, path: str, params: dict[str, Any] | None = None) -> dict[str, Any]:
        url = f"{self.base_url}{path}"
        attempt = 0

        while True:
            attempt += 1
            try:
                response = self.session.get(url, params=params, timeout=self.timeout)
                response.raise_for_status()
                return response.json()
            except requests.HTTPError as exc:
                status_code = exc.response.status_code if exc.response is not None else None
                if status_code not in RETRYABLE_STATUS_CODES or attempt > self.max_retries:
                    raise
                self._sleep_before_retry(path=path, attempt=attempt, status_code=status_code, response=exc.response)
            except (requests.ConnectionError, requests.Timeout) as exc:
                if attempt > self.max_retries:
                    raise
                self._sleep_before_retry(path=path, attempt=attempt, status_code=None, response=None, error=exc)

    def _sleep_before_retry(
        self,
        path: str,
        attempt: int,
        status_code: int | None,
        response: requests.Response | None,
        error: Exception | None = None,
    ) -> None:
        retry_after_seconds = _parse_retry_after_seconds(response)
        base_backoff = min(
            self.max_backoff_seconds,
            self.initial_backoff_seconds * (2 ** max(attempt - 1, 0)),
        )
        jitter_seconds = random.uniform(0.0, max(base_backoff * 0.25, 0.0))
        sleep_seconds = max(retry_after_seconds or 0.0, min(self.max_backoff_seconds, base_backoff + jitter_seconds))
        reason = f"status={status_code}" if status_code is not None else error.__class__.__name__ if error else "unknown"
        self.retry_events.append(
            {
                "path": path,
                "attempt": attempt,
                "reason": reason,
                "sleep_seconds": sleep_seconds,
            }
        )
        logger.warning(
            "Kalshi request retry scheduled for %s after %s attempt=%s sleep=%.2fs",
            path,
            reason,
            attempt,
            sleep_seconds,
        )
        time.sleep(sleep_seconds)

    def retry_summary(self) -> dict[str, Any]:
        return {
            "total_retries": len(self.retry_events),
            "events": [dict(event) for event in self.retry_events],
        }

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


def _parse_retry_after_seconds(response: requests.Response | None) -> float | None:
    if response is None:
        return None
    raw = response.headers.get("Retry-After")
    if raw is None:
        return None
    try:
        return max(float(raw), 0.0)
    except (TypeError, ValueError):
        return None
