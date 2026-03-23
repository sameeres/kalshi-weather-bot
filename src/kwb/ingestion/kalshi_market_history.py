from __future__ import annotations

import json
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
DEFAULT_KALSHI_HISTORY_CHUNKS_DIRNAME = "kalshi_history_chunks"
DEFAULT_KALSHI_HISTORY_MANIFEST_FILENAME = "kalshi_history_manifest.json"
INTERVAL_TO_MINUTES = {"1m": 1, "5m": 5, "15m": 15, "1h": 60, "1d": 1440}


class KalshiHistoryIngestionError(RuntimeError):
    """Raised when Kalshi history ingestion makes partial progress but cannot finish."""

    def __init__(self, message: str, details: dict[str, Any]) -> None:
        super().__init__(message)
        self.details = details


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
    resume: bool = False,
    max_retries: int = 4,
    initial_backoff_seconds: float = 1.0,
    max_backoff_seconds: float = 30.0,
    return_details: bool = False,
) -> tuple[Path, Path] | tuple[Path, Path, dict[str, Any]]:
    """Ingest market definitions and candles for enabled Kalshi weather series.

    Successful city and market chunks are persisted immediately under
    `kalshi_history_chunks/`. Final staged parquet files are only consolidated and
    written when every required chunk completes.
    """
    cities = load_enabled_cities(config_path)
    if not cities:
        raise ValueError(f"No enabled cities with Kalshi series tickers found in {config_path}.")
    if interval not in INTERVAL_TO_MINUTES:
        raise ValueError(f"Unsupported candle interval {interval!r}. Expected one of {sorted(INTERVAL_TO_MINUTES)}.")

    client = client or _build_kalshi_client(
        max_retries=max_retries,
        initial_backoff_seconds=initial_backoff_seconds,
        max_backoff_seconds=max_backoff_seconds,
    )
    output_dir = output_dir or STAGING_DIR
    output_dir.mkdir(parents=True, exist_ok=True)
    chunk_dir = output_dir / DEFAULT_KALSHI_HISTORY_CHUNKS_DIRNAME
    chunk_dir.mkdir(parents=True, exist_ok=True)
    manifest_path = output_dir / DEFAULT_KALSHI_HISTORY_MANIFEST_FILENAME
    ingested_at = datetime.now(timezone.utc).isoformat()

    manifest = _load_or_initialize_manifest(
        manifest_path=manifest_path,
        chunk_dir=chunk_dir,
        config_path=config_path,
        start_date=start_date,
        end_date=end_date,
        interval=interval,
        cities=cities,
        resume=resume,
    )

    try:
        for city in cities:
            city_market_rows = _get_or_build_market_chunk(
                client=client,
                city=city,
                ingested_at=ingested_at,
                chunk_dir=chunk_dir,
                manifest=manifest,
                manifest_path=manifest_path,
                resume=resume,
            )

            for market_row in city_market_rows:
                _get_or_build_candle_chunk(
                    client=client,
                    city=city,
                    market_row=market_row,
                    start_date=start_date,
                    end_date=end_date,
                    interval=interval,
                    ingested_at=ingested_at,
                    chunk_dir=chunk_dir,
                    manifest=manifest,
                    manifest_path=manifest_path,
                    resume=resume,
                )
    except Exception as exc:
        manifest["status"] = "failed"
        manifest["last_error"] = str(exc)
        manifest["updated_at_utc"] = datetime.now(timezone.utc).isoformat()
        manifest["retry_summary"] = _extract_retry_summary(client)
        _refresh_manifest_counts(manifest)
        _write_json(manifest_path, manifest)
        details = _build_kalshi_history_details(
            manifest=manifest,
            manifest_path=manifest_path,
            chunk_dir=chunk_dir,
            output_dir=output_dir,
            final_outputs_written=False,
        )
        raise KalshiHistoryIngestionError(
            f"Kalshi history build stopped after partial progress. Resume with --resume. Manifest: {manifest_path}",
            details,
        ) from exc

    markets_df = _consolidate_market_chunks(manifest)
    candles_df = _consolidate_candle_chunks(manifest)

    markets_path = output_dir / DEFAULT_MARKETS_FILENAME
    candles_path = output_dir / DEFAULT_CANDLES_FILENAME
    _write_parquet(markets_df, markets_path)
    _write_parquet(candles_df, candles_path)

    manifest["status"] = "completed"
    manifest["last_error"] = None
    manifest["updated_at_utc"] = datetime.now(timezone.utc).isoformat()
    manifest["retry_summary"] = _extract_retry_summary(client)
    _refresh_manifest_counts(manifest)
    _write_json(manifest_path, manifest)

    logger.info(
        "Saved %s Kalshi markets and %s candles for %s enabled cities to %s and %s",
        len(markets_df),
        len(candles_df),
        len(cities),
        markets_path,
        candles_path,
    )

    details = _build_kalshi_history_details(
        manifest=manifest,
        manifest_path=manifest_path,
        chunk_dir=chunk_dir,
        output_dir=output_dir,
        final_outputs_written=True,
    )
    if return_details:
        return markets_path, candles_path, details
    return markets_path, candles_path


def load_kalshi_history_manifest(output_dir: Path | None = None) -> dict[str, Any] | None:
    """Load the latest Kalshi history manifest if it exists."""
    output_dir = output_dir or STAGING_DIR
    manifest_path = output_dir / DEFAULT_KALSHI_HISTORY_MANIFEST_FILENAME
    if not manifest_path.exists():
        return None
    return json.loads(manifest_path.read_text(encoding="utf-8"))


def summarize_kalshi_history_manifest(output_dir: Path | None = None) -> dict[str, Any] | None:
    """Return a compact summary of chunk/resume progress for reporting."""
    manifest = load_kalshi_history_manifest(output_dir=output_dir)
    if manifest is None:
        return None
    _refresh_manifest_counts(manifest)
    return {
        "status": manifest.get("status"),
        "manifest_path": str((output_dir or STAGING_DIR) / DEFAULT_KALSHI_HISTORY_MANIFEST_FILENAME),
        "chunk_dir": manifest.get("chunk_dir"),
        "resume_supported": True,
        "resume_recommended": manifest.get("status") == "failed",
        "completed_market_chunks": manifest.get("completed_market_chunks", 0),
        "failed_market_chunks": manifest.get("failed_market_chunks", 0),
        "pending_market_chunks": manifest.get("pending_market_chunks", 0),
        "completed_candle_chunks": manifest.get("completed_candle_chunks", 0),
        "failed_candle_chunks": manifest.get("failed_candle_chunks", 0),
        "pending_candle_chunks": manifest.get("pending_candle_chunks", 0),
        "retry_summary": manifest.get("retry_summary", {"total_retries": 0, "events": []}),
        "last_error": manifest.get("last_error"),
        "updated_at_utc": manifest.get("updated_at_utc"),
    }


def _get_or_build_market_chunk(
    client: "KalshiClient",
    city: dict[str, Any],
    ingested_at: str,
    chunk_dir: Path,
    manifest: dict[str, Any],
    manifest_path: Path,
    resume: bool,
) -> list[dict[str, Any]]:
    city_key = str(city.get("city_key"))
    series_ticker = str(city.get("kalshi_series_ticker"))
    chunk_key = city_key
    chunk_path = _market_chunk_path(chunk_dir, city_key, series_ticker)
    manifest_entry = manifest["markets_chunks"].setdefault(
        chunk_key,
        {
            "city_key": city_key,
            "series_ticker": series_ticker,
            "path": str(chunk_path),
            "status": "pending",
            "row_count": 0,
            "error": None,
        },
    )

    if resume and manifest_entry.get("status") == "complete" and chunk_path.exists():
        logger.info("Resuming Kalshi markets from existing chunk for city_key=%s", city_key)
        return _load_chunk_rows(chunk_path)

    try:
        event_index = _fetch_event_index_for_series(client=client, city=city)
        rows = _fetch_market_rows_for_series(
            client=client,
            city=city,
            event_index=event_index,
            ingested_at=ingested_at,
        )
        _write_parquet(_build_markets_frame(rows), chunk_path)
        manifest_entry["status"] = "complete"
        manifest_entry["row_count"] = len(rows)
        manifest_entry["error"] = None
        manifest["updated_at_utc"] = datetime.now(timezone.utc).isoformat()
        _refresh_manifest_counts(manifest)
        _write_json(manifest_path, manifest)
        return rows
    except Exception as exc:
        manifest_entry["status"] = "failed"
        manifest_entry["error"] = str(exc)
        manifest["updated_at_utc"] = datetime.now(timezone.utc).isoformat()
        _refresh_manifest_counts(manifest)
        _write_json(manifest_path, manifest)
        raise


def _get_or_build_candle_chunk(
    client: "KalshiClient",
    city: dict[str, Any],
    market_row: dict[str, Any],
    start_date: str,
    end_date: str,
    interval: str,
    ingested_at: str,
    chunk_dir: Path,
    manifest: dict[str, Any],
    manifest_path: Path,
    resume: bool,
) -> None:
    market_ticker = str(market_row.get("market_ticker") or "")
    if not market_ticker:
        return

    chunk_path = _candle_chunk_path(chunk_dir, market_ticker)
    manifest_entry = manifest["candle_chunks"].setdefault(
        market_ticker,
        {
            "city_key": str(city.get("city_key")),
            "series_ticker": str(city.get("kalshi_series_ticker")),
            "market_ticker": market_ticker,
            "path": str(chunk_path),
            "status": "pending",
            "row_count": 0,
            "error": None,
        },
    )

    if resume and manifest_entry.get("status") == "complete" and chunk_path.exists():
        logger.info("Resuming Kalshi candles from existing chunk for market_ticker=%s", market_ticker)
        return

    try:
        rows = _fetch_candle_rows_for_market(
            client=client,
            city=city,
            market_row=market_row,
            start_date=start_date,
            end_date=end_date,
            interval=interval,
            ingested_at=ingested_at,
        )
        _write_parquet(_build_candles_frame(rows), chunk_path)
        manifest_entry["status"] = "complete"
        manifest_entry["row_count"] = len(rows)
        manifest_entry["error"] = None
        manifest["updated_at_utc"] = datetime.now(timezone.utc).isoformat()
        _refresh_manifest_counts(manifest)
        _write_json(manifest_path, manifest)
    except Exception as exc:
        manifest_entry["status"] = "failed"
        manifest_entry["error"] = str(exc)
        manifest["updated_at_utc"] = datetime.now(timezone.utc).isoformat()
        _refresh_manifest_counts(manifest)
        _write_json(manifest_path, manifest)
        raise


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


def _consolidate_market_chunks(manifest: dict[str, Any]) -> pd.DataFrame:
    frames = [pd.read_parquet(entry["path"]) for entry in manifest["markets_chunks"].values() if entry["status"] == "complete"]
    if not frames:
        return _build_markets_frame([])
    rows: list[dict[str, Any]] = []
    for frame in frames:
        rows.extend(frame.to_dict(orient="records"))
    return _build_markets_frame(rows)


def _consolidate_candle_chunks(manifest: dict[str, Any]) -> pd.DataFrame:
    frames = [pd.read_parquet(entry["path"]) for entry in manifest["candle_chunks"].values() if entry["status"] == "complete"]
    if not frames:
        return _build_candles_frame([])
    rows: list[dict[str, Any]] = []
    for frame in frames:
        rows.extend(frame.to_dict(orient="records"))
    return _build_candles_frame(rows)


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


def _build_kalshi_client(
    max_retries: int = 4,
    initial_backoff_seconds: float = 1.0,
    max_backoff_seconds: float = 30.0,
) -> "KalshiClient":
    from kwb.clients.kalshi import KalshiClient

    return KalshiClient(
        max_retries=max_retries,
        initial_backoff_seconds=initial_backoff_seconds,
        max_backoff_seconds=max_backoff_seconds,
    )


def _load_or_initialize_manifest(
    manifest_path: Path,
    chunk_dir: Path,
    config_path: Path,
    start_date: str,
    end_date: str,
    interval: str,
    cities: list[dict[str, Any]],
    resume: bool,
) -> dict[str, Any]:
    if resume and manifest_path.exists():
        manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
        expected = {
            "config_path": str(config_path),
            "start_date": start_date,
            "end_date": end_date,
            "interval": interval,
        }
        observed = {key: manifest.get(key) for key in expected}
        if observed != expected:
            raise ValueError(
                "Cannot resume Kalshi history with different parameters. "
                f"Expected {expected}, found {observed} in {manifest_path}."
            )
        manifest["resumed_at_utc"] = datetime.now(timezone.utc).isoformat()
        return manifest

    manifest = {
        "status": "in_progress",
        "started_at_utc": datetime.now(timezone.utc).isoformat(),
        "updated_at_utc": datetime.now(timezone.utc).isoformat(),
        "config_path": str(config_path),
        "chunk_dir": str(chunk_dir),
        "start_date": start_date,
        "end_date": end_date,
        "interval": interval,
        "enabled_city_keys": [str(city.get("city_key")) for city in cities if city.get("city_key")],
        "resume_supported": True,
        "markets_chunks": {},
        "candle_chunks": {},
        "retry_summary": {"total_retries": 0, "events": []},
        "last_error": None,
    }
    _write_json(manifest_path, manifest)
    return manifest


def _market_chunk_path(chunk_dir: Path, city_key: str, series_ticker: str) -> Path:
    return chunk_dir / f"markets__{_safe_token(city_key)}__{_safe_token(series_ticker)}.parquet"


def _candle_chunk_path(chunk_dir: Path, market_ticker: str) -> Path:
    return chunk_dir / f"candles__{_safe_token(market_ticker)}.parquet"


def _safe_token(value: str) -> str:
    sanitized = "".join(character if character.isalnum() else "_" for character in value)
    return sanitized.strip("_") or "unknown"


def _load_chunk_rows(path: Path) -> list[dict[str, Any]]:
    frame = pd.read_parquet(path)
    return frame.to_dict(orient="records")


def _write_parquet(frame: pd.DataFrame, path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temp_path = path.with_suffix(path.suffix + ".tmp")
    frame.to_parquet(temp_path, index=False)
    temp_path.replace(path)


def _write_json(path: Path, payload: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temp_path = path.with_suffix(path.suffix + ".tmp")
    temp_path.write_text(json.dumps(payload, indent=2, sort_keys=True), encoding="utf-8")
    temp_path.replace(path)


def _refresh_manifest_counts(manifest: dict[str, Any]) -> None:
    market_statuses = [entry.get("status") for entry in manifest.get("markets_chunks", {}).values()]
    candle_statuses = [entry.get("status") for entry in manifest.get("candle_chunks", {}).values()]
    manifest["completed_market_chunks"] = sum(status == "complete" for status in market_statuses)
    manifest["failed_market_chunks"] = sum(status == "failed" for status in market_statuses)
    manifest["pending_market_chunks"] = sum(status != "complete" for status in market_statuses)
    manifest["completed_candle_chunks"] = sum(status == "complete" for status in candle_statuses)
    manifest["failed_candle_chunks"] = sum(status == "failed" for status in candle_statuses)
    manifest["pending_candle_chunks"] = sum(status != "complete" for status in candle_statuses)


def _extract_retry_summary(client: "KalshiClient") -> dict[str, Any]:
    retry_summary = getattr(client, "retry_summary", None)
    if callable(retry_summary):
        return retry_summary()
    return {"total_retries": 0, "events": []}


def _build_kalshi_history_details(
    manifest: dict[str, Any],
    manifest_path: Path,
    chunk_dir: Path,
    output_dir: Path,
    final_outputs_written: bool,
) -> dict[str, Any]:
    _refresh_manifest_counts(manifest)
    return {
        "status": manifest.get("status"),
        "manifest_path": str(manifest_path),
        "chunk_dir": str(chunk_dir),
        "resume_supported": True,
        "resume_recommended": manifest.get("status") == "failed",
        "completed_market_chunks": manifest.get("completed_market_chunks", 0),
        "failed_market_chunks": manifest.get("failed_market_chunks", 0),
        "pending_market_chunks": manifest.get("pending_market_chunks", 0),
        "completed_candle_chunks": manifest.get("completed_candle_chunks", 0),
        "failed_candle_chunks": manifest.get("failed_candle_chunks", 0),
        "pending_candle_chunks": manifest.get("pending_candle_chunks", 0),
        "retry_summary": manifest.get("retry_summary", {"total_retries": 0, "events": []}),
        "last_error": manifest.get("last_error"),
        "final_outputs_written": final_outputs_written,
        "markets_output_path": str(output_dir / DEFAULT_MARKETS_FILENAME),
        "candles_output_path": str(output_dir / DEFAULT_CANDLES_FILENAME),
    }
