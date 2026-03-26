from __future__ import annotations

import json
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import TYPE_CHECKING, Any

import pandas as pd

from kwb.ingestion.kalshi_events import DEFAULT_CITIES_CONFIG_PATH, load_enabled_cities
from kwb.settings import STAGING_DIR
from kwb.utils.logging import get_logger

if TYPE_CHECKING:
    from kwb.clients.kalshi import KalshiClient

logger = get_logger(__name__)

DEFAULT_MICROSTRUCTURE_SNAPSHOTS_FILENAME = "kalshi_market_microstructure_snapshots.parquet"
DEFAULT_ORDERBOOK_LEVELS_FILENAME = "kalshi_orderbook_levels.parquet"
DEFAULT_MICROSTRUCTURE_CAPTURE_SUMMARY_FILENAME = "kalshi_microstructure_capture_summary.json"

REQUIRED_MICROSTRUCTURE_SNAPSHOT_COLUMNS = {
    "snapshot_ts",
    "city_key",
    "series_ticker",
    "market_ticker",
    "market_status",
    "best_yes_bid_cents",
    "best_yes_ask_cents",
    "best_no_bid_cents",
    "best_no_ask_cents",
    "orderbook_available",
    "ingested_at",
}

REQUIRED_ORDERBOOK_LEVEL_COLUMNS = {
    "snapshot_ts",
    "city_key",
    "series_ticker",
    "market_ticker",
    "side",
    "level_rank",
    "price_cents",
    "quantity",
    "ingested_at",
}

SNAPSHOT_COLUMNS = [
    "snapshot_ts",
    "city_key",
    "series_ticker",
    "event_ticker",
    "market_ticker",
    "strike_date",
    "floor_strike",
    "cap_strike",
    "strike_type",
    "market_title",
    "market_subtitle",
    "market_status",
    "close_time",
    "expiration_time",
    "response_price_units",
    "tick_size",
    "price_level_structure",
    "price_ranges_json",
    "last_price_cents",
    "volume_contracts",
    "open_interest_contracts",
    "liquidity_dollars",
    "fractional_trading_enabled",
    "can_close_early",
    "market_yes_bid_cents",
    "market_yes_bid_size",
    "market_yes_ask_cents",
    "market_yes_ask_size",
    "market_no_bid_cents",
    "market_no_bid_size",
    "market_no_ask_cents",
    "market_no_ask_size",
    "orderbook_yes_bid_cents",
    "orderbook_yes_bid_size",
    "orderbook_yes_ask_cents",
    "orderbook_yes_ask_size",
    "orderbook_no_bid_cents",
    "orderbook_no_bid_size",
    "orderbook_no_ask_cents",
    "orderbook_no_ask_size",
    "best_yes_bid_cents",
    "best_yes_bid_size",
    "best_yes_ask_cents",
    "best_yes_ask_size",
    "best_no_bid_cents",
    "best_no_bid_size",
    "best_no_ask_cents",
    "best_no_ask_size",
    "yes_spread_cents",
    "no_spread_cents",
    "orderbook_available",
    "orderbook_depth_requested",
    "orderbook_capture_error",
    "quote_source",
    "ingested_at",
]

LEVEL_COLUMNS = [
    "snapshot_ts",
    "city_key",
    "series_ticker",
    "event_ticker",
    "market_ticker",
    "market_status",
    "side",
    "level_rank",
    "price_cents",
    "quantity",
    "orderbook_depth_requested",
    "tick_size",
    "price_level_structure",
    "ingested_at",
]


def capture_kalshi_microstructure_for_enabled_cities(
    config_path: Path = DEFAULT_CITIES_CONFIG_PATH,
    output_dir: Path | None = None,
    client: "KalshiClient | None" = None,
    status: str | None = "open",
    include_orderbook: bool = True,
    orderbook_depth: int = 10,
    iterations: int = 1,
    poll_interval_seconds: float | None = None,
    return_summary: bool = False,
) -> tuple[Path, Path, Path] | tuple[Path, Path, Path, dict[str, Any]]:
    """Capture forward-looking Kalshi market microstructure for enabled weather series.

    The stored datasets are append-only research tables in `data/staging/`:
    - one row per market per snapshot timestamp with top-of-book and metadata
    - one row per orderbook price level per snapshot timestamp when orderbook data is available
    """
    if iterations < 1:
        raise ValueError("iterations must be at least 1.")
    if iterations > 1 and (poll_interval_seconds is None or poll_interval_seconds <= 0):
        raise ValueError("poll_interval_seconds must be positive when iterations > 1.")
    if orderbook_depth < 1:
        raise ValueError("orderbook_depth must be at least 1.")

    cities = load_enabled_cities(config_path)
    if not cities:
        raise ValueError(f"No enabled cities with Kalshi series tickers found in {config_path}.")

    client = client or _build_kalshi_client()
    output_dir = output_dir or STAGING_DIR
    output_dir.mkdir(parents=True, exist_ok=True)

    snapshots_path = output_dir / DEFAULT_MICROSTRUCTURE_SNAPSHOTS_FILENAME
    levels_path = output_dir / DEFAULT_ORDERBOOK_LEVELS_FILENAME
    summary_path = output_dir / DEFAULT_MICROSTRUCTURE_CAPTURE_SUMMARY_FILENAME

    all_snapshot_rows: list[dict[str, Any]] = []
    all_level_rows: list[dict[str, Any]] = []
    iteration_summaries: list[dict[str, Any]] = []

    for iteration_index in range(iterations):
        snapshot_ts = datetime.now(timezone.utc).isoformat()
        ingested_at = snapshot_ts
        snapshot_rows, level_rows, iteration_summary = _capture_once(
            cities=cities,
            client=client,
            snapshot_ts=snapshot_ts,
            ingested_at=ingested_at,
            status=status,
            include_orderbook=include_orderbook,
            orderbook_depth=orderbook_depth,
        )
        all_snapshot_rows.extend(snapshot_rows)
        all_level_rows.extend(level_rows)
        iteration_summaries.append(iteration_summary)
        if iteration_index + 1 < iterations:
            time.sleep(float(poll_interval_seconds))

    snapshots_df = _build_snapshot_frame(all_snapshot_rows)
    levels_df = _build_level_frame(all_level_rows)
    snapshots_written = _append_or_create_parquet(
        path=snapshots_path,
        frame=snapshots_df,
        unique_keys=["snapshot_ts", "market_ticker"],
        sort_keys=["snapshot_ts", "city_key", "market_ticker"],
    )
    levels_written = _append_or_create_parquet(
        path=levels_path,
        frame=levels_df,
        unique_keys=["snapshot_ts", "market_ticker", "side", "level_rank"],
        sort_keys=["snapshot_ts", "market_ticker", "side", "level_rank"],
    )

    summary = {
        "captured_at_utc": datetime.now(timezone.utc).isoformat(),
        "config_path": str(config_path),
        "output_dir": str(output_dir),
        "status_filter": status,
        "include_orderbook": include_orderbook,
        "orderbook_depth": orderbook_depth,
        "iterations": iterations,
        "poll_interval_seconds": poll_interval_seconds,
        "enabled_city_keys": [str(city.get("city_key")) for city in cities if city.get("city_key")],
        "snapshot_rows_captured": len(all_snapshot_rows),
        "orderbook_levels_captured": len(all_level_rows),
        "snapshots_total_rows": snapshots_written,
        "orderbook_levels_total_rows": levels_written,
        "snapshot_output_path": str(snapshots_path),
        "orderbook_levels_output_path": str(levels_path),
        "iteration_summaries": iteration_summaries,
    }
    summary_path.write_text(json.dumps(summary, indent=2, sort_keys=True) + "\n", encoding="utf-8")

    logger.info(
        "Captured Kalshi microstructure snapshots=%s levels=%s into %s and %s",
        len(all_snapshot_rows),
        len(all_level_rows),
        snapshots_path,
        levels_path,
    )
    if return_summary:
        return snapshots_path, levels_path, summary_path, summary
    return snapshots_path, levels_path, summary_path


def _capture_once(
    cities: list[dict[str, Any]],
    client: "KalshiClient",
    snapshot_ts: str,
    ingested_at: str,
    status: str | None,
    include_orderbook: bool,
    orderbook_depth: int,
) -> tuple[list[dict[str, Any]], list[dict[str, Any]], dict[str, Any]]:
    snapshot_rows: list[dict[str, Any]] = []
    level_rows: list[dict[str, Any]] = []
    orderbook_failures: list[dict[str, str]] = []

    for city in cities:
        city_key = str(city.get("city_key") or "")
        series_ticker = str(city.get("kalshi_series_ticker") or "")
        if not city_key or not series_ticker:
            continue
        for market in _fetch_series_markets(client=client, series_ticker=series_ticker, status=status):
            market_ticker = str(market.get("ticker") or market.get("market_ticker") or "")
            if not market_ticker:
                continue
            orderbook_payload: dict[str, Any] | None = None
            orderbook_error: str | None = None
            if include_orderbook:
                try:
                    orderbook_payload = client.get_market_orderbook(market_ticker=market_ticker, depth=orderbook_depth)
                except Exception as exc:  # pragma: no cover - network/runtime dependent
                    orderbook_error = str(exc)
                    orderbook_failures.append({"market_ticker": market_ticker, "error": orderbook_error})
                    logger.warning("Orderbook capture failed for %s: %s", market_ticker, exc)

            orderbook_levels = _extract_orderbook_levels(orderbook_payload)
            snapshot_rows.append(
                _build_snapshot_row(
                    city_key=city_key,
                    series_ticker=series_ticker,
                    market=market,
                    snapshot_ts=snapshot_ts,
                    ingested_at=ingested_at,
                    orderbook_levels=orderbook_levels,
                    orderbook_depth=orderbook_depth,
                    orderbook_error=orderbook_error,
                )
            )
            level_rows.extend(
                _build_level_rows(
                    city_key=city_key,
                    series_ticker=series_ticker,
                    market=market,
                    snapshot_ts=snapshot_ts,
                    ingested_at=ingested_at,
                    orderbook_depth=orderbook_depth,
                    orderbook_levels=orderbook_levels,
                )
            )

    return (
        snapshot_rows,
        level_rows,
        {
            "snapshot_ts": snapshot_ts,
            "market_snapshots_captured": len(snapshot_rows),
            "orderbook_levels_captured": len(level_rows),
            "orderbook_failures": orderbook_failures,
        },
    )


def _fetch_series_markets(
    client: "KalshiClient",
    series_ticker: str,
    status: str | None,
) -> list[dict[str, Any]]:
    markets: list[dict[str, Any]] = []
    cursor: str | None = None
    while True:
        payload = client.list_markets(
            series_ticker=series_ticker,
            status=status,
            limit=200,
            cursor=cursor,
        )
        page_markets = payload.get("markets", [])
        if not isinstance(page_markets, list):
            raise TypeError(f"Expected list of markets for series {series_ticker}, got {type(page_markets)}")
        markets.extend(market for market in page_markets if isinstance(market, dict))
        cursor = payload.get("cursor")
        if not cursor:
            break
    return markets


def _build_snapshot_row(
    city_key: str,
    series_ticker: str,
    market: dict[str, Any],
    snapshot_ts: str,
    ingested_at: str,
    orderbook_levels: dict[str, list[tuple[float, float]]],
    orderbook_depth: int,
    orderbook_error: str | None,
) -> dict[str, Any]:
    market_yes_bid = _coerce_cents_price(market.get("yes_bid_dollars") or market.get("yes_bid"))
    market_yes_ask = _coerce_cents_price(market.get("yes_ask_dollars") or market.get("yes_ask"))
    market_no_bid = _coerce_cents_price(market.get("no_bid_dollars") or market.get("no_bid"))
    market_no_ask = _coerce_cents_price(market.get("no_ask_dollars") or market.get("no_ask"))

    market_yes_bid_size = _coerce_quantity(market.get("yes_bid_size_fp") or market.get("yes_bid_size"))
    market_yes_ask_size = _coerce_quantity(market.get("yes_ask_size_fp") or market.get("yes_ask_size"))
    market_no_bid_size = _coerce_quantity(market.get("no_bid_size_fp") or market.get("no_bid_size"))
    market_no_ask_size = _coerce_quantity(market.get("no_ask_size_fp") or market.get("no_ask_size"))

    orderbook_yes = orderbook_levels.get("yes", [])
    orderbook_no = orderbook_levels.get("no", [])
    orderbook_yes_bid, orderbook_yes_bid_size = _best_level(orderbook_yes)
    orderbook_no_bid, orderbook_no_bid_size = _best_level(orderbook_no)
    orderbook_yes_ask = None if orderbook_no_bid is None else round(100.0 - orderbook_no_bid, 6)
    orderbook_yes_ask_size = orderbook_no_bid_size
    orderbook_no_ask = None if orderbook_yes_bid is None else round(100.0 - orderbook_yes_bid, 6)
    orderbook_no_ask_size = orderbook_yes_bid_size

    orderbook_available = bool(orderbook_yes or orderbook_no)
    quote_source = "orderbook" if orderbook_available else "markets_endpoint"

    best_yes_bid = orderbook_yes_bid if orderbook_available and orderbook_yes_bid is not None else market_yes_bid
    best_yes_bid_size = (
        orderbook_yes_bid_size if orderbook_available and orderbook_yes_bid_size is not None else market_yes_bid_size
    )
    best_yes_ask = orderbook_yes_ask if orderbook_available and orderbook_yes_ask is not None else market_yes_ask
    best_yes_ask_size = (
        orderbook_yes_ask_size if orderbook_available and orderbook_yes_ask_size is not None else market_yes_ask_size
    )
    best_no_bid = orderbook_no_bid if orderbook_available and orderbook_no_bid is not None else market_no_bid
    best_no_bid_size = (
        orderbook_no_bid_size if orderbook_available and orderbook_no_bid_size is not None else market_no_bid_size
    )
    best_no_ask = orderbook_no_ask if orderbook_available and orderbook_no_ask is not None else market_no_ask
    best_no_ask_size = (
        orderbook_no_ask_size if orderbook_available and orderbook_no_ask_size is not None else market_no_ask_size
    )

    return {
        "snapshot_ts": snapshot_ts,
        "city_key": city_key,
        "series_ticker": series_ticker,
        "event_ticker": market.get("event_ticker"),
        "market_ticker": market.get("ticker") or market.get("market_ticker"),
        "strike_date": market.get("strike_date"),
        "floor_strike": _coerce_float(market.get("floor_strike")),
        "cap_strike": _coerce_float(market.get("cap_strike")),
        "strike_type": market.get("strike_type"),
        "market_title": market.get("title"),
        "market_subtitle": market.get("subtitle") or market.get("sub_title"),
        "market_status": market.get("status"),
        "close_time": market.get("close_time") or market.get("close_ts"),
        "expiration_time": market.get("expiration_time") or market.get("latest_expiration_time") or market.get("expiration_ts"),
        "response_price_units": market.get("response_price_units"),
        "tick_size": _coerce_float(market.get("tick_size")),
        "price_level_structure": market.get("price_level_structure"),
        "price_ranges_json": _json_or_none(market.get("price_ranges")),
        "last_price_cents": _coerce_cents_price(market.get("last_price_dollars") or market.get("last_price")),
        "volume_contracts": _coerce_quantity(market.get("volume_fp") or market.get("volume")),
        "open_interest_contracts": _coerce_quantity(market.get("open_interest_fp") or market.get("open_interest")),
        "liquidity_dollars": _coerce_float(market.get("liquidity_dollars")),
        "fractional_trading_enabled": _coerce_bool(market.get("fractional_trading_enabled")),
        "can_close_early": _coerce_bool(market.get("can_close_early")),
        "market_yes_bid_cents": market_yes_bid,
        "market_yes_bid_size": market_yes_bid_size,
        "market_yes_ask_cents": market_yes_ask,
        "market_yes_ask_size": market_yes_ask_size,
        "market_no_bid_cents": market_no_bid,
        "market_no_bid_size": market_no_bid_size,
        "market_no_ask_cents": market_no_ask,
        "market_no_ask_size": market_no_ask_size,
        "orderbook_yes_bid_cents": orderbook_yes_bid,
        "orderbook_yes_bid_size": orderbook_yes_bid_size,
        "orderbook_yes_ask_cents": orderbook_yes_ask,
        "orderbook_yes_ask_size": orderbook_yes_ask_size,
        "orderbook_no_bid_cents": orderbook_no_bid,
        "orderbook_no_bid_size": orderbook_no_bid_size,
        "orderbook_no_ask_cents": orderbook_no_ask,
        "orderbook_no_ask_size": orderbook_no_ask_size,
        "best_yes_bid_cents": best_yes_bid,
        "best_yes_bid_size": best_yes_bid_size,
        "best_yes_ask_cents": best_yes_ask,
        "best_yes_ask_size": best_yes_ask_size,
        "best_no_bid_cents": best_no_bid,
        "best_no_bid_size": best_no_bid_size,
        "best_no_ask_cents": best_no_ask,
        "best_no_ask_size": best_no_ask_size,
        "yes_spread_cents": _spread(best_yes_bid, best_yes_ask),
        "no_spread_cents": _spread(best_no_bid, best_no_ask),
        "orderbook_available": orderbook_available,
        "orderbook_depth_requested": orderbook_depth,
        "orderbook_capture_error": orderbook_error,
        "quote_source": quote_source,
        "ingested_at": ingested_at,
    }


def _build_level_rows(
    city_key: str,
    series_ticker: str,
    market: dict[str, Any],
    snapshot_ts: str,
    ingested_at: str,
    orderbook_depth: int,
    orderbook_levels: dict[str, list[tuple[float, float]]],
) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    for side, levels in orderbook_levels.items():
        for index, (price_cents, quantity) in enumerate(levels, start=1):
            rows.append(
                {
                    "snapshot_ts": snapshot_ts,
                    "city_key": city_key,
                    "series_ticker": series_ticker,
                    "event_ticker": market.get("event_ticker"),
                    "market_ticker": market.get("ticker") or market.get("market_ticker"),
                    "market_status": market.get("status"),
                    "side": side,
                    "level_rank": index,
                    "price_cents": price_cents,
                    "quantity": quantity,
                    "orderbook_depth_requested": orderbook_depth,
                    "tick_size": _coerce_float(market.get("tick_size")),
                    "price_level_structure": market.get("price_level_structure"),
                    "ingested_at": ingested_at,
                }
            )
    return rows


def _extract_orderbook_levels(payload: dict[str, Any] | None) -> dict[str, list[tuple[float, float]]]:
    if not isinstance(payload, dict):
        return {"yes": [], "no": []}
    orderbook = payload.get("orderbook", payload)
    if not isinstance(orderbook, dict):
        return {"yes": [], "no": []}
    return {
        "yes": _normalize_orderbook_side(orderbook.get("yes")),
        "no": _normalize_orderbook_side(orderbook.get("no")),
    }


def _normalize_orderbook_side(raw_levels: Any) -> list[tuple[float, float]]:
    if not isinstance(raw_levels, list):
        return []
    levels: list[tuple[float, float]] = []
    for raw_level in raw_levels:
        if not isinstance(raw_level, (list, tuple)) or len(raw_level) < 2:
            continue
        price = _coerce_float(raw_level[0])
        quantity = _coerce_quantity(raw_level[1])
        if price is None or quantity is None:
            continue
        levels.append((round(price, 6), round(quantity, 6)))
    return sorted(levels, key=lambda item: item[0], reverse=True)


def _build_snapshot_frame(rows: list[dict[str, Any]]) -> pd.DataFrame:
    return pd.DataFrame(rows, columns=SNAPSHOT_COLUMNS)


def _build_level_frame(rows: list[dict[str, Any]]) -> pd.DataFrame:
    return pd.DataFrame(rows, columns=LEVEL_COLUMNS)


def _append_or_create_parquet(
    path: Path,
    frame: pd.DataFrame,
    unique_keys: list[str],
    sort_keys: list[str],
) -> int:
    if path.exists():
        existing = pd.read_parquet(path)
        combined = pd.concat([existing, frame], ignore_index=True)
    else:
        combined = frame.copy()
    if combined.empty:
        combined = combined.reindex(columns=frame.columns)
    combined = combined.drop_duplicates(subset=unique_keys, keep="last")
    if not combined.empty:
        combined = combined.sort_values(sort_keys, kind="stable").reset_index(drop=True)
    path.parent.mkdir(parents=True, exist_ok=True)
    combined.to_parquet(path, index=False)
    return int(len(combined))


def _best_level(levels: list[tuple[float, float]]) -> tuple[float | None, float | None]:
    if not levels:
        return None, None
    return levels[0]


def _coerce_cents_price(value: Any) -> float | None:
    if value in (None, ""):
        return None
    if isinstance(value, str):
        try:
            return round(float(value) * 100.0, 6)
        except ValueError:
            return None
    if isinstance(value, (int, float)):
        return round(float(value), 6)
    return None


def _coerce_quantity(value: Any) -> float | None:
    if value in (None, ""):
        return None
    try:
        return round(float(value), 6)
    except (TypeError, ValueError):
        return None


def _coerce_float(value: Any) -> float | None:
    if value in (None, ""):
        return None
    try:
        return round(float(value), 6)
    except (TypeError, ValueError):
        return None


def _coerce_bool(value: Any) -> bool | None:
    if value is None:
        return None
    if isinstance(value, bool):
        return value
    if isinstance(value, str):
        lowered = value.strip().lower()
        if lowered in {"true", "1", "yes"}:
            return True
        if lowered in {"false", "0", "no"}:
            return False
    return None


def _spread(bid_cents: float | None, ask_cents: float | None) -> float | None:
    if bid_cents is None or ask_cents is None:
        return None
    return round(ask_cents - bid_cents, 6)


def _json_or_none(value: Any) -> str | None:
    if value is None:
        return None
    return json.dumps(value, sort_keys=True)


def _build_kalshi_client() -> "KalshiClient":
    from kwb.clients.kalshi import KalshiClient

    return KalshiClient()
