from __future__ import annotations

import json
from collections.abc import Iterable
from datetime import datetime, timezone
from pathlib import Path
from typing import TYPE_CHECKING, Any

import pandas as pd

from kwb.mapping.settlement_sources import extract_settlement_sources
from kwb.settings import CONFIG_DIR, STAGING_DIR
from kwb.utils.io import read_yaml, write_yaml
from kwb.utils.logging import get_logger

if TYPE_CHECKING:
    from kwb.clients.kalshi import KalshiClient

logger = get_logger(__name__)

DEFAULT_CITIES_CONFIG_PATH = CONFIG_DIR / "cities.yml"
DEFAULT_EVENTS_FILENAME = "kalshi_events.parquet"


def load_city_configs(config_path: Path = DEFAULT_CITIES_CONFIG_PATH) -> list[dict[str, Any]]:
    """Load raw city configuration rows from YAML."""
    payload = read_yaml(config_path)
    cities = payload.get("cities", [])
    return [city for city in cities if isinstance(city, dict)]


def load_enabled_cities(config_path: Path = DEFAULT_CITIES_CONFIG_PATH) -> list[dict[str, Any]]:
    """Return enabled cities that already have a Kalshi series ticker configured."""
    cities = load_city_configs(config_path)
    enabled = [city for city in cities if city.get("enabled")]
    return [city for city in enabled if city.get("kalshi_series_ticker")]


def ingest_enabled_city_events(
    config_path: Path = DEFAULT_CITIES_CONFIG_PATH,
    output_dir: Path | None = None,
    client: "KalshiClient | None" = None,
    update_city_config: bool = True,
) -> Path:
    """Ingest event metadata for enabled cities and write a normalized staging parquet."""
    cities = load_enabled_cities(config_path)
    if not cities:
        raise ValueError(f"No enabled cities with Kalshi series tickers found in {config_path}.")

    client = client or _build_kalshi_client()
    output_dir = output_dir or STAGING_DIR
    output_dir.mkdir(parents=True, exist_ok=True)

    rows: list[dict[str, Any]] = []
    fetched_at = datetime.now(timezone.utc).isoformat()

    for city in cities:
        city_rows = _ingest_city_events(city=city, client=client, fetched_at=fetched_at)
        rows.extend(city_rows)

    df = _build_events_frame(rows)
    outpath = output_dir / DEFAULT_EVENTS_FILENAME
    df.to_parquet(outpath, index=False)
    logger.info("Saved %s Kalshi event rows to %s", len(df), outpath)

    if update_city_config:
        _update_city_config_with_sources(config_path=config_path, events_df=df)

    return outpath


def ingest_events_for_series(
    series_ticker: str,
    output_dir: Path | None = None,
    client: "KalshiClient | None" = None,
) -> Path:
    """Ingest event metadata for a single Kalshi series ticker."""
    client = client or _build_kalshi_client()
    output_dir = output_dir or STAGING_DIR
    output_dir.mkdir(parents=True, exist_ok=True)

    city = {
        "city_key": series_ticker.lower(),
        "city_name": series_ticker,
        "timezone": None,
        "kalshi_series_ticker": series_ticker,
    }
    fetched_at = datetime.now(timezone.utc).isoformat()
    rows = _ingest_city_events(city=city, client=client, fetched_at=fetched_at)
    df = _build_events_frame(rows)

    outpath = output_dir / f"kalshi_events_{series_ticker}.parquet"
    df.to_parquet(outpath, index=False)
    logger.info("Saved %s rows to %s", len(df), outpath)
    return outpath


def _ingest_city_events(
    city: dict[str, Any],
    client: "KalshiClient",
    fetched_at: str,
) -> list[dict[str, Any]]:
    series_ticker = city["kalshi_series_ticker"]
    event_summaries = _fetch_all_events_for_series(client=client, series_ticker=series_ticker)
    logger.info(
        "Fetched %s event summaries for city=%s series=%s",
        len(event_summaries),
        city.get("city_key"),
        series_ticker,
    )

    rows: list[dict[str, Any]] = []
    for event_summary in event_summaries:
        event_ticker = event_summary.get("event_ticker") or event_summary.get("ticker")
        if not event_ticker:
            continue

        event_detail = client.get_event(event_ticker)
        event_metadata = client.get_event_metadata(event_ticker)
        rows.append(
            _build_event_row(
                city=city,
                event_summary=event_summary,
                event_detail=event_detail,
                event_metadata=event_metadata,
                fetched_at=fetched_at,
            )
        )

    return rows


def _fetch_all_events_for_series(
    client: "KalshiClient",
    series_ticker: str,
    page_size: int = 200,
) -> list[dict[str, Any]]:
    events: list[dict[str, Any]] = []
    cursor: str | None = None

    while True:
        payload = client.get_events(
            series_ticker=series_ticker,
            limit=page_size,
            cursor=cursor,
            with_nested_markets=False,
        )
        page_events = payload.get("events", [])
        if not isinstance(page_events, list):
            raise TypeError(f"Expected list of events for series {series_ticker}, got {type(page_events)}")

        events.extend(event for event in page_events if isinstance(event, dict))
        cursor = payload.get("cursor")
        if not cursor:
            break

    return events


def _build_event_row(
    city: dict[str, Any],
    event_summary: dict[str, Any],
    event_detail: dict[str, Any],
    event_metadata: dict[str, Any],
    fetched_at: str,
) -> dict[str, Any]:
    detail_event = event_detail.get("event", event_detail)
    if not isinstance(detail_event, dict):
        detail_event = {}

    settlement_sources = extract_settlement_sources(event_metadata) or extract_settlement_sources(event_detail)
    primary_source = settlement_sources[0] if settlement_sources else {}

    return {
        "city_key": city.get("city_key"),
        "city_name": city.get("city_name"),
        "timezone": city.get("timezone"),
        "series_ticker": city.get("kalshi_series_ticker") or event_summary.get("series_ticker"),
        "event_ticker": detail_event.get("event_ticker") or event_summary.get("event_ticker"),
        "event_title": detail_event.get("title") or event_summary.get("title"),
        "event_subtitle": detail_event.get("sub_title")
        or detail_event.get("subtitle")
        or event_summary.get("sub_title")
        or event_summary.get("subtitle"),
        "strike_date": detail_event.get("strike_date") or event_summary.get("strike_date"),
        "strike_period": detail_event.get("strike_period") or event_summary.get("strike_period"),
        "event_last_updated_ts": detail_event.get("last_updated_ts") or event_summary.get("last_updated_ts"),
        "settlement_source_count": len(settlement_sources),
        "settlement_source_name": primary_source.get("name"),
        "settlement_source_url": primary_source.get("url"),
        "settlement_source_names": json.dumps(_dedupe_preserve_order(_source_field_values(settlement_sources, "name"))),
        "settlement_source_urls": json.dumps(_dedupe_preserve_order(_source_field_values(settlement_sources, "url"))),
        "settlement_sources_json": json.dumps(settlement_sources, sort_keys=True),
        "data_fetched_at": fetched_at,
    }


def _build_events_frame(rows: list[dict[str, Any]]) -> pd.DataFrame:
    columns = [
        "city_key",
        "city_name",
        "timezone",
        "series_ticker",
        "event_ticker",
        "event_title",
        "event_subtitle",
        "strike_date",
        "strike_period",
        "event_last_updated_ts",
        "settlement_source_count",
        "settlement_source_name",
        "settlement_source_url",
        "settlement_source_names",
        "settlement_source_urls",
        "settlement_sources_json",
        "data_fetched_at",
    ]
    df = pd.DataFrame(rows, columns=columns)
    if not df.empty:
        df = df.sort_values(["city_key", "strike_date", "event_ticker"], kind="stable").reset_index(drop=True)
    return df


def _update_city_config_with_sources(config_path: Path, events_df: pd.DataFrame) -> None:
    payload = read_yaml(config_path)
    cities = payload.get("cities", [])
    if not isinstance(cities, list):
        raise TypeError(f"Expected 'cities' list in {config_path}.")

    changed = False
    for city in cities:
        if not isinstance(city, dict):
            continue

        city_key = city.get("city_key")
        if not city_key:
            continue

        source_pair = _resolve_unique_city_source(events_df=events_df, city_key=city_key)
        if source_pair is None:
            continue

        discovered_name, discovered_url = source_pair
        existing_name = city.get("settlement_source_name")
        existing_url = city.get("settlement_source_url")

        if existing_name and discovered_name and existing_name != discovered_name:
            raise ValueError(
                f"Config/source mismatch for city_key={city_key}: "
                f"{existing_name!r} != {discovered_name!r}"
            )
        if existing_url and discovered_url and existing_url != discovered_url:
            raise ValueError(
                f"Config/source mismatch for city_key={city_key}: "
                f"{existing_url!r} != {discovered_url!r}"
            )

        if existing_name != discovered_name:
            city["settlement_source_name"] = discovered_name
            changed = True
        if existing_url != discovered_url:
            city["settlement_source_url"] = discovered_url
            changed = True

    if changed:
        write_yaml(config_path, payload)
        logger.info("Updated settlement-source fields in %s", config_path)


def _resolve_unique_city_source(
    events_df: pd.DataFrame,
    city_key: str,
) -> tuple[str | None, str | None] | None:
    city_rows = events_df.loc[events_df["city_key"] == city_key]
    if city_rows.empty:
        return None

    source_pairs = {
        (row["settlement_source_name"], row["settlement_source_url"])
        for row in city_rows[["settlement_source_name", "settlement_source_url"]].to_dict("records")
        if row["settlement_source_name"] or row["settlement_source_url"]
    }
    if not source_pairs:
        return None
    if len(source_pairs) > 1:
        raise ValueError(
            f"Found multiple settlement sources for city_key={city_key}: {sorted(source_pairs)!r}"
        )
    return next(iter(source_pairs))


def _source_field_values(sources: list[dict[str, Any]], field: str) -> list[str]:
    values: list[str] = []
    for source in sources:
        value = source.get(field)
        if isinstance(value, str) and value:
            values.append(value)
    return values


def _dedupe_preserve_order(values: Iterable[str]) -> list[str]:
    seen: set[str] = set()
    deduped: list[str] = []
    for value in values:
        if value in seen:
            continue
        seen.add(value)
        deduped.append(value)
    return deduped


def _build_kalshi_client() -> "KalshiClient":
    from kwb.clients.kalshi import KalshiClient

    return KalshiClient()
