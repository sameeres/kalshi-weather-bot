from __future__ import annotations

import re
import time
from datetime import date, datetime, timedelta, timezone
from pathlib import Path
from typing import TYPE_CHECKING, Any
from zoneinfo import ZoneInfo

import pandas as pd

from kwb.ingestion.kalshi_events import DEFAULT_CITIES_CONFIG_PATH, load_enabled_cities
from kwb.ingestion.nws_forecast import DEFAULT_FORECAST_SNAPSHOTS_FILENAME, _merge_snapshot_frames
from kwb.settings import STAGING_DIR
from kwb.utils.logging import get_logger

if TYPE_CHECKING:
    from kwb.clients.iem import IEMClient

logger = get_logger(__name__)

# Select the ZFP issued before this local time (the trading decision time).
# Must match what build_backtest_dataset uses.
DECISION_TIME_LOCAL = "10:00"

# Throttle between requests to be polite to IEM.
_REQUEST_DELAY_SECONDS = 0.5


class IEMHistoricalForecastError(ValueError):
    """Raised when IEM historical forecast ingestion cannot complete safely."""


def fetch_iem_historical_forecasts(
    start_date: date | str,
    end_date: date | str,
    config_path: Path = DEFAULT_CITIES_CONFIG_PATH,
    output_dir: Path | None = None,
    client: "IEMClient | None" = None,
    append: bool = True,
    request_delay: float = _REQUEST_DELAY_SECONDS,
) -> Path:
    """Fetch historical NWS Zone Forecast Product data from IEM AFOS archive.

    For each enabled city with a ``nws_afos_pil`` and for each calendar date in
    [start_date, end_date], queries IEM for the ZFP issued most recently before
    the city's 10:00 AM local decision time.  Parses the "High near XX" (and
    equivalent) phrase from the ``.TODAY...`` section and writes one row per
    event_date per city into ``nws_forecast_hourly_snapshots.parquet`` using the
    same schema as the live NWS hourly ingestion.

    Two API calls are made per date per city:
    1. ``/api/1/nws/afos/list.json`` — list product IDs for the PIL + date.
    2. ``/api/1/nwstext/{product_id}`` — fetch the text of the chosen product.

    Args:
        start_date: First event_date to backfill (inclusive).
        end_date: Last event_date to backfill (inclusive).
        config_path: Path to cities.yml.
        output_dir: Directory for the output parquet (defaults to STAGING_DIR).
        client: IEMClient instance (created automatically if None).
        append: If True, merge into the existing snapshot parquet; otherwise overwrite.
        request_delay: Seconds to sleep between API calls.

    Returns:
        Path to the written/updated ``nws_forecast_hourly_snapshots.parquet``.
    """
    start = _parse_date(start_date)
    end = _parse_date(end_date)
    if end < start:
        raise IEMHistoricalForecastError(f"end_date {end} must be >= start_date {start}")

    cities = [c for c in load_enabled_cities(config_path) if c.get("nws_afos_pil")]
    if not cities:
        raise IEMHistoricalForecastError(
            f"No enabled cities with nws_afos_pil found in {config_path}."
        )

    if client is None:
        from kwb.clients.iem import IEMClient as _IEMClient
        client = _IEMClient()

    output_dir = output_dir or STAGING_DIR
    output_dir.mkdir(parents=True, exist_ok=True)
    outpath = output_dir / DEFAULT_FORECAST_SNAPSHOTS_FILENAME

    date_range = [start + timedelta(days=i) for i in range((end - start).days + 1)]
    rows: list[dict[str, Any]] = []
    skipped = 0

    for city in cities:
        city_key = city["city_key"]
        pil: str = city["nws_afos_pil"]
        tz = ZoneInfo(city["timezone"])
        logger.info(
            "Fetching IEM ZFP history city=%s pil=%s %s → %s",
            city_key, pil, start, end,
        )

        for event_date in date_range:
            # Decision time in UTC: 10:00 AM local on event_date
            decision_ts_utc = _local_decision_ts(event_date=event_date, tz=tz)

            result = _fetch_product_for_date(
                client=client,
                pil=pil,
                event_date=event_date,
                decision_ts_utc=decision_ts_utc,
                request_delay=request_delay,
            )
            if result is None:
                logger.debug("No IEM product found city=%s date=%s", city_key, event_date)
                skipped += 1
                continue

            product_text, entered_utc = result
            row = _parse_zfp_product(
                text=product_text,
                city=city,
                event_date=event_date,
                snapshot_ts_utc=entered_utc,
            )
            if row is None:
                logger.debug("Could not parse ZFP high temp city=%s date=%s", city_key, event_date)
                skipped += 1
                continue

            rows.append(row)

    logger.info(
        "Parsed %d IEM ZFP rows, skipped %d for %d cities over %d dates",
        len(rows), skipped, len(cities), len(date_range),
    )

    fresh_df = _build_forecast_snapshot_frame(rows)

    if append and outpath.exists():
        existing_df = pd.read_parquet(outpath)
        df = _merge_snapshot_frames(existing_df=existing_df, fresh_df=fresh_df)
    else:
        df = fresh_df

    df.to_parquet(outpath, index=False)
    logger.info("Saved %d total NWS snapshot rows to %s", len(df), outpath)
    return outpath


def _fetch_product_for_date(
    client: "IEMClient",
    pil: str,
    event_date: date,
    decision_ts_utc: datetime,
    request_delay: float,
) -> tuple[str, datetime] | None:
    """List + fetch the most recent ZFP before the decision time.

    The list endpoint is per-calendar-date (UTC).  Since the decision time (10 AM
    local) can be 14–16 UTC depending on timezone, the relevant ZFP may have been
    issued earlier the same UTC date (e.g. 08:00 UTC) OR late on the previous UTC
    date (e.g. 22:00 UTC = 5 PM local night before).  We query both days and pick
    the best match.
    """
    try:
        result = client.fetch_afos_text_before(
            pil=pil,
            product_date=event_date,
            before_utc=decision_ts_utc,
        )
        if request_delay > 0:
            time.sleep(request_delay)

        # If no same-day match, check the previous UTC date (covers midnight-local)
        if result is None:
            prev_date = event_date - timedelta(days=1)
            result = client.fetch_afos_text_before(
                pil=pil,
                product_date=prev_date,
                before_utc=decision_ts_utc,
            )
            if request_delay > 0:
                time.sleep(request_delay)

        return result
    except Exception as exc:
        logger.warning("IEM fetch failed pil=%s date=%s: %s", pil, event_date, exc)
        return None


# ---------------------------------------------------------------------------
# ZFP text parsing
# ---------------------------------------------------------------------------

_HIGH_EXACT_RE = re.compile(
    r"\bHigh(?:s)?\s+(?:near|around|of|at)?\s*(\d{2,3})\b",
    re.IGNORECASE,
)
_HIGH_RANGE_RE = re.compile(
    r"\bHigh(?:s)?\s+in\s+the\s+(lower?|mid(?:dle)?|upper?|high)\s+(\d{2})s\b",
    re.IGNORECASE,
)
_HIGH_SPAN_RE = re.compile(
    r"\bHigh(?:s)?\s+(\d{2,3})\s+to\s+(\d{2,3})\b",
    re.IGNORECASE,
)
# "Near steady temperature in the mid 40s" / "Near steady temperature around 45"
# Used when temperatures change little (e.g. rainy or overcast winter days).
_STEADY_EXACT_RE = re.compile(
    r"(?:Near\s+)?steady\s+temperature(?:s)?\s+(?:near|around|of|at)?\s*(\d{2,3})\b",
    re.IGNORECASE,
)
_STEADY_RANGE_RE = re.compile(
    r"(?:Near\s+)?steady\s+temperature(?:s)?\s+in\s+the\s+(lower?|mid(?:dle)?|upper?|high)\s+(\d{2})s\b",
    re.IGNORECASE,
)


def _parse_zfp_product(
    text: str,
    city: dict[str, Any],
    event_date: date,
    snapshot_ts_utc: datetime,
) -> dict[str, Any] | None:
    """Parse a ZFP text product into a forecast snapshot row.

    Extracts the forecast-high temperature from the first ``.TODAY...`` section.
    Returns None if the high temperature cannot be parsed.
    """
    today_text = _extract_today_section(text)
    if today_text is None:
        return None

    high_f = _extract_high_temp(today_text)
    if high_f is None:
        return None

    tz = ZoneInfo(city["timezone"])
    period_start_local = datetime(event_date.year, event_date.month, event_date.day, 0, 0, tzinfo=tz)
    period_end_local = datetime(event_date.year, event_date.month, event_date.day, 23, 59, tzinfo=tz)
    period_start_utc = period_start_local.astimezone(timezone.utc)
    period_end_utc = period_end_local.astimezone(timezone.utc)
    lead_hours = round((period_start_utc - snapshot_ts_utc).total_seconds() / 3600.0, 3)

    return {
        "snapshot_ts": snapshot_ts_utc.isoformat(),
        "city_key": city.get("city_key"),
        "city_name": city.get("city_name"),
        "timezone": city.get("timezone"),
        "series_ticker": city.get("kalshi_series_ticker"),
        "settlement_station_id": city.get("settlement_station_id"),
        "settlement_station_name": city.get("settlement_station_name"),
        "forecast_points_url": None,
        "forecast_hourly_url": f"iem://afos/{city.get('nws_afos_pil')}",
        "forecast_updated_at": None,
        "forecast_generated_at": snapshot_ts_utc.isoformat(),
        "period_number": 1,
        "period_name": "Today",
        "period_start_ts": period_start_utc.isoformat(),
        "period_end_ts": period_end_utc.isoformat(),
        "period_date_local": event_date.strftime("%Y-%m-%d"),
        "lead_hours": lead_hours,
        "temperature_f": round(float(high_f), 3),
        "temperature_unit": "F",
        "is_daytime": True,
        "short_forecast": f"High near {int(round(high_f))}",
    }


def _extract_today_section(text: str) -> str | None:
    """Return the text of the today forecast section, or None if not found.

    Matches both ``.TODAY...`` (morning issuance) and ``.REST OF TODAY...``
    (afternoon issuance), stopping at the next section header.
    """
    match = re.search(
        r"\.(?:REST\s+OF\s+)?TODAY\.\.\.(.*?)(?=\n\.[A-Z]|\Z)",
        text,
        re.IGNORECASE | re.DOTALL,
    )
    if match:
        return match.group(1).strip()
    return None


def _extract_high_temp(section_text: str) -> float | None:
    """Extract forecast high temperature (°F) from a ZFP section text."""
    # "Highs 45 to 50" → midpoint
    m = _HIGH_SPAN_RE.search(section_text)
    if m:
        return round((float(m.group(1)) + float(m.group(2))) / 2.0, 1)

    # "High near 58", "High around 52", "High of 45"
    m = _HIGH_EXACT_RE.search(section_text)
    if m:
        return float(m.group(1))

    # "High in the mid 50s", "Highs in the lower 60s", "High in the upper 40s"
    m = _HIGH_RANGE_RE.search(section_text)
    if m:
        return _decade_temp(m.group(1), m.group(2))

    # "Near steady temperature around 45" (rainy/isothermal days)
    m = _STEADY_EXACT_RE.search(section_text)
    if m:
        return float(m.group(1))

    # "Near steady temperature in the mid 40s"
    m = _STEADY_RANGE_RE.search(section_text)
    if m:
        return _decade_temp(m.group(1), m.group(2))

    return None


def _decade_temp(qualifier: str, decade_str: str) -> float:
    """Convert a decade qualifier and decade string to a temperature estimate."""
    decade = float(decade_str)
    q = qualifier.lower()
    if q in {"low", "lower"}:
        return decade + 2.0
    if q in {"mid", "middle"}:
        return decade + 5.0
    return decade + 8.0  # upper / high


def _local_decision_ts(event_date: date, tz: ZoneInfo) -> datetime:
    """Return the 10:00 AM local decision timestamp as UTC-aware datetime."""
    local_dt = datetime(event_date.year, event_date.month, event_date.day, 10, 0, tzinfo=tz)
    return local_dt.astimezone(timezone.utc)


def _build_forecast_snapshot_frame(rows: list[dict[str, Any]]) -> pd.DataFrame:
    columns = [
        "snapshot_ts", "city_key", "city_name", "timezone", "series_ticker",
        "settlement_station_id", "settlement_station_name", "forecast_points_url",
        "forecast_hourly_url", "forecast_updated_at", "forecast_generated_at",
        "period_number", "period_name", "period_start_ts", "period_end_ts",
        "period_date_local", "lead_hours", "temperature_f", "temperature_unit",
        "is_daytime", "short_forecast",
    ]
    df = pd.DataFrame(rows, columns=columns)
    if df.empty:
        return df
    return (
        df.drop_duplicates(subset=["snapshot_ts", "city_key", "period_start_ts"], keep="last")
        .sort_values(["snapshot_ts", "city_key", "period_start_ts"], kind="stable")
        .reset_index(drop=True)
    )


def _parse_date(value: date | str) -> date:
    if isinstance(value, date):
        return value
    return date.fromisoformat(str(value))
