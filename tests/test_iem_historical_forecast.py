from __future__ import annotations

from datetime import date, datetime, timezone
from pathlib import Path

import pandas as pd
import pytest

from kwb.ingestion.iem_historical_forecast import (
    IEMHistoricalForecastError,
    _extract_high_temp,
    _extract_today_section,
    _local_decision_ts,
    _parse_zfp_product,
    fetch_iem_historical_forecasts,
)

# ---------------------------------------------------------------------------
# _extract_high_temp
# ---------------------------------------------------------------------------

def test_extract_high_temp_near() -> None:
    assert _extract_high_temp("Mostly sunny. High near 58. Light winds.") == 58.0


def test_extract_high_temp_around() -> None:
    assert _extract_high_temp("Cloudy. High around 45.") == 45.0


def test_extract_high_temp_of() -> None:
    assert _extract_high_temp("Showers. High of 52.") == 52.0


def test_extract_high_temp_mid_decade() -> None:
    assert _extract_high_temp("High in the mid 50s.") == 55.0


def test_extract_high_temp_lower_decade() -> None:
    assert _extract_high_temp("Highs in the lower 60s.") == 62.0


def test_extract_high_temp_upper_decade() -> None:
    assert _extract_high_temp("High in the upper 40s.") == 48.0


def test_extract_high_temp_span() -> None:
    result = _extract_high_temp("Highs 45 to 50.")
    assert result == pytest.approx(47.5)


def test_extract_high_temp_none() -> None:
    assert _extract_high_temp("Mostly cloudy. Low around 32.") is None


def test_extract_high_temp_multiline() -> None:
    text = "Partly sunny.\nHigh near 63. Southeast winds around 10 mph."
    assert _extract_high_temp(text) == 63.0


# ---------------------------------------------------------------------------
# _extract_today_section
# ---------------------------------------------------------------------------

SAMPLE_ZFP = """\
267
FPUS51 KOKX 151012
ZFPOKX

Zone Forecast Product
National Weather Service New York NY
512 AM EST Sat Nov 15 2025

NYZ072-160000-
New York (Manhattan)-
512 AM EST Sat Nov 15 2025

.TODAY...Sunny early, then partly sunny with a slight chance of
showers this afternoon. Highs in the mid 50s. Northwest winds
around 5 mph.
.TONIGHT...Showers. Lows in the upper 40s.
.SUNDAY...Mostly sunny. Highs in the mid 50s.
"""


def test_extract_today_section_found() -> None:
    section = _extract_today_section(SAMPLE_ZFP)
    assert section is not None
    assert "Highs in the mid 50s" in section
    # Should not bleed into TONIGHT
    assert "Lows in the upper 40s" not in section


def test_extract_today_section_missing() -> None:
    assert _extract_today_section("No today section here.") is None


# ---------------------------------------------------------------------------
# _local_decision_ts
# ---------------------------------------------------------------------------

def test_local_decision_ts_nyc_est() -> None:
    from zoneinfo import ZoneInfo
    tz = ZoneInfo("America/New_York")
    ts = _local_decision_ts(date(2025, 11, 15), tz)
    # Nov 15 = EST (UTC-5): 10 AM EST = 15:00 UTC
    assert ts.hour == 15
    assert ts.minute == 0
    assert ts.tzinfo == timezone.utc


def test_local_decision_ts_chicago_cst() -> None:
    from zoneinfo import ZoneInfo
    tz = ZoneInfo("America/Chicago")
    ts = _local_decision_ts(date(2025, 11, 15), tz)
    # Nov 15 = CST (UTC-6): 10 AM CST = 16:00 UTC
    assert ts.hour == 16


# ---------------------------------------------------------------------------
# _parse_zfp_product
# ---------------------------------------------------------------------------

CITY = {
    "city_key": "nyc",
    "city_name": "New York City",
    "timezone": "America/New_York",
    "kalshi_series_ticker": "KXHIGHNY",
    "settlement_station_id": "KNYC",
    "settlement_station_name": "Central Park",
    "nws_afos_pil": "ZFPOKX",
}


def test_parse_zfp_product_returns_row() -> None:
    event_date = date(2025, 11, 15)
    snapshot_ts = datetime(2025, 11, 15, 10, 12, tzinfo=timezone.utc)
    row = _parse_zfp_product(
        text=SAMPLE_ZFP, city=CITY, event_date=event_date, snapshot_ts_utc=snapshot_ts
    )
    assert row is not None
    assert row["city_key"] == "nyc"
    assert row["temperature_f"] == 55.0  # mid 50s → 55
    assert row["period_date_local"] == "2025-11-15"
    assert row["is_daytime"] is True
    assert row["temperature_unit"] == "F"
    snap_ts = pd.to_datetime(row["snapshot_ts"], utc=True)
    assert snap_ts.hour == 10
    assert snap_ts.minute == 12


def test_parse_zfp_product_no_today_section() -> None:
    text = "FPUS51 KOKX 141000\n\nNo forecast sections."
    event_date = date(2025, 11, 15)
    snapshot_ts = datetime(2025, 11, 15, 10, 12, tzinfo=timezone.utc)
    assert _parse_zfp_product(
        text=text, city=CITY, event_date=event_date, snapshot_ts_utc=snapshot_ts
    ) is None


def test_parse_zfp_product_no_high_temp() -> None:
    text = "FPUS51 KOKX 141000\n\nZFPOKX\n\n.TODAY...\nMostly cloudy. Low around 32.\n"
    event_date = date(2025, 11, 15)
    snapshot_ts = datetime(2025, 11, 15, 10, 12, tzinfo=timezone.utc)
    assert _parse_zfp_product(
        text=text, city=CITY, event_date=event_date, snapshot_ts_utc=snapshot_ts
    ) is None


# ---------------------------------------------------------------------------
# fetch_iem_historical_forecasts (end-to-end with fake client)
# ---------------------------------------------------------------------------

class _FakeIEMClient:
    """Fake IEM client that returns a fixed product for any PIL/date."""

    def __init__(self, product_text: str = SAMPLE_ZFP) -> None:
        self._product_text = product_text
        self.calls: list[dict] = []

    def fetch_afos_text_before(
        self, pil: str, product_date: date, before_utc: datetime
    ) -> tuple[str, datetime] | None:
        self.calls.append({"pil": pil, "product_date": product_date})
        # Simulate a product issued at 10:12 UTC on the product_date
        entered_utc = datetime(
            product_date.year, product_date.month, product_date.day, 10, 12,
            tzinfo=timezone.utc,
        )
        if entered_utc < before_utc:
            return self._product_text, entered_utc
        return None


def _write_cities_yml(tmp_path: Path) -> Path:
    config_path = tmp_path / "cities.yml"
    config_path.write_text(
        """
cities:
  - city_key: nyc
    city_name: New York City
    timezone: America/New_York
    kalshi_series_ticker: KXHIGHNY
    settlement_station_id: KNYC
    settlement_station_name: Central Park
    station_lat: 40.7789
    station_lon: -73.9692
    nws_afos_pil: ZFPOKX
    enabled: true
"""
    )
    return config_path


def test_fetch_iem_historical_forecasts_writes_parquet(tmp_path: Path) -> None:
    config_path = _write_cities_yml(tmp_path)
    fake_client = _FakeIEMClient()

    outpath = fetch_iem_historical_forecasts(
        start_date="2025-11-14",
        end_date="2025-11-15",
        config_path=config_path,
        output_dir=tmp_path,
        client=fake_client,
        append=False,
        request_delay=0.0,
    )

    assert outpath.exists()
    df = pd.read_parquet(outpath)
    assert len(df) == 2
    assert set(df["city_key"].unique()) == {"nyc"}
    assert set(df["period_date_local"].unique()) == {"2025-11-14", "2025-11-15"}
    assert (df["temperature_f"] == 55.0).all()
    assert (df["is_daytime"] == True).all()


def test_fetch_iem_historical_forecasts_appends_to_existing(tmp_path: Path) -> None:
    config_path = _write_cities_yml(tmp_path)
    fake_client = _FakeIEMClient()

    fetch_iem_historical_forecasts(
        start_date="2025-11-14",
        end_date="2025-11-14",
        config_path=config_path,
        output_dir=tmp_path,
        client=fake_client,
        append=False,
        request_delay=0.0,
    )
    fetch_iem_historical_forecasts(
        start_date="2025-11-15",
        end_date="2025-11-15",
        config_path=config_path,
        output_dir=tmp_path,
        client=fake_client,
        append=True,
        request_delay=0.0,
    )

    df = pd.read_parquet(tmp_path / "nws_forecast_hourly_snapshots.parquet")
    assert len(df) == 2
    assert set(df["period_date_local"].unique()) == {"2025-11-14", "2025-11-15"}


def test_fetch_iem_historical_forecasts_skips_none_response(tmp_path: Path) -> None:
    config_path = _write_cities_yml(tmp_path)

    class _NoneClient:
        def fetch_afos_text_before(self, pil, product_date, before_utc):
            return None

    outpath = fetch_iem_historical_forecasts(
        start_date="2025-11-14",
        end_date="2025-11-14",
        config_path=config_path,
        output_dir=tmp_path,
        client=_NoneClient(),
        append=False,
        request_delay=0.0,
    )

    df = pd.read_parquet(outpath)
    assert df.empty


def test_fetch_iem_historical_forecasts_invalid_date_range(tmp_path: Path) -> None:
    config_path = _write_cities_yml(tmp_path)
    with pytest.raises(IEMHistoricalForecastError, match="end_date"):
        fetch_iem_historical_forecasts(
            start_date="2025-11-20",
            end_date="2025-11-14",
            config_path=config_path,
            output_dir=tmp_path,
            client=_FakeIEMClient(),
            request_delay=0.0,
        )


def test_fetch_iem_historical_forecasts_no_pil_in_config(tmp_path: Path) -> None:
    config_path = tmp_path / "cities.yml"
    config_path.write_text(
        """
cities:
  - city_key: nyc
    city_name: New York City
    timezone: America/New_York
    kalshi_series_ticker: KXHIGHNY
    settlement_station_id: KNYC
    settlement_station_name: Central Park
    station_lat: 40.7789
    station_lon: -73.9692
    enabled: true
"""
    )
    with pytest.raises(IEMHistoricalForecastError, match="nws_afos_pil"):
        fetch_iem_historical_forecasts(
            start_date="2025-11-14",
            end_date="2025-11-14",
            config_path=config_path,
            output_dir=tmp_path,
            client=_FakeIEMClient(),
            request_delay=0.0,
        )
