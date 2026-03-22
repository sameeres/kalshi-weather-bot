from __future__ import annotations

from datetime import date, datetime, time, timezone
from pathlib import Path
from typing import Any
from zoneinfo import ZoneInfo

import pandas as pd

from kwb.ingestion.kalshi_events import DEFAULT_CITIES_CONFIG_PATH, load_enabled_cities
from kwb.settings import MARTS_DIR, STAGING_DIR
from kwb.utils.logging import get_logger

logger = get_logger(__name__)

DEFAULT_BACKTEST_DATASET_FILENAME = "backtest_dataset.parquet"

DEFAULT_STAGED_WEATHER_PATH = STAGING_DIR / "weather_daily.parquet"
DEFAULT_STAGED_NORMALS_PATH = STAGING_DIR / "weather_normals_daily.parquet"
DEFAULT_STAGED_MARKETS_PATH = STAGING_DIR / "kalshi_markets.parquet"
DEFAULT_STAGED_CANDLES_PATH = STAGING_DIR / "kalshi_candles.parquet"

REQUIRED_WEATHER_COLUMNS = {"station_id", "city_key", "obs_date", "tmax_f"}
REQUIRED_NORMALS_COLUMNS = {"station_id", "city_key", "month_day", "normal_tmax_f", "normals_period", "normals_source"}
REQUIRED_MARKETS_COLUMNS = {
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
}
REQUIRED_CANDLES_COLUMNS = {"city_key", "market_ticker", "candle_ts", "close", "interval"}


class BacktestDatasetBuildError(ValueError):
    """Raised when staged inputs cannot produce a valid backtest-ready mart."""


def build_backtest_dataset(
    decision_time_local: str,
    config_path: Path = DEFAULT_CITIES_CONFIG_PATH,
    weather_path: Path | None = None,
    normals_path: Path | None = None,
    markets_path: Path | None = None,
    candles_path: Path | None = None,
    output_dir: Path | None = None,
) -> tuple[Path, dict[str, int | str]]:
    """Build a point-in-time-safe backtest dataset from staged local inputs only."""
    weather_path = weather_path or DEFAULT_STAGED_WEATHER_PATH
    normals_path = normals_path or DEFAULT_STAGED_NORMALS_PATH
    markets_path = markets_path or DEFAULT_STAGED_MARKETS_PATH
    candles_path = candles_path or DEFAULT_STAGED_CANDLES_PATH
    output_dir = output_dir or MARTS_DIR
    output_dir.mkdir(parents=True, exist_ok=True)

    city_index = _load_enabled_city_index(config_path)
    _ensure_required_inputs_exist([weather_path, normals_path, markets_path, candles_path])

    weather_df = _load_staged_frame(weather_path, REQUIRED_WEATHER_COLUMNS)
    normals_df = _load_staged_frame(normals_path, REQUIRED_NORMALS_COLUMNS)
    markets_df = _load_staged_frame(markets_path, REQUIRED_MARKETS_COLUMNS)
    candles_df = _load_staged_frame(candles_path, REQUIRED_CANDLES_COLUMNS)

    merged_df, stats = _build_backtest_frame(
        weather_df=weather_df,
        normals_df=normals_df,
        markets_df=markets_df,
        candles_df=candles_df,
        city_index=city_index,
        decision_time_local=decision_time_local,
    )

    outpath = output_dir / DEFAULT_BACKTEST_DATASET_FILENAME
    merged_df.to_parquet(outpath, index=False)
    stats["rows_written"] = len(merged_df)
    stats["cities_covered"] = merged_df["city_key"].nunique() if not merged_df.empty else 0

    logger.info(
        "Saved %s backtest dataset rows across %s cities to %s using decision_time_local=%s",
        stats["rows_written"],
        stats["cities_covered"],
        outpath,
        decision_time_local,
    )
    return outpath, stats


def _build_backtest_frame(
    weather_df: pd.DataFrame,
    normals_df: pd.DataFrame,
    markets_df: pd.DataFrame,
    candles_df: pd.DataFrame,
    city_index: dict[str, dict[str, Any]],
    decision_time_local: str,
) -> tuple[pd.DataFrame, dict[str, int | str]]:
    decision_time = _parse_decision_time_local(decision_time_local)
    market_rows = _prepare_market_rows(markets_df=markets_df, city_index=city_index, decision_time=decision_time)
    candle_rows = _prepare_candles(candles_df)
    weather_rows = _prepare_weather(weather_df)
    normals_rows = _prepare_normals(normals_df)

    decision_rows = _attach_decision_candles(market_rows, candle_rows)
    merged = decision_rows.merge(weather_rows, how="left", on=["city_key", "event_date"])
    merged = merged.merge(normals_rows, how="left", on=["city_key", "month_day"])

    merged["actual_tmax_f"] = merged["tmax_f"]
    merged["tmax_anomaly_f"] = (merged["actual_tmax_f"] - merged["normal_tmax_f"]).round(3)
    merged["resolved_yes"] = merged.apply(_resolve_market_bucket_row, axis=1)

    total_rows = len(merged)
    missing_price_rows = int(merged["decision_price"].isna().sum())
    missing_weather_rows = int(merged["actual_tmax_f"].isna().sum())
    missing_normals_rows = int(merged["normal_tmax_f"].isna().sum())

    merged = merged.loc[
        merged["decision_price"].notna() & merged["actual_tmax_f"].notna() & merged["normal_tmax_f"].notna()
    ].copy()
    merged = merged.sort_values(["city_key", "event_date", "market_ticker"], kind="stable").reset_index(drop=True)
    merged["strike_date"] = merged["strike_date"].map(lambda value: value.isoformat() if pd.notna(value) else None)
    merged["decision_ts"] = merged["decision_ts"].map(lambda value: value.isoformat() if pd.notna(value) else None)

    columns = [
        "city_key",
        "city_name",
        "timezone",
        "series_ticker",
        "event_ticker",
        "market_ticker",
        "strike_date",
        "event_date",
        "month_day",
        "market_title",
        "market_subtitle",
        "status",
        "floor_strike",
        "cap_strike",
        "strike_type",
        "decision_time_local",
        "decision_ts",
        "decision_candle_ts",
        "decision_price",
        "candle_interval",
        "actual_tmax_f",
        "normal_tmax_f",
        "tmax_anomaly_f",
        "weather_station_id",
        "normals_station_id",
        "normals_period",
        "normals_source",
        "resolved_yes",
    ]
    merged = merged.reindex(columns=columns)

    stats: dict[str, int | str] = {
        "decision_time_local": decision_time_local,
        "input_market_rows": total_rows,
        "dropped_missing_price_rows": missing_price_rows,
        "dropped_missing_weather_rows": missing_weather_rows,
        "dropped_missing_normals_rows": missing_normals_rows,
        "dropped_total_rows": total_rows - len(merged),
    }
    return merged, stats


def _load_enabled_city_index(config_path: Path) -> dict[str, dict[str, Any]]:
    cities = load_enabled_cities(config_path)
    if not cities:
        raise BacktestDatasetBuildError(f"No enabled cities with Kalshi series tickers found in {config_path}.")
    if len(cities) > 3:
        raise BacktestDatasetBuildError(f"Enabled city count exceeds MVP limit: found {len(cities)} enabled cities.")

    index: dict[str, dict[str, Any]] = {}
    for city in cities:
        city_key = city.get("city_key")
        timezone_name = city.get("timezone")
        if not city_key:
            raise BacktestDatasetBuildError(f"Enabled city row is missing city_key in {config_path}.")
        if city_key in index:
            raise BacktestDatasetBuildError(f"Duplicate enabled city_key found in {config_path}: {city_key}")
        if not timezone_name:
            raise BacktestDatasetBuildError(f"Enabled city {city_key} is missing timezone in {config_path}.")
        index[city_key] = city

    return index


def _ensure_required_inputs_exist(paths: list[Path]) -> None:
    missing = [str(path) for path in paths if not path.exists()]
    if missing:
        raise BacktestDatasetBuildError(
            "Required staged input parquet files are missing:\n" + "\n".join(missing)
        )


def _load_staged_frame(path: Path, required_columns: set[str]) -> pd.DataFrame:
    frame = pd.read_parquet(path)
    missing_columns = sorted(required_columns - set(frame.columns))
    if missing_columns:
        raise BacktestDatasetBuildError(
            f"Required columns are missing from {path}: {', '.join(missing_columns)}"
        )
    return frame.copy()


def _prepare_market_rows(
    markets_df: pd.DataFrame,
    city_index: dict[str, dict[str, Any]],
    decision_time: time,
) -> pd.DataFrame:
    df = markets_df.copy()
    df = df.loc[df["city_key"].isin(city_index)].copy()
    df["strike_date"] = pd.to_datetime(df["strike_date"], utc=True, errors="coerce")
    if df["strike_date"].isna().any():
        raise BacktestDatasetBuildError("kalshi_markets.parquet contains invalid strike_date values.")

    df["event_date"] = df["strike_date"].dt.strftime("%Y-%m-%d")
    df["month_day"] = df["strike_date"].dt.strftime("%m-%d")
    df["city_name"] = df["city_key"].map(lambda key: city_index[key].get("city_name"))
    df["timezone"] = df["city_key"].map(lambda key: city_index[key]["timezone"])
    df["decision_time_local"] = decision_time.strftime("%H:%M")
    df["decision_ts"] = df.apply(
        lambda row: _event_date_to_decision_ts(
            event_date=row["event_date"],
            timezone_name=row["timezone"],
            decision_time=decision_time,
        ),
        axis=1,
    )
    return df


def _prepare_candles(candles_df: pd.DataFrame) -> pd.DataFrame:
    df = candles_df.copy()
    df["candle_ts"] = pd.to_datetime(df["candle_ts"], utc=True, errors="coerce")
    if df["candle_ts"].isna().any():
        raise BacktestDatasetBuildError("kalshi_candles.parquet contains invalid candle_ts values.")
    return df.sort_values(["market_ticker", "candle_ts"], kind="stable").reset_index(drop=True)


def _prepare_weather(weather_df: pd.DataFrame) -> pd.DataFrame:
    df = weather_df.copy()
    df = df.rename(columns={"obs_date": "event_date", "station_id": "weather_station_id"})
    return df[["city_key", "event_date", "weather_station_id", "tmax_f"]].drop_duplicates(
        subset=["city_key", "event_date"],
        keep="last",
    )


def _prepare_normals(normals_df: pd.DataFrame) -> pd.DataFrame:
    df = normals_df.copy()
    df = df.rename(columns={"station_id": "normals_station_id"})
    return df[
        ["city_key", "month_day", "normals_station_id", "normal_tmax_f", "normals_period", "normals_source"]
    ].drop_duplicates(subset=["city_key", "month_day"], keep="last")


def _attach_decision_candles(markets_df: pd.DataFrame, candles_df: pd.DataFrame) -> pd.DataFrame:
    candle_index = {
        market_ticker: frame.reset_index(drop=True)
        for market_ticker, frame in candles_df.groupby("market_ticker", sort=False)
    }

    decision_candle_ts: list[str | None] = []
    decision_prices: list[float | None] = []
    candle_intervals: list[str | None] = []

    for row in markets_df.itertuples(index=False):
        market_candles = candle_index.get(row.market_ticker)
        if market_candles is None:
            decision_candle_ts.append(None)
            decision_prices.append(None)
            candle_intervals.append(None)
            continue

        eligible = market_candles.loc[market_candles["candle_ts"] <= row.decision_ts]
        if eligible.empty:
            decision_candle_ts.append(None)
            decision_prices.append(None)
            candle_intervals.append(None)
            continue

        latest = eligible.iloc[-1]
        decision_candle_ts.append(latest["candle_ts"].isoformat())
        decision_prices.append(latest["close"])
        candle_intervals.append(latest["interval"])

    merged = markets_df.copy()
    merged["decision_candle_ts"] = decision_candle_ts
    merged["decision_price"] = decision_prices
    merged["candle_interval"] = candle_intervals
    return merged


def _parse_decision_time_local(value: str) -> time:
    try:
        parsed = time.fromisoformat(value)
    except ValueError as exc:
        raise BacktestDatasetBuildError(
            f"Invalid decision_time_local {value!r}. Expected HH:MM or HH:MM:SS."
        ) from exc
    return parsed.replace(tzinfo=None)


def _event_date_to_decision_ts(event_date: str, timezone_name: str, decision_time: time) -> datetime:
    local_dt = datetime.combine(date.fromisoformat(event_date), decision_time, tzinfo=ZoneInfo(timezone_name))
    return local_dt.astimezone(timezone.utc)


def _resolve_market_bucket_row(row: pd.Series) -> bool | None:
    return resolve_bucket(
        actual_tmax_f=row.get("actual_tmax_f"),
        floor_strike=row.get("floor_strike"),
        cap_strike=row.get("cap_strike"),
        strike_type=row.get("strike_type"),
    )


def resolve_bucket(
    actual_tmax_f: Any,
    floor_strike: Any,
    cap_strike: Any,
    strike_type: Any,
) -> bool | None:
    """Resolve the market bucket against the realized temperature.

    Assumptions:
    - `between` buckets are inclusive of both `floor_strike` and `cap_strike`.
    - `above`/`greater` buckets are inclusive of `floor_strike`.
    - `below`/`less` buckets are inclusive of `cap_strike`.
    """
    if actual_tmax_f is None or pd.isna(actual_tmax_f):
        return None
    if not isinstance(strike_type, str):
        raise BacktestDatasetBuildError(f"Unsupported strike_type {strike_type!r}")

    normalized_type = strike_type.strip().lower()
    actual_value = float(actual_tmax_f)

    if normalized_type == "between":
        if floor_strike is None or cap_strike is None or pd.isna(floor_strike) or pd.isna(cap_strike):
            raise BacktestDatasetBuildError("between strike_type requires both floor_strike and cap_strike")
        return float(floor_strike) <= actual_value <= float(cap_strike)

    if normalized_type in {"above", "greater", "greater_than", "at_or_above"}:
        if floor_strike is None or pd.isna(floor_strike):
            raise BacktestDatasetBuildError(f"{strike_type!r} strike_type requires floor_strike")
        return actual_value >= float(floor_strike)

    if normalized_type in {"below", "less", "less_than", "at_or_below"}:
        if cap_strike is None or pd.isna(cap_strike):
            raise BacktestDatasetBuildError(f"{strike_type!r} strike_type requires cap_strike")
        return actual_value <= float(cap_strike)

    raise BacktestDatasetBuildError(f"Unsupported strike_type {strike_type!r}")
