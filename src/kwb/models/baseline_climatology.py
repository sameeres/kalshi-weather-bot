from __future__ import annotations

from pathlib import Path
from typing import Any

import pandas as pd

from kwb.marts.backtest_dataset import (
    DEFAULT_BACKTEST_DATASET_FILENAME,
    resolve_bucket,
)
from kwb.settings import MARTS_DIR as SETTINGS_MARTS_DIR
from kwb.settings import STAGING_DIR as SETTINGS_STAGING_DIR
from kwb.utils.logging import get_logger

logger = get_logger(__name__)

DEFAULT_BACKTEST_MART_PATH = SETTINGS_MARTS_DIR / DEFAULT_BACKTEST_DATASET_FILENAME
DEFAULT_HISTORY_PATH = SETTINGS_STAGING_DIR / "weather_daily.parquet"
DEFAULT_SCORED_FILENAME = "backtest_scored_climatology.parquet"
DEFAULT_MODEL_NAME = "baseline_climatology_v1"

REQUIRED_BACKTEST_COLUMNS = {
    "city_key",
    "market_ticker",
    "event_date",
    "month_day",
    "decision_ts",
    "decision_price",
    "actual_tmax_f",
    "normal_tmax_f",
    "tmax_anomaly_f",
    "resolved_yes",
    "floor_strike",
    "cap_strike",
    "strike_type",
}
REQUIRED_HISTORY_COLUMNS = {"city_key", "obs_date", "tmax_f"}


class ClimatologyModelError(ValueError):
    """Raised when the climatology baseline cannot be scored safely."""


def score_climatology_baseline(
    backtest_dataset_path: Path | None = None,
    history_path: Path | None = None,
    output_dir: Path | None = None,
    day_window: int = 0,
    min_lookback_samples: int = 1,
) -> tuple[Path, dict[str, int | float | str]]:
    """Score the backtest mart with a climatology-only YES probability estimate."""
    backtest_dataset_path = backtest_dataset_path or DEFAULT_BACKTEST_MART_PATH
    history_path = history_path or DEFAULT_HISTORY_PATH
    output_dir = output_dir or SETTINGS_MARTS_DIR
    output_dir.mkdir(parents=True, exist_ok=True)

    if day_window < 0:
        raise ClimatologyModelError(f"day_window must be non-negative, got {day_window}")
    if min_lookback_samples < 1:
        raise ClimatologyModelError(f"min_lookback_samples must be at least 1, got {min_lookback_samples}")

    _ensure_inputs_exist([backtest_dataset_path, history_path])
    backtest_df = _load_frame(backtest_dataset_path, REQUIRED_BACKTEST_COLUMNS)
    history_df = _load_frame(history_path, REQUIRED_HISTORY_COLUMNS)

    scored_df, summary = _score_frame(
        backtest_df=backtest_df,
        history_df=history_df,
        day_window=day_window,
        min_lookback_samples=min_lookback_samples,
    )

    outpath = output_dir / DEFAULT_SCORED_FILENAME
    scored_df.to_parquet(outpath, index=False)
    summary["rows_scored"] = len(scored_df)
    logger.info(
        "Saved %s climatology-scored rows to %s with model=%s",
        summary["rows_scored"],
        outpath,
        DEFAULT_MODEL_NAME,
    )
    return outpath, summary


def evaluate_scored_climatology(df: pd.DataFrame) -> dict[str, int | float]:
    if df.empty:
        return {
            "rows_scored": 0,
            "average_lookback_sample_size": 0.0,
            "brier_score": 0.0,
            "average_edge_yes": 0.0,
        }

    required = {"model_prob_yes", "resolved_yes", "lookback_sample_size", "edge_yes"}
    missing = sorted(required - set(df.columns))
    if missing:
        raise ClimatologyModelError(f"Scored dataset is missing evaluation columns: {', '.join(missing)}")

    resolved = df["resolved_yes"].astype(float)
    probs = df["model_prob_yes"].astype(float)
    brier_score = float(((probs - resolved) ** 2).mean())
    return {
        "rows_scored": int(len(df)),
        "average_lookback_sample_size": round(float(df["lookback_sample_size"].mean()), 3),
        "brier_score": round(brier_score, 6),
        "average_edge_yes": round(float(df["edge_yes"].mean()), 6),
    }


def _score_frame(
    backtest_df: pd.DataFrame,
    history_df: pd.DataFrame,
    day_window: int,
    min_lookback_samples: int,
) -> tuple[pd.DataFrame, dict[str, int | float | str]]:
    backtest_rows = _prepare_backtest_frame(backtest_df)
    history_rows = _prepare_history_frame(history_df)

    grouped_history = {
        city_key: frame.reset_index(drop=True)
        for city_key, frame in history_rows.groupby("city_key", sort=False)
    }

    scored_rows: list[dict[str, Any]] = []
    insufficient_rows = 0

    for row in backtest_rows.to_dict("records"):
        city_history = grouped_history.get(row["city_key"])
        if city_history is None:
            insufficient_rows += 1
            continue

        lookback = select_climatology_lookback(
            history_df=city_history,
            city_key=row["city_key"],
            event_date=row["event_date"],
            month_day=row["month_day"],
            day_window=day_window,
        )
        lookback_sample_size = len(lookback)
        if lookback_sample_size < min_lookback_samples:
            insufficient_rows += 1
            continue

        model_prob_yes = estimate_climatology_prob_yes(
            history_df=lookback,
            floor_strike=row["floor_strike"],
            cap_strike=row["cap_strike"],
            strike_type=row["strike_type"],
        )
        model_prob_no = round(1.0 - model_prob_yes, 6)
        fair_yes = model_prob_yes
        fair_no = model_prob_no
        decision_price = float(row["decision_price"])
        edge_yes = round(model_prob_yes - decision_price / 100.0, 6)

        scored_rows.append(
            {
                "city_key": row["city_key"],
                "market_ticker": row["market_ticker"],
                "event_date": row["event_date"],
                "decision_ts": row["decision_ts"],
                "decision_price": decision_price,
                "actual_tmax_f": row["actual_tmax_f"],
                "normal_tmax_f": row["normal_tmax_f"],
                "tmax_anomaly_f": row["tmax_anomaly_f"],
                "resolved_yes": row["resolved_yes"],
                "model_prob_yes": model_prob_yes,
                "model_prob_no": model_prob_no,
                "fair_yes": fair_yes,
                "fair_no": fair_no,
                "edge_yes": edge_yes,
                "lookback_sample_size": lookback_sample_size,
                "model_name": DEFAULT_MODEL_NAME,
            }
        )

    scored_df = _build_scored_frame(scored_rows)
    summary = evaluate_scored_climatology(scored_df)
    summary["unscored_insufficient_history_rows"] = insufficient_rows
    summary["day_window"] = day_window
    summary["min_lookback_samples"] = min_lookback_samples
    return scored_df, summary


def select_climatology_lookback(
    history_df: pd.DataFrame,
    city_key: str,
    event_date: str,
    month_day: str,
    day_window: int,
) -> pd.DataFrame:
    df = history_df.loc[history_df["city_key"] == city_key].copy()
    event_dt = pd.Timestamp(event_date)
    df = df.loc[df["obs_date"] < event_dt].copy()
    if df.empty:
        return df.reset_index(drop=True)

    target_day_of_year = _month_day_to_day_of_year(month_day)
    df["seasonal_distance"] = df["month_day"].map(lambda value: _day_of_year_distance(target_day_of_year, _month_day_to_day_of_year(value)))
    df = df.loc[df["seasonal_distance"] <= day_window].copy()
    return df.sort_values(["obs_date", "city_key"], kind="stable").reset_index(drop=True)


def estimate_climatology_prob_yes(
    history_df: pd.DataFrame,
    floor_strike: Any,
    cap_strike: Any,
    strike_type: Any,
) -> float:
    if history_df.empty:
        raise ClimatologyModelError("Cannot estimate climatology probability from an empty lookback sample.")

    outcomes = history_df["tmax_f"].map(
        lambda value: resolve_bucket(
            actual_tmax_f=value,
            floor_strike=floor_strike,
            cap_strike=cap_strike,
            strike_type=strike_type,
        )
    )
    if outcomes.isna().any():
        raise ClimatologyModelError("Lookback sample produced unresolved bucket outcomes.")
    return round(float(outcomes.astype(float).mean()), 6)


def _prepare_backtest_frame(df: pd.DataFrame) -> pd.DataFrame:
    prepared = df.copy()
    prepared["event_date"] = pd.to_datetime(prepared["event_date"], errors="coerce")
    if prepared["event_date"].isna().any():
        raise ClimatologyModelError("Backtest dataset contains invalid event_date values.")
    prepared["month_day"] = prepared["month_day"].astype(str)
    if prepared["decision_price"].isna().any():
        raise ClimatologyModelError("Backtest dataset contains missing decision_price values.")
    if prepared["resolved_yes"].isna().any():
        raise ClimatologyModelError("Backtest dataset contains missing resolved_yes values.")
    return prepared


def _prepare_history_frame(df: pd.DataFrame) -> pd.DataFrame:
    prepared = df.copy()
    prepared["obs_date"] = pd.to_datetime(prepared["obs_date"], errors="coerce")
    if prepared["obs_date"].isna().any():
        raise ClimatologyModelError("Weather history contains invalid obs_date values.")
    prepared = prepared.loc[prepared["tmax_f"].notna()].copy()
    prepared["month_day"] = prepared["obs_date"].dt.strftime("%m-%d")
    return prepared


def _build_scored_frame(rows: list[dict[str, Any]]) -> pd.DataFrame:
    columns = [
        "city_key",
        "market_ticker",
        "event_date",
        "decision_ts",
        "decision_price",
        "actual_tmax_f",
        "normal_tmax_f",
        "tmax_anomaly_f",
        "resolved_yes",
        "model_prob_yes",
        "model_prob_no",
        "fair_yes",
        "fair_no",
        "edge_yes",
        "lookback_sample_size",
        "model_name",
    ]
    df = pd.DataFrame(rows, columns=columns)
    if df.empty:
        return df
    df = df.sort_values(["city_key", "event_date", "market_ticker"], kind="stable").reset_index(drop=True)
    return df


def _ensure_inputs_exist(paths: list[Path]) -> None:
    missing = [str(path) for path in paths if not path.exists()]
    if missing:
        raise ClimatologyModelError(
            "Required local input parquet files are missing:\n" + "\n".join(missing)
        )


def _load_frame(path: Path, required_columns: set[str]) -> pd.DataFrame:
    frame = pd.read_parquet(path)
    missing = sorted(required_columns - set(frame.columns))
    if missing:
        raise ClimatologyModelError(f"Required columns are missing from {path}: {', '.join(missing)}")
    return frame.copy()


def _month_day_to_day_of_year(month_day: str) -> int:
    return pd.Timestamp(f"2001-{month_day}").dayofyear


def _day_of_year_distance(day_a: int, day_b: int) -> int:
    raw_distance = abs(day_a - day_b)
    return min(raw_distance, 365 - raw_distance)
