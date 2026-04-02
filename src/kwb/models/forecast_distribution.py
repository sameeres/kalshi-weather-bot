from __future__ import annotations

import math
from pathlib import Path
from typing import Any

import numpy as np
import pandas as pd

from kwb.models.baseline_climatology import DEFAULT_HISTORY_PATH, select_climatology_lookback
from kwb.marts.backtest_dataset import DEFAULT_BACKTEST_DATASET_FILENAME
from kwb.settings import MARTS_DIR as SETTINGS_MARTS_DIR
from kwb.settings import STAGING_DIR as SETTINGS_STAGING_DIR
from kwb.utils.logging import get_logger

logger = get_logger(__name__)

DEFAULT_BACKTEST_MART_PATH = SETTINGS_MARTS_DIR / DEFAULT_BACKTEST_DATASET_FILENAME
DEFAULT_FORECAST_SNAPSHOTS_PATH = SETTINGS_STAGING_DIR / "nws_forecast_hourly_snapshots.parquet"
DEFAULT_SCORED_FILENAME = "backtest_scored_forecast_distribution.parquet"
DEFAULT_MODEL_NAME = "forecast_distribution_v1"

REQUIRED_BACKTEST_COLUMNS = {
    "city_key",
    "market_ticker",
    "event_date",
    "month_day",
    "decision_ts",
    "decision_price",
    "yes_bid",
    "yes_ask",
    "no_bid",
    "no_ask",
    "actual_tmax_f",
    "normal_tmax_f",
    "tmax_anomaly_f",
    "resolved_yes",
    "floor_strike",
    "cap_strike",
    "strike_type",
}
REQUIRED_HISTORY_COLUMNS = {"city_key", "obs_date", "tmax_f"}
REQUIRED_FORECAST_COLUMNS = {
    "snapshot_ts",
    "city_key",
    "period_start_ts",
    "period_end_ts",
    "period_date_local",
    "temperature_f",
}


class ForecastDistributionModelError(ValueError):
    """Raised when the forecast distribution scorer cannot complete safely."""


def score_forecast_distribution(
    backtest_dataset_path: Path | None = None,
    history_path: Path | None = None,
    forecast_snapshots_path: Path | None = None,
    output_dir: Path | None = None,
    day_window: int = 1,
    min_lookback_samples: int = 30,
    max_snapshot_age_hours: float = 18.0,
    base_sigma_f: float = 3.0,
    sigma_per_range_f: float = 0.15,
) -> tuple[Path, dict[str, int | float | str]]:
    backtest_dataset_path = backtest_dataset_path or DEFAULT_BACKTEST_MART_PATH
    history_path = history_path or DEFAULT_HISTORY_PATH
    forecast_snapshots_path = forecast_snapshots_path or DEFAULT_FORECAST_SNAPSHOTS_PATH
    output_dir = output_dir or SETTINGS_MARTS_DIR
    output_dir.mkdir(parents=True, exist_ok=True)

    _ensure_inputs_exist([backtest_dataset_path, history_path, forecast_snapshots_path])
    backtest_df = _load_frame(backtest_dataset_path, REQUIRED_BACKTEST_COLUMNS)
    history_df = _load_frame(history_path, REQUIRED_HISTORY_COLUMNS)
    forecast_df = _load_frame(forecast_snapshots_path, REQUIRED_FORECAST_COLUMNS)

    scored_df, summary = _score_frame(
        backtest_df=backtest_df,
        history_df=history_df,
        forecast_df=forecast_df,
        day_window=day_window,
        min_lookback_samples=min_lookback_samples,
        max_snapshot_age_hours=max_snapshot_age_hours,
        base_sigma_f=base_sigma_f,
        sigma_per_range_f=sigma_per_range_f,
    )

    outpath = output_dir / DEFAULT_SCORED_FILENAME
    scored_df.to_parquet(outpath, index=False)
    summary["rows_scored"] = len(scored_df)
    logger.info(
        "Saved %s forecast-distribution scored rows to %s with model=%s",
        summary["rows_scored"],
        outpath,
        DEFAULT_MODEL_NAME,
    )
    return outpath, summary


def evaluate_scored_forecast_distribution(df: pd.DataFrame) -> dict[str, int | float]:
    if df.empty:
        return {
            "rows_scored": 0,
            "average_lookback_sample_size": 0.0,
            "average_snapshot_age_hours": 0.0,
            "average_distribution_sigma_f": 0.0,
            "brier_score": 0.0,
            "average_edge_yes": 0.0,
        }

    resolved = df["resolved_yes"].astype(float)
    probs = df["model_prob_yes"].astype(float)
    brier_score = float(((probs - resolved) ** 2).mean())
    return {
        "rows_scored": int(len(df)),
        "average_lookback_sample_size": round(float(df["lookback_sample_size"].mean()), 3),
        "average_snapshot_age_hours": round(float(df["snapshot_age_hours"].mean()), 3),
        "average_distribution_sigma_f": round(float(df["distribution_sigma_f"].mean()), 3),
        "brier_score": round(brier_score, 6),
        "average_edge_yes": round(float(df["edge_yes"].mean()), 6),
    }


def estimate_forecast_prob_yes(
    history_samples: np.ndarray,
    forecast_max_temp_f: float,
    normal_tmax_f: float,
    floor_strike: Any,
    cap_strike: Any,
    strike_type: Any,
    distribution_sigma_f: float,
) -> float:
    if history_samples.size == 0:
        raise ForecastDistributionModelError("Cannot estimate forecast probability from an empty lookback sample.")
    if distribution_sigma_f <= 0:
        raise ForecastDistributionModelError("distribution_sigma_f must be positive.")

    forecast_anomaly_f = float(forecast_max_temp_f) - float(normal_tmax_f)
    shifted_means = history_samples.astype(float) + forecast_anomaly_f
    probabilities = _bucket_probability_from_gaussian_mixture(
        means=shifted_means,
        sigma=distribution_sigma_f,
        floor_strike=floor_strike,
        cap_strike=cap_strike,
        strike_type=strike_type,
    )
    return round(float(probabilities.mean()), 6)


def _score_frame(
    backtest_df: pd.DataFrame,
    history_df: pd.DataFrame,
    forecast_df: pd.DataFrame,
    day_window: int,
    min_lookback_samples: int,
    max_snapshot_age_hours: float,
    base_sigma_f: float,
    sigma_per_range_f: float,
) -> tuple[pd.DataFrame, dict[str, int | float | str]]:
    backtest_rows = _prepare_backtest_frame(backtest_df)
    history_rows = _prepare_history_frame(history_df)
    forecast_rows = _prepare_forecast_frame(forecast_df)

    grouped_history = {
        city_key: frame.reset_index(drop=True)
        for city_key, frame in history_rows.groupby("city_key", sort=False)
    }
    grouped_forecast = {
        city_key: frame.reset_index(drop=True)
        for city_key, frame in forecast_rows.groupby("city_key", sort=False)
    }

    scored_rows: list[dict[str, Any]] = []
    missing_forecast_rows = 0
    insufficient_history_rows = 0

    for row in backtest_rows.to_dict("records"):
        city_history = grouped_history.get(row["city_key"])
        city_forecasts = grouped_forecast.get(row["city_key"])
        if city_history is None or city_forecasts is None:
            missing_forecast_rows += 1
            continue

        forecast_features = _select_forecast_features_for_row(
            forecast_df=city_forecasts,
            decision_ts=row["decision_ts"],
            event_date=row["event_date"],
            max_snapshot_age_hours=max_snapshot_age_hours,
            base_sigma_f=base_sigma_f,
            sigma_per_range_f=sigma_per_range_f,
        )
        if forecast_features is None:
            missing_forecast_rows += 1
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
            insufficient_history_rows += 1
            continue

        model_prob_yes = estimate_forecast_prob_yes(
            history_samples=lookback["tmax_f"].to_numpy(dtype=float),
            forecast_max_temp_f=float(forecast_features["forecast_max_temp_f"]),
            normal_tmax_f=float(row["normal_tmax_f"]),
            floor_strike=row["floor_strike"],
            cap_strike=row["cap_strike"],
            strike_type=row["strike_type"],
            distribution_sigma_f=float(forecast_features["distribution_sigma_f"]),
        )
        model_prob_no = round(1.0 - model_prob_yes, 6)
        decision_price = float(row["decision_price"])
        edge_yes = round(model_prob_yes - decision_price / 100.0, 6)

        scored_rows.append(
            {
                "city_key": row["city_key"],
                "market_ticker": row["market_ticker"],
                "event_date": row["event_date"],
                "decision_ts": row["decision_ts"],
                "decision_price": decision_price,
                "yes_bid": row["yes_bid"],
                "yes_ask": row["yes_ask"],
                "no_bid": row["no_bid"],
                "no_ask": row["no_ask"],
                "actual_tmax_f": row["actual_tmax_f"],
                "normal_tmax_f": row["normal_tmax_f"],
                "tmax_anomaly_f": row["tmax_anomaly_f"],
                "resolved_yes": row["resolved_yes"],
                "model_prob_yes": model_prob_yes,
                "model_prob_no": model_prob_no,
                "fair_yes": model_prob_yes,
                "fair_no": model_prob_no,
                "edge_yes": edge_yes,
                "lookback_sample_size": lookback_sample_size,
                "forecast_snapshot_ts": forecast_features["forecast_snapshot_ts"],
                "forecast_period_count": forecast_features["forecast_period_count"],
                "forecast_max_temp_f": forecast_features["forecast_max_temp_f"],
                "forecast_min_temp_f": forecast_features["forecast_min_temp_f"],
                "forecast_intraday_range_f": forecast_features["forecast_intraday_range_f"],
                "forecast_anomaly_f": forecast_features["forecast_anomaly_f"],
                "distribution_sigma_f": forecast_features["distribution_sigma_f"],
                "snapshot_age_hours": forecast_features["snapshot_age_hours"],
                "model_name": DEFAULT_MODEL_NAME,
            }
        )

    scored_df = _build_scored_frame(scored_rows)
    summary = evaluate_scored_forecast_distribution(scored_df)
    summary["unscored_missing_forecast_rows"] = missing_forecast_rows
    summary["unscored_insufficient_history_rows"] = insufficient_history_rows
    summary["day_window"] = day_window
    summary["min_lookback_samples"] = min_lookback_samples
    summary["max_snapshot_age_hours"] = max_snapshot_age_hours
    return scored_df, summary


def _select_forecast_features_for_row(
    forecast_df: pd.DataFrame,
    decision_ts: pd.Timestamp,
    event_date: pd.Timestamp,
    max_snapshot_age_hours: float,
    base_sigma_f: float,
    sigma_per_range_f: float,
) -> dict[str, float | int | str] | None:
    candidate_rows = forecast_df.loc[
        (forecast_df["snapshot_ts"] <= decision_ts)
        & (forecast_df["snapshot_ts"] >= decision_ts - pd.Timedelta(hours=max_snapshot_age_hours))
        & (forecast_df["period_date_local"] == event_date.strftime("%Y-%m-%d"))
        & (forecast_df["period_end_ts"] > forecast_df["snapshot_ts"])
    ].copy()
    if candidate_rows.empty:
        return None

    latest_snapshot_ts = candidate_rows["snapshot_ts"].max()
    snapshot_rows = candidate_rows.loc[candidate_rows["snapshot_ts"] == latest_snapshot_ts].copy()
    if snapshot_rows.empty:
        return None

    forecast_max_temp_f = float(snapshot_rows["temperature_f"].max())
    forecast_min_temp_f = float(snapshot_rows["temperature_f"].min())
    forecast_intraday_range_f = round(forecast_max_temp_f - forecast_min_temp_f, 6)
    distribution_sigma_f = round(max(base_sigma_f, base_sigma_f + (forecast_intraday_range_f * sigma_per_range_f)), 6)
    snapshot_age_hours = round((decision_ts - latest_snapshot_ts).total_seconds() / 3600.0, 6)

    return {
        "forecast_snapshot_ts": latest_snapshot_ts.isoformat(),
        "forecast_period_count": int(len(snapshot_rows)),
        "forecast_max_temp_f": round(forecast_max_temp_f, 6),
        "forecast_min_temp_f": round(forecast_min_temp_f, 6),
        "forecast_intraday_range_f": forecast_intraday_range_f,
        "forecast_anomaly_f": round(forecast_max_temp_f, 6),
        "distribution_sigma_f": distribution_sigma_f,
        "snapshot_age_hours": snapshot_age_hours,
    }


def _bucket_probability_from_gaussian_mixture(
    means: np.ndarray,
    sigma: float,
    floor_strike: Any,
    cap_strike: Any,
    strike_type: Any,
) -> np.ndarray:
    normalized_type = str(strike_type or "").strip().lower()
    if normalized_type == "between":
        if floor_strike is None or cap_strike is None:
            raise ForecastDistributionModelError("between strike_type requires both floor_strike and cap_strike")
        lower = _normal_cdf((float(floor_strike) - means) / sigma)
        upper = _normal_cdf((float(cap_strike) - means) / sigma)
        return upper - lower
    if normalized_type in {"above", "greater", "greater_than", "at_or_above"}:
        if floor_strike is None:
            raise ForecastDistributionModelError(f"{strike_type!r} strike_type requires floor_strike")
        return 1.0 - _normal_cdf((float(floor_strike) - means) / sigma)
    if normalized_type in {"below", "less", "less_than", "at_or_below"}:
        if cap_strike is None:
            raise ForecastDistributionModelError(f"{strike_type!r} strike_type requires cap_strike")
        return _normal_cdf((float(cap_strike) - means) / sigma)
    raise ForecastDistributionModelError(f"Unsupported strike_type {strike_type!r}")


def _normal_cdf(z_scores: np.ndarray) -> np.ndarray:
    return 0.5 * (1.0 + np.vectorize(math.erf)(z_scores / math.sqrt(2.0)))


def _prepare_backtest_frame(df: pd.DataFrame) -> pd.DataFrame:
    prepared = df.copy()
    prepared["event_date"] = pd.to_datetime(prepared["event_date"], errors="coerce")
    prepared["decision_ts"] = pd.to_datetime(prepared["decision_ts"], utc=True, errors="coerce")
    if prepared["event_date"].isna().any():
        raise ForecastDistributionModelError("Backtest dataset contains invalid event_date values.")
    if prepared["decision_ts"].isna().any():
        raise ForecastDistributionModelError("Backtest dataset contains invalid decision_ts values.")
    if prepared["decision_price"].isna().any():
        raise ForecastDistributionModelError("Backtest dataset contains missing decision_price values.")
    if prepared["resolved_yes"].isna().any():
        raise ForecastDistributionModelError("Backtest dataset contains missing resolved_yes values.")
    return prepared


def _prepare_history_frame(df: pd.DataFrame) -> pd.DataFrame:
    prepared = df.copy()
    prepared["obs_date"] = pd.to_datetime(prepared["obs_date"], errors="coerce")
    if prepared["obs_date"].isna().any():
        raise ForecastDistributionModelError("Weather history contains invalid obs_date values.")
    prepared = prepared.loc[prepared["tmax_f"].notna()].copy()
    prepared["month_day"] = prepared["obs_date"].dt.strftime("%m-%d")
    return prepared


def _prepare_forecast_frame(df: pd.DataFrame) -> pd.DataFrame:
    prepared = df.copy()
    prepared["snapshot_ts"] = pd.to_datetime(prepared["snapshot_ts"], utc=True, errors="coerce")
    prepared["period_start_ts"] = pd.to_datetime(prepared["period_start_ts"], utc=True, errors="coerce")
    prepared["period_end_ts"] = pd.to_datetime(prepared["period_end_ts"], utc=True, errors="coerce")
    invalid = prepared["snapshot_ts"].isna() | prepared["period_start_ts"].isna() | prepared["period_end_ts"].isna()
    if invalid.any():
        raise ForecastDistributionModelError("Forecast snapshots contain invalid timestamp values.")
    prepared = prepared.loc[prepared["temperature_f"].notna()].copy()
    return prepared


def _build_scored_frame(rows: list[dict[str, Any]]) -> pd.DataFrame:
    columns = [
        "city_key",
        "market_ticker",
        "event_date",
        "decision_ts",
        "decision_price",
        "yes_bid",
        "yes_ask",
        "no_bid",
        "no_ask",
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
        "forecast_snapshot_ts",
        "forecast_period_count",
        "forecast_max_temp_f",
        "forecast_min_temp_f",
        "forecast_intraday_range_f",
        "forecast_anomaly_f",
        "distribution_sigma_f",
        "snapshot_age_hours",
        "model_name",
    ]
    df = pd.DataFrame(rows, columns=columns)
    if df.empty:
        return df
    return df.sort_values(["city_key", "event_date", "market_ticker"], kind="stable").reset_index(drop=True)


def _ensure_inputs_exist(paths: list[Path]) -> None:
    missing = [str(path) for path in paths if not path.exists()]
    if missing:
        raise ForecastDistributionModelError(
            "Required local input parquet files are missing:\n" + "\n".join(missing)
        )


def _load_frame(path: Path, required_columns: set[str]) -> pd.DataFrame:
    frame = pd.read_parquet(path)
    missing = sorted(required_columns - set(frame.columns))
    if missing:
        raise ForecastDistributionModelError(f"Required columns are missing from {path}: {', '.join(missing)}")
    return frame.copy()
