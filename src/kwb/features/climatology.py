from __future__ import annotations

import pandas as pd


def same_day_of_year_window(df: pd.DataFrame, target_dayofyear: int, window: int = 7) -> pd.DataFrame:
    if "obs_date" not in df.columns:
        raise ValueError("Expected obs_date column")
    x = df.copy()
    x["doy"] = pd.to_datetime(x["obs_date"]).dt.dayofyear
    lower = target_dayofyear - window
    upper = target_dayofyear + window
    return x[(x["doy"] >= lower) & (x["doy"] <= upper)].drop(columns=["doy"])
