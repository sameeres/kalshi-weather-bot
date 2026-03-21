from __future__ import annotations

import numpy as np
import pandas as pd


def sample_from_history(df: pd.DataFrame, column: str = "tmax_f", n: int = 5000, seed: int = 42) -> np.ndarray:
    values = df[column].dropna().to_numpy()
    if len(values) == 0:
        raise ValueError("No historical values available to sample from.")
    rng = np.random.default_rng(seed)
    return rng.choice(values, size=n, replace=True)
