from __future__ import annotations

import numpy as np


def shift_samples(samples: np.ndarray, anomaly_f: float) -> np.ndarray:
    return samples + anomaly_f
