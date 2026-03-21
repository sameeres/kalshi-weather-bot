from __future__ import annotations

from dataclasses import dataclass
from typing import Iterable

import numpy as np


@dataclass
class Bucket:
    label: str
    low: float
    high: float


def bucket_probability(samples: np.ndarray, bucket: Bucket) -> float:
    return float(((samples >= bucket.low) & (samples <= bucket.high)).mean())


def bucket_probabilities(samples: np.ndarray, buckets: Iterable[Bucket]) -> list[float]:
    return [bucket_probability(samples, b) for b in buckets]
