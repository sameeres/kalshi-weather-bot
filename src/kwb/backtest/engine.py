from __future__ import annotations

from dataclasses import dataclass


@dataclass
class BacktestConfig:
    min_edge: float = 0.03
    fee_buffer: float = 0.01
    slippage_buffer: float = 0.01
