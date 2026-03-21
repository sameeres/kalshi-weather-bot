from __future__ import annotations


def has_yes_edge(prob_yes: float, yes_ask: float, fee_buffer: float, slippage_buffer: float, min_edge: float) -> bool:
    edge = prob_yes - yes_ask - fee_buffer - slippage_buffer
    return edge >= min_edge
