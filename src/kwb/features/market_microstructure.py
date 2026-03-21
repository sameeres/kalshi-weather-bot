from __future__ import annotations


def spread(yes_bid: float | None, yes_ask: float | None) -> float | None:
    if yes_bid is None or yes_ask is None:
        return None
    return yes_ask - yes_bid
