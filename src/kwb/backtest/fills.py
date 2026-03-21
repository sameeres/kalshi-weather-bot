from __future__ import annotations


def conservative_fill(ask: float | None, bid: float | None, side: str) -> float | None:
    side = side.lower()
    if side == "yes":
        return ask
    if side == "no":
        return ask
    if side == "sell_yes":
        return bid
    if side == "sell_no":
        return bid
    raise ValueError(f"Unsupported side: {side}")
