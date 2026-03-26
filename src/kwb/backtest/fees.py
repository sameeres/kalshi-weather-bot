from __future__ import annotations

import math


VALID_FEE_MODELS = {"flat_per_contract", "kalshi_standard_taker"}


def ceil_to_cent(amount_dollars: float) -> float:
    if amount_dollars <= 0:
        return 0.0
    return math.ceil(amount_dollars * 100.0) / 100.0


def kalshi_standard_taker_fee(fill_price: float, contracts: int = 1) -> float:
    """Return Kalshi's standard immediate-match fee in dollars.

    Formula supplied by user guidance for standard markets:
    fee = ceil_to_cent(0.07 * C * P * (1 - P))
    """
    if contracts < 1:
        raise ValueError(f"contracts must be at least 1, got {contracts}")
    if fill_price < 0 or fill_price > 1:
        raise ValueError(f"fill_price must be between 0 and 1 dollars, got {fill_price}")
    raw_fee = 0.07 * float(contracts) * float(fill_price) * (1.0 - float(fill_price))
    return ceil_to_cent(raw_fee)


def modeled_trade_fee(
    fill_price: float,
    contracts: int = 1,
    fee_model: str = "flat_per_contract",
    fee_per_contract: float = 0.0,
) -> float:
    if fee_model not in VALID_FEE_MODELS:
        raise ValueError(f"Unsupported fee_model {fee_model!r}. Expected one of {sorted(VALID_FEE_MODELS)}.")
    if contracts < 1:
        raise ValueError(f"contracts must be at least 1, got {contracts}")
    if fee_per_contract < 0:
        raise ValueError(f"fee_per_contract must be non-negative, got {fee_per_contract}")

    if fee_model == "flat_per_contract":
        return round(float(fee_per_contract) * int(contracts), 6)
    return round(kalshi_standard_taker_fee(fill_price=float(fill_price), contracts=int(contracts)), 6)
