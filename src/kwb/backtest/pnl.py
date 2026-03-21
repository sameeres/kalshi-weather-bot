from __future__ import annotations


def contract_pnl(fill_price: float, resolved_value: float, contracts: int = 1, fee_per_contract: float = 0.0) -> float:
    gross = (resolved_value - fill_price) * contracts
    fees = fee_per_contract * contracts
    return gross - fees
