import pytest

from kwb.backtest.fees import ceil_to_cent, kalshi_standard_taker_fee, modeled_trade_fee


def test_ceil_to_cent_rounds_up() -> None:
    assert ceil_to_cent(0.0001) == 0.01
    assert ceil_to_cent(0.0100) == 0.01
    assert ceil_to_cent(0.0101) == 0.02


def test_kalshi_standard_taker_fee_matches_official_formula() -> None:
    assert kalshi_standard_taker_fee(fill_price=0.50, contracts=1) == 0.02
    assert kalshi_standard_taker_fee(fill_price=0.10, contracts=1) == 0.01


def test_modeled_trade_fee_supports_flat_and_kalshi_modes() -> None:
    assert modeled_trade_fee(fill_price=0.45, contracts=2, fee_model="flat_per_contract", fee_per_contract=0.03) == 0.06
    assert modeled_trade_fee(fill_price=0.50, contracts=1, fee_model="kalshi_standard_taker") == 0.02


def test_modeled_trade_fee_rejects_unknown_mode() -> None:
    with pytest.raises(ValueError, match="Unsupported fee_model"):
        modeled_trade_fee(fill_price=0.50, contracts=1, fee_model="unknown")
