from kwb.backtest.fills import conservative_fill


def test_conservative_fill_yes_uses_ask():
    assert conservative_fill(ask=0.54, bid=0.51, side="yes") == 0.54
