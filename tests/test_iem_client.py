from __future__ import annotations

from datetime import date, datetime, timezone

from kwb.clients.iem import _parse_entered, _split_afos_response


def test_split_afos_response_single_product() -> None:
    text = "000\nFPUS51 KOKX 141000\n\nZFPOKX\n\n.TODAY...\nHigh near 58."
    parts = _split_afos_response(text)
    assert len(parts) == 1
    assert "ZFPOKX" in parts[0]


def test_split_afos_response_multiple_products_triple_newline() -> None:
    text = "product one\n\n\nproduct two\n\n\nproduct three"
    parts = _split_afos_response(text)
    assert len(parts) == 3
    assert parts[0] == "product one"
    assert parts[2] == "product three"


def test_split_afos_response_equals_separator() -> None:
    text = "product A\n===\nproduct B"
    parts = _split_afos_response(text)
    assert len(parts) == 2


def test_split_afos_response_empty_text() -> None:
    assert _split_afos_response("") == []
    assert _split_afos_response("   \n\n\n  ") == []


def test_parse_entered_utc_iso() -> None:
    ts = _parse_entered("2025-11-15T10:12:00Z")
    assert ts is not None
    assert ts.tzinfo == timezone.utc
    assert ts.hour == 10
    assert ts.minute == 12


def test_parse_entered_invalid() -> None:
    assert _parse_entered("not-a-date") is None
    assert _parse_entered("") is None
