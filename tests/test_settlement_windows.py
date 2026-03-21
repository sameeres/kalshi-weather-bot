from datetime import datetime, timezone

from kwb.utils.dates import to_local


def test_to_local_changes_timezone():
    dt = datetime(2026, 3, 21, 16, 0, tzinfo=timezone.utc)
    local = to_local(dt, "America/New_York")
    assert local.tzinfo is not None
