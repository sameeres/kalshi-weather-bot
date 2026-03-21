from __future__ import annotations

from datetime import datetime
from zoneinfo import ZoneInfo


def to_local(dt: datetime, tz_name: str) -> datetime:
    return dt.astimezone(ZoneInfo(tz_name))
