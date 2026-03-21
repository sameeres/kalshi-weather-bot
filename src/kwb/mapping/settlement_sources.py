from __future__ import annotations

import json
from typing import Any


def extract_settlement_sources(event_payload: dict[str, Any]) -> list[dict[str, Any]]:
    candidates = [
        event_payload,
        event_payload.get("event"),
        event_payload.get("metadata"),
        event_payload.get("event_metadata"),
    ]

    for candidate in candidates:
        if not isinstance(candidate, dict):
            continue

        sources = candidate.get("settlement_sources")
        if isinstance(sources, list):
            return [source for source in sources if isinstance(source, dict)]

        source = candidate.get("settlement_source")
        if isinstance(source, dict):
            return [source]

        encoded_sources = candidate.get("settlement_sources_json")
        if isinstance(encoded_sources, str):
            try:
                decoded = json.loads(encoded_sources)
            except json.JSONDecodeError:
                continue
            if isinstance(decoded, list):
                return [source for source in decoded if isinstance(source, dict)]

    return []
