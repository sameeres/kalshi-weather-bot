from __future__ import annotations

from typing import Any

import requests

from kwb.settings import NWS_BASE_URL, USER_AGENT


class NWSClient:
    def __init__(self, base_url: str = NWS_BASE_URL, timeout: int = 20) -> None:
        self.base_url = base_url.rstrip("/")
        self.timeout = timeout
        self.session = requests.Session()
        self.session.headers.update({"User-Agent": USER_AGENT, "Accept": "application/geo+json"})

    def _get(self, path: str) -> dict[str, Any]:
        url = f"{self.base_url}{path}"
        response = self.session.get(url, timeout=self.timeout)
        response.raise_for_status()
        return response.json()

    def get_points(self, latitude: float, longitude: float) -> dict[str, Any]:
        return self._get(f"/points/{latitude},{longitude}")
