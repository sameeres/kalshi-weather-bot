from __future__ import annotations

import os
from typing import Any

import requests


class NCEIClient:
    def __init__(self, timeout: int = 20) -> None:
        self.base_url = "https://www.ncei.noaa.gov/cdo-web/api/v2"
        self.timeout = timeout
        self.session = requests.Session()
        token = os.getenv("NCEI_API_TOKEN")
        if token:
            self.session.headers.update({"token": token})

    def _get(self, path: str, params: dict[str, Any] | None = None) -> dict[str, Any]:
        url = f"{self.base_url}{path}"
        response = self.session.get(url, params=params, timeout=self.timeout)
        response.raise_for_status()
        return response.json()

    def get_daily_station_observations(
        self,
        station_id: str,
        start_date: str,
        end_date: str,
        datasetid: str = "GHCND",
        units: str = "metric",
        limit: int = 1000,
        offset: int = 1,
    ) -> dict[str, Any]:
        params = {
            "datasetid": datasetid,
            "stationid": station_id,
            "startdate": start_date,
            "enddate": end_date,
            "units": units,
            "limit": limit,
            "offset": offset,
        }
        return self._get("/data", params=params)
