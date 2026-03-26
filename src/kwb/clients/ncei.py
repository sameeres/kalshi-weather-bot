from __future__ import annotations

import os
from typing import Any

import requests


class NCEIClient:
    def __init__(self, timeout: int = 20) -> None:
        self.base_url = "https://www.ncei.noaa.gov/cdo-web/api/v2"
        self.access_services_url = "https://www.ncei.noaa.gov/access/services/data/v1"
        self.timeout = timeout
        self.session = requests.Session()
        self.token = os.getenv("NCEI_API_TOKEN")
        if self.token:
            self.session.headers.update({"token": self.token})

    def _get(self, path: str, params: dict[str, Any] | None = None) -> dict[str, Any]:
        url = f"{self.base_url}{path}"
        response = self.session.get(url, params=params, timeout=self.timeout)
        response.raise_for_status()
        return response.json()

    def _get_access_service(self, params: dict[str, Any]) -> list[dict[str, Any]]:
        response = self.session.get(self.access_services_url, params=params, timeout=self.timeout)
        response.raise_for_status()
        payload = response.json()
        if not isinstance(payload, list):
            raise TypeError(f"Expected list response from NOAA Access Services, got {type(payload)}")
        return payload

    def get_daily_station_observations(
        self,
        station_id: str,
        start_date: str,
        end_date: str,
        datasetid: str = "GHCND",
        datatypeids: list[str] | None = None,
        units: str = "metric",
        limit: int = 1000,
        offset: int = 1,
    ) -> dict[str, Any]:
        if not self.token:
            params = {
                "dataset": "daily-summaries",
                "stations": _normalize_access_station_id(station_id),
                "startDate": start_date,
                "endDate": end_date,
                "format": "json",
                "units": units,
                "includeAttributes": "false",
            }
            if datatypeids:
                params["dataTypes"] = ",".join(datatypeids)
            rows = self._get_access_service(params)
            return {"results": _flatten_access_daily_rows(rows, datasetid=datasetid, datatypeids=datatypeids)}
        params = {
            "datasetid": datasetid,
            "stationid": station_id,
            "startdate": start_date,
            "enddate": end_date,
            "units": units,
            "limit": limit,
            "offset": offset,
        }
        if datatypeids:
            params["datatypeid"] = datatypeids
        return self._get("/data", params=params)

    def get_daily_climate_normals(
        self,
        station_id: str,
        start_date: str = "2010-01-01",
        end_date: str = "2010-12-31",
        datasetid: str = "NORMAL_DLY",
        datatypeids: list[str] | None = None,
        units: str = "metric",
        limit: int = 1000,
        offset: int = 1,
    ) -> dict[str, Any]:
        if not self.token:
            params = {
                "dataset": "normals-daily-1991-2020",
                "stations": _normalize_access_station_id(station_id),
                "startDate": start_date,
                "endDate": end_date,
                "format": "json",
                "units": units,
                "includeAttributes": "false",
            }
            if datatypeids:
                params["dataTypes"] = ",".join(datatypeids)
            rows = self._get_access_service(params)
            return {"results": _flatten_access_normals_rows(rows, datasetid=datasetid, datatypeids=datatypeids)}
        params = {
            "datasetid": datasetid,
            "stationid": station_id,
            "startdate": start_date,
            "enddate": end_date,
            "units": units,
            "limit": limit,
            "offset": offset,
        }
        if datatypeids:
            params["datatypeid"] = datatypeids
        return self._get("/data", params=params)


def _normalize_access_station_id(station_id: str) -> str:
    return station_id.split(":", 1)[-1]


def _flatten_access_daily_rows(
    rows: list[dict[str, Any]],
    datasetid: str,
    datatypeids: list[str] | None,
) -> list[dict[str, Any]]:
    normalized: list[dict[str, Any]] = []
    requested = datatypeids or []
    for row in rows:
        if not isinstance(row, dict):
            continue
        for datatype in requested:
            raw_value = row.get(datatype)
            if raw_value in (None, ""):
                continue
            normalized.append(
                {
                    "station": row.get("STATION"),
                    "date": row.get("DATE"),
                    "datatype": datatype,
                    "value": _access_metric_c_to_tenths(raw_value),
                    "datasetid": datasetid,
                }
            )
    return normalized


def _flatten_access_normals_rows(
    rows: list[dict[str, Any]],
    datasetid: str,
    datatypeids: list[str] | None,
) -> list[dict[str, Any]]:
    normalized: list[dict[str, Any]] = []
    requested = datatypeids or []
    for row in rows:
        if not isinstance(row, dict):
            continue
        month_day = row.get("DATE")
        if not isinstance(month_day, str):
            continue
        synthetic_date = f"2010-{month_day}"
        for datatype in requested:
            raw_value = row.get(datatype)
            if raw_value in (None, ""):
                continue
            normalized.append(
                {
                    "station": row.get("STATION"),
                    "date": synthetic_date,
                    "datatype": datatype,
                    "value": _access_metric_c_to_tenths(raw_value),
                    "datasetid": datasetid,
                }
            )
    return normalized


def _access_metric_c_to_tenths(value: Any) -> float:
    return round(float(value) * 10.0, 3)
