from __future__ import annotations

from typing import Any

from kwb.clients.ncei import NCEIClient


class _Response:
    def __init__(self, payload: Any) -> None:
        self._payload = payload

    def raise_for_status(self) -> None:
        return None

    def json(self) -> Any:
        return self._payload


def test_ncei_client_uses_access_service_for_daily_summaries_without_token(monkeypatch) -> None:
    client = NCEIClient()
    client.token = None
    captured: dict[str, Any] = {}

    def fake_get(url: str, params=None, timeout: int = 20):
        captured["url"] = url
        captured["params"] = params
        return _Response(
            [
                {"DATE": "2015-01-01", "STATION": "USW00014819", "TMAX": "2.2"},
            ]
        )

    monkeypatch.setattr(client.session, "get", fake_get)

    payload = client.get_daily_station_observations(
        station_id="GHCND:USW00014819",
        start_date="2015-01-01",
        end_date="2015-01-01",
        datatypeids=["TMAX"],
    )

    assert captured["url"] == client.access_services_url
    assert captured["params"]["dataset"] == "daily-summaries"
    assert captured["params"]["stations"] == "USW00014819"
    assert payload["results"] == [
        {
            "station": "USW00014819",
            "date": "2015-01-01",
            "datatype": "TMAX",
            "value": 22.0,
            "datasetid": "GHCND",
        }
    ]


def test_ncei_client_uses_access_service_for_daily_normals_without_token(monkeypatch) -> None:
    client = NCEIClient()
    client.token = None

    def fake_get(url: str, params=None, timeout: int = 20):
        return _Response(
            [
                {
                    "DATE": "01-01",
                    "STATION": "USW00014819",
                    "DLY-TMAX-NORMAL": "0.9",
                    "DLY-TMIN-NORMAL": "-6.1",
                },
            ]
        )

    monkeypatch.setattr(client.session, "get", fake_get)

    payload = client.get_daily_climate_normals(
        station_id="GHCND:USW00014819",
        datatypeids=["DLY-TMAX-NORMAL", "DLY-TMIN-NORMAL"],
    )

    assert payload["results"] == [
        {
            "station": "USW00014819",
            "date": "2010-01-01",
            "datatype": "DLY-TMAX-NORMAL",
            "value": 9.0,
            "datasetid": "NORMAL_DLY",
        },
        {
            "station": "USW00014819",
            "date": "2010-01-01",
            "datatype": "DLY-TMIN-NORMAL",
            "value": -61.0,
            "datasetid": "NORMAL_DLY",
        },
    ]
