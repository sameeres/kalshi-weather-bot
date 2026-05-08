from __future__ import annotations

from datetime import date, datetime
from typing import Any

import requests

IEM_API_BASE_URL = "https://mesonet.agron.iastate.edu/api/1"


class IEMClient:
    """Client for Iowa State Environmental Mesonet (IEM) NWS text product archive.

    IEM archives NWS text products (including Zone Forecast Products) going back
    many years via two endpoints:
    - ``/nws/afos/list.json`` — lists product IDs issued for a given PIL and date
    - ``/nwstext/{product_id}`` — returns the raw text of a specific product

    Typical workflow: call ``list_afos_products`` to find the right product_id,
    then ``fetch_product_text`` to get the full text.
    """

    def __init__(self, base_url: str = IEM_API_BASE_URL, timeout: int = 30) -> None:
        self.base_url = base_url.rstrip("/")
        self.timeout = timeout
        self.session = requests.Session()
        self.session.headers.update({"User-Agent": "kalshi-weather-bot/1.0 research contact@example.com"})

    def list_afos_products(self, pil: str, product_date: date) -> list[dict[str, Any]]:
        """List NWS text products for a given PIL and calendar date (UTC).

        Args:
            pil: AFOS PIL code, e.g. 'ZFPOKX' (NYC) or 'ZFPLOT' (Chicago).
            product_date: UTC calendar date to list products for.

        Returns:
            List of product dicts with keys: ``entered`` (UTC ISO str), ``pil``,
            ``product_id``, ``cccc``, ``text_link``.  Sorted by entered time ascending.
        """
        url = f"{self.base_url}/nws/afos/list.json"
        params = {"pil": pil, "date": product_date.isoformat()}
        response = self.session.get(url, params=params, timeout=self.timeout)
        response.raise_for_status()
        payload = response.json()
        return payload.get("data", [])

    def fetch_product_text(self, product_id: str) -> str:
        """Fetch the raw NWS text for a given IEM product_id.

        Args:
            product_id: IEM product identifier, e.g.
                ``202511151012-KOKX-FPUS51-ZFPOKX``.

        Returns:
            Raw NWS text product as a string.
        """
        url = f"{self.base_url}/nwstext/{product_id}"
        response = self.session.get(url, timeout=self.timeout)
        response.raise_for_status()
        return response.text

    def fetch_afos_text_before(
        self,
        pil: str,
        product_date: date,
        before_utc: datetime,
    ) -> tuple[str, datetime] | None:
        """Fetch the most recent ZFP text issued for *product_date* before *before_utc*.

        Args:
            pil: AFOS PIL code.
            product_date: Calendar date (UTC) to search.
            before_utc: Return the latest product whose ``entered`` time is
                strictly before this UTC timestamp.

        Returns:
            ``(text, entered_utc)`` tuple, or ``None`` if no matching product found.
        """
        products = self.list_afos_products(pil=pil, product_date=product_date)
        if not products:
            return None

        # Products come sorted by entered time; pick the last one before the cutoff.
        selected: dict[str, Any] | None = None
        for product in products:
            entered_str = product.get("entered", "")
            if not entered_str:
                continue
            entered_dt = _parse_entered(entered_str)
            if entered_dt is None:
                continue
            if entered_dt < before_utc:
                selected = product
            else:
                break  # products are ascending; once we exceed cutoff we're done

        if selected is None:
            return None

        text = self.fetch_product_text(selected["product_id"])
        entered_utc = _parse_entered(selected["entered"])
        return text, entered_utc  # type: ignore[return-value]


def _parse_entered(entered_str: str) -> datetime | None:
    """Parse an IEM entered timestamp string to a UTC-aware datetime."""
    from datetime import timezone
    import pandas as pd

    ts = pd.to_datetime(entered_str, utc=True, errors="coerce")
    if pd.isna(ts):
        return None
    return ts.to_pydatetime().astimezone(timezone.utc)


def _split_afos_response(text: str) -> list[str]:
    """Split an AFOS text response into individual product strings.

    Kept for backward compatibility with tests; not used in the primary flow.
    """
    import re
    chunks = re.split(r"\n={3,}\n|\n\n\n+", text)
    return [chunk.strip() for chunk in chunks if chunk.strip()]
