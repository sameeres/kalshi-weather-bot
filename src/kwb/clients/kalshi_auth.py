from __future__ import annotations

import base64
import hashlib
import os
import time
from pathlib import Path
from typing import Any

import requests

from kwb.clients.kalshi import KalshiClient
from kwb.utils.logging import get_logger

logger = get_logger(__name__)

_KALSHI_API_KEY_ID_ENV = "KALSHI_API_KEY_ID"
_KALSHI_PRIVATE_KEY_PATH_ENV = "KALSHI_PRIVATE_KEY_PATH"
_KALSHI_PRIVATE_KEY_ENV = "KALSHI_PRIVATE_KEY"


class KalshiAuthError(ValueError):
    """Raised when Kalshi authenticated client cannot be initialized or a signed request fails."""


class KalshiAuthClient(KalshiClient):
    """Kalshi client extended with RSA-PSS request signing and order-placement endpoints.

    Kalshi's trading API requires every mutating request (and balance/portfolio reads)
    to carry three extra headers:
      KALSHI-ACCESS-KEY       — the API key ID (UUID string from the dashboard)
      KALSHI-ACCESS-TIMESTAMP — current time in milliseconds since epoch (str)
      KALSHI-ACCESS-SIGNATURE — base64(RSA-PSS-SHA256(timestamp + METHOD + path))

    The private key must be the RSA PEM key downloaded from the Kalshi dashboard.
    """

    def __init__(
        self,
        api_key_id: str | None = None,
        private_key_path: str | Path | None = None,
        private_key_pem: str | None = None,
        **kwargs: Any,
    ) -> None:
        super().__init__(**kwargs)
        self._api_key_id = api_key_id or os.environ.get(_KALSHI_API_KEY_ID_ENV, "")
        # Inline PEM content takes priority over file path.
        self._private_key_pem = private_key_pem or os.environ.get(_KALSHI_PRIVATE_KEY_ENV, "")
        raw_path = private_key_path or os.environ.get(_KALSHI_PRIVATE_KEY_PATH_ENV, "")
        self._private_key_path = Path(raw_path).expanduser() if raw_path else None
        self._private_key = self._load_private_key()

    # ------------------------------------------------------------------
    # Internal helpers
    # ------------------------------------------------------------------

    def _load_private_key(self) -> Any:
        from cryptography.hazmat.primitives.serialization import load_pem_private_key

        if not self._api_key_id:
            raise KalshiAuthError(
                f"Kalshi API key ID is missing. "
                f"Set {_KALSHI_API_KEY_ID_ENV} in your environment or .env file."
            )

        # Resolve PEM bytes from inline content or file path.
        if self._private_key_pem:
            # Env vars can't contain real newlines, so allow \n as a substitute.
            pem_bytes = self._private_key_pem.replace("\\n", "\n").encode("utf-8")
        elif self._private_key_path and self._private_key_path.exists():
            pem_bytes = self._private_key_path.read_bytes()
        else:
            raise KalshiAuthError(
                f"No Kalshi private key found. "
                f"Set {_KALSHI_PRIVATE_KEY_ENV} to the PEM content, or "
                f"set {_KALSHI_PRIVATE_KEY_PATH_ENV} to the path of your RSA PEM file."
            )

        # Try PEM first. If that fails, treat the content as raw base64 DER and
        # wrap it with PKCS#8 headers before retrying — Kalshi sometimes provides
        # the key as bare base64 without PEM armor.
        for header, footer in [
            (b"-----BEGIN RSA PRIVATE KEY-----", b"-----END RSA PRIVATE KEY-----"),
            (b"-----BEGIN PRIVATE KEY-----", b"-----END PRIVATE KEY-----"),
        ]:
            if header in pem_bytes:
                try:
                    return load_pem_private_key(pem_bytes, password=None)
                except Exception as exc:
                    raise KalshiAuthError(f"Could not load RSA private key: {exc}") from exc

        # No PEM headers found — assume raw base64 DER, try both PKCS#1 and PKCS#8 wrappers.
        import base64 as _b64
        raw_b64 = pem_bytes.replace(b"\\n", b"").replace(b"\n", b"").replace(b" ", b"")
        for header, footer in [
            (b"-----BEGIN RSA PRIVATE KEY-----", b"-----END RSA PRIVATE KEY-----"),
            (b"-----BEGIN PRIVATE KEY-----", b"-----END PRIVATE KEY-----"),
        ]:
            wrapped = header + b"\n" + raw_b64 + b"\n" + footer
            try:
                return load_pem_private_key(wrapped, password=None)
            except Exception:
                continue

        raise KalshiAuthError(
            "Could not load RSA private key. Ensure KALSHI_PRIVATE_KEY contains a valid "
            "PEM or base64-encoded RSA private key."
        )

    def _sign_request(self, method: str, path: str) -> dict[str, str]:
        from cryptography.hazmat.primitives import hashes
        from cryptography.hazmat.primitives.asymmetric import padding
        from urllib.parse import urlparse

        # Kalshi requires the full server path (including the /trade-api/v2 prefix)
        # in the signed message, not just the endpoint-relative path.
        base_path = urlparse(self.base_url).path.rstrip("/")
        full_path = base_path + path
        timestamp_ms = str(int(time.time() * 1000))
        message = (timestamp_ms + method.upper() + full_path).encode("utf-8")
        signature_bytes = self._private_key.sign(
            message,
            padding.PSS(
                mgf=padding.MGF1(hashes.SHA256()),
                salt_length=padding.PSS.DIGEST_LENGTH,
            ),
            hashes.SHA256(),
        )
        return {
            "KALSHI-ACCESS-KEY": self._api_key_id,
            "KALSHI-ACCESS-TIMESTAMP": timestamp_ms,
            "KALSHI-ACCESS-SIGNATURE": base64.b64encode(signature_bytes).decode("utf-8"),
            "Content-Type": "application/json",
        }

    def _auth_get(self, path: str, params: dict[str, Any] | None = None) -> dict[str, Any]:
        url = f"{self.base_url}{path}"
        headers = self._sign_request("GET", path)
        response = self.session.get(url, headers=headers, params=params, timeout=self.timeout)
        response.raise_for_status()
        return response.json()

    def _post(self, path: str, body: dict[str, Any]) -> dict[str, Any]:
        import json as _json

        url = f"{self.base_url}{path}"
        headers = self._sign_request("POST", path)
        attempt = 0

        while True:
            attempt += 1
            try:
                response = self.session.post(
                    url,
                    headers=headers,
                    data=_json.dumps(body),
                    timeout=self.timeout,
                )
                response.raise_for_status()
                return response.json()
            except requests.HTTPError as exc:
                status_code = exc.response.status_code if exc.response is not None else None
                from kwb.clients.kalshi import RETRYABLE_STATUS_CODES
                if status_code not in RETRYABLE_STATUS_CODES or attempt > self.max_retries:
                    raise
                self._sleep_before_retry(path=path, attempt=attempt, status_code=status_code, response=exc.response)
            except (requests.ConnectionError, requests.Timeout) as exc:
                if attempt > self.max_retries:
                    raise
                self._sleep_before_retry(path=path, attempt=attempt, status_code=None, response=None, error=exc)

    def _delete(self, path: str) -> dict[str, Any]:
        url = f"{self.base_url}{path}"
        headers = self._sign_request("DELETE", path)
        response = self.session.delete(url, headers=headers, timeout=self.timeout)
        response.raise_for_status()
        return response.json()

    # ------------------------------------------------------------------
    # Account / portfolio endpoints
    # ------------------------------------------------------------------

    def get_balance(self) -> dict[str, Any]:
        """Return current portfolio balance. Useful for verifying credentials."""
        return self._auth_get("/portfolio/balance")

    def get_portfolio_orders(
        self,
        ticker: str | None = None,
        status: str | None = None,
        limit: int = 100,
    ) -> dict[str, Any]:
        params: dict[str, Any] = {"limit": limit}
        if ticker:
            params["ticker"] = ticker
        if status:
            params["status"] = status
        return self._auth_get("/portfolio/orders", params=params)

    def get_order(self, order_id: str) -> dict[str, Any]:
        return self._auth_get(f"/portfolio/orders/{order_id}")

    # ------------------------------------------------------------------
    # Order placement and cancellation
    # ------------------------------------------------------------------

    def create_order(
        self,
        market_ticker: str,
        side: str,
        price_cents: int,
        count: int = 1,
        order_type: str = "limit",
    ) -> dict[str, Any]:
        """Place a limit order on a Kalshi market.

        Args:
            market_ticker: Kalshi market ticker (e.g. 'HIGHNY-25JAN01-T50').
            side: 'yes' or 'no'.
            price_cents: Limit price in cents (1–99).
            count: Number of contracts.
            order_type: 'limit' (only supported type for now).

        Returns:
            The Kalshi order response dict containing order_id, status, etc.
        """
        side = side.lower()
        if side not in ("yes", "no"):
            raise KalshiAuthError(f"side must be 'yes' or 'no', got {side!r}")
        if not (1 <= price_cents <= 99):
            raise KalshiAuthError(f"price_cents must be 1–99, got {price_cents}")
        if count < 1:
            raise KalshiAuthError(f"count must be >= 1, got {count}")

        body: dict[str, Any] = {
            "action": "buy",
            "count": count,
            "side": side,
            "ticker": market_ticker,
            "type": order_type,
        }
        if side == "yes":
            body["yes_price"] = price_cents
        else:
            body["no_price"] = price_cents

        logger.info(
            "Placing %s order: ticker=%s side=%s price=%d¢ count=%d",
            order_type,
            market_ticker,
            side,
            price_cents,
            count,
        )
        result = self._post("/portfolio/orders", body)
        order = result.get("order", result)
        logger.info(
            "Order response: order_id=%s status=%s filled=%s remaining=%s",
            order.get("order_id"),
            order.get("status"),
            order.get("filled_count"),
            order.get("remaining_count"),
        )
        return result

    def cancel_order(self, order_id: str) -> dict[str, Any]:
        """Cancel an open order by order_id."""
        logger.info("Cancelling order: order_id=%s", order_id)
        return self._delete(f"/portfolio/orders/{order_id}")


def build_auth_client_from_env(**kwargs: Any) -> KalshiAuthClient:
    """Instantiate KalshiAuthClient from environment variables.

    Raises KalshiAuthError if credentials are missing or invalid.
    """
    return KalshiAuthClient(**kwargs)
