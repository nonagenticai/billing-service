"""Shared Keycloak service-account token acquisition.

Used by billing and budget sync to authenticate metering->licenses calls.
"""

import time

import httpx
import structlog

from settings import Settings

logger = structlog.stdlib.get_logger(__name__)


class ServiceTokenProvider:
    """Acquires and caches a Keycloak service-account JWT via client credentials."""

    def __init__(self, settings: Settings) -> None:
        self._token_url = settings.keycloak_token_url
        self._client_id = settings.service_client_id
        self._client_secret = settings.service_client_secret
        self._token: str | None = None
        self._expires_at: float = 0.0

    @property
    def configured(self) -> bool:
        return bool(self._token_url and self._client_secret)

    async def get_token(self) -> str | None:
        """Return a cached or fresh service-account JWT, or None if not configured."""
        if not self.configured:
            return None

        if self._token and time.monotonic() < self._expires_at:
            return self._token

        try:
            async with httpx.AsyncClient(timeout=5.0) as client:
                resp = await client.post(
                    self._token_url,
                    data={
                        "grant_type": "client_credentials",
                        "client_id": self._client_id,
                        "client_secret": self._client_secret,
                    },
                )
                if resp.status_code == 200:
                    body = resp.json()
                    self._token = body["access_token"]
                    expires_in = body.get("expires_in", 300)
                    self._expires_at = time.monotonic() + expires_in - 30
                    return self._token
                logger.warning("service_token_failed", status=resp.status_code)
        except httpx.HTTPError:
            logger.warning("service_token_error", exc_info=True)

        return None

    def build_headers(self, tenant_id: str) -> dict[str, str]:
        """Build request headers for an internal licenses call."""
        headers: dict[str, str] = {"X-Tenant-Id": tenant_id}
        if self._token and time.monotonic() < self._expires_at:
            headers["Authorization"] = f"Bearer {self._token}"
        return headers
