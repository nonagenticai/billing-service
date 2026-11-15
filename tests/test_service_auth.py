"""Tests for the shared service-account token acquisition."""

import time
from unittest.mock import AsyncMock, MagicMock, patch

import httpx
import pytest

from service_auth import ServiceTokenProvider
from settings import Settings
from tests.conftest import T

# ``build_headers`` puts this straight into X-Tenant-Id on an outbound internal
# call, and every receiver validates it as a uuid since migration 0011. A
# readable-but-invalid label such as "t-1" would pass these dict assertions
# while being exactly the value shape that 400s on the wire.
TENANT_ID = T("t-1")


@pytest.fixture
def settings_with_keycloak() -> Settings:
    return Settings(
        keycloak_token_url="http://keycloak.test/realms/secure-apps/protocol/openid-connect/token",
        service_client_id="billing-service",
        service_client_secret="test-secret",
    )


@pytest.fixture
def settings_without_keycloak() -> Settings:
    return Settings(
        keycloak_token_url="",
        service_client_id="",
        service_client_secret="",
    )


class TestServiceTokenProvider:
    async def test_acquires_token_from_keycloak(self, settings_with_keycloak):
        provider = ServiceTokenProvider(settings_with_keycloak)

        mock_response = MagicMock()
        mock_response.status_code = 200
        mock_response.json.return_value = {"access_token": "jwt-123", "expires_in": 300}

        mock_client = AsyncMock()
        mock_client.post = AsyncMock(return_value=mock_response)
        mock_client.__aenter__ = AsyncMock(return_value=mock_client)
        mock_client.__aexit__ = AsyncMock(return_value=False)

        with patch("service_auth.httpx.AsyncClient", return_value=mock_client):
            token = await provider.get_token()

        assert token == "jwt-123"

    async def test_caches_token_within_expiry(self, settings_with_keycloak):
        provider = ServiceTokenProvider(settings_with_keycloak)

        mock_response = MagicMock()
        mock_response.status_code = 200
        mock_response.json.return_value = {"access_token": "jwt-123", "expires_in": 300}

        mock_client = AsyncMock()
        mock_client.post = AsyncMock(return_value=mock_response)
        mock_client.__aenter__ = AsyncMock(return_value=mock_client)
        mock_client.__aexit__ = AsyncMock(return_value=False)

        with patch("service_auth.httpx.AsyncClient", return_value=mock_client):
            await provider.get_token()
            await provider.get_token()

        assert mock_client.post.await_count == 1

    async def test_returns_none_when_not_configured(self, settings_without_keycloak):
        provider = ServiceTokenProvider(settings_without_keycloak)
        token = await provider.get_token()
        assert token is None

    async def test_returns_none_on_keycloak_error(self, settings_with_keycloak):
        provider = ServiceTokenProvider(settings_with_keycloak)

        mock_response = MagicMock()
        mock_response.status_code = 401

        mock_client = AsyncMock()
        mock_client.post = AsyncMock(return_value=mock_response)
        mock_client.__aenter__ = AsyncMock(return_value=mock_client)
        mock_client.__aexit__ = AsyncMock(return_value=False)

        with patch("service_auth.httpx.AsyncClient", return_value=mock_client):
            token = await provider.get_token()

        assert token is None

    async def test_returns_none_on_http_error(self, settings_with_keycloak):
        provider = ServiceTokenProvider(settings_with_keycloak)

        mock_client = AsyncMock()
        mock_client.post = AsyncMock(side_effect=httpx.ConnectError("connection refused"))
        mock_client.__aenter__ = AsyncMock(return_value=mock_client)
        mock_client.__aexit__ = AsyncMock(return_value=False)

        with patch("service_auth.httpx.AsyncClient", return_value=mock_client):
            token = await provider.get_token()

        assert token is None

    def test_configured_true(self, settings_with_keycloak):
        provider = ServiceTokenProvider(settings_with_keycloak)
        assert provider.configured is True

    def test_configured_false(self, settings_without_keycloak):
        provider = ServiceTokenProvider(settings_without_keycloak)
        assert provider.configured is False

    def test_build_headers_with_token(self, settings_with_keycloak):
        provider = ServiceTokenProvider(settings_with_keycloak)
        provider._token = "jwt-456"
        provider._expires_at = time.monotonic() + 999
        headers = provider.build_headers(tenant_id=TENANT_ID)
        assert headers == {
            "X-Tenant-Id": TENANT_ID,
            "Authorization": "Bearer jwt-456",
        }

    def test_build_headers_without_token(self, settings_without_keycloak):
        provider = ServiceTokenProvider(settings_without_keycloak)
        headers = provider.build_headers(tenant_id=TENANT_ID)
        assert headers == {"X-Tenant-Id": TENANT_ID}

    def test_build_headers_expired_token(self, settings_with_keycloak):
        provider = ServiceTokenProvider(settings_with_keycloak)
        provider._token = "jwt-old"
        provider._expires_at = time.monotonic() - 100  # expired
        headers = provider.build_headers(tenant_id=TENANT_ID)
        assert headers == {"X-Tenant-Id": TENANT_ID}
        assert "Authorization" not in headers

    async def test_refreshes_expired_token(self, settings_with_keycloak):
        provider = ServiceTokenProvider(settings_with_keycloak)
        provider._token = "jwt-old"
        provider._expires_at = time.monotonic() - 100  # expired

        mock_response = MagicMock()
        mock_response.status_code = 200
        mock_response.json.return_value = {"access_token": "jwt-new", "expires_in": 300}

        mock_client = AsyncMock()
        mock_client.post = AsyncMock(return_value=mock_response)
        mock_client.__aenter__ = AsyncMock(return_value=mock_client)
        mock_client.__aexit__ = AsyncMock(return_value=False)

        with patch("service_auth.httpx.AsyncClient", return_value=mock_client):
            token = await provider.get_token()

        assert token == "jwt-new"
