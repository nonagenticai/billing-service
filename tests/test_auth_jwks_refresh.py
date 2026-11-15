"""JWKS cache refresh tests — TTL expiry + refetch-on-kid-miss.

Regression suite for B1: ``_jwks_cache`` previously had no TTL or
invalidation, so every Keycloak signing-key rotation silently broke
metering auth until pods were restarted.
"""

from unittest.mock import AsyncMock, MagicMock, patch

import pytest
from fastapi import HTTPException


def _make_mock_client(json_payloads: list[dict]) -> tuple[MagicMock, AsyncMock]:
    """Build an httpx.AsyncClient mock that returns each payload in sequence.

    Each call to ``client.get`` produces a response whose ``json()`` returns
    the next entry from ``json_payloads``. Returns ``(client_mock, get_mock)``
    so tests can introspect call count.
    """
    responses = []
    for payload in json_payloads:
        resp = MagicMock()
        resp.json.return_value = payload
        resp.raise_for_status = MagicMock()
        responses.append(resp)

    get_mock = AsyncMock(side_effect=responses)
    client = AsyncMock()
    client.get = get_mock
    client.__aenter__ = AsyncMock(return_value=client)
    client.__aexit__ = AsyncMock(return_value=False)
    return client, get_mock


@pytest.fixture(autouse=True)
def _reset_jwks_cache():
    """Each test starts with an empty JWKS cache."""
    import auth as auth_module

    old_cache = auth_module._jwks_cache
    old_ts = auth_module._jwks_cache_ts
    auth_module._jwks_cache = None
    auth_module._jwks_cache_ts = 0.0
    yield
    auth_module._jwks_cache = old_cache
    auth_module._jwks_cache_ts = old_ts


class TestJwksTtlAndRefetch:
    async def test_first_request_fetches_jwks(self):
        """Cold cache: a single fetch happens and the result is returned."""
        import auth as auth_module

        client, get_mock = _make_mock_client([{"keys": [{"kid": "k1"}]}])

        with patch("auth.httpx.AsyncClient", return_value=client):
            jwks = await auth_module._get_jwks()

        assert jwks == {"keys": [{"kid": "k1"}]}
        get_mock.assert_awaited_once()

    async def test_second_request_within_ttl_uses_cache(self):
        """Second call within TTL must NOT issue a second fetch."""
        import auth as auth_module

        client, get_mock = _make_mock_client(
            [{"keys": [{"kid": "k1"}]}, {"keys": [{"kid": "should-not-fetch"}]}]
        )

        with patch("auth.httpx.AsyncClient", return_value=client):
            jwks1 = await auth_module._get_jwks()
            jwks2 = await auth_module._get_jwks()

        assert jwks1 == jwks2 == {"keys": [{"kid": "k1"}]}
        # Critical: only one fetch despite two calls.
        assert get_mock.await_count == 1

    async def test_request_after_ttl_refetches(self, monkeypatch):
        """After TTL expires, the next call fetches fresh JWKS."""
        import auth as auth_module

        client, get_mock = _make_mock_client([{"keys": [{"kid": "k1"}]}, {"keys": [{"kid": "k2"}]}])

        # Fake monotonic clock controlled by ``current[0]``: pre-fetch we set it
        # to 1000.0, then advance past the TTL before the second call. Using a
        # mutable holder (not a generator) keeps the mock safe even if other
        # code paths read the clock more than once per _get_jwks() call.
        current = [1000.0]
        monkeypatch.setattr(auth_module.time, "monotonic", lambda: current[0])

        with patch("auth.httpx.AsyncClient", return_value=client):
            first = await auth_module._get_jwks()
            current[0] = 1000.0 + auth_module._JWKS_TTL_SECONDS + 1.0
            second = await auth_module._get_jwks()

        assert first == {"keys": [{"kid": "k1"}]}
        assert second == {"keys": [{"kid": "k2"}]}
        assert get_mock.await_count == 2

    async def test_force_refresh_refetches_within_ttl(self):
        """force_refresh=True bypasses the TTL window."""
        import auth as auth_module

        client, get_mock = _make_mock_client([{"keys": [{"kid": "k1"}]}, {"keys": [{"kid": "k2"}]}])

        with patch("auth.httpx.AsyncClient", return_value=client):
            await auth_module._get_jwks()
            refreshed = await auth_module._get_jwks(force_refresh=True)

        assert refreshed == {"keys": [{"kid": "k2"}]}
        assert get_mock.await_count == 2

    async def test_kid_miss_triggers_refetch_and_succeeds(self, monkeypatch):
        """A token with a freshly-rotated kid should drive ONE refetch then verify.

        This is the production rotation scenario: cached JWKS has key ``old``,
        but a brand-new token is signed by ``new``. We expect the verifier to
        refetch JWKS, find ``new``, and decode successfully.
        """
        import auth as auth_module

        monkeypatch.setattr(auth_module, "DASHBOARD_AUTH_ENABLED", True)

        # Pre-seed cache with the OLD JWKS so the first verify-attempt misses.
        auth_module._jwks_cache = {"keys": [{"kid": "old"}]}
        auth_module._jwks_cache_ts = auth_module.time.monotonic()

        # The refetch returns the NEW JWKS (rotation has happened upstream).
        client, get_mock = _make_mock_client([{"keys": [{"kid": "new"}]}])

        request = MagicMock()
        request.headers = MagicMock()
        request.headers.get = MagicMock(return_value="Bearer freshly-rotated-token")

        # PyJWKSet.from_dict returns a set whose only key matches the JWKS payload.
        def fake_jwkset_from_dict(payload):
            kid = payload["keys"][0]["kid"]
            return MagicMock(keys=[MagicMock(key_id=kid, key=f"key-material-{kid}")])

        decoded_payload = {"sub": "u", "realm_access": {"roles": ["license-admin"]}}

        with (
            patch("auth.httpx.AsyncClient", return_value=client),
            patch("auth.jwt.PyJWKSet.from_dict", side_effect=fake_jwkset_from_dict),
            patch("auth.jwt.get_unverified_header", return_value={"kid": "new"}),
            patch("auth.jwt.decode", return_value=decoded_payload) as decode_mock,
        ):
            result = await auth_module._decode_bearer_token(request)

        assert result == decoded_payload
        # Exactly one refetch — not zero (kid was missing) and not many (no loop).
        assert get_mock.await_count == 1
        # Decode happened with the NEW key material.
        decode_mock.assert_called_once()
        _, kwargs = decode_mock.call_args
        # First positional arg is the token, second is the key.
        assert decode_mock.call_args.args[1] == "key-material-new"

    async def test_kid_miss_after_refetch_raises_401_no_loop(self, monkeypatch):
        """If refetched JWKS still lacks the kid, surface 401 and don't loop."""
        import auth as auth_module

        monkeypatch.setattr(auth_module, "DASHBOARD_AUTH_ENABLED", True)

        # Cache holds an old key; refetch ALSO returns a non-matching key.
        auth_module._jwks_cache = {"keys": [{"kid": "old"}]}
        auth_module._jwks_cache_ts = auth_module.time.monotonic()

        client, get_mock = _make_mock_client([{"keys": [{"kid": "still-not-it"}]}])

        request = MagicMock()
        request.headers = MagicMock()
        request.headers.get = MagicMock(return_value="Bearer foreign-token")

        def fake_jwkset_from_dict(payload):
            kid = payload["keys"][0]["kid"]
            return MagicMock(keys=[MagicMock(key_id=kid, key=f"key-material-{kid}")])

        with (
            patch("auth.httpx.AsyncClient", return_value=client),
            patch("auth.jwt.PyJWKSet.from_dict", side_effect=fake_jwkset_from_dict),
            patch("auth.jwt.get_unverified_header", return_value={"kid": "alien"}),
            patch("auth.jwt.decode") as decode_mock,
            pytest.raises(HTTPException) as exc_info,
        ):
            await auth_module._decode_bearer_token(request)

        assert exc_info.value.status_code == 401
        assert "Unknown signing key" in exc_info.value.detail
        # Exactly ONE refetch — not a loop. The first verify uses the cache.
        assert get_mock.await_count == 1
        # decode is never reached when kid never resolves.
        decode_mock.assert_not_called()

    async def test_known_kid_does_not_trigger_refetch(self, monkeypatch):
        """Happy path: cached kid matches → no extra fetch on verify."""
        import auth as auth_module

        monkeypatch.setattr(auth_module, "DASHBOARD_AUTH_ENABLED", True)

        auth_module._jwks_cache = {"keys": [{"kid": "current"}]}
        auth_module._jwks_cache_ts = auth_module.time.monotonic()

        # If a fetch is attempted, this mock would explode (no responses queued).
        client, get_mock = _make_mock_client([])

        request = MagicMock()
        request.headers = MagicMock()
        request.headers.get = MagicMock(return_value="Bearer ok-token")

        def fake_jwkset_from_dict(payload):
            kid = payload["keys"][0]["kid"]
            return MagicMock(keys=[MagicMock(key_id=kid, key="km")])

        decoded = {"sub": "u", "realm_access": {"roles": ["license-admin"]}}

        with (
            patch("auth.httpx.AsyncClient", return_value=client),
            patch("auth.jwt.PyJWKSet.from_dict", side_effect=fake_jwkset_from_dict),
            patch("auth.jwt.get_unverified_header", return_value={"kid": "current"}),
            patch("auth.jwt.decode", return_value=decoded),
        ):
            result = await auth_module._decode_bearer_token(request)

        assert result == decoded
        get_mock.assert_not_awaited()
