"""Bearer-token validation contract for the secure-apps admin/config gates.

Every token here is genuinely RS256-signed and goes through the real
``auth._decode_bearer_token`` -> role-check path; only the JWKS network fetch is
stubbed. Positive cases mirror the real production callers (issuer as Keycloak
emits it under ``KC_HOSTNAME=https://auth.nonagentic.ai``); negative cases are
one per attack class.
"""

from __future__ import annotations

import base64
import json
import time

import jwt
import pytest
from cryptography.hazmat.primitives.asymmetric import rsa
from fastapi import HTTPException
from jwt.algorithms import RSAAlgorithm
from starlette.requests import Request

ISS = "https://auth.nonagentic.ai/realms/secure-apps"
PORTAL_ISS = "https://auth.nonagentic.ai/realms/portal"

_SA_KEY = rsa.generate_private_key(public_exponent=65537, key_size=2048)
_SA_KID = "secure-apps-sig"
_ENC_KEY = rsa.generate_private_key(public_exponent=65537, key_size=2048)
_ENC_KID = "secure-apps-enc"
_PORTAL_KEY = rsa.generate_private_key(public_exponent=65537, key_size=2048)
_PORTAL_KID = "portal-sig"


def _jwk(key: rsa.RSAPrivateKey, kid: str, use: str, alg: str) -> dict:
    jwk = json.loads(RSAAlgorithm.to_jwk(key.public_key()))
    jwk.update({"kid": kid, "use": use, "alg": alg})
    return jwk


def _secure_apps_jwks() -> dict:
    # Keycloak's realm JWKS carries an RS256 `sig` key AND an RSA-OAEP `enc` key.
    return {
        "keys": [
            _jwk(_SA_KEY, _SA_KID, "sig", "RS256"),
            _jwk(_ENC_KEY, _ENC_KID, "enc", "RSA-OAEP"),
        ]
    }


def _claims(roles: list[str], azp: str, extra: dict | None = None, **overrides: object) -> dict:
    now = int(time.time())
    claims: dict[str, object] = {
        "sub": "6f1c1b4e-6a43-4d5e-9d38-6b1f1b2c3d4e",
        "iss": ISS,
        "azp": azp,
        "typ": "Bearer",
        "iat": now,
        "exp": now + 300,
        "realm_access": {"roles": roles},
    }
    claims.update(overrides)
    claims.update(extra or {})
    return {k: v for k, v in claims.items() if v is not None}


def _mint(
    claims: dict,
    *,
    key: rsa.RSAPrivateKey = _SA_KEY,
    kid: str = _SA_KID,
    headers: dict | None = None,
) -> str:
    return jwt.encode(claims, key, algorithm="RS256", headers={"kid": kid, **(headers or {})})


def _b64(obj: dict) -> str:
    return base64.urlsafe_b64encode(json.dumps(obj).encode()).rstrip(b"=").decode()


def _request(token: str) -> Request:
    return Request(
        {
            "type": "http",
            "method": "PUT",
            "path": "/api/config/tenants/x/commercial-terms",
            "headers": [(b"authorization", f"Bearer {token}".encode())],
            "query_string": b"",
        }
    )


@pytest.fixture(autouse=True)
def real_jwt(monkeypatch):
    import auth

    fetched: list[str] = []

    async def _fake_fetch_jwks() -> dict:
        fetched.append("secure-apps")
        return _secure_apps_jwks()

    monkeypatch.setattr(auth, "DASHBOARD_AUTH_ENABLED", True)
    monkeypatch.setattr(auth, "KEYCLOAK_URL", "https://auth.nonagentic.ai")
    monkeypatch.setattr(auth, "KEYCLOAK_REALM", "secure-apps")
    monkeypatch.delenv("KEYCLOAK_ISSUER", raising=False)
    monkeypatch.setattr(auth, "_fetch_jwks", _fake_fetch_jwks)
    monkeypatch.setattr(auth, "_jwks_cache", None)
    monkeypatch.setattr(auth, "_jwks_cache_ts", 0.0)
    return fetched


# --- issuer derivation ------------------------------------------------------


@pytest.mark.unit
def test_issuer_derived_from_keycloak_url_and_realm():
    import auth

    assert auth.secure_apps_issuer() == ISS


@pytest.mark.unit
def test_issuer_explicit_override_wins(monkeypatch):
    import auth

    monkeypatch.setenv("KEYCLOAK_ISSUER", "https://kc.example/realms/secure-apps/")
    assert auth.secure_apps_issuer() == "https://kc.example/realms/secure-apps"


# --- positive: each legitimate production caller ------------------------------


@pytest.mark.unit
@pytest.mark.parametrize(
    "dep,azp,roles",
    [
        # commonpractice -> PUT /commercial-terms (require_pricing_writer)
        ("require_pricing_writer", "commonpractice-licenses-admin", ["license-admin"]),
        # kag-frontend's admin SA, same surface
        ("require_pricing_writer", "kag-licenses-admin", ["license-admin"]),
        # licences service provisioning offerings -> markup/access-charge writes
        (
            "require_pricing_writer",
            "licenses",
            ["service-internal", "billing-internal", "pricing-writer"],
        ),
        # dashboard / require_admin routes
        ("require_admin", "commonpractice-licenses-admin", ["license-admin"]),
        # billing's own pricing-seed CronJob SA
        ("require_pricing_admin", "billing-service", ["billing-internal"]),
    ],
)
async def test_legitimate_callers_pass(dep, azp, roles):
    import auth_deps

    payload = await getattr(auth_deps, dep)(_request(_mint(_claims(roles, azp))))
    assert payload["azp"] == azp
    assert payload["iss"] == ISS


# --- negative: one per attack class --------------------------------------------


async def _expect(status: int, token: str, dep: str = "require_pricing_writer") -> str:
    import auth_deps

    with pytest.raises(HTTPException) as exc:
        await getattr(auth_deps, dep)(_request(token))
    assert exc.value.status_code == status, exc.value.detail
    return str(exc.value.detail)


@pytest.mark.unit
async def test_wrong_realm_issuer_refused_even_with_secure_apps_key():
    # Signed by the secure-apps key but claiming the portal realm: only the
    # issuer pin catches this (the signature alone would pass).
    await _expect(401, _mint(_claims(["license-admin"], "fr-frontend", iss=PORTAL_ISS)))


@pytest.mark.unit
async def test_portal_realm_token_refused():
    # A genuine portal-realm token (its own realm key) with an admin-ish role.
    token = _mint(
        _claims(["license-admin", "super_admin"], "fr-frontend", iss=PORTAL_ISS),
        key=_PORTAL_KEY,
        kid=_PORTAL_KID,
    )
    await _expect(401, token)


@pytest.mark.unit
async def test_internal_hostname_issuer_refused():
    # Keycloak never emits this under KC_HOSTNAME; a token that does is foreign.
    iss = "http://keycloak.keycloak.svc.cluster.local:8080/realms/secure-apps"
    await _expect(401, _mint(_claims(["license-admin"], "x", iss=iss)))


@pytest.mark.unit
async def test_missing_iss_refused():
    await _expect(401, _mint(_claims(["license-admin"], "x", iss=None)))


@pytest.mark.unit
async def test_missing_exp_refused():
    # Before hardening this passed: PyJWT only checks exp when present.
    await _expect(401, _mint(_claims(["license-admin"], "x", exp=None)))


@pytest.mark.unit
async def test_expired_refused():
    past = int(time.time()) - 3600
    detail = await _expect(401, _mint(_claims(["license-admin"], "x", iat=past - 60, exp=past)))
    assert "expired" in detail.lower()


@pytest.mark.unit
async def test_missing_sub_refused():
    await _expect(401, _mint(_claims(["license-admin"], "x", sub=None)))


@pytest.mark.unit
@pytest.mark.parametrize(
    "roles",
    [["billing-internal"], ["onboarding-internal"], ["service-internal"], []],
)
async def test_missing_role_refused(roles):
    await _expect(403, _mint(_claims(roles, "commonpractice-licenses-admin")))


@pytest.mark.unit
async def test_flat_roles_claim_does_not_satisfy_writer_gate():
    await _expect(
        403,
        _mint(_claims([], "x", extra={"roles": ["license-admin", "pricing-writer"]})),
    )


@pytest.mark.unit
async def test_unknown_role_holder_other_client_still_needs_role():
    # azp is not an authorisation input; a different client WITHOUT the role fails.
    await _expect(403, _mint(_claims(["offline_access"], "some-other-client")))


@pytest.mark.unit
async def test_alg_none_refused():
    token = (
        f"{_b64({'alg': 'none', 'typ': 'JWT', 'kid': _SA_KID})}."
        f"{_b64(_claims(['license-admin'], 'x'))}."
    )
    await _expect(401, token)


@pytest.mark.unit
async def test_hmac_alg_confusion_refused():
    # Classic confusion: HS256 keyed with the realm's PUBLIC key PEM.
    from cryptography.hazmat.primitives import serialization

    pem = _SA_KEY.public_key().public_bytes(
        serialization.Encoding.PEM, serialization.PublicFormat.SubjectPublicKeyInfo
    )
    header = _b64({"alg": "HS256", "typ": "JWT", "kid": _SA_KID})
    body = _b64(_claims(["license-admin"], "x"))
    import hashlib
    import hmac

    sig = hmac.new(pem, f"{header}.{body}".encode(), hashlib.sha256).digest()
    token = f"{header}.{body}.{base64.urlsafe_b64encode(sig).rstrip(b'=').decode()}"
    await _expect(401, token)


@pytest.mark.unit
async def test_jku_header_is_ignored(real_jwt):
    # Attacker signs with their own key and points jku at their own JWKS. The
    # JWKS URL is pinned config, so the attacker kid is unknown -> 401, and no
    # fetch other than the pinned secure-apps one happens.
    attacker = rsa.generate_private_key(public_exponent=65537, key_size=2048)
    token = _mint(
        _claims(["license-admin"], "x"),
        key=attacker,
        kid="attacker",
        headers={"jku": "https://evil.example/jwks.json"},
    )
    await _expect(401, token)
    assert set(real_jwt) == {"secure-apps"}


@pytest.mark.unit
async def test_attacker_key_reusing_real_kid_refused():
    attacker = rsa.generate_private_key(public_exponent=65537, key_size=2048)
    await _expect(401, _mint(_claims(["license-admin"], "x"), key=attacker, kid=_SA_KID))


@pytest.mark.unit
async def test_enc_key_is_never_a_verification_key():
    # Signed with the private half of the realm's `enc` (RSA-OAEP) key: that key
    # is published in the JWKS but must not be accepted for signatures.
    await _expect(401, _mint(_claims(["license-admin"], "x"), key=_ENC_KEY, kid=_ENC_KID))


@pytest.mark.unit
@pytest.mark.parametrize("dep", ["require_admin", "require_pricing_admin", "require_internal"])
async def test_every_bearer_gate_shares_the_issuer_pin(dep):
    token = _mint(_claims(["license-admin", "billing-internal"], "x", iss=PORTAL_ISS))
    await _expect(401, token, dep=dep)
