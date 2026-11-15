"""Dashboard authentication — Keycloak JWT validation requiring license-admin role."""

import os
import time

import httpx
import jwt
import structlog
from fastapi import HTTPException, Request

from tenancy import derived_tenant_id

logger = structlog.stdlib.get_logger(__name__)

# Per gitops-production/docs/config-externalization-policy.md: source code does
# NOT carry hardcoded service URL fallbacks. KEYCLOAK_URL is read from the env
# var (set by the Deployment manifest); falling back to settings.keycloak_url
# (KEYCLOAK_URL) lets the Pydantic prod validator catch missing values
# at startup with a uniform error.
KEYCLOAK_URL = os.getenv("KEYCLOAK_URL", "")
KEYCLOAK_REALM = os.getenv("KEYCLOAK_REALM", "secure-apps")
DASHBOARD_AUTH_ENABLED = os.getenv("DASHBOARD_AUTH_ENABLED", "true").lower() == "true"
REQUIRED_ROLE = "license-admin"


def _get_keycloak_url() -> str:
    """Return Keycloak base URL, falling back to settings if env var unset.

    Production: ``KEYCLOAK_URL`` env var is set by the Deployment manifest, so
    the module-level constant above wins. If both that env var AND
    ``KEYCLOAK_URL`` are unset, Settings() will already have raised
    in production via :attr:`Settings._REQUIRED_IN_PRODUCTION`.
    """
    if KEYCLOAK_URL:
        return KEYCLOAK_URL
    # Lazy import: avoid pulling settings at module load (test conftest sets
    # ENVIRONMENT=test before settings is constructed).
    from settings import get_settings

    return get_settings().keycloak_url


# JWKS cache: refreshed on TTL expiry or on a `kid` miss. Without TTL the cache
# never reflected Keycloak signing-key rotations and licenses → metering auth
# silently broke every quarterly rotation until pods were restarted.
_JWKS_TTL_SECONDS = 600  # 10 minutes
_jwks_cache: dict | None = None
_jwks_cache_ts: float = 0.0


class _UnknownKidError(Exception):
    """Raised internally when a token's kid is not in the cached JWKS."""


async def _fetch_jwks() -> dict:
    """Fetch JWKS from Keycloak. Raises HTTPException(503) on network failure."""
    jwks_url = f"{_get_keycloak_url()}/realms/{KEYCLOAK_REALM}/protocol/openid-connect/certs"
    try:
        async with httpx.AsyncClient(verify=False, timeout=5.0) as client:  # nosec B501  # noqa: S501
            resp = await client.get(jwks_url)
            resp.raise_for_status()
            return resp.json()
    except httpx.HTTPError as exc:
        logger.warning("jwks_fetch_failed", url=jwks_url, exc_info=True)
        raise HTTPException(status_code=503, detail="Authentication service unavailable") from exc


async def _get_jwks(*, force_refresh: bool = False) -> dict:
    """Return JWKS, refetching when the cache is empty, stale, or forced.

    Refresh triggers:
      * `force_refresh=True` — caller observed an unknown `kid`
      * cache empty (`_jwks_cache is None`)
      * cache age > _JWKS_TTL_SECONDS (default 10 min)
    """
    global _jwks_cache, _jwks_cache_ts  # noqa: PLW0603
    now = time.monotonic()
    stale = (now - _jwks_cache_ts) > _JWKS_TTL_SECONDS
    if force_refresh or _jwks_cache is None or stale:
        _jwks_cache = await _fetch_jwks()
        _jwks_cache_ts = now
    return _jwks_cache


def secure_apps_issuer() -> str:
    """The ONE issuer a Bearer token on this service may carry.

    ``KEYCLOAK_ISSUER`` wins when set; else ``{KEYCLOAK_URL}/realms/{KEYCLOAK_REALM}``.
    Keycloak pins ``iss`` to its ``KC_HOSTNAME`` (https://auth.nonagentic.ai) even
    for tokens minted via the in-cluster URL, so every caller -- whichever token
    endpoint it uses -- presents ``https://auth.nonagentic.ai/realms/secure-apps``.

    Before this pin the realm boundary held ONLY because the JWKS is fetched from
    the secure-apps realm and Keycloak realms have disjoint keys; ``iss`` was never
    read. Pinning it makes the boundary explicit instead of incidental.
    """
    explicit = os.getenv("KEYCLOAK_ISSUER", "").strip().rstrip("/")
    if explicit:
        return explicit
    return f"{_get_keycloak_url().rstrip('/')}/realms/{KEYCLOAK_REALM}"


def _verify_with_jwks(token: str, jwks: dict) -> dict:
    """Verify ``token`` against ``jwks``. Raises _UnknownKidError if kid not present.

    Contract: RS256 only (no ``none``/HMAC), signing keys only (an ``enc`` key in
    the realm JWKS is never a verification key), ``iss`` pinned to
    :func:`secure_apps_issuer`, and ``exp``/``iss``/``sub`` REQUIRED -- a token
    without ``exp`` would otherwise never expire. ``aud`` is not bound: role
    membership (checked by every caller of this function) is the authorisation,
    and each role is granted per service account in the secure-apps realm.
    Header ``jku``/``x5u`` are ignored -- the JWKS URL is pinned config, never
    taken from the token.
    """
    jwk_set = jwt.PyJWKSet.from_dict(jwks)
    unverified = jwt.get_unverified_header(token)
    kid = unverified.get("kid")
    signing_key = next(
        (k for k in jwk_set.keys if k.key_id == kid and k.public_key_use != "enc"),
        None,
    )
    if signing_key is None:
        raise _UnknownKidError(kid)
    return jwt.decode(
        token,
        signing_key.key,
        algorithms=["RS256"],
        issuer=secure_apps_issuer(),
        options={
            "verify_signature": True,
            "verify_exp": True,
            "verify_iss": True,
            "verify_aud": False,
            "require": ["exp", "iss", "sub"],
        },
    )


#: The subject a dev-mode bypass hands back. It MUST be a real tenant uuid,
#: never the readable label "dev": `require_tenant` resolves a JWT's `sub`
#: through canonical_tenant_id(), and since tenant_id became a uuid column an
#: unresolvable label 400s every route that depends on it instead of producing
#: a junk row. Both bypass paths below use this — patching only one of them is
#: how /internal kept returning "dev" after the dashboard path was fixed.
DEV_SUBJECT_TENANT_ID = derived_tenant_id("dev")


async def _decode_bearer_token(request: Request) -> dict:
    """Decode and signature-verify a Bearer JWT from the Authorization header.

    Does NOT enforce any role — callers layer their own role check on top.
    """
    if not DASHBOARD_AUTH_ENABLED:
        return {
            "sub": DEV_SUBJECT_TENANT_ID,
            "roles": [REQUIRED_ROLE, "billing-internal"],
            "realm_access": {"roles": [REQUIRED_ROLE, "billing-internal"]},
        }

    auth_header = request.headers.get("authorization", "")
    if not auth_header.lower().startswith("bearer "):
        raise HTTPException(status_code=401, detail="Missing bearer token")

    token = auth_header[7:]

    try:
        jwks = await _get_jwks()
        try:
            payload = _verify_with_jwks(token, jwks)
        except _UnknownKidError as e:
            # Cached JWKS may be stale after a Keycloak signing-key rotation.
            # Refetch ONCE and retry; if still missing, surface 401 below.
            logger.info(
                "jwks_kid_miss_refetching",
                kid=str(e),
                reason="kid not in cached JWKS — possible rotation",
            )
            jwks = await _get_jwks(force_refresh=True)
            try:
                payload = _verify_with_jwks(token, jwks)
            except _UnknownKidError as e2:
                logger.warning(
                    "jwks_kid_miss_after_refresh",
                    kid=str(e2),
                    reason="kid still unknown after refetch — token signed by foreign key",
                )
                raise HTTPException(status_code=401, detail=f"Unknown signing key kid={e2}") from e2
    except jwt.ExpiredSignatureError as exc:
        raise HTTPException(status_code=401, detail="Token expired") from exc
    except (jwt.InvalidTokenError, ValueError) as e:
        raise HTTPException(status_code=401, detail=f"Invalid token: {e}") from e

    return payload


async def verify_admin_token(request: Request) -> dict:
    """Verify JWT from Authorization header has license-admin role.

    Used as a FastAPI dependency on dashboard endpoints.
    """
    if not DASHBOARD_AUTH_ENABLED:
        # See DEV_SUBJECT_TENANT_ID: a readable label here 400s every route.
        return {"sub": DEV_SUBJECT_TENANT_ID, "roles": [REQUIRED_ROLE]}
    payload = await _decode_bearer_token(request)
    realm_roles = payload.get("realm_access", {}).get("roles", [])
    if REQUIRED_ROLE not in realm_roles:
        raise HTTPException(status_code=403, detail="Requires license-admin role")
    return payload


# --- forwarded portal-user token (REV F currency change) ---------------------
#
# A trusted relay (provisioning, acting for the owner in fr) forwards the
# owner's own portal-realm JWT in ``X-Forwarded-User-Authorization``. Same
# contract as licences' ``verify_portal_user_jwt``: signature against the
# PORTAL realm's JWKS, the issuer PINNED to that realm, ``exp`` required and
# enforced, RS256 only, a ``sub`` required. Any failure is a refusal -- there
# is no weaker fallback, and the dev bypass (DASHBOARD_AUTH_ENABLED=false)
# deliberately does NOT apply here. The token is never logged or stored.

FORWARDED_USER_AUTH_HEADER = "X-Forwarded-User-Authorization"
PORTAL_REALM = "portal"
#: Realm roles on a VERIFIED forwarded portal token that carry the owner capability.
PORTAL_OWNER_ROLES = frozenset({"super_admin", "platform-admin"})

_portal_jwks_cache: dict | None = None
_portal_jwks_cache_ts: float = 0.0


class ForwardedTokenError(Exception):
    """The forwarded portal token could not be verified (never carries the token)."""


def portal_issuer() -> str:
    """The ONE issuer a forwarded owner token may carry.

    ``PORTAL_JWT_ISSUER`` wins when set; else ``{KEYCLOAK_URL}/realms/portal``
    (billing's KEYCLOAK_URL is the public URL precisely so it matches ``iss``).
    """
    explicit = os.getenv("PORTAL_JWT_ISSUER", "").strip().rstrip("/")
    if explicit:
        return explicit
    return f"{_get_keycloak_url().rstrip('/')}/realms/{PORTAL_REALM}"


async def _fetch_portal_jwks() -> dict:
    url = f"{portal_issuer()}/protocol/openid-connect/certs"
    try:
        async with httpx.AsyncClient(verify=False, timeout=5.0) as client:  # nosec B501  # noqa: S501
            resp = await client.get(url)
            resp.raise_for_status()
            return resp.json()
    except httpx.HTTPError as exc:
        logger.warning("portal_jwks_fetch_failed", url=url)
        raise HTTPException(status_code=503, detail="Authentication service unavailable") from exc


async def _get_portal_jwks(*, force_refresh: bool = False) -> dict:
    global _portal_jwks_cache, _portal_jwks_cache_ts  # noqa: PLW0603
    now = time.monotonic()
    stale = (now - _portal_jwks_cache_ts) > _JWKS_TTL_SECONDS
    if force_refresh or _portal_jwks_cache is None or stale:
        _portal_jwks_cache = await _fetch_portal_jwks()
        _portal_jwks_cache_ts = now
    return _portal_jwks_cache


def reset_portal_jwks_cache() -> None:
    global _portal_jwks_cache, _portal_jwks_cache_ts  # noqa: PLW0603
    _portal_jwks_cache = None
    _portal_jwks_cache_ts = 0.0


def _verify_portal(token: str, jwks: dict) -> dict:
    jwk_set = jwt.PyJWKSet.from_dict(jwks)
    kid = jwt.get_unverified_header(token).get("kid")
    signing_key = next((k for k in jwk_set.keys if k.key_id == kid), None)
    if signing_key is None:
        raise _UnknownKidError(kid)
    return jwt.decode(
        token,
        signing_key.key,
        algorithms=["RS256"],
        issuer=portal_issuer(),
        options={
            "verify_signature": True,
            "verify_exp": True,
            "verify_iss": True,
            # Same as licences: `aud` varies per portal client; not bound here.
            # The owner relay binds `azp` instead (auth_deps._verified_forwarded_owner).
            "verify_aud": False,
            "require": ["exp", "iss", "sub"],
        },
    )


async def verify_forwarded_portal_token(header_value: str | None) -> dict:
    """Verify ``X-Forwarded-User-Authorization: Bearer <portal JWT>``; claims or raise.

    Raises :class:`ForwardedTokenError` on absence, a non-Bearer value, a bad
    signature, an unknown key, a wrong issuer, expiry or a missing ``sub``.
    """
    if not header_value:
        raise ForwardedTokenError("missing")
    scheme, _, token = header_value.strip().partition(" ")
    token = token.strip()
    if scheme.lower() != "bearer" or not token:
        raise ForwardedTokenError("not_bearer")
    try:
        try:
            claims = _verify_portal(token, await _get_portal_jwks())
        except _UnknownKidError:
            claims = _verify_portal(token, await _get_portal_jwks(force_refresh=True))
    except _UnknownKidError as exc:
        raise ForwardedTokenError("unknown_kid") from exc
    except jwt.ExpiredSignatureError as exc:
        raise ForwardedTokenError("expired") from exc
    except (jwt.InvalidTokenError, ValueError) as exc:
        raise ForwardedTokenError(type(exc).__name__) from exc
    return claims
