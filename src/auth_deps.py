"""Auth dependencies for billing-service endpoints.

Five access levels:
- require_admin: Keycloak JWT with license-admin role
- require_pricing_admin: Keycloak JWT with license-admin OR billing-internal —
  the provider-pricing seed surface plus the money-config READ routes
- require_pricing_writer: Keycloak JWT with license-admin OR pricing-writer —
  the money-config WRITE routes (markup rules, access charges). Deliberately a
  DIFFERENT, narrower set than require_pricing_admin: billing-internal must not
  be able to change what a customer is billed.
- require_tenant: X-Tenant-Id header from ext_authz OR admin JWT
- require_internal: Keycloak service-account JWT with any internal SA role
  (billing-internal, onboarding-internal, license-admin)

Each dep stashes ``request.state.audit_actor = {"sub": ..., "role": ...}``
as a side effect so downstream audit logging can read who performed the
action without re-decoding the JWT. The stash is purely additive — return
values are unchanged.
"""

import uuid

import structlog
from fastapi import HTTPException, Request

from auth import (
    FORWARDED_USER_AUTH_HEADER,
    PORTAL_OWNER_ROLES,
    ForwardedTokenError,
    _decode_bearer_token,
    verify_admin_token,
    verify_forwarded_portal_token,
)
from settings import get_settings
from tenancy import canonical_tenant_id, redact_tenant_id

logger = structlog.stdlib.get_logger(__name__)

# Mirrors licenses repo `auth/dependencies.py:INTERNAL_SERVICE_ROLES` —
# any internal SA caller (licenses, onboarding, admin) is accepted on
# require_internal-protected endpoints. The licenses/n8n SAs mint a
# `billing-internal` SA token to provision budgets; require_admin still gates
# license-admin specifically. (metering-internal was retired here 2026-07-16
# after the secure-apps SAs rotated to billing-internal — Layer 2 step 3.)
INTERNAL_SERVICE_ROLES = {
    "billing-internal",
    "onboarding-internal",
    "license-admin",
}

# Preferred-role precedence inside require_internal: pick the most specific
# match first. license-admin wins over billing-internal wins over
# onboarding-internal so an admin SA token is recorded as 'license-admin'.
_INTERNAL_ROLE_PRECEDENCE = (
    "license-admin",
    "billing-internal",
    "onboarding-internal",
)


def _stash_actor(request: Request, *, sub: str | None, role: str | None) -> None:
    """Side-effect: record actor identity on request.state for audit logging."""
    request.state.audit_actor = {"sub": sub, "role": role}


async def require_admin(request: Request) -> dict:
    """Require Keycloak JWT with license-admin role."""
    payload = await verify_admin_token(request)
    _stash_actor(request, sub=payload.get("sub"), role="license-admin")
    return payload


# Pricing routes accept license-admin (humans + admin SAs) AND billing-internal
# (the billing-service SA, used by the pricing-seed CronJob). Onboarding-
# internal is intentionally NOT in this set — pricing is billing's own data,
# not onboarding's. (metering-internal retired 2026-07-16, Layer 2 step 3 —
# the billing-service SA now presents billing-internal.) Burned 2026-05-09:
# cronjob shipped with SERVICE_CLIENT_ID that had only metering-internal, so
# /provider-pricing guarded by require_admin returned 403 every night and
# pricing seeds never landed (cost_usd was NULL on claude rows for weeks).
_PRICING_ADMIN_ROLES = {"license-admin", "billing-internal"}


async def require_pricing_admin(request: Request) -> dict:
    """Require Keycloak JWT with license-admin OR billing-internal role.

    Used on /api/config/provider-pricing — the billing-service service
    account self-manages its pricing seed via the daily CronJob.
    """
    payload = await _decode_bearer_token(request)
    realm_roles = set(payload.get("realm_access", {}).get("roles", []))
    top_roles = set(payload.get("roles", []))
    matched = (realm_roles | top_roles) & _PRICING_ADMIN_ROLES
    if not matched:
        raise HTTPException(
            status_code=403,
            detail="Requires license-admin or billing-internal role",
        )
    chosen_role = "license-admin" if "license-admin" in matched else "billing-internal"
    _stash_actor(request, sub=payload.get("sub"), role=chosen_role)
    return payload


# Money-config WRITES — POST /api/config/markup-rules and /api/config/access-
# charges — set the multipliers and fees a CUSTOMER is billed on. #55 (2026-07-24)
# deliberately moved them off require_pricing_admin precisely so the broad
# `billing-internal` SA role could not rewrite billed money, and
# tests/test_markup_routes.py::TestMoneyConfigRequiresLicenseAdmin is the
# mutation guard for that decision. That decision stands and is NOT reversed here.
#
# What #55 missed is that these two POSTs have exactly one production caller: the
# licences service, provisioning a tenant offering (licenses/main.py, POST
# /tenants/{t}/offerings/{app}/{code}), reached from the fr super-admin
# "provision offering" button. Its SA `service-account-licenses` (realm
# secure-apps) holds service-internal + billing-internal and no license-admin, so
# every provision has 403'd on the billing hop since #55 — proven at the wire
# against deployed billing with that SA's own token.
#
# The fix is a dedicated, single-purpose realm role rather than re-widening an
# existing one: `pricing-writer` names exactly one capability (write billed money
# config) and can be granted to that one service account without opening any other
# require_admin route in this service. license-admin keeps the capability so human
# operators and the admin SAs are unaffected.
_PRICING_WRITER_ROLES = {"license-admin", "pricing-writer"}

# Precedence for the audit actor_role stamp: an operator token that happens to
# carry both is recorded as license-admin, the more privileged identity.
_PRICING_WRITER_PRECEDENCE = ("license-admin", "pricing-writer")


async def require_pricing_writer(request: Request) -> dict:
    """Require Keycloak JWT with license-admin OR pricing-writer role.

    Gates the money-config WRITE routes only. ``billing-internal`` alone is NOT
    sufficient here (that is the whole point of the split from
    :func:`require_pricing_admin`) — a service account that may seed provider
    cost must not thereby be able to change what a customer is charged.

    Reads ``realm_access.roles`` ONLY. The sibling deps in this module also union
    in a flat top-level ``roles`` claim, but ``require_admin`` ->
    ``verify_admin_token`` (auth.py) does not — so honouring it here would give
    these two money-writes a strictly LARGER claim surface than the
    ``require_admin`` gate they moved off, which is the opposite of this change's
    intent. Nothing in the ``secure-apps`` realm emits a flat ``roles`` claim
    today (no such protocol mapper exists), so this is dead code we decline to
    inherit rather than a behaviour change; anyone with ``manage-clients`` could
    add such a mapper, and this gate must not silently widen when they do. The
    flat-claim convention is left untouched in require_pricing_admin /
    require_internal — narrowing those is a separate, wider decision.
    """
    payload = await _decode_bearer_token(request)
    realm_roles = set(payload.get("realm_access", {}).get("roles", []))
    matched = realm_roles & _PRICING_WRITER_ROLES
    if not matched:
        raise HTTPException(
            status_code=403,
            detail="Requires license-admin or pricing-writer role",
        )
    chosen_role = next(r for r in _PRICING_WRITER_PRECEDENCE if r in matched)
    _stash_actor(request, sub=payload.get("sub"), role=chosen_role)
    return payload


async def require_tenant(request: Request) -> str:
    """Require trusted tenant context, resolved to a canonical tenant UUID.

    Accepts X-Tenant-Id header (from ext_authz) or admin JWT. The return value is
    ALWAYS a canonical lowercase uuid string — since migration 0011 every
    ``tenant_id`` column is a Postgres ``uuid``, and this dependency's return
    value is used verbatim as a WHERE value and as an INSERT value (e.g.
    ``routers/webhooks.py``). Letting an arbitrary string through means the
    caller is told ``503 DATABASE_UNAVAILABLE`` for their own bad input.

    Unresolvable input is a **400**, deliberately, not a 401 and not a 500:
    the caller authenticated fine (ext_authz already validated the license token
    upstream, or the admin JWT verified) — what is wrong is the identifier they
    supplied. A 401 would tell them to re-authenticate, which cannot help.
    """
    raw_header = request.headers.get("x-tenant-id")
    if raw_header is not None and not raw_header.strip():
        # PRESENT-BUT-EMPTY is a caller error, not "no header". Falling through
        # to the admin-JWT branch here would resolve the CALLER'S OWN subject —
        # a Keycloak service-account `sub` is a uuid, so it resolves fine — and
        # silently attribute the request to the service account as if it were a
        # tenant. That is the `__admin__` pseudo-tenant defect this change set
        # deletes, re-entering through a different door. `license-authz` sends
        # exactly this: `tenant_id = result.tenant_id or ""` (main.py) when
        # introspection returns no tenant.
        logger.warning(
            "empty_tenant_header",
            path=request.url.path,
            client_host=request.client.host if request.client else None,
            user_agent=request.headers.get("user-agent", ""),
        )
        raise HTTPException(status_code=400, detail="Invalid X-Tenant-Id (must be a tenant UUID)")
    if raw_header:
        # Customer call: ext_authz already validated the license token; we
        # don't have a sub here, so fall back to the tenant_id as the actor.
        tenant_id = canonical_tenant_id(raw_header)
        if tenant_id is None:
            # Redacted: this header has been observed carrying a raw
            # ``Bearer <jwt>`` in production (4 rows), so it is potentially a
            # secret and must not be logged verbatim or echoed to the caller.
            logger.warning(
                "invalid_tenant_header",
                raw_tenant_id=redact_tenant_id(raw_header),
                path=request.url.path,
                client_host=request.client.host if request.client else None,
                user_agent=request.headers.get("user-agent", ""),
            )
            raise HTTPException(
                status_code=400, detail="Invalid X-Tenant-Id (must be a tenant UUID)"
            )
        _stash_actor(request, sub=tenant_id, role="tenant")
        return tenant_id
    try:
        payload = await verify_admin_token(request)
    except HTTPException as exc:
        raise HTTPException(
            status_code=401, detail="Tenant context required (X-Tenant-Id header or admin JWT)"
        ) from exc
    # NOTE: the ``payload.get("sub", "__admin__")`` fallback that used to live
    # here is deleted. That literal was INSERTed straight into
    # ``tenant_webhooks.tenant_id`` (routers/webhooks.py) and used as a WHERE
    # value — it minted a pseudo-tenant that owns real rows. An admin JWT whose
    # subject is not a tenant uuid does not identify a tenant at all; such a
    # caller must name the tenant explicitly via X-Tenant-Id.
    sub = payload.get("sub")
    _stash_actor(request, sub=sub, role="license-admin")
    tenant_id = canonical_tenant_id(sub)
    if tenant_id is None:
        logger.warning(
            "admin_jwt_subject_is_not_a_tenant",
            sub=redact_tenant_id(sub),
            path=request.url.path,
        )
        raise HTTPException(
            status_code=400,
            detail=(
                "Admin JWT subject is not a tenant UUID; "
                "supply X-Tenant-Id to select the tenant to act on"
            ),
        )
    return tenant_id


def get_tenant_name(request: Request) -> str:
    """Extract the human-readable tenant name from the X-Tenant-Name header.

    Returns empty string when absent — callers that need the name for display
    use it as a best-effort label. For strict enforcement use require_tenant.
    Phase-2 cutover will make this header mandatory once license-authz injects
    it on every authenticated request.
    """
    return request.headers.get("x-tenant-name", "")


async def get_subject_id(request: Request) -> uuid.UUID:
    """Extract subject_id (end-user identity) from the X-Subject-Id header.

    Used by self-service ``/api/usage/me`` and ``/api/costs/me`` endpoints —
    license-authz injects the header from the introspect response. Raises 401
    if the header is missing or not a valid UUID, since these endpoints have
    no other notion of "the caller".
    """
    raw = request.headers.get("x-subject-id")
    if not raw:
        raise HTTPException(
            status_code=401,
            detail="Subject context required (X-Subject-Id header)",
        )
    try:
        return uuid.UUID(raw)
    except ValueError as exc:
        raise HTTPException(
            status_code=401,
            detail="Invalid X-Subject-Id header (must be a UUID)",
        ) from exc


def resolve_tenant_or_400(raw: object, *, field: str = "tenant_id") -> str:
    """Resolve a tenant identifier that did NOT arrive in the X-Tenant-Id header.

    ``require_tenant`` covers the header. This covers everywhere else a tenant id
    reaches a ``uuid`` column: a PATH parameter, a value read out of another
    service's JSON, a slug parsed back out of a legacy Redis key. Those sites had
    no resolver at all, so a registered tenant NAME — which the header accepts —
    became an opaque ``DataError`` from Postgres instead of a stated refusal.

    Raises the SAME 400 shape as the header path so a caller cannot tell which
    part of the request carried the bad identifier, and the value is never echoed:
    a tenant field has been observed carrying a raw bearer token in production.
    """
    resolved = canonical_tenant_id(raw)
    if resolved is None:
        logger.warning(
            "invalid_tenant_identifier",
            field=field,
            raw_tenant_id=redact_tenant_id(raw),
        )
        raise HTTPException(
            status_code=400,
            detail=f"Invalid {field} (must be a tenant UUID or a registered tenant name)",
        )
    return resolved


def enforce_tenant_isolation(request: Request, path_tenant_id: str, auth_tenant: str) -> None:
    """Raise 403 if a non-admin caller tries to access another tenant's data.

    When X-Tenant-Id header is present, the caller is a customer and must only
    access their own data.  When absent, the caller authenticated via admin JWT
    and may access any tenant.

    Both sides are canonicalised before comparison so ``ABC-…`` and ``abc-…``
    (and the braced / dashless spellings) are recognised as the SAME tenant — a
    raw string compare would 403 a caller who spelled their own uuid in upper
    case. A value that does not resolve falls back to its literal form rather
    than to ``None``, so two unresolvable-but-different strings still mismatch:
    collapsing both to ``None`` would make every pair of junk values compare
    equal and turn this isolation check into a hole.
    """
    if not request.headers.get("x-tenant-id"):
        return
    path_canon = canonical_tenant_id(path_tenant_id) or str(path_tenant_id)
    auth_canon = canonical_tenant_id(auth_tenant) or str(auth_tenant)
    if path_canon != auth_canon:
        raise HTTPException(status_code=403, detail="Tenant mismatch")


async def require_internal(request: Request) -> dict:
    """Require Keycloak service-account JWT with billing-internal (or license-admin) role.

    Decodes the bearer token without enforcing license-admin — internal callers
    (e.g., licenses → billing budget provisioning) carry billing-internal
    instead. Use require_admin when license-admin specifically is needed.
    """
    payload = await _decode_bearer_token(request)
    # Check realm_access.roles (production Keycloak tokens) and top-level roles (dev bypass)
    realm_roles = set(payload.get("realm_access", {}).get("roles", []))
    top_roles = set(payload.get("roles", []))
    all_roles = realm_roles | top_roles
    matched = all_roles & INTERNAL_SERVICE_ROLES
    if not matched:
        raise HTTPException(status_code=403, detail="Internal service role required")
    # Pick the most specific role for audit purposes.
    chosen_role = next((r for r in _INTERNAL_ROLE_PRECEDENCE if r in matched), None)
    _stash_actor(request, sub=payload.get("sub"), role=chosen_role)
    return payload


async def require_service_caller(request: Request) -> dict:
    """Require a VERIFIED Keycloak service-account JWT — never a tenant licence token.

    ⛔ WHAT THIS EXISTS TO REFUSE, STATED FIRST. ``require_tenant`` accepts the
    ``X-Tenant-Id`` header that Istio's ext_authz / license-authz injects after
    validating a LICENCE token, and every member of a tenant holds one. Pairing
    it with :func:`enforce_tenant_isolation` therefore proves only "this caller
    belongs to the tenant whose data they asked for" — which is the correct gate
    for a tenant-aggregate read and the WRONG one for a PER-MEMBER breakdown.
    Per-actor cost names who inside the workspace spent what; it is admin data,
    and under ``require_tenant`` any seat could read their colleagues' spend.

    ⭐ THE ESTABLISHED PATH IS REUSED, NOT REBUILT. This is
    :func:`require_internal` — the same ``INTERNAL_SERVICE_ROLES`` set, the same
    ``_decode_bearer_token`` verification, the same audit-actor stash — with ONE
    added behaviour: the failure SHAPE. CP's backend proxies this route for its
    own tenant admins, so it presents an internal SA token AND, legitimately,
    ``X-Tenant-Id`` naming the tenant it is proxying for. A caller that presents
    the header but cannot produce an internal SA token is exactly the licence-
    token member this gate exists to stop, and they get a **403** that says so
    rather than a bare 401 telling them to re-authenticate — which they cannot
    usefully do, because no licence token will ever satisfy this route.

    ⚠️ THE HEADER IS NOT THE CREDENTIAL AND IS NEVER TREATED AS ONE. It is read
    only to choose between two REFUSALS; it can never turn a refusal into an
    acceptance. Reversing that — "no header, so treat the caller as a service" —
    is the ``__admin__`` pseudo-tenant defect ``require_tenant`` above documents,
    inverted, and would make the gate openable by DELETING a header.

    ⚠️ HONEST RESIDUAL: a genuine service caller whose SA token has merely
    EXPIRED, and which also sends ``X-Tenant-Id``, is told 403 where 401 would be
    more precise. Accepted deliberately — the alternative is to inspect the
    decode failure's cause and branch on it, which makes the gate's verdict
    depend on distinguishing token-shape errors, and the expensive direction of
    that mistake is admitting a caller. The detail string names both cases so the
    operator is not left guessing.
    """
    try:
        return await require_internal(request)
    except HTTPException:
        if request.headers.get("x-tenant-id"):
            logger.warning(
                "service_caller_required_tenant_token_refused",
                path=request.url.path,
                client_host=request.client.host if request.client else None,
            )
            raise HTTPException(
                status_code=403,
                detail=(
                    "Per-member cost is admin data: this endpoint requires a verified "
                    "internal service-account token, which a tenant licence token is "
                    "not. (A service caller seeing this should check its own token has "
                    "not expired.)"
                ),
            ) from None
        raise


#: Service accounts that may read ``GET /api/usage/breakdown`` for the ONE tenant they
#: proxy for (``X-Tenant-Id``) WITHOUT holding an internal billing role. Least privilege:
#: an app that only DISPLAYS its tenant's usage must not be handed ``billing-internal``,
#: which also opens pricing reads and every ``require_internal`` write. Matched on the
#: verified token's ``azp`` AND the Keycloak service-account username
#: ``service-account-<azp>``, which only a ``client_credentials`` grant for that client
#: can carry (a human logging in through the client has their own username).
#: The realm is pinned by signature: ``_decode_bearer_token`` verifies against the
#: secure-apps JWKS only.
USAGE_BREAKDOWN_READER_CLIENTS: frozenset[str] = frozenset(
    {
        # raisemytickets analytics_app /ai-usage (SERVICE_ACTOR_ID 105f8bfa-...).
        "analytics-app-service-account",
    }
)


def _allowlisted_usage_reader(payload: dict) -> str | None:
    azp = payload.get("azp") or payload.get("client_id")
    if not isinstance(azp, str) or azp not in USAGE_BREAKDOWN_READER_CLIENTS:
        return None
    if payload.get("preferred_username") != f"service-account-{azp}":
        return None
    return azp


async def require_usage_breakdown_reader(request: Request) -> dict:
    """:func:`require_service_caller`, plus allow-listed per-tenant reader SAs.

    An allow-listed reader (:data:`USAGE_BREAKDOWN_READER_CLIENTS`) is accepted ONLY
    with ``X-Tenant-Id``, so the handler's ``enforce_tenant_isolation`` pins it to that
    one tenant; without the header it is refused 403 -- it can never take the
    platform-token "any tenant" path. Every other caller goes through
    :func:`require_service_caller` unchanged (same refusals, same shapes).
    """
    try:
        payload: dict | None = await _decode_bearer_token(request)
    except HTTPException:
        payload = None
    azp = _allowlisted_usage_reader(payload) if payload is not None else None
    if azp is not None:
        # Roles still win: an allow-listed SA that ALSO holds an internal role is
        # treated as the internal caller it is.
        roles = set((payload or {}).get("realm_access", {}).get("roles", []))
        if not roles & INTERNAL_SERVICE_ROLES:
            if not request.headers.get("x-tenant-id"):
                raise HTTPException(
                    status_code=403,
                    detail="usage reader service accounts must pin X-Tenant-Id",
                )
            _stash_actor(request, sub=(payload or {}).get("sub"), role=f"usage-reader:{azp}")
            return payload or {}
    return await require_service_caller(request)


async def require_internal_or_secret(request: Request) -> dict | None:
    """Accept EITHER the shared X-Internal-Secret OR a Keycloak internal SA JWT.

    Used on the exec-quality READ endpoints, which now have two caller classes:
    n8n internal workflows (the prompt-prefetch / kaizen activities), which
    authenticate with the shared ``X-Internal-Secret`` header — the same gate
    the exec-quality WRITE + watchdog recorders already trust — and
    model-evaluator's eval-set sampler, which carries an internal SA JWT.

    The shared-secret path is checked first so n8n callers never touch keycloak
    JWKS; the SA-JWT fallback (``require_internal``) runs only when no
    X-Internal-Secret header is present. A present-but-wrong secret is rejected
    outright (no silent fall-through), so a misconfigured caller is not masked.
    """
    secret = request.headers.get("x-internal-secret")
    if secret is not None:
        expected = get_settings().internal_secret
        if expected and secret == expected:
            _stash_actor(request, sub="n8n-internal", role="internal-secret")
            return None
        raise HTTPException(status_code=401, detail="internal secret mismatch")
    return await require_internal(request)


#: SA roles that may RELAY an owner's currency change (REV F). Realm roles only.
_OWNER_RELAY_SA_ROLES = frozenset({"onboarding-internal", "license-admin"})


async def require_owner_via_service(request: Request) -> dict:
    """An internal SA relaying the OWNER's verified portal token (REV F currency change).

    BOTH must hold, there is no weaker fallback:

    1. ``Authorization`` -- a signature-verified token whose ``realm_access.roles``
       include ``onboarding-internal`` (the role provisioning's
       ``provisioning-billing`` SA already uses for the billing money calls) or
       ``license-admin``. ``billing-internal`` alone is NOT enough: a currency
       change rewrites billed money and has exactly one relaying caller.
    2. ``X-Forwarded-User-Authorization: Bearer <portal JWT>`` -- verified against
       the PORTAL realm (signature, pinned issuer, expiry, sub), minted for an
       allow-listed portal client (``azp`` in ``FORWARDED_OWNER_ALLOWED_AZP``,
       default ``fr-frontend``) and carrying the realm role ``super_admin`` or
       ``platform-admin``.

    Missing / unverifiable forwarded token -> 401; verified but not an owner -> 403.
    The audit actor becomes the OWNER (``sub``, matched role); the relaying SA's
    ``azp`` is returned as ``via``. The token is never logged, echoed or stored.
    """
    service = await _decode_bearer_token(request)
    sa_roles = set((service.get("realm_access") or {}).get("roles") or [])
    if not sa_roles & _OWNER_RELAY_SA_ROLES:
        raise HTTPException(
            status_code=403,
            detail={
                "reason": "relay_role_required",
                "message": "Requires onboarding-internal or license-admin on the caller SA",
            },
        )
    owner_sub, owner_role = await _verified_forwarded_owner(request)
    via = service.get("azp") or service.get("client_id") or service.get("sub")
    _stash_actor(request, sub=owner_sub, role=owner_role)
    return {"owner_sub": owner_sub, "owner_role": owner_role, "via": via}


async def _verified_forwarded_owner(request: Request) -> tuple[str, str]:
    """Verify ``X-Forwarded-User-Authorization`` as an OWNER portal token: (sub, role).

    The forwarded leg of :func:`require_owner_via_service`, shared so every route
    that records an owner relayed by a service account applies the SAME check:
    portal-realm signature / pinned issuer / expiry / ``sub``, an ``azp`` in
    ``FORWARDED_OWNER_ALLOWED_AZP`` (the portal client the owner signed in
    through -- fr's SPA; a super_admin token issued to any other portal client
    must not be relayable), plus ``super_admin`` or ``platform-admin``.
    Missing / unverifiable / foreign client -> 401; verified but not an owner
    -> 403. The token is never logged, echoed or stored.

    ``aud`` is deliberately not bound: Keycloak derives it from the client roles
    the USER holds (e.g. ``account``), not from the client that minted the
    token, so it does not identify the relaying client; ``azp`` does.
    """
    try:
        claims = await verify_forwarded_portal_token(
            request.headers.get(FORWARDED_USER_AUTH_HEADER)
        )
        azp = claims.get("azp")
        if not isinstance(azp, str) or azp not in get_settings().forwarded_owner_allowed_azps:
            # Only the client id is logged -- never the token.
            logger.warning(
                "forwarded_owner_token_client_refused",
                azp=azp if isinstance(azp, str) else None,
            )
            raise ForwardedTokenError("azp_not_allowed")
    except ForwardedTokenError as exc:
        logger.warning("forwarded_owner_token_refused", reason=str(exc))
        raise HTTPException(
            status_code=401,
            detail={
                "reason": "forwarded_user_token_invalid",
                "message": f"{FORWARDED_USER_AUTH_HEADER} missing or could not be verified",
            },
        ) from exc
    roles = set((claims.get("realm_access") or {}).get("roles") or [])
    matched = sorted(roles & PORTAL_OWNER_ROLES)
    if not matched:
        raise HTTPException(
            status_code=403,
            detail={
                "reason": "owner_role_required",
                "message": "forwarded portal user lacks super_admin / platform-admin",
            },
        )
    return str(claims["sub"]), matched[0]


async def adopt_forwarded_owner_actor(request: Request) -> str | None:
    """Record the relayed OWNER as the audit actor of a money-config write.

    Call ONLY after a pricing-writer gate (:func:`require_pricing_writer`) has
    authenticated the relaying caller -- the forwarded identity is trusted
    solely because that caller is. Same header and verification as
    :func:`require_owner_via_service` (fr relays its signed-in super_admin's
    portal JWT, exactly as it does for provisioning's inference apply):

    - header ABSENT -> ``None``; the actor stays the caller token's own ``sub``
      (an operator / SA writing for itself).
    - header PRESENT -> it must verify as a ``super_admin`` / ``platform-admin``
      portal token, else 401 / 403. A claimed-but-unverifiable human is a
      refusal, never a silent fall back to the SA -- a write must not be
      attributed to someone other than the caller says.

    On success the audit actor becomes the owner (``sub``, matched role), as in
    :func:`require_owner_via_service`. Returns the owner sub.
    """
    if request.headers.get(FORWARDED_USER_AUTH_HEADER) is None:
        return None
    owner_sub, owner_role = await _verified_forwarded_owner(request)
    _stash_actor(request, sub=owner_sub, role=owner_role)
    return owner_sub


#: Service accounts that may INVOICE a money-budget top-up (``POST
#: /api/budget/money/topup``) without holding ``license-admin``. Least privilege,
#: same matching rule as :data:`USAGE_BREAKDOWN_READER_CLIENTS`: the verified
#: token's ``azp`` AND the Keycloak service-account username
#: ``service-account-<azp>``, which only a ``client_credentials`` grant for that
#: client can carry. The realm is pinned by signature (secure-apps JWKS only).
MONEY_TOPUP_WRITER_CLIENTS: frozenset[str] = frozenset(
    {
        # CommonPractice backend (LICENSE_SA_CLIENT_ID in the commonpractice
        # Deployment). CP authorises its own tenant_admin before calling.
        "commonpractice-licenses-admin",
    }
)

#: Realm roles that may invoice a top-up for any tenant (operators + admin SAs).
#: ⛔ ``billing-internal`` / ``onboarding-internal`` are deliberately NOT here: a
#: top-up puts a charge on a customer's invoice, the same "who may change billed
#: money" line #55 drew for ``require_pricing_writer``.
_MONEY_TOPUP_WRITER_ROLES = frozenset({"license-admin"})


async def require_money_topup_writer(request: Request) -> dict:
    """A verified SA token that may invoice a money top-up: license-admin OR allow-listed.

    Returns the payload; stashes the audit actor as ``license-admin`` or
    ``topup-writer:<azp>``. Anything else -- a broad internal SA role, a tenant
    licence token (``X-Tenant-Id`` without a bearer), a human portal user -- is
    refused: 401 without a verifiable bearer, 403 with one that lacks the grant.
    Reads ``realm_access.roles`` only (no flat ``roles`` claim), like
    :func:`require_pricing_writer`.
    """
    payload = await _decode_bearer_token(request)
    roles = set((payload.get("realm_access") or {}).get("roles") or [])
    if roles & _MONEY_TOPUP_WRITER_ROLES:
        _stash_actor(request, sub=payload.get("sub"), role="license-admin")
        return payload
    azp = payload.get("azp") or payload.get("client_id")
    if (
        isinstance(azp, str)
        and azp in MONEY_TOPUP_WRITER_CLIENTS
        and payload.get("preferred_username") == f"service-account-{azp}"
    ):
        _stash_actor(request, sub=payload.get("sub"), role=f"topup-writer:{azp}")
        return payload
    raise HTTPException(
        status_code=403,
        detail={
            "reason": "topup_writer_required",
            "message": "Requires license-admin or an allow-listed top-up service account",
        },
    )
