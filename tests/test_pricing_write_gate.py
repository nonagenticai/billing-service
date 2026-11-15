"""Who may WRITE billed money config: `pricing-writer`, not `billing-internal`.

Background — two decisions that must both hold at once:

* billing #55 (2026-07-24) deliberately moved ``POST /api/config/markup-rules``
  and ``POST /api/config/access-charges`` off ``require_pricing_admin`` onto the
  strict ``require_admin``, specifically so the broad ``billing-internal`` SA
  role could not rewrite what a customer is billed.
  ``test_markup_routes.py::TestMoneyConfigRequiresLicenseAdmin`` is that
  decision's mutation guard and it is left untouched by this module.
* #55 also broke the only production caller of those two POSTs. The licences
  service provisions a tenant offering (``POST /tenants/{t}/offerings/{app}/
  {code}``, reached from the fr super-admin "provision offering" button) using
  ``service-account-licenses``, which holds ``service-internal`` +
  ``billing-internal`` and no ``license-admin``. Every provision has 403'd on
  the billing hop since — proven at the wire against deployed billing.

The reconciliation is a dedicated narrow realm role, ``pricing-writer``, that
names exactly one capability and is granted to exactly that one SA. So:

* ``license-admin``                 -> may write (unchanged)
* ``pricing-writer``                -> may write (new)
* ``billing-internal`` alone        -> 403 (#55's decision, preserved)
* anything else                     -> 403

``TestMoneyConfigWriteGateOverRealJwts`` proves the same table end-to-end over
genuinely RS256-signed tokens with only the JWKS *network fetch* substituted, so
it cannot pass by stubbing the auth dependency away.
"""

from __future__ import annotations

import time

import jwt
import pytest
from cryptography.hazmat.primitives.asymmetric import rsa
from fastapi.routing import iter_route_contexts
from fastapi.testclient import TestClient
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

import auth_deps
from models.db import AuditLog
from tests.conftest import T

# --- Principals -------------------------------------------------------------
# The realm-role set carried TODAY by the licences service account in the
# secure-apps realm (verified live with kcadm): no license-admin, no
# pricing-writer. This is the set that 403s in production right now.
LICENSES_SA_SUB = "service-account-licenses"
LICENSES_SA_ROLES_TODAY = ["default-roles-secure-apps", "service-internal", "billing-internal"]
# The set it will carry once `pricing-writer` is granted (the operator step that
# makes this fix live). Everything below pins the behaviour of THAT token.
LICENSES_SA_ROLES_GRANTED = [*LICENSES_SA_ROLES_TODAY, "pricing-writer"]

ADMIN_SUB = "service-account-fr-backend"
ADMIN_ROLES = ["default-roles-secure-apps", "service-internal", "license-admin"]

# Holds an internal role but neither write role — e.g. the onboarding SA.
OUTSIDER_SUB = "service-account-onboarding"
OUTSIDER_ROLES = ["default-roles-secure-apps", "service-internal", "onboarding-internal"]

MARKUP_PATH = "/api/config/markup-rules"
ACCESS_CHARGE_PATH = "/api/config/access-charges"
MARKUP_BODY = {"tenant_id": T("ws1-tenant"), "service_key": "fr", "markup_pct": "30"}
ACCESS_CHARGE_BODY = {
    "tenant_id": T("ws1-tenant"),
    "service_key": "fr",  # pragma: allowlist secret — product key, not a credential
    "amount_usd": "49",
    "cadence": "monthly",
}
AUTH_HEADERS = {"Authorization": "Bearer fake"}


def _install_principal(monkeypatch, *, sub: str, roles: list[str]) -> dict:
    """Substitute the bearer DECODE step with a fixed Keycloak-shaped payload.

    Signature verification is exercised separately (and for real) by
    :class:`TestMoneyConfigWriteGateOverRealJwts`; here we isolate the role gate,
    which still runs unmodified.
    """
    payload = {"sub": sub, "realm_access": {"roles": roles}}

    async def _fake_decode(_request):
        return payload

    import auth

    monkeypatch.setattr(auth, "DASHBOARD_AUTH_ENABLED", True)
    monkeypatch.setattr(auth, "_decode_bearer_token", _fake_decode)
    monkeypatch.setattr(auth_deps, "_decode_bearer_token", _fake_decode)
    return payload


@pytest.fixture
def licenses_sa_granted(monkeypatch) -> dict:
    """The production caller once `pricing-writer` is granted to its SA."""
    return _install_principal(monkeypatch, sub=LICENSES_SA_SUB, roles=LICENSES_SA_ROLES_GRANTED)


@pytest.fixture
def licenses_sa_today(monkeypatch) -> dict:
    """The production caller as it stands before the Keycloak grant."""
    return _install_principal(monkeypatch, sub=LICENSES_SA_SUB, roles=LICENSES_SA_ROLES_TODAY)


@pytest.fixture
def license_admin(monkeypatch) -> dict:
    return _install_principal(monkeypatch, sub=ADMIN_SUB, roles=ADMIN_ROLES)


@pytest.fixture
def outsider(monkeypatch) -> dict:
    return _install_principal(monkeypatch, sub=OUTSIDER_SUB, roles=OUTSIDER_ROLES)


async def _fetch_audit(db_engine, action: str) -> list[AuditLog]:
    factory = async_sessionmaker(bind=db_engine, class_=AsyncSession, expire_on_commit=False)
    async with factory() as s:
        stmt = select(AuditLog).where(AuditLog.action == action)
        return list((await s.execute(stmt)).scalars().all())


@pytest.mark.unit
class TestMarkupRuleWriteGate:
    async def test_pricing_writer_can_upsert_markup_rule(
        self, app_client: TestClient, db_engine, licenses_sa_granted
    ) -> None:
        """RED before the fix: 403 'Requires license-admin role'.

        This is the exact call licences makes while provisioning a priced
        offering — `_OFFERING_USAGE_FIELDS` carries `markup_pct`, and all four
        production offerings set it to 30.
        """
        resp = app_client.post(MARKUP_PATH, headers=AUTH_HEADERS, json=MARKUP_BODY)
        assert resp.status_code == 201, resp.text
        assert resp.json()["data"]["tenant_id"] == T("ws1-tenant")

        # The write is real and the audit trail names the SA under the role that
        # actually admitted it — not a fabricated 'license-admin'.
        audit = await _fetch_audit(db_engine, "config.markup_rule.upsert")
        assert len(audit) == 1
        assert audit[0].actor_sub == LICENSES_SA_SUB
        assert audit[0].actor_role == "pricing-writer"

    async def test_license_admin_can_still_upsert_markup_rule(
        self, app_client: TestClient, db_engine, license_admin
    ) -> None:
        resp = app_client.post(MARKUP_PATH, headers=AUTH_HEADERS, json=MARKUP_BODY)
        assert resp.status_code == 201, resp.text

        audit = await _fetch_audit(db_engine, "config.markup_rule.upsert")
        assert len(audit) == 1
        assert audit[0].actor_role == "license-admin"

    async def test_billing_internal_alone_is_still_rejected(
        self, app_client: TestClient, db_engine, licenses_sa_today
    ) -> None:
        """#55's decision, preserved: billing-internal may not write billed money.

        Also the honest statement of why the Keycloak grant is load-bearing —
        merging this PR alone changes nothing for the licences SA.
        """
        resp = app_client.post(MARKUP_PATH, headers=AUTH_HEADERS, json=MARKUP_BODY)
        assert resp.status_code == 403, resp.text
        assert await _fetch_audit(db_engine, "config.markup_rule.upsert") == []

    async def test_principal_with_no_write_role_is_rejected(
        self, app_client: TestClient, db_engine, outsider
    ) -> None:
        resp = app_client.post(MARKUP_PATH, headers=AUTH_HEADERS, json=MARKUP_BODY)
        assert resp.status_code == 403, resp.text
        assert await _fetch_audit(db_engine, "config.markup_rule.upsert") == []


@pytest.mark.unit
class TestAccessChargeWriteGate:
    async def test_pricing_writer_can_upsert_access_charge(
        self, app_client: TestClient, db_engine, licenses_sa_granted
    ) -> None:
        """RED before the fix: 403 'Requires license-admin role'."""
        resp = app_client.post(ACCESS_CHARGE_PATH, headers=AUTH_HEADERS, json=ACCESS_CHARGE_BODY)
        assert resp.status_code == 201, resp.text
        assert resp.json()["data"]["tenant_id"] == T("ws1-tenant")

        audit = await _fetch_audit(db_engine, "config.access_charge.upsert")
        assert len(audit) == 1
        assert audit[0].actor_sub == LICENSES_SA_SUB
        assert audit[0].actor_role == "pricing-writer"

    async def test_license_admin_can_still_upsert_access_charge(
        self, app_client: TestClient, db_engine, license_admin
    ) -> None:
        resp = app_client.post(ACCESS_CHARGE_PATH, headers=AUTH_HEADERS, json=ACCESS_CHARGE_BODY)
        assert resp.status_code == 201, resp.text

        audit = await _fetch_audit(db_engine, "config.access_charge.upsert")
        assert len(audit) == 1
        assert audit[0].actor_role == "license-admin"

    async def test_billing_internal_alone_is_still_rejected(
        self, app_client: TestClient, db_engine, licenses_sa_today
    ) -> None:
        resp = app_client.post(ACCESS_CHARGE_PATH, headers=AUTH_HEADERS, json=ACCESS_CHARGE_BODY)
        assert resp.status_code == 403, resp.text
        assert await _fetch_audit(db_engine, "config.access_charge.upsert") == []

    async def test_principal_with_no_write_role_is_rejected(
        self, app_client: TestClient, db_engine, outsider
    ) -> None:
        resp = app_client.post(ACCESS_CHARGE_PATH, headers=AUTH_HEADERS, json=ACCESS_CHARGE_BODY)
        assert resp.status_code == 403, resp.text
        assert await _fetch_audit(db_engine, "config.access_charge.upsert") == []


@pytest.mark.unit
class TestPricingWriteGateDoesNotWidenTheReadGate:
    """The write role must not become a skeleton key for the rest of billing.

    ``pricing-writer`` exists to unlock two POSTs. If it ever also satisfied the
    ordinary ``require_admin`` routes, the "narrower blast radius than granting
    license-admin" argument for this design would be false.
    """

    async def test_pricing_writer_cannot_write_tenant_markup_config(
        self, app_client: TestClient, db_engine, licenses_sa_granted
    ) -> None:
        resp = app_client.put(
            f"/api/config/tenants/{T('ws1-tenant')}",
            headers=AUTH_HEADERS,
            json={"markup_pct": 25},
        )
        assert resp.status_code == 403, resp.text

    async def test_pricing_writer_cannot_read_audit_log(
        self, app_client: TestClient, db_engine, licenses_sa_granted
    ) -> None:
        resp = app_client.get("/api/audit/recent", headers=AUTH_HEADERS)
        assert resp.status_code == 403, resp.text


# --- Real-JWT contract ------------------------------------------------------

_PRIVATE_KEY = rsa.generate_private_key(public_exponent=65537, key_size=2048)
_KID = "ws1-pricing-write-gate"


def _jwks() -> dict:
    """Public JWKS for the module keypair, in Keycloak's response shape."""
    import json

    from jwt.algorithms import RSAAlgorithm

    jwk = json.loads(RSAAlgorithm.to_jwk(_PRIVATE_KEY.public_key()))
    jwk.update({"kid": _KID, "alg": "RS256", "use": "sig"})
    return {"keys": [jwk]}


def _mint(sub: str, roles: list[str] | None = None, extra_claims: dict | None = None) -> str:
    """A genuinely RS256-signed Keycloak-shaped access token.

    ``roles`` populates ``realm_access.roles`` (the only place this gate looks).
    ``extra_claims`` is an explicit dict, NOT ``**kwargs`` — a test that places a
    role in a flat top-level ``roles`` claim would otherwise collide with the
    ``roles`` parameter and die with a TypeError, which would look like a failing
    test while asserting nothing at all.
    """
    now = int(time.time())
    claims: dict[str, object] = {
        "sub": sub,
        "iss": "https://auth.nonagentic.ai/realms/secure-apps",
        "azp": "billing-caller",
        "iat": now,
        "exp": now + 300,
    }
    if roles is not None:
        claims["realm_access"] = {"roles": roles}
    claims.update(extra_claims or {})
    return jwt.encode(claims, _PRIVATE_KEY, algorithm="RS256", headers={"kid": _KID})


@pytest.fixture
def real_jwt_auth(monkeypatch):
    """Enable real signature verification; stub ONLY the JWKS network fetch.

    Nothing here touches ``require_admin`` / ``require_pricing_writer`` or the
    routes' dependencies, so a test in this class cannot be made to pass by
    mocking the auth gate away — the whole decode + role-check path executes.
    """
    import auth

    async def _fake_fetch_jwks() -> dict:
        return _jwks()

    monkeypatch.setattr(auth, "DASHBOARD_AUTH_ENABLED", True)
    # _mint() stamps the production issuer; the gate now pins `iss`.
    monkeypatch.setenv("KEYCLOAK_ISSUER", "https://auth.nonagentic.ai/realms/secure-apps")
    monkeypatch.setattr(auth, "_fetch_jwks", _fake_fetch_jwks)
    monkeypatch.setattr(auth, "_jwks_cache", None)
    monkeypatch.setattr(auth, "_jwks_cache_ts", 0.0)


@pytest.mark.unit
class TestMoneyConfigWriteGateOverRealJwts:
    """End-to-end role contract for the money-config write surface.

    The table is the contract: exactly ``license-admin`` and ``pricing-writer``
    may write, and ``billing-internal`` may not. Both routes are asserted, so
    neither can drift alone.
    """

    @pytest.mark.parametrize("path,body", [(MARKUP_PATH, MARKUP_BODY)])
    @pytest.mark.parametrize(
        "roles,expected",
        [
            (["license-admin"], 201),
            (["pricing-writer"], 201),
            (["service-internal", "billing-internal", "pricing-writer"], 201),
            (["billing-internal"], 403),
            (["service-internal", "billing-internal"], 403),
            (["onboarding-internal"], 403),
            (["license-reader"], 403),
            ([], 403),
        ],
    )
    async def test_markup_rule_write_role_table(
        self,
        app_client: TestClient,
        db_engine,
        real_jwt_auth,
        roles: list[str],
        expected: int,
        path: str,
        body: dict,
    ) -> None:
        token = _mint("service-account-under-test", roles)
        resp = app_client.post(path, headers={"Authorization": f"Bearer {token}"}, json=body)
        assert resp.status_code == expected, resp.text

    @pytest.mark.parametrize(
        "roles,expected",
        [
            (["license-admin"], 201),
            (["pricing-writer"], 201),
            (["service-internal", "billing-internal", "pricing-writer"], 201),
            (["billing-internal"], 403),
            (["service-internal", "billing-internal"], 403),
            (["onboarding-internal"], 403),
            ([], 403),
        ],
    )
    async def test_access_charge_write_role_table(
        self,
        app_client: TestClient,
        db_engine,
        real_jwt_auth,
        roles: list[str],
        expected: int,
    ) -> None:
        token = _mint("service-account-under-test", roles)
        resp = app_client.post(
            ACCESS_CHARGE_PATH,
            headers={"Authorization": f"Bearer {token}"},
            json=ACCESS_CHARGE_BODY,
        )
        assert resp.status_code == expected, resp.text

    @pytest.mark.parametrize("path", [MARKUP_PATH, ACCESS_CHARGE_PATH])
    @pytest.mark.parametrize(
        "claims",
        [
            pytest.param({"roles": ["pricing-writer"]}, id="flat-roles-pricing-writer"),
            pytest.param({"roles": ["license-admin"]}, id="flat-roles-license-admin"),
            pytest.param(
                {"resource_access": {"billing-service": {"roles": ["pricing-writer"]}}},
                id="client-role-pricing-writer",
            ),
        ],
    )
    async def test_the_role_is_honoured_only_in_realm_access(
        self, app_client: TestClient, db_engine, real_jwt_auth, claims: dict, path: str
    ) -> None:
        """A write role counts ONLY from `realm_access.roles`.

        `require_admin` (the gate these two routes moved off) reads only
        `realm_access.roles`. If this gate also honoured a flat top-level `roles`
        claim or a client role, the money-writes would sit behind a *larger*
        claim surface than the one they left — the opposite of the intent, and a
        latent widening the moment anyone with `manage-clients` adds such a
        mapper. Pin it closed while no mapper emits these.
        """
        token = _mint("service-account-under-test", roles=None, extra_claims=claims)
        # Deliberately invalid body: if the gate ever admitted this token the
        # response would be 422, so a 403 can only have come from the gate.
        resp = app_client.post(
            path, headers={"Authorization": f"Bearer {token}"}, json={"bogus": 1}
        )
        assert resp.status_code == 403, resp.text

    async def test_an_unsigned_token_is_still_rejected(
        self, app_client: TestClient, db_engine, real_jwt_auth
    ) -> None:
        """Positive control for the harness itself.

        If this returned 201 the parametrised table above would be measuring a
        disabled verifier rather than the role gate.
        """
        forged = jwt.encode(
            {"sub": "attacker", "realm_access": {"roles": ["pricing-writer"]}},
            "not-the-signing-key",
            algorithm="HS256",
            headers={"kid": _KID},
        )
        resp = app_client.post(
            MARKUP_PATH, headers={"Authorization": f"Bearer {forged}"}, json=MARKUP_BODY
        )
        assert resp.status_code == 401, resp.text


@pytest.mark.unit
class TestConfigRouteGateInventory:
    """Pin the auth dependency on every /api/config route.

    #55 re-gated two routes and shipped green because no test asserted the
    mapping as a whole. This inventory makes any future re-gate — in either
    direction, on any config route — a deliberate, reviewed diff.
    """

    def test_config_route_gates_are_pinned(self) -> None:
        from main import app

        expected = {
            ("GET", "/api/config/providers"): "require_admin",
            ("PUT", "/api/config/providers/{provider}"): "require_admin",
            ("GET", "/api/config/apps"): "require_admin",
            ("PUT", "/api/config/apps/{app_key}"): "require_admin",
            ("GET", "/api/config/tenants/{tenant_id}"): "require_admin",
            ("PUT", "/api/config/tenants/{tenant_id}"): "require_admin",
            ("GET", "/api/config/markup"): "require_admin",
            ("PUT", "/api/config/markup"): "require_admin",
            ("GET", "/api/config/provider-pricing"): "require_pricing_admin",
            ("GET", "/api/config/provider-pricing/history"): "require_pricing_admin",
            ("POST", "/api/config/provider-pricing"): "require_pricing_admin",
            ("GET", "/api/config/markup-rules"): "require_pricing_admin",
            ("GET", "/api/config/markup-rules/history"): "require_pricing_admin",
            ("POST", "/api/config/markup-rules"): "require_pricing_writer",
            ("GET", "/api/config/access-charges"): "require_pricing_admin",
            ("GET", "/api/config/access-charges/history"): "require_pricing_admin",
            ("GET", "/api/config/tenant-allowance-usage"): "require_pricing_admin",
            ("POST", "/api/config/access-charges"): "require_pricing_writer",
            # FX rates (alembic 0044; PLAN rev E §2.1): billed-money config,
            # both verbs on the money-config write gate per the plan.
            ("GET", "/api/config/fx-rates"): "require_pricing_writer",
            ("PUT", "/api/config/fx-rates"): "require_pricing_writer",
            # Commercial terms (2026-09-10; POST re-gated 2026-09-11).
            #
            # ⛔ THE ORIGINAL ENTRY HERE WAS WRONG, AND WRONG IN THE DANGEROUS
            # DIRECTION. It put BOTH routes on `require_pricing_admin` and called
            # that "DELIBERATELY TIGHTER than the `require_pricing_writer` on the
            # single-term POSTs above". Compare the actual sets in auth_deps.py:
            # `_PRICING_ADMIN_ROLES = {license-admin, billing-internal}` against
            # `_PRICING_WRITER_ROLES = {license-admin, pricing-writer}`. Neither
            # contains the other, so "tighter" is not even the right relation — and
            # on the only axis that matters (who may rewrite billed money) pricing_
            # admin is strictly LOOSER: it admits `billing-internal`, the precise
            # role #55 removed from money writes. The claim reasoned from the NAMES
            # ("admin" sounds stronger than "writer"), not from the role sets.
            #
            # It was a LIVE capability, not a theoretical one. billing-service's own
            # in-pod SA token (`azp: billing-service`) carries `billing-internal` +
            # `service-internal` and NO `license-admin` — measured against the
            # deployed pod. So that SA, and the nightly pricing-seed CronJob sharing
            # it, could set any tenant's platform fee, seat fee and markup through
            # `POST /terms`. #55 removed exactly that; /terms reopened it by the
            # back door, and no test sent a billing-internal token here to notice.
            #
            # The original entry's INTENT was right and is better served by the
            # correction: `POST /terms` writes a tenant's WHOLE rate card in one
            # call, and with `?force=true` re-prices a live tenant. That argues for
            # a gate AT LEAST as strict as the single-term POSTs, never a looser
            # one. `require_pricing_writer` is that floor, and `pricing-writer`
            # exists precisely so the licences SA can provision offerings — which
            # is /terms' intended caller.
            #
            # The GET deliberately STAYS on `require_pricing_admin`. Disclosure is
            # not a money write, and provisioning/onboarding must be able to verify
            # what a tenant is on. The asymmetry is the point; see the docstring on
            # `get_tenant_terms`.
            ("POST", "/api/config/tenants/{tenant_id}/terms"): "require_pricing_writer",
            ("GET", "/api/config/tenants/{tenant_id}/terms"): "require_pricing_admin",
            # `/commercial-terms` is the CommonPractice shape of the same statement — billing_mode,
            # the flat platform fee, a `seat:<app>` per-seat rate and a service-scoped markup, in one
            # transaction — so it carries the SAME gates as `/terms` above, for the same reasons:
            # the write is a whole rate card (floor: `require_pricing_writer`, which is what lets the
            # licences SA provision an offering), and the read is disclosure rather than a money
            # write, so provisioning can verify what a tenant is on.
            ("PUT", "/api/config/tenants/{tenant_id}/commercial-terms"): "require_pricing_writer",
            ("GET", "/api/config/tenants/{tenant_id}/commercial-terms"): "require_pricing_admin",
            # The STANDARD rate card (settings.default_*) CP prefills its workspace forms
            # from: disclosure, never a write, so the same read gate as the two term GETs.
            ("GET", "/api/config/rate-card"): "require_pricing_admin",
            # Attribution rules (alembic 0041) decide WHO PAYS, so the writes are
            # money-config writes and take the writer floor; the read is disclosure.
            ("GET", "/api/config/attribution-rules"): "require_pricing_admin",
            ("PUT", "/api/config/attribution-rules"): "require_pricing_writer",
            ("DELETE", "/api/config/attribution-rules/{rule_id}"): "require_pricing_writer",
        }

        actual: dict[tuple[str, str], str] = {}
        # FastAPI >=0.137 nests included routers behind `_IncludedRouter`
        # wrappers; iter_route_contexts flattens them to the effective routes
        # (prefix + router-level dependencies applied), as app.routes used to.
        for route in iter_route_contexts(app.routes):
            path = route.path or ""
            dependant = getattr(route, "dependant", None)
            if not path.startswith("/api/config") or dependant is None:
                continue
            gates = sorted(
                {
                    d.call.__name__
                    for d in dependant.dependencies
                    if getattr(d, "call", None) is not None
                    and d.call.__name__.startswith("require_")
                }
            )
            methods = route.methods
            assert methods is not None, path
            for method in sorted(methods - {"HEAD", "OPTIONS"}):
                actual[(method, path)] = ",".join(gates)

        assert actual == expected
