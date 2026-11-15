"""Per-member cost is admin data; a tenant licence token must not read it.

THE DEFECT UNDER TEST
---------------------
``GET /api/costs/tenant/{id}/actors`` shipped behind ``require_tenant`` +
``enforce_tenant_isolation``. Together those prove exactly one thing: *the caller
belongs to the tenant they asked about*. That is the correct gate for the tenant
AGGREGATE reads beside it and the wrong one here, because ``require_tenant``
accepts the ``X-Tenant-Id`` header that license-authz injects after validating a
LICENCE token -- and **every member of a tenant holds one**. So any seat could
enumerate every colleague's individual spend, and the isolation check would
agree the whole time, because the caller genuinely was in that tenant.

⚠️ THE ISOLATION CHECK IS NOT THE HOLE AND MUST NOT BE "FIXED". It does its job
perfectly. The hole is that belonging to a tenant was ever treated as sufficient
for a PER-MEMBER breakdown.

WHAT MUST NOT MOVE
------------------
``/api/costs/me`` and ``/api/costs/user/{subject_id}`` are read by fr. They
filter ``subject_id`` (WHO PAYS); this route groups ``actor_id`` (WHO ACTED).
Section 3 pins both, because a change that tightened them would break fr silently
and no assertion in sections 1-2 would notice.
"""

from __future__ import annotations

import uuid
from datetime import UTC, datetime
from decimal import Decimal

import pytest
from sqlalchemy import insert

import auth_deps
from models.db import CostActorDaily, CostDaily
from tests.conftest import T

TENANT = T("actor-auth")
OTHER_TENANT = T("actor-auth-other")
ACTOR = uuid.uuid5(uuid.NAMESPACE_URL, "actor-auth/alice")
SUBJECT = uuid.uuid5(uuid.NAMESPACE_URL, "actor-auth/payer")

ACTORS_PATH = f"/api/costs/tenant/{TENANT}/actors"
BEARER = {"Authorization": "Bearer fake"}

# A distinct value so a response that reports it cannot have come from anywhere
# else in the fixture set.
ACTOR_COST = Decimal("81.81")
TENANT_COST = Decimal("82.82")


def _token(monkeypatch, roles: list[str], sub: str = "test-sa"):
    """Force bearer decoding to a known payload, patching BOTH modules.

    ``auth_deps`` imports ``_decode_bearer_token`` by name at module load, so
    patching only ``auth`` leaves the copy the dependency actually calls
    untouched -- the reason ``tests/test_commercial_terms.py::admin_actor``
    patches both, and the reason this helper exists rather than being inlined
    three times.
    """
    payload = {"sub": sub, "realm_access": {"roles": roles}}

    async def _fake_decode(_request):
        return payload

    import auth

    monkeypatch.setattr(auth, "DASHBOARD_AUTH_ENABLED", True)
    monkeypatch.setattr(auth, "_decode_bearer_token", _fake_decode)
    monkeypatch.setattr(auth_deps, "_decode_bearer_token", _fake_decode)
    return payload


def _no_token(monkeypatch):
    """Bearer decoding fails, as it does for a licence token from license-authz.

    A licence token is not a Keycloak service-account JWT: it does not verify
    against the secure-apps JWKS, so the real dependency raises 401 here. That
    401 is the INPUT to the behaviour under test, not the behaviour.
    """
    from fastapi import HTTPException

    async def _fail(_request):
        raise HTTPException(status_code=401, detail="Invalid token")

    import auth

    monkeypatch.setattr(auth, "DASHBOARD_AUTH_ENABLED", True)
    monkeypatch.setattr(auth, "_decode_bearer_token", _fail)
    monkeypatch.setattr(auth_deps, "_decode_bearer_token", _fail)


async def _seed(session_factory) -> None:
    """One actor-rollup row and one daily row, so a 200 can be non-empty.

    ⛔ A 200 over an EMPTY table cannot distinguish "authorised, and there is no
    data" from "authorised, and the query is broken". The service-caller test
    below therefore asserts the VALUE, not the status code alone.

    UTC, like the route (``timeutil.default_end`` -> ``end.date()``):
    ``date.today()`` is LOCAL, and on a host ahead of UTC it is tomorrow for
    part of every day, so the seeded row falls past the window and the 200
    comes back empty.
    """
    today = datetime.now(UTC).date()
    async with session_factory() as session:
        await session.execute(
            insert(CostActorDaily),
            [
                {
                    "id": uuid.uuid4(),
                    "bucket_date": today,
                    "tenant_id": TENANT,
                    "license_id": uuid.uuid4(),
                    "subject_id": SUBJECT,
                    "actor_id": ACTOR,
                    "service_key": "commonpractice",  # pragma: allowlist secret
                    "provider": "claude",
                    "served_model": "claude-3-5-sonnet-20241022",
                    "byo": False,
                    "owner_provided": False,
                    "total_input_tokens": 100,
                    "total_output_tokens": 50,
                    "total_cost_usd": ACTOR_COST,
                    "total_vendor_cost_usd": None,
                    "request_count": 1,
                }
            ],
        )
        await session.execute(
            insert(CostDaily),
            [
                {
                    "bucket_date": today,
                    "tenant_id": TENANT,
                    "tenant_name": "",
                    "license_id": uuid.uuid4(),
                    "subject_id": SUBJECT,
                    "service_key": "commonpractice",  # pragma: allowlist secret
                    "provider": "claude",
                    "served_model": "claude-3-5-sonnet-20241022",
                    "total_input_tokens": 100,
                    "total_output_tokens": 50,
                    "total_cost_usd": TENANT_COST,
                    "request_count": 1,
                }
            ],
        )
        await session.commit()


# ---------------------------------------------------------------------------
# 1. The refusal
# ---------------------------------------------------------------------------


class TestALicenceTokenCallerIsRefused:
    @pytest.mark.integration
    async def test_a_tenant_member_gets_403_not_200(self, app_client, monkeypatch):
        """⚠️ MUTATION TARGET: change the dependency back to ``require_tenant``.

        ⛔ AND THE STATUS IS PART OF THE ASSERTION. Before this change the exact
        same request returned **200 with every colleague's spend**. A test that
        only asserted "not 200" would also pass against a route that 500s, and a
        test that accepted 401 would pass against a gate that merely asked this
        caller to re-authenticate -- which cannot help, because no licence token
        will ever satisfy this route.
        """
        _no_token(monkeypatch)
        resp = app_client.get(
            ACTORS_PATH,
            headers={**BEARER, "X-Tenant-Id": TENANT},
        )
        assert resp.status_code == 403, resp.text

    @pytest.mark.integration
    async def test_the_refusal_says_a_licence_token_is_not_enough(self, app_client, monkeypatch):
        """The operator-facing half. A bare 403 sends the reader to the wrong
        team; the detail names both the rule and the expired-SA-token case the
        gate deliberately cannot distinguish."""
        _no_token(monkeypatch)
        resp = app_client.get(ACTORS_PATH, headers={**BEARER, "X-Tenant-Id": TENANT})
        body = resp.json()
        assert "licence token" in str(body).lower() or "license" in str(body).lower()

    @pytest.mark.integration
    async def test_a_valid_sa_token_without_an_internal_role_is_also_refused(
        self, app_client, monkeypatch
    ):
        """⚠️ MUTATION TARGET: widen ``INTERNAL_SERVICE_ROLES``.

        A token that decodes perfectly but carries no internal service role must
        not pass. This is the case a "did the bearer parse?" check would wave
        through.
        """
        _token(monkeypatch, roles=["portal-user"])
        resp = app_client.get(ACTORS_PATH, headers={**BEARER, "X-Tenant-Id": TENANT})
        assert resp.status_code == 403, resp.text

    @pytest.mark.integration
    async def test_no_credential_at_all_is_401_not_403(self, app_client, monkeypatch):
        """⛔ THE HEADER IS NOT THE CREDENTIAL, AND THIS IS THE TEST THAT SAYS SO.

        ``require_service_caller`` reads ``X-Tenant-Id`` only to choose between
        two REFUSALS. A caller with no header and no usable token gets the
        underlying 401 -- unchanged. If a future edit ever inverted the check
        ("no header, so treat the caller as a service"), the gate would become
        openable by DELETING a header, and this assertion is what fails.
        """
        _no_token(monkeypatch)
        resp = app_client.get(ACTORS_PATH, headers=BEARER)
        assert resp.status_code == 401, resp.text


# ---------------------------------------------------------------------------
# 2. The admission
# ---------------------------------------------------------------------------


class TestAServiceCallerIsAdmitted:
    @pytest.mark.integration
    async def test_an_internal_sa_token_gets_200_and_the_real_numbers(
        self, app_client, db_session_factory, monkeypatch
    ):
        """⭐ THE CONTROL THAT KEEPS SECTION 1 HONEST.

        Without it, every refusal above is satisfied by a route that refuses
        EVERYONE -- which would break CP's backend and read as a security win.
        It asserts the VALUE, not merely the 200, so an authorised-but-broken
        query cannot pass either.
        """
        await _seed(db_session_factory)
        _token(monkeypatch, roles=["billing-internal"])
        resp = app_client.get(ACTORS_PATH, headers=BEARER)
        assert resp.status_code == 200, resp.text

        payload = resp.json()["data"]
        assert payload["tenant_id"] == TENANT
        by_actor = {line["actor_id"]: line for line in payload["by_actor"]}
        assert str(ACTOR) in by_actor
        assert Decimal(str(by_actor[str(ACTOR)]["cost_usd"])) == ACTOR_COST

    @pytest.mark.integration
    async def test_cp_may_proxy_for_its_own_tenant_with_the_header_present(
        self, app_client, db_session_factory, monkeypatch
    ):
        """CP's backend proxies this route for its tenant admins and legitimately
        names the tenant it is proxying for.

        ⚠️ MUTATION TARGET: refuse whenever ``X-Tenant-Id`` is present.

        That is the obvious-looking gate and it would 403 the one production
        caller this endpoint exists for -- a failure that no refusal test in
        section 1 can see, because there the header is present AND the token is
        bad.
        """
        await _seed(db_session_factory)
        _token(monkeypatch, roles=["billing-internal"])
        resp = app_client.get(ACTORS_PATH, headers={**BEARER, "X-Tenant-Id": TENANT})
        assert resp.status_code == 200, resp.text

    @pytest.mark.integration
    async def test_a_service_caller_cannot_read_another_tenant_under_its_own_assertion(
        self, app_client, db_session_factory, monkeypatch
    ):
        """⚠️ MUTATION TARGET: delete the ``enforce_tenant_isolation`` call.

        Dropping ``require_tenant`` also dropped the isolation check that rode on
        it. Re-adding it is not ceremony: without it a service caller proxying
        for tenant A can read tenant B's per-member breakdown while asserting A.
        """
        await _seed(db_session_factory)
        _token(monkeypatch, roles=["billing-internal"])
        resp = app_client.get(
            f"/api/costs/tenant/{OTHER_TENANT}/actors",
            headers={**BEARER, "X-Tenant-Id": TENANT},
        )
        assert resp.status_code == 403, resp.text

    @pytest.mark.integration
    async def test_an_unresolvable_path_tenant_is_a_400_not_a_503(self, app_client, monkeypatch):
        """⚠️ MUTATION TARGET: delete the ``resolve_tenant_or_400`` call.

        ``require_tenant`` used to canonicalise the tenant AND hand back a uuid;
        removing it removed that parse too. Without the replacement, a non-uuid
        reaches a Postgres ``uuid`` column and the caller is told
        503 DATABASE_UNAVAILABLE for their own bad input.
        """
        _token(monkeypatch, roles=["billing-internal"])
        resp = app_client.get(
            "/api/costs/tenant/not-a-uuid/actors",
            headers=BEARER,
        )
        assert resp.status_code == 400, resp.text


# ---------------------------------------------------------------------------
# 3. What did NOT change -- fr reads these
# ---------------------------------------------------------------------------


class TestTheSelfServiceRoutesAreUntouched:
    @pytest.mark.integration
    async def test_costs_me_still_admits_a_licence_token_caller(
        self, app_client, db_session_factory, monkeypatch
    ):
        """⛔ WHAT DID NOT CHANGE. ``/costs/me`` is self-service: it is scoped to
        the caller's OWN subject in their OWN active tenant, so a licence-token
        caller is exactly who it is for. A change that tightened this would break
        fr, and nothing in sections 1-2 would notice.
        """
        await _seed(db_session_factory)
        _no_token(monkeypatch)
        resp = app_client.get(
            "/api/costs/me",
            headers={"X-Tenant-Id": TENANT, "X-Subject-Id": str(SUBJECT)},
        )
        assert resp.status_code == 200, resp.text

    @pytest.mark.integration
    async def test_costs_user_still_requires_admin_and_nothing_more(
        self, app_client, db_session_factory, monkeypatch
    ):
        """``/costs/user/{id}`` was, and remains, ``require_admin``."""
        await _seed(db_session_factory)
        _token(monkeypatch, roles=["license-admin"])
        resp = app_client.get(f"/api/costs/user/{SUBJECT}", headers=BEARER)
        assert resp.status_code == 200, resp.text


# ---------------------------------------------------------------------------
# 4. The dependency in isolation
# ---------------------------------------------------------------------------


class TestRequireServiceCaller:
    @pytest.mark.unit
    async def test_it_returns_the_payload_for_an_internal_role(self, monkeypatch):
        from fastapi import Request

        payload = _token(monkeypatch, roles=["license-admin"])
        scope = {
            "type": "http",
            "method": "GET",
            "path": "/api/costs/tenant/x/actors",
            "headers": [],
            "client": ("127.0.0.1", 1234),
            "query_string": b"",
        }
        request = Request(scope)
        assert await auth_deps.require_service_caller(request) == payload
        # The audit stash is the documented side effect of every dep in that
        # module; losing it would silently blank the actor on audit rows.
        assert request.state.audit_actor["role"] == "license-admin"
