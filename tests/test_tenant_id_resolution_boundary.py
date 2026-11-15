"""One tenant identifier, one meaning — at EVERY entry point, not just the header.

Since ``0011_tenant_id_is_a_uuid`` every ``tenant_id`` column is a real Postgres
``uuid``. ``require_tenant`` has always resolved the ``X-Tenant-Id`` HEADER (a
registered tenant NAME resolves to its registry uuid; junk is a stated 400), but
the BODY, PATH and QUERY carriers of the same identifier had no resolver at all.
Measured against the running app before the fix::

    POST /api/budget            {"tenant_id": "n8n-dev"}  -> 400 INVALID_IDENTIFIER
    GET  /api/quota/status/…    X-Tenant-Id: n8n-dev      -> 200

The same identifier meant two different things depending on which part of the
request carried it, and the unresolved paths failed as an opaque
``DataError`` -> 400/503 rather than a stated refusal.

That is now fixed in THREE independent places:

* ``TenantId`` / ``OptionalTenantId`` — a pydantic ``BeforeValidator``
  (``src/models/domain.py``) for request bodies;
* ``resolve_tenant_or_400()`` (``src/auth_deps.py``) for path / query / values
  read out of another service's JSON;
* ``require_tenant`` (``src/auth_deps.py``) for the header.

Three mechanisms, one contract — and nothing asserted that they agree. This
module is that assertion. It is deliberately written against the HTTP surface
(not against ``canonical_tenant_id``) because the defect was never in the
resolver; it was in the entry points that never called one.

WHY THE UUIDS ARE LITERALS HERE. The expected values below are hard-coded rather
than imported from ``tenancy.LEGACY_TENANT_NAMES``. Importing the map under test
would make every assertion agree with any future wrong edit to that map — the
test would keep passing while ``n8n-dev`` silently started resolving to somebody
else's tenant. These literals are the licences registry as measured on
2026-08-25; if one of them ever disagrees with the map, THAT is the finding.
"""

from __future__ import annotations

import uuid
from datetime import UTC, datetime
from urllib.parse import quote

import pytest
from fastapi.testclient import TestClient
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

import auth_deps
from models.db import MarkupRule, RequestQuotaConfig, TenantAccessCharge

# --- Ground truth: the licences registry, measured live 2026-08-25 ------------

N8N_DEV = "n8n-dev"
N8N_DEV_UUID = "019de842-c92a-7e37-8fdd-eb0d8c5396f9"

MARKETING = "marketing"
MARKETING_UUID = "019edc61-9495-76bc-b65f-c92a8e6880df"

SALES = "sales"
SALES_UUID = "019edc61-9495-7ee7-93dd-ea9e1f687f42"

#: RETIRED registry entry — deliberately NOT remapped. ``gerard-dev`` was removed
#: from ``licenses.tenants``; migration 0011 preserves its historical rows under a
#: *derived* (uuid5) id purely so the audit trail stays readable. Preserving old
#: rows is NOT the same as accepting new ones: re-adding this name to
#: ``LEGACY_TENANT_NAMES`` would let a retired tenant be written to again, and the
#: derived id it would resolve to is not a registry identity at all. If this test
#: ever fails because somebody "helpfully" re-added the name, the fix is to remove
#: the map entry, not to relax this test.
RETIRED_NAME = "gerard-dev"

#: Never registered, never derived — a plain client error.
JUNK_NAME = "not-a-tenant"

#: Names are case-SENSITIVE by design (uuids are not — see the spelling table
#: below). The registry key is ``marketing``; ``MARKETING`` is a different string
#: and resolves to nothing. Case-folding names here would silently merge two
#: distinct registry keys the day one is added that differs only in case.
WRONG_CASE_NAME = "MARKETING"

#: Every spelling of the SAME uuid that ``uuid.UUID`` accepts. All must land on
#: the identical canonical lowercase-dashed form at every entry point.
UUID_SPELLINGS = {
    "canonical": N8N_DEV_UUID,
    "uppercase": N8N_DEV_UUID.upper(),
    "braced": "{" + N8N_DEV_UUID + "}",
    "dashless": N8N_DEV_UUID.replace("-", ""),
}

AUTH_HEADERS = {"Authorization": "Bearer fake"}

#: Scopes the seeded markup rule / access charge so the history routes return a
#: row rather than an empty list — an empty 200 would prove nothing about whether
#: the NAME was resolved to the uuid the row is stored under.
SEED_SERVICE_KEY = "boundary-probe"  # pragma: allowlist secret


# --- Principals --------------------------------------------------------------
# Same shape as tests/test_pricing_write_gate.py::_install_principal. The four
# /api/config routes sit behind require_admin / require_pricing_admin and
# POST /api/quota/config behind require_internal; `license-admin` satisfies all
# three, so one principal covers every entry point under test. Installed
# explicitly rather than leaning on conftest's DASHBOARD_AUTH_ENABLED=false
# bypass so the auth dependency really executes ahead of the resolver — several
# of these routes resolve auth and a DB session BEFORE body validation, and a
# test that 401s instead of reaching the validator would assert nothing.


def _install_principal(monkeypatch, *, sub: str, roles: list[str]) -> dict:
    payload = {"sub": sub, "realm_access": {"roles": roles}}

    async def _fake_decode(_request):
        return payload

    import auth

    monkeypatch.setattr(auth, "DASHBOARD_AUTH_ENABLED", True)
    monkeypatch.setattr(auth, "_decode_bearer_token", _fake_decode)
    monkeypatch.setattr(auth_deps, "_decode_bearer_token", _fake_decode)
    return payload


@pytest.fixture
def license_admin(monkeypatch) -> dict:
    return _install_principal(
        monkeypatch,
        sub="service-account-fr-backend",
        roles=["default-roles-secure-apps", "service-internal", "license-admin"],
    )


# --- Seed --------------------------------------------------------------------


@pytest.fixture
async def seeded(db_engine) -> None:
    """Rows the history routes can actually return.

    Stored under the canonical UUID — never the name — because that is the whole
    point: a request naming ``n8n-dev`` must find rows written under
    ``019de842-…``. Also seeds the GLOBAL (NULL-tenant) markup rule that the
    nullable-query claim depends on.
    """
    factory = async_sessionmaker(bind=db_engine, class_=AsyncSession, expire_on_commit=False)
    now = datetime.now(UTC)
    async with factory() as s:
        s.add(
            MarkupRule(
                tenant_id=N8N_DEV_UUID,
                service_key=SEED_SERVICE_KEY,
                markup_pct=30,
                effective_from=now,
            )
        )
        s.add(
            MarkupRule(  # the global wildcard: every match dimension NULL
                tenant_id=None,
                markup_pct=10,
                effective_from=now,
            )
        )
        s.add(
            TenantAccessCharge(
                tenant_id=N8N_DEV_UUID,
                service_key=SEED_SERVICE_KEY,
                amount_usd=49,
                cadence="monthly",
                effective_from=now,
            )
        )
        await s.commit()


# --- Entry points ------------------------------------------------------------
#
# Each probe submits `value` at ONE carrier and returns the tenant_id the service
# actually settled on — read back from the response where the route echoes it,
# and from the DATABASE where it does not (the body route), because "the response
# was 201" says nothing about which uuid reached the column.


def _path(value: str) -> str:
    # quote(): the braced spelling carries `{`/`}`, which must survive the URL.
    return quote(value, safe="")


async def _via_config_tenant_get_path(client: TestClient, engine, value: str) -> str:
    resp = client.get(f"/api/config/tenants/{_path(value)}", headers=AUTH_HEADERS)
    assert resp.status_code == 200, resp.text
    return resp.json()["data"]["tenant_id"]


async def _via_config_tenant_put_path(client: TestClient, engine, value: str) -> str:
    resp = client.put(
        f"/api/config/tenants/{_path(value)}",
        headers=AUTH_HEADERS,
        json={"markup_pct": 12.5},
    )
    assert resp.status_code == 200, resp.text
    return resp.json()["data"]["tenant_id"]


async def _via_markup_history_query(client: TestClient, engine, value: str) -> str:
    resp = client.get(
        "/api/config/markup-rules/history",
        headers=AUTH_HEADERS,
        params={"tenant_id": value, "service_key": SEED_SERVICE_KEY},
    )
    assert resp.status_code == 200, resp.text
    rows = resp.json()["data"]
    # Non-vacuity: the seeded row is stored under the UUID. Finding it proves the
    # submitted spelling was resolved, not merely accepted.
    assert len(rows) == 1, f"expected the seeded rule, got {rows}"
    return rows[0]["tenant_id"]


async def _via_access_charge_history_query(client: TestClient, engine, value: str) -> str:
    resp = client.get(
        "/api/config/access-charges/history",
        headers=AUTH_HEADERS,
        params={"tenant_id": value, "service_key": SEED_SERVICE_KEY},
    )
    assert resp.status_code == 200, resp.text
    rows = resp.json()["data"]
    assert len(rows) == 1, f"expected the seeded access charge, got {rows}"
    return rows[0]["tenant_id"]


async def _via_allowance_usage_query(client: TestClient, engine, value: str) -> str:
    resp = client.get(
        "/api/config/tenant-allowance-usage",
        headers=AUTH_HEADERS,
        params={"tenant_id": value},
    )
    assert resp.status_code == 200, resp.text
    return resp.json()["data"]["tenant_id"]


async def _via_quota_config_body(client: TestClient, engine, value: str) -> str:
    """POST /api/quota/config — QuotaConfigRequest.tenant_id is a ``TenantId``."""
    license_id = str(uuid.uuid4())
    resp = client.post(
        "/api/quota/config",
        headers=AUTH_HEADERS,
        json={"license_id": license_id, "quota_monthly": 5000, "tenant_id": value},
    )
    assert resp.status_code == 201, resp.text
    # Read the COLUMN, not the response: this route echoes no tenant_id, so the
    # only honest observation of what it resolved is the durable row.
    factory = async_sessionmaker(bind=engine, class_=AsyncSession, expire_on_commit=False)
    async with factory() as s:
        row = (
            await s.execute(
                select(RequestQuotaConfig).where(
                    RequestQuotaConfig.license_id == uuid.UUID(license_id)
                )
            )
        ).scalar_one()
    return row.tenant_id


#: name -> (probe, refusal-status). The refusal status is STATED per carrier:
#: 400 wherever ``resolve_tenant_or_400`` guards a path/query value, 422 wherever
#: the pydantic ``TenantId`` validator guards a body field. Both are client
#: errors that name the problem; neither is the 503 DATABASE_UNAVAILABLE that a
#: raw uuid ``DataError`` used to produce.
ENTRY_POINTS: dict[str, tuple] = {
    "path:GET /api/config/tenants/{tenant_id}": (_via_config_tenant_get_path, 400),
    "path:PUT /api/config/tenants/{tenant_id}": (_via_config_tenant_put_path, 400),
    "query:GET /api/config/markup-rules/history": (_via_markup_history_query, 400),
    "query:GET /api/config/access-charges/history": (_via_access_charge_history_query, 400),
    "query:GET /api/config/tenant-allowance-usage": (_via_allowance_usage_query, 400),
    "body:POST /api/quota/config": (_via_quota_config_body, 422),
}

#: The refusal probes call the same URLs with a bad value and just report the
#: response, since the success probes assert 200/201 and would blow up first.
REFUSAL_CALLS = {
    "path:GET /api/config/tenants/{tenant_id}": lambda c, v: c.get(
        f"/api/config/tenants/{_path(v)}", headers=AUTH_HEADERS
    ),
    "path:PUT /api/config/tenants/{tenant_id}": lambda c, v: c.put(
        f"/api/config/tenants/{_path(v)}", headers=AUTH_HEADERS, json={"markup_pct": 12.5}
    ),
    "query:GET /api/config/markup-rules/history": lambda c, v: c.get(
        "/api/config/markup-rules/history", headers=AUTH_HEADERS, params={"tenant_id": v}
    ),
    "query:GET /api/config/access-charges/history": lambda c, v: c.get(
        "/api/config/access-charges/history", headers=AUTH_HEADERS, params={"tenant_id": v}
    ),
    "query:GET /api/config/tenant-allowance-usage": lambda c, v: c.get(
        "/api/config/tenant-allowance-usage", headers=AUTH_HEADERS, params={"tenant_id": v}
    ),
    "body:POST /api/quota/config": lambda c, v: c.post(
        "/api/quota/config",
        headers=AUTH_HEADERS,
        json={"license_id": str(uuid.uuid4()), "quota_monthly": 10, "tenant_id": v},
    ),
}


def test_the_two_entry_point_tables_cover_the_same_routes() -> None:
    """Positive control for this module's own tables.

    A route present in one table and missing from the other would be asserted in
    only half the claims below, silently.
    """
    assert set(ENTRY_POINTS) == set(REFUSAL_CALLS)


# --- Claim 1 -----------------------------------------------------------------


@pytest.mark.unit
class TestARegisteredNameResolvesEverywhere:
    async def test_a_registered_name_resolves_identically_at_every_entry_point(
        self, app_client: TestClient, db_engine, license_admin, seeded
    ) -> None:
        """`n8n-dev` -> 019de842-… in the path, the query AND the body.

        RED before the fix: the header answered 200 while the body answered 400
        and the path/query died in Postgres. Asserting the whole set at once is
        the point — the three resolvers are separate code, and a per-route test
        would let two of them drift apart while both stayed green.
        """
        observed = {
            name: await probe(app_client, db_engine, N8N_DEV)
            for name, (probe, _status) in ENTRY_POINTS.items()
        }
        assert observed == dict.fromkeys(ENTRY_POINTS, N8N_DEV_UUID)
        # And they agree with each other, not merely each with the literal.
        assert len(set(observed.values())) == 1


# --- Claim 2 -----------------------------------------------------------------


@pytest.mark.unit
class TestUuidSpellingsCanonicalise:
    @pytest.mark.parametrize("spelling", sorted(UUID_SPELLINGS), ids=sorted(UUID_SPELLINGS))
    async def test_every_uuid_spelling_canonicalises_at_every_entry_point(
        self, app_client: TestClient, db_engine, license_admin, seeded, spelling: str
    ) -> None:
        """UPPERCASE / braced / dashless are the SAME tenant, everywhere.

        Uuids are case-INsensitive (unlike names): a caller who spells their own
        id in upper case must not be told it is a different tenant, and must not
        write a second, differently-spelled copy of themselves into a column.
        """
        value = UUID_SPELLINGS[spelling]
        observed = {
            name: await probe(app_client, db_engine, value)
            for name, (probe, _status) in ENTRY_POINTS.items()
        }
        assert observed == dict.fromkeys(ENTRY_POINTS, N8N_DEV_UUID)


# --- Claim 3 -----------------------------------------------------------------


@pytest.mark.unit
class TestJunkIsAStatedRefusalAndIsNeverEchoed:
    @pytest.mark.parametrize("entry_point", sorted(REFUSAL_CALLS), ids=sorted(REFUSAL_CALLS))
    async def test_junk_is_refused_with_the_stated_status(
        self, app_client: TestClient, db_engine, license_admin, entry_point: str
    ) -> None:
        """400 from the path/query helper, 422 from the body validator.

        Both are stated client errors. Neither is 503 DATABASE_UNAVAILABLE, which
        is what an unresolved value produced before — blaming the database for a
        caller's own bad input.
        """
        _probe, expected = ENTRY_POINTS[entry_point]
        resp = REFUSAL_CALLS[entry_point](app_client, JUNK_NAME)
        assert resp.status_code == expected, resp.text

    @pytest.mark.parametrize("entry_point", sorted(REFUSAL_CALLS), ids=sorted(REFUSAL_CALLS))
    async def test_the_submitted_value_is_never_echoed_back(
        self, app_client: TestClient, db_engine, license_admin, entry_point: str
    ) -> None:
        """A tenant field has been observed in production carrying a raw bearer
        token (four rows). Reflecting the submitted value into a body that is
        logged, toasted and screenshotted turns a validation failure into a
        credential-disclosure path — so pydantic's ``input`` key is stripped by
        ``_sanitised_validation_details`` and the 400 detail names the FIELD, not
        the value.
        """
        # Synthetic, and the point of the test is that it is NEVER echoed back.
        secret_shaped = (
            "Bearer eyJhbGciOiJSUzI1NiJ9.tenant-boundary-probe"  # pragma: allowlist secret
        )
        resp = REFUSAL_CALLS[entry_point](app_client, secret_shaped)
        assert resp.status_code in (400, 422), resp.text
        body = resp.text
        assert secret_shaped not in body
        assert "tenant-boundary-probe" not in body
        assert "eyJhbGciOiJSUzI1NiJ9" not in body

    @pytest.mark.parametrize("entry_point", sorted(REFUSAL_CALLS), ids=sorted(REFUSAL_CALLS))
    async def test_tenant_names_are_case_sensitive(
        self, app_client: TestClient, db_engine, license_admin, entry_point: str
    ) -> None:
        """`MARKETING` is not `marketing`.

        Deliberate asymmetry with claim 2: a uuid is a case-insensitive encoding
        of the same 128 bits, while a name is a registry KEY. Case-folding names
        would merge two distinct keys the day one is registered that differs only
        in case, and would silently invent a resolution the registry never made.
        """
        _probe, expected = ENTRY_POINTS[entry_point]
        resp = REFUSAL_CALLS[entry_point](app_client, WRONG_CASE_NAME)
        assert resp.status_code == expected, resp.text


# --- Claim 4 -----------------------------------------------------------------


@pytest.mark.unit
class TestRetiredRegistryEntryIsRefused:
    @pytest.mark.parametrize("entry_point", sorted(REFUSAL_CALLS), ids=sorted(REFUSAL_CALLS))
    async def test_a_retired_registry_name_is_refused(
        self, app_client: TestClient, db_engine, license_admin, entry_point: str
    ) -> None:
        """`gerard-dev` is REFUSED, and that is not an oversight.

        It is a RETIRED ``licenses.tenants`` entry. Migration 0011 preserves its
        pre-existing rows under a *derived* (uuid5) id so the historical audit
        trail stays attributable — but preserving old rows is not the same as
        accepting new ones. Re-adding the name to ``LEGACY_TENANT_NAMES`` would
        make a retired tenant writable again under an id the registry has never
        issued, i.e. it would mint an identity. Do not "helpfully" re-add it; if
        the tenant comes back, it comes back through the registry.
        """
        _probe, expected = ENTRY_POINTS[entry_point]
        resp = REFUSAL_CALLS[entry_point](app_client, RETIRED_NAME)
        assert resp.status_code == expected, resp.text


# --- Claim 5 -----------------------------------------------------------------


@pytest.mark.unit
class TestNullTenantIsTheGlobalWildcardNotARefusal:
    async def test_omitting_the_nullable_tenant_query_selects_the_global_rule(
        self, app_client: TestClient, db_engine, license_admin, seeded
    ) -> None:
        """No ``tenant_id`` means the GLOBAL (NULL-tenant) markup lineage.

        ``markup_rules.tenant_id`` is nullable and NULL is the wildcard the seed
        migration relies on so a markup lookup never misses. Pushing ``None``
        through ``resolve_tenant_or_400`` would resolve it to nothing and turn the
        global scope into a 400 — the fix would have broken the default rule while
        "fixing" tenant resolution. Hence the ``if tenant_id is not None`` guard
        in the route, and hence this test.
        """
        resp = app_client.get("/api/config/markup-rules/history", headers=AUTH_HEADERS)
        assert resp.status_code == 200, resp.text
        rows = resp.json()["data"]
        assert len(rows) == 1, f"expected exactly the global wildcard rule, got {rows}"
        assert rows[0]["tenant_id"] is None
        # It really is the global row, not the tenant-scoped one filtered by luck.
        assert rows[0]["service_key"] is None
        assert str(rows[0]["markup_pct"]) == "10.00"

    async def test_an_explicitly_null_tenant_query_is_not_a_refusal(
        self, app_client: TestClient, db_engine, license_admin, seeded
    ) -> None:
        """The same via an empty-string query param — still not a 400 wildcard."""
        resp = app_client.get(
            "/api/config/markup-rules/history",
            headers=AUTH_HEADERS,
            params={"service_key": SEED_SERVICE_KEY},
        )
        assert resp.status_code == 200, resp.text
        # service_key set, tenant omitted -> the (NULL tenant, this app) lineage,
        # which has no rows. An empty 200 is the right answer; a 400 is not.
        assert resp.json()["data"] == []


# --- Claim 6 -----------------------------------------------------------------


@pytest.mark.unit
class TestIsolationRunsBeforeResolution:
    """The ORDER of the two checks on ``GET /api/quota/status/{tenant_id}``.

    ``enforce_tenant_isolation`` runs BEFORE ``resolve_tenant_or_400``, and that
    ordering is load-bearing, not incidental. Resolve first and the route answers
    **403** when the named target is a registered tenant and **400** when it is
    not — which hands any authenticated customer a tenant-name ENUMERATION ORACLE
    on a customer-facing route: probe names, read the status code, learn the
    registry. Checking isolation first makes both answers identical, so a
    cross-tenant caller learns exactly one thing: not yours.

    ``enforce_tenant_isolation`` canonicalises both sides itself (and falls back
    to the literal for an unresolvable value rather than to ``None``, so two
    different junk strings do not compare equal), so nothing is lost by running
    it first.
    """

    @pytest.mark.parametrize(
        "target,why",
        [
            (SALES, "a REGISTERED tenant that is not the caller"),
            (SALES_UUID, "the same registered tenant, named by uuid"),
            (JUNK_NAME, "an UNREGISTERED name"),
            (RETIRED_NAME, "a retired registry name"),
        ],
    )
    async def test_a_cross_tenant_caller_gets_403_not_400(
        self, app_client: TestClient, db_engine, target: str, why: str
    ) -> None:
        resp = app_client.get(
            f"/api/quota/status/{_path(target)}",
            headers={"X-Tenant-Id": MARKETING},
        )
        assert resp.status_code == 403, f"{why}: {resp.status_code} {resp.text}"

    async def test_the_403s_are_indistinguishable_from_each_other(
        self, app_client: TestClient, db_engine
    ) -> None:
        """Same status AND same message — a differing body is the same oracle.

        Closing the status-code channel while leaving "unknown tenant" vs
        "forbidden" in the message would leak exactly what the ordering exists to
        hide.
        """

        def _body(target: str) -> dict:
            resp = app_client.get(
                f"/api/quota/status/{_path(target)}", headers={"X-Tenant-Id": MARKETING}
            )
            assert resp.status_code == 403, resp.text
            err = resp.json()["error"]
            # request_id/timestamp vary per request by design.
            return {k: v for k, v in err.items() if k not in ("request_id", "timestamp")}

        assert _body(SALES) == _body(JUNK_NAME)

    async def test_the_caller_is_not_blanket_403d(self, app_client: TestClient, db_engine) -> None:
        """Positive control: the caller's OWN tenant is not forbidden.

        Without this, every assertion above would still pass if the route simply
        403'd everything. The caller names itself by NAME while the header
        carries the same name; isolation passes, resolution runs, and the route
        goes on to consult the licences service — unreachable in-test, so the
        honest answer is 503. The point is only that it is neither 403 nor 400.
        """
        resp = app_client.get(
            f"/api/quota/status/{MARKETING_UUID}", headers={"X-Tenant-Id": MARKETING}
        )
        assert resp.status_code not in (400, 403), resp.text
        assert resp.status_code == 503, resp.text
