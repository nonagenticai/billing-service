"""Archival must be an off-switch on the PRE-SPEND path.

⛔ WHY THESE TESTS EXIST AT ALL. Measured 2026-09-10: `tenants.archived_at` is
enforced in exactly ONE place in the whole estate — licences' license-MINT path,
whose own comment reads "archiving still revokes nothing: this blocks new
licences only, and the existing ones keep working exactly as before". It has zero
hits in license-authz, in this service, in inference-router and in provisioning.
17 tenants were archived with all 19 of their licences `revoked`, and `rmt-e2e`
(019fb1f4-…-645f, archived 2026-09-03) was *still emitting* — 405 cost events in
14 days, the last inside the preceding 24 h.

⭐ The discriminator that decided WHERE the refusal goes: all 405 of those rows
carry `license_id IS NULL`. The spend that survives archival is spend that never
presents a licence, because Istio ext_authz attaches `context: GATEWAY` and
cannot see east-west traffic. So revocation is not a control and no licence check
can become one. The tenant is the only identity such a request does present.
"""

from __future__ import annotations

import pytest
from sqlalchemy.ext.asyncio import AsyncSession

from services import tenant_names
from services.tenant_names import TenantInfo, parse_tenant_rows
from tests._budget_factory import make_budget
from tests.conftest import T


@pytest.fixture(autouse=True)
def _clean_registry_cache():
    """The registry cache is process-wide; leaking it across tests would make
    one test's archived tenant another test's mystery 429."""
    tenant_names.reset_cache()
    yield
    tenant_names.reset_cache()


def _seed_registry(mapping: dict[str, TenantInfo]) -> None:
    """Populate the real TTL cache so the production code path runs.

    Deliberately NOT a monkeypatch of ``is_tenant_archived``: patching the
    predicate would prove only that the router calls something, leaving the
    parse — which is where the fail-open direction actually lives — untested.
    """
    tenant_names._name_cache[tenant_names._CACHE_KEY] = {k.lower(): v for k, v in mapping.items()}


@pytest.mark.asyncio
async def test_archived_tenant_is_refused_with_its_own_reason(app_client, db_session: AsyncSession):
    db_session.add(make_budget(tenant=T("t-arch"), cap=10_000, consumed=10, source="test"))
    await db_session.commit()
    _seed_registry({T("t-arch"): TenantInfo(name="retired-co", archived=True)})

    r = app_client.post(
        "/api/budget/reserve",
        headers={"X-Tenant-Id": T("t-arch")},
        json={"estimated_tokens": 100, "ttl_seconds": 30},
    )

    assert r.status_code == 429
    body = r.json()["error"]["details"]
    assert body["allowed"] is False
    # Not `over_budget`: this tenant has 9,990 tokens of headroom. The reason
    # must name the actual cause, because the caller's policy branches on it —
    # inference-router allows `no_active_budget` and refuses this.
    assert body["reason"] == "tenant_archived"
    # ⚠️ The body must carry the REAL cap, not zeros. A human reading a 429 that
    # says cap=0 concludes "misconfigured budget" and goes looking in the wrong
    # place; the refusal is deliberate and the numbers must show it.
    assert body["cap_tokens"] == 10_000
    assert body["consumed_tokens"] == 10


@pytest.mark.asyncio
async def test_live_tenant_in_the_same_registry_is_unaffected(app_client, db_session: AsyncSession):
    """The negative control: a registry that CAN say archived, saying not-archived.

    Without this, a test suite where the predicate always returned True would be
    indistinguishable from one where it works.
    """
    db_session.add(make_budget(tenant=T("t-live"), cap=10_000, consumed=0, source="test"))
    await db_session.commit()
    _seed_registry({T("t-live"): TenantInfo(name="live-co", archived=False)})

    r = app_client.post(
        "/api/budget/reserve",
        headers={"X-Tenant-Id": T("t-live")},
        json={"estimated_tokens": 500, "ttl_seconds": 30},
    )

    assert r.status_code == 200
    assert r.json()["data"]["allowed"] is True


@pytest.mark.asyncio
async def test_unknown_tenant_fails_open_not_closed(app_client, db_session: AsyncSession):
    """⛔ THE MOST IMPORTANT TEST IN THIS FILE.

    An empty registry is what a licences outage looks like: ``get_tenant_info``
    returns ``{}`` on any failure and caches that for the full TTL. If absence
    read as "archived", a five-second licences blip would refuse ALL inference
    for five minutes. The ordering of harms is explicit and matches the
    inference-router gate's: unbounded spend during a bounded dependency outage
    is a bounded loss; refusing all inference is not.
    """
    db_session.add(make_budget(tenant=T("t-unknown"), cap=10_000, consumed=0, source="test"))
    await db_session.commit()
    _seed_registry({})  # licences answered, but knows nothing — or answered nothing

    r = app_client.post(
        "/api/budget/reserve",
        headers={"X-Tenant-Id": T("t-unknown")},
        json={"estimated_tokens": 500, "ttl_seconds": 30},
    )

    assert r.status_code == 200, "an unknown tenant must ALLOW, never refuse"
    assert r.json()["data"]["allowed"] is True


@pytest.mark.asyncio
async def test_archived_refusal_precedes_the_cap_check(app_client, db_session: AsyncSession):
    """An archived tenant with NO budget row must still be refused.

    ⛔ This is the case that proves arming the budget cap does not close the gap:
    ``reserve_budget`` answers ``no_active_budget`` for a tenant with no rows, and
    inference-router's gate deliberately ALLOWS that reason (refusing because a
    configuration row is absent is an outage, not enforcement). So an archived
    tenant with no budget would sail straight through a cap-only check — which is
    exactly the shape of the tenant measured still spending.
    """
    _seed_registry({T("t-arch-nobudget"): TenantInfo(name="retired-2", archived=True)})

    r = app_client.post(
        "/api/budget/reserve",
        headers={"X-Tenant-Id": T("t-arch-nobudget")},
        json={"estimated_tokens": 100, "ttl_seconds": 30},
    )

    assert r.status_code == 429
    assert r.json()["error"]["details"]["reason"] == "tenant_archived"


@pytest.mark.parametrize(
    ("raw", "expected"),
    [
        (True, True),
        (False, False),
        ("false", False),  # TRUTHY in Python — the trap this asserts against
        ("true", False),  # a string is not a bool; do not start trusting one
        (1, False),
        (None, False),
        ("__absent__", False),
    ],
)
def test_archived_is_parsed_strictly_and_degrades_to_not_archived(raw, expected):
    """``row.get("archived") is True`` — identity, not truthiness.

    ⛔ THIS TEST USED TO ASSERT THE EXPRESSION INLINE and therefore tested
    nothing: a mutant relaxing `is True` to `bool(...)` left it green, because
    the test re-implemented the logic instead of calling it. It now drives the
    real :func:`parse_tenant_rows`, which is why that function was extracted.

    The source field is a JSON bool. ``"false"`` is TRUTHY in Python, so a
    truthiness test would archive a LIVE tenant on a payload-shape change. Every
    non-bool degrades to not-archived — the fail-open direction.
    """
    tid = "0195f48b-0000-7000-8000-000000000001"
    row: dict = {"id": tid, "name": "n", "kind": "internal"}
    if raw != "__absent__":
        row["archived"] = raw

    parsed = parse_tenant_rows([row])

    assert parsed[tid].archived is expected


def test_parse_keeps_the_other_fields_while_adding_archived():
    """Negative control on the same parse: adding `archived` must not have cost
    the fields this module already depended on. Without this, deleting `name` or
    `kind` handling would be invisible to the archival tests."""
    parsed = parse_tenant_rows(
        [
            {
                "id": "0195F48B-0000-7000-8000-000000000002",
                "name": "  Retired Co  ",
                "kind": " internal ",
                "cost_centre": "  ",
                "archived": True,
            }
        ]
    )
    info = parsed["0195f48b-0000-7000-8000-000000000002"]  # id is lowercased
    assert info.name == "Retired Co"
    assert info.kind == "internal"
    assert info.cost_centre is None  # whitespace-only normalises to None
    assert info.archived is True
