"""The write-time gate that refuses a token-budget row for an unvouched tenant.

THE DEFECT THIS CLOSES
-----------------------
``token_budgets.tenant_id`` has no foreign key, and neither ``create_budget``
nor ``upsert_license_sync_budget`` ever checked the id against anything before
this change — measured on prod 2026-09-10: 61 live rows whose ``tenant_id`` was
actually a ``licenses.subject.id`` (a PERSON's primary key) rather than an
organisation. ``upsert_license_sync_budget`` is the sharper of the two: its
idempotency key deliberately omits ``tenant_id`` (a licence can legitimately
move tenants), which means a bad id does not just insert a bad row — it can
REPOINT an existing, correctly-owned budget onto the bad id. Test 5 below pins
that exact mechanism.

WHY THE FRESHNESS GATE IS THE LOAD-BEARING HALF
------------------------------------------------
``tenancy.write_gate_refuses_tenant`` is not a plain membership test.
"``canon`` is absent from the registry" has two different causes that demand
OPPOSITE verdicts: the overlay is loaded and genuinely does not know this id
(REFUSE), or the refresh loop has never succeeded / has not succeeded recently,
so absence proves nothing about the id and everything about the outage
(ALLOW). Collapsing those two would turn a licences-service outage into a
refusal of every budget write in the estate — a worse failure than the one
this guard exists to close. That is why test 2 below is a NEGATIVE CONTROL, not
an afterthought: fail-open on an unloaded overlay is the behaviour that keeps
this guard safe to turn on by default.
"""

from __future__ import annotations

import uuid
from unittest.mock import AsyncMock, MagicMock, patch

import pytest
from sqlalchemy import select

import redis_client
import tenancy
from models.db import TokenBudget
from models.domain import BudgetPeriod
from services.budget import (
    UnregisteredTenantError,
    create_budget,
    upsert_license_sync_budget,
)
from tests.conftest import T

pytestmark = pytest.mark.unit


@pytest.fixture(autouse=True)
def _clean_overlay():
    """No live overlay before or after any test here.

    Mirrors ``tests/test_live_tenant_registry.py``'s ``_clean_overlay``: without
    the teardown, an overlay installed by one test would leak into every other
    test file in the run, and the leak is invisible because it only ever makes
    resolution (and this gate's refusal) succeed more often.
    """
    tenancy.reset_live_tenant_registry()
    yield
    tenancy.reset_live_tenant_registry()


def _install_fresh_overlay() -> None:
    """Load a live overlay whose age is ~0s — the one state the gate may refuse in.

    The anchor name/value are arbitrary; only a successful install matters,
    because that is what stamps ``_live_refreshed_at`` and makes
    ``live_tenant_registry_stats()`` report a small positive age instead of
    ``-1.0``.
    """
    tenancy.set_live_tenant_registry({"gate-fresh-anchor": str(uuid.uuid4())})


@pytest.fixture
def gate_disabled(monkeypatch):
    """Disable ``budget_write_requires_registered_tenant`` for one test.

    Copies the ``armed`` idiom from ``tests/test_unregistered_tenant_exclusion.py``
    (lines 76-92), patching the cached ``Settings`` instance rather than the
    environment because the flag is read once per call via ``get_settings()``.
    The guard is checked inside ``services.budget_crud``, so that module's
    ``get_settings`` name — NOT ``services.cost_flusher``'s — is the one that
    must be patched.
    """
    from settings import get_settings

    get_settings.cache_clear()
    settings = get_settings()
    monkeypatch.setattr(settings, "budget_write_requires_registered_tenant", False)
    monkeypatch.setattr("settings.get_settings", lambda: settings)
    monkeypatch.setattr("services.budget_crud.get_settings", lambda: settings)
    yield settings
    get_settings.cache_clear()


async def _all_budgets(session):
    return list((await session.execute(select(TokenBudget))).scalars().all())


# ---------------------------------------------------------------------------
# create_budget
# ---------------------------------------------------------------------------


async def test_create_budget_refuses_an_unregistered_tenant_when_the_overlay_is_fresh(
    db_session,
):
    _install_fresh_overlay()
    unknown = str(uuid.uuid4())

    with pytest.raises(UnregisteredTenantError) as excinfo:
        await create_budget(
            tenant_id=unknown,
            budget_tokens=10_000,
            session=db_session,
        )
    assert excinfo.value.tenant_id == unknown
    assert await _all_budgets(db_session) == []


async def test_create_budget_allows_an_unknown_tenant_when_the_overlay_was_never_refreshed(
    db_session,
):
    """NEGATIVE CONTROL — the deliberate fail-open.

    No overlay has ever been installed (``reset_live_tenant_registry`` ran in
    the autouse fixture and nothing re-loaded it), so ``live_tenant_registry_
    stats()`` reports age ``-1.0``. Absence from a registry that was never
    loaded is not evidence the tenant is unregistered — it is evidence the
    licences service has not been reachable, and refusing here is exactly what
    would turn a licences outage into a refusal of every budget write in the
    estate. The row MUST be created.
    """
    assert tenancy.live_tenant_registry_stats() == (0, -1.0)
    unknown = str(uuid.uuid4())

    budget = await create_budget(
        tenant_id=unknown,
        budget_tokens=10_000,
        session=db_session,
    )
    assert budget.tenant_id == unknown
    rows = await _all_budgets(db_session)
    assert len(rows) == 1
    assert rows[0].tenant_id == unknown


async def test_create_budget_allows_a_registered_tenant_with_a_fresh_overlay(db_session):
    """NEGATIVE CONTROL — the ordinary case the guard must never break."""
    _install_fresh_overlay()
    tenant_id = T("gate-ok-1")

    budget = await create_budget(
        tenant_id=tenant_id,
        budget_tokens=10_000,
        session=db_session,
    )
    assert budget.tenant_id == tenant_id
    rows = await _all_budgets(db_session)
    assert len(rows) == 1


# ---------------------------------------------------------------------------
# upsert_license_sync_budget
# ---------------------------------------------------------------------------


async def test_upsert_license_sync_refuses_an_unregistered_tenant_when_the_overlay_is_fresh(
    db_session,
):
    _install_fresh_overlay()
    unknown = str(uuid.uuid4())

    with pytest.raises(UnregisteredTenantError) as excinfo:
        await upsert_license_sync_budget(
            db_session,
            tenant_id=unknown,
            license_id=str(uuid.uuid4()),
            period=BudgetPeriod.monthly,
            budget_tokens=10_000,
        )
    assert excinfo.value.tenant_id == unknown
    assert await _all_budgets(db_session) == []


async def test_upsert_license_sync_cannot_repoint_an_existing_budget_to_an_unregistered_tenant(
    db_session,
):
    """THE MOST IMPORTANT TEST — pins the actual production mechanism.

    ``upsert_license_sync_budget``'s idempotency key is (license_id, period,
    source='license_sync', active) — deliberately NOT tenant-keyed, because a
    licence can legitimately move tenants. That means a re-sync call carrying a
    bad tenant_id does not just fail to insert a junk row: absent this guard it
    would silently overwrite the tenant_id on an existing, correctly-owned
    budget. The row must come back from the DATABASE untouched, not just from
    the in-memory object the failed call happened to touch.
    """
    owner = T("gate-owner")
    lic_id = str(uuid.uuid4())

    owned, created = await upsert_license_sync_budget(
        db_session,
        tenant_id=owner,
        license_id=lic_id,
        period=BudgetPeriod.monthly,
        budget_tokens=50_000,
    )
    assert created is True
    assert owned.tenant_id == owner
    budget_id = owned.id

    _install_fresh_overlay()
    intruder = str(uuid.uuid4())

    with pytest.raises(UnregisteredTenantError) as excinfo:
        await upsert_license_sync_budget(
            db_session,
            tenant_id=intruder,
            license_id=lic_id,
            period=BudgetPeriod.monthly,
            budget_tokens=99_999,
        )
    assert excinfo.value.tenant_id == intruder

    reloaded = (
        await db_session.execute(select(TokenBudget).where(TokenBudget.id == budget_id))
    ).scalar_one()
    assert reloaded.tenant_id == owner
    assert reloaded.budget_tokens == 50_000


# ---------------------------------------------------------------------------
# HTTP boundary
# ---------------------------------------------------------------------------


def test_post_budget_returns_422_not_400_for_an_unregistered_tenant(app_client):
    _install_fresh_overlay()
    unknown = str(uuid.uuid4())

    response = app_client.post(
        "/api/budget",
        json={
            "tenant_id": unknown,
            "budget_tokens": 10_000,
            "period": "monthly",
        },
    )
    assert response.status_code == 422
    # NOT 400: on this service 400 already means "does not parse or resolve to
    # any tenant shape" (see `resolve_tenant_or_400` / `_canonical_tenant`). This
    # value parses and resolves fine — it is simply not a tenant the registry
    # has — and collapsing the two status codes would make "is my id malformed,
    # or is my tenant not registered?" unanswerable from the response alone.
    assert response.status_code != 400


# ---------------------------------------------------------------------------
# Settings escape hatch
# ---------------------------------------------------------------------------


async def test_the_gate_can_be_disabled_by_settings(db_session, gate_disabled):
    _install_fresh_overlay()
    unknown = str(uuid.uuid4())

    budget = await create_budget(
        tenant_id=unknown,
        budget_tokens=10_000,
        session=db_session,
    )
    assert budget.tenant_id == unknown
    rows = await _all_budgets(db_session)
    assert len(rows) == 1


# ---------------------------------------------------------------------------
# The predicate itself
# ---------------------------------------------------------------------------


def test_write_gate_refuses_tenant_is_a_pure_predicate():
    unknown = str(uuid.uuid4())
    registered = T("gate-predicate-registered")

    # Never refreshed: absence proves nothing -> ALLOW.
    assert tenancy.live_tenant_registry_stats() == (0, -1.0)
    assert tenancy.write_gate_refuses_tenant(unknown, max_overlay_age_s=900.0) is False

    _install_fresh_overlay()

    # Fresh + unknown -> REFUSE.
    assert tenancy.write_gate_refuses_tenant(unknown, max_overlay_age_s=900.0) is True

    # Fresh + registered -> ALLOW.
    assert tenancy.write_gate_refuses_tenant(registered, max_overlay_age_s=900.0) is False

    # The nil sentinel is a real registry row (default-anon) -> ALLOW, regardless
    # of overlay freshness.
    assert (
        tenancy.write_gate_refuses_tenant(tenancy.NIL_TENANT_ID, max_overlay_age_s=900.0) is False
    )

    # A zero threshold makes even a freshly-installed overlay stale BY POLICY:
    # by the time age is measured some nonzero monotonic time has elapsed, so
    # `0.0 <= age <= 0.0` can never hold -> ALLOW.
    assert tenancy.write_gate_refuses_tenant(unknown, max_overlay_age_s=0.0) is False


# ---------------------------------------------------------------------------
# The bulk reconcile loop — the path that produced the 61 bad rows in prod,
# and the one where a mistake is catastrophic rather than merely wrong.
# ---------------------------------------------------------------------------


async def test_sync_from_licenses_skips_one_unregistered_licence_and_still_writes_the_others(
    app_client, db_session, fake_redis, monkeypatch
):
    """The guard must cost exactly one licence, never the whole reconcile run.

    `routers/budget.py`'s sync-from-licenses loop shares ONE transaction and ONE
    commit across every licence it processes, and the surrounding `try` only
    catches `httpx.RequestError` — NOT `UnregisteredTenantError`. If the guard's
    raise ever escaped the loop's own `except UnregisteredTenantError` (the one
    at routers/budget.py:859), it would roll back EVERY other tenant's budget
    and quota write in the same run, turning one bad upstream row into a
    fleet-wide no-op. The route refuses without Redis (quota keys), so the
    repo's ``fake_redis`` is installed.
    """
    import routers.budget as budget_router

    monkeypatch.setattr(redis_client, "_redis", fake_redis)

    _install_fresh_overlay()

    good_tenant = T("sync-ok")
    good_lic_id = str(uuid.uuid4())
    bad_tenant = str(uuid.uuid4())  # NOT passed through T() -- must stay unregistered
    bad_lic_id = str(uuid.uuid4())

    licenses = [
        {
            "id": good_lic_id,
            "tenant_id": good_tenant,
            "status": "active",
            "plan": {"token_budget_monthly": 50_000},
        },
        {
            "id": bad_lic_id,
            "tenant_id": bad_tenant,
            "status": "active",
            "plan": {"token_budget_monthly": 50_000},
        },
    ]

    mock_resp = MagicMock()
    mock_resp.status_code = 200
    mock_resp.json.return_value = licenses
    mock_client = AsyncMock()
    mock_client.get.return_value = mock_resp
    mock_client.__aenter__ = AsyncMock(return_value=mock_client)
    mock_client.__aexit__ = AsyncMock(return_value=False)

    with patch.object(budget_router.httpx, "AsyncClient", return_value=mock_client):
        resp = app_client.post("/api/budget/sync-from-licenses")

    # THE ANTI-ROLLBACK ASSERTION. A raise escaping the loop for the bad licence
    # would 500 this request and discard the GOOD licence's write in the same
    # transaction -- the exact failure mode the per-licence catch exists to
    # prevent. 200 here is what proves the batch was split, not discarded.
    assert resp.status_code == 200, resp.text
    body = resp.json()
    assert body["skipped_count"] == 1

    good_result = await db_session.execute(
        select(TokenBudget).where(TokenBudget.license_id == uuid.UUID(good_lic_id))
    )
    good_row = good_result.scalars().one_or_none()
    bad_result = await db_session.execute(
        select(TokenBudget).where(TokenBudget.license_id == uuid.UUID(bad_lic_id))
    )
    bad_row = bad_result.scalars().one_or_none()
    assert good_row is not None, "the registered licence's budget was not written"
    assert good_row.tenant_id == good_tenant
    assert bad_row is None, "the unregistered licence's budget was written anyway"
