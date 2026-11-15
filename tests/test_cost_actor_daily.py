"""Per-ACTOR daily cost (alembic 0040) and the provider-reported vendor cost on the daily rollup.

⭐ ``actor_id`` (WHO ACTED — the Keycloak sub) and ``vendor_cost_usd`` (the provider's own charge) are
on the raw event but were dropped by the daily rollup, and raw events are retained for days. These
tests pin that both now survive into durable rollups, and that the actor table RECONCILES with
``CostDaily`` — the same money split a different way, never a second population.
"""

from __future__ import annotations

import uuid
from datetime import UTC, datetime, timedelta
from decimal import Decimal
from unittest.mock import AsyncMock, patch

import httpx
import pytest
from sqlalchemy import func, select

from models.db import NULL_ACTOR_ID, CostActorDaily, CostDaily, CostEvent, TenantConfig
from observability import metrics as om
from services.aggregator import rollup_daily
from services.rollup_engine import daily_actor_axis_cardinality
from tests.conftest import T


async def _seed_events(factory, tenant: str) -> tuple[uuid.UUID, uuid.UUID]:
    yesterday = (datetime.now(UTC) - timedelta(days=1)).date()
    ts = datetime.combine(yesterday, datetime.min.time(), tzinfo=UTC)
    lic, sub = uuid.uuid4(), uuid.uuid4()
    alice, bob = uuid.uuid4(), uuid.uuid4()

    def ev(actor, cost, vendor=None, byo=False):
        return CostEvent(
            tenant_id=tenant,
            license_id=lic,
            subject_id=sub,
            actor_id=actor,
            service_key="commonpractice",  # pragma: allowlist secret
            provider="openrouter",
            served_model="m",
            input_tokens=10,
            output_tokens=2,
            cost_usd=Decimal(cost),
            vendor_cost_usd=None if vendor is None else Decimal(vendor),
            cost_source="commonpractice",
            usage_extra={"byo": True} if byo else {},
            created_at=ts,
        )

    async with factory() as s:
        s.add_all(
            [
                ev(alice, "1.00", vendor="0.90", byo=True),
                ev(alice, "2.00", byo=True),
                ev(bob, "4.00", byo=True),
                ev(None, "8.00"),  # platform work — no actor
            ]
        )
        await s.commit()
    return alice, bob


async def test_the_actor_rollup_reconciles_with_cost_daily_and_keeps_every_actor(
    db_session_factory,
):
    tenant = T("t-actor-rollup")
    alice, bob = await _seed_events(db_session_factory, tenant)

    await rollup_daily(db_session_factory)

    async with db_session_factory() as s:
        actor_rows = (
            (await s.execute(select(CostActorDaily).where(CostActorDaily.tenant_id == tenant)))
            .scalars()
            .all()
        )
        daily_total = (
            await s.execute(
                select(func.sum(CostDaily.total_cost_usd)).where(CostDaily.tenant_id == tenant)
            )
        ).scalar_one()

    by_actor: dict[uuid.UUID, Decimal] = {}
    for r in actor_rows:
        by_actor[r.actor_id] = by_actor.get(r.actor_id, Decimal("0")) + r.total_cost_usd
    assert by_actor[alice] == Decimal("3.00")
    assert by_actor[bob] == Decimal("4.00")
    # No person acted: the sentinel, never dropped and never attributed to someone.
    assert by_actor[NULL_ACTOR_ID] == Decimal("8.00")
    # ⛔ THE RECONCILIATION: the same money split by actor, not a second population.
    assert sum(by_actor.values()) == daily_total == Decimal("15.00")


async def test_vendor_cost_survives_the_rollup_and_null_is_not_zero(db_session_factory):
    tenant = T("t-vendor-rollup")
    await _seed_events(db_session_factory, tenant)

    await rollup_daily(db_session_factory)

    async with db_session_factory() as s:
        rows = (
            (await s.execute(select(CostDaily).where(CostDaily.tenant_id == tenant)))
            .scalars()
            .all()
        )
    byo_row = next(r for r in rows if r.byo)
    platform_row = next(r for r in rows if not r.byo)
    # Only one BYO event reported a vendor cost; the sum carries it.
    assert byo_row.total_vendor_cost_usd == Decimal("0.90")
    # No event in the group reported one: NULL, never a fabricated $0.
    assert platform_row.total_vendor_cost_usd is None


async def test_actor_endpoint_returns_per_actor_lines(db_engine, db_session_factory, app_client):
    tenant = T("t-actor-api")
    alice, bob = await _seed_events(db_session_factory, tenant)
    await rollup_daily(db_session_factory)

    start = (datetime.now(UTC) - timedelta(days=3)).isoformat()
    r = app_client.get(f"/api/costs/tenant/{tenant}/actors", params={"start": start})
    assert r.status_code == 200, r.text
    data = r.json()["data"]
    lines = {line["actor_id"]: line for line in data["by_actor"]}
    assert Decimal(lines[str(alice)]["cost_usd"]) == Decimal("3.00")
    assert Decimal(lines[str(alice)]["vendor_cost_usd"]) == Decimal("0.90")
    assert lines[str(bob)]["vendor_cost_usd"] is None
    assert Decimal(data["total_cost_usd"]) == Decimal("15.00")


@pytest.fixture
def _licences_answers_byo_mode(monkeypatch):
    """Answer the licences ``byo-mode`` endpoints, URL-scoped, delegating everything else.

    ⚠️ A DEPENDENCY THIS TEST ACQUIRED, and it is the per-app BYO change making an existing
    accident visible rather than a new fragility. `test_settings.licenses_url` is
    `http://localhost:9999`, where nothing listens: the TENANT-WIDE lookup absorbed that
    ConnectError into its revenue-safe default, so this file has been pricing against a GUESSED
    billing mode. The per-app lookup for a `commonpractice` line refuses to guess — an outage that
    silently prices a BYO line at the tenant-wide mode is the under-bill the change exists to
    close — so the dependency now has to be stated. Stating it is the improvement: this test
    exercises the real 200 path instead of a swallowed connection failure.

    Same URL-scoped, delegating shape as `tests/test_invoices.py::_licences_answers_byo_mode`, and
    never a blanket stub: every other GET is handed to the real method, so this cannot silently
    satisfy another test's expectation.
    """
    from services import byo_mode as byo

    byo.clear_byo_cache()
    real_get = httpx.AsyncClient.get

    async def _get(self, url, *a, **kw):  # noqa: ANN001, ANN202
        if "byo-mode" in str(url):
            return httpx.Response(200, json={"byo_mode": "byo_required"})
        return await real_get(self, url, *a, **kw)

    monkeypatch.setattr(httpx.AsyncClient, "get", _get)
    yield
    byo.clear_byo_cache()


async def test_billing_prefers_the_vendor_cost_on_byo_lines_only(
    db_session, test_settings, _licences_answers_byo_mode
):
    """A BYO daily row with a provider-reported cost is costed at THAT figure; a platform row with
    one keeps its price card. MUTATION TARGET: drop the case() in generate_billing and the BYO
    reference cost reads 10.00, not 9.00."""
    from services.billing import generate_billing

    tenant = T("t-vendor-pref")
    today = datetime.now(UTC).date()
    month = today.strftime("%Y-%m")
    lic = uuid.uuid4()
    db_session.add(
        TenantConfig(
            tenant_id=tenant,
            tenant_name="vp",
            markup_pct=Decimal("20"),
            billing_mode="byo_markup_only",
        )
    )
    for byo, cost, vendor in ((True, "10", "9"), (False, "10", "7")):
        db_session.add(
            CostDaily(
                tenant_id=tenant,
                tenant_name="vp",
                license_id=lic,
                service_key="commonpractice",  # pragma: allowlist secret
                provider="openrouter" if byo else "local",
                served_model="m",
                byo=byo,
                cost_source="commonpractice",
                bucket_date=today,
                total_input_tokens=1,
                total_output_tokens=1,
                total_cost_usd=Decimal(cost),
                total_vendor_cost_usd=Decimal(vendor),
                request_count=1,
            )
        )
    await db_session.commit()

    # ⚠️ THE LICENCE ENRICHMENT IS STUBBED, and the reason is the point of the per-app BYO change.
    # `generate_billing` derives a CommonPractice line's LICENCES app key from this payload; with
    # the real `fetch_licenses` unreachable at `localhost:9999` the enrichment is DEGRADED, and a
    # degraded enrichment plus a CP line now REFUSES (`ByoModeUnavailableError`) instead of pricing
    # the line at the tenant-wide mode. That refusal is the guard, not a fixture inconvenience —
    # silently billing a BYO line at the tenant-wide posture is the under-bill it exists to stop.
    # ⛔ The app key here is deliberately NOT the `service_key`: they are different namespaces, and
    # in production they merely happen to coincide (measured: licences `app.key = commonpractice`).
    payload = [{"id": str(lic), "app": {"key": "common-practice"}, "plan": {"code": "pro"}}]
    with patch("services.billing.fetch_licenses", new_callable=AsyncMock) as fetch:
        fetch.return_value = payload
        totals = (await generate_billing(tenant, month, db_session, test_settings))["totals"]
    assert Decimal(totals["byo_reference_cost_usd"]) == Decimal("9.00")
    # The platform line keeps its price card as internal cost.
    assert Decimal(totals["internal_cost_usd"]) == Decimal("10.00")


async def test_actor_axis_cardinality_counts_real_actors_never_the_sentinel(db_session_factory):
    """billing_cost_actor_daily_distinct_actors must read the REAL-actor axis, not the sentinel.

    ⭐ THE COLLAPSE THIS GUARDS. ``actor_id`` on the raw event is nullable and never defaulted
    (alembic 0021) -- most emitters do not send it yet, so it coalesces to ``NULL_ACTOR_ID`` in
    the rollup. A count that does not exclude that sentinel reports "3 actors" for tenant fixture
    below (alice, bob, the sentinel) when the true onboarded-actor count is 2 -- indistinguishable
    from a genuinely healthy axis until someone checks which 3 values they were.
    """
    tenant = T("t-actor-cardinality")
    alice, bob = await _seed_events(db_session_factory, tenant)
    await rollup_daily(db_session_factory)

    distinct = await daily_actor_axis_cardinality(db_session_factory)

    # db_engine TRUNCATEs every table before each test, so exactly alice and bob exist here.
    assert distinct == 2, "alice and bob, never the sentinel row's own value"
    assert om.cost_actor_daily_distinct_actors._value.get() == distinct, (
        "the gauge must be published, not just returned"
    )

    async with db_session_factory() as s:
        real_actor_ids = set(
            (
                await s.execute(
                    select(CostActorDaily.actor_id).where(CostActorDaily.actor_id != NULL_ACTOR_ID)
                )
            )
            .scalars()
            .all()
        )
    assert {alice, bob} <= real_actor_ids
    assert NULL_ACTOR_ID not in real_actor_ids, "the sentinel must never be counted as a real actor"


async def test_actor_axis_cardinality_reads_zero_when_only_the_sentinel_exists(db_session_factory):
    """A fleet with no actor-sending emitter yet must read a measured 0, not an absent gauge.

    Seeds ONLY platform-attributed spend (no actor on the wire) so every row in
    ``billing_cost_actor_daily`` coalesces to ``NULL_ACTOR_ID``, and asserts the gauge reads
    exactly 0 -- the correct, expected value for that state, per the gauge's own HELP text.
    """
    tenant = T("t-actor-cardinality-zero")
    yesterday = (datetime.now(UTC) - timedelta(days=1)).date()
    ts = datetime.combine(yesterday, datetime.min.time(), tzinfo=UTC)
    async with db_session_factory() as s:
        s.add(
            CostEvent(
                tenant_id=tenant,
                license_id=uuid.uuid4(),
                actor_id=None,
                service_key="commonpractice",  # pragma: allowlist secret
                provider="openrouter",
                served_model="m",
                input_tokens=10,
                output_tokens=2,
                cost_usd=Decimal("5.00"),
                cost_source="commonpractice",
                created_at=ts,
            )
        )
        await s.commit()

    await rollup_daily(db_session_factory)

    async with db_session_factory() as s:
        tenant_actor_ids = set(
            (
                await s.execute(
                    select(CostActorDaily.actor_id).where(CostActorDaily.tenant_id == tenant)
                )
            )
            .scalars()
            .all()
        )
    assert tenant_actor_ids == {NULL_ACTOR_ID}, "every row for this tenant must be sentinel-only"

    # db_engine TRUNCATEs every table before each test, so the actor table holds ONLY this
    # tenant's sentinel-only rows here — the global count must read exactly 0.
    distinct = await daily_actor_axis_cardinality(db_session_factory)
    assert distinct == 0, "no real actor exists yet; the sentinel must not be counted as one"
    assert om.cost_actor_daily_distinct_actors._value.get() == 0
