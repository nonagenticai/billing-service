"""Router-vouched delegation is counted apart from licence/tenant mismatch.

inference-router honours a delegated call -- a SERVICE licence calling on behalf
of an END tenant -- only when the vouched subject is in its
``AUTH_DELEGATING_SUBJECTS`` allowlist, and stamps ``delegated_by`` (the vouched
subject id) on that cost event and on no other. Such a row carries
``license_id = service licence`` and ``tenant_id = end tenant`` BY DESIGN, so
scoring it as a mismatch made ``BillingMisattributedSpendIncreased`` fire on
correct, intended traffic -- and an alert that fires on the intended shape is
one that gets silenced, taking the real mismatches down with it.

⛔ ONLY THE ROUTER MAY VOUCH. ``delegated_by`` from any other ``cost_source`` is
ignored and the row stays a mismatch: the key is a whitelisted wire field, so
any emitter can send it, and honouring it from an emitter that does not run the
allowlist check would turn a metadata key into a mismatch-alert bypass.

The SQL half is graded against real Postgres (``->>`` on a ``json`` column,
``IS NOT NULL`` / ``<> ''`` three-valued logic and the GROUP BY are Postgres'
behaviour, not Python's); the classification half is a pure function.
"""

from __future__ import annotations

import json
import uuid
from datetime import UTC, datetime, timedelta
from decimal import Decimal

import pytest
from sqlalchemy import select, text

from models.db import CostEvent
from services import attribution_audit as aa
from services._parsers import COST_METADATA_WHITELIST, DELEGATED_BY_MAX_LEN
from services.aggregator import flush_costs
from services.attribution_audit import classify_attribution
from tests._aggregator_helpers import _make_cost
from tests.conftest import T

_INSERT = text(
    "INSERT INTO billing_cost_events"
    " (id, tenant_id, tenant_name, license_id, service_key, provider, cost_source,"
    "  input_tokens, output_tokens, cost_usd,"
    "  excluded_from_billing, metadata, created_at)"
    " VALUES (:id, :tenant_id, 'acme', :license_id, 'inference-router', 'pgx-local',"
    "  :cost_source, 100, 50, :cost_usd, false, CAST(:meta AS json), :created_at)"
)


async def _insert(
    session,
    *,
    tenant: str,
    licence: str,
    cost: str,
    cost_source: str = "inference-router",
    meta: dict | None = None,
) -> None:
    await session.execute(
        _INSERT,
        {
            "id": uuid.uuid4(),
            "tenant_id": tenant,
            "license_id": licence,
            "cost_source": cost_source,
            "cost_usd": Decimal(cost),
            "meta": json.dumps(meta) if meta is not None else None,
            "created_at": datetime.now(UTC) - timedelta(hours=1),
        },
    )


async def _window(session) -> list:
    floor = datetime.now(UTC) - timedelta(hours=24)
    return (await session.execute(aa._WINDOW_SQL, {"floor": floor})).all()


def test_whitelist_keeps_delegated_by() -> None:
    """A key the whitelist omits is silently dropped by the flusher, and the
    row still looks correct -- so the receiving half must land first."""
    assert "delegated_by" in COST_METADATA_WHITELIST


@pytest.mark.asyncio
async def test_flusher_stores_a_subject_id_and_drops_an_oversized_one(db_session_factory) -> None:
    ok = "svc-strata-rag"
    buffer = [
        _make_cost(tenant_id=T("deleg-ok"), delegated_by=ok, schema_id="s"),
        _make_cost(
            tenant_id=T("deleg-long"), delegated_by="x" * (DELEGATED_BY_MAX_LEN + 1), schema_id="s"
        ),
    ]
    assert await flush_costs(buffer, db_session_factory, None) == 2

    async with db_session_factory() as session:
        rows = {str(r.tenant_id): r for r in (await session.execute(select(CostEvent))).scalars()}
    ok_meta = rows[T("deleg-ok")].extra_metadata
    long_meta = rows[T("deleg-long")].extra_metadata
    assert ok_meta is not None and ok_meta["delegated_by"] == ok
    # CONTROL: the row itself survived, only the key was refused.
    assert long_meta is not None and long_meta["schema_id"] == "s"
    assert "delegated_by" not in long_meta


@pytest.mark.asyncio
async def test_window_marks_only_router_vouched_rows_as_delegated(db_session) -> None:
    """Same licence, same tenant, three emit shapes -> three distinct groups.

    Costs are powers of two so each group's spend NAMES the rows in it.
    """
    licence, end_tenant = str(uuid.uuid4()), str(uuid.uuid4())
    vouched = {"delegated_by": "svc-strata-rag"}

    await _insert(db_session, tenant=end_tenant, licence=licence, cost="1.00", meta=vouched)
    await _insert(db_session, tenant=end_tenant, licence=licence, cost="2.00", meta=vouched)
    # The bypass attempt: same metadata, NOT the router.
    await _insert(
        db_session,
        tenant=end_tenant,
        licence=licence,
        cost="4.00",
        cost_source="worker-management",
        meta=vouched,
    )
    # Plain undelegated router row, and an empty-string delegated_by.
    await _insert(db_session, tenant=end_tenant, licence=licence, cost="8.00")
    await _insert(
        db_session, tenant=end_tenant, licence=licence, cost="16.00", meta={"delegated_by": ""}
    )
    await db_session.commit()

    groups = {
        bool(r.delegated): (int(r.events), float(r.spend))
        for r in await _window(db_session)
        if r.license_id == licence
    }

    assert groups[True] == (2, pytest.approx(3.00))
    # 4 + 8 + 16: the non-router vouch and the empty vouch stay undelegated.
    assert groups[False] == (3, pytest.approx(28.00))


@pytest.mark.asyncio
async def test_delegated_router_rows_reach_the_audit_as_delegated(db_session) -> None:
    """End to end through the real SQL: window rows -> classify_attribution."""
    licence, owner, end_tenant = str(uuid.uuid4()), str(uuid.uuid4()), str(uuid.uuid4())
    vouched = {"delegated_by": "svc-strata-rag"}
    await _insert(db_session, tenant=end_tenant, licence=licence, cost="1.00", meta=vouched)
    await _insert(
        db_session,
        tenant=end_tenant,
        licence=licence,
        cost="4.00",
        cost_source="worker-management",
        meta=vouched,
    )
    await db_session.commit()

    rows = [
        (r.license_id, r.tenant_id, int(r.events), float(r.spend), bool(r.delegated))
        for r in await _window(db_session)
        if r.license_id == licence
    ]
    audit = classify_attribution(rows=rows, owner_by_license={licence: owner})

    assert audit.delegated_events == 1
    assert audit.delegated_spend_usd == pytest.approx(1.00)
    assert audit.mismatch_events == 1
    assert audit.mismatch_spend_usd == pytest.approx(4.00)


class TestClassifyDelegated:
    def test_delegated_mismatch_is_delegated_not_mismatch(self) -> None:
        lic, owner, end_tenant = str(uuid.uuid4()), str(uuid.uuid4()), str(uuid.uuid4())
        audit = classify_attribution(
            rows=[(lic, end_tenant, 7, 3.5, True)],
            owner_by_license={lic: owner},
        )
        assert audit.mismatch_events == 0
        assert audit.mismatch_spend_usd == pytest.approx(0.0)
        assert audit.delegated_events == 7
        assert audit.delegated_spend_usd == pytest.approx(3.5)
        assert audit.top_pairs == []
        assert audit.top_delegated_pairs == [(lic, end_tenant, owner, 7)]
        assert audit.total_events == 7

    def test_undelegated_mismatch_is_unchanged(self) -> None:
        lic, owner, billed = str(uuid.uuid4()), str(uuid.uuid4()), str(uuid.uuid4())
        for row in ((lic, billed, 5, 2.0), (lic, billed, 5, 2.0, False)):
            audit = classify_attribution(rows=[row], owner_by_license={lic: owner})
            assert audit.mismatch_events == 5
            assert audit.mismatch_spend_usd == pytest.approx(2.0)
            assert audit.delegated_events == 0
            assert audit.top_pairs == [(lic, billed, owner, 5)]
            assert audit.top_delegated_pairs == []

    def test_delegated_flag_does_not_change_matched_or_unresolved(self) -> None:
        lic, tenant = str(uuid.uuid4()), str(uuid.uuid4())
        matched = classify_attribution(
            rows=[(lic, tenant, 3, 1.0, True)], owner_by_license={lic: tenant}
        )
        assert matched.matched_events == 3
        assert matched.delegated_events == 0
        unresolved = classify_attribution(rows=[(lic, tenant, 4, 1.0, True)], owner_by_license={})
        assert unresolved.unresolved_events == 4
        assert unresolved.delegated_events == 0
