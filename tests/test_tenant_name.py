"""Tests for the Phase 3 tenant_name column on all metering tables.

Covers:
- Schema: every tenant-scoped table has tenant_name VARCHAR(255), NOT NULL
          (except audit_log where it is nullable matching tenant_id).
- Aggregator: flush_events carries tenant_name through to metering_events.
- Aggregator: flush_costs carries tenant_name through to cost_events.
- Rollup: rollup_hourly propagates tenant_name from metering_events to metering_hourly.
- API response: UsageResponse and CostResponse carry tenant_name.
"""

from __future__ import annotations

import uuid
from typing import cast

import pytest
from sqlalchemy import String, Table, inspect, select

from models.db import (
    AuditLog,
    CostDaily,
    CostEvent,
    MeteringDaily,
    MeteringEvent,
    MeteringHourly,
    TenantConfig,
    TokenBudget,
    TokenUsage,
)
from tests.conftest import T

# ---------------------------------------------------------------------------
# Schema assertions (pure ORM introspection — no DB required)
# ---------------------------------------------------------------------------


class TestTenantNameSchema:
    """Verify the ORM models declare tenant_name on every tenant-scoped table."""

    @pytest.mark.unit
    @pytest.mark.parametrize(
        "model,nullable",
        [
            (MeteringEvent, False),
            (CostEvent, False),
            (MeteringHourly, False),
            (MeteringDaily, False),
            (CostDaily, False),
            (TokenBudget, False),
            (TokenUsage, False),
            (TenantConfig, False),
            (AuditLog, True),  # audit_log.tenant_id is nullable → tenant_name is too
        ],
    )
    def test_tenant_name_column_present(self, model, nullable: bool) -> None:
        table = cast(Table, model.__table__)
        assert "tenant_name" in table.columns, f"{model.__tablename__} missing tenant_name column"
        col = table.columns["tenant_name"]
        assert col.nullable is nullable, (
            f"{model.__tablename__}.tenant_name nullable mismatch: "
            f"expected {nullable}, got {col.nullable}"
        )
        assert cast(String, col.type).length == 255

    @pytest.mark.unit
    @pytest.mark.parametrize(
        "model,idx_name",
        [
            (MeteringEvent, "ix_metering_events_tenant_name"),
            (CostEvent, "ix_cost_events_tenant_name"),
            (MeteringHourly, "ix_metering_hourly_tenant_name"),
            (MeteringDaily, "ix_metering_daily_tenant_name"),
            (CostDaily, "ix_cost_daily_tenant_name"),
            (TokenBudget, "ix_token_budgets_tenant_name"),
            (TokenUsage, "ix_token_usage_tenant_name"),
            (AuditLog, "ix_audit_log_tenant_name"),
        ],
    )
    def test_tenant_name_index_present(self, model, idx_name: str) -> None:
        table = cast(Table, model.__table__)
        idx_names = {ix.name for ix in table.indexes}
        assert idx_name in idx_names, (
            f"{model.__tablename__} missing index {idx_name}; found: {idx_names}"
        )


# ---------------------------------------------------------------------------
# DB integration — schema actually applies to Postgres
# ---------------------------------------------------------------------------


class TestTenantNameInDatabase:
    @pytest.mark.unit
    async def test_metering_events_has_tenant_name_column(self, db_engine) -> None:
        async with db_engine.connect() as conn:
            cols = await conn.run_sync(lambda c: inspect(c).get_columns("billing_metering_events"))
        names = {c["name"] for c in cols}
        assert "tenant_name" in names

    @pytest.mark.unit
    async def test_cost_events_has_tenant_name_column(self, db_engine) -> None:
        async with db_engine.connect() as conn:
            cols = await conn.run_sync(lambda c: inspect(c).get_columns("billing_cost_events"))
        names = {c["name"] for c in cols}
        assert "tenant_name" in names

    @pytest.mark.unit
    async def test_metering_hourly_has_tenant_name_column(self, db_engine) -> None:
        async with db_engine.connect() as conn:
            cols = await conn.run_sync(lambda c: inspect(c).get_columns("billing_metering_hourly"))
        names = {c["name"] for c in cols}
        assert "tenant_name" in names

    @pytest.mark.unit
    async def test_token_budgets_has_tenant_name_column(self, db_engine) -> None:
        async with db_engine.connect() as conn:
            cols = await conn.run_sync(lambda c: inspect(c).get_columns("token_budgets"))
        names = {c["name"] for c in cols}
        assert "tenant_name" in names

    @pytest.mark.unit
    async def test_token_usage_has_tenant_name_column(self, db_engine) -> None:
        async with db_engine.connect() as conn:
            cols = await conn.run_sync(lambda c: inspect(c).get_columns("token_usage"))
        names = {c["name"] for c in cols}
        assert "tenant_name" in names


# ---------------------------------------------------------------------------
# Aggregator: flush_events persists tenant_name
# ---------------------------------------------------------------------------


class TestFlushEventsTenantName:
    @pytest.mark.unit
    async def test_tenant_name_written_to_metering_events(self, db_session_factory) -> None:
        """flush_events stores tenant_name from the payload in metering_events."""
        from services.aggregator import flush_events

        event = {
            "tenant_id": T("some-uuid-placeholder"),
            "tenant_name": "n8n-dev",
            "license_id": str(uuid.uuid4()),
            "app_id": str(uuid.uuid4()),
            "plan_id": str(uuid.uuid4()),
            "method": "GET",
            "path": "/workers/api/tasks",
            "status": "allowed",
            "_service_key": "workers",
        }

        count = await flush_events([event], db_session_factory, None)
        assert count == 1

        async with db_session_factory() as session:
            result = await session.execute(select(MeteringEvent))
            row = result.scalars().first()
            assert row is not None
            assert row.tenant_name == "n8n-dev"
            assert row.tenant_id == T("some-uuid-placeholder")

    # An unresolvable label is the SUBJECT here: tenant_id sinks to the nil
    # tenant while the human label survives in tenant_name. That is the 0011
    # contract, so the nil row is intended, not a missed T() conversion.
    @pytest.mark.nil_tenant
    @pytest.mark.unit
    async def test_tenant_name_falls_back_to_tenant_id_when_absent(
        self, db_session_factory
    ) -> None:
        """When tenant_name is absent, flush_events defaults to tenant_id."""
        from services.aggregator import flush_events

        event = {
            "tenant_id": "fallback-tenant",
            # no tenant_name key
            "license_id": str(uuid.uuid4()),
            "app_id": str(uuid.uuid4()),
            "plan_id": str(uuid.uuid4()),
            "method": "POST",
            "path": "/kanban/api/boards",
            "status": "allowed",
            "_service_key": "kanban",
        }

        await flush_events([event], db_session_factory, None)

        async with db_session_factory() as session:
            result = await session.execute(select(MeteringEvent))
            row = result.scalars().first()
            assert row is not None
            # Falls back to tenant_id when tenant_name absent/empty
            assert row.tenant_name == "fallback-tenant"


# ---------------------------------------------------------------------------
# Aggregator: flush_costs persists tenant_name
# ---------------------------------------------------------------------------


class TestFlushCostsTenantName:
    @pytest.mark.unit
    async def test_tenant_name_written_to_cost_events(self, db_session_factory) -> None:
        """flush_costs stores tenant_name from the payload in cost_events."""
        from services.aggregator import flush_costs

        cost = {
            "tenant_id": T("some-uuid-placeholder"),
            "tenant_name": "n8n-dev",
            "license_id": str(uuid.uuid4()),
            "provider": "claude",
            "model": "claude-sonnet-4-20250514",
            "served_model": "claude-sonnet-4-20250514",
            "input_tokens": "500",
            "output_tokens": "100",
            "cost_usd": "0.005",
            "service_key": "inference-router",
        }

        count = await flush_costs([cost], db_session_factory, None)
        assert count == 1

        async with db_session_factory() as session:
            result = await session.execute(select(CostEvent))
            row = result.scalars().first()
            assert row is not None
            assert row.tenant_name == "n8n-dev"
            assert row.tenant_id == T("some-uuid-placeholder")

    @pytest.mark.unit
    async def test_cost_tenant_name_falls_back_to_tenant_id(self, db_session_factory) -> None:
        """When tenant_name absent in cost payload, falls back to tenant_id.

        CONTRACT CHANGE (2026-08-26): the tenant used to be the unresolvable
        label "fallback-cost-tenant", which made this a nil-sink test — the row
        landed on the nil tenant and was never billed to anyone. Such an event is
        now dead-lettered by ``_cost_schema.validate_cost_event``, so the subject
        (does an absent tenant_name fall back to the emitted identifier?) is
        pinned with a REGISTERED legacy name instead. That is still a fallback
        from a non-uuid wire value, it is still the display column being tested,
        and the event is genuinely billable — so the test no longer needs
        ``@pytest.mark.nil_tenant``.
        """
        from services.aggregator import flush_costs

        cost = {
            "tenant_id": "n8n-dev",
            # no tenant_name
            "license_id": str(uuid.uuid4()),
            "provider": "openai",
            "model": "gpt-4o",
            "input_tokens": "200",
            "output_tokens": "50",
            "cost_usd": "0.001",
            "service_key": "inference-router",
        }

        await flush_costs([cost], db_session_factory, None)

        async with db_session_factory() as session:
            result = await session.execute(select(CostEvent))
            row = result.scalars().first()
            assert row is not None
            assert row.tenant_name == "n8n-dev"


# ---------------------------------------------------------------------------
# billing-service #135 — the uuid must not masquerade as a name
# ---------------------------------------------------------------------------


class TestTenantNameIsNeverTheUuid:
    """``tenant_name`` must hold a name or an honest absence — never the id.

    THE DEFECT (#135, on ``billing_metering_events``; the same line had it on
    ``billing_cost_events``). Since ``0011_tenant_id_is_a_uuid`` the emitters
    send a uuid as ``tenant_id`` and no ``tenant_name``, so both flushers'
    fallback — ``original_tenant_label``, which faithfully preserves whatever was
    on the wire — preserved the UUID into a display column. Measured 2026-09-05:
    9 of 10 tenants and 4,469 metering rows, with ZERO honest absences ever
    written, plus 66,103 of 66,103 cost rows.

    WHY A UUID IS WORSE THAN AN EMPTY STRING, and why this class exists rather
    than a looser "it is not the uuid" assertion elsewhere: a uuid is non-empty
    AND distinct per tenant, so it satisfies every downstream truthiness guard
    and reads as data. ``fr``'s metering page guards its id column with
    ``{#if t.tenant_name}``; the fake name passed the guard and the page printed
    the same id twice. fr narrowed it server-side (fr#980), but the stored data
    was still wrong for every other consumer.

    NULL is not available: ``billing_metering_events.tenant_name`` is ``NOT NULL``
    with no default (alembic 0001), so ``""`` is the honest value that exists
    today — and it renders identically, because ``resolve_tenant_name`` already
    discards a stored name equal to the uuid.
    """

    @pytest.fixture(autouse=True)
    def _clean_overlay(self):
        import tenancy

        tenancy.reset_live_tenant_registry()
        yield
        tenancy.reset_live_tenant_registry()

    REGISTERED_UUID = "01a0cccc-0000-7000-8000-0000000000c1"

    def _event(self, tenant_id: str, **extra) -> dict:
        e = {
            "tenant_id": tenant_id,
            "license_id": str(uuid.uuid4()),
            "app_id": str(uuid.uuid4()),
            "plan_id": str(uuid.uuid4()),
            "method": "GET",
            "path": "/workers/api/tasks",
            "status": "allowed",
            "_service_key": "workers",
        }
        e.update(extra)
        return e

    @pytest.mark.unit
    async def test_metering_event_stores_an_absence_not_the_uuid(self, db_session_factory) -> None:
        """The reported defect, on the reported table."""
        from services.aggregator import flush_events

        tenant = T("metering-no-name")
        assert await flush_events([self._event(tenant)], db_session_factory, None) == 1

        async with db_session_factory() as session:
            row = (await session.execute(select(MeteringEvent))).scalars().one()
        assert row.tenant_id == tenant
        assert row.tenant_name != row.tenant_id, "the uuid is masquerading as a name again"
        assert row.tenant_name == ""

    @pytest.mark.unit
    async def test_the_stored_absence_is_falsy_for_a_downstream_guard(
        self, db_session_factory
    ) -> None:
        """The property fr's ``{#if t.tenant_name}`` actually needs.

        Asserted as FALSINESS rather than as the literal ``""`` so a later change
        to a different honest absence (NULL, after a nullability migration) still
        satisfies the consumer this defect broke.
        """
        from services.aggregator import flush_events

        await flush_events([self._event(T("metering-falsy"))], db_session_factory, None)
        async with db_session_factory() as session:
            row = (await session.execute(select(MeteringEvent))).scalars().one()
        assert not row.tenant_name

    @pytest.mark.unit
    async def test_metering_event_resolves_the_real_name_from_the_registry(
        self, db_session_factory
    ) -> None:
        """The other half: when the registry knows the tenant, store the NAME."""
        import tenancy
        from services.aggregator import flush_events

        tenancy.set_live_tenant_registry({"acme-corp": self.REGISTERED_UUID})
        await flush_events([self._event(self.REGISTERED_UUID)], db_session_factory, None)

        async with db_session_factory() as session:
            row = (await session.execute(select(MeteringEvent))).scalars().one()
        assert row.tenant_name == "acme-corp"

    @pytest.mark.unit
    async def test_both_write_paths_share_one_helper(self) -> None:
        """ONE defect, one fix. Pins the shared point against a re-inline.

        The two flushers carried the identical expression and therefore the
        identical defect. If either stops calling the shared helper, one table
        silently regresses while the other's tests stay green — which is exactly
        how this survived being fixed on the read paths.
        """
        import inspect

        import services.cost_flusher as cf
        import services.event_flusher as ef

        assert "stored_tenant_name(" in inspect.getsource(cf.flush_costs)
        assert "stored_tenant_name(" in inspect.getsource(ef.flush_events)

    @pytest.mark.unit
    async def test_an_unresolved_metering_name_is_counted(self, db_session_factory) -> None:
        """Distinguishable, on the metering stream's OWN counter.

        Deliberately not ``billing_placeholder_value_total``: that counter's help
        text names a COST denominator, so counting metering rows there would put
        two populations on one series and make every rate against it wrong.
        """
        import observability.metrics as om
        from services.aggregator import flush_events

        def _counter() -> float:
            return om.metering_placeholder_value_total.labels(
                field="tenant_name", service_key="workers"
            )._value.get()

        before = _counter()
        await flush_events([self._event(T("metering-counted"))], db_session_factory, None)
        assert _counter() - before == 1

    @pytest.mark.unit
    async def test_a_resolved_metering_name_is_not_counted(self, db_session_factory) -> None:
        """The control. A counter that fires on every row is a constant."""
        import observability.metrics as om
        import tenancy
        from services.aggregator import flush_events

        tenancy.set_live_tenant_registry({"acme-corp": self.REGISTERED_UUID})

        def _counter() -> float:
            return om.metering_placeholder_value_total.labels(
                field="tenant_name", service_key="workers"
            )._value.get()

        before = _counter()
        await flush_events([self._event(self.REGISTERED_UUID)], db_session_factory, None)
        assert _counter() - before == 0

    @pytest.mark.unit
    async def test_an_emitter_sentinel_is_kept_verbatim_not_coerced(
        self, db_session_factory
    ) -> None:
        """⚠️ ONLY the uuid is coerced. Every other sentinel is preserved.

        ``billing_placeholder_value_total``'s own help text is explicit that
        nulling a sentinel would move the laundering from "a fake value" to "an
        honest-looking absence" rather than end it. So an emitter that literally
        writes "unknown" still gets "unknown" stored — and counted. The uuid is
        different in kind: it does not stand in for a name, it IMPERSONATES one.
        """
        from services.aggregator import flush_events

        await flush_events(
            [self._event(T("metering-sentinel"), tenant_name="unknown")],
            db_session_factory,
            None,
        )
        async with db_session_factory() as session:
            row = (await session.execute(select(MeteringEvent))).scalars().one()
        assert row.tenant_name == "unknown"

    # The nil sink is the PRECONDITION here, not an accident: a credential is
    # unresolvable, so tenant_id sinks to NIL_TENANT_ID on the usage stream
    # (which, unlike the cost stream, does not dead-letter) while the human label
    # survives redacted in the free-text column. Resolving the tenant would
    # remove the very branch under test.
    @pytest.mark.nil_tenant
    @pytest.mark.unit
    async def test_a_redacted_credential_label_still_survives(self, db_session_factory) -> None:
        """The redaction path must not be collapsed into the absence path.

        ``flush_events`` does not dead-letter an unresolvable tenant the way
        ``flush_costs`` does — it sinks the id to the nil tenant and keeps the
        human label. A leaked auth header must therefore still land REDACTED in
        tenant_name (never verbatim), not be silently blanked, or the DLQ-less
        metering stream loses the only signal naming the leaking emitter.
        """
        from services.aggregator import flush_events

        # Deliberately NOT token-shaped, unlike the copy in test_cost_flusher.py:
        # `looks_like_credential` trips on the "bearer" PREFIX alone, so the
        # synthetic body exercises the identical branch without adding another
        # entropy-scanner hit (and another opaque .secrets.baseline entry) for a
        # value that is not a secret.
        leaked = "Bearer THIS-IS-NOT-A-REAL-TOKEN-only-long-enough-to-redact"
        await flush_events([self._event(leaked)], db_session_factory, None)

        async with db_session_factory() as session:
            row = (await session.execute(select(MeteringEvent))).scalars().one()
        assert row.tenant_name.startswith(leaked[:8])
        assert row.tenant_name.endswith(f"(redacted,len={len(leaked)})")
        assert leaked[:16] not in row.tenant_name
