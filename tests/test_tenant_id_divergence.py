"""services.tenant_id_divergence — the dollar-denominated divergence gauge + ledger.

Two things under test, matching the module's own split:

1. ``_aggregate_divergence`` — pure. Proves rows and dollars can disagree by
   class (the estate fact this whole gauge exists to surface: one bucket can
   be most of the ROWS and almost none of the MONEY), and that a registered
   tenant never appears in the output.
2. ``record_tenant_id_divergence_ledger`` — DB-backed. Proves it records a
   divergent row, skips a registered one, and is idempotent on re-run — the
   three properties ``alembic/versions/0039_tenant_id_divergence.py`` and the
   ledger's own unique constraint exist to guarantee.
"""

from __future__ import annotations

import uuid
from decimal import Decimal

import pytest
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from models.db import AuditLog, CostEvent, TenantIdDivergenceAudit
from observability import metrics as observability_metrics
from services.tenant_id_divergence import (
    DEFAULT_LEDGER_TABLES,
    _aggregate_divergence,
    record_tenant_id_divergence_ledger,
    run_tenant_id_divergence_audit,
)
from tenancy import LEGACY_TENANT_NAMES, derived_tenant_id

REGISTERED_TENANT = LEGACY_TENANT_NAMES["fr-test"]


# ---------------------------------------------------------------------------
# _aggregate_divergence — pure
# ---------------------------------------------------------------------------


def test_rows_and_dollars_can_disagree_by_class() -> None:
    """The exact estate shape this gauge exists to surface.

    Many rows, almost no money (a 'v4' bucket) beside few rows, real money (a
    'v5_derived' bucket) — rows alone would rank them backwards.
    """
    many_row_tenant = str(uuid.uuid4())
    few_row_tenant = derived_tenant_id("r15-divergence-aggregate-test")
    per_tenant = [(many_row_tenant, 10_000, 0.02), (few_row_tenant, 2, 500.00)]

    snapshot = _aggregate_divergence(per_tenant)

    assert snapshot.rows_by_class["v4"] == 10_000
    assert snapshot.spend_by_class["v4"] == pytest.approx(0.02)
    assert snapshot.rows_by_class["v5_derived"] == 2
    assert snapshot.spend_by_class["v5_derived"] == pytest.approx(500.00)


def test_registered_tenant_is_excluded_from_every_bucket() -> None:
    per_tenant = [(REGISTERED_TENANT, 999, 999.99)]

    snapshot = _aggregate_divergence(per_tenant)

    assert sum(snapshot.rows_by_class.values()) == 0
    assert sum(snapshot.spend_by_class.values()) == 0.0
    assert snapshot.tenants_seen == 1


def test_every_divergence_class_is_preseeded_at_zero_on_an_empty_window() -> None:
    """A measured zero, not an absent key — the whole point of the dataclass default."""
    snapshot = _aggregate_divergence([])

    assert snapshot.rows_by_class == {
        "synthetic": 0,
        "v5_derived": 0,
        "v7": 0,
        "v4": 0,
        "other": 0,
    }
    assert set(snapshot.spend_by_class) == set(snapshot.rows_by_class)


def test_multiple_tenants_in_the_same_class_are_summed() -> None:
    tenant_a, tenant_b = str(uuid.uuid4()), str(uuid.uuid4())
    per_tenant = [(tenant_a, 3, 1.11), (tenant_b, 4, 2.22)]

    snapshot = _aggregate_divergence(per_tenant)

    assert snapshot.rows_by_class["v4"] == 7
    assert snapshot.spend_by_class["v4"] == pytest.approx(3.33)


# ---------------------------------------------------------------------------
# record_tenant_id_divergence_ledger — DB-backed, read-only against sources
# ---------------------------------------------------------------------------


@pytest.mark.unit
async def test_records_a_divergent_row(db_session: AsyncSession) -> None:
    divergent = str(uuid.uuid4())
    db_session.add(
        AuditLog(
            action="create",
            resource_type="token_budget",
            tenant_id=divergent,
        )
    )
    await db_session.commit()

    inserted = await record_tenant_id_divergence_ledger(db_session, tables=(("audit_log", "id"),))

    assert inserted == {"audit_log": 1}
    rows = list(
        (
            await db_session.execute(
                select(TenantIdDivergenceAudit).where(
                    TenantIdDivergenceAudit.tenant_id == divergent
                )
            )
        ).scalars()
    )
    assert len(rows) == 1
    assert rows[0].table_name == "audit_log"
    assert rows[0].class_detected == "v4"


@pytest.mark.unit
async def test_a_registered_tenant_is_never_recorded(db_session: AsyncSession) -> None:
    db_session.add(
        AuditLog(
            action="create",
            resource_type="token_budget",
            tenant_id=REGISTERED_TENANT,
        )
    )
    await db_session.commit()

    inserted = await record_tenant_id_divergence_ledger(db_session, tables=(("audit_log", "id"),))

    assert inserted == {"audit_log": 0}
    count = (
        (
            await db_session.execute(
                select(TenantIdDivergenceAudit).where(
                    TenantIdDivergenceAudit.tenant_id == REGISTERED_TENANT
                )
            )
        )
        .scalars()
        .all()
    )
    assert count == []


@pytest.mark.unit
async def test_recording_is_idempotent(db_session: AsyncSession) -> None:
    """Same (table_name, row_pk) recorded twice writes exactly one ledger row.

    Pins the unique constraint alembic 0039 puts on the ledger, and pins
    'never rewrites' — a second pass over an unchanged table records nothing
    new rather than erroring or duplicating.
    """
    divergent = str(uuid.uuid4())
    db_session.add(AuditLog(action="create", resource_type="token_budget", tenant_id=divergent))
    await db_session.commit()

    first = await record_tenant_id_divergence_ledger(db_session, tables=(("audit_log", "id"),))
    second = await record_tenant_id_divergence_ledger(db_session, tables=(("audit_log", "id"),))

    assert first == {"audit_log": 1}
    assert second == {"audit_log": 0}
    rows = (
        (
            await db_session.execute(
                select(TenantIdDivergenceAudit).where(
                    TenantIdDivergenceAudit.tenant_id == divergent
                )
            )
        )
        .scalars()
        .all()
    )
    assert len(rows) == 1


@pytest.mark.unit
async def test_never_writes_the_source_row_tenant_id(db_session: AsyncSession) -> None:
    """⛔ The backfill was refused. Recording must never touch the source column."""
    divergent = str(uuid.uuid4())
    row = AuditLog(action="create", resource_type="token_budget", tenant_id=divergent)
    db_session.add(row)
    await db_session.commit()
    row_id = row.id

    await record_tenant_id_divergence_ledger(db_session, tables=(("audit_log", "id"),))

    refreshed = (
        await db_session.execute(select(AuditLog).where(AuditLog.id == row_id))
    ).scalar_one()
    assert refreshed.tenant_id == divergent


@pytest.mark.unit
async def test_run_audit_publishes_rows_and_dollars_by_class(db_session_factory) -> None:
    """The end-to-end gauge path: real billing_cost_events rows -> real .set()s."""
    divergent = str(uuid.uuid4())
    async with db_session_factory() as session:
        session.add(
            CostEvent(
                tenant_id=divergent,
                service_key="inference-router",  # pragma: allowlist secret
                provider="claude",
                model="claude-sonnet-4-20250514",
                input_tokens=1,
                output_tokens=1,
                cost_usd=Decimal("12.50"),
                correlation_id=uuid.uuid4(),
            )
        )
        await session.commit()

    await run_tenant_id_divergence_audit(db_session_factory)

    assert (
        observability_metrics.tenant_id_divergence_rows.labels(classification="v4")._value.get()
        == 1
    )
    assert observability_metrics.tenant_id_divergence_spend_usd.labels(
        classification="v4"
    )._value.get() == pytest.approx(12.50)
    # Freshness marker moved -- the pass completed.
    assert (
        observability_metrics.tenant_id_divergence_audit_last_success_timestamp_seconds._value.get()
        > 0
    )


@pytest.mark.unit
async def test_run_audit_excludes_registered_tenants(db_session_factory) -> None:
    async with db_session_factory() as session:
        session.add(
            CostEvent(
                tenant_id=REGISTERED_TENANT,
                service_key="inference-router",  # pragma: allowlist secret
                provider="claude",
                model="claude-sonnet-4-20250514",
                input_tokens=1,
                output_tokens=1,
                cost_usd=Decimal("999.00"),
                correlation_id=uuid.uuid4(),
            )
        )
        await session.commit()

    snapshot = await run_tenant_id_divergence_audit(db_session_factory)

    assert sum(snapshot.spend_by_class.values()) == pytest.approx(0.0)


def test_default_ledger_tables_matches_the_measured_8() -> None:
    """The 8 base tables the 2026-09-16 estate measurement found divergence in."""
    assert {name for name, _pk in DEFAULT_LEDGER_TABLES} == {
        "audit_log",
        "billing_cost_daily",
        "billing_cost_events",
        "token_budgets",
        "billing_metering_events",
        "billing_metering_hourly",
        "token_usage",
        "billing_metering_daily",
    }
