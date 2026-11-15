"""Money columns must hold real-world magnitudes, not overflow.

`billing_cost_events.cost_usd` was `Numeric(12, 8)` (max ~$9,999.99999999) and
`billing_cost_daily.total_cost_usd` was `Numeric(12, 6)` (max $999,999.999999).
A single high cost event (e.g. a self_hosted_amortized / monthly_fixed_cost_usd
line) or a high-volume tenant's daily aggregate exceeds those caps and raises
Postgres `numeric field overflow` on INSERT — aborting the whole cost_flusher
batch (stuck consumer) or failing the day's rollup (the invoice source). These
columns are now `Numeric(20, 8)` / `Numeric(20, 6)`.

Both tests are mutation-proof: reverting either column to its old precision
makes the write raise `DataError: numeric field overflow`.
"""

from __future__ import annotations

import uuid
from datetime import UTC, date, datetime
from decimal import Decimal

import pytest
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from models.db import CostDaily, CostEvent
from tests.conftest import T


@pytest.mark.asyncio
async def test_cost_event_accepts_cost_above_10k(db_session: AsyncSession) -> None:
    """A cost event >= $10k persists at full 8dp precision (was capped at ~$10k)."""
    big = Decimal("12345.67890000")  # > 9,999.99999999 → overflows Numeric(12,8)
    ev = CostEvent(
        id=uuid.uuid7(),
        tenant_id=T("t-big"),
        tenant_name="t-big",
        service_key="svc",
        provider="self_hosted",
        model=None,
        served_model="amortized",
        input_tokens=1,
        output_tokens=1,
        cache_creation_input_tokens=0,
        cache_read_input_tokens=0,
        cost_usd=big,
        cost_source="test",
        created_at=datetime(2026, 5, 21, 12, 0, tzinfo=UTC),
    )
    db_session.add(ev)
    await db_session.commit()

    got = (
        await db_session.execute(select(CostEvent.cost_usd).where(CostEvent.id == ev.id))
    ).scalar_one()
    assert got == big


@pytest.mark.asyncio
async def test_cost_daily_accepts_total_above_1m(db_session: AsyncSession) -> None:
    """A per-tuple daily aggregate > $1M persists (was capped at $999,999.999999)."""
    big = Decimal("2500000.500000")  # > 999,999.999999 → overflows Numeric(12,6)
    row = CostDaily(
        tenant_id=T("t-big"),
        service_key="svc",
        provider="anthropic",
        bucket_date=date(2026, 5, 21),
        total_cost_usd=big,
    )
    db_session.add(row)
    await db_session.commit()

    got = (
        await db_session.execute(select(CostDaily.total_cost_usd).where(CostDaily.id == row.id))
    ).scalar_one()
    assert got == big
