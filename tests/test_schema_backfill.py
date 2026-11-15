"""Backfill behavior of ``apply_additive_migrations``.

The startup hook runs on every pod boot, so each statement must be idempotent
and the provider_pricing seed must not multiply rows on repeat execution.
"""

from __future__ import annotations

from contextlib import AsyncExitStack
from datetime import UTC, datetime
from decimal import Decimal

import pytest
from sqlalchemy import select
from sqlalchemy.ext.asyncio import create_async_engine

from db import apply_additive_migrations
from models.db import ProviderCost, ProviderPricing
from tests.conftest import TEST_DATABASE_URL


@pytest.mark.unit
async def test_backfill_creates_one_row_per_provider(db_session, db_engine) -> None:
    """Inserting a provider_costs row then running migrations creates a baseline."""
    db_session.add(
        ProviderCost(
            provider="anthropic",
            input_per_1m_usd=Decimal("3"),
            output_per_1m_usd=Decimal("15"),
            updated_at=datetime.now(UTC),
        )
    )
    await db_session.commit()

    await apply_additive_migrations(db_engine)

    stmt = select(ProviderPricing).where(ProviderPricing.provider == "anthropic")
    rows = (await db_session.execute(stmt)).scalars().all()
    assert len(rows) == 1
    row = rows[0]
    assert row.cost_model == "per_token"
    assert row.model is None
    assert row.input_per_1m_usd == Decimal("3")
    assert row.output_per_1m_usd == Decimal("15")
    assert row.effective_from.astimezone(UTC) == datetime(2026, 1, 1, tzinfo=UTC)
    assert row.effective_until is None


@pytest.mark.unit
async def test_backfill_is_idempotent(db_session, db_engine) -> None:
    """Running the migration twice must not duplicate the seed row."""
    db_session.add(
        ProviderCost(
            provider="openai",
            input_per_1m_usd=Decimal("2.5"),
            output_per_1m_usd=Decimal("10"),
            updated_at=datetime.now(UTC),
        )
    )
    await db_session.commit()

    await apply_additive_migrations(db_engine)
    await apply_additive_migrations(db_engine)

    stmt = select(ProviderPricing).where(ProviderPricing.provider == "openai")
    rows = (await db_session.execute(stmt)).scalars().all()
    assert len(rows) == 1


@pytest.mark.unit
async def test_backfill_skips_providers_already_seeded(db_session, db_engine) -> None:
    """If a ProviderPricing row already exists for the provider, backfill no-ops."""
    db_session.add(
        ProviderCost(
            provider="custom",
            input_per_1m_usd=Decimal("1"),
            output_per_1m_usd=Decimal("4"),
            updated_at=datetime.now(UTC),
        )
    )
    db_session.add(
        ProviderPricing(
            provider="custom",
            model=None,
            cost_model="per_token",
            input_per_1m_usd=Decimal("99"),
            output_per_1m_usd=Decimal("99"),
            effective_from=datetime(2026, 4, 1, tzinfo=UTC),
        )
    )
    await db_session.commit()

    await apply_additive_migrations(db_engine)

    stmt = select(ProviderPricing).where(ProviderPricing.provider == "custom")
    rows = (await db_session.execute(stmt)).scalars().all()
    # The pre-existing row stays untouched; no duplicate seeded from
    # provider_costs because a (custom, NULL) pricing row already existed.
    assert len(rows) == 1
    assert rows[0].input_per_1m_usd == Decimal("99")


@pytest.mark.unit
async def test_backfill_with_no_provider_costs_inserts_nothing(db_session, db_engine) -> None:
    """No provider_costs ⇒ nothing seeded (the SELECT yields zero rows)."""
    await apply_additive_migrations(db_engine)
    stmt = select(ProviderPricing)
    rows = (await db_session.execute(stmt)).scalars().all()
    assert rows == []


@pytest.mark.unit
async def test_it_hands_the_pool_back_a_connection_with_no_statement_timeout() -> None:
    """⭐ THE SESSION-WIDE SIDE EFFECT, pinned.

    ``apply_additive_migrations`` sets ``statement_timeout = '5min'`` on an
    AUTOCOMMIT connection so a hung ALTER cannot hold an AccessExclusiveLock
    forever. ``SET`` there is correct — ``SET LOCAL`` would die with the
    implicit single-statement transaction and never reach the ALTERs — but a
    plain ``SET`` is SESSION-scoped, and SQLAlchemy's check-in only issues a
    ROLLBACK, which does not undo a GUC set outside a transaction. MEASURED on
    postgres 18 before the fix: set 5min, close, check the SAME pooled
    connection out again — still 5min.

    In the SUITE that is the timeout half of the truncate mystery. The tests
    above call this function with the SHARED, session-scoped ``db_engine``, so
    from then on one of its pooled connections carried a ``statement_timeout``
    nothing in the run asked for. The next test to draw it ran its
    ``db_engine`` ``TRUNCATE ... CASCADE`` under that timeout — and a TRUNCATE
    blocks on ACCESS EXCLUSIVE behind any connection sitting in a leaked
    transaction — so it died with ``canceling statement due to statement
    timeout`` in SETUP, on a test with no relationship to this file.

    ⛔ THIS TEST OWNS ITS ENGINE, AND THAT IS THE POINT. Written against the
    shared ``db_engine`` it was VACUOUS and I nearly shipped it that way: the
    tests above have already run the migrations on that pool, so the
    "before" sample ALREADY read ``5min`` and ``after == before`` held under
    the bug. Measured — with the ``RESET`` replaced by ``SELECT 1`` the shared
    engine version reported ``5 passed``. A private engine gives a baseline
    that predates any call, which is what makes the comparison able to fail.
    """
    engine = create_async_engine(TEST_DATABASE_URL, echo=False)
    try:
        baseline = await _pooled_statement_timeouts(engine)

        await apply_additive_migrations(engine)

        after = await _pooled_statement_timeouts(engine)
        assert after == baseline, (
            "apply_additive_migrations left a statement_timeout on a pooled connection: "
            f"before={sorted(baseline)} after={sorted(after)}"
        )
    finally:
        await engine.dispose()


async def _pooled_statement_timeouts(engine) -> set[str]:
    """``statement_timeout`` on every connection the pool can hand out.

    Opens them CONCURRENTLY on purpose: sequential ``connect()`` calls check the
    same connection back out again and again, so a GUC stranded on a second
    pooled connection would never be sampled — the check would pass without
    being able to fail.
    """
    async with AsyncExitStack() as stack:
        conns = [await stack.enter_async_context(engine.connect()) for _ in range(5)]
        return {(await conn.exec_driver_sql("SHOW statement_timeout")).scalar() for conn in conns}
