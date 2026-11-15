"""0013 — parking the worker-management duplicate of a router-metered call.

The defect: inference-router meters every call it serves, and worker_management
ALSO emitted a cost event for the same call whenever the tenant's config named
the `local` preset — whose provider IS the router. Prod, 2026-08-26: 14,833 of
14,840 correlation-bearing worker-management rows had an exact router twin.

What is asserted here, in the order the owner's requirements list them:

* REVERSIBLE — nothing is deleted; downgrade un-parks and restates.
* EXACT — only rows with a PROVEN twin under the strict correlation key, with a
  COUNTED pairing so a group can never lose more legs than it has twins.
* VERIFIABLE — before/after totals move by exactly the parked amount.
* ROLLUP — `billing_cost_daily` is restated, and the migration's hand-written
  recompute SQL is pinned against the real `rollup_engine.rollup_daily` output
  rather than trusted to stay in step with it.
* IDEMPOTENT — a second run parks nothing and changes no total.
"""

from __future__ import annotations

import asyncio
import importlib.util
import uuid
from datetime import UTC, datetime, timedelta
from decimal import Decimal
from pathlib import Path

import pytest
from alembic import command
from alembic.config import Config
from sqlalchemy import text

from db import ADDITIVE_MIGRATIONS
from services.rollup_engine import rollup_daily

pytestmark = pytest.mark.unit

REPO_ROOT = Path(__file__).resolve().parents[1]
REVISION = "0013_park_router_duplicates"
PREVIOUS_REVISION = "0012_runtime_ddl_schema_drift"

_MIGRATION_PATH = REPO_ROOT / "alembic" / "versions" / f"{REVISION}.py"
_spec = importlib.util.spec_from_file_location("_migration_0013", _MIGRATION_PATH)
assert _spec is not None and _spec.loader is not None
migration = importlib.util.module_from_spec(_spec)
_spec.loader.exec_module(migration)

NEW_COLUMNS = (
    ("billing_cost_events", "excluded_from_billing"),
    ("billing_cost_events", "exclusion_reason"),
    ("billing_cost_events", "duplicate_of_event_id"),
    ("billing_cost_events", "event_key"),
)
CLAIM_TABLE = "processed_cost_event_keys"

DAY = datetime(2026, 8, 20, 12, 0, tzinfo=UTC)


# ---------------------------------------------------------------------------
# Seeding
# ---------------------------------------------------------------------------


async def _insert(session, **over) -> uuid.UUID:
    row = {
        "id": uuid.uuid4(),
        "tenant_id": over.pop("tenant_id"),
        "tenant_name": "acme",
        "license_id": None,
        "subject_id": None,
        "app_id": None,
        "service_key": "worker-management",
        "provider": "pgx-local",
        "model": "local-llm",
        "served_model": "local-llm",
        "input_tokens": 100,
        "output_tokens": 50,
        "cache_creation_input_tokens": 0,
        "cache_read_input_tokens": 0,
        "cost_usd": Decimal("0.01000000"),
        "correlation_id": None,
        "cost_source": "worker-management",
        "usage_extra": None,
        "latency_ms": None,
        "created_at": DAY,
    }
    row.update(over)
    await session.execute(
        text(
            "INSERT INTO billing_cost_events (id, tenant_id, tenant_name, license_id,"
            " subject_id, app_id, service_key, provider, model, served_model,"
            " input_tokens, output_tokens, cache_creation_input_tokens,"
            " cache_read_input_tokens, cost_usd, correlation_id, cost_source,"
            " usage_extra, latency_ms, created_at)"
            " VALUES (:id, :tenant_id, :tenant_name, :license_id, :subject_id, :app_id,"
            " :service_key, :provider, :model, :served_model, :input_tokens,"
            " :output_tokens, :cache_creation_input_tokens, :cache_read_input_tokens,"
            " :cost_usd, :correlation_id, :cost_source, CAST(:usage_extra AS jsonb),"
            " :latency_ms, :created_at)"
        ),
        row,
    )
    return row["id"]


async def _seed_corpus(session, tenant: uuid.UUID) -> dict[str, list[uuid.UUID]]:
    """A corpus with one of every case the park predicate has to tell apart."""
    ids: dict[str, list[uuid.UUID]] = {k: [] for k in ("dup", "keep")}

    # 1. The plain twin pair: one router row, one worker-management row. DUP.
    c1 = uuid.uuid4()
    await _insert(session, tenant_id=tenant, service_key="inference-router", correlation_id=c1)
    ids["dup"].append(
        await _insert(
            session,
            tenant_id=tenant,
            correlation_id=c1,
            usage_extra='{"byo": "true"}',
            created_at=DAY + timedelta(seconds=3),
        )
    )

    # 2. Same correlation, DIFFERENT tokens — a different call in the same
    #    operation. No twin, KEEP. This is why correlation_id alone is not the key.
    ids["keep"].append(
        await _insert(session, tenant_id=tenant, correlation_id=c1, input_tokens=999)
    )

    # 3. A correlation carrying THREE worker-management legs but only TWO router
    #    legs: park 2, keep 1. Losing all three would be an under-bill.
    c3 = uuid.uuid4()
    for _ in range(2):
        await _insert(session, tenant_id=tenant, service_key="inference-router", correlation_id=c3)
    # Distinct created_at so the (created_at, id) rank order — and therefore
    # WHICH two of the three get parked — is deterministic rather than uuid luck.
    three = [
        await _insert(
            session,
            tenant_id=tenant,
            correlation_id=c3,
            created_at=DAY + timedelta(seconds=10 + i),
        )
        for i in range(3)
    ]
    ids["dup"] += three[:2]
    ids["keep"].append(three[2])

    # 4. No correlation_id at all — unprovable, KEEP even though the tokens
    #    match a router row exactly. The deliberate residual.
    await _insert(session, tenant_id=tenant, service_key="inference-router", correlation_id=None)
    ids["keep"].append(await _insert(session, tenant_id=tenant, correlation_id=None))

    # 5. A genuine BYO call: no router row anywhere. KEEP.
    ids["keep"].append(
        await _insert(
            session,
            tenant_id=tenant,
            correlation_id=uuid.uuid4(),
            provider="anthropic",
            model="claude-sonnet-4-6",
            served_model="claude-sonnet-4-6",
        )
    )

    # 6. A twin whose COST differs — pricing disagreed, so it is not the same
    #    priced fact. KEEP.
    c6 = uuid.uuid4()
    await _insert(session, tenant_id=tenant, service_key="inference-router", correlation_id=c6)
    ids["keep"].append(
        await _insert(session, tenant_id=tenant, correlation_id=c6, cost_usd=Decimal("0.02"))
    )
    await session.commit()
    return ids


async def _park(session) -> int:
    result = await session.execute(text(migration.PARK_SQL))
    await session.commit()
    return result.rowcount


async def _parked_ids(session) -> set[uuid.UUID]:
    rows = await session.execute(
        text("SELECT id FROM billing_cost_events WHERE excluded_from_billing = true")
    )
    return {r[0] for r in rows}


async def _totals(session) -> tuple[int, Decimal]:
    row = (
        await session.execute(
            text(
                "SELECT count(*), coalesce(sum(cost_usd), 0) FROM billing_cost_events"
                " WHERE service_key = 'worker-management' AND excluded_from_billing = false"
            )
        )
    ).one()
    return row[0], row[1]


# ---------------------------------------------------------------------------
# EXACT — only proven twins
# ---------------------------------------------------------------------------


async def test_parks_exactly_the_proven_twins_and_nothing_else(db_session):
    tenant = uuid.uuid4()
    ids = await _seed_corpus(db_session, tenant)

    parked = await _park(db_session)

    assert parked == len(ids["dup"])
    assert await _parked_ids(db_session) == set(ids["dup"])
    # Named explicitly: every keeper is still billable.
    still_billable = set(ids["keep"]) - await _parked_ids(db_session)
    assert still_billable == set(ids["keep"])


async def test_a_group_never_loses_more_legs_than_it_has_twins(db_session):
    """3 worker-management legs, 2 router legs ⇒ park 2, keep 1."""
    tenant = uuid.uuid4()
    corr = uuid.uuid4()
    for _ in range(2):
        await _insert(
            db_session, tenant_id=tenant, service_key="inference-router", correlation_id=corr
        )
    wm = [await _insert(db_session, tenant_id=tenant, correlation_id=corr) for _ in range(3)]
    await db_session.commit()

    await _park(db_session)

    assert len(await _parked_ids(db_session)) == 2
    assert len(set(wm) - await _parked_ids(db_session)) == 1


async def test_the_router_leg_is_never_parked(db_session):
    """The surviving row must be the meter of record, not the duplicate."""
    tenant = uuid.uuid4()
    corr = uuid.uuid4()
    ir = await _insert(
        db_session, tenant_id=tenant, service_key="inference-router", correlation_id=corr
    )
    wm = await _insert(db_session, tenant_id=tenant, correlation_id=corr)
    await db_session.commit()

    await _park(db_session)

    assert await _parked_ids(db_session) == {wm}
    row = (
        await db_session.execute(
            text(
                "SELECT duplicate_of_event_id, exclusion_reason FROM billing_cost_events"
                " WHERE id = :i"
            ),
            {"i": wm},
        )
    ).one()
    assert row[0] == ir, "the park must record WHICH row it duplicates"
    assert row[1] == migration.EXCLUSION_REASON


# ---------------------------------------------------------------------------
# REVERSIBLE — nothing is deleted
# ---------------------------------------------------------------------------


async def test_no_row_is_ever_deleted(db_session):
    tenant = uuid.uuid4()
    await _seed_corpus(db_session, tenant)
    before = (await db_session.execute(text("SELECT count(*) FROM billing_cost_events"))).scalar()

    await _park(db_session)

    after = (await db_session.execute(text("SELECT count(*) FROM billing_cost_events"))).scalar()
    assert after == before
    assert "DELETE" not in migration.PARK_SQL.upper()


async def test_a_park_can_be_reversed_by_one_update(db_session):
    tenant = uuid.uuid4()
    ids = await _seed_corpus(db_session, tenant)
    billable_before = await _totals(db_session)
    await _park(db_session)
    assert await _totals(db_session) != billable_before

    await db_session.execute(
        text(
            "UPDATE billing_cost_events SET excluded_from_billing = false,"
            " exclusion_reason = NULL, duplicate_of_event_id = NULL"
            " WHERE exclusion_reason = :r"
        ),
        {"r": migration.EXCLUSION_REASON},
    )
    await db_session.commit()

    assert await _totals(db_session) == billable_before
    assert await _parked_ids(db_session) == set()
    assert ids["dup"]  # the corpus really did contain duplicates


# ---------------------------------------------------------------------------
# IDEMPOTENT
# ---------------------------------------------------------------------------


async def test_a_second_run_parks_nothing_and_moves_no_money(db_session):
    tenant = uuid.uuid4()
    await _seed_corpus(db_session, tenant)

    first = await _park(db_session)
    after_first = await _totals(db_session)
    parked_first = await _parked_ids(db_session)

    second = await _park(db_session)

    assert first > 0
    assert second == 0, "re-running must be a no-op, not a second sweep"
    assert await _totals(db_session) == after_first
    assert await _parked_ids(db_session) == parked_first


async def test_re_running_does_not_re_rank_and_park_the_survivor(db_session):
    """The subtle idempotence bug this SQL is written to avoid.

    If the ranked set were filtered to unparked rows, the survivor of a
    3-wm/2-ir group would be re-ranked to rn=1 on the next run and parked —
    silently under-billing a legitimate call on the second pass.
    """
    tenant = uuid.uuid4()
    corr = uuid.uuid4()
    for _ in range(2):
        await _insert(
            db_session, tenant_id=tenant, service_key="inference-router", correlation_id=corr
        )
    for _ in range(3):
        await _insert(db_session, tenant_id=tenant, correlation_id=corr)
    await db_session.commit()

    await _park(db_session)
    await _park(db_session)
    await _park(db_session)

    assert len(await _parked_ids(db_session)) == 2


# ---------------------------------------------------------------------------
# ROLLUP — and the drift pin
# ---------------------------------------------------------------------------


async def _daily_snapshot(session) -> set[tuple]:
    rows = await session.execute(
        text(
            "SELECT tenant_id, license_id, subject_id, service_key, provider,"
            " served_model, byo, owner_provided, cost_source, bucket_date,"
            " total_input_tokens, total_output_tokens, total_cost_usd, request_count"
            " FROM billing_cost_daily ORDER BY 1,2,3,4,5,6,7,8,9,10"
        )
    )
    return {tuple(r) for r in rows}


async def test_rollup_daily_excludes_parked_rows(db_session, db_session_factory):
    tenant = uuid.uuid4()
    await _seed_corpus(db_session, tenant)
    await _park(db_session)

    await rollup_daily(db_session_factory, target_date=DAY.date())

    wm_total = (
        await db_session.execute(
            text(
                "SELECT coalesce(sum(total_cost_usd), 0) FROM billing_cost_daily"
                " WHERE service_key = 'worker-management'"
            )
        )
    ).scalar()
    billable = (await _totals(db_session))[1]
    assert wm_total == billable, "the rollup must sum only the billable legs"


async def test_the_migrations_recompute_sql_matches_the_real_rollup_engine(
    db_session, db_session_factory
):
    """The drift pin.

    The migration restates `billing_cost_daily` with hand-written SQL because
    calling the async rollup engine from inside a sync alembic transaction would
    contend on the same rows. That SQL is only safe if it produces EXACTLY what
    the engine produces — so this measures the two against each other instead of
    asserting they agree.
    """
    tenant = uuid.uuid4()
    await _seed_corpus(db_session, tenant)
    # Give the corpus every dimension of uq_cost_daily something to vary on.
    await _insert(
        db_session,
        tenant_id=tenant,
        correlation_id=None,
        service_key="inference-router",
        usage_extra='{"owner_provided": "true"}',
        served_model=None,
        cost_source="unknown",
    )
    await db_session.commit()
    await _park(db_session)

    dates = [DAY.date()]
    await db_session.execute(text(migration.DELETE_STALE_DAILY_SQL), {"dates": dates})
    await db_session.execute(text(migration.REBUILD_DAILY_SQL), {"dates": dates})
    await db_session.commit()
    from_migration = await _daily_snapshot(db_session)

    await db_session.execute(text(migration.DELETE_STALE_DAILY_SQL), {"dates": dates})
    await db_session.commit()
    await rollup_daily(db_session_factory, target_date=DAY.date())
    from_engine = await _daily_snapshot(db_session)

    assert from_migration, "the corpus must actually produce rollup rows"
    assert from_migration == from_engine


async def test_the_restatement_is_idempotent(db_session):
    tenant = uuid.uuid4()
    await _seed_corpus(db_session, tenant)
    await _park(db_session)
    dates = [DAY.date()]

    for _ in range(2):
        await db_session.execute(text(migration.DELETE_STALE_DAILY_SQL), {"dates": dates})
        await db_session.execute(text(migration.REBUILD_DAILY_SQL), {"dates": dates})
        await db_session.commit()
        snapshot = await _daily_snapshot(db_session)

    await db_session.execute(text(migration.DELETE_STALE_DAILY_SQL), {"dates": dates})
    await db_session.execute(text(migration.REBUILD_DAILY_SQL), {"dates": dates})
    await db_session.commit()
    assert await _daily_snapshot(db_session) == snapshot


# ---------------------------------------------------------------------------
# The DDL, and the drift ratchet 0012 exists to enforce
# ---------------------------------------------------------------------------


def _alembic_config() -> Config:
    cfg = Config()
    cfg.set_main_option("script_location", str(REPO_ROOT / "alembic"))
    return cfg


def _upgrade() -> None:
    cfg = _alembic_config()
    command.stamp(cfg, PREVIOUS_REVISION)
    command.upgrade(cfg, REVISION)


async def test_upgrade_creates_the_columns_it_claims_to(db_session):
    """Dropped first — otherwise `create_all` has already made this vacuous.

    Every statement is `IF NOT EXISTS`, so a test that only upgrades and then
    asserts "the column exists" passes against a migration that did nothing.
    """
    for table, column in NEW_COLUMNS:
        await db_session.execute(text(f"ALTER TABLE {table} DROP COLUMN IF EXISTS {column}"))
    await db_session.execute(text(f"DROP TABLE IF EXISTS {CLAIM_TABLE}"))
    await db_session.execute(text("DROP TABLE IF EXISTS alembic_version"))
    await db_session.commit()

    await asyncio.to_thread(_upgrade)

    for table, column in NEW_COLUMNS:
        present = (
            await db_session.execute(
                text(
                    "SELECT count(*) FROM information_schema.columns"
                    " WHERE table_name = :t AND column_name = :c"
                ),
                {"t": table, "c": column},
            )
        ).scalar()
        assert present == 1, f"{table}.{column} missing after upgrade"
    pk = (
        await db_session.execute(
            text(
                "SELECT pg_get_constraintdef(oid) FROM pg_constraint"
                f" WHERE conrelid = '{CLAIM_TABLE}'::regclass AND contype = 'p'"
            )
        )
    ).scalar()
    assert pk == "PRIMARY KEY (event_key)"
    await db_session.execute(text("DROP TABLE IF EXISTS alembic_version"))
    await db_session.commit()


def test_every_statement_is_verbatim_in_src_db_py():
    """The drift ratchet. A divergence between the two IS the 0012 defect."""
    missing = [s for s in migration.STATEMENTS if s not in ADDITIVE_MIGRATIONS]
    assert not missing, (
        "these 0013 statements are not byte-identical entries of "
        f"ADDITIVE_MIGRATIONS in src/db.py: {missing}"
    )


def test_the_migration_contains_no_delete_of_billing_rows():
    """A hard, greppable guarantee over the whole file, not just PARK_SQL.

    The only DELETE permitted is against billing_cost_daily, which is DERIVED
    and rebuilt in the same statement pair.
    """
    source = _MIGRATION_PATH.read_text()
    for line in source.splitlines():
        upper = line.upper()
        if "DELETE FROM" in upper:
            assert "BILLING_COST_DAILY" in upper, f"unexpected DELETE: {line.strip()}"


def test_revision_id_fits_the_alembic_version_column():
    """``alembic_version.version_num`` is varchar(32)."""
    assert len(REVISION) <= 32


def test_the_chain_has_a_single_head_and_0013_follows_0012():
    """The invariant is LINEARITY, not that 0013 is forever the tip.

    This used to assert ``get_heads() == [REVISION]``, which made it a tripwire
    that every later migration had to come back and edit -- and a test whose
    only failure mode is "someone did the normal thing" gets edited on autopilot,
    which is how a real branch would eventually be waved through. The hazard
    worth pinning is a BRANCHED chain (two heads = an ambiguous upgrade path);
    0013's own position in it is pinned by its ``down_revision`` below.
    """
    from alembic.script import ScriptDirectory

    script = ScriptDirectory.from_config(_alembic_config())
    heads = list(script.get_heads())
    assert len(heads) == 1, f"alembic chain has branched: {heads}"
    assert script.get_revision(REVISION).down_revision == PREVIOUS_REVISION


# ---------------------------------------------------------------------------
# The LOOSE rule — the correlation-less remainder
#
# The owner authorised this heuristic knowing it cannot tell a duplicate from
# two genuinely identical calls in the same minute. What is asserted here is the
# containment that was the condition of that decision:
#
#   * a loose park carries the LOOSE reason, never the strict one;
#   * a strict park keeps the strict reason even when the loose rule also runs;
#   * un-parking the loose set restores exactly those rows and leaves every
#     strict park alone;
#   * re-running changes nothing.
# ---------------------------------------------------------------------------


async def _park_loose(session) -> int:
    result = await session.execute(text(migration.LOOSE_PARK_SQL))
    await session.commit()
    return result.rowcount


async def _reason_of(session, event_id: uuid.UUID) -> str | None:
    return (
        await session.execute(
            text("SELECT exclusion_reason FROM billing_cost_events WHERE id = :i"),
            {"i": event_id},
        )
    ).scalar()


async def _ids_with_reason(session, reason: str) -> set[uuid.UUID]:
    rows = await session.execute(
        text("SELECT id FROM billing_cost_events WHERE exclusion_reason = :r"),
        {"r": reason},
    )
    return {r[0] for r in rows}


async def _seed_mixed(session, tenant: uuid.UUID) -> dict[str, uuid.UUID]:
    """One strict-provable pair and one loose-only pair, in the SAME minute.

    Both worker-management legs are identical apart from `correlation_id`, so a
    rule that ignored the discriminator would stamp them the same.
    """
    corr = uuid.uuid4()
    ir_strict = await _insert(
        session, tenant_id=tenant, service_key="inference-router", correlation_id=corr
    )
    wm_strict = await _insert(
        session,
        tenant_id=tenant,
        correlation_id=corr,
        created_at=DAY + timedelta(seconds=1),
    )
    # The loose pair: a different model so it lands in its own minute-group and
    # cannot borrow the strict pair's router leg.
    ir_loose = await _insert(
        session,
        tenant_id=tenant,
        service_key="inference-router",
        correlation_id=None,
        model="loose-llm",
        served_model="loose-llm",
        created_at=DAY + timedelta(seconds=2),
    )
    wm_loose = await _insert(
        session,
        tenant_id=tenant,
        correlation_id=None,
        model="loose-llm",
        served_model="loose-llm",
        created_at=DAY + timedelta(seconds=5),
    )
    await session.commit()
    return {
        "ir_strict": ir_strict,
        "wm_strict": wm_strict,
        "ir_loose": ir_loose,
        "wm_loose": wm_loose,
    }


async def test_a_loose_matched_pair_parks_with_the_loose_reason(db_session):
    tenant = uuid.uuid4()
    ids = await _seed_mixed(db_session, tenant)

    await _park(db_session)
    parked = await _park_loose(db_session)

    assert parked == 1
    assert await _reason_of(db_session, ids["wm_loose"]) == migration.EXCLUSION_REASON_LOOSE
    twin = (
        await db_session.execute(
            text("SELECT duplicate_of_event_id FROM billing_cost_events WHERE id = :i"),
            {"i": ids["wm_loose"]},
        )
    ).scalar()
    assert twin == ids["ir_loose"], "a loose park must still record WHICH row it duplicates"


async def test_a_strict_matched_pair_keeps_the_strict_reason(db_session):
    """Requirement 6: if both rules could reach a row, strict wins.

    They cannot in fact overlap — strict requires `correlation_id IS NOT NULL`
    and loose requires `correlation_id IS NULL` — and this pins that disjointness
    rather than trusting it.
    """
    tenant = uuid.uuid4()
    ids = await _seed_mixed(db_session, tenant)

    await _park(db_session)
    assert await _reason_of(db_session, ids["wm_strict"]) == migration.EXCLUSION_REASON

    await _park_loose(db_session)

    assert await _reason_of(db_session, ids["wm_strict"]) == migration.EXCLUSION_REASON
    assert await _ids_with_reason(db_session, migration.EXCLUSION_REASON) == {ids["wm_strict"]}
    assert await _ids_with_reason(db_session, migration.EXCLUSION_REASON_LOOSE) == {ids["wm_loose"]}
    # The router legs are the meter of record and are never parked.
    assert ids["ir_strict"] not in await _parked_ids(db_session)
    assert ids["ir_loose"] not in await _parked_ids(db_session)


async def test_the_loose_rule_never_touches_a_correlation_bearing_row(db_session):
    """A correlated row with NO proven twin stays billable — loose must not take it.

    Without the `correlation_id IS NULL` restriction the loose rule would reach
    the 7 prod rows the strict rule deliberately left alone, silently converting
    "no proof" into "parked anyway".
    """
    tenant = uuid.uuid4()
    await _insert(db_session, tenant_id=tenant, service_key="inference-router", correlation_id=None)
    orphan = await _insert(
        db_session,
        tenant_id=tenant,
        correlation_id=uuid.uuid4(),
        created_at=DAY + timedelta(seconds=4),
    )
    await db_session.commit()

    await _park(db_session)
    await _park_loose(db_session)

    assert orphan not in await _parked_ids(db_session)


async def test_a_minute_group_never_loses_more_legs_than_it_has_router_legs(db_session):
    """One router leg, three identical correlation-less legs ⇒ park 1, keep 2."""
    tenant = uuid.uuid4()
    await _insert(db_session, tenant_id=tenant, service_key="inference-router", correlation_id=None)
    wm = [
        await _insert(
            db_session,
            tenant_id=tenant,
            correlation_id=None,
            created_at=DAY + timedelta(seconds=10 + i),
        )
        for i in range(3)
    ]
    await db_session.commit()

    await _park_loose(db_session)

    parked = await _parked_ids(db_session)
    assert len(parked) == 1
    assert len(set(wm) - parked) == 2


async def test_the_loose_capacity_is_reduced_by_what_strict_already_parked(db_session):
    """Both rules together can never surrender more legs than there are twins.

    One router leg in the minute; one correlated worker-management leg that the
    strict rule parks against it, and one correlation-less leg in the SAME
    minute-group. The loose rule must find zero capacity left.
    """
    tenant = uuid.uuid4()
    corr = uuid.uuid4()
    await _insert(db_session, tenant_id=tenant, service_key="inference-router", correlation_id=corr)
    await _insert(
        db_session, tenant_id=tenant, correlation_id=corr, created_at=DAY + timedelta(seconds=1)
    )
    orphan = await _insert(
        db_session, tenant_id=tenant, correlation_id=None, created_at=DAY + timedelta(seconds=2)
    )
    await db_session.commit()

    strict = await _park(db_session)
    loose = await _park_loose(db_session)

    assert strict == 1
    assert loose == 0, "the strict park already consumed the only router leg"
    assert orphan not in await _parked_ids(db_session)


async def test_a_pair_in_a_different_minute_is_not_loose_matched(db_session):
    """`date_trunc('minute')`, not a sliding window. Under-parking is the safe side."""
    tenant = uuid.uuid4()
    await _insert(
        db_session,
        tenant_id=tenant,
        service_key="inference-router",
        correlation_id=None,
        created_at=DAY + timedelta(seconds=59),
    )
    late = await _insert(
        db_session,
        tenant_id=tenant,
        correlation_id=None,
        created_at=DAY + timedelta(seconds=61),
    )
    await db_session.commit()

    assert await _park_loose(db_session) == 0
    assert late not in await _parked_ids(db_session)


async def test_re_running_the_loose_park_changes_nothing(db_session):
    tenant = uuid.uuid4()
    await _seed_mixed(db_session, tenant)
    await _park(db_session)

    first = await _park_loose(db_session)
    after_first = await _totals(db_session)
    parked_first = await _parked_ids(db_session)

    assert await _park_loose(db_session) == 0
    assert await _park_loose(db_session) == 0
    assert first == 1
    assert await _totals(db_session) == after_first
    assert await _parked_ids(db_session) == parked_first


async def test_un_parking_the_loose_set_leaves_the_strict_set_alone(db_session):
    """The load-bearing property: the heuristic is INDEPENDENTLY reversible.

    Not `alembic downgrade` — a targeted UPDATE that restores exactly the loose
    rows and nothing else.
    """
    tenant = uuid.uuid4()
    ids = await _seed_mixed(db_session, tenant)
    billable_before_any_park = await _totals(db_session)

    await _park(db_session)
    strict_snapshot = await _parked_ids(db_session)
    billable_after_strict = await _totals(db_session)
    await _park_loose(db_session)
    assert await _totals(db_session) != billable_after_strict

    unparked = (await db_session.execute(text(migration.UNPARK_LOOSE_SQL))).rowcount
    await db_session.commit()

    assert unparked == 1
    assert await _parked_ids(db_session) == strict_snapshot
    assert await _totals(db_session) == billable_after_strict
    assert await _reason_of(db_session, ids["wm_strict"]) == migration.EXCLUSION_REASON
    assert await _reason_of(db_session, ids["wm_loose"]) is None
    assert billable_before_any_park != billable_after_strict  # the corpus really parked something


async def test_no_row_is_deleted_by_the_loose_rule(db_session):
    tenant = uuid.uuid4()
    await _seed_mixed(db_session, tenant)
    before = (await db_session.execute(text("SELECT count(*) FROM billing_cost_events"))).scalar()

    await _park(db_session)
    await _park_loose(db_session)
    await db_session.execute(text(migration.UNPARK_LOOSE_SQL))
    await db_session.commit()

    after = (await db_session.execute(text("SELECT count(*) FROM billing_cost_events"))).scalar()
    assert after == before
    assert "DELETE" not in migration.LOOSE_PARK_SQL.upper()
    assert "DELETE" not in migration.UNPARK_LOOSE_SQL.upper()


async def test_the_rollup_restatement_covers_the_loose_set_too(db_session):
    """Requirement 5. `AFFECTED_DATES_SQL` must see BOTH reasons.

    A loose park that never reached `billing_cost_daily` would leave the money
    on the invoice while the event looked handled.
    """
    tenant = uuid.uuid4()
    await _insert(
        db_session,
        tenant_id=tenant,
        service_key="inference-router",
        correlation_id=None,
        model="loose-only",
        served_model="loose-only",
    )
    await _insert(
        db_session,
        tenant_id=tenant,
        correlation_id=None,
        model="loose-only",
        served_model="loose-only",
        created_at=DAY + timedelta(seconds=5),
    )
    await db_session.commit()
    assert await _park_loose(db_session) == 1

    dates = [r[0] for r in await db_session.execute(text(migration.AFFECTED_DATES_SQL))]
    assert dates == [DAY.date()], "the loose park's date must be restated"

    await db_session.execute(text(migration.DELETE_STALE_DAILY_SQL), {"dates": dates})
    await db_session.execute(text(migration.REBUILD_DAILY_SQL), {"dates": dates})
    await db_session.commit()

    wm_total = (
        await db_session.execute(
            text(
                "SELECT coalesce(sum(total_cost_usd), 0) FROM billing_cost_daily"
                " WHERE service_key = 'worker-management'"
            )
        )
    ).scalar()
    assert wm_total == (await _totals(db_session))[1]


async def test_the_two_reasons_are_distinct_and_fit_the_column(db_session):
    """The discriminator itself: two different values, both storable."""
    assert migration.EXCLUSION_REASON != migration.EXCLUSION_REASON_LOOSE
    assert len(migration.EXCLUSION_REASON_LOOSE) <= 64
    assert not migration.EXCLUSION_REASON_LOOSE.startswith(migration.EXCLUSION_REASON + "'")
    # Storable, not merely short enough in Python.
    tenant = uuid.uuid4()
    row = await _insert(db_session, tenant_id=tenant)
    await db_session.execute(
        text("UPDATE billing_cost_events SET exclusion_reason = :r WHERE id = :i"),
        {"r": migration.EXCLUSION_REASON_LOOSE, "i": row},
    )
    await db_session.commit()
    assert await _reason_of(db_session, row) == migration.EXCLUSION_REASON_LOOSE


async def test_the_latency_shape_query_counts_what_it_claims_to(db_session):
    """The false-positive instrument, measured — and its own bias pinned.

    Three loose-matched pairs, one per shape the operator has to tell apart:

    * `twinlike` — a 339ms gap on a 66s call. The outer/inner-leg shape actually
      seen in prod (median 183ms against a 4,718ms median call). NOT dissenting
      on any measure.
    * `shortcall` — the SAME 183ms gap on a 500ms call. Trips the 5% RELATIVE
      threshold while being the identical physical phenomenon. This is why the
      relative number is not a false-positive rate, and pinning it here stops
      anyone "simplifying" the instrument back to that one number.
    * `wmfaster` — the worker-management leg FASTER than the router leg it
      wraps, by 7s. Physically impossible for one call; the only genuinely
      unexplained shape. 203 such pairs in prod.
    """
    tenant = uuid.uuid4()
    for model, ir_latency, wm_latency in (
        ("twinlike", 66_000, 66_339),
        ("shortcall", 500, 683),
        ("wmfaster", 10_000, 3_000),
    ):
        await _insert(
            db_session,
            tenant_id=tenant,
            service_key="inference-router",
            correlation_id=None,
            model=model,
            served_model=model,
            latency_ms=ir_latency,
        )
        await _insert(
            db_session,
            tenant_id=tenant,
            correlation_id=None,
            model=model,
            served_model=model,
            latency_ms=wm_latency,
            created_at=DAY + timedelta(seconds=5),
        )
    await db_session.commit()
    assert await _park_loose(db_session) == 3

    pairs, wm_faster, over_1s, relative_groups = (
        await db_session.execute(text(migration.LOOSE_LATENCY_SHAPE_SQL))
    ).one()

    assert pairs == 3
    assert wm_faster == 1, "only `wmfaster` has the wm leg faster than the router leg"
    assert over_1s == 1, "only `wmfaster` exceeds an absolute 1s gap"
    assert relative_groups == 2, (
        "`shortcall` and `wmfaster` trip the 5% relative threshold — and "
        "`shortcall` is the same 183ms wrapper overhead as `twinlike`, which "
        "does not. That is the bias this assertion exists to record."
    )


async def test_the_latency_shape_query_ignores_pairs_without_latency(db_session):
    """`pairs` is reported precisely because NULL latency is invisible to the rest.

    In prod coverage happened to be total (0 of 21,853 missing). A future
    producer that stops stamping latency must make the instrument report a
    smaller `pairs`, not silently shrink every other number.
    """
    tenant = uuid.uuid4()
    await _insert(
        db_session,
        tenant_id=tenant,
        service_key="inference-router",
        correlation_id=None,
        model="nolatency",
        served_model="nolatency",
        latency_ms=None,
    )
    await _insert(
        db_session,
        tenant_id=tenant,
        correlation_id=None,
        model="nolatency",
        served_model="nolatency",
        latency_ms=5_000,
        created_at=DAY + timedelta(seconds=5),
    )
    await db_session.commit()
    assert await _park_loose(db_session) == 1

    pairs = (await db_session.execute(text(migration.LOOSE_LATENCY_SHAPE_SQL))).one()[0]
    assert pairs == 0, "the pair is parked but unmeasurable, and must be reported as such"


def _downgrade() -> None:
    cfg = _alembic_config()
    command.stamp(cfg, REVISION)
    command.downgrade(cfg, PREVIOUS_REVISION)


async def test_downgrade_un_parks_both_reasons(db_session):
    """A downgrade of 0013 means "undo 0013" — both rules, not just the strict one.

    Un-parking only the strict reason would leave the loose rows excluded with
    the migration marked as reversed: money missing from the invoice with
    nothing in the schema history saying why.
    """
    tenant = uuid.uuid4()
    await _seed_mixed(db_session, tenant)
    billable_before = await _totals(db_session)
    await _park(db_session)
    await _park_loose(db_session)
    assert len(await _parked_ids(db_session)) == 2

    await asyncio.to_thread(_downgrade)

    assert await _parked_ids(db_session) == set()
    assert await _totals(db_session) == billable_before
    await db_session.execute(text("DROP TABLE IF EXISTS alembic_version"))
    await db_session.commit()
