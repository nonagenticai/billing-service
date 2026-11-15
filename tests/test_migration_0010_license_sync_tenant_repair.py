"""Behavioural tests for alembic ``0010_license_sync_tenant_repair``.

These run the REAL migration through the REAL alembic runner (``command.stamp``
to 0009 + ``command.upgrade`` to 0010) against the test Postgres, with real rows
seeded first — not an assertion about the migration's SQL text, and never against
a pristine/empty schema alone (billing #75 shipped a guard that "passed"
precisely because zero rows were ever written).

Three groups matter most:

* ``TestGuardARefuses…`` — a guard that cannot fire is not a guard.
* ``TestEmptyDatabase`` — and a guard that cannot PASS is a deploy outage: CI
  runs ``alembic upgrade head`` against an empty ephemeral ``postgres:18``
  before any test, and the ``migrate`` init container runs it on every roll.
* ``TestWritePathUnderTheIndex`` — the index changes what the app's own write
  path is allowed to do. These drive ``upsert_license_sync_budget`` and the
  ``/api/budget/sync-from-licenses`` endpoint against a DB that HAS the index,
  with the tenant changed upstream: the exact event that created this bug.

Isolation: alembic opens its OWN connection, so every seed must be COMMITTED
before the migration runs, and the migration's effects (the partial unique index
and the ``alembic_version`` table) are torn down after each test — neither is
part of ``Base.metadata`` so conftest's TRUNCATE does not clean them up.

THE PRE-0011 COLUMN TYPE IS PART OF THE FIXTURE, NOT A CONVENIENCE
------------------------------------------------------------------
``0011_tenant_id_is_a_uuid`` turns ``tenant_id`` into a real Postgres ``uuid``
on 14 tables, and conftest builds the test schema with
``Base.metadata.create_all`` — i.e. at HEAD — so under pytest the column is
already ``uuid`` before 0010 has run. 0010 was written for the world in which it
actually ran, where the column was ``varchar(255)`` and the tenants of record
were readable strings.

Re-seeding those strings as derived uuids (``T("portal")``) would not fix a
single test here, and measuring it says so: 0010 fails on an **empty** table.
Its Guard A folds the literal tuple ``('…'::uuid, 'portal', '…'::uuid)`` at plan
time and raises ``invalid input syntax for type uuid: "portal"`` before it reads
a row. The legacy strings are IN THE MIGRATION; no choice of seed can remove
them. So each test restores the pre-0011 type of the one table 0010 touches
(:data:`LEGACY_VARCHAR_TABLES`) and puts it back in teardown — which also keeps
the subject intact: this file is about repairing legacy *readable* tenant
values, and asserting that against uuids would delete what it tests.
"""

from __future__ import annotations

import asyncio
import os
import uuid
from datetime import UTC, datetime, timedelta
from decimal import Decimal
from pathlib import Path
from unittest.mock import patch

import psycopg
import pytest
from alembic import command
from alembic.config import Config
from alembic.script import ScriptDirectory
from sqlalchemy import select, text
from sqlalchemy.exc import DBAPIError, IntegrityError

from models.db import TokenBudget
from models.domain import BudgetPeriod
from services.budget import upsert_license_sync_budget
from tests.conftest import TEST_DATABASE_URL, T
from tests.test_budget_license_sync import _make_stub_redis, _mock_licenses_client

pytestmark = pytest.mark.unit

REPO_ROOT = Path(__file__).resolve().parents[1]

REVISION = "0010_license_sync_tenant_repair"
PREVIOUS_REVISION = "0009_budget_period_anchor"
INDEX_NAME = "uq_token_budgets_license_sync_active"

# The three prod rows this migration repairs, derived from the corpus on
# worker4 2026-08-10 (every token_budgets row whose tenant_id is not UUID-shaped,
# cross-checked against licenses.license for the tenant of record).
BUDGET_PORTAL = uuid.UUID("019dcaf3-6782-7176-b801-188012fe7dfe")
BUDGET_N8N_TEST = uuid.UUID("019dcaf3-67c0-7456-bf6c-2873c987059a")
BUDGET_GERARD = uuid.UUID("019dcaf3-69ef-7293-9ba3-8e3327506c0d")

LICENSE_PORTAL = uuid.UUID("a005bf37-ee63-43cc-b58a-6d2e62a82e4e")
LICENSE_N8N_TEST = uuid.UUID("9d96dc27-2a27-4567-8b5d-804f36a117dc")
LICENSE_GERARD = uuid.UUID("019dbfa3-2dae-70d1-9bd4-cea20fe2c5e2")

TENANT_PORTAL = "019de842-c92b-7564-b932-98240d274b3b"
TENANT_N8N_TEST = "019de842-c92b-752e-b39c-30d64950e1cc"
TENANT_GERARD = "019de842-c92b-74cd-a7ce-848302bce3b2"

# (budget id, stale tenant name, licence id, tenant uuid, consumed_tokens)
STALE_ROWS = (
    (BUDGET_PORTAL, "portal", LICENSE_PORTAL, TENANT_PORTAL, 0),
    (BUDGET_N8N_TEST, "n8n-test", LICENSE_N8N_TEST, TENANT_N8N_TEST, 0),
    (BUDGET_GERARD, "gerard-dev", LICENSE_GERARD, TENANT_GERARD, 12),
)

NIL_LICENSE_ID = uuid.UUID("00000000-0000-0000-0000-000000000000")
SEEDED_AT = datetime(2026, 1, 2, 3, 4, 5, tzinfo=UTC)

NEIGHBOUR_HEALTHY = uuid.UUID("019dcaf3-0000-7000-8000-00000000000a")
NEIGHBOUR_MANUAL = uuid.UUID("019dcaf3-0000-7000-8000-00000000000b")
NEIGHBOUR_TENANT_WIDE = uuid.UUID("019dcaf3-0000-7000-8000-00000000000c")


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _alembic_config() -> Config:
    """Alembic config WITHOUT an ini file.

    ``alembic.ini`` carries a ``[loggers]`` section, and ``env.py`` feeds it to
    ``logging.config.fileConfig``, whose ``disable_existing_loggers`` default
    would silence every logger already configured in this pytest session and
    poison unrelated tests. Building the Config programmatically leaves
    ``config_file_name`` None so ``env.py`` skips that call. The URL comes from
    ``DATABASE_URL`` (conftest sets it to TEST_DATABASE_URL) inside ``env.py``.
    """
    cfg = Config()
    cfg.set_main_option("script_location", str(REPO_ROOT / "alembic"))
    return cfg


def _upgrade() -> None:
    cfg = _alembic_config()
    command.stamp(cfg, PREVIOUS_REVISION)
    command.upgrade(cfg, REVISION)


def _downgrade() -> None:
    command.downgrade(_alembic_config(), PREVIOUS_REVISION)


async def _run_upgrade() -> None:
    await asyncio.to_thread(_upgrade)


async def _run_downgrade() -> None:
    await asyncio.to_thread(_downgrade)


def _sync_dsn() -> str:
    return os.environ.get("DATABASE_URL", TEST_DATABASE_URL).replace("+psycopg", "")


def _drop_migration_artifacts_sync() -> None:
    with psycopg.connect(_sync_dsn(), autocommit=True) as conn, conn.cursor() as cur:
        cur.execute(f"DROP INDEX IF EXISTS {INDEX_NAME}")
        cur.execute("DROP TABLE IF EXISTS alembic_version")


# The tables whose ``tenant_id`` 0010 reads or writes. ONLY ``token_budgets``:
# the migration names no other table, so nothing else is reverted — a wider flip
# would be a schema change this file does not test and cannot justify.
#
# ``token_budgets`` carries only plain (non-functional) indexes on ``tenant_id``
# (``models/db/budgets.py:86-94``), which ``ALTER COLUMN … TYPE`` rebuilds by
# itself. There is deliberately nothing here like 0011's ``markup_rules``
# COALESCE index dance: that table is not in this migration's blast radius.
LEGACY_VARCHAR_TABLES = ("token_budgets",)

_TO_VARCHAR = tuple(
    f'ALTER TABLE "{t}" ALTER COLUMN tenant_id TYPE VARCHAR(255) USING tenant_id::text'
    for t in LEGACY_VARCHAR_TABLES
)
# Purge first: the legacy strings this file seeds on purpose ('portal',
# 'gerard-dev', …) have no ``::uuid``, so the restoring ALTER would fail on them
# and leave the schema varchar for every test that runs after this file in the
# same schema. conftest TRUNCATEs at the START of each db test anyway, so
# emptying the table here costs nothing and cannot hide a leak.
_TO_UUID = tuple(
    stmt
    for t in LEGACY_VARCHAR_TABLES
    for stmt in (
        f'TRUNCATE "{t}" RESTART IDENTITY CASCADE',
        f'ALTER TABLE "{t}" ALTER COLUMN tenant_id TYPE uuid USING tenant_id::uuid',
    )
)


async def _set_legacy_varchar(engine) -> None:
    async with engine.begin() as conn:
        for stmt in _TO_VARCHAR:
            await conn.execute(text(stmt))


async def _restore_uuid(engine) -> None:
    async with engine.begin() as conn:
        for stmt in _TO_UUID:
            await conn.execute(text(stmt))


def _set_legacy_varchar_sync() -> None:
    with psycopg.connect(_sync_dsn(), autocommit=True) as conn, conn.cursor() as cur:
        for stmt in _TO_VARCHAR:
            cur.execute(stmt)


def _restore_uuid_sync() -> None:
    with psycopg.connect(_sync_dsn(), autocommit=True) as conn, conn.cursor() as cur:
        for stmt in _TO_UUID:
            cur.execute(stmt)


_INSERT = text(
    """
    INSERT INTO token_budgets (
        id, tenant_id, tenant_name, license_id, budget_tokens, period,
        includes_input, includes_output, active, priority, starts_at,
        expires_at, consumed_tokens, source, created_at, updated_at
    ) VALUES (
        :id, :tenant_id, :tenant_name, :license_id, :budget_tokens, :period,
        true, true, :active, 100, :ts,
        NULL, :consumed, :source, :ts, :ts
    )
    """
)


async def _seed(
    conn,
    *,
    id: uuid.UUID,
    tenant_id: str,
    license_id: uuid.UUID,
    tenant_name: str | None = None,
    budget_tokens: int = 10_000_000,
    period: str = "monthly",
    active: bool = True,
    source: str = "license_sync",
    consumed: int = 0,
) -> None:
    await conn.execute(
        _INSERT,
        {
            "id": id,
            "tenant_id": tenant_id,
            "tenant_name": tenant_name if tenant_name is not None else tenant_id,
            "license_id": license_id,
            "budget_tokens": budget_tokens,
            "period": period,
            "active": active,
            "source": source,
            "consumed": consumed,
            "ts": SEEDED_AT,
        },
    )


async def _seed_neighbours(conn) -> None:
    """Realistic rows that must survive the repair untouched.

    Without them an over-broad UPDATE (one keyed on nothing but
    ``source='license_sync'``) would still pass the positive assertions.
    """
    # A healthy license_sync row already on a UUID tenant.
    await _seed(
        conn,
        id=NEIGHBOUR_HEALTHY,
        tenant_id="019de842-c92b-7000-b000-00000000000a",
        license_id=uuid.UUID("11111111-1111-4111-8111-111111111111"),
    )
    # A manual (stacking) row on the SAME licence as the portal budget.
    await _seed(
        conn,
        id=NEIGHBOUR_MANUAL,
        tenant_id=TENANT_PORTAL,
        license_id=LICENSE_PORTAL,
        source="manual",
        budget_tokens=500,
    )
    # A tenant-wide (nil licence) row.
    await _seed(
        conn,
        id=NEIGHBOUR_TENANT_WIDE,
        tenant_id=TENANT_PORTAL,
        license_id=NIL_LICENSE_ID,
        budget_tokens=42,
    )


async def _seed_prod_shape(engine, *, skip: set[uuid.UUID] | None = None) -> None:
    skip = skip or set()
    async with engine.begin() as conn:
        for budget_id, stale_tenant, license_id, _, consumed in STALE_ROWS:
            if budget_id in skip:
                continue
            await _seed(
                conn,
                id=budget_id,
                tenant_id=stale_tenant,
                license_id=license_id,
                consumed=consumed,
            )
        await _seed_neighbours(conn)


async def _row(engine, budget_id: uuid.UUID) -> dict:
    async with engine.connect() as conn:
        res = await conn.execute(
            text(
                "SELECT tenant_id, tenant_name, license_id, budget_tokens, "
                "consumed_tokens, source, active, period, updated_at "
                "FROM token_budgets WHERE id = :id"
            ),
            {"id": budget_id},
        )
        return dict(res.mappings().one())


async def _index_def(engine) -> str | None:
    async with engine.connect() as conn:
        res = await conn.execute(
            text("SELECT indexdef FROM pg_indexes WHERE indexname = :n"),
            {"n": INDEX_NAME},
        )
        return res.scalar_one_or_none()


def _raised(excinfo) -> str:
    """The message Postgres actually RAISEd — not the echoed statement.

    ``str(DBAPIError)`` appends the full ``[SQL: ...]`` text of the failing
    statement. Both guards live in ONE DO block, so that echo contains BOTH
    RAISE messages verbatim: asserting ``"diverged" in str(excinfo.value)``
    passes no matter which guard fired, and no matter whether either fired for
    the right reason. ``.orig`` is the psycopg exception, whose str() is the
    diagnostic alone. Two mutants (Guard A dropping ``active`` / ``period``)
    survived the battery against the echoed form and are killed by this one.
    """
    return str(excinfo.value.orig)


@pytest.fixture
async def migration_db(db_engine):
    """The 0010 world, and teardown of everything conftest cannot truncate.

    Setup restores the pre-0011 ``varchar(255)`` ``tenant_id`` (see the module
    docstring: 0010's own SQL is untypeable against a ``uuid`` column, even on an
    empty table). Teardown must undo ALL THREE of the things this file leaves
    behind — the partial unique index, ``alembic_version`` and the column type —
    because none of them is in ``Base.metadata`` and every later test in the same
    schema inherits them. In particular ``db_engine``'s nil-tenant tripwire
    compares ``tenant_id = '…'::uuid``, which errors outright against a varchar
    column; the restore therefore runs HERE, in the fixture that depends on
    ``db_engine``, so it is finalised before that teardown.
    """
    await _set_legacy_varchar(db_engine)
    try:
        yield db_engine
    finally:
        async with db_engine.begin() as conn:
            await conn.execute(text(f"DROP INDEX IF EXISTS {INDEX_NAME}"))
            await conn.execute(text("DROP TABLE IF EXISTS alembic_version"))
        await _restore_uuid(db_engine)


@pytest.fixture
def indexed_db_sync(db_engine):
    """Sync-callable fixture that applies the REAL migration to an empty table.

    For the TestClient (sync) tests. Relies on Guard A's ``stale = 0`` arm — the
    migration must be applicable to a database that has none of the stale rows,
    which is the same property CI and the migrate initContainer depend on.

    Same pre-0011 column type as ``migration_db``, and the same obligation to put
    it back: this fixture depends on ``db_engine`` too, so its teardown runs
    before the tripwire.
    """
    _set_legacy_varchar_sync()
    try:
        _upgrade()
        yield
    finally:
        _drop_migration_artifacts_sync()
        _restore_uuid_sync()


# ---------------------------------------------------------------------------
# Chain wiring
# ---------------------------------------------------------------------------


class TestRevisionChain:
    def test_0010_follows_0009_on_the_single_head_path(self) -> None:
        """One head, and 0010 sits on the path to it directly after 0009.

        This used to assert ``get_heads() == [REVISION]``. That is no longer
        true and must not be made true again: ``0011_tenant_id_is_a_uuid`` was
        added on top of 0010, so 0010 is now an interior revision. What the test
        was actually protecting survives unweakened — the chain still has exactly
        ONE head (a second head means two migrations race for the same
        ``alembic_version`` row), 0010 still descends from 0009, and 0010 is
        still reachable from that head, so ``alembic upgrade head`` still runs
        it. Asserting reachability rather than identity is what makes the test
        outlive the next migration instead of failing on it.
        """
        script = ScriptDirectory.from_config(_alembic_config())
        heads = list(script.get_heads())
        assert len(heads) == 1, f"the chain must not branch, found heads: {heads}"
        rev = script.get_revision(REVISION)
        assert rev.down_revision == PREVIOUS_REVISION
        path = {r.revision for r in script.iterate_revisions(heads[0], "base")}
        assert REVISION in path, f"{REVISION} is not reachable from head {heads[0]}"

    def test_revision_id_fits_alembic_version_column(self) -> None:
        """``alembic_version.version_num`` is varchar(32)."""
        assert len(REVISION) <= 32


# ---------------------------------------------------------------------------
# The empty-database arm — CI and the migrate initContainer depend on it
# ---------------------------------------------------------------------------


class TestEmptyDatabase:
    async def test_upgrade_succeeds_on_an_empty_table(self, migration_db) -> None:
        """`alembic upgrade head` must not abort where there is nothing to repair.

        CI runs this against an ephemeral empty postgres:18 BEFORE ruff/ty/pytest,
        and billing's migrate initContainer runs it on every roll — on a fresh,
        staging or DR-restored database there are no stale rows at all. A guard
        that aborts at n=0 is a permanent CrashLoop, not a safety feature.
        """
        async with migration_db.connect() as conn:
            count = await conn.execute(text("SELECT count(*) FROM token_budgets"))
            assert count.scalar_one() == 0, "precondition: table must be empty"

        await _run_upgrade()

        assert await _index_def(migration_db) is not None

    async def test_upgrade_succeeds_when_rows_exist_but_none_are_stale(self, migration_db) -> None:
        """A healthy prod (already repaired by hand) is also the n=0 arm."""
        async with migration_db.begin() as conn:
            await _seed_neighbours(conn)
            for budget_id, stale_tenant, license_id, tenant_uuid, consumed in STALE_ROWS:
                await _seed(
                    conn,
                    id=budget_id,
                    tenant_id=tenant_uuid,
                    tenant_name=stale_tenant,
                    license_id=license_id,
                    consumed=consumed,
                )

        await _run_upgrade()

        assert await _index_def(migration_db) is not None
        for budget_id, _, _, tenant_uuid, _ in STALE_ROWS:
            assert (await _row(migration_db, budget_id))["tenant_id"] == tenant_uuid


# ---------------------------------------------------------------------------
# upgrade() — the repair
# ---------------------------------------------------------------------------


class TestUpgradeRepairsTheStaleRows:
    async def test_all_three_tenant_ids_corrected(self, migration_db) -> None:
        await _seed_prod_shape(migration_db)

        await _run_upgrade()

        for budget_id, stale_tenant, license_id, tenant_uuid, consumed in STALE_ROWS:
            row = await _row(migration_db, budget_id)
            assert row["tenant_id"] == tenant_uuid, f"{stale_tenant} not repaired"
            assert row["tenant_name"] == stale_tenant
            assert row["license_id"] == license_id
            assert row["active"] is True
            assert int(row["consumed_tokens"]) == consumed, "consumption must survive"

    async def test_gerard_dev_row_is_in_scope(self, migration_db) -> None:
        """The row the hand-listed scope missed — and the only one with usage."""
        await _seed_prod_shape(migration_db)

        await _run_upgrade()

        row = await _row(migration_db, BUDGET_GERARD)
        assert row["tenant_id"] == TENANT_GERARD
        assert int(row["consumed_tokens"]) == 12

    async def test_updated_at_is_bumped(self, migration_db) -> None:
        await _seed_prod_shape(migration_db)

        await _run_upgrade()

        for budget_id, *_ in STALE_ROWS:
            row = await _row(migration_db, budget_id)
            assert row["updated_at"] > SEEDED_AT + timedelta(days=1), (
                f"{budget_id} updated_at was not bumped by the repair"
            )

    async def test_neighbour_rows_are_untouched(self, migration_db) -> None:
        """The UPDATEs are keyed on the full stale tuple, not a broad predicate."""
        await _seed_prod_shape(migration_db)

        await _run_upgrade()

        healthy = await _row(migration_db, NEIGHBOUR_HEALTHY)
        assert healthy["tenant_id"] == "019de842-c92b-7000-b000-00000000000a"
        assert healthy["updated_at"] == SEEDED_AT

        manual = await _row(migration_db, NEIGHBOUR_MANUAL)
        assert manual["source"] == "manual"
        assert manual["budget_tokens"] == 500
        assert manual["updated_at"] == SEEDED_AT

        tenant_wide = await _row(migration_db, NEIGHBOUR_TENANT_WIDE)
        assert tenant_wide["license_id"] == NIL_LICENSE_ID
        assert tenant_wide["updated_at"] == SEEDED_AT

    async def test_index_created_with_licence_keyed_predicate(self, migration_db) -> None:
        await _seed_prod_shape(migration_db)

        await _run_upgrade()

        indexdef = await _index_def(migration_db)
        assert indexdef is not None, f"{INDEX_NAME} was not created"
        normalised = " ".join(indexdef.lower().split())
        assert "unique index" in normalised
        # LICENCE-keyed, not tenant-keyed — the whole point of the guard.
        assert "(license_id, period)" in normalised
        assert "tenant_id" not in normalised
        assert "where" in normalised
        assert "'license_sync'" in normalised
        assert "active" in normalised
        assert "00000000-0000-0000-0000-000000000000" in normalised


# ---------------------------------------------------------------------------
# The guards — a guard that cannot fire is not a guard
# ---------------------------------------------------------------------------


class TestGuardARefusesADivergedProd:
    async def test_raises_when_only_two_of_three_stale_rows_remain(self, migration_db) -> None:
        """Prod partially repaired by hand — must NOT blind-fill the remainder."""
        await _seed_prod_shape(migration_db, skip={BUDGET_GERARD})
        async with migration_db.begin() as conn:
            await _seed(
                conn,
                id=BUDGET_GERARD,
                tenant_id=TENANT_GERARD,
                tenant_name="gerard-dev",
                license_id=LICENSE_GERARD,
                consumed=12,
            )

        with pytest.raises(DBAPIError) as excinfo:
            await _run_upgrade()
        assert "diverged from the state" in _raised(excinfo)

        # Nothing repaired, nothing created — the whole migration rolled back.
        assert (await _row(migration_db, BUDGET_PORTAL))["tenant_id"] == "portal"
        assert await _index_def(migration_db) is None

    async def test_raises_when_only_one_stale_row_remains(self, migration_db) -> None:
        await _seed_prod_shape(migration_db, skip={BUDGET_GERARD, BUDGET_N8N_TEST})

        with pytest.raises(DBAPIError) as excinfo:
            await _run_upgrade()
        assert "diverged from the state" in _raised(excinfo)
        assert await _index_def(migration_db) is None

    async def test_raises_when_a_stale_row_is_inactive(self, migration_db) -> None:
        """``active`` is part of the tuple — a deactivated row is not repairable."""
        await _seed_prod_shape(migration_db, skip={BUDGET_GERARD})
        async with migration_db.begin() as conn:
            await _seed(
                conn,
                id=BUDGET_GERARD,
                tenant_id="gerard-dev",
                license_id=LICENSE_GERARD,
                consumed=12,
                active=False,
            )

        with pytest.raises(DBAPIError) as excinfo:
            await _run_upgrade()
        assert "diverged from the state" in _raised(excinfo)
        assert (await _row(migration_db, BUDGET_PORTAL))["tenant_id"] == "portal"

    async def test_raises_when_a_stale_row_is_not_monthly(self, migration_db) -> None:
        """``period='monthly'`` is part of Guard A's tuple.

        Without that predicate the guard would count a weekly row as one of the
        three and wave through a state the repair does not cover (every UPDATE
        pins period='monthly', so the weekly row would be left stale while the
        migration stamped as applied).
        """
        await _seed_prod_shape(migration_db, skip={BUDGET_GERARD})
        async with migration_db.begin() as conn:
            await _seed(
                conn,
                id=BUDGET_GERARD,
                tenant_id="gerard-dev",
                license_id=LICENSE_GERARD,
                consumed=12,
                period="weekly",
            )

        with pytest.raises(DBAPIError) as excinfo:
            await _run_upgrade()
        # Must be GUARD A's message: with the period predicate removed Guard A
        # counts 3 and it is Guard B that trips instead.
        assert "diverged from the state" in _raised(excinfo)
        assert (await _row(migration_db, BUDGET_GERARD))["tenant_id"] == "gerard-dev"
        assert await _index_def(migration_db) is None


class TestGuardBRefusesAnAlreadyDuplicatedProd:
    async def test_raises_when_the_sweep_already_inserted_a_duplicate(self, migration_db) -> None:
        await _seed_prod_shape(migration_db)
        async with migration_db.begin() as conn:
            # What the licences reconcile did when it could not see the stale
            # row: a SECOND active license_sync row for the same licence.
            await _seed(
                conn,
                id=uuid.UUID("019dcaf3-0000-7000-8000-00000000000e"),
                tenant_id=TENANT_PORTAL,
                license_id=LICENSE_PORTAL,
            )

        with pytest.raises(DBAPIError) as excinfo:
            await _run_upgrade()
        message = _raised(excinfo)
        assert "already duplicated" in message
        assert "Deactivate the surplus" in message

        # Guard B fires BEFORE any write — the stale rows are still stale.
        for budget_id, stale_tenant, *_ in STALE_ROWS:
            assert (await _row(migration_db, budget_id))["tenant_id"] == stale_tenant
        assert await _index_def(migration_db) is None

    async def test_a_weekly_license_sync_row_does_not_trip_guard_b(self, migration_db) -> None:
        """Guard B is scoped to monthly, matching the index grain.

        A weekly license_sync budget for an affected licence is not a duplicate —
        the index would accept it — so it must not abort the roll.
        """
        await _seed_prod_shape(migration_db)
        async with migration_db.begin() as conn:
            await _seed(
                conn,
                id=uuid.UUID("019dcaf3-0000-7000-8000-000000000014"),
                tenant_id=TENANT_PORTAL,
                license_id=LICENSE_PORTAL,
                period="weekly",
            )

        await _run_upgrade()

        assert (await _row(migration_db, BUDGET_PORTAL))["tenant_id"] == TENANT_PORTAL
        assert await _index_def(migration_db) is not None


# ---------------------------------------------------------------------------
# The structural guard — and its negative controls
# ---------------------------------------------------------------------------


class TestUniqueIndexBehaviour:
    async def test_rejects_a_second_active_license_sync_row_for_the_same_licence(
        self, migration_db
    ) -> None:
        await _seed_prod_shape(migration_db)
        await _run_upgrade()

        with pytest.raises(IntegrityError) as excinfo:
            async with migration_db.begin() as conn:
                await _seed(
                    conn,
                    id=uuid.UUID("019dcaf3-0000-7000-8000-00000000000f"),
                    tenant_id="a-completely-different-tenant",
                    license_id=LICENSE_PORTAL,
                )
        assert INDEX_NAME in str(excinfo.value)

    async def test_permits_a_second_manual_row_for_the_same_licence(self, migration_db) -> None:
        """Negative control: source='manual' stacking is the product feature."""
        await _seed_prod_shape(migration_db)
        await _run_upgrade()

        async with migration_db.begin() as conn:
            await _seed(
                conn,
                id=uuid.UUID("019dcaf3-0000-7000-8000-000000000010"),
                tenant_id=TENANT_PORTAL,
                license_id=LICENSE_PORTAL,
                source="manual",
                budget_tokens=250,
            )

        row = await _row(migration_db, uuid.UUID("019dcaf3-0000-7000-8000-000000000010"))
        assert row["source"] == "manual"

    async def test_permits_a_second_nil_licence_license_sync_row(self, migration_db) -> None:
        """Negative control: the nil UUID marks TENANT-WIDE budgets, plural."""
        await _seed_prod_shape(migration_db)
        await _run_upgrade()

        async with migration_db.begin() as conn:
            await _seed(
                conn,
                id=uuid.UUID("019dcaf3-0000-7000-8000-000000000011"),
                tenant_id=TENANT_N8N_TEST,
                license_id=NIL_LICENSE_ID,
                budget_tokens=7,
            )

        row = await _row(migration_db, uuid.UUID("019dcaf3-0000-7000-8000-000000000011"))
        assert row["license_id"] == NIL_LICENSE_ID

    async def test_permits_a_second_row_once_the_first_is_deactivated(self, migration_db) -> None:
        """Negative control: the reconcile's deactivate-then-insert path survives."""
        await _seed_prod_shape(migration_db)
        await _run_upgrade()

        async with migration_db.begin() as conn:
            await conn.execute(
                text("UPDATE token_budgets SET active = false WHERE id = :id"),
                {"id": BUDGET_PORTAL},
            )
            await _seed(
                conn,
                id=uuid.UUID("019dcaf3-0000-7000-8000-000000000012"),
                tenant_id=TENANT_PORTAL,
                license_id=LICENSE_PORTAL,
            )

        row = await _row(migration_db, uuid.UUID("019dcaf3-0000-7000-8000-000000000012"))
        assert row["active"] is True

    async def test_permits_the_same_licence_on_a_different_period(self, migration_db) -> None:
        """Negative control: ``period`` is part of the key."""
        await _seed_prod_shape(migration_db)
        await _run_upgrade()

        async with migration_db.begin() as conn:
            await _seed(
                conn,
                id=uuid.UUID("019dcaf3-0000-7000-8000-000000000013"),
                tenant_id=TENANT_PORTAL,
                license_id=LICENSE_PORTAL,
                period="weekly",
            )

        row = await _row(migration_db, uuid.UUID("019dcaf3-0000-7000-8000-000000000013"))
        assert row["period"] == "weekly"


# ---------------------------------------------------------------------------
# The write path under the index — the scenario the migration exists for
# ---------------------------------------------------------------------------


class TestWritePathUnderTheIndex:
    """``upsert_license_sync_budget`` must survive a tenant change upstream.

    A licence moving tenants is EXACTLY the event that produced the three stale
    rows. With the index in place and a tenant-keyed SELECT, that event turns
    the reconcile's harmless duplicate row into an IntegrityError — and
    ``routers/budget.py`` catches only ``httpx.RequestError``, so it becomes a
    500 that rolls back every budget and quota write of the run.

    WHY THESE FOUR SEED ``T("legacy-name")`` AND THE REST SEED THE RAW STRING
    ------------------------------------------------------------------------
    Every other test in this file seeds ``token_budgets`` with raw SQL and is
    asserting about the MIGRATION, whose own literals are readable strings — so
    it runs against the pre-0011 varchar column and seeds what 0010 really saw.
    These four assert about the APP's write path AS IT SHIPS TODAY, and the
    shipped models declare ``Uuid(as_uuid=False)``: SQLAlchemy renders
    ``%(tenant_id)s::UUID`` into the INSERT, so ``upsert_license_sync_budget``
    can no longer carry a readable label at all, whatever the column type is.
    Seeding one here would test a writer that no longer exists. The subject is
    the tenant CHANGE, not the spelling of the old tenant, and it is fully
    preserved: the row starts on one uuid and the licence moves to another.
    """

    async def test_tenant_change_updates_in_place_instead_of_violating_the_index(
        self, migration_db, db_session
    ) -> None:
        lic_id = uuid.uuid4()
        first, created = await upsert_license_sync_budget(
            db_session,
            tenant_id=T("legacy-name"),
            license_id=str(lic_id),
            period=BudgetPeriod.monthly,
            budget_tokens=10_000,
        )
        assert created is True
        await db_session.commit()

        await _run_upgrade()  # the index lands on a live row

        second, created_again = await upsert_license_sync_budget(
            db_session,
            tenant_id=TENANT_PORTAL,  # the licence moved to its UUID tenant
            license_id=str(lic_id),
            period=BudgetPeriod.monthly,
            budget_tokens=20_000,
        )
        await db_session.commit()

        assert created_again is False, "a tenant change must reconcile in place"
        assert second.id == first.id
        assert second.tenant_id == TENANT_PORTAL
        assert second.tenant_name == TENANT_PORTAL
        assert second.budget_tokens == 20_000

        rows = (
            (await db_session.execute(select(TokenBudget).where(TokenBudget.license_id == lic_id)))
            .scalars()
            .all()
        )
        assert len(list(rows)) == 1, "no duplicate row may be created"

    async def test_consumption_survives_a_tenant_change(self, migration_db, db_session) -> None:
        """The gerard-dev shape: the moved row carries usage that must not reset."""
        lic_id = uuid.uuid4()
        budget, _ = await upsert_license_sync_budget(
            db_session,
            tenant_id=T("gerard-dev"),
            license_id=str(lic_id),
            period=BudgetPeriod.monthly,
            budget_tokens=10_000,
        )
        budget.consumed_tokens = Decimal("12")
        await db_session.commit()

        await _run_upgrade()

        moved, created = await upsert_license_sync_budget(
            db_session,
            tenant_id=TENANT_GERARD,
            license_id=str(lic_id),
            period=BudgetPeriod.monthly,
            budget_tokens=10_000,
        )
        await db_session.commit()

        assert created is False
        assert int(moved.consumed_tokens) == 12

    async def test_concurrent_insert_is_absorbed_not_propagated(
        self, migration_db, db_session, monkeypatch
    ) -> None:
        """Losing the insert race must converge, not poison the transaction.

        Simulates the interleaving by forcing the pre-INSERT lookup to miss while
        a row really is present: the SAVEPOINT recovery has to re-select and
        update rather than let IntegrityError escape.
        """
        lic_id = uuid.uuid4()
        await upsert_license_sync_budget(
            db_session,
            tenant_id=T("legacy-name"),
            license_id=str(lic_id),
            period=BudgetPeriod.monthly,
            budget_tokens=10_000,
        )
        await db_session.commit()
        await _run_upgrade()

        import services.budget_crud as crud

        real = crud._select_active_license_sync_budget
        calls = {"n": 0}

        async def _miss_once(session, license_id, period):
            calls["n"] += 1
            if calls["n"] == 1:
                return None  # the racing writer had not committed yet
            return await real(session, license_id, period)

        monkeypatch.setattr(crud, "_select_active_license_sync_budget", _miss_once)

        row, created = await upsert_license_sync_budget(
            db_session,
            tenant_id=TENANT_PORTAL,
            license_id=str(lic_id),
            period=BudgetPeriod.monthly,
            budget_tokens=33_000,
        )
        await db_session.commit()

        assert calls["n"] == 2, "the recovery path must re-select"
        assert created is False
        assert row.budget_tokens == 33_000
        rows = (
            (await db_session.execute(select(TokenBudget).where(TokenBudget.license_id == lic_id)))
            .scalars()
            .all()
        )
        assert len(list(rows)) == 1

    def test_sync_from_licenses_survives_a_tenant_change(
        self, app_client, monkeypatch, indexed_db_sync
    ) -> None:
        """End-to-end through the endpoint: no 500, no duplicate, no lost run."""
        import redis_client

        monkeypatch.setattr(redis_client, "_redis", _make_stub_redis())

        lic_id = str(uuid.uuid4())
        other_lic = str(uuid.uuid4())
        payload_v1 = [
            {"id": lic_id, "tenant_id": T("portal"), "plan": {"token_budget_monthly": 10_000}},
            {"id": other_lic, "tenant_id": T("portal"), "plan": {"token_budget_monthly": 5_000}},
        ]
        payload_v2 = [
            {"id": lic_id, "tenant_id": TENANT_PORTAL, "plan": {"token_budget_monthly": 10_000}},
            {"id": other_lic, "tenant_id": TENANT_PORTAL, "plan": {"token_budget_monthly": 5_000}},
        ]

        with patch(
            "routers.budget.httpx.AsyncClient", return_value=_mock_licenses_client(payload_v1)
        ):
            r1 = app_client.post("/api/budget/sync-from-licenses")
        assert r1.status_code == 200, r1.text
        assert r1.json()["created_count"] == 2

        with patch(
            "routers.budget.httpx.AsyncClient", return_value=_mock_licenses_client(payload_v2)
        ):
            r2 = app_client.post("/api/budget/sync-from-licenses")
        assert r2.status_code == 200, r2.text
        # The whole run must complete — not just the first licence.
        assert r2.json()["updated_count"] == 2
        assert r2.json()["created_count"] == 0
        # An `error` from the licences fetch surfaces as a 502, not a key here.
        assert r2.json()["status"] == "ok"
        assert r2.json()["processed_count"] == 2

        with psycopg.connect(_sync_dsn()) as conn, conn.cursor() as cur:
            cur.execute(
                "SELECT license_id, tenant_id, count(*) FROM token_budgets "
                "WHERE source = 'license_sync' AND active GROUP BY 1, 2"
            )
            rows = cur.fetchall()
        assert len(rows) == 2, f"expected one active row per licence, got {rows}"
        assert {str(r[1]) for r in rows} == {TENANT_PORTAL}
        assert all(r[2] == 1 for r in rows)


# ---------------------------------------------------------------------------
# downgrade()
# ---------------------------------------------------------------------------


class TestDowngradeRoundTrip:
    async def test_downgrade_restores_the_legacy_tenants_and_drops_the_index(
        self, migration_db
    ) -> None:
        await _seed_prod_shape(migration_db)
        await _run_upgrade()
        assert (await _row(migration_db, BUDGET_PORTAL))["tenant_id"] == TENANT_PORTAL

        await _run_downgrade()

        for budget_id, stale_tenant, *_ in STALE_ROWS:
            row = await _row(migration_db, budget_id)
            assert row["tenant_id"] == stale_tenant
            assert row["tenant_name"] == stale_tenant
        assert await _index_def(migration_db) is None

    async def test_downgrade_does_not_blind_stamp_an_unrelated_row(self, migration_db) -> None:
        """The reverse UPDATEs are tuple-guarded, like the forward ones.

        If the PK is reused by a row that is NOT what upgrade() produced,
        downgrade must leave it alone rather than stamping a legacy tenant name
        onto it.
        """
        async with migration_db.begin() as conn:
            await _seed(
                conn,
                id=BUDGET_PORTAL,
                tenant_id="somebody-elses-tenant",
                license_id=uuid.UUID("33333333-3333-4333-8333-333333333333"),
                source="manual",
            )
        # No stale rows present → Guard A's n=0 arm → index only, no repair.
        await _run_upgrade()

        await _run_downgrade()

        row = await _row(migration_db, BUDGET_PORTAL)
        assert row["tenant_id"] == "somebody-elses-tenant"
        assert row["source"] == "manual"

    async def test_upgrade_is_replayable_after_downgrade(self, migration_db) -> None:
        """Round-trip leaves prod in a state ``upgrade()`` still accepts."""
        await _seed_prod_shape(migration_db)
        await _run_upgrade()
        await _run_downgrade()

        await _run_upgrade()

        for budget_id, _, _, tenant_uuid, _ in STALE_ROWS:
            assert (await _row(migration_db, budget_id))["tenant_id"] == tenant_uuid
        assert await _index_def(migration_db) is not None
