"""Behavioural tests for alembic ``0012_runtime_ddl_schema_drift``.

``0012`` exists because this service builds part of its schema TWICE: once in
``alembic/versions/*`` and once in the ``ADDITIVE_MIGRATIONS`` tuple of
``src/db.py``, which every pod replays on start. Six objects lived ONLY in the
second one, so a fresh alembic-only database was a genuinely different schema
from production — and that already ABORTED ``0011`` on a fresh database (see the
comment at ``alembic/versions/0011_tenant_id_is_a_uuid.py:725``).

⛔ THE VACUITY TRAP THIS FILE IS BUILT AROUND
---------------------------------------------
``tests/conftest.py`` bootstraps the schema with ``Base.metadata.create_all``,
and the ORM already declares all six objects. Under pytest they are therefore
ALREADY PRESENT (and ``total_cost_usd`` is already ``numeric(20,6)``) before the
migration runs. A test that merely calls ``command.upgrade`` and then asserts
"the column exists" passes against a migration whose statements are all
``IF NOT EXISTS`` and which consequently did nothing at all — it is asserting
``create_all``'s work, not ``0012``'s.

Every test that claims ``0012`` CREATES something therefore drives
:func:`_drop_drift_objects` first, which puts the database into the shape a
fresh alembic-only database is actually in: no ``owner_provided``, no
``rate_class``, no ``tenant_access_charge``, neither access-charge index, and
``billing_cost_daily.total_cost_usd`` narrowed back to the baseline's
``numeric(12,6)``. :class:`TestPreStateIsDrifted` asserts that pre-state
directly — if the trap ever reopens, THAT test fails and names the reason
instead of the rest of the file going quietly green. It also carries two
self-enforcing controls: an ``INSERT`` into ``tenant_access_charge`` cannot even
be issued against the drifted pre-state, and a daily total above $999,999.999999
cannot be stored in it.

The complementary risk — that ``0012`` is NOT a no-op against production, where
all six objects already exist — is covered by :class:`TestIdempotence`, which
runs the migration against the untouched ``create_all`` schema and asserts the
catalogue definitions are byte-identical before and after.

:class:`TestStatementsTrackSrcDbPy` is the ratchet that keeps the two sources of
truth from diverging again: every statement in the migration must appear
VERBATIM in ``src/db.py``'s ``ADDITIVE_MIGRATIONS``. Editing one without the
other reopens the exact defect ``0012`` closes, and fails here.

THE TWO DEFECT CLASSES WITH DEDICATED CLASSES BELOW
---------------------------------------------------
:class:`TestConcurrentCreatorRace` — ``IF NOT EXISTS`` is NOT race-safe. It
consults COMMITTED catalogue rows only, so a session that is mid-``CREATE TABLE``
is invisible, and the loser dies on ``pg_type_typname_nsp_index``. Since
``alembic/env.py`` runs the whole upgrade in ONE transaction, that took the
ENTIRE migration down (measured: rolled back to ``0011``, even the unrelated
``owner_provided`` gone). This is the everyday case here, not an exotic one:
``src/db.py`` replays the same DDL on every pod start. The class drives the real
two-session race, and carries a CONTROL that removes the SAVEPOINT and shows the
abort coming straight back — without it, the passing test would prove nothing.

:class:`TestVerifyChecksDefinitionsNotNames` — ``ADD COLUMN`` /
``CREATE INDEX IF NOT EXISTS`` match by NAME. A database that already held
``rate_class`` as ``text`` or ``ix_accesscharge_open`` as a NON-unique index with
no ``WHERE`` clause got a "verified" stamp from a name-only postcondition, while
still accepting two OPEN access charges for one (tenant, service_key) — the
exact "two rows of record for one bill" the migration exists to prevent.

Isolation
---------
* alembic opens its OWN connection, so anything the tests set up is COMMITTED.
* ``alembic_version`` is not in ``Base.metadata``, so conftest's TRUNCATE never
  touches it; every fixture below drops it on BOTH sides so a crashed test
  cannot hand the next one a stamped schema.
* The fixtures depend on the session-scoped ``_db_engine_session`` rather than
  on ``db_engine`` — same reason as ``test_migration_0011_tenant_id_is_a_uuid``:
  they need a connection alembic's own connection can see, and they do their own
  cleanup. No row written here reaches the nil tenant, and the rows written into
  ``billing_cost_daily`` are written in a transaction that is never committed.
* Dropping ``owner_provided`` takes ``uq_cost_daily`` with it and dropping
  ``rate_class`` takes both ``provider_pricing`` indexes with it. The fixture
  therefore CAPTURES those three definitions from the catalogue before dropping
  and replays them on teardown, rather than retyping them here — a hand-written
  restore is one more copy of the schema that can drift, which is the defect
  under test.
"""

from __future__ import annotations

import asyncio
import importlib.util
import uuid
from datetime import UTC, datetime
from pathlib import Path

import pytest
from alembic import command
from alembic.config import Config
from alembic.script import ScriptDirectory
from sqlalchemy import text
from sqlalchemy.exc import DBAPIError, ProgrammingError, SQLAlchemyError

from db import _RACE_CLASS_ORIG_NAMES, ADDITIVE_MIGRATIONS

pytestmark = pytest.mark.unit

REPO_ROOT = Path(__file__).resolve().parents[1]

REVISION = "0012_runtime_ddl_schema_drift"
PREVIOUS_REVISION = "0011_tenant_id_is_a_uuid"

DRIFT_TABLE = "tenant_access_charge"
DRIFT_INDEXES = ("ix_accesscharge_lookup", "ix_accesscharge_open")
DRIFT_COLUMNS = (("billing_cost_daily", "owner_provided"), ("provider_pricing", "rate_class"))

# The drift that is a TYPE, not a missing object: the baseline creates
# `total_cost_usd` numeric(12,6) (0001_billing_baseline.py:120), src/db.py:77
# widens it to numeric(20,6). Nothing is absent in the pre-state — it is simply
# too narrow to hold a high-volume tenant's daily total, which is the invoice
# source. See TestTheDailyCostWidening.
NARROW_COLUMN = ("billing_cost_daily", "total_cost_usd")
BASELINE_PRECISION = (12, 6)
WIDENED_PRECISION = (20, 6)
# Just over the numeric(12,6) ceiling of $999,999.999999.
OVERFLOWING_DAILY_TOTAL = "1000000.500000"

# Objects that are collateral of dropping the two drift COLUMNS: they are built
# on top of them, so `DROP COLUMN` takes them too. Captured and replayed by the
# fixture, never retyped.
COLLATERAL_INDEXES = ("ix_providerpricing_lookup", "ix_providerpricing_open")
COLLATERAL_CONSTRAINT = ("billing_cost_daily", "uq_cost_daily")


# ---------------------------------------------------------------------------
# The migration module itself, loaded by path (``0012_runtime_ddl_schema_drift``
# is not an importable module name). STATEMENTS is read from the migration
# rather than retyped, so the verbatim-tracking test below cannot go stale.
# ---------------------------------------------------------------------------

_MIGRATION_PATH = REPO_ROOT / "alembic" / "versions" / f"{REVISION}.py"
_spec = importlib.util.spec_from_file_location("_migration_0012", _MIGRATION_PATH)
assert _spec is not None and _spec.loader is not None
migration = importlib.util.module_from_spec(_spec)
_spec.loader.exec_module(migration)

STATEMENTS: tuple[str, ...] = migration.STATEMENTS

# The one statement that is idempotent WITHOUT `IF NOT EXISTS`: re-applying a
# column's existing type is a no-op, and Postgres applies the widening as a
# catalogue-only change (numeric's typmod transform — no rewrite, no scan).
WIDENING_STATEMENT = "ALTER TABLE billing_cost_daily ALTER COLUMN total_cost_usd TYPE NUMERIC(20,6)"


def _statement_starting(prefix: str) -> str:
    """The single migration statement starting with ``prefix`` (read, not retyped)."""
    matches = [s for s in STATEMENTS if s.startswith(prefix)]
    assert len(matches) == 1, f"expected exactly one statement starting {prefix!r}, got {matches}"
    return matches[0]


# ---------------------------------------------------------------------------
# alembic plumbing. Identical rationale to
# ``test_migration_0011_tenant_id_is_a_uuid._alembic_config``.
# ---------------------------------------------------------------------------


def _alembic_config() -> Config:
    """Alembic config WITHOUT an ini file.

    ``alembic.ini`` carries a ``[loggers]`` section which ``env.py`` feeds to
    ``logging.config.fileConfig``, whose ``disable_existing_loggers`` default
    would silence every logger already configured in this pytest session.
    Building the Config programmatically leaves ``config_file_name`` None so
    ``env.py`` skips that call. The URL is NOT overridden — ``env.py`` reads
    ``DATABASE_URL``, which conftest sets to ``TEST_DATABASE_URL``.
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
    """Alembic is synchronous and opens its own connection — run it off-loop."""
    await asyncio.to_thread(_upgrade)


async def _run_downgrade() -> None:
    await asyncio.to_thread(_downgrade)


# ---------------------------------------------------------------------------
# Live-schema helpers. Everything is read from the catalogue, never from
# ``Base.metadata`` — the models describe what the ORM wants, and the whole
# point of this migration is that the database did not have it.
# ---------------------------------------------------------------------------


async def _column(conn, table: str, column: str) -> dict[str, object] | None:
    res = await conn.execute(
        text(
            "SELECT data_type, character_maximum_length, numeric_precision, numeric_scale, "
            "       is_nullable, column_default "
            "FROM information_schema.columns "
            "WHERE table_schema = current_schema() AND table_name = :t AND column_name = :c"
        ),
        {"t": table, "c": column},
    )
    row = res.first()
    return None if row is None else dict(row._mapping)


async def _precision(conn, table: str, column: str) -> tuple[object, object]:
    """``(numeric_precision, numeric_scale)`` as the catalogue reports them.

    Left as ``object`` rather than narrowed: the values come straight out of
    ``information_schema`` and are compared against literal tuples, so a cast
    here would only be a claim the catalogue has not made.
    """
    col = await _column(conn, table, column)
    assert col is not None, f"{table}.{column} is missing"
    return (col["numeric_precision"], col["numeric_scale"])


async def _index_def(conn, name: str) -> str | None:
    res = await conn.execute(
        text(
            "SELECT indexdef FROM pg_indexes WHERE schemaname = current_schema() AND indexname = :n"
        ),
        {"n": name},
    )
    return res.scalar_one_or_none()


async def _constraint_def(conn, table: str, name: str) -> str | None:
    res = await conn.execute(
        text(
            "SELECT pg_get_constraintdef(c.oid) FROM pg_constraint c "
            "WHERE c.conname = :n AND c.conrelid = to_regclass(:t)"
        ),
        {"n": name, "t": table},
    )
    return res.scalar_one_or_none()


async def _table_exists(conn, table: str) -> bool:
    res = await conn.execute(text("SELECT to_regclass(:t)"), {"t": table})
    return res.scalar() is not None


async def _stamped_revision(conn) -> str | None:
    if not await _table_exists(conn, "alembic_version"):
        return None
    res = await conn.execute(text("SELECT version_num FROM alembic_version"))
    return res.scalar_one_or_none()


async def _present(conn) -> dict[str, object]:
    """The drift objects as the catalogue currently reports them."""
    state: dict[str, object] = {}
    for table, column in DRIFT_COLUMNS:
        state[f"{table}.{column}"] = await _column(conn, table, column)
    state["{}.{}".format(*NARROW_COLUMN)] = await _column(conn, *NARROW_COLUMN)
    state[DRIFT_TABLE] = await _table_exists(conn, DRIFT_TABLE)
    for index in DRIFT_INDEXES:
        state[index] = await _index_def(conn, index)
    return state


async def _insert_daily_total(conn, total: str) -> None:
    """Write one ``billing_cost_daily`` row with ``total_cost_usd = total``.

    Deliberately NOT committed by any caller — the surrounding connection is
    closed without commit, so nothing survives into the shared schema.
    """
    await conn.execute(
        text(
            "INSERT INTO billing_cost_daily (id, tenant_id, license_id, service_key, provider, "
            "bucket_date, total_input_tokens, total_output_tokens, total_cost_usd, request_count, "
            "tenant_name) VALUES (:id, :tenant, :lic, 'svc', 'anthropic', DATE '2026-01-01', "
            "0, 0, :total, 1, 'probe')"
        ),
        {
            "id": str(uuid.uuid4()),
            "tenant": str(uuid.uuid4()),
            "lic": str(uuid.uuid4()),
            "total": total,
        },
    )


# ---------------------------------------------------------------------------
# Pre-state: the alembic-only schema a fresh database really has.
# ---------------------------------------------------------------------------


async def _drop_drift_objects(conn) -> dict[str, str | None]:
    """Remove the objects ``0012`` is responsible for; return what fell with them.

    ``DROP COLUMN`` also drops everything built on the column, so
    ``uq_cost_daily`` and both ``provider_pricing`` indexes disappear here. Their
    definitions are captured FIRST and returned so teardown can replay them
    verbatim — the session-scoped schema is shared with the rest of the suite and
    must come back exactly as ``create_all`` left it.

    ``total_cost_usd`` is narrowed rather than dropped: on a fresh alembic
    database it EXISTS, at the baseline's ``numeric(12,6)``.
    """
    captured: dict[str, str | None] = {
        COLLATERAL_CONSTRAINT[1]: await _constraint_def(conn, *COLLATERAL_CONSTRAINT),
    }
    for index in COLLATERAL_INDEXES:
        captured[index] = await _index_def(conn, index)

    await conn.execute(text("DROP TABLE IF EXISTS alembic_version"))
    await conn.execute(
        text("ALTER TABLE billing_cost_daily DROP COLUMN IF EXISTS owner_provided CASCADE")
    )
    await conn.execute(
        text("ALTER TABLE provider_pricing DROP COLUMN IF EXISTS rate_class CASCADE")
    )
    await conn.execute(text(f"DROP TABLE IF EXISTS {DRIFT_TABLE} CASCADE"))
    await conn.execute(
        text(
            "ALTER TABLE billing_cost_daily ALTER COLUMN total_cost_usd "
            f"TYPE NUMERIC({BASELINE_PRECISION[0]},{BASELINE_PRECISION[1]})"
        )
    )
    return captured


async def _restore(conn, captured: dict[str, str | None]) -> None:
    """Put the schema back: the migration's own DDL, then the collateral."""
    await conn.execute(text("DROP TABLE IF EXISTS alembic_version"))
    for statement in STATEMENTS:
        await conn.execute(text(statement))

    table, name = COLLATERAL_CONSTRAINT
    constraint_def = captured.get(name)
    if constraint_def:
        await conn.execute(text(f"ALTER TABLE {table} DROP CONSTRAINT IF EXISTS {name}"))
        await conn.execute(text(f"ALTER TABLE {table} ADD CONSTRAINT {name} {constraint_def}"))

    for index in COLLATERAL_INDEXES:
        index_def = captured.get(index)
        if index_def:
            await conn.execute(text(f"DROP INDEX IF EXISTS {index}"))
            await conn.execute(text(index_def))


@pytest.fixture
async def clean_db(_db_engine_session):
    """The untouched ``create_all`` schema, with ``alembic_version`` removed.

    This is PRODUCTION's shape for the purposes of ``0012``: all six objects
    already exist. Used by the idempotence tests.
    """
    engine = _db_engine_session
    async with engine.begin() as conn:
        await conn.execute(text("DROP TABLE IF EXISTS alembic_version"))
    yield engine
    async with engine.begin() as conn:
        await conn.execute(text("DROP TABLE IF EXISTS alembic_version"))


@pytest.fixture
async def drifted_db(_db_engine_session):
    """``clean_db`` with the alembic-only (pre-``0012``) pre-state forced."""
    engine = _db_engine_session
    async with engine.begin() as conn:
        captured = await _drop_drift_objects(conn)
    try:
        yield engine
    finally:
        async with engine.begin() as conn:
            await _restore(conn, captured)


@pytest.fixture
async def type_drifted_db(drifted_db):
    """The pre-state, then re-seeded with objects of the RIGHT NAME and WRONG SHAPE.

    This is the database an operator actually has when an earlier hand-fix, an
    older ``create_all``, or a hand-written hotfix put an object there under the
    name the migration checks for. ``ADD COLUMN IF NOT EXISTS`` and
    ``CREATE INDEX IF NOT EXISTS`` are then no-ops, so nothing in the migration's
    DDL corrects any of it.

    Teardown returns the database to the plain drifted pre-state so
    ``drifted_db``'s own teardown can restore it exactly.
    """
    engine = drifted_db
    async with engine.begin() as conn:
        await conn.execute(
            text("ALTER TABLE billing_cost_daily ADD COLUMN owner_provided varchar(8)")
        )
        await conn.execute(text("ALTER TABLE provider_pricing ADD COLUMN rate_class text"))
        await conn.execute(text(_statement_starting("CREATE TABLE IF NOT EXISTS")))
        await conn.execute(text(_statement_starting("CREATE INDEX IF NOT EXISTS")))
        # THE DEFECT: right name, non-unique, and no `WHERE effective_until IS NULL`.
        await conn.execute(
            text(
                f"CREATE INDEX {DRIFT_INDEXES[1]} ON {DRIFT_TABLE} "
                "(tenant_id, coalesce(service_key, ''))"
            )
        )
    try:
        yield engine
    finally:
        async with engine.begin() as conn:
            await _drop_drift_objects(conn)


# ---------------------------------------------------------------------------
# Chain wiring
# ---------------------------------------------------------------------------


class TestRevisionChain:
    def test_0012_sits_on_the_single_linear_chain_after_0011(self) -> None:
        """0012 follows 0011, and the tree is a single unbranched chain.

        This used to assert `get_heads() == [REVISION]`, which made 0012 "the
        head" part of its own contract — so the NEXT migration (0013) broke a
        test about 0012 that had nothing to do with it. What actually matters is
        that 0012 is on the one chain and still follows 0011; the head moving
        forward is the normal case, a FORK is the defect.
        """
        script = ScriptDirectory.from_config(_alembic_config())
        heads = list(script.get_heads())
        assert len(heads) == 1, f"alembic has forked: {heads}"
        assert script.get_revision(REVISION).down_revision == PREVIOUS_REVISION
        chain = {r.revision for r in script.walk_revisions("base", heads[0])}
        assert REVISION in chain

    def test_revision_id_fits_alembic_version_column(self) -> None:
        """``alembic_version.version_num`` is varchar(32)."""
        assert len(REVISION) <= 32


# ---------------------------------------------------------------------------
# The anti-drift ratchet. This is the test that keeps the defect closed.
# ---------------------------------------------------------------------------


class TestStatementsTrackSrcDbPy:
    def test_every_statement_appears_verbatim_in_src_db_py(self) -> None:
        """The migration and the runtime DDL must stay byte-identical.

        If someone edits ``ADDITIVE_MIGRATIONS`` (a wider VARCHAR, a different
        default, another index column) and not ``0012``, a fresh database and
        production diverge again — silently, exactly as before. Substring
        matching would not do: a differing DEFAULT is still a superstring of the
        column name, and that difference is the whole bug.
        """
        runtime = set(ADDITIVE_MIGRATIONS)
        missing = [s for s in STATEMENTS if s not in runtime]
        assert not missing, (
            "0012 statements that no longer appear verbatim in src/db.py's "
            f"ADDITIVE_MIGRATIONS: {missing}"
        )

    def test_the_six_objects_are_all_covered(self) -> None:
        """A statement list that silently lost an object would still be verbatim."""
        blob = "\n".join(STATEMENTS)
        for _table, column in DRIFT_COLUMNS:
            assert f"ADD COLUMN IF NOT EXISTS {column} " in blob
        assert f"CREATE TABLE IF NOT EXISTS {DRIFT_TABLE} (" in blob
        for index in DRIFT_INDEXES:
            assert f"INDEX IF NOT EXISTS {index} " in blob
        assert WIDENING_STATEMENT in STATEMENTS

    def test_every_statement_is_repeatable_and_none_drops(self) -> None:
        """The no-op-against-production contract, asserted on the DDL itself.

        Every statement is either ``IF NOT EXISTS``-guarded or — for the one
        ``ALTER COLUMN … TYPE`` — idempotent because re-applying a column's
        existing type is a no-op. Nothing DROPs: a ``DROP`` against a live
        database whose pods are mid-roll is a different risk class, and the two
        remaining divergences that need one are named in the module docstring of
        the migration and deliberately left for their own reviewed migration.
        """
        for statement in STATEMENTS:
            assert "DROP" not in statement.upper(), statement
            if statement == WIDENING_STATEMENT:
                assert " ALTER COLUMN " in statement and " TYPE " in statement
                continue
            assert "IF NOT EXISTS" in statement, statement

    def test_the_race_class_names_track_src_db_py(self) -> None:
        """The migration restates ``_RACE_CLASS_ORIG_NAMES`` instead of importing it.

        Deliberate: a historical migration must not break because application
        code renamed a private symbol. The cost of restating is that the two can
        drift, so the drift is asserted here rather than left to chance. If
        ``src/db.py`` learns about a new benign race SQLSTATE, the migration must
        learn about it too — otherwise the ``migrate`` init container starts
        aborting on a race the pods themselves shrug off.
        """
        assert migration.RACE_CLASS_ORIG_NAMES == _RACE_CLASS_ORIG_NAMES


# ---------------------------------------------------------------------------
# The vacuity control. If this class fails, nothing else below means anything —
# read the module docstring.
# ---------------------------------------------------------------------------


class TestPreStateIsDrifted:
    async def test_conftest_hands_us_every_object_before_we_touch_anything(self, clean_db) -> None:
        """Half of the trap: the suite's DEFAULT state is already migrated.

        ``Base.metadata.create_all`` builds all of them, so a test that just
        calls ``command.upgrade`` and asserts they exist proves nothing about
        ``0012``.
        """
        async with clean_db.connect() as conn:
            state = await _present(conn)
            assert await _precision(conn, *NARROW_COLUMN) == WIDENED_PRECISION
        assert all(value for value in state.values()), state

    async def test_forced_pre_state_is_missing_everything(self, drifted_db) -> None:
        """The other half: the pre-state really is a fresh alembic-only schema."""
        async with drifted_db.connect() as conn:
            for table, column in DRIFT_COLUMNS:
                assert await _column(conn, table, column) is None, f"{table}.{column} still here"
            assert not await _table_exists(conn, DRIFT_TABLE)
            for index in DRIFT_INDEXES:
                assert await _index_def(conn, index) is None, f"{index} still here"
            assert await _precision(conn, *NARROW_COLUMN) == BASELINE_PRECISION

    async def test_the_table_cannot_even_be_queried_in_the_pre_state(self, drifted_db) -> None:
        """Self-enforcing control: a fixture that stopped dropping would fail here.

        ``.orig`` is the psycopg diagnostic alone; ``str(excinfo.value)`` would
        also echo the statement, which names the table anyway and would make the
        assertion pass for the wrong reason.
        """
        with pytest.raises(ProgrammingError) as excinfo:
            async with drifted_db.connect() as conn:
                await conn.execute(text(f"SELECT 1 FROM {DRIFT_TABLE}"))
        assert f'relation "{DRIFT_TABLE}" does not exist' in str(excinfo.value.orig)


# ---------------------------------------------------------------------------
# What 0012 actually does on a fresh (alembic-only) database.
# ---------------------------------------------------------------------------


class TestUpgradeOnTheAlembicOnlySchema:
    async def test_upgrade_creates_every_object(self, drifted_db) -> None:
        await _run_upgrade()
        async with drifted_db.connect() as conn:
            for table, column in DRIFT_COLUMNS:
                assert await _column(conn, table, column) is not None, f"{table}.{column} missing"
            assert await _table_exists(conn, DRIFT_TABLE)
            for index in DRIFT_INDEXES:
                assert await _index_def(conn, index) is not None, f"{index} missing"

    async def test_owner_provided_matches_src_db_py_exactly(self, drifted_db) -> None:
        """BOOLEAN NOT NULL DEFAULT false — ``src/db.py:179``.

        A column created with a DIFFERENT type than the runtime DDL makes the
        drift worse, not better: two environments would then disagree about the
        column instead of about its existence.
        """
        await _run_upgrade()
        async with drifted_db.connect() as conn:
            col = await _column(conn, "billing_cost_daily", "owner_provided")
        assert col == {
            "data_type": "boolean",
            "character_maximum_length": None,
            "numeric_precision": None,
            "numeric_scale": None,
            "is_nullable": "NO",
            "column_default": "false",
        }

    async def test_rate_class_matches_src_db_py_exactly(self, drifted_db) -> None:
        """VARCHAR(16) NOT NULL DEFAULT 'cost' — ``src/db.py:207``."""
        await _run_upgrade()
        async with drifted_db.connect() as conn:
            col = await _column(conn, "provider_pricing", "rate_class")
        assert col == {
            "data_type": "character varying",
            "character_maximum_length": 16,
            "numeric_precision": None,
            "numeric_scale": None,
            "is_nullable": "NO",
            "column_default": "'cost'::character varying",
        }

    async def test_tenant_access_charge_tenant_id_is_uuid_not_varchar(self, drifted_db) -> None:
        """``0011`` already converted this column in production.

        Recreating the table with ``varchar`` would silently undo the fleet-wide
        "tenant_id is ALWAYS a uuid" invariant on every fresh environment, and
        ``0011``'s own ``NO_MERGE_KEYS`` guard for this table depends on it.
        """
        await _run_upgrade()
        async with drifted_db.connect() as conn:
            col = await _column(conn, DRIFT_TABLE, "tenant_id")
        assert col is not None
        assert col["data_type"] == "uuid"
        assert col["is_nullable"] == "NO"

    async def test_the_open_index_really_enforces_one_open_charge_per_tenant(
        self, drifted_db
    ) -> None:
        """A behavioural check, not a catalogue read.

        ``ix_accesscharge_open`` is the UNIQUE index whose exact key ``0011``
        encodes in ``NO_MERGE_KEYS['tenant_access_charge']``. Creating the table
        without it would let a fresh database hold two open access charges for
        one (tenant, service_key) — two rows of record for one bill. The
        ``service_key`` here is NULL on purpose: that is the coalesce()d arm of
        the index and the one a plain UNIQUE would not catch.
        """
        await _run_upgrade()
        tenant = str(uuid.uuid4())
        insert = text(
            f"INSERT INTO {DRIFT_TABLE} (tenant_id, service_key, amount_usd, effective_from) "
            "VALUES (:t, NULL, 10.0, :ts)"
        )
        params = {"t": tenant, "ts": datetime(2026, 1, 1, tzinfo=UTC)}
        async with drifted_db.begin() as conn:
            await conn.execute(insert, params)
        with pytest.raises(DBAPIError) as excinfo:
            async with drifted_db.begin() as conn:
                await conn.execute(insert, params)
        assert "ix_accesscharge_open" in str(excinfo.value.orig)

    async def test_the_lookup_index_keeps_the_coalesce_and_the_desc(self, drifted_db) -> None:
        """``src/db.py:230`` — ``(tenant_id, coalesce(service_key, ''), effective_from DESC)``."""
        await _run_upgrade()
        async with drifted_db.connect() as conn:
            indexdef = await _index_def(conn, "ix_accesscharge_lookup")
        assert indexdef is not None
        assert "COALESCE(service_key" in indexdef
        assert "effective_from DESC" in indexdef


# ---------------------------------------------------------------------------
# The third divergence: a TYPE, not a missing object.
# ---------------------------------------------------------------------------


class TestTheDailyCostWidening:
    """``billing_cost_daily.total_cost_usd``: numeric(12,6) baseline vs (20,6) live.

    Brought INTO ``0012`` rather than documented as out-of-scope, because unlike
    ``uq_cost_daily`` and the two ``provider_pricing`` indexes it needs no
    ``DROP``: the target domain is a strict superset of the current one, so no
    stored value can fail to fit and no rounding changes.
    """

    async def test_the_pre_state_cannot_store_a_million_dollar_day(self, drifted_db) -> None:
        """Why this belongs in the migration at all — the behavioural cost.

        ``billing_cost_daily`` is the invoice source. At ``numeric(12,6)`` a
        single (tenant, licence, service, provider, model, day) total is capped
        at $999,999.999999, and the day's rollup dies on the INSERT rather than
        rounding.
        """
        with pytest.raises(DBAPIError) as excinfo:
            async with drifted_db.connect() as conn:
                await _insert_daily_total(conn, OVERFLOWING_DAILY_TOTAL)
        assert "numeric field overflow" in str(excinfo.value.orig)

    async def test_upgrade_widens_it_and_the_same_row_then_stores(self, drifted_db) -> None:
        await _run_upgrade()
        async with drifted_db.connect() as conn:
            assert await _precision(conn, *NARROW_COLUMN) == WIDENED_PRECISION
            # Never committed — the connection closes without commit.
            await _insert_daily_total(conn, OVERFLOWING_DAILY_TOTAL)
            stored = await conn.execute(
                text("SELECT total_cost_usd FROM billing_cost_daily WHERE tenant_name = 'probe'")
            )
            assert str(stored.scalar_one()) == OVERFLOWING_DAILY_TOTAL


# ---------------------------------------------------------------------------
# D-1: `IF NOT EXISTS` is not race-safe, and env.py runs ONE transaction.
# ---------------------------------------------------------------------------


class TestConcurrentCreatorRace:
    """A pod replaying ``ADDITIVE_MIGRATIONS`` while the migrate job runs.

    ``IF NOT EXISTS`` only consults COMMITTED catalogue rows, so an uncommitted
    concurrent ``CREATE TABLE`` is invisible; both sessions then insert the same
    ``pg_type`` row and the loser gets
    ``duplicate key value violates unique constraint "pg_type_typname_nsp_index"``.
    """

    @staticmethod
    async def _hold_uncommitted_create(engine):
        """Open a connection that has created the table but NOT committed."""
        holder = await engine.connect()
        await holder.execute(text(_statement_starting("CREATE TABLE IF NOT EXISTS")))
        return holder

    async def test_upgrade_survives_a_concurrent_uncommitted_create_table(self, drifted_db) -> None:
        holder = await self._hold_uncommitted_create(drifted_db)
        try:
            upgrade_task = asyncio.create_task(_run_upgrade())
            # The migration must be BLOCKED on the holder's uncommitted catalogue
            # row. If it already finished, no race was joined and everything
            # below would be vacuous.
            await asyncio.sleep(1.5)
            if upgrade_task.done():
                upgrade_task.result()  # re-raise the real failure, if any
                pytest.fail("the migration never contended with the concurrent creator")
            await holder.commit()
        finally:
            await holder.close()

        await asyncio.wait_for(upgrade_task, timeout=60)

        async with drifted_db.connect() as conn:
            assert await _stamped_revision(conn) == REVISION
            state = await _present(conn)
            assert all(value for value in state.values()), state
            # The whole postcondition, not just the raced object: the earlier
            # statements must have survived the loser's rollback too.
            await conn.run_sync(migration._verify)

    async def test_the_same_race_aborts_a_transaction_without_savepoints(self, drifted_db) -> None:
        """THE CONTROL. Remove the SAVEPOINT and the reproduced defect returns.

        This replays the migration's statements the way ``upgrade()`` used to —
        straight onto the single transaction ``alembic/env.py`` opens. The race
        does not merely skip a statement: the driver error poisons the whole
        transaction, so every statement before it is rolled back too. That is
        why catching the exception in Python is not sufficient on its own, and
        why the passing test above is not a tautology.
        """
        holder = await self._hold_uncommitted_create(drifted_db)
        victim = await drifted_db.connect()

        async def _replay_without_savepoints() -> None:
            for statement in STATEMENTS:
                await victim.execute(text(statement))

        try:
            victim_task = asyncio.create_task(_replay_without_savepoints())
            await asyncio.sleep(1.5)
            assert not victim_task.done(), "no race was joined"
            await holder.commit()

            with pytest.raises(DBAPIError) as excinfo:
                await asyncio.wait_for(victim_task, timeout=60)
            assert "pg_type_typname_nsp_index" in str(excinfo.value.orig)

            # …and the transaction is now unusable, which is the half that the
            # `try/except` alone cannot fix.
            with pytest.raises(SQLAlchemyError) as poisoned:
                await victim.execute(text("SELECT 1"))
            message = str(poisoned.value).lower()
            assert "rolled back" in message or "aborted" in message, poisoned.value
        finally:
            await victim.rollback()
            await victim.close()
            await holder.close()


# ---------------------------------------------------------------------------
# D-2: a name-only postcondition blesses the wrong object.
# ---------------------------------------------------------------------------


class TestVerifyChecksDefinitionsNotNames:
    async def test_wrong_shaped_objects_of_the_right_name_abort_the_upgrade(
        self, type_drifted_db
    ) -> None:
        """The reproduced defect: ``0012`` used to print "verified" over this schema.

        Every one of the seeded objects has the name the migration looks for and
        a definition ``src/db.py`` would never produce, so every ``IF NOT
        EXISTS`` statement is a no-op and nothing is corrected. The postcondition
        must therefore RAISE, naming each mismatch.
        """
        with pytest.raises(RuntimeError) as excinfo:
            await _run_upgrade()
        message = str(excinfo.value)

        assert "WRONG DEFINITION" in message
        assert "owner_provided: data_type is 'character varying', expected 'boolean'" in message
        assert "rate_class: data_type is 'text', expected 'character varying'" in message
        assert f"index {DRIFT_INDEXES[1]}: NON-UNIQUE, expected UNIQUE" in message
        assert "effective_until IS NULL" in message

        async with type_drifted_db.connect() as conn:
            # The migration did not stamp itself over a schema it could not vouch
            # for: the transaction rolled back with the revision still at 0011.
            assert await _stamped_revision(conn) == PREVIOUS_REVISION

    async def test_that_wrong_shape_really_is_the_two_bills_defect(self, type_drifted_db) -> None:
        """Why the RAISE matters: this schema accepts two OPEN charges for one key.

        Without this, the test above would only prove the postcondition is
        fussy. ``ix_accesscharge_open`` as seeded is non-unique and unfiltered,
        so the "one open access charge per (tenant, service_key)" invariant that
        ``0011``'s ``NO_MERGE_KEYS`` relies on is simply absent.
        """
        tenant = str(uuid.uuid4())
        insert = text(
            f"INSERT INTO {DRIFT_TABLE} (tenant_id, service_key, amount_usd, effective_from) "
            "VALUES (:t, 'svc', 10.0, :ts)"
        )
        params = {"t": tenant, "ts": datetime(2026, 1, 1, tzinfo=UTC)}
        async with type_drifted_db.connect() as conn:
            await conn.execute(insert, params)
            await conn.execute(insert, params)
            open_rows = await conn.execute(
                text(
                    f"SELECT count(*) FROM {DRIFT_TABLE} "
                    "WHERE tenant_id = :t AND effective_until IS NULL"
                ),
                {"t": tenant},
            )
            assert open_rows.scalar_one() == 2  # noqa: PLR2004 — two rows of record for one bill
            # Not committed: the connection closes without commit.

    async def test_verify_raises_for_a_genuinely_absent_object(self, drifted_db) -> None:
        """The postcondition must still catch plain ABSENCE, not only wrong shapes."""
        async with drifted_db.connect() as conn:
            with pytest.raises(RuntimeError) as excinfo:
                await conn.run_sync(migration._verify)
        message = str(excinfo.value)
        assert "ABSENT" in message
        assert f"table {DRIFT_TABLE}" in message
        for index in DRIFT_INDEXES:
            assert f"index {index}" in message

    async def test_verify_names_one_dropped_index_after_an_otherwise_clean_upgrade(
        self, drifted_db
    ) -> None:
        """Per-object sensitivity: drop exactly one thing and it is the thing named."""
        await _run_upgrade()
        async with drifted_db.begin() as conn:
            await conn.execute(text(f"DROP INDEX {DRIFT_INDEXES[0]}"))
        try:
            async with drifted_db.connect() as conn:
                with pytest.raises(RuntimeError) as excinfo:
                    await conn.run_sync(migration._verify)
        finally:
            async with drifted_db.begin() as conn:
                await conn.execute(text(_statement_starting("CREATE INDEX IF NOT EXISTS")))
        message = str(excinfo.value)
        assert f"index {DRIFT_INDEXES[0]}" in message
        assert DRIFT_INDEXES[1] not in message

    async def test_verify_passes_on_the_real_schema(self, clean_db) -> None:
        """The other side of the raise-tests: it is not raising unconditionally."""
        async with clean_db.connect() as conn:
            await conn.run_sync(migration._verify)


# ---------------------------------------------------------------------------
# The production contract: every object already exists there, so 0012 must
# change NOTHING and must not raise.
# ---------------------------------------------------------------------------


class TestIdempotence:
    async def test_upgrade_against_a_database_that_already_has_everything(self, clean_db) -> None:
        """PRODUCTION's case: ``src/db.py`` has been creating these for months."""
        async with clean_db.connect() as conn:
            before = await _present(conn)
        await _run_upgrade()
        async with clean_db.connect() as conn:
            after = await _present(conn)
        assert after == before

    async def test_upgrade_twice_in_a_row(self, drifted_db) -> None:
        """Second run has nothing left to create and must still exit cleanly.

        This is the init-container contract: the ``migrate`` container runs on
        every roll, and a re-run after a rollback must not fail.
        """
        await _run_upgrade()
        async with drifted_db.connect() as conn:
            first = await _present(conn)
        await _run_upgrade()
        async with drifted_db.connect() as conn:
            second = await _present(conn)
        assert second == first
        assert all(value for value in second.values()), second

    async def test_upgrade_survives_the_runtime_ddl_having_run_first(self, drifted_db) -> None:
        """A pod booted and replayed ``ADDITIVE_MIGRATIONS`` before the migrate job.

        That ordering is not hypothetical — a surviving replica replays the
        runtime DDL while the new revision's ``migrate`` container is still
        starting. ``0012`` must find the objects already there and do nothing.
        """
        async with drifted_db.begin() as conn:
            for statement in STATEMENTS:
                await conn.execute(text(statement))
            before = await _present(conn)
        await _run_upgrade()
        async with drifted_db.connect() as conn:
            after = await _present(conn)
        assert after == before


# ---------------------------------------------------------------------------
# The documented no-op downgrade.
# ---------------------------------------------------------------------------


class TestDowngradeIsADocumentedNoOp:
    async def test_downgrade_leaves_every_object_in_place(self, drifted_db) -> None:
        """Dropping these would break a RUNNING pod — see ``downgrade()``'s docstring.

        ``src/db.py`` recreates them all on every pod start, ``owner_provided``
        and ``rate_class`` carry live constraints/indexes,
        ``tenant_access_charge`` holds revenue CONFIGURATION that is not
        derivable, and re-narrowing ``total_cost_usd`` would either fail on a
        stored value or silently re-impose the overflow. Going back to ``0011``
        means "stop asserting these", never "remove them".
        """
        await _run_upgrade()
        async with drifted_db.connect() as conn:
            before = await _present(conn)
        await _run_downgrade()
        async with drifted_db.connect() as conn:
            after = await _present(conn)
        assert after == before
        assert all(value for value in after.values()), after

    async def test_upgrade_after_downgrade_is_safe(self, drifted_db) -> None:
        """Re-applying over objects the downgrade deliberately left behind."""
        await _run_upgrade()
        await _run_downgrade()
        await _run_upgrade()
        async with drifted_db.connect() as conn:
            state = await _present(conn)
        assert all(value for value in state.values()), state
