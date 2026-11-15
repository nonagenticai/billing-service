"""Behavioural tests for alembic ``0011_tenant_id_is_a_uuid``.

``0011`` rewrites ``tenant_id`` from ``varchar(255)`` to ``uuid`` on 14 tables of
a live 850 MB billing database — one of them a 13-partition RANGE-partitioned
parent — and on the way it MERGES colliding rows *irreversibly*: the losing rows
are ``DELETE``d and their additive counters summed into a survivor. It runs
inside a Kubernetes ``migrate`` init container, in a single transaction. Until
this file existed nothing exercised any of it.

⛔ THE VACUITY TRAP THIS FILE IS BUILT AROUND
---------------------------------------------
``tests/conftest.py`` bootstraps the schema with ``Base.metadata.create_all``,
and ``src/models/db/*.py`` already declare ``tenant_id`` as ``Uuid``. Under
pytest all 14 columns are therefore ALREADY ``uuid`` before the migration runs.
A test that merely calls ``command.upgrade`` computes an EMPTY ``text_tables``,
so the credential scrub, the coverage guard, every merge, the remap, the index
rebuild and all 14 ALTERs are no-ops — and every assertion written against that
run passes against a migration that did nothing at all.

Every test here therefore drives :func:`_force_varchar_pre_state` FIRST, which
puts the database back into the shape production is actually in: 14
``varchar(255)`` columns and the two ``markup_rules`` functional indexes in
their legacy ``COALESCE(tenant_id, ''::character varying)`` form (the exact
state ``0011``'s own ``downgrade()`` produces). :class:`TestPreStateIsVarchar`
asserts that pre-state directly, so if the trap ever reopens THAT test fails and
names the reason rather than the rest of the file going quietly green.

There is a second, self-enforcing non-vacuity control: every seed below writes a
legacy string such as ``'n8n-dev'`` into ``tenant_id``. Against the pytest
default schema that INSERT fails outright with ``invalid input syntax for type
uuid``. The corpus cannot be seeded unless the pre-state really was forced.

Isolation
---------
* alembic opens its OWN connection, so every seed is COMMITTED first.
* ``alembic_version`` and ``tenant_id_crosswalk`` are not in ``Base.metadata``,
  so conftest's TRUNCATE never touches them — :func:`_restore_uuid_post_state`
  drops both, and it runs on BOTH sides of the fixture so a crashed test cannot
  hand the next one a half-migrated schema.
* The fixture depends on the session-scoped ``_db_engine_session`` rather than
  on ``db_engine``, and does its own TRUNCATE. That is deliberate and is NOT an
  end-run around ``conftest._nil_tenant_tripwire``: that tripwire exists to
  catch an app-path test whose readable tenant label silently sank to the nil
  tenant, and its opt-out marker is a closed set policed by
  ``tests/test_nil_tenant_marker_is_narrow.py``. Here the nil tenant is the
  *asserted* destination of the ``''`` / ``unknown`` / ``__unattributed__``
  sentinels — that IS the migration's contract — and the teardown truncates, so
  nothing leaks into any other test.
"""

from __future__ import annotations

import asyncio
import hashlib
import importlib.util
import uuid
from datetime import UTC, date, datetime
from decimal import Decimal
from pathlib import Path
from typing import cast

import pytest
from alembic import command
from alembic.config import Config
from alembic.script import ScriptDirectory
from sqlalchemy import Table, text
from sqlalchemy.exc import DBAPIError

from db import Base
from models.db import ALLOW_TRUNCATE_GUC, ALLOW_TRUNCATE_VALUE, CostDaily, CostEvent, MarkupRule
from tenancy import DEFAULT_CONFIG_TENANT_ID

pytestmark = pytest.mark.unit

REPO_ROOT = Path(__file__).resolve().parents[1]

REVISION = "0011_tenant_id_is_a_uuid"
PREVIOUS_REVISION = "0010_license_sync_tenant_repair"

CROSSWALK = "tenant_id_crosswalk"
MARKUP_LOOKUP = "ix_markuprules_lookup"
MARKUP_OPEN = "ix_markuprules_open"


# ---------------------------------------------------------------------------
# The migration module itself.
#
# Loaded by path because ``0011_tenant_id_is_a_uuid`` is not an importable
# module name. The point is MERGE_TABLES: the additive-column lists asserted in
# TestConservation are read from the migration rather than retyped here, so a
# column added to (or dropped from) the summed set fails a test instead of
# silently escaping conservation checking.
# ---------------------------------------------------------------------------

_MIGRATION_PATH = REPO_ROOT / "alembic" / "versions" / f"{REVISION}.py"
_spec = importlib.util.spec_from_file_location("_migration_0011", _MIGRATION_PATH)
assert _spec is not None and _spec.loader is not None
migration = importlib.util.module_from_spec(_spec)
_spec.loader.exec_module(migration)

TENANT_TABLES: tuple[str, ...] = migration.TENANT_TABLES
MERGE_TABLES: dict[str, dict[str, tuple[str, ...]]] = migration.MERGE_TABLES

# Byte-identical to src/tenancy.py and to the two predecessor fleet migrations.
TENANT_NAMESPACE = uuid.UUID("6ba7b811-9dad-11d1-80b4-00c04fd430c8")
NIL = "00000000-0000-0000-0000-000000000000"

# n8n-dev's REGISTRY uuid — the one licence-token traffic already sends, and the
# one billing must map the label onto (44,712 rows already carry it).
N8N_DEV_UUID = "019de842-c92a-7e37-8fdd-eb0d8c5396f9"


def _derived(label: str) -> str:
    return str(uuid.uuid5(TENANT_NAMESPACE, label))


# Not a real credential: a shape. It matches ``_looks_like_credential`` twice
# over (``bearer `` prefix AND embedded whitespace) without carrying anything
# recoverable. The migration must record it only as a sha256 prefix.
LEAKED_HEADER_SHAPE = "Bearer not-a-real-value-just-the-shape"
LEAKED_HEADER_DIGEST = "bearer:" + hashlib.sha256(LEAKED_HEADER_SHAPE.encode()).hexdigest()[:12]


# ---------------------------------------------------------------------------
# Corpus. Explicit ids so "which row survived" is an assertion, not a guess.
# Survivor is always ``(array_agg(id ORDER BY id))[1]`` — the LOWEST id — so
# every _KEEP constant below sorts before its _VICTIM.
# ---------------------------------------------------------------------------

TU_KEEP = uuid.UUID("00000000-0000-4000-8000-000000000001")
TU_VICTIM = uuid.UUID("00000000-0000-4000-8000-000000000002")
MD_KEEP = uuid.UUID("00000000-0000-4000-8000-000000000003")
MD_VICTIM = uuid.UUID("00000000-0000-4000-8000-000000000004")
MH_KEEP = uuid.UUID("00000000-0000-4000-8000-000000000005")
MH_VICTIM = uuid.UUID("00000000-0000-4000-8000-000000000006")
CD_KEEP = uuid.UUID("00000000-0000-4000-8000-000000000007")
CD_VICTIM = uuid.UUID("00000000-0000-4000-8000-000000000008")

TU_NEIGHBOUR = uuid.UUID("00000000-0000-4000-8000-0000000000aa")
MD_NEIGHBOUR = uuid.UUID("00000000-0000-4000-8000-0000000000bb")
MH_NEIGHBOUR = uuid.UUID("00000000-0000-4000-8000-0000000000cc")
CD_NEIGHBOUR = uuid.UUID("00000000-0000-4000-8000-0000000000dd")

CRED_ROW = uuid.UUID("00000000-0000-4000-8000-0000000000ee")

LBL_A_KEEP = uuid.UUID("00000000-0000-4000-8000-000000000011")
LBL_A_OTHER = uuid.UUID("00000000-0000-4000-8000-000000000012")
LBL_B_KEEP = uuid.UUID("00000000-0000-4000-8000-000000000013")
LBL_B_OTHER = uuid.UUID("00000000-0000-4000-8000-000000000014")

CFG_EMPTY = uuid.UUID("00000000-0000-4000-8000-000000000021")
CFG_UNKNOWN = uuid.UUID("00000000-0000-4000-8000-000000000022")

# One uuid, two legal spellings. Postgres accepts BOTH into a varchar column and
# keeps them verbatim, so they are two distinct text values and one uuid — see
# TestNoMergeGuard.test_two_spellings_of_one_uuid_...
CFG_UPPER = uuid.UUID("00000000-0000-4000-8000-000000000023")
CFG_LOWER = uuid.UUID("00000000-0000-4000-8000-000000000024")

CE_ROW = uuid.UUID("00000000-0000-4000-8000-000000000031")

LIC_TU = uuid.UUID("019dcaf3-0000-7000-8000-0000000000a1")
LIC_MD = uuid.UUID("019dcaf3-0000-7000-8000-0000000000a2")
LIC_MH = uuid.UUID("019dcaf3-0000-7000-8000-0000000000a3")
LIC_CD = uuid.UUID("019dcaf3-0000-7000-8000-0000000000a4")
LIC_CRED = uuid.UUID("019dcaf3-0000-7000-8000-0000000000a5")
LIC_LBL_A = uuid.UUID("019dcaf3-0000-7000-8000-0000000000a6")
LIC_LBL_B = uuid.UUID("019dcaf3-0000-7000-8000-0000000000a7")

NULL_SUBJECT_ID = uuid.UUID("00000000-0000-0000-0000-000000000003")
NULL_APP_ID = uuid.UUID("00000000-0000-0000-0000-000000000001")
NULL_PLAN_ID = uuid.UUID("00000000-0000-0000-0000-000000000002")

COLLIDING_DATE = date(2026, 8, 1)
NEIGHBOUR_DATE = date(2026, 7, 1)
COLLIDING_HOUR = datetime(2026, 8, 1, 5, 0, 0, tzinfo=UTC)
NEIGHBOUR_HOUR = datetime(2026, 7, 1, 5, 0, 0, tzinfo=UTC)

# Distinct primes per side and per column: no "kept one side" bug can produce a
# value that happens to equal the sum, and no two columns share a value, so a
# merge that sums the WRONG column is visible too.
LOW = (7, 11, 13, 29)
HIGH = (17, 19, 23, 31)
SUM = (24, 30, 36, 60)
NEIGHBOUR_COUNTS = (101, 103, 107, 109)

# The money column carries the same primes at Numeric(20,6) scale so a Decimal
# rounding or truncation bug is visible in the last digit.
CD_COST_LOW = Decimal("13.000013")
CD_COST_HIGH = Decimal("23.000023")
CD_COST_SUM = Decimal("36.000036")
CD_COST_NEIGHBOUR = Decimal("107.000107")

#: What each merge survivor's additive columns must hold afterwards. Keyed by
#: the column names the MIGRATION declares — asserted against
#: ``MERGE_TABLES[table]["additive"]`` in TestConservation so this map cannot
#: silently fall behind the migration's own list.
EXPECTED_SUMS: dict[str, dict[str, int | Decimal]] = {
    "token_usage": {
        "input_tokens": SUM[0],
        "output_tokens": SUM[1],
        "total_tokens": SUM[2],
    },
    "billing_metering_daily": {
        "request_count": SUM[0],
        "denied_count": SUM[1],
        "budget_denied_count": SUM[2],
    },
    "billing_metering_hourly": {
        "request_count": SUM[0],
        "denied_count": SUM[1],
        "budget_denied_count": SUM[2],
    },
    "billing_cost_daily": {
        "total_input_tokens": SUM[0],
        "total_output_tokens": SUM[1],
        "total_cost_usd": CD_COST_SUM,
        "request_count": SUM[3],
    },
}

SURVIVORS: dict[str, uuid.UUID] = {
    "token_usage": TU_KEEP,
    "billing_metering_daily": MD_KEEP,
    "billing_metering_hourly": MH_KEEP,
    "billing_cost_daily": CD_KEEP,
}

VICTIMS: dict[str, uuid.UUID] = {
    "token_usage": TU_VICTIM,
    "billing_metering_daily": MD_VICTIM,
    "billing_metering_hourly": MH_VICTIM,
    "billing_cost_daily": CD_VICTIM,
}

NEIGHBOURS: dict[str, uuid.UUID] = {
    "token_usage": TU_NEIGHBOUR,
    "billing_metering_daily": MD_NEIGHBOUR,
    "billing_metering_hourly": MH_NEIGHBOUR,
    "billing_cost_daily": CD_NEIGHBOUR,
}


# ---------------------------------------------------------------------------
# alembic plumbing
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


async def _run_upgrade() -> None:
    """Alembic is synchronous and opens its own connection — run it off-loop."""
    await asyncio.to_thread(_upgrade)


# ---------------------------------------------------------------------------
# Live-schema helpers
# ---------------------------------------------------------------------------


async def _tenant_column_types(conn) -> dict[str, str]:
    res = await conn.execute(
        text(
            "SELECT table_name, data_type FROM information_schema.columns "
            "WHERE table_schema = current_schema() AND column_name = 'tenant_id' "
            "  AND table_name = ANY(:names) ORDER BY 1"
        ),
        {"names": list(TENANT_TABLES)},
    )
    return {r[0]: r[1] for r in res}


async def _index_def(conn, name: str) -> str | None:
    res = await conn.execute(
        text(
            "SELECT indexdef FROM pg_indexes WHERE schemaname = current_schema() AND indexname = :n"
        ),
        {"n": name},
    )
    return res.scalar_one_or_none()


async def _column_names(conn, table: str) -> set[str]:
    res = await conn.execute(
        text(
            "SELECT column_name FROM information_schema.columns "
            "WHERE table_schema = current_schema() AND table_name = :t"
        ),
        {"t": table},
    )
    return {r[0] for r in res}


async def _count(conn, table: str) -> int:
    res = await conn.execute(text(f"SELECT count(*) FROM {table}"))
    return int(res.scalar_one())


async def _row(conn, table: str, row_id) -> dict | None:
    """One row with ``tenant_id`` rendered as text.

    ``::text`` and not the raw column: before the migration the column is
    ``varchar`` and after it is ``uuid`` (psycopg hands back a ``UUID``
    object), and every assertion in this file wants one comparable spelling.
    """
    res = await conn.execute(
        text(f"SELECT *, tenant_id::text AS tenant_id_text FROM {table} WHERE id = :id"),
        {"id": row_id},
    )
    got = res.mappings().one_or_none()
    return dict(got) if got is not None else None


async def _row_required(conn, table: str, row_id) -> dict:
    """`_row`, for callers that need the row to exist: absence fails loudly."""
    got = await _row(conn, table, row_id)
    assert got is not None, f"{table} row {row_id} is missing"
    return got


# ---------------------------------------------------------------------------
# PRE-state / POST-state
# ---------------------------------------------------------------------------

# The legacy index forms, copied from ``0011``'s own ``downgrade()``. These are
# what production holds today and what makes the ALTER impossible without a
# DROP first: ``coalesce(uuid, '')`` has no type resolution in Postgres.
_LEGACY_MARKUP_LOOKUP = (
    f"CREATE INDEX {MARKUP_LOOKUP} ON markup_rules "
    "(COALESCE(tenant_id, ''::character varying), provider, model, effective_from DESC)"
)
_LEGACY_MARKUP_OPEN = (
    f"CREATE UNIQUE INDEX {MARKUP_OPEN} ON markup_rules "
    "(COALESCE(tenant_id, ''::character varying), COALESCE(license_id::text, ''), "
    "COALESCE(service_key, ''), COALESCE(provider, ''), COALESCE(model, '')) "
    "WHERE effective_until IS NULL"
)

#: How Postgres renders the LEGACY tenant_id term of both markup indexes.
#: Matched exactly, not by the looser ``''::character varying`` — ``service_key``,
#: ``provider`` and ``model`` are genuinely varchar and coalesce to that same
#: literal in the POST-migration index too, so the loose form asserts nothing.
_LEGACY_TENANT_TERM = "COALESCE(tenant_id, ''::character varying)"
#: And how it must render afterwards.
_CAST_TENANT_TERM = "COALESCE((tenant_id)::text, ''::text)"


async def _force_varchar_pre_state(conn) -> None:
    """Put the schema back into the shape production is actually in.

    Without this the whole file is vacuous — see the module docstring.
    """
    await conn.execute(text(f"DROP INDEX IF EXISTS {MARKUP_LOOKUP}"))
    await conn.execute(text(f"DROP INDEX IF EXISTS {MARKUP_OPEN}"))
    for table in sorted(await _tenant_column_types(conn)):
        # billing_cost_events may be the partitioned PARENT — altering it
        # recurses into every child, which is exactly what production does.
        await conn.execute(
            text(
                f"ALTER TABLE {table} ALTER COLUMN tenant_id "
                "TYPE VARCHAR(255) USING tenant_id::text"
            )
        )
    await conn.execute(text(_LEGACY_MARKUP_LOOKUP))
    await conn.execute(text(_LEGACY_MARKUP_OPEN))


async def _assert_pre_state(conn) -> None:
    """All 14 columns varchar, both markup indexes in their legacy form."""
    types = await _tenant_column_types(conn)
    assert set(types) == set(TENANT_TABLES), (
        f"expected all {len(TENANT_TABLES)} tenant tables present, missing "
        f"{sorted(set(TENANT_TABLES) - set(types))}"
    )
    wrong = {t: dt for t, dt in types.items() if dt != "character varying"}
    assert not wrong, f"pre-state was not forced; these are not varchar: {wrong}"
    for name in (MARKUP_LOOKUP, MARKUP_OPEN):
        indexdef = await _index_def(conn, name)
        assert indexdef is not None, f"{name} missing from the pre-state"
        assert _LEGACY_TENANT_TERM in indexdef, (
            f"{name} is not in its legacy pre-migration form: {indexdef}"
        )


async def _restore_uuid_post_state(conn) -> None:
    """Return the session schema to exactly what ``create_all`` produced.

    Runs on BOTH sides of the fixture. The session-scoped schema is shared with
    every other test in the run, so a varchar column, a stray crosswalk table or
    a partitioned ``billing_cost_events`` left behind here would fail unrelated
    tests with an error naming neither this file nor the migration.
    """
    await conn.execute(text("DROP TABLE IF EXISTS alembic_version"))
    await conn.execute(text(f"DROP TABLE IF EXISTS {CROSSWALK}"))

    # Undo the partitioned rebuild, if a partition test (or a crashed one) left
    # one in place.
    res = await conn.execute(
        text(
            "SELECT relkind FROM pg_class WHERE relname = 'billing_cost_events' "
            "AND relnamespace = current_schema()::regnamespace"
        )
    )
    relkind = res.scalar_one_or_none()
    if relkind == "p":
        await conn.execute(text("DROP TABLE IF EXISTS billing_cost_events CASCADE"))
        await conn.run_sync(
            lambda sync_conn: cast(Table, CostEvent.__table__).create(sync_conn, checkfirst=True)
        )

    await conn.execute(text(f"DROP INDEX IF EXISTS {MARKUP_LOOKUP}"))
    await conn.execute(text(f"DROP INDEX IF EXISTS {MARKUP_OPEN}"))

    tables = ", ".join(f'"{t.name}"' for t in Base.metadata.sorted_tables)
    if tables:
        # TRUNCATE BEFORE the ALTER: a legacy label such as 'n8n-dev' cannot be
        # cast to uuid, so the type restore would fail on the corpus this file
        # just seeded.
        # alembic 0017 BEFORE TRUNCATE guard — same arming conftest does.
        await conn.exec_driver_sql(f"SET LOCAL {ALLOW_TRUNCATE_GUC} = '{ALLOW_TRUNCATE_VALUE}'")
        await conn.execute(text(f"TRUNCATE {tables} RESTART IDENTITY CASCADE"))

    for table, data_type in sorted((await _tenant_column_types(conn)).items()):
        if data_type != "uuid":
            await conn.execute(
                text(f"ALTER TABLE {table} ALTER COLUMN tenant_id TYPE uuid USING tenant_id::uuid")
            )

    # Recreate from Base.metadata rather than from hand-written DDL, so what the
    # rest of the suite sees is byte-identical to create_all's output.
    await conn.run_sync(
        lambda sync_conn: [
            index.create(sync_conn, checkfirst=True)
            for index in cast(Table, MarkupRule.__table__).indexes
        ]
    )


@pytest.fixture
async def migration_db(_db_engine_session):
    """Clean, post-0011 schema in; clean, post-0011 schema out.

    Depends on ``_db_engine_session`` (not ``db_engine``) on purpose — see the
    module docstring's Isolation note.
    """
    engine = _db_engine_session
    async with engine.begin() as conn:
        await _restore_uuid_post_state(conn)
    yield engine
    async with engine.begin() as conn:
        await _restore_uuid_post_state(conn)


@pytest.fixture
async def varchar_db(migration_db):
    """``migration_db`` with the production (varchar) pre-state forced."""
    async with migration_db.begin() as conn:
        await _force_varchar_pre_state(conn)
    return migration_db


# ---------------------------------------------------------------------------
# Seeds. Every INSERT below writes a legacy STRING into tenant_id and therefore
# cannot run at all unless the pre-state was really forced.
# ---------------------------------------------------------------------------

_INSERT_TOKEN_USAGE = text(
    """
    INSERT INTO token_usage (
        id, tenant_id, tenant_name, license_id, period_type, period_start,
        input_tokens, output_tokens, total_tokens
    ) VALUES (
        :id, :tenant_id, :tenant_name, :license_id, 'monthly', :period_start,
        :c0, :c1, :c2
    )
    """
)

_INSERT_METERING_DAILY = text(
    """
    INSERT INTO billing_metering_daily (
        id, tenant_id, tenant_name, license_id, subject_id, app_id, plan_id,
        service_key, bucket_date, request_count, denied_count, budget_denied_count
    ) VALUES (
        :id, :tenant_id, :tenant_name, :license_id, :subject_id, :app_id, :plan_id,
        'chat', :bucket_date, :c0, :c1, :c2
    )
    """
)

_INSERT_METERING_HOURLY = text(
    """
    INSERT INTO billing_metering_hourly (
        id, tenant_id, tenant_name, license_id, subject_id, app_id, plan_id,
        service_key, bucket_hour, request_count, denied_count, budget_denied_count
    ) VALUES (
        :id, :tenant_id, :tenant_name, :license_id, :subject_id, :app_id, :plan_id,
        'chat', :bucket_hour, :c0, :c1, :c2
    )
    """
)

_INSERT_COST_DAILY = text(
    """
    INSERT INTO billing_cost_daily (
        id, tenant_id, tenant_name, license_id, subject_id, service_key, provider,
        served_model, byo, owner_provided, cost_source, bucket_date,
        total_input_tokens, total_output_tokens, total_cost_usd, request_count
    ) VALUES (
        :id, :tenant_id, :tenant_name, :license_id, :subject_id, 'chat', 'anthropic',
        'claude-x', false, false, 'unknown', :bucket_date,
        :c0, :c1, :cost, :c3
    )
    """
)


async def _seed_merge_corpus(conn) -> None:
    """One colliding pair plus one non-colliding neighbour per merge table.

    The neighbour is the control for "no collateral loss": an over-broad merge
    keyed on nothing but the remapped tenant would fold it in too, and every
    positive assertion would still pass.
    """
    # --- token_usage: 'n8n-dev' vs its own REGISTRY uuid, one (licence, period)
    await conn.execute(
        _INSERT_TOKEN_USAGE,
        {
            "id": TU_KEEP,
            "tenant_id": "n8n-dev",
            "tenant_name": "n8n-dev",
            "license_id": LIC_TU,
            "period_start": COLLIDING_DATE,
            "c0": LOW[0],
            "c1": LOW[1],
            "c2": LOW[2],
        },
    )
    await conn.execute(
        _INSERT_TOKEN_USAGE,
        {
            "id": TU_VICTIM,
            "tenant_id": N8N_DEV_UUID,
            "tenant_name": N8N_DEV_UUID,
            "license_id": LIC_TU,
            "period_start": COLLIDING_DATE,
            "c0": HIGH[0],
            "c1": HIGH[1],
            "c2": HIGH[2],
        },
    )
    await conn.execute(
        _INSERT_TOKEN_USAGE,
        {
            "id": TU_NEIGHBOUR,
            "tenant_id": "gerard-dev",
            "tenant_name": "gerard-dev",
            "license_id": LIC_TU,
            "period_start": NEIGHBOUR_DATE,
            "c0": NEIGHBOUR_COUNTS[0],
            "c1": NEIGHBOUR_COUNTS[1],
            "c2": NEIGHBOUR_COUNTS[2],
        },
    )

    # --- billing_metering_daily: '' vs 'unknown', both -> the NIL tenant
    for row_id, tenant, counts in (
        (MD_KEEP, "", LOW),
        (MD_VICTIM, "unknown", HIGH),
    ):
        await conn.execute(
            _INSERT_METERING_DAILY,
            {
                "id": row_id,
                "tenant_id": tenant,
                "tenant_name": tenant,
                "license_id": LIC_MD,
                "subject_id": NULL_SUBJECT_ID,
                "app_id": NULL_APP_ID,
                "plan_id": NULL_PLAN_ID,
                "bucket_date": COLLIDING_DATE,
                "c0": counts[0],
                "c1": counts[1],
                "c2": counts[2],
            },
        )
    await conn.execute(
        _INSERT_METERING_DAILY,
        {
            "id": MD_NEIGHBOUR,
            "tenant_id": "gerard-dev",
            "tenant_name": "gerard-dev",
            "license_id": LIC_MD,
            "subject_id": NULL_SUBJECT_ID,
            "app_id": NULL_APP_ID,
            "plan_id": NULL_PLAN_ID,
            "bucket_date": NEIGHBOUR_DATE,
            "c0": NEIGHBOUR_COUNTS[0],
            "c1": NEIGHBOUR_COUNTS[1],
            "c2": NEIGHBOUR_COUNTS[2],
        },
    )

    # --- billing_metering_hourly: '__unattributed__' vs 'unknown' -> NIL
    for row_id, tenant, counts in (
        (MH_KEEP, "__unattributed__", LOW),
        (MH_VICTIM, "unknown", HIGH),
    ):
        await conn.execute(
            _INSERT_METERING_HOURLY,
            {
                "id": row_id,
                "tenant_id": tenant,
                "tenant_name": tenant,
                "license_id": LIC_MH,
                "subject_id": NULL_SUBJECT_ID,
                "app_id": NULL_APP_ID,
                "plan_id": NULL_PLAN_ID,
                "bucket_hour": COLLIDING_HOUR,
                "c0": counts[0],
                "c1": counts[1],
                "c2": counts[2],
            },
        )
    await conn.execute(
        _INSERT_METERING_HOURLY,
        {
            "id": MH_NEIGHBOUR,
            "tenant_id": "gerard-dev",
            "tenant_name": "gerard-dev",
            "license_id": LIC_MH,
            "subject_id": NULL_SUBJECT_ID,
            "app_id": NULL_APP_ID,
            "plan_id": NULL_PLAN_ID,
            "bucket_hour": NEIGHBOUR_HOUR,
            "c0": NEIGHBOUR_COUNTS[0],
            "c1": NEIGHBOUR_COUNTS[1],
            "c2": NEIGHBOUR_COUNTS[2],
        },
    )

    # --- billing_cost_daily: the money table. 'n8n-dev' vs its registry uuid.
    for row_id, tenant, counts, cost in (
        (CD_KEEP, "n8n-dev", LOW, CD_COST_LOW),
        (CD_VICTIM, N8N_DEV_UUID, HIGH, CD_COST_HIGH),
    ):
        await conn.execute(
            _INSERT_COST_DAILY,
            {
                "id": row_id,
                "tenant_id": tenant,
                "tenant_name": tenant,
                "license_id": LIC_CD,
                "subject_id": NULL_SUBJECT_ID,
                "bucket_date": COLLIDING_DATE,
                "c0": counts[0],
                "c1": counts[1],
                "cost": cost,
                "c3": counts[3],
            },
        )
    await conn.execute(
        _INSERT_COST_DAILY,
        {
            "id": CD_NEIGHBOUR,
            "tenant_id": "gerard-dev",
            "tenant_name": "gerard-dev",
            "license_id": LIC_CD,
            "subject_id": NULL_SUBJECT_ID,
            "bucket_date": NEIGHBOUR_DATE,
            "c0": NEIGHBOUR_COUNTS[0],
            "c1": NEIGHBOUR_COUNTS[1],
            "cost": CD_COST_NEIGHBOUR,
            "c3": NEIGHBOUR_COUNTS[3],
        },
    )


async def _seed_credential_row(conn) -> None:
    """The measured prod leak: an Authorization header sitting in tenant_id.

    Its own ``tenant_name`` carries the same string — that is what makes the
    scrub load-bearing rather than cosmetic.
    """
    await conn.execute(
        _INSERT_TOKEN_USAGE,
        {
            "id": CRED_ROW,
            "tenant_id": LEAKED_HEADER_SHAPE,
            "tenant_name": LEAKED_HEADER_SHAPE,
            "license_id": LIC_CRED,
            "period_start": COLLIDING_DATE,
            "c0": 2,
            "c1": 3,
            "c2": 5,
        },
    )


async def _seed_label_groups(conn) -> None:
    """Two merge groups that differ only in WHICH row carries the good label.

    Group A puts the good label on the HIGHER id (the row that does NOT
    survive), which is the case that separates the pinned-label rule from
    "whichever row happened to be inserted first". Group B puts it on the lower
    id, and the two groups are inserted in opposite physical order, so the
    outcome cannot depend on heap order either.
    """
    # Group A — insert the HIGHER id first.
    await conn.execute(
        _INSERT_TOKEN_USAGE,
        {
            "id": LBL_A_OTHER,
            "tenant_id": "unknown",
            "tenant_name": "acme-industries",
            "license_id": LIC_LBL_A,
            "period_start": COLLIDING_DATE,
            "c0": HIGH[0],
            "c1": HIGH[1],
            "c2": HIGH[2],
        },
    )
    await conn.execute(
        _INSERT_TOKEN_USAGE,
        {
            "id": LBL_A_KEEP,
            "tenant_id": "",
            "tenant_name": "unknown",
            "license_id": LIC_LBL_A,
            "period_start": COLLIDING_DATE,
            "c0": LOW[0],
            "c1": LOW[1],
            "c2": LOW[2],
        },
    )
    # Group B — insert the LOWER id first, good label already on the survivor.
    await conn.execute(
        _INSERT_TOKEN_USAGE,
        {
            "id": LBL_B_KEEP,
            "tenant_id": "",
            "tenant_name": "beta-werks",
            "license_id": LIC_LBL_B,
            "period_start": COLLIDING_DATE,
            "c0": LOW[0],
            "c1": LOW[1],
            "c2": LOW[2],
        },
    )
    await conn.execute(
        _INSERT_TOKEN_USAGE,
        {
            "id": LBL_B_OTHER,
            "tenant_id": "unknown",
            "tenant_name": "",
            "license_id": LIC_LBL_B,
            "period_start": COLLIDING_DATE,
            "c0": HIGH[0],
            "c1": HIGH[1],
            "c2": HIGH[2],
        },
    )


# ---------------------------------------------------------------------------
# Chain wiring
# ---------------------------------------------------------------------------


class TestRevisionChain:
    def test_the_chain_is_linear_and_0011_sits_directly_after_0010(self) -> None:
        """0011 follows 0010, and the chain has exactly ONE head.

        This deliberately does NOT assert that 0011 *is* the head. It used to,
        and adding 0012 broke it — which was the test being wrong, not the
        migration: pinning "X is head" makes every later migration fail a test
        that was never about X's position at the tip. The property worth keeping
        is that the chain never BRANCHES (two heads = `alembic upgrade head`
        becomes ambiguous and one branch silently never runs), plus 0011's own
        parent link.
        """
        script = ScriptDirectory.from_config(_alembic_config())
        heads = list(script.get_heads())
        assert len(heads) == 1, f"migration chain has branched: {heads}"
        assert script.get_revision(REVISION).down_revision == PREVIOUS_REVISION
        # 0011 must still be an ancestor of the single head, i.e. genuinely in
        # the chain rather than orphaned on a dead branch.
        ancestry = {r.revision for r in script.iterate_revisions(heads[0], "base")}
        assert REVISION in ancestry, f"{REVISION} is not an ancestor of head {heads[0]}"

    def test_revision_id_fits_alembic_version_column(self) -> None:
        """``alembic_version.version_num`` is varchar(32)."""
        assert len(REVISION) <= 32


# ---------------------------------------------------------------------------
# The vacuity control. If this class fails, nothing else in the file means
# anything — read the module docstring.
# ---------------------------------------------------------------------------


class TestPreStateIsVarchar:
    async def test_conftest_hands_us_uuid_columns_before_we_touch_anything(
        self, migration_db
    ) -> None:
        """Half of the trap: the suite's DEFAULT state is already migrated.

        ``Base.metadata.create_all`` builds ``tenant_id`` as ``uuid``, so a test
        that just calls ``command.upgrade`` exercises nothing at all.
        """
        async with migration_db.connect() as conn:
            types = await _tenant_column_types(conn)
        assert set(types) == set(TENANT_TABLES)
        assert set(types.values()) == {"uuid"}

    async def test_forced_pre_state_is_varchar_with_the_legacy_markup_indexes(
        self, varchar_db
    ) -> None:
        """The other half: the pre-state really is production's shape."""
        async with varchar_db.connect() as conn:
            await _assert_pre_state(conn)
            for name in (MARKUP_LOOKUP, MARKUP_OPEN):
                indexdef = await _index_def(conn, name)
                assert indexdef is not None, f"{name} missing from the pre-state"
                assert "tenant_id)::text" not in indexdef, (
                    f"{name} already carries the POST-migration cast: {indexdef}"
                )

    async def test_a_legacy_label_cannot_even_be_seeded_without_the_pre_state(
        self, migration_db
    ) -> None:
        """Self-enforcing control: the corpus is unseedable against uuid columns.

        This is why no other test in this file can quietly degrade into a no-op
        without failing — if the pre-state stops being forced, the seed dies
        with ``invalid input syntax for type uuid`` rather than going green.
        """
        with pytest.raises(DBAPIError) as excinfo:
            async with migration_db.begin() as conn:
                await _seed_merge_corpus(conn)
        # `.orig` is the psycopg diagnostic alone; `str(excinfo.value)` would
        # also echo the whole INSERT, which mentions uuid columns anyway and
        # would make this assertion pass for the wrong reason.
        assert "invalid input syntax for type uuid" in str(excinfo.value.orig)


# ---------------------------------------------------------------------------
# ASSERTION 8 — the zero-arms. CI runs `alembic upgrade head` against an empty
# ephemeral postgres:18, and the `migrate` init container runs it on every roll.
# ---------------------------------------------------------------------------


class TestZeroArms:
    async def test_upgrade_succeeds_against_a_database_with_no_legacy_rows(
        self, varchar_db
    ) -> None:
        """The init-container contract: nothing to remap is not an error."""
        async with varchar_db.connect() as conn:
            for table in TENANT_TABLES:
                assert await _count(conn, table) == 0, f"precondition: {table} must be empty"

        await _run_upgrade()

        async with varchar_db.connect() as conn:
            types = await _tenant_column_types(conn)
        assert set(types.values()) == {"uuid"}

    async def test_upgrade_succeeds_when_every_column_is_already_uuid(self, migration_db) -> None:
        """A re-run / already-migrated database must be a clean no-op, not a crash."""
        await _run_upgrade()

        async with migration_db.connect() as conn:
            types = await _tenant_column_types(conn)
            assert set(types.values()) == {"uuid"}
            # The crosswalk is still seeded — it is documentation for the CODE
            # paths (routers/config.py's '__default__'), not only for the data.
            assert await _count(conn, CROSSWALK) > 0


# ---------------------------------------------------------------------------
# ASSERTION 8b — the ALEMBIC-ONLY schema.
#
# `TestZeroArms` above is a FALSE NEGATIVE for this: pytest's schema comes from
# `Base.metadata.create_all`, and the models DO declare
# `billing_cost_daily.owner_provided`, so under pytest the column is ALWAYS
# present. In a brand-new database it is NOT: that column is created by the
# runtime additive DDL in `src/db.py`, which no migration runs. So on CI's own
# "upgrade head against an empty postgres:18" — and on every fresh environment
# — 0011's `_merge` called `_assert_columns` on a table that lacks the column
# and aborted the WHOLE migration with
#   RuntimeError: billing_cost_daily is missing column(s) ['owner_provided'] ...
# The zero-row arm was proved; the missing-COLUMN arm never was.
# ---------------------------------------------------------------------------


@pytest.fixture
async def cost_daily_without_owner_provided(varchar_db):
    """``varchar_db`` with ``billing_cost_daily.owner_provided`` removed.

    Reproduces the alembic-only schema a fresh database really has.
    """
    async with varchar_db.begin() as conn:
        await conn.execute(text("ALTER TABLE billing_cost_daily DROP COLUMN owner_provided"))
    yield varchar_db
    # Restore from Base.metadata, never by hand: `owner_provided` participates
    # in the `uq_cost_daily` unique constraint, so DROP COLUMN took that
    # constraint with it, and only create_all's own DDL puts BOTH back in the
    # shape the rest of the session-scoped schema expects. Same pattern as
    # `_restore_uuid_post_state`'s partition rebuild.
    async with varchar_db.begin() as conn:
        await conn.execute(text("DROP TABLE IF EXISTS billing_cost_daily CASCADE"))
        await conn.run_sync(
            lambda sync_conn: cast(Table, CostDaily.__table__).create(sync_conn, checkfirst=True)
        )


class TestAlembicOnlySchema:
    async def test_upgrade_succeeds_when_an_additive_column_is_missing_and_the_table_is_empty(
        self, cost_daily_without_owner_provided
    ) -> None:
        """THE DEFECT: `alembic upgrade head` on a brand-new database.

        An EMPTY table has no duplicates to fold, so the column assertion could
        only ever refuse work that does not exist. `_merge` must skip and the
        migration must complete.
        """
        engine = cost_daily_without_owner_provided
        async with engine.connect() as conn:
            assert "owner_provided" not in await _column_names(conn, "billing_cost_daily"), (
                "precondition: the alembic-only schema must NOT have owner_provided"
            )
            assert await _count(conn, "billing_cost_daily") == 0, (
                "precondition: billing_cost_daily must be empty"
            )

        await _run_upgrade()

        async with engine.connect() as conn:
            types = await _tenant_column_types(conn)
        assert set(types.values()) == {"uuid"}, (
            f"the migration did not complete on an alembic-only schema: {types}"
        )

    async def test_the_missing_column_still_raises_when_the_table_holds_rows(
        self, cost_daily_without_owner_provided
    ) -> None:
        """The complement, so the fix cannot be "always skip the assertion".

        A table that HOLDS rows is the only place summing the wrong columns
        could produce a wrong invoice, so there the guard must still bite and
        still name the column it cannot find.
        """
        engine = cost_daily_without_owner_provided
        async with engine.begin() as conn:
            # `owner_provided` is deliberately absent from this INSERT — the
            # column does not exist in the alembic-only schema.
            await conn.execute(
                text(
                    "INSERT INTO billing_cost_daily ("
                    "  id, tenant_id, tenant_name, license_id, subject_id, service_key, "
                    "  provider, served_model, byo, cost_source, bucket_date, "
                    "  total_input_tokens, total_output_tokens, total_cost_usd, request_count"
                    ") VALUES ("
                    "  :id, 'n8n-dev', 'n8n-dev', :license_id, :subject_id, 'chat', "
                    "  'anthropic', 'claude-x', false, 'unknown', :bucket_date, "
                    "  :c0, :c1, :cost, :c3"
                    ")"
                ),
                {
                    "id": CD_KEEP,
                    "license_id": LIC_CD,
                    "subject_id": NULL_SUBJECT_ID,
                    "bucket_date": COLLIDING_DATE,
                    "c0": LOW[0],
                    "c1": LOW[1],
                    "cost": CD_COST_LOW,
                    "c3": LOW[3],
                },
            )

        with pytest.raises(RuntimeError) as excinfo:
            await _run_upgrade()

        message = str(excinfo.value)
        assert "billing_cost_daily" in message
        assert "owner_provided" in message, f"the guard did not name the column: {message}"
        assert "must group or sum over" in message

        async with engine.connect() as conn:
            # One transaction: a raise leaves the pre-state completely intact.
            await _assert_pre_state(conn)
            assert await _count(conn, "billing_cost_daily") == 1


# ---------------------------------------------------------------------------
# ASSERTION 5 — the crosswalk
# ---------------------------------------------------------------------------


class TestCrosswalk:
    async def test_row_count_matches_the_frozen_literal_set(self, varchar_db) -> None:
        expected = sorted(set(migration.LEGACY_VALUES) | set(migration.RESERVED_SCOPES))
        assert len(expected) == 31, (
            "the frozen literal set changed size (30 measured literals + "
            f"'__default__'); got {len(expected)}"
        )

        # Which ``source`` each literal must carry, re-derived from the
        # migration's constant SETS — deliberately NOT by calling
        # ``canonical_tenant_uuid``, which is the very function that writes the
        # column and would therefore agree with any classification, including a
        # wrong one.
        expected_sources = {
            value: (
                "registry"
                if value in migration.REGISTRY
                else "sentinel"
                if value in migration.SENTINELS or value in migration.RESERVED_SCOPES
                else "derived"
            )
            for value in expected
        }
        by_class: dict[str, int] = {}
        for source in expected_sources.values():
            by_class[source] = by_class.get(source, 0) + 1
        # Measured against the production copy after 0011: 33 rows — 6 registry,
        # 21 derived, 4 sentinel, 2 credential. The two credential rows come
        # from real leaked bearer tokens and are matched by SHAPE, never by
        # literal, so a fresh database seeds none of them and the frozen set is
        # the other 31. ``test_a_credential_is_recorded_only_as_a_hash_...``
        # covers that fourth class.
        assert by_class == {"registry": 6, "derived": 21, "sentinel": 4}, (
            f"the frozen literal set re-classified: {by_class}"
        )

        await _run_upgrade()

        async with varchar_db.connect() as conn:
            res = await conn.execute(text(f"SELECT legacy_value, source FROM {CROSSWALK}"))
            rows = res.all()
            # Sorted in PYTHON, not by `ORDER BY`: the database collation is
            # en_US.utf8, which ignores punctuation, so '__default__' sorts
            # among the 'a's there and the comparison would fail on collation
            # rather than on content.
            got = sorted(r[0] for r in rows)
        assert len(got) == len(expected), (
            f"crosswalk holds {len(got)} rows, expected {len(expected)}; "
            f"unexpected: {sorted(set(got) - set(expected))}, "
            f"missing: {sorted(set(expected) - set(got))}"
        )
        # One row per literal and nothing extra: a duplicate would lengthen
        # ``got``, an omission would shorten it, and an unfrozen value shows up
        # as a membership difference.
        assert got == expected
        got_sources = dict(rows)
        assert got_sources == expected_sources, (
            "crosswalk source classification is wrong for: "
            f"{sorted(v for v in expected_sources if got_sources.get(v) != expected_sources[v])}"
        )

    async def test_default_config_scope_matches_the_tenancy_constant(self, varchar_db) -> None:
        """``src/tenancy.py`` and the migration must DERIVE the same uuid.

        Two hand-copied spellings of one uuid is a typo waiting to happen, and
        ``routers/config.py`` writes the literal ``__default__`` on a live path.
        """
        await _run_upgrade()

        async with varchar_db.connect() as conn:
            res = await conn.execute(
                text(f"SELECT tenant_uuid::text, source FROM {CROSSWALK} WHERE legacy_value = :v"),
                {"v": "__default__"},
            )
            tenant_uuid, source = res.one()
        assert tenant_uuid == _derived("__default__")
        assert tenant_uuid == DEFAULT_CONFIG_TENANT_ID
        assert source == "sentinel"

    async def test_n8n_dev_maps_to_the_registry_uuid_not_a_derived_one(self, varchar_db) -> None:
        """The whole point: billing already holds 44,712 rows under this uuid."""
        await _run_upgrade()

        async with varchar_db.connect() as conn:
            res = await conn.execute(
                text(f"SELECT tenant_uuid::text, source FROM {CROSSWALK} WHERE legacy_value = :v"),
                {"v": "n8n-dev"},
            )
            tenant_uuid, source = res.one()
        assert tenant_uuid == N8N_DEV_UUID
        assert source == "registry"
        assert tenant_uuid != _derived("n8n-dev"), (
            "n8n-dev must NOT take worker_management's derived uuid here — that "
            "value carries 0 rows in this database"
        )

    async def test_the_three_sentinel_spellings_all_map_to_the_nil_tenant(self, varchar_db) -> None:
        await _run_upgrade()

        async with varchar_db.connect() as conn:
            res = await conn.execute(
                text(
                    f"SELECT legacy_value, tenant_uuid::text, source FROM {CROSSWALK} "
                    "WHERE legacy_value = ANY(:v)"
                ),
                {"v": ["", "unknown", "__unattributed__"]},
            )
            rows = res.all()
        assert sorted(r[0] for r in rows) == ["", "__unattributed__", "unknown"]
        assert {r[1] for r in rows} == {NIL}
        assert {r[2] for r in rows} == {"sentinel"}

    async def test_an_unregistered_label_is_derived_uuid5(self, varchar_db) -> None:
        await _run_upgrade()

        async with varchar_db.connect() as conn:
            res = await conn.execute(
                text(f"SELECT tenant_uuid::text, source FROM {CROSSWALK} WHERE legacy_value = :v"),
                {"v": "gerard-dev"},
            )
            tenant_uuid, source = res.one()
        assert tenant_uuid == _derived("gerard-dev")
        assert source == "derived"

    async def test_a_credential_is_recorded_only_as_a_hash_and_scrubbed_everywhere(
        self, varchar_db
    ) -> None:
        """The leak must be neutralised without the token entering anything.

        The recorded ``bearer:<12 hex>`` is not reversible; the raw string must
        appear in NO crosswalk column and in NO ``tenant_name`` anywhere.
        """
        async with varchar_db.begin() as conn:
            await _seed_credential_row(conn)

        await _run_upgrade()

        async with varchar_db.connect() as conn:
            res = await conn.execute(
                text(f"SELECT tenant_uuid::text, source FROM {CROSSWALK} WHERE legacy_value = :v"),
                {"v": LEAKED_HEADER_DIGEST},
            )
            recorded = res.one_or_none()
            assert recorded is not None, (
                f"no crosswalk row for the redacted digest {LEAKED_HEADER_DIGEST!r}"
            )
            assert recorded[0] == NIL
            assert recorded[1] == "credential"

            # The row itself: remapped to nil, display name scrubbed.
            row = await _row(conn, "token_usage", CRED_ROW)
            assert row is not None, "the credential row must be remapped, never deleted"
            assert row["tenant_id_text"] == NIL
            assert row["tenant_name"] == "__unattributed__"

            # The raw value appears nowhere in the crosswalk...
            res = await conn.execute(
                text(
                    f"SELECT count(*) FROM {CROSSWALK} "
                    "WHERE legacy_value LIKE '%' || :v || '%' OR note LIKE '%' || :v || '%' "
                    "   OR source LIKE '%' || :v || '%'"
                ),
                {"v": LEAKED_HEADER_SHAPE},
            )
            assert res.scalar_one() == 0

            # ...nor in any tenant_name column in the database.
            res = await conn.execute(
                text(
                    "SELECT table_name FROM information_schema.columns "
                    "WHERE table_schema = current_schema() AND column_name = 'tenant_name' "
                    "ORDER BY 1"
                )
            )
            named_tables = [r[0] for r in res]
            assert named_tables, "no tenant_name columns found — this check is vacuous"
            for table in named_tables:
                res = await conn.execute(
                    text(f"SELECT count(*) FROM {table} WHERE tenant_name = :v"),
                    {"v": LEAKED_HEADER_SHAPE},
                )
                assert res.scalar_one() == 0, f"the leaked header survived in {table}.tenant_name"


# ---------------------------------------------------------------------------
# ASSERTIONS 1 + 2 + 3 — the merge really fires, conserves, and takes nothing else
# ---------------------------------------------------------------------------


class TestCollisionMerge:
    async def test_every_merge_group_collapses_to_the_expected_survivor(self, varchar_db) -> None:
        """ASSERTION 1. Two spellings of one tenant become exactly one row."""
        async with varchar_db.begin() as conn:
            await _seed_merge_corpus(conn)

        await _run_upgrade()

        async with varchar_db.connect() as conn:
            for table, keep_id in SURVIVORS.items():
                assert await _row(conn, table, keep_id) is not None, (
                    f"{table}: the expected survivor {keep_id} was deleted"
                )
                assert await _row(conn, table, VICTIMS[table]) is None, (
                    f"{table}: the victim {VICTIMS[table]} was NOT folded away — "
                    "the merge did not fire"
                )

    async def test_token_usage_merges_n8n_dev_onto_its_registry_uuid(self, varchar_db) -> None:
        """ASSERTION 1, the named prod case: usage billed to two identities."""
        async with varchar_db.begin() as conn:
            await _seed_merge_corpus(conn)

        await _run_upgrade()

        async with varchar_db.connect() as conn:
            res = await conn.execute(
                text(
                    "SELECT id, tenant_id::text FROM token_usage "
                    "WHERE license_id = :lic AND period_start = :d"
                ),
                {"lic": LIC_TU, "d": COLLIDING_DATE},
            )
            rows = res.all()
        assert len(rows) == 1, f"expected exactly one surviving row, got {rows}"
        assert rows[0][0] == TU_KEEP
        assert rows[0][1] == N8N_DEV_UUID

    async def test_metering_daily_merges_the_two_sentinel_spellings(self, varchar_db) -> None:
        """ASSERTION 1: '' and 'unknown' are one licence-day, not two."""
        async with varchar_db.begin() as conn:
            await _seed_merge_corpus(conn)

        await _run_upgrade()

        async with varchar_db.connect() as conn:
            res = await conn.execute(
                text(
                    "SELECT id, tenant_id::text FROM billing_metering_daily "
                    "WHERE license_id = :lic AND bucket_date = :d"
                ),
                {"lic": LIC_MD, "d": COLLIDING_DATE},
            )
            rows = res.all()
        assert len(rows) == 1, f"expected exactly one surviving row, got {rows}"
        assert rows[0][0] == MD_KEEP
        assert rows[0][1] == NIL

    async def test_every_additive_column_is_the_exact_sum_of_both_sides(self, varchar_db) -> None:
        """ASSERTION 2 — conservation, over the migration's OWN additive lists.

        The column names come from ``MERGE_TABLES``; the expected values use
        distinct primes per side and per column, so "kept one side", "summed the
        wrong column" and "double-summed" are all separately visible.
        """
        assert set(EXPECTED_SUMS) == set(MERGE_TABLES), (
            "MERGE_TABLES changed shape: this test now covers "
            f"{sorted(EXPECTED_SUMS)} but the migration merges {sorted(MERGE_TABLES)}"
        )

        async with varchar_db.begin() as conn:
            await _seed_merge_corpus(conn)

        await _run_upgrade()

        async with varchar_db.connect() as conn:
            for table, expected in EXPECTED_SUMS.items():
                additive = MERGE_TABLES[table]["additive"]
                assert set(expected) == set(additive), (
                    f"{table}: this test pins {sorted(expected)} but the migration "
                    f"sums {sorted(additive)} — an unchecked column would produce a "
                    "wrong invoice"
                )
                row = await _row(conn, table, SURVIVORS[table])
                assert row is not None
                for column, value in expected.items():
                    got = row[column]
                    if isinstance(value, Decimal):
                        assert Decimal(got) == value, f"{table}.{column}: {got} != {value}"
                    else:
                        assert int(got) == value, f"{table}.{column}: {got} != {value}"

    async def test_non_colliding_neighbours_survive_with_counters_unchanged(
        self, varchar_db
    ) -> None:
        """ASSERTION 3 — no collateral loss.

        Without this, a merge keyed on nothing but the remapped tenant (ignoring
        the rest of the unique key) passes every positive assertion above.
        """
        async with varchar_db.begin() as conn:
            await _seed_merge_corpus(conn)

        await _run_upgrade()

        async with varchar_db.connect() as conn:
            for table, neighbour_id in NEIGHBOURS.items():
                row = await _row(conn, table, neighbour_id)
                assert row is not None, f"{table}: the neighbour row was folded away"
                assert row["tenant_id_text"] == _derived("gerard-dev")
                assert row["tenant_name"] == "gerard-dev"
                for index, column in enumerate(MERGE_TABLES[table]["additive"]):
                    if column == "total_cost_usd":
                        assert Decimal(row[column]) == CD_COST_NEIGHBOUR
                    else:
                        assert int(row[column]) == NEIGHBOUR_COUNTS[index], (
                            f"{table}.{column} on the neighbour was rewritten"
                        )

    async def test_each_merge_table_loses_exactly_one_row(self, varchar_db) -> None:
        """ASSERTION 3 — the count drops by exactly the number of victims."""
        async with varchar_db.begin() as conn:
            await _seed_merge_corpus(conn)

        async with varchar_db.connect() as conn:
            before = {t: await _count(conn, t) for t in MERGE_TABLES}
        assert set(before.values()) == {3}, f"expected 3 seeded rows per merge table: {before}"

        await _run_upgrade()

        async with varchar_db.connect() as conn:
            after = {t: await _count(conn, t) for t in MERGE_TABLES}
        for table, count in after.items():
            assert count == before[table] - 1, (
                f"{table}: {before[table]} -> {count}; exactly one victim must be deleted"
            )

    async def test_survivor_takes_the_best_human_label_not_the_lowest_id_one(
        self, varchar_db
    ) -> None:
        """ASSERTION 10 — the PINNED survivor label rule.

        Group A's good label sits on the row that does NOT survive. Under the
        old behaviour (inherit the lowest id's name) the survivor would read
        ``'unknown'``; the rule says the group's best available label wins.
        """
        async with varchar_db.begin() as conn:
            await _seed_label_groups(conn)

        await _run_upgrade()

        async with varchar_db.connect() as conn:
            group_a = await _row(conn, "token_usage", LBL_A_KEEP)
            assert group_a is not None, "group A survivor must be the lowest id"
            assert await _row(conn, "token_usage", LBL_A_OTHER) is None
            assert group_a["tenant_name"] == "acme-industries", (
                "the good label lives on the HIGHER id; the survivor must still "
                f"take it, got {group_a['tenant_name']!r}"
            )
            assert group_a["tenant_id_text"] == NIL

            group_b = await _row(conn, "token_usage", LBL_B_KEEP)
            assert group_b is not None
            assert await _row(conn, "token_usage", LBL_B_OTHER) is None
            assert group_b["tenant_name"] == "beta-werks", (
                "insertion order must not decide the label either"
            )

    async def test_a_group_with_no_good_label_falls_back_to_the_lowest_id(self, varchar_db) -> None:
        """ASSERTION 10, negative control.

        The rule is documented as "can only improve" the old behaviour. When
        every candidate ranks badly ('' and 'unknown') the survivor keeps the
        lowest id's name — pinning that stops the rule from quietly inventing a
        label instead of choosing one.
        """
        async with varchar_db.begin() as conn:
            await _seed_merge_corpus(conn)

        await _run_upgrade()

        async with varchar_db.connect() as conn:
            row = await _row(conn, "billing_metering_daily", MD_KEEP)
        assert row is not None
        assert row["tenant_name"] == "", (
            "no row in this group carries a usable label, so the lowest id's "
            f"name must survive; got {row['tenant_name']!r}"
        )


# ---------------------------------------------------------------------------
# ASSERTION 4 — types and indexes
# ---------------------------------------------------------------------------


class TestTypesAndIndexes:
    async def test_all_fourteen_tenant_columns_report_uuid(self, varchar_db) -> None:
        async with varchar_db.begin() as conn:
            await _seed_merge_corpus(conn)

        await _run_upgrade()

        async with varchar_db.connect() as conn:
            types = await _tenant_column_types(conn)
        assert len(TENANT_TABLES) == 14
        assert set(types) == set(TENANT_TABLES), (
            f"not every tenant table was present: missing {sorted(set(TENANT_TABLES) - set(types))}"
        )
        still_text = {t: dt for t, dt in types.items() if dt != "uuid"}
        assert not still_text, f"tenant_id is STILL not uuid on: {still_text}"

    async def test_both_markup_indexes_are_rebuilt_with_the_text_cast(self, varchar_db) -> None:
        """``coalesce(uuid, '')`` has no type resolution — the cast is mandatory."""
        async with varchar_db.begin() as conn:
            await conn.execute(
                text(
                    "INSERT INTO markup_rules (tenant_id, provider, markup_pct, effective_from) "
                    "VALUES ('gerard-dev', 'anthropic', 30, now())"
                )
            )

        await _run_upgrade()

        async with varchar_db.connect() as conn:
            for name in (MARKUP_LOOKUP, MARKUP_OPEN):
                indexdef = await _index_def(conn, name)
                assert indexdef is not None, f"{name} was dropped and never recreated"
                assert "tenant_id)::text" in indexdef, (
                    f"{name} does not cast tenant_id to text: {indexdef}"
                )
                assert _CAST_TENANT_TERM in indexdef, (
                    f"{name} does not coalesce the CAST tenant_id: {indexdef}"
                )
                assert _LEGACY_TENANT_TERM not in indexdef, (
                    f"{name} still carries the legacy varchar coalesce on tenant_id: {indexdef}"
                )
            open_def = await _index_def(conn, MARKUP_OPEN)
            assert open_def is not None, f"{MARKUP_OPEN} was dropped and never recreated"
            assert "UNIQUE" in open_def
            assert "effective_until IS NULL" in open_def


# ---------------------------------------------------------------------------
# ASSERTION 7 — partition recursion
#
# billing_cost_events is a RANGE-partitioned parent with 13 monthly children in
# production, but Base.metadata.create_all builds it as a PLAIN table. The
# migration deliberately alters only the parent and relies on Postgres
# recursing. Nothing tests that, and it would be catastrophic if wrong — so the
# table is rebuilt here in its production shape.
# ---------------------------------------------------------------------------


def _partition_months() -> list[tuple[str, str, str]]:
    """The 13 monthly children, 2026_05 .. 2027_05, no DEFAULT partition."""
    out: list[tuple[str, str, str]] = []
    year, month = 2026, 5
    for _ in range(13):
        next_year, next_month = (year + 1, 1) if month == 12 else (year, month + 1)
        out.append(
            (
                f"billing_cost_events_{year}_{month:02d}",
                f"{year}-{month:02d}-01 00:00:00+00",
                f"{next_year}-{next_month:02d}-01 00:00:00+00",
            )
        )
        year, month = next_year, next_month
    return out


PARTITIONS = _partition_months()


@pytest.fixture
async def partitioned_cost_events(migration_db):
    """Rebuild ``billing_cost_events`` as the production partitioned parent.

    ``LIKE ... INCLUDING DEFAULTS`` copies the column list verbatim so this
    cannot drift from the model. No teardown of its own: ``migration_db``'s
    ``_restore_uuid_post_state`` detects ``relkind = 'p'`` and rebuilds the plain
    table from ``Base.metadata``, which also covers a mid-test crash.
    """
    async with migration_db.begin() as conn:
        await conn.execute(
            text(
                "CREATE TABLE _bce_partitioned "
                "(LIKE billing_cost_events INCLUDING DEFAULTS) PARTITION BY RANGE (created_at)"
            )
        )
        await conn.execute(text("DROP TABLE billing_cost_events CASCADE"))
        await conn.execute(text("ALTER TABLE _bce_partitioned RENAME TO billing_cost_events"))
        for name, lower, upper in PARTITIONS:
            await conn.execute(
                text(
                    f"CREATE TABLE {name} PARTITION OF billing_cost_events "
                    f"FOR VALUES FROM ('{lower}') TO ('{upper}')"
                )
            )
        await _force_varchar_pre_state(conn)
    return migration_db


class TestPartitionRecursion:
    async def test_every_child_partition_is_flipped_by_the_parent_alter(
        self, partitioned_cost_events
    ) -> None:
        engine = partitioned_cost_events

        async with engine.begin() as conn:
            res = await conn.execute(
                text(
                    "SELECT count(*) FROM pg_inherits i "
                    "JOIN pg_class p ON p.oid = i.inhparent "
                    "WHERE p.relname = 'billing_cost_events' "
                    "  AND p.relnamespace = current_schema()::regnamespace"
                )
            )
            assert res.scalar_one() == 13, "precondition: 13 monthly children must exist"

            # Pre-state control: the children must start as varchar, otherwise
            # the post-assertion below proves nothing.
            res = await conn.execute(
                text(
                    "SELECT table_name, data_type FROM information_schema.columns "
                    "WHERE table_schema = current_schema() AND column_name = 'tenant_id' "
                    "  AND table_name = ANY(:names)"
                ),
                {"names": [p[0] for p in PARTITIONS]},
            )
            before = {r[0]: r[1] for r in res}
            assert len(before) == 13
            assert set(before.values()) == {"character varying"}

            # A legacy row inside ONE child — the parent UPDATE must reach it.
            await conn.execute(
                text(
                    # input_tokens / output_tokens are NOT NULL with PYTHON-side
                    # defaults only, so `LIKE ... INCLUDING DEFAULTS` copies no
                    # default for them and they must be supplied here.
                    "INSERT INTO billing_cost_events "
                    "(id, tenant_id, tenant_name, service_key, provider, "
                    " input_tokens, output_tokens, created_at) "
                    "VALUES (:id, 'n8n-dev', 'n8n-dev', 'chat', 'anthropic', 5, 7, :ts)"
                ),
                {"id": CE_ROW, "ts": datetime(2026, 8, 15, 12, 0, 0, tzinfo=UTC)},
            )

        await _run_upgrade()

        async with engine.connect() as conn:
            res = await conn.execute(
                text(
                    "SELECT table_name, data_type FROM information_schema.columns "
                    "WHERE table_schema = current_schema() AND column_name = 'tenant_id' "
                    "  AND table_name = ANY(:names) ORDER BY 1"
                ),
                {"names": [p[0] for p in PARTITIONS] + ["billing_cost_events"]},
            )
            after = {r[0]: r[1] for r in res}
            assert len(after) == 14, f"expected the parent + 13 children, got {sorted(after)}"
            still_text = {t: dt for t, dt in after.items() if dt != "uuid"}
            assert not still_text, (
                "the parent ALTER did not recurse into every partition: "
                f"{still_text}. Children are never altered individually, so a "
                "non-recursing ALTER leaves the table unusable."
            )

            row = await _row(conn, "billing_cost_events", CE_ROW)
            assert row is not None, "the row inside the partition was lost"
            assert row["tenant_id_text"] == N8N_DEV_UUID, (
                "the remap UPDATE on the parent must reach rows stored in children"
            )


# ---------------------------------------------------------------------------
# ASSERTION 6 — the guards fire, and fire CLEAN
# ---------------------------------------------------------------------------


class TestCoverageGuard:
    async def test_an_uncovered_literal_raises_and_names_it(self, varchar_db) -> None:
        """``__admin__`` is deliberately NOT minted — see the migration docstring.

        ``auth_deps.py`` can return it and ``routers/webhooks.py`` would INSERT
        it. Minting a tenant for an admin JWT would make it look like a tenant
        forever, so the guard must RAISE and name the literal instead.

        Seeded into ``audit_log`` rather than ``tenant_webhooks``: same code
        path, no ``secret`` column for the repo's ``detect-secrets`` hook to
        squint at.
        """
        async with varchar_db.begin() as conn:
            await conn.execute(
                text(
                    "INSERT INTO audit_log (action, resource_type, tenant_id, tenant_name) "
                    "VALUES ('update', 'tenant_config', '__admin__', '__admin__')"
                )
            )

        with pytest.raises(RuntimeError) as excinfo:
            await _run_upgrade()

        message = str(excinfo.value)
        assert "is NOT total" in message
        assert "'__admin__'" in message, f"the guard did not name the literal: {message}"

    async def test_the_coverage_guard_rewrites_nothing_before_it_raises(self, varchar_db) -> None:
        """A guard that raises AFTER half the work is worse than no guard.

        The whole migration is one transaction (``alembic/env.py``), and the
        guard runs before the merges, the remap and every ALTER. Assert the
        schema and the corpus are untouched, not merely that it raised.
        """
        async with varchar_db.begin() as conn:
            await _seed_merge_corpus(conn)
            await conn.execute(
                text(
                    "INSERT INTO audit_log (action, resource_type, tenant_id, tenant_name) "
                    "VALUES ('update', 'tenant_config', '__admin__', '__admin__')"
                )
            )

        with pytest.raises(RuntimeError):
            await _run_upgrade()

        async with varchar_db.connect() as conn:
            # Nothing rewritten: still varchar, still the legacy indexes.
            await _assert_pre_state(conn)
            # Nothing merged, nothing deleted.
            for table in MERGE_TABLES:
                assert await _count(conn, table) == 3, f"{table} was modified before the raise"
                row = await _row(conn, table, SURVIVORS[table])
                assert row is not None
                for index, column in enumerate(MERGE_TABLES[table]["additive"]):
                    if column == "total_cost_usd":
                        assert Decimal(row[column]) == CD_COST_LOW
                    else:
                        assert int(row[column]) == LOW[index]
            # The crosswalk is created inside the same transaction, so a
            # rollback must take it with it.
            res = await conn.execute(text("SELECT to_regclass(:t)"), {"t": CROSSWALK})
            assert res.scalar_one() is None, (
                f"{CROSSWALK} survived a rolled-back upgrade — the guard did not "
                "run inside the migration transaction"
            )


class TestNoMergeGuard:
    async def test_a_post_remap_duplicate_in_tenant_config_raises(self, varchar_db) -> None:
        """Two config rows for one key cannot be summed — only chosen by hand."""
        async with varchar_db.begin() as conn:
            for row_id, tenant in ((CFG_EMPTY, ""), (CFG_UNKNOWN, "unknown")):
                await conn.execute(
                    text(
                        "INSERT INTO tenant_config (id, tenant_id, tenant_name, markup_pct, "
                        "billing_mode, updated_at) "
                        "VALUES (:id, :tenant_id, :tenant_id, 30, 'standard_markup', now())"
                    ),
                    {"id": row_id, "tenant_id": tenant},
                )

        with pytest.raises(RuntimeError) as excinfo:
            await _run_upgrade()

        message = str(excinfo.value)
        assert "tenant_config" in message
        assert "duplicate group(s)" in message
        assert "cannot be summed" in message

    async def test_the_no_merge_guard_rewrites_nothing_before_it_raises(self, varchar_db) -> None:
        async with varchar_db.begin() as conn:
            for row_id, tenant in ((CFG_EMPTY, ""), (CFG_UNKNOWN, "unknown")):
                await conn.execute(
                    text(
                        "INSERT INTO tenant_config (id, tenant_id, tenant_name, markup_pct, "
                        "billing_mode, updated_at) "
                        "VALUES (:id, :tenant_id, :tenant_id, 30, 'standard_markup', now())"
                    ),
                    {"id": row_id, "tenant_id": tenant},
                )

        with pytest.raises(RuntimeError):
            await _run_upgrade()

        async with varchar_db.connect() as conn:
            await _assert_pre_state(conn)
            assert await _count(conn, "tenant_config") == 2
            assert (await _row_required(conn, "tenant_config", CFG_EMPTY))["tenant_id_text"] == ""
            assert (await _row_required(conn, "tenant_config", CFG_UNKNOWN))[
                "tenant_id_text"
            ] == "unknown"

    async def test_two_spellings_of_one_uuid_are_caught_by_the_guard_not_by_the_alter(
        self, varchar_db
    ) -> None:
        """THE DEFECT: the post-remap expression was written out TWICE.

        ``_merge`` and ``_no_merge_guard`` each carried their own copy of it, and
        when the ``::uuid`` cast was added it landed in only ONE of them — so the
        guard grouped on TEXT. ``019DE842-…`` and ``019de842-…`` are the SAME
        uuid in two legal spellings; a ``varchar`` column stores both verbatim,
        so as text they are two distinct values. A caller can create exactly this
        state today with ``PUT /api/config/tenants/019DE842-…`` while the column
        is still varchar. Reproduced before the fix::

            coverage guard: PASSED   _no_merge_guard: PASSED (0 duplicate groups)
            *** ALTER FAILED: UniqueViolation on tenant_config_tenant_id_key

        Neither value is a crosswalk ``legacy_value`` (both are already
        uuid-parseable, so the coverage guard passes them straight through) and
        neither is credential-shaped, so both take the ``COALESCE`` fallback and
        the cast is the ONLY thing that can collapse them. The guard must RAISE
        and name ``tenant_config`` BEFORE anything is rewritten: a
        ``UniqueViolation`` out of the ALTER instead rolls the whole migration
        back, and the ``migrate`` init container behind it retries forever.

        Sibling of the two tests above, and deliberately the SAME table: this is
        the arm they cannot reach, because ``''`` and ``'unknown'`` collide via
        the crosswalk and would collide on text just as well.
        """
        upper = N8N_DEV_UUID.upper()
        assert upper != N8N_DEV_UUID, "precondition: two DIFFERENT strings..."
        assert uuid.UUID(upper) == uuid.UUID(N8N_DEV_UUID), "...that are the SAME uuid"

        async with varchar_db.begin() as conn:
            for row_id, tenant in ((CFG_UPPER, upper), (CFG_LOWER, N8N_DEV_UUID)):
                await conn.execute(
                    text(
                        "INSERT INTO tenant_config (id, tenant_id, tenant_name, markup_pct, "
                        "billing_mode, updated_at) "
                        "VALUES (:id, :tenant_id, :tenant_id, 30, 'standard_markup', now())"
                    ),
                    {"id": row_id, "tenant_id": tenant},
                )
            # The premise, asserted rather than assumed: varchar keeps the
            # caller's spelling, so both rows really are stored unnormalised.
            assert (await _row_required(conn, "tenant_config", CFG_UPPER))[
                "tenant_id_text"
            ] == upper
            assert (await _row_required(conn, "tenant_config", CFG_LOWER))[
                "tenant_id_text"
            ] == N8N_DEV_UUID

        # DBAPIError is in the expected set ON PURPOSE. Without the cast this
        # test must fail by NAMING the UniqueViolation the guard should have
        # pre-empted — the distinction between the two exceptions IS the test.
        with pytest.raises((RuntimeError, DBAPIError)) as excinfo:
            await _run_upgrade()

        error = excinfo.value
        assert not isinstance(error, DBAPIError), (
            "the guard did NOT fire — the ALTER died on tenant_config's unique "
            "index instead, which is exactly the defect this test pins. psycopg "
            f"said: {getattr(error, 'orig', error)}"
        )
        # A RuntimeError echoes no SQL and carries no `.orig`, so unlike the
        # DBAPI assertions elsewhere in this file, str() here is the guard's own
        # message and nothing else — it cannot pass for the wrong reason.
        message = str(error)
        assert "tenant_config" in message, f"the guard did not name the table: {message}"
        assert "duplicate group(s)" in message
        assert "cannot be summed" in message

        async with varchar_db.connect() as conn:
            # One transaction: the raise leaves the pre-state completely intact.
            await _assert_pre_state(conn)
            assert await _count(conn, "tenant_config") == 2, "a row was folded away"
            assert (await _row_required(conn, "tenant_config", CFG_UPPER))[
                "tenant_id_text"
            ] == upper
            assert (await _row_required(conn, "tenant_config", CFG_LOWER))[
                "tenant_id_text"
            ] == N8N_DEV_UUID


# ---------------------------------------------------------------------------
# ASSERTION 11 — NULL semantics
# ---------------------------------------------------------------------------


class TestNullTenantSemantics:
    async def test_null_markup_rules_are_neither_merged_nor_raised_on(self, varchar_db) -> None:
        """NULL is the WILDCARD in ``markup_rules`` — not a tenant, not a dupe.

        ``GROUP BY`` is ``IS NOT DISTINCT FROM``, so two NULL-keyed rows land in
        one group; the live unique index coalesces NULL to ``''`` and these two
        differ on ``provider``, so neither the merge nor the no-merge guard may
        touch them. A remap that treated NULL as a legacy literal would rewrite
        the global default rule onto a tenant.
        """
        async with varchar_db.begin() as conn:
            await conn.execute(
                text(
                    "INSERT INTO markup_rules (tenant_id, provider, markup_pct, effective_from) "
                    "VALUES (NULL, 'anthropic', 10, now()), "
                    "       (NULL, 'openai', 20, now()), "
                    "       ('gerard-dev', 'anthropic', 30, now())"
                )
            )

        await _run_upgrade()

        async with varchar_db.connect() as conn:
            res = await conn.execute(
                text(
                    "SELECT provider, tenant_id::text, markup_pct FROM markup_rules "
                    "ORDER BY provider, tenant_id NULLS FIRST"
                )
            )
            rows = res.all()

        assert len(rows) == 3, f"a markup_rules row was merged away: {rows}"
        by_key = {(r[0], r[1]): r[2] for r in rows}
        assert by_key[("anthropic", None)] == 10, "the global default rule must stay NULL"
        assert by_key[("openai", None)] == 20, "the second wildcard rule must stay NULL"
        assert by_key[("anthropic", _derived("gerard-dev"))] == 30


# ---------------------------------------------------------------------------
# ASSERTION 9 — idempotency. A double-summing merge shows here and nowhere else.
# ---------------------------------------------------------------------------


class TestIdempotency:
    async def test_a_second_stamp_and_upgrade_is_a_clean_noop(self, varchar_db) -> None:
        """The literal re-run: stamp 0010, upgrade 0011 again."""
        async with varchar_db.begin() as conn:
            await _seed_merge_corpus(conn)

        await _run_upgrade()
        async with varchar_db.begin() as conn:
            # `command.stamp` needs to move BACK to 0010; drop the table rather
            # than depend on alembic's downgrade-stamp behaviour.
            await conn.execute(text("DROP TABLE IF EXISTS alembic_version"))

        await _run_upgrade()

        async with varchar_db.connect() as conn:
            for table, expected in EXPECTED_SUMS.items():
                assert await _count(conn, table) == 2
                row = await _row(conn, table, SURVIVORS[table])
                assert row is not None
                for column, value in expected.items():
                    got = row[column]
                    if isinstance(value, Decimal):
                        assert Decimal(got) == value, f"{table}.{column} changed on re-run"
                    else:
                        assert int(got) == value, f"{table}.{column} changed on re-run"

    async def test_replaying_the_migration_over_merged_data_does_not_double_sum(
        self, varchar_db
    ) -> None:
        """The re-run that can actually go wrong.

        The plain re-run above short-circuits at ``text_tables == []`` because
        the columns are already uuid, so it cannot double-sum. Force the varchar
        pre-state a SECOND time — the shape a DR restore or a rolled-back deploy
        produces — and replay. The merge now really runs again over already
        merged rows and must find no duplicate group at all.
        """
        async with varchar_db.begin() as conn:
            await _seed_merge_corpus(conn)

        await _run_upgrade()

        async with varchar_db.begin() as conn:
            await conn.execute(text("DROP TABLE IF EXISTS alembic_version"))
            await _force_varchar_pre_state(conn)

        await _run_upgrade()

        async with varchar_db.connect() as conn:
            for table, expected in EXPECTED_SUMS.items():
                assert await _count(conn, table) == 2, (
                    f"{table}: the replay deleted another row — the merge is not idempotent"
                )
                row = await _row(conn, table, SURVIVORS[table])
                assert row is not None
                for column, value in expected.items():
                    got = row[column]
                    if isinstance(value, Decimal):
                        assert Decimal(got) == value, (
                            f"{table}.{column} was summed twice: {got} != {value}"
                        )
                    else:
                        assert int(got) == value, (
                            f"{table}.{column} was summed twice: {got} != {value}"
                        )
                neighbour = await _row(conn, table, NEIGHBOURS[table])
                assert neighbour is not None, f"{table}: the replay ate the neighbour"

            types = await _tenant_column_types(conn)
            assert set(types.values()) == {"uuid"}
