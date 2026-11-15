"""The compound rollup-watermark gate on the metering ledger (alembic 0023).

THE DEFECT
----------
The 2026-09-05T07:01:56Z TRUNCATE named ``billing_metering_events`` in the same
statement that emptied ``billing_cost_events`` and its 12 partitions. #140
(alembic 0019) gave the COST ledger a state gate — refuse while raw rows exist
newer than the rollup watermark, even for a session that has already armed
0017's intent hatch. Measured on the production primary before this change, the
metering ledger carried ``billing_delete_audit_trg``,
``billing_truncate_audit_trg`` and ``billing_truncate_guard_trg`` — the intent
gate — and no state gate at all.

0019 could not simply be attached here: ``billing_forbid_unrolled_truncate()``
hardcodes ``public.billing_cost_daily`` and ignores ``TG_TABLE_NAME``, so on the
metering table it would have judged metering rows against the COST watermark —
a gate that answers confidently about a table it never measured.

BOTH ARMS ARE ASSERTED, DELIBERATELY
------------------------------------
A gate that refuses everything is as broken as one that refuses nothing. So the
caught-up arm (``..._is_allowed_when_both_stages_have_caught_up``) is a
first-class test here, not an afterthought: removing the ``IF NOT v_unrolled
THEN RETURN NULL`` early-out must go red just as loudly as removing the ``RAISE
EXCEPTION`` does, and the two sets of failures must be DISJOINT.

THE COMPOUND PREDICATE IS THE POINT
-----------------------------------
Metering aggregates in two stages that lag independently. ``rollup_daily``
builds a day from whatever ``billing_metering_hourly`` holds for it without
requiring all 24 hours, so ``max(bucket_date)`` can advance over holed hourly
coverage — which is why ``..._daily_watermark_alone_does_not_open_the_gate``
exists and why it is the test that separates this control from a naive port of
0019.
"""

from __future__ import annotations

import importlib.util
import os
import re
import uuid
from collections.abc import Iterator
from datetime import UTC, date, datetime, timedelta
from pathlib import Path

import pytest
import sqlalchemy as sa
from alembic import command
from alembic.config import Config
from sqlalchemy.exc import DBAPIError

import models.db.metering_unrolled_guard as mg
import models.db.unrolled_guard as ug
from models.db import (
    ALLOW_TRUNCATE_GUC,
    ALLOW_TRUNCATE_VALUE,
    ALLOW_UNROLLED_METERING_TRUNCATE_GUC,
    ALLOW_UNROLLED_METERING_TRUNCATE_VALUE,
    ALLOW_UNROLLED_TRUNCATE_GUC,
    NULL_APP_ID,
    NULL_LICENSE_ID,
    NULL_PLAN_ID,
    NULL_SUBJECT_ID,
    UNROLLED_METERING_OVERRIDE_OPERATION,
)
from tests.conftest import throwaway_db_name

MIGRATION = Path(__file__).parent.parent / "alembic" / "versions" / "0023_metering_unrolled_gate.py"

TENANT = "11111111-1111-1111-1111-111111111111"


def _load_migration():  # noqa: ANN202
    spec = importlib.util.spec_from_file_location("_mig_0023", MIGRATION)
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


# ---------------------------------------------------------------------------
# A schema that can actually be wrong about partitioning
#
# ⛔ ``db_engine`` is built by ``Base.metadata.create_all``, which never
# partitions anything. Any catalog question whose answer DIFFERS between the ORM
# and the migration chain — relkind above all — is unanswerable there, and
# answers 'r' about every table with total confidence. That is not a theoretical
# limit: billing_cost_events reads 'r' on db_engine and 'p' with 14 children
# here, on the same postgres, in the same test run.
#
# So the partitioning oracle gets its own database: the FULL chain from 0001
# onto a throwaway, catalogs read there, database dropped. Module-scoped because
# the upgrade is the expensive part and every case below asks a read-only
# question of the same schema. Same shape as
# tests/test_migration_0022_provider_pricing_cost_basis.py, which built one for
# the same reason.
# ---------------------------------------------------------------------------

# ⚠️ Per-PROCESS, not per-xdist-worker. The previous name was fixed and
# suffixed only under xdist, which isolated workers from each other and left
# two independent lanes on the same Postgres server sharing it — and the
# teardown below is `DROP DATABASE … WITH (FORCE)`, which SIGTERMs whatever is
# attached. See `throwaway_db_name` in tests/conftest.py.
_THROWAWAY_DB = throwaway_db_name("_billing_0023_partition_check")


def _alembic_cfg() -> Config:
    repo_root = Path(__file__).resolve().parents[1]
    cfg = Config(str(repo_root / "alembic.ini"))
    cfg.set_main_option("script_location", str(repo_root / "alembic"))
    return cfg


@pytest.fixture(scope="module")
def alembic_built_db() -> Iterator[sa.engine.URL]:  # noqa: ANN201
    dsn = os.getenv("TEST_DATABASE_URL") or os.getenv("DATABASE_URL")
    assert dsn, "TEST_DATABASE_URL/DATABASE_URL must be set"

    # `_THROWAWAY_DB` is already unique to this pytest PROCESS, which covers
    # xdist (each worker is its own process) AND the concurrent-lane case an
    # xdist-only suffix could not see.
    name = _THROWAWAY_DB

    server = sa.make_url(dsn).set(database="postgres")
    admin = sa.create_engine(server, isolation_level="AUTOCOMMIT", future=True)
    with admin.connect() as conn:
        conn.execute(sa.text(f'DROP DATABASE IF EXISTS "{name}" WITH (FORCE)'))
        conn.execute(sa.text(f'CREATE DATABASE "{name}"'))
    admin.dispose()

    db_url = server.set(database=name)
    saved = os.environ.get("DATABASE_URL")
    try:
        cfg = _alembic_cfg()
        rendered = db_url.render_as_string(hide_password=False)
        cfg.set_main_option("sqlalchemy.url", rendered)
        # alembic/env.py resolves the URL from the environment; setting only the
        # config option would migrate whatever DATABASE_URL points at instead.
        os.environ["DATABASE_URL"] = rendered
        command.upgrade(cfg, "head")
        yield db_url
    finally:
        if saved is None:
            os.environ.pop("DATABASE_URL", None)
        else:
            os.environ["DATABASE_URL"] = saved
        cleanup = sa.create_engine(server, isolation_level="AUTOCOMMIT", future=True)
        with cleanup.connect() as conn:
            conn.execute(sa.text(f'DROP DATABASE IF EXISTS "{name}" WITH (FORCE)'))
        cleanup.dispose()


def _metering_event_row(created_at: datetime) -> dict:
    return {
        "id": str(uuid.uuid4()),
        "tenant_id": TENANT,
        "tenant_name": "gate-test",
        "method": "GET",
        "path": "/gate-test",
        "service_key": "gate-test",  # pragma: allowlist secret
        "status": "allowed",
        "created_at": created_at,
    }


def _hourly_row(bucket_hour: datetime) -> dict:
    return {
        "id": str(uuid.uuid4()),
        "tenant_id": TENANT,
        "tenant_name": "gate-test",
        "license_id": str(NULL_LICENSE_ID),
        "subject_id": str(NULL_SUBJECT_ID),
        "app_id": str(NULL_APP_ID),
        "plan_id": str(NULL_PLAN_ID),
        "service_key": "gate-test",  # pragma: allowlist secret
        "bucket_hour": bucket_hour,
        "request_count": 1,
        "denied_count": 0,
    }


def _daily_row(bucket_date: date) -> dict:
    return {
        "id": str(uuid.uuid4()),
        "tenant_id": TENANT,
        "tenant_name": "gate-test",
        "license_id": str(NULL_LICENSE_ID),
        "subject_id": str(NULL_SUBJECT_ID),
        "app_id": str(NULL_APP_ID),
        "plan_id": str(NULL_PLAN_ID),
        "service_key": "gate-test",  # pragma: allowlist secret
        "bucket_date": bucket_date,
        "request_count": 1,
        "denied_count": 0,
    }


async def _seed(conn, table: str, rows: list[dict]) -> None:
    cols = ", ".join(rows[0])
    binds = ", ".join(f":{c}" for c in rows[0])
    await conn.execute(sa.text(f"INSERT INTO {table} ({cols}) VALUES ({binds})"), rows)


async def _arm_intent(conn) -> None:
    """0017's hatch only: 'I mean to truncate'."""
    await conn.exec_driver_sql(f"SET LOCAL {ALLOW_TRUNCATE_GUC} = '{ALLOW_TRUNCATE_VALUE}'")


async def _arm_metering_override(conn) -> None:
    """0023's hatch: 'I mean to delete un-aggregated metering'."""
    await conn.exec_driver_sql(
        f"SET LOCAL {ALLOW_UNROLLED_METERING_TRUNCATE_GUC} = "
        f"'{ALLOW_UNROLLED_METERING_TRUNCATE_VALUE}'"
    )


async def _reset(conn) -> None:
    """Empty the three metering relations from a KNOWN state, past every gate.

    Not ``db_engine``'s own truncate: that arms the override, so it is the very
    behaviour under test and cannot also be the fixture that sets up for it.
    A plain DELETE fires no TRUNCATE trigger at all.
    """
    await conn.execute(sa.text("DELETE FROM billing_metering_events"))
    await conn.execute(sa.text("DELETE FROM billing_metering_hourly"))
    await conn.execute(sa.text("DELETE FROM billing_metering_daily"))
    await conn.execute(sa.text("DELETE FROM billing_delete_audit"))


# ---------------------------------------------------------------------------
# The two copies of the DDL cannot drift
# ---------------------------------------------------------------------------


def test_migration_ddl_is_byte_identical_to_the_module() -> None:
    """0012 and 0015 exist only because this repo let two schema sources drift."""
    mig = _load_migration()
    assert mig.METERING_UNROLLED_GUARD_DDL == mg.METERING_UNROLLED_GUARD_DDL
    assert mig.METERING_UNROLLED_GUARD_FUNCTION_DDL == mg.METERING_UNROLLED_GUARD_FUNCTION_DDL
    assert (
        mig.ATTACH_METERING_GATE_TO_EXISTING_PARTITIONS_DDL
        == mg.ATTACH_METERING_GATE_TO_EXISTING_PARTITIONS_DDL
    )


def test_the_override_guc_is_a_third_distinct_guc() -> None:
    """Neither 0017's intent hatch nor 0019's COST watermark hatch.

    The incident's shape was ONE statement naming MANY tables. If metering
    shared 0019's GUC, an operator draining the cost ledger would silently
    acquire permission to destroy un-aggregated metering in the same statement.
    """
    assert ALLOW_UNROLLED_METERING_TRUNCATE_GUC != ALLOW_TRUNCATE_GUC
    assert ALLOW_UNROLLED_METERING_TRUNCATE_GUC != ALLOW_UNROLLED_TRUNCATE_GUC


def test_this_gate_does_not_reuse_0019s_cost_bound_function() -> None:
    """0019's function hardcodes billing_cost_daily and ignores TG_TABLE_NAME.

    Attaching it to the metering table would judge metering rows against the
    COST watermark: right-looking, and measuring the wrong table.
    """
    # 0019 READS the cost rollup; this one must not. Matched on the FROM clause,
    # not on a bare substring: this function's comments name billing_cost_daily
    # when they explain why it is not reused, and a substring test would have
    # been satisfied by that prose while the function still queried the table.
    assert "FROM public.billing_cost_daily" in ug.UNROLLED_GUARD_FUNCTION_DDL
    assert "FROM public.billing_cost_daily" not in mg.METERING_UNROLLED_GUARD_FUNCTION_DDL
    assert "FROM public.billing_metering_hourly" in mg.METERING_UNROLLED_GUARD_FUNCTION_DDL
    assert "FROM public.billing_metering_daily" in mg.METERING_UNROLLED_GUARD_FUNCTION_DDL


def test_0019s_docstring_no_longer_claims_the_gap_is_separately_tracked() -> None:
    """⚠️ CORRECTED: it was not tracked — 0 PRs, 0 issues, against 6 issues live.

    The retraction must leave a visible sign that a wrong version existed, so
    this asserts BOTH the absence of the false claim and the presence of the
    correction marker. Asserting only the absence would pass for a silent
    deletion, which is the failure mode being guarded against.
    """
    doc = ug.__doc__ or ""
    # The ORIGINAL sentence, in its original context. Not a bare
    # "Separately tracked." substring: the retraction quotes that phrase in
    # order to say it was wrong, so a substring test would go red on a correct
    # correction and green on a silent deletion — exactly backwards.
    assert "confidently wrong rather than absent. Separately tracked." not in doc
    assert "⚠️ CORRECTED 2026-09-08" in doc
    assert "IT\nWAS NOT TRACKED" in doc
    assert "metering_unrolled_guard" in doc


# ---------------------------------------------------------------------------
# Partitioning — a fact about today's schema, pinned so a change cannot be quiet
# ---------------------------------------------------------------------------


@pytest.mark.integration
# The module fixture runs the FULL chain from 0001; measured 1m50s on a private
# postgres 18.6. Fixture setup counts against the consuming test's timeout, and
# the file-level default is 60s.
@pytest.mark.timeout(300)
@pytest.mark.parametrize(
    ("relation", "expected_relkind", "expect_children"),
    [
        # THE SUBJECT. 0023 attaches the state gate to this relation only.
        ("billing_metering_events", "r", False),
        # ⭐ THE CONTROL, AND IT IS THE WHOLE REASON THIS TEST MOVED OFF
        # ``db_engine``. billing_cost_events IS range-partitioned — but only by
        # the migration chain, never by the model — so an oracle that cannot
        # report 'p' here is an oracle that cannot see partitioning at all, and
        # its green on the metering row means nothing. This case fails on the
        # exact schema the deleted version of this test passed against.
        ("billing_cost_events", "p", True),
    ],
)
def test_partitioning_is_read_from_the_alembic_built_schema(
    alembic_built_db: sa.engine.URL,
    relation: str,
    expected_relkind: str,
    expect_children: bool,
) -> None:
    """If the metering row ever fails, the gate needs partition attachment BEFORE the merge.

    Postgres does NOT clone statement-level TRUNCATE triggers to partitions: a
    raw ``CREATE TABLE ... PARTITION OF`` inherits none. ``billing_cost_events``
    is partitioned and that is exactly why 0019 had to extend the
    partition-minting function. ``billing_metering_events`` is an ordinary table
    today, so this control attaches to one relation — a simplification that is
    only valid while this assertion holds.

    ⚠️ **CORRECTED 2026-09-08 — the version of this test that shipped in the
    first push of this PR COULD NOT FAIL for that reason.** It read
    ``pg_class.relkind`` from the ``db_engine`` fixture, i.e. from the
    ``Base.metadata.create_all`` schema, and ``create_all`` never partitions
    anything. Measured on a private postgres 18.6, three schemas, same query:

    ==========================  =========================  ====================
    schema                      billing_cost_events        billing_metering_events
    ==========================  =========================  ====================
    ``create_all`` (db_engine)  ``r``  (0 children)        ``r``  (0 children)
    ``alembic upgrade head``    ``p``  (14 children)       ``r``  (0 children)
    production primary          ``p``  (12 children)       ``r``
    ==========================  =========================  ====================

    So the old oracle reported ``'r'`` about a table that is ``'p'`` in every
    schema that matters. It would have stayed green through exactly the change
    it claimed to catch: ``billing_cost_events`` got partitioned **in a
    migration, not in the model**, and a future migration partitioning
    ``billing_metering_events`` the same way would leave 0023 attached to the
    parent only, every child ungated, and the build green.

    The fix is not a stronger assertion, it is a schema the assertion can see:
    this runs the FULL chain from 0001 onto a throwaway database and reads the
    catalogs there. The cost row is carried permanently as a positive control —
    an oracle that cannot tell ``'p'`` from ``'r'`` reds on it immediately,
    rather than in whichever future PR happens to trip over it.
    """
    eng = sa.create_engine(alembic_built_db, future=True)
    try:
        with eng.connect() as conn:
            row = conn.execute(
                sa.text(
                    "SELECT c.relkind, "
                    "       (SELECT count(*) FROM pg_inherits i WHERE i.inhparent = c.oid) "
                    "  FROM pg_class c "
                    "  JOIN pg_namespace n ON n.oid = c.relnamespace "
                    " WHERE n.nspname = 'public' AND c.relname = :rel"
                ),
                {"rel": relation},
            ).first()
    finally:
        eng.dispose()

    assert row is not None, f"{relation} does not exist in the alembic-built schema"
    relkind, children = row

    assert relkind == expected_relkind, (
        f"{relation} relkind is {relkind!r}, expected {expected_relkind!r}. "
        "If this is billing_metering_events it is no longer an ordinary table: alembic "
        "0023 attaches the state gate to the parent only, statement-level TRUNCATE "
        "triggers are NOT inherited by partitions, so every child now needs "
        "billing_unrolled_metering_truncate_guard_trg attached explicitly and whatever "
        "mints the partitions must attach it too. If this is billing_cost_events the "
        "CONTROL has broken: this oracle can no longer distinguish a partitioned table "
        "from an ordinary one, so its verdict on the metering row is worthless."
    )
    assert (children > 0) is expect_children, (
        f"{relation} has {children} partitions, expected "
        f"{'at least one' if expect_children else 'none'}"
    )


@pytest.mark.integration
@pytest.mark.timeout(300)
def test_the_gate_trigger_is_attached_in_the_alembic_built_schema(
    alembic_built_db: sa.engine.URL,
) -> None:
    """The trigger, read from the schema a real deployment actually gets.

    ``test_the_gate_trigger_is_attached_by_catalog_query`` below asks the same
    question of the ``create_all`` schema. That is not redundant — it is the
    cheap one that runs in every unit pass — but it shares the blind spot the
    partitioning test above was corrected for, so the load-bearing copy is this
    one: it proves alembic 0023 itself attaches the trigger, on the same
    throwaway database the partitioning control is read from.
    """
    eng = sa.create_engine(alembic_built_db, future=True)
    try:
        with eng.connect() as conn:
            head = conn.execute(sa.text("SELECT version_num FROM alembic_version")).scalar()
            names = {
                r[0]
                for r in conn.execute(
                    sa.text(
                        "SELECT t.tgname FROM pg_trigger t "
                        "  JOIN pg_class c ON c.oid = t.tgrelid "
                        " WHERE NOT t.tgisinternal AND c.relname = 'billing_metering_events'"
                    )
                ).all()
            }
    finally:
        eng.dispose()

    # ⚠️ ANCESTOR-OR-SELF, not equality. This asserted `head == MIGRATION.stem`,
    # which pins the alembic HEAD to this one revision — so the very next
    # migration to land breaks a test about a trigger it does not touch. It broke
    # on 0024_billing_invoices within hours, and would have broken again on the
    # 0025 already queued behind it.
    #
    # The property this test is actually about is "0023 ran, and its trigger is
    # attached". That is true whether or not later revisions sit on top of it, so
    # the guard should be that 0023 is in the applied chain — which is what the
    # walk below checks. Equality on a value every future branch moves is the
    # same shared-scalar contention that per-file baselines exist to remove.
    chain: dict[str, str | None] = {}
    for path in MIGRATION.parent.glob("*.py"):
        text = path.read_text()
        rev = re.search(r'^revision:?\s*(?::\s*str\s*)?=\s*["\']([^"\']+)', text, re.M)
        down = re.search(r'^down_revision[^=]*=\s*["\']([^"\']+)', text, re.M)
        if rev:
            chain[rev.group(1)] = down.group(1) if down else None
    ancestry, cursor = [], head
    while cursor is not None and cursor in chain:
        ancestry.append(cursor)
        cursor = chain[cursor]
    assert MIGRATION.stem in ancestry, (
        f"{MIGRATION.stem!r} is not in the applied chain; `alembic upgrade head` "
        f"reached {head!r}, whose ancestry is {ancestry!r}"
    )
    assert mg.METERING_GATE_TRIGGER in names, (
        f"{mg.METERING_GATE_TRIGGER} is not attached after `alembic upgrade head`; "
        f"billing_metering_events carries {sorted(names)}"
    )
    # Additive: the intent gate from 0017/0018 must survive 0023.
    assert "billing_truncate_guard_trg" in names


@pytest.mark.asyncio
async def test_the_gate_trigger_is_attached_by_catalog_query(db_engine) -> None:
    """Proven from pg_trigger, not by reading the DDL that claims to attach it."""
    async with db_engine.begin() as conn:
        rows = (
            await conn.execute(
                sa.text(
                    "SELECT t.tgname, p.proname, t.tgtype "
                    "  FROM pg_trigger t "
                    "  JOIN pg_class c ON c.oid = t.tgrelid "
                    "  JOIN pg_proc p ON p.oid = t.tgfoid "
                    " WHERE NOT t.tgisinternal "
                    "   AND c.relname = 'billing_metering_events' "
                    " ORDER BY t.tgname"
                )
            )
        ).all()

    by_name = {r[0]: r[1] for r in rows}
    assert by_name.get(mg.METERING_GATE_TRIGGER) == "billing_forbid_unrolled_metering_truncate"
    # The intent gate (0017/0018) must still be there: this control is additive.
    assert by_name.get("billing_truncate_guard_trg") == "billing_forbid_truncate"
    # And the state gate must sort AFTER the intent gate, so an un-armed session
    # is refused by the cheap check before paying for two watermark reads.
    assert mg.METERING_GATE_TRIGGER > "billing_truncate_guard_trg"


# ---------------------------------------------------------------------------
# ARM 1 — a stage is BEHIND: refuse
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_an_armed_truncate_is_refused_while_the_hourly_stage_is_behind(
    db_engine,
) -> None:
    """0017's hatch is armed — the everyday, blessed path — and it is still refused."""
    hour = datetime(2026, 9, 5, 6, 0, tzinfo=UTC)
    async with db_engine.begin() as conn:
        await _reset(conn)
        await _seed(conn, "billing_metering_hourly", [_hourly_row(hour)])
        await _seed(conn, "billing_metering_daily", [_daily_row(date(2026, 9, 5))])
        # An event two hours after the newest rolled hour: hourly is behind.
        await _seed(
            conn, "billing_metering_events", [_metering_event_row(hour + timedelta(hours=2))]
        )
        await _arm_intent(conn)
        with pytest.raises(DBAPIError) as exc:
            await conn.exec_driver_sql("TRUNCATE billing_metering_events")

    assert "un-aggregated metering event" in str(exc.value)


@pytest.mark.asyncio
async def test_the_daily_watermark_alone_does_not_open_the_gate(db_engine) -> None:
    """⭐ THE COMPOUND PREDICATE, and the reason a port of 0019 would be wrong.

    ``rollup_daily`` builds a day's metering_daily rows from whatever
    ``billing_metering_hourly`` holds for that date — it does NOT require all 24
    hours. So ``max(bucket_date)`` can sit on a day whose hourly coverage is
    holed. Here daily says 09-05 is fully rolled while hourly stopped at 06:00,
    and raw events exist at 09:00. A daily-only gate — 0019's shape — passes.
    This one must refuse.
    """
    async with db_engine.begin() as conn:
        await _reset(conn)
        await _seed(
            conn,
            "billing_metering_hourly",
            [_hourly_row(datetime(2026, 9, 5, 6, 0, tzinfo=UTC))],
        )
        # Daily claims the WHOLE day is rolled.
        await _seed(conn, "billing_metering_daily", [_daily_row(date(2026, 9, 5))])
        await _seed(
            conn,
            "billing_metering_events",
            [_metering_event_row(datetime(2026, 9, 5, 9, 0, tzinfo=UTC))],
        )
        await _arm_intent(conn)
        with pytest.raises(DBAPIError) as exc:
            await conn.exec_driver_sql("TRUNCATE billing_metering_events")

    message = str(exc.value)
    assert "un-aggregated metering event" in message
    # The refusal must name the stage that actually bound, or an operator cannot
    # act on it: the daily watermark looks perfectly healthy in this state.
    assert "hourly frontier" in message


@pytest.mark.asyncio
async def test_the_hourly_watermark_alone_does_not_open_the_gate(db_engine) -> None:
    """The mirror case: hourly is ahead, daily has rolled nothing for that day.

    Rows folded into hourly are technically re-derivable — but the 2026-09-05
    statement named the raw table AND the rollup tables together, so hourly is a
    witness the same statement destroys. The EARLIER frontier binds.
    """
    async with db_engine.begin() as conn:
        await _reset(conn)
        await _seed(
            conn,
            "billing_metering_hourly",
            [_hourly_row(datetime(2026, 9, 5, 23, 0, tzinfo=UTC))],
        )
        # Daily has only reached the PREVIOUS day.
        await _seed(conn, "billing_metering_daily", [_daily_row(date(2026, 9, 4))])
        await _seed(
            conn,
            "billing_metering_events",
            [_metering_event_row(datetime(2026, 9, 5, 12, 0, tzinfo=UTC))],
        )
        await _arm_intent(conn)
        with pytest.raises(DBAPIError) as exc:
            await conn.exec_driver_sql("TRUNCATE billing_metering_events")

    assert "daily frontier" in str(exc.value)


@pytest.mark.asyncio
async def test_no_rows_are_lost_when_the_gate_refuses(db_engine) -> None:
    """A refusal must ABORT the statement, not partially truncate."""
    hour = datetime(2026, 9, 5, 6, 0, tzinfo=UTC)
    # Seeded in its OWN committed transaction. If the seeds shared the
    # transaction that the refusal aborts, the rollback would remove them and
    # this test would read 0 rows under BOTH a working gate and a gate that
    # truncated everything -- it would prove nothing while looking strict.
    async with db_engine.begin() as conn:
        await _reset(conn)
        await _seed(conn, "billing_metering_hourly", [_hourly_row(hour)])
        await _seed(conn, "billing_metering_daily", [_daily_row(date(2026, 9, 5))])
        await _seed(
            conn,
            "billing_metering_events",
            [_metering_event_row(hour + timedelta(hours=2)) for _ in range(3)],
        )

    with pytest.raises(DBAPIError):
        async with db_engine.begin() as conn:
            await _arm_intent(conn)
            await conn.exec_driver_sql("TRUNCATE billing_metering_events")

    async with db_engine.begin() as conn:
        surviving = (
            await conn.execute(sa.text("SELECT count(*) FROM billing_metering_events"))
        ).scalar()
    assert surviving == 3


@pytest.mark.asyncio
@pytest.mark.parametrize("empty_table", ["billing_metering_hourly", "billing_metering_daily"])
async def test_an_empty_rollup_stage_refuses(db_engine, empty_table: str) -> None:
    """FAIL CLOSED, per stage.

    ``least()`` ignores NULLs, so coalescing each frontier to ``-infinity``
    BEFORE ``least()`` is what makes an empty stage refuse instead of silently
    delegating the whole decision to the other one.
    """
    async with db_engine.begin() as conn:
        await _reset(conn)
        if empty_table != "billing_metering_hourly":
            await _seed(
                conn,
                "billing_metering_hourly",
                [_hourly_row(datetime(2026, 9, 5, 23, 0, tzinfo=UTC))],
            )
        if empty_table != "billing_metering_daily":
            await _seed(conn, "billing_metering_daily", [_daily_row(date(2026, 9, 5))])
        await _seed(
            conn,
            "billing_metering_events",
            [_metering_event_row(datetime(2026, 9, 5, 12, 0, tzinfo=UTC))],
        )
        await _arm_intent(conn)
        with pytest.raises(DBAPIError) as exc:
            await conn.exec_driver_sql("TRUNCATE billing_metering_events")

    assert "un-aggregated metering event" in str(exc.value)


@pytest.mark.asyncio
async def test_the_refusal_names_the_hatch_and_says_it_is_not_the_cost_one(
    db_engine,
) -> None:
    """An operator who is refused must be able to act without reading the source.

    ⛔ And must NOT be told to arm ``billing.allow_unrolled_truncate``: that is
    the COST ledger's hatch and it does nothing here. A hint that names the
    wrong GUC sends the operator to arm a control that will not move.
    """
    hour = datetime(2026, 9, 5, 6, 0, tzinfo=UTC)
    async with db_engine.begin() as conn:
        await _reset(conn)
        await _seed(conn, "billing_metering_hourly", [_hourly_row(hour)])
        await _seed(conn, "billing_metering_daily", [_daily_row(date(2026, 9, 5))])
        await _seed(
            conn, "billing_metering_events", [_metering_event_row(hour + timedelta(hours=2))]
        )
        await _arm_intent(conn)
        with pytest.raises(DBAPIError) as exc:
            await conn.exec_driver_sql("TRUNCATE billing_metering_events")

    message = str(exc.value)
    assert ALLOW_UNROLLED_METERING_TRUNCATE_GUC in message
    assert UNROLLED_METERING_OVERRIDE_OPERATION in message
    assert "2026-09-05" in message
    # ⛔ ABSENCE, asserted: the hint must not point at the cost ledger's hatch as
    # the thing to arm. It may only appear in the sentence that says it is a
    # DIFFERENT GUC, which the assertion above the next one pins.
    assert "SET LOCAL billing.allow_unrolled_truncate" not in message


@pytest.mark.asyncio
@pytest.mark.parametrize("value", ["true", "1", "yes", "ON", "", "off"])
async def test_only_the_exact_value_arms_the_override(db_engine, value: str) -> None:
    """A fat-fingered SET must not arm a control that deletes revenue."""
    hour = datetime(2026, 9, 5, 6, 0, tzinfo=UTC)
    async with db_engine.begin() as conn:
        await _reset(conn)
        await _seed(conn, "billing_metering_hourly", [_hourly_row(hour)])
        await _seed(conn, "billing_metering_daily", [_daily_row(date(2026, 9, 5))])
        await _seed(
            conn, "billing_metering_events", [_metering_event_row(hour + timedelta(hours=2))]
        )
        await _arm_intent(conn)
        await conn.exec_driver_sql(f"SET LOCAL {ALLOW_UNROLLED_METERING_TRUNCATE_GUC} = '{value}'")
        with pytest.raises(DBAPIError):
            await conn.exec_driver_sql("TRUNCATE billing_metering_events")


@pytest.mark.asyncio
async def test_the_cost_ledgers_override_does_not_arm_this_gate(db_engine) -> None:
    """⭐ THE WHOLE POINT OF A THIRD GUC, asserted directly.

    A session that armed 0019's cost-ledger override — the exact shape of an
    operator draining the cost ledger — must still be refused here.
    """
    hour = datetime(2026, 9, 5, 6, 0, tzinfo=UTC)
    async with db_engine.begin() as conn:
        await _reset(conn)
        await _seed(conn, "billing_metering_hourly", [_hourly_row(hour)])
        await _seed(conn, "billing_metering_daily", [_daily_row(date(2026, 9, 5))])
        await _seed(
            conn, "billing_metering_events", [_metering_event_row(hour + timedelta(hours=2))]
        )
        await _arm_intent(conn)
        await conn.exec_driver_sql(f"SET LOCAL {ALLOW_UNROLLED_TRUNCATE_GUC} = 'on'")
        with pytest.raises(DBAPIError):
            await conn.exec_driver_sql("TRUNCATE billing_metering_events")


# ---------------------------------------------------------------------------
# ARM 2 — both stages are CAUGHT UP: allow
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_an_armed_truncate_is_allowed_when_both_stages_have_caught_up(
    db_engine,
) -> None:
    """⭐ A gate that refuses everything is as broken as one that refuses nothing."""
    async with db_engine.begin() as conn:
        await _reset(conn)
        # Every raw row is at 09-05 06:xx; hourly covers 06:00 and daily covers
        # the whole of 09-05, so both frontiers are past every row.
        await _seed(
            conn,
            "billing_metering_hourly",
            [_hourly_row(datetime(2026, 9, 5, 6, 0, tzinfo=UTC))],
        )
        await _seed(conn, "billing_metering_daily", [_daily_row(date(2026, 9, 5))])
        await _seed(
            conn,
            "billing_metering_events",
            [_metering_event_row(datetime(2026, 9, 5, 6, 30, tzinfo=UTC))],
        )
        await _arm_intent(conn)
        await conn.exec_driver_sql("TRUNCATE billing_metering_events")
        remaining = (
            await conn.execute(sa.text("SELECT count(*) FROM billing_metering_events"))
        ).scalar()
    assert remaining == 0


@pytest.mark.asyncio
async def test_an_empty_metering_ledger_is_always_truncatable(db_engine) -> None:
    """No raw rows means nothing un-aggregated, whatever the watermarks say."""
    async with db_engine.begin() as conn:
        await _reset(conn)
        await _arm_intent(conn)
        await conn.exec_driver_sql("TRUNCATE billing_metering_events")


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("created_at", "refused"),
    [
        # hourly covers [06:00, 07:00). 06:59:59 is INSIDE it -> allowed.
        (datetime(2026, 9, 5, 6, 59, 59, tzinfo=UTC), False),
        # 07:00:00 is the first instant hourly does NOT cover -> refused.
        (datetime(2026, 9, 5, 7, 0, 0, tzinfo=UTC), True),
    ],
)
async def test_the_hourly_frontier_is_the_end_of_the_bucket_not_its_start(
    db_engine, created_at: datetime, refused: bool
) -> None:
    """⭐ OFF-BY-ONE on the cutoff, both sides.

    ``rollup_hourly`` covers ``[bucket_hour, bucket_hour + 1 hour)``, so the
    cutoff is ``max(bucket_hour) + 1 hour``. A gate that used ``max(bucket_hour)``
    itself would refuse every row in the newest ROLLED hour — permanently, on a
    perfectly healthy system.
    """
    async with db_engine.begin() as conn:
        await _reset(conn)
        await _seed(
            conn,
            "billing_metering_hourly",
            [_hourly_row(datetime(2026, 9, 5, 6, 0, tzinfo=UTC))],
        )
        # Daily is deliberately far ahead so ONLY the hourly frontier can bind.
        await _seed(conn, "billing_metering_daily", [_daily_row(date(2026, 9, 30))])
        await _seed(conn, "billing_metering_events", [_metering_event_row(created_at)])
        await _arm_intent(conn)
        if refused:
            with pytest.raises(DBAPIError):
                await conn.exec_driver_sql("TRUNCATE billing_metering_events")
        else:
            await conn.exec_driver_sql("TRUNCATE billing_metering_events")


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("created_at", "refused"),
    [
        # Daily covers the WHOLE of 09-05 in UTC. 23:59:59Z is inside it.
        (datetime(2026, 9, 5, 23, 59, 59, tzinfo=UTC), False),
        # 09-06 00:00:00Z is the first instant daily does not cover.
        (datetime(2026, 9, 6, 0, 0, 0, tzinfo=UTC), True),
    ],
)
async def test_the_daily_frontier_is_the_utc_day_boundary(
    db_engine, created_at: datetime, refused: bool
) -> None:
    """The other boundary, with the hourly frontier held far ahead so daily binds."""
    async with db_engine.begin() as conn:
        await _reset(conn)
        await _seed(
            conn,
            "billing_metering_hourly",
            [_hourly_row(datetime(2026, 9, 30, 0, 0, tzinfo=UTC))],
        )
        await _seed(conn, "billing_metering_daily", [_daily_row(date(2026, 9, 5))])
        await _seed(conn, "billing_metering_events", [_metering_event_row(created_at)])
        await _arm_intent(conn)
        if refused:
            with pytest.raises(DBAPIError):
                await conn.exec_driver_sql("TRUNCATE billing_metering_events")
        else:
            await conn.exec_driver_sql("TRUNCATE billing_metering_events")


# ---------------------------------------------------------------------------
# ARM 3 — the override: allowed, and audited BEFORE the rows go
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_the_override_allows_the_truncate_and_audits_it(db_engine) -> None:
    """The audit row must survive and must carry the numbers that are about to die."""
    hour = datetime(2026, 9, 5, 6, 0, tzinfo=UTC)
    rows = [
        _metering_event_row(hour + timedelta(hours=2)),
        _metering_event_row(hour + timedelta(hours=3)),
    ]
    async with db_engine.begin() as conn:
        await _reset(conn)
        await _seed(conn, "billing_metering_hourly", [_hourly_row(hour)])
        await _seed(conn, "billing_metering_daily", [_daily_row(date(2026, 9, 5))])
        await _seed(conn, "billing_metering_events", rows)
        await _arm_intent(conn)
        await _arm_metering_override(conn)
        await conn.exec_driver_sql("SET LOCAL billing.delete_reason = 'gate test drain'")
        await conn.exec_driver_sql("TRUNCATE billing_metering_events")

    async with db_engine.begin() as conn:
        audit = (
            await conn.execute(
                sa.text(
                    "SELECT table_name, rows_deleted, operation, reason, "
                    "       created_at_min, created_at_max "
                    "  FROM billing_delete_audit "
                    " WHERE operation = :op"
                ),
                {"op": UNROLLED_METERING_OVERRIDE_OPERATION},
            )
        ).all()
        remaining = (
            await conn.execute(sa.text("SELECT count(*) FROM billing_metering_events"))
        ).scalar()

    assert remaining == 0
    assert len(audit) == 1
    row = audit[0]
    assert row[0] == "billing_metering_events"
    # The UN-AGGREGATED count, not the table total — here they coincide, and the
    # boundary test above is what separates the two definitions.
    assert row[1] == 2
    assert "gate test drain" in row[3]
    assert row[4] is not None and row[5] is not None


@pytest.mark.asyncio
async def test_the_override_alone_does_not_bypass_0017s_intent_guard(db_engine) -> None:
    """The two controls COMPOSE. Arming state without intent is still refused."""
    hour = datetime(2026, 9, 5, 6, 0, tzinfo=UTC)
    async with db_engine.begin() as conn:
        await _reset(conn)
        await _seed(conn, "billing_metering_hourly", [_hourly_row(hour)])
        await _seed(conn, "billing_metering_daily", [_daily_row(date(2026, 9, 5))])
        await _seed(
            conn, "billing_metering_events", [_metering_event_row(hour + timedelta(hours=2))]
        )
        await _arm_metering_override(conn)  # state armed, intent NOT
        with pytest.raises(DBAPIError) as exc:
            await conn.exec_driver_sql("TRUNCATE billing_metering_events")

    # 0017's message, not this gate's — the cheap intent gate fires first.
    assert "is blocked" in str(exc.value)


@pytest.mark.asyncio
async def test_the_gate_does_not_touch_insert_or_delete(db_engine) -> None:
    """A BEFORE TRUNCATE trigger fires on TRUNCATE and nothing else.

    The ingest hot path must not be able to notice this control exists, even
    with both stages maximally behind.
    """
    async with db_engine.begin() as conn:
        await _reset(conn)
        # No rollup rows at all: the gate is in its most-refusing state.
        await _seed(
            conn,
            "billing_metering_events",
            [_metering_event_row(datetime(2026, 9, 5, 12, 0, tzinfo=UTC))],
        )
        inserted = (
            await conn.execute(sa.text("SELECT count(*) FROM billing_metering_events"))
        ).scalar()
        await conn.execute(sa.text("DELETE FROM billing_metering_events"))
        after = (
            await conn.execute(sa.text("SELECT count(*) FROM billing_metering_events"))
        ).scalar()
    assert inserted == 1
    assert after == 0


# ---------------------------------------------------------------------------
# The function refuses to answer for a table it was not written for
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_the_function_refuses_to_answer_for_another_table(db_engine) -> None:
    """The defect that forced a second function is trivially repeatable by hand.

    Anyone can attach this trigger to another relation, and 0019's function
    demonstrates exactly how quiet that failure is. So attaching this one
    elsewhere must RAISE about the misattachment rather than return a confident
    verdict derived from the metering watermarks.
    """
    async with db_engine.begin() as conn:
        await _reset(conn)
        await conn.exec_driver_sql(
            "CREATE TABLE IF NOT EXISTS _gate_misattach (created_at timestamptz)"
        )
        await conn.exec_driver_sql(
            "CREATE TRIGGER _gate_misattach_trg BEFORE TRUNCATE ON _gate_misattach "
            "FOR EACH STATEMENT "
            "EXECUTE FUNCTION public.billing_forbid_unrolled_metering_truncate()"
        )
        with pytest.raises(DBAPIError) as exc:
            await conn.exec_driver_sql("TRUNCATE _gate_misattach")

    message = str(exc.value)
    assert "is not billing_metering_events" in message
    # ⛔ ABSENCE, asserted: it must NOT have produced the ordinary un-aggregated
    # verdict, which is what a function that ignored TG_TABLE_NAME would emit.
    assert "un-aggregated metering event" not in message


# ---------------------------------------------------------------------------
# The lag metric — the gate refuses silently without it
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_metering_hourly_lag_is_measured_and_healthy_is_one(db_engine) -> None:
    """⭐ Without this, a metering stall is as invisible as the cost-side 3-day lag.

    ``billing_rollup_lag_days`` covers the DAILY tables only. Nothing measured
    the hourly stage — the first of metering's two — and that is the stage this
    gate binds on.
    """
    from sqlalchemy.ext.asyncio import async_sessionmaker

    from services.rollup_engine import metering_hourly_lag_hours

    factory = async_sessionmaker(db_engine, expire_on_commit=False)
    now = datetime(2026, 9, 5, 12, 30, tzinfo=UTC)

    async with db_engine.begin() as conn:
        await _reset(conn)
    # Empty table publishes no sample and reports None -- there is no head to be
    # behind. Asserted rather than assumed: a sentinel here would be readable as
    # a real lag.
    assert await metering_hourly_lag_hours(factory, now=now) is None

    async with db_engine.begin() as conn:
        await _seed(
            conn,
            "billing_metering_hourly",
            [_hourly_row(datetime(2026, 9, 5, 11, 0, tzinfo=UTC))],
        )
    assert await metering_hourly_lag_hours(factory, now=now) == 1

    async with db_engine.begin() as conn:
        await conn.execute(sa.text("DELETE FROM billing_metering_hourly"))
        await _seed(
            conn,
            "billing_metering_hourly",
            [_hourly_row(datetime(2026, 9, 5, 8, 0, tzinfo=UTC))],
        )
    assert await metering_hourly_lag_hours(factory, now=now) == 4


@pytest.mark.asyncio
async def test_the_lag_value_and_its_freshness_timestamp_are_written_together(db_engine) -> None:
    """⛔ Without the timestamp there is NO reading of this gauge that means "unknown".

    ``billing_metering_hourly_lag_hours`` is an UNLABELLED prometheus_client
    Gauge, and those are exported from the moment they are constructed —
    measured: a fresh unlabelled gauge appears as ``probe_unlabelled 0.0`` in
    ``generate_latest`` before any ``.set()``, while a labelled one appears not
    at all. Two consequences, both of which this test pins:

    * ``absent()`` can NEVER fire on this series, so the advice that shipped in
      the first push of this PR ("cover an empty table with absent()") could not
      have worked. That advice is corrected in place beside this gauge.
    * a replica that has never measured reads ``0``, which is LOWER than the
      healthy ``1`` — the un-measured state renders as better-than-healthy.

    So the pair is the instrument, not the lag alone: the timestamp advances
    **only** when a sample was actually published, which is what makes a frozen
    value legible as frozen. Prior art:
    ``billing_attribution_audit_last_success_timestamp_seconds``.
    """
    from sqlalchemy.ext.asyncio import async_sessionmaker

    import observability.metrics as om
    from services.rollup_engine import metering_hourly_lag_hours

    factory = async_sessionmaker(db_engine, expire_on_commit=False)
    now = datetime(2026, 9, 5, 12, 30, tzinfo=UTC)

    def _stamp() -> float:
        return om.metering_hourly_lag_timestamp_seconds._value.get()

    def _lag() -> float:
        return om.metering_hourly_lag_hours._value.get()

    # ARM 1 — NOTHING TO MEASURE. Empty table: no sample, so the timestamp must
    # NOT move. A timestamp that advanced here would bless a lag value written
    # minutes ago about a table that no longer has a head at all.
    async with db_engine.begin() as conn:
        await _reset(conn)
    om.metering_hourly_lag_timestamp_seconds.set(0)
    om.metering_hourly_lag_hours.set(-1)
    assert await metering_hourly_lag_hours(factory, now=now) is None
    assert _stamp() == 0, "the freshness timestamp advanced on a pass that published no sample"
    assert _lag() == -1, "a sample was published for an empty table"

    # ARM 2 — MEASURED. Value and timestamp both written, timestamp after it.
    async with db_engine.begin() as conn:
        await _seed(
            conn,
            "billing_metering_hourly",
            [_hourly_row(datetime(2026, 9, 5, 8, 0, tzinfo=UTC))],
        )
    assert await metering_hourly_lag_hours(factory, now=now) == 4
    assert _lag() == 4
    assert _stamp() > 0, (
        "a lag sample was published with no freshness timestamp beside it — nothing "
        "downstream can then tell a maintained 4 from a 4 frozen at the moment the "
        "rollup loop died"
    )
