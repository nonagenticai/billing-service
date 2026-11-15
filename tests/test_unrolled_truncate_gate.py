"""The rollup-watermark gate on the cost ledger (alembic 0019).

THE DEFECT
----------
``billing_cost_daily`` holds zero rows for 2026-09-02, 09-03 and 09-04. The
rollup ran every one of those days and never reached those buckets — UUIDv7 ids
decode write times and show one bucket written per run, permanently ~3 days
behind. At 2026-09-05T07:01:56Z a TRUNCATE emptied ``billing_cost_events`` while
the last rolled bucket was 09-01, deleting exactly the un-rolled backlog.
``min(created_at)`` is now 09-05T07:01:58Z — two seconds after. Nothing is
recoverable.

0017's guard asks "did you MEAN to truncate?". That question was never the one
that mattered here: an ARMED truncate — the blessed, everyday path — destroys
un-aggregated revenue just as completely. This suite pins the missing question:
"has the rollup caught up?"

BOTH ARMS ARE ASSERTED, DELIBERATELY
------------------------------------
A gate that refuses everything is as broken as one that refuses nothing: it
would simply move the outage from the ledger to the operator. So the
caught-up arm (``..._is_allowed_when_the_watermark_has_caught_up``) and the
day-boundary arm (``..._on_the_watermark_day_itself_is_not_un_rolled``) are
first-class tests here, not afterthoughts — removing the ``IF NOT v_unrolled
THEN RETURN NULL`` early-out must go red just as loudly as removing the
``RAISE EXCEPTION`` does.
"""

from __future__ import annotations

import importlib.util
import uuid
from datetime import UTC, date, datetime
from pathlib import Path

import pytest
import sqlalchemy as sa
from sqlalchemy.exc import DBAPIError

import models.db.truncate_guard as tg
import models.db.unrolled_guard as ug
from db import Base
from models.db import (
    ALLOW_TRUNCATE_GUC,
    ALLOW_TRUNCATE_VALUE,
    ALLOW_UNROLLED_TRUNCATE_GUC,
    ALLOW_UNROLLED_TRUNCATE_VALUE,
    NULL_LICENSE_ID,
    UNROLLED_OVERRIDE_OPERATION,
)

MIGRATION = Path(__file__).parent.parent / "alembic" / "versions" / "0019_unrolled_truncate_gate.py"


def _load_migration():  # noqa: ANN202
    spec = importlib.util.spec_from_file_location("_mig_0019", MIGRATION)
    assert spec is not None and spec.loader is not None, f"cannot load {MIGRATION}"
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def _cost_event_row(tenant: str, created_at: datetime) -> dict:
    return {
        "id": str(uuid.uuid4()),
        "tenant_id": tenant,
        "tenant_name": tenant,
        "service_key": "gate-test",  # pragma: allowlist secret
        "provider": "test",
        "input_tokens": 1,
        "output_tokens": 1,
        "created_at": created_at,
    }


def _cost_daily_row(tenant: str, bucket_date: date) -> dict:
    return {
        "id": str(uuid.uuid4()),
        "tenant_id": tenant,
        "tenant_name": tenant,
        "license_id": str(NULL_LICENSE_ID),
        "service_key": "gate-test",  # pragma: allowlist secret
        "provider": "test",
        "bucket_date": bucket_date,
        "total_input_tokens": 1,
        "total_output_tokens": 1,
        "total_cost_usd": 1,
        "request_count": 1,
    }


async def _seed(conn, table: str, rows: list[dict]) -> None:
    cols = ", ".join(rows[0])
    binds = ", ".join(f":{c}" for c in rows[0])
    await conn.execute(sa.text(f"INSERT INTO {table} ({cols}) VALUES ({binds})"), rows)


async def _arm_intent(conn) -> None:
    """0017's hatch only: 'I mean to truncate'."""
    await conn.exec_driver_sql(f"SET LOCAL {ALLOW_TRUNCATE_GUC} = '{ALLOW_TRUNCATE_VALUE}'")


async def _arm_override(conn) -> None:
    """0019's hatch: 'I mean to delete un-aggregated revenue'."""
    await conn.exec_driver_sql(
        f"SET LOCAL {ALLOW_UNROLLED_TRUNCATE_GUC} = '{ALLOW_UNROLLED_TRUNCATE_VALUE}'"
    )


async def _reset(conn) -> None:
    """Empty both tables from a KNOWN state, past both gates.

    Not ``db_engine``'s own truncate: that arms the override, so it is the very
    behaviour under test and cannot also be the fixture that sets up for it.
    A plain DELETE fires no TRUNCATE trigger at all.
    """
    await conn.execute(sa.text("DELETE FROM billing_cost_events"))
    await conn.execute(sa.text("DELETE FROM billing_cost_daily"))
    await conn.execute(sa.text("DELETE FROM billing_delete_audit"))


# ---------------------------------------------------------------------------
# The two copies of the DDL cannot drift
# ---------------------------------------------------------------------------


def test_migration_ddl_is_byte_identical_to_the_module() -> None:
    """0012 and 0015 exist only because this repo let two schema sources drift.

    The migration freezes a copy rather than importing, which is correct and is
    exactly why it needs a test.
    """
    mig = _load_migration()
    assert mig.UNROLLED_GUARD_DDL == ug.UNROLLED_GUARD_DDL
    assert mig.UNROLLED_GUARD_FUNCTION_DDL == ug.UNROLLED_GUARD_FUNCTION_DDL
    assert mig.PARTITION_FN_WITH_GATE_DDL == ug.PARTITION_FN_WITH_GATE_DDL


def test_downgrade_restores_0017s_partition_function_exactly() -> None:
    """Reverting THIS control must leave 0016's audit and 0017's guard intact.

    ``create_cost_events_partition_for`` is one function carrying three controls.
    A downgrade that wrote an invented body would silently stop future partitions
    inheriting the audit or the intent guard — a hole in a different control,
    opened by reverting this one, visible only months later on the next mint.
    """
    mig = _load_migration()
    assert mig._FN_WITH_GUARD_ONLY == tg.PARTITION_FN_WITH_GUARD_DDL
    assert "billing_truncate_guard_trg" in mig._FN_WITH_GUARD_ONLY
    assert "billing_delete_audit_trg" in mig._FN_WITH_GUARD_ONLY
    assert "billing_unrolled_truncate_guard_trg" not in mig._FN_WITH_GUARD_ONLY


def test_the_gated_partition_function_keeps_all_three_controls() -> None:
    """Derived from 0017's body, which is derived from 0016's — nothing may be lost."""
    fn = ug.PARTITION_FN_WITH_GATE_DDL
    assert "billing_unrolled_truncate_guard_trg" in fn, "watermark gate missing"
    assert "billing_truncate_guard_trg" in fn, "0017 intent guard was lost"
    assert "billing_delete_audit_trg" in fn, "0016 DELETE audit was lost"
    assert "billing_truncate_audit_trg" in fn, "0016 TRUNCATE audit was lost"


def test_the_override_guc_is_not_the_ordinary_truncate_guc() -> None:
    """The whole design: a blessed truncate must not inherit permission to lose money.

    ``tests/conftest.py`` arms ``billing.allow_truncate`` thousands of times a
    day. If the override shared that GUC the gate would be disarmed everywhere it
    matters, and this file would be a very expensive way to test nothing.
    """
    assert ALLOW_UNROLLED_TRUNCATE_GUC != ALLOW_TRUNCATE_GUC


# ---------------------------------------------------------------------------
# ARM 1 — the watermark is BEHIND: refuse
# ---------------------------------------------------------------------------


async def test_an_armed_truncate_is_refused_while_the_rollup_is_behind(db_engine) -> None:
    """THE INCIDENT, reproduced: watermark 09-01, raw events on 09-02..09-04."""
    tenant = str(uuid.uuid4())
    async with db_engine.begin() as conn:
        await _reset(conn)
        await _seed(conn, "billing_cost_daily", [_cost_daily_row(tenant, date(2026, 9, 1))])
        await _seed(
            conn,
            "billing_cost_events",
            [_cost_event_row(tenant, datetime(2026, 9, d, 12, 0, tzinfo=UTC)) for d in (2, 3, 4)],
        )

    with pytest.raises(DBAPIError) as exc:
        async with db_engine.begin() as conn:
            await _arm_intent(conn)
            await conn.execute(sa.text("TRUNCATE billing_cost_events"))

    message = str(exc.value)
    assert "un-aggregated" in message
    assert "3" in message, message


async def test_no_rows_are_lost_when_the_gate_refuses(db_engine) -> None:
    """A refusal that still emptied the table would be theatre."""
    tenant = str(uuid.uuid4())
    async with db_engine.begin() as conn:
        await _reset(conn)
        await _seed(conn, "billing_cost_daily", [_cost_daily_row(tenant, date(2026, 9, 1))])
        await _seed(
            conn, "billing_cost_events", [_cost_event_row(tenant, datetime(2026, 9, 3, tzinfo=UTC))]
        )

    with pytest.raises(DBAPIError):
        async with db_engine.begin() as conn:
            await _arm_intent(conn)
            await conn.execute(sa.text("TRUNCATE billing_cost_events"))

    async with db_engine.begin() as conn:
        remaining = (
            await conn.execute(sa.text("SELECT count(*) FROM billing_cost_events"))
        ).scalar()
    assert remaining == 1


async def test_the_refusal_names_the_escape_hatch_and_the_watermark(db_engine) -> None:
    """An operator who cannot find the hatch will reach for a bigger hammer."""
    tenant = str(uuid.uuid4())
    async with db_engine.begin() as conn:
        await _reset(conn)
        await _seed(conn, "billing_cost_daily", [_cost_daily_row(tenant, date(2026, 9, 1))])
        await _seed(
            conn, "billing_cost_events", [_cost_event_row(tenant, datetime(2026, 9, 3, tzinfo=UTC))]
        )

    with pytest.raises(DBAPIError) as exc:
        async with db_engine.begin() as conn:
            await _arm_intent(conn)
            await conn.execute(sa.text("TRUNCATE billing_cost_events"))

    message = str(exc.value)
    assert ALLOW_UNROLLED_TRUNCATE_GUC in message
    assert "2026-09-01" in message, "the refusal does not say what the watermark is"
    assert "nothing was recoverable" in message.lower()


async def test_an_empty_rollup_table_refuses(db_engine) -> None:
    """FAIL CLOSED. No bucket has ever been written -> every raw row is un-aggregated.

    An unreadable watermark is not evidence that deletion is safe; it is the
    situation in which deletion is most obviously unsafe.
    """
    tenant = str(uuid.uuid4())
    async with db_engine.begin() as conn:
        await _reset(conn)
        await _seed(conn, "billing_cost_events", [_cost_event_row(tenant, datetime.now(UTC))])

    with pytest.raises(DBAPIError) as exc:
        async with db_engine.begin() as conn:
            await _arm_intent(conn)
            await conn.execute(sa.text("TRUNCATE billing_cost_events"))
    assert "no bucket has ever been written" in str(exc.value)


@pytest.mark.parametrize("value", ["", "true", "1", "yes", "ON", "off"])
async def test_only_the_exact_value_arms_the_override(db_engine, value: str) -> None:
    """One exact spelling, so a fat-fingered SET cannot disarm the gate."""
    tenant = str(uuid.uuid4())
    async with db_engine.begin() as conn:
        await _reset(conn)
        await _seed(conn, "billing_cost_daily", [_cost_daily_row(tenant, date(2026, 9, 1))])
        await _seed(
            conn, "billing_cost_events", [_cost_event_row(tenant, datetime(2026, 9, 3, tzinfo=UTC))]
        )

    with pytest.raises(DBAPIError):
        async with db_engine.begin() as conn:
            await _arm_intent(conn)
            await conn.exec_driver_sql(f"SET LOCAL {ALLOW_UNROLLED_TRUNCATE_GUC} = '{value}'")
            await conn.execute(sa.text("TRUNCATE billing_cost_events"))


# ---------------------------------------------------------------------------
# ARM 2 — the watermark has CAUGHT UP: allow
# ---------------------------------------------------------------------------


async def test_an_armed_truncate_is_allowed_when_the_watermark_has_caught_up(db_engine) -> None:
    """⭐ THE OTHER ARM. A gate that refuses everything is as broken as one that
    refuses nothing — it just relocates the outage.

    Same rows, same statement, same hatch as the refusal above; the ONLY
    difference is that ``billing_cost_daily`` covers the newest event's day.
    """
    tenant = str(uuid.uuid4())
    day = datetime(2026, 9, 3, 12, 0, tzinfo=UTC)
    async with db_engine.begin() as conn:
        await _reset(conn)
        await _seed(conn, "billing_cost_daily", [_cost_daily_row(tenant, date(2026, 9, 3))])
        await _seed(conn, "billing_cost_events", [_cost_event_row(tenant, day)])

    async with db_engine.begin() as conn:
        await _arm_intent(conn)
        await conn.execute(sa.text("TRUNCATE billing_cost_events"))
        remaining = (
            await conn.execute(sa.text("SELECT count(*) FROM billing_cost_events"))
        ).scalar()
        audited = list(
            (
                await conn.execute(
                    sa.text("SELECT operation FROM billing_delete_audit ORDER BY id")
                )
            ).scalars()
        )

    assert remaining == 0, "the gate refused a truncate whose backlog was fully rolled up"
    # 0016's AFTER TRUNCATE audit still fires, and NO override row is written:
    # nothing was overridden.
    assert audited == ["TRUNCATE"], audited


async def test_an_empty_ledger_is_always_truncatable(db_engine) -> None:
    """No rows -> nothing un-aggregated -> no objection, whatever the watermark says.

    The degenerate caught-up case, and the one the test harness itself relies on.
    """
    async with db_engine.begin() as conn:
        await _reset(conn)

    async with db_engine.begin() as conn:
        await _arm_intent(conn)
        await conn.execute(sa.text("TRUNCATE billing_cost_events"))


@pytest.mark.parametrize(
    ("event_at", "refused"),
    [
        # Watermark is 2026-09-01. The last instant OF that day is rolled up...
        (datetime(2026, 9, 1, 23, 59, 59, tzinfo=UTC), False),
        # ...the first instant of the NEXT day is not.
        (datetime(2026, 9, 2, 0, 0, 0, tzinfo=UTC), True),
    ],
)
async def test_the_day_boundary_is_the_utc_day_boundary(
    db_engine, event_at: datetime, refused: bool
) -> None:
    """The gate's cutoff must be the same UTC boundary ``rollup_daily`` buckets on.

    ``rollup_engine`` assigns ``bucket_date`` with
    ``cast(timezone('UTC', created_at) as date)``, deliberately UTC-explicit. The
    gate's sargable rewrite (``created_at >= (watermark + 1) AT TIME ZONE 'UTC'``)
    must agree with it to the second, or it either refuses truncates of fully
    rolled-up data or passes un-aggregated rows through. Both failures are silent.
    """
    tenant = str(uuid.uuid4())
    async with db_engine.begin() as conn:
        await _reset(conn)
        await _seed(conn, "billing_cost_daily", [_cost_daily_row(tenant, date(2026, 9, 1))])
        await _seed(conn, "billing_cost_events", [_cost_event_row(tenant, event_at)])

    async def _truncate() -> None:
        async with db_engine.begin() as conn:
            await _arm_intent(conn)
            await conn.execute(sa.text("TRUNCATE billing_cost_events"))

    if refused:
        with pytest.raises(DBAPIError):
            await _truncate()
    else:
        await _truncate()
        async with db_engine.begin() as conn:
            remaining = (
                await conn.execute(sa.text("SELECT count(*) FROM billing_cost_events"))
            ).scalar()
        assert remaining == 0


# ---------------------------------------------------------------------------
# ARM 3 — the override: possible, explicit, audited
# ---------------------------------------------------------------------------


async def test_the_override_allows_the_truncate_and_audits_it(db_engine) -> None:
    """A legitimate operator truncate stays possible — and leaves a record.

    This is the row 2026-09-05 did not have. Reconstructing that loss took
    decoding UUIDv7 ids out of a surviving rollup table.
    """
    tenant = str(uuid.uuid4())
    async with db_engine.begin() as conn:
        await _reset(conn)
        await _seed(conn, "billing_cost_daily", [_cost_daily_row(tenant, date(2026, 9, 1))])
        await _seed(
            conn,
            "billing_cost_events",
            [_cost_event_row(tenant, datetime(2026, 9, d, 12, 0, tzinfo=UTC)) for d in (2, 3, 4)],
        )

    async with db_engine.begin() as conn:
        await _arm_intent(conn)
        await _arm_override(conn)
        await conn.exec_driver_sql(
            "SET LOCAL billing.delete_reason = 'ledger migration, ticket 4711'"
        )
        await conn.execute(sa.text("TRUNCATE billing_cost_events"))
        rows = list(
            (
                await conn.execute(
                    sa.text(
                        "SELECT operation, table_name, rows_deleted, reason, "
                        "created_at_min, created_at_max FROM billing_delete_audit "
                        "WHERE operation = :op"
                    ),
                    {"op": UNROLLED_OVERRIDE_OPERATION},
                )
            ).mappings()
        )
        remaining = (
            await conn.execute(sa.text("SELECT count(*) FROM billing_cost_events"))
        ).scalar()

    assert remaining == 0, "the override did not actually allow the truncate"
    assert len(rows) == 1, rows
    row = rows[0]
    assert row["table_name"] == "billing_cost_events"
    assert row["rows_deleted"] == 3, "the audit row must count the UN-AGGREGATED rows"
    assert "ledger migration, ticket 4711" in row["reason"]
    assert "2026-09-01" in row["reason"], "the audit row must name the watermark"
    assert row["created_at_min"].astimezone(UTC) == datetime(2026, 9, 2, 12, 0, tzinfo=UTC)
    assert row["created_at_max"].astimezone(UTC) == datetime(2026, 9, 4, 12, 0, tzinfo=UTC)


async def test_the_override_alone_does_not_bypass_0017s_intent_guard(db_engine) -> None:
    """The two controls stack; neither subsumes the other."""
    tenant = str(uuid.uuid4())
    async with db_engine.begin() as conn:
        await _reset(conn)
        await _seed(conn, "billing_cost_events", [_cost_event_row(tenant, datetime.now(UTC))])

    with pytest.raises(DBAPIError) as exc:
        async with db_engine.begin() as conn:
            await _arm_override(conn)  # but NOT billing.allow_truncate
            await conn.execute(sa.text("TRUNCATE billing_cost_events"))
    assert "blocked" in str(exc.value).lower(), "0017's guard should have refused first"


async def test_the_gate_does_not_touch_insert_or_delete(db_engine) -> None:
    """A TRUNCATE trigger fires on TRUNCATE and nothing else.

    Asserted rather than assumed: this lands on the table the ingest hot path
    writes to, and 'it only affects TRUNCATE' is the claim that makes it safe to
    apply to a live billing primary.
    """
    tenant = str(uuid.uuid4())
    async with db_engine.begin() as conn:
        await _reset(conn)
        # No billing_cost_daily row at all — the maximally un-rolled state.
        await _seed(conn, "billing_cost_events", [_cost_event_row(tenant, datetime.now(UTC))])
        n = (await conn.execute(sa.text("SELECT count(*) FROM billing_cost_events"))).scalar()
        assert n == 1
        await conn.execute(
            sa.text("DELETE FROM billing_cost_events WHERE tenant_id = :t"), {"t": tenant}
        )
        n = (await conn.execute(sa.text("SELECT count(*) FROM billing_cost_events"))).scalar()
        assert n == 0


# ---------------------------------------------------------------------------
# Partitions — where the incident actually landed
# ---------------------------------------------------------------------------


@pytest.fixture
async def partitioned_cost_events(db_engine):  # noqa: ANN201
    """Rebuild ``billing_cost_events`` as a real partitioned table, armed as 0019 arms it.

    The ORM builds a PLAIN table, so without this the partition assertions would
    be vacuous — passing against a table that has no partitions to be blind to.
    Same shape as ``tests/test_truncate_guard.py``'s fixture of the same name,
    with the third control applied in the order ``models/db/__init__`` registers
    the listeners: audit (0016), intent guard (0017), watermark gate (0019).
    """
    import models.db.delete_audit as da

    async def _apply(conn, statements) -> None:
        for stmt in statements:
            await conn.exec_driver_sql(stmt.replace("%", "%%"))

    async with db_engine.begin() as conn:
        await conn.exec_driver_sql(
            "ALTER TABLE billing_cost_events RENAME TO billing_cost_events_orm_shape"
        )
        await conn.exec_driver_sql(
            "CREATE TABLE billing_cost_events "
            "(LIKE billing_cost_events_orm_shape INCLUDING DEFAULTS) "
            "PARTITION BY RANGE (created_at)"
        )
        await conn.exec_driver_sql(
            "ALTER TABLE billing_cost_events ADD PRIMARY KEY (id, created_at)"
        )
        await conn.exec_driver_sql("DROP TABLE billing_cost_events_orm_shape CASCADE")
        await _apply(conn, da.DELETE_AUDIT_DDL)
        await _apply(conn, tg.TRUNCATE_GUARD_DDL)
        await _apply(conn, ug.UNROLLED_GUARD_DDL)

    yield db_engine

    async with db_engine.begin() as conn:
        await conn.exec_driver_sql("DROP TABLE IF EXISTS billing_cost_events CASCADE")
        await conn.run_sync(Base.metadata.tables["billing_cost_events"].create)


async def test_a_truncate_aimed_at_a_child_partition_is_gated(
    partitioned_cost_events,
) -> None:
    """THE INCIDENT'S ACTUAL SHAPE: the statement named all 12 children.

    A statement trigger on the parent does not fire for a TRUNCATE naming a
    child, so a parent-only attachment would be blind to precisely the thing this
    control exists for. Drop ``ATTACH_GATE_TO_EXISTING_PARTITIONS_DDL`` and this
    goes red while every parent-only test above stays green.
    """
    engine = partitioned_cost_events
    tenant = str(uuid.uuid4())
    async with engine.begin() as conn:
        await conn.execute(sa.text("DELETE FROM billing_cost_daily"))
        await _seed(conn, "billing_cost_daily", [_cost_daily_row(tenant, date(2026, 9, 1))])
        await conn.execute(sa.text("SELECT create_cost_events_partition_for(2026, 9)"))
        await _seed(
            conn,
            "billing_cost_events",
            [_cost_event_row(tenant, datetime(2026, 9, 3, 12, 0, tzinfo=UTC))],
        )

    with pytest.raises(DBAPIError) as exc:
        async with engine.begin() as conn:
            await _arm_intent(conn)
            await conn.execute(sa.text("TRUNCATE billing_cost_events_2026_09"))
    assert "un-aggregated" in str(exc.value)


async def test_a_freshly_minted_partition_carries_all_three_controls(
    partitioned_cost_events,
) -> None:
    """A partition minted NEXT MONTH must be audited, guarded AND gated.

    This also pins the import ordering in ``models/db/__init__``: this module's
    function body is derived from truncate_guard's, which is derived from
    delete_audit's, so if the listeners ran in another order one of these three
    would be missing here.
    """
    engine = partitioned_cost_events
    async with engine.begin() as conn:
        minted = (
            await conn.execute(sa.text("SELECT create_cost_events_partition_for(2031, 4)"))
        ).scalar()
        triggers = sorted(
            r[0]
            for r in (
                await conn.execute(
                    sa.text(
                        "SELECT t.tgname FROM pg_trigger t JOIN pg_class c ON c.oid = t.tgrelid "
                        "WHERE c.relname = :p AND NOT t.tgisinternal"
                    ),
                    {"p": minted},
                )
            ).all()
        )

    assert minted == "billing_cost_events_2031_04"
    assert triggers == [
        "billing_delete_audit_trg",
        "billing_truncate_audit_trg",
        "billing_truncate_guard_trg",
        "billing_unrolled_truncate_guard_trg",
    ], f"minted partition {minted} carries {triggers}"


async def test_a_child_truncate_is_judged_on_that_childs_own_rows(
    partitioned_cost_events,
) -> None:
    """⭐ The caught-up arm, at partition granularity.

    A child holding only fully-rolled-up rows is truncatable even while a SIBLING
    holds backlog. Judging every child by the parent's contents would refuse
    partition retention work forever — the gate would then be the outage.
    """
    engine = partitioned_cost_events
    tenant = str(uuid.uuid4())
    async with engine.begin() as conn:
        await conn.execute(sa.text("DELETE FROM billing_cost_daily"))
        # Watermark covers August but not September.
        await _seed(conn, "billing_cost_daily", [_cost_daily_row(tenant, date(2026, 8, 31))])
        await conn.execute(sa.text("SELECT create_cost_events_partition_for(2026, 8)"))
        await conn.execute(sa.text("SELECT create_cost_events_partition_for(2026, 9)"))
        await _seed(
            conn,
            "billing_cost_events",
            [
                _cost_event_row(tenant, datetime(2026, 8, 15, 12, 0, tzinfo=UTC)),
                _cost_event_row(tenant, datetime(2026, 9, 3, 12, 0, tzinfo=UTC)),
            ],
        )

    # The August child is fully rolled up -> allowed.
    async with engine.begin() as conn:
        await _arm_intent(conn)
        await conn.execute(sa.text("TRUNCATE billing_cost_events_2026_08"))

    # The September sibling still holds backlog -> refused.
    with pytest.raises(DBAPIError):
        async with engine.begin() as conn:
            await _arm_intent(conn)
            await conn.execute(sa.text("TRUNCATE billing_cost_events_2026_09"))

    async with engine.begin() as conn:
        surviving = (
            await conn.execute(sa.text("SELECT count(*) FROM billing_cost_events"))
        ).scalar()
    assert surviving == 1, "the un-aggregated September row must still be there"
