"""The DELETE-audit control for issue #92, exercised against a real database.

22,438 nil-tenant financial rows left ``billing_cost_events`` with no record of
it. ``pg_stat_user_tables.n_tup_del`` per partition was the only witness — a
counter, which cannot say which rows, whose tenant, by whom, or under what
statement, and ``log_statement=ddl`` logs no DML.

These tests assert the record EXISTS AND CARRIES THE FORENSICS. A test that only
checked "a row appeared" would pass against an audit table full of NULLs, which
is the same blindness in a new location.

⚠️ THE PARTITION HALF NEEDS ITS OWN SCHEMA, AND THIS FILE BUILDS ONE.
``CostEvent`` has no ``postgresql_partition_by``, so the suite's ``create_all``
schema gives ``billing_cost_events`` as a PLAIN table. Against that table the
child loop in ``ATTACH_TO_EXISTING_PARTITIONS_DDL`` iterates zero rows and
``PARTITION_FN_WITH_AUDIT_DDL`` is never invoked — both could be deleted from
the module and the migration together and every test that only touches the
parent would still pass. That is precisely the half the design leans on: a
parent-only attachment is blind to a delete aimed at a child, which is the shape
#92 actually had. So ``partitioned_cost_events`` below rebuilds
``billing_cost_events`` as a REAL RANGE-partitioned table and the tests under
"The partition half" run against it.
"""

from __future__ import annotations

import importlib.util
import uuid
from datetime import UTC, datetime, timedelta
from pathlib import Path

import pytest
import pytest_asyncio
import sqlalchemy as sa

import models.db.delete_audit as da
from models.db import ALLOW_TRUNCATE_GUC, ALLOW_TRUNCATE_VALUE, NULL_LICENSE_ID


def _load_migration():  # noqa: ANN202
    """Import alembic 0016 as a module, without importing it as a package."""
    path = (
        Path(da.__file__).resolve().parents[3]
        / "alembic"
        / "versions"
        / "0016_delete_audit_trigger.py"
    )
    spec = importlib.util.spec_from_file_location("_mig_0016", path)
    assert spec and spec.loader
    mig = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mig)
    return mig


def _cost_event_row(tenant_id: str, created_at: datetime) -> dict:
    return {
        "id": uuid.uuid4(),
        "tenant_id": tenant_id,
        "tenant_name": "acme",
        "service_key": "inference",  # pragma: allowlist secret
        "provider": "anthropic",
        "input_tokens": 0,
        "output_tokens": 0,
        "created_at": created_at,
    }


def _metering_event_row(tenant_id: str, created_at: datetime) -> dict:
    return {
        "id": uuid.uuid4(),
        "tenant_id": tenant_id,
        "tenant_name": "acme",
        "method": "POST",
        "path": "/v1/messages",
        "service_key": "inference",  # pragma: allowlist secret
        "status": "200",
        "created_at": created_at,
    }


async def _seed(conn, table: str, rows: list[dict]) -> None:
    cols = ", ".join(rows[0])
    binds = ", ".join(f":{c}" for c in rows[0])
    await conn.execute(sa.text(f"INSERT INTO {table} ({cols}) VALUES ({binds})"), rows)


async def _arm_truncate(conn) -> None:
    """Arm alembic 0017's BEFORE TRUNCATE escape hatch for this transaction."""
    await conn.exec_driver_sql(f"SET LOCAL {ALLOW_TRUNCATE_GUC} = '{ALLOW_TRUNCATE_VALUE}'")


async def _fresh(conn) -> None:
    """Clear the audit rows the per-test isolation fixture itself produced.

    ``tests/conftest.py`` isolates tests with ``TRUNCATE <every table> RESTART
    IDENTITY CASCADE``. That legitimately fires the TRUNCATE audit trigger on
    both ledgers, and an AFTER TRUNCATE trigger runs after the truncation — so
    the two rows it writes survive into the test. Correct behaviour of the
    control, not a defect; cleared here so each test counts only its own DELETE.
    """
    await conn.execute(sa.text("DELETE FROM billing_delete_audit"))


async def _audit(conn) -> list[sa.RowMapping]:
    result = await conn.execute(sa.text("SELECT * FROM billing_delete_audit ORDER BY id"))
    return list(result.mappings())


# ---------------------------------------------------------------------------
# The control itself
# ---------------------------------------------------------------------------


async def test_delete_from_cost_events_lands_an_audit_row(db_engine) -> None:
    """THE DEFECT (#92): a DELETE against the financial ledger left no record.

    This is the load-bearing assertion. It fails if the trigger is absent, if it
    is attached to the wrong relation, or if it fires but records nothing
    identifying — proven by removing the trigger and watching it go red.
    """
    tenant = str(uuid.uuid4())
    ts = datetime.now(UTC) - timedelta(days=1)
    async with db_engine.begin() as conn:
        await _fresh(conn)
        await _seed(conn, "billing_cost_events", [_cost_event_row(tenant, ts)])
        assert await _audit(conn) == [], "seeding must not write an audit row"
        await conn.execute(
            sa.text("DELETE FROM billing_cost_events WHERE tenant_id = :t"), {"t": tenant}
        )
        rows = await _audit(conn)

    assert len(rows) == 1, f"DELETE left no audit row (or too many): {rows}"
    row = rows[0]
    assert row["table_name"] == "billing_cost_events"
    assert row["operation"] == "DELETE"
    assert row["rows_deleted"] == 1
    # The forensics #92 needed and did not have.
    assert row["statement"] is not None and "DELETE FROM billing_cost_events" in row["statement"]
    # psycopg returns uuid[] as UUID objects on a raw SELECT (no ORM typing).
    assert [str(t) for t in row["tenant_ids"]] == [tenant], (
        "the deleted tenant must be identifiable"
    )
    assert row["distinct_tenant_count"] == 1
    assert row["db_user"], "no acting database user recorded"
    assert row["backend_pid"], "no backend pid recorded"
    assert row["created_at_min"] is not None and row["created_at_max"] is not None


async def test_delete_from_metering_events_lands_an_audit_row(db_engine) -> None:
    """The second audited ledger. Same trigger function, different table."""
    tenant = str(uuid.uuid4())
    ts = datetime.now(UTC) - timedelta(days=1)
    async with db_engine.begin() as conn:
        await _fresh(conn)
        await _seed(conn, "billing_metering_events", [_metering_event_row(tenant, ts)])
        await conn.execute(
            sa.text("DELETE FROM billing_metering_events WHERE tenant_id = :t"), {"t": tenant}
        )
        rows = await _audit(conn)

    assert len(rows) == 1, f"DELETE left no audit row: {rows}"
    assert rows[0]["table_name"] == "billing_metering_events"
    assert rows[0]["rows_deleted"] == 1


async def test_nil_tenant_delete_is_recorded_with_its_tenant(db_engine) -> None:
    """#92's exact shape: rows for ONE tenant, removed by a tenant predicate.

    Recorded here as the tenant that lost the rows, which is the question
    n_tup_del could never answer.
    """
    victim = str(uuid.uuid4())
    survivor = str(uuid.uuid4())
    ts = datetime.now(UTC) - timedelta(days=1)
    async with db_engine.begin() as conn:
        await _fresh(conn)
        await _seed(
            conn,
            "billing_cost_events",
            [_cost_event_row(victim, ts) for _ in range(3)] + [_cost_event_row(survivor, ts)],
        )
        await conn.execute(
            sa.text("DELETE FROM billing_cost_events WHERE tenant_id = :t"), {"t": victim}
        )
        rows = await _audit(conn)
        remaining = (
            await conn.execute(sa.text("SELECT count(*) FROM billing_cost_events"))
        ).scalar_one()

    assert remaining == 1
    assert len(rows) == 1
    assert rows[0]["rows_deleted"] == 3
    assert [str(t) for t in rows[0]["tenant_ids"]] == [victim]
    assert rows[0]["distinct_tenant_count"] == 1
    assert len(rows[0]["sample_ids"]) == 3


# ---------------------------------------------------------------------------
# Retention must not be amplified — the objection, answered by measurement
# ---------------------------------------------------------------------------


async def test_retention_sized_delete_writes_exactly_one_audit_row(db_engine) -> None:
    """A blanket trigger that fires per ROW would amplify routine cleanup.

    ``rollup_engine.cleanup_old_events`` prunes ``billing_metering_events`` with
    ONE bulk ``DELETE ... WHERE created_at < cutoff``. Statement-level means the
    whole sweep costs one audit row carrying the count, not one row per event.
    Asserted on 250 rows so a per-row trigger cannot pass by coincidence.
    """
    tenant = str(uuid.uuid4())
    old = datetime.now(UTC) - timedelta(days=400)
    cutoff = datetime.now(UTC) - timedelta(days=90)
    async with db_engine.begin() as conn:
        await _fresh(conn)
        await _seed(
            conn,
            "billing_metering_events",
            [_metering_event_row(tenant, old) for _ in range(250)],
        )
        await conn.execute(
            sa.text("DELETE FROM billing_metering_events WHERE created_at < :c"), {"c": cutoff}
        )
        rows = await _audit(conn)

    assert len(rows) == 1, f"retention sweep amplified into {len(rows)} audit rows"
    assert rows[0]["rows_deleted"] == 250
    # Capped, so a million-row sweep cannot write a million-element array.
    assert len(rows[0]["sample_ids"]) == 20
    assert len(rows[0]["tenant_ids"]) == 1


async def test_reason_labels_but_never_suppresses(db_engine) -> None:
    """``billing.delete_reason`` is a LABEL, not an exclusion.

    The alternative design excluded retention from the audit via a session GUC.
    That was rejected: a control the audited caller can switch off is not a
    control, and it would also blind the audit to retention going wrong. Setting
    the GUC changes ONE text column and still writes the row — this test is what
    stops a later change from turning the label into a filter.
    """
    tenant = str(uuid.uuid4())
    ts = datetime.now(UTC) - timedelta(days=400)
    async with db_engine.begin() as conn:
        await _fresh(conn)
        await _seed(conn, "billing_metering_events", [_metering_event_row(tenant, ts)])
        await conn.execute(sa.text("SET LOCAL billing.delete_reason = 'retention_sweep'"))
        await conn.execute(sa.text("DELETE FROM billing_metering_events"))
        rows = await _audit(conn)

    assert len(rows) == 1, "a labelled delete was SUPPRESSED — the label became a filter"
    assert rows[0]["reason"] == "retention_sweep"
    assert rows[0]["rows_deleted"] == 1


async def test_unlabelled_delete_is_recorded_as_unspecified(db_engine) -> None:
    """The #92 delete set no GUC. Its absence must not break the insert."""
    tenant = str(uuid.uuid4())
    ts = datetime.now(UTC) - timedelta(days=1)
    async with db_engine.begin() as conn:
        await _fresh(conn)
        await _seed(conn, "billing_cost_events", [_cost_event_row(tenant, ts)])
        await conn.execute(sa.text("DELETE FROM billing_cost_events"))
        rows = await _audit(conn)

    assert len(rows) == 1
    assert rows[0]["reason"] == da.DELETE_REASON_UNSPECIFIED


async def test_delete_matching_nothing_writes_no_row(db_engine) -> None:
    """A predicate that matched no rows is not an event. Keeps the table signal."""
    async with db_engine.begin() as conn:
        await _fresh(conn)
        await conn.execute(
            sa.text("DELETE FROM billing_cost_events WHERE tenant_id = :t"),
            {"t": str(uuid.uuid4())},
        )
        rows = await _audit(conn)
    assert rows == []


async def test_truncate_is_audited_too(db_engine) -> None:
    """TRUNCATE empties a financial table and fires no DELETE trigger at all.

    Measured on PostgreSQL 18.6 while designing this: with only the DELETE
    trigger installed, ``TRUNCATE`` left the audit table empty. Leaving that
    open would leave the obvious evasion open. It has no transition table, so
    ``rows_deleted`` is NULL by design rather than wrong.
    """
    tenant = str(uuid.uuid4())
    ts = datetime.now(UTC) - timedelta(days=1)
    async with db_engine.begin() as conn:
        await _fresh(conn)
        await _seed(conn, "billing_cost_events", [_cost_event_row(tenant, ts)])
        # alembic 0017 installs a BEFORE TRUNCATE guard that refuses an
        # unarmed session. This test is about the AFTER TRUNCATE audit
        # trigger, so it arms the hatch and asserts the audit behaviour it
        # always did — the guard is exercised on its own in
        # tests/test_truncate_guard.py.
        await _arm_truncate(conn)
        # alembic 0019 adds a watermark gate on top of 0017's: an armed truncate
        # is refused while raw events are newer than max(bucket_date). Put the
        # rollup in the caught-up state rather than arming 0019's override —
        # the override writes an audit row of its own and this test counts rows.
        await conn.execute(
            sa.text(
                "INSERT INTO billing_cost_daily (id, tenant_id, tenant_name, license_id, "
                "service_key, provider, bucket_date, total_input_tokens, "
                "total_output_tokens, total_cost_usd, request_count) "
                "VALUES (gen_random_uuid(), CAST(:t AS uuid), CAST(:t AS varchar), CAST(:lic AS uuid), 'audit-test', 'test', :d, 0, 0, 0, 0)"
            ),
            {"t": tenant, "lic": str(NULL_LICENSE_ID), "d": ts.astimezone(UTC).date()},
        )
        await conn.execute(sa.text("TRUNCATE billing_cost_events"))
        rows = await _audit(conn)

    assert len(rows) == 1, "TRUNCATE of a financial ledger was not audited"
    assert rows[0]["operation"] == "TRUNCATE"
    assert rows[0]["rows_deleted"] is None
    assert rows[0]["table_name"] == "billing_cost_events"


async def test_triggers_are_attached_to_both_audited_ledgers(db_engine) -> None:
    """Attachment, asserted from the catalog rather than inferred from a pass.

    A trigger on the wrong relation still lets every test above pass if they all
    happen to target the one relation that has it.
    """
    async with db_engine.begin() as conn:
        await _fresh(conn)
        result = await conn.execute(
            sa.text(
                """
                SELECT c.relname, t.tgname
                  FROM pg_trigger t
                  JOIN pg_class c ON c.oid = t.tgrelid
                 WHERE NOT t.tgisinternal
                   AND t.tgname IN ('billing_delete_audit_trg', 'billing_truncate_audit_trg')
                """
            )
        )
        attached = {(r[0], r[1]) for r in result}

    for table in da.AUDITED_TABLES:
        for trg in ("billing_delete_audit_trg", "billing_truncate_audit_trg"):
            assert (table, trg) in attached, f"{trg} is not attached to {table}"


# ---------------------------------------------------------------------------
# Drift guard between the migration and the module the tests exercise
# ---------------------------------------------------------------------------


def test_migration_ddl_is_identical_to_the_module_ddl() -> None:
    """The tests run against ``create_all`` + the module DDL; production runs the
    migration. If those two texts drift, every test above becomes a statement
    about a schema production does not have.

    This repo has already paid for exactly that: alembic 0012 and 0015 exist
    only to reconcile two schema sources that drifted. The migration stays
    frozen (it imports nothing from ``src``), so the equality is asserted here
    instead.
    """
    mig = _load_migration()

    assert mig.AUDITED_TABLES == da.AUDITED_TABLES
    assert mig.DELETE_AUDIT_DDL == da.DELETE_AUDIT_DDL, (
        "alembic 0016 and models/db/delete_audit.py have drifted — the tests "
        "above no longer describe the schema the migration builds"
    )


# ---------------------------------------------------------------------------
# The partition half — a REAL partitioned table, because create_all's is plain
# ---------------------------------------------------------------------------

#: A month that no other test and no retention sweep will ever mint, used as the
#: partition that already exists when the control is installed.
_PRE_EXISTING = (2030, 1)
#: A different month, minted by ``create_cost_events_partition_for`` AFTER the
#: control is installed. This is the "next month rolls around" case.
_MINTED_LATER = (2031, 3)


def _part_name(year: int, month: int) -> str:
    return f"billing_cost_events_{year:04d}_{month:02d}"


def _month_start(year: int, month: int) -> str:
    return f"{year:04d}-{month:02d}-01 00:00:00+00"


def _next_month_start(year: int, month: int) -> str:
    return _month_start(year + 1, 1) if month == 12 else _month_start(year, month + 1)


async def _apply(conn, ddl: tuple[str, ...]) -> None:
    """Run DDL the way ``models.db.delete_audit`` does at the driver boundary.

    psycopg3 reads ``%`` in a driver-level statement as a placeholder and this
    DDL is full of plpgsql ``format()`` specifiers, so they are doubled here —
    the same escape the module's ``after_create`` listener performs, and for the
    same reason. Alembic's ``op.execute`` does not interpolate, so the constants
    themselves stay un-escaped.
    """
    for stmt in ddl:
        await conn.exec_driver_sql(stmt.replace("%", "%%"))


@pytest_asyncio.fixture
async def partitioned_cost_events(db_engine):
    """Rebuild ``billing_cost_events`` as a genuinely RANGE-partitioned table.

    Why this fixture has to exist: production's ``billing_cost_events`` is
    partitioned by ``created_at`` (14 partitions at the time of writing) but the
    ORM model carries no ``postgresql_partition_by``, so ``create_all`` builds a
    plain table. Every assertion about partitions made against that schema is
    vacuous.

    The table is built with ``LIKE`` from the ORM table so the column set is the
    real one rather than a hand-written subset that could drift.

    The state it hands over deliberately mirrors what the migration finds on the
    prod primary at the moment it runs:

    1. a partitioned parent with ONE partition already attached, so
       ``ATTACH_TO_EXISTING_PARTITIONS_DDL``'s loop has a row to iterate;
    2. ``create_cost_events_partition_for`` present in its PRE-AUDIT form —
       alembic 0003's ``_FN_BILLING``, taken from the migration itself so this
       fixture cannot drift from it. This matters for the mutation proof:
       without it, deleting ``PARTITION_FN_WITH_AUDIT_DDL`` would fail the test
       with "function does not exist" rather than with the defect under test,
       which is a partition minted WITHOUT the audit trigger.

    Only then is the control applied, exactly as the migration applies it.
    """
    from db import Base

    mig = _load_migration()
    pre = _part_name(*_PRE_EXISTING)

    async with db_engine.begin() as conn:
        await conn.exec_driver_sql(
            "ALTER TABLE billing_cost_events RENAME TO billing_cost_events_orm_shape"
        )
        # INCLUDING DEFAULTS, not INCLUDING ALL: the ORM primary key is (id)
        # alone, and a partitioned table's PK must contain the partition key.
        await conn.exec_driver_sql(
            "CREATE TABLE billing_cost_events "
            "(LIKE billing_cost_events_orm_shape INCLUDING DEFAULTS) "
            "PARTITION BY RANGE (created_at)"
        )
        await conn.exec_driver_sql(
            "ALTER TABLE billing_cost_events ADD PRIMARY KEY (id, created_at)"
        )
        await conn.exec_driver_sql("DROP TABLE billing_cost_events_orm_shape CASCADE")
        await conn.exec_driver_sql(
            f"CREATE TABLE {pre} PARTITION OF billing_cost_events "
            f"FOR VALUES FROM ('{_month_start(*_PRE_EXISTING)}') "
            f"TO ('{_next_month_start(*_PRE_EXISTING)}')"
        )
        # The pre-audit minting function, verbatim from the migration.
        await _apply(conn, (mig._FN_BILLING_WITHOUT_AUDIT,))
        # The control, exactly as alembic 0016 applies it.
        await _apply(conn, da.DELETE_AUDIT_DDL)

    yield db_engine

    async with db_engine.begin() as conn:
        await conn.exec_driver_sql("DROP TABLE IF EXISTS billing_cost_events CASCADE")
        # Table.create fires the module's ``after_create`` listener, so the
        # restored plain table comes back ARMED for the rest of the session.
        await conn.run_sync(Base.metadata.tables["billing_cost_events"].create)


@pytest.mark.integration
async def test_delete_aimed_at_a_child_partition_is_audited(partitioned_cost_events) -> None:
    """#92's ACTUAL SHAPE: the non-zero ``n_tup_del`` was on a CHILD, not the parent.

    A statement trigger on the parent does not fire for a DELETE that names a
    partition, so a parent-only attachment would have been blind to the very
    event this control exists for. This is the assertion that
    ``ATTACH_TO_EXISTING_PARTITIONS_DDL`` is load-bearing — remove that DDL and
    this goes red while every parent-only test stays green.
    """
    engine = partitioned_cost_events
    child = _part_name(*_PRE_EXISTING)
    tenant = str(uuid.uuid4())
    ts = datetime(2030, 1, 15, 12, 0, tzinfo=UTC)

    async with engine.begin() as conn:
        await _fresh(conn)
        await _seed(conn, "billing_cost_events", [_cost_event_row(tenant, ts) for _ in range(3)])
        # Confirm the partition really is one, so a plain table cannot pass here.
        parent_of = (
            await conn.execute(
                sa.text(
                    "SELECT i.inhparent::regclass::text FROM pg_inherits i "
                    "WHERE i.inhrelid = to_regclass(:c)"
                ),
                {"c": child},
            )
        ).scalar_one()
        assert parent_of == "billing_cost_events"

        # The delete names the CHILD, never the parent.
        await conn.execute(sa.text(f"DELETE FROM {child} WHERE tenant_id = :t"), {"t": tenant})
        rows = await _audit(conn)

    assert len(rows) == 1, f"a DELETE aimed at partition {child} left no audit row: {rows}"
    row = rows[0]
    assert row["table_name"] == child, "the audit must name the partition that lost the rows"
    assert row["operation"] == "DELETE"
    assert row["rows_deleted"] == 3
    assert [str(t) for t in row["tenant_ids"]] == [tenant]
    assert row["statement"] is not None and child in row["statement"]


@pytest.mark.integration
async def test_a_partition_minted_after_the_control_is_armed(partitioned_cost_events) -> None:
    """The control must not lapse the first month a new partition appears.

    ``ensure_cost_event_partitions`` mints months ahead through
    ``create_cost_events_partition_for``. If that function is left in its 0003
    body, every future partition is born unaudited and the control quietly
    expires. Remove ``PARTITION_FN_WITH_AUDIT_DDL`` and this goes red — the
    fixture leaves the pre-audit function in place, so the failure is "the new
    partition was not armed", not "the function is missing".
    """
    engine = partitioned_cost_events
    year, month = _MINTED_LATER
    minted_name = _part_name(year, month)
    tenant = str(uuid.uuid4())
    ts = datetime(year, month, 9, 12, 0, tzinfo=UTC)

    async with engine.begin() as conn:
        await _fresh(conn)
        minted = (
            await conn.execute(
                sa.text("SELECT create_cost_events_partition_for(:y, :m)"),
                {"y": year, "m": month},
            )
        ).scalar_one()
        assert minted == minted_name

        await _seed(conn, "billing_cost_events", [_cost_event_row(tenant, ts)])
        assert await _audit(conn) == [], "seeding must not write an audit row"

        await conn.execute(
            sa.text(f"DELETE FROM {minted_name} WHERE tenant_id = :t"), {"t": tenant}
        )
        rows = await _audit(conn)

    assert len(rows) == 1, (
        f"partition {minted_name}, minted AFTER the control was installed, was "
        f"not armed — a DELETE against it left no audit row: {rows}"
    )
    assert rows[0]["table_name"] == minted_name
    assert rows[0]["rows_deleted"] == 1
    assert [str(t) for t in rows[0]["tenant_ids"]] == [tenant]


@pytest.mark.integration
async def test_truncate_of_a_partitioned_parent_writes_one_row_per_relation_emptied(
    partitioned_cost_events,
) -> None:
    """TRUNCATE of the parent writes N+1 rows, and that is the CORRECT behaviour.

    ``TRUNCATE billing_cost_events`` recurses into every partition, and each
    truncated relation fires its OWN statement trigger — so the audit records
    the parent plus every child, N+1 rows for N partitions (measured: 2 here
    with one partition; 15 on the prod primary's 14). This test exists because
    an earlier version of this PR's description claimed "exactly ONE row per
    statement", which is true only of DELETE and only against a plain table.

    Not collapsed to one row deliberately: the rows are what say WHICH
    partitions were emptied, and an audit that drops that is the same blindness
    ``n_tup_del`` already had. The invariant worth pinning is that TRUNCATE is
    never SILENT, and that each row names a real relation.
    """
    engine = partitioned_cost_events
    tenant = str(uuid.uuid4())
    ts = datetime(2030, 1, 15, 12, 0, tzinfo=UTC)

    async with engine.begin() as conn:
        await _fresh(conn)
        await _seed(conn, "billing_cost_events", [_cost_event_row(tenant, ts)])
        children = [
            r[0]
            for r in await conn.execute(
                sa.text(
                    "SELECT c.relname FROM pg_inherits i JOIN pg_class c ON c.oid = i.inhrelid "
                    "WHERE i.inhparent = 'billing_cost_events'::regclass"
                )
            )
        ]
        # alembic 0017 installs a BEFORE TRUNCATE guard that refuses an
        # unarmed session. This test is about the AFTER TRUNCATE audit
        # trigger, so it arms the hatch and asserts the audit behaviour it
        # always did — the guard is exercised on its own in
        # tests/test_truncate_guard.py.
        await _arm_truncate(conn)
        await conn.execute(sa.text("TRUNCATE billing_cost_events"))
        rows = await _audit(conn)

    assert len(rows) == len(children) + 1, (
        f"TRUNCATE of a parent with {len(children)} partition(s) wrote {len(rows)} "
        f"audit rows; expected N+1 = {len(children) + 1}"
    )
    assert {r["table_name"] for r in rows} == {"billing_cost_events", *children}
    for row in rows:
        assert row["operation"] == "TRUNCATE"
        # No transition table exists for TRUNCATE — NULL by design, not by bug.
        assert row["rows_deleted"] is None
