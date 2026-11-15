"""Tamper-evidence on ``audit_log`` — exercised against a real database.

THE DEFECT, MEASURED ON THE PROD PRIMARY: ``audit_log`` holds 4,103 live rows and
carried exactly ONE trigger (``billing_truncate_guard_trg``). The #92 forensic
control is attached to ``billing_metering_events`` and every
``billing_cost_events_*`` partition and was NEVER attached here, while
``services.audit.purge_old_audit_log`` DELETEs from it on a retention schedule
and self-commits. The table that records who changed money could be purged or
edited with no trace.

Two of the tests below are KILLING TESTS — they fail on ``origin/main`` because
the control does not exist there, not because a literal changed:

* :func:`test_a_delete_from_audit_log_leaves_exactly_one_forensic_row` — today
  the DELETE lands and ``billing_delete_audit`` stays empty.
* :func:`test_an_update_of_an_audit_row_is_refused` — today the UPDATE succeeds
  silently and nothing anywhere records that the row was rewritten.

And three are CONTROLS on the ways this change could be wrong rather than absent:

* :func:`test_the_real_retention_purge_still_deletes_and_is_recorded` calls the
  REAL ``purge_old_audit_log``, not a hand-written DELETE. Attaching the LEDGER
  function (``billing_audit_deleted_rows()``, which aggregates ``d.id`` into a
  ``uuid[]`` and reads ``d.created_at``) to this bigint-id, ``ts``-stamped table
  would install cleanly — a plpgsql body is parsed at EXECUTION — and break the
  purge at its next run. This test is what fails in that world.
* :func:`test_the_ledger_audit_function_cannot_read_audit_logs_shape` proves that
  hazard is REAL by attaching the ledger function here inside a rolled-back
  transaction and watching the DELETE raise. Without it, "a dedicated function
  was necessary" is a claim in a docstring.
* :func:`test_a_byte_identical_update_of_an_audit_row_is_permitted` proves the
  refusal is keyed on the DATA and not on UPDATE-in-general. Without it,
  :func:`test_the_deleted_backfill_statement_is_refused_because_it_would_launder_a_uuid`
  would pass just as happily against a blanket refusal.

⚠️ UPDATED 2026-09-15 — ``src/db.py`` NO LONGER UPDATES ``audit_log``. It used to
replay ``UPDATE audit_log SET tenant_name = tenant_id::text WHERE tenant_name IS
NULL`` on every pod start, inert only while every ``tenant_name IS NULL`` row also
had ``tenant_id IS NULL``. That precondition broke (prod, 2026-09-15: 4,186 rows,
286 with ``tenant_name IS NULL``, **55 of them with ``tenant_id`` SET**), the guard
below started refusing it on every boot, and the statement was DELETED — the
answer its own comment and this guard's HINT both prescribe.
:func:`test_no_startup_statement_updates_audit_log` is what keeps it deleted.
"""

from __future__ import annotations

import re
import uuid
from datetime import UTC, datetime, timedelta

import pytest
import sqlalchemy as sa
from sqlalchemy.exc import DBAPIError

from db import ADDITIVE_MIGRATIONS
from models.db import (
    ALLOW_TRUNCATE_GUC,
    ALLOW_TRUNCATE_VALUE,
    AUDIT_LOG_DELETE_REASON_PREFIX,
    AUDIT_LOG_TRUNCATE_REASON_PREFIX,
    AuditLog,
)
from services.audit import purge_old_audit_log

pytestmark = pytest.mark.unit


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

_INSERT_AUDIT_ROW = sa.text(
    """
    INSERT INTO audit_log (
        ts, actor_sub, actor_role, action, resource_type, resource_id,
        tenant_id, tenant_name, request_id
    ) VALUES (
        :ts, :actor_sub, 'admin', :action, 'token_budget', 'r1',
        :tenant_id, :tenant_name, 'req-1'
    )
    RETURNING id
    """
)


async def _seed(
    conn,
    *,
    tenant_id: str | None,
    ts: datetime | None = None,
    action: str = "budget.update",
    actor_sub: str = "tester",
    tenant_name: str | None = "acme",
) -> int:
    """Insert one audit row and return its (bigint) id."""
    result = await conn.execute(
        _INSERT_AUDIT_ROW,
        {
            "ts": ts or datetime.now(UTC),
            "actor_sub": actor_sub,
            "action": action,
            "tenant_id": tenant_id,
            "tenant_name": tenant_name,
        },
    )
    return int(result.scalar_one())


async def _clear_forensics(conn) -> None:
    """Drop the forensic rows the per-test isolation step itself produced.

    ``tests/conftest.py`` isolates tests with ``TRUNCATE <every table> RESTART
    IDENTITY CASCADE`` and arms the hatch to do it. That legitimately fires the
    AFTER TRUNCATE forensic trigger on ``audit_log`` (and on both ledgers), and
    an AFTER trigger runs after the truncation — so those rows survive into the
    test. Correct behaviour of the control, not a defect; cleared here so each
    test counts only its own statement.
    """
    await conn.execute(sa.text("DELETE FROM billing_delete_audit"))


async def _forensics(conn, table: str = "audit_log") -> list[sa.RowMapping]:
    result = await conn.execute(
        sa.text("SELECT * FROM billing_delete_audit WHERE table_name = :t ORDER BY id"),
        {"t": table},
    )
    return list(result.mappings())


# ---------------------------------------------------------------------------
# ⭐ KILLING TEST A — a DELETE leaves a record
# ---------------------------------------------------------------------------


async def test_a_delete_from_audit_log_leaves_exactly_one_forensic_row(db_engine) -> None:
    """⭐ THE DEFECT. A DELETE against ``audit_log`` left NO record of any kind.

    Fails on ``origin/main`` with zero rows: the #92 trigger was never attached
    to this table. It also fails if the trigger fires but records nothing
    identifying — a row count alone is the ``n_tup_del`` blindness in a new place.
    """
    tenant = str(uuid.uuid4())
    ts = datetime.now(UTC) - timedelta(days=200)

    async with db_engine.begin() as conn:
        await _clear_forensics(conn)
        row_id = await _seed(conn, tenant_id=tenant, ts=ts)
        assert await _forensics(conn) == [], "seeding must not write a forensic row"

        await conn.execute(sa.text("DELETE FROM audit_log WHERE tenant_id = :t"), {"t": tenant})
        rows = await _forensics(conn)

    assert len(rows) == 1, f"DELETE from audit_log left no forensic row (or too many): {rows}"
    row = rows[0]
    assert row["table_name"] == "audit_log"
    assert row["operation"] == "DELETE"
    assert row["rows_deleted"] == 1
    # The forensics the bare row count cannot give: who, whose, when, and the
    # statement itself — the field whose absence made #92 unanswerable.
    assert row["statement"] is not None and "DELETE FROM audit_log" in row["statement"]
    assert [str(t) for t in row["tenant_ids"]] == [tenant]
    assert row["distinct_tenant_count"] == 1
    assert row["db_user"]
    assert row["backend_pid"]
    # audit_log's timestamp column is `ts`; it lands in created_at_min/max,
    # which are named for the ledgers' column.
    assert row["created_at_min"] == row["created_at_max"]
    assert row["created_at_min"].astimezone(UTC) == ts
    # ⚠️ billing_delete_audit.sample_ids is uuid[] and audit_log.id is bigint, so
    # the ids CANNOT go there. They are carried in `reason` instead — asserted,
    # because a silently-NULL id field is exactly the kind of gap that reads as
    # "the trigger collected nothing".
    assert row["sample_ids"] is None
    assert row["reason"].startswith(AUDIT_LOG_DELETE_REASON_PREFIX)
    assert f"id range [{row_id}, {row_id}]" in row["reason"]
    assert "operator reason: unspecified" in row["reason"]


# ---------------------------------------------------------------------------
# ⭐ KILLING TEST B — an UPDATE is refused
# ---------------------------------------------------------------------------


async def test_an_update_of_an_audit_row_is_refused(db_engine) -> None:
    """⭐ THE SECOND HALF. Today this UPDATE succeeds and nothing records it.

    Unlike the event ledgers, ``audit_log`` can afford a refusal rather than a
    recording: nothing in ``src/`` legitimately rewrites an audit row, and the
    table has no column that could say a row had been edited — so an edit is
    indistinguishable from the truth.
    """
    tenant = str(uuid.uuid4())
    async with db_engine.begin() as conn:
        row_id = await _seed(conn, tenant_id=tenant, action="budget.update")

    async with db_engine.connect() as conn:
        trans = await conn.begin()
        with pytest.raises(DBAPIError) as err:
            await conn.execute(
                sa.text("UPDATE audit_log SET action = 'budget.read' WHERE id = :i"),
                {"i": row_id},
            )
        await trans.rollback()

    message = str(err.value)
    assert "is blocked" in message, message
    # The house style: say WHAT is blocked, and name the row.
    assert f"UPDATE of audit_log row {row_id}" in message, message
    # DETAIL names the column that moved, so the refusal is actionable.
    assert "changed: action" in message, message
    # HINT says what to do instead.
    assert "APPEND a new audit row" in message, message

    async with db_engine.begin() as conn:
        survivor = (
            await conn.execute(sa.text("SELECT action FROM audit_log WHERE id = :i"), {"i": row_id})
        ).scalar_one()
    assert survivor == "budget.update", "the refused UPDATE changed the row anyway"


async def test_the_refusal_covers_every_column_including_a_new_one(db_engine) -> None:
    """The comparison is whole-row ``to_jsonb``, not a column list.

    A per-column denylist rots in the worst direction — the next ``ADD COLUMN``
    is unguarded and nothing says so. Asserted here against ``before``, a jsonb
    column, and ``tenant_name``, which was added to this table later than the
    rest and is exactly the shape that would have been missed.
    """
    tenant = str(uuid.uuid4())
    async with db_engine.begin() as conn:
        row_id = await _seed(conn, tenant_id=tenant)

    for column, value in (
        ("tenant_name", "'renamed'"),
        ("before", """'{"limit": 1}'::jsonb"""),
        ("actor_sub", "'someone-else'"),
        ("ts", "now()"),
    ):
        async with db_engine.connect() as conn:
            trans = await conn.begin()
            with pytest.raises(DBAPIError) as err:
                await conn.execute(
                    sa.text(f"UPDATE audit_log SET {column} = {value} WHERE id = :i"),
                    {"i": row_id},
                )
            await trans.rollback()
        assert f"changed: {column}" in str(err.value), f"{column}: {err.value}"


# ---------------------------------------------------------------------------
# ⭐ THE PURGE-STILL-WORKS CONTROL — the real function, not a hand-written DELETE
# ---------------------------------------------------------------------------


def _audit_row(*, days_ago: float, tenant_id: str, action: str = "budget.update") -> AuditLog:
    return AuditLog(
        ts=datetime.now(UTC) - timedelta(days=days_ago),
        actor_sub="tester",
        actor_role="admin",
        action=action,
        resource_type="token_budget",
        resource_id="r1",
        tenant_id=tenant_id,
        request_id="req-1",
    )


async def test_the_real_retention_purge_still_deletes_and_is_recorded(
    db_session, db_engine
) -> None:
    """⭐ THE CONTROL THAT CATCHES THE SILENT BREAKAGE.

    Calls ``services.audit.purge_old_audit_log`` — the real retention function,
    the one live DELETE path against this table — rather than a hand-written
    DELETE. That distinction is the whole point: the obvious implementation of
    this change (adding ``audit_log`` to ``delete_audit.AUDITED_TABLES``) INSTALLS
    cleanly, because a plpgsql body is parsed at execution, and then fails the
    first time the purge runs, months later, because the ledger function
    aggregates ``d.id`` into a ``uuid[]`` (``audit_log.id`` is bigint) and reads
    ``d.created_at`` (``audit_log`` has only ``ts``).

    So this asserts BOTH halves: retention still removes what it should, and the
    statement is now recorded exactly once.
    """
    tenant = str(uuid.uuid4())
    async with db_engine.begin() as conn:
        await _clear_forensics(conn)

    db_session.add_all(
        [
            _audit_row(days_ago=200, tenant_id=tenant, action="ancient"),
            _audit_row(days_ago=181, tenant_id=tenant, action="just-past-cutoff"),
            _audit_row(days_ago=10, tenant_id=tenant, action="recent"),
        ]
    )
    await db_session.commit()

    deleted = await purge_old_audit_log(db_session, retention_days=180)

    assert deleted == 2, "the retention purge stopped deleting"
    remaining = (await db_session.execute(sa.select(AuditLog))).scalars().all()
    assert [r.action for r in remaining] == ["recent"]

    async with db_engine.begin() as conn:
        rows = await _forensics(conn)
    assert len(rows) == 1, f"the purge wrote {len(rows)} forensic rows, expected exactly 1: {rows}"
    assert rows[0]["operation"] == "DELETE"
    assert rows[0]["rows_deleted"] == 2
    assert rows[0]["table_name"] == "audit_log"
    # The purge issues an ORM-built DELETE; the statement text proves the record
    # came from the real caller and not from this test.
    assert "DELETE FROM audit_log" in rows[0]["statement"]


async def test_the_idempotent_second_purge_writes_no_forensic_row(db_session, db_engine) -> None:
    """A DELETE that matched nothing is not an event worth a row.

    ``purge_old_audit_log`` is documented idempotent and the suite already pins
    that a second call deletes zero. A statement trigger fires even on zero rows,
    so without the explicit ``v_rows = 0`` skip the retention job would write a
    forensic row every time it ran and find nothing — turning the audit table
    into a heartbeat log and burying the statements that matter.
    """
    tenant = str(uuid.uuid4())
    db_session.add(_audit_row(days_ago=200, tenant_id=tenant))
    await db_session.commit()

    assert await purge_old_audit_log(db_session, retention_days=180) == 1
    async with db_engine.begin() as conn:
        await _clear_forensics(conn)

    assert await purge_old_audit_log(db_session, retention_days=180) == 0

    async with db_engine.begin() as conn:
        rows = await _forensics(conn)
    assert rows == [], f"a zero-row purge wrote a forensic row: {rows}"


# ---------------------------------------------------------------------------
# Statement, not row
# ---------------------------------------------------------------------------


async def test_one_delete_statement_of_many_rows_writes_one_forensic_row(db_engine) -> None:
    """⭐ ONE ROW PER STATEMENT, not per row.

    This is what makes recording-rather-than-excluding affordable: the 180-day
    retention sweep costs exactly one forensic row however many audit rows it
    removes. A FOR EACH ROW trigger here would amplify a routine purge into
    thousands of rows and invite the exclusion that ``delete_audit.py`` rejected.
    """
    tenant = str(uuid.uuid4())
    base = datetime.now(UTC) - timedelta(days=300)

    async with db_engine.begin() as conn:
        await _clear_forensics(conn)
        ids = [
            await _seed(conn, tenant_id=tenant, ts=base + timedelta(days=i), action=f"a{i}")
            for i in range(5)
        ]
        await conn.execute(sa.text("DELETE FROM audit_log WHERE tenant_id = :t"), {"t": tenant})
        rows = await _forensics(conn)

    assert len(rows) == 1, f"a 5-row DELETE wrote {len(rows)} forensic rows"
    assert rows[0]["rows_deleted"] == 5
    assert f"id range [{min(ids)}, {max(ids)}]" in rows[0]["reason"]
    assert rows[0]["created_at_min"].astimezone(UTC) == base
    assert rows[0]["created_at_max"].astimezone(UTC) == base + timedelta(days=4)


async def test_a_delete_of_many_tenants_records_the_tenant_set(db_engine) -> None:
    """``tenant_id`` IS a uuid here, so the uuid[] columns do fit and are used."""
    tenants = sorted(str(uuid.uuid4()) for _ in range(3))
    async with db_engine.begin() as conn:
        await _clear_forensics(conn)
        for tenant in tenants:
            await _seed(conn, tenant_id=tenant)
        await conn.execute(sa.text("DELETE FROM audit_log"))
        rows = await _forensics(conn)

    assert len(rows) == 1
    assert rows[0]["distinct_tenant_count"] == 3
    assert sorted(str(t) for t in rows[0]["tenant_ids"]) == tenants


# ---------------------------------------------------------------------------
# Append control
# ---------------------------------------------------------------------------


async def test_an_insert_into_audit_log_is_unaffected(db_engine) -> None:
    """The append path — the only writer this table has — must not notice.

    ``services.audit.record`` INSERTs one row per mutating request on the write
    surface. A guard that taxed or blocked that would take the audit log offline
    in the name of protecting it.
    """
    tenant = str(uuid.uuid4())
    async with db_engine.begin() as conn:
        await _clear_forensics(conn)
        row_id = await _seed(conn, tenant_id=tenant, action="budget.create")
        stored = (
            await conn.execute(
                sa.text("SELECT action, tenant_id FROM audit_log WHERE id = :i"), {"i": row_id}
            )
        ).one()
        forensics = await _forensics(conn)

    assert stored[0] == "budget.create"
    assert str(stored[1]) == tenant
    assert forensics == [], f"an INSERT wrote a forensic row: {forensics}"


async def test_the_service_writer_still_records_through_the_orm(db_session, db_engine) -> None:
    """The same thing through the ORM path the service actually uses."""
    tenant = str(uuid.uuid4())
    db_session.add(_audit_row(days_ago=0, tenant_id=tenant, action="markup.update"))
    await db_session.commit()

    async with db_engine.begin() as conn:
        count = (await conn.execute(sa.text("SELECT count(*) FROM audit_log"))).scalar_one()
    assert count == 1


# ---------------------------------------------------------------------------
# TRUNCATE
# ---------------------------------------------------------------------------


async def test_a_truncate_of_audit_log_is_recorded(db_engine) -> None:
    """TRUNCATE is a DELETE-shaped way to empty the table that row triggers never see.

    ⚠️ The hatch is armed here deliberately, and it is armed for the SCHEMA rather
    than for this statement: in PROD, alembic 0018's catalog sweep put
    ``billing_truncate_guard_trg`` on ``audit_log``, so an un-armed TRUNCATE is
    refused there, while under ``create_all`` (this suite's schema)
    ``truncate_guard.py`` arms only the ledgers and the invoice tables. Arming it
    makes this test assert the same thing against both schemas.

    ``rows_deleted`` is NULL because TRUNCATE has no transition table — and the
    ``reason`` says so, so a reader cannot mistake "not measured" for "none".
    """
    tenant = str(uuid.uuid4())
    async with db_engine.begin() as conn:
        await _clear_forensics(conn)
        await _seed(conn, tenant_id=tenant)

    async with db_engine.begin() as conn:
        await conn.exec_driver_sql(f"SET LOCAL {ALLOW_TRUNCATE_GUC} = '{ALLOW_TRUNCATE_VALUE}'")
        await conn.exec_driver_sql("SET LOCAL billing.delete_reason = 'operator emptied it'")
        await conn.execute(sa.text("TRUNCATE TABLE audit_log"))
        rows = await _forensics(conn)

    assert len(rows) == 1, f"TRUNCATE left no forensic row: {rows}"
    assert rows[0]["operation"] == "TRUNCATE"
    assert rows[0]["rows_deleted"] is None
    assert rows[0]["reason"].startswith(AUDIT_LOG_TRUNCATE_REASON_PREFIX)
    assert "operator reason: operator emptied it" in rows[0]["reason"]
    assert "TRUNCATE TABLE audit_log" in rows[0]["statement"]


async def test_the_delete_reason_guc_is_recorded_and_never_enforced(db_engine) -> None:
    """``billing.delete_reason`` labels a statement; it can never suppress a row.

    ``delete_audit.py``'s docstring is explicit that the reason is "recorded,
    never enforced", and that is the line between a label and an override.
    Mislabelling is possible and harmless — the user, address, statement and row
    count are captured regardless.
    """
    tenant = str(uuid.uuid4())
    async with db_engine.begin() as conn:
        await _clear_forensics(conn)
        await _seed(conn, tenant_id=tenant)
        await conn.exec_driver_sql("SET LOCAL billing.delete_reason = 'retention sweep'")
        await conn.execute(sa.text("DELETE FROM audit_log"))
        rows = await _forensics(conn)

    assert len(rows) == 1, "a labelled DELETE suppressed its own forensic row"
    assert "operator reason: retention sweep" in rows[0]["reason"]


# ---------------------------------------------------------------------------
# ⛔ NOTHING IN src/ MAY UPDATE audit_log — the startup backfill is GONE
# ---------------------------------------------------------------------------

#: The statement ``src/db.py`` used to replay on every pod start, DELETED
#: 2026-09-15. It is kept here as a PROBE and nothing else: it is the positive
#: control that proves the scanner below can recognise an offending statement,
#: and it is the exact shape the guard must refuse.
#:
#: ⛔ It must NEVER be put back into ``ADDITIVE_MIGRATIONS``, nor reworded to
#: ``SET tenant_name = ''``, nor reached by any other route. Measured on prod
#: 2026-09-15: 55 audit rows carry ``tenant_id`` SET with ``tenant_name`` NULL,
#: so it would stamp 55 uuids into a human-facing display column on every boot.
#: The full record is in ``src/db.py``, just above ``ix_audit_log_tenant_name``.
_DELETED_TENANT_NAME_BACKFILL = (
    "UPDATE audit_log SET tenant_name = tenant_id::text WHERE tenant_name IS NULL"
)

#: Matches a statement that writes to ``audit_log``. TWO alternations, and the
#: second exists because an audit caught the first one missing a real shape:
#:   1. ``UPDATE`` ... ``audit_log`` — ``UPDATE audit_log``,
#:      ``UPDATE public.audit_log``, and a CTE that reaches it.
#:   2. ``audit_log`` ... ``DO UPDATE`` — an upsert, where the table name comes
#:      FIRST: ``INSERT INTO audit_log (...) ON CONFLICT (id) DO UPDATE SET ...``.
#:
#: ⛔ Alternation 1 ALONE let that upsert through. The database trigger still
#: refuses it at runtime, so the hole was in this tripwire, not in the defence —
#: but a guard that cannot see a shape it claims to cover is exactly the
#: "reports zero because it was blinded" failure this file exists to prevent.
#:
#: Deliberately broad: a false positive here is a loud failure that a human
#: reads, which is the correct outcome for any new write to this table.
_UPDATES_AUDIT_LOG = re.compile(
    r"\bUPDATE\b[\s\S]*?\baudit_log\b|\baudit_log\b[\s\S]*?\bDO\s+UPDATE\b",
    re.IGNORECASE,
)


def test_no_startup_statement_updates_audit_log() -> None:
    """⛔ THE GUARD AGAINST SILENT REINSTATEMENT. Keep this red-able.

    ``src/db.py`` replays ``ADDITIVE_MIGRATIONS`` on EVERY pod start and each
    statement fails SOFT. An UPDATE against ``audit_log`` reintroduced here would
    therefore never crash anything: it would either rewrite append-only audit
    rows, or — while ``billing_forbid_audit_log_update()`` is attached — log
    ``additive_migration_failed`` at WARNING forever, poisoning the one signal
    that means "a schema change did not apply". Neither is discoverable from a
    green suite, so it is asserted here instead.

    The non-vacuity block runs FIRST and on purpose: a scanner that reads an
    empty, non-tuple or renamed collection would report "no UPDATEs" for a reason
    that has nothing to do with the property, and a guard that reports zero
    because it was blinded is worse than no guard at all.
    """
    # --- non-vacuity: prove we are scanning the real, populated tuple ---------
    assert isinstance(ADDITIVE_MIGRATIONS, tuple), (
        f"ADDITIVE_MIGRATIONS is {type(ADDITIVE_MIGRATIONS)!r}, not a tuple — this "
        "test is no longer reading what src/db.py replays."
    )
    assert len(ADDITIVE_MIGRATIONS) > 50, (
        f"ADDITIVE_MIGRATIONS holds {len(ADDITIVE_MIGRATIONS)} statements. It held "
        "106 when this guard was written; an empty or gutted tuple would make the "
        "assertion below pass vacuously."
    )
    assert all(isinstance(s, str) and s.strip() for s in ADDITIVE_MIGRATIONS), (
        "ADDITIVE_MIGRATIONS contains a non-string or blank entry; the scan below "
        "cannot be trusted over it."
    )

    touching = [s for s in ADDITIVE_MIGRATIONS if "audit_log" in s]
    assert len(touching) >= 2, (
        "ADDITIVE_MIGRATIONS no longer mentions audit_log at all "
        f"({touching!r}). It must still carry the ADD COLUMN and the CREATE INDEX "
        "— both genuinely additive. If the table was renamed, this scanner is "
        "looking for a name that no longer exists and can never fire again."
    )

    # --- positive control: the scanner can see the statement that was deleted --
    assert _UPDATES_AUDIT_LOG.search(_DELETED_TENANT_NAME_BACKFILL), (
        "The matcher cannot recognise the very statement this test exists to "
        f"forbid: {_DELETED_TENANT_NAME_BACKFILL!r}. It is blinded."
    )

    # --- positive control: the UPSERT shape alternation 2 exists for ----------
    #: ⛔ Alternation 1 alone returned None here. An audit produced this exact
    #: mutant and it walked past the guard. Narrowing the pattern back reds this.
    assert _UPDATES_AUDIT_LOG.search(
        "INSERT INTO audit_log (id, tenant_name) VALUES (1, 'x') "
        "ON CONFLICT (id) DO UPDATE SET tenant_name = EXCLUDED.tenant_name"
    ), (
        "The matcher cannot see an UPSERT against audit_log, where the table "
        "name precedes the write verb. That is the shape that blinded it once."
    )

    # --- negative control: the surviving additive statements are not flagged ---
    for benign in (
        "ALTER TABLE audit_log ADD COLUMN IF NOT EXISTS tenant_name VARCHAR(255)",
        "CREATE INDEX IF NOT EXISTS ix_audit_log_tenant_name ON audit_log (tenant_name)",
        # From the class alternation 2 widened into: an append-only upsert that
        # writes NOTHING on conflict is legitimate and must stay unflagged.
        "INSERT INTO audit_log (id) VALUES (1) ON CONFLICT (id) DO NOTHING",
    ):
        assert not _UPDATES_AUDIT_LOG.search(benign), (
            f"The matcher flags a purely additive statement: {benign!r}."
        )

    # --- the property ---------------------------------------------------------
    offenders = [s for s in ADDITIVE_MIGRATIONS if _UPDATES_AUDIT_LOG.search(s)]
    assert not offenders, (
        "ADDITIVE_MIGRATIONS UPDATEs audit_log, which src/db.py replays on every "
        "pod start against an APPEND-ONLY table. The statement that used to live "
        "there was DELETED 2026-09-15 because its precondition broke: 55 prod "
        "audit rows carry tenant_id SET with tenant_name NULL, so it had started "
        "laundering uuids into a display column and "
        "billing_forbid_audit_log_update() was refusing it on every boot. ⛔ The "
        "answer is not to reinstate it, not to rewrite it as '', and not to widen "
        "the guard — see the block above ix_audit_log_tenant_name in src/db.py. "
        "An audit row is the record OF a change; append a new one instead "
        f"(services.audit.record). Offending statements: {offenders}"
    )


async def test_a_byte_identical_update_of_an_audit_row_is_permitted(db_engine) -> None:
    """The refusal is keyed on the DATA, not on the UPDATE keyword.

    ⭐ NON-VACUITY FOR THE TEST BELOW. Without this, the refusal test would pass
    against a blanket "no UPDATE ever" trigger, and the distinction the guard is
    built on — a write that moves no byte has nothing to be evidence of — would
    be untested. The statement used is the deleted backfill, run here directly
    (never through ``apply_additive_migrations``, which fails soft and would hide
    a refusal behind a WARNING).
    """
    async with db_engine.begin() as conn:
        row_id = await _seed(conn, tenant_id=None, tenant_name=None)
        await conn.exec_driver_sql(_DELETED_TENANT_NAME_BACKFILL)
        after = (
            await conn.execute(
                sa.text("SELECT tenant_id, tenant_name FROM audit_log WHERE id = :i"),
                {"i": row_id},
            )
        ).one()

    # NULL here is MEANINGFUL ("this event had no tenant") and must stay NULL —
    # see the record above ix_audit_log_tenant_name in src/db.py.
    assert after[0] is None
    assert after[1] is None


async def test_the_deleted_backfill_statement_is_refused_because_it_would_launder_a_uuid(
    db_engine,
) -> None:
    """⭐ WHY THE STATEMENT WAS DELETED, reproduced against the real database.

    This is the row shape that broke the backfill's precondition — ``tenant_id``
    SET, ``tenant_name`` NULL — and prod holds 55 of them (measured 2026-09-15).
    Against such a row the statement stamps a uuid into a display column: the
    laundering ``services._parsers.stored_tenant_name`` exists to prevent. The
    database refuses it.

    The statement is no longer in ``ADDITIVE_MIGRATIONS``; this test keeps the
    hazard measurable, so "we deleted it because it had started failing" stays a
    demonstrated fact rather than a line in a commit message — and so the guard
    still refuses it if anyone reinstates it despite
    :func:`test_no_startup_statement_updates_audit_log`.
    """
    tenant = str(uuid.uuid4())
    async with db_engine.connect() as conn:
        trans = await conn.begin()
        await _seed(conn, tenant_id=tenant, tenant_name=None)
        with pytest.raises(DBAPIError) as err:
            await conn.exec_driver_sql(_DELETED_TENANT_NAME_BACKFILL)
        await trans.rollback()

    assert "changed: tenant_name" in str(err.value), str(err.value)


# ---------------------------------------------------------------------------
# ⛔ Why the dedicated function exists — proven, not asserted in prose
# ---------------------------------------------------------------------------


async def test_the_ledger_audit_function_cannot_read_audit_logs_shape(db_engine) -> None:
    """⭐ THE HAZARD, REPRODUCED. The obvious implementation installs and then breaks.

    ``delete_audit.billing_audit_deleted_rows()`` aggregates ``d.id`` into a
    ``uuid[]`` and reads ``d.created_at``. ``audit_log.id`` is bigint and the
    table has no ``created_at`` at all. A plpgsql body is parsed at EXECUTION, so
    ``CREATE TRIGGER`` here SUCCEEDS — as it does below — and the failure only
    arrives with the next DELETE, i.e. inside the retention purge, long after the
    migration went green.

    Everything happens inside a transaction that is rolled back, so the probe
    trigger never outlives the test.
    """
    tenant = str(uuid.uuid4())
    async with db_engine.connect() as conn:
        trans = await conn.begin()
        await _seed(conn, tenant_id=tenant)
        # Named with a 'p' so it fires AFTER this module's own trigger: the error
        # that surfaces is then unambiguously the ledger function's.
        await conn.exec_driver_sql(
            "CREATE TRIGGER probe_ledger_audit_fn_trg AFTER DELETE ON public.audit_log "
            "REFERENCING OLD TABLE AS deleted FOR EACH STATEMENT "
            "EXECUTE FUNCTION public.billing_audit_deleted_rows()"
        )
        with pytest.raises(DBAPIError) as err:
            await conn.execute(sa.text("DELETE FROM audit_log"))
        await trans.rollback()

    # ``created_at`` is the first mismatch the server reaches; the bigint -> uuid[]
    # assignment is the second. Either way the purge dies.
    assert "created_at" in str(err.value), str(err.value)


async def test_the_delete_trigger_on_audit_log_points_at_the_audit_log_function(
    db_engine,
) -> None:
    """⛔ AND NOTHING MAY RE-POINT IT AT THE LEDGER FUNCTION.

    Alembic 0018 widened the TRUNCATE guard with a CATALOG sweep over every base
    table, which is the right shape for that control and the wrong one for this:
    a future "attach the delete audit everywhere it is missing" migration of the
    same shape would find ``audit_log`` lacking ``billing_delete_audit_trg``,
    attach ``billing_audit_deleted_rows()`` to it, pass, and reopen the breakage
    the test above reproduces. Proven from ``pg_trigger``/``pg_proc``, not by
    reading the DDL that claims to attach it.
    """
    async with db_engine.begin() as conn:
        rows = dict(
            (
                await conn.execute(
                    sa.text(
                        "SELECT t.tgname, p.proname FROM pg_trigger t "
                        "JOIN pg_class c ON c.oid = t.tgrelid "
                        "JOIN pg_proc p ON p.oid = t.tgfoid "
                        "WHERE c.relname = 'audit_log' AND NOT t.tgisinternal"
                    )
                )
            ).all()
        )

    assert rows.get("billing_audit_log_delete_audit_trg") == "billing_audit_log_deleted_rows", rows
    assert rows.get("billing_audit_log_truncate_audit_trg") == "billing_audit_log_deleted_rows", (
        rows
    )
    assert rows.get("billing_audit_log_immutability_trg") == "billing_forbid_audit_log_update", rows
    assert "billing_audit_deleted_rows" not in rows.values(), (
        "the LEDGER delete-audit function is attached to audit_log; it aggregates "
        "d.id into a uuid[] and reads d.created_at, neither of which audit_log has, "
        "so the retention purge will fail at its next run"
    )


# --------------------------------------------------------------------------- #
# The migration carries a VERBATIM COPY of this module's DDL, because no file
# under alembic/versions imports from `models` — that is this tree's convention,
# verified rather than assumed. A copy with no equality check is a copy that rots:
# the module would be edited, the migration would keep installing last week's
# function, and a FRESH database would diverge from a MIGRATED one with nothing
# reporting it.
#
# This test is written here, outside the agent that wrote the module, precisely
# because it cannot pass until the migration exists. ⛔ It must never be wrapped
# in `if migration_exists:` — an `if` around the only assertion is a skip nobody
# can grep for, and it would pass loudest at the moment the file went missing.
# --------------------------------------------------------------------------- #


def _load_migration_module():  # noqa: ANN202
    """Import alembic/versions/0029 by path; version files are not on sys.path."""
    import importlib.util
    from pathlib import Path

    path = (
        Path(__file__).resolve().parents[1]
        / "alembic"
        / "versions"
        / "0029_audit_log_tamper_evidence.py"
    )
    assert path.is_file(), (
        f"{path} is missing. The audit_log guard DDL lives in "
        "models/db/audit_log_guard.py but is INSTALLED by that migration; without "
        "it a migrated database has no guard at all while create_all databases do, "
        "so the suite would be green and production unprotected."
    )
    spec = importlib.util.spec_from_file_location("_mig_0029", path)
    assert spec and spec.loader
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def test_the_migration_ddl_has_not_drifted_from_the_module() -> None:
    """Byte equality, statement by statement, in order."""
    from models.db.audit_log_guard import AUDIT_LOG_GUARD_DDL, AUDIT_LOG_GUARD_DROP_DDL

    mig = _load_migration_module()

    assert mig.UPGRADE_DDL == AUDIT_LOG_GUARD_DDL, (
        "alembic 0029's UPGRADE_DDL no longer matches "
        "models.db.audit_log_guard.AUDIT_LOG_GUARD_DDL. A fresh create_all database "
        "and a migrated one would install DIFFERENT trigger functions. Regenerate "
        "the migration from the module — do not hand-edit one side."
    )
    assert mig.DOWNGRADE_DDL == AUDIT_LOG_GUARD_DROP_DDL, (
        "alembic 0029's DOWNGRADE_DDL no longer matches AUDIT_LOG_GUARD_DROP_DDL."
    )


def test_the_migration_is_chained_and_not_a_sibling_head() -> None:
    """0029 must extend 0028, not fork from it.

    Two revisions sharing a `down_revision` give alembic two heads; `upgrade head`
    then fails, and a single-head check run against either branch cannot see the
    other. This repo has shipped that defect before.
    """
    mig = _load_migration_module()
    assert mig.revision == "0029_audit_log_tamper_evidence"
    assert mig.down_revision == "0028_cost_event_event_time"
