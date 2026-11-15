"""The anti-truncation guard is armed, and until now nothing watched it.

CONTEXT. 2026-09-05 07:01:56Z the whole billing ledger was TRUNCATEd on prod by
this repo's own ``tests/conftest.py`` isolation step pointed at the production
DSN. Migrations 0017/0018/0019/0023 then installed ``billing_forbid_truncate()``
plus BEFORE TRUNCATE triggers, and conftest gained a production tripwire. Both
halves are live on prod (verified 2026-09-10).

⚠️ WHAT WAS STILL MISSING, AND WHAT THESE TESTS COVER: a sweep of every Prometheus
rule file found **zero** references to the guard, the trigger, or
``billing_delete_audit``. The incident rows were discovered five days after the
fact, by going to look. An armed control with no detector degrades to a hope the
moment someone edits the schema — so the guard's ATTACHMENT is now a gauge, and a
fall in it is the alert.

⛔ THESE TESTS DO NOT ATTEMPT A TRUNCATE, and that is deliberate rather than a
gap. The refusal itself is proven in ``tests/test_truncate_guard.py``; what rots
in practice is attachment, when a migration is rewritten or a partition is
created by a path that forgot to attach. That is what is measured here.
"""

from __future__ import annotations

from collections.abc import Mapping, Sequence
from typing import Any

import pytest
from sqlalchemy import text

from observability import metrics as observability_metrics
from services.ledger_guard_audit import run_ledger_guard_audit

pytestmark = pytest.mark.asyncio


@pytest.mark.unit
async def test_the_guard_is_counted_on_the_ledger_tables(db_session_factory) -> None:
    """The gauge reports the guard actually present on this database.

    The assertion is against a COUNT TAKEN FROM POSTGRES in the same test, not
    against a literal. A hard-coded expectation would go stale every month when
    ``create_cost_events_partition_for`` attaches the guard to a new partition —
    and a test that has to be edited monthly gets edited to whatever makes it
    pass.
    """
    attached, _rows = await run_ledger_guard_audit(db_session_factory)

    async with db_session_factory() as s:
        expected = int(
            (
                await s.execute(
                    text(
                        "SELECT count(*) FROM pg_trigger t JOIN pg_class c ON c.oid = t.tgrelid "
                        "WHERE t.tgname = 'billing_truncate_guard_trg' AND NOT t.tgisinternal"
                    )
                )
            ).scalar_one()
        )

    assert attached == expected
    assert expected > 0, (
        "no relation in this database carries billing_truncate_guard_trg — either "
        "the migrations did not run, or the guard has been detached; both are the "
        "condition this gauge exists to report"
    )
    assert observability_metrics.truncate_guard_relations._value.get() == expected


@pytest.mark.unit
async def test_a_detached_guard_is_reported_as_a_fall(db_session_factory) -> None:
    """⭐ THE MUTATION GUARD, and the only one that matters.

    Detaching the trigger from one relation must lower the gauge. A watcher that
    reported a constant would be indistinguishable from a working one on a healthy
    database, and would stay indistinguishable through exactly the event it exists
    to catch.

    The trigger is restored in a ``finally`` — this runs against a real schema,
    and a test that leaves the guard off would hand the next test a database whose
    ledger is unprotected, which is the defect, not the fixture.
    """
    before, _ = await run_ledger_guard_audit(db_session_factory)

    async with db_session_factory() as s:
        await s.execute(
            text(
                "DROP TRIGGER IF EXISTS billing_truncate_guard_trg "
                "ON public.billing_metering_events"
            )
        )
        await s.commit()
    try:
        after, _ = await run_ledger_guard_audit(db_session_factory)
        assert after == before - 1, (
            f"detaching one guard did not lower the count ({before} -> {after}); "
            "the watcher cannot see the thing it watches"
        )
        # ⚠️ The return value and the GAUGE are two different claims, and asserting
        # only the first leaves the publish step untested: a mutant that hard-codes
        # the gauge while returning the true count passes every assertion above.
        # Measured — that mutant survives without this line.
        assert observability_metrics.truncate_guard_relations._value.get() == after
    finally:
        async with db_session_factory() as s:
            await s.execute(
                text(
                    "CREATE TRIGGER billing_truncate_guard_trg BEFORE TRUNCATE "
                    "ON public.billing_metering_events FOR EACH STATEMENT "
                    "EXECUTE FUNCTION public.billing_forbid_truncate()"
                )
            )
            await s.commit()

    restored, _ = await run_ledger_guard_audit(db_session_factory)
    assert restored == before, "the fixture did not restore the guard it removed"


@pytest.mark.unit
async def test_an_allowed_truncate_leaves_a_counted_audit_row(db_session_factory) -> None:
    """A TRUNCATE the guard lets through must become a number, not just a row.

    This arms the real escape hatch and performs a real TRUNCATE against the TEST
    database, so the audit row is written by migration 0016's AFTER TRUNCATE
    trigger rather than inserted by hand. An earlier draft inserted a synthetic
    row and could not even be made to compile against the table's NOT NULL columns
    — which was the table telling me the truth: those columns exist precisely so
    every row names the session that caused it, and a hand-written row would have
    asserted that the watcher reads a shape the trigger never produces.

    Safe here and nowhere else: ``tests/conftest.py:81`` refuses to resolve a
    production DSN, which is the other half of the control this file watches.
    """
    async with db_session_factory() as s:
        before_rows = {
            str(op): int(n)
            for op, n in (
                await s.execute(
                    text("SELECT operation, count(*) FROM billing_delete_audit GROUP BY operation")
                )
            ).all()
        }
        # The guard REFUSES without this line. That asymmetry is the whole
        # control: a deliberate truncate is one extra statement, an automated
        # client pointed at the wrong DSN writes no such statement.
        await s.execute(text("SET LOCAL billing.allow_truncate = 'on'"))
        await s.execute(text("TRUNCATE TABLE billing_metering_events"))
        await s.commit()

    _attached, rows = await run_ledger_guard_audit(db_session_factory)

    assert rows.get("TRUNCATE", 0) > before_rows.get("TRUNCATE", 0), (
        "an ALLOWED truncate produced no counted audit row — the 2026-09-05 "
        "incident would be just as invisible as it was"
    )
    child = observability_metrics.delete_audit_rows.labels(operation="TRUNCATE")
    assert child._value.get() == rows["TRUNCATE"]


@pytest.mark.unit
async def test_the_guard_refuses_an_unarmed_truncate(db_session_factory) -> None:
    """⛔ THE CONTROL ITSELF, and the reason the gauge above is worth publishing.

    Without the GUC the TRUNCATE must RAISE. If this ever passes silently the
    guard is inert and every other assertion in this file is measuring the
    attachment of a trigger that does nothing.
    """
    import sqlalchemy.exc

    async with db_session_factory() as s:
        with pytest.raises(sqlalchemy.exc.DBAPIError):
            await s.execute(text("TRUNCATE TABLE billing_metering_events"))
        await s.rollback()


@pytest.mark.unit
async def test_an_empty_audit_table_publishes_no_truncate_label(db_session_factory) -> None:
    """NEGATIVE CONTROL. A clean ledger must not manufacture a TRUNCATE reading.

    The watcher publishes only the operations it actually saw. Pre-seeding a zero
    for TRUNCATE would be worse than absence here: it would make "no truncate has
    ever happened" and "the audit trigger was dropped, so nothing is recorded"
    report the same number, which is precisely the pair the 2026-09-05 incident
    needed told apart.
    """
    async with db_session_factory() as s:
        await s.execute(text("DELETE FROM billing_delete_audit"))
        await s.commit()

    _attached, rows = await run_ledger_guard_audit(db_session_factory)

    assert "TRUNCATE" not in rows
    assert rows == {}, f"an empty audit table reported operations: {rows}"


async def _db_now(db_session_factory):
    async with db_session_factory() as s:
        return (await s.execute(text("SELECT clock_timestamp()"))).scalar_one()


async def _allowed_truncate(db_session_factory) -> None:
    async with db_session_factory() as s:
        await s.execute(text("SET LOCAL billing.allow_truncate = 'on'"))
        await s.execute(text("TRUNCATE TABLE billing_metering_events"))
        await s.commit()


def _truncate_warnings(logs: Sequence[Mapping[str, Any]]) -> list[Mapping[str, Any]]:
    return [e for e in logs if e.get("event") == "ledger_truncate_audit_rows_present"]


@pytest.mark.unit
async def test_a_truncate_newer_than_the_acknowledged_mark_still_warns(db_session_factory) -> None:
    """The acknowledged high-water mark must silence ONLY what it covers: a
    TRUNCATE after it warns on the very next pass, naming how many are new."""
    from structlog.testing import capture_logs

    mark = await _db_now(db_session_factory)
    await _allowed_truncate(db_session_factory)

    with capture_logs() as logs:
        _attached, rows = await run_ledger_guard_audit(
            db_session_factory, acknowledged_through=mark
        )

    warnings = _truncate_warnings(logs)
    assert len(warnings) == 1, logs
    assert warnings[0]["unacknowledged_truncate_rows"] >= 1
    assert rows["TRUNCATE"] >= warnings[0]["unacknowledged_truncate_rows"]


@pytest.mark.unit
async def test_acknowledged_truncates_stop_warning_but_stay_on_the_gauge(
    db_session_factory,
) -> None:
    """The 2026-09-05 / 09-17 shape: investigated rows must not warn forever,
    and the all-time gauge must still carry them."""
    from structlog.testing import capture_logs

    await _allowed_truncate(db_session_factory)
    mark = await _db_now(db_session_factory)

    with capture_logs() as logs:
        _attached, rows = await run_ledger_guard_audit(
            db_session_factory, acknowledged_through=mark
        )

    assert _truncate_warnings(logs) == []
    assert rows["TRUNCATE"] >= 1
    child = observability_metrics.delete_audit_rows.labels(operation="TRUNCATE")
    assert child._value.get() == rows["TRUNCATE"]


@pytest.mark.unit
async def test_no_acknowledged_mark_warns_on_every_truncate_ever(db_session_factory) -> None:
    from structlog.testing import capture_logs

    await _allowed_truncate(db_session_factory)
    with capture_logs() as logs:
        await run_ledger_guard_audit(db_session_factory, acknowledged_through=None)
    assert len(_truncate_warnings(logs)) == 1


@pytest.mark.unit
def test_the_default_mark_covers_both_known_incidents_and_nothing_later() -> None:
    from datetime import UTC, datetime

    from settings import Settings

    mark = Settings.model_fields["ledger_truncate_acknowledged_through"].default
    # The last known TRUNCATE row (measured in prod billing_delete_audit) is
    # 2026-09-17 17:18:12.655017Z; anything later must still warn.
    assert mark == datetime(2026, 9, 17, 17, 18, 12, 655017, tzinfo=UTC)
