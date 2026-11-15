"""Tests for ``scripts/backfill_app_attribution.py``.

WHY THESE ARE DATABASE TESTS AND THE SIBLING'S ARE NOT
------------------------------------------------------
``tests/test_backfill_license_attribution.py`` tests pure functions, because the
risky part of that script is a decision (``_in_window``) and a fail-closed HTTP
read. This script has neither: it has no HTTP call at all, and its decision is
one dict lookup inside ``license_resolver.resolve_app``, which is already tested
where it lives. Everything that can actually go wrong here is SQL —

* whether a dry run really writes nothing;
* whether ``ATTRIBUTE_SQL`` stamps only the rows it resolved, and never
  overwrites an ``app_id`` someone else established;
* whether the money guard can fire at all, and whether a fired guard actually
  rolls the batch back;
* whether ``VERIFY_SQL`` counts OUR writes rather than a concurrent flusher's;
* whether the keyset loop pages, terminates, and resumes.

— and none of that is observable without a database. An audit of the first
version of this script returned **UNPROVEN for ``--apply``** for exactly that
reason: ``apply_batch`` / ``ATTRIBUTE_SQL`` / ``VERIFY_SQL`` had executed zero
times, in production or anywhere else. These tests are the first execution.

The script is loaded by path (it lives under ``scripts/``, not an importable
package) the same way the sibling's tests load theirs, and its ``init_db`` /
``get_session_factory`` / ``configure_logging`` are redirected onto the test
fixtures — ``_run`` itself, the loop, the census and every statement are the
real ones.
"""

from __future__ import annotations

import asyncio
import importlib.util
import sys
import uuid
from datetime import UTC, datetime
from decimal import Decimal
from pathlib import Path

import pytest
from sqlalchemy import text

from models.db import NULL_APP_ID, CostEvent, RequestQuotaConfig
from services import license_resolver
from tests.conftest import T

pytestmark = pytest.mark.unit

SCRIPT_PATH = Path(__file__).resolve().parent.parent / "scripts" / "backfill_app_attribution.py"


def _load_module():
    spec = importlib.util.spec_from_file_location("backfill_app_attribution", SCRIPT_PATH)
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    sys.modules["backfill_app_attribution"] = module
    spec.loader.exec_module(module)
    return module


_mod = _load_module()

TS = datetime(2026, 8, 1, 12, 0, tzinfo=UTC)


@pytest.fixture(autouse=True)
def _reset_resolver_cache():
    """``get_app_map`` memoises for 300s in a module global.

    Without this a mirror seeded by one test is still in force for the next one,
    so a test that seeds NO licences would silently reuse the previous test's
    map and the fail-closed abort would never be reached.
    """
    license_resolver.reset_cache()
    yield
    license_resolver.reset_cache()


@pytest.fixture
def run_script(db_session_factory, monkeypatch):
    """Run the script's real ``_run`` against the test database."""

    async def _init(_settings):  # the script's init_db, neutered
        return None

    monkeypatch.setattr(_mod, "init_db", _init)
    monkeypatch.setattr(_mod, "get_session_factory", lambda: db_session_factory)
    # structlog configuration is process-global; leave the suite's alone.
    monkeypatch.setattr(_mod, "configure_logging", lambda _settings: None)

    async def _go(*, apply: bool, batch: int = 10, deadline_s: float = 30.0) -> int:
        # A deadline, not a bare await: the keyset loop's termination is one of
        # the things under test, and a non-terminating loop must FAIL here rather
        # than hang until the suite-wide timeout kills the whole session.
        # (Named deadline_s, not timeout: ruff ASYNC109 reserves that name.)
        async with asyncio.timeout(deadline_s):
            return await _mod._run(apply, batch)

    return _go


# ---------------------------------------------------------------------------
# seeding helpers
# ---------------------------------------------------------------------------


async def _seed_licence(session, *, license_id: uuid.UUID, app_id: uuid.UUID | None) -> None:
    session.add(
        RequestQuotaConfig(
            license_id=license_id,
            tenant_id=T("bf-app"),
            quota_monthly=1000,
            app_id=app_id,
            app_name="an app",
        )
    )
    await session.commit()


async def _seed_event(
    session,
    *,
    license_id: uuid.UUID | None,
    app_id: uuid.UUID | None = None,
    cost_usd: str = "0.25",
    excluded: bool = False,
    service_key: str = "inference-router",
) -> uuid.UUID:
    event = CostEvent(
        tenant_id=T("bf-app"),
        license_id=license_id,
        app_id=app_id,
        service_key=service_key,
        provider="claude",
        input_tokens=100,
        output_tokens=20,
        cost_usd=Decimal(cost_usd),
        cost_source="inference-router",
        excluded_from_billing=excluded,
        created_at=TS,
    )
    session.add(event)
    await session.commit()
    return event.id


async def _app_ids(session, ids: list[uuid.UUID]) -> dict[uuid.UUID, uuid.UUID | None]:
    rows = (
        await session.execute(
            text("SELECT id, app_id FROM billing_cost_events WHERE id = ANY(:ids)"),
            {"ids": ids},
        )
    ).all()
    return dict(rows)  # type: ignore[arg-type]


async def _updates_for(session, ids: list[uuid.UUID], app_id: uuid.UUID) -> list[dict]:
    """Build ``apply_batch`` input straight from the stored rows.

    ``created_at`` is read back rather than re-derived so the UPDATE's partition
    key matches to the microsecond.
    """
    rows = (
        await session.execute(
            text("SELECT id, created_at FROM billing_cost_events WHERE id = ANY(:ids) ORDER BY id"),
            {"ids": ids},
        )
    ).all()
    return [{"id": r, "created_at": c, "app_id": app_id} for r, c in rows]


# ---------------------------------------------------------------------------
# 1. a dry run writes nothing
# ---------------------------------------------------------------------------


async def test_dry_run_writes_nothing(db_session, run_script, capsys):
    """The default mode must be inert. Not "mostly inert" — zero writes.

    Kills the mutant that drops ``apply and`` from ``if apply and pending:``.
    """
    lic, app = uuid.uuid4(), uuid.uuid4()
    await _seed_licence(db_session, license_id=lic, app_id=app)
    ids = [await _seed_event(db_session, license_id=lic) for _ in range(3)]

    assert await run_script(apply=False) == 0

    after = await _app_ids(db_session, ids)
    assert set(after.values()) == {None}, "a dry run wrote an app_id"

    out = capsys.readouterr().out
    assert "DRY RUN. Nothing was written." in out
    # The resolvable rows must be reported as STILL NULL, not as attributed.
    assert "resolvable_but_dry_run=3" in out


# ---------------------------------------------------------------------------
# 2. --apply stamps app_id, and only on candidate rows
# ---------------------------------------------------------------------------


async def test_apply_stamps_only_candidate_rows(db_session, run_script):
    """Three populations, one run, three different correct outcomes."""
    lic, app = uuid.uuid4(), uuid.uuid4()
    other_app = uuid.uuid4()
    await _seed_licence(db_session, license_id=lic, app_id=app)

    candidate = await _seed_event(db_session, license_id=lic)
    parked = await _seed_event(db_session, license_id=lic, excluded=True)
    already = await _seed_event(db_session, license_id=lic, app_id=other_app)
    no_licence = await _seed_event(db_session, license_id=None)

    assert await run_script(apply=True) == 0

    got = await _app_ids(db_session, [candidate, parked, already, no_licence])
    assert got[candidate] == app
    # A parked row's app is still a true fact about that spend — see the
    # CANDIDATES_SQL comment on the deliberately absent excluded_from_billing
    # filter. Kills the mutant that adds one.
    assert got[parked] == app
    # Never overwrite an app someone else established.
    assert got[already] == other_app
    # No licence, nothing to derive from, nothing invented.
    assert got[no_licence] is None


async def test_a_licence_absent_from_the_mirror_is_declined_and_counted(
    db_session, run_script, capsys
):
    """Rule 1: leave it NULL and keep counting it.

    The counter matters as much as the NULL. Dropping the decline branch would
    append ``app_id=None`` to the batch — the row would still read NULL, so only
    the audit line can tell "declined" from "never looked at".
    """
    known, unknown = uuid.uuid4(), uuid.uuid4()
    app = uuid.uuid4()
    await _seed_licence(db_session, license_id=known, app_id=app)

    resolvable = await _seed_event(db_session, license_id=known)
    orphan = await _seed_event(db_session, license_id=unknown)

    assert await run_script(apply=True) == 0

    got = await _app_ids(db_session, [resolvable, orphan])
    assert got[resolvable] == app
    assert got[orphan] is None

    out = capsys.readouterr().out
    assert "declined_licence_not_in_mirror=1" in out
    assert "examined=2" in out


async def test_the_null_app_sentinel_is_never_stamped_onto_an_event(db_session, run_script, capsys):
    """``00000000-…-0001`` is a rollup sentinel, not an app.

    On an event it is a fabricated app id that looks real to every downstream
    reader. The mirror's ``app_id`` is a plain nullable uuid with nothing
    forbidding it, which is why this branch is kept while the licence-sentinel
    branch was removed as unreachable.
    """
    lic = uuid.uuid4()
    await _seed_licence(db_session, license_id=lic, app_id=NULL_APP_ID)

    event = await _seed_event(db_session, license_id=lic)

    assert await run_script(apply=True) == 0

    assert (await _app_ids(db_session, [event]))[event] is None
    assert "declined_sentinel_app=1" in capsys.readouterr().out


async def test_an_empty_mirror_aborts_instead_of_reporting_a_clean_no_op(db_session, run_script):
    """Fail CLOSED. "0 updated" must not be how blindness looks."""
    event = await _seed_event(db_session, license_id=uuid.uuid4())

    assert await run_script(apply=True) == 1

    assert (await _app_ids(db_session, [event]))[event] is None


async def test_it_refuses_to_run_if_billing_cost_daily_gains_app_grain(db_session, run_script):
    """The entire justification for having no restatement step.

    Checked against the live schema, and checked BEFORE any candidate is read —
    so the failure mode is a refusal, never a half-applied backfill.
    """
    lic, app = uuid.uuid4(), uuid.uuid4()
    await _seed_licence(db_session, license_id=lic, app_id=app)
    event = await _seed_event(db_session, license_id=lic)

    await db_session.execute(text("ALTER TABLE billing_cost_daily ADD COLUMN app_id uuid"))
    await db_session.commit()
    try:
        with pytest.raises(RuntimeError, match="restatement"):
            await run_script(apply=True)
    finally:
        await db_session.rollback()
        await db_session.execute(text("ALTER TABLE billing_cost_daily DROP COLUMN app_id"))
        await db_session.commit()

    assert (await _app_ids(db_session, [event]))[event] is None


# ---------------------------------------------------------------------------
# 3. the money guard
# ---------------------------------------------------------------------------

_MONEY_TRIGGER = """
CREATE OR REPLACE FUNCTION bf_app_test_move_money() RETURNS trigger
LANGUAGE plpgsql AS $$
BEGIN
    NEW.cost_usd := coalesce(NEW.cost_usd, 0) + 1;
    RETURN NEW;
END
$$;
CREATE TRIGGER bf_app_test_money BEFORE UPDATE ON billing_cost_events
FOR EACH ROW EXECUTE FUNCTION bf_app_test_move_money();
"""

_DROP_MONEY_TRIGGER = """
DROP TRIGGER IF EXISTS bf_app_test_money ON billing_cost_events;
DROP FUNCTION IF EXISTS bf_app_test_move_money();
"""


async def test_the_money_guard_aborts_and_rolls_back_when_the_write_moves_money(
    db_session, db_session_factory
):
    """The one thing the guard exists for, actually made to happen.

    Prod carries zero user triggers on this table today, so the guard cannot
    fire from normal operation — which is precisely why it has to be provoked
    here. A trigger that rewrites ``cost_usd`` on UPDATE is the failure the
    comment claims to catch; without this test that claim is untested prose.
    """
    lic, app = uuid.uuid4(), uuid.uuid4()
    await _seed_licence(db_session, license_id=lic, app_id=app)
    event = await _seed_event(db_session, license_id=lic, cost_usd="0.25")

    await db_session.execute(text(_MONEY_TRIGGER))
    await db_session.commit()
    try:
        async with db_session_factory() as session:
            updates = await _updates_for(session, [event], app)
            with pytest.raises(_mod.MoneyMovedError, match="monetary column"):
                await _mod.apply_batch(session, updates)
    finally:
        await db_session.rollback()
        await db_session.execute(text(_DROP_MONEY_TRIGGER))
        await db_session.commit()

    row = (
        await db_session.execute(
            text("SELECT app_id, cost_usd FROM billing_cost_events WHERE id = :i"),
            {"i": event},
        )
    ).one()
    # Rolled back, not merely reported: neither the app nor the money landed.
    assert row[0] is None
    assert row[1] == Decimal("0.25")


async def test_the_batch_rows_are_locked_so_concurrent_traffic_cannot_trip_the_guard(
    db_session, db_session_factory
):
    """The fix for a guard that could only false-alarm.

    Before ``LOCK_BATCH_SQL`` the two monetary snapshots were separate statements
    under READ COMMITTED, so a concurrent aggregator repricing one of these rows
    between them read as "money moved" and aborted a run that had done nothing
    wrong. The lock is what makes ``before != after`` mean "our write" — proven
    here by showing a concurrent monetary UPDATE cannot proceed while it is held.
    """
    event = await _seed_event(db_session, license_id=uuid.uuid4())

    async with db_session_factory() as holder:
        locked = (await holder.execute(text(_mod.LOCK_BATCH_SQL), {"ids": [event]})).all()
        assert locked == [(event, True)], "the lock read must also report attributability"

        async with db_session_factory() as rival:
            await rival.execute(text("SET LOCAL lock_timeout = '400ms'"))
            with pytest.raises(Exception, match="(?i)lock"):
                await rival.execute(
                    text("UPDATE billing_cost_events SET cost_usd = 9 WHERE id = :i"),
                    {"i": event},
                )
            await rival.rollback()

        await holder.rollback()


async def test_verified_does_not_count_a_row_a_concurrent_flusher_stamped(
    db_session, db_session_factory
):
    """``updated`` must be OUR writes, not every row that ends up non-NULL.

    The race is real: a row is a candidate at scan time and the flusher stamps
    it before ``apply_batch`` locks it. ``ATTRIBUTE_SQL`` correctly skips it —
    but counting it here credited the backfill with someone else's write.
    """
    app, theirs = uuid.uuid4(), uuid.uuid4()
    ours = await _seed_event(db_session, license_id=uuid.uuid4())
    raced = await _seed_event(db_session, license_id=uuid.uuid4(), app_id=theirs)

    async with db_session_factory() as session:
        updates = await _updates_for(session, [ours, raced], app)
        assert len(updates) == 2, "both rows must be offered to the batch"
        stamped = await _mod.apply_batch(session, updates)

    assert stamped == 1, "the already-stamped row was counted as ours"

    got = await _app_ids(db_session, [ours, raced])
    assert got[ours] == app
    assert got[raced] == theirs, "an app someone else established was overwritten"


# ---------------------------------------------------------------------------
# 4. the keyset loop: pages, terminates, resumes
# ---------------------------------------------------------------------------


async def test_the_batch_loop_pages_through_every_candidate_and_terminates(
    db_session, run_script, capsys
):
    """Five candidates, ``--batch 2``. Three pages, then a clean stop.

    The DRY RUN is the load-bearing half and it is deliberately first. Under
    ``--apply`` the keyset cursor is not the only thing bounding the loop: each
    committed batch stamps ``app_id``, so those rows leave the ``app_id IS NULL``
    candidate set and the scan would terminate even with a cursor that never
    advanced (measured — a mutant removing ``after = row_id`` passes the apply
    path outright). A dry run writes nothing, so the candidate set never shrinks
    and ``id > :after`` is the ONLY thing standing between this and an endless
    re-read of page one. Hence ``asyncio.wait_for``: non-termination has to fail
    here, not hang.
    """
    lic, app = uuid.uuid4(), uuid.uuid4()
    await _seed_licence(db_session, license_id=lic, app_id=app)
    ids = [await _seed_event(db_session, license_id=lic) for _ in range(5)]

    assert await run_script(apply=False, batch=2) == 0
    assert "examined=5" in capsys.readouterr().out
    assert set((await _app_ids(db_session, ids)).values()) == {None}

    assert await run_script(apply=True, batch=2) == 0

    got = await _app_ids(db_session, ids)
    assert list(got.values()) == [app] * 5
    assert "examined=5" in capsys.readouterr().out


async def test_a_re_run_skips_what_it_wrote_and_re_scans_what_it_declined(
    db_session, run_script, capsys
):
    """Resumability, stated exactly as the docstring now states it.

    A stamped row drops out of the candidate set. A DECLINED row does not — it
    is still ``app_id IS NULL``, so every re-run re-reads it from the cursor
    seed. That is the honest limit of "resumable" and it is asserted here so the
    docstring cannot drift from the behaviour.
    """
    known, unknown = uuid.uuid4(), uuid.uuid4()
    app = uuid.uuid4()
    await _seed_licence(db_session, license_id=known, app_id=app)
    stamped = await _seed_event(db_session, license_id=known)
    declined = await _seed_event(db_session, license_id=unknown)

    assert await run_script(apply=True) == 0
    first = capsys.readouterr().out
    assert "examined=2" in first

    assert await run_script(apply=True) == 0
    second = capsys.readouterr().out
    # Only the decline is still a candidate; the stamped row is gone from the set.
    assert "examined=1" in second
    assert "declined_licence_not_in_mirror=1" in second
    assert "attributed an app to 0 of them" in second

    got = await _app_ids(db_session, [stamped, declined])
    assert got[stamped] == app
    assert got[declined] is None
