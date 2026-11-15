"""The guard that fails a leaking test must be shown able to FAIL.

``tests/_leaked_transaction_guard.py`` is what turns "a random test 200 tests
later dies in *setup* on ``canceling statement due to statement timeout``" into
"THIS test leaked a transaction". A guard that has only ever been observed
passing is indistinguishable from one that cannot fire, so this module seeds
the exact condition and asserts it is caught — including the state the first
hand-written version of the predicate silently missed.
"""

from __future__ import annotations

import importlib.util as _importlib_util
import time
from pathlib import Path as _Path

import psycopg
import pytest

from tests.conftest import LEAK_GUARD, PG_APPLICATION_NAME, TEST_DATABASE_URL

# Load the guard module the same way conftest does — by path — so this asserts
# the code that actually runs, not a second copy of it.
_spec = _importlib_util.spec_from_file_location(
    "_leaked_transaction_guard_under_test",
    _Path(__file__).parent / "_leaked_transaction_guard.py",
)
assert _spec is not None and _spec.loader is not None  # loader failure must be loud
guard_module = _importlib_util.module_from_spec(_spec)
_spec.loader.exec_module(guard_module)


def _leaked_pids(expected: set[int], timeout: float = 5.0) -> dict[int, str]:
    """Poll the guard until it reports ``expected``, or give up.

    Polls rather than reads once because ``pg_stat_activity`` is fed by the
    stats collector; a single read can be a moment stale and would make this
    proof flaky for a reason that has nothing to do with the predicate.
    """
    deadline = time.monotonic() + timeout
    seen: dict[int, str] = {}
    while time.monotonic() < deadline:
        seen = {row[0]: row[2] for row in LEAK_GUARD.leaked()}
        if expected <= seen.keys():
            return seen
        time.sleep(0.1)
    return seen


@pytest.mark.unit
def test_guard_catches_both_an_open_and_an_aborted_transaction(request) -> None:
    """⭐ THE PROOF THAT THE GUARD CAN FIRE.

    Seeds the two shapes a leak actually takes and asserts the guard sees both:

    * ``idle in transaction`` — a test that opened a transaction and walked away.
    * ``idle in transaction (aborted)`` — a test whose statement FAILED and was
      never rolled back. This is the one that kills the next TRUNCATE, because
      an aborted transaction still holds every lock it took.

    ⚠️ THE CONTROL, asserted below: the obvious predicate
    ``state = 'idle in transaction'`` MISSES the aborted one. A probe written
    that way passed clean on three consecutive tests while an aborted
    transaction sat there holding the locks. If someone ever "tidies" the
    ``LIKE`` back into an ``=``, this test goes red instead of the guard going
    quietly blind.
    """
    dsn = guard_module.sync_dsn(TEST_DATABASE_URL)

    open_conn = psycopg.connect(dsn, application_name=PG_APPLICATION_NAME)
    aborted_conn = psycopg.connect(dsn, application_name=PG_APPLICATION_NAME)
    try:
        # 1. An open, healthy transaction that nobody closed.
        with open_conn.cursor() as cur:
            cur.execute("SELECT 1")
            open_pid = open_conn.info.backend_pid

        # 2. A transaction poisoned by a failed statement and left unrolled-back
        #    — byte-for-byte what an unhandled DB error inside a test leaves on
        #    a pooled connection.
        aborted_pid = aborted_conn.info.backend_pid
        with pytest.raises(psycopg.errors.DivisionByZero), aborted_conn.cursor() as cur:
            cur.execute("SELECT 1 / 0")

        states = _leaked_pids({open_pid, aborted_pid})

        assert open_pid in states, (
            f"the guard did not see an OPEN leaked transaction (pid {open_pid}); it saw {states}"
        )
        assert aborted_pid in states, (
            f"the guard did not see an ABORTED leaked transaction (pid {aborted_pid}); "
            f"it saw {states} — this is the state that kills the next TRUNCATE"
        )
        assert states[open_pid] == "idle in transaction", states
        assert states[aborted_pid] == "idle in transaction (aborted)", states

        # THE CONTROL: an equality predicate would have reported the dangerous
        # one as clean.
        strict = {pid for pid, state in states.items() if state == "idle in transaction"}
        assert aborted_pid not in strict, (
            "the aborted transaction matched `state = 'idle in transaction'` exactly, "
            "so this control no longer distinguishes the two predicates"
        )
    finally:
        open_conn.rollback()
        open_conn.close()
        aborted_conn.rollback()
        aborted_conn.close()

    # …and once they are cleaned up the guard reports clean again, so a pass is
    # a real reading and not a predicate that can only ever say "yes".
    assert not [row for row in LEAK_GUARD.settled() if row[0] in {open_pid, aborted_pid}]


@pytest.mark.unit
def test_the_guard_is_actually_wired_in_autouse(request) -> None:
    """The predicate above is worth nothing if nothing runs it after each test.

    ``_no_leaked_transactions`` is autouse in ``tests/conftest.py``; if it is
    ever removed or renamed, the suite would go on passing while the guard
    quietly stopped existing.
    """
    assert "_no_leaked_transactions" in request.fixturenames


@pytest.mark.unit
def test_the_scope_is_this_process_not_the_whole_database() -> None:
    """Another lane's connection must never redden this run.

    Dozens of concurrent sessions share Postgres on this box, and xdist runs
    several worker PROCESSES against one database. A guard scoped by ``datname``
    would blame this lane for someone else's idle transaction — an unfixable
    red on a diff that did not cause it.
    """
    dsn = guard_module.sync_dsn(TEST_DATABASE_URL)
    other_lane = psycopg.connect(dsn, application_name="some-other-lane")
    try:
        with other_lane.cursor() as cur:
            cur.execute("SELECT 1")
        foreign_pid = other_lane.info.backend_pid
        time.sleep(0.2)
        assert foreign_pid not in {row[0] for row in LEAK_GUARD.leaked()}
    finally:
        other_lane.rollback()
        other_lane.close()
