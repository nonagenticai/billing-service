"""The leaked-transaction guard: fail AT the leaking test, not at its victim.

THE CLASS THIS CLOSES
---------------------
A test whose transaction is abandoned without a ROLLBACK returns its connection
to the pool as ``idle in transaction`` (or ``idle in transaction (aborted)``)
while STILL HOLDING every lock it took. The next test's ``db_engine`` TRUNCATE
needs ACCESS EXCLUSIVE on every table, blocks behind those locks, and dies at
``statement_timeout`` with::

    sqlalchemy.exc.OperationalError: (psycopg.errors.QueryCanceled)
    canceling statement due to statement timeout
    [SQL: TRUNCATE "..." ... CASCADE]

The timeout aborts the truncating transaction too, so — if that one is also
left unrolled-back — it becomes another holder. It is SELF-AMPLIFYING: one leak
early in a run reappears as several unrelated *setup* errors later, on tests
that share nothing with it, and every victim passes in isolation. Measured on
this repo: 1 error at 27 tests, 1 at 52, 4 under xdist, 10 under xdist on a
contended database. That is an order-dependent mystery that blocks every push
and points at the wrong file.

This module turns it into a named failure on the test that actually leaked.

⚠️ POSTGRES REPORTS TWO STATES AND MATCHING ONLY ONE FAILS OPEN
---------------------------------------------------------------
``idle in transaction`` and ``idle in transaction (aborted)`` are DISTINCT
values of ``pg_stat_activity.state``. A hand-run probe written as
``state = 'idle in transaction'`` passed clean on three consecutive tests while
an aborted transaction sat there holding the locks that killed the next
TRUNCATE. The aborted one is the dangerous one — it is exactly what an
unrolled-back FAILED statement leaves behind. Hence ``LIKE 'idle in
transaction%'``, and hence ``tests/test_leaked_transaction_guard.py``, which
seeds BOTH states and asserts this predicate sees both. A guard that has only
ever been observed passing is indistinguishable from one that cannot fire.

SCOPE
-----
``application_name`` — this pytest PROCESS's backends only, never ``datname``
alone. This box runs dozens of concurrent sessions against Postgres and
pytest-xdist runs several worker processes against one database (each with its
own ``TEST_SCHEMA``), so a datname-scoped guard would redden this lane for
another lane's connection: an unfixable red on somebody else's diff.

Loaded BY PATH from ``tests/conftest.py`` (and from the test that asserts it),
for the same reason ``_production_tripwire.py`` is: ``tests/`` is deliberately
not on ``sys.path`` — putting it there shadows ``src/routers``.
"""

from __future__ import annotations

import re
import time

import psycopg

#: Matches BOTH ``idle in transaction`` and ``idle in transaction (aborted)``.
#: ``%%`` because this is passed to psycopg with a parameter — a single ``%``
#: would be read as a placeholder.
LEAKED_TXN_SQL = """
SELECT pid,
       datname,
       state,
       now() - xact_start AS held_for,
       left(coalesce(query, ''), 400) AS last_query
FROM pg_stat_activity
WHERE application_name = %s
  AND pid <> pg_backend_pid()
  AND state LIKE 'idle in transaction%%'
ORDER BY xact_start
"""


def sync_dsn(url: str) -> str:
    """``postgresql+psycopg://…`` → ``postgresql://…`` for a raw psycopg connect."""
    return re.sub(r"^postgresql\+\w+://", "postgresql://", url)


class LeakedTransactionGuard:
    """One long-lived AUTOCOMMIT psycopg connection that polices the rest.

    AUTOCOMMIT so the guard can never be the thing it is looking for, and
    deliberately a plain psycopg connection rather than the test engine: it
    shares no pool, no event loop and no transaction with the code it polices,
    so it still answers when that code has wedged itself.
    """

    def __init__(self, url: str, application_name: str) -> None:
        self._dsn = sync_dsn(url)
        self.application_name = application_name
        self._conn: psycopg.Connection | None = None
        #: Times :meth:`leaked` actually ran. A guard nobody can prove ran is
        #: indistinguishable from one that was silently skipped.
        self.checks = 0

    def _connection(self) -> psycopg.Connection:
        if self._conn is None or self._conn.closed:
            self._conn = psycopg.connect(self._dsn, autocommit=True)
        return self._conn

    def leaked(self) -> list[tuple]:
        """Backends of THIS process sitting in an open or aborted transaction."""
        self.checks += 1
        with self._connection().cursor() as cur:
            cur.execute(LEAKED_TXN_SQL, (self.application_name,))
            return cur.fetchall()

    def settled(self, attempts: int = 5, pause: float = 0.1) -> list[tuple]:
        """:meth:`leaked`, re-read a bounded number of times before believing it.

        An async connection handed back to the pool a moment ago may not have
        finished its server-side ROLLBACK yet. This CANNOT hide a real leak —
        nothing ever rolls a leaked transaction back, so it is still there on
        the last look — it only stops close-lag producing a false red.
        """
        rows = self.leaked()
        for _ in range(attempts - 1):
            if not rows:
                break
            time.sleep(pause)
            rows = self.leaked()
        return rows

    def close(self) -> None:
        if self._conn is not None and not self._conn.closed:
            self._conn.close()
        self._conn = None


def describe(rows: list[tuple]) -> str:
    return "\n".join(
        f"  pid={pid} db={datname} state={state!r} held_for={held_for} last_query={query!r}"
        for pid, datname, state, held_for, query in rows
    )
