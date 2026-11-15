"""Refuse to run the test suite against a production billing database.

WHY THIS IS A MODULE AND NOT FOUR LINES IN ``conftest.py``
----------------------------------------------------------
``conftest.py`` is not importable by name from a test (pytest loads it by path
under a private module name), so a check that lives only there cannot be
asserted by a test — and an unasserted guard is one nobody notices has stopped
working. This is the half of the truncate control that lives outside the
database; it deserves to be exercised, so it lives somewhere importable.

THE INCIDENT
------------
On 2026-09-05 07:01:56Z a ``TRUNCATE ... RESTART IDENTITY`` naming 24 billing
relations ran against the PROD billing primary and emptied
``billing_cost_events``, all 12 partitions, ``billing_metering_events`` and
every rollup table. Roughly three days of per-tenant cost attribution
(2026-09-02 → 07:01:58Z) are unrecoverable. ``billing_delete_audit`` ids 1-14
are the only surviving record: db_user ``billing_user``, empty
``application_name``, ``client_addr`` equal to the shared node egress
address, reason ``unspecified``.

⛔ THE CLIENT IS UNIDENTIFIED — AND THIS SUITE IS RULED OUT
-----------------------------------------------------------
This module, ``src/models/db/truncate_guard.py`` and alembic 0017 originally
attributed the incident to THIS SUITE: "byte-for-byte the shape ``db_engine``
builds — ``Base.metadata.sorted_tables`` in order, ``RESTART IDENTITY``". That
attribution is REFUTED, on two axes that fail independently of each other.

(a) NO ``CASCADE``. The audited statement ends
    ``..."billing_cost_events_2027_05" RESTART IDENTITY`` and stops there.
    ``length(statement)`` is 713 against an 8000-char capture cap, so the text
    was NOT truncated by the capture — the absence of ``CASCADE`` is real, not
    an artefact. ``tests/conftest.py`` has emitted the literal
    ``RESTART IDENTITY CASCADE`` in every commit since the repo carve-out::

        git log -S 'RESTART IDENTITY' --oneline -- tests/conftest.py
        # 7084c14 feat: carve billing-service out of metering-service (Phase 1)

    One commit, the carve-out. The suite has never emitted the audited form.

(b) WRONG TABLE SET, AND WRONG ORDER. Measured::

        PYTHONPATH=src python -c "import models.db; from db import Base; \
            print([t.name for t in Base.metadata.sorted_tables])"

    * at HEAD: 22 tables, first ``audit_log``
    * at the pre-#114 tree (9e6898d~1), the shape closest to what was deployed
      at 07:01:56Z: 21 tables, first ``audit_log``, ``billing_delete_audit``
      not yet mapped

    The audited statement is 24 relations, first ``billing_metering_events``.
    It INCLUDES the 12 ``billing_cost_events_YYYY_MM`` partitions, which plpgsql
    mints and SQLAlchemy never registers, so they cannot appear in
    ``sorted_tables`` under any revision of this repo. And it OMITS ten tables
    the metadata does contain: ``audit_log``, ``billing_app_cost_rates``,
    ``billing_cost_daily``, ``markup_rules``, ``provider_pricing``,
    ``request_quota_config``, ``tenant_access_charge``, ``tenant_config``,
    ``tenant_webhooks``, ``token_budgets``.

    The survivors are the proof, and they are stronger than the argument:
    ``billing_cost_daily`` still holds 1235 rows dating to 2026-04-26. Any
    statement built from ``sorted_tables`` would have emptied it.

⚠️ ``client_addr`` IS NOT AN IDENTITY. It is the shared node
EGRESS address; 25+ distinct db/user pairs SNAT through it, and both the
``pgbouncer-…`` and ``billing-service-…`` pods run there. It narrows the client
to "something in the cluster" and no further. The prod pod has no pytest
installed and ``TEST_DATABASE_URL`` unset, so it is not running this suite
either.

No replacement culprit is named here, because none has been established. What
IS established is that the conftest fixture did not write that statement.

WHY THIS MODULE STILL EXISTS
----------------------------
Its justification was never that it caused the incident — it is that it is a
live, unblocked path to the same outcome. ``conftest.db_engine`` MUST arm
``billing.allow_truncate`` or every DB test dies on its own isolation step, so
this suite pointed at the prod DSN would arm the hatch and truncate prod for
real. Alembic 0017's ``BEFORE TRUNCATE`` trigger cannot stop that, because the
suite is exactly the client that arms it. The trigger is the in-database half of
the control and this refusal is the outside-the-database half; neither is
sufficient. Removing either one is a PR-worthy statement, not an implementation
detail.
"""

from __future__ import annotations

#: Databases this suite must never touch.
#:
#: A DENYLIST of known-production names, deliberately, not an allowlist of test
#: names. An allowlist fails CLOSED on an unrecognised database — which sounds
#: safer and is worse here: it breaks every developer whose local database is
#: named something new, and the resulting pressure to relax it lands on the
#: guard itself. What is being protected is one specific, known database.
PRODUCTION_DB_NAMES: frozenset[str] = frozenset(
    {"billing", "billing_prod", "metering", "metering_prod"}
)


def database_name(url: str) -> str:
    """The database name from a SQLAlchemy/libpq URL.

    Deliberately string-only: this runs at ``conftest`` import time, before any
    engine exists, and must not depend on SQLAlchemy being importable yet.
    """
    without_query = url.split("?", 1)[0]
    parts = without_query.rsplit("/", 1)
    return parts[-1] if len(parts) == 2 else ""


def refuse_production_database(url: str) -> None:
    """Raise if ``url`` names a production billing database."""
    name = database_name(url)
    if name in PRODUCTION_DB_NAMES:
        raise RuntimeError(
            f"REFUSING TO RUN: TEST_DATABASE_URL points at database {name!r}, which is a "
            "PRODUCTION billing database. This suite TRUNCATEs every table before each "
            "test, and arms the billing.allow_truncate escape hatch in order to do it. "
            "The BEFORE TRUNCATE trigger from alembic 0017 cannot save you here, because "
            "this suite is exactly the client that arms it: point the suite at prod and "
            "it empties the prod cost ledger and all 12 of its partitions, the way an "
            "unidentified client did on 2026-09-05. Point TEST_DATABASE_URL at a test "
            "database."
        )


# ---------------------------------------------------------------------------
# The other direction: refuse to run with NO database configured
# ---------------------------------------------------------------------------
#
# Lives here, next to refuse_production_database, for the reason this module's
# docstring gives: conftest.py is not importable by name, so a guard that lives
# only there cannot be asserted by a test, and an unasserted guard is one nobody
# notices has stopped working. tests/test_test_database_url_required.py asserts
# this one.
#
# WHY IT EXISTS. Until 2026-09-07 conftest resolved the DSN as
# TEST_DATABASE_URL -> DATABASE_URL -> a HARDCODED literal. A hardcoded default
# is not a fallback, it is a silent redirect: it connects to whatever is
# listening on that host:port. On this dev box that is another lane's test
# database, and the session-scoped engine fixture DROP/CREATEs the schema in it.
# Three lanes were corrupted on 2026-09-07 -- one had its schema dropped
# mid-run, one deadlocked, one hit a failing CREATE DATABASE -- and two of the
# three debugged their own diff first, because a live peer database yields a
# PLAUSIBLE red suite where an absent one would have yielded an instantly
# legible "connection refused".
#
# The failure DIRECTION is the whole defect. Hence: no default, ever.

#: Env vars consulted for the test DSN, in precedence order.
TEST_DSN_ENV_VARS: tuple[str, ...] = ("TEST_DATABASE_URL", "DATABASE_URL")

#: The example export shown when nothing is set.
#:
#: ⛔ PLACEHOLDERS, NOT A WORKING VALUE, AND THAT IS THE POINT. A copy-pasteable
#: DSN in this string would be copy-pasted by every lane that ever hits it,
#: which rebuilds the shared-database collision this guard exists to close --
#: with everyone's explicit blessing, which is worse than the accident. The port
#: and the database name are PER-LANE. Do not put a real port here.
TEST_DSN_EXPORT_TEMPLATE = (
    "export TEST_DATABASE_URL="
    "'postgresql+psycopg://postgres:postgres@127.0.0.1:<YOUR_FREE_PORT>/<YOUR_OWN_DB>'"
)

_NO_TEST_DATABASE_CONFIGURED = f"""\
REFUSING TO RUN: no test database is configured.

Neither TEST_DATABASE_URL nor DATABASE_URL is set, and this suite has NO
default. Nothing has run yet -- this is NOT a failure of the code you are
changing and NOT a red on your diff.

FIX -- point the suite at a database THIS lane owns, and only this lane:

    {TEST_DSN_EXPORT_TEMPLATE}

  <YOUR_FREE_PORT>  a port nothing is already listening on. Check before you
                    pick one:   lsof -nP -iTCP:<port> -sTCP:LISTEN
                    No output = free. Any output = pick a different port.
  <YOUR_OWN_DB>     a database name unique to this lane, e.g.
                    billing_test_<your-branch-slug>.

DO NOT reuse another lane's port or database name, and do not paste a value a
colleague sent you. Sharing either one recreates the exact failure this refusal
exists to prevent.

WHY THERE IS DELIBERATELY NO DEFAULT: the session-scoped engine fixture runs
DROP SCHEMA ... CASCADE and recreates it. A hardcoded default DSN does not fail
closed -- it CONNECTS to whatever is listening on that host:port, which on a
shared dev box is another lane's test database, whose schema is then dropped
mid-run. The victim sees a plausible red suite on their OWN diff -- a dropped
schema, a deadlock, a failing CREATE DATABASE -- with nothing naming the real
cause. That happened to three lanes on 2026-09-07.

WHERE THIS NORMALLY COMES FROM, so you can tell what is missing:
  * CI: the fleet archetype's "Start ephemeral CI database" step exports DATABASE_URL
    into $GITHUB_ENV before pytest runs. Seeing this IN CI means that step did
    not run. Do not "fix" that by restoring a default here.
  * git push: the pre-push hook exports DATABASE_URL from [tool.local-ci.env]
    in pyproject.toml.
  * ad-hoc pytest: nothing sets it for you. Export TEST_DATABASE_URL yourself,
    as above. TEST_DATABASE_URL takes precedence over DATABASE_URL, so
    exporting it also isolates you during git push.

If TEST_DATABASE_URL IS set and the database is merely unreachable, you get an
ordinary psycopg connection error instead of this message."""


def resolve_test_database_url(test_database_url: str | None, database_url: str | None) -> str:
    """The test DSN, or raise if neither env var supplies one.

    Takes the two values as ARGUMENTS rather than reading ``os.environ`` so a
    test can exercise every branch without mutating the environment of the
    session that is running it.

    Precedence: ``TEST_DATABASE_URL`` then ``DATABASE_URL``. An empty string is
    not a DSN and is treated as unset -- ``TEST_DATABASE_URL=`` in a stale
    exported env must not silently select an empty DSN.

    Raises only for the UNSET case. A DSN that is set but unreachable is
    returned unchanged and fails later as an ordinary connection error, so a
    genuine database outage never masquerades as a configuration mistake.
    """
    for value in (test_database_url, database_url):
        if value and value.strip():
            return value
    raise RuntimeError(_NO_TEST_DATABASE_CONFIGURED)
