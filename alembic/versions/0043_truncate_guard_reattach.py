"""Re-run 0018's ATTACH-IF-ABSENT truncate-guard sweep: two tables were born unguarded.

billing-service#238
--------------------
``billing_cost_actor_daily`` (0040, 2026-09-17) and ``billing_attribution_rules``
(0041, 2026-09-25) were both created AFTER 0018 ran its one-time catalog sweep,
so neither ever got ``billing_truncate_guard_trg``. 0018's own docstring named
this exact failure mode: "the next table added to this schema would be born
unguarded, and nothing would say so." Nothing re-ran the sweep when 0040/0041
landed, so it happened again.

WHAT THIS DOES
--------------
Re-executes 0018's ATTACH-IF-ABSENT catalog sweep VERBATIM -- same SQL/DO
block, same exclusions (``alembic_version``, extension-owned relations,
non-permanent relations), same idempotent ``NOT EXISTS`` predicate, same
bounded ``lock_timeout``/``statement_timeout`` discipline. It is catalog-driven
for the same reason 0018 was: this closes the hole for
``billing_attribution_rules`` and ``billing_cost_actor_daily`` specifically,
and for any OTHER public base table that has slipped through unnoticed since
0018, without anyone having to enumerate them by hand.

Because the sweep only attaches where the trigger is absent, every table 0018
(or this migration, on a re-run) already armed is skipped untouched -- this
cannot open a window on an already-guarded ledger, and re-running it is a
no-op.

DOWNGRADE
---------
Scoped to the two tables this issue is about, not a general revert of
whatever the sweep above happened to catch: ``DROP TRIGGER IF EXISTS
billing_truncate_guard_trg`` on ``billing_attribution_rules`` and
``billing_cost_actor_daily`` only. Any other relation this migration armed
(a table that slipped through 0018 for reasons unrelated to #238) is left
guarded rather than guessed at, following 0018's convention of downgrading
to a named, deliberate set rather than "whatever upgrade() most recently did."

⛔ NOT APPLIED TO PROD BY THIS PR.

Revision ID: 0043_truncate_guard_reattach
Revises: 0042_metering_retarget_tenant
Create Date: 2026-09-30
"""

from __future__ import annotations

from collections.abc import Sequence

from alembic import op
from sqlalchemy.exc import DBAPIError

revision: str = "0043_truncate_guard_reattach"
down_revision: str | None = "0042_metering_retarget_tenant"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


# ⛔ Literal trigger name and schema, exactly as 0018 -- never a Python constant
# interpolated in, for the same reason 0018 gives: the NOT EXISTS predicate that
# makes this idempotent only recognises a prior attachment if the name matches
# byte-for-byte.


#: Fail loudly if 0017's function is missing rather than emitting CREATE TRIGGERs
#: that each fail with a bare "function does not exist". Identical to 0018's.
REQUIRE_GUARD_FUNCTION_DDL = r"""
DO $do$
BEGIN
    IF to_regprocedure('public.billing_forbid_truncate()') IS NULL THEN
        RAISE EXCEPTION
            'alembic 0043 requires public.billing_forbid_truncate() from 0017'
            USING HINT =
                'The guard function is missing, so there is nothing to attach. '
                'Run alembic 0017 first, or downgrade past 0018/0043 as well.';
    END IF;
END
$do$;
"""


#: 0018's sweep, reused verbatim. Catalog-driven on purpose -- see the docstring.
ATTACH_GUARD_TO_ALL_BASE_TABLES_DDL = r"""
DO $do$
DECLARE
    rel   text;
    armed integer := 0;
BEGIN
    FOR rel IN
        SELECT c.relname
          FROM pg_class c
          JOIN pg_namespace ns ON ns.oid = c.relnamespace
         WHERE ns.nspname = 'public'
           -- 'r' ordinary table (this is what a partition is, too) and
           -- 'p' partitioned parent. Views, matviews, sequences, indexes and
           -- foreign tables cannot be TRUNCATEd and are not candidates.
           AND c.relkind IN ('r', 'p')
           -- Permanent only: TEMP/UNLOGGED relations are per-session scratch.
           AND c.relpersistence = 'p'
           -- Alembic's own bookkeeping is not billing data.
           AND c.relname <> 'alembic_version'
           -- Extension-owned tables are not ours to put triggers on.
           AND NOT EXISTS (
                   SELECT 1
                     FROM pg_depend d
                    WHERE d.classid = to_regclass('pg_class')
                      AND d.objid = c.oid
                      AND d.deptype = 'e')
           -- ⛔ The idempotency predicate AND the promise not to disturb any
           -- relation already armed (by 0018 or a prior run of this migration).
           -- Attach only where the trigger is ABSENT; never drop and recreate
           -- one that already exists.
           AND NOT EXISTS (
                   SELECT 1
                     FROM pg_trigger t
                    WHERE t.tgrelid = c.oid
                      AND t.tgname = 'billing_truncate_guard_trg')
         ORDER BY c.relname
    LOOP
        EXECUTE format(
            'CREATE TRIGGER billing_truncate_guard_trg BEFORE TRUNCATE ON public.%I '
            'FOR EACH STATEMENT EXECUTE FUNCTION public.billing_forbid_truncate()',
            rel);
        armed := armed + 1;
        RAISE NOTICE 'billing_truncate_guard_trg -> public.%', rel;
    END LOOP;

    RAISE NOTICE 'alembic 0043: armed % newly-guarded relation(s)', armed;
END
$do$;
"""


#: Downgrade scope: ONLY the two tables #238 is about (see module docstring).
#: Deliberately not a catalog-driven "un-sweep" -- unlike 0018's downgrade
#: (which backs out to exactly 0017's original 14), this migration has no
#: single prior baseline to restore to, since it may also have armed tables
#: unrelated to #238 that slipped through 0018 for other reasons. Leaving
#: those guarded on downgrade is the safe default.
DETACH_GUARD_FROM_0043_TABLES_DDL = r"""
DO $do$
BEGIN
    EXECUTE 'DROP TRIGGER IF EXISTS billing_truncate_guard_trg '
            'ON public.billing_attribution_rules';
    EXECUTE 'DROP TRIGGER IF EXISTS billing_truncate_guard_trg '
            'ON public.billing_cost_actor_daily';
END
$do$;
"""


# --- lock discipline (identical to 0017/0018) -------------------------------

#: 3 seconds, the same bound 0016/0017/0018 chose against the same hot path.
LOCK_TIMEOUT = "3s"

#: Catalog-only work once the locks are held; 60s cannot be hit by healthy work.
STATEMENT_TIMEOUT = "60s"

#: 55P03 lock_not_available / 57014 query_canceled.
_TIMEOUT_SQLSTATES = frozenset({"55P03", "57014"})

_RETRY_IN_A_QUIETER_WINDOW = f"""
alembic 0043 could not acquire its DDL locks within {LOCK_TIMEOUT}. The whole
migration has ROLLED BACK - nothing was applied, and it is safe to re-run.

Something held a conflicting lock on one of the billing tables. Find it with:

    SELECT pid, state, xact_start, wait_event_type, left(query, 120)
      FROM pg_stat_activity
     WHERE datname = current_database() AND xact_start IS NOT NULL
     ORDER BY xact_start;

RETRY IN A QUIETER WINDOW (or after that transaction ends).

Do NOT raise LOCK_TIMEOUT to get past this. CREATE TRIGGER takes
ShareRowExclusiveLock, which conflicts with the RowExclusiveLock every INSERT
holds, and the lock queue is FIFO: while this migration waits, every NEW insert
into the billing tables queues behind it. A longer timeout is a longer billing
ingest stall, not a better chance of success.
""".strip()


def _bound_the_wait() -> None:
    """Bound this migration's lock wait, and prove the bound actually took.

    ``SET LOCAL`` outside a transaction block is a WARNING and a no-op, which
    would leave this guard vacuous. Read back rather than trusted -- same
    reasoning as 0016/0017/0018.
    """
    op.execute(f"SET LOCAL lock_timeout = '{LOCK_TIMEOUT}'")
    op.execute(f"SET LOCAL statement_timeout = '{STATEMENT_TIMEOUT}'")
    effective = op.get_bind().exec_driver_sql("SHOW lock_timeout").scalar()
    if effective in (None, "0", 0):
        raise RuntimeError(
            "alembic 0043 refuses to run without a bounded lock_timeout: SET LOCAL "
            f"reported lock_timeout={effective!r}, which means this is not running "
            "inside a transaction block and the DDL below could queue the billing "
            "INSERT path behind itself indefinitely."
        )


def _fail_fast(fn):  # noqa: ANN001, ANN202
    """Turn a lock/statement timeout into an instruction the operator can act on."""
    try:
        fn()
    except DBAPIError as exc:
        sqlstate = getattr(exc.orig, "sqlstate", None)
        if sqlstate in _TIMEOUT_SQLSTATES:
            raise RuntimeError(_RETRY_IN_A_QUIETER_WINDOW) from exc
        raise


def upgrade() -> None:
    _bound_the_wait()
    _fail_fast(_upgrade_ddl)


def _upgrade_ddl() -> None:
    op.execute(REQUIRE_GUARD_FUNCTION_DDL)
    op.execute(ATTACH_GUARD_TO_ALL_BASE_TABLES_DDL)


def downgrade() -> None:
    # DROP TRIGGER takes the same ShareRowExclusiveLock as CREATE TRIGGER, so
    # the downgrade can stall ingest exactly the way the upgrade can.
    _bound_the_wait()
    _fail_fast(_downgrade_ddl)


def _downgrade_ddl() -> None:
    op.execute(DETACH_GUARD_FROM_0043_TABLES_DDL)
