"""A ``BEFORE TRUNCATE`` guard on the cost ledger — refuse unless deliberately armed.

WHAT HAPPENED
-------------
On 2026-09-05 at 07:01:56.876899Z an automated client TRUNCATEd
``billing_cost_events`` and all 12 of its partitions on the PROD billing
primary, together with ``billing_metering_events`` and every rollup table, in
one statement. ``billing_delete_audit`` ids 1-14 hold the only record: db_user
``billing_user``, empty ``application_name``, ``client_addr`` equal to the
shared node egress address, reason ``unspecified``. The statement was::

    TRUNCATE TABLE "billing_metering_events", "billing_metering_hourly",
    "billing_metering_daily", "billing_metering_monthly_usage",
    "billing_provider_costs", "processed_cost_messages",
    "processed_event_messages", "processed_cost_event_keys",
    "budget_overage_alerts", "token_usage", "billing_delete_audit",
    "billing_cost_events", "billing_cost_events_2026_06", … RESTART IDENTITY

⛔ ATTRIBUTION CORRECTED — THE CLIENT IS UNIDENTIFIED. This paragraph used to
read "that is a TEST-HARNESS shape: ``Base.metadata.sorted_tables`` in order …
this repo's own suite, pointed at the prod database". That is REFUTED twice
over, and the full working is in ``tests/_production_tripwire.py``. In short:
the audited statement carries NO ``CASCADE`` (``length(statement)``=713 against
an 8000-char cap, so not a capture truncation) while ``tests/conftest.py`` has
emitted ``RESTART IDENTITY CASCADE`` in every commit since the carve-out; and
the table SET does not match — 24 relations starting ``billing_metering_events``
against a metadata registry of 22 (21 pre-#114) starting ``audit_log``, with 12
never-registered partitions included and ten registered tables omitted.
``billing_cost_daily`` survived with 1235 rows dating to 2026-04-26; a
``sorted_tables`` statement would have emptied it. ``client_addr`` is the
shared node EGRESS address, common to 25+ db/user pairs, not an identity. No
replacement culprit is named, because none is established.

Read the ordering consequence too — ``billing_delete_audit`` is
eleventh in that list, so the audit table itself was emptied by the same
statement that the audit trigger was writing into. The 14 surviving rows exist
only because an AFTER TRUNCATE trigger runs after the truncation.

WHY NOT ``REVOKE TRUNCATE``
---------------------------
Measured: ``billing_user`` OWNS every one of these relations
(``pg_tables.tableowner``). TRUNCATE rights flow from ownership and cannot be
revoked from an owner — ``REVOKE TRUNCATE ON … FROM billing_user`` executes and
changes nothing. It is inert here, not merely weak.

WHY A TRIGGER, AND WHY AN ESCAPE HATCH
--------------------------------------
A ``BEFORE TRUNCATE`` statement trigger is the one mechanism that sits between
an owner and the truncation and can refuse it. Refusing unconditionally is not
an option: partition maintenance, a genuine restore and the test suite all
truncate legitimately. So the guard refuses unless the session has explicitly
said it means it::

    SET LOCAL billing.allow_truncate = 'on';
    TRUNCATE billing_cost_events;

The asymmetry is the whole design. A deliberate truncate is one extra line that
an operator writes on purpose; an automated client pointed at the wrong DSN
writes no such line, and is stopped. This is the shape the operator approved.

⛔ WHAT THIS GUARD DOES NOT STOP — READ BEFORE TRUSTING IT
----------------------------------------------------------
``tests/conftest.py`` HAS to arm the hatch, or every DB test fails on its own
isolation step. Which means: this suite, run against prod, would arm the hatch
and truncate prod again — the exact incident above. The trigger alone is
therefore VACUOUS against the one event it was written for.

That is closed on the other side, in ``tests/conftest.py``: the fixture refuses
to arm the hatch when the target database is the production billing database,
and fails loudly instead. Both halves are required. Neither is sufficient. If
you remove either one, say so in the PR rather than discovering it in an
incident.

Nothing here defends against a client that arms the hatch deliberately. It is
not meant to: the goal is to make truncation of a financial ledger an ACT rather
than an accident.

RELATIONSHIP TO THE 0016 AUDIT TRIGGER
--------------------------------------
``billing_truncate_audit_trg`` (AFTER TRUNCATE, from #92) records a truncation
that happened. This one prevents one that should not. They do not conflict and
they are not redundant:

* Postgres fires ALL ``BEFORE`` triggers before performing the truncation, and
  the ``AFTER`` triggers only once it has completed. A refusal here raises, the
  statement aborts, and the audit trigger never runs.
* So a BLOCKED truncate leaves NO row in ``billing_delete_audit`` — there is
  nothing to audit, because nothing was truncated. The refusal is recorded in
  the PostgreSQL server log as an ERROR, and at the client. Do not go looking
  for blocked attempts in the audit table; they are not there, by construction.
* An ALLOWED truncate (hatch armed) behaves exactly as it does today: the guard
  returns, the truncation proceeds, and the audit trigger writes its row. The
  audit control is strictly unaffected.

The two triggers are attached to the same 14 relations by the same rules, and
the partition-minting function carries both, so a future partition inherits both
or neither.
"""

from __future__ import annotations

from sqlalchemy import event

from .delete_audit import PARTITION_FN_WITH_AUDIT_DDL
from .events import CostEvent, MeteringEvent

#: The GUC a session sets to declare that it means to truncate.
#:
#: A *custom* GUC (``billing.``-prefixed) rather than a table or a role flag on
#: purpose: it is session-scoped and, with ``SET LOCAL``, transaction-scoped, so
#: it cannot be left armed for the next user of a pooled connection. A row in a
#: table would be global state that someone forgets to clear.
ALLOW_TRUNCATE_GUC = "billing.allow_truncate"

#: The value that arms it. Any other value — including unset, empty, 'true',
#: '1', 'yes' — refuses. A single exact spelling means an accidental
#: ``SET billing.allow_truncate = ''`` cannot arm it, and it reads unambiguously
#: in a psql transcript during an incident review.
ALLOW_TRUNCATE_VALUE = "on"

TRUNCATE_GUARD_FUNCTION_DDL = r"""
CREATE OR REPLACE FUNCTION public.billing_forbid_truncate()
RETURNS trigger
LANGUAGE plpgsql
AS $fn$
BEGIN
    -- current_setting(..., true) -> NULL for an unset GUC instead of raising,
    -- so an un-armed session takes the refusal path rather than an
    -- "unrecognized configuration parameter" error that reads like a bug in
    -- the guard.
    IF coalesce(current_setting('billing.allow_truncate', true), '') = 'on' THEN
        RETURN NULL;
    END IF;

    RAISE EXCEPTION
        'TRUNCATE of financial ledger %.% is blocked', TG_TABLE_SCHEMA, TG_TABLE_NAME
        USING
            ERRCODE = 'P0001',
            HINT =
                'This table holds billing records; billing is append-only and has no '
                'refund path, so an accidental TRUNCATE is unrecoverable. On 2026-09-05 '
                'an automated client emptied this ledger and every partition of it. If '
                'you MEAN to do this, arm the escape hatch in the same transaction: '
                'SET LOCAL billing.allow_truncate = ''on''; -- then TRUNCATE. '
                'Set billing.delete_reason too, so the audit row says why.';
END
$fn$;
"""

AUDITED_TABLES = ("billing_cost_events", "billing_metering_events")


def _guard_attach_ddl(relation: str) -> str:
    """Idempotent BEFORE TRUNCATE guard attachment for one relation."""
    return f"""
DROP TRIGGER IF EXISTS billing_truncate_guard_trg ON public.{relation};
CREATE TRIGGER billing_truncate_guard_trg
    BEFORE TRUNCATE ON public.{relation}
    FOR EACH STATEMENT EXECUTE FUNCTION public.billing_forbid_truncate();
"""


ATTACH_GUARD_TO_EXISTING_PARTITIONS_DDL = r"""
DO $do$
DECLARE
    v_parent oid;
    part     text;
BEGIN
    -- to_regclass, not a ::regclass cast, for the same reason 0016 uses it: this
    -- also runs from a per-table create hook where the parent may not exist yet.
    v_parent := to_regclass('public.billing_cost_events');
    IF v_parent IS NULL THEN
        RETURN;
    END IF;
    FOR part IN
        SELECT c.relname
          FROM pg_inherits i
          JOIN pg_class c ON c.oid = i.inhrelid
         WHERE i.inhparent = v_parent
    LOOP
        EXECUTE format(
            'DROP TRIGGER IF EXISTS billing_truncate_guard_trg ON public.%I', part);
        EXECUTE format(
            'CREATE TRIGGER billing_truncate_guard_trg BEFORE TRUNCATE ON public.%I '
            'FOR EACH STATEMENT EXECUTE FUNCTION public.billing_forbid_truncate()', part);
    END LOOP;
END
$do$;
"""


#: The guard trigger, as the partition-minting function must attach it.
_MINT_GUARD_BLOCK = """
            -- A TRUNCATE aimed at a partition does not fire the parent's
            -- trigger, so every minted partition carries its own guard --
            -- exactly as it carries its own audit trigger above. A partition
            -- created without this is a hole in the control, in precisely the
            -- place the 2026-09-05 truncation landed.
            EXECUTE format(
                'DROP TRIGGER IF EXISTS billing_truncate_guard_trg ON public.%I',
                partition_name);
            EXECUTE format(
                'CREATE TRIGGER billing_truncate_guard_trg BEFORE TRUNCATE ON public.%I '
                'FOR EACH STATEMENT EXECUTE FUNCTION public.billing_forbid_truncate()',
                partition_name);

            RETURN partition_name;"""


def _with_guard(audit_fn_ddl: str) -> str:
    """Extend 0016's partition-minting function with the guard attachment.

    DERIVED from ``delete_audit.PARTITION_FN_WITH_AUDIT_DDL``, never copied. The
    function is a single ``CREATE OR REPLACE``, so this migration's version
    REPLACES 0016's wholesale — a hand-copied body would silently lose 0016's
    audit attachment the moment 0016's copy changed, and the loss would show up
    only on the next partition mint, months later, as a partition that is
    unaudited. Deriving it means the audit half cannot be dropped by accident:
    if 0016's body moves, this moves with it, and the byte-equality test against
    alembic 0017's frozen copy goes red so the migration is updated too.
    """
    needle = "\n            RETURN partition_name;"
    if audit_fn_ddl.count(needle) != 1:
        raise RuntimeError(
            "truncate_guard could not locate the single 'RETURN partition_name;' in "
            "delete_audit.PARTITION_FN_WITH_AUDIT_DDL. That function's body changed "
            "shape; re-derive the guard attachment rather than guessing."
        )
    return audit_fn_ddl.replace(needle, _MINT_GUARD_BLOCK)


PARTITION_FN_WITH_GUARD_DDL = _with_guard(PARTITION_FN_WITH_AUDIT_DDL)


#: The whole control, in application order. Alembic 0017 executes exactly this.
TRUNCATE_GUARD_DDL: tuple[str, ...] = (
    TRUNCATE_GUARD_FUNCTION_DDL,
    *(_guard_attach_ddl(t) for t in AUDITED_TABLES),
    ATTACH_GUARD_TO_EXISTING_PARTITIONS_DDL,
    PARTITION_FN_WITH_GUARD_DDL,
)


def _install_for(relation: str) -> tuple[str, ...]:
    """The DDL that arms ONE relation, in dependency order."""
    ddl = [TRUNCATE_GUARD_FUNCTION_DDL, _guard_attach_ddl(relation)]
    if relation == "billing_cost_events":
        ddl += [ATTACH_GUARD_TO_EXISTING_PARTITIONS_DDL, PARTITION_FN_WITH_GUARD_DDL]
    return tuple(ddl)


def _arm(relation: str):  # noqa: ANN202
    def _listener(target, connection, **kw) -> None:  # noqa: ANN001, ARG001
        for stmt in _install_for(relation):
            # Same driver-boundary escaping as delete_audit._arm: psycopg3 reads
            # "%" in a driver-level statement as a placeholder, and this DDL is
            # full of plpgsql format() specifiers. Escaped HERE, never in the
            # constants, because alembic's op.execute() does not interpolate.
            connection.exec_driver_sql(stmt.replace("%", "%%"))

    return _listener


# Armed PER TABLE, for the reason spelled out in delete_audit: the test suite
# builds its schema with create_all rather than alembic, so without this hook the
# guard would exist only in the migration and every test of it would be vacuous.
#
# ⛔ ORDER MATTERS, and it is the import order in ``models/db/__init__.py``.
# delete_audit's listener and this one both CREATE OR REPLACE
# create_cost_events_partition_for. This module's version is the superset (it is
# derived from delete_audit's), so it must run SECOND. ``__init__`` imports
# delete_audit before truncate_guard to guarantee that, and
# tests/test_truncate_guard.py asserts a freshly minted partition carries BOTH
# triggers — which is what actually holds the ordering in place.
for _model in (CostEvent, MeteringEvent):
    event.listen(_model.__table__, "after_create", _arm(_model.__tablename__))
