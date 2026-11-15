"""A ``BEFORE TRUNCATE`` gate on the rollup watermark — un-aggregated revenue is not deletable.

WHAT THIS CLOSES, AND WHY 0017/0018 DID NOT CLOSE IT
----------------------------------------------------
``billing_cost_daily`` holds **zero rows** for 2026-09-02, 09-03 and 09-04,
bounded by populated days on either side. The rollup *ran* on all three days; it
never reached those buckets. ``billing_cost_daily.id`` is UUIDv7, so the ids
decode write times, and they show every run writing exactly ONE ``bucket_date``,
advancing one day per run, permanently ~3 days behind::

    09-01 21:15 -> bucket 08-29     09-03 18:43 -> bucket 08-31
    09-02 21:32 -> bucket 08-30     09-04 23:14 -> bucket 09-01

Then, at **2026-09-05T07:01:56Z**, the TRUNCATE recorded in
``billing_delete_audit`` emptied ``billing_cost_events`` (and all 12 partitions)
plus ``billing_metering_events``. The last rolled bucket at that instant was
**09-01**. The statement therefore deleted *exactly the un-rolled backlog*:
``min(created_at)`` in ``billing_cost_events`` is now **09-05T07:01:58Z — two
seconds after the truncate**. ⛔ **Nothing is recoverable.** No surviving table
witnesses that window; 09-05 00:00–07:01Z is gone too, so even the boundary day
is partial. This module does not and cannot recover it — a backfill would invent
numbers, because there is no source to read.

0017 and 0018 gate on **INTENT**: is this session deliberately truncating
(``billing.allow_truncate = 'on'``)? That is necessary and it is not sufficient.
An *armed* truncate — the routine, blessed, everyday path — still destroys
un-aggregated revenue without a word, because nothing in that control looks at
whether the rollup has caught up. This module adds the missing predicate and
gates on **STATE**: are there raw cost events newer than
``max(bucket_date)`` in ``billing_cost_daily``? If yes, the truncate is refused
even when ``billing.allow_truncate`` is armed.

The two controls compose, and the composition is the point:

===========================  ==========================  ==============
``billing.allow_truncate``   rows after the watermark     result
===========================  ==========================  ==============
unset                        (not reached)                refused by 0017
``on``                       none                         **allowed**
``on``                       some                         refused HERE
``on``                       some, override armed         allowed + audited
===========================  ==========================  ==============

Both arms matter. A gate that refuses everything is as broken as one that
refuses nothing, which is why ``test_unrolled_truncate_gate.py`` asserts the
caught-up arm as hard as the behind arm.

WHY A DATABASE TRIGGER, AND NOT APPLICATION CODE
------------------------------------------------
This was weighed explicitly rather than assumed, and the incident decides it.

**An application-level guard would not have prevented this incident.** There is
no TRUNCATE anywhere in ``src/`` — ``grep -rn TRUNCATE src/ --include='*.py'``
returns only this control's own DDL and prose. The service has no truncate seam
for a guard to sit in, so an application-level check would have had *nothing to
intercept*: the 2026-09-05 statement did not travel through this service's code.
It arrived as raw SQL on a connection as ``billing_user``. A Python guard would
have been fully armed, fully tested, fully green, and completely bypassed.

And the client remains **UNIDENTIFIED** — ``tests/_production_tripwire.py``
refutes the test-harness attribution on two independent axes (no ``CASCADE`` in
the audited statement; a table set ``Base.metadata.sorted_tables`` cannot
produce). A control aimed at a *particular* client is a control aimed at a guess.
A trigger is client-agnostic by construction: it fires for psql, for a migration
tool, for a cron job, for this service, for a human with a superuser prompt who
is about to have a very bad morning.

So: **trigger, not application code** — deliberately, not by default. The cost of
that choice is that the gate is only testable against a real Postgres, which is
why every behavioural test here takes ``db_engine`` rather than a mock.

⚠️ Not "both". A duplicate check in the application would add a second place to
maintain the watermark predicate and a second place for it to drift, in exchange
for guarding a path that does not exist. If a truncate seam is ever added to
``src/``, it should call the database and let the trigger answer — not
re-implement the comparison.

THE ESCAPE HATCH — EXPLICIT, AUDITED, AND *NOT* THE DEFAULT PATH
-----------------------------------------------------------------
A legitimate operator truncate must remain possible. This one::

    SET LOCAL billing.allow_truncate = 'on';            -- 0017's hatch: intent
    SET LOCAL billing.allow_unrolled_truncate = 'on';   -- THIS hatch: state
    SET LOCAL billing.delete_reason = 'why, in words';
    TRUNCATE billing_cost_events;

⭐ The override is a **SECOND, DISTINCT GUC**, and that separation is the whole
design. Reusing ``billing.allow_truncate`` would mean every already-blessed
truncate — including the test harness's per-test isolation step, which arms it
several thousand times a day — silently inherits permission to destroy
un-aggregated revenue. The routine hatch must not be the un-aggregated hatch.
Someone who means to delete un-rolled money has to say so in a second sentence.

Taking the override writes a row into ``billing_delete_audit`` **before** the
truncate happens, with ``operation = 'TRUNCATE_UNROLLED_OVERRIDE'``, the
watermark, the row count and the ``created_at`` window that is about to be
destroyed. The existing ``AFTER TRUNCATE`` audit trigger (alembic 0016) still
writes its own row; this one is additive and answers the question 0016's row
cannot: *was there un-aggregated revenue in there, and did somebody knowingly
override the gate?* On 2026-09-05 nothing recorded that, which is why the loss
took forensic UUIDv7 decoding to reconstruct at all.

⚠️ Known limit, stated rather than hidden: if ``billing_delete_audit`` is itself
named in the same TRUNCATE statement — it was eleventh in the 2026-09-05 list —
this row is written by the BEFORE trigger and then destroyed by the same
statement, exactly as 0016's row is. That is a property of TRUNCATE, not of this
gate, and closing it needs an audit sink outside the database.

SCOPE, AND WHAT IS DELIBERATELY LEFT OUT
-----------------------------------------
Attached to ``billing_cost_events`` and to every partition of it, existing and
future. Partition coverage is not optional: a TRUNCATE aimed at a child does not
fire the parent's trigger, and the 2026-09-05 statement named all 12 children
explicitly. The gate reads the watermark once and probes the relation it actually
fired on, so a child-targeted truncate is judged on that child's own rows.

⛔ ``billing_metering_events`` is NOT gated here. Its aggregate chain is two
stages (``events -> billing_metering_hourly -> billing_metering_daily``), so its
watermark is a different, compound predicate, and writing the cost predicate
against the metering table would produce a gate that is confidently wrong rather
than absent — ``billing_forbid_unrolled_truncate`` hardcodes
``public.billing_cost_daily`` and ignores ``TG_TABLE_NAME``, so attached to the
metering table it would judge metering rows against the COST watermark.

⚠️ CORRECTED 2026-09-08 — this paragraph used to end "Separately tracked." IT
WAS NOT TRACKED. Measured on 2026-09-08: ``gh pr list --state all --search
'metering truncate'`` and ``gh issue list --state all`` over
``nonagenticai/billing-service`` returned **zero** PRs and **zero** issues
naming this gap, against a positive control of **6** issues in the same listing
— two of them (#134, #135) about ``billing_metering_events`` itself, so the
query could see metering work and there was none of this. For three days the
sentence was the only thing standing between a known, named hole in a financial
control and an owner, and it asserted an owner that did not exist. ⛔ Do not
write "separately tracked" without a number; an untracked gap that reads as
tracked is worse than one that reads as open.

⚠️ CORRECTED 2026-09-08 — this paragraph also used to say the metering watermark
is "an hourly watermark, not a daily one". Measured while building the gate:
NEITHER stage alone is correct. ``rollup_daily`` aggregates whatever
``billing_metering_hourly`` holds for a day without checking that all 24 hours
are present, so ``max(bucket_date)`` can advance over a day with holed hourly
coverage — a daily-only gate passes there. And hourly alone trusts a witness the
2026-09-05 statement was destroying in the same breath. The binding cutoff is
the EARLIER of the two stages' coverage ends.

The gate itself now lives in ``models/db/metering_unrolled_guard.py`` /
alembic ``0023_metering_unrolled_gate``, with its own function
(``billing_forbid_unrolled_metering_truncate``) and its own GUC
(``billing.allow_unrolled_metering_truncate`` — a third one, so arming the cost
ledger's override cannot silently authorise draining metering in the same
statement).

⛔ The **3-day rollup LAG** — the defect that created the backlog in the first
place — is NOT fixed here. It lives in ``src/services/rollup_engine.py`` /
``src/models/db/rollups.py`` and is a separately-tracked follow-up sequenced
behind another change. This module makes the lag *non-destructive*; it does not
make it stop. Do not read this as having fixed the lag.

WHY THE PREDICATE IS SPELLED THE WAY IT IS
-------------------------------------------
``rollup_engine.rollup_daily`` buckets a cost event by
``cast(timezone('UTC', created_at) as date)``, deliberately UTC-explicit so the
session timezone cannot shift a day boundary. A row is therefore un-aggregated
iff that expression is ``> max(bucket_date)``.

The gate asks the equivalent but **sargable** question::

    created_at >= ((max(bucket_date) + 1)::timestamp AT TIME ZONE 'UTC')

i.e. at or after the start of the day *after* the watermark. Identical truth
value, but it is an index/partition-prunable range predicate rather than a
per-row function call, so the *allowed* arm — the one that runs on a healthy
system — costs an index probe and not a full scan of the ledger. The expensive
``count(*)``/``min``/``max`` runs only once a refusal or an override is already
certain, and both of those are abnormal by definition.

FAIL CLOSED
-----------
An empty ``billing_cost_daily`` (``max(bucket_date)`` is NULL) means *nothing has
ever been rolled up*, so every raw row is un-aggregated: the cutoff becomes
``-infinity`` and the gate refuses. Same if ``billing_cost_daily`` does not exist
at all. A watermark this control cannot read is not evidence that deletion is
safe — it is the one situation where deletion is most obviously unsafe.
"""

from __future__ import annotations

from sqlalchemy import event

from .events import CostEvent
from .truncate_guard import PARTITION_FN_WITH_GUARD_DDL

#: The GUC that overrides the watermark gate.
#:
#: ⭐ Deliberately NOT ``billing.allow_truncate``. See "THE ESCAPE HATCH" above:
#: sharing 0017's GUC would hand every routine armed truncate — the test
#: harness's included — a standing licence to delete un-aggregated revenue.
ALLOW_UNROLLED_TRUNCATE_GUC = "billing.allow_unrolled_truncate"

#: The one value that arms it. Same single-exact-spelling rule as 0017's hatch:
#: 'true', '1', 'yes' and '' all refuse, so a fat-fingered ``SET`` cannot arm the
#: override, and a psql transcript reads unambiguously at incident review.
ALLOW_UNROLLED_TRUNCATE_VALUE = "on"

#: ``operation`` written to ``billing_delete_audit`` when the override is taken.
#: A distinct value (not 'TRUNCATE') so the override is greppable in one query:
#: ``SELECT * FROM billing_delete_audit WHERE operation LIKE '%OVERRIDE'``.
UNROLLED_OVERRIDE_OPERATION = "TRUNCATE_UNROLLED_OVERRIDE"

#: The relation whose watermark is authoritative for cost events.
WATERMARK_TABLE = "billing_cost_daily"

UNROLLED_GUARD_FUNCTION_DDL = r"""
CREATE OR REPLACE FUNCTION public.billing_forbid_unrolled_truncate()
RETURNS trigger
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path = public, pg_temp
AS $fn$
DECLARE
    v_watermark date;
    v_cutoff    timestamptz;
    v_unrolled  boolean;
    v_rows      bigint;
    v_min       timestamptz;
    v_max       timestamptz;
BEGIN
    -- to_regclass, not a cast: this function is also installed by the metadata
    -- after_create hook, where the rollup table may not exist yet. A missing
    -- watermark table is treated as "nothing has been rolled up", which is the
    -- fail-CLOSED reading -- an unreadable watermark is not evidence of safety.
    IF to_regclass('public.billing_cost_daily') IS NULL THEN
        v_watermark := NULL;
    ELSE
        SELECT max(bucket_date) INTO v_watermark FROM public.billing_cost_daily;
    END IF;

    IF v_watermark IS NULL THEN
        -- No bucket has ever been written: every raw row is un-aggregated.
        v_cutoff := '-infinity'::timestamptz;
    ELSE
        -- Start of the day AFTER the last rolled bucket. Equivalent to
        -- cast(timezone('UTC', created_at) as date) > v_watermark -- which is
        -- exactly how rollup_engine.rollup_daily assigns bucket_date -- but as a
        -- sargable range predicate, so the healthy path is an index probe.
        v_cutoff := (v_watermark + 1)::timestamp AT TIME ZONE 'UTC';
    END IF;

    -- Probe the relation the trigger actually fired on: on the parent this
    -- scans every partition, on a child only that child. A TRUNCATE aimed at a
    -- single partition is judged on that partition's own rows -- which is the
    -- shape the 2026-09-05 statement had.
    EXECUTE format(
        'SELECT EXISTS (SELECT 1 FROM public.%I WHERE created_at >= $1)',
        TG_TABLE_NAME)
      INTO v_unrolled
      USING v_cutoff;

    IF NOT v_unrolled THEN
        -- The rollup has caught up with this relation. Whatever intent-level
        -- control ran before us has already had its say; this gate has no
        -- further objection. THIS ARM MATTERS: a gate that refuses everything
        -- is as broken as one that refuses nothing.
        RETURN NULL;
    END IF;

    -- From here on a scan is affordable: either we abort the statement, or the
    -- caller is about to destroy the table anyway.
    EXECUTE format(
        'SELECT count(*), min(created_at), max(created_at) '
        'FROM public.%I WHERE created_at >= $1',
        TG_TABLE_NAME)
      INTO v_rows, v_min, v_max
      USING v_cutoff;

    IF coalesce(current_setting('billing.allow_unrolled_truncate', true), '')
       <> 'on' THEN
        RAISE EXCEPTION
            'TRUNCATE of %.% would delete % un-aggregated cost event(s)',
            TG_TABLE_SCHEMA, TG_TABLE_NAME, v_rows
            USING
                ERRCODE = 'P0001',
                DETAIL = format(
                    'billing_cost_daily watermark is %s; %s row(s) with created_at in '
                    '[%s, %s] have not been rolled up into any daily bucket. Deleting '
                    'them destroys revenue that no other table witnesses.',
                    coalesce(v_watermark::text, '(no bucket has ever been written)'),
                    v_rows, coalesce(v_min::text, '-'), coalesce(v_max::text, '-')),
                HINT =
                    'On 2026-09-05 a truncate removed exactly this backlog and NOTHING '
                    'was recoverable. Run the rollup until max(bucket_date) covers these '
                    'rows, then retry. If you MEAN to delete un-aggregated revenue, say '
                    'so explicitly in the same transaction: '
                    'SET LOCAL billing.allow_unrolled_truncate = ''on''; '
                    'SET LOCAL billing.delete_reason = ''<why>''; -- then TRUNCATE. '
                    'That path is audited into billing_delete_audit as '
                    'TRUNCATE_UNROLLED_OVERRIDE.';
    END IF;

    -- Override taken. Record it BEFORE the rows go, with the numbers that will
    -- otherwise be unreconstructable -- the 2026-09-05 loss took UUIDv7 decoding
    -- to characterise because no row like this existed.
    INSERT INTO public.billing_delete_audit (
        table_name, rows_deleted, operation, reason, db_user,
        session_user_name, application_name, client_addr, backend_pid,
        txid, statement, created_at_min, created_at_max
    ) VALUES (
        TG_TABLE_NAME,
        -- The UN-AGGREGATED count, not the total the statement removes. 0016's
        -- AFTER TRUNCATE row carries the statement; this row carries the loss.
        v_rows,
        'TRUNCATE_UNROLLED_OVERRIDE',
        format(
            'unrolled-truncate override: %s row(s) after watermark %s. operator reason: %s',
            v_rows,
            coalesce(v_watermark::text, '(none)'),
            coalesce(nullif(current_setting('billing.delete_reason', true), ''),
                     'unspecified')),
        current_user, session_user,
        nullif(current_setting('application_name', true), ''),
        inet_client_addr(), pg_backend_pid(),
        pg_current_xact_id_if_assigned()::text::bigint,
        left(current_query(), 8000),
        v_min, v_max
    );

    RETURN NULL;
END
$fn$;
"""


def _gate_attach_ddl(relation: str) -> str:
    """Idempotent BEFORE TRUNCATE gate attachment for one relation.

    Named ``billing_unrolled_truncate_guard_trg``, which sorts AFTER 0017's
    ``billing_truncate_guard_trg``. Postgres fires BEFORE TRUNCATE triggers in
    trigger-name order, so an un-armed session is refused by the intent gate
    first and never pays for this one's watermark read. Either order refuses;
    this order refuses more cheaply and with the more useful message.
    """
    return f"""
DROP TRIGGER IF EXISTS billing_unrolled_truncate_guard_trg ON public.{relation};
CREATE TRIGGER billing_unrolled_truncate_guard_trg
    BEFORE TRUNCATE ON public.{relation}
    FOR EACH STATEMENT EXECUTE FUNCTION public.billing_forbid_unrolled_truncate();
"""


ATTACH_GATE_TO_EXISTING_PARTITIONS_DDL = r"""
DO $do$
DECLARE
    v_parent oid;
    part     text;
BEGIN
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
            'DROP TRIGGER IF EXISTS billing_unrolled_truncate_guard_trg ON public.%I',
            part);
        EXECUTE format(
            'CREATE TRIGGER billing_unrolled_truncate_guard_trg BEFORE TRUNCATE ON public.%I '
            'FOR EACH STATEMENT EXECUTE FUNCTION public.billing_forbid_unrolled_truncate()',
            part);
    END LOOP;
END
$do$;
"""


#: The gate trigger, as the partition-minting function must attach it.
_MINT_GATE_BLOCK = """
            -- A TRUNCATE aimed at a partition does not fire the parent's
            -- trigger, so every minted partition carries the watermark gate too
            -- -- exactly as it carries the audit and intent triggers above. A
            -- partition minted without this is a hole in the control in
            -- precisely the place the 2026-09-05 truncation landed.
            EXECUTE format(
                'DROP TRIGGER IF EXISTS billing_unrolled_truncate_guard_trg ON public.%I',
                partition_name);
            EXECUTE format(
                'CREATE TRIGGER billing_unrolled_truncate_guard_trg BEFORE TRUNCATE ON public.%I '
                'FOR EACH STATEMENT EXECUTE FUNCTION public.billing_forbid_unrolled_truncate()',
                partition_name);

            RETURN partition_name;"""


def _with_gate(guarded_fn_ddl: str) -> str:
    """Extend 0017's partition-minting function with the watermark-gate attachment.

    DERIVED from ``truncate_guard.PARTITION_FN_WITH_GUARD_DDL``, never copied —
    the same discipline 0017 applied to 0016's version, for the same reason. The
    function is a single ``CREATE OR REPLACE``, so this migration's version
    replaces 0017's wholesale; a hand-copied body would silently lose the audit
    trigger (0016) or the intent guard (0017) the moment either changed, and the
    loss would surface only on the next partition mint, months later, as a
    partition that is unaudited or ungated.

    If 0017's body ever changes shape this raises instead of guessing, and the
    byte-equality test against the frozen alembic 0019 copy goes red so the
    migration is updated in the same commit.
    """
    needle = "\n            RETURN partition_name;"
    if guarded_fn_ddl.count(needle) != 1:
        raise RuntimeError(
            "unrolled_guard could not locate the single 'RETURN partition_name;' in "
            "truncate_guard.PARTITION_FN_WITH_GUARD_DDL. That function's body changed "
            "shape; re-derive the gate attachment rather than guessing."
        )
    return guarded_fn_ddl.replace(needle, _MINT_GATE_BLOCK)


PARTITION_FN_WITH_GATE_DDL = _with_gate(PARTITION_FN_WITH_GUARD_DDL)


#: The whole control, in application order. Alembic 0019 executes exactly this.
UNROLLED_GUARD_DDL: tuple[str, ...] = (
    UNROLLED_GUARD_FUNCTION_DDL,
    _gate_attach_ddl("billing_cost_events"),
    ATTACH_GATE_TO_EXISTING_PARTITIONS_DDL,
    PARTITION_FN_WITH_GATE_DDL,
)


def _arm(target, connection, **kw) -> None:  # noqa: ANN001, ARG001
    for stmt in UNROLLED_GUARD_DDL:
        # Same driver-boundary escaping as delete_audit._arm and
        # truncate_guard._arm: psycopg3 reads "%" in a driver-level statement as
        # a placeholder and this DDL is full of plpgsql format() specifiers.
        # Escaped HERE, never in the constants, because alembic's op.execute()
        # does not interpolate.
        connection.exec_driver_sql(stmt.replace("%", "%%"))


# Armed on the metadata create hook for the reason delete_audit and
# truncate_guard spell out: the test suite builds its schema with create_all
# rather than alembic, so without this the gate would exist only in the migration
# and every test of it would be vacuous.
#
# ⛔ ORDER MATTERS, and it is the import order in ``models/db/__init__.py``.
# delete_audit, truncate_guard and this module all CREATE OR REPLACE
# create_cost_events_partition_for; this module's version is the superset (it is
# derived from truncate_guard's, which is derived from delete_audit's), so it
# must run LAST. ``__init__`` imports it last to guarantee that, and
# ``test_a_freshly_minted_partition_carries_all_three_controls`` is what actually
# holds the ordering in place.
event.listen(CostEvent.__table__, "after_create", _arm)
