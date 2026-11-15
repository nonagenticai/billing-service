"""billing_invoices: a draft->issued UPDATE may not also rewrite the money

Revision ID: 0030_draft_to_issued_money_diff
Revises: 0029_audit_log_tamper_evidence
Create Date: 2026-09-11

``billing_forbid_invoice_mutation()`` (0024) freezes an ISSUED invoice properly:
the one permitted UPDATE is to ``'void'``, and an allowlist jsonb diff --
``to_jsonb(OLD) - 'status' - 'voided_at' - 'voided_by' - 'void_reason'`` against
the same subtraction on NEW -- refuses a void that also restates a figure.

⛔ THE DRAFT BRANCH HAD NO SUCH CHECK, AND THAT MADE THE ISSUED BRANCH WORTH VERY
LITTLE. On ``OLD.status = 'draft'`` the function asserted only that a transition
to ``'issued'`` carried a non-NULL ``invoice_number`` and ``issued_at``, and then
``RETURN NEW``. So the row was frozen one instant AFTER the only moment at which
a figure could still be rewritten, and ONE statement::

    UPDATE billing_invoices
       SET status = 'issued', invoice_number = 'NA-2026-000001',
           issued_at = now(), total_usd = 999999
     WHERE id = '<draft id>';

produced a frozen, immutable, permanently unalterable statement of record
carrying a total no generator ever computed. Everything downstream then AGREED
with it: the row is ``issued``, so the void-only guard defends it from that
point on; ``services.invoices.reproduce`` reads the frozen row and nothing else
by design; and ``snapshot_sha256`` -- which DOES cover the money columns, see
``_money_from_snapshot`` in ``src/services/invoices.py`` -- disagrees with the
row forever, with no column anywhere recording when or why they diverged. The
guard's own module docstring already called a statement "a record, not a row";
the mint itself was the unguarded edge.

This migration ``CREATE OR REPLACE``s the function so the draft->issued
transition carries the SAME allowlist diff, over the four columns issuance
legitimately sets.

⚠️ WHY THE ALLOWLIST IS THOSE FOUR AND NOT "the money columns". Measured against
the only writer: ``services.invoices.issue`` assigns ``invoice_number``,
``status``, ``issued_at`` and ``issued_by``, in that block, and nothing else in
the tree performs a draft->issued UPDATE (``src/routers/billing.py``'s
``issue_invoice`` is its sole caller; every other ``status = 'issued'`` in
``src/`` is a read predicate or an INSERT). ``billing_invoices`` has no
``updated_at``/``onupdate`` column, so an ORM flush moves nothing else either. A
money-only check would have been a DENYLIST in its most tempting costume -- the
set of columns worth protecting on an invoice is not knowable in advance
(``snapshot``, ``inputs``, ``snapshot_sha256``, ``generator_version``,
``tenant_name``, ``period_start`` and the present-and-NULL legal set are each as
load-bearing as ``total_usd``) and a column added by a later migration would
arrive unguarded, silently, with nothing going red. The allowlist freezes
everything that does not exist yet, by default.

⚠️ draft->draft AND draft->void ARE DELIBERATELY UNCHANGED. A draft is working
state and must stay freely mutable -- ``tests/test_invoice_immutability.py::
test_the_same_update_against_a_draft_row_succeeds`` is the control that proves
this migration did not break that. draft->void keeps its freedom too, on a
narrower justification: nobody was ever shown the statement, so there is no
claim to have been laundered. Narrowing it is a separate decision.

⚠️ WHY THIS IS SAFE ON A LIVE DATABASE. ``CREATE OR REPLACE FUNCTION``, never
DROP: ``billing_invoice_immutability_trg`` on ``billing_invoices`` (0024) holds a
dependency on this function by OID, so dropping it would need the trigger
dropped and recreated -- a window in which ``billing_invoices`` is unguarded.
Replacing the body in place keeps the trigger attached throughout and needs no
ACCESS EXCLUSIVE lock on the table. No column is altered, no row is touched, no
other function is replaced. Existing ``issued`` and ``void`` rows are unaffected:
their branches are byte-identical to 0024's.

⚠️ DDL IS INLINED VERBATIM, per this tree's convention that a migration imports
nothing (no file under ``alembic/versions`` imports from ``models``). It was
GENERATED from ``src/models/db/invoice_immutability.py``, not retyped, and
``tests/test_invoice_immutability.py::test_migration_ddl_is_byte_identical_to_the_module``
asserts byte equality against THIS migration -- the NEWEST installer of the
function -- so the copy cannot rot. 0024 keeps its own pre-0030 copy untouched
and that test asserts it equals this file's ``DOWNGRADE_DDL``; read the test's
docstring before re-pointing it again.
"""

from __future__ import annotations

from alembic import op

revision = "0030_draft_to_issued_money_diff"
down_revision = "0029_audit_log_tamper_evidence"
branch_labels = None
depends_on = None


#: The post-0030 function body. Byte-identical to
#: ``models.db.invoice_immutability.INVOICE_IMMUTABILITY_FUNCTION_DDL``.
#:
#: ⚠️ The CREATE TRIGGER statements are NOT repeated here and must not be. 0024
#: attached ``billing_invoice_immutability_trg`` to ``billing_invoices`` and
#: nothing since has changed it; a trigger binds the function by name/OID, so
#: replacing the body is enough and re-issuing the attachment would only add a
#: DROP TRIGGER window where none is needed.
INVOICE_IMMUTABILITY_FUNCTION_DDL = r"""
CREATE OR REPLACE FUNCTION public.billing_forbid_invoice_mutation()
RETURNS trigger
LANGUAGE plpgsql
AS $fn$
DECLARE
    -- The row MINUS the allowlist for the transition in hand: the columns
    -- issuance may move in the draft branch, the columns a void may move in the
    -- issued branch. Everything else in the row is compared. A column added to
    -- billing_invoices later is guarded the moment it exists, without editing
    -- this function -- see the module docstring in
    -- src/models/db/invoice_immutability.py for why a per-column list was
    -- rejected. One pair of variables serves both branches because the branches
    -- are mutually exclusive on OLD.status and each assigns before it reads.
    v_old_frozen jsonb;
    v_new_frozen jsonb;
BEGIN
    -- ⛔ THIS FUNCTION BODY CARRIES NO PERCENT SIGN, DELIBERATELY. Every
    -- message is built with || and handed to RAISE as USING MESSAGE/DETAIL
    -- rather than through a RAISE format string. This DDL ships two ways:
    -- alembic's op.execute(), which passes the text to the server untouched,
    -- and exec_driver_sql() from the create_all hook in
    -- invoice_immutability.py, which crosses the psycopg3 boundary where a
    -- percent sign is a parameter placeholder and has to be doubled. With none
    -- in the body both paths carry byte-identical SQL, there is no escaping
    -- left to get wrong, and there is no way to land a "too many parameters
    -- specified for RAISE" -- a failure that would surface at VOID time, in
    -- production, rather than at install time. Do not reintroduce format() or
    -- a RAISE format string here.
    --
    -- coalesce() on every interpolated value for the same class of reason:
    -- '||' with a NULL operand yields NULL, and USING MESSAGE = NULL raises
    -- 22004 instead of the refusal the caller has to be able to read.
    IF TG_OP = 'DELETE' THEN
        RAISE EXCEPTION
            USING
                ERRCODE = 'P0001',
                MESSAGE =
                    'DELETE of billing invoice ' || coalesce(OLD.id::text, '<NULL>')
                    || ' (status ' || coalesce(OLD.status, '<NULL>') || ') is blocked',
                HINT =
                    'An issued statement is a record, not a row: it is the only '
                    'evidence that this amount was shown to this customer. Deleting '
                    'it leaves nothing behind that says it ever existed. VOID it '
                    'instead -- UPDATE billing_invoices SET status = ''void'', '
                    'void_reason = ''<why>'', voided_at = now(), voided_by = ''<who>'' '
                    'WHERE id = ''<id>''; -- then issue a replacement invoice.';
    END IF;

    -- A draft is working state: the generator writes it, an operator inspects
    -- it, it is re-rendered. Nothing to freeze yet -- UNLESS this is the UPDATE
    -- that leaves draft for 'issued', which is the act that mints the document
    -- and is therefore the one statement on a draft that has rules.
    IF OLD.status = 'draft' THEN
        IF NEW.status = 'issued' THEN
            IF NEW.invoice_number IS NULL OR NEW.issued_at IS NULL THEN
                RAISE EXCEPTION
                    USING
                        ERRCODE = 'P0001',
                        MESSAGE =
                            'invoice ' || coalesce(OLD.id::text, '<NULL>')
                            || ' cannot be issued without invoice_number and issued_at',
                        HINT =
                            'Issuing is the act that makes this row a document. Allocate '
                            'the next number from billing_invoice_counters in THIS '
                            'transaction (SELECT ... FOR UPDATE -- the series must stay '
                            'gapless) and set issued_at in the same UPDATE.';
            END IF;

            -- ⛔ ISSUANCE IS A STAMP, NOT A RESTATEMENT. THIS BRANCH USED TO
            -- RETURN NEW HERE, and that made the whole 'issued' branch below
            -- worth very little: it froze the row one instant AFTER the only
            -- moment at which a figure could still be rewritten, so ONE
            -- statement --
            --   UPDATE billing_invoices
            --      SET status = 'issued', invoice_number = 'NA-2026-000001',
            --          issued_at = now(), total_usd = <anything>
            --    WHERE status = 'draft'
            -- -- minted a frozen, immutable, permanently unalterable document
            -- carrying a total no generator ever computed. Everything downstream
            -- then agreed with it: the row is issued, so the void-only guard
            -- defends it, and snapshot_sha256 (which DOES cover the money
            -- columns -- see _money_from_snapshot in services/invoices.py)
            -- disagrees with the row forever with nothing on the row recording
            -- when or why they diverged.
            --
            -- The allowlist is the four columns services.invoices.issue writes
            -- and nothing else: status, invoice_number, issued_at, issued_by
            -- (invoices.py, in the block that allocates the number). Same jsonb
            -- shape as the void check below, for the same reason -- a column
            -- added to this table later is frozen across issuance the moment it
            -- exists, and opening one means editing this subtraction list.
            --
            -- ⚠️ An operator who genuinely needs a different figure edits the
            -- DRAFT and issues afterwards; both are permitted, in that order.
            -- The refusal is only of doing both in the same statement, which is
            -- the spelling under which the edit leaves no reviewable trace.
            v_old_frozen := to_jsonb(OLD) - 'status' - 'invoice_number' - 'issued_at' - 'issued_by';
            v_new_frozen := to_jsonb(NEW) - 'status' - 'invoice_number' - 'issued_at' - 'issued_by';

            IF v_old_frozen IS DISTINCT FROM v_new_frozen THEN
                RAISE EXCEPTION
                    USING
                        ERRCODE = 'P0001',
                        MESSAGE =
                            'issuing invoice ' || coalesce(OLD.id::text, '<NULL>')
                            || ' may not change any other column',
                        -- Both sides are to_jsonb() of the SAME table, so the
                        -- key sets are identical and comparing OLD's keys
                        -- against NEW's values finds every difference. IS
                        -- DISTINCT FROM, again, so a column that became NULL is
                        -- reported rather than read as unchanged.
                        DETAIL =
                            'changed: '
                            || (SELECT coalesce(string_agg(o.key, ', ' ORDER BY o.key), '(none)')
                                  FROM jsonb_each(v_old_frozen) AS o
                                 WHERE v_new_frozen -> o.key IS DISTINCT FROM o.value),
                        HINT =
                            'Issuing freezes the draft AS IT STANDS; it is not an '
                            'opportunity to restate it. Only status, invoice_number, '
                            'issued_at and issued_by may move in this UPDATE. To change '
                            'a figure: UPDATE the row while it is still a draft, then '
                            'issue it in a SEPARATE statement -- or, if it is already '
                            'issued, void it with a reason and issue a replacement. A '
                            'total that arrives in the same statement as the invoice '
                            'number was never produced by the generator whose output '
                            'snapshot_sha256 attests to.';
            END IF;
        END IF;

        -- draft -> draft (freely mutable working state) and draft -> void (a
        -- statement nobody was ever shown, abandoned) both land here. The
        -- latter is NOT tightened, deliberately: no claim was relied on, so
        -- there is nothing to have been laundered, and narrowing it is a
        -- separate decision from closing the issuance hole above. See the
        -- module docstring.
        RETURN NEW;
    END IF;

    -- Void is terminal. Reviving one would let a single invoice number describe
    -- two different amounts, with nothing on the row recording that it had.
    IF OLD.status = 'void' THEN
        RAISE EXCEPTION
            USING
                ERRCODE = 'P0001',
                MESSAGE =
                    'invoice ' || coalesce(OLD.id::text, '<NULL>')
                    || ' is void; void is terminal and admits no UPDATE',
                HINT =
                    'A voided statement is superseded by a NEW invoice for the same '
                    'period, never revived. ux_billing_invoices_open_period excludes '
                    'void rows precisely so the period can be re-issued.';
    END IF;

    IF OLD.status = 'issued' THEN
        IF NEW.status <> 'void' THEN
            RAISE EXCEPTION
                USING
                    ERRCODE = 'P0001',
                    MESSAGE =
                        'invoice ' || coalesce(OLD.id::text, '<NULL>')
                        || ' is issued; the only permitted change is to ''void'''
                        || ' (attempted status ' || coalesce(NEW.status, '<NULL>') || ')',
                    HINT =
                        'An issued statement has been shown to a customer and must '
                        'keep saying the same thing. To correct it: void this row '
                        'with a reason, then issue a replacement invoice.';
        END IF;

        -- ⚠️ btrim(NEW.void_reason) = '' WAS THE BUG. DO NOT SIMPLIFY BACK TO IT.
        -- Single-argument btrim() strips the SPACE character and nothing else --
        -- not tab, not newline, not U+00A0 -- so E'\t', E'\n' and U&'\00A0' all
        -- read as a "non-empty" reason, and an issued statement could be
        -- withdrawn with a justification that renders blank to every human who
        -- later asks why. The test is now "does this reason consist ENTIRELY of
        -- whitespace or invisible formatting characters".
        --
        -- The Unicode code points are listed explicitly ALONGSIDE [[:space:]]
        -- rather than relying on it: [[:space:]] resolves through the server's
        -- ctype table, glibc does not classify U+00A0 as space in every locale,
        -- and a control whose coverage depends on the database's LC_CTYPE is a
        -- control that is silently weaker on some hosts than on the one it was
        -- tested on. The listed set is Unicode White_Space plus the zero-width
        -- and BOM characters, which are not White_Space but are just as blank
        -- on a rendered invoice.
        IF NEW.void_reason IS NULL
           OR NEW.void_reason ~ '^[[:space:]\u0085\u00a0\u1680\u2000-\u200d\u2028\u2029\u202f\u205f\u2060\u3000\ufeff]*$' THEN
            RAISE EXCEPTION
                USING
                    ERRCODE = 'P0001',
                    MESSAGE =
                        'voiding invoice ' || coalesce(OLD.id::text, '<NULL>')
                        || ' requires a non-empty void_reason',
                    HINT =
                        'The reason is the whole audit value of a void: without it '
                        'the record says a statement was withdrawn and nothing about '
                        'why, which is what an auditor will ask. A reason that is '
                        'only whitespace is refused for exactly the same reason a '
                        'NULL one is -- it answers nothing.';
        END IF;

        IF NEW.voided_at IS NULL OR NEW.voided_by IS NULL THEN
            RAISE EXCEPTION
                USING
                    ERRCODE = 'P0001',
                    MESSAGE =
                        'voiding invoice ' || coalesce(OLD.id::text, '<NULL>')
                        || ' requires voided_at and voided_by',
                    HINT =
                        'WHEN and WHO, recorded on the row itself. Set voided_at = '
                        'now() and voided_by to the acting identity in the same '
                        'UPDATE that sets status.';
        END IF;

        v_old_frozen := to_jsonb(OLD) - 'status' - 'voided_at' - 'voided_by' - 'void_reason';
        v_new_frozen := to_jsonb(NEW) - 'status' - 'voided_at' - 'voided_by' - 'void_reason';

        IF v_old_frozen IS DISTINCT FROM v_new_frozen THEN
            RAISE EXCEPTION
                USING
                    ERRCODE = 'P0001',
                    MESSAGE =
                        'voiding invoice ' || coalesce(OLD.id::text, '<NULL>')
                        || ' may not change any other column',
                    -- Both sides are to_jsonb() of the SAME table, so the key
                    -- sets are identical and comparing OLD's keys against
                    -- NEW's values finds every difference. IS DISTINCT FROM,
                    -- again, so a column that became NULL is reported.
                    DETAIL =
                        'changed: '
                        || (SELECT coalesce(string_agg(o.key, ', ' ORDER BY o.key), '(none)')
                              FROM jsonb_each(v_old_frozen) AS o
                             WHERE v_new_frozen -> o.key IS DISTINCT FROM o.value),
                    HINT =
                        'A void withdraws a statement; it does not restate it. Only '
                        'status, void_reason, voided_at and voided_by may move. To '
                        'change a figure, void this row and issue a replacement.';
        END IF;

        RETURN NEW;
    END IF;

    -- Unreachable while ck_billing_invoices_status holds. Reached only if a
    -- future migration widens that CHECK without teaching this function what
    -- the new status means -- in which case refusing is the correct default:
    -- an unrecognised status must not inherit 'draft''s freedom.
    RAISE EXCEPTION
        USING
            ERRCODE = 'P0001',
            MESSAGE =
                'invoice ' || coalesce(OLD.id::text, '<NULL>')
                || ' carries unrecognised status ' || coalesce(OLD.status, '<NULL>'),
            HINT =
                'billing_forbid_invoice_mutation() knows draft, issued and void. A '
                'status added to ck_billing_invoices_status must be given an '
                'explicit branch here (and considered for '
                'ux_billing_invoices_open_period''s predicate) before it is used.';
END
$fn$;
"""

UPGRADE_DDL: tuple[str, ...] = (INVOICE_IMMUTABILITY_FUNCTION_DDL,)


#: The PRE-0030 function body, verbatim from alembic 0024, so ``downgrade()``
#: restores the definition that was actually running at 0029 rather than an
#: approximation of it.
#:
#: ⛔ A DOWNGRADE THAT DROPPED THE FUNCTION WOULD BE WRONG, not merely blunt: the
#: trigger 0024 created would be dropped with it (CASCADE) or the DROP would fail
#: (RESTRICT), so ``billing_invoices`` would come back from a downgrade either
#: unguarded or not at all. Replacing the body in place leaves the 0029-era
#: control exactly as it was -- including, knowingly, the draft->issued hole this
#: migration closes. That is what a downgrade MEANS; it is not an endorsement.
#:
#: ⚠️ This copy is asserted equal to 0024's own constant by
#: ``tests/test_invoice_immutability.py::test_migration_ddl_is_byte_identical_to_the_module``,
#: which is what makes "restores the 0029-era definition" a measurement rather
#: than a claim in a comment.
PRE_0030_INVOICE_IMMUTABILITY_FUNCTION_DDL = r"""
CREATE OR REPLACE FUNCTION public.billing_forbid_invoice_mutation()
RETURNS trigger
LANGUAGE plpgsql
AS $fn$
DECLARE
    -- The ALLOWLIST: the only columns a void may move. Everything else in the
    -- row is compared. A column added to billing_invoices later is guarded the
    -- moment it exists, without editing this function -- see the module
    -- docstring in src/models/db/invoice_immutability.py for why a per-column
    -- list was rejected.
    v_old_frozen jsonb;
    v_new_frozen jsonb;
BEGIN
    -- ⛔ THIS FUNCTION BODY CARRIES NO PERCENT SIGN, DELIBERATELY. Every
    -- message is built with || and handed to RAISE as USING MESSAGE/DETAIL
    -- rather than through a RAISE format string. This DDL ships two ways:
    -- alembic's op.execute(), which passes the text to the server untouched,
    -- and exec_driver_sql() from the create_all hook in
    -- invoice_immutability.py, which crosses the psycopg3 boundary where a
    -- percent sign is a parameter placeholder and has to be doubled. With none
    -- in the body both paths carry byte-identical SQL, there is no escaping
    -- left to get wrong, and there is no way to land a "too many parameters
    -- specified for RAISE" -- a failure that would surface at VOID time, in
    -- production, rather than at install time. Do not reintroduce format() or
    -- a RAISE format string here.
    --
    -- coalesce() on every interpolated value for the same class of reason:
    -- '||' with a NULL operand yields NULL, and USING MESSAGE = NULL raises
    -- 22004 instead of the refusal the caller has to be able to read.
    IF TG_OP = 'DELETE' THEN
        RAISE EXCEPTION
            USING
                ERRCODE = 'P0001',
                MESSAGE =
                    'DELETE of billing invoice ' || coalesce(OLD.id::text, '<NULL>')
                    || ' (status ' || coalesce(OLD.status, '<NULL>') || ') is blocked',
                HINT =
                    'An issued statement is a record, not a row: it is the only '
                    'evidence that this amount was shown to this customer. Deleting '
                    'it leaves nothing behind that says it ever existed. VOID it '
                    'instead -- UPDATE billing_invoices SET status = ''void'', '
                    'void_reason = ''<why>'', voided_at = now(), voided_by = ''<who>'' '
                    'WHERE id = ''<id>''; -- then issue a replacement invoice.';
    END IF;

    -- A draft is working state: the generator writes it, an operator inspects
    -- it, it is re-rendered. Nothing to freeze yet.
    IF OLD.status = 'draft' THEN
        IF NEW.status = 'issued'
           AND (NEW.invoice_number IS NULL OR NEW.issued_at IS NULL) THEN
            RAISE EXCEPTION
                USING
                    ERRCODE = 'P0001',
                    MESSAGE =
                        'invoice ' || coalesce(OLD.id::text, '<NULL>')
                        || ' cannot be issued without invoice_number and issued_at',
                    HINT =
                        'Issuing is the act that makes this row a document. Allocate '
                        'the next number from billing_invoice_counters in THIS '
                        'transaction (SELECT ... FOR UPDATE -- the series must stay '
                        'gapless) and set issued_at in the same UPDATE.';
        END IF;
        RETURN NEW;
    END IF;

    -- Void is terminal. Reviving one would let a single invoice number describe
    -- two different amounts, with nothing on the row recording that it had.
    IF OLD.status = 'void' THEN
        RAISE EXCEPTION
            USING
                ERRCODE = 'P0001',
                MESSAGE =
                    'invoice ' || coalesce(OLD.id::text, '<NULL>')
                    || ' is void; void is terminal and admits no UPDATE',
                HINT =
                    'A voided statement is superseded by a NEW invoice for the same '
                    'period, never revived. ux_billing_invoices_open_period excludes '
                    'void rows precisely so the period can be re-issued.';
    END IF;

    IF OLD.status = 'issued' THEN
        IF NEW.status <> 'void' THEN
            RAISE EXCEPTION
                USING
                    ERRCODE = 'P0001',
                    MESSAGE =
                        'invoice ' || coalesce(OLD.id::text, '<NULL>')
                        || ' is issued; the only permitted change is to ''void'''
                        || ' (attempted status ' || coalesce(NEW.status, '<NULL>') || ')',
                    HINT =
                        'An issued statement has been shown to a customer and must '
                        'keep saying the same thing. To correct it: void this row '
                        'with a reason, then issue a replacement invoice.';
        END IF;

        -- ⚠️ btrim(NEW.void_reason) = '' WAS THE BUG. DO NOT SIMPLIFY BACK TO IT.
        -- Single-argument btrim() strips the SPACE character and nothing else --
        -- not tab, not newline, not U+00A0 -- so E'\t', E'\n' and U&'\00A0' all
        -- read as a "non-empty" reason, and an issued statement could be
        -- withdrawn with a justification that renders blank to every human who
        -- later asks why. The test is now "does this reason consist ENTIRELY of
        -- whitespace or invisible formatting characters".
        --
        -- The Unicode code points are listed explicitly ALONGSIDE [[:space:]]
        -- rather than relying on it: [[:space:]] resolves through the server's
        -- ctype table, glibc does not classify U+00A0 as space in every locale,
        -- and a control whose coverage depends on the database's LC_CTYPE is a
        -- control that is silently weaker on some hosts than on the one it was
        -- tested on. The listed set is Unicode White_Space plus the zero-width
        -- and BOM characters, which are not White_Space but are just as blank
        -- on a rendered invoice.
        IF NEW.void_reason IS NULL
           OR NEW.void_reason ~ '^[[:space:]\u0085\u00a0\u1680\u2000-\u200d\u2028\u2029\u202f\u205f\u2060\u3000\ufeff]*$' THEN
            RAISE EXCEPTION
                USING
                    ERRCODE = 'P0001',
                    MESSAGE =
                        'voiding invoice ' || coalesce(OLD.id::text, '<NULL>')
                        || ' requires a non-empty void_reason',
                    HINT =
                        'The reason is the whole audit value of a void: without it '
                        'the record says a statement was withdrawn and nothing about '
                        'why, which is what an auditor will ask. A reason that is '
                        'only whitespace is refused for exactly the same reason a '
                        'NULL one is -- it answers nothing.';
        END IF;

        IF NEW.voided_at IS NULL OR NEW.voided_by IS NULL THEN
            RAISE EXCEPTION
                USING
                    ERRCODE = 'P0001',
                    MESSAGE =
                        'voiding invoice ' || coalesce(OLD.id::text, '<NULL>')
                        || ' requires voided_at and voided_by',
                    HINT =
                        'WHEN and WHO, recorded on the row itself. Set voided_at = '
                        'now() and voided_by to the acting identity in the same '
                        'UPDATE that sets status.';
        END IF;

        v_old_frozen := to_jsonb(OLD) - 'status' - 'voided_at' - 'voided_by' - 'void_reason';
        v_new_frozen := to_jsonb(NEW) - 'status' - 'voided_at' - 'voided_by' - 'void_reason';

        IF v_old_frozen IS DISTINCT FROM v_new_frozen THEN
            RAISE EXCEPTION
                USING
                    ERRCODE = 'P0001',
                    MESSAGE =
                        'voiding invoice ' || coalesce(OLD.id::text, '<NULL>')
                        || ' may not change any other column',
                    -- Both sides are to_jsonb() of the SAME table, so the key
                    -- sets are identical and comparing OLD's keys against
                    -- NEW's values finds every difference. IS DISTINCT FROM,
                    -- again, so a column that became NULL is reported.
                    DETAIL =
                        'changed: '
                        || (SELECT coalesce(string_agg(o.key, ', ' ORDER BY o.key), '(none)')
                              FROM jsonb_each(v_old_frozen) AS o
                             WHERE v_new_frozen -> o.key IS DISTINCT FROM o.value),
                    HINT =
                        'A void withdraws a statement; it does not restate it. Only '
                        'status, void_reason, voided_at and voided_by may move. To '
                        'change a figure, void this row and issue a replacement.';
        END IF;

        RETURN NEW;
    END IF;

    -- Unreachable while ck_billing_invoices_status holds. Reached only if a
    -- future migration widens that CHECK without teaching this function what
    -- the new status means -- in which case refusing is the correct default:
    -- an unrecognised status must not inherit 'draft''s freedom.
    RAISE EXCEPTION
        USING
            ERRCODE = 'P0001',
            MESSAGE =
                'invoice ' || coalesce(OLD.id::text, '<NULL>')
                || ' carries unrecognised status ' || coalesce(OLD.status, '<NULL>'),
            HINT =
                'billing_forbid_invoice_mutation() knows draft, issued and void. A '
                'status added to ck_billing_invoices_status must be given an '
                'explicit branch here (and considered for '
                'ux_billing_invoices_open_period''s predicate) before it is used.';
END
$fn$;
"""

DOWNGRADE_DDL: tuple[str, ...] = (PRE_0030_INVOICE_IMMUTABILITY_FUNCTION_DDL,)


def upgrade() -> None:
    for stmt in UPGRADE_DDL:
        op.execute(stmt)


def downgrade() -> None:
    for stmt in DOWNGRADE_DDL:
        op.execute(stmt)
