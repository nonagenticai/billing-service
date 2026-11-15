"""Repair the ``license_sync`` budgets keyed on a legacy tenant NAME, and make the
licence-scoped uniqueness the write path has always assumed structurally true.

WHAT IS BROKEN
--------------
``billing.token_budgets`` rows with ``source='license_sync'`` are written by the
licences service through ``POST /api/budget`` →
``services.budget_crud.upsert_license_sync_budget``. THREE ACTIVE rows carry the
legacy tenant NAME in ``tenant_id`` where the licences service now carries the
tenant UUID:

    id                                    tenant_id   license_id  consumed
    019dcaf3-6782-7176-b801-188012fe7dfe  portal      a005bf37-…  0
    019dcaf3-67c0-7456-bf6c-2873c987059a  n8n-test    9d96dc27-…  0
    019dcaf3-69ef-7293-9ba3-8e3327506c0d  gerard-dev  019dbfa3-…  12

The set is DERIVED FROM THE CORPUS, not hand-listed: every ``token_budgets`` row
whose ``tenant_id`` does not match the UUID shape

    SELECT id, tenant_id, license_id, period, source, active, consumed_tokens
      FROM token_budgets
     WHERE tenant_id !~ '^[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-…$'

cross-checked against ``licenses.license`` for the tenant of record. An earlier
draft of this migration said "two rows"; that was a hand-listed scope and it was
undercounted — ``gerard-dev`` was missed because its licence is ``status='revoked'``
upstream, and it is the ONLY one of the three carrying consumption.

Tenants of record (``licenses.license.tenant_id`` → ``licenses.tenants.name``):
``019de842-c92b-7564-b932-98240d274b3b`` = portal, ``019de842-c92b-752e-b39c-30d64950e1cc``
= n8n-test, ``019de842-c92b-74cd-a7ce-848302bce3b2`` = gerard-dev.

DELIBERATELY OUT OF SCOPE (measured, not overlooked)
----------------------------------------------------
* ~110 INACTIVE ``license_sync`` rows on legacy names (``n8n-dev``, ``portal``,
  ``gerard-dev``, ``n8n-test``, ``e2e-smoke-…``). ``active=false`` puts them
  outside the partial index and outside every read path; they are history.
* TWO ACTIVE ``source='manual'`` rows on the legacy name ``n8n-dev``
  (``019d30c3-731a-7476-a702-2dfe997c05b4`` consumed 48,929 and
  ``019d3076-2a1f-739b-93e0-e18604846447`` consumed 36,228, both cap 100,000).
  They are NOT written by the licences reconcile, are NOT covered by the index
  below, and carry real consumption an operator may still want to read under the
  old name. Repairing them is a separate, human decision — but note they are
  unreachable from a UUID-keyed ``GET /api/budget/list/{tenant}`` in exactly the
  same way, so the class is NOT fully closed by this migration.

WHAT THE DAMAGE ACTUALLY IS (and what it is NOT)
------------------------------------------------
The licences metering reconcile reads coverage via
``GET /api/budget/list/{tenant}`` keyed on the LICENCES-side tenant, so it cannot
see these rows and its upsert misses into an INSERT — a second active
``license_sync`` row for the same licence under the UUID tenant.

That does NOT double the entitlement, and any claim that it "stacks to 20M" is
REFUTED:

  * ``services.budget_debit._select_active_budgets`` filters
    ``TokenBudget.tenant_id == tenant_id`` FIRST, so two rows sitting under two
    different tenant strings can never appear in the same cascade, let alone the
    same sum.
  * the Redis enforcement key is ``budget:limit:{license_id}:{period}``
    (``services.budget_redis``) — LICENCE-scoped, not row-scoped — so a second
    write lands on the same key and overwrites at the same 10,000,000.

The real damage is double-bookkeeping: two rows of record for one licence's
entitlement, ``consumed_tokens`` accumulating on at most one of them, and every
per-tenant budget listing, dashboard and audit trail disagreeing about which is
the truth. The stale row is unreachable from every TENANT-KEYED path — it can
never be debited (``_select_active_budgets``), listed, or repaired by the
reconcile. It is NOT unreachable from every path: an earlier draft of this
docstring claimed it "can never be deactivated by any code path", which is false.
``budget_crud.deactivate_orphan_license_sync_budgets`` filters only
``source``/``active``/``license_id`` — it is tenant-agnostic and does reach these
rows. That is in fact the expected fate of the ``gerard-dev`` row, whose licence
is already ``revoked`` upstream; repairing its tenant first means the eventual
``budget.deactivate_orphan`` audit row is attributed to the right tenant.

WHY THE STRUCTURAL GUARD IS LICENCE-KEYED
-----------------------------------------
A licence belongs to exactly one tenant, and the enforcement key is already
licence-scoped. A per-TENANT uniqueness key is therefore the wrong shape — it is
precisely what let these rows hide, because the same licence under two different
tenant strings satisfies it. (Measured: prod already has two active
``license_sync`` rows under tenant ``019de842-c92b-752e-b39c-30d64950e1cc`` for
two DIFFERENT licences, which a tenant-keyed index would have to allow anyway.)
The partial unique index below is keyed on ``(license_id, period)`` and applies
only to active ``license_sync`` rows; ``source='manual'``/``'topup'`` keep
stacking by design, and the nil-UUID sentinel is excluded because
``NULL_LICENSE_ID`` marks TENANT-WIDE budgets, of which a tenant may legitimately
hold several.

⚠️ The index is only safe because ``upsert_license_sync_budget`` was changed in
the same commit to look its row up on ``(license_id, period, source, active)`` —
the SAME key the index enforces — and to recover from a concurrent insert via a
SAVEPOINT. Before that change, a licence whose tenant changed upstream (exactly
the event that created this bug) missed the tenant-keyed SELECT, INSERTed, and
raised ``IntegrityError``; ``routers/budget.py`` catches only
``httpx.RequestError``, so it would surface as a 500 that rolls back every budget
and quota write of the whole reconcile run. Do not land this index without that
write-path fix.

Measured on prod 2026-08-10: 9 active ``license_sync`` rows over 9 DISTINCT
license_ids, 0 duplicate ``(license_id, period)`` groups, 195 rows in the table —
so the index creates clean, and a plain (non-CONCURRENT) CREATE INDEX inside the
alembic transaction is appropriate at this size.

FAIL LOUD ON DIVERGENCE, NO-OP ON ABSENCE
------------------------------------------
Guard A accepts exactly 0 (nothing to repair) or exactly 3 (the measured state)
and aborts on anything between — a partially-repaired prod is a state this file
can no longer derive a correct repair for, and guessing is worse than stopping.

The 0 arm is load-bearing, not a loophole: ``alembic upgrade head`` must succeed
against an EMPTY database or CI (which runs it against an ephemeral
``postgres:18`` before any test) goes red, and billing's ``migrate`` init
container would CrashLoop forever on any fresh, staging or DR-restored DB. Note
this is the same shape as ``0007_quota_tenant_not_null``, whose guard is
``IF n > 0 THEN RAISE`` — also a no-op on clean data. An earlier draft of this
file used ``IF n <> 2``, which had no such arm and would have bricked every fresh
deploy.

The UPDATEs are additionally keyed on the FULL stale tuple (not just the PK), so
they are self-limiting: a divergent row is a no-op rather than a silent
overwrite, and on an empty database they simply match nothing.

Revision ID: 0010_license_sync_tenant_repair
Revises: 0009_budget_period_anchor
Create Date: 2026-08-10

(Revision id kept <=32 chars — ``alembic_version.version_num`` is varchar(32).)
"""

from __future__ import annotations

from alembic import op

revision = "0010_license_sync_tenant_repair"
down_revision = "0009_budget_period_anchor"
branch_labels = None
depends_on = None

# (budget id, stale tenant NAME, licence id, tenant UUID of record).
# tenant_name keeps the human name — it is what dashboards and the
# ix_token_budgets_tenant_name index read, and it matches licenses.tenants.name.
REPAIRS: tuple[tuple[str, str, str, str], ...] = (
    (
        "019dcaf3-6782-7176-b801-188012fe7dfe",
        "portal",
        "a005bf37-ee63-43cc-b58a-6d2e62a82e4e",
        "019de842-c92b-7564-b932-98240d274b3b",
    ),
    (
        "019dcaf3-67c0-7456-bf6c-2873c987059a",
        "n8n-test",
        "9d96dc27-2a27-4567-8b5d-804f36a117dc",
        "019de842-c92b-752e-b39c-30d64950e1cc",
    ),
    (
        "019dcaf3-69ef-7293-9ba3-8e3327506c0d",
        "gerard-dev",
        "019dbfa3-2dae-70d1-9bd4-cea20fe2c5e2",
        "019de842-c92b-74cd-a7ce-848302bce3b2",
    ),
)

NIL_LICENSE_ID = "00000000-0000-0000-0000-000000000000"
UNIQUE_INDEX = "uq_token_budgets_license_sync_active"

_STALE_TUPLES = ",\n                    ".join(
    f"('{bid}'::uuid, '{stale}', '{lic}'::uuid)" for bid, stale, lic, _ in REPAIRS
)
_LICENCE_LIST = ", ".join(f"'{lic}'::uuid" for _, _, lic, _ in REPAIRS)
_EXPECTED = len(REPAIRS)


def upgrade() -> None:
    # --- Guards ------------------------------------------------------------
    # A: the measured stale set must be intact (3) or already gone (0).
    # B: only reached when there IS something to repair — exactly one active
    #    monthly license_sync row per affected licence. More means the licences
    #    reconcile already ran against the UUID tenant and inserted the
    #    duplicate this migration exists to prevent; picking a survivor is an
    #    operator decision (consumed_tokens differ), not a migration's guess.
    #    Scoped to period='monthly' to match the index grain exactly — a
    #    legitimate weekly license_sync budget is not a duplicate.
    op.execute(
        f"""
        DO $$
        DECLARE
            stale bigint;
            active_rows bigint;
        BEGIN
            SELECT count(*) INTO stale
            FROM token_budgets
            WHERE source = 'license_sync'
              AND active
              AND period = 'monthly'
              AND (id, tenant_id, license_id) IN (
                    {_STALE_TUPLES}
              );

            IF stale NOT IN (0, {_EXPECTED}) THEN
                RAISE EXCEPTION
                    'token_budgets has % row(s) matching the stale license_sync '
                    'tuple, expected 0 (nothing to repair) or {_EXPECTED} (the '
                    '2026-08-10 measurement) — prod has diverged from the state '
                    'this migration was derived from. Re-derive the affected '
                    'rows (SELECT ... WHERE tenant_id !~ the UUID shape) against '
                    'the licences service before applying 0010; refusing to '
                    'blind-fill a tenant-of-record column', stale;
            END IF;

            IF stale = 0 THEN
                -- Fresh / CI / DR-restored database, or an already-repaired
                -- prod. Nothing to repair; the index below still applies.
                RETURN;
            END IF;

            SELECT count(*) INTO active_rows
            FROM token_budgets
            WHERE source = 'license_sync'
              AND active
              AND period = 'monthly'
              AND license_id IN ({_LICENCE_LIST});

            IF active_rows <> {_EXPECTED} THEN
                RAISE EXCEPTION
                    'expected exactly {_EXPECTED} active monthly license_sync '
                    'budgets across the affected licences, found % — the '
                    'licences reconcile sweep has already duplicated these '
                    'rows. Deactivate the surplus row(s) by hand (keep the one '
                    'carrying consumed_tokens) before applying 0010', active_rows;
            END IF;
        END $$
        """
    )

    # --- The repair --------------------------------------------------------
    # Keyed on the PK *and* the full stale tuple: a no-op if anything differs,
    # and a no-op on an empty database.
    for budget_id, stale_tenant, license_id, tenant_uuid in REPAIRS:
        op.execute(
            f"""
            UPDATE token_budgets
               SET tenant_id = '{tenant_uuid}',
                   tenant_name = '{stale_tenant}',
                   updated_at = now()
             WHERE id = '{budget_id}'::uuid
               AND tenant_id = '{stale_tenant}'
               AND license_id = '{license_id}'::uuid
               AND source = 'license_sync'
               AND active
               AND period = 'monthly'
            """
        )

    # --- The structural guard ----------------------------------------------
    op.execute(
        f"""
        CREATE UNIQUE INDEX {UNIQUE_INDEX}
            ON token_budgets (license_id, period)
         WHERE source = 'license_sync' AND active
           AND license_id <> '{NIL_LICENSE_ID}'::uuid
        """
    )


def downgrade() -> None:
    op.execute(f"DROP INDEX IF EXISTS {UNIQUE_INDEX}")
    # Guarded exactly like upgrade(): keyed on the PK AND the state upgrade()
    # left behind. An unguarded "UPDATE ... WHERE id = ..." would blind-stamp a
    # legacy tenant name onto whatever row now holds that PK — the very thing
    # the upgrade guards refuse to do.
    for budget_id, stale_tenant, license_id, tenant_uuid in REPAIRS:
        op.execute(
            f"""
            UPDATE token_budgets
               SET tenant_id = '{stale_tenant}',
                   tenant_name = '{stale_tenant}',
                   updated_at = now()
             WHERE id = '{budget_id}'::uuid
               AND tenant_id = '{tenant_uuid}'
               AND license_id = '{license_id}'::uuid
               AND source = 'license_sync'
               AND active
               AND period = 'monthly'
            """
        )
