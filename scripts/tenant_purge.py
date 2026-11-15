"""Sanctioned, audited deletion of billing history (and optionally config) by tenant.

⛔ THIS IS THE ONLY SANCTIONED WAY TO DELETE BILLING ROWS BY TENANT.
Before it existed the only precedent was an operator ``psql`` session running a
whole-ledger ``TRUNCATE`` (R16, 2026-09-17, ``billing_delete_audit`` ids 17-33),
which cannot be tenant-scoped, and an unexplained one (2026-09-05, ids 1-14)
that nobody can attribute. This script exists so the next deletion is scoped,
dry-run first, reasoned, and leaves a receipt.

WHAT IT NEVER DOES
------------------
* No ``TRUNCATE``, ``DROP``, ``ALTER``, trigger disable, or GUC hatch. It never
  sets ``billing.allow_truncate`` / ``billing.allow_unrolled_*``. Every guard
  installed by alembic 0016/0017/0018/0019/0023/0024/0029 stays armed and is
  exercised, not bypassed: row DELETEs on the two event ledgers and on
  ``audit_log`` are recorded by their statement triggers into
  ``billing_delete_audit`` with the ``--reason`` this script sets via
  ``set_config('billing.delete_reason', <reason>, true)`` (transaction-local).
* Never deletes ``billing_invoices``. 0024 refuses that with no escape hatch.
  ``--void-invoices`` voids them instead, with exactly the state transition
  ``services.invoices.void`` makes (status='void', void_reason, voided_at,
  voided_by, after a ``SELECT ... FOR UPDATE``), the same prepaid-credit lock and
  ``drawdown_reversal`` ledger entry ``void()`` writes (alembic 0055), plus the
  ``audit_log`` row that ``POST /invoices/{id}/void`` writes
  (``routers/billing.py:809-845``).
* Never connects through pgbouncer. Transaction pooling hands a session GUC to
  the next client; the delete reason must live and die with this transaction on
  a DIRECT connection. A DSN whose host contains ``pgbouncer`` (or whose port is
  6432, pgbouncer's port in this estate) exits 2 before connecting.

MODES
-----
Without ``--execute`` the run is a DRY-RUN: ONE ``REPEATABLE READ READ ONLY``
transaction that only SELECTs. It writes nothing to the database — not even
audit rows — and the receipt records ``transaction_read_only = on`` as read back
from the server.

With ``--execute`` everything happens in ONE ``REPEATABLE READ`` transaction:
voids, then per-table DELETEs in a fixed order. Every table's counts are
cross-checked inside the transaction (deleted == eligible, after == before -
deleted, rows NOT eligible unchanged, eligible remaining == 0). Any mismatch or
any statement error ROLLS BACK EVERYTHING. A concurrent writer touching a row
this transaction reads raises a serialization failure, which is also a full
rollback — re-run it.

SCOPE
-----
Exactly one of ``--tenants id1,id2`` or ``--all-tenants``. ``--all-tenants``
applies NO tenant predicate at all, so rows whose tenant column is NULL
(``audit_log.tenant_id`` is nullable) are in scope too.

"Before the cutoff" per table (see ``ALLOWLIST``):

* ``instant``  — ``<ts col> < cutoff``.
* ``hour``/``day``/``month`` buckets — the bucket must END at or before the
  cutoff. A bucket straddling the cutoff is KEPT, because it also aggregates
  rows at/after the cutoff that this run keeps. Buckets are UTC.
* ``token_usage`` only has ``period_type = 'monthly'`` writers; a row of any
  other period_type is never eligible (kept, and visible in the receipt).
* ``billing_metering_monthly_usage`` (the monthly request-quota counter,
  ``services/quota.py:621``) has NO tenant column: it is keyed by
  ``license_id`` + ``subject_id`` and its period is ``month_start`` (a DATE, the
  first of the UTC month, ``timeutil.current_month_start``). Under
  ``--all-tenants`` a month is deleted when it ENDS at or before the cutoff, the
  same bucket rule as the other rollups; the current month straddles any cutoff
  in it and is kept, so live quota enforcement (which reads only the current
  month) is untouched. Under ``--tenants`` see UNRESOLVABLE TABLES.

UNRESOLVABLE TABLES (``--tenants``)
-----------------------------------
A table with no tenant column cannot be scoped to tenants unless billing owns an
AUTHORITATIVE licence->tenant mapping, and it does not: ``request_quota_config``
is a current-state MIRROR of the licence registry, pruned by
``services/budget_crud.delete_absent_quota_configs`` and point-in-time blind
(``services/license_resolver._build_owner_map``), and
``billing_metering_events.license_id``/``tenant_id`` is a per-event derived
stamp that is itself purged by this script. Guessing through either could
delete another tenant's history or silently leave the scoped tenant's. So with
``--tenants``, if such a table has ANY row before the cutoff (for any licence),
the run ABORTS (exit 3). With zero such rows there is nothing to leave behind
and the run proceeds; the table is reported with 0 rows in scope.

``--include-config`` adds ``token_budgets``, ``request_quota_config`` and
``audit_log``. All three have a usable timestamp, so the semantics are "rows
CREATED before the cutoff" (``created_at`` for the two config tables, ``ts`` for
audit_log). With the cutoff set to the approval time that is every config row
that existed when the deletion was approved; rows created after it survive.

⛔ An ACTIVE ``token_budgets`` row is never deleted, whatever its age (#244: the
2026-09-30 run deleted netcare's active EUR 250 budget). It is not eligible,
stays in the kept set, and is reported per table as ``active_skipped``.
Deactivate it through the API first if it really must go. Since alembic 0046
``token_budgets`` and ``request_quota_config`` DELETEs are recorded in
``billing_delete_audit`` like the ledgers'.

UNKNOWN-TABLE GUARD
-------------------
Silent incompleteness is the failure mode this prevents. At start the script
lists every base/partitioned table (partitions excluded — they are covered by
their parent) in the current schema having any IDENTITY column: a name matching
``%tenant%``, or one of ``IDENTITY_COLUMNS`` (licence, subject, app, calling
client, actor, project — see ``_identity_keyed_tables``). A ``%tenant%``-only
guard missed ``billing_metering_monthly_usage``, which is keyed by
``license_id``/``subject_id``. Each such table must be in ``ALLOWLIST`` or
``EXCLUDED`` (with a reason); otherwise the run ABORTS (exit 3) before any
statement that could write. Each allowlisted table's declared tenant/timestamp
columns must exist. Any foreign key REFERENCING a
table about to be purged also aborts (none exist at 0042: the only FK in the
schema is ``billing_cost_events.pricing_id -> provider_pricing``).

WHAT DELETING CONFIG DOES AT REQUEST TIME (measured in code, 1270eae)
---------------------------------------------------------------------
See ``CONFIG_ABSENCE_NOTES`` — copied verbatim into every receipt.

Usage
-----
    BILLING_PURGE_DSN='postgresql://billing_user:…@<direct primary host>:5432/billing' \\
      python scripts/tenant_purge.py \\
        --cutoff 2026-09-30T12:00:00Z --reason 'TENANCY Q4 …' --actor george-rulings \\
        --all-tenants --include-config --void-invoices \\
        --receipt ~/.claude/plans/george-rulings-scratch/purge-dry-run.json
    # review the receipt, then the same command with --execute and a new --receipt

Exit codes: 0 success; 2 refused (arguments, DSN, receipt path); 3 aborted by a
guard or a count cross-check (rolled back); 1 unexpected database error (rolled
back). A receipt is written for 0, 3 and 1.
"""

from __future__ import annotations

import argparse
import json
import os
import sys
import uuid
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

import psycopg
from psycopg import sql
from psycopg.conninfo import conninfo_to_dict
from psycopg.types.json import Jsonb

APPLICATION_NAME = "billing-tenant-purge"
DELETE_REASON_GUC = "billing.delete_reason"

EXIT_OK = 0
EXIT_ERROR = 1
EXIT_REFUSED = 2
EXIT_ABORTED = 3


@dataclass(frozen=True)
class PurgeTable:
    """One allowlisted table: where its tenant and its time live.

    ``ts_kind`` decides what "before the cutoff" means (module docstring, SCOPE).
    Column names were read from ``src/models/db/*.py`` and confirmed against an
    alembic-``head`` schema; ``_verify_columns`` re-checks them on every run.
    ``tenant_col=None`` means the table has no tenant column (``keyed_by`` names
    what it IS keyed by) and cannot be scoped by ``--tenants`` (UNRESOLVABLE
    TABLES in the module docstring).
    """

    name: str
    tenant_col: str | None
    ts_col: str
    ts_kind: str  # instant | hour | day | month | month_start
    config: bool = False  # only with --include-config
    keyed_by: tuple[str, ...] = ()
    #: Never delete a row whose ``active`` is true, whatever its age (#244: the
    #: 2026-09-30 run deleted netcare's ACTIVE budget). Such rows are not
    #: eligible; the receipt counts them as ``active_skipped``.
    protect_active: bool = False


#: FK-safe order. No FK references any of these tables (verified at run time by
#: ``_referencing_fks``), so order is chosen for legibility: raw ledgers, then
#: the rollups derived from them, then usage/alerts, then config, audit_log last.
ALLOWLIST: tuple[PurgeTable, ...] = (
    PurgeTable("billing_cost_events", "tenant_id", "created_at", "instant"),
    PurgeTable("billing_metering_events", "tenant_id", "created_at", "instant"),
    PurgeTable("billing_cost_daily", "tenant_id", "bucket_date", "day"),
    PurgeTable("billing_cost_actor_daily", "tenant_id", "bucket_date", "day"),
    PurgeTable("billing_cost_usage_daily", "tenant_id", "bucket_date", "day"),
    PurgeTable("billing_metering_daily", "tenant_id", "bucket_date", "day"),
    PurgeTable("billing_metering_hourly", "tenant_id", "bucket_hour", "hour"),
    PurgeTable(
        "billing_metering_monthly_usage",
        None,
        "month_start",
        "month_start",
        keyed_by=("license_id", "subject_id"),
    ),
    PurgeTable("token_usage", "tenant_id", "period_start", "month"),
    PurgeTable("budget_overage_alerts", "tenant_id", "dispatched_at", "instant"),
    PurgeTable(
        "token_budgets", "tenant_id", "created_at", "instant", config=True, protect_active=True
    ),
    PurgeTable("request_quota_config", "tenant_id", "created_at", "instant", config=True),
    PurgeTable("audit_log", "tenant_id", "ts", "instant", config=True),
)

#: Identity columns besides ``%tenant%`` that make a table per-customer data.
#: ``plan_id`` (a product), ``app_key`` (the app catalogue's global rate row) and
#: the ``processed_*`` dedupe keys are deliberately NOT identities.
IDENTITY_COLUMNS: tuple[str, ...] = (
    "license_id",
    "subject_id",
    "app_id",
    "caller_client_id",
    "actor_id",
    "actor_sub",
    "project_id",
)

#: Identity-keyed tables that this script deliberately never deletes.
EXCLUDED: dict[str, str] = {
    "billing_invoices": (
        "issued statements are records; alembic 0024 refuses DELETE with no hatch. "
        "--void-invoices voids them instead"
    ),
    "billing_delete_audit": (
        "the forensic record of deletes (alembic 0016) — including THIS run's; "
        "purging it would destroy the receipt of the purge"
    ),
    "billing_dlq_reattributions": (
        "receipts for operator DLQ re-attribution (alembic 0047); each holds the only "
        "copy of a quarantine entry that was XDELed after it was written"
    ),
    "tenant_id_divergence_audit": (
        "INSERT-only forensic ledger of tenant_id divergence (alembic 0039), not tenant history"
    ),
    "tenant_id_crosswalk": (
        "alembic 0011 legacy-label -> uuid mapping; the only way to read pre-0011 "
        "values, not per-tenant data"
    ),
    "app_registry": (
        "mirror of the licences app catalogue (alembic 0050), keyed by app_id; global "
        "registry data, not per-tenant history. Refreshed by sync-from-licenses"
    ),
    "billing_attribution_rules": (
        "source->bill tenant routing config; not in the operator's config set"
    ),
    "markup_rules": "pricing config; not in the operator's config set",
    "tenant_access_charge": "pricing config; not in the operator's config set",
    "tenant_config": "tenant config; not in the operator's config set",
    "tenant_money_budget": (
        "EUR money budget config (alembic 0044); not in the operator's config set"
    ),
    "tenant_money_budget_adjustment": (
        "money-budget currency-conversion ledger (alembic 0045); the audited history "
        "of tenant_money_budget, which is itself excluded"
    ),
    "tenant_credit_ledger": (
        "PREPAID CREDIT ledger (alembic 0055): invoiced top-ups and the drawdowns "
        "frozen by issued statements; deleting it would re-grant credit already spent"
    ),
    "tenant_webhooks": "tenant webhook config; not in the operator's config set",
}

#: Delete-audit triggers. A table carrying none has NO record of a row DELETE
#: except the counts in this script's receipt. ``billing_delete_audit_trg`` is
#: on the ledgers (0016) and token_budgets (0046);
#: ``billing_quota_config_delete_audit_trg`` is request_quota_config's (0046).
_DELETE_AUDIT_TRIGGERS = (
    "billing_delete_audit_trg",
    "billing_audit_log_delete_audit_trg",
    "billing_quota_config_delete_audit_trg",
)

CONFIG_ABSENCE_NOTES: dict[str, str] = {
    "request_quota_config_absent": (
        "NOT a reject and NOT a default quota: routers/quota.py:120-135 falls back to "
        "the CALLER-SUPPLIED quota_monthly query param when no row exists "
        "(`limit = config.quota_monthly if config is not None else quota_monthly`); "
        "license-authz sends none, so limit is None, and services/quota.py:551-560 "
        "treats None as UNLIMITED (remaining=None, never over_quota). Batch path "
        "routers/quota.py:206 (resolve_quota_limits, services/quota.py:444-461) "
        "likewise falls back to the plan value. Also: services/license_resolver.py:"
        "180-193 and 304-315 read request_quota_config for licence->tenant/app "
        "attribution, so deleting it removes app-based attribution until "
        "sync-from-licenses (routers/budget.py:1011) re-upserts the rows."
    ),
    "token_budgets_absent": (
        "REJECT at reserve time: services/budget_redis.py:1112-1123 returns "
        "allowed=False reason='no_active_budget' when a tenant has no active budget, "
        "and routers/budget.py:280-290 turns every allowed=False into HTTP 429. "
        "⚠️ This CONTRADICTS the comments at routers/budget.py:216-219, :502-505 and "
        "services/budget_redis.py:125-127, which say absence means 'unlimited'/ALLOW — "
        "the code refuses. Any caller that gates spend on POST /api/budget/reserve "
        "is refused for a scoped tenant after the delete. The post-hoc debit path "
        "does NOT refuse: services/cost_flusher.py:1914-1920 and "
        "services/budget_debit.py:325-331 log 'no_active_budgets' and skip the "
        "debit, so spend is still recorded, uncapped."
    ),
    "invoice_void_reopens_period": (
        "a void leaves ux_billing_invoices_open_period's set "
        "(services/invoices.py:2496-2500), so the automated close may issue a "
        "replacement invoice for that period from whatever history survives"
    ),
    "metering_retarget": (
        "billing_metering_events is scoped on tenant_id only; retarget_tenant_id is "
        "'recorded only; nothing bills on it yet' (src/models/db/events.py:55-60). "
        "retarget_in_scope_not_owner counts rows retargeted TO a scoped tenant but "
        "owned by another; they are kept"
    ),
}


class Refused(Exception):  # noqa: N818 - an outcome, not a bug
    """Refuse before connecting (exit 2)."""


class Aborted(Exception):  # noqa: N818 - an outcome, not a bug
    """A guard or cross-check stopped the run; the transaction rolls back (exit 3)."""


# --------------------------------------------------------------------------- #
# Arguments
# --------------------------------------------------------------------------- #


def _parse_cutoff(raw: str) -> datetime:
    try:
        value = datetime.fromisoformat(raw.replace("Z", "+00:00"))
    except ValueError as exc:
        raise argparse.ArgumentTypeError(f"--cutoff is not ISO8601: {raw!r}") from exc
    if value.tzinfo is None or value.utcoffset() != UTC.utcoffset(None):
        raise argparse.ArgumentTypeError(
            f"--cutoff must be an explicit UTC timestamp (Z or +00:00), got {raw!r}"
        )
    return value.astimezone(UTC)


def _parse_tenants(raw: str) -> list[str]:
    out: list[str] = []
    for part in raw.split(","):
        part = part.strip()
        if not part:
            continue
        try:
            out.append(str(uuid.UUID(part)))
        except ValueError as exc:
            raise argparse.ArgumentTypeError(f"--tenants: not a uuid: {part!r}") from exc
    if not out:
        raise argparse.ArgumentTypeError("--tenants is empty")
    return sorted(set(out))


def _non_empty(raw: str) -> str:
    if not raw or not raw.strip():
        raise argparse.ArgumentTypeError("must be non-empty (and not only whitespace)")
    return raw


def build_parser() -> argparse.ArgumentParser:
    epilog = "CONFIG_ABSENCE_NOTES (copied into every receipt):\n" + "\n".join(
        f"  {k}: {v}" for k, v in CONFIG_ABSENCE_NOTES.items()
    )
    parser = argparse.ArgumentParser(
        description=__doc__,
        epilog=epilog,
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    parser.add_argument(
        "--cutoff",
        required=True,
        type=_parse_cutoff,
        help="ISO8601 UTC. Rows before it are deleted (see SCOPE). Must not be in the future.",
    )
    parser.add_argument(
        "--reason",
        required=True,
        type=_non_empty,
        help="Recorded in billing_delete_audit.reason and as every invoice's void_reason.",
    )
    parser.add_argument(
        "--actor",
        required=True,
        type=_non_empty,
        help="Who is acting: voided_by on invoices, actor_sub on their audit_log rows.",
    )
    scope = parser.add_mutually_exclusive_group(required=True)
    scope.add_argument("--tenants", type=_parse_tenants, help="Comma-separated tenant uuids.")
    scope.add_argument(
        "--all-tenants",
        action="store_true",
        help="No tenant predicate at all (NULL-tenant rows included).",
    )
    parser.add_argument(
        "--include-config",
        action="store_true",
        help=(
            "Also delete token_budgets, request_quota_config and audit_log rows CREATED "
            "before the cutoff (created_at / ts); ACTIVE token_budgets rows are never "
            "deleted. See CONFIG_ABSENCE_NOTES for the request-time effect: no "
            "token_budgets => /api/budget/reserve 429s."
        ),
    )
    parser.add_argument(
        "--void-invoices",
        action="store_true",
        help=(
            "Void (never delete) draft/issued invoices of the scoped tenants whose "
            "period_start is before the cutoff's UTC date."
        ),
    )
    parser.add_argument("--receipt", required=True, type=Path, help="JSON receipt path (new file).")
    parser.add_argument(
        "--execute",
        action="store_true",
        help="Actually delete. Without it: one READ ONLY transaction, nothing written.",
    )
    parser.add_argument(
        "--lock-timeout",
        default="5s",
        help="SET LOCAL lock_timeout (default 5s: fail fast, never queue ingest).",
    )
    parser.add_argument(
        "--statement-timeout", default="300s", help="SET LOCAL statement_timeout (default 300s)."
    )
    return parser


# --------------------------------------------------------------------------- #
# DSN
# --------------------------------------------------------------------------- #


def _libpq_dsn(dsn: str) -> str:
    """Accept the SQLAlchemy spelling (postgresql+psycopg://) as well as libpq's."""
    for prefix in ("postgresql+psycopg://", "postgresql+psycopg2://", "postgres+psycopg://"):
        if dsn.startswith(prefix):
            return "postgresql://" + dsn[len(prefix) :]
    return dsn


def check_dsn(dsn: str | None) -> str:
    """Return a libpq DSN, or raise :class:`Refused` for anything but a direct connection."""
    if not dsn:
        raise Refused("BILLING_PURGE_DSN is not set")
    dsn = _libpq_dsn(dsn)
    try:
        params = conninfo_to_dict(dsn)
    except psycopg.ProgrammingError as exc:
        raise Refused(f"BILLING_PURGE_DSN is not a valid DSN: {exc}") from exc
    host = str(params.get("host") or params.get("hostaddr") or os.environ.get("PGHOST", ""))
    port = str(params.get("port") or os.environ.get("PGPORT", ""))
    if "pgbouncer" in host.lower():
        raise Refused(
            f"refusing DSN host {host!r}: pgbouncer transaction pooling would leak this "
            "run's session state to other clients. Connect DIRECTLY to the primary."
        )
    if "6432" in port.split(","):
        raise Refused(f"refusing DSN port {port!r}: 6432 is pgbouncer. Connect DIRECTLY.")
    return dsn


# --------------------------------------------------------------------------- #
# Catalog guards
# --------------------------------------------------------------------------- #


def _row(cur: psycopg.Cursor) -> tuple:
    """fetchone() for a query that always returns exactly one row."""
    row = cur.fetchone()
    if row is None:
        raise Aborted("a single-row catalog/count query returned no row")
    return row


def _identity_keyed_tables(cur: psycopg.Cursor) -> list[str]:
    """Every top-level table with a ``%tenant%`` column or any ``IDENTITY_COLUMNS``.

    Matching also ``*_license_id`` / ``*_subject_id`` / … so a prefixed spelling
    (``source_subject_id``) cannot slip past.
    """
    cur.execute(
        """
        SELECT DISTINCT c.relname
          FROM pg_class c
          JOIN pg_namespace n ON n.oid = c.relnamespace
          JOIN pg_attribute a ON a.attrelid = c.oid AND a.attnum > 0 AND NOT a.attisdropped
         WHERE n.nspname = current_schema()
           AND c.relkind IN ('r', 'p')
           AND NOT c.relispartition
           AND (a.attname ILIKE '%%tenant%%'
                OR a.attname = ANY(%(cols)s)
                OR EXISTS (SELECT 1 FROM unnest(%(cols)s::text[]) k
                            WHERE right(a.attname, length(k) + 1) = '_' || k))
         ORDER BY 1
        """,
        {"cols": list(IDENTITY_COLUMNS)},
    )
    return [r[0] for r in cur.fetchall()]


def check_table_coverage(cur: psycopg.Cursor) -> list[str]:
    """Abort if any identity-keyed table is neither allowlisted nor excluded."""
    known = {t.name for t in ALLOWLIST} | set(EXCLUDED)
    found = _identity_keyed_tables(cur)
    unknown = [t for t in found if t not in known]
    if unknown:
        raise Aborted(
            "unclassified tenant/licence/subject-keyed table(s) "
            f"{unknown}: add each to ALLOWLIST (to purge) or EXCLUDED (with a reason) "
            "in scripts/tenant_purge.py. Nothing was deleted."
        )
    return found


def _verify_columns(cur: psycopg.Cursor, tables: list[PurgeTable]) -> None:
    for t in tables:
        cur.execute(
            """
            SELECT a.attname
              FROM pg_attribute a
              JOIN pg_class c ON c.oid = a.attrelid
              JOIN pg_namespace n ON n.oid = c.relnamespace
             WHERE n.nspname = current_schema() AND c.relname = %s
               AND a.attnum > 0 AND NOT a.attisdropped
            """,
            (t.name,),
        )
        cols = {r[0] for r in cur.fetchall()}
        if not cols:
            raise Aborted(f"allowlisted table {t.name} does not exist in current_schema()")
        missing = ({t.ts_col, *t.keyed_by} | ({t.tenant_col} if t.tenant_col else set())) - cols
        if t.ts_kind == "month":
            missing |= {"period_type"} - cols
        if t.protect_active:
            missing |= {"active"} - cols
        if missing:
            raise Aborted(f"{t.name} lacks declared column(s) {sorted(missing)}")


def _referencing_fks(cur: psycopg.Cursor, tables: list[PurgeTable]) -> list[str]:
    cur.execute(
        """
        SELECT conname, conrelid::regclass::text, confrelid::regclass::text
          FROM pg_constraint
         WHERE contype = 'f' AND confrelid::regclass::text = ANY(%s)
        """,
        ([t.name for t in tables],),
    )
    return [f"{r[0]}: {r[1]} -> {r[2]}" for r in cur.fetchall()]


def _audited(cur: psycopg.Cursor, table: str) -> bool:
    cur.execute(
        """
        SELECT count(*) FROM pg_trigger t
          JOIN pg_class c ON c.oid = t.tgrelid
          JOIN pg_namespace n ON n.oid = c.relnamespace
         WHERE n.nspname = current_schema() AND c.relname = %s
           AND t.tgname = ANY(%s) AND t.tgenabled <> 'D'
        """,
        (table, list(_DELETE_AUDIT_TRIGGERS)),
    )
    return _row(cur)[0] > 0


# --------------------------------------------------------------------------- #
# Predicates
# --------------------------------------------------------------------------- #


def _eligible(t: PurgeTable) -> sql.Composable:
    """``<row may be deleted>``: before the cutoff and, if protected, not active.

    Every count, the DELETE and the cross-checks use this one predicate, so an
    active row of a ``protect_active`` table lands in the KEPT set and the
    "kept rows unchanged" check proves it survived.
    """
    before = _before_cutoff(t)
    if t.protect_active:
        return sql.SQL("({} AND active IS NOT TRUE)").format(before)
    return before


def _active_skipped(t: PurgeTable) -> sql.Composable:
    """Rows before the cutoff that ``protect_active`` keeps."""
    return sql.SQL("({} AND active IS TRUE)").format(_before_cutoff(t))


def _before_cutoff(t: PurgeTable) -> sql.Composable:
    """``<row is before the cutoff>`` for this table. Uses the %(cutoff)s param."""
    col = sql.Identifier(t.ts_col)
    if t.ts_kind == "instant":
        return sql.SQL("{} < %(cutoff)s").format(col)
    if t.ts_kind == "hour":
        return sql.SQL("({} + interval '1 hour') <= %(cutoff)s").format(col)
    if t.ts_kind == "day":
        return sql.SQL("({} + interval '1 day') <= (%(cutoff)s AT TIME ZONE 'UTC')").format(col)
    if t.ts_kind == "month":
        return sql.SQL(
            "(period_type = 'monthly' AND ({} + interval '1 month') "
            "<= (%(cutoff)s AT TIME ZONE 'UTC'))"
        ).format(col)
    if t.ts_kind == "month_start":
        return sql.SQL("({} + interval '1 month') <= (%(cutoff)s AT TIME ZONE 'UTC')").format(col)
    raise ValueError(f"unknown ts_kind {t.ts_kind!r}")


def _scope(t: PurgeTable, tenants: list[str] | None) -> sql.Composable:
    if tenants is None:
        return sql.SQL("TRUE")
    if t.tenant_col is None:
        # Only reached after check_unresolvable() proved it holds NO eligible row
        # for any licence: nothing of the scoped tenants' can be left behind.
        return sql.SQL("FALSE")
    return sql.SQL("{} = ANY(%(tenants)s::uuid[])").format(sql.Identifier(t.tenant_col))


def _count(cur: psycopg.Cursor, t: PurgeTable, where: sql.Composable, params: dict) -> int:
    cur.execute(
        sql.SQL("SELECT count(*) FROM {} WHERE ").format(sql.Identifier(t.name)) + where, params
    )
    return int(_row(cur)[0])


# --------------------------------------------------------------------------- #
# Invoices
# --------------------------------------------------------------------------- #

_INVOICE_SCOPE = sql.SQL(
    "status IN ('draft', 'issued') AND period_start < (%(cutoff)s AT TIME ZONE 'UTC')::date"
)


def _invoice_candidates(cur: psycopg.Cursor, tenants: list[str] | None, params: dict) -> list:
    tenant_sql = (
        sql.SQL("TRUE") if tenants is None else sql.SQL("tenant_id = ANY(%(tenants)s::uuid[])")
    )
    cur.execute(
        sql.SQL(
            "SELECT id::text, tenant_id::text, invoice_number, status, period_start, period_end "
            "FROM billing_invoices WHERE "
        )
        + tenant_sql
        + sql.SQL(" AND ")
        + _INVOICE_SCOPE
        + sql.SQL(" ORDER BY tenant_id, period_start, id"),
        params,
    )
    return cur.fetchall()


#: ``services.prepaid_credit.lock_tenant_credit``'s advisory-lock namespace. Spelled
#: out (this script imports nothing from ``src``); pinned equal by
#: ``tests/test_tenant_purge.py``.
CREDIT_LOCK_PREFIX = "billing:prepaid-credit:"


def void_invoice(
    cur: psycopg.Cursor, invoice_id: str, *, reason: str, actor: str, run_id: str
) -> dict:
    """Mirror ``services.invoices.void`` (services/invoices.py:2483-2528) + its route's audit row.

    Same statement shape the 0024 trigger admits: only status/void_reason/
    voided_at/voided_by change; the row is locked first; an already-void row is
    refused (void is terminal).

    ⭐ PREPAID CREDIT (alembic 0055), exactly as ``void()`` does it: the tenant's
    credit lock (``services.prepaid_credit.lock_tenant_credit``, same advisory
    key) is taken first, and the invoice's ``usage_drawdown`` is reversed by a
    ``drawdown_reversal`` entry in THIS transaction. Without it a purge-void would
    keep debiting credit the customer paid for, for a statement that no longer
    exists.

    The audit row has the columns ``services.audit.record`` fills (ts,
    actor_sub, actor_role, action, resource_type, resource_id, tenant_id, after,
    request_id; before stays NULL as the route passes none). Two deliberate
    differences: ``actor_role`` is ``billing-tenant-purge`` rather than a JWT
    role (there is no request), and ``request_id`` is this run's ``run_id``.
    """
    cur.execute(
        "SELECT status, tenant_id::text FROM billing_invoices WHERE id = %s FOR UPDATE",
        (invoice_id,),
    )
    row = cur.fetchone()
    if row is None:
        raise Aborted(f"invoice {invoice_id} vanished before it could be voided")
    status, tenant_id = row
    if status == "void":
        raise Aborted(f"invoice {invoice_id} is already void; void is terminal")
    now = datetime.now(UTC)
    cur.execute(
        "SELECT pg_advisory_xact_lock(hashtextextended(%s, 0))",
        (f"{CREDIT_LOCK_PREFIX}{tenant_id}",),
    )
    cur.execute(
        "UPDATE billing_invoices SET status = 'void', void_reason = %s, voided_at = %s, "
        "voided_by = %s WHERE id = %s",
        (reason, now, actor, invoice_id),
    )
    cur.execute(
        "INSERT INTO tenant_credit_ledger (id, tenant_id, kind, entry_key, amount_usd, "
        "period_start, invoice_id, created_at) "
        "SELECT gen_random_uuid(), tenant_id, 'drawdown_reversal', 'reversal:' || invoice_id::text, "
        "amount_usd, period_start, invoice_id, %s FROM tenant_credit_ledger "
        "WHERE tenant_id = %s::uuid AND entry_key = 'usage:' || %s::text "
        "ON CONFLICT ON CONSTRAINT uq_credit_ledger_entry DO NOTHING",
        (now, tenant_id, str(invoice_id)),
    )
    # routers/billing.py:837-845 — audit.record(action="billing.invoice.void", …)
    cur.execute(
        "INSERT INTO audit_log (ts, actor_sub, actor_role, action, resource_type, "
        "resource_id, tenant_id, after, request_id) VALUES (%s, %s, %s, "
        "'billing.invoice.void', 'invoice', %s, %s, %s, %s)",
        (
            now,
            actor,
            APPLICATION_NAME,
            invoice_id,
            tenant_id,
            Jsonb({"status": "void", "void_reason": reason}),
            run_id,
        ),
    )
    return {"id": invoice_id, "tenant_id": tenant_id, "previous_status": status}


# --------------------------------------------------------------------------- #
# Run
# --------------------------------------------------------------------------- #


def _resolve_tenants(
    cur: psycopg.Cursor, tables: list[PurgeTable], tenants: list[str] | None, params: dict
) -> dict:
    """The in-scope tenants that actually own an eligible row."""
    per_table: dict[str, list[str]] = {}
    null_rows = 0
    for t in tables:
        if t.tenant_col is None:
            continue
        cur.execute(
            sql.SQL("SELECT DISTINCT {}::text FROM {} WHERE ").format(
                sql.Identifier(t.tenant_col), sql.Identifier(t.name)
            )
            + _scope(t, tenants)
            + sql.SQL(" AND ")
            + _eligible(t),
            params,
        )
        ids = [r[0] for r in cur.fetchall()]
        if None in ids:
            null_rows += _count(
                cur,
                t,
                sql.SQL("{} IS NULL AND ").format(sql.Identifier(t.tenant_col)) + _eligible(t),
                params,
            )
        per_table[t.name] = sorted(i for i in ids if i is not None)
    everyone = sorted({i for ids in per_table.values() for i in ids})
    return {"with_eligible_rows": everyone, "null_tenant_eligible_rows": null_rows}


def check_unresolvable(
    cur: psycopg.Cursor, tables: list[PurgeTable], tenants: list[str] | None, params: dict
) -> dict[str, int]:
    """With ``--tenants``, abort if a tenant-less table holds ANY row before the cutoff.

    Returns ``{table: eligible rows for ANY licence}`` for the tenant-less tables.
    """
    out: dict[str, int] = {}
    for t in tables:
        if t.tenant_col is not None:
            continue
        out[t.name] = _count(cur, t, _eligible(t), params)
    if tenants is not None:
        blocking = {k: v for k, v in out.items() if v}
        if blocking:
            raise Aborted(
                f"--tenants cannot scope {blocking} (rows before the cutoff, for ANY "
                "licence): these tables have no tenant column (keyed by license_id/"
                "subject_id) and billing has no AUTHORITATIVE licence->tenant mapping "
                "(request_quota_config is a pruned, current-state mirror; "
                "billing_metering_events.license_id/tenant_id is a derived per-event "
                "stamp). Guessing could delete another tenant's history or leave the "
                "scoped tenants'. Use --all-tenants, or resolve the licences upstream "
                "first. Nothing was deleted."
            )
    return out


def run(args: argparse.Namespace, dsn: str, receipt: dict[str, Any]) -> None:
    """Fill ``receipt`` in place, so a guard abort still leaves what was measured."""
    tables = [t for t in ALLOWLIST if args.include_config or not t.config]
    tenants: list[str] | None = None if args.all_tenants else args.tenants
    params: dict[str, Any] = {"cutoff": args.cutoff, "tenants": tenants}
    # Stored as audit_log.request_id on every void row, the column the route
    # fills from request.state.request_id: one id ties the rows to this receipt.
    run_id = f"tenant-purge-{uuid.uuid4().hex}"
    receipt.update(
        {
            "tool": "scripts/tenant_purge.py",
            "run_id": run_id,
            "dry_run": not args.execute,
            "started_at": datetime.now(UTC).isoformat(),
            "args": {
                "cutoff": args.cutoff.isoformat(),
                "reason": args.reason,
                "actor": args.actor,
                "tenants": tenants,
                "all_tenants": bool(args.all_tenants),
                "include_config": bool(args.include_config),
                "void_invoices": bool(args.void_invoices),
                "execute": bool(args.execute),
                "lock_timeout": args.lock_timeout,
                "statement_timeout": args.statement_timeout,
            },
            "cutoff": args.cutoff.isoformat(),
            "reason": args.reason,
            "excluded_tables": EXCLUDED,
            "notes": {
                "config_semantics": (
                    "--include-config deletes token_budgets/request_quota_config rows with "
                    "created_at < cutoff and audit_log rows with ts < cutoff (all have a "
                    "usable timestamp); with cutoff = approval time that is every row that "
                    "existed at approval. token_budgets rows with active = true are NEVER "
                    "deleted (#244); they are counted per table as active_skipped."
                ),
                "bucket_semantics": (
                    "hour/day/month buckets are deleted only if they END at or before the "
                    "cutoff (UTC); a straddling bucket is kept."
                ),
                **CONFIG_ABSENCE_NOTES,
            },
            "outcome": "started",
        }
    )

    with psycopg.connect(dsn, application_name=APPLICATION_NAME, autocommit=False) as conn:
        conn.isolation_level = psycopg.IsolationLevel.REPEATABLE_READ
        conn.read_only = not args.execute
        try:
            with conn.cursor() as cur:
                cur.execute(
                    "SELECT set_config('lock_timeout', %s, true), "
                    "set_config('statement_timeout', %s, true)",
                    (args.lock_timeout, args.statement_timeout),
                )
                cur.execute("SELECT current_setting('transaction_read_only'), current_schema()")
                read_only, schema = _row(cur)
                receipt["transaction_read_only"] = read_only
                receipt["schema"] = schema
                if not args.execute and read_only != "on":
                    raise Aborted("dry-run transaction is not READ ONLY; refusing to continue")

                receipt["identity_keyed_tables_seen"] = check_table_coverage(cur)
                _verify_columns(cur, tables)
                fks = _referencing_fks(cur, tables)
                if fks:
                    raise Aborted(
                        f"foreign keys reference tables in the purge set: {fks}. Deleting "
                        "would cascade or fail; resolve them first."
                    )
                untenanted = check_unresolvable(cur, tables, tenants, params)
                receipt["resolved_scope"] = {
                    "requested": "all-tenants" if tenants is None else tenants,
                    **_resolve_tenants(cur, tables, tenants, params),
                    "untenanted_eligible_rows_any_licence": untenanted,
                }

                cur.execute("SELECT count(*), coalesce(max(id), 0) FROM billing_delete_audit")
                audit_n_before, audit_max_before = _row(cur)

                # Invoices first: a void refusal aborts before any delete runs.
                invoices = _invoice_candidates(cur, tenants, params)
                receipt["invoices"] = {
                    "candidates": [
                        {
                            "id": r[0],
                            "tenant_id": r[1],
                            "invoice_number": r[2],
                            "status": r[3],
                            "period_start": r[4].isoformat(),
                            "period_end": r[5].isoformat(),
                        }
                        for r in invoices
                    ],
                    "voided": [],
                    "requested": bool(args.void_invoices),
                }
                if args.execute and args.void_invoices:
                    for r in invoices:
                        receipt["invoices"]["voided"].append(
                            void_invoice(
                                cur, r[0], reason=args.reason, actor=args.actor, run_id=run_id
                            )
                        )

                if args.execute:
                    cur.execute("SELECT set_config(%s, %s, true)", (DELETE_REASON_GUC, args.reason))

                per_table: list[dict[str, Any]] = []
                receipt["tables"] = per_table
                for t in tables:
                    scope = _scope(t, tenants)
                    elig = _eligible(t)
                    before = _count(cur, t, scope, params)
                    eligible = _count(cur, t, scope + sql.SQL(" AND ") + elig, params)
                    kept_before = _count(
                        cur, t, scope + sql.SQL(" AND NOT (") + elig + sql.SQL(")"), params
                    )
                    entry: dict[str, Any] = {
                        "table": t.name,
                        "tenant_col": t.tenant_col,
                        "keyed_by": list(t.keyed_by),
                        "ts_col": t.ts_col,
                        "ts_kind": t.ts_kind,
                        "config": t.config,
                        "delete_audited": _audited(cur, t.name),
                        "before": before,
                        "eligible": eligible,
                        "untouched_after_cutoff_before": kept_before,
                    }
                    if not entry["delete_audited"]:
                        entry["unaudited"] = "counts are the only record"
                    if t.protect_active:
                        entry["active_skipped"] = _count(
                            cur, t, scope + sql.SQL(" AND ") + _active_skipped(t), params
                        )
                    if t.tenant_col is None and tenants is not None:
                        entry["tenant_scope"] = (
                            "unresolvable (no tenant column); 0 rows in scope because the "
                            "table holds no row before the cutoff for ANY licence"
                        )
                    if t.name == "billing_metering_events" and tenants is not None:
                        entry["retarget_in_scope_not_owner"] = _count(
                            cur,
                            t,
                            sql.SQL(
                                "retarget_tenant_id = ANY(%(tenants)s::uuid[]) "
                                "AND NOT tenant_id = ANY(%(tenants)s::uuid[])"
                            ),
                            params,
                        )
                    if args.execute:
                        cur.execute(
                            sql.SQL("DELETE FROM {} WHERE ").format(sql.Identifier(t.name))
                            + scope
                            + sql.SQL(" AND ")
                            + elig,
                            params,
                        )
                        deleted = cur.rowcount
                        after = _count(cur, t, scope, params)
                        kept_after = _count(
                            cur, t, scope + sql.SQL(" AND NOT (") + elig + sql.SQL(")"), params
                        )
                        remaining = _count(cur, t, scope + sql.SQL(" AND ") + elig, params)
                    else:
                        deleted, after, kept_after, remaining = 0, before, kept_before, eligible
                    entry.update(
                        deleted=deleted,
                        after=after,
                        untouched_after_cutoff=kept_after,
                        eligible_remaining=remaining,
                    )
                    per_table.append(entry)
                    if args.execute:
                        problems = []
                        if deleted != eligible:
                            problems.append(f"deleted {deleted} != eligible {eligible}")
                        if after != before - deleted:
                            problems.append(f"after {after} != before {before} - deleted {deleted}")
                        if kept_after != kept_before:
                            problems.append(f"kept rows moved {kept_before} -> {kept_after}")
                        if remaining != 0:
                            problems.append(f"{remaining} eligible rows remain")
                        if problems:
                            raise Aborted(f"{t.name}: " + "; ".join(problems))

                cur.execute(
                    "SELECT id, ts, table_name, rows_deleted, operation, reason, txid "
                    "FROM billing_delete_audit WHERE id > %s ORDER BY id",
                    (audit_max_before,),
                )
                new_audit = cur.fetchall()
                cur.execute("SELECT count(*) FROM billing_delete_audit")
                audit_n_after = _row(cur)[0]
                receipt["billing_delete_audit"] = {
                    "rows_before": audit_n_before,
                    "rows_after": audit_n_after,
                    "delta": audit_n_after - audit_n_before,
                    "new_rows": [
                        {
                            "id": r[0],
                            "ts": r[1].isoformat(),
                            "table_name": r[2],
                            "rows_deleted": r[3],
                            "operation": r[4],
                            "reason": r[5],
                            "txid": r[6],
                        }
                        for r in new_audit
                    ],
                }
                if args.execute:
                    # audit_log's own trigger (0029) composes its reason as
                    # "audit_log delete: N row(s), id range [..]. operator reason: <reason>",
                    # so containment, not equality.
                    bad = [r[0] for r in new_audit if args.reason not in (r[5] or "")]
                    if bad:
                        raise Aborted(f"billing_delete_audit rows {bad} do not carry --reason")

            if args.execute:
                conn.commit()
                receipt["outcome"] = "committed"
            else:
                conn.rollback()
                receipt["outcome"] = "dry_run_rolled_back"
        except BaseException:
            conn.rollback()
            raise


def _write_receipt(path: Path, receipt: dict) -> None:
    receipt["ended_at"] = datetime.now(UTC).isoformat()
    path.write_text(json.dumps(receipt, indent=2, sort_keys=False, default=str) + "\n")


def main(argv: list[str] | None = None) -> int:
    parser = build_parser()
    args = parser.parse_args(argv)
    try:
        if args.cutoff > datetime.now(UTC):
            raise Refused(f"--cutoff {args.cutoff.isoformat()} is in the future")
        receipt_path: Path = args.receipt.expanduser()
        if receipt_path.exists():
            raise Refused(
                f"--receipt {receipt_path} already exists; receipts are never overwritten"
            )
        if not receipt_path.parent.is_dir():
            raise Refused(f"--receipt directory {receipt_path.parent} does not exist")
        dsn = check_dsn(os.environ.get("BILLING_PURGE_DSN"))
    except Refused as exc:
        print(f"REFUSED: {exc}", file=sys.stderr)
        return EXIT_REFUSED

    receipt: dict[str, Any] = {"started_at": datetime.now(UTC).isoformat()}
    try:
        run(args, dsn, receipt)
        code = EXIT_OK
    except Aborted as exc:
        receipt.update(outcome="aborted_rolled_back", error=str(exc))
        code = EXIT_ABORTED
    except psycopg.Error as exc:
        receipt.update(outcome="error_rolled_back", error=f"{type(exc).__name__}: {exc}")
        code = EXIT_ERROR
    receipt.setdefault("dry_run", not args.execute)
    receipt.setdefault("cutoff", args.cutoff.isoformat())
    receipt.setdefault("reason", args.reason)
    _write_receipt(receipt_path, receipt)
    print(f"{receipt['outcome']}: receipt at {receipt_path}")
    for entry in receipt.get("tables", []):
        if entry.get("active_skipped"):
            print(
                f"{entry['table']}: kept {entry['active_skipped']} ACTIVE row(s) created "
                "before the cutoff (active rows are never deleted)"
            )
    if code != EXIT_OK:
        print(f"ERROR: {receipt.get('error')}", file=sys.stderr)
    return code


if __name__ == "__main__":
    raise SystemExit(main())
