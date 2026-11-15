"""Re-key tenant ``book-fulfillment`` onto its new registry id (data only).

WHY
---
The licences registry re-keys ``book-fulfillment`` from
``1d0090ac-adbf-4c07-b704-bc66061b572e`` to
``01a1109c-f895-7766-ba28-6ea56e56a5f7``: the old id equals a Keycloak
PRINCIPAL id, and a tenant id that is also a principal id is banned. Billing
has no FK to licences, so without this revision its live rows stay on an id the
registry no longer knows — the budgets and quota config silently stop applying.

WHAT MOVES (live config / state — the rows that decide what happens NEXT)
-------------------------------------------------------------------------
``token_budgets`` (+ ``tenant_name`` where it holds the old uuid AS A STRING),
``tenant_money_budget``, ``request_quota_config``, ``tenant_config``
(+ ``tenant_name``, same rule), ``tenant_webhooks``, ``markup_rules``,
``tenant_access_charge``, ``billing_attribution_rules`` (source AND bill side).
Measured on prod 2026-10-06: only ``token_budgets`` (2) and
``request_quota_config`` (2) hold rows; the rest are covered so a row written
between the measurement and the deploy is not orphaned.

WHAT DOES NOT MOVE (history — a record of what DID happen, under the id it
happened under; rewriting it would falsify the ledger)
-------------------------------------------------------------------------
``audit_log`` (UPDATE is refused by its tamper guard anyway),
``billing_delete_audit``, ``tenant_id_divergence_audit``,
``billing_dlq_reattributions`` / ``billing_dlq_archive``,
``billing_metering_events``, ``billing_cost_events``, every rollup
(``billing_metering_hourly`` / ``_daily``, ``billing_cost_daily``,
``billing_cost_actor_daily``, ``token_usage``), ``billing_invoices``,
``budget_overage_alerts`` (dedupe is keyed on ``budget_id``, which is
unchanged) and ``tenant_money_budget_adjustment``.

SAFETY
------
* Explicit SQL and two literal uuids; nothing is rendered from live metadata.
* No-op on a database with no such rows (fresh / CI), and on a second run.
* A unique slot already occupied on the NEW id is REFUSED before any write
  (``RuntimeError``; the whole revision rolls back) — never merged or dropped.
  The unique indexes themselves remain the backstop.
* Bounded ``lock_timeout``: ``UPDATE token_budgets`` contends with the debit
  path's row locks; fail and retry rather than queue behind it.

Downgrade applies the reverse mapping (pre-prod is forward-only; kept valid).

Revision ID: 0048_rekey_book_fulfillment
Revises: 0047_dlq_reattribute_archive
Create Date: 2026-10-06
"""

from __future__ import annotations

import logging
from collections.abc import Sequence

import sqlalchemy as sa
from alembic import op

revision: str = "0048_rekey_book_fulfillment"  # <= 32 chars: alembic_version.version_num
down_revision: str | None = "0047_dlq_reattribute_archive"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None

OLD_TENANT_ID = "1d0090ac-adbf-4c07-b704-bc66061b572e"
NEW_TENANT_ID = "01a1109c-f895-7766-ba28-6ea56e56a5f7"

LOCK_TIMEOUT = "5s"

_log = logging.getLogger("alembic.runtime.migration")

# (label, SQL returning the number of unique slots held on BOTH ids). Each
# mirrors the unique index named in the label, term for term.
_COLLISION_CHECKS: tuple[tuple[str, str], ...] = (
    (
        "tenant_config (unique tenant_id)",
        "SELECT count(*) FROM tenant_config o JOIN tenant_config n ON true "
        "WHERE o.tenant_id = CAST(:old AS uuid) AND n.tenant_id = CAST(:new AS uuid)",
    ),
    (
        "tenant_money_budget (uq_money_budget_active_tenant)",
        "SELECT count(*) FROM tenant_money_budget o JOIN tenant_money_budget n ON true "
        "WHERE o.tenant_id = CAST(:old AS uuid) AND o.active "
        "AND n.tenant_id = CAST(:new AS uuid) AND n.active",
    ),
    (
        "markup_rules (ix_markuprules_open)",
        "SELECT count(*) FROM markup_rules o JOIN markup_rules n ON "
        "coalesce(o.license_id::text, '') = coalesce(n.license_id::text, '') "
        "AND coalesce(o.service_key, '') = coalesce(n.service_key, '') "
        "AND coalesce(o.provider, '') = coalesce(n.provider, '') "
        "AND coalesce(o.model, '') = coalesce(n.model, '') "
        "AND coalesce(o.byo::text, '') = coalesce(n.byo::text, '') "
        "AND coalesce(o.project_id::text, '') = coalesce(n.project_id::text, '') "
        "WHERE o.tenant_id = CAST(:old AS uuid) AND o.effective_until IS NULL "
        "AND n.tenant_id = CAST(:new AS uuid) AND n.effective_until IS NULL",
    ),
    (
        "tenant_access_charge (ix_accesscharge_open)",
        "SELECT count(*) FROM tenant_access_charge o JOIN tenant_access_charge n ON "
        "coalesce(o.service_key, '') = coalesce(n.service_key, '') "
        "WHERE o.tenant_id = CAST(:old AS uuid) AND o.effective_until IS NULL "
        "AND n.tenant_id = CAST(:new AS uuid) AND n.effective_until IS NULL",
    ),
    (
        "billing_attribution_rules (uq_attribution_rules_match)",
        "SELECT count(*) FROM billing_attribution_rules o JOIN billing_attribution_rules n ON "
        "coalesce(o.source_subject_id, '00000000-0000-0000-0000-000000000000'::uuid) = "
        "coalesce(n.source_subject_id, '00000000-0000-0000-0000-000000000000'::uuid) "
        "AND o.match_project = n.match_project "
        "WHERE o.source_tenant_id = CAST(:old AS uuid) "
        "AND n.source_tenant_id = CAST(:new AS uuid)",
    ),
)

# (label, UPDATE). The tenant_name statements move ONLY a name that is the old
# uuid rendered as text; a real name ('book-fulfillment') is left alone.
_UPDATES: tuple[tuple[str, str], ...] = (
    (
        "token_budgets.tenant_id",
        "UPDATE token_budgets SET tenant_id = CAST(:new AS uuid) "
        "WHERE tenant_id = CAST(:old AS uuid)",
    ),
    (
        "token_budgets.tenant_name",
        "UPDATE token_budgets SET tenant_name = :new WHERE lower(tenant_name) = :old",
    ),
    (
        "tenant_money_budget.tenant_id",
        "UPDATE tenant_money_budget SET tenant_id = CAST(:new AS uuid) "
        "WHERE tenant_id = CAST(:old AS uuid)",
    ),
    (
        "request_quota_config.tenant_id",
        "UPDATE request_quota_config SET tenant_id = CAST(:new AS uuid) "
        "WHERE tenant_id = CAST(:old AS uuid)",
    ),
    (
        "tenant_config.tenant_id",
        "UPDATE tenant_config SET tenant_id = CAST(:new AS uuid) "
        "WHERE tenant_id = CAST(:old AS uuid)",
    ),
    (
        "tenant_config.tenant_name",
        "UPDATE tenant_config SET tenant_name = :new WHERE lower(tenant_name) = :old",
    ),
    (
        "tenant_webhooks.tenant_id",
        "UPDATE tenant_webhooks SET tenant_id = CAST(:new AS uuid) "
        "WHERE tenant_id = CAST(:old AS uuid)",
    ),
    (
        "markup_rules.tenant_id",
        "UPDATE markup_rules SET tenant_id = CAST(:new AS uuid) "
        "WHERE tenant_id = CAST(:old AS uuid)",
    ),
    (
        "tenant_access_charge.tenant_id",
        "UPDATE tenant_access_charge SET tenant_id = CAST(:new AS uuid) "
        "WHERE tenant_id = CAST(:old AS uuid)",
    ),
    (
        "billing_attribution_rules.source_tenant_id",
        "UPDATE billing_attribution_rules SET source_tenant_id = CAST(:new AS uuid) "
        "WHERE source_tenant_id = CAST(:old AS uuid)",
    ),
    (
        "billing_attribution_rules.bill_tenant_id",
        "UPDATE billing_attribution_rules SET bill_tenant_id = CAST(:new AS uuid) "
        "WHERE bill_tenant_id = CAST(:old AS uuid)",
    ),
)


def rekey(bind: sa.engine.Connection, *, old: str, new: str) -> dict[str, int]:
    """Move every live row from ``old`` to ``new``; return rows moved per column.

    Refuses (before writing anything) if a unique slot is held on both ids.
    """
    params = {"old": old, "new": new}
    collisions = {
        label: n
        for label, sql in _COLLISION_CHECKS
        if (n := bind.execute(sa.text(sql), params).scalar_one())
    }
    if collisions:
        raise RuntimeError(
            f"alembic {revision} refuses to re-key tenant {old} -> {new}: rows already "
            f"exist on the target id in the same unique slot: {collisions}. Nothing was "
            "changed. Decide which row wins by hand, then re-run."
        )
    moved = {label: bind.execute(sa.text(sql), params).rowcount for label, sql in _UPDATES}
    _log.info("%s: re-keyed %s -> %s: %s", revision, old, new, moved)
    return moved


def upgrade() -> None:
    op.execute(f"SET LOCAL lock_timeout = '{LOCK_TIMEOUT}'")
    rekey(op.get_bind(), old=OLD_TENANT_ID, new=NEW_TENANT_ID)


def downgrade() -> None:
    op.execute(f"SET LOCAL lock_timeout = '{LOCK_TIMEOUT}'")
    rekey(op.get_bind(), old=NEW_TENANT_ID, new=OLD_TENANT_ID)
