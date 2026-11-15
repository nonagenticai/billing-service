"""Re-key tenant ``portal-render-e2e`` onto its new registry id — config AND history.

WHY
---
``portal-render-e2e`` (the w2v end-to-end test tenant) carries the id
``e148e8af-3751-49bf-a0fa-56ab46d31ddc``, which is also the id of a (deleted)
Keycloak PRINCIPAL; a tenant id that is also a principal id is banned. The
registry re-keys it to ``01a1113b-674f-72b0-a84b-dc746d39afca`` (uuid7).

DIFFERENCE FROM 0048 (``book-fulfillment``): 0048 left history on the old id.
The owner's ruling for this tenant is to MOVE HISTORY TOO, in one window, so the
tenant's spend, usage and rollups stay one continuous series under one id.

⚠️ TENANT-SENSE ONLY. The same uuid also occurs as a USER (``subject_id``,
``actor_id``, ``actor_sub``, ``reattributed_by``, ``voided_by`` …). No statement
here names a user-sense column, and no JSON document is text-replaced: each JSON
update rewrites ONE named key.

MOVED — live config / state (the 0048 set)
------------------------------------------
``token_budgets`` (+ ``tenant_name`` where it holds the old uuid AS TEXT; a real
name is left alone — same rule for every ``tenant_name`` below),
``tenant_money_budget``, ``request_quota_config``, ``tenant_config``
(+ ``tenant_name``), ``tenant_webhooks``, ``markup_rules``,
``tenant_access_charge``, ``billing_attribution_rules`` (source AND bill side).

MOVED — history / ledger (one line each: why it moves)
------------------------------------------------------
* ``billing_cost_events`` ``tenant_id`` / ``tenant_name`` — the cost ledger; the
  series must be continuous under the new id. Partitioned by ``created_at``,
  which is not touched, so no row changes partition.
* ``billing_cost_events.metadata ->> 'attributed_from_tenant_id'`` — the SOURCE
  tenant of a re-attributed event (``services.attribution_rules``); read back as
  a tenant by ``services.attribution_audit``. That one key only: ``metadata`` is
  emitter-supplied and may hold the same uuid in a user sense under other keys.
  The column is ``json``; it round-trips through ``jsonb`` (key order and
  insignificant whitespace are not preserved, the value is).
* ``billing_metering_events`` ``tenant_id`` / ``retarget_tenant_id`` /
  ``tenant_name`` — the request ledger; ``retarget_tenant_id`` is a tenant too.
* ``billing_metering_hourly`` / ``billing_metering_daily`` / ``billing_cost_daily``
  (+ ``tenant_name``), ``billing_cost_actor_daily``, ``token_usage``
  (+ ``tenant_name``) — rollups; they must agree with the events they summarise,
  and ``token_usage`` is the seed for the Redis month-to-date counter.
* ``budget_overage_alerts`` — alert history per tenant (dedupe is on
  ``budget_id``, so no slot can collide).
* ``tenant_money_budget_adjustment`` — the money-budget change ledger, read per
  tenant next to the (moved) budget row. ``actor_sub`` is NOT touched.
* ``billing_dlq_reattributions`` ``tenant_id`` / ``reattributed_from`` /
  ``original_fields ->> 'tenant_id'`` — receipts must name the tenant the
  re-injected event now lives under.
* ``billing_dlq_archive.fields ->> 'tenant_id'`` — the recovery copy of a dead
  letter; restored under the old id it would be an unregistered tenant.
* ``billing_delete_audit.tenant_ids`` — the tenants a destructive statement hit;
  element-wise replace (``statement`` text is verbatim evidence and is left).
* ``tenant_id_crosswalk.tenant_uuid`` — 0011's legacy-value → uuid map
  (``legacy_value`` is the key and is left).

NOT MOVED
---------
* ``audit_log`` — ⛔ PROTECTED: ``billing_audit_log_immutability_trg`` refuses
  any UPDATE that changes a row. Not bypassed. Rows stay on the old id; the
  count left behind is logged. Correct an audit trail by APPENDING.
* ``billing_invoices`` — ⛔ PROTECTED: ``billing_invoice_immutability_trg``
  freezes issued/void rows, and ``snapshot`` (which embeds ``tenant_id``) is
  attested by ``snapshot_sha256`` on every row, drafts included. Not bypassed.
  Rows stay on the old id; the count left behind is logged.
* ``tenant_id_divergence_audit`` — a finding ("this row carried an id no
  registry vouches for"); rewritten to the new, registered id it would be false.
* ``billing_cost_events.usage_extra``, every other ``metadata`` key,
  ``audit_log.before/after``, invoice ``inputs/lines`` — no tenant-keyed reader;
  emitter/forensic payloads that may hold the uuid in a user sense.
* ``billing_metering_monthly_usage``, ``processed_*`` — no tenant column.

SAFETY
------
* Explicit SQL and two literal uuids; nothing is rendered from live metadata.
* No-op on a database with no such rows (fresh / CI), and on a second run.
* A unique slot already occupied on the NEW id is REFUSED before any write
  (``RuntimeError``; the whole revision rolls back) — never merged or dropped.
  That covers the five rollup slots as well as the config slots. The unique
  indexes themselves remain the backstop.
* Bounded ``lock_timeout``; no trigger is disabled, no guard GUC is set.
* Redis is NOT touched here — see REDIS RESEED below.

REDIS RESEED (a cutover step — this revision does not touch Redis)
------------------------------------------------------------------
There is no per-tenant rebuild function; the all-tenant one is idempotent and
is what to use. After this revision commits, with the tenant quiesced:

1. Rebuild the NEW keys from the (moved) rows:
   ``services.budget_redis.sync_all_budgets_to_redis`` writes
   ``budget:limit:tenant:<NEW>:<period>``, ``tokens:budget:<budget_id>:consumed``
   and ``tokens:tenant:<NEW>:remaining``. It runs on every pod start
   (``main.py`` lifespan) and on every ``run_quota_resync_loop`` cycle, so the
   rollout that ships this revision already does it:
   ``kubectl -n <ns> rollout restart deploy/billing-service`` forces it.
   (``POST /api/budget/sync-from-licenses`` also ends in it, but first backfills
   budgets from licences — a wider act than a reseed; prefer the restart.)
   ``tokens:tenant:<NEW>:<YYYY-MM>`` needs nothing: the cost flusher seeds an
   absent key from ``token_usage``, which moved. ``quota:limit:<license_id>`` is
   licence-keyed and unaffected.
2. Delete the keys left on the OLD id (nothing rebuilds or reads them again;
   they only lapse by TTL, and ``reconcile_orphan_budget_limits`` is DRY-RUN
   unless ``BUDGET_REDIS_PRUNE_STALE`` is armed). With OLD = the old uuid::

       redis-cli DEL "tokens:tenant:$OLD:remaining" "tokens:tenant:$OLD:reserved" \
         "tokens:tenant:{$OLD}:reserved" "tokens:tenant:{$OLD}:resv" \
         "tokens:tenant:{$OLD}:resv:amt" \
         "budget:money:{$OLD}:reserved" "budget:money:{$OLD}:resv" \
         "budget:money:{$OLD}:resv:amt" "budget:money:{$OLD}:ccy"
       redis-cli --scan --pattern "budget:limit:tenant:$OLD:*"   # DEL each
       redis-cli --scan --pattern "tokens:tenant:$OLD:20*"       # DEL each
       redis-cli --scan --pattern "budget:money:{$OLD}:idem:*"   # DEL each

   The reservation pools (``…:reserved`` / ``…:resv*``) are in-flight state with
   no database copy: they start at zero on the NEW id, which is correct only if
   no request for this tenant is in flight — hence "quiesced".

Downgrade applies the reverse mapping (pre-prod is forward-only; kept valid).

Revision ID: 0051_rekey_portal_render_e2e
Revises: 0050_app_registry
Create Date: 2026-10-06
"""

from __future__ import annotations

import logging
from collections.abc import Sequence

import sqlalchemy as sa
from alembic import op

revision: str = "0051_rekey_portal_render_e2e"  # <= 32 chars: alembic_version.version_num
down_revision: str | None = "0050_app_registry"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None

OLD_TENANT_ID = "e148e8af-3751-49bf-a0fa-56ab46d31ddc"
NEW_TENANT_ID = "01a1113b-674f-72b0-a84b-dc746d39afca"

LOCK_TIMEOUT = "5s"

_log = logging.getLogger("alembic.runtime.migration")


def _slot_check(table: str, columns: tuple[str, ...]) -> str:
    """Rows of ``table`` on the old id whose unique slot is taken on the new id.

    ``columns`` is the unique index minus ``tenant_id``, term for term; plain
    ``=`` because every one of these columns is NOT NULL.
    """
    on = " AND ".join(f"o.{c} = n.{c}" for c in columns)
    return (
        f"SELECT count(*) FROM {table} o JOIN {table} n ON {on} "
        "WHERE o.tenant_id = CAST(:old AS uuid) AND n.tenant_id = CAST(:new AS uuid)"
    )


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
    (
        "token_usage (uq_token_usage)",
        _slot_check("token_usage", ("license_id", "period_type", "period_start")),
    ),
    (
        "billing_metering_hourly (uq_hourly)",
        _slot_check(
            "billing_metering_hourly",
            ("license_id", "subject_id", "app_id", "plan_id", "service_key", "bucket_hour"),
        ),
    ),
    (
        "billing_metering_daily (uq_daily)",
        _slot_check(
            "billing_metering_daily",
            ("license_id", "subject_id", "app_id", "plan_id", "service_key", "bucket_date"),
        ),
    ),
    (
        "billing_cost_daily (uq_cost_daily)",
        _slot_check(
            "billing_cost_daily",
            (
                "license_id",
                "subject_id",
                "service_key",
                "provider",
                "served_model",
                "byo",
                "owner_provided",
                "cost_source",
                "bucket_date",
                "project_id",
            ),
        ),
    ),
    (
        "billing_cost_actor_daily (uq_cost_actor_daily)",
        _slot_check(
            "billing_cost_actor_daily",
            (
                "bucket_date",
                "license_id",
                "subject_id",
                "actor_id",
                "service_key",
                "provider",
                "served_model",
                "byo",
                "owner_provided",
            ),
        ),
    ),
)


def _move_id(table: str, column: str = "tenant_id") -> tuple[str, str]:
    return (
        f"{table}.{column}",
        f"UPDATE {table} SET {column} = CAST(:new AS uuid) WHERE {column} = CAST(:old AS uuid)",
    )


def _move_name(table: str) -> tuple[str, str]:
    """Move ONLY a ``tenant_name`` that is the old uuid rendered as text."""
    return (
        f"{table}.tenant_name",
        f"UPDATE {table} SET tenant_name = :new WHERE lower(tenant_name) = :old",
    )


def _move_jsonb_key(table: str, column: str, key: str) -> tuple[str, str]:
    """Rewrite ONE top-level key of a ``jsonb`` document, nothing else in it."""
    return (
        f"{table}.{column}->>{key}",
        f"UPDATE {table} SET {column} = jsonb_set({column}, '{{{key}}}', to_jsonb(CAST(:new AS text))) "
        f"WHERE lower({column} ->> '{key}') = :old",
    )


_UPDATES: tuple[tuple[str, str], ...] = (
    # ── live config / state (the 0048 set) ──────────────────────────────────
    _move_id("token_budgets"),
    _move_name("token_budgets"),
    _move_id("tenant_money_budget"),
    _move_id("request_quota_config"),
    _move_id("tenant_config"),
    _move_name("tenant_config"),
    _move_id("tenant_webhooks"),
    _move_id("markup_rules"),
    _move_id("tenant_access_charge"),
    _move_id("billing_attribution_rules", "source_tenant_id"),
    _move_id("billing_attribution_rules", "bill_tenant_id"),
    # ── history / ledger ────────────────────────────────────────────────────
    _move_id("billing_cost_events"),
    _move_name("billing_cost_events"),
    (
        # `metadata` is json, not jsonb: cast, set the one key, cast back.
        "billing_cost_events.metadata->>attributed_from_tenant_id",
        "UPDATE billing_cost_events SET metadata = jsonb_set(CAST(metadata AS jsonb), "
        "'{attributed_from_tenant_id}', to_jsonb(CAST(:new AS text)))::json "
        "WHERE lower(metadata ->> 'attributed_from_tenant_id') = :old",
    ),
    _move_id("billing_metering_events"),
    _move_id("billing_metering_events", "retarget_tenant_id"),
    _move_name("billing_metering_events"),
    _move_id("billing_metering_hourly"),
    _move_name("billing_metering_hourly"),
    _move_id("billing_metering_daily"),
    _move_name("billing_metering_daily"),
    _move_id("billing_cost_daily"),
    _move_name("billing_cost_daily"),
    _move_id("billing_cost_actor_daily"),
    _move_id("token_usage"),
    _move_name("token_usage"),
    _move_id("budget_overage_alerts"),
    _move_id("tenant_money_budget_adjustment"),
    _move_id("billing_dlq_reattributions"),
    (
        "billing_dlq_reattributions.reattributed_from",
        "UPDATE billing_dlq_reattributions SET reattributed_from = :new "
        "WHERE lower(reattributed_from) = :old",
    ),
    _move_jsonb_key("billing_dlq_reattributions", "original_fields", "tenant_id"),
    _move_jsonb_key("billing_dlq_archive", "fields", "tenant_id"),
    (
        # A row naming BOTH ids keeps one entry, and its distinct count follows.
        "billing_delete_audit.tenant_ids (already names new)",
        "UPDATE billing_delete_audit SET "
        "tenant_ids = array_remove(tenant_ids, CAST(:old AS uuid)), "
        "distinct_tenant_count = distinct_tenant_count - 1 "
        "WHERE CAST(:old AS uuid) = ANY (tenant_ids) AND CAST(:new AS uuid) = ANY (tenant_ids)",
    ),
    (
        "billing_delete_audit.tenant_ids",
        "UPDATE billing_delete_audit SET "
        "tenant_ids = array_replace(tenant_ids, CAST(:old AS uuid), CAST(:new AS uuid)) "
        "WHERE CAST(:old AS uuid) = ANY (tenant_ids)",
    ),
    _move_id("tenant_id_crosswalk", "tenant_uuid"),
)

# Protected tables: counted, logged, never written. See NOT MOVED above.
_LEFT_BEHIND: tuple[tuple[str, str], ...] = (
    (
        "audit_log (billing_audit_log_immutability_trg)",
        "SELECT count(*) FROM audit_log WHERE tenant_id = CAST(:old AS uuid)",
    ),
    (
        "billing_invoices (billing_invoice_immutability_trg + snapshot_sha256)",
        "SELECT count(*) FROM billing_invoices WHERE tenant_id = CAST(:old AS uuid)",
    ),
)


def rekey(bind: sa.engine.Connection, *, old: str, new: str) -> dict[str, int]:
    """Move every tenant-sense value from ``old`` to ``new``; return rows moved per column.

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
    left = {
        label: n
        for label, sql in _LEFT_BEHIND
        if (n := bind.execute(sa.text(sql), params).scalar_one())
    }
    if left:
        _log.warning(
            "%s: rows LEFT on %s in immutable tables (not bypassed, by design): %s",
            revision,
            old,
            left,
        )
    return moved


def upgrade() -> None:
    op.execute(f"SET LOCAL lock_timeout = '{LOCK_TIMEOUT}'")
    rekey(op.get_bind(), old=OLD_TENANT_ID, new=NEW_TENANT_ID)


def downgrade() -> None:
    op.execute(f"SET LOCAL lock_timeout = '{LOCK_TIMEOUT}'")
    rekey(op.get_bind(), old=NEW_TENANT_ID, new=OLD_TENANT_ID)
