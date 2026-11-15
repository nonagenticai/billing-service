"""Re-key the CDMSFIRST demo tenant onto its new registry id — config, history AND audit.

WHY
---
``cdmsfirst`` (Atlific, the recorded demo tenant) carries the reserved slot id
``0195f48b-d000-7000-8000-000000000100``. Owner ruling (plan §8, "B2"): re-key it
to ``01a1117c-d9b0-72fc-aeda-a9d1187f2a36`` (uuid7) in one coordinated window
with licences, core_app and the shared re-key tool, history included.

WHAT MOVES
----------
Exactly 0051's tenant-sense set (config, ledger, rollups, alerts, DLQ receipts,
``billing_delete_audit.tenant_ids``, the crosswalk) — copied here as explicit
SQL rather than imported, so 0051 stays frozen. See 0051's docstring for the
one-line reason per table. Two additions for this tenant:

* ``audit_log`` — ⚠️ DIFFERENCE FROM 0051. The owner's B2 plan moves the audit
  rows too, so the tenant has one id everywhere. ``tenant_id``, ``resource_id``
  (when it IS the tenant id — the ``quota.config.set`` rows) and the top-level
  ``before`` / ``after`` keys ``tenant_id`` / ``tenant_name`` when they hold the
  old id. ``billing_audit_log_immutability_trg`` refuses that UPDATE by design
  (no GUC opt-out), so this revision ``ALTER TABLE audit_log DISABLE TRIGGER``
  INSIDE the migration transaction and ``ENABLE``\\ s it again before commit:
  no other session ever sees it disabled (ALTER TABLE holds ACCESS EXCLUSIVE
  until commit) and a failure rolls the DISABLE back with everything else.
  Prod 2026-10-07: 13 rows (``tenant_id``), 3 ``resource_id``, 10 ``after``;
  owner of ``audit_log`` = ``billing_user`` = the role ``alembic`` runs as.
* ``billing_metering_events.path`` — 4 rows ``/api/admin/licenses/budget/<old>``:
  the tenant id is the path parameter. Anchored replace of ``/budget/<old>``.
* ``billing_cost_usage_daily.tenant_id`` (table added by 0053, after 0051 was
  written) — the cross rollup's tenant key; its unique slot is collision-checked.
* ``billing_metering_events.actor_id`` (column added by 0052). An actor is a
  Keycloak sub and OLD is a synthetic slot id that is never one, so any OLD
  value there is the tenant id written into the actor slot: it moves too, so
  no column anywhere still names OLD.

NOT MOVED
---------
* ``billing_invoices`` — ⛔ immutability trigger + ``snapshot_sha256``; prod holds
  0 rows for this tenant (checked 2026-10-07). Counted and logged if any appear.
* ``tenant_id_divergence_audit`` and every user-sense column, exactly as 0051.

SAFETY
------
* Explicit SQL and two literal uuids; nothing rendered from live metadata.
* Collision refusal before any write (0051's ten unique slots plus
  ``uq_cost_usage_daily``).
* HARD ASSERTION after the writes: zero rows still carry the old id in every
  column this revision touches, and the immutability trigger is ENABLED again —
  otherwise ``RuntimeError`` and the whole revision rolls back.
* No-op on a database with no such rows (fresh / CI) and on a second run.
* Redis is NOT touched — see 0051's REDIS RESEED; the same steps apply with
  this tenant's ids.

Downgrade applies the reverse mapping (pre-prod is forward-only; kept valid).

Revision ID: 0054_rekey_cdmsfirst_demo
Revises: 0053_cost_funding_usage_daily
Create Date: 2026-10-07
"""

from __future__ import annotations

import logging
from collections.abc import Sequence

import sqlalchemy as sa
from alembic import op

revision: str = "0054_rekey_cdmsfirst_demo"  # <= 32 chars: alembic_version.version_num
down_revision: str | None = "0053_cost_funding_usage_daily"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None

OLD_TENANT_ID = "0195f48b-d000-7000-8000-000000000100"
NEW_TENANT_ID = "01a1117c-d9b0-72fc-aeda-a9d1187f2a36"

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
        "billing_cost_usage_daily (uq_cost_usage_daily)",
        _slot_check(
            "billing_cost_usage_daily",
            (
                "bucket_date",
                "project_id",
                "actor_id",
                "app_id",
                "funding",
                "provider",
                "served_model",
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
    _move_id("billing_metering_events", "actor_id"),
    _move_name("billing_metering_events"),
    _move_id("billing_metering_hourly"),
    _move_name("billing_metering_hourly"),
    _move_id("billing_metering_daily"),
    _move_name("billing_metering_daily"),
    _move_id("billing_cost_daily"),
    _move_name("billing_cost_daily"),
    _move_id("billing_cost_actor_daily"),
    _move_id("billing_cost_usage_daily"),
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

# ── audit_log: written ONLY between DISABLE and ENABLE of the immutability
#    trigger, inside this transaction (see the docstring). ─────────────────────
AUDIT_TRIGGER = "billing_audit_log_immutability_trg"


def _audit_jsonb_key(column: str, key: str) -> tuple[str, str]:
    return (
        f"audit_log.{column}->>{key}",
        f"UPDATE audit_log SET {column} = jsonb_set({column}, '{{{key}}}', "
        f"to_jsonb(CAST(:new AS text))) WHERE lower({column} ->> '{key}') = :old",
    )


_AUDIT_UPDATES: tuple[tuple[str, str], ...] = (
    _move_id("audit_log"),
    (
        "audit_log.resource_id",
        "UPDATE audit_log SET resource_id = :new WHERE lower(resource_id) = :old",
    ),
    _move_name("audit_log"),
    _audit_jsonb_key("before", "tenant_id"),
    _audit_jsonb_key("before", "tenant_name"),
    _audit_jsonb_key("after", "tenant_id"),
    _audit_jsonb_key("after", "tenant_name"),
)

_PATH_ANCHOR = "/budget/"
_PATH_UPDATE: tuple[str, str] = (
    "billing_metering_events.path (/budget/<id>)",
    "UPDATE billing_metering_events "
    "SET path = replace(path, :anchor || :old, :anchor || :new) "
    "WHERE strpos(lower(path), :anchor || :old) > 0",
)

# Rows still on the OLD id, per touched column — every one must be 0 after the
# writes. Mirrors _UPDATES + _AUDIT_UPDATES + _PATH_UPDATE term for term.
_REMAINING: tuple[tuple[str, str], ...] = tuple(
    (label, f"SELECT count(*) FROM {table} WHERE {cond}")
    for label, table, cond in (
        *(
            (f"{t}.tenant_id", t, "tenant_id = CAST(:old AS uuid)")
            for t in (
                "token_budgets",
                "tenant_money_budget",
                "request_quota_config",
                "tenant_config",
                "tenant_webhooks",
                "markup_rules",
                "tenant_access_charge",
                "billing_cost_events",
                "billing_metering_events",
                "billing_metering_hourly",
                "billing_metering_daily",
                "billing_cost_daily",
                "billing_cost_actor_daily",
                "billing_cost_usage_daily",
                "token_usage",
                "budget_overage_alerts",
                "tenant_money_budget_adjustment",
                "billing_dlq_reattributions",
                "audit_log",
            )
        ),
        *(
            (f"{t}.tenant_name", t, "lower(tenant_name) = :old")
            for t in (
                "token_budgets",
                "tenant_config",
                "billing_cost_events",
                "billing_metering_events",
                "billing_metering_hourly",
                "billing_metering_daily",
                "billing_cost_daily",
                "token_usage",
                "audit_log",
            )
        ),
        (
            "billing_attribution_rules",
            "billing_attribution_rules",
            "CAST(:old AS uuid) IN (source_tenant_id, bill_tenant_id)",
        ),
        (
            "billing_cost_events.metadata->>attributed_from_tenant_id",
            "billing_cost_events",
            "lower(metadata ->> 'attributed_from_tenant_id') = :old",
        ),
        (
            "billing_metering_events.retarget_tenant_id",
            "billing_metering_events",
            "retarget_tenant_id = CAST(:old AS uuid)",
        ),
        (
            "billing_metering_events.actor_id",
            "billing_metering_events",
            "actor_id = CAST(:old AS uuid)",
        ),
        (
            "billing_metering_events.path",
            "billing_metering_events",
            "strpos(lower(path), :anchor || :old) > 0",
        ),
        (
            "billing_dlq_reattributions.reattributed_from/original_fields",
            "billing_dlq_reattributions",
            "lower(reattributed_from) = :old OR lower(original_fields ->> 'tenant_id') = :old",
        ),
        (
            "billing_dlq_archive.fields->>tenant_id",
            "billing_dlq_archive",
            "lower(fields ->> 'tenant_id') = :old",
        ),
        (
            "billing_delete_audit.tenant_ids",
            "billing_delete_audit",
            "CAST(:old AS uuid) = ANY (tenant_ids)",
        ),
        (
            "tenant_id_crosswalk.tenant_uuid",
            "tenant_id_crosswalk",
            "tenant_uuid = CAST(:old AS uuid)",
        ),
        ("audit_log.resource_id", "audit_log", "lower(resource_id) = :old"),
        *(
            (f"audit_log.{c}->>{k}", "audit_log", f"lower({c} ->> '{k}') = :old")
            for c in ("before", "after")
            for k in ("tenant_id", "tenant_name")
        ),
    )
)

# Protected: counted, logged, never written.
_LEFT_BEHIND: tuple[tuple[str, str], ...] = (
    (
        "billing_invoices (billing_invoice_immutability_trg + snapshot_sha256)",
        "SELECT count(*) FROM billing_invoices WHERE tenant_id = CAST(:old AS uuid)",
    ),
)

_TRIGGER_ENABLED = (
    "SELECT t.tgenabled FROM pg_trigger t JOIN pg_class c ON c.oid = t.tgrelid "
    "WHERE c.relname = 'audit_log' AND t.tgname = :name AND NOT t.tgisinternal"
)


def rekey(bind: sa.engine.Connection, *, old: str, new: str) -> dict[str, int]:
    """Move every tenant-sense value from ``old`` to ``new``; return rows moved per column.

    Refuses (before writing anything) if a unique slot is held on both ids, and
    raises (rolling everything back) if any touched column still holds ``old``
    afterwards or the audit_log immutability trigger is not enabled again.
    """
    params = {"old": old, "new": new, "anchor": _PATH_ANCHOR}
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
    moved[_PATH_UPDATE[0]] = bind.execute(sa.text(_PATH_UPDATE[1]), params).rowcount

    bind.execute(sa.text(f"ALTER TABLE audit_log DISABLE TRIGGER {AUDIT_TRIGGER}"))
    for label, sql in _AUDIT_UPDATES:
        moved[label] = bind.execute(sa.text(sql), params).rowcount
    bind.execute(sa.text(f"ALTER TABLE audit_log ENABLE TRIGGER {AUDIT_TRIGGER}"))

    enabled = bind.execute(sa.text(_TRIGGER_ENABLED), {"name": AUDIT_TRIGGER}).scalar_one_or_none()
    if enabled != "O":
        raise RuntimeError(
            f"alembic {revision}: {AUDIT_TRIGGER} is not ENABLED after the re-key "
            f"(tgenabled={enabled!r}); rolling back."
        )
    remaining = {
        label: n
        for label, sql in _REMAINING
        if (n := bind.execute(sa.text(sql), params).scalar_one())
    }
    if remaining:
        raise RuntimeError(
            f"alembic {revision}: rows still on {old} after the re-key: {remaining}; rolling back."
        )
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
