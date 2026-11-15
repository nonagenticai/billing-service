"""Shared building blocks for the ORM model package.

- ``Base`` is reused from ``src/db.py`` so every table registers on the same
  SQLAlchemy metadata.
- ``ASSET_KIND_ENUM`` is declared once here so SQLAlchemy emits a single
  ``CREATE TYPE`` per ``metadata.create_all()`` (tests). Production hits
  alembic 0002's DO block instead.
- ``NULL_*`` sentinels replace NULL on rollup-table writes (Postgres treats
  NULL != NULL, which would break ON CONFLICT upserts).
"""

from __future__ import annotations

import uuid
from datetime import UTC, datetime

from sqlalchemy import TypeDecorator
from sqlalchemy.dialects.postgresql import ENUM as PG_ENUM
from sqlalchemy.dialects.postgresql import UUID as PG_UUID

from db import Base

# Native Postgres enum for asset kinds. Used by exec_quality, asset_daily_stats,
# and asset_baseline. Declared once + reused so SQLAlchemy emits CREATE TYPE
# only once per metadata.create_all() (tests). In production, alembic 0002's
# raw DO block creates the type before any column references it.
#
# Why this matters: a previous version of this model declared `asset_kind`
# as String(32) "to keep tests working with create_all()". The runtime
# Postgres column is the real ENUM, so SQLAlchemy bound parameters as
# `$1::VARCHAR`, which Postgres refused with "operator does not exist:
# asset_kind = character varying" on every read filtering by asset_kind.
# Tying the model type to the real ENUM fixes prod queries; create_type=True
# lets tests' Base.metadata.create_all() also create the type fresh per
# session.
ASSET_KIND_ENUM = PG_ENUM(
    "workflow",
    "prompt",
    # `procedure` added 2026-06-14 (migration 0025) so the improvability governor's
    # procedure branch can accumulate exec_quality: any card that USES an agentic
    # procedure during DOING records procedure_used; qa-evaluate writes a supplemental
    # procedure-kind exec_quality row from it. Prod gets the value via the ALTER TYPE
    # migration; tests' create_all() pick it up from this list.
    "procedure",
    # asset-authoring kinds added 2026-06-17 (migration 0028) so the grounded
    # `oracle_tier=structural` verdict the n8n verify-consistency oracle stamps on
    # freshly-authored principle/requirement/decision/choice_option assets can be
    # recorded as a real exec_quality row (and exported by the by-tier gauges).
    # qa-evaluate writes a supplemental <kind>-kind row from the authored
    # deliverable. Prod gets these via the ALTER TYPE migration; tests' create_all()
    # pick them up from this list.
    "principle",
    "requirement",
    "decision",
    "choice_option",
    name="asset_kind",
    create_type=True,
    metadata=Base.metadata,
)


class DBUUID(TypeDecorator):
    """UUID type using native PostgreSQL UUID."""

    impl = PG_UUID(as_uuid=True)
    cache_ok = True

    def process_bind_param(self, value, dialect):
        if value is None:
            return value
        return value

    def process_result_value(self, value, dialect):
        if value is None:
            return value
        if isinstance(value, uuid.UUID):
            return value
        return uuid.UUID(str(value))


# Sentinel UUIDs used in place of NULL for FK columns in tables that have
# unique constraints involving them (CostDaily, TokenBudget, TokenUsage).
# PostgreSQL treats NULL != NULL, which breaks ON CONFLICT upserts.
NULL_LICENSE_ID = uuid.UUID("00000000-0000-0000-0000-000000000000")
NULL_APP_ID = uuid.UUID("00000000-0000-0000-0000-000000000001")
NULL_PLAN_ID = uuid.UUID("00000000-0000-0000-0000-000000000002")
# Sentinel for subject_id (the licenses Subject UUID = the end-user identity).
NULL_SUBJECT_ID = uuid.UUID("00000000-0000-0000-0000-000000000003")
# Sentinel for project_id (the kanban project spend is priced against, alembic
# 0036). ⛔ NOT NULL on CostDaily and it must stay that way: project_id is a
# member of uq_cost_daily -- the rollup's declared ON CONFLICT target and the
# ONLY database-side guard on the daily grain -- and a NULL there never matches
# itself, so the constraint would simply stop refusing duplicate project-less
# rows from any writer. ⚠️ NOT for the reason this comment used to give ("every
# rollup cycle would INSERT a fresh row"): services/rollup_engine.py DELETEs the
# whole bucket_date before inserting it (~:818), so a re-rollup never reaches ON
# CONFLICT. The sentinel is defence-in-depth against that DELETE being removed
# or narrowed, plus the residual concurrent-writer path. Means "no project was
# asserted", never a real or default project. ⚠️ markup_rules.project_id is NULLABLE on purpose and
# does NOT use this: there NULL is the tenant-wide wildcard, matching every other
# match dimension's convention.
NULL_PROJECT_ID = uuid.UUID("00000000-0000-0000-0000-000000000004")
# Sentinel for actor_id on billing_cost_actor_daily (alembic 0040): "no person acted" — platform or
# service-to-service work, or an emitter that sent no actor. NOT NULL for the same ON CONFLICT reason
# as the sentinels above. Never a real Keycloak sub.
NULL_ACTOR_ID = uuid.UUID("00000000-0000-0000-0000-000000000005")


def is_system_license(license_id: uuid.UUID | None) -> bool:
    """True when a rollup row's license_id is the licence-less sentinel (or None).

    ⛔ UNCHANGED ON PURPOSE, AND STILL THE INVOICE GATE. System/S2S cost is never
    attributed to a customer invoice line, so both the per-tenant invoice
    (``services.billing.generate_billing``) and the all-tenant overview
    (``routers.billing.compute_billing_overview``) exclude these rows. This is the
    Python-side counterpart to ``CostDaily.license_attributed_clause()`` (the SQL
    predicate); keeping the definition in one place stops the two billing paths
    from drifting on what "system cost" means. Deleting either would put system
    cost onto customer invoices, which is worse than the gap below.

    ⚠️ WHAT THE NAME OVERCLAIMS. This predicate cannot see system-ness. Nothing on
    the wire asserts it: ``NULL_LICENSE_ID`` is written by exactly one rule --
    ``license_id or NULL_LICENSE_ID`` in ``services.rollup_engine`` and
    ``services.cost_flusher`` -- so the sentinel means "no licence reached
    billing", full stop. Deliberate system/S2S cost and a licensed customer whose
    ``x-license-id`` header was dropped east-west land on the SAME value and
    become indistinguishable, and the second class is then silently written off.

    Measured on prod 2026-09-06 (``billing_cost_daily``, all time, SELECT-only):
    $5,870.33 total, of which $2,217.53 (37.8%) carries a real licence. Of the
    $3,652.80 the clause drops, **$3,386.52 across 22 tenants that DO own licences
    in the local mirror** is real customers' spend; only $266.28 across 32 tenants
    has no customer relationship to attribute to at all.

    :func:`classify_cost_attribution` is the fix: it separates those two
    populations WITHOUT changing this predicate or the value of the sentinel, so
    the exclusion above keeps holding exactly as it did while the written-off
    money stops being invisible.
    """
    return license_id is None or license_id == NULL_LICENSE_ID


# ---------------------------------------------------------------------------
# Attribution status -- the discriminator that splits the sentinel's two meanings
# ---------------------------------------------------------------------------
#
# WHY A SEPARATE COLUMN AND NOT ONE OF THE OTHER TWO SHAPES
#
# (a) A SECOND SENTINEL (keep ...0000 = system, mint ...0004 = "no licence
#     supplied") was rejected because ``NULL_LICENSE_ID`` is not a label, it is
#     load-bearing MECHANISM in three places that have nothing to do with
#     reporting:
#       * ``services.budget_debit._select_active_budgets`` narrows to
#         ``TokenBudget.license_id.in_({license_id, NULL_LICENSE_ID})``. That set
#         literal IS the money invariant: licence-less spend drains the
#         tenant-level pool and nothing else. A row carrying ...0004 would match
#         neither branch and would stop being capped -- turning a REPORTING gap
#         into an ENFORCEMENT gap, which is strictly worse.
#       * ``uq_cost_daily`` and ``uq_token_usage`` include ``license_id``. A new
#         value splits every existing conflict key, so the same day's spend
#         upserts onto two rows and the ON CONFLICT accumulate silently changes
#         shape.
#       * ``routers.budget`` and ``services.budget_status`` map the sentinel back
#         to ``None`` for the API. Two sentinels means every such site needs the
#         same edit, and the one that is missed fails open.
#
# (b) A BOOLEAN THE EMITTERS SET was rejected on shippability, measured rather
#     than assumed: ``services.license_resolver`` records ``app_id`` NULL on
#     401,316 of 401,316 cost events, and the flusher's own comment records
#     5,168 of 5,213 requests arriving with no licence -- because Istio ext_authz
#     runs only at the ingress gateway, so no east-west emitter ever receives
#     ``x-license-id`` to forward. A field only 0% of emitters populate reads
#     `false` = "not system" on every row until 21 n8n node packages and several
#     services all ship at once; until then it puts system cost ON customer
#     invoices. A representation that is wrong in the expensive direction until a
#     fleet-wide migration completes is not shippable.
#
# (c) THIS: a derived ``attribution_status`` alongside the id. ``license_id`` and
#     every upsert key, budget predicate and API mapping stay byte-identical, no
#     emitter changes, and the value is computed at rollup time from facts
#     billing ALREADY owns locally (the licence mirror ``request_quota_config``).
#     It is recorded as DERIVED, never asserted -- same discipline
#     ``license_resolver`` applies to a derived licence.

#: The caller (or the resolver) supplied a real licence. Invoiceable today.
ATTRIBUTION_ATTRIBUTED = "attributed"
#: No licence reached billing, but the billed tenant OWNS a licence the caller's
#: ``tenant_owns_licence`` counts as relevant -- a real customer whose header
#: was dropped in transit. Visible and attributable to that tenant; still not
#: invoiced (who pays is a business call). ⚠️ As of 2026-09-07 the rollup
#: caller (``services.rollup_engine``) scopes this to "owns a licence for the
#: SPECIFIC app the call's ``service_key`` maps to" wherever that app is
#: knowable, and only falls back to "owns ANY licence, for any product" for a
#: ``service_key`` the map does not cover -- see the caller for why.
ATTRIBUTION_UNLICENSED = "unlicensed"
#: No licence, and no customer relationship the caller's ``tenant_owns_licence``
#: recognises: the nil tenant sentinel, or (per the caller's current scoping) a
#: tenant holding no licence relevant to this call. Genuine system/S2S cost, OR
#: real usage of a product this tenant has never licensed -- both excluded from
#: every customer aggregate, exactly as before. This bucket is coarser than its
#: name: it cannot tell "no customer relationship at all" apart from "a real,
#: known customer, just not for this app" -- that distinction was rejected as a
#: fifth wire value (see the module docstring's rule (a) for why a new value is
#: not free), so a human reading a SYSTEM row needs ``tenant_id`` to tell the
#: two apart, not this label alone.
ATTRIBUTION_SYSTEM = "system"
#: The caller NAMED a licence and billing REFUSED it: the id is absent from the
#: local mirror, or it resolves to a DIFFERENT tenant than the one the event is
#: billed to, or the mirror does not consider it active. 15 chars — the
#: ``varchar(16)`` bound is real and ``tests/test_cp_license_adjudication.py``
#: pins it.
#:
#: ⛔ THIS IS NOT ``unlicensed`` AND MUST NEVER BE COLLAPSED INTO IT. ``unlicensed``
#: means the caller asserted NOTHING and we believe the tenant is a real customer
#: whose ``x-license-id`` was dropped in transit — an honest absence. This means
#: the caller asserted something and it was WRONG. Per
#: ``services.attribution_audit``'s own measurement (prod 2026-08-25, 7d):
#: **11,407 events / $363.56 named a licence owned by SOMEONE ELSE against 7,562
#: events / $86.76 that named their own — 81% of ATTRIBUTED spend was foreign.**
#: That module's docstring states the reason the two must stay apart: "That is
#: worse than a NULL: a NULL is honestly unknown, a mismatch is confidently
#: wrong."
#:
#: ⛔ THE LABEL IS NOT WHAT KEEPS THE ROW OFF AN INVOICE, and believing otherwise
#: is the trap this whole value is easiest to get wrong on. The invoice gate is
#: ``CostDaily.license_attributed_clause()``, which is a DENYLIST ON
#: ``license_id`` (``IS NOT NULL AND != NULL_LICENSE_ID``) and does not read this
#: column at all. A row carrying a refused licence in ``license_id`` would be
#: invoiced no matter what this column said. The refusal is therefore delivered
#: by ``services.cost_flusher`` REMOVING the id from the row; this value exists so
#: that, having removed it, the reason is still legible. Adding a value here
#: without the removal there would be a label over an unchanged invoice line.
#:
#: ⭐ IT IS DISCLOSED, NOT SILENT. ``services.invoices.measure_excluded_unbillable``
#: groups the ``not_(license_attributed_clause())`` population BY THIS COLUMN into
#: the invoice's ``excluded_by_status`` block, so a refused licence's spend shows
#: up on the invoice as excluded-and-named rather than vanishing.
#:
#: ⚠️ RESIDUAL, stated rather than left to be found: ``routers/billing.py``'s
#: overview reads ``if row.attribution_status == ATTRIBUTION_UNLICENSED: ... else:
#: platform_system += row.cost``, so on THAT report this population is folded into
#: platform system cost — exactly as ``unknown`` already is. Both are excluded
#: from every customer total either way, so no money moves; the overview simply
#: cannot name this bucket yet. Fixing it is a one-branch change in
#: ``routers/billing.py``, which is owned by another change set.
ATTRIBUTION_REFUSED_LICENSE = "refused_license"

#: The marker ``services.cost_flusher`` appends to a refused row's ``cost_source``,
#: mirroring the ``+resolved`` suffix ``services.license_resolver`` already uses
#: for a DERIVED attribution.
#:
#: ⭐ WHY A ``cost_source`` SUFFIX CARRIES THE VERDICT AND NOT A NEW COLUMN.
#: ``cost_source`` is already a ``uq_cost_daily`` dimension, so the rollup groups
#: by it and the marker reaches ``billing_cost_daily`` with NO migration, no new
#: column on a RANGE-partitioned hot-ingest table, and no window in which a pod
#: carrying the new flusher meets a database that lacks the column — the outage
#: class every ``ADDITIVE_MIGRATIONS`` entry in ``src/db.py`` exists to prevent.
#:
#: ⛔ IT LIVES HERE, NOT IN ``services.cost_flusher``, AND THAT IS THE POINT.
#: The WRITER is the flusher and the READER is ``services.rollup_engine``; if the
#: literal lived with either, the other would have to import across a services
#: boundary or — far likelier — hand-copy it. A hand-copied marker that drifts
#: from its writer is the exact defect ``models.db._common`` already records
#: about ``NULL_PROJECT_ID`` ("a duplicated sentinel that drifts from the column
#: default is exactly the defect this audit measures"). One home for the pair.
LICENSE_REFUSED_COST_SOURCE_MARKER = "+license_refused"


def cost_source_marks_refusal(cost_source: str | None) -> bool:
    """Did :data:`LICENSE_REFUSED_COST_SOURCE_MARKER` get appended to this value?

    A named predicate rather than a bare ``in`` at each site, for the reason this
    repo keeps rediscovering: two independently-written copies of one rule are
    two chances for one to drift (see ``services.cost_flusher``'s own comment on
    how the two ``provider`` parses in ``routers/budget.py`` came to disagree
    about one licence id).

    ⚠️ SUBSTRING, NOT SUFFIX, and deliberately. ``cost_source`` already carries
    one composable suffix (``+resolved``), so a future third marker could land
    after this one and an ``endswith`` would silently stop matching — failing in
    the direction of "not refused", which puts the row back on an invoice. A
    substring test degrades the other way.
    """
    return bool(cost_source) and LICENSE_REFUSED_COST_SOURCE_MARKER in cost_source


#: Pre-migration rows that no rollup has re-stamped. Never written by code.
ATTRIBUTION_UNKNOWN = "unknown"

#: The nil uuid, used as the "no tenant" sentinel (``services._parsers.NIL_TENANT_ID``).
#: Duplicated as a literal here rather than imported: ``models.db`` must not
#: depend on ``services``, and ``_parsers`` already imports from ``models.db``.
_NIL_TENANT_UUID = "00000000-0000-0000-0000-000000000000"


def classify_cost_attribution(
    license_id: uuid.UUID | None,
    *,
    tenant_id: str | None,
    tenant_owns_licence: bool,
    license_refused: bool = False,
) -> str:
    """Name WHY a cost row does or does not carry a licence.

    The single definition of the distinction ``NULL_LICENSE_ID`` cannot express.
    Pure, so both the Python rollup writer and alembic 0020's backfill SQL can be
    checked against the same four branches.

    ``tenant_owns_licence`` means "this tenant has at least one non-sentinel
    licence this caller counts as covering this call" -- evaluated by the caller,
    not here, against the local licence mirror (``request_quota_config``) billing
    already reads on every flush. This function does not know or care HOW the
    caller scoped it; it only knows the caller's answer.

    As of 2026-09-07, ``services.rollup_engine`` scopes it by ``service_key`` ->
    app (via the SAME ``cost_attribution_app_by_service_key`` map
    ``services.license_resolver`` uses to attribute a licence to an event), and
    falls back to "owns ANY licence, for any product" only for a ``service_key``
    that map does not cover. Neither this function nor its caller can filter on
    licence status/expiry AT THE BUCKET DATE -- ``request_quota_config`` carries
    no status/starts_at/expires_at/revoked_at column, see
    ``services.license_resolver``'s identical documented limitation.

    ⚠️ It is a DERIVED classification, not a caller's assertion, and it can still
    be wrong in the direction #141 chose deliberately: a tenant with no relevant
    licence at all reads ``system`` even when it is a real, known customer --
    just not one licensed for the product that generated this cost. That is
    coarser than "no customer relationship", but it is NOT the pre-#141 defect:
    the tenant-wide OR-of-all-products case ("owns nothing, anywhere") still
    correctly excludes only tenants with zero customer relationship. It cannot
    move money either way: nothing downstream bills on this column (see
    ``routers.billing.compute_billing_overview``, which reads it into
    report-only fields and leaves ``license_attributed_clause`` intact).

    ``license_refused`` is the caller's answer to a DIFFERENT question from
    ``tenant_owns_licence``: not "does this tenant own something relevant?" but
    "did this event NAME a licence that billing declined?". It is evaluated by
    the caller for exactly the same reason ``tenant_owns_licence`` is — this
    function stays pure and database-free so its branches can be checked without
    postgres — and today its one producer is ``services.rollup_engine``, reading
    the marker ``services.cost_flusher`` left on the event's ``cost_source``.

    ⛔ IT IS TESTED FIRST, AND THE ORDER IS LOAD-BEARING. A refused row reaches
    the rollup with ``license_id`` ALREADY NULLED by the flusher — that removal
    is what keeps it off the invoice — so every later branch here would read it
    as ``unlicensed`` or ``system`` and the refusal would be erased by the very
    mechanism that enforced it. Refusal is strictly more specific than absence
    and must win.

    ⚠️ ``license_refused`` DEFAULTS TO ``False`` so every pre-existing caller is
    byte-identical, and alembic 0020's SQL twin — which cannot express this
    branch — stays a correct backfill: it classifies rows written before any
    refusal could be recorded, and none of them can be refusals.
    """
    if license_refused:
        return ATTRIBUTION_REFUSED_LICENSE
    if not is_system_license(license_id):
        return ATTRIBUTION_ATTRIBUTED
    if tenant_id is None or str(tenant_id) == _NIL_TENANT_UUID:
        return ATTRIBUTION_SYSTEM
    return ATTRIBUTION_UNLICENSED if tenant_owns_licence else ATTRIBUTION_SYSTEM


def _new_uuid() -> uuid.UUID:
    """Generate a time-sortable UUIDv7 for primary keys."""
    return uuid.uuid7()


def _utcnow() -> datetime:
    return datetime.now(UTC)
