"""Batch insert cost events + price + debit budgets + update token-usage rollups."""

import uuid
from collections import Counter
from collections.abc import Callable, Mapping
from datetime import UTC, date, datetime
from decimal import Decimal
from typing import Any, cast

import redis.asyncio as aioredis
import structlog
from redis.exceptions import RedisError
from sqlalchemy import Table
from sqlalchemy.dialects.postgresql import insert as pg_insert
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

import observability.metrics as observability_metrics
from health_probes import (
    HEALTH_PROBE_EXCLUSION_REASON,
    match_health_probe,
    probe_endpoint,
)
from models.db import (
    ACTIVE_LICENSE_STATUS,
    LICENSE_REFUSED_COST_SOURCE_MARKER,
    NULL_LICENSE_ID,
    CostEvent,
    ProcessedCostEventKey,
    ProcessedCostMessage,
    TokenUsage,
)
from services import attribution_rules as attribution_svc
from services import funding as funding_svc
from services import money_budget as money_svc
from services import pricing as pricing_svc
from services._cost_schema import validate_cost_event
from services._parsers import (
    COST_METADATA_WHITELIST,
    DEFAULTED_COST_FIELDS,
    DELEGATED_BY_MAX_LEN,
    OPTIONAL_COST_METADATA_FIELDS,
    UNATTRIBUTED_TENANT_ID,
    _normalize_tenant_id,
    _parse_decimal,
    _parse_int,
    _parse_optional_decimal,
    _parse_usage_extra,
    _parse_uuid,
    derive_event_key,
    is_placeholder,
    note_event_key_source,
    note_placeholder_value,
    original_tenant_label,
    parse_event_ts,
    parse_optional_metadata_str,
    stored_tenant_name,
)
from services.budget import (
    BudgetDebit,
    debit_budget,
    release_budget_reservations,
    split_debit_outcomes,
)
from services.license_resolver import (
    derive_license,
    get_app_licence_map,
    get_app_map,
    get_owner_map,
    get_resolution_map,
    parse_service_key_app_map,
    record_app_resolved,
    record_app_unattributed,
    record_resolved,
    resolve_app,
    resolve_asserted_app_keys,
    split_asserted_app,
)
from services.project_owner_map import get_project_owner_map
from services.tenant_names import is_tenant_archived
from settings import get_settings
from tenancy import is_registered_tenant_id, tenant_level_project_id
from timeutil import current_month_start, current_week_start

logger = structlog.stdlib.get_logger(__name__)

#: The bounded ``billing_cost_events.quantity_unit`` domain (alembic 0021).
#: 'megapixels'/'passages' are not reachable via cost_model derivation today
#: (see services.pricing) but are valid values an emitter may assert directly.
QUANTITY_UNITS: frozenset[str] = frozenset(
    {"tokens", "characters", "seconds", "pages", "megapixels", "passages", "requests"}
)

#: The bounded ``billing_cost_events.cost_basis`` domain (alembic 0021).
COST_BASIS_VALUES: frozenset[str] = frozenset({"cash", "internal_transfer"})

# ---------------------------------------------------------------------------
# Adjudicating a licence the CALLER asserted
# ---------------------------------------------------------------------------
#
# ⛔ WHAT WAS WRONG. This flusher accepted ANY well-formed uuid as ``license_id``
# — ``_parse_uuid`` does a stdlib parse and nothing else — so an emitter could
# name any licence in the estate and be billed against it.
# ``services.attribution_audit`` measured the consequence on prod 2026-08-25
# over a 7-day window: **11,407 events / $363.56 named a licence owned by
# SOMEONE ELSE against 7,562 events / $86.76 that named their own — 81% of
# ATTRIBUTED spend was foreign.** That module states why it is worse than a
# NULL: "a NULL is honestly unknown, a mismatch is confidently wrong."
#
# It is not only a label. ``debit_budget`` scopes the cascade to the EVENT's
# tenant, so a foreign licence's spend drains the billed tenant's shared pool
# while the named licence's own budget stays untouched — the audit recorded one
# licence reading 1,321 consumed tokens against 94,624,434 actually streamed for
# it, i.e. an enforcement gate seeing 0.0% of a 10M budget where reality was
# 946%.
#
# ⛔ THE REFUSAL IS NOT A DROP, AND THAT IS NOT TIMIDITY. By the time an event
# reaches this module the provider has ALREADY been paid. Dropping the row would
# convert an attribution fault into an UNMETERED one — strictly worse, and
# unrecoverable, because billing is append-only with no refund path. This is the
# same reasoning ``routers/budget.py`` and the unregistered-tenant block below
# both record. The row is stored, it still rolls up, and it still debits; only
# the false attribution is removed.
#
# ⭐ AND REMOVING THE ID IS WHAT ENFORCES IT — not the label. The invoice gate is
# ``CostDaily.license_attributed_clause()``, a DENYLIST ON ``license_id``
# (``IS NOT NULL AND != NULL_LICENSE_ID``) which does not read
# ``attribution_status`` at all. A row that kept a refused licence in
# ``license_id`` would be invoiced no matter what any status column said. So the
# id is nulled here; ``ATTRIBUTION_REFUSED_LICENSE`` exists so that, having
# nulled it, the REASON survives to the rollup and onto the invoice's own
# ``excluded_by_status`` disclosure block
# (``services.invoices.measure_excluded_unbillable``).

#: The emitters whose ASSERTED ``license_id`` is adjudicated here.
#:
#: ⛔ A FROZENSET OF ONE, ON PURPOSE. CommonPractice is the emitter that began
#: sending an explicit ``license_id`` (with ``subject_id``, ``actor_id`` and
#: ``event_key``), so it is the only one whose assertion is rich enough to
#: adjudicate AND the only one whose behaviour under refusal has been reasoned
#: about. Every other emitter is untouched and continues to be believed —
#: deliberately, because arming this fleet-wide against a mirror that is
#: 5-minutes-stale by construction (see ``license_resolver.get_owner_map``) would
#: strip real attribution from real invoices on a sync lag. Estate rule: DO NOT
#: CLOSE A GATE BEFORE ITS COUNTER READS 0 — ``billing_license_refused_total``
#: is that counter, and widening this set is the arming step, one emitter at a
#: time, after reading it.
LICENSE_ADJUDICATED_SERVICE_KEYS: frozenset[str] = frozenset({"commonpractice"})

#: The bounded ``reason`` domain of ``billing_license_refused_total``. Kept as a
#: tuple beside the pure function that produces it (never imported from the
#: metrics module) for the same reason :data:`PROJECT_ID_REFUSED_REASONS` is: the
#: adjudication stays testable with no Prometheus registry in scope.
LICENSE_REFUSAL_REASONS: tuple[str, ...] = (
    # The mirror has no such licence at all. A forged or stale id at the emitter
    # — or a licence provisioned inside the mirror's TTL. Distinct from the two
    # below because the owner of the fix is different.
    "unknown_license",
    # The licence exists and belongs to a DIFFERENT tenant than the one this
    # event is billed to. THE 81% CLASS.
    "foreign_tenant",
    # The licence exists, belongs to this tenant, and the mirror does not call it
    # active. ⚠️ Revocation only — see the residual note on
    # :func:`adjudicate_asserted_license`.
    "inactive_license",
)

# ⚠️ The ``cost_source`` marker this module APPENDS lives in ``models.db`` as
# :data:`~models.db.LICENSE_REFUSED_COST_SOURCE_MARKER`, NOT here, because its
# reader is ``services.rollup_engine``. See that constant for why one home
# matters. ``billing_cost_events.cost_source`` is ``String(64)`` and the longest
# value this module can produce is ``"unknown+license_refused"`` (23); a test
# pins that bound.


def adjudicate_asserted_license(
    license_id: uuid.UUID | None,
    *,
    tenant_id: str | None,
    service_key: str | None,
    owner_map: Mapping[uuid.UUID, tuple[str, str]],
    adjudicated_service_keys: frozenset[str] = LICENSE_ADJUDICATED_SERVICE_KEYS,
) -> str | None:
    """Should this event's ASSERTED licence be refused, and why?

    Returns one of :data:`LICENSE_REFUSAL_REASONS`, or ``None`` meaning "accept"
    — which covers three genuinely different accepts and deliberately does not
    distinguish them, because the caller does the same thing in all three: the
    emitter is not adjudicated, nothing was asserted, or the assertion checked
    out.

    Pure, and every collaborator is an argument. Same discipline as
    :func:`_resolve_project_id` below and for the same stated reason: this
    decides whether real money reaches an invoice line, so its branches must be
    checkable without a database or a metrics registry in scope, or they will
    quietly stop being checked.

    ⛔ ``adjudicated_service_keys`` IS A NARROWING AND CAN ONLY EVER REFUSE LESS.
    An emitter outside it returns ``None`` on the first line, before the map is
    consulted at all, so no existing producer's attribution can change.

    ⛔ ABSENCE IS NOT A REFUSAL. ``license_id is None`` accepts. This function
    adjudicates an ASSERTION; an event that asserted nothing has nothing to be
    wrong about, and it is already handled by ``license_resolver`` +
    ``billing_license_unattributed_persisted_total``. Refusing here would
    double-count the pre-existing unattributed population as a new defect.

    ⚠️ **POINT-IN-TIME BLIND, AND THE BRIEF ASKED FOR MORE THAN THE DATA CAN
    GIVE.** The intended rule was "active at the event's ``created_at``, or
    revoked after it". That is NOT COMPUTABLE HERE: the licence mirror
    ``request_quota_config`` carries no lifecycle timestamps at all — no
    ``issued_at``, ``valid_from``, ``expires_at`` or ``revoked_at`` (see
    ``models/db/quota.py`` and ``license_resolver._build_owner_map``). ``status``
    is upstream's CURRENT stored value, and upstream returns ``lic.status``
    rather than its derived ``effective_status()``, so a licence past its expiry
    whose stored status is still ``active`` reads active here. What this
    implements is therefore **existence + ownership + current revocation**, and
    it is stated that way rather than described as the temporal rule. The
    consequence has a SIGN: a licence revoked AFTER the event is refused
    retroactively, which under-attributes rather than over-attributing — the row
    is stored and one UPDATE restores it, whereas the opposite bias would put an
    un-owed line on an immutable, gaplessly-numbered invoice. Closing it properly
    needs the lifecycle columns mirrored by
    ``POST /api/budget/sync-from-licenses`` first.

    ⚠️ Tenant comparison is case-insensitive on both sides. ``tenant_id`` here
    has been through ``_normalize_tenant_id`` and the mirror's column is a real
    ``uuid``, so today both are already canonical lowercase — the casefold is
    defence against a future caller passing a raw header value, where a bare
    ``!=`` would refuse a tenant for spelling its own uuid in upper case.
    """
    if not service_key or service_key not in adjudicated_service_keys:
        return None
    if license_id is None:
        return None
    owner = owner_map.get(license_id)
    if owner is None:
        return "unknown_license"
    owner_tenant, status = owner
    if not tenant_id or owner_tenant.casefold() != str(tenant_id).casefold():
        return "foreign_tenant"
    # ``== active`` and never ``!= revoked`` — the same fail-CLOSED choice
    # ``license_resolver``'s own predicate documents, so an unrecognised or
    # newly-added upstream status is refused rather than accepted by default.
    if status != ACTIVE_LICENSE_STATUS:
        return "inactive_license"
    return None


def birth_license_refused_series() -> tuple[tuple[str, str], ...]:
    """Export ``billing_license_refused_total`` at 0 for every reachable label pair.

    ⭐ ACCEPTANCE: after this runs, an emitter billing adjudicates has a series
    even though it has refused nothing.

    Same reasoning as ``license_resolver.birth_resolved_series``, and it matters
    MORE here because the healthy steady state of this counter IS zero. A
    prometheus_client child is only created on first ``.inc()``, so without this
    "no licence has ever been refused" and "the adjudication is not running" are
    the same empty vector, and any alert written over it is vacuously green —
    which is exactly the state the fleet would be in if
    :data:`LICENSE_ADJUDICATED_SERVICE_KEYS` were ever accidentally emptied.

    ⛔ THE BIRTH LIST IS THE REACHABLE SET, NOT A ROSTER BESIDE IT. It is the
    cartesian product of the two bounded domains the one call site can actually
    produce, DERIVED from those domains rather than hand-copied beside them, so
    widening either automatically births the new series.
    """
    born = tuple(
        (key, reason)
        for key in sorted(LICENSE_ADJUDICATED_SERVICE_KEYS)
        for reason in LICENSE_REFUSAL_REASONS
    )
    for key, reason in born:
        observability_metrics.cp_license_refused_total.labels(service_key=key, reason=reason)
    return born


def rollup_tenant_name(tenant_names: Mapping[str, str], tenant_id: str) -> str:
    """The ``token_usage.tenant_name`` for ``tenant_id``, or the absence marker.

    ⛔ THE DEFAULT IS ``""``, NEVER THE UUID. This read used to be spelled
    ``tenant_names.get(tenant_id, tenant_id)`` inline, which made the ONE rollup
    write path the only place in this service that still wrote a uuid into a
    display column. :func:`services._parsers.stored_tenant_name` establishes
    ``""`` as the canonical "the registry could not resolve a name" value for
    exactly this column family, and states why: a uuid is non-empty and distinct
    per tenant, so it satisfies every downstream truthiness guard and
    IMPERSONATES a name, while ``""`` asserts nothing. Both event write paths
    (``cost_flusher`` above, ``event_flusher``) already honour that contract via
    that helper; this made the third one agree.

    ⚠️ Honest about reachability: ``tenant_names`` is populated from the same
    ``survived`` loop that builds ``token_counts``, under the same
    ``excluded_from_billing`` guard, so today every key looked up here is
    present and the default does not fire in production. It is kept — and
    extracted into a named function — because the VALUE of a defensive default
    is a contract statement that outlives the loop structure that currently
    makes it unreachable, and because a contract nothing can test is a contract
    nothing protects.

    Values already IN the map are passed through verbatim, ``""`` included: they
    came from ``stored_tenant_name`` and have been through its rules.
    """
    return tenant_names.get(tenant_id, "")


#: :func:`_resolve_project_id`'s second return value, one of these or ``None``.
#: Mirrors ``observability.metrics.PROJECT_ID_REFUSED_REASONS`` exactly — kept
#: as a plain tuple here (not imported from there) so this module's pure
#: resolution logic has zero dependency on the metrics module and stays
#: testable with no Prometheus registry in scope; a test asserts the two stay
#: in sync.
PROJECT_ID_REFUSED_REASONS: tuple[str, ...] = (
    "is_tenant_id",
    "registry_unavailable",
    "owner_map_unavailable",
    "foreign_tenant",
    "unknown_project",
    "project_tenant_unset",
)

#: Ownership verdicts that are POSITIVE knowledge of a mismatch -- the only ones
#: ``enforce`` mode may null a project_id on. ``owner_map_unavailable`` (no map)
#: and ``project_tenant_unset`` (fr knows the project but it has no tenant of
#: record) are absence of knowledge and always KEEP the value.
PROJECT_OWNERSHIP_REFUSABLE: frozenset[str] = frozenset({"foreign_tenant", "unknown_project"})


def _resolve_project_id(
    raw: Any,
    *,
    parse_uuid: Callable[[Any], uuid.UUID | None] | None = None,
    check_tenant: Callable[[str], bool] | None = None,
    tenant_id: str | None = None,
) -> tuple[uuid.UUID | None, str | None]:
    """Resolve the wire's ``project_id``, refusing one that encodes a tenant.

    ⭐ OWNER RULING 2026-10-07 (tenant-level projects): project-less apps
    (core_app, worker_management) send ``tenancy.tenant_level_project_id`` of
    their tenant -- the reversed-hex form, NOT the raw tenant id. ``tenant_id``
    here is the event's own EFFECTIVE (already resolved) tenant id:

    * ``project_id == tenant_level_project_id(tenant_id)`` -- KEPT, no reason:
      a tenant-level project, counted as project coverage, and the caller
      skips fr ownership adjudication (it is not an fr project).
    * ``project_id == tenant_level_project_id(<another registered tenant>)``
      -- REFUSED as ``is_tenant_id``: a cross-tenant mislabel, the same class
      as D1 below. The mapping is an involution, so the owning tenant is
      ``tenant_level_project_id(project_id)`` with no lookup table.
    * ``project_id`` == ANY registered tenant id, the event's OWN included --
      REFUSED as ``is_tenant_id``, unchanged (the ruling keeps this guard so
      ``BillingProjectIdEqualsTenant`` stays meaningful).

    D1 (billing-licensing estate repair, round 16): measured on prod
    2026-09-17, two distinct tenant uuids were being used as ``project_id``
    under a DIFFERENT payer tenant — ``0195f48b-…-0005`` (129 rows) and ``-0004``
    (37 rows). The same-row ``project_id == tenant_id`` forged-project detector
    (``services.attribution_audit._PROJECT_SELF_TENANT_SQL`` /
    ``billing_project_id_equals_tenant_projects``) cannot see this class: it
    only compares a row against ITS OWN tenant_id, and these rows carry
    someone else's.

    Pure aside from the two injected callables (both pure themselves —
    `_parse_uuid` does no I/O and `is_registered_tenant_id` is documented never
    to raise, backed by the checked-in snapshot even with no live registry
    refresh): no metrics, no logging, no DB. That is deliberate — it is what
    lets this be unit-tested without a Prometheus registry or a database, and
    it is why the reasons are :data:`PROJECT_ID_REFUSED_REASONS`, a plain tuple
    here rather than a metrics-module import.

    ⚠️ `parse_uuid`/`check_tenant` default to ``None`` and are resolved to the
    module-level `_parse_uuid` / `is_registered_tenant_id` INSIDE the body, not
    as ordinary default-argument values. A default argument is bound ONCE, at
    function-definition time, into the function object — a test that
    monkeypatches `cost_flusher.is_registered_tenant_id` afterwards would
    silently keep hitting the original object through an already-captured
    default, exactly as invisibly as if the patch had never run. Resolving by
    name inside the body is an ordinary global lookup, evaluated on every call,
    so a monkeypatched module attribute is honoured immediately — the same
    reason the row-builder's own call site above uses a bare name for
    `is_registered_tenant_id` rather than capturing it into a local at import.

    Returns ``(resolved_project_id, refused_reason)``:

    * ``(None, None)`` — nothing on the wire, or unparseable. The ordinary
      "no project asserted" case; never a sentinel (see the row-builder's own
      comment on why ``project_id`` is never defaulted).
    * ``(uuid, None)`` — an ordinary project id, kept as-is.
    * ``(uuid, None)`` also for the event's own tenant-level project id.
    * ``(None, "is_tenant_id")`` — REFUSED: `check_tenant` positively confirmed
      the value is a registered tenant id, or ANOTHER registered tenant's
      tenant-level project id. Nulled, never the whole event.
    * ``(uuid, "registry_unavailable")`` — the classification itself could not
      answer. ⛔ REFUSE ONLY ON POSITIVE KNOWLEDGE: the value is KEPT unchanged
      here, exactly as `check_tenant` failing to raise would have kept it, so a
      defensive fault in a check that is not supposed to be able to fail can
      never erase a caller's real project attribution. `check_tenant` is
      documented to never raise; this branch exists anyway because the
      guarantee lives in a DIFFERENT module (`tenancy`) than the one making
      the promise about it, and the caller of this function runs on the hot
      ingest path — same "do not close a gate on input you cannot trust" rule
      `cost_events_unregistered_tenant_total`'s own arming criterion is built
      on.
    """
    parse_uuid = parse_uuid if parse_uuid is not None else _parse_uuid
    check_tenant = check_tenant if check_tenant is not None else is_registered_tenant_id
    resolved = parse_uuid(raw)
    if resolved is None:
        return None, None
    canon = str(resolved)
    if tenant_id and canon == tenant_level_project_id(tenant_id):
        return resolved, None
    # The tenant this value would be the tenant-level project of (involution).
    owner_of_tenant_level = tenant_level_project_id(canon)
    try:
        is_tenant = check_tenant(canon)
        is_foreign_tenant_level = (
            owner_of_tenant_level is not None
            and owner_of_tenant_level != canon
            and check_tenant(owner_of_tenant_level)
        )
    except Exception:  # pragma: no cover - check_tenant is documented never to raise
        return resolved, "registry_unavailable"
    if is_tenant or is_foreign_tenant_level:
        return None, "is_tenant_id"
    return resolved, None


def _adjudicate_project_owner(
    project_id: uuid.UUID,
    *,
    tenant_id: str,
    owner_map: Mapping[str, str | None] | None,
    mode: str,
) -> tuple[uuid.UUID | None, str | None, dict[str, Any] | None]:
    """Check an asserted ``project_id`` against fr's project -> tenant map.

    2026-09-28 decision: per-project numbers are a product surface, so a trusted
    pod must not be able to book spend onto another tenant's project. See
    ``services.project_owner_map`` for the source of truth and cache.

    Pure: no I/O, no metrics. Returns ``(project_id_to_store, reason, meta)``:

    * owned -> ``(project_id, None, None)``, nothing counted.
    * ``foreign_tenant`` / ``unknown_project`` (positive knowledge) -- in
      ``observe`` mode the value is KEPT and ``meta`` is
      ``{"project_id_ownership": {"verdict", "registry_tenant"}}``; in
      ``enforce`` mode it is NULLED and ``meta`` is
      ``{"project_id_refused": {"asserted", "reason"}}``.
    * ``owner_map_unavailable`` (no map: fr down, token/URL unset, empty map)
      -> KEPT in BOTH modes; meta is ``{"project_id_ownership": {"verdict":
      "unverified", "reason": "owner_map_unavailable"}}``. Refuse only on
      positive knowledge. (Counter label is ``owner_map_unavailable`` so it
      cannot be confused with ``_resolve_project_id``'s tenancy-registry
      ``registry_unavailable``; the metadata reason uses the same name.)
    * ``project_tenant_unset`` (fr knows the project, no tenant of record) ->
      KEPT in BOTH modes; meta is ``{"project_id_ownership": {"verdict":
      "unverified", "reason": "project_tenant_unset"}}``.

    Any mode other than ``"enforce"`` behaves as observe: an unrecognised mode
    must never be the thing that starts erasing attribution.
    """
    if not owner_map:
        return (
            project_id,
            "owner_map_unavailable",
            {"project_id_ownership": {"verdict": "unverified", "reason": "owner_map_unavailable"}},
        )
    pid = str(project_id)
    if pid not in owner_map:
        verdict, registry_tenant = "unknown_project", None
    else:
        registry_tenant = owner_map[pid]
        if registry_tenant is None:
            return (
                project_id,
                "project_tenant_unset",
                {
                    "project_id_ownership": {
                        "verdict": "unverified",
                        "reason": "project_tenant_unset",
                    }
                },
            )
        if registry_tenant == str(tenant_id).lower():
            return project_id, None, None
        verdict = "foreign_tenant"
    if mode == "enforce" and verdict in PROJECT_OWNERSHIP_REFUSABLE:
        return None, verdict, {"project_id_refused": {"asserted": pid, "reason": verdict}}
    return (
        project_id,
        verdict,
        {"project_id_ownership": {"verdict": verdict, "registry_tenant": registry_tenant}},
    )


def _parse_bounded_str(val: Any, allowed: frozenset[str]) -> str | None:
    """A wire value that is honoured verbatim only if it is in the bounded set.

    An emitter that sends something outside the domain (a typo, a future value
    this pod predates) is treated as though it sent nothing — the caller falls
    through to its own derivation — rather than storing an unvalidated string
    a bounded column is supposed to rule out.
    """
    if val is None:
        return None
    s = str(val).strip()
    return s if s in allowed else None


async def flush_costs(
    costs: list[dict[str, Any]],
    session_factory: async_sessionmaker[AsyncSession],
    redis_conn: aioredis.Redis | None,
    consumer_group: str | None = None,
    dead_letter_stream: str | None = None,
) -> int:
    """Batch insert cost events and update token usage counters.

    The event's `cost_usd` is treated as a hint only — metering computes the
    authoritative cost via :func:`services.pricing.compute_cost_usd` against
    the effective-dated `provider_pricing` table. Drift >5% between a NON-ZERO
    hint and authoritative emits a `cost_drift` warning; a zero hint means the
    emitter priced nothing and emits `cost_hint_absent` at INFO instead (it is
    not a disagreement — see the branch itself); pricing misses keep the hint and
    log `pricing_miss`.

    ``consumer_group`` enables ACK-after-commit (see ``flush_events``).

    Events that fail the minimum billable schema (``services._cost_schema``) are
    quarantined to ``dead_letter_stream`` instead of being inserted as a
    fail-open $0/NULL row, and their metric ``cost_dead_letter_total`` fires so
    the corruption is loud rather than silent. When ``dead_letter_stream`` is
    None (e.g. a direct test insert) malformed events are simply dropped.
    """
    if not costs:
        return 0

    batch = list(costs)
    costs.clear()
    # Snapshot ack coordinates before any await — the consumer keeps mutating
    # `costs` between scheduling points.
    pending_acks: list[tuple[Any, Any]] = []
    if consumer_group is not None and redis_conn is not None:
        for c in batch:
            stream = c.get("_stream_name")
            msg_id = c.get("_msg_id")
            if stream is not None and msg_id is not None:
                pending_acks.append((stream, msg_id))

    # Read ONCE per flush, not per event: get_settings() is lru_cached but the
    # per-event call would still make the batch's behaviour depend on when the
    # cache was cleared mid-loop. One batch, one decision.
    settings = get_settings()
    exclude_unregistered = settings.billing_exclude_unregistered_tenants
    exclude_health_probes = settings.billing_exclude_health_probes
    # Project -> tenant ownership (2026-09-28). One cached whole-map read per
    # batch, never per event; None = no answer (see services.project_owner_map).
    project_ownership_mode = settings.billing_project_ownership_mode
    project_owner_map = await get_project_owner_map(
        settings.fr_base_url, settings.fr_internal_token
    )

    token_counts: dict[tuple[str, str | None, str, date], tuple[int, int]] = {}
    # tenant_id → tenant_name mapping accumulated per batch (last writer wins;
    # all rows for the same tenant_id carry the same name).
    tenant_names: dict[str, str] = {}
    # Per-(tenant, license) (in, out) totals for cascade-debit on the budget
    # path. Keyed by LICENSE, not just tenant: a tenant's budgets are
    # per-license, so aggregating to the tenant threw away the one fact the
    # debit needs and let license A's traffic drain license B's allowance.
    # License-less (system/S2S) spend keys on NULL_LICENSE_ID — the same
    # sentinel the token_usage rollup below uses — and therefore debits only
    # the tenant-level pool.
    license_io: dict[tuple[str, uuid.UUID], tuple[int, int]] = {}
    # ⛔ THE PARALLEL MAP TO license_io, AND IT HAS TO BE A MAP. Per
    # (tenant, license) — the SAME key as license_io — the reservation handles
    # this group settles, each with its own token total:
    # {(tenant, license): {reservation_id: tokens}}. The release is PER-BATCH,
    # not per-event (license_io above collapses every event of the group into one
    # (in, out) pair, and one `release_budget_reservations` call settles the lot),
    # so a SCALAR handle has nothing to map onto: N events in a batch can carry N
    # distinct reservation ids, and each one's own key must be decremented by its
    # own share. Without this, settlement released from an anonymous per-tenant
    # pool and drained OTHER callers' live reservations — 151.6 harmful
    # over-releases/hour, measured (billing-service #178). Keys are `str` because
    # they travel to Redis.
    license_handles: dict[tuple[str, uuid.UUID], dict[str, int]] = {}
    # ⛔ ENFORCEMENT STAYS ON THE SOURCE TENANT. id(row) -> (source tenant_id,
    # source tenant_name) for every row an attribution rule re-tenanted (alembic
    # 0041, services.attribution_rules). The STORED row bills the bill tenant,
    # but the budget debit, the token_usage rollup and the reservation
    # settlement below key on the source: the pre-flight reservation was taken
    # against the source tenant's Redis pool, so settling it on the bill tenant
    # would leak the one pool and over-release the other.
    attribution_source: dict[int, tuple[str, str]] = {}
    # id(row) -> the app KEY the caller asserted in `app_id` (the X-App-Id slug,
    # e.g. "core-app"). Every inference-router caller is a registered app in
    # licences and sends its key, not its uuid; the app-derivation block below
    # resolves it through `get_app_key_map`. A row in this dict is NEVER derived
    # from its licence: for router traffic the licence names the router, so a
    # licence-derived app would be a guessed caller. Registered at the
    # `pre_rows.append` site, so every key here is a row kept alive by pre_rows.
    asserted_app_key: dict[int, str] = {}

    # Pre-build per-event row dicts (without authoritative cost_usd / pricing_id).
    # Pricing lookup + the idempotency claim happen inside the DB session below.
    # Each entry carries its Redis stream message id (the 4th tuple element) so
    # the post-claim filter can drop already-processed redeliveries; None means
    # the event did not arrive via the stream (e.g. a direct test insert).
    pre_rows: list[tuple[dict[str, Any], str | None, datetime, str | None]] = []
    # id(row) of every row whose wire carried a `funding` field (valid OR not).
    # Those are never re-derived: a valid value is honoured, an invalid one is
    # stored NULL and counted. See services.funding. A side set, not a row key,
    # because every row dict is splatted verbatim into the INSERT.
    funding_asserted: set[int] = set()

    # Malformed events (schema-invalid) — quarantined to the dead-letter stream
    # rather than billed as a fail-open $0/NULL row. (raw event dict, reason).
    dead_letters: list[tuple[dict[str, Any], str]] = []

    now = datetime.now(UTC)

    for c in batch:
        malformed_reason = validate_cost_event(c)
        if malformed_reason is not None:
            dead_letters.append((c, malformed_reason))
            continue

        input_tokens = _parse_int(c.get("input_tokens", "0"))
        output_tokens = _parse_int(c.get("output_tokens", "0"))
        cache_creation = _parse_int(c.get("cache_creation_input_tokens", "0"))
        cache_read = _parse_int(c.get("cache_read_input_tokens", "0"))
        cost_usd_hint = _parse_decimal(c.get("cost_usd", "0"))

        license_id = _parse_uuid(c.get("license_id", ""))
        # One label expression, read once, used by BOTH counters below. The
        # unattributed counter is meaningless without the population it came
        # from: 10 unattributed events is a catastrophe out of 10 and noise out
        # of 10,000, and until now no flush counter carried service_key at all
        # (billing_cost_events_flushed_total is labelled {status} only, and its
        # label set is load-bearing for the gitops alert rules). Incrementing
        # the denominator HERE, in the same iteration and off the same
        # expression, is what makes the ratio safe: the two cannot drift, and no
        # future edit can move one without moving the other.
        service_key_label = c.get("service_key") or "unknown"
        observability_metrics.cost_events_by_service_total.labels(
            service_key=service_key_label
        ).inc()
        if license_id is None:
            # License-less spend is BILLABLE and CAPPED (it drains the
            # tenant-level pool — see budget_debit._select_active_budgets), but
            # it can never be attributed to a customer invoice line, because
            # CostDaily.license_attributed_clause drops it from every
            # license-keyed aggregate. That makes it an INVOICING gap rather
            # than an enforcement one, and until now nothing measured it.
            #
            # Baseline when this counter was added (2026-08-11, 7d window):
            # 5,274 of 9,888 cost events — 53% of events and 64% of tokens —
            # carried no license_id. Root cause is upstream of billing: Istio
            # ext_authz runs only at the ingress gateway, so east-west calls
            # never receive license-authz's x-license-id header and no emitter
            # forwards one. Deliberately NOT alerted on: a threshold over a 53%
            # baseline fires immediately and teaches everyone to ignore it.
            #
            # ⚠️ AND THE NAME ABOVE IS A LIE ABOUT WHERE THIS RUNS. This site is
            # INGRESS: it fires while the wire is being parsed, BEFORE the
            # idempotency claim and BEFORE services.license_resolver gets a
            # chance to attribute the row. It measures "the caller did not
            # assert a licence", not "the row is unattributed". Measured over a
            # 10h window on 2026-09-05, strictly after that day's truncate:
            # +2,089 here against 27,941 stored rows of which only 113 had a
            # NULL license_id — the resolver filled ~94% of what this counted.
            # Reading it as an invoicing gap has caused false alarm in at least
            # three sessions.
            #
            # So BOTH counters are incremented, at this one site, off the one
            # label expression — the honest name and the misnamed one, exact
            # aliases. The old name is NOT retired here because it is
            # load-bearing for two live PrometheusRules and three rule-test
            # fixtures in gitops-production; re-pointing those is a gitops
            # change and is deliberately not smuggled into this PR. The
            # POST-RESOLUTION number, which is what an operator actually wants,
            # is billing_license_unattributed_persisted_total, counted over
            # `survived` below.
            observability_metrics.license_caller_unasserted_total.labels(
                service_key=service_key_label
            ).inc()
            observability_metrics.license_unattributed_total.labels(
                service_key=service_key_label
            ).inc()
        subject_id = _parse_uuid(c.get("subject_id", ""))
        # WHO ACTED, distinct from subject_id (who pays). NULLABLE and NEVER
        # DEFAULTED (alembic 0021 decision 1) — a missing/invalid actor_id
        # stays None, exactly like subject_id above. No emitter sends this yet;
        # this is the wire contract future emitters land on.
        raw_actor_id = c.get("actor_id", "")
        actor_id = _parse_uuid(raw_actor_id)
        # A NON-EMPTY actor that does not parse is a broken emitter, not an
        # absent actor — it still lands as NULL (never guessed), but it is
        # counted so the two are distinguishable. Empty/absent is not malformed.
        if raw_actor_id and actor_id is None:
            observability_metrics.actor_malformed_total.labels(service_key=service_key_label).inc()
        # Wire-asserted quantity_unit / cost_basis, honoured verbatim if valid.
        # Both are re-derived below (after pricing resolves) when the wire sent
        # nothing usable — see the "Authoritative pricing" loop.
        wire_quantity_unit = _parse_bounded_str(c.get("quantity_unit"), QUANTITY_UNITS)
        wire_cost_basis = _parse_bounded_str(c.get("cost_basis"), COST_BASIS_VALUES)
        # What the vendor actually charged. NEVER defaulted to 0 — absence must
        # stay absence (alembic 0021 decision 3).
        vendor_cost_usd = _parse_optional_decimal(c.get("vendor_cost_usd"))
        # ISO 4217. Decision 3 states the default explicitly (unlike actor_id /
        # cost_basis, which must never be guessed), so baking 'USD' in here —
        # rather than leaving it to the column's server_default — is the
        # flusher stating the same contract the model documents. A malformed
        # value (not a 3-letter code) falls back to 'USD' rather than storing
        # something the VARCHAR(3) column would truncate silently.
        _raw_currency = str(c.get("currency") or "").strip().upper()
        currency = _raw_currency if len(_raw_currency) == 3 else "USD"
        # `validate_cost_event` above has already rejected every input for which
        # this would return `is_non_uuid=True` (absent, unresolvable, nil, or a
        # leaked credential — each one dead-lettered with its own reason). What
        # reaches here always resolves to a REAL tenant uuid, so the second
        # element is dead on this path and the `tenant_unattributed_total`
        # counter it used to drive could only ever read 0 for stream="cost".
        # The counter itself is NOT dead — `event_flusher` still fires it for
        # stream="usage", which this change does not touch.
        tenant_id, _resolved_uuid = _normalize_tenant_id(c.get("tenant_id", ""))
        # THE ACCEPT PATH WAS FAIL-OPEN. `canonical_tenant_id` parses any
        # well-formed uuid with the stdlib and returns it with NO registry
        # lookup, so a caller could invent a tenant simply by putting a random
        # uuid in X-Tenant-Id — and be metered as one. `validate_cost_event`
        # does not catch it either: it only rejects a tenant that fails to
        # RESOLVE, and an unregistered uuid resolves perfectly.
        #
        # The fix is NOT to reject. Dead-lettering here would destroy the money
        # record, which is worse than mis-attributing it and is unrecoverable —
        # billing is append-only with no refund path, and the nil sentinel is
        # itself dead-lettered by _cost_schema, so routing there would delete
        # the events outright. The row is STORED and PARKED instead: it stays in
        # the ledger, stays auditable, stays reversible with one UPDATE, and is
        # dropped from every billable aggregate by the `excluded_from_billing`
        # filter those aggregates already carry.
        # The park is ON by default and armed in production since 2026-09-16
        # (`billing_exclude_unregistered_tenants`). Set
        # BILLING_EXCLUDE_UNREGISTERED_TENANTS=false to disarm: the check still
        # runs and the counter moves with action="counted", but the row bills,
        # rolls up and debits as if this block did not exist.
        #
        # The verdict is trustworthy because `is_registered_tenant_id` consults
        # the LIVE licences overlay (GET {licenses_url}/internal/tenants) first;
        # the hand-refreshed `_REGISTERED_TENANT_IDS` snapshot answers only when
        # that overlay is unavailable, and each check is reported on
        # billing_tenant_registration_source_total{source}. A sustained
        # source="snapshot_degraded" rate is the signal to disarm. Arming
        # history and the reversal UPDATE: docs/UNREGISTERED_TENANT_GATE.md.
        unregistered_tenant = not is_registered_tenant_id(tenant_id)
        if unregistered_tenant:
            observability_metrics.cost_events_unregistered_tenant_total.labels(
                service_key=service_key_label,
                action="excluded" if exclude_unregistered else "counted",
            ).inc()
        park_unregistered = unregistered_tenant and exclude_unregistered
        # ------------------------------------------------------------------
        # ARCHIVAL IS FLAG-AND-COUNT HERE, NEVER A REFUSAL.
        #
        # `archived_at` is enforced in FOUR other places (licences' own
        # license-mint path, its token-mint path, and `routers/budget.py`'s
        # pre-spend `reserve_tenant_budget`) — all of them BEFORE the compute
        # happens. This is the one place that sees spend AFTER it already
        # happened: inference-router and mlx-ingestion-backend push cost
        # events onto `billing:costs` directly, east-west, bypassing both the
        # Envoy ext_authz gateway check (GATEWAY-scoped, blind to east-west)
        # and the budget reservation gate. By the time a row reaches this
        # loop the money is already spent, so refusing it here would not stop
        # the spend — it would only delete the record of it. Per
        # `routers/budget.py`'s own comment: dropping the row "converts an
        # attribution fault into an UNMETERED one", which is strictly worse.
        # So the row is ALWAYS persisted; only the flag and the counter below
        # register that it happened while archived.
        #
        # Reuses `is_tenant_archived` rather than re-deriving the answer: it
        # shares `get_tenant_info`'s single TTL-cached registry read with
        # every other caller (this router's own reservation gate included),
        # so this adds no HTTP call of its own beyond what already runs. Same
        # fail-open direction as its other callers — an unreachable licences
        # answers "not known to be archived" rather than refusing a row over a
        # dependency blip.
        tenant_archived = await is_tenant_archived(tenant_id, settings)
        if tenant_archived:
            observability_metrics.cost_events_billed_while_archived_total.labels(
                service_key=service_key_label,
            ).inc()
        # ------------------------------------------------------------------
        # SELF-METERING HEALTH PROBES: recorded, never billed.
        #
        # Readiness probes across the estate deliberately call REAL data-plane
        # endpoints — that is what makes them readiness probes — and every such
        # call mints a billable cost event. Measured on the prod billing primary
        # 2026-09-07: 38,494 of 39,519 rows carrying the mlx-backend service
        # key in 24h (99.96%) are probe traffic, and probe rows are ~82% of every row in
        # billing_cost_events while carrying 0.41% of the money. So the ledger
        # is four-fifths synthetic: every row count and every per-endpoint
        # distribution taken off it describes the probe, not usage.
        #
        # ⛔ THE FIX DOES NOT TOUCH THE PROBE. Two earlier attempts did and were
        # rejected: raising the probe's cache TTL removes a fraction of the rows
        # and pays for it in slower failure detection, and re-pointing the probe
        # at a non-metering endpoint (nlp_service PR #61) makes it exercise a
        # route production never calls while no longer exercising the one it
        # does. Probes keep calling the real endpoints. The meter changes.
        #
        # ⛔ AND IT IS KEYED ON IDENTITY, NEVER ON PAYLOAD SHAPE. The karma probe
        # is exactly 30 input_tokens and the nlp_service probe exactly 16, one
        # distinct value each across tens of thousands of 24h rows — a length
        # rule would look flawless on every dashboard and would also silently
        # stop billing any genuine 30-character request from ANY tenant, which
        # is far worse than the problem being fixed and invisible for exactly
        # the same reason. `match_health_probe` can only fire on a
        # (subject_id, service_key, endpoint) an operator wrote into
        # health_probes.HEALTH_PROBE_EMITTERS; it fails closed (= bills) on
        # every other input, including the absent subject_id most live rows
        # carry.
        #
        # The endpoint comes from usage_extra, hoisted above the row dict below
        # purely so it can be read here — it is not a column and nothing about
        # this needed a migration.
        usage_extra = _parse_usage_extra(c.get("usage_extra"))
        # `tenant_id` is passed as the SECOND narrowing and it is not optional
        # padding: the karma subject is SHARED — `nonagentic-gtm` bills 342 real
        # mlx-backend rows / 7d on the same subject and the same two endpoints —
        # so a subject-only rule would park another tenant's genuine traffic.
        # See health_probes."A SUBJECT IS NOT A SERVICE".
        # `input_tokens` is the LAST conjunct and it is a NARROWING, never a key:
        # it is unreachable except for an identity already matched on subject,
        # tenant, service key and endpoint, so it can only ever move rows back
        # INTO billing. Measured, it recovers the 5 real svc-nlp-service rows /
        # $0.112 per 7d that the identity rule alone parks. ⛔ A payload-size
        # rule on its own would stop billing a genuine 16- or 30-token request
        # from any tenant in the estate — see health_probes."PAYLOAD SIZE IS A
        # CONJUNCT, NEVER A KEY" for why the two have opposite signs.
        probe_rule = match_health_probe(
            subject_id,
            tenant_id,
            service_key_label,
            probe_endpoint(usage_extra),
            input_tokens,
        )
        if probe_rule is not None:
            observability_metrics.cost_events_health_probe_total.labels(
                subject_id=probe_rule.subject_id,
                tenant_id=probe_rule.tenant_id or "",
                service_key=probe_rule.service_key or "",
                endpoint=probe_rule.endpoint or "",
                action="excluded" if exclude_health_probes else "counted",
            ).inc()
        park_probe = probe_rule is not None and exclude_health_probes
        # tenant_id is now ALWAYS a uuid (or the nil sentinel), so the old
        # `or tenant_id` fallback would store a uuid string in a display column
        # and stop it displaying anything. Fall back to the label carried by the
        # RAW emitted value instead — credential-redacted, so a leaked Bearer
        # token cannot reach tenant_name the way it did in the 4 live rows that
        # hold a raw auth header in BOTH columns.
        #
        # ⛔ THAT FALLBACK IS NOW THE ORDINARY PATH, AND IT STORES A UUID.
        # Since migration 0011 the emitters send a uuid as `tenant_id` and stop
        # sending `tenant_name` at all, so `original_tenant_label` — which
        # faithfully preserves whatever the emitter put on the wire — preserves
        # the uuid. Measured on the prod billing primary 2026-09-05:
        # billing_cost_events.tenant_name is a uuid on 66,103 of 66,103 rows
        # ($310.49), and it propagates into billing_cost_daily.tenant_name and
        # into every SQL consumer and rollup. It was invisible because the
        # column is never NULL and never empty — it is 100% noise that LOOKS
        # populated. The two surfaces that looked right (`/dashboard/costs` and
        # the compliance CSV export) were each re-resolving it at READ time;
        # nothing else could.
        #
        # So resolve at WRITE time, from the SAME live registry those two read
        # paths use, via the ONE helper `services.event_flusher` also calls —
        # the two flushers carried this identical expression and therefore the
        # identical defect (billing-service #135 reports it on
        # billing_metering_events), so the fix belongs at the shared point, not
        # copied twice. `stored_tenant_name` never raises and never blocks: a
        # registry miss must not fail a money record over a display label.
        #
        # ⚠️ DO NOT FABRICATE. A tenant the registry cannot vouch for gets no
        # invented label. It gets "" when all that was on offer was the id echoed
        # back — see the helper for why the uuid is worse than an absence — and
        # its own sentinel verbatim otherwise. Either way the case is COUNTED on
        # the established billing_placeholder_value_total series
        # (field="tenant_name"), so "the registry did not know" stays
        # distinguishable from "the registry said so" instead of being hidden by
        # the very fix that closes the defect.
        tenant_name, tenant_name_unresolved = stored_tenant_name(
            tenant_id, c.get("tenant_name", ""), c.get("tenant_id", "")
        )
        if tenant_name_unresolved:
            note_placeholder_value("tenant_name", service_key_label)
        provider = c.get("provider", "unknown")
        # Token VOLUME, paired with the event-count denominator incremented ~200
        # lines above off the SAME `service_key_label`. Placed HERE rather than
        # beside that one for one reason: `provider` is parsed on this line, and
        # re-reading `c.get("provider")` earlier would be a second, independently
        # handled parse of the same wire value -- which is precisely how the two
        # branches in `routers/budget.py` came to disagree about one licence id.
        # Nothing between the two sites can `continue`, `return` or `raise`, so
        # both counters see the same population and a ratio over them is honest.
        #
        # ⚠️ This is the numerator for "did the pipeline SEE these tokens?", not a
        # billable total -- it counts arrivals, so a replay re-counts where the DB
        # would dedupe on event_key. The billable figure lives in the table.
        observability_metrics.cost_tokens_by_service_total.labels(
            service_key=service_key_label,
            provider=provider,
        ).inc(input_tokens + output_tokens + cache_creation + cache_read)
        legacy_model = c.get("model")
        served_model = c.get("served_model") or legacy_model
        # End-to-end correlation key. Optional on the wire — older producers
        # may not emit it yet. _parse_uuid returns None on missing/invalid.
        correlation_id = _parse_uuid(c.get("correlation_id", ""))
        # THE HANDLE ON THE PRE-FLIGHT BUDGET RESERVATION this cost settles
        # (billing-service #178). Present only when the producer actually
        # reserved — absent, never empty — so None here means "this spend was
        # never reserved" and its settlement keeps the historical anonymous
        # per-tenant release. Parsed with the SAME helper as correlation_id
        # directly above: a reservation id is a uuid and a second, independently
        # written parser for one is how two call sites come to disagree about one
        # id (see the `provider` comment above for that exact failure).
        reservation_id = _parse_uuid(c.get("reservation_id", ""))
        # THE PRODUCER'S OWN EVENT TIME — the field that fixes the wrong-month
        # bug. `created_at` (this row's other clock) is stamped at INGEST, so a
        # flush that crosses midnight buckets the usage on the wrong day and, at
        # a month boundary, onto the wrong invoice. Normally the lag is seconds;
        # an observed consumer stall made it 62 hours.
        #
        # The field was ALREADY on the wire the whole time — every measured
        # emitter sends `timestamp` = `datetime.now(UTC).isoformat()`
        # (inference-router services/metering.py, w2v_common metering/cost_bus.py,
        # kag_common metering/__init__.py) and `consumer._stage_message` appends
        # the raw field dict verbatim, so it arrives here intact. Billing just
        # never read it. Reading it is the entire write-side fix.
        #
        # `now` (captured once before this loop) is the received_at reference, so
        # a skewed producer clock cannot book a cost into the future: see
        # `parse_event_ts` for every refusal and — importantly — for why
        # `event_ts <= created_at` is an INVARIANT the rollup's partition-pruning
        # bound depends on. None stays None: NULL means "no usable producer event
        # time", the rollup falls back to `created_at` for that row and counts it.
        # ⛔ Never substitute `now`/`created_at` here. That would restate ingest
        # time as event time under a column that promises otherwise, and would
        # make the fallback counter read zero while measuring nothing.
        event_ts = parse_event_ts(c.get("timestamp"), received_at=now)

        extra_meta = {}
        # `stage` is the per-operation sub-step (w2v DAG stage) carried alongside
        # correlation_id. Stored in metadata (not a column) so per-stage cost can
        # be grouped at read time without a schema migration. Optional on the wire.
        # `worker_name` is the ORIGINATING caller — the only field on the wire that
        # says which app's work this spend belongs to. inference-router serves every
        # origin behind one service_key, so `cost_attribution_app_by_service_key`
        # cannot tell requirements' spend from kag's: measured 2026-09-03, tenant
        # 0195f48b-…0001 drives 3,605 rows/24h of requirements work and tenant
        # …0099 drives 1,106 of kag/karma, both under service_key
        # "inference-router". Mapping that one key to any single app therefore
        # encodes a falsehood that stays harmless only while the OTHER tenant
        # happens not to hold that app's licence — a landmine, not a fix.
        #
        # This list is a WHITELIST: a key the producer sends but this tuple omits
        # is silently dropped, and the row looks correct. That is exactly how the
        # origin went missing — inference-router has `x_worker_name` in scope at
        # the emit site and it never survived to the row.
        # `caller_client_id` is the IMMEDIATE CALLING SERVICE — the upstream
        # microservice's Keycloak service account, as verified by license-authz
        # (which injects `x-caller-client-id` only after checking the inbound
        # `X-Caller-Authorization` JWT). It is NOT `worker_name` and must not be
        # folded into it: `worker_name` says whose WORK this is (the origin of the
        # request), `caller_client_id` says who made THIS hop. On a chain like
        # requirements -> worker-management -> inference-router they differ at
        # every hop, and it is exactly that difference that lets an app-to-app
        # call be attributed to its caller instead of to whichever service
        # happens to emit the event.
        #
        # Measured 2026-09-04: no emitter puts a caller on the wire at all, so
        # every service-to-service cost event in the ledger is anonymous as to who
        # called. worker_management already HAS the value in a ContextVar
        # (`identity_context.get_caller_client_id_from_context`) and simply never
        # writes it. Whitelisting lands FIRST, deliberately — see the WHITELIST
        # note below: a producer that ships before the receiver knows the key has
        # it dropped silently, which is the failure this very comment records.
        #
        # The tuple itself lives in ``services._parsers`` as
        # COST_METADATA_WHITELIST so the placeholder counter's `field` label
        # domain is DERIVED from this list rather than hand-copied beside it —
        # add a key here and its series is born automatically.
        for k in COST_METADATA_WHITELIST:
            if c.get(k):
                extra_meta[k] = c[k]
                # A whitelisted key that arrives holding "unknown" is not data.
                # It is stored verbatim (changing the coercion would only move
                # the laundering — see billing_placeholder_value_total), but it
                # is counted so the fabrication is legible: measured 2026-09-05,
                # 968 of 968 inference-router rows carry job_id = 'unknown'.
                if is_placeholder(c[k]):
                    note_placeholder_value(k, service_key_label)
            else:
                # ABSENT (missing, empty, or falsy) is the other half of the
                # same signal and must be counted on the same series, or a field
                # nobody ever sends reads as 0% placeholder — perfect health —
                # purely because it never appears at all. Absence and a sentinel
                # are the same fact about the emitter.
                note_placeholder_value(k, service_key_label)
        # `delegated_by` decides whether the attribution audit treats a
        # licence/tenant mismatch as intended delegation, so only a short
        # string subject id is admitted -- anything else is not a vouch.
        _delegated_by = extra_meta.get("delegated_by")
        if _delegated_by is not None and (
            not isinstance(_delegated_by, str) or len(_delegated_by) > DELEGATED_BY_MAX_LEN
        ):
            del extra_meta["delegated_by"]
        # Optional conveyor attribution (card_id / execution_id): stored when the
        # wire carries a usable id, NEVER placeholder-counted -- absence is the
        # correct state for every non-conveyor emitter. See
        # services._parsers.OPTIONAL_COST_METADATA_FIELDS.
        for k in OPTIONAL_COST_METADATA_FIELDS:
            _opt = parse_optional_metadata_str(c.get(k))
            if _opt is not None:
                extra_meta[k] = _opt

        # The three NOT NULL columns the flusher defaults to the literal
        # "unknown" when the wire carries nothing. Counted at the point of
        # substitution, off the SAME service_key label expression as the
        # whitelist above, so the two halves of this counter can never drift.
        for _defaulted in DEFAULTED_COST_FIELDS:
            if is_placeholder(c.get(_defaulted)):
                note_placeholder_value(_defaulted, service_key_label)

        # Resolve the idempotency key BEFORE the row is built, and count how it
        # was obtained on the SAME service_key label expression the placeholder
        # counters above use, so the two can never drift.
        event_key, event_key_source = derive_event_key(c)
        note_event_key_source(service_key_label, event_key_source)

        # D1 (billing-licensing estate repair, round 16): a project_id that is
        # ITSELF a registered tenant id is refused before the row is built. See
        # `_resolve_project_id`'s own docstring for the measured prod shape
        # (0195f48b-…-0005 / -0004 used as project_id under n8n-dev), why the
        # same-row forged-project detector cannot see this class, and why
        # `registry_unavailable` KEEPS the value instead of nulling it.
        # Owner ruling 2026-10-07: the event's OWN tenant-level project id
        # (`tenancy.tenant_level_project_id`) is kept, covered, and NOT sent to
        # fr ownership adjudication -- it is not an fr project. Another
        # tenant's tenant-level id is refused like a raw tenant id.
        resolved_project_id, project_id_refused_reason = _resolve_project_id(
            c.get("project_id", ""), tenant_id=tenant_id
        )
        if project_id_refused_reason is not None:
            if project_id_refused_reason == "registry_unavailable":
                logger.warning("project_id_registry_check_failed")
        elif resolved_project_id is not None and str(
            resolved_project_id
        ) != tenant_level_project_id(tenant_id):
            # 2026-09-28: project -> tenant OWNERSHIP check against fr's map
            # (fetched once per batch above). At most ONE reason is counted per
            # event: an id already refused/faulted above is not re-adjudicated.
            # observe (default) keeps the value + stamps metadata; enforce nulls
            # only on positive knowledge (foreign_tenant / unknown_project).
            resolved_project_id, project_id_refused_reason, ownership_meta = (
                _adjudicate_project_owner(
                    resolved_project_id,
                    tenant_id=tenant_id,
                    owner_map=project_owner_map,
                    mode=project_ownership_mode,
                )
            )
            if ownership_meta:
                extra_meta.update(ownership_meta)
        if project_id_refused_reason is not None:
            observability_metrics.project_id_refused_total.labels(
                reason=project_id_refused_reason
            ).inc()

        asserted_app_uuid, app_key = split_asserted_app(c.get("app_id"))
        funding_present, wire_funding = funding_svc.parse_wire_funding(c.get("funding"))
        row = {
            "tenant_id": tenant_id,
            "tenant_name": tenant_name,
            "license_id": license_id,  # raw CostEvent keeps None; sentinel only for rollup tables
            "subject_id": subject_id,  # raw CostEvent keeps None too
            # NULLABLE, NEVER DEFAULTED — see the parse site above.
            "actor_id": actor_id,
            # A uuid is kept as asserted. A non-uuid, non-empty value is an app
            # KEY (X-App-Id slug): it stays None HERE and is resolved after the
            # map read below -- see `asserted_app_key`.
            "app_id": asserted_app_uuid,
            # NULLABLE, NEVER DEFAULTED. inference-router stamps a top-level
            # `project_id` and OMITS the field entirely when it has none, so a
            # missing project id must stay missing — never a sentinel, never the
            # literal "unknown". Cf. metadata.job_id, which WAS defaulted to
            # "unknown" and therefore reported 100% coverage of a value no caller
            # ever populated.
            # ⭐ R16: `resolved_project_id`, not a fresh `_parse_uuid` call — see
            # the D1 block above, which nulls this when the wire's project_id is
            # itself a registered tenant id.
            "project_id": resolved_project_id,
            # A missing service_key is genuinely unknown provenance — record it as
            # "unknown" (the same honest sentinel provider/tenant_id/cost_source
            # already use on this row), NOT the fabricated routing slug
            # "inference-router". The old default misattributed every
            # service_key-less event to the router and would silently defeat any
            # future service_key-scoped pricing row for those events. NB the
            # billing_cost_events.service_key column is NOT NULL (0001 baseline),
            # so None is not an option. Resolution is unaffected: no pricing row is
            # keyed on service_key "unknown" (or "inference-router"), so both fall
            # through to the model-default rungs (2/4) and price identically.
            "service_key": c.get("service_key") or "unknown",
            "provider": provider,
            "model": legacy_model,
            "served_model": served_model,
            "input_tokens": input_tokens,
            "output_tokens": output_tokens,
            "cache_creation_input_tokens": cache_creation,
            "cache_read_input_tokens": cache_read,
            # quantity_unit / cost_basis: honour an explicit wire value now;
            # both are re-derived below (from the resolved pricing row) if the
            # wire sent nothing valid — see the "Authoritative pricing" loop.
            "quantity_unit": wire_quantity_unit,
            "cost_usd": None,  # filled below from pricing service
            "cost_usd_hint": cost_usd_hint,
            "cost_basis": wire_cost_basis,
            "vendor_cost_usd": vendor_cost_usd,
            "currency": currency,
            "pricing_id": None,
            "provider_request_id": c.get("provider_request_id"),
            "usage_extra": usage_extra,
            # The LLM setup IR served this hop under (usage_extra.setup, §3.2),
            # recorded whenever it is a valid 1/2/3. The EUR consumption columns
            # are filled below ONLY for a money tenant (budget_unit='money');
            # NULL on every other row, permanently.
            "llm_setup": money_svc.parse_llm_setup(usage_extra),
            "consumption_amount": None,
            "consumption_currency": None,
            "latency_ms": _parse_int(c.get("latency_ms", "0")) or None,
            "metadata": extra_meta or None,
            "correlation_id": correlation_id,
            # The reservation this row settles, or NULL when the producer did not
            # reserve. Stored as a column (nullable) rather than in `metadata` so
            # an over-release can be reconstructed from the ledger after the fact:
            # the Redis reserved-pool keys are TTL'd, so without this the only
            # record of WHICH reservation a charge settled is gone within the
            # hour. The settlement itself does NOT read it back from here — it
            # uses the in-memory map accumulated below, because the release runs
            # post-commit over a whole batch.
            "reservation_id": reservation_id,
            # Producer event time, or NULL when nothing usable was asserted
            # (alembic 0028). NULL is the honest answer and the rollup resolves it
            # to `created_at`; see the parse site above and the column comment in
            # models/db/events.py. `created_at` is deliberately NOT set here — it
            # keeps its server/ORM ingest default, because the two clocks answer
            # two different questions and collapsing them is the bug being fixed.
            "event_ts": event_ts,
            "cost_source": (c.get("cost_source") or "unknown"),
            # WHO FUNDS this call (alembic 0053). The wire value when valid;
            # derived in the pricing loop below when absent -- it needs the
            # resolved pricing row's contract cost_basis. See services.funding.
            "funding": wire_funding,
            # Per-call idempotency key, RESOLVED rather than merely read. See
            # `derive_event_key` for the full rung list and — importantly — for
            # why content-hashing a keyless event is NOT one of the rungs: 26,283
            # live mlx-backend rows share one exact content signature, so a
            # content hash would drop ~26,282 real billable calls. Rung 2 mints a
            # key from the provider's own per-call id when the producer sent no
            # key of its own; rung 3 leaves None, which the claim below skips, so
            # such a row is stored and billed exactly as before. The source is
            # counted per producer so the keyless population is legible instead
            # of being a silent NULL — that counter is the arming criterion for
            # ever making the key required.
            "event_key": event_key,
            # Reversible park, never a delete — the same mechanism 0013 uses for
            # duplicates. Written explicitly rather than left to the column
            # default so the false case is asserted by this code path too: a
            # future edit that drops the flag cannot silently start billing
            # unregistered tenants again while the tests still pass.
            # UNREGISTERED WINS THE TIE, and the order is not arbitrary. Both
            # conditions can hold at once, the column holds ONE reason, and
            # "unregistered_tenant" is the finding a human must act on — it says
            # a uuid nothing vouches for is metering, which is a security and
            # attribution question. "health_probe" is a known, allowlisted,
            # deliberately-excluded population; labelling a genuinely
            # unregistered subject as a probe would retire that alarm by
            # renaming it. Both counters above fire regardless of which reason
            # wins, so neither population can be hidden by the other.
            "excluded_from_billing": park_unregistered or park_probe,
            "exclusion_reason": (
                "unregistered_tenant"
                if park_unregistered
                else (HEALTH_PROBE_EXCLUSION_REASON if park_probe else None)
            ),
            # FLAG, NEVER EXCLUDE (alembic 0037). Deliberately independent of
            # `excluded_from_billing` above: an archived tenant's spend is
            # still real, already-incurred compute, and it is still billed —
            # this column only records that it happened after archival, for
            # audit/collection follow-up. Never combine it into the exclusion
            # OR above.
            "billed_while_archived": tenant_archived,
        }
        # Every row that reaches here is attributable: `validate_cost_event`
        # quarantines an absent / unresolvable / nil / credential tenant to the
        # dead-letter stream, so the sentinel can no longer be INSERTED at all.
        # That replaces the old arrangement, where such an event was written as
        # a nil-tenant cost row and then EXCLUDED from the token rollup and the
        # budget debit below — 22,226 live rows / $353.09 of spend that was
        # recorded and never billed to anyone. A REGISTERED legacy name still
        # resolves to its real tenant uuid and bills normally; the human label is
        # preserved in tenant_name either way. Emitters that send no tenant must
        # be fixed at the emitter (docs/EMITTER_GUIDE.md) — the DLQ is now where
        # they become visible.
        if funding_present:
            funding_asserted.add(id(row))
        pre_rows.append((row, served_model, now, c.get("_msg_id")))
        if app_key is not None:
            asserted_app_key[id(row)] = app_key

    # Robust-by-construction dedup: collapse any repeated stream msg id
    # (``p[3]``) to its FIRST occurrence BEFORE the idempotency claim + the
    # ``survived`` filter below. The claim yields a SET of claimed ids, so the
    # membership test ``p[3] in claimed`` would let BOTH copies of a doubled id
    # survive → double cost-event insert + double token rollup + double budget
    # debit. Today the 300s XAUTOCLAIM min-idle window makes an intra-batch
    # duplicate unreachable, but this defends it by construction, not by timing.
    # None-id rows (direct/test inserts) carry no dedup key and are ALL kept.
    seen_msg_ids: set[str] = set()
    deduped_pre_rows = []
    for pr in pre_rows:
        mid = pr[3]
        if mid is not None:
            if mid in seen_msg_ids:
                continue
            seen_msg_ids.add(mid)
        deduped_pre_rows.append(pr)
    pre_rows = deduped_pre_rows

    # Same intra-batch dedup for the dead-letter branch: a malformed message that
    # appears twice in one batch must be quarantined ONCE, not double-XADD'd to
    # the dead-letter stream / double-counted in cost_dead_letter_total. None-id
    # rows carry no dedup key and are all kept.
    seen_dead_ids: set[str] = set()
    deduped_dead_letters = []
    for dl in dead_letters:
        dl_mid = dl[0].get("_msg_id")
        if dl_mid is not None:
            if dl_mid in seen_dead_ids:
                continue
            seen_dead_ids.add(dl_mid)
        deduped_dead_letters.append(dl)
    dead_letters = deduped_dead_letters

    rows: list[dict[str, Any]] = []
    dead_lettered_count = 0
    # (service_key, reason) -> count, for the attribution log after the flush.
    dead_letter_producers: Counter[tuple[str, str]] = Counter()

    if pre_rows or dead_letters:
        async with session_factory() as session:
            # --- Idempotency claim (SAME transaction as the cost_events insert) ---
            # Claim each Redis stream message id with INSERT … ON CONFLICT DO
            # NOTHING. A redelivered message — crash-after-commit-before-XACK, or
            # a NOGROUP recovery that recreates the group at id=0 against a now
            # durable stream — conflicts and is dropped, so it is never re-billed.
            # The claim and the cost_events insert commit atomically, so a crash
            # mid-flush rolls back both (the message stays un-ACKed and replays).
            # Malformed message ids are claimed alongside the billable ones so a
            # redelivery is deduped and never double-dead-lettered.
            claim_ids = [mid for (_r, _m, _t, mid) in pre_rows if mid]
            claim_ids += [mid for (c, _reason) in dead_letters if (mid := c.get("_msg_id"))]
            if claim_ids:
                claim_stmt = pg_insert(ProcessedCostMessage).values(
                    [{"stream_msg_id": m} for m in claim_ids]
                )
                claim_stmt = claim_stmt.on_conflict_do_nothing(index_elements=["stream_msg_id"])
                claimed = {
                    r[0]
                    for r in await session.execute(
                        claim_stmt.returning(ProcessedCostMessage.stream_msg_id)
                    )
                }
                dup = len(claim_ids) - len(claimed)
                if dup:
                    observability_metrics.cost_events_deduplicated_total.inc(dup)
                    logger.info("cost_events_deduplicated", skipped=dup, batch=len(pre_rows))
                survived = [p for p in pre_rows if p[3] is None or p[3] in claimed]
                survived_dead = [
                    (c, r)
                    for (c, r) in dead_letters
                    if c.get("_msg_id") is None or c.get("_msg_id") in claimed
                ]
            else:
                survived = list(pre_rows)
                survived_dead = list(dead_letters)

            # --- Second claim: the PRODUCER's per-call event_key -------------
            # The claim above dedupes a redelivery of the same Redis MESSAGE.
            # It cannot see a producer that re-XADDs the same physical call (a
            # retried emit, a replayed outbox, a stream recreated under the
            # producer): those are new message ids carrying a call billing has
            # already charged for. worker_management stamps `event_key` once per
            # completed call, so claiming it here — same transaction, same
            # ON CONFLICT DO NOTHING shape — makes that redelivery a no-op too.
            # Rows without a key are untouched: an absent key means "this
            # producer cannot promise uniqueness", not "reject this event".
            #
            # Intra-batch collapse first: a batch may legitimately carry the same
            # key twice, and pg_insert would abort on the duplicate VALUES row
            # rather than dedupe it.
            key_rows: list[tuple[Any, ...]] = [p for p in survived if p[0].get("event_key")]
            if key_rows:
                seen_in_batch: set[str] = set()
                batch_unique: list[tuple[Any, ...]] = []
                intra_batch_dup = 0
                for p in key_rows:
                    k = p[0]["event_key"]
                    if k in seen_in_batch:
                        intra_batch_dup += 1
                        continue
                    seen_in_batch.add(k)
                    batch_unique.append(p)

                key_claim = pg_insert(ProcessedCostEventKey).values(
                    [{"event_key": k} for k in seen_in_batch]
                )
                key_claim = key_claim.on_conflict_do_nothing(index_elements=["event_key"])
                claimed_keys = {
                    r[0]
                    for r in await session.execute(
                        key_claim.returning(ProcessedCostEventKey.event_key)
                    )
                }
                kept = [p for p in batch_unique if p[0]["event_key"] in claimed_keys]
                key_dup = intra_batch_dup + (len(batch_unique) - len(kept))
                if key_dup:
                    observability_metrics.cost_events_deduplicated_total.inc(key_dup)
                    logger.info(
                        "cost_events_deduplicated_by_event_key",
                        skipped=key_dup,
                        intra_batch=intra_batch_dup,
                        batch=len(pre_rows),
                    )
                # Keyless rows pass through untouched.
                survived = [p for p in survived if not p[0].get("event_key")] + kept

            # --- Resolve a licence for events that arrived without one ---
            # Placed HERE deliberately: after dedup (so a dropped redelivery is
            # never resolved or counted) and BEFORE the rollup aggregation below,
            # which keys token_counts/license_io on row["license_id"]. Resolving
            # after that block would store one licence on the event and bill the
            # rollup against a different one — the two must never disagree.
            #
            # FAIL-SOFT, AND EXACTLY WHAT THAT BUYS. A map-build error must not
            # cost the batch: the events are already priced and claimed, so
            # raising here would drop real billing data to improve an
            # attribution hint. `except Exception` ALONE did not deliver that,
            # and the comment that used to sit here promised a guarantee the
            # code did not have. Both map builds SELECT on the SAME session that
            # INSERTs the batch below; a failed statement leaves that session
            # needing a rollback, so swallowing the error only deferred the
            # death to the INSERT, which then raised PendingRollbackError with a
            # traceback naming the wrong statement.
            #
            # Each map build therefore runs inside its own SAVEPOINT
            # (`session.begin_nested()`). A statement error inside it rolls back
            # the nested scope only; the outer transaction stays usable and the
            # INSERT below still commits. Chosen over building the maps on a
            # separate session ahead of the batch because the read has to stay
            # at its use site — licence resolution is order-coupled to the dedup
            # above and to the rollup below — and because a savepoint costs no
            # extra pooled connection, only a SAVEPOINT/RELEASE round trip that
            # is nothing beside the batch's own writes.
            #
            # WHAT IT DOES NOT COVER, stated rather than implied: if the
            # CONNECTION dies (pool disconnect, backend termination, an admin
            # `pg_terminate_backend`) the enclosing transaction is gone and no
            # savepoint can rescue it — the flush raises, the idempotency claim
            # rolls back with it, and the messages stay un-ACKed and replay.
            # That costs the batch a RETRY, never the data. The same mechanism
            # and the same limit apply to the app derivation below.
            if survived:
                # --- Resolve an asserted app KEY first (X-App-Id) ---
                # Runs BEFORE licence derivation because that derivation keys on
                # the asserted app: a delegated call (kag -> mlx-backend, licence
                # deliberately dropped by the leaf) is attributable only via
                # (tenant, asserted app) — a shared leaf cannot sit in the
                # service_key map. Same SAVEPOINT + fail-soft shape as its
                # neighbours. Unknown key -> NULL + billing_app_key_unknown_total.
                # Shared with event_flusher: license_resolver.resolve_asserted_app_keys.
                await resolve_asserted_app_keys(
                    session,
                    [
                        (row, asserted_app_key[id(row)])
                        for row, _served, _ts, _mid in survived
                        if id(row) in asserted_app_key
                    ],
                    stream="cost",
                )
                try:
                    app_by_key = parse_service_key_app_map(
                        get_settings().cost_attribution_app_by_service_key
                    )
                    # A row "asserted an app" if it carries a uuid app_id (wire
                    # uuid, or a key resolved just above) OR asserted a key the
                    # mirror could not resolve. Asserted rows resolve ONLY from
                    # (tenant, app) — see license_resolver.derive_license.
                    unlicensed = [
                        (row, row["app_id"] is not None or id(row) in asserted_app_key)
                        for row, _served, _ts, _mid in survived
                        if row["license_id"] is None
                    ]
                    resolution_map = None
                    app_licence_map = None
                    if app_by_key:
                        async with session.begin_nested():
                            resolution_map = await get_resolution_map(session)
                    if any(asserted for _row, asserted in unlicensed):
                        # Own try: a failed (tenant, app) map read must not cost
                        # the un-asserted rows their service_key derivation.
                        # app_licence_map stays None -> asserted rows stay NULL.
                        try:
                            async with session.begin_nested():
                                app_licence_map = await get_app_licence_map(session)
                        except Exception:
                            logger.warning("app_licence_map_failed", exc_info=True)
                    if resolution_map is not None or app_licence_map is not None:
                        for row, asserted in unlicensed:
                            resolved = derive_license(
                                tenant_id=row["tenant_id"],
                                service_key=row["service_key"],
                                app_asserted=asserted,
                                asserted_app_id=row["app_id"],
                                app_by_service_key=app_by_key,
                                resolution_map=resolution_map,
                                app_licence_map=app_licence_map,
                            )
                            if resolved is None:
                                continue
                            row["license_id"] = resolved
                            # Mark the attribution as DERIVED, not asserted by the
                            # caller. Without this the stored row is
                            # indistinguishable from one whose emitter genuinely
                            # presented a licence, and a derived number must never
                            # be quotable as evidence that the transport works.
                            row["cost_source"] = f"{row['cost_source']}+resolved"
                            record_resolved(row["service_key"])
                except Exception:
                    logger.warning("license_resolution_failed", exc_info=True)

                # --- Adjudicate a licence the CALLER asserted (CP only) -----
                # See `adjudicate_asserted_license` above for the measured
                # defect (81% of attributed spend named a foreign licence), for
                # why a refusal nulls the id rather than dropping the row, and
                # for the point-in-time residual.
                #
                # ⛔ ORDERING IS LOAD-BEARING IN BOTH DIRECTIONS, and neither
                # neighbour is arbitrary:
                #   * AFTER licence resolution. That block only fills rows whose
                #     license_id is None, so it cannot touch a CP event that
                #     asserted one — and running it after this block would let
                #     it "helpfully" re-derive a licence for a row we JUST
                #     refused, silently restoring an invoice line the refusal
                #     exists to withhold. A refused row stays licence-less; the
                #     resolver never gets a second look at it.
                #   * BEFORE app derivation. That block reads `row["license_id"]`
                #     to stamp `app_id`, so a foreign licence left in place here
                #     would stamp the FOREIGN app onto the row — spreading the
                #     mis-attribution onto a second axis that nothing downstream
                #     would flag.
                #   * BEFORE the `survived` persisted-gap loop below, so a
                #     refused row correctly counts on
                #     billing_license_unattributed_persisted_total: it IS now an
                #     unattributed stored row, and hiding that would make the
                #     refusal invisible to the one counter that measures the
                #     stored population.
                #
                # Same SAVEPOINT + fail-soft shape as its two neighbours, for the
                # same reason and with the same limit: the events are already
                # priced and claimed, so a map-build error must cost the
                # attribution hint, never the batch. ⚠️ The fail-soft direction
                # here is ACCEPT — a batch whose owner map could not be built
                # keeps every asserted licence, exactly as before this code
                # existed. That is deliberate and it is the cheap direction: a
                # transient mirror read failure must not strip attribution off
                # real invoices, and the events remain adjudicable after the
                # fact because the raw rows are stored.
                try:
                    async with session.begin_nested():
                        owner_map = await get_owner_map(session)
                    for row, _served, _ts, _mid in survived:
                        refusal = adjudicate_asserted_license(
                            row["license_id"],
                            tenant_id=row["tenant_id"],
                            service_key=row["service_key"],
                            owner_map=owner_map,
                        )
                        if refusal is None:
                            continue
                        observability_metrics.cp_license_refused_total.labels(
                            service_key=row["service_key"],
                            reason=refusal,
                        ).inc()
                        logger.warning(
                            "asserted_license_refused",
                            reason=refusal,
                            service_key=row["service_key"],
                            tenant_id=row["tenant_id"],
                            # The REFUSED id is logged: it is not a credential,
                            # and without it an operator cannot tell a forged id
                            # from a sync lag — which is the whole point of
                            # splitting the three reasons.
                            asserted_license_id=str(row["license_id"]),
                        )
                        # ⛔ THE LINE THAT ACTUALLY WITHHOLDS THE MONEY. Nulling
                        # the id is what puts this row outside
                        # CostDaily.license_attributed_clause(); the cost_source
                        # marker below only explains it.
                        row["license_id"] = None
                        row["cost_source"] = (
                            f"{row['cost_source']}{LICENSE_REFUSED_COST_SOURCE_MARKER}"
                        )
                except Exception:
                    logger.warning("license_adjudication_failed", exc_info=True)

                # --- Derive the app from the licence, for rows still missing one ---
                # Runs AFTER licence resolution above so a licence derived there
                # also yields an app, and in its OWN try/except because it must
                # not be skipped when the operator has left
                # cost_attribution_app_by_service_key empty: this derivation does
                # not consult that mapping at all, only the event's own licence.
                #
                # Measured 2026-09-02: app_id was NULL on 401,316 of 401,316 rows,
                # while 85.6% of a 7-day window already carried a licence that maps
                # to a known app. The grain existed; nothing stamped it.
                #
                # An app_id the caller genuinely presented is never overwritten.
                # We deliberately do NOT append a "+app" suffix to cost_source the
                # way licence resolution does: cost_source is already queried by
                # exact value, and a second independent suffix would multiply it
                # into combinations nobody is filtering for. The DERIVED-vs-asserted
                # distinction is carried by billing_app_resolved_total instead,
                # which is the same guarantee without the cardinality.
                #
                # An asserted app KEY was already resolved above, before licence
                # derivation. A key-bearing row is never handed to licence->app
                # derivation, even when the key is unknown or the map read
                # failed: never guess a caller.
                try:
                    # Same SAVEPOINT as the licence map above, for the same
                    # reason and with the same limit — see that block comment.
                    async with session.begin_nested():
                        app_map = await get_app_map(session)
                    for row, _served, _ts, _mid in survived:
                        if row["app_id"] is not None:
                            continue
                        if id(row) in asserted_app_key:
                            # Caller asserted a key the mirror could not
                            # resolve: stay NULL, do NOT derive from the licence.
                            record_app_unattributed(row["service_key"])
                            continue
                        derived_app = resolve_app(license_id=row["license_id"], app_map=app_map)
                        if derived_app is None:
                            # No licence, or a licence whose app the mirror does
                            # not know (synced before migration 0014). Keep it
                            # NULL and keep counting it -- rule 1, never fabricate.
                            record_app_unattributed(row["service_key"])
                            continue
                        row["app_id"] = derived_app
                        record_app_resolved(row["service_key"])
                except Exception:
                    logger.warning("app_resolution_failed", exc_info=True)

                # --- Attribution rules: bill tenant + member (alembic 0041) ---
                # ⛔ ORDERING IS LOAD-BEARING: AFTER licence resolution,
                # adjudication and app derivation. Run earlier, the resolver
                # would derive a licence for the BILL tenant, and
                # `adjudicate_asserted_license` would refuse the source tenant's
                # own licence as `foreign_tenant` — nulling license_id and
                # dropping the spend off every invoice line. See
                # services.attribution_rules' module docstring.
                #
                # Same SAVEPOINT + fail-soft shape as its neighbours. The
                # fail-soft direction is "no attribution": the row stays on the
                # tenant that emitted it, exactly as before this code existed,
                # and the ledger is still correct for that tenant.
                try:
                    async with session.begin_nested():
                        rules_map = await attribution_svc.get_rules_map(session)
                    if rules_map:
                        attributed = 0
                        for row, _served, _ts, _mid in survived:
                            rule = attribution_svc.select_rule(
                                rules_map,
                                tenant_id=row["tenant_id"],
                                subject_id=row["subject_id"],
                                project_id=row["project_id"],
                            )
                            if rule is None:
                                continue
                            if rule.bill_tenant_id == row["tenant_id"]:
                                # Member-only rule: same tenant, stamp the actor.
                                attribution_svc.apply_rule(row, rule)
                                attributed += 1
                                continue
                            attribution_source[id(row)] = (row["tenant_id"], row["tenant_name"])
                            attribution_svc.apply_rule(row, rule)
                            bill_name, _unresolved = stored_tenant_name(
                                rule.bill_tenant_id, "", rule.bill_tenant_id
                            )
                            row["tenant_name"] = bill_name
                            # Archival is a fact about who is BILLED; re-ask for
                            # the bill tenant (TTL-cached, fail-open — see the
                            # parse-loop comment on `tenant_archived`).
                            row["billed_while_archived"] = await is_tenant_archived(
                                rule.bill_tenant_id, settings
                            )
                            attributed += 1
                        if attributed:
                            logger.info("cost_events_attributed", count=attributed)
                except Exception:
                    logger.warning("attribution_rules_failed", exc_info=True)

                # --- The number an operator actually wants: what PERSISTED ---
                # Everything above this line that counts an attribution gap does
                # so at INGRESS, before resolution, and therefore answers "did
                # the caller assert one?" — a question about the emitter's
                # transport. This loop answers the question people have twice
                # mistaken those counters for: of the rows that SURVIVED the
                # idempotency claim and are about to be written to
                # billing_cost_events, how many carry no licence and no tenant?
                #
                # Counted over `survived`, in its own loop, deliberately AFTER
                # both resolution blocks and OUTSIDE their try/except: if
                # licence resolution raised, its rows are still unattributed and
                # must still be counted — a gap that goes uncounted because the
                # thing that would have closed it crashed is the exact failure
                # this counter exists to make visible.
                #
                # Same denominator and same caveat as billing_app_unattributed_total
                # directly above: incremented in memory BEFORE the batch
                # transaction commits, so a rollback un-claims the messages and
                # the replayed batch counts the same rows again. It over-counts
                # on a failed flush and is a trend, not a ledger.
                for row, _served, _ts, _mid in survived:
                    if row["license_id"] is None:
                        observability_metrics.license_unattributed_persisted_total.labels(
                            service_key=row.get("service_key") or "unknown"
                        ).inc()
                    # Expected to stay at FLAT ZERO on this stream, and that is
                    # the POINT rather than dead code: validate_cost_event
                    # dead-letters an unresolvable tenant before it can become a
                    # pre_row, so a non-zero reading here means that guard has
                    # been weakened or bypassed. It is the control that proves
                    # billing_tenant_caller_unasserted_total's rise is a
                    # header-propagation signal and not a ledger gap.
                    if row["tenant_id"] == UNATTRIBUTED_TENANT_ID:
                        observability_metrics.tenant_unattributed_persisted_total.labels(
                            service_key=row.get("service_key") or "unknown"
                        ).inc()

            # Aggregate the token_usage rollup + per-tenant debit inputs from the
            # SURVIVED rows only, so a deduped redelivery never double-counts
            # usage or double-debits budgets.
            #
            # --- EUR money tenants (alembic 0044; PLAN rev E §3.1/§3.2) ---
            # A row settles against the MONEY ledger iff it states a setup
            # (usage_extra.setup) AND its (source) tenant has budget_unit='money'.
            # Such a row is withheld from the token debit and from the token
            # reservation settlement below — it was reserved in EUR, and its
            # handle names a money hold, not a token one. An event without
            # `setup` keeps the legacy token path, unchanged.
            money_units = await money_svc.tenant_budget_units(
                session,
                {
                    attribution_source.get(id(r), (r["tenant_id"], ""))[0]
                    for r, _s, _t, _m in survived
                    if r["llm_setup"] is not None
                },
            )
            money_rows: list[tuple[dict[str, Any], str, datetime]] = []
            month_start = current_month_start()
            week_start = current_week_start()
            for row, _served, _ts, _mid in survived:
                # No `if tid == UNATTRIBUTED_TENANT_ID: continue` guard any more.
                # It was the exclusion that made an unattributable cost row
                # unbilled — and it is now unreachable, because such an event is
                # dead-lettered by `validate_cost_event` before it ever becomes a
                # pre_row. Every survivor is a real tenant and must roll up and
                # debit. (The nil id remains excluded from billing by the only
                # means that is honest: it is not stored.)
                # ...but a row PARKED on excluded_from_billing must skip both,
                # or the exclusion would be a lie told only to the aggregates:
                # the row would still drain a budget and still inflate
                # token_usage, i.e. still be billed, while every report said it
                # was not. The row itself is already appended to `rows` above
                # and IS stored — that is the whole point, the money record
                # survives. Only its billable side effects are withheld.
                if row["excluded_from_billing"]:
                    continue
                # Source tenant for a re-attributed row — see `attribution_source`.
                tid, tname = attribution_source.get(id(row), (row["tenant_id"], row["tenant_name"]))
                tenant_names[tid] = tname
                in_t = row["input_tokens"]
                out_t = row["output_tokens"]
                lic_str = str(row["license_id"]) if row["license_id"] else None
                tkey = (tid, lic_str, "monthly", month_start)
                prev = token_counts.get(tkey, (0, 0))
                token_counts[tkey] = (prev[0] + in_t, prev[1] + out_t)
                wkey = (tid, lic_str, "weekly", week_start)
                wprev = token_counts.get(wkey, (0, 0))
                token_counts[wkey] = (wprev[0] + in_t, wprev[1] + out_t)
                if row["llm_setup"] is not None and money_units.get(tid) == "money":
                    money_rows.append((row, tid, _ts))
                    continue
                dkey = (tid, row["license_id"] or NULL_LICENSE_ID)
                lio = license_io.get(dkey, (0, 0))
                license_io[dkey] = (lio[0] + in_t, lio[1] + out_t)
                # Handles accumulate on the SAME dkey and in the SAME loop as
                # license_io, behind the SAME `excluded_from_billing` continue
                # above — a parked row must contribute to NEITHER map, or the
                # exclusion would be a lie told only to the aggregates while the
                # row still settled somebody's reservation.
                #
                # ⛔ A MAP, NOT A SCALAR, and this is the site a future reader will
                # try to simplify. The settlement below is per-(tenant, licence)
                # over the WHOLE BATCH: license_io has already summed N events into
                # one total, `debit_budget` cascades that one total, and
                # `release_budget_reservations` issues ONE release for it. So the
                # handles of all N events have to arrive together, each carrying
                # its own token share, for the callee to decrement each
                # reservation's own key. Collapse this to one id and the release
                # goes back to draining an anonymous per-tenant pool, which is
                # billing-service #178.
                #
                # Only events that ACTUALLY carry a handle are included: an
                # unreserved event contributes tokens to license_io (it is real
                # spend and must debit) but no entry here, so it cannot invent a
                # reservation to release against.
                if row["reservation_id"] is not None:
                    handle = str(row["reservation_id"])
                    group = license_handles.setdefault(dkey, {})
                    group[handle] = group.get(handle, 0) + in_t + out_t

            # Authoritative pricing per surviving event.
            for row, served_model, ts, _mid in survived:
                (
                    authoritative,
                    pricing_id,
                    resolved_cost_model,
                    resolved_cost_basis,
                ) = await pricing_svc.compute_cost_usd_with_cost_model(
                    session,
                    provider=row["provider"],
                    served_model=served_model,
                    input_tokens=row["input_tokens"],
                    output_tokens=row["output_tokens"],
                    cache_creation_input_tokens=row["cache_creation_input_tokens"],
                    cache_read_input_tokens=row["cache_read_input_tokens"],
                    # T1.4 — pass the per-event service_key so a row keyed at
                    # (provider, model, service_key) can override the provider/
                    # model default. Pre-T1.4 callers passing nothing remain
                    # backward-compatible (rungs 2+4 of the precedence still match).
                    service_key=row["service_key"],
                    ts=ts,
                )
                # quantity_unit: the wire wins if it sent a valid one (see the
                # parse site above); otherwise derive from the provider name
                # first, then the resolved pricing row's cost_model family.
                # Stays NULL — never guessed — when neither is available (e.g.
                # a pricing miss on an unrecognised provider).
                if row["quantity_unit"] is None:
                    row["quantity_unit"] = pricing_svc.quantity_unit_for_provider(
                        row["provider"], resolved_cost_model
                    )
                # cost_basis: the wire wins if it sent a valid one; otherwise
                # take the RESOLVED PRICING ROW's own declared cost_basis
                # (alembic 0022 — a PROVIDER CONTRACT fact on provider_pricing,
                # not something derived from cost_model). ⛔ Deriving from
                # cost_model was the defect this replaced: per_token/per_
                # character/per_second say HOW WE METER, not WHETHER CASH
                # LEFT THE BUILDING, so every self-hosted per-unit provider
                # (pgx-local, mlx-nlp, ...) derived to 'cash' — exactly
                # backwards. ⛔ NEVER left None here — this is a NOT NULL
                # column and an explicit NULL in the INSERT bypasses the
                # column's server_default, so the fallback has to be spelled
                # out rather than left implicit. A PRICING MISS (no row
                # resolved at all, resolved_cost_basis is None) is not "cash":
                # there is nothing to say this was a real vendor charge, so it
                # takes the same safe backstop as the column's own
                # server_default (internal_transfer) — under-claiming cash
                # spend rather than fabricating one.
                # The emitter's own valid cost_basis, captured BEFORE the
                # backstop below overwrites a None (funding must not read the
                # backstop as evidence -- see services.funding rule 3).
                row_wire_basis = row["cost_basis"]
                if row["cost_basis"] is None:
                    row["cost_basis"] = (
                        resolved_cost_basis
                        if resolved_cost_basis is not None
                        else pricing_svc.PRICING_MISS_COST_BASIS
                    )
                # funding: derive ONLY when the wire sent nothing. The contract
                # basis is the RESOLVED pricing row's (else the wire's own valid
                # one) -- never the pricing-miss backstop written just above.
                if id(row) not in funding_asserted:
                    row["funding"] = funding_svc.derive_funding(
                        usage_extra=row["usage_extra"],
                        provider=row["provider"],
                        contract_basis=(
                            resolved_cost_basis
                            if resolved_cost_basis is not None
                            else row_wire_basis
                        ),
                        cost_model=resolved_cost_model,
                    )
                if row["funding"] is None:
                    observability_metrics.funding_unknown_total.labels(
                        service_key=row["service_key"]
                    ).inc()
                hint = row["cost_usd_hint"]
                if authoritative is None:
                    logger.warning(
                        "pricing_miss",
                        provider=row["provider"],
                        served_model=served_model,
                        tenant_id=row["tenant_id"],
                        input_tokens=row["input_tokens"],
                        output_tokens=row["output_tokens"],
                        cost_usd_hint=str(hint) if hint is not None else None,
                    )
                else:
                    # Drift detection: hint vs authoritative differ by > 5%.
                    #
                    # A ZERO hint is NOT drift. `_parse_decimal` (services/
                    # _parsers.py:81) maps a missing key, an empty string and a
                    # None alike to Decimal("0"), so on the stream path a zero
                    # hint is indistinguishable from an absent one: it means the
                    # emitter did not price the call, and billing's authoritative
                    # value is the only number there ever was. Reporting that as
                    # a hint-vs-authoritative disagreement is a claim the wire
                    # cannot support — and it produced a ratio of exactly 1 every
                    # time, so it ALWAYS cleared the 5% threshold. In production
                    # that was 701 of 894 cost_drift warnings in a 2h window
                    # (2026-08-11), i.e. 78% of the signal this log exists to
                    # carry, burying the ~193 genuine disagreements underneath it.
                    #
                    # An UNPARSEABLE cost never reaches here at all: it is
                    # rejected as "invalid_cost_usd" by validate_cost_event and
                    # dead-lettered above, so this branch only ever sees a hint
                    # that is either a real number or a real zero.
                    if hint is not None and authoritative > 0:
                        if hint == 0:
                            logger.info(
                                "cost_hint_absent",
                                provider=row["provider"],
                                served_model=served_model,
                                tenant_id=row["tenant_id"],
                                cost_usd_authoritative=str(authoritative),
                            )
                        else:
                            diff = abs(hint - authoritative)
                            ratio = diff / authoritative
                            if ratio > Decimal("0.05"):
                                logger.warning(
                                    "cost_drift",
                                    provider=row["provider"],
                                    served_model=served_model,
                                    tenant_id=row["tenant_id"],
                                    cost_usd_hint=str(hint),
                                    cost_usd_authoritative=str(authoritative),
                                    ratio=str(ratio),
                                )
                row["cost_usd"] = authoritative
                row["pricing_id"] = pricing_id
                if authoritative is not None:
                    observability_metrics.cost_events_priced_total.inc()
                rows.append(row)

            # Money consumption, in each tenant's BUDGET currency (REV F),
            # stamped on the row BEFORE the insert so the ledger row and the
            # budget debit below come from one computation. The budgets are
            # locked FOR UPDATE first (sorted), so a concurrent currency change
            # cannot slip between pricing and the debit. A tenant whose budget
            # vanished keeps the legacy EUR stamp (debit then logs no budget).
            money_currencies = await money_svc.lock_money_budget_currencies(
                session, {tid for _r, tid, _ts in money_rows}
            )
            money_debits: dict[str, Decimal] = {}
            money_handles: dict[str, list[str]] = {}
            for row, tid, ts in money_rows:
                amount = await money_svc.price_row_consumption(
                    session,
                    row,
                    tenant_id=tid,
                    fallback_ts=ts,
                    currency=money_currencies.get(tid, money_svc.DEFAULT_CURRENCY),
                )
                if amount is not None:
                    money_debits[tid] = money_debits.get(tid, Decimal(0)) + amount
                if row["reservation_id"] is not None:
                    money_handles.setdefault(tid, []).append(str(row["reservation_id"]))

            if rows:
                await session.execute(cast("Table", CostEvent.__table__).insert(), rows)

            # Upsert token usage as a single batched statement. token_counts keys
            # are unique, so ON CONFLICT can't fire twice for the same row in-batch.
            if token_counts:
                token_rows = [
                    {
                        "tenant_id": tenant_id,
                        "tenant_name": rollup_tenant_name(tenant_names, tenant_id),
                        "license_id": uuid.UUID(lic_str) if lic_str else NULL_LICENSE_ID,
                        "period_type": period_type,
                        "period_start": period_start,
                        "input_tokens": inp,
                        "output_tokens": outp,
                        "total_tokens": inp + outp,
                    }
                    for (tenant_id, lic_str, period_type, period_start), (
                        inp,
                        outp,
                    ) in token_counts.items()
                ]
                token_stmt = pg_insert(TokenUsage).values(token_rows)
                token_stmt = token_stmt.on_conflict_do_update(
                    constraint="uq_token_usage",
                    set_={
                        "input_tokens": TokenUsage.input_tokens + token_stmt.excluded.input_tokens,
                        "output_tokens": TokenUsage.output_tokens
                        + token_stmt.excluded.output_tokens,
                        "total_tokens": TokenUsage.total_tokens + token_stmt.excluded.total_tokens,
                    },
                )
                await session.execute(token_stmt)

            # Cascade-debit per-(tenant, license) totals against stacked budgets
            # — STAGED IN THIS SAME TRANSACTION (commit=False) so the
            # idempotency claim, cost_events, token_usage rollup and the budget
            # debit commit (or roll back) atomically. license_io is built from
            # the deduped survivors, so a redelivered batch stages no debit here.
            # Lock order is deterministic across HA replicas:
            # processed_cost_messages PK-insert → cost_events insert →
            # token_usage upsert → TokenBudget FOR UPDATE, so concurrent
            # flushers cannot deadlock. A debit error is NOT swallowed — it
            # propagates, rolls the whole flush back, and the un-ACKed message
            # replays (exactly-once via the claim).
            #
            # Each group is scoped to the license that INCURRED the cost, so a
            # license only ever drains its own budget plus the shared
            # tenant-level pool. Groups of the same tenant may both touch that
            # shared pool; they run sequentially inside this one transaction and
            # debit_budget flushes each step, so the second group reads the
            # first group's consumption — the pool cannot be double-spent.
            license_debits: dict[tuple[str, uuid.UUID], list[BudgetDebit]] = {}
            # Stage debits in a canonical (sorted) (tenant, license) order so
            # concurrent flushers on different replicas acquire the TokenBudget
            # FOR UPDATE locks in the SAME order — no cross-tenant deadlock cycle.
            for (tenant_id, lic_id), (inp, outp) in sorted(
                license_io.items(), key=lambda kv: (kv[0][0], str(kv[0][1]))
            ):
                total = inp + outp
                if total <= 0:
                    continue
                license_debits[(tenant_id, lic_id)] = await debit_budget(
                    session,
                    None,
                    tenant_id,
                    total,
                    in_tokens=inp,
                    out_tokens=outp,
                    license_id=lic_id,
                    commit=False,
                )

            # Money debits, same transaction, FOR UPDATE, canonical tenant order
            # (the token debits above use the same discipline against deadlock).
            for tid in sorted(money_debits):
                await money_svc.debit_money_budget(session, tid, money_debits[tid], now=now)

            # Quarantine malformed events to the dead-letter stream BEFORE the
            # commit. xadd-before-commit is at-least-once: a crash after xadd but
            # before commit rolls the idempotency claim back, so the message stays
            # un-ACKed and replays → re-quarantined (dedupable by _dlq_original_id).
            # The reverse order (commit then xadd) would durably claim the message
            # and then lose the DLQ record on a crash — the silent $0 loss we are
            # closing — so a Redis failure here MUST propagate (roll back + replay),
            # never be swallowed. Internal _-prefixed keys (_stream_name/_msg_id)
            # are stripped from the quarantined payload; the original id is kept as
            # _dlq_original_id for dedup on replay.
            if survived_dead and dead_letter_stream is not None and redis_conn is not None:
                for c, reason in survived_dead:
                    payload = {k: str(v) for k, v in c.items() if not k.startswith("_")}
                    # A leaked Authorization header sent AS the tenant_id now
                    # REACHES this stream: it fails validation as
                    # `invalid_tenant_id`, and before this guard the raw event
                    # was copied here verbatim. That would have moved the secret
                    # out of a redacted Postgres column and into a Redis stream
                    # operators read, retained for 30d — re-opening exactly the
                    # leak `_normalize_tenant_id` exists to prevent. Redact both
                    # columns a credential is known to arrive in (there are live
                    # rows carrying an auth header in BOTH). A non-credential
                    # value is returned unchanged, so an ordinary bad tenant is
                    # still fully visible to whoever has to fix the emitter.
                    for _leaky in ("tenant_id", "tenant_name"):
                        if payload.get(_leaky):
                            payload[_leaky] = original_tenant_label(payload[_leaky])
                    payload["_dlq_reason"] = reason
                    payload["_dlq_original_id"] = str(c.get("_msg_id") or "")
                    payload["_dlq_at"] = now.isoformat()
                    # cast("Any", …) mirrors budget_redis.py: redis-py's xadd stub
                    # types `fields` as an invariant Dict, so a dict[str, str] trips
                    # the checker despite being valid at runtime.
                    await cast("Any", redis_conn.xadd)(
                        dead_letter_stream, payload, maxlen=100_000, approximate=True
                    )
                    observability_metrics.cost_dead_letter_total.labels(reason=reason).inc()
                    dead_letter_producers[(str(c.get("service_key") or "<unset>"), reason)] += 1
                dead_lettered_count = len(survived_dead)

            await session.commit()

            # Post-commit: the debit is now durable. Emit per-tenant metrics /
            # logs and release the reservations it settled. Done after the
            # single owning commit so a rolled-back flush never emits a phantom
            # "ok" or releases headroom it did not consume.
            for (tenant_id, lic_id), debits in license_debits.items():
                if not debits:
                    observability_metrics.budget_debits_total.labels(outcome="no_budget").inc()
                    logger.debug(
                        "budget_debit_skipped",
                        tenant_id=tenant_id,
                        license_id=str(lic_id),
                        reason="no_active_budgets",
                        tokens=sum(license_io[(tenant_id, lic_id)]),
                    )
                else:
                    # Each cascaded debit row counts as one outcome. The overflow
                    # step debit_budget appends when a budget ran dry is NOT an
                    # "ok" debit — it is the cap failing to cap. Counting it as
                    # "ok" is what made ``{outcome="oversold"}`` a series that has
                    # never existed, while two Grafana panels chart it and the
                    # comment here promised an alert on it. Measured 2026-08-25:
                    # 739 budget_oversold warnings in 2h, every one reported as
                    # "ok", against a live budget standing at 294% of its limit.
                    ok_steps, oversold_steps = split_debit_outcomes(debits)
                    if oversold_steps:
                        observability_metrics.budget_debits_total.labels(outcome="oversold").inc(
                            oversold_steps
                        )
                    if ok_steps:
                        observability_metrics.budget_debits_total.labels(outcome="ok").inc(ok_steps)
                    if redis_conn is not None:
                        # `handles` is this group's {reservation_id: tokens} map —
                        # see its accumulation site for why it is a map. THIS call
                        # is the per-batch settlement: `debits` below it is the
                        # cascade over a single summed total for the whole group,
                        # so the one release it performs covers every reservation
                        # the batch touched and needs all of their handles at once.
                        # `or {}` keeps the handle-less group (no producer
                        # reserved) on the unchanged anonymous path rather than
                        # passing None around.
                        await release_budget_reservations(
                            redis_conn,
                            tenant_id,
                            debits,
                            handles=license_handles.get((tenant_id, lic_id)) or {},
                        )

            # Release the EUR holds these events settled — by handle, so only
            # THESE reservations are discharged. Best-effort: the debit is
            # already durable, and an unreleased hold lapses at its own TTL.
            if redis_conn is not None:
                for tid, handles in money_handles.items():
                    try:
                        await money_svc.release_money_reservations(redis_conn, tid, handles)
                    except Exception:
                        logger.warning(
                            "money_reservation_release_failed",
                            tenant_id=tid,
                            handles=len(handles),
                            exc_info=True,
                        )

    # ACK after the single owning commit. The budget debit is now folded into
    # that same transaction (see the staged debit_budget(..., commit=False)
    # above), so by the time we reach here the claim, cost_events, token_usage
    # AND the budget debit are all durable together. A crash before the commit
    # leaves the message un-ACKed → it replays and debits exactly once; a
    # redelivered batch stages no debit (survivors empty) → no double-debit.
    if consumer_group is not None and redis_conn is not None and pending_acks:
        by_stream: dict[Any, list[Any]] = {}
        for stream, msg_id in pending_acks:
            by_stream.setdefault(stream, []).append(msg_id)
        for stream, msg_ids in by_stream.items():
            try:
                await redis_conn.xack(stream, consumer_group, *msg_ids)
            except RedisError:
                logger.warning(
                    "xack_after_flush_failed",
                    stream=stream,
                    count=len(msg_ids),
                    exc_info=True,
                )

    logger.info("costs_flushed", count=len(rows), dead_lettered=dead_lettered_count)
    # WHO produced the quarantined events. `cost_dead_letter_total` is labelled by
    # `reason` only, and the producing `service_key` lives nowhere else outside the
    # DLQ payload itself — so answering "which service is emitting these" has meant
    # XRANGE-ing `billing:costs:dead` with the metering Redis credential. That made
    # a live, ongoing defect (arrival ~14/h on 2026-08-27) effectively unattributable
    # to anyone without that credential, and sent at least one investigation chasing
    # the wrong producer.
    #
    # Logged rather than added as a metric LABEL deliberately. `service_key` would be
    # safe on cardinality, but adding a label to a live Counter births new series and
    # zeroes the old ones — and `BillingCostDeadLetter` alerts on
    # `increase(cost_dead_letter_total[...])`, which reads 0 across a series birth.
    # The instrument must not disturb the alert that depends on it.
    if dead_letter_producers:
        logger.warning(
            "cost_dead_letter_producers",
            # Sort the Counter ITEMS (where the count is an int) and build the
            # dicts after — sorting the dicts trips `-d["count"]` on a value the
            # checker widens to `str | int`.
            producers=[
                {"service_key": svc, "reason": rsn, "count": n}
                for (svc, rsn), n in sorted(
                    dead_letter_producers.items(), key=lambda kv: (-kv[1], kv[0][0])
                )
            ],
        )
    return len(rows)
