"""Resolve which licence a licence-less cost event belongs to.

WHY THIS EXISTS
---------------
Measured on prod 2026-08-27 (24h, billable rows only): **3,751 of 4,993**
``billing_cost_events`` rows carry ``license_id IS NULL`` -- 75% of billable
events. That spend is charged and capped (it drains the tenant-level pool) but
it can never reach a customer invoice line, because
``CostDaily.license_attributed_clause`` drops it from every licence-keyed
aggregate.

The cause is a transport gap upstream, not a billing bug: Istio ext_authz runs
only at the ingress gateway (verified live -- exactly three namespaces carry a
CUSTOM AuthorizationPolicy and NO emitter namespace does), so east-west calls
never receive license-authz's ``x-license-id`` header, and most callers do not
forward one by hand.

Fixing every caller is a fleet-wide migration across 21 n8n node packages and
several services. This module closes the invoicing gap at the one place that
sees every event, without changing any caller.

WHAT IT IS, AND WHAT IT IS NOT
------------------------------
This is an **accounting** stamp, never an authorization decision. The billed
tenant has already been decided and already charged by the time we get here; all
this does is name WHICH of that tenant's own licences the existing charge
belongs to. It cannot over-charge, and it cannot grant access to anything.

Because the attribution is DERIVED rather than asserted by the caller, it is
recorded distinctly (``billing_license_resolved_total``, and the event's
``cost_source`` suffix). Attribution that cannot be told apart from a caller's
own assertion is how a derived number later gets quoted as if it were evidence.

THE RULES, IN THE ORDER THAT MATTERS
------------------------------------
1. **Never fabricate.** No match, an ambiguous match, or an unmapped
   ``service_key`` all mean: leave ``license_id`` NULL and keep counting it. A
   wrong ``license_id`` is an invented invoice line and is strictly worse than a
   missing one.
2. **Exactly one, or nothing.** Every tenant that matters owns several licences
   (measured: platform-core 20, platform-cp-1 5, platform-cp-3 4), which is why
   tenant alone cannot pick one. Even keyed by ``(tenant, app)`` a second
   candidate can appear -- two active licences for one app -- and that case
   resolves to NOTHING rather than to the first row. "Pick one" is guessing.
3. **Only resolvable rows.** Both queries apply the SAME two predicates:
   ``app_id IS NOT NULL`` and ``status = 'active'``. ``app_id IS NULL`` (a
   licence synced before migration 0014, or one whose app the upstream omitted)
   is not a candidate, and neither is a licence the registry no longer considers
   active.

   The status half closes what this docstring used to record as a LIMITATION:
   ``request_quota_config`` carried NO status/active/revoked/expiry column, so a
   REVOKED licence stayed resolvable and its tenant kept attracting attributed
   invoice lines until :func:`services.budget_crud.delete_absent_quota_configs`
   happened to remove the row on the next reconcile — and that reconcile is
   operator-triggered, not scheduled. Measured on the registry 2026-09-08: 43
   unrevoked, unexpired tokens hung off ``status='revoked'`` licences (control:
   177 hung off active ones). Migration 0025 added the column and
   ``POST /api/budget/sync-from-licenses`` now mirrors every licence's status
   rather than only fetching the active ones, so the predicate below has
   something real to exclude.

   The test is ``== 'active'``, deliberately not ``!= 'revoked'``: an
   unrecognised or newly-added upstream status must fail CLOSED (not
   resolvable), never resolve by default.

   ⚠️ **RESIDUAL — expiry is NOT covered.** Upstream returns its STORED
   ``status``, not ``License.effective_status()``, so a licence past
   ``expires_at`` whose stored status is still ``active`` mirrors here as
   ``active`` and stays resolvable. Closing that needs ``expires_at`` mirrored
   too. Stated rather than left to be rediscovered: this filters revocation, not
   expiry.
"""

from __future__ import annotations

import time
import uuid
from collections.abc import Iterable, Mapping
from typing import TYPE_CHECKING, Any, Protocol

import structlog
from sqlalchemy import select

import observability.metrics as observability_metrics
from models.db import ACTIVE_LICENSE_STATUS, AppRegistry, RequestQuotaConfig

if TYPE_CHECKING:  # pragma: no cover - typing only
    pass


class _Executor(Protocol):  # pragma: no cover - typing only
    """The only thing ``_build_map`` needs from a session: one read.

    Narrower than ``AsyncSession`` on purpose. It states the real dependency
    (a single SELECT), and it lets the tests drive the ambiguity rules with a
    tiny stand-in instead of a live database — these rules decide which invoice
    line a real charge lands on, so they must be testable without postgres
    being reachable, or they will quietly stop being tested.
    """

    async def execute(self, *args: Any, **kwargs: Any) -> Any: ...


logger = structlog.get_logger(__name__)

#: How long a built map is reused. Licences change on a human timescale (a
#: provisioning call), while this is consulted once per flush batch, so a short
#: TTL costs one small query per interval and bounds staleness to it. A NEW
#: licence simply stays unresolved for at most this long -- which is the safe
#: direction, since the alternative to a stale map is no attribution at all.
_MAP_TTL_SECONDS = 300

#: A ``None`` VALUE inside the map means "several active licences for this pair,
#: deliberately unresolved" — distinct from the map itself being ``None``, which
#: means "not built yet". See :func:`_build_map`.
_cached_map: dict[tuple[str, str], uuid.UUID | None] | None = None
_cached_at: float = 0.0
_cached_app_map: dict[uuid.UUID, uuid.UUID] | None = None
_cached_app_at: float = 0.0
#: ``license_id -> (tenant_of_record, status)``. See :func:`_build_owner_map`.
_cached_owner_map: dict[uuid.UUID, tuple[str, str]] | None = None
_cached_owner_at: float = 0.0
#: ``app_key -> app_id``. See :func:`_build_app_key_map`.
_cached_app_key_map: dict[str, uuid.UUID] | None = None
_cached_app_key_at: float = 0.0


def parse_service_key_app_map(raw: str) -> dict[str, str]:
    """Parse the ``service_key=App Name`` CSV from settings.

    Tolerant of blanks and stray whitespace, because this is operator-authored
    config and a trailing comma must not disable attribution for the whole
    fleet. A malformed PAIR (no ``=``) is skipped and logged rather than raising:
    this is consulted on the flush path, and taking the flusher down over a typo
    in an attribution hint would turn a cosmetic config error into lost billing
    data.
    """
    out: dict[str, str] = {}
    for chunk in raw.split(","):
        item = chunk.strip()
        if not item:
            continue
        if "=" not in item:
            logger.warning("cost_attribution_map_malformed_pair", pair=item)
            continue
        key, _, app = item.partition("=")
        key, app = key.strip(), app.strip()
        if key and app:
            out[key] = app.casefold()
    return out


async def _build_map(session: _Executor) -> dict[tuple[str, str], uuid.UUID | None]:
    """Build ``(tenant_id, app_name) -> license_id`` from the local licence map.

    Ambiguous keys are never resolved to an arbitrary row -- see rule 2 in the
    module docstring. They are logged once per build with their cardinality so an
    operator can see exactly which (tenant, app) pairs are unattributable and
    why, instead of wondering why a tenant never gets attributed.

    ⭐ AN AMBIGUOUS KEY IS NOW PRESENT-WITH-``None`` RATHER THAN ABSENT, and that
    distinction is the whole point of the change. Both shapes make
    :func:`resolve_license` return ``None`` -- the RESOLUTION behaviour is
    byte-identical, nothing is guessed, no event changes attribution -- but the
    two are no longer the same fact:

        key absent          -> the registry has no active licence for this pair
        key present, None   -> the registry has SEVERAL, and we refuse to pick

    Only the second is a data defect somebody upstream can fix, and until now it
    was invisible at the event level. The sole evidence was an UNLABELLED gauge
    plus an info log emitted at most once per 300s cache build, so the 49 live
    rows carrying NULL ``license_id`` for one duplicated pair looked exactly like
    rows for a tenant that simply has no licence. See
    ``billing_license_ambiguous_total``.

    ⚠️ Callers must therefore use ``.get(key)`` / ``key in map`` and MUST NOT
    treat membership as "a licence exists". ``resolve_license`` is the supported
    reader and already does this correctly.
    """
    rows = (
        await session.execute(
            select(
                RequestQuotaConfig.tenant_id,
                RequestQuotaConfig.app_name,
                RequestQuotaConfig.license_id,
            ).where(
                RequestQuotaConfig.app_id.is_not(None),
                # Revoked/expired licences are NOT attribution candidates --
                # see rule 3. `== active` and never `!= revoked`, so an
                # unrecognised upstream status fails CLOSED.
                RequestQuotaConfig.status == ACTIVE_LICENSE_STATUS,
            )
        )
    ).all()

    candidates: dict[tuple[str, str], list[uuid.UUID]] = {}
    for tenant_id, app_name, license_id in rows:
        if not tenant_id or not app_name:
            continue
        candidates.setdefault((str(tenant_id), app_name.casefold()), []).append(license_id)

    resolved: dict[tuple[str, str], uuid.UUID | None] = {}
    ambiguous: list[tuple[str, str, int]] = []
    for key, lic_ids in candidates.items():
        if len(lic_ids) == 1:
            resolved[key] = lic_ids[0]
        else:
            # Recorded as None rather than omitted, so the refusal to guess is
            # readable downstream. `resolve_license` still returns None for it.
            resolved[key] = None
            ambiguous.append((key[0], key[1], len(lic_ids)))

    # Publish the count on EVERY build, including zero, so the gauge can FALL
    # when a registry duplicate is cleaned up rather than sticking at its
    # high-water mark.
    #
    # What this line does NOT buy is the thing this comment used to claim.
    # `attribution_ambiguous_pairs` is an UNLABELLED Gauge, so prometheus_client
    # creates its child at import and exports 0.0 whether or not this function
    # ever runs — "no series" was never the failure mode. The real hazard is the
    # INVERSE: a constant 0.0 is indistinguishable from "the resolver never
    # ran". `_build_map` is only reached while `cost_attribution_app_by_service_key`
    # (settings.py) is non-empty, because cost_flusher gates the whole licence
    # block on `if app_by_key:`. Blank that operator setting and this gauge
    # freezes at 0.0 while nothing is resolved at all, and every `> 0` alert on
    # it goes vacuously green. Any alert on this gauge therefore needs a
    # liveness partner — billing_license_resolved_total stops INCREASING when
    # the resolver stops running, which this gauge cannot express.
    observability_metrics.attribution_ambiguous_pairs.set(len(ambiguous))
    if ambiguous:
        logger.info(
            "cost_attribution_ambiguous_pairs",
            pairs=[{"tenant_id": t, "app": a, "candidates": n} for t, a, n in ambiguous],
        )
    return resolved


async def get_resolution_map(
    session: _Executor, *, now: float | None = None
) -> dict[tuple[str, str], uuid.UUID | None]:
    """Return the cached ``(tenant, app) -> licence`` map, rebuilding on expiry.

    A ``None`` VALUE means "several active licences, deliberately unresolved" —
    see :func:`_build_map`. Read it with :func:`resolve_license`.
    """
    global _cached_map, _cached_at
    stamp = time.monotonic() if now is None else now
    if _cached_map is not None and (stamp - _cached_at) < _MAP_TTL_SECONDS:
        return _cached_map
    _cached_map = await _build_map(session)
    _cached_at = stamp
    return _cached_map


def reset_cache() -> None:
    """Drop every cached map. For tests and for a forced refresh after a sync."""
    global _cached_map, _cached_at, _cached_app_map, _cached_app_at
    global _cached_owner_map, _cached_owner_at
    global _cached_app_key_map, _cached_app_key_at
    global _cached_app_licence_map, _cached_app_licence_at
    _cached_app_licence_map = None
    _cached_app_licence_at = 0.0
    _cached_app_key_map = None
    _cached_app_key_at = 0.0
    _cached_map = None
    _cached_at = 0.0
    _cached_app_map = None
    _cached_app_at = 0.0
    _cached_owner_map = None
    _cached_owner_at = 0.0


async def _build_owner_map(session: _Executor) -> dict[uuid.UUID, tuple[str, str]]:
    """Build ``license_id -> (tenant_of_record, status)`` from the local mirror.

    The INVERSE question from the two maps above. They ask "given a tenant and an
    app, which licence?" so a licence-LESS event can be attributed. This asks
    "given a licence the caller NAMED, whose is it and is it live?" so an
    asserted licence can be ADJUDICATED. Same table, opposite direction, and the
    direction is why it needs its own map rather than a reuse:
    :func:`_build_app_map` is keyed by licence but discards the tenant, and
    :func:`_build_map` is keyed by ``(tenant, app)`` and therefore cannot be
    probed by a licence id at all.

    ⛔ NO ``status`` PREDICATE, AND THAT IS THE ONE DELIBERATE DIFFERENCE FROM
    ITS TWO SIBLINGS. Both of those filter ``status == 'active'`` because their
    job is to pick a licence to ATTRIBUTE to, where a revoked row must simply not
    be a candidate. This map's job is to explain a REFUSAL, and "there is no such
    licence" and "there is such a licence but it is revoked" are different
    findings with different owners — the first is a forged or stale id at the
    emitter, the second is a customer whose licence lapsed. Filtering here would
    collapse the second into the first and send every investigation to the wrong
    team. ``app_id IS NOT NULL`` is dropped for the same reason: a licence synced
    before migration 0014 exists and belongs to someone, and reporting it as
    unknown would be a lie about the mirror.

    ⚠️ WHAT THIS MAP CANNOT ANSWER, STATED SO IT IS NOT ASSUMED. It carries NO
    licence lifecycle timestamps, because ``request_quota_config`` has none —
    there is no ``issued_at``/``valid_from``/``expires_at``/``revoked_at`` column
    on it (see ``models/db/quota.py``'s own ⚠️ and
    ``models.db._common.classify_cost_attribution``'s "Neither this function nor
    its caller can filter on licence status/expiry AT THE BUCKET DATE"). So
    "was this licence active AT THE EVENT'S ``created_at``?" is NOT computable
    here: ``status`` is upstream's CURRENT stored value, and a licence past
    ``expires_at`` whose stored status is still ``active`` mirrors as active.
    Callers get point-in-time-BLIND adjudication and must not describe it
    otherwise. Closing that needs the lifecycle columns mirrored first, which is
    a change to ``POST /api/budget/sync-from-licenses`` and the mirror's schema.
    """
    rows = (
        await session.execute(
            select(
                RequestQuotaConfig.license_id,
                RequestQuotaConfig.tenant_id,
                RequestQuotaConfig.status,
            )
        )
    ).all()
    return {
        license_id: (str(tenant_id), str(status))
        for license_id, tenant_id, status in rows
        if license_id and tenant_id
    }


async def get_owner_map(
    session: _Executor, *, now: float | None = None
) -> dict[uuid.UUID, tuple[str, str]]:
    """Return the cached ``licence -> (tenant, status)`` map, rebuilding on expiry.

    Same TTL as the two maps above, and the staleness direction is the one that
    matters: a licence provisioned within the last ``_MAP_TTL_SECONDS`` is absent
    from this map, so an event naming it would be adjudicated as unknown and its
    spend would be stored-but-not-invoiced for up to five minutes' worth of
    events. That is recoverable (the row is intact and one UPDATE re-attributes
    it); the opposite bias — caching a licence past its revocation — would keep
    invoicing against it. Neither is free, and this is the cheaper one.
    """
    global _cached_owner_map, _cached_owner_at
    stamp = time.monotonic() if now is None else now
    if _cached_owner_map is not None and (stamp - _cached_owner_at) < _MAP_TTL_SECONDS:
        return _cached_owner_map
    _cached_owner_map = await _build_owner_map(session)
    _cached_owner_at = stamp
    return _cached_owner_map


def resolve_license(
    *,
    tenant_id: str | None,
    service_key: str | None,
    app_by_service_key: dict[str, str],
    # Mapping, not dict: dict is INVARIANT in its value type, so a caller
    # holding a plain dict[..., UUID] — every test helper, and any future caller
    # that builds a map without duplicates — would not type-check against
    # dict[..., UUID | None]. Mapping is covariant and accepts both.
    resolution_map: Mapping[tuple[str, str], uuid.UUID | None],
) -> uuid.UUID | None:
    """Resolve the licence for one licence-less event, or None.

    Returning None is a first-class, expected outcome -- see rule 1. The caller
    keeps its existing ``license_unattributed_total`` accounting so the
    unattributed rate stays measurable and is never masked by this feature.

    ⭐ ONE of the ``None`` paths is now counted here: the AMBIGUOUS one. The
    other three (no tenant, no service_key, unmapped service_key, no licence for
    the pair) still increment nothing, exactly as before.

    Counting it at THIS point, rather than leaving it to the caller the way
    :func:`record_resolved` is left to the caller, is deliberate: this is the
    only place in the process that still knows WHICH of the reasons produced the
    ``None``. One line up the stack the information is gone — the caller sees a
    bare ``None`` and cannot tell a duplicated licence from an absent one, which
    is precisely the blindness being fixed. Pushing the count outward would mean
    re-deriving the reason at both call sites (``services.cost_flusher`` and
    ``services.event_flusher``), i.e. two copies of one rule, which is how the
    two flushers grew the identical tenant_name defect.

    ⛔ THE RESOLUTION BEHAVIOUR IS UNCHANGED. An ambiguous pair returned None
    before this commit and returns None after it. Nothing is guessed, no event
    changes attribution, and no row that used to carry a licence stops carrying
    one. The only new thing in the world is a counter.
    """
    if not tenant_id or not service_key:
        return None
    app = app_by_service_key.get(service_key)
    if app is None:
        return None
    key = (str(tenant_id), app)
    if key not in resolution_map:
        # No active licence for this pair at all. Silent, as before — it is the
        # ordinary state of every tenant who has not bought the app.
        return None
    licence = resolution_map[key]
    if licence is None:
        observability_metrics.license_ambiguous_total.labels(
            service_key=service_key or "unknown"
        ).inc()
        return None
    return licence


# ---------------------------------------------------------------------------
# Licence derivation from an ASSERTED app (tenant, app uuid)
# ---------------------------------------------------------------------------
#
# WHY: per-app licensing (live 2026-10-14) made DELEGATED calls common: a service
# (kag) calls a shared leaf (mlx-backend) with its OWN service licence plus
# ``X-Effective-Tenant-Id`` naming the acting tenant, and the leaf deliberately
# drops the service licence so it never lands on another tenant's row. The row
# arrives licence-less. The ``service_key -> app`` map above cannot attribute it:
# a shared leaf serves several apps, so it cannot sit in that map without
# guessing. Measured 2026-10-15: mlx-backend rows for a tenant holding an active
# KAG Module licence stored ``license_id`` NULL. When the caller ASSERTS the app
# (``X-App-Id``, a uuid or a key resolved by :func:`resolve_asserted_app_keys`),
# the app is known, and ``(tenant, app)`` is exactly the key rule 2 needs.
#
# Same three rules as :func:`_build_map`: same ``app_id IS NOT NULL`` +
# ``status == 'active'`` predicates (revoked licences never resolve), exactly
# one candidate or nothing, and an ambiguous pair is present-with-``None`` so the
# refusal is counted on ``billing_license_ambiguous_total``. Keyed by the app
# UUID rather than the app NAME because the assertion IS a uuid after key
# resolution; mapping it back through ``app_name`` would add a second, lossy
# join (names are not unique upstream) for no gain.
#
# The tenant half of the key is the row's own tenant, so an asserted app can
# only ever select a licence the billed tenant itself holds -- an asserted app
# whose licence belongs to someone else is simply absent from this tenant's key.

_cached_app_licence_map: dict[tuple[str, uuid.UUID], uuid.UUID | None] | None = None
_cached_app_licence_at: float = 0.0


async def _build_app_licence_map(
    session: _Executor,
) -> dict[tuple[str, uuid.UUID], uuid.UUID | None]:
    """Build ``(tenant_id, app_id) -> license_id``; ambiguous pairs map to None."""
    rows = (
        await session.execute(
            select(
                RequestQuotaConfig.tenant_id,
                RequestQuotaConfig.app_id,
                RequestQuotaConfig.license_id,
            ).where(
                RequestQuotaConfig.app_id.is_not(None),
                # Same predicate as _build_map (rule 3): `== active`, so a
                # revoked or unrecognised status fails CLOSED.
                RequestQuotaConfig.status == ACTIVE_LICENSE_STATUS,
            )
        )
    ).all()

    candidates: dict[tuple[str, uuid.UUID], list[uuid.UUID]] = {}
    for tenant_id, app_id, license_id in rows:
        if not tenant_id or not app_id or not license_id:
            continue
        candidates.setdefault((str(tenant_id), app_id), []).append(license_id)

    resolved: dict[tuple[str, uuid.UUID], uuid.UUID | None] = {}
    ambiguous: list[dict[str, Any]] = []
    for key, lic_ids in candidates.items():
        if len(lic_ids) == 1:
            resolved[key] = lic_ids[0]
        else:
            resolved[key] = None
            ambiguous.append(
                {"tenant_id": key[0], "app_id": str(key[1]), "candidates": len(lic_ids)}
            )
    if ambiguous:
        logger.info("cost_attribution_ambiguous_app_pairs", pairs=ambiguous)
    return resolved


async def get_app_licence_map(
    session: _Executor, *, now: float | None = None
) -> dict[tuple[str, uuid.UUID], uuid.UUID | None]:
    """Return the cached ``(tenant, app_id) -> licence`` map (same TTL as the others)."""
    global _cached_app_licence_map, _cached_app_licence_at
    stamp = time.monotonic() if now is None else now
    if _cached_app_licence_map is not None and (stamp - _cached_app_licence_at) < _MAP_TTL_SECONDS:
        return _cached_app_licence_map
    _cached_app_licence_map = await _build_app_licence_map(session)
    _cached_app_licence_at = stamp
    return _cached_app_licence_map


def resolve_license_for_asserted_app(
    *,
    tenant_id: str | None,
    app_id: uuid.UUID | None,
    service_key: str | None,
    app_licence_map: Mapping[tuple[str, uuid.UUID], uuid.UUID | None],
) -> uuid.UUID | None:
    """Resolve the licence for a licence-less event whose caller asserted the app.

    Same contract as :func:`resolve_license`: ``None`` for no tenant, no app, no
    active licence for the pair, or several (the last one counted on
    ``billing_license_ambiguous_total``). Never guesses.
    """
    if not tenant_id or app_id is None:
        return None
    key = (str(tenant_id), app_id)
    if key not in app_licence_map:
        return None
    licence = app_licence_map[key]
    if licence is None:
        observability_metrics.license_ambiguous_total.labels(
            service_key=service_key or "unknown"
        ).inc()
        return None
    return licence


def derive_license(
    *,
    tenant_id: str | None,
    service_key: str | None,
    app_asserted: bool,
    asserted_app_id: uuid.UUID | None,
    app_by_service_key: dict[str, str],
    resolution_map: Mapping[tuple[str, str], uuid.UUID | None] | None,
    app_licence_map: Mapping[tuple[str, uuid.UUID], uuid.UUID | None] | None,
) -> uuid.UUID | None:
    """The ONE ordering rule both flushers share for a licence-less row.

    * The caller asserted an app (``app_asserted``) -> resolve from
      ``(tenant, asserted app)`` ONLY. If that yields nothing -- the tenant holds
      no active licence for that app, the pair is ambiguous, or an asserted KEY
      did not resolve (``asserted_app_id`` None) -- the row stays licence-less.
      It does NOT fall back to the ``service_key`` map: that would name a
      licence for a DIFFERENT app than the one the caller asserted, i.e. an
      invoice line that disagrees with the row's own ``app_id``.
    * Nothing asserted -> the ``service_key`` map, unchanged.

    A ``None`` map means "not built" (its build failed or was not needed) and
    resolves nothing.
    """
    if app_asserted:
        if app_licence_map is None:
            return None
        return resolve_license_for_asserted_app(
            tenant_id=tenant_id,
            app_id=asserted_app_id,
            service_key=service_key,
            app_licence_map=app_licence_map,
        )
    if resolution_map is None:
        return None
    return resolve_license(
        tenant_id=tenant_id,
        service_key=service_key,
        app_by_service_key=app_by_service_key,
        resolution_map=resolution_map,
    )


def record_resolved(service_key: str | None) -> None:
    """Count a DERIVED attribution, separately from an asserted one."""
    observability_metrics.license_resolved_total.labels(service_key=service_key or "unknown").inc()


def birth_resolved_series(app_by_service_key: Mapping[str, str]) -> tuple[str, ...]:
    """Export ``billing_license_resolved_total`` at 0 for every reachable key.

    ⭐ ACCEPTANCE: after this runs, every service_key the operator has mapped has
    a series on a pod that has resolved nothing for it.

    ⚠️ THIS ADDS NO INCREMENT. :func:`record_resolved` is live and correct; the
    counter was measured incrementing on 2026-09-14. What it had was ONE series
    against a map carrying 14 service_keys that day — read the setting, never a
    remembered count — because a child is only created once that key has actually
    resolved a licence. Both ``billing:licence_derived_share:1h`` and
    ``BillingLicenceAssertionRegressed`` are evaluated ``by (service_key)``, so
    for every unborn key "this emitter's resolution stopped" and "it has never
    happened" are the same empty vector, and an expression over an empty vector is
    vacuously green. The keys that cannot be seen going quiet are exactly the ones
    worth seeing. Birth is what separates absence from silence.

    ⛔ THE BIRTH LIST IS THE REACHABLE SET, NOT A ROSTER BESIDE IT. ``record_resolved``
    is called from exactly one place (``services.cost_flusher``), inside
    ``if app_by_key:``, and only for a row ``resolve_license`` actually resolved —
    which requires ``app_by_service_key.get(row["service_key"])`` to have hit. So
    the values that call can produce are precisely the KEYS of this mapping, and
    passing the caller the SAME ``parse_service_key_app_map(...)`` output the
    flusher gates on is what stops the two from drifting.

    ⛔ ``"unknown"`` IS DELIBERATELY NOT BIRTHED, even though this module's own
    ``or "unknown"`` fallback names it and the sibling counter
    ``billing_license_unattributed_persisted_total`` legitimately births it.
    ``cost_flusher`` normalises every staged row to a non-empty ``service_key``
    before this point, so that fallback is unreachable from the only call site;
    a licence is only ever resolved for a key the operator mapped. Birthing it
    would pin a series at 0 forever, which reads as an instrument watching a
    healthy population while it watches nothing.

    Returns the birthed keys, sorted, so a caller or a test can assert WHAT was
    born rather than only that something was.
    """
    born = tuple(sorted(app_by_service_key))
    for key in born:
        observability_metrics.license_resolved_total.labels(service_key=key)
    return born


# ---------------------------------------------------------------------------
# App derivation
# ---------------------------------------------------------------------------
#
# WHY THIS IS A SEPARATE MAP, AND WHY IT IS SAFER THAN THE ONE ABOVE
#
# Measured 2026-09-02: ``app_id`` is NULL on 401,316 of 401,316 cost events --
# 100%, since the column existed. Per-application cost is therefore not merely
# inaccurate, it is arithmetically impossible. Yet 125,424 of the last 146,495
# events (85.6%) already carry a ``license_id`` whose licence is present in the
# local mirror WITH a non-NULL ``app_id``. The grain we cannot report is already
# in the database; nothing was propagating it onto the event.
#
# Crucially this derivation is NOT exposed to rule 2 above. ``license_id`` is
# the PRIMARY KEY of ``request_quota_config``, so a licence maps to exactly one
# app by construction -- the duplicate ``(tenant, app)`` pairs that force
# ``_build_map`` to drop a key (measured: 7 such pairs, 16 licences) cannot
# produce an ambiguous licence->app edge. "Exactly one, or nothing" is satisfied
# by the schema rather than by a filter.
#
# Rule 1 still holds unchanged: a licence we cannot map, or an event with no
# licence at all, leaves ``app_id`` NULL and keeps being counted. An invented
# app is an invented product line and is strictly worse than a missing one.


async def _build_app_map(session: _Executor) -> dict[uuid.UUID, uuid.UUID]:
    """Build ``license_id -> app_id`` from the local licence mirror.

    No ambiguity pass is needed or possible: ``license_id`` is the primary key,
    so the SELECT cannot return two rows for one licence. Rows whose ``app_id``
    is NULL (a licence synced before migration 0014, or one whose app the
    upstream omitted) are excluded by the same predicate ``_build_map`` uses, so
    both maps agree on what "resolvable" means.
    """
    rows = (
        await session.execute(
            select(
                RequestQuotaConfig.license_id,
                RequestQuotaConfig.app_id,
            ).where(
                RequestQuotaConfig.app_id.is_not(None),
                # Same pair as _build_map, so both maps agree on what
                # "resolvable" means -- a licence excluded from one but present
                # in the other would attribute cost to a licence the (tenant,
                # app) map cannot name.
                RequestQuotaConfig.status == ACTIVE_LICENSE_STATUS,
            )
        )
    ).all()
    return {license_id: app_id for license_id, app_id in rows if license_id and app_id}


async def get_app_map(
    session: _Executor, *, now: float | None = None
) -> dict[uuid.UUID, uuid.UUID]:
    """Return the cached ``licence -> app`` map, rebuilding on expiry.

    Same TTL as the licence map, and for the same reason: a newly provisioned
    licence stays un-attributed for at most one interval, which is the safe
    direction compared with holding a stale map open indefinitely.
    """
    global _cached_app_map, _cached_app_at
    stamp = time.monotonic() if now is None else now
    if _cached_app_map is not None and (stamp - _cached_app_at) < _MAP_TTL_SECONDS:
        return _cached_app_map
    _cached_app_map = await _build_app_map(session)
    _cached_app_at = stamp
    return _cached_app_map


def resolve_app(
    *,
    license_id: uuid.UUID | None,
    app_map: dict[uuid.UUID, uuid.UUID],
) -> uuid.UUID | None:
    """Derive the app for one event from its licence, or None.

    ``None`` is a first-class outcome: an event with no licence, or one whose
    licence is absent from the mirror, is left unattributed and counted. This
    never overwrites an ``app_id`` the caller actually presented -- the caller
    checks that before calling.
    """
    if license_id is None:
        return None
    return app_map.get(license_id)


def record_app_resolved(service_key: str | None) -> None:
    """Count a DERIVED app attribution, separately from an asserted one."""
    observability_metrics.app_resolved_total.labels(service_key=service_key or "unknown").inc()


def record_app_unattributed(service_key: str | None) -> None:
    """Count an event stored with no app, so the remaining gap stays visible."""
    observability_metrics.app_unattributed_total.labels(service_key=service_key or "unknown").inc()


# ---------------------------------------------------------------------------
# App KEY resolution (X-App-Id)
# ---------------------------------------------------------------------------
#
# Every inference-router caller is a registered app in licences, and its
# ``X-App-Id`` header carries the app KEY (slug: ``core-app``,
# ``chat-bridge-rmt``, ``workers``), not the uuid. The licence on a router event
# names the ROUTER's app, so deriving the app from the licence loses the caller.
# This map turns the asserted key back into the registry uuid.
#
# SOURCE: the ``app_registry`` mirror of the licences APP registry (migration
# 0050, filled by sync-from-licenses from ``GET /internal/apps``). It used to be
# ``request_quota_config.app_key``, which only holds apps that own a LICENCE --
# so registration-only platform apps (``core-app``, ``chat-bridge*``) were
# never in the map and every one of their events resolved as unknown. That
# column is now only a FALLBACK, used when ``app_registry`` is empty (a pod on a
# database that has not been synced since 0050), and the fallback is counted on
# ``billing_app_key_map_fallback_total`` so it cannot pass for the real thing.
#
# "Exactly one, or nothing" on the fallback: a key the licence mirror maps to two
# different app ids is DROPPED rather than picked -- the same rule
# ``_build_map`` applies to an ambiguous (tenant, app) pair. ``app_registry``
# cannot be ambiguous (``app_key`` is UNIQUE there, as it is upstream). Status
# is NOT filtered on either source: the key -> app edge is a fact about the
# app, so a revoked licence -- or a revoked app, which still owns its
# historical spend -- does not un-register the key.


def birth_app_key_resolved_series(app_keys: Iterable[str]) -> None:
    """Create ``billing_app_key_resolved_total`` children for every mapped key.

    The ``app_key`` label is bounded by the map (only a mapped key is ever
    incremented), so the birth domain IS the map: no roster to drift, and an
    unbounded caller-supplied key can never reach a label.
    """
    for app_key in app_keys:
        for stream in observability_metrics.APP_KEY_STREAMS:
            observability_metrics.app_key_resolved_total.labels(stream=stream, app_key=app_key)


async def _build_app_key_map_from_licence_mirror(session: _Executor) -> dict[str, uuid.UUID]:
    """The pre-0050 source: licensed apps only (distinct, non-null, unambiguous)."""
    rows = (
        await session.execute(
            select(RequestQuotaConfig.app_key, RequestQuotaConfig.app_id)
            .where(
                RequestQuotaConfig.app_key.is_not(None),
                RequestQuotaConfig.app_id.is_not(None),
            )
            .distinct()
        )
    ).all()
    seen: dict[str, set[uuid.UUID]] = {}
    for app_key, app_id in rows:
        if app_key and app_id:
            seen.setdefault(app_key, set()).add(app_id)
    ambiguous = sorted(k for k, ids in seen.items() if len(ids) > 1)
    if ambiguous:
        logger.warning("app_key_map_ambiguous_keys_dropped", app_keys=ambiguous)
    return {k: next(iter(ids)) for k, ids in seen.items() if len(ids) == 1}


async def _build_app_key_map(session: _Executor) -> dict[str, uuid.UUID]:
    """Build ``app_key -> app_id`` from ``app_registry``; licence mirror if it is empty."""
    rows = (await session.execute(select(AppRegistry.app_key, AppRegistry.app_id))).all()
    key_map: dict[str, uuid.UUID] = {row.app_key: row.app_id for row in rows}
    if not key_map:
        observability_metrics.app_key_map_fallback_total.inc()
        logger.warning("app_key_map_fallback_to_licence_mirror", reason="app_registry_empty")
        key_map = await _build_app_key_map_from_licence_mirror(session)
    birth_app_key_resolved_series(key_map)
    return key_map


async def get_app_key_map(session: _Executor, *, now: float | None = None) -> dict[str, uuid.UUID]:
    """Return the cached ``app_key -> app_id`` map, rebuilding on expiry (same TTL)."""
    global _cached_app_key_map, _cached_app_key_at
    stamp = time.monotonic() if now is None else now
    if _cached_app_key_map is not None and (stamp - _cached_app_key_at) < _MAP_TTL_SECONDS:
        return _cached_app_key_map
    _cached_app_key_map = await _build_app_key_map(session)
    _cached_app_key_at = stamp
    return _cached_app_key_map


def split_asserted_app(raw: Any) -> tuple[uuid.UUID | None, str | None]:
    """Split a wire ``app_id`` into ``(app_uuid, app_key)``.

    A uuid is the app as asserted -> ``(uuid, None)``. Any other non-empty
    string is an app KEY (the X-App-Id slug) -> ``(None, key)``. Absent/blank or
    a non-string -> ``(None, None)``, which leaves licence derivation unchanged.
    """
    if not isinstance(raw, str):
        return None, None
    value = raw.strip()
    if not value:
        return None, None
    try:
        return uuid.UUID(value), None
    except ValueError:
        return None, value


async def resolve_asserted_app_keys(
    session: Any,
    keyed_rows: list[tuple[dict[str, Any], str]],
    *,
    stream: str,
) -> None:
    """Stamp ``row["app_id"]`` for each ``(row, app_key)`` the mirror can resolve.

    Shared by the cost and metering flushers. Fail-soft: the map read runs in its
    own SAVEPOINT, and on error every row keeps ``app_id`` NULL (logged). An
    unknown key keeps NULL and is counted on ``billing_app_key_unknown_total``.
    The CALLER must still exclude these rows from licence derivation -- never
    guess a caller from the licence.
    """
    if not keyed_rows:
        return
    try:
        async with session.begin_nested():
            key_map = await get_app_key_map(session)
    except Exception:
        logger.warning("app_key_resolution_failed", stream=stream, exc_info=True)
        return
    for row, app_key in keyed_rows:
        app_id = key_map.get(app_key)
        if app_id is None:
            observability_metrics.app_key_unknown_total.labels(stream=stream).inc()
            continue
        row["app_id"] = app_id
        # Label bounded: only keys present in the map reach here.
        observability_metrics.app_key_resolved_total.labels(stream=stream, app_key=app_key).inc()
