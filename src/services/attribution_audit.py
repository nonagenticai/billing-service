"""Does the licence on a cost event belong to the tenant the event is billed to?

Nothing in the fleet asked that question. Measured on prod 2026-08-25 across a
7-day window of ``billing_cost_events``:

===============================================  ========  ==========
population                                        events     spend
===============================================  ========  ==========
licence present AND owned by the event's tenant      7,562      $86.76
licence present AND owned by SOMEONE ELSE           11,407     $363.56
no licence at all                                   13,466     $435.51
licence present but absent from the registry             0           -
===============================================  ========  ==========

So 81% of *attributed* spend named a foreign licence. That is worse than a NULL:
a NULL is honestly unknown, a mismatch is confidently wrong. The last row is the
control -- every licence resolved, so this is not a stale-id artifact.

It is not only an invoicing label. ``debit_budget`` scopes the cascade to the
EVENT's tenant (``_select_active_budgets``), so a foreign licence's spend drains
the event tenant's shared pool while the named licence's own budget is untouched.
Licence ``019f82c0-...`` read **1,321** consumed tokens while the stream recorded
**94,624,434** for it -- the enforcement gate in ``license-authz`` saw 0.0% of a
10M budget where reality was 946%. Meanwhile the pool that DID absorb it sat at
264,243,479 / 90,000,000 = 294%, ``active``, refusing nothing.

This module measures both, on the leader's rollup tick. It **refuses nothing and
changes no behaviour** -- deliberately. Refusing on the inference hot path before
anyone knows who it would cut off is how an attribution fix becomes an outage.
Count first; the policy decision (is a platform-pays arrangement intended?) is
not an engineering one.

The SQL lives in :func:`run_attribution_audit`; the adjudication lives in pure
functions above it so a regression is catchable without a database.

⛔ WHERE "WHO OWNS THIS LICENCE" COMES FROM, AND WHY IT IS NOT A LOCAL TABLE
---------------------------------------------------------------------------
Until 2026-09-11 the owners map came from ``SELECT license_id, tenant_id FROM
request_quota_config`` -- and **that made the audit structurally incapable of
failing.** ``request_quota_config.tenant_id`` is CALLER-ASSERTED: the only writer
is ``routers/quota.py`` ``set_quota_config``, gated on nothing but a role check
(``require_internal``), which forwards the request body's ``tenant_id`` into
``services/quota.py``'s ``on_conflict_do_update`` -- where ``tenant_id`` is set
**unconditionally** while every sibling column is COALESCE-protected. The table is
then refreshed from the same event stream the audit is auditing (measured: its
``updated_at`` postdates the licences' ``created_at``), so it agrees with whatever
tenant the event carried, by construction.

Measured 2026-09-11, the two sources side by side:

=========================================  =========================
source                                     mismatches found in 24h
=========================================  =========================
``request_quota_config`` (self-derived)     **0**  ($0.00)
licences registry ``license`` table         **188**  ($0.1242 of $320.79,
                                            4 license_ids)
=========================================  =========================

and one license_id carried **two different tenants** in ``request_quota_config``
while the registry maps it to exactly one. A self-derived control cannot refute
the thing that derived it; the alert ``BillingMisattributedSpendIncreased`` was
understated by construction, not by threshold.

The owners map is now resolved from the **licences registry over HTTP** --
:func:`fetch_licence_owners`. ``request_quota_config`` is still read, but it has
been demoted from AUTHORITY to SUBJECT: the audit now reports how far the local
proxy DISAGREES with the registry (see :func:`compare_local_owner_proxy`) instead
of believing it.

⚠️ THERE IS NO FALLBACK, AND THAT IS THE POINT. When licences cannot be reached
the audit publishes **nothing** for the four ownership gauges and **withholds the
freshness timestamp**, so the whole pass is uncertified and
``billing:attribution_audit:fresh`` goes false. A ``0`` that means "I could not
check" is the exact defect being removed here, and it is worse than a stale
reading: the stale one is at least a number somebody once measured. The
precedent is :class:`services.byo_mode.ByoModeUnavailableError` -- a money-
FREEZING caller refuses, a REPORTING caller keeps a safe default -- and this is a
reporting caller whose entire output is a number an alert reads, so its "safe
default" is ABSTENTION, not a value. The module already holds that idiom: see
``attribution_rate``, which publishes nothing rather than a 0.0 on an empty
window for the identical reason.
"""

from __future__ import annotations

import time
import uuid
from collections.abc import Iterable, Mapping, Sequence
from dataclasses import dataclass, field
from datetime import UTC, datetime, timedelta
from typing import TYPE_CHECKING, Any

import httpx
import structlog
from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from models.db import NULL_LICENSE_ID

# ⚠️ FROM THE PRIVATE MODULE, not `models.db`, and not a copy of the literal.
# `models/db/__init__.py` re-exports the older sentinels but not this one, and
# that file belongs to another change set -- so the choice here is the private
# path or a second hand-written `…-0004` in this file. A duplicated sentinel
# that drifts from the column default is exactly the defect this audit measures.
from models.db._common import NULL_PROJECT_ID
from observability import metrics as observability_metrics
from service_auth import ServiceTokenProvider
from services.tenant_names import UNCLASSIFIED, get_tenant_info

if TYPE_CHECKING:
    from settings import Settings

logger = structlog.get_logger(__name__)

#: How far back each audit pass looks. One day is long enough to survive a few
#: missed leader ticks and short enough that a fixed emitter shows up as a
#: falling gauge within a day rather than being diluted by a week of history.
DEFAULT_WINDOW_HOURS = 24

#: Emitted verbatim on every audit log line, beside the three utilisation arms.
#: ⛔ THIS STRING IS THE POINT OF THE THREE ARMS BEING ON ONE LINE. Three numbers
#: that disagree invite the reader to believe the largest one, and the largest is
#: reliably the one nothing enforces -- that is precisely the false alarm of
#: 2026-09-10 (per-row 4.77 against tenants at 57.2% / 22.5%) and again of
#: 2026-09-11 (per-licence 205% against THAT licence's tenant at 14.7%). So the line SAYS
#: which arm the gate reads.
BUDGET_GRAIN_LEGEND = (
    "ENFORCED=tenant_aggregate "
    "(reserve_budget: consumed + reserved + estimated <= sum(budget_tokens)); "
    "DIAGNOSTIC-ONLY=per_licence (debit scope: the licence's rows + the shared "
    "tenant pool) and per_row (the debit cascade dumps every remainder on the "
    "LAST row it visits). A per_licence or per_row ratio above 1.0 refuses "
    "nothing and is NOT evidence the cap stopped enforcing."
)

#: :attr:`AttributionAudit.owner_source` -- the ownership map came from the
#: licences registry and the four ownership gauges may be published.
OWNER_SOURCE_REGISTRY = "licences_registry"

#: :attr:`AttributionAudit.owner_source` -- licences could not state ownership
#: this tick. ⛔ The four ownership gauges MUST NOT be published and the freshness
#: timestamp MUST be withheld. There is deliberately no third value: a local
#: fallback would need its own gauge to be distinguishable on a panel, and until
#: one exists an undistinguishable fallback is just the old defect with a longer
#: code path.
OWNER_SOURCE_UNAVAILABLE = "unavailable"


class LicenceOwnersUnavailableError(RuntimeError):
    """The licences registry could not state licence -> owning-tenant this tick.

    Distinct from "the registry holds no licences", which is a successful 200
    carrying ``[]`` and is ALSO refused -- see :func:`parse_licence_owners`.

    Deliberately a plain ``RuntimeError``, for the same reason
    :class:`services.byo_mode.ByoModeUnavailableError` and
    :class:`services.seats.SeatCountUnavailableError` are: a transport failure
    talking to another service is not a defect of the module that was asking.

    ⚠️ :func:`run_attribution_audit` CATCHES this rather than propagating it.
    That is not a softening -- the abstention it triggers is strictly louder than
    a raise would be, because it still publishes the DB-only populations (a human
    mid-outage can still read unattributed/unregistered/budget figures) while
    withholding the freshness timestamp, which is the module's ONE documented
    "do not believe this pass" signal. A raise would publish nothing at all and
    leave the same timestamp stale, i.e. the same verdict with less information.
    """


@dataclass
class AttributionAudit:
    """Outcome of one coherence pass over the cost-event window."""

    #: Events in the window that carry a licence AT ALL. NOT the whole billable
    #: population -- see :attr:`billable_events`. Every field below it is an
    #: adjudication of a licence that exists, so this is their denominator.
    total_events: int = 0
    matched_events: int = 0
    mismatch_events: int = 0
    unresolved_events: int = 0
    mismatch_spend_usd: float = 0.0
    #: Groups whose licence owner differs from the billed tenant BECAUSE
    #: inference-router honoured a delegated call (a service licence spending on
    #: behalf of an end tenant; ``metadata.delegated_by`` set by the router).
    #: Counted HERE and not in ``mismatch_*``: the shape is intended, and an
    #: alert that fires on intended traffic is one that gets silenced. Only a
    #: router-emitted vouch counts -- see ``_WINDOW_SQL``.
    delegated_events: int = 0
    delegated_spend_usd: float = 0.0
    #: Events in the SAME window, under the SAME billable filter, carrying no
    #: license_id at all. Measured 2026-08-27: 3,751 of 4,993 in 24h. The
    #: coherence fields above are structurally blind to these -- there is no
    #: ownership claim to adjudicate -- so before this was counted the whole
    #: dashboard described only the 25% of the population that already worked.
    unattributed_events: int = 0
    unattributed_spend_usd: float = 0.0
    #: Billable events in the SAME window whose TENANT is absent from billing's
    #: own registry -- no request_quota_config row and no token_budgets row.
    #: A DIFFERENT defect from the two above: those adjudicate a licence, this
    #: asks whether the identity the spend is booked to exists here at all.
    #: Measured 2026-08-28: 26 of the 61 distinct tenant_ids in
    #: billing_cost_events have no row in licenses.tenants either -- including a
    #: literal ``0195f48b-dead-7000-8000-00000000beef``. Nothing validated
    #: tenant identity at the financial boundary.
    unregistered_tenant_events: int = 0
    unregistered_tenant_spend_usd: float = 0.0
    #: (license_id, billed_tenant, owning_tenant) -> events, for the log line.
    top_pairs: list[tuple[str, str, str, int]] = field(default_factory=list)
    #: Same shape as ``top_pairs``, for the delegated groups -- kept apart so the
    #: mismatch pairs a human acts on are never crowded out by intended traffic.
    top_delegated_pairs: list[tuple[str, str, str, int]] = field(default_factory=list)
    #: (tenant_id, events) for the worst offenders, so the log line NAMES the
    #: identity instead of only counting it. A count alone is unactionable.
    top_unregistered_tenants: list[tuple[str, int]] = field(default_factory=list)

    #: WHERE the ownership verdicts above came from: :data:`OWNER_SOURCE_REGISTRY`
    #: or :data:`OWNER_SOURCE_UNAVAILABLE`.
    #:
    #: ⚠️ DEFAULTS TO ``""`` -- neither value -- ON PURPOSE. A bare
    #: ``AttributionAudit()`` has adjudicated nothing, and the two honest
    #: alternatives both lie: ``REGISTRY`` would certify a pass that never ran,
    #: and ``UNAVAILABLE`` would name an outage that never happened. ``""`` is
    #: falsy, so :attr:`coherence_measured` reads False for it and every
    #: publication guard fails SAFE against a caller that forgot to set it.
    owner_source: str = ""
    #: Licences the registry returned (the denominator of the owners map).
    registry_licences: int = 0
    #: Registry rows carrying no usable tenant-of-record. Counted, not raised on:
    #: ``InternalLicenseResponse.tenant_id`` is a REQUIRED ``str`` upstream, so a
    #: nonzero value here means the registry's own column went NULL and is a
    #: finding in its own right. Those licences are EXCLUDED from the map, so
    #: their spend reads ``unresolved`` -- never ``matched``.
    registry_unowned_licences: int = 0
    #: ⭐ THE OLD AUTHORITY, MEASURED INSTEAD OF BELIEVED. Licences where
    #: ``request_quota_config`` names a DIFFERENT owning tenant than the registry
    #: does. This is the size of the lie the audit used to be told; 0 here means
    #: the local proxy happens to agree today, NOT that it is a valid source.
    local_owner_disagreements: int | None = None
    #: Licences ``request_quota_config`` holds that the registry does not have at
    #: all. Its own finding (billing mirrors a licence the registry dropped), and
    #: the reason this is counted separately from a disagreement: the remedies
    #: differ (reconcile the mirror vs. correct an owner).
    local_owner_only_licences: int | None = None

    @property
    def coherence_measured(self) -> bool:
        """May the four ownership gauges be published from this pass?

        ⛔ The ONE predicate every ownership publication is gated on. Asked as a
        positive question about the SOURCE rather than as ``if mismatch_events``,
        because 0 mismatches is a perfectly ordinary healthy reading and is also
        exactly what an unchecked window looks like -- which is the entire defect
        this field exists to make impossible to re-introduce.
        """
        return self.owner_source == OWNER_SOURCE_REGISTRY

    @property
    def billable_events(self) -> int:
        """Every billable event in the window, attributed or not."""
        return self.total_events + self.unattributed_events

    @property
    def mismatch_ratio(self) -> float | None:
        """Mismatched share of the LICENCE-BEARING window, or None when empty.

        Denominator is ``total_events``, not ``billable_events``, deliberately:
        a licence-less event cannot be mismatched, so folding it in would dilute
        this rate with a population it does not describe and make a coherence
        regression look smaller every time the unattributed gap grew.

        Returns ``None`` rather than 0.0 on an empty population: a rate without
        its denominator invents a defect, and "0% mismatched" out of zero events
        reads as health when it is actually silence.
        """
        if self.total_events <= 0:
            return None
        return self.mismatch_events / self.total_events

    @property
    def attribution_rate(self) -> float | None:
        """Share of the billable window that carries a licence, or None.

        Answers "can this spend reach an invoice line at all", NOT "is the
        licence the right one" -- coherence of a licence that IS present is
        :attr:`mismatch_ratio`'s job. An event can be counted here and still be
        mismatched; the two rates are independent defects.

        ``None`` on an empty window, and the caller must then publish NOTHING.
        0.0 would say "not one event could be attributed" -- total breakage --
        on a quiet night whose true answer is "there was nothing to attribute".
        """
        if self.billable_events <= 0:
            return None
        return self.total_events / self.billable_events


@dataclass
class BudgetUtilisation:
    """One active budget's standing consumption, as the audit sees it."""

    budget_id: uuid.UUID
    #: The tenant this budget belongs to. Carried so utilisation can be
    #: summarised at the grain the ENFORCER uses -- see
    #: :func:`summarise_budget_utilisation`.
    tenant_id: str
    budget_tokens: int
    consumed_tokens: int
    #: True when ``consumed_tokens`` belongs to an ALREADY-ROLLED period.
    stale: bool
    #: The licence scope this row belongs to -- ``TokenBudget.license_id``, which
    #: is NOT NULL and carries :data:`NULL_LICENSE_ID` for the shared
    #: tenant-level pool. Carried because the DEBIT path is licence-scoped
    #: (``cost_flusher`` passes the event's licence into ``debit_budget``) while
    #: the RESERVE path in front of it usually is not, so the two paths drain and
    #: enforce at different grains. Defaults to the pool sentinel for the same
    #: reason the column does: a row that names no licence IS the shared pool.
    license_id: uuid.UUID = NULL_LICENSE_ID


@dataclass
class UtilisationSummary:
    considered: int = 0
    #: PER-ROW counts. A single budget row standing over its own cap is NOT an
    #: overspend -- see the ``budget_debit`` cascade note in
    #: :func:`summarise_budget_utilisation`.
    over_limit: int = 0
    max_ratio: float = 0.0
    #: PER-TENANT aggregate: consumed vs ``sum(budget_tokens)``, which is the
    #: quantity ``reserve_budget`` actually enforces. ⭐ THIS IS THE ONLY ARM A
    #: READER MAY TREAT AS "is a tenant over its cap" -- it is the grain the
    #: GATE enforces.
    tenants_considered: int = 0
    tenants_over_limit: int = 0
    tenant_max_ratio: float = 0.0
    #: PER-LICENCE-SCOPE: consumed vs cap summed over the rows ONE debit call
    #: can drain -- that licence's own rows PLUS the shared tenant-level pool
    #: (``_select_active_budgets``' scope rule). ⛔ NOT ENFORCED BY ANYTHING.
    #: ``reserve_budget`` is reached from inference-router with no ``license_id``
    #: at all, so admission is decided on the tenant aggregate above; this arm
    #: measures where the DEBITS land afterwards. A value over 1.0 here means
    #: "one licence's share of a pool is oversubscribed", NEVER "the gate is
    #: failing" -- the gate never looked at this number.
    licences_considered: int = 0
    licences_over_limit: int = 0
    licence_max_ratio: float = 0.0


def _same_tenant(left: Any, right: Any) -> bool:
    """Compare two tenant identifiers without caring which type they arrived as.

    ``tenant_id`` is mid-migration from ``String`` to a UUID column across this
    repo (a sibling change set is in flight). A ``uuid.UUID`` and its own string
    form name the same tenant, so comparing them raw would flip every event in
    the estate to "mismatched" the moment the column type changes underneath
    this query -- a fleet-wide false alarm caused purely by a cast.
    """
    if left is None or right is None:
        return False
    return str(left).strip().lower() == str(right).strip().lower()


def classify_attribution(
    *,
    rows: Sequence[tuple[Any, ...]],
    owner_by_license: dict[str, Any],
    unattributed_events: int = 0,
    unattributed_spend_usd: float = 0.0,
    owner_source: str = OWNER_SOURCE_REGISTRY,
) -> AttributionAudit:
    """Adjudicate ``(license_id, billed_tenant, events, spend[, delegated])`` groups.

    ``delegated`` (optional, default False) is ``_WINDOW_SQL``'s router-vouched
    delegation flag. It only re-files a group that WOULD be a mismatch: a
    delegated group whose owner matches is still matched, and one whose licence
    cannot be resolved is still unresolved.

    A licence missing from ``owner_by_license`` is counted as **unresolved**, not
    as a mismatch. We cannot adjudicate what we cannot resolve, and inflating the
    mismatch signal with unknowns is the same defect as billing a NULL.

    ⚠️ Since the map became registry-sourced, ``unresolved`` means something
    STRONGER than it used to: "the licences registry does not hold this licence
    at all", which is the control row in this module's docstring table (measured
    0 on prod). It used to mean "billing's local mirror has not synced it".

    ``unattributed_*`` are carried through rather than derived: nothing in
    ``rows`` can express them (a row without a licence has no group to be in),
    so they arrive already counted from the complement query. Passing them here
    rather than assigning them at the call site keeps every rate on the
    dataclass computable from one object -- and testable without a database.

    ``owner_source`` defaults to :data:`OWNER_SOURCE_REGISTRY` because a caller
    that hands over a concrete dict is, by handing it over, asserting it HAS an
    ownership map -- which is what every hand-built test case and every live
    registry pass are doing. The unavailable path does not come through here at
    all; it cannot, because there is no map to adjudicate against.
    """
    audit = AttributionAudit(
        unattributed_events=int(unattributed_events),
        unattributed_spend_usd=float(unattributed_spend_usd or 0.0),
        owner_source=owner_source,
    )
    pairs: list[tuple[str, str, str, int]] = []
    delegated_pairs: list[tuple[str, str, str, int]] = []
    for row in rows:
        license_id, billed_tenant, events, spend = row[:4]
        delegated = bool(row[4]) if len(row) > 4 else False
        audit.total_events += events
        owner = owner_by_license.get(str(license_id))
        if owner is None:
            audit.unresolved_events += events
            continue
        if _same_tenant(owner, billed_tenant):
            audit.matched_events += events
            continue
        if delegated:
            audit.delegated_events += events
            audit.delegated_spend_usd += float(spend or 0.0)
            delegated_pairs.append((str(license_id), str(billed_tenant), str(owner), events))
            continue
        audit.mismatch_events += events
        audit.mismatch_spend_usd += float(spend or 0.0)
        pairs.append((str(license_id), str(billed_tenant), str(owner), events))
    audit.top_pairs = sorted(pairs, key=lambda p: p[3], reverse=True)[:5]
    audit.top_delegated_pairs = sorted(delegated_pairs, key=lambda p: p[3], reverse=True)[:5]
    return audit


#: The licences endpoint that states licence -> owning-tenant.
#:
#: ⭐ READ THIS BEFORE "REUSING ``fetch_licenses``" -- THAT FUNCTION IS THE WRONG
#: SHAPE AND THE ENDPOINT IS THE RIGHT ONE.
#: ``services.billing.fetch_licenses`` hits exactly this path, and its records DO
#: carry ``tenant_id`` (licences ``main.py``: ``InternalLicenseResponse`` declares
#: ``tenant_id: str`` as a REQUIRED field, built as ``str(lic.tenant_id)`` from the
#: registry's own uuid column). But it sends ``params={"tenant_id": ...,
#: "status": "active"}`` plus an ``X-Tenant-Id`` header, so it can only answer
#: "which licences does THIS tenant hold". The audit's question is the inverse --
#: "who owns licence L" -- for an L whose owner it has no guess for, which is the
#: whole point. Calling it once per billed tenant would resolve only licences whose
#: owner ALSO has spend in the same window, silently turning exactly the
#: cross-tenant mismatches being hunted into ``unresolved``; and ``status=active``
#: would drop a revoked licence that is still spending.
#:
#: Called with NO query params the same endpoint returns EVERY licence, EVERY
#: status, EVERY tenant, in one request. That is not an invented endpoint: two
#: production call sites already use it in precisely this unscoped form --
#: ``routers/budget.py`` (``sync_from_licenses``) and
#: ``services/licence_budget_reconcile.py`` (``_fetch_licence_lifecycle``, a
#: leader-elected timer, i.e. the same shape as this audit). The reconcile one
#: throws the ``tenant_id`` away because it only needs lifecycle; this reads it.
_LICENCE_OWNERS_PATH = "/internal/licenses"

#: ⛔ NO ``X-Tenant-Id`` HEADER ON THE UNSCOPED CALL. licences'
#: ``AuthMiddleware.dispatch`` runs ``validate_tenant_uuid(raw_tenant_id,
#: source="X-Tenant-Id")`` BEFORE it reads the Authorization header, so any
#: non-uuid value -- and there is no tenant to name here -- is a 400 before the
#: bearer is ever considered. ``routers/budget.py`` shipped with
#: ``X-Tenant-Id: "__admin__"`` for exactly this reason and every call it ever
#: made was a 400 folded into a 200 response body. Hence headers are built here
#: rather than via ``ServiceTokenProvider.build_headers``, which always sets it.
#:
#: ⭐ The bearer IS required: ``/internal/licenses`` is gated on licences'
#: ``require_internal_service`` (roles ``service-internal``,
#: ``onboarding-internal``, ``license-admin``) and billing's own SA token carries
#: ``service-internal`` -- measured 2026-09-11, see ``services/seats.py``, where an
#: ``/internal/`` call that simply never attached the header 401'd for weeks.
_LICENCE_FETCH_TIMEOUT_SECONDS = 10.0

#: Service-account token providers, keyed on (token endpoint, client id).
#: Mirrors ``services.seats._default_providers`` verbatim, for the same reason
#: and with the same key: this module runs on the rollup leader's tick and has no
#: ``app.state`` to take a provider from, and ``run_attribution_audit``'s only
#: caller (``services.aggregator``) passes it nothing. ⛔ The key EXCLUDES the
#: client secret -- dict keys end up in reprs and logs.
_default_providers: dict[tuple[str, str], ServiceTokenProvider] = {}


def _provider_for(settings: Settings) -> ServiceTokenProvider:
    key = (settings.keycloak_token_url or "", settings.service_client_id or "")
    provider = _default_providers.get(key)
    if provider is None:
        provider = ServiceTokenProvider(settings)
        _default_providers[key] = provider
    return provider


def clear_licence_owner_providers() -> None:
    """Drop the cached token providers -- for tests that swap Keycloak settings.

    Mirrors ``services.seats.clear_seat_cache``'s provider half. There is
    deliberately NO owners cache to clear: the map is re-fetched every tick, on
    purpose, because a cached map would let one good answer certify later passes
    that could not check -- which is the defect this whole module-level change
    exists to remove, arriving through a TTL instead of through a SQL table.
    """
    _default_providers.clear()


def parse_licence_owners(payload: Any) -> tuple[dict[str, str], int]:
    """``(licence -> owning tenant, unowned licence count)`` from the registry JSON.

    Pure, so every refusal below is testable without a socket.

    :raises LicenceOwnersUnavailableError: on any payload this module cannot read
        as a complete licence list.

    ⛔ AN EMPTY LIST IS REFUSED, NOT HONOURED, and that is the single most
    important line here. A 200 carrying ``[]`` is what a half-migrated database, a
    mis-scoped query and a tenant-filtered response all look like -- and honouring
    it would make every licence-bearing event ``unresolved`` and
    ``mismatch_events`` exactly **0**, i.e. it would reproduce the fabricated zero
    this change removes, through the new code path. An estate with zero licences
    is a state this system has never been in. Precedent:
    ``licence_budget_reconcile._fetch_licence_lifecycle`` refuses the same shape
    for the same reason, and ``delete_absent_quota_configs`` before it.

    ⚠️ A dict is refused for a reason that is NOT pedantry: ``for lic in {...}``
    iterates KEYS, so a silently-enveloped/paginated response would yield zero
    parseable licences and read as a healthy-but-empty registry.

    A licence with no usable ``tenant_id`` is COUNTED and EXCLUDED rather than
    raised on. ``InternalLicenseResponse.tenant_id`` is a required ``str``
    upstream, so this cannot happen from a healthy registry; excluding it makes
    that licence's spend read ``unresolved``, which is the honest verdict, while
    the count surfaces the registry-side defect on the audit log line. Raising
    would let one bad upstream row blind the whole estate's coherence reading --
    the blast-radius rule ``routers/budget.py`` already applies to this payload.
    """
    if not isinstance(payload, list):
        raise LicenceOwnersUnavailableError("licences returned a non-list payload")
    if not payload:
        raise LicenceOwnersUnavailableError("licences returned zero licences")

    owners: dict[str, str] = {}
    unowned = 0
    for lic in payload:
        if not isinstance(lic, dict):
            raise LicenceOwnersUnavailableError("licences returned a non-object licence")
        raw_id = lic.get("id")
        if not raw_id:
            # Cannot be keyed, so it can never be matched to an event's licence.
            # Refuse the whole tick: this might be the very licence carrying the
            # mismatch, and omitting it would publish a LOWER mismatch count --
            # an under-report that looks like an improvement.
            raise LicenceOwnersUnavailableError("licences returned a licence with no id")
        raw_tenant = lic.get("tenant_id")
        tenant = str(raw_tenant).strip() if raw_tenant is not None else ""
        # "none"/"null" as TEXT, not just a falsy value: the upstream response
        # builds this as `str(lic.tenant_id)`, so a NULL column arrives as the
        # four-character string "None" -- which is truthy, and which would become
        # an owning "tenant" that no event can ever match, turning every one of
        # that licence's events into a MISMATCH. A fabricated mismatch is as
        # wrong as a fabricated match.
        if not tenant or tenant.lower() in {"none", "null"}:
            unowned += 1
            continue
        # Keys normalised (`_licence_key`), values NOT. `classify_attribution`
        # looks up `str(license_id)` from `e.license_id::text`, which Postgres
        # renders as a lowercase canonical uuid, so a registry that ever answered
        # in upper case would resolve NOTHING and publish 0 mismatches. Tenant
        # VALUES are compared through `_same_tenant`, which normalises both sides
        # itself, so normalising them here would only hide the original form from
        # the log line.
        owners[_licence_key(raw_id)] = tenant
    return owners, unowned


async def fetch_licence_owners(
    settings: Settings,
    *,
    token_provider: ServiceTokenProvider | None = None,
    client: httpx.AsyncClient | None = None,
) -> tuple[dict[str, str], int]:
    """THE ONLY PLACE THIS MODULE TALKS TO LICENCES. Never returns a guess.

    :raises LicenceOwnersUnavailableError: on an unconfigured ``licenses_url``, any
        transport error, any non-200 status, unparseable JSON, or any payload
        :func:`parse_licence_owners` refuses.

    ⚠️ A status code is NOT an ``httpx`` exception -- ``httpx`` does not raise on
    4xx/5xx. ``fetch_licenses`` shipped without a status check and fell through to
    ``return []``, making an expired S2S token indistinguishable from a
    licence-free tenant; that is the same bug in the same call, so it is checked
    explicitly here.

    The unconfigured-URL guard is first and is not defensive padding:
    ``Settings.licenses_url`` defaults to ``""``, so without it a deployment that
    forgot the variable would attempt ``GET "/internal/licenses"`` and surface as
    an obscure protocol error instead of "you did not configure licences".
    """
    base = (settings.licenses_url or "").strip().rstrip("/")
    if not base:
        raise LicenceOwnersUnavailableError("licenses_url is not configured")

    provider = token_provider or _provider_for(settings)
    token = await provider.get_token()
    headers: dict[str, str] = {}
    if token:
        headers["Authorization"] = f"Bearer {token}"
    # NOT fail-closed on a missing token, unlike services.seats: that function is
    # consulted by a close and must refuse rather than send an unauthenticated
    # request. Here the request is attempted and the 401 is reported as the
    # unavailability it is -- the distinction costs one round trip and keeps a
    # local/dev licences instance with auth disabled usable.
    if not token:
        logger.warning("licence_owners_no_service_token")

    url = f"{base}{_LICENCE_OWNERS_PATH}"
    try:
        if client is not None:
            resp = await client.get(url, headers=headers)
        else:
            async with httpx.AsyncClient(timeout=_LICENCE_FETCH_TIMEOUT_SECONDS) as c:
                resp = await c.get(url, headers=headers)
    except httpx.HTTPError as exc:
        raise LicenceOwnersUnavailableError(f"licences unreachable: {exc!r}") from exc

    if resp.status_code != 200:
        raise LicenceOwnersUnavailableError(f"licences returned HTTP {resp.status_code}")

    try:
        payload = resp.json()
    except ValueError as exc:
        raise LicenceOwnersUnavailableError("licences returned unparseable JSON") from exc

    return parse_licence_owners(payload)


def compare_local_owner_proxy(
    registry_owners: dict[str, str], local_owners: dict[str, Any]
) -> tuple[int, int]:
    """``(disagreements, licences the local table has and the registry does not)``.

    ⭐ THIS IS WHAT ``request_quota_config`` IS FOR NOW. It was the audit's
    authority and it was self-derived; it is now the audit's SUBJECT. Reporting
    the size of the disagreement is the only use of that table that does not
    re-create the circularity -- and it is the measurement that would have caught
    the original defect, because a proxy that disagrees with the registry on N
    licences cannot also be the thing that validates it.

    ⛔ NOT a fallback and NOT a tiebreak. Nothing here feeds
    :func:`classify_attribution`.
    """
    disagreements = 0
    local_only = 0
    for licence, local_tenant in local_owners.items():
        key = _licence_key(licence)
        registry_tenant = registry_owners.get(key)
        if registry_tenant is None:
            local_only += 1
        elif not _same_tenant(registry_tenant, local_tenant):
            disagreements += 1
    return disagreements, local_only


#: ``TokenBudget.license_id`` for the shared tenant-level pool -- the scope
#: every licence of a tenant draws on in ADDITION to its own rows, and the scope
#: licence-less system/S2S spend draws on exclusively.
_TENANT_POOL_LICENCE = str(NULL_LICENSE_ID).lower()


def _licence_key(value: Any) -> str:
    """Normalise a licence identifier to a comparable string.

    Same rationale as :func:`_same_tenant`: ``license_id`` reaches this module as
    a ``uuid.UUID`` from the DB row but as a string from callers and tests, and a
    ``UUID``/``str`` mismatch against :data:`_TENANT_POOL_LICENCE` would silently
    reclassify the shared pool as a licence of its own -- splitting the grain the
    debit cascade actually uses.
    """
    return str(value).strip().lower()


def _summarise_scopes(scopes: Iterable[Sequence[int]]) -> tuple[int, int, float]:
    """Count ``(cap, consumed)`` buckets, returning (considered, over, max ratio).

    Shared by the tenant and licence arms so the two can never drift in how they
    treat a zero cap or an exhausted-but-not-exceeded scope. A ``cap`` of 0 is
    BLOCK-ALL, not unlimited (``budget_debit`` treats ``<= 0`` as exhausted), so
    any consumption against it is over-limit at a saturated ratio.
    """
    considered = 0
    over = 0
    max_ratio = 0.0
    for cap_total, consumed_total in scopes:
        considered += 1
        if cap_total <= 0:
            if consumed_total > 0:
                over += 1
                max_ratio = max(max_ratio, 1.0)
            continue
        ratio = consumed_total / cap_total
        max_ratio = max(max_ratio, ratio)
        if consumed_total > cap_total:
            over += 1
    return considered, over, max_ratio


def summarise_budget_utilisation(
    budgets: list[BudgetUtilisation],
    ts: datetime,  # noqa: ARG001 -- staleness is resolved by the caller, see below
) -> UtilisationSummary:
    """Summarise active budget consumption at all THREE grains, which differ.

    Staleness is decided by the caller via ``services.budget_period`` -- the
    accessor ``models/db/budgets.py`` names as mandatory -- and arrives here as
    ``BudgetUtilisation.stale``. Reading ``consumed_tokens`` raw would report a
    budget whose month has rolled as permanently over-limit and alert forever.

    ``budget_tokens == 0`` is BLOCK-ALL, not unlimited (``budget_debit`` treats
    ``<= 0`` as exhausted), so any consumption against it is over-limit and the
    ratio is reported as saturated rather than dividing by zero.

    ⛔ **The two grains are not interchangeable, and reading the per-row one as a
    tenant fact is a measured false alarm.** ``budget_debit`` walks a tenant's
    active budgets in priority order taking ``min(room, wants)`` and dumps the
    remainder on the LAST row it visits, so any filled non-last row sits at
    EXACTLY its own cap by design. Measured 2026-09-10: the per-row max read
    **4.77** while the two tenants involved were at **57.2%** and **22.5%** of
    their aggregate caps -- and ``reserve_budget`` enforces
    ``consumed + reserved + estimated <= sum(budget_tokens)``, i.e. the
    aggregate. The per-row arm therefore measures the cascade's shape; only the
    ``tenant_*`` arm answers "is this tenant over its cap".

    Both are returned rather than one replacing the other: the per-row arm still
    detects a single budget row mis-seeded above its own cap, which the aggregate
    hides whenever the tenant has headroom elsewhere.

    ⭐ **THE THIRD ARM: the DEBIT grain, which is ALSO real and is ALSO not the
    enforced one.** ``cost_flusher`` debits with the event's ``license_id``, so
    ``_select_active_budgets`` narrows the cascade to that licence's own rows PLUS
    the shared tenant-level pool. ``reserve_budget`` -- the only thing that can
    REFUSE a request -- is called from inference-router with no ``license_id`` at
    all, so admission is decided on the whole tenant pool. Net effect:

      * ``tenant_*``   -> what the GATE enforces. Over 1.0 here = over cap.
      * ``licence_*``  -> what the DEBITS drain. Over 1.0 here = one licence's
        share of the pool is oversubscribed while the tenant may still be far
        under cap. Measured 2026-09-11, each licence with ITS OWN tenant's
        enforced aggregate: 205% (tenant at 14.7%), 194% (24.8%), 145% (60.3%) --
        the two series do not sort alike, so read them as pairs. ⛔ A reader who sees 205%
        here has NOT seen a failing gate; the gate never evaluated this number.
      * ``max_ratio``  -> the cascade's shape, per ROW. See above.

    Three arms, three questions, none substitutable for another.
    """
    summary = UtilisationSummary()
    by_tenant: dict[str, list[int]] = {}
    # Debit-grain accumulators, kept apart because a licence's scope is NOT its
    # own rows: the shared pool belongs to EVERY licence of that tenant, so it is
    # summed once per tenant and folded into each licence scope below.
    pool_by_tenant: dict[str, list[int]] = {}
    by_licence: dict[tuple[str, str], list[int]] = {}
    for b in budgets:
        summary.considered += 1
        consumed = 0 if b.stale else max(0, int(b.consumed_tokens or 0))
        cap = max(0, int(b.budget_tokens or 0))
        tenant = str(b.tenant_id)

        # Aggregate arm: accumulate at the grain reserve_budget enforces. A
        # stale row contributes its CAP but no consumption -- dropping the cap
        # too would shrink the denominator and inflate the tenant ratio.
        totals = by_tenant.setdefault(tenant, [0, 0])
        totals[0] += cap
        totals[1] += consumed

        # Debit arm: same stale rule, same reason.
        licence = _licence_key(b.license_id)
        if licence == _TENANT_POOL_LICENCE:
            scope_totals = pool_by_tenant.setdefault(tenant, [0, 0])
        else:
            scope_totals = by_licence.setdefault((tenant, licence), [0, 0])
        scope_totals[0] += cap
        scope_totals[1] += consumed

        if b.budget_tokens <= 0:
            if consumed > 0:
                summary.over_limit += 1
                summary.max_ratio = max(summary.max_ratio, 1.0)
            continue
        ratio = consumed / b.budget_tokens
        summary.max_ratio = max(summary.max_ratio, ratio)
        if consumed > b.budget_tokens:
            summary.over_limit += 1

    (
        summary.tenants_considered,
        summary.tenants_over_limit,
        summary.tenant_max_ratio,
    ) = _summarise_scopes(by_tenant.values())

    # One scope per licence (own rows + its tenant's pool), plus the pool ITSELF
    # as a scope: licence-less system/S2S spend is debited with
    # NULL_LICENSE_ID and reaches the tenant-level pool ONLY, never a
    # licence-attributed row (budget_debit's third scope bullet). Omitting it
    # would leave that spend measured at no grain but the per-row one.
    scopes: list[list[int]] = [list(pool) for pool in pool_by_tenant.values()]
    for (tenant, _licence), own in by_licence.items():
        pool_cap, pool_consumed = pool_by_tenant.get(tenant, [0, 0])
        scopes.append([own[0] + pool_cap, own[1] + pool_consumed])
    (
        summary.licences_considered,
        summary.licences_over_limit,
        summary.licence_max_ratio,
    ) = _summarise_scopes(scopes)
    return summary


_WINDOW_SQL = text(
    """
    SELECT e.license_id::text AS license_id,
           -- The tenant that USED the licence. A row re-tenanted by a
           -- billing_attribution_rules rule (alembic 0041) is BILLED to another
           -- tenant but was still spent on the source tenant's licence;
           -- adjudicating ownership against the bill tenant would score every
           -- attributed row as a mismatch.
           coalesce((e.metadata->>'attributed_from_tenant_id')::uuid,
                    e.tenant_id)::text AS tenant_id,
           count(*)           AS events,
           coalesce(sum(e.cost_usd), 0)::float8 AS spend,
           -- Router-vouched delegation: inference-router stamps delegated_by
           -- ONLY when it honoured a delegated call (subject allowlisted in
           -- AUTH_DELEGATING_SUBJECTS), so licence and tenant differ on
           -- purpose. ONLY the router may vouch: the key is a whitelisted wire
           -- field any emitter can send, so from any other cost_source it is
           -- ignored and the group stays a mismatch. Exact '=' on purpose -- a
           -- '+resolved' / refusal-marked cost_source means billing, not the
           -- router, decided the licence.
           (coalesce(e.metadata->>'delegated_by', '') <> ''
            AND e.cost_source = 'inference-router') AS delegated
    FROM billing_cost_events e
    WHERE e.created_at >= :floor
      -- KEPT, and it is not the blindness: this query GROUPs BY license_id to
      -- adjudicate an ownership claim, and a NULL licence makes no claim -- it
      -- would collapse into one giant NULL group and be reported as
      -- `unresolved`, i.e. "the licence sync is stale", which it is not. The
      -- licence-less population is measured by _UNATTRIBUTED_SQL below, over
      -- the same window and the same billable filter, so the two together
      -- cover every billable row exactly once.
      AND e.license_id IS NOT NULL
      -- Parked duplicates are excluded from billing; counting them here would
      -- audit attribution against spend the invoice does not contain.
      AND e.excluded_from_billing = false
    GROUP BY 1, 2, 5
    """
)

#: The complement of _WINDOW_SQL: same window, same billable filter, inverted
#: licence predicate. Measured on prod 2026-08-27 over billable rows only,
#: 3,751 of 4,993 events in 24h landed here -- a population every gauge on the
#: coherence dashboard was computed WITHOUT, because the audit could only see
#: rows that already had the thing it was auditing.
#:
#: A second statement rather than a FULL OUTER JOIN or a FILTER aggregate: this
#: runs once per rollup tick, not once per event, so the cost of an extra scan
#: is irrelevant next to the cost of a reader mis-reading a fused query. It also
#: keeps the billable filter textually identical in both halves, which is the
#: property the parked-row test pins.
_UNATTRIBUTED_SQL = text(
    """
    SELECT count(*)                            AS events,
           coalesce(sum(e.cost_usd), 0)::float8 AS spend
    FROM billing_cost_events e
    WHERE e.created_at >= :floor
      AND e.license_id IS NULL
      AND e.excluded_from_billing = false
    """
)

# ⛔ DEMOTED 2026-09-11 FROM AUTHORITY TO SUBJECT. DO NOT FEED THIS TO
# `classify_attribution`. It is read ONLY so `compare_local_owner_proxy` can
# report how far it disagrees with the registry; ownership verdicts come from
# `fetch_licence_owners`.
#
# Why it can never be the authority: `request_quota_config.tenant_id` is
# CALLER-ASSERTED (routers/quota.py `set_quota_config` -> `require_internal` role
# check only -> body's tenant_id -> services/quota.py `on_conflict_do_update`,
# which sets tenant_id UNCONDITIONALLY while COALESCE-protecting its siblings) and
# the table is refreshed from the same event stream the audit audits. So it agrees
# with whatever tenant the event carried, and reported 0 mismatches on a 24h window
# the registry scored at 188 events / $0.1242 across 4 license_ids -- one of which
# it holds under TWO different tenants at once. See the module docstring.
#
# ⚠️ The old comment's "no cross-database join is needed or possible -- billing has
# no DSN for the licences registry, only an HTTP client" was TRUE and was read as
# "therefore the local table is the best available source". The HTTP client IS the
# source; the absent DSN only rules out a JOIN.
_OWNERS_SQL = text("SELECT license_id::text, tenant_id::text FROM request_quota_config")

#: Same window, same billable filter as the two statements above; the question
#: is different. Those adjudicate a LICENCE; this asks whether the TENANT the
#: spend is booked to exists in billing's records at all.
#:
#: ⚠️ ``request_quota_config`` UNION ``token_budgets`` is billing's LOCAL notion
#: of a known tenant, and it is a PROXY for the real registry, not the registry.
#: Billing cannot see ``licenses.tenants``: there is no DSN for it, and the
#: service account gets **HTTP 403 on GET /tenants** (measured in the pod
#: 2026-08-28; ``/internal/licenses`` returns 200, so the token is fine and the
#: role is not). Measured against the truth on the same day: 61 distinct tenants
#: in ``billing_cost_events``, 26 genuinely absent from ``licenses.tenants``,
#: and this proxy flags 28 -- 3 false positives (a real tenant that happens to
#: hold neither a quota nor a budget row) and 1 miss (billing holds a row for a
#: tenant the licences registry does not have, which is its own finding).
#:
#: 96% recall / 89% precision is fine for a gauge that REFUSES NOTHING, and the
#: metric is named for what it measures rather than what one wishes it measured.
_DERIVED_SPEND_SQL = text(
    """
    SELECT coalesce(sum(e.cost_usd), 0)::float8 AS attributed_usd,
           coalesce(sum(
               CASE WHEN e.cost_source LIKE '%+resolved' THEN e.cost_usd ELSE 0 END
           ), 0)::float8 AS derived_usd
    FROM billing_cost_events e
    WHERE e.created_at >= :floor
      AND e.excluded_from_billing = false
      AND e.license_id IS NOT NULL
    """
)
# ⚠️ `license_id IS NOT NULL` scopes this to the ATTRIBUTED population on purpose.
# The question is "of the spend we DID attribute, how much rests on an inference",
# so unattributed rows belong to a different denominator (billing_cost_daily's
# attribution_status) and mixing them in would make the share fall whenever
# attribution got WORSE -- a number that improves as the estate degrades.
#
# `LIKE '%+resolved'` rather than a parsed suffix: cost_flusher.py appends it to
# whatever cost_source arrived, so the producer name varies and only the suffix is
# stable. ⛔ Do NOT tighten to '= ...+resolved' for a specific producer.


# ⛔ DELIBERATELY **NOT** CONVERTED TO THE REGISTRY SOURCE, 2026-09-11. Its
# circularity is real -- `request_quota_config.tenant_id` is caller-asserted, same
# as for `_OWNERS_SQL` above -- but the registry data that fixed the OWNERS map
# cannot answer THIS question, so converting it would be a rename, not a repair.
#
# `GET /internal/licenses` yields the set of tenants that HOLD A LICENCE. That is
# not the tenant registry: a real tenant with no licence is absent from it, so
# substituting it trades a proxy whose error is MEASURED (89% precision / 96%
# recall against `licenses.tenants`, 2026-08-28; the full figures are in the
# comment block above) for one whose error is unmeasured and which systematically
# flags every licence-less-but-real tenant. The authoritative source is
# `GET /tenants`, and billing's service account gets **403** there (measured in the
# pod 2026-08-28, while `/internal/licenses` returns 200 -- so the token is fine and
# the ROLE is not). ⇒ THIS NEEDS A LICENCES-SIDE GRANT, NOT A BILLING-SIDE CHANGE.
#
# ⛔ Do not "improve" it by unioning the licence-holder set in. That changes what
# the published metric means without changing what it can know, and it is exactly
# the half-conversion that leaves a reader unable to say which definition a number
# on the panel was computed under.
_UNREGISTERED_TENANT_SQL = text(
    """
    SELECT e.tenant_id::text AS tenant_id,
           count(*)          AS events,
           coalesce(sum(e.cost_usd), 0)::float8 AS spend
    FROM billing_cost_events e
    WHERE e.created_at >= :floor
      -- ⛔ NOT the plain billable filter the neighbouring statements use, and
      -- the difference is the whole reason this gauge survives arming
      -- `billing_exclude_unregistered_tenants` (#241). Armed, the flusher
      -- parks exactly this population: excluded_from_billing = true,
      -- exclusion_reason = 'unregistered_tenant'. A plain
      -- `excluded_from_billing = false` would filter out the rows this query
      -- exists to find, and `license_tenant_unregistered_events` / `_spend_usd`
      -- would fall to ZERO the day the gate was armed -- reading as "registry
      -- gap fixed" when the rows were merely hidden.
      --
      -- Rows parked for any OTHER reason (0013's router duplicates, health
      -- probes) stay excluded: absent for an unrelated cause, and counting them
      -- would inflate the gap with spend that is not a registry problem.
      AND (
            e.excluded_from_billing = false
            OR e.exclusion_reason = 'unregistered_tenant'
      )
      AND NOT EXISTS (
            SELECT 1 FROM request_quota_config q WHERE q.tenant_id = e.tenant_id
      )
      AND NOT EXISTS (
            SELECT 1 FROM token_budgets b WHERE b.tenant_id = e.tenant_id
      )
    GROUP BY 1
    ORDER BY 2 DESC
    """
)

_BUDGETS_SQL = text(
    """
    SELECT id, tenant_id, license_id, budget_tokens, consumed_tokens, period,
           consumed_period_start, starts_at
    FROM token_budgets
    WHERE active
    """
)

# ⭐ THE TWO PROJECT-AXIS DETECTORS BELOW ARE PERIODIC DB READS ON PURPOSE, AND
# THAT IS THE ONLY CORRECT SHAPE FOR THEM.
#
# The forgery question ("is one project_id arriving under two tenants?") looks
# like a hot-path counter, and both hot-path implementations are wrong:
#
#   * an IN-PROCESS "which tenant asserted this project first" dict is
#     PER-REPLICA and RESETS ON RESTART. Two replicas each see roughly half the
#     traffic, so the same cross-tenant reuse is invisible to both, and the
#     under-count has no measurable size. It would be an instrument whose zero
#     means nothing.
#   * a PER-EVENT lookup answers correctly and puts a query on the ingest path,
#     which takes ~283k rows/month.
#
# Reading the stored rows here sees EVERY replica's writes, on a read-only
# leader tick that is already out of band from the cost flush. The measurement
# is correct by construction and costs the hot path nothing. The price is that
# both metrics are LEVELS over this window, not monotonic totals -- their help
# text says so, because `rate()` on them would be nonsense.

#: Distinct project ids seen under MORE THAN ONE tenant in the window, ONE ROW
#: PER PROJECT, carrying the distinct tenant ids that shared it.
#:
#: ⚠️ DELIBERATELY WITHOUT the `excluded_from_billing = false` filter every
#: money query above carries. Those audit SPEND, so they must see only what an
#: invoice can contain. This audits CALLER BEHAVIOUR: a project id stamped by a
#: tenant that does not own it is the same evidence whether or not the row it
#: rode in on was later parked as a duplicate, and filtering would let a forger
#: hide behind the dedupe.
#:
#: ⭐ R16: was `SELECT count(*) AS projects` over the same subquery -- a single
#: number that could say THAT a project crossed a tenant boundary but not WHICH
#: tenants, so nothing downstream could tell an internal-only crossing from one
#: touching a paying customer (D1, billing-licensing estate repair round 16).
#: `array_agg(DISTINCT e.tenant_id)` carries the set the caller needs to classify
#: each project's `scope` without a second scan or a second round trip.
_PROJECT_MULTI_TENANT_SQL = text(
    """
    SELECT e.project_id,
           array_agg(DISTINCT coalesce((e.metadata->>'attributed_from_tenant_id')::uuid,
                                       e.tenant_id)) AS tenant_ids
    FROM billing_cost_events e
    WHERE e.project_id IS NOT NULL
      AND e.created_at >= :floor
    GROUP BY e.project_id
    -- Caller behaviour is judged on the tenant that ASSERTED the project, not
    -- the one a billing_attribution_rules rule (0041) billed it to.
    HAVING count(DISTINCT coalesce((e.metadata->>'attributed_from_tenant_id')::uuid,
                                   e.tenant_id)) > 1
    """
)

#: Rollup rows that carry a real project versus the NULL_PROJECT_ID sentinel --
#: the coverage denominator behind `billing_cost_daily_project_attribution_rows`.
#:
#: ⚠️ ONE STATEMENT WITH TWO `FILTER`s, unlike _WINDOW_SQL / _UNATTRIBUTED_SQL,
#: which were deliberately split. Those two halves carry DIFFERENT predicates
#: and the split keeps the shared billable filter textually identical in both.
#: These two halves are exact complements of ONE predicate over a NOT NULL
#: column, so they partition the window by construction -- and because they are
#: published as a ratio, two separate scans could straddle a rollup upsert and
#: yield a numerator and denominator from different states of the table. One
#: scan cannot.
#:
#: ⚠️ `:day_floor` IS THE SAME WINDOW, not a second one. `billing_cost_daily`
#: is keyed by a DATE, which cannot express "24 hours ago"; the bind is
#: DEFAULT_WINDOW_HOURS' own floor truncated to its day in UTC, in Python, so
#: the coarsening is visible here rather than hidden in a server-timezone cast.
_PROJECT_ATTRIBUTION_SQL = text(
    """
    SELECT count(*) FILTER (WHERE d.project_id =  :null_project_id) AS sentinel_rows,
           count(*) FILTER (WHERE d.project_id <> :null_project_id) AS project_rows
    FROM billing_cost_daily d
    WHERE d.bucket_date >= :day_floor
    """
)

#: Projects, rows and DOLLARS in the window whose `project_id` is byte-identical
#: to the `tenant_id` on the same row -- a caller stamping its own tenant uuid on
#: the project axis instead of a real project (or instead of leaving it NULL).
#:
#: ⭐ NOTHING ELSE IN THE ESTATE DETECTS THIS CLASS, and both existing project
#: gauges report it as HEALTH. `_PROJECT_MULTI_TENANT_SQL` looks for ONE project
#: under TWO tenants; a tenant asserting its own id is one project under exactly
#: one tenant, so that count stays 0. `_PROJECT_ATTRIBUTION_SQL` counts a
#: non-sentinel uuid as ATTRIBUTED, so the forged row counts as COVERAGE. A
#: forged id of this shape is simultaneously invisible to both.
#: Measured on prod 2026-09-16 over 24h: 62 of 414 attributed rows, $6.0458 of
#: $9.7622 -- 15.0% of the rows, 61.9% of the money.
#:
#: ⚠️ THREE NUMBERS, NOT ONE, and none of them substitutes for another. Rows
#: alone reported a 15% nuisance; dollars alone reported a 62% emergency; and
#: one forged id repeated 62 times (one emitter with one bug) versus 62 distinct
#: forged ids (a whole population doing it) are the same row count and different
#: defects. `count(DISTINCT e.project_id)` is what separates the last pair.
#:
#: ⚠️ DELIBERATELY WITHOUT the `excluded_from_billing = false` filter every money
#: query above carries -- the same judgement, for the same reason, as
#: `_PROJECT_MULTI_TENANT_SQL` states at length. Those audit SPEND, so they must
#: see only what an invoice can contain. This audits CALLER BEHAVIOUR: a project
#: id a tenant forged from its own uuid is the same evidence whether or not the
#: row it rode in on was later parked as a duplicate, and filtering would let a
#: forger hide behind the dedupe. The consequence is on the gauge's help text:
#: `..._spend_usd` is "dollars that ARRIVED carrying a forged project id", NOT
#: "dollars an invoice will contain", and it may legitimately exceed an invoice
#: total. ⛔ Do not reconcile it against one, and do not "fix" it by adding the
#: filter -- that would make a forger invisible by parking their own rows.
#:
#: One statement, because all three numbers describe the SAME population and a
#: second scan could see a different one after a flush.
_PROJECT_SELF_TENANT_SQL = text(
    """
    SELECT count(DISTINCT e.project_id)      AS projects,
           count(*)                          AS events,
           coalesce(sum(e.cost_usd), 0)::float8 AS spend
    FROM billing_cost_events e
    WHERE e.project_id IS NOT NULL
      AND e.project_id = e.tenant_id
      AND e.created_at >= :floor
    """
)

#: Billable SPEND in the window split three ways on the project axis -- the
#: DOLLAR-denominated sibling of `_PROJECT_ATTRIBUTION_SQL`, which counts ROWS.
#:
#: ⭐ WHY A SECOND COVERAGE QUERY AT ALL: a row share is not a spend share. This
#: estate has already seen 80% of rows be 0.5% of the bill, and the project axis
#: has already seen the reverse (15.0% of rows, 61.9% of the dollars, prod
#: 2026-09-16). There was no dollar-denominated project-coverage number anywhere
#: before this; every "project coverage is N%" statement was a row count wearing
#: a money question, and the row gauge had to be re-read today to stop one such
#: statement becoming a false finding. The unit is named in the help text for
#: that reason.
#:
#: ⚠️ OVER `billing_cost_events`, NOT `billing_cost_daily`, unlike its sibling.
#: The rollup's project column is NOT NULL and carries the sentinel, so it can
#: only ever express two states; the dollars and the genuinely NULLABLE
#: `project_id` both live here, and so does the third state below.
#:
#: ⚠️ ONE STATEMENT WITH THREE `FILTER`s, for exactly the reason its sibling
#: gives for two: the three predicates are exact complements of each other over
#: one scan, so they partition the window BY CONSTRUCTION -- their sum is the
#: window's billable total, which is the integrity check a reader can run. Three
#: separate scans could each see a different state of the table across a cost
#: flush and yield three buckets that sum to nothing in particular. One scan
#: cannot straddle a flush.
#:
#: The three buckets:
#:   * `project`       -- `project_id IS NOT NULL`: the caller named a project.
#:   * `tenant_scoped` -- no project AND `metadata->>'spend_scope' = 'tenant'`:
#:     the caller EXPLICITLY said this spend belongs to the whole tenant. That is
#:     a complete attribution, not a gap, and keeping it out of `unattributed` is
#:     the entire point of having three buckets rather than two.
#:     ⚠️ This header is being added by another change set this round, so this
#:     bucket LEGITIMATELY READS $0 until it lands. A zero here is a measurement.
#:   * `unattributed`  -- everything else: no project and nothing said about it.
#:     The only bucket that is actually a coverage gap. Written as `IS DISTINCT
#:     FROM 'tenant'` rather than `<> 'tenant'` on purpose: `->>` yields NULL for
#:     a NULL metadata column AND for a present column missing the key, and `<>`
#:     would drop both of those rows out of all three buckets and silently break
#:     the partition -- which is the exact failure mode the sum identity exists
#:     to catch.
#:
#: ⚠️ BILLABLE ONLY, unlike `_PROJECT_SELF_TENANT_SQL` directly above. That one
#: audits caller behaviour; this one audits MONEY, so it takes the same
#: `excluded_from_billing = false` filter as `_WINDOW_SQL` -- a parked
#: duplicate counted here would put the same dollar in the coverage denominator
#: twice. (`_UNREGISTERED_TENANT_SQL` additionally admits rows parked as
#: 'unregistered_tenant' -- see its comment, #241.)
_PROJECT_SPEND_COVERAGE_SQL = text(
    """
    SELECT coalesce(sum(e.cost_usd) FILTER (
               WHERE e.project_id IS NOT NULL
           ), 0)::float8 AS project_usd,
           coalesce(sum(e.cost_usd) FILTER (
               WHERE e.project_id IS NULL
                 AND e.metadata->>'spend_scope' = 'tenant'
           ), 0)::float8 AS tenant_scoped_usd,
           coalesce(sum(e.cost_usd) FILTER (
               WHERE e.project_id IS NULL
                 AND e.metadata->>'spend_scope' IS DISTINCT FROM 'tenant'
           ), 0)::float8 AS unattributed_usd
    FROM billing_cost_events e
    WHERE e.created_at >= :floor
      AND e.excluded_from_billing = false
    """
)


#: Per-emitter attribution COVERAGE: for each service_key in the window, how
#: many billable events there were and how many carried a project / an actor.
#: Feeds `billing_attribution_coverage_events` + `billing_attribution_covered_
#: events` (see their banner in observability/metrics.py).
#:
#: ⚠️ ROLLING `:floor` (now - window_hours), NOT `day_floor`. The rollup-row
#: gauge keys on a UTC calendar day and so resets its population at midnight;
#: a coverage ratio built on that shows a step change no emitter caused.
#:
#: ⚠️ `project_id IS NOT NULL` ONLY -- tenant_scoped rows (project_id NULL,
#: spend_scope='tenant') are complete attribution but NOT project coverage.
#:
#: Same billable filter and same `created_at` lower bound as
#: `_PROJECT_SPEND_COVERAGE_SQL`, so the planner takes the same path: runtime
#: partition pruning on the monthly RANGE partitions (created_at is the
#: partition key) plus the `created_at` lower bound. One statement so numerator
#: and denominator describe one snapshot.
_EMITTER_COVERAGE_SQL = text(
    """
    SELECT e.service_key                                    AS service_key,
           count(*)                                         AS events,
           count(*) FILTER (WHERE e.project_id IS NOT NULL) AS project_events,
           count(*) FILTER (WHERE e.actor_id IS NOT NULL)   AS actor_events
    FROM billing_cost_events e
    WHERE e.created_at >= :floor
      AND e.excluded_from_billing = false
    GROUP BY e.service_key
    """
)


async def load_emitter_coverage(
    session: AsyncSession, window_hours: int
) -> list[tuple[str, int, int, int]]:
    """``(service_key, events, project_events, actor_events)`` over the window."""
    floor = datetime.now(UTC) - timedelta(hours=window_hours)
    return [
        (
            str(r.service_key),
            int(r.events or 0),
            int(r.project_events or 0),
            int(r.actor_events or 0),
        )
        for r in (await session.execute(_EMITTER_COVERAGE_SQL, {"floor": floor})).all()
    ]


def publish_emitter_coverage(rows: Iterable[tuple[str, int, int, int]]) -> None:
    """Write the per-emitter coverage gauges and REMOVE vanished label sets.

    A labelled Gauge never forgets a child, so a service_key that stopped
    emitting would otherwise export its last ratio forever. Removal is by diff
    (not ``.clear()`` then re-set) so a scrape landing mid-publish never sees
    the whole family momentarily absent. An empty window removes every series:
    there is no emitter to have a ratio, and 0/0 is not a measurement.
    """
    events_gauge = observability_metrics.attribution_coverage_events
    covered_gauge = observability_metrics.attribution_covered_events
    live: set[tuple[str, str]] = set()
    for service_key, events, project_events, actor_events in rows:
        for axis, covered in (("project", project_events), ("actor", actor_events)):
            events_gauge.labels(service_key=service_key, axis=axis).set(events)
            covered_gauge.labels(service_key=service_key, axis=axis).set(covered)
            live.add((service_key, axis))
    for gauge in (events_gauge, covered_gauge):
        published = {
            (s.labels["service_key"], s.labels["axis"])
            for metric in gauge.collect()
            for s in metric.samples
        }
        for service_key, axis in published - live:
            gauge.remove(service_key, axis)


def classify_multi_tenant_scope(
    project_tenants: Iterable[Iterable[str]],
    kind_by_tenant: Mapping[str, str],
) -> dict[str, int]:
    """Split the multi-tenant projects `_load` found into `{scope: count}`.

    Pure, no I/O -- like every other adjudication function in this module, so a
    mutant here is catchable without a database or a licences mock.

    Per project, in this order:

    1. ANY tenant sharing it has registry ``kind == "customer"`` -> ``"customer"``.
       Customer always wins, regardless of what the other tenants on the project
       classify as.
    2. Else, ANY tenant sharing it is ABSENT from ``kind_by_tenant`` (unregistered,
       or the registry could not be asked at all -- see below) OR carries
       ``kind == services.tenant_names.UNCLASSIFIED`` -> ``"unknown"``.
    3. Else (every tenant sharing it resolved to a NON-customer, NON-unclassified
       kind -- ``internal``, ``service``, ``test``, ...) -> ``"internal"``.

    ⛔ CORRECTED, ROUND 16 FIX: this used to fold BOTH the absent case and the
    ``unclassified`` case into ``"internal"`` -- a deliberate fail-OPEN the
    caller called safe because it could "only make the alarm quieter". It
    contradicted :data:`services.tenant_names.UNCLASSIFIED`'s own invariant
    ("an unclassified tenant must never be silently counted as ... internal"),
    and because the alert built on this gauge fires on non-``internal`` scopes
    ONLY, folding an unknown tenant into ``internal`` did not just make it
    quieter -- it SILENCED a real customer crossing for as long as that tenant
    (or the whole registry, see the empty-map case below) stayed unclassified or
    unreachable. ``"unknown"`` is a THIRD, alerting scope so a registry that
    cannot say "not a customer" can no longer be read as "not a customer".

    ``kind_by_tenant`` keys are expected lowercase (``services.tenant_names``'s
    own shape); tenant ids from `_PROJECT_MULTI_TENANT_SQL` are lower-cased
    again here so a caller cannot get this wrong by construction.

    ⚠️ A TOTALLY EMPTY ``kind_by_tenant`` IS NOT HANDLED HERE. This function has
    no way to tell "the registry answered and every tenant on every project is
    unclassified" (a real, if unlikely, ``"unknown"`` reading) apart from "the
    registry did not answer at all" (every tenant reads absent, so every project
    would ALSO read ``"unknown"`` -- a symptom of the outage wearing the shape of
    a measurement). That distinction needs the caller's context (was the map
    literally ``{}``?), so :func:`run_attribution_audit` makes it: an empty map
    means this function is not called at all and the scope gauges abstain for
    the tick, the same way the four licence-ownership gauges already do on a
    licences outage.
    """
    counts = {"customer": 0, "internal": 0, "unknown": 0}
    for tenant_ids in project_tenants:
        kinds = [kind_by_tenant.get(str(t).lower()) for t in tenant_ids]
        if any(k == "customer" for k in kinds):
            scope = "customer"
        elif any(k is None or k == UNCLASSIFIED for k in kinds):
            scope = "unknown"
        else:
            scope = "internal"
        counts[scope] += 1
    return counts


async def _load(
    session: AsyncSession, window_hours: int
) -> tuple[
    list[tuple[Any, Any, int, float, bool]],
    dict[str, Any],
    list[BudgetUtilisation],
    tuple[int, float],
    list[tuple[str, int, float]],
    tuple[float, float],
    list[list[str]],
    tuple[int, int],
    tuple[int, int, float],
    tuple[float, float, float],
]:
    from services.budget_period import is_stale_period

    now = datetime.now(UTC)
    floor = now - timedelta(hours=window_hours)
    # The same window, at the grain billing_cost_daily is keyed by. Derived from
    # `floor` rather than from `now` so a reader cannot end up with two windows.
    day_floor = floor.date()

    rows = [
        (r.license_id, r.tenant_id, int(r.events), float(r.spend), bool(r.delegated))
        for r in (await session.execute(_WINDOW_SQL, {"floor": floor})).all()
    ]
    # Same :floor bind, same statement, one tick -- so the two halves describe
    # the same window even if a row lands between them.
    unattributed_rows = (await session.execute(_UNATTRIBUTED_SQL, {"floor": floor})).all()
    # count(*) always returns exactly one row from Postgres; the guard is for a
    # caller that stubs the statement, not for the database.
    unattributed = (
        (int(unattributed_rows[0].events), float(unattributed_rows[0].spend or 0.0))
        if unattributed_rows
        else (0, 0.0)
    )
    # Same :floor bind again, same tick. Grouped by tenant rather than counted
    # flat so the log line can NAME the offenders -- a bare count of
    # "spend booked to an identity we do not have" is unactionable.
    unregistered = [
        (str(r.tenant_id), int(r.events), float(r.spend or 0.0))
        for r in (await session.execute(_UNREGISTERED_TENANT_SQL, {"floor": floor})).all()
    ]
    # ⛔ STILL READ, NO LONGER BELIEVED. This is the caller-asserted local proxy;
    # it feeds `compare_local_owner_proxy` ONLY. See `_OWNERS_SQL`'s banner. A
    # reader who changes this to feed `classify_attribution` re-introduces an
    # audit that cannot fail.
    local_owners = {
        str(license_id): tenant_id
        for license_id, tenant_id in (await session.execute(_OWNERS_SQL)).all()
    }
    derived_row = (await session.execute(_DERIVED_SPEND_SQL, {"floor": floor})).one()
    derived_spend = (
        float(derived_row.attributed_usd or 0.0),
        float(derived_row.derived_usd or 0.0),
    )

    # Same :floor bind, same tick, same rules as every statement above. Both
    # legitimately read ZERO today -- nothing in the fleet sends a project id
    # yet -- and a real zero is the point: see the .set() sites in
    # run_attribution_audit for why it is written rather than withheld.
    multi_tenant_rows = (await session.execute(_PROJECT_MULTI_TENANT_SQL, {"floor": floor})).all()
    # One row per project, each carrying the distinct tenant ids that shared it
    # (see _PROJECT_MULTI_TENANT_SQL's R16 banner). The scope classification
    # (customer vs internal) happens in run_attribution_audit, which is the only
    # place with access to the licences registry's `kind` -- this function stays
    # a pure DB read.
    multi_tenant_project_tenants: list[list[str]] = [
        [str(t) for t in (row.tenant_ids or [])] for row in multi_tenant_rows
    ]
    attribution_rows = (
        await session.execute(
            _PROJECT_ATTRIBUTION_SQL,
            {"day_floor": day_floor, "null_project_id": NULL_PROJECT_ID},
        )
    ).all()
    project_attribution = (
        (
            int(attribution_rows[0].project_rows or 0),
            int(attribution_rows[0].sentinel_rows or 0),
        )
        if attribution_rows
        else (0, 0)
    )

    # Same :floor bind, same tick, same rules. ⚠️ The three numbers are kept
    # TOGETHER in one tuple rather than returned as three positional slots: this
    # function's return is an unnamed positional tuple destructured at one call
    # site, so every loose slot is another position a future reader can transpose
    # silently. Grouping them also states that they describe ONE population --
    # projects, rows and dollars from a single scan.
    self_tenant_rows = (await session.execute(_PROJECT_SELF_TENANT_SQL, {"floor": floor})).all()
    # Same one-row guard, and the same reason, as _UNATTRIBUTED_SQL's above: a
    # bare aggregate always returns exactly one row from Postgres, so this is for
    # a caller that stubs the statement, not for the database.
    project_self_tenant = (
        (
            int(self_tenant_rows[0].projects or 0),
            int(self_tenant_rows[0].events or 0),
            float(self_tenant_rows[0].spend or 0.0),
        )
        if self_tenant_rows
        else (0, 0, 0.0)
    )
    coverage_rows = (await session.execute(_PROJECT_SPEND_COVERAGE_SQL, {"floor": floor})).all()
    # ⚠️ Returned in the label order the gauge publishes them in
    # (project, tenant_scoped, unattributed) so the destructuring at the .set()
    # site reads in the same order as the series -- a transposition here would
    # swap two real measurements and nothing downstream could tell.
    project_spend_coverage = (
        (
            float(coverage_rows[0].project_usd or 0.0),
            float(coverage_rows[0].tenant_scoped_usd or 0.0),
            float(coverage_rows[0].unattributed_usd or 0.0),
        )
        if coverage_rows
        else (0.0, 0.0, 0.0)
    )

    budgets: list[BudgetUtilisation] = []
    for r in (await session.execute(_BUDGETS_SQL)).all():
        # Call the accessor, do not mirror it. The hand-rolled version here
        # used `consumed_period_start != anchor`, which differs from
        # is_stale_period's `<` in two ways: a NULL anchor was reported stale
        # instead of falling back to the period of `starts_at` (the rule the
        # backfill migration used), and a FUTURE anchor was reported stale
        # instead of reading through. is_stale_period takes any object with the
        # three attributes, so a Row works -- hence starts_at in the SELECT.
        stale = is_stale_period(r, now)
        budgets.append(
            BudgetUtilisation(
                budget_id=r.id,
                tenant_id=str(r.tenant_id),
                budget_tokens=int(r.budget_tokens or 0),
                consumed_tokens=int(r.consumed_tokens or 0),
                stale=stale,
                # Read, never defaulted: the field HAS a default (the pool
                # sentinel) so the dataclass stays constructible in tests, and a
                # caller that forgot to pass it would fold every licence into
                # the pool and make the licence arm read exactly like the tenant
                # arm -- a silent collapse of the grain this row exists to
                # expose. tests/test_attribution_audit.py drives the real _load
                # path and asserts the two arms DIFFER for that reason.
                license_id=r.license_id,
            )
        )
    return (
        rows,
        local_owners,
        budgets,
        unattributed,
        unregistered,
        derived_spend,
        multi_tenant_project_tenants,
        project_attribution,
        project_self_tenant,
        project_spend_coverage,
    )


async def run_attribution_audit(
    session_factory: async_sessionmaker[AsyncSession],
    *,
    window_hours: int = DEFAULT_WINDOW_HOURS,
    settings: Settings | None = None,
    token_provider: ServiceTokenProvider | None = None,
) -> AttributionAudit:
    """Measure licence/tenant coherence + standing budget utilisation.

    Read-only. Runs on the rollup leader's tick, out of band from the cost
    flush, so it adds nothing to the hot ingest path. Exceptions are the
    caller's to swallow -- this must never be able to abort a rollup cycle.

    ``settings`` and ``token_provider`` are OPTIONAL and default to
    ``get_settings()`` + a module-cached provider. They are keyword-only additions
    precisely so ``services.aggregator``'s existing ``await
    run_attribution_audit(session_factory)`` keeps working: the call site is owned
    by another change set and a positional signature change would have been a
    cross-file edit to deliver one HTTP header.

    ⛔ WHEN LICENCES CANNOT STATE OWNERSHIP THIS DOES NOT RAISE AND DOES NOT
    FALL BACK. It publishes the DB-only populations, publishes NOTHING for the
    four ownership gauges, and WITHHOLDS the freshness timestamp -- so the pass is
    uncertified, ``billing:attribution_audit:fresh`` goes false, and every alert
    that ANDs on it (including ``BillingMisattributedSpendIncreased``) stops
    believing this replica until a pass completes. See
    :class:`LicenceOwnersUnavailableError` for why that is louder than raising.
    """
    if settings is None:
        from settings import get_settings

        settings = get_settings()

    # ⚠️ RESOLVED BEFORE THE DB READ, AND OUTSIDE THE SESSION. Holding a pooled
    # connection open across a 10s HTTP call to another service is how a
    # read-only measurement starts causing pool exhaustion on the hot path.
    registry_owners: dict[str, str] | None = None
    registry_unowned = 0
    owners_error: str | None = None
    try:
        registry_owners, registry_unowned = await fetch_licence_owners(
            settings, token_provider=token_provider
        )
    except LicenceOwnersUnavailableError as exc:
        owners_error = str(exc)
        logger.warning("attribution_audit_licence_owners_unavailable", error=owners_error)

    # Same "resolve before the DB read, outside the session" discipline as
    # `registry_owners` above. `get_tenant_info` is `services.tenant_names`'s
    # existing fail-open registry cache (5-min TTL, `{}` on any licences
    # outage) -- reused rather than a second client, matching every other
    # `kind` reader in this service. See `classify_multi_tenant_scope`'s
    # docstring for why an outage here only ever makes the scope split
    # quieter, never fabricates a customer alert.
    tenant_kind_by_id = {
        tid: info.kind for tid, info in (await get_tenant_info(settings, token_provider)).items()
    }

    async with session_factory() as session:
        (
            rows,
            local_owners,
            budgets,
            (unattributed_events, unattributed_spend),
            unregistered,
            (attributed_usd, derived_usd),
            multi_tenant_project_tenants,
            (project_rows, sentinel_rows),
            (self_tenant_projects, self_tenant_rows, self_tenant_spend),
            (project_spend_usd, tenant_scoped_spend_usd, unattributed_spend_project_usd),
        ) = await _load(session, window_hours)
        # Separate from `_load`'s positional tuple on purpose (its callers
        # unpack it by position). Same session, same tick, rolling window.
        emitter_coverage = await load_emitter_coverage(session, window_hours)

    if registry_owners is None:
        # ⛔ `classify_attribution` IS NOT CALLED. Calling it with an empty dict
        # would return unresolved=total / mismatch=0 -- a shape indistinguishable
        # on the dataclass from "checked, and clean". `total_events` is summed
        # directly instead: it comes from `_WINDOW_SQL` and owes nothing to the
        # registry, so the attribution RATE (can this spend reach an invoice line
        # at all?) stays a real measurement during a licences outage. Only the
        # ownership verdicts abstain.
        audit = AttributionAudit(
            total_events=sum(int(row[2]) for row in rows),
            unattributed_events=int(unattributed_events),
            unattributed_spend_usd=float(unattributed_spend or 0.0),
            owner_source=OWNER_SOURCE_UNAVAILABLE,
        )
    else:
        audit = classify_attribution(
            rows=rows,
            owner_by_license=registry_owners,
            unattributed_events=unattributed_events,
            unattributed_spend_usd=unattributed_spend,
            owner_source=OWNER_SOURCE_REGISTRY,
        )
        audit.registry_licences = len(registry_owners) + registry_unowned
        audit.registry_unowned_licences = registry_unowned
        # The old authority, measured against the new one. Only computable when
        # there IS a registry answer -- `None` otherwise, never 0, because "the
        # proxy agreed on nothing we could check" must not render as "the proxy
        # agreed".
        (
            audit.local_owner_disagreements,
            audit.local_owner_only_licences,
        ) = compare_local_owner_proxy(registry_owners, local_owners)
    # Set on the audit AFTER classification rather than threaded through
    # `classify_attribution`: that function adjudicates ownership CLAIMS, and a
    # tenant with no registry row makes no claim to adjudicate. Folding it in
    # would put an unrelated defect behind the same denominator and make each
    # one look smaller whenever the other grew.
    audit.unregistered_tenant_events = sum(events for _t, events, _s in unregistered)
    audit.unregistered_tenant_spend_usd = sum(spend for _t, _e, spend in unregistered)
    audit.top_unregistered_tenants = [(t, e) for t, e, _s in unregistered[:5]]

    util = summarise_budget_utilisation(budgets, datetime.now(UTC))

    # ⛔ THE FOUR OWNERSHIP GAUGES, AND THE ONE GUARD IN FRONT OF THEM.
    #
    # Gated on `audit.coherence_measured` -- a question about the SOURCE -- and not
    # on the numbers, because `mismatch_events == 0` is both a healthy reading and
    # what an unchecked window looks like, and telling those two apart is the
    # entire point of this change. Before it, the map came from
    # `request_quota_config`, which is derived FROM the events being audited, so
    # these four always published a self-confirming 0 and
    # `BillingMisattributedSpendIncreased` could not fire on a real mismatch.
    #
    # On abstention they keep their previous value and the freshness timestamp at
    # the bottom is NOT written, so nothing believes them. ⚠️ Not `.set(0)`, and
    # not a separate "unknown" sentinel gauge: `observability/metrics.py` is owned
    # by another change set in flight, so a new series cannot be added here -- and
    # the existing freshness timestamp is this module's documented, single
    # discriminator for exactly this ("There is deliberately no second
    # mechanism", below).
    if audit.coherence_measured:
        observability_metrics.license_tenant_mismatch_events.set(audit.mismatch_events)
        observability_metrics.license_tenant_mismatch_spend_usd.set(audit.mismatch_spend_usd)
        observability_metrics.license_tenant_delegated_events.set(audit.delegated_events)
        observability_metrics.license_tenant_delegated_spend_usd.set(audit.delegated_spend_usd)
        observability_metrics.license_tenant_unresolved_events.set(audit.unresolved_events)
        observability_metrics.license_attributed_events.set(audit.matched_events)
    observability_metrics.license_unattributed_events.set(audit.unattributed_events)
    rate = audit.attribution_rate
    if rate is not None:
        observability_metrics.attribution_rate.set(rate)
    # else: write NOTHING. An empty window means there was nothing to attribute,
    # and 0.0 says the opposite -- "not one billable event could reach an
    # invoice line" -- which is exactly the shape of total breakage and would
    # fire any alert written against this gauge on the quietest night of the
    # year. The gauge deliberately keeps its previous value instead; a stale
    # reading is honest, a fabricated zero is not.
    #
    # This is an abstention by the AUDIT, not an absent series -- prometheus_
    # client exports an unlabelled gauge as 0.0 from process start, so a replica
    # that has never completed a pass still scrapes 0.0 here. That case is
    # already covered by the SAME discriminator the six gauges above rely on:
    # the freshness timestamp below is 0 on such a replica, so nothing believes
    # it. There is deliberately no second mechanism.
    observability_metrics.license_tenant_unregistered_events.set(audit.unregistered_tenant_events)
    observability_metrics.license_tenant_unregistered_spend_usd.set(
        audit.unregistered_tenant_spend_usd
    )
    observability_metrics.budgets_over_limit.set(util.over_limit)
    observability_metrics.budget_max_utilisation_ratio.set(util.max_ratio)
    # The arm a reader may treat as "a tenant is over its cap". Published
    # BESIDE the per-row pair, never instead of it -- see
    # summarise_budget_utilisation for why the two differ and which is which.
    observability_metrics.budget_tenants_over_limit.set(util.tenants_over_limit)
    observability_metrics.budget_tenant_max_utilisation_ratio.set(util.tenant_max_ratio)
    # The DEBIT grain, published BESIDE both arms above and never instead of
    # either. It is the grain cost_flusher actually drains and the only one of
    # the three that was previously unmeasured; it is NOT the grain anything
    # enforces. Both gauges' help text says so, because a bare number on a panel
    # is where that distinction gets lost.
    observability_metrics.budget_licences_over_limit.set(util.licences_over_limit)
    observability_metrics.budget_licence_max_utilisation_ratio.set(util.licence_max_ratio)
    # ⚠️ PUBLISH NOTHING when there is no attributed spend to take a share OF.
    # 0.0 would read "every attributed dollar was asserted by its emitter" --
    # the most flattering possible reading of an empty window, and indistinguishable
    # from the goal state this gauge exists to measure progress toward. The gauge
    # keeps its previous value instead; staleness is detected by
    # billing:attribution_audit:fresh, exactly as attribution_rate does above.
    if attributed_usd > 0:
        observability_metrics.licence_derived_spend_share.set(derived_usd / attributed_usd)
    # ⭐ THE PROJECT AXIS, AND WHY THESE THREE ARE WRITTEN UNCONDITIONALLY.
    #
    # The rule the four ownership gauges follow (⛔ block above) is "do not
    # publish a value you did not measure" -- their source can be UNREACHABLE, so
    # a 0 there would be an unchecked window wearing a healthy number. Neither
    # applies here: both numbers come from this session's own SELECTs over the
    # same window as everything above, so there is no state in which they are
    # unknown. Today they read 0 / 0 / N because NOTHING IN THE FLEET SENDS A
    # PROJECT ID YET, and that zero is a MEASUREMENT -- withholding it would
    # leave a series that is absent on a healthy estate and absent on a broken
    # one, which is the exact failure `cost_daily_project_attribution_rows` was
    # created to end. A zero that was measured gets written.
    #
    # Leader/follower is handled the same way as every gauge above it and needs
    # nothing extra: this whole function runs on the rollup leader's tick
    # (services/aggregator.py), a follower therefore never reaches these lines,
    # and its export stays at the birth value in observability/metrics.py. What
    # tells a reader which replica's numbers are real is the freshness timestamp
    # below -- written LAST, so an exception anywhere in here leaves the series
    # stale rather than believed. Aggregate both with max(), never sum(): a
    # leadership handover inside one scrape window can leave two replicas
    # holding a value.
    # ⭐ R16 FIX ROUND 16: SCOPED by `classify_multi_tenant_scope`, using the
    # tenant-kind map resolved outside the session above -- BUT ONLY WHEN THAT
    # MAP IS NON-EMPTY. `get_tenant_info` folds a 401, a transport error and a
    # genuinely empty registry into the SAME `{}` (its own documented
    # contract), and `get_tenant_info_strict` already treats an empty map as a
    # non-answer rather than "the estate has no tenants" for exactly that
    # reason (billing cannot run without this registry holding rows). A
    # completely empty map would make EVERY tenant on EVERY project read
    # "absent", so `classify_multi_tenant_scope` would classify every crossing
    # project `"unknown"` -- not a measurement, the outage wearing the shape of
    # one. So this abstains: same mechanism the four licence-ownership gauges
    # above use (publish nothing, keep the previous value, withhold the
    # freshness timestamp below) rather than write a misleading classification.
    # A map that is merely PARTIAL -- some tenants present, others absent or
    # `unclassified` -- is not this case: those tenants correctly read
    # `"unknown"` per-project, which is the fix, not the abstention.
    tenant_kind_registry_unavailable = not tenant_kind_by_id
    multi_tenant_projects = len(multi_tenant_project_tenants)
    multi_tenant_scope_counts: dict[str, int] | None = None
    if not tenant_kind_registry_unavailable:
        multi_tenant_scope_counts = classify_multi_tenant_scope(
            multi_tenant_project_tenants, tenant_kind_by_id
        )
        observability_metrics.project_id_multi_tenant_projects.labels(scope="customer").set(
            multi_tenant_scope_counts["customer"]
        )
        observability_metrics.project_id_multi_tenant_projects.labels(scope="internal").set(
            multi_tenant_scope_counts["internal"]
        )
        observability_metrics.project_id_multi_tenant_projects.labels(scope="unknown").set(
            multi_tenant_scope_counts["unknown"]
        )
    observability_metrics.cost_daily_project_attribution_rows.labels(attribution="project").set(
        project_rows
    )
    observability_metrics.cost_daily_project_attribution_rows.labels(attribution="sentinel").set(
        sentinel_rows
    )
    # The forged-project detector. Same unconditional rule as the three above,
    # and for the same reason: it comes from this session's own SELECT over the
    # same window, so there is no state in which it is unknown and a measured
    # zero is written rather than withheld. ⚠️ Unlike its neighbours this one is
    # NOT zero on this estate today -- 62 rows / $6.0458 on prod 2026-09-16 --
    # which is exactly why all three series are published: rows alone understate
    # it 4x against dollars, and the distinct-project count is what separates one
    # buggy emitter from a whole population.
    observability_metrics.project_id_equals_tenant_projects.set(self_tenant_projects)
    observability_metrics.project_id_equals_tenant_rows.set(self_tenant_rows)
    observability_metrics.project_id_equals_tenant_spend_usd.set(self_tenant_spend)
    # Dollar-denominated project coverage. ⛔ ALL THREE LABELS ARE WRITTEN ON
    # EVERY PASS, never only the non-zero ones: a `tenant_scoped` or
    # `unattributed` series that is absent on a healthy estate and absent on a
    # broken one is the failure this whole block exists to end, and the
    # partition identity (the three sum to the window's billable total) is only
    # checkable if all three are present. `tenant_scoped` legitimately reads 0.0
    # until the spend_scope header lands; that zero is a measurement.
    observability_metrics.cost_events_project_spend_usd.labels(attribution="project").set(
        project_spend_usd
    )
    observability_metrics.cost_events_project_spend_usd.labels(attribution="tenant_scoped").set(
        tenant_scoped_spend_usd
    )
    observability_metrics.cost_events_project_spend_usd.labels(attribution="unattributed").set(
        unattributed_spend_project_usd
    )
    # Per-emitter project/actor coverage (counts; ratio in PromQL). Written
    # unconditionally like the project axis above -- this session's own SELECT,
    # so never unknown -- and BEFORE the freshness timestamp so a failure here
    # leaves the pass uncertified rather than certifying stale series.
    publish_emitter_coverage(emitter_coverage)
    # LAST, and only here. Prometheus scrapes every replica but only the leader
    # writes the coherence gauges above, and a non-leader keeps exporting what it
    # last wrote (process-lifetime state, not a default 0) -- so the panels and
    # alerts select on this timestamp being recent, not on who claims to be
    # leader. Publishing it after every .set() is what makes "fresh" mean
    # "they are all from the same completed pass": an exception anywhere above
    # leaves the timestamp at its previous value, the series goes stale, and the
    # replica stops being believed. Setting it first, or in a finally, would
    # certify a partial pass.
    #
    # ⛔ AND A PASS THAT COULD NOT RESOLVE OWNERSHIP IS A PARTIAL PASS. Writing the
    # timestamp here would certify the four ownership gauges' PREVIOUS values as
    # belonging to this window -- the stale-reading version of the fabricated zero.
    # The cost is real and accepted: the budget/unattributed/unregistered arms are
    # genuinely measured this tick and go uncertified along with it. That is the
    # contract this timestamp's own docstring states ("they are all from the same
    # completed pass"), it recovers on the very next tick once licences answers,
    # and the numbers are still on the log line for a human mid-outage.
    # ⭐ R16 FIX: the tenant-kind abstention above is IN this predicate too, for
    # the same reason `audit.coherence_measured` already is. This timestamp's
    # contract is "everything gated on it is from the SAME completed pass" --
    # a scope split abstained for want of a registry answer is exactly as
    # partial a pass as ownership verdicts abstained for the same reason, so
    # the freshness gate cannot certify one without certifying the other.
    if audit.coherence_measured and not tenant_kind_registry_unavailable:
        observability_metrics.audit_last_success_timestamp_seconds.set(time.time())

    # ⚠️ `util.licences_over_limit` is deliberately NOT in this predicate, and
    # neither is `util.tenants_over_limit`. Licence-grain oversubscription is a
    # STANDING condition on this estate (three licences over 1.0 as of
    # 2026-09-11) that refuses nothing, so escalating the line on it would turn
    # every audit tick into a warning and teach the reader to ignore all of
    # them. The severity of this line is unchanged by this measurement; the
    # numbers are on it either way.
    #
    # ⭐ `not audit.coherence_measured` IS IN THE PREDICATE, unlike the two
    # standing conditions above, because it is not a standing condition: it means
    # the estate's misattribution signal is DARK for this tick. A reporting pass
    # that silently logged `info` while publishing nothing is how an unwatched
    # instrument stays unwatched.
    log = (
        logger.warning
        if (
            not audit.coherence_measured
            # ⭐ R16 FIX: the tenant-kind registry going dark is not a standing
            # condition either -- it means the scope split is unmeasured for
            # this tick, same reasoning as `coherence_measured` above.
            or tenant_kind_registry_unavailable
            or audit.mismatch_events
            or audit.unattributed_events
            or audit.unregistered_tenant_events
            or util.over_limit
            # ⭐ IN the predicate, and for the same reason `coherence_measured`
            # is: it is not a standing condition. It is 0 across this estate
            # today, so it costs no noise, and any non-zero means one caller is
            # stamping a project id another tenant also uses -- the finding this
            # detector exists for, which nothing else in the estate would say.
            # ⚠️ NOT a payer-selection defect; do not page on it. See
            # billing_project_id_multi_tenant_projects' help text.
            or multi_tenant_projects
        )
        else logger.info
    )
    # ⛔ EVERY OWNERSHIP FIELD IS `None` WHEN THE SOURCE WAS UNAVAILABLE, never the
    # dataclass's 0. A structured log is read by humans and by log-based panels
    # alike, and `mismatch_events=0` on this line is the same fabricated zero the
    # gauges refuse to publish -- just in a different store.
    measured = audit.coherence_measured
    log(
        "attribution_audit",
        window_hours=window_hours,
        # WHERE the ownership verdicts came from, on the same line as the verdicts.
        owner_source=audit.owner_source,
        owner_source_error=owners_error,
        registry_licences=audit.registry_licences if measured else None,
        registry_unowned_licences=audit.registry_unowned_licences if measured else None,
        # ⭐ The size of the lie the audit used to be told: licences where
        # `request_quota_config` names a different owner than the registry does.
        # 0 means the local proxy happens to agree today, NOT that it is a source.
        local_owner_disagreements=audit.local_owner_disagreements,
        local_owner_only_licences=audit.local_owner_only_licences,
        # total_events is from _WINDOW_SQL and owes the registry nothing, so it is
        # a real measurement even on the abstention path. The four below are not.
        total_events=audit.total_events,
        matched_events=audit.matched_events if measured else None,
        mismatch_events=audit.mismatch_events if measured else None,
        unresolved_events=audit.unresolved_events if measured else None,
        mismatch_spend_usd=round(audit.mismatch_spend_usd, 4) if measured else None,
        delegated_events=audit.delegated_events if measured else None,
        delegated_spend_usd=round(audit.delegated_spend_usd, 4) if measured else None,
        unattributed_events=audit.unattributed_events,
        unattributed_spend_usd=round(audit.unattributed_spend_usd, 4),
        billable_events=audit.billable_events,
        # None on an empty window rather than a 0.0 that reads as health -- and
        # None on an unchecked one, for the same reason one step further back.
        mismatch_ratio=audit.mismatch_ratio if measured else None,
        # Ditto -- None, never 0.0, when there was nothing to attribute.
        attribution_rate=audit.attribution_rate,
        budgets_considered=util.considered,
        budgets_over_limit=util.over_limit,
        budget_max_utilisation_ratio=round(util.max_ratio, 4),
        budget_tenants_over_limit=util.tenants_over_limit,
        budget_tenant_max_utilisation_ratio=round(util.tenant_max_ratio, 4),
        budget_licences_considered=util.licences_considered,
        budget_licences_over_limit=util.licences_over_limit,
        budget_licence_max_utilisation_ratio=round(util.licence_max_ratio, 4),
        # Which of the three preceding ratios the gate reads, spelled out on the
        # same line as the ratios themselves.
        budget_grain_legend=BUDGET_GRAIN_LEGEND,
        attributed_spend_usd=round(attributed_usd, 4),
        licence_derived_spend_usd=round(derived_usd, 4),
        licence_derived_spend_share=(
            round(derived_usd / attributed_usd, 4) if attributed_usd > 0 else None
        ),
        unregistered_tenant_events=audit.unregistered_tenant_events,
        unregistered_tenant_spend_usd=round(audit.unregistered_tenant_spend_usd, 4),
        # The project axis, beside the numbers it must be read with. Both are
        # REAL ZEROS until an emitter starts stamping projects, never absent.
        project_id_multi_tenant_projects=multi_tenant_projects,
        # R16 FIX: the scope split behind the three `scope=` gauge series, on
        # the same line as the total -- see classify_multi_tenant_scope. `None`
        # on every field when the tenant-kind registry did not answer, same
        # rule the ownership fields above follow: a 0 here would be the
        # fabricated-zero this whole abstention exists to prevent.
        tenant_kind_registry_unavailable=tenant_kind_registry_unavailable,
        project_id_multi_tenant_projects_customer=(
            multi_tenant_scope_counts["customer"] if multi_tenant_scope_counts else None
        ),
        project_id_multi_tenant_projects_internal=(
            multi_tenant_scope_counts["internal"] if multi_tenant_scope_counts else None
        ),
        project_id_multi_tenant_projects_unknown=(
            multi_tenant_scope_counts["unknown"] if multi_tenant_scope_counts else None
        ),
        cost_daily_project_rows=project_rows,
        cost_daily_sentinel_rows=sentinel_rows,
        # ⚠️ NOT in the `log` severity predicate above, and NOT by oversight --
        # by the same rule that keeps `util.licences_over_limit` out of it. A
        # forged project id is a STANDING condition on this estate (62 rows /
        # $6.0458 of $9.7622 on prod 2026-09-16), so escalating on it would make
        # every audit tick a warning and teach the reader to ignore all of them.
        # `multi_tenant_projects` IS in the predicate precisely because it is 0
        # here today and therefore costs no noise. When these three reach 0 they
        # become eligible for the predicate on the same grounds; until then the
        # escalation belongs on an alert with a threshold, not on this line.
        project_id_equals_tenant_projects=self_tenant_projects,
        project_id_equals_tenant_rows=self_tenant_rows,
        # Rounded like every other dollar figure on this line. NOT an invoice
        # figure -- measured without the billable filter; see the SQL's banner.
        project_id_equals_tenant_spend_usd=round(self_tenant_spend, 4),
        # Dollar-denominated project coverage, all three buckets on one line so a
        # reader can check the partition (they sum to the window's billable
        # total) without leaving the log. ⚠️ DOLLARS, not rows -- the two
        # cost_daily_* fields directly above are rows, and they are the fields
        # this set exists to stop being quoted as a share of spend.
        project_spend_usd=round(project_spend_usd, 4),
        tenant_scoped_spend_usd=round(tenant_scoped_spend_usd, 4),
        unattributed_project_spend_usd=round(unattributed_spend_project_usd, 4),
        emitter_coverage_service_keys=len(emitter_coverage),
        top_pairs=audit.top_pairs if measured else None,
        top_delegated_pairs=audit.top_delegated_pairs if measured else None,
        # NAMES the identities, not just a count -- `0195f48b-dead-...-beef`
        # reaching a financial table is the strongest evidence that nothing
        # checked, and it is only evidence if someone can read it.
        top_unregistered_tenants=audit.top_unregistered_tenants,
    )
    return audit
