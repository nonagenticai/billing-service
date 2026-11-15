"""Periodic budget↔licence reconcile: a licence that stopped being active
upstream must not leave its ``source='license_sync'`` token budget spendable.

WHY THIS EXISTS (and why the push-side fix is not enough)
--------------------------------------------------------
``licenses`` PR #192 tears the budget down synchronously when a human calls
``POST /license/revoke`` with a ``license_id`` and billing is reachable. That
closes exactly one path. Measured on production 2026-09-10, the residue it
cannot reach is not hypothetical:

* ``reap_expired_licences`` (CronJob, 05:11 UTC daily) flips licences to
  ``status='revoked'`` with a direct DB write. It imports ``models`` only —
  never ``main`` — so it structurally cannot reach the teardown push. It mints
  new revoked licences every night, outside #192's code path.
* ``expire_stale_licences`` (04:07 UTC) sets ``status='expired'``. ``'expired'``
  is not ``'revoked'``; nothing on the revoke path fires.
* ``delete_user_licenses`` / ``delete_orphan_licenses_by_tenant`` HARD-delete
  the licence row. The licence is then gone, so even a status-comparing check
  upstream would find nothing to compare.
* The push is ``_spawn_background_task``'d fire-and-forget: ``revoke_license``
  has already committed and the HTTP 200 says "revoked" whether or not billing
  ever heard about it. There is no timer to sweep what it dropped.

A push is open-loop — it only fires on paths someone remembered to instrument.
This loop is closed-loop: it asks "which active license_sync budgets are NOT
backed by an active licence right now" and converges, without caring HOW the
licence stopped being active. That set difference covers revoked, expired,
reaped, hard-deleted and push-failed alike, which is precisely the property
that makes it durable rather than one more path-specific patch.

RELATIONSHIP TO ``POST /api/budget/sync-from-licenses``
------------------------------------------------------
That endpoint already performs this deactivation, via the same
``deactivate_orphan_license_sync_budgets`` helper. It is NOT a substitute: its
own docstring records that nothing calls it on a timer, and the estate's only
callers are two n8n WEBHOOK workflows (onboard-customer, change-plan) — both
event-driven. Between two such events the divergence is unmeasured and
unrepaired.

This loop deliberately does NOT schedule that endpoint. That endpoint also
upserts every active licence's budget from its plan and hard-deletes absent
``request_quota_config`` rows; putting that blast radius on an unattended timer
is a much larger bet than the defect requires. This module does one thing.

⚠️ STRICTLY MORE CONSERVATIVE THAN THE MANUAL ENDPOINT — ON PURPOSE
-------------------------------------------------------------------
``sync_from_licenses`` builds its active set from ``status == 'active'`` only,
and ``continue``s past a licence whose status it does not recognise BEFORE
adding it to that set. For an operator-triggered run that is fine: a human is
watching the response. For an unattended loop it is a live hazard — the day
``licenses`` grows a ``'suspended'`` or ``'trialing'`` status, an unrecognised
value would silently make every budget behind it look like an orphan and this
loop would deactivate them all.

So the set passed to the helper here is ``active ∪ unrecognised ∪
missing-status``: deactivation requires a POSITIVE signal that the licence
stopped being active (a status we understand that is not active), or that the
licence is GONE from the registry entirely (which is what catches the hard
deletes). A licence we merely failed to understand is PROTECTED, counted, and
published on ``billing_licence_budget_reconcile_unreadable_total`` so that
"upstream's vocabulary moved" surfaces as a metric instead of as missing money.

⛔ WHAT THIS DOES NOT COVER — read before trusting it
----------------------------------------------------
* ``source='manual'`` and ``source='topup'`` budgets. The helper is scoped to
  ``source='license_sync'``, and widening it would let this loop delete
  hand-granted credit that no licence ever backed. A manual budget cut against
  a revoked licence stays live and only a human will find it. (Measured
  2026-09-10: all 19 divergent rows were ``license_sync``, so this is a real
  but currently-empty gap.)
* The window between ticks. This bounds the exposure to
  ``licence_budget_reconcile_interval_seconds``; it does not eliminate it.
  The push in #192 is still the fast path and is still worth having.
* A cycle where the licences fetch fails. The loop refuses to mutate on a
  partial or failed read (see ``_fetch_licence_lifecycle``), which is the right
  trade — a truncated read would orphan live budgets — but it means an
  unreachable licences service pauses repair. That is what the freshness
  timestamp is for.
* Redis keys for budgets this loop did not touch. Only the rows it deactivated
  are re-published.
"""

from __future__ import annotations

import asyncio
import contextlib
import time
import uuid
from typing import Any

import httpx
import structlog
from sqlalchemy import select
from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from models.db import ACTIVE_LICENSE_STATUS, LICENSE_STATUSES, TokenBudget
from observability import metrics as observability_metrics
from services.budget_crud import deactivate_orphan_license_sync_budgets
from services.leader_election import LICENCE_BUDGET_RECONCILE_LOCK_KEY, AsyncLeaderElection
from services.licence_limits import effective_limit
from services.quota import record_quota_config_value_drift, snapshot_quota_config_values
from settings import Settings

logger = structlog.stdlib.get_logger(__name__)

# A cycle that keeps failing hands the lease back so a replica that is not
# wedged can reconcile. Same shape as invoice_closer.
MAX_CONSECUTIVE_RECONCILE_FAILURES: int = 5


class LicenceFetchError(RuntimeError):
    """The licences registry could not be read completely this cycle.

    Raised rather than returned so a caller cannot accidentally treat an
    unreadable registry as "no licences are active", which is the one
    misreading that would deactivate every budget in the estate.
    """


class NoActiveLicencesError(LicenceFetchError):
    """The registry read PERFECTLY and classified not one licence as
    active-or-unreadable.

    A subclass of :class:`LicenceFetchError` on purpose, so every existing
    caller that refuses to mutate on a fetch error keeps refusing on this one
    -- the fail-safe direction is identical and must not depend on anyone
    remembering to catch a second type.

    What differs is the DIAGNOSIS, and that is why it is a distinct type rather
    than a distinct message. The loop files ``LicenceFetchError`` under
    ``billing_licence_budget_reconcile_failures_total{reason=
    "licences_unreadable"}``, whose own metric docstring defines that reason as
    "the registry could not be read completely". Here the registry could be,
    and was. Filing this refusal under that reason sends an operator to debug a
    licences service that is perfectly healthy, while the real condition -- an
    estate in which zero licences classify active -- goes unnamed. The repair
    never converges and leadership churns every
    ``MAX_CONSECUTIVE_RECONCILE_FAILURES`` ticks while it does.
    """


class LicenceLifecycle:
    """The result of one complete read of the licences registry.

    ``protected_ids`` is what the orphan reconcile is keyed on: it is
    deliberately WIDER than ``active_ids`` (see the module docstring). Keeping
    both on one object stops a caller from reconstructing the wider set itself
    and getting the fail-safe direction wrong.

    ``plan_quota_monthly`` is the UPSTREAM half of the quota-value drift
    comparison — ``plan.quota_monthly`` exactly as the registry returned it. It
    rides on this object rather than being re-fetched because there is no dblink
    between billing's Postgres and licences', so the only truthful comparison is
    against a genuinely-just-fetched payload; a second fetch would be a second
    point in time. See :func:`services.quota.record_quota_config_value_drift`.
    """

    __slots__ = (
        "active_ids",
        "protected_ids",
        "non_active_count",
        "unreadable_count",
        "plan_quota_monthly",
    )

    def __init__(
        self,
        *,
        active_ids: set[uuid.UUID],
        protected_ids: set[uuid.UUID],
        non_active_count: int,
        unreadable_count: int,
        plan_quota_monthly: dict[uuid.UUID, int] | None = None,
    ) -> None:
        self.active_ids = active_ids
        self.protected_ids = protected_ids
        self.non_active_count = non_active_count
        self.unreadable_count = unreadable_count
        self.plan_quota_monthly = plan_quota_monthly if plan_quota_monthly is not None else {}


async def _fetch_licence_lifecycle(
    settings: Settings,
    token_provider: Any | None,
    *,
    client: httpx.AsyncClient,
) -> LicenceLifecycle:
    """Read EVERY licence and classify it. Raises ``LicenceFetchError`` on any
    incomplete read.

    NO ``status`` query param and NO ``X-Tenant-Id`` header, for the two
    reasons ``routers.budget.sync_from_licenses`` documents at length: filtering
    to active makes a revoked licence indistinguishable from one that never
    existed, and ``'__admin__'`` is not a UUID so the licences middleware 400s
    the call before it ever reads the bearer token.
    """
    headers: dict[str, str] = {}
    if token_provider is not None:
        token = await token_provider.get_token()
        if token:
            headers["Authorization"] = f"Bearer {token}"

    try:
        resp = await client.get(f"{settings.licenses_url}/internal/licenses", headers=headers)
    except httpx.RequestError as exc:
        raise LicenceFetchError("licences service unreachable") from exc

    if resp.status_code != 200:
        raise LicenceFetchError(f"licences service returned {resp.status_code}")

    try:
        payload = resp.json()
    except ValueError as exc:
        raise LicenceFetchError("licences service returned unparseable JSON") from exc

    if not isinstance(payload, list):
        # A dict here is what a paginated envelope would look like. Refusing it
        # matters: `for lic in {...}` iterates KEYS, so a silently-enveloped
        # response would yield zero parseable licences and read as "nothing is
        # active" — the exact shape that deactivates the whole estate.
        raise LicenceFetchError("licences service returned a non-list payload")

    if not payload:
        # ⛔ AN EMPTY REGISTRY IS REFUSED, NOT HONOURED — and this is the one
        # place this module deliberately disagrees with
        # ``deactivate_orphan_license_sync_budgets``, whose docstring says an
        # empty active set "legitimately means the licences service returned
        # zero active licenses".
        #
        # That reading is defensible for an operator who typed the request and
        # is reading the response. It is not defensible on a timer. An estate
        # with zero licences is a state this system has never been in, whereas
        # "a 200 carrying []" is what a half-migrated database, a mis-scoped
        # query and a tenant-filtered response all look like — and honouring it
        # once would deactivate EVERY license_sync budget in the estate in a
        # single tick. The precedent is already in this repo:
        # ``delete_absent_quota_configs`` raises ``EmptyActiveSetError`` rather
        # than honouring an empty set, for the same reason.
        raise LicenceFetchError("licences service returned zero licences")

    active_ids: set[uuid.UUID] = set()
    protected_ids: set[uuid.UUID] = set()
    plan_quota_monthly: dict[uuid.UUID, int] = {}
    non_active_count = 0
    unreadable_count = 0

    for lic in payload:
        if not isinstance(lic, dict):
            raise LicenceFetchError("licences service returned a non-object licence")
        raw_id = lic.get("id")
        if not raw_id:
            # Cannot be classified and cannot be matched to a budget. Counting
            # it as unreadable is not enough: a licence with no id might be the
            # very one backing a live budget, and we would deactivate it as
            # "absent". Refuse the whole cycle instead.
            raise LicenceFetchError("licences service returned a licence with no id")
        try:
            lic_uuid = uuid.UUID(str(raw_id))
        except (ValueError, TypeError) as exc:
            raise LicenceFetchError("licences service returned an unparseable licence id") from exc

        # The upstream half of the quota-value drift comparison, harvested from
        # the SAME read that classifies the licence — see the LicenceLifecycle
        # docstring for why it must not be a second fetch.
        #
        # ⛔ A LICENCE WHOSE PLAN CARRIES NO `quota_monthly` IS OMITTED, NOT
        # COERCED TO 0, and that is a correctness rule rather than tidiness:
        # `services.quota.check_quota` reads 0 as BLOCK ALL, so coercing an
        # absent/unlimited plan quota to 0 would manufacture drift against every
        # licence nobody configured a quota for. `record_quota_config_value_drift`
        # states the same rule in its SCOPE paragraph; it is enforced here, at the
        # only place that can tell "absent" from "zero".
        #
        # A bool is rejected explicitly: `isinstance(True, int)` is True in
        # Python, and a JSON `true` arriving in this field would otherwise be
        # compared as the integer 1 against a real stored limit.
        #
        # The EFFECTIVE quota (per-licence `quota_override` wins over the plan
        # default) — the same value `sync_from_licenses` writes to the mirror.
        # Comparing against the bare plan value would report every overridden
        # licence as drift forever. Despite its name, `plan_quota_monthly`
        # therefore carries the effective upstream limit.
        plan = lic.get("plan")
        raw_quota = effective_limit(
            lic.get("quota_override"),
            plan.get("quota_monthly") if isinstance(plan, dict) else None,
            field="quota_override",
            license_id=raw_id,
        )
        if isinstance(raw_quota, int) and not isinstance(raw_quota, bool) and raw_quota >= 0:
            plan_quota_monthly[lic_uuid] = raw_quota

        raw_status = lic.get("status")
        status = str(raw_status).strip().lower() if raw_status else ""

        if not status:
            # ABSENT status. `sync_from_licenses` defaults this to active; here
            # it merely PROTECTS the budget without claiming the licence is
            # active. Same fail-safe direction, one fewer fabricated fact.
            unreadable_count += 1
            protected_ids.add(lic_uuid)
            continue

        if status not in LICENSE_STATUSES:
            # Upstream's lifecycle vocabulary has grown a value this service
            # does not model. Protect, count, publish — never infer.
            logger.warning("licence_budget_reconcile_unrecognised_status", status=status)
            unreadable_count += 1
            protected_ids.add(lic_uuid)
            continue

        if status == ACTIVE_LICENSE_STATUS:
            active_ids.add(lic_uuid)
            protected_ids.add(lic_uuid)
        else:
            # A status we understand that is not active: revoked, expired.
            # THIS is the positive signal that permits deactivation.
            non_active_count += 1

    return LicenceLifecycle(
        active_ids=active_ids,
        protected_ids=protected_ids,
        non_active_count=non_active_count,
        unreadable_count=unreadable_count,
        plan_quota_monthly=plan_quota_monthly,
    )


async def measure_divergence(session: AsyncSession, *, protected_ids: set[uuid.UUID]) -> int:
    """Count active ``license_sync`` budgets not backed by a protected licence.

    READ-ONLY. This is the number an alert should watch: it is computed BEFORE
    any mutation, so a run that measures 19 and then repairs 19 publishes 19 —
    and the NEXT run publishes 0. A gauge computed after the repair would read
    0 forever and could never distinguish "healthy" from "never ran".
    """
    stmt = select(TokenBudget.id).where(
        TokenBudget.source == "license_sync",
        TokenBudget.active.is_(True),
        TokenBudget.license_id.not_in(protected_ids),
    )
    return len((await session.execute(stmt)).scalars().all())


async def _measure_quota_config_value_drift(
    session_factory: async_sessionmaker[AsyncSession],
    lifecycle: LicenceLifecycle,
) -> None:
    """Publish ``billing_quota_config_value_drift`` from THIS tick's registry read.

    ⭐ ACCEPTANCE: the gauge leaves **-1**. It read -1 on both prod pods —
    ``observability/metrics.py``'s module-level initial, documented there as
    "NEVER MEASURED" — because :func:`services.quota.record_quota_config_value_drift`
    had no caller at all. ``routers.budget.sync_from_licenses`` is the only place
    that ever reached :func:`services.quota.record_quota_config_drift`, and it
    passes neither ``expected_quota_monthly`` nor ``mirror_quota_before``, so the
    value comparison stayed opt-out-by-default and the detector never ran once.
    A correct detector that is never called is worth exactly as much as a broken
    one, and costs more, because its correctness reads as coverage.

    WHY THIS LOOP IS THE RIGHT HOST, and the two properties that decide it:

    1. It already holds the UPSTREAM side. ``_fetch_licence_lifecycle`` above
       performs the same ``GET {licenses_url}/internal/licenses`` the detector's
       docstring names, and now carries each licence's ``plan.quota_monthly`` off
       that one read. There is no dblink between the two databases, so a
       just-fetched payload is the only truthful upstream half available.
    2. It CANNOT self-grade, structurally rather than by discipline. The
       "instrument that grades its own subject" failure — a sync that upserts the
       mirror and only then measures it, reporting 0 divergence forever — is what
       ``snapshot_quota_config_values``' docstring warns about. THIS loop never
       writes ``request_quota_config`` at all: its only mutation is
       ``deactivate_orphan_license_sync_budgets``, which touches ``token_budgets``.
       So the snapshot is pre-write by construction, not by ordering luck.

    It also runs UNATTENDED on a timer, which the operator-triggered endpoint does
    not — that endpoint's own docstring records that nothing calls it on a timer,
    and a measurement that only happens when a human types it cannot take a gauge
    off -1 on a freshly rolled pod.

    ⛔ ITS OWN SESSION, not the reconcile's. The snapshot is a SELECT on the same
    engine the mutation below commits on; ``services/cost_flusher.py`` records at
    length what sharing one costs — a failed statement leaves the session needing a
    rollback and the failure resurfaces at the INSERT as a ``PendingRollbackError``
    naming the wrong statement. A separate read-only session cannot reach the
    repair transaction at all, which is a stronger guarantee than a SAVEPOINT and
    is affordable here (once per tick, not once per batch).

    ⛔ AN EMPTY UPSTREAM MAP PUBLISHES NOTHING. Leaving the gauge untouched is the
    documented contract of ``record_quota_config_drift``'s opt-in arguments: a
    caller that did not measure must not be able to overwrite a real measurement
    with a reassuring 0, and -1 must keep meaning "never measured".

    ⚠️ FAIL-SOFT, and what that costs. A drift-measurement fault must not stop the
    repair this loop exists to perform, so it is logged and swallowed. The gauge
    then keeps its previous value — -1 if it has never measured — which is exactly
    the reading an operator should act on, so the failure is not silent.

    ⚠️ LEADER-ONLY, SO THE FOLLOWER STAYS AT -1 FOREVER, and that is a property of
    the host, stated rather than left to be rediscovered on a dashboard. This loop
    is leader-elected (``LICENCE_BUDGET_RECONCILE_LOCK_KEY``), so exactly one
    replica measures per tick — the same asymmetry ``billing_licence_budget_divergent_total``
    already has. Read it as ``max(billing_quota_config_value_drift)`` across pods,
    and alert on ``> 0``. ⛔ An alert written as ``== -1`` would fire on the
    follower permanently and say nothing about the data.
    """
    if not lifecycle.plan_quota_monthly:
        logger.debug("quota_config_value_drift_not_measured", reason="no_upstream_quotas")
        return
    try:
        async with session_factory() as drift_session:
            mirror_before = await snapshot_quota_config_values(drift_session)
        record_quota_config_value_drift(
            expected_quota_monthly=lifecycle.plan_quota_monthly,
            mirror_quota_before=mirror_before,
        )
    except Exception:
        logger.warning("quota_config_value_drift_measurement_failed", exc_info=True)


async def reconcile_once(
    settings: Settings,
    session_factory: async_sessionmaker[AsyncSession],
    token_provider: Any | None,
    *,
    client: httpx.AsyncClient | None = None,
) -> tuple[int, int]:
    """One full reconcile pass. Returns ``(divergent_found, deactivated)``.

    ``divergent_found`` is measured before the mutation and is published even
    when ``licence_budget_reconcile_deactivate_enabled`` is False, so the
    detection half of this loop is useful from the moment it merges and the
    mutating half can be armed after a cycle has been watched.

    It also publishes ``billing_quota_config_value_drift`` — a SECOND, unrelated
    detector that had no caller anywhere in the service and therefore sat at its
    -1 "never measured" initial on every pod. Hosted here because this tick
    already holds both halves the comparison needs and, unlike the endpoint that
    would otherwise host it, cannot write the table it audits; see
    :func:`_measure_quota_config_value_drift`. It is measured on EVERY successful
    tick, including a measure-only one and one that later refuses on the
    deactivation cap: it is read-only and describes the mirror, not the repair.
    """
    owns_client = client is None
    http_client = client or httpx.AsyncClient(timeout=10.0)
    try:
        lifecycle = await _fetch_licence_lifecycle(settings, token_provider, client=http_client)
    finally:
        if owns_client:
            await http_client.aclose()

    if not lifecycle.protected_ids:
        # ⛔ THE PAYLOAD GUARD AT THE TOP OF ``_fetch_licence_lifecycle`` IS NOT
        # THIS GUARD, AND THE GAP BETWEEN THEM IS THE WHOLE ESTATE.
        #
        # That one refuses an empty PAYLOAD. This one refuses an empty DERIVED
        # SET, and a NON-EMPTY payload reaches it: ``protected_ids`` is empty
        # whenever not one licence in the response classifies active-or-
        # unreadable. A 200 carrying ``[{revoked}, {expired}]`` is non-empty,
        # passes the payload guard, and yields ``protected_ids == set()`` —
        # which ``deactivate_orphan_license_sync_budgets`` HONOURS, because its
        # docstring says in as many words that an empty set is legitimate and
        # "the safety gate lives at the call site, not here". This is that call
        # site. Measured on a scratch database: 3 live ``license_sync`` budgets
        # + that exact 2-element payload = ``divergent=3, deactivated=3``, the
        # entire estate gone in one 900s tick.
        #
        # ⚠️ The precedent cited when this module was written,
        # ``delete_absent_quota_configs``, gates on the SET
        # (``if not active_license_ids``) — not on the fetch that produced it.
        # Copying its payload half and not its set half is what left this open.
        #
        # An estate in which ZERO licences are active-or-unreadable is a state
        # this system has never been in, and it is what a mis-scoped query, a
        # half-migrated status column and a tenant-filtered response all look
        # like. Refuse the cycle; the next one converges if the registry is
        # really in that state and a human has looked.
        #
        # A SUBCLASS of the empty-payload path's exception, not the same class.
        # Subclass, so the failure counter, the leadership handover and the
        # freshness gauge all keep working without new wiring, and so any
        # caller that refuses to mutate on ``LicenceFetchError`` keeps refusing
        # on this. Distinct, because the two refusals have opposite diagnoses:
        # there the registry could not be read, here it read perfectly and the
        # answer was empty. Filing both under ``licences_unreadable`` -- which
        # this module did until the second audit -- points the operator at a
        # healthy licences service and leaves the real condition unnamed. The
        # loop maps this type to ``reason="no_active_licences"``.
        raise NoActiveLicencesError(
            "licences service returned no active-or-unreadable licences "
            f"(payload was non-empty: {lifecycle.non_active_count} non-active, "
            f"{lifecycle.unreadable_count} unreadable)"
        )

    await _measure_quota_config_value_drift(session_factory, lifecycle)

    deactivated_ids: list[uuid.UUID] = []
    async with session_factory() as session:
        divergent = await measure_divergence(session, protected_ids=lifecycle.protected_ids)

        observability_metrics.licence_budget_divergent_total.set(divergent)
        observability_metrics.licence_budget_reconcile_unreadable_total.set(
            lifecycle.unreadable_count
        )

        if not settings.licence_budget_reconcile_deactivate_enabled:
            # Measure-only mode. The gauges above are already published, so a
            # dry run is not a silent one.
            if divergent:
                logger.warning(
                    "licence_budget_divergence_detected_not_repaired",
                    divergent=divergent,
                    reason="licence_budget_reconcile_deactivate_enabled=false",
                )
            observability_metrics.licence_budget_reconcile_last_success_timestamp_seconds.set(
                time.time()
            )
            return divergent, 0

        cap = settings.licence_budget_reconcile_max_deactivations_per_cycle
        if divergent > cap:
            # ALL-OR-NOTHING, never a partial repair. Deactivating the first
            # `cap` rows would convert "something is badly wrong upstream" into
            # `cap` silently-unspendable budgets per tick and would still reach
            # the whole estate, just more slowly and with the evidence spread
            # across hours of logs. Refusing the cycle keeps the divergence
            # gauge pinned at the real number for a human to read.
            #
            # `divergent` is the right quantity to gate on: measure_divergence
            # uses the SAME predicate as the helper below (source='license_sync'
            # AND active AND license_id NOT IN protected_ids) against the same
            # open transaction, so it is exactly the row count the helper would
            # mutate -- known before mutating rather than counted after.
            #
            # Counter, not exception: the loop's `except Exception` would file
            # this as `unexpected`, which reads as a bug in this module. It is
            # not; it is this module working. The freshness gauge is
            # deliberately NOT advanced -- a capped cycle did not converge, so
            # `time() - last_success` must keep climbing until someone acts.
            observability_metrics.licence_budget_reconcile_failures_total.labels(
                reason="deactivation_cap_exceeded"
            ).inc()
            logger.error(
                "licence_budget_reconcile_deactivation_cap_exceeded",
                divergent=divergent,
                cap=cap,
                protected=len(lifecycle.protected_ids),
                non_active=lifecycle.non_active_count,
                unreadable=lifecycle.unreadable_count,
                action="deactivated nothing this cycle; raise "
                "licence_budget_reconcile_max_deactivations_per_cycle only after "
                "confirming the divergent count is real",
            )
            return divergent, 0

        priors = await deactivate_orphan_license_sync_budgets(
            session, active_license_ids=lifecycle.protected_ids
        )
        deactivated_ids = [prior.id for prior in priors]
        await session.commit()

    if deactivated_ids:
        logger.info(
            "licence_budget_reconcile_deactivated",
            count=len(deactivated_ids),
            budget_ids=[str(b) for b in deactivated_ids],
        )

    # Value THEN timestamp, and only on the success path: an alert reads
    # `time() - this` so a loop that stops converging ages out rather than
    # holding its last happy number forever.
    observability_metrics.licence_budget_reconcile_last_success_timestamp_seconds.set(time.time())
    return divergent, len(deactivated_ids)


async def run_licence_budget_reconcile_loop(
    settings: Settings,
    session_factory: async_sessionmaker[AsyncSession],
    token_provider: Any | None,
    stop_event: asyncio.Event,
) -> None:
    """Leader-elected periodic reconcile.

    Its own advisory lock so it can hold leadership on a different replica from
    the rollup / watcher / invoice loops. Leadership matters here even though
    the work is idempotent: two replicas racing would each pay for a full
    registry fetch and write the same audit trail twice.
    """
    direct_dsn = settings.resolve_leader_election_dsn()
    election = AsyncLeaderElection(direct_dsn, LICENCE_BUDGET_RECONCILE_LOCK_KEY)

    # A follower must never wait longer than the leader ticks, or it re-races
    # for the lock more slowly than leadership can change hands.
    retry_seconds = min(
        settings.leader_retry_interval_seconds,
        settings.licence_budget_reconcile_interval_seconds,
    )
    wait_seconds = retry_seconds
    consecutive_failures = 0

    async def _cycle_failed(reason: str) -> None:
        nonlocal consecutive_failures, wait_seconds
        consecutive_failures += 1
        observability_metrics.licence_budget_reconcile_failures_total.labels(reason=reason).inc()
        if consecutive_failures < MAX_CONSECUTIVE_RECONCILE_FAILURES:
            return
        with contextlib.suppress(Exception):
            await election.release()
        logger.warning("licence_budget_reconcile_leadership_handover", exc_info=True)
        consecutive_failures = 0
        wait_seconds = retry_seconds

    try:
        while not stop_event.is_set():
            try:
                with contextlib.suppress(TimeoutError):
                    await asyncio.wait_for(stop_event.wait(), timeout=wait_seconds)
                if stop_event.is_set():
                    break

                # Not yet proven leader for THIS cycle: anything that raises
                # below should retry soon, not park for a full interval.
                wait_seconds = retry_seconds

                leader = False
                try:
                    leader = await election.is_leader()
                finally:
                    observability_metrics.licence_budget_reconcile_is_leader.set(1 if leader else 0)
                if not leader:
                    continue

                wait_seconds = settings.licence_budget_reconcile_interval_seconds

                await reconcile_once(
                    settings,
                    session_factory,
                    token_provider,
                )
                consecutive_failures = 0

            except asyncio.CancelledError:
                break
            except NoActiveLicencesError:
                # ⚠️ MUST stay ABOVE the LicenceFetchError arm: this is a
                # SUBCLASS of it, and Python matches except-arms in source
                # order, so reordering these two silently restores the exact
                # misfiling this arm exists to correct -- with no test failure
                # unless one asserts the reason label. There is one.
                #
                # The registry read completely; what was empty is the derived
                # active-or-unreadable set. Its own reason, because
                # `licences_unreadable` means "could not be read completely"
                # and would send an operator to debug a healthy service.
                await _cycle_failed("no_active_licences")
                logger.warning("licence_budget_reconcile_no_active_licences", exc_info=True)
            except LicenceFetchError:
                # Deliberately NOT a mutation-suppressing bug: the fetch guard
                # already refused to act. Counted so an unreachable registry is
                # visible as a paused repair rather than as silence.
                await _cycle_failed("licences_unreadable")
                logger.warning("licence_budget_reconcile_fetch_failed", exc_info=True)
            except SQLAlchemyError:
                await _cycle_failed("infra_error")
                logger.warning("licence_budget_reconcile_error_infra", exc_info=True)
            except Exception:
                # Broad on purpose, same reasoning as the sibling loops: one bad
                # cycle must cost one cycle, not the loop for the process
                # lifetime.
                await _cycle_failed("unexpected")
                logger.error("licence_budget_reconcile_error", exc_info=True)
    finally:
        observability_metrics.licence_budget_reconcile_is_leader.set(0)
        with contextlib.suppress(Exception):
            await election.release()
