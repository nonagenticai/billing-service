"""Super-admin endpoints — system-wide aggregates that deliberately cross tenants.

Gated by ``require_admin`` (Keycloak ``license-admin`` role). Non-admin
callers receive 403. Distinct from ``/api/usage/*`` which scopes to a single
tenant or subject.
"""

import redis.asyncio as aioredis
from fastapi import APIRouter, Depends, HTTPException, Query, Request
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from auth_deps import require_admin, require_pricing_admin
from db import get_session, get_session_factory
from models.domain import (
    DlqDiscardRequest,
    DlqDiscardResult,
    DlqInspectResponse,
    DlqReattributeRequest,
    DlqReattributeResult,
    DlqReplayRequest,
    DlqReplayResult,
    Envelope,
    RepriceRequest,
    RepriceResult,
    SystemUsageResponse,
    envelope,
)
from redis_client import get_redis
from services.admin_aggregate import resolve_window, system_usage
from services.audit import record as audit_record
from services.dlq_ops import (
    REPLAY_OVERRIDE_FIELDS,
    ReattributeTargetError,
    discard_dlq,
    inspect_dlq,
    reattribute_dlq,
    replay_dlq,
)
from services.reprice import reprice_null_costs
from settings import get_settings

router = APIRouter(prefix="/api/admin", tags=["internal"])


@router.get(
    "/usage/system",
    response_model=Envelope[SystemUsageResponse],
    operation_id="get_admin_system_usage",
    summary="System-wide usage + cost aggregate (super-admin only)",
)
async def admin_usage_system(
    request: Request,
    window: str = Query(
        default="month",
        description=(
            "Window keyword: 'today' (today only), 'month' (1st of this "
            "month through today), or 'Nd' (last N days, 1-365)."
        ),
    ),
    session: AsyncSession = Depends(get_session),
    _admin: dict = Depends(require_admin),
) -> dict:
    """Aggregate usage + cost across all tenants.

    Returns grand totals (incurred + billed), per-tenant rows (incurred,
    markup, billed) sorted by incurred spend desc, and per-provider rows
    (incurred-only) sorted by spend desc.
    """
    try:
        resolve_window(window)
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc

    settings = get_settings()
    payload = await system_usage(session, settings, window)
    return envelope(payload, request)


@router.post(
    "/reprice",
    response_model=Envelope[RepriceResult],
    operation_id="reprice_null_cost_events",
    summary="Backfill cost_usd for events flushed during a pricing gap",
)
async def admin_reprice(
    request: Request,
    body: RepriceRequest,
    session: AsyncSession = Depends(get_session),
    session_factory: async_sessionmaker[AsyncSession] = Depends(get_session_factory),
    _admin: dict = Depends(require_pricing_admin),
) -> dict:
    """Re-resolve pricing for ``cost_usd IS NULL`` events in ``[since, until)``.

    Events flushed before their provider's ``provider_pricing`` row existed are
    written with a NULL cost that nothing ever recomputes. This endpoint replays
    the pricing lookup for those rows and, on a real run, restates the affected
    ``billing_cost_daily`` buckets so the rollup matches the raw table again.

    ``dry_run`` defaults to **true** — the operator must opt in to writing money
    data. Already-priced events are never touched, so re-running is a no-op.
    """
    try:
        result = await reprice_null_costs(
            session_factory,
            since=body.since,
            until=body.until,
            provider=body.provider,
            limit=body.limit,
            dry_run=body.dry_run,
        )
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc

    # Exactly one audit row per invocation, dry runs included — "who looked" is
    # as much a part of the money trail as "who wrote". `before` is the state of
    # the rows we touched: they were all cost_usd IS NULL, i.e. contributing
    # exactly $0 to every report, which is the whole bug. Recorded on the
    # request-scoped session AFTER the reprice (which owns its own sessions and
    # has already committed); a failure here cannot roll the backfill back, so
    # the log line in services.reprice is the backstop.
    await audit_record(
        session,
        request,
        action="admin.reprice",
        resource_type="cost_events",
        resource_id=None,
        tenant_id=None,
        before={
            "since": body.since.isoformat(),
            "until": body.until.isoformat(),
            "provider": body.provider,
            "dry_run": body.dry_run,
            "null_cost_events_scanned": result.scanned,
            "total_cost_usd": "0",
        },
        after={
            "repriced": result.repriced,
            "still_unpriced": result.still_unpriced,
            "total_cost_usd": str(result.total_cost_usd_added),
            "affected_bucket_dates": [str(d) for d in result.affected_bucket_dates],
            "restated_bucket_dates": [str(d) for d in result.restated_bucket_dates],
        },
    )
    await session.commit()
    return envelope(result, request)


@router.get(
    "/dlq/inspect",
    response_model=Envelope[DlqInspectResponse],
    operation_id="admin_dlq_inspect",
    summary="Inspect quarantined cost events on the billing:costs:dead stream",
)
async def admin_dlq_inspect(
    request: Request,
    reason: str | None = Query(
        default=None,
        description="Filter to one _dlq_reason (e.g. 'missing_provider'). None = all.",
    ),
    limit: int = Query(default=100, ge=1, le=500, description="Max entries to return (cap 500)."),
    redis_conn: aioredis.Redis | None = Depends(get_redis),
    _admin: dict = Depends(require_pricing_admin),
) -> dict:
    """Read-only page of the dead-letter stream — the first half of the
    BillingCostDeadLetter runbook. Splits each entry into its business fields and
    the ``_dlq_*`` bookkeeping so an operator can see *why* it was quarantined
    before deciding to replay.
    """
    if redis_conn is None:
        raise HTTPException(status_code=503, detail="redis unavailable")
    result = await inspect_dlq(redis_conn, reason=reason, limit=limit)
    return envelope(result, request)


@router.post(
    "/dlq/replay",
    response_model=Envelope[DlqReplayResult],
    operation_id="admin_dlq_replay",
    summary="Replay dead-lettered cost events back onto billing:costs",
)
async def admin_dlq_replay(
    request: Request,
    body: DlqReplayRequest,
    redis_conn: aioredis.Redis | None = Depends(get_redis),
    session: AsyncSession = Depends(get_session),
    _admin: dict = Depends(require_pricing_admin),
) -> dict:
    """Re-inject the requested dead-letter ids onto the live cost stream — the
    second half of the runbook, run once the malformed condition is fixed.

    Safe to run under pressure: each id is idempotency-claimed before any XADD,
    an entry that still fails validation is left quarantined (never looped back),
    and one bad entry never aborts the batch. Exactly one audit row per
    invocation records who replayed what and the outcome.

    ``overrides`` is the drain for a zero-cost entry with a REAL tenant that the
    emitter wrote without a mandatory contract field (typically ``provider``):
    discard refuses it as ``tenant_attributable`` and plain replay re-rejects
    the stored payload. It is confined to ONE id, to allowlisted business
    fields, and requires a ``reason``; all three are recorded in the audit row.
    """
    if redis_conn is None:
        raise HTTPException(status_code=503, detail="redis unavailable")

    if body.overrides:
        if len(body.ids) != 1:
            raise HTTPException(
                status_code=422,
                detail="overrides apply to exactly one id; split the request",
            )
        unknown = sorted(set(body.overrides) - REPLAY_OVERRIDE_FIELDS)
        if unknown:
            raise HTTPException(
                status_code=422,
                detail=(
                    f"overrides may not set {unknown}; allowed: {sorted(REPLAY_OVERRIDE_FIELDS)}"
                ),
            )
        if body.reason is None or not body.reason.strip():
            raise HTTPException(
                status_code=422,
                detail="overrides require a non-blank reason (recorded in the audit row)",
            )

    try:
        result = await replay_dlq(
            redis_conn,
            ids=body.ids,
            delete_on_success=body.delete_on_success,
            overrides=body.overrides,
        )
    except ValueError as exc:
        # The route above validates the allowlist; the service re-checks so a
        # direct caller cannot bypass it. Surface it as a request error, not 500.
        raise HTTPException(status_code=422, detail=str(exc)) from exc

    await audit_record(
        session,
        request,
        action="admin.dlq_replay",
        resource_type="cost_dead_letter",
        resource_id=None,
        tenant_id=None,
        before={
            "requested_ids": body.ids,
            "delete_on_success": body.delete_on_success,
            "overrides": body.overrides,
            "reason": body.reason,
        },
        after={
            "replayed": result.replayed,
            "already_processed": result.already_processed,
            "still_malformed": result.still_malformed,
            "deleted_from_dlq": result.deleted_from_dlq,
        },
    )
    await session.commit()
    return envelope(result, request)


@router.post(
    "/dlq/discard",
    response_model=Envelope[DlqDiscardResult],
    operation_id="admin_dlq_discard",
    summary="Discard undrainable zero-cost, unattributable dead-letter entries",
)
async def admin_dlq_discard(
    request: Request,
    body: DlqDiscardRequest,
    redis_conn: aioredis.Redis | None = Depends(get_redis),
    session: AsyncSession = Depends(get_session),
    _admin: dict = Depends(require_pricing_admin),
) -> dict:
    """Permanently drop dead-letter entries that no replay can ever drain.

    The escape hatch for the third state the runbook has no move for: an entry
    that is neither fixable nor replayable, because the malformed condition is
    IN the stored payload. ``nil_tenant_id`` is the case — ``replay_dlq``
    re-injects the stored fields, ``validate_cost_event`` re-rejects them
    identically, and the entry re-quarantines forever while
    BillingCostDeadLetterBacklog fires.

    ``confirm`` defaults to **false** — a plain call is a DRY RUN reporting what
    would be discarded, and deletes nothing. An entry is only ever eligible when
    it is BOTH ``cost_usd == 0`` AND unattributable (absent or nil tenant);
    anything else is refused and counted, never deleted. See
    :func:`services.dlq_ops.discard_dlq` for the deletion principle this excepts
    and why these entries are the exception.

    One audit row per invocation, dry runs included.
    """
    if redis_conn is None:
        raise HTTPException(status_code=503, detail="redis unavailable")

    result = await discard_dlq(
        redis_conn,
        ids=body.ids,
        reason=body.reason,
        confirm=body.confirm,
    )

    # Audited on EVERY invocation, dry runs included — same rule as
    # /admin/reprice: "who looked" is part of the trail. This matters more here
    # than anywhere else in the service, because the discarded entries do not
    # survive to be re-read: this row plus the per-entry ``dlq_entry_discarded``
    # log lines are the whole record that they ever existed.
    await audit_record(
        session,
        request,
        action="admin.dlq_discard",
        resource_type="cost_dead_letter",
        resource_id=None,
        tenant_id=None,
        before={
            "requested_ids": body.ids,
            "reason": body.reason,
            "confirm": body.confirm,
        },
        after={
            "examined": result.examined,
            "eligible": result.eligible,
            "discarded": result.discarded,
            "refused": result.refused,
        },
    )
    await session.commit()
    return envelope(result, request)


@router.post(
    "/dlq/reattribute",
    response_model=Envelope[DlqReattributeResult],
    operation_id="admin_dlq_reattribute",
    summary="Re-attribute nil/blank-tenant dead-letter cost events to a registered tenant",
)
async def admin_dlq_reattribute(
    request: Request,
    body: DlqReattributeRequest,
    redis_conn: aioredis.Redis | None = Depends(get_redis),
    session: AsyncSession = Depends(get_session),
    _admin: dict = Depends(require_admin),
) -> dict:
    """Bill a quarantined nil/blank-tenant cost event to an operator-named tenant.

    The exit for the entry neither replay nor discard can take (#243): a
    ``nil_tenant_id`` event with ``cost_usd != 0``. Only entries whose STORED
    tenant is nil or blank are accepted; the target must be a registered,
    non-nil tenant (400 otherwise). The rewritten event goes through replay's
    own marker claim, validation and atomic re-inject, so it is billed at most
    once, and a Postgres receipt is committed before the entry is XDELed.

    ``confirm`` defaults to **false** — a plain call is a DRY RUN. Gated on
    ``require_admin`` (license-admin), NOT ``require_pricing_admin``: this
    decides which customer is charged, and the ``billing-internal`` service
    account must not be able to do that (same reasoning as
    ``require_pricing_writer``). One audit row per invocation, dry runs included.
    """
    if redis_conn is None:
        raise HTTPException(status_code=503, detail="redis unavailable")

    actor = getattr(request.state, "audit_actor", None) or {}
    try:
        result = await reattribute_dlq(
            redis_conn,
            session,
            ids=body.ids,
            tenant_id=body.tenant_id,
            reason=body.reason,
            reattributed_by=actor.get("sub") or "unknown",
            request_id=getattr(request.state, "request_id", None),
            confirm=body.confirm,
        )
    except ReattributeTargetError as exc:
        raise HTTPException(status_code=400, detail=exc.code) from exc

    await audit_record(
        session,
        request,
        action="admin.dlq_reattribute",
        resource_type="cost_dead_letter",
        resource_id=None,
        tenant_id=result.tenant_id,
        before={
            "requested_ids": body.ids,
            "tenant_id": body.tenant_id,
            "reason": body.reason,
            "confirm": body.confirm,
        },
        after={
            "examined": result.examined,
            "eligible": result.eligible,
            "reattributed": result.reattributed,
            "already_processed": result.already_processed,
            "still_malformed": result.still_malformed,
            "refused": result.refused,
            "receipt_failed": result.receipt_failed,
            "deleted_from_dlq": result.deleted_from_dlq,
        },
    )
    await session.commit()
    return envelope(result, request)
