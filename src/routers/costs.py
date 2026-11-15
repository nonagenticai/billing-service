"""Cost query endpoints."""

import uuid
from datetime import UTC, date, datetime, timedelta
from decimal import ROUND_HALF_UP, Decimal

from fastapi import APIRouter, Depends, HTTPException, Query, Request
from sqlalchemy import ColumnElement, Date, func, select
from sqlalchemy.ext.asyncio import AsyncSession

from auth_deps import (
    enforce_tenant_isolation,
    get_subject_id,
    get_tenant_name,
    require_admin,
    require_service_caller,
    require_tenant,
    resolve_tenant_or_400,
)
from db import get_session
from models.db import NULL_PROJECT_ID, CostActorDaily, CostDaily, CostEvent
from models.domain import (
    ActorCostLine,
    ActorCostResponse,
    CorrelationCostGroup,
    CorrelationCostResponse,
    CostResponse,
    CostSummary,
    Envelope,
    ProjectCardCost,
    ProjectCostDay,
    ProjectCostProvisional,
    ProjectCostResponse,
    ProjectCostTotals,
    ProvisionalCostResponse,
    envelope,
)
from routers._aggregation import aggregate_by_project, aggregate_by_provider
from routers.export import MAX_WINDOW_DAYS
from routers.usage import active_tenant_or_400
from services.rollup_engine import cost_event_time
from timeutil import default_end, default_start

router = APIRouter(prefix="/api/costs", tags=["costs"])

# Bounds for the per-correlation read so a caller can't request an unbounded scan.
_MAX_CORRELATION_IDS = 500
_MAX_CORRELATION_ROWS = 50_000
# Dimensions a caller may group the per-correlation cost by.
_CORRELATION_GROUP_DIMS = {"stage", "model", "provider", "correlation_id", "day"}


def _validate_window(start: datetime, end: datetime) -> None:
    """Reject an inverted or oversized [start, end] window.

    Mirrors the semantics of ``routers.export._parse_window`` (``end`` must be
    strictly after ``start``; span capped at the same ``MAX_WINDOW_DAYS``), but
    the caller here always supplies concrete start/end (both defaulted) rather
    than optional since/until. Without this an inverted range silently returns
    an empty 200 and a huge range triggers a full scan. Raised as 422 (invalid
    query params) — export uses 400; the discrepancy is intentional and noted.
    """
    if end <= start:
        raise HTTPException(status_code=422, detail="end must be after start")
    if (end - start) > timedelta(days=MAX_WINDOW_DAYS):
        raise HTTPException(
            status_code=422,
            detail=f"window > {MAX_WINDOW_DAYS} days; narrow the time range",
        )


@router.get(
    "/tenant/{tenant_id}",
    response_model=Envelope[CostResponse],
    operation_id="get_costs_by_tenant",
)
async def costs_by_tenant(
    request: Request,
    tenant_id: str,
    start: datetime = Query(default_factory=default_start),
    end: datetime = Query(default_factory=default_end),
    provider: str | None = Query(default=None),
    subject_id: uuid.UUID | None = Query(
        default=None,
        description="Optional: narrow to a single end-user (Subject UUID).",
    ),
    session: AsyncSession = Depends(get_session),
    _tenant: str = Depends(require_tenant),
    _tenant_name: str = Depends(get_tenant_name),
) -> dict:
    """Get internal costs for a tenant."""
    _validate_window(start, end)
    enforce_tenant_isolation(request, tenant_id, _tenant)
    # Compare Date to date, not datetime — postgres casts Date→timestamptz using
    # the session TZ (Europe/Amsterdam on CI), so `bucket_date >= utc_dt` drops
    # today's row whenever local-vs-UTC date differs.
    filters = [
        CostDaily.tenant_id == tenant_id,
        CostDaily.bucket_date >= start.date(),
        CostDaily.bucket_date <= end.date(),
    ]
    if provider:
        filters.append(CostDaily.provider == provider)
    if subject_id is not None:
        filters.append(CostDaily.subject_id == subject_id)

    # Same `filters` list to both helpers — by_provider and by_project are two
    # decompositions of ONE row set, so each independently sums to the totals
    # below. Passing a narrowed filter list to either would put two populations
    # in one response with nothing saying so.
    by_provider = await aggregate_by_provider(session, filters)
    by_project = await aggregate_by_project(session, filters)

    return envelope(
        CostResponse(
            tenant_id=tenant_id,
            tenant_name=_tenant_name or None,
            start=start,
            end=end,
            total_cost_usd=sum((p.cost_usd for p in by_provider), Decimal("0")),
            total_input_tokens=sum(p.input_tokens for p in by_provider),
            total_output_tokens=sum(p.output_tokens for p in by_provider),
            by_provider=by_provider,
            by_project=by_project,
        ),
        request,
    )


@router.get(
    "/provider",
    response_model=Envelope[CostResponse],
    operation_id="get_costs_by_provider",
)
async def costs_by_provider(
    request: Request,
    start: datetime = Query(default_factory=default_start),
    end: datetime = Query(default_factory=default_end),
    session: AsyncSession = Depends(get_session),
    _user: dict = Depends(require_admin),
) -> dict:
    """Get costs aggregated by provider (operator view)."""
    _validate_window(start, end)
    # Hoisted to one list so both axes are computed over literally the same
    # predicate — two inline copies are two chances for one to drift.
    filters: list[ColumnElement[bool]] = [
        CostDaily.bucket_date >= start.date(),
        CostDaily.bucket_date <= end.date(),
    ]
    by_provider = await aggregate_by_provider(session, filters)
    by_project = await aggregate_by_project(session, filters)

    return envelope(
        CostResponse(
            start=start,
            end=end,
            total_cost_usd=sum((p.cost_usd for p in by_provider), Decimal("0")),
            total_input_tokens=sum(p.input_tokens for p in by_provider),
            total_output_tokens=sum(p.output_tokens for p in by_provider),
            by_provider=by_provider,
            by_project=by_project,
        ),
        request,
    )


@router.get(
    "/summary",
    response_model=Envelope[CostSummary],
    operation_id="get_costs_summary",
)
async def costs_summary(
    request: Request,
    start: datetime = Query(default_factory=default_start),
    end: datetime = Query(default_factory=default_end),
    session: AsyncSession = Depends(get_session),
    _user: dict = Depends(require_admin),
) -> dict:
    """Get total platform cost summary."""
    _validate_window(start, end)
    filters: list[ColumnElement[bool]] = [
        CostDaily.bucket_date >= start.date(),
        CostDaily.bucket_date <= end.date(),
    ]
    by_provider = await aggregate_by_provider(session, filters)
    by_project = await aggregate_by_project(session, filters)

    return envelope(
        CostSummary(
            start=start,
            end=end,
            total_internal_cost_usd=sum((p.cost_usd for p in by_provider), Decimal("0")),
            total_input_tokens=sum(p.input_tokens for p in by_provider),
            total_output_tokens=sum(p.output_tokens for p in by_provider),
            by_provider=by_provider,
            by_project=by_project,
        ),
        request,
    )


async def _costs_for_subject(
    session: AsyncSession,
    subject_id: uuid.UUID,
    start: datetime,
    end: datetime,
    tenant_id: str | None = None,
    provider: str | None = None,
) -> CostResponse:
    """Aggregate internal cost for one subject across all licenses + tenants."""
    _validate_window(start, end)
    filters = [
        CostDaily.subject_id == subject_id,
        CostDaily.bucket_date >= start.date(),
        CostDaily.bucket_date <= end.date(),
    ]
    if tenant_id is not None:
        filters.append(CostDaily.tenant_id == tenant_id)
    if provider is not None:
        filters.append(CostDaily.provider == provider)

    by_provider = await aggregate_by_provider(session, filters)
    by_project = await aggregate_by_project(session, filters)

    return CostResponse(
        tenant_id=tenant_id,
        start=start,
        end=end,
        total_cost_usd=sum((p.cost_usd for p in by_provider), Decimal("0")),
        total_input_tokens=sum(p.input_tokens for p in by_provider),
        total_output_tokens=sum(p.output_tokens for p in by_provider),
        by_provider=by_provider,
        by_project=by_project,
    )


@router.get(
    "/user/{subject_id}",
    response_model=Envelope[CostResponse],
    operation_id="get_costs_by_user",
)
async def costs_by_user(
    request: Request,
    subject_id: uuid.UUID,
    start: datetime = Query(default_factory=default_start),
    end: datetime = Query(default_factory=default_end),
    tenant_id: str | None = Query(
        default=None,
        description="Optional: narrow to one tenant. Default sums across all tenants.",
    ),
    provider: str | None = Query(default=None),
    session: AsyncSession = Depends(get_session),
    _user: dict = Depends(require_admin),
) -> dict:
    """Get LLM cost attributed to one end-user (Subject), admin-scoped.

    ⚠️ NOT tenant-scoped by default. `tenant_id` is OPTIONAL here and
    `_costs_for_subject` applies the predicate only `if tenant_id is not None`,
    so the default sums the subject's spend across EVERY tenant they hold a
    licence in — deliberate for an operator view, but it makes this a THIRD
    call site (with `/provider` and `/summary`) where a project uuid asserted by
    two tenants would have merged. `routers._aggregation.aggregate_by_project`
    groups by `(tenant_id, project_id)` so it cannot, whatever the caller passes.
    `/me` is the contrasting case: it ALWAYS passes `active_tenant_or_400(...)`,
    so it is genuinely tenant-scoped and needed no change.
    """
    payload = await _costs_for_subject(session, subject_id, start, end, tenant_id, provider)
    return envelope(payload, request)


@router.get(
    "/tenant/{tenant_id}/actors",
    response_model=Envelope[ActorCostResponse],
    operation_id="get_costs_by_actor",
)
async def costs_by_actor(
    request: Request,
    tenant_id: str,
    start: datetime = Query(default_factory=default_start),
    end: datetime = Query(default_factory=default_end),
    session: AsyncSession = Depends(get_session),
    _service: dict = Depends(require_service_caller),
) -> dict:
    """Per-ACTOR cost for a tenant — who initiated the spend, not whose licence paid.

    ⭐ Reads ``billing_cost_actor_daily`` (alembic 0040), which survives raw-event retention. The
    existing ``/user/{subject_id}`` and ``/me`` filter ``subject_id`` (WHO PAYS) and are left
    unchanged; this is the WHO-ACTED view per-user chargeback needs.

    ⛔ SERVICE CALLERS ONLY, AND THAT IS A DELIBERATE NARROWING OF WHAT SHIPPED.
    This route was guarded by ``require_tenant`` + ``enforce_tenant_isolation``,
    which together prove only "the caller belongs to the tenant they asked
    about". That is the right gate for the tenant AGGREGATE reads above it and
    the WRONG one here: ``require_tenant`` accepts the ``X-Tenant-Id`` header
    license-authz injects after validating a LICENCE token, and every member of
    a tenant holds one — so under the old gate any seat could enumerate their
    colleagues' individual spend. Per-member cost is admin data.
    :func:`auth_deps.require_service_caller` is the Keycloak service-account JWT
    path (``require_internal``'s role set, verified through the same JWKS), and
    CP's backend proxies this route for its own tenant admins.

    ⚠️ ``/costs/me`` AND ``/costs/user/{subject_id}`` ARE UNTOUCHED. fr reads
    both. ``/me`` is self-service and already scoped to the caller's own subject
    + active tenant; ``/user/{id}`` is already ``require_admin``. Neither is
    widened or narrowed here, and the ``subject_id`` (WHO PAYS) semantics they
    filter on are a different axis from this route's ``actor_id`` (WHO ACTED).
    """
    _validate_window(start, end)
    # ⛔ The path tenant is now caller-supplied with no dependency having
    # resolved it: ``require_tenant`` used to canonicalise the header AND hand
    # back a uuid, and dropping it also dropped that parse. Without this a
    # registered tenant NAME — or any non-uuid — reaches a Postgres ``uuid``
    # column and the caller is told 503 DATABASE_UNAVAILABLE for their own bad
    # input. ``resolve_tenant_or_400`` is the repo's own helper for exactly this
    # "a tenant id that did NOT arrive in the header" case and raises the same
    # 400 shape the header path does.
    tenant_id = resolve_tenant_or_400(tenant_id)
    # Belt-and-braces, and NOT redundant with the gate above. A service caller
    # proxying for tenant A may legitimately send ``X-Tenant-Id: A``; this stops
    # it then reading tenant B's per-member breakdown under that assertion. When
    # no header is present (an operator's admin SA token) the helper returns
    # immediately and the caller may read any tenant, exactly as ``/user/{id}``
    # and ``/provider`` already allow.
    header_tenant = request.headers.get("x-tenant-id") or ""
    enforce_tenant_isolation(request, tenant_id, header_tenant)
    rows = (
        await session.execute(
            select(
                CostActorDaily.actor_id,
                CostActorDaily.byo,
                func.sum(CostActorDaily.total_cost_usd).label("cost"),
                func.sum(CostActorDaily.total_vendor_cost_usd).label("vendor_cost"),
                func.sum(CostActorDaily.total_input_tokens).label("inp"),
                func.sum(CostActorDaily.total_output_tokens).label("outp"),
                func.sum(CostActorDaily.request_count).label("cnt"),
            )
            .where(
                CostActorDaily.tenant_id == tenant_id,
                CostActorDaily.bucket_date >= start.date(),
                CostActorDaily.bucket_date <= end.date(),
            )
            .group_by(CostActorDaily.actor_id, CostActorDaily.byo)
            .order_by(CostActorDaily.actor_id, CostActorDaily.byo)
        )
    ).all()
    lines = [
        ActorCostLine(
            actor_id=str(r.actor_id),
            byo=bool(r.byo),
            cost_usd=Decimal(r.cost or 0),
            vendor_cost_usd=None if r.vendor_cost is None else Decimal(r.vendor_cost),
            input_tokens=int(r.inp or 0),
            output_tokens=int(r.outp or 0),
            request_count=int(r.cnt or 0),
        )
        for r in rows
    ]
    return envelope(
        ActorCostResponse(
            tenant_id=tenant_id,
            start=start,
            end=end,
            total_cost_usd=sum((line.cost_usd for line in lines), Decimal("0")),
            by_actor=lines,
        ),
        request,
    )


@router.get(
    "/me",
    response_model=Envelope[CostResponse],
    operation_id="get_my_costs",
)
async def costs_me(
    request: Request,
    start: datetime = Query(default_factory=default_start),
    end: datetime = Query(default_factory=default_end),
    provider: str | None = Query(default=None),
    session: AsyncSession = Depends(get_session),
    subject_id: uuid.UUID = Depends(get_subject_id),
) -> dict:
    """Return the caller's own LLM cost in the ACTIVE tenant (X-Subject-Id + X-Tenant-Id)."""
    # `tenant_id` must be passed explicitly: `_costs_for_subject` applies the
    # predicate only `if tenant_id is not None`, so omitting it — which is what
    # this route did — summed the caller's spend across EVERY tenant they hold a
    # licence in and presented the total as this workspace's bill. Money, not just
    # a count: the figure a customer reads here was another tenant's spend too.
    # The helper lives in `routers.usage` because all four `/me` reads must refuse
    # on exactly the same condition with exactly the same 400; two copies of this
    # guard is two chances for one of them to drift open.
    tenant_id = active_tenant_or_400(request)
    payload = await _costs_for_subject(
        session, subject_id, start, end, tenant_id=tenant_id, provider=provider
    )
    return envelope(payload, request)


def _group_key(event: CostEvent, dims: list[str]) -> tuple[tuple[str, str], ...]:
    """Build the ordered group key for one cost event over the requested dims."""
    parts: list[tuple[str, str]] = []
    for dim in dims:
        if dim == "stage":
            value = (event.extra_metadata or {}).get("stage", "") or ""
        elif dim == "model":
            value = event.served_model or event.model or ""
        elif dim == "provider":
            value = event.provider or ""
        elif dim == "correlation_id":
            value = str(event.correlation_id) if event.correlation_id else ""
        elif dim == "day":
            # UTC day, not the session-local day. psycopg3 returns a timestamptz
            # as a datetime whose tzinfo is the DB session TimeZone (prod's
            # pgbouncer session is Europe/Amsterdam), so a bare `.date()` would
            # group a 23:30-UTC event under the next Amsterdam day — the same
            # boundary bug the rollup casts fix. Normalize to UTC first.
            value = event.created_at.astimezone(UTC).date().isoformat() if event.created_at else ""
        else:  # pragma: no cover - dims validated by caller
            value = ""
        parts.append((dim, value))
    return tuple(parts)


@router.get(
    "/by-correlation",
    response_model=Envelope[CorrelationCostResponse],
    operation_id="get_costs_by_correlation",
)
async def costs_by_correlation(
    request: Request,
    ids: str = Query(
        ...,
        description="Comma-separated correlation UUIDs (e.g. w2v run_ids). Max 500.",
    ),
    group_by: str = Query(
        default="",
        description="Comma-separated dims to group by: stage, model, provider, "
        "correlation_id, day. Empty = single all-up total.",
    ),
    session: AsyncSession = Depends(get_session),
    _tenant: str = Depends(require_tenant),
) -> dict:
    """Priced cost for one or more correlation ids, grouped by the requested dims.

    Reads raw ``cost_events`` (correlation_id is indexed) so cost can be sliced by
    the caller's operation (run/stage/model) — the per-tenant ``CostDaily`` rollup
    cannot do this. Tenant-scoped: a customer caller (X-Tenant-Id present) only
    sees its own events; an admin JWT caller sees all.
    """
    correlation_ids: list[uuid.UUID] = []
    for raw in (s.strip() for s in ids.split(",")):
        if not raw:
            continue
        try:
            correlation_ids.append(uuid.UUID(raw))
        except ValueError as exc:
            raise HTTPException(status_code=422, detail=f"Invalid correlation id: {raw}") from exc
    if not correlation_ids:
        raise HTTPException(status_code=422, detail="At least one correlation id is required")
    if len(correlation_ids) > _MAX_CORRELATION_IDS:
        raise HTTPException(
            status_code=422,
            detail=f"Too many correlation ids (max {_MAX_CORRELATION_IDS})",
        )

    dims: list[str] = []
    for raw in (s.strip() for s in group_by.split(",")):
        if not raw:
            continue
        if raw not in _CORRELATION_GROUP_DIMS:
            raise HTTPException(status_code=422, detail=f"Invalid group_by dimension: {raw}")
        if raw not in dims:
            dims.append(raw)

    filters: list[ColumnElement[bool]] = [
        CostEvent.correlation_id.in_(correlation_ids),
        # Per-correlation is exactly the view where a double-metered call showed
        # up as 2x. Parked duplicates are excluded from billing, so they are
        # excluded from the number this endpoint reports as cost.
        CostEvent.excluded_from_billing.is_(False),
    ]
    # Tenant isolation: a customer caller (X-Tenant-Id present) is restricted to
    # its own events; an admin-JWT caller (no header) may read across tenants.
    if request.headers.get("x-tenant-id"):
        filters.append(CostEvent.tenant_id == _tenant)

    # Fetch one more than the cap so a full page is unambiguously "truncated"
    # rather than "happened to land exactly on the cap".
    rows = (
        (await session.execute(select(CostEvent).where(*filters).limit(_MAX_CORRELATION_ROWS + 1)))
        .scalars()
        .all()
    )
    # Silent truncation would report a money total LOWER than reality with a 200,
    # which a caller can't distinguish from the real cost. Refuse instead so the
    # caller narrows the id set or time window rather than trusting a partial sum.
    if len(rows) > _MAX_CORRELATION_ROWS:
        raise HTTPException(
            status_code=400,
            detail=(
                f"result set exceeds {_MAX_CORRELATION_ROWS} rows; narrow the "
                "correlation id set or the time window"
            ),
        )

    total_cost = Decimal("0")
    total_in = 0
    total_out = 0
    groups: dict[tuple[tuple[str, str], ...], CorrelationCostGroup] = {}
    for ev in rows:
        # cost_usd is NULL for events with no matching ProviderPricing row — count
        # the tokens but contribute $0 (honest: unpriced ≠ free, just unknown).
        cost = ev.cost_usd or Decimal("0")
        total_cost += cost
        total_in += ev.input_tokens or 0
        total_out += ev.output_tokens or 0
        key = _group_key(ev, dims)
        grp = groups.get(key)
        if grp is None:
            grp = CorrelationCostGroup(key=dict(key))
            groups[key] = grp
        grp.cost_usd += cost
        grp.input_tokens += ev.input_tokens or 0
        grp.output_tokens += ev.output_tokens or 0
        grp.event_count += 1

    return envelope(
        CorrelationCostResponse(
            correlation_ids=[str(c) for c in correlation_ids],
            group_by=dims,
            total_cost_usd=total_cost,
            total_input_tokens=total_in,
            total_output_tokens=total_out,
            event_count=len(rows),
            groups=list(groups.values()),
        ),
        request,
    )


#: What ``cost`` means on the per-project read. Stated in the response so a
#: reader never has to guess whether it is a price, an invoice line or a vendor
#: bill: it is neither of the last two.
PROJECT_COST_BASIS = (
    "internal_cost_usd: billing_cost_events.cost_usd priced from provider_pricing "
    "at event time (pre-markup booked cost, not the customer price); "
    "settled = billing_cost_daily (UTC days <= rollup watermark), "
    "provisional = raw events on UTC days after the watermark, excluded_from_billing rows dropped"
)


#: Output scale of every cost on the per-project read: ``billing_cost_daily.total_cost_usd``
#: is ``Numeric(20, 6)``, which is what ``/api/costs/tenant/*`` serializes. Raw events are
#: ``Numeric(20, 8)``; without this the settled and provisional halves of ONE response
#: would carry different scales.
_USD_QUANTUM = Decimal("0.000001")


def _usd(value: Decimal) -> Decimal:
    """Quantize a cost to the rollup's 6-dp scale — OUTPUT EDGE ONLY, never before summing.

    ROUND_HALF_UP (away from zero) is what Postgres applies when ``rollup_daily``
    casts a ``Numeric(20, 8)`` sum into the ``Numeric(20, 6)`` column, so a
    provisional figure rounds the way it will once settled.
    """
    return value.quantize(_USD_QUANTUM, rounding=ROUND_HALF_UP)


def _event_day() -> ColumnElement:
    """UTC day of a raw cost event — the SAME expression ``rollup_daily`` buckets on."""
    return func.cast(func.timezone("UTC", cost_event_time), Date)


def _is_tenant_uuid(project_id: uuid.UUID, tenant_id: str) -> bool:
    try:
        return uuid.UUID(tenant_id) == project_id
    except ValueError:
        return False


def _raw_cost_filters(tenant_id: str, *scope: ColumnElement[bool]) -> list[ColumnElement[bool]]:
    """Raw-event predicates every cost read shares: tenant, caller scope, billable rows only."""
    return [
        CostEvent.tenant_id == tenant_id,
        *scope,
        CostEvent.excluded_from_billing.is_(False),
    ]


async def _rollup_watermark(session: AsyncSession) -> date | None:
    """The GLOBAL rollup watermark: last UTC day ``billing_cost_daily`` covers."""
    return (await session.execute(select(func.max(CostDaily.bucket_date)))).scalar()


async def _provisional_cost(
    session: AsyncSession, watermark: date | None, raw_filters: list[ColumnElement[bool]]
) -> tuple[ProjectCostProvisional, Decimal]:
    """Raw cost AFTER the global rollup watermark — the one provisional implementation.

    Returns ``(provisional, full_precision_cost)``. ``watermark`` comes from
    :func:`_rollup_watermark`, read by the caller BEFORE any other query so the
    split point is fixed for the whole request. It is the
    GLOBAL ``max(billing_cost_daily.bucket_date)`` — the same predicate
    ``unrolled_guard`` treats as "rolled up" — and raw events are bucketed by the
    rollup's own ``cost_event_time`` expression, so a day is never counted from
    both the rollup and the raw table. ``watermark is None`` (nothing rolled up
    yet) means every retained raw event is provisional. The full-precision cost
    is returned so callers that add it to a settled figure quantize only once.
    """
    since = None
    prov_filters = list(raw_filters)
    if watermark is not None:
        since = watermark + timedelta(days=1)
        prov_filters += [
            # Sargable partition-prune bound, sound by the same invariant
            # rollup_daily relies on (event_ts <= created_at for every stored row).
            CostEvent.created_at >= datetime(since.year, since.month, since.day, tzinfo=UTC),
            _event_day() >= since,
        ]
    prov = (
        await session.execute(
            select(
                func.sum(CostEvent.cost_usd).label("cost"),
                func.count().label("cnt"),
                func.sum(CostEvent.input_tokens).label("inp"),
                func.sum(CostEvent.output_tokens).label("outp"),
            ).where(*prov_filters)
        )
    ).one()
    # cost_usd NULL = pricing miss: counted as a call, contributes $0 (unknown, not free).
    prov_cost = Decimal(prov.cost or 0)
    provisional = ProjectCostProvisional(
        since=since,
        cost=_usd(prov_cost),
        calls=int(prov.cnt or 0),
        tokens_in=int(prov.inp or 0),
        tokens_out=int(prov.outp or 0),
    )
    return provisional, prov_cost


@router.get(
    "/projects/{project_id}",
    response_model=Envelope[ProjectCostResponse],
    operation_id="get_costs_by_project",
)
async def costs_by_project(
    request: Request,
    project_id: uuid.UUID,
    session: AsyncSession = Depends(get_session),
    _tenant: str = Depends(require_tenant),
) -> dict:
    """Settled (rollup) + provisional (raw, same-day) cost for ONE project of the caller's tenant.

    Tenant scope: the tenant comes from ``require_tenant`` (the edge-injected
    ``X-Tenant-Id``, or an admin JWT whose subject is the tenant) and EVERY
    query is filtered on it. A project id is minted by kanban and only unique
    within a tenant, so a caller naming another tenant's project gets that
    project's spend *inside its own tenant* — i.e. zeros — never the other
    tenant's figures. Deliberately 200-with-zeros rather than 404/403: a
    distinguishable answer would be an oracle for "this uuid exists in some
    other tenant", and a new project with no spend yet is a normal state.

    The split point is the GLOBAL rollup watermark ``max(bucket_date)`` — the
    same predicate ``unrolled_guard`` treats as "rolled up". Days <= watermark
    come from ``billing_cost_daily`` only; days after it from raw events only,
    bucketed by the rollup's own ``cost_event_time`` expression, so a day is
    never counted from both sources.
    """
    if project_id == NULL_PROJECT_ID:
        # The rollup's "no project asserted" sentinel is not a project; on raw
        # events the same spend is NULL, so the two halves would disagree.
        raise HTTPException(status_code=422, detail="project_id is the unattributed sentinel")
    tenant_id = _tenant
    if _is_tenant_uuid(project_id, tenant_id):
        # rollup_daily stores project_id = tenant_id as NULL_PROJECT_ID
        # (nullif(project_id, tenant_id)), so the settled half would always be
        # empty while the raw half counts the spend — which then vanishes from
        # the totals once the day rolls up. Same refusal as the sentinel.
        raise HTTPException(
            status_code=422,
            detail="project_id is the tenant's own id (unattributed in the rollup)",
        )
    as_of = datetime.now(UTC)

    watermark = await _rollup_watermark(session)

    settled_rows = (
        await session.execute(
            select(
                CostDaily.bucket_date.label("day"),
                func.sum(CostDaily.total_cost_usd).label("cost"),
                func.sum(CostDaily.request_count).label("cnt"),
                func.sum(CostDaily.total_input_tokens).label("inp"),
                func.sum(CostDaily.total_output_tokens).label("outp"),
            )
            .where(CostDaily.tenant_id == tenant_id, CostDaily.project_id == project_id)
            .group_by(CostDaily.bucket_date)
            .order_by(CostDaily.bucket_date)
        )
    ).all()
    settled_raw = [(r, Decimal(r.cost or 0)) for r in settled_rows]
    settled = [
        ProjectCostDay(
            day=r.day,
            cost=_usd(cost),
            calls=int(r.cnt or 0),
            tokens_in=int(r.inp or 0),
            tokens_out=int(r.outp or 0),
        )
        for r, cost in settled_raw
    ]

    raw_filters = _raw_cost_filters(tenant_id, CostEvent.project_id == project_id)
    provisional, prov_cost = await _provisional_cost(session, watermark, raw_filters)

    # '' and absent/JSON-null both mean "no card asserted" -> one NULL bucket.
    card_expr = func.nullif(CostEvent.extra_metadata["card_id"].as_string(), "")
    card_rows = (
        await session.execute(
            select(
                card_expr.label("card_id"),
                func.sum(CostEvent.cost_usd).label("cost"),
                func.count().label("cnt"),
                func.min(_event_day()).label("first_day"),
            )
            .where(*raw_filters)
            .group_by(card_expr)
            .order_by(card_expr.nulls_first())
        )
    ).all()
    by_card = [
        ProjectCardCost(card_id=r.card_id, cost=_usd(Decimal(r.cost or 0)), calls=int(r.cnt or 0))
        for r in card_rows
    ]
    first_days = [r.first_day for r in card_rows if r.first_day is not None]

    # Sum in full precision; quantize only what is emitted.
    settled_cost = sum((cost for _, cost in settled_raw), Decimal("0"))
    totals = ProjectCostTotals(
        cost=_usd(settled_cost + prov_cost),
        calls=sum(d.calls for d in settled) + provisional.calls,
        tokens_in=sum(d.tokens_in for d in settled) + provisional.tokens_in,
        tokens_out=sum(d.tokens_out for d in settled) + provisional.tokens_out,
        settled_cost=_usd(settled_cost),
        provisional_cost=_usd(prov_cost),
    )
    return envelope(
        ProjectCostResponse(
            project_id=str(project_id),
            tenant_id=tenant_id,
            settled=settled,
            provisional=provisional,
            by_card=by_card,
            by_card_since=min(first_days) if first_days else None,
            totals=totals,
            basis=PROJECT_COST_BASIS,
            as_of=as_of,
        ),
        request,
    )


#: What the figures on ``/api/costs/provisional`` mean — the provisional half of
#: :data:`PROJECT_COST_BASIS`, computed by the same helper.
PROVISIONAL_COST_BASIS = (
    "internal_cost_usd: billing_cost_events.cost_usd priced from provider_pricing "
    "at event time (pre-markup booked cost, not the customer price); "
    "provisional = raw events on UTC days after the rollup watermark (settled_through), "
    "excluded_from_billing rows dropped, unpriced events counted as calls at $0; "
    "not final until rolled up"
)


@router.get(
    "/provisional",
    response_model=Envelope[ProvisionalCostResponse],
    operation_id="get_costs_provisional",
)
async def costs_provisional(
    request: Request,
    subject_id: uuid.UUID | None = Query(
        default=None,
        description="Optional: narrow to one end-user (Subject). Default = the whole tenant.",
    ),
    session: AsyncSession = Depends(get_session),
    tenant_id: str = Depends(require_tenant),
) -> dict:
    """Not-yet-rolled-up (provisional) cost of the caller's tenant, optionally one subject.

    The settled counterpart is ``/tenant/{id}`` / ``/user/{subject_id}`` /
    ``/me`` (rollup only, days <= ``settled_through``); this read covers the
    days after it, from the same helper as ``/projects/{project_id}``'s
    ``provisional`` block, so the two never disagree on where the split is.

    Tenant scope: from ``require_tenant`` and applied to every query, so a
    ``subject_id`` belonging to another tenant yields zeros, never its spend.
    ``subject_id`` filters ``billing_cost_events.subject_id`` — the column
    ``/user/{subject_id}`` filters on the rollup.
    """
    as_of = datetime.now(UTC)
    watermark = await _rollup_watermark(session)
    scope = [CostEvent.subject_id == subject_id] if subject_id is not None else []
    provisional, _ = await _provisional_cost(
        session, watermark, _raw_cost_filters(tenant_id, *scope)
    )
    return envelope(
        ProvisionalCostResponse(
            **provisional.model_dump(),
            tenant_id=tenant_id,
            subject_id=str(subject_id) if subject_id is not None else None,
            settled_through=watermark,
            basis=PROVISIONAL_COST_BASIS,
            as_of=as_of,
        ),
        request,
    )
