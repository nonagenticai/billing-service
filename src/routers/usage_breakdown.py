"""``/api/usage/breakdown`` -- usage per project × member × funding × app × provider × model.

Reads ``billing_cost_usage_daily`` (alembic 0053), the one rollup that carries
project AND actor AND funding together. Owner decision D2 (2026-10-07).

AUTH -- the gates are the ones ``routers.costs.costs_by_actor`` already uses for
per-member cost, reused rather than rebuilt:

* ``GET /api/usage/breakdown`` is PER-MEMBER data, so it takes
  :func:`auth_deps.require_service_caller` (a verified internal service-account
  or license-admin JWT; a tenant LICENCE token is refused 403).
  - With ``X-Tenant-Id`` (a platform backend proxying for ITS tenant's admin):
    pinned to that tenant by :func:`auth_deps.enforce_tenant_isolation`; a
    different ``tenant_id`` is 403.
  - Without it (a platform admin / operator token): any tenant, ``tenant_id``
    required.
  - An allow-listed READER service account (``auth_deps.USAGE_BREAKDOWN_READER_CLIENTS``,
    e.g. raisemytickets analytics) holds no internal role and is accepted only WITH
    ``X-Tenant-Id`` -- pinned to that one tenant, never the any-tenant path.
* ``GET /api/usage/breakdown/me`` is self-service: the caller's own
  ``X-Actor-Id`` inside the active ``X-Tenant-Id`` -- the same header-pair model
  as ``/api/usage/me`` (``active_tenant_or_400``). The gateway strips a client's
  ``x-actor-id`` and only injects the verified JWT ``sub``; in-mesh callers
  forward the human they act for.
"""

from __future__ import annotations

import uuid
from datetime import UTC, date, datetime, timedelta
from decimal import Decimal
from typing import Any

from fastapi import APIRouter, Depends, HTTPException, Query, Request
from sqlalchemy import ColumnElement, func, select
from sqlalchemy.ext.asyncio import AsyncSession

from auth_deps import (
    enforce_tenant_isolation,
    require_usage_breakdown_reader,
    resolve_tenant_or_400,
)
from db import get_session
from models.db import NULL_ACTOR_ID, NULL_APP_ID, NULL_PROJECT_ID, CostUsageDaily
from models.db.app_registry import AppRegistry
from models.domain import (
    Envelope,
    UsageBreakdownRow,
    UsageBreakdownTotals,
    UsageCrossBreakdownResponse,
    envelope,
)
from routers.export import MAX_WINDOW_DAYS
from routers.usage import active_tenant_or_400
from services.funding import FUNDING_UNKNOWN, FUNDING_VALUES

router = APIRouter(prefix="/api/usage", tags=["usage"])

#: group_by dimension -> rollup column.
DIMENSIONS: dict[str, Any] = {
    "project": CostUsageDaily.project_id,
    "actor": CostUsageDaily.actor_id,
    "funding": CostUsageDaily.funding,
    "app": CostUsageDaily.app_id,
    "provider": CostUsageDaily.provider,
    "model": CostUsageDaily.served_model,
    "day": CostUsageDaily.bucket_date,
}
_SENTINELS = {
    "project": NULL_PROJECT_ID,
    "actor": NULL_ACTOR_ID,
    "app": NULL_APP_ID,
}
#: Accepted as a project_id / actor_id / app filter value: "the unattributed bucket".
_NONE_TOKEN = "none"
MAX_ROWS = 5000
DEFAULT_WINDOW_DAYS = 30


def _csv(raw: str | None) -> list[str]:
    return [p.strip() for p in (raw or "").split(",") if p.strip()]


def _parse_group_by(raw: str | None, default: str) -> list[str]:
    dims = _csv(raw if raw is not None else default)
    bad = [d for d in dims if d not in DIMENSIONS]
    if bad:
        raise HTTPException(
            status_code=422,
            detail=f"unknown group_by dimension(s) {bad}; allowed: {sorted(DIMENSIONS)}",
        )
    # De-duplicate, keep order.
    return list(dict.fromkeys(dims))


def _parse_window(from_date: date | None, to_date: date | None) -> tuple[date, date]:
    today = datetime.now(UTC).date()
    to_d = to_date or today
    from_d = from_date or (to_d - timedelta(days=DEFAULT_WINDOW_DAYS))
    if to_d < from_d:
        raise HTTPException(status_code=422, detail="'to' must be on or after 'from'")
    if (to_d - from_d).days > MAX_WINDOW_DAYS:
        raise HTTPException(
            status_code=422, detail=f"window > {MAX_WINDOW_DAYS} days; narrow the time range"
        )
    return from_d, to_d


def _uuid_filter(name: str, raw: str | None, sentinel: uuid.UUID) -> list[uuid.UUID]:
    out: list[uuid.UUID] = []
    for v in _csv(raw):
        if v.lower() == _NONE_TOKEN:
            out.append(sentinel)
            continue
        try:
            out.append(uuid.UUID(v))
        except ValueError as exc:
            raise HTTPException(
                status_code=422, detail=f"{name} must be a uuid or '{_NONE_TOKEN}'"
            ) from exc
    return out


async def _app_filter(session: AsyncSession, raw: str | None) -> list[uuid.UUID]:
    """App filter values are registry KEYS or app uuids. An unknown key matches nothing."""
    ids: list[uuid.UUID] = []
    keys: list[str] = []
    for v in _csv(raw):
        if v.lower() == _NONE_TOKEN:
            ids.append(NULL_APP_ID)
            continue
        try:
            ids.append(uuid.UUID(v))
        except ValueError:
            keys.append(v)
    if keys:
        found = (
            await session.execute(select(AppRegistry.app_id).where(AppRegistry.app_key.in_(keys)))
        ).scalars()
        ids.extend(found)
    return ids


def _funding_filter(raw: str | None) -> list[str]:
    vals = _csv(raw)
    allowed = FUNDING_VALUES | {FUNDING_UNKNOWN}
    bad = [v for v in vals if v not in allowed]
    if bad:
        raise HTTPException(
            status_code=422, detail=f"unknown funding value(s) {bad}; allowed: {sorted(allowed)}"
        )
    return vals


async def _breakdown(
    session: AsyncSession,
    *,
    tenant_id: str,
    from_d: date,
    to_d: date,
    group_by: list[str],
    project_id: str | None,
    actor_ids: list[uuid.UUID],
    funding: str | None,
    app: str | None,
    provider: str | None,
    model: str | None,
) -> tuple[UsageBreakdownTotals, list[UsageBreakdownRow], bool]:
    # ⛔ The tenant predicate is FIRST and unconditional: every read is one tenant.
    filters: list[ColumnElement[bool]] = [
        CostUsageDaily.tenant_id == tenant_id,
        CostUsageDaily.bucket_date >= from_d,
        CostUsageDaily.bucket_date <= to_d,
    ]
    projects = _uuid_filter("project_id", project_id, NULL_PROJECT_ID)
    if projects:
        filters.append(CostUsageDaily.project_id.in_(projects))
    if actor_ids:
        filters.append(CostUsageDaily.actor_id.in_(actor_ids))
    fundings = _funding_filter(funding)
    if fundings:
        filters.append(CostUsageDaily.funding.in_(fundings))
    if _csv(app):
        # An empty resolution (only unregistered keys) is an IN () that matches
        # nothing -- never a dropped filter, which would widen the read to all apps.
        filters.append(CostUsageDaily.app_id.in_(await _app_filter(session, app)))
    if _csv(provider):
        filters.append(CostUsageDaily.provider.in_(_csv(provider)))
    if _csv(model):
        filters.append(CostUsageDaily.served_model.in_(_csv(model)))

    metrics = (
        func.coalesce(func.sum(CostUsageDaily.event_count), 0).label("events"),
        func.coalesce(func.sum(CostUsageDaily.total_input_tokens), 0).label("inp"),
        func.coalesce(func.sum(CostUsageDaily.total_output_tokens), 0).label("outp"),
        func.coalesce(func.sum(CostUsageDaily.total_cost_usd), 0).label("cost"),
    )
    tot = (await session.execute(select(*metrics).where(*filters))).one()
    totals = UsageBreakdownTotals(
        events=int(tot.events),
        input_tokens=int(tot.inp),
        output_tokens=int(tot.outp),
        cost_usd=Decimal(tot.cost),
    )

    cols = [DIMENSIONS[d].label(d) for d in group_by]
    stmt = select(*cols, *metrics).where(*filters)
    if cols:
        stmt = stmt.group_by(*[DIMENSIONS[d] for d in group_by])
    stmt = stmt.order_by(
        func.sum(CostUsageDaily.total_cost_usd).desc(),
        func.sum(CostUsageDaily.event_count).desc(),
        *[DIMENSIONS[d] for d in group_by],
    ).limit(MAX_ROWS + 1)
    raw_rows = (await session.execute(stmt)).all()
    truncated = len(raw_rows) > MAX_ROWS
    raw_rows = raw_rows[:MAX_ROWS]

    app_keys: dict[uuid.UUID, str] = {}
    if "app" in group_by:
        app_ids = {r.app for r in raw_rows if r.app != NULL_APP_ID}
        if app_ids:
            reg = await session.execute(
                select(AppRegistry.app_id, AppRegistry.app_key).where(
                    AppRegistry.app_id.in_(app_ids)
                )
            )
            app_keys = dict(reg.tuples().all())

    rows: list[UsageBreakdownRow] = []
    for r in raw_rows:
        dims: dict[str, str | None] = {}
        for d in group_by:
            v = getattr(r, d)
            if d in _SENTINELS and v == _SENTINELS[d]:
                dims[d] = None
            elif d == "app":
                dims[d] = app_keys.get(v, str(v))
            elif d == "day":
                dims[d] = v.isoformat()
            else:
                dims[d] = str(v)
        rows.append(
            UsageBreakdownRow(
                dims=dims,
                events=int(r.events),
                input_tokens=int(r.inp),
                output_tokens=int(r.outp),
                cost_usd=Decimal(r.cost),
            )
        )
    return totals, rows, truncated


@router.get(
    "/breakdown",
    response_model=Envelope[UsageCrossBreakdownResponse],
    operation_id="get_usage_cross_breakdown",
)
async def usage_cross_breakdown(
    request: Request,
    tenant_id: str | None = Query(
        default=None, description="Tenant uuid. Defaults to (and must equal) X-Tenant-Id if sent."
    ),
    from_date: date | None = Query(default=None, alias="from"),
    to_date: date | None = Query(default=None, alias="to"),
    group_by: str | None = Query(
        default=None,
        description="CSV of project,actor,funding,app,provider,model,day. "
        "Default project,actor,funding.",
    ),
    project_id: str | None = Query(default=None, description="CSV of uuids, or 'none'."),
    actor_id: str | None = Query(default=None, description="CSV of uuids, or 'none'."),
    funding: str | None = Query(default=None, description="CSV of funding values."),
    app: str | None = Query(default=None, description="CSV of app keys / app uuids, or 'none'."),
    provider: str | None = Query(default=None, description="CSV of providers."),
    model: str | None = Query(default=None, description="CSV of served models."),
    session: AsyncSession = Depends(get_session),
    _service: dict = Depends(require_usage_breakdown_reader),
) -> dict:
    """Usage + cost for ONE tenant, grouped by any of project/actor/funding/app/provider/model/day."""
    header_tenant = request.headers.get("x-tenant-id") or ""
    raw_tenant = tenant_id or header_tenant
    if not raw_tenant:
        raise HTTPException(status_code=400, detail="tenant_id (or X-Tenant-Id) is required")
    resolved = resolve_tenant_or_400(raw_tenant)
    # A proxy pinned to tenant A by its header cannot read tenant B.
    enforce_tenant_isolation(request, resolved, header_tenant)
    from_d, to_d = _parse_window(from_date, to_date)
    dims = _parse_group_by(group_by, "project,actor,funding")
    totals, rows, truncated = await _breakdown(
        session,
        tenant_id=resolved,
        from_d=from_d,
        to_d=to_d,
        group_by=dims,
        project_id=project_id,
        actor_ids=_uuid_filter("actor_id", actor_id, NULL_ACTOR_ID),
        funding=funding,
        app=app,
        provider=provider,
        model=model,
    )
    return envelope(
        UsageCrossBreakdownResponse(
            tenant_id=resolved,
            from_date=from_d,
            to_date=to_d,
            group_by=dims,
            totals=totals,
            rows=rows,
            truncated=truncated,
        ),
        request,
        total=len(rows),
    )


@router.get(
    "/breakdown/me",
    response_model=Envelope[UsageCrossBreakdownResponse],
    operation_id="get_my_usage_cross_breakdown",
)
async def usage_cross_breakdown_me(
    request: Request,
    from_date: date | None = Query(default=None, alias="from"),
    to_date: date | None = Query(default=None, alias="to"),
    group_by: str | None = Query(
        default=None,
        description="CSV of project,funding,app,provider,model,day. Default project,funding.",
    ),
    project_id: str | None = Query(default=None),
    funding: str | None = Query(default=None),
    app: str | None = Query(default=None),
    provider: str | None = Query(default=None),
    model: str | None = Query(default=None),
    session: AsyncSession = Depends(get_session),
) -> dict:
    """The CALLER's own usage (X-Actor-Id) in the active tenant (X-Tenant-Id)."""
    tenant_id = active_tenant_or_400(request)
    raw_actor = (request.headers.get("x-actor-id") or "").strip()
    try:
        actor = uuid.UUID(raw_actor)
    except ValueError as exc:
        raise HTTPException(
            status_code=401, detail="Actor context required (X-Actor-Id header, a UUID)"
        ) from exc
    if actor == NULL_ACTOR_ID:
        raise HTTPException(status_code=401, detail="X-Actor-Id must be a real actor")
    from_d, to_d = _parse_window(from_date, to_date)
    dims = _parse_group_by(group_by, "project,funding")
    totals, rows, truncated = await _breakdown(
        session,
        tenant_id=tenant_id,
        from_d=from_d,
        to_d=to_d,
        group_by=dims,
        project_id=project_id,
        actor_ids=[actor],
        funding=funding,
        app=app,
        provider=provider,
        model=model,
    )
    return envelope(
        UsageCrossBreakdownResponse(
            tenant_id=tenant_id,
            from_date=from_d,
            to_date=to_d,
            group_by=dims,
            actor_id=str(actor),
            totals=totals,
            rows=rows,
            truncated=truncated,
        ),
        request,
        total=len(rows),
    )
