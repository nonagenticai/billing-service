"""Usage query endpoints."""

import re
import uuid
from collections.abc import Callable
from datetime import UTC, date, datetime, timedelta
from decimal import Decimal

from fastapi import APIRouter, Depends, HTTPException, Query, Request
from sqlalchemy import ColumnElement, and_, func, select
from sqlalchemy.ext.asyncio import AsyncSession

from auth_deps import (
    enforce_tenant_isolation,
    get_subject_id,
    get_tenant_name,
    require_admin,
    require_tenant,
    resolve_tenant_or_400,
)
from db import get_session
from models.db import (
    NULL_LICENSE_ID,
    NULL_PROJECT_ID,
    CostDaily,
    MeteringDaily,
    MeteringHourly,
)
from models.domain import (
    Envelope,
    Granularity,
    UsageBreakdownEntry,
    UsageBreakdownResponse,
    UsageResponse,
    UsageSeriesBucket,
    UsageSeriesResponse,
    UsageSummary,
    envelope,
)
from routers._aggregation import (
    aggregate_by_license,
    aggregate_by_service,
    granularity_window,
)
from routers.export import MAX_WINDOW_DAYS
from services.billing import get_billing_mode, get_markup_pct
from services.list_pricing import ListPricingResolver, build_list_pricing_resolver
from services.markup import (
    MarkupResolver,
    build_markup_resolver,
    compute_billed_line,
    quantize_money,
)
from settings import get_settings
from timeutil import default_end, default_start

router = APIRouter(prefix="/api/usage", tags=["usage"])


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


def active_tenant_or_400(request: Request) -> str:
    """Resolve the active tenant for a ``/me`` route, or 400.

    THE SUBJECT ALONE IS NOT A SCOPE. ``X-Subject-Id`` names one human, and one
    human can hold licences in several tenants — that is the normal case the
    admin route ``/api/usage/user/{subject_id}`` exists to sum deliberately. So a
    ``/me`` read filtered on ``subject_id`` ONLY returns that person's usage and
    cost from EVERY tenant they belong to, rendered into whichever workspace the
    caller happens to be looking at. The tenant predicate these routes now pass
    down is the only thing that stops workspace A's dashboard from showing rows
    that belong to workspace B.

    Read as the ``/me`` sibling of ``require_tenant``, NOT a call to it, for the
    same reason ``routers.budget.budget_check`` reads the header directly:
    ``require_tenant`` falls back to the admin JWT when the header is absent and
    resolves the caller's own ``sub``. A Keycloak service-account ``sub`` is a
    uuid, so it resolves cleanly, and ``/me`` would silently answer for "the
    tenant whose id happens to equal this service account". On ``/me`` there is
    no such thing as an admin caller: "me" IS the header pair.

    A missing, empty or unresolvable header is a **400**, the same shape
    ``resolve_tenant_or_400`` raises everywhere else — the caller authenticated
    fine upstream; what is wrong is the identifier they supplied. It is NOT a 401
    (re-authenticating cannot help) and NOT an empty 200: answering with zero
    rows would make an un-scoped caller indistinguishable from a caller who
    genuinely has no usage. The live caller (``fr/backend``'s metering client)
    already injects both headers on every ``/me`` read, so this is transparent to
    it; what it deliberately does not send is a bearer token, which is why this
    must not become a token check.
    """
    raw_tenant_id = request.headers.get("x-tenant-id")
    if not raw_tenant_id or not raw_tenant_id.strip():
        # Present-but-empty is a caller error too, not "no header": license-authz
        # sends `tenant_id = result.tenant_id or ""` when introspection returns
        # no tenant, and "" must not reach the resolver as if it were an id.
        raise HTTPException(
            status_code=400,
            detail="Active tenant context required (X-Tenant-Id header)",
        )
    # Resolved through the SAME resolver the header path uses, ONCE, at the top:
    # a registered tenant NAME has to become a uuid before it reaches a WHERE on
    # a `uuid` column, or Postgres answers with an opaque DataError.
    return resolve_tenant_or_400(raw_tenant_id, field="X-Tenant-Id")


@router.get(
    "/tenant/{tenant_id}",
    response_model=Envelope[UsageResponse],
    operation_id="get_usage_by_tenant",
)
async def usage_by_tenant(
    request: Request,
    tenant_id: str,
    start: datetime = Query(default_factory=default_start),
    end: datetime = Query(default_factory=default_end),
    granularity: Granularity = Query(default=Granularity.daily),
    service_key: str | None = Query(default=None),
    subject_id: uuid.UUID | None = Query(
        default=None,
        description="Optional: narrow to a single end-user (Subject UUID).",
    ),
    session: AsyncSession = Depends(get_session),
    _tenant: str = Depends(require_tenant),
    _tenant_name: str = Depends(get_tenant_name),
) -> dict:
    """Get usage for a tenant."""
    _validate_window(start, end)
    enforce_tenant_isolation(request, tenant_id, _tenant)
    table, date_col, range_start, range_end = granularity_window(start, end, granularity)
    filters = [
        table.tenant_id == tenant_id,
        date_col >= range_start,
        date_col <= range_end,
    ]
    if service_key:
        filters.append(table.service_key == service_key)
    if subject_id is not None:
        filters.append(table.subject_id == subject_id)

    by_service = await aggregate_by_service(session, table, filters)
    by_license = await aggregate_by_license(session, table, filters)

    total = sum(s.request_count for s in by_service)
    total_denied = sum(s.denied_count for s in by_service)

    return envelope(
        UsageResponse(
            tenant_id=tenant_id,
            tenant_name=_tenant_name or None,
            start=start,
            end=end,
            granularity=granularity.value,
            total_requests=total,
            total_denied=total_denied,
            by_service=by_service,
            by_license=by_license,
        ),
        request,
    )


@router.get(
    "/license/{license_id}",
    response_model=Envelope[UsageResponse],
    operation_id="get_usage_by_license",
)
async def usage_by_license(
    request: Request,
    license_id: str,
    start: datetime = Query(default_factory=default_start),
    end: datetime = Query(default_factory=default_end),
    granularity: Granularity = Query(default=Granularity.daily),
    subject_id: uuid.UUID | None = Query(
        default=None,
        description="Optional: narrow to a single end-user (Subject UUID).",
    ),
    session: AsyncSession = Depends(get_session),
    _tenant: str = Depends(require_tenant),
) -> dict:
    """Get usage for a specific license.

    Tenant isolation: a customer caller (X-Tenant-Id present) only sees rows for
    the license under its OWN tenant; an admin-JWT caller (no header) may read
    across tenants. Without this scope a customer could pass any tenant's
    ``license_id`` — a UUID, not a secret — and read that tenant's per-service
    usage counts. Mirrors ``costs.costs_by_correlation``, the sibling
    ``require_tenant`` endpoint keyed on a non-tenant dimension.
    """
    _validate_window(start, end)
    table, date_col, range_start, range_end = granularity_window(start, end, granularity)
    filters = [
        table.license_id == license_id,
        date_col >= range_start,
        date_col <= range_end,
    ]
    if request.headers.get("x-tenant-id"):
        filters.append(table.tenant_id == _tenant)
    if subject_id is not None:
        filters.append(table.subject_id == subject_id)

    by_service = await aggregate_by_service(session, table, filters)

    total = sum(s.request_count for s in by_service)
    total_denied = sum(s.denied_count for s in by_service)

    return envelope(
        UsageResponse(
            license_id=license_id,
            start=start,
            end=end,
            granularity=granularity.value,
            total_requests=total,
            total_denied=total_denied,
            by_service=by_service,
        ),
        request,
    )


@router.get(
    "/app/{app_id}",
    response_model=Envelope[UsageResponse],
    operation_id="get_usage_by_app",
)
async def usage_by_app(
    request: Request,
    app_id: str,
    start: datetime = Query(default_factory=default_start),
    end: datetime = Query(default_factory=default_end),
    tenant_id: str | None = Query(default=None),
    session: AsyncSession = Depends(get_session),
    _user: dict = Depends(require_admin),
) -> dict:
    """Get usage for a specific app."""
    _validate_window(start, end)
    filters = [
        MeteringDaily.app_id == app_id,
        MeteringDaily.bucket_date >= start.date(),
        MeteringDaily.bucket_date <= end.date(),
    ]
    if tenant_id:
        filters.append(MeteringDaily.tenant_id == tenant_id)

    by_service = await aggregate_by_service(session, MeteringDaily, filters)

    total = sum(s.request_count for s in by_service)
    total_denied = sum(s.denied_count for s in by_service)

    return envelope(
        UsageResponse(
            app_id=app_id,
            tenant_id=tenant_id,
            start=start,
            end=end,
            granularity="daily",
            total_requests=total,
            total_denied=total_denied,
            by_service=by_service,
        ),
        request,
    )


@router.get(
    "/summary",
    response_model=Envelope[UsageSummary],
    operation_id="get_usage_summary",
)
async def usage_summary(
    request: Request,
    tenant_id: str,
    start: datetime = Query(default_factory=default_start),
    end: datetime = Query(default_factory=default_end),
    session: AsyncSession = Depends(get_session),
    _tenant: str = Depends(require_tenant),
    _tenant_name: str = Depends(get_tenant_name),
) -> dict:
    """Get aggregate usage summary for a tenant."""
    _validate_window(start, end)
    enforce_tenant_isolation(request, tenant_id, _tenant)
    by_service = await aggregate_by_service(
        session,
        MeteringDaily,
        [
            MeteringDaily.tenant_id == tenant_id,
            MeteringDaily.bucket_date >= start.date(),
            MeteringDaily.bucket_date <= end.date(),
        ],
    )

    return envelope(
        UsageSummary(
            tenant_id=tenant_id,
            tenant_name=_tenant_name or None,
            start=start,
            end=end,
            total_requests=sum(s.request_count for s in by_service),
            total_denied=sum(s.denied_count for s in by_service),
            by_service=by_service,
        ),
        request,
    )


async def _usage_for_subject(
    session: AsyncSession,
    subject_id: uuid.UUID,
    start: datetime,
    end: datetime,
    granularity: Granularity,
    tenant_id: str | None = None,
) -> UsageResponse:
    """Aggregate usage for one subject across all licenses + (optionally) tenants.

    Shared between the admin ``/user/{subject_id}`` endpoint and the
    self-service ``/me`` endpoint.
    """
    _validate_window(start, end)
    table, date_col, range_start, range_end = granularity_window(start, end, granularity)
    filters = [
        table.subject_id == subject_id,
        date_col >= range_start,
        date_col <= range_end,
    ]
    if tenant_id is not None:
        filters.append(table.tenant_id == tenant_id)

    by_service = await aggregate_by_service(session, table, filters)
    by_license = await aggregate_by_license(session, table, filters)

    total = sum(s.request_count for s in by_service)
    total_denied = sum(s.denied_count for s in by_service)

    return UsageResponse(
        tenant_id=tenant_id,
        start=start,
        end=end,
        granularity=granularity.value,
        total_requests=total,
        total_denied=total_denied,
        by_service=by_service,
        by_license=by_license,
    )


@router.get(
    "/user/{subject_id}",
    response_model=Envelope[UsageResponse],
    operation_id="get_usage_by_user",
)
async def usage_by_user(
    request: Request,
    subject_id: uuid.UUID,
    start: datetime = Query(default_factory=default_start),
    end: datetime = Query(default_factory=default_end),
    granularity: Granularity = Query(default=Granularity.daily),
    tenant_id: str | None = Query(
        default=None,
        description="Optional: narrow to one tenant. Default sums across all tenants.",
    ),
    session: AsyncSession = Depends(get_session),
    _user: dict = Depends(require_admin),
) -> dict:
    """Get usage attributed to one end-user (Subject), admin-scoped.

    Sums across every tenant + license + app the user has accessed. Useful for
    cross-tenant attribution (e.g. one Keycloak user with licenses in multiple
    tenants).
    """
    payload = await _usage_for_subject(session, subject_id, start, end, granularity, tenant_id)
    return envelope(payload, request)


@router.get(
    "/me",
    response_model=Envelope[UsageResponse],
    operation_id="get_my_usage",
)
async def usage_me(
    request: Request,
    start: datetime = Query(default_factory=default_start),
    end: datetime = Query(default_factory=default_end),
    granularity: Granularity = Query(default=Granularity.daily),
    session: AsyncSession = Depends(get_session),
    subject_id: uuid.UUID = Depends(get_subject_id),
) -> dict:
    """Return the caller's own usage in the ACTIVE tenant (X-Subject-Id + X-Tenant-Id)."""
    # `tenant_id` is passed explicitly because omitting it drops the predicate
    # entirely (`_usage_for_subject` applies it only `if tenant_id is not None`),
    # which is exactly how this route used to sum one subject's rows across every
    # tenant they hold a licence in.
    tenant_id = active_tenant_or_400(request)
    payload = await _usage_for_subject(
        session, subject_id, start, end, granularity, tenant_id=tenant_id
    )
    return envelope(payload, request)


# --- Per-user time-series + per-service breakdown ---------------------------
#
# `window=Nd` is parsed into a timedelta. The window always ends at "now" —
# callers that want absolute ranges should fall back to `/api/usage/me`
# (which already accepts `start`/`end`) plus `/api/costs/me`.

_WINDOW_PATTERN = re.compile(r"^(\d+)d$")


def _parse_window(window: str) -> timedelta:
    """Parse a ``Nd`` window string into a timedelta.

    Only days are accepted today (`30d`, `7d`, `1d`, ...). Hours-suffixed
    windows (`24h`) are explicitly rejected so callers don't conflate
    granularity with window size.
    """
    m = _WINDOW_PATTERN.match(window or "")
    if not m:
        raise HTTPException(
            status_code=400,
            detail="Invalid window: expected Nd (e.g. '30d', '7d')",
        )
    days = int(m.group(1))
    if days <= 0 or days > 365:
        raise HTTPException(
            status_code=400,
            detail="window must be between 1d and 365d",
        )
    return timedelta(days=days)


class _MarkupCtx:
    """Per-request markup state: the tenant, an in-memory rule resolver, the
    billing mode, and the legacy tenant-default markup fallback. Built once per
    request so each cost line resolves without its own DB round-trip."""

    __slots__ = ("tenant_id", "resolver", "billing_mode", "fallback_pct", "list_pricing")

    def __init__(
        self,
        tenant_id: str,
        resolver: MarkupResolver,
        billing_mode: str,
        fallback_pct: float,
        list_pricing: ListPricingResolver,
    ) -> None:
        self.tenant_id = tenant_id
        self.resolver = resolver
        self.billing_mode = billing_mode
        self.fallback_pct = fallback_pct
        self.list_pricing = list_pricing


async def _build_markup_ctx(tenant_id: str, session: AsyncSession) -> _MarkupCtx:
    """Load the markup context for a tenant (current effective rules)."""
    settings = get_settings()
    now = datetime.now(UTC)
    fallback = await get_markup_pct(tenant_id, session, settings)
    mode = await get_billing_mode(tenant_id, session)
    resolver = await build_markup_resolver(session, tenant_id=tenant_id, ts=now)
    list_pricing = await build_list_pricing_resolver(session, now)
    return _MarkupCtx(tenant_id, resolver, mode, fallback, list_pricing)


# NOTE: a second, laxer reader of X-Tenant-Id used to sit here
# (``_markup_for_request``): it returned None when the header was absent, so
# ``billed_cost_usd`` silently fell back to ``cost_usd``, and it handed the RAW
# header value to ``_build_markup_ctx`` without canonicalising it. Both halves
# are now unreachable and both were traps — the /me routes resolve the tenant
# ONCE via ``active_tenant_or_400`` and pass the canonical uuid to
# ``_build_markup_ctx``, so the tenant that prices a line is byte-identical to
# the tenant that filters the rows. Do not reintroduce a header reader that can
# answer "no tenant" on a route where the tenant is mandatory.


def _billed_for_line(
    ctx: _MarkupCtx | None,
    *,
    incurred: Decimal,
    license_id: uuid.UUID | None,
    project_id: uuid.UUID | None,
    service_key: str | None,
    provider: str | None,
    model: str | None,
    byo: bool,
    input_tokens: int,
    output_tokens: int,
) -> Decimal:
    """Per-line billed amount, or the incurred cost when no tenant context.

    ⛔ UNQUANTISED — `compute_billed_line` no longer rounds a line, and neither
    does this. Callers accumulate these across the lines of a bucket / service
    and `quantize_money` the SUM once, so the figure a dashboard shows does not
    depend on how the same spend was split into rollup rows.
    """
    if ctx is None:
        return incurred
    lic = license_id if license_id and license_id != NULL_LICENSE_ID else None
    # ⭐ THE SENTINEL IS UNWRAPPED BACK TO `None` BEFORE IT REACHES THE MATCHER
    # — the same idiom as the `lic` line above, and as
    # services.billing.generate_billing. `billing_cost_daily.project_id` is NOT
    # NULL and carries `NULL_PROJECT_ID` when no project was asserted; that
    # sentinel is a storage device for the rollup's `ON CONFLICT` (a NULL never
    # equals itself in a plain UNIQUE), NOT a project id. `markup_rules.
    # project_id` is nullable and uses NULL as the wildcard, so handing
    # `_matches` the raw sentinel would ask it to find a literal project whose
    # id is `...0004`: the tenant-wide rule would stop matching project-less
    # spend on this dashboard while it still matched it on the invoice.
    proj = project_id if project_id and project_id != NULL_PROJECT_ID else None
    billed, _pct, _rule = compute_billed_line(
        ctx.resolver,
        ctx.billing_mode,
        tenant_id=ctx.tenant_id,
        incurred_usd=incurred,
        fallback_pct=ctx.fallback_pct,
        license_id=lic,
        project_id=proj,
        service_key=service_key,
        provider=provider,
        model=model,
        byo=byo,
        input_tokens=input_tokens,
        output_tokens=output_tokens,
    )
    return billed


def _provider_estimate_for_line(
    ctx: _MarkupCtx | None,
    *,
    service_key: str | None,
    provider: str | None,
    model: str | None,
    byo: bool,
    input_tokens: int,
    output_tokens: int,
) -> Decimal | None:
    """Estimated public *provider* cost for a BYO line, else None.

    Only BYO lines carry an estimate — non-BYO usage already has an
    authoritative ``cost_usd``. Returns None when there is no tenant context or
    no ``rate_class='list'`` row covers the provider, so the estimate is
    additive and never overwrites a real cost.
    """
    if ctx is None or not byo:
        return None
    return ctx.list_pricing.estimate(
        provider=provider,
        served_model=model,
        service_key=service_key,
        input_tokens=input_tokens,
        output_tokens=output_tokens,
    )


# Scope builder: given a bucket model, return the WHERE predicate that
# narrows rows to one subject (``/me/*``) or one tenant (``/tenant/*``).
# All three bucket tables expose both ``subject_id`` and ``tenant_id``.
_ScopeBuilder = Callable[[type[MeteringHourly | MeteringDaily | CostDaily]], ColumnElement[bool]]


async def _usage_series_payload(
    request: Request,
    session: AsyncSession,
    *,
    window: str,
    granularity: Granularity,
    scope: _ScopeBuilder,
    markup: _MarkupCtx | None,
) -> dict:
    """Per-bucket time-series for an arbitrary scope (subject or tenant).

    Hourly buckets come from ``metering_hourly`` (request counts only —
    cost rollups are daily-grain). Daily buckets join ``metering_daily``
    request counts with ``cost_daily`` totals (tokens + cost_usd). When the
    relevant cost rows are absent, ``cost_usd`` is reported as ``"0"`` and
    the top-level ``cost_unavailable`` flag is set so the UI can render `—`
    instead of a misleading ``$0.00``.

    That flag is deliberately NOT all-or-nothing: a window where only SOME days
    have cost rows still sets it, and ``cost_unavailable_days`` names the days
    whose zero is an absence. The request never fails on a gap — an unmeasured
    day is expressible, not fatal.
    """
    delta = _parse_window(window)
    end = datetime.now(UTC)
    start = end - delta

    if granularity == Granularity.hourly:
        # Hourly: requests only. No hourly cost rollup exists today.
        stmt = (
            select(
                MeteringHourly.bucket_hour.label("ts"),
                func.sum(MeteringHourly.request_count).label("requests"),
            )
            .where(
                scope(MeteringHourly),
                MeteringHourly.bucket_hour >= start,
                MeteringHourly.bucket_hour <= end,
            )
            .group_by(MeteringHourly.bucket_hour)
            .order_by(MeteringHourly.bucket_hour.asc())
        )
        rows = (await session.execute(stmt)).all()
        buckets = [
            UsageSeriesBucket(
                ts=r.ts,
                requests=int(r.requests or 0),
                tokens=0,
                cost_usd="0",
                billed_cost_usd="0",
            )
            for r in rows
        ]
        return envelope(
            UsageSeriesResponse(
                window=window,
                granularity=granularity.value,
                start=start,
                end=end,
                buckets=buckets,
                # Hourly granularity has no cost rollup today.
                cost_unavailable=True,
                markup_pct=markup.fallback_pct if markup else None,
            ),
            request,
        )

    # Daily granularity: combine MeteringDaily (requests) + CostDaily
    # (tokens + cost_usd) on bucket_date.
    req_stmt = (
        select(
            MeteringDaily.bucket_date.label("d"),
            func.sum(MeteringDaily.request_count).label("requests"),
        )
        .where(
            scope(MeteringDaily),
            MeteringDaily.bucket_date >= start.date(),
            MeteringDaily.bucket_date <= end.date(),
        )
        .group_by(MeteringDaily.bucket_date)
    )
    # Grouped to the markup line key so each line bills at its own rate; then
    # re-aggregated to one bucket per day for display.
    #
    # ⭐ `project_id` IS PART OF THE GROUPING KEY, NOT JUST OF THE SELECT. A
    # project-pinned markup rule scores at `_W_PROJECT` in `services.markup`, so
    # two rows differing ONLY in project can carry two different percentages; a
    # key without `project_id` sums them into one line and prices that mixture
    # once, which is a number the invoice never charges. Same key, same reason,
    # as services.billing.generate_billing's cost aggregate.
    cost_stmt = (
        select(
            CostDaily.bucket_date.label("d"),
            CostDaily.license_id,
            CostDaily.project_id,
            CostDaily.service_key,
            CostDaily.provider,
            CostDaily.served_model,
            CostDaily.byo,
            func.sum(CostDaily.total_input_tokens).label("inp"),
            func.sum(CostDaily.total_output_tokens).label("outp"),
            func.sum(CostDaily.total_cost_usd).label("cost"),
        )
        .where(
            scope(CostDaily),
            CostDaily.bucket_date >= start.date(),
            CostDaily.bucket_date <= end.date(),
        )
        .group_by(
            CostDaily.bucket_date,
            CostDaily.license_id,
            CostDaily.project_id,
            CostDaily.service_key,
            CostDaily.provider,
            CostDaily.served_model,
            CostDaily.byo,
        )
    )
    req_rows = (await session.execute(req_stmt)).all()
    cost_rows = (await session.execute(cost_stmt)).all()

    by_day_requests: dict[date, int] = {r.d: int(r.requests or 0) for r in req_rows}
    # Accumulate (tokens, incurred, billed) per day across its markup lines.
    by_day_cost: dict[date, list] = {}
    for r in cost_rows:
        inp, outp = int(r.inp or 0), int(r.outp or 0)
        incurred = Decimal(r.cost or 0)
        billed = _billed_for_line(
            markup,
            incurred=incurred,
            license_id=r.license_id,
            project_id=r.project_id,
            service_key=r.service_key,
            provider=r.provider,
            model=r.served_model,
            byo=r.byo,
            input_tokens=inp,
            output_tokens=outp,
        )
        estimate = _provider_estimate_for_line(
            markup,
            service_key=r.service_key,
            provider=r.provider,
            model=r.served_model,
            byo=r.byo,
            input_tokens=inp,
            output_tokens=outp,
        )
        agg = by_day_cost.setdefault(r.d, [0, Decimal(0), Decimal(0), None])
        agg[0] += inp + outp
        agg[1] += incurred
        agg[2] += billed
        if estimate is not None:
            agg[3] = (agg[3] or Decimal(0)) + estimate

    all_days = sorted(set(by_day_requests) | set(by_day_cost))
    # A day that HAS metered requests but NO cost_daily row is an absent
    # measurement, not a $0 day. `len(cost_rows) == 0` alone only catches the
    # all-or-nothing case, so a window with a single priced day rendered every
    # other day as a real zero with the flag still False. Name the gap days so
    # the UI can grey exactly those cells instead of the whole column — and so
    # a partial gap is never silently rounded to "cost is fine".
    gap_days = sorted(d for d, n in by_day_requests.items() if n > 0 and d not in by_day_cost)
    buckets = []
    for d in all_days:
        tokens, cost, billed, estimate = by_day_cost.get(d, [0, Decimal(0), Decimal(0), None])
        # Render the bucket timestamp as midnight UTC of bucket_date.
        ts = datetime(d.year, d.month, d.day, tzinfo=UTC)
        buckets.append(
            UsageSeriesBucket(
                ts=ts,
                requests=by_day_requests.get(d, 0),
                tokens=tokens,
                cost_usd=str(cost),
                billed_cost_usd=str(quantize_money(billed)),
                provider_estimate_usd=None if estimate is None else str(estimate),
            )
        )

    return envelope(
        UsageSeriesResponse(
            window=window,
            granularity=granularity.value,
            start=start,
            end=end,
            buckets=buckets,
            # Set for a WHOLE-window absence OR any per-day gap (see gap_days).
            cost_unavailable=len(cost_rows) == 0 or bool(gap_days),
            cost_unavailable_days=[d.isoformat() for d in gap_days],
            markup_pct=markup.fallback_pct if markup else None,
        ),
        request,
    )


@router.get(
    "/me/series",
    response_model=Envelope[UsageSeriesResponse],
    operation_id="get_my_usage_series",
)
async def usage_me_series(
    request: Request,
    window: str = Query(default="30d", description="Lookback window, e.g. '30d', '7d'."),
    granularity: Granularity = Query(default=Granularity.daily),
    session: AsyncSession = Depends(get_session),
    subject_id: uuid.UUID = Depends(get_subject_id),
) -> dict:
    """Return the caller's own per-bucket time-series (X-Subject-Id + X-Tenant-Id)."""
    # The scope is the subject AND the active tenant. With the subject predicate
    # alone every bucket this chart drew summed the caller's rows from every
    # tenant they hold a licence in, so switching workspaces changed the label
    # above the chart and nothing in it.
    tenant_id = active_tenant_or_400(request)
    markup = await _build_markup_ctx(tenant_id, session)
    return await _usage_series_payload(
        request,
        session,
        window=window,
        granularity=granularity,
        scope=lambda m: and_(m.subject_id == subject_id, m.tenant_id == tenant_id),
        markup=markup,
    )


@router.get(
    "/tenant/{tenant_id}/series",
    response_model=Envelope[UsageSeriesResponse],
    operation_id="get_tenant_usage_series",
)
async def usage_tenant_series(
    request: Request,
    tenant_id: str,
    window: str = Query(default="30d", description="Lookback window, e.g. '30d', '7d'."),
    granularity: Granularity = Query(default=Granularity.daily),
    session: AsyncSession = Depends(get_session),
    _tenant: str = Depends(require_tenant),
) -> dict:
    """Per-bucket time-series for a whole tenant (all subjects in the tenant).

    Tenant-scoped sibling of ``/me/series`` — fr's Usage page calls this with
    the active workspace tenant so the dashboard reflects the workspace, not
    the logged-in human (whose own subject rarely carries metered rows).
    """
    enforce_tenant_isolation(request, tenant_id, _tenant)
    markup = await _build_markup_ctx(tenant_id, session)
    return await _usage_series_payload(
        request,
        session,
        window=window,
        granularity=granularity,
        scope=lambda m: m.tenant_id == tenant_id,
        markup=markup,
    )


async def _usage_breakdown_payload(
    request: Request,
    session: AsyncSession,
    *,
    window: str,
    scope: _ScopeBuilder,
    markup: _MarkupCtx | None,
) -> dict:
    """Per-service usage + cost for an arbitrary scope (subject or tenant).

    Request counts come from ``metering_daily.service_key``; tokens + cost
    from ``cost_daily.service_key``. The result is the union of services
    seen on either axis — services with requests but no cost row carry
    ``tokens=0, cost_usd="0"``, and those service keys are listed in
    ``cost_unavailable_services`` with ``cost_unavailable`` set, so that zero
    is readable as "not measured" rather than "free". The request still
    succeeds and still returns every service it has.
    """
    delta = _parse_window(window)
    end = datetime.now(UTC)
    start = end - delta

    req_stmt = (
        select(
            MeteringDaily.service_key.label("service"),
            func.sum(MeteringDaily.request_count).label("requests"),
        )
        .where(
            scope(MeteringDaily),
            MeteringDaily.bucket_date >= start.date(),
            MeteringDaily.bucket_date <= end.date(),
        )
        .group_by(MeteringDaily.service_key)
    )
    # Grouped to the markup line key, then re-aggregated per service for display.
    #
    # ⭐ `project_id` IS PART OF THE GROUPING KEY, NOT JUST OF THE SELECT — see
    # the identical note on the series payload's aggregate above. Without it two
    # projects' spend is summed before any rule resolves, so no per-project rate
    # is expressible and this dashboard reports a total the invoice does not
    # charge.
    cost_stmt = (
        select(
            CostDaily.service_key.label("service"),
            CostDaily.license_id,
            CostDaily.project_id,
            CostDaily.provider,
            CostDaily.served_model,
            CostDaily.byo,
            func.sum(CostDaily.total_input_tokens).label("inp"),
            func.sum(CostDaily.total_output_tokens).label("outp"),
            func.sum(CostDaily.total_cost_usd).label("cost"),
        )
        .where(
            scope(CostDaily),
            CostDaily.bucket_date >= start.date(),
            CostDaily.bucket_date <= end.date(),
        )
        .group_by(
            CostDaily.service_key,
            CostDaily.license_id,
            CostDaily.project_id,
            CostDaily.provider,
            CostDaily.served_model,
            CostDaily.byo,
        )
    )
    req_rows = (await session.execute(req_stmt)).all()
    cost_rows = (await session.execute(cost_stmt)).all()

    by_service_requests: dict[str, int] = {r.service: int(r.requests or 0) for r in req_rows}
    # Accumulate (tokens, incurred, billed) per service across its markup lines.
    by_service_cost: dict[str, list] = {}
    for r in cost_rows:
        inp, outp = int(r.inp or 0), int(r.outp or 0)
        incurred = Decimal(r.cost or 0)
        billed = _billed_for_line(
            markup,
            incurred=incurred,
            license_id=r.license_id,
            project_id=r.project_id,
            service_key=r.service,
            provider=r.provider,
            model=r.served_model,
            byo=r.byo,
            input_tokens=inp,
            output_tokens=outp,
        )
        estimate = _provider_estimate_for_line(
            markup,
            service_key=r.service,
            provider=r.provider,
            model=r.served_model,
            byo=r.byo,
            input_tokens=inp,
            output_tokens=outp,
        )
        agg = by_service_cost.setdefault(r.service, [0, Decimal(0), Decimal(0), None])
        agg[0] += inp + outp
        agg[1] += incurred
        agg[2] += billed
        if estimate is not None:
            agg[3] = (agg[3] or Decimal(0)) + estimate

    all_services = sorted(set(by_service_requests) | set(by_service_cost))
    # Same absent-vs-zero distinction as the series payload, per service key:
    # a service with metered requests and no cost row is UNMEASURED, and the
    # `cost_usd:"0"` it renders must not be read as "this service was free".
    gap_services = sorted(
        s for s, n in by_service_requests.items() if n > 0 and s not in by_service_cost
    )
    services = []
    for s in all_services:
        tokens, cost, billed, estimate = by_service_cost.get(s, [0, Decimal(0), Decimal(0), None])
        services.append(
            UsageBreakdownEntry(
                service=s,
                requests=by_service_requests.get(s, 0),
                tokens=tokens,
                cost_usd=str(cost),
                billed_cost_usd=str(quantize_money(billed)),
                provider_estimate_usd=None if estimate is None else str(estimate),
            )
        )
    # Sort by request count desc for stable UI rendering.
    services.sort(key=lambda e: e.requests, reverse=True)

    return envelope(
        UsageBreakdownResponse(
            window=window,
            start=start,
            end=end,
            services=services,
            cost_unavailable=len(cost_rows) == 0 or bool(gap_services),
            cost_unavailable_services=gap_services,
            markup_pct=markup.fallback_pct if markup else None,
        ),
        request,
        total=len(services),
    )


@router.get(
    "/me/breakdown",
    response_model=Envelope[UsageBreakdownResponse],
    operation_id="get_my_usage_breakdown",
)
async def usage_me_breakdown(
    request: Request,
    window: str = Query(default="30d", description="Lookback window, e.g. '30d', '7d'."),
    session: AsyncSession = Depends(get_session),
    subject_id: uuid.UUID = Depends(get_subject_id),
) -> dict:
    """Return per-service usage + cost for the caller (X-Subject-Id + X-Tenant-Id)."""
    # Same cross-tenant leak as /me/series, per service key: without the tenant
    # predicate a service the caller only ever used in ANOTHER tenant appeared as
    # a row — with its cost — in this workspace's breakdown table.
    tenant_id = active_tenant_or_400(request)
    markup = await _build_markup_ctx(tenant_id, session)
    return await _usage_breakdown_payload(
        request,
        session,
        window=window,
        scope=lambda m: and_(m.subject_id == subject_id, m.tenant_id == tenant_id),
        markup=markup,
    )


@router.get(
    "/tenant/{tenant_id}/breakdown",
    response_model=Envelope[UsageBreakdownResponse],
    operation_id="get_tenant_usage_breakdown",
)
async def usage_tenant_breakdown(
    request: Request,
    tenant_id: str,
    window: str = Query(default="30d", description="Lookback window, e.g. '30d', '7d'."),
    session: AsyncSession = Depends(get_session),
    _tenant: str = Depends(require_tenant),
) -> dict:
    """Per-service usage + cost for a whole tenant (all subjects in the tenant).

    Tenant-scoped sibling of ``/me/breakdown`` — fr's Usage page calls this
    with the active workspace tenant.
    """
    enforce_tenant_isolation(request, tenant_id, _tenant)
    markup = await _build_markup_ctx(tenant_id, session)
    return await _usage_breakdown_payload(
        request,
        session,
        window=window,
        scope=lambda m: m.tenant_id == tenant_id,
        markup=markup,
    )
