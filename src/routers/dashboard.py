"""Admin dashboard — server-rendered HTML via Jinja2 + HTMX."""

import os
from collections import defaultdict
from datetime import UTC, datetime
from decimal import Decimal
from pathlib import Path
from typing import Any

import structlog
from fastapi import APIRouter, Depends, Query, Request
from fastapi.responses import HTMLResponse
from fastapi.templating import Jinja2Templates
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from auth import verify_admin_token
from db import get_session
from models.db import AppCostRate, ProviderCost, TokenBudget
from services.billing import generate_billing
from services.budget import check_budget, get_budgets_for_tenant
from services.markup import quantize_money
from settings import get_settings

logger = structlog.stdlib.get_logger(__name__)

templates_dir = Path(os.path.dirname(__file__)).parent / "templates"
templates = Jinja2Templates(directory=str(templates_dir))

router = APIRouter(prefix="/dashboard", tags=["dashboard"])


def _current_month() -> str:
    return datetime.now(UTC).strftime("%Y-%m")


def _exact_money(value: Decimal) -> str:
    """The sub-cent pre-image, TRIMMED but never rounded.

    ``billing_cost_daily.total_cost_usd`` is ``Numeric(20, 6)``, so the figure
    arrives at full COLUMN SCALE and the sub-cent tooltip read ``$0.005000`` —
    three digits of storage padding on a number whose entire point is how small
    it is.

    ⛔ THE FIX IS NOT A ROUNDING, AND SPECIFICALLY NOT ``quantize_money``.
    Rounding this to the cent yields ``$0.00``, which is the exact statement the
    marker exists to contradict — it would delete the figure it is displayed to
    preserve. Only trailing ZEROS are stripped, so the value is arithmetically
    unchanged and every significant digit survives: ``0.005000`` → ``0.005``,
    ``0.0000012`` → ``0.0000012``, ``10.000000`` → ``10``.

    ⚠️ ``format(value, "f")`` rather than ``str()`` or ``.normalize()``: both of
    those can emit scientific notation (``Decimal("10.000000").normalize()`` is
    ``Decimal("1E+1")``), and ``$1E+1`` in a tooltip is worse than the padding
    it replaced. ``"f"`` is fixed-point at any exponent.

    ⛔ DISPLAY ONLY — it returns a STRING for exactly that reason. Nothing on
    this page sums or divides by it.
    """
    text = format(value, "f")
    return text.rstrip("0").rstrip(".") if "." in text else text


@router.get(
    "/",
    response_class=HTMLResponse,
    operation_id="dashboard_index",
)
async def dashboard_overview(
    request: Request,
    month: str = Query(default_factory=_current_month, pattern=r"^\d{4}-\d{2}$"),
    session: AsyncSession = Depends(get_session),
    _user: dict = Depends(verify_admin_token),
) -> HTMLResponse:
    """Platform overview — all tenants, costs, margins."""
    from routers.billing import compute_billing_overview

    overview = await compute_billing_overview(month, session)

    # Compute margin per tenant
    tenant_rows: list[dict[str, Any]] = []
    for t in overview.tenants:
        # ⭐ EVERY FIGURE ON THIS PAGE IS THE CENT IT IS DISPLAYED AS, AND THE
        # MARGIN IS DERIVED FROM THE TWO OF THEM — never rounded independently
        # off a raw difference. Same fold as the invoice's
        # `margin_q = customer_cost_q − internal_cost_q` (services/billing.py).
        #
        # `compute_billing_overview` now quantizes both columns at source, so
        # these two calls are IDEMPOTENT on the real path; they are here because
        # THIS is the layer that decides what the cell says, and a margin taken
        # against a figure the page does not show is unreconcilable by anyone
        # reading it. `q(customer) − q(internal)` and `q(customer − internal)`
        # differ by a cent for half a cent of inputs: measured, internal
        # 20.005000 against customer 60.01 printed $40.01 beside two columns
        # that subtract to $40.00.
        #
        # ⛔ THESE TWO CALLS ARE DEAD ON EVERY PRODUCTION PATH, AND THEY ARE THE
        # SOLE REASON A DELIBERATELY UNFAITHFUL TEST FIXTURE EXISTS. The only
        # producer, `compute_billing_overview` (routers/billing.py), already
        # emits `internal_charged` / `customer_charged` in these two fields, so
        # on real data `q(x) == x` here and NOTHING observable changes if both
        # calls are deleted. That is exactly why
        # `tests/test_dashboard.py::_overview_payload` carries an
        # `upstream_folded=False` switch: it hands the page a payload no producer
        # ever emits (both display columns raw) purely so the removal of THESE
        # calls can fail a test. On a producer-faithful payload it cannot —
        # defence in depth disarms its own mutation control.
        # ⛔ So do NOT "tidy" these away as redundant. Deleting them silently
        # deletes the only justification for that fixture class, and the next
        # reader will then delete the fixture as pointless too — leaving the
        # renderer's fold ungraded at BOTH layers. If they ever genuinely must
        # go, the producer's fold becomes the only guard and
        # `upstream_folded=False` goes with them, as one deliberate decision.
        internal_charged = quantize_money(t.internal_cost_usd)
        customer_charged = quantize_money(t.customer_cost_usd)
        margin = customer_charged - internal_charged
        # None -- not 0 -- when there is no internal cost to take a margin on.
        # Same idiom, same reason as AttributionAudit.mismatch_ratio /
        # .attribution_rate in services/attribution_audit.py: a rate without its
        # denominator invents a fact. A BYO tenant (or one billed only a
        # per-request surcharge) can have internal_cost == 0 and
        # customer_cost > 0, where the true margin is UNBOUNDED; rendering that
        # as "0%" said "no margin at all" about the most profitable row on the
        # page -- the exact opposite of the truth. The template renders None as
        # a distinct marker so it can never be mistaken for a measured zero.
        #
        # ⚠️ The denominator is the DISPLAYED internal cost, so a tenant whose
        # incurred cost rounds to $0.00 gets NO percentage rather than one taken
        # against a denominator the page does not show. That is the same
        # judgement as the paragraph above, not a new one: a ratio whose
        # denominator displays as zero has no denominator.
        margin_pct = (
            round(float(margin) / float(internal_charged) * 100, 1)
            if internal_charged > 0
            else None
        )
        # ⭐ ...BUT "NO PERCENTAGE" IS TWO DIFFERENT FACTS AND THE CELL MUST SAY
        # WHICH. Suppressing the ratio is right; rendering both cases as the same
        # "n/a" labelled "No internal cost this month" is not — for a tenant whose
        # incurred cost is real but under half a cent that label is simply FALSE,
        # and the row loses the only trace of its spend on this page. Measured on
        # prod 2026-09-15: 12 of 60 licence-attributed tenant-months are positive
        # and sub-cent, including one in the current month.
        #
        # The fix is a THIRD state, not a rescued percentage: `margin_pct` stays
        # None (a ratio against a displayed $0.00 would be a fabricated figure
        # whichever denominator produced it), and the marker beside it carries
        # the raw cost so the information is on the page instead of erased. A
        # reader can then tell a $0-spend tenant from a sub-cent one without
        # either cell claiming to be the ratio of the two columns shown.
        #
        # ⚠️ THE THIRD STATE IS KEYED ON `== 0`, SO A **NEGATIVE** INTERNAL COST
        # FALLS THROUGH TO THE FALSE LABEL. `margin_pct` is suppressed by
        # `internal_charged > 0` (so any negative gets no percentage), but
        # `sub_cent_internal` needs `internal_charged == 0`, which a negative
        # fails — so the cell renders the plain "n/a" whose tooltip asserts "No
        # internal cost this month" about a tenant carrying a negative one.
        # Unreachable today: `internal_cost_usd` is a sum of non-negative
        # per-licence costs, and no credit, refund or adjustment writes into this
        # axis. ⛔ Noted because the day one does, this cell starts lying rather
        # than erroring, and nothing here would fail. The fix at that point is a
        # FOURTH state, not widening this `== 0` to `<= 0` — a negative cost is
        # its own fact and must not be relabelled as sub-cent.
        internal_exact = Decimal(t.internal_cost_exact_usd)
        sub_cent_internal = internal_charged == 0 and internal_exact > 0
        tenant_rows.append(
            {
                "tenant_id": t.tenant_id,
                "total_requests": t.total_requests,
                "internal_cost_usd": internal_charged,
                "customer_cost_usd": customer_charged,
                # Already the difference of two whole cents — NOT re-rounded.
                "margin_usd": margin,
                "margin_pct": margin_pct,
                # Display-only companions to `margin_pct is None`; see above.
                # ⛔ Neither is money on this page — never sum or divide by them.
                "internal_sub_cent": sub_cent_internal,
                # Trimmed for the tooltip, NOT rounded — see `_exact_money`.
                "internal_cost_exact_display": _exact_money(internal_exact),
            }
        )

    # ⭐ A DISPLAYED TOTAL IS THE SUM OF THE DISPLAYED COMPONENTS — `Σq`, never
    # `qΣ`. Taking the footer from the platform totals instead made the Margin
    # card `q(Σcustomer − Σinternal)` while the column above it was
    # `Σ q(customer_i) − q(internal_i)`: measured, two tenants at internal
    # 20.005000 / customer 30.02 sum to $20.02 down the column under a card
    # reading $20.03, and an operator cannot reconcile a table that does not add
    # up to its own footer. The error grows with tenant count, so it is worst
    # where hand-checking is least likely.
    # ⛔ Do NOT "simplify" these back to the `overview.platform_total_*` fields.
    internal_total = sum((r["internal_cost_usd"] for r in tenant_rows), Decimal("0"))
    customer_total = sum((r["customer_cost_usd"] for r in tenant_rows), Decimal("0"))
    margin_total = sum((r["margin_usd"] for r in tenant_rows), Decimal("0"))

    return templates.TemplateResponse(
        request,
        "overview.html",
        {
            "active": "overview",
            "month": month,
            "tenants": tenant_rows,
            "platform_requests": sum(t.total_requests for t in overview.tenants),
            # Each is already a sum of whole cents; the quantize only fixes the
            # display exponent (the empty-table case sums to a bare `0`).
            "internal_total": quantize_money(internal_total),
            "customer_total": quantize_money(customer_total),
            "margin_total": quantize_money(margin_total),
        },
    )


@router.get(
    "/tenant/{tenant_id}",
    response_class=HTMLResponse,
    operation_id="dashboard_tenant",
)
async def tenant_detail(
    request: Request,
    tenant_id: str,
    month: str = Query(default_factory=_current_month, pattern=r"^\d{4}-\d{2}$"),
    session: AsyncSession = Depends(get_session),
    _user: dict = Depends(verify_admin_token),
) -> HTMLResponse:
    """Single tenant detail — per-license breakdown with budget status."""
    settings = get_settings()
    billing = await generate_billing(tenant_id, month, session, settings)

    # Get budget statuses
    budgets_list = await get_budgets_for_tenant(tenant_id, session)
    budget_statuses = [await check_budget(b) for b in budgets_list]

    # Convert billing to a simple namespace for template
    from models.domain import BillingResponse

    billing_resp = BillingResponse(**billing)

    return templates.TemplateResponse(
        request,
        "tenant.html",
        {
            "active": "overview",
            "tenant_id": tenant_id,
            "month": month,
            "billing": billing_resp,
            "budgets": budget_statuses,
        },
    )


@router.get(
    "/costs",
    response_class=HTMLResponse,
    operation_id="dashboard_costs",
)
async def costs_page(
    request: Request,
    session: AsyncSession = Depends(get_session),
    _user: dict = Depends(verify_admin_token),
) -> HTMLResponse:
    """Cost configuration and summary, plus the per-tenant / per-subject ledger."""
    from datetime import timedelta

    from sqlalchemy import func as sa_func

    from models.db import CostDaily
    from service_auth import ServiceTokenProvider
    from services.tenant_names import (
        UNATTRIBUTED,
        get_tenant_info,
        resolve_cost_centre,
        resolve_tenant_name,
    )
    from tenancy import NIL_TENANT_ID

    providers_result = await session.execute(select(ProviderCost))
    providers = [
        {
            "provider": p.provider,
            "input_per_1m_usd": p.input_per_1m_usd,
            "output_per_1m_usd": p.output_per_1m_usd,
        }
        for p in providers_result.scalars().all()
    ]

    apps_result = await session.execute(select(AppCostRate))
    apps = [
        {"app_key": a.app_key, "cost_per_request_usd": a.cost_per_request_usd}
        for a in apps_result.scalars().all()
    ]

    # Cost summary for last 30 days. Compare Date to date, not datetime: bucket_date
    # is a Date, and postgres casts Date→timestamptz using the session TZ
    # (Europe/Amsterdam in prod), so `bucket_date >= <utc datetime>` mis-includes
    # the boundary day whenever local-vs-UTC date differs (same fix as costs.py).
    start = (datetime.now(UTC) - timedelta(days=30)).date()
    cost_stmt = select(
        sa_func.sum(CostDaily.total_cost_usd).label("cost"),
        sa_func.sum(CostDaily.total_input_tokens).label("inp"),
        sa_func.sum(CostDaily.total_output_tokens).label("outp"),
    ).where(CostDaily.bucket_date >= start)
    cost_row = (await session.execute(cost_stmt)).one_or_none()
    summary = {
        "total_internal_cost_usd": cost_row.cost or Decimal("0") if cost_row else Decimal("0"),
        "total_input_tokens": cost_row.inp or 0 if cost_row else 0,
        "total_output_tokens": cost_row.outp or 0 if cost_row else 0,
    }

    # --- Per-tenant and per-subject ledger over the SAME 30-day window ---
    #
    # `start` above is reused deliberately: recomputing it here would open a
    # second window that can straddle a UTC midnight mid-render, so the global
    # summary card and the per-tenant rows below it would disagree by a day for
    # anyone reading the page at the wrong moment.
    _tokens = sa_func.sum(CostDaily.total_input_tokens) + sa_func.sum(CostDaily.total_output_tokens)
    _ledger_cols = (
        # MAX, not an arbitrary pick: a tenant's rows can carry a mix of "" and
        # a real name (emitters differ), and MAX sorts a real name above "".
        sa_func.max(CostDaily.tenant_name).label("tenant_name"),
        sa_func.sum(CostDaily.total_input_tokens).label("inp"),
        sa_func.sum(CostDaily.total_output_tokens).label("outp"),
        sa_func.sum(CostDaily.total_cost_usd).label("cost"),
        sa_func.sum(CostDaily.request_count).label("requests"),
        sa_func.count(sa_func.distinct(CostDaily.subject_id)).label("subjects"),
    )

    tenant_stmt = (
        select(CostDaily.tenant_id.label("tenant_id"), *_ledger_cols)
        .where(CostDaily.bucket_date >= start)
        .group_by(CostDaily.tenant_id)
        .order_by(_tokens.desc())
    )
    tenant_rows = (await session.execute(tenant_stmt)).all()

    subject_stmt = (
        select(
            CostDaily.tenant_id.label("tenant_id"),
            CostDaily.subject_id.label("subject_id"),
            *_ledger_cols,
        )
        .where(CostDaily.bucket_date >= start)
        .group_by(CostDaily.tenant_id, CostDaily.subject_id)
        .order_by(_tokens.desc())
        .limit(50)
    )
    subject_rows = (await session.execute(subject_stmt)).all()

    # Best-effort display names. `get_tenant_names` never raises and never
    # blocks longer than its own short timeout, so a licences outage costs the
    # page its labels, not its 200.
    token_provider: ServiceTokenProvider | None = getattr(
        request.app.state, "service_token_provider", None
    )
    tenant_info = await get_tenant_info(get_settings(), token_provider)
    tenant_names = {tid: info.name for tid, info in tenant_info.items()}

    def _label(tenant_id: str, stored_name: str | None) -> str:
        # The nil tenant is a REAL 34M-token bucket ("we could not attribute
        # this"), not a row to hide — it is named, not filtered.
        if tenant_id == NIL_TENANT_ID:
            return UNATTRIBUTED
        return resolve_tenant_name(tenant_id, stored_name, tenant_names)

    def _centre(tenant_id: str) -> str:
        # Same treatment as _label: the nil bucket is not "unclassified" (which
        # would mean "licences has not decided"), it is spend we could not
        # attribute to any tenant at all. Collapsing the two would hide a real
        # 34M-token bucket inside a classification gap.
        if tenant_id == NIL_TENANT_ID:
            return UNATTRIBUTED
        return resolve_cost_centre(tenant_id, tenant_info)

    by_tenant = [
        {
            "tenant_id": r.tenant_id,
            "tenant_name": _label(r.tenant_id, r.tenant_name),
            "cost_centre": _centre(r.tenant_id),
            "input_tokens": r.inp or 0,
            "output_tokens": r.outp or 0,
            "request_count": r.requests or 0,
            "cost_usd": r.cost or Decimal("0"),
            "subject_count": r.subjects or 0,
        }
        for r in tenant_rows
    ]

    # Cost split by cost centre. Rolls up the SAME `tenant_rows` the table above
    # renders — a second query could straddle a UTC midnight mid-render and
    # disagree with it, the same trap the shared `start` above avoids.
    _centre_totals: dict[str, dict] = {}
    for r in tenant_rows:
        bucket = _centre_totals.setdefault(
            _centre(r.tenant_id),
            {"cost_centre": _centre(r.tenant_id), "cost_usd": Decimal("0"), "tenants": 0},
        )
        bucket["cost_usd"] += r.cost or Decimal("0")
        bucket["tenants"] += 1
    by_cost_centre = sorted(_centre_totals.values(), key=lambda b: b["cost_usd"], reverse=True)

    by_subject = [
        {
            "tenant_id": r.tenant_id,
            "tenant_name": _label(r.tenant_id, r.tenant_name),
            "subject_id": str(r.subject_id),
            "input_tokens": r.inp or 0,
            "output_tokens": r.outp or 0,
            "request_count": r.requests or 0,
            "cost_usd": r.cost or Decimal("0"),
        }
        for r in subject_rows
    ]

    return templates.TemplateResponse(
        request,
        "costs.html",
        {
            "active": "costs",
            "providers": providers,
            "apps": apps,
            "summary": summary,
            "by_tenant": by_tenant,
            "by_cost_centre": by_cost_centre,
            "by_subject": by_subject,
        },
    )


@router.get(
    "/budgets",
    response_class=HTMLResponse,
    operation_id="dashboard_budgets",
)
async def budgets_page(
    request: Request,
    session: AsyncSession = Depends(get_session),
    _user: dict = Depends(verify_admin_token),
) -> HTMLResponse:
    """Budget management — view, create, delete budgets."""
    # Get all active budgets grouped by tenant
    stmt = select(TokenBudget).where(TokenBudget.active.is_(True)).order_by(TokenBudget.tenant_id)
    result = await session.execute(stmt)
    all_budgets = list(result.scalars().all())

    budgets_by_tenant: dict[str, list] = defaultdict(list)
    for b in all_budgets:
        budgets_by_tenant[b.tenant_id].append(await check_budget(b))

    return templates.TemplateResponse(
        request,
        "budgets.html",
        {
            "active": "budgets",
            "budgets_by_tenant": dict(budgets_by_tenant),
        },
    )
