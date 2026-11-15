"""Billing endpoints — generate tenant bills with markup."""

from collections import defaultdict
from datetime import UTC, date, datetime, timedelta
from decimal import Decimal
from uuid import UUID

import structlog
from fastapi import APIRouter, Depends, HTTPException, Query, Request, status
from sqlalchemy import Date, func, select
from sqlalchemy.ext.asyncio import AsyncSession

import observability.metrics as observability_metrics
from auth_deps import (
    enforce_tenant_isolation,
    require_admin,
    require_pricing_writer,
    require_service_caller,
    require_tenant,
    resolve_tenant_or_400,
)
from db import get_session
from models.db import (
    ATTRIBUTION_UNLICENSED,
    NULL_LICENSE_ID,
    NULL_PROJECT_ID,
    AppCostRate,
    BillingInvoice,
    CostDaily,
    CostEvent,
    MeteringDaily,
    TenantConfig,
)
from models.domain import (
    EXCLUSION_BASIS_RAW_EVENTS,
    EXCLUSION_BASIS_UNMEASURABLE,
    PREVIEW_BASIS_NONE,
    PREVIEW_BASIS_ROLLUP,
    BillingOverview,
    BillingOverviewEntry,
    BillingResponse,
    CloseInvoiceRequest,
    Envelope,
    InvoiceDocument,
    InvoicePreviewExclusions,
    InvoicePreviewResponse,
    InvoiceResponse,
    InvoiceSummary,
    VoidInvoiceRequest,
    envelope,
)
from services import audit
from services.access_charge import (
    active_access_charges_all_tenants,
    active_access_charges_for_month,
    active_allowances_all_tenants,
    allocate_allowance,
    seat_rates_all_tenants,
)
from services.billing import generate_billing, get_markup_pct_map
from services.byo_mode import effective_billing_mode, get_byo_mode_map
from services.invoices import (
    BucketGapError,
    InvoiceMoneyImbalanceError,
    InvoicePeriodError,
    InvoiceStateError,
    PeriodNotBillableError,
    TenantNotInvoiceableError,
    UnpricedProviderCostError,
    UnpricedServiceKeyError,
    close_period,
    issue,
    reproduce,
)
from services.invoices import (
    void as void_invoice,
)
from services.markup import build_markup_resolver, compute_billed_line, quantize_money
from services.seats import SeatCountUnavailableError, get_seat_count
from settings import get_settings

logger = structlog.stdlib.get_logger(__name__)

router = APIRouter(prefix="/api/billing", tags=["billing"])


def _current_month() -> str:
    return datetime.now(UTC).strftime("%Y-%m")


@router.get(
    "/tenant/{tenant_id}",
    response_model=Envelope[BillingResponse],
    operation_id="get_billing_by_tenant",
)
async def billing_for_tenant(
    request: Request,
    tenant_id: str,
    month: str = Query(default_factory=_current_month, pattern=r"^\d{4}-\d{2}$"),
    session: AsyncSession = Depends(get_session),
    _tenant: str = Depends(require_tenant),
) -> dict:
    """Generate a billing report for a tenant for a given month."""
    enforce_tenant_isolation(request, tenant_id, _tenant)
    settings = get_settings()
    token_provider = getattr(request.app.state, "service_token_provider", None)
    payload = await generate_billing(
        tenant_id, month, session, settings, token_provider=token_provider
    )
    return envelope(payload, request)


async def compute_billing_overview(month: str, session: AsyncSession) -> BillingOverview:
    """Compute the BillingOverview model. Shared between the API route and the
    HTML dashboard. The route wraps the result in the response envelope; the
    dashboard consumes it directly."""
    settings = get_settings()
    year, mon = int(month[:4]), int(month[5:7])
    month_start = date(year, mon, 1)
    month_end = date(year + 1, 1, 1) if mon == 12 else date(year, mon + 1, 1)

    # Get per-tenant usage counts + service keys for app-cost calculation
    usage_stmt = (
        select(
            MeteringDaily.tenant_id,
            MeteringDaily.service_key,
            func.sum(MeteringDaily.request_count).label("requests"),
        )
        .where(
            MeteringDaily.bucket_date >= month_start,
            MeteringDaily.bucket_date < month_end,
        )
        .group_by(MeteringDaily.tenant_id, MeteringDaily.service_key)
    )
    result = await session.execute(usage_stmt)
    # {tenant_id: {total_requests, per-service counts}}
    usage_by_tenant: dict[str, dict] = {}
    for row in result.all():
        entry = usage_by_tenant.setdefault(row.tenant_id, {"requests": 0, "svc_counts": {}})
        entry["requests"] += row.requests
        entry["svc_counts"][row.service_key] = row.requests

    # LLM costs grouped to the markup line key (per-tenant rollup — no license
    # dimension here, so license-pinned rules are approximated at this aggregate
    # view; they still resolve exactly on the per-license invoice).
    #
    # ⭐ `project_id` IS PART OF THE GROUPING KEY, NOT JUST OF THE SELECT — the
    # same requirement, for the same reason, as services.billing.generate_billing.
    # Two days of spend differing ONLY in project can carry two different
    # percentages (`services.markup` scores a project-pinned rule at
    # `_W_PROJECT`); a key without `project_id` sums them into one line and
    # prices the mixture once, so this overview would report a total the invoice
    # does not charge. Splitting the group moves no money on its own — the same
    # rate over two halves sums to the same as over the whole — it is what makes
    # a per-project rate expressible here at all.
    cost_stmt = (
        select(
            CostDaily.tenant_id,
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
            CostDaily.bucket_date >= month_start,
            CostDaily.bucket_date < month_end,
            # Exclude system/S2S cost (sentinel license), mirroring
            # generate_billing — so the overview reconciles to the sum of the
            # per-tenant invoices (system cost is never billed to a customer nor
            # netted against a customer's allowance).
            CostDaily.license_attributed_clause(),
        )
        .group_by(
            CostDaily.tenant_id,
            CostDaily.project_id,
            CostDaily.service_key,
            CostDaily.provider,
            CostDaily.served_model,
            CostDaily.byo,
        )
        # ⚠️ PRESENTATIONAL ONLY — this ORDER BY no longer defends the money, and
        # the claim that it did has been removed rather than left to rot. The
        # allowance used to be drained line by line before per-line markup, so
        # planner row order changed the total; `allocate_allowance` now splits
        # each bucket PRO-RATA across every line at once and the total is
        # identical under every permutation. A fixed order still makes a re-run
        # byte-comparable, which is worth the zero it costs. Same key and same
        # reasoning as services.billing.generate_billing.
        .order_by(
            CostDaily.tenant_id,
            CostDaily.project_id,
            CostDaily.service_key,
            CostDaily.provider,
            CostDaily.served_model,
            CostDaily.byo,
        )
    )
    cost_rows = (await session.execute(cost_stmt)).all()

    # Get app cost rates for per-request cost calculation
    app_rate_result = await session.execute(select(AppCostRate))
    app_rates = {a.app_key: a.cost_per_request_usd for a in app_rate_result.scalars().all()}

    # Per-tenant billing mode (gates byo/pass-through).
    mode_result = await session.execute(select(TenantConfig.tenant_id, TenantConfig.billing_mode))
    mode_map = {row.tenant_id: row.billing_mode for row in mode_result.all()}

    # ACCESS-billing axis: per-tenant fixed access fees for the month (billed at
    # the last instant of the month, matching generate_billing). Includes tenants
    # with a paid charge but zero usage, so they are counted in the platform total.
    bill_ts = datetime(month_end.year, month_end.month, month_end.day, tzinfo=UTC) - timedelta(
        microseconds=1
    )
    access_by_tenant = await active_access_charges_all_tenants(
        session, month_start=month_start, month_end=month_end, bill_ts=bill_ts
    )
    # SEAT axis. `active_access_charges_all_tenants` deliberately EXCLUDES
    # per_seat rows (their amount_usd is a rate, not a charge), so seat revenue is
    # resolved here where a licences lookup is acceptable.
    #
    # ⚠️ A licences outage must degrade THIS LINE ONLY, never the whole overview:
    # a tenant whose seat count cannot be fetched contributes no seat revenue and
    # the page still renders. That is the opposite of `close_period`, which
    # refuses — the difference is that nothing here is frozen. See
    # services.seats' module docstring.
    seat_rates = await seat_rates_all_tenants(
        session, month_start=month_start, month_end=month_end, bill_ts=bill_ts
    )
    seat_fee_by_tenant: dict[str, Decimal] = {}
    for _tid, _rates in seat_rates.items():
        try:
            _seats = await get_seat_count(_tid, settings)
        except SeatCountUnavailableError:
            logger.warning("overview_seat_count_unavailable", tenant_id=_tid)
            continue
        # ⭐ Σ q(rate × seats), PER TERM — the fold `generate_billing` charges on
        # (`seat_fee_total`). Multiplying one SUMMED rate and quantising once is
        # `q(Σ rate × seats)`, which differs by a cent per term the moment any
        # `rate × seats` carries sub-cent scale, and the portal would then show a
        # different figure from the statement it is meant to reconcile to.
        # ⛔ Do NOT "simplify" this back to `sum(_rates) * _seats`.
        seat_fee_by_tenant[_tid] = sum(
            (quantize_money(_rate * _seats) for _rate in _rates), Decimal("0")
        )
    # Included-usage allowances (per-tenant, per-service_key) so the overview's
    # customer totals stay in parity with the per-tenant invoice: the allowance
    # nets against LLM incurred before markup, exactly as in generate_billing.
    allowance_by_tenant = await active_allowances_all_tenants(
        session, month_start=month_start, month_end=month_end, bill_ts=bill_ts
    )

    all_tenants = (
        set(usage_by_tenant)
        | {c.tenant_id for c in cost_rows}
        | set(access_by_tenant)
        # A tenant billed on seats alone, with no usage and no flat fee, is still
        # a paying tenant and must appear in the platform total.
        | set(seat_rates)
    )
    # Legacy per-tenant markup fallback (used when only the global-default rule
    # matches) + one in-memory resolver loaded with every tenant's rules.
    fallback_map = await get_markup_pct_map(sorted(all_tenants), session, settings)
    # HISTORICAL month: resolve markup rules as of the LAST INSTANT OF THE
    # BILLED MONTH (bill_ts), never wall-clock. `generate_billing` already does
    # this, and this overview is supposed to reconcile to the sum of those
    # invoices — resolving at now() meant re-rendering an old month picked up
    # every rule change made since, so the overview and the invoices drifted
    # apart and the same historical month rendered differently each time.
    resolver = await build_markup_resolver(session, ts=bill_ts)
    # Phase 4 byo_mode gate: internal/demo tenants bill $0 (effective mode
    # 'internal'); commercial (byo_required) keep their configured mode.
    byo_map = await get_byo_mode_map(sorted(all_tenants), settings)

    internal_by_tenant: dict[str, Decimal] = defaultdict(Decimal)
    customer_by_tenant: dict[str, Decimal] = defaultdict(Decimal)

    # App-request lines (provider/model NULL).
    for tid, usage_entry in usage_by_tenant.items():
        mode = effective_billing_mode(
            mode_map.get(tid, "standard_markup"), byo_map.get(tid, "byo_required")
        )
        fb = fallback_map.get(tid, settings.default_markup_pct)
        for svc_key, count in usage_entry["svc_counts"].items():
            # NO RATE IS NOT A RATE OF ZERO. `.get(key, Decimal("0"))` silently
            # invoices an unmapped service_key at $0.00, and that $0 is a
            # FABRICATED number rather than a priced one — indistinguishable
            # downstream from a service genuinely configured free.
            #
            # ⚠️ It also SUPPRESSES ITS OWN ALARM. `services.markup.
            # compute_billed_line` raises `markup_zero_margin` only when
            # `incurred_usd > 0` (correctly — a genuinely $0 line is not a margin
            # event), so a line fabricated at $0 here is the one case that
            # signal structurally cannot report. The miss therefore has to be
            # counted HERE, where the difference between "absent" and "zero" is
            # still knowable; three lines further on it is gone forever.
            rate = app_rates.get(svc_key)
            if rate is None:
                observability_metrics.app_rate_missing_total.labels(
                    service_key=svc_key or "none", surface="overview"
                ).inc()
                logger.warning(
                    "app_rate_missing",
                    service_key=svc_key,
                    request_count=count,
                    tenant_id=tid,
                    note=(
                        "no AppCostRate for this service_key; the per-request "
                        "line is being charged at $0.00 by default, not by "
                        "configuration"
                    ),
                )
                rate = Decimal("0")
            incurred = rate * count
            internal_by_tenant[tid] += incurred
            billed, _pct, _rule = compute_billed_line(
                resolver,
                mode,
                tenant_id=tid,
                incurred_usd=incurred,
                fallback_pct=fb,
                # ⭐ EXPLICIT `None`, AND NOT A PLACEHOLDER FOR A LOOKUP. These
                # rows come from `billing_metering_daily`, which deliberately
                # carries NO `project_id` column (alembic 0036: money is
                # attributed per project, entitlement is enforced per licence —
                # a project column on a QUOTA key splits a cap, a fail-open,
                # rather than a bill). An app-request line therefore has no
                # project dimension to assert, exactly as it has no provider and
                # no BYO one, and `_matches` must refuse a project-pinned rule
                # for it rather than guess a project. Same call, same reason, as
                # services.billing.generate_billing's app-request line.
                # ⛔ Do NOT "complete" this by joining a project in from
                # CostDaily: that prices request volume against a project the
                # request ledger never claimed.
                project_id=None,
                service_key=svc_key,
                request_count=count,
            )
            customer_by_tenant[tid] += billed

    # LLM cost lines.
    #
    # ⭐ THE ALLOWANCE IS ALLOCATED PER TENANT, ACROSS THAT TENANT'S WHOLE MONTH,
    # before any line is priced — parity with the per-tenant invoice, which does
    # the same thing for the same reason. The credit is spent ahead of a PER-LINE
    # markup rate, so a sequential drain made "which line absorbs it" (and
    # therefore the total) a function of row order. Grouping the rows by tenant
    # here reorders nothing that matters: every accumulation below is additive.
    idx_by_tenant: dict[str, list[int]] = defaultdict(list)
    for i, c in enumerate(cost_rows):
        idx_by_tenant[c.tenant_id].append(i)
    eff_incurred_by_row: list[Decimal] = [Decimal("0")] * len(cost_rows)
    for tid, idxs in idx_by_tenant.items():
        allocation = allocate_allowance(
            allowance_by_tenant.setdefault(tid, {}),
            [(cost_rows[i].service_key, Decimal(cost_rows[i].cost or 0)) for i in idxs],
        )
        for i, (eff, _applied) in zip(idxs, allocation, strict=True):
            eff_incurred_by_row[i] = eff

    for row_index, c in enumerate(cost_rows):
        tid = c.tenant_id
        mode = effective_billing_mode(
            mode_map.get(tid, "standard_markup"), byo_map.get(tid, "byo_required")
        )
        fb = fallback_map.get(tid, settings.default_markup_pct)
        incurred = c.cost or Decimal("0")
        internal_by_tenant[tid] += incurred
        # Allowance-netted incurred for THIS row; internal_cost above stays raw.
        eff_incurred = eff_incurred_by_row[row_index]
        # ⭐ THE SENTINEL IS UNWRAPPED BACK TO `None` BEFORE IT REACHES THE
        # MATCHER — the same idiom as services.billing.generate_billing.
        # `billing_cost_daily.project_id` is NOT NULL and carries
        # `NULL_PROJECT_ID` when no project was asserted; that sentinel is a
        # storage device for the rollup's `ON CONFLICT` (a NULL never equals
        # itself in a plain UNIQUE), NOT a project id. `markup_rules.project_id`
        # is nullable and uses NULL as the wildcard, so handing `_matches` the
        # raw sentinel would ask it to find a literal project whose id is
        # `...0004`: the tenant-wide rule would stop matching project-less spend
        # and an operator could write a rule that prices the sentinel.
        proj_uuid = c.project_id if c.project_id != NULL_PROJECT_ID else None
        billed, _pct, _rule = compute_billed_line(
            resolver,
            mode,
            tenant_id=tid,
            incurred_usd=eff_incurred,
            fallback_pct=fb,
            project_id=proj_uuid,
            service_key=c.service_key,
            provider=c.provider,
            model=c.served_model,
            byo=c.byo,
            input_tokens=int(c.inp or 0),
            output_tokens=int(c.outp or 0),
        )
        customer_by_tenant[tid] += billed

    tenants = []
    platform_internal = Decimal("0")
    platform_customer = Decimal("0")
    # UNATTRIBUTED COST, MADE VISIBLE.
    #
    # `license_attributed_clause` above drops every sentinel-licence row, which
    # is correct for system/S2S cost and wrong for a licensed customer whose
    # x-license-id was dropped in transit -- and until `attribution_status`
    # existed the two were the same value. This second pass reports the split.
    # It reads the SENTINEL rows only, so it can never double-count anything the
    # cost_stmt above already counted, and its output is never folded into
    # `internal_cost_usd`/`customer_cost_usd` or the platform totals: no invoice
    # line moves because of it.
    unattributed_stmt = (
        select(
            CostDaily.tenant_id,
            CostDaily.attribution_status,
            func.sum(CostDaily.total_cost_usd).label("cost"),
        )
        .where(
            CostDaily.bucket_date >= month_start,
            CostDaily.bucket_date < month_end,
            CostDaily.license_id == NULL_LICENSE_ID,
        )
        .group_by(CostDaily.tenant_id, CostDaily.attribution_status)
    )
    unlicensed_by_tenant: dict[str, Decimal] = defaultdict(Decimal)
    platform_system = Decimal("0")
    for row in (await session.execute(unattributed_stmt)).all():
        if row.attribution_status == ATTRIBUTION_UNLICENSED:
            unlicensed_by_tenant[row.tenant_id] += row.cost
        else:
            # 'system' and the pre-migration 'unknown' both report as system:
            # 'unknown' means no rollup has re-stamped that day, and reporting an
            # unclassified row as a customer's unlicensed spend would be a claim
            # the column cannot support.
            platform_system += row.cost
    # A tenant whose ONLY spend this month was unlicensed has no row in
    # `cost_rows` and no metering row either, so without this it would be absent
    # from the overview entirely -- which is exactly the invisibility being
    # fixed. Measured on prod: 22 tenants carry unlicensed cost.
    all_tenants |= set(unlicensed_by_tenant)

    platform_unlicensed = Decimal("0")
    for tid in sorted(all_tenants):
        internal = internal_by_tenant.get(tid, Decimal("0"))
        customer = customer_by_tenant.get(tid, Decimal("0"))
        # Fold in the fixed access fee (pure revenue, no internal cost) unless the
        # tenant bills $0 as internal/demo.
        mode = effective_billing_mode(
            mode_map.get(tid, "standard_markup"), byo_map.get(tid, "byo_required")
        )
        if mode != "internal":
            # ⭐ QUANTIZE THE LEAVES, THEN SUM — the same fold as the invoice
            # (services/billing.py, `access_total_charged` / `seat_fee_total_charged`).
            # The access fee and the seat fee enter a customer total as the CENTS
            # THEY ARE CHARGED AS, never as their sub-cent pre-images, because
            # those two cents are what the `access_charge_usd` / `seat_fee_usd`
            # invoice COLUMNS carry. Adding the pre-images here made this reader
            # compute `q(B + A + S)` against the invoice's `q(B + q(A) + q(S))`:
            # measured, an access charge of 10.005 plus a seat fee of 20.005
            # invoices $30.02 and rendered $30.01 on /billing/overview — the
            # portal understating a real bill by a cent, on exactly the tenants
            # whose fees carry sub-cent scale.
            #
            # This was dormant, not absent, before the invoice fold changed:
            # while every billed line was itself a whole cent the two forms
            # could only differ by a cent MULTIPLE, i.e. usually zero.
            # ⛔ Do NOT "simplify" this back to `+= access_by_tenant.get(...)`.
            # ⭐ Σ q(fee), PER TERM — the fold `generate_billing` charges on.
            # `active_access_charges_all_tenants` hands back the individual
            # amounts precisely so this sum can be taken over the CENTS each
            # term is charged as; folding one pre-summed figure would be
            # `q(Σ fee)` and differ from the statement by a cent per term.
            customer += sum(
                (quantize_money(_fee) for _fee in access_by_tenant.get(tid, ())), Decimal("0")
            )
            # Already Σ q(rate × seats) from the seat fold above — this quantize
            # is IDEMPOTENT and kept only so the two fee terms read alike.
            customer += quantize_money(seat_fee_by_tenant.get(tid, Decimal("0")))
        # The cent this tenant's row RENDERS. Computed once and used for both the
        # row and the roll-up, so the two cannot drift apart by construction.
        customer_charged = quantize_money(customer)
        # ⭐ THE INTERNAL LEAF IS A DISPLAYED FIGURE TOO, SO IT IS QUANTIZED TOO.
        # Same rule, same shape and the same one-place-computed discipline as
        # `customer_charged` immediately above, and as the invoice's
        # `internal_cost_q` (services/billing.py) — which quantizes at the point
        # of storage precisely so `customer − internal = margin` holds at the
        # cent instead of usually.
        #
        # Leaving this raw made every row MIXED FORM: one column in cents, the
        # other at Numeric(20,6)/(20,8) scale, and a margin cell computed as
        # `q(cents − raw)` that NO arithmetic on the two displayed numbers
        # reproduces. Measured on /dashboard/: internal 20.005000 against
        # customer 60.01 rendered a $40.01 margin where the row's own two
        # columns subtract to $40.00, and margin % took its ratio over a raw
        # numerator AND a raw denominator while the cell beside it was rounded.
        # Sub-cent internal cost is not lost — it stays raw per-licence on
        # `costs.llm_cost_usd` / `costs.app_request_cost_usd`, exactly as the
        # invoice fold documents.
        # ⛔ Do NOT "simplify" this back to `platform_internal += internal`.
        internal_charged = quantize_money(internal)
        platform_internal += internal_charged
        # ⭐ A DISPLAYED TOTAL IS THE SUM OF THE DISPLAYED COMPONENTS. Accumulating
        # the RAW `customer` here and quantizing once at the end made the footer
        # `q(Σ customer_i)` while every row above it was `q(customer_i)` — `qΣ`
        # against `Σq`. Two tenants at 30.005 render 30.01 + 30.01 = $60.02 under
        # a platform total of $60.01, and an operator cannot reconcile a table
        # whose column does not add up to its own footer. The error grows with
        # tenant count (up to half a cent each), so it is worst at the scale
        # where it is least likely to be spotted by hand.
        platform_customer += customer_charged
        tenants.append(
            BillingOverviewEntry(
                tenant_id=tid,
                total_requests=usage_by_tenant.get(tid, {"requests": 0})["requests"],
                internal_cost_usd=internal_charged,
                # The raw pre-image, carried beside the cent it renders as so a
                # reader can tell "no internal cost" from "under a cent of
                # internal cost". See the field's docstring; it is never summed.
                internal_cost_exact_usd=internal,
                customer_cost_usd=customer_charged,
                unlicensed_cost_usd=unlicensed_by_tenant.get(tid, Decimal("0")),
            )
        )
        platform_unlicensed += unlicensed_by_tenant.get(tid, Decimal("0"))

    return BillingOverview(
        month=month,
        tenants=tenants,
        # Already a sum of whole cents (the fold above accumulates
        # `internal_charged`), so this quantize is IDEMPOTENT — it only fixes the
        # display exponent at two places, including for the empty-tenant case.
        # The accumulation is what makes the total equal its own column.
        platform_total_internal_usd=quantize_money(platform_internal),
        # `platform_customer` is already a sum of whole cents (see the fold
        # above), so this quantize is IDEMPOTENT — it fixes the display exponent
        # at two places and cannot move the figure. It is deliberately not the
        # thing that makes the total right; the accumulation is.
        platform_total_customer_usd=quantize_money(platform_customer),
        platform_total_unlicensed_usd=platform_unlicensed,
        platform_total_system_usd=platform_system,
    )


@router.get(
    "/overview",
    response_model=Envelope[BillingOverview],
    operation_id="get_billing_overview",
)
async def billing_overview(
    request: Request,
    month: str = Query(default_factory=_current_month, pattern=r"^\d{4}-\d{2}$"),
    session: AsyncSession = Depends(get_session),
    _user: dict = Depends(require_admin),
) -> dict:
    """All-tenant billing overview for a month (operator view)."""
    body = await compute_billing_overview(month, session)
    return envelope(body, request, total=len(body.tenants))


# --------------------------------------------------------------------------- #
# Invoices
#
# generate_billing() above is a REPORT: it recomputes a month every time it is
# called, from mutable markup rules and a rollup that reprice can restate. These
# routes are the other half — they write one such computation down so the number
# stays the number. See services/invoices.py.
#
# ⛔ The WRITE routes take require_pricing_writer, NOT require_admin. Minting a
# customer's statement is a money-config write, and auth_deps is explicit that a
# service account allowed to seed provider cost must not thereby be able to
# change what a customer is charged.
# --------------------------------------------------------------------------- #


def _invoice_response(inv) -> InvoiceResponse:  # noqa: ANN001 - ORM row
    """Project a BillingInvoice row onto the wire model."""
    return InvoiceResponse(
        id=inv.id,
        tenant_id=inv.tenant_id,
        tenant_name=inv.tenant_name,
        period_start=inv.period_start,
        period_end=inv.period_end,
        status=inv.status,
        invoice_number=inv.invoice_number,
        currency=inv.currency,
        subtotal_usd=inv.subtotal_usd,
        total_usd=inv.total_usd,
        access_charge_usd=inv.access_charge_usd,
        platform_fee_usd=inv.platform_fee_usd,
        markup_pct=inv.markup_pct,
        effective_markup_pct=inv.effective_markup_pct,
        markup_source=inv.markup_source,
        excluded_unbillable_usd=inv.excluded_unbillable_usd,
        excluded_unbillable_rows=inv.excluded_unbillable_rows,
        excluded_by_status=inv.excluded_by_status,
        snapshot_sha256=inv.snapshot_sha256,
        generator_version=inv.generator_version,
        created_at=inv.created_at,
        issued_at=inv.issued_at,
        voided_at=inv.voided_at,
        void_reason=inv.void_reason,
    )


@router.post(
    "/invoices/close",
    response_model=Envelope[InvoiceResponse],
    operation_id="close_billing_invoice",
)
async def close_invoice(
    request: Request,
    body: CloseInvoiceRequest,
    session: AsyncSession = Depends(get_session),
    actor: dict = Depends(require_pricing_writer),
) -> dict:
    """Freeze one tenant-month into a persisted statement.

    IDEMPOTENT, and deliberately 200 rather than 201 or 409: a repeat call
    returns the existing live invoice for the period unchanged and does NOT
    recompute. Recomputing would produce a different answer whenever a markup
    rule moved in between, which is the exact failure this object exists to
    prevent.

    Refuses rather than degrades in five cases. A period whose ``total_usd`` is
    not the sum of the component columns that explain it is an
    InvoiceMoneyImbalanceError -> 409: the statement does not add up, and the row
    is immutable and drawn from a gapless series once written, so there is no
    edit afterwards. A period containing a day with
    no rollup rows at all is a BucketGapError -> 409: the source is absent for
    that date, so any total is short by an unknown amount and a statement that
    cannot say so is worse than no statement. A period with billable requests on
    a service_key that has no billing_app_cost_rates row is an
    UnpricedServiceKeyError -> ALSO 409, and for the same reason: that app line
    would be invoiced at a fabricated $0.00, indistinguishable on the statement
    from a deliberately free service, on an invoice number that is gapless and
    irreversible. A period holding cost events that no provider_pricing row could
    price (billing_cost_daily.unpriced_event_count > 0) is an
    UnpricedProviderCostError -> ALSO 409: the rollup's coalesce(sum(cost_usd), 0)
    has already turned that unknown spend into a hard $0.000000, so closing would
    freeze "free" where the truth is "unknown" — understating internal cost and
    overstating margin. ⚠️ It is a DISTINCT error from UnpricedServiceKeyError and
    is mapped separately on purpose: the remedy is a row in provider_pricing, not
    in billing_app_cost_rates, and an operator handed one message for the other
    edits the wrong table. A period that is not one calendar month is an
    InvoicePeriodError -> 422 rather than being widened to the month containing
    it.
    """
    settings = get_settings()
    token_provider = getattr(request.app.state, "service_token_provider", None)
    try:
        inv = await close_period(
            session,
            tenant_id=body.tenant_id,
            period_start=body.period_start,
            period_end=body.period_end,
            settings=settings,
            token_provider=token_provider,
            actor=actor.get("sub") if isinstance(actor, dict) else None,
            issue_immediately=body.issue_immediately,
        )
    except (
        BucketGapError,
        UnpricedServiceKeyError,
        UnpricedProviderCostError,
        # ⛔ MAPPED, NOT LEFT TO FALL THROUGH — the comment below explains why an
        # unmapped InvoiceError subclass becomes a 500 here, and that reading
        # would be actively misleading for this one: "billing is broken" instead
        # of "billing refused to freeze a statement whose components do not
        # account for its total". 409 like its three siblings: the request is
        # well-formed and the period is a legal shape; what blocks it is the state
        # of the MONEY. The detail string already names every component and the
        # signed residual, so the caller need not re-run the close to see it.
        InvoiceMoneyImbalanceError,
    ) as exc:
        raise HTTPException(status_code=status.HTTP_409_CONFLICT, detail=str(exc)) from exc
    # ⛔ MAPPED EXPLICITLY, ORDERED BEFORE THE PARENT ARMS, AND NOT LEFT TO FALL
    # THROUGH. `InvoiceError` is caught in exactly one place in src/ — the sweep in
    # close_due_periods — so an unmapped subclass reaching THIS handler becomes a
    # 500: an operator's manual close of an unregistered or archived tenant would
    # read as "billing is broken" rather than "billing refused, correctly".
    #
    # Both are 409, matching the three gates above them: the request is
    # well-formed and the period is a legal shape (that is what 422 means here);
    # what blocks it is the CURRENT STATE of the registry or of the money.
    # ⚠️ TenantNotInvoiceableError carries `.reason`, so the detail string already
    # names which of the two refusals fired — the caller does not have to parse it.
    except (TenantNotInvoiceableError, PeriodNotBillableError) as exc:
        raise HTTPException(status_code=status.HTTP_409_CONFLICT, detail=str(exc)) from exc
    except InvoicePeriodError as exc:
        raise HTTPException(
            status_code=status.HTTP_422_UNPROCESSABLE_ENTITY, detail=str(exc)
        ) from exc

    await audit.record(
        session,
        request,
        action="billing.invoice.close",
        resource_type="invoice",
        resource_id=str(inv.id),
        tenant_id=inv.tenant_id,
        after={"status": inv.status, "invoice_number": inv.invoice_number},
    )

    # ⛔ THE COMMIT IS HERE, AND IT IS NOT OPTIONAL. Nothing above it persists.
    #
    # `db.get_session` is `async with _session_factory() as session: yield
    # session` — it never commits, and AsyncSession.__aexit__ CLOSES, which rolls
    # back whatever transaction is open. `close_period` only flushes (and
    # `services.audit.record` only flushes, by its own documented contract: "the
    # route handler is responsible for committing"). So without this line this
    # endpoint did its whole job — drew a number from the gapless counter, built
    # the row, wrote the audit row — and then threw all of it away on the way out.
    #
    # ⚠️ A 200 WITH NO COMMIT IS THE WORST AVAILABLE FAILURE, which is why this
    # was invisible for so long: the caller is handed an `invoice_number` for a
    # statement that does not exist and never did. An operator who has that
    # number in hand has every reason to believe it was issued, and `reproduce`
    # will 404 on it. A 500 would have been strictly better.
    #
    # ⛔ It is explicit HERE rather than a commit inside `get_session` on purpose.
    # `get_session` is the session dependency for EVERY endpoint in this service,
    # read routes included; committing there would put a write boundary on a
    # couple of dozen pure readers and silently persist any accidental ORM
    # mutation they made. The blast radius of a one-line change in `get_session`
    # is the whole service. Every sibling write router — admin, config, budget,
    # quota — commits explicitly at the end of its handler; this file was the
    # omission, not the precedent.
    #
    # The automated sweep was unaffected and that is what hid this: `invoices.
    # close_due_periods` owns its own session and commits at
    # services/invoices.py:1671. So the CronJob path persisted and the manual
    # operator path did not — the one the runbook points at for the first invoice.
    #
    # ⚠️ ORDERING: after `audit.record` (so the audit row rides the same
    # transaction — if this commit fails the audit row goes with it, which is the
    # point), and the wire model is built BEFORE the commit. `db.py:413` sets
    # `expire_on_commit=False`, so reading `inv`'s attributes after a commit is
    # safe TODAY; capturing first means this route does not silently depend on
    # that setting, because if it is ever flipped, every attribute read in
    # `_invoice_response` becomes a lazy SELECT on a just-committed session.
    payload = _invoice_response(inv)
    await session.commit()
    return envelope(payload, request)


@router.post(
    "/invoices/{invoice_id}/issue",
    response_model=Envelope[InvoiceResponse],
    operation_id="issue_billing_invoice",
)
async def issue_invoice(
    request: Request,
    invoice_id: UUID,
    session: AsyncSession = Depends(get_session),
    actor: dict = Depends(require_pricing_writer),
) -> dict:
    """Allocate the next number and move draft -> issued.

    Re-issuing an already-issued invoice is a NO-OP returning the same number and
    takes no second draw from the counter, so a client retry cannot open a hole
    in a series whose whole purpose is to have none.
    """
    try:
        inv = await issue(
            session,
            invoice_id=invoice_id,
            actor=actor.get("sub") if isinstance(actor, dict) else None,
        )
    except InvoiceStateError as exc:
        raise HTTPException(status_code=status.HTTP_409_CONFLICT, detail=str(exc)) from exc

    await audit.record(
        session,
        request,
        action="billing.invoice.issue",
        resource_type="invoice",
        resource_id=str(inv.id),
        tenant_id=inv.tenant_id,
        after={"status": inv.status, "invoice_number": inv.invoice_number},
    )

    # ⛔ COMMIT — see the long note on `close_invoice` above for the whole
    # reasoning. In short: `get_session` does not commit and `AsyncSession`'s exit
    # rolls back, `services.invoices.issue` and `services.audit.record` only
    # flush, and it is explicit here rather than in `get_session` because that
    # dependency is shared with every read route in the service.
    #
    # ⚠️ THIS SITE IS THE SHARPEST CASE OF THE 200-WITHOUT-A-COMMIT FAILURE.
    # Issuing is the act that mints the number: without the commit this route
    # returned `invoice_number` in the body while the UPDATE that set it, AND the
    # counter increment it was drawn from, were both rolled back. The caller holds
    # a number; the series never advanced; the invoice is still a draft. (The
    # series stays gapless through that — `issue` increments the counter row
    # inside the caller's transaction precisely so a rollback takes the draw with
    # it — so the damage is a phantom number, not a hole.) The automated path is
    # unaffected because `close_due_periods` commits its own session at
    # services/invoices.py:1671.
    #
    # Built before the commit for the same reason as above (`expire_on_commit=
    # False` at db.py:413 makes it safe either way; not depending on that is free).
    payload = _invoice_response(inv)
    await session.commit()
    return envelope(payload, request)


@router.post(
    "/invoices/{invoice_id}/void",
    response_model=Envelope[InvoiceResponse],
    operation_id="void_billing_invoice",
)
async def void_billing_invoice(
    request: Request,
    invoice_id: UUID,
    body: VoidInvoiceRequest,
    session: AsyncSession = Depends(get_session),
    actor: dict = Depends(require_pricing_writer),
) -> dict:
    """Move a statement to the terminal void status, with a recorded reason.

    The number is NOT returned to the series. Gapless means gapless in
    ALLOCATION, not in liveness — the void row keeps its number and stays
    readable, which is what lets a reader see that a statement was superseded
    rather than destroyed. The period leaves the open-period uniqueness set, so
    it can be closed again.
    """
    try:
        inv = await void_invoice(
            session,
            invoice_id=invoice_id,
            reason=body.reason,
            actor=actor.get("sub") if isinstance(actor, dict) else None,
        )
    except InvoiceStateError as exc:
        raise HTTPException(status_code=status.HTTP_409_CONFLICT, detail=str(exc)) from exc

    await audit.record(
        session,
        request,
        action="billing.invoice.void",
        resource_type="invoice",
        resource_id=str(inv.id),
        tenant_id=inv.tenant_id,
        after={"status": inv.status, "void_reason": inv.void_reason},
    )

    # ⛔ COMMIT — see the long note on `close_invoice` above. `get_session` never
    # commits and the session's exit rolls back; `services.invoices.void` and
    # `services.audit.record` only flush; it is explicit here rather than in
    # `get_session` because that dependency is shared with every read route.
    #
    # ⚠️ Here a 200 without a commit reports the OPPOSITE of the truth about an
    # issued statement: the caller is told status `void` with the reason recorded,
    # while the row stays `issued` and live, still holds the period against
    # `ux_billing_invoices_open_period` (so the re-close that is the whole point of
    # voiding is refused), and the audit row that would have said who withdrew it
    # and why is gone too. The automated sweep never voids and commits its own
    # session regardless (services/invoices.py:1671), so nothing about this was
    # visible from the CronJob path.
    #
    # Built before the commit for the same reason as above (`expire_on_commit=
    # False` at db.py:413 makes it safe either way; not depending on that is free).
    payload = _invoice_response(inv)
    await session.commit()
    return envelope(payload, request)


@router.get(
    "/invoices/{invoice_id}/reproduce",
    response_model=Envelope[InvoiceDocument],
    operation_id="reproduce_billing_invoice",
)
async def reproduce_invoice(
    request: Request,
    invoice_id: UUID,
    session: AsyncSession = Depends(get_session),
    _tenant: str = Depends(require_tenant),
) -> dict:
    """Re-render a closed statement from the ROW, reading nothing else.

    This is the route the whole table exists for. It touches no rollup, no
    markup rule and no licences call, so the answer cannot drift with them.
    ``digest_matches`` is recomputed here and tells the caller whether the frozen
    pair still hashes to its stored digest.
    """
    try:
        doc = await reproduce(session, invoice_id=invoice_id)
    except InvoiceStateError as exc:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail=str(exc)) from exc

    enforce_tenant_isolation(request, doc["tenant_id"], _tenant)
    return envelope(InvoiceDocument(**doc), request)


# --------------------------------------------------------------------------- #
# Invoice READS — the tenant-admin panel's three questions
#
# ⛔ REGISTRATION ORDER IS LOAD-BEARING AND IT IS THE FIRST WAY THIS BLOCK GOES
# WRONG. FastAPI matches routes in the order they are added, so
# ``GET /invoices/preview`` MUST be declared before ``GET /invoices/{invoice_id}``.
# Reversed, ``/invoices/preview`` is captured by the parameterised route,
# ``invoice_id: UUID`` fails to parse the literal string "preview", and the
# endpoint answers **422 for every single call** — a route that exists, appears
# in ``docs/openapi.json``, and can never be reached. ⚠️ It fails loudly rather
# than silently only because the path parameter is a UUID; had it been ``str``
# the preview would have become a 404-on-a-missing-invoice instead, which reads
# like "no invoice yet" and is the shape nobody notices. Do not reorder these
# three, and do not "tidy" them next to their write siblings above.
#
# ⛔ ALL THREE TAKE ``require_service_caller``, NOT ``require_tenant``, AND THE
# DIFFERENCE IS THE WHOLE ACCESS DECISION. ``require_tenant`` accepts the
# ``X-Tenant-Id`` header that license-authz injects after validating a LICENCE
# token, and every member of a tenant holds one — so under it any seat could
# read the workspace's statements. A statement names the platform fee, the seat
# count, the negotiated markup and the realised margin; that is commercial data
# for a tenant ADMIN, and CommonPractice's backend already presents an internal
# SA token to proxy such reads for its own admins (the established path, on
# ``GET /api/costs/tenant/{id}/actors``). Same dependency, same reason.
# ⚠️ The header is still NOT the credential here: ``require_service_caller``
# reads it only to choose between two REFUSALS (403 vs 401), never to grant one.
#
# ⛔ AND THEY ARE READS. Nothing below writes a row, moves a status, or draws
# from ``billing_invoice_counters``. ``get_session`` never commits and the
# session's exit rolls back (see the long note on ``close_invoice``), so the
# absence of a ``session.commit()`` in this block is the correctness property,
# not an omission — do not add one.
# --------------------------------------------------------------------------- #


def _invoice_summary(inv) -> InvoiceSummary:  # noqa: ANN001 - ORM row
    """Project a ``BillingInvoice`` row onto the READ wire model.

    ⭐ BUILT ON ``_invoice_response``, NEVER BESIDE IT. ``InvoiceSummary``
    subclasses ``InvoiceResponse``, so every inherited field is projected by
    exactly one expression — the one above — and this function adds only the
    columns the parent does not carry. Re-listing ``id``/``status``/``total_usd``
    here would be a second projector for the same row, free to drift from the
    first with no test able to see it.
    """
    return InvoiceSummary(
        **_invoice_response(inv).model_dump(),
        internal_cost_usd=inv.internal_cost_usd,
        margin_usd=inv.margin_usd,
        llm_usage_usd=inv.llm_usage_usd,
        llm_markup_usd=inv.llm_markup_usd,
        app_request_usage_usd=inv.app_request_usage_usd,
        app_request_markup_usd=inv.app_request_markup_usd,
        seat_fee_usd=inv.seat_fee_usd,
        # ⚠️ NO ``or 0``. NULL here means "seats were not part of this
        # statement's basis", which is a different fact from zero seats — see
        # the column's own comment and ``InvoiceSummary.seat_count``.
        seat_count=inv.seat_count,
        prepaid_credit_applied_usd=inv.prepaid_credit_applied_usd,
        billing_mode=inv.billing_mode,
    )


@router.get(
    "/invoices",
    response_model=Envelope[list[InvoiceSummary]],
    operation_id="list_billing_invoices",
)
async def list_invoices(
    request: Request,
    tenant_id: str = Query(description="The tenant whose statements to list."),
    limit: int = Query(default=50, ge=1, le=200),
    session: AsyncSession = Depends(get_session),
    _service: dict = Depends(require_service_caller),
) -> dict:
    """One tenant's statements, newest period first.

    ⛔ THE ``tenant_id`` PREDICATE IS THE ISOLATION, AND IT IS NOT OPTIONAL.
    ``require_service_caller`` proves the caller is an internal service; it says
    NOTHING about which tenant they may read, because an internal SA token
    legitimately spans the estate. Every row returned here is therefore selected
    by an explicit ``WHERE tenant_id =`` rather than filtered afterwards — a
    post-filter is one forgotten ``continue`` away from leaking another
    customer's platform fee and margin. ``enforce_tenant_isolation`` below is the
    second, independent gate for the proxying case.

    ⭐ VOIDED AND DRAFT STATEMENTS ARE INCLUDED, DELIBERATELY. ``void`` is a
    terminal status that KEEPS its invoice number and stays readable — that is
    what lets a reader see a statement was superseded rather than destroyed (see
    ``void_billing_invoice``) — and hiding it here would make a re-closed period
    look like it had only ever been billed once. ``status`` discriminates; the
    caller filters if it wants to.

    ⚠️ THE SECOND ORDERING KEY IS NOT DECORATION. ``ix_billing_invoices_tenant_
    period`` is ``(tenant_id, period_start DESC)``, which is the index this scan
    wants, but ``period_start`` is NOT unique per tenant: voiding a statement
    drops it out of ``ux_billing_invoices_open_period`` precisely so the period
    can be closed AGAIN, so a re-billed month holds two or more rows sharing
    every ordering column. Without ``created_at DESC`` the planner is free to
    return them in either order, and a panel showing "the latest invoice for
    September" would show the VOIDED one on some page loads and the live one on
    others, non-deterministically. ``id`` breaks the residual tie so the order is
    total.
    """
    tenant_id = resolve_tenant_or_400(tenant_id)
    # Belt-and-braces, and NOT redundant with the gate above — the same pairing
    # as ``GET /api/costs/tenant/{id}/actors``. A service caller proxying for
    # tenant A may legitimately send ``X-Tenant-Id: A``; this stops it then
    # listing tenant B's statements under that assertion. With no header present
    # (an operator's admin SA token) the helper returns immediately.
    enforce_tenant_isolation(request, tenant_id, request.headers.get("x-tenant-id") or "")

    stmt = (
        select(BillingInvoice)
        .where(BillingInvoice.tenant_id == tenant_id)
        .order_by(
            BillingInvoice.period_start.desc(),
            BillingInvoice.created_at.desc(),
            BillingInvoice.id.desc(),
        )
        .limit(limit)
    )
    rows = list((await session.execute(stmt)).scalars().all())
    payload = [_invoice_summary(inv) for inv in rows]
    # ⚠️ ``total`` IS THE SIZE OF THIS PAGE, NOT THE TENANT'S INVOICE COUNT —
    # the same meaning ``billing_overview`` gives it, and the honest one here
    # because ``limit`` has already truncated the set. A reader must not use it
    # to decide whether more pages exist; when ``total == limit`` there may be
    # more, and there is deliberately no COUNT(*) behind it to promise otherwise.
    return envelope(payload, request, total=len(payload))


async def _measure_exclusions(
    session: AsyncSession, *, tenant_id: str, month_start: date, month_end: date
) -> InvoicePreviewExclusions:
    """Spend this tenant METERED in the period and was deliberately NOT billed.

    ⛔ THIS IS THE FIRST DEFINITION OF "EXCLUDED" AS A SELECTED POPULATION IN
    ``src/``, AND THAT IS WHY IT IS A FUNCTION. Every existing site selects the
    COMPLEMENT — ``routers/export.py``, ``services/reprice.py`` and the five raw-SQL
    predicates in ``services/attribution_audit.py`` all filter
    ``excluded_from_billing = false`` — because their job is to agree with the
    invoice. Nothing anywhere sums the parked side per tenant per window; the
    only prior statement of it is ``scripts/unpark_loose_duplicates.py``'s
    ``MEASURE_SQL``, which is a one-off with no tenant and no date range. Named
    once here so the next caller inherits this definition rather than writing a
    third one.

    ⭐ THE BUCKETING MIRRORS ``rollup_daily`` EXACTLY, AND IT HAS TO. That
    aggregate assigns a row to a day by
    ``cast(timezone('UTC', coalesce(event_ts, created_at)) AS date)`` — NOT by
    ``created_at`` — so measuring the parked side on ``created_at`` alone would
    put the two numbers over two different partitions of the same population,
    and the difference between them would stop being the exclusion. The sargable
    LOWER bound on ``created_at`` is kept for partition pruning (the same
    lower-bound-only shape ``rollup_daily`` uses, and for the same reason: an
    upper bound on the partition key would cut off late-arriving rows whose
    resolved time is in the window).

    ⚠️ THE COST AXIS ONLY. ``billing_metering_events`` carries no exclusion
    column at all — verified, and nothing in ``rollup_hourly`` filters usage that
    way — so a per-request app charge cannot be parked today and this is not a
    census of everything unbilled. Do not let a caller render it as one.

    ⚠️ ``sum(cost_usd)`` SKIPS NULLS, and ``cost_usd`` is nullable by design (it
    is the load-bearing "no ProviderPricing matched" sentinel). So a parked row
    that was also UNPRICED contributes to ``excluded_rows`` and not to
    ``excluded_usd``. That is the honest direction — the alternative is
    ``coalesce(..., 0)``, which is precisely the fabricated-zero move this whole
    endpoint exists to refuse — and the row count is what lets a reader see the
    two figures disagree.
    """
    # The resolved event time ``rollup_daily`` buckets on. Built here rather than
    # imported because rollup_engine builds it inline too; if it ever becomes a
    # shared helper, both sites should take it from there.
    event_time = func.coalesce(CostEvent.event_ts, CostEvent.created_at)
    resolved_date = func.cast(func.timezone("UTC", event_time), Date)
    window_start = datetime(month_start.year, month_start.month, month_start.day, tzinfo=UTC)
    in_window = (
        # Partition pruning. LOWER BOUND ONLY — see the docstring.
        CostEvent.created_at >= window_start,
        resolved_date >= month_start,
        resolved_date < month_end,
    )

    # ⛔ PRESENCE IS PROBED SEPARATELY FROM THE EXCLUDED SUM, AND THE ORDER OF
    # THE TWO QUESTIONS IS THE WHOLE CORRECTNESS ARGUMENT. "No parked rows" and
    # "no rows at all" produce the identical empty result from the grouped query
    # below, and they mean opposite things: the first is a measured zero, the
    # second is a window whose partition has been dropped (retention is
    # ``settings.retention_days``, default 90, partition-granular) and whose
    # excluded amount is therefore unknowable. Reporting the second as $0.00
    # would tell a tenant admin nothing was withheld when the truth is that
    # nobody can say.
    present = (
        await session.execute(
            select(func.count())
            .select_from(CostEvent)
            .where(CostEvent.tenant_id == tenant_id, *in_window)
        )
    ).scalar_one()
    if not present:
        return InvoicePreviewExclusions(basis=EXCLUSION_BASIS_UNMEASURABLE)

    rows = (
        await session.execute(
            select(
                CostEvent.exclusion_reason,
                func.count().label("rows"),
                func.sum(CostEvent.cost_usd).label("usd"),
            )
            .where(
                CostEvent.tenant_id == tenant_id,
                *in_window,
                CostEvent.excluded_from_billing.is_(True),
            )
            .group_by(CostEvent.exclusion_reason)
        )
    ).all()

    by_reason: dict[str, Decimal] = {}
    excluded_rows = 0
    excluded_usd = Decimal("0")
    for row in rows:
        excluded_rows += row.rows
        amount = Decimal(row.usd or 0)
        excluded_usd += amount
        # ⚠️ ``exclusion_reason`` IS NULLABLE while ``excluded_from_billing`` is
        # NOT, so the two can disagree: a row parked with no reason recorded is
        # possible in the schema. It is keyed ``"unspecified"`` rather than
        # dropped — a reason-less exclusion still moved money off the bill, and
        # silently omitting it would make ``sum(by_reason.values())`` quietly
        # miss ``excluded_usd``. Today's live reasons are ``health_probe``,
        # ``unregistered_tenant`` and the two 0013 duplicate classes.
        key = row.exclusion_reason or "unspecified"
        by_reason[key] = by_reason.get(key, Decimal("0")) + amount

    return InvoicePreviewExclusions(
        basis=EXCLUSION_BASIS_RAW_EVENTS,
        excluded_usd=excluded_usd,
        excluded_rows=excluded_rows,
        # ⭐ ``{}`` HERE IS A MEASURED ZERO AND IS THE RIGHT ANSWER. It reads
        # "there were rows in this window and none of them were parked", which is
        # exactly what ``basis == "raw_cost_events"`` licenses. The ``None`` case
        # is the OTHER branch above, and keeping them apart is this field's job.
        by_reason=by_reason,
    )


@router.get(
    "/invoices/preview",
    response_model=Envelope[InvoicePreviewResponse],
    operation_id="preview_billing_invoice",
)
async def preview_invoice(
    request: Request,
    tenant_id: str = Query(description="The tenant whose current bill to preview."),
    period: str = Query(default_factory=_current_month, pattern=r"^\d{4}-\d{2}$"),
    session: AsyncSession = Depends(get_session),
    _service: dict = Depends(require_service_caller),
) -> dict:
    """What this period WOULD bill, computed now and frozen nowhere.

    ⛔ IT CLOSES NOTHING AND CONSUMES NO INVOICE NUMBER. ``generate_billing`` is
    a pure read — SELECTs plus the licences lookups — and this handler adds three
    more SELECTs and no write. In particular it never reaches
    ``services.invoices.issue``, so ``billing_invoice_counters.next_seq`` does
    not move: a caller may preview the same period a thousand times and the
    gapless series is untouched. There is no ``session.commit()`` below on
    purpose; ``get_session`` never commits and the session's exit rolls back, so
    the read path leaves nothing behind even if something upstream dirtied the
    identity map.

    ⭐ THE SEAT COUNT IS THE PERIOD'S, NOT ``now()``'s — AND ``generate_billing``
    ALREADY GUARANTEES THAT, WHICH IS WHY THIS ROUTE REUSES IT RATHER THAN
    REBUILDING A PREVIEW. Its per-seat block resolves every term through
    ``_resolve_seat_count(tenant_id, settings, as_of=bill_ts, **scope)`` where
    ``bill_ts`` is the LAST INSTANT OF THE BILLED MONTH, and ``scope`` comes from
    ``services.seats.seat_scope_for_charge``. That gives two bases, both
    period-anchored and neither of them a wall-clock count:

    * a ``seat:<app>`` term (CommonPractice's shape, written by
      ``PUT /commercial-terms``) → ``seat_basis = "active_during_period"``: that
      one app's seats held at ANY instant in ``[period_start, period_end)``,
      counted as ``human_subjects`` — DISTINCT PEOPLE, not licence rows, so an
      employee revoked and re-licensed inside the month is one seat and a
      service-realm licence is none;
    * an unpinned term (the legacy ``__seat__``, or a NULL ``service_key``) →
      ``seat_basis = "active_licences"``, counted ``as_of`` the last instant of
      the period, also as ``human_subjects`` (#265): ``service`` and
      ``cp-tenant`` subjects are not seats.

    ⚠️ THE ONE HONEST RESIDUAL, STATED RATHER THAN HIDDEN. For a CURRENT period
    the unpinned basis' ``as_of`` is in the FUTURE, and licences resolves a
    future instant against today's rows — so mid-month that count is effectively
    the live one. That is not a defect being papered over: end-of-month seats are
    genuinely unknowable on the 12th, and ``seat_basis`` is on the response so a
    reader can see WHICH basis produced the number instead of assuming. The
    period-scoped basis has no such residual: it counts the window that has
    actually elapsed. ⛔ Do not "fix" this by substituting ``as_of=now()`` — that
    would also change the answer for a CLOSED period, which is the one case
    ``as_of`` exists to get right.

    ⚠️ ``seat_basis = "unavailable"`` is a THIRD outcome and does not mean zero
    seats: it means licences could not be asked, and the report path fails SOFT
    with a $0.00 seat line precisely so a dashboard survives a licences outage.
    ``close_period`` refuses to freeze that state instead. A consumer that
    renders the seat line without reading ``seat_basis`` will show $0.00 for a
    paying tenant during an outage.

    ⛔ AND A MISSING BASIS IS NOT A ZERO BILL. See ``basis`` /
    ``InvoicePreviewResponse``: when nothing underlies the period every money
    field is ``None``, never 0.00.
    """
    tenant_id = resolve_tenant_or_400(tenant_id)
    enforce_tenant_isolation(request, tenant_id, request.headers.get("x-tenant-id") or "")

    year, mon = int(period[:4]), int(period[5:7])
    month_start = date(year, mon, 1)
    month_end = date(year + 1, 1, 1) if mon == 12 else date(year, mon + 1, 1)
    # The same instant ``generate_billing`` and ``close_period`` resolve terms
    # at: the LAST instant of the half-open period, never its exclusive
    # midnight boundary (which sits before a charge created during the period).
    bill_ts = datetime(month_end.year, month_end.month, month_end.day, tzinfo=UTC) - timedelta(
        microseconds=1
    )

    # ⛔ THE BASIS PREDICATE IS THREE COUNTS, NOT ONE, AND THE THIRD IS THE ONE
    # THAT IS EASY TO FORGET. A tenant with an open ``tenant_access_charge`` and
    # zero usage genuinely owes its flat fee — measured on prod, the one tenant
    # with open rows carries $5,000.00/month — so a basis test that looked only
    # at the rollups would report ``no_basis`` and ``None`` money for a real
    # $5,000 bill. Each count is disclosed on the response so a reader can see
    # WHICH of the three was empty rather than only the verdict.
    #
    # ⛔ AND THEY ARE TAKEN IN THIS REQUEST, EVERY TIME. There is deliberately no
    # short-circuit on the period's dates: ``billing_cost_daily`` is empty for
    # August and most of September 2026 after an authorised purge, but a retained
    # dump holds 720 August rollup rows and restoring it is an operator decision
    # that may be taken at any moment. A date-keyed fast path would keep
    # answering "no data" after the rows came back — a cached absence, not a
    # measurement.
    cost_rows = (
        await session.execute(
            select(func.count())
            .select_from(CostDaily)
            .where(
                CostDaily.tenant_id == tenant_id,
                CostDaily.bucket_date >= month_start,
                CostDaily.bucket_date < month_end,
            )
        )
    ).scalar_one()
    metering_rows = (
        await session.execute(
            select(func.count())
            .select_from(MeteringDaily)
            .where(
                MeteringDaily.tenant_id == tenant_id,
                MeteringDaily.bucket_date >= month_start,
                MeteringDaily.bucket_date < month_end,
            )
        )
    ).scalar_one()
    access_terms = len(
        await active_access_charges_for_month(
            session,
            tenant_id=tenant_id,
            month_start=month_start,
            month_end=month_end,
            bill_ts=bill_ts,
        )
    )

    # ⛔ MEASURED INDEPENDENTLY OF THE BASIS VERDICT, AND NOT SKIPPED WHEN THERE
    # IS NONE. A window whose events were ALL parked has ZERO rollup rows by
    # construction (``rollup_daily`` filters them out), so "no basis" and
    # "everything was excluded" are the same empty rollup — and short-circuiting
    # the exclusion query on ``no_basis`` would hide precisely the spend this
    # disclosure exists to surface. Measured fleet-wide 2026-09-17: $118.708744
    # raw against $118.184744 rolled up, the $0.524 difference being 18
    # ``unregistered_tenant`` rows and 2,750 ``health_probe`` rows.
    exclusions = await _measure_exclusions(
        session, tenant_id=tenant_id, month_start=month_start, month_end=month_end
    )

    if not cost_rows and not metering_rows and not access_terms:
        # ⛔ EVERY MONEY FIELD STAYS UNSET, WHICH MEANS ``None`` ON THE WIRE — the
        # defaults on ``InvoicePreviewResponse`` are ``None`` for exactly this
        # construction. ⚠️ DO NOT "COMPLETE" THIS BY CALLING ``generate_billing``
        # AND RETURNING WHAT IT SAYS: it would answer $0.00 for every field,
        # truthfully and uselessly, and a consumer cannot tell that zero from the
        # genuine zero below. ``exclusions`` is still attached, because a window
        # with no rollup may still have parked spend.
        return envelope(
            InvoicePreviewResponse(
                tenant_id=tenant_id,
                period=period,
                period_start=month_start,
                period_end=month_end,
                basis=PREVIEW_BASIS_NONE,
                cost_rollup_rows=cost_rows,
                metering_rollup_rows=metering_rows,
                access_charge_terms=access_terms,
                exclusions=exclusions,
            ),
            request,
        )

    settings = get_settings()
    token_provider = getattr(request.app.state, "service_token_provider", None)
    report = await generate_billing(
        tenant_id, period, session, settings, token_provider=token_provider
    )
    totals = report["totals"]
    return envelope(
        InvoicePreviewResponse(
            tenant_id=tenant_id,
            period=period,
            period_start=month_start,
            period_end=month_end,
            basis=PREVIEW_BASIS_ROLLUP,
            cost_rollup_rows=cost_rows,
            metering_rollup_rows=metering_rows,
            access_charge_terms=access_terms,
            # ⚠️ ``customer_cost_usd`` IS THE TOTAL, and it already INCLUDES the
            # quantised access and seat fees (``generate_billing`` folds
            # ``access_total_charged + seat_fee_total_charged`` into it before
            # storing). The two are broken out below for display; ⛔ summing them
            # onto this figure double-charges the tenant, which is the exact
            # error ``BillingTotals.seat_fee_usd`` warns about.
            total_usd=Decimal(totals["customer_cost_usd"]),
            internal_cost_usd=Decimal(totals["internal_cost_usd"]),
            margin_usd=Decimal(totals["margin_usd"]),
            access_charge_usd=Decimal(totals["access_charge_usd"]),
            # ⚠️ A COMPONENT OF THE BILLED LINES, NOT AN ADDITION TO THEM — the
            # absolute BYO surcharge already sits inside the marked-up usage.
            platform_fee_usd=Decimal(totals["platform_fee_usd"]),
            seat_fee_usd=Decimal(totals["seat_fee_usd"]),
            # ⛔ NO ``or 0`` ON EITHER. ``seat_count=None`` with ``seat_basis=None``
            # means no per-seat term applies; ``None`` with ``"unavailable"``
            # means licences could not be asked. Flattening either to 0 erases the
            # distinction ``billing_invoices.seat_count`` is nullable to preserve.
            seat_count=totals["seat_count"],
            seat_basis=totals["seat_basis"],
            effective_markup_pct=report["effective_markup_pct"],
            billing_mode=report["billing_mode"],
            # PREPAID CREDIT: already netted out of `total_usd`, broken out here
            # with the provisional balance after this period (alembic 0055).
            prepaid_credit_applied_usd=Decimal(totals["prepaid_credit_applied_usd"]),
            prepaid_credit_remaining_usd=Decimal(totals["prepaid_credit"]["remaining_usd"]),
            exclusions=exclusions,
        ),
        request,
    )


@router.get(
    "/invoices/{invoice_id}",
    response_model=Envelope[InvoiceSummary],
    operation_id="get_billing_invoice",
)
async def get_invoice(
    request: Request,
    invoice_id: UUID,
    tenant_id: str = Query(description="The tenant the statement must belong to."),
    session: AsyncSession = Depends(get_session),
    _service: dict = Depends(require_service_caller),
) -> dict:
    """One statement, scoped to the tenant that owns it.

    ⛔ A FOREIGN INVOICE IS **404**, NOT 403, AND THE TENANT IS IN THE ``WHERE``
    CLAUSE RATHER THAN IN AN ``if`` AFTER THE FETCH. Both halves matter:

    * 403 would be an ORACLE. "Forbidden" says the id EXISTS and belongs to
      someone else; 404 says only "not yours, here". Invoice ids are uuid4, so
      enumeration is not the threat — a caller holding a leaked id learning it is
      live is. The two refusals are indistinguishable to a legitimate caller and
      quite different to an illegitimate one.
    * Filtering in SQL means the row for another tenant is never loaded into this
      process at all, so no later edit to this handler — a log line, an error
      message, a debug dump — can leak a field off it. A post-fetch check is one
      forgotten ``raise`` away from returning the row it just rejected.

    ⚠️ REGISTRATION ORDER: this route MUST stay below ``/invoices/preview``. See
    the block comment above ``_invoice_summary``.
    """
    tenant_id = resolve_tenant_or_400(tenant_id)
    enforce_tenant_isolation(request, tenant_id, request.headers.get("x-tenant-id") or "")

    inv = (
        await session.execute(
            select(BillingInvoice).where(
                BillingInvoice.id == invoice_id,
                BillingInvoice.tenant_id == tenant_id,
            )
        )
    ).scalar_one_or_none()
    if inv is None:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="No such invoice for this tenant",
        )
    return envelope(_invoice_summary(inv), request)
