"""System-wide aggregation queries for super-admin dashboards.

Used by ``/api/admin/usage/system`` to surface real provider/incurred cost
across all tenants alongside per-tenant billed cost (markup applied) and
per-provider incurred-only spend (we don't bill providers).

Grand totals join request counts from ``metering_daily`` with cost from
``cost_daily``. Per-provider rows only carry incurred USD because markup
is per-tenant — provider rows would not be billed.
"""

from __future__ import annotations

from datetime import UTC, date, datetime
from decimal import Decimal

from sqlalchemy import func, select
from sqlalchemy.ext.asyncio import AsyncSession

from models.db import NULL_PROJECT_ID, CostDaily, MeteringDaily, TenantConfig
from models.domain import (
    SystemProviderUsage,
    SystemTenantUsage,
    SystemUsageGrandTotal,
    SystemUsageResponse,
)
from services.access_charge import active_allowances_all_tenants, allocate_allowance
from services.billing import get_markup_pct_map
from services.byo_mode import effective_billing_mode, get_byo_mode_map
from services.markup import build_markup_resolver, compute_billed_line, quantize_money
from settings import Settings


def resolve_window(window: str) -> tuple[date, date]:
    """Resolve a window keyword into a (start_date, end_date) pair.

    ``today`` → today only. ``month`` → 1st of this month through today.
    ``Nd`` → N days back through today. Anything else → ValueError, so
    callers can return HTTP 400 with a clear message.
    """
    today = datetime.now(UTC).date()
    if window == "today":
        return today, today
    if window == "month":
        return today.replace(day=1), today
    if window.endswith("d"):
        try:
            n = int(window[:-1])
        except ValueError as exc:
            raise ValueError(f"Invalid window: {window!r}") from exc
        if n <= 0 or n > 365:
            raise ValueError("window must be between 1d and 365d")
        from datetime import timedelta

        return today - timedelta(days=n), today
    raise ValueError(f"Invalid window {window!r}: expected 'today', 'month', or 'Nd' (e.g. '30d')")


async def system_usage(
    session: AsyncSession,
    settings: Settings,
    window: str,
) -> SystemUsageResponse:
    """Aggregate usage + cost across all tenants for the given window.

    Returns:
        - grand_total: sum of requests, tokens, incurred cost, and billed
          cost (with per-tenant markup applied row-by-row before summing).
        - by_tenant: one row per tenant with incurred + markup + billed.
        - by_provider: one row per LLM provider with incurred-only cost.
    """
    start_d, end_d = resolve_window(window)

    # Requests by tenant — from metering_daily (covers every API call,
    # not just LLM-bearing ones).
    req_stmt = (
        select(
            MeteringDaily.tenant_id,
            func.max(MeteringDaily.tenant_name).label("tenant_name"),
            func.sum(MeteringDaily.request_count).label("requests"),
        )
        .where(
            MeteringDaily.bucket_date >= start_d,
            MeteringDaily.bucket_date <= end_d,
        )
        .group_by(MeteringDaily.tenant_id)
    )
    req_rows = (await session.execute(req_stmt)).all()
    req_by_tenant: dict[str, tuple[str | None, int]] = {
        r.tenant_id: (r.tenant_name or None, int(r.requests or 0)) for r in req_rows
    }

    # Cost + tokens by tenant — from cost_daily, grouped to the markup line key
    # (no license dimension at this aggregate view) so billed cost resolves
    # per (project, app, provider, model) line and is summed back per tenant.
    #
    # ⭐ `project_id` IS PART OF THE GROUPING KEY, NOT JUST OF THE SELECT — the
    # same requirement, for the same reason, as services.billing.generate_billing.
    # `services.markup` scores a project-pinned rule at `_W_PROJECT`, so two
    # rows differing ONLY in project can carry two different percentages; a key
    # without `project_id` sums them into one line and prices that mixture once,
    # at whichever project's rate the merged line happens to resolve to. This
    # report is supposed to reconcile to the invoices it summarises, so the
    # grain has to match theirs.
    cost_stmt = (
        select(
            CostDaily.tenant_id,
            func.max(CostDaily.tenant_name).label("tenant_name"),
            CostDaily.project_id,
            CostDaily.service_key,
            CostDaily.provider,
            CostDaily.served_model,
            CostDaily.byo,
            func.sum(CostDaily.total_input_tokens).label("input_tokens"),
            func.sum(CostDaily.total_output_tokens).label("output_tokens"),
            func.sum(CostDaily.total_cost_usd).label("incurred_cost"),
        )
        .where(
            CostDaily.bucket_date >= start_d,
            CostDaily.bucket_date <= end_d,
            # License-attributed cost only: exclude the system/S2S sentinel
            # license so this per-tenant incurred/billed aggregate matches the
            # real invoice (services.billing.generate_billing also drops these
            # rows). Without it, markup was applied to system cost and the admin
            # report drifted above what tenants are actually billed. The
            # canonical predicate keeps the two paths from diverging.
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
        # ⚠️ PRESENTATIONAL ONLY — and the money claim that used to be here has
        # been removed rather than left to rot. The allowance was drained line by
        # line before per-line markup, so planner row order changed the reported
        # billed total; `allocate_allowance` now splits each bucket PRO-RATA
        # across every line at once and the total is identical under every
        # permutation. A fixed order still makes a re-run byte-comparable. Same
        # key and reasoning as services.billing.generate_billing.
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

    # Union of tenants seen on either axis — preserves tenants with
    # request activity but zero cost (e.g. local-only LLM usage).
    all_tenants = sorted(set(req_by_tenant) | {r.tenant_id for r in cost_rows})
    markup_map = await get_markup_pct_map(all_tenants, session, settings)
    mode_result = await session.execute(select(TenantConfig.tenant_id, TenantConfig.billing_mode))
    mode_map = {row.tenant_id: row.billing_mode for row in mode_result.all()}
    # Resolve markup as of the LAST INSTANT OF THE WINDOW, not wall-clock: for
    # any window that has already closed, now() picks up every rule change made
    # since and re-renders history at today's prices.
    window_ts = datetime(end_d.year, end_d.month, end_d.day, 23, 59, 59, 999999, tzinfo=UTC)
    resolver = await build_markup_resolver(session, ts=window_ts)
    # BYO GATE — the same helper the invoice uses. Deriving billed cost from
    # TenantConfig.billing_mode alone reported marked-up revenue for internal /
    # demo tenants that `generate_billing` bills at $0, so this report could not
    # be reconciled against the invoices it is supposed to summarise.
    byo_map = await get_byo_mode_map(all_tenants, settings)

    # ALLOWANCE NETTING — only meaningful for a month-anchored window. An
    # included-usage allowance is granted per BILLED MONTH, so netting the whole
    # monthly grant against an arbitrary slice (a '7d' window, or a window
    # spanning two months) would over-credit and make the report LESS accurate
    # than leaving it out. Applied when the window starts on the 1st and ends in
    # the same month (covers 'today' on the 1st, 'month', and any Nd window that
    # happens to line up); skipped otherwise, which is the conservative
    # direction (billed is then reported gross of the allowance).
    allowance_by_tenant: dict[str, dict[str, Decimal]] = {}
    if start_d.day == 1 and (start_d.year, start_d.month) == (end_d.year, end_d.month):
        month_end = (
            date(start_d.year + 1, 1, 1)
            if start_d.month == 12
            else date(start_d.year, start_d.month + 1, 1)
        )
        allowance_by_tenant = await active_allowances_all_tenants(
            session,
            month_start=start_d,
            month_end=month_end,
            bill_ts=window_ts,
        )

    # ⭐ ALLOWANCE ALLOCATED PER TENANT ACROSS THE WHOLE WINDOW, BEFORE PRICING.
    # The credit is netted ahead of a PER-LINE markup rate, so "which line
    # absorbs it" decides which rate is forgone and therefore the billed total.
    # Draining the pool row by row answered that with the planner's row order;
    # `allocate_allowance` answers it with each line's share of the demand, which
    # is the same answer for every ordering. Parity with the invoice, which does
    # exactly this (services.billing.generate_billing).
    idx_by_tenant: dict[str, list[int]] = {}
    for i, r in enumerate(cost_rows):
        idx_by_tenant.setdefault(r.tenant_id, []).append(i)
    eff_incurred_by_row: list[Decimal] = [Decimal(0)] * len(cost_rows)
    for tid, idxs in idx_by_tenant.items():
        allocation = allocate_allowance(
            allowance_by_tenant.setdefault(tid, {}),
            [(cost_rows[i].service_key, Decimal(cost_rows[i].incurred_cost or 0)) for i in idxs],
        )
        for i, (eff, _applied) in zip(idxs, allocation, strict=True):
            eff_incurred_by_row[i] = eff

    # Accumulate per-tenant incurred/billed across markup lines.
    cost_by_tenant: dict[str, dict] = {}
    for row_index, r in enumerate(cost_rows):
        tid = r.tenant_id
        agg = cost_by_tenant.setdefault(
            tid,
            {
                "name": None,
                "in": 0,
                "out": 0,
                "incurred": Decimal(0),
                # RAW provider cost, and the ALLOWANCE-NETTED cost markup is
                # actually applied to. Both are kept because they are different
                # money and a ratio may only ever mix one of them with `billed`.
                "eff_incurred": Decimal(0),
                "billed": Decimal(0),
            },
        )
        agg["name"] = agg["name"] or (r.tenant_name or None)
        inp, outp = int(r.input_tokens or 0), int(r.output_tokens or 0)
        incurred = Decimal(r.incurred_cost or 0)
        agg["in"] += inp
        agg["out"] += outp
        agg["incurred"] += incurred
        # The allowance-netted incurred for THIS row, from the per-tenant
        # pro-rata allocation above; `incurred` stays raw.
        eff_incurred = eff_incurred_by_row[row_index]
        agg["eff_incurred"] += eff_incurred
        # ⭐ THE SENTINEL IS UNWRAPPED BACK TO `None` BEFORE IT REACHES THE
        # MATCHER — the same idiom as services.billing.generate_billing.
        # `billing_cost_daily.project_id` is NOT NULL and carries
        # `NULL_PROJECT_ID` when no project was asserted; that sentinel is a
        # storage device for the rollup's `ON CONFLICT`, NOT a project id.
        # `markup_rules.project_id` uses NULL as its wildcard, so the raw
        # sentinel would ask `_matches` for a literal project `...0004`: the
        # tenant-wide rule would stop matching project-less spend here while it
        # still matched it on the invoice, and this report would diverge.
        proj_uuid = r.project_id if r.project_id != NULL_PROJECT_ID else None
        billed, _pct, _rule = compute_billed_line(
            resolver,
            effective_billing_mode(
                mode_map.get(tid, "standard_markup"), byo_map.get(tid, "byo_required")
            ),
            tenant_id=tid,
            incurred_usd=eff_incurred,
            fallback_pct=markup_map.get(tid, settings.default_markup_pct),
            project_id=proj_uuid,
            service_key=r.service_key,
            provider=r.provider,
            model=r.served_model,
            byo=r.byo,
            input_tokens=inp,
            output_tokens=outp,
        )
        agg["billed"] += billed

    by_tenant: list[SystemTenantUsage] = []
    grand_requests = 0
    grand_input = 0
    grand_output = 0
    grand_incurred = Decimal(0)
    grand_billed = Decimal(0)

    for tid in all_tenants:
        req_name, requests = req_by_tenant.get(tid, (None, 0))
        agg = cost_by_tenant.get(tid)
        if agg is not None:
            tenant_name = agg["name"] or req_name
            in_tok, out_tok = agg["in"], agg["out"]
            incurred, billed = agg["incurred"], agg["billed"]
            eff_incurred = agg["eff_incurred"]
        else:
            tenant_name, in_tok, out_tok = req_name, 0, 0
            incurred = billed = eff_incurred = Decimal(0)
        # Effective blended MARKUP RATE -- billed over the base markup was
        # applied to, which is the ALLOWANCE-NETTED cost, not the raw one.
        #
        # `billed` is the sum of compute_billed_line() outputs, and that helper
        # is fed `eff_incurred` (post-consume_allowance). Dividing it by the raw
        # `incurred` therefore mixed two different bases and reported a rate
        # nobody ever charged: $100 incurred, a $50 allowance and a genuine 20%
        # markup rendered as -40% -- a NEGATIVE markup, the opposite sign of the
        # truth. The mismatch was invisible without an allowance, which is why
        # it survived: netting is only applied on a month-anchored window.
        #
        # Deliberately the rate, NOT the realised uplift on raw cost
        # (`billed / incurred - 1`). Those are two different business
        # quantities and this field can only carry one. The rate wins because
        # the fallback arm of this very expression already returns a configured
        # rate (`markup_map` / `default_markup_pct`) -- one field must not mean
        # "rate charged" on one branch and "uplift realised after discounts" on
        # the other. The realised-uplift figure is still derivable by the reader
        # from `incurred_cost_usd` and `billed_cost_usd`, both on the row.
        #
        # `effective_incurred_cost_usd` is published alongside so the row is
        # self-checking: billed == effective * (1 + markup_pct/100), and
        # incurred - effective == the allowance credited. A ratio whose
        # denominator is not on the row cannot be audited by its reader.
        markup_pct = (
            round(float((billed / eff_incurred - 1) * 100), 2)
            if eff_incurred
            else markup_map.get(tid, settings.default_markup_pct)
        )

        # The cent this tenant's ROW reports. Computed once and reused by the
        # roll-up below so the two cannot drift apart by construction.
        billed_charged = quantize_money(billed)

        by_tenant.append(
            SystemTenantUsage(
                tenant_id=tid,
                tenant_name=tenant_name,
                requests=requests,
                input_tokens=in_tok,
                output_tokens=out_tok,
                incurred_cost_usd=str(incurred),
                effective_incurred_cost_usd=str(eff_incurred),
                markup_pct=markup_pct,
                billed_cost_usd=str(billed_charged),
            )
        )
        grand_requests += requests
        grand_input += in_tok
        grand_output += out_tok
        grand_incurred += incurred
        # ⭐ A DISPLAYED TOTAL IS THE SUM OF THE DISPLAYED COMPONENTS — the same
        # rule as the invoice fold and `routers.billing.compute_billing_overview`.
        # Accumulating RAW `billed` here made `grand_total.billed_cost_usd`
        # `q(Σ billed_i)` while every row above it was `q(billed_i)`: `qΣ` against
        # `Σq`. Two tenants at 30.005 report 30.01 + 30.01 = $60.02 against a
        # grand total of $60.01, and the admin report stops adding up to itself.
        # ⛔ Do NOT "simplify" this back to `+= billed`.
        grand_billed += billed_charged

    # By provider (incurred-only).
    prov_stmt = (
        select(
            CostDaily.provider,
            func.sum(CostDaily.request_count).label("requests"),
            func.sum(CostDaily.total_input_tokens).label("input_tokens"),
            func.sum(CostDaily.total_output_tokens).label("output_tokens"),
            func.sum(CostDaily.total_cost_usd).label("incurred_cost"),
        )
        .where(
            CostDaily.bucket_date >= start_d,
            CostDaily.bucket_date <= end_d,
            # Same system/S2S-license exclusion as cost_stmt (and generate_billing)
            # so the by-provider breakdown reconciles with the grand total rather
            # than exceeding it by the amount of unattributed system spend.
            CostDaily.license_attributed_clause(),
        )
        .group_by(CostDaily.provider)
        .order_by(func.sum(CostDaily.total_cost_usd).desc())
    )
    prov_rows = (await session.execute(prov_stmt)).all()
    by_provider = [
        SystemProviderUsage(
            provider=r.provider,
            requests=int(r.requests or 0),
            input_tokens=int(r.input_tokens or 0),
            output_tokens=int(r.output_tokens or 0),
            incurred_cost_usd=str(Decimal(r.incurred_cost or 0)),
        )
        for r in prov_rows
    ]

    by_tenant.sort(key=lambda t: Decimal(t.incurred_cost_usd), reverse=True)

    return SystemUsageResponse(
        window=window,
        start=datetime(start_d.year, start_d.month, start_d.day, tzinfo=UTC),
        end=datetime(end_d.year, end_d.month, end_d.day, tzinfo=UTC),
        generated_at=datetime.now(UTC),
        grand_total=SystemUsageGrandTotal(
            requests=grand_requests,
            input_tokens=grand_input,
            output_tokens=grand_output,
            incurred_cost_usd=str(grand_incurred),
            # Idempotent: `grand_billed` is already a sum of whole cents (see the
            # fold above). Kept only to fix the display exponent at two places.
            billed_cost_usd=str(quantize_money(grand_billed)),
        ),
        by_tenant=by_tenant,
        by_provider=by_provider,
    )
