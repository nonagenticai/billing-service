"""Access charges — the ACCESS-billing axis.

The USAGE axis (:mod:`services.markup` over ``markup_rules``) decides what we
CHARGE for a tenant's token consumption. The ACCESS axis, here, decides what a
tenant pays for *access* to a product at all — a fixed recurring/one-time fee,
independent of consumption (the "paid license" concept).

An access charge is inherently tenant- and product-scoped, so — unlike markup —
there is no consumption-axis specificity ladder: the match tuple is just
``(tenant_id, service_key)`` with a NULL ``service_key`` meaning "tenant-wide".
Storage mirrors :class:`MarkupRule`'s effective-dated, one-open-row-per-tuple
shape (:func:`upsert_access_charge` closes the prior open row and opens a new
effective-dated one, for auditable history).

:func:`active_access_charges_for_month` is the billing-side entry point: given a
billed month it returns the charges that apply, honouring ``cadence`` (``monthly``
bills every effective month; ``one_time`` bills only in its ``effective_from``
month; ``per_seat`` bills every effective month like ``monthly`` but its
``amount_usd`` is a PER-SEAT RATE the caller multiplies by a seat count). Consumed
by :func:`services.billing.generate_billing` +
:func:`routers.billing.compute_billing_overview`.

⚠️ A ``per_seat`` row is a RATE everywhere it appears. Any aggregation that sums
``amount_usd`` across cadences without resolving seats reports the rate as
revenue — see :func:`active_access_charges_all_tenants`, which excludes it for
exactly that reason.
"""

from __future__ import annotations

from collections.abc import Sequence
from datetime import UTC, date, datetime, timedelta
from decimal import Decimal
from typing import Any

import structlog
from sqlalchemy import func, or_, select
from sqlalchemy.ext.asyncio import AsyncSession

from models.db import CostDaily, TenantAccessCharge
from models.domain import AccessChargeCreate

logger = structlog.stdlib.get_logger(__name__)


async def list_current_access_charges(session: AsyncSession) -> list[TenantAccessCharge]:
    """All currently-effective access charges (effective_until IS NULL)."""
    stmt = (
        select(TenantAccessCharge)
        .where(TenantAccessCharge.effective_until.is_(None))
        .order_by(TenantAccessCharge.tenant_id, TenantAccessCharge.service_key)
    )
    return list((await session.execute(stmt)).scalars().all())


async def list_access_charge_history(
    session: AsyncSession,
    *,
    tenant_id: str,
    service_key: str | None = None,
) -> list[TenantAccessCharge]:
    """Full effective-dated history for one (tenant, service_key) tuple, newest first.

    ``service_key=None`` scopes to the tenant-wide charge's lineage (NULL match),
    not "any service_key" — so history tracks one logical charge over time.
    """
    stmt = select(TenantAccessCharge).where(TenantAccessCharge.tenant_id == tenant_id)
    stmt = stmt.where(
        TenantAccessCharge.service_key.is_(None)
        if service_key is None
        else TenantAccessCharge.service_key == service_key
    )
    stmt = stmt.order_by(TenantAccessCharge.effective_from.desc())
    return list((await session.execute(stmt)).scalars().all())


async def _one_time_lineage(
    session: AsyncSession, *, tenant_id: str, service_key: str | None
) -> list[TenantAccessCharge]:
    """Every ``one_time`` row ever written for one ``(tenant, service_key)`` lineage.

    Closed rows included — a grant that has already been billed and superseded
    is exactly the thing that must stop a re-upsert from billing it again. A
    NULL ``service_key`` scopes to the tenant-wide lineage (NULL match), never
    to "any product", mirroring :func:`list_access_charge_history`.
    """
    stmt = select(TenantAccessCharge).where(
        TenantAccessCharge.tenant_id == tenant_id,
        TenantAccessCharge.cadence == "one_time",
    )
    stmt = stmt.where(
        TenantAccessCharge.service_key.is_(None)
        if service_key is None
        else TenantAccessCharge.service_key == service_key
    )
    return list((await session.execute(stmt)).scalars().all())


async def upsert_access_charge(
    session: AsyncSession,
    payload: AccessChargeCreate,
    *,
    ts: datetime | None = None,
) -> tuple[TenantAccessCharge, TenantAccessCharge | None]:
    """Close the open row for the same (tenant, service_key) and insert a new one.

    Mirrors :func:`services.markup.upsert_markup_rule` — editing a tenant's
    product charge never disturbs its tenant-wide charge or any other product's.
    The closed prior row is returned for the audit before/after.

    ONE_TIME IDEMPOTENCY. A ``one_time`` fee is billed in its ``effective_from``
    month and only there (:func:`_applies_this_month`). ``AccessChargeCreate``
    defaults ``effective_from`` to *now*, so a plain re-upsert — bumping the
    amount, fixing a typo in ``notes``, re-running a provisioning script —
    stamped a fresh grant instant and the whole fee fell due AGAIN in the month
    of the edit. Billing here is append-only: negative amounts are rejected at
    ingest and nothing but a migration ever sets
    ``CostEvent.excluded_from_billing``, so that second charge could not be
    refunded.

    The natural key of a one_time grant is its lineage — ``(tenant_id,
    coalesce(service_key, ''))``, the same tuple ``ix_accesscharge_open`` allows
    exactly one OPEN row for. So the grant instant is pinned to that lineage's
    EARLIEST one_time ``effective_from`` and every earlier one_time row is
    retired to a zero-width interval (``effective_until = effective_from``),
    which is what keeps the pinned row from OVERLAPPING its own ancestors and
    double-billing the grant month. Net effect: a one_time charge is billed once
    per lineage, ever; an edit amends that one charge rather than adding another.

    Consequence, stated because it is the under-charge direction and equally
    unrecoverable: a genuinely SECOND one_time purchase on the same
    ``(tenant, service_key)`` cannot be expressed — the data model has no room
    for two concurrent one_time charges on one key. It needs its own
    ``service_key``.
    """
    closed_at = ts or datetime.now(UTC)

    open_stmt = select(TenantAccessCharge).where(
        TenantAccessCharge.effective_until.is_(None),
        TenantAccessCharge.tenant_id == payload.tenant_id,
    )
    open_stmt = open_stmt.where(
        TenantAccessCharge.service_key.is_(None)
        if payload.service_key is None
        else TenantAccessCharge.service_key == payload.service_key
    )
    prior = (await session.execute(open_stmt)).scalar_one_or_none()
    if prior is not None:
        prior.effective_until = closed_at

    effective_from = payload.effective_from or closed_at
    if payload.cadence == "one_time":
        granted = await _one_time_lineage(
            session, tenant_id=payload.tenant_id, service_key=payload.service_key
        )
        if granted:
            # Already granted once. Keep the ORIGINAL grant instant so
            # _applies_this_month still points at the month that was billed…
            effective_from = min(r.effective_from for r in granted)
            # …and retire the ancestors to zero-width intervals so the pinned
            # row does not overlap them in that month (an overlap would select
            # two one_time rows for one grant and bill the fee twice). Ordered
            # after the `prior` close above deliberately: when `prior` IS a
            # one_time row it belongs to this set and this is the correct
            # close instant for it.
            for row in granted:
                row.effective_until = row.effective_from

    new_row = TenantAccessCharge(
        tenant_id=payload.tenant_id,
        service_key=payload.service_key,
        amount_usd=payload.amount_usd,
        cadence=payload.cadence,
        included_cost_usd=payload.included_cost_usd,
        effective_from=effective_from,
        effective_until=payload.effective_until,
        notes=payload.notes,
    )
    session.add(new_row)
    await session.flush()
    return new_row, prior


async def active_access_charges_for_month(
    session: AsyncSession,
    *,
    tenant_id: str,
    month_start: date,
    month_end: date,
    bill_ts: datetime,
) -> list[TenantAccessCharge]:
    """The access charges billable for one tenant-month.

    A row applies when it is effective at ``bill_ts`` (the last instant of the
    billed month) AND its ``cadence`` says this month is billable:

    - ``monthly``  — billed every month it is effective.
    - ``one_time`` — billed only in the month containing ``effective_from``
      (``month_start <= effective_from < month_end``), so re-querying a later
      month does not re-bill it.
    """
    stmt = select(TenantAccessCharge).where(
        TenantAccessCharge.tenant_id == tenant_id,
        TenantAccessCharge.effective_from <= bill_ts,
        or_(
            TenantAccessCharge.effective_until.is_(None),
            TenantAccessCharge.effective_until > bill_ts,
        ),
    )
    rows = list((await session.execute(stmt)).scalars().all())

    def _applies(row: TenantAccessCharge) -> bool:
        if row.cadence == "one_time":
            ef = row.effective_from
            # effective_from is timestamptz; .date() alone yields the DB session-TZ
            # (prod pgbouncer = Europe/Amsterdam) calendar date, but month_start/
            # month_end are UTC-derived — so a charge within ~2h of a month boundary
            # would bucket into the wrong month. Normalize to UTC first.
            ef_date = ef.astimezone(UTC).date() if isinstance(ef, datetime) else ef
            return month_start <= ef_date < month_end
        # 'monthly' and 'per_seat' bill every effective month. ⚠️ So does any
        # UNKNOWN cadence — this `return True` is why `per_seat` could not be
        # added without its multiplier in the same commit: before
        # services.billing resolved seats, a per_seat row reaching this branch
        # was billed as a FLAT $25 regardless of seat count.
        return True

    return [r for r in rows if _applies(r)]


def _applies_this_month(row: TenantAccessCharge, *, month_start: date, month_end: date) -> bool:
    """Whether a charge's cadence makes it billable in the given month."""
    if row.cadence == "one_time":
        ef = row.effective_from
        # UTC-normalize the timestamptz before .date() — see active_access_charges_
        # for_month._applies: a bare .date() resolves in the session TZ and
        # mis-buckets a boundary-instant charge into the wrong month.
        ef_date = ef.astimezone(UTC).date() if isinstance(ef, datetime) else ef
        return month_start <= ef_date < month_end
    return True  # 'monthly' (and any forward-compat cadence) bills every effective month


async def active_access_charges_all_tenants(
    session: AsyncSession,
    *,
    month_start: date,
    month_end: date,
    bill_ts: datetime,
) -> dict[str, list[Decimal]]:
    """Per-tenant flat access charge AMOUNTS for the month, across all tenants.

    Used by the platform billing overview so a tenant with a paid access charge
    but zero usage is still counted. Mirrors
    :func:`active_access_charges_for_month`'s effective + cadence rules.

    ⚠️ A LIST PER TENANT, NOT A SUMMED TOTAL, and the difference is money. The
    invoice charges each flat term as the cent IT is charged as —
    ``Σ q(amount_i)``, see ``access_total`` in
    :func:`services.billing.generate_billing` — so a caller handed one pre-summed
    figure can only compute ``q(Σ amount_i)``, and the two part company by up to
    a cent per term as soon as any amount carries sub-cent scale. The rounding
    policy belongs to the reader that decides what is DISPLAYED and CHARGED, not
    to this query. Callers that only want the tenant SET (e.g.
    :func:`services.invoices.find_close_candidates`) read the keys and are
    unaffected. Symmetric with :func:`seat_rates_all_tenants`.
    """
    stmt = select(TenantAccessCharge).where(
        TenantAccessCharge.effective_from <= bill_ts,
        or_(
            TenantAccessCharge.effective_until.is_(None),
            TenantAccessCharge.effective_until > bill_ts,
        ),
    )
    totals: dict[str, list[Decimal]] = {}
    for row in (await session.execute(stmt)).scalars().all():
        # ⛔ A per_seat row's amount_usd is a RATE, not a charge — summing it here
        # would report $25 of revenue for a tenant with 40 seats, and the overview
        # would disagree with that tenant's own invoice. Seat revenue is summed
        # separately by `seat_rates_all_tenants` + the caller's seat counts,
        # because a count needs a licences lookup that this flat-fee query has no
        # business making. Caught by
        # `test_per_seat_is_excluded_from_the_flat_access_total`.
        if row.cadence == "per_seat":
            continue
        if _applies_this_month(row, month_start=month_start, month_end=month_end):
            totals.setdefault(row.tenant_id, []).append(Decimal(row.amount_usd))
    return totals


async def seat_rates_all_tenants(
    session: AsyncSession,
    *,
    month_start: date,
    month_end: date,
    bill_ts: datetime,
) -> dict[str, list[Decimal]]:
    """Per-tenant per-seat RATES for the month — the companion to the flat total.

    Returns the RATES, not the charge: the caller multiplies each by a seat count
    it obtains from :mod:`services.seats`. Split this way on purpose — a tenant's
    seat revenue needs a licences round-trip, and burying one inside the flat-fee
    aggregation would make the platform overview fail whenever licences is down.

    ⚠️ A LIST PER TENANT, NOT A SUMMED RATE, and the difference is money. The
    invoice charges each per-seat term as its own cent —
    ``Σ q(rate_i × seats)`` (see ``seat_fee_total`` in
    :func:`services.billing.generate_billing`) — so a caller handed one summed
    rate can only compute ``q(Σ rate_i × seats)``, and the two part company by a
    cent per term as soon as any ``rate × seats`` carries sub-cent scale. Summing
    here made the overview unable to reproduce the statement it is supposed to
    reconcile to. Callers that only want the tenant SET (e.g.
    :func:`services.invoices.find_close_candidates`) read the keys and are
    unaffected.
    """
    stmt = select(TenantAccessCharge).where(
        TenantAccessCharge.cadence == "per_seat",
        TenantAccessCharge.effective_from <= bill_ts,
        or_(
            TenantAccessCharge.effective_until.is_(None),
            TenantAccessCharge.effective_until > bill_ts,
        ),
    )
    rates: dict[str, list[Decimal]] = {}
    for row in (await session.execute(stmt)).scalars().all():
        if _applies_this_month(row, month_start=month_start, month_end=month_end):
            rates.setdefault(row.tenant_id, []).append(Decimal(row.amount_usd))
    return rates


async def active_allowances_all_tenants(
    session: AsyncSession,
    *,
    month_start: date,
    month_end: date,
    bill_ts: datetime,
) -> dict[str, dict[str, Decimal]]:
    """Per-tenant, per-``service_key`` included-usage allowance for the month.

    ``{tenant_id: {service_key_or_"": remaining_usd}}`` — a NULL ``service_key``
    becomes the ``""`` (tenant-wide) bucket. ``monthly``-cadence charges grant a
    recurring allowance that resets each billed month; ``one_time`` charges grant
    a durable prepaid credit whose *remaining* balance (grant minus prior-month
    consumption) is folded in via :func:`one_time_remaining_before`. Mirrors
    :func:`active_access_charges_all_tenants`' effective + cadence rules. Used by
    the platform overview to keep customer totals in parity with the per-tenant
    invoice (:func:`services.billing.generate_billing`).
    """
    stmt = select(TenantAccessCharge).where(
        TenantAccessCharge.effective_from <= bill_ts,
        or_(
            TenantAccessCharge.effective_until.is_(None),
            TenantAccessCharge.effective_until > bill_ts,
        ),
    )
    rows = list((await session.execute(stmt)).scalars().all())
    buckets: dict[str, dict[str, Decimal]] = {}
    for row in rows:
        if row.cadence != "monthly" or row.included_cost_usd is None:
            continue
        if not _applies_this_month(row, month_start=month_start, month_end=month_end):
            continue
        key = row.service_key or ""
        tbucket = buckets.setdefault(row.tenant_id, {})
        tbucket[key] = tbucket.get(key, Decimal("0")) + Decimal(row.included_cost_usd)

    # Fold in durable one_time credit remaining, per tenant that holds any open
    # one_time charge (bounded to those tenants — a no-op for everyone else).
    onetime_tenants = {
        row.tenant_id
        for row in rows
        if row.cadence == "one_time" and row.included_cost_usd is not None
    }
    for tenant_id in onetime_tenants:
        remaining = await one_time_remaining_before(
            session, tenant_id=tenant_id, month_start=month_start, bill_ts=bill_ts
        )
        if not remaining:
            continue
        tbucket = buckets.setdefault(tenant_id, {})
        for key, amt in remaining.items():
            tbucket[key] = tbucket.get(key, Decimal("0")) + amt
    return buckets


def consume_allowance(
    buckets: dict[str, Decimal], service_key: str | None, incurred: Decimal
) -> tuple[Decimal, Decimal]:
    """Deplete an included-usage allowance by ONE AGGREGATE draw.

    ``buckets`` is one tenant's ``{service_key_or_"": remaining_usd}`` map.
    Consumes the product-specific bucket first, then the tenant-wide (``""``)
    bucket. Returns ``(effective_incurred, applied)`` with
    ``effective_incurred = incurred - applied`` (never negative). Mutates
    ``buckets``. A no-op when the tenant has no allowance configured.

    ⛔ THIS IS NOT THE INVOICE PATH ANY MORE — use :func:`allocate_allowance`.
    Draining a shared pool ONE BILLED LINE AT A TIME makes the invoice total a
    function of the order the lines happen to arrive in: the allowance is netted
    BEFORE markup, so whichever line eats it is the line whose markup rate is
    NOT charged. Measured on two projects at different rates, the same inputs
    billed **$8.00 or $5.20** depending only on how the two group-by rows
    sorted. The money path now allocates PRO-RATA across every line at once, so
    ordering is irrelevant.

    ⭐ THE TWO SURVIVING CALLERS ARE CORRECT AND MUST NOT BE MIGRATED.
    :func:`_consume_one_time_month` and :func:`tenant_allowance_usage` feed this
    function a PER-``service_key`` AGGREGATE (one call per bucket key), not a
    billed line. A bucket's total draw is ``min(balance, Σ residual)`` and is
    therefore already independent of how that sum was reached — there is no
    per-line markup rate downstream of either, so there is nothing for an order
    to change. ``tests/test_access_charge.py`` holds 17 tests on that property.
    """
    remaining = incurred
    applied = Decimal("0")
    for bkey in [service_key, ""] if service_key else [""]:
        avail = buckets.get(bkey, Decimal("0"))
        if avail <= 0 or remaining <= 0:
            continue
        take = min(avail, remaining)
        buckets[bkey] = avail - take
        remaining -= take
        applied += take
    return remaining, applied


def allocate_allowance(
    buckets: dict[str, Decimal],
    lines: Sequence[tuple[str | None, Decimal]],
) -> list[tuple[Decimal, Decimal]]:
    """Net an included-usage allowance PRO-RATA across a tenant's cost lines.

    ``buckets`` is one tenant's ``{service_key_or_"": remaining_usd}`` map (it is
    MUTATED, exactly as :func:`consume_allowance` mutates it). ``lines`` is the
    whole month's billable cost lines as ``(service_key, incurred_usd)``. Returns
    one ``(effective_incurred, applied)`` pair per input line, in input order,
    with ``effective_incurred + applied`` reconstructing ``incurred`` TO
    DECIMAL'S PRECISION — which is what the ⚠️ note below says, and is not the
    same as exactly. The per-line split is a Decimal division, so the pair does
    not always re-sum to the input bit-for-bit: measured over ~80,000 lines, 181
    differ, worst case **1e-27**. That is nineteen orders of magnitude below the
    ``Numeric(20,8)`` these figures are STORED at and twenty-five below the cent
    they are BILLED at, so it moves no money and cannot: the residual is erased
    by the first storage write, let alone by ``quantize_money``. The two ends
    that ARE exact are named below; do not read this line as a third one.

    ⭐ WHY PRO-RATA, AND WHY IT IS A MONEY DECISION RATHER THAN A TIDY-UP.
    The allowance is netted BEFORE markup, and markup is resolved PER LINE — two
    projects, two apps or two models can carry different percentages. So the
    question "which line does the allowance absorb?" is the question "which
    line's markup do we forgo?", and it moves the invoice total. Sequentially
    draining the pool answered it with whatever order the rows arrived in;
    measured on two projects at 20% and 30% with a $4 allowance against $12 of
    spend, the same inputs billed **$8.00 or $5.20 (+53.8%)** depending purely on
    how two UUIDs sorted. Pro-rata answers it with the lines' own shares of the
    demand, which is the same answer for every ordering — so the invoice stops
    being a function of the planner.

    THE LADDER IS UNCHANGED: the product-specific bucket is drawn first and the
    tenant-wide (``""``) bucket second, exactly as :func:`consume_allowance`
    does. Only the WITHIN-a-bucket split changed, from first-come to by-share.

    ⚠️ EXACT AT BOTH ENDS, approximate only in between. A bucket is debited by
    the exact ``min(balance, demand)`` it funded, and a fully-absorbed line is
    zeroed EXACTLY (``take == demand`` is special-cased, so "the allowance covers
    everything" still bills $0.00 rather than $0.00000000000000000001). Only the
    per-line split carries Decimal division's relative error, ~1e-27 — twenty-odd
    orders of magnitude below the cent the invoice is quantised to.

    ⛔ A LINE'S SHARE DEPENDS ON NOTHING BUT THE LINE, the bucket balance and the
    bucket's total demand. That is the whole guarantee; keep it. Any "give the
    rounding remainder to the last line" refinement would reinstate the order
    dependence this function exists to remove.
    """
    residual = [Decimal(incurred) for _, incurred in lines]
    applied = [Decimal("0") for _ in lines]

    # Draw order: every product-specific bucket, then the tenant-wide one. The
    # product buckets are disjoint (one per service_key) so their relative order
    # cannot matter; `sorted` is for readable debugging, not for correctness.
    by_key: dict[str, list[int]] = {}
    for i, (service_key, _incurred) in enumerate(lines):
        if service_key:
            by_key.setdefault(service_key, []).append(i)
    draws: list[tuple[str, list[int]]] = [(k, by_key[k]) for k in sorted(by_key)]
    # A line with no service_key draws from the tenant-wide bucket only, which is
    # `consume_allowance`'s `[service_key, ""] if service_key else [""]` ladder.
    draws.append(("", list(range(len(lines)))))

    for bkey, idxs in draws:
        avail = buckets.get(bkey, Decimal("0"))
        if avail <= 0:
            continue
        demand = sum((residual[i] for i in idxs if residual[i] > 0), Decimal("0"))
        if demand <= 0:
            continue
        take = min(avail, demand)
        buckets[bkey] = avail - take
        if take >= demand:
            # Fully absorbed: zero the lines EXACTLY rather than via a ratio.
            for i in idxs:
                if residual[i] > 0:
                    applied[i] += residual[i]
                    residual[i] = Decimal("0")
            continue
        # `survive` is the fraction of each line's residual that the allowance
        # did NOT cover. Multiplying is equivalent to allocating `take` by share
        # and cheaper to read: every line is scaled by the same factor, so no
        # line can be favoured and no remainder has to be handed to anyone.
        survive = (demand - take) / demand
        for i in idxs:
            before = residual[i]
            if before <= 0:
                continue
            after = before * survive
            residual[i] = after
            applied[i] += before - after

    return list(zip(residual, applied, strict=True))


def _month_bounds(month_start: date) -> tuple[date, date, datetime]:
    """(start, end, bill_ts) for the calendar month beginning at ``month_start``.

    ``bill_ts`` is the last instant of the month — the same resolution point
    :func:`services.billing.generate_billing` uses for effective-dated matching.
    """
    month_end = (
        date(month_start.year + 1, 1, 1)
        if month_start.month == 12
        else date(month_start.year, month_start.month + 1, 1)
    )
    bill_ts = datetime(month_end.year, month_end.month, month_end.day, tzinfo=UTC) - timedelta(
        microseconds=1
    )
    return month_start, month_end, bill_ts


async def _monthly_allowance_buckets(
    session: AsyncSession, *, tenant_id: str, bill_ts: datetime
) -> dict[str, Decimal]:
    """One tenant's recurring (monthly) allowance buckets effective at ``bill_ts``.

    ``{service_key_or_"": credit_usd}`` — the same buckets
    :func:`services.billing.generate_billing` rebuilds each billed month. Used
    only inside the one_time fold, so a prior month's one_time draw is computed
    against the residual *after* that month's recurring allowance.
    """
    stmt = select(TenantAccessCharge).where(
        TenantAccessCharge.tenant_id == tenant_id,
        TenantAccessCharge.cadence == "monthly",
        TenantAccessCharge.included_cost_usd.is_not(None),
        TenantAccessCharge.effective_from <= bill_ts,
        or_(
            TenantAccessCharge.effective_until.is_(None),
            TenantAccessCharge.effective_until > bill_ts,
        ),
    )
    buckets: dict[str, Decimal] = {}
    for row in (await session.execute(stmt)).scalars().all():
        if row.included_cost_usd is None:
            continue
        key = row.service_key or ""
        buckets[key] = buckets.get(key, Decimal("0")) + Decimal(row.included_cost_usd)
    return buckets


async def _license_cost_by_service_key(
    session: AsyncSession, *, tenant_id: str, month_start: date, month_end: date
) -> list[tuple[str | None, Decimal]]:
    """License-attributed incurred cost for a tenant-month, summed per service_key.

    Excludes the system/sentinel license, mirroring
    :func:`services.billing.generate_billing` which nets allowances only against
    license-attributed cost lines (system S2S cost is never netted). Aggregating
    per service_key is equivalent to the per-line consume for the purpose of
    bucket depletion (a bucket's total draw is ``min(balance, summed_residual)``,
    independent of line order).
    """
    stmt = (
        select(
            CostDaily.service_key,
            func.sum(CostDaily.total_cost_usd).label("cost"),
        )
        .where(
            CostDaily.tenant_id == tenant_id,
            CostDaily.bucket_date >= month_start,
            CostDaily.bucket_date < month_end,
            CostDaily.license_attributed_clause(),
        )
        .group_by(CostDaily.service_key)
    )
    return [(r.service_key, Decimal(r.cost or 0)) for r in (await session.execute(stmt)).all()]


async def _consume_one_time_month(
    session: AsyncSession, *, tenant_id: str, month_start: date, balances: dict[str, Decimal]
) -> None:
    """Draw one month's usage against the running one_time ``balances`` (in place).

    Rebuilds that month's recurring (monthly) buckets so the specificity ladder
    in :func:`consume_allowance` (product bucket first, tenant-wide ``""`` last)
    absorbs usage in the same order the live invoice would, then carries the
    leftover one_time balances forward. One_time bucket keys are disjoint from
    monthly keys (one open charge per ``(tenant, service_key)``), so a one_time
    key's post-consume value is purely its own remaining credit.
    """
    m_start, m_end, m_bill_ts = _month_bounds(month_start)
    buckets = await _monthly_allowance_buckets(session, tenant_id=tenant_id, bill_ts=m_bill_ts)
    for key, bal in balances.items():
        buckets[key] = bal  # disjoint from monthly keys — see docstring
    for service_key, cost in await _license_cost_by_service_key(
        session, tenant_id=tenant_id, month_start=m_start, month_end=m_end
    ):
        if cost > 0:
            consume_allowance(buckets, service_key, cost)
    for key in balances:
        balances[key] = buckets.get(key, Decimal("0"))


async def one_time_remaining_before(
    session: AsyncSession, *, tenant_id: str, month_start: date, bill_ts: datetime
) -> dict[str, Decimal]:
    """Remaining one_time (durable prepaid) credit per bucket at the start of a month.

    A ``one_time`` charge's ``included_cost_usd`` is a durable credit granted once
    at ``effective_from`` and drawn down across months until exhausted (unlike a
    ``monthly`` allowance, which resets each billed month). This computes each
    open one_time bucket's balance entering the billed month — ``grant`` minus the
    credit consumed in every month from its grant month up to (not including) the
    billed month — purely from ``cost_daily`` history. No ledger, no persistence;
    :func:`services.billing.generate_billing` seeds ``allowance_remaining`` with
    the result and the billed month's own usage draws it down normally.

    Returns ``{service_key_or_"": remaining_usd}`` for buckets with credit left.
    Empty when the tenant has no open one_time credit (the common case — a single
    cheap query, then done).
    """
    stmt = select(TenantAccessCharge).where(
        TenantAccessCharge.tenant_id == tenant_id,
        TenantAccessCharge.cadence == "one_time",
        TenantAccessCharge.included_cost_usd.is_not(None),
        TenantAccessCharge.effective_from <= bill_ts,
        or_(
            TenantAccessCharge.effective_until.is_(None),
            TenantAccessCharge.effective_until > bill_ts,
        ),
    )
    charges = list((await session.execute(stmt)).scalars().all())
    if not charges:
        return {}

    # Group each credit's grant into the calendar month it starts in.
    grants_by_month: dict[date, list[tuple[str, Decimal]]] = {}
    for c in charges:
        if c.included_cost_usd is None:
            continue
        ef = c.effective_from
        # UTC-normalize before deriving the grant month — a session-TZ .date()
        # would assign a boundary-instant grant to the wrong calendar month, so
        # the durable credit's draw-down window would start a month early/late.
        ef_date = ef.astimezone(UTC).date() if isinstance(ef, datetime) else ef
        grant_month = date(ef_date.year, ef_date.month, 1)
        grants_by_month.setdefault(grant_month, []).append(
            (c.service_key or "", Decimal(c.included_cost_usd))
        )
    if not grants_by_month:
        return {}

    latest_grant = max(grants_by_month)
    balances: dict[str, Decimal] = {}
    j = min(grants_by_month)
    while j < month_start:
        for key, amt in grants_by_month.get(j, []):
            balances[key] = balances.get(key, Decimal("0")) + amt
        if any(v > 0 for v in balances.values()):
            await _consume_one_time_month(
                session, tenant_id=tenant_id, month_start=j, balances=balances
            )
        # Nothing left to draw and no later grant to seed → stop early.
        if j >= latest_grant and not any(v > 0 for v in balances.values()):
            break
        j = _month_bounds(j)[1]

    # Credits first granted in the billed month itself carry their full grant
    # (nothing consumed before this month); the billed month's loop draws them.
    for key, amt in grants_by_month.get(month_start, []):
        balances[key] = balances.get(key, Decimal("0")) + amt

    return {k: v for k, v in balances.items() if v > 0}


async def tenant_allowance_usage(
    session: AsyncSession, *, tenant_id: str, month_start: date
) -> dict[str, dict[str, Decimal]]:
    """Live used / allowance / remaining per ``service_key`` for a tenant-month.

    Pure compute-on-read fold over ``cost_daily`` — no ledger, no persistence —
    mirroring :func:`services.billing.generate_billing`'s allowance netting so the
    numbers reconcile with the invoice. Returns
    ``{service_key_or_"": {"used_usd", "allowance_usd", "remaining_usd"}}`` where
    ``""`` is the tenant-wide (NULL ``service_key``) bucket.

    - ``used_usd``      = license-attributed cost incurred this month, per service_key.
    - ``allowance_usd`` = this month's recurring (``monthly``) included-usage credit
      plus any durable ``one_time`` credit remaining entering the month.
    - ``remaining_usd`` = the allowance after the invoice's specificity ladder
      (:func:`consume_allowance`: product bucket first, tenant-wide ``""`` last)
      absorbs this month's usage.

    Powers the fr "Your plan" tile. A tenant with neither usage nor allowance this
    month yields an empty dict.
    """
    _m_start, month_end, bill_ts = _month_bounds(month_start)

    # Allowance buckets: recurring monthly credit + durable one_time remaining,
    # exactly as generate_billing seeds them.
    buckets = await _monthly_allowance_buckets(session, tenant_id=tenant_id, bill_ts=bill_ts)
    for key, amt in (
        await one_time_remaining_before(
            session, tenant_id=tenant_id, month_start=month_start, bill_ts=bill_ts
        )
    ).items():
        buckets[key] = buckets.get(key, Decimal("0")) + amt

    allowance_snapshot = dict(buckets)  # pre-consume balance per bucket

    # License-attributed cost this month, summed per service_key.
    used_by_key: dict[str, Decimal] = {}
    for service_key, cost in await _license_cost_by_service_key(
        session, tenant_id=tenant_id, month_start=month_start, month_end=month_end
    ):
        used_by_key[service_key or ""] = used_by_key.get(service_key or "", Decimal("0")) + cost

    # Draw usage down against allowance via the invoice's ladder (mutates buckets).
    for key, cost in used_by_key.items():
        if cost > 0:
            consume_allowance(buckets, key, cost)

    result: dict[str, dict[str, Decimal]] = {}
    for key in set(allowance_snapshot) | set(used_by_key):
        result[key] = {
            "used_usd": used_by_key.get(key, Decimal("0")),
            "allowance_usd": allowance_snapshot.get(key, Decimal("0")),
            "remaining_usd": buckets.get(key, Decimal("0")),
        }
    return result


def access_charge_to_response_dict(row: TenantAccessCharge) -> dict[str, Any]:
    """Plain dict view of a TenantAccessCharge — ready for AccessChargeResponse."""
    return {
        "id": row.id,
        "tenant_id": row.tenant_id,
        "service_key": row.service_key,
        "amount_usd": row.amount_usd,
        "cadence": row.cadence,
        "included_cost_usd": row.included_cost_usd,
        "effective_from": row.effective_from,
        "effective_until": row.effective_until,
        "notes": row.notes,
    }
