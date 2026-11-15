"""A displayed figure must equal the figures it is displayed as the sum of.

``services.billing.generate_billing`` renders money in TWO forms — quantised
(the cents a thing is charged as) and raw (the sub-cent figure behind it) — and
both are legitimate. What is not legitimate is MIXING them inside one array or
one arithmetic identity, because the reader has no way to tell which form a
given field is in:

  1. ``totals.access_charges[]`` had three producers writing into ONE list. The
     per-seat line and the explicit-zero line rendered ``quantize_money(...)``;
     the flat-charge line rendered the raw amount. The aggregate column that
     totals the list is ``q(access_total)``. So a $10.005 platform fee itemised
     as ``10.00500000`` under a column reading ``10.01``.

  2. ``licenses[].costs`` renders ``app_request_billed_usd`` and
     ``llm_billed_usd`` raw, and rendered ``billed_cost_usd`` — which IS their
     sum — quantised. A total that its own two displayed addends do not reach.

⚠️ WHY NOTHING CAUGHT EITHER. Every existing fixture that reads these fields
uses WHOLE-CENT money (``tests/test_access_charge.py`` bills $50, $25, $77;
``tests/test_project_markup_pricing.py`` asserts ``Decimal("15.00")``), and on a
whole cent ``q(x) == x``, so the two forms are indistinguishable. The defect is
only observable on a fixture carrying sub-cent scale. That is the property every
test below varies, and each docstring names which one it relies on.

⚠️ CORRECTED 2026-09-15 — THIS PARAGRAPH USED TO SAY "THESE TESTS ARE ABOUT
DISPLAY ONLY" AND THAT ``access_total`` "STAYS RAW". Quantising the LINE while
the accumulator behind it stayed raw is not a display change at all: it broke
the itemisation identity outright, because ``Σ q(line)`` and the column
``q(Σ line)`` are different numbers — two $10.005 fees itemise as 10.01 + 10.01
= 20.02 under a column reading 20.01. Quantising the leaf is only half of
"quantise the leaves, THEN sum"; the ACCESS and SEAT accumulators now sum the
quantised leaf too (``services.billing.generate_billing``), so
``q(total) == Σ q(line)`` holds by construction for any number of lines. That is
a pricing rule, not a display rule — see the comment at ``access_total`` for the
production measurement that makes it inert today.

⛔ ``total_customer`` DOES stay raw, and that half was always right: it is the
billed stream ``B`` on which ``services.invoices``' close-path identity
``residual == q(B) - B`` rests. The regression control that would catch someone
"tidying" it lives in ``tests/test_invoices.py`` — see
``test_the_component_identity_survives_three_licences_at_a_half_cent_boundary``,
which needs the close path and all of that module's fixtures.
"""

from __future__ import annotations

import uuid
from datetime import UTC, datetime
from decimal import Decimal
from typing import cast

import pytest
from sqlalchemy.ext.asyncio import AsyncSession

from models.db import AppCostRate, TenantAccessCharge, TenantConfig
from models.domain import AccessChargeCreate, BillingResponse, LicenseCostBilling
from services import access_charge as ac_svc
from services.billing import generate_billing
from services.markup import quantize_money
from tests.conftest import T

# ⚠️ REUSED, NOT REBUILT. These are the seeders `tests/test_access_charge.py`
# already drives `generate_billing` with, so a change to how a billable month is
# constructed cannot leave this file testing a shape nothing else produces. The
# `PRODUCT` literal is imported for the same reason it exists there: a bare
# `service_key="..."` kwarg trips detect-secrets' Secret Keyword plugin.
from tests.test_access_charge import (
    OTHER_PRODUCT,
    PRODUCT,
    _seed_cost_daily,
    _seed_metering_daily,
    _this_month,
)

# ⚠️ A PLAIN FUNCTION, NOT A FIXTURE, which is why it imports cleanly. It stubs
# the licences seat-count endpoint AND the service-account token provider —
# `services.seats` fails CLOSED before the HTTP call without one, so a GET stub
# alone leaves the seat line resolving to "unavailable" and the form comparison
# below would pass on a $0.00 line that never exercised the per-seat producer.
from tests.test_invoices import _seats_answer

#: The flat access fee, at a TRUE HALF CENT. `quantize_money` is ROUND_HALF_UP,
#: so this rounds to 10.01 and `q(x) != x` — the single property that makes the
#: raw and quantised renderings of this one number distinguishable. At $10.00
#: (what every existing access-charge fixture bills) both forms print the same
#: digits and neither assertion below can fail.
#: `tenant_access_charge.amount_usd` is `Numeric(20, 8)`, so this stores exactly.
_HALF_CENT_ACCESS_USD = Decimal("10.005")

#: A per-seat RATE, kept whole so the seat axis contributes no rounding of its
#: own: this file's seat assertions are about which FORM the line is rendered
#: in, not about seat arithmetic (that is `tests/test_commercial_terms.py`).
_SEAT_RATE_USD = Decimal("25")
_SEATS = 3

#: Sub-cent LLM spend. SIX decimal places exactly: `billing_cost_daily.
#: total_cost_usd` is `Numeric(20, 6)` and Postgres ROUNDS anything longer on
#: the way in, which would silently hand the test a different number than the
#: one written here.
_SUBCENT_LLM_COST_USD = Decimal("0.004999")

#: Sub-cent app-request rate, against a single metered request, so the SECOND
#: billed stream is also sub-cent. `billing_app_cost_rates.cost_per_request_usd`
#: is `Numeric(12, 8)`.
_SUBCENT_APP_RATE_USD = Decimal("0.00300000")
_SUBCENT_APP_REQUESTS = 1


async def _seed_half_cent_access_month(db_session: AsyncSession, *, tenant: str) -> None:
    """A billable month whose ONLY flat access charge lands on a half cent."""
    db_session.add(TenantConfig(tenant_id=T(tenant), tenant_name=tenant, markup_pct=Decimal("30")))
    lic = uuid.uuid4()
    await _seed_cost_daily(
        db_session, tenant=tenant, license_id=lic, service_key=PRODUCT, cost="10"
    )
    await _seed_metering_daily(db_session, tenant=tenant, license_id=lic, service_key=PRODUCT)
    await ac_svc.upsert_access_charge(
        db_session,
        AccessChargeCreate(
            tenant_id=T(tenant), service_key=PRODUCT, amount_usd=_HALF_CENT_ACCESS_USD
        ),
    )
    await db_session.commit()


async def _seed_subcent_billed_month(db_session: AsyncSession, *, tenant: str) -> uuid.UUID:
    """A billable month where BOTH billed streams are sub-cent. Returns the licence."""
    db_session.add(TenantConfig(tenant_id=T(tenant), tenant_name=tenant, markup_pct=Decimal("30")))
    lic = uuid.uuid4()
    await _seed_cost_daily(
        db_session,
        tenant=tenant,
        license_id=lic,
        service_key=PRODUCT,
        cost=str(_SUBCENT_LLM_COST_USD),
    )
    # The APP-REQUEST stream. Without a priced rate it is 0.00 and the identity
    # below degenerates to `billed == llm_billed`, which is true by construction
    # and proves nothing about the sum.
    for _ in range(_SUBCENT_APP_REQUESTS):
        await _seed_metering_daily(db_session, tenant=tenant, license_id=lic, service_key=PRODUCT)
    db_session.add(AppCostRate(app_key=PRODUCT, cost_per_request_usd=_SUBCENT_APP_RATE_USD))
    await db_session.commit()
    return lic


def _flat_access_line(totals: dict) -> dict:
    """The one non-seat access line, insisted on rather than assumed."""
    flat = [c for c in totals["access_charges"] if c["cadence"] != "per_seat"]
    assert len(flat) == 1, (
        f"expected exactly one flat access line so it is comparable against the "
        f"aggregate that totals it, got {flat!r}"
    )
    return flat[0]


# ---------------------------------------------------------------------------
# 1. access_charges[] — one list, one form, agreeing with its own aggregate
# ---------------------------------------------------------------------------


@pytest.mark.unit
async def test_a_half_cent_access_line_is_itemised_as_the_cents_it_is_charged_as(
    db_session: AsyncSession, test_settings
) -> None:
    """⭐ THE ITEMISATION MUST EQUAL THE AGGREGATE IT IS THE ONLY LINE OF.

    THE FIXTURE PROPERTY THAT MAKES THIS OBSERVABLE: the access charge is
    ``$10.005`` — a TRUE half cent, so ``quantize_money`` moves it. Seed the
    ``$50`` every other access-charge test uses and the raw and quantised
    renderings are the same eight characters, the assertion passes on the broken
    code, and the test is worthless.

    RED BEFORE / GREEN AFTER: the flat-charge producer rendered ``str(amt)``, so
    the line read ``10.00500000`` while ``totals.access_charge_usd`` — which is
    ``q(access_total)`` over this single charge — read ``10.01``.

    ⚠️ CORRECTED 2026-09-15 — this said "THE FIX IS THE LINE, NOT THE
    ACCUMULATOR", and that is exactly the half-fix that broke the identity this
    test is named for. With ONE line ``Σ q(line) ≡ q(Σ line)``, so this test
    cannot see the difference and passed either way; at two lines the line-only
    fix makes the itemisation exceed its own column by a cent. The accumulator
    sums the quantised leaf too now — see
    ``test_two_sub_cent_access_lines_sum_exactly_to_the_aggregate_column``,
    which is the test this one structurally could not be.
    """
    tenant = "t-itemisation-half-cent"
    await _seed_half_cent_access_month(db_session, tenant=tenant)
    month, *_ = _this_month()

    totals = (await generate_billing(T(tenant), month, db_session, test_settings))["totals"]

    # NON-VACUITY FIRST: prove the seeded fee really is sub-cent. If this ever
    # degrades to a whole cent every assertion below passes for free.
    assert quantize_money(_HALF_CENT_ACCESS_USD) != _HALF_CENT_ACCESS_USD, (
        f"{_HALF_CENT_ACCESS_USD} is already a whole cent, so the raw and "
        "quantised renderings are identical and this test cannot fail"
    )
    assert Decimal(totals["access_charge_usd"]) == quantize_money(_HALF_CENT_ACCESS_USD), (
        "the aggregate column is not q(the seeded fee); the fixture did not reach "
        "the flat access axis and the comparison below would be against nothing"
    )

    line = _flat_access_line(totals)
    assert Decimal(line["amount_usd"]) == Decimal(totals["access_charge_usd"]), (
        f"the itemised fee is {line['amount_usd']} but the column that totals it "
        f"says {totals['access_charge_usd']}: a bill whose only line does not add "
        "up to its own aggregate cannot be reconciled by anyone"
    )
    # ...and it is genuinely IN cents, not merely equal by luck of the compare.
    assert Decimal(line["amount_usd"]) == quantize_money(Decimal(line["amount_usd"]))


@pytest.mark.unit
async def test_the_seat_line_and_the_flat_line_render_the_same_form(
    db_session: AsyncSession, test_settings, monkeypatch
) -> None:
    """⭐ ONE ARRAY, ONE FORM — across ALL of its producers.

    ``access_charges`` is written by three code paths (per-seat, flat, and the
    explicit-zero line). Comparing a line only against the aggregate, as the test
    above does, cannot see a list that is internally inconsistent: it reads ONE
    element. This asserts the invariant over every element, and separately pins
    that BOTH producers actually ran.

    THE FIXTURE PROPERTY: a half-cent flat charge AND a per-seat charge on the
    same tenant-month. Every existing fixture seeds one or the other, so no test
    anywhere has ever had the two forms side by side in one array — which is how
    a list with two renderings in it stayed unremarkable.

    ⚠️ The seat rate is deliberately WHOLE ($25 x 3): if both lines carried
    sub-cent scale, a regression that dropped quantisation from BOTH producers
    would keep them consistent with each other and this would stay green. The
    flat line is the one under test; the seat line is the reference.
    """
    from services import seats as seats_svc

    tenant = "t-itemisation-two-forms"
    await _seed_half_cent_access_month(db_session, tenant=tenant)
    await ac_svc.upsert_access_charge(
        db_session,
        AccessChargeCreate(
            tenant_id=T(tenant),
            service_key="__seat__",  # pragma: allowlist secret
            amount_usd=_SEAT_RATE_USD,
            cadence="per_seat",
        ),
    )
    await db_session.commit()
    month, *_ = _this_month()

    _seats_answer(monkeypatch, _SEATS)
    try:
        totals = (await generate_billing(T(tenant), month, db_session, test_settings))["totals"]
    finally:
        # Module state with a 5-minute TTL — one test's answer must not decide
        # another file's.
        seats_svc.clear_seat_cache()

    # NON-VACUITY: both producers must have run. A seat stub that silently failed
    # yields seat_basis="unavailable" and a $0.00 line, which is quantised and
    # would satisfy the invariant below while exercising nothing.
    assert totals["seat_count"] == _SEATS, (
        f"seat_count is {totals['seat_count']!r} (basis {totals['seat_basis']!r}); "
        "the per-seat producer did not resolve, so the reference line is a $0.00 "
        "placeholder rather than a real seat charge"
    )
    lines_by_cadence = {c["cadence"]: c for c in totals["access_charges"]}
    assert {"monthly", "per_seat"} <= set(lines_by_cadence), (
        f"expected a flat AND a per-seat line in one array, got "
        f"{sorted(lines_by_cadence)}: the two forms are not side by side"
    )

    # ⭐ THE INVARIANT, over EVERY element of the list.
    mixed = {
        c["cadence"]: c["amount_usd"]
        for c in totals["access_charges"]
        if Decimal(c["amount_usd"]) != quantize_money(Decimal(c["amount_usd"]))
    }
    assert not mixed, (
        f"these access lines are rendered raw while their siblings are rendered "
        f"in cents: {mixed}. A reader cannot tell which form any given line is in"
    )


#: A per-seat RATE whose resolved CHARGE lands on a true half cent:
#: ``3.335 x 3 = 10.005``. The whole ``$25`` rate above is deliberately inert
#: under quantisation, so it cannot fork the two folds on the seat axis.
_HALF_CENT_SEAT_RATE_USD = Decimal("3.335")


async def _seed_two_half_cent_flat_charges(db_session: AsyncSession, *, tenant: str) -> None:
    """A billable month with TWO flat access charges, each on a true half cent.

    ⚠️ TWO DIFFERENT ``service_key``s. ``ix_accesscharge_open`` is UNIQUE on
    ``(tenant_id, coalesce(service_key,''))`` for open rows, so two concurrent
    flat charges need two keys; ``_applies_this_month`` does not look at
    ``service_key``, so both are billed.
    """
    db_session.add(TenantConfig(tenant_id=T(tenant), tenant_name=tenant, markup_pct=Decimal("30")))
    for key in (PRODUCT, OTHER_PRODUCT):
        await ac_svc.upsert_access_charge(
            db_session,
            AccessChargeCreate(
                tenant_id=T(tenant), service_key=key, amount_usd=_HALF_CENT_ACCESS_USD
            ),
        )
    await db_session.commit()


@pytest.mark.unit
async def test_two_sub_cent_access_lines_sum_exactly_to_the_aggregate_column(
    db_session: AsyncSession, test_settings
) -> None:
    """⭐ THE ITEMISATION MUST EQUAL THE AGGREGATE ACROSS **ALL** ITS LINES.

    THE FIXTURE PROPERTY THAT MAKES THIS OBSERVABLE, and the reason the test
    above it could not: **TWO** flat access charges, each at a true half cent.
    With one line ``Σ q(line)`` and ``q(Σ line)`` are the same number, so a
    fixture holding a single charge is green whichever fold the accumulator
    uses — the file's own single-line test says so in its title ("THE ONLY LINE
    OF"). At two half-cent lines the two folds are a full cent apart
    (``10.01 + 10.01 = 20.02`` against ``q(20.010) = 20.01``), and only then can
    a bill whose lines do not add up to its own column be seen at all.

    RED BEFORE / GREEN AFTER: with the line quantised and ``access_total``
    accumulating the RAW amount, ``totals.access_charge_usd`` reads ``20.01``
    under an itemisation summing to ``20.02``.
    """
    tenant = "t-itemisation-two-flat-lines"
    await _seed_two_half_cent_flat_charges(db_session, tenant=tenant)
    month, *_ = _this_month()

    totals = (await generate_billing(T(tenant), month, db_session, test_settings))["totals"]

    # NON-VACUITY, in the two ways this fixture can silently degrade.
    assert quantize_money(_HALF_CENT_ACCESS_USD) != _HALF_CENT_ACCESS_USD, (
        f"{_HALF_CENT_ACCESS_USD} is already a whole cent: the two folds agree "
        "on it and this test cannot fail"
    )
    flat = [c for c in totals["access_charges"] if c["cadence"] != "per_seat"]
    assert len(flat) == 2, (
        f"expected TWO flat access lines, got {flat!r}: at one line the "
        "itemisation identity holds under either fold and proves nothing"
    )
    assert quantize_money(_HALF_CENT_ACCESS_USD) * 2 != quantize_money(_HALF_CENT_ACCESS_USD * 2), (
        "the per-line and per-axis folds agree on these amounts; the fixture is inert"
    )

    # ⭐ THE IDENTITY: the lines add up to the column that totals them, EXACTLY.
    itemised = sum(Decimal(c["amount_usd"]) for c in flat)
    assert itemised == Decimal(totals["access_charge_usd"]), (
        f"the itemised access lines sum to {itemised} but the column that totals "
        f"them says {totals['access_charge_usd']}: an invoice whose lines do not "
        "add up to its own aggregate cannot be reconciled by anyone"
    )
    # ...and the figure is the per-LINE fold, not the per-axis one.
    assert itemised == quantize_money(_HALF_CENT_ACCESS_USD) * 2
    assert itemised != quantize_money(_HALF_CENT_ACCESS_USD * 2)


@pytest.mark.unit
async def test_two_sub_cent_seat_lines_sum_exactly_to_the_seat_fee_column(
    db_session: AsyncSession, test_settings, monkeypatch
) -> None:
    """⭐ THE SAME IDENTITY, ON THE SEAT AXIS — which has the same shape.

    ``seat_fee_total`` and ``access_total`` are separate accumulators against
    separate columns (``seat_fee_usd`` / ``access_charge_usd``), so the flat
    axis's fix does not reach this one and a test of the flat axis cannot
    observe it.

    THE FIXTURE PROPERTY: TWO per-seat terms whose RESOLVED CHARGE carries
    sub-cent scale (``3.335 x 3 = 10.005`` each). Both halves are needed — one
    term makes the two folds identical, and the ``$25`` rate every other seat
    fixture uses is unmoved by quantisation at any seat count.

    RED BEFORE / GREEN AFTER: with ``seat_fee_total`` accumulating
    ``seat_rate * seat_count`` raw, ``totals.seat_fee_usd`` reads ``20.01``
    under two lines itemised at ``10.01`` each.
    """
    from services import seats as seats_svc

    tenant = "t-itemisation-two-seat-lines"
    db_session.add(TenantConfig(tenant_id=T(tenant), tenant_name=tenant, markup_pct=Decimal("30")))
    for key in ("__seat__", "__seat_two__"):  # pragma: allowlist secret
        await ac_svc.upsert_access_charge(
            db_session,
            AccessChargeCreate(
                tenant_id=T(tenant),
                service_key=key,
                amount_usd=_HALF_CENT_SEAT_RATE_USD,
                cadence="per_seat",
            ),
        )
    await db_session.commit()
    month, *_ = _this_month()

    _seats_answer(monkeypatch, _SEATS)
    try:
        totals = (await generate_billing(T(tenant), month, db_session, test_settings))["totals"]
    finally:
        seats_svc.clear_seat_cache()

    # NON-VACUITY: the seat lookup ran, there are two lines, and the resolved
    # charge really is sub-cent.
    assert totals["seat_count"] == _SEATS, (
        f"seat_count is {totals['seat_count']!r} (basis {totals['seat_basis']!r}): "
        "a swallowed SeatCountUnavailableError disarms this fixture to $0.00 lines"
    )
    charge = _HALF_CENT_SEAT_RATE_USD * _SEATS
    assert quantize_money(charge) != charge, (
        f"{charge} is already a whole cent; the two folds agree and this cannot fail"
    )
    seat_lines = [c for c in totals["access_charges"] if c["cadence"] == "per_seat"]
    assert len(seat_lines) == 2, (
        f"expected TWO per-seat lines, got {seat_lines!r}: at one line the "
        "identity holds under either fold"
    )

    # ⭐ THE IDENTITY.
    itemised = sum(Decimal(c["amount_usd"]) for c in seat_lines)
    assert itemised == Decimal(totals["seat_fee_usd"]), (
        f"the itemised seat lines sum to {itemised} but seat_fee_usd says "
        f"{totals['seat_fee_usd']}: the seat axis does not add up to itself"
    )
    assert itemised == quantize_money(charge) * 2
    assert itemised != quantize_money(charge * 2)


#: Far beyond any ``bill_ts`` this suite computes, so a row carrying it is OPEN
#: for billing purposes while still being a non-NULL ``effective_until`` — the
#: only way two flat charges can share one ``service_key``. See the seeder.
_FAR_FUTURE = datetime(2099, 1, 1, tzinfo=UTC)


async def _seed_two_half_cent_charges_on_one_app(
    db_session: AsyncSession, *, tenant: str
) -> uuid.UUID:
    """One APP carrying TWO concurrent flat charges, each on a true half cent.

    ⚠️ INSERTED DIRECTLY, AND THE REASON IS A REAL FINDING ABOUT THE SHAPE.
    ``ac_svc.upsert_access_charge`` is the only writer, and it closes the prior
    open row for the lineage at ``now`` — which is strictly before this month's
    ``bill_ts`` (the last instant of the month), so the superseded row is
    filtered out by ``active_access_charges_for_month`` and the two are never
    billable together. ``ix_accesscharge_open`` then forbids the alternative
    (two rows with ``effective_until IS NULL``). So TWO CONCURRENT FLAT TERMS ON
    ONE ``service_key`` CANNOT BE PRODUCED THROUGH THE SERVICE TODAY; they are
    reachable only by writing an ``effective_until`` that is in the future,
    which the schema permits and no writer emits.

    ⭐ THAT IS WHY THE TEST IS WORTH HAVING RATHER THAN WHY IT IS VACUOUS. The
    accumulator under test is written as a FOLD (``+=``), and a fold is only
    distinguishable from an assignment at two or more terms. The mutation is
    ALSO caught at one term — ``access_by_app[k] = amt`` renders
    ``"10.00500000"`` where the line beside it says ``"10.01"`` — but at one
    term the ``Σq`` / ``qΣ`` property the ``+=`` exists for is unobservable, and
    a future writer that does emit a forward-dated ``effective_until`` (or a
    second concurrent lineage on one key) would land on exactly this shape.

    Returns the licence, whose ``app_key`` resolves to ``PRODUCT`` — the
    metering row is what puts the key in ``license_usage['service_keys']``, and
    without it the licence entry would look ``access_by_app`` up under
    ``"unknown"`` and read ``$0.00`` whatever the fold does.
    """
    db_session.add(TenantConfig(tenant_id=T(tenant), tenant_name=tenant, markup_pct=Decimal("30")))
    lic = uuid.uuid4()
    await _seed_cost_daily(
        db_session, tenant=tenant, license_id=lic, service_key=PRODUCT, cost="10"
    )
    await _seed_metering_daily(db_session, tenant=tenant, license_id=lic, service_key=PRODUCT)
    for until in (_FAR_FUTURE, None):
        db_session.add(
            TenantAccessCharge(
                tenant_id=T(tenant),
                service_key=PRODUCT,
                amount_usd=_HALF_CENT_ACCESS_USD,
                cadence="monthly",
                effective_from=datetime(2026, 1, 1, tzinfo=UTC),
                effective_until=until,
            )
        )
    await db_session.commit()
    return lic


@pytest.mark.unit
async def test_the_per_app_access_breakdown_sums_the_cents_its_own_lines_render(
    db_session: AsyncSession, test_settings
) -> None:
    """⭐ ``licenses[].costs.access_charge_usd`` IS a breakdown, so it must add up.

    It is the per-app split of the APP-SCOPED lines in ``totals.access_charges[]``
    — those lines, cut by ``service_key`` — and every one of them renders
    ``quantize_money(amt)``. A breakdown accumulating the RAW amount is the same
    defect as ``access_total`` was, one level down: two $10.005 fees on one app
    itemise as ``10.01 + 10.01 = 20.02`` under a per-app figure reading
    ``20.010000``.

    ⛔ CORRECTED 2026-09-15 — "the same lines" was too strong and this test must
    not be read as pinning that. ``access_by_app`` is populated under
    ``if c.service_key is not None``, so a TENANT-WIDE charge
    (``service_key IS NULL``) is in ``access_charges[]`` and in
    ``totals.access_charge_usd`` but in NO per-app bucket: that tenant's
    ``licenses[].costs.access_charge_usd`` are all the 0 default under a non-zero
    aggregate, and Σ-breakdown ≠ aggregate by the whole fee. Measured on the prod
    ``billing`` DB 2026-09-15, the only tenant with an open access charge has
    exactly that shape ($5000.00 ``monthly``, ``service_key IS NULL``). This test
    pins the FOLD on the app-scoped path; it says nothing about the NULL-key
    fee, and no test does, because attributing one is an unmade commercial
    decision (see the comment at that ``if``).

    ⚠️ MUTATION TARGET: in ``services.billing.generate_billing``, restore
    ``access_by_app[c.service_key] = access_by_app.get(c.service_key, 0) + amt``
    in place of ``+ quantize_money(amt)``.

    ⚠️ NO MONEY MOVES ON THIS ONE, AND THE DOCSTRING SAYS SO. ``access_by_app``
    feeds no total and no invoice column — ``access_total`` is a separate
    accumulator — so this field is a DISPLAYED BREAKDOWN only. That is a reason
    to state the scope precisely, not a reason to leave the fold unexercised:
    the fold is deployed, it was reachable by mutation with 136 tests green, and
    a breakdown that disagrees with the lines it breaks down is unreconcilable
    by the person reading it whether or not it is summed anywhere.

    THE FIXTURE PROPERTY: two half-cent flat charges on ONE ``service_key`` —
    see the seeder for why that shape has to be written directly, and for what
    that says about its reachability.
    """
    tenant = "t-itemisation-per-app-breakdown"
    lic = await _seed_two_half_cent_charges_on_one_app(db_session, tenant=tenant)
    month, *_ = _this_month()

    result = await generate_billing(T(tenant), month, db_session, test_settings)
    totals = result["totals"]

    # NON-VACUITY, three ways this fixture can silently degrade into nothing.
    assert quantize_money(_HALF_CENT_ACCESS_USD) != _HALF_CENT_ACCESS_USD, (
        f"{_HALF_CENT_ACCESS_USD} is already a whole cent: raw and quantised "
        "renderings coincide and this test cannot fail"
    )
    assert quantize_money(_HALF_CENT_ACCESS_USD) * 2 != quantize_money(_HALF_CENT_ACCESS_USD * 2), (
        "the per-term and per-axis folds agree on these amounts; the fixture is inert"
    )
    app_lines = [
        c
        for c in totals["access_charges"]
        if c["cadence"] != "per_seat" and c["service_key"] == PRODUCT
    ]
    assert len(app_lines) == 2, (
        f"expected TWO flat lines on {PRODUCT!r}, got {app_lines!r}: the second "
        "row was filtered out at bill_ts (or the index rejected it) and the "
        "breakdown has collapsed to a single leaf, where a fold proves nothing"
    )

    entry = next(e for e in result["licenses"] if e["license_id"] == str(lic))
    assert entry["app_key"] == PRODUCT, (
        f"the licence resolved to app_key {entry['app_key']!r}, so its "
        "access_charge_usd is a lookup miss reading $0.00 rather than the "
        "breakdown under test"
    )

    # ⭐ THE IDENTITY: the per-app breakdown equals the cents ITS OWN lines render.
    breakdown = Decimal(entry["costs"]["access_charge_usd"])
    itemised = sum(Decimal(c["amount_usd"]) for c in app_lines)
    assert breakdown == itemised, (
        f"the per-app access breakdown for {PRODUCT!r} is {breakdown} but the "
        f"lines it breaks down render {' + '.join(c['amount_usd'] for c in app_lines)} "
        f"= {itemised}: the breakdown is summing the raw amounts while the "
        "itemisation shows the cents each fee is charged as"
    )
    # ...and it is the per-TERM fold, not the per-axis one.
    assert breakdown == quantize_money(_HALF_CENT_ACCESS_USD) * 2
    assert breakdown != quantize_money(_HALF_CENT_ACCESS_USD * 2)
    # The aggregate column folds the same way, so the two agree here by
    # construction — asserted so a fix to one that misses the other is caught.
    assert breakdown == Decimal(totals["access_charge_usd"])


# ---------------------------------------------------------------------------
# 2. costs{} — a displayed total equals its own displayed components
# ---------------------------------------------------------------------------


@pytest.mark.unit
async def test_billed_cost_usd_equals_the_two_components_it_is_displayed_as(
    db_session: AsyncSession, test_settings
) -> None:
    """⭐ ``billed_cost_usd == app_request_billed_usd + llm_billed_usd``, AS SHOWN.

    THE FIXTURE PROPERTY: BOTH billed streams are sub-cent — ``$0.004999`` of LLM
    spend and one metered request at ``$0.003`` — so their sum is not a whole
    cent and the quantised total genuinely differs from the raw addends. Priced
    fixtures elsewhere bill whole dollars (``tests/test_project_markup_pricing.py``
    asserts ``15.00``), where ``q(x) == x`` and this identity holds either way.

    ⚠️ THE APP-REQUEST RATE IS LOAD-BEARING, not decoration. With
    ``billing_app_cost_rates`` empty — which is the estate's measured live state —
    ``app_request_billed_usd`` is ``0`` and the identity collapses to
    ``billed_cost_usd == llm_billed_usd``, true by construction on broken code.

    RED BEFORE / GREEN AFTER: the total rendered ``q(billed_cost)`` while its two
    addends rendered raw.

    ⛔ AND THE FIX IS THE TOTAL, NOT THE ADDENDS. ``services.invoices.
    _money_from_snapshot`` rebuilds four invoice columns from exactly those two
    keys and its docstring names rounding them as a defect: it would add four
    half-cent errors to a residual the close gate allows one cent of.
    """
    tenant = "t-costs-subcent"
    lic = await _seed_subcent_billed_month(db_session, tenant=tenant)
    month, *_ = _this_month()

    result = await generate_billing(T(tenant), month, db_session, test_settings)
    entry = next(e for e in result["licenses"] if e["license_id"] == str(lic))
    costs = entry["costs"]

    app_billed = Decimal(costs["app_request_billed_usd"])
    llm_billed = Decimal(costs["llm_billed_usd"])

    # NON-VACUITY, three ways — this fixture is elaborate enough that a silent
    # mis-seed is the likeliest way for it to rot into a test of nothing.
    assert app_billed > 0, (
        "the app-request stream billed nothing; the AppCostRate/MeteringDaily "
        "pair did not take and the identity is only covering half the money"
    )
    assert llm_billed > 0, "the llm stream billed nothing"
    assert quantize_money(app_billed + llm_billed) != app_billed + llm_billed, (
        f"the two components sum to {app_billed + llm_billed}, which is already a "
        "whole cent — the raw and quantised renderings coincide and this test "
        "cannot distinguish the fixed code from the broken code"
    )

    assert Decimal(costs["billed_cost_usd"]) == app_billed + llm_billed, (
        f"billed_cost_usd is {costs['billed_cost_usd']} but the two components it "
        f"is displayed as the sum of are {costs['app_request_billed_usd']} + "
        f"{costs['llm_billed_usd']} = {app_billed + llm_billed}"
    )


# ---------------------------------------------------------------------------
# 3. ...and it must survive the response model, which is where keys go to die
# ---------------------------------------------------------------------------


def _dropped_paths(emitted: object, survived: object, path: str = "$") -> list[str]:
    """Every key present in the generator's dict and absent after the model.

    ⭐ STRUCTURAL, NOT PER-MODEL. It walks the emitted document and the
    serialised model side by side, so it covers ``BillingResponse``,
    ``BillingTotals``, ``AccessChargeLine``, ``LicenseBilling``,
    ``LicenseUsageBilling``, ``LicenseCostBilling`` and ``LicenseTokenBilling``
    without naming any of them — and it covers the EIGHTH model nobody has
    written yet. A per-model key list would be the same trap one level along:
    it pins the models that exist today and says nothing about the next one.

    ⚠️ KEYS ONLY, NEVER VALUES. Money legitimately changes representation
    crossing the boundary (``str`` -> ``Decimal`` -> ``str``); the arithmetic
    identities that DO care about the value live in the tests above. Compared
    against ``model_dump`` rather than ``model_fields`` for the reason the
    original cost-block test named: a field that is declared but excluded from
    serialisation is just as absent from the customer's bill.
    """
    dropped: list[str] = []
    if isinstance(emitted, dict):
        if not isinstance(survived, dict):
            return [f"{path} (an object became {type(survived).__name__})"]
        for key, value in emitted.items():
            if key not in survived:
                dropped.append(f"{path}.{key}")
            else:
                survived_map = cast("dict[object, object]", survived)
                dropped += _dropped_paths(value, survived_map[key], f"{path}.{key}")
    elif isinstance(emitted, list):
        if not isinstance(survived, list) or len(survived) != len(emitted):
            return [f"{path} (a {len(emitted)}-element list became {survived!r})"]
        for i, item in enumerate(emitted):
            dropped += _dropped_paths(item, survived[i], f"{path}[{i}]")
    return dropped


def _emitted_key_count(node: object) -> int:
    """How many keys the walker above actually had to check — the non-vacuity
    denominator. A fixture that degraded to an empty month makes
    ``_dropped_paths`` return ``[]`` for the best possible reason and the worst:
    there was nothing to drop."""
    if isinstance(node, dict):
        return len(node) + sum(_emitted_key_count(v) for v in node.values())
    if isinstance(node, list):
        return sum(_emitted_key_count(v) for v in node)
    return 0


async def _seed_month_reaching_every_model(db_session: AsyncSession, *, tenant: str) -> uuid.UUID:
    """A month that populates EVERY model in the response chain at once.

    The sub-cent billed month gives ``licenses[]`` — and with it the usage, costs
    and tokens blocks. The two access charges give ``totals.access_charges[]``
    BOTH of its non-trivial producers: a flat monthly line and a per-seat line.
    Without the per-seat row the three seat keys on ``AccessChargeLine`` are
    never emitted by anything and the walker cannot see them missing.
    """
    lic = await _seed_subcent_billed_month(db_session, tenant=tenant)
    await ac_svc.upsert_access_charge(
        db_session,
        AccessChargeCreate(
            tenant_id=T(tenant), service_key=PRODUCT, amount_usd=_HALF_CENT_ACCESS_USD
        ),
    )
    await ac_svc.upsert_access_charge(
        db_session,
        AccessChargeCreate(
            tenant_id=T(tenant),
            service_key="__seat__",  # pragma: allowlist secret
            amount_usd=_SEAT_RATE_USD,
            cadence="per_seat",
        ),
    )
    await db_session.commit()
    return lic


@pytest.mark.unit
async def test_every_generated_key_survives_its_response_model(
    db_session: AsyncSession, test_settings, monkeypatch
) -> None:
    """⛔ A KEY THE GENERATOR EMITS AND NO MODEL DECLARES IS DROPPED SILENTLY.

    None of these models sets ``model_config``, so Pydantic v2 defaults to
    ``extra='ignore'``: an undeclared key vanishes at ``routers/billing.py``
    behind a 200 with nothing in the logs. ``InvoiceDocument`` carries the same
    warning in ``models/domain.py``; this is the test that side has and this side
    did not. Measured, this had happened FOUR times over, on four different
    models:

      * ``LicenseCostBilling`` — the whole billed/markup axis (``billed_cost_usd``,
        ``llm_billed_usd``, ``app_request_billed_usd``, ``markup_pct``,
        ``billed_by_provider``): what the spend cost US reached the caller and
        what it cost THEM did not.
      * ``BillingTotals`` — ``total_denied``, ``total_budget_denied``,
        ``billed_by_provider``, and ⭐ the entire SEAT axis (``seat_fee_usd``,
        ``seat_count``, ``seat_basis``). ``seat_fee_usd`` is a
        ``_TOTAL_COMPONENT_COLUMNS`` member, so a caller reconciling
        ``customer_cost_usd`` off this response was short by the whole seat fee.
      * ``BillingResponse`` — ``effective_markup_pct`` and ``billing_mode``.
      * ``AccessChargeLine`` — ``seat_rate_usd`` / ``seat_count`` /
        ``seat_basis``, the "$25 x 3" the per-seat producer emits *because* its
        own comment says a line reading "$75" alone is not reconcilable.

    ⭐ THE EXPECTED SET IS DERIVED FROM THE GENERATOR AND THE SHAPE IS WALKED,
    NEVER LISTED. The predecessor of this test checked one hardcoded model, which
    is why the other three drops sat underneath it undetected. This reds for the
    next field added to ``generate_billing`` and forgotten — whichever model it
    belongs to, including a model that does not exist yet.
    """
    from services import seats as seats_svc

    tenant = "t-response-model-coverage"
    lic = await _seed_month_reaching_every_model(db_session, tenant=tenant)
    month, *_ = _this_month()

    _seats_answer(monkeypatch, _SEATS)
    try:
        result = await generate_billing(T(tenant), month, db_session, test_settings)
    finally:
        seats_svc.clear_seat_cache()

    # --- NON-VACUITY: prove the fixture actually reached every model ---------
    entry = next(e for e in result["licenses"] if e["license_id"] == str(lic))
    assert {"usage", "costs", "tokens"} <= set(entry), (
        f"the licence entry is {sorted(entry)}: the per-licence sub-models were "
        "never populated, so the walk below cannot see a drop in any of them"
    )
    cadences = {c["cadence"] for c in result["totals"]["access_charges"]}
    assert {"monthly", "per_seat"} <= cadences, (
        f"access_charges carries cadences {sorted(map(str, cadences))}: both "
        "producers must run or AccessChargeLine's seat keys are never emitted"
    )
    assert result["totals"]["seat_count"] == _SEATS, (
        f"seat_count is {result['totals']['seat_count']!r} (basis "
        f"{result['totals']['seat_basis']!r}); the seat stub did not resolve and "
        "the seat axis is a $0.00 placeholder"
    )
    emitted_keys = _emitted_key_count(result)
    assert emitted_keys >= 50, (
        f"the generator emitted only {emitted_keys} keys in total; the month "
        "degraded and an empty walk passes for the wrong reason"
    )

    # --- THE WALK -----------------------------------------------------------
    survived = BillingResponse.model_validate(result).model_dump(mode="json")
    dropped = _dropped_paths(result, survived)
    assert not dropped, (
        f"these keys are emitted by services/billing.py and never reach the "
        f"wire: {dropped}. Declare each on the model that owns its level — "
        "Pydantic drops them with a 200 and no log line"
    )

    # And the same check against ``LicenseCostBilling`` ON ITS OWN, kept because
    # it is the one block whose loss was measured rather than reasoned about —
    # and because validating the sub-model directly proves the drop is not being
    # masked by some parent's serialisation.
    costs = entry["costs"]
    costs_survived = set(LicenseCostBilling.model_validate(costs).model_dump(mode="json"))
    assert set(costs) <= costs_survived, (
        f"LicenseCostBilling drops {sorted(set(costs) - costs_survived)} when validated on its own"
    )


@pytest.mark.unit
async def test_the_billed_cost_identity_holds_at_the_response_boundary(
    db_session: AsyncSession, test_settings
) -> None:
    """⭐ THE SAME IDENTITY, ON WHAT THE CALLER ACTUALLY RECEIVES.

    The internal-dict assertion above is necessary and not sufficient: these
    figures pass through ``BillingResponse`` -> ``LicenseBilling`` ->
    ``LicenseCostBilling``, which coerces every money field from the generator's
    ``str`` to ``Decimal``. An identity that holds on the dict and breaks on the
    wire is still a bill that does not add up, and until ``billed_cost_usd`` was
    DECLARED on that model this assertion could not even be written — the field
    was dropped before any caller could compare it.

    THE FIXTURE PROPERTY is the same sub-cent pair as above, and it matters more
    here: coercion through a numeric type is exactly where sub-cent digits get
    lost, and on whole-cent money there are none to lose.
    """
    tenant = "t-costs-boundary"
    lic = await _seed_subcent_billed_month(db_session, tenant=tenant)
    month, *_ = _this_month()

    result = await generate_billing(T(tenant), month, db_session, test_settings)
    resp = BillingResponse.model_validate(result)
    costs = next(e for e in resp.licenses if e.license_id == str(lic)).costs

    # NON-VACUITY: the coercion must not have flattened the sub-cent tail, and
    # both streams must carry money.
    assert costs.app_request_billed_usd > 0 and costs.llm_billed_usd > 0
    total = costs.app_request_billed_usd + costs.llm_billed_usd
    assert quantize_money(total) != total, (
        f"the components sum to {total}, a whole cent on the wire — either the "
        "fixture degraded or the model rounded, and either way this cannot fail"
    )

    assert costs.billed_cost_usd == total, (
        f"on the response model billed_cost_usd is {costs.billed_cost_usd} but "
        f"its two components sum to {total}"
    )

    # And in the SERIALISED body, which is what the caller parses.
    dumped = costs.model_dump(mode="json")
    assert Decimal(str(dumped["billed_cost_usd"])) == Decimal(
        str(dumped["app_request_billed_usd"])
    ) + Decimal(str(dumped["llm_billed_usd"]))
