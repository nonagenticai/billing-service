"""A READER of the pricer must report what the INVOICE charges (0036).

The project axis landed in ``services.billing`` — the invoice pricer resolves
markup with ``project_id`` and groups its ``CostDaily`` aggregate by it. Every
OTHER surface that prices the same rows is a second implementation of the same
money, and each one that was still calling the pricer without ``project_id``
would, the moment any project-scoped rule exists, put a number in front of a
customer that the bill does not agree with:

- ``routers.billing.compute_billing_overview``   — the all-tenant overview
- ``services.admin_aggregate.system_usage``      — the admin system report
- ``routers.usage`` ``/me/series`` + ``/me/breakdown`` — the usage dashboard

Two distinct defects had to be closed at each site, and this module would pass
on only one of them being fixed if it asserted anything weaker than equality:

1. THE ARGUMENT. ``compute_billed_line(..., project_id=...)`` — without it the
   pricer defaulted to ``None`` and ``_matches`` could never select a
   project-pinned rule, so the reader silently charged the tenant-wide rate.
2. THE GRAIN. ``project_id`` in the aggregate's GROUP BY — without it rows
   differing only in project are summed BEFORE any rule resolves, at which
   point no per-project rate is expressible no matter what argument is passed.

⭐ THE ASSERTION IS EQUALITY WITH ``generate_billing``, NOT A LITERAL. A literal
expected total would be a second opinion about what the bill should be, and if
the invoice pricer changed (a rescale, a new axis, a rounding change) the
literal would keep passing while the reader drifted away from the actual bill.
The property under test is "reader == invoice", so the invoice is the fixture.

⚠️ The fixture is built so that equality is NOT vacuous —
``test_fixture_actually_exercises_the_project_axis`` proves the project rule
moves the invoice away from BOTH project-blind totals. Without that guard, a
reader and an invoice that both ignored the project axis would agree perfectly
and this file would be green while the defect stood.

⚠️ Fixtures write ``CostDaily`` rows DIRECTLY rather than through the rollup —
same reason as ``tests/test_project_markup_pricing.py``: reading the axis back
out of the rollup would measure the rollup lane instead of the pricers, and
would read green-because-sentinel if the rollup were not writing projects yet.
"""

from __future__ import annotations

import uuid
from datetime import UTC, date, datetime
from decimal import Decimal

import httpx
import pytest
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from models.db import (
    NULL_PROJECT_ID,
    CostDaily,
    MarkupRule,
    TenantAccessCharge,
    TenantConfig,
)
from routers.billing import compute_billing_overview
from services.admin_aggregate import system_usage
from services.billing import generate_billing
from services.markup import quantize_money
from tests.conftest import T

PAST = datetime(2026, 1, 1, tzinfo=UTC)

# Every percentage is DIFFERENT, so a billed total names the mechanism that
# produced it rather than merely "some markup was applied".
FALLBACK_PCT = Decimal("10")  # tenant_config.markup_pct — no rule at all
TENANT_WIDE_PCT = Decimal("30")  # tenant-only rule       → specificity 1
PROJECT_PCT = Decimal("100")  # tenant + project rule  → specificity 2+1 = 3
# A rule pinned to the SENTINEL uuid. `billing_cost_daily.project_id` is NOT
# NULL and stores `NULL_PROJECT_ID` for "no project"; a reader that forwards
# that sentinel raw instead of unwrapping it to `None` matches THIS rule, and
# the invoice (which unwraps) matches the tenant-wide one. That divergence is
# invisible without a sentinel-pinned rule to catch it.
SENTINEL_TRAP_PCT = Decimal("80")

LINE_COST = Decimal("10.000000")


def _today() -> date:
    return datetime.now(UTC).date()


def _month() -> str:
    return _today().strftime("%Y-%m")


def _rule(**kw) -> MarkupRule:
    kw.setdefault("effective_from", PAST)
    return MarkupRule(**kw)


def _cost_row(*, tenant, license_id, subject_id, project_id) -> CostDaily:
    """One day of LLM spend. EVERY dimension but the project is held constant,
    so the only thing that can make two of these bill differently is the
    project — and the only thing that can make a reader disagree with the
    invoice is that it dropped or mangled that project."""
    return CostDaily(
        tenant_id=tenant,
        tenant_name="readers-agree",
        license_id=license_id,
        subject_id=subject_id,
        project_id=project_id,
        service_key="inference-router",  # pragma: allowlist secret
        provider="openai",
        served_model="gpt-5",
        byo=False,
        cost_source="inference-router",
        bucket_date=_today(),
        total_input_tokens=1000,
        total_output_tokens=500,
        total_cost_usd=LINE_COST,
        request_count=1,
    )


async def _seed(session: AsyncSession, label: str) -> tuple[str, uuid.UUID]:
    """Seed one tenant with three cost lines that differ ONLY in project:

    * ``project_x``        → the project-pinned rule   (100%)
    * ``project_y``        → no project rule, so the tenant-wide rule (30%)
    * ``NULL_PROJECT_ID``  → no project asserted, so the tenant-wide rule (30%)

    Returns ``(tenant_id, subject_id)``.

    ⚠️ NO ``MeteringDaily`` rows are seeded on purpose. The invoice's
    app-request line prices from ``billing_metering_daily``, which carries no
    project column at all; leaving that axis empty keeps every figure compared
    here an LLM-cost figure, so an equality can only be broken by the project
    axis and not by an unrelated request-volume difference.
    """
    tenant = T(label)
    lic = uuid.uuid4()
    subject = uuid.uuid4()
    project_x, project_y = uuid.uuid4(), uuid.uuid4()

    session.add(
        TenantConfig(
            tenant_id=tenant,
            tenant_name="readers-agree",
            markup_pct=FALLBACK_PCT,
            billing_mode="standard_markup",
        )
    )
    session.add(_rule(tenant_id=tenant, markup_pct=TENANT_WIDE_PCT))
    session.add(_rule(tenant_id=tenant, project_id=project_x, markup_pct=PROJECT_PCT))
    session.add(_rule(tenant_id=tenant, project_id=NULL_PROJECT_ID, markup_pct=SENTINEL_TRAP_PCT))
    session.add_all(
        [
            _cost_row(tenant=tenant, license_id=lic, subject_id=subject, project_id=p)
            for p in (project_x, project_y, NULL_PROJECT_ID)
        ]
    )
    await session.commit()
    return tenant, subject


async def _invoice_total(session: AsyncSession, tenant: str, settings) -> Decimal:
    result = await generate_billing(tenant, _month(), session, settings)
    return Decimal(result["totals"]["customer_cost_usd"])


async def _invoice_internal(session: AsyncSession, tenant: str, settings) -> Decimal:
    """The INTERNAL column of the same invoice — `quantize_money(total_internal)`.

    Same shape and the same anchoring discipline as `_invoice_total` above: the
    invoice is the reference implementation for both columns, so a reader's
    internal figure is compared with the invoice's rather than with a literal.
    """
    result = await generate_billing(tenant, _month(), session, settings)
    return Decimal(result["totals"]["internal_cost_usd"])


@pytest.mark.unit
async def test_fixture_actually_exercises_the_project_axis(db_session, test_settings):
    """⭐ NON-VACUITY GUARD FOR EVERY OTHER TEST IN THIS FILE.

    Equality between a reader and the invoice is worthless if neither of them
    can see the project axis: two blind implementations agree perfectly. So
    pin, first, that the invoice total is a number ONLY a project-aware pricer
    can produce — different from what a project-blind pricer would report at
    either of the two rates in play.
    """
    tenant, _subject = await _seed(db_session, "ragree-fixture")
    total = await _invoice_total(db_session, tenant, test_settings)

    gross = LINE_COST * 3
    blind_at_tenant_rate = (gross * (1 + TENANT_WIDE_PCT / 100)).quantize(Decimal("0.01"))
    blind_at_project_rate = (gross * (1 + PROJECT_PCT / 100)).quantize(Decimal("0.01"))

    # $10 @100% + $10 @30% + $10 @30% = $20 + $13 + $13.
    assert total == Decimal("46.00")
    assert total != blind_at_tenant_rate  # 39.00 — project rule never matched
    assert total != blind_at_project_rate  # 60.00 — project rule over-applied


@pytest.mark.unit
async def test_overview_total_equals_invoice(db_session, test_settings):
    """`routers.billing.compute_billing_overview` — the all-tenant overview.

    This is the surface a customer-facing total is read off, and it is supposed
    to reconcile to the sum of the per-tenant invoices.
    """
    tenant, _subject = await _seed(db_session, "ragree-overview")
    invoice = await _invoice_total(db_session, tenant, test_settings)

    overview = await compute_billing_overview(_month(), db_session)
    entry = {t.tenant_id: t for t in overview.tenants}[tenant]

    assert Decimal(entry.customer_cost_usd) == invoice


@pytest.mark.unit
async def test_admin_system_usage_total_equals_invoice(db_session, test_settings):
    """`services.admin_aggregate.system_usage` — the admin system report."""
    tenant, _subject = await _seed(db_session, "ragree-admin")
    invoice = await _invoice_total(db_session, tenant, test_settings)

    system = await system_usage(db_session, test_settings, "today")
    row = {t.tenant_id: t for t in system.by_tenant}[tenant]

    assert Decimal(row.billed_cost_usd) == invoice


@pytest.mark.unit
async def test_usage_dashboard_totals_equal_invoice(db_engine, app_client, test_settings):
    """`routers.usage` ``/me/series`` and ``/me/breakdown`` — the dashboard.

    Both payloads re-aggregate per-line billed amounts (per day, per service);
    every seeded line shares one day and one service_key, so each payload's
    total is the whole tenant's billed cost for the window and is directly
    comparable with the invoice.

    ⭐ THE EQUALITY HOLDS ONLY BECAUSE THERE IS EXACTLY ONE BUCKET, AND THAT
    PRECONDITION IS NOW ASSERTED RATHER THAN ASSUMED. Each bucket emits
    `quantize_money(billed)` — it is an EMITTED FIGURE, the cent drawn in that
    cell of the chart or table, and correctly so: neither payload carries a
    total of its own, so there is no `q(Σ raw)` anywhere for a `Σ q` to
    contradict. What a reader that re-sums the buckets gets is `Σ q(bᵢ)`, which
    drifts from the invoice's `q(Σ bᵢ)` by up to half a cent PER BUCKET — about
    ±$0.15 over a 30-day window.
    ⛔ So this assertion is NOT the general property "the series sums to the
    invoice". That property is false, and asserting it against a one-bucket
    fixture made it look true. It is the narrow, real property "the dashboard
    prices the same rows the same way", which is what this module is about
    (the project axis) — and the `len(...) == 1` guards below are what stop a
    later fixture from adding a second day and quietly turning a true statement
    into a false one that still passes.
    """
    factory = async_sessionmaker(bind=db_engine, class_=AsyncSession, expire_on_commit=False)
    async with factory() as session:
        tenant, subject = await _seed(session, "ragree-usage")
        invoice = await _invoice_total(session, tenant, test_settings)

    headers = {"X-Subject-Id": str(subject), "X-Tenant-Id": tenant}

    series = app_client.get("/api/usage/me/series?window=30d&granularity=daily", headers=headers)
    assert series.status_code == 200, series.text
    buckets = series.json()["data"]["buckets"]
    assert len(buckets) == 1, (
        "this equality is only valid on a single bucket — every seeded line shares "
        f"one day, but {len(buckets)} buckets came back. With more than one, each "
        "bucket is separately quantized and the re-summed series legitimately "
        "drifts from the invoice by up to half a cent per bucket. Compare the "
        "dashboard's PRICING here, not its rounding."
    )
    series_billed = sum((Decimal(b["billed_cost_usd"]) for b in buckets), Decimal(0))
    assert series_billed == invoice

    breakdown = app_client.get("/api/usage/me/breakdown?window=30d", headers=headers)
    assert breakdown.status_code == 200, breakdown.text
    svcs = breakdown.json()["data"]["services"]
    assert len(svcs) == 1, (
        "same single-entry precondition as the series above — every seeded line "
        f"shares one service_key, but {len(svcs)} entries came back."
    )
    breakdown_billed = sum((Decimal(s["billed_cost_usd"]) for s in svcs), Decimal(0))
    assert breakdown_billed == invoice


# --------------------------------------------------------------------------- #
# The half-cent boundary — where two rounding MODES stop agreeing
# --------------------------------------------------------------------------- #
# ⭐ WHY THIS FIXTURE EXISTS AND THE ONES ABOVE DO NOT COVER IT. Every other
# total in this module lands on a whole cent ($46.00, $39.00, $60.00), and on a
# whole cent ROUND_HALF_UP and ROUND_HALF_EVEN are the SAME arithmetic. So
# `test_overview_total_equals_invoice` was green throughout the defect below and
# could not have caught it: a reader and an invoice rounding two different ways
# agree everywhere except the half-cent, which no fixture here ever produced.
#
# ⚠️ IT WAS ALSO HARMLESS UNTIL THIS BRANCH. While `services.markup` quantised
# every LINE, each figure reaching the overview was already a cent multiple and
# the reader's own `.quantize(...)` was a no-op — a bare quantize could not
# round because there was never anything left to round. Removing per-line
# rounding made the reader's input CONTINUOUS, which is what turned a dormant
# omission into a customer-visible disagreement.
#
# $8.23 incurred at 50% is EXACTLY $12.345: a true half-cent, reached by exact
# Decimal arithmetic with no float anywhere, which is the smallest fixture that
# can tell the two modes apart.
HALF_CENT_COST = Decimal("8.230000")
HALF_CENT_PCT = Decimal("50")
HALF_CENT_RAW = Decimal("12.345")  # 8.23 * 1.5 — exact, a true half
HALF_CENT_UP = Decimal("12.35")  # ROUND_HALF_UP  — what the INVOICE charges
HALF_CENT_EVEN = Decimal("12.34")  # ROUND_HALF_EVEN — what a BARE quantize shows


async def _seed_half_cent(session: AsyncSession, label: str) -> str:
    """One tenant, one line, one rule — nothing that could perturb the total.

    Deliberately minimal: no second cost line, no `MeteringDaily` app-request
    volume, no access charge and no seat fee, so the emitted total is exactly
    one markup applied to one incurred figure. Anything else in the fixture
    could move the total off the half-cent and silently disarm the test.
    """
    tenant = T(label)
    session.add(
        TenantConfig(
            tenant_id=tenant,
            tenant_name="half-cent",
            markup_pct=Decimal("0"),
            billing_mode="standard_markup",
        )
    )
    session.add(_rule(tenant_id=tenant, markup_pct=HALF_CENT_PCT))
    row = _cost_row(
        tenant=tenant,
        license_id=uuid.uuid4(),
        subject_id=uuid.uuid4(),
        project_id=NULL_PROJECT_ID,
    )
    row.total_cost_usd = HALF_CENT_COST
    session.add(row)
    await session.commit()
    return tenant


@pytest.mark.unit
async def test_the_half_cent_fixture_really_lands_on_a_half_cent(db_session, test_settings):
    """⭐ NON-VACUITY GUARD for the two tests below.

    An "overview == invoice" assertion proves nothing unless the figure under it
    is one the two rounding modes actually DISAGREE about. Pin, first, that this
    fixture produces a true half and that the two modes give two different cents
    — otherwise the equality below could be green because nothing rounded.
    """
    tenant = await _seed_half_cent(db_session, "halfcent-fixture")

    # The arithmetic is exact: no float, no repeating division.
    assert HALF_CENT_COST * (1 + HALF_CENT_PCT / 100) == HALF_CENT_RAW
    # ...and the two modes genuinely part company on it.
    assert quantize_money(HALF_CENT_RAW) == HALF_CENT_UP
    assert HALF_CENT_RAW.quantize(Decimal("0.01")) == HALF_CENT_EVEN
    assert HALF_CENT_UP != HALF_CENT_EVEN

    # The invoice is the side that was already right; it anchors the comparison.
    assert await _invoice_total(db_session, tenant, test_settings) == HALF_CENT_UP


@pytest.mark.unit
async def test_overview_and_invoice_agree_on_a_half_cent(db_session, test_settings):
    """The per-tenant overview figure must be the cent the tenant is INVOICED.

    ⚠️ MUTATION TARGET: restore the bare `customer.quantize(Decimal("0.01"))` at
    `routers.billing.compute_billing_overview` and this fails with the two
    figures side by side — shown 12.34, charged 12.35.
    """
    tenant = await _seed_half_cent(db_session, "halfcent-overview")
    invoice = await _invoice_total(db_session, tenant, test_settings)

    overview = await compute_billing_overview(_month(), db_session)
    shown = Decimal({t.tenant_id: t for t in overview.tenants}[tenant].customer_cost_usd)

    assert shown == invoice, (
        f"/billing/overview shows {shown} where the invoice charges {invoice}: "
        "the two halves of one statement are rounding a half-cent in opposite "
        "directions. Both must go through services.markup.quantize_money."
    )
    assert shown == HALF_CENT_UP
    assert shown != HALF_CENT_EVEN


@pytest.mark.unit
async def test_platform_total_and_invoice_agree_on_a_half_cent(db_session, test_settings):
    """The SECOND bare quantize — the platform roll-up, not the tenant row.

    Kept as its own test on purpose: the two call sites are independent, and a
    single test covering only the per-tenant figure would stay green while the
    platform total on the same response body disagreed with the same invoice.

    ⚠️ MUTATION TARGET: restore the bare
    `platform_customer.quantize(Decimal("0.01"))`.
    """
    tenant = await _seed_half_cent(db_session, "halfcent-platform")
    invoice = await _invoice_total(db_session, tenant, test_settings)

    overview = await compute_billing_overview(_month(), db_session)
    # Tables are truncated per test, so this tenant is the whole platform — which
    # is what makes the roll-up comparable to a single invoice at all.
    assert len(overview.tenants) == 1, [t.tenant_id for t in overview.tenants]

    platform = Decimal(overview.platform_total_customer_usd)
    assert platform == invoice, (
        f"the platform total shows {platform} where the only invoice on the "
        f"platform charges {invoice}."
    )
    assert platform == HALF_CENT_UP
    assert platform != HALF_CENT_EVEN


# --------------------------------------------------------------------------- #
# The FOLD — `q(B) + q(A) + q(S)` vs `q(B + A + S)`, and `Σq` vs `qΣ`
# --------------------------------------------------------------------------- #
# ⭐ WHY THE HALF-CENT FIXTURE ABOVE COULD NOT SEE EITHER OF THESE. Read
# `_seed_half_cent`'s docstring: "no access charge and no seat fee". Those are
# the exact two terms the invoice fold quantizes INDIVIDUALLY
# (`access_total_charged` / `seat_fee_total_charged` in services/billing.py),
# and a fixture that sets both to zero holds the forking term at the one value
# where every fold agrees. The test was not weak — it was BLIND BY FIXTURE, and
# no strengthening of its assertions could have opened its eyes.
#
# ⚠️ AND `test_platform_total_and_invoice_agree_on_a_half_cent` PINS
# `len(overview.tenants) == 1`. It has to, to compare a platform roll-up with a
# single invoice — but one row is also the exact population in which `Σ q(xᵢ)`
# and `q(Σ xᵢ)` are the SAME arithmetic. It is structurally unable to observe a
# footer that disagrees with the column above it, however many half-cents the
# fixture carries.
#
# Both defects shipped in #207. Neither is reachable from the fixtures that
# existed, so both get their own.

# Fees chosen so that BOTH carry sub-cent scale and BOTH round UP: the two folds
# then differ by a full cent, not by a fraction that could be argued away.
#   q(10.005) + q(20.005) = 10.01 + 20.01 = 30.02   ← the invoice, and the bill
#   q(10.005  +  20.005)  = q(30.010)    = 30.01    ← the unsimplified reader
ACCESS_FEE = Decimal("10.005")
SEAT_RATE = Decimal("10.0025")
SEAT_COUNT = 2  # 10.0025 x 2 = 20.005 — a seat FEE with sub-cent scale
FEE_CHARGED = Decimal("30.02")  # q(A) + q(S) — what the customer is invoiced
FEE_UNSIMPLIFIED = Decimal("30.01")  # q(A + S) — what the portal used to show


class _StubTokenProvider:
    """A service-account token provider that always has a token.

    `services.seats` fails CLOSED with `SeatCountUnavailableError` BEFORE the
    HTTP call when no `Authorization` header can be built, so stubbing the GET
    alone would leave the seat fee at $0 — and a $0 seat fee is precisely the
    blind fixture this section exists to escape. The failure would look like a
    passing test.
    """

    async def get_token(self) -> str:
        return "test-service-token"  # pragma: allowlist secret

    def build_headers(self, tenant_id: str) -> dict[str, str]:
        return {  # pragma: allowlist secret
            "X-Tenant-Id": tenant_id,
            "Authorization": "Bearer test-service-token",  # pragma: allowlist secret
        }


@pytest.fixture
def licences_says_two_seats(monkeypatch):
    """Stub the licences seat-count endpoint at `SEAT_COUNT`.

    ⚠️ URL-SCOPED AND DELEGATING, never a blanket stub — same shape and same
    reason as `conftest._licences_answers_the_tenant_registry`, which is autouse
    and therefore installed FIRST: this wrapper captures that one as `real_get`
    and hands every non-seat URL on to it. A blanket stub would feed a seat-count
    object to `generate_billing`'s tenant-registry lookup and surface as an
    unrelated failure deep inside the pricer.

    ⚠️ The seat cache is module state with a 5-minute TTL, and its key includes
    `as_of` — the invoice asks as of the last instant of the month, the overview
    asks with `as_of=None`. Two entries, two HTTP calls, both served here.
    Cleared on both sides so no ordering can decide another test's answer.
    """
    from services import seats as seats_svc

    seats_svc.clear_seat_cache()
    monkeypatch.setattr(seats_svc, "_provider_for", lambda _s: _StubTokenProvider())

    real_get = httpx.AsyncClient.get

    async def _get(self, url, *a, **kw):  # noqa: ANN001, ANN202
        if "seat-count" in str(url):
            return httpx.Response(
                200,
                json={"tenant_id": "", "seats": SEAT_COUNT, "basis": "active_licences"},
            )
        return await real_get(self, url, *a, **kw)

    monkeypatch.setattr(httpx.AsyncClient, "get", _get)
    yield
    seats_svc.clear_seat_cache()


async def _seed_sub_cent_fees(session: AsyncSession, label: str) -> str:
    """One tenant whose ONLY revenue is a sub-cent access fee and a sub-cent seat fee.

    ⭐ NO COST ROWS ON PURPOSE. With the billed usage stream at exactly zero the
    emitted total IS the fold of the two fees and nothing else, so a disagreement
    between two readers can only be the fold. Mixing in usage would still expose
    the defect but would leave the failure message ambiguous about which term
    rounded.

    ⚠️ TWO ROWS, TWO DIFFERENT `service_key`s. `ix_accesscharge_open` is UNIQUE on
    `(tenant_id, coalesce(service_key,''))` for open rows, so a tenant-wide flat
    row and a tenant-wide per-seat row cannot coexist. `_applies` does not filter
    on `service_key` at all, so both are billed regardless — the key is here to
    satisfy the index, not to scope the charge.
    """
    tenant = T(label)
    session.add(
        TenantConfig(
            tenant_id=tenant,
            tenant_name="sub-cent-fees",
            markup_pct=Decimal("0"),
            billing_mode="standard_markup",
        )
    )
    session.add(
        TenantAccessCharge(
            tenant_id=tenant,
            service_key=None,
            amount_usd=ACCESS_FEE,
            cadence="monthly",
            effective_from=PAST,
        )
    )
    session.add(
        TenantAccessCharge(
            tenant_id=tenant,
            service_key="seats",  # pragma: allowlist secret
            amount_usd=SEAT_RATE,
            cadence="per_seat",
            effective_from=PAST,
        )
    )
    await session.commit()
    return tenant


@pytest.mark.unit
async def test_the_sub_cent_fee_fixture_really_forks_the_two_folds(
    db_session, test_settings, licences_says_two_seats
):
    """⭐ NON-VACUITY GUARD for the test below, and for the fixture's REACH.

    Three separate ways this fixture could be green-because-nothing-happened,
    each pinned here rather than assumed:

    1. The two folds must actually give different cents on these inputs — if
       they agreed, the equality below would prove nothing.
    2. The seat stub must have been REACHED. A `SeatCountUnavailableError` is
       swallowed by both readers (deliberately — see `_resolve_seat_count`), so
       a broken stub yields a $0 seat fee and a fixture that silently reverts to
       "no seat fee", the exact blindness this section exists to escape.
    3. The invoice — the reference implementation — must charge the quantized
       sum, because every assertion below is anchored to it.
    """
    tenant = await _seed_sub_cent_fees(db_session, "subcent-fixture")

    # 1. The two folds genuinely part company on these two fees.
    assert quantize_money(ACCESS_FEE) + quantize_money(SEAT_RATE * SEAT_COUNT) == FEE_CHARGED
    assert quantize_money(ACCESS_FEE + SEAT_RATE * SEAT_COUNT) == FEE_UNSIMPLIFIED
    assert FEE_CHARGED != FEE_UNSIMPLIFIED

    result = await generate_billing(tenant, _month(), db_session, test_settings)

    # 2. The seat lookup succeeded and the seat fee is a real, non-zero charge.
    assert Decimal(result["totals"]["seat_fee_usd"]) == quantize_money(SEAT_RATE * SEAT_COUNT)
    assert Decimal(result["totals"]["seat_fee_usd"]) > 0, (
        "the seat stub was not reached: a swallowed SeatCountUnavailableError "
        "silently disarms this whole fixture back to 'no seat fee'"
    )
    assert Decimal(result["totals"]["access_charge_usd"]) == quantize_money(ACCESS_FEE)

    # 3. The invoice charges q(A) + q(S), and that is the customer's bill.
    assert Decimal(result["totals"]["customer_cost_usd"]) == FEE_CHARGED


@pytest.mark.unit
async def test_overview_and_invoice_agree_when_fees_carry_sub_cent_scale(
    db_session, test_settings, licences_says_two_seats
):
    """The portal must show the cent the customer is INVOICED, fees included.

    ⚠️ MUTATION TARGET: in `routers.billing.compute_billing_overview`, drop the
    two `quantize_money(...)` calls wrapping `access_by_tenant` /
    `seat_fee_by_tenant` — i.e. restore `customer += access_by_tenant.get(...)`.
    This then fails with `shown 30.01` against `invoiced 30.02`.
    """
    tenant = await _seed_sub_cent_fees(db_session, "subcent-overview")
    invoice = await _invoice_total(db_session, tenant, test_settings)

    overview = await compute_billing_overview(_month(), db_session)
    shown = Decimal({t.tenant_id: t for t in overview.tenants}[tenant].customer_cost_usd)

    assert shown == invoice, (
        f"/billing/overview shows {shown} where the invoice charges {invoice}: "
        f"the portal is folding the RAW access and seat fees ({ACCESS_FEE} + "
        f"{SEAT_RATE * SEAT_COUNT}) into the total, while the invoice folds the "
        "CENTS THEY ARE CHARGED AS. Quantize each fee, then sum."
    )
    assert shown == FEE_CHARGED
    assert shown != FEE_UNSIMPLIFIED


@pytest.mark.unit
async def test_overview_rows_sum_to_the_platform_total_across_two_tenants(
    db_session, test_settings
):
    """The footer must be the sum of the COLUMN, on a platform with >1 tenant.

    ⭐ WHY TWO TENANTS IS THE WHOLE POINT. `Σ q(xᵢ)` and `q(Σ xᵢ)` are the same
    arithmetic for one row, so the existing platform test — which pins
    `len(overview.tenants) == 1` in order to compare against a single invoice —
    cannot express this property at all, let alone fail on it.

    ⚠️ MUTATION TARGET: in `compute_billing_overview`, restore
    `platform_customer += customer` (the RAW figure) in place of
    `+= customer_charged`. The rows then render 12.35 + 12.35 = 24.70 under a
    platform total of q(12.345 + 12.345) = q(24.690) = 24.69.
    """
    t1 = await _seed_half_cent(db_session, "halfcent-twoup-a")
    t2 = await _seed_half_cent(db_session, "halfcent-twoup-b")
    invoices = {
        t1: await _invoice_total(db_session, t1, test_settings),
        t2: await _invoice_total(db_session, t2, test_settings),
    }

    overview = await compute_billing_overview(_month(), db_session)
    rows = {t.tenant_id: t for t in overview.tenants}
    # Non-vacuity: the drift is only expressible with two rows that each carry a
    # half-cent residual, so prove the population before reading the footer.
    assert set(rows) == {t1, t2}, sorted(rows)
    assert invoices[t1] == invoices[t2] == HALF_CENT_UP

    rendered = [Decimal(rows[t].customer_cost_usd) for t in (t1, t2)]
    platform = Decimal(overview.platform_total_customer_usd)

    assert platform == sum(rendered), (
        f"the platform total is {platform} but the rows above it render "
        f"{' + '.join(str(r) for r in rendered)} = {sum(rendered)}: the footer is "
        "a second rounding of the raw pre-images (qΣ) while every row is a "
        "rounding of its own figure (Σq). A table must add up to itself."
    )
    # ...and the footer still reconciles to the money actually invoiced.
    assert platform == sum(invoices.values()) == Decimal("24.70")


@pytest.mark.unit
async def test_admin_rows_sum_to_the_grand_total_across_two_tenants(db_session, test_settings):
    """`services.admin_aggregate.system_usage` — the same fold, its own call site.

    Kept separate from the overview test on purpose: the two roll-ups are
    independent implementations of one rule, and a single test covering only one
    of them would stay green while the other admin surface disagreed with itself.

    ⚠️ MUTATION TARGET: in `admin_aggregate.system_usage`, restore
    `grand_billed += billed` (the RAW figure) in place of `+= billed_charged`.
    """
    t1 = await _seed_half_cent(db_session, "halfcent-admin-a")
    t2 = await _seed_half_cent(db_session, "halfcent-admin-b")
    invoices = [
        await _invoice_total(db_session, t1, test_settings),
        await _invoice_total(db_session, t2, test_settings),
    ]

    system = await system_usage(db_session, test_settings, "today")
    rows = {t.tenant_id: t for t in system.by_tenant}
    assert set(rows) == {t1, t2}, sorted(rows)
    assert invoices == [HALF_CENT_UP, HALF_CENT_UP]

    rendered = [Decimal(rows[t].billed_cost_usd) for t in (t1, t2)]
    grand = Decimal(system.grand_total.billed_cost_usd)

    assert grand == sum(rendered), (
        f"the admin grand total is {grand} but its own by-tenant rows render "
        f"{' + '.join(str(r) for r in rendered)} = {sum(rendered)}"
    )
    assert grand == sum(invoices) == Decimal("24.70")


# --------------------------------------------------------------------------- #
# The INTERNAL column — the same two folds, on the other half of the row
# --------------------------------------------------------------------------- #
# ⭐ WHY NOTHING ABOVE COVERS THIS, MEASURED RATHER THAN ASSUMED. Every fixture
# in this module so far is aimed at `customer_cost_usd`, and `_seed_half_cent`
# reaches its half-cent by applying a 50% markup to a WHOLE-CENT incurred figure
# ($8.23). `internal_cost_usd` carries no markup — it is the raw
# `sum(total_cost_usd)` — so on that fixture the internal column is exactly
# $8.23 and a quantize of it cannot round. Every internal figure this file has
# ever produced was already a cent multiple, which is precisely the state in
# which `q(x) == x`, `Σq == qΣ`, and any omission is dormant.
#
# ⛔ AND THE DASHBOARD SUITE CANNOT SEE IT EITHER, for a second and independent
# reason: `tests/test_dashboard.py` patches `routers.billing.
# compute_billing_overview` with a canned object, so it never executes this
# function at all — and `dashboard.py` quantizes again downstream, masking it a
# second time. Measured: reverting `internal_cost_usd=internal_charged` back to
# `=internal` left all 58 tests in that module green. The mutant SURVIVED.
#
# ⚠️ This is not a dashboard-only surface. `GET /billing/overview`
# (response_model `Envelope[BillingOverview]`) serves `internal_cost_usd` and
# `platform_total_internal_usd` as JSON to callers who never render the HTML
# page, so the raw `Numeric(20,6)` pre-image reached the wire unrounded.
#
# THE FIXTURE PROPERTY THAT MAKES THESE TESTS ABLE TO FAIL: the seeded
# `total_cost_usd` is a TRUE HALF-CENT at the internal grain — $12.345000,
# stored exactly in a `Numeric(20,6)` column, with no markup, no allowance, no
# access fee and no seat fee between it and the emitted figure. A whole-cent
# amount here would make every assertion below green on the mutant as well,
# because nothing would round; that is exactly the blindness described above,
# and `test_whole_cent_internal_cost_is_unmoved_by_the_quantize` below pins the
# whole-cent case SEPARATELY as a control rather than letting it masquerade as
# coverage.
#
# ⚠️ ROUND_HALF_UP, NOT BANKER'S. `services.markup.quantize_money` is explicitly
# ROUND_HALF_UP (see MONEY_ROUNDING); the expected cents below are computed from
# THAT mode. Under ROUND_HALF_EVEN 12.345 would be 12.34 and every figure here
# would be a cent lower — which is what `SUB_CENT_INTERNAL_EVEN` pins, so a
# silent switch to the context default reds these tests instead of shifting them.
SUB_CENT_INTERNAL_COST = Decimal("12.345000")  # a TRUE half-cent, exactly stored
# A non-zero markup on purpose: it forces `customer_cost_usd` (18.52) away from
# `internal_cost_usd` (12.35), so a test that read the WRONG COLUMN would fail
# rather than pass on a coincidence. At 0% the two columns would be identical.
SUB_CENT_INTERNAL_PCT = Decimal("50")
SUB_CENT_INTERNAL_UP = Decimal("12.35")  # q(12.345) HALF_UP — what must be shown
SUB_CENT_INTERNAL_EVEN = Decimal("12.34")  # what banker's rounding would show
SUB_CENT_CUSTOMER_UP = Decimal("18.52")  # q(12.345 * 1.5) = q(18.5175) — not a half
# Two tenants, each at the same half-cent:
INTERNAL_SUM_OF_SHOWN = Decimal("24.70")  # Σ q(12.345) — the column adds to this
INTERNAL_Q_OF_SUM = Decimal("24.69")  # q(12.345 + 12.345) = q(24.690)

# The CONTROL amount. A whole cent, so `q(x) == x` and `Σq == qΣ` both hold and
# the quantize provably moves nothing.
WHOLE_CENT_INTERNAL_COST = Decimal("10.000000")
WHOLE_CENT_INTERNAL = Decimal("10.00")

# --- The UNDER-A-CENT amount — the only state `internal_cost_exact_usd` exists for
#
# ⚠️ NOT THE SAME THING AS `SUB_CENT_INTERNAL_COST` ABOVE, and the difference is
# the whole feature. $12.345000 carries sub-cent SCALE but quantizes to a
# NON-ZERO cent ($12.35), so `internal_cost_usd > 0`, the margin percentage has
# a denominator, and the renderer's sub-cent branch is never taken. Only an
# amount UNDER half a cent makes `q(x) == 0.00` while `x > 0` — the one state in
# which a tenant with real spend and a tenant with NO spend render the identical
# `$0.00` column and the exact pre-image is the only thing that tells them apart.
UNDER_CENT_INTERNAL_COST = Decimal("0.004000")  # positive, and under half a cent
UNDER_CENT_INTERNAL_SHOWN = Decimal("0.00")  # q(0.004) HALF_UP — the column rounds it away
UNDER_CENT_CUSTOMER_UP = Decimal("0.01")  # q(0.004 × 1.5) = q(0.006) — the 50% rule, HALF_UP


async def _seed_internal(
    session: AsyncSession, label: str, *, cost: Decimal = SUB_CENT_INTERNAL_COST
) -> str:
    """One tenant, one cost line, one rule — the same minimal shape as
    `_seed_half_cent`, with the incurred amount PARAMETERISED.

    ⭐ THE AMOUNT IS THE ONLY DIFFERENCE BETWEEN THE TESTS AND THEIR CONTROL.
    The sub-cent tests and `test_whole_cent_internal_cost_is_unmoved_by_the_
    quantize` run through this identical seeder and differ in nothing but
    `cost`, so a green control cannot be explained by a different fixture path —
    only by the amount already being a whole cent.

    Deliberately carries NO access charge, NO seat fee, NO allowance and NO
    `MeteringDaily` rows: `internal_cost_usd` is the raw `sum(total_cost_usd)`,
    and any of those terms could move the emitted figure off the half-cent and
    silently disarm the fixture. `_cost_row` supplies a real (non-sentinel)
    `license_id`, which `CostDaily.license_attributed_clause` requires — a
    sentinel licence would be dropped from the overview and the internal column
    would read $0.00, i.e. green-because-empty.
    """
    tenant = T(label)
    session.add(
        TenantConfig(
            tenant_id=tenant,
            tenant_name="sub-cent-internal",
            markup_pct=Decimal("0"),
            billing_mode="standard_markup",
        )
    )
    session.add(_rule(tenant_id=tenant, markup_pct=SUB_CENT_INTERNAL_PCT))
    row = _cost_row(
        tenant=tenant,
        license_id=uuid.uuid4(),
        subject_id=uuid.uuid4(),
        project_id=NULL_PROJECT_ID,
    )
    row.total_cost_usd = cost
    session.add(row)
    await session.commit()
    return tenant


@pytest.mark.unit
async def test_the_internal_fixture_really_lands_on_a_sub_cent(db_session, test_settings):
    """⭐ NON-VACUITY GUARD for the two internal tests below.

    Three ways those tests could be green-because-nothing-happened, each pinned
    here rather than assumed:

    1. The seeded internal figure must actually carry SUB-CENT SCALE. If it
       landed on a whole cent, `quantize_money` would be a no-op and the mutants
       below would all survive — the exact dormancy this section exists to end.
    2. The two rounding MODES must disagree on it, so a silent fall back to the
       context default (ROUND_HALF_EVEN) is a failure here rather than a
       one-cent shift in everything downstream.
    3. The internal and customer columns must be DIFFERENT numbers, so a test
       below that read the wrong attribute off the entry would fail.

    This test asserts properties of the fixture and of `quantize_money`, not of
    the reader, so it is expected to stay green under every mutation named
    below — that is what makes it a usable precondition for them.
    """
    tenant = await _seed_internal(db_session, "internal-fixture")

    # 1. Exact Decimal arithmetic: no float, no repeating division.
    assert SUB_CENT_INTERNAL_COST.as_tuple().exponent == -6
    assert SUB_CENT_INTERNAL_COST != SUB_CENT_INTERNAL_UP, (
        "the seeded amount is already a whole cent: nothing can round, and every "
        "assertion below would be vacuous"
    )
    # 2. The two modes genuinely part company on it.
    assert quantize_money(SUB_CENT_INTERNAL_COST) == SUB_CENT_INTERNAL_UP
    assert SUB_CENT_INTERNAL_COST.quantize(Decimal("0.01")) == SUB_CENT_INTERNAL_EVEN
    assert SUB_CENT_INTERNAL_UP != SUB_CENT_INTERNAL_EVEN
    # ...and the `Σq` vs `qΣ` fork is a whole cent on two of them.
    assert SUB_CENT_INTERNAL_UP + SUB_CENT_INTERNAL_UP == INTERNAL_SUM_OF_SHOWN
    assert quantize_money(SUB_CENT_INTERNAL_COST * 2) == INTERNAL_Q_OF_SUM
    assert INTERNAL_SUM_OF_SHOWN != INTERNAL_Q_OF_SUM

    # 3. The invoice — the reference implementation — charges the quantized cent,
    #    and the markup keeps the two columns apart.
    assert await _invoice_internal(db_session, tenant, test_settings) == SUB_CENT_INTERNAL_UP
    assert await _invoice_total(db_session, tenant, test_settings) == SUB_CENT_CUSTOMER_UP
    assert SUB_CENT_INTERNAL_UP != SUB_CENT_CUSTOMER_UP


@pytest.mark.unit
async def test_overview_internal_cost_is_the_quantized_cent(db_session, test_settings):
    """`BillingOverviewEntry.internal_cost_usd` must be a CENT, not the raw sum.

    The entry is a wire model (`GET /billing/overview` serves it as JSON, and
    `internal_cost_usd: Decimal` carries no `decimal_places` constraint and no
    serializer), so whatever this function puts on it is what a caller receives.
    Leaving it raw put a `Numeric(20,6)` pre-image on the wire beside a
    customer column already in cents, and made every consumer's
    `customer − internal` margin a figure that the row's own two columns do not
    reproduce.

    ⚠️ MUTATION TARGET: in `routers.billing.compute_billing_overview`, restore
    `internal_cost_usd=internal` in place of `=internal_charged`. The entry then
    carries 12.345000 — three significant decimal places — where the invoice's
    internal column charges 12.35.

    ⚠️ ONLY FAILS BECAUSE THE FIXTURE IS SUB-CENT: `_seed_internal` seeds
    $12.345000 of incurred LLM cost, and `internal_cost_usd` applies no markup,
    so the emitted figure is that half-cent unmodified. On a whole-cent amount
    the mutant and the fix are the same number (see the control below).
    """
    tenant = await _seed_internal(db_session, "internal-overview")
    invoice_internal = await _invoice_internal(db_session, tenant, test_settings)

    overview = await compute_billing_overview(_month(), db_session)
    entry = {t.tenant_id: t for t in overview.tenants}[tenant]
    shown = Decimal(entry.internal_cost_usd)

    exponent = shown.as_tuple().exponent
    assert isinstance(exponent, int), f"{shown} is not a finite decimal"
    assert exponent >= -2, (
        f"/billing/overview put {shown} in `internal_cost_usd` — the raw "
        "Numeric(20,6) sum, not a cent. A displayed money figure goes through "
        "services.markup.quantize_money exactly once, like customer_cost_usd "
        "beside it."
    )
    assert shown == invoice_internal, (
        f"the overview shows internal {shown} where the invoice's internal "
        f"column carries {invoice_internal}"
    )
    assert shown == SUB_CENT_INTERNAL_UP
    assert shown != SUB_CENT_INTERNAL_EVEN  # not banker's rounding
    assert shown != SUB_CENT_INTERNAL_COST  # not the raw pre-image
    # Read the right column: the markup keeps customer well away from internal,
    # so this cannot be passing on a coincidence between the two.
    assert Decimal(entry.customer_cost_usd) == SUB_CENT_CUSTOMER_UP


@pytest.mark.unit
async def test_internal_rows_sum_to_the_platform_internal_total_across_two_tenants(
    db_session, test_settings
):
    """The internal FOOTER must be the sum of the internal COLUMN — `Σq`, not `qΣ`.

    ⭐ TWO TENANTS IS THE MINIMUM THAT CAN EXPRESS THIS. `Σ q(xᵢ)` and
    `q(Σ xᵢ)` are the same arithmetic on a single row, so no one-tenant fixture
    — including every internal-column assertion above — can distinguish the two
    folds, let alone fail on them.

    ⚠️ MUTATION TARGET: in `compute_billing_overview`, restore
    `platform_internal += internal` (the RAW figure) in place of
    `+= internal_charged`. The rows then render 12.35 + 12.35 = 24.70 under a
    platform total of q(12.345 + 12.345) = q(24.690) = 24.69 — a footer a cent
    below its own column.

    ⚠️ THE LITERALS ARE LOAD-BEARING AGAINST A COMBINED REVERT. Reverting BOTH
    internal call sites at once (`internal_cost_usd=internal` as well) would
    make `platform == sum(rendered)` true again at 24.690000 == 24.690000, since
    Decimal equality ignores the exponent. The `== INTERNAL_SUM_OF_SHOWN` and
    per-row `== SUB_CENT_INTERNAL_UP` anchors below are what still red it.
    """
    t1 = await _seed_internal(db_session, "internal-twoup-a")
    t2 = await _seed_internal(db_session, "internal-twoup-b")
    invoices = {
        t1: await _invoice_internal(db_session, t1, test_settings),
        t2: await _invoice_internal(db_session, t2, test_settings),
    }

    overview = await compute_billing_overview(_month(), db_session)
    rows = {t.tenant_id: t for t in overview.tenants}
    # Non-vacuity: the drift is only expressible with two rows that each carry a
    # sub-cent residual, so prove the population before reading the footer.
    assert set(rows) == {t1, t2}, sorted(rows)
    assert invoices[t1] == invoices[t2] == SUB_CENT_INTERNAL_UP

    rendered = [Decimal(rows[t].internal_cost_usd) for t in (t1, t2)]
    platform = Decimal(overview.platform_total_internal_usd)

    assert rendered == [SUB_CENT_INTERNAL_UP, SUB_CENT_INTERNAL_UP], (
        f"the internal column renders {rendered}, not two quantized cents — the "
        "per-row leaf is raw, so the footer comparison below would be comparing "
        "two raw figures and could not fail"
    )
    assert platform == sum(rendered), (
        f"platform_total_internal_usd is {platform} but the internal column "
        f"above it renders {' + '.join(str(r) for r in rendered)} = "
        f"{sum(rendered)}: the footer is a second rounding of the raw pre-images "
        "(qΣ) while every row is a rounding of its own figure (Σq). A table must "
        "add up to itself."
    )
    assert platform == INTERNAL_SUM_OF_SHOWN
    assert platform != INTERNAL_Q_OF_SUM
    # ...and the footer still reconciles to the internal cost actually invoiced.
    assert platform == sum(invoices.values())


@pytest.mark.unit
async def test_whole_cent_internal_cost_is_unmoved_by_the_quantize(db_session, test_settings):
    """⭐ THE CONTROL: on whole-cent input the change moves NOTHING.

    The three tests above prove the quantize acts. This one proves it acts only
    where it should — that a tenant whose internal cost was ALREADY correct sees
    the same money before and after, at the row and at the footer. Without it,
    "the figure changed" is evidence of a quantize, not of a CORRECT quantize: a
    truncation, a floor, or a rounding to the wrong exponent would satisfy every
    assertion above and would be caught only here.

    ⚠️ CORRECTED 2026-09-15 — THIS DOCSTRING USED TO CLAIM THE TEST IS
    "DELIBERATELY GREEN ON BOTH SIDES OF THE MUTATION". IT IS NOT, AND IT NEVER
    WAS. Measured: restore `internal_cost_usd=internal` / `platform_internal +=
    internal` in `routers.billing.compute_billing_overview` and this test FAILS,
    at `assert shown.as_tuple().exponent >= -2` below, with `assert -6 >= -2` —
    the raw figure arrives at `Numeric(20, 6)` scale. The prose and the
    assertions were describing different tests, and the prose is what was wrong.
    That matters in the dangerous direction: a reader told "this one cannot fail
    for the mutation" would have read a genuine red here as an unrelated break.

    ⭐ WHAT IT IS, STATED SO THE TWO AGREE. It is a control ON THE MONEY and a
    killer ON THE FORM, and those are separate claims because the assertions are
    separate. Every VALUE assertion (`shown == WHOLE_CENT_INTERNAL_COST`,
    `== invoices[tid]`, `platform == sum(rendered)`) is deliberately inert under
    that mutation — `q(10.000000) == 10.000000` and `Σq == qΣ` on whole cents —
    which is exactly what makes them a control: they kill any change that MOVES
    an already-correct figure (a truncating or floor/ceiling quantize, a
    quantize to the wrong exponent, a double-accumulation of the internal leaf)
    and nothing else. The EXPONENT assertion is the deliberate exception: the
    display exponent is normalised from `Numeric(20, 6)` scale to two places
    (`Decimal("10.000000")` → `Decimal("10.00")`), which is the whole purpose of
    the change, so on whole-cent input the FORM is the only observable the
    quantize has left — and a test that let a raw six-place figure through here
    would be asserting the money and ignoring the change.

    ⛔ Do NOT "restore the control" by deleting the exponent assertion. It is the
    only place in this file that pins the rendered scale on the whole-cent path,
    and weakening an assertion to make a sentence true is how a test stops being
    able to fail. The sentence was fixed instead.

    Runs through the SAME `_seed_internal` helper as the tests above, differing
    in nothing but the seeded amount, so a green here cannot be a different
    code path.
    """
    t1 = await _seed_internal(db_session, "internal-control-a", cost=WHOLE_CENT_INTERNAL_COST)
    t2 = await _seed_internal(db_session, "internal-control-b", cost=WHOLE_CENT_INTERNAL_COST)
    invoices = {
        t1: await _invoice_internal(db_session, t1, test_settings),
        t2: await _invoice_internal(db_session, t2, test_settings),
    }

    # The precondition that makes this a control and not a duplicate of the
    # tests above: on THIS amount the quantize is provably inert, both folds
    # agree, and both rounding modes agree.
    assert quantize_money(WHOLE_CENT_INTERNAL_COST) == WHOLE_CENT_INTERNAL_COST
    assert WHOLE_CENT_INTERNAL_COST.quantize(Decimal("0.01")) == WHOLE_CENT_INTERNAL
    assert quantize_money(WHOLE_CENT_INTERNAL_COST) * 2 == quantize_money(
        WHOLE_CENT_INTERNAL_COST * 2
    )

    overview = await compute_billing_overview(_month(), db_session)
    rows = {t.tenant_id: t for t in overview.tenants}
    assert set(rows) == {t1, t2}, sorted(rows)

    rendered = [Decimal(rows[t].internal_cost_usd) for t in (t1, t2)]
    platform = Decimal(overview.platform_total_internal_usd)

    # The money is unchanged: every figure equals the raw pre-image it came from
    # AND equals the cent the invoice carries.
    for tid, shown in zip((t1, t2), rendered, strict=True):
        assert shown == WHOLE_CENT_INTERNAL_COST, (
            f"{tid} incurred exactly {WHOLE_CENT_INTERNAL_COST} and the overview "
            f"now reports {shown}: the quantize moved a figure that was already "
            "a whole cent, so it is not rounding — it is altering."
        )
        assert shown == WHOLE_CENT_INTERNAL
        assert shown == invoices[tid]
        exponent = shown.as_tuple().exponent
        assert isinstance(exponent, int), f"{shown} is not a finite decimal"
        assert exponent >= -2

    assert platform == sum(rendered) == WHOLE_CENT_INTERNAL * 2
    assert platform == WHOLE_CENT_INTERNAL_COST * 2  # equal to the raw sum, exactly
    assert platform == sum(invoices.values())


# --------------------------------------------------------------------------- #
# The EXACT PRE-IMAGE — the field the quantize above makes necessary
# --------------------------------------------------------------------------- #
# ⭐ WHY THIS NEEDS ITS OWN SECTION, AND WHY THE TESTS ABOVE CANNOT STAND IN FOR
# IT. Quantising `internal_cost_usd` is what maps every tenant-month under half
# a cent onto the same `$0.00` as a tenant-month with no spend at all, so
# `BillingOverviewEntry.internal_cost_exact_usd` was added to keep those two
# facts apart. `routers/billing.py`'s `internal_cost_exact_usd=internal` is the
# ONLY line that populates it in production; the field carries a
# `Decimal("0")` default, so deleting that line is silent — `internal_sub_cent`
# in `routers/dashboard.py` becomes permanently False and `n/a (cost < $0.01)`
# can never render for anyone.
#
# ⛔ AND THE DASHBOARD SUITE IS DOUBLE-BLIND TO IT, for exactly the two reasons
# the internal-quantize section above documents, one of which is worse here.
# `tests/test_dashboard.py::test_sub_cent_internal_cost_gets_its_own_marker_and_
# keeps_its_figure` constructs `BillingOverviewEntry(internal_cost_exact_usd=…)`
# BY HAND inside `_overview_payload` and renders it behind
# `patch("routers.billing.compute_billing_overview", …)`. It grades the TEMPLATE
# against a value the test itself supplied and NEVER EXECUTES the producing
# line. Measured: deleting `internal_cost_exact_usd=internal,` from
# `compute_billing_overview` left 391 tests green across all nine
# overview-touching suites. The mutant SURVIVED — the identical double-masking
# hole this file already closed for `internal_cost_usd`, never applied to the
# new field beside it.
#
# THE FIXTURE PROPERTY: `UNDER_CENT_INTERNAL_COST`. Every other internal fixture
# in this file quantizes to a non-zero cent, where `internal_cost_usd > 0` and
# the feature's whole branch is unreachable.


@pytest.mark.unit
async def test_the_under_cent_fixture_really_rounds_away_to_nothing(db_session, test_settings):
    """⭐ NON-VACUITY GUARD for the exact-pre-image tests below.

    Three ways those could be green-because-nothing-happened, each pinned here:

    1. The seeded amount must round to `$0.00` — not merely carry sub-cent
       scale. At `SUB_CENT_INTERNAL_COST` ($12.345 → $12.35) the displayed
       column is non-zero, the margin percentage has a denominator, and the
       sub-cent state the pre-image exists for is never entered.
    2. It must nonetheless be POSITIVE, so "rounds to zero" and "is zero" are
       genuinely different facts about this tenant-month.
    3. The invoice — the reference implementation — must also charge `$0.00`
       internal, so a reader showing `$0.00` is CORRECT and the pre-image is
       carrying information the correct figure destroys, not patching a bug.

    Asserts properties of the fixture and of `quantize_money`, never of the
    reader, so it stays green under the mutations named below — which is what
    makes it a usable precondition for them.
    """
    tenant = await _seed_internal(db_session, "under-cent-fixture", cost=UNDER_CENT_INTERNAL_COST)

    # 1 + 2. Positive, and rounds away.
    assert UNDER_CENT_INTERNAL_COST > 0
    assert quantize_money(UNDER_CENT_INTERNAL_COST) == UNDER_CENT_INTERNAL_SHOWN == 0
    assert UNDER_CENT_INTERNAL_COST != UNDER_CENT_INTERNAL_SHOWN, (
        "the seeded amount survives the quantize: `internal_cost_usd` is non-zero "
        "and the sub-cent branch under test is unreachable on this fixture"
    )
    # ...and it is a DIFFERENT state from the sub-cent-scale fixture above, which
    # rounds to a real cent. Both are 'sub-cent'; only one renders $0.00.
    assert quantize_money(SUB_CENT_INTERNAL_COST) > 0

    # 3. The invoice agrees the internal column is $0.00, and the customer column
    #    is a different, non-zero number — so a test reading the wrong attribute
    #    off the entry cannot pass by coincidence.
    assert await _invoice_internal(db_session, tenant, test_settings) == UNDER_CENT_INTERNAL_SHOWN
    assert await _invoice_total(db_session, tenant, test_settings) == UNDER_CENT_CUSTOMER_UP
    assert UNDER_CENT_CUSTOMER_UP > 0


@pytest.mark.unit
async def test_overview_carries_the_exact_internal_pre_image_when_it_rounds_to_zero(
    db_session, test_settings
):
    """`internal_cost_exact_usd` must carry the REAL cost, not the field default.

    ⚠️ MUTATION TARGET, AND THE REASON THIS TEST EXISTS: delete
    `internal_cost_exact_usd=internal,` from the `BillingOverviewEntry(...)`
    construction in `routers.billing.compute_billing_overview`. That is the sole
    production writer of the field; without it every entry falls back to the
    model default `Decimal("0")`, `routers.dashboard`'s
    `sub_cent_internal = internal_charged == 0 and internal_exact > 0` is
    permanently False, and the `n/a (cost < $0.01)` marker can never render for
    any tenant on any month — while 391 tests stay green.

    ⚠️ ONLY FAILS BECAUSE THE FIXTURE ROUNDS TO ZERO. On any amount that
    quantizes to a non-zero cent the marker is not reached at all, so the field
    could default to 0 forever and nothing would look different; that is exactly
    the blindness the section comment above measures.

    ⛔ It is asserted as the PRE-IMAGE, not merely as non-zero: a mutation
    writing `internal_cost_exact_usd=internal_charged` (the cent) is just as
    destructive — it re-rounds the figure to 0.00 and erases the same fact — and
    a `> 0` assertion alone would let it through.
    """
    tenant = await _seed_internal(db_session, "under-cent-overview", cost=UNDER_CENT_INTERNAL_COST)

    overview = await compute_billing_overview(_month(), db_session)
    entry = {t.tenant_id: t for t in overview.tenants}[tenant]
    shown = Decimal(entry.internal_cost_usd)
    exact = Decimal(entry.internal_cost_exact_usd)

    # The displayed column is correct AND uninformative — the precondition that
    # makes the pre-image load-bearing rather than redundant.
    assert shown == UNDER_CENT_INTERNAL_SHOWN, (
        f"the internal column renders {shown}, not $0.00: this tenant-month does "
        "not round away, so the pre-image below is not the only surviving trace "
        "of its spend and this test is not measuring the feature"
    )

    # ⭐ THE KILLER.
    assert exact == UNDER_CENT_INTERNAL_COST, (
        f"internal_cost_exact_usd is {exact} where this tenant incurred "
        f"{UNDER_CENT_INTERNAL_COST}. If it is 0, `internal_cost_exact_usd="
        "internal` is not populating the entry and the field is its own default: "
        "/dashboard/ then reports 'No internal cost this month' — a false "
        "statement — for every tenant whose spend is under a cent."
    )
    assert exact > 0
    assert exact != shown  # the pre-image is not the cent, and not a re-round

    # The renderer's own predicate, evaluated here so the field's ONLY purpose is
    # pinned by the test that drives the real producer.
    assert (shown == 0 and exact > 0) is True, (
        "routers.dashboard's `sub_cent_internal` is False on a tenant-month that "
        "IS positive and sub-cent: the third state cannot render"
    )
    # Read the right column: the markup keeps customer away from internal.
    assert Decimal(entry.customer_cost_usd) == UNDER_CENT_CUSTOMER_UP


@pytest.mark.unit
async def test_whole_cent_internal_pre_image_equals_the_cent_and_raises_no_marker(
    db_session, test_settings
):
    """⭐ THE CONTROL: on whole-cent input the pre-image says nothing new.

    The test above proves the field carries information the column destroys.
    This one proves it carries the SAME money the column shows when nothing was
    destroyed — so the field is the raw internal cost and not some third figure,
    and the marker it feeds does NOT fire on a tenant whose cost is fully
    displayed. Without it, "the field is non-zero" would be evidence of a
    populated field, not of a CORRECTLY populated one: writing the customer
    cost, the unlicensed cost, or a hard-coded constant into it would satisfy
    every assertion above and be caught only here.

    Runs through the SAME `_seed_internal` helper, differing in nothing but the
    seeded amount.

    ⚠️ IT IS NOT INERT UNDER THE MUTATION, and the docstring says so rather than
    claiming a purity it does not have: deleting `internal_cost_exact_usd=
    internal` also reds this test, at `exact == WHOLE_CENT_INTERNAL_COST` with
    `0 != 10.000000`. What makes it a control is the DIRECTION of what it pins —
    it is the only place asserting that the field equals the displayed cent when
    the two should agree, and that `sub_cent_internal` stays False there.
    """
    tenant = await _seed_internal(db_session, "whole-cent-exact", cost=WHOLE_CENT_INTERNAL_COST)

    # The precondition that makes this a control: on THIS amount the quantize is
    # provably inert, so exact and shown MUST be the same money.
    assert quantize_money(WHOLE_CENT_INTERNAL_COST) == WHOLE_CENT_INTERNAL_COST

    overview = await compute_billing_overview(_month(), db_session)
    entry = {t.tenant_id: t for t in overview.tenants}[tenant]
    shown = Decimal(entry.internal_cost_usd)
    exact = Decimal(entry.internal_cost_exact_usd)

    assert shown == WHOLE_CENT_INTERNAL == WHOLE_CENT_INTERNAL_COST
    assert exact == WHOLE_CENT_INTERNAL_COST, (
        f"internal_cost_exact_usd is {exact} on a tenant that incurred exactly "
        f"{WHOLE_CENT_INTERNAL_COST}: the field is not the raw internal cost."
    )
    assert exact == shown, (
        "the pre-image and the displayed cent disagree on a whole-cent amount, "
        "where the quantize moves nothing — the field is carrying some other "
        "figure than the internal cost behind the column"
    )
    # ...and the marker must NOT fire here: this row's cost is fully displayed.
    assert (shown == 0 and exact > 0) is False, (
        "a tenant whose internal cost renders in full is being flagged "
        "'cost < $0.01' — the third state has become the default state"
    )


# ---------------------------------------------------------------------------
# The two folds part company only when a tenant holds MORE THAN ONE term on an
# axis. Every fixture above holds exactly one flat and one per-seat lineage, so
# `Σq` and `qΣ` are the same number there and neither reader can be wrong.
# ---------------------------------------------------------------------------

#: A SECOND flat term and a SECOND per-seat term, each identical to the first.
#: ⭐ THE FIXTURE PROPERTY: with two terms per axis the per-term fold and the
#: per-axis fold differ by a FULL CENT on each axis — `q(10.005) × 2 = 20.02`
#: against `q(10.005 × 2) = 20.01` — which is unobservable at one term per axis.
TWO_TERM_FEE_CHARGED = Decimal("60.04")  # Σ q(term) — what the invoice charges
TWO_TERM_FEE_PER_AXIS = Decimal("60.02")  # Σ q(axis) — what a pre-summed reader gets


async def _seed_two_terms_per_axis(session: AsyncSession, label: str) -> str:
    """A tenant holding TWO flat and TWO per-seat terms, all carrying sub-cent scale.

    ⚠️ FOUR DISTINCT `service_key`s, for the index and nothing else.
    `ix_accesscharge_open` is UNIQUE on `(tenant_id, coalesce(service_key,''))`
    for open rows, so two concurrent flat terms need two keys; `_applies` does
    not filter on `service_key`, so all four are billed.
    """
    tenant = T(label)
    session.add(
        TenantConfig(
            tenant_id=tenant,
            tenant_name="two-terms-per-axis",
            markup_pct=Decimal("0"),
            billing_mode="standard_markup",
        )
    )
    for key, cadence, amount in (
        (None, "monthly", ACCESS_FEE),
        ("flat-two", "monthly", ACCESS_FEE),  # pragma: allowlist secret
        ("seats", "per_seat", SEAT_RATE),  # pragma: allowlist secret
        ("seats-two", "per_seat", SEAT_RATE),  # pragma: allowlist secret
    ):
        session.add(
            TenantAccessCharge(
                tenant_id=tenant,
                service_key=key,
                amount_usd=amount,
                cadence=cadence,
                effective_from=PAST,
            )
        )
    await session.commit()
    return tenant


@pytest.mark.unit
async def test_the_two_term_fixture_really_forks_the_per_term_and_per_axis_folds(
    db_session, test_settings, licences_says_two_seats
):
    """⭐ NON-VACUITY GUARD for the test below.

    Three ways it could be green-because-nothing-happened, each pinned here:

    1. The per-term and per-axis folds must give DIFFERENT cents on these
       inputs — at one term per axis they cannot, which is why every fixture
       above this point is blind to the defect the test below measures.
    2. All four terms must actually have been billed (a seat stub that failed,
       or an index collision that dropped a row, would silently halve the
       fixture back to the one-term-per-axis shape).
    3. The invoice — the reference implementation — must charge `Σ q(term)`.
    """
    # 1. The two folds genuinely part company, on BOTH axes.
    assert quantize_money(ACCESS_FEE) * 2 != quantize_money(ACCESS_FEE * 2)
    assert quantize_money(SEAT_RATE * SEAT_COUNT) * 2 != quantize_money(SEAT_RATE * SEAT_COUNT * 2)
    assert TWO_TERM_FEE_CHARGED != TWO_TERM_FEE_PER_AXIS

    tenant = await _seed_two_terms_per_axis(db_session, "two-term-fixture")
    totals = (await generate_billing(tenant, _month(), db_session, test_settings))["totals"]

    # 2. Four terms, four lines, and the seat lookup succeeded.
    assert totals["seat_count"] == SEAT_COUNT, (
        f"seat_count is {totals['seat_count']!r} (basis {totals['seat_basis']!r}): "
        "the seat stub was not reached and the per-seat half of this fixture is inert"
    )
    cadences = sorted(c["cadence"] for c in totals["access_charges"])
    assert cadences == ["monthly", "monthly", "per_seat", "per_seat"], (
        f"expected two flat and two per-seat lines, got {cadences}: the fixture "
        "collapsed to one term per axis, where the two folds agree"
    )

    # 3. The invoice charges the sum of the per-term cents.
    assert Decimal(totals["access_charge_usd"]) == quantize_money(ACCESS_FEE) * 2
    assert Decimal(totals["seat_fee_usd"]) == quantize_money(SEAT_RATE * SEAT_COUNT) * 2
    assert Decimal(totals["customer_cost_usd"]) == TWO_TERM_FEE_CHARGED


@pytest.mark.unit
async def test_overview_and_invoice_agree_when_a_tenant_holds_two_terms_per_axis(
    db_session, test_settings, licences_says_two_seats
):
    """The portal must show the invoiced cent for a MULTI-TERM tenant too.

    ⚠️ MUTATION TARGET — THE MONEY ONE, which is the one that counts. Collapse
    either fold IN THE READER to its per-axis form:

      * `customer += quantize_money(sum(access_by_tenant.get(tid, ()), 0))`
        in place of the per-term generator (`routers/billing.py`, the
        `access_by_tenant` fold), or
      * `seat_fee_by_tenant[_tid] = quantize_money(sum(_rates, 0) * _seats)`
        in place of the per-term generator above it.

    MEASURED, one at a time and then together: `shown 60.03` and `shown 60.02`
    against `invoiced 60.04`, at the `shown == invoice` assertion below. Both
    forms are TYPE-VALID, so they die of the MONEY and nothing else — which is
    what makes them the mutation this test is graded on.

    ⚠️ CORRECTED 2026-09-15 — THIS DOCSTRING USED TO NAME A DIFFERENT MUTATION
    AND ATTACH THOSE SAME FIGURES TO IT. It said: make either all-tenant
    aggregator pre-sum its rows (`totals[tid] = totals.get(tid, 0) + …` in
    `services.access_charge.active_access_charges_all_tenants`, or `rates[tid] =
    rates.get(tid, 0) + …` in `seat_rates_all_tenants`) "and this fails with
    `shown 60.03`/`60.02` against `invoiced 60.04`". ⛔ IT CANNOT. Both
    aggregators are typed `dict[str, list[Decimal]]` and BOTH readers ITERATE
    what they return, so a pre-summed `Decimal` never reaches an assertion at
    all. Measured, both mutants applied separately:

        TypeError: 'decimal.Decimal' object is not iterable
          — src/routers/billing.py:464   (the access fold)
          — src/routers/billing.py:235   (the seat fold)

    ⛔ A MUTANT THAT DIES OF A CRASH IS A **THREW SENTINEL**, and this repo
    refuses to count one as a kill (see `a-mutation-control-with-a-threw-
    sentinel-passes-when-the-mutant-crashes`): the test never evaluated the
    property, it merely failed to run. Claiming a money comparison that never
    happened is the dangerous direction — a reader would believe the money is
    pinned by a mutation that only proves an argument is iterable, and a future
    refactor making the pre-summed form TYPE-VALID (e.g. an aggregator returning
    `dict[str, Decimal]` with the reader adapted to it) would be a silent
    revert. The reader-side collapses named above are the type-valid form of
    exactly that refactor, and they are what the `shown == invoice` /
    `== TWO_TERM_FEE_CHARGED` / `!= TWO_TERM_FEE_PER_AXIS` assertions catch.

    A pre-summing aggregator is still a real defect and still reds this test —
    on a TypeError. Fix it; just do not read that red as evidence about money.

    THE FIXTURE PROPERTY: two terms on each axis. The single-term fixtures above
    pass under every mutation named here, because one term makes `Σq` and `qΣ`
    the same number — which is exactly how a per-axis fold stayed unremarkable.
    """
    tenant = await _seed_two_terms_per_axis(db_session, "two-term-overview")
    invoice = await _invoice_total(db_session, tenant, test_settings)

    # ⭐ THE TWO FOLDS, COMPUTED HERE RATHER THAN REMEMBERED, so the literals
    # below are anchored to arithmetic. `per_axis` is precisely what a reader
    # that quantizes each AXIS once returns on this fixture — a cent per extra
    # term short of the bill — and it is the figure the type-valid mutation
    # above produces. Without this, a change to the fee constants could leave
    # both literals stale and the equality passing on a coincidence.
    per_term = quantize_money(ACCESS_FEE) * 2 + quantize_money(SEAT_RATE * SEAT_COUNT) * 2
    per_axis = quantize_money(ACCESS_FEE * 2) + quantize_money(SEAT_RATE * SEAT_COUNT * 2)
    assert per_term == TWO_TERM_FEE_CHARGED
    assert per_axis == TWO_TERM_FEE_PER_AXIS
    assert per_term != per_axis, (
        "the per-term and per-axis folds agree on these fees; the fixture cannot "
        "distinguish the reader under test from a per-axis one"
    )

    overview = await compute_billing_overview(_month(), db_session)
    shown = Decimal({t.tenant_id: t for t in overview.tenants}[tenant].customer_cost_usd)

    assert shown == invoice, (
        f"/billing/overview shows {shown} where the invoice charges {invoice}: "
        "the portal is folding each AXIS to a cent while the invoice folds each "
        "TERM to a cent. Quantize every term, then sum."
    )
    assert shown == TWO_TERM_FEE_CHARGED
    assert shown != TWO_TERM_FEE_PER_AXIS


@pytest.mark.unit
async def test_the_platform_totals_carry_two_places_on_a_month_with_no_tenants(
    db_session, test_settings
):
    """⭐ THE ONE CASE THE PLATFORM-TOTAL QUANTIZES ARE NOT IDEMPOTENT ON.

    `platform_total_internal_usd` / `platform_total_customer_usd` accumulate
    already-quantized per-tenant cents, so `quantize_money(...)` around them
    moves no money and every fixture in this file leaves a mutation that drops
    those calls ALIVE. That is not a reason to delete them and not a reason to
    assert an identity: with NO tenants the accumulator is a bare `Decimal("0")`
    at exponent 0, and the call is the only thing that makes the platform render
    `$0.00` rather than `$0`.

    ⚠️ MUTATION TARGET: `platform_total_internal_usd=platform_internal` (drop the
    `quantize_money`) in `routers.billing.compute_billing_overview`. This then
    fails with exponent 0. THE FIXTURE PROPERTY is the empty month — every other
    test here seeds at least one tenant, where the call is provably inert.
    """
    overview = await compute_billing_overview("2019-01", db_session)

    assert overview.tenants == [], (
        "this month must have no tenants at all — with one, the accumulators "
        "already carry two places and the quantize under test is inert"
    )
    for field, value in (
        ("platform_total_internal_usd", Decimal(overview.platform_total_internal_usd)),
        ("platform_total_customer_usd", Decimal(overview.platform_total_customer_usd)),
    ):
        assert value == 0
        assert value.as_tuple().exponent == -2, (
            f"{field} renders {value} on an empty month: the platform total is a "
            "bare zero rather than a money figure at two places"
        )
