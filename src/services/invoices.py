"""Freeze a computed bill into the persisted invoice OBJECT.

:func:`services.billing.generate_billing` is a REPORT: it recomputes a month
from ``billing_cost_daily`` + ``markup_rules`` + ``tenant_access_charge`` every
time it is called, so a rule edited afterwards silently changes the answer. This
module is the other half — it takes one such computation and writes it down,
together with everything mutable that fed it, so the number stays the number.

THE ONE FUNCTION THAT JUSTIFIES THE TABLE
-----------------------------------------
:func:`reproduce` reads ``billing_invoices`` and NOTHING ELSE. It does not call
``generate_billing``, does not open a rollup, does not resolve a markup rule. If
re-rendering a statement needed any mutable table, the statement would not be a
record — and every guarantee below would be decoration.

WHAT "FROZEN" COVERS AND WHAT IT DOES NOT
-----------------------------------------
``snapshot`` is the generator's answer. ``inputs`` is the world it read to get
there. ``snapshot_sha256`` binds the two, so a later reader can prove the pair
has not been edited. What none of them can do is prove the SOURCE was complete
when it was read: see :func:`assert_no_bucket_gap`, which states plainly the
class of under-billing this schema currently cannot detect.
"""

from __future__ import annotations

import hashlib
import json
import uuid as uuid_mod
from dataclasses import dataclass
from datetime import UTC, date, datetime, time, timedelta
from decimal import ROUND_HALF_UP, Decimal
from typing import TYPE_CHECKING, Any

import structlog
from sqlalchemy import func, not_, select, text
from sqlalchemy.exc import IntegrityError
from sqlalchemy.ext.asyncio import AsyncSession

import observability.metrics as observability_metrics
from models.db import (
    NULL_LICENSE_ID,
    AppCostRate,
    BillingInvoice,
    CostDaily,
    MarkupRule,
    MeteringDaily,
    ProviderPricing,
    TenantAccessCharge,
    TenantConfig,
)
from services import prepaid_credit as prepaid_credit_svc
from services.access_charge import (
    active_access_charges_all_tenants,
    active_access_charges_for_month,
    one_time_remaining_before,
    seat_rates_all_tenants,
)
from services.billing import (
    LicensesUnavailableError,
    app_cost_rate_map,
    fetch_licenses,
    generate_billing,
    get_app_cost_rate_rows,
    get_app_cost_rates,
    get_billing_mode,
    get_markup_pct_with_source,
)
from services.byo_mode import (
    ByoModeUnavailableError,
    effective_billing_mode,
    get_byo_mode,
    get_byo_mode_strict,
)
from services.markup import BILLING_MODE_INTERNAL, MARKUP_SOURCE_UNKNOWN, MONEY_QUANT
from services.seats import SeatCountUnavailableError, get_seat_count, seat_scope_for_charge
from services.tenant_names import (
    TenantInfo,
    TenantRegistryUnavailableError,
    get_tenant_info_strict,
    get_tenant_names,
    resolve_tenant_name,
)
from settings import Settings

if TYPE_CHECKING:
    from collections.abc import Sequence

    from sqlalchemy.ext.asyncio import async_sessionmaker

    from service_auth import ServiceTokenProvider

logger = structlog.stdlib.get_logger(__name__)

#: Version of the frozen document SHAPE (``snapshot`` + ``inputs`` layout), not
#: of the arithmetic. A reader that understands ``invoice.v1`` knows which keys
#: exist; it learns nothing about which generator produced the figures, which is
#: what ``BillingInvoice.generator_version`` carries.
INVOICE_SCHEMA_VERSION = "invoice.v1"

#: Name of the partial unique index that backstops :func:`close_period`'s
#: idempotency. Spelled here as a literal for the same reason alembic spells its
#: trigger names as literals: the constraint lives in the database, and a
#: constant imported from the model would let the two drift without a failure.
_OPEN_PERIOD_INDEX = "ux_billing_invoices_open_period"

#: The number series used while ``BillingInvoice.legal_entity`` is still part of
#: the present-and-null legal set. See :func:`_counter_scope`.
#:
#: ⚠️ THIS VALUE IS BOTH THE COUNTER SCOPE AND THE RENDERED NUMBER'S PREFIX, and
#: they must never be allowed to drift apart. The counter is keyed per
#: ``(series, year)``, but ``ux_billing_invoices_number`` is GLOBALLY unique. If
#: the number omitted the series, then the moment a second ``legal_entity`` was
#: populated both series would draw seq 1 for the same year, both would render
#: the identical string, and the second issue() would die on that unique index.
#: Deriving both from one value is what keeps filling in ``legal_entity`` a
#: fill-in rather than a migration against rows that are immutable by design.
_DEFAULT_NUMBER_SERIES = "INV"

#: First ``CostDaily.bucket_date`` whose ``unpriced_event_count == 0`` is a
#: MEASUREMENT OF ZERO rather than an ABSENCE OF MEASUREMENT. Below this date the
#: column's 0 says nothing at all, and :func:`assert_no_unpriced_provider_cost`
#: must not read it as a clean bill.
#:
#: ⚠️ A 0 ON A PRE-0031 ROW IS NOT EVIDENCE. alembic
#: ``0031_cost_daily_unpriced_visible`` (its own ``Create Date: 2026-09-11``)
#: added the column ``NOT NULL DEFAULT 0`` and ships NO BACKFILL on purpose — its
#: own "NO BACKFILL, DELIBERATELY" block states why: the only source is
#: ``billing_cost_events``, which is monthly RANGE-partitioned with retention and
#: was TRUNCATEd 2026-09-05T07:01:56Z, so a figure computed for an older day
#: would be "an undercount wearing the costume of a measurement". Every row
#: written before that migration therefore reads EXACTLY like a day on which
#: nothing was unpriced, and the two are not distinguishable after the fact.
#:
#: ⚠️ THE DAY AFTER the migration's date, not the day of it, and the extra day is
#: deliberate. ``billing_cost_daily`` has no ``created_at`` and no ``updated_at``
#: — ``bucket_date`` is its only time column (the same schema absence
#: :func:`assert_no_bucket_gap` (c) records) — so the instant a row was WRITTEN
#: cannot be read back. A row bucketed 2026-09-11 may have been written at 00:05
#: that morning by a pod that predates the deploy, in which case its 0 is the
#: server default. Declining to claim one extra day is the cheap error; claiming
#: a day that was never measured is the expensive one, and choosing that
#: direction is the entire purpose of this constant.
#:
#: ⚠️ IT GATES THE *CLEAN* VERDICT ONLY, NEVER THE REFUSAL. A count ABOVE zero is
#: a real measurement whatever its bucket_date — only code that knows the column
#: exists can write a non-zero into it — so a positive on an older day is
#: refused, not excused. See the gate's docstring.
_UNPRICED_EVENT_COUNT_MEASURED_FROM = date(2026, 9, 12)


# --------------------------------------------------------------------------- #
# Errors
# --------------------------------------------------------------------------- #


class InvoiceError(RuntimeError):
    """Base for every refusal this module raises."""


class BucketGapError(InvoiceError):
    """The cost rollup wrote NO rows at all for one or more days in the period.

    Distinct and named because the correct reaction is distinct: this is not a
    transient failure to retry and not a tenant with a quiet week — it is the
    invoice's own source being absent for a date, which under-bills by whatever
    happened that day with nothing on the statement to say so. Closing is
    REFUSED rather than degraded.

    ``missing_dates`` is the ordered list of ``date`` objects with no rollup row.
    """

    def __init__(self, tenant_id: str, missing_dates: Sequence[date]) -> None:
        self.tenant_id = tenant_id
        self.missing_dates = list(missing_dates)
        rendered = ", ".join(d.isoformat() for d in self.missing_dates)
        super().__init__(
            f"refusing to close a period for tenant {tenant_id}: billing_cost_daily "
            f"has no rows at all for {len(self.missing_dates)} day(s) in the range "
            f"({rendered}). The rollup did not write those buckets, so any total "
            f"computed over this period is short by an unknown amount."
        )


class UnpricedServiceKeyError(InvoiceError):
    """A ``service_key`` with billable requests has NO ``AppCostRate`` row at all.

    Distinct and named for the same reason as :class:`BucketGapError`, and with
    the same stance — REFUSED, not degraded. ``services/billing.py:562`` resolves
    the per-request app charge with ``app_rates.get(row.service_key,
    Decimal("0"))``, so a key the rates table has never heard of is invoiced at
    **$0.00**, and on the statement that fabricated zero is indistinguishable
    from a service the operator priced free on purpose. It is also SILENT: the
    $0 makes ``app_incurred`` zero and ``services/markup.py:498`` gates the
    zero-margin alarm on ``Decimal(incurred_usd) > 0``, so the fabrication
    SUPPRESSES the one alarm that would have named it. The comment at
    ``services/billing.py:524-542`` marks that site ``⛔ REPORT ONLY`` — it
    counts the gap and invents no rate. This class is the half that stops the
    gap from being written down: an invoice number is drawn from a gapless
    series and the row is immutable, so there is no edit afterwards.

    ⚠️ MEMBERSHIP, NOT VALUE, IS THE DISCRIMINATOR. ``get_app_cost_rates``
    returns EVERY row, including one whose ``cost_per_request_usd`` is 0 — the
    column is NOT NULL, so a priced-at-zero service is a present key holding a
    present value. ``key in app_rates`` is therefore true for a genuine free
    tier and false only for an ABSENT row. Testing ``rate == 0`` instead would
    refuse to close every tenant on a deliberate free tier, which is the defect
    rather than the fix: a stated 0 is a price, a missing row is an unanswered
    question.

    ``service_keys`` is the sorted list of keys that had requests and no rate.
    """

    def __init__(self, tenant_id: str, service_keys: Sequence[str]) -> None:
        self.tenant_id = tenant_id
        self.service_keys = list(service_keys)
        rendered = ", ".join(self.service_keys)
        super().__init__(
            f"refusing to close a period for tenant {tenant_id}: "
            f"{len(self.service_keys)} service_key(s) with billable requests have no "
            f"billing_app_cost_rates row ({rendered}), so every app line for them would be "
            f"invoiced at $0.00 and would read as a deliberately free service. Add a row to "
            f"billing_app_cost_rates for each key — a rate of 0 is an acceptable answer and "
            f"states the intent; an absent row does not."
        )


class UnpricedProviderCostError(InvoiceError):
    """Provider spend in the period could not be PRICED AT ALL, so its cost is unknown.

    ⛔ THIS IS NOT :class:`UnpricedServiceKeyError` AND MUST NEVER BE COLLAPSED
    INTO IT. That one is a missing APP-REQUEST RATE (``billing_app_cost_rates``):
    the per-request charge this platform adds on top. This one is a missing
    PROVIDER PRICE (``provider_pricing``): what the tokens actually COST us. The
    remedies are different rows in different tables, the money moves in different
    directions (one under-charges the customer, the other under-states internal
    cost and therefore OVER-states margin), and an operator handed one message for
    the other would go and edit the wrong table. ``rollup_engine``'s own comment on
    the counter is explicit that ``assert_all_service_keys_priced`` "gates only on
    a missing app-request RATE and cannot see a missing PROVIDER price at all".

    THE SHAPE IT REFUSES, end to end. ``services.pricing`` REFUSES to guess when
    no ``provider_pricing`` row matches and returns ``None``; ``cost_flusher``
    stores that refusal as ``billing_cost_events.cost_usd IS NULL`` (the column
    carries "NO default: NULL is the load-bearing 'no ProviderPricing matched'
    sentinel"); ``rollup_engine`` then aggregates with
    ``coalesce(sum(cost_usd), 0)`` into a ``NOT NULL`` column, and when EVERY row
    of a group is unpriced that writes a hard ``$0.000000`` beside a real token
    count. At that point "this spend was free" and "this spend is unknown" are the
    same stored bytes. ``CostDaily.unpriced_event_count`` (alembic 0031) is the
    one column that still knows the difference, and this error is the close
    refusing rather than freezing the collapse onto an immutable row drawn from a
    gapless series.

    ``dates`` is the sorted list of ``bucket_date``s holding unpriced events;
    ``providers`` the sorted list of providers they were spent on. Both are named
    in the message because the remedy is per (provider, day): price the provider,
    let ``daily_reconcile_days`` re-roll those days, then close.
    """

    def __init__(
        self,
        tenant_id: str,
        dates: Sequence[date],
        providers: Sequence[str],
    ) -> None:
        self.tenant_id = tenant_id
        self.dates = list(dates)
        self.providers = list(providers)
        rendered_dates = ", ".join(d.isoformat() for d in self.dates)
        rendered_providers = ", ".join(self.providers)
        super().__init__(
            f"refusing to close a period for tenant {tenant_id}: billing_cost_daily "
            f"carries unpriced cost events on {len(self.dates)} day(s) "
            f"({rendered_dates}) for {len(self.providers)} provider(s) "
            f"({rendered_providers}). Those events have no matching provider_pricing "
            f"row, so services.pricing left cost_usd NULL and the rollup's "
            f"coalesce(sum(cost_usd), 0) stored the unknown spend as $0.000000 — on "
            f"the statement an unpriced provider is indistinguishable from a free "
            f"one, and it understates internal cost while overstating margin. Add a "
            f"provider_pricing row for each provider/model above and let the rollup "
            f"restate those days before closing."
        )


class TenantNotInvoiceableError(InvoiceError):
    """This tenant must not be invoiced for this period at all.

    ⭐ THE GAP THIS CLOSES IS NOT A WRONG FIGURE, IT IS A WRONG RECIPIENT. Every
    other gate in this module asks whether the NUMBERS can be stated honestly.
    None of them asked whether this tenant should receive a statement, so the
    sweep's tenant set -- "everyone with a rollup or metering row in the period"
    (:func:`close_due_periods`) -- was also its billing set.

    Measured 2026-09-13 against the live registry, for the August period a first
    armed tick would have closed:

    * ``ad976f5b-…``, ``6c6a0da6-…``, ``351fdc59-…``, ``0195f48b-…-0001f2`` are in
      the cost ledger and in NO registry row. ``services.byo_mode`` maps their
      licences 404 to ``byo_required`` -- a deliberate FAIL-CLOSED-TO-BILL that is
      correct for the live access gate and wrong here, because
      :func:`services.byo_mode.effective_billing_mode` then keeps the configured
      mode and the close mints a MARKED-UP COMMERCIAL statement for a tenant that
      does not exist.
    * ``019fb1f4-…`` (``rmt-e2e``) is ``kind='test'``, ``byo_mode='byo_required'``
      and ARCHIVED since 2026-09-03 -- so it too resolves commercial.

    ⛔ AND THE LICENCES SERVICE ALREADY REFUSES THE WEAKER VERSION OF THIS. Its
    mint path declines an archived tenant outright ("refusing to mint: tenant is
    archived -- un-archive the tenant before creating a license"). A close that
    invoices a tenant licences would not even issue a licence for is laxer than
    the service it bills on behalf of.

    ``reason`` is carried so the counter can discriminate: "refused three phantom
    tenants" and "refused every tenant because the registry was unreachable" are
    opposite operational facts that an undifferentiated refusal count reports
    identically.
    """

    def __init__(self, tenant_id: str, reason: str, detail: str = "") -> None:
        self.tenant_id = tenant_id
        self.reason = reason
        suffix = f" ({detail})" if detail else ""
        super().__init__(
            f"refusing to close: tenant {tenant_id} is not invoiceable [{reason}]{suffix}"
        )


class PeriodNotBillableError(InvoiceError):
    """Nothing in this tenant-period is billable, so no number is drawn.

    ⛔ A SKIP, NOT A FAILURE, AND THE DISTINCTION IS THE WHOLE POINT. The invoice
    number comes from a GAPLESS counter row (:func:`issue`) and a burned number is
    audit evidence no later fix can retract. Measured 2026-09-13: the estate's one
    and only invoice, ``INV-2026-000001``, is **$0.00** for
    ``0195f48b-…-0001`` = ``n8n-dev-portal-realm``, an INTERNAL ``cost_centre=ci``
    tenant -- and ~10 further internal cost centres had August activity, so an
    armed loop would have drawn a number a month, each, for statements with no
    money on them.

    ⚠️ THE TEST IS EVERY MONEY COLUMN BEING EXACTLY ZERO, not ``total_usd == 0``
    alone. A total can net to zero from non-zero components (a credit against a
    charge), and that period IS billable -- the customer is owed the statement
    that shows both lines. Only a period where nothing was charged at all is
    pointless to number.

    ⚠️ WHAT THIS IS NOT: it is not "internal tenants are never invoiced". A tenant
    whose mode is internal but which carries an access charge still bills, and a
    commercial tenant with a genuinely empty month is still skipped. The rule is
    about the MONEY, so it cannot be wrong about the classification.
    """

    def __init__(self, tenant_id: str, month: str, billing_mode: str = "") -> None:
        self.tenant_id = tenant_id
        self.month = month
        #: ⭐ THE RESOLVED MODE RIDES ON THE REFUSAL, and it is evidence nothing
        #: else can supply. A skipped period leaves NO row, so
        #: ``billing_invoices.billing_mode`` -- the only other place the mode is
        #: recorded -- does not exist to be read. Without it, "the close correctly
        #: stayed internal and therefore charged nothing" and "the close degraded to
        #: commercial and happened to charge nothing" are the SAME observation, and
        #: the TOCTOU protection in ``close_period``'s byo_mode ordering would have
        #: no assertable consequence for any period that is skipped.
        self.billing_mode = billing_mode
        mode = f" mode={billing_mode}" if billing_mode else ""
        super().__init__(
            f"skipping close: tenant {tenant_id} has no billable basis for {month}{mode}"
        )


class InvoiceMoneyImbalanceError(InvoiceError):
    """``total_usd`` is not the sum of the component columns that explain it.

    ⛔ A NEW CLASS RATHER THAN A REUSED ONE, AND THE REASON IS THE SAME ONE
    :class:`UnpricedProviderCostError` STATES: the remedy differs. Every sibling
    here names an INPUT the close could not obtain — an absent rollup day, an
    unpriced service key, an unpriced provider, a tenant the registry does not
    hold. This one names an ARITHMETIC failure in the generator's own output,
    with every input present and answered. An operator handed one message for the
    other would go and edit a rate table over a code defect.

    ⛔ AND IT IS EMPHATICALLY NOT :class:`PeriodNotBillableError`. That is a SKIP
    — expected, uncounted as a failure, logged at info, and the correct outcome
    for most of the estate every month. Raising it here would file an invoice
    that does not add up under "nothing to bill", which is the single worst place
    this refusal could land: the period would be silently passed over and the
    defect would never be read.

    WHAT THE IDENTITY IS, AND WHY THESE SIX TERMS
    ---------------------------------------------
    ``services/billing.py`` composes the customer total as::

        customer_cost = Σ(app_request_billed + llm_billed) + access_total + seat_fee_total

    and each billed stream is carried on the invoice as an incurred/markup PAIR
    that sums back to it. So the six component columns account for the total
    exactly, and ``platform_fee_usd`` is deliberately NOT among them: it is a
    surcharge folded INSIDE the billed lines (``lu["platform_fee"]`` is
    accumulated from the same ``compute_billed_line_detailed`` results whose
    ``billed_usd`` is already in the streams), so adding it would double-count it.
    ``subtotal_usd``, ``internal_cost_usd``, ``margin_usd`` and
    ``excluded_unbillable_usd`` are not components of the total either — the first
    equals it, the next two are the cost/margin axis, and the last is spend the
    statement deliberately did NOT bill.

    ``residual`` is ``total_usd − Σcomponents``, signed, so the message says which
    way the statement is wrong: negative means the components claim MORE than the
    customer is charged, positive means the total charges money no component
    accounts for (the shape the missing app-request pair had).
    """

    def __init__(
        self,
        tenant_id: str,
        month: str,
        total: Decimal,
        components: dict[str, Decimal],
        residual: Decimal,
    ) -> None:
        self.tenant_id = tenant_id
        self.month = month
        self.total = total
        #: Every component and its value, so the refusal names the figures rather
        #: than only the verdict — an operator reading the log must be able to see
        #: WHICH line is wrong without re-running the close.
        self.components = dict(components)
        self.residual = residual
        rendered = ", ".join(f"{name}={value}" for name, value in components.items())
        super().__init__(
            f"refusing to close a period for tenant {tenant_id} ({month}): "
            f"total_usd={total} is not the sum of its components ({rendered}); "
            f"residual={residual}. An invoice whose stored components cannot "
            "account for the amount charged is not reconcilable, and the row is "
            "immutable and drawn from a gapless series once written."
        )


class InvoiceNegativeTotalError(InvoiceError):
    """``total_usd`` came out below zero: a statement may never pay the customer.

    The one way it could arise was prepaid credit rounding: credit applied as
    usage rounded to cents, subtracted from the UNROUNDED usage (10.005 -> -0.01),
    a residual the one-cent identity tolerance let through. Credit now rounds DOWN
    (``services.prepaid_credit.CreditForMonth.applied``); this refusal makes any
    future path to a negative statement a refused close rather than an issued one.
    """

    def __init__(self, tenant_id: str, month: str, total: Decimal) -> None:
        super().__init__(f"invoice for {tenant_id} {month} totals {total} < 0")
        self.tenant_id = tenant_id
        self.month = month
        self.total = total


class InvoicePeriodError(InvoiceError):
    """The requested period is not a shape the generator can compute.

    ``generate_billing`` takes a ``YYYY-MM`` month and derives the bounds itself,
    so a period that is not exactly one calendar month cannot be honoured. It is
    refused rather than silently widened to the containing month.
    """


class InvoiceStateError(InvoiceError):
    """The requested lifecycle transition is not permitted from this status."""


# --------------------------------------------------------------------------- #
# Results
# --------------------------------------------------------------------------- #


@dataclass(frozen=True)
class CloseDueOutcome:
    """What one sweep of :func:`close_due_periods` actually did.

    ⚠️ THE FOUR FIELDS ARE FOUR DIFFERENT OUTCOMES and the caller must be able
    to tell them apart. A bare ``list[BillingInvoice]`` could not: a period that
    was already closed and a period closed just now arrive at the call site as
    the same object, and a refusal arrives as nothing at all.
    """

    #: Invoices this sweep drew.
    created: list[BillingInvoice]
    #: Periods that already held a live invoice before this sweep ran.
    unchanged: list[BillingInvoice]
    #: Tenants that raised :class:`InvoiceError` — the module refusing on purpose.
    refused: int
    #: Tenants that died of anything else. Kept APART from ``refused`` because a
    #: refusal is this module working and a failure is not.
    failed: int
    #: Tenants whose period charged nothing, so no number was drawn
    #: (:class:`PeriodNotBillableError`). ⛔ ITS OWN FIELD, NOT FOLDED INTO
    #: ``refused``: a refusal means "this period cannot be stated honestly YET"
    #: and the next sweep retries it, while a skip means "there is nothing here to
    #: bill" and every future sweep will skip it again. An operator reading a
    #: refusal count reaches for a cause; reading a skip count they should reach
    #: for nothing at all. Merging them would make a month of correct skips look
    #: like a month of unresolved problems.
    skipped: int = 0


@dataclass(frozen=True)
class UnpricedCostCoverage:
    """What :func:`assert_no_unpriced_provider_cost` could and could NOT measure.

    ⚠️ THE GATE RETURNS THIS RATHER THAN ``None`` SO THAT "IT DID NOT RAISE" AND
    "THE PERIOD IS PROVEN CLEAN" CANNOT BE THE SAME OBSERVATION. They are not the
    same fact: every ``billing_cost_daily`` row bucketed before
    :data:`_UNPRICED_EVENT_COUNT_MEASURED_FROM` reads ``unpriced_event_count = 0``
    whether or not anything was unpriced that day, so a silent pass over those
    rows is an absence of evidence being reported as evidence of absence. A caller
    (and a test) can read these counts; a bare ``None`` would force both to infer
    coverage from the gate's silence, which is the defect the floor exists to
    prevent.
    """

    #: Rows whose 0 IS a measurement — bucketed at/after the floor.
    measured_rows: int
    #: Rows whose 0 proves NOTHING — bucketed before the floor. Not a refusal (see
    #: the gate's docstring for why), but never counted as clean either.
    unmeasurable_rows: int
    #: The distinct ``bucket_date``s those unmeasurable rows sit on, sorted.
    unmeasurable_dates: list[date]


# --------------------------------------------------------------------------- #
# JSON helpers
# --------------------------------------------------------------------------- #


def _jsonable(value: Any) -> Any:
    """Recursively coerce a value into something ``json.dumps`` accepts.

    ``Decimal`` becomes a STRING, never a float: the frozen figures are money and
    a float round-trip is a silent edit to a record that must not change. Dates
    and datetimes become ISO-8601, UUIDs their canonical text, sets a sorted list
    (a set has no order, and the digest below must be reproducible).
    """
    if isinstance(value, Decimal):
        return str(value)
    if isinstance(value, datetime):
        return value.astimezone(UTC).isoformat()
    if isinstance(value, date):
        return value.isoformat()
    if isinstance(value, uuid_mod.UUID):
        return str(value)
    if isinstance(value, dict):
        return {str(k): _jsonable(v) for k, v in value.items()}
    if isinstance(value, list | tuple):
        return [_jsonable(v) for v in value]
    if isinstance(value, set | frozenset):
        return sorted(_jsonable(v) for v in value)
    return value


#: The money columns :func:`close_period` DERIVES from the frozen ``snapshot``
#: and stores as COPIES. Named once and consumed by :func:`_money_payload`, by
#: the ``BillingInvoice`` construction in :func:`close_period` and by
#: :func:`reproduce`, so the set that is hashed at close time and the set that is
#: verified at read time cannot drift into being two different sets.
_MONEY_COLUMNS: tuple[str, ...] = (
    "subtotal_usd",
    "total_usd",
    "internal_cost_usd",
    "margin_usd",
    "access_charge_usd",
    "platform_fee_usd",
    "llm_usage_usd",
    "llm_markup_usd",
    # The SECOND billed stream. `generate_billing` folds `app_request_billed` and
    # `llm_billed` into one `totals.customer_cost_usd`, and until alembic 0033
    # only the llm half had columns — so the stored components understated the
    # stored total by exactly the app-request amount, and `reproduce` could not
    # see it because `_money_from_snapshot` dropped the same term on both sides.
    "app_request_usage_usd",
    "app_request_markup_usd",
    "seat_fee_usd",
    "seat_count",
    "excluded_unbillable_usd",
    # PREPAID CREDIT applied (alembic 0055). See `_DIGEST_OMIT_WHEN_ZERO`.
    "prepaid_credit_applied_usd",
)

#: Money columns hashed ONLY when non-zero. ⭐ A column added AFTER statements
#: already exist must not change their digest: every statement closed before
#: alembic 0055 applied no credit and stores 0, so leaving a zero out of the
#: payload keeps every historical ``snapshot_sha256`` reproducible byte for byte,
#: while any non-zero credit is covered exactly like every other money column.
_DIGEST_OMIT_WHEN_ZERO: frozenset[str] = frozenset({"prepaid_credit_applied_usd"})

#: The money columns that decide whether a period is worth an invoice NUMBER:
#: does the customer owe anything, and is a commercial term in force? Read by the
#: no-billable-basis skip in :func:`close_period`; see
#: :class:`PeriodNotBillableError` for why a period with all three at zero must not
#: draw from the gapless series.
#:
#: ⛔ THE COMPONENT LINES ARE DELIBERATELY ABSENT, AND A MEASUREMENT IS WHY. This
#: set first included ``llm_usage_usd``/``llm_markup_usd``/``subtotal_usd`` and the
#: skip did not fire on the very shape it was written for. Measured 2026-09-13
#: against the internal-mode close that reproduces ``INV-2026-000001``:
#:
#:     internal_cost_usd=70.00  llm_usage_usd=70.000000  llm_markup_usd=-70.000000
#:     subtotal_usd=0.00  total_usd=0.00  access_charge_usd=0.00  seat_fee_usd=0.00
#:
#: ⇒ ``BILLING_MODE_INTERNAL`` is implemented as a markup that EXACTLY OFFSETS the
#: usage, not as an absence of usage. So a $0.00 internal statement carries
#: non-zero component lines, and any rule that reads them concludes "billable" for
#: every internal cost centre in the estate — passing its own unit test while never
#: firing in production.
#:
#: ⛔ ``app_request_usage_usd``/``app_request_markup_usd`` (alembic 0033) are
#: EXCLUDED FOR EXACTLY THE MEASUREMENT ABOVE, not by omission. They are the
#: app-request stream's component pair and they take the identical shape under
#: ``BILLING_MODE_INTERNAL``: the incurred rate is charged at $0, so the pair
#: reads ``+X``/``−X`` and nets to zero on a statement that charges nothing.
#: Adding either would re-open the very hole the llm pair was removed to close —
#: every internal cost centre in the estate would read as billable and the skip
#: would never fire in production while passing its own unit test.
#:
#: ⛔ ``platform_fee_usd`` is excluded for the same reason and not by oversight: it
#: is a line INSIDE ``total_usd``, so a real fee is already visible in the total,
#: while an internal mode that offsets only the llm lines would leave it non-zero
#: on a statement that charges nothing.
#:
#: ⚠️ ``access_charge_usd`` and ``seat_fee_usd`` ARE here even though they too sit
#: inside the total, and the asymmetry is intentional: they are the footprint of a
#: COMMERCIAL TERM somebody configured. If one is non-zero, intent to bill exists
#: and the period gets its statement even should the total net to zero. When in
#: doubt about a term, invoice — the opposite error is silently never billing a
#: customer who has one.
_CHARGED_MONEY_COLUMNS: tuple[str, ...] = (
    "total_usd",
    "access_charge_usd",
    "seat_fee_usd",
    # ⭐ A month whose LLM usage prepaid credit covered in full nets to $0 with no
    # fee -- and MUST still be closed, because the close is what freezes its
    # drawdown on the credit ledger. Skipped, the same credit would be offered to
    # the next month again: covered usage billed for free twice.
    "prepaid_credit_applied_usd",
)

#: The scale of every money column on ``billing_invoices`` — they are all
#: ``Numeric(20, 8)``. See :func:`_money_canon` for why the exact scale matters.
_MONEY_EXP = Decimal("0.00000000")


def _money_canon(value: Decimal | int | str | None) -> str | int | None:
    """Canonical text for ONE money value, at the storage column's own scale.

    ⛔ THE FIXED SCALE IS LOAD-BEARING, NOT TIDINESS, and getting it wrong fails
    in the direction that looks like success. The digest is computed in
    :func:`close_period` from values that have NOT yet been through Postgres, and
    re-verified in :func:`reproduce` from values that HAVE. ``Numeric(20, 8)``
    reads back as ``Decimal("0.00000000")`` where the pre-insert Python value was
    ``Decimal("0")`` — the same money, two different strings. A digest over the
    raw values would therefore mismatch on EVERY invoice ever written, and a
    detector that reports tampering unconditionally is indistinguishable from one
    that reports it never. Quantizing both sides to the column's scale is what
    makes the two readings comparable at all.

    ``ROUND_HALF_UP`` is the ``decimal`` module's round-half-AWAY-FROM-ZERO,
    which is what Postgres ``numeric`` does when it rounds a literal into scale
    8, so a value carrying more than 8 decimal places canonicalises identically
    on both sides rather than differing by one ulp.

    ``seat_count`` is an ``Integer`` and passes through untouched. ``None`` stays
    ``None``: ``BillingInvoice.seat_count`` documents "no per-seat term" and
    "zero seats" as DIFFERENT facts, and coercing one to the other here would
    hash them the same.

    ⛔ ``format(q, "f")``, NOT ``str(q)``. ``Decimal("0").quantize(Decimal(
    "0.00000000"))`` is ``Decimal("0E-8")`` and ``str()`` of that is the literal
    text ``"0E-8"`` — scientific notation, for the commonest money value there
    is. Both sides of the comparison would render it the same way, so the digest
    would still match and the defect would hide; what it would corrupt is the
    ``money_mismatches`` report, which exists to be READ by an operator, and any
    later reader comparing a canonical form against a plain decimal string. The
    ``"f"`` presentation type is fixed-point always, with no exponent.
    """
    if value is None:
        return None
    if isinstance(value, int):
        return value
    return format(Decimal(value).quantize(_MONEY_EXP, rounding=ROUND_HALF_UP), "f")


def _money_payload(values: dict) -> dict:
    """The digestible form of the money columns — canonical, and complete.

    ``values`` must carry every name in :data:`_MONEY_COLUMNS`; a missing one is
    a ``KeyError`` here rather than a silently unhashed money column, which is
    the whole failure this function exists to make impossible.
    """
    payload = {name: _money_canon(values[name]) for name in _MONEY_COLUMNS}
    for name in _DIGEST_OMIT_WHEN_ZERO:
        if Decimal(str(values[name])) == 0:
            del payload[name]
    return payload


def _money_from_snapshot(snapshot: dict) -> dict:
    """Derive the money columns from the frozen ``snapshot`` and NOTHING else.

    ⭐ ONE DERIVATION, TWO READERS. :func:`close_period` calls this to produce
    the values it WRITES to the columns; :func:`reproduce` calls it again on the
    same frozen document to check the stored columns still agree with it. Before
    this existed the derivation was inline in ``close_period`` and NO reader
    anywhere compared its output against the columns afterwards — so a wrong
    ``total_usd`` was undetectable, because the digest covered only ``snapshot``
    and ``inputs``, which are exactly the two fields an edit to ``total_usd``
    does not touch.

    ⛔ READS THE SNAPSHOT ONLY. No mutable table, no ``generate_billing`` —
    :func:`reproduce` is forbidden both, and this function must not break that
    prohibition on its behalf.

    ⚠️ THE FOUR USAGE/MARKUP COLUMNS ARE SUB-CENT ON PURPOSE, and quantising them
    here would be a defect, not a tidy-up. They are what the tenant's spend and
    uplift ACTUALLY were (``llm_usage_usd`` is asserted against the production
    row sum at full precision in ``tests/test_invoice_from_production_rows.py``),
    and the identity in :func:`assert_total_is_the_sum_of_its_components` is held
    by the TOTAL's fold — ``generate_billing`` adds the access and seat fees in
    as the cents they are charged as — not by rounding these four. Rounding them
    would ADD four independent half-cent errors to a residual that is currently
    one, and push it back outside the tolerance the gate is allowed.
    ``access_charge_usd`` / ``seat_fee_usd`` arrive already quantised because the
    snapshot stores them that way; that asymmetry is the mechanism, not an
    oversight.

    ⚠️ ``excluded_unbillable_usd`` is deliberately ABSENT from the returned dict.
    It comes from :func:`measure_excluded_unbillable`, not from the snapshot, so
    it is not re-derivable here. The DIGEST still covers it (it is in
    :data:`_MONEY_COLUMNS`); the per-field comparison in :func:`reproduce`
    cannot, and that asymmetry is stated there rather than papered over.
    """
    totals = snapshot.get("totals") or {}

    llm_incurred = Decimal("0")
    llm_billed = Decimal("0")
    # ⭐ THE SECOND BILLED STREAM, summed in the SAME loop and from the SAME
    # per-licence `costs` block. `services/billing.py` accumulates
    # `lu["app_request_cost"]` / `lu["app_request_billed"]` per licence and writes
    # them out as `costs.app_request_cost_usd` / `costs.app_request_billed_usd`,
    # then folds `app_request_billed + llm_billed` into ONE
    # `totals.customer_cost_usd`. Deriving only the llm half — which is what this
    # function did before alembic 0033 — left `total_usd` with components that
    # could not add up to it, by exactly the app-request amount.
    app_incurred = Decimal("0")
    app_billed = Decimal("0")
    for entry in snapshot.get("licenses") or []:
        costs = entry.get("costs") or {}
        llm_incurred += Decimal(str(costs.get("llm_cost_usd", "0")))
        llm_billed += Decimal(str(costs.get("llm_billed_usd", "0")))
        # ⚠️ Defaulting to "0" like its llm sibling, and for the same narrow
        # reason: a snapshot frozen before the key existed. It is NOT a licence
        # for a caller to omit the key — `generate_billing` writes both on every
        # entry, so a fresh close never takes the default.
        app_incurred += Decimal(str(costs.get("app_request_cost_usd", "0")))
        app_billed += Decimal(str(costs.get("app_request_billed_usd", "0")))

    customer = Decimal(str(totals.get("customer_cost_usd", "0")))
    return {
        # subtotal == total while the legal set is present-and-null: this PR
        # decides no tax, so there is nothing between the two. A tax slice that
        # writes tax_amount must also stop copying total from subtotal here.
        "subtotal_usd": customer,
        "total_usd": customer,
        "internal_cost_usd": Decimal(str(totals.get("internal_cost_usd", "0"))),
        "margin_usd": Decimal(str(totals.get("margin_usd", "0"))),
        "access_charge_usd": Decimal(str(totals.get("access_charge_usd", "0"))),
        # generate_billing tracks the byo_platform_fee absolute surcharge
        # separately from the billed line it is folded into and reports it as
        # `totals.platform_fee_usd`, so there IS a figure to copy and this is the
        # amount genuinely charged rather than a hardcoded 0.
        #
        # ⭐ The default is "0" only for a snapshot that predates that field, and
        # a $0.00 fee is written EXPLICITLY rather than left to the column's
        # server_default: an absent line cannot be reconciled, a zero one can.
        # ⚠️ This is a COMPONENT of total_usd (it lives inside the billed lines),
        # not an addition to it — do not add it to the total when rendering.
        "platform_fee_usd": Decimal(str(totals.get("platform_fee_usd", "0"))),
        "llm_usage_usd": llm_incurred,
        "llm_markup_usd": llm_billed - llm_incurred,
        # APP-REQUEST axis (alembic 0033), derived identically to the llm pair
        # above so the two streams are reconciled the same way: usage is what the
        # per-request rate INCURRED, markup is what was charged on top, and the
        # pair sums back to the billed stream that entered `customer_cost_usd`.
        #
        # ⚠️ The markup is SIGNED and a negative value is correct, not a bug:
        # under BILLING_MODE_INTERNAL every line bills $0 against a real incurred
        # cost, so this reads `−app_incurred` and the pair nets to zero. Clamping
        # it at 0 would break the close-path identity for every internal tenant.
        "app_request_usage_usd": app_incurred,
        "app_request_markup_usd": app_billed - app_incurred,
        # SEAT axis (2026-09-10). `generate_billing` resolves a per-seat access
        # charge against the tenant's active-licence count, so there IS a figure
        # to copy and these two columns are not structurally empty.
        #
        # ⚠️ `seat_count` stays nullable and `.get("seat_count")` carries NO
        # default on purpose — BillingInvoice.seat_count documents "no per-seat
        # term" and "zero seats" as different facts, and a `0` default here would
        # relabel every snapshot predating this change as a zero-seat tenant.
        "seat_fee_usd": Decimal(str(totals.get("seat_fee_usd", "0"))),
        "seat_count": totals.get("seat_count"),
        # PREPAID CREDIT (alembic 0055): a DEDUCTION, see
        # `_TOTAL_DEDUCTION_COLUMNS`. "0" for a snapshot frozen before it existed,
        # which is exactly what such a statement applied.
        "prepaid_credit_applied_usd": Decimal(str(totals.get("prepaid_credit_applied_usd", "0"))),
    }


#: The component columns that must ADD UP TO ``total_usd``, and the complete set
#: of them. Named once and consumed by
#: :func:`assert_total_is_the_sum_of_its_components` and by its tests, so the set
#: the gate enforces and the set a test believes it enforces cannot drift.
#:
#: ⛔ THIS IS NOT "every money column", AND EACH OMISSION IS A DECISION:
#:
#: * ``subtotal_usd`` EQUALS the total while the legal set is present-and-null —
#:   including it would assert ``total == 2×total``.
#: * ``platform_fee_usd`` is a surcharge folded INSIDE the billed lines, not added
#:   to them (``models/db/invoices.py`` and ``_money_from_snapshot`` both say so).
#:   It is already inside ``llm_markup_usd``/``app_request_markup_usd``; adding it
#:   here would double-count it and refuse every statement carrying a BYO fee.
#: * ``internal_cost_usd``/``margin_usd`` are the COST axis — what the platform
#:   paid and what it kept — not lines the customer is charged.
#: * ``excluded_unbillable_usd`` is spend the statement deliberately did NOT bill;
#:   summing it into a charge would inverse its meaning.
#: * ``seat_count`` is a QUANTITY, not money.
_TOTAL_COMPONENT_COLUMNS: tuple[str, ...] = (
    "access_charge_usd",
    "seat_fee_usd",
    "llm_usage_usd",
    "llm_markup_usd",
    "app_request_usage_usd",
    "app_request_markup_usd",
)

#: How far ``total_usd`` may sit from the sum of its components before the close
#: is refused: ONE cent, which is ``services.markup.MONEY_QUANT``.
#:
#: ⛔ THE TOLERANCE IS DERIVED, NOT CHOSEN, and the derivation below is the ONLY
#: reason one quantum is enough. ⚠️ IT WAS RE-DERIVED WHEN PER-LINE ROUNDING WAS
#: REMOVED — the old derivation rested on an assumption that is now FALSE, and
#: leaving the constant alone while the assumption rotted would have turned a
#: proof into a coincidence.
#:
#: THE OLD DERIVATION (kept, because the difference is the point): every billed
#: LINE used to be ``quantize_money``d, so the two billed streams ``B`` summed to
#: an exact cent multiple; the total ``q(B + A + S)`` equalled ``B + q(A + S)``
#: while the components carried ``B + q(A) + q(S)``. The residual was therefore
#: ``q(A+S) − q(A) − q(S)`` — a CENT MULTIPLE bounded by three half-cent
#: roundings (0.015), hence one of ``{-0.01, 0, +0.01}``, hence inside a cent.
#:
#: ⛔ AND THAT IS EXACTLY WHY REMOVING PER-LINE ROUNDING BROKE IT. With ``B``
#: unrounded the residual stops being a cent multiple and becomes CONTINUOUS on
#: the same ±0.015 interval — measured **−0.0140**, which is not a defect, is not
#: detectable as one, and refused the close outright on a path that has no
#: afterwards (gapless counter, immutable row).
#:
#: THE CURRENT DERIVATION: ``generate_billing`` folds the access and seat fees
#: into the total AS THE CENTS THEY ARE CHARGED AS — ``q(A)`` and ``q(S)``, the
#: very figures written to the ``access_charge_usd`` / ``seat_fee_usd`` columns —
#: so ``total = q(B + q(A) + q(S)) = q(B) + q(A) + q(S)`` because ``q(A)+q(S)`` is
#: already a whole number of cents. The components carry ``B + q(A) + q(S)``. The
#: residual is therefore ``q(B) − B``: ONE rounding, bounded by HALF a quantum,
#: and — unlike before — completely independent of whether the access and seat
#: fees carry sub-cent scale.
#:
#: ⭐ THE GATE IS THEREFORE STRICTLY STRONGER THAN IT WAS, not weaker: the honest
#: residual it must tolerate fell from a whole cent to half of one, while the
#: constant stayed put. ⛔ Loosening the constant was the alternative and it is
#: the wrong one — it would blind the whole-dropped-stream detector this exists
#: for by exactly the amount it was loosened.
#:
#: ⚠️ SO THE GATE CANNOT DETECT A DEFECT WORTH ONE CENT OR LESS, and that limit is
#: stated rather than hidden. It is the right trade: the defect this exists for is
#: a whole dropped billed STREAM, which is the app-request charge for an entire
#: tenant-month — orders of magnitude above a cent the instant any
#: ``billing_app_cost_rates`` row is non-zero. Tightening to half a quantum would
#: buy nothing and would refuse any honest statement whose snapshot predates the
#: fee fold above.
_TOTAL_IDENTITY_TOLERANCE_USD = MONEY_QUANT

#: Columns SUBTRACTED from the components: ``total_usd = Σ components - Σ these``.
#: Prepaid credit (alembic 0055) is whole cents by construction, so it adds no
#: rounding to the residual derived above.
_TOTAL_DEDUCTION_COLUMNS: tuple[str, ...] = ("prepaid_credit_applied_usd",)


def assert_total_is_the_sum_of_its_components(
    money: dict,
    *,
    tenant_id: str,
    month: str,
) -> None:
    """Refuse a close whose ``total_usd`` its own component columns cannot explain.

    ⭐ WHY A GATE AND NOT A TEST. The identity held only by CONVENTION before
    alembic 0033 — and it was false, by exactly the app-request billed amount,
    for any tenant-month priced on a non-zero ``billing_app_cost_rates`` row.
    Nothing detected that: ``reproduce``'s ``money_matches_snapshot`` re-derives
    the columns with :func:`_money_from_snapshot` and compares the result against
    the columns THAT SAME FUNCTION produced, so a term missing from the derivation
    is missing from both sides of the comparison and the check agrees with itself.
    A convention that no instrument can falsify is not an invariant; this makes it
    one, on the only path that writes the row.

    ⛔ IT RUNS ON THE CLOSE PATH, BEFORE ANYTHING PERMANENT EXISTS, because there
    is no afterwards. The row is immutable (``invoice_immutability``) and its
    number comes from a GAPLESS counter, so an unbalanced statement cannot be
    edited, and voiding one burns audit evidence. The only moment this is cheap is
    before the INSERT.

    ⚠️ IT CHECKS THE FIGURES ABOUT TO BE WRITTEN, NOT THE SNAPSHOT. ``money`` is
    :func:`_money_from_snapshot`'s output as :func:`close_period` will store it —
    which is what makes this an independent check rather than a second reading of
    the same derivation. If the derivation drops a stream, the sum here does not
    reach the total and the close refuses; that is precisely the mutation it is
    written to kill.

    :raises InvoiceMoneyImbalanceError: if the residual exceeds one money quantum.
        See :data:`_TOTAL_IDENTITY_TOLERANCE_USD` for why the tolerance is one
        cent and what that means the gate cannot see.
    """
    total = Decimal(str(money["total_usd"]))
    # ⚠️ KeyError, not `.get(..., 0)`. A component absent from `money` is a money
    # column that was never derived, and defaulting it to zero here would let the
    # identity pass over the exact omission this gate exists to catch — the same
    # failure shape `_money_payload` refuses for the digest.
    components = {name: Decimal(str(money[name])) for name in _TOTAL_COMPONENT_COLUMNS}
    deductions = {name: Decimal(str(money[name])) for name in _TOTAL_DEDUCTION_COLUMNS}
    residual = (
        total - sum(components.values(), Decimal("0")) + sum(deductions.values(), Decimal("0"))
    )

    if abs(residual) <= _TOTAL_IDENTITY_TOLERANCE_USD:
        return

    # Structured BEFORE the raise so the figures are greppable even where the
    # caller only records the exception's type — the sweep's `except InvoiceError`
    # arm logs a traceback, and a traceback is not a place to read six amounts
    # out of. Mirrors the no-billable-basis skip's logging in `close_period`.
    logger.error(
        "invoice_money_identity_violation",
        tenant_id=tenant_id,
        month=month,
        total_usd=str(total),
        residual_usd=str(residual),
        **{name: str(value) for name, value in components.items()},
    )
    raise InvoiceMoneyImbalanceError(tenant_id, month, total, components, residual)


def _digest(snapshot: dict, inputs: dict, money: dict) -> str:
    """``sha256`` over the canonical JSON of the frozen pair AND the money columns.

    ``sort_keys`` + tight separators are load-bearing: the digest has to be
    recomputable byte-for-byte by a later reader (and by :func:`reproduce`),
    which it cannot be if key order or whitespace is left to the encoder.

    ⭐ ``money`` IS WHY THIS SIGNATURE HAS THREE ARGUMENTS. The digest used to
    cover ``snapshot`` + ``inputs`` only, which are the generator's answer and
    the world it read — neither of which changes when someone edits
    ``total_usd``. The money columns are DERIVED from the snapshot once and then
    stored as copies, and nothing compared a copy against its source, so a wrong
    invoice total was not detectable by any code path in the repo. Binding the
    copies into the same hash as their source is what makes the stored figure
    falsifiable.

    ``money`` is a REQUIRED positional argument and not an optional one on
    purpose: a default would let a third call site be added that digests a pair
    with no money in it, re-opening the hole silently and leaving the two
    existing call sites looking fine.
    """
    payload = json.dumps(
        {"snapshot": snapshot, "inputs": inputs, "money": _money_payload(money)},
        sort_keys=True,
        separators=(",", ":"),
    )
    return hashlib.sha256(payload.encode()).hexdigest()


def _month_str(period_start: date, period_end: date) -> str:
    """Validate the period is one whole calendar month and return its ``YYYY-MM``.

    ``period_end`` is EXCLUSIVE, matching ``BillingInvoice``'s half-open interval.
    """
    if period_start.day != 1:
        raise InvoicePeriodError(
            f"period_start {period_start} is not the first of a month; "
            "generate_billing computes whole calendar months only"
        )
    expected_end = (
        date(period_start.year + 1, 1, 1)
        if period_start.month == 12
        else date(period_start.year, period_start.month + 1, 1)
    )
    if period_end != expected_end:
        raise InvoicePeriodError(
            f"period [{period_start}, {period_end}) is not one calendar month "
            f"(expected exclusive end {expected_end}); generate_billing derives "
            "its own bounds from a YYYY-MM month and cannot compute a partial period"
        )
    return f"{period_start.year:04d}-{period_start.month:02d}"


def _bill_ts(period_end: date) -> datetime:
    """The last instant of the billed period — the resolution point for every
    effective-dated lookup, copied from ``generate_billing`` so the frozen
    ``inputs`` are resolved at exactly the instant the figures were."""
    return datetime(period_end.year, period_end.month, period_end.day, tzinfo=UTC) - timedelta(
        microseconds=1
    )


# --------------------------------------------------------------------------- #
# Source-completeness gate
# --------------------------------------------------------------------------- #


async def assert_no_bucket_gap(
    session: AsyncSession,
    *,
    tenant_id: str,
    period_start: date,
    period_end: date,
) -> None:
    """Refuse to close a period any of whose days has NO cost rollup row at all.

    ⚠️ WHAT THIS DETECTS, AS A COMPLETE STATEMENT OF ITS POWER

    (a) It detects an ENTIRELY ABSENT bucket: a ``bucket_date`` in
        ``[period_start, period_end)`` for which ``billing_cost_daily`` holds
        zero rows fleet-wide. Measured precedent: 2026-09-02, 09-03 and 09-04
        have no rows at all, with 08-25→09-01 present at 29-41 rows/day and
        09-05 resuming. A ``pg_dump`` taken 2026-09-05T20:08Z already ends at
        09-01, so the rollup never wrote those buckets. Closing a period
        spanning them would under-bill by roughly $900-1,200 with nothing on the
        statement to indicate it.

    (b) It does NOT detect a bucket that was WRITTEN SHORT — a day whose rollup
        ran but covered only part of the day. That is not a caveat, it is a
        measured counter-example: 2026-09-05 holds 32 rows / 47,071 requests /
        $257.19, squarely inside the normal 25-42 rows/day band, while
        ``billing_cost_events`` was TRUNCATE'd at 07:01:58Z that day — so the
        bucket covers roughly 17 of 24 hours. A presence check sails straight
        over it, and a partial rollup is indistinguishable from a quiet day.

    (c) The reason (b) is undetectable BY ANY MEANS CURRENTLY AVAILABLE is an
        absence in the schema, not a shortcut taken here: ``billing_cost_daily``
        carries no ``updated_at``, no ``created_at`` and no coverage-window
        column, and a schema-wide search for any column or table matching
        watermark/covered/window returns nothing. There is no rollup watermark
        to compare against. Catching a written-short bucket needs one.

    ⛔ Do NOT substitute row count as a completeness proxy. It is close to
    uncorrelated here — rows are per (tenant, licence, service_key, provider, …)
    grouping, not per hour — and 2026-09-06 has FEWER rows than the truncated
    09-05 while being complete. A row-count floor would have passed 09-05 and
    would fail honest low-traffic days.

    ⚠️ THE PRESENCE TEST IS FLEET-WIDE, NOT TENANT-SCOPED, and that is deliberate.
    A single tenant legitimately has quiet days, so "this tenant wrote no row on
    the 3rd" is not evidence of anything. "NOBODY wrote a row on the 3rd" is: it
    means the rollup did not run. ``tenant_id`` is carried for the log line and
    for the exception, and its own per-date counts are logged as context.

    Refusing is correct whether a bucket was never written or was lost after the
    fact; this function deliberately makes no claim about which.
    """
    days = (period_end - period_start).days
    if days <= 0:
        raise InvoicePeriodError(
            f"period [{period_start}, {period_end}) is empty or inverted; "
            "period_end is EXCLUSIVE and must be after period_start"
        )
    expected = {period_start + timedelta(days=i) for i in range(days)}

    present_stmt = (
        select(CostDaily.bucket_date)
        .where(
            CostDaily.bucket_date >= period_start,
            CostDaily.bucket_date < period_end,
        )
        .group_by(CostDaily.bucket_date)
    )
    present = {row.bucket_date for row in (await session.execute(present_stmt)).all()}

    missing = sorted(expected - present)
    if not missing:
        return

    tenant_stmt = (
        select(CostDaily.bucket_date, func.count().label("rows"))
        .where(
            CostDaily.tenant_id == tenant_id,
            CostDaily.bucket_date >= period_start,
            CostDaily.bucket_date < period_end,
        )
        .group_by(CostDaily.bucket_date)
    )
    tenant_counts = {
        r.bucket_date.isoformat(): r.rows for r in (await session.execute(tenant_stmt)).all()
    }
    logger.error(
        "invoice_close_refused_bucket_gap",
        tenant_id=tenant_id,
        period_start=period_start.isoformat(),
        period_end=period_end.isoformat(),
        missing_dates=[d.isoformat() for d in missing],
        tenant_rows_by_date=tenant_counts,
    )
    observability_metrics.invoice_close_refused_total.labels(reason="bucket_gap").inc()
    raise BucketGapError(tenant_id, missing)


async def assert_tenant_invoiceable(
    *,
    tenant_id: str,
    settings: Settings,
    token_provider: ServiceTokenProvider | None = None,
) -> TenantInfo:
    """Refuse to close a period for a tenant that must not be invoiced.

    ⭐ FIRST AMONG THE GATES, ahead of even :func:`assert_no_bucket_gap`, and the
    ordering is the same argument the others make one step further on: refuse
    while refusing is still free. The bucket-gap, pricing, seat and byo_mode gates
    all ask "can this period be stated honestly FOR THIS TENANT" -- a question
    worth no query at all if the tenant should receive no statement. This gate
    costs one cached HTTP call for the whole sweep (:data:`_name_cache` holds a
    single entry keyed for the entire registry), so putting it first makes the
    sweep cheaper as well as safer.

    Two refusals, deliberately distinguished by ``reason`` rather than merged:

    ``tenant_unregistered``
        The registry answered and this id is not in it. ⛔ Do NOT "fix" this by
        having :func:`services.byo_mode.get_byo_mode` stop defaulting -- its 404
        -> ``byo_required`` is correct for the LIVE access gate, where the
        alternative lets an unknown caller spend platform GPU for free. The two
        callers want opposite answers to the same 404, which is exactly why the
        close asks its own question here instead of changing theirs.

    ``tenant_archived``
        The registry answered, the row exists, and it is retired. Archiving
        revokes nothing upstream by design, so such a tenant keeps accruing cost
        rows -- which is why this has to be asked at close time rather than
        assumed away.

    :raises TenantNotInvoiceableError: either refusal above. Counted as
        ``refused`` by :func:`close_due_periods`; draws NO invoice number; leaves
        the period OPEN, which is a visible state the next sweep retries.
    :raises services.tenant_names.TenantRegistryUnavailableError: the registry
        could not answer. ⚠️ This refuses EVERY tenant in the sweep, and that is
        the correct direction but it is not free: a licences outage stops the
        month closing until it ends. The alternative -- proceeding on a degraded
        map -- would commercially invoice the entire estate, because a fail-open
        ``{}`` makes every tenant look unregistered. See
        :func:`~services.tenant_names.get_tenant_info_strict` for why the strict
        reader exists at all.
    :returns: the registry row, so the caller can log what it allowed on. A bare
        ``None`` would make "the gate passed" and "the gate ran" the same
        observation -- the same reason :func:`assert_no_unpriced_provider_cost`
        returns its coverage.
    """
    registry = await get_tenant_info_strict(settings, token_provider)
    info = registry.get(tenant_id.lower())

    if info is None:
        logger.warning(
            "invoice_close_refused_tenant_unregistered",
            tenant_id=tenant_id,
            registry_rows=len(registry),
        )
        observability_metrics.invoice_close_refused_total.labels(reason="tenant_unregistered").inc()
        raise TenantNotInvoiceableError(
            tenant_id,
            "tenant_unregistered",
            f"absent from a registry of {len(registry)} rows",
        )

    if info.archived:
        logger.warning(
            "invoice_close_refused_tenant_archived",
            tenant_id=tenant_id,
            tenant_name=info.name,
            kind=info.kind,
        )
        observability_metrics.invoice_close_refused_total.labels(reason="tenant_archived").inc()
        raise TenantNotInvoiceableError(
            tenant_id, "tenant_archived", f"name={info.name} kind={info.kind}"
        )

    logger.info(
        "invoice_close_tenant_invoiceable",
        tenant_id=tenant_id,
        tenant_name=info.name,
        kind=info.kind,
        cost_centre=info.cost_centre,
    )
    return info


async def assert_seat_count_available(
    session: AsyncSession,
    *,
    tenant_id: str,
    settings: Settings,
    period_start: date,
    period_end: date,
) -> None:
    """Refuse to close a period whose per-seat charge has no countable seats.

    Mirrors :func:`assert_no_bucket_gap` and
    :func:`assert_all_service_keys_priced`: a precondition that refuses while
    refusing is still free, before any row exists to clean up.

    ⭐ THE SHAPE THIS PREVENTS IS A FABRICATED ZERO, not an outage. If licences
    cannot state a seat count and the close proceeded anyway, the invoice freezes
    ``seat_fee_usd = 0.00`` with ``seat_count = 0`` — indistinguishable from a
    tenant that genuinely has no seats. Nothing downstream can repair it: there is
    no "recount the seats on closed invoices" pass, and the tenant holds a
    statement that under-bills with no marker saying so. A refusal is loud, costs
    nothing, and can be retried the moment licences is back.

    ⚠️ SCOPED TO TENANTS THAT ACTUALLY HAVE A PER-SEAT TERM. A licences outage
    must not block closing the invoices of tenants billed on usage alone — that
    would convert a narrow dependency into an estate-wide one. Measured
    2026-09-10: 0 rows in `tenant_access_charge`, so today this gate is inert for
    every tenant and arms per-tenant as terms are materialised.
    """
    # ⚠️ bill_ts is the LAST INSTANT of the period, not its midnight boundary.
    # `period_end` is EXCLUSIVE (BillingInvoice's half-open interval), and a
    # charge is only effective from its own `effective_from` — so probing at
    # `datetime.combine(period_end, time.min)` would sit BEFORE a charge created
    # during the period and find nothing, making this gate silently inert for
    # exactly the tenants it exists to protect. (That bug was written, and caught
    # by a test that asserted the RAISE rather than merely "no exception".) Same
    # convention as `generate_billing` and `compute_billing_overview`.
    bill_ts = datetime.combine(period_end, time.min, tzinfo=UTC) - timedelta(microseconds=1)
    rows = await active_access_charges_for_month(
        session,
        tenant_id=tenant_id,
        month_start=period_start,
        month_end=period_end,
        bill_ts=bill_ts,
    )
    per_seat = [r for r in rows if r.cadence == "per_seat"]
    if not per_seat:
        return
    # Probe EVERY distinct scope the report path will bill (see
    # `services.seats.seat_scope_for_charge`): a gate that checked only the unscoped count would
    # let a close through whose pinned per-seat term licences cannot answer.
    scopes = {
        r.service_key or "": seat_scope_for_charge(r.service_key, period_start, period_end)
        for r in per_seat
    }

    try:
        # ⭐ `as_of=bill_ts` — the LAST INSTANT OF THE PERIOD, not now. The licenses
        # endpoint's own parameter description is explicit: "A CLOSED billing period
        # must be counted at its own instant, never at whenever the query is
        # re-run." Without it, closing August in September bills August at
        # September's seat count, and a re-close after a licence is added or
        # expires silently produces a different answer for the same month.
        #
        # ⚠️ It cannot reconstruct a licence that has since been DELETED — licences
        # has no history table, so this filters the current rows by their own
        # starts_at/expires_at. That is exactly why the count is frozen into the
        # invoice snapshot: the frozen number, not this endpoint, is the
        # historical record.
        seats = {
            key: await get_seat_count(tenant_id, settings, as_of=bill_ts, **scope)
            for key, scope in scopes.items()
        }
    except SeatCountUnavailableError as exc:
        logger.warning(
            "invoice_close_refused_seat_count_unavailable",
            tenant_id=tenant_id,
            error=str(exc),
        )
        observability_metrics.invoice_close_refused_total.labels(
            reason="seat_count_unavailable"
        ).inc()
        raise
    logger.info(
        "invoice_close_seat_count_resolved",
        tenant_id=tenant_id,
        seats=seats,
        as_of=bill_ts.isoformat(),
    )


async def assert_byo_mode_available(
    *,
    tenant_id: str,
    settings: Settings,
) -> str:
    """Refuse to close a period whose ``byo_mode`` licences cannot state.

    The sibling of :func:`assert_seat_count_available`, and the same argument
    about the same upstream service — but about a larger number.

    ⭐ WHAT THIS PREVENTS IS A FABRICATED *COMMERCIAL* INVOICE. ``byo_mode``
    decides, via :func:`services.byo_mode.effective_billing_mode`, whether a
    tenant is billed at all: ``local_allowed``/``platform_only`` collapse to
    ``BILLING_MODE_INTERNAL`` and bill $0, while ``byo_required`` keeps the
    configured mode (default ``standard_markup`` — 20% markup *plus* the access
    charges). ``services.byo_mode.get_byo_mode`` defaults to ``byo_required`` on
    a lookup failure, which is right for the LIVE gate (a transient decision,
    and the alternative gives platform GPU away for free) and wrong here, where
    the output is an immutable row drawn from a gapless invoice series. Measured
    2026-09-11: of 59 unarchived tenants **49** carry
    ``local_allowed``/``platform_only``, so one licences blip mid-close could
    mint permanent commercial statements — markup plus a $5,000 flat access
    charge — for up to 49 internal cost centres, with no pass anywhere that
    recomputes the mode of a closed invoice.

    ⚠️ UNLIKE SEATS, THIS GATE IS UNCONDITIONAL, and the difference is not an
    oversight. ``assert_seat_count_available`` is scoped to tenants that actually
    carry a ``cadence='per_seat'`` term because a seat count is only an input to
    one charge line; ``byo_mode`` is an input to whether ANY line is billed, so
    there is no tenant for which an unknown answer is harmless. There is
    therefore no cheap DB pre-check to skip the lookup with.

    ⭐ AND IT IS A CACHE PRIME, NOT ONLY A GATE — see the call site in
    :func:`close_period`. On success the real answer is in the module's 5-minute
    TTL cache before ``_capture_inputs`` and ``generate_billing`` resolve it with
    the DEFAULTING resolver, so if licences dies between the gate and those
    calls they read the cached real answer instead of guessing ``byo_required``.
    That is the TOCTOU window the ordering closes; it is not closable by the gate
    alone, because neither of those callers takes an injected mode.

    :returns: the resolved ``byo_mode``, for the caller to log. The value is
        deliberately NOT threaded into the generator — see the call site.
    :raises services.byo_mode.ByoModeUnavailableError: if licences cannot answer.
    """
    try:
        byo_mode = await get_byo_mode_strict(tenant_id, settings)
    except ByoModeUnavailableError as exc:
        logger.warning(
            "invoice_close_refused_byo_mode_unavailable",
            tenant_id=tenant_id,
            error=str(exc),
        )
        observability_metrics.invoice_close_refused_total.labels(
            reason="byo_mode_unavailable"
        ).inc()
        raise
    logger.info("invoice_close_byo_mode_resolved", tenant_id=tenant_id, byo_mode=byo_mode)
    return byo_mode


async def assert_all_service_keys_priced(
    session: AsyncSession,
    *,
    tenant_id: str,
    period_start: date,
    period_end: date,
    app_rates: dict[str, Decimal] | None = None,
) -> None:
    """Refuse to close a period in which a BILLED ``service_key`` carries no app rate.

    The GATE counterpart of :func:`_unpriced_service_keys`, which is only a
    RECORDER: that helper writes the unpriced key set into the frozen ``inputs``
    so an already-closed statement is provable after the fact, while this
    function stops the statement from being written at all. Both are needed, and
    neither substitutes for the other — see :class:`UnpricedServiceKeyError` for
    why a fabricated $0.00 app line cannot be allowed onto an immutable row
    numbered from a gapless series.

    ⚠️ ``request_count > 0`` IS LOAD-BEARING, not a tidy-up. A key with zero
    requests bills ``rate * 0 == 0`` whatever its rate is, so an absent rate row
    cannot fabricate a charge for it and refusing on it would be a FALSE refusal
    — a tenant blocked from closing over a service it did not use this period.
    :func:`_unpriced_service_keys` deliberately does NOT apply this filter (a
    disclosure should over-report), which is why this guard runs its own query
    rather than reading that helper's answer.

    ⚠️ THE SYSTEM/S2S SENTINEL IS EXCLUDED, and this mirrors the generator
    rather than inventing a predicate: ``generate_billing``'s usage loop does
    ``if is_system_license(row.license_id): continue`` (``services/billing.py:516``)
    BEFORE it touches ``app_rates``, so a key seen only under ``NULL_LICENSE_ID``
    never produces an app line and an absent rate cannot fabricate anything
    for it. The sentinel carries the majority of measured spend, so including it
    here would refuse closes that are not in fact defective.

    ``app_rates`` is the OPTIONAL injected rate snapshot. Omitted (a direct
    caller, a test), this reads the table itself as before; :func:`close_period`
    passes the same mapping the generator will bill on, so this gate cannot
    allow a close on a rate set the invoice is not then computed from.
    """
    # `is None`, not falsiness — an empty rate set is the measured prod state and
    # a real answer. See the matching note in services/billing.py.
    if app_rates is None:
        app_rates = await get_app_cost_rates(session)

    usage_stmt = (
        select(
            MeteringDaily.service_key,
            func.coalesce(func.sum(MeteringDaily.request_count), 0).label("requests"),
        )
        .where(
            MeteringDaily.tenant_id == tenant_id,
            MeteringDaily.bucket_date >= period_start,
            MeteringDaily.bucket_date < period_end,
            # System/S2S usage is skipped by the generator before the rate
            # lookup, so it cannot be under-priced. See the docstring.
            MeteringDaily.license_id != NULL_LICENSE_ID,
        )
        .group_by(MeteringDaily.service_key)
    )
    billed = {
        r.service_key: int(r.requests or 0) for r in (await session.execute(usage_stmt)).all()
    }

    # requests > 0 only: a key nobody called bills zero at ANY rate.
    unpriced = sorted(k for k, reqs in billed.items() if reqs > 0 and k not in app_rates)
    if not unpriced:
        return

    for key in unpriced:
        # ⚠️ surface="close_gate" is a NEW third value, not a reuse of "invoice".
        # This guard runs BEFORE generate_billing, so the same gap would
        # otherwise be counted twice under one surface — and the metric's own
        # help text is explicit that `surface` names WHERE the miss was
        # observed, because collapsing surfaces hides a miss that only one path
        # reaches.
        observability_metrics.app_rate_missing_total.labels(
            service_key=key or "unknown", surface="close_gate"
        ).inc()

    logger.error(
        "invoice_close_refused_unpriced_service_keys",
        tenant_id=tenant_id,
        period_start=period_start.isoformat(),
        period_end=period_end.isoformat(),
        unpriced_service_keys=unpriced,
        requests_by_service_key={k: billed[k] for k in unpriced},
        priced_keys_in_force=sorted(app_rates),
    )
    raise UnpricedServiceKeyError(tenant_id, unpriced)


async def assert_no_unpriced_provider_cost(
    session: AsyncSession,
    *,
    tenant_id: str,
    period_start: date,
    period_end: date,
) -> UnpricedCostCoverage:
    """Refuse to close a period whose provider spend could not be PRICED.

    The sibling of :func:`assert_all_service_keys_priced`, for the other half of
    the money and a different table — see :class:`UnpricedProviderCostError` for
    why the two are separate errors and must stay so.
    ``rollup_engine.py``'s comment on the counter this reads is the specification:
    it is "the signal an invoice close can REFUSE on instead of billing an unknown
    as zero", and it records that the app-rate gate "cannot see a missing PROVIDER
    price at all".

    ⭐ WHAT MAKES THE COUNT TRUSTWORTHY AS A GATE INPUT is that
    ``rollup_engine`` computes it INSIDE the same aggregate that computes
    ``total_cost_usd`` (``sum(case((cost_usd IS NULL, 1), else_=0))``), so it can
    never describe a different row set than the one the invoice bills. A second
    query over ``billing_cost_events`` would be free to answer about a different
    population — a concurrent flush, a reprice landing between two statements —
    and its number would then agree with nothing.

    ⚠️ THE FLOOR GATES THE CLEAN VERDICT, NOT THE REFUSAL, and the asymmetry is
    the point:

    * ``unpriced_event_count > 0`` REFUSES at ANY ``bucket_date``. A non-zero can
      only have been written by code that knows the column exists, so it is a
      measurement wherever it sits, and excusing an old one would be discarding
      the only honest reading available.
    * ``unpriced_event_count == 0`` is believed only at/after
      :data:`_UNPRICED_EVENT_COUNT_MEASURED_FROM`. Below it the 0 may be alembic
      0031's ``server_default`` on a row nobody ever measured, and a pass over
      those rows is reported as :class:`UnpricedCostCoverage`
      ``unmeasurable_rows`` rather than counted as clean.

    ⛔ AN UNMEASURABLE DAY IS REPORTED, NOT REFUSED, and that is a deliberate
    choice rather than a softened gate. Refusing on ``bucket_date < floor`` would
    make EVERY period before 2026-09-12 permanently uncloseable — there is no
    backfill that can ever clear it (0031 explains why one would be a fabrication,
    and ``billing_cost_events``' partitions for those days are gone), so the
    refusal would have no remedy and would convert a measurement gap into a
    billing outage. What it must not do instead is pass SILENTLY, which is why the
    verdict is a value and why the log line below names the dates.

    ⚠️ SCOPED WITH ``CostDaily.license_attributed_clause()`` — the repo's own
    canonical predicate, the exact one ``generate_billing`` bills on, reused rather
    than re-spelled here so the gate and the generator cannot drift into
    disagreeing about which rows are even on the invoice. System/S2S spend under
    ``NULL_LICENSE_ID`` never becomes a customer line, so an unknown cost there
    cannot fabricate a customer's figure and refusing on it would be a FALSE
    refusal (the sentinel carries the majority of measured spend — the same
    argument :func:`assert_all_service_keys_priced` makes for its own exclusion).

    ⚠️ NO METRIC IS INCREMENTED HERE, unlike the app-rate gate, and the reason is
    specific: ``observability.metrics.pricing_miss_total`` is the INGEST path's
    counter (one increment site, ``services/pricing.py``'s ``row is None``
    branch) and ``MeteringPricingMiss`` alerts on
    ``increase(billing_pricing_miss_total[1h]) > 0``. It carries no ``surface``
    label to separate an observation from an occurrence, so incrementing it from a
    close gate would forge fresh ingest misses every time a sweep retried an old
    period — one metric name meaning two opposite things. Adding a counter of its
    own belongs with ``observability/metrics.py``, which this change does not own.

    :returns: the coverage verdict, for the caller to log.
    :raises UnpricedProviderCostError: if any in-scope group carries unpriced events.
    """
    stmt = (
        select(
            CostDaily.bucket_date,
            CostDaily.provider,
            func.coalesce(func.sum(CostDaily.unpriced_event_count), 0).label("unpriced"),
            func.count().label("rows"),
        )
        .where(
            CostDaily.tenant_id == tenant_id,
            CostDaily.bucket_date >= period_start,
            CostDaily.bucket_date < period_end,
            CostDaily.license_attributed_clause(),
        )
        .group_by(CostDaily.bucket_date, CostDaily.provider)
        .order_by(CostDaily.bucket_date, CostDaily.provider)
    )
    groups = (await session.execute(stmt)).all()

    offending = [g for g in groups if int(g.unpriced or 0) > 0]
    if offending:
        dates = sorted({g.bucket_date for g in offending})
        providers = sorted({g.provider or "unknown" for g in offending})
        logger.error(
            "invoice_close_refused_unpriced_provider_cost",
            tenant_id=tenant_id,
            period_start=period_start.isoformat(),
            period_end=period_end.isoformat(),
            unpriced_dates=[d.isoformat() for d in dates],
            unpriced_providers=providers,
            unpriced_events_by_date_provider={
                f"{g.bucket_date.isoformat()}/{g.provider}": int(g.unpriced) for g in offending
            },
            measured_from=_UNPRICED_EVENT_COUNT_MEASURED_FROM.isoformat(),
        )
        raise UnpricedProviderCostError(tenant_id, dates, providers)

    unmeasurable = [g for g in groups if g.bucket_date < _UNPRICED_EVENT_COUNT_MEASURED_FROM]
    coverage = UnpricedCostCoverage(
        measured_rows=sum(
            int(g.rows) for g in groups if g.bucket_date >= _UNPRICED_EVENT_COUNT_MEASURED_FROM
        ),
        unmeasurable_rows=sum(int(g.rows) for g in unmeasurable),
        unmeasurable_dates=sorted({g.bucket_date for g in unmeasurable}),
    )
    if coverage.unmeasurable_rows:
        # ⚠️ WARNING, not info: this close is about to freeze a figure that NOTHING
        # has checked for a pricing miss. It is allowed (see the docstring) and it
        # must not be quiet.
        logger.warning(
            "invoice_close_unpriced_cost_unmeasurable",
            tenant_id=tenant_id,
            period_start=period_start.isoformat(),
            period_end=period_end.isoformat(),
            measured_from=_UNPRICED_EVENT_COUNT_MEASURED_FROM.isoformat(),
            unmeasurable_rows=coverage.unmeasurable_rows,
            measured_rows=coverage.measured_rows,
            unmeasurable_dates=[d.isoformat() for d in coverage.unmeasurable_dates],
        )
    return coverage


# --------------------------------------------------------------------------- #
# Disclosure
# --------------------------------------------------------------------------- #


async def measure_excluded_unbillable(
    session: AsyncSession,
    *,
    tenant_id: str,
    period_start: date,
    period_end: date,
) -> dict:
    """Sum + count the cost rows this tenant's invoice DROPS, per attribution status.

    ⛔ DISCLOSURE ONLY. Nothing here may be added to ``subtotal_usd`` or
    ``total_usd``. Deciding who pays for unlicensed spend is a business call and
    this module does not make it — see
    ``CostDaily.unlicensed_attributable_clause``'s "⛔ NOT an invoice predicate".

    The excluded population is defined as the complement of the repo's own
    ``CostDaily.license_attributed_clause()`` — the exact predicate
    ``generate_billing`` bills on — rather than by a fresh predicate written
    here, so the two cannot drift into disagreeing about what was dropped. The
    two named sub-populations are read through
    ``unlicensed_attributable_clause()`` and ``system_cost_clause()`` for the
    same reason.

    Returns Decimals (the caller writes some of them to ``Numeric`` columns);
    ``_jsonable`` handles the JSONB boundary.
    """
    period = (
        CostDaily.tenant_id == tenant_id,
        CostDaily.bucket_date >= period_start,
        CostDaily.bucket_date < period_end,
    )

    by_status_stmt = (
        select(
            CostDaily.attribution_status,
            func.coalesce(func.sum(CostDaily.total_cost_usd), 0).label("usd"),
            func.count().label("rows"),
        )
        .where(*period, not_(CostDaily.license_attributed_clause()))
        .group_by(CostDaily.attribution_status)
        .order_by(CostDaily.attribution_status)
    )
    by_status: dict[str, dict[str, Any]] = {}
    total_usd = Decimal("0")
    total_rows = 0
    for row in (await session.execute(by_status_stmt)).all():
        usd = Decimal(row.usd or 0)
        by_status[row.attribution_status] = {"usd": usd, "rows": int(row.rows)}
        total_usd += usd
        total_rows += int(row.rows)

    async def _clause_total(clause: Any) -> dict[str, Any]:
        stmt = select(
            func.coalesce(func.sum(CostDaily.total_cost_usd), 0).label("usd"),
            func.count().label("rows"),
        ).where(*period, clause)
        r = (await session.execute(stmt)).one()
        return {"usd": Decimal(r.usd or 0), "rows": int(r.rows)}

    return {
        "total_usd": total_usd,
        "row_count": total_rows,
        "by_status": by_status,
        # A real customer's spend that reached billing with no licence. Visible,
        # attributable, and NOT charged.
        "unlicensed_attributable": await _clause_total(CostDaily.unlicensed_attributable_clause()),
        # Genuine system/S2S cost with no customer to attribute it to.
        "system_cost": await _clause_total(CostDaily.system_cost_clause()),
    }


# --------------------------------------------------------------------------- #
# Input capture
# --------------------------------------------------------------------------- #


async def _rollup_watermarks(
    session: AsyncSession,
    *,
    tenant_id: str,
    period_start: date,
    period_end: date,
) -> dict:
    """Per-``bucket_date`` shape of the two rollups this tenant's bill was read from.

    ⚠️ These are MUTATION detectors, not completeness proofs, and the distinction
    is forced by the schema rather than chosen: neither ``billing_cost_daily``
    nor ``billing_metering_daily`` carries ``updated_at``, ``created_at`` or any
    coverage-window column, so there is no watermark to freeze. What CAN be
    frozen is the shape the buckets had at close time — row count plus the
    summed figures — and a later re-read that disagrees proves the source moved
    after the statement was issued. It cannot prove the source was whole when it
    was read. See :func:`assert_no_bucket_gap` (c).
    """
    cost_stmt = (
        select(
            CostDaily.bucket_date,
            func.count().label("rows"),
            func.coalesce(func.sum(CostDaily.total_cost_usd), 0).label("cost_usd"),
            func.coalesce(func.sum(CostDaily.request_count), 0).label("requests"),
        )
        .where(
            CostDaily.tenant_id == tenant_id,
            CostDaily.bucket_date >= period_start,
            CostDaily.bucket_date < period_end,
        )
        .group_by(CostDaily.bucket_date)
        .order_by(CostDaily.bucket_date)
    )
    metering_stmt = (
        select(
            MeteringDaily.bucket_date,
            func.count().label("rows"),
            func.coalesce(func.sum(MeteringDaily.request_count), 0).label("requests"),
            func.coalesce(func.sum(MeteringDaily.denied_count), 0).label("denied"),
        )
        .where(
            MeteringDaily.tenant_id == tenant_id,
            MeteringDaily.bucket_date >= period_start,
            MeteringDaily.bucket_date < period_end,
        )
        .group_by(MeteringDaily.bucket_date)
        .order_by(MeteringDaily.bucket_date)
    )
    return {
        "note": (
            "row/sum shape per bucket_date at close time. Detects post-close "
            "MUTATION of the source. Cannot detect a bucket written short: no "
            "updated_at / created_at / coverage-window column exists on either "
            "rollup table."
        ),
        "cost_daily": [
            {
                "bucket_date": r.bucket_date,
                "row_count": int(r.rows),
                "total_cost_usd": Decimal(r.cost_usd or 0),
                "request_count": int(r.requests or 0),
            }
            for r in (await session.execute(cost_stmt)).all()
        ],
        "metering_daily": [
            {
                "bucket_date": r.bucket_date,
                "row_count": int(r.rows),
                "request_count": int(r.requests or 0),
                "denied_count": int(r.denied or 0),
            }
            for r in (await session.execute(metering_stmt)).all()
        ],
    }


async def _unpriced_service_keys(
    session: AsyncSession,
    *,
    tenant_id: str,
    period_start: date,
    period_end: date,
    app_rates: dict[str, Decimal],
) -> dict:
    """Service keys billed in this period that have NO ``AppCostRate`` row.

    WHY THIS IS RECORDED. ``services/billing.py`` resolves the per-request app
    charge with ``app_rates.get(row.service_key, Decimal("0"))`` — an unmapped
    key therefore invoices at **$0 silently**. And ``services/markup.py`` gates
    the zero-margin alarm on ``incurred_usd > 0``, so the fabricated $0 also
    suppresses the alarm that would have named it. The two defects compose into
    a charge of nothing that reports nothing.

    A peer lane is fixing that forward. Recording the key set here does not
    change who gets charged — it makes an already-closed statement PROVABLE
    ("this key was unpriced when we billed it") instead of indistinguishable
    from a tenant that genuinely owed zero.

    ⚠️ TWO POPULATIONS, and only the first is the live defect. The
    ``app_rates.get`` call above consumes ``billing_metering_daily`` rows, so
    ``metering`` is the set that actually fabricates $0 today. ``cost`` is
    reported alongside it because a service_key seen only on the cost side is
    the same class of hole one table over, and a reader of a closed invoice
    should not have to know which loop reached which table.
    """
    metering_stmt = (
        select(MeteringDaily.service_key)
        .where(
            MeteringDaily.tenant_id == tenant_id,
            MeteringDaily.bucket_date >= period_start,
            MeteringDaily.bucket_date < period_end,
        )
        .group_by(MeteringDaily.service_key)
    )
    cost_stmt = (
        select(CostDaily.service_key)
        .where(
            CostDaily.tenant_id == tenant_id,
            CostDaily.bucket_date >= period_start,
            CostDaily.bucket_date < period_end,
        )
        .group_by(CostDaily.service_key)
    )
    metering_keys = {r.service_key for r in (await session.execute(metering_stmt)).all()}
    cost_keys = {r.service_key for r in (await session.execute(cost_stmt)).all()}
    priced = set(app_rates)
    return {
        "metering": sorted(metering_keys - priced),
        "cost": sorted(cost_keys - priced),
        "priced_keys_in_force": sorted(priced),
    }


async def _capture_inputs(
    session: AsyncSession,
    *,
    tenant_id: str,
    period_start: date,
    period_end: date,
    month: str,
    settings: Settings,
    licenses_response: list[dict],
    excluded: dict,
    app_rates: dict[str, Decimal] | None = None,
    app_rate_rows: Sequence[AppCostRate] | None = None,
) -> dict:
    """Everything mutable that fed the figures, resolved at ``bill_ts``.

    The test this has to pass: a reader holding only this dict and ``snapshot``
    can say WHY the total is what it is without opening a table that has since
    moved.

    ``app_rates`` / ``app_rate_rows`` are the OPTIONAL injected rate snapshot —
    the mapping the generator bills on and the same rows whole. Both come from
    ONE read in :func:`close_period`; omitted, each is read here as before.
    ⚠️ They must be two views of the SAME read, never two reads: the mapping
    decides ``unpriced_service_keys`` and the rows are the ``app_cost_rates``
    provenance array, so a disagreement would freeze an ``inputs`` document whose
    rate card does not explain its own unpriced-key set.
    """
    bill_ts = _bill_ts(period_end)

    fallback_pct, fallback_source = await get_markup_pct_with_source(tenant_id, session, settings)
    configured_mode = await get_billing_mode(tenant_id, session)
    byo_mode = await get_byo_mode(tenant_id, settings)
    effective_mode = effective_billing_mode(configured_mode, byo_mode)

    cfg = (
        await session.execute(select(TenantConfig).where(TenantConfig.tenant_id == tenant_id))
    ).scalar_one_or_none()

    # Every rule build_markup_resolver would load, serialised WHOLE — the
    # surcharge columns are as load-bearing as markup_pct on a byo line, so a
    # trimmed projection would make some invoices unexplainable.
    rules_stmt = select(MarkupRule).where(
        MarkupRule.effective_from <= bill_ts,
        MarkupRule.effective_until.is_(None) | (MarkupRule.effective_until > bill_ts),
        MarkupRule.tenant_id.is_(None) | (MarkupRule.tenant_id == tenant_id),
    )
    markup_rules = [_row_dict(r) for r in (await session.execute(rules_stmt)).scalars().all()]

    # Access charges + allowance. Skipped by generate_billing entirely for
    # internal/demo tenants, so it is skipped here too — recording charges the
    # generator never read would be a false account of the computation.
    access_rows: list[TenantAccessCharge] = []
    allowance_opening: dict[str, Decimal] = {}
    if effective_mode != BILLING_MODE_INTERNAL:
        access_rows = await active_access_charges_for_month(
            session,
            tenant_id=tenant_id,
            month_start=period_start,
            month_end=period_end,
            bill_ts=bill_ts,
        )
        for c in access_rows:
            if c.cadence == "monthly" and c.included_cost_usd is not None:
                key = c.service_key or ""
                allowance_opening[key] = allowance_opening.get(key, Decimal("0")) + Decimal(
                    c.included_cost_usd
                )
        for key, remaining in (
            await one_time_remaining_before(
                session, tenant_id=tenant_id, month_start=period_start, bill_ts=bill_ts
            )
        ).items():
            allowance_opening[key] = allowance_opening.get(key, Decimal("0")) + remaining

    # One read when both are injected (the close path); otherwise one read each,
    # as before. `is None` for the same reason as everywhere else on this path:
    # zero rate rows is the measured prod state and a real answer.
    if app_rate_rows is None:
        app_rate_rows = await get_app_cost_rate_rows(session)
    if app_rates is None:
        app_rates = app_cost_rate_map(app_rate_rows)
    frozen_app_rate_rows = [_row_dict(r) for r in app_rate_rows]

    # provider_pricing is NOT a direct input to generate_billing — cost is
    # already resolved into billing_cost_daily upstream — but it is the rate card
    # that PRODUCED those figures, so the ids and their effective windows are
    # frozen as provenance. ⚠️ The table has no updated_at; effective_from /
    # effective_until are the only version markers it carries.
    pricing_stmt = select(
        ProviderPricing.id,
        ProviderPricing.rate_class,
        ProviderPricing.provider,
        ProviderPricing.model,
        ProviderPricing.service_key,
        ProviderPricing.effective_from,
        ProviderPricing.effective_until,
    ).where(
        ProviderPricing.effective_from <= bill_ts,
        ProviderPricing.effective_until.is_(None) | (ProviderPricing.effective_until > bill_ts),
    )
    provider_pricing = [dict(r._mapping) for r in (await session.execute(pricing_stmt)).all()]

    return {
        "schema_version": INVOICE_SCHEMA_VERSION,
        "month": month,
        "period_start": period_start,
        # EXCLUSIVE, matching BillingInvoice's half-open interval.
        "period_end": period_end,
        "bill_ts": bill_ts,
        "tenant_config": {
            "markup_pct": cfg.markup_pct if cfg is not None else None,
            "billing_mode": cfg.billing_mode if cfg is not None else None,
            "row_present": cfg is not None,
            # What generate_billing actually used, after the DB-default and
            # env-default fallbacks and after the byo_mode gate.
            "fallback_markup_pct": fallback_pct,
            # ⚠️ WHICH of the three fallback branches answered. `row_present`
            # above only says whether THIS tenant has a config row; it cannot
            # distinguish the DB-persisted global default from the env constant,
            # and those are a commercial decision and a code default respectively.
            "fallback_markup_source": fallback_source,
            "configured_billing_mode": configured_mode,
            "byo_mode": byo_mode,
            "effective_billing_mode": effective_mode,
        },
        "markup_rules": markup_rules,
        # ⭐ THIS ARRAY AND ``snapshot["totals"]["access_charges"]`` ARE EXPECTED
        # TO DIFFER IN LENGTH ON EVERY NO-MATCH INVOICE, AND THAT IS THE POINT.
        # They answer different questions:
        #   * THIS one records WHAT THE QUERY RETURNED — nothing matched, so it
        #     is legitimately ``[]``. That is the reproducible INPUT.
        #   * The snapshot records WHAT THE BILL SAYS — one explicit $0.00
        #     platform-fee line, because an absent line cannot be audited or
        #     reconciled and a zero one can. That is the whole point of the
        #     change that introduced it.
        # It is the same absent-vs-zero distinction one layer in. Freezing the
        # two as equal would destroy exactly the information the zero line
        # exists to create, so the difference is LOAD-BEARING, NOT DRIFT.
        #
        # ⚠️ An auditor reconciling these two arrays BY COUNT is asking the
        # wrong question and will read a false mismatch. Verified 2026-09-08
        # across src/, scripts/, tests/ and alembic/: NO current reader compares
        # them — nothing reads ``inputs["access_charges"]`` at all, and the only
        # place both documents meet is ``_digest(snapshot, inputs)``, which
        # hashes the pair as a unit rather than comparing them.
        "access_charges": [_row_dict(r) for r in access_rows],
        "allowance": {
            "opening": allowance_opening,
            "opening_total": sum(allowance_opening.values(), Decimal("0")),
            # ⚠️ Per-bucket draw is NOT recorded: generate_billing consumes the
            # buckets inside its cost loop and only reports the tenant-wide
            # total (``totals.included_applied_usd``). Replaying the draw here
            # would mean duplicating that loop's ordering, which is exactly the
            # kind of second copy that drifts. Filled from the snapshot below.
            "draw_total": None,
            "closing_total": None,
            "draw_note": "per-service_key draw is not exposed by generate_billing",
        },
        "app_cost_rates": frozen_app_rate_rows,
        "provider_pricing_in_force": provider_pricing,
        # Verbatim, as returned by the licences service. NOT normalised: the
        # labels on the statement came from this exact document.
        "licenses_response": licenses_response,
        "rollup_watermarks": await _rollup_watermarks(
            session, tenant_id=tenant_id, period_start=period_start, period_end=period_end
        ),
        "unpriced_service_keys": await _unpriced_service_keys(
            session,
            tenant_id=tenant_id,
            period_start=period_start,
            period_end=period_end,
            app_rates=app_rates,
        ),
        "excluded_unbillable": excluded,
    }


def _row_dict(row: Any) -> dict:
    """Serialise an ORM row WHOLE, by mapped column, with no hand-picked subset.

    A projection would decide today which of a rule's columns can explain a
    future invoice, and a column added later would silently stop being frozen.
    """
    return {c.key: getattr(row, c.key) for c in row.__table__.columns}


# --------------------------------------------------------------------------- #
# Lookup
# --------------------------------------------------------------------------- #


async def find_open(
    session: AsyncSession,
    *,
    tenant_id: str,
    period_start: date,
    period_end: date,
) -> BillingInvoice | None:
    """The live (draft or issued) invoice for this tenant-period, if any.

    The status set is spelled POSITIVELY — ``('draft', 'issued')``, never
    ``!= 'void'`` — to match ``ux_billing_invoices_open_period``'s predicate
    exactly. Under ``!=`` a status added later would join this result set
    without an edit here, and the backstop index and this query would start
    disagreeing about what "open" means.
    """
    stmt = select(BillingInvoice).where(
        BillingInvoice.tenant_id == tenant_id,
        BillingInvoice.period_start == period_start,
        BillingInvoice.period_end == period_end,
        BillingInvoice.status.in_(("draft", "issued")),
    )
    return (await session.execute(stmt)).scalars().first()


# --------------------------------------------------------------------------- #
# Close
# --------------------------------------------------------------------------- #


async def close_period(
    session: AsyncSession,
    *,
    tenant_id: str,
    period_start: date,
    period_end: date,
    settings: Settings,
    token_provider: ServiceTokenProvider | None = None,
    actor: str | None = None,
    issue_immediately: bool = True,
) -> BillingInvoice:
    """Freeze one tenant-period into a ``billing_invoices`` row.

    Order of operations, and why it is this order:

    1. :func:`find_open` — an existing live invoice is returned UNCHANGED and
       nothing is recomputed. Re-running a close must not re-render a statement
       the customer may already hold.
    2. :func:`assert_no_bucket_gap` — FIRST among the computing steps. A period
       with an absent rollup day is refused before any work, and before any row
       exists to have to clean up.
    3. :func:`assert_all_service_keys_priced` — beside it and for the same
       reason: a period whose billed ``service_key`` has no ``AppCostRate`` row
       would invoice that app line at a fabricated $0.00, so it is refused here
       while refusing is still free — before the HTTP call, and before any
       number is drawn from the gapless series. The rate snapshot it runs on is
       read ONCE, just above it, and threaded into ``_capture_inputs`` and
       ``generate_billing`` — see the comment at that read.
    4. :func:`assert_no_unpriced_provider_cost` — immediately after 3 and at the
       same tier, because it is the OTHER HALF of the same question and neither
       gate can see the other's gap. 3 asks whether the platform's per-request
       charge has a rate (``billing_app_cost_rates``); this asks whether the
       PROVIDER spend underneath it could be priced at all
       (``provider_pricing``). An event with no matching price row is stored with
       ``cost_usd IS NULL`` on purpose, the rollup's
       ``coalesce(sum(cost_usd), 0)`` then freezes it as a hard $0.000000, and
       only ``CostDaily.unpriced_event_count`` still knows the difference between
       free and unknown. Refused here, where refusing is still free.
    5. :func:`assert_seat_count_available` — beside the three above, and for the
       next instance of the same reason. A tenant with a ``cadence='per_seat'``
       access charge whose seat count licences cannot state would be invoiced at a
       FABRICATED ZERO seats, indistinguishable from a tenant that genuinely has
       none. `generate_billing` is allowed to degrade (it reports
       ``seat_basis="unavailable"``) because nothing it produces is frozen; a
       close is, so it refuses. A tenant with no per-seat term is unaffected —
       there is nothing to count.
    6. :func:`assert_byo_mode_available` — the next instance, and the most
       expensive one to get wrong. ``byo_mode`` decides whether the tenant is
       billed AT ALL, so unlike seats it is checked for EVERY tenant, and its
       non-answer is not a fabricated zero but a fabricated COMMERCIAL invoice
       for a cost centre that bills $0. It is also ordered here to PRIME the
       5-minute cache — see the comment at the call site.
    7. :func:`fetch_licenses` — the one HTTP call, made HERE and deliberately
       outside any counter lock. ``LicensesUnavailableError`` propagates and
       leaves NO draft behind: an outage must abort the close, never degrade the
       licence list to ``[]`` and freeze a statement whose labels are missing
       with nothing recording that they were.
    8. inputs, then ``generate_billing``, then the digest, then the INSERT.

    ⚠️ ``generate_billing`` makes its OWN licences call and, unlike this
    function, catches ``LicensesUnavailableError`` and degrades to ``[]``
    (``services/billing.py``, ``billing_license_enrichment_degraded``). There is
    no parameter to inject the response we already hold. :func:`fetch_licenses`'s
    own call site (step 7) therefore narrows the window rather than closing it
    (named rather than numbered: the number rots every time a gate is inserted
    above it, and this reference was already stale before this one was):
    if licences dies between the two
    calls, the frozen ``inputs.licenses_response`` is populated while the
    snapshot's labels are degraded. That is visible in the record — which is the
    property this module exists for — but it is not prevented here, and closing
    it needs a change to ``generate_billing``'s signature.

    Idempotency has two layers. ``find_open`` is the fast path; the partial
    unique index ``ux_billing_invoices_open_period`` is the backstop for a
    concurrent close, caught below and resolved by re-reading.
    """
    month = _month_str(period_start, period_end)

    # ⭐ PREPAID CREDIT IS READ (generate_billing) AND WRITTEN (the drawdown below)
    # IN THIS TRANSACTION, so it is serialised per tenant before anything reads
    # it. Two closes for one tenant -- the sweep and an operator's manual close
    # of a month the sweep refused -- would otherwise both see the same balance
    # and spend it twice. Transaction-scoped: released by the caller's commit.
    await prepaid_credit_svc.lock_tenant_credit(session, tenant_id)

    existing = await find_open(
        session, tenant_id=tenant_id, period_start=period_start, period_end=period_end
    )
    if existing is not None:
        logger.info(
            "invoice_close_noop_existing",
            tenant_id=tenant_id,
            month=month,
            invoice_id=str(existing.id),
            status=existing.status,
        )
        return existing

    # ⭐ FIRST GATE, AHEAD OF EVERY QUERY. Asks whether this tenant should be
    # invoiced AT ALL before anything asks whether its numbers are stateable.
    # Ordered after `find_open` on purpose and only that: an invoice that already
    # exists is returned unchanged no matter what the registry now says, because
    # re-rendering a statement the customer may already hold is the one thing this
    # function must never do — including for a tenant that has since been
    # archived.
    tenant_info = await assert_tenant_invoiceable(
        tenant_id=tenant_id, settings=settings, token_provider=token_provider
    )

    await assert_no_bucket_gap(
        session, tenant_id=tenant_id, period_start=period_start, period_end=period_end
    )

    # ⭐ ONE READ OF `billing_app_cost_rates` FOR THE WHOLE CLOSE, taken here —
    # before the gate, because the gate is the first consumer.
    #
    # It used to be read THREE times through this one session, under Postgres
    # default READ COMMITTED, where every statement gets a FRESH snapshot: the
    # gate (`assert_all_service_keys_priced`), the provenance
    # (`_capture_inputs` — twice over, mapping and rows), and the money
    # (`get_app_cost_rates` inside `generate_billing`). An operator inserting or
    # repricing a rate row mid-close — exactly when they would, since the gate's
    # refusal is the message telling them to — made the three disagree, so the
    # set the close was ALLOWED on, the set the invoice CLAIMS it read, and the
    # set the lines were CHARGED at could all differ. The row is immutable and
    # numbered from a gapless series, so there is no later pass that could
    # reconcile them, and `reproduce` compares the stored total against the
    # snapshot — it would report the disagreement without being able to say
    # which read was the real one.
    #
    # Threaded as an explicit argument rather than cached: the dependency belongs
    # in the signatures, where a future edit cannot drop it without the call site
    # changing. (Contrast `byo_mode` below, which is narrowed by a TTL cache
    # because neither of ITS consumers takes an injected value.)
    app_rate_rows = await get_app_cost_rate_rows(session)
    app_rates = app_cost_rate_map(app_rate_rows)

    await assert_all_service_keys_priced(
        session,
        tenant_id=tenant_id,
        period_start=period_start,
        period_end=period_end,
        app_rates=app_rates,
    )

    # ⭐ IMMEDIATELY AFTER THE APP-RATE GATE AND BEFORE ANYTHING ELSE, because it
    # is the same TIER of gate: a refuse-before-any-row-exists check that needs
    # nothing but this session. It is the OTHER half of the pricing question — that
    # gate asks whether the platform's per-request charge has a rate, this one asks
    # whether the provider spend underneath it has a price at all. Neither can see
    # the other's gap.
    #
    # The verdict is logged rather than discarded: "did not raise" is NOT "proven
    # clean" for any day bucketed before `_UNPRICED_EVENT_COUNT_MEASURED_FROM`, and
    # a close that freezes an unverifiable figure should say so in the record of
    # the run that made it.
    unpriced_coverage = await assert_no_unpriced_provider_cost(
        session,
        tenant_id=tenant_id,
        period_start=period_start,
        period_end=period_end,
    )
    logger.info(
        "invoice_close_unpriced_cost_coverage",
        tenant_id=tenant_id,
        month=month,
        measured_rows=unpriced_coverage.measured_rows,
        unmeasurable_rows=unpriced_coverage.unmeasurable_rows,
    )

    await assert_seat_count_available(
        session,
        tenant_id=tenant_id,
        settings=settings,
        period_start=period_start,
        period_end=period_end,
    )

    # ⭐ ORDERED BEFORE `_capture_inputs` AND `generate_billing` ON PURPOSE — this
    # call is a gate AND a cache prime, and the ordering is the half that closes
    # the TOCTOU window.
    #
    # Both of those resolve the same tenant's byo_mode again through the
    # DEFAULTING `get_byo_mode`, which answers "byo_required" (= BILL
    # COMMERCIALLY) when licences cannot be reached. On success this call has
    # already written the tenant's REAL classification into
    # `services.byo_mode`'s 5-minute TTL cache, so if licences dies in the
    # window between here and there, both of them are served the cached real
    # answer and no guess can reach the frozen row. Move this below them and the
    # gate still refuses on a licences outage that is already in progress, but
    # stops protecting the much likelier case — a blip that STARTS mid-close.
    #
    # A parameter threading the value into `generate_billing` would close the
    # window outright; that is deliberately not done here because it changes the
    # generator's signature (the same reasoning as the licences note above).
    await assert_byo_mode_available(tenant_id=tenant_id, settings=settings)

    # Raises LicensesUnavailableError -> the close aborts with no draft written.
    try:
        licenses_response = await fetch_licenses(tenant_id, settings, token_provider=token_provider)
    except LicensesUnavailableError as exc:
        logger.warning(
            "invoice_close_refused_licenses_unavailable",
            tenant_id=tenant_id,
            error=str(exc),
        )
        observability_metrics.invoice_close_refused_total.labels(
            reason="licenses_unavailable"
        ).inc()
        raise

    excluded = await measure_excluded_unbillable(
        session, tenant_id=tenant_id, period_start=period_start, period_end=period_end
    )
    inputs = await _capture_inputs(
        session,
        tenant_id=tenant_id,
        period_start=period_start,
        period_end=period_end,
        month=month,
        settings=settings,
        licenses_response=licenses_response,
        excluded=excluded,
        # The SAME rate snapshot the gate ran on, in both of its views.
        app_rates=app_rates,
        app_rate_rows=app_rate_rows,
    )

    # ⭐ THE SAME licences payload that was frozen into `inputs` above — not a
    # second fetch. Two calls meant a blip between them could freeze `inputs`
    # populated while the snapshot's labelling degraded to [], leaving the pair
    # internally inconsistent with nothing recording that it had happened.
    # The pre-fetch at the top of this function raises on an outage, so the
    # close aborts with no draft written; that is now the ONLY licences failure
    # mode an invoice can take.
    snapshot = await generate_billing(
        tenant_id,
        month,
        session,
        settings,
        token_provider=token_provider,
        licenses_data=licenses_response,
        # ⭐ THE SAME rate snapshot the gate allowed this close on and `inputs`
        # froze above — not a third read. Without this the generator re-reads
        # `billing_app_cost_rates` and the money lines can be computed from a
        # rate set that neither the gate nor the frozen provenance ever saw.
        app_rates=app_rates,
    )
    totals = snapshot.get("totals", {})

    # Allowance draw/closing come from the generator's own total (see the note
    # in _capture_inputs); filled after the fact so the pair reconciles.
    draw = Decimal(str(totals.get("included_applied_usd", "0")))
    inputs["allowance"]["draw_total"] = draw
    inputs["allowance"]["closing_total"] = inputs["allowance"]["opening_total"] - draw

    # JSON round-trip ONLY — no reshaping, no rounding, no key renaming. The
    # snapshot is the generator's answer verbatim; anything else here would make
    # the frozen document a summary of the computation rather than the
    # computation.
    snapshot_json = _jsonable(snapshot)
    inputs_json = _jsonable(inputs)

    # ⭐ DERIVED BEFORE THE DIGEST, AND FROM `snapshot_json` RATHER THAN
    # `snapshot`. The ordering is what lets the money columns be hashed at all —
    # they used to be computed thirty lines BELOW the digest, so the digest could
    # not have covered them without this move.
    #
    # The argument is the JSONABLE document, not the live one, because that is
    # the document `reproduce` will re-derive from: `_jsonable` turns every
    # Decimal into a string, so deriving from the live dict here and from the
    # JSONB there would be two derivations over two representations — the exact
    # shape that drifts. Same bytes in, same figures out.
    money = _money_from_snapshot(snapshot_json)
    # Not snapshot-derivable: it comes from measure_excluded_unbillable, a
    # separate query over cost_daily. It is digested all the same — the column is
    # a stored copy like the others, and an edit to it must not go unnoticed just
    # because `reproduce` cannot re-derive it.
    money["excluded_unbillable_usd"] = excluded["total_usd"]

    # ⭐ THE SKIP, AND IT HAS TO BE HERE: after the money is computed, before the
    # row exists and before `issue` draws a number. Everything above this line is
    # free to redo; everything below it is permanent.
    #
    # ⚠️ `_CHARGED_MONEY_COLUMNS` deliberately EXCLUDES `internal_cost_usd`,
    # `margin_usd` and `excluded_unbillable_usd`, and that exclusion is the whole
    # mechanism. An internal cost centre's month has a large `internal_cost_usd`
    # and charges nothing — which is precisely the $0.00 statement we must not
    # number. `excluded_unbillable_usd` is spend the system has already ruled
    # UNBILLABLE, so letting it make a period billable would invert its meaning,
    # and `margin_usd` is derived from the two.
    if not any(Decimal(str(money[col])) for col in _CHARGED_MONEY_COLUMNS):
        logger.info(
            "invoice_close_skipped_no_billable_basis",
            tenant_id=tenant_id,
            month=month,
            tenant_name=tenant_info.name,
            kind=tenant_info.kind,
            billing_mode=snapshot.get("billing_mode", ""),
            internal_cost_usd=str(money["internal_cost_usd"]),
            excluded_unbillable_usd=str(money["excluded_unbillable_usd"]),
        )
        observability_metrics.invoice_close_skipped_total.labels(reason="no_billable_basis").inc()
        raise PeriodNotBillableError(tenant_id, month, snapshot.get("billing_mode", ""))

    # ⭐ THE IDENTITY GATE, AND ITS PLACEMENT IS TWO DECISIONS, NOT ONE.
    #
    # AFTER the skip, deliberately: a period that charges nothing is SKIPPED, and
    # running the balance check first would turn that benign, expected outcome
    # into a counted REFUSAL for every internal cost centre in the estate. (The
    # identity holds for them anyway — internal mode bills $0 against a real
    # incurred cost, so each component pair reads +X/−X and nets to the $0 total —
    # so nothing is lost by checking them second.)
    #
    # BEFORE the digest and before the INSERT, non-negotiably: everything below
    # this line is permanent. The row is immutable (`invoice_immutability`) and
    # `issue` draws its number from a GAPLESS counter, so an unbalanced statement
    # cannot be edited afterwards and voiding one burns audit evidence. Hashing an
    # unbalanced money set would also do the specific damage this module exists to
    # prevent: freeze the imbalance INTO the tamper digest, so every later reader
    # would confirm the bytes are pristine and none would notice they do not add
    # up.
    assert_total_is_the_sum_of_its_components(money, tenant_id=tenant_id, month=month)
    # ⛔ A statement never pays the customer. Checked on the figure about to be
    # WRITTEN, after the identity gate, because the identity's one-cent tolerance
    # is exactly the margin a -0.01 total hid inside (#267 re-audit).
    if Decimal(str(money["total_usd"])) < 0:
        logger.error(
            "invoice_negative_total_refused",
            tenant_id=tenant_id,
            month=month,
            total_usd=str(money["total_usd"]),
        )
        raise InvoiceNegativeTotalError(tenant_id, month, Decimal(str(money["total_usd"])))

    sha = _digest(snapshot_json, inputs_json, money)

    names = await get_tenant_names(settings, token_provider)
    stored_name = (
        await session.execute(
            select(CostDaily.tenant_name)
            .where(CostDaily.tenant_id == tenant_id, CostDaily.tenant_name != "")
            .limit(1)
        )
    ).scalar_one_or_none()

    # ⛔ EVERY MONEY COLUMN BELOW READS FROM `money`, AND THAT IS THE INVARIANT.
    # The derivations that used to sit here now live in `_money_from_snapshot`,
    # which ran above and whose output is inside `sha`. A column assigned from
    # anything other than `money[...]` would be a figure the digest does not
    # cover — which is the defect this change closes — so the assignments are
    # deliberately mechanical. The commentary explaining each figure moved with
    # the code, into that helper.
    customer = money["total_usd"]

    invoice = BillingInvoice(
        tenant_id=tenant_id,
        tenant_name=resolve_tenant_name(tenant_id, stored_name, names),
        period_start=period_start,
        period_end=period_end,
        status="draft",
        subtotal_usd=money["subtotal_usd"],
        total_usd=money["total_usd"],
        internal_cost_usd=money["internal_cost_usd"],
        margin_usd=money["margin_usd"],
        access_charge_usd=money["access_charge_usd"],
        platform_fee_usd=money["platform_fee_usd"],
        llm_usage_usd=money["llm_usage_usd"],
        llm_markup_usd=money["llm_markup_usd"],
        app_request_usage_usd=money["app_request_usage_usd"],
        app_request_markup_usd=money["app_request_markup_usd"],
        seat_fee_usd=money["seat_fee_usd"],
        seat_count=money["seat_count"],
        excluded_unbillable_usd=money["excluded_unbillable_usd"],
        prepaid_credit_applied_usd=money["prepaid_credit_applied_usd"],
        # ⚠️ NOT in `money` and NOT digested — a ROW COUNT, not an amount. It
        # rides along with excluded_unbillable_usd rather than being part of the
        # money set, and the set is closed by _MONEY_COLUMNS rather than by what
        # happens to be adjacent here.
        excluded_unbillable_rows=excluded["row_count"],
        excluded_by_status={k: str(v["usd"]) for k, v in excluded["by_status"].items()},
        billing_mode=snapshot.get("billing_mode", ""),
        markup_pct=Decimal(str(snapshot.get("markup_pct", 0))),
        # ⭐ WHERE that percentage came from — an effective-dated `markup_rules`
        # row, a tenant_config row, or `settings.default_markup_pct` (a code
        # constant). A frozen statement that records the rate but not its source
        # is not reconcilable against a rate card.
        markup_source=str(snapshot.get("markup_source", MARKUP_SOURCE_UNKNOWN)),
        effective_markup_pct=Decimal(str(snapshot.get("effective_markup_pct", 0))),
        snapshot=snapshot_json,
        inputs=inputs_json,
        snapshot_sha256=sha,
        lines=snapshot_json.get("licenses", []),
        generator_version=INVOICE_SCHEMA_VERSION,
        created_by=actor,
    )

    try:
        async with session.begin_nested():
            session.add(invoice)
            await session.flush()
    except IntegrityError as exc:
        # The backstop fired: a concurrent close won the (tenant, period) race.
        # The savepoint above is what makes this recoverable — without it the
        # whole transaction would be poisoned and the re-read below impossible.
        if _OPEN_PERIOD_INDEX not in str(exc.orig):
            raise
        raced = await find_open(
            session, tenant_id=tenant_id, period_start=period_start, period_end=period_end
        )
        if raced is None:
            raise
        logger.info(
            "invoice_close_lost_race",
            tenant_id=tenant_id,
            month=month,
            invoice_id=str(raced.id),
            status=raced.status,
        )
        return raced

    # ⭐ FREEZE THIS INVOICE'S CREDIT DRAWDOWN IN THE SAME TRANSACTION AS THE ROW,
    # under the lock taken at the top. It is keyed by the INVOICE, so a void
    # (which reverses it) plus a re-close freezes a fresh figure, and the ledger
    # and the statements always agree. ``freeze_drawdown`` re-checks that the
    # balance is still >= 0 and raises, rolling the INSERT back, rather than
    # overdraw. Written only when credit was applied (the zero-credit tenant,
    # i.e. almost every tenant, gets no ledger noise).
    if Decimal(str(money["prepaid_credit_applied_usd"])) > 0:
        await prepaid_credit_svc.freeze_drawdown(
            session,
            tenant_id=tenant_id,
            month_start=period_start,
            amount_usd=money["prepaid_credit_applied_usd"],
            invoice_id=invoice.id,
        )

    logger.info(
        "invoice_closed",
        tenant_id=tenant_id,
        month=month,
        invoice_id=str(invoice.id),
        total_usd=str(customer),
        excluded_unbillable_usd=str(excluded["total_usd"]),
        unpriced_service_keys=inputs["unpriced_service_keys"]["metering"],
        snapshot_sha256=sha,
    )

    if issue_immediately:
        return await issue(session, invoice_id=invoice.id, actor=actor)
    return invoice


# --------------------------------------------------------------------------- #
# Lifecycle
# --------------------------------------------------------------------------- #


def _counter_scope(invoice: BillingInvoice) -> tuple[str, str, int]:
    """``(scope, series, year)`` for the number series this invoice belongs to.

    ⚠️ ``series`` is returned so the RENDERED NUMBER and the COUNTER KEY are
    derived from the same value. They are keyed per ``(series, year)`` while
    ``ux_billing_invoices_number`` is globally unique, so a number that omitted
    the series would collide across legal entities the first time a second one
    existed — see :data:`_DEFAULT_NUMBER_SERIES`.

    The year is the PERIOD's year, not the year the number happens to be minted
    in, matching ``BillingInvoiceCounter.period_year``'s name: a December period
    closed in January stays in the December year's series, and a void-and-reissue
    of an old period lands back in that period's series rather than opening a
    hole in it. ``legal_entity`` is still part of the present-and-null legal set,
    so every invoice shares the ``'NA'`` scope today.
    """
    year = invoice.period_start.year
    series = invoice.legal_entity or _DEFAULT_NUMBER_SERIES
    return f"{series}:{year}", series, year


async def issue(
    session: AsyncSession,
    *,
    invoice_id: uuid_mod.UUID,
    actor: str | None = None,
) -> BillingInvoice:
    """Allocate the next number and move ``draft`` -> ``issued``.

    ⛔ GAPLESS, which is why this is a row and not a sequence: ``nextval`` is
    non-transactional, so a rollback BURNS its number forever. Here the counter
    is incremented inside the caller's transaction under ``FOR UPDATE``, so a
    rollback takes the increment with it and the series stays consecutive.

    Re-issuing an already-issued invoice is a NO-OP that returns the same row
    with the same number and takes NO second draw. The check happens before the
    counter is touched, so a retry cannot burn a number.

    ⚠️ Nothing that can block on the network belongs inside the ``FOR UPDATE``
    window below — it serialises issuance for the whole (entity, year) series.
    The licences call lives in :func:`close_period`, before this is reached.
    """
    invoice = await _load(session, invoice_id, for_update=True)

    if invoice.status == "issued":
        logger.info(
            "invoice_issue_noop",
            invoice_id=str(invoice_id),
            invoice_number=invoice.invoice_number,
        )
        return invoice
    if invoice.status != "draft":
        raise InvoiceStateError(
            f"invoice {invoice_id} is {invoice.status!r}; only a draft can be issued"
        )

    scope, series, year = _counter_scope(invoice)

    # Seed the row if the series has never been drawn from. DO NOTHING rather
    # than DO UPDATE: a concurrent seeder must not reset next_seq.
    await session.execute(
        text(
            "INSERT INTO billing_invoice_counters (scope, period_year, next_seq) "
            "VALUES (:scope, :yr, 1) ON CONFLICT (scope) DO NOTHING"
        ),
        {"scope": scope, "yr": year},
    )
    seq = (
        await session.execute(
            text("SELECT next_seq FROM billing_invoice_counters WHERE scope = :scope FOR UPDATE"),
            {"scope": scope},
        )
    ).scalar_one()
    await session.execute(
        text("UPDATE billing_invoice_counters SET next_seq = next_seq + 1 WHERE scope = :scope"),
        {"scope": scope},
    )

    invoice.invoice_number = f"{series}-{year}-{int(seq):06d}"
    invoice.status = "issued"
    invoice.issued_at = datetime.now(UTC)
    invoice.issued_by = actor
    # Flushed here, not left to the caller's commit: the immutability trigger
    # refuses draft -> issued without BOTH invoice_number and issued_at, and a
    # violation should surface at this call site rather than at commit.
    await session.flush()

    logger.info(
        "invoice_issued",
        invoice_id=str(invoice_id),
        tenant_id=invoice.tenant_id,
        invoice_number=invoice.invoice_number,
        scope=scope,
        seq=int(seq),
    )
    return invoice


async def void(
    session: AsyncSession,
    *,
    invoice_id: uuid_mod.UUID,
    reason: str,
    actor: str | None = None,
) -> BillingInvoice:
    """Move an invoice to the terminal ``void`` status.

    ``reason`` must be non-empty. That is not politeness: the immutability
    trigger refuses a void without one, and a void with no recorded reason is a
    statement that vanished with nothing saying why.

    Voiding does NOT return the invoice's number to the series. A gapless series
    is gapless in ALLOCATION, not in liveness — the void row keeps its number and
    remains readable, which is exactly what lets an auditor see that
    ``INV-2026-000041`` was superseded rather than destroyed. The period leaves
    ``ux_billing_invoices_open_period``'s set, so it can be closed again.
    """
    if not reason or not reason.strip():
        raise InvoiceStateError(
            f"refusing to void invoice {invoice_id} without a reason: the "
            "immutability trigger requires void_reason, and a void with no "
            "recorded cause is unauditable"
        )

    invoice = await _load(session, invoice_id, for_update=True)
    if invoice.status == "void":
        raise InvoiceStateError(
            f"invoice {invoice_id} is already void; void is terminal and admits no UPDATE"
        )

    # Same per-tenant serialisation as close_period: the reversal below changes
    # the credit balance that a concurrent close would read.
    await prepaid_credit_svc.lock_tenant_credit(session, invoice.tenant_id)
    invoice.status = "void"
    invoice.void_reason = reason
    invoice.voided_at = datetime.now(UTC)
    invoice.voided_by = actor
    await session.flush()
    # ⭐ A voided statement no longer spends credit. Its drawdown is reversed by
    # a compensating ledger entry in THIS transaction, so the period's re-close
    # freezes the new figure and invoice and ledger keep agreeing (the stale
    # debit kept by ON CONFLICT DO NOTHING was the second #267 audit finding).
    await prepaid_credit_svc.reverse_drawdown(
        session, tenant_id=str(invoice.tenant_id), invoice_id=invoice.id
    )

    logger.info(
        "invoice_voided",
        invoice_id=str(invoice_id),
        tenant_id=invoice.tenant_id,
        invoice_number=invoice.invoice_number,
        reason=reason,
    )
    return invoice


async def _load(
    session: AsyncSession, invoice_id: uuid_mod.UUID, *, for_update: bool = False
) -> BillingInvoice:
    stmt = select(BillingInvoice).where(BillingInvoice.id == invoice_id)
    if for_update:
        stmt = stmt.with_for_update()
    invoice = (await session.execute(stmt)).scalar_one_or_none()
    if invoice is None:
        raise InvoiceStateError(f"invoice {invoice_id} does not exist")
    return invoice


# --------------------------------------------------------------------------- #
# Reproduce
# --------------------------------------------------------------------------- #


async def reproduce(session: AsyncSession, *, invoice_id: uuid_mod.UUID) -> dict:
    """Re-render a closed statement from the ROW, reading nothing else.

    ⛔ THIS FUNCTION MUST NEVER CALL ``generate_billing`` OR READ A MUTABLE
    TABLE. It is the whole reason ``billing_invoices`` exists rather than a
    query. The moment re-rendering consults ``markup_rules``, ``cost_daily`` or
    the licences service, a statement stops being a record and goes back to
    being a report that agrees with itself only until an input moves.

    ``snapshot``, ``inputs`` and ``snapshot_sha256`` come back VERBATIM.

    ⭐ WHAT ``digest_matches`` NOW MEANS, AND WHAT IT USED TO MEAN. It is now
    recomputed from the two frozen documents AND the STORED money columns
    (:data:`_MONEY_COLUMNS`), so ``True`` asserts all three of: the snapshot is
    unedited, the inputs are unedited, and every money column still holds the
    value that was hashed when the row was written. It previously covered only
    ``snapshot`` + ``inputs`` — which this function then re-hashed from the very
    fields it had just read — so it confirmed itself and could not go False for
    an edited ``total_usd``. A wrong invoice total was detectable by nothing in
    the repo.

    ``money_matches_snapshot`` is the SECOND, independent check, and it is not a
    restatement of the first: the digest proves a column has not moved SINCE the
    close, while this re-derives each column from ``snapshot`` through
    :func:`_money_from_snapshot` — the same function ``close_period`` used to
    produce them — and proves the column agrees with the document it was derived
    FROM. A row written with a digest over an already-wrong total would pass the
    digest check and fail this one. ``money_mismatches`` names the offending
    columns with both readings, because "this statement is wrong somewhere" is
    not an actionable finding.

    ⚠️ ``excluded_unbillable_usd`` is inside ``digest_matches`` but NOT inside
    ``money_matches_snapshot``: it comes from ``measure_excluded_unbillable``, a
    query over ``cost_daily``, so re-deriving it would mean reading a mutable
    table — which this function may never do. Its coverage is the digest alone.
    """
    stmt = select(
        BillingInvoice.id,
        BillingInvoice.tenant_id,
        BillingInvoice.tenant_name,
        BillingInvoice.period_start,
        BillingInvoice.period_end,
        BillingInvoice.status,
        BillingInvoice.invoice_number,
        BillingInvoice.currency,
        BillingInvoice.subtotal_usd,
        BillingInvoice.total_usd,
        BillingInvoice.internal_cost_usd,
        BillingInvoice.margin_usd,
        # ⭐ These five were OMITTED, and `reproduce` is the only re-render path
        # there is. They were recoverable solely by hand-parsing the raw snapshot
        # JSONB — which is to say, not recoverable by any caller of this API.
        BillingInvoice.access_charge_usd,
        BillingInvoice.platform_fee_usd,
        BillingInvoice.markup_pct,
        BillingInvoice.effective_markup_pct,
        BillingInvoice.markup_source,
        # ⭐ AND THESE SEVEN WERE OMITTED TOO, for the same reason and with a
        # sharper consequence: `total_usd` was returned with NO COMPONENTS to
        # explain it. The seat fee and its quantity, the LLM usage and the markup
        # taken on it, and the spend the statement DROPPED are the figures an
        # operator reconciles a total out of — and without `billing_mode` the
        # re-render cannot say whether the tenant was billed commercially or at
        # $0, while without `issued_by` it cannot name who issued it. All seven
        # are written by `close_period`/`issue` and were readable only by
        # hand-parsing the raw snapshot JSONB, i.e. not by any caller of this API.
        #
        # ⚠️ The five money ones are also load-bearing for `digest_matches`: they
        # are in `_MONEY_COLUMNS`, so the digest cannot be recomputed without
        # selecting them. A future edit that drops one from this list breaks the
        # verification, not just the rendering.
        BillingInvoice.llm_usage_usd,
        BillingInvoice.llm_markup_usd,
        # ⛔ SELECTED BECAUSE THEY ARE IN `_MONEY_COLUMNS`, not merely to render
        # them. `stored_money` below comprehends over that tuple, so a money
        # column missing from THIS list is a `KeyError` that breaks every call to
        # `reproduce` — the digest cannot be recomputed without them. The warning
        # two comments up applies to this pair with the same force.
        BillingInvoice.app_request_usage_usd,
        BillingInvoice.app_request_markup_usd,
        BillingInvoice.seat_fee_usd,
        BillingInvoice.seat_count,
        BillingInvoice.excluded_unbillable_usd,
        # In `_MONEY_COLUMNS` (alembic 0055) -- selected for the same reason.
        BillingInvoice.prepaid_credit_applied_usd,
        BillingInvoice.billing_mode,
        BillingInvoice.issued_by,
        BillingInvoice.lines,
        BillingInvoice.snapshot,
        BillingInvoice.inputs,
        BillingInvoice.snapshot_sha256,
        BillingInvoice.generator_version,
        BillingInvoice.issued_at,
        BillingInvoice.voided_at,
        BillingInvoice.void_reason,
    ).where(BillingInvoice.id == invoice_id)
    row = (await session.execute(stmt)).one_or_none()
    if row is None:
        raise InvoiceStateError(f"invoice {invoice_id} does not exist")

    frozen = dict(row._mapping)
    snapshot = frozen["snapshot"]
    inputs = frozen["inputs"]

    # The digest is recomputed from the columns AS STORED — not from the snapshot
    # — so an edit to one of them changes `recomputed` and `digest_matches` goes
    # False. Recomputing it from a re-derivation instead would reproduce the old
    # defect one level up: the check would again be comparing the snapshot with
    # itself and would stay True over any column edit.
    stored_money = {name: frozen[name] for name in _MONEY_COLUMNS}
    recomputed = _digest(snapshot, inputs, stored_money)

    # The independent half: do the columns still agree with the document they
    # were derived from? Both sides go through `_money_canon` so a scale-only
    # difference (the columns are Numeric(20, 8); the snapshot carries plain
    # strings) is not reported as a money mismatch.
    money_mismatches = {
        name: {
            "stored": _money_canon(stored_money[name]),
            "from_snapshot": _money_canon(derived),
        }
        for name, derived in _money_from_snapshot(snapshot).items()
        if _money_canon(stored_money[name]) != _money_canon(derived)
    }

    return {
        "invoice_id": str(frozen["id"]),
        "tenant_id": frozen["tenant_id"],
        "tenant_name": frozen["tenant_name"],
        "period_start": frozen["period_start"].isoformat(),
        "period_end": frozen["period_end"].isoformat(),
        "status": frozen["status"],
        "invoice_number": frozen["invoice_number"],
        "currency": frozen["currency"],
        "subtotal_usd": str(frozen["subtotal_usd"]),
        "total_usd": str(frozen["total_usd"]),
        "internal_cost_usd": str(frozen["internal_cost_usd"]),
        "margin_usd": str(frozen["margin_usd"]),
        # ⚠️ MONEY AS STRING, like every other figure here — these are frozen
        # amounts and a float round-trip would be a silent edit. The percentages
        # follow the same rule for the same reason.
        "access_charge_usd": str(frozen["access_charge_usd"]),
        "platform_fee_usd": str(frozen["platform_fee_usd"]),
        "markup_pct": str(frozen["markup_pct"]),
        "effective_markup_pct": str(frozen["effective_markup_pct"]),
        "markup_source": frozen["markup_source"],
        # THE SEVEN. Money as string, like every figure here; `seat_count` stays
        # an int-or-None because it is a QUANTITY, and `None` ("no per-seat term")
        # must not be rendered as the string "None" or flattened to 0.
        "llm_usage_usd": str(frozen["llm_usage_usd"]),
        "llm_markup_usd": str(frozen["llm_markup_usd"]),
        # The app-request pair (alembic 0033), returned for the same reason the
        # llm pair is: a re-render that hands back `total_usd` with only SOME of
        # the components that explain it is not reconcilable, and this pair is
        # precisely the one that used to be missing from the arithmetic.
        # ⛔ Both are declared on `models.domain.InvoiceDocument`. That model has
        # no `model_config`, so Pydantic v2 drops an undeclared key SILENTLY at
        # `routers/billing.py` — a 200 whose body lacks the field, with nothing in
        # the logs. Adding a key here without adding it there is invisible.
        "app_request_usage_usd": str(frozen["app_request_usage_usd"]),
        "app_request_markup_usd": str(frozen["app_request_markup_usd"]),
        "seat_fee_usd": str(frozen["seat_fee_usd"]),
        "seat_count": frozen["seat_count"],
        "excluded_unbillable_usd": str(frozen["excluded_unbillable_usd"]),
        "prepaid_credit_applied_usd": str(frozen["prepaid_credit_applied_usd"]),
        "billing_mode": frozen["billing_mode"],
        "issued_by": frozen["issued_by"],
        "lines": frozen["lines"],
        "snapshot": snapshot,
        "inputs": inputs,
        "snapshot_sha256": frozen["snapshot_sha256"],
        "generator_version": frozen["generator_version"],
        "issued_at": frozen["issued_at"].isoformat() if frozen["issued_at"] else None,
        "voided_at": frozen["voided_at"].isoformat() if frozen["voided_at"] else None,
        "void_reason": frozen["void_reason"],
        # Covers snapshot + inputs + every money column. See the docstring for
        # what this asserts now and what it used to.
        "digest_matches": recomputed == frozen["snapshot_sha256"],
        "recomputed_sha256": recomputed,
        "money_matches_snapshot": not money_mismatches,
        "money_mismatches": money_mismatches,
    }


# --------------------------------------------------------------------------- #
# Scheduled close
# --------------------------------------------------------------------------- #


def _previous_month(now: datetime) -> tuple[date, date]:
    """``(period_start, period_end)`` of the last COMPLETED calendar month at ``now``."""
    current_start = date(now.year, now.month, 1)
    prev_end = current_start
    prev_start = (
        date(current_start.year - 1, 12, 1)
        if current_start.month == 1
        else date(current_start.year, current_start.month - 1, 1)
    )
    return prev_start, prev_end


@dataclass(frozen=True)
class CloseCandidates:
    """The tenants a period's close will attempt, split by WHY each one qualified.

    ⛔ THE THREE SETS OVERLAP; THE UNION — NEVER THE SUM — IS THE CANDIDATE
    COUNT. A tenant on a flat access charge that also burns tokens is in ``cost``
    AND in ``access_charge``, and both memberships are true statements about the
    same tenant. The sets are kept apart rather than pre-unioned because the one
    operational question worth asking of this query is per-axis — "did the access
    axis contribute anything at all this month" — and a single set cannot answer
    it. :attr:`access_charge_only` is the subset the axis is the SOLE reason for,
    i.e. exactly the tenants the pre-2026-09-13 definition silently never billed.
    """

    #: Tenants with a ``billing_cost_daily`` row bucketed inside the period.
    cost: frozenset[str]
    #: Tenants with a ``billing_metering_daily`` row bucketed inside the period.
    metering: frozenset[str]
    #: Tenants carrying an access charge EFFECTIVE AND APPLICABLE in the period —
    #: flat (``monthly``/``one_time``) or ``per_seat``. Presence, not amount: see
    #: :func:`find_close_candidates` for why a $0.00 term still lands here.
    access_charge: frozenset[str]

    @property
    def tenant_ids(self) -> list[str]:
        """Every candidate, deduplicated and ordered — what the close iterates.

        Sorted so a sweep's per-tenant log lines and the invoice numbers it draws
        come out in a stable order across replicas; the close is per-tenant
        transactional, so ordering is a readability property, not a correctness
        one.
        """
        return sorted(self.cost | self.metering | self.access_charge)

    @property
    def access_charge_only(self) -> frozenset[str]:
        """Candidates the ACCESS axis alone found — zero usage rows in the period.

        ⭐ THIS IS THE POPULATION THE DEFECT COST MONEY ON. Before the access axis
        was unioned in, a tenant here was not a candidate at all: no invoice, no
        refusal, no skip, no log line, nothing for an alert to key on — while
        ``routers.billing``'s platform overview counted its flat charge as revenue
        via :func:`services.access_charge.active_access_charges_all_tenants`. A
        $5,000/month access charge on a tenant that made no API calls that month
        was reported as revenue with no statement behind it, silently, every
        month, forever.
        """
        return self.access_charge - (self.cost | self.metering)


async def find_close_candidates(
    session: AsyncSession,
    *,
    period_start: date,
    period_end: date,
) -> CloseCandidates:
    """Which tenants could owe a statement for ``[period_start, period_end)``.

    ⭐ ONE DEFINITION, TWO CALLERS, AND THAT IS THE POINT. :func:`close_due_periods`
    uses it to decide who to close; ``services.invoice_closer._measure_invoice_close_lag``
    uses it to decide who SHOULD have been closed. Those two were separate copies
    of "any tenant with a cost or metering row", which meant the backlog gauge was
    blind to precisely the population the close was blind to — the hole could not
    show up as lag, because the instrument shared the defect. Two copies that agree
    today are the shape that drifts; there is one query now.

    THE THREE AXES, and why each is a candidate:

    ``cost`` / ``metering``
        Usage happened. Unchanged from the original definition.

    ``access_charge``
        An intent-to-bill term exists for this period. ``_CHARGED_MONEY_COLUMNS``
        deliberately carries ``access_charge_usd`` and ``seat_fee_usd`` so that a
        commercial term produces a statement even when the total nets to zero —
        but the candidate set never consulted ``tenant_access_charge``, so for a
        tenant with a term and no usage that intent had nothing to act on. Both
        columns are covered here: :func:`services.access_charge.active_access_charges_all_tenants`
        supplies the flat (``monthly``/``one_time``) cadences and
        :func:`services.access_charge.seat_rates_all_tenants` the ``per_seat``
        ones, which the flat aggregation EXCLUDES on purpose (a ``per_seat``
        ``amount_usd`` is a RATE, not a charge). Either function alone would leave
        one of the two money columns unrepresented.

    ⚠️ REUSED, NOT REWRITTEN, and the boundary semantics are theirs, not ours. A
    row qualifies when ``effective_from <= bill_ts`` and ``effective_until IS NULL
    OR effective_until > bill_ts``, with ``bill_ts`` the LAST INSTANT of the period
    (:func:`_bill_ts`) — the same resolution point ``generate_billing`` and
    ``compute_billing_overview`` use. The ``>`` is STRICT: a charge closed at or
    before that instant does not qualify even if it was effective for 27 of the
    month's 28 days. That asymmetry is deliberately inherited rather than softened
    — ``generate_billing`` bills the access axis through the same effective-dated
    match, so a "generous" candidate rule here would nominate tenants whose access
    lines it then computes as $0.00, and the only thing that would produce is a
    period the no-billable-basis skip declines on every future sweep. Candidate
    membership and billability must be answered by the same predicate.

    ⚠️ PRESENCE, NOT AMOUNT. Both helpers key their result by tenant regardless of
    the figure, so a term configured at $0.00 still yields a candidate. Correct
    direction, and not an accident: whether a period charges anything is
    ``close_period``'s decision (the ``_CHARGED_MONEY_COLUMNS`` skip), made after
    the money is computed. A candidate query that pre-judged it would be a second,
    cruder billability gate sited where it cannot see the figures.

    ⛔ THIS GRANTS NO CANDIDATE ANY PRIVILEGE. Everything it returns is a tenant
    id and nothing else — the close cannot tell which axis nominated a tenant, so
    the invoiceability, archived, bucket-gap, unpriced-rate, unpriced-provider,
    seat and byo_mode gates all see it identically, and a term-only candidate whose
    statement charges nothing is skipped by the same ``PeriodNotBillableError`` as
    an internal cost centre's. Adding an axis widens WHO IS ASKED; it must never
    widen who is allowed through.

    Read-only: three grouped/effective-dated SELECTs on the caller's session.
    """
    cost_tenants = {
        r.tenant_id
        for r in (
            await session.execute(
                select(CostDaily.tenant_id)
                .where(
                    CostDaily.bucket_date >= period_start,
                    CostDaily.bucket_date < period_end,
                )
                .group_by(CostDaily.tenant_id)
            )
        ).all()
    }
    metering_tenants = {
        r.tenant_id
        for r in (
            await session.execute(
                select(MeteringDaily.tenant_id)
                .where(
                    MeteringDaily.bucket_date >= period_start,
                    MeteringDaily.bucket_date < period_end,
                )
                .group_by(MeteringDaily.tenant_id)
            )
        ).all()
    }

    bill_ts = _bill_ts(period_end)
    flat = await active_access_charges_all_tenants(
        session, month_start=period_start, month_end=period_end, bill_ts=bill_ts
    )
    per_seat = await seat_rates_all_tenants(
        session, month_start=period_start, month_end=period_end, bill_ts=bill_ts
    )

    return CloseCandidates(
        cost=frozenset(cost_tenants),
        metering=frozenset(metering_tenants),
        access_charge=frozenset(flat) | frozenset(per_seat),
    )


async def close_due_periods(
    session_factory: async_sessionmaker[AsyncSession],
    settings: Settings,
    *,
    now: datetime,
) -> CloseDueOutcome:
    """Close the last completed month for every tenant that could owe for it.

    ⚠️ "COULD OWE" IS WIDER THAN "HAD ACTIVITY", and the difference was a money
    defect. The candidate set is :func:`find_close_candidates` — usage rows OR an
    access charge effective in the period. It used to be usage rows alone, so a
    tenant on a flat monthly access charge who made no API calls that month was
    never even considered: no invoice, and no refusal or skip either, because
    nothing reached a gate. Read that function's docstring for the boundary
    semantics the access axis inherits.

    The :class:`CloseDueOutcome` separates the four things one sweep can do to a
    tenant: ``created`` are the invoices drawn here, ``unchanged`` the periods
    that already held a live invoice when the sweep started, ``refused`` the
    count of tenants a gate DECLINED — :class:`InvoiceError`, or an upstream that
    could not state a figure the invoice needs
    (:class:`services.seats.SeatCountUnavailableError`,
    :class:`services.billing.LicensesUnavailableError`,
    :class:`services.byo_mode.ByoModeUnavailableError`) — and ``failed`` the count
    that died of anything else, i.e. of a defect. See the classification site
    below for why the two must not be conflated.

    ⚠️ ``created`` means "nothing was open when we looked, and we drew one", not
    "we are provably the author". A writer that inserted between our
    :func:`find_open` and ``close_period``'s own ``IntegrityError`` backstop
    re-read would have its row returned to us and counted here. Under the leader
    election this loop is meant to run behind there is no such second writer, so
    the case is theoretical — but it is why ``created`` is not spelled as proof.

    ⚠️ ONE SESSION AND ONE TRANSACTION PER TENANT, deliberately. A shared
    transaction would make one tenant's refusal — a bucket gap, a licences
    outage, a lost race — roll back every tenant closed before it, so a single
    bad tenant would block the whole month. It would also hold the invoice-number
    counter's ``FOR UPDATE`` across the entire fleet's HTTP calls.

    Refusals are logged and SKIPPED, never degraded: a tenant whose period
    cannot be honestly frozen is left unclosed, which is a visible state, rather
    than closed on a total nobody can stand behind.

    ⚠️ This function does no leader election of its own. A caller running it on
    every replica must gate it — ``INVOICE_LOCK_KEY`` in
    :mod:`services.leader_election` exists for that.
    """
    period_start, period_end = _previous_month(now)
    month = f"{period_start.year:04d}-{period_start.month:02d}"

    # ⛔ THE FLOOR IS HERE AND IN THE LAG CENSUS, AND NOWHERE ELSE. A due period
    # strictly before `settings.invoice_first_billable_period` is not billable, so
    # the sweep does not look for candidates, attempts no close and draws no
    # number: "nothing due", the same shape an empty month returns. It is NOT
    # inside `close_period` or `assert_no_bucket_gap` on purpose -- a period at or
    # after the floor must still be refused on a gap, and a manual close of an
    # older period must still meet every gate rather than a new refusal.
    first_billable = settings.invoice_first_billable_period_start
    if first_billable is not None and period_start < first_billable:
        logger.info(
            "invoice_close_due_before_first_billable_period",
            month=month,
            first_billable_period=settings.invoice_first_billable_period,
        )
        return CloseDueOutcome(created=[], unchanged=[], refused=0, failed=0, skipped=0)

    async with session_factory() as session:
        candidates = await find_close_candidates(
            session, period_start=period_start, period_end=period_end
        )
    tenant_ids = candidates.tenant_ids

    # ⚠️ THE THREE LABELS DO NOT PARTITION, so `sum(...) by (source)` is NOT the
    # candidate count — a tenant with usage AND a term increments two of them. It
    # is labelled this way because the question worth asking is per-axis: an
    # `access_charge` series that is flat zero while the estate demonstrably holds
    # open `tenant_access_charge` rows means the union regressed, and no single
    # aggregate number could say that.
    #
    # ⛔ EMITTED HERE AND NOT INSIDE `find_close_candidates`, deliberately. The
    # other caller (`invoice_closer._measure_invoice_close_lag`) runs the same
    # query over up to 24 historical periods on every tick; counting there would
    # add ~24 increments per tick for periods nobody is closing and make the
    # counter's rate a function of the census window rather than of the sweep.
    for source, members in (
        ("cost", candidates.cost),
        ("metering", candidates.metering),
        ("access_charge", candidates.access_charge),
    ):
        observability_metrics.invoice_close_candidates_total.labels(source=source).inc(len(members))

    # Named, not just counted. A tenant the ACCESS axis alone found is one the old
    # definition dropped on the floor — no invoice, no refusal, no skip, nothing —
    # so the ids are logged: a count tells an operator the hole existed, the ids
    # tell them which customers were in it and therefore which back-bills to check.
    if candidates.access_charge_only:
        logger.info(
            "invoice_close_due_access_charge_only_candidates",
            month=month,
            tenant_ids=sorted(candidates.access_charge_only),
            count=len(candidates.access_charge_only),
        )

    created: list[BillingInvoice] = []
    unchanged: list[BillingInvoice] = []
    refused = 0
    failed = 0
    skipped = 0
    for tenant_id in tenant_ids:
        try:
            async with session_factory() as session:
                # ⚠️ ASKED HERE, not inferred from what close_period returns. Its
                # no-op path hands back the EXISTING row, identical in shape to a
                # fresh one, so a sweep that ran hourly over an already-closed
                # month would report a new issue on every tick for the rest of
                # the month. Nothing is committed on this branch: no row is
                # touched, so there is no work to commit.
                existing = await find_open(
                    session,
                    tenant_id=tenant_id,
                    period_start=period_start,
                    period_end=period_end,
                )
                if existing is not None:
                    logger.info(
                        "invoice_close_due_unchanged",
                        tenant_id=tenant_id,
                        month=month,
                        invoice_id=str(existing.id),
                        status=existing.status,
                    )
                    unchanged.append(existing)
                    continue

                invoice = await close_period(
                    session,
                    tenant_id=tenant_id,
                    period_start=period_start,
                    period_end=period_end,
                    settings=settings,
                    actor="close_due_periods",
                )
                await session.commit()
            created.append(invoice)
        except PeriodNotBillableError:
            # ⛔ ORDERED ABOVE THE `InvoiceError` ARM AND THAT IS LOAD-BEARING —
            # it IS an InvoiceError, so a broader arm first would swallow every
            # skip into the refusal count and the distinction the field exists for
            # would be unreachable. `except` arms are tried in order, so this is
            # the only placement that works.
            #
            # No `exc_info`: a skip is the expected outcome for most of the
            # estate's tenants every month (measured 2026-09-13: ~10 internal cost
            # centres with August activity), and a traceback per tenant per tick
            # would bury the refusals that do need reading.
            skipped += 1
            logger.info("invoice_close_due_skipped", tenant_id=tenant_id, month=month)
        except InvoiceError:
            refused += 1
            logger.warning(
                "invoice_close_due_refused", tenant_id=tenant_id, month=month, exc_info=True
            )
        except (
            SeatCountUnavailableError,
            LicensesUnavailableError,
            ByoModeUnavailableError,
            # ⭐ The fourth instance of the same class of event, added 2026-09-13
            # with `assert_tenant_invoiceable`: the licences TENANT REGISTRY could
            # not state its contents. Same remedy as its three siblings (retry
            # when licences answers), same zero cost (no number drawn), and the
            # same reason it must not land on the blind arm below — `tenant_error`
            # pages a human to read a traceback, and a licences blip is not that.
            # ⚠️ Unlike the siblings it refuses EVERY tenant in the sweep rather
            # than one, so a whole month stays open during an outage. That is
            # correct and it is also why it is loud: see the gate's docstring.
            TenantRegistryUnavailableError,
        ) as exc:
            # ⭐ AN UPSTREAM NON-ANSWER IS A REFUSAL, NOT A DEFECT, AND THE
            # DIFFERENCE IS WHAT AN OPERATOR TRIAGES FROM.
            #
            # `refused` is exported as
            # `billing_invoice_close_failures_total{reason="tenant_refused"}` and
            # `failed` as `{reason="tenant_error"}` (services/invoice_closer.py).
            # The Prometheus alert `BillingInvoiceCloseErrored` pages on
            # `tenant_error` PRECISELY to mean "the module broke" — so leaving
            # these three on the blind arm below made every routine licences blip
            # page as a code defect and diluted the one signal that is supposed
            # to mean a human must read a traceback.
            #
            # They belong with InvoiceError because the REMEDY is the same and is
            # the opposite of a defect's: a refusal is self-healing the moment the
            # missing fact is suppliable (licences answers again) and it consumes
            # NO invoice number — the period is simply left open, which is a
            # visible state the next sweep retries. A defect needs a code change
            # and leaves nothing to retry. Each of these three is a gate declining
            # ON PURPOSE rather than the module breaking: see
            # `assert_seat_count_available`, `assert_byo_mode_available` and
            # `fetch_licenses`' call site in `close_period`, all of which refuse so
            # that a fabricated figure cannot be frozen.
            #
            # ⚠️ CLASSIFIED HERE, NOT BY RE-PARENTING THE EXCEPTIONS, and the
            # blast radius is the reason. Measured 2026-09-11: `InvoiceError`
            # itself is caught in exactly ONE place in `src/` — this arm — while
            # `routers/billing.py` maps the CONCRETE subclasses (BucketGapError
            # and UnpricedServiceKeyError -> 409, InvoicePeriodError -> 422,
            # InvoiceStateError -> 409). So re-parenting would not change a
            # response code TODAY; what it would do is silently hand every FUTURE
            # `except InvoiceError` a licences-transport failure, and it would put
            # "the licences service is unreachable" inside the hierarchy whose
            # base docstring is "every refusal THIS MODULE raises" — which is what
            # makes the one-line catch in a router look safe when it is not.
            # ⛔ An HTTP status is a separate decision from a sweep's outcome
            # accounting; only the latter is in scope here, so the knowledge lives
            # at the classification site and each class keeps its own parent.
            #
            # ⚠️ Same log key as the InvoiceError arm on purpose, so one grep finds
            # every refusal — with `refusal` naming the class, because a single
            # undifferentiated key would trade the metric's blindness for the
            # log's.
            refused += 1
            logger.warning(
                "invoice_close_due_refused",
                tenant_id=tenant_id,
                month=month,
                refusal=type(exc).__name__,
                exc_info=True,
            )
        except Exception:
            # Blind on purpose: a transport error or an unexpected shape for ONE
            # tenant must not stop the other tenants' months from being closed.
            # Every one is logged with its tenant. (An upstream non-answer —
            # licences, seats, byo_mode — no longer lands here: it is a named
            # refusal on the arm above.)
            failed += 1
            logger.warning(
                "invoice_close_due_failed", tenant_id=tenant_id, month=month, exc_info=True
            )

    logger.info(
        "invoice_close_due_complete",
        month=month,
        candidates=len(tenant_ids),
        created=len(created),
        unchanged=len(unchanged),
        refused=refused,
        failed=failed,
        # ⚠️ Added to the EXISTING census rather than logged as a second event:
        # two complete-events per sweep would double every dashboard built on this
        # key, and `candidates` only reconciles if all five classes appear beside it.
        skipped=skipped,
    )
    return CloseDueOutcome(
        created=created,
        unchanged=unchanged,
        refused=refused,
        failed=failed,
        skipped=skipped,
    )
