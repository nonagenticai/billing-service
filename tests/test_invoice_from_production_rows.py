"""A NON-ZERO invoice derived from REAL production rows — the only one there is.

WHAT THIS FILE PROVES, AND WHY NOTHING ELSE IN THE SUITE PROVES IT

``billing_invoices`` holds exactly ONE closed row in production: ``INV-2026-000001``,
``total_usd`` **0.00**. That zero is arithmetically CORRECT — every tenant with
closable spend resolves to ``byo_mode`` ``local_allowed``/``platform_only``, which
``services.byo_mode.effective_billing_mode`` maps to ``BILLING_MODE_INTERNAL``, and
an internal tenant bills $0 by design. So the estate has never once produced a
non-zero statement, and the write-side projection has never been exercised on real
money.

``tests/test_commercial_terms.py::TestConfiguredTenantInvoiceRow`` already closes a
non-zero invoice ($5,087.00 = 12.00 + 5000.00 + 75.00), and it is a good test — but
every figure in it is INVENTED ($10.00 of usage on one synthetic bucket). What has
never existed is a non-zero invoice computed from the rows Postgres actually holds.
This file is that proof, and it is built so it cannot rot: every date, amount and
identifier is a FIXED LITERAL measured from production on 2026-09-12, so the test
asserts the same arithmetic in 2027 as today.

THE SOURCE OF EVERY ROW

Tenant ``cdmsfirst`` (``01a1117c-d9b0-72fc-aeda-a9d1187f2a36``) is the only
``byo_required`` — i.e. commercially billable — tenant with attributed spend. The
eight rows in :data:`CDMSFIRST_PRODUCTION_ROWS` are its complete
``billing_cost_daily`` population for 2026-09-05 … 2026-09-11, read with
``attribution_status='attributed'``, all eight on ``service_key='inference-router'``  # pragma: allowlist secret
/ ``provider='pgx-local'`` / ``served_model='local-llm'``, ``byo=False``,
``owner_provided=False``, ``cost_source='unknown'``, under one licence. The ninth
row (``n8n-dev``, $6.696305 on 2026-09-06) is also real and is seeded for a reason
given at :func:`_seed_fleet_presence_for_september`.

WHY PRODUCTION ITSELF CANNOT BILL THIS MONEY — TWO INDEPENDENT REASONS

1. **The terms postdate the spend.** cdmsfirst's real ``markup_rules`` and
   ``tenant_access_charge`` rows carry ``effective_from = 2026-09-11 10:05Z``, which
   is AFTER seven of the eight buckets. ``build_markup_resolver`` and
   ``active_access_charges_for_month`` both resolve at ``bill_ts`` (the last instant
   of the billed month), so the rows would in fact be picked up for September — but
   they describe a period they did not govern. This file therefore seeds the SAME
   terms with ``effective_from = 2026-09-01``, stated here rather than left as a
   silent edit, because that one field is the difference between "these are the
   production terms" and "these are the production terms, back-dated".
2. **September has three destroyed bucket days.** ``billing_cost_daily`` holds no
   rows at all, fleet-wide, for 2026-09-02, 09-03 and 09-04, and
   ``services.invoices.assert_no_bucket_gap`` refuses any period containing them.
   That refusal is correct and is itself asserted here
   (:class:`TestBucketGapRefusalBurnsNoInvoiceNumber`).

⚠️ THE PERIOD THIS FILE CLOSES IS THE CALENDAR MONTH, NOT THE SPEND WINDOW, AND
THAT IS NOT A CHOICE — IT IS A MEASURED CONSTRAINT. A partial window such as
``[2026-09-05, 2026-09-12)`` CANNOT be closed by this service at all:
``close_period``'s very first statement is ``_month_str(period_start, period_end)``,
which raises :class:`~services.invoices.InvoicePeriodError` unless
``period_start.day == 1`` and ``period_end`` is the next month's first, because
``generate_billing`` derives its own bounds from a ``YYYY-MM`` string and cannot
compute a partial period. :class:`TestAPartialSpendWindowCannotBeClosedAtAll` pins
that refusal so the reason this file uses September entire is asserted rather than
asserted in prose. Widening to the month moves NO figure: the eight rows above are
cdmsfirst's complete September population, a ``monthly`` access charge bills in full
for any effective month, and the padding added to satisfy the gap gate is $0.00.
"""

from __future__ import annotations

import uuid
from datetime import UTC, date, datetime, timedelta
from decimal import Decimal

import httpx
import pytest
from sqlalchemy import func, select
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from models.db import (
    ATTRIBUTION_ATTRIBUTED,
    BillingInvoice,
    BillingInvoiceCounter,
    CostDaily,
    MarkupRule,
)
from models.domain import AccessChargeCreate
from services import access_charge as access_charge_svc
from services import seats as seats_svc
from tests.conftest import T

pytestmark = pytest.mark.asyncio


# --------------------------------------------------------------------------- #
# THE REAL DATA — measured from production 2026-09-12. Do not adjust.
# --------------------------------------------------------------------------- #

#: Tenant ``cdmsfirst``: the only ``byo_required`` tenant with attributed spend.
#: A LITERAL production uuid rather than ``T("cdmsfirst")``, because the point of
#: this file is that the identifiers are the ones Postgres holds. Registration in
#: ``tenancy._REGISTERED_TENANT_IDS`` is deliberately NOT arranged: the exclusion
#: it gates (``settings.billing_exclude_unregistered_tenants``) acts on the cost
#: FLUSHER, not on any read in the close path, so vouching for this id
#: here would be a widening that buys nothing.
CDMSFIRST = "01a1117c-d9b0-72fc-aeda-a9d1187f2a36"

#: The one licence all eight rows are attributed to (prod: ``01a06ff2-…``).
CDMSFIRST_LICENSE_ID = uuid.UUID("01a06ff2-0000-7000-8000-000000000001")

#: ⚠️ THREE OF THE EIGHT ROWS SHARE ``bucket_date = 2026-09-07``, AND THEY MUST
#: DIFFER IN A KEY COLUMN OR THE INSERT CONFLICTS. ``uq_cost_daily`` is
#: ``(tenant_id, license_id, subject_id, service_key, provider, served_model, byo,
#: owner_provided, cost_source, bucket_date)`` — every one of those is identical
#: across the three except ``subject_id``, which is exactly how they are separate
#: rows in production (three distinct end users inside one licence on one day). So
#: the three carry DISTINCT ``subject_id`` values below, and the other five share
#: one. This is reproducing the production shape, not working around a constraint.
#:
#: ⭐ It also cannot move the money: ``services.billing``'s cost query groups by
#: ``(license_id, service_key, provider, served_model, byo)`` and NOT by
#: ``subject_id``, so all eight rows collapse into ONE billed line whose incurred
#: cost is their sum, whatever the subjects are.
_SUBJ_A = uuid.UUID("0195f48b-d000-7000-8000-0000000000a1")
_SUBJ_B = uuid.UUID("0195f48b-d000-7000-8000-0000000000b2")
_SUBJ_C = uuid.UUID("0195f48b-d000-7000-8000-0000000000c3")

#: ``(bucket_date, total_input_tokens, total_output_tokens, total_cost_usd,
#: request_count, subject_id)`` — cdmsfirst's COMPLETE attributed
#: ``billing_cost_daily`` population for the spend window, verbatim.
CDMSFIRST_PRODUCTION_ROWS: tuple[tuple[date, int, int, str, int, uuid.UUID], ...] = (
    (date(2026, 9, 5), 24617, 1006, "0.271320", 25, _SUBJ_A),
    (date(2026, 9, 7), 86878, 8094, "1.071130", 127, _SUBJ_A),
    (date(2026, 9, 7), 2328, 469, "0.035005", 7, _SUBJ_B),
    (date(2026, 9, 7), 148838, 19482, "1.975430", 258, _SUBJ_C),
    (date(2026, 9, 8), 87397, 9434, "1.109820", 101, _SUBJ_A),
    (date(2026, 9, 9), 268114, 31698, "3.473590", 360, _SUBJ_A),
    (date(2026, 9, 10), 387294, 42602, "4.937990", 486, _SUBJ_A),
    (date(2026, 9, 11), 81291, 7903, "1.010485", 77, _SUBJ_A),
)

#: The sum of the eight rows' ``total_cost_usd``. ⭐ ASSERTED AGAINST THE SEEDED
#: FIXTURE IN EVERY TEST BELOW (``_assert_seeded_usage_is_the_measured_sum``) so a
#: future edit to the table above cannot silently change the expected invoice total
#: — the fixture and the expectation are pinned to each other, not merely adjacent.
MEASURED_USAGE_USD = Decimal("13.884770")

#: A REAL production row belonging to a DIFFERENT tenant, on the one day cdmsfirst
#: has no spend. See :func:`_seed_fleet_presence_for_september` for why it is here
#: and :meth:`TestCrossTenantLeak.test_the_other_tenants_spend_is_absent` for the
#: assertion that its money never reaches cdmsfirst's statement.
OTHER_TENANT = T("n8n-dev")
OTHER_TENANT_LICENSE_ID = uuid.UUID("01a06ff2-0000-7000-8000-0000000000f9")
OTHER_TENANT_SPEND_USD = Decimal("6.696305")
OTHER_TENANT_BUCKET = date(2026, 9, 6)

#: Carries the $0.00 fleet-presence padding. A THIRD tenant rather than cdmsfirst
#: (which ``tests/test_commercial_terms.py::_pad_month_for_bucket_gap`` uses for
#: its own padding) so that cdmsfirst's ``billing_cost_daily`` population in this
#: file is EXACTLY the eight production rows and nothing else — a reader comparing
#: the fixture against the production query cannot be confused by a padding row
#: wearing the subject's tenant_id.
PADDING_TENANT = T("fleet-presence-padding")

#: The period actually closed: September 2026 entire, ``period_end`` EXCLUSIVE.
PERIOD_START = date(2026, 9, 1)
PERIOD_END = date(2026, 10, 1)

#: The spend window the operator named. ⛔ UNCLOSEABLE BY CONSTRUCTION — see the
#: module docstring and :class:`TestAPartialSpendWindowCannotBeClosedAtAll`.
SPEND_WINDOW_START = date(2026, 9, 5)
SPEND_WINDOW_END = date(2026, 9, 12)

#: The three days production lost. ``assert_no_bucket_gap`` refuses on any of them.
DESTROYED_BUCKET_DAYS = (date(2026, 9, 2), date(2026, 9, 3), date(2026, 9, 4))


# --------------------------------------------------------------------------- #
# THE EXPECTED ARITHMETIC — fixed constants. ⛔ Never fitted to the output.
# --------------------------------------------------------------------------- #
#
#   usage incurred               13.884770
#   + 20% markup rule      x1.20 16.661724 -> NOT rounded here; see below
#   flat monthly access fee       5000.00   (billed IN FULL for an effective
#                                            month; `active_access_charges_for_
#                                            month._applies` returns True for
#                                            'monthly' with no proration term
#                                            anywhere in the path)
#   seats                  3x25      75.00
#                                 ---------
#   total_usd                      5091.66
#   internal_cost_usd                13.88   (quantise(13.884770))
#   margin_usd                     5077.78   (total - internal, by construction)

#: ⚠️ CHANGED 2026-09-15, AND IT IS A COMPONENT — NOT THE TOTAL. The total is
#: ``EXPECTED_TOTAL_USD`` below and it is UNMOVED at ``5091.66``; the invoice
#: charges cdmsfirst exactly what it charged before. What changed is the SCALE at
#: which the usage axis is recorded on the row: money is now quantised ONCE per
#: invoice instead of once per billed line, so ``llm_usage_usd + llm_markup_usd``
#: is the true ``13.884770 x 1.20 = 16.661724`` rather than a per-line rounding of
#: it. The eight production rows share one (licence, project, app, provider,
#: model, byo) key, so they GROUP into a single billed line — which is exactly why
#: the total cannot move here: with one line, "round each line" and "round the
#: total" were always the same arithmetic. This file's value therefore proves the
#: scale change and NOT a repricing, and that is the whole reason it is safe.
#:
#: ⛔ Do not "restore" 16.66. It would re-pin the defect: a rollup key gaining a
#: column (as ``project_id`` did) splits these eight rows into several lines, and
#: under per-line rounding that alone would move the money.
EXPECTED_BILLED_USAGE_USD = Decimal("16.661724")
EXPECTED_ACCESS_CHARGE_USD = Decimal("5000.00")
EXPECTED_SEAT_FEE_USD = Decimal("75.00")
EXPECTED_SEAT_COUNT = 3
EXPECTED_TOTAL_USD = Decimal("5091.66")
EXPECTED_INTERNAL_COST_USD = Decimal("13.88")
EXPECTED_MARGIN_USD = Decimal("5077.78")


# --------------------------------------------------------------------------- #
# Harness — mirrored from tests/test_commercial_terms.py, not re-invented
# --------------------------------------------------------------------------- #
#
# COPIED RATHER THAN IMPORTED, deliberately. Importing a fixture by name into
# another module works in pytest but reads to ruff as an unused import (the
# fixture is consumed by parameter name, not by reference), so the import would
# have to be noqa'd in a way that hides a genuinely dead one later. The copies
# below are annotated with what they are for; the originals, and the measured
# incidents behind each warning, are at tests/test_commercial_terms.py:55-150.


@pytest.fixture(autouse=True)
def _seat_cache_isolated():
    """The seat cache is module state with a 5-minute TTL — one test's answer must
    not decide another's. Cleared both sides so ordering cannot matter."""
    seats_svc.clear_seat_cache()
    yield
    seats_svc.clear_seat_cache()


@pytest.fixture
def seats_say(monkeypatch):
    """Stub the licences SEAT-COUNT endpoint, and every other GET the close makes.

    ⚠️ URL-AWARE ON PURPOSE, same as the original. ``httpx.AsyncClient.get`` is
    also how ``services.billing.fetch_licenses`` retrieves the licence LIST and how
    ``services.byo_mode`` retrieves ``byo_mode``, so a blanket stub feeds one
    caller another's shape and surfaces as an ``AttributeError`` deep inside
    ``generate_billing`` that looks like a billing bug.

    ⭐ AND IT ANSWERS EVERY CALL, NOT ONLY THE LAST. ``close_period`` makes TWO
    seat lookups — ``assert_seat_count_available`` (the gate) and then
    ``generate_billing`` (the number that is FROZEN) — so a stub that served one
    would leave the other raising for a reason unrelated to what is asserted here.

    ⭐ ``byo-mode`` ANSWERS ``byo_required``, AND THAT IS THE WHOLE REASON THIS
    FILE CAN PRODUCE A NON-ZERO INVOICE AT ALL. ``effective_billing_mode`` maps
    ``local_allowed``/``platform_only`` to ``BILLING_MODE_INTERNAL``, which bills
    $0 — the measured state of every production tenant with closable spend, and
    the reason the estate's one real invoice is correctly 0.00. cdmsfirst's real
    answer is ``byo_required``, so it keeps its configured mode
    (``standard_markup``, the default for a tenant with no ``tenant_config`` row).
    """

    def _install(seats: int | None = EXPECTED_SEAT_COUNT, status_code: int = 200):
        async def _fake_get(self, url, *a, **kw):  # noqa: ANN001
            if "seat-count" in str(url):
                payload = {"tenant_id": CDMSFIRST, "seats": seats, "basis": "active_licences"}
                return httpx.Response(status_code, json=payload)
            if "byo-mode" in str(url):
                return httpx.Response(200, json={"byo_mode": "byo_required"})
            # ⛔ THE REGISTRY IS NOT A LABEL LOOKUP ANY MORE, and this arm had to be
            # split out on 2026-09-13 because of it. The catch-all below used to
            # serve `/internal/tenants` an empty list, with the (then true) comment
            # that "labels degrade to empty; no figure comes from this call".
            # `services.invoices.assert_tenant_invoiceable` now REFUSES a close for
            # a tenant that registry does not vouch for, so an empty list is no
            # longer a degraded label — it is every tenant in the estate reading as a
            # phantom, and this file's four closes all refused on it.
            if str(url).endswith("/internal/tenants"):
                return httpx.Response(
                    200,
                    json=[
                        {"id": t, "name": n, "kind": "customer", "archived": False}
                        for t, n in (
                            (CDMSFIRST, "cdmsfirst"),
                            (OTHER_TENANT, "n8n-dev"),
                            (PADDING_TENANT, "fleet-presence-padding"),
                        )
                    ],
                )
            # fetch_licenses wants a list. Labels degrade to empty; no figure on the
            # statement comes from this call.
            return httpx.Response(200, json=[])

        monkeypatch.setattr(httpx.AsyncClient, "get", _fake_get)

    return _install


class _StubTokenProvider:
    """A service-account token provider that always has a token."""

    async def get_token(self) -> str:
        # pragma: allowlist secret - a fixed test string, not a credential
        return "test-service-token"  # pragma: allowlist secret

    def build_headers(self, tenant_id: str) -> dict[str, str]:
        return {  # pragma: allowlist secret
            "X-Tenant-Id": tenant_id,
            "Authorization": "Bearer test-service-token",  # pragma: allowlist secret
        }


@pytest.fixture(autouse=True)
def _seat_service_token(monkeypatch):
    """``/internal/`` routes on licences need a service token and ``services.seats``
    fails CLOSED without one — it raises BEFORE the HTTP call, so a missing token
    would make every test here raise ``SeatCountUnavailableError`` for a reason
    unrelated to what it asserts. The mint is a POST; this module stubs GET."""
    monkeypatch.setattr(seats_svc, "_provider_for", lambda _s: _StubTokenProvider())


async def _rows(db_engine, model):
    """Read a table through a SEPARATE session.

    ⭐ NOT a re-SELECT on the seeding session: its identity map would hand back the
    very Python instance ``close_period`` built in memory, so a "re-read" could not
    see a value Postgres refused, defaulted or truncated. A fresh session has an
    empty identity map, so every figure asserted below came out of the database.
    """
    factory = async_sessionmaker(bind=db_engine, class_=AsyncSession, expire_on_commit=False)
    async with factory() as s:
        return list((await s.execute(select(model))).scalars().all())


async def _counter_state(db_engine) -> dict[str, int]:
    """``{scope: next_seq}`` from ``billing_invoice_counters``, via a fresh session."""
    factory = async_sessionmaker(bind=db_engine, class_=AsyncSession, expire_on_commit=False)
    async with factory() as s:
        rows = (
            await s.execute(select(BillingInvoiceCounter.scope, BillingInvoiceCounter.next_seq))
        ).all()
    return {r.scope: int(r.next_seq) for r in rows}


# --------------------------------------------------------------------------- #
# Seeding — the production rows, the production terms, and the gap padding
# --------------------------------------------------------------------------- #


def _cost_row(
    *,
    tenant_id: str,
    tenant_name: str,
    license_id: uuid.UUID,
    subject_id: uuid.UUID,
    bucket_date: date,
    inp: int,
    outp: int,
    cost: str,
    requests: int,
) -> CostDaily:
    """One ``billing_cost_daily`` row in the production shape.

    Every non-varying column is the measured production value:
    ``service_key='inference-router'``, ``provider='pgx-local'``,  # pragma: allowlist secret
    ``served_model='local-llm'``, ``byo=False``, ``owner_provided=False``,
    ``cost_source='unknown'``, ``attribution_status='attributed'``.

    ``unpriced_event_count`` is left at 0. ⚠️ On a row bucketed before
    ``services.invoices._UNPRICED_EVENT_COUNT_MEASURED_FROM`` (2026-09-12) that 0
    means "NOT MEASURED", not "none unpriced" — which is precisely why
    ``assert_no_unpriced_provider_cost`` REPORTS these days as
    ``unmeasurable_rows`` instead of passing them as clean, and why it does not
    refuse them either. Every bucket in this file is in that window, so that gate
    is a no-op here by design rather than by luck.
    """
    return CostDaily(
        tenant_id=tenant_id,
        tenant_name=tenant_name,
        license_id=license_id,
        subject_id=subject_id,
        service_key="inference-router",  # pragma: allowlist secret
        provider="pgx-local",
        served_model="local-llm",
        byo=False,
        owner_provided=False,
        cost_source="unknown",
        attribution_status=ATTRIBUTION_ATTRIBUTED,
        bucket_date=bucket_date,
        total_input_tokens=inp,
        total_output_tokens=outp,
        total_cost_usd=Decimal(cost),
        request_count=requests,
    )


def _seed_cdmsfirst_spend(session) -> None:
    """The eight measured rows, nothing else, on cdmsfirst's tenant_id."""
    for bucket, inp, outp, cost, requests, subject in CDMSFIRST_PRODUCTION_ROWS:
        session.add(
            _cost_row(
                tenant_id=CDMSFIRST,
                tenant_name="cdmsfirst",
                license_id=CDMSFIRST_LICENSE_ID,
                subject_id=subject,
                bucket_date=bucket,
                inp=inp,
                outp=outp,
                cost=cost,
                requests=requests,
            )
        )


def _seed_fleet_presence_for_september(session, *, omit: tuple[date, ...] = ()) -> None:
    """Make every September day present FLEET-WIDE, because that is what the gate asks.

    ⚠️ ``assert_no_bucket_gap`` IS FLEET-WIDE DAY PRESENCE, NOT TENANT-SCOPED, and
    the distinction is the whole design: one tenant legitimately has quiet days, so
    "cdmsfirst wrote no row on the 6th" is evidence of nothing, while "NOBODY wrote
    a row on the 6th" means the rollup did not run. cdmsfirst's own eight rows cover
    only 09-05 and 09-07…09-11, so closing September needs the other 22 days present
    from SOMEBODY.

    ⭐ 2026-09-06 IS SATISFIED BY A REAL ROW FROM A REAL OTHER TENANT — ``n8n-dev``,
    ``inference-router``/``pgx-local``, $6.696305, ``attribution_status='attributed'``  # pragma: allowlist secret
    — because production has exactly that row and because it doubles as a
    CROSS-TENANT-LEAK CONTROL. ⛔ ITS MONEY MUST NOT APPEAR ON CDMSFIRST'S INVOICE:
    it belongs to a different tenant, and every query on the close path is filtered
    by ``tenant_id``. That is not taken on trust here —
    ``TestCrossTenantLeak.test_the_other_tenants_spend_is_absent`` asserts the
    absence of $6.696305 from each of cdmsfirst's money columns, and would fail if
    any of those filters were dropped.

    ⭐ THE REMAINING PADDING IS $0.00 AND ZERO REQUESTS, on a THIRD tenant. It makes
    the gate pass without moving a single figure the assertions state, so the
    arithmetic below stays the arithmetic of the EIGHT measured rows. (Same device
    as ``tests/test_commercial_terms.py::_pad_month_for_bucket_gap``; a smaller
    fixture that satisfies the real gate does not exist.)

    :param omit: days to leave ABSENT fleet-wide, so a caller can reproduce
        production's destroyed buckets and exercise the refusal.
    """
    session.add(
        _cost_row(
            tenant_id=OTHER_TENANT,
            tenant_name="n8n-dev",
            license_id=OTHER_TENANT_LICENSE_ID,
            subject_id=_SUBJ_A,
            bucket_date=OTHER_TENANT_BUCKET,
            inp=0,
            outp=0,
            cost=str(OTHER_TENANT_SPEND_USD),
            requests=1,
        )
    )

    covered = {r[0] for r in CDMSFIRST_PRODUCTION_ROWS} | {OTHER_TENANT_BUCKET}
    day = PERIOD_START
    while day < PERIOD_END:
        if day not in covered and day not in omit:
            session.add(
                _cost_row(
                    tenant_id=PADDING_TENANT,
                    tenant_name="fleet-presence-padding",
                    license_id=uuid.UUID("01a06ff2-0000-7000-8000-0000000000fa"),
                    subject_id=_SUBJ_A,
                    bucket_date=day,
                    inp=0,
                    outp=0,
                    cost="0",
                    requests=0,
                )
            )
        day += timedelta(days=1)


async def _seed_cdmsfirst_terms(session) -> MarkupRule:
    """cdmsfirst's real production terms, back-dated to cover the spend.

    ⚠️ ``effective_from = 2026-09-01``. Production's rows start at
    ``2026-09-11 10:05Z``, AFTER seven of the eight buckets — which is the first of
    the two reasons production cannot bill this money (module docstring). The date
    is the ONE field changed from the measured value, and it is changed here rather
    than silently.

    ⭐ THE MARKUP RULE IS TENANT-PINNED, AND THAT PIN IS LOAD-BEARING.
    ``compute_billed_line_detailed`` only honours a resolved rule when
    ``_specificity(rule) > 0``; ``_W_TENANT = 1``, so a rule pinned to cdmsfirst and
    wildcard on everything else scores 1 and WINS the line. A rule with
    ``tenant_id`` NULL too would score 0, match, be DECLINED in favour of
    ``fallback_pct``, and never appear in ``markup_sources_applied`` — the statement
    would read 20% off ``settings.default_markup_pct`` (also 20.0) and nothing in
    the total could tell the two apart. ``TestSnapshotReasoning`` is what separates
    them.

    ⚠️ TWO RULES, ``byo=None`` AND ``byo=True``, matching production. Only the
    ``byo=None`` row can bill these lines — all eight rows are ``byo=False``, and
    ``MarkupRule.byo`` has three meanings (True = BYO-only, False =
    platform-provided-only, None = both). The ``byo=True`` row is seeded because it
    EXISTS in production and because ``_W_BYO = 64`` (post-0036) is the highest specificity
    weight: if it ever started matching a platform-provided line, it would outrank
    the rule that should bill it, and the total here would move.

    ⛔ NO ``tenant_config`` ROW IS SEEDED, and that is deliberate rather than an
    omission. cdmsfirst has none in production (``tenant_config`` held 0 rows when
    last measured), so ``get_markup_pct_with_source`` answers
    ``(20.0, 'settings_default')`` and the invoice's ``markup_pct`` /
    ``markup_source`` COLUMNS say ``settings_default`` — while the usage LINE is
    billed by the rule. Both facts are asserted; see ``TestSnapshotReasoning``'s
    docstring for why the column cannot carry ``markup_rule:<id>`` at all.

    ⛔ NO ``billing_app_cost_rates`` ROW EITHER. Production holds zero, and there
    are no ``billing_metering_daily`` rows in this fixture, so
    ``assert_all_service_keys_priced`` (which reads METERING, not COST) sees no
    billed ``service_key`` and is inert. An app-request line would add money the
    expected arithmetic does not contain.

    :returns: the ``byo=None`` ``MarkupRule``, so a caller can name it with
        ``services.markup.markup_source_for_rule`` rather than inventing a literal.
    """
    terms_effective_from = datetime(2026, 9, 1, tzinfo=UTC)

    tenant_wide_rule = MarkupRule(
        tenant_id=CDMSFIRST,
        markup_pct=Decimal("20.00"),
        byo=None,
        effective_from=terms_effective_from,
        effective_until=None,
    )
    session.add(tenant_wide_rule)
    session.add(
        MarkupRule(
            tenant_id=CDMSFIRST,
            markup_pct=Decimal("20.00"),
            byo=True,
            effective_from=terms_effective_from,
            effective_until=None,
        )
    )

    # ⚠️ The flat platform fee owns ``service_key`` NULL and the seat rate a
    # DISTINCT key (``__seat__``), or ``ix_accesscharge_open`` — one open row per
    # (tenant, coalesce(service_key,'')) — forces one to close the other.
    for key, cadence, amount in (
        (None, "monthly", Decimal("5000.00")),
        ("__seat__", "per_seat", Decimal("25.00")),  # pragma: allowlist secret
    ):
        await access_charge_svc.upsert_access_charge(
            session,
            AccessChargeCreate(
                tenant_id=CDMSFIRST,
                service_key=key,
                amount_usd=amount,
                cadence=cadence,
                effective_from=terms_effective_from,
            ),
        )

    await session.flush()
    return tenant_wide_rule


async def _assert_seeded_usage_is_the_measured_sum(session) -> None:
    """⭐ THE FIXTURE AND THE EXPECTATION, PINNED TO EACH OTHER.

    Asserted from the DATABASE before any close, so an edit to
    :data:`CDMSFIRST_PRODUCTION_ROWS` that changes cdmsfirst's total cannot quietly
    re-point the expected invoice figures at a different number — it fails HERE,
    naming the fixture, instead of failing an invoice assertion that looks like a
    billing defect.
    """
    seeded = (
        await session.execute(
            select(func.coalesce(func.sum(CostDaily.total_cost_usd), 0)).where(
                CostDaily.tenant_id == CDMSFIRST,
                CostDaily.bucket_date >= PERIOD_START,
                CostDaily.bucket_date < PERIOD_END,
            )
        )
    ).scalar_one()
    assert Decimal(seeded) == MEASURED_USAGE_USD, (
        f"the seeded production rows sum to {seeded}, not the measured "
        f"{MEASURED_USAGE_USD}: the fixture was edited without the expected "
        "invoice arithmetic being re-derived"
    )


async def _seed_everything(session) -> MarkupRule:
    """The full fixture: eight real rows + fleet presence + back-dated terms."""
    _seed_cdmsfirst_spend(session)
    _seed_fleet_presence_for_september(session)
    rule = await _seed_cdmsfirst_terms(session)
    await _assert_seeded_usage_is_the_measured_sum(session)
    return rule


async def _close_september(db_session, test_settings) -> BillingInvoice:
    """Close September 2026 for cdmsfirst and commit.

    ⚠️ ``clear_byo_cache()`` FIRST. ``byo_mode`` is module state with a 5-minute
    TTL, exactly like the seat cache, and ``_seat_cache_isolated`` covers only
    seats. A neighbour test's cached ``local_allowed``/``platform_only`` would
    collapse this tenant to ``BILLING_MODE_INTERNAL`` and bill $0 — every
    assertion below would read 0.00 and name the wrong cause.
    """
    from services.byo_mode import clear_byo_cache
    from services.invoices import close_period

    clear_byo_cache()
    inv = await close_period(
        db_session,
        tenant_id=CDMSFIRST,
        period_start=PERIOD_START,
        period_end=PERIOD_END,
        settings=test_settings,
    )
    await db_session.commit()
    return inv


# --------------------------------------------------------------------------- #
# 1. The money
# --------------------------------------------------------------------------- #


@pytest.mark.unit
class TestInvoiceFromProductionRows:
    """⭐ THE ONE NON-ZERO INVOICE DERIVED FROM REAL ROWS.

    Every figure asserted here is non-zero and non-default by construction, which
    is what separates this from the estate's one real statement: a hardcoded 0, a
    dropped key in the ``totals``->column projection, or a ``None`` ``seat_count``
    each make it red. (Every money column but ``subtotal_usd``/``total_usd`` carries
    ``server_default "0"``, so an all-zero assertion cannot tell "correctly
    projected" from "never written".)
    """

    async def test_the_eight_production_rows_bill_to_the_expected_money(
        self, db_session: AsyncSession, db_engine, test_settings, seats_say
    ) -> None:
        """⛔ THE ARITHMETIC, ON THE PERSISTED ROW, FROM MEASURED INPUTS.

        usage 13.884770 x 1.20 = 16.661724 -> 16.66
        + 5000.00 flat monthly + 3 x 25.00 seats = total 5091.66
        internal 13.88, margin 5077.78.
        """
        seats_say(seats=EXPECTED_SEAT_COUNT)
        await _seed_everything(db_session)
        await db_session.commit()

        await _close_september(db_session, test_settings)

        rows = await _rows(db_engine, BillingInvoice)
        assert len(rows) == 1, f"expected exactly one invoice, got {len(rows)}"
        row = rows[0]
        assert row.tenant_id == CDMSFIRST

        # The three components, each in its own column and each disjoint.
        assert row.access_charge_usd == EXPECTED_ACCESS_CHARGE_USD, (
            "the $5,000 'monthly' platform fee did not bill in full for this "
            f"period; the column reads {row.access_charge_usd}. If this is a "
            "PRORATED figure, the flat-fee cadence has changed behaviour and the "
            "expected total in this file must be re-derived from the new rule — "
            "it must not be edited to match"
        )
        assert row.seat_count == EXPECTED_SEAT_COUNT
        assert row.seat_fee_usd == EXPECTED_SEAT_FEE_USD  # 3 x $25

        # ⭐ THE USAGE AXIS, TIED TO THE MEASURED ROWS AT FULL PRECISION.
        # llm_usage_usd is summed RAW from the snapshot's per-licence lines, so it
        # is the production sum itself and not a rounded restatement of it.
        assert row.llm_usage_usd == MEASURED_USAGE_USD, (
            f"the invoice's incurred LLM cost is {row.llm_usage_usd}, not the "
            f"{MEASURED_USAGE_USD} the eight production rows hold"
        )
        assert row.llm_usage_usd + row.llm_markup_usd == EXPECTED_BILLED_USAGE_USD

        # The total, and the two figures it balances against.
        assert row.total_usd == EXPECTED_TOTAL_USD  # 16.66 + 5000.00 + 75.00
        assert row.subtotal_usd == EXPECTED_TOTAL_USD  # no tax: subtotal == total
        assert row.internal_cost_usd == EXPECTED_INTERNAL_COST_USD
        assert row.margin_usd == EXPECTED_MARGIN_USD

        # ⭐ AND THE EQUATION ITSELF, not just the three values. An invoice on
        # which total - internal != margin cannot be reconciled by anyone, and the
        # three were independently rounded once.
        assert row.total_usd - row.internal_cost_usd == row.margin_usd

    async def test_the_invoice_is_issued_numbered_and_digested(
        self, db_session: AsyncSession, db_engine, test_settings, seats_say
    ) -> None:
        """A statement that is not issued, numbered and hashed is not a statement.

        ``invoice_number`` comes from ``billing_invoice_counters`` under
        ``FOR UPDATE`` inside the caller's transaction — never from a sequence —
        precisely so a rollback takes the increment with it and the series stays
        gapless. ``snapshot_sha256`` covers the snapshot, the inputs AND the money
        columns, so the figures asserted above are inside the digest.
        """
        seats_say(seats=EXPECTED_SEAT_COUNT)
        await _seed_everything(db_session)
        await db_session.commit()

        await _close_september(db_session, test_settings)

        row = (await _rows(db_engine, BillingInvoice))[0]
        assert row.status == "issued", f"the close did not reach issue(): {row.status!r}"
        assert row.invoice_number is not None, "an issued invoice with no number"
        # Series + PERIOD year + 6-digit seq, first draw of a truncated table.
        assert row.invoice_number == "INV-2026-000001", row.invoice_number
        assert row.issued_at is not None
        assert row.snapshot_sha256, "no digest: the frozen document is unverifiable"
        assert len(row.snapshot_sha256) == 64

        # ⭐ THE NUMBER WAS DRAWN FROM THE SERIES, AND THAT IS WHAT MAKES THE
        # REFUSAL CONTROL BELOW NON-VACUOUS: it asserts the counter does NOT move,
        # which is only meaningful because this test shows it does.
        #
        # ⚠️ The scope is NOT hardcoded here. ``_counter_scope`` spells it
        # ``'<legal_entity or _DEFAULT_NUMBER_SERIES>:<PERIOD year>'`` and the
        # rendered number uses the SAME ``series`` value, so deriving the scope from
        # the number cross-checks the two rather than pinning a literal that the
        # model's own docstring still gets wrong (it says ``'NA:2026'``; the code
        # says ``INV``).
        counters = await _counter_state(db_engine)
        assert len(counters) == 1, counters
        ((scope, next_seq),) = counters.items()
        assert next_seq == 2, f"one invoice was issued; next_seq is {next_seq}"
        assert scope == f"{row.invoice_number.split('-')[0]}:{row.period_start.year}"


# --------------------------------------------------------------------------- #
# 2. The snapshot's own reasoning
# --------------------------------------------------------------------------- #


@pytest.mark.unit
class TestSnapshotReasoning:
    """⭐ READ THE SNAPSHOT'S REASONING, NOT JUST ITS TOTAL.

    ⚠️ ``settings.default_markup_pct`` IS ALSO 20.0. So ``markup_pct == 20.00`` and
    ``total_usd == 5091.66`` hold whether the seeded commercial RULE billed the
    usage or the code constant did — a prior round filed a false defect by judging
    a total without reading the snapshot. The discriminator is the snapshot's own
    ``markup_sources_applied`` set.

    ⚠️ AND THE COLUMN CANNOT CARRY THE ANSWER. ``BillingInvoice.markup_source`` is
    wired to ``get_markup_pct_with_source``, whose only three answers are
    ``tenant_config``, ``tenant_config_default`` and ``settings_default`` — it can
    never read ``markup_rule:<id>``. So asserting the column would prove nothing
    about which rate billed the line, and this class asserts the per-line
    provenance the generator records instead.
    """

    async def test_the_usage_line_was_billed_by_the_seeded_rule_not_the_code_constant(
        self, db_session: AsyncSession, db_engine, test_settings, seats_say
    ) -> None:
        from services.markup import MARKUP_SOURCE_SETTINGS, markup_source_for_rule

        seats_say(seats=EXPECTED_SEAT_COUNT)
        rule = await _seed_everything(db_session)
        await db_session.commit()

        await _close_september(db_session, test_settings)

        row = (await _rows(db_engine, BillingInvoice))[0]
        applied = row.snapshot["totals"]["markup_sources_applied"]

        assert markup_source_for_rule(rule) in applied, (
            f"{applied} does not name the seeded rule: cdmsfirst's tenant-scoped "
            "markup rule billed nothing, the usage line fell back to "
            "settings.default_markup_pct, and the 20% in this invoice is a "
            "coincidence rather than the rule being applied"
        )
        assert MARKUP_SOURCE_SETTINGS not in applied, (
            f"{applied} names the code constant: a line on this statement was "
            "billed at settings.default_markup_pct, which is the provenance this "
            "test exists to refute"
        )
        # Exactly one provenance: one billed line, one source. A second entry would
        # mean a line was priced from somewhere this fixture did not configure.
        assert applied == [markup_source_for_rule(rule)]

        # ⭐ AND THE $5,000 CAME FROM A REAL TERM, not a placeholder line. The
        # generator always emits a platform-fee line; `basis` is what says whether
        # an amount is behind it. `no_matching_access_charge` would mean nothing
        # matched, `not_applicable_internal_tenant` that the tenant bills $0.
        from services.billing import ACCESS_BASIS_CHARGE

        charges = row.snapshot["totals"]["access_charges"]
        bases = {c["basis"] for c in charges}
        assert bases == {ACCESS_BASIS_CHARGE}, (
            f"access-charge bases are {bases}: at least one line on this statement "
            "is a placeholder, not a configured tenant_access_charge"
        )
        flat = next(c for c in charges if c["cadence"] == "monthly")
        assert Decimal(flat["amount_usd"]) == EXPECTED_ACCESS_CHARGE_USD
        # The seat line itemises rate AND quantity: "$75" alone is not
        # reconcilable against a rate card.
        seat = next(c for c in charges if c["cadence"] == "per_seat")
        assert Decimal(seat["seat_rate_usd"]) == Decimal("25.00")
        assert seat["seat_count"] == EXPECTED_SEAT_COUNT
        assert seat["seat_basis"] == "active_licences"

        # The mode that makes any of this billable at all.
        assert row.billing_mode == "standard_markup", (
            f"billing_mode is {row.billing_mode!r}: if this is 'internal', the "
            "byo_mode stub did not reach the close and every figure above is a 0 "
            "wearing the wrong explanation"
        )


# --------------------------------------------------------------------------- #
# 3. The cross-tenant control
# --------------------------------------------------------------------------- #


@pytest.mark.unit
class TestCrossTenantLeak:
    async def test_the_other_tenants_spend_is_absent_from_cdmsfirsts_invoice(
        self, db_session: AsyncSession, db_engine, test_settings, seats_say
    ) -> None:
        """⭐ $6.696305 OF SOMEONE ELSE'S MONEY IS IN THE TABLE AND MUST NOT BE HERE.

        The ``n8n-dev`` row is seeded because ``assert_no_bucket_gap`` needs
        2026-09-06 present fleet-wide (see
        :func:`_seed_fleet_presence_for_september`). Having put a real other
        tenant's spend inside the closed period, this test is what stops it being a
        hazard: every query on the close path is filtered by ``tenant_id``, and if
        any of those filters were dropped the amounts below would move.

        ⚠️ THE CONTROL IS NON-VACUOUS ONLY BECAUSE THE ROW IS IN RANGE. It is
        asserted to be both present and inside ``[period_start, period_end)``
        first — an out-of-period row could not leak regardless of the filters, and
        the absence assertions would then prove nothing.
        """
        seats_say(seats=EXPECTED_SEAT_COUNT)
        await _seed_everything(db_session)
        await db_session.commit()

        # The leak is possible: the row exists, belongs to another tenant, and sits
        # inside the very period being closed.
        other = (
            (await db_session.execute(select(CostDaily).where(CostDaily.tenant_id == OTHER_TENANT)))
            .scalars()
            .all()
        )
        assert len(list(other)) == 1
        assert other[0].total_cost_usd == OTHER_TENANT_SPEND_USD
        assert PERIOD_START <= other[0].bucket_date < PERIOD_END

        await _close_september(db_session, test_settings)

        row = (await _rows(db_engine, BillingInvoice))[0]

        # Incurred cost is cdmsfirst's eight rows EXACTLY — not 13.884770 + 6.696305.
        assert row.llm_usage_usd == MEASURED_USAGE_USD
        assert row.llm_usage_usd != MEASURED_USAGE_USD + OTHER_TENANT_SPEND_USD
        assert row.internal_cost_usd == EXPECTED_INTERNAL_COST_USD
        assert row.total_usd == EXPECTED_TOTAL_USD
        # And not the marked-up leak either (6.696305 x 1.20 = 8.035566 -> 8.04).
        assert row.total_usd != EXPECTED_TOTAL_USD + Decimal("8.04")

        # ⛔ The other tenant is not even NAMED on the statement: it owns a
        # different licence, so a leak would have added a whole licence line.
        billed_licences = {entry["license_id"] for entry in row.lines}
        assert billed_licences == {str(CDMSFIRST_LICENSE_ID)}
        assert str(OTHER_TENANT_LICENSE_ID) not in billed_licences


# --------------------------------------------------------------------------- #
# 4. The refusals — each must consume nothing
# --------------------------------------------------------------------------- #


@pytest.mark.unit
class TestBucketGapRefusalBurnsNoInvoiceNumber:
    async def test_a_destroyed_fleet_wide_day_refuses_and_draws_no_number(
        self, db_session: AsyncSession, db_engine, test_settings, seats_say
    ) -> None:
        """⛔ A REFUSAL MUST COST NOTHING — NO ROW, AND NO INVOICE NUMBER.

        Production lost 2026-09-02, 09-03 and 09-04 entirely: no
        ``billing_cost_daily`` rows at all, fleet-wide. This fixture reproduces
        that by omitting those days' padding, so the ONLY difference from the
        passing test above is the gap. ``assert_no_bucket_gap`` runs FIRST among
        the computing steps precisely so a refusal happens while it is still free.

        ⭐ THE COUNTER IS PRE-SEEDED TO next_seq=1 RATHER THAN LEFT EMPTY, and that
        is what makes this control read a VALUE instead of an absence. The table is
        truncated before every test, so "0 rows after the refusal" would be
        absence-vs-absence and would also pass if the gate ran but ``issue`` were
        never reachable for an unrelated reason. Seeded, the assertion is
        1 -> still 1: had the refusal been bypassed, ``issue`` would have drawn the
        number and left 2. (``billing_invoice_counters`` carries the TRUNCATE guard
        only — no immutability trigger — so seeding it is exactly what ``issue``'s
        own ``INSERT ... ON CONFLICT DO NOTHING`` does.)

        ⚠️ The assertion compares the WHOLE counter map, not one key's value, so it
        stays correct even if the seeded scope spelling were wrong: a draw under any
        other scope would ADD a row and the comparison would still fail.
        """
        from services.byo_mode import clear_byo_cache
        from services.invoices import BucketGapError, close_period

        seats_say(seats=EXPECTED_SEAT_COUNT)
        _seed_cdmsfirst_spend(db_session)
        _seed_fleet_presence_for_september(db_session, omit=DESTROYED_BUCKET_DAYS)
        await _seed_cdmsfirst_terms(db_session)
        await _assert_seeded_usage_is_the_measured_sum(db_session)
        db_session.add(BillingInvoiceCounter(scope="INV:2026", period_year=2026, next_seq=1))
        await db_session.commit()

        assert await _counter_state(db_engine) == {"INV:2026": 1}

        clear_byo_cache()
        with pytest.raises(BucketGapError) as caught:
            await close_period(
                db_session,
                tenant_id=CDMSFIRST,
                period_start=PERIOD_START,
                period_end=PERIOD_END,
                settings=test_settings,
            )
        await db_session.rollback()

        # The error names the days production actually lost.
        assert caught.value.missing_dates == list(DESTROYED_BUCKET_DAYS)

        # Nothing was written, and nothing was consumed.
        assert await _rows(db_engine, BillingInvoice) == []
        assert await _counter_state(db_engine) == {"INV:2026": 1}, (
            "the refusal burned an invoice number: a gapless series cannot "
            "recover a number spent on a statement that was never written"
        )


@pytest.mark.unit
class TestAPartialSpendWindowCannotBeClosedAtAll:
    async def test_the_measured_spend_window_is_refused_as_not_a_calendar_month(
        self, db_session: AsyncSession, db_engine, test_settings, seats_say
    ) -> None:
        """⚠️ WHY THIS FILE CLOSES SEPTEMBER ENTIRE — asserted, not asserted in prose.

        ``[2026-09-05, 2026-09-12)`` is the window the eight measured rows occupy,
        and it is the natural thing to ask for. ``close_period`` cannot do it:
        ``_month_str`` is its FIRST statement and refuses anything that is not one
        whole calendar month, because ``generate_billing`` derives its own bounds
        from a ``YYYY-MM`` string. Pinned here so that if the service ever grows
        partial-period support, this test goes red and whoever sees it knows to
        re-derive the arithmetic above for a 7-day window (most importantly: whether
        the $5,000 ``monthly`` fee then prorates, which today it provably does not).

        The refusal is also checked to consume nothing — it is raised before any
        gate, any HTTP call and any counter draw.
        """
        from services.byo_mode import clear_byo_cache
        from services.invoices import InvoicePeriodError, close_period

        seats_say(seats=EXPECTED_SEAT_COUNT)
        await _seed_everything(db_session)
        await db_session.commit()

        clear_byo_cache()
        with pytest.raises(InvoicePeriodError, match="not the first of a month"):
            await close_period(
                db_session,
                tenant_id=CDMSFIRST,
                period_start=SPEND_WINDOW_START,
                period_end=SPEND_WINDOW_END,
                settings=test_settings,
            )
        await db_session.rollback()

        assert await _rows(db_engine, BillingInvoice) == []
        assert await _counter_state(db_engine) == {}
