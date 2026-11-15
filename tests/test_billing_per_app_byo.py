"""The PER-APP BYO posture on the invoice — the under-bill that had no symptom.

WHAT THIS FILE PROVES

``services.billing.generate_billing`` used to derive ONE ``billing_mode`` from the
TENANT-WIDE ``byo_mode`` and price every line with it. A tenant whose tenant-wide
posture is ``local_allowed``/``platform_only`` maps through
``effective_billing_mode`` to ``BILLING_MODE_INTERNAL`` and bills **$0 on every
line** — including lines for a product that is itself ``byo_required``.
CommonPractice is exactly that shape, and 49 of 59 unarchived tenants carry an
internal tenant-wide mode (measured 2026-09-11, see ``services.byo_mode``). The
resulting invoice line reads ``$0.00``, which is what a correctly-billed internal
line also reads, so nothing anywhere said a term had been missed.

⛔ THE THREE THINGS THAT MAKE THIS NON-TRIVIAL, ALL ASSERTED HERE RATHER THAN
ARGUED IN PROSE:

1. ``service_key`` IS NOT THE LICENCES APP KEY. The fixtures below deliberately
   give CommonPractice the ``service_key`` ``"commonpractice"`` and the licences
   app key ``"common-practice"`` — DIFFERENT STRINGS — so a resolver that passes
   the ``service_key`` to the per-product route asks about a product that does not
   exist, takes a 404 ("no override"), falls back to the tenant-wide posture and
   bills $0 again. That mutation is killed by
   ``test_the_per_app_question_is_asked_with_the_licences_app_key``.
2. A LICENCES OUTAGE MUST NOT SILENTLY CHANGE A BILL. ``license_map`` is the
   source of the app key AND is deliberately allowed to degrade to ``{}``, so the
   cheapest implementation turns an outage into "every line takes the tenant-wide
   posture" — the exact silent under-bill this change exists to close, smuggled
   back in by the change itself. Pinned by
   ``test_a_licences_outage_refuses_instead_of_silently_billing_tenant_wide``.
3. NON-COMMONPRACTICE LINES MUST BE BYTE-IDENTICAL. This is the whole fleet's
   invoice path. ``test_a_non_commonpractice_key_is_untouched_and_asks_nothing``
   asserts both halves: the money AND the absence of any per-product request.
"""

from __future__ import annotations

import uuid
from datetime import date
from decimal import Decimal

import httpx
import pytest

from models.db import ATTRIBUTION_ATTRIBUTED, CostDaily
from services import byo_mode as byo
from services.billing import generate_billing
from services.byo_mode import ByoModeUnavailableError
from services.markup import BILLING_MODE_INTERNAL
from tests.conftest import T

pytestmark = pytest.mark.asyncio


TENANT = T("cp-perapp-byo")
MONTH = "2026-09"

#: ⚠️ THE TWO STRINGS ARE DIFFERENT ON PURPOSE — see the module docstring, point 1.
#: ``service_key`` is what the EMITTER stamps on a cost row (measured literal, from
#: ``settings.cost_attribution_app_by_service_key``); the app key is what LICENCES
#: calls the product and is the only value the per-product route accepts.
CP_SERVICE_KEY = "commonpractice"  # pragma: allowlist secret
CP_LICENCES_APP_KEY = "common-practice"

#: A second, ordinary product on the same invoice. Its money must not move.
OTHER_SERVICE_KEY = "inference-router"  # pragma: allowlist secret
OTHER_LICENCES_APP_KEY = "inference"

CP_LICENCE_A = uuid.UUID("01a06ff2-0000-7000-8000-0000000000c1")
CP_LICENCE_B = uuid.UUID("01a06ff2-0000-7000-8000-0000000000c2")
OTHER_LICENCE = uuid.UUID("01a06ff2-0000-7000-8000-0000000000f1")

_SUBJECT = uuid.UUID("0195f48b-d000-7000-8000-0000000000a1")

#: No ``markup_rules`` row and no ``tenant_config`` row are seeded, so every line
#: falls back to ``settings.default_markup_pct`` = 20% with
#: ``markup_source=settings_default``, and ``get_billing_mode`` returns the default
#: ``standard_markup``. Both are the measured production state (``markup_rules``
#: holds no open row; ``tenant_config`` holds none at all), so the arithmetic below
#: is the arithmetic production would do.
CP_INCURRED = Decimal("10.000000")
OTHER_INCURRED = Decimal("20.000000")
CP_BILLED_COMMERCIALLY = Decimal("12.00")  # 10.00 x 1.20


@pytest.fixture(autouse=True)
def _byo_cache_isolated():
    """5-minute process-global module state — one test's answer must not decide
    another's, and the per-app entries share the cache with the tenant-wide ones."""
    byo.clear_byo_cache()
    yield
    byo.clear_byo_cache()


class _Licences:
    """A URL-aware licences stub that RECORDS what was asked.

    ⚠️ URL-AWARE, NEVER BLANKET — the same reason ``tests/test_commercial_terms.py``
    says so: ``httpx.AsyncClient.get`` is how ``fetch_licenses`` retrieves the
    licence LIST *and* how ``services.byo_mode`` retrieves a posture, so one generic
    answer feeds a caller the other's shape and surfaces as an ``AttributeError``
    deep inside ``generate_billing`` that reads like a billing bug.

    ⭐ IT CHAINS TO THE PREVIOUS ``get`` rather than replacing it. ``tests/conftest``
    installs an autouse stub for ``/internal/tenants``; swallowing every URL here
    would silently satisfy an expectation another fixture wrote.
    """

    def __init__(
        self,
        *,
        tenant_wide: str = "local_allowed",
        app_postures: dict[str, str] | None = None,
        app_status: int = 200,
        licences_status: int = 200,
        licences: list[dict] | None = None,
    ) -> None:
        self.tenant_wide = tenant_wide
        self.app_postures = app_postures or {}
        self.app_status = app_status
        self.licences_status = licences_status
        self.licences = licences if licences is not None else []
        #: Every per-product URL asked, in order. A LIST, not a set: the constraint
        #: under test is "one call per DISTINCT app", and only the ORDERED RECORD can
        #: tell one-per-app from one-per-line when a tenant holds one app.
        self.app_requests: list[str] = []
        self.tenant_wide_requests: list[str] = []

    def install(self, monkeypatch) -> _Licences:
        previous = httpx.AsyncClient.get
        stub = self

        async def _get(self, url, *a, **kw):  # noqa: ANN001, ANN202
            text = str(url)
            # ⚠️ THE PER-PRODUCT ARM MUST BE TESTED FIRST. Both routes end in
            # `byo-mode`; an `endswith` check alone would answer the per-product
            # question with the tenant-wide posture and make every assertion here
            # pass for the wrong reason.
            if "/apps/" in text and text.endswith("/byo-mode"):
                stub.app_requests.append(text)
                if stub.app_status != 200:
                    return httpx.Response(stub.app_status, json={})
                app_key = text.split("/apps/")[1].removesuffix("/byo-mode")
                posture = stub.app_postures.get(app_key)
                if posture is None:
                    # 404 = "no override for this product", the NORMAL answer.
                    return httpx.Response(404, json={})
                return httpx.Response(200, json={"byo_mode": posture})
            if text.endswith("/byo-mode"):
                stub.tenant_wide_requests.append(text)
                return httpx.Response(200, json={"byo_mode": stub.tenant_wide})
            if text.endswith("/internal/licenses"):
                if stub.licences_status != 200:
                    return httpx.Response(stub.licences_status, json={})
                return httpx.Response(200, json=stub.licences)
            return await previous(self, url, *a, **kw)

        monkeypatch.setattr(httpx.AsyncClient, "get", _get)
        return self


def _licence(licence_id: uuid.UUID, app_key: str) -> dict:
    """One row in the shape ``fetch_licenses`` returns — ``app.key`` is the point."""
    return {
        "id": str(licence_id),
        "app": {"key": app_key, "name": app_key},
        "plan": {"code": "standard", "quota_monthly": None},
    }


def _cost_row(
    *,
    licence_id: uuid.UUID,
    service_key: str,
    cost: Decimal,
    bucket_date: date,
    byo_line: bool = True,
) -> CostDaily:
    """One ``billing_cost_daily`` row. ``byo=True`` by default: the reported defect
    is about BYO usage billing $0, and under ``standard_markup`` a BYO line is
    marked up at the fallback percentage exactly like any other (the forced zero
    belongs to ``byo_platform_fee``, which this tenant is not on)."""
    return CostDaily(
        tenant_id=TENANT,
        tenant_name="cp-perapp-byo",
        license_id=licence_id,
        subject_id=_SUBJECT,
        service_key=service_key,
        provider="anthropic",
        served_model="claude",
        byo=byo_line,
        owner_provided=False,
        cost_source="unknown",
        attribution_status=ATTRIBUTION_ATTRIBUTED,
        bucket_date=bucket_date,
        total_input_tokens=1000,
        total_output_tokens=100,
        total_cost_usd=cost,
        request_count=10,
    )


async def _seed(session, rows: list[CostDaily]) -> None:
    for row in rows:
        session.add(row)
    await session.commit()


def _billed_for(report: dict, licence_id: uuid.UUID) -> Decimal:
    """The billed LLM figure on one licence entry of the statement."""
    for entry in report["licenses"]:
        if entry["license_id"] == str(licence_id):
            return Decimal(entry["costs"]["llm_billed_usd"])
    raise AssertionError(f"no licence entry for {licence_id} in {report['licenses']}")


# --------------------------------------------------------------------------- #
# The defect itself
# --------------------------------------------------------------------------- #


async def test_a_per_app_byo_required_posture_bills_markup_on_commonpractice_lines(
    db_session, test_settings, monkeypatch
):
    """MUTATION TARGET: the ``_line_billing_mode(crow.license_id, crow.service_key)``
    argument to ``compute_billed_line_detailed`` in the LLM-cost loop of
    ``services.billing.generate_billing``. Restore it to the bare tenant-wide
    ``billing_mode`` and the CommonPractice line bills $0.00 again — silently,
    which is the whole reason this test exists."""
    await _seed(
        db_session,
        [
            _cost_row(
                licence_id=CP_LICENCE_A,
                service_key=CP_SERVICE_KEY,
                cost=CP_INCURRED,
                bucket_date=date(2026, 9, 5),
            )
        ],
    )
    _Licences(
        # The tenant as a whole is NOT commercial...
        tenant_wide="local_allowed",
        # ...but CommonPractice inside it is.
        app_postures={CP_LICENCES_APP_KEY: "byo_required"},
        licences=[_licence(CP_LICENCE_A, CP_LICENCES_APP_KEY)],
    ).install(monkeypatch)

    report = await generate_billing(TENANT, MONTH, db_session, test_settings)

    assert _billed_for(report, CP_LICENCE_A) == CP_BILLED_COMMERCIALLY
    assert Decimal(report["totals"]["customer_cost_usd"]) == CP_BILLED_COMMERCIALLY
    # ⭐ THE CONTROL THAT KEEPS THE ASSERTION HONEST: the tenant-wide field is still
    # the tenant-wide answer. If this had flipped to a commercial mode the test would
    # be proving that the TENANT was reclassified, which is a different (and wrong)
    # change that would also take the line from $0 to $12.00.
    assert report["billing_mode"] == BILLING_MODE_INTERNAL


async def test_the_per_app_question_is_asked_with_the_licences_app_key(
    db_session, test_settings, monkeypatch
):
    """MUTATION TARGET: ``_licences_app_key`` — specifically, replacing its
    ``license_map[...]["app"]["key"]`` derivation with the row's ``service_key``.
    The fixtures give the two DIFFERENT strings, so the substitution asks licences
    about ``commonpractice`` (a product it does not know), takes the 404 that means
    "no override", falls back to the tenant-wide ``local_allowed`` and bills $0."""
    await _seed(
        db_session,
        [
            _cost_row(
                licence_id=CP_LICENCE_A,
                service_key=CP_SERVICE_KEY,
                cost=CP_INCURRED,
                bucket_date=date(2026, 9, 5),
            )
        ],
    )
    licences = _Licences(
        tenant_wide="local_allowed",
        app_postures={CP_LICENCES_APP_KEY: "byo_required"},
        licences=[_licence(CP_LICENCE_A, CP_LICENCES_APP_KEY)],
    ).install(monkeypatch)

    report = await generate_billing(TENANT, MONTH, db_session, test_settings)

    assert licences.app_requests, "the per-product route was never asked at all"
    asked = licences.app_requests[0]
    assert f"/apps/{CP_LICENCES_APP_KEY}/byo-mode" in asked
    assert f"/apps/{CP_SERVICE_KEY}/" not in asked
    assert _billed_for(report, CP_LICENCE_A) == CP_BILLED_COMMERCIALLY


# --------------------------------------------------------------------------- #
# The blast-radius promise: everything that is not CommonPractice
# --------------------------------------------------------------------------- #


async def test_a_non_commonpractice_key_is_untouched_and_asks_nothing(
    db_session, test_settings, monkeypatch
):
    """MUTATION TARGET: ``services.billing.PER_APP_BYO_SERVICE_KEYS`` — widen it to
    every service_key (or delete the membership test in ``_line_billing_mode``) and
    this test fails on BOTH halves.

    Both halves matter and neither implies the other. The MONEY half is the fleet's
    byte-identity promise. The REQUEST half is why the allowlist is not merely
    belt-and-braces: a general mechanism would make every other product's bill
    depend on whether licences happens to hold an override for it — a fact this repo
    cannot see, and therefore cannot promise is absent."""
    await _seed(
        db_session,
        [
            _cost_row(
                licence_id=OTHER_LICENCE,
                service_key=OTHER_SERVICE_KEY,
                cost=OTHER_INCURRED,
                bucket_date=date(2026, 9, 5),
            )
        ],
    )
    licences = _Licences(
        tenant_wide="local_allowed",
        # licences DOES hold a commercial override for this product. It must not be
        # consulted, so it must not move the money.
        app_postures={OTHER_LICENCES_APP_KEY: "byo_required"},
        licences=[_licence(OTHER_LICENCE, OTHER_LICENCES_APP_KEY)],
    ).install(monkeypatch)

    report = await generate_billing(TENANT, MONTH, db_session, test_settings)

    assert licences.app_requests == [], (
        "a non-allowlisted service_key asked licences the per-product question"
    )
    assert _billed_for(report, OTHER_LICENCE) == Decimal("0.00")
    assert Decimal(report["totals"]["customer_cost_usd"]) == Decimal("0.00")


async def test_both_products_on_one_invoice_are_priced_independently(
    db_session, test_settings, monkeypatch
):
    """MUTATION TARGET: the ``line_billing_mode`` local in the LLM-cost loop. The
    two products share one tenant, one statement and one tenant-wide posture, so a
    per-INVOICE (rather than per-LINE) resolution passes every single-product test
    above and fails only here."""
    await _seed(
        db_session,
        [
            _cost_row(
                licence_id=CP_LICENCE_A,
                service_key=CP_SERVICE_KEY,
                cost=CP_INCURRED,
                bucket_date=date(2026, 9, 5),
            ),
            _cost_row(
                licence_id=OTHER_LICENCE,
                service_key=OTHER_SERVICE_KEY,
                cost=OTHER_INCURRED,
                bucket_date=date(2026, 9, 5),
            ),
        ],
    )
    _Licences(
        tenant_wide="local_allowed",
        app_postures={CP_LICENCES_APP_KEY: "byo_required"},
        licences=[
            _licence(CP_LICENCE_A, CP_LICENCES_APP_KEY),
            _licence(OTHER_LICENCE, OTHER_LICENCES_APP_KEY),
        ],
    ).install(monkeypatch)

    report = await generate_billing(TENANT, MONTH, db_session, test_settings)

    assert _billed_for(report, CP_LICENCE_A) == CP_BILLED_COMMERCIALLY
    assert _billed_for(report, OTHER_LICENCE) == Decimal("0.00")
    # internal_cost is the RAW incurred of both lines — unaffected by either posture.
    assert Decimal(report["totals"]["internal_cost_usd"]) == Decimal("30.00")
    assert Decimal(report["totals"]["customer_cost_usd"]) == CP_BILLED_COMMERCIALLY


# --------------------------------------------------------------------------- #
# The fan-out bound
# --------------------------------------------------------------------------- #


async def test_one_licences_call_per_distinct_app_not_per_line(
    db_session, test_settings, monkeypatch
):
    """MUTATION TARGET: hoisting ``_resolve_per_app_billing_modes`` out of
    ``generate_billing``'s prologue and into either billing loop. Four billable
    CommonPractice rows across two licences of ONE app must produce exactly ONE
    request; a per-line resolver produces four, and a per-licence one produces two.

    ⚠️ The count is asserted on an ordered LIST, not a set — a set cannot tell one
    call from four."""
    await _seed(
        db_session,
        [
            _cost_row(
                licence_id=CP_LICENCE_A,
                service_key=CP_SERVICE_KEY,
                cost=Decimal("2.500000"),
                bucket_date=date(2026, 9, 5),
            ),
            _cost_row(
                licence_id=CP_LICENCE_A,
                service_key=CP_SERVICE_KEY,
                cost=Decimal("2.500000"),
                bucket_date=date(2026, 9, 6),
            ),
            _cost_row(
                licence_id=CP_LICENCE_B,
                service_key=CP_SERVICE_KEY,
                cost=Decimal("2.500000"),
                bucket_date=date(2026, 9, 5),
            ),
            _cost_row(
                licence_id=CP_LICENCE_B,
                service_key=CP_SERVICE_KEY,
                cost=Decimal("2.500000"),
                bucket_date=date(2026, 9, 6),
            ),
        ],
    )
    licences = _Licences(
        tenant_wide="local_allowed",
        app_postures={CP_LICENCES_APP_KEY: "byo_required"},
        licences=[
            _licence(CP_LICENCE_A, CP_LICENCES_APP_KEY),
            _licence(CP_LICENCE_B, CP_LICENCES_APP_KEY),
        ],
    ).install(monkeypatch)

    report = await generate_billing(TENANT, MONTH, db_session, test_settings)

    assert len(licences.app_requests) == 1, licences.app_requests
    # $10.00 of incurred spend, however it is split into rows, bills $12.00.
    assert Decimal(report["totals"]["customer_cost_usd"]) == CP_BILLED_COMMERCIALLY


# --------------------------------------------------------------------------- #
# The two failure arms — and they are deliberately OPPOSITE
# --------------------------------------------------------------------------- #


async def test_a_per_app_5xx_raises_rather_than_falling_back(
    db_session, test_settings, monkeypatch
):
    """MUTATION TARGET: ``get_byo_mode_strict`` -> ``get_byo_mode`` in
    ``_resolve_per_app_billing_modes``.

    ``get_byo_mode`` swallows every non-answer into ``"byo_required"``, which on
    THIS route is both wrong and invisible: a licences 5xx would resolve
    CommonPractice to the tenant-wide ``local_allowed`` and bill $0 — an under-bill
    manufactured by an infra blip and laundered through an ordinary-looking answer.
    The mutation makes this test see a returned report instead of a raise."""
    await _seed(
        db_session,
        [
            _cost_row(
                licence_id=CP_LICENCE_A,
                service_key=CP_SERVICE_KEY,
                cost=CP_INCURRED,
                bucket_date=date(2026, 9, 5),
            )
        ],
    )
    _Licences(
        tenant_wide="local_allowed",
        app_postures={CP_LICENCES_APP_KEY: "byo_required"},
        app_status=503,
        licences=[_licence(CP_LICENCE_A, CP_LICENCES_APP_KEY)],
    ).install(monkeypatch)

    with pytest.raises(ByoModeUnavailableError) as excinfo:
        await generate_billing(TENANT, MONTH, db_session, test_settings)
    assert excinfo.value.reason == "http_error"


async def test_a_404_on_the_per_app_route_falls_back_to_the_tenant_wide_posture(
    db_session, test_settings, monkeypatch
):
    """MUTATION TARGET: the 404 arm of ``services.byo_mode._resolve_app_override``,
    and any attempt to "simplify" the 5xx rule above into "any non-200 raises".

    ⛔ THE CONTROL FOR THE TEST DIRECTLY ABOVE, and it is the assertion that stops
    this change being worse than the bug it fixes. A 404 is the NORMAL answer for
    every product a tenant has not overridden. Treating it like the 5xx would bill
    every internal tenant commercially for every product — the over-bill the
    tenant-wide default is carefully scoped to avoid. So: no override, no raise, and
    the money is byte-identically what it was before this change existed."""
    await _seed(
        db_session,
        [
            _cost_row(
                licence_id=CP_LICENCE_A,
                service_key=CP_SERVICE_KEY,
                cost=CP_INCURRED,
                bucket_date=date(2026, 9, 5),
            )
        ],
    )
    licences = _Licences(
        tenant_wide="local_allowed",
        app_postures={},  # licences knows the tenant; it holds no override for CP
        licences=[_licence(CP_LICENCE_A, CP_LICENCES_APP_KEY)],
    ).install(monkeypatch)

    report = await generate_billing(TENANT, MONTH, db_session, test_settings)

    assert len(licences.app_requests) == 1
    assert _billed_for(report, CP_LICENCE_A) == Decimal("0.00")
    assert Decimal(report["totals"]["customer_cost_usd"]) == Decimal("0.00")


async def test_a_licences_outage_refuses_instead_of_silently_billing_tenant_wide(
    db_session, test_settings, monkeypatch
):
    """MUTATION TARGET: the ``if underivable and licenses_degraded: raise`` branch in
    ``_resolve_per_app_billing_modes`` (equivalently, pinning ``licenses_degraded``
    to ``False``).

    ⛔ THIS IS THE TEST THAT FAILS IF SOMEONE LATER MAKES THE OUTAGE SILENT, and the
    failure mode it guards is not hypothetical — it is the CHEAPEST implementation
    of this whole change. ``generate_billing`` swallows ``LicensesUnavailableError``
    and continues with ``license_map = {}``; that tolerance was free while the map
    only labelled things, and stopped being free the moment the map became the
    source of the app key a line is PRICED against. Delete the branch and this test
    does not error — it gets a perfectly ordinary-looking report in which the
    CommonPractice line bills $0.00 because licences was down. That is the original
    defect, restored by its own fix, with no log line saying a guess was made.

    ⚠️ The assertion is deliberately on the RAISE, not merely on "not $0.00": a
    future author who decides to fail CLOSED to bill instead must come here, read
    this, and change the decision on purpose rather than by omission."""
    await _seed(
        db_session,
        [
            _cost_row(
                licence_id=CP_LICENCE_A,
                service_key=CP_SERVICE_KEY,
                cost=CP_INCURRED,
                bucket_date=date(2026, 9, 5),
            )
        ],
    )
    _Licences(
        tenant_wide="local_allowed",
        app_postures={CP_LICENCES_APP_KEY: "byo_required"},
        # The licence LIST is what fails here — not the posture route. That is the
        # door the cheap implementation leaves open.
        licences_status=503,
    ).install(monkeypatch)

    with pytest.raises(ByoModeUnavailableError) as excinfo:
        await generate_billing(TENANT, MONTH, db_session, test_settings)
    assert excinfo.value.reason == "unavailable"
    assert CP_SERVICE_KEY in str(excinfo.value)


async def test_an_outage_with_no_commonpractice_lines_still_degrades_quietly(
    db_session, test_settings, monkeypatch
):
    """MUTATION TARGET: the ``if not scoped: return {}`` early exit, and the scoping
    of the raise above to allowlisted rows only.

    ⛔ THE COUNTERWEIGHT TO THE PREVIOUS TEST. Turning a licences blip into a hard
    failure for the WHOLE fleet would be a self-inflicted outage on a path that is
    read by a dashboard and by an operator mid-incident, and ``generate_billing``'s
    own comments promise it stays available. Broadening the refusal from "lines I
    cannot price" to "any degraded map" passes the test above and fails this one."""
    await _seed(
        db_session,
        [
            _cost_row(
                licence_id=OTHER_LICENCE,
                service_key=OTHER_SERVICE_KEY,
                cost=OTHER_INCURRED,
                bucket_date=date(2026, 9, 5),
            )
        ],
    )
    _Licences(tenant_wide="byo_required", licences_status=503).install(monkeypatch)

    report = await generate_billing(TENANT, MONTH, db_session, test_settings)

    # Labelling degraded (no licence metadata), figures intact: 20.00 x 1.20.
    assert _billed_for(report, OTHER_LICENCE) == Decimal("24.00")
