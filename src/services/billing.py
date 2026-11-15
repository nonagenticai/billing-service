"""Billing calculation service — joins metering data with licenses API."""

from __future__ import annotations

from datetime import UTC, date, datetime, timedelta
from decimal import Decimal
from typing import TYPE_CHECKING

import httpx
import structlog
from sqlalchemy import and_, case, func, select
from sqlalchemy.ext.asyncio import AsyncSession

import observability.metrics as observability_metrics
from models.db import (
    NULL_LICENSE_ID,
    NULL_PROJECT_ID,
    AppCostRate,
    CostDaily,
    MeteringDaily,
    MeteringMonthlyUsage,
    ProviderCost,
    TenantConfig,
    TokenBudget,
    TokenUsage,
    is_system_license,
)
from services import prepaid_credit as prepaid_credit_svc
from services.access_charge import (
    active_access_charges_for_month,
    allocate_allowance,
    one_time_remaining_before,
)
from services.byo_mode import (
    ByoModeUnavailableError,
    effective_billing_mode,
    get_byo_mode,
    get_byo_mode_strict,
)
from services.licence_limits import effective_limit
from services.markup import (
    BILLING_MODE_BYO_MARKUP_ONLY,
    BILLING_MODE_INTERNAL,
    MARKUP_SOURCE_SETTINGS,
    MARKUP_SOURCE_TENANT_CONFIG,
    MARKUP_SOURCE_TENANT_CONFIG_DEFAULT,
    build_markup_resolver,
    compute_billed_line_detailed,
    quantize_money,
)
from services.seats import (
    SEAT_BASIS_ACTIVE_DURING_PERIOD,
    SEAT_BASIS_ACTIVE_LICENCES,
    SEAT_BASIS_UNAVAILABLE,
    SeatCountUnavailableError,
    get_seat_count,
    seat_scope_for_charge,
)
from settings import Settings
from tenancy import DEFAULT_CONFIG_TENANT_ID

if TYPE_CHECKING:
    from collections.abc import Sequence

    from service_auth import ServiceTokenProvider

logger = structlog.stdlib.get_logger(__name__)


# --------------------------------------------------------------------------- #
# Access / platform-fee line BASIS
# --------------------------------------------------------------------------- #
# ⭐ AN ABSENT LINE IS NOT A ZERO LINE. Every access/platform-fee line carries a
# basis so a reader can tell WHY it says what it says. The three are genuinely
# different facts and collapsing any two of them loses audit information.
#: A real `tenant_access_charge` row matched and was charged.
ACCESS_BASIS_CHARGE = "tenant_access_charge"
#: The axis WAS evaluated against `tenant_access_charge` and nothing matched.
ACCESS_BASIS_NO_MATCH = "no_matching_access_charge"
#: The axis does not apply — internal/demo tenants are not billed at all, so no
#: lookup was performed. NOT the same as "looked, found nothing".
ACCESS_BASIS_NOT_APPLICABLE = "not_applicable_internal_tenant"


# --------------------------------------------------------------------------- #
# PER-APP BYO POSTURE — the under-bill this file used to produce silently
# --------------------------------------------------------------------------- #
#
# ⛔ THE DEFECT: `billing_mode` below is derived from the TENANT-WIDE `byo_mode`
# alone. A tenant whose tenant-wide posture is `local_allowed`/`platform_only`
# maps through `effective_billing_mode` to `BILLING_MODE_INTERNAL` and bills
# **$0 on every line** — including lines for a product that is itself
# `byo_required`. CommonPractice is exactly that shape: a commercial, billed
# product living inside tenants whose tenant-wide posture is not commercial.
# Measured 2026-09-11 (see `services.byo_mode`): 49 of 59 unarchived tenants
# carry an internal tenant-wide mode, so the population this silently zeroes is
# most of the estate, and nothing on an invoice reading $0.00 says a term was
# missed. licences has carried a per-product override the whole time
# (`GET /tenants/{id}/apps/{app}/byo-mode`); this file simply never asked.

#: The ``service_key`` values whose lines ask licences the PER-PRODUCT question
#: instead of settling for the tenant-wide posture.
#:
#: ⛔ AN ALLOWLIST, AND THE NARROWNESS *IS* THE SAFETY ARGUMENT — do not "generalise"
#: it to every service_key. This is the whole fleet's invoice path. Asking the
#: per-product question for every app would make every other product's bill depend
#: on whether licences happens to hold an override for it, which is a fact this repo
#: cannot see and therefore cannot promise is absent. With the allowlist, every
#: NON-CommonPractice ``service_key`` takes byte-identically the code path it took
#: before this change existed — by CONSTRUCTION, not by hoping. Widen it one app at
#: a time, having first measured that app's override population in licences.
#:
#: ⚠️ IT IS KEYED ON ``service_key``, WHICH IS *NOT* THE LICENCES APP KEY — the two
#: are different namespaces and conflating them is the first way this change goes
#: wrong. ``service_key`` is what the EMITTER stamps on a cost/metering row, so it is
#: the right thing to SCOPE on, and ``"commonpractice"`` is measured ground truth:
#: it is the literal in ``settings.cost_attribution_app_by_service_key``
#: (``"commonpractice=Common Practice"``), itself taken from CommonPractice's own
#: ``backend/src/host/metering-emit.ts``. The key SENT TO LICENCES is a different
#: value and is derived separately — see :func:`_licences_app_key`.
PER_APP_BYO_SERVICE_KEYS: frozenset[str] = frozenset({"commonpractice"})


def _licences_app_key(license_id, license_map: dict[str, dict]) -> str | None:
    """The LICENCES app key for a billed line, or ``None`` if it cannot be derived.

    ⛔ ``service_key`` IS NOT A SUBSTITUTE AND MUST NEVER BE PASSED AS ONE. The
    per-product route is ``/tenants/{id}/apps/{app_key}/byo-mode`` and ``app_key``
    there is licences' OWN app key — the same value this module already reads out of
    the licences response at ``license_map[lic_id]["app"]["key"]`` to label a licence
    entry. A ``service_key`` that is not an app key does not error: it 404s, a 404 on
    that route means "no override for this product", and the caller falls back to the
    tenant-wide posture. So substituting it would reproduce the exact silent
    under-bill this change exists to close, while LOOKING like the fix and costing an
    HTTP round trip per invoice to do it. (The ``service_key`` fallback further down
    this file, on ``licenses[].app_key``, is a DISPLAY label — a different job.)

    ⚠️ AND IN PRODUCTION THE TWO VALUES ARE CURRENTLY EQUAL, WHICH IS WHY THIS
    DOCSTRING IS LONG. Measured 2026-09-17 against the licences registry on worker4:
    ``select key, name from app where key ilike '%common%'`` returns exactly
    ``commonpractice|Common Practice``. So a reader who "simplifies" this to pass
    ``service_key`` straight through gets a GREEN fleet and a correct invoice today,
    and the defect only appears the day CommonPractice's app key diverges from its
    service key — or the day this scope is widened to a product where they already
    differ. The tests fix the app key as ``"common-practice"`` (deliberately NOT the
    service key) so the mechanism is what is pinned, not the coincidence.

    Returns ``None`` rather than guessing when the licence is absent from the map.
    The caller decides what an absence means, because the two causes are different
    facts and only one of them is an outage — see
    :func:`_resolve_per_app_billing_modes`.
    """
    lic = license_map.get(str(license_id)) or {}
    key = (lic.get("app") or {}).get("key")
    return str(key) if key else None


async def _resolve_per_app_billing_modes(
    tenant_id: str,
    settings: Settings,
    *,
    configured_billing_mode: str,
    license_map: dict[str, dict],
    licenses_degraded: bool,
    rows: Sequence,
    token_provider: ServiceTokenProvider | None = None,
) -> dict[str, str]:
    """``{licences app_key: billing_mode}`` for this invoice's allowlisted lines.

    ⭐ ONE LICENCES CALL PER **DISTINCT APP**, NEVER PER LINE. The app keys are
    collapsed to a set before anything is asked, so a tenant with 300 CommonPractice
    cost rows across 12 licences still issues exactly one request. That matters for
    the same reason ``services.byo_mode._MAP_CONCURRENCY`` exists: the failure mode
    of a naive per-line lookup is not latency, it is a self-inflicted thundering herd
    against the service whose 503s this path has to survive. A plain sequential loop
    is enough precisely BECAUSE the allowlist bounds the set at one app today; if it
    is ever widened past a handful, bound the fan-out here before widening it.

    ⛔ ``get_byo_mode_strict``, NOT ``get_byo_mode``, AND THE DIFFERENCE IS THE 5xx.
    ``get_byo_mode`` swallows every non-answer into the revenue-safe
    ``"byo_required"`` default. On the TENANT-WIDE route that default is correct and
    deliberately argued for. Here it would be the wrong direction *and* invisible: a
    licences 5xx on the per-product route would silently resolve CommonPractice to
    the tenant-wide posture and bill $0 — an under-bill manufactured by an infra blip
    and laundered through a perfectly ordinary-looking answer. A 5xx therefore
    RAISES. A 404 does not: it is the normal answer for a product with no override,
    ``_resolve_app_override`` returns ``None``, and the resolution falls through to
    the tenant-wide posture, which is byte-identically today's behaviour.

    ⛔ THE OUTAGE CASE IS DECIDED EXPLICITLY, BECAUSE THE DEFAULT WOULD HAVE BEEN
    SILENT. ``license_map`` is built from ``fetch_licenses``, and ``generate_billing``
    deliberately SWALLOWS ``LicensesUnavailableError`` and continues with ``[]`` — a
    tolerance that is correct while the map only ENRICHES LABELS (its docstring says
    so, and no figure came from it). The moment the per-line app key is derived from
    that map, the same tolerance stops being free: an empty map makes every app key
    underivable, every CommonPractice line collapses to the tenant-wide posture, and
    a licences outage SILENTLY CHANGES A BILL in the under-bill direction. That is
    the precise shape this change exists to remove, so it must not be re-introduced
    by the change itself.

    So the two causes of an absent licence are separated, because they are not the
    same fact:

    * ``licenses_degraded`` — ``fetch_licenses`` RAISED and we swallowed it. This is
      an outage, not an answer, and it RAISES :class:`ByoModeUnavailableError` with
      the same ``reason="unavailable"`` classification every other non-answer in this
      path carries. ⚠️ It adds no new failure surface: in that same outage the
      per-app lookup a few lines below would raise anyway, so the only thing this
      branch changes is that the refusal is honest about WHICH call could not be
      made. It is scoped to tenants with billable allowlisted lines, so the
      report/dashboard availability this module promises elsewhere is untouched for
      everyone else.
    * licences ANSWERED and this licence is simply not in the active set (revoked,
      expired, archived — ``fetch_licenses`` filters ``status=active``). That is a
      real, durable answer about a healthy service, it is NOT an outage, and it falls
      back to the tenant-wide posture exactly as today. Logged, not raised: refusing
      an invoice because a licence lapsed would be a self-inflicted outage.
    """
    scoped = [
        r
        for r in rows
        if r.service_key in PER_APP_BYO_SERVICE_KEYS and not is_system_license(r.license_id)
    ]
    if not scoped:
        # The overwhelmingly common case fleet-wide. Nothing is asked, nothing can
        # fail, and the invoice is byte-identical to the pre-change one.
        return {}

    app_keys: set[str] = set()
    underivable: set[str] = set()
    for row in scoped:
        app_key = _licences_app_key(row.license_id, license_map)
        if app_key is None:
            underivable.add(str(row.service_key))
        else:
            app_keys.add(app_key)

    if underivable and licenses_degraded:
        logger.error(
            "billing_per_app_byo_undeterminable_during_licences_outage",
            tenant_id=tenant_id,
            service_keys=sorted(underivable),
            detail=(
                "licence enrichment degraded, so the per-app BYO key cannot be "
                "derived; refusing rather than silently billing these lines at the "
                "tenant-wide posture"
            ),
        )
        raise ByoModeUnavailableError(
            "licence enrichment degraded; cannot derive the per-app BYO app key for "
            f"service_keys={sorted(underivable)}",
            reason="unavailable",
        )
    if underivable:
        # licences ANSWERED; these lines carry no ACTIVE licence. Today's behaviour,
        # said out loud so a reader of a $0 CommonPractice line has somewhere to look.
        logger.warning(
            "billing_per_app_byo_no_active_licence",
            tenant_id=tenant_id,
            service_keys=sorted(underivable),
            detail="falling back to the tenant-wide byo posture for these lines",
        )

    modes: dict[str, str] = {}
    for app_key in sorted(app_keys):
        app_byo_mode = await get_byo_mode_strict(
            tenant_id, settings, app_key=app_key, token_provider=token_provider
        )
        modes[app_key] = effective_billing_mode(configured_billing_mode, app_byo_mode)
    return modes


async def get_markup_pct_with_source(
    tenant_id: str, session: AsyncSession, settings: Settings
) -> tuple[float, str]:
    """``(markup_pct, source)`` — the effective markup AND where it came from.

    Precedence is unchanged: tenant-specific > DB global default > env default.
    What is new is the second element, which names WHICH of those three answered.

    ⭐ Why the source is worth a return value: the three branches are
    indistinguishable in the number alone, and the last one is a CODE CONSTANT
    rather than anything an operator decided. An immutable statement that
    records "30%" without recording that nobody chose it cannot be reconciled
    against a rate card. Measured 2026-09-08: `tenant_config` has 0 rows, so
    every tenant currently takes the third branch.
    """
    stmt = select(TenantConfig.markup_pct).where(TenantConfig.tenant_id == tenant_id)
    result = await session.execute(stmt)
    custom = result.scalar_one_or_none()
    # ⚠️ `is not None`, not row existence: a tenant_config row whose markup_pct
    # is NULL (created by PUT /api/budget/money only to flip budget_unit,
    # alembic 0044) has NOT configured a markup, and must keep the fallback.
    if custom is not None:
        return float(custom), MARKUP_SOURCE_TENANT_CONFIG

    # Check DB-persisted global default
    default_stmt = select(TenantConfig.markup_pct).where(
        TenantConfig.tenant_id == DEFAULT_CONFIG_TENANT_ID
    )
    default_result = await session.execute(default_stmt)
    db_default = default_result.scalar_one_or_none()
    if db_default is not None:
        return float(db_default), MARKUP_SOURCE_TENANT_CONFIG_DEFAULT

    return settings.default_markup_pct, MARKUP_SOURCE_SETTINGS


async def get_markup_pct(tenant_id: str, session: AsyncSession, settings: Settings) -> float:
    """Get effective markup for a tenant: tenant-specific > DB default > env default."""
    pct, _source = await get_markup_pct_with_source(tenant_id, session, settings)
    return pct


async def get_billing_mode(tenant_id: str, session: AsyncSession) -> str:
    """The tenant's billing mode (standard_markup | byo_platform_fee | pass_through).

    Defaults to ``standard_markup`` when the tenant has no config row.
    """
    stmt = select(TenantConfig.billing_mode).where(TenantConfig.tenant_id == tenant_id)
    mode = (await session.execute(stmt)).scalar_one_or_none()
    return mode or "standard_markup"


def apply_markup(internal_usd: Decimal, markup_pct: float) -> Decimal:
    """Convert internal (provider/incurred) cost to billed (charged) cost.

    Markup is always applied per-tenant: a single global multiplier is wrong
    when tenants are on different rate cards.

    ⛔ DEAD IN ``src/`` AND DELIBERATELY LEFT ALONE — DO NOT "UNIFY" IT.
    Measured: the only references to this function anywhere are in
    ``tests/test_admin_usage_system.py``; nothing in ``src/`` calls it, so it
    prices no invoice and no response body. It is also the repo's one place that
    takes the context-default ROUND_HALF_EVEN *on purpose*:
    ``test_admin_usage_system.py:129`` asserts 1.125 → 1.12 and names banker's
    rounding in a comment. The live money path is ROUND_HALF_UP via
    ``services.markup.quantize_money`` (see the contract there); converting this
    helper would flip a test that documents its behaviour in order to change a
    number nobody is billed. It is also ``float``-based, which is the other
    reason not to grow a caller for it — the live path is Decimal end to end.
    Either delete it with its tests, or leave it; it is not part of this change.
    """
    multiplier = Decimal(str(1 + markup_pct / 100))
    return (internal_usd * multiplier).quantize(Decimal("0.01"))


async def get_markup_pct_map(
    tenant_ids: list[str], session: AsyncSession, settings: Settings
) -> dict[str, float]:
    """Batch lookup of markup_pct for many tenants in one round-trip.

    Falls back to DB-default → env-default for any tenant without a row.
    """
    if not tenant_ids:
        return {}
    stmt = select(TenantConfig.tenant_id, TenantConfig.markup_pct).where(
        TenantConfig.tenant_id.in_([*set(tenant_ids), DEFAULT_CONFIG_TENANT_ID])
    )
    rows = (await session.execute(stmt)).all()
    # markup_pct NULL = "not configured" (alembic 0044): same as no row, so it
    # falls through to the default exactly like get_markup_pct_with_source.
    by_tenant: dict[str, float] = {
        r.tenant_id: float(r.markup_pct) for r in rows if r.markup_pct is not None
    }
    default_pct = by_tenant.pop(DEFAULT_CONFIG_TENANT_ID, settings.default_markup_pct)
    return {tid: by_tenant.get(tid, default_pct) for tid in tenant_ids}


async def get_provider_costs(session: AsyncSession) -> dict[str, dict[str, Decimal]]:
    """Load all provider cost rates."""
    stmt = select(ProviderCost)
    result = await session.execute(stmt)
    return {
        p.provider: {
            "input_per_1m_usd": p.input_per_1m_usd,
            "output_per_1m_usd": p.output_per_1m_usd,
        }
        for p in result.scalars().all()
    }


async def get_app_cost_rate_rows(session: AsyncSession) -> list[AppCostRate]:
    """ONE read of ``billing_app_cost_rates``, rows whole.

    Split out of :func:`get_app_cost_rates` so a caller that must not read the
    table twice can take the rows once and derive everything from them — see
    ``services.invoices.close_period``, which needs both the mapping (gate,
    generator) and the whole rows (frozen provenance) and must not let the two
    come from different statements.
    """
    return list((await session.execute(select(AppCostRate))).scalars().all())


def app_cost_rate_map(rows: Sequence[AppCostRate]) -> dict[str, Decimal]:
    """The ONE place ``AppCostRate`` rows become the ``{app_key: rate}`` mapping.

    A second comprehension elsewhere would be a second definition of what
    "priced" means, and ⚠️ MEMBERSHIP (not value) is the discriminator the gate
    and the generator both test on — so the two copies drifting would change who
    gets refused and who gets billed $0.
    """
    return {a.app_key: a.cost_per_request_usd for a in rows}


async def get_app_cost_rates(session: AsyncSession) -> dict[str, Decimal]:
    """Load all app cost rates.

    Unchanged signature and behaviour: one read, mapped. Callers that need the
    rows as well should use :func:`get_app_cost_rate_rows` +
    :func:`app_cost_rate_map` instead of calling both.
    """
    return app_cost_rate_map(await get_app_cost_rate_rows(session))


class LicensesUnavailableError(RuntimeError):
    """The licenses service could not be consulted, so its answer is UNKNOWN.

    Distinct from "the tenant has no licenses", which is a successful 200 with
    an empty list. Conflating the two is what made an auth outage look like a
    quota-free tenant.
    """

    def __init__(self, tenant_id: str, reason: str) -> None:
        super().__init__(f"licenses service unavailable for tenant {tenant_id}: {reason}")
        self.tenant_id = tenant_id
        self.reason = reason


#: Cached default service-account token providers for the callers on this module
#: that are reached WITHOUT one -- keyed by the credentials they were built from.
#: Identical shape and rationale to ``services.seats._default_providers`` and
#: ``services.tenant_names._default_providers``; see the comment at the fallback in
#: :func:`fetch_licenses` for what it cost not to have this here.
_default_providers: dict[tuple[str, str], ServiceTokenProvider] = {}


def _default_token_provider(settings: Settings) -> ServiceTokenProvider | None:
    """The cached default provider for these credentials, or ``None``.

    ⚠️ ``None`` RATHER THAN RAISING, deliberately: this exists to ADD a header that
    was previously absent, so it must never convert a reachable licences service
    into an exception for a caller that already worked. With ``None`` the request
    goes out exactly as it did before, and the 401 it then takes is reported by
    :func:`fetch_licenses`'s own status check rather than by a surprise here.
    ``getattr`` for the same reason -- a narrow test settings fake must not fail
    over credentials it is not asserting.
    """
    from service_auth import ServiceTokenProvider as _Provider

    key = (
        getattr(settings, "keycloak_token_url", "") or "",
        getattr(settings, "service_client_id", "") or "",
    )
    if not any(key):
        return None
    provider = _default_providers.get(key)
    if provider is None:
        try:
            provider = _Provider(settings)
        except Exception:
            logger.info("licences_default_provider_unavailable", exc_info=True)
            return None
        _default_providers[key] = provider
    return provider


def clear_default_token_providers() -> None:
    """Drop the cached providers. For tests that change Keycloak settings."""
    _default_providers.clear()


async def fetch_licenses(
    tenant_id: str,
    settings: Settings,
    token_provider: ServiceTokenProvider | None = None,
) -> list[dict]:
    """Fetch active licenses from the internal licenses endpoint.

    Raises :class:`LicensesUnavailableError` when the service cannot be reached
    or answers anything other than 200. It deliberately does NOT degrade to
    ``[]``: an empty list is a real, meaningful answer ("this tenant holds no
    active licenses") and callers treat it as authoritative. The old code made
    the two indistinguishable, twice over:

      * there was no ``raise_for_status()``, so any non-200 fell out of the
        ``if`` to a bare ``return []``;
      * a 401/403 — the most likely failure of all, an expired S2S token or a
        Keycloak outage — is a *response*, not an ``httpx.HTTPError``, so the
        ``except`` never fired and not even a warning was logged.

    A dedicated exception rather than a bare ``httpx`` error because the two
    callers need to react DIFFERENTLY (see ``quota_status`` and
    ``generate_billing``); catching ``httpx.HTTPError`` would also swallow
    unrelated transport failures raised elsewhere in a caller's own work.
    """
    # ⛔ FALL BACK TO THE DEFAULT PROVIDER RATHER THAN SENDING NO HEADER, because
    # "no provider injected" is not a hypothetical on this path and it is not
    # harmless. `/internal/licenses` is under the licences service's
    # PROTECTED_PREFIXES, and `services.invoices.close_due_periods` calls
    # `close_period` with NO `token_provider` -- so every sweep-path fetch went out
    # unauthenticated and took an HTTP 401.
    #
    # MEASURED IN PRODUCTION 2026-09-13T07:50Z, on the first tick the monthly close
    # loop has ever run: of 35 August candidate tenants, **28 refused with
    # LicensesUnavailableError: HTTP 401** -- every one of them solely for want of
    # this header. The refusal itself was correct (a close must abort rather than
    # freeze a statement whose licence list silently degraded to []), which is
    # exactly why the symptom is a stuck month rather than a wrong invoice: the
    # periods are left OPEN and retried every tick, forever, until this is fixed.
    #
    # Same fallback, same cache shape and the same reasoning as
    # `services.seats._provider_for` and `services.tenant_names._provider_for` --
    # and `seats` having had it is precisely why seat resolution worked on the
    # sweep path while this did not. An injected provider still wins; both are
    # built from the same Settings, so they authenticate as the same account.
    provider = token_provider if token_provider is not None else _default_token_provider(settings)
    headers: dict[str, str] = {"X-Tenant-Id": tenant_id}
    if provider is not None:
        token = await provider.get_token()
        if token:
            headers["Authorization"] = f"Bearer {token}"

    try:
        async with httpx.AsyncClient(timeout=5.0) as client:
            resp = await client.get(
                f"{settings.licenses_url}/internal/licenses",
                params={"tenant_id": tenant_id, "status": "active"},
                headers=headers,
            )
    except httpx.HTTPError as exc:
        logger.warning("licenses_fetch_failed", tenant_id=tenant_id, exc_info=True)
        raise LicensesUnavailableError(tenant_id, f"transport error: {exc!r}") from exc

    if resp.status_code != 200:
        # A status code is NOT an httpx exception. 401/403 used to land here and
        # fall straight through to `return []` without logging.
        logger.warning(
            "licenses_fetch_failed",
            tenant_id=tenant_id,
            status_code=resp.status_code,
        )
        raise LicensesUnavailableError(tenant_id, f"HTTP {resp.status_code}")

    return resp.json()


async def _resolve_seat_count(
    tenant_id: str,
    settings: Settings,
    *,
    as_of: datetime | None = None,
    app_key: str | None = None,
    period_start: datetime | None = None,
    period_end: datetime | None = None,
) -> tuple[int | None, str]:
    """``(seat_count, seat_basis)`` for the per-seat access charge.

    ⭐ ``as_of`` IS THE INSTANT THE COUNT IS TAKEN AT, AND IT IS THIS FUNCTION — NOT
    ``assert_seat_count_available`` — THAT DECIDES WHAT AN INVOICE BILLS. The gate
    only refuses; the number multiplied by the rate and frozen into
    ``seat_fee_usd`` comes from here, via ``generate_billing``, which
    ``close_period`` then freezes. So a correct ``as_of`` on the gate alone leaves
    the billed figure counted at ``now()``.
    ⚠️ Found by test, not by reading: deleting ``as_of`` from the gate left the
    whole of ``tests/test_commercial_terms.py`` green, and the close-path test that
    now covers this showed the request reaching licences with no ``as_of`` at all.

    ⚠️ THE REPORT PATH AND THE CLOSE PATH MUST DIVERGE HERE, and this function is
    the report path. `generate_billing` is read by a dashboard and by an operator
    mid-outage, so it must stay available: a licences failure yields
    ``(None, "unavailable")`` and a $0 seat fee with the reason attached.

    ⛔ That tolerance is ONLY safe because nothing here is frozen.
    `services.invoices.close_period` calls `services.seats.get_seat_count`
    through `assert_seat_count_available` and lets `SeatCountUnavailableError`
    propagate, so an invoice is never written with a seat count licences declined
    to state. If you ever make close_period reuse this helper, a licences blip
    silently bills zero seats on a frozen invoice — the exact failure
    `seat_basis` exists to make visible.
    """
    try:
        count = await get_seat_count(
            tenant_id,
            settings,
            as_of=as_of,
            app_key=app_key,
            period_start=period_start,
            period_end=period_end,
        )
    except SeatCountUnavailableError as exc:
        logger.warning("seat_count_unavailable_report_path", tenant_id=tenant_id, error=str(exc))
        return None, SEAT_BASIS_UNAVAILABLE
    if period_start is not None:
        return count, SEAT_BASIS_ACTIVE_DURING_PERIOD
    return count, SEAT_BASIS_ACTIVE_LICENCES


async def generate_billing(
    tenant_id: str,
    month: str,
    session: AsyncSession,
    settings: Settings,
    token_provider: ServiceTokenProvider | None = None,
    licenses_data: list[dict] | None = None,
    app_rates: dict[str, Decimal] | None = None,
) -> dict:
    """Generate a billing report for a tenant for a given month (YYYY-MM).

    ``app_rates`` is an OPTIONAL injected snapshot of ``billing_app_cost_rates``.
    Omit it — as every report caller does (``routers/billing.py``,
    ``routers/dashboard.py``) — and this reads the table itself, exactly as
    before. ``services.invoices.close_period`` passes the one mapping it already
    read, so the gate that ALLOWED the close, the ``inputs`` that RECORD it and
    the lines that CHARGE for it cannot be computed from three different reads.
    """
    year, mon = int(month[:4]), int(month[5:7])
    month_start = date(year, mon, 1)
    month_end = date(year + 1, 1, 1) if mon == 12 else date(year, mon + 1, 1)

    fallback_pct, fallback_source = await get_markup_pct_with_source(tenant_id, session, settings)
    # ⭐ THE CONFIGURED MODE IS KEPT, NOT OVERWRITTEN, and that is what makes a
    # per-app posture expressible at all. `effective_billing_mode` is
    # `(configured_mode, byo_mode) -> mode`, so a SECOND byo posture has to be
    # combined with the SAME configured mode — not with the already-resolved
    # tenant-wide answer. This line used to reassign `billing_mode` in place, which
    # destroyed the left-hand input one line before it was needed again.
    configured_billing_mode = await get_billing_mode(tenant_id, session)
    # Phase 4 byo_mode gate: internal/demo tenants (local_allowed|platform_only)
    # bill $0; commercial (byo_required) keep their configured mode.
    byo_mode = await get_byo_mode(tenant_id, settings)
    # ⚠️ `billing_mode` KEEPS ITS NAME AND ITS MEANING: the TENANT-WIDE effective
    # mode. It stays the default for every line and the sole input to the two
    # tenant-level readers below (see the note at the access-charge block). Only
    # allowlisted lines consult `per_app_billing_mode`.
    billing_mode = effective_billing_mode(configured_billing_mode, byo_mode)
    # Per-line markup is resolved at the last instant of the billed month; a
    # mid-month rule change therefore takes effect from the next month's invoice.
    bill_ts = datetime(month_end.year, month_end.month, month_end.day, tzinfo=UTC) - timedelta(
        microseconds=1
    )
    resolver = await build_markup_resolver(session, tenant_id=tenant_id, ts=bill_ts)
    # ⚠️ `is None`, NOT falsiness: an EMPTY mapping is a real answer — the
    # measured prod state of `billing_app_cost_rates` is zero rows — and `or`
    # here would silently re-read the table for the caller that injected it,
    # putting the three-reads defect back for exactly the tenant whose rates
    # table is empty.
    if app_rates is None:
        app_rates = await get_app_cost_rates(session)

    # Get usage from metering_daily
    usage_stmt = (
        select(
            MeteringDaily.license_id,
            MeteringDaily.service_key,
            func.sum(MeteringDaily.request_count).label("requests"),
            func.sum(MeteringDaily.denied_count).label("denied"),
            func.sum(MeteringDaily.budget_denied_count).label("budget_denied"),
        )
        .where(
            MeteringDaily.tenant_id == tenant_id,
            MeteringDaily.bucket_date >= month_start,
            MeteringDaily.bucket_date < month_end,
        )
        .group_by(MeteringDaily.license_id, MeteringDaily.service_key)
    )
    usage_result = await session.execute(usage_stmt)
    usage_rows = usage_result.all()

    # Get costs from cost_daily — grouped to the markup line key
    # (license, project, app, provider, model, byo) so markup resolves per line.
    #
    # ⭐ `project_id` IS PART OF THE GROUPING KEY, NOT JUST OF THE SELECT.
    # `services.markup` scores a project-pinned rule at `_W_PROJECT`, so two
    # days of spend that differ ONLY in project can carry two different
    # percentages. A grouping key without `project_id` sums them into one line
    # first and then prices that mixture once — so whichever project's rate the
    # merged line resolved to would silently be charged for both, and the
    # project axis would be unbillable no matter what the resolver does.
    # Splitting the group is what makes a per-project rate *expressible*; it
    # moves no tenant's total on its own, because the same rate applied to two
    # halves sums to the same money as applied to the whole.
    cost_stmt = (
        select(
            CostDaily.license_id,
            CostDaily.project_id,
            CostDaily.service_key,
            CostDaily.provider,
            CostDaily.served_model,
            CostDaily.byo,
            func.sum(CostDaily.total_input_tokens).label("inp"),
            func.sum(CostDaily.total_output_tokens).label("outp"),
            # ⭐ BYO COST BASIS: the provider's OWN reported charge where the rollup has one, the
            # price-card estimate otherwise — chosen PER DAILY ROW, never on the group sum, so a
            # month where only some days reported a vendor cost is not understated. Scoped to BYO
            # lines, where the figure is the customer's real provider bill and the base of the
            # byo_markup_only fee; platform-paid lines keep the price card they are costed on.
            # ⚠️ Inert today: vendor_cost_usd is NULL on every event until an emitter sends it.
            func.sum(
                case(
                    (
                        and_(
                            CostDaily.byo.is_(True),
                            CostDaily.total_vendor_cost_usd.isnot(None),
                            CostDaily.total_vendor_cost_usd > 0,
                        ),
                        CostDaily.total_vendor_cost_usd,
                    ),
                    else_=CostDaily.total_cost_usd,
                )
            ).label("cost"),
        )
        .where(
            CostDaily.tenant_id == tenant_id,
            CostDaily.bucket_date >= month_start,
            CostDaily.bucket_date < month_end,
        )
        .group_by(
            CostDaily.license_id,
            CostDaily.project_id,
            CostDaily.service_key,
            CostDaily.provider,
            CostDaily.served_model,
            CostDaily.byo,
        )
        # ⚠️ THIS ORDER BY NO LONGER DEFENDS ANY MONEY, AND THE COMMENT THAT SAID
        # IT DID IS GONE ON PURPOSE. It used to be load-bearing: the allowance
        # was drained line by line BEFORE per-line markup, so whichever line the
        # planner emitted first was the line whose markup rate was forgone, and
        # the same month could re-bill to a different number. That hole is closed
        # at the source — `allocate_allowance` splits the allowance PRO-RATA
        # across every line at once, so the total is the same for every
        # permutation and no ORDER BY can protect or endanger it.
        #
        # What is left is presentational and worth keeping: the snapshot's
        # `licenses` list and the per-provider breakdowns are built by iterating
        # these rows, so a fixed order makes a re-run's DOCUMENT byte-comparable
        # instead of merely arithmetically equal. Ordering on the whole GROUP BY
        # key is a total order (no ties left to the planner) and costs nothing.
        # ⛔ Do NOT re-derive a money argument from this comment: if a future
        # change makes the total order-sensitive again, that change is the defect.
        .order_by(
            CostDaily.license_id,
            CostDaily.project_id,
            CostDaily.service_key,
            CostDaily.provider,
            CostDaily.served_model,
            CostDaily.byo,
        )
    )
    cost_result = await session.execute(cost_stmt)
    cost_rows = cost_result.all()

    # Also check monthly_usage for the current month (may not be in daily yet).
    # A single license has one row PER (subject_id, plan_id) — e.g. per-user +
    # S2S rows — so aggregate request_count with a SUM grouped by license_id;
    # building a plain dict here would non-deterministically keep one row.
    #
    # ⚠️ `billing_metering_monthly_usage` HAS NO `tenant_id` COLUMN — not on the
    # model (models/db/rollups.py) and not in the table (alembic 0001 baseline:
    # id, license_id, plan_id, month_start, request_count, budget_denied_count,
    # subject_id, denied_count). There is therefore NO `tenant_id` predicate to
    # add here, and tenant isolation on this read rests entirely on the LICENCE:
    # a licence belongs to exactly one tenant, the rows are SUMmed per
    # `license_id`, and the only keys ever looked up (`:~470` below) come from
    # `license_usage`, which is built solely from the tenant-filtered
    # `MeteringDaily` / `CostDaily` queries above.
    #
    # ⭐ SO THE SCOPE IS MADE STRUCTURAL RATHER THAN LEFT IMPLICIT. Restricting
    # the read to the licence ids those two tenant-filtered queries returned is
    # OUTPUT-IDENTICAL (no other key is ever consulted) and turns "no other
    # tenant's row can reach this dict because nobody asks for it" into "no other
    # tenant's row is ever fetched". It also stops a per-tenant invoice scanning
    # the whole fleet's month. If a tenant_id column is ever added to this table,
    # the predicate belongs here beside the licence filter — not instead of it.
    #
    # ⛔ System/S2S licences are excluded because both billing loops below skip
    # them (`is_system_license`), so the sentinel is never a `license_usage` key.
    # The sentinel id is the ONE licence id shared across tenants, which is
    # exactly why it must not be fetched per-tenant as if it were one tenant's.
    billed_license_ids = {
        r.license_id for r in usage_rows if not is_system_license(r.license_id)
    } | {c.license_id for c in cost_rows if not is_system_license(c.license_id)}
    monthly_rows: dict[str, int] = {}
    if billed_license_ids:
        monthly_stmt = (
            select(
                MeteringMonthlyUsage.license_id,
                func.sum(MeteringMonthlyUsage.request_count).label("request_count"),
            )
            .where(
                MeteringMonthlyUsage.license_id.in_(billed_license_ids),
                MeteringMonthlyUsage.month_start == month_start,
            )
            .group_by(MeteringMonthlyUsage.license_id)
        )
        monthly_result = await session.execute(monthly_stmt)
        monthly_rows = {str(m.license_id): (m.request_count or 0) for m in monthly_result.all()}

    # Get token usage
    token_stmt = select(TokenUsage).where(
        TokenUsage.tenant_id == tenant_id,
        TokenUsage.period_type == "monthly",
        TokenUsage.period_start == month_start,
    )
    token_result = await session.execute(token_stmt)
    token_rows = {
        str(t.license_id) if t.license_id != NULL_LICENSE_ID else "tenant": t
        for t in token_result.scalars().all()
    }

    # Get token budgets
    budget_stmt = select(TokenBudget).where(
        TokenBudget.tenant_id == tenant_id,
        TokenBudget.active.is_(True),
        TokenBudget.period == "monthly",
    )
    budget_result = await session.execute(budget_stmt)
    budgets = {
        str(b.license_id) if b.license_id != NULL_LICENSE_ID else "tenant": b
        for b in budget_result.scalars().all()
    }

    # Fetch licenses from licenses service.
    #
    # DELIBERATELY TOLERANT here, unlike `quota_status` which 503s. The two
    # callers want different things from the same outage:
    #   * quota_status EXISTS to answer "what are this tenant's quotas?" — an
    #     empty list IS the answer it would give, so an outage rendering as an
    #     empty list is indistinguishable from a real one and must fail;
    #   * this function's money comes from CostDaily / MeteringDaily in our own
    #     DB. `license_map` only ENRICHES the per-license entries with app/plan
    #     metadata. Losing it degrades labelling, not a single figure — so
    #     hard-failing every invoice on a licenses blip would be a worse trade.
    # It is LOGGED rather than swallowed, which is what the old code did (a
    # non-200 returned [] with no log line at all).
    # ⭐ A CALLER MAY SUPPLY THE LICENCES IT ALREADY HOLDS, and services.invoices
    # does. Without this the invoice path fetched twice — once to freeze into
    # `inputs.licenses_response`, once here — so a licences blip BETWEEN the two
    # calls froze a record whose `inputs` said one thing and whose `snapshot`
    # labelling said another. No figure ever differed (see above: this map only
    # ENRICHES labels), but a statement that exists to be reproducible must not
    # be internally inconsistent about what it read. One fetch, one truth.
    #
    # Passing None keeps the previous behaviour exactly, which is what the three
    # report callers want: a labelling blip must not fail a dashboard.
    #
    # ⚠️ "INVOICE FIGURES UNAFFECTED" IS NO LONGER UNCONDITIONALLY TRUE, WHICH IS WHY
    # THE DEGRADATION IS NOW RECORDED IN A VARIABLE INSTEAD OF ONLY IN A LOG LINE.
    # Since the per-app BYO posture landed, `license_map` is the source of the app key
    # a CommonPractice line is priced against — so for THOSE lines an empty map is not
    # a labelling gap, it is a missing input to a money decision. Everything else on
    # the statement is still unaffected exactly as before.
    licenses_degraded = False
    if licenses_data is None:
        try:
            licenses_data = await fetch_licenses(tenant_id, settings, token_provider=token_provider)
        except LicensesUnavailableError:
            licenses_degraded = True
            logger.warning(
                "billing_license_enrichment_degraded",
                tenant_id=tenant_id,
                month=month,
                detail=(
                    "licenses unreachable; metadata omitted. Figures are unaffected "
                    "EXCEPT for service_keys in PER_APP_BYO_SERVICE_KEYS, whose app "
                    "key cannot be derived — those refuse rather than degrade"
                ),
            )
            licenses_data = []
    # ⭐ AN INJECTED LIST IS NOT DEGRADED. `services.invoices.close_period` passes the
    # licences it already fetched, and that fetch RAISES on failure — so if we are
    # holding a caller's list, licences answered. Defaulting this to True "to be safe"
    # would refuse every close in the estate for CommonPractice tenants.
    license_map = {}
    for lic in licenses_data:
        lic_id = lic.get("id", "")
        license_map[lic_id] = lic

    # PER-APP BYO POSTURE. One licences call per DISTINCT app key, issued here — once,
    # before either billing loop — rather than inside them. See
    # `_resolve_per_app_billing_modes` for the 5xx rule and the outage decision.
    #
    # ⚠️ THE TENANT-WIDE LOOKUP ABOVE HAS ALREADY PRIMED THE 5-MINUTE CACHE under the
    # bare `tenant_id` key, so an app with NO override (a 404 on the per-product
    # route) costs exactly ONE request in total and then reads the tenant-wide answer
    # out of cache — it does not double the licences traffic for the fall-through
    # case.
    per_app_billing_mode = await _resolve_per_app_billing_modes(
        tenant_id,
        settings,
        configured_billing_mode=configured_billing_mode,
        license_map=license_map,
        licenses_degraded=licenses_degraded,
        rows=[*usage_rows, *cost_rows],
        token_provider=token_provider,
    )

    def _line_billing_mode(license_id, service_key: str | None) -> str:
        """The billing mode for ONE line: the per-app posture, else the tenant-wide one.

        ⛔ EVERY EXIT THAT IS NOT AN ALLOWLISTED APP RETURNS `billing_mode` UNCHANGED.
        That is the byte-identity promise for the rest of the fleet, written as a
        single function so it cannot drift between the two loops that call it.
        """
        if not per_app_billing_mode or service_key not in PER_APP_BYO_SERVICE_KEYS:
            return billing_mode
        app_key = _licences_app_key(license_id, license_map)
        if app_key is None:
            return billing_mode
        return per_app_billing_mode.get(app_key, billing_mode)

    # Build per-license billing
    def _blank_license_usage() -> dict:
        return {
            "requests": 0,
            "denied": 0,
            "budget_denied": 0,
            "app_request_cost": Decimal("0"),
            "app_request_billed": Decimal("0"),
            "llm_cost": Decimal("0"),
            "llm_billed": Decimal("0"),
            # ALLOWANCE axis: included-usage credit applied to this license's LLM
            # lines before markup (reduces billed, never internal). See below.
            "included_applied": Decimal("0"),
            # PLATFORM-FEE axis: the absolute byo_platform_fee surcharge charged
            # on this licence's BYO lines, tracked SEPARATELY from llm_billed
            # (which it is also part of). Without this it is unrecoverable — the
            # relative and absolute components are indistinguishable once summed,
            # which is why the frozen invoice column could only ever say 0.
            "platform_fee": Decimal("0"),
            # BYO REFERENCE axis (billing_mode byo_markup_only): the provider cost of BYO lines the
            # CUSTOMER paid. Kept visible, but NOT in llm_cost — the platform paid nothing for it,
            # and counting it as internal cost reports a negative margin on every such invoice.
            "byo_reference_cost": Decimal("0"),
            "by_provider": {},
            "by_provider_billed": {},
            "service_keys": set(),
        }

    license_usage: dict[str, dict] = {}
    # ⭐ Every distinct markup provenance actually APPLIED to a line this month.
    # A statement quoting one percentage can still have been billed from several
    # sources (a per-model rule here, the settings constant there); recording the
    # set is what makes "why is this the number?" answerable from the row alone.
    markup_sources_applied: set[str] = set()
    # Service keys already reported as unpriced this run — the counter counts
    # every occurrence, the LOG fires once per key so a month of daily buckets
    # for one unpriced key does not produce 30 identical lines.
    unpriced_keys_logged: set[str] = set()

    # ACCESS-billing axis: fixed access fees + included-usage allowances for this
    # tenant-month. Fetched BEFORE the billing loops so the allowance can offset
    # incurred LLM cost *before* markup is applied (the loops below consume it).
    # Pure access revenue (no internal cost behind it) → the fixed fee folds into
    # customer_cost + margin (never internal_cost); the allowance reduces billed.
    # Skipped for internal/demo tenants (which bill $0).
    access_charges: list[dict] = []
    access_total = Decimal("0")
    access_by_app: dict[str, Decimal] = {}
    # SEAT axis, kept SEPARATE from access_total on purpose. Both are access
    # revenue and both land in customer_cost, but `access_charge_usd` means "flat
    # access fees" on the invoice and `seat_fee_usd` is its own column
    # (billing_invoices, since 0024). Summing them into one number would make the
    # two columns overlap and an invoice unreconcilable against its own rate card.
    seat_fee_total = Decimal("0")
    seat_count: int | None = None
    seat_basis: str | None = None
    seat_counts_by_scope: dict[str, tuple[int | None, str]] = {}
    # Allowance buckets: service_key -> remaining USD credit; "" = tenant-wide
    # (service_key IS NULL). monthly-cadence charges grant a recurring allowance
    # that resets each billed month (rebuilt here from included_cost_usd);
    # one_time charges grant a durable prepaid credit whose remaining balance is
    # seeded below from history (grant minus prior-month consumption).
    allowance_remaining: dict[str, Decimal] = {}
    # ⛔ READER 1 OF 2 THAT DELIBERATELY STAYS TENANT-WIDE, AND THE SILENCE WOULD HAVE
    # BEEN THE BUG. `billing_mode` used to be one scalar governing three things; the
    # per-app posture makes it non-scalar, so each reader has to be DECIDED rather
    # than inherited. This one, and the `ACCESS_BASIS_NOT_APPLICABLE` line below it,
    # keep reading the TENANT-WIDE mode on purpose:
    #
    #   * What this gate admits is `tenant_access_charge` — the flat platform fee
    #     ($5,000/month for the one tenant that carries an open row), the per-seat
    #     fee, and the included-usage allowances. Those are TENANT-level contractual
    #     terms. Whether a tenant pays to be on the platform is not a per-product BYO
    #     question, and no per-product override in licences expresses it.
    #   * Driving it per-app would make one commercial product inside an internal
    #     tenant pull in the WHOLE tenant's access charge — a ~$5,000/month over-bill
    #     nobody asked for, arriving as a side effect of a change whose entire purpose
    #     is to correct a $0 usage line. The seat axis hangs off the same branch and
    #     would move with it.
    #   * Direction matters: leaving it tenant-wide can only UNDER-charge access (the
    #     pre-existing behaviour, unchanged here); moving it would OVER-charge on a
    #     posture that was never about access at all.
    #
    # So the access/seat/allowance axis is out of scope BY DECISION. If per-product
    # access terms are ever wanted, they belong in `tenant_access_charge` (which
    # already has a `service_key` column), not in the BYO posture.
    if billing_mode != BILLING_MODE_INTERNAL:
        for c in await active_access_charges_for_month(
            session,
            tenant_id=tenant_id,
            month_start=month_start,
            month_end=month_end,
            bill_ts=bill_ts,
        ):
            if c.cadence == "per_seat":
                # ⚠️ A per-seat row's amount_usd is a RATE, not a charge. It is
                # resolved against the seat count and must NOT be added to
                # access_total — before `per_seat` existed, an unknown cadence fell
                # through `_applies`' `return True` and would have been billed as a
                # flat $25 for any seat count. That is why the cadence and this
                # multiplier had to land in the same commit.
                seat_rate = Decimal(c.amount_usd)
                # ⭐ PER-TERM SCOPE. A term pinned to a service_key counts THAT app's seats over
                # the period (see `seat_scope_for_charge`); an unpinned term keeps the all-apps
                # point-in-time count. Resolved per scope and memoised, so two unpinned terms
                # still share one lookup exactly as before.
                scope = seat_scope_for_charge(c.service_key, month_start, month_end)
                scope_key = c.service_key or ""
                if scope_key not in seat_counts_by_scope:
                    seat_counts_by_scope[scope_key] = await _resolve_seat_count(
                        tenant_id, settings, as_of=bill_ts, **scope
                    )
                line_seat_count, line_seat_basis = seat_counts_by_scope[scope_key]
                if seat_count is None and seat_basis is None:
                    # The totals-level figure is the FIRST per-seat term's count. With one term
                    # (every tenant today) it is exact; each line carries its own count below.
                    seat_count, seat_basis = line_seat_count, line_seat_basis
                if line_seat_count is not None:
                    # ⭐ THE ACCUMULATOR SUMS THE CENTS THE LINES ARE CHARGED AS.
                    # Identical shape and identical reason to the flat axis at
                    # `access_total` below: this expression is the SAME one the
                    # line renders (`quantize_money(seat_rate * seat_count)`), so
                    # `q(seat_fee_total) == Σ q(line)` holds BY CONSTRUCTION
                    # across any number of per-seat terms rather than only while
                    # a tenant has exactly one.
                    #
                    # ⚠️ NAME THE AXIS: the `Σ q(line)` here is over the
                    # `cadence == "per_seat"` lines of `access_charges[]` ONLY,
                    # and the column it reconciles to is `totals.seat_fee_usd`
                    # (via `seat_fee_total_charged`), NOT `access_charge_usd`.
                    # The two are DISJOINT — see the corrected note on the flat
                    # line's `amount_usd` below for the full mapping. Note also
                    # that a line whose `seat_count is None` renders `q(0)` and
                    # this accumulator skips it, so the identity survives that
                    # branch too.
                    seat_fee_total += quantize_money(seat_rate * line_seat_count)
                access_charges.append(
                    {
                        "service_key": c.service_key,
                        # The resolved CHARGE, plus the rate and quantity beside it —
                        # a line reading "$75" without "$25 x 3" is not reconcilable.
                        "amount_usd": str(
                            quantize_money(seat_rate * line_seat_count)
                            if line_seat_count is not None
                            else quantize_money(Decimal("0"))
                        ),
                        "seat_rate_usd": str(seat_rate),
                        "seat_count": line_seat_count,
                        "seat_basis": line_seat_basis,
                        "cadence": c.cadence,
                        "basis": ACCESS_BASIS_CHARGE,
                    }
                )
                continue
            amt = Decimal(c.amount_usd)
            # ⭐ QUANTIZE THE LEAF, THEN SUM THE LEAVES — BOTH HALVES, OR NEITHER.
            #
            # This comment used to say the accumulator must take the RAW amount
            # because `access_total` is quantised ONCE at `access_total_charged`.
            # That is only coherent while the LINE is also rendered raw. Once the
            # line below became `q(amt)`, the aggregate `q(Σ amt)` and the
            # itemisation `Σ q(amt)` are different numbers, and the invoice stops
            # adding up to itself: two fees at $10.005 itemise as 10.01 + 10.01 =
            # 20.02 under a column reading 20.01. Summing the quantised leaf makes
            # `q(access_total) == Σ q(line)` hold BY CONSTRUCTION, for any number
            # of lines — which is what "quantise the leaves, then sum" means.
            #
            # ⚠️ IT IS A PRICING RULE, NOT A DISPLAY RULE: each contractual fee is
            # charged as its own cent, so a tenant with n sub-cent fees can be
            # billed up to n/2 cents more than `q(Σ)` would have charged. MEASURED
            # ON PRODUCTION 2026-09-15 before choosing it, so the change is
            # provably inert today rather than merely plausible:
            #   * `tenant_access_charge` holds 10 rows, **0 of them sub-cent**
            #     (`amount_usd <> round(amount_usd, 2)` → 0), so `q(amt) == amt`
            #     for every row that exists and no charged total can move; and
            #   * each of the 3 tenants holding rows has exactly ONE flat lineage
            #     (`service_key IS NULL`) and one `per_seat` lineage — where a
            #     "lineage" is the effective-dated series for one
            #     `(tenant_id, service_key)`. `active_access_charges_for_month`
            #     filters at a single `bill_ts`, so two rows of one lineage can
            #     never be billed in one month. Both halves of the trigger —
            #     sub-cent scale AND a second concurrent lineage — are absent,
            #     and `ix_accesscharge_open` (UNIQUE on
            #     `(tenant_id, coalesce(service_key,''))` WHERE
            #     `effective_until IS NULL`) means the second one cannot appear
            #     without a deliberate new `service_key`.
            #
            # ⚠️ CORRECTED 2026-09-15 — this bullet used to read as "all three
            # tenants carry a live flat charge". They do NOT, and the difference
            # matters to anyone sizing a change here. Re-measured on the prod
            # `billing` DB (worker4), all 10 rows dumped with their windows:
            #   * ONLY tenant `0195f48b-…0100` has OPEN rows (`effective_until IS
            #     NULL`): its flat $5000.00/`monthly` and its `__seat__`
            #     $25.00/`per_seat`.
            #   * The other two tenants have **ZERO** open rows. Each of their
            #     four rows is closed: one window ran 2026-09-11 10:05 → 16:42,
            #     and its "supersession" was written with
            #     `effective_from == effective_until` (both
            #     `2026-09-11 16:42:22.922442+00`) — an EMPTY window, which
            #     `effective_until > bill_ts` can never select. So those two
            #     tenants bill **$0.00 access** for every month after
            #     2026-09-11 16:42, and a superseding row that terminates its own
            #     lineage is almost certainly not what the writer intended.
            # This does not weaken the argument above (a closed lineage cannot
            # make two terms concurrent either) — it means the live population
            # this change was measured inert against is ONE tenant, not three.
            #
            # ⚠️ The overview must fold the fees the SAME way or the two readers
            # disagree: see `services.access_charge.active_access_charges_all_tenants`
            # and `seat_rates_all_tenants`, both of which quantise per lineage.
            access_total += quantize_money(amt)
            access_charges.append(
                {
                    "service_key": c.service_key,
                    # ⭐ DISPLAY ONLY, and quantised so this list carries ONE
                    # form. Its two sibling producers — the per_seat line above
                    # and the explicit zero line below — both render
                    # `quantize_money(...)` too.
                    #
                    # ⛔ CORRECTED 2026-09-15 — this used to say "the aggregate
                    # column that totals this list is `q(access_total)`". NO
                    # SINGLE COLUMN TOTALS THIS LIST. `access_charges[]` is one
                    # itemisation over TWO DISJOINT axes, split by CADENCE, and
                    # the identity is per-axis:
                    #   * this branch (`cadence != "per_seat"` — today `monthly`
                    #     and `one_time`, and any forward-compat cadence, since
                    #     the `per_seat` test above is the only discriminator)
                    #     accumulates `access_total` → `totals.access_charge_usd`;
                    #   * the per_seat branch accumulates `seat_fee_total` →
                    #     `totals.seat_fee_usd` (see `models/domain.py`, whose
                    #     `seat_fee_usd` docstring says "⚠️ DISJOINT from
                    #     access_charge_usd");
                    #   * the no-match line below feeds NEITHER accumulator, and
                    #     its amount is exactly `q(0)`, so it cannot move either.
                    # So `q(access_total) == Σ q(line)` over the FLAT lines only,
                    # and `q(seat_fee_total) == Σ q(line)` over the per-seat lines
                    # only. Σ over the whole list matches neither column: measured
                    # on the prod `billing` DB 2026-09-15, the one tenant with open
                    # rows carries BOTH shapes (`service_key IS NULL`/$5000.00/
                    # `monthly` and `__seat__`/$25.00/`per_seat`), so the list sums
                    # to `5000.00 + q(25 × seats)` while `access_charge_usd` is
                    # `5000.00`. `tests/test_invoice_itemisation_is_self_consistent
                    # .py` encodes the split by FILTERING ON CADENCE on both sides
                    # (`c["cadence"] != "per_seat"` :341, `== "per_seat"` :415).
                    #
                    # Either way the quantize here is load-bearing: rendered raw, a
                    # $10.005 fee itemised as "10.00500000" under a column reading
                    # "10.01", and an invoice whose lines do not add up to the
                    # column that totals THEIR axis cannot be reconciled by anyone.
                    "amount_usd": str(quantize_money(amt)),
                    "cadence": c.cadence,
                    "basis": ACCESS_BASIS_CHARGE,
                }
            )
            if c.service_key is not None:
                # Same fold as `access_total` above, for the same reason: where
                # this map IS populated it is the per-app breakdown of the FLAT
                # axis only — `access_total` / `totals.access_charge_usd`,
                # rendered per licence at `licenses[].costs.access_charge_usd` —
                # so summing raw here would put the breakdown a cent off that
                # aggregate.
                #
                # ⛔ CORRECTED 2026-09-15 — this said "the per-app breakdown of
                # the lines that list sums to". It is NOT a breakdown of
                # `access_charges[]`: the `per_seat` branch `continue`s well
                # before this line, so seat lines never reach this map and
                # `seat_fee_usd` has no per-app breakdown at all. Only the flat
                # axis does.
                #
                # ⛔ CORRECTED 2026-09-15 — this comment used to claim the
                # breakdown "adds up to the aggregate it breaks down". IT DOES
                # NOT, and the quantize is INERT for the only shape production
                # currently bills. THE GUARD ABOVE IS THE LIMITATION: a
                # TENANT-WIDE charge (`service_key IS NULL`) lands in
                # `access_total` — and therefore in `totals.access_charge_usd` —
                # but is deliberately NOT attributable to any app, so it enters
                # no `access_by_app` bucket. `licenses[].costs.access_charge_usd`
                # reads `access_by_app.get(app_key, Decimal("0"))`, so every
                # per-app cell falls back to the zero default (rendered $0.00)
                # under an aggregate carrying the full fee. Measured on the prod
                # `billing` DB 2026-09-15: the ONLY tenant with an open access
                # charge holds exactly that shape — `service_key IS NULL`,
                # $5000.00 `monthly` — so it reports
                # `totals.access_charge_usd = "5000.00"` over a per-app breakdown
                # of 0, and `Σ licenses[].costs.access_charge_usd` misses the
                # aggregate by $5000.00, not by a rounding cent. The fold below
                # is therefore INERT on today's population: no row reaches it.
                # ⚠️ This is a REPORTING gap only: nothing here is a charge. The
                # invoice's own `access_charge_usd` column comes from
                # `access_total`, and `access_charges[]` itemises the NULL-key
                # line explicitly, so no customer is over- or under-billed and no
                # total moves.
                # ⛔ Do NOT "fix" it by spreading a tenant-wide fee across apps
                # here: which app a non-app-scoped fee belongs to is a commercial
                # decision (equal split? by usage? by licence?), it would make the
                # per-licence figure depend on how many licences a tenant happens
                # to hold, and it has its own blast radius through every consumer
                # of `licenses[].costs`. Whoever takes that decision owns it; this
                # line only refuses to invent it.
                access_by_app[c.service_key] = access_by_app.get(
                    c.service_key, Decimal("0")
                ) + quantize_money(amt)
            if c.cadence == "monthly" and c.included_cost_usd is not None:
                key = c.service_key or ""
                allowance_remaining[key] = allowance_remaining.get(key, Decimal("0")) + Decimal(
                    c.included_cost_usd
                )
        # Durable one_time credits net across months. active_access_charges_for_month
        # gates one_time to its grant month (correct for the fixed FEE), so netting
        # is seeded independently: the remaining balance entering this month, drawn
        # down by the cost loop below like a monthly bucket but WITHOUT resetting.
        for key, remaining in (
            await one_time_remaining_before(
                session, tenant_id=tenant_id, month_start=month_start, bill_ts=bill_ts
            )
        ).items():
            allowance_remaining[key] = allowance_remaining.get(key, Decimal("0")) + remaining

    # ⭐ THE PLATFORM FEE IS ALWAYS A LINE; ITS AMOUNT MAY BE $0.
    #
    # `totals.access_charge_usd` was already an explicit "0.00" when nothing
    # matched, but `access_charges` was left `[]` — so the AGGREGATE said "this
    # axis was evaluated and came to zero" while the ITEMISATION said nothing at
    # all. An invoice with no platform-fee row cannot be audited or reconciled;
    # one carrying an explicit $0.00 row can. The two now agree.
    #
    # ⚠️ This adds a line, never an amount: `access_total`, `seat_fee_total` and
    # `access_by_app` are deliberately NOT touched, so no tenant's price moves by
    # a cent. (`seat_fee_total` named explicitly as of 2026-09-15: the old text
    # listed only two of the THREE accumulators `access_charges[]` is itemised
    # against, which read as if the seat axis were covered by `access_total`.)
    #
    # ⚠️ IT IS THE THIRD PRODUCER OF `access_charges[]` AND IT BELONGS TO NEITHER
    # AXIS: `cadence` is None, so it is excluded from the per-seat filter
    # (`== "per_seat"`) and INCLUDED by the flat one (`!= "per_seat"`) that the
    # itemisation tests use. That is safe only because its amount is exactly
    # `q(0)` — it can move no axis' total. ⛔ Never give this line a nonzero
    # amount without also deciding which accumulator it feeds.
    if not access_charges:
        access_charges.append(
            {
                "service_key": None,
                "amount_usd": str(quantize_money(Decimal("0"))),
                "cadence": None,
                # ⛔ READER 2 OF 2 THAT STAYS TENANT-WIDE. It DESCRIBES the branch
                # directly above — "the access axis was not evaluated, because this
                # tenant is internal" — so it must read the same mode that gate read
                # or the basis would state something the code did not do. A per-app
                # mode here would label a $0.00 access line `no_matching_access_charge`
                # on a tenant whose access lookup was never performed at all.
                "basis": (
                    ACCESS_BASIS_NOT_APPLICABLE
                    if billing_mode == BILLING_MODE_INTERNAL
                    else ACCESS_BASIS_NO_MATCH
                ),
            }
        )

    for row in usage_rows:
        # License-attributed billing only: S2S/system usage (sentinel license)
        # is excluded from invoices, mirroring the LLM-cost loop below. Without
        # this guard, system traffic on a service_key with a nonzero AppCostRate
        # gets a spurious per-request charge (and a sentinel license entry).
        if is_system_license(row.license_id):
            continue
        lic_id = str(row.license_id)
        lu = license_usage.setdefault(lic_id, _blank_license_usage())
        lu["requests"] += row.requests
        lu["denied"] += row.denied
        lu["budget_denied"] += row.budget_denied or 0
        lu["service_keys"].add(row.service_key)

        # ⭐ A MISSING RATE ROW AND A DELIBERATE $0 RATE ARE DIFFERENT FACTS.
        # `app_rates.get(key, 0)` collapses them: a service_key with no row in
        # `billing_app_cost_rates` bills at $0 exactly like one an operator
        # priced at 0 on purpose. Worse, that fabricated $0 makes `app_incurred`
        # zero, and services/markup.py gates its zero-margin alarm on
        # `incurred_usd > 0` — so the missing rate SUPPRESSES the alarm that
        # would have reported it. Measured 2026-09-08: `billing_app_cost_rates`
        # has 0 rows in prod, so every service_key with usage takes this path
        # and nothing anywhere says so.
        #
        # ⚠️ MEMBERSHIP, not value, is the discriminator. `get_app_cost_rates`
        # returns every row including one whose `cost_per_request_usd` is 0
        # (the column is NOT NULL), so `key in app_rates` is true for a genuine
        # free service and false only for an absent row. Testing `rate == 0`
        # instead would flag deliberate zeroes and make the signal useless.
        #
        # ⛔ REPORT ONLY. No rate is invented and no price moves; `rate` below is
        # resolved exactly as before.
        if row.requests > 0 and row.service_key not in app_rates:
            observability_metrics.app_rate_missing_total.labels(
                service_key=row.service_key or "unknown", surface="invoice"
            ).inc()
            if row.service_key not in unpriced_keys_logged:
                unpriced_keys_logged.add(row.service_key)
                logger.warning(
                    "app_rate_missing",
                    tenant_id=tenant_id,
                    service_key=row.service_key,
                    month=month,
                    requests=row.requests,
                )

        # App-request cost line, billed per (license, app) — provider/model NULL.
        # ⚠️ The unpriced-service_key signal is emitted ONCE, at the guard above —
        # an earlier revision of this branch counted it a second time here on the
        # SAME condition, which would have double-counted every unpriced key and
        # logged one line per daily bucket instead of one per key.
        rate = app_rates.get(row.service_key, Decimal("0"))
        app_incurred = rate * row.requests
        lu["app_request_cost"] += app_incurred
        lic_uuid = row.license_id if row.license_id != NULL_LICENSE_ID else None
        app_line = compute_billed_line_detailed(
            resolver,
            # ⭐ READER 3: PER-LINE. An app-request line is priced for ONE product —
            # `row.service_key` is on the grouping key — so it is exactly the axis the
            # per-app posture is about. It is included alongside the LLM lines below
            # rather than left behind because the defect is the same on both: a
            # CommonPractice line inside an internally-postured tenant bills $0
            # whichever ledger it came from, and fixing one while leaving the other
            # would make the same product bill two different ways on one statement.
            _line_billing_mode(row.license_id, row.service_key),
            tenant_id=tenant_id,
            incurred_usd=app_incurred,
            fallback_pct=fallback_pct,
            fallback_source=fallback_source,
            license_id=lic_uuid,
            # ⭐ EXPLICIT `None`, AND IT IS NOT A PLACEHOLDER FOR A LOOKUP.
            # `row` here is a `billing_metering_daily` aggregate, and that table
            # deliberately carries NO `project_id` column: alembic 0036 states
            # the rule as "money is attributed per project; entitlement is
            # enforced per licence", because a project column on a QUOTA key
            # splits a cap (a fail-open) rather than a bill. So an app-request
            # line genuinely has no project dimension to assert, exactly as it
            # has no provider and no BYO one — and `_matches` must therefore
            # refuse a project-pinned rule for it rather than guess a project.
            # ⛔ Do NOT "complete" this by joining a project in from CostDaily:
            # that would price request volume against a project the request
            # ledger never claimed.
            project_id=None,
            service_key=row.service_key,
            # ⭐ EXPLICIT `None`, not the default, because this is a money path and
            # the intent must be readable AT the call site. An app-request line has
            # no provider and no BYO dimension, so it must not match a byo-pinned
            # rule in either direction — and `_W_BYO = 32` is the highest
            # specificity weight, so such a rule would outrank this line's own
            # service_key. `_rule_matches` gives exactly that for `None`, since
            # `True == None` and `False == None` are both False.
            byo=None,
            request_count=row.requests,
        )
        lu["app_request_billed"] += app_line.billed_usd
        lu["platform_fee"] += app_line.platform_fee_usd
        markup_sources_applied.add(app_line.markup_source)

    # ALLOWANCE: net any included-usage credit against incurred cost BEFORE
    # markup, so paid-access "included tokens" bill at $0 instead of being marked
    # up. internal_cost (llm_cost) stays RAW below — only the billed side
    # shrinks, so margin correctly absorbs the included usage. Surcharges
    # (token/request based) still apply on the reduced incurred.
    #
    # ⭐ ALLOCATED ACROSS THE WHOLE MONTH AT ONCE, NOT LINE BY LINE. The credit
    # is spent before a PER-LINE markup rate is applied, so "which line absorbs
    # it" is "whose markup do we forgo" — a money question, and one that a
    # sequential drain answered with the row order. `allocate_allowance` splits
    # each bucket pro-rata across the lines that can draw on it; every ordering
    # gives the same answer. See its docstring for the measured $8.00-vs-$5.20.
    #
    # ⛔ THE SENTINEL FILTER MUST HAPPEN BEFORE THE ALLOCATION, not inside the
    # loop below. System/S2S cost is never billed and was never netted against a
    # customer's allowance; leaving those rows in the `lines` list would let them
    # claim a share of the credit and silently shrink what the customer's own
    # lines can absorb.
    billable_cost_rows = [c for c in cost_rows if not is_system_license(c.license_id)]
    cost_allocations = allocate_allowance(
        allowance_remaining,
        [(c.service_key, Decimal(c.cost or 0)) for c in billable_cost_rows],
    )

    for crow, (eff_incurred, applied) in zip(billable_cost_rows, cost_allocations, strict=True):
        lic_id = str(crow.license_id)
        lu = license_usage.setdefault(lic_id, _blank_license_usage())

        incurred = crow.cost or Decimal("0")
        # ⭐ READER 4: PER-LINE, AND BOTH USES IN THIS LOOP TAKE THE SAME VALUE.
        # Resolved ONCE into a local: the mode decides which ACCUMULATOR the incurred
        # cost lands in (`byo_reference_cost` vs `llm_cost`, immediately below) as well
        # as how the line is PRICED. Reading it twice would let a future edit move one
        # and not the other, and a line counted as byo-reference while priced as
        # internal reports a margin nobody can reconcile.
        line_billing_mode = _line_billing_mode(crow.license_id, crow.service_key)
        if line_billing_mode == BILLING_MODE_BYO_MARKUP_ONLY and crow.byo:
            lu["byo_reference_cost"] += incurred
        else:
            lu["llm_cost"] += incurred
        lu["by_provider"][crow.provider] = lu["by_provider"].get(crow.provider, Decimal("0")) + (
            incurred
        )
        lu["included_applied"] += applied

        # LLM cost line, billed per (license, project, app, provider, model, byo).
        #
        # ⭐ THE SENTINEL IS UNWRAPPED BACK TO `None` BEFORE IT REACHES THE
        # MATCHER — the same idiom as `lic_uuid` in the app-request loop above.
        # `billing_cost_daily.project_id` is NOT NULL and carries
        # `NULL_PROJECT_ID` when no project was asserted; that sentinel is a
        # storage device for the rollup's `ON CONFLICT` (a NULL never equals
        # itself in a plain UNIQUE), NOT a project id. `markup_rules.project_id`
        # is nullable and uses NULL as the wildcard, so handing `_matches` the
        # raw sentinel would ask it to find a literal project whose id is
        # `...0004`: the tenant-wide rule would stop matching project-less
        # spend, and an operator could write a rule that prices the sentinel.
        proj_uuid = crow.project_id if crow.project_id != NULL_PROJECT_ID else None
        llm_line = compute_billed_line_detailed(
            resolver,
            line_billing_mode,
            tenant_id=tenant_id,
            incurred_usd=eff_incurred,
            fallback_pct=fallback_pct,
            fallback_source=fallback_source,
            license_id=crow.license_id,
            project_id=proj_uuid,
            service_key=crow.service_key,
            provider=crow.provider,
            model=crow.served_model,
            byo=crow.byo,
            input_tokens=crow.inp or 0,
            output_tokens=crow.outp or 0,
        )
        billed = llm_line.billed_usd
        lu["llm_billed"] += billed
        lu["platform_fee"] += llm_line.platform_fee_usd
        markup_sources_applied.add(llm_line.markup_source)
        lu["by_provider_billed"][crow.provider] = (
            lu["by_provider_billed"].get(crow.provider, Decimal("0")) + billed
        )

    # Format response
    license_entries = []
    total_requests = 0
    total_denied = 0
    total_budget_denied = 0
    total_internal = Decimal("0")
    total_customer = Decimal("0")
    #: The LLM half of `total_customer` -- the metered usage prepaid credit covers.
    total_llm_billed = Decimal("0")
    total_included = Decimal("0")
    total_platform_fee = Decimal("0")
    total_byo_reference = Decimal("0")
    total_inp = 0
    total_outp = 0
    provider_totals: dict[str, Decimal] = {}
    provider_billed_totals: dict[str, Decimal] = {}

    for lic_id, lu in license_usage.items():
        lic_data = license_map.get(lic_id, {})
        app_key = lic_data.get("app", {}).get("key", next(iter(lu["service_keys"]), "unknown"))
        plan_code = lic_data.get("plan", {}).get("code")
        # Effective limit: per-licence override wins over the plan default, so
        # the statement reports the quota actually enforced (services.licence_limits).
        quota_monthly = effective_limit(
            lic_data.get("quota_override"),
            lic_data.get("plan", {}).get("quota_monthly"),
            field="quota_override",
            license_id=lic_id,
        )

        req_count = lu["requests"]
        # Use monthly_usage if larger (covers current incomplete day)
        if lic_id in monthly_rows and monthly_rows[lic_id] > req_count:
            req_count = monthly_rows[lic_id]

        denied_count = lu["denied"]
        budget_denied = lu["budget_denied"]
        internal_cost = lu["app_request_cost"] + lu["llm_cost"]
        billed_cost = lu["app_request_billed"] + lu["llm_billed"]
        # Effective blended markup for this license (display only).
        lic_markup_pct = (
            round(float((billed_cost / internal_cost - 1) * 100), 2) if internal_cost else 0.0
        )
        total_requests += req_count
        total_denied += denied_count
        total_budget_denied += budget_denied
        total_internal += internal_cost
        total_customer += billed_cost
        total_llm_billed += lu["llm_billed"]
        total_included += lu["included_applied"]
        total_platform_fee += lu["platform_fee"]
        total_byo_reference += lu["byo_reference_cost"]

        for prov, cost in lu["by_provider"].items():
            provider_totals[prov] = provider_totals.get(prov, Decimal("0")) + cost
        for prov, billed in lu["by_provider_billed"].items():
            provider_billed_totals[prov] = provider_billed_totals.get(prov, Decimal("0")) + billed

        # Token info
        token_info = token_rows.get(lic_id)
        budget_info = budgets.get(lic_id)
        inp = token_info.input_tokens if token_info else 0
        outp = token_info.output_tokens if token_info else 0
        total_inp += inp
        total_outp += outp

        license_entries.append(
            {
                "license_id": lic_id,
                "app_key": app_key,
                "plan_code": plan_code,
                "usage": {
                    "request_count": req_count,
                    "denied_count": denied_count,
                    "budget_denied_count": budget_denied,
                    "quota_monthly": quota_monthly,
                    # Mirrors budget_pct below: quota_monthly is None -> unlimited
                    # (no percentage); quota_monthly == 0 -> block-all, reported
                    # as fully consumed, not "0% used" behind a green bar.
                    "quota_pct": (
                        None
                        if quota_monthly is None
                        else 100.0
                        if quota_monthly == 0
                        else round(req_count / quota_monthly * 100, 1)
                    ),
                },
                "costs": {
                    "app_request_cost_usd": str(lu["app_request_cost"]),
                    "app_request_billed_usd": str(lu["app_request_billed"]),
                    "llm_cost_usd": str(lu["llm_cost"]),
                    "llm_billed_usd": str(lu["llm_billed"]),
                    # ⭐ RAW, BECAUSE ITS OWN TWO COMPONENTS ARE RAW. This is
                    # `app_request_billed + llm_billed` and both are rendered
                    # unrounded two lines up, so quantising only the total made
                    # a displayed sum that its own displayed addends do not
                    # reach.
                    #
                    # ⛔ DO NOT "FIX" THIS BY QUANTISING THE OTHER TWO INSTEAD.
                    # `services.invoices._money_from_snapshot` rebuilds
                    # `llm_usage/llm_markup/app_request_usage/app_request_markup`
                    # from exactly those two keys, and rounding them is the
                    # defect its docstring names: it would add four half-cent
                    # errors to a residual the close gate allows only one cent
                    # of. Nor is the `total_customer += billed_cost` accumulator
                    # above a candidate — it is the raw stream `B` that the
                    # gate's whole derivation (`residual == q(B) - B`) rests on,
                    # and quantising it there costs half a cent PER LICENCE.
                    "billed_cost_usd": str(billed_cost),
                    "markup_pct": lic_markup_pct,
                    "by_provider": {k: str(v) for k, v in lu["by_provider"].items()},
                    "billed_by_provider": {k: str(v) for k, v in lu["by_provider_billed"].items()},
                    "access_charge_usd": str(access_by_app.get(app_key, Decimal("0"))),
                    "included_applied_usd": str(lu["included_applied"]),
                    # Part of llm_billed_usd above, NOT additional to it. Broken
                    # out because the relative and absolute components are
                    # indistinguishable once summed.
                    "platform_fee_usd": str(lu["platform_fee"]),
                },
                "tokens": {
                    "input_tokens": inp,
                    "output_tokens": outp,
                    "budget_tokens": budget_info.budget_tokens if budget_info else None,
                    "budget_remaining": max(0, budget_info.budget_tokens - (inp + outp))
                    if budget_info
                    else None,
                    # None means "no budget of record" (no active row). A 0 cap
                    # is a different thing entirely — a block-all budget — and
                    # reports 100.0, matching budget_status.check_budget. The
                    # old single `budget_tokens > 0` guard collapsed both into
                    # None, and templates/tenant.html renders `budget_pct or 0`
                    # as "0% used" behind a GREEN bar, so a fully-blocked
                    # licence looked healthiest of all on the statement.
                    "budget_pct": (
                        None
                        if budget_info is None
                        else 100.0
                        if budget_info.budget_tokens == 0
                        else round((inp + outp) / budget_info.budget_tokens * 100, 1)
                    ),
                },
            }
        )

    # Effective markup is a USAGE-axis metric — the blended rate on incurred
    # usage. Compute it from usage billed vs incurred BEFORE folding in the fixed
    # access fee (which is not a markup and would distort the ratio).
    effective_markup_pct = (
        round(float((total_customer / total_internal - 1) * 100), 2) if total_internal else 0.0
    )

    # Customer cost is the sum of per-line billed usage plus the fixed access
    # fees (computed above); margin is what's left over internal cost.
    #
    # ⭐ THE ACCESS AND SEAT FEES ENTER THE TOTAL AS THE CENTS THEY ARE CHARGED
    # AS, NOT AS THEIR SUB-CENT PRE-IMAGES — and that is what keeps the invoice's
    # component identity provable now that billed LINES are no longer rounded.
    #
    # `totals.access_charge_usd` and `totals.seat_fee_usd` below are
    # `quantize_money(...)` of these two sums, and those are the figures
    # `services.invoices._money_from_snapshot` copies into the
    # `access_charge_usd` / `seat_fee_usd` COLUMNS — i.e. the amounts actually
    # billed. Folding the unrounded pre-images into the total instead made the
    # stored total disagree with the stored components by the two roundings it
    # had swallowed. That was tolerable only while every billed line was itself a
    # whole cent, which made the residual a cent MULTIPLE and so either 0 or
    # exactly the one cent `_TOTAL_IDENTITY_TOLERANCE_USD` allows. Unrounded
    # lines make it continuous: measured −0.0140, and `close_period` refuses the
    # statement outright on a gapless, immutable, unrecoverable path.
    #
    # With the fees pre-quantised the whole residual is `q(B) − B` for the billed
    # stream `B` — bounded by HALF a cent, strictly tighter than what the gate
    # tolerated before — and it no longer depends on the fees' scale at all.
    # ⚠️ AS OF THE LEAF FOLD ABOVE, THESE TWO QUANTIZES ARE IDEMPOTENT — and the
    # note that used to stand here ("do NOT simplify this back to `+
    # access_total + seat_fee_total`") no longer describes a hazard, so it is
    # removed rather than left to be believed. Both accumulators now sum the
    # CENTS their lines are itemised as, so both are already whole cents and
    # `q(x) == x` for each. The protection moved to the accumulators, where the
    # itemisation identity `q(total) == Σ q(line)` is enforced by construction;
    # these calls remain because THIS is the point of storage, they fix the
    # display exponent, and a future accumulator change must not silently make
    # the stored column a sub-cent figure.
    access_total_charged = quantize_money(access_total)
    seat_fee_total_charged = quantize_money(seat_fee_total)

    # ⭐ PREPAID CREDIT (alembic 0055, owner ruling 2026-10-13). Billed LLM usage
    # the tenant's prepaid credit covers is NOT invoiced again -- the top-up was
    # invoiced when bought, as the `one_time` `topup:<id>` access line above. The
    # credit applied is whole cents, so the total identity stays one rounding:
    # `total = q(B) + q(A) + q(S) - credit`, and `services.invoices` subtracts the
    # same figure from the components. A closed month applies its FROZEN drawdown
    # (see services.prepaid_credit.credit_for_month), never a recomputed one.
    credit = await prepaid_credit_svc.credit_for_month(
        session, tenant_id=tenant_id, month_start=month_start
    )
    credit_applied = credit.applied(total_llm_billed)
    customer_cost = total_customer - credit_applied + access_total_charged + seat_fee_total_charged

    # ⭐ QUANTIZE ONCE, AT THE POINT OF STORAGE, AND DERIVE THE THIRD COLUMN.
    #
    # These three figures are not three independent roundings — they are an
    # EQUATION, and an invoice on which `total − internal ≠ margin` cannot be
    # reconciled by anyone. Before this, all three were wrong in a different way:
    #   * `internal_cost_usd` was written RAW (`str(total_internal)`, e.g.
    #     "0.025000") while the other two were rounded, so the arithmetic failed
    #     at the cent by construction;
    #   * `customer_cost` and `margin` used a BARE `.quantize(Decimal("0.01"))`,
    #     i.e. the context default ROUND_HALF_EVEN, while every per-line figure
    #     they are built from was rounded ROUND_HALF_UP in services/markup.py —
    #     two rounding modes inside one statement, disagreeing on a half-cent;
    #   * `margin` was rounded INDEPENDENTLY from the unrounded difference, so
    #     even with one mode `q(c) − q(i) ≠ q(c − i)` for half a cent of inputs.
    # `quantize_money` is the single mode (see services/markup.py), and margin is
    # now the DIFFERENCE OF THE TWO STORED CENTS, which makes the equation hold
    # identically rather than usually.
    #
    # ⚠️ `internal_cost_usd` is now cents, not the raw Numeric(20,6)/(20,8) sum.
    # Sub-cent incurred cost is NOT lost — it stays visible per-licence on
    # `costs.llm_cost_usd` / `costs.app_request_cost_usd`, which are deliberately
    # left raw. What the TOTAL may no longer do is disagree with the two columns
    # it is supposed to balance against.
    internal_cost_q = quantize_money(total_internal)
    customer_cost_q = quantize_money(customer_cost)
    margin_q = customer_cost_q - internal_cost_q

    return {
        "tenant_id": tenant_id,
        "month": month,
        # Tenant default markup (legacy field). Per-line markup lives on each
        # license entry; effective_markup_pct is the blended rate actually billed.
        "markup_pct": fallback_pct,
        # ⭐ WHERE `markup_pct` CAME FROM. `markup_pct` is exactly `fallback_pct`,
        # so its provenance is exactly the branch of get_markup_pct_with_source
        # that answered. Without this, a statement reading "30%" cannot be told
        # apart from one billed off an effective-dated commercial rule — and as
        # of 2026-09-08 every open `markup_rules` row is closed, so the honest
        # answer for every tenant today is `settings_default`, a code constant.
        "markup_source": fallback_source,
        "effective_markup_pct": effective_markup_pct,
        # ⚠️ THIS IS THE TENANT-WIDE MODE AND IT IS NO LONGER THE WHOLE TRUTH. Since
        # the per-app posture landed, an allowlisted line can be priced under a
        # DIFFERENT mode (see `_line_billing_mode`), so a reader must not infer "every
        # line on this statement was priced as X" from this one field. It is kept
        # unchanged deliberately — `services.invoices` freezes this document and
        # `snapshot_sha256` covers it, so redefining an existing key would silently
        # change what every historical consumer thinks it is reading. A per-line
        # posture belongs on the LINE, as its own additive key, whenever a consumer
        # actually needs it.
        "billing_mode": billing_mode,
        "licenses": license_entries,
        "totals": {
            "total_requests": total_requests,
            "total_denied": total_denied,
            "total_budget_denied": total_budget_denied,
            # The balancing triple: customer_cost_usd − internal_cost_usd is
            # EXACTLY margin_usd, at the cent. See the derivation above.
            "internal_cost_usd": str(internal_cost_q),
            "customer_cost_usd": str(customer_cost_q),
            "margin_usd": str(margin_q),
            # BYO REFERENCE axis (billing_mode byo_markup_only): the provider cost of BYO lines the
            # customer paid directly. Disclosed so the markup can be checked against its base, and
            # deliberately OUTSIDE internal_cost_usd — see `lu["byo_reference_cost"]`.
            "byo_reference_cost_usd": str(quantize_money(total_byo_reference)),
            "cost_by_provider": {k: str(v) for k, v in provider_totals.items()},
            "billed_by_provider": {k: str(v) for k, v in provider_billed_totals.items()},
            "total_input_tokens": total_inp,
            "total_output_tokens": total_outp,
            "access_charge_usd": str(access_total_charged),
            "access_charges": access_charges,
            # SEAT axis. Disjoint from access_charge_usd above, and both are
            # components of total_usd.
            #
            # ⚠️ `seat_count` is None when NO per-seat row applies, which is a
            # different fact from 0 seats — billing_invoices.seat_count is nullable
            # precisely to keep them apart. `seat_basis` says which: None = no
            # per-seat term, "active_licences" = counted, "unavailable" = licences
            # could not be asked (report only; close_period refuses instead).
            "seat_fee_usd": str(seat_fee_total_charged),
            "seat_count": seat_count,
            "seat_basis": seat_basis,
            # The byo_platform_fee absolute surcharge, separated at last. This is
            # the figure close_period had no way to copy and therefore froze as a
            # hardcoded 0 on every invoice. It is a COMPONENT of customer_cost_usd
            # (already inside the billed lines), never an addition to it.
            "platform_fee_usd": str(quantize_money(total_platform_fee)),
            # Every provenance actually applied to a line, sorted for stable
            # digests. Empty only when the month produced no billable line.
            "markup_sources_applied": sorted(markup_sources_applied),
            "included_applied_usd": str(quantize_money(total_included)),
            # PREPAID CREDIT netted out of this statement's billed LLM usage. A
            # DEDUCTION from customer_cost_usd (services.invoices subtracts it
            # from the components). `prepaid_credit` says where it came from.
            "prepaid_credit_applied_usd": str(credit_applied),
            "prepaid_credit": {
                "currency": "USD",
                # Via the class: `available_usd` alone is NOT the same quantity for an
                # open and a closed month (see CreditForMonth.entering/remaining).
                "available_usd": str(credit.entering(credit_applied)),
                "applied_usd": str(credit_applied),
                "remaining_usd": str(credit.remaining(credit_applied)),
                "billed_llm_usd": str(quantize_money(total_llm_billed)),
                "frozen": credit.frozen_drawdown_usd is not None,
            },
        },
    }
