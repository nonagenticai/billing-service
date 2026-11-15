"""Resolve a tenant's BYO entitlement (``byo_mode``) and map it onto a billing mode.

Phase 4 billing gate (decided with George 2026-06-23): platform GPU/LLM cost is
billed only to **commercial** tenants. A tenant's ``byo_mode`` (owned by the
licenses service) is the policy source:

    byo_required                 → commercial: bill normally (markup applies)
    local_allowed | platform_only → internal/demo: bill $0 (no customer line)

The effective billing mode is computed **at billing time** (not stored in
tenant_config), so a tenant's mode follows its current licenses entitlement with
no extra migration. ``byo_mode`` is fetched from the licenses internal endpoint
``GET /tenants/{id}/byo-mode``. On any lookup failure we default to
``byo_required`` (BILL) — revenue-safe: an over-bill is reviewable on the
invoice, an under-bill is silent lost revenue.

⛔ THIS CALL SENDS A SERVICE-ACCOUNT TOKEN, AND THE NOTE THAT USED TO EXCUSE IT
IS RETIRED. Until 2026-09-13 the line above read "(open in-cluster, same posture
as introspect)" and the request carried neither ``Authorization`` nor
``X-Tenant-Id``. That was an ASSUMPTION about a route this module does not own,
inherited from a sibling service's posture, and it is the FOURTH site in this
repo to make it: ``services.seats`` (measured 401 from inside the billing pod,
2026-09-11 — every seat lookup raised, so the per-seat charge could never be
billed at all), ``services.tenant_names`` (its registry read had no header on the
automated close path, so the strict reader would have refused every tenant in the
estate while looking like a working gate), and ``services.billing.fetch_licenses``
all had to have the header added AFTER the fact, one at a time, in production.
``licenses`` is additionally being changed in a sibling PR to require auth on
``POST /license/introspect`` — the very route the "same posture as introspect"
sentence leaned on — so the assumption is about to be false as well as unverified.
The header is therefore attached UNCONDITIONALLY and fail-SOFT: if no token can
be minted the request still goes out exactly as it did before (see
:func:`_provider_for`), because turning "cannot authenticate" into a refusal here
would refuse every close against a route that answers 200 today.

⭐ AND "LICENCES SAID ``byo_required``" IS NOW DISTINGUISHABLE FROM "I COULD NOT
ASK". See :class:`ByoModeVerdict` and :func:`get_byo_mode_verdict`: the string
resolvers below are unchanged, but a caller that is about to FREEZE money can now
ask whether the value it holds is an ANSWER or a DEFAULT, instead of inferring
authority from a bare ``"byo_required"``. The laundering is also counted —
``billing_byo_mode_indeterminate_total{reason=...}`` — so an estate-wide blip is
a visible number rather than 49 plausible commercial invoices.

⚠️ THE DEFAULT IS CACHED ONLY WHEN IT IS AN ANSWER. Only a status that says
something about THE TENANT is written to the 5-min cache: ``200`` (the real
classification) and ``404`` (unknown tenant — durable). A 5xx, an unexpected
4xx, or a network error says something about the licenses SERVICE, so it returns
the default *uncached*. Caching those bills an internal/demo tenant commercially
for the whole TTL window on the strength of one blip. This is the opposite default from worker_management's access-side
lookup (which fails to ``local_allowed`` to avoid locking inference out).

⭐ AND THERE ARE TWO QUESTIONS, NOT ONE, SINCE 2026-09-17. licences carries a
PER-PRODUCT override (``GET /tenants/{id}/apps/{app}/byo-mode``) beside the
tenant-wide posture, and until now every caller here asked only the tenant-wide
one. That is an UNDER-BILL, which is the silent direction: a tenant whose
tenant-wide mode is ``local_allowed``/``platform_only`` maps to
``BILLING_MODE_INTERNAL`` and bills **$0 on every line**, including lines for a
product that is itself ``byo_required``. CommonPractice is exactly that — a
commercial, billed product living inside tenants whose tenant-wide posture is
not commercial — and 49 of 59 unarchived tenants carry an internal tenant-wide
mode. Pass ``app_key=<the event's service_key>`` to the resolvers and the
per-product posture wins, falling back to the tenant-wide one on a 404 and ONLY
on a 404; see :func:`_resolve_app_override`.

⚠️ THE CAPABILITY IS HERE; THE BILLING CALL SITE IS NOT YET PASSING IT.
``services.billing.generate_billing`` still calls ``get_byo_mode(tenant_id,
settings)`` with no ``app_key`` (that file is owned elsewhere), so the fleet
behaviour is UNCHANGED until that one call learns the line's ``service_key``.
Stated rather than implied, because "the resolver supports per-app postures" and
"invoices use per-app postures" are different claims and only the first is true
today.

⛔ THERE ARE TWO RESOLVERS AND THE ASYMMETRY IS THE POINT. See
:func:`get_byo_mode_strict`: the live gate defaults to BILL because its output is
a transient decision, while :func:`services.invoices.close_period` REFUSES
because its output is a permanent, immutable invoice row. Do not collapse them.
"""

from __future__ import annotations

import asyncio
import logging
from dataclasses import dataclass
from typing import TYPE_CHECKING

import httpx
from cachetools import TTLCache

from service_auth import ServiceTokenProvider
from services.markup import BILLING_MODE_INTERNAL

if TYPE_CHECKING:
    from settings import Settings

logger = logging.getLogger(__name__)

# tenant_id -> byo_mode, 5-min TTL (matches WM + licenses introspect caches).
#
# ⚠️ TWO KEY SHAPES SHARE THIS ONE CACHE, AND THE ASYMMETRY IS DELIBERATE. The
# TENANT-WIDE entry is keyed on the bare ``tenant_id`` STRING, exactly as it
# always was; a PER-APP entry is keyed on the ``(tenant_id, app_key)`` TUPLE.
# Keying both as tuples would have been tidier and was rejected: several existing
# tests assert directly on ``byo._byo_cache.get(tenant)`` (tests/test_byo_mode.py
# and the poisoning control in tests/test_invoices.py), and those assertions are
# the ONLY thing standing between a 5xx and a five-minute commercial
# classification of an internal tenant. Rewriting their key shape would have made
# them pass for a new reason while proving nothing about the old one.
_byo_cache: TTLCache = TTLCache(maxsize=2000, ttl=300)

#: Cached under a ``(tenant_id, app_key)`` key to mean "licences has NO per-product
#: override for this app" — a real, durable answer, not a missing one.
#:
#: ⛔ IT MUST BE CACHED, AND A BARE ``None`` CANNOT EXPRESS IT. ``TTLCache.get``
#: returns ``None`` for a miss, so "no override" and "not looked up yet" would be
#: the same value and every resolution would re-probe the per-app route. That is
#: an extra round trip to licences PER BILLED LINE — the thundering herd
#: ``_MAP_CONCURRENCY`` exists to bound, arriving through a different door. It is
#: deliberately not a valid ``byo_mode`` string, so it can never be mistaken for
#: one if it ever escapes this module.
_NO_APP_OVERRIDE = "__no_app_override__"

# byo_modes that mean "internal / not a paying customer" -> bill $0.
_INTERNAL_BYO_MODES = frozenset({"local_allowed", "platform_only"})

#: Cached default service-account token providers, keyed on the credentials they
#: were built from. ⛔ THE KEY DELIBERATELY EXCLUDES THE CLIENT SECRET — dict keys
#: end up in reprs and logs. Byte-identical in shape to
#: ``services.seats._default_providers`` and ``services.tenant_names._default_providers``
#: so a test that swaps ``Settings`` does not inherit another settings' token.
#:
#: Why a default provider at all rather than requiring every caller to inject one:
#: the consumers that need the header are ``services.invoices`` (via
#: ``assert_byo_mode_available``, which takes no provider), ``_capture_inputs`` and
#: ``services.billing.generate_billing`` — none of which has access to
#: ``app.state``, and ``close_due_periods`` calls ``close_period`` with no provider
#: at all. Threading one down would be several signature changes to deliver one
#: header. An injected provider still WINS (``token_provider=`` below), and both
#: paths authenticate as the same service account because both are built from the
#: same ``Settings``.
#:
#: ⚠️ CONSOLIDATION PENDING. This is the FOURTH local copy of this block
#: (``services.seats``, ``services.tenant_names``, ``services.billing``). George is
#: extracting all four into one helper in a later commit — do not extract it here,
#: and do not "fix" the duplication by importing a sibling's private ``_provider_for``.
_default_providers: dict[tuple[str, str], ServiceTokenProvider] = {}


def _provider_for(settings: Settings) -> ServiceTokenProvider | None:
    """The cached default provider for these credentials, or ``None``.

    ⚠️ RETURNS ``None`` RATHER THAN RAISING, and the direction is the whole point
    — copied deliberately from :func:`services.tenant_names._provider_for` and
    NOT from :func:`services.seats._provider_for`, whose caller raises
    ``SeatCountUnavailableError`` when no ``Authorization`` could be built.

    The asymmetry is not taste. ``seats`` calls an ``/internal/`` route that is
    KNOWN to require the token, so an absent credential there is a guaranteed
    401 and refusing early names billing's own misconfiguration instead of
    sending the operator to debug licences. This route answers **200
    unauthenticated today**; this function exists to ADD a header that was
    previously absent. If it could raise, then a settings object with no Keycloak
    credentials — which is every unit-test ``Settings`` in this repo, and any
    deploy whose secret has not landed yet — would turn a reachable route into
    ``ByoModeUnavailableError`` for EVERY tenant, and
    ``assert_byo_mode_available`` would refuse every close in the estate. A
    fail-closed default here would be a self-inflicted version of exactly the
    outage this module exists to survive.

    ``getattr`` rather than attribute access for the same reason
    ``tenant_names`` uses it: the suite's narrow settings fakes carry only the
    fields their subject reads, and a contract test about the WIRE HEADERS must
    not be able to fail over credentials it is not asserting.
    """
    key = (
        getattr(settings, "keycloak_token_url", "") or "",
        getattr(settings, "service_client_id", "") or "",
    )
    if not any(key):
        return None
    provider = _default_providers.get(key)
    if provider is None:
        try:
            provider = ServiceTokenProvider(settings)
        except Exception:
            logger.info("byo_mode_default_provider_unavailable", exc_info=True)
            return None
        _default_providers[key] = provider
    return provider


class ByoModeUnavailableError(RuntimeError):
    """The licences service could not state this tenant's ``byo_mode``.

    Raised only by :func:`get_byo_mode_strict`, never by :func:`get_byo_mode`.
    A caller that is FREEZING money must propagate it and refuse; a caller that
    is merely GATING or REPORTING keeps the revenue-safe default instead.

    Deliberately a plain ``RuntimeError`` and not an
    :class:`services.invoices.InvoiceError`, exactly like
    :class:`services.seats.SeatCountUnavailableError`: ``InvoiceError``'s base
    docstring is "every refusal THIS MODULE raises", and a licences service that
    cannot be reached is not that. Re-parenting would also hand every future
    ``except InvoiceError`` a transport failure it did not ask for.

    ⚠️ That is a statement about the HIERARCHY, not about the OUTCOME.
    ``close_due_periods`` counts this as ``refused``, beside ``InvoiceError``, by
    naming it at its classification site — because the remedy is a refusal's (it
    is self-healing once licences answers, and consumes no invoice number), not a
    defect's. ``failed`` exports as
    ``billing_invoice_close_failures_total{reason="tenant_error"}``, which the
    ``BillingInvoiceCloseErrored`` alert pages on to mean the module BROKE; an
    outage landing there pages on a designed refusal.

    ``reason`` carries WHICH fault class this was, drawn from
    :data:`INDETERMINATE_REASONS`. It exists so the counter and the log line can
    discriminate: "licences rejected our credential" and "licences timed out" are
    opposite remedies — fix billing's own secret, versus wait for licences — that
    an undifferentiated refusal reports identically.
    """

    def __init__(self, message: str, *, reason: str = "unavailable") -> None:
        super().__init__(message)
        #: One of :data:`INDETERMINATE_REASONS`. Defaulted rather than required so
        #: a raise site added later that forgets it degrades to the honest
        #: catch-all instead of raising ``TypeError`` inside an outage path.
        self.reason = reason


#: The fault classes a non-answer can belong to — the label values of
#: ``billing_byo_mode_indeterminate_total{reason=...}``.
#:
#: ``auth``        licences answered 401/403. OUR credential, not their outage.
#: ``timeout``     the request did not complete in time.
#: ``http_error``  any other non-200/404 status (5xx, an unexpected 4xx).
#: ``unavailable`` transport failure, or no tenant_id to ask about at all.
#:
#: ⚠️ ``auth`` IS SPLIT OUT FROM ``http_error`` ON PURPOSE, and it is the reason
#: this label exists at all. The header added to this module on 2026-09-13 is the
#: only thing standing between a 401 and 49 commercial invoices, and a 401 folded
#: into a generic bucket is indistinguishable from the 503s a licences readiness
#: blip already produces (since gitops #2558 licences readiness is wired to
#: ``/health/ready``, so a database blip drops BOTH replicas from the
#: EndpointSlice and istio answers this call with a 503). An operator watching one
#: undifferentiated number could not tell "our token stopped working" from "their
#: database hiccuped", and only the first is billing's to fix.
INDETERMINATE_REASONS: tuple[str, ...] = ("auth", "timeout", "http_error", "unavailable")


@dataclass(frozen=True)
class ByoModeVerdict:
    """A resolved ``byo_mode`` **and whether it was actually resolved**.

    ⭐ THE DEFECT THIS CLOSES IS NOT THE FALLBACK — IT IS THAT THE FALLBACK IS
    INDISTINGUISHABLE FROM AN ANSWER. For the LIVE ACCESS GATE the fallback is
    CORRECT and :func:`get_byo_mode`'s docstring argues for it deliberately. But
    that function returns the bare string ``"byo_required"`` for a tenant licences
    positively classified as commercial AND for a 401, a timeout and a 503 — and
    because ``byo_required`` -> :func:`effective_billing_mode` keeps the
    configured commercial mode, every one of those reads downstream as "this
    tenant is commercial". Measured 2026-09-11: 49 of 59 unarchived tenants carry
    ``local_allowed``/``platform_only`` and bill $0 today, so one blip could
    present as 49 plausible commercial classifications with nothing marking them
    as guesses.

    Three shapes were considered; this one was chosen:

    * **A new sentinel VALUE** (returning e.g. ``"indeterminate"`` as the mode) —
      ⛔ rejected, and not narrowly. :func:`effective_billing_mode` maps anything
      outside :data:`_INTERNAL_BYO_MODES` to the configured commercial mode, so a
      new string would still bill COMMERCIALLY through the existing mapping while
      looking like a fix, and every ``== "byo_required"`` comparison in the repo
      would silently change meaning. It also breaks the constraint that no
      tenant's ``byo_mode`` VALUE may change.
    * **Making :func:`get_byo_mode` raise** — rejected: that function already
      exists, it is :func:`get_byo_mode_strict`, and collapsing the two deletes
      the live gate's revenue-safe default, which is the half that is right.
    * **This value object, returned by a PARALLEL accessor**
      (:func:`get_byo_mode_verdict`) — chosen. The string resolvers keep
      byte-identical behaviour (``get_byo_mode`` is now literally
      ``(await get_byo_mode_verdict(...)).mode``), so no existing reader can
      regress, while a caller whose output is IMMUTABLE can ask the one question
      it needs and refuse on the answer. It is additive, so the four gates in
      ``services.invoices`` can adopt it one at a time.

    ⛔ ``determined is False`` MUST NEVER REACH A FROZEN ROW — and note that
    ``mode`` is still populated in that case, with the revenue-safe default, so a
    live-gate caller can keep reading ``.mode`` and keep failing closed to bill.
    The consequence: a caller that reads ``.mode`` and ignores ``.determined`` is
    exactly as wrong as the code this replaces. Check the flag.
    """

    #: The mode to ACT on. Always populated; equals the revenue-safe default
    #: ``"byo_required"`` whenever ``determined`` is False.
    mode: str
    #: True only when licences made a statement ABOUT THIS TENANT — a 200, or a
    #: 404 (durable information: licences does not know this tenant).
    determined: bool
    #: ``None`` when ``determined``; otherwise one of :data:`INDETERMINATE_REASONS`.
    reason: str | None = None


def _count_indeterminate(reason: str) -> None:
    """Count one laundering of an infra failure into a billing classification.

    ⚠️ IMPORTED LAZILY AND WRAPPED, and that is not defensive padding.
    ``billing_byo_mode_indeterminate_total`` is declared in
    ``src/observability/metrics.py``, which this change does not own; a top-level
    import would make ``services.byo_mode`` unimportable until that declaration
    lands, and an unimportable ``services.byo_mode`` takes the whole service down
    (``services.billing``, ``services.invoices`` and ``routers.billing`` all
    import it at module scope). The same lazy-import-inside-a-try shape is used by
    :func:`services.tenant_names.run_tenant_registry_refresh_loop`, for the same
    reason: instrumentation must never be able to break the path it measures.

    ⛔ ONLY THE DEFAULTING PATH INCREMENTS THIS. :func:`get_byo_mode_strict`
    RAISES, so nothing is laundered there; counting it too would double-count an
    outage and overstate how many guesses actually reached money — which is the
    one thing this counter is for.
    """
    try:
        from observability.metrics import byo_mode_indeterminate_total

        byo_mode_indeterminate_total.labels(reason=reason).inc()
    except Exception:  # pragma: no cover - instrumentation must not break billing
        logger.debug("byo_mode_indeterminate_counter_unavailable reason=%s", reason)


def clear_byo_cache() -> None:
    """Drop the TTL cache — for tests, and for an operator who has just fixed a
    tenant's entitlement and wants the next close to see it without waiting 5 min.

    Mirrors :func:`services.seats.clear_seat_cache`, including dropping the cached
    default token providers: a test that changes the Keycloak settings must not
    keep authenticating with a provider built from the previous ones.
    """
    _byo_cache.clear()
    _default_providers.clear()


async def _licences_get(
    url: str,
    tenant_id: str,
    settings: Settings,
    *,
    client: httpx.AsyncClient | None = None,
    token_provider: ServiceTokenProvider | None = None,
) -> httpx.Response:
    """Issue ONE authenticated GET to licences, or raise a classified non-answer.

    ⭐ EXTRACTED SO THERE IS STILL EXACTLY ONE PLACE THAT BUILDS THESE HEADERS.
    The module docstring's promise — one request shape, one header set — was true
    while there was one URL. There are now TWO (the tenant-wide posture and the
    per-product override), and the cheap way to add the second would have been to
    copy the block. That copy is the FIFTH instance of the defect the ⛔ at the
    top of this file catalogues: ``services.seats``, ``services.tenant_names``,
    ``services.billing.fetch_licenses`` and this module each had the header
    retro-fitted in production, one at a time, after it bit. A copy inside the
    module that already learned that lesson would be the worst of the five.

    Raises :class:`ByoModeUnavailableError` for transport faults only. STATUS is
    the caller's to interpret, because the two URLs read the same status
    differently — a 404 on the tenant-wide route is "unknown tenant" (durable,
    fail closed to bill) while a 404 on the per-product route is "no override for
    this app" (delegate to the tenant-wide posture). Folding either meaning in
    here would make one of them wrong.
    """
    headers: dict[str, str] = {"X-Tenant-Id": tenant_id}
    provider = token_provider if token_provider is not None else _provider_for(settings)
    if provider is not None:
        await provider.get_token()
        headers = provider.build_headers(tenant_id)

    try:
        if client is not None:
            return await client.get(url, headers=headers)
        async with httpx.AsyncClient(timeout=5.0) as c:
            return await c.get(url, headers=headers)
    except httpx.TimeoutException as exc:
        # ⚠️ CAUGHT BEFORE httpx.HTTPError, WHICH IS ITS BASE. A timeout is the one
        # transport failure an operator can act on differently from the rest (the
        # licences pod is up and slow, not gone), and it is also the shape a
        # database-blip readiness flap produces most often. Ordering this arm
        # second would make `reason="timeout"` unreachable, and an unreachable
        # label value is a metric that quietly cannot report the case it names.
        logger.warning("byo_mode_lookup_timeout tenant_id=%s url=%s error=%s", tenant_id, url, exc)
        raise ByoModeUnavailableError(f"licences timed out: {exc}", reason="timeout") from exc
    except httpx.HTTPError as exc:
        # Transient licenses outage. NOT cached, so a later real answer isn't
        # shadowed for the TTL window.
        logger.warning("byo_mode_lookup_failed tenant_id=%s url=%s error=%s", tenant_id, url, exc)
        raise ByoModeUnavailableError(f"licences unreachable: {exc}", reason="unavailable") from exc


def _status_reason(status_code: int) -> str:
    """The fault class of a status that is NOT an answer about the tenant.

    ⚠️ 401/403 IS LABELLED ``auth``, NOT ``http_error``, and it is the status this
    module's own missing header would have produced. Keeping it separate is what
    lets "billing's credential is wrong" be told apart from "licences is unwell"
    on one graph — see :data:`INDETERMINATE_REASONS`.
    """
    return "auth" if status_code in (401, 403) else "http_error"


async def _resolve_app_override(
    tenant_id: str,
    app_key: str,
    settings: Settings,
    *,
    client: httpx.AsyncClient | None = None,
    token_provider: ServiceTokenProvider | None = None,
) -> str | None:
    """This tenant's PER-PRODUCT ``byo_mode`` for ``app_key``, or ``None`` if unset.

    ⛔ THE DEFECT THIS CLOSES IS AN UNDER-BILL, WHICH IS THE SILENT DIRECTION.
    Until now every caller asked licences only for the TENANT-WIDE posture, but
    licences also carries a per-product override. A tenant whose tenant-wide mode
    is ``local_allowed`` or ``platform_only`` maps through
    :func:`effective_billing_mode` to ``BILLING_MODE_INTERNAL`` and bills **$0 on
    every line** — including lines for a product that is itself ``byo_required``.
    CommonPractice is exactly that shape: a commercial, billed product inside
    tenants whose tenant-wide posture is not commercial. Measured 2026-09-11, 49
    of 59 unarchived tenants carry an internal tenant-wide mode, so the population
    this silently zeroes is most of the estate. Nothing on an invoice reading
    $0.00 says a term was missed.

    ⭐ A 404 MEANS "NO OVERRIDE", NOT "UNKNOWN TENANT", and conflating the two is
    the one mistake that would make this change worse than the bug. On the
    tenant-wide route a 404 is durable information about the TENANT and fails
    CLOSED to ``byo_required`` (bill). On this route a 404 is the NORMAL answer
    for every product a tenant has not overridden — treating it as
    ``byo_required`` would bill every internal tenant commercially for every
    product, i.e. exactly the over-bill the tenant-wide default is carefully
    scoped to avoid. So it returns ``None`` and the caller falls back.

    ⛔ AND A 5xx MUST NOT FALL BACK. It raises, like every other non-answer in
    this module. Falling back would turn "CommonPractice is byo_required but
    licences is unwell" into "the tenant-wide posture says local_allowed, so bill
    $0" — a silent under-bill produced by an infra blip, laundered through a
    perfectly ordinary-looking tenant-wide answer, with no counter and no log line
    saying a guess was made. The whole module exists to stop that shape.

    The ``(tenant_id, app_key)`` cache entry stores either the override or
    :data:`_NO_APP_OVERRIDE`; both are answers, so both are cached. Nothing else
    is.
    """
    key = (tenant_id, app_key)
    cached = _byo_cache.get(key)
    if cached is not None:
        return None if cached == _NO_APP_OVERRIDE else cached

    url = f"{settings.licenses_url}/tenants/{tenant_id}/apps/{app_key}/byo-mode"
    resp = await _licences_get(
        url, tenant_id, settings, client=client, token_provider=token_provider
    )

    if resp.status_code == 200:
        mode = str(resp.json().get("byo_mode") or "") or None
        # ⚠️ A 200 whose body carries no `byo_mode` is NOT an override. licences
        # returning `{}` or `{"byo_mode": null}` for a product with no posture is
        # an answer meaning "nothing set here", and coercing it to the
        # `"byo_required"` default (which the tenant-wide parse does, correctly,
        # for its own route) would bill every such product commercially on the
        # strength of an empty object.
        _byo_cache[key] = mode or _NO_APP_OVERRIDE
        return mode
    if resp.status_code == 404:
        _byo_cache[key] = _NO_APP_OVERRIDE
        return None

    reason = _status_reason(resp.status_code)
    logger.warning(
        "byo_mode_app_lookup_unavailable tenant_id=%s app_key=%s status=%s reason=%s",
        tenant_id,
        app_key,
        resp.status_code,
        reason,
    )
    raise ByoModeUnavailableError(
        f"licences returned {resp.status_code} for app {app_key}", reason=reason
    )


async def _resolve_byo_mode(
    tenant_id: str,
    settings: Settings,
    *,
    app_key: str | None = None,
    client: httpx.AsyncClient | None = None,
    token_provider: ServiceTokenProvider | None = None,
) -> str:
    """THE ONLY PLACE THAT TALKS TO LICENCES about ``byo_mode``.

    Returns the tenant's mode for a status that answers ABOUT THE TENANT — 200
    (the real classification) and 404 (unknown tenant) — and caches it for the
    TTL. Raises :class:`ByoModeUnavailableError` for everything that is a
    statement about the licences SERVICE instead: any network error, and any
    other status.

    ⭐ ``app_key`` SELECTS THE PER-PRODUCT POSTURE FIRST, falling back to the
    tenant-wide one when licences has no override for that product. See
    :func:`_resolve_app_override` for why the fallback is a 404-only path and why
    a 5xx on the per-app route must NOT fall back. Omit ``app_key`` and the
    behaviour, the URL, the cache key and every status rule are byte-identical to
    what they were — which is the claim
    ``tests/test_byo_mode.py`` continues to pin unchanged.

    ⚠️ IT DOES NOT APPLY A DEFAULT. Choosing what to do with a non-answer is the
    CALLER's decision, because the two callers make opposite ones, and folding
    either default in here is what made them indistinguishable before. All THREE
    public resolvers below (:func:`get_byo_mode`, :func:`get_byo_mode_strict`,
    :func:`get_byo_mode_verdict`) are thin wrappers over this function so there is
    exactly one request, one set of headers, one parse and one cache-write rule to
    reason about.
    """
    if app_key:
        override = await _resolve_app_override(
            tenant_id, app_key, settings, client=client, token_provider=token_provider
        )
        if override is not None:
            return override
        # No override for this product → fall through to the tenant-wide posture
        # below, which owns its own cache entry under the bare `tenant_id` key.
        # Deliberately NOT cached a second time under the per-app key: one entry
        # per fact means a newly-created override takes effect within one TTL
        # instead of needing two independent entries to expire.

    cached = _byo_cache.get(tenant_id)
    if cached is not None:
        return cached

    url = f"{settings.licenses_url}/tenants/{tenant_id}/byo-mode"

    # ⭐ THE HEADERS THIS CALL SPENT ITS WHOLE LIFE WITHOUT. It sent neither
    # `Authorization` nor `X-Tenant-Id`; the docstring above excused that with
    # "open in-cluster, same posture as introspect", which was an ASSUMPTION about
    # a route owned by another service and is being invalidated by the sibling PR
    # that puts auth on `POST /license/introspect`. Three siblings had to have the
    # same header retro-fitted after it bit in production — `services.seats`
    # (measured 401, so the per-seat charge could never be billed at all),
    # `services.tenant_names` (unauthenticated on the automated close path) and
    # `services.billing.fetch_licenses`. This was the fourth and last.
    #
    # ⚠️ FAIL-SOFT, NOT FAIL-CLOSED, and `_provider_for`'s docstring is the
    # argument: the route answers 200 unauthenticated TODAY, so a missing
    # credential must leave the request exactly as it was rather than convert a
    # reachable route into a refusal of every close. When licences does start
    # gating it, the missing-credential case arrives as a 401 -> `reason="auth"`,
    # which is a named, counted, non-cached non-answer rather than a silent
    # commercial classification.
    #
    # `get_token()` is awaited BEFORE `build_headers`, because `build_headers` only
    # attaches `Authorization` from the provider's ALREADY-CACHED token and never
    # fetches one itself (see `service_auth.py:60-65`) — the same ordering
    # `services.seats.get_seat_count` uses.
    #
    # ⚠️ CONSOLIDATION PENDING: George is extracting this block plus the
    # `_default_providers`/`_provider_for` pair above into ONE shared helper across
    # all four call sites in a later commit. Kept local here deliberately — do not
    # extract it, and do not import a sibling's private `_provider_for`.
    #
    # ⭐ MOVED INTO `_licences_get` 2026-09-17, WITHIN THIS MODULE ONLY. That is
    # not the cross-module consolidation the note above defers: it is the
    # single-module rule "one place builds these headers" surviving the arrival of
    # a SECOND URL. Copying the block for the per-product route would have made
    # this module the fifth site to grow an unauthenticated licences call.
    resp = await _licences_get(
        url, tenant_id, settings, client=client, token_provider=token_provider
    )

    if resp.status_code == 200:
        mode = str(resp.json().get("byo_mode") or "byo_required")
    elif resp.status_code == 404:
        # Unknown tenant. This IS a real answer about this tenant, and a
        # durable one, so it fails CLOSED to bill (matching licenses
        # introspect) and is cached like any other classification.
        mode = "byo_required"
    else:
        # ⚠️ ANY OTHER STATUS IS NOT AN ANSWER ABOUT THIS TENANT — it is a
        # statement about the licenses service. httpx does NOT raise on
        # status, so before this branch existed a 500/502/503 fell through
        # to ``mode = "byo_required"`` and was CACHED for the full 5-minute
        # TTL, exactly the poisoning the httpx.HTTPError arm above exists to
        # prevent, arriving through the branch that did the opposite.
        #
        # What that cost: an internal/demo tenant (local_allowed /
        # platform_only -> BILLING_MODE_INTERNAL -> bills $0) was billed
        # COMMERCIALLY — markup plus access charges — for five minutes per
        # blip. And it is more reachable than it looks: since gitops #2558
        # licences readiness is wired to /health/ready, so a database blip
        # can take BOTH licences replicas out of the EndpointSlice and istio
        # then answers this call with a 503.
        #
        # Treated as the same fault class as a network error, and handled the
        # same way by both callers: never cached.
        #
        # ⚠️ 401/403 IS LABELLED `auth`, NOT `http_error`, and it is the status
        # this module's own missing header would have produced. Keeping it
        # separate is what lets "billing's credential is wrong" be told apart from
        # "licences is unwell" on one graph — see INDETERMINATE_REASONS. The
        # classification lives in `_status_reason` so the per-product route below
        # cannot label the same status differently from this one.
        reason = _status_reason(resp.status_code)
        logger.warning(
            "byo_mode_lookup_unavailable tenant_id=%s status=%s reason=%s",
            tenant_id,
            resp.status_code,
            reason,
        )
        raise ByoModeUnavailableError(f"licences returned {resp.status_code}", reason=reason)

    # Only reached for a status that answered ABOUT THE TENANT (200 or 404).
    # Every non-answer raised above, uncached.
    _byo_cache[tenant_id] = mode
    return mode


async def get_byo_mode_verdict(
    tenant_id: str,
    settings: Settings,
    *,
    app_key: str | None = None,
    client: httpx.AsyncClient | None = None,
    token_provider: ServiceTokenProvider | None = None,
) -> ByoModeVerdict:
    """Resolve ``byo_mode`` AND say whether the value is an answer or a default.

    ⭐ THE THIRD ACCESSOR, AND WHY THE OTHER TWO DID NOT COVER THIS. There were
    two: :func:`get_byo_mode` returns a string and silently defaults,
    :func:`get_byo_mode_strict` raises. That is the right pair for a caller that is
    EITHER purely transient OR purely permanent, and the pair is unchanged. It is
    the wrong pair for a caller that must PROCEED on the transient answer while
    recording, provably, that it was a guess — which is every caller that computes
    a figure now and freezes it a few lines later. The two such callers in this
    repo are ``services.invoices._capture_inputs`` (whose ``inputs["byo_mode"]``
    lands in an immutable JSONB document covered by ``snapshot_sha256``) and
    ``services.billing.generate_billing`` (whose answer decides
    ``effective_billing_mode`` for the figures that document freezes). Both
    currently call :func:`get_byo_mode` and neither can tell a 503 from a
    classification.

    ⚠️ IT IS NOT A REPLACEMENT FOR :func:`get_byo_mode_strict` AT THE GATE. The
    gate's job is to REFUSE before any work happens, and an exception is the right
    shape for that; this is for the sites that have already passed the gate and
    are about to write the value down. ``close_period``'s existing ordering —
    ``assert_byo_mode_available`` first, priming the 5-minute cache — means the
    common case here is a cache hit with ``determined=True``, and this verdict is
    what turns the remaining TOCTOU window from invisible into assertable.

    ``determined`` is True for exactly the two statuses :func:`_resolve_byo_mode`
    caches, because they are the same set: a 200 and a 404 both say something about
    THE TENANT. Everything else says something about the licences SERVICE and is
    counted as ``billing_byo_mode_indeterminate_total{reason=...}``.

    ⛔ An empty ``tenant_id`` is ``determined=False`` too, not a quiet default. It
    is not an infra fault, but it is equally not an answer, and an invoice must not
    be built on "we had nobody to ask about". The returned ``mode`` is still
    ``"byo_required"``, so :func:`get_byo_mode`'s long-standing behaviour for an
    empty id — pinned by
    ``test_strict_refuses_an_empty_tenant_id_where_the_gate_defaults`` — is
    unchanged.

    ⭐ ``app_key`` ASKS THE PER-PRODUCT QUESTION, falling back to the tenant-wide
    posture when licences has no override for that product. ``determined`` keeps
    its exact meaning: True when licences made a statement — whether that
    statement was the product's override, or "no override for this product" plus
    a tenant-wide classification. It is False only when a lookup on EITHER route
    was a non-answer, and a per-product 5xx is a non-answer rather than a licence
    to read the tenant-wide value (see :func:`_resolve_app_override`).
    """
    if not tenant_id:
        _count_indeterminate("unavailable")
        logger.warning("byo_mode_lookup_no_tenant_id (defaulting byo_required)")
        return ByoModeVerdict(mode="byo_required", determined=False, reason="unavailable")
    try:
        mode = await _resolve_byo_mode(
            tenant_id, settings, app_key=app_key, client=client, token_provider=token_provider
        )
    except ByoModeUnavailableError as exc:
        # The fault itself is already logged by _resolve_byo_mode with the status
        # or the transport error; what is recorded here is the DECISION.
        reason = getattr(exc, "reason", "unavailable") or "unavailable"
        _count_indeterminate(reason)
        logger.warning(
            "byo_mode_lookup_defaulted tenant_id=%s reason=%s "
            "(defaulting byo_required, NOT caching, NOT determined)",
            tenant_id,
            reason,
        )
        return ByoModeVerdict(mode="byo_required", determined=False, reason=reason)
    return ByoModeVerdict(mode=mode, determined=True, reason=None)


async def get_byo_mode(
    tenant_id: str,
    settings: Settings,
    *,
    app_key: str | None = None,
    client: httpx.AsyncClient | None = None,
    token_provider: ServiceTokenProvider | None = None,
) -> str:
    """Resolve a tenant's ``byo_mode`` via the licenses service (cached 5 min).

    Defaults to ``"byo_required"`` (bill) on a missing row or any error —
    revenue-safe for the billing gate. ⛔ Do NOT use this at invoice close; see
    :func:`get_byo_mode_strict`.

    ⭐ PASS ``app_key`` WHEN THE LINE BEING PRICED BELONGS TO A PRODUCT. Without
    it this answers the TENANT-WIDE question only, and a billed product inside a
    tenant whose tenant-wide posture is ``local_allowed``/``platform_only`` maps
    to ``BILLING_MODE_INTERNAL`` and bills $0 — silently, on an invoice line that
    looks perfectly ordinary. ``app_key`` is the event's ``service_key``.

    ⚠️ ITS BEHAVIOUR IS DELIBERATELY UNCHANGED, and it is now a one-line
    projection of :func:`get_byo_mode_verdict` so that there is exactly one place
    the default is chosen and one place the laundering is counted. A caller that
    cannot tell whether the string it is holding is an answer should be calling
    the verdict accessor instead — this one cannot tell it, by construction.
    """
    verdict = await get_byo_mode_verdict(
        tenant_id, settings, app_key=app_key, client=client, token_provider=token_provider
    )
    return verdict.mode


async def get_byo_mode_strict(
    tenant_id: str,
    settings: Settings,
    *,
    app_key: str | None = None,
    client: httpx.AsyncClient | None = None,
    token_provider: ServiceTokenProvider | None = None,
) -> str:
    """Resolve ``byo_mode`` with NO default — for callers that are FREEZING money.

    :raises ByoModeUnavailableError: on any network error and on any non-200/404
        status. Never returns a guess. 200 and 404 are treated exactly as
        :func:`get_byo_mode` treats them, cache write included, because a 404 is
        a real and durable answer about that tenant.

    ⭐ THE ASYMMETRY WITH :func:`get_byo_mode` IS DELIBERATE AND BOTH HALVES ARE
    CORRECT, because the two callers produce different kinds of output:

    * The LIVE GATE (``services.billing.generate_billing``, the access-side
      checks) defaults to ``byo_required`` = BILL. Its output is a TRANSIENT
      decision: guessing "bill" for the five minutes of a blip is reviewable on
      the invoice, and guessing "internal" would let someone spend platform GPU
      budget for free with nothing recording it.
    * The CLOSE (``services.invoices.close_period``) refuses. Its output is a
      PERMANENT row on a gapless, irreversible invoice series. ``byo_required``
      there does not mean "bill for five minutes", it means mint a commercial
      statement — markup plus a flat access charge — for a tenant that bills
      $0. Measured 2026-09-11: of 59 unarchived tenants, **49** carry
      ``local_allowed``/``platform_only`` and therefore bill $0 today, so a
      single licences blip during a close could commercially invoice up to 49
      internal cost centres, and nothing downstream repairs a frozen invoice.

    The precedent is the seats contract, which is the same argument about the
    same service: see :mod:`services.seats` — ``get_seat_count`` raises
    :class:`~services.seats.SeatCountUnavailableError` rather than returning a
    fabricated 0, ``generate_billing`` degrades to
    ``seat_basis="unavailable"``, and ``close_period`` refuses. A refusal is
    loud, costs nothing, and can be retried the moment licences is back.
    """
    if not tenant_id:
        raise ByoModeUnavailableError("no tenant_id", reason="unavailable")
    return await _resolve_byo_mode(
        tenant_id, settings, app_key=app_key, client=client, token_provider=token_provider
    )


#: How many licences lookups :func:`get_byo_mode_verdict_map` may have in flight.
#:
#: ⚠️ THE N+1 HERE WAS NEVER *SEQUENTIAL*, AND THE FIX IS THE OPPOSITE OF THE ONE
#: THAT DESCRIPTION IMPLIES. ``asyncio.gather`` over a comprehension starts every
#: coroutine at once, and ``httpx.AsyncClient``'s default pool allows 100
#: connections, so the previous code opened ~59 SIMULTANEOUS requests to licences
#: on every aggregate render — not 59 round trips back to back. Latency was
#: therefore already ~1 round trip; the actual hazard is a self-inflicted
#: thundering herd against the service whose 503s this module has to launder,
#: arriving from every replica that renders a dashboard, and it gets worse exactly
#: as the estate grows (76 tenants in the registry today, 59 unarchived).
#:
#: ⛔ NO BULK ENDPOINT WAS INVENTED. Whether licences exposes one is not knowable
#: from this repo, and adding a speculative ``GET /tenants/byo-mode`` call would
#: be a 404 laundered into 59 commercial classifications by the very fallback this
#: change exists to make visible.
#:
#: 8, not 1 and not 59: at 8 a 59-tenant render is 8 waves rather than 1, which
#: against licences' own 5s timeout is bounded at the same order of magnitude as
#: before, while the per-tenant TTL cache means the steady state is far fewer
#: requests than tenants anyway. Round numbers are a choice here, not a
#: measurement — if an aggregate render is ever measured as slow, raise this and
#: say what you measured.
_MAP_CONCURRENCY = 8


async def get_byo_mode_verdict_map(
    tenant_ids: list[str],
    settings: Settings,
    *,
    token_provider: ServiceTokenProvider | None = None,
) -> dict[str, ByoModeVerdict]:
    """Batch resolution that keeps the answer/guess distinction per tenant.

    ⭐ WHY THE MAP NEEDS A VERDICT FORM AT ALL, even though its two callers
    (``routers.billing``'s overview and ``services.admin_aggregate``) are REPORTS
    and are allowed to degrade. An estate-wide licences failure is precisely the
    case where a report is most misleading: every tenant flips to the configured
    commercial mode at once, so the aggregate page shows an estate that has
    apparently all become commercial, and there is no per-row marker to say
    otherwise. A caller that wants to render "mode unknown" now can; one that does
    not care keeps calling :func:`get_byo_mode_map` and is unaffected.

    One client for the whole batch, as before — and it is also what makes the
    semaphore meaningful: a per-call client would give each request its own
    connection pool and the cap would bound nothing but coroutine count.
    """
    uniq = sorted({t for t in tenant_ids if t})
    if not uniq:
        return {}

    sem = asyncio.Semaphore(_MAP_CONCURRENCY)

    async def _one(tenant_id: str, client: httpx.AsyncClient) -> ByoModeVerdict:
        # The client is a PARAMETER, not a closure over the `async with` target
        # below: a free variable would still work (it is bound before the first
        # call) but it would make the lifetime of the pool unreadable at the point
        # of use, and this helper must never outlive the context manager.
        async with sem:
            return await get_byo_mode_verdict(
                tenant_id, settings, client=client, token_provider=token_provider
            )

    async with httpx.AsyncClient(timeout=5.0) as http_client:
        verdicts = await asyncio.gather(*(_one(t, http_client) for t in uniq))
    return dict(zip(uniq, verdicts, strict=True))


async def get_byo_mode_map(
    tenant_ids: list[str],
    settings: Settings,
    *,
    token_provider: ServiceTokenProvider | None = None,
) -> dict[str, str]:
    """Batch ``byo_mode`` resolution for the aggregate billing path.

    Unchanged contract — ``{tenant_id: mode}``, defaulting per tenant — and now a
    projection of :func:`get_byo_mode_verdict_map` so the fan-out bound and the
    laundering counter cannot apply to one batch path and not the other.
    """
    verdicts = await get_byo_mode_verdict_map(tenant_ids, settings, token_provider=token_provider)
    return {tenant_id: v.mode for tenant_id, v in verdicts.items()}


def effective_billing_mode(configured_mode: str, byo_mode: str) -> str:
    """Map (configured billing_mode, byo_mode) -> the mode used for this invoice.

    Internal/demo tenants (``local_allowed`` / ``platform_only``) are forced to
    ``internal`` (bill $0). Commercial tenants (``byo_required``) keep their
    configured mode (default ``standard_markup``).
    """
    if byo_mode in _INTERNAL_BYO_MODES:
        return BILLING_MODE_INTERNAL
    return configured_mode


__all__ = [
    "INDETERMINATE_REASONS",
    "ByoModeUnavailableError",
    "ByoModeVerdict",
    "clear_byo_cache",
    "effective_billing_mode",
    "get_byo_mode",
    "get_byo_mode_map",
    "get_byo_mode_strict",
    "get_byo_mode_verdict",
    "get_byo_mode_verdict_map",
]
