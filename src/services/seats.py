"""Seat counts — the denominator of the per-seat access charge.

ONE SEAT = ONE PERSON holding an active licence: licences' ``human_subjects``
(George chose the person basis 2026-09-11; until #265 only ``seat:<app>`` terms
read it and every other per-seat term billed ``seats``, the licence-row count,
which also counts ``service`` robots and CommonPractice's ``cp-tenant`` tenant
licences). The count comes from the licenses service, which owns both halves of
the predicate, and billing does not reassemble it here. That division is
deliberate and it is expensive to get wrong:

⛔ COUNTING `GET /internal/licenses` IN BILLING WOULD FABRICATE REVENUE.
Measured in the licenses DB 2026-09-10: **164** licences carry
``status='active'`` — 107 on `internal` tenants, 34 on `service`, 15 on `test`,
and only **8** on the five active ``kind='customer'`` tenants. Tenant ``kind``
and ``archived_at`` are not on that endpoint at all; they live on
``GET /internal/tenants``. So a naive per-tenant count over the licence list
bills **156 phantom seats — about $3,900/month of invented revenue** at $25. The
predicate belongs next to the columns that define it, which is why this module
calls a counted endpoint instead of doing arithmetic on a list.

⛔ BUT THE ENDPOINT DOES NOT FILTER ON ``kind``, AND THIS MODULE MUST NOT ASSUME
IT DOES. Measured live 2026-09-10 right after deploy: ``n8n-dev``
(``kind='internal'``, 23 active licences) returns ``seats: 23``, not 0. It
DISCLOSES ``tenant_kind`` instead — deliberately, because a 0 for a non-customer
is a fabricated zero indistinguishable from a customer with no seats.

So what actually stops an internal tenant being billed 23 seats is TWO gates on
THIS side, and both must stay:
  1. a per-seat charge only exists for a tenant an operator materialised
     ``tenant_access_charge`` terms for (0 such rows at the time of writing); and
  2. ``services.billing.generate_billing`` skips the ENTIRE access-charge block
     when ``effective_billing_mode`` is ``internal``.

⚠️ GATE 2 KEYS ON ``byo_mode``, NOT ON ``kind``, AND THE TWO DISAGREE. Measured
2026-09-10 across the five active ``kind='customer'`` tenants: ``cdmsfirst``,
``better-results-group`` and ``subhankar-kag`` are ``byo_required`` (bill), while
``ahtapol`` is ``local_allowed`` and ``book-fulfillment`` is ``platform_only`` —
both resolve to ``internal`` and therefore bill **$0**, customers or not. Never
read ``kind='customer'`` as "will be billed", and never read a seat count as
revenue without resolving the mode first.

⚠️ THIS MODULE FAILS CLOSED, AND THAT IS THE OPPOSITE OF `services.byo_mode`.
`byo_mode` defaults to "bill" on a lookup failure because an over-bill is visible
on an invoice while an under-bill is silent. A seat count has no such
revenue-safe default in EITHER direction: a stale high count over-bills, and a
zero under-bills. So there is no default at all — a failure raises
:class:`SeatCountUnavailableError`, and `services.invoices.close_period` treats that
as a precondition failure and REFUSES TO CLOSE, exactly like
``assert_no_bucket_gap``.

The alternative was considered and rejected: returning 0 seats on error would
write a $0.00 seat line onto a frozen invoice, and a fabricated zero is
indistinguishable from a tenant that genuinely has no seats. This repo has
already been bitten by precisely that shape — see ``services.reprice``, whose
backfill keys on ``cost_usd IS NULL`` and therefore could never repair a
fabricated zero. A refusal is loud, recoverable, and cannot be invoiced.

The report path (`generate_billing`) is NOT the close path and must stay
available during a licences outage: it reports ``seat_basis="unavailable"`` with
a null seat fee and a warning, never a silent 0.
"""

from __future__ import annotations

from datetime import UTC, date, datetime, time
from typing import Any

import httpx
import structlog
from cachetools import TTLCache

from service_auth import ServiceTokenProvider
from settings import Settings

logger = structlog.stdlib.get_logger(__name__)

# tenant_id -> seat count. 5-min TTL, matching services.byo_mode and the
# licenses introspect cache. ⚠️ ONLY REAL ANSWERS ARE CACHED (see get_seat_count):
# caching a failure would make one blip decide a whole invoice window.
_seat_cache: TTLCache = TTLCache(maxsize=2000, ttl=300)

# What `basis` values mean on the invoice snapshot and the terms read-back.
#: A per-seat term NOT pinned to a product: the people (``human_subjects``) holding an active
#: licence for any app at the billed instant. ⚠️ The value keeps its historical spelling because
#: frozen invoice snapshots carry it; it names licences' basis, not the counted unit.
SEAT_BASIS_ACTIVE_LICENCES = "active_licences"
SEAT_BASIS_UNAVAILABLE = "unavailable"
#: A per-seat term pinned to a ``service_key``: that app's seats held at ANY instant in the billed
#: period (licences ``basis='active_during_period'``). A mid-period joiner and leaver each count.
SEAT_BASIS_ACTIVE_DURING_PERIOD = "active_during_period"


#: A per-seat term whose ``service_key`` starts with this prefix is APP-SCOPED: ``seat:commonpractice``
#: bills the ``commonpractice`` app's seats over the period.
#:
#: ⛔ EXPLICIT OPT-IN, NOT INFERRED FROM ANY service_key. Per-seat terms already exist on keys with
#: TENANT-WIDE meaning (the reserved ``__seat__``, and operator-chosen keys such as the
#: ``__seat_two__`` fixtures), so treating every non-null key as an app would silently re-scope them.
#: No existing term can carry this prefix, so every existing term bills exactly as before.
SCOPED_SEAT_KEY_PREFIX = "seat:"


def seat_scope_for_charge(
    service_key: str | None, month_start: date, month_end: date
) -> dict[str, Any]:
    """The seat-count scope a per-seat term implies — ``{}`` unless it opted in.

    ⭐ ``seat:<app_key>`` bills THAT product's seats held at any instant in
    ``[month_start, month_end)`` (a mid-period joiner and leaver each count). ⚠️ If ``<app_key>`` is
    not a licences app key, licences answers 422 and the close REFUSES — loud, never a silent zero.
    """
    if not service_key or not service_key.startswith(SCOPED_SEAT_KEY_PREFIX):
        return {}
    app_key = service_key[len(SCOPED_SEAT_KEY_PREFIX) :]
    if not app_key:
        return {}
    return {
        "app_key": app_key,
        "period_start": datetime.combine(month_start, time.min, tzinfo=UTC),
        "period_end": datetime.combine(month_end, time.min, tzinfo=UTC),
    }


class SeatCountUnavailableError(RuntimeError):
    """The licences service could not state this tenant's seat count.

    Raised rather than defaulted. A caller that is FREEZING money (close_period)
    must propagate this and refuse; a caller that is merely REPORTING may catch
    it and present ``seat_basis="unavailable"`` with no seat fee.
    """


#: Service-account token providers, keyed on (token endpoint, client id) so a test
#: that swaps settings does not inherit another settings' cached token. ⛔ The key
#: deliberately EXCLUDES the client secret: dict keys end up in reprs and logs.
#:
#: Why a default at all, rather than requiring every caller to inject one: the four
#: call sites live in two routers and two service modules, and the two service ones
#: (`services.invoices.assert_seat_count_available`, `services.billing`) have no
#: access to `app.state`. Threading a provider down through `close_period` and
#: `generate_billing` would have been four signature changes to deliver one header.
#: An injected provider still wins when given — `token_provider=` below — and both
#: paths use the SAME credentials, because both are built from the same `Settings`.
_default_providers: dict[tuple[str, str], ServiceTokenProvider] = {}


def _provider_for(settings: Settings) -> ServiceTokenProvider:
    key = (settings.keycloak_token_url or "", settings.service_client_id or "")
    provider = _default_providers.get(key)
    if provider is None:
        provider = ServiceTokenProvider(settings)
        _default_providers[key] = provider
    return provider


def clear_seat_cache() -> None:
    """Drop the TTL cache — for tests, and for an operator who has just fixed
    licence data and wants the next invoice run to see it without waiting 5 min.

    Also drops the cached service-account token providers, so a test that changes
    the Keycloak settings does not keep authenticating with the previous one.
    """
    _seat_cache.clear()
    _default_providers.clear()


async def get_seat_count(
    tenant_id: str,
    settings: Settings,
    *,
    client: httpx.AsyncClient | None = None,
    token_provider: ServiceTokenProvider | None = None,
    as_of: datetime | None = None,
    app_key: str | None = None,
    period_start: datetime | None = None,
    period_end: datetime | None = None,
) -> int:
    """Active-licence seat count for ``tenant_id``.

    :raises SeatCountUnavailableError: on any network error, any non-200/404 status,
        or a response whose shape does not carry an integer count. NEVER returns
        a guess.

    A **404 returns 0** and is cached: that is a real, durable answer about this
    tenant (licences does not know it), and a tenant licences has never heard of
    has no seats to bill. Every other non-200 is a statement about the licences
    SERVICE, not about the tenant, so it raises and is NOT cached.
    """
    if not tenant_id:
        raise SeatCountUnavailableError("no tenant_id")

    # ⚠️ `as_of` IS PART OF THE CACHE KEY, and leaving it out would be worse than
    # not caching at all: "seats as of 2026-08-31" and "seats now" are different
    # questions, so a shared key lets a live read serve a closing period a current
    # count, or a period read poison every subsequent live one.
    # ⭐ SCOPED COUNT (a per-seat term pinned to a service_key, e.g. CommonPractice): one app's
    # seats, held at ANY instant in [period_start, period_end) — a mid-period leaver is a seat.
    # Every scope input is part of the cache key for the same reason `as_of` is.
    if (period_start is None) != (period_end is None):
        raise SeatCountUnavailableError("period_start and period_end must be given together")
    cache_key = (
        tenant_id,
        as_of.isoformat() if as_of is not None else None,
        app_key,
        period_start.isoformat() if period_start is not None else None,
        period_end.isoformat() if period_end is not None else None,
    )
    cached = _seat_cache.get(cache_key)
    if cached is not None:
        return int(cached)

    url = f"{settings.licenses_url}/internal/tenants/{tenant_id}/seat-count"
    # ⭐ `/internal/` ROUTES REQUIRE A SERVICE-ACCOUNT TOKEN, AND THIS CALL SENT NONE.
    # Measured 2026-09-11 from inside the billing pod:
    #   GET {licenses}/internal/tenants/<uuid>/seat-count
    #   -> 401 {"error":{"message":"Missing authentication token"}}
    # licenses gates it with `require_internal_service`, whose
    # `INTERNAL_SERVICE_ROLES = {"service-internal", "onboarding-internal",
    # "license-admin"}` (licenses `auth/dependencies.py`), and billing's own SA token
    # carries `service-internal` — so the credential was always right and simply was
    # never attached. Every seat lookup therefore raised
    # `SeatCountUnavailableError`, which means the per-seat access charge could never
    # be billed at all, and (once any tenant had a `per_seat` term)
    # `assert_seat_count_available` refused every close.
    #
    # ⚠️ Why `services.byo_mode` was unaffected and looked like a counter-example:
    # it calls `/tenants/{id}/byo-mode`, OUTSIDE `/internal/`, which needs no token.
    provider = token_provider or _provider_for(settings)
    await provider.get_token()
    headers = provider.build_headers(tenant_id)
    if "Authorization" not in headers:
        # ⛔ FAIL CLOSED AND SAY WHY. `build_headers` omits the header when no token
        # is cached rather than raising, so proceeding here would send an
        # unauthenticated request and surface as a bare 401 — an outage shape that
        # sends the operator to debug licences instead of billing's own credential.
        logger.warning("seat_count_no_service_token", tenant_id=tenant_id)
        raise SeatCountUnavailableError(
            "no service-account token available for the licences seat-count call"
        )

    # ⭐ `as_of` IS NOT OPTIONAL POLISH FOR A CLOSE. The endpoint's own parameter
    # description: "A CLOSED billing period must be counted at its own instant,
    # never at whenever the query is re-run." Omitting it while authenticating would
    # replace an honest refusal with a confidently wrong number — August's invoice
    # billed at today's seat count.
    params: dict[str, str] = {}
    if as_of is not None and period_start is None:
        params["as_of"] = as_of.isoformat()
    if app_key is not None:
        params["app"] = app_key
    if period_start is not None and period_end is not None:
        params["period_start"] = period_start.isoformat()
        params["period_end"] = period_end.isoformat()
    try:
        if client is not None:
            resp = await client.get(url, headers=headers, params=params or None)
        else:
            async with httpx.AsyncClient(timeout=5.0) as c:
                resp = await c.get(url, headers=headers, params=params or None)
    except httpx.HTTPError as exc:
        logger.warning("seat_count_lookup_failed", tenant_id=tenant_id, error=str(exc))
        raise SeatCountUnavailableError(f"licences unreachable: {exc}") from exc

    if resp.status_code == 404:
        _seat_cache[cache_key] = 0
        return 0
    if resp.status_code != 200:
        # ⚠️ httpx does NOT raise on status. Without this branch a 503 would fall
        # through to the parse below, fail to find `seats`, and raise the same
        # exception with a misleading message — so the operator would debug the
        # response shape instead of the outage.
        logger.warning(
            "seat_count_lookup_unavailable", tenant_id=tenant_id, status=resp.status_code
        )
        raise SeatCountUnavailableError(f"licences returned {resp.status_code}")

    try:
        payload = resp.json()
        # Tolerate both the bare object and an Envelope-wrapped `data`, because
        # the licenses service wraps some internal routes and not others, and
        # which one this route uses is not billing's business to assume.
        body = payload.get("data") if isinstance(payload.get("data"), dict) else payload
        licence_rows = int(body["seats"])
        # ⛔ A SCOPED REQUEST MUST BE ANSWERED SCOPED, OR NOT AT ALL. A licences build that
        # predates the scope parameters IGNORES them (FastAPI drops unknown query params) and
        # returns the all-apps, point-in-time count with a 200 — a plausible number for the wrong
        # question. The response echoes its basis; if it does not confirm the scope, refuse.
        if app_key is not None and body.get("app_key") != app_key:
            raise SeatCountUnavailableError(
                f"licences did not scope the seat count to app {app_key!r} "
                f"(answered app_key={body.get('app_key')!r}); refusing an unscoped count"
            )
        if period_start is not None and body.get("basis") != "active_during_period":
            raise SeatCountUnavailableError(
                "licences did not count over the requested period "
                f"(answered basis={body.get('basis')!r}); refusing a point-in-time count"
            )
        # ⛔ EVERY PER-SEAT TERM BILLS PEOPLE, NEVER NON-HUMAN SUBJECTS (#265). `seats` is
        # `len(licence rows)` and includes licences held by subjects licences classifies as
        # non-human (`NON_HUMAN_SUBJECT_REALMS`): `service` robots such as
        # `svc-workers-<tenant>`, and CommonPractice's tenant-licence subjects
        # `cp-tenant:<tenantId>:a|b` (licences#259). Billing `seats` charged a seat for each of
        # them. `human_subjects` is the person basis the operator chose (George, 2026-09-11) and
        # licences computes it for exactly this; the same field already billed pinned terms.
        #   * A PERIOD count (a `seat:<app>` term): `human_subjects` is REQUIRED, as before.
        #     Every licences build that answers `basis='active_during_period'` returns it, so a
        #     missing one is a malformed answer and the close refuses.
        #   * Every other per-seat term: a licences build that predates the person basis has no
        #     `human_subjects`. Falling back to the licence count is exactly what billing billed
        #     before #265, so it is LOGGED, not refused: refusing would block every close on a
        #     licences rollback, for a field whose absence only restores the previous number.
        human_subjects = body.get("human_subjects")
        if human_subjects is not None:
            seats = int(human_subjects)
        elif period_start is not None:
            raise KeyError("human_subjects")
        else:
            logger.warning(
                "seat_count_human_subjects_missing_fallback_to_licences",
                tenant_id=tenant_id,
                seats=licence_rows,
            )
            seats = licence_rows
    except (ValueError, KeyError, TypeError) as exc:
        logger.warning("seat_count_unparseable", tenant_id=tenant_id, error=str(exc))
        raise SeatCountUnavailableError(f"licences response not parseable: {exc}") from exc

    if seats < 0:
        raise SeatCountUnavailableError(f"licences returned a negative seat count: {seats}")

    _seat_cache[cache_key] = seats
    return seats
