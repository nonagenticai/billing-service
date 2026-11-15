"""Best-effort ``tenant_id`` -> human name resolution for the admin dashboard.

Why this is not in :mod:`services.billing`
------------------------------------------
``fetch_licenses`` there is deliberately FAIL-LOUD: it raises
:class:`~services.billing.LicensesUnavailableError` because its callers bill and
gate on the answer, and "no licences" must stay distinguishable from "I could
not ask". This module is the opposite contract on purpose. Its only consumer is
a *display label* on ``/dashboard/costs``: every failure mode degrades to the
uuid the ledger already holds, and nothing here may ever raise into a request
handler. Sharing one helper between the two would force one of the two contracts
onto the wrong caller.

⚠️ It is no longer only a display label. Since 2026-09-05
:func:`refresh_tenant_registry` feeds the same fetch into :mod:`tenancy`'s live
NAME -> uuid overlay, which sits on the resolution hot path for every inbound
``X-Tenant-Id`` header and every Redis-stream payload. That RAISES the stakes of
the fail-soft contract below without changing it: a licences outage must degrade
to the checked-in snapshot, never to a 400 storm. It is also why the fetch is
not duplicated — one client, one timeout, one place the payload is parsed.

``GET {licenses_url}/internal/tenants`` IS deployed — it shipped in licenses
PR #174 and was corrected in #177, and it now returns ``kind`` and
``cost_centre`` per tenant alongside ``name``. The "not deployed yet, so every
call 404s" note that stood here until 2026-09-03 is STALE; do not reason from it.

⚠️ **The "in prod it answers billing with 401" note that stood here until
2026-09-05 is ALSO STALE — do not reason from it, and do not treat the
empty-map path as the ordinary case.** Measured live from inside
``deploy/billing-service`` on 2026-09-05: billing's own ``ServiceTokenProvider``
mints a JWT and ``GET {licenses_url}/internal/tenants``
answers **200** with the full tenant list (unauthenticated it is 401, which is
the endpoint working as designed, not the defect). 14 of the 16 distinct
tenant_ids in ``billing_cost_events`` resolve to a real name. So resolution is
the EXPECTED path now, and a caller that skips this helper because "it 401s
anyway" is shipping uuids for no reason.

What has NOT changed is the fail-soft contract, and it is still load-bearing:
the endpoint requires a service-account JWT carrying one of licenses'
INTERNAL_SERVICE_ROLES, so a token-provider misconfiguration, an expired
secret, or a licences outage still degrades to ``{}``. That is why the fetch
stays wrapped whole in ``except Exception`` rather than the narrow
``httpx.HTTPError`` ``fetch_licenses`` uses: an auth failure, a transport
error, or a shape change in a payload we do not control must not be able to
500 the dashboard — or to fail a compliance CSV export (``routers.export``,
the second consumer since 2026-09-05).

``cost_centre`` is nullable at the source ON PURPOSE — licenses migration 027
adds the column and leaves it NULL for every row, because inventing values
before the apportionment basis exists would bake in a guess that later reads as
a measurement. It was NULL fleet-wide until 2026-09-03, when cost centres were
populated for 49 of 59 live tenants (customers/improvement/ci/tests/service/merqi);
10 rows are still deliberately untagged, so ``kind`` remains the reliable
partition that actually carries information. :func:`resolve_cost_centre`
encodes that precedence in one place so callers do not each re-derive it.
"""

from __future__ import annotations

import asyncio
from collections.abc import Mapping
from dataclasses import dataclass
from typing import TYPE_CHECKING

import httpx
import structlog
from cachetools import TTLCache

from tenancy import live_tenant_registry_stats, set_live_tenant_registry

if TYPE_CHECKING:
    from service_auth import ServiceTokenProvider
    from settings import Settings

logger = structlog.stdlib.get_logger(__name__)

#: The whole map is one cache entry — the endpoint returns every tenant in one
#: call, so there is nothing to key per tenant. 5-min TTL matches the licences
#: introspect + ``services.byo_mode`` caches.
_CACHE_KEY = "__all_tenants__"
_name_cache: TTLCache = TTLCache(maxsize=1, ttl=300)

#: What a tenant that licences could not classify reports as. Mirrors the
#: ``unclassified`` server_default on ``tenants.kind`` (licenses migration 027),
#: which fails CLOSED on purpose: an unclassified tenant must never be silently
#: counted as a customer (inflating revenue) or as internal (hiding cost).
UNCLASSIFIED = "unclassified"

#: Shown instead of a cost centre for the nil tenant, matching the label the
#: dashboard already gives that bucket.
UNATTRIBUTED = "unattributed"

#: Short on purpose. This call sits IN a page render, so its timeout is the
#: page's worst-case added latency. ``fetch_licenses`` uses 5s; a display label
#: does not get to hold the dashboard that long.
_TIMEOUT_SECONDS = 3.0

#: Query string for the fetch. ``include_archived`` is NOT optional politeness —
#: without it the map is missing every retired tenant and this module's callers
#: silently write ``""`` for them forever.
#:
#: ``GET /internal/tenants`` EXCLUDES archived tenants by default
#: (``include_archived: bool = Query(False)`` — licenses ``main.py``, shipped
#: with the endpoint itself in licenses #174, and confirmed on the LIVE pod's
#: ``/openapi.json`` on 2026-09-09, where the parameter is declared
#: ``{"name": "include_archived", "in": "query", "schema": {"type": "boolean",
#: "default": false}}``). Measured the same day against ``licenses.tenants``:
#: **17 of 76 tenants are archived**, and licences' own docstring says why they
#: must still be nameable — "a retired tenant still owns historical cost rows,
#: so a backfill over an old window has to be able to name it".
#:
#: The measured victim: ``019fb1f4-…-645f`` = ``rmt-e2e``, a tenant licences has
#: listed since 2026-08-11 and archived on 2026-09-03. Billing kept writing
#: ``""`` into ``tenant_name`` for its rows right through 09-09, because
#: :func:`stored_tenant_name` correctly refuses to store the uuid and the
#: registry — asked the wrong question — had nothing else to offer.
#:
#: ⚠️ This also widens :func:`refresh_tenant_registry`'s NAME -> uuid overlay by
#: the same 17 rows, and that is intended: billing ATTRIBUTES, it does not
#: authorise, and a late-arriving event for a just-archived tenant must land on
#: that tenant rather than be rejected as unregistered.
#:
#: ⚠️ NAMES ARE NO LONGER UNIQUE. Since the licences tenant-ownership change a
#: tenant name is unique per OWNING APP (``owner_key``), so two apps may each own
#: a ``netcare``. :func:`tenancy.set_live_tenant_registry` records such a name
#: as ambiguous and resolves it to NO id (counted on
#: ``billing_tenant_name_ambiguous_total``) — never a pick. That is why
#: :func:`refresh_tenant_registry` hands it (name, uuid) PAIRS rather than a
#: ``{name: uuid}`` dict, which would hide the duplicate by last-one-wins.
#:
#: Sent as a string, not a bool: httpx serialises ``True`` as ``"true"`` already,
#: but FastAPI's bool parser accepts the lowercase spelling on every version and
#: writing it out makes the wire value reviewable.
_TENANT_QUERY: dict[str, str] = {"include_archived": "true"}


#: Cached default service-account token providers, keyed by the credentials they
#: were built from — EXACTLY the pattern and the cache shape of
#: ``services.seats._default_providers``, and for the same reason, which that
#: module states: the callers that need a token here live in service modules with
#: no access to ``app.state``, and threading a provider down through
#: ``close_due_periods`` -> ``close_period`` would be several signature changes to
#: deliver one header. An injected provider still wins.
#:
#: ⛔ WITHOUT THIS, THE AUTOMATED SWEEP READS THE REGISTRY UNAUTHENTICATED AND
#: SEES NOTHING. ``GET /internal/tenants`` sits under licences'
#: ``PROTECTED_PREFIXES`` ("/internal") and requires ``license-admin`` or
#: ``license-reader``; ``services.invoices.close_due_periods`` calls
#: ``close_period`` with NO ``token_provider``, so every registry read on the
#: automated path had no Authorization header at all. Measured consequence before
#: this existed: the loop's invoices carried DEGRADED tenant names (the fail-open
#: reader's ``{}``), and the strict reader would have refused every tenant in the
#: estate every month while looking like a working gate.
_default_providers: dict[tuple[str, str], ServiceTokenProvider] = {}


def _provider_for(settings: Settings) -> ServiceTokenProvider | None:
    """The cached default provider for these credentials, or ``None``.

    ⚠️ RETURNS ``None`` RATHER THAN RAISING when a provider cannot be built from
    this ``settings``, and the direction matters: this function exists to ADD a
    header that was previously absent, so it must not be able to turn a reachable
    registry into an exception for callers that never had one. A ``None`` here
    sends the request unauthenticated — exactly the prior behaviour — and the
    strict reader then refuses on the resulting non-200, which is the correct and
    LOUD outcome for a service that cannot authenticate.

    ``getattr`` rather than attribute access for the same reason: the suite's
    narrow settings fakes carry only the fields their subject reads, and a
    contract test about the WIRE PARAMETER must not be able to fail over
    credentials it is not asserting.
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
            logger.info("tenant_registry_default_provider_unavailable", exc_info=True)
            return None
        _default_providers[key] = provider
    return provider


@dataclass(frozen=True)
class TenantInfo:
    """One row of ``GET /internal/tenants``, reduced to what billing displays.

    Frozen because it lives in a process-wide TTL cache shared by every
    concurrent dashboard render — a mutable value there is a cross-request
    write. Matches the ``@dataclass(frozen=True)`` value objects in
    :mod:`services.budget_debit` and :mod:`services.budget_watcher`.

    ``kind`` is non-null at the source (``NOT NULL DEFAULT 'unclassified'``) but
    is defaulted here anyway: this class is built from an untrusted JSON body,
    and a missing field must degrade to the fail-closed value rather than raise
    inside a display path.
    """

    name: str
    kind: str = UNCLASSIFIED
    cost_centre: str | None = None
    #: ⭐ THE PAYLOAD ALREADY CARRIED THIS AND THIS CLASS THREW IT AWAY.
    #: ``GET /internal/tenants`` returns ``"archived": t.archived_at is not None``
    #: for every row, and this module has been asking for archived tenants
    #: (``include_archived=true``) since it was written -- but only to NAME them.
    #: Capturing the flag is what lets a caller ask the authorisation question
    #: instead of only the display question.
    #: ⛔ DEFAULTS FALSE, AND THE DIRECTION IS DELIBERATE. A missing, non-bool or
    #: unparseable value means "we did not learn that this tenant is archived",
    #: which must degrade to NOT-archived: every consumer of this module is
    #: fail-open by construction (:func:`get_tenant_info` returns ``{}`` on any
    #: failure and caches that for the full TTL), so a fail-CLOSED default here
    #: would turn a licences blip into a refusal of live traffic. Cf. the
    #: budget gate's own policy: refusing because a CONFIGURATION row is absent
    #: is an outage, not enforcement.
    archived: bool = False
    #: The app that owns this tenant in licences (``commonpractice``, ``fr``,
    #: ``platform``, ...). ``None`` when licences did not send one (an older
    #: licences build). Names are unique only within one owner, so this is what
    #: tells two same-named tenants apart in a log or a report.
    owner_key: str | None = None
    #: The owning app's own id for the tenant, when it registered one. Nullable
    #: at the source.
    external_ref: str | None = None


def reset_cache() -> None:
    """Drop the cached name map. For tests — the TTL is the production path.

    Also drops the cached default token providers, for the same reason
    ``services.seats.clear_seat_cache`` does: a test that changes the Keycloak
    settings must not keep authenticating with a provider built from the old ones.
    """
    _name_cache.clear()
    _default_providers.clear()


async def _fetch_tenant_info(
    settings: Settings,
    token_provider: ServiceTokenProvider | None,
) -> dict[str, TenantInfo]:
    """Fetch ``{tenant_id_lowercase: TenantInfo}`` from the licences service.

    Returns ``{}`` for every "no usable answer" case. May raise — the public
    :func:`get_tenant_info` is what guarantees it never escapes.
    """
    if not settings.licenses_url:
        return {}

    # ⚠️ FALL BACK TO THE DEFAULT PROVIDER RATHER THAN SENDING NO HEADER. This
    # endpoint is auth-PROTECTED (see _default_providers above), so "no provider
    # injected" used to mean "guaranteed non-200", which the caller then reported
    # as an empty registry. Same credentials either way — both are built from the
    # same Settings — so an injected provider and this one authenticate as the
    # same service account.
    provider = token_provider if token_provider is not None else _provider_for(settings)
    headers: dict[str, str] = {}
    token = await provider.get_token() if provider is not None else None
    if token:
        headers["Authorization"] = f"Bearer {token}"

    async with httpx.AsyncClient(timeout=_TIMEOUT_SECONDS) as client:
        resp = await client.get(
            f"{settings.licenses_url}/internal/tenants",
            headers=headers,
            params=_TENANT_QUERY,
        )

    if resp.status_code != 200:
        # httpx does not raise on status. info, not warning: every consumer of
        # this module degrades to the uuid rather than failing, so a non-200 is
        # a lost label, not an incident. It IS unexpected now though — the
        # endpoint answers 200 in prod (see the module docstring).
        logger.info("tenant_names_unavailable", status_code=resp.status_code)
        return {}

    payload = resp.json()
    if not isinstance(payload, list):
        logger.info("tenant_names_unexpected_shape", shape=type(payload).__name__)
        return {}

    return parse_tenant_rows(payload)


def parse_tenant_rows(payload: list) -> dict[str, TenantInfo]:
    """Turn ``GET /internal/tenants``' JSON list into the id -> info map.

    ⭐ EXTRACTED SO IT CAN BE MUTATION-TESTED. While it was inline in
    :func:`_fetch_tenant_info`, the only way to reach it was through an HTTP
    call, so the endpoint tests seeded the cache directly and never executed a
    single line of it. Two mutants proved that: dropping ``archived=`` from the
    constructor, and relaxing ``is True`` to ``bool(...)``, BOTH left the suite
    green. A test that cannot fail for a mutation is not covering the code.
    """
    tenants: dict[str, TenantInfo] = {}
    for row in payload:
        if not isinstance(row, dict):
            continue
        tenant_id = row.get("id")
        name = row.get("name")
        if not (isinstance(tenant_id, str) and isinstance(name, str) and name.strip()):
            continue
        # A non-str or absent `kind` degrades to the fail-closed default rather
        # than dropping the row: the NAME is what the ledger cannot render
        # without, and losing a label over a classification field we only just
        # started reading would be a regression against the previous behaviour.
        kind = row.get("kind")
        kind = kind.strip() if isinstance(kind, str) and kind.strip() else UNCLASSIFIED
        # Nullable at the source; populated for most but not all tenants (see module
        # docstring). "" is normalised to None so callers have one absent-value
        # check, not two.
        centre = row.get("cost_centre")
        centre = centre.strip() if isinstance(centre, str) and centre.strip() else None
        # `is True`, not truthiness: the field is a JSON bool at the source, and a
        # string "false" -- which is truthy in Python -- would otherwise archive a
        # live tenant. The identity test makes every non-bool degrade to False,
        # which is the fail-open direction documented on the field.
        archived = row.get("archived") is True
        owner = row.get("owner_key")
        owner = owner.strip() if isinstance(owner, str) and owner.strip() else None
        ext = row.get("external_ref")
        ext = ext.strip() if isinstance(ext, str) and ext.strip() else None
        tenants[tenant_id.lower()] = TenantInfo(
            name=name.strip(),
            kind=kind,
            cost_centre=centre,
            archived=archived,
            owner_key=owner,
            external_ref=ext,
        )
    return tenants


async def get_tenant_info(
    settings: Settings,
    token_provider: ServiceTokenProvider | None = None,
) -> dict[str, TenantInfo]:
    """Return ``{tenant_id_lowercase: TenantInfo}``, or ``{}`` on any failure.

    Never raises and never blocks longer than :data:`_TIMEOUT_SECONDS`.

    A failed lookup is cached as ``{}`` for the full TTL, deliberately: NOT
    caching the miss would make every dashboard render and every CSV export pay
    the timeout again while licences is down. The cost of that choice is bounded
    — five minutes of uuid labels after a transient blip, on a label-only
    column. (This paragraph used to justify itself with "the endpoint answers
    billing with 401 in prod today"; that is stale, it answers 200 — see the
    module docstring. The caching rationale stands on its own.)
    """
    cached = _name_cache.get(_CACHE_KEY)
    if cached is not None:
        return cached

    try:
        tenants = await _fetch_tenant_info(settings, token_provider)
    except Exception:
        # Deliberately blind: transport error, timeout, a non-JSON body, an
        # unexpected shape from an endpoint whose payload we do not control.
        # None of them may reach the dashboard handler.
        logger.info("tenant_names_fetch_failed", exc_info=True)
        tenants = {}

    _name_cache[_CACHE_KEY] = tenants
    return tenants


class TenantRegistryUnavailableError(RuntimeError):
    """The licences tenant registry could not state its contents.

    ⭐ THIS EXISTS SO THAT "THE REGISTRY SAYS THIS TENANT IS ABSENT" AND "THE
    REGISTRY DID NOT ANSWER" CAN BE DIFFERENT FACTS. :func:`get_tenant_info` is
    fail-open by construction -- it returns ``{}`` for a transport error, a
    non-200, an unparseable body *and* for a registry that genuinely holds
    nothing, then caches that for the full TTL. For a label that is the right
    trade (five minutes of uuid labels beats five minutes of timeouts). For a
    caller that wants to REFUSE on absence it is fatal: every tenant in the
    estate reads as absent the moment licences blips, so a gate built on the
    fail-open reader would refuse every close and call it enforcement.

    Same contract, and the same reason, as
    :class:`services.seats.SeatCountUnavailableError` and
    :class:`services.byo_mode.ByoModeUnavailableError`: an upstream non-answer is
    a REFUSAL the caller retries, never a fabricated zero it freezes.
    """


async def get_tenant_info_strict(
    settings: Settings,
    token_provider: ServiceTokenProvider | None = None,
) -> dict[str, TenantInfo]:
    """Return the registry map, or RAISE. Never answers with a degraded ``{}``.

    For callers whose output is PERMANENT -- the invoice close, which freezes a
    row on a gapless, irreversible number series. Contrast
    :func:`get_tenant_info`, whose output is a display label.

    ⚠️ AN EMPTY MAP IS TREATED AS A NON-ANSWER, NOT AS "THE ESTATE HAS NO
    TENANTS". The second reading is not reachable in practice (the registry holds
    ~76 rows and billing itself cannot exist without one) while the first is
    reachable on every blip, so the ambiguity is resolved toward refusing. A
    registry that truly emptied is a catastrophe this function is right to refuse
    during.

    ⚠️ IT WILL NOT BE SATISFIED BY A CACHED ``{}``. ``if cached`` tests
    truthiness, not ``is not None``, so a poisoned entry written by a prior
    fail-open call is re-fetched rather than inherited -- otherwise one blip
    during a dashboard render would refuse every close for the rest of the TTL.
    A SUCCESSFUL fetch here populates the shared entry, so the two readers still
    cost one HTTP request between them.
    """
    cached = _name_cache.get(_CACHE_KEY)
    if cached:
        return cached

    try:
        tenants = await _fetch_tenant_info(settings, token_provider)
    except Exception as exc:  # transport, timeout, non-JSON, unexpected shape
        logger.warning("tenant_registry_strict_fetch_failed", exc_info=True)
        raise TenantRegistryUnavailableError(f"licences registry unreachable: {exc}") from exc

    if not tenants:
        # _fetch_tenant_info does NOT raise on a non-200 or a bad shape -- it
        # logs and returns {}. So this arm is the only place those become a
        # refusal for the close path.
        logger.warning("tenant_registry_strict_empty")
        raise TenantRegistryUnavailableError("licences registry returned no usable tenant rows")

    _name_cache[_CACHE_KEY] = tenants
    return tenants


async def get_tenant_names(
    settings: Settings,
    token_provider: ServiceTokenProvider | None = None,
) -> dict[str, str]:
    """Return ``{tenant_id_lowercase: display_name}``, or ``{}`` on any failure.

    Name-only view of :func:`get_tenant_info`, kept so callers that want a label
    and nothing else are unaffected by the richer payload. It shares that
    function's single cache entry — calling both costs one HTTP request, not two.
    """
    return {
        tid: info.name for tid, info in (await get_tenant_info(settings, token_provider)).items()
    }


async def is_tenant_archived(
    tenant_id: str,
    settings: Settings,
    token_provider: ServiceTokenProvider | None = None,
) -> bool:
    """Return True only when the licences registry POSITIVELY says archived.

    ⛔ EVERY UNKNOWN ANSWERS FALSE, AND THAT IS THE WHOLE CONTRACT. Unreachable
    licences, a non-200, an unexpected shape, a tenant absent from the map, a
    cold cache -- all of them mean "not known to be archived" and therefore
    ALLOW. This is the same ordering of harms the inference-router budget gate
    states explicitly: unbounded spend during a bounded dependency outage is a
    bounded loss, refusing all inference is not.

    ⚠️ It shares :func:`get_tenant_info`'s single cache entry, so asking this on
    a request path costs no HTTP call of its own for up to the full TTL. That is
    the reason the check is affordable where a per-request licences fetch would
    not have been: ``fetch_licenses`` opens a fresh client per call AND raises
    ``LicensesUnavailableError`` rather than degrading, which is correct for the
    invoice close and wrong for a hot path.

    ⚠️ CONSEQUENCE OF THE TTL, STATED: archiving a tenant takes up to the cache
    TTL to bite, and un-archiving takes the same. This is an off-switch with a
    five-minute fuse, not an instant kill. Anything needing immediacy must not
    be built on this function.
    """
    info = (await get_tenant_info(settings, token_provider)).get(tenant_id.lower())
    return bool(info and info.archived)


def resolve_tenant_name(
    tenant_id: str,
    stored_name: str | None,
    names: Mapping[str, str],
) -> str:
    """Resolve a display name for ``tenant_id``, in strict fallback order.

    1. the name the licences service reported, else
    2. ``CostDaily.tenant_name`` when it is non-empty AND is not merely the uuid
       echoed back (emitters that only know the uuid write it into that column,
       so it is not a name), else
    3. the uuid itself.
    """
    resolved = names.get(tenant_id.lower())
    if resolved:
        return resolved

    stored = (stored_name or "").strip()
    if stored and stored.lower() != tenant_id.lower():
        return stored

    return tenant_id


def resolve_cost_centre(
    tenant_id: str,
    tenants: Mapping[str, TenantInfo],
) -> str:
    """Resolve the reporting bucket for ``tenant_id``, in strict fallback order.

    1. ``cost_centre`` when licences reported a non-empty one, else
    2. ``kind`` — the partition that carries the information TODAY, because
       licenses migration 027 leaves ``cost_centre`` nullable and 10 live rows
       are still deliberately untagged
       until an apportionment basis exists, else
    3. :data:`UNCLASSIFIED`, which is also where every tenant lands while the
       fetch is failing (prod answers billing 401 right now). A whole page of
       ``unclassified`` is the honest rendering of "licences did not tell us",
       NOT a bug — and it is the same value licences itself defaults to, so the
       dashboard never invents a classification the registry did not make.

    Deliberately NOT derived from tenant NAME. Name prefixes are not a partition
    — ``svc-image-e2e-customer`` matches the service, test and customer greps at
    once, which is exactly how four hand-derived classifications disagreed and
    why ``tenants.kind`` was added as data in the first place.
    """
    info = tenants.get(tenant_id.lower())
    if info is None:
        return UNCLASSIFIED
    if info.cost_centre:
        return info.cost_centre
    return info.kind or UNCLASSIFIED


async def refresh_tenant_registry(
    settings: Settings,
    token_provider: ServiceTokenProvider | None = None,
) -> int:
    """Refresh :mod:`tenancy`'s live NAME -> uuid overlay. Returns names installed.

    The fetch half of the split described in
    :func:`tenancy.set_live_tenant_registry`: this module owns the HTTP client
    and the token, ``tenancy`` owns the pure map and stays dependency-free so it
    can keep being imported by pydantic validators. There is deliberately no
    second client — reusing :func:`get_tenant_info` means one timeout, one cache
    and one place where a payload we do not control is parsed.

    Never raises: :func:`get_tenant_info` already swallows everything, and an
    empty answer installs nothing and leaves the previous overlay in place. A
    ``0`` return means "the checked-in snapshot is still answering", which is
    the pre-existing behaviour, not an error.

    The shared TTL cache is cleared first so each loop tick is a REAL fetch
    rather than a re-read of a map this loop itself installed. That also leaves
    the cache freshly warm for the dashboard and the CSV export, which then pay
    no fetch of their own.
    """
    reset_cache()
    info = await get_tenant_info(settings, token_provider)
    # Hand over (name, uuid) PAIRS, not a {name: uuid} dict: tenant names are
    # unique per owning app, not globally, and a dict comprehension here would
    # last-one-win two same-named tenants BEFORE set_live_tenant_registry could
    # see the clash — resolving a bare name to whichever owner iterated last.
    installed = set_live_tenant_registry([(i.name, tid) for tid, i in info.items()])

    from observability.metrics import (
        tenant_registry_refresh_age_seconds,
        tenant_registry_refresh_total,
    )

    tenant_registry_refresh_total.labels(result="ok" if installed else "empty").inc()
    _, age = live_tenant_registry_stats()
    tenant_registry_refresh_age_seconds.set(age)
    if installed:
        logger.info("tenant_registry_refreshed", names=installed)
    else:
        logger.info("tenant_registry_refresh_empty", note="snapshot still resolving")
    return installed


def install_demand_refresh_hook(
    settings: Settings,
    token_provider: ServiceTokenProvider | None = None,
) -> None:
    """Let :mod:`tenancy` ask for a refresh when the hot path meets an unknown id.

    The loop above keeps the overlay fresh on a timer; this closes the window
    BETWEEN ticks. ``tenancy.is_registered_tenant_id`` calls the hook (rate
    limited to one call per ``tenancy.DEMAND_REFRESH_COOLDOWN_SECONDS``, and only
    when NOTHING vouches for the id) and never awaits it, so a tenant registered
    since the last tick is picked up in seconds instead of at the next interval.

    The hook is a closure rather than a direct import so ``tenancy`` keeps its
    one load-bearing property: no settings, no httpx, no token provider, and
    therefore importable from a pydantic validator. Same fetch/install split
    :func:`tenancy.set_live_tenant_registry` already documents.

    ⚠️ Installing it can only make resolution succeed MORE often. The scheduled
    refresh cannot change the answer the caller is being given right now, and
    ``refresh_tenant_registry`` swallows every failure, so a licences outage
    still degrades to the checked-in snapshot.
    """
    from tenancy import set_registry_refresh_hook

    def _hook():
        return refresh_tenant_registry(settings, token_provider)

    set_registry_refresh_hook(_hook)


async def run_tenant_registry_refresh_loop(
    settings: Settings,
    token_provider: ServiceTokenProvider | None,
    stop_event: asyncio.Event,
) -> None:
    """Keep the live overlay fresh for the life of the process.

    Runs on EVERY replica and is NOT leader-elected: the overlay is per-process
    in-memory state, so there is nothing to coordinate and a leader-gated
    refresh would leave every follower resolving from the snapshot forever.

    Not gated on ``consumer_enabled`` either — the resolver is on the HTTP
    boundary, which is live in every mode including the Phase 2 read-only
    deploy, and it is not gated on Redis for the same reason.

    Every iteration is wrapped: this loop dying must leave the snapshot
    answering rather than take the task down. The first tick runs immediately
    rather than after a full interval, so a pod is on the live registry seconds
    after boot; it is deliberately NOT awaited inside ``lifespan`` startup,
    because a licences timeout there would delay readiness for a map this
    service is designed to run without.
    """
    interval = max(30, settings.tenant_registry_refresh_seconds)
    while not stop_event.is_set():
        try:
            await refresh_tenant_registry(settings, token_provider)
        except Exception:  # pragma: no cover - defence in depth
            try:
                from observability.metrics import tenant_registry_refresh_total

                tenant_registry_refresh_total.labels(result="error").inc()
            except Exception:
                pass
            logger.warning("tenant_registry_refresh_loop_error", exc_info=True)
        try:
            await asyncio.wait_for(stop_event.wait(), timeout=interval)
        except TimeoutError:
            continue
