"""Tests for byo_mode resolution + the cache-poisoning guard (services/byo_mode.py).

The billing gate resolves each tenant's ``byo_mode`` from the licenses service
and caches it for 5 min. The guard we prove here: a *transient* licenses outage
returns the revenue-safe ``byo_required`` default but must NOT be cached — else
a single blip would pin a tenant to that mode for the whole TTL window.

⭐ AND THE ASYMMETRY, pinned in ``TestStrictResolver`` below: there are two
resolvers over one request path, and they are deliberately opposite on the
failure arms. ``get_byo_mode`` keeps the revenue-safe ``byo_required`` default
because the live gate's output is a TRANSIENT decision; ``get_byo_mode_strict``
raises because ``close_period``'s output is a PERMANENT invoice row, and
``byo_required`` there means minting a commercial statement (markup plus a
$5,000 access charge) for a tenant that bills $0. Measured 2026-09-11: 49 of 59
unarchived tenants are in exactly that class. Same argument, same upstream
service, as the seats contract in ``tests/test_commercial_terms.py``.

⭐ TWO THINGS WERE ADDED 2026-09-13 AND NEITHER CHANGES A SINGLE EXISTING
ASSERTION, which is itself the claim under test:

``TestRequestHeaders``
    This was the FOURTH licences call site in the repo sending no
    ``Authorization`` and no ``X-Tenant-Id``, and the only one left. The other
    three were each found in production, one at a time, after the damage. Its
    docstring excused it with "open in-cluster, same posture as introspect" — an
    assumption about a route this repo does not own, which the sibling PR putting
    auth on ``POST /license/introspect`` invalidates.

``TestVerdict``
    ``get_byo_mode`` answers the identical string ``"byo_required"`` for a
    genuinely commercial tenant and for a 401/timeout/503, and
    ``effective_billing_mode`` turns that into the configured COMMERCIAL mode. So
    a caller freezing an immutable invoice could not tell an answer from a guess.
    :class:`services.byo_mode.ByoModeVerdict` makes the two different observable
    outcomes; ``test_the_live_access_gate_still_fails_closed_to_bill`` is the
    control proving the live gate's revenue-safe default is untouched.
"""

from __future__ import annotations

import asyncio
from typing import cast

import httpx
import pytest

from service_auth import ServiceTokenProvider
from services import byo_mode as byo
from tests.conftest import T

pytestmark = pytest.mark.asyncio


class _FakeResp:
    def __init__(self, status_code: int, payload: dict | None = None) -> None:
        self.status_code = status_code
        self._payload = payload or {}

    def json(self) -> dict:
        return self._payload


class _RaisingClient:
    """AsyncClient stub whose GET always raises a transient network error."""

    def __init__(self, exc: Exception | None = None) -> None:
        self.calls = 0
        self.headers: list[dict | None] = []
        self._exc = exc or httpx.ConnectError("licenses unreachable")

    # ⚠️ `**kwargs`, NOT a bare `url`. The resolver now sends `headers=`, and a
    # stub with a fixed signature would fail every test with a TypeError about
    # the STUB rather than an assertion about the SUBJECT. Every other byo-mode
    # stub in the suite (tests/conftest.py, tests/test_invoices.py,
    # tests/test_invoice_closer.py, tests/routers/test_invoices_routes.py,
    # tests/test_commercial_terms.py, tests/test_invoice_from_production_rows.py)
    # was already written `(self, url, *a, **kw)` and needed no change; these two
    # were the only strict ones.
    async def get(self, url: str, **kwargs) -> _FakeResp:
        self.calls += 1
        self.headers.append(kwargs.get("headers"))
        raise self._exc


class _OkClient:
    def __init__(self, status_code: int, payload: dict | None = None) -> None:
        self._resp = _FakeResp(status_code, payload)
        self.calls = 0
        #: Every request's headers, in order — a LIST, not a last-write dict. The
        #: same reason `tests/test_commercial_terms.py` records `seats_requests`
        #: as a list: the close resolves byo_mode more than once, and grading only
        #: the final call lets a missing header on an earlier one pass.
        self.headers: list[dict | None] = []

    async def get(self, url: str, **kwargs) -> _FakeResp:
        self.calls += 1
        self.headers.append(kwargs.get("headers"))
        return self._resp


class _StubProvider:
    """A ``ServiceTokenProvider``-shaped stub with a token already in hand.

    Deliberately NOT a real provider: a real one would try to reach Keycloak, and
    what is under test is that the header REACHES THE WIRE, not how it is minted.
    """

    def __init__(self, token: str | None = "stub-jwt") -> None:
        self._token = token
        self.token_calls = 0

    async def get_token(self) -> str | None:
        self.token_calls += 1
        return self._token

    def build_headers(self, tenant_id: str) -> dict[str, str]:
        headers = {"X-Tenant-Id": tenant_id}
        if self._token:
            headers["Authorization"] = f"Bearer {self._token}"
        return headers


@pytest.fixture(autouse=True)
def _clear_byo_cache():
    # ⚠️ `clear_byo_cache()`, not `_byo_cache.clear()`: it now also drops the
    # cached default token providers, which are keyed on the Keycloak settings.
    # A test that swaps those must not inherit a provider built from the previous
    # ones — the same reason `services.seats.clear_seat_cache` clears both.
    byo.clear_byo_cache()
    yield
    byo.clear_byo_cache()


async def test_transient_failure_defaults_but_does_not_cache(test_settings):
    """Transient outage -> byo_required WITHOUT caching, so a later real answer
    wins (no cache poisoning). This is the load-bearing FIX-3 assertion."""
    tenant = T("tenant-abc")

    failing = _RaisingClient()
    mode = await byo.get_byo_mode(tenant, test_settings, client=cast(httpx.AsyncClient, failing))
    assert mode == "byo_required"
    assert failing.calls == 1
    # The failure must NOT have populated the cache.
    assert tenant not in byo._byo_cache

    # A later real answer (local_allowed) must win — proving the outage did not
    # pin the tenant to the fallback for the TTL window.
    ok = _OkClient(200, {"byo_mode": "local_allowed"})
    mode2 = await byo.get_byo_mode(tenant, test_settings, client=cast(httpx.AsyncClient, ok))
    assert mode2 == "local_allowed"
    assert ok.calls == 1  # cache miss -> the real fetch actually happened
    assert byo._byo_cache.get(tenant) == "local_allowed"


async def test_real_answer_is_cached(test_settings):
    """A successful classification IS cached — a second call does not re-fetch."""
    tenant = T("tenant-xyz")

    ok = _OkClient(200, {"byo_mode": "byo_required"})
    assert (
        await byo.get_byo_mode(tenant, test_settings, client=cast(httpx.AsyncClient, ok))
        == "byo_required"
    )
    assert ok.calls == 1

    # Second call is served from cache: the (different) client is never hit.
    other = _OkClient(200, {"byo_mode": "local_allowed"})
    assert (
        await byo.get_byo_mode(tenant, test_settings, client=cast(httpx.AsyncClient, other))
        == "byo_required"
    )
    assert other.calls == 0


async def test_404_fails_closed_to_bill_and_caches(test_settings):
    """Unknown tenant (404) fails CLOSED to byo_required and IS cached — a 404 is
    a real answer, not a transient error."""
    tenant = T("tenant-404")

    ok = _OkClient(404)
    assert (
        await byo.get_byo_mode(tenant, test_settings, client=cast(httpx.AsyncClient, ok))
        == "byo_required"
    )
    assert byo._byo_cache.get(tenant) == "byo_required"


@pytest.mark.parametrize("status", [500, 502, 503, 504])
async def test_5xx_defaults_but_does_not_cache(test_settings, status: int):
    """A licences 5xx must NOT be cached as a billing classification.

    ⚠️ ASSERT ON THE CACHE, NOT THE RETURN VALUE. ``get_byo_mode`` returns
    ``byo_required`` whether or not it cached, so a test that only checks the
    return value is VACUOUS — it passes identically against the defect. The
    cache write is the entire bug.

    THE DEFECT: ``httpx`` does not raise on status, so a 5xx skipped the
    ``httpx.HTTPError`` arm (which deliberately does not cache) and fell through
    to the shared ``mode = "byo_required"`` + cache write. An internal/demo
    tenant — ``local_allowed``/``platform_only`` -> ``BILLING_MODE_INTERNAL`` ->
    bills $0 — was therefore billed COMMERCIALLY for the full 5-minute TTL on
    the strength of one blip.

    Mutant that turns this RED: fold the 5xx branch back into the 404 branch
    (i.e. ``else: mode = "byo_required"`` with no early return).
    """
    tenant = T(f"tenant-{status}")

    failing = _OkClient(status)
    assert (
        await byo.get_byo_mode(tenant, test_settings, client=cast(httpx.AsyncClient, failing))
        == "byo_required"
    )
    assert failing.calls == 1
    assert tenant not in byo._byo_cache, (
        f"a {status} from licences was cached as a billing classification; an "
        "internal tenant would be billed commercially for the whole TTL window"
    )


async def test_5xx_does_not_shadow_a_later_real_answer(test_settings):
    """The consequence that makes the cache write expensive, driven end to end.

    A blip followed by a recovery must yield the tenant's REAL mode. Against the
    defect the second call is served from the poisoned cache and returns
    ``byo_required``, so this fails on the value — the discriminator the
    cache-membership assertion above cannot express on its own.
    """
    tenant = T("tenant-internal")

    blip = _OkClient(503)
    assert (
        await byo.get_byo_mode(tenant, test_settings, client=cast(httpx.AsyncClient, blip))
        == "byo_required"
    )

    recovered = _OkClient(200, {"byo_mode": "local_allowed"})
    assert (
        await byo.get_byo_mode(tenant, test_settings, client=cast(httpx.AsyncClient, recovered))
        == "local_allowed"
    ), (
        "the 503 shadowed the real answer — this tenant bills $0 and was being "
        "charged markup plus access charges"
    )
    assert recovered.calls == 1, "the second call must be a cache MISS and actually re-fetch"
    assert byo._byo_cache.get(tenant) == "local_allowed"


async def test_unexpected_4xx_is_not_cached_either(test_settings):
    """401/403 is a misconfiguration, not an answer about the tenant.

    Pinned so a future 'simplify' that folds everything non-200 back into the
    404 branch cannot quietly restore the poisoning for auth failures.
    """
    tenant = T("tenant-403")

    ok = _OkClient(403)
    assert (
        await byo.get_byo_mode(tenant, test_settings, client=cast(httpx.AsyncClient, ok))
        == "byo_required"
    )
    assert tenant not in byo._byo_cache


# --------------------------------------------------------------------------- #
# The STRICT resolver — the close-path contract
# --------------------------------------------------------------------------- #
#
# ⚠️ THE NON-STRICT 503 CONTRACT IS ALREADY PINNED ABOVE and is NOT repeated
# here: ``test_5xx_defaults_but_does_not_cache`` is parametrized over
# [500, 502, 503, 504] and asserts both halves of the live-gate behaviour (the
# ``byo_required`` return AND the absent cache entry), and
# ``test_5xx_does_not_shadow_a_later_real_answer`` drives the consequence end to
# end. Those tests are the regression guard for "the strict resolver was
# introduced by changing the default one", and they were not edited.


class TestStrictResolver:
    """``get_byo_mode_strict`` — no default, because its caller freezes money."""

    async def test_strict_raises_on_5xx_and_does_not_cache(self, test_settings):
        """⛔ THE ONE THAT PROTECTS MONEY.

        A 503 from licences is a statement about the SERVICE. Defaulted to
        ``byo_required`` at close it becomes a permanent commercial invoice for
        an internal cost centre, and nothing downstream recomputes the billing
        mode of a closed invoice.

        ⚠️ MUTATION CONTROL: replace the ``raise ByoModeUnavailableError`` in
        ``_resolve_byo_mode``'s non-200/404 arm with ``mode = "byo_required"``
        (i.e. fold it back into the 404 branch) and this test fails on the
        missing exception — and ``test_5xx_defaults_but_does_not_cache`` above
        fails too, on the cache write.
        """
        tenant = T("strict-503")

        blip = _OkClient(503)
        with pytest.raises(byo.ByoModeUnavailableError) as exc:
            await byo.get_byo_mode_strict(
                tenant, test_settings, client=cast(httpx.AsyncClient, blip)
            )
        assert "503" in str(exc.value), (
            "the refusal does not name the status an operator must chase"
        )
        assert blip.calls == 1
        # Not cached: the next attempt, seconds later against a recovered
        # licences, must get the real answer rather than a 5-min-old failure.
        assert tenant not in byo._byo_cache

        recovered = _OkClient(200, {"byo_mode": "local_allowed"})
        mode = await byo.get_byo_mode_strict(
            tenant, test_settings, client=cast(httpx.AsyncClient, recovered)
        )
        assert mode == "local_allowed"
        assert recovered.calls == 1, "the retry was served from a poisoned cache"

    async def test_strict_raises_on_a_network_error(self, test_settings):
        """⚠️ MUTATION CONTROL: change the ``httpx.HTTPError`` arm of
        ``_resolve_byo_mode`` back to ``return "byo_required"`` and this fails on
        the missing exception.
        """
        tenant = T("strict-netfail")

        failing = _RaisingClient()
        with pytest.raises(byo.ByoModeUnavailableError):
            await byo.get_byo_mode_strict(
                tenant, test_settings, client=cast(httpx.AsyncClient, failing)
            )
        assert failing.calls == 1
        assert tenant not in byo._byo_cache

    async def test_strict_returns_the_real_value_on_200_and_caches_it(self, test_settings):
        """⭐ THE CACHE WRITE IS LOAD-BEARING, NOT AN OPTIMISATION.

        ``close_period`` calls this gate BEFORE ``_capture_inputs`` and
        ``generate_billing``, which both re-resolve the same tenant through the
        DEFAULTING ``get_byo_mode``. The gate's cache entry is what makes those
        two read the real answer if licences dies in between, instead of
        guessing ``byo_required`` onto the frozen row.

        ⚠️ MUTATION CONTROL: delete the ``_byo_cache[tenant_id] = mode`` line in
        ``_resolve_byo_mode`` and this fails on ``second.calls == 0``.
        """
        tenant = T("strict-200")

        ok = _OkClient(200, {"byo_mode": "platform_only"})
        assert (
            await byo.get_byo_mode_strict(tenant, test_settings, client=cast(httpx.AsyncClient, ok))
            == "platform_only"
        )
        assert ok.calls == 1
        assert byo._byo_cache.get(tenant) == "platform_only"

        # The TOCTOU-closing property, driven: a LATER defaulting lookup against
        # a licences that is now dead still gets the real answer.
        dead = _RaisingClient()
        assert (
            await byo.get_byo_mode(tenant, test_settings, client=cast(httpx.AsyncClient, dead))
            == "platform_only"
        )
        assert dead.calls == 0, "the defaulting resolver re-fetched and could have guessed"

    async def test_strict_treats_404_as_a_durable_answer_and_caches_it(self, test_settings):
        """A 404 is licences ANSWERING — it does not know this tenant. That is
        durable information about the tenant, not about the service, so strict
        and non-strict agree here: ``byo_required``, cached.

        ⚠️ MUTATION CONTROL: move the 404 into the raising arm and this fails on
        the exception; drop its cache write and it fails on the cache assertion.
        """
        tenant = T("strict-404")

        ok = _OkClient(404)
        assert (
            await byo.get_byo_mode_strict(tenant, test_settings, client=cast(httpx.AsyncClient, ok))
            == "byo_required"
        )
        assert byo._byo_cache.get(tenant) == "byo_required"

    async def test_strict_refuses_an_empty_tenant_id_where_the_gate_defaults(self, test_settings):
        """The two resolvers on the SAME input, side by side — the asymmetry as a
        single assertion pair, so a 'simplify' that unifies them cannot pass.
        """
        with pytest.raises(byo.ByoModeUnavailableError):
            await byo.get_byo_mode_strict("", test_settings)
        assert await byo.get_byo_mode("", test_settings) == "byo_required"

    @pytest.mark.parametrize("mode", ["local_allowed", "platform_only", "byo_required"])
    async def test_both_resolvers_agree_on_every_real_answer(self, test_settings, mode: str):
        """The refactor's own control: one request path, so a 200 must produce the
        identical value through both entry points. Run second from the cache the
        first populated, which is also the contract ``close_period`` relies on.
        """
        tenant = T(f"strict-agree-{mode}")

        ok = _OkClient(200, {"byo_mode": mode})
        assert (
            await byo.get_byo_mode_strict(tenant, test_settings, client=cast(httpx.AsyncClient, ok))
            == mode
        )
        assert (
            await byo.get_byo_mode(tenant, test_settings, client=cast(httpx.AsyncClient, ok))
            == mode
        )
        assert ok.calls == 1, "the second resolver re-fetched instead of reading the shared cache"


# --------------------------------------------------------------------------- #
# The HEADERS — the fourth licences call site that sent none
# --------------------------------------------------------------------------- #


class TestRequestHeaders:
    """⛔ THE FOURTH SITE, AND THE ONLY ONE THAT WAS STILL UNAUTHENTICATED.

    ``services.seats`` (measured 401 from inside the billing pod 2026-09-11, so
    the per-seat charge could never be billed at all), ``services.tenant_names``
    (no header on the automated close path, so the strict registry reader would
    have refused every tenant in the estate) and ``services.billing.fetch_licenses``
    all had this retro-fitted AFTER it bit, one at a time, in production. This one
    was excused by a docstring claiming the route was "open in-cluster, same
    posture as introspect" — an assumption about a route this repo does not own,
    and one the sibling PR putting auth on ``POST /license/introspect`` is about to
    falsify.
    """

    async def test_tenant_header_is_always_sent(self, test_settings):
        """⚠️ FAILS ON UNMODIFIED CODE, on the VALUE not on a missing symbol: the
        old call was ``client.get(url)`` with no ``headers=`` at all, so the
        recorded headers are ``None``.

        ⚠️ MUTATION CONTROL: delete the ``X-Tenant-Id`` entry from the resolver's
        header dict and this fails; delete the whole ``headers=`` argument and it
        fails on ``is not None``.
        """
        tenant = T("hdr-tenant")

        ok = _OkClient(200, {"byo_mode": "local_allowed"})
        assert (
            await byo.get_byo_mode(tenant, test_settings, client=cast(httpx.AsyncClient, ok))
            == "local_allowed"
        )

        assert ok.headers and ok.headers[0] is not None, (
            "the licences call sent no headers at all — this is the shape that made "
            "three sibling modules 401 in production"
        )
        assert ok.headers[0].get("X-Tenant-Id") == tenant

    async def test_an_injected_provider_wins_and_its_token_reaches_the_wire(self, test_settings):
        """An injected provider must beat the cached default, and its token must be
        ON THE REQUEST — not merely minted.

        ⚠️ MUTATION CONTROL: drop the ``await provider.get_token()`` before
        ``build_headers`` and a REAL provider would silently send no
        ``Authorization`` (``build_headers`` only attaches an ALREADY-cached
        token — ``service_auth.py:60-65``). This stub cannot catch that ordering
        bug on its own, which is why ``test_token_is_fetched_before_headers_are_built``
        below asserts the ordering directly.
        """
        tenant = T("hdr-injected")
        provider = _StubProvider("injected-jwt")

        ok = _OkClient(200, {"byo_mode": "platform_only"})
        mode = await byo.get_byo_mode(
            tenant,
            test_settings,
            client=cast(httpx.AsyncClient, ok),
            token_provider=cast(ServiceTokenProvider, provider),
        )
        assert mode == "platform_only"
        assert ok.headers[0] is not None
        assert ok.headers[0].get("Authorization") == "Bearer injected-jwt"
        assert ok.headers[0].get("X-Tenant-Id") == tenant

    async def test_token_is_fetched_before_headers_are_built(self, test_settings):
        """The ORDERING, asserted on its own.

        ``build_headers`` never fetches a token; it reads the one the provider has
        already cached. So a resolver that builds headers first and awaits
        ``get_token()`` afterwards (or not at all) sends an unauthenticated request
        while looking correct. Asserted as "the provider was asked" rather than by
        inspecting internals.
        """
        tenant = T("hdr-order")
        provider = _StubProvider("ordered-jwt")

        ok = _OkClient(200, {"byo_mode": "byo_required"})
        await byo.get_byo_mode(
            tenant,
            test_settings,
            client=cast(httpx.AsyncClient, ok),
            token_provider=cast(ServiceTokenProvider, provider),
        )
        assert provider.token_calls == 1, "get_token() was never awaited before the request"

    async def test_no_credentials_still_sends_the_request(self, test_settings):
        """⛔ FAIL-SOFT, AND THIS IS THE ASSERTION THAT PINS IT.

        ``test_settings`` carries no ``keycloak_token_url`` and no
        ``service_client_secret``, so the provider exists but is NOT ``configured``
        and mints no token. The route answers 200 unauthenticated TODAY, so the
        request must still go out exactly as it did before — if a missing
        credential raised instead (the ``services.seats`` contract), every close in
        the estate would refuse, which is a self-inflicted version of the outage
        this module exists to survive.

        ⚠️ ``_provider_for`` IS NOT ``None`` HERE, AND THAT IS NOT THE BUG.
        ``Settings.service_client_id`` defaults to ``"billing-service"``
        (``src/settings.py:281-284``), so the ``if not any(key)`` guard — copied
        verbatim from ``services.tenant_names._provider_for``, which has exactly the
        same behaviour for the same reason — does not fire on a default
        ``Settings``. What makes this safe is one layer down:
        ``ServiceTokenProvider.configured`` needs BOTH a token url and a secret
        (``src/service_auth.py:26-28``), so ``get_token()`` returns ``None`` without
        making any network call at all. Asserted below, because "no Authorization
        header" and "no Keycloak round trip from a unit test" are different facts
        and the suite depends on the second.

        ⚠️ MUTATION CONTROL: make ``_provider_for`` raise, or make the resolver
        raise ``ByoModeUnavailableError`` when no ``Authorization`` could be built
        (the ``seats`` shape), and this fails.
        """
        tenant = T("hdr-nocreds")
        provider = byo._provider_for(test_settings)
        assert provider is not None and provider.configured is False

        ok = _OkClient(200, {"byo_mode": "local_allowed"})
        assert (
            await byo.get_byo_mode(tenant, test_settings, client=cast(httpx.AsyncClient, ok))
            == "local_allowed"
        )
        assert ok.calls == 1
        assert ok.headers[0] is not None
        assert "Authorization" not in ok.headers[0]
        assert ok.headers[0].get("X-Tenant-Id") == tenant


# --------------------------------------------------------------------------- #
# THE MONEY DEFECT — "licences said byo_required" vs "I could not determine it"
# --------------------------------------------------------------------------- #


class TestVerdict:
    """⛔ THE PAIR OF FACTS THAT USED TO BE ONE STRING.

    ``get_byo_mode`` answers ``"byo_required"`` for a tenant licences positively
    classified as commercial AND for a 401, a timeout and a 503 — and
    ``effective_billing_mode`` maps ``byo_required`` to the configured COMMERCIAL
    mode. Measured 2026-09-11: 49 of 59 unarchived tenants carry
    ``local_allowed``/``platform_only`` and bill $0, so one blip presents as up to
    49 plausible commercial classifications with nothing marking them as guesses.

    ⚠️ THE FIX IS NOT A CHANGE OF BEHAVIOUR FOR THE LIVE GATE, and
    ``test_the_live_access_gate_still_fails_closed_to_bill`` below is the control
    that proves it: the string resolvers are byte-identical, the new information is
    ADDITIVE, and a caller that ignores it is exactly as wrong as before.
    """

    @pytest.mark.parametrize(
        ("status", "expected_reason"),
        [(401, "auth"), (403, "auth"), (500, "http_error"), (503, "http_error")],
    )
    async def test_a_non_answer_is_indeterminate_with_its_fault_class(
        self, test_settings, status: int, expected_reason: str
    ):
        """⛔ THE ONE THE INVOICE CALLER NEEDS. Fails on unmodified code, which has
        no way to express this at all.

        ⚠️ MUTATION CONTROL that keeps it from being a mere symbol-existence test:
        make ``get_byo_mode_verdict`` return ``determined=True`` on the except arm
        and this goes red on every parameter; collapse ``auth`` into
        ``http_error`` and the 401/403 rows go red while the 5xx rows stay green.
        """
        tenant = T(f"verdict-{status}")

        blip = _OkClient(status)
        verdict = await byo.get_byo_mode_verdict(
            tenant, test_settings, client=cast(httpx.AsyncClient, blip)
        )

        assert verdict.determined is False, (
            f"a {status} from licences presented as an AUTHORITATIVE classification; "
            "an invoice built on it is a permanent commercial statement for a tenant "
            "that bills $0"
        )
        assert verdict.reason == expected_reason
        assert verdict.reason in byo.INDETERMINATE_REASONS
        # The revenue-safe default is still CARRIED, so a live-gate caller reading
        # `.mode` keeps failing closed to bill. That is the point of the shape.
        assert verdict.mode == "byo_required"
        # And the non-answer is still not cached.
        assert tenant not in byo._byo_cache

    async def test_a_timeout_is_indeterminate_and_labelled_timeout(self, test_settings):
        """``timeout`` is its own reason, and the arm ordering is what makes it
        reachable: ``httpx.TimeoutException`` is a SUBCLASS of ``httpx.HTTPError``.

        ⚠️ MUTATION CONTROL: move the ``httpx.TimeoutException`` arm below the
        ``httpx.HTTPError`` arm in ``_resolve_byo_mode`` and this fails with
        ``reason == "unavailable"`` — an unreachable label value, i.e. a metric
        that silently cannot report the case it names.
        """
        tenant = T("verdict-timeout")

        slow = _RaisingClient(httpx.ReadTimeout("licences took too long"))
        verdict = await byo.get_byo_mode_verdict(
            tenant, test_settings, client=cast(httpx.AsyncClient, slow)
        )

        assert verdict.determined is False
        assert verdict.reason == "timeout"
        assert verdict.mode == "byo_required"
        assert tenant not in byo._byo_cache

    async def test_a_transport_failure_is_labelled_unavailable(self, test_settings):
        tenant = T("verdict-connfail")

        failing = _RaisingClient()
        verdict = await byo.get_byo_mode_verdict(
            tenant, test_settings, client=cast(httpx.AsyncClient, failing)
        )
        assert (verdict.determined, verdict.reason) == (False, "unavailable")

    async def test_a_genuine_byo_required_is_determined_and_still_commercial(self, test_settings):
        """⭐ THE OTHER HALF, AND WITHOUT IT THE FIX WOULD BE INDISTINGUISHABLE FROM
        BREAKING BILLING. A real commercial answer must read as DETERMINED and must
        still map to the configured commercial mode — the constraint is that no
        tenant's ``byo_mode`` value and no ``effective_billing_mode`` mapping
        changes.

        ⚠️ MUTATION CONTROL: make the success arm return ``determined=False`` and
        this fails; change ``effective_billing_mode``'s commercial branch and the
        second assertion fails.
        """
        tenant = T("verdict-real-commercial")

        ok = _OkClient(200, {"byo_mode": "byo_required"})
        verdict = await byo.get_byo_mode_verdict(
            tenant, test_settings, client=cast(httpx.AsyncClient, ok)
        )

        assert (verdict.mode, verdict.determined, verdict.reason) == ("byo_required", True, None)
        assert byo.effective_billing_mode("standard_markup", verdict.mode) == "standard_markup"
        # A real answer IS cached, unchanged.
        assert byo._byo_cache.get(tenant) == "byo_required"

    async def test_a_404_is_determined_because_it_is_a_durable_answer(self, test_settings):
        """⛔ THE 404 CONTRACT IS UNTOUCHED. A 404 is licences ANSWERING — it does
        not know this tenant — so it is durable information ABOUT THE TENANT, is
        cached, and is ``determined=True``. ``services.invoices`` refuses such a
        tenant through its own ``assert_tenant_invoiceable`` registry gate, which is
        where that question belongs; relabelling the 404 as indeterminate here
        would move a deliberate, documented answer into the outage bucket.
        """
        tenant = T("verdict-404")

        ok = _OkClient(404)
        verdict = await byo.get_byo_mode_verdict(
            tenant, test_settings, client=cast(httpx.AsyncClient, ok)
        )
        assert (verdict.mode, verdict.determined, verdict.reason) == ("byo_required", True, None)
        assert byo._byo_cache.get(tenant) == "byo_required"

    @pytest.mark.parametrize("failure", [_OkClient(401), _RaisingClient(httpx.ReadTimeout("slow"))])
    async def test_the_live_access_gate_still_fails_closed_to_bill(self, test_settings, failure):
        """⭐ THE CONTROL. The live gate's revenue-safe default is CORRECT and this
        change must not touch it: a 401 and a timeout must still present to
        ``get_byo_mode`` as ``byo_required``, and therefore still resolve to the
        configured COMMERCIAL mode, so nobody gets free platform GPU because a
        lookup failed.

        ⚠️ MUTATION CONTROL: make ``get_byo_mode`` raise, or return the verdict
        object instead of ``.mode``, and this fails.
        """
        tenant = T("verdict-gate-control")

        mode = await byo.get_byo_mode(tenant, test_settings, client=failure)
        assert mode == "byo_required"
        assert byo.effective_billing_mode("standard_markup", mode) == "standard_markup"

    async def test_an_empty_tenant_id_is_indeterminate_but_still_defaults(self, test_settings):
        """No tenant to ask about is not an answer either — but the long-standing
        return value is unchanged (pinned by
        ``test_strict_refuses_an_empty_tenant_id_where_the_gate_defaults`` above).
        """
        verdict = await byo.get_byo_mode_verdict("", test_settings)
        assert (verdict.mode, verdict.determined, verdict.reason) == (
            "byo_required",
            False,
            "unavailable",
        )

    async def test_the_laundering_is_counted(self, test_settings, monkeypatch):
        """The counter is the estate-wide view: 49 quiet defaults and one real
        commercial tenant are the same INVOICE population, and only a count
        separates them.

        ⚠️ ASSERTED THROUGH A SPY, NOT AGAINST THE REAL COUNTER. The metric
        ``billing_byo_mode_indeterminate_total`` is declared in
        ``src/observability/metrics.py``, which this change does not own, and
        ``_count_indeterminate`` imports it lazily inside a ``try`` precisely so
        this module stays importable before that declaration lands. A test bound to
        the real Counter would therefore be asserting the DECLARATION's existence,
        not this module's behaviour, and would go red for a reason that is not this
        module's defect.

        ⚠️ MUTATION CONTROL: delete the ``_count_indeterminate`` call from the
        except arm and this fails; move it to the success arm and it fails on the
        200 assertion.
        """
        seen: list[str] = []
        monkeypatch.setattr(byo, "_count_indeterminate", lambda reason: seen.append(reason))

        await byo.get_byo_mode_verdict(
            T("counted-503"), test_settings, client=cast(httpx.AsyncClient, _OkClient(503))
        )
        await byo.get_byo_mode_verdict(
            T("counted-401"), test_settings, client=cast(httpx.AsyncClient, _OkClient(401))
        )
        assert seen == ["http_error", "auth"]

        # ⛔ A DETERMINED ANSWER MUST NOT INCREMENT IT. A counter that also counts
        # successes cannot be alerted on.
        await byo.get_byo_mode_verdict(
            T("counted-ok"),
            test_settings,
            client=cast(httpx.AsyncClient, _OkClient(200, {"byo_mode": "local_allowed"})),
        )
        assert seen == ["http_error", "auth"]

    async def test_the_strict_resolver_does_not_count_a_refusal(self, test_settings, monkeypatch):
        """⛔ THE COUNTER MEASURES LAUNDERING, NOT OUTAGES. ``get_byo_mode_strict``
        RAISES, so no guess reaches money through it; counting it too would
        double-count a single blip (the close calls strict AND the defaulting
        resolver) and overstate how many guesses actually got written down.
        """
        seen: list[str] = []
        monkeypatch.setattr(byo, "_count_indeterminate", lambda reason: seen.append(reason))

        blip = _OkClient(503)
        with pytest.raises(byo.ByoModeUnavailableError) as exc:
            await byo.get_byo_mode_strict(
                T("strict-uncounted"), test_settings, client=cast(httpx.AsyncClient, blip)
            )
        # ⭐ The reason rides on the EXCEPTION too, so the close's own log line can
        # name the fault class without re-parsing the message.
        assert exc.value.reason == "http_error"
        assert seen == []


# --------------------------------------------------------------------------- #
# The batch path — the fan-out bound
# --------------------------------------------------------------------------- #


class TestVerdictMap:
    """``get_byo_mode_map`` / ``get_byo_mode_verdict_map`` over the whole estate."""

    async def test_map_projects_modes_and_keeps_verdicts(self, test_settings, monkeypatch):
        """Both batch forms answer about the same population; the string form is a
        projection of the verdict form, so the two cannot drift.
        """
        internal = T("map-internal")
        commercial = T("map-commercial")
        answers = {internal: "local_allowed", commercial: "byo_required"}

        real_get = httpx.AsyncClient.get

        async def _get(self, url, *a, **kw):  # noqa: ANN001, ANN202
            if "byo-mode" in str(url):
                for tid, mode in answers.items():
                    if tid in str(url):
                        return httpx.Response(200, json={"byo_mode": mode})
            return await real_get(self, url, *a, **kw)

        monkeypatch.setattr(httpx.AsyncClient, "get", _get)

        verdicts = await byo.get_byo_mode_verdict_map([internal, commercial], test_settings)
        assert {t: v.mode for t, v in verdicts.items()} == answers
        assert all(v.determined for v in verdicts.values())

        # The projection is served from the TTL cache the call above populated.
        assert await byo.get_byo_mode_map([internal, commercial], test_settings) == answers

    async def test_the_fan_out_is_bounded(self, test_settings, monkeypatch):
        """⚠️ THE N+1 HERE WAS NEVER SEQUENTIAL, AND THAT MATTERS FOR WHAT THIS
        ASSERTS. ``asyncio.gather`` over a comprehension starts every coroutine at
        once and ``httpx.AsyncClient``'s default pool allows 100 connections, so the
        previous code opened ~59 SIMULTANEOUS requests per aggregate render, from
        every replica rendering a dashboard — a self-inflicted thundering herd
        against the very service whose 503s this module has to launder, not 59 round
        trips back to back. So the fix is a CAP, not parallelism, and the assertion
        is on peak concurrency.

        ⚠️ MUTATION CONTROL: delete the ``async with sem`` in
        ``get_byo_mode_verdict_map`` and ``peak`` becomes 20 (== the tenant count),
        which is the pre-fix behaviour, and this fails.
        """
        tenants = [T(f"fanout-{i:02d}") for i in range(20)]
        assert len(tenants) > byo._MAP_CONCURRENCY, "the cap must be smaller than the population"

        state = {"in_flight": 0, "peak": 0}
        real_get = httpx.AsyncClient.get

        async def _get(self, url, *a, **kw):  # noqa: ANN001, ANN202
            if "byo-mode" not in str(url):
                return await real_get(self, url, *a, **kw)
            state["in_flight"] += 1
            state["peak"] = max(state["peak"], state["in_flight"])
            # Yield so every admitted coroutine is genuinely overlapping; without
            # this the loop could run each to completion and peak would read 1
            # whether or not the cap exists — a vacuous pass.
            await asyncio.sleep(0)
            state["in_flight"] -= 1
            return httpx.Response(200, json={"byo_mode": "local_allowed"})

        monkeypatch.setattr(httpx.AsyncClient, "get", _get)

        verdicts = await byo.get_byo_mode_verdict_map(tenants, test_settings)

        assert len(verdicts) == len(tenants)
        assert state["peak"] <= byo._MAP_CONCURRENCY, (
            f"{state['peak']} concurrent licences lookups from one render; the cap is "
            f"{byo._MAP_CONCURRENCY}"
        )
        assert state["peak"] > 1, (
            "the batch serialised completely — a cap of 1 would turn a render into "
            "59 real round trips, which is the failure mode the cap must not become"
        )

    async def test_an_empty_input_makes_no_call(self, test_settings):
        assert await byo.get_byo_mode_verdict_map([], test_settings) == {}
        assert await byo.get_byo_mode_map(["", ""], test_settings) == {}
