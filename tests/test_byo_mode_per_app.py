"""Per-PRODUCT ``byo_mode``: the override that stops a billed app billing $0.

⛔ THE DEFECT, AND WHY IT IS THE DANGEROUS DIRECTION. ``services.byo_mode`` asked
licences only for the TENANT-WIDE posture. A tenant whose tenant-wide mode is
``local_allowed`` or ``platform_only`` maps through ``effective_billing_mode`` to
``BILLING_MODE_INTERNAL``, and ``generate_billing`` then bills **$0 on every
line** and skips the entire access-charge block. CommonPractice is a commercial,
BILLED product that lives inside exactly such tenants — so its BYO markup,
platform fee and seat fee all silently evaluated to nothing, on invoices that
looked perfectly ordinary. Measured 2026-09-11: 49 of 59 unarchived tenants carry
an internal tenant-wide mode, so the population this zeroes is most of the
estate. An over-bill is reviewable on an invoice; this is the other one.

⭐ THE FALLBACK IS 404-ONLY, AND THAT IS THE WHOLE DESIGN. A 404 on the
per-product route is the NORMAL answer for every product a tenant has not
overridden, so it delegates to the tenant-wide posture. A 5xx/401/timeout is a
statement about the licences SERVICE and must NOT delegate: delegating would turn
"CommonPractice is byo_required but licences is unwell" into "the tenant-wide
posture says local_allowed, so bill $0" — an under-bill manufactured by an infra
blip and laundered through an answer that looks real.
``test_a_5xx_on_the_per_app_route_refuses_rather_than_falling_back`` is the one
test in this file that, if deleted, lets that back in.

Every test names its MUTATION TARGET: the edit in ``src/services/byo_mode.py``
that makes it fail. A test whose mutation target survives is not measuring
anything.
"""

from __future__ import annotations

from typing import cast

import httpx
import pytest

from service_auth import ServiceTokenProvider
from services import byo_mode as byo
from services.markup import BILLING_MODE_BYO_MARKUP_ONLY, BILLING_MODE_INTERNAL
from tests.conftest import T

pytestmark = pytest.mark.asyncio

APP = "commonpractice"


class _FakeResp:
    def __init__(self, status_code: int, payload: dict | None = None) -> None:
        self.status_code = status_code
        self._payload = payload if payload is not None else {}

    def json(self) -> dict:
        return self._payload


class _RoutedClient:
    """AsyncClient stub that answers the two byo-mode URLs differently.

    ⚠️ URL-AWARE ON PURPOSE, and this is the single most important property of
    the stub. The change under test adds a SECOND URL; a blanket stub answers
    both with the same object, so a resolver that ignored ``app_key`` entirely —
    the exact mutation these tests exist to catch — would still return the
    per-app payload and every assertion would pass. Each URL is answered
    separately and each is COUNTED, so "which route was actually asked" is
    assertable rather than inferred.
    """

    def __init__(
        self,
        *,
        app_resp: _FakeResp | Exception | None = None,
        tenant_resp: _FakeResp | Exception | None = None,
    ) -> None:
        self._app = app_resp
        self._tenant = tenant_resp
        self.app_calls = 0
        self.tenant_calls = 0
        #: Every request's headers, in order — a LIST, not a last-write dict, for
        #: the reason `tests/test_byo_mode.py::_OkClient` gives: a resolution can
        #: touch both routes, and grading only the final call lets a missing
        #: header on the first one pass.
        self.headers: list[dict | None] = []
        self.urls: list[str] = []

    async def get(self, url: str, **kwargs) -> _FakeResp:
        self.urls.append(str(url))
        self.headers.append(kwargs.get("headers"))
        if "/apps/" in str(url):
            self.app_calls += 1
            target = self._app
        else:
            self.tenant_calls += 1
            target = self._tenant
        if target is None:
            raise AssertionError(f"the stub was not given an answer for {url}")
        if isinstance(target, Exception):
            raise target
        return target


class _StubProvider:
    """A ``ServiceTokenProvider``-shaped stub with a token already in hand."""

    def __init__(self, token: str = "stub-jwt") -> None:  # pragma: allowlist secret
        self._token = token
        self.token_calls = 0

    async def get_token(self) -> str:
        self.token_calls += 1
        return self._token

    def build_headers(self, tenant_id: str) -> dict[str, str]:
        return {"X-Tenant-Id": tenant_id, "Authorization": f"Bearer {self._token}"}


def _as_client(stub: _RoutedClient) -> httpx.AsyncClient:
    """Type the URL-aware stub as the ``AsyncClient`` it duck-types."""
    return cast(httpx.AsyncClient, stub)


def _as_provider(stub: _StubProvider) -> ServiceTokenProvider:
    """Type the stub as the ``ServiceTokenProvider`` it duck-types."""
    return cast(ServiceTokenProvider, stub)


@pytest.fixture(autouse=True)
def _clear_byo_cache():
    """The cache is module state with a 5-min TTL — one test's answer must never
    decide another's. Cleared both sides so ordering cannot matter."""
    byo.clear_byo_cache()
    yield
    byo.clear_byo_cache()


# --------------------------------------------------------------------------- #
# The override, and the fallback
# --------------------------------------------------------------------------- #


class TestPerAppOverride:
    async def test_a_per_app_override_wins_over_the_tenant_wide_posture(self, test_settings):
        """⭐ THE HEADLINE BEHAVIOUR.

        ⚠️ MUTATION TARGET: delete the ``if app_key:`` block at the top of
        ``_resolve_byo_mode``. The tenant-wide ``local_allowed`` is then returned
        and this asserts ``byo_required``.
        """
        tenant = T("cp-tenant")
        client = _RoutedClient(
            app_resp=_FakeResp(200, {"byo_mode": "byo_required"}),
            tenant_resp=_FakeResp(200, {"byo_mode": "local_allowed"}),
        )
        mode = await byo.get_byo_mode(tenant, test_settings, app_key=APP, client=_as_client(client))
        assert mode == "byo_required"
        # The per-app route answered, so the tenant-wide one was never asked.
        assert client.app_calls == 1
        assert client.tenant_calls == 0
        assert f"/tenants/{tenant}/apps/{APP}/byo-mode" in client.urls[0]

    async def test_a_404_on_the_per_app_route_falls_back_to_the_tenant_wide_posture(
        self, test_settings
    ):
        """A 404 here means "no override for this product", NOT "unknown tenant".

        ⚠️ MUTATION TARGET: make the 404 arm of ``_resolve_app_override`` return
        ``"byo_required"`` instead of ``None`` — the tenant-wide route then never
        runs, ``tenant_calls`` is 0 and the mode is wrong for every unoverridden
        product in the estate.
        """
        tenant = T("no-override")
        client = _RoutedClient(
            app_resp=_FakeResp(404),
            tenant_resp=_FakeResp(200, {"byo_mode": "local_allowed"}),
        )
        mode = await byo.get_byo_mode(tenant, test_settings, app_key=APP, client=_as_client(client))
        assert mode == "local_allowed"
        assert client.app_calls == 1
        assert client.tenant_calls == 1

    async def test_a_200_with_no_byo_mode_in_the_body_is_not_an_override(self, test_settings):
        """``{}`` from the per-product route is "nothing set here", not a mode.

        The tenant-wide parse coerces a missing key to ``byo_required``, which is
        correct for THAT route (a tenant always has a posture). Copying that
        coercion here would bill every product commercially on the strength of an
        empty object.

        ⚠️ MUTATION TARGET: change ``str(resp.json().get("byo_mode") or "") or None``
        to the tenant-wide route's ``or "byo_required"`` — the fallback stops
        happening and this asserts ``local_allowed``.
        """
        tenant = T("empty-body")
        client = _RoutedClient(
            app_resp=_FakeResp(200, {}),
            tenant_resp=_FakeResp(200, {"byo_mode": "local_allowed"}),
        )
        mode = await byo.get_byo_mode(tenant, test_settings, app_key=APP, client=_as_client(client))
        assert mode == "local_allowed"
        assert client.tenant_calls == 1

    async def test_omitting_app_key_asks_only_the_tenant_wide_route(self, test_settings):
        """⭐ THE CONTROL. Every existing caller omits ``app_key``, so the claim
        that this change is inert for them has to be measured, not asserted in a
        comment. The per-app URL must never be touched.
        """
        tenant = T("legacy-caller")
        client = _RoutedClient(
            app_resp=_FakeResp(200, {"byo_mode": "byo_required"}),
            tenant_resp=_FakeResp(200, {"byo_mode": "platform_only"}),
        )
        mode = await byo.get_byo_mode(tenant, test_settings, client=_as_client(client))
        assert mode == "platform_only"
        assert client.app_calls == 0
        assert client.tenant_calls == 1
        # And the tenant-wide cache key is still the BARE STRING that
        # tests/test_byo_mode.py and tests/test_invoices.py assert on directly.
        assert byo._byo_cache.get(tenant) == "platform_only"


# --------------------------------------------------------------------------- #
# Fail-closed: a non-answer on the per-app route must not become an under-bill
# --------------------------------------------------------------------------- #


class TestPerAppFailsClosed:
    @pytest.mark.parametrize("status", [500, 502, 503])
    async def test_a_5xx_on_the_per_app_route_refuses_rather_than_falling_back(
        self, test_settings, status
    ):
        """⛔ THE ONE THAT MATTERS. Falling back on a 5xx reads the tenant-wide
        ``local_allowed`` and bills $0 for a product that is billed — an
        under-bill produced by an infra blip, indistinguishable from a correct
        classification. Since gitops #2558 licences readiness is wired to
        ``/health/ready``, so a database blip drops both replicas and istio
        answers 503: this is reachable, not theoretical.

        ⚠️ MUTATION TARGET: replace the ``raise`` at the end of
        ``_resolve_app_override`` with ``return None``. The strict resolver then
        returns ``local_allowed`` instead of raising, and the gate returns the
        tenant-wide value instead of the revenue-safe default.
        """
        tenant = T("blip")
        client = _RoutedClient(
            app_resp=_FakeResp(status),
            tenant_resp=_FakeResp(200, {"byo_mode": "local_allowed"}),
        )
        with pytest.raises(byo.ByoModeUnavailableError) as exc:
            await byo.get_byo_mode_strict(
                tenant, test_settings, app_key=APP, client=_as_client(client)
            )
        assert exc.value.reason == "http_error"
        # It refused INSTEAD of asking the tenant-wide route, not as well as.
        assert client.tenant_calls == 0
        # And nothing was cached, so a later real answer is not shadowed.
        assert (tenant, APP) not in byo._byo_cache
        assert tenant not in byo._byo_cache

    @pytest.mark.parametrize("status", [401, 403])
    async def test_a_per_app_auth_failure_is_labelled_auth_not_http_error(
        self, test_settings, status
    ):
        """401/403 is OUR credential, not their outage, and only the first is
        billing's to fix. Folding it into ``http_error`` makes the two
        indistinguishable on the one graph an operator watches.

        ⚠️ MUTATION TARGET: inline ``"http_error"`` in place of the
        ``_status_reason(...)`` call in ``_resolve_app_override``.
        """
        tenant = T("bad-token")
        client = _RoutedClient(
            app_resp=_FakeResp(status), tenant_resp=_FakeResp(200, {"byo_mode": "byo_required"})
        )
        with pytest.raises(byo.ByoModeUnavailableError) as exc:
            await byo.get_byo_mode_strict(
                tenant, test_settings, app_key=APP, client=_as_client(client)
            )
        assert exc.value.reason == "auth"

    async def test_a_per_app_timeout_is_labelled_timeout_and_the_gate_still_bills(
        self, test_settings
    ):
        """The LIVE GATE keeps its revenue-safe default on a per-app non-answer,
        exactly as it does on a tenant-wide one — an over-bill is reviewable, an
        under-bill is silent. The verdict says it was a guess.

        ⚠️ MUTATION TARGET: catch ``httpx.HTTPError`` before
        ``httpx.TimeoutException`` in ``_licences_get`` — ``reason`` becomes
        ``unavailable`` and the timeout label goes unreachable.
        """
        tenant = T("slow")
        client = _RoutedClient(
            app_resp=httpx.ReadTimeout("licences slow"),
            tenant_resp=_FakeResp(200, {"byo_mode": "local_allowed"}),
        )
        verdict = await byo.get_byo_mode_verdict(
            tenant, test_settings, app_key=APP, client=_as_client(client)
        )
        assert verdict.mode == "byo_required"  # revenue-safe default
        assert verdict.determined is False  # ...and it says so
        assert verdict.reason == "timeout"
        assert client.tenant_calls == 0


# --------------------------------------------------------------------------- #
# Caching: two key shapes, one cache
# --------------------------------------------------------------------------- #


class TestPerAppCaching:
    async def test_a_per_app_answer_is_cached_under_its_own_key(self, test_settings):
        """A second resolution makes NO request, and the per-app entry does not
        shadow or replace the tenant-wide one.

        ⚠️ MUTATION TARGET: delete the ``_byo_cache[key] = ...`` write in the 200
        arm of ``_resolve_app_override`` — ``app_calls`` becomes 2.
        """
        tenant = T("cached-app")
        client = _RoutedClient(
            app_resp=_FakeResp(200, {"byo_mode": "byo_required"}),
            tenant_resp=_FakeResp(200, {"byo_mode": "local_allowed"}),
        )
        assert await byo.get_byo_mode(
            tenant, test_settings, app_key=APP, client=_as_client(client)
        ) == ("byo_required")
        assert await byo.get_byo_mode(
            tenant, test_settings, app_key=APP, client=_as_client(client)
        ) == ("byo_required")
        assert client.app_calls == 1

        # ⭐ The per-app entry is keyed on the TUPLE and the tenant-wide entry on
        # the BARE STRING. The tenant-wide lookup is therefore still a miss, and
        # still answers with the tenant's own posture.
        assert byo._byo_cache.get((tenant, APP)) == "byo_required"
        assert tenant not in byo._byo_cache
        assert (
            await byo.get_byo_mode(tenant, test_settings, client=_as_client(client))
            == "local_allowed"
        )

    async def test_the_absence_of_an_override_is_cached_so_the_probe_is_not_repeated(
        self, test_settings
    ):
        """⛔ "No override" is an ANSWER and must be cached, or the per-app probe
        runs on every resolution — an extra round trip to licences per billed
        line, which is the thundering herd ``_MAP_CONCURRENCY`` exists to bound
        arriving through a different door. A bare ``None`` cannot express it
        because ``TTLCache.get`` already returns ``None`` for a miss; hence the
        ``_NO_APP_OVERRIDE`` sentinel.

        ⚠️ MUTATION TARGET: delete the ``_byo_cache[key] = _NO_APP_OVERRIDE``
        write in the 404 arm — ``app_calls`` becomes 2.
        """
        tenant = T("no-override-cached")
        client = _RoutedClient(
            app_resp=_FakeResp(404), tenant_resp=_FakeResp(200, {"byo_mode": "local_allowed"})
        )
        await byo.get_byo_mode(tenant, test_settings, app_key=APP, client=_as_client(client))
        await byo.get_byo_mode(tenant, test_settings, app_key=APP, client=_as_client(client))
        assert client.app_calls == 1
        # The tenant-wide answer is cached under its own key, so that route is
        # asked once too.
        assert client.tenant_calls == 1
        assert byo._byo_cache.get((tenant, APP)) == byo._NO_APP_OVERRIDE

    async def test_two_apps_in_one_tenant_do_not_share_an_entry(self, test_settings):
        """The cache key is ``(tenant, app)``. Keying it on the tenant alone would
        make the first product resolved decide the mode for every other one.

        ⚠️ MUTATION TARGET: change the key in ``_resolve_app_override`` to
        ``tenant_id`` — the second app reads the first's answer.
        """
        tenant = T("two-apps")
        cp = _RoutedClient(app_resp=_FakeResp(200, {"byo_mode": "byo_required"}))
        other = _RoutedClient(
            app_resp=_FakeResp(404), tenant_resp=_FakeResp(200, {"byo_mode": "platform_only"})
        )
        assert await byo.get_byo_mode(
            tenant, test_settings, app_key=APP, client=_as_client(cp)
        ) == ("byo_required")
        assert await byo.get_byo_mode(
            tenant, test_settings, app_key="kag", client=_as_client(other)
        ) == ("platform_only")
        assert other.app_calls == 1  # it really did ask about its own app


# --------------------------------------------------------------------------- #
# Wire contract + the verdict
# --------------------------------------------------------------------------- #


class TestPerAppWireContract:
    async def test_the_per_app_request_carries_the_service_token_and_tenant_header(
        self, test_settings
    ):
        """The header this module spent its whole life without must reach the NEW
        route too. Four sibling call sites each had it retro-fitted in production
        after the damage; a second URL that quietly forgot it would be the fifth.

        ⚠️ MUTATION TARGET: build the request inline in ``_resolve_app_override``
        with a bare ``client.get(url)`` instead of routing through
        ``_licences_get``.
        """
        tenant = T("hdrs")
        provider = _StubProvider()
        client = _RoutedClient(app_resp=_FakeResp(200, {"byo_mode": "byo_required"}))
        await byo.get_byo_mode(
            tenant,
            test_settings,
            app_key=APP,
            client=_as_client(client),
            token_provider=_as_provider(provider),
        )
        assert client.headers[0] == {
            "X-Tenant-Id": tenant,
            "Authorization": "Bearer stub-jwt",  # pragma: allowlist secret
        }
        # `get_token()` is awaited BEFORE `build_headers`, which only attaches an
        # ALREADY-CACHED token — the same ordering the tenant-wide route uses.
        assert provider.token_calls == 1

    async def test_a_per_app_answer_is_determined_and_the_fallback_is_too(self, test_settings):
        """``determined`` means "licences made a statement", and both shapes are
        statements: the product's own override, and "no override" plus a
        tenant-wide classification.
        """
        tenant = T("verdicts")
        direct = _RoutedClient(app_resp=_FakeResp(200, {"byo_mode": "byo_required"}))
        v1 = await byo.get_byo_mode_verdict(
            tenant, test_settings, app_key=APP, client=_as_client(direct)
        )
        assert (v1.mode, v1.determined, v1.reason) == ("byo_required", True, None)

        byo.clear_byo_cache()
        fell_back = _RoutedClient(
            app_resp=_FakeResp(404), tenant_resp=_FakeResp(200, {"byo_mode": "local_allowed"})
        )
        v2 = await byo.get_byo_mode_verdict(
            tenant, test_settings, app_key=APP, client=_as_client(fell_back)
        )
        assert (v2.mode, v2.determined, v2.reason) == ("local_allowed", True, None)

    async def test_a_per_app_non_answer_is_counted_as_a_laundering(
        self, test_settings, monkeypatch
    ):
        """The guess that reaches money is counted, on the per-app route as on the
        tenant-wide one. An uncounted guess is an estate-wide blip presenting as
        49 plausible classifications.

        ⚠️ MUTATION TARGET: remove the ``_count_indeterminate(reason)`` call from
        ``get_byo_mode_verdict``'s except arm.
        """
        seen: list[str] = []
        monkeypatch.setattr(byo, "_count_indeterminate", seen.append)
        client = _RoutedClient(app_resp=_FakeResp(503))
        await byo.get_byo_mode_verdict(
            T("counted"), test_settings, app_key=APP, client=_as_client(client)
        )
        assert seen == ["http_error"]


# --------------------------------------------------------------------------- #
# The money consequence
# --------------------------------------------------------------------------- #


class TestTheBillingConsequence:
    async def test_a_per_app_byo_required_tenant_bills_markup_while_tenant_wide_is_internal(
        self, test_settings
    ):
        """⭐ THE DEFECT, STATED AS MONEY.

        A CommonPractice tenant configured ``byo_markup_only`` whose TENANT-WIDE
        posture is ``local_allowed`` resolved to ``BILLING_MODE_INTERNAL`` — every
        line $0, the whole access-charge block skipped, no platform fee, no seat
        fee, no markup. Asking the per-product question instead keeps the
        configured commercial mode, so the BYO markup is actually charged.

        ⚠️ MUTATION TARGET: drop ``app_key=`` from the ``get_byo_mode`` call below
        (which is exactly what ``services.billing.generate_billing`` still does
        today) and ``effective_billing_mode`` returns ``internal``.

        ⚠️ THIS ASSERTS THE RESOLVER, NOT THE INVOICE. ``generate_billing`` owns
        the call site and is not in this change's scope; until it passes the
        line's ``service_key`` the fleet behaviour is unchanged. Said plainly
        because "the resolver supports it" and "invoices use it" are different
        claims.
        """
        tenant = T("cp-in-internal-tenant")
        client = _RoutedClient(
            app_resp=_FakeResp(200, {"byo_mode": "byo_required"}),
            tenant_resp=_FakeResp(200, {"byo_mode": "local_allowed"}),
        )
        configured = BILLING_MODE_BYO_MARKUP_ONLY

        per_app = await byo.get_byo_mode(
            tenant, test_settings, app_key=APP, client=_as_client(client)
        )
        assert byo.effective_billing_mode(configured, per_app) == BILLING_MODE_BYO_MARKUP_ONLY

        # The control: the old, tenant-wide-only question on the SAME tenant.
        byo.clear_byo_cache()
        tenant_wide = await byo.get_byo_mode(tenant, test_settings, client=_as_client(client))
        assert byo.effective_billing_mode(configured, tenant_wide) == BILLING_MODE_INTERNAL
