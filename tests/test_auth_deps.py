"""Unit tests for auth dependency functions.

Tenant identity became a real Postgres ``uuid`` in migration 0011, so
``require_tenant`` no longer echoes back whatever ``X-Tenant-Id`` happened to
carry: it resolves the header through :func:`tenancy.canonical_tenant_id` and
answers **400** to anything that does not resolve. Its return value is used
verbatim as a uuid WHERE/INSERT value downstream, so "what exactly comes back"
is the contract under test here, not merely "was the request allowed".

The cases below pin that contract one property at a time: every accepted
spelling normalises to the same bytes, a registered legacy NAME resolves to its
uuid and never survives as a name, and each rejected shape is checked for what
it must NOT put in the response body.
"""

import re

import pytest
from fastapi import Depends, FastAPI, HTTPException, Request
from httpx import ASGITransport, AsyncClient

from auth_deps import (
    enforce_tenant_isolation,
    require_admin,
    require_internal,
    require_pricing_admin,
    require_tenant,
)
from tests.conftest import T

# A tenant that is uuid-keyed from birth — the ordinary post-0011 shape. ``T``
# derives it deterministically from a readable label, so the wire value has the
# shape production carries while the test still says which tenant it means.
TENANT_UUID = T("acme-corp")
OTHER_TENANT_UUID = T("competitor-corp")

# One of the six registered legacy names, and the uuid ``licenses.tenants``
# actually holds for it. Written out as a literal rather than imported from
# ``tenancy.LEGACY_TENANT_NAMES``: asserting against the mapping under test
# would agree with any future edit to that mapping, including a wrong one.
LEGACY_TENANT_NAME = "n8n-dev"
LEGACY_TENANT_UUID = "019de842-c92a-7e37-8fdd-eb0d8c5396f9"

# Four names that ARE registered in ``licenses.tenants`` but were absent from the
# hand-picked six the map used to hold — so each of these got a hard 400 from
# this service while ``n8n-dev`` worked. That asymmetry was the defect.
#
# The uuids are literals measured against the registry on worker4 (2026-08-25),
# NOT read out of ``tenancy.LEGACY_TENANT_NAMES``. Importing the mapping and
# comparing it to itself would make this suite agree with any future edit to the
# map, including one that silently repoints a live tenant at someone else's data.
REGISTERED_TENANT_UUIDS = {
    "marketing": "019edc61-9495-76bc-b65f-c92a8e6880df",
    "sales": "019edc61-9495-7ee7-93dd-ea9e1f687f42",
    "platform-core": "0195f48b-d000-7000-8000-000000000002",
    "book-fulfillment": "01a1109c-f895-7766-ba28-6ea56e56a5f7",
}

# A tenant that was RETIRED from the registry. Migration 0011 preserves its
# historical rows under a derived uuid so the audit trail stays readable, but the
# boundary must refuse to let anyone write anything NEW under it.
RETIRED_TENANT_NAME = "gerard-dev"

# A name that was never registered at all — the ordinary typo/garbage case.
UNREGISTERED_TENANT_NAME = "not-a-tenant"

# The map is a snapshot of a registry that had 72 rows when it was taken. The
# floor is deliberately well below that (the registry grows, and pinning 72 would
# turn every new tenant into a red build) but far above the six it used to hold.
MIN_REGISTERED_TENANT_NAMES = 60

# The shape four production rows actually arrived in: a raw Authorization header
# pasted into X-Tenant-Id. Rejecting it is only half the requirement — the token
# must not come back out in the error body either.
LEAKED_TOKEN = "eyJhbGciOiJSUzI1NiJ9.c2VjcmV0LXBheWxvYWQ.c2lnbmF0dXJl"
BEARER_SHAPED_HEADER = f"Bearer {LEAKED_TOKEN}"

# A Keycloak SERVICE-ACCOUNT subject: also a uuid, and deliberately not any
# tenant's uuid. Kept distinct from TENANT_UUID so "the admin's own sub came
# back" is distinguishable from "the requested tenant came back" — an assertion
# written against TENANT_UUID could not tell those two apart.
ADMIN_SUBJECT_UUID = T("service-account-license-authz")


async def _get(app: FastAPI, url: str, headers: dict[str, str] | None = None):
    """One-shot ASGI GET against the test app."""
    transport = ASGITransport(app=app)
    async with AsyncClient(transport=transport, base_url="http://test") as client:
        return await client.get(url, headers=headers)


def _patch_admin_jwt(monkeypatch, sub: str | None) -> None:
    """Turn on auth and make the admin-JWT branch verify, returning ``sub``.

    ``sub=None`` produces a payload with NO ``sub`` claim at all (not a null
    one), which is the second of the two ways an admin token can fail to name a
    tenant.
    """
    import auth
    import auth_deps

    payload: dict = {"roles": ["license-admin"], "realm_access": {"roles": ["license-admin"]}}
    if sub is not None:
        payload["sub"] = sub

    async def _fake_verify(_request):
        return payload

    monkeypatch.setattr(auth, "DASHBOARD_AUTH_ENABLED", True)
    monkeypatch.setattr(auth_deps, "verify_admin_token", _fake_verify)


def _request_with_headers(**headers: str) -> Request:
    """Bare request for calling ``enforce_tenant_isolation`` directly.

    Underscores in kwarg names become hyphens (``x_tenant_id`` ->
    ``X-Tenant-Id``). Only ``.headers`` is ever read by the function under test.
    """
    raw = [(k.replace("_", "-").encode(), v.encode()) for k, v in headers.items()]
    return Request({"type": "http", "method": "GET", "path": "/", "headers": raw})


def _make_app() -> FastAPI:
    """Minimal FastAPI app wired with auth deps for testing."""
    app = FastAPI()

    @app.get("/admin")
    async def admin_endpoint(user: dict = Depends(require_admin)):
        return {"sub": user.get("sub")}

    @app.get("/tenant")
    async def tenant_endpoint(tenant: str = Depends(require_tenant)):
        return {"tenant_id": tenant}

    @app.get("/tenant/{tenant_id}")
    async def tenant_scoped_endpoint(
        request: Request,
        tenant_id: str,
        _tenant: str = Depends(require_tenant),
    ):
        enforce_tenant_isolation(request, tenant_id, _tenant)
        return {"tenant_id": tenant_id}

    @app.get("/internal")
    async def internal_endpoint(user: dict = Depends(require_internal)):
        return {"sub": user.get("sub")}

    @app.get("/pricing")
    async def pricing_endpoint(request: Request, user: dict = Depends(require_pricing_admin)):
        return {
            "sub": user.get("sub"),
            "actor_role": getattr(request.state, "audit_actor", {}).get("role"),
        }

    return app


@pytest.fixture
def test_app():
    return _make_app()


@pytest.mark.unit
class TestRequireAdmin:
    async def test_dev_mode_bypasses_auth(self, test_app):
        """With DASHBOARD_AUTH_ENABLED=false, returns dev payload."""
        transport = ASGITransport(app=test_app)
        async with AsyncClient(transport=transport, base_url="http://test") as client:
            resp = await client.get("/admin")
        assert resp.status_code == 200
        # The dev bypass mints a real tenant uuid, not the label "dev": once
        # require_tenant resolved `sub` through canonical_tenant_id(), a label
        # that a uuid column cannot store 400'd every admin-JWT route.
        assert resp.json()["sub"] != "dev"
        assert resp.json()["sub"] == T("dev")

    async def test_rejects_missing_token_when_enabled(self, test_app, monkeypatch):
        """With auth enabled, missing bearer token returns 401."""
        import auth

        monkeypatch.setattr(auth, "DASHBOARD_AUTH_ENABLED", True)
        transport = ASGITransport(app=test_app)
        async with AsyncClient(transport=transport, base_url="http://test") as client:
            resp = await client.get("/admin")
        assert resp.status_code == 401


@pytest.mark.unit
class TestRequireTenant:
    async def test_accepts_canonical_uuid_header(self, test_app):
        """A canonical lowercase uuid is accepted and returned unchanged.

        This is the reference result the spelling cases below compare against.
        """
        resp = await _get(test_app, "/tenant", {"X-Tenant-Id": TENANT_UUID})
        assert resp.status_code == 200, resp.text
        assert resp.json()["tenant_id"] == TENANT_UUID

    @pytest.mark.parametrize(
        "spelling",
        [
            pytest.param(TENANT_UUID.upper(), id="uppercase"),
            pytest.param("{" + TENANT_UUID + "}", id="braced"),
            pytest.param(TENANT_UUID.replace("-", ""), id="dashless"),
            pytest.param("urn:uuid:" + TENANT_UUID, id="urn"),
        ],
    )
    async def test_alternate_spellings_normalise_to_identical_bytes(self, test_app, spelling):
        """Every accepted spelling of ONE uuid returns byte-identical output.

        This is the property ``enforce_tenant_isolation`` rests on. If two
        spellings of the same id did not collapse to the same string, a customer
        who spelled their own tenant in upper case would be 403'd off their own
        data — and a WHERE clause built from the un-normalised value would miss
        rows that are in fact theirs.
        """
        assert spelling != TENANT_UUID, "spelling must differ from the canonical form"
        resp = await _get(test_app, "/tenant", {"X-Tenant-Id": spelling})
        assert resp.status_code == 200, resp.text
        assert resp.json()["tenant_id"] == TENANT_UUID

    async def test_registered_legacy_name_resolves_to_its_registered_uuid(self, test_app):
        """A registered legacy NAME is an input alias — the uuid is what comes back.

        The name must not survive into the return value: it is used directly as a
        uuid column value, where a name raises ``DataError``.
        """
        resp = await _get(test_app, "/tenant", {"X-Tenant-Id": LEGACY_TENANT_NAME})
        assert resp.status_code == 200, resp.text
        assert resp.json()["tenant_id"] == LEGACY_TENANT_UUID
        assert LEGACY_TENANT_NAME not in resp.text

    @pytest.mark.parametrize(
        ("name", "expected_uuid"),
        [pytest.param(n, u, id=n) for n, u in REGISTERED_TENANT_UUIDS.items()],
    )
    async def test_registered_name_outside_the_original_six_resolves(
        self, test_app, name, expected_uuid
    ):
        """A registered name the old six omitted resolves — it used to 400.

        ``LEGACY_TENANT_NAMES`` held six hand-picked slugs while the licences
        registry — the SAME source of truth migration 0011 reads for
        ``source='registry'`` — held 72. So ``n8n-dev`` worked and ``marketing``,
        ``sales``, ``platform-core`` and ``book-fulfillment`` were refused at the
        boundary despite being genuinely registered tenants. The shim was never
        the problem; being an arbitrary subset of the registry was.

        The name still must not survive into the answer: it is an input alias,
        and the uuid is the only thing a uuid column can hold.
        """
        resp = await _get(test_app, "/tenant", {"X-Tenant-Id": name})
        assert resp.status_code == 200, resp.text
        assert resp.json()["tenant_id"] == expected_uuid
        assert name not in resp.text

    async def test_retired_tenant_name_is_rejected_and_not_echoed(self, test_app):
        """``gerard-dev`` was retired from the registry and stays a 400.

        DO NOT "helpfully" re-add this name when regenerating the map. Migration
        0011 preserves its EXISTING rows under a derived uuid so the historical
        audit trail stays attributable — that is a read-side concession, not a
        licence to keep writing. Accepting the name here would let a caller
        create NEW rows under a tenant that no longer exists, which is exactly
        the pseudo-tenant accumulation this change set removes.

        The regeneration query in ``tenancy.py`` selects from the live registry,
        so a retired name cannot come back by accident — only by hand.
        """
        resp = await _get(test_app, "/tenant", {"X-Tenant-Id": RETIRED_TENANT_NAME})
        assert resp.status_code == 400, resp.text
        assert RETIRED_TENANT_NAME not in resp.text

    async def test_unregistered_name_is_rejected_and_not_echoed(self, test_app):
        """A name that was never in the registry is still a 400.

        The negative control for the four accepted names above: widening the map
        to the whole registry must not have widened it to "any slug at all". The
        shim resolves names that already exist; it never mints one.
        """
        resp = await _get(test_app, "/tenant", {"X-Tenant-Id": UNREGISTERED_TENANT_NAME})
        assert resp.status_code == 400, resp.text
        assert UNREGISTERED_TENANT_NAME not in resp.text

    async def test_unregistered_slug_is_rejected_and_not_echoed(self, test_app):
        """An unknown slug is a 400 — not a 401 (auth was fine) and not a 500.

        The value is not reflected back: this header is an untrusted, sometimes
        secret-bearing input (see the Bearer case), so nothing from it is echoed.
        """
        resp = await _get(test_app, "/tenant", {"X-Tenant-Id": "acme-corp"})
        assert resp.status_code == 400, resp.text
        assert "acme-corp" not in resp.text

    async def test_bearer_shaped_header_is_rejected_without_leaking_the_token(self, test_app):
        """A raw auth header in X-Tenant-Id: 400, and the token never comes back.

        Four production rows arrived in exactly this shape. A body that quoted
        the offending value to be helpful would hand the token to whoever reads
        the response or the client-side log.
        """
        resp = await _get(test_app, "/tenant", {"X-Tenant-Id": BEARER_SHAPED_HEADER})
        assert resp.status_code == 400, resp.text
        assert LEAKED_TOKEN not in resp.text
        # Also fails a truncated echo (e.g. a redactor that keeps a prefix).
        assert LEAKED_TOKEN[:16] not in resp.text
        assert "Bearer" not in resp.text

    @pytest.mark.parametrize(
        "blank",
        [
            pytest.param("", id="empty-string"),
            pytest.param("   ", id="whitespace-only"),
            pytest.param("\t", id="tab-only"),
        ],
    )
    async def test_present_but_empty_header_is_rejected_and_names_no_tenant(self, test_app, blank):
        """A header that is PRESENT but carries nothing is a 400, not "no header".

        ``license-authz`` sends exactly this shape: its ``main.py`` computes
        ``tenant_id = result.tenant_id or ""`` and forwards the empty string when
        token introspection returns no tenant. The value is falsy, so a
        ``if raw_header:`` test treats it as absent and falls through to the
        admin-JWT branch — see the companion case below for why that is a
        silent mis-attribution rather than a cosmetic status-code difference.

        The body must name no tenant at all: this 400 is the answer to "you told
        me nothing", so there is nothing to echo, and echoing a uuid here would
        hand the caller an id they never supplied.
        """
        resp = await _get(test_app, "/tenant", {"X-Tenant-Id": blank})
        assert resp.status_code == 400, resp.text
        assert TENANT_UUID not in resp.text
        assert LEGACY_TENANT_UUID not in resp.text
        assert ADMIN_SUBJECT_UUID not in resp.text
        # Nothing uuid-SHAPED at all, so the assertion cannot be satisfied by a
        # body quoting some other uuid the test does not happen to know about.
        assert not re.search(r"[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-", resp.text), resp.text

    async def test_empty_header_is_not_silently_resolved_to_the_admin_subject(
        self, test_app, monkeypatch
    ):
        """Empty header + a VALID admin JWT whose ``sub`` is a uuid → still 400.

        This is the case that makes the two blank-header cases above mean
        something. They pass in the buggy version too, because with no admin JWT
        the fall-through lands on ``verify_admin_token`` failing and produces a
        4xx anyway. Here the admin branch would SUCCEED: a Keycloak
        service-account ``sub`` is itself a uuid, so ``canonical_tenant_id(sub)``
        resolves, and the request is attributed to the CALLER'S OWN service
        account as though it were a tenant — the ``__admin__`` pseudo-tenant
        defect this change set deletes, re-entering through a different door.

        So the status code is only half the assertion: a 200 carrying
        ``ADMIN_SUBJECT_UUID`` is the actual bug, and it must not be the answer.
        """
        _patch_admin_jwt(monkeypatch, ADMIN_SUBJECT_UUID)
        resp = await _get(
            test_app,
            "/tenant",
            {"X-Tenant-Id": "", "Authorization": "Bearer fake"},
        )
        assert resp.status_code == 400, resp.text
        # The attributed tenant is NOT the admin subject — asserted on the body
        # rather than only on the status, because the defect is the ATTRIBUTION.
        assert resp.json().get("tenant_id") != ADMIN_SUBJECT_UUID
        assert ADMIN_SUBJECT_UUID not in resp.text

    async def test_accepts_admin_jwt_with_a_tenant_uuid_subject(self, test_app, monkeypatch):
        """No header + an admin JWT whose ``sub`` IS a tenant uuid → that tenant."""
        _patch_admin_jwt(monkeypatch, TENANT_UUID)
        resp = await _get(test_app, "/tenant", {"Authorization": "Bearer fake"})
        assert resp.status_code == 200, resp.text
        assert resp.json()["tenant_id"] == TENANT_UUID

    async def test_admin_jwt_subject_is_canonicalised_too(self, test_app, monkeypatch):
        """The JWT branch canonicalises as well — not only the header branch."""
        _patch_admin_jwt(monkeypatch, TENANT_UUID.upper())
        resp = await _get(test_app, "/tenant", {"Authorization": "Bearer fake"})
        assert resp.status_code == 200, resp.text
        assert resp.json()["tenant_id"] == TENANT_UUID

    async def test_genuinely_absent_header_still_takes_the_admin_path(self, test_app, monkeypatch):
        """Negative control for the empty-header 400: ABSENT still means admin.

        Identical to
        ``test_empty_header_is_not_silently_resolved_to_the_admin_subject`` in
        every respect except that ``X-Tenant-Id`` is not sent at all — so a 200
        here and a 400 there isolate "header present but blank" as the single
        cause. Without this the empty-header fix could not be distinguished from
        simply having broken the admin flow for everyone.
        """
        _patch_admin_jwt(monkeypatch, ADMIN_SUBJECT_UUID)
        resp = await _get(test_app, "/tenant", {"Authorization": "Bearer fake"})
        assert resp.status_code == 200, resp.text
        assert resp.json()["tenant_id"] == ADMIN_SUBJECT_UUID

    @pytest.mark.parametrize(
        "sub",
        [pytest.param("dev", id="sub-is-not-a-uuid"), pytest.param(None, id="sub-absent")],
    )
    async def test_rejects_admin_jwt_whose_subject_is_not_a_tenant(
        self, test_app, monkeypatch, sub
    ):
        """An admin JWT that names no tenant is a 400, distinct from the header 400.

        This is where the deleted ``payload.get("sub", "__admin__")`` fallback
        used to mint a pseudo-tenant that then owned real ``tenant_webhooks``
        rows. Authenticating as an admin is not the same as being a tenant; such
        a caller has to name the tenant explicitly.
        """
        _patch_admin_jwt(monkeypatch, sub)
        resp = await _get(test_app, "/tenant", {"Authorization": "Bearer fake"})
        assert resp.status_code == 400, resp.text
        assert "__admin__" not in resp.text
        # Distinguishable from the malformed-header 400 above: the caller is
        # told their *token* names no tenant, not that their header was junk.
        assert "Admin JWT subject is not a tenant UUID" in resp.json()["detail"]

    async def test_rejects_when_no_header_and_no_jwt(self, test_app, monkeypatch):
        """Without X-Tenant-Id and with auth enabled (no JWT), returns 401.

        401, not 400: here the caller genuinely has not authenticated, so
        re-authenticating is an action that can help them.
        """
        import auth

        monkeypatch.setattr(auth, "DASHBOARD_AUTH_ENABLED", True)
        resp = await _get(test_app, "/tenant")
        assert resp.status_code == 401


@pytest.mark.unit
class TestLegacyTenantNamesIsStructurallySound:
    """Properties the NAME→uuid map must hold however it is regenerated.

    The cases above pin five specific tenants. These pin the shape of the whole
    mapping, so a bad regeneration is caught even when it happens to leave those
    five alone. This is the class that has to survive the next person who reruns
    the query in ``tenancy.py`` and pastes the result in.
    """

    def test_every_value_is_a_canonical_uuid_string(self):
        """Each value goes into a Postgres ``uuid`` column verbatim.

        A value that does not parse is not a slow-burn data-quality problem: it
        is a ``DataError`` on the first WHERE clause that touches it, for a
        tenant whose name resolves "successfully". And a value that parses but
        is not in canonical lowercase-dashed form breaks the equality that
        ``enforce_tenant_isolation`` rests on — the header spelling normalises,
        the mapped one would not, and the tenant is 403'd off their own data.
        """
        import uuid as _uuid

        from tenancy import LEGACY_TENANT_NAMES

        for name, value in LEGACY_TENANT_NAMES.items():
            parsed = _uuid.UUID(value)  # raises ValueError on junk, naming the row
            assert str(parsed) == value, f"{name} maps to a non-canonical spelling: {value!r}"

    def test_no_two_names_map_to_the_same_uuid(self):
        """Distinct names must be distinct tenants.

        Two names sharing a uuid means two callers silently write into one
        tenant's rows and can read each other's data — a cross-tenant leak that
        no isolation check downstream can detect, because by then the two
        requests are genuinely the same tenant. A copy/paste slip while editing
        72 near-identical lines produces exactly this.

        A DECLARED rename alias is the one legitimate exception, and it is
        exempted by name rather than by loosening the rule: after licences #170
        renamed a row, the old and new spellings are two doors onto one tenant
        ON PURPOSE. Only the exact groups ``RENAMED_TENANT_ALIASES`` describes
        pass — a third name joining such a group, or any undeclared pair, still
        fails, which is the copy/paste slip this test exists to catch.
        """
        from tenancy import LEGACY_TENANT_NAMES, RENAMED_TENANT_ALIASES

        # The only permitted group per uuid: every name named by a DECLARED
        # alias pair for that uuid, on either side of the arrow. Built as a
        # union rather than seeded with {new_name}, because a row can be renamed
        # TWICE (…0003: platform-cp-3 -> george-dev-portal-realm -> aa-agents).
        # In such a chain the middle spelling is both a value and a key, and
        # seeding the group on first sight of the uuid dropped whichever end of
        # the chain was visited second. This stays exactly as strict: a name
        # that no declared pair mentions still collides.
        permitted: dict[str, set[str]] = {}
        for old_name, new_name in RENAMED_TENANT_ALIASES.items():
            permitted.setdefault(LEGACY_TENANT_NAMES[new_name], set()).update((old_name, new_name))

        by_uuid: dict[str, list[str]] = {}
        for name, value in LEGACY_TENANT_NAMES.items():
            by_uuid.setdefault(value, []).append(name)
        collisions = {
            v: names
            for v, names in by_uuid.items()
            if len(names) > 1 and set(names) != permitted.get(v)
        }
        assert not collisions, f"names sharing one tenant uuid: {collisions}"

    def test_no_key_is_itself_uuid_parseable(self):
        """A uuid-shaped KEY is unreachable, and worse than merely dead.

        ``canonical_tenant_id`` tries ``uuid.UUID(s)`` BEFORE consulting this
        map, so a key that parses as a uuid can never be looked up: the parse
        branch answers first and returns the string itself. If such a key were
        listed pointing at some OTHER uuid, the file would assert a mapping the
        code cannot perform — one string with two contradictory meanings, and
        the reader of the map gets the wrong one.

        The registry does contain such a row (a tenant whose ``name`` column is
        a uuid string). It is deliberately excluded by the ``not name ~ '^…'``
        predicate in the regeneration query. This test is what keeps it excluded
        when someone regenerates the map without that predicate.
        """
        import uuid as _uuid

        from tenancy import LEGACY_TENANT_NAMES

        def _parses(s: str) -> bool:
            try:
                _uuid.UUID(s)
            except ValueError, AttributeError, TypeError:
                return False
            return True

        unreachable = sorted(k for k in LEGACY_TENANT_NAMES if _parses(k))
        assert not unreachable, (
            "uuid-shaped keys are shadowed by the uuid parse branch and can "
            f"never be reached: {unreachable}"
        )

    def test_map_still_covers_the_registry_not_a_hand_picked_subset(self):
        """A floor, not an exact count — shrinking it re-breaks real tenants.

        The bug this change set fixes was the map being an arbitrary SUBSET of
        the registry: six names out of 72, so four real tenants got a 400 from
        this service. Any edit that takes it back toward a handful reintroduces
        that, silently, for whichever tenants fall out — the service keeps
        answering 400 and nothing looks broken from in here.

        Deliberately not ``== 72``: the registry grows, and a new tenant must
        not turn into a red build. The floor only has to sit far enough above
        six to make "somebody trimmed the list" impossible to miss.
        """
        from tenancy import LEGACY_TENANT_NAMES

        assert len(LEGACY_TENANT_NAMES) >= MIN_REGISTERED_TENANT_NAMES, (
            f"map holds {len(LEGACY_TENANT_NAMES)} names, below the "
            f"{MIN_REGISTERED_TENANT_NAMES} floor — it is a snapshot of the "
            "whole licences registry, not a hand-picked subset"
        )


#: The two rows licences #170 (6e195da, migration 024) renamed, as
#: OLD SPELLING -> NEW SPELLING -> the uuid BOTH must resolve to.
#:
#: Written out as literals on purpose. Reading these from
#: ``tenancy.RENAMED_TENANT_ALIASES`` would make the test agree with whatever
#: the map happens to say — including an empty map, which is precisely the
#: regression being guarded against. The uuids are copied from the licences
#: registry on worker4, not from ``LEGACY_TENANT_NAMES``.
RENAMED_REGISTRY_ROWS = [
    ("platform-cp-1", "n8n-dev-portal-realm", "0195f48b-d000-7000-8000-000000000001"),
    ("platform-cp-3", "george-dev-portal-realm", "0195f48b-d000-7000-8000-000000000003"),
    # The same …0003 row renamed AGAIN: george-dev-portal-realm -> aa-agents.
    # The alias ships BEFORE the registry UPDATE, so this row is red until the
    # pair is in tenancy.py and stays red if a regeneration drops it.
    ("george-dev-portal-realm", "aa-agents", "0195f48b-d000-7000-8000-000000000003"),
]


@pytest.mark.unit
class TestRenamedTenantNamesKeepResolving:
    """A registry rename must not turn into a 400 for callers on the old name.

    ``LEGACY_TENANT_NAMES`` is a SNAPSHOT of the licences registry, regenerated
    by pasting in the output of the psql query in ``tenancy.py``. That query
    returns the names the registry holds TODAY. licences #170 renamed
    ``…0001``/``…0003`` out of the ``platform-cp-N`` placeholders they were
    registered under in 2026-07-10's migration 014, so the next regeneration
    returns the new spellings and DROPS the old ones.

    That drop is silent and it is not cosmetic: an unresolvable name is a hard
    400 at the HTTP boundary (``auth_deps.resolve_tenant_or_400``), and both
    uuids are real portal-realm tenant ids that ``fr`` forwards verbatim out of
    the Keycloak ``tenant_id`` claim — i.e. the old spelling is in live traffic.
    Renaming a registry row is not a coordinated deploy of every caller.

    This class is the guard the comment in ``tenancy.py`` points at. Deleting
    the alias entries turns it RED.
    """

    @pytest.mark.parametrize(
        "old_name,new_name,tenant_uuid",
        RENAMED_REGISTRY_ROWS,
        ids=[old for old, _, _ in RENAMED_REGISTRY_ROWS],
    )
    def test_both_spellings_resolve_to_the_one_uuid(
        self, old_name: str, new_name: str, tenant_uuid: str
    ) -> None:
        """Old and new both resolve, and to the SAME tenant.

        Resolving to *something* is not enough: if the old spelling resolved to
        a different uuid the caller would be silently writing into another
        tenant's rows, which is worse than the 400 this prevents.
        """
        from tenancy import canonical_tenant_id

        assert canonical_tenant_id(new_name) == tenant_uuid, (
            f"{new_name!r} is the name the registry holds for {tenant_uuid} "
            "after licences #170; it must resolve"
        )
        assert canonical_tenant_id(old_name) == tenant_uuid, (
            f"{old_name!r} no longer resolves — a regeneration of "
            "LEGACY_TENANT_NAMES dropped the alias, and every caller still "
            f"sending {old_name!r} now takes a hard 400. Re-add it: see "
            "RENAMED_TENANT_ALIASES in tenancy.py"
        )

    @pytest.mark.parametrize(
        "old_name,new_name,tenant_uuid",
        RENAMED_REGISTRY_ROWS,
        ids=[old for old, _, _ in RENAMED_REGISTRY_ROWS],
    )
    def test_the_alias_declaration_matches_the_map(
        self, old_name: str, new_name: str, tenant_uuid: str
    ) -> None:
        """``RENAMED_TENANT_ALIASES`` must describe what the map actually does.

        The declaration is what exempts these pairs from
        ``test_no_two_names_map_to_the_same_uuid`` and what tells a regenerator
        which keys to re-add. A declaration that has drifted from the map grants
        an exemption for a collision nobody intended.
        """
        from tenancy import LEGACY_TENANT_NAMES, RENAMED_TENANT_ALIASES

        assert RENAMED_TENANT_ALIASES.get(old_name) == new_name, (
            f"{old_name!r} is not declared as an alias of {new_name!r}"
        )
        assert LEGACY_TENANT_NAMES.get(old_name) == tenant_uuid
        assert LEGACY_TENANT_NAMES.get(new_name) == tenant_uuid


@pytest.mark.unit
class TestEnforceTenantIsolation:
    async def test_customer_can_access_own_tenant(self, test_app):
        """Customer with X-Tenant-Id matching the path tenant succeeds."""
        resp = await _get(test_app, f"/tenant/{TENANT_UUID}", {"X-Tenant-Id": TENANT_UUID})
        assert resp.status_code == 200, resp.text
        assert resp.json()["tenant_id"] == TENANT_UUID

    @pytest.mark.parametrize(
        ("path_spelling", "header_spelling"),
        [
            pytest.param(TENANT_UUID.upper(), TENANT_UUID, id="path-uppercase"),
            pytest.param(TENANT_UUID, TENANT_UUID.upper(), id="header-uppercase"),
            pytest.param(TENANT_UUID.replace("-", ""), TENANT_UUID, id="path-dashless"),
            pytest.param(TENANT_UUID, TENANT_UUID.replace("-", ""), id="header-dashless"),
        ],
    )
    async def test_one_tenant_in_two_spellings_is_one_tenant(
        self, test_app, path_spelling, header_spelling
    ):
        """Same uuid, different spellings on each side → allowed, never 403.

        A raw string compare would lock a caller out of their own data for
        spelling their id in a form ``uuid.UUID`` accepts.
        """
        assert path_spelling != header_spelling
        resp = await _get(test_app, f"/tenant/{path_spelling}", {"X-Tenant-Id": header_spelling})
        assert resp.status_code == 200, resp.text

    async def test_customer_cannot_access_other_tenant(self, test_app):
        """Genuinely different tenants still get 403."""
        resp = await _get(test_app, f"/tenant/{OTHER_TENANT_UUID}", {"X-Tenant-Id": TENANT_UUID})
        assert resp.status_code == 403, resp.text

    async def test_admin_can_access_any_tenant(self, test_app, monkeypatch):
        """No X-Tenant-Id → admin JWT → isolation does not apply.

        The admin's own tenant is deliberately NOT the tenant in the path, so
        this proves the check was skipped rather than accidentally matched.
        """
        _patch_admin_jwt(monkeypatch, TENANT_UUID)
        resp = await _get(
            test_app, f"/tenant/{OTHER_TENANT_UUID}", {"Authorization": "Bearer fake"}
        )
        assert resp.status_code == 200, resp.text
        assert resp.json()["tenant_id"] == OTHER_TENANT_UUID

    def test_unresolvable_values_do_not_collapse_into_one_tenant(self):
        """Two DIFFERENT unresolvable ids must still mismatch.

        ``canonical_tenant_id`` returns ``None`` for both. If the comparison used
        those ``None``s, every pair of junk values would compare equal and this
        gate would pass any cross-tenant request made with a junk header.
        Exercised directly because ``require_tenant`` 400s junk before a route
        can reach here — the property still has to hold for every other caller
        of this function, which does its own canonicalisation.
        """
        request = _request_with_headers(x_tenant_id="junk-a")
        with pytest.raises(HTTPException) as exc_info:
            enforce_tenant_isolation(request, "junk-b", "junk-a")
        assert exc_info.value.status_code == 403
        # ...while identical unresolvable values are still the same tenant.
        enforce_tenant_isolation(request, "junk-a", "junk-a")

    def test_absent_tenant_header_skips_the_check_entirely(self):
        """No X-Tenant-Id means an admin caller: any path tenant is permitted."""
        enforce_tenant_isolation(_request_with_headers(), OTHER_TENANT_UUID, TENANT_UUID)


@pytest.mark.unit
class TestRequireInternal:
    async def test_dev_mode_bypasses_auth(self, test_app):
        """With DASHBOARD_AUTH_ENABLED=false, dev payload has license-admin role."""
        transport = ASGITransport(app=test_app)
        async with AsyncClient(transport=transport, base_url="http://test") as client:
            resp = await client.get("/internal")
        assert resp.status_code == 200
        # The dev bypass mints a real tenant uuid, not the label "dev": once
        # require_tenant resolved `sub` through canonical_tenant_id(), a label
        # that a uuid column cannot store 400'd every admin-JWT route.
        assert resp.json()["sub"] != "dev"
        assert resp.json()["sub"] == T("dev")

    async def test_rejects_missing_token_when_enabled(self, test_app, monkeypatch):
        """With auth enabled, missing JWT returns 401."""
        import auth

        monkeypatch.setattr(auth, "DASHBOARD_AUTH_ENABLED", True)
        transport = ASGITransport(app=test_app)
        async with AsyncClient(transport=transport, base_url="http://test") as client:
            resp = await client.get("/internal")
        assert resp.status_code == 401

    async def test_rejects_retired_metering_internal_role(self, test_app, monkeypatch):
        """Layer-2 step 3: `metering-internal` was retired 2026-07-16. A token
        bearing ONLY the old role (no billing-internal/license-admin) must now
        be REJECTED by require_internal — the secure-apps SAs mint
        billing-internal instead.
        """
        import auth
        import auth_deps

        async def _fake_decode(_request):
            return {
                "sub": "stale-sa",
                "realm_access": {"roles": ["metering-internal"]},
            }

        monkeypatch.setattr(auth, "DASHBOARD_AUTH_ENABLED", True)
        monkeypatch.setattr(auth_deps, "_decode_bearer_token", _fake_decode)
        transport = ASGITransport(app=test_app)
        async with AsyncClient(transport=transport, base_url="http://test") as client:
            resp = await client.get("/internal", headers={"Authorization": "Bearer fake"})
        assert resp.status_code == 403, resp.text

    async def test_accepts_billing_internal_role(self, test_app, monkeypatch):
        """Layer-2 rotation: a token bearing ONLY `billing-internal` (no
        license-admin) must be accepted by require_internal — this is what the
        secure-apps SAs mint after the metering-internal → billing-internal
        rotation.
        """
        import auth
        import auth_deps

        async def _fake_decode(_request):
            return {
                "sub": "licenses-sa",
                "realm_access": {"roles": ["billing-internal"]},
            }

        monkeypatch.setattr(auth, "DASHBOARD_AUTH_ENABLED", True)
        monkeypatch.setattr(auth_deps, "_decode_bearer_token", _fake_decode)
        transport = ASGITransport(app=test_app)
        async with AsyncClient(transport=transport, base_url="http://test") as client:
            resp = await client.get("/internal", headers={"Authorization": "Bearer fake"})
        assert resp.status_code == 200, resp.text
        assert resp.json()["sub"] == "licenses-sa"

    async def test_accepts_onboarding_internal_role(self, test_app, monkeypatch):
        """Onboarding SA tokens (onboarding-internal) are also accepted."""
        import auth
        import auth_deps

        async def _fake_decode(_request):
            return {
                "sub": "onboarding-sa",
                "realm_access": {"roles": ["onboarding-internal"]},
            }

        monkeypatch.setattr(auth, "DASHBOARD_AUTH_ENABLED", True)
        monkeypatch.setattr(auth_deps, "_decode_bearer_token", _fake_decode)
        transport = ASGITransport(app=test_app)
        async with AsyncClient(transport=transport, base_url="http://test") as client:
            resp = await client.get("/internal", headers={"Authorization": "Bearer fake"})
        assert resp.status_code == 200

    async def test_rejects_token_with_no_internal_role(self, test_app, monkeypatch):
        """A token without any INTERNAL_SERVICE_ROLES role gets 403."""
        import auth
        import auth_deps

        async def _fake_decode(_request):
            return {"sub": "user", "realm_access": {"roles": ["customer"]}}

        monkeypatch.setattr(auth, "DASHBOARD_AUTH_ENABLED", True)
        monkeypatch.setattr(auth_deps, "_decode_bearer_token", _fake_decode)
        transport = ASGITransport(app=test_app)
        async with AsyncClient(transport=transport, base_url="http://test") as client:
            resp = await client.get("/internal", headers={"Authorization": "Bearer fake"})
        assert resp.status_code == 403


@pytest.mark.unit
class TestRequireAdminVsInternalSeparateGate:
    """require_admin must remain a strict license-admin gate even after
    require_internal was widened. A `metering-internal`-only token must
    NOT be able to hit admin-only endpoints.
    """

    async def test_metering_internal_rejected_by_require_admin(self, test_app, monkeypatch):
        import auth

        async def _fake_decode(_request):
            return {
                "sub": "licenses-sa",
                "realm_access": {"roles": ["metering-internal"]},
            }

        monkeypatch.setattr(auth, "DASHBOARD_AUTH_ENABLED", True)
        # _decode_bearer_token lives in auth and is also called by verify_admin_token
        monkeypatch.setattr(auth, "_decode_bearer_token", _fake_decode)
        transport = ASGITransport(app=test_app)
        async with AsyncClient(transport=transport, base_url="http://test") as client:
            resp = await client.get("/admin", headers={"Authorization": "Bearer fake"})
        assert resp.status_code == 403, resp.text

    async def test_billing_internal_rejected_by_require_admin(self, test_app, monkeypatch):
        """The new billing-internal role must also be rejected by the strict
        require_admin gate (only license-admin passes /admin).
        """
        import auth

        async def _fake_decode(_request):
            return {
                "sub": "billing-sa",
                "realm_access": {"roles": ["billing-internal"]},
            }

        monkeypatch.setattr(auth, "DASHBOARD_AUTH_ENABLED", True)
        monkeypatch.setattr(auth, "_decode_bearer_token", _fake_decode)
        transport = ASGITransport(app=test_app)
        async with AsyncClient(transport=transport, base_url="http://test") as client:
            resp = await client.get("/admin", headers={"Authorization": "Bearer fake"})
        assert resp.status_code == 403, resp.text


@pytest.mark.unit
class TestRequirePricingAdmin:
    """require_pricing_admin gates the pricing-seed CronJob path. It must accept
    license-admin and billing-internal (the billing-service SA post-rotation),
    and reject everything else (incl. the retired metering-internal).
    This path had NO coverage before the Layer-2 rotation.
    """

    @pytest.mark.parametrize(
        ("role", "expected_actor"),
        [
            ("license-admin", "license-admin"),
            ("billing-internal", "billing-internal"),
        ],
    )
    async def test_accepts_pricing_roles(self, test_app, monkeypatch, role, expected_actor):
        import auth
        import auth_deps

        async def _fake_decode(_request):
            return {"sub": "billing-sa", "realm_access": {"roles": [role]}}

        monkeypatch.setattr(auth, "DASHBOARD_AUTH_ENABLED", True)
        monkeypatch.setattr(auth_deps, "_decode_bearer_token", _fake_decode)
        transport = ASGITransport(app=test_app)
        async with AsyncClient(transport=transport, base_url="http://test") as client:
            resp = await client.get("/pricing", headers={"Authorization": "Bearer fake"})
        assert resp.status_code == 200, resp.text
        assert resp.json()["actor_role"] == expected_actor

    async def test_rejects_non_pricing_role(self, test_app, monkeypatch):
        import auth
        import auth_deps

        async def _fake_decode(_request):
            return {"sub": "user", "realm_access": {"roles": ["onboarding-internal"]}}

        monkeypatch.setattr(auth, "DASHBOARD_AUTH_ENABLED", True)
        monkeypatch.setattr(auth_deps, "_decode_bearer_token", _fake_decode)
        transport = ASGITransport(app=test_app)
        async with AsyncClient(transport=transport, base_url="http://test") as client:
            resp = await client.get("/pricing", headers={"Authorization": "Bearer fake"})
        assert resp.status_code == 403, resp.text
