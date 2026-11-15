"""An ARCHIVED tenant is nameable, and a missing name is never the uuid.

Two live defects, one contract. ``services._parsers.stored_tenant_name`` decided
on 2026-09-06 that ``""`` is the canonical "the registry could not resolve a
name" value for the ``tenant_name`` column family, and that a uuid must NEVER be
written there because it is non-empty, distinct per tenant, and therefore
impersonates a name to every downstream truthiness guard.

Both halves of that contract were still being broken on 2026-09-09:

1. ``services.tenant_names`` asked ``GET /internal/tenants`` WITHOUT
   ``include_archived``, and licences excludes archived tenants by default
   (``include_archived: bool = Query(False)``, confirmed on the live pod's
   ``/openapi.json``). 17 of 76 tenants are archived, so for every one of them
   the resolver had nothing to offer and ``stored_tenant_name`` — correctly
   refusing to store the uuid — wrote ``""`` forever. Measured victim:
   ``rmt-e2e``, listed by licences since 2026-08-11, archived 2026-09-03, ~300
   billing rows carrying ``""`` through 09-09. The name was always there; it was
   the QUESTION that was wrong.
2. ``services.cost_flusher``'s ``token_usage`` rollup defaulted its name lookup
   to the uuid, which is the one value the contract forbids.

⚠️ What these tests do NOT claim. Defect 2's default is unreachable in
production today — ``tenant_names`` is filled from the same ``survived`` loop
that fills ``token_counts``, so the key is always present. That is precisely why
the lookup was extracted into :func:`services.cost_flusher.rollup_tenant_name`
and is asserted here directly: an end-to-end flush test would pass identically
before and after the fix and would prove nothing about it.
"""

from __future__ import annotations

from typing import TYPE_CHECKING, cast
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from services.cost_flusher import rollup_tenant_name
from services.tenant_names import get_tenant_info, reset_cache

if TYPE_CHECKING:
    from settings import Settings

pytestmark = pytest.mark.unit

ARCHIVED_TENANT_ID = "019fb1f4-0000-7000-8000-00000000645f"
LIVE_TENANT_ID = "019fb1f4-0000-7000-8000-0000000064aa"


@pytest.fixture(autouse=True)
def _reset_tenant_name_cache():
    """The module cache holds ONE entry for 5 minutes, tests included.

    Without this the first test's answer is served to the second, which would
    then assert against a payload it never sent.
    """
    reset_cache()
    yield
    reset_cache()


class _Settings:
    licenses_url = "http://licenses.licenses.svc.cluster.local:8544"


_TRUTHY = {"true", "1", "yes", "t"}


def _mock_licenses_client(tenants: list[dict[str, object]]) -> MagicMock:
    """An ``httpx.AsyncClient`` stand-in for ``GET /internal/tenants``.

    ⛔ THIS MOCK HONOURS ``params``, AND THAT IS THE WHOLE POINT.

    It used to return ``tenants`` verbatim on every call, whatever was asked.
    Under that mock the two archived-tenant tests below could not fail for the
    reason they name: delete ``params=_TENANT_QUERY`` from
    ``services.tenant_names`` and they still passed, because the stand-in handed
    back the archived row regardless. Only the wire assertion bit, so a suite of
    three had one real test in it.

    Here the mock behaves like the real endpoint does: ``include_archived``
    defaults to false (``include_archived: bool = Query(False)`` on the live
    licences pod, read 2026-09-09) and rows carrying ``"archived": True`` are
    WITHHELD unless the caller asks for them. Every test below now fails if the
    parameter stops being sent — proven by deleting it and watching them go red.
    """
    client = AsyncMock()

    async def _get(_url: str, **kwargs: object) -> MagicMock:
        params = cast("dict[str, object]", kwargs.get("params") or {})
        asked = str(dict(params).get("include_archived", "")).strip().lower()
        include_archived = asked in _TRUTHY
        visible = [t for t in tenants if include_archived or not t.get("archived")]
        response = MagicMock()
        response.status_code = 200
        response.json.return_value = visible
        return response

    client.get = AsyncMock(side_effect=_get)
    client.__aenter__ = AsyncMock(return_value=client)
    client.__aexit__ = AsyncMock(return_value=False)
    return client


class TestArchivedTenantsAreRequested:
    async def test_fetch_asks_for_archived_tenants(self) -> None:
        """The request carries ``include_archived=true``.

        ⛔ This asserts the WIRE, not the result, on purpose. The parameter is
        the whole fix, and a server that ignores an unknown filter param answers
        200 with a plausible body either way — so a test that only inspected the
        returned map would pass against a misspelled parameter name. The
        spelling asserted here is the one the LIVE licences pod declares in
        ``/openapi.json`` (``{"name": "include_archived", "in": "query",
        "schema": {"type": "boolean", "default": false}}``, read 2026-09-09).
        """
        client = _mock_licenses_client([])
        with patch("services.tenant_names.httpx.AsyncClient", return_value=client):
            await get_tenant_info(cast("Settings", _Settings()))

        assert client.get.await_count == 1
        assert client.get.await_args.kwargs["params"] == {"include_archived": "true"}

    async def test_an_archived_tenant_resolves_to_its_real_name(self) -> None:
        """DEFECT 1, end to end through the resolver.

        ``rmt-e2e`` is archived. With the flag it is in the payload, so it
        resolves — and its name is a name, not the uuid and not ``""``.

        The stand-in WITHHOLDS archived rows unless asked, exactly as the live
        endpoint does, so dropping ``include_archived`` makes this fail with
        ``KeyError: '019fb1f4-…-645f'`` rather than passing on a mock that never
        looked at the request.
        """
        client = _mock_licenses_client(
            [
                {
                    "id": ARCHIVED_TENANT_ID,
                    "name": "rmt-e2e",
                    "kind": "tests",
                    "cost_centre": None,
                    "archived": True,
                },
                {
                    "id": LIVE_TENANT_ID,
                    "name": "n8n-dev",
                    "kind": "customers",
                    "cost_centre": "customers",
                    "archived": False,
                },
            ]
        )
        with patch("services.tenant_names.httpx.AsyncClient", return_value=client):
            info = await get_tenant_info(cast("Settings", _Settings()))

        assert info[ARCHIVED_TENANT_ID].name == "rmt-e2e"
        assert info[ARCHIVED_TENANT_ID].name != ARCHIVED_TENANT_ID
        # The live tenant is unaffected: widening the question must not narrow
        # the answer.
        assert info[LIVE_TENANT_ID].name == "n8n-dev"

    async def test_archived_only_payload_is_not_special_cased(self) -> None:
        """NEGATIVE CONTROL for the widening.

        Nothing downstream filters on ``archived``. If a future change starts
        dropping archived rows again, this fails alongside the test above rather
        than leaving one of them to pass on its own. With the params-honouring
        stand-in the payload is archived-ONLY, so without the flag the resolver
        sees an empty list and this asserts ``[] == [ARCHIVED_TENANT_ID]``.
        """
        client = _mock_licenses_client(
            [{"id": ARCHIVED_TENANT_ID, "name": "rmt-e2e", "archived": True}]
        )
        with patch("services.tenant_names.httpx.AsyncClient", return_value=client):
            info = await get_tenant_info(cast("Settings", _Settings()))

        assert list(info) == [ARCHIVED_TENANT_ID]


class TestRollupTenantNameNeverWritesTheUuid:
    def test_an_unresolved_tenant_falls_back_to_the_absence_marker(self) -> None:
        """DEFECT 2. The default is ``""`` — the one value that asserts nothing.

        Fails against the previous ``tenant_names.get(tenant_id, tenant_id)``,
        which returned the uuid.
        """
        assert rollup_tenant_name({}, LIVE_TENANT_ID) == ""

    def test_the_uuid_is_never_the_fallback(self) -> None:
        assert rollup_tenant_name({}, LIVE_TENANT_ID) != LIVE_TENANT_ID

    def test_a_resolved_name_is_passed_through_verbatim(self) -> None:
        """The fix must not become "always empty" — this is its control."""
        assert rollup_tenant_name({LIVE_TENANT_ID: "n8n-dev"}, LIVE_TENANT_ID) == "n8n-dev"

    def test_a_stored_empty_name_stays_empty(self) -> None:
        """``""`` in the map came from ``stored_tenant_name`` and is already an
        answer — it is not re-derived, and certainly not upgraded to the uuid."""
        assert rollup_tenant_name({LIVE_TENANT_ID: ""}, LIVE_TENANT_ID) == ""
