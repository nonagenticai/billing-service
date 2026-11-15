"""A tenant NAME shared by two licences tenants resolves to NO id — never a pick.

Licences tenant names became unique per OWNING APP (``owner_key``) instead of
globally: commonpractice and fr may each own a tenant called ``netcare``. A bare
name then does not identify a tenant. Resolving it to either uuid would land one
customer's spend on another customer's ledger, so billing answers ``None``, logs
a warning and counts it on ``billing_tenant_name_ambiguous_total{name}``.

The end-to-end test matters most: before this change the refresh folded the
uuid-keyed registry into a ``{name: uuid}`` dict, which last-one-won the clash
BEFORE the installer's ambiguity check could see it.
"""

from __future__ import annotations

from unittest.mock import AsyncMock, MagicMock, patch

import pytest

import tenancy
from tenancy import (
    LEGACY_TENANT_NAMES,
    canonical_tenant_id,
    is_registered_tenant_id,
    live_tenant_label,
    reset_live_tenant_registry,
    resolve_tenant_name_to_id,
    set_live_tenant_registry,
)

pytestmark = pytest.mark.unit

CP_NETCARE = "01a0bbbb-0000-7000-8000-0000000000b1"
FR_NETCARE = "01a0bbbb-0000-7000-8000-0000000000b2"
UNIQUE_ID = "01a0bbbb-0000-7000-8000-0000000000b3"


@pytest.fixture(autouse=True)
def _clean_overlay():
    from services.tenant_names import reset_cache

    reset_live_tenant_registry()
    reset_cache()
    yield
    reset_live_tenant_registry()
    reset_cache()


def _ambiguous_count(name: str) -> float:
    from observability.metrics import tenant_name_ambiguous_total

    return tenant_name_ambiguous_total.labels(name=name)._value.get()


def _resolution_count(source: str) -> float:
    from observability.metrics import tenant_name_resolution_total

    return tenant_name_resolution_total.labels(source=source)._value.get()


def test_a_unique_name_resolves() -> None:
    set_live_tenant_registry([("netcare", CP_NETCARE), ("acme", UNIQUE_ID)])
    assert resolve_tenant_name_to_id("netcare") == CP_NETCARE
    assert resolve_tenant_name_to_id("acme") == UNIQUE_ID


def test_an_ambiguous_name_resolves_to_none_and_is_counted(monkeypatch) -> None:
    set_live_tenant_registry(
        [("netcare", CP_NETCARE), ("netcare", FR_NETCARE), ("acme", UNIQUE_ID)]
    )
    before = _ambiguous_count("netcare")
    before_src = _resolution_count("ambiguous")
    # Captured on the module logger itself, not via caplog: other suites in the run
    # reconfigure logging propagation, which makes caplog order-dependent.
    warnings: list[str] = []
    monkeypatch.setattr(tenancy.logger, "warning", lambda msg, *a, **_k: warnings.append(msg % a))

    assert resolve_tenant_name_to_id("netcare") is None
    assert canonical_tenant_id("netcare") is None

    assert _ambiguous_count("netcare") - before == 2
    assert _resolution_count("ambiguous") - before_src == 2
    assert any("more than one live licences tenant" in w for w in warnings)
    # The unique name in the same install is unaffected.
    assert resolve_tenant_name_to_id("acme") == UNIQUE_ID


def test_both_ambiguous_uuids_stay_registered_and_labelled() -> None:
    """Only NAME -> id is unanswerable; each uuid is still a known tenant."""
    set_live_tenant_registry([("netcare", CP_NETCARE), ("netcare", FR_NETCARE)])
    assert is_registered_tenant_id(CP_NETCARE)
    assert is_registered_tenant_id(FR_NETCARE)
    assert live_tenant_label(CP_NETCARE) == "netcare"
    assert live_tenant_label(FR_NETCARE) == "netcare"


def test_an_ambiguous_name_does_not_fall_through_to_the_snapshot() -> None:
    """The snapshot's uuid would be a pick too — just an older one."""
    legacy_name, legacy_id = next(iter(LEGACY_TENANT_NAMES.items()))
    assert legacy_id != FR_NETCARE
    set_live_tenant_registry([(legacy_name, legacy_id), (legacy_name, FR_NETCARE)])
    assert resolve_tenant_name_to_id(legacy_name) is None


def test_uuid_input_is_unaffected_by_an_ambiguous_name() -> None:
    set_live_tenant_registry([("netcare", CP_NETCARE), ("netcare", FR_NETCARE)])
    before = _ambiguous_count("netcare")
    assert canonical_tenant_id(CP_NETCARE) == CP_NETCARE
    assert canonical_tenant_id(FR_NETCARE.upper()) == FR_NETCARE
    assert _ambiguous_count("netcare") == before


def test_ambiguity_clears_when_the_registry_no_longer_lists_the_clash() -> None:
    set_live_tenant_registry([("netcare", CP_NETCARE), ("netcare", FR_NETCARE)])
    assert resolve_tenant_name_to_id("netcare") is None
    set_live_tenant_registry([("netcare", FR_NETCARE)])
    assert resolve_tenant_name_to_id("netcare") == FR_NETCARE


def test_the_counter_failing_does_not_break_resolution(monkeypatch) -> None:
    import observability.metrics as metrics

    class _Exploding:
        def labels(self, **_kw):
            raise RuntimeError("boom")

    monkeypatch.setattr(metrics, "tenant_name_ambiguous_total", _Exploding())
    set_live_tenant_registry([("netcare", CP_NETCARE), ("netcare", FR_NETCARE)])
    assert resolve_tenant_name_to_id("netcare") is None


def _client(payload: list) -> AsyncMock:
    mock_client = AsyncMock()
    resp = MagicMock()
    resp.status_code = 200
    resp.json.return_value = payload
    mock_client.get = AsyncMock(return_value=resp)
    mock_client.__aenter__ = AsyncMock(return_value=mock_client)
    mock_client.__aexit__ = AsyncMock(return_value=False)
    return mock_client


async def test_refresh_sees_a_clash_across_owners_end_to_end() -> None:
    """Through the real fetch: two owners, one name ⇒ no id, owner_key kept."""
    from services.tenant_names import get_tenant_info, refresh_tenant_registry
    from settings import Settings

    payload = [
        {"id": CP_NETCARE, "name": "netcare", "owner_key": "commonpractice"},
        {"id": FR_NETCARE, "name": "netcare", "owner_key": "fr", "external_ref": "netcare"},
        {"id": UNIQUE_ID, "name": "acme", "owner_key": "platform"},
    ]
    settings = Settings(licenses_url="https://licenses.example.com")
    with patch("services.tenant_names.httpx.AsyncClient", return_value=_client(payload)):
        installed = await refresh_tenant_registry(settings, None)
        info = await get_tenant_info(settings, None)

    assert installed == 1  # only "acme" is a resolvable name
    assert resolve_tenant_name_to_id("netcare") is None
    assert resolve_tenant_name_to_id("acme") == UNIQUE_ID
    assert is_registered_tenant_id(CP_NETCARE) and is_registered_tenant_id(FR_NETCARE)
    assert info[CP_NETCARE].owner_key == "commonpractice"
    assert info[FR_NETCARE].owner_key == "fr"
    assert info[FR_NETCARE].external_ref == "netcare"
    assert info[UNIQUE_ID].external_ref is None
    assert tenancy._live_ambiguous_names == frozenset({"netcare"})


def test_parse_rows_tolerates_missing_or_bad_owner_fields() -> None:
    from services.tenant_names import parse_tenant_rows

    rows = parse_tenant_rows(
        [
            {"id": CP_NETCARE, "name": "a"},
            {"id": FR_NETCARE, "name": "b", "owner_key": 7, "external_ref": "  "},
        ]
    )
    assert rows[CP_NETCARE].owner_key is None and rows[CP_NETCARE].external_ref is None
    assert rows[FR_NETCARE].owner_key is None and rows[FR_NETCARE].external_ref is None
