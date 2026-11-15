"""The budget write gate re-checks licences BEFORE refusing a just-created tenant.

THE DEFECT (prod, 2026-10-13)
-----------------------------
CommonPractice creates a tenant in licences, then writes its budget. The write
gate (``tenancy.write_gate_refuses_tenant``) reads an in-memory overlay that is
refreshed every 300s, and the demand refresh it schedules is fire-and-forget, so
the verdict for THAT request was the stale overlay's: 422
``budget_write_refused_unregistered_tenant`` for up to five minutes.

The fix awaits ONE refresh on a miss (licences has no single-tenant lookup, so
it is a full one) and rate-limits it. These tests pin:

* miss -> refresh finds the id -> the write is ALLOWED;
* miss -> refresh does not find it -> REFUSED;
* refresh raises / times out / installs nothing -> REFUSED (fail closed);
* per-id negative cache and the global floor bound licences traffic, and
  concurrent misses share one fetch;
* nothing that would not be refused pays for a refresh.

MUTATION THAT TURNS THIS RED: in ``budget_crud._require_registered_tenant`` call
``write_gate_refuses_tenant`` (the old, refresh-free predicate) again — the
"found" tests get ``UnregisteredTenantError``.
"""

from __future__ import annotations

import asyncio
import uuid

import pytest

import tenancy
from observability.metrics import tenant_registry_miss_refresh_total
from services.budget import UnregisteredTenantError, create_budget

pytestmark = pytest.mark.unit

MAX_AGE = 900.0


@pytest.fixture(autouse=True)
def _clean_overlay():
    tenancy.reset_live_tenant_registry()
    yield
    tenancy.reset_live_tenant_registry()


class FakeLicences:
    """A refresh hook standing in for ``refresh_tenant_registry``.

    ``registry`` is what licences "holds"; each call installs it into the
    overlay exactly as the real refresh does, and returns the installed count.
    """

    def __init__(self, registry: dict[str, str]) -> None:
        self.registry = dict(registry)
        self.calls = 0
        self.raises: Exception | None = None
        self.delay = 0.0

    def hook(self):
        async def _refresh() -> int:
            self.calls += 1
            if self.delay:
                await asyncio.sleep(self.delay)
            if self.raises is not None:
                raise self.raises
            return tenancy.set_live_tenant_registry(self.registry)

        return _refresh()


def _fresh_overlay_and_licences(**extra: str) -> FakeLicences:
    """A fresh overlay that does NOT know the new tenant, and a licences that
    (after ``extra`` is added) does — the 'created seconds ago' state."""
    anchor = {"gate-fresh-anchor": str(uuid.uuid4())}
    tenancy.set_live_tenant_registry(anchor)
    fake = FakeLicences({**anchor, **extra})
    tenancy.set_registry_refresh_hook(fake.hook)
    return fake


def _count(result: str) -> float:
    return tenant_registry_miss_refresh_total.labels(result=result)._value.get()


# --------------------------------------------------------------------------- #
# The verdicts
# --------------------------------------------------------------------------- #


async def test_a_tenant_created_since_the_last_tick_is_allowed_after_one_refresh(db_session):
    new_tenant = str(uuid.uuid4())
    fake = _fresh_overlay_and_licences(**{"Just Created BV": new_tenant})
    before = _count("found")

    budget = await create_budget(tenant_id=new_tenant, budget_tokens=10_000, session=db_session)

    assert budget.tenant_id == new_tenant
    assert fake.calls == 1
    assert _count("found") == before + 1


async def test_an_id_licences_does_not_have_is_still_refused(db_session):
    fake = _fresh_overlay_and_licences()
    unknown = str(uuid.uuid4())
    before = _count("absent")

    with pytest.raises(UnregisteredTenantError):
        await create_budget(tenant_id=unknown, budget_tokens=10_000, session=db_session)

    assert fake.calls == 1
    assert _count("absent") == before + 1


async def test_a_refresh_that_raises_keeps_refusing(db_session):
    new_tenant = str(uuid.uuid4())
    fake = _fresh_overlay_and_licences(**{"Just Created BV": new_tenant})
    fake.raises = RuntimeError("licences down")
    before = _count("failed")

    with pytest.raises(UnregisteredTenantError):
        await create_budget(tenant_id=new_tenant, budget_tokens=10_000, session=db_session)

    assert fake.calls == 1
    assert _count("failed") == before + 1


async def test_a_refresh_that_installs_nothing_keeps_refusing():
    _fresh_overlay_and_licences()

    async def _empty() -> int:
        return 0  # get_tenant_info's {} for a 401 / timeout / bad body

    tenancy.set_registry_refresh_hook(_empty)
    assert await tenancy.write_gate_refuses_tenant_after_refresh(
        str(uuid.uuid4()), max_overlay_age_s=MAX_AGE
    )


async def test_a_refresh_that_hangs_is_cut_off_and_refuses(monkeypatch):
    new_tenant = str(uuid.uuid4())
    fake = _fresh_overlay_and_licences(**{"Just Created BV": new_tenant})
    fake.delay = 1.0
    monkeypatch.setattr(tenancy, "MISS_REFRESH_TIMEOUT_SECONDS", 0.05)

    assert await tenancy.write_gate_refuses_tenant_after_refresh(
        new_tenant, max_overlay_age_s=MAX_AGE
    )
    await asyncio.sleep(1.1)  # let the shielded fetch finish inside this test


async def test_no_hook_refuses_exactly_as_before():
    tenancy.set_live_tenant_registry({"gate-fresh-anchor": str(uuid.uuid4())})
    assert await tenancy.write_gate_refuses_tenant_after_refresh(
        str(uuid.uuid4()), max_overlay_age_s=MAX_AGE
    )


# --------------------------------------------------------------------------- #
# Nothing that would not be refused pays for a refresh
# --------------------------------------------------------------------------- #


async def test_a_registered_id_does_not_refresh():
    known = str(uuid.uuid4())
    fake = _fresh_overlay_and_licences()
    tenancy.set_live_tenant_registry({"Known": known})
    assert not await tenancy.write_gate_refuses_tenant_after_refresh(
        known, max_overlay_age_s=MAX_AGE
    )
    assert fake.calls == 0


async def test_a_stale_overlay_allows_without_refreshing():
    fake = FakeLicences({})
    tenancy.set_registry_refresh_hook(fake.hook)  # overlay never loaded: age -1
    assert not await tenancy.write_gate_refuses_tenant_after_refresh(
        str(uuid.uuid4()), max_overlay_age_s=MAX_AGE
    )
    assert fake.calls == 0


async def test_a_sentinel_is_refused_without_asking_licences():
    sentinel = next(iter(tenancy.SENTINEL_TENANT_IDS))
    fake = _fresh_overlay_and_licences()
    assert await tenancy.write_gate_refuses_tenant_after_refresh(
        sentinel, max_overlay_age_s=MAX_AGE
    )
    assert fake.calls == 0


# --------------------------------------------------------------------------- #
# Rate limits
# --------------------------------------------------------------------------- #


async def test_negative_cache_suppresses_a_repeat_refresh_for_the_same_id(monkeypatch):
    clock = [1000.0]
    monkeypatch.setattr(tenancy, "_monotonic", lambda: clock[0])
    fake = _fresh_overlay_and_licences()
    unknown = str(uuid.uuid4())
    before = _count("negative_cached")

    assert await tenancy.write_gate_refuses_tenant_after_refresh(unknown, max_overlay_age_s=MAX_AGE)
    assert fake.calls == 1

    # Past the global floor, inside the per-id TTL: no second fetch.
    clock[0] += tenancy.MISS_REFRESH_MIN_INTERVAL_SECONDS + 1
    assert await tenancy.write_gate_refuses_tenant_after_refresh(unknown, max_overlay_age_s=MAX_AGE)
    assert fake.calls == 1
    assert _count("negative_cached") == before + 1

    # Past the TTL: asked again — and if licences has it by now, allowed.
    clock[0] += tenancy.MISS_REFRESH_NEGATIVE_TTL_SECONDS
    fake.registry["Late Arrival"] = unknown
    assert not await tenancy.write_gate_refuses_tenant_after_refresh(
        unknown, max_overlay_age_s=MAX_AGE
    )
    assert fake.calls == 2


async def test_global_floor_bounds_a_flood_of_distinct_unknown_ids(monkeypatch):
    clock = [1000.0]
    monkeypatch.setattr(tenancy, "_monotonic", lambda: clock[0])
    fake = _fresh_overlay_and_licences()
    before = _count("rate_limited")

    for _ in range(50):
        assert await tenancy.write_gate_refuses_tenant_after_refresh(
            str(uuid.uuid4()), max_overlay_age_s=MAX_AGE
        )
    assert fake.calls == 1
    assert _count("rate_limited") == before + 49

    clock[0] += tenancy.MISS_REFRESH_MIN_INTERVAL_SECONDS
    assert await tenancy.write_gate_refuses_tenant_after_refresh(
        str(uuid.uuid4()), max_overlay_age_s=MAX_AGE
    )
    assert fake.calls == 2


async def test_concurrent_misses_share_one_fetch():
    new_tenant = str(uuid.uuid4())
    fake = _fresh_overlay_and_licences(**{"Just Created BV": new_tenant})
    fake.delay = 0.05

    verdicts = await asyncio.gather(
        *(
            tenancy.write_gate_refuses_tenant_after_refresh(new_tenant, max_overlay_age_s=MAX_AGE)
            for _ in range(10)
        )
    )
    assert verdicts == [False] * 10
    assert fake.calls == 1


async def test_negative_cache_is_bounded(monkeypatch):
    monkeypatch.setattr(tenancy, "MISS_NEGATIVE_CACHE_CAP", 8)
    for i in range(20):
        tenancy._remember_miss(str(uuid.uuid4()), float(i))
    assert len(tenancy._miss_negative_cache) == 8
