"""The live licences registry overlays the snapshot — and never replaces it.

THE DEFECT
----------
``tenancy.LEGACY_TENANT_NAMES`` is a hand-regenerated SNAPSHOT of
``licenses.tenants``. Its own comments record the failure twice: six hand-picked
names until 2026-08-25 (so ``marketing`` and ``sales`` took a hard 400 while
``n8n-dev`` worked), then an eight-entry drift in BOTH directions at once
between 2026-08-25 and 2026-09-05 while looking perfectly healthy.
``test_tenant_registry_shim.py`` catches that in CI against a checked-in
fixture. Nothing caught it in PRODUCTION, where a registered tenant absent from
the map is rejected outright.

So the registry is now read live and installed as an overlay. The premise was
re-measured before this shipped: from inside ``deploy/billing-service``, with
this service's own service-account JWT,
``GET http://licenses.licenses.svc.cluster.local:8544/internal/tenants``
returns **200 with 59 tenants**.

WHAT THIS FILE GUARDS
---------------------
The three properties that make the overlay safe to put on the hot path, each of
which is a way this change could have made things WORSE rather than better:

1. FAIL SOFT — every registry failure degrades to the snapshot, never to a
   rejection. ``tenancy`` sits on the boundary for every inbound header and
   every stream payload, so a hard dependency here would take metering down
   with licences.
2. NEVER MINT — an unknown name still resolves to ``None``. The predecessor's
   invented tenants accumulated 13,670 real rows.
3. NEVER SHRINK — a name only the snapshot holds keeps resolving. That set is
   exactly ``RENAMED_TENANT_ALIASES``, the retired spellings live callers still
   send; "the registry no longer lists it" is not evidence that nobody sends it.
"""

from __future__ import annotations

import pytest

import tenancy
from tenancy import (
    LEGACY_TENANT_NAMES,
    RENAMED_TENANT_ALIASES,
    canonical_tenant_id,
    is_registered_tenant_id,
    live_tenant_label,
    live_tenant_registry_stats,
    reset_live_tenant_registry,
    set_live_tenant_registry,
)

pytestmark = pytest.mark.unit

# Two uuids deliberately absent from the checked-in snapshot, standing in for
# the six genuinely registered names the snapshot was missing on 2026-09-05.
# Synthetic rather than borrowed from the live registry ON PURPOSE: every real
# id is a candidate for being added to the snapshot by the next regeneration,
# which would silently turn "absent from the snapshot" into "present" and make
# half of this file assert nothing.
NEW_TENANT_ID = "01a0aaaa-0000-7000-8000-0000000000a1"
OTHER_TENANT_ID = "01a0aaaa-0000-7000-8000-0000000000a2"


@pytest.fixture(autouse=True)
def _clean_overlay():
    """No overlay before or after any test here.

    Without the teardown an installed overlay would leak into every other test
    file in the run, and the leak would be INVISIBLE: it only ever makes
    resolution succeed more often.
    """
    reset_live_tenant_registry()
    yield
    reset_live_tenant_registry()


class TestFailSoft:
    def test_with_no_refresh_resolution_is_exactly_the_snapshot(self) -> None:
        """The pre-change behaviour, byte for byte, is the zero-refresh state.

        This is the control. If it ever fails, the overlay has stopped being an
        addition and started being a replacement — which is the shape in which a
        licences outage becomes a 400 storm.
        """
        assert live_tenant_registry_stats() == (0, -1.0)
        for name, expected in LEGACY_TENANT_NAMES.items():
            assert canonical_tenant_id(name) == expected

    def test_an_empty_registry_answer_does_not_clear_a_good_overlay(self) -> None:
        """``{}`` means "could not ask" as often as it means "nothing there".

        ``get_tenant_info`` returns ``{}`` for a 401, a timeout, a non-JSON body
        AND an empty registry — indistinguishable. Clearing on it would let one
        transient blip silently change resolution.
        """
        assert set_live_tenant_registry({"brand-new-tenant": NEW_TENANT_ID}) == 1
        assert set_live_tenant_registry({}) == 0
        assert canonical_tenant_id("brand-new-tenant") == NEW_TENANT_ID

    def test_a_name_only_the_snapshot_holds_survives_a_live_refresh(self) -> None:
        """NEVER SHRINK. The retired spellings are the whole point of this.

        ``RENAMED_TENANT_ALIASES`` names spellings the registry has renamed away
        and that live callers — notably the raw Keycloak ``tenant_id`` claim
        ``fr`` forwards — still send. A refresh that dropped them would 400
        exactly those callers.
        """
        retired = next(iter(RENAMED_TENANT_ALIASES))
        assert retired not in {"brand-new-tenant"}
        set_live_tenant_registry({"brand-new-tenant": NEW_TENANT_ID})
        assert canonical_tenant_id(retired) == LEGACY_TENANT_NAMES[retired]


class TestNeverMint:
    def test_an_unknown_name_still_resolves_to_none(self) -> None:
        set_live_tenant_registry({"brand-new-tenant": NEW_TENANT_ID})
        assert canonical_tenant_id("no-such-tenant-anywhere") is None

    def test_a_non_uuid_registry_value_is_dropped_not_stored(self) -> None:
        """The payload comes from a service we do not control.

        Installing an unparseable value would put a non-uuid into a resolver
        whose entire job since migration 0011 is to keep non-uuids out of uuid
        columns.
        """
        assert set_live_tenant_registry({"junk": "not-a-uuid"}) == 0
        assert canonical_tenant_id("junk") is None

    def test_a_name_claimed_by_two_ids_is_dropped_entirely(self) -> None:
        """Arbitrating would attribute somebody's spend to the wrong tenant.

        Falling back to the snapshot (here: nothing) is the honest outcome; a
        coin flip between two real tenants is not.
        """
        installed = set_live_tenant_registry(
            {"ambiguous": NEW_TENANT_ID, "ambiguous ": OTHER_TENANT_ID}
        )
        assert installed == 0
        assert canonical_tenant_id("ambiguous") is None


class TestLiveOverlayWins:
    def test_a_registered_name_absent_from_the_snapshot_now_resolves(self) -> None:
        """The measured 2026-09-05 defect: six registered names took a 400."""
        assert canonical_tenant_id("brand-new-tenant") is None
        assert set_live_tenant_registry({"brand-new-tenant": NEW_TENANT_ID}) == 1
        assert canonical_tenant_id("brand-new-tenant") == NEW_TENANT_ID

    def test_the_live_uuid_wins_when_the_snapshot_disagrees(self) -> None:
        """A drifted snapshot resolved a name to the WRONG tenant, silently.

        That is worse than a rejection: the caller is served, and the spend
        lands on somebody else's ledger.
        """
        stale_name = next(iter(LEGACY_TENANT_NAMES))
        assert LEGACY_TENANT_NAMES[stale_name] != NEW_TENANT_ID
        set_live_tenant_registry({stale_name: NEW_TENANT_ID})
        assert canonical_tenant_id(stale_name) == NEW_TENANT_ID

    def test_a_live_id_counts_as_registered(self) -> None:
        """Union, never substitution.

        ``is_registered_tenant_id`` gates ``billing_exclude_unregistered_tenants``.
        If the overlay REPLACED the snapshot's id set, a licences blip would
        start parking real rows on ``excluded_from_billing``; because it is a
        union, a refresh can only make the accusation rarer.
        """
        assert not is_registered_tenant_id(NEW_TENANT_ID)
        set_live_tenant_registry({"brand-new-tenant": NEW_TENANT_ID})
        assert is_registered_tenant_id(NEW_TENANT_ID)
        snapshot_id = next(iter(LEGACY_TENANT_NAMES.values()))
        assert is_registered_tenant_id(snapshot_id), "the snapshot's ids must survive"


class TestResolutionCounter:
    """The staleness of the snapshot must be measurable, not merely testable."""

    def _read(self, source: str) -> float:
        from observability.metrics import tenant_name_resolution_total

        return tenant_name_resolution_total.labels(source=source)._value.get()

    def test_each_branch_increments_its_own_series(self) -> None:
        set_live_tenant_registry({"brand-new-tenant": NEW_TENANT_ID})
        snapshot_only = next(iter(RENAMED_TENANT_ALIASES))

        before = {s: self._read(s) for s in ("live", "snapshot", "unknown")}
        canonical_tenant_id("brand-new-tenant")
        canonical_tenant_id(snapshot_only)
        canonical_tenant_id("definitely-not-a-tenant")
        after = {s: self._read(s) for s in ("live", "snapshot", "unknown")}

        assert after["live"] - before["live"] == 1
        assert after["snapshot"] - before["snapshot"] == 1
        assert after["unknown"] - before["unknown"] == 1

    def test_a_uuid_input_increments_nothing_here(self) -> None:
        """Disjoint populations. The uuid path is billing_tenant_unregistered_total.

        If a uuid also incremented this family the two would double-count the
        same request and neither would have a readable denominator.
        """
        before = {s: self._read(s) for s in ("live", "snapshot", "unknown")}
        canonical_tenant_id(NEW_TENANT_ID)
        after = {s: self._read(s) for s in ("live", "snapshot", "unknown")}
        assert after == before


class TestDriftGauges:
    def test_drift_is_published_on_install(self) -> None:
        """Refresh-time, not request-time: drift is a property of the two maps."""
        from observability.metrics import tenant_registry_drift, tenant_registry_live_entries

        snapshot_name, snapshot_id = next(iter(LEGACY_TENANT_NAMES.items()))
        set_live_tenant_registry(
            {
                snapshot_name: snapshot_id,  # agrees
                "brand-new-tenant": NEW_TENANT_ID,  # missing from the snapshot
            }
        )
        assert tenant_registry_live_entries._value.get() == 2
        assert tenant_registry_drift.labels(kind="missing_from_snapshot")._value.get() == 1
        assert tenant_registry_drift.labels(kind="conflicting_uuid")._value.get() == 0
        # Everything else the snapshot holds is absent from this tiny live map.
        assert tenant_registry_drift.labels(kind="absent_from_live")._value.get() == (
            len(LEGACY_TENANT_NAMES) - 1
        )

    def test_a_conflicting_uuid_is_counted_separately_from_a_missing_name(self) -> None:
        """The conflict is the dangerous shape — it resolved, to the wrong tenant."""
        from observability.metrics import tenant_registry_drift

        stale_name = next(iter(LEGACY_TENANT_NAMES))
        set_live_tenant_registry({stale_name: NEW_TENANT_ID})
        assert tenant_registry_drift.labels(kind="conflicting_uuid")._value.get() == 1


class TestMetricsFailureCannotBreakResolution:
    def test_a_broken_counter_does_not_reject_a_tenant(self, monkeypatch) -> None:
        """An observability defect must never turn a resolvable tenant into a 400.

        The counter sits INSIDE the hot resolution path, so it is one bad
        prometheus registry away from being able to 400 every caller. The
        swallow is asserted here rather than assumed, against a counter that
        raises on the exact call the resolver makes.
        """
        import observability.metrics as metrics

        class _Exploding:
            def labels(self, **_kw):  # noqa: ANN003, ANN201
                raise RuntimeError("prometheus is on fire")

        monkeypatch.setattr(metrics, "tenant_name_resolution_total", _Exploding())

        set_live_tenant_registry({"brand-new-tenant": NEW_TENANT_ID})
        assert canonical_tenant_id("brand-new-tenant") == NEW_TENANT_ID
        name, expected = next(iter(LEGACY_TENANT_NAMES.items()))
        assert canonical_tenant_id(name) == expected
        assert canonical_tenant_id("no-such-tenant-anywhere") is None

    def test_the_counter_is_actually_on_the_resolution_path(self, monkeypatch) -> None:
        """Guards the test above from being vacuous.

        If ``_count_name_resolution`` were never called, the swallow test would
        pass against a resolver carrying no instrumentation at all.
        """
        seen: list[str] = []
        monkeypatch.setattr(tenancy, "_count_name_resolution", seen.append)
        name = next(iter(LEGACY_TENANT_NAMES))
        tenancy.resolve_tenant_name_to_id(name)
        tenancy.resolve_tenant_name_to_id("no-such-tenant-anywhere")
        assert seen == ["snapshot", "unknown"]


class TestRefreshEndToEnd:
    """Through the real fetch, not just the pure installer.

    The installer can be perfect and the wiring still wrong — the failure that
    matters is "licences answers 401 and every tenant starts taking a 400", and
    only the end-to-end path can show that it does not happen.
    """

    @pytest.fixture(autouse=True)
    def _reset_fetch_cache(self):
        from services.tenant_names import reset_cache

        reset_cache()
        yield
        reset_cache()

    def _client(self, **kw):
        from unittest.mock import AsyncMock, MagicMock

        mock_client = AsyncMock()
        if kw.get("exc") is not None:
            mock_client.get = AsyncMock(side_effect=kw["exc"])
        else:
            resp = MagicMock()
            resp.status_code = kw.get("status_code", 200)
            resp.json.return_value = kw.get("tenants", [])
            mock_client.get = AsyncMock(return_value=resp)
        mock_client.__aenter__ = AsyncMock(return_value=mock_client)
        mock_client.__aexit__ = AsyncMock(return_value=False)
        return mock_client

    async def test_a_successful_refresh_installs_the_live_names(self) -> None:
        from unittest.mock import patch

        from services.tenant_names import refresh_tenant_registry
        from settings import Settings

        payload = [
            {"id": NEW_TENANT_ID, "name": "brand-new-tenant", "kind": "internal"},
            {"id": OTHER_TENANT_ID, "name": "second-new-tenant", "kind": "service"},
        ]
        with patch(
            "services.tenant_names.httpx.AsyncClient",
            return_value=self._client(tenants=payload),
        ):
            installed = await refresh_tenant_registry(
                Settings(licenses_url="https://licenses.example.com"), None
            )

        assert installed == 2
        assert canonical_tenant_id("brand-new-tenant") == NEW_TENANT_ID

    @pytest.mark.parametrize(
        "kwargs",
        [
            {"status_code": 401},
            {"status_code": 403},
            {"status_code": 500},
            {"exc": Exception("no route to licences")},
            {"tenants": []},
        ],
        ids=["401", "403", "500", "transport", "empty-body"],
    )
    async def test_every_failure_mode_leaves_the_snapshot_resolving(self, kwargs) -> None:
        """THE non-negotiable property. A registry outage is not a 400 storm.

        Parametrised over the five shapes licences can actually fail in,
        because it is exactly one of them (the 401 that stood in this repo's
        comments for a week) that this change is built on top of.
        """
        from unittest.mock import patch

        from services.tenant_names import refresh_tenant_registry
        from settings import Settings

        name, expected = next(iter(LEGACY_TENANT_NAMES.items()))
        with patch("services.tenant_names.httpx.AsyncClient", return_value=self._client(**kwargs)):
            installed = await refresh_tenant_registry(
                Settings(licenses_url="https://licenses.example.com"), None
            )

        assert installed == 0
        assert canonical_tenant_id(name) == expected
        assert canonical_tenant_id("no-such-tenant-anywhere") is None

    async def test_a_failure_after_a_good_refresh_keeps_the_live_names(self) -> None:
        """The overlay is not expired and not cleared: stale beats absent."""
        from unittest.mock import patch

        from services.tenant_names import refresh_tenant_registry
        from settings import Settings

        settings = Settings(licenses_url="https://licenses.example.com")
        payload = [{"id": NEW_TENANT_ID, "name": "brand-new-tenant", "kind": "internal"}]
        with patch(
            "services.tenant_names.httpx.AsyncClient",
            return_value=self._client(tenants=payload),
        ):
            await refresh_tenant_registry(settings, None)
        with patch(
            "services.tenant_names.httpx.AsyncClient", return_value=self._client(status_code=401)
        ):
            await refresh_tenant_registry(settings, None)

        assert canonical_tenant_id("brand-new-tenant") == NEW_TENANT_ID

    async def test_the_refresh_does_not_reuse_its_own_cached_answer(self) -> None:
        """Each tick must be a real fetch, or the loop only ever re-reads itself.

        ``get_tenant_info`` caches for 5 minutes, so without the explicit
        ``reset_cache`` a refresh loop running on that same interval could serve
        the map it installed last time and never notice a new tenant.
        """
        from unittest.mock import patch

        from services.tenant_names import refresh_tenant_registry
        from settings import Settings

        settings = Settings(licenses_url="https://licenses.example.com")
        first = self._client(
            tenants=[{"id": NEW_TENANT_ID, "name": "brand-new-tenant", "kind": "internal"}]
        )
        with patch("services.tenant_names.httpx.AsyncClient", return_value=first):
            await refresh_tenant_registry(settings, None)
        second = self._client(
            tenants=[
                {"id": NEW_TENANT_ID, "name": "brand-new-tenant", "kind": "internal"},
                {"id": OTHER_TENANT_ID, "name": "second-new-tenant", "kind": "service"},
            ]
        )
        with patch("services.tenant_names.httpx.AsyncClient", return_value=second):
            installed = await refresh_tenant_registry(settings, None)

        assert second.get.await_count == 1, "the second tick did not hit the network"
        assert installed == 2
        assert canonical_tenant_id("second-new-tenant") == OTHER_TENANT_ID


class TestTheRefreshLoop:
    """The loop must tick immediately, survive a failure, and stop on demand."""

    async def test_the_first_tick_is_immediate_and_stop_event_ends_it(self, monkeypatch) -> None:
        """A loop that slept first would leave a fresh pod on the snapshot for
        a full interval; one that ignored ``stop_event`` would hang shutdown
        behind its own timeout, which is how a rolling deploy stalls.
        """
        import asyncio

        import services.tenant_names as tn
        from settings import Settings

        stop_event = asyncio.Event()
        calls: list[int] = []

        async def _fake_refresh(_settings, _tp):  # noqa: ANN001, ANN202
            calls.append(1)
            stop_event.set()
            return 1

        monkeypatch.setattr(tn, "refresh_tenant_registry", _fake_refresh)
        # A long interval: if the loop waited before its first tick, or ignored
        # stop_event while waiting, this would hit the test timeout instead.
        settings = Settings(tenant_registry_refresh_seconds=3600)
        await asyncio.wait_for(
            tn.run_tenant_registry_refresh_loop(settings, None, stop_event), timeout=5
        )
        assert calls == [1]

    async def test_a_raising_refresh_does_not_kill_the_loop(self, monkeypatch) -> None:
        """``refresh_tenant_registry`` is not supposed to raise. If it ever does,
        the loop dying would freeze the overlay for the life of the pod with
        nothing but a task-death log to say so.
        """
        import asyncio

        import services.tenant_names as tn
        from settings import Settings

        stop_event = asyncio.Event()
        calls: list[int] = []

        async def _boom(_settings, _tp):  # noqa: ANN001, ANN202
            calls.append(1)
            if len(calls) >= 2:
                stop_event.set()
            raise RuntimeError("licences exploded")

        monkeypatch.setattr(tn, "refresh_tenant_registry", _boom)
        settings = Settings(tenant_registry_refresh_seconds=30)  # the floor
        task = asyncio.create_task(tn.run_tenant_registry_refresh_loop(settings, None, stop_event))
        await asyncio.sleep(0.05)
        assert calls, "the loop did not even reach its first tick"
        stop_event.set()
        task.cancel()
        with pytest.raises((asyncio.CancelledError, TimeoutError)):
            await asyncio.wait_for(task, timeout=2)


class TestReverseLookupForTheWritePath:
    """``live_tenant_label`` — the uuid -> NAME direction the FLUSHER needs.

    THE DEFECT IT CLOSES. ``billing_cost_events.tenant_name`` is written at flush
    time, and since migration 0011 the emitters send a uuid as ``tenant_id`` and
    no ``tenant_name`` at all, so ``original_tenant_label`` faithfully preserved
    the uuid into the display column: 66,103 of 66,103 live rows ($310.49), which
    then propagates into ``billing_cost_daily.tenant_name`` and every SQL
    consumer. Two READ paths (``/dashboard/costs``, the CSV export) re-resolved
    it; nothing else could.

    The overlay is already in-process and healthy on the flush path
    (``billing_tenant_registry_live_entries 59``), so the fix is to consult it at
    WRITE time. What this class guards is that consulting it cannot make things
    worse than the uuid it replaces.
    """

    def test_a_registry_hit_yields_the_real_name(self) -> None:
        set_live_tenant_registry({"brand-new-tenant": NEW_TENANT_ID})
        assert live_tenant_label(NEW_TENANT_ID) == "brand-new-tenant"

    def test_the_two_directions_come_from_one_install(self) -> None:
        """Forward and reverse must not be able to disagree about the registry.

        They are derived from the same ``resolved`` map in one call, so a name
        that resolves to an id must round-trip back to that name. If this ever
        fails, the write path and the read path have separate sources of truth
        and ``billing_cost_events.tenant_name`` can drift from what the dashboard
        renders for the same row.
        """
        set_live_tenant_registry({"brand-new-tenant": NEW_TENANT_ID})
        assert live_tenant_label(canonical_tenant_id("brand-new-tenant")) == "brand-new-tenant"

    def test_a_registry_miss_does_not_fabricate(self) -> None:
        """NEVER MINT, in this direction too.

        An id the registry does not vouch for answers ``None`` so the caller
        keeps its existing fallback and COUNTS it. Inventing a plausible label
        here would be strictly worse than the uuid: a uuid is visibly not a name,
        while a fabricated label reads as a measurement.
        """
        set_live_tenant_registry({"brand-new-tenant": NEW_TENANT_ID})
        assert live_tenant_label(OTHER_TENANT_ID) is None

    def test_an_empty_overlay_is_a_miss_not_a_failure(self) -> None:
        """No refresh has landed / licences is down. Must be quiet, not fatal."""
        assert live_tenant_registry_stats() == (0, -1.0)
        assert live_tenant_label(NEW_TENANT_ID) is None

    @pytest.mark.parametrize("bad", [None, "", "   ", "not-a-uuid", 12345, object()])
    def test_no_input_can_make_it_raise(self, bad) -> None:
        """FAIL SOFT. This runs once per cost event on the flush path.

        A throw here would fail a MONEY record over a display label — far worse
        than storing the uuid. Every shape a malformed wire value can take must
        answer ``None``.
        """
        set_live_tenant_registry({"brand-new-tenant": NEW_TENANT_ID})
        assert live_tenant_label(bad) is None

    def test_a_uuid_claimed_by_two_names_is_dropped_not_guessed(self) -> None:
        """The reverse of the forward ambiguity rule, and for the same reason.

        Two registry spellings on one uuid would otherwise resolve by dict
        iteration order — a silently-varying answer to a question about somebody's
        money. Dropping it falls back to the uuid, which is a wrong answer that is
        at least visibly wrong and is counted.

        The forward direction is unaffected: BOTH names still resolve to the id.
        """
        assert set_live_tenant_registry({"alias-a": NEW_TENANT_ID, "alias-b": NEW_TENANT_ID}) == 2
        assert live_tenant_label(NEW_TENANT_ID) is None
        assert canonical_tenant_id("alias-a") == NEW_TENANT_ID
        assert canonical_tenant_id("alias-b") == NEW_TENANT_ID

    def test_reset_clears_the_reverse_map_too(self) -> None:
        """A half-cleared overlay would leak labels into every later test."""
        set_live_tenant_registry({"brand-new-tenant": NEW_TENANT_ID})
        reset_live_tenant_registry()
        assert live_tenant_label(NEW_TENANT_ID) is None


# ---------------------------------------------------------------------------
# The REGISTRATION check on the hot path: live first, snapshot as fallback
# ---------------------------------------------------------------------------
#
# ``docs/UNREGISTERED_TENANT_GATE.md`` precondition 2 — "billing can consult the
# registry live, or the refresh is automated and verified" — is what these
# assert. Not that the code CAN consult the registry (it could before, for
# names), but that:
#
#   * the UUID registration check the cost-flusher gate is built on reads the
#     live overlay FIRST, and therefore vouches for a tenant the checked-in
#     snapshot has never heard of (the six missing on 2026-09-05),
#   * a registry that never answered degrades to the snapshot rather than
#     raising or refusing, and
#   * every one of those outcomes is COUNTED, so the live path can be PROVEN
#     in production instead of assumed. That counter is the control that can
#     fail: without it a token misconfiguration falls back to the hand-pasted
#     file forever, in silence, with the service still green.


def _source_count(source: str) -> float:
    from observability import metrics as observability_metrics

    return observability_metrics.tenant_registration_source_total.labels(source=source)._value.get()


class TestRegistrationConsultsTheLiveRegistry:
    def test_live_registry_vouches_for_a_tenant_absent_from_the_snapshot(self) -> None:
        """⭐ THE LOAD-BEARING ONE.

        This id is in no checked-in map. Before the overlay was consulted by the
        registration check, it answered False — and an armed gate would have
        PARKED its spend as unregistered, which is exactly what happened to
        ``svc-w2v-shared`` and five others while the snapshot was 11 days stale.
        """
        assert tenancy.is_registered_tenant_id(NEW_TENANT_ID) is False
        set_live_tenant_registry({"brand-new-tenant": NEW_TENANT_ID})
        assert tenancy.is_registered_tenant_id(NEW_TENANT_ID) is True
        assert tenancy.registration_source(NEW_TENANT_ID) == "live"

    def test_an_unreachable_registry_falls_back_to_the_snapshot_without_raising(self) -> None:
        """FAIL SAFE, NOT CLOSED. No refresh ever landed — the outage state.

        Every snapshot tenant must still answer True, and the source must say
        ``snapshot_degraded`` so the degradation is nameable in production. A
        licences blip may never park legitimate revenue rows.

        ``SENTINEL_TENANT_IDS`` is excluded, and an outage is exactly when that
        exclusion matters most: ``"system"`` is a fabricated placeholder, so it
        must answer "not registered" whether the live registry is reachable or
        not. A sentinel that became billable during a licences blip would be a
        gate that opens under precisely the conditions it exists for.
        """
        assert live_tenant_registry_stats() == (0, -1.0)
        assert tenancy.live_registry_is_fresh() is False
        for tenant_id in LEGACY_TENANT_NAMES.values():
            if tenant_id in tenancy.SENTINEL_TENANT_IDS:
                assert tenancy.is_registered_tenant_id(tenant_id) is False
                assert tenancy.registration_source(tenant_id) == "sentinel"
                continue
            assert tenancy.is_registered_tenant_id(tenant_id) is True
            assert tenancy.registration_source(tenant_id) == "snapshot_degraded"

    def test_a_fresh_overlay_distinguishes_drift_from_degradation(self) -> None:
        """``snapshot`` and ``snapshot_degraded`` are different facts.

        A retired spelling the registry no longer lists is ordinary drift while
        the overlay is fresh; the SAME answer given with no overlay at all means
        billing is running off a hand-pasted file. Collapsing them would make
        the counter unable to say which.
        """
        snapshot_only = LEGACY_TENANT_NAMES[next(iter(RENAMED_TENANT_ALIASES))]
        set_live_tenant_registry({"brand-new-tenant": NEW_TENANT_ID})
        assert tenancy.live_registry_is_fresh() is True
        assert tenancy.registration_source(snapshot_only) == "snapshot"

    def test_a_stale_overlay_is_degraded_even_though_it_is_still_used(self) -> None:
        """The overlay is never EXPIRED — but it stops being evidence.

        Its ids keep answering ``live`` (an old live map still beats the
        snapshot); what changes is that a snapshot-only answer is now reported
        as a degradation, because nothing recent has confirmed the registry.
        """
        set_live_tenant_registry({"brand-new-tenant": NEW_TENANT_ID})
        assert tenancy._live_refreshed_at is not None
        tenancy._live_refreshed_at -= tenancy.REGISTRY_STALE_AFTER_SECONDS + 1
        assert tenancy.live_registry_is_fresh() is False
        assert tenancy.registration_source(NEW_TENANT_ID) == "live"
        assert tenancy.registration_source(next(iter(LEGACY_TENANT_NAMES.values()))) == (
            "snapshot_degraded"
        )

    def test_an_id_no_map_holds_is_unknown_and_still_fails_open(self) -> None:
        """The population the gate would park. Nothing is rejected here."""
        assert tenancy.registration_source(OTHER_TENANT_ID) == "unknown"
        assert tenancy.is_registered_tenant_id(OTHER_TENANT_ID) is False
        # canonical_tenant_id still RETURNS it: the money record must survive.
        assert canonical_tenant_id(OTHER_TENANT_ID) == OTHER_TENANT_ID

    def test_the_nil_tenant_is_registered_from_the_snapshot(self) -> None:
        """``default-anon`` is a real row in licenses.tenants.

        Answering False for it would route one condition to two controls — it is
        already dead-lettered by validate_cost_event long before this is asked.
        """
        assert tenancy.is_registered_tenant_id(tenancy.NIL_TENANT_ID) is True


class TestRegistrationSourceCounter:
    """The control that can FAIL. Without it the change is unprovable."""

    @pytest.mark.parametrize(
        "source",
        ["live", "snapshot", "snapshot_degraded", "unknown"],
    )
    def test_every_label_value_is_born_at_zero(self, source: str) -> None:
        """An absent series reads as "never happened", not as "not wired up".

        Checks the child exists WITHOUT calling ``.labels()``, which would
        create it and make the assertion vacuous.
        """
        from observability import metrics as observability_metrics

        assert (source,) in observability_metrics.tenant_registration_source_total._metrics, (
            f"{source!r} is not born at import, so an alert on it cannot tell "
            "'never happened' from 'the instrument is not wired'"
        )

    def test_the_live_branch_increments_live(self) -> None:
        set_live_tenant_registry({"brand-new-tenant": NEW_TENANT_ID})
        before = _source_count("live")
        assert tenancy.is_registered_tenant_id(NEW_TENANT_ID) is True
        assert _source_count("live") == before + 1

    def test_the_snapshot_fallback_branch_increments_snapshot_degraded(self) -> None:
        before = _source_count("snapshot_degraded")
        assert tenancy.is_registered_tenant_id(next(iter(LEGACY_TENANT_NAMES.values()))) is True
        assert _source_count("snapshot_degraded") == before + 1

    def test_the_drift_branch_increments_snapshot(self) -> None:
        set_live_tenant_registry({"brand-new-tenant": NEW_TENANT_ID})
        snapshot_only = LEGACY_TENANT_NAMES[next(iter(RENAMED_TENANT_ALIASES))]
        before = _source_count("snapshot")
        assert tenancy.is_registered_tenant_id(snapshot_only) is True
        assert _source_count("snapshot") == before + 1

    def test_the_unknown_branch_increments_unknown(self) -> None:
        before = _source_count("unknown")
        assert tenancy.is_registered_tenant_id(OTHER_TENANT_ID) is False
        assert _source_count("unknown") == before + 1

    def test_the_counter_and_the_verdict_cannot_disagree(self) -> None:
        """Both are derived from ONE call to ``registration_source``.

        A counter computed separately from the boolean it describes can drift
        from it — and this counter is the evidence the gate would be armed on.
        """
        set_live_tenant_registry({"brand-new-tenant": NEW_TENANT_ID})
        for tenant_id in (NEW_TENANT_ID, OTHER_TENANT_ID, tenancy.NIL_TENANT_ID):
            expected = tenancy.registration_source(tenant_id) != "unknown"
            assert tenancy.is_registered_tenant_id(tenant_id) is expected


class TestDemandRefresh:
    """The window between two loop ticks, closed — off the request path."""

    def test_an_unknown_id_asks_for_a_refresh_at_most_once_per_cooldown(self) -> None:
        import asyncio

        calls: list[int] = []

        async def _hook() -> None:
            calls.append(1)

        async def _exercise() -> None:
            tenancy.set_registry_refresh_hook(_hook)
            for _ in range(5):
                assert tenancy.is_registered_tenant_id(OTHER_TENANT_ID) is False
            # Let the scheduled task(s) run.
            await asyncio.sleep(0)
            await asyncio.sleep(0)

        asyncio.run(_exercise())
        assert calls == [1], (
            "the hot path must ask for at most ONE refresh per cooldown — a "
            "caller spraying random uuids would otherwise make one licences "
            "request per cost event"
        )

    def test_a_registered_id_never_asks_for_a_refresh(self) -> None:
        import asyncio

        calls: list[int] = []

        async def _hook() -> None:
            calls.append(1)

        async def _exercise() -> None:
            set_live_tenant_registry({"brand-new-tenant": NEW_TENANT_ID})
            tenancy.set_registry_refresh_hook(_hook)
            assert tenancy.is_registered_tenant_id(NEW_TENANT_ID) is True
            assert tenancy.is_registered_tenant_id(next(iter(LEGACY_TENANT_NAMES.values())))
            await asyncio.sleep(0)

        asyncio.run(_exercise())
        assert calls == []

    def test_with_no_event_loop_the_check_still_answers(self) -> None:
        """This module is called from sync code, including pydantic validators."""

        def _hook():  # pragma: no cover - must never be called without a loop
            raise AssertionError("scheduled without an event loop")

        tenancy.set_registry_refresh_hook(_hook)
        assert tenancy.is_registered_tenant_id(OTHER_TENANT_ID) is False

    def test_a_hook_that_explodes_cannot_break_resolution(self) -> None:
        import asyncio

        def _hook():
            raise RuntimeError("licences client is misconfigured")

        async def _exercise() -> None:
            tenancy.set_registry_refresh_hook(_hook)
            assert tenancy.is_registered_tenant_id(OTHER_TENANT_ID) is False

        asyncio.run(_exercise())

    def test_reset_uninstalls_the_hook(self) -> None:
        """Otherwise one test's hook schedules tasks onto another's loop."""
        tenancy.set_registry_refresh_hook(lambda: None)
        reset_live_tenant_registry()
        assert tenancy._refresh_hook is None


class TestDemandRefreshIsWiredToARealFetch:
    """⛔ INERT WIRING IS THE FAILURE MODE THIS REPO KEEPS HITTING.

    Every test above installs its own fake hook, so all of them would still pass
    against a service where nothing ever calls
    ``install_demand_refresh_hook`` — a value computed, stored, and read by
    nobody. This one drives the real installer against a stubbed fetch and
    asserts the OVERLAY MOVED.
    """

    def test_the_installed_hook_actually_refreshes_the_overlay(self, monkeypatch) -> None:
        import asyncio
        import types
        from typing import cast

        from services import tenant_names
        from settings import Settings

        async def _fake_get_tenant_info(settings, token_provider=None):
            return {NEW_TENANT_ID: tenant_names.TenantInfo(name="registered-just-now")}

        monkeypatch.setattr(tenant_names, "get_tenant_info", _fake_get_tenant_info)
        settings = types.SimpleNamespace(licenses_url="http://licences.invalid")

        async def _exercise() -> None:
            tenant_names.install_demand_refresh_hook(cast(Settings, settings), None)
            # Nothing vouches for it yet -> the hot path asks for a refresh.
            assert tenancy.is_registered_tenant_id(NEW_TENANT_ID) is False
            for _ in range(10):
                await asyncio.sleep(0)
                if tenancy.is_registered_tenant_id(NEW_TENANT_ID):
                    break
            assert tenancy.is_registered_tenant_id(NEW_TENANT_ID) is True
            assert tenancy.registration_source(NEW_TENANT_ID) == "live"

        try:
            asyncio.run(_exercise())
        finally:
            tenant_names.reset_cache()
