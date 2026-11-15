"""The resolver may only ever attribute spend it can prove belongs to a licence.

Every test here exists because the failure it pins would put a REAL charge on the
WRONG customer's invoice, which is strictly worse than the unattributed row the
resolver is trying to remove.
"""

from __future__ import annotations

import pathlib
import uuid

import pytest

from services.license_resolver import (
    _MAP_TTL_SECONDS,
    _build_app_map,
    _build_map,
    get_app_map,
    parse_service_key_app_map,
    reset_cache,
    resolve_app,
    resolve_license,
)
from settings import Settings


class _FakeResult:
    def __init__(self, rows):
        self._rows = rows

    def all(self):
        return self._rows


class _FakeSession:
    """Stands in for AsyncSession — _build_map only ever calls execute().all()."""

    def __init__(self, rows):
        self._rows = rows

    async def execute(self, _stmt):
        return _FakeResult(self._rows)


LIC_A = uuid.uuid4()
LIC_B = uuid.uuid4()
TENANT = "0195f48b-d000-7000-8000-000000000001"


def test_map_parses_pairs_and_casefolds_the_app():
    parsed = parse_service_key_app_map("inference-router=Workers Module,kag-graph=KAG Module")
    assert parsed == {
        "inference-router": "workers module",
        "kag-graph": "kag module",
    }


def test_default_map_resolves_kag_and_w2v_image_service_keys():
    """`kag` and `w2v-image` were absent from the live default map (measured
    2026-09-04: 4 events/100% NULL and 247 events/19% NULL respectively over
    30d). Both must now resolve, and `mlx-ingestion-backend` must stay pinned
    to KAG Module — a wrong app for it silently unattributes the 92.6% of its
    volume (tenants ...099 and ...101) that resolves correctly today via an
    active KAG Module licence.
    """
    parsed = parse_service_key_app_map(Settings().cost_attribution_app_by_service_key)
    assert parsed["kag"] == "kag module"
    assert parsed["w2v-image"] == "web to video"
    assert parsed["mlx-ingestion-backend"] == "kag module"
    # kag's siblings, unchanged by this addition.
    assert parsed["kag-graph"] == "kag module"
    assert parsed["hirag"] == "kag module"
    assert parsed["karma"] == "kag module"


def test_default_map_resolves_the_w2v_tts_byo_service_key():
    """`w2v-tts-byo` covers modality_gateway's BYO-leg rename.

    A live `w2v-tts` collision (measured 2026-09-07): modality_gateway's and
    tts_backend's `services/metering.py` both shipped the identical literal
    `w2v-tts`, and the two have different billing semantics (tts_backend meters
    every call; modality_gateway meters only its BYO leg). Measured 7d: all 146
    `w2v-tts` rows were `provider='kokoro'` (tts_backend only), so a rename of
    modality_gateway's BYO leg was safe.

    CONFIRMED, not speculative: modality_gateway#37
    (`a4431d23a36940c65addb764513cad7f81b82980`, merged to its `main`
    2026-09-07) renamed the gateway's `SERVICE_KEY` to exactly `w2v-tts-byo`
    and added `tests/test_service_key_drift.py` there pinning
    `EXPECTED_SERVICE_KEYS["tts"] == "w2v-tts-byo"`, so this pair is guarded
    from both sides now. This entry must resolve to the same app as `w2v-tts`
    itself (both are the Web to Video product) so the rename does not also
    unattribute the BYO leg's spend.
    """
    parsed = parse_service_key_app_map(Settings().cost_attribution_app_by_service_key)
    assert parsed["w2v-tts-byo"] == "web to video"
    assert parsed["w2v-tts-byo"] == parsed["w2v-tts"]


def test_default_map_resolves_the_w2v_html_render_service_key():
    """`w2v-html-render` was absent from the map entirely: pricing exists for it
    (scripts/initial_pricing.yaml) but with no attribution entry the resolver
    could never map its cost events to a licence, so `license_id` resolved
    NULL and the spend never reached an invoice line. It is a Web to Video
    sub-service, the same umbrella app already covering
    `w2v-tts`/`w2v-tts-byo`/`w2v-image`.
    """
    parsed = parse_service_key_app_map(Settings().cost_attribution_app_by_service_key)
    assert parsed["w2v-html-render"] == "web to video"
    assert parsed["w2v-html-render"] == parsed["w2v-tts"]


@pytest.mark.asyncio
async def test_html_render_service_key_resolves_via_the_default_map():
    """End-to-end: a `w2v-html-render` cost event with no license_id resolves
    against a tenant's active Web to Video licence, the same way `w2v-image`
    already does. This is the non-vacuity proof for closing the billing-chain
    gap: without the settings.py entry this assertion fails (license_id is
    None), not just the parse-level check above.
    """
    lic = uuid.uuid4()
    app_by_key = parse_service_key_app_map(Settings().cost_attribution_app_by_service_key)
    resolution_map = {(TENANT, "web to video"): lic}
    resolved = resolve_license(
        tenant_id=TENANT,
        service_key="w2v-html-render",  # pragma: allowlist secret
        app_by_service_key=app_by_key,
        resolution_map=resolution_map,
    )
    assert resolved == lic


def test_default_map_resolves_the_delegated_w2v_receiver_keys():
    """`w2v-video-compiler` and `w2v-video-analyzer` were absent from the map.

    A delegated (customer-tenant) call through a w2v_common receiver
    deliberately binds NO licence at the emitter (w2v_common
    `middleware.py:965-972`, issue #170), so the row reaches the flusher with
    `license_id` NULL and this map is the only thing that can attribute it.
    Measured 2026-09-16: merqi's one w2v run produced a `w2v-tts` row that
    resolved (`w2v-tts+resolved`) and two `w2v-video-compiler` rows that stayed
    NULL -- same tenant, same run, same licence. Both keys bill against the
    same `licenses.app` (`w2v`, "Web to Video": video-compiler 365/367
    self-attributed rows, video-analyzer 600/600), so they must resolve to the
    same app as `w2v-tts`. The existing w2v entries are pinned alongside so the
    addition cannot silently displace one.
    """
    parsed = parse_service_key_app_map(Settings().cost_attribution_app_by_service_key)
    assert parsed["w2v-video-compiler"] == "web to video"
    assert parsed["w2v-video-analyzer"] == "web to video"
    assert parsed["w2v-video-compiler"] == parsed["w2v-tts"]
    assert parsed["w2v-video-analyzer"] == parsed["w2v-tts"]
    # Unchanged siblings.
    assert parsed["w2v-tts"] == "web to video"
    assert parsed["w2v-tts-byo"] == "web to video"
    assert parsed["w2v-image"] == "web to video"
    assert parsed["w2v-html-render"] == "web to video"


@pytest.mark.parametrize(
    "service_key",
    ["w2v-video-compiler", "w2v-video-analyzer"],  # pragma: allowlist secret
)
def test_delegated_w2v_receiver_key_resolves_against_the_tenants_w2v_licence(service_key):
    """Non-vacuity: a licence-less row under either key resolves to the tenant's
    single active Web to Video licence through the SHIPPED default -- exactly
    the path the delegated merqi rows take. Without the settings.py entry this
    returns None, not merely fails the parse-level check above.
    """
    lic = uuid.uuid4()
    app_by_key = parse_service_key_app_map(Settings().cost_attribution_app_by_service_key)
    resolved = resolve_license(
        tenant_id=TENANT,
        service_key=service_key,
        app_by_service_key=app_by_key,
        resolution_map={(TENANT, "web to video"): lic},
    )
    assert resolved == lic


def test_delegated_w2v_receiver_key_stays_null_for_a_tenant_without_a_w2v_licence():
    """No false attribution: a tenant that holds licences for OTHER apps but no
    Web to Video licence must keep `license_id` NULL for a `w2v-video-compiler`
    row. The map decides which invoice line real money lands on; resolving to
    a KAG or Workers licence here would be a mis-bill, not a fix.
    """
    app_by_key = parse_service_key_app_map(Settings().cost_attribution_app_by_service_key)
    other_tenant_lic = uuid.uuid4()
    resolution_map = {
        (TENANT, "kag module"): uuid.uuid4(),
        (TENANT, "workers module"): uuid.uuid4(),
        # Another tenant's w2v licence must not leak across the tenant key.
        ("other-tenant", "web to video"): other_tenant_lic,
    }
    resolved = resolve_license(
        tenant_id=TENANT,
        service_key="w2v-video-compiler",  # pragma: allowlist secret
        app_by_service_key=app_by_key,
        resolution_map=resolution_map,
    )
    assert resolved is None


def test_w2v_tts_still_resolves_exactly_as_before_the_receiver_keys_were_added():
    """The existing `w2v-tts` case, unchanged by the two new entries."""
    lic = uuid.uuid4()
    app_by_key = parse_service_key_app_map(Settings().cost_attribution_app_by_service_key)
    resolved = resolve_license(
        tenant_id=TENANT,
        service_key="w2v-tts",  # pragma: allowlist secret
        app_by_service_key=app_by_key,
        resolution_map={(TENANT, "web to video"): lic},
    )
    assert resolved == lic


@pytest.mark.asyncio
async def test_kag_service_key_resolves_via_the_default_map():
    """End-to-end: a `kag` cost event with no license_id now resolves against
    a tenant's KAG Module licence, the same way `kag-graph` already does.
    """
    lic = uuid.uuid4()
    app_by_key = parse_service_key_app_map(Settings().cost_attribution_app_by_service_key)
    resolution_map = {(TENANT, "kag module"): lic}
    resolved = resolve_license(
        tenant_id=TENANT,
        service_key="kag",  # pragma: allowlist secret
        app_by_service_key=app_by_key,
        resolution_map=resolution_map,
    )
    assert resolved == lic


def test_map_survives_operator_typos_rather_than_disabling_attribution():
    # A trailing comma or a stray blank must not take the whole fleet's
    # attribution down, and a pair with no '=' is skipped, not raised: this is
    # parsed on the flush path, where raising would cost real billing data.
    parsed = parse_service_key_app_map("  , inference-router = Workers Module ,,garbage,")
    assert parsed == {"inference-router": "workers module"}


@pytest.mark.asyncio
async def test_ambiguous_tenant_app_pair_resolves_to_nothing():
    """THE test. Two active licences for one (tenant, app) must resolve to NONE.

    Picking the first row is guessing, and a guessed license_id is an invented
    invoice line. A mutant that returns lic_ids[0] instead of refusing the key
    fails here by name.

    The key is now PRESENT WITH A ``None`` VALUE rather than absent. That is a
    reporting change, not a resolution change — see
    ``test_an_ambiguous_pair_still_resolves_to_no_licence`` below, which pins the
    behaviour this test used to pin via the map shape.
    """
    session = _FakeSession(
        [
            (TENANT, "Workers Module", LIC_A),
            (TENANT, "Workers Module", LIC_B),
        ]
    )
    built = await _build_map(session)
    assert built == {(TENANT, "workers module"): None}
    assert built[(TENANT, "workers module")] not in (LIC_A, LIC_B), "a licence was guessed"


@pytest.mark.asyncio
async def test_unambiguous_pair_resolves_and_is_casefolded():
    session = _FakeSession([(TENANT, "Workers Module", LIC_A)])
    built = await _build_map(session)
    assert built == {(TENANT, "workers module"): LIC_A}


@pytest.mark.asyncio
async def test_ambiguity_does_not_poison_the_unambiguous_pairs_beside_it():
    session = _FakeSession(
        [
            (TENANT, "Workers Module", LIC_A),
            (TENANT, "Workers Module", LIC_B),
            (TENANT, "KAG Module", LIC_A),
        ]
    )
    built = await _build_map(session)
    assert built[(TENANT, "kag module")] == LIC_A
    assert built[(TENANT, "workers module")] is None


@pytest.mark.asyncio
async def test_rows_without_a_resolvable_app_are_not_candidates():
    # app_name NULL is a licence synced before migration 0014, or one whose app
    # upstream omitted. It must not become a candidate under an empty-string key.
    session = _FakeSession([(TENANT, None, LIC_A), (None, "Workers Module", LIC_B)])
    assert await _build_map(session) == {}


def _map():
    return {(TENANT, "workers module"): LIC_A}


def test_resolves_a_mapped_service_key():
    assert (
        resolve_license(
            tenant_id=TENANT,
            # A routing slug, not a credential: detect-secrets' `Secret Keyword`
            # plugin fires on any `*_key="..."` assignment regardless of value.
            service_key="inference-router",  # pragma: allowlist secret
            app_by_service_key={"inference-router": "workers module"},
            resolution_map=_map(),
        )
        == LIC_A
    )


@pytest.mark.parametrize(
    ("tenant_id", "service_key", "app_map", "why"),
    [
        (
            TENANT,
            "unmapped-service",
            {"inference-router": "workers module"},
            "unmapped service_key",
        ),
        (TENANT, "inference-router", {"inference-router": "kag module"}, "tenant owns no such app"),
        (None, "inference-router", {"inference-router": "workers module"}, "no tenant"),
        (TENANT, None, {"inference-router": "workers module"}, "no service_key"),
        (
            "some-other-tenant",
            "inference-router",
            {"inference-router": "workers module"},
            "other tenant",
        ),
    ],
)
def test_never_fabricates_a_licence(tenant_id, service_key, app_map, why):
    """Every one of these must be None. None is the SAFE outcome, not a failure.

    A resolver that falls back to "any licence of this tenant" would pass the
    happy-path test above and bill the wrong invoice line here.
    """
    assert (
        resolve_license(
            tenant_id=tenant_id,
            service_key=service_key,
            app_by_service_key=app_map,
            resolution_map=_map(),
        )
        is None
    ), why


def test_0014_follows_0013_and_keeps_the_chain_linear():
    """0014 adds the app columns the resolver reads; a branch would make the
    upgrade path ambiguous and could leave app_id absent in one environment."""
    from alembic.config import Config
    from alembic.script import ScriptDirectory

    root = pathlib.Path(__file__).resolve().parents[1]
    cfg = Config(str(root / "alembic.ini"))
    cfg.set_main_option("script_location", str(root / "alembic"))
    script = ScriptDirectory.from_config(cfg)

    heads = list(script.get_heads())
    assert len(heads) == 1, f"alembic chain has branched: {heads}"
    assert script.get_revision("0014_quota_config_app").down_revision == (
        "0013_park_router_duplicates"
    )


# ---------------------------------------------------------------------------
# App derivation — licence -> app
# ---------------------------------------------------------------------------

APP_X = uuid.uuid4()
APP_Y = uuid.uuid4()
LIC_C = uuid.uuid4()
LIC_D = uuid.uuid4()


@pytest.mark.asyncio
async def test_app_map_maps_each_licence_to_its_app():
    session = _FakeSession([(LIC_A, APP_X), (LIC_B, APP_Y)])
    assert await _build_app_map(session) == {LIC_A: APP_X, LIC_B: APP_Y}


@pytest.mark.asyncio
async def test_licences_without_an_app_are_left_unattributed():
    """A NULL app_id must not become an edge — see rule 3 and rule 1.

    The SELECT already filters `app_id IS NOT NULL`, but the stand-in cannot
    honour a SQL predicate, so what this pins is the in-Python guard that has to
    agree with it. Without both, a licence synced before migration 0014 would
    map to `None` and every event on it would be stamped with a NULL app that
    later reads as "attributed".
    """
    session = _FakeSession([(LIC_A, APP_X), (LIC_B, None)])
    built = await _build_app_map(session)
    assert built == {LIC_A: APP_X}
    assert LIC_B not in built


@pytest.mark.asyncio
async def test_two_licences_for_one_app_both_resolve_because_the_key_is_the_licence():
    """THE test, and the reason this derivation is safe where the other is not.

    `license_id` is the PRIMARY KEY of request_quota_config, so a licence maps to
    exactly one app by construction and rule 2 ("exactly one, or nothing") is
    satisfied by the schema, not by a filter. The rows below are the EXACT shape
    that makes `_build_map` drop its key — same tenant, same app, two licences —
    and here both must survive. A copy-paste of `_build_map`'s ambiguity pass
    into `_build_app_map` would silently return {} and un-attribute the busiest
    tenants (measured: 7 such pairs, 16 licences).
    """
    session = _FakeSession([(LIC_A, APP_X), (LIC_B, APP_X)])
    built = await _build_app_map(session)
    assert built == {LIC_A: APP_X, LIC_B: APP_X}


def test_never_fabricates_an_app():
    """None is the SAFE outcome: an invented app is an invented product line."""
    app_map = {LIC_A: APP_X}
    assert resolve_app(license_id=None, app_map=app_map) is None
    assert resolve_app(license_id=LIC_C, app_map=app_map) is None
    assert resolve_app(license_id=LIC_A, app_map=app_map) == APP_X


@pytest.mark.asyncio
async def test_app_map_is_reused_within_the_ttl_and_rebuilt_after_it():
    reset_cache()
    first = _FakeSession([(LIC_A, APP_X)])
    second = _FakeSession([(LIC_B, APP_Y)])

    assert await get_app_map(first, now=1000.0) == {LIC_A: APP_X}
    # Inside the TTL the second session is never executed at all — a rebuild here
    # would mean one query per flush batch instead of one per interval.
    assert await get_app_map(second, now=1000.0 + _MAP_TTL_SECONDS - 1) == {LIC_A: APP_X}
    # At the boundary the map expires, so a newly provisioned licence stays
    # unattributed for at most one interval rather than forever.
    assert await get_app_map(second, now=1000.0 + _MAP_TTL_SECONDS) == {LIC_B: APP_Y}
    reset_cache()


@pytest.mark.asyncio
async def test_the_ambiguous_pair_gauge_reports_the_pairs_actually_dropped():
    """`billing_attribution_ambiguous_pairs` had ZERO tests before this one.

    It is the only signal that a (tenant, app) pair has been switched off — an
    ambiguous pair resolves to NOTHING by rule 2, so every event for it stays
    license_id NULL forever, silently. A gauge nothing exercises is a gauge that
    can quietly stop tracking what it names, which is worse than not having it:
    an operator would read a stale 0.0 as "no duplicates".

    Both directions are asserted. Reporting the COUNT is one half; FALLING back
    to 0 when the duplicates are cleaned up is the other, and it is the half a
    `if ambiguous:` guard around the `.set()` would break — the gauge would then
    stick at its high-water mark and no cleanup could ever be seen to land.
    """
    import observability.metrics as om

    tenant_2 = "0195f48b-d000-7000-8000-000000000002"
    session = _FakeSession(
        [
            # Ambiguous pair 1: one tenant, one app, two licences.
            (TENANT, "Workers Module", LIC_A),
            (TENANT, "Workers Module", LIC_B),
            # Ambiguous pair 2, for a different tenant.
            (tenant_2, "KAG Module", LIC_C),
            (tenant_2, "KAG Module", uuid.uuid4()),
            # ...and one unambiguous pair that must survive beside them.
            (TENANT, "KAG Module", LIC_D),
        ]
    )
    built = await _build_map(session)

    # Exactly the unattributable pairs, not the licences behind them (4) and not
    # the candidate keys (3): 2 pairs were switched off.
    assert om.attribution_ambiguous_pairs._value.get() == 2
    # The gauge counts exactly the pairs the map REFUSED — now carried as
    # present-with-None rather than as absent, so the two can be compared
    # directly instead of inferred from a missing key.
    assert {key for key, lic in built.items() if lic is None} == {
        (TENANT, "workers module"),
        (tenant_2, "kag module"),
    }
    # ...and the unambiguous pair beside them still resolves.
    assert built[(TENANT, "kag module")] == LIC_D

    # Clean the registry up and the gauge must come back DOWN.
    await _build_map(_FakeSession([(TENANT, "Workers Module", LIC_A)]))
    assert om.attribution_ambiguous_pairs._value.get() == 0


@pytest.mark.asyncio
async def test_reset_cache_clears_the_app_map_too():
    """Regression guard: reset_cache() once cleared only the licence map.

    The timestamps are deliberately one second apart — well inside the TTL — so
    a reset that forgot the app map would hand back the STALE map here instead of
    rebuilding, and every test relying on reset_cache() would silently share
    state with whichever test ran first.
    """
    reset_cache()
    assert await get_app_map(_FakeSession([(LIC_A, APP_X)]), now=1000.0) == {LIC_A: APP_X}

    reset_cache()
    assert await get_app_map(_FakeSession([(LIC_B, APP_Y)]), now=1001.0) == {LIC_B: APP_Y}
    reset_cache()


def test_default_map_resolves_the_fr_backend_service_key():
    """`fr-backend` was absent from the live default map — measured 2026-09-13,
    24h: 8,257 events, 100% licence-NULL, and 99.6% of ALL usage-bus volume.

    ⛔ THE DECOY HERE IS NOT EMPTY, which is what makes this entry different
    from `w2v-image` and `mlx-ingestion-backend` above. Those were settled by
    the candidate app having ZERO grants fleet-wide; a distinctly named
    "fr Frontend" app exists AND carries live grants, so that argument is not
    available and coverage is what decides it. Of the 6 real emitting tenants:

        FR Module    12 active grants fleet-wide — held by 6 of 6
        fr Frontend   3 active grants fleet-wide — held by 1 of 6

    Each of the 6 holds exactly ONE active FR Module licence, so every pair is
    unambiguous and the resolver's refuse-on-ambiguity branch is not leaned on.

    ⚠️ `fr Frontend` must never become the value here: it would resolve one
    emitter out of six and silently unattribute the rest.
    """
    parsed = parse_service_key_app_map(Settings().cost_attribution_app_by_service_key)
    assert parsed["fr-backend"] == "fr module"
    assert "fr frontend" not in parsed.values()
    # The keys this addition must not disturb — each settled by its own
    # measurement in the setting's comments.
    assert parsed["commonpractice"] == "common practice"
    assert parsed["mlx-ingestion-backend"] == "kag module"
    assert parsed["w2v-image"] == "web to video"


class TestTheDerivedAttributionCounterIsBornForEveryMappedKey:
    """Absence and silence are the same reading for a service_key never born.

    ⚠️ NOT A FIX FOR A DEAD COUNTER. ``record_resolved`` increments correctly and
    was measured doing so live on 2026-09-14. No increment is added anywhere here
    — a second writer on a live counter is a defect, not a remedy.

    What was measured is THINNESS: ONE series against the whole
    ``cost_attribution_app_by_service_key`` map — 14 keys on the shipped default
    that day; this test reads the setting rather than pinning a count, because the
    map grows. A prometheus_client child is created only when that key has actually
    resolved a licence, and both ``billing:licence_derived_share:1h`` and
    ``BillingLicenceAssertionRegressed`` are evaluated ``by (service_key)`` — so
    for every unborn key "this emitter's resolution stopped" and "it has never
    happened" are the SAME empty vector, which is vacuously green.
    """

    @staticmethod
    def _children() -> set[str]:
        from observability.metrics import license_resolved_total

        return {
            sample.labels["service_key"]
            for metric in license_resolved_total.collect()
            for sample in metric.samples
        }

    def test_the_birth_list_is_the_reachable_set_and_nothing_else(self) -> None:
        """⛔ ASSERTED AGAINST THE PRODUCING CODE'S OWN CONFIG, not a roster beside it.

        ``resolve_license`` returns None unless ``app_by_service_key.get(key)``
        hits, so the values ``record_resolved`` can produce ARE the keys of this
        mapping. Deriving the birth list from the same
        ``parse_service_key_app_map`` output ``cost_flusher`` gates the increment
        on is what stops the two drifting; a hand-copied list would not.
        """
        from services.license_resolver import birth_resolved_series

        app_by_key = parse_service_key_app_map(Settings().cost_attribution_app_by_service_key)
        born = birth_resolved_series(app_by_key)

        assert set(born) == set(app_by_key)
        assert born == tuple(sorted(app_by_key)), "deterministic, so two runs agree"
        assert self._children() >= set(app_by_key)

    def test_unknown_is_never_birthed(self) -> None:
        """⛔ THE FABRICATION CONTROL. ``record_resolved`` carries an
        ``or "unknown"`` fallback, and the sibling counter
        ``billing_license_unattributed_persisted_total`` legitimately births that
        value. This one must NOT: ``cost_flusher`` normalises every staged row to
        a non-empty ``service_key`` before this point, and a licence is only ever
        resolved for a key the operator mapped — so no reachable path produces it.
        A series born for it would sit at 0 forever and read as a working
        instrument watching a healthy population while watching nothing.
        """
        from services.license_resolver import birth_resolved_series

        app_by_key = parse_service_key_app_map(Settings().cost_attribution_app_by_service_key)
        assert "unknown" not in app_by_key, "the premise: the default map maps no such key"
        assert "unknown" not in birth_resolved_series(app_by_key)

    def test_an_unmapped_key_is_not_birthed(self) -> None:
        """NEGATIVE CONTROL on the list itself: birthing everything would pass
        the first test's ``>=`` half while destroying its meaning.
        """
        from services.license_resolver import birth_resolved_series

        born = birth_resolved_series({"r9b-mapped-probe": "Probe Module"})
        assert born == ("r9b-mapped-probe",)
        assert "r9b-never-mapped-probe" not in self._children()


@pytest.mark.asyncio
class TestTheBirthActuallyRunsAtBoot:
    """A birth helper nobody calls is worth exactly as much as no birth at all.

    ⭐ THE MUTATION THIS CATCHES: deleting the ``birth_resolved_series(...)`` line
    from ``main.lifespan``. The probe key below is unique to this test, so no other
    test in the process can create that child — which is what keeps this from
    being the vacuous registry scan that passes on somebody else's increment.

    Boot is the right moment rather than first-flush: the reachable domain is
    operator config, known once settings resolve, and a pod whose cost bus is
    quiet is exactly the pod whose empty series must not read as health.
    """

    _PROBE = "r9b-boot-birth-probe"

    async def test_lifespan_births_the_configured_service_keys(
        self, test_settings, fake_redis, db_session_factory, monkeypatch
    ):
        from unittest.mock import AsyncMock, patch

        from prometheus_client import REGISTRY

        import main as main_module

        # Built from a variable rather than written as a literal on a `*_key`
        # assignment: detect-secrets' "Secret Keyword" rule fires on that shape,
        # and an inline pragma is fragile because ruff-format may reflow the line
        # out from under it — the trap observability/metrics.py records against
        # tests/test_service_key_is_not_fabricated.py.
        probe_map = f"{self._PROBE}=Probe Module"
        test_settings.cost_attribution_app_by_service_key = probe_map
        monkeypatch.setattr(main_module, "get_settings", lambda: test_settings)

        sample = "billing_license_resolved_total"
        assert REGISTRY.get_sample_value(sample, {"service_key": self._PROBE}) is None, (
            "premise: nothing in this process has produced the probe key yet"
        )

        with (
            patch("main.init_redis", new_callable=AsyncMock, return_value=fake_redis),
            patch("main.init_db", new_callable=AsyncMock),
            patch("main.dispose_engine", new_callable=AsyncMock),
            patch("main.close_redis", new_callable=AsyncMock),
            patch("db.get_session_factory", return_value=db_session_factory),
        ):
            async with main_module.lifespan(main_module.app):
                born = REGISTRY.get_sample_value(sample, {"service_key": self._PROBE})

        assert born == 0.0, (
            "main.lifespan no longer births billing_license_resolved_total; every "
            "configured service_key that has not yet resolved a licence is back to "
            "an empty vector, which every by(service_key) rule reads as health"
        )


class TestDuplicateActiveLicencesAreNamedRatherThanSilent:
    """A duplicated licence and an absent one are different defects; say which.

    THE LIVE POPULATION. Measured on the prod billing primary 2026-09-16:
    ``request_quota_config`` holds TWO active rows for tenant
    ``e4f70a83-81ee-429a-abca-6999e3a0d24f`` / app ``RaiseMyTickets``, so
    ``_build_map`` refuses the pair and every event on it is written with
    ``license_id IS NULL`` — 49 such rows. That spend is billed and it drains the
    tenant budget pool, but it can never reach a licence-keyed invoice line.

    WHAT WAS SILENT. Nothing at the event level said "ambiguous". The only
    evidence was ``billing_attribution_ambiguous_pairs`` — an UNLABELLED gauge
    counting registry PAIRS, not lost events — and an ``info`` log emitted at
    most once per 300s map build. ``billing_license_unattributed_persisted_total``
    counted the 49 rows but could not distinguish them from rows for a tenant
    that simply owns no licence, and the two need opposite actions: deduplicate
    the registry vs. sell/sync a licence.

    ⛔ WHAT DID NOT CHANGE. Resolution. An ambiguous pair returned ``None``
    before and returns ``None`` after; rule 2 ("exactly one, or nothing") is
    intact and no licence is ever guessed. The tests below assert that first,
    because a counter bought with a behaviour change would be a regression
    wearing an instrument.
    """

    @staticmethod
    def _count(service_key: str) -> float:
        import observability.metrics as om

        return om.license_ambiguous_total.labels(service_key=service_key)._value.get()

    @staticmethod
    def _ambiguous_map():
        return {(TENANT, "workers module"): None}

    def test_an_ambiguous_pair_still_resolves_to_no_licence(self):
        """⛔ THE BEHAVIOUR GUARD. This is what the old ``built == {}`` pinned.

        If this ever returns a licence, the change stopped being a reporting
        change and started putting a real charge on a guessed invoice line.
        """
        assert (
            resolve_license(
                tenant_id=TENANT,
                service_key="inference-router",  # pragma: allowlist secret
                app_by_service_key={"inference-router": "workers module"},
                resolution_map=self._ambiguous_map(),
            )
            is None
        )

    def test_an_ambiguous_pair_is_counted(self):
        """The whole point: the 49 rows can now be told apart from the rest."""
        before = self._count("inference-router")
        resolve_license(
            tenant_id=TENANT,
            service_key="inference-router",  # pragma: allowlist secret
            app_by_service_key={"inference-router": "workers module"},
            resolution_map=self._ambiguous_map(),
        )
        assert self._count("inference-router") == before + 1

    def test_a_pair_with_no_licence_at_all_is_not_counted_as_ambiguous(self):
        """⭐ THE NEGATIVE CONTROL. Absent is not duplicated.

        This is the ordinary state of every tenant who has not bought the app,
        and it is by far the larger population — 2,886 rows carry NULL
        license_id in total against the 49 lost to the duplicate. A counter that
        fired here too would report the whole unattributed backlog as a registry
        defect and bury the one pair somebody can actually fix.

        It must also behave EXACTLY as it did before this commit: no licence, no
        exception, no counter.
        """
        before = self._count("inference-router")
        assert (
            resolve_license(
                tenant_id=TENANT,
                service_key="inference-router",  # pragma: allowlist secret
                app_by_service_key={"inference-router": "workers module"},
                resolution_map={},
            )
            is None
        )
        assert self._count("inference-router") == before, "an absent pair was called ambiguous"

    def test_a_resolvable_pair_is_not_counted(self):
        """The other direction — a healthy pair must leave the instrument alone."""
        before = self._count("inference-router")
        assert (
            resolve_license(
                tenant_id=TENANT,
                service_key="inference-router",  # pragma: allowlist secret
                app_by_service_key={"inference-router": "workers module"},
                resolution_map=_map(),
            )
            == LIC_A
        )
        assert self._count("inference-router") == before

    def test_an_unmapped_service_key_is_not_counted(self):
        """It returns None for a different reason, and that reason is not this one.

        The fix is an operator setting (``cost_attribution_app_by_service_key``),
        not a registry deduplication.
        """
        before = self._count("kag")
        assert (
            resolve_license(
                tenant_id=TENANT,
                service_key="kag",  # pragma: allowlist secret
                app_by_service_key={},
                resolution_map=self._ambiguous_map(),
            )
            is None
        )
        assert self._count("kag") == before

    @pytest.mark.asyncio
    async def test_the_map_distinguishes_duplicated_from_absent(self):
        """The two shapes that carry the distinction, asserted together.

        ``key present -> None`` means several; ``key absent`` means none. A
        future edit that goes back to dropping the key would make the counter
        above permanently unreachable while every other test still passed.
        """
        session = _FakeSession(
            [
                (TENANT, "Workers Module", LIC_A),
                (TENANT, "Workers Module", LIC_B),
                (TENANT, "KAG Module", LIC_A),
            ]
        )
        built = await _build_map(session)
        assert built[(TENANT, "workers module")] is None
        assert built[(TENANT, "kag module")] == LIC_A
        assert (TENANT, "never sold") not in built

    @pytest.mark.asyncio
    async def test_the_pair_gauge_still_counts_pairs_not_events(self):
        """The gauge and the counter measure different things and both must work.

        A duplicate nobody's traffic hits moves the gauge and never the counter.
        """
        import observability.metrics as om

        session = _FakeSession(
            [
                (TENANT, "Workers Module", LIC_A),
                (TENANT, "Workers Module", LIC_B),
            ]
        )
        await _build_map(session)
        assert om.attribution_ambiguous_pairs._value.get() == 1
