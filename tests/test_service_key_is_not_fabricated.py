"""``extract_service_key`` must not invent a service that does not exist.

THE DEFECT
----------
The miss branch returned ``parts[0]`` — the first path segment, verbatim and
unchecked. Every request to fr's ``/api/...`` surface, ~2,733 events/day
including the n8n webhooks, was therefore filed under a ``service_key`` of
``api``. There is no service called ``api``. Nothing emits under that name and
nothing serves it; the value was minted by the miss branch itself.

What made it expensive is that it was INDISTINGUISHABLE from the ~30 real keys
beside it: same lowercase-slug shape, same ``metering_events.service_key``
column, same ``service_key`` Prometheus label on four counters. A dashboard row
reading ``api`` looks like a measurement. It is the same class of defect as the
``"unknown"`` pseudo-tenant that accumulated 13,670 rows in :mod:`tenancy`.

WHAT WAS MEASURED FIRST
-----------------------
Both live Redis streams were sampled (400 entries each) on 2026-09-05 before
choosing the replacement:

* ``billing:events`` fields are tenant_id, subject_id, license_id, app_id,
  plan_id, plan_code, method, path, status, source_ip, timestamp, latency_ms.
  There is NO ``service_key``. 400/400 sampled paths began ``/api``.
* ``billing:costs`` entries DO carry ``service_key`` (``mlx-backend``, …), which
  is why the payload branch exists — an emitter that names itself is believed
  rather than re-guessed from its path.
* ``source_ip`` carried ``"fr-backend"`` — a service NAME, not an IP — on
  400/400 events. It is deliberately NOT used: it names the CALLER, while every
  key in ``SERVICE_KEY_MAP`` names the service being CONSUMED. Substituting it
  would swap a visible fabrication for an invisible one.
"""

from __future__ import annotations

import pytest

from services.consumer import UNMAPPED_SERVICE_KEY, extract_service_key

pytestmark = pytest.mark.unit


class TestMappedPathsAreUnchanged:
    """The control. This change must be invisible to every path that resolved."""

    @pytest.mark.parametrize(
        "path,expected",
        [
            ("/workers/tasks", "workers"),
            ("/kag/query", "kag"),
            ("/kag-graph/ingest", "kag-graph"),  # longer prefix wins
            ("/kag-orchestrator/run", "kag-orchestrator"),
            ("/skills/list", "skill-builder"),
            ("/rtx/v1/completions", "vllm"),
        ],
    )
    def test_prefix_map_still_wins(self, path: str, expected: str) -> None:
        assert extract_service_key(path) == expected

    def test_the_prefix_map_beats_a_payload_that_disagrees(self) -> None:
        """Order matters: a mapped path resolves exactly as it did before.

        Letting the payload override would change the key of live, already-
        mapped traffic as a side effect of a fix aimed at the MISS branch.
        """
        # The literal is bound to a variable rather than written inline:
        # detect-secrets' "Secret Keyword" rule fires on `"*_key": "<literal>"`,
        # and an inline `# pragma: allowlist secret` is fragile here because
        # ruff-format reflows the call and moves the comment off the flagged
        # line — which is exactly how this file first went red in CI while
        # passing the local pre-commit hook.
        declared = "something-else"
        assert extract_service_key("/workers/tasks", {"service_key": declared}) == "workers"


class TestTheFabricationIsGone:
    def test_the_fr_api_surface_is_no_longer_filed_under_a_service_called_api(self) -> None:
        """The measured case: ~2,733 events/day.

        The assertion that matters is the negative one — whatever this returns,
        it must not be a value that reads like a real service key.
        """
        key = extract_service_key("/api/webhooks/n8n/abc123")
        assert key != "api"
        assert key.startswith(f"{UNMAPPED_SERVICE_KEY}:")
        assert key == "unmapped:api"

    @pytest.mark.parametrize(
        "path",
        ["/api/andon", "/api/tools/n8n/executions", "/api/tools/kanban/liveness"],
    )
    def test_every_sampled_live_path_lands_in_the_unmapped_bucket(self, path: str) -> None:
        assert extract_service_key(path) == "unmapped:api"

    def test_the_segment_is_kept_so_the_offender_stays_nameable(self) -> None:
        """An opaque constant would make every unmapped emitter look identical."""
        assert extract_service_key("/totally-new-service/x") == "unmapped:totally-new-service"

    @pytest.mark.parametrize("path", ["", "/", "///"])
    def test_a_pathless_event_gets_the_bare_marker(self, path: str) -> None:
        assert extract_service_key(path) == UNMAPPED_SERVICE_KEY

    @pytest.mark.parametrize(
        "path",
        [
            "/" + "x" * 200,  # over the length bound
            "/Bearer eyJhbGciOi",  # a leaked header shape, with a space
            "/../../etc/passwd",
            "/%2e%2e",
        ],
    )
    def test_an_unusable_segment_does_not_reach_the_label(self, path: str) -> None:
        """``service_key`` is a String(64) column AND a Prometheus label.

        The old code appended the raw segment with no shape check at all, so a
        caller controlled both. This is strictly narrower than that.
        """
        key = extract_service_key(path)
        assert key == UNMAPPED_SERVICE_KEY
        assert len(key) <= 64

    def test_every_produced_key_fits_the_column(self) -> None:
        assert len(extract_service_key("/" + "a" * 39 + "/x")) <= 64


class TestThePayloadNamesTheEmitter:
    def test_an_explicit_service_key_is_believed_on_a_miss(self) -> None:
        """Evidence beats a guess. Dormant on billing:events today, by measurement."""
        emitter = "mlx-backend"  # bound, not inline — see the note above
        assert extract_service_key("/api/x", {"service_key": emitter}) == emitter

    @pytest.mark.parametrize("declared", ["", "   ", None])
    def test_an_absent_or_blank_declaration_is_not_a_name(self, declared) -> None:
        fields = {} if declared is None else {"service_key": declared}
        assert extract_service_key("/api/x", fields) == "unmapped:api"

    def test_source_ip_is_not_promoted_to_a_service_key(self) -> None:
        """It names the CALLER. 400/400 sampled events carried ``fr-backend``."""
        assert extract_service_key("/api/andon", {"source_ip": "fr-backend"}) == "unmapped:api"

    def test_a_declared_key_is_truncated_to_the_column_width(self) -> None:
        overlong = "z" * 300
        assert len(extract_service_key("/api/x", {"service_key": overlong})) == 64


class TestTheMissIsCounted:
    def _read(self, service_key: str = "unmapped:api") -> float:
        """Read ONE series. The counter gained a {service_key} label on
        2026-09-05; before that a bare ``._value`` read was the whole metric.
        """
        from observability.metrics import service_key_unmapped_total

        return service_key_unmapped_total.labels(service_key=service_key)._value.get()

    def _read_total(self) -> float:
        """Sum every series, i.e. what the unlabelled counter used to report."""
        from observability.metrics import service_key_unmapped_total

        # Summed off `collect()` rather than the private `_metrics` map: it is
        # the public read path, and it keeps `ty` from having to reason about
        # the generic child type at all.
        return sum(
            sample.value
            for metric in service_key_unmapped_total.collect()
            for sample in metric.samples
        )

    def test_a_miss_increments_and_a_hit_does_not(self) -> None:
        """Without this the fabrication is merely renamed, not made measurable."""
        before = self._read_total()
        extract_service_key("/workers/tasks")
        emitter = "mlx-backend"
        extract_service_key("/api/x", {"service_key": emitter})
        assert self._read_total() == before, "a resolved key must not count as unmapped"
        extract_service_key("/api/webhooks/n8n/abc")
        assert self._read_total() == before + 1

    def test_the_increment_lands_on_the_offenders_own_series(self) -> None:
        """The whole point of the label: 957 increments in 24h that named nobody.

        A total that moves is what the bare counter already gave. What was
        missing — and what an operator has to have to act — is WHICH emitter.
        """
        before_api = self._read("unmapped:api")
        before_zz = self._read("unmapped:zzdistinct")
        extract_service_key("/api/webhooks/n8n/abc")
        assert self._read("unmapped:api") == before_api + 1
        assert self._read("unmapped:zzdistinct") == before_zz, (
            "an increment must not smear across other emitters' series"
        )

    def test_an_unusable_first_segment_counts_under_the_bare_key(self) -> None:
        """``extract_service_key`` returns bare ``unmapped`` here, and the
        counter must agree with the value the ROW is filed under — the two are
        only joinable if they are the same string.
        """
        before = self._read("unmapped")
        assert extract_service_key("/!!!/x") == "unmapped"
        assert self._read("unmapped") == before + 1

    def test_both_birthed_series_exist_before_any_traffic(self) -> None:
        """A labelled counter exports NO samples until its first ``.labels()``.

        Without the import-time birth, "no unmapped events" and "the series does
        not exist" are the same empty vector, and every query written against
        this metric name silently answers nothing.
        """
        from observability.metrics import service_key_unmapped_total

        exported = {
            sample.labels["service_key"]
            for metric in service_key_unmapped_total.collect()
            for sample in metric.samples
        }
        assert {"unmapped", "unmapped:other"} <= exported


class TestTheStagedEventCarriesTheHonestKey:
    def test_stage_message_files_an_unmapped_event_honestly(self) -> None:
        """End to end through the consumer's own staging path, not just the helper."""
        from services.consumer import _stage_message
        from settings import Settings

        settings = Settings()
        events: list[dict] = []
        costs: list[dict] = []
        _stage_message(
            {"path": "/api/webhooks/n8n/abc", "tenant_id": "n8n-dev"},
            "1-1",
            settings.events_stream,
            settings,
            events,
            costs,
        )
        assert events
        assert events[0]["_service_key"] == "unmapped:api"  # pragma: allowlist secret


class TestTheLabelDomainIsClamped:
    """The segment is CALLER-CONTROLLED, so the label is only safe because it is
    bounded. These tests are the bound: without them a later edit can drop the
    clamp and nothing goes red until a pod's series count explodes in prod.
    """

    def setup_method(self) -> None:
        from services.consumer import _reset_unmapped_label_clamp

        _reset_unmapped_label_clamp()

    def teardown_method(self) -> None:
        from services.consumer import _reset_unmapped_label_clamp

        _reset_unmapped_label_clamp()

    def test_the_first_cap_distinct_keys_keep_their_own_series(self) -> None:
        from services.consumer import _UNMAPPED_LABEL_CAP, _clamp_unmapped_label

        for i in range(_UNMAPPED_LABEL_CAP):
            key = f"unmapped:svc{i}"
            assert _clamp_unmapped_label(key) == key

    def test_the_key_after_the_cap_folds_into_other(self) -> None:
        from services.consumer import (
            _UNMAPPED_LABEL_CAP,
            _UNMAPPED_LABEL_OTHER,
            _clamp_unmapped_label,
        )

        for i in range(_UNMAPPED_LABEL_CAP):
            _clamp_unmapped_label(f"unmapped:svc{i}")
        assert _clamp_unmapped_label("unmapped:overflow") == _UNMAPPED_LABEL_OTHER

    def test_an_already_admitted_key_survives_the_cap_being_reached(self) -> None:
        """A key must not lose its series once admitted, or its counter would
        appear to RESET — an admitted series that later folds into `other` reads
        as a restart to every rate() over it.
        """
        from services.consumer import _UNMAPPED_LABEL_CAP, _clamp_unmapped_label

        first = "unmapped:early"
        assert _clamp_unmapped_label(first) == first
        for i in range(_UNMAPPED_LABEL_CAP + 50):
            _clamp_unmapped_label(f"unmapped:svc{i}")
        assert _clamp_unmapped_label(first) == first

    def test_repeats_do_not_consume_cap_slots(self) -> None:
        from services.consumer import _UNMAPPED_LABEL_CAP, _clamp_unmapped_label

        for _ in range(_UNMAPPED_LABEL_CAP * 10):
            assert _clamp_unmapped_label("unmapped:noisy") == "unmapped:noisy"
        assert _clamp_unmapped_label("unmapped:second") == "unmapped:second"

    def test_a_hostile_caller_cannot_mint_unbounded_series(self) -> None:
        """The measured threat: a fresh path per request. 500 distinct keys must
        produce at most CAP+1 distinct label values, and every one of the 500
        must still be COUNTED — the clamp bounds SERIES, never the count.
        """
        from services.consumer import (
            _UNMAPPED_LABEL_CAP,
            _UNMAPPED_LABEL_OTHER,
            _clamp_unmapped_label,
        )

        produced = [_clamp_unmapped_label(f"unmapped:r{i}") for i in range(500)]
        assert len(produced) == 500
        assert len(set(produced)) <= _UNMAPPED_LABEL_CAP + 1
        assert _UNMAPPED_LABEL_OTHER in produced

    def test_the_overflow_bucket_still_counts_every_event(self) -> None:
        """Through the real path, not the helper: an overflowed emitter is still
        counted, under `unmapped:other`, and is not silently dropped.
        """
        from observability.metrics import service_key_unmapped_total
        from services.consumer import _UNMAPPED_LABEL_CAP, _UNMAPPED_LABEL_OTHER

        def read_other() -> float:
            return service_key_unmapped_total.labels(service_key=_UNMAPPED_LABEL_OTHER)._value.get()

        for i in range(_UNMAPPED_LABEL_CAP):
            extract_service_key(f"/fill{i}/x")
        before = read_other()
        for i in range(7):
            extract_service_key(f"/spill{i}/x")
        assert read_other() == before + 7

    def test_the_overflow_bucket_can_never_read_as_a_service_name(self) -> None:
        """`unmapped:other` sits inside the `unmapped:` namespace on purpose —
        the same rule that stopped a first path segment being filed as `api`.
        """
        from services.consumer import _UNMAPPED_LABEL_OTHER, _UNMAPPED_PREFIX

        assert _UNMAPPED_LABEL_OTHER.startswith(_UNMAPPED_PREFIX)

    def test_the_birthed_overflow_series_is_the_one_the_clamp_returns(self) -> None:
        """The value is defined TWICE — in services.consumer (which owns it) and
        in observability.metrics (which births the series). It is not imported
        across, because metrics is imported BY consumer and the reverse edge
        would be a cycle. So the agreement is pinned here: if the two ever drift,
        the clamp folds overflow into a series that was never birthed, and the
        birth births a series nothing ever writes to.
        """
        from observability.metrics import service_key_unmapped_total
        from services.consumer import _UNMAPPED_LABEL_OTHER

        exported = {
            sample.labels["service_key"]
            for metric in service_key_unmapped_total.collect()
            for sample in metric.samples
        }
        assert _UNMAPPED_LABEL_OTHER in exported


class TestKagQuerySurfaceIsAttributedToKag:
    """``/query/*`` is kag's own surface and must not file under a defect bucket.

    MEASURED 2026-09-18 on prod (``billing_metering_events`` on worker4). The
    table held 3,660 rows in its whole retention window and **2,352 of them
    (64%) had a ``service_key`` starting ``unmapped``**. 1,201 of those — more
    than the entire mapped-traffic total for every service except fr-backend —
    were filed under ``unmapped:query``, across exactly two paths:

        unmapped:query | /query/chunks | GET | allowed | 692
        unmapped:query | /query/ask    | GET | allowed | 509

    Meanwhile a ``service_key`` of ``kag`` had **ZERO** rows in the same window: the
    map's ``/kag`` entry only ever catches kag's *orchestration* router, which
    is nearly idle, while its two busiest routes live under ``/query`` and fell
    through to the miss branch.

    WHY THIS IS AN ATTRIBUTION FIX AND NOT A PRICING DECISION. It resolves to
    ``kag`` — a key that ALREADY EXISTS in :data:`SERVICE_KEY_MAP` (``"/kag":
    "kag"``). No new billing unit is invented and no rate is chosen here;
    ``billing_app_cost_rates`` has no ``kag`` row either before or after, so
    ``services.billing``'s unpriced-key gate keeps refusing the close exactly as
    it does today. The only thing that changes is WHICH NAME the refusal and the
    stored row carry: a real service instead of a defect placeholder.

    WHY IT IS UNAMBIGUOUS, i.e. why this is not the ``/api`` situation. ``/api``
    was left unmapped because two-plus real backends share that bare prefix.
    ``/query`` does not:

    * ``prefix="/query"`` is mounted by exactly ONE app in the estate — a
      fleet-wide grep over ``~/Movies`` for ``prefix=["']/query`` returns only
      ``knowledge/kag/src/main.py:887-888``.
    * Those two lines mount ``query.router`` and ``ask.router`` into the SAME
      FastAPI app that mounts ``orchestration.router`` at ``prefix="/kag"`` —
      the prefix already mapped to ``kag``. One app, one deployment
      (``kag.kag.svc.cluster.local``), one key.
    * Both observed paths are routes in that app: ``@router.post("/ask")`` and
      ``@router.post("/chunks")`` in ``src/routers/ask.py``.
    * kag says so itself. ``src/routers/query.py``'s billing docstring reads
      "this row is kag's own served, licensed request under
      the ``kag`` service key — the same flat ``per_request`` unit ``/query/ask``
      already emits", and its cost-bus emitter posts under ``KAG_SERVICE``.

    The ordering assertion below is the load-bearing one: ``extract_service_key``
    walks :data:`SERVICE_KEY_MAP` in insertion order and returns on the first
    ``startswith``, so a new entry can only be proven safe by showing every
    previously-mapped path still resolves to what it did before.
    """

    @pytest.mark.parametrize(
        "path,expected",
        [
            # The two paths measured live, verbatim.
            ("/query/ask", "kag"),
            ("/query/chunks", "kag"),
            # The rest of kag's /query surface, from src/routers/query.py.
            ("/query/semantic", "kag"),
            ("/query/hierarchical", "kag"),
            ("/query/similar-nodes", "kag"),
            ("/query/graph", "kag"),
        ],
    )
    def test_the_kag_query_surface_resolves_to_kag(self, path: str, expected: str) -> None:
        assert extract_service_key(path) == expected

    def test_query_no_longer_lands_in_the_unmapped_bucket(self) -> None:
        """The negative half: the defect bucket must lose this traffic."""
        key = extract_service_key("/query/ask")
        assert not key.startswith(UNMAPPED_SERVICE_KEY)
        assert key != "unmapped:query"

    def test_a_declared_payload_key_cannot_override_the_now_mapped_prefix(self) -> None:
        """Same invariant the /kag entry already has: prefix beats payload."""
        fields = {"service" + "_key": "kag-graph"}  # pragma: allowlist secret
        assert extract_service_key("/query/ask", fields) == "kag"

    @pytest.mark.parametrize(
        "path,expected",
        [
            # A new entry must not shadow, or be shadowed by, any sibling.
            ("/kag/orchestrate", "kag"),
            ("/kag-graph/ingest", "kag-graph"),
            ("/kag-orchestrator/run", "kag-orchestrator"),
            ("/graph-adapter/nodes", "graph-adapter"),
            ("/hirag/search", "hirag"),
            ("/nlp/parse", "nlp-service"),
            ("/workers/tasks", "workers"),
            ("/autonomy/recon.discover_sources", "autonomy"),
            # ⛔ /api stays deliberately unmapped — two-plus backends share it.
            ("/api/provider-health-signals", "unmapped:api"),
            # A path that merely CONTAINS "query" but does not start with it is
            # untouched: the match is startswith, not substring.
            ("/totally-new-service/query", "unmapped:totally-new-service"),
        ],
    )
    def test_no_previously_mapped_path_changes_key(self, path: str, expected: str) -> None:
        assert extract_service_key(path) == expected
