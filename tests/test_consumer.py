"""Tests for the Redis stream consumer."""

import asyncio
import os
import sys
import uuid

import pytest
from redis.exceptions import ConnectionError as RedisConnectionError

from services.consumer import (
    SERVICE_KEY_MAP,
    consume_events,
    ensure_consumer_groups,
    extract_service_key,
    resolve_consumer_name,
)
from settings import Settings
from tests.conftest import T

# NEVER put 5436 back here: that port is ``ir-r21-pg-5436``, ANOTHER session's
# Postgres container. With TEST_DATABASE_URL unset these tests connected to it
# and wrote into a peer's database. This repo's own test container is
# 127.0.0.1:5502/metering_test.
_DEFAULT_TEST_DATABASE_URL = "postgresql+psycopg://postgres:postgres@127.0.0.1:5502/metering_test"
TEST_DATABASE_URL = os.environ.get("TEST_DATABASE_URL", "")
if not TEST_DATABASE_URL:
    TEST_DATABASE_URL = _DEFAULT_TEST_DATABASE_URL
    print(
        "TEST_DATABASE_URL is UNSET — falling back to this repo's own test "
        f"container {_DEFAULT_TEST_DATABASE_URL}. If that container is not up, "
        "these tests will fail to connect; do NOT point them at another "
        "session's Postgres.",
        file=sys.stderr,
    )

# ===========================================================================
# extract_service_key
# ===========================================================================


class TestExtractServiceKey:
    def test_known_paths(self):
        """Every entry in SERVICE_KEY_MAP should resolve correctly."""
        # Core API
        assert extract_service_key("/workers/api/tasks") == "workers"
        assert extract_service_key("/kanban/api/boards") == "kanban"
        assert extract_service_key("/kag/api/graphs") == "kag"
        assert extract_service_key("/skills/api/builder") == "skill-builder"
        assert extract_service_key("/mcp/api/tools") == "mcp"
        assert extract_service_key("/coding/api/analyze") == "coding"
        assert extract_service_key("/signal/api/events") == "signal"
        assert extract_service_key("/licenses/api/list") == "licenses"
        assert extract_service_key("/artifacts/api/upload") == "artifacts"
        assert extract_service_key("/procedures/api/create") == "procedures"
        assert extract_service_key("/requirements/api/list") == "requirements"
        assert extract_service_key("/secrets/api/get") == "secrets"
        assert extract_service_key("/tools/api/list") == "tools"
        # Flywheel / ML Pipeline
        assert extract_service_key("/inference-router/v1/completions") == "inference-router"
        assert extract_service_key("/model-registry/api/models") == "model-registry"
        assert extract_service_key("/data-collector/api/samples") == "data-collector"
        assert extract_service_key("/model-evaluator/api/evaluate") == "model-evaluator"
        assert (
            extract_service_key("/training-data-generator/api/generate")
            == "training-data-generator"
        )
        assert extract_service_key("/improvement-loop/api/usage") == "improvement-loop"
        assert extract_service_key("/rtx/v1/chat") == "vllm"
        assert extract_service_key("/vllm/v1/chat") == "vllm"
        # laya Jev bridge (gitops route-registry: /laya/ -> metering_service_key laya)
        assert extract_service_key("/laya/v1/systemone") == "laya"
        # KAG Ecosystem
        assert extract_service_key("/kag-graph/api/query") == "kag-graph"
        assert extract_service_key("/kag-orchestrator/api/run") == "kag-orchestrator"
        assert extract_service_key("/graph-adapter/api/adapt") == "graph-adapter"
        assert extract_service_key("/karma/api/score") == "karma"
        assert extract_service_key("/hirag/api/retrieve") == "hirag"
        assert extract_service_key("/nlp/api/parse") == "nlp-service"
        # Storage & Ingestion
        assert extract_service_key("/storage/api/files") == "storage"
        assert extract_service_key("/ingestion/api/ingest") == "ingestion"
        # Infrastructure
        assert extract_service_key("/npm/@scope/package") == "npm"
        # Autonomy Control Service (control_service repo) — writes directly to
        # the metering:events stream via its own metering_stream.py, building
        # the path as f"/autonomy/{capability}". See FIX 1 write-up.
        assert extract_service_key("/autonomy/demandgen.track_funnel") == "autonomy"

    def test_api_prefix_is_deliberately_unmapped(self):
        """ "/api" must NOT resolve to a single service — it is genuinely shared.

        Live evidence (2026-09-16): of unmapped:api traffic in the last 24h,
        96% is control_service's own /api/* routes (source_ip=
        10.42.6.80, control-service-waypoint), not fr-backend (whose own
        /api/* traffic, source_ip="fr-backend", emitted nothing in the same
        window). Mapping "/api" to either service would misattribute the
        other's spend, so it stays in the unmapped: bucket on purpose — an
        honest, still-alerting label beats a wrong one.
        """
        assert extract_service_key("/api/deliverability-signals") == "unmapped:api"
        assert extract_service_key("/api/webhooks/n8n/router-dedupe") == "unmapped:api"
        assert "/api" not in SERVICE_KEY_MAP

    def test_all_service_key_map_entries_covered(self):
        """Exhaustively verify every SERVICE_KEY_MAP prefix returns the expected key."""
        for prefix, expected_key in SERVICE_KEY_MAP.items():
            result = extract_service_key(prefix)
            assert result == expected_key, (
                f"prefix={prefix!r} => {result!r}, expected {expected_key!r}"
            )

    def test_prefix_only_without_trailing_path(self):
        """Bare prefix without additional path segments should still match."""
        assert extract_service_key("/workers") == "workers"
        assert extract_service_key("/kanban") == "kanban"
        assert extract_service_key("/skills") == "skill-builder"

    # ⛔ THE FOUR TESTS BELOW USED TO PIN THE FABRICATION AS THE SPEC.
    # They asserted that an unmapped path returns its FIRST PATH SEGMENT
    # verbatim — which is precisely the behaviour that invented a service
    # called ``api`` and filed ~2,733 fr events/day under it. A test that
    # encodes the bug outvotes every attempt to fix it, so they are rewritten
    # rather than deleted: the miss branch is still exercised, it just has to
    # produce a value nobody can mistake for a real service key.
    # Full rationale + the stream measurements: tests/test_service_key_is_not_fabricated.py

    def test_unknown_path(self):
        assert extract_service_key("/unknown/api/foo") == "unmapped:unknown"

    def test_root_path(self):
        assert extract_service_key("/") == "unmapped"

    def test_empty_path(self):
        assert extract_service_key("") == "unmapped"

    def test_nested_unknown_path(self):
        """An unmapped path is labelled unmapped, and keeps naming its segment."""
        assert extract_service_key("/custom-service/api/v2/items") == "unmapped:custom-service"

    def test_path_with_query_string(self):
        """Query strings are part of the path string — prefix match still works."""
        assert extract_service_key("/workers/api/tasks?limit=10") == "workers"

    def test_deeply_nested_known_path(self):
        """Deep paths under a known prefix should still resolve."""
        assert extract_service_key("/coding/api/analyze/project/123/file") == "coding"

    def test_similar_but_not_matching_prefix(self):
        """A path that starts with a known word but different prefix should fall through."""
        # "worker" (no 's') is not in the map. It used to be returned as-is,
        # which put a service_key one character away from a real one into the
        # ledger with nothing marking it as a guess.
        assert extract_service_key("/worker/api/tasks") == "unmapped:worker"

    def test_case_sensitivity(self):
        """SERVICE_KEY_MAP keys are lowercase; uppercase must NOT match.

        It lands in the unmapped bucket rather than being echoed back as
        ``Workers`` — a second, casing-only spelling of a real key would split
        that service's series in every dashboard.
        """
        assert extract_service_key("/Workers/api/tasks") == "unmapped:workers"

    def test_no_leading_slash(self):
        """A path with no leading slash matches no prefix, so it is unmapped."""
        assert extract_service_key("workers/api/tasks") == "unmapped:workers"


# ===========================================================================
# Helpers
# ===========================================================================


def _test_settings(**overrides: object) -> Settings:
    defaults: dict[str, object] = {
        "database_url": TEST_DATABASE_URL,
        "redis_url": "redis://localhost:6379/15",
        "log_format": "console",
        "environment": "test",
        "licenses_url": "http://localhost:9999",
        "events_stream": "billing:events",
        "costs_stream": "billing:costs",
        "consumer_group": "billing-service",
        "consumer_name": "test-worker",
    }
    defaults.update(overrides)
    return Settings.model_validate(defaults)


# ===========================================================================
# ensure_consumer_groups
# ===========================================================================


class TestEnsureConsumerGroups:
    async def test_creates_groups(self, fake_redis):
        """Groups should be created on both streams."""
        settings = _test_settings()

        await ensure_consumer_groups(fake_redis, settings)

        # Verify groups exist by calling XINFO GROUPS
        event_groups = await fake_redis.xinfo_groups(settings.events_stream)
        cost_groups = await fake_redis.xinfo_groups(settings.costs_stream)

        assert len(event_groups) == 1
        assert event_groups[0]["name"] == settings.consumer_group
        assert len(cost_groups) == 1
        assert cost_groups[0]["name"] == settings.consumer_group

    async def test_handles_existing_groups(self, fake_redis):
        """Calling ensure_consumer_groups twice should not raise (BUSYGROUP swallowed)."""
        settings = _test_settings()

        await ensure_consumer_groups(fake_redis, settings)
        # Second call should not raise
        await ensure_consumer_groups(fake_redis, settings)

        # Still exactly one group per stream
        event_groups = await fake_redis.xinfo_groups(settings.events_stream)
        assert len(event_groups) == 1

    async def test_creates_streams_if_missing(self, fake_redis):
        """mkstream=True should create the streams even if they don't exist yet."""
        settings = _test_settings(
            events_stream="new:events",
            costs_stream="new:costs",
        )

        await ensure_consumer_groups(fake_redis, settings)

        # Streams should exist now (xinfo_stream doesn't raise)
        info = await fake_redis.xinfo_stream(settings.events_stream)
        assert info is not None

    async def test_propagates_non_busygroup_error(self, fake_redis):
        """Non-BUSYGROUP ResponseError should propagate."""
        import redis.asyncio as aioredis

        settings = _test_settings()

        async def _raise_other(*args, **kwargs):
            raise aioredis.ResponseError("WRONGTYPE some other error")

        fake_redis.xgroup_create = _raise_other

        with pytest.raises(aioredis.ResponseError, match="WRONGTYPE"):
            await ensure_consumer_groups(fake_redis, settings)

    async def test_custom_stream_names(self, fake_redis):
        """Consumer groups should be created on custom-named streams."""
        settings = _test_settings(
            events_stream="custom:events:v2",
            costs_stream="custom:costs:v2",
            consumer_group="custom-group",
        )

        await ensure_consumer_groups(fake_redis, settings)

        event_groups = await fake_redis.xinfo_groups("custom:events:v2")
        cost_groups = await fake_redis.xinfo_groups("custom:costs:v2")
        assert event_groups[0]["name"] == "custom-group"
        assert cost_groups[0]["name"] == "custom-group"


# ===========================================================================
# consume_events
# ===========================================================================


class TestConsumeEvents:
    async def test_consumes_events_from_stream(self, fake_redis):
        """Messages added to the events stream should land in event_buffer."""
        settings = _test_settings()
        event_buffer: list[dict] = []
        cost_buffer: list[dict] = []
        stop_event = asyncio.Event()

        lic_id = str(uuid.uuid4())
        app_id = str(uuid.uuid4())
        plan_id = str(uuid.uuid4())

        # Pre-populate the events stream BEFORE consumer starts
        await fake_redis.xadd(
            settings.events_stream,
            {
                "tenant_id": T("t1"),
                "license_id": lic_id,
                "app_id": app_id,
                "plan_id": plan_id,
                "method": "GET",
                "path": "/workers/api/tasks",
                "status": "allowed",
            },
        )
        await fake_redis.xadd(
            settings.events_stream,
            {
                "tenant_id": T("t1"),
                "license_id": lic_id,
                "app_id": app_id,
                "plan_id": plan_id,
                "method": "POST",
                "path": "/kanban/api/boards",
                "status": "allowed",
            },
        )

        async def _stop_soon():
            await asyncio.sleep(0.3)
            stop_event.set()

        await asyncio.gather(
            consume_events(fake_redis, settings, event_buffer, cost_buffer, stop_event),
            _stop_soon(),
        )

        assert len(event_buffer) == 2
        assert len(cost_buffer) == 0

        # Verify service_key was extracted and injected
        paths = {e["path"] for e in event_buffer}
        assert paths == {"/workers/api/tasks", "/kanban/api/boards"}
        for e in event_buffer:
            assert "_service_key" in e

    async def test_service_key_injected_correctly(self, fake_redis):
        """The _service_key field should match the expected service key for each path."""
        settings = _test_settings()
        event_buffer: list[dict] = []
        cost_buffer: list[dict] = []
        stop_event = asyncio.Event()

        await fake_redis.xadd(
            settings.events_stream,
            {
                "tenant_id": T("t1"),
                "license_id": str(uuid.uuid4()),
                "app_id": str(uuid.uuid4()),
                "plan_id": str(uuid.uuid4()),
                "method": "GET",
                "path": "/skills/api/build",
                "status": "allowed",
            },
        )

        async def _stop_soon():
            await asyncio.sleep(0.3)
            stop_event.set()

        await asyncio.gather(
            consume_events(fake_redis, settings, event_buffer, cost_buffer, stop_event),
            _stop_soon(),
        )

        assert len(event_buffer) == 1
        assert event_buffer[0]["_service_key"] == "skill-builder"

    async def test_event_with_missing_path_gets_unmapped_key(self, fake_redis):
        """An event with no 'path' is filed as unmapped, not as a made-up service."""
        settings = _test_settings()
        event_buffer: list[dict] = []
        cost_buffer: list[dict] = []
        stop_event = asyncio.Event()

        await fake_redis.xadd(
            settings.events_stream,
            {
                "tenant_id": T("t1"),
                "license_id": str(uuid.uuid4()),
                "app_id": str(uuid.uuid4()),
                "plan_id": str(uuid.uuid4()),
                "method": "GET",
                "status": "allowed",
                # No "path" field
            },
        )

        async def _stop_soon():
            await asyncio.sleep(0.3)
            stop_event.set()

        await asyncio.gather(
            consume_events(fake_redis, settings, event_buffer, cost_buffer, stop_event),
            _stop_soon(),
        )

        assert len(event_buffer) == 1
        # detect-secrets' "Secret Keyword" rule fires on any `*_key == "<literal>"`.
        # This is a metering service_key, not a credential; pragma'd at the site
        # rather than absorbed into .secrets.baseline, per the note in
        # observability/metrics.py — a baseline entry is an opaque hash that says
        # nothing about why it is safe.
        assert event_buffer[0]["_service_key"] == "unmapped"  # pragma: allowlist secret

    async def test_consumes_costs_from_stream(self, fake_redis):
        """Messages on the costs stream should land in cost_buffer."""
        settings = _test_settings()
        event_buffer: list[dict] = []
        cost_buffer: list[dict] = []
        stop_event = asyncio.Event()

        await fake_redis.xadd(
            settings.costs_stream,
            {
                "tenant_id": T("t1"),
                "license_id": str(uuid.uuid4()),
                "service_key": "inference-router",
                "provider": "claude",
                "model": "claude-sonnet-4-20250514",
                "input_tokens": "1000",
                "output_tokens": "200",
                "cost_usd": "0.0165",
            },
        )

        async def _stop_soon():
            await asyncio.sleep(0.3)
            stop_event.set()

        await asyncio.gather(
            consume_events(fake_redis, settings, event_buffer, cost_buffer, stop_event),
            _stop_soon(),
        )

        assert len(cost_buffer) == 1
        assert len(event_buffer) == 0
        assert cost_buffer[0]["provider"] == "claude"

    async def test_cost_buffer_preserves_all_fields(self, fake_redis):
        """All fields from a cost message should be preserved in cost_buffer."""
        settings = _test_settings()
        event_buffer: list[dict] = []
        cost_buffer: list[dict] = []
        stop_event = asyncio.Event()

        lic_id = str(uuid.uuid4())
        await fake_redis.xadd(
            settings.costs_stream,
            {
                "tenant_id": T("t1"),
                "license_id": lic_id,
                "service_key": "inference-router",
                "provider": "openai",
                "model": "gpt-4o",
                "input_tokens": "500",
                "output_tokens": "100",
                "cost_usd": "0.005",
            },
        )

        async def _stop_soon():
            await asyncio.sleep(0.3)
            stop_event.set()

        await asyncio.gather(
            consume_events(fake_redis, settings, event_buffer, cost_buffer, stop_event),
            _stop_soon(),
        )

        assert len(cost_buffer) == 1
        msg = cost_buffer[0]
        assert msg["tenant_id"] == T("t1")
        assert msg["license_id"] == lic_id
        assert msg["service_key"] == "inference-router"
        assert msg["provider"] == "openai"
        assert msg["model"] == "gpt-4o"
        assert msg["input_tokens"] == "500"
        assert msg["output_tokens"] == "100"
        assert msg["cost_usd"] == "0.005"
        # Cost messages should NOT get _service_key injected
        assert "_service_key" not in msg

    async def test_stops_on_stop_event(self, fake_redis):
        """Consumer loop should exit when stop_event is set."""
        settings = _test_settings()
        event_buffer: list[dict] = []
        cost_buffer: list[dict] = []
        stop_event = asyncio.Event()

        # Set immediately — consumer should exit after at most one iteration
        stop_event.set()

        # Should complete quickly without hanging
        await asyncio.wait_for(
            consume_events(fake_redis, settings, event_buffer, cost_buffer, stop_event),
            timeout=5.0,
        )

        # No messages were added, buffers should be empty
        assert len(event_buffer) == 0
        assert len(cost_buffer) == 0

    async def test_messages_remain_pending_until_flush(self, fake_redis):
        """Consumer alone must NOT XACK — the flusher acks after the DB commit.

        Acking inside the consumer would lose events on a replica crash
        between in-memory buffer and the next flush. The invariant is:
        messages stay in the pending-entries-list, tagged with ``_msg_id``
        + ``_stream_name``, until ``flush_events`` succeeds.
        """
        settings = _test_settings()
        event_buffer: list[dict] = []
        cost_buffer: list[dict] = []
        stop_event = asyncio.Event()

        await fake_redis.xadd(
            settings.events_stream,
            {
                "tenant_id": T("t1"),
                "license_id": str(uuid.uuid4()),
                "app_id": str(uuid.uuid4()),
                "plan_id": str(uuid.uuid4()),
                "method": "GET",
                "path": "/workers/x",
                "status": "allowed",
            },
        )

        async def _stop_soon():
            await asyncio.sleep(0.3)
            stop_event.set()

        await asyncio.gather(
            consume_events(fake_redis, settings, event_buffer, cost_buffer, stop_event),
            _stop_soon(),
        )

        assert len(event_buffer) == 1
        # ACK coordinates are stamped so the flusher can xack post-commit.
        assert event_buffer[0]["_msg_id"] is not None
        assert event_buffer[0]["_stream_name"] == settings.events_stream

        # Message must still be pending — consumer no longer acks.
        pending = await fake_redis.xpending(settings.events_stream, settings.consumer_group)
        assert pending["pending"] == 1

    async def test_multiple_events_remain_pending_until_flush(self, fake_redis):
        """All buffered messages stay in the PEL until the flusher acks them."""
        settings = _test_settings()
        event_buffer: list[dict] = []
        cost_buffer: list[dict] = []
        stop_event = asyncio.Event()

        for i in range(5):
            await fake_redis.xadd(
                settings.events_stream,
                {
                    "tenant_id": T(f"t{i}"),
                    "license_id": str(uuid.uuid4()),
                    "app_id": str(uuid.uuid4()),
                    "plan_id": str(uuid.uuid4()),
                    "method": "GET",
                    "path": f"/workers/api/task{i}",
                    "status": "allowed",
                },
            )

        async def _stop_soon():
            await asyncio.sleep(0.3)
            stop_event.set()

        await asyncio.gather(
            consume_events(fake_redis, settings, event_buffer, cost_buffer, stop_event),
            _stop_soon(),
        )

        assert len(event_buffer) == 5
        pending = await fake_redis.xpending(settings.events_stream, settings.consumer_group)
        assert pending["pending"] == 5

    async def test_consumer_recovers_from_transient_error(self, fake_redis):
        """Consumer should log error and continue after non-fatal exception."""
        settings = _test_settings()
        event_buffer: list[dict] = []
        cost_buffer: list[dict] = []
        stop_event = asyncio.Event()

        call_count = 0
        original_xreadgroup = fake_redis.xreadgroup

        async def _flaky_xreadgroup(*args, **kwargs):
            nonlocal call_count
            call_count += 1
            if call_count == 1:
                raise RedisConnectionError("Transient Redis failure")
            return await original_xreadgroup(*args, **kwargs)

        # Pre-create groups
        await ensure_consumer_groups(fake_redis, settings)

        # Add a message after groups exist
        await fake_redis.xadd(
            settings.events_stream,
            {
                "tenant_id": T("t1"),
                "license_id": str(uuid.uuid4()),
                "app_id": str(uuid.uuid4()),
                "plan_id": str(uuid.uuid4()),
                "method": "GET",
                "path": "/workers/x",
                "status": "allowed",
            },
        )

        fake_redis.xreadgroup = _flaky_xreadgroup

        async def _stop_after_consumed():
            # Wait until consumer has recovered and processed the message
            for _ in range(50):
                if event_buffer:
                    break
                await asyncio.sleep(0.1)
            stop_event.set()

        await asyncio.gather(
            consume_events(fake_redis, settings, event_buffer, cost_buffer, stop_event),
            _stop_after_consumed(),
        )

        # After recovery, the message should have been consumed
        assert len(event_buffer) >= 1

    async def test_consume_events_cancelled_error(self, fake_redis):
        """CancelledError during xreadgroup should break the loop cleanly."""
        settings = _test_settings()
        event_buffer: list[dict] = []
        cost_buffer: list[dict] = []
        stop_event = asyncio.Event()

        # Pre-create groups so ensure_consumer_groups doesn't fail
        await ensure_consumer_groups(fake_redis, settings)

        call_count = 0
        original_xreadgroup = fake_redis.xreadgroup

        async def _cancelling_xreadgroup(*args, **kwargs):
            nonlocal call_count
            call_count += 1
            if call_count == 1:
                raise asyncio.CancelledError()
            return await original_xreadgroup(*args, **kwargs)

        fake_redis.xreadgroup = _cancelling_xreadgroup

        await asyncio.wait_for(
            consume_events(fake_redis, settings, event_buffer, cost_buffer, stop_event),
            timeout=5.0,
        )

        # Should have exited cleanly
        assert call_count >= 1
        assert len(event_buffer) == 0
        assert len(cost_buffer) == 0

    async def test_empty_stream_returns_empty_buffers(self, fake_redis):
        """Consumer with no messages should produce empty buffers."""
        settings = _test_settings()
        event_buffer: list[dict] = []
        cost_buffer: list[dict] = []
        stop_event = asyncio.Event()

        async def _stop_soon():
            await asyncio.sleep(0.3)
            stop_event.set()

        await asyncio.gather(
            consume_events(fake_redis, settings, event_buffer, cost_buffer, stop_event),
            _stop_soon(),
        )

        assert event_buffer == []
        assert cost_buffer == []

    async def test_multiple_consumers_same_group(self, fake_redis):
        """Two consumer instances in the same group should not duplicate messages."""
        settings1 = _test_settings(consumer_name="worker-1")
        settings2 = _test_settings(consumer_name="worker-2")

        # Create groups and add messages
        await ensure_consumer_groups(fake_redis, settings1)

        for i in range(4):
            await fake_redis.xadd(
                settings1.events_stream,
                {
                    "tenant_id": T(f"t{i}"),
                    "license_id": str(uuid.uuid4()),
                    "app_id": str(uuid.uuid4()),
                    "plan_id": str(uuid.uuid4()),
                    "method": "GET",
                    "path": f"/workers/api/task{i}",
                    "status": "allowed",
                },
            )

        buf1: list[dict] = []
        buf2: list[dict] = []
        cost1: list[dict] = []
        cost2: list[dict] = []
        stop1 = asyncio.Event()
        stop2 = asyncio.Event()

        async def _stop_soon(ev):
            await asyncio.sleep(0.5)
            ev.set()

        await asyncio.gather(
            consume_events(fake_redis, settings1, buf1, cost1, stop1),
            consume_events(fake_redis, settings2, buf2, cost2, stop2),
            _stop_soon(stop1),
            _stop_soon(stop2),
        )

        # Total messages consumed across both consumers should be 4 (no duplicates)
        total = len(buf1) + len(buf2)
        assert total == 4

    async def test_consumer_handles_many_messages(self, fake_redis):
        """Consumer should handle a burst of many messages without dropping any."""
        settings = _test_settings()
        event_buffer: list[dict] = []
        cost_buffer: list[dict] = []
        stop_event = asyncio.Event()

        msg_count = 50
        for i in range(msg_count):
            await fake_redis.xadd(
                settings.events_stream,
                {
                    "tenant_id": T("burst-tenant"),
                    "license_id": str(uuid.uuid4()),
                    "app_id": str(uuid.uuid4()),
                    "plan_id": str(uuid.uuid4()),
                    "method": "POST",
                    "path": f"/workers/api/task{i}",
                    "status": "allowed",
                },
            )

        async def _stop_soon():
            await asyncio.sleep(0.5)
            stop_event.set()

        await asyncio.gather(
            consume_events(fake_redis, settings, event_buffer, cost_buffer, stop_event),
            _stop_soon(),
        )

        assert len(event_buffer) == msg_count
        assert all(e["_service_key"] == "workers" for e in event_buffer)

    async def test_nogroup_error_recreates_consumer_groups(self, fake_redis):
        """NOGROUP error triggers consumer group re-creation instead of looping forever."""
        import redis.asyncio as aioredis

        settings = _test_settings()
        event_buffer: list[dict] = []
        cost_buffer: list[dict] = []
        stop_event = asyncio.Event()

        await ensure_consumer_groups(fake_redis, settings)

        # Delete the consumer groups to simulate Redis restart
        await fake_redis.xgroup_destroy(settings.events_stream, settings.consumer_group)
        await fake_redis.xgroup_destroy(settings.costs_stream, settings.consumer_group)

        # Verify they're gone — xreadgroup should fail
        with pytest.raises(aioredis.ResponseError, match="NOGROUP"):
            await fake_redis.xreadgroup(
                groupname=settings.consumer_group,
                consumername=settings.consumer_name,
                streams={settings.events_stream: ">"},
                count=1,
                block=0,
            )

        # Now run the consumer — it should recover by re-creating groups
        async def _stop_after_recovery():
            # Give consumer time to hit the NOGROUP error and recover
            await asyncio.sleep(2.0)
            stop_event.set()

        await asyncio.wait_for(
            asyncio.gather(
                consume_events(fake_redis, settings, event_buffer, cost_buffer, stop_event),
                _stop_after_recovery(),
            ),
            timeout=10.0,
        )

        # Verify groups were re-created
        event_groups = await fake_redis.xinfo_groups(settings.events_stream)
        assert any(g["name"] == settings.consumer_group for g in event_groups)


# ===========================================================================
# FIX A2 — XAUTOCLAIM PEL reclaim of stranded (crashed-consumer) entries
# ===========================================================================


class TestReclaimPending:
    async def test_idle_pel_entry_reclaimed_and_persisted_once(
        self, fake_redis, db_session_factory
    ):
        """An entry stranded in the PEL by a crashed consumer is reclaimed by
        XAUTOCLAIM, persisted exactly once, and a re-claim is a no-op via the inbox."""
        from sqlalchemy import func, select

        from models.db import CostEvent, ProcessedCostMessage
        from services.cost_flusher import flush_costs

        # min-idle 0 so the just-read entry is immediately reclaimable in-test.
        settings = _test_settings(consumer_pel_reclaim_min_idle_seconds=0)
        cg = settings.consumer_group

        await fake_redis.xadd(
            settings.costs_stream,
            {
                "tenant_id": T("t1"),
                "license_id": str(uuid.uuid4()),
                "service_key": "inference-router",
                "provider": "claude",
                "model": "claude-sonnet-4-20250514",
                "input_tokens": "1000",
                "output_tokens": "200",
                "cost_usd": "0.0165",
            },
        )
        await ensure_consumer_groups(fake_redis, settings)

        # Simulate a crash: the consumer XREADGROUP'd the message (now in the PEL)
        # but died before flushing/acking — ">" will never return it again.
        await fake_redis.xreadgroup(
            groupname=cg,
            consumername=settings.consumer_name,
            streams={settings.events_stream: ">", settings.costs_stream: ">"},
            count=100,
            block=0,
        )
        pending = await fake_redis.xpending(settings.costs_stream, cg)
        assert pending["pending"] == 1

        # Reclaim #1 → stage the stranded entry; flush WITHOUT ack (entry stays in PEL).
        from services.consumer import reclaim_pending

        ev1: list[dict] = []
        co1: list[dict] = []
        staged = await reclaim_pending(fake_redis, settings, ev1, co1)
        assert staged == 1
        assert len(co1) == 1 and co1[0]["_msg_id"] is not None
        assert await flush_costs(co1, db_session_factory, fake_redis, None) == 1

        # Reclaim #2 (entry still pending) → re-stages the SAME id; flush dedups it.
        ev2: list[dict] = []
        co2: list[dict] = []
        staged2 = await reclaim_pending(fake_redis, settings, ev2, co2)
        assert staged2 == 1  # re-claimed from the PEL again
        assert await flush_costs(co2, db_session_factory, fake_redis, None) == 0  # inbox dedups

        async with db_session_factory() as session:
            n = (await session.execute(select(func.count()).select_from(CostEvent))).scalar()
            claims = (
                await session.execute(select(func.count()).select_from(ProcessedCostMessage))
            ).scalar()
        assert n == 1  # persisted exactly once despite two reclaims
        assert claims == 1

    async def test_reclaim_noop_when_pel_empty(self, fake_redis):
        """With nothing stranded, reclaim stages nothing."""
        settings = _test_settings(consumer_pel_reclaim_min_idle_seconds=0)
        await ensure_consumer_groups(fake_redis, settings)
        from services.consumer import reclaim_pending

        ev: list[dict] = []
        co: list[dict] = []
        assert await reclaim_pending(fake_redis, settings, ev, co) == 0
        assert ev == [] and co == []


# ===========================================================================
# The PEL-eviction (silent-data-loss) branch of the reclaim
#
# The producer bounds both ingest streams with ``XADD MAXLEN ~ 100_000`` and
# Redis trims by COUNT regardless of PEL membership, so a long enough
# persistence stall destroys entries this consumer already claimed but has not
# yet acked. XAUTOCLAIM drops such ids from the PEL and reports them — nothing
# is left to persist, the event is gone. The ONLY observable for that loss is
# the ``pel_entry_evicted_before_reclaim`` warning plus the ``evicted=`` count
# on the reclaim summary, so those two are what these tests pin.
#
# Redis reports the loss in TWO DIFFERENT SHAPES, and the tests below cover
# both explicitly. Both framings were captured from REAL Redis with the
# production client (redis-py 8.0.0), not invented:
#
#   Redis 7.4.11 (== production ``redis:7.4-alpine``) — THREE elements, the
#   dropped ids in ``reply[2]``, ``reply[1]`` holding only live entries:
#       reply[0] = '0-0'
#       reply[1] = [('1787667180244-1', {'k': '1'})]
#       reply[2] = ['1787667180244-0', '1787667180244-2']
#
#   Redis 6.2.24 (legacy) — TWO elements, the dropped entries inline in
#   ``reply[1]``; redis-py's ``parse_stream_list`` maps a nil entry to
#   ``(None, None)``, so the id is NOT recoverable on this path:
#       reply[0] = '0-0'
#       reply[1] = [(None, None), ('1787667180761-0', {'k': '1'}), (None, None)]
#
# fakeredis 2.36.1 emits the 7.x three-element shape byte-for-byte (verified
# against the capture above), so the production-path tests drive fakeredis
# UNTRANSLATED. Only the legacy path needs a reply-framing translation, and it
# reproduces the real 6.2 ``(None, None)`` framing — NOT the ``(msg_id, None)``
# shape that no redis-py build has ever emitted.
# ===========================================================================


def _record_xautoclaim_replies(redis_conn) -> list:
    """Record every XAUTOCLAIM reply handed to the code under test, untranslated.

    fakeredis already speaks the production (Redis >= 7) three-element shape, so
    the production-path tests need no translation — only a way to assert, on the
    exact replies ``reclaim_pending`` consumed, that the premise held.
    """
    original = redis_conn.xautoclaim
    replies: list = []

    async def _wrapped(*args, **kwargs):
        reply = await original(*args, **kwargs)
        replies.append(reply)
        return reply

    redis_conn.xautoclaim = _wrapped
    return replies


def _reframe_reply_as_legacy_redis_62(redis_conn, batch_size: int | None = None) -> list:
    """Translate fakeredis's Redis-7 reply into the real Redis 6.2 framing.

    The eviction itself stays REAL (real XADD, real XREADGROUP into the PEL,
    real XDEL — what a MAXLEN trim does to an un-acked entry); only the reply
    FRAMING is rewritten: the ``reply[2]`` ids are folded back into ``reply[1]``
    at their id-sorted position as ``(None, None)`` — redis-py's parser drops the
    id — and the third element is dropped, exactly as measured from
    redis-server 6.2.24 above. Returns the list of reframed replies handed out.

    ``batch_size`` splits one translated reply across successive calls with a
    non-terminal cursor in between, reproducing what real Redis does when the PEL
    holds more entries than the ``COUNT`` the reclaim asks for (it asks for 100).
    Pagination is what makes the cross-batch ``seen`` bookkeeping observable.
    """
    original = redis_conn.xautoclaim
    replies: list = []
    queued: list = []

    def _id_sort_key(entry) -> tuple[int, int]:
        ms, _, seq = entry[0].partition("-")
        return (int(ms), int(seq or 0))

    async def _wrapped(*args, **kwargs):
        if queued:  # serve the remainder of an already-translated reply
            legacy = queued.pop(0)
            replies.append(legacy)
            return legacy
        reply = await original(*args, **kwargs)
        if len(reply) < 3:  # already the 2-element shape — nothing to translate
            replies.append(reply)
            return reply
        next_cursor, claimed, deleted = reply[0], list(reply[1]), reply[2]
        merged = sorted([*claimed, *((msg_id, None) for msg_id in deleted)], key=_id_sort_key)
        entries = [(mid, f) if f is not None else (None, None) for mid, f in merged]
        if batch_size is None or len(entries) <= batch_size:
            legacy = (next_cursor, entries)
        else:
            # Non-terminal cursor on the head batch: more PEL to walk.
            legacy = (merged[batch_size - 1][0], entries[:batch_size])
            queued.append((next_cursor, entries[batch_size:]))
        replies.append(legacy)
        return legacy

    redis_conn.xautoclaim = _wrapped
    return replies


def _cost_payload(model: str) -> dict[str, str]:
    return {
        "tenant_id": T("t1"),
        "license_id": str(uuid.uuid4()),
        "service_key": "inference-router",
        "provider": "claude",
        "model": model,
        "input_tokens": "1000",
        "output_tokens": "200",
        "cost_usd": "0.0165",
    }


class TestReclaimPendingEviction:
    @staticmethod
    async def _strand_in_pel(fake_redis, settings) -> None:
        """XREADGROUP everything so it sits un-acked in this consumer's PEL."""
        await ensure_consumer_groups(fake_redis, settings)
        await fake_redis.xreadgroup(
            groupname=settings.consumer_group,
            consumername=settings.consumer_name,
            streams={settings.events_stream: ">", settings.costs_stream: ">"},
            count=100,
            block=0,
        )

    async def test_production_reply_reports_the_deleted_ids_from_the_third_element(
        self, fake_redis
    ):
        """PRODUCTION PATH (Redis 7.4 + redis-py 8, three-element reply).

        The lost ids arrive ONLY in ``reply[2]``; ``reply[1]`` contains no nil
        entry at all, so code that looks solely for inline nil fields sees a
        perfectly empty, perfectly healthy batch and reports evicted=0.

        Framing: fakeredis, UNTRANSLATED — it emits the same three-element shape
        captured from real redis-server 7.4.11 with redis-py 8.0.0.

        One evicted entry per stream, so a log that hardcoded either the stream
        name or the id cannot pass.
        """
        from structlog.testing import capture_logs

        from services.consumer import reclaim_pending

        settings = _test_settings(consumer_pel_reclaim_min_idle_seconds=0)

        event_id = await fake_redis.xadd(
            settings.events_stream,
            {"tenant_id": T("t1"), "path": "/workers/api/tasks", "status_code": "200"},
        )
        cost_id = await fake_redis.xadd(settings.costs_stream, _cost_payload("claude-opus-4"))
        await self._strand_in_pel(fake_redis, settings)

        # The MAXLEN trim: both entries leave the stream while still un-acked.
        assert await fake_redis.xdel(settings.events_stream, event_id) == 1
        assert await fake_redis.xdel(settings.costs_stream, cost_id) == 1

        replies = _record_xautoclaim_replies(fake_redis)

        ev: list[dict] = []
        co: list[dict] = []
        with capture_logs() as logs:
            staged = await reclaim_pending(fake_redis, settings, ev, co)

        # Guard the premise on the replies the code ACTUALLY consumed: the
        # three-element shape, the lost ids in reply[2], and NO nil entry in
        # reply[1] — otherwise this test proves nothing about production.
        assert all(len(r) == 3 for r in replies), "premise broken: not the Redis>=7 reply"
        assert {mid for r in replies for mid in r[2]} == {event_id, cost_id}
        assert all(fields for r in replies for _mid, fields in r[1]), (
            "premise broken: reply[1] must carry NO nil entry on the production shape"
        )

        assert staged == 0
        assert ev == [] and co == []  # nothing to persist — the data is gone

        evictions = [e for e in logs if e.get("event") == "pel_entry_evicted_before_reclaim"]
        assert len(evictions) == 2, (
            f"every evicted entry must be logged individually; got {[e.get('event') for e in logs]}"
        )
        assert all(e["log_level"] == "warning" for e in evictions)
        assert {(e["stream"], e["msg_id"]) for e in evictions} == {
            (settings.events_stream, event_id),
            (settings.costs_stream, cost_id),
        }

    async def test_legacy_reply_counts_every_nil_entry_and_never_logs_none_as_an_id(
        self, fake_redis
    ):
        """LEGACY PATH (Redis 6.2 / redis-py < 5, two-element reply).

        Two separate losses arrive as two ``(None, None)`` entries. They share
        the id ``None``, so a ``mid not in seen`` dedup collapses them to one
        and the count under-reports 2 -> 1. And ``None`` must never be logged as
        though it were the id that was lost.

        Framing: fakeredis + :func:`_reframe_reply_as_legacy_redis_62`,
        reproducing the framing captured from real redis-server 6.2.24.
        """
        from structlog.testing import capture_logs

        from services.consumer import reclaim_pending

        settings = _test_settings(consumer_pel_reclaim_min_idle_seconds=0)

        lost_a = await fake_redis.xadd(settings.costs_stream, _cost_payload("claude-opus-4"))
        lost_b = await fake_redis.xadd(settings.costs_stream, _cost_payload("claude-sonnet-4"))
        await self._strand_in_pel(fake_redis, settings)

        assert await fake_redis.xdel(settings.costs_stream, lost_a) == 1
        assert await fake_redis.xdel(settings.costs_stream, lost_b) == 1
        replies = _reframe_reply_as_legacy_redis_62(fake_redis)

        ev: list[dict] = []
        co: list[dict] = []
        with capture_logs() as logs:
            staged = await reclaim_pending(fake_redis, settings, ev, co)

        # Guard the premise on the replies the code ACTUALLY consumed: the
        # two-element shape, both losses framed as (None, None) in ONE reply.
        assert all(len(r) == 2 for r in replies), "premise broken: not the legacy reply"
        assert [(None, None), (None, None)] in [list(r[1]) for r in replies]

        assert staged == 0
        assert ev == [] and co == []

        evictions = [e for e in logs if e.get("event") == "pel_entry_evicted_before_reclaim"]
        assert len(evictions) == 2, (
            "two nil entries are two distinct losses; the id-based dedup must not collapse them"
        )
        assert all(e["stream"] == settings.costs_stream for e in evictions)
        assert all(e["msg_id"] is not None for e in evictions), (
            "the legacy reply carries no id — logging msg_id=None reads as a real id"
        )

        summaries = [e for e in logs if e.get("event") == "pel_entries_reclaimed"]
        assert len(summaries) == 1
        assert summaries[0]["evicted"] == 2

    @pytest.mark.parametrize("framing", ["production_redis7", "legacy_redis62"])
    async def test_mixed_batch_stages_the_survivor_and_counts_only_the_evicted(
        self, fake_redis, framing
    ):
        """3 claimed, 2 evicted, 1 survivor — on BOTH reply framings.

        ``evicted=`` must be exactly 2: not 0 (blind to the loss) and not 3
        (counting everything), and the survivor must still be staged intact with
        its payload and ack coordinates untouched.
        """
        from structlog.testing import capture_logs

        from services.consumer import reclaim_pending

        settings = _test_settings(consumer_pel_reclaim_min_idle_seconds=0)

        lost_a = await fake_redis.xadd(settings.costs_stream, _cost_payload("claude-opus-4"))
        survivor = await fake_redis.xadd(settings.costs_stream, _cost_payload("claude-haiku-4"))
        lost_b = await fake_redis.xadd(settings.costs_stream, _cost_payload("claude-sonnet-4"))
        await self._strand_in_pel(fake_redis, settings)

        assert await fake_redis.xdel(settings.costs_stream, lost_a) == 1
        assert await fake_redis.xdel(settings.costs_stream, lost_b) == 1
        if framing == "legacy_redis62":
            _reframe_reply_as_legacy_redis_62(fake_redis)

        ev: list[dict] = []
        co: list[dict] = []
        with capture_logs() as logs:
            staged = await reclaim_pending(fake_redis, settings, ev, co)

        assert staged == 1  # only the survivor was staged

        summaries = [e for e in logs if e.get("event") == "pel_entries_reclaimed"]
        assert len(summaries) == 1, "the reclaim summary must be emitted exactly once"
        assert summaries[0]["count"] == 1
        assert summaries[0]["evicted"] == 2  # not 0 (blind) and not 3 (counts everything)

        evictions = [e for e in logs if e.get("event") == "pel_entry_evicted_before_reclaim"]
        assert len(evictions) == 2
        assert survivor not in {e["msg_id"] for e in evictions}
        if framing == "production_redis7":
            # The three-element reply carries the real ids — assert them.
            assert {e["msg_id"] for e in evictions} == {lost_a, lost_b}

        # The survivor is staged intact — the skip drops ONLY the evicted ids.
        assert ev == []
        assert len(co) == 1
        staged_entry = co[0]
        assert staged_entry["_msg_id"] == survivor
        assert staged_entry["_stream_name"] == settings.costs_stream
        assert staged_entry["model"] == "claude-haiku-4"
        assert staged_entry["cost_usd"] == "0.0165"

    @pytest.mark.parametrize("framing", ["production_redis7", "legacy_redis62"])
    async def test_all_evicted_batch_still_emits_the_summary(self, fake_redis, framing):
        """A batch that is ENTIRELY evicted stages nothing, so ``count`` is 0.

        Gating the summary on the staged count made total loss — the worst case —
        the one case that logged no summary at all. It must still be emitted,
        with the eviction count on it.
        """
        from structlog.testing import capture_logs

        from services.consumer import reclaim_pending

        settings = _test_settings(consumer_pel_reclaim_min_idle_seconds=0)

        lost_a = await fake_redis.xadd(settings.costs_stream, _cost_payload("claude-opus-4"))
        lost_b = await fake_redis.xadd(settings.costs_stream, _cost_payload("claude-sonnet-4"))
        await self._strand_in_pel(fake_redis, settings)

        assert await fake_redis.xdel(settings.costs_stream, lost_a) == 1
        assert await fake_redis.xdel(settings.costs_stream, lost_b) == 1
        if framing == "legacy_redis62":
            _reframe_reply_as_legacy_redis_62(fake_redis)

        ev: list[dict] = []
        co: list[dict] = []
        with capture_logs() as logs:
            staged = await reclaim_pending(fake_redis, settings, ev, co)

        assert staged == 0
        assert ev == [] and co == []

        summaries = [e for e in logs if e.get("event") == "pel_entries_reclaimed"]
        assert len(summaries) == 1, (
            "an all-evicted batch must still summarise — it is the loudest case, not the quietest"
        )
        assert summaries[0]["count"] == 0
        assert summaries[0]["evicted"] == 2

    async def test_legacy_nils_across_two_batches_are_not_collapsed_by_the_id_dedup(
        self, fake_redis
    ):
        """LEGACY PATH, paginated — the form the ``seen`` dedup actually breaks.

        Every legacy nil entry carries the SAME id (``None``). Putting it in the
        per-stream ``seen`` set makes the FIRST nil poison the rest: every later
        batch's nils are filtered out as "already seen" and the loss silently
        stops being counted. Within one batch the list comprehension never sees
        ``seen`` mutate, so the collapse only shows once the PEL is walked in
        more than one XAUTOCLAIM call — which is exactly what real Redis does
        whenever the pending list is longer than the COUNT the reclaim asks for.

        Framing: fakeredis + :func:`_reframe_reply_as_legacy_redis_62` in its
        paginating mode; entry framing as measured from real redis-server 6.2.24.
        """
        from structlog.testing import capture_logs

        from services.consumer import reclaim_pending

        settings = _test_settings(consumer_pel_reclaim_min_idle_seconds=0)

        lost = [
            await fake_redis.xadd(settings.costs_stream, _cost_payload(f"claude-model-{i}"))
            for i in range(4)
        ]
        await self._strand_in_pel(fake_redis, settings)
        for msg_id in lost:
            assert await fake_redis.xdel(settings.costs_stream, msg_id) == 1
        replies = _reframe_reply_as_legacy_redis_62(fake_redis, batch_size=2)

        ev: list[dict] = []
        co: list[dict] = []
        with capture_logs() as logs:
            assert await reclaim_pending(fake_redis, settings, ev, co) == 0

        # Premise: the four losses really were served as two legacy batches.
        paginated = [r for r in replies if r[1] == [(None, None), (None, None)]]
        assert len(paginated) == 2, f"premise broken: expected two nil batches, got {replies}"

        evictions = [e for e in logs if e.get("event") == "pel_entry_evicted_before_reclaim"]
        assert len(evictions) == 4, (
            "all four losses must be counted; a `seen` set holding None drops every "
            "nil after the first batch"
        )
        summaries = [e for e in logs if e.get("event") == "pel_entries_reclaimed"]
        assert len(summaries) == 1
        assert summaries[0]["evicted"] == 4


# ===========================================================================
# Decoupled consumer — runs on every replica (NO leader election), per-pod
# consumer name, HA multi-consumer deduped by the idempotency inbox. Replaces
# the removed FIX A3 leader-gate tests: the cost consumer no longer depends on
# the (separately-broken) advisory-lock election.
# ===========================================================================


class TestResolveConsumerName:
    def test_prefers_hostname(self, monkeypatch):
        """In K8s, HOSTNAME (the pod name) gives each replica a distinct name."""
        monkeypatch.setenv("HOSTNAME", "billing-service-7c9d-abc12")
        settings = _test_settings(consumer_name="worker-0")
        assert resolve_consumer_name(settings) == "billing-service-7c9d-abc12"

    def test_falls_back_to_settings_when_hostname_unset(self, monkeypatch):
        """Local / unit runs with no HOSTNAME use the configured consumer_name."""
        monkeypatch.delenv("HOSTNAME", raising=False)
        settings = _test_settings(consumer_name="worker-0")
        assert resolve_consumer_name(settings) == "worker-0"


class TestConsumerNoElection:
    async def test_consumes_with_no_election_wired(self, fake_redis):
        """The default path takes no election: a message is read, staged, and
        left pending for the flusher to persist+XACK (consumer never acks)."""
        settings = _test_settings()
        event_buffer: list[dict] = []
        cost_buffer: list[dict] = []
        stop_event = asyncio.Event()

        await fake_redis.xadd(
            settings.costs_stream,
            {
                "tenant_id": T("t1"),
                "license_id": str(uuid.uuid4()),
                "service_key": "inference-router",
                "provider": "claude",
                "model": "claude-sonnet-4-20250514",
                "input_tokens": "1000",
                "output_tokens": "200",
                "cost_usd": "0.0165",
            },
        )

        async def _stop_soon():
            await asyncio.sleep(0.3)
            stop_event.set()

        await asyncio.gather(
            consume_events(fake_redis, settings, event_buffer, cost_buffer, stop_event),
            _stop_soon(),
        )

        assert len(cost_buffer) == 1
        assert cost_buffer[0]["_msg_id"] is not None
        # Consumer staged but did not ack — still pending for the flusher.
        pending = await fake_redis.xpending(settings.costs_stream, settings.consumer_group)
        assert pending["pending"] == 1

    async def test_two_per_pod_names_split_and_dedup(
        self, fake_redis, db_session_factory, monkeypatch
    ):
        """Two distinct per-pod consumer names both consume + persist; a duplicate
        redelivery of the SAME stream id to the second consumer is inbox-deduped,
        so cost_events holds exactly one row per id (no double-count)."""
        from sqlalchemy import func, select

        from models.db import CostEvent, ProcessedCostMessage
        from services.consumer import reclaim_pending
        from services.cost_flusher import flush_costs

        # min-idle 0 so a just-read entry is immediately reclaimable in-test.
        settings = _test_settings(consumer_pel_reclaim_min_idle_seconds=0)
        cg = settings.consumer_group

        ids = []
        for _ in range(2):
            mid = await fake_redis.xadd(
                settings.costs_stream,
                {
                    "tenant_id": T("t1"),
                    "license_id": str(uuid.uuid4()),
                    "service_key": "inference-router",
                    "provider": "claude",
                    "model": "claude-sonnet-4-20250514",
                    "input_tokens": "1000",
                    "output_tokens": "200",
                    "cost_usd": "0.0165",
                },
            )
            ids.append(mid)
        await ensure_consumer_groups(fake_redis, settings)

        # Replica A (pod-a) reads BOTH ids under its own consumer name and persists.
        monkeypatch.setenv("HOSTNAME", "pod-a")
        name_a = resolve_consumer_name(settings)
        co_a: list[dict] = []
        ev_a: list[dict] = []
        await fake_redis.xreadgroup(
            groupname=cg,
            consumername=name_a,
            streams={settings.events_stream: ">", settings.costs_stream: ">"},
            count=100,
            block=0,
        )
        # Stage what pod-a read via its own PEL reclaim, then flush.
        assert await reclaim_pending(fake_redis, settings, ev_a, co_a, name_a) == 2
        assert await flush_costs(co_a, db_session_factory, fake_redis, None) == 2

        # Replica B (pod-b) is a DISTINCT consumer. It reclaims the same ids
        # (e.g. they were re-delivered / pod-a's ack lost) under its own name —
        # the inbox dedups them so nothing is persisted twice.
        monkeypatch.setenv("HOSTNAME", "pod-b")
        name_b = resolve_consumer_name(settings)
        assert name_b != name_a
        co_b: list[dict] = []
        ev_b: list[dict] = []
        await reclaim_pending(fake_redis, settings, ev_b, co_b, name_b)
        # A redelivery is deduped: zero NEW cost rows persisted by pod-b.
        assert await flush_costs(co_b, db_session_factory, fake_redis, None) == 0

        async with db_session_factory() as session:
            rows = (await session.execute(select(func.count()).select_from(CostEvent))).scalar()
            claims = (
                await session.execute(select(func.count()).select_from(ProcessedCostMessage))
            ).scalar()
        assert rows == 2  # one row per id, despite two consumers seeing them
        assert claims == 2

    async def test_a1_trim_runs_via_consumer_loop(self, fake_redis):
        """FIX A1: the consumer loop age-trims BOTH ingest streams itself (XTRIM
        MINID with an age-bounded id), independent of any leader election."""
        import time as _time

        settings = _test_settings(cost_stream_trim_interval_seconds=0.0)
        event_buffer: list[dict] = []
        cost_buffer: list[dict] = []
        stop_event = asyncio.Event()

        trim_calls: list[tuple] = []
        orig_xtrim = fake_redis.xtrim

        async def _spy_xtrim(name, *args, **kwargs):
            trim_calls.append((name, args, kwargs))
            return await orig_xtrim(name, *args, **kwargs)

        fake_redis.xtrim = _spy_xtrim

        async def _stop_soon():
            await asyncio.sleep(0.3)
            stop_event.set()

        await asyncio.gather(
            consume_events(fake_redis, settings, event_buffer, cost_buffer, stop_event),
            _stop_soon(),
        )

        # Trim was invoked on BOTH streams with an age-bounded MINID that is in
        # the past (now - 36h), i.e. far below the current ms clock.
        assert trim_calls, "consumer loop never trimmed the ingest streams"
        name, _args, kwargs = trim_calls[0]
        assert name == settings.costs_stream
        minid = kwargs.get("minid")
        assert minid is not None
        now_ms = int(_time.time() * 1000)
        assert 0 < int(minid) < now_ms  # an age-bounded (past) cutoff, not 0 / not now

        # The events stream is age-trimmed too (the defect this fix closes): a
        # low-volume event must not outlive its inbox row and double-count on replay.
        trimmed_names = {name for name, _a, _k in trim_calls}
        assert settings.events_stream in trimmed_names


# ===========================================================================
# The PEL counters — `billing_pel_entries_evicted_total` (confirmed loss) and
# `billing_pel_entries_reclaimed_total` (its denominator)
#
# The eviction tests above pin the LOG. A structlog warning cannot be alerted
# on, ranked, or read as a rate, and the code comment at the site says this
# "must be loud" if it ever happens — so the loss must also be a NUMBER. These
# tests pin that number, on both reply shapes, and pin that the healthy reclaim
# path moves the denominator and NOT the loss counter.
#
# `stream` carries the bounded metric vocabulary (`usage` | `cost`), not the
# configured Redis stream name: the raw names come from settings, so they differ
# per environment and cannot be birthed at import inside observability.metrics.
#
# Reads are DELTAS: the default registry is session-global and the eviction
# tests above increment the same series.
# ===========================================================================


def _pel_evicted(stream: str) -> float:
    import observability.metrics as om

    return om.pel_entries_evicted_total.labels(stream=stream)._value.get()


def _pel_reclaimed(stream: str) -> float:
    import observability.metrics as om

    return om.pel_entries_reclaimed_total.labels(stream=stream)._value.get()


def _pel_snapshot() -> dict[tuple[str, str], float]:
    import observability.metrics as om

    return {
        (kind, stream): reader(stream)
        for stream in om.PEL_STREAMS
        for kind, reader in (("evicted", _pel_evicted), ("reclaimed", _pel_reclaimed))
    }


class TestPelCounters:
    async def test_production_shape_counts_one_eviction_per_stream(self, fake_redis):
        """PRODUCTION PATH (Redis 7.4 + redis-py 8): the lost ids arrive in reply[2].

        One loss on EACH stream, so a counter that hardcoded a single label value
        — or that labelled both losses the same — cannot pass. The reclaimed
        counter must NOT move: nothing was recovered, the entries are gone.
        """
        from services.consumer import reclaim_pending

        settings = _test_settings(consumer_pel_reclaim_min_idle_seconds=0)
        before = _pel_snapshot()

        event_id = await fake_redis.xadd(
            settings.events_stream,
            {"tenant_id": T("t1"), "path": "/workers/api/tasks", "status_code": "200"},
        )
        cost_id = await fake_redis.xadd(settings.costs_stream, _cost_payload("claude-opus-4"))
        await TestReclaimPendingEviction._strand_in_pel(fake_redis, settings)

        # The trim: both entries leave the stream while still un-acked.
        assert await fake_redis.xdel(settings.events_stream, event_id) == 1
        assert await fake_redis.xdel(settings.costs_stream, cost_id) == 1

        replies = _record_xautoclaim_replies(fake_redis)
        ev: list[dict] = []
        co: list[dict] = []
        staged = await reclaim_pending(fake_redis, settings, ev, co)

        # Guard the premise on the replies the code ACTUALLY consumed — otherwise
        # this test proves nothing about production.
        assert all(len(r) == 3 for r in replies), "premise broken: not the Redis>=7 reply"
        assert {mid for r in replies for mid in r[2]} == {event_id, cost_id}
        assert staged == 0

        after = _pel_snapshot()
        assert after[("evicted", "usage")] == before[("evicted", "usage")] + 1
        assert after[("evicted", "cost")] == before[("evicted", "cost")] + 1
        assert after[("reclaimed", "usage")] == before[("reclaimed", "usage")], (
            "nothing was recovered — a destroyed entry must never count as reclaimed"
        )
        assert after[("reclaimed", "cost")] == before[("reclaimed", "cost")]

    async def test_healthy_reclaim_counts_reclaimed_and_never_evicted(self, fake_redis):
        """THE CONTROL. A stranded entry that still EXISTS is recovered, not lost.

        Without this case a counter wired into the wrong branch — or incremented
        unconditionally once per entry — passes the eviction tests above while
        reporting data loss on every successful recovery, which is the alarm this
        counter exists to make trustworthy.
        """
        from services.consumer import reclaim_pending

        settings = _test_settings(consumer_pel_reclaim_min_idle_seconds=0)
        before = _pel_snapshot()

        await fake_redis.xadd(settings.costs_stream, _cost_payload("claude-opus-4"))
        await fake_redis.xadd(
            settings.events_stream,
            {"tenant_id": T("t1"), "path": "/workers/api/tasks", "status_code": "200"},
        )
        await TestReclaimPendingEviction._strand_in_pel(fake_redis, settings)
        # NO xdel: the entries are still in their streams, so XAUTOCLAIM recovers
        # them and the loss counter must stay exactly where it was.

        ev: list[dict] = []
        co: list[dict] = []
        staged = await reclaim_pending(fake_redis, settings, ev, co)

        assert staged == 2, "premise broken: both stranded entries must be reclaimable"
        assert len(ev) == 1 and len(co) == 1

        after = _pel_snapshot()
        assert after[("reclaimed", "usage")] == before[("reclaimed", "usage")] + 1
        assert after[("reclaimed", "cost")] == before[("reclaimed", "cost")] + 1
        assert after[("evicted", "usage")] == before[("evicted", "usage")], (
            "a successful recovery must never be counted as confirmed data loss"
        )
        assert after[("evicted", "cost")] == before[("evicted", "cost")]

    async def test_legacy_nil_entries_each_count_once(self, fake_redis):
        """LEGACY PATH (Redis 6.2 / redis-py < 5): two losses share the id `None`.

        They must be counted POSITIONALLY, like the log lines are. An id-keyed
        dedup would collapse two destroyed cost events into one increment and
        under-report the loss by half — the same defect the log test above pins,
        at the other instrument.
        """
        from services.consumer import reclaim_pending

        settings = _test_settings(consumer_pel_reclaim_min_idle_seconds=0)
        before = _pel_snapshot()

        lost_a = await fake_redis.xadd(settings.costs_stream, _cost_payload("claude-opus-4"))
        lost_b = await fake_redis.xadd(settings.costs_stream, _cost_payload("claude-sonnet-4"))
        await TestReclaimPendingEviction._strand_in_pel(fake_redis, settings)
        assert await fake_redis.xdel(settings.costs_stream, lost_a) == 1
        assert await fake_redis.xdel(settings.costs_stream, lost_b) == 1

        replies = _reframe_reply_as_legacy_redis_62(fake_redis)
        ev: list[dict] = []
        co: list[dict] = []
        staged = await reclaim_pending(fake_redis, settings, ev, co)

        assert all(len(r) == 2 for r in replies), "premise broken: not the legacy reply"
        assert [(None, None), (None, None)] in [list(r[1]) for r in replies]
        assert staged == 0

        after = _pel_snapshot()
        assert after[("evicted", "cost")] == before[("evicted", "cost")] + 2, (
            "two nil entries are two distinct losses; an id-based dedup must not collapse them"
        )
        assert after[("evicted", "usage")] == before[("evicted", "usage")]
        assert after[("reclaimed", "cost")] == before[("reclaimed", "cost")]
