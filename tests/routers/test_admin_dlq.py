"""Tests for the DLQ-operability admin endpoints + services.dlq_ops.

These drive the real code path: a real fakeredis stream, the real
``services._cost_schema.validate_cost_event`` boundary, and the real audit
write against Postgres. The dead-letter payload is seeded in exactly the shape
``services.cost_flusher`` writes it (business wire fields + ``_dlq_reason`` /
``_dlq_original_id`` / ``_dlq_at``), so replay's quarantine-reversal is tested
against the true producer format.

Redis injection mirrors the app: the endpoints resolve the live connection via
the module-level ``redis_client._redis`` (through the ``get_redis`` dependency),
so a fixture points that global at a FakeRedis the test seeds directly.
"""

from __future__ import annotations

import fakeredis.aioredis
import pytest
import pytest_asyncio
from sqlalchemy import select

import auth_deps
from models.db import AuditLog
from services.dlq_ops import inspect_dlq, replay_dlq
from settings import get_settings
from tests.conftest import T

DEAD_STREAM = "billing:costs:dead"
COSTS_STREAM = "billing:costs"


@pytest.fixture
def admin_actor(monkeypatch):
    """Force the bearer decode to a known license-admin payload (matches
    test_reprice.admin_actor)."""
    fake_payload = {
        "sub": "test-admin-user",
        "realm_access": {"roles": ["license-admin"]},
    }

    async def _fake_decode(_request):
        return fake_payload

    import auth

    monkeypatch.setattr(auth, "DASHBOARD_AUTH_ENABLED", True)
    monkeypatch.setattr(auth, "_decode_bearer_token", _fake_decode)
    monkeypatch.setattr(auth_deps, "_decode_bearer_token", _fake_decode)
    return fake_payload


@pytest_asyncio.fixture
async def dlq_env(app_client, monkeypatch):
    """Point ``redis_client._redis`` at a fresh FakeRedis AFTER app_client has
    nulled it, and hand back both the client and the raw connection to seed."""
    import redis_client

    fake = fakeredis.aioredis.FakeRedis(decode_responses=True)
    monkeypatch.setattr(redis_client, "_redis", fake)
    yield app_client, fake
    await fake.aclose()


async def _seed_dead(
    redis,
    *,
    reason: str = "missing_provider",
    original_id: str = "10-1",
    provider: str = "claude",
    **extra: str,
) -> str:
    """Seed one dead-letter entry in cost_flusher's on-wire shape; return its id."""
    fields = {
        "provider": provider,
        "model": "sonnet",
        "input_tokens": "100",
        "output_tokens": "50",
        "tenant_id": T("tenant-a"),
        "cost_source": "test",
        "_dlq_reason": reason,
        "_dlq_original_id": original_id,
        "_dlq_at": "2026-07-24T00:00:00+00:00",
    }
    fields.update(extra)
    return await redis.xadd(DEAD_STREAM, fields)


@pytest.mark.unit
class TestDlqInspect:
    async def test_inspect_returns_entries(self, dlq_env, admin_actor) -> None:
        client, fake = dlq_env
        for i in range(3):
            await _seed_dead(fake, original_id=f"10-{i}")

        resp = client.get("/api/admin/dlq/inspect")
        assert resp.status_code == 200, resp.text
        data = resp.json()["data"]
        assert data["count"] == 3
        assert len(data["entries"]) == 3
        # Business fields kept; _dlq_* bookkeeping surfaced separately.
        first = data["entries"][0]
        assert first["reason"] == "missing_provider"
        assert first["original_id"] == "10-0"
        assert "_dlq_reason" not in first["fields"]
        assert first["fields"]["provider"] == "claude"

    async def test_inspect_filters_by_reason(self, dlq_env, admin_actor) -> None:
        client, fake = dlq_env
        await _seed_dead(fake, reason="missing_provider", original_id="a")
        await _seed_dead(fake, reason="missing_provider", original_id="b")
        await _seed_dead(fake, reason="negative_output_tokens", original_id="c")

        resp = client.get("/api/admin/dlq/inspect", params={"reason": "missing_provider"})
        assert resp.status_code == 200, resp.text
        data = resp.json()["data"]
        assert data["count"] == 2
        assert {e["original_id"] for e in data["entries"]} == {"a", "b"}

    async def test_inspect_caps_limit_at_500(self, fake_redis, monkeypatch) -> None:
        """The service clamps limit to 500 before it ever reaches Redis — proven
        by spying on the count kwarg xrange is called with."""
        await _seed_dead(fake_redis, original_id="only")

        captured: dict[str, int | None] = {}
        original = fake_redis.xrange

        async def _spy(*args, **kwargs):
            captured["count"] = kwargs.get("count")
            return await original(*args, **kwargs)

        monkeypatch.setattr(fake_redis, "xrange", _spy)

        result = await inspect_dlq(fake_redis, limit=10_000)
        assert captured["count"] == 500
        assert result.count == 1

    async def test_inspect_requires_pricing_admin(self, dlq_env, monkeypatch) -> None:
        """No token with auth enabled => 401 (matches test_reprice convention)."""
        import auth

        monkeypatch.setattr(auth, "DASHBOARD_AUTH_ENABLED", True)
        client, _fake = dlq_env
        resp = client.get("/api/admin/dlq/inspect")
        assert resp.status_code == 401, resp.text


@pytest.mark.unit
class TestDlqReplay:
    async def test_replay_reinjects_and_strips_dlq_fields(
        self, dlq_env, db_session, admin_actor
    ) -> None:
        """A now-valid quarantined entry is re-injected onto billing:costs with
        NO _dlq_* keys and removed from the dead stream; one audit row lands."""
        client, fake = dlq_env
        # Seeded with a real provider => validate_cost_event passes on replay.
        dead_id = await _seed_dead(fake, provider="claude", reason="invalid_input_tokens")

        resp = client.post("/api/admin/dlq/replay", json={"ids": [dead_id]})
        assert resp.status_code == 200, resp.text
        data = resp.json()["data"]
        assert data["replayed"] == [dead_id]
        assert data["deleted_from_dlq"] == [dead_id]
        assert data["still_malformed"] == []

        # Re-injected onto billing:costs, quarantine bookkeeping stripped.
        live = await fake.xrange(COSTS_STREAM)
        assert len(live) == 1
        _live_id, live_fields = live[0]
        assert live_fields["provider"] == "claude"
        assert not any(k.startswith("_dlq_") for k in live_fields)
        # Removed from the dead stream.
        assert await fake.xrange(DEAD_STREAM) == []

        # Exactly one audit row recording the replay.
        audits = list(
            (
                await db_session.execute(
                    select(AuditLog).where(AuditLog.action == "admin.dlq_replay")
                )
            )
            .scalars()
            .all()
        )
        assert len(audits) == 1
        assert audits[0].before["requested_ids"] == [dead_id]
        assert audits[0].after["replayed"] == [dead_id]

    async def test_replay_refuses_still_malformed(self, dlq_env, admin_actor) -> None:
        """The infinite-loop guard: an entry that STILL fails validation stays on
        the dead stream, is reported in still_malformed, and is NOT re-injected."""
        client, fake = dlq_env
        # provider="" => cleaned payload still fails validate_cost_event.
        dead_id = await _seed_dead(fake, provider="", reason="missing_provider")

        resp = client.post("/api/admin/dlq/replay", json={"ids": [dead_id]})
        assert resp.status_code == 200, resp.text
        data = resp.json()["data"]
        assert data["replayed"] == []
        assert data["still_malformed"] == [{"id": dead_id, "reason": "missing_provider"}]

        # Left quarantined, nothing re-injected.
        assert len(await fake.xrange(DEAD_STREAM)) == 1
        assert await fake.xrange(COSTS_STREAM) == []
        # Marker released so a genuine future fix can retry. Assert the REAL
        # key (keyed on _dlq_original_id) — asserting the old entry-id key would
        # now pass vacuously and stop testing the release at all.
        assert await fake.get("dlq:replayed:orig:10-1") is None

    async def test_replay_idempotent_on_rerun(self, dlq_env, admin_actor) -> None:
        """Replaying the same id twice re-injects exactly once; the second run
        reports already_processed (delete_on_success=False keeps the entry so the
        Redis marker, not stream absence, is what dedupes)."""
        client, fake = dlq_env
        dead_id = await _seed_dead(fake, provider="claude", reason="invalid_input_tokens")

        first = client.post(
            "/api/admin/dlq/replay",
            json={"ids": [dead_id], "delete_on_success": False},
        )
        assert first.status_code == 200, first.text
        assert first.json()["data"]["replayed"] == [dead_id]

        second = client.post(
            "/api/admin/dlq/replay",
            json={"ids": [dead_id], "delete_on_success": False},
        )
        assert second.status_code == 200, second.text
        sdata = second.json()["data"]
        assert sdata["replayed"] == []
        assert sdata["already_processed"] == [dead_id]

        # Exactly one re-injection despite two calls.
        assert len(await fake.xrange(COSTS_STREAM)) == 1

    async def test_replay_service_missing_id_is_skipped(self, fake_redis) -> None:
        """A requested id absent from the stream belongs to no result bucket."""
        result = await replay_dlq(fake_redis, ids=["999-0"])
        assert result.replayed == []
        assert result.already_processed == []
        assert result.still_malformed == []
        assert result.deleted_from_dlq == []

    async def test_replay_marker_outlives_the_dead_entry_it_guards(self, fake_redis) -> None:
        """Double-bill guard: the idempotency marker MUST outlive the dead entry
        it guards. Otherwise, in delete_on_success=False mode a second drain
        after the marker expired (but while the entry is still retained)
        re-injects the SAME event → double bill.

        ⚠️ ``TTL`` RETURNS A SENTINEL, NOT ONLY A MAGNITUDE. ``-1`` means "key
        exists, never expires" — the STRONGEST possible satisfaction of this
        invariant — and ``-2`` means "no such key", a total failure of it. This
        assertion used to read ``ttl >= retention_seconds``, which ranks both
        sentinels BELOW every finite TTL and so went red on the guard becoming
        permanent while staying green on the guard being absent. Compare against
        the sentinels explicitly.

        (A finite-but-sufficient TTL is still accepted: the retention window is
        what a TTL would have to beat, and pinning "must be exactly permanent"
        here would over-specify a best-effort Redis call.)
        """
        dead_id = await _seed_dead(
            fake_redis, provider="claude", reason="invalid_input_tokens", original_id="42-0"
        )
        retention_seconds = get_settings().dead_letter_stream_max_age_hours * 3600

        # Observe the CLAIM ttl mid-flight — after the SET NX, before the
        # re-inject completes and the marker is PERSISTed. Without this the
        # end-state assertion below could no longer see the claim TTL at all,
        # and a regression that dropped the floor to (say) an hour would go
        # unnoticed: the crash-recovery window is a real guarantee of its own.
        # Spies `eval`, not `xadd`: the re-inject and the marker's PERSIST are
        # one atomic script (dlq_ops._REINJECT_LUA), so the script call is where
        # the claim TTL is still observable.
        claim_ttl: list[int] = []
        real_eval = fake_redis.eval

        async def _spy_eval(*args, **kwargs):
            claim_ttl.append(await fake_redis.ttl("dlq:replayed:orig:42-0"))
            return await real_eval(*args, **kwargs)

        fake_redis.eval = _spy_eval
        try:
            await replay_dlq(fake_redis, ids=[dead_id], delete_on_success=False)
        finally:
            fake_redis.eval = real_eval

        assert claim_ttl and claim_ttl[0] >= retention_seconds, (
            f"the in-flight claim TTL was {claim_ttl}, inside the {retention_seconds}s "
            "retention window the dead entry can survive"
        )

        # Keyed on the ORIGINAL event id (see services.dlq_ops._replay_marker):
        # the dead-stream entry id deduped the wrong thing — one source event
        # can be quarantined twice under two dead ids.
        ttl = await fake_redis.ttl("dlq:replayed:orig:42-0")
        assert ttl != -2, "the replay marker is GONE — a second drain would re-bill the event"
        # -1 = no expiry (what replay_dlq now PERSISTs it to); otherwise the TTL
        # must at least beat the retention window the entry could survive.
        assert ttl == -1 or ttl >= retention_seconds, (
            f"marker expires in {ttl}s, inside the {retention_seconds}s the dead entry can survive"
        )

    async def test_replay_releases_marker_when_reinject_fails(
        self, fake_redis, monkeypatch
    ) -> None:
        """Fail-open guard: if the re-inject onto billing:costs raises AFTER the
        marker is claimed, the marker must be released so a retry can re-inject —
        the event must NOT be silently dropped (left on :dead, marked replayed).

        Patches ``eval``, not ``xadd``: the re-inject and the marker's PERSIST
        are one atomic script (``dlq_ops._REINJECT_LUA``), so the script call is
        the operation that can fail here.
        """
        dead_id = await _seed_dead(fake_redis, provider="claude", reason="invalid_input_tokens")

        async def _boom(*_args, **_kwargs):
            raise ConnectionError("redis down mid-replay")

        monkeypatch.setattr(fake_redis, "eval", _boom)
        result = await replay_dlq(fake_redis, ids=[dead_id])

        assert result.replayed == []
        # Marker released → a retry is possible (goes RED without the release fix).
        assert await fake_redis.get("dlq:replayed:orig:10-1") is None
        # Entry preserved on the dead stream, not lost.
        assert len(await fake_redis.xrange(DEAD_STREAM)) == 1

    async def test_replay_double_bill_prevented_within_retention(self, fake_redis) -> None:
        """End-to-end of the TTL fix: with the entry retained (delete_on_success
        =False) and the marker still live, a second drain pass does NOT re-inject —
        exactly one copy on billing:costs despite two calls."""
        dead_id = await _seed_dead(fake_redis, provider="claude", reason="invalid_input_tokens")
        await replay_dlq(fake_redis, ids=[dead_id], delete_on_success=False)
        second = await replay_dlq(fake_redis, ids=[dead_id], delete_on_success=False)

        assert second.replayed == []
        assert second.already_processed == [dead_id]
        assert len(await fake_redis.xrange(COSTS_STREAM)) == 1


@pytest.mark.unit
class TestDlqInspectHasMore:
    async def test_has_more_false_at_exactly_full_page(self, fake_redis) -> None:
        """has_more must be False when the stream holds exactly `limit` entries —
        nothing lies beyond a page that is merely full (XLEN, not fetch count)."""
        for i in range(3):
            await _seed_dead(fake_redis, original_id=f"10-{i}")
        result = await inspect_dlq(fake_redis, limit=3)
        assert result.count == 3
        assert result.has_more is False

    async def test_has_more_true_when_stream_exceeds_page(self, fake_redis) -> None:
        for i in range(4):
            await _seed_dead(fake_redis, original_id=f"10-{i}")
        result = await inspect_dlq(fake_redis, limit=3)
        assert result.count == 3
        assert result.has_more is True


@pytest.mark.unit
class TestDeadLetterDepthResampleOnReplay:
    """Replay XDELs too, so it must refresh the depth gauge exactly as discard does.

    THE MUTANT THAT SURVIVED. The discard half of this change shipped with three
    tests; the replay half shipped with NONE. An adversarial pass deleted
    ``if deleted_from_dlq: await _resample_depth(...)`` from ``replay_dlq`` and
    the FULL suite stayed green — 1786 passed, byte-identical to baseline. Half a
    behavioural change was unobservable, and the discard tests could not see it
    because they never call replay.

    The gauge is poisoned with a value nothing could reach before each call, so a
    pass cannot be the coincidence of an already-correct reading.
    """

    async def test_a_replay_that_deletes_resamples_the_depth(self, fake_redis) -> None:
        from observability import metrics as observability_metrics

        keep_id = await _seed_dead(fake_redis, original_id="80-0")
        drop_id = await _seed_dead(
            fake_redis, provider="claude", reason="invalid_input_tokens", original_id="80-1"
        )
        observability_metrics.cost_dead_letter_depth.set(999)

        result = await replay_dlq(fake_redis, ids=[drop_id], delete_on_success=True)

        assert result.deleted_from_dlq == [drop_id]
        assert observability_metrics.cost_dead_letter_depth._value.get() == 1
        assert [i for i, _ in await fake_redis.xrange(DEAD_STREAM)] == [keep_id]

    async def test_delete_on_success_false_does_not_resample(self, fake_redis) -> None:
        """The case that makes the guard choice load-bearing.

        ``delete_on_success=False`` re-injects and XDELs NOTHING, so the depth has
        not moved and resampling would spend a round trip to write back the value
        the gauge already holds. This is the replay-side twin of
        ``test_a_wholly_refused_batch_does_not_resample``: guard on
        ``deleted_from_dlq``, never on "a replay happened".
        """
        from observability import metrics as observability_metrics

        dead_id = await _seed_dead(
            fake_redis, provider="claude", reason="invalid_input_tokens", original_id="81-0"
        )
        observability_metrics.cost_dead_letter_depth.set(999)

        result = await replay_dlq(fake_redis, ids=[dead_id], delete_on_success=False)

        assert result.replayed == [dead_id]
        assert result.deleted_from_dlq == []
        assert observability_metrics.cost_dead_letter_depth._value.get() == 999
