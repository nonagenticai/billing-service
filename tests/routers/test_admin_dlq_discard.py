"""Tests for POST /api/admin/dlq/discard + services.dlq_ops.discard_dlq.

The endpoint is the ONE place this service deletes anything, so these tests are
weighted almost entirely toward the DENY paths: a passing happy path proves
nothing here. Every guard below is asserted against the real fakeredis stream —
"refused" is only meaningful if the entry is still THERE afterwards, so each
refusal test re-reads the stream rather than trusting the response counts.

Fixtures mirror ``tests/routers/test_admin_dlq.py`` (same ``admin_actor`` bearer
stub, same ``redis_client._redis`` injection), so the auth and Redis wiring under
test is the wiring the replay tests already exercise.

NB ``caplog`` is inert in this repo — structlog bypasses the stdlib handlers — so
the log-trail assertion uses ``capsys``.
"""

from __future__ import annotations

import fakeredis.aioredis
import pytest
import pytest_asyncio
from sqlalchemy import select

import auth_deps
from models.db import AuditLog
from services.dlq_ops import discard_dlq
from tenancy import NIL_TENANT_ID
from tests.conftest import T

DEAD_STREAM = "billing:costs:dead"


@pytest.fixture
def admin_actor(monkeypatch):
    """Force the bearer decode to a known license-admin payload (matches
    tests/routers/test_admin_dlq.py::admin_actor)."""
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
    reason: str = "nil_tenant_id",
    original_id: str = "10-1",
    tenant_id: str | None = NIL_TENANT_ID,
    cost_usd: str | None = "0.00",
    service_key: str = "worker-management",
    **extra: str,
) -> str:
    """Seed one dead-letter entry in cost_flusher's on-wire shape; return its id.

    Defaults reproduce the 892-entry population this feature exists for: the nil
    tenant, ``cost_usd`` exactly zero, quarantined as ``nil_tenant_id``. Pass
    ``tenant_id=None`` / ``cost_usd=None`` to omit the field entirely.
    """
    fields = {
        "provider": "claude",
        "model": "sonnet",
        "input_tokens": "100",
        "output_tokens": "50",
        "service_key": service_key,
        "cost_source": "test",
        "_dlq_reason": reason,
        "_dlq_original_id": original_id,
        "_dlq_at": "2026-08-27T00:00:00+00:00",
    }
    if tenant_id is not None:
        fields["tenant_id"] = tenant_id
    if cost_usd is not None:
        fields["cost_usd"] = cost_usd
    fields.update(extra)
    return await redis.xadd(DEAD_STREAM, fields)


async def _stream_ids(redis) -> list[str]:
    return [entry_id for entry_id, _ in (await redis.xrange(DEAD_STREAM) or [])]


@pytest.mark.unit
class TestDiscardAuthGate:
    async def test_discard_requires_a_token(self, dlq_env, monkeypatch) -> None:
        """No bearer with auth enabled => 401 (same convention as
        TestDlqInspect::test_inspect_requires_pricing_admin). Goes RED if the
        route is registered without require_pricing_admin."""
        import auth

        monkeypatch.setattr(auth, "DASHBOARD_AUTH_ENABLED", True)
        client, fake = dlq_env
        dead_id = await _seed_dead(fake)

        resp = client.post(
            "/api/admin/dlq/discard",
            json={"ids": [dead_id], "reason": "unauthenticated attempt", "confirm": True},
        )
        assert resp.status_code in (401, 403), resp.text
        # And the entry is untouched — a rejected call must not have deleted.
        assert await _stream_ids(fake) == [dead_id]

    async def test_discard_rejects_wrong_role(self, dlq_env, monkeypatch) -> None:
        """A valid token whose roles miss BOTH license-admin and billing-internal
        gets 403 from require_pricing_admin, and deletes nothing."""
        import auth

        async def _fake_decode(_request):
            return {"sub": "someone-else", "realm_access": {"roles": ["tenant-user"]}}

        monkeypatch.setattr(auth, "DASHBOARD_AUTH_ENABLED", True)
        monkeypatch.setattr(auth, "_decode_bearer_token", _fake_decode)
        monkeypatch.setattr(auth_deps, "_decode_bearer_token", _fake_decode)

        client, fake = dlq_env
        dead_id = await _seed_dead(fake)

        resp = client.post(
            "/api/admin/dlq/discard",
            json={"ids": [dead_id], "reason": "wrong role", "confirm": True},
        )
        assert resp.status_code == 403, resp.text
        assert await _stream_ids(fake) == [dead_id]


@pytest.mark.unit
class TestDiscardRefusesIneligible:
    """The carve-out is BOTH conditions. Each of these entries fails exactly one
    of them and must survive, even under confirm=true."""

    async def test_refuses_nonzero_cost(self, dlq_env, admin_actor) -> None:
        client, fake = dlq_env
        dead_id = await _seed_dead(fake, cost_usd="0.42")

        resp = client.post(
            "/api/admin/dlq/discard",
            json={"ids": [dead_id], "reason": "drain nil-tenant backlog", "confirm": True},
        )
        assert resp.status_code == 200, resp.text
        data = resp.json()["data"]
        assert data["examined"] == 1
        assert data["eligible"] == []
        assert data["discarded"] == []
        assert data["refused"] == [{"id": dead_id, "reason": "cost_usd_nonzero"}]
        # The point of the assertion: it is still on the stream.
        assert await _stream_ids(fake) == [dead_id]

    async def test_refuses_real_tenant(self, dlq_env, admin_actor) -> None:
        client, fake = dlq_env
        dead_id = await _seed_dead(fake, tenant_id=T("tenant-a"), cost_usd="0.00")

        resp = client.post(
            "/api/admin/dlq/discard",
            json={"ids": [dead_id], "reason": "drain nil-tenant backlog", "confirm": True},
        )
        assert resp.status_code == 200, resp.text
        data = resp.json()["data"]
        assert data["refused"] == [{"id": dead_id, "reason": "tenant_attributable"}]
        assert data["discarded"] == []
        assert await _stream_ids(fake) == [dead_id]

    async def test_refuses_unresolvable_tenant(self, fake_redis) -> None:
        """Narrower than _cost_schema's unattributable set on purpose: a slug that
        is merely not registered yet can still be made attributable, so it is
        REFUSED rather than discarded."""
        dead_id = await _seed_dead(fake_redis, tenant_id="some-unregistered-slug")

        result = await discard_dlq(
            fake_redis, ids=[dead_id], reason="drain nil-tenant backlog", confirm=True
        )
        assert result.refused == [{"id": dead_id, "reason": "tenant_unresolved"}]
        assert result.discarded == []
        assert await _stream_ids(fake_redis) == [dead_id]

    async def test_an_emitter_sentinel_at_zero_cost_is_eligible(self, fake_redis) -> None:
        """The one unresolved tenant that can never become attributable.

        ``unauthenticated`` is what an emitter writes when it has NO tenant at all
        (asr-backend's Rust gate-disabled path). Registering it as a real tenant
        would silently turn an unauthenticated call into a billable customer, so
        no operator action can recover the attribution -- the same property that
        makes the nil tenant discardable. Measured live 2026-11-26: the standing
        asr-backend fossil is exactly this shape.
        """
        dead_id = await _seed_dead(
            fake_redis,
            tenant_id="unauthenticated",
            reason="missing_provider",
            cost_usd="0.0",
        )

        result = await discard_dlq(
            fake_redis,
            ids=[dead_id],
            reason="zero-cost sentinel-tenant fossil from the pre-fix ASR emitter",
            confirm=True,
        )

        assert result.eligible == [dead_id]
        assert result.discarded == [dead_id]
        assert await _stream_ids(fake_redis) == []

    async def test_an_emitter_sentinel_with_nonzero_cost_is_still_refused(self, fake_redis) -> None:
        """The sentinel widens WHO may be unattributable, never the money gate."""
        dead_id = await _seed_dead(fake_redis, tenant_id="unauthenticated", cost_usd="0.42")

        result = await discard_dlq(
            fake_redis, ids=[dead_id], reason="drain nil-tenant backlog", confirm=True
        )

        assert result.refused == [{"id": dead_id, "reason": "cost_usd_nonzero"}]
        assert result.discarded == []
        assert await _stream_ids(fake_redis) == [dead_id]

    async def test_refuses_absent_cost(self, fake_redis) -> None:
        """An absent cost is NOT read as zero: a value that cannot be read is a
        value that cannot be ruled out."""
        dead_id = await _seed_dead(fake_redis, cost_usd=None)

        result = await discard_dlq(
            fake_redis, ids=[dead_id], reason="drain nil-tenant backlog", confirm=True
        )
        assert result.refused == [{"id": dead_id, "reason": "cost_usd_absent"}]
        assert await _stream_ids(fake_redis) == [dead_id]

    async def test_refuses_unparseable_cost(self, fake_redis) -> None:
        dead_id = await _seed_dead(fake_redis, cost_usd="not-a-number")

        result = await discard_dlq(
            fake_redis, ids=[dead_id], reason="drain nil-tenant backlog", confirm=True
        )
        assert result.refused == [{"id": dead_id, "reason": "cost_usd_unparseable"}]
        assert await _stream_ids(fake_redis) == [dead_id]

    async def test_refused_entry_never_blocks_an_eligible_sibling(self, fake_redis) -> None:
        """Per-id isolation: a mixed batch discards exactly the eligible entry and
        leaves the refused one in place."""
        keep_id = await _seed_dead(fake_redis, cost_usd="1.50", original_id="20-0")
        drop_id = await _seed_dead(fake_redis, original_id="20-1")

        result = await discard_dlq(
            fake_redis, ids=[keep_id, drop_id], reason="mixed batch", confirm=True
        )
        assert result.examined == 2
        assert result.eligible == [drop_id]
        assert result.discarded == [drop_id]
        assert result.refused == [{"id": keep_id, "reason": "cost_usd_nonzero"}]
        assert await _stream_ids(fake_redis) == [keep_id]


@pytest.mark.unit
class TestDiscardDryRunIsTheDefault:
    async def test_default_call_deletes_nothing_but_reports_counts(
        self, dlq_env, admin_actor
    ) -> None:
        """No ``confirm`` in the body at all: the entry is eligible, is REPORTED as
        eligible, and is still on the stream. Goes RED if confirm defaults True or
        the dry-run short-circuit is dropped."""
        client, fake = dlq_env
        dead_id = await _seed_dead(fake)

        resp = client.post(
            "/api/admin/dlq/discard",
            json={"ids": [dead_id], "reason": "measure the backlog"},
        )
        assert resp.status_code == 200, resp.text
        data = resp.json()["data"]
        assert data["examined"] == 1
        assert data["eligible"] == [dead_id]
        assert data["discarded"] == []
        assert data["refused"] == []
        assert await _stream_ids(fake) == [dead_id]

    async def test_confirm_false_explicitly_deletes_nothing(self, fake_redis) -> None:
        dead_id = await _seed_dead(fake_redis)
        result = await discard_dlq(
            fake_redis, ids=[dead_id], reason="measure the backlog", confirm=False
        )
        assert result.eligible == [dead_id]
        assert result.discarded == []
        assert await _stream_ids(fake_redis) == [dead_id]


@pytest.mark.unit
class TestDiscardRequestValidation:
    async def test_reason_is_required(self, dlq_env, admin_actor) -> None:
        client, fake = dlq_env
        dead_id = await _seed_dead(fake)
        resp = client.post("/api/admin/dlq/discard", json={"ids": [dead_id], "confirm": True})
        assert resp.status_code == 422, resp.text
        assert await _stream_ids(fake) == [dead_id]

    async def test_blank_reason_is_rejected(self, dlq_env, admin_actor) -> None:
        """``min_length=1`` alone admits a single space — the field validator is
        what makes the audit string meaningful."""
        client, fake = dlq_env
        dead_id = await _seed_dead(fake)
        resp = client.post(
            "/api/admin/dlq/discard",
            json={"ids": [dead_id], "reason": "   ", "confirm": True},
        )
        assert resp.status_code == 422, resp.text
        assert await _stream_ids(fake) == [dead_id]


@pytest.mark.unit
class TestDiscardHappyPathAndTrail:
    async def test_confirmed_discard_removes_eligible_entry(
        self, dlq_env, db_session, admin_actor
    ) -> None:
        """The one path that deletes: cost_usd == 0.00 AND the nil tenant, with
        confirm=true. One audit row records who dropped what and why."""
        client, fake = dlq_env
        dead_id = await _seed_dead(fake)

        resp = client.post(
            "/api/admin/dlq/discard",
            json={
                "ids": [dead_id],
                "reason": "undrainable nil_tenant_id backlog (#92)",
                "confirm": True,
            },
        )
        assert resp.status_code == 200, resp.text
        data = resp.json()["data"]
        assert data["examined"] == 1
        assert data["eligible"] == [dead_id]
        assert data["discarded"] == [dead_id]
        assert data["refused"] == []
        assert await _stream_ids(fake) == []

        audits = list(
            (
                await db_session.execute(
                    select(AuditLog).where(AuditLog.action == "admin.dlq_discard")
                )
            )
            .scalars()
            .all()
        )
        assert len(audits) == 1
        assert audits[0].before["requested_ids"] == [dead_id]
        assert audits[0].before["reason"] == "undrainable nil_tenant_id backlog (#92)"
        assert audits[0].before["confirm"] is True
        assert audits[0].after["discarded"] == [dead_id]

    async def test_absent_tenant_field_is_eligible(self, fake_redis) -> None:
        """The other unattributable shape: no tenant_id key at all."""
        dead_id = await _seed_dead(fake_redis, tenant_id=None, reason="missing_tenant_id")
        result = await discard_dlq(fake_redis, ids=[dead_id], reason="drain backlog", confirm=True)
        assert result.discarded == [dead_id]
        assert await _stream_ids(fake_redis) == []

    async def test_missing_id_is_not_examined(self, fake_redis) -> None:
        result = await discard_dlq(fake_redis, ids=["999-0"], reason="drain backlog", confirm=True)
        assert result.examined == 0
        assert result.eligible == []
        assert result.discarded == []
        assert result.refused == []

    async def test_each_discard_leaves_a_log_trail(self, fake_redis, capsys) -> None:
        """capsys, NOT caplog: structlog bypasses the stdlib handlers in this repo.

        The entry does not survive to explain itself, so the log line is the only
        record it existed — it must name the service, the operator's reason, and
        the run's counts.
        """
        dead_id = await _seed_dead(fake_redis, service_key="inference-router")

        await discard_dlq(
            fake_redis,
            ids=[dead_id],
            reason="undrainable nil_tenant_id backlog (#92)",
            confirm=True,
        )

        out = capsys.readouterr().out
        assert "dlq_entry_discarded" in out
        assert "inference-router" in out
        assert "undrainable nil_tenant_id backlog (#92)" in out
        # Summary line with the run's counts.
        assert "dlq_discarded" in out
        assert "discarded=1" in out or '"discarded": 1' in out or '"discarded":1' in out


@pytest.mark.unit
class TestDeadLetterDepthResample:
    """The depth gauge must not stay stale for five minutes after a drain.

    THE INCIDENT. ``billing_cost_dead_letter_depth`` had exactly one writer: the
    consumer's trim block, which runs once per
    ``cost_stream_trim_interval_seconds`` (default 300s, no live override). On
    2026-08-28 a confirmed discard emptied the stream at 16:13:59Z and the gauge
    still read 36 a minute later. Nothing was wrong — the sample was simply up to
    five minutes old — but a lagging gauge and a failed discard are
    indistinguishable to a reader, and the reading was nearly filed as a broken
    metric. The next reader would have re-run a destructive command against an
    already-empty queue.
    """

    async def test_a_confirmed_discard_resamples_the_depth(self, fake_redis) -> None:
        from observability import metrics as observability_metrics

        keep_id = await _seed_dead(fake_redis, cost_usd="1.50", original_id="90-0")
        drop_id = await _seed_dead(fake_redis, original_id="90-1")
        # Poison the gauge with a value nothing could reach, so a passing
        # assertion cannot be the coincidence of an already-correct reading.
        observability_metrics.cost_dead_letter_depth.set(999)

        await discard_dlq(fake_redis, ids=[drop_id], reason="resample check", confirm=True)

        # One entry deleted, one refused and still present.
        assert await _stream_ids(fake_redis) == [keep_id]
        assert observability_metrics.cost_dead_letter_depth._value.get() == 1

    async def test_a_dry_run_does_not_resample(self, fake_redis) -> None:
        """The negative control, and it is about cost, not correctness.

        A dry run deletes nothing, so the depth cannot have changed; resampling
        there would spend a round trip to write back the value the gauge already
        holds. Guarding on ``discarded`` rather than on ``confirm`` is what makes
        that true, and this test is what stops the guard drifting to ``confirm``.
        """
        from observability import metrics as observability_metrics

        dead_id = await _seed_dead(fake_redis, original_id="91-0")
        observability_metrics.cost_dead_letter_depth.set(999)

        result = await discard_dlq(
            fake_redis, ids=[dead_id], reason="resample dry run", confirm=False
        )

        assert result.discarded == []
        assert observability_metrics.cost_dead_letter_depth._value.get() == 999

    async def test_a_wholly_refused_batch_does_not_resample(self, fake_redis) -> None:
        """A confirmed run that deletes nothing must not resample either.

        Same reason as the dry run: ``confirm=True`` is not evidence that the
        stream moved. Without this, a guard on ``confirm`` would look correct in
        every test above.
        """
        from observability import metrics as observability_metrics

        dead_id = await _seed_dead(fake_redis, cost_usd="1.50", original_id="92-0")
        observability_metrics.cost_dead_letter_depth.set(999)

        result = await discard_dlq(fake_redis, ids=[dead_id], reason="all refused", confirm=True)

        assert result.discarded == []
        assert result.refused
        assert observability_metrics.cost_dead_letter_depth._value.get() == 999
