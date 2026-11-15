"""Tests for POST /api/admin/dlq/reattribute + services.dlq_ops.reattribute_dlq (#243).

THE DONE-CRITERION: a ``nil_tenant_id`` entry with nonzero cost is
re-attributed and billed EXACTLY ONCE, and an entry whose stored tenant is not
nil/blank is refused. "Billed" is proven at the ledger, not at the response: the
re-injected ``billing:costs`` messages are drained through the real
``services.cost_flusher.flush_costs`` and the ``billing_cost_events`` rows are
counted. A response that says ``reattributed`` while the flusher parks or
re-quarantines the event would pass every response-level assertion.

Fixtures mirror ``tests/routers/test_admin_dlq_discard.py`` (same bearer stub,
same ``redis_client._redis`` injection).
"""

from __future__ import annotations

import uuid
from decimal import Decimal
from typing import TYPE_CHECKING, cast

import fakeredis.aioredis
import pytest
import pytest_asyncio
from sqlalchemy import func, select

import auth_deps
from models.db import AuditLog, CostEvent, DlqReattribution
from services.cost_flusher import flush_costs
from services.dlq_ops import ReattributeTargetError, reattribute_dlq
from tenancy import NIL_TENANT_ID
from tests.conftest import T

if TYPE_CHECKING:
    from sqlalchemy.ext.asyncio import AsyncSession

DEAD_STREAM = "billing:costs:dead"
COSTS_STREAM = "billing:costs"
ROUTE = "/api/admin/dlq/reattribute"
REASON = "tester spend from fr-test onboarding run, see #243"


@pytest.fixture
def admin_actor(monkeypatch):
    """license-admin bearer (matches the replay/discard tests)."""
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
    cost_usd: str | None = "0.4200",
    provider: str | None = "claude",
    **extra: str,
) -> str:
    """One dead-letter entry in cost_flusher's on-wire shape. Defaults are the
    #243 population: nil tenant, nonzero cost, quarantined ``nil_tenant_id``."""
    fields = {
        "model": "sonnet",
        "input_tokens": "100",
        "output_tokens": "50",
        "service_key": "inference-router",  # pragma: allowlist secret
        "license_id": str(uuid.uuid4()),
        "cost_source": "test",
        "_dlq_reason": reason,
        "_dlq_original_id": original_id,
        "_dlq_at": "2026-09-02T00:00:00+00:00",
    }
    if provider is not None:
        fields["provider"] = provider
    if tenant_id is not None:
        fields["tenant_id"] = tenant_id
    if cost_usd is not None:
        fields["cost_usd"] = cost_usd
    fields.update(extra)
    return await redis.xadd(DEAD_STREAM, fields)


async def _ids(redis, stream: str) -> list[str]:
    return [entry_id for entry_id, _ in (await redis.xrange(stream) or [])]


async def _drain_costs_into_ledger(redis, session_factory) -> int:
    """Feed every billing:costs message through the REAL flusher, the way the
    consumer stages it (``_msg_id`` / ``_stream_name``)."""
    buffer = []
    for msg_id, fields in await redis.xrange(COSTS_STREAM) or []:
        staged = dict(fields)
        staged["_msg_id"] = msg_id
        staged["_stream_name"] = COSTS_STREAM
        buffer.append(staged)
    if not buffer:
        return 0
    return await flush_costs(buffer, session_factory, redis, dead_letter_stream=DEAD_STREAM)


async def _ledger_rows(session_factory, tenant_id: str) -> list[CostEvent]:
    async with session_factory() as s:
        return list(
            (await s.execute(select(CostEvent).where(CostEvent.tenant_id == tenant_id)))
            .scalars()
            .all()
        )


async def _receipt_count(session_factory) -> int:
    async with session_factory() as s:
        return (await s.execute(select(func.count()).select_from(DlqReattribution))).scalar_one()


@pytest.mark.unit
class TestReattributeBillsExactlyOnce:
    async def test_nil_tenant_nonzero_cost_is_billed_exactly_once(
        self, dlq_env, admin_actor, db_session_factory
    ) -> None:
        """THE done-criterion. Confirm twice, then confirm a re-quarantined TWIN
        of the same event (same ``_dlq_original_id``, new dead id — the crash
        window ``_replay_marker`` exists for). Drain everything through the real
        flusher: exactly one ledger row, on the target, not excluded."""
        client, fake = dlq_env
        target = T("reattribute-target")
        dead_id = await _seed_dead(fake, original_id="77-0")

        body = {"ids": [dead_id], "tenant_id": target, "reason": REASON, "confirm": True}
        first = client.post(ROUTE, json=body)
        assert first.status_code == 200, first.text
        data = first.json()["data"]
        assert data["tenant_id"] == target
        assert data["reattributed"] == [dead_id]
        assert data["deleted_from_dlq"] == [dead_id]
        assert data["refused"] == []
        assert data["receipt_failed"] == []
        assert await _ids(fake, DEAD_STREAM) == []

        # Second identical confirm: the entry is gone, nothing is examined.
        second = client.post(ROUTE, json=body)
        assert second.status_code == 200, second.text
        assert second.json()["data"]["examined"] == 0
        assert second.json()["data"]["reattributed"] == []

        # A twin of the SAME event under a new dead id: the marker refuses it.
        twin_id = await _seed_dead(fake, original_id="77-0")
        third = client.post(ROUTE, json={**body, "ids": [twin_id]})
        assert third.status_code == 200, third.text
        assert third.json()["data"]["already_processed"] == [twin_id]
        assert third.json()["data"]["reattributed"] == []

        # Exactly one message reached the live stream ...
        assert len(await _ids(fake, COSTS_STREAM)) == 1
        # ... and exactly one billable ledger row came out of it.
        assert await _drain_costs_into_ledger(fake, db_session_factory) == 1
        rows = await _ledger_rows(db_session_factory, target)
        assert len(rows) == 1
        assert rows[0].cost_usd_hint == Decimal("0.42")
        assert rows[0].excluded_from_billing is False
        # The nil tenant received nothing.
        assert await _ledger_rows(db_session_factory, NIL_TENANT_ID) == []

        # One receipt, carrying the full original record and the provenance.
        async with db_session_factory() as s:
            receipts = list((await s.execute(select(DlqReattribution))).scalars().all())
        assert len(receipts) == 1
        r = receipts[0]
        assert r.dead_entry_id == dead_id
        assert r.original_id == "77-0"
        assert r.replay_marker == "dlq:replayed:orig:77-0"
        assert r.reattributed_from == NIL_TENANT_ID
        assert r.tenant_id == target
        assert r.reattributed_by == "test-admin-user"
        assert r.reason == REASON
        assert r.original_fields["tenant_id"] == NIL_TENANT_ID
        assert r.original_fields["_dlq_reason"] == "nil_tenant_id"
        assert r.reinjected_stream_id == (await _ids(fake, COSTS_STREAM))[0]

        # The re-injected event carries its provenance on the wire.
        (_, live_fields) = (await fake.xrange(COSTS_STREAM))[0]
        assert live_fields["tenant_id"] == target
        assert live_fields["reattributed_from"] == NIL_TENANT_ID
        assert live_fields["reattributed_by"] == "test-admin-user"
        assert live_fields["reattribution_reason"] == REASON
        assert not any(k.startswith("_dlq") for k in live_fields)

    async def test_receipt_failure_keeps_entry_and_still_bills_once(
        self, fake_redis, db_session_factory
    ) -> None:
        """If the receipt cannot be committed the entry is NOT XDELed — and the
        surviving entry cannot be billed a second time by a re-run, because the
        marker was made permanent in the same script that injected."""

        class _FailingSession:
            def add(self, _obj) -> None:
                pass

            async def commit(self) -> None:
                raise RuntimeError("postgres down")

            async def rollback(self) -> None:
                pass

        target = T("reattribute-target")
        dead_id = await _seed_dead(fake_redis, original_id="78-0")

        result = await reattribute_dlq(
            fake_redis,
            cast("AsyncSession", _FailingSession()),
            ids=[dead_id],
            tenant_id=target,
            reason=REASON,
            reattributed_by="op",
            confirm=True,
        )
        assert result.reattributed == [dead_id]
        assert result.receipt_failed == [dead_id]
        assert result.deleted_from_dlq == []
        assert await _ids(fake_redis, DEAD_STREAM) == [dead_id]
        assert await fake_redis.ttl("dlq:replayed:orig:78-0") == -1  # permanent

        async with db_session_factory() as session:
            rerun = await reattribute_dlq(
                fake_redis,
                session,
                ids=[dead_id],
                tenant_id=target,
                reason=REASON,
                reattributed_by="op",
                confirm=True,
            )
        assert rerun.already_processed == [dead_id]
        assert rerun.reattributed == []
        assert len(await _ids(fake_redis, COSTS_STREAM)) == 1
        assert await _drain_costs_into_ledger(fake_redis, db_session_factory) == 1
        assert len(await _ledger_rows(db_session_factory, target)) == 1

    async def test_blank_stored_tenant_is_eligible(self, fake_redis, db_session_factory) -> None:
        """The other unattributable shape — no tenant_id field at all. The
        receipt records the source as NULL, not as a fabricated value."""
        target = T("reattribute-target")
        dead_id = await _seed_dead(
            fake_redis, tenant_id=None, reason="missing_tenant_id", original_id="79-0"
        )
        async with db_session_factory() as session:
            result = await reattribute_dlq(
                fake_redis,
                session,
                ids=[dead_id],
                tenant_id=target,
                reason=REASON,
                reattributed_by="op",
                confirm=True,
            )
        assert result.reattributed == [dead_id]
        async with db_session_factory() as s:
            receipt = (await s.execute(select(DlqReattribution))).scalar_one()
        assert receipt.reattributed_from is None
        (_, live_fields) = (await fake_redis.xrange(COSTS_STREAM))[0]
        assert live_fields["reattributed_from"] == ""


@pytest.mark.unit
class TestReattributeRefusesStoredTenant:
    async def test_non_nil_entry_is_refused(self, dlq_env, admin_actor, db_session_factory) -> None:
        """Done-criterion part two: an entry that already names a real tenant is
        refused even under confirm, stays on the stream, and nothing is injected."""
        client, fake = dlq_env
        dead_id = await _seed_dead(fake, tenant_id=T("tenant-a"), reason="missing_provider")

        resp = client.post(
            ROUTE,
            json={
                "ids": [dead_id],
                "tenant_id": T("reattribute-target"),
                "reason": REASON,
                "confirm": True,
            },
        )
        assert resp.status_code == 200, resp.text
        data = resp.json()["data"]
        assert data["refused"] == [{"id": dead_id, "reason": "tenant_attributable"}]
        assert data["reattributed"] == []
        assert await _ids(fake, DEAD_STREAM) == [dead_id]
        assert await _ids(fake, COSTS_STREAM) == []
        assert await fake.exists("dlq:replayed:orig:10-1") == 0
        assert await _receipt_count(db_session_factory) == 0

    async def test_unresolvable_stored_tenant_is_refused(self, fake_redis, db_session) -> None:
        """A stored slug that is merely unregistered keeps a recoverable
        attribution, so the operator may not override it."""
        dead_id = await _seed_dead(fake_redis, tenant_id="some-unregistered-slug")
        result = await reattribute_dlq(
            fake_redis,
            db_session,
            ids=[dead_id],
            tenant_id=T("reattribute-target"),
            reason=REASON,
            reattributed_by="op",
            confirm=True,
        )
        assert result.refused == [{"id": dead_id, "reason": "tenant_unresolved"}]
        assert await _ids(fake_redis, DEAD_STREAM) == [dead_id]
        assert await _ids(fake_redis, COSTS_STREAM) == []

    async def test_rewritten_payload_still_malformed_is_not_injected(
        self, fake_redis, db_session
    ) -> None:
        """Nil tenant AND no provider: re-attribution fixes only the tenant, so
        the rewritten event still fails validation and is left quarantined with
        no marker claimed."""
        dead_id = await _seed_dead(fake_redis, provider=None, original_id="80-0")
        result = await reattribute_dlq(
            fake_redis,
            db_session,
            ids=[dead_id],
            tenant_id=T("reattribute-target"),
            reason=REASON,
            reattributed_by="op",
            confirm=True,
        )
        assert result.still_malformed == [{"id": dead_id, "reason": "missing_provider"}]
        assert await _ids(fake_redis, DEAD_STREAM) == [dead_id]
        assert await _ids(fake_redis, COSTS_STREAM) == []
        assert await fake_redis.exists("dlq:replayed:orig:80-0") == 0


@pytest.mark.unit
class TestReattributeDryRun:
    async def test_default_call_changes_nothing(
        self, dlq_env, admin_actor, db_session, db_session_factory
    ) -> None:
        """No ``confirm`` in the body: eligible is REPORTED, and the stream, the
        live stream, the marker namespace and the receipt table are untouched.
        One audit row records that someone looked."""
        client, fake = dlq_env
        dead_id = await _seed_dead(fake, original_id="81-0")

        resp = client.post(
            ROUTE,
            json={"ids": [dead_id], "tenant_id": T("reattribute-target"), "reason": REASON},
        )
        assert resp.status_code == 200, resp.text
        data = resp.json()["data"]
        assert data["examined"] == 1
        assert data["eligible"] == [dead_id]
        assert data["reattributed"] == []
        assert data["deleted_from_dlq"] == []
        assert await _ids(fake, DEAD_STREAM) == [dead_id]
        assert await _ids(fake, COSTS_STREAM) == []
        assert await fake.keys("dlq:replayed:*") == []
        assert await _receipt_count(db_session_factory) == 0

        audits = list(
            (
                await db_session.execute(
                    select(AuditLog).where(AuditLog.action == "admin.dlq_reattribute")
                )
            )
            .scalars()
            .all()
        )
        assert len(audits) == 1
        assert audits[0].before["confirm"] is False
        assert audits[0].after["reattributed"] == []


@pytest.mark.unit
class TestReattributeRequestValidation:
    @pytest.mark.parametrize(
        ("target", "code"),
        [
            (NIL_TENANT_ID, "tenant_id_nil"),
            ("not-a-registered-name", "tenant_id_unresolvable"),
            (str(uuid.uuid4()), "tenant_id_unregistered"),
        ],
    )
    async def test_bad_target_is_400_and_touches_nothing(
        self, dlq_env, admin_actor, target, code
    ) -> None:
        client, fake = dlq_env
        dead_id = await _seed_dead(fake)
        resp = client.post(
            ROUTE,
            json={"ids": [dead_id], "tenant_id": target, "reason": REASON, "confirm": True},
        )
        assert resp.status_code == 400, resp.text
        assert code in resp.text
        assert await _ids(fake, DEAD_STREAM) == [dead_id]
        assert await _ids(fake, COSTS_STREAM) == []

    async def test_service_refuses_nil_target(self, fake_redis, db_session) -> None:
        dead_id = await _seed_dead(fake_redis)
        with pytest.raises(ReattributeTargetError, match="tenant_id_nil"):
            await reattribute_dlq(
                fake_redis,
                db_session,
                ids=[dead_id],
                tenant_id=NIL_TENANT_ID,
                reason=REASON,
                reattributed_by="op",
                confirm=True,
            )
        assert await _ids(fake_redis, DEAD_STREAM) == [dead_id]

    async def test_blank_reason_is_rejected(self, dlq_env, admin_actor) -> None:
        client, fake = dlq_env
        dead_id = await _seed_dead(fake)
        resp = client.post(
            ROUTE,
            json={
                "ids": [dead_id],
                "tenant_id": T("reattribute-target"),
                "reason": "   ",
                "confirm": True,
            },
        )
        assert resp.status_code == 422, resp.text
        assert await _ids(fake, DEAD_STREAM) == [dead_id]
        assert await _ids(fake, COSTS_STREAM) == []

    async def test_service_refuses_blank_reason(self, fake_redis, db_session) -> None:
        dead_id = await _seed_dead(fake_redis)
        with pytest.raises(ValueError, match="reason"):
            await reattribute_dlq(
                fake_redis,
                db_session,
                ids=[dead_id],
                tenant_id=T("reattribute-target"),
                reason=" ",
                reattributed_by="op",
                confirm=True,
            )
        assert await _ids(fake_redis, COSTS_STREAM) == []

    async def test_billing_internal_role_is_refused(self, dlq_env, monkeypatch) -> None:
        """require_admin, not require_pricing_admin: the billing-internal SA may
        replay and discard, but may not choose which customer pays."""
        import auth

        async def _fake_decode(_request):
            return {"sub": "billing-sa", "realm_access": {"roles": ["billing-internal"]}}

        monkeypatch.setattr(auth, "DASHBOARD_AUTH_ENABLED", True)
        monkeypatch.setattr(auth, "_decode_bearer_token", _fake_decode)
        monkeypatch.setattr(auth_deps, "_decode_bearer_token", _fake_decode)

        client, fake = dlq_env
        dead_id = await _seed_dead(fake)
        resp = client.post(
            ROUTE,
            json={
                "ids": [dead_id],
                "tenant_id": T("reattribute-target"),
                "reason": REASON,
                "confirm": True,
            },
        )
        assert resp.status_code == 403, resp.text
        assert await _ids(fake, DEAD_STREAM) == [dead_id]
