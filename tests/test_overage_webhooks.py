"""Tests for T1.6 budget overage webhooks.

Covers:
- ``budget_watcher.check_and_fire_overages`` detects 50/80/100% crossings.
- Idempotency: re-running the watcher within the same period does NOT
  re-fire for an already-dispatched (budget, threshold) pair.
- HMAC signing matches the documented ``sha256=<hex>`` format.
- /api/webhooks router CRUD round-trip.
- ``no_webhooks`` outcome when a tenant crosses a threshold without any
  registered webhook (still records the dedupe row so the alert isn't
  spam-fired once a webhook *is* registered).
"""

import asyncio
import contextlib
import hashlib
import hmac
import json
import uuid
from datetime import UTC, datetime
from unittest.mock import AsyncMock

import httpx
import pytest
from sqlalchemy import select

from models.db import (
    BudgetOverageAlert,
    TenantWebhook,
    TokenBudget,
)
from services.budget_watcher import (
    LEGACY_TARGET,
    _period_start_for,
    _platform_webhook_from,
    _PlatformWebhook,
    check_and_fire_overages,
    sign_payload,
    target_key_for,
)
from tests._budget_factory import make_budget
from tests.conftest import T


def _new_budget(
    *, tenant_id: str, budget_tokens: int, consumed: int, period: str = "monthly"
) -> TokenBudget:
    return make_budget(
        tenant=tenant_id,
        cap=budget_tokens,
        consumed=consumed,
        period=period,
        source="manual",
    )


class TestSignPayload:
    @pytest.mark.unit
    def test_hmac_signature_matches_documented_format(self):
        secret = "topsecretkey1234567890"
        body = b'{"event":"budget.threshold_crossed"}'
        expected = "sha256=" + hmac.new(secret.encode(), body, hashlib.sha256).hexdigest()
        assert sign_payload(secret, body) == expected


class TestThresholdDetection:
    @pytest.mark.unit
    async def test_below_50_pct_no_alert(self, db_session_factory):
        async with db_session_factory() as session:
            session.add(_new_budget(tenant_id=T("t-low"), budget_tokens=1000, consumed=400))
            await session.commit()

        fired = await check_and_fire_overages(db_session_factory)
        assert fired == 0

        async with db_session_factory() as session:
            rows = (await session.execute(select(BudgetOverageAlert))).scalars().all()
            assert list(rows) == []

    @pytest.mark.unit
    async def test_50_pct_crossing_records_alert(self, db_session_factory):
        async with db_session_factory() as session:
            session.add(_new_budget(tenant_id=T("t-50"), budget_tokens=1000, consumed=600))
            await session.commit()

        fired = await check_and_fire_overages(db_session_factory)
        assert fired == 1

        async with db_session_factory() as session:
            rows = list(
                (
                    await session.execute(
                        select(BudgetOverageAlert).where(BudgetOverageAlert.tenant_id == T("t-50"))
                    )
                )
                .scalars()
                .all()
            )
            assert len(rows) == 1
            assert rows[0].threshold_pct == 50
            # No webhooks registered → outcome is no_webhooks.
            assert rows[0].outcome == "no_webhooks"

    @pytest.mark.unit
    async def test_at_100_pct_fires_all_three_thresholds(self, db_session_factory):
        async with db_session_factory() as session:
            session.add(_new_budget(tenant_id=T("t-100"), budget_tokens=1000, consumed=1000))
            await session.commit()

        fired = await check_and_fire_overages(db_session_factory)
        assert fired == 3

        async with db_session_factory() as session:
            rows = list(
                (
                    await session.execute(
                        select(BudgetOverageAlert).where(BudgetOverageAlert.tenant_id == T("t-100"))
                    )
                )
                .scalars()
                .all()
            )
            assert sorted(r.threshold_pct for r in rows) == [50, 80, 100]


class TestIdempotency:
    @pytest.mark.unit
    async def test_second_run_does_not_refire(self, db_session_factory):
        async with db_session_factory() as session:
            session.add(_new_budget(tenant_id=T("t-idem"), budget_tokens=1000, consumed=900))
            await session.commit()

        first = await check_and_fire_overages(db_session_factory)
        assert first == 2  # 50 + 80

        second = await check_and_fire_overages(db_session_factory)
        assert second == 0  # no new dispatches; UNIQUE constraint blocks dupes

        async with db_session_factory() as session:
            rows = list(
                (
                    await session.execute(
                        select(BudgetOverageAlert).where(
                            BudgetOverageAlert.tenant_id == T("t-idem")
                        )
                    )
                )
                .scalars()
                .all()
            )
            assert len(rows) == 2


class TestWebhookDelivery:
    @pytest.mark.unit
    async def test_webhook_called_with_signed_body(self, db_session_factory):
        captured = {}

        def _handler(request: httpx.Request) -> httpx.Response:
            captured["url"] = str(request.url)
            captured["body"] = request.content
            captured["sig"] = request.headers.get("X-Metering-Signature")
            captured["payload"] = json.loads(request.content)
            return httpx.Response(200, json={"ok": True})

        secret = "supersecretkey-with-enough-entropy"
        async with db_session_factory() as session:
            session.add(
                TenantWebhook(
                    tenant_id=T("t-hook"),
                    url="https://hook.test/notify",
                    secret=secret,
                    active=True,
                )
            )
            session.add(_new_budget(tenant_id=T("t-hook"), budget_tokens=1000, consumed=520))
            await session.commit()

        transport = httpx.MockTransport(_handler)
        async with httpx.AsyncClient(transport=transport, timeout=5.0) as client:
            fired = await check_and_fire_overages(db_session_factory, http_client=client)

        assert fired == 1
        assert captured["url"] == "https://hook.test/notify"
        assert captured["payload"]["event"] == "budget.threshold_crossed"
        assert captured["payload"]["threshold_pct"] == 50
        assert captured["payload"]["tenant_id"] == T("t-hook")
        # Signature must match HMAC-SHA256 of the body bytes.
        assert captured["sig"] == sign_payload(secret, captured["body"])

        async with db_session_factory() as session:
            row = (
                await session.execute(
                    select(BudgetOverageAlert).where(BudgetOverageAlert.tenant_id == T("t-hook"))
                )
            ).scalar_one()
            assert row.outcome == "ok"

    @pytest.mark.unit
    async def test_transient_failure_is_not_recorded_so_it_retries(self, db_session_factory):
        """A 5xx leaves NO dedupe row → the crossing re-fires next tick. This is
        what stops a crossing from being silently lost when the receiver is
        briefly down (and, paired with the pre-dispatch skip, stops the watcher
        spamming a webhook every tick once delivery succeeds)."""

        def _handler(_request: httpx.Request) -> httpx.Response:
            return httpx.Response(503, json={"error": "down"})

        async with db_session_factory() as session:
            session.add(
                TenantWebhook(
                    tenant_id=T("t-bad"),
                    url="https://hook.test/down",
                    secret="anothersecret-with-lots-of-bits",
                    active=True,
                )
            )
            session.add(_new_budget(tenant_id=T("t-bad"), budget_tokens=1000, consumed=520))
            await session.commit()

        transport = httpx.MockTransport(_handler)
        async with httpx.AsyncClient(transport=transport, timeout=5.0) as client:
            fired = await check_and_fire_overages(db_session_factory, http_client=client)

        # Nothing delivered → nothing recorded → the alert will be retried.
        assert fired == 0
        async with db_session_factory() as session:
            rows = list(
                (
                    await session.execute(
                        select(BudgetOverageAlert).where(BudgetOverageAlert.tenant_id == T("t-bad"))
                    )
                )
                .scalars()
                .all()
            )
            assert rows == []

    @pytest.mark.unit
    async def test_delivered_crossing_is_not_redispatched(self, db_session_factory):
        """The spam fix: once a crossing is delivered (row exists), a later sweep
        does NOT POST the webhook again."""
        calls = {"n": 0}

        def _handler(_request: httpx.Request) -> httpx.Response:
            calls["n"] += 1
            return httpx.Response(200, json={"ok": True})

        async with db_session_factory() as session:
            session.add(
                TenantWebhook(
                    tenant_id=T("t-once"),
                    url="https://hook.test/once",
                    secret="once-secret-with-enough-entropy",  # pragma: allowlist secret
                    active=True,
                )
            )
            # 60% → only the 50 threshold crosses (one dispatch expected).
            session.add(_new_budget(tenant_id=T("t-once"), budget_tokens=1000, consumed=600))
            await session.commit()

        transport = httpx.MockTransport(_handler)
        async with httpx.AsyncClient(transport=transport, timeout=5.0) as client:
            first = await check_and_fire_overages(db_session_factory, http_client=client)
            second = await check_and_fire_overages(db_session_factory, http_client=client)

        assert first == 1
        assert second == 0  # already delivered → skipped
        assert calls["n"] == 1  # the webhook was POSTed exactly once, not per-sweep


class TestPlatformSink:
    """The platform-wide sink fires on every tenant's crossing — including
    tenants with no registered tenant_webhooks rows — so budget alerts reach
    the notification hub for all tenants without per-tenant registration."""

    @pytest.mark.unit
    def test_platform_webhook_from_requires_both_url_and_secret(self, test_settings):
        test_settings.platform_budget_webhook_url = ""
        test_settings.platform_budget_webhook_secret = ""
        assert _platform_webhook_from(test_settings) is None

        # url without secret is inert (a signed request needs the key).
        test_settings.platform_budget_webhook_url = "https://fr.test/hook"
        test_settings.platform_budget_webhook_secret = ""
        assert _platform_webhook_from(test_settings) is None

        test_settings.platform_budget_webhook_secret = (
            "platform-secret-with-entropy"  # pragma: allowlist secret
        )
        wh = _platform_webhook_from(test_settings)
        assert wh is not None
        assert wh.url == "https://fr.test/hook"
        assert wh.secret == "platform-secret-with-entropy"  # pragma: allowlist secret

    @pytest.mark.unit
    async def test_platform_sink_fires_for_tenant_without_own_webhooks(self, db_session_factory):
        captured = {}

        def _handler(request: httpx.Request) -> httpx.Response:
            captured["url"] = str(request.url)
            captured["body"] = request.content
            captured["sig"] = request.headers.get("X-Metering-Signature")
            captured["payload"] = json.loads(request.content)
            return httpx.Response(200, json={"ok": True})

        secret = "platform-sink-secret-plenty-of-bits"  # pragma: allowlist secret
        platform = _PlatformWebhook(url="https://fr.test/budget-alert", secret=secret)

        async with db_session_factory() as session:
            # No TenantWebhook row for this tenant — only the platform sink.
            session.add(_new_budget(tenant_id=T("t-plat"), budget_tokens=1000, consumed=820))
            await session.commit()

        transport = httpx.MockTransport(_handler)
        async with httpx.AsyncClient(transport=transport, timeout=5.0) as client:
            fired = await check_and_fire_overages(
                db_session_factory, http_client=client, platform_webhook=platform
            )

        assert fired == 2  # 50 + 80
        assert captured["url"] == "https://fr.test/budget-alert"
        assert captured["payload"]["event"] == "budget.threshold_crossed"
        assert captured["payload"]["tenant_id"] == T("t-plat")
        # Signature must verify against the platform secret over the exact body.
        assert captured["sig"] == sign_payload(secret, captured["body"])

        async with db_session_factory() as session:
            rows = list(
                (
                    await session.execute(
                        select(BudgetOverageAlert).where(
                            BudgetOverageAlert.tenant_id == T("t-plat")
                        )
                    )
                )
                .scalars()
                .all()
            )
            assert {r.threshold_pct for r in rows} == {50, 80}
            assert all(r.outcome == "ok" for r in rows)

    @pytest.mark.unit
    async def test_platform_sink_fires_alongside_tenant_webhook(self, db_session_factory):
        hits: list[str] = []

        def _handler(request: httpx.Request) -> httpx.Response:
            hits.append(str(request.url))
            return httpx.Response(200, json={"ok": True})

        platform = _PlatformWebhook(
            url="https://fr.test/budget-alert",
            secret="platform-secret-abcdefg-1234567",  # pragma: allowlist secret
        )
        async with db_session_factory() as session:
            session.add(
                TenantWebhook(
                    tenant_id=T("t-both"),
                    url="https://tenant.test/own-hook",
                    secret="tenant-own-secret-with-entropy",  # pragma: allowlist secret
                    active=True,
                )
            )
            session.add(_new_budget(tenant_id=T("t-both"), budget_tokens=1000, consumed=520))
            await session.commit()

        transport = httpx.MockTransport(_handler)
        async with httpx.AsyncClient(transport=transport, timeout=5.0) as client:
            fired = await check_and_fire_overages(
                db_session_factory, http_client=client, platform_webhook=platform
            )

        # One 50% crossing × TWO destinations = two dedupe rows. Dedupe is
        # per-destination (0005): a single row per crossing used to be written as
        # soon as ANY destination 2xx'd, which silently dropped the crossing for
        # every other destination.
        assert fired == 2
        # BOTH the tenant's own hook and the platform sink received the crossing.
        assert "https://tenant.test/own-hook" in hits
        assert "https://fr.test/budget-alert" in hits


class TestWebhooksRouter:
    @pytest.mark.unit
    def test_webhook_crud_round_trip(self, app_client, monkeypatch):
        # require_tenant pulls the tenant from the X-Tenant-Id header set
        # by ext_authz; the test hits the FastAPI app directly so we need
        # to stub the dependency to avoid the auth bypass paths.
        from auth_deps import require_tenant
        from main import app

        def _fake_tenant() -> str:
            return T("t-router")

        app.dependency_overrides[require_tenant] = _fake_tenant
        try:
            r = app_client.post(
                "/api/webhooks",
                json={
                    "url": "https://hook.test/cb",
                    "secret": "router-test-secret-xxxxx",
                    "active": True,
                },
            )
            assert r.status_code == 201, r.text
            created = r.json()["data"]
            assert created["tenant_id"] == T("t-router")
            assert created["url"] == "https://hook.test/cb"
            assert "secret" not in created
            wid = created["id"]

            r = app_client.get("/api/webhooks")
            assert r.status_code == 200
            body = r.json()
            assert body["meta"]["total"] >= 1
            assert any(w["id"] == wid and w["active"] for w in body["data"])

            r = app_client.delete(f"/api/webhooks/{wid}")
            assert r.status_code == 204

            r = app_client.get("/api/webhooks")
            # Deactivated rows still show up in list (active=False) — that
            # matches the "soft delete" semantic in the router.
            assert r.status_code == 200
            assert all(not w["active"] for w in r.json()["data"] if w["id"] == wid)
        finally:
            app.dependency_overrides.pop(require_tenant, None)

    @pytest.mark.unit
    def test_webhook_create_rejects_non_http_url(self, app_client):
        from auth_deps import require_tenant
        from main import app

        app.dependency_overrides[require_tenant] = lambda: T("t-bad-url")
        try:
            r = app_client.post(
                "/api/webhooks",
                json={
                    "url": "ftp://hook.test/cb",
                    "secret": "secret-with-enough-entropy-here",
                },
            )
            assert r.status_code == 400
            assert "http" in r.json()["error"]["message"]
        finally:
            app.dependency_overrides.pop(require_tenant, None)

    @pytest.mark.unit
    def test_webhook_delete_unknown_returns_404(self, app_client):
        from auth_deps import require_tenant
        from main import app

        app.dependency_overrides[require_tenant] = lambda: T("t-404")
        try:
            r = app_client.delete(f"/api/webhooks/{uuid.uuid4()}")
            assert r.status_code == 404
        finally:
            app.dependency_overrides.pop(require_tenant, None)


# ===========================================================================
# Roll-window gap — the watcher's FOLLOWER path must retry on the SHORT interval
# ===========================================================================


class _WatcherNeverLeader:
    def __init__(self, *_args, **_kwargs):
        pass

    async def is_leader(self) -> bool:
        return False

    async def release(self) -> None:
        return None


class _WatcherAlwaysLeader(_WatcherNeverLeader):
    async def is_leader(self) -> bool:
        return True


def _record_wait_timeouts(monkeypatch) -> list[float]:
    """Record every ``timeout=`` the watcher loop hands to ``asyncio.wait_for``."""
    timeouts: list[float] = []
    real_wait_for = asyncio.wait_for

    async def recording_wait_for(aw, timeout=None):  # noqa: ASYNC109 — wraps wait_for
        assert timeout is not None, "the loop must always bound its wait"
        timeouts.append(timeout)
        return await real_wait_for(aw, timeout)

    monkeypatch.setattr(asyncio, "wait_for", recording_wait_for)
    return timeouts


async def _drive_watcher(settings, session_factory, monkeypatch, election, want_waits: int):
    """Run ``run_watcher_loop`` until it has waited ``want_waits`` times, then stop."""
    from services import budget_watcher as bw

    timeouts = _record_wait_timeouts(monkeypatch)
    monkeypatch.setattr("services.leader_election.AsyncLeaderElection", election)
    monkeypatch.setattr(bw, "check_and_fire_overages", AsyncMock(return_value=0))

    stop_event = asyncio.Event()
    task = asyncio.create_task(bw.run_watcher_loop(settings, session_factory, stop_event))
    deadline = asyncio.get_event_loop().time() + 5.0
    while asyncio.get_event_loop().time() < deadline:
        if len(timeouts) >= want_waits:
            break
        await asyncio.sleep(0.01)
    stop_event.set()
    task.cancel()
    with contextlib.suppress(asyncio.CancelledError):
        await task
    return timeouts


class TestWatcherLeaderRetryWindow:
    """Same roll-window fix as the rollup loop (see tests/test_aggregator.py).

    ``budget_watcher_interval_seconds`` stays at the production 60 so that a loop
    that wrongly waits it on the follower path blows the 5s budget and FAILS,
    rather than passing by coincidence.
    """

    @pytest.mark.unit
    async def test_follower_retries_on_the_short_interval(
        self, test_settings, db_session_factory, monkeypatch
    ):
        test_settings.budget_watcher_interval_seconds = 60
        test_settings.leader_retry_interval_seconds = 0.05

        timeouts = await _drive_watcher(
            test_settings, db_session_factory, monkeypatch, _WatcherNeverLeader, want_waits=3
        )

        assert len(timeouts) >= 3, f"follower did not re-poll for leadership: {timeouts}"
        assert all(t == 0.05 for t in timeouts), timeouts
        assert 60 not in timeouts

    @pytest.mark.unit
    async def test_leader_keeps_the_full_watcher_interval(
        self, test_settings, db_session_factory, monkeypatch
    ):
        test_settings.budget_watcher_interval_seconds = 60
        test_settings.leader_retry_interval_seconds = 0.05

        timeouts = await _drive_watcher(
            test_settings, db_session_factory, monkeypatch, _WatcherAlwaysLeader, want_waits=1
        )

        assert timeouts[:1] == [60]

    @pytest.mark.unit
    async def test_stop_event_cuts_the_follower_wait_immediately(
        self, test_settings, db_session_factory, monkeypatch
    ):
        """No shutdown regression: the 15s follower wait is interruptible."""
        from services import budget_watcher as bw

        test_settings.budget_watcher_interval_seconds = 60
        test_settings.leader_retry_interval_seconds = 15.0
        monkeypatch.setattr("services.leader_election.AsyncLeaderElection", _WatcherNeverLeader)

        stop_event = asyncio.Event()
        task = asyncio.create_task(
            bw.run_watcher_loop(test_settings, db_session_factory, stop_event)
        )
        await asyncio.sleep(0.05)  # let the loop park in its wait
        loop = asyncio.get_event_loop()
        began = loop.time()
        stop_event.set()
        await asyncio.wait_for(task, timeout=5.0)  # no cancel() — must exit on its own
        assert loop.time() - began < 1.0


class TestWatcherResilience:
    """A single bad destination or a single bad sweep must not stop the watcher.

    Before these fixes the watcher loop caught only ``SQLAlchemyError``, so any
    other exception escaped the ``while``, ran ``finally: election.release()``
    and left budget alerting dead FLEET-WIDE until the pod restarted — with the
    follower re-hitting the same error on promotion.
    """

    @pytest.mark.unit
    async def test_unpostable_stored_url_does_not_sink_the_sweep(self, db_session_factory):
        """A stored url of "https://" (no host) makes httpx raise a bare
        ValueError — NOT an httpx.HTTPError. It must degrade to one failed
        destination, and the platform sink must still be delivered."""
        hits: list[str] = []

        def _handler(request: httpx.Request) -> httpx.Response:
            hits.append(str(request.url))
            return httpx.Response(200, json={"ok": True})

        platform = _PlatformWebhook(
            url="https://fr.test/budget-alert",
            secret="platform-secret-abcdefg-1234567",  # pragma: allowlist secret
        )
        async with db_session_factory() as session:
            # Persisted directly: it predates the router's host check, exactly
            # like a row already sitting in prod.
            session.add(
                TenantWebhook(
                    tenant_id=T("t-badurl"),
                    url="https://",
                    secret="tenant-own-secret-with-entropy",  # pragma: allowlist secret
                    active=True,
                )
            )
            session.add(_new_budget(tenant_id=T("t-badurl"), budget_tokens=1000, consumed=520))
            await session.commit()

        transport = httpx.MockTransport(_handler)
        async with httpx.AsyncClient(transport=transport, timeout=5.0) as client:
            # Must NOT raise.
            fired = await check_and_fire_overages(
                db_session_factory, http_client=client, platform_webhook=platform
            )

        assert "https://fr.test/budget-alert" in hits, "sink lost to a sibling's bad URL"
        assert fired == 1, "only the reachable destination should be recorded"

        async with db_session_factory() as session:
            rows = list(
                (
                    await session.execute(
                        select(BudgetOverageAlert).where(
                            BudgetOverageAlert.tenant_id == T("t-badurl")
                        )
                    )
                )
                .scalars()
                .all()
            )
        # Exactly one row: the platform sink. The broken destination stays
        # unrecorded so it is retried, never marked delivered.
        assert len(rows) == 1
        assert rows[0].target_key == target_key_for("https://fr.test/budget-alert")

    @pytest.mark.unit
    async def test_a_raising_sweep_does_not_kill_the_loop(
        self, test_settings, db_session_factory, monkeypatch
    ):
        """A non-SQLAlchemy exception costs one cycle, not the watcher."""
        from services import budget_watcher as bw

        test_settings.budget_watcher_interval_seconds = 0.05
        test_settings.leader_retry_interval_seconds = 0.05

        calls = {"n": 0}

        async def _boom(*_args, **_kwargs):
            calls["n"] += 1
            raise ValueError("unknown url type: '/'")

        timeouts = _record_wait_timeouts(monkeypatch)
        monkeypatch.setattr("services.leader_election.AsyncLeaderElection", _WatcherAlwaysLeader)
        monkeypatch.setattr(bw, "check_and_fire_overages", _boom)

        stop_event = asyncio.Event()
        task = asyncio.create_task(
            bw.run_watcher_loop(test_settings, db_session_factory, stop_event)
        )
        deadline = asyncio.get_event_loop().time() + 5.0
        while asyncio.get_event_loop().time() < deadline:
            if calls["n"] >= 3:
                break
            await asyncio.sleep(0.01)
        stop_event.set()
        task.cancel()
        with contextlib.suppress(asyncio.CancelledError):
            await task

        # The loop survived a raising sweep and kept re-running it.
        assert calls["n"] >= 3, f"loop died after {calls['n']} sweep(s)"
        assert len(timeouts) >= 2, "loop stopped waiting → it exited the while"


class TestPerDestinationDedupe:
    @pytest.mark.unit
    async def test_failed_destination_retries_without_respamming_the_healthy_one(
        self, db_session_factory
    ):
        """The bug this fixes: one dedupe row covered ALL destinations and was
        written as soon as ANY 2xx'd, so a sink that timed out never got the
        crossing. The inverse naive fix (record only when ALL succeed) would
        re-POST the healthy destination every tick — check both properties."""
        hits: list[str] = []
        tenant_status = {"code": 500}

        def _handler(request: httpx.Request) -> httpx.Response:
            url = str(request.url)
            hits.append(url)
            if url == "https://tenant.test/own-hook":
                return httpx.Response(tenant_status["code"], json={"nope": True})
            return httpx.Response(200, json={"ok": True})

        platform = _PlatformWebhook(
            url="https://fr.test/budget-alert",
            secret="platform-secret-abcdefg-1234567",  # pragma: allowlist secret
        )
        async with db_session_factory() as session:
            session.add(
                TenantWebhook(
                    tenant_id=T("t-retry"),
                    url="https://tenant.test/own-hook",
                    secret="tenant-own-secret-with-entropy",  # pragma: allowlist secret
                    active=True,
                )
            )
            session.add(_new_budget(tenant_id=T("t-retry"), budget_tokens=1000, consumed=520))
            await session.commit()

        transport = httpx.MockTransport(_handler)
        async with httpx.AsyncClient(transport=transport, timeout=5.0) as client:
            await check_and_fire_overages(
                db_session_factory, http_client=client, platform_webhook=platform
            )
            assert sorted(hits) == [
                "https://fr.test/budget-alert",
                "https://tenant.test/own-hook",
            ]

            # Second sweep: the sink already 2xx'd, the tenant hook did not.
            hits.clear()
            tenant_status["code"] = 200
            fired = await check_and_fire_overages(
                db_session_factory, http_client=client, platform_webhook=platform
            )

        assert hits == ["https://tenant.test/own-hook"], (
            "only the failed destination may be retried"
        )
        assert fired == 1

        async with db_session_factory() as session:
            rows = list(
                (
                    await session.execute(
                        select(BudgetOverageAlert).where(
                            BudgetOverageAlert.tenant_id == T("t-retry")
                        )
                    )
                )
                .scalars()
                .all()
            )
        assert {r.target_key for r in rows} == {
            target_key_for("https://fr.test/budget-alert"),
            target_key_for("https://tenant.test/own-hook"),
        }

    @pytest.mark.unit
    async def test_legacy_row_suppresses_the_whole_crossing(self, db_session_factory):
        """A pre-0005 row (target_key='') means "crossing already handled".
        Introducing per-destination dedupe must NOT replay history."""
        hits: list[str] = []

        def _handler(request: httpx.Request) -> httpx.Response:
            hits.append(str(request.url))
            return httpx.Response(200, json={"ok": True})

        platform = _PlatformWebhook(
            url="https://fr.test/budget-alert",
            secret="platform-secret-abcdefg-1234567",  # pragma: allowlist secret
        )
        async with db_session_factory() as session:
            budget = _new_budget(tenant_id=T("t-legacy"), budget_tokens=1000, consumed=520)
            session.add(budget)
            await session.flush()
            period_start = _period_start_for(budget, datetime.now(UTC))
            for threshold in (50,):
                session.add(
                    BudgetOverageAlert(
                        tenant_id=T("t-legacy"),
                        budget_id=budget.id,
                        threshold_pct=threshold,
                        period_start=period_start,
                        outcome="no_webhooks",
                        target_key=LEGACY_TARGET,
                    )
                )
            await session.commit()

        transport = httpx.MockTransport(_handler)
        async with httpx.AsyncClient(transport=transport, timeout=5.0) as client:
            fired = await check_and_fire_overages(
                db_session_factory, http_client=client, platform_webhook=platform
            )

        assert hits == [], "legacy dedupe row must suppress replay of a past crossing"
        assert fired == 0


class TestWebhookUrlValidation:
    @pytest.mark.unit
    def test_webhook_create_rejects_hostless_url(self, app_client):
        """ "https://" passes a naive startswith() scheme check but makes httpx
        raise a bare ValueError at dispatch time — reject it at the boundary."""
        from auth_deps import require_tenant
        from main import app

        app.dependency_overrides[require_tenant] = lambda: T("t-hostless")
        try:
            r = app_client.post(
                "/api/webhooks",
                json={
                    "url": "https://",
                    "secret": "secret-with-enough-entropy-here",  # pragma: allowlist secret
                },
            )
            assert r.status_code == 400
            assert "host" in r.json()["error"]["message"]
        finally:
            app.dependency_overrides.pop(require_tenant, None)
