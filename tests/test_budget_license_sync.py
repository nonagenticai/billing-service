"""Tests for the license-sync idempotent upsert path.

The license-sync code path must NOT stack new budgets every time the same
license re-syncs — otherwise every periodic sync cycle compounds budgets
without bound. The constraint is enforced in code (not the DB) via
upsert_license_sync_budget; manual/topup budgets continue to stack as
designed by the cascade-drain rewrite.
"""

from __future__ import annotations

import uuid
from unittest.mock import AsyncMock, MagicMock, patch

import httpx
import pytest
from sqlalchemy import select

from models.db import TokenBudget
from models.domain import BudgetPeriod
from services.budget import (
    create_budget,
    deactivate_orphan_license_sync_budgets,
    debit_budget,
    upsert_license_sync_budget,
)
from tests.conftest import T

# ---------------------------------------------------------------------------
# Direct service-layer tests
# ---------------------------------------------------------------------------


class TestUpsertLicenseSyncBudget:
    @pytest.mark.unit
    async def test_first_sync_creates_row(self, db_session):
        """First call inserts a new license_sync budget."""
        lic_id = uuid.uuid4()
        budget, created = await upsert_license_sync_budget(
            db_session,
            tenant_id=T("t1"),
            license_id=str(lic_id),
            period=BudgetPeriod.monthly,
            budget_tokens=10_000,
        )
        assert created is True
        assert budget.tenant_id == T("t1")
        assert budget.license_id == lic_id
        assert budget.budget_tokens == 10_000
        assert budget.period == "monthly"
        assert budget.source == "license_sync"
        assert int(budget.consumed_tokens) == 0
        assert budget.tenant_name == T("t1")

        rows = (await db_session.execute(select(TokenBudget))).scalars().all()
        assert len(list(rows)) == 1

    @pytest.mark.unit
    async def test_second_sync_same_plan_no_duplicate(self, db_session):
        """Re-syncing with identical plan returns the same row, no new insert."""
        lic_id = uuid.uuid4()
        b1, c1 = await upsert_license_sync_budget(
            db_session,
            tenant_id=T("t1"),
            license_id=str(lic_id),
            period=BudgetPeriod.monthly,
            budget_tokens=10_000,
        )
        b2, c2 = await upsert_license_sync_budget(
            db_session,
            tenant_id=T("t1"),
            license_id=str(lic_id),
            period=BudgetPeriod.monthly,
            budget_tokens=10_000,
        )
        assert c1 is True
        assert c2 is False
        assert b1.id == b2.id

        rows = list((await db_session.execute(select(TokenBudget))).scalars().all())
        assert len(rows) == 1

    @pytest.mark.unit
    async def test_resync_with_changed_cap_updates_in_place(self, db_session):
        """Plan change reconciles in place: same row id, updated budget_tokens,
        consumed_tokens preserved."""
        lic_id = uuid.uuid4()
        b1, _ = await upsert_license_sync_budget(
            db_session,
            tenant_id=T("t1"),
            license_id=str(lic_id),
            period=BudgetPeriod.monthly,
            budget_tokens=10_000,
        )
        original_id = b1.id
        original_consumed = int(b1.consumed_tokens)

        b2, created = await upsert_license_sync_budget(
            db_session,
            tenant_id=T("t1"),
            license_id=str(lic_id),
            period=BudgetPeriod.monthly,
            budget_tokens=25_000,
        )
        assert created is False
        assert b2.id == original_id
        assert b2.budget_tokens == 25_000
        assert int(b2.consumed_tokens) == original_consumed

        rows = list((await db_session.execute(select(TokenBudget))).scalars().all())
        assert len(rows) == 1

    @pytest.mark.unit
    async def test_resync_preserves_consumed_tokens_after_debit(self, db_session):
        """After a real debit, re-syncing with a doubled cap preserves consumed_tokens."""
        lic_id = uuid.uuid4()
        b1, _ = await upsert_license_sync_budget(
            db_session,
            tenant_id=T("t1"),
            license_id=str(lic_id),
            period=BudgetPeriod.monthly,
            budget_tokens=10_000,
        )
        await db_session.commit()

        debits = await debit_budget(db_session, None, T("t1"), 100)
        assert len(debits) == 1
        assert debits[0].debited_tokens == 100

        # Re-sync with a doubled cap — same row, consumed preserved, cap doubled.
        b2, created = await upsert_license_sync_budget(
            db_session,
            tenant_id=T("t1"),
            license_id=str(lic_id),
            period=BudgetPeriod.monthly,
            budget_tokens=20_000,
        )
        assert created is False
        assert b2.id == b1.id
        assert b2.budget_tokens == 20_000
        assert int(b2.consumed_tokens) == 100

        rows = list((await db_session.execute(select(TokenBudget))).scalars().all())
        assert len(rows) == 1

    @pytest.mark.unit
    async def test_manual_budget_untouched_by_sync(self, db_session):
        """A manual budget for the same tenant+license+period coexists with the
        license_sync row; sync only reconciles the license_sync row."""
        lic_id = uuid.uuid4()
        manual = await create_budget(
            tenant_id=T("t1"),
            budget_tokens=99_999,
            period="monthly",
            license_id=lic_id,
            source="manual",
            session=db_session,
        )
        manual_id = manual.id

        synced, created = await upsert_license_sync_budget(
            db_session,
            tenant_id=T("t1"),
            license_id=str(lic_id),
            period=BudgetPeriod.monthly,
            budget_tokens=10_000,
        )
        assert created is True
        assert synced.id != manual_id

        # Re-sync — license_sync row reconciled, manual row untouched.
        synced2, created2 = await upsert_license_sync_budget(
            db_session,
            tenant_id=T("t1"),
            license_id=str(lic_id),
            period=BudgetPeriod.monthly,
            budget_tokens=15_000,
        )
        assert created2 is False
        assert synced2.id == synced.id
        assert synced2.budget_tokens == 15_000

        # Manual row still at 99_999, source still 'manual'.
        await db_session.refresh(manual)
        assert manual.budget_tokens == 99_999
        assert manual.source == "manual"
        assert manual.active is True

        rows = list((await db_session.execute(select(TokenBudget))).scalars().all())
        assert len(rows) == 2

    @pytest.mark.unit
    async def test_different_periods_are_separate_rows(self, db_session):
        """Monthly and weekly license_sync budgets for the same license are independent."""
        lic_id = uuid.uuid4()
        b_m, _ = await upsert_license_sync_budget(
            db_session,
            tenant_id=T("t1"),
            license_id=str(lic_id),
            period=BudgetPeriod.monthly,
            budget_tokens=10_000,
        )
        b_w, created_w = await upsert_license_sync_budget(
            db_session,
            tenant_id=T("t1"),
            license_id=str(lic_id),
            period=BudgetPeriod.weekly,
            budget_tokens=2_000,
        )
        assert created_w is True
        assert b_m.id != b_w.id

    @pytest.mark.unit
    async def test_different_licenses_are_separate_rows(self, db_session):
        """Same tenant, different licenses → two license_sync rows."""
        lic1 = uuid.uuid4()
        lic2 = uuid.uuid4()
        b1, _ = await upsert_license_sync_budget(
            db_session,
            tenant_id=T("t1"),
            license_id=str(lic1),
            period=BudgetPeriod.monthly,
            budget_tokens=10_000,
        )
        b2, created2 = await upsert_license_sync_budget(
            db_session,
            tenant_id=T("t1"),
            license_id=str(lic2),
            period=BudgetPeriod.monthly,
            budget_tokens=20_000,
        )
        assert created2 is True
        assert b1.id != b2.id

    @pytest.mark.unit
    async def test_inactive_license_sync_row_is_skipped(self, db_session):
        """A soft-deleted license_sync row should not be reconciled — a new
        active row is created instead. (Inactive rows are history.)"""
        lic_id = uuid.uuid4()
        b1, _ = await upsert_license_sync_budget(
            db_session,
            tenant_id=T("t1"),
            license_id=str(lic_id),
            period=BudgetPeriod.monthly,
            budget_tokens=10_000,
        )
        # Soft-delete the row directly.
        b1.active = False
        await db_session.flush()

        b2, created = await upsert_license_sync_budget(
            db_session,
            tenant_id=T("t1"),
            license_id=str(lic_id),
            period=BudgetPeriod.monthly,
            budget_tokens=15_000,
        )
        assert created is True
        assert b2.id != b1.id
        assert b2.active is True

    @pytest.mark.unit
    async def test_includes_flags_are_updated(self, db_session):
        """Re-sync overrides includes_input/includes_output to the incoming values."""
        lic_id = uuid.uuid4()
        b1, _ = await upsert_license_sync_budget(
            db_session,
            tenant_id=T("t1"),
            license_id=str(lic_id),
            period=BudgetPeriod.monthly,
            budget_tokens=10_000,
            includes_input=True,
            includes_output=True,
        )
        b2, created = await upsert_license_sync_budget(
            db_session,
            tenant_id=T("t1"),
            license_id=str(lic_id),
            period=BudgetPeriod.monthly,
            budget_tokens=10_000,
            includes_input=False,
            includes_output=True,
        )
        assert created is False
        assert b2.id == b1.id
        assert b2.includes_input is False
        assert b2.includes_output is True


class TestDeactivateOrphanLicenseSyncBudgets:
    """The orphan-reconcile helper: a license_sync budget whose license is no
    longer in the active set is soft-deleted; everything else is left alone."""

    @pytest.mark.unit
    async def test_orphan_deactivated_active_kept(self, db_session):
        lic_keep, lic_orphan = uuid.uuid4(), uuid.uuid4()
        b_keep, _ = await upsert_license_sync_budget(
            db_session,
            tenant_id=T("t1"),
            license_id=str(lic_keep),
            period=BudgetPeriod.monthly,
            budget_tokens=10_000,
        )
        b_orphan, _ = await upsert_license_sync_budget(
            db_session,
            tenant_id=T("t1"),
            license_id=str(lic_orphan),
            period=BudgetPeriod.monthly,
            budget_tokens=20_000,
        )

        priors = await deactivate_orphan_license_sync_budgets(
            db_session, active_license_ids={lic_keep}
        )
        assert [p.id for p in priors] == [b_orphan.id]
        assert priors[0].active is True  # snapshot captures the PRIOR (active) state

        await db_session.refresh(b_keep)
        await db_session.refresh(b_orphan)
        assert b_keep.active is True
        assert b_orphan.active is False

    @pytest.mark.unit
    async def test_manual_budget_never_orphaned(self, db_session):
        lic = uuid.uuid4()
        manual = await create_budget(
            tenant_id=T("t1"),
            budget_tokens=99_999,
            period="monthly",
            license_id=lic,
            source="manual",
            session=db_session,
        )
        # Even with an empty active set, a manual budget is not a license_sync orphan.
        priors = await deactivate_orphan_license_sync_budgets(db_session, active_license_ids=set())
        assert priors == []
        await db_session.refresh(manual)
        assert manual.active is True

    @pytest.mark.unit
    async def test_empty_active_set_deactivates_all_license_sync(self, db_session):
        # Proves the danger the router's `error is None` gate guards against: an
        # empty active set (only legitimate on a real 200) orphans every row.
        lic1, lic2 = uuid.uuid4(), uuid.uuid4()
        await upsert_license_sync_budget(
            db_session,
            tenant_id=T("t1"),
            license_id=str(lic1),
            period=BudgetPeriod.monthly,
            budget_tokens=10_000,
        )
        await upsert_license_sync_budget(
            db_session,
            tenant_id=T("t2"),
            license_id=str(lic2),
            period=BudgetPeriod.monthly,
            budget_tokens=20_000,
        )
        priors = await deactivate_orphan_license_sync_budgets(db_session, active_license_ids=set())
        assert len(priors) == 2
        rows = list(
            (
                await db_session.execute(
                    select(TokenBudget).where(TokenBudget.source == "license_sync")
                )
            )
            .scalars()
            .all()
        )
        assert rows and all(r.active is False for r in rows)

    @pytest.mark.unit
    async def test_already_inactive_row_not_returned(self, db_session):
        lic = uuid.uuid4()
        b, _ = await upsert_license_sync_budget(
            db_session,
            tenant_id=T("t1"),
            license_id=str(lic),
            period=BudgetPeriod.monthly,
            budget_tokens=10_000,
        )
        b.active = False
        await db_session.flush()
        priors = await deactivate_orphan_license_sync_budgets(db_session, active_license_ids=set())
        assert priors == []


# ---------------------------------------------------------------------------
# Endpoint-level tests via TestClient — proves payload counts
# ---------------------------------------------------------------------------


def _mock_licenses_client(payload: list[dict]) -> AsyncMock:
    mock_resp = MagicMock()
    mock_resp.status_code = 200
    mock_resp.json.return_value = payload
    mock_client = AsyncMock()
    mock_client.get.return_value = mock_resp
    mock_client.__aenter__ = AsyncMock(return_value=mock_client)
    mock_client.__aexit__ = AsyncMock(return_value=False)
    return mock_client


def _make_stub_redis() -> AsyncMock:
    """A redis stand-in that always returns success on writes.

    fakeredis.FakeRedis caches its socket queue against the first event loop
    it sees; calling the same instance from a fresh TestClient request loop
    raises 'bound to a different event loop'. For multi-request tests we use
    this stub instead — the route only needs writes to not raise.
    """
    stub = AsyncMock()
    stub.set = AsyncMock(return_value=True)
    stub.get = AsyncMock(return_value=None)
    stub.incrby = AsyncMock(return_value=1)
    stub.decrby = AsyncMock(return_value=0)
    stub.expire = AsyncMock(return_value=True)
    stub.delete = AsyncMock(return_value=1)
    # ⚠️ A double NARROWER than the real client cannot fail. On the real async
    # client `scan_iter` is an async GENERATOR; a bare AsyncMock attribute
    # returns a coroutine, so any caller doing `async for ... in scan_iter(...)`
    # dies with "'async for' requires an object with __aiter__". This stub was
    # silently narrower until a reconcile started scanning, which
    # is exactly the class of hole that lets a reconcile path go untested.
    stub.scan_iter = _empty_scan_iter
    return stub


async def _empty_scan_iter(*_args, **_kwargs):
    """Real async-generator shape for ``redis.scan_iter``, yielding nothing."""
    for key in ():
        yield key


def _count_license_sync_rows_sync(db_engine_url: str, lic_id: uuid.UUID) -> int:
    """Sync DB count via psycopg directly — avoids cross-event-loop issues
    when called from a sync test that also uses fakeredis."""
    import psycopg

    sync_url = db_engine_url.replace("+psycopg", "")
    with psycopg.connect(sync_url) as conn, conn.cursor() as cur:
        cur.execute(
            "SELECT count(*) FROM token_budgets WHERE license_id = %s AND source = 'license_sync'",
            (str(lic_id),),
        )
        row = cur.fetchone()
        assert row is not None
        return int(row[0])


def _read_budgets_for_license_sync(db_engine_url: str, lic_id: uuid.UUID) -> list[dict]:
    import psycopg

    sync_url = db_engine_url.replace("+psycopg", "")
    with psycopg.connect(sync_url) as conn, conn.cursor() as cur:
        cur.execute(
            "SELECT id, budget_tokens, source, active FROM token_budgets WHERE license_id = %s",
            (str(lic_id),),
        )
        rows = cur.fetchall()
        return [{"id": r[0], "budget_tokens": r[1], "source": r[2], "active": r[3]} for r in rows]


class TestSyncEndpointReconciles:
    @pytest.mark.unit
    def test_sync_response_reports_created_and_updated(self, app_client, monkeypatch):
        """First sync reports 1 created / 0 updated; second sync reports 0/1."""
        import redis_client

        monkeypatch.setattr(redis_client, "_redis", _make_stub_redis())

        lic_id = str(uuid.uuid4())
        payload: list[dict] = [
            {
                "id": lic_id,
                "tenant_id": T("t-sync-resp"),
                "plan": {"token_budget_monthly": 10_000},
            },
        ]

        # First sync — creates one budget.
        with patch(
            "routers.budget.httpx.AsyncClient",
            return_value=_mock_licenses_client(payload),
        ):
            r1 = app_client.post("/api/budget/sync-from-licenses")
        assert r1.status_code == 200, r1.text
        d1 = r1.json()
        assert d1["status"] == "ok"
        assert d1["created_count"] == 1
        assert d1["updated_count"] == 0
        assert d1["processed_count"] == 1

        # Second sync — same payload, same license. Reconciled in place.
        with patch(
            "routers.budget.httpx.AsyncClient",
            return_value=_mock_licenses_client(payload),
        ):
            r2 = app_client.post("/api/budget/sync-from-licenses")
        assert r2.status_code == 200
        d2 = r2.json()
        assert d2["created_count"] == 0
        assert d2["updated_count"] == 1
        assert d2["processed_count"] == 1

    @pytest.mark.unit
    def test_resyncing_does_not_duplicate_budgets(self, app_client, monkeypatch, db_engine):
        """Repeated syncs of the same license keep exactly one license_sync row."""
        import redis_client

        monkeypatch.setattr(redis_client, "_redis", _make_stub_redis())

        lic_id = str(uuid.uuid4())
        payload: list[dict] = [
            {
                "id": lic_id,
                "tenant_id": T("t-no-dup"),
                "plan": {"token_budget_monthly": 10_000},
            },
        ]

        for _ in range(3):
            with patch(
                "routers.budget.httpx.AsyncClient",
                return_value=_mock_licenses_client(payload),
            ):
                r = app_client.post("/api/budget/sync-from-licenses")
            assert r.status_code == 200, r.text

        import os

        db_url = os.environ["DATABASE_URL"]
        assert _count_license_sync_rows_sync(db_url, uuid.UUID(lic_id)) == 1

    @pytest.mark.unit
    def test_resync_with_changed_cap_updates_payload_in_place(
        self, app_client, monkeypatch, db_engine
    ):
        """Plan change between syncs reflects on the existing row, not a new one."""
        import redis_client

        monkeypatch.setattr(redis_client, "_redis", _make_stub_redis())

        lic_id = str(uuid.uuid4())
        payload_v1: list[dict] = [
            {
                "id": lic_id,
                "tenant_id": T("t-changed-cap"),
                "plan": {"token_budget_monthly": 10_000},
            },
        ]
        payload_v2: list[dict] = [
            {
                "id": lic_id,
                "tenant_id": T("t-changed-cap"),
                "plan": {"token_budget_monthly": 50_000},
            },
        ]

        with patch(
            "routers.budget.httpx.AsyncClient",
            return_value=_mock_licenses_client(payload_v1),
        ):
            app_client.post("/api/budget/sync-from-licenses")

        with patch(
            "routers.budget.httpx.AsyncClient",
            return_value=_mock_licenses_client(payload_v2),
        ):
            r = app_client.post("/api/budget/sync-from-licenses")
        assert r.status_code == 200
        assert r.json()["updated_count"] == 1

        import os

        db_url = os.environ["DATABASE_URL"]
        rows = _read_budgets_for_license_sync(db_url, uuid.UUID(lic_id))
        assert len(rows) == 1
        assert rows[0]["budget_tokens"] == 50_000

    @pytest.mark.unit
    def test_partial_status_includes_zero_counts(self, app_client, monkeypatch):
        """Connection error fails loud with 502; structured detail carries zero counts."""
        import redis_client

        monkeypatch.setattr(redis_client, "_redis", _make_stub_redis())

        mock_client = AsyncMock()
        mock_client.get.side_effect = httpx.ConnectError("nope")
        mock_client.__aenter__ = AsyncMock(return_value=mock_client)
        mock_client.__aexit__ = AsyncMock(return_value=False)

        with patch("routers.budget.httpx.AsyncClient", return_value=mock_client):
            r = app_client.post("/api/budget/sync-from-licenses")
        assert r.status_code == 502
        d = r.json()["error"]["details"]
        assert d["error"]
        assert d["created_count"] == 0
        assert d["updated_count"] == 0

    @pytest.mark.unit
    def test_orphan_budget_deactivated_when_license_drops(self, app_client, monkeypatch, db_engine):
        """A license that drops out of the active list has its license_sync budget
        soft-deactivated on the next sync; a still-active license is untouched."""
        import os

        import redis_client

        monkeypatch.setattr(redis_client, "_redis", _make_stub_redis())

        lic_a, lic_b = str(uuid.uuid4()), str(uuid.uuid4())
        payload_both: list[dict] = [
            {"id": lic_a, "tenant_id": T("t-orphan"), "plan": {"token_budget_monthly": 10_000}},
            {"id": lic_b, "tenant_id": T("t-orphan"), "plan": {"token_budget_monthly": 20_000}},
        ]
        payload_b_only: list[dict] = [
            {"id": lic_b, "tenant_id": T("t-orphan"), "plan": {"token_budget_monthly": 20_000}},
        ]

        with patch(
            "routers.budget.httpx.AsyncClient",
            return_value=_mock_licenses_client(payload_both),
        ):
            app_client.post("/api/budget/sync-from-licenses")

        # lic_a deactivated upstream — no longer in the active list.
        with patch(
            "routers.budget.httpx.AsyncClient",
            return_value=_mock_licenses_client(payload_b_only),
        ):
            r = app_client.post("/api/budget/sync-from-licenses")
        assert r.status_code == 200, r.text
        assert r.json()["orphans_deactivated"] == 1

        db_url = os.environ["DATABASE_URL"]
        rows_a = _read_budgets_for_license_sync(db_url, uuid.UUID(lic_a))
        rows_b = _read_budgets_for_license_sync(db_url, uuid.UUID(lic_b))
        assert len(rows_a) == 1 and rows_a[0]["active"] is False
        assert len(rows_b) == 1 and rows_b[0]["active"] is True

    @pytest.mark.unit
    def test_fetch_failure_does_not_deactivate(self, app_client, monkeypatch, db_engine):
        """A failed licenses fetch must NEVER deactivate budgets (safety gate)."""
        import os

        import redis_client

        monkeypatch.setattr(redis_client, "_redis", _make_stub_redis())

        lic_a = str(uuid.uuid4())
        payload: list[dict] = [
            {"id": lic_a, "tenant_id": T("t-fail"), "plan": {"token_budget_monthly": 10_000}},
        ]
        with patch(
            "routers.budget.httpx.AsyncClient",
            return_value=_mock_licenses_client(payload),
        ):
            app_client.post("/api/budget/sync-from-licenses")

        # The licenses fetch now errors — orphan reconcile must not run.
        mock_client = AsyncMock()
        mock_client.get.side_effect = httpx.ConnectError("nope")
        mock_client.__aenter__ = AsyncMock(return_value=mock_client)
        mock_client.__aexit__ = AsyncMock(return_value=False)
        with patch("routers.budget.httpx.AsyncClient", return_value=mock_client):
            r = app_client.post("/api/budget/sync-from-licenses")
        assert r.status_code == 502
        d = r.json()["error"]["details"]
        assert d.get("orphans_deactivated", 0) == 0

        db_url = os.environ["DATABASE_URL"]
        rows_a = _read_budgets_for_license_sync(db_url, uuid.UUID(lic_a))
        assert len(rows_a) == 1 and rows_a[0]["active"] is True


class TestSyncOutboundHeaders:
    """What sync-from-licenses actually puts on the wire to the licenses service.

    The handler used to send ``X-Tenant-Id: __admin__``. ``/internal`` is in the
    licenses service's ``PROTECTED_PREFIXES`` and its ``AuthMiddleware`` runs
    ``validate_tenant_uuid(raw_tenant_id, source="X-Tenant-Id")`` *before* the
    auth branch, so a non-UUID sentinel was a hard 400 on every single call —
    swallowed here into ``error=`` and reported as 200 with 0 synced.
    """

    @pytest.mark.unit
    def test_no_tenant_header_is_sent_and_bearer_is_kept(self, app_client, monkeypatch):
        """No X-Tenant-Id on the wire; the service bearer IS still there."""
        import redis_client
        from main import app

        monkeypatch.setattr(redis_client, "_redis", _make_stub_redis())

        class _StubProvider:
            async def get_token(self) -> str:
                return "svc-token-abc"

        monkeypatch.setattr(app.state, "service_token_provider", _StubProvider(), raising=False)

        lic_id = str(uuid.uuid4())
        payload: list[dict] = [
            {"id": lic_id, "tenant_id": T("t-hdr"), "plan": {"token_budget_monthly": 10_000}},
        ]
        mock_client = _mock_licenses_client(payload)
        with patch("routers.budget.httpx.AsyncClient", return_value=mock_client):
            r = app_client.post("/api/budget/sync-from-licenses")

        assert r.status_code == 200, r.text
        assert r.json().get("error") is None

        # Two licences reads per run since migration 0050: /internal/licenses and
        # /internal/apps. BOTH are under licences' /internal prefix, so the rule
        # below holds for every call, not just the first.
        urls = [c.args[0] for c in mock_client.get.await_args_list]
        assert sorted(u.rsplit("/", 1)[-1] for u in urls) == ["apps", "licenses"], urls
        for call in mock_client.get.await_args_list:
            sent = call.kwargs["headers"]
            lowered = {k.lower(): v for k, v in sent.items()}

            # The fix. Case-insensitive: httpx headers are case-insensitive and a
            # differently-cased key would 400 in licences exactly the same way.
            assert "x-tenant-id" not in lowered, (
                f"sync-from-licenses still sends a tenant header: {sent!r}. "
                "licences validates it as a UUID before auth, so any non-UUID "
                "value 400s every call."
            )
            # PAIRED assertion: dropping ALL headers would satisfy the assertion
            # above while breaking authorisation entirely. require_internal_service
            # authorises on this bearer and nothing else.
            assert lowered.get("authorization") == "Bearer svc-token-abc"


class TestSyncSkipsUnwritableLicencesWithoutLosingTheBatch:
    """One malformed licence must cost that licence, not the whole reconcile.

    Every write in ``sync-from-licenses`` — budgets, ``request_quota_config``
    rows, and the orphan reconcile that follows the loop — shares ONE
    transaction and ONE commit at the end of the handler. So any licence that
    can raise mid-loop takes every OTHER tenant's repair down with it, and this
    endpoint is a manual reconcile-of-last-resort whose whole value is fixing as
    much drift as it can reach.

    Two identifiers could raise, and BOTH did:

    * ``tenant_id`` comes out of the licences service's JSON, so
      ``require_tenant`` never saw it and it may be a registered tenant NAME.
      Against a ``uuid`` column (migration 0011) that is a ``DataError``.
    * ``license_id`` is parsed by ``upsert_license_sync_budget`` itself
      (``uuid.UUID(license_id)``), so a malformed one raised a bare
      ``ValueError`` that nothing in the handler catches — ``except
      httpx.RequestError`` does not — and the endpoint 500'd.

    Both are now skipped, counted into ONE ``skipped_count`` (the response body
    answers a single question — "did upstream hand us rows we could not
    write?" — and the operator's next move is the same either way; the two
    causes are told apart by their distinct log events, which is where a
    diagnosis belongs) and, critically, neither aborts the batch.
    """

    @pytest.mark.unit
    def test_a_registered_tenant_name_is_resolved_not_rejected(self, app_client, monkeypatch):
        """The licences service may report a tenant by NAME; it must reach SQL as a uuid."""
        import redis_client
        from tenancy import LEGACY_TENANT_NAMES

        monkeypatch.setattr(redis_client, "_redis", _make_stub_redis())
        lic_id = str(uuid.uuid4())
        payload = [{"id": lic_id, "tenant_id": "n8n-dev", "plan": {"token_budget_monthly": 7_000}}]

        with patch("routers.budget.httpx.AsyncClient", return_value=_mock_licenses_client(payload)):
            r = app_client.post("/api/budget/sync-from-licenses")

        assert r.status_code == 200, r.text
        assert r.json()["created_count"] == 1, r.text
        assert r.json()["skipped_count"] == 0, r.text

        # And it landed under the canonical uuid, not the name.
        lr = app_client.get(
            f"/api/budget/list/{LEGACY_TENANT_NAMES['n8n-dev']}",
            headers={"X-Tenant-Id": LEGACY_TENANT_NAMES["n8n-dev"]},
        )
        assert lr.status_code == 200, lr.text
        assert any(b["license_id"] == lic_id for b in lr.json()["data"]), lr.text

    @pytest.mark.unit
    def test_an_unresolvable_tenant_is_skipped_and_counted(self, app_client, monkeypatch):
        import redis_client

        monkeypatch.setattr(redis_client, "_redis", _make_stub_redis())
        payload = [
            {
                "id": str(uuid.uuid4()),
                "tenant_id": "no-such-tenant",
                "plan": {"token_budget_monthly": 1_000},
            }
        ]

        with patch("routers.budget.httpx.AsyncClient", return_value=_mock_licenses_client(payload)):
            r = app_client.post("/api/budget/sync-from-licenses")

        assert r.status_code == 200, r.text
        d = r.json()
        assert d["skipped_count"] == 1, d
        # Counted, NOT quietly folded into a smaller processed_count.
        assert d["created_count"] == 0, d
        assert d["processed_count"] == 0, d

    @pytest.mark.unit
    def test_an_unparseable_license_id_is_skipped_and_counted(self, app_client, monkeypatch):
        """Was a bare ValueError out of upsert_license_sync_budget → uncaught → 500."""
        import redis_client

        monkeypatch.setattr(redis_client, "_redis", _make_stub_redis())
        payload = [
            {
                "id": "not-a-uuid",
                "tenant_id": T("t-bad-lic-id"),
                "plan": {"token_budget_monthly": 1_000, "quota_monthly": 5},
            }
        ]

        with patch("routers.budget.httpx.AsyncClient", return_value=_mock_licenses_client(payload)):
            r = app_client.post("/api/budget/sync-from-licenses")

        assert r.status_code == 200, r.text
        d = r.json()
        assert d["skipped_count"] == 1, d
        assert d["created_count"] == 0, d
        # The quota branch must make the SAME decision about the same licence —
        # it used to re-parse the id privately and skip quietly while the budget
        # branch above blew the request up.
        assert d["quotas_synced"] == 0, d

    @pytest.mark.unit
    def test_one_bad_licence_does_not_roll_back_the_others(self, app_client, monkeypatch):
        """The whole point: the batch commits for every licence that IS writable."""
        import redis_client
        from tenancy import LEGACY_TENANT_NAMES

        monkeypatch.setattr(redis_client, "_redis", _make_stub_redis())
        good = str(uuid.uuid4())
        payload = [
            {"id": good, "tenant_id": "n8n-dev", "plan": {"token_budget_monthly": 10_000}},
            # bad tenant
            {
                "id": str(uuid.uuid4()),
                "tenant_id": "no-such-tenant",
                "plan": {"token_budget_monthly": 1},
            },
            # bad licence id
            {"id": "not-a-uuid", "tenant_id": T("t-mixed"), "plan": {"token_budget_monthly": 1}},
            # no tenant at all
            {"id": str(uuid.uuid4()), "plan": {"token_budget_monthly": 1}},
        ]

        with patch("routers.budget.httpx.AsyncClient", return_value=_mock_licenses_client(payload)):
            r = app_client.post("/api/budget/sync-from-licenses")

        assert r.status_code == 200, r.text
        d = r.json()
        assert d["created_count"] == 1, d
        assert d["processed_count"] == 1, d
        assert d["skipped_count"] == 3, d

        # Durability, not just a hopeful counter: the good licence's budget is
        # committed. A rollback would leave the counters intact and the row gone.
        import os

        assert _count_license_sync_rows_sync(os.environ["DATABASE_URL"], uuid.UUID(good)) == 1
        lr = app_client.get(
            f"/api/budget/list/{LEGACY_TENANT_NAMES['n8n-dev']}",
            headers={"X-Tenant-Id": LEGACY_TENANT_NAMES["n8n-dev"]},
        )
        assert any(b["license_id"] == good for b in lr.json()["data"]), lr.text


def _read_quota_config_sync(db_url: str, lic_id: uuid.UUID) -> int | None:
    import psycopg

    with psycopg.connect(db_url.replace("+psycopg", "")) as conn, conn.cursor() as cur:
        cur.execute(
            "SELECT quota_monthly FROM request_quota_config WHERE license_id = %s",
            (str(lic_id),),
        )
        row = cur.fetchone()
        return None if row is None else int(row[0])


class TestSyncHonoursPerLicenceOverrides:
    """``token_budget_override`` / ``quota_override`` win over the plan default.

    Measured prod harm (2026-10-07): licence 01a0fdb9-… carried an override of
    100,000 in licences, and a sync-from-licenses run rewrote billing's budget to
    the plan's 50,000,000. The override is what licences' nightly reconcile
    pushes, so ignoring it here made the two writers fight.
    """

    @staticmethod
    def _sync(app_client, lic: dict) -> None:
        with patch("routers.budget.httpx.AsyncClient", return_value=_mock_licenses_client([lic])):
            r = app_client.post("/api/budget/sync-from-licenses")
        assert r.status_code == 200, r.text

    @pytest.mark.unit
    @pytest.mark.parametrize(("budget_override", "quota_override"), [(100_000, 42), (0, 0)])
    def test_override_wins_in_db(self, app_client, db_engine, budget_override, quota_override):
        import os

        lic_id = uuid.uuid4()
        lic = {
            "id": str(lic_id),
            "tenant_id": T("t-override"),
            "status": "active",
            "plan": {"token_budget_monthly": 50_000_000, "quota_monthly": 1_000},
            "token_budget_override": budget_override,
            "quota_override": quota_override,
        }
        self._sync(app_client, lic)

        db_url = os.environ["DATABASE_URL"]
        rows = _read_budgets_for_license_sync(db_url, lic_id)
        assert [r["budget_tokens"] for r in rows] == [budget_override]
        assert _read_quota_config_sync(db_url, lic_id) == quota_override

    @pytest.mark.unit
    @pytest.mark.parametrize("with_null_fields", [True, False])
    def test_absent_or_null_override_keeps_the_plan_value(
        self, app_client, db_engine, with_null_fields
    ):
        import os

        lic_id = uuid.uuid4()
        lic: dict = {
            "id": str(lic_id),
            "tenant_id": T("t-no-override"),
            "status": "active",
            "plan": {"token_budget_monthly": 50_000_000, "quota_monthly": 1_000},
        }
        if with_null_fields:
            lic |= {"token_budget_override": None, "quota_override": None}
        self._sync(app_client, lic)

        db_url = os.environ["DATABASE_URL"]
        assert [r["budget_tokens"] for r in _read_budgets_for_license_sync(db_url, lic_id)] == [
            50_000_000
        ]
        assert _read_quota_config_sync(db_url, lic_id) == 1_000

    @pytest.mark.unit
    def test_override_replaces_a_previously_synced_plan_value(self, app_client, db_engine):
        """The prod incident in reverse: plan value on disk, override arrives → override."""
        import os

        lic_id = uuid.uuid4()
        base = {
            "id": str(lic_id),
            "tenant_id": T("t-override-later"),
            "status": "active",
            "plan": {"token_budget_monthly": 50_000_000, "quota_monthly": 1_000},
        }
        self._sync(app_client, base)
        self._sync(
            app_client,
            {**base, "token_budget_override": 100_000, "quota_override": 10},
        )
        db_url = os.environ["DATABASE_URL"]
        assert [r["budget_tokens"] for r in _read_budgets_for_license_sync(db_url, lic_id)] == [
            100_000
        ]
        assert _read_quota_config_sync(db_url, lic_id) == 10
