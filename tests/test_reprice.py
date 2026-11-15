"""Tests for services.reprice — the NULL-cost backfill + rollup restatement.

Every test drives the real code path (real Postgres, real
``services.pricing._resolve_pricing_row``, real ``rollup_engine.rollup_daily``).
Nothing here mocks pricing away, because the bug being fixed *is* a pricing
resolution that never happened.

Note on fixtures: ``CostEvent.cost_usd`` has an ORM ``default=0``, so a NULL-cost
event must pass ``cost_usd=None`` explicitly — omitting it silently produces a
priced-at-zero row, which is exactly the state this module must not create.
"""

from __future__ import annotations

import uuid
from datetime import UTC, date, datetime
from decimal import Decimal
from typing import cast

import pytest
from fastapi.testclient import TestClient
from sqlalchemy import Table, func, select
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

import auth_deps
from models.db import AuditLog, CostDaily, CostEvent, ProviderPricing
from services.reprice import reprice_null_costs
from tests.conftest import T

# Midday UTC: `cast(timestamptz AS date)` runs in the session TimeZone, so a
# midday instant keeps the same calendar date under any sane server setting.
DAY = datetime(2026, 5, 21, 12, 0, tzinfo=UTC)
DAY_DATE = date(2026, 5, 21)
WINDOW_START = datetime(2026, 5, 1, tzinfo=UTC)
WINDOW_END = datetime(2026, 6, 1, tzinfo=UTC)


def _pricing(
    *,
    provider: str,
    model: str | None = None,
    service_key: str | None = None,
    input_per_1m_usd: Decimal | None = Decimal("3"),
    output_per_1m_usd: Decimal | None = Decimal("15"),
    effective_from: datetime = datetime(2026, 1, 1, tzinfo=UTC),
    effective_until: datetime | None = None,
    # ⚠️ WAS "per_token", WHICH CONTRADICTED THIS FIXTURE'S OWN PROVIDER.
    # `_event()` emits provider='kokoro', and kokoro is metered in CHARACTERS —
    # `services.pricing.QUANTITY_UNIT_BY_PROVIDER['kokoro'] == 'characters'`, and
    # `scripts/initial_pricing.yaml` seeds kokoro as `per_character`. The pair
    # (kokoro, per_token) exists nowhere in production; the fixture could only
    # carry it because nothing validated the unit against the cost_model, which
    # is the defect this branch fixes. Cost assertions are UNCHANGED: the three
    # token-family cost_models share identical arithmetic, so relabelling the
    # fixture moves no number — it only stops the fixture from asserting, by
    # construction, that a mispriced pair is fine.
    cost_model: str = "per_character",
) -> ProviderPricing:
    return ProviderPricing(
        provider=provider,
        model=model,
        service_key=service_key,
        rate_class="cost",
        cost_model=cost_model,
        input_per_1m_usd=input_per_1m_usd,
        output_per_1m_usd=output_per_1m_usd,
        effective_from=effective_from,
        effective_until=effective_until,
    )


def _event(
    *,
    created_at: datetime = DAY,
    provider: str = "kokoro",
    served_model: str | None = "kokoro-82m",
    model: str | None = None,
    service_key: str = "w2v",
    input_tokens: int = 1000,
    output_tokens: int = 200,
    cost_usd: Decimal | None = None,
    tenant_id: str = T("tenant-a"),
    tenant_name: str = "tenant-a",
    quantity_unit: str | None = None,
) -> CostEvent:
    """A cost event. ``cost_usd`` defaults to None = the pricing-miss state."""
    return CostEvent(
        id=uuid.uuid7(),
        tenant_id=tenant_id,
        tenant_name=tenant_name,
        service_key=service_key,
        provider=provider,
        model=model,
        served_model=served_model,
        input_tokens=input_tokens,
        output_tokens=output_tokens,
        cache_creation_input_tokens=0,
        cache_read_input_tokens=0,
        cost_usd=cost_usd,
        cost_source="test",
        created_at=created_at,
        quantity_unit=quantity_unit,
    )


# Column set written by the fixture Core insert below. Every name here is both
# the ORM attribute and the DB column name.
_COST_EVENT_COLUMNS = (
    "id",
    "tenant_id",
    "tenant_name",
    "service_key",
    "provider",
    "model",
    "served_model",
    "input_tokens",
    "output_tokens",
    "cache_creation_input_tokens",
    "cache_read_input_tokens",
    "cost_usd",
    "cost_source",
    "created_at",
    # The emitter's asserted unit. In the column set because reprice passes it
    # to `_cost_from_row`: a quantity in the wrong unit for the resolved row's
    # cost_model must be refusable on the repair path too, not only at flush.
    "quantity_unit",
)


async def _seed(db_session: AsyncSession, *objs) -> None:
    """Persist fixtures — cost events through a **Core** insert, deliberately.

    ``CostEvent.cost_usd`` carries an ORM ``default=0``, and the ORM applies a
    column default whenever the attribute is None. So ``session.add(CostEvent(
    cost_usd=None))`` silently lands a **0**, not a NULL, and the bug under test
    becomes unreproducible. ``cost_flusher`` writes via
    ``CostEvent.__table__.insert()`` (cost_flusher.py:258), which honours an
    explicit None — that is how the NULL-cost rows in prod came to exist, so the
    fixtures use exactly the same path.
    """
    cost_rows: list[dict[str, object]] = []
    for obj in objs:
        if isinstance(obj, CostEvent):
            cost_rows.append({col: getattr(obj, col) for col in _COST_EVENT_COLUMNS})
        else:
            db_session.add(obj)
    # Flush the ORM objects first so e.g. ProviderPricing.id is assigned.
    await db_session.flush()
    if cost_rows:
        await db_session.execute(cast(Table, CostEvent.__table__).insert(), cost_rows)
    await db_session.commit()


async def _fetch_events(factory: async_sessionmaker[AsyncSession]) -> list[CostEvent]:
    async with factory() as s:
        stmt = select(CostEvent).order_by(CostEvent.created_at, CostEvent.id)
        return list((await s.execute(stmt)).scalars().all())


async def _fetch_event(factory: async_sessionmaker[AsyncSession], event_id: uuid.UUID) -> CostEvent:
    async with factory() as s:
        stmt = select(CostEvent).where(CostEvent.id == event_id)
        return (await s.execute(stmt)).scalar_one()


@pytest.mark.unit
class TestRepriceNullCosts:
    async def test_prices_null_event_after_pricing_row_lands(
        self, db_session, db_session_factory
    ) -> None:
        """The core bug: pricing added AFTER the flush now reaches the event."""
        pricing = _pricing(provider="kokoro", model="kokoro-82m")
        event = _event()
        await _seed(db_session, pricing, event)

        result = await reprice_null_costs(db_session_factory, since=WINDOW_START, until=WINDOW_END)

        assert result.scanned == 1
        assert result.repriced == 1
        assert result.still_unpriced == 0
        # 1000 * 3/1M + 200 * 15/1M = 0.003 + 0.003 = 0.006
        assert result.total_cost_usd_added == Decimal("0.00600000")
        assert result.affected_bucket_dates == [DAY_DATE]
        assert result.by_provider[0].provider == "kokoro"
        assert result.by_provider[0].repriced == 1

        stored = await _fetch_event(db_session_factory, event.id)
        assert stored.cost_usd == Decimal("0.00600000")
        assert stored.pricing_id == pricing.id

    async def test_second_run_reprices_nothing_but_still_restates(
        self, db_session, db_session_factory
    ) -> None:
        """Re-running must not double-count — but MUST still restate the rollup.

        The restatement set is derived from the request window, not from what
        this run happened to reprice, so a date whose events are already priced
        is still re-rolled. That is what makes a retry-after-crash converge.
        """
        await _seed(db_session, _pricing(provider="kokoro", model="kokoro-82m"), _event())

        first = await reprice_null_costs(db_session_factory, since=WINDOW_START, until=WINDOW_END)
        assert first.repriced == 1

        second = await reprice_null_costs(db_session_factory, since=WINDOW_START, until=WINDOW_END)
        assert second.scanned == 0
        assert second.repriced == 0
        assert second.total_cost_usd_added == Decimal("0")
        # Nothing to reprice…
        assert second.affected_bucket_dates == []
        # …yet the already-priced date is still restated, from the DB not memory.
        assert second.restated_bucket_dates == [DAY_DATE]

        stored = (await _fetch_events(db_session_factory))[0]
        assert stored.cost_usd == Decimal("0.00600000")

    async def test_rerun_after_crash_converges_the_rollup(
        self, db_session, db_session_factory
    ) -> None:
        """The regression this module exists to prevent.

        Simulates a process that died after a batch's repricing commit but
        before the rollup step: the raw event is durably priced while
        ``billing_cost_daily`` still carries the stale $0 total. A retry cannot
        rediscover the date via ``cost_usd IS NULL`` — the row is no longer NULL
        — so if the restatement set came from in-run state the rollup would stay
        permanently wrong. It must converge instead.
        """
        from services import rollup_engine

        event = _event(input_tokens=1000, output_tokens=0)
        await _seed(db_session, _pricing(provider="kokoro", model="kokoro-82m"), event)

        # Stale rollup: written while the event's cost_usd was still NULL.
        await rollup_engine.rollup_daily(db_session_factory, DAY_DATE)

        # The crash: the money UPDATE committed, the rollup never ran.
        async with db_session_factory() as s:
            await s.execute(
                cast(Table, CostEvent.__table__)
                .update()
                .where(CostEvent.id == event.id)
                .values(cost_usd=Decimal("0.00300000"))
            )
            await s.commit()

        async with db_session_factory() as s:
            stale = (
                await s.execute(
                    select(func.coalesce(func.sum(CostDaily.total_cost_usd), 0)).where(
                        CostDaily.bucket_date == DAY_DATE
                    )
                )
            ).scalar_one()
        assert Decimal(stale) == Decimal("0"), "precondition: rollup is stale"

        # The retry: identical request, finds nothing to reprice.
        result = await reprice_null_costs(db_session_factory, since=WINDOW_START, until=WINDOW_END)
        assert result.repriced == 0
        assert result.restated_bucket_dates == [DAY_DATE]

        async with db_session_factory() as s:
            rolled = (
                await s.execute(
                    select(func.coalesce(func.sum(CostDaily.total_cost_usd), 0)).where(
                        CostDaily.bucket_date == DAY_DATE
                    )
                )
            ).scalar_one()
        assert Decimal(rolled) == Decimal("0.00300000")

    async def test_already_priced_event_is_never_modified(
        self, db_session, db_session_factory
    ) -> None:
        """A non-NULL cost_usd survives byte-for-byte, even a deliberately wrong one."""
        priced = _event(cost_usd=Decimal("1.23456789"))
        await _seed(db_session, _pricing(provider="kokoro", model="kokoro-82m"), priced)

        result = await reprice_null_costs(db_session_factory, since=WINDOW_START, until=WINDOW_END)

        assert result.scanned == 0
        assert result.repriced == 0
        stored = await _fetch_event(db_session_factory, priced.id)
        assert stored.cost_usd == Decimal("1.23456789")
        assert stored.pricing_id is None

    async def test_dry_run_reports_but_writes_nothing(self, db_session, db_session_factory) -> None:
        """Operator preview: full dollar delta reported, DB untouched."""
        await _seed(db_session, _pricing(provider="kokoro", model="kokoro-82m"), _event())

        result = await reprice_null_costs(
            db_session_factory, since=WINDOW_START, until=WINDOW_END, dry_run=True
        )

        assert result.dry_run is True
        assert result.repriced == 1
        assert result.total_cost_usd_added == Decimal("0.00600000")
        assert result.affected_bucket_dates == [DAY_DATE]
        # Nothing was restated, and nothing was written.
        assert result.restated_bucket_dates == []

        stored = (await _fetch_events(db_session_factory))[0]
        assert stored.cost_usd is None
        assert stored.pricing_id is None

        async with db_session_factory() as s:
            rows = (await s.execute(select(CostDaily))).scalars().all()
        assert rows == []

    async def test_window_is_half_open_and_respected(self, db_session, db_session_factory) -> None:
        """[since, until): the `until` instant is excluded, earlier events untouched."""
        before = _event(created_at=datetime(2026, 4, 20, 12, 0, tzinfo=UTC))
        on_lower_bound = _event(created_at=WINDOW_START)
        inside = _event()
        on_upper_bound = _event(created_at=WINDOW_END)
        after = _event(created_at=datetime(2026, 6, 15, 12, 0, tzinfo=UTC))
        await _seed(
            db_session,
            _pricing(provider="kokoro", model="kokoro-82m"),
            before,
            on_lower_bound,
            inside,
            on_upper_bound,
            after,
        )

        result = await reprice_null_costs(db_session_factory, since=WINDOW_START, until=WINDOW_END)

        # since is inclusive, until is exclusive.
        assert result.scanned == 2
        assert result.repriced == 2

        for event in (before, on_upper_bound, after):
            assert (await _fetch_event(db_session_factory, event.id)).cost_usd is None
        for event in (on_lower_bound, inside):
            assert (await _fetch_event(db_session_factory, event.id)).cost_usd == Decimal(
                "0.00600000"
            )

    async def test_still_unpriced_stays_null_not_zero(self, db_session, db_session_factory) -> None:
        """No pricing row = we still don't know. NULL must not become 0."""
        priceable = _event()
        orphan = _event(provider="mystery-provider", served_model="who-knows")
        await _seed(db_session, _pricing(provider="kokoro", model="kokoro-82m"), priceable, orphan)

        result = await reprice_null_costs(db_session_factory, since=WINDOW_START, until=WINDOW_END)

        assert result.scanned == 2
        assert result.repriced == 1
        assert result.still_unpriced == 1

        stored = await _fetch_event(db_session_factory, orphan.id)
        assert stored.cost_usd is None, "an unpriceable event must stay NULL, never 0"
        assert stored.pricing_id is None

        by_provider = {p.provider: p for p in result.by_provider}
        assert by_provider["mystery-provider"].still_unpriced == 1
        assert by_provider["mystery-provider"].repriced == 0
        assert by_provider["kokoro"].repriced == 1

    async def test_provider_filter_narrows_the_scan(self, db_session, db_session_factory) -> None:
        kokoro = _event()
        karma = _event(provider="karma", served_model="karma-1")
        await _seed(
            db_session,
            _pricing(provider="kokoro", model="kokoro-82m"),
            _pricing(provider="karma", model="karma-1"),
            kokoro,
            karma,
        )

        result = await reprice_null_costs(
            db_session_factory, since=WINDOW_START, until=WINDOW_END, provider="karma"
        )

        assert result.scanned == 1
        assert result.repriced == 1
        assert (await _fetch_event(db_session_factory, kokoro.id)).cost_usd is None
        assert (await _fetch_event(db_session_factory, karma.id)).cost_usd is not None

    async def test_batches_page_forward_past_unpriceable_rows(
        self, db_session, db_session_factory
    ) -> None:
        """limit=1 with an unpriceable row in the middle must still terminate.

        The keyset cursor is what prevents an infinite loop here: still_unpriced
        rows stay NULL, so a naive "re-query the NULLs" loop would re-read them
        forever.
        """
        events = [
            _event(created_at=DAY, input_tokens=1000, output_tokens=0),
            _event(provider="mystery-provider", served_model="who-knows"),
            _event(created_at=DAY, input_tokens=2000, output_tokens=0),
        ]
        await _seed(db_session, _pricing(provider="kokoro", model="kokoro-82m"), *events)

        result = await reprice_null_costs(
            db_session_factory, since=WINDOW_START, until=WINDOW_END, limit=1
        )

        assert result.scanned == 3
        assert result.repriced == 2
        assert result.still_unpriced == 1
        # 1000 * 3/1M + 2000 * 3/1M = 0.003 + 0.006
        assert result.total_cost_usd_added == Decimal("0.00900000")

    async def test_effective_dated_row_is_resolved_per_event_timestamp(
        self, db_session, db_session_factory
    ) -> None:
        """The memo must not leak a rate across an effective_from boundary."""
        old_rate = _pricing(
            provider="kokoro",
            model="kokoro-82m",
            input_per_1m_usd=Decimal("3"),
            output_per_1m_usd=Decimal("0"),
            effective_from=datetime(2026, 1, 1, tzinfo=UTC),
            effective_until=datetime(2026, 5, 15, tzinfo=UTC),
        )
        new_rate = _pricing(
            provider="kokoro",
            model="kokoro-82m",
            input_per_1m_usd=Decimal("6"),
            output_per_1m_usd=Decimal("0"),
            effective_from=datetime(2026, 5, 15, tzinfo=UTC),
        )
        early = _event(
            created_at=datetime(2026, 5, 2, 12, 0, tzinfo=UTC),
            input_tokens=1000,
            output_tokens=0,
        )
        late = _event(created_at=DAY, input_tokens=1000, output_tokens=0)
        await _seed(db_session, old_rate, new_rate, early, late)

        await reprice_null_costs(db_session_factory, since=WINDOW_START, until=WINDOW_END)

        assert (await _fetch_event(db_session_factory, early.id)).cost_usd == Decimal("0.00300000")
        assert (await _fetch_event(db_session_factory, late.id)).cost_usd == Decimal("0.00600000")

    async def test_bad_window_is_rejected(self, db_session_factory) -> None:
        with pytest.raises(ValueError):
            await reprice_null_costs(db_session_factory, since=WINDOW_END, until=WINDOW_START)
        with pytest.raises(ValueError):
            await reprice_null_costs(
                db_session_factory,
                since=datetime(2020, 1, 1, tzinfo=UTC),
                until=datetime(2026, 1, 1, tzinfo=UTC),
            )


@pytest.mark.unit
class TestRollupRestatement:
    async def test_cost_daily_matches_repriced_events(self, db_session, db_session_factory) -> None:
        """Half a fix is worse than none: the rollup must agree with the raw table."""
        events = [
            _event(input_tokens=1000, output_tokens=0),
            _event(
                input_tokens=2000,
                output_tokens=0,
                tenant_id=T("tenant-b"),
                tenant_name="tenant-b",
            ),
        ]
        await _seed(db_session, _pricing(provider="kokoro", model="kokoro-82m"), *events)

        # Pre-existing (stale) rollup for the day: everything was $0 because
        # every event's cost_usd was NULL.
        from services import rollup_engine

        await rollup_engine.rollup_daily(db_session_factory, DAY_DATE)
        async with db_session_factory() as s:
            stale = (
                await s.execute(
                    select(func.coalesce(func.sum(CostDaily.total_cost_usd), 0)).where(
                        CostDaily.bucket_date == DAY_DATE
                    )
                )
            ).scalar_one()
        assert Decimal(stale) == Decimal("0")

        result = await reprice_null_costs(db_session_factory, since=WINDOW_START, until=WINDOW_END)
        assert result.restated_bucket_dates == [DAY_DATE]

        async with db_session_factory() as s:
            rolled = (
                await s.execute(
                    select(func.coalesce(func.sum(CostDaily.total_cost_usd), 0)).where(
                        CostDaily.bucket_date == DAY_DATE
                    )
                )
            ).scalar_one()
            # All seeded events fall on DAY_DATE, so an unfiltered sum of the
            # raw table is the sum for that bucket.
            raw = (
                await s.execute(select(func.coalesce(func.sum(CostEvent.cost_usd), 0)))
            ).scalar_one()

        # 1000*3/1M + 2000*3/1M = 0.009
        assert Decimal(rolled) == Decimal("0.00900000")
        assert Decimal(rolled) == Decimal(raw) == result.total_cost_usd_added


@pytest.fixture
def admin_actor(monkeypatch):
    """Force the bearer decode to a known license-admin payload."""
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


@pytest.mark.unit
class TestRepriceEndpoint:
    @staticmethod
    def _override_factory(db_engine) -> async_sessionmaker[AsyncSession]:
        """Point the route's get_session_factory dependency at the test engine.

        Cleared by the app_client fixture's dependency_overrides.clear().
        """
        from db import get_session_factory
        from main import app

        factory = async_sessionmaker(bind=db_engine, class_=AsyncSession, expire_on_commit=False)
        app.dependency_overrides[get_session_factory] = lambda: factory
        return factory

    async def test_defaults_to_dry_run_and_audits(
        self, app_client: TestClient, db_engine, db_session, admin_actor
    ) -> None:
        await _seed(db_session, _pricing(provider="kokoro", model="kokoro-82m"), _event())
        factory = self._override_factory(db_engine)

        resp = app_client.post(
            "/api/admin/reprice",
            headers={"Authorization": "Bearer fake"},
            json={
                "since": WINDOW_START.isoformat(),
                "until": WINDOW_END.isoformat(),
            },
        )
        assert resp.status_code == 200, resp.text
        data = resp.json()["data"]
        assert data["dry_run"] is True
        assert data["repriced"] == 1
        assert Decimal(data["total_cost_usd_added"]) == Decimal("0.00600000")

        # dry_run default => nothing written to the money column…
        assert (await _fetch_events(factory))[0].cost_usd is None
        # …but exactly one audit row, recording the window and the delta.
        async with factory() as s:
            audits = list(
                (await s.execute(select(AuditLog).where(AuditLog.action == "admin.reprice")))
                .scalars()
                .all()
            )
        assert len(audits) == 1
        before, after = audits[0].before, audits[0].after
        assert before is not None and after is not None
        assert before["dry_run"] is True
        assert before["total_cost_usd"] == "0"
        assert after["total_cost_usd"] == "0.00600000"

    async def test_explicit_write_run(
        self, app_client: TestClient, db_engine, db_session, admin_actor
    ) -> None:
        await _seed(db_session, _pricing(provider="kokoro", model="kokoro-82m"), _event())
        factory = self._override_factory(db_engine)

        resp = app_client.post(
            "/api/admin/reprice",
            headers={"Authorization": "Bearer fake"},
            json={
                "since": WINDOW_START.isoformat(),
                "until": WINDOW_END.isoformat(),
                "dry_run": False,
            },
        )
        assert resp.status_code == 200, resp.text
        data = resp.json()["data"]
        assert data["dry_run"] is False
        assert data["restated_bucket_dates"] == [str(DAY_DATE)]
        assert (await _fetch_events(factory))[0].cost_usd == Decimal("0.00600000")

    async def test_inverted_window_is_422(
        self, app_client: TestClient, db_engine, admin_actor
    ) -> None:
        self._override_factory(db_engine)
        resp = app_client.post(
            "/api/admin/reprice",
            headers={"Authorization": "Bearer fake"},
            json={
                "since": WINDOW_END.isoformat(),
                "until": WINDOW_START.isoformat(),
            },
        )
        assert resp.status_code == 422, resp.text

    async def test_requires_auth(self, app_client: TestClient, db_engine, monkeypatch) -> None:
        """Unauthenticated callers cannot restate money data.

        conftest sets DASHBOARD_AUTH_ENABLED=false (which short-circuits the
        decode to a dev payload), so re-enable it for this test — otherwise the
        assertion would pass for the wrong reason in a real deployment.
        """
        import auth

        monkeypatch.setattr(auth, "DASHBOARD_AUTH_ENABLED", True)
        self._override_factory(db_engine)
        resp = app_client.post(
            "/api/admin/reprice",
            json={
                "since": WINDOW_START.isoformat(),
                "until": WINDOW_END.isoformat(),
            },
        )
        assert resp.status_code == 401, resp.text


@pytest.mark.unit
class TestRepriceRefusesAUnitMismatch:
    """⛔ THE REPAIR PATH MUST REFUSE WHAT THE FLUSH PATH REFUSES.

    ``reprice`` composes ``_resolve_pricing_row`` + ``_cost_from_row`` precisely
    so the two paths cannot diverge. Before the unit check existed, a matched row
    from the WRONG unit family produced a number here and the backfill wrote it —
    making the misprice permanent, because the row is no longer NULL and no
    later run will ever revisit it.
    """

    async def test_seconds_event_against_a_per_token_row_stays_unpriced(
        self, db_session, db_session_factory
    ) -> None:
        """⭐ KILLING TEST. On unmodified code this reprices to 0.00600000.

        The live shape: a ``per_token`` row shadowing the ``per_second`` rate for
        a provider whose emitter measures AUDIO-SECONDS.
        """
        event = _event(
            provider="video-analyzer",
            served_model="video-analyzer-v1",
            service_key="w2v-video-analyzer",  # pragma: allowlist secret
            quantity_unit="seconds",
        )
        await _seed(
            db_session,
            # Rung 3 of the resolver: (provider, model IS NULL, service_key) —
            # the shape row 32 actually has in production.
            _pricing(
                provider="video-analyzer",
                model=None,
                service_key="w2v-video-analyzer",  # pragma: allowlist secret
                cost_model="per_token",
            ),
            event,
        )

        result = await reprice_null_costs(db_session_factory, since=WINDOW_START, until=WINDOW_END)

        assert result.scanned == 1
        assert result.repriced == 0, (
            "a quantity in SECONDS was repriced by a per_token rate; the backfill "
            "would have written a number and the row could never be repaired again"
        )
        assert result.still_unpriced == 1
        assert result.total_cost_usd_added == Decimal("0")
        stored = await _fetch_event(db_session_factory, event.id)
        assert stored.cost_usd is None, "absent must stay absent, never a stored 0"

    async def test_the_same_event_prices_once_the_row_is_in_the_right_family(
        self, db_session, db_session_factory
    ) -> None:
        """⚠️ NEGATIVE CONTROL / repair proof. The identical event, against a
        ``per_second`` row, reprices normally — so the refusal above is about the
        UNIT, not about the provider, the event, or the window."""
        event = _event(
            provider="video-analyzer",
            served_model="video-analyzer-v1",
            service_key="w2v-video-analyzer",  # pragma: allowlist secret
            quantity_unit="seconds",
        )
        await _seed(
            db_session,
            _pricing(
                provider="video-analyzer",
                model=None,
                service_key="w2v-video-analyzer",  # pragma: allowlist secret
                cost_model="per_second",
            ),
            event,
        )

        result = await reprice_null_costs(db_session_factory, since=WINDOW_START, until=WINDOW_END)

        assert result.repriced == 1
        assert result.still_unpriced == 0
        # 1000 * 3/1M + 200 * 15/1M = 0.006 — identical arithmetic to per_token,
        # which is exactly why the unit had to be checked rather than the value.
        assert result.total_cost_usd_added == Decimal("0.00600000")
        stored = await _fetch_event(db_session_factory, event.id)
        assert stored.cost_usd == Decimal("0.00600000")

    async def test_an_event_with_no_quantity_unit_still_reprices(
        self, db_session, db_session_factory
    ) -> None:
        """⚠️ OVER-STRICTNESS GUARD. Almost every historic row has
        ``quantity_unit IS NULL`` (the column is alembic 0021). If absence
        refused, the backfill would stop repairing the rows it exists for."""
        event = _event(quantity_unit=None)
        await _seed(db_session, _pricing(provider="kokoro", model="kokoro-82m"), event)

        result = await reprice_null_costs(db_session_factory, since=WINDOW_START, until=WINDOW_END)

        assert result.repriced == 1
        assert result.still_unpriced == 0
        stored = await _fetch_event(db_session_factory, event.id)
        assert stored.cost_usd == Decimal("0.00600000")
