"""Tests for services.cost_flusher (flush_costs + pricing + budget debit)."""

import uuid
from datetime import UTC, datetime
from decimal import Decimal

import pytest
from prometheus_client import REGISTRY
from sqlalchemy import func, select, text

import observability.metrics as om
from models.db import (
    NULL_LICENSE_ID,
    CostEvent,
    ProcessedCostMessage,
    ProviderPricing,
    TokenUsage,
)
from services.aggregator import (
    flush_costs,
)
from services.cost_flusher import PROJECT_ID_REFUSED_REASONS, _resolve_project_id
from tenancy import LEGACY_TENANT_NAMES, NIL_TENANT_ID, tenant_level_project_id
from tests._aggregator_helpers import _make_cost
from tests.conftest import T
from timeutil import current_week_start as _current_week_start


class TestFlushCosts:
    @pytest.mark.unit
    async def test_flush_empty_buffer(self, db_session_factory):
        """Empty buffer should be a no-op."""
        result = await flush_costs([], db_session_factory, None)
        assert result == 0

        async with db_session_factory() as session:
            count = (await session.execute(select(func.count()).select_from(CostEvent))).scalar()
            assert count == 0

    @pytest.mark.unit
    async def test_flush_costs_inserts_rows(self, db_session_factory):
        """Cost events should produce CostEvent rows with parsed numeric fields."""
        lic_id = str(uuid.uuid4())

        buffer = [
            _make_cost(
                tenant_id=T("test-tenant"),
                license_id=lic_id,
                provider="claude",
                model="claude-sonnet-4-20250514",
                input_tokens="1000",
                output_tokens="200",
                cost_usd="0.0165",
                latency_ms="350",
            ),
        ]

        count = await flush_costs(buffer, db_session_factory, None)
        assert count == 1
        assert len(buffer) == 0

        async with db_session_factory() as session:
            rows = (
                (
                    await session.execute(
                        select(CostEvent).where(CostEvent.tenant_id == T("test-tenant"))
                    )
                )
                .scalars()
                .all()
            )
            assert len(rows) == 1
            row = rows[0]
            assert row.provider == "claude"
            assert row.input_tokens == 1000
            assert row.output_tokens == 200
            # No pricing row registered for this provider → authoritative
            # cost is NULL but the hint from the event payload is preserved
            # on cost_usd_hint. (Phase 4: metering owns authoritative cost.)
            assert row.cost_usd is None
            assert row.cost_usd_hint == Decimal("0.0165")
            assert row.pricing_id is None
            assert row.latency_ms == 350

    @pytest.mark.unit
    async def test_flush_costs_token_usage_upsert(self, db_session_factory):
        """flush_costs should upsert TokenUsage rows for monthly + weekly periods."""
        lic_id = str(uuid.uuid4())

        buffer = [
            _make_cost(
                tenant_id=T("test-tenant"),
                license_id=lic_id,
                input_tokens="500",
                output_tokens="100",
            ),
        ]

        await flush_costs(buffer, db_session_factory, None)

        async with db_session_factory() as session:
            rows = (await session.execute(select(TokenUsage))).scalars().all()
            # Should have at least 2 rows: monthly and weekly
            assert len(rows) >= 2
            periods = {r.period_type for r in rows}
            assert "monthly" in periods
            assert "weekly" in periods
            for r in rows:
                assert r.input_tokens == 500
                assert r.output_tokens == 100
                assert r.total_tokens == 600

    @pytest.mark.unit
    async def test_flush_costs_no_license(self, db_session_factory):
        """Raw CostEvent with empty license_id should store None (not sentinel).
        Sentinel is only used in rollup tables (CostDaily, TokenUsage)."""
        buffer = [
            {
                "tenant_id": T("test-tenant"),
                "license_id": "",
                "service_key": "inference-router",
                "provider": "Qwen3-4B-AWQ",
                "input_tokens": "500",
                "output_tokens": "100",
                "cost_usd": "0",
            },
        ]

        count = await flush_costs(buffer, db_session_factory, None)
        assert count == 1

        async with db_session_factory() as session:
            row = (await session.execute(select(CostEvent))).scalar_one()
            assert row.license_id is None  # raw events preserve None
            assert row.provider == "Qwen3-4B-AWQ"

        # But TokenUsage rows should use the sentinel for the same event
        async with db_session_factory() as session:
            token_rows = (await session.execute(select(TokenUsage))).scalars().all()
            # Same vacuity guard as above: with no rows the loop proves nothing,
            # so a flusher that stopped writing rollups would read as "sentinel
            # used correctly".
            assert {tr.period_type for tr in token_rows} == {"monthly", "weekly"}
            for tr in token_rows:
                assert tr.license_id == NULL_LICENSE_ID  # rollup uses sentinel

    @pytest.mark.unit
    async def test_flush_costs_debits_active_budget(self, db_session_factory, fake_redis):
        """flush_costs should cascade-debit the tenant's active TokenBudget rows."""
        from services.budget import create_budget

        lic_id = str(uuid.uuid4())

        # Seed an active budget for the tenant.
        async with db_session_factory() as s:
            budget = await create_budget(
                tenant_id=T("test-tenant"),
                budget_tokens=10_000,
                period="monthly",
                session=s,
            )
            # create_budget no longer commits — caller owns the transaction
            # boundary so audit-log writes can land in the same TX. The
            # downstream flush_costs uses its own session and won't see the
            # budget without a commit here.
            await s.commit()

        buffer = [
            _make_cost(
                license_id=lic_id,
                tenant_id=T("test-tenant"),
                input_tokens="1000",
                output_tokens="200",
            ),
        ]

        count = await flush_costs(buffer, db_session_factory, fake_redis)
        assert count == 1

        # DB consumed_tokens matches.
        async with db_session_factory() as s:
            from sqlalchemy import select

            from models.db import TokenBudget

            row = (
                await s.execute(select(TokenBudget).where(TokenBudget.id == budget.id))
            ).scalar_one()
            assert int(row.consumed_tokens) == 1200

    @pytest.mark.unit
    async def test_flush_costs_redis_no_budget_skips(self, db_session_factory, fake_redis):
        """Without an active budget, flush_costs still persists costs but logs skip."""
        buffer = [
            _make_cost(
                license_id="",
                tenant_id=T("tenant-x"),
                input_tokens="300",
                output_tokens="100",
            ),
        ]

        count = await flush_costs(buffer, db_session_factory, fake_redis)
        assert count == 1

        # The cost event was persisted.
        async with db_session_factory() as session:
            row = (await session.execute(select(CostEvent))).scalar_one()
            assert row.tenant_id == T("tenant-x")
            assert row.input_tokens == 300

    @pytest.mark.unit
    async def test_flush_costs_metadata(self, db_session_factory):
        """Extra metadata fields should be stored in the metadata JSON column."""
        buffer = [
            _make_cost(
                tenant_id=T("test-tenant"),
                schema_id="test-schema",
                job_id="job-123",
                fallback_used="true",
            ),
        ]

        count = await flush_costs(buffer, db_session_factory, None)
        assert count == 1

        async with db_session_factory() as session:
            row = (await session.execute(select(CostEvent))).scalar_one()
            assert row.extra_metadata is not None
            assert row.extra_metadata["schema_id"] == "test-schema"
            assert row.extra_metadata["job_id"] == "job-123"
            assert row.extra_metadata["fallback_used"] == "true"

    @pytest.mark.unit
    async def test_flush_costs_stage_stored_in_metadata(self, db_session_factory):
        """The per-operation `stage` is lifted into the metadata JSON column."""
        buffer = [_make_cost(tenant_id=T("test-tenant"), stage="script_generation")]

        count = await flush_costs(buffer, db_session_factory, None)
        assert count == 1

        async with db_session_factory() as session:
            row = (await session.execute(select(CostEvent))).scalar_one()
            assert row.extra_metadata is not None
            assert row.extra_metadata["stage"] == "script_generation"

    @pytest.mark.unit
    async def test_flush_costs_worker_name_survives_the_metadata_whitelist(
        self, db_session_factory
    ):
        """The ORIGINATING worker reaches the row, so spend can be traced to an app.

        inference-router serves every origin behind one ``service_key``, so
        ``cost_attribution_app_by_service_key`` cannot tell requirements' spend from
        kag's. Measured 2026-09-03 over 24h: 3,605 unattributed rows from one tenant
        are requirements work and 1,106 from another are kag/karma, all under
        ``service_key="inference-router"``. ``worker_name`` is the only field on the
        wire that separates them.

        It was being dropped: ``extra_meta`` is a WHITELIST, and a key the producer
        sends but the tuple omits vanishes silently while the row still looks correct.
        """
        buffer = [
            _make_cost(
                tenant_id=T("test-tenant"),
                worker_name="decision_quality",
                schema_id="test-schema",
            ),
        ]

        count = await flush_costs(buffer, db_session_factory, None)
        assert count == 1

        async with db_session_factory() as session:
            row = (await session.execute(select(CostEvent))).scalar_one()
            assert row.extra_metadata is not None
            # CONTROL: an already-whitelisted key. Passes before AND after the fix —
            # if this ever fails the harness is broken and the assertion below proves
            # nothing.
            assert row.extra_metadata["schema_id"] == "test-schema"
            # THE REGRESSION.
            assert row.extra_metadata["worker_name"] == "decision_quality"

    @pytest.mark.unit
    async def test_flush_costs_absent_worker_name_is_not_invented(self, db_session_factory):
        """A producer that sends no worker_name must not gain a fabricated one.

        Trading a silent drop for a silent invention would be worse: an origin that
        is genuinely unknown must stay distinguishable from one that is known.
        """
        buffer = [_make_cost(tenant_id=T("test-tenant"), schema_id="test-schema")]

        assert await flush_costs(buffer, db_session_factory, None) == 1

        async with db_session_factory() as session:
            row = (await session.execute(select(CostEvent))).scalar_one()
            assert row.extra_metadata is not None
            assert row.extra_metadata["schema_id"] == "test-schema"  # CONTROL
            assert "worker_name" not in row.extra_metadata

    @pytest.mark.unit
    async def test_flush_costs_preserves_caller_client_id(self, db_session_factory):
        """The IMMEDIATE CALLING SERVICE must survive to the row.

        Measured 2026-09-04: no emitter puts a caller on the wire, so every
        service-to-service cost event in the ledger is anonymous as to who called —
        which is why an app-to-app hop gets billed to the service that emits rather
        than to the one that asked. worker_management already holds this value in a
        ContextVar and never writes it.

        This assertion is the receiving half, and it lands BEFORE any producer sends
        the key: ``extra_meta`` is a whitelist, so a producer shipping first would
        have the field dropped silently while the row still looked correct.
        """
        buffer = [
            _make_cost(
                tenant_id=T("test-tenant"),
                caller_client_id="svc-requirements",
                schema_id="test-schema",
            ),
        ]

        assert await flush_costs(buffer, db_session_factory, None) == 1

        async with db_session_factory() as session:
            row = (await session.execute(select(CostEvent))).scalar_one()
            assert row.extra_metadata is not None
            # CONTROL: an already-whitelisted key. If this ever fails the harness is
            # broken and the assertion below proves nothing.
            assert row.extra_metadata["schema_id"] == "test-schema"
            # THE REGRESSION.
            assert row.extra_metadata["caller_client_id"] == "svc-requirements"

    @pytest.mark.unit
    async def test_flush_costs_absent_caller_client_id_is_not_invented(self, db_session_factory):
        """An unknown caller must stay distinguishable from a known one.

        Trading a silent drop for a silent invention would be worse: most of today's
        traffic genuinely has no caller on the wire, and a fabricated one would make
        that indistinguishable from a hop that really did identify itself.
        """
        buffer = [_make_cost(tenant_id=T("test-tenant"), schema_id="test-schema")]

        assert await flush_costs(buffer, db_session_factory, None) == 1

        async with db_session_factory() as session:
            row = (await session.execute(select(CostEvent))).scalar_one()
            assert row.extra_metadata is not None
            assert row.extra_metadata["schema_id"] == "test-schema"  # CONTROL
            assert "caller_client_id" not in row.extra_metadata

    @pytest.mark.unit
    async def test_flush_costs_preserves_spend_scope(self, db_session_factory):
        """The emitter's own statement of WHAT this spend is attributed to.

        ``spend_scope='tenant'`` means "this cost belongs to the whole tenant" —
        a COMPLETE attribution, not a coverage gap. It is what
        ``attribution_audit._PROJECT_SPEND_COVERAGE_SQL`` reads to separate the
        ``tenant_scoped`` bucket of ``billing_cost_events_project_spend_usd``
        from genuinely unattributed spend.

        ⛔ AND THE RECEIVING HALF LANDS FIRST, exactly as it did for
        ``caller_client_id`` above and for the same mechanical reason:
        inference-router writes ``spend_scope`` as a flat field on the cost-bus
        event, ``extra_meta`` is a WHITELIST, and a key the producer sends but
        ``COST_METADATA_WHITELIST`` omits vanishes silently while the row still
        looks correct. Without this the coverage query's ``tenant_scoped`` bucket
        would read $0 forever and be indistinguishable from "no emitter ever sent
        one".
        """
        buffer = [
            _make_cost(
                tenant_id=T("test-tenant"),
                spend_scope="tenant",
                schema_id="test-schema",
            ),
        ]

        assert await flush_costs(buffer, db_session_factory, None) == 1

        async with db_session_factory() as session:
            row = (await session.execute(select(CostEvent))).scalar_one()
            assert row.extra_metadata is not None
            # CONTROL: an already-whitelisted key. If this ever fails the harness
            # is broken and the assertion below proves nothing.
            assert row.extra_metadata["schema_id"] == "test-schema"
            # THE REGRESSION.
            assert row.extra_metadata["spend_scope"] == "tenant"
            # ⭐ And it is readable by the exact expression the audit uses — the
            # column is `json`, and `metadata->>'spend_scope'` is what
            # _PROJECT_SPEND_COVERAGE_SQL's tenant_scoped FILTER compares. A
            # dict assertion alone would pass for a shape that query cannot read.
            scope = (
                await session.execute(
                    text("SELECT metadata->>'spend_scope' AS s FROM billing_cost_events")
                )
            ).one()
            assert scope.s == "tenant"

    @pytest.mark.unit
    async def test_flush_costs_absent_spend_scope_is_not_invented(self, db_session_factory):
        """No coercion, no default — ``absent`` is a real and correct state.

        Most traffic genuinely has no scope on the wire today. Defaulting it to
        ``'tenant'`` would move every such row out of the ``unattributed``
        coverage bucket and into ``tenant_scoped``, i.e. it would report an
        attribution the producer never made — the ``job_id = "unknown"`` defect
        again (968 of 968 rows carrying a fabricated value that every reader
        treated as populated), in a column that now decides a money split.
        """
        buffer = [_make_cost(tenant_id=T("test-tenant"), schema_id="test-schema")]

        assert await flush_costs(buffer, db_session_factory, None) == 1

        async with db_session_factory() as session:
            row = (await session.execute(select(CostEvent))).scalar_one()
            assert row.extra_metadata is not None
            assert row.extra_metadata["schema_id"] == "test-schema"  # CONTROL
            assert "spend_scope" not in row.extra_metadata

    @pytest.mark.unit
    async def test_spend_scope_is_counted_per_emitter_whether_sent_or_not(self) -> None:
        """⭐ Membership in the whitelist BIRTHS a per-service_key instrument.

        ``PLACEHOLDER_COUNTED_FIELDS`` is derived from ``COST_METADATA_WHITELIST``,
        so adding the key gives
        ``billing_placeholder_value_total{field="spend_scope", service_key=…}``
        for free, and the flusher's existing loop counts BOTH halves — a sentinel
        value AND absence. That is the only thing that can answer "is this emitter
        actually sending a scope?", which is the question behind any claim that
        the project-coverage split is complete.

        ⛔ MUTATION CONTROL: remove ``"spend_scope"`` from the whitelist and this
        fails on the derivation while ``test_flush_costs_preserves_spend_scope``
        fails on the storage — two halves of the same omission. ⚠️ MEASURED, and
        only TWO of the three fail: ``..._absent_spend_scope_is_not_invented``
        still PASSES under the mutation, because "the key is not in metadata" is
        true both when it is dropped and when it was never sent. That test is a
        guard against a future default, not a detector of this omission, and it
        must not be counted as one.
        """
        from services._parsers import COST_METADATA_WHITELIST, PLACEHOLDER_COUNTED_FIELDS

        assert "spend_scope" in COST_METADATA_WHITELIST
        # DERIVED, not hand-copied — the whole reason the key goes on the tuple
        # rather than into a special case in the flusher.
        assert "spend_scope" in PLACEHOLDER_COUNTED_FIELDS

    @pytest.mark.unit
    async def test_caller_client_id_and_worker_name_are_kept_apart(self, db_session_factory):
        """They answer different questions and must not collapse into each other.

        ``worker_name`` says whose WORK this is (the origin); ``caller_client_id``
        says who made THIS hop. On requirements -> worker-management ->
        inference-router they differ at every hop, and folding one into the other
        would silently re-introduce exactly the ambiguity this whitelist entry
        exists to remove.
        """
        buffer = [
            _make_cost(
                tenant_id=T("test-tenant"),
                worker_name="decision_quality",
                caller_client_id="svc-requirements",
            ),
        ]

        assert await flush_costs(buffer, db_session_factory, None) == 1

        async with db_session_factory() as session:
            row = (await session.execute(select(CostEvent))).scalar_one()
            assert row.extra_metadata is not None
            assert row.extra_metadata["worker_name"] == "decision_quality"
            assert row.extra_metadata["caller_client_id"] == "svc-requirements"
            assert row.extra_metadata["worker_name"] != row.extra_metadata["caller_client_id"]

    @pytest.mark.unit
    async def test_flush_costs_accumulates_tokens(self, db_session_factory):
        """Multiple costs for same license in one batch should sum in TokenUsage."""
        lic_id = str(uuid.uuid4())

        buffer = [
            _make_cost(
                tenant_id=T("test-tenant"),
                license_id=lic_id,
                input_tokens="100",
                output_tokens="50",
            ),
            _make_cost(
                tenant_id=T("test-tenant"),
                license_id=lic_id,
                input_tokens="200",
                output_tokens="30",
            ),
        ]

        await flush_costs(buffer, db_session_factory, None)

        async with db_session_factory() as session:
            rows = (
                (
                    await session.execute(
                        select(TokenUsage).where(TokenUsage.period_type == "monthly")
                    )
                )
                .scalars()
                .all()
            )
            assert len(rows) == 1
            assert rows[0].input_tokens == 300  # 100 + 200
            assert rows[0].output_tokens == 80  # 50 + 30
            assert rows[0].total_tokens == 380

    @pytest.mark.unit
    async def test_flush_costs_accumulates_tokens_across_batches(self, db_session_factory):
        """Two SEPARATE flushes into the same period must sum, not over-count.

        This is the ordinary production shape — the flusher drains the stream
        every few seconds, so a tenant's monthly row is written once and then
        UPDATEd thousands of times.

        It is also the only test that reaches the ``ON CONFLICT DO UPDATE``
        accumulate branch at cost_flusher.py:376. ``token_counts`` is aggregated
        in Python *before* the upsert, so a single ``flush_costs`` call is always
        a conflict-free INSERT: the in-batch test above proves the Python
        aggregation, never the SQL one. Without this test, doubling the
        ``excluded.*`` increment is invisible to the entire suite while
        over-counting every event after the first — which is the exact
        arithmetic of double-billing.
        """
        lic_id = str(uuid.uuid4())

        await flush_costs(
            [
                _make_cost(
                    tenant_id=T("test-tenant"),
                    license_id=lic_id,
                    input_tokens="100",
                    output_tokens="50",
                )
            ],
            db_session_factory,
            None,
        )
        await flush_costs(
            [
                _make_cost(
                    tenant_id=T("test-tenant"),
                    license_id=lic_id,
                    input_tokens="200",
                    output_tokens="30",
                )
            ],
            db_session_factory,
            None,
        )

        async with db_session_factory() as session:
            rows = (
                (
                    await session.execute(
                        select(TokenUsage).where(TokenUsage.period_type == "monthly")
                    )
                )
                .scalars()
                .all()
            )
            # The second flush must UPDATE the first row, not insert a second.
            assert len(rows) == 1
            assert rows[0].input_tokens == 300  # 100 + 200, NOT 100 + 400
            assert rows[0].output_tokens == 80  # 50 + 30, NOT 50 + 60
            assert rows[0].total_tokens == 380

    @pytest.mark.unit
    async def test_flush_costs_model_stored(self, db_session_factory):
        """Model field should be stored on the CostEvent row."""
        buffer = [_make_cost(tenant_id=T("test-tenant"), model="gpt-4o-mini")]
        await flush_costs(buffer, db_session_factory, None)

        async with db_session_factory() as session:
            row = (await session.execute(select(CostEvent))).scalar_one()
            assert row.model == "gpt-4o-mini"

    @pytest.mark.unit
    async def test_flush_costs_no_metadata_when_empty(self, db_session_factory):
        """When no extra metadata fields are present, metadata column should be None."""
        buffer = [_make_cost(tenant_id=T("test-tenant"))]  # No schema_id, job_id, or fallback_used
        await flush_costs(buffer, db_session_factory, None)

        async with db_session_factory() as session:
            row = (await session.execute(select(CostEvent))).scalar_one()
            assert row.extra_metadata is None

    @pytest.mark.unit
    async def test_flush_costs_latency_ms_zero_stored_as_none(self, db_session_factory):
        """latency_ms of 0 should be stored as None (the `or None` logic)."""
        buffer = [_make_cost(tenant_id=T("test-tenant"), latency_ms="0")]
        await flush_costs(buffer, db_session_factory, None)

        async with db_session_factory() as session:
            row = (await session.execute(select(CostEvent))).scalar_one()
            assert row.latency_ms is None

    @pytest.mark.unit
    async def test_flush_costs_absent_numeric_fields_default_to_zero(self, db_session_factory):
        """ABSENT/empty optional numeric fields default to 0 and the event is still
        billed — only GARBAGE numeric values are dead-lettered (see
        TestFlushCostsDeadLetter). An event that omits cache-token counts and the
        cost hint is valid, not malformed: empty ("") is skipped by
        validate_cost_event, whereas "not-a-number" is quarantined.
        """
        buffer = [
            _make_cost(
                tenant_id=T("test-tenant"),
                input_tokens="100",
                output_tokens="50",
                cost_usd="",  # empty hint → Decimal("0"), NOT malformed
                cache_creation_input_tokens="",
                cache_read_input_tokens="",
            ),
        ]
        count = await flush_costs(buffer, db_session_factory, None)
        assert count == 1

        async with db_session_factory() as session:
            row = (await session.execute(select(CostEvent))).scalar_one()
            assert row.input_tokens == 100
            assert row.cache_creation_input_tokens == 0
            assert row.cache_read_input_tokens == 0
            # Phase 4: with no pricing row registered, authoritative cost is
            # NULL; the empty hint parses to Decimal("0").
            assert row.cost_usd is None
            assert row.cost_usd_hint == Decimal("0")

    @pytest.mark.unit
    async def test_flush_costs_multiple_providers(self, db_session_factory):
        """Costs from different providers should all be inserted."""
        buffer = [
            _make_cost(
                tenant_id=T("test-tenant"),
                provider="claude",
                input_tokens="100",
                output_tokens="50",
            ),
            _make_cost(
                tenant_id=T("test-tenant"),
                provider="openai",
                input_tokens="200",
                output_tokens="100",
            ),
            _make_cost(
                tenant_id=T("test-tenant"),
                provider="Qwen3-4B-AWQ",
                input_tokens="50",
                output_tokens="20",
            ),
        ]

        count = await flush_costs(buffer, db_session_factory, None)
        assert count == 3

        async with db_session_factory() as session:
            rows = (await session.execute(select(CostEvent))).scalars().all()
            providers = {r.provider for r in rows}
            assert providers == {"claude", "openai", "Qwen3-4B-AWQ"}

    @pytest.mark.unit
    async def test_flush_costs_weekly_token_usage_created(self, db_session_factory):
        """flush_costs should create both monthly and weekly TokenUsage entries."""
        lic_id = str(uuid.uuid4())
        buffer = [
            _make_cost(
                tenant_id=T("test-tenant"),
                license_id=lic_id,
                input_tokens="100",
                output_tokens="50",
            )
        ]
        await flush_costs(buffer, db_session_factory, None)

        async with db_session_factory() as session:
            weekly = (
                await session.execute(select(TokenUsage).where(TokenUsage.period_type == "weekly"))
            ).scalar_one()
            assert weekly.input_tokens == 100
            assert weekly.output_tokens == 50
            assert weekly.total_tokens == 150
            assert weekly.period_start == _current_week_start()

    @pytest.mark.unit
    async def test_flush_costs_no_legacy_tenant_weekly_key(self, db_session_factory, fake_redis):
        """The legacy `tokens:tenant:{id}:week:YYYY-WW` key is no longer written."""
        buffer = [
            _make_cost(tenant_id=T("my-tenant"), input_tokens="300", output_tokens="100"),
        ]
        await flush_costs(buffer, db_session_factory, fake_redis)

        week = _current_week_start()
        iso = week.isocalendar()
        legacy = f"tokens:tenant:{T('my-tenant')}:week:{iso[0]}-{iso[1]:02d}"
        assert await fake_redis.get(legacy) is None


# ===========================================================================
# cleanup_old_events
# ===========================================================================


class TestFlushCostsEdge:
    @pytest.mark.unit
    async def test_flush_costs_all_invalid_returns_zero(self, db_session_factory):
        """Buffer is non-empty but produces zero rows after clear -> early return 0.

        The flush_costs function always appends to rows (it doesn't filter like
        flush_events does), so the `if not rows: return 0` branch at line 198
        can only be hit if the batch list itself is empty after clearing. Since the
        function does `batch = list(costs); costs.clear()` and then iterates batch,
        the only way to get an empty rows list is an empty batch. But that's caught
        by the `if not costs: return 0` check. So the branch is defensive code.
        We test it by verifying the normal path works.
        """
        buffer = [_make_cost(tenant_id=T("test-tenant"))]
        count = await flush_costs(buffer, db_session_factory, None)
        assert count == 1
        assert len(buffer) == 0

    @pytest.mark.unit
    async def test_flush_costs_service_key_defaults(self, db_session_factory):
        """A missing service_key defaults to the honest 'unknown' sentinel, NOT
        the fabricated 'inference-router' routing slug (billing_cost_events.
        service_key is NOT NULL, so it must be *some* value — 'unknown' is the
        same sentinel provider/tenant/cost_source use when provenance is absent).
        """
        buffer = [
            {
                "tenant_id": T("t"),
                "license_id": str(uuid.uuid4()),
                "provider": "claude",
                "input_tokens": "10",
                "output_tokens": "5",
                "cost_usd": "0.001",
            },
        ]
        count = await flush_costs(buffer, db_session_factory, None)
        assert count == 1

        async with db_session_factory() as session:
            row = (await session.execute(select(CostEvent))).scalar_one()
            # "unknown" is a sentinel, not a credential (detect-secrets false-positive).
            assert row.service_key == "unknown"  # pragma: allowlist secret

    @pytest.mark.unit
    async def test_flush_costs_missing_provider_dropped_without_dlq(self, db_session_factory):
        """A provider-less event can no longer be priced or repriced, so it is NOT
        billed as provider='unknown' (the old fail-open). With no dead_letter_stream
        configured (redis_conn=None) it is simply dropped; TestFlushCostsDeadLetter
        covers the quarantine path when a DLQ stream IS configured.
        """
        buffer = [
            {
                "tenant_id": T("t"),
                "license_id": str(uuid.uuid4()),
                "input_tokens": "10",
                "output_tokens": "5",
                "cost_usd": "0.001",
            },
        ]
        count = await flush_costs(buffer, db_session_factory, None)  # dead_letter_stream=None
        assert count == 0

        async with db_session_factory() as session:
            n = (await session.execute(select(func.count()).select_from(CostEvent))).scalar()
            assert n == 0  # dropped, not silently billed as provider='unknown'

    @pytest.mark.unit
    async def test_flush_costs_missing_tenant_dropped_without_dlq(self, db_session_factory):
        """A tenant-less cost event is NOT written to the nil tenant any more.

        CONTRACT CHANGE (2026-08-26, was ``test_flush_costs_tenant_defaults``):
        this used to assert the row landed with ``tenant_id == NIL_TENANT_ID``
        and the honest label "unknown" in the display column. Writing it was the
        bug: the flusher then EXCLUDED the nil tenant from the token rollup and
        the budget debit, so the row was recorded and billed to nobody — 22,226
        such rows / $353.09 on the live table, still growing today. The event is
        unbillable, so it is now quarantined like any other malformed event.

        Mirrors ``test_flush_costs_missing_provider_dropped_without_dlq``: with
        no dead_letter_stream configured (redis_conn=None) it is simply dropped;
        ``TestFlushCostsDeadLetter`` covers the quarantine path.

        It also no longer needs ``@pytest.mark.nil_tenant`` — it writes no
        nil-tenant row at all now, which is the whole point.
        """
        buffer = [
            {
                "license_id": str(uuid.uuid4()),
                "provider": "claude",
                "input_tokens": "10",
                "output_tokens": "5",
                "cost_usd": "0.001",
            },
        ]
        count = await flush_costs(buffer, db_session_factory, None)
        assert count == 0

        async with db_session_factory() as session:
            n = (await session.execute(select(func.count()).select_from(CostEvent))).scalar()
            assert n == 0  # NOT a nil-tenant row that nobody is billed for


# ===========================================================================
# Per-user metering: subject_id grain (Phase 3)
# ===========================================================================


class TestTenantIdGuard:
    """An unattributable tenant_id is QUARANTINED, never persisted.

    CONTRACT CHANGE (2026-08-26). This class used to assert that a leaked
    credential and the reserved nil uuid were WRITTEN as cost rows under the
    unattributed sentinel and merely excluded from billing side effects. That
    exclusion is what made the spend disappear: 22,226 live rows / $353.09
    recorded against the nil tenant and billed to nobody, 2026-05-01 → today.
    ``_cost_schema.validate_cost_event`` now rejects such an event, so it is
    dead-lettered instead of silently absorbed. Neither test needs
    ``@pytest.mark.nil_tenant`` any more — neither writes a nil-tenant row.

    A REGISTERED legacy name is NOT affected: it resolves to a real tenant uuid
    and bills normally, which the third test pins.
    """

    DLQ = "billing:costs:dead"

    @pytest.mark.unit
    async def test_bearer_token_tenant_id_is_dead_lettered_and_redacted(
        self, db_session_factory, fake_redis
    ):
        """A leaked auth header sent as tenant_id is quarantined, not billed —
        and the secret must not survive into the quarantine record either.

        The redaction assertions moved WITH the value: the credential used to be
        redacted on its way into the ``tenant_name`` Postgres column, and the
        only place it can now land is the dead-letter stream, which copies raw
        wire fields verbatim. Without the guard in the flusher's DLQ payload
        loop, this event would put a replayable token into a Redis stream that
        operators read and that retains for 30 days.
        """
        # A misconfigured emitter sends its raw Authorization header as tenant_id.
        leaked = "Bearer TESTTOKEN0000000000000000000000000000000000"
        buffer = [_make_cost(license_id="", tenant_id=leaked, _msg_id="90-1")]

        count = await flush_costs(
            buffer, db_session_factory, fake_redis, dead_letter_stream=self.DLQ
        )

        assert count == 0
        async with db_session_factory() as session:
            n = (await session.execute(select(func.count()).select_from(CostEvent))).scalar()
            assert n == 0  # not persisted at all — nil-tenant row included
            token_rows = (await session.execute(select(TokenUsage))).scalars().all()
            assert token_rows == []

        entries = await fake_redis.xrange(self.DLQ)
        assert len(entries) == 1
        _id, fields = entries[0]
        # A credential is unresolvable, so it fails as an invalid tenant rather
        # than as the nil one — the reason has to name the real defect for the
        # operator who reads the DLQ.
        assert fields["_dlq_reason"] == "invalid_tenant_id"

        # Asserted as PROPERTIES of tenancy.redact_tenant_id rather than as its
        # exact output, so a cosmetic change to the label (the U+2026 ellipsis,
        # the comma spacing — presentation shared with auth_deps) does not break
        # this test while the one change that actually leaks (widening the
        # preserved prefix) still does.
        label = fields["tenant_id"]
        # 1. The diagnostic prefix survives, so an operator can see WHICH
        #    emitter is leaking and that it is an auth header at all.
        assert label.startswith(leaked[:8])
        # 2. The original length is recorded, so a redacted label is
        #    distinguishable from a genuinely short tenant name.
        assert label.endswith(f"(redacted,len={len(leaked)})")
        # 3. The secret is truncated: neither the whole token, nor the body
        #    after the preserved prefix, nor even a 16-character head of it
        #    survives. NB the scheme word "Bearer" IS deliberately still
        #    present — it is the first six characters of the kept prefix and
        #    carries no secret; the replayable material is what this forbids.
        assert leaked not in label
        assert leaked[8:] not in label
        assert leaked[:16] not in label
        # And nowhere else in the record either: a leaking emitter is known to
        # send the header in tenant_name as well.
        assert all(leaked[:16] not in str(v) for v in fields.values())

    @pytest.mark.unit
    async def test_legacy_slug_tenant_id_resolves_to_registered_uuid(self, db_session_factory):
        """A REGISTERED legacy slug is resolved and BILLED — not quarantined.

        This is the boundary of the new guard, and the reason it delegates to
        ``tenancy.canonical_tenant_id`` instead of demanding a literal uuid on
        the wire: "n8n-dev" is a real tenant that the registry holds a uuid for.
        Rejecting it would quarantine attributable, billable traffic — trading
        one silent loss for a loud one.
        """
        buffer = [_make_cost(license_id="", tenant_id="n8n-dev")]
        count = await flush_costs(buffer, db_session_factory, None)
        assert count == 1
        expected = LEGACY_TENANT_NAMES["n8n-dev"]
        async with db_session_factory() as session:
            row = (await session.execute(select(CostEvent))).scalar_one()
            assert row.tenant_id == expected
            assert row.tenant_name == "n8n-dev"
            token_rows = (await session.execute(select(TokenUsage))).scalars().all()
            assert len(token_rows) >= 1
            assert all(r.tenant_id == expected for r in token_rows)
            assert all(r.tenant_name == "n8n-dev" for r in token_rows)

    @pytest.mark.unit
    async def test_nil_uuid_tenant_id_is_dead_lettered_not_billed(
        self, db_session_factory, fake_redis
    ):
        """The nil uuid — the emitters' own "I have no tenant" fallback — is
        quarantined rather than written as an unbilled row.

        This is the single largest contributor to the live blast radius: the nil
        uuid is what an emitter sends by default when it cannot resolve a
        tenant, and every one of those events was recorded and never billed.
        """
        buffer = [_make_cost(license_id="", tenant_id=NIL_TENANT_ID, _msg_id="91-1")]

        count = await flush_costs(
            buffer, db_session_factory, fake_redis, dead_letter_stream=self.DLQ
        )

        assert count == 0
        async with db_session_factory() as session:
            n = (await session.execute(select(func.count()).select_from(CostEvent))).scalar()
            assert n == 0
            token_rows = (await session.execute(select(TokenUsage))).scalars().all()
            assert token_rows == []

        entries = await fake_redis.xrange(self.DLQ)
        assert len(entries) == 1
        assert entries[0][1]["_dlq_reason"] == "nil_tenant_id"


class TestResolveProjectIdPure:
    """Pure unit tests of ``_resolve_project_id`` -- no database, no metrics
    registry. This is the CORE logic of D1's ingest-side change; the DB-backed
    ``TestProjectIdRefusal`` class below proves it is actually wired into the
    row the flusher stores.
    """

    def test_an_ordinary_project_id_is_kept(self) -> None:
        project = str(uuid.uuid4())
        resolved, reason = _resolve_project_id(project, check_tenant=lambda _c: False)
        assert str(resolved) == project
        assert reason is None

    def test_missing_project_id_stays_none_and_the_tenant_check_is_never_called(self) -> None:
        calls: list[str] = []
        resolved, reason = _resolve_project_id("", check_tenant=lambda c: calls.append(c) or False)
        assert resolved is None
        assert reason is None
        assert calls == []  # nothing to classify when there is nothing to check

    def test_a_registered_tenant_id_is_refused_and_nulled(self) -> None:
        """⭐ MUTATION CONTROL. Comment out the `if is_tenant:` branch's
        `return None, "is_tenant_id"` (i.e. fall through to `return resolved,
        None` unconditionally) and this test goes RED — it is the one
        assertion in this file that a caller-supplied tenant id is nulled
        rather than kept. See the module docstring for the measured prod shape
        this closes (0195f48b-…-0005 / -0004 used as project_id under
        n8n-dev).
        """
        tenant_as_project = str(uuid.uuid4())
        resolved, reason = _resolve_project_id(tenant_as_project, check_tenant=lambda _c: True)
        assert resolved is None
        assert reason == "is_tenant_id"

    def test_the_tenant_check_receives_the_canonical_string_form(self) -> None:
        """`check_tenant` must see a normalised uuid string, not the raw wire
        value -- a caller sending upper-case or braced input must still match
        the registry's lower-case canonical ids."""
        seen: list[str] = []
        raw = "{" + str(uuid.uuid4()).upper() + "}"

        def _check(canon: str) -> bool:
            seen.append(canon)
            return False

        _resolve_project_id(raw, check_tenant=_check)
        # Two lookups since owner ruling 2026-10-07: the value itself, and the
        # tenant it would be the tenant-level project of. Both canonical.
        assert len(seen) == 2
        assert all(c == c.lower() and "{" not in c for c in seen)
        assert seen[0] == raw.strip("{}").lower()

    def test_own_tenant_level_project_id_is_kept_without_a_registry_lookup(self) -> None:
        """Owner ruling 2026-10-07. ⭐ MUTATION CONTROL: drop the own-tenant
        early return and the (always-True) check refuses it."""
        tenant = str(uuid.uuid4())
        calls: list[str] = []
        tl = tenant_level_project_id(tenant)
        resolved, reason = _resolve_project_id(
            tl, check_tenant=lambda c: calls.append(c) or True, tenant_id=tenant
        )
        assert str(resolved) == tl
        assert reason is None
        assert calls == []

    def test_another_tenants_tenant_level_project_id_is_refused(self) -> None:
        """Cross-tenant mislabel. ⭐ MUTATION CONTROL: drop
        `is_foreign_tenant_level` from the refusal and this goes RED."""
        own, other = str(uuid.uuid4()), str(uuid.uuid4())
        resolved, reason = _resolve_project_id(
            tenant_level_project_id(other),
            check_tenant=lambda c: c == other,
            tenant_id=own,
        )
        assert resolved is None
        assert reason == "is_tenant_id"

    def test_the_raw_own_tenant_id_is_still_refused(self) -> None:
        """Owner ruling 2026-10-07 keeps the raw-tenant-id guard, own tenant
        included (BillingProjectIdEqualsTenant stays meaningful)."""
        tenant = str(uuid.uuid4())
        resolved, reason = _resolve_project_id(
            tenant, check_tenant=lambda c: c == tenant, tenant_id=tenant
        )
        assert resolved is None
        assert reason == "is_tenant_id"

    def test_an_ordinary_project_is_kept_with_tenant_id_given(self) -> None:
        tenant, project = str(uuid.uuid4()), str(uuid.uuid4())
        resolved, reason = _resolve_project_id(
            project, check_tenant=lambda c: c == tenant, tenant_id=tenant
        )
        assert str(resolved) == project
        assert reason is None

    def test_a_faulting_tenant_check_keeps_the_value_and_names_the_reason(self) -> None:
        """⛔ REFUSE ONLY ON POSITIVE KNOWLEDGE. `check_tenant` raising must
        never be able to erase a caller's real project_id."""
        project = str(uuid.uuid4())

        def _boom(_canon: str) -> bool:
            raise RuntimeError("registry exploded")

        resolved, reason = _resolve_project_id(project, check_tenant=_boom)
        assert str(resolved) == project
        assert reason == "registry_unavailable"

    def test_reason_vocabulary_matches_the_metric_label_domain(self) -> None:
        """The reasons this function can return and the counter's pre-seeded
        label domain must never drift apart -- a reason this function invents
        without a matching pre-seeded series would be invisible until the
        first time it fires."""
        assert PROJECT_ID_REFUSED_REASONS == om.PROJECT_ID_REFUSED_REASONS


class TestProjectIdRefusal:
    """D1 (billing-licensing estate repair, round 16): a caller-supplied
    ``project_id`` that is ITSELF a registered tenant id is refused before the
    row is stored -- never the whole event, only the project attribution.

    Measured on prod 2026-09-17: two distinct tenant uuids were being used as
    project_id under a DIFFERENT payer tenant -- 0195f48b-...-0005 (129 rows)
    and -0004 (37 rows). The same-row ``project_id == tenant_id`` forged-project
    detector (``services.attribution_audit._PROJECT_SELF_TENANT_SQL`` /
    ``billing_project_id_equals_tenant_projects``) cannot see this class: it
    only compares a row against ITS OWN tenant_id, and these rows carry
    someone else's.
    """

    @pytest.mark.unit
    async def test_project_id_refused_when_it_is_a_tenant_id(self, db_session_factory):
        import observability.metrics as om

        payer = T("r16-payer")
        victim_tenant = T("r16-victim-tenant")  # a REAL registered tenant, used as project_id
        before = om.project_id_refused_total.labels(reason="is_tenant_id")._value.get()

        buffer = [_make_cost(license_id="", tenant_id=payer, project_id=victim_tenant)]
        count = await flush_costs(buffer, db_session_factory, None)

        assert count == 1
        async with db_session_factory() as session:
            row = (await session.execute(select(CostEvent))).scalar_one()
            assert row.project_id is None
        after = om.project_id_refused_total.labels(reason="is_tenant_id")._value.get()
        assert after - before == 1

    @pytest.mark.unit
    async def test_project_id_kept_when_it_is_an_ordinary_project(self, db_session_factory):
        """The negative control: an unregistered uuid is a perfectly ordinary
        project id and must survive untouched."""
        import observability.metrics as om

        payer = T("r16-payer-2")
        project = str(uuid.uuid4())  # not registered as a tenant anywhere
        before = om.project_id_refused_total.labels(reason="is_tenant_id")._value.get()

        buffer = [_make_cost(license_id="", tenant_id=payer, project_id=project)]
        count = await flush_costs(buffer, db_session_factory, None)

        assert count == 1
        async with db_session_factory() as session:
            row = (await session.execute(select(CostEvent))).scalar_one()
            assert str(row.project_id) == project
        after = om.project_id_refused_total.labels(reason="is_tenant_id")._value.get()
        assert after == before

    @pytest.mark.unit
    async def test_project_id_kept_when_the_registry_check_faults(
        self, db_session_factory, monkeypatch
    ):
        """⛔ REFUSE ONLY ON POSITIVE KNOWLEDGE. When the classification itself
        cannot answer, the caller's project_id survives unchanged and the fault
        is counted separately -- the same "do not close a gate on input you
        cannot trust" rule ``cost_events_unregistered_tenant_total``'s own
        arming criterion is built on. Only the ``project_id`` lookup is made to
        fault here; the row's own tenant_id check must keep working normally,
        which is why the stub delegates to the real function for every other
        input rather than replacing it outright.
        """
        import observability.metrics as om
        import services.cost_flusher as cost_flusher_module

        real_is_registered = cost_flusher_module.is_registered_tenant_id
        payer = T("r16-payer-3")
        project = str(uuid.uuid4())

        def _flaky(canon):
            if canon == project:
                raise RuntimeError("registry exploded")
            return real_is_registered(canon)

        monkeypatch.setattr(cost_flusher_module, "is_registered_tenant_id", _flaky)

        before = om.project_id_refused_total.labels(reason="registry_unavailable")._value.get()
        buffer = [_make_cost(license_id="", tenant_id=payer, project_id=project)]
        count = await flush_costs(buffer, db_session_factory, None)

        assert count == 1
        async with db_session_factory() as session:
            row = (await session.execute(select(CostEvent))).scalar_one()
            assert str(row.project_id) == project
        after = om.project_id_refused_total.labels(reason="registry_unavailable")._value.get()
        assert after - before == 1

    @pytest.mark.unit
    async def test_own_tenant_level_project_is_kept_and_counted_as_covered(
        self, db_session_factory
    ):
        """Owner ruling 2026-10-07: stored, not refused, and counted by the
        per-emitter coverage SQL (`_EMITTER_COVERAGE_SQL`) that
        `BillingAttributionCoverageBelowFloor` reads."""
        import observability.metrics as om
        from services import attribution_audit as aa

        tenant = T("r16-tenant-level-project")
        tl = tenant_level_project_id(tenant)
        assert tl is not None
        before = {
            r: om.project_id_refused_total.labels(reason=r)._value.get()
            for r in om.PROJECT_ID_REFUSED_REASONS
        }
        buffer = [_make_cost(license_id="", tenant_id=tenant, project_id=tl)]
        assert await flush_costs(buffer, db_session_factory, None) == 1

        async with db_session_factory() as session:
            row = (await session.execute(select(CostEvent))).scalar_one()
            assert str(row.project_id) == tl
            assert row.excluded_from_billing is False
            coverage = await aa.load_emitter_coverage(session, 24)
        assert coverage == [("inference-router", 1, 1, 0)]
        after = {
            r: om.project_id_refused_total.labels(reason=r)._value.get()
            for r in om.PROJECT_ID_REFUSED_REASONS
        }
        assert after == before

    @pytest.mark.unit
    async def test_another_tenants_tenant_level_project_is_refused(self, db_session_factory):
        import observability.metrics as om

        payer, other = T("r16-tl-payer"), T("r16-tl-other")
        other_tl = tenant_level_project_id(other)
        assert other_tl is not None
        before = om.project_id_refused_total.labels(reason="is_tenant_id")._value.get()
        buffer = [_make_cost(license_id="", tenant_id=payer, project_id=other_tl)]
        assert await flush_costs(buffer, db_session_factory, None) == 1
        async with db_session_factory() as session:
            row = (await session.execute(select(CostEvent))).scalar_one()
            assert row.project_id is None
        after = om.project_id_refused_total.labels(reason="is_tenant_id")._value.get()
        assert after - before == 1

    @pytest.mark.unit
    async def test_own_raw_tenant_id_as_project_is_still_refused(self, db_session_factory):
        tenant = T("r16-own-raw")
        buffer = [_make_cost(license_id="", tenant_id=tenant, project_id=tenant)]
        assert await flush_costs(buffer, db_session_factory, None) == 1
        async with db_session_factory() as session:
            row = (await session.execute(select(CostEvent))).scalar_one()
            assert row.project_id is None

    @pytest.mark.unit
    async def test_missing_project_id_stays_none(self, db_session_factory):
        """No project_id on the wire must stay None, not become a sentinel --
        the pre-existing contract this change must not disturb."""
        payer = T("r16-payer-4")
        buffer = [_make_cost(license_id="", tenant_id=payer)]
        count = await flush_costs(buffer, db_session_factory, None)
        assert count == 1
        async with db_session_factory() as session:
            row = (await session.execute(select(CostEvent))).scalar_one()
            assert row.project_id is None


class TestDeadLetterProducerAttribution:
    """WHO produced the quarantined events — the question the metric cannot answer.

    `cost_dead_letter_total` is labelled by `reason` only. The producing
    `service_key` exists nowhere outside the DLQ payload, so attributing a live
    arrival rate meant XRANGE-ing the stream with the metering Redis credential.
    On 2026-08-27 that gap sent an investigation after the wrong producer while
    events arrived at ~14/h.
    """

    DLQ = "billing:costs:dead"

    async def test_the_producing_service_key_is_logged(
        self, db_session_factory, fake_redis, capsys
    ):
        """The service_key and reason reach the log, with counts.

        ⚠️ `capsys`, not `caplog`. This service logs through **structlog**, which
        renders to stdout via its own processor chain rather than through the
        stdlib handlers `caplog` attaches to — so `caplog.text` is empty here even
        when the line is emitted perfectly. Asserting on `caplog` would have been a
        test that could never pass, and its mirror image (asserting absence on
        `caplog`) is a test that can never fail.
        """
        buffer = [
            _make_cost(
                license_id="", tenant_id=NIL_TENANT_ID, _msg_id="200-1", service_key="svc-alpha"
            ),
            _make_cost(
                license_id="", tenant_id=NIL_TENANT_ID, _msg_id="200-2", service_key="svc-alpha"
            ),
            _make_cost(
                license_id="", tenant_id=NIL_TENANT_ID, _msg_id="200-3", service_key="svc-beta"
            ),
        ]

        count = await flush_costs(
            buffer, db_session_factory, fake_redis, dead_letter_stream=self.DLQ
        )

        assert count == 0
        out = capsys.readouterr().out
        assert "cost_dead_letter_producers" in out
        assert "svc-alpha" in out
        assert "svc-beta" in out
        # Counts, not just names — 2 alpha vs 1 beta is what makes it actionable.
        assert "'count': 2" in out
        assert "'count': 1" in out

    async def test_nothing_is_logged_when_nothing_is_quarantined(
        self, db_session_factory, fake_redis, capsys
    ):
        """A clean flush stays quiet, or the signal is useless.

        Paired with the test above so the guard cannot silently become
        "never logs" — the failure mode a single positive test cannot see.
        """
        buffer = [_make_cost(tenant_id=T("test-tenant"), _msg_id="201-1")]

        await flush_costs(buffer, db_session_factory, fake_redis, dead_letter_stream=self.DLQ)

        assert "cost_dead_letter_producers" not in capsys.readouterr().out


class TestFlushCostsIdempotency:
    """At-least-once → exactly-once dedup on the Redis stream message id."""

    def _stream_cost(self, msg_id: str, **kw):
        # Mirror what the consumer tags onto each event before buffering.
        kw.setdefault("tenant_id", T("test-tenant"))
        return _make_cost(_msg_id=msg_id, _stream_name="improvement-loop:costs", **kw)

    @pytest.mark.unit
    async def test_redelivery_of_same_msg_id_is_deduped(self, db_session_factory):
        """A message redelivered in a later batch must not insert a second row."""
        lic = str(uuid.uuid4())
        first = await flush_costs(
            [self._stream_cost("1700000000000-0", license_id=lic)], db_session_factory, None
        )
        assert first == 1

        # Same Redis stream id again (crash-after-commit-before-XACK / NOGROUP replay).
        second = await flush_costs(
            [self._stream_cost("1700000000000-0", license_id=lic)], db_session_factory, None
        )
        assert second == 0

        async with db_session_factory() as session:
            n = (await session.execute(select(func.count()).select_from(CostEvent))).scalar()
            assert n == 1
            claimed = (
                await session.execute(select(func.count()).select_from(ProcessedCostMessage))
            ).scalar()
            assert claimed == 1

    @pytest.mark.unit
    async def test_mixed_batch_only_new_ids_persist(self, db_session_factory):
        """A batch mixing an already-processed id with a fresh one inserts only the fresh one."""
        await flush_costs([self._stream_cost("1700000000001-0")], db_session_factory, None)

        count = await flush_costs(
            [
                self._stream_cost("1700000000001-0"),  # duplicate
                self._stream_cost("1700000000002-0"),  # new
            ],
            db_session_factory,
            None,
        )
        assert count == 1

        async with db_session_factory() as session:
            n = (await session.execute(select(func.count()).select_from(CostEvent))).scalar()
            assert n == 2  # one from each flush, the duplicate dropped

    @pytest.mark.unit
    async def test_events_without_msg_id_always_insert(self, db_session_factory):
        """Direct (non-stream) events have no dedup key and are never dropped."""
        a = await flush_costs([_make_cost(tenant_id=T("test-tenant"))], db_session_factory, None)
        b = await flush_costs([_make_cost(tenant_id=T("test-tenant"))], db_session_factory, None)
        assert a == 1 and b == 1
        async with db_session_factory() as session:
            n = (await session.execute(select(func.count()).select_from(CostEvent))).scalar()
            assert n == 2
            claimed = (
                await session.execute(select(func.count()).select_from(ProcessedCostMessage))
            ).scalar()
            assert claimed == 0

    @pytest.mark.unit
    async def test_intra_batch_duplicate_msg_id_billed_once(self, db_session_factory, fake_redis):
        """The SAME stream msg id appearing TWICE in one batch (both newly claimed)
        must persist ONE cost row and debit the budget ONCE.

        Robust-by-construction dedup: the flusher collapses repeated msg ids
        before the idempotency claim, so a doubled id can never double-bill —
        it is defended by construction, not by the 300s XAUTOCLAIM idle window.

        MUTATION-PROOF: revert to the set-membership `survived` filter
        (`p[3] in claimed`) and BOTH copies pass — `claimed` holds the id once,
        but membership is true for both — inserting two cost rows and debiting
        the budget twice (2400). This test then fails on count / row-count /
        consumed_tokens.
        """
        from sqlalchemy import select as _select

        from models.db import TokenBudget
        from services.budget import create_budget

        async with db_session_factory() as s:
            budget = await create_budget(
                tenant_id=T("dup-tenant"), budget_tokens=100_000, period="monthly", session=s
            )
            await s.commit()

        dup_id = "1900000000000-0"
        batch = [
            self._stream_cost(
                dup_id,
                tenant_id=T("dup-tenant"),
                license_id=str(uuid.uuid4()),
                input_tokens="1000",
                output_tokens="200",
            ),
            self._stream_cost(
                dup_id,
                tenant_id=T("dup-tenant"),
                license_id=str(uuid.uuid4()),
                input_tokens="1000",
                output_tokens="200",
            ),
        ]

        count = await flush_costs(batch, db_session_factory, fake_redis)
        assert count == 1  # NOT 2 — the duplicate collapsed

        async with db_session_factory() as s:
            n_events = (await s.execute(select(func.count()).select_from(CostEvent))).scalar()
            assert n_events == 1
            row = (
                await s.execute(_select(TokenBudget).where(TokenBudget.id == budget.id))
            ).scalar_one()
            # Debited ONCE (1000+200), not twice (2400).
            assert int(row.consumed_tokens) == 1200

    @pytest.mark.unit
    async def test_redelivery_does_not_double_count_token_usage(self, db_session_factory):
        """Token-usage rollups must not double on a redelivered message."""
        lic = str(uuid.uuid4())
        await flush_costs(
            [
                self._stream_cost(
                    "1700000000003-0", license_id=lic, input_tokens="500", output_tokens="100"
                )
            ],
            db_session_factory,
            None,
        )
        await flush_costs(
            [
                self._stream_cost(
                    "1700000000003-0", license_id=lic, input_tokens="500", output_tokens="100"
                )
            ],
            db_session_factory,
            None,
        )
        async with db_session_factory() as session:
            rows = (await session.execute(select(TokenUsage))).scalars().all()
            # Assert the rollup EXISTS before asserting it was not doubled.
            # Without this, a flusher that writes nothing at all makes the loop
            # below iterate zero times and the test passes — i.e. "wrote nothing"
            # and "wrote each row exactly once" would be indistinguishable, and
            # the one test standing between us and double-billing could not fail
            # in the case that matters.
            assert {r.period_type for r in rows} == {"monthly", "weekly"}
            assert len(rows) == 2
            for r in rows:
                assert r.input_tokens == 500
                assert r.output_tokens == 100


class TestFlushCostsAtomicDebit:
    """The budget debit is folded into the idempotency-claim transaction.

    Fixes the drop-window where the claim + cost_events committed but the
    (separately-committed, separate-session) budget debit was lost on a crash
    or dropped on redelivery. The debit now commits or rolls back with the
    claim, so a redelivered batch debits exactly once and a crash-before-commit
    replays the debit exactly once.
    """

    def _stream_cost(self, msg_id: str, **kw):
        kw.setdefault("tenant_id", T("test-tenant"))
        return _make_cost(_msg_id=msg_id, _stream_name="improvement-loop:costs", **kw)

    @pytest.mark.unit
    async def test_redelivered_batch_debits_budget_exactly_once(
        self, db_session_factory, fake_redis
    ):
        """A redelivered stream message must not double-debit the budget.

        Before the fold, tenant_io was already deduped so the *survivors* were
        empty on redelivery — but the debit lived in a separate session that
        could be lost on crash. This test pins the exactly-once contract end to
        end: consumed_tokens reflects the tokens ONCE after an identical re-flush.
        """
        from sqlalchemy import select as _select

        from models.db import TokenBudget
        from services.budget import create_budget

        async with db_session_factory() as s:
            budget = await create_budget(
                tenant_id=T("atomic-tenant"),
                budget_tokens=100_000,
                period="monthly",
                session=s,
            )
            await s.commit()

        batch = [
            self._stream_cost(
                "1800000000000-0",
                tenant_id=T("atomic-tenant"),
                license_id=str(uuid.uuid4()),
                input_tokens="1000",
                output_tokens="200",
            )
        ]

        first = await flush_costs(list(batch), db_session_factory, fake_redis)
        assert first == 1

        async with db_session_factory() as s:
            row = (
                await s.execute(_select(TokenBudget).where(TokenBudget.id == budget.id))
            ).scalar_one()
            assert int(row.consumed_tokens) == 1200

        # Redelivery of the identical message: claim conflicts, survivors empty,
        # no cost row and — critically — no second debit.
        second = await flush_costs(list(batch), db_session_factory, fake_redis)
        assert second == 0

        async with db_session_factory() as s:
            row = (
                await s.execute(_select(TokenBudget).where(TokenBudget.id == budget.id))
            ).scalar_one()
            # Debited ONCE, not twice.
            assert int(row.consumed_tokens) == 1200
            # cost_events + claim dedup exactly as before.
            n_events = (await s.execute(select(func.count()).select_from(CostEvent))).scalar()
            assert n_events == 1
            n_claims = (
                await s.execute(select(func.count()).select_from(ProcessedCostMessage))
            ).scalar()
            assert n_claims == 1

    @pytest.mark.unit
    async def test_debit_atomic_with_claim_and_events(self, db_session_factory, fake_redis):
        """The debit and the cost_events insert land in the same commit.

        After a successful flush, the cost row, the token_usage rollup and the
        budget debit are ALL present — proving they committed together rather
        than in the old two-session split.
        """
        from sqlalchemy import select as _select

        from models.db import TokenBudget
        from services.budget import create_budget

        async with db_session_factory() as s:
            budget = await create_budget(
                tenant_id=T("atomic-tenant-2"),
                budget_tokens=50_000,
                period="monthly",
                session=s,
            )
            await s.commit()

        count = await flush_costs(
            [
                _make_cost(
                    tenant_id=T("atomic-tenant-2"),
                    license_id=str(uuid.uuid4()),
                    input_tokens="700",
                    output_tokens="300",
                )
            ],
            db_session_factory,
            fake_redis,
        )
        assert count == 1

        async with db_session_factory() as s:
            n_events = (await s.execute(select(func.count()).select_from(CostEvent))).scalar()
            assert n_events == 1
            row = (
                await s.execute(_select(TokenBudget).where(TokenBudget.id == budget.id))
            ).scalar_one()
            assert int(row.consumed_tokens) == 1000  # 700 + 300


class TestDebitCommitBoundary:
    """`debit_budget(commit=False)` stages into the caller's transaction."""

    @pytest.mark.unit
    async def test_commit_false_does_not_persist_until_caller_commits(
        self, db_session, db_session_factory
    ):
        """A commit=False debit is invisible to other sessions until commit."""
        from sqlalchemy import select as _select

        from models.db import TokenBudget
        from services.budget import create_budget, debit_budget

        budget = await create_budget(
            tenant_id=T("staged-tenant"), budget_tokens=10_000, period="monthly", session=db_session
        )
        await db_session.commit()

        debits = await debit_budget(db_session, None, T("staged-tenant"), 400, commit=False)
        assert debits and debits[0].debited_tokens == 400

        # Staged (flushed) but NOT committed → a separate session sees zero.
        async with db_session_factory() as other:
            row = (
                await other.execute(_select(TokenBudget).where(TokenBudget.id == budget.id))
            ).scalar_one()
            assert int(row.consumed_tokens) == 0

        # Caller commits → now durable.
        await db_session.commit()
        async with db_session_factory() as other:
            row = (
                await other.execute(_select(TokenBudget).where(TokenBudget.id == budget.id))
            ).scalar_one()
            assert int(row.consumed_tokens) == 400

    @pytest.mark.unit
    async def test_commit_false_rolls_back_with_caller(self, db_session, db_session_factory):
        """A commit=False debit rolls back when the caller's transaction does."""
        from sqlalchemy import select as _select

        from models.db import TokenBudget
        from services.budget import create_budget, debit_budget

        budget = await create_budget(
            tenant_id=T("rollback-tenant"),
            budget_tokens=10_000,
            period="monthly",
            session=db_session,
        )
        await db_session.commit()
        # Capture the PK before rollback: rollback() expires the ORM object, so
        # reading budget.id afterwards would fire a lazy reload in the wrong
        # greenlet. The value itself is unaffected by the rollback under test.
        budget_id = budget.id

        await debit_budget(db_session, None, T("rollback-tenant"), 500, commit=False)
        # Caller aborts the unit of work.
        await db_session.rollback()

        async with db_session_factory() as other:
            row = (
                await other.execute(_select(TokenBudget).where(TokenBudget.id == budget_id))
            ).scalar_one()
            assert int(row.consumed_tokens) == 0


class TestFlushCostsDeadLetter:
    """Malformed cost events must be quarantined to the dead-letter stream, never
    silently billed at $0. Each test crafts an event the lenient parsers would
    otherwise coerce into a free/NULL row and asserts it is dead-lettered instead
    — and that a valid event in the same batch is unaffected. These fail against
    the pre-guard code (which inserts the malformed row and returns it as billed).
    """

    DLQ = "billing:costs:dead"

    async def _dlq(self, fake_redis):
        return await fake_redis.xrange(self.DLQ)

    async def _cost_event_count(self, db_session_factory):
        async with db_session_factory() as session:
            return (await session.execute(select(func.count()).select_from(CostEvent))).scalar()

    @pytest.mark.unit
    async def test_non_numeric_tokens_dead_lettered(self, db_session_factory, fake_redis):
        """A non-numeric token count (silently billed $0 today) is quarantined."""
        import observability.metrics as om

        before = om.cost_dead_letter_total.labels(reason="invalid_input_tokens")._value.get()
        buffer = [_make_cost(tenant_id=T("test-tenant"), input_tokens="abc", _msg_id="10-1")]

        result = await flush_costs(
            buffer, db_session_factory, fake_redis, dead_letter_stream=self.DLQ
        )

        assert result == 0
        assert await self._cost_event_count(db_session_factory) == 0  # NOT a $0 row

        entries = await self._dlq(fake_redis)
        assert len(entries) == 1
        _id, fields = entries[0]
        assert fields["_dlq_reason"] == "invalid_input_tokens"
        assert fields["_dlq_original_id"] == "10-1"
        assert fields["provider"] == "claude"  # raw wire fields preserved
        assert "_msg_id" not in fields and "_stream_name" not in fields  # internals stripped

        after = om.cost_dead_letter_total.labels(reason="invalid_input_tokens")._value.get()
        assert after - before == 1

        async with db_session_factory() as session:
            claimed = (
                (await session.execute(select(ProcessedCostMessage.stream_msg_id))).scalars().all()
            )
            assert "10-1" in claimed  # claimed so a redelivery won't re-quarantine

    @pytest.mark.unit
    async def test_intra_batch_duplicate_malformed_dead_lettered_once(
        self, db_session_factory, fake_redis
    ):
        """A malformed event appearing TWICE in one batch (same _msg_id) is
        quarantined ONCE — not double-XADD'd to the dead-letter stream nor
        double-counted in cost_dead_letter_total. Mirrors the pre_rows intra-batch
        dedup for the dead-letter branch. Reverting the dead_letters dedup makes
        this quarantine twice."""
        import observability.metrics as om

        before = om.cost_dead_letter_total.labels(reason="invalid_input_tokens")._value.get()
        dup = _make_cost(tenant_id=T("test-tenant"), input_tokens="abc", _msg_id="dup-dead-1")
        buffer = [dict(dup), dict(dup)]  # same _msg_id twice in the same batch

        result = await flush_costs(
            buffer, db_session_factory, fake_redis, dead_letter_stream=self.DLQ
        )

        assert result == 0
        entries = await self._dlq(fake_redis)
        assert len(entries) == 1  # quarantined once, not twice
        after = om.cost_dead_letter_total.labels(reason="invalid_input_tokens")._value.get()
        assert after - before == 1

    @pytest.mark.unit
    async def test_negative_tokens_dead_lettered(self, db_session_factory, fake_redis):
        """int('-5') slips past _parse_int as a NEGATIVE count — quarantine it."""
        buffer = [_make_cost(tenant_id=T("test-tenant"), output_tokens="-5", _msg_id="10-2")]
        result = await flush_costs(
            buffer, db_session_factory, fake_redis, dead_letter_stream=self.DLQ
        )
        assert result == 0
        assert await self._cost_event_count(db_session_factory) == 0
        entries = await self._dlq(fake_redis)
        assert len(entries) == 1
        assert entries[0][1]["_dlq_reason"] == "negative_output_tokens"

    @pytest.mark.unit
    async def test_missing_provider_dead_lettered(self, db_session_factory, fake_redis):
        """A provider-less event cannot be priced — quarantine, don't NULL-bill."""
        buffer = [_make_cost(tenant_id=T("test-tenant"), provider="", _msg_id="10-3")]
        result = await flush_costs(
            buffer, db_session_factory, fake_redis, dead_letter_stream=self.DLQ
        )
        assert result == 0
        assert await self._cost_event_count(db_session_factory) == 0
        entries = await self._dlq(fake_redis)
        assert len(entries) == 1
        assert entries[0][1]["_dlq_reason"] == "missing_provider"

    @pytest.mark.unit
    async def test_missing_model_is_billed_not_dead_lettered(self, db_session_factory, fake_redis):
        """~41% of prod events carry no model; they price via the provider-default
        rung and MUST be inserted, never dead-lettered. Guards the boundary — this
        fails if a missing-model rule is (re)introduced into validate_cost_event.
        """
        buffer = [_make_cost(tenant_id=T("test-tenant"), model="", _msg_id="10-4")]
        result = await flush_costs(
            buffer, db_session_factory, fake_redis, dead_letter_stream=self.DLQ
        )
        assert result == 1  # inserted as a normal (NULL-cost) row
        assert await self._dlq(fake_redis) == []
        assert await self._cost_event_count(db_session_factory) == 1

    # -- tenant_id: an unattributable tenant is unbillable spend ---------------
    #
    # These four pin the contract added 2026-08-26. Before it, each of the first
    # three was INSERTED as a nil-tenant cost row and then excluded from the
    # token rollup and the budget debit — recorded, and billed to nobody. On the
    # live table that is 22,226 rows / $353.0916 between 2026-05-01 and
    # 2026-08-26, still accruing. The fourth is the anti-vacuity control: it
    # fails if the new guard is written broadly enough to quarantine real
    # traffic, which would trade a silent loss for a loud one.

    @pytest.mark.unit
    async def test_missing_tenant_id_dead_lettered(self, db_session_factory, fake_redis):
        """No tenant_id key at all → quarantined, NOT inserted."""
        import observability.metrics as om

        before = om.cost_dead_letter_total.labels(reason="missing_tenant_id")._value.get()
        buffer = [
            {
                "license_id": str(uuid.uuid4()),
                "service_key": "inference-router",
                "provider": "claude",
                "input_tokens": "1000",
                "output_tokens": "200",
                "cost_usd": "0.0165",
                "_msg_id": "12-1",
            }
        ]

        result = await flush_costs(
            buffer, db_session_factory, fake_redis, dead_letter_stream=self.DLQ
        )

        assert result == 0
        assert await self._cost_event_count(db_session_factory) == 0

        entries = await self._dlq(fake_redis)
        assert len(entries) == 1
        _id, fields = entries[0]
        assert fields["_dlq_reason"] == "missing_tenant_id"
        assert fields["_dlq_original_id"] == "12-1"
        assert fields["provider"] == "claude"  # raw wire fields preserved

        after = om.cost_dead_letter_total.labels(reason="missing_tenant_id")._value.get()
        assert after - before == 1

        # No rollup and no budget side effect either — the point of the change is
        # that nothing about this event is half-processed.
        async with db_session_factory() as session:
            assert (await session.execute(select(TokenUsage))).scalars().all() == []

    @pytest.mark.unit
    async def test_blank_tenant_id_dead_lettered(self, db_session_factory, fake_redis):
        """A present-but-blank tenant_id → quarantined, NOT inserted.

        Distinct from the absent case on purpose: the emitters that produce this
        send the key with an empty value rather than omitting it, and a guard
        written as a bare ``"tenant_id" in fields`` membership test would pass
        this straight through to the nil sink.
        """
        buffer = [_make_cost(tenant_id="   ", _msg_id="12-2")]

        result = await flush_costs(
            buffer, db_session_factory, fake_redis, dead_letter_stream=self.DLQ
        )

        assert result == 0
        assert await self._cost_event_count(db_session_factory) == 0

        entries = await self._dlq(fake_redis)
        assert len(entries) == 1
        assert entries[0][1]["_dlq_reason"] == "missing_tenant_id"

    @pytest.mark.unit
    async def test_nil_uuid_tenant_id_dead_lettered(self, db_session_factory, fake_redis):
        """The nil uuid → quarantined under its OWN reason, NOT inserted.

        It gets a reason of its own because it is a different operator problem
        from a garbled id: the nil uuid is a REAL registered tenant row
        (``default-anon``) and is what a correctly-functioning emitter sends when
        it genuinely cannot resolve a tenant. Collapsing it into
        ``invalid_tenant_id`` would hide the single largest bucket of unbilled
        spend inside a bucket labelled "corrupt".
        """
        import observability.metrics as om

        before = om.cost_dead_letter_total.labels(reason="nil_tenant_id")._value.get()
        buffer = [_make_cost(tenant_id=NIL_TENANT_ID, _msg_id="12-3")]

        result = await flush_costs(
            buffer, db_session_factory, fake_redis, dead_letter_stream=self.DLQ
        )

        assert result == 0
        assert await self._cost_event_count(db_session_factory) == 0

        entries = await self._dlq(fake_redis)
        assert len(entries) == 1
        assert entries[0][1]["_dlq_reason"] == "nil_tenant_id"

        after = om.cost_dead_letter_total.labels(reason="nil_tenant_id")._value.get()
        assert after - before == 1

        async with db_session_factory() as session:
            assert (await session.execute(select(TokenUsage))).scalars().all() == []

    @pytest.mark.unit
    async def test_real_uuid_tenant_still_inserts_and_debits(self, db_session_factory, fake_redis):
        """ANTI-VACUITY CONTROL. A real-uuid tenant must be unaffected.

        Asserts all three side effects the guard must NOT have broken: the cost
        row inserts, the token_usage rollup is written, and the tenant's active
        budget is debited. A guard that quarantines everything makes the three
        tests above pass and this one fail — which is the only reason those
        three mean anything.
        """
        from sqlalchemy import select as _select

        from models.db import TokenBudget
        from services.budget import create_budget

        tenant = T("dlq-control-tenant")
        async with db_session_factory() as s:
            budget = await create_budget(
                tenant_id=tenant, budget_tokens=100_000, period="monthly", session=s
            )
            await s.commit()

        buffer = [
            _make_cost(
                tenant_id=tenant,
                license_id=str(uuid.uuid4()),
                input_tokens="1000",
                output_tokens="200",
                _msg_id="12-4",
            )
        ]

        result = await flush_costs(
            buffer, db_session_factory, fake_redis, dead_letter_stream=self.DLQ
        )

        assert result == 1
        assert await self._dlq(fake_redis) == []  # NOT quarantined
        assert await self._cost_event_count(db_session_factory) == 1

        async with db_session_factory() as session:
            row = (await session.execute(select(CostEvent))).scalar_one()
            assert row.tenant_id == tenant

            # The rollup that the old nil-tenant exclusion used to skip.
            token_rows = (await session.execute(select(TokenUsage))).scalars().all()
            assert {tr.period_type for tr in token_rows} == {"monthly", "weekly"}
            assert all(tr.tenant_id == tenant for tr in token_rows)
            assert all(tr.total_tokens == 1200 for tr in token_rows)

            # The budget debit it used to skip.
            budget_row = (
                await session.execute(_select(TokenBudget).where(TokenBudget.id == budget.id))
            ).scalar_one()
            assert int(budget_row.consumed_tokens) == 1200

    @pytest.mark.unit
    async def test_mixed_batch_splits_valid_from_malformed(self, db_session_factory, fake_redis):
        """One malformed event must not drop the valid events sharing its batch."""
        buffer = [
            _make_cost(tenant_id=T("test-tenant"), _msg_id="11-1"),  # valid
            _make_cost(
                tenant_id=T("test-tenant"), input_tokens="oops", _msg_id="11-2"
            ),  # malformed
            _make_cost(tenant_id=T("test-tenant"), _msg_id="11-3"),  # valid
        ]
        result = await flush_costs(
            buffer, db_session_factory, fake_redis, dead_letter_stream=self.DLQ
        )
        assert result == 2  # the two valid rows
        assert await self._cost_event_count(db_session_factory) == 2
        entries = await self._dlq(fake_redis)
        assert len(entries) == 1
        assert entries[0][1]["_dlq_original_id"] == "11-2"

    @pytest.mark.unit
    async def test_dead_letter_idempotent_on_replay(self, db_session_factory, fake_redis):
        """A redelivered malformed message is deduped by its claim — quarantined once."""
        import observability.metrics as om

        dedup_before = om.cost_events_deduplicated_total._value.get()
        await flush_costs(
            [_make_cost(tenant_id=T("test-tenant"), input_tokens="nope", _msg_id="12-1")],
            db_session_factory,
            fake_redis,
            dead_letter_stream=self.DLQ,
        )
        # Same message id redelivered.
        await flush_costs(
            [_make_cost(tenant_id=T("test-tenant"), input_tokens="nope", _msg_id="12-1")],
            db_session_factory,
            fake_redis,
            dead_letter_stream=self.DLQ,
        )

        assert len(await self._dlq(fake_redis)) == 1  # not doubled
        assert om.cost_events_deduplicated_total._value.get() - dedup_before == 1


@pytest.mark.unit
class TestLicenceAttributionMetric:
    """`billing_license_unattributed_total` — the invoicing gap needs a number.

    License-less cost is billed and capped (it drains the tenant-level pool),
    but `CostDaily.license_attributed_clause` drops it from every license-keyed
    aggregate, so it can never reach a customer invoice line. Measured baseline
    when this was added (2026-08-11, 7d): 5,274 of 9,888 events — 53% of events,
    64% of tokens — carried no license_id, and nothing on the platform reported
    it. A gap with no metric reads as "covered".

    Asserted against SYNTHETIC events with a known license_id, never against
    whatever the flusher happened to return — a gate compared with itself is a
    tautology.
    """

    @staticmethod
    def _counter(service_key: str) -> float:
        import observability.metrics as om

        return om.license_unattributed_total.labels(service_key=service_key)._value.get()

    async def test_a_licence_less_event_increments_the_counter(self, db_session_factory):
        before = self._counter("inference-router")
        count = await flush_costs(
            [_make_cost(tenant_id=T("test-tenant"), license_id="")], db_session_factory, None
        )
        assert count == 1
        assert self._counter("inference-router") - before == 1

    async def test_a_licence_bearing_event_leaves_it_untouched(self, db_session_factory):
        """The other half — a counter that only ever goes up measures nothing."""
        before = self._counter("inference-router")
        count = await flush_costs(
            [_make_cost(tenant_id=T("test-tenant"), license_id=str(uuid.uuid4()))],
            db_session_factory,
            None,
        )
        assert count == 1
        assert self._counter("inference-router") - before == 0

    async def test_an_unparseable_licence_id_counts_as_unattributed(self, db_session_factory):
        """`_parse_uuid` maps garbage to None, and that spend is just as unbillable."""
        before = self._counter("inference-router")
        count = await flush_costs(
            [_make_cost(tenant_id=T("test-tenant"), license_id="not-a-uuid")],
            db_session_factory,
            None,
        )
        assert count == 1
        assert self._counter("inference-router") - before == 1

    async def test_the_counter_is_labelled_per_service_key(self, db_session_factory):
        """Per-service_key is the whole point: it names WHICH emitter to fix."""
        hirag_before = self._counter("hirag")
        router_before = self._counter("inference-router")
        await flush_costs(
            [_make_cost(tenant_id=T("test-tenant"), license_id="", service_key="hirag")],
            db_session_factory,
            None,
        )
        assert self._counter("hirag") - hirag_before == 1
        assert self._counter("inference-router") - router_before == 0

    @staticmethod
    def _denominator(service_key: str) -> float:
        import observability.metrics as om

        return om.cost_events_by_service_total.labels(service_key=service_key)._value.get()

    async def test_every_flushed_event_increments_the_denominator(self, db_session_factory):
        """`billing_license_unattributed_total` alone cannot express a RATE.

        No flush counter carried service_key at all — billing_cost_events_
        flushed_total is labelled {status} only — so a service emitting 10
        unattributed events out of 10 was indistinguishable from one emitting
        10 out of 10,000. Both are counted here off the SAME label expression,
        in the same loop iteration, so numerator and denominator cannot drift.
        """
        before_all = self._denominator("inference-router")
        before_gap = self._counter("inference-router")

        count = await flush_costs(
            [
                _make_cost(tenant_id=T("test-tenant"), license_id="", _msg_id="d-1"),
                _make_cost(tenant_id=T("test-tenant"), license_id=str(uuid.uuid4()), _msg_id="d-2"),
                _make_cost(tenant_id=T("test-tenant"), license_id=str(uuid.uuid4()), _msg_id="d-3"),
            ],
            db_session_factory,
            None,
        )

        assert count == 3
        # The denominator counts ALL three; the numerator only the licence-less
        # one. 1/3 is the per-service rate that was previously not computable.
        assert self._denominator("inference-router") - before_all == 3
        assert self._counter("inference-router") - before_gap == 1

    async def test_the_denominator_carries_the_same_service_key_as_the_numerator(
        self, db_session_factory
    ):
        """Same bucket, or the ratio silently divides two different populations."""
        before_all = self._denominator("hirag")
        before_gap = self._counter("hirag")
        await flush_costs(
            [_make_cost(tenant_id=T("test-tenant"), license_id="", service_key="hirag")],
            db_session_factory,
            None,
        )
        assert self._denominator("hirag") - before_all == 1
        assert self._counter("hirag") - before_gap == 1

    async def test_a_missing_service_key_buckets_as_unknown_not_a_crash(self, db_session_factory):
        """An empty service_key must not raise, and must not silently vanish."""
        before = self._counter("unknown")
        count = await flush_costs(
            [_make_cost(tenant_id=T("test-tenant"), license_id="", service_key="")],
            db_session_factory,
            None,
        )
        assert count == 1
        assert self._counter("unknown") - before == 1


@pytest.mark.unit
class TestAppAttribution:
    """`billing_cost_events.app_id` — the grain that was in the DB but never stamped.

    Measured on prod 2026-09-02: ``app_id`` was NULL on 401,316 of 401,316 cost
    events, so per-application cost was not merely inaccurate, it was
    arithmetically impossible — while 85.6% of a 7-day window already carried a
    ``license_id`` present in the local ``request_quota_config`` mirror WITH a
    non-NULL ``app_id``. `flush_costs` now derives the app from the event's own
    licence.

    Every assertion here reads the row BACK OUT OF POSTGRES rather than
    inspecting the in-memory dict: the change is only real if it lands in the
    table, and a dict the flusher happened to mutate proves nothing about the
    INSERT. Rule 1 from `services.license_resolver` is the other half — a
    licence we cannot map must leave `app_id` NULL, never guess.
    """

    @pytest.fixture(autouse=True)
    def _fresh_resolver_caches(self):
        """Both derivation maps are process-global with a 300s TTL.

        Nothing else in the suite resets them, so a map built by an earlier test
        (against an already-truncated table, i.e. EMPTY) would be reused here and
        every derivation assertion would fail for a reason that has nothing to do
        with the code under test. Reset on the way in AND out, so these tests
        neither inherit nor leak a cached map.

        `get_settings` is cleared alongside them, because `_patch_app_map` below
        overrides `cost_attribution_app_by_service_key` through the environment
        and that lru_cache would otherwise outlive `monkeypatch.undo()` — a
        blanked attribution map leaking into every later test in the process,
        failing them for a reason that is nowhere in their own source.
        """
        from services.license_resolver import reset_cache
        from settings import get_settings

        reset_cache()
        get_settings.cache_clear()
        yield
        reset_cache()
        get_settings.cache_clear()

    @staticmethod
    def _patch_app_map(monkeypatch, value: str) -> None:
        """Override `cost_attribution_app_by_service_key` for one test.

        `get_settings` is lru_cached, so setting the env var alone changes
        nothing — the cache has to be dropped for the new value to be read. The
        matching clear on the way OUT lives in `_fresh_resolver_caches` above,
        which runs for every test in this class.
        """
        from settings import get_settings

        monkeypatch.setenv("COST_ATTRIBUTION_APP_BY_SERVICE_KEY", value)
        get_settings.cache_clear()

    @staticmethod
    async def _seed_licence(
        db_session_factory,
        *,
        tenant_id: str,
        license_id: uuid.UUID,
        app_id: uuid.UUID | None,
        app_name: str | None = "Workers Module",
    ) -> None:
        """Mirror one licence row exactly as the licences reconcile writes it."""
        from models.db import RequestQuotaConfig

        async with db_session_factory() as session:
            session.add(
                RequestQuotaConfig(
                    license_id=license_id,
                    tenant_id=tenant_id,
                    quota_monthly=1_000_000,
                    app_id=app_id,
                    app_name=app_name,
                )
            )
            await session.commit()

    @staticmethod
    async def _stored(db_session_factory, tenant_id: str) -> CostEvent:
        """The single stored row for this tenant, read back from postgres."""
        async with db_session_factory() as session:
            rows = (
                (await session.execute(select(CostEvent).where(CostEvent.tenant_id == tenant_id)))
                .scalars()
                .all()
            )
        assert len(rows) == 1, f"expected exactly one stored event, got {len(rows)}"
        return rows[0]

    @staticmethod
    def _resolved(service_key: str) -> float:
        import observability.metrics as om

        return om.app_resolved_total.labels(service_key=service_key)._value.get()

    @staticmethod
    def _unattributed(service_key: str) -> float:
        import observability.metrics as om

        return om.app_unattributed_total.labels(service_key=service_key)._value.get()

    async def test_an_app_less_event_is_stored_with_the_app_of_its_licence(
        self, db_session_factory
    ):
        """The headline: the licence's app lands in the COLUMN, not just in a dict."""
        tenant = T("app-derive-headline")
        lic = uuid.uuid4()
        app = uuid.uuid4()
        await self._seed_licence(db_session_factory, tenant_id=tenant, license_id=lic, app_id=app)

        count = await flush_costs(
            [_make_cost(tenant_id=tenant, license_id=str(lic), app_id="")],
            db_session_factory,
            None,
        )
        assert count == 1

        row = await self._stored(db_session_factory, tenant)
        assert str(row.app_id) == str(app)
        assert str(row.license_id) == str(lic)
        # The licence was ASSERTED by the caller, so the licence resolver must
        # not have touched it — only the app derivation ran.
        assert "+resolved" not in row.cost_source

    async def test_an_app_id_the_caller_presented_is_never_overwritten(self, db_session_factory):
        """A derived value must never displace one the emitter actually knew."""
        tenant = T("app-derive-presented")
        lic = uuid.uuid4()
        mirror_app = uuid.uuid4()
        presented_app = uuid.uuid4()
        await self._seed_licence(
            db_session_factory, tenant_id=tenant, license_id=lic, app_id=mirror_app
        )

        before = self._resolved("inference-router")
        count = await flush_costs(
            [_make_cost(tenant_id=tenant, license_id=str(lic), app_id=str(presented_app))],
            db_session_factory,
            None,
        )
        assert count == 1

        row = await self._stored(db_session_factory, tenant)
        assert str(row.app_id) == str(presented_app)
        assert str(row.app_id) != str(mirror_app)
        # And it is not counted as a DERIVED attribution, or the metric would
        # overstate what the derivation actually contributed.
        assert self._resolved("inference-router") - before == 0

    async def test_an_event_with_no_licence_keeps_a_null_app(self, db_session_factory):
        """Rule 1 — no licence means no app. Never fabricate one.

        `service_key` is deliberately absent from
        `cost_attribution_app_by_service_key`, so the licence resolver cannot
        supply a licence either and the event reaches the app derivation with
        nothing to derive from.
        """
        tenant = T("app-derive-no-licence")
        before = self._unattributed("totally-unmapped-service")

        count = await flush_costs(
            [
                _make_cost(
                    tenant_id=tenant,
                    license_id="",
                    app_id="",
                    service_key="totally-unmapped-service",
                )
            ],
            db_session_factory,
            None,
        )
        assert count == 1

        row = await self._stored(db_session_factory, tenant)
        assert row.license_id is None
        assert row.app_id is None
        assert self._unattributed("totally-unmapped-service") - before == 1

    async def test_a_licence_absent_from_the_mirror_keeps_a_null_app(self, db_session_factory):
        """An unknown licence is a miss, not a reason to guess an app."""
        tenant = T("app-derive-unknown-licence")
        lic = uuid.uuid4()  # deliberately NOT seeded into request_quota_config
        before = self._unattributed("inference-router")

        count = await flush_costs(
            [_make_cost(tenant_id=tenant, license_id=str(lic), app_id="")],
            db_session_factory,
            None,
        )
        assert count == 1

        row = await self._stored(db_session_factory, tenant)
        assert str(row.license_id) == str(lic)
        assert row.app_id is None
        assert self._unattributed("inference-router") - before == 1

    async def test_a_mirror_row_with_a_null_app_id_keeps_a_null_app(self, db_session_factory):
        """The pre-0014 case: the licence is known, its app is not.

        `_build_app_map` excludes `app_id IS NULL` rows by the same predicate
        `_build_map` uses, so "resolvable" means the same thing to both maps. A
        licence whose app the upstream never sent must degrade to NULL rather
        than borrow an app from anywhere else.

        The stored-row assertions alone are NOT enough, and used to be all this
        test had: a stored `app_id IS NULL` here is indistinguishable from the
        "licence absent from the mirror" case one test up, and `resolve_app`
        returns None for a `{licence: None}` entry exactly as it does for a
        missing key -- so dropping BOTH exclusions (the SQL predicate and the
        `if license_id and app_id` guard in `_build_app_map`) left this test
        green. It therefore asserts the two things that actually separate the
        cases: the licence IS in the mirror, and it is nonetheless ABSENT from
        the built map rather than present with a None value.
        """
        tenant = T("app-derive-null-mirror-app")
        lic = uuid.uuid4()
        await self._seed_licence(
            db_session_factory,
            tenant_id=tenant,
            license_id=lic,
            app_id=None,
            app_name=None,
        )
        before = self._unattributed("inference-router")

        count = await flush_costs(
            [_make_cost(tenant_id=tenant, license_id=str(lic), app_id="")],
            db_session_factory,
            None,
        )
        assert count == 1

        row = await self._stored(db_session_factory, tenant)
        assert str(row.license_id) == str(lic)
        assert row.app_id is None
        assert self._unattributed("inference-router") - before == 1

        # This is the licence-IS-known half: without it the assertions above are
        # satisfied by the unknown-licence case too.
        from models.db import RequestQuotaConfig

        async with db_session_factory() as session:
            mirrored = (
                await session.execute(
                    select(RequestQuotaConfig).where(RequestQuotaConfig.license_id == lic)
                )
            ).scalar_one()
        assert mirrored.app_id is None

        # And the guard itself: the licence must not be a KEY of the map at all.
        # `{lic: None}` would satisfy every assertion above while meaning
        # something quite different -- "resolvable, to nothing".
        from services.license_resolver import get_app_map, reset_cache

        reset_cache()
        async with db_session_factory() as session:
            built = await get_app_map(session)
        assert lic not in built, "a NULL app_id must not become a map entry, not even a None one"

    async def test_the_counters_are_labelled_per_service_key(self, db_session_factory):
        """Per-service_key on BOTH counters, or the gap names no emitter to fix."""
        tenant = T("app-derive-labelled")
        lic = uuid.uuid4()
        app = uuid.uuid4()
        await self._seed_licence(db_session_factory, tenant_id=tenant, license_id=lic, app_id=app)

        hit_before = self._resolved("hirag")
        miss_before = self._unattributed("hirag")
        other_hit_before = self._resolved("inference-router")
        other_miss_before = self._unattributed("inference-router")

        count = await flush_costs(
            [
                # A hit: the seeded licence.
                _make_cost(
                    tenant_id=tenant,
                    license_id=str(lic),
                    app_id="",
                    service_key="hirag",
                    _msg_id="app-lbl-1",
                ),
                # A miss: a licence the mirror has never seen.
                _make_cost(
                    tenant_id=tenant,
                    license_id=str(uuid.uuid4()),
                    app_id="",
                    service_key="hirag",
                    _msg_id="app-lbl-2",
                ),
            ],
            db_session_factory,
            None,
        )
        assert count == 2

        assert self._resolved("hirag") - hit_before == 1
        assert self._unattributed("hirag") - miss_before == 1
        # Nothing bleeds into another emitter's bucket.
        assert self._resolved("inference-router") - other_hit_before == 0
        assert self._unattributed("inference-router") - other_miss_before == 0

    async def test_a_missing_service_key_buckets_as_unknown_not_a_crash(self, db_session_factory):
        """An empty service_key must not raise, and must not vanish from the gap."""
        tenant = T("app-derive-unknown-key")
        before = self._unattributed("unknown")

        count = await flush_costs(
            [_make_cost(tenant_id=tenant, license_id=str(uuid.uuid4()), app_id="", service_key="")],
            db_session_factory,
            None,
        )
        assert count == 1

        row = await self._stored(db_session_factory, tenant)
        assert row.app_id is None
        assert self._unattributed("unknown") - before == 1

    async def test_a_derived_licence_also_yields_an_app_in_the_same_pass(
        self, db_session_factory, monkeypatch
    ):
        """The interaction that could regress: derivation order is licence-THEN-app.

        An event arriving with NO licence has one resolved by the licence
        resolver; the app derivation runs AFTER that block and therefore sees the
        licence it just wrote. Reverse the two and this event stores a licence
        with no app forever — which is exactly the 100%-NULL state this change
        exists to end.

        The (service_key -> app) pair is MONKEYPATCHED rather than borrowed from
        the shipped `inference-router=Workers Module` default. Leaning on the
        default made this test fail whenever an operator edited a config value
        that has nothing to do with derivation ORDER, which is the only thing
        under test here — a failure that names the wrong defect costs more than
        the two lines it saves.
        """
        # `service_key` + a string literal trips detect-secrets' Secret Keyword
        # plugin. It is a routing label, not a credential -- the same false
        # positive is already pragma'd at dozens of `service_key=` call sites
        # across this suite, which is why the CI secret gate went red here and
        # not on the other literals this commit added.
        service_key = "order-probe-service"  # pragma: allowlist secret
        app_name = "Order Probe App"
        self._patch_app_map(monkeypatch, f"{service_key}={app_name}")

        tenant = T("app-derive-after-licence")
        lic = uuid.uuid4()
        app = uuid.uuid4()
        await self._seed_licence(
            db_session_factory,
            tenant_id=tenant,
            license_id=lic,
            app_id=app,
            app_name=app_name,
        )

        lic_before = self._resolved(service_key)
        count = await flush_costs(
            [
                _make_cost(
                    tenant_id=tenant,
                    license_id="",
                    app_id="",
                    service_key=service_key,
                )
            ],
            db_session_factory,
            None,
        )
        assert count == 1

        row = await self._stored(db_session_factory, tenant)
        # Both grains, from one pass over the same event.
        assert str(row.license_id) == str(lic)
        assert str(row.app_id) == str(app)
        # The licence was DERIVED, and says so; the app carries no suffix by
        # design (the distinction lives in billing_app_resolved_total).
        assert row.cost_source.endswith("+resolved")
        assert self._resolved(service_key) - lic_before == 1

    async def test_a_delegated_w2v_video_compiler_row_resolves_to_the_tenants_w2v_licence(
        self, db_session_factory
    ):
        """The measured defect, end to end through the SHIPPED default map.

        A delegated (customer-tenant) call through a w2v_common receiver binds
        no licence at the emitter by design (w2v_common `middleware.py:965-972`,
        issue #170), so the row arrives with `license_id` NULL. Measured
        2026-09-16: merqi's one run left its `w2v-tts` row resolved
        (`w2v-tts+resolved`) and its two `w2v-video-compiler` rows NULL/NULL,
        because the key was absent from `cost_attribution_app_by_service_key`.
        This deliberately does NOT monkeypatch the map: the default itself is
        what is under test, and a patched value would pass with the entry
        missing.
        """
        tenant = T("delegated-w2v-compiler")
        lic = uuid.uuid4()
        app = uuid.uuid4()
        await self._seed_licence(
            db_session_factory,
            tenant_id=tenant,
            license_id=lic,
            app_id=app,
            app_name="Web to Video",
        )

        count = await flush_costs(
            [
                _make_cost(
                    tenant_id=tenant,
                    license_id="",
                    app_id="",
                    service_key="w2v-video-compiler",  # pragma: allowlist secret
                )
            ],
            db_session_factory,
            None,
        )
        assert count == 1

        row = await self._stored(db_session_factory, tenant)
        assert str(row.license_id) == str(lic)
        assert str(row.app_id) == str(app)
        assert row.cost_source.endswith("+resolved")

    async def test_a_delegated_w2v_video_compiler_row_stays_null_without_a_w2v_licence(
        self, db_session_factory
    ):
        """No false attribution: the tenant holds an active licence for a
        DIFFERENT app only. The new map entry must resolve nothing here -- a
        row billed against a Workers Module licence for video-compiler spend
        would be a mis-bill dressed up as a fix.
        """
        tenant = T("delegated-w2v-compiler-unlicensed")
        await self._seed_licence(
            db_session_factory,
            tenant_id=tenant,
            license_id=uuid.uuid4(),
            app_id=uuid.uuid4(),
            app_name="Workers Module",
        )

        count = await flush_costs(
            [
                _make_cost(
                    tenant_id=tenant,
                    license_id="",
                    app_id="",
                    service_key="w2v-video-compiler",  # pragma: allowlist secret
                )
            ],
            db_session_factory,
            None,
        )
        assert count == 1

        row = await self._stored(db_session_factory, tenant)
        assert row.license_id is None
        assert row.app_id is None
        assert "+resolved" not in row.cost_source

    async def test_app_derivation_runs_even_with_an_empty_service_key_map(
        self, db_session_factory, monkeypatch
    ):
        """The DESIGN REASON for the second, separate try/except — pinned.

        App derivation reads the event's OWN licence and never consults
        `cost_attribution_app_by_service_key`. Nesting it back inside the
        licence block's `if app_by_key:` gate is a plausible tidy-up that
        compiles, reads better, and silently makes per-app cost depend on an
        unrelated operator setting: blank that CSV and every event stores
        app_id NULL again, which is the exact 100%-NULL state this change
        exists to end. Nothing pinned that — the whole flusher suite stayed
        green under the re-nesting — so this test blanks the setting and
        demands the app still lands.
        """
        self._patch_app_map(monkeypatch, "")

        tenant = T("app-derive-empty-map")
        lic = uuid.uuid4()
        app = uuid.uuid4()
        await self._seed_licence(db_session_factory, tenant_id=tenant, license_id=lic, app_id=app)

        before = self._resolved("inference-router")
        count = await flush_costs(
            [_make_cost(tenant_id=tenant, license_id=str(lic), app_id="")],
            db_session_factory,
            None,
        )
        assert count == 1

        row = await self._stored(db_session_factory, tenant)
        assert str(row.app_id) == str(app)
        assert self._resolved("inference-router") - before == 1
        # The licence resolver really was skipped — no suffix, so this proves
        # derivation ran on its own and not as a side effect of that block.
        assert "+resolved" not in row.cost_source

    async def test_a_failed_app_map_build_does_not_cost_the_batch(
        self, db_session_factory, monkeypatch
    ):
        """The fail-soft claim, made real — regression guard for the SAVEPOINT.

        `except Exception` around the map build was never enough. The map is
        SELECTed on the SAME session that INSERTs the batch, so a failed
        statement leaves the session needing a rollback: the exception was
        swallowed, and the INSERT a few lines later then raised
        PendingRollbackError and killed the batch anyway — precisely what the
        comment promised could not happen.

        So the failure injected here is a REAL failing statement on the real
        session, not a bare `raise` that never touches the transaction: a bare
        raise passes with or without the savepoint and would guard nothing.
        The batch must still be stored, counted and committed.
        """
        from sqlalchemy import text

        import services.cost_flusher as cf

        async def _boom(session, **_kw):
            await session.execute(text("SELECT * FROM no_such_table_appid_failsoft"))
            raise AssertionError("unreachable: the SELECT above must raise")

        monkeypatch.setattr(cf, "get_app_map", _boom)

        tenant = T("app-derive-failsoft")
        lic = uuid.uuid4()
        app = uuid.uuid4()
        await self._seed_licence(db_session_factory, tenant_id=tenant, license_id=lic, app_id=app)

        count = await flush_costs(
            [_make_cost(tenant_id=tenant, license_id=str(lic), app_id="")],
            db_session_factory,
            None,
        )
        # Counted...
        assert count == 1
        # ...and actually COMMITTED, which is the half PendingRollbackError ate.
        row = await self._stored(db_session_factory, tenant)
        assert str(row.license_id) == str(lic)
        # Attribution is what degrades, and only attribution: no app was
        # derived, so the row keeps a NULL app_id rather than a wrong one.
        assert row.app_id is None

    async def test_a_failed_licence_map_build_does_not_cost_the_batch(
        self, db_session_factory, monkeypatch
    ):
        """The same savepoint, on the OLDER of the two map builds.

        `get_resolution_map` has the identical problem and predates this change:
        it SELECTs on the batch's session, and its `except Exception` was just
        as unable to keep a poisoned transaction alive. Without a test the
        licence-side savepoint could be deleted as redundant and nothing would
        go red — measured: removing it left all ten tests in this class green.

        The tell is deliberately the APP derivation, which runs after the failed
        licence map build in the SAME transaction. It can only succeed if the
        session was genuinely rescued, so this asserts the recovery rather than
        merely the absence of a crash.
        """
        from sqlalchemy import text

        import services.cost_flusher as cf

        async def _boom(session, **_kw):
            await session.execute(text("SELECT * FROM no_such_table_licence_failsoft"))
            raise AssertionError("unreachable: the SELECT above must raise")

        monkeypatch.setattr(cf, "get_resolution_map", _boom)

        tenant = T("licence-map-failsoft")
        lic = uuid.uuid4()
        app = uuid.uuid4()
        await self._seed_licence(db_session_factory, tenant_id=tenant, license_id=lic, app_id=app)

        count = await flush_costs(
            [_make_cost(tenant_id=tenant, license_id=str(lic), app_id="")],
            db_session_factory,
            None,
        )
        assert count == 1

        row = await self._stored(db_session_factory, tenant)
        assert str(row.license_id) == str(lic)
        # The session survived the failed map build: the app derivation that
        # follows it still read the mirror and still committed.
        assert str(row.app_id) == str(app)

    async def test_a_commonpractice_event_resolves_its_common_practice_licence(
        self, db_session_factory
    ):
        """`commonpractice` was absent from the map, so 100% of its spend was lost.

        Measured 2026-09-02 over 12h: 154 events, 154 unattributed. commonpractice
        emits its own cost events with `license_id: ''` HARDCODED
        (`backend/src/host/metering-emit.ts:80`) and attaches no identity to its
        outbound LLM call, so neither it nor inference-router can attribute the
        spend on the wire. The map entry is the only lever that needs no change
        in that repo.

        This asserts the ENTRY DOES WORK. Its pair below asserts the entry is
        what does the work -- delete `commonpractice=Common Practice` from
        `settings.cost_attribution_app_by_service_key` and THIS test fails.
        """
        tenant = T("cp-resolve-headline")
        lic = uuid.uuid4()
        app = uuid.uuid4()
        await self._seed_licence(
            db_session_factory,
            tenant_id=tenant,
            license_id=lic,
            app_id=app,
            app_name="Common Practice",
        )
        before = self._resolved("commonpractice")

        count = await flush_costs(
            [
                _make_cost(
                    tenant_id=tenant,
                    license_id="",
                    app_id="",
                    service_key="commonpractice",
                )
            ],
            db_session_factory,
            None,
        )
        assert count == 1

        row = await self._stored(db_session_factory, tenant)
        assert str(row.license_id) == str(lic)
        assert row.cost_source.endswith("+resolved")
        assert self._resolved("commonpractice") - before == 1

    async def test_an_unmapped_service_key_with_the_same_licence_stays_unattributed(
        self, db_session_factory
    ):
        """The control that makes the test above mean something.

        Identical seeded licence, identical event -- only the `service_key`
        differs, and it is one the map does not carry. If this ALSO resolved,
        the test above would be passing on some other mechanism (a tenant-only
        fallback, say) and would not be evidence for the map entry at all.

        It also pins Rule 1 of the resolver for the new entry's neighbourhood:
        an unmapped service_key is never resolved, it stays visibly
        unattributed. That is the honest outcome, not a bug to paper over --
        which is exactly why the other 49 commonpractice rows (7 tenants that
        hold no licence at all) must remain unattributed too.
        """
        tenant = T("cp-resolve-control")
        lic = uuid.uuid4()
        app = uuid.uuid4()
        await self._seed_licence(
            db_session_factory,
            tenant_id=tenant,
            license_id=lic,
            app_id=app,
            app_name="Common Practice",
        )
        before = self._unattributed("commonpractice-but-not-really")

        count = await flush_costs(
            [
                _make_cost(
                    tenant_id=tenant,
                    license_id="",
                    app_id="",
                    service_key="commonpractice-but-not-really",
                )
            ],
            db_session_factory,
            None,
        )
        assert count == 1

        row = await self._stored(db_session_factory, tenant)
        assert row.license_id is None
        assert self._unattributed("commonpractice-but-not-really") - before == 1


@pytest.mark.unit
class TestTheIngressCounterIsAliasedNotRenamed:
    """`billing_license_unattributed_total` is MISNAMED, and the fix is an alias.

    It fires at INGRESS, while the wire is parsed, BEFORE the idempotency claim
    and BEFORE `license_resolver` runs. It measures "the caller did not assert a
    licence" — a statement about the EMITTER's transport — and the resolver then
    fills ~94% of what it counted. Measured over a 10h window on 2026-09-05,
    strictly after that day's truncate: +2,089 here against 27,941 stored rows
    of which 113 had a NULL license_id. Read as an invoicing gap it has caused
    false alarm in at least three sessions.

    The old name is NOT deleted. It is load-bearing for two live PrometheusRules
    (`BillingAttributionRateRegressed`, `BillingUnattributedVolumeSustained`) and
    three `ci/prometheus-rule-tests/` fixtures in gitops-production. A hard
    rename would not error anywhere — it would EMPTY those series, and an alert
    over an empty vector is vacuously green, which is a worse outcome than a
    badly-named metric. These tests pin the alias so a later "tidy-up" that
    drops the old name goes red here instead of going quiet in production.
    """

    @staticmethod
    def _old(service_key: str) -> float:
        import observability.metrics as om

        return om.license_unattributed_total.labels(service_key=service_key)._value.get()

    @staticmethod
    def _new(service_key: str) -> float:
        import observability.metrics as om

        return om.license_caller_unasserted_total.labels(service_key=service_key)._value.get()

    async def test_both_names_move_together_on_a_licence_less_event(self, db_session_factory):
        old_before, new_before = self._old("inference-router"), self._new("inference-router")
        count = await flush_costs(
            [_make_cost(tenant_id=T("alias-both"), license_id="")], db_session_factory, None
        )
        assert count == 1
        assert self._old("inference-router") - old_before == 1
        assert self._new("inference-router") - new_before == 1

    async def test_neither_name_moves_on_an_asserted_licence(self, db_session_factory):
        """The other half — a counter that only goes up measures nothing."""
        old_before, new_before = self._old("inference-router"), self._new("inference-router")
        count = await flush_costs(
            [_make_cost(tenant_id=T("alias-neither"), license_id=str(uuid.uuid4()))],
            db_session_factory,
            None,
        )
        assert count == 1
        assert self._old("inference-router") - old_before == 0
        assert self._new("inference-router") - new_before == 0

    async def test_the_alias_is_exact_over_a_mixed_batch(self, db_session_factory):
        """Not "both rose" — both rose by the SAME amount, on the SAME label.

        An alias that drifts is worse than no alias: the two names would then
        disagree and the gitops rules would silently track the wrong one.
        """
        old_before, new_before = self._old("hirag"), self._new("hirag")
        count = await flush_costs(
            [
                _make_cost(
                    tenant_id=T("alias-mix"), license_id="", service_key="hirag", _msg_id="a-1"
                ),
                _make_cost(
                    tenant_id=T("alias-mix"), license_id="", service_key="hirag", _msg_id="a-2"
                ),
                _make_cost(
                    tenant_id=T("alias-mix"),
                    license_id=str(uuid.uuid4()),
                    service_key="hirag",
                    _msg_id="a-3",
                ),
            ],
            db_session_factory,
            None,
        )
        assert count == 3
        old_delta = self._old("hirag") - old_before
        new_delta = self._new("hirag") - new_before
        assert old_delta == 2
        assert new_delta == old_delta

    def test_the_deprecated_name_still_exists_for_the_gitops_rules(self) -> None:
        """The five live consumers break the moment this metric NAME disappears."""
        import observability.metrics as om

        names = {metric.name for metric in om.license_unattributed_total.collect()}
        assert "billing_license_unattributed" in names

    def test_the_deprecated_help_no_longer_asserts_the_wrong_meaning(self) -> None:
        """A misnamed metric with an honest HELP is recoverable; one whose HELP
        confirms the wrong reading is what produced three false alarms.
        """
        import observability.metrics as om

        doc = om.license_unattributed_total._documentation
        assert "DEPRECATED" in doc
        assert "billing_license_caller_unasserted_total" in doc


@pytest.mark.unit
class TestThePersistedGapIsCountedAfterResolution:
    """`billing_license_unattributed_persisted_total` — the number operators want.

    Every other attribution counter on the cost path fires at ingress and so
    answers "did the caller assert one?". This one runs over `survived`, after
    the idempotency claim, after the event_key dedup and after BOTH resolution
    blocks, and answers "is this spend in the ledger attributable to an invoice
    line?". Those are different populations, and conflating them is the defect
    this whole change exists to close.
    """

    @staticmethod
    def _persisted(service_key: str) -> float:
        import observability.metrics as om

        return om.license_unattributed_persisted_total.labels(service_key=service_key)._value.get()

    @staticmethod
    def _tenant_persisted(service_key: str) -> float:
        import observability.metrics as om

        return om.tenant_unattributed_persisted_total.labels(service_key=service_key)._value.get()

    @staticmethod
    def _ingress(service_key: str) -> float:
        import observability.metrics as om

        return om.license_caller_unasserted_total.labels(service_key=service_key)._value.get()

    async def test_a_row_stored_with_a_null_licence_is_counted(self, db_session_factory):
        before = self._persisted("inference-router")
        count = await flush_costs(
            [_make_cost(tenant_id=T("persisted-gap"), license_id="")], db_session_factory, None
        )
        assert count == 1
        assert self._persisted("inference-router") - before == 1

    async def test_an_asserted_licence_is_not_counted(self, db_session_factory):
        before = self._persisted("inference-router")
        count = await flush_costs(
            [_make_cost(tenant_id=T("persisted-ok"), license_id=str(uuid.uuid4()))],
            db_session_factory,
            None,
        )
        assert count == 1
        assert self._persisted("inference-router") - before == 0

    async def test_it_counts_over_survived_not_over_pre_rows(self, db_session_factory):
        """THE distinguishing test, and the reason the two families are not a
        ratio. A redelivered message inflates the INGRESS counter (it is read
        off the wire again) but must not touch the persisted one, which runs
        after the claim drops the duplicate.
        """
        msg = "persist-dedup-1"
        ingress_before = self._ingress("inference-router")
        persisted_before = self._persisted("inference-router")

        for _ in range(2):
            await flush_costs(
                [_make_cost(tenant_id=T("persisted-dedup"), license_id="", _msg_id=msg)],
                db_session_factory,
                None,
            )

        # Both deliveries were parsed off the wire, so ingress counted twice...
        assert self._ingress("inference-router") - ingress_before == 2
        # ...while the ProcessedCostMessage claim dropped the second, so exactly
        # one row reached the ledger and exactly one is counted. If these two
        # deltas are ever equal on this test, the persisted counter has drifted
        # back to the pre-claim population and measures the alias's population.
        assert self._persisted("inference-router") - persisted_before == 1

    async def test_the_persisted_count_is_labelled_by_the_offending_service(
        self, db_session_factory
    ):
        """Per-service_key or it names no one to fix — the same defect the
        unmapped counter had for 957 increments in 24h.
        """
        hirag_before = self._persisted("hirag")
        router_before = self._persisted("inference-router")
        await flush_costs(
            [_make_cost(tenant_id=T("persisted-label"), license_id="", service_key="hirag")],
            db_session_factory,
            None,
        )
        assert self._persisted("hirag") - hirag_before == 1
        assert self._persisted("inference-router") - router_before == 0

    async def test_the_tenant_persisted_counter_stays_at_zero_on_the_cost_stream(
        self, db_session_factory
    ):
        """Not dead code — the CONTROL. `validate_cost_event` dead-letters an
        unresolvable tenant before it can become a row, so this must read zero.
        That zero is the evidence that the ingress tenant counter's +1,100 was a
        header-propagation signal and not a ledger gap. A non-zero reading here
        means that guard has been weakened or bypassed.
        """
        before = self._tenant_persisted("inference-router")
        count = await flush_costs(
            [_make_cost(tenant_id=T("tenant-persisted"), license_id="")],
            db_session_factory,
            None,
        )
        assert count == 1
        assert self._tenant_persisted("inference-router") - before == 0

    def test_the_two_families_are_documented_as_non_comparable(self) -> None:
        """The trap this change closes is reading one against the other."""
        import observability.metrics as om

        assert "NOT comparable" in om.license_unattributed_persisted_total._documentation


class TestTenantNameResolvedAtWriteTime:
    """``billing_cost_events.tenant_name`` must hold a NAME, not the tenant uuid.

    THE DEFECT. Since ``0011_tenant_id_is_a_uuid`` the emitters send a uuid as
    ``tenant_id`` and stop sending ``tenant_name`` at all, so the flusher's
    fallback (``original_tenant_label``, which faithfully preserves whatever was
    on the wire) preserved the UUID into a display column. Measured on the prod
    billing primary 2026-09-05: a uuid on 66,103 of 66,103 rows, $310.49 — and
    it propagates into ``billing_cost_daily.tenant_name`` and every SQL consumer.
    It was invisible because the column is never NULL and never empty: 100% noise
    that LOOKS populated. Only ``/dashboard/costs`` and the CSV export were right,
    because each re-resolved the name at READ time.

    The registry that resolves it is already in-process on the flush path
    (``billing_tenant_registry_live_entries 59``), so the fix consults it at
    WRITE time. These tests pin the three ways that could have gone wrong:
    fabricating a label, raising on a miss, or resolving from a second source
    that can drift from what the read paths use.
    """

    REGISTERED_UUID = "01a0bbbb-0000-7000-8000-0000000000b1"

    @pytest.fixture(autouse=True)
    def _clean_overlay(self):
        import tenancy

        tenancy.reset_live_tenant_registry()
        yield
        tenancy.reset_live_tenant_registry()

    @pytest.mark.unit
    async def test_a_registry_hit_stores_the_real_name(self, db_session_factory):
        """The fix itself: a uuid on the wire, a NAME in the column."""
        import tenancy

        tenancy.set_live_tenant_registry({"acme-corp": self.REGISTERED_UUID})
        buffer = [_make_cost(license_id="", tenant_id=self.REGISTERED_UUID)]

        assert await flush_costs(buffer, db_session_factory, None) == 1
        async with db_session_factory() as session:
            row = (await session.execute(select(CostEvent))).scalar_one()
            assert row.tenant_id == self.REGISTERED_UUID
            assert row.tenant_name == "acme-corp"
            # The uuid must not survive anywhere in the display column — this is
            # the exact shape the 66,103 rows carried.
            assert row.tenant_name != row.tenant_id

    @pytest.mark.unit
    async def test_the_stored_name_matches_what_the_read_path_would_render(
        self, db_session_factory
    ):
        """Write-time and read-time resolution must not be able to disagree.

        Both consult the same registry snapshot, so a row's stored label and the
        label ``/dashboard/costs`` computes for it are the same string. If this
        ever fails the fix has introduced a SECOND source of truth for a name,
        which is worse than the single wrong one it replaced.
        """
        import tenancy
        from services.tenant_names import resolve_tenant_name

        tenancy.set_live_tenant_registry({"acme-corp": self.REGISTERED_UUID})
        await flush_costs(
            [_make_cost(license_id="", tenant_id=self.REGISTERED_UUID)], db_session_factory, None
        )
        async with db_session_factory() as session:
            row = (await session.execute(select(CostEvent))).scalar_one()
        rendered = resolve_tenant_name(
            row.tenant_id, row.tenant_name, {self.REGISTERED_UUID.lower(): "acme-corp"}
        )
        assert row.tenant_name == rendered == "acme-corp"

    @pytest.mark.unit
    async def test_a_registry_miss_does_not_fabricate_and_does_not_raise(self, db_session_factory):
        """⚠️ NEVER INVENT A LABEL, and NEVER FAIL THE MONEY RECORD.

        A tenant genuinely unknown to the registry keeps the pre-existing
        fallback — the uuid — because a plausible fabricated name would read as a
        measurement, while a uuid is visibly not a name. And the event still
        BILLS: a display label must never be able to fail a cost row.
        """
        import tenancy

        tenancy.set_live_tenant_registry({"acme-corp": self.REGISTERED_UUID})
        unknown = T("t-not-in-registry")
        assert tenancy.live_tenant_label(unknown) is None

        assert (
            await flush_costs(
                [_make_cost(license_id="", tenant_id=unknown)], db_session_factory, None
            )
            == 1
        )
        async with db_session_factory() as session:
            row = (await session.execute(select(CostEvent))).scalar_one()
            assert row.tenant_id == unknown
            # No invention — and specifically not the neighbouring real name.
            assert row.tenant_name != "acme-corp"
            # And NOT the uuid either. A uuid in a name column is worse than an
            # absence: it is non-empty and per-tenant distinct, so it satisfies
            # every downstream truthiness guard and reads as data (see #135).
            assert row.tenant_name != row.tenant_id
            assert row.tenant_name == ""

    @pytest.mark.unit
    async def test_an_unresolved_name_is_counted_so_it_stays_visible(self, db_session_factory):
        """A miss must be DISTINGUISHABLE, or this fix merely hides the defect.

        Reuses the established ``billing_placeholder_value_total`` mechanism
        rather than inventing a second instrument — a stored "name" that is
        merely the uuid echoed back is exactly what that counter exists to make
        countable.
        """
        import observability.metrics as om

        def _counter() -> float:
            return om.placeholder_value_total.labels(
                field="tenant_name", service_key="inference-router"
            )._value.get()

        before = _counter()
        await flush_costs(
            [_make_cost(license_id="", tenant_id=T("t-uncounted-name"))], db_session_factory, None
        )
        assert _counter() - before == 1

    @pytest.mark.unit
    async def test_a_resolved_name_is_not_counted_as_a_placeholder(self, db_session_factory):
        """The other half of the same instrument.

        Without this, a counter that fired on EVERY row would read exactly like
        one that fired on none — the gauge would be a constant, and constants
        cannot explain a varying outcome.
        """
        import observability.metrics as om
        import tenancy

        tenancy.set_live_tenant_registry({"acme-corp": self.REGISTERED_UUID})

        def _counter() -> float:
            return om.placeholder_value_total.labels(
                field="tenant_name", service_key="inference-router"
            )._value.get()

        before = _counter()
        await flush_costs(
            [_make_cost(license_id="", tenant_id=self.REGISTERED_UUID)], db_session_factory, None
        )
        assert _counter() - before == 0

    @pytest.mark.unit
    async def test_an_emitted_name_still_wins_over_the_bare_uuid(self, db_session_factory):
        """An emitter that DOES send a real name keeps it when the registry is
        silent — the fallback chain still has three rungs, not one."""
        unknown = T("t-emitter-named")
        buffer = [_make_cost(license_id="", tenant_id=unknown, tenant_name="emitter-said-so")]
        assert await flush_costs(buffer, db_session_factory, None) == 1
        async with db_session_factory() as session:
            row = (await session.execute(select(CostEvent))).scalar_one()
            assert row.tenant_name == "emitter-said-so"


# ---------------------------------------------------------------------------
# Decisions 1-3 (alembic 0021): actor_id, quantity_unit, cost_basis,
# vendor_cost_usd, currency.
# ---------------------------------------------------------------------------


async def _seed_pricing(db_session_factory, **kwargs) -> ProviderPricing:
    defaults: dict = {
        "model": None,
        "effective_from": datetime(2026, 1, 1, tzinfo=UTC),
    }
    defaults.update(kwargs)
    row = ProviderPricing(**defaults)
    async with db_session_factory() as session:
        session.add(row)
        await session.commit()
        await session.refresh(row)
    return row


@pytest.mark.unit
class TestAttributionAndPricingGrain:
    async def test_a_row_without_the_new_fields_still_succeeds(self, db_session_factory):
        """Backward compatibility: today's emitters send none of this."""
        buffer = [_make_cost(tenant_id=T("attrib-bc"), provider="claude")]
        count = await flush_costs(buffer, db_session_factory, None)
        assert count == 1

        async with db_session_factory() as session:
            row = (
                await session.execute(
                    select(CostEvent).where(CostEvent.tenant_id == T("attrib-bc"))
                )
            ).scalar_one()
            # actor_id: NULLABLE, NEVER DEFAULTED.
            assert row.actor_id is None
            # vendor_cost_usd: NULLABLE, never defaulted to 0.
            assert row.vendor_cost_usd is None
            # currency: decision 3's own stated default.
            assert row.currency == "USD"
            # cost_basis: NOT NULL, derived (no pricing row here -> the pricing-
            # miss safe backstop, not 'cash').
            assert row.cost_basis == "internal_transfer"

    async def test_a_row_with_all_new_fields_round_trips_each_one(self, db_session_factory):
        actor = str(uuid.uuid4())
        buffer = [
            _make_cost(
                tenant_id=T("attrib-roundtrip"),
                provider="anthropic",
                actor_id=actor,
                quantity_unit="tokens",
                cost_basis="cash",
                vendor_cost_usd="1.23456789",
                currency="EUR",
            )
        ]
        count = await flush_costs(buffer, db_session_factory, None)
        assert count == 1

        async with db_session_factory() as session:
            row = (
                await session.execute(
                    select(CostEvent).where(CostEvent.tenant_id == T("attrib-roundtrip"))
                )
            ).scalar_one()
            assert str(row.actor_id) == actor
            assert row.quantity_unit == "tokens"
            assert row.cost_basis == "cash"
            assert row.vendor_cost_usd == Decimal("1.23456789")
            assert row.currency == "EUR"

    @pytest.mark.parametrize(
        "wire_actor,expect_counted",
        [
            ("not-a-uuid", True),
            ("00000000-0000-0000-0000-00000000000Z", True),
            ("", False),
            (None, False),
        ],
    )
    async def test_malformed_actor_id_is_counted_and_stored_null(
        self, db_session_factory, wire_actor, expect_counted
    ):
        """A NON-EMPTY actor_id that fails to parse is counted on
        billing_actor_malformed_total{service_key}; an empty/absent one is not.
        Either way the row lands with actor_id NULL (never guessed).

        ⛔ FAILS WHEN REVERTED: origin/main stored the malformed actor as NULL
        with no signal, indistinguishable from an emitter that sent none.
        """
        service_key = f"actor-malformed-{uuid.uuid4().hex[:8]}"
        tenant = T(f"actor-malformed-{wire_actor!s}")
        extra = {} if wire_actor is None else {"actor_id": wire_actor}
        before = REGISTRY.get_sample_value(
            "billing_actor_malformed_total", {"service_key": service_key}
        )
        buffer = [_make_cost(tenant_id=tenant, service_key=service_key, **extra)]
        assert await flush_costs(buffer, db_session_factory, None) == 1

        after = REGISTRY.get_sample_value(
            "billing_actor_malformed_total", {"service_key": service_key}
        )
        if expect_counted:
            assert (after or 0.0) - (before or 0.0) == 1.0
        else:
            assert (after or 0.0) == (before or 0.0)

        async with db_session_factory() as session:
            row = (
                await session.execute(select(CostEvent).where(CostEvent.tenant_id == tenant))
            ).scalar_one()
            assert row.actor_id is None

    async def test_quantity_unit_is_derived_for_a_token_provider(self, db_session_factory):
        """pgx-local -- provider-name rung, not the cost_model fallback."""
        await _seed_pricing(
            db_session_factory,
            provider="pgx-local",
            cost_model="per_token",
            input_per_1m_usd=Decimal("10"),
            output_per_1m_usd=Decimal("25"),
        )
        buffer = [_make_cost(tenant_id=T("attrib-tok"), provider="pgx-local")]
        assert await flush_costs(buffer, db_session_factory, None) == 1

        async with db_session_factory() as session:
            row = (
                await session.execute(
                    select(CostEvent).where(CostEvent.tenant_id == T("attrib-tok"))
                )
            ).scalar_one()
            assert row.quantity_unit == "tokens"
            # pgx-local is self-hosted -- per_token says HOW we meter (real
            # tokens), not WHETHER cash leaves the building. cost_basis is a
            # PROVIDER CONTRACT fact (alembic 0022); the seeded pricing row's
            # own cost_basis defaults 'internal_transfer' and is not derived
            # from cost_model.
            assert row.cost_basis == "internal_transfer"

    async def test_a_self_hosted_character_provider_derives_internal_transfer(
        self, db_session_factory
    ):
        """THE REGRESSION THAT MATTERS: the live defect, reproduced.

        mlx-nlp is self-hosted (runs on mini2) and metered per_character. The
        defect derived cost_basis from cost_model alone, so anything not
        self_hosted_amortized -- including every self-hosted per_token/per_
        character provider -- landed on 'cash'. Measured live 2026-09-04:
        15-and-growing billing_cost_events rows, all mlx-nlp/per_character,
        mislabelled 'cash' within minutes of the 0021 deploy.
        """
        await _seed_pricing(
            db_session_factory,
            provider="mlx-nlp",
            cost_model="per_character",
            input_per_1m_usd=Decimal("1"),
        )
        buffer = [_make_cost(tenant_id=T("attrib-mlxnlp"), provider="mlx-nlp")]
        assert await flush_costs(buffer, db_session_factory, None) == 1

        async with db_session_factory() as session:
            row = (
                await session.execute(
                    select(CostEvent).where(CostEvent.tenant_id == T("attrib-mlxnlp"))
                )
            ).scalar_one()
            assert row.cost_basis == "internal_transfer"

    async def test_an_explicitly_cash_pricing_row_derives_cash(self, db_session_factory):
        """A provider_pricing row that DECLARES cost_basis='cash' derives cash.

        This is the mechanism a genuine external vendor uses (alembic 0022):
        the fact lives on the pricing row, set explicitly by an admin, not
        inferred from the rate's shape.
        """
        await _seed_pricing(
            db_session_factory,
            provider="openai",
            cost_model="per_token",
            input_per_1m_usd=Decimal("5"),
            output_per_1m_usd=Decimal("15"),
            cost_basis="cash",
        )
        buffer = [_make_cost(tenant_id=T("attrib-openai-cash"), provider="openai")]
        assert await flush_costs(buffer, db_session_factory, None) == 1

        async with db_session_factory() as session:
            row = (
                await session.execute(
                    select(CostEvent).where(CostEvent.tenant_id == T("attrib-openai-cash"))
                )
            ).scalar_one()
            assert row.cost_basis == "cash"

    async def test_quantity_unit_is_derived_for_a_character_provider(self, db_session_factory):
        """mlx-embed -- proof: 512-token model, input_tokens=1,065,551 in prod."""
        await _seed_pricing(
            db_session_factory,
            provider="mlx-embed",
            cost_model="per_character",
            input_per_1m_usd=Decimal("1"),
        )
        buffer = [
            _make_cost(
                tenant_id=T("attrib-char"),
                provider="mlx-embed",
                input_tokens="1065551",
                output_tokens="0",
            )
        ]
        assert await flush_costs(buffer, db_session_factory, None) == 1

        async with db_session_factory() as session:
            row = (
                await session.execute(
                    select(CostEvent).where(CostEvent.tenant_id == T("attrib-char"))
                )
            ).scalar_one()
            assert row.quantity_unit == "characters"

    async def test_cost_basis_derives_to_internal_transfer_for_a_self_hosted_row(
        self, db_session_factory
    ):
        """A self-hosted provider_pricing row (any cost_model) defaults 'internal_transfer'.

        cost_basis is a PROVIDER CONTRACT fact on provider_pricing (alembic
        0022), not something inferred from cost_model -- self_hosted_amortized
        is unrelated to why this derives internal_transfer here; the seeded
        row's own cost_basis column, left unset, takes its NOT NULL default.
        """
        await _seed_pricing(
            db_session_factory,
            provider="gemma",
            cost_model="self_hosted_amortized",
            monthly_fixed_cost_usd=Decimal("500"),
        )
        buffer = [_make_cost(tenant_id=T("attrib-selfhosted"), provider="gemma")]
        assert await flush_costs(buffer, db_session_factory, None) == 1

        async with db_session_factory() as session:
            row = (
                await session.execute(
                    select(CostEvent).where(CostEvent.tenant_id == T("attrib-selfhosted"))
                )
            ).scalar_one()
            assert row.cost_basis == "internal_transfer"
            assert row.quantity_unit == "tokens"

    async def test_wire_asserted_quantity_unit_and_cost_basis_win_over_derivation(
        self, db_session_factory
    ):
        """An explicit, valid wire value is honoured verbatim (decision 2/3)."""
        await _seed_pricing(
            db_session_factory,
            provider="mflux",
            cost_model="per_request",
            per_request_usd=Decimal("0.05"),
        )
        buffer = [
            _make_cost(
                tenant_id=T("attrib-wire-wins"),
                provider="mflux",
                quantity_unit="megapixels",
                cost_basis="cash",
            )
        ]
        assert await flush_costs(buffer, db_session_factory, None) == 1

        async with db_session_factory() as session:
            row = (
                await session.execute(
                    select(CostEvent).where(CostEvent.tenant_id == T("attrib-wire-wins"))
                )
            ).scalar_one()
            # Deriving from cost_model alone would say 'requests' -- the wire's
            # measured 'megapixels' must win instead.
            assert row.quantity_unit == "megapixels"
            assert row.cost_basis == "cash"

    async def test_an_invalid_wire_quantity_unit_falls_through_to_derivation(
        self, db_session_factory
    ):
        """A value outside the bounded set is treated as absent, not stored."""
        await _seed_pricing(
            db_session_factory,
            provider="pgx-local",
            cost_model="per_token",
            input_per_1m_usd=Decimal("10"),
            output_per_1m_usd=Decimal("25"),
        )
        buffer = [
            _make_cost(
                tenant_id=T("attrib-invalid-unit"),
                provider="pgx-local",
                quantity_unit="bushels",
            )
        ]
        assert await flush_costs(buffer, db_session_factory, None) == 1

        async with db_session_factory() as session:
            row = (
                await session.execute(
                    select(CostEvent).where(CostEvent.tenant_id == T("attrib-invalid-unit"))
                )
            ).scalar_one()
            assert row.quantity_unit == "tokens"


class TestArchivedTenantBilling:
    """Archival is FLAG-AND-COUNT here, never a refusal (alembic 0037).

    ⛔ WHY THESE TESTS EXIST AT ALL. `archived_at` is enforced pre-spend in FOUR
    places elsewhere in the estate -- licences' licence-mint path
    (`licenses/main.py:3704`), its token-mint path (`licenses/main.py:4775`),
    Envoy ext_authz's `/license/introspect` (GATEWAY-scoped, blind to
    east-west traffic), and `routers/budget.py`'s pre-spend reservation gate --
    and NONE of them is this ingestion path. `inference-router` and
    `mlx-ingestion-backend` push cost events onto `billing:costs` directly,
    east-west, bypassing all four. Measured: tenant `rmt-e2e` billed $1.3151
    over 7 days while archived (licences #207, #197).

    By the time a row reaches `flush_costs` the compute is already spent, so
    the flusher must never refuse it -- `routers/budget.py`'s own comment
    states the general rule: dropping the row "converts an attribution fault
    into an UNMETERED one". These tests pin the row down on BOTH sides of that
    rule: an archived tenant's row is billed exactly like any other AND
    carries the flag; a non-archived tenant's row carries no flag at all.
    """

    @staticmethod
    def _seed(tenant_id: str, *, archived: bool) -> None:
        """Seed the registry TTL cache directly -- bypasses HTTP entirely.

        Mirrors `tests/test_budget_reserve_archived_tenant.py`'s own
        `_seed_registry`: patching `is_tenant_archived` itself would prove only
        that the flusher calls something, leaving the fail-open parse -- where
        the real risk lives -- untested.
        """
        from services import tenant_names
        from services.tenant_names import TenantInfo

        tenant_names._name_cache[tenant_names._CACHE_KEY] = {
            tenant_id.lower(): TenantInfo(name="test-tenant", archived=archived)
        }

    @pytest.fixture(autouse=True)
    def _clean_registry_cache(self):
        """The registry cache is process-wide; leaking it would make one
        test's archived tenant another test's mystery flag."""
        from services import tenant_names

        tenant_names.reset_cache()
        yield
        tenant_names.reset_cache()

    async def test_archived_tenant_row_is_persisted_flagged_and_counted(self, db_session_factory):
        import observability.metrics as om

        tenant = T("archived-billing-yes")
        self._seed(tenant, archived=True)

        before = om.cost_events_billed_while_archived_total.labels(
            service_key="inference-router"
        )._value.get()

        count = await flush_costs(
            [_make_cost(tenant_id=tenant, service_key="inference-router")],
            db_session_factory,
            None,
        )
        assert count == 1

        after = om.cost_events_billed_while_archived_total.labels(
            service_key="inference-router"
        )._value.get()
        assert after - before == 1

        async with db_session_factory() as session:
            row = (
                await session.execute(select(CostEvent).where(CostEvent.tenant_id == tenant))
            ).scalar_one()
            assert row.billed_while_archived is True
            # ⭐ THE POINT OF THIS SUITE: archival never parks the row. Both
            # must hold, or the flag has quietly become a second exclusion.
            assert row.excluded_from_billing is False
            assert row.exclusion_reason is None

    async def test_non_archived_tenant_row_is_persisted_not_flagged_counter_unmoved(
        self, db_session_factory
    ):
        import observability.metrics as om

        tenant = T("archived-billing-no")
        self._seed(tenant, archived=False)

        before = om.cost_events_billed_while_archived_total.labels(
            service_key="inference-router"
        )._value.get()

        count = await flush_costs(
            [_make_cost(tenant_id=tenant, service_key="inference-router")],
            db_session_factory,
            None,
        )
        assert count == 1

        after = om.cost_events_billed_while_archived_total.labels(
            service_key="inference-router"
        )._value.get()
        assert after - before == 0

        async with db_session_factory() as session:
            row = (
                await session.execute(select(CostEvent).where(CostEvent.tenant_id == tenant))
            ).scalar_one()
            assert row.billed_while_archived is False
            assert row.excluded_from_billing is False
            assert row.exclusion_reason is None
