"""Aggregator authoritative-cost path — Phase 4.

inference-router emits cost_usd as a hint. metering computes the authoritative
value from provider_pricing and stores both, plus pricing_id. This file pins
the contract: pricing miss preserves the hint and logs, drift >5% logs, drift
within 5% does not log, cache tokens flow through, and back-compat with the
legacy `model` field works when `served_model` is absent.
"""

from __future__ import annotations

import uuid
from datetime import UTC, datetime
from decimal import Decimal

import pytest
from sqlalchemy import select

from models.db import CostEvent, ProviderPricing
from services.aggregator import flush_costs
from tests.conftest import T


def _seed_anthropic_pricing(input_rate: str = "3", output_rate: str = "15") -> ProviderPricing:
    return ProviderPricing(
        provider="anthropic",
        model="claude-3-5-sonnet-20241022",
        cost_model="per_token",
        input_per_1m_usd=Decimal(input_rate),
        output_per_1m_usd=Decimal(output_rate),
        effective_from=datetime(2026, 1, 1, tzinfo=UTC),
    )


def _make_cost_payload(**overrides) -> dict[str, str]:
    """Stream-shape payload mirroring inference-router 72f9ced."""
    base = {
        "tenant_id": T("test-tenant"),
        "license_id": str(uuid.uuid4()),
        "service_key": "inference-router",
        "provider": "anthropic",
        "model": "anthropic-sonnet",  # legacy alias / schema id
        "served_model": "claude-3-5-sonnet-20241022",
        "input_tokens": "1000",
        "output_tokens": "500",
        "cache_creation_input_tokens": "0",
        "cache_read_input_tokens": "0",
        "cost_usd": "0.0105",  # hint
        "latency_ms": "100",
        "provider_request_id": "req_abc123",
    }
    base.update(overrides)
    return base


@pytest.mark.unit
class TestAuthoritativeCost:
    async def test_authoritative_overrides_hint(self, db_session_factory) -> None:
        """Hint $0.10, authoritative ~$0.0105 → drift logged, authoritative wins."""
        async with db_session_factory() as s:
            s.add(_seed_anthropic_pricing())
            await s.commit()

        # Hint is wildly off (almost 10x) — drift must fire.
        buffer = [_make_cost_payload(cost_usd="0.10")]
        await flush_costs(buffer, db_session_factory, None)

        async with db_session_factory() as s:
            row = (await s.execute(select(CostEvent))).scalar_one()
            # 1000 * 3 / 1e6 + 500 * 15 / 1e6 = 0.0105
            assert row.cost_usd == Decimal("0.01050000")
            assert row.cost_usd_hint == Decimal("0.10")
            assert row.pricing_id is not None
            assert row.served_model == "claude-3-5-sonnet-20241022"
            assert row.provider_request_id == "req_abc123"

    async def test_drift_logged_when_above_5pct(self, db_session_factory, caplog, capsys) -> None:
        import logging

        async with db_session_factory() as s:
            s.add(_seed_anthropic_pricing())
            await s.commit()

        with caplog.at_level(logging.WARNING):
            # Hint differs by ~9.5x → ratio >> 5%.
            buffer = [_make_cost_payload(cost_usd="0.10")]
            await flush_costs(buffer, db_session_factory, None)

        captured = capsys.readouterr()
        record_text = " ".join(r.getMessage() for r in caplog.records)
        haystack = captured.out + captured.err + record_text
        assert "cost_drift" in haystack

    async def test_no_drift_log_when_within_5pct(self, db_session_factory, caplog, capsys) -> None:
        import logging

        async with db_session_factory() as s:
            s.add(_seed_anthropic_pricing())
            await s.commit()

        with caplog.at_level(logging.WARNING):
            # authoritative=$0.0105; hint=$0.0107 → ratio ≈ 1.9% < 5%.
            buffer = [_make_cost_payload(cost_usd="0.0107")]
            await flush_costs(buffer, db_session_factory, None)

        captured = capsys.readouterr()
        record_text = " ".join(r.getMessage() for r in caplog.records)
        haystack = captured.out + captured.err + record_text
        assert "cost_drift" not in haystack


@pytest.mark.unit
class TestAbsentHintIsNotDrift:
    """A zero hint means the emitter did not price the call — that is not drift.

    ``_parse_decimal`` (services/_parsers.py:81) returns ``Decimal("0")`` for a
    missing key, an empty string AND an unparseable one, so on the stream path a
    zero hint is indistinguishable from an absent one. Reporting it as drift is
    a claim the wire cannot support, and in production it was 78% of all
    ``cost_drift`` warnings (701 of 894 in a 2h window, 2026-08-11) — enough to
    bury the genuine hint-vs-authoritative disagreements it exists to surface.
    """

    @staticmethod
    def _haystack(caplog, capsys) -> str:
        """Combined log text. NB ``capsys.readouterr()`` drains the buffer, so
        call this exactly once per test and reuse the string."""
        captured = capsys.readouterr()
        record_text = " ".join(r.getMessage() for r in caplog.records)
        return captured.out + captured.err + record_text

    @staticmethod
    def _levels_for(haystack: str, event: str) -> list[str]:
        """Levels of every rendered line mentioning ``event``.

        The LEVEL is the entire point of this change — the defect was WARNING
        VOLUME — so a test that asserts only the event NAME survives a mutant
        that leaves the event at warning. This closes that gap.

        Parsed out of the RENDERED text rather than ``caplog.records``: structlog
        is configured with a console renderer writing to stdout, so these events
        never reach a stdlib handler and ``caplog.records`` is empty for them.
        (Asserting on caplog here silently passed a vacuous ``[] == []``.)
        Rendered shape: ``2026-08-11 10:50:20 [info     ] cost_hint_absent ...``
        """
        levels = []
        for line in haystack.splitlines():
            if event in line and "[" in line:
                levels.append(line.split("[", 1)[1].split("]", 1)[0].strip().upper())
        return levels

    # "not-a-number" is deliberately ABSENT: validate_cost_event returns
    # "invalid_cost_usd" for it (services/_cost_schema.py:67), so it is
    # dead-lettered before pricing and never reaches this branch at all.
    # Asserting cost_hint_absent for it would pin a contract the code does not
    # have; test_an_unparseable_hint_is_dead_lettered_not_absent pins the real one.
    @pytest.mark.parametrize("hint", ["0", "0E-8", "0.00", ""])
    async def test_zero_or_empty_hint_is_reported_as_absent_not_drift(
        self, db_session_factory, caplog, capsys, hint
    ) -> None:
        import logging

        async with db_session_factory() as s:
            s.add(_seed_anthropic_pricing())
            await s.commit()

        with caplog.at_level(logging.INFO):
            await flush_costs([_make_cost_payload(cost_usd=hint)], db_session_factory, None)

        haystack = self._haystack(caplog, capsys)
        assert "cost_drift" not in haystack
        assert "cost_hint_absent" in haystack
        # The LEVEL is the point: this defect was WARNING VOLUME, so demoting
        # the event name without demoting its level fixes nothing.
        assert self._levels_for(haystack, "cost_hint_absent") == ["INFO"]

    async def test_an_unparseable_hint_is_dead_lettered_not_absent(
        self, db_session_factory, caplog, capsys
    ) -> None:
        """The boundary: a garbage cost is QUARANTINED, not reclassified as absent.

        Pins that this change did not widen the absent-hint branch to swallow
        events the schema validator is supposed to reject.
        """
        import logging

        async with db_session_factory() as s:
            s.add(_seed_anthropic_pricing())
            await s.commit()

        with caplog.at_level(logging.INFO):
            await flush_costs(
                [_make_cost_payload(cost_usd="not-a-number")], db_session_factory, None
            )

        haystack = self._haystack(caplog, capsys)
        assert "cost_hint_absent" not in haystack
        assert "cost_drift" not in haystack
        async with db_session_factory() as s:
            assert (await s.execute(select(CostEvent))).first() is None

    async def test_the_authoritative_cost_is_still_stored(self, db_session_factory) -> None:
        """Reclassifying the LOG must not change what is BILLED."""
        async with db_session_factory() as s:
            s.add(_seed_anthropic_pricing())
            await s.commit()

        await flush_costs([_make_cost_payload(cost_usd="0")], db_session_factory, None)

        async with db_session_factory() as s:
            row = (await s.execute(select(CostEvent))).scalar_one()
            assert row.cost_usd == Decimal("0.01050000")
            assert row.cost_usd_hint == Decimal("0")

    async def test_a_real_disagreement_still_fires_cost_drift(
        self, db_session_factory, caplog, capsys
    ) -> None:
        """The GREEN half — the surviving warning must still be reachable.

        Without this, a change that silenced cost_drift entirely would pass the
        RED half above and look like a fix.
        """
        import logging

        async with db_session_factory() as s:
            s.add(_seed_anthropic_pricing())
            await s.commit()

        with caplog.at_level(logging.INFO):
            await flush_costs([_make_cost_payload(cost_usd="0.10")], db_session_factory, None)

        haystack = self._haystack(caplog, capsys)
        assert "cost_drift" in haystack
        assert "cost_hint_absent" not in haystack
        # The surviving warning must still BE a warning.
        assert self._levels_for(haystack, "cost_drift") == ["WARNING"]


@pytest.mark.unit
class TestPricingMiss:
    async def test_no_pricing_row_persists_event_with_null_cost(
        self, db_session_factory, caplog, capsys
    ) -> None:
        import logging

        # NO pricing row seeded — authoritative cost should be NULL.
        with caplog.at_level(logging.WARNING):
            buffer = [_make_cost_payload(cost_usd="0.10")]
            await flush_costs(buffer, db_session_factory, None)

        async with db_session_factory() as s:
            row = (await s.execute(select(CostEvent))).scalar_one()
            assert row.cost_usd is None
            assert row.cost_usd_hint == Decimal("0.10")  # hint preserved
            assert row.pricing_id is None
            assert row.served_model == "claude-3-5-sonnet-20241022"

        captured = capsys.readouterr()
        record_text = " ".join(r.getMessage() for r in caplog.records)
        haystack = captured.out + captured.err + record_text
        assert "pricing_miss" in haystack


@pytest.mark.unit
class TestCacheTokens:
    async def test_cache_tokens_flow_through(self, db_session_factory) -> None:
        async with db_session_factory() as s:
            s.add(
                ProviderPricing(
                    provider="anthropic",
                    model="claude-3-5-sonnet-20241022",
                    cost_model="per_token",
                    input_per_1m_usd=Decimal("3"),
                    output_per_1m_usd=Decimal("15"),
                    cache_creation_per_1m_usd=Decimal("3.75"),
                    cache_read_per_1m_usd=Decimal("0.30"),
                    effective_from=datetime(2026, 1, 1, tzinfo=UTC),
                )
            )
            await s.commit()

        buffer = [
            _make_cost_payload(
                input_tokens="1000",
                output_tokens="500",
                cache_creation_input_tokens="2000",
                cache_read_input_tokens="4000",
                cost_usd="0",  # let drift fire if anything
            )
        ]
        await flush_costs(buffer, db_session_factory, None)

        async with db_session_factory() as s:
            row = (await s.execute(select(CostEvent))).scalar_one()
            # 0.003 + 0.0075 + 0.0075 + 0.0012 = 0.0192
            assert row.cost_usd == Decimal("0.01920000")
            assert row.cache_creation_input_tokens == 2000
            assert row.cache_read_input_tokens == 4000


@pytest.mark.unit
class TestServedModelBackcompat:
    async def test_falls_back_to_legacy_model_when_served_model_absent(
        self, db_session_factory
    ) -> None:
        """An old-shape event with only `model` set should still price correctly
        if a per-model pricing row matches the legacy model name."""
        async with db_session_factory() as s:
            s.add(
                ProviderPricing(
                    provider="anthropic",
                    model="claude-3-5-sonnet-20241022",
                    cost_model="per_token",
                    input_per_1m_usd=Decimal("3"),
                    output_per_1m_usd=Decimal("15"),
                    effective_from=datetime(2026, 1, 1, tzinfo=UTC),
                )
            )
            await s.commit()

        # No served_model key in payload — legacy `model` carries the actual model.
        payload = _make_cost_payload(model="claude-3-5-sonnet-20241022")
        payload.pop("served_model", None)

        buffer = [payload]
        await flush_costs(buffer, db_session_factory, None)

        async with db_session_factory() as s:
            row = (await s.execute(select(CostEvent))).scalar_one()
            assert row.cost_usd == Decimal("0.01050000")
            assert row.served_model == "claude-3-5-sonnet-20241022"  # filled from `model`
            assert row.pricing_id is not None

    async def test_streaming_zero_token_event_still_persisted(self, db_session_factory) -> None:
        """Legacy 0/0 token event (e.g. stream-tracking heartbeat) — pricing
        miss yields cost_usd=NULL but the row is persisted."""
        # No pricing row seeded — guaranteed miss.
        payload = _make_cost_payload(input_tokens="0", output_tokens="0", cost_usd="0")
        await flush_costs([payload], db_session_factory, None)

        async with db_session_factory() as s:
            row = (await s.execute(select(CostEvent))).scalar_one()
            assert row.input_tokens == 0
            assert row.output_tokens == 0
            assert row.cost_usd is None  # pricing miss
            assert row.cost_usd_hint == Decimal("0")
