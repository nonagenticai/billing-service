"""Tests for T1.4 per-endpoint pricing — provider_pricing.service_key.

The 4-rung resolution precedence in services.pricing._resolve_pricing_row:
  1. (provider, model, service_key) — exact match on all three
  2. (provider, model, NULL)        — model-specific default for any service
  3. (provider, NULL, service_key)  — provider-default for this service_key
  4. (provider, NULL, NULL)         — provider-default for anything

These tests seed concrete provider_pricing rows + invoke compute_cost_usd
with varying (provider, model, service_key) to verify each rung is selected
in isolation, then verify the partial-unique index permits one row per
triple but rejects duplicates.
"""

from __future__ import annotations

from datetime import UTC, datetime
from decimal import Decimal

import pytest
import sqlalchemy.exc
from sqlalchemy import insert, select

from models.db import ProviderPricing
from models.domain import ProviderPricingCreate
from services.pricing import compute_cost_usd, upsert_pricing

_NOW = datetime.now(UTC)


def _seed_row(
    *,
    provider: str,
    model: str | None,
    service_key: str | None,
    input_per_1m: str,
    output_per_1m: str,
    notes: str | None = None,
) -> dict:
    return {
        "provider": provider,
        "model": model,
        "service_key": service_key,
        "cost_model": "per_token",
        "input_per_1m_usd": Decimal(input_per_1m),
        "output_per_1m_usd": Decimal(output_per_1m),
        "effective_from": _NOW,
        "notes": notes,
    }


class TestResolutionPrecedence:
    @pytest.mark.unit
    async def test_rung1_exact_triple_wins_over_all_defaults(self, db_session_factory):
        """When (provider, model, service_key) all match, no default is consulted."""
        async with db_session_factory() as s:
            await s.execute(
                insert(ProviderPricing),
                [
                    _seed_row(
                        provider="claude",
                        model="claude-sonnet-4-20250514",
                        service_key="kag-graph",
                        input_per_1m="100",
                        output_per_1m="200",
                        notes="rung1",
                    ),
                    _seed_row(
                        provider="claude",
                        model="claude-sonnet-4-20250514",
                        service_key=None,
                        input_per_1m="3",
                        output_per_1m="15",
                        notes="rung2",
                    ),
                    _seed_row(
                        provider="claude",
                        model=None,
                        service_key="kag-graph",
                        input_per_1m="50",
                        output_per_1m="100",
                        notes="rung3",
                    ),
                    _seed_row(
                        provider="claude",
                        model=None,
                        service_key=None,
                        input_per_1m="1",
                        output_per_1m="2",
                        notes="rung4",
                    ),
                ],
            )
            await s.commit()

            cost, pricing_id = await compute_cost_usd(
                s,
                provider="claude",
                served_model="claude-sonnet-4-20250514",
                service_key="kag-graph",
                input_tokens=1_000_000,
                output_tokens=1_000_000,
                ts=_NOW,
            )
            # rung 1: 100 input + 200 output per 1M
            assert cost == Decimal("300.00000000")
            row = (
                await s.execute(select(ProviderPricing).where(ProviderPricing.id == pricing_id))
            ).scalar_one()
            assert row.notes == "rung1"

    @pytest.mark.unit
    async def test_rung2_model_default_when_no_service_match(self, db_session_factory):
        """Service_key on event has no matching row → fallback to (model, NULL)."""
        async with db_session_factory() as s:
            await s.execute(
                insert(ProviderPricing),
                [
                    _seed_row(
                        provider="claude",
                        model="claude-sonnet-4-20250514",
                        service_key="kag-graph",
                        input_per_1m="100",
                        output_per_1m="200",
                        notes="rung1",
                    ),
                    _seed_row(
                        provider="claude",
                        model="claude-sonnet-4-20250514",
                        service_key=None,
                        input_per_1m="3",
                        output_per_1m="15",
                        notes="rung2",
                    ),
                ],
            )
            await s.commit()

            cost, pricing_id = await compute_cost_usd(
                s,
                provider="claude",
                served_model="claude-sonnet-4-20250514",
                # different service_key → rung 1 misses → fallback to rung 2
                service_key="hirag",
                input_tokens=1_000_000,
                output_tokens=1_000_000,
                ts=_NOW,
            )
            assert cost == Decimal("18.00000000")  # 3 + 15
            row = (
                await s.execute(select(ProviderPricing).where(ProviderPricing.id == pricing_id))
            ).scalar_one()
            assert row.notes == "rung2"

    @pytest.mark.unit
    async def test_rung3_provider_service_default_when_no_model(self, db_session_factory):
        """Model has no row → fallback to provider default for this service_key."""
        async with db_session_factory() as s:
            await s.execute(
                insert(ProviderPricing),
                [
                    _seed_row(
                        provider="claude",
                        model=None,
                        service_key="kag-graph",
                        input_per_1m="50",
                        output_per_1m="100",
                        notes="rung3",
                    ),
                    _seed_row(
                        provider="claude",
                        model=None,
                        service_key=None,
                        input_per_1m="1",
                        output_per_1m="2",
                        notes="rung4",
                    ),
                ],
            )
            await s.commit()

            cost, pricing_id = await compute_cost_usd(
                s,
                provider="claude",
                served_model="some-new-unrouted-model",
                service_key="kag-graph",
                input_tokens=1_000_000,
                output_tokens=1_000_000,
                ts=_NOW,
            )
            assert cost == Decimal("150.00000000")
            row = (
                await s.execute(select(ProviderPricing).where(ProviderPricing.id == pricing_id))
            ).scalar_one()
            assert row.notes == "rung3"

    @pytest.mark.unit
    async def test_rung4_provider_default_catches_anything(self, db_session_factory):
        """No model+service match → falls all the way through to (NULL, NULL)."""
        async with db_session_factory() as s:
            await s.execute(
                insert(ProviderPricing),
                [
                    _seed_row(
                        provider="claude",
                        model=None,
                        service_key=None,
                        input_per_1m="1",
                        output_per_1m="2",
                        notes="rung4",
                    ),
                ],
            )
            await s.commit()

            cost, pricing_id = await compute_cost_usd(
                s,
                provider="claude",
                served_model="brand-new-model",
                service_key="brand-new-service",
                input_tokens=1_000_000,
                output_tokens=1_000_000,
                ts=_NOW,
            )
            assert cost == Decimal("3.00000000")
            row = (
                await s.execute(select(ProviderPricing).where(ProviderPricing.id == pricing_id))
            ).scalar_one()
            assert row.notes == "rung4"

    @pytest.mark.unit
    async def test_pre_t14_callers_unchanged(self, db_session_factory):
        """Pre-T1.4: caller passes no service_key. Existing rows (with NULL
        service_key) still match exactly as before — rungs 2 + 4 only."""
        async with db_session_factory() as s:
            await s.execute(
                insert(ProviderPricing),
                [
                    _seed_row(
                        provider="claude",
                        model="claude-sonnet-4-20250514",
                        service_key=None,
                        input_per_1m="3",
                        output_per_1m="15",
                        notes="rung2",
                    ),
                ],
            )
            await s.commit()

            cost, pricing_id = await compute_cost_usd(
                s,
                provider="claude",
                served_model="claude-sonnet-4-20250514",
                # no service_key kwarg — test backward-compat
                input_tokens=1_000_000,
                output_tokens=1_000_000,
                ts=_NOW,
            )
            assert cost == Decimal("18.00000000")
            assert pricing_id is not None

    @pytest.mark.unit
    async def test_no_match_returns_none(self, db_session_factory):
        """Empty pricing table → cost_usd is None, pricing_id is None."""
        async with db_session_factory() as s:
            cost, pricing_id = await compute_cost_usd(
                s,
                provider="unknown-provider",
                served_model="x",
                service_key="y",
                input_tokens=100,
                output_tokens=10,
                ts=_NOW,
            )
            assert cost is None
            assert pricing_id is None


class TestPartialUniqueIndex:
    @pytest.mark.unit
    async def test_open_row_per_triple_allowed(self, db_session_factory):
        """Three open rows with the same (provider, model) but different
        service_keys should all coexist — the new index is per triple."""
        async with db_session_factory() as s:
            await s.execute(
                insert(ProviderPricing),
                [
                    _seed_row(
                        provider="claude",
                        model="claude-sonnet-4-20250514",
                        service_key="kag-graph",
                        input_per_1m="100",
                        output_per_1m="200",
                    ),
                    _seed_row(
                        provider="claude",
                        model="claude-sonnet-4-20250514",
                        service_key="hirag",
                        input_per_1m="80",
                        output_per_1m="160",
                    ),
                    _seed_row(
                        provider="claude",
                        model="claude-sonnet-4-20250514",
                        service_key=None,
                        input_per_1m="3",
                        output_per_1m="15",
                    ),
                ],
            )
            await s.commit()
            n = (
                await s.execute(
                    select(ProviderPricing).where(
                        ProviderPricing.provider == "claude",
                        ProviderPricing.model == "claude-sonnet-4-20250514",
                    )
                )
            ).all()
            assert len(n) == 3

    @pytest.mark.unit
    async def test_duplicate_open_triple_rejected(self, db_session_factory):
        """Two open rows for the same (provider, model, service_key) are
        rejected by ix_providerpricing_open."""
        async with db_session_factory() as s:
            await s.execute(
                insert(ProviderPricing),
                [
                    _seed_row(
                        provider="claude",
                        model="claude-sonnet-4-20250514",
                        service_key="kag-graph",
                        input_per_1m="100",
                        output_per_1m="200",
                    ),
                ],
            )
            await s.commit()

            with pytest.raises(sqlalchemy.exc.IntegrityError):
                await s.execute(
                    insert(ProviderPricing),
                    [
                        _seed_row(
                            provider="claude",
                            model="claude-sonnet-4-20250514",
                            service_key="kag-graph",
                            input_per_1m="999",
                            output_per_1m="999",
                        ),
                    ],
                )
                await s.commit()


class TestUpsertScopedByServiceKey:
    @pytest.mark.unit
    async def test_upsert_with_service_key_only_closes_matching_open_row(self, db_session_factory):
        """upsert_pricing for (claude, model, kag-graph) must NOT close the
        (claude, model, NULL) default — that's a different row in the unique
        index and the default keeps charging unchanged for everyone else."""
        async with db_session_factory() as s:
            # Seed a (claude, model, NULL) default.
            default_row, _ = await upsert_pricing(
                s,
                ProviderPricingCreate(
                    provider="claude",
                    model="claude-sonnet-4-20250514",
                    service_key=None,
                    cost_model="per_token",
                    input_per_1m_usd=Decimal("3"),
                    output_per_1m_usd=Decimal("15"),
                ),
            )
            await s.commit()

            # Now add a service-scoped override.
            override, prior = await upsert_pricing(
                s,
                ProviderPricingCreate(
                    provider="claude",
                    model="claude-sonnet-4-20250514",
                    service_key="kag-graph",
                    cost_model="per_token",
                    input_per_1m_usd=Decimal("100"),
                    output_per_1m_usd=Decimal("200"),
                ),
            )
            await s.commit()
            # Prior should be None — there was no open row at THIS triple.
            assert prior is None

            # The default row stays open.
            refreshed = (
                await s.execute(select(ProviderPricing).where(ProviderPricing.id == default_row.id))
            ).scalar_one()
            assert refreshed.effective_until is None
            # Override is also open.
            assert override.effective_until is None
