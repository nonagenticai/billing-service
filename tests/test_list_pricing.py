"""Tests for rate_class (cost vs list) + the read-time BYO list estimator."""

from __future__ import annotations

from datetime import UTC, datetime
from decimal import Decimal
from typing import Any, cast

import pytest
from pydantic import ValidationError
from sqlalchemy import select
from sqlalchemy.exc import IntegrityError

from models.db import ProviderPricing
from models.domain import ProviderPricingCreate, TenantConfigUpdate
from services import pricing as pricing_svc
from services.list_pricing import build_list_pricing_resolver

_FROM = datetime(2026, 1, 1, tzinfo=UTC)
_TS = datetime(2026, 4, 1, tzinfo=UTC)


def _row(
    *,
    provider: str,
    model: str | None = None,
    service_key: str | None = None,
    rate_class: str = "cost",
    input_per_1m_usd: Decimal | None = None,
    output_per_1m_usd: Decimal | None = None,
    effective_from: datetime = _FROM,
    effective_until: datetime | None = None,
) -> ProviderPricing:
    return ProviderPricing(
        provider=provider,
        model=model,
        service_key=service_key,
        rate_class=rate_class,
        cost_model="per_token",
        input_per_1m_usd=input_per_1m_usd,
        output_per_1m_usd=output_per_1m_usd,
        effective_from=effective_from,
        effective_until=effective_until,
    )


@pytest.mark.unit
class TestRateClassIsolation:
    async def test_cost_hot_path_ignores_list_row(self, db_session) -> None:
        """A 'list' row for the same tuple must not affect the authoritative cost."""
        db_session.add(
            _row(
                provider="openai",
                model="gpt-4o",
                input_per_1m_usd=Decimal("2.5"),
                output_per_1m_usd=Decimal("10"),
            )
        )
        db_session.add(
            _row(
                provider="openai",
                model="gpt-4o",
                rate_class="list",
                input_per_1m_usd=Decimal("99"),
                output_per_1m_usd=Decimal("99"),
            )
        )
        await db_session.commit()

        cost, _pid = await pricing_svc.compute_cost_usd(
            db_session,
            provider="openai",
            served_model="gpt-4o",
            input_tokens=1000,
            output_tokens=1000,
            ts=_TS,
        )
        # 1000*2.5/1e6 + 1000*10/1e6 = 0.0025 + 0.01 = 0.0125 (cost, not list 99)
        assert cost == Decimal("0.01250000")

    async def test_list_resolver_resolves_list_only(self, db_session) -> None:
        db_session.add(
            _row(
                provider="openai",
                model="gpt-4o",
                input_per_1m_usd=Decimal("2.5"),
                output_per_1m_usd=Decimal("10"),
            )
        )
        db_session.add(
            _row(
                provider="openai",
                model="gpt-4o",
                rate_class="list",
                input_per_1m_usd=Decimal("5"),
                output_per_1m_usd=Decimal("20"),
            )
        )
        await db_session.commit()

        resolver = await build_list_pricing_resolver(db_session, _TS)
        est = resolver.estimate(
            provider="openai",
            served_model="gpt-4o",
            service_key=None,
            input_tokens=1000,
            output_tokens=1000,
        )
        # list rate: 1000*5/1e6 + 1000*20/1e6 = 0.005 + 0.02 = 0.025
        assert est == Decimal("0.02500000")

    async def test_estimate_none_when_no_list_row(self, db_session) -> None:
        db_session.add(
            _row(provider="openai", model="gpt-4o", input_per_1m_usd=Decimal("2.5"))
        )  # cost-only
        await db_session.commit()
        resolver = await build_list_pricing_resolver(db_session, _TS)
        assert (
            resolver.estimate(
                provider="openai",
                served_model="gpt-4o",
                service_key=None,
                input_tokens=10,
                output_tokens=10,
            )
            is None
        )
        # And provider=None short-circuits.
        assert (
            resolver.estimate(
                provider=None,
                served_model=None,
                service_key=None,
                input_tokens=10,
                output_tokens=10,
            )
            is None
        )

    async def test_estimate_4rung_precedence_provider_default(self, db_session) -> None:
        """A provider-default (model NULL) 'list' row covers an unlisted model."""
        db_session.add(
            _row(
                provider="deepseek",
                model=None,
                rate_class="list",
                input_per_1m_usd=Decimal("1"),
                output_per_1m_usd=Decimal("2"),
            )
        )
        await db_session.commit()
        resolver = await build_list_pricing_resolver(db_session, _TS)
        est = resolver.estimate(
            provider="deepseek",
            served_model="deepseek-v4-flash",
            service_key="kag-graph",  # pragma: allowlist secret
            input_tokens=1000,
            output_tokens=0,
        )
        assert est == Decimal("0.00100000")

    async def test_upsert_list_does_not_close_open_cost_row(self, db_session) -> None:
        # Seed an open cost row via upsert.
        await pricing_svc.upsert_pricing(
            db_session,
            ProviderPricingCreate(
                provider="openai",
                model="gpt-4o",
                cost_model="per_token",
                input_per_1m_usd=Decimal("2.5"),
                effective_from=_FROM,
            ),
            ts=_FROM,
        )
        # Now upsert a list row for the same tuple.
        await pricing_svc.upsert_pricing(
            db_session,
            ProviderPricingCreate(
                provider="openai",
                model="gpt-4o",
                rate_class="list",
                cost_model="per_token",
                input_per_1m_usd=Decimal("5"),
                effective_from=_TS,
            ),
            ts=_TS,
        )
        await db_session.commit()
        open_rows = (
            (
                await db_session.execute(
                    select(ProviderPricing).where(ProviderPricing.effective_until.is_(None))
                )
            )
            .scalars()
            .all()
        )
        by_class = {r.rate_class for r in open_rows}
        # Both remain OPEN — the list upsert did not close the cost row.
        assert by_class == {"cost", "list"}
        assert len(open_rows) == 2


@pytest.mark.unit
class TestOpenRowUniqueness:
    async def test_one_open_cost_and_one_open_list_coexist(self, db_session) -> None:
        db_session.add(
            _row(
                provider="openai",
                model="gpt-4o",
                rate_class="cost",
                input_per_1m_usd=Decimal("2.5"),
            )
        )
        db_session.add(
            _row(
                provider="openai", model="gpt-4o", rate_class="list", input_per_1m_usd=Decimal("5")
            )
        )
        await db_session.commit()  # must NOT raise — unique includes rate_class

    async def test_two_open_cost_rows_same_tuple_rejected(self, db_session) -> None:
        db_session.add(_row(provider="openai", model="gpt-4o", input_per_1m_usd=Decimal("2.5")))
        db_session.add(_row(provider="openai", model="gpt-4o", input_per_1m_usd=Decimal("3")))
        with pytest.raises(IntegrityError):
            await db_session.commit()


@pytest.mark.unit
class TestDomainValidation:
    def test_invalid_billing_mode_rejected(self) -> None:
        with pytest.raises(ValidationError):
            TenantConfigUpdate(markup_pct=0, billing_mode=cast(Any, "bogus_mode"))

    def test_valid_billing_modes_accepted(self) -> None:
        for mode in ("standard_markup", "byo_platform_fee", "pass_through"):
            assert TenantConfigUpdate(markup_pct=0, billing_mode=mode).billing_mode == mode
        # None (omit) stays valid.
        assert TenantConfigUpdate(markup_pct=0).billing_mode is None

    def test_invalid_rate_class_rejected(self) -> None:
        with pytest.raises(ValidationError):
            ProviderPricingCreate(
                provider="openai",
                cost_model="per_token",
                input_per_1m_usd=Decimal("1"),
                rate_class=cast(Any, "bogus"),
            )

    def test_rate_class_defaults_to_cost(self) -> None:
        assert (
            ProviderPricingCreate(
                provider="openai", cost_model="per_token", input_per_1m_usd=Decimal("1")
            ).rate_class
            == "cost"
        )
