"""Tests for the pricing service — metering's authoritative cost source."""

from __future__ import annotations

from datetime import UTC, datetime
from decimal import Decimal
from typing import Any, cast

import pytest
from prometheus_client import REGISTRY
from pydantic import ValidationError
from sqlalchemy import select

from models.db import ProviderPricing
from models.domain import ProviderPricingCreate
from services import pricing as pricing_svc


def _malformed_count(*, provider: str, model: str, cost_model: str) -> float:
    """Current value of billing_pricing_row_malformed_total for one label set."""
    value = REGISTRY.get_sample_value(
        "billing_pricing_row_malformed_total",
        {"provider": provider, "model": model, "cost_model": cost_model},
    )
    return float(value or 0.0)


def _seed(
    *,
    provider: str,
    model: str | None,
    service_key: str | None = None,
    cost_model: str = "per_token",
    cost_basis: str | None = None,
    input_per_1m_usd: Decimal | None = None,
    output_per_1m_usd: Decimal | None = None,
    cache_creation_per_1m_usd: Decimal | None = None,
    cache_read_per_1m_usd: Decimal | None = None,
    per_request_usd: Decimal | None = None,
    monthly_fixed_cost_usd: Decimal | None = None,
    effective_from: datetime,
    effective_until: datetime | None = None,
) -> ProviderPricing:
    kwargs: dict = {}
    if cost_basis is not None:
        kwargs["cost_basis"] = cost_basis
    return ProviderPricing(
        provider=provider,
        model=model,
        service_key=service_key,
        cost_model=cost_model,
        input_per_1m_usd=input_per_1m_usd,
        output_per_1m_usd=output_per_1m_usd,
        cache_creation_per_1m_usd=cache_creation_per_1m_usd,
        cache_read_per_1m_usd=cache_read_per_1m_usd,
        per_request_usd=per_request_usd,
        monthly_fixed_cost_usd=monthly_fixed_cost_usd,
        effective_from=effective_from,
        effective_until=effective_until,
        **kwargs,
    )


@pytest.mark.unit
class TestComputeCostUsdPerToken:
    async def test_basic_input_output(self, db_session) -> None:
        """Anthropic-like rates: input $3/M, output $15/M."""
        ts = datetime(2026, 4, 1, tzinfo=UTC)
        db_session.add(
            _seed(
                provider="anthropic",
                model="claude-3-5-sonnet-20241022",
                input_per_1m_usd=Decimal("3"),
                output_per_1m_usd=Decimal("15"),
                effective_from=datetime(2026, 1, 1, tzinfo=UTC),
            )
        )
        await db_session.commit()

        cost, pid = await pricing_svc.compute_cost_usd(
            db_session,
            provider="anthropic",
            served_model="claude-3-5-sonnet-20241022",
            input_tokens=1000,
            output_tokens=500,
            ts=ts,
        )
        # 1000 * 3 / 1_000_000 + 500 * 15 / 1_000_000 = 0.003 + 0.0075 = 0.0105
        assert cost == Decimal("0.01050000")
        assert pid is not None

    async def test_cache_tokens_explicit_rates(self, db_session) -> None:
        """Cache rates differentiate from base input rate."""
        ts = datetime(2026, 4, 1, tzinfo=UTC)
        db_session.add(
            _seed(
                provider="anthropic",
                model="claude-3-5-sonnet-20241022",
                input_per_1m_usd=Decimal("3"),
                output_per_1m_usd=Decimal("15"),
                cache_creation_per_1m_usd=Decimal("3.75"),
                cache_read_per_1m_usd=Decimal("0.30"),
                effective_from=datetime(2026, 1, 1, tzinfo=UTC),
            )
        )
        await db_session.commit()

        cost, _pid = await pricing_svc.compute_cost_usd(
            db_session,
            provider="anthropic",
            served_model="claude-3-5-sonnet-20241022",
            input_tokens=1000,
            output_tokens=500,
            cache_creation_input_tokens=2000,
            cache_read_input_tokens=4000,
            ts=ts,
        )
        # input  = 1000  * 3.00 / 1e6 = 0.003
        # output = 500   * 15.0 / 1e6 = 0.0075
        # ccwrt  = 2000  * 3.75 / 1e6 = 0.0075
        # cread  = 4000  * 0.30 / 1e6 = 0.0012
        # total = 0.0192
        assert cost == Decimal("0.01920000")

    async def test_cache_tokens_fallback_to_input_rate(self, db_session) -> None:
        """NULL cache rates fall back to plain input_per_1m_usd."""
        ts = datetime(2026, 4, 1, tzinfo=UTC)
        db_session.add(
            _seed(
                provider="openai",
                model=None,
                input_per_1m_usd=Decimal("2"),
                output_per_1m_usd=Decimal("8"),
                effective_from=datetime(2026, 1, 1, tzinfo=UTC),
            )
        )
        await db_session.commit()

        cost, _pid = await pricing_svc.compute_cost_usd(
            db_session,
            provider="openai",
            served_model="gpt-4o-mini",
            input_tokens=0,
            output_tokens=0,
            cache_creation_input_tokens=1_000_000,
            cache_read_input_tokens=2_000_000,
            ts=ts,
        )
        # Both cache classes use input rate ($2/M).
        # 1M*$2/M + 2M*$2/M = $2 + $4 = $6
        assert cost == Decimal("6.00000000")


@pytest.mark.unit
class TestEffectiveDated:
    async def test_lookup_picks_row_active_at_ts(self, db_session) -> None:
        """Two rows with non-overlapping windows; the matching window wins."""
        first_from = datetime(2026, 1, 1, tzinfo=UTC)
        first_until = datetime(2026, 4, 1, tzinfo=UTC)
        second_from = datetime(2026, 4, 1, tzinfo=UTC)

        db_session.add(
            _seed(
                provider="anthropic",
                model=None,
                input_per_1m_usd=Decimal("2"),
                output_per_1m_usd=Decimal("10"),
                effective_from=first_from,
                effective_until=first_until,
            )
        )
        db_session.add(
            _seed(
                provider="anthropic",
                model=None,
                input_per_1m_usd=Decimal("3"),
                output_per_1m_usd=Decimal("15"),
                effective_from=second_from,
            )
        )
        await db_session.commit()

        # Pull the two row IDs for assertions.
        rows = (
            (await db_session.execute(select(ProviderPricing).order_by(ProviderPricing.id)))
            .scalars()
            .all()
        )
        first_id, second_id = rows[0].id, rows[1].id

        # Inside the first window
        _cost, pid_old = await pricing_svc.compute_cost_usd(
            db_session,
            provider="anthropic",
            served_model=None,
            input_tokens=1000,
            output_tokens=0,
            ts=datetime(2026, 3, 15, tzinfo=UTC),
        )
        assert pid_old == first_id

        # Inside the second window
        _cost, pid_new = await pricing_svc.compute_cost_usd(
            db_session,
            provider="anthropic",
            served_model=None,
            input_tokens=1000,
            output_tokens=0,
            ts=datetime(2026, 4, 15, tzinfo=UTC),
        )
        assert pid_new == second_id


@pytest.mark.unit
class TestProviderDefaultFallback:
    async def test_specific_model_wins_over_default(self, db_session) -> None:
        ts = datetime(2026, 4, 1, tzinfo=UTC)
        db_session.add(
            _seed(
                provider="anthropic",
                model=None,  # provider default
                input_per_1m_usd=Decimal("2"),
                output_per_1m_usd=Decimal("10"),
                effective_from=datetime(2026, 1, 1, tzinfo=UTC),
            )
        )
        db_session.add(
            _seed(
                provider="anthropic",
                model="claude-3-5-sonnet-20241022",
                input_per_1m_usd=Decimal("3"),
                output_per_1m_usd=Decimal("15"),
                effective_from=datetime(2026, 1, 1, tzinfo=UTC),
            )
        )
        await db_session.commit()

        cost_specific, _pid = await pricing_svc.compute_cost_usd(
            db_session,
            provider="anthropic",
            served_model="claude-3-5-sonnet-20241022",
            input_tokens=1_000_000,
            output_tokens=0,
            ts=ts,
        )
        # 1M tokens * $3/M = $3 — uses the specific row, not the default.
        assert cost_specific == Decimal("3.00000000")

    async def test_unknown_model_falls_back_to_default(self, db_session) -> None:
        ts = datetime(2026, 4, 1, tzinfo=UTC)
        db_session.add(
            _seed(
                provider="anthropic",
                model=None,
                input_per_1m_usd=Decimal("2"),
                output_per_1m_usd=Decimal("10"),
                effective_from=datetime(2026, 1, 1, tzinfo=UTC),
            )
        )
        db_session.add(
            _seed(
                provider="anthropic",
                model="claude-3-5-sonnet-20241022",
                input_per_1m_usd=Decimal("3"),
                output_per_1m_usd=Decimal("15"),
                effective_from=datetime(2026, 1, 1, tzinfo=UTC),
            )
        )
        await db_session.commit()

        cost_default, _pid = await pricing_svc.compute_cost_usd(
            db_session,
            provider="anthropic",
            served_model="claude-3-haiku-20240307",
            input_tokens=1_000_000,
            output_tokens=0,
            ts=ts,
        )
        # 1M tokens * $2/M = $2 — falls back to the default row.
        assert cost_default == Decimal("2.00000000")


@pytest.mark.unit
class TestMixedFamilyDefaultRefusal:
    """A provider with rows in >1 cost_model family must NOT price an unknown
    model off its provider-default — the default is one unit and the event may
    be another.

    Regression for the 40x under-bill: openai fronts text tokens ($2.50/1M tok)
    AND audio seconds ($100/1M sec). Before this guard, an ASR event whose
    per-model row was missing fell through to the text default and billed
    seconds at the token rate — 40x under. The inverse (an unseeded embedding
    priced at the text default instead of $0.005) is a 500x OVER-bill. Both are
    fixed by refusing the default and emitting a loud pricing_miss instead.
    """

    async def test_mixed_family_unknown_model_is_a_miss_not_the_text_default(
        self, db_session
    ) -> None:
        ts = datetime(2026, 4, 1, tzinfo=UTC)
        eff = datetime(2026, 1, 1, tzinfo=UTC)
        # openai fronts BOTH a per_token text default and a per_second ASR row.
        db_session.add(
            _seed(
                provider="openai",
                model=None,  # text provider-default
                cost_model="per_token",
                input_per_1m_usd=Decimal("2.50"),
                output_per_1m_usd=Decimal("10"),
                effective_from=eff,
            )
        )
        db_session.add(
            _seed(
                provider="openai",
                model="whisper-1",
                cost_model="per_second",
                input_per_1m_usd=Decimal("100"),
                output_per_1m_usd=Decimal("0"),
                effective_from=eff,
            )
        )
        await db_session.commit()

        # An unseeded openai ASR model (input_tokens = audio seconds). The old
        # behaviour resolved rung 4 -> the $2.50 text default -> 60 sec priced
        # at 60*2.50/1e6 = $0.00015 (40x under the $0.006 it should cost).
        cost, pid = await pricing_svc.compute_cost_usd(
            db_session,
            provider="openai",
            served_model="whisper-2-nonexistent",
            input_tokens=60,
            output_tokens=0,
            ts=ts,
        )
        # Mixed-family provider -> refuse the default -> loud miss, not a guess.
        assert cost is None
        assert pid is None

    async def test_single_family_provider_still_uses_its_default(self, db_session) -> None:
        """The guard is a no-op for the common case: a provider whose rows are
        all one family keeps falling back to its default exactly as before."""
        ts = datetime(2026, 4, 1, tzinfo=UTC)
        eff = datetime(2026, 1, 1, tzinfo=UTC)
        # deepseek: every row is per_token, so the default is unambiguous.
        db_session.add(
            _seed(
                provider="deepseek",
                model=None,
                cost_model="per_token",
                input_per_1m_usd=Decimal("0.435"),
                output_per_1m_usd=Decimal("0.87"),
                effective_from=eff,
            )
        )
        db_session.add(
            _seed(
                provider="deepseek",
                model="deepseek-v4-flash",
                cost_model="per_token",
                input_per_1m_usd=Decimal("0.14"),
                output_per_1m_usd=Decimal("0.28"),
                effective_from=eff,
            )
        )
        await db_session.commit()

        cost, pid = await pricing_svc.compute_cost_usd(
            db_session,
            provider="deepseek",
            served_model="deepseek-v9-unreleased",
            input_tokens=1_000_000,
            output_tokens=0,
            ts=ts,
        )
        # Single family -> default applies: 1M * $0.435/M = $0.435.
        assert cost == Decimal("0.43500000")
        assert pid is not None


@pytest.mark.unit
class TestServiceKeyRungsDoNotCollapse:
    """Rung 1 and rung 2 must stay distinct when a (provider, model) carries two
    open rows that differ ONLY in ``service_key`` — and in cost_model FAMILY.

    ⚠️ DEFENSIVE, NOT A LIVE DEFECT. Live ``provider_pricing`` holds exactly this
    shape for ``video-analyzer`` / ``local``: id 32 (``service_key=
    'w2v-video-analyzer'``, ``cost_model='per_token'``) and id 117
    (``service_key=NULL``, ``cost_model='per_second'``), both open on
    ``rate_class='cost'``. Every live event carries
    ``service_key='w2v-video-analyzer'``, so rung 1 wins on every request and id
    117 is never reached. Nothing is mispriced today and this test asserts no
    production behaviour change.

    It is pinned because the two rows are NOT interchangeable. video-analyzer's
    average call is ~115 seconds, so the per_second row and the per_token row
    price the same event orders of magnitude apart. A resolver that ignored
    ``service_key``, or that tried rung 2 before rung 1, would hand all live
    per_token traffic to the per_second row silently — no pricing_miss, no NULL
    ``cost_usd``, nothing to reprice. Note also that the mixed-family refusal at
    rung 4 does NOT protect this case: both rows pin ``model='local'``, so the
    walk never reaches the provider-default rung at all.
    """

    _EFF = datetime(2026, 1, 1, tzinfo=UTC)
    _TS = datetime(2026, 4, 1, tzinfo=UTC)

    async def _seed_video_analyzer(self, db_session) -> None:
        """The live shape: one (provider, model), two service_key scopes, two families."""
        db_session.add(
            _seed(
                provider="video-analyzer",
                model="local",
                service_key="w2v-video-analyzer",  # id 32 in prod
                cost_model="per_token",
                input_per_1m_usd=Decimal("0.50"),
                output_per_1m_usd=Decimal("1.50"),
                effective_from=self._EFF,
            )
        )
        db_session.add(
            _seed(
                provider="video-analyzer",
                model="local",
                service_key=None,  # id 117 in prod
                cost_model="per_second",
                input_per_1m_usd=Decimal("100"),
                output_per_1m_usd=Decimal("0"),
                effective_from=self._EFF,
            )
        )
        await db_session.commit()

    async def test_each_service_key_scope_resolves_its_own_row(self, db_session) -> None:
        await self._seed_video_analyzer(db_session)

        # Rung 1 — the scope ALL live traffic carries.
        keyed = await pricing_svc._resolve_pricing_row(
            db_session,
            provider="video-analyzer",
            served_model="local",
            service_key="w2v-video-analyzer",
            ts=self._TS,
        )
        assert keyed is not None
        assert keyed.service_key == "w2v-video-analyzer"
        assert keyed.cost_model == "per_token"

        # Rung 2 — the scope-less row, reachable only on its own terms.
        unkeyed = await pricing_svc._resolve_pricing_row(
            db_session,
            provider="video-analyzer",
            served_model="local",
            service_key=None,
            ts=self._TS,
        )
        assert unkeyed is not None
        assert unkeyed.service_key is None
        assert unkeyed.cost_model == "per_second"

        # The collapse assertion proper: two scopes, two DIFFERENT rows. Asserted
        # on ids as well as on cost_model so a resolver that returned the same
        # row twice cannot pass by coincidence of field values.
        assert keyed.id != unkeyed.id


@pytest.mark.unit
class TestPricingMiss:
    async def test_unknown_provider_returns_none(self, db_session, caplog, capsys) -> None:
        import logging

        ts = datetime(2026, 4, 1, tzinfo=UTC)
        with caplog.at_level(logging.WARNING):
            cost, pid = await pricing_svc.compute_cost_usd(
                db_session,
                provider="totally-unknown",
                served_model="x",
                input_tokens=10,
                output_tokens=5,
                ts=ts,
            )
        assert cost is None
        assert pid is None
        # structlog renders to stdout via the stdlib handler in some test
        # configurations; in other test orderings stdlib processors render
        # straight into caplog records. Check both.
        captured = capsys.readouterr()
        record_text = " ".join(r.getMessage() for r in caplog.records)
        haystack = captured.out + captured.err + record_text
        assert "pricing_miss" in haystack


@pytest.mark.unit
class TestCostModels:
    async def test_per_request_ignores_token_counts(self, db_session) -> None:
        ts = datetime(2026, 4, 1, tzinfo=UTC)
        db_session.add(
            _seed(
                provider="some-app",
                model=None,
                cost_model="per_request",
                per_request_usd=Decimal("0.01234567"),
                effective_from=datetime(2026, 1, 1, tzinfo=UTC),
            )
        )
        await db_session.commit()

        cost, _pid = await pricing_svc.compute_cost_usd(
            db_session,
            provider="some-app",
            served_model=None,
            input_tokens=99_999_999,
            output_tokens=12_345,
            ts=ts,
        )
        assert cost == Decimal("0.01234567")

    async def test_self_hosted_amortized_returns_zero(self, db_session) -> None:
        ts = datetime(2026, 4, 1, tzinfo=UTC)
        db_session.add(
            _seed(
                provider="vllm",
                model=None,
                cost_model="self_hosted_amortized",
                monthly_fixed_cost_usd=Decimal("250"),
                effective_from=datetime(2026, 1, 1, tzinfo=UTC),
            )
        )
        await db_session.commit()

        cost, pid = await pricing_svc.compute_cost_usd(
            db_session,
            provider="vllm",
            served_model="Qwen3-4B-AWQ",
            input_tokens=1_000_000,
            output_tokens=500_000,
            ts=ts,
        )
        # Per-event cost is zero. Amortization happens at billing-report time.
        assert cost == Decimal("0")
        assert pid is not None


@pytest.mark.unit
class TestUpsertPricing:
    async def test_upsert_closes_prior_open_row(self, db_session) -> None:
        """upsert_pricing must close any open row for the same (provider, model)
        and open a new one with continuous boundaries (no gap, no overlap)."""
        first_from = datetime(2026, 1, 1, tzinfo=UTC)
        new_from = datetime(2026, 4, 1, tzinfo=UTC)

        db_session.add(
            _seed(
                provider="anthropic",
                model="claude-3-5-sonnet-20241022",
                input_per_1m_usd=Decimal("3"),
                output_per_1m_usd=Decimal("15"),
                effective_from=first_from,
            )
        )
        await db_session.commit()

        payload = ProviderPricingCreate(
            provider="anthropic",
            model="claude-3-5-sonnet-20241022",
            cost_model="per_token",
            input_per_1m_usd=Decimal("4"),
            output_per_1m_usd=Decimal("18"),
            effective_from=new_from,
        )
        new_row, prior = await pricing_svc.upsert_pricing(db_session, payload, ts=new_from)
        await db_session.commit()
        await db_session.refresh(new_row)
        assert prior is not None
        await db_session.refresh(prior)

        # Continuity boundary: prior.effective_until == new.effective_from.
        assert prior.effective_until == new_from
        assert new_row.effective_from == new_from
        assert new_row.effective_until is None

        # The two rows now sandwich the timeline cleanly.
        all_rows = (
            (
                await db_session.execute(
                    select(ProviderPricing)
                    .where(ProviderPricing.provider == "anthropic")
                    .order_by(ProviderPricing.effective_from)
                )
            )
            .scalars()
            .all()
        )
        assert len(all_rows) == 2

    async def test_upsert_when_no_prior_row(self, db_session) -> None:
        """Inserting the first ever pricing row returns prior=None."""
        new_from = datetime(2026, 4, 1, tzinfo=UTC)
        payload = ProviderPricingCreate(
            provider="newprov",
            model=None,
            cost_model="per_token",
            input_per_1m_usd=Decimal("1"),
            output_per_1m_usd=Decimal("4"),
            effective_from=new_from,
        )
        new_row, prior = await pricing_svc.upsert_pricing(db_session, payload, ts=new_from)
        await db_session.commit()
        assert prior is None
        assert new_row.id is not None


@pytest.mark.unit
class TestListCurrentPricing:
    async def test_returns_only_open_rows_sorted(self, db_session) -> None:
        ts = datetime(2026, 4, 1, tzinfo=UTC)
        # one closed row, one open default, one open per-model
        db_session.add(
            _seed(
                provider="anthropic",
                model=None,
                input_per_1m_usd=Decimal("2"),
                output_per_1m_usd=Decimal("10"),
                effective_from=datetime(2026, 1, 1, tzinfo=UTC),
                effective_until=ts,  # closed
            )
        )
        db_session.add(
            _seed(
                provider="anthropic",
                model=None,
                input_per_1m_usd=Decimal("3"),
                output_per_1m_usd=Decimal("15"),
                effective_from=ts,
            )
        )
        db_session.add(
            _seed(
                provider="anthropic",
                model="claude-3-5-sonnet-20241022",
                input_per_1m_usd=Decimal("3"),
                output_per_1m_usd=Decimal("15"),
                effective_from=ts,
            )
        )
        await db_session.commit()

        rows = await pricing_svc.list_current_pricing(db_session)
        # Only the two open rows.
        assert len(rows) == 2
        # Provider-default (model is None) sorts first.
        assert rows[0].model is None
        assert rows[1].model == "claude-3-5-sonnet-20241022"


@pytest.mark.unit
class TestMalformedRowObservability:
    """A matched-but-rateless row used to price real usage at $0.00 and look like
    a successful charge — invisible in the data AND invisible to reprice, whose
    backfill selects ``cost_usd IS NULL``. It now REFUSES (returns None) so the
    spend stays NULL and is repairable. Legacy rows already in the table cannot
    be caught by the ProviderPricingCreate validator, so this is the guard."""

    def test_per_token_row_with_no_rates_refuses_and_increments_metric(self) -> None:
        row = _seed(
            provider="badprov",
            model="badmodel",
            cost_model="per_token",
            input_per_1m_usd=None,
            output_per_1m_usd=None,
            effective_from=datetime(2026, 4, 1, tzinfo=UTC),
        )
        before = _malformed_count(provider="badprov", model="badmodel", cost_model="per_token")
        cost = pricing_svc._cost_from_row(row, input_tokens=1000, output_tokens=500)
        # ⛔ NOT Decimal("0") — a fabricated zero is unbillable forever because
        # reprice only ever revisits rows whose cost_usd IS NULL.
        assert cost is None
        after = _malformed_count(provider="badprov", model="badmodel", cost_model="per_token")
        assert after == before + 1

    def test_per_request_row_with_no_price_refuses(self) -> None:
        """The per_request mirror: an unstated amount must not become $0."""
        row = _seed(
            provider="reqprov",
            model="reqmodel",
            cost_model="per_request",
            per_request_usd=None,
            effective_from=datetime(2026, 4, 1, tzinfo=UTC),
        )
        before = _malformed_count(provider="reqprov", model="reqmodel", cost_model="per_request")
        assert pricing_svc._cost_from_row(row, input_tokens=10, output_tokens=5) is None
        after = _malformed_count(provider="reqprov", model="reqmodel", cost_model="per_request")
        assert after == before + 1

    def test_explicit_zero_per_request_is_a_free_tier_not_a_refusal(self) -> None:
        """⚠️ REGRESSION GUARD. A STATED 0 is a deliberate free tier — the
        validator accepts such rows on purpose (test_zero_rate_row_is_accepted in
        tests/test_provider_pricing_routes.py). Refusing on VALUE instead of on
        NULL-ness would turn every correct free-tier charge into a fake
        "unbilled" NULL row, which is the opposite of the bug being fixed."""
        row = _seed(
            provider="freereq",
            model="m",
            cost_model="per_request",
            per_request_usd=Decimal("0"),
            effective_from=datetime(2026, 4, 1, tzinfo=UTC),
        )
        before = _malformed_count(provider="freereq", model="m", cost_model="per_request")
        assert pricing_svc._cost_from_row(row, input_tokens=10, output_tokens=5) == Decimal("0")
        assert _malformed_count(provider="freereq", model="m", cost_model="per_request") == before

    def test_explicit_zero_token_rates_are_a_free_tier_not_a_refusal(self) -> None:
        """⚠️ REGRESSION GUARD, token-family half. Stated 0/0 rates with real
        tokens price at $0 and must NOT refuse — same reason as above."""
        row = _seed(
            provider="freetok",
            model="m",
            cost_model="per_token",
            input_per_1m_usd=Decimal("0"),
            output_per_1m_usd=Decimal("0"),
            effective_from=datetime(2026, 4, 1, tzinfo=UTC),
        )
        cost = pricing_svc._cost_from_row(row, input_tokens=1_000_000, output_tokens=1_000_000)
        assert cost == Decimal("0.00000000")

    def test_zero_quantity_against_an_unstated_rate_is_zero_not_a_refusal(self) -> None:
        """⚠️ REGRESSION GUARD. A genuinely zero-quantity event costs $0 whatever
        the rate is, so an unstated rate must not be consulted: refusing here
        would mint a NULL "unbilled" row for a call that cost nothing."""
        row = _seed(
            provider="zeroqty",
            model="m",
            cost_model="per_token",
            input_per_1m_usd=None,
            output_per_1m_usd=None,
            effective_from=datetime(2026, 4, 1, tzinfo=UTC),
        )
        before = _malformed_count(provider="zeroqty", model="m", cost_model="per_token")
        assert pricing_svc._cost_from_row(row, input_tokens=0, output_tokens=0) == Decimal(
            "0.00000000"
        )
        assert _malformed_count(provider="zeroqty", model="m", cost_model="per_token") == before

    def test_one_stated_rate_is_enough_to_price(self) -> None:
        """Only BOTH primary rates unstated refuses — output-only rows are a
        legitimate shape the validator accepts."""
        row = _seed(
            provider="outonly",
            model="m",
            cost_model="per_token",
            input_per_1m_usd=None,
            output_per_1m_usd=Decimal("15"),
            effective_from=datetime(2026, 4, 1, tzinfo=UTC),
        )
        cost = pricing_svc._cost_from_row(row, input_tokens=1_000_000, output_tokens=1_000_000)
        assert cost == Decimal("15.00000000")

    def test_provider_default_row_labels_model_default(self) -> None:
        """A NULL-model (provider-default) row must still be attributable."""
        row = _seed(
            provider="baddefault",
            model=None,
            cost_model="per_token",
            effective_from=datetime(2026, 4, 1, tzinfo=UTC),
        )
        before = _malformed_count(provider="baddefault", model="default", cost_model="per_token")
        pricing_svc._cost_from_row(row, input_tokens=10, output_tokens=0)
        after = _malformed_count(provider="baddefault", model="default", cost_model="per_token")
        assert after == before + 1

    def test_zero_token_event_does_not_increment(self) -> None:
        """A genuinely zero-token event priced at $0 is correct, not malformed."""
        row = _seed(
            provider="zerotok",
            model="m",
            cost_model="per_token",
            effective_from=datetime(2026, 4, 1, tzinfo=UTC),
        )
        before = _malformed_count(provider="zerotok", model="m", cost_model="per_token")
        cost = pricing_svc._cost_from_row(row, input_tokens=0, output_tokens=0)
        assert cost == Decimal("0.00000000")
        assert _malformed_count(provider="zerotok", model="m", cost_model="per_token") == before

    def test_self_hosted_amortized_zero_does_not_increment(self) -> None:
        """$0 per event is by design here — amortized monthly at report time."""
        row = _seed(
            provider="selfhosted",
            model="llama-3-70b",
            cost_model="self_hosted_amortized",
            monthly_fixed_cost_usd=Decimal("1200"),
            effective_from=datetime(2026, 4, 1, tzinfo=UTC),
        )
        before = _malformed_count(
            provider="selfhosted", model="llama-3-70b", cost_model="self_hosted_amortized"
        )
        cost = pricing_svc._cost_from_row(row, input_tokens=100000, output_tokens=50000)
        assert cost == Decimal("0")
        assert (
            _malformed_count(
                provider="selfhosted", model="llama-3-70b", cost_model="self_hosted_amortized"
            )
            == before
        )

    def test_partially_free_row_does_not_increment(self) -> None:
        """One free axis (cache_read at $0) is legitimate — only an ENTIRELY
        zero rate set is malformed."""
        row = _seed(
            provider="partial",
            model="m",
            cost_model="per_token",
            input_per_1m_usd=Decimal("3"),
            output_per_1m_usd=Decimal("15"),
            cache_read_per_1m_usd=Decimal("0"),
            effective_from=datetime(2026, 4, 1, tzinfo=UTC),
        )
        before = _malformed_count(provider="partial", model="m", cost_model="per_token")
        cost = pricing_svc._cost_from_row(
            row, input_tokens=1_000_000, output_tokens=0, cache_read_input_tokens=1_000_000
        )
        assert cost == Decimal("3.00000000")
        assert _malformed_count(provider="partial", model="m", cost_model="per_token") == before


@pytest.mark.unit
class TestProviderPricingCreateValidation:
    """Door validation — stops NEW malformed rows from ever being written."""

    def test_per_token_requires_a_rate(self) -> None:
        with pytest.raises(ValidationError) as exc:
            ProviderPricingCreate(provider="p", cost_model="per_token")
        msg = str(exc.value)
        assert "input_per_1m_usd" in msg
        assert "per_token" in msg

    def test_per_token_output_only_is_valid(self) -> None:
        payload = ProviderPricingCreate(
            provider="p", cost_model="per_token", output_per_1m_usd=Decimal("15")
        )
        assert payload.output_per_1m_usd == Decimal("15")

    def test_per_request_requires_price(self) -> None:
        with pytest.raises(ValidationError) as exc:
            ProviderPricingCreate(provider="p", cost_model="per_request")
        assert "per_request_usd" in str(exc.value)

    def test_self_hosted_requires_monthly_fixed_cost(self) -> None:
        with pytest.raises(ValidationError) as exc:
            ProviderPricingCreate(provider="p", cost_model="self_hosted_amortized")
        assert "monthly_fixed_cost_usd" in str(exc.value)

    def test_per_page_requires_a_rate(self) -> None:
        with pytest.raises(ValidationError) as exc:
            ProviderPricingCreate(provider="p", cost_model="per_page")
        assert "per_page" in str(exc.value)

    def test_per_page_is_accepted(self) -> None:
        payload = ProviderPricingCreate(
            provider="p", cost_model="per_page", input_per_1m_usd=Decimal("1500")
        )
        assert payload.cost_model == "per_page"

    def test_unknown_cost_model_rejected(self) -> None:
        with pytest.raises(ValidationError):
            ProviderPricingCreate(
                provider="p", cost_model=cast(Any, "per_tokens"), input_per_1m_usd=Decimal("3")
            )


# ---------------------------------------------------------------------------
# cost_basis: a PROVIDER CONTRACT fact (alembic 0022), not derived from
# cost_model. See services.pricing's COST_BASIS_BY_COST_MODEL comment for the
# defect this replaced (self-hosted per_token/per_character providers derived
# 'cash' because only self_hosted_amortized mapped to 'internal_transfer').
# ---------------------------------------------------------------------------


@pytest.mark.unit
class TestComputeCostUsdWithCostModelReturnsCostBasis:
    async def test_a_self_hosted_per_token_row_returns_internal_transfer(self, db_session) -> None:
        """THE REGRESSION THAT MATTERS: a self-hosted per_token provider must
        NOT derive 'cash' just because its cost_model isn't
        self_hosted_amortized."""
        ts = datetime(2026, 4, 1, tzinfo=UTC)
        db_session.add(
            _seed(
                provider="pgx-local",
                model=None,
                cost_model="per_token",
                input_per_1m_usd=Decimal("10"),
                output_per_1m_usd=Decimal("25"),
                effective_from=datetime(2026, 1, 1, tzinfo=UTC),
            )
        )
        await db_session.commit()

        _cost, _pid, cost_model, cost_basis = await pricing_svc.compute_cost_usd_with_cost_model(
            db_session,
            provider="pgx-local",
            served_model=None,
            input_tokens=100,
            output_tokens=50,
            ts=ts,
        )
        assert cost_model == "per_token"
        assert cost_basis == "internal_transfer"

    async def test_an_explicitly_cash_row_returns_cash(self, db_session) -> None:
        ts = datetime(2026, 4, 1, tzinfo=UTC)
        db_session.add(
            _seed(
                provider="openai",
                model=None,
                cost_model="per_token",
                cost_basis="cash",
                input_per_1m_usd=Decimal("5"),
                output_per_1m_usd=Decimal("15"),
                effective_from=datetime(2026, 1, 1, tzinfo=UTC),
            )
        )
        await db_session.commit()

        _cost, _pid, _cost_model, cost_basis = await pricing_svc.compute_cost_usd_with_cost_model(
            db_session,
            provider="openai",
            served_model=None,
            input_tokens=100,
            output_tokens=50,
            ts=ts,
        )
        assert cost_basis == "cash"

    async def test_a_pricing_miss_returns_none_for_all_four(self, db_session) -> None:
        ts = datetime(2026, 4, 1, tzinfo=UTC)
        result = await pricing_svc.compute_cost_usd_with_cost_model(
            db_session,
            provider="never-seeded-provider",
            served_model=None,
            input_tokens=1,
            output_tokens=1,
            ts=ts,
        )
        assert result == (None, None, None, None)

    async def test_mutation_check_cost_model_based_inference_would_be_wrong_here(
        self, db_session
    ) -> None:
        """Guards against silently reverting to the retired derivation.

        This directly exercises the retired ``COST_BASIS_BY_COST_MODEL`` /
        ``cost_basis_for_cost_model`` shape (still present, unused, as a
        historical marker) against the exact row that broke production:
        self-hosted, cost_model='per_character'. If a future edit reintroduces
        ``cost_basis_for_cost_model(resolved_cost_model)`` on the write path,
        it derives 'cash' here — the opposite of the correct
        'internal_transfer' the resolved pricing row's own column declares.
        """
        ts = datetime(2026, 4, 1, tzinfo=UTC)
        db_session.add(
            _seed(
                provider="mlx-nlp",
                model=None,
                cost_model="per_character",
                input_per_1m_usd=Decimal("1"),
                effective_from=datetime(2026, 1, 1, tzinfo=UTC),
            )
        )
        await db_session.commit()

        (
            _cost,
            _pid,
            resolved_cost_model,
            resolved_cost_basis,
        ) = await pricing_svc.compute_cost_usd_with_cost_model(
            db_session,
            provider="mlx-nlp",
            served_model=None,
            input_tokens=1000,
            output_tokens=0,
            ts=ts,
        )
        # Correct (current) behaviour:
        assert resolved_cost_basis == "internal_transfer"
        # The retired, defective derivation the old write path used — proof
        # that reinstating it would redden this exact assertion:
        old_defective_derivation = pricing_svc.cost_basis_for_cost_model(resolved_cost_model)
        assert old_defective_derivation == "cash"
        assert old_defective_derivation != resolved_cost_basis


@pytest.mark.unit
class TestQuantityUnitIsEnforcedAgainstCostModel:
    """⛔ THE UNIT LABEL USED TO BE DECORATIVE.

    ``per_token`` / ``per_second`` / ``per_character`` share IDENTICAL arithmetic
    (``_cost_from_row`` multiplies ``input_tokens``/``output_tokens``/both cache
    counters by per-1M rates and, before this branch, never referenced
    ``quantity_unit`` at all). So a quantity emitted in SECONDS and priced by a
    ``per_token`` rate produced a number that is arithmetically
    indistinguishable from a correct charge — no NULL, no exception, no
    out-of-range value, nothing to find afterwards.

    Measured live: ``provider_pricing`` row 32 (video-analyzer /
    w2v-video-analyzer, ``per_token``, OPEN) shadows rows 117/118
    (``per_second``, OPEN) for the only ``service_key`` that emits, and the
    mispricing is invisible ONLY because every rate involved happens to be
    ``283.33333333``.

    LEGITIMATE ``(quantity_unit, cost_model)`` PAIRS — derived from
    ``QUANTITY_UNIT_BY_COST_MODEL`` (the total function over the five live
    ``cost_model`` values) restricted to the token family:

      * ``('tokens', 'per_token')``
      * ``('seconds', 'per_second')``
      * ``('characters', 'per_character')``
      * ``(None, <anything>)``  — absence asserts nothing
      * ``(<anything>, 'per_request')`` and
        ``(<anything>, 'self_hosted_amortized')`` — neither consults the
        quantity, so a unit disagreement cannot mis-charge. This is what keeps
        mflux (``megapixels``) and mlx-rerank (``passages``) priceable.
      * zero quantity, any pair — ``$0`` in every unit
    """

    # --- KILLING TESTS: these return a Decimal on unmodified code -----------

    def test_seconds_quantity_against_a_per_token_rate_refuses(self) -> None:
        """⭐ KILLING TEST. Unmodified code returns Decimal('1.70000000')."""
        row = _seed(
            provider="unitprov",
            model="unitmodel",
            cost_model="per_token",
            input_per_1m_usd=Decimal("283.33333333"),
            output_per_1m_usd=Decimal("283.33333333"),
            effective_from=datetime(2026, 4, 1, tzinfo=UTC),
        )
        before = _malformed_count(provider="unitprov", model="unitmodel", cost_model="per_token")
        cost = pricing_svc._cost_from_row(
            row, input_tokens=6000, output_tokens=0, quantity_unit="seconds"
        )
        assert cost is None, (
            "6000 AUDIO-SECONDS were priced by a per_token rate and the result came "
            "back as a number; a unit mismatch has no other signature, so a silently "
            "computed value here is permanently indistinguishable from a real charge"
        )
        after = _malformed_count(provider="unitprov", model="unitmodel", cost_model="per_token")
        assert after == before + 1

    def test_per_page_prices_pages_and_refuses_tokens(self) -> None:
        """per_page (strata-ingest OCR, owner $0.0015/page = $1,500 / 1M pages)
        prices a page count with or without an asserted unit, and refuses a
        quantity the emitter says is tokens."""
        row = _seed(
            provider="mlx-api",
            model="ocr-model",
            cost_model="per_page",
            input_per_1m_usd=Decimal("1500"),
            output_per_1m_usd=Decimal("0"),
            effective_from=datetime(2026, 4, 1, tzinfo=UTC),
        )
        assert pricing_svc._cost_from_row(
            row, input_tokens=8, output_tokens=0, quantity_unit=None
        ) == Decimal("0.01200000")
        assert pricing_svc._cost_from_row(
            row, input_tokens=1, output_tokens=0, quantity_unit="pages"
        ) == Decimal("0.00150000")
        assert (
            pricing_svc._cost_from_row(row, input_tokens=8, output_tokens=0, quantity_unit="tokens")
            is None
        )

    def test_per_page_derives_the_pages_unit(self) -> None:
        assert pricing_svc.quantity_unit_for_provider("mlx-api", "per_page") == "pages"

    def test_the_live_video_analyzer_shadow_row_shape_refuses(self) -> None:
        """⭐ KILLING TEST — the exact live shape.

        Row 32 (``per_token``) shadows rows 117/118 (``per_second``) for
        w2v-video-analyzer. Every rate is 283.33333333, so the WRONG row and the
        RIGHT row return the same number today: the refusal is the only thing
        that can tell them apart, and it has to fire on the unit, not the value.
        """
        shadow = _seed(
            provider="video-analyzer",
            model=None,
            service_key="w2v-video-analyzer",
            cost_model="per_token",
            input_per_1m_usd=Decimal("283.33333333"),
            effective_from=datetime(2026, 4, 1, tzinfo=UTC),
        )
        correct = _seed(
            provider="video-analyzer",
            model=None,
            service_key="w2v-video-analyzer",
            cost_model="per_second",
            input_per_1m_usd=Decimal("283.33333333"),
            effective_from=datetime(2026, 4, 1, tzinfo=UTC),
        )
        # Proof the two are indistinguishable by VALUE — the whole reason a
        # value-based check could never have caught this.
        assert pricing_svc._cost_from_row(
            correct, input_tokens=120, output_tokens=0, quantity_unit="seconds"
        ) == Decimal("0.03400000")
        assert (
            pricing_svc._cost_from_row(
                shadow, input_tokens=120, output_tokens=0, quantity_unit="seconds"
            )
            is None
        )

    def test_a_providers_measured_unit_refuses_a_wrong_family_shadow_row(self) -> None:
        """⭐ KILLING TEST, and the one that covers the FLUSH hot path.

        ``cost_flusher`` does not pass ``quantity_unit`` yet, so the enforcement
        has to work from what ``_cost_from_row`` can already see: the resolved
        row's own ``provider``. ``QUANTITY_UNIT_BY_PROVIDER`` is the operator's
        measured mapping (and alembic 0021's backfill twin), so a video-compiler
        row in the wrong family is refusable with no caller change at all.
        """
        row = _seed(
            provider="video-compiler",
            model="ffmpeg",
            cost_model="per_token",
            input_per_1m_usd=Decimal("5"),
            effective_from=datetime(2026, 4, 1, tzinfo=UTC),
        )
        assert pricing_svc.QUANTITY_UNIT_BY_PROVIDER["video-compiler"] == "seconds"
        assert pricing_svc._cost_from_row(row, input_tokens=600, output_tokens=0) is None

    # --- NEGATIVE CONTROLS: nothing currently priced correctly may refuse ---

    @pytest.mark.parametrize(
        ("quantity_unit", "cost_model"),
        [("tokens", "per_token"), ("seconds", "per_second"), ("characters", "per_character")],
    )
    def test_every_legitimate_token_family_pair_still_prices(
        self, quantity_unit: str, cost_model: str
    ) -> None:
        """⚠️ OVER-STRICTNESS GUARD. The three legitimate pairs, exhaustively."""
        row = _seed(
            provider="okprov",
            model="m",
            cost_model=cost_model,
            input_per_1m_usd=Decimal("2"),
            output_per_1m_usd=Decimal("4"),
            effective_from=datetime(2026, 4, 1, tzinfo=UTC),
        )
        cost = pricing_svc._cost_from_row(
            row, input_tokens=1_000_000, output_tokens=1_000_000, quantity_unit=quantity_unit
        )
        assert cost == Decimal("6.00000000")

    def test_an_absent_quantity_unit_still_prices(self) -> None:
        """⚠️ OVER-STRICTNESS GUARD. ``quantity_unit`` is nullable by design
        (alembic 0021) and every row predating it is NULL. Absence asserts
        nothing and must never refuse."""
        row = _seed(
            provider="noprov",
            model="m",
            cost_model="per_token",
            input_per_1m_usd=Decimal("3"),
            effective_from=datetime(2026, 4, 1, tzinfo=UTC),
        )
        assert pricing_svc._cost_from_row(
            row, input_tokens=1_000_000, output_tokens=0, quantity_unit=None
        ) == Decimal("3.00000000")
        # An empty string is the same claim as NULL, not a contradiction.
        assert pricing_svc._cost_from_row(
            row, input_tokens=1_000_000, output_tokens=0, quantity_unit=""
        ) == Decimal("3.00000000")

    @pytest.mark.parametrize("quantity_unit", ["megapixels", "passages", "requests", "tokens"])
    def test_a_unit_disagreement_with_per_request_is_not_a_refusal(
        self, quantity_unit: str
    ) -> None:
        """⚠️ OVER-STRICTNESS GUARD, and the one that protects mflux /
        mlx-rerank. ``per_request`` is a FLAT fee: it never multiplies the
        quantity by a rate, so no unit can mis-charge it. ``megapixels`` and
        ``passages`` are in ``CostEvent.quantity_unit``'s bounded set and
        reachable from NO cost_model, so this is the only pairing they ever
        have."""
        row = _seed(
            provider="mflux",
            model="schnell",
            cost_model="per_request",
            per_request_usd=Decimal("0.05"),
            effective_from=datetime(2026, 4, 1, tzinfo=UTC),
        )
        assert pricing_svc._cost_from_row(
            row, input_tokens=900, output_tokens=0, quantity_unit=quantity_unit
        ) == Decimal("0.05000000")

    def test_a_unit_disagreement_with_self_hosted_amortized_is_not_a_refusal(self) -> None:
        """⚠️ OVER-STRICTNESS GUARD. $0 per event is correct by design here
        (amortized monthly at report time) — the quantity is never priced, so
        its unit cannot contradict anything."""
        row = _seed(
            provider="selfhostunit",
            model="llama-3-70b",
            cost_model="self_hosted_amortized",
            monthly_fixed_cost_usd=Decimal("1200"),
            effective_from=datetime(2026, 4, 1, tzinfo=UTC),
        )
        assert pricing_svc._cost_from_row(
            row, input_tokens=100000, output_tokens=50000, quantity_unit="passages"
        ) == Decimal("0")

    def test_zero_quantity_with_a_contradicting_unit_is_not_a_refusal(self) -> None:
        """⚠️ OVER-STRICTNESS GUARD, mirroring the rate refusals' own rule: a
        zero-quantity event costs $0 in every unit, so refusing would mint a
        fake unbilled row for a call that cost nothing."""
        row = _seed(
            provider="zerounit",
            model="m",
            cost_model="per_token",
            input_per_1m_usd=Decimal("3"),
            effective_from=datetime(2026, 4, 1, tzinfo=UTC),
        )
        before = _malformed_count(provider="zerounit", model="m", cost_model="per_token")
        assert pricing_svc._cost_from_row(
            row, input_tokens=0, output_tokens=0, quantity_unit="seconds"
        ) == Decimal("0.00000000")
        assert _malformed_count(provider="zerounit", model="m", cost_model="per_token") == before

    def test_an_unrecognised_cost_model_is_not_refused_for_its_unit(self) -> None:
        """⚠️ OVER-STRICTNESS GUARD. ``provider_pricing.cost_model`` is a plain
        VARCHAR for forward-compat, and the token-family fall-through is
        DELIBERATE: it is what lets a pod predating a new unit compute the same
        number during a rollout. We cannot say a unit disagrees with rates whose
        meaning we do not know, so an unknown cost_model keeps its existing
        ``pricing_unknown_cost_model`` warning and is NOT refused here."""
        row = _seed(
            provider="futureprov",
            model="m",
            cost_model="per_gigabyte",
            input_per_1m_usd=Decimal("3"),
            effective_from=datetime(2026, 4, 1, tzinfo=UTC),
        )
        assert pricing_svc._cost_from_row(
            row, input_tokens=1_000_000, output_tokens=0, quantity_unit="seconds"
        ) == Decimal("3.00000000")

    @pytest.mark.parametrize(
        ("provider", "cost_model"),
        [
            # Every provider in QUANTITY_UNIT_BY_PROVIDER, paired with the
            # cost_model scripts/initial_pricing.yaml actually seeds for it.
            # ⛔ If this parametrisation ever reds, the provider-measured
            # fallback has started refusing REAL seeded traffic.
            ("pgx-local", "per_token"),
            ("mlx-nlp", "per_character"),
            ("mlx-embed", "per_character"),
            ("kokoro", "per_character"),
            ("video-compiler", "per_second"),
        ],
    )
    def test_each_measured_unit_provider_prices_against_its_seeded_cost_model(
        self, provider: str, cost_model: str
    ) -> None:
        """⚠️ OVER-STRICTNESS GUARD against the REAL seed, not invented rows."""
        row = _seed(
            provider=provider,
            model=None,
            cost_model=cost_model,
            input_per_1m_usd=Decimal("1"),
            effective_from=datetime(2026, 4, 1, tzinfo=UTC),
        )
        assert pricing_svc._cost_from_row(row, input_tokens=1_000_000, output_tokens=0) == Decimal(
            "1.00000000"
        )

    @pytest.mark.parametrize(
        ("provider", "cost_model"),
        [("mflux", "per_request"), ("mlx-rerank", "per_request")],
    )
    def test_the_two_unreachable_unit_providers_still_price(
        self, provider: str, cost_model: str
    ) -> None:
        """⚠️ OVER-STRICTNESS GUARD. mflux (megapixels) and mlx-rerank
        (passages) have units no cost_model can produce; both are seeded
        ``per_request``, which is exempt. If the exemption is ever narrowed to
        "the unit must match", these two stop billing entirely."""
        row = _seed(
            provider=provider,
            model="m",
            cost_model=cost_model,
            per_request_usd=Decimal("0.01"),
            effective_from=datetime(2026, 4, 1, tzinfo=UTC),
        )
        assert pricing_svc._cost_from_row(row, input_tokens=5, output_tokens=0) == Decimal(
            "0.01000000"
        )


@pytest.mark.unit
class TestNoResolvedRateIsNullNeverZero:
    """⛔ ABSENT IS NOT ZERO. ``services.reprice`` backfills on ``cost_usd IS
    NULL``, so a fabricated ``$0`` is unbillable FOREVER — it can never be found
    or repaired.

    Measured live: ``provider='openai_compatible'`` / ``service_key=
    'commonpractice'`` has NO ``provider_pricing`` row at all (45 rows exist for
    ``openai``, none for it) and ``billing_cost_daily`` carries
    ``total_cost_usd = 0.000000`` for its 2026-08-08 bucket against 6,087
    tokens.

    ⚠️ These are REGRESSION LOCKS, not killing tests, and that distinction is the
    finding: the pricing path ALREADY returns NULL for a total miss. The
    ``0.000000`` is manufactured downstream, by ``rollup_engine``'s
    ``coalesce(sum(CostEvent.cost_usd), 0)`` folding those NULLs into
    ``billing_cost_daily.total_cost_usd``, a ``nullable=False`` column. Neither
    is in this module."""

    async def test_a_provider_with_no_pricing_row_anywhere_is_none_not_zero(
        self, db_session
    ) -> None:
        """A sibling provider's rows must not be borrowed, and the miss must not
        collapse to $0."""
        ts = datetime(2026, 8, 8, 12, 0, tzinfo=UTC)
        # The decoys: 'openai' IS priced, 'openai_compatible' is a different
        # provider string and is not.
        db_session.add(
            _seed(
                provider="openai",
                model="gpt-4o",
                cost_model="per_token",
                input_per_1m_usd=Decimal("2.50"),
                output_per_1m_usd=Decimal("10"),
                effective_from=datetime(2026, 1, 1, tzinfo=UTC),
            )
        )
        await db_session.commit()

        cost, pricing_id = await pricing_svc.compute_cost_usd(
            db_session,
            provider="openai_compatible",
            served_model="gpt-4o",
            service_key="commonpractice",  # pragma: allowlist secret
            input_tokens=6087,
            output_tokens=0,
            ts=ts,
        )
        assert cost is None, "a provider with no pricing row priced to a value"
        assert cost != Decimal("0")
        assert pricing_id is None

    async def test_all_four_outputs_are_none_on_a_total_miss(self, db_session) -> None:
        """``cost_basis``/``cost_model`` must not be invented either — the
        caller's own ``PRICING_MISS_COST_BASIS`` backstop depends on seeing
        ``None`` here."""
        ts = datetime(2026, 8, 8, 12, 0, tzinfo=UTC)
        result = await pricing_svc.compute_cost_usd_with_cost_model(
            db_session,
            provider="openai_compatible",
            served_model=None,
            service_key="commonpractice",  # pragma: allowlist secret
            input_tokens=6087,
            output_tokens=0,
            ts=ts,
        )
        assert result == (None, None, None, None)

    async def test_an_explicit_zero_rate_row_prices_zero_not_none(self, db_session) -> None:
        """⚠️ NEGATIVE CONTROL — the half that must NOT change. A rate row that
        genuinely STATES 0 is a deliberate free tier and keeps pricing at $0.
        Absent vs zero is the whole point; collapsing them in either direction
        is the bug."""
        ts = datetime(2026, 8, 8, 12, 0, tzinfo=UTC)
        db_session.add(
            _seed(
                provider="freetier_e2e",
                model="free-model",
                cost_model="per_token",
                input_per_1m_usd=Decimal("0"),
                output_per_1m_usd=Decimal("0"),
                effective_from=datetime(2026, 1, 1, tzinfo=UTC),
            )
        )
        await db_session.commit()

        cost, pricing_id = await pricing_svc.compute_cost_usd(
            db_session,
            provider="freetier_e2e",
            served_model="free-model",
            input_tokens=6087,
            output_tokens=1000,
            ts=ts,
        )
        assert cost == Decimal("0"), "a STATED zero rate must still price at $0, not NULL"
        assert cost is not None
        assert pricing_id is not None
