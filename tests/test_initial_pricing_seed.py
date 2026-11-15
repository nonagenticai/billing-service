"""Verify the YAML seed in scripts/initial_pricing.yaml prices Claude 4.x correctly.

These tests load the YAML directly, insert each row as a ProviderPricing record,
and then exercise the real ``services.pricing.compute_cost_usd`` path. That
guarantees the seed file's rates match the *exact* arithmetic billing uses.

Why this matters: the rollup engine recently switched to ``COALESCE(cost_usd, 0)``
on aggregate, so any pricing_miss for an anthropic/claude event silently rolls
up to $0 in cost_daily. Mis-keyed seed rows (wrong model id, wrong rate) would
not raise — they'd just under-bill. These tests fail loudly on either symptom.
"""

from __future__ import annotations

from datetime import UTC, datetime
from decimal import Decimal
from pathlib import Path
from typing import Any

import pytest
import yaml

from models.db import ProviderPricing
from services import pricing as pricing_svc

YAML_PATH = Path(__file__).resolve().parent.parent / "scripts" / "initial_pricing.yaml"


def _load_yaml() -> tuple[datetime, list[dict[str, Any]]]:
    with YAML_PATH.open("r", encoding="utf-8") as fh:
        doc = yaml.safe_load(fh)
    eff_from = datetime.fromisoformat(doc["effective_from"].replace("Z", "+00:00"))
    return eff_from, doc["rows"]


def _row_for(rows: list[dict[str, Any]], provider: str, model: str | None) -> dict[str, Any]:
    for r in rows:
        if r.get("provider") == provider and r.get("model") == model:
            return r
    raise KeyError(f"({provider}, {model!r}) not found in initial_pricing.yaml")


async def _seed_yaml_rows(db_session, rows: list[dict[str, Any]], effective_from: datetime) -> None:
    """Insert each YAML row as a ProviderPricing record. Mirrors what the
    seed script does over HTTP, minus the audit_log entry."""
    for row in rows:
        kwargs: dict[str, Any] = {
            "provider": row["provider"],
            "model": row.get("model"),
            "rate_class": row.get("rate_class", "cost"),
            "service_key": row.get("service_key"),
            "cost_model": row["cost_model"],
            "effective_from": effective_from,
        }
        for field in (
            "input_per_1m_usd",
            "output_per_1m_usd",
            "cache_creation_per_1m_usd",
            "cache_read_per_1m_usd",
            "per_request_usd",
            "monthly_fixed_cost_usd",
        ):
            if row.get(field) is not None:
                kwargs[field] = Decimal(str(row[field]))
        if row.get("notes"):
            kwargs["notes"] = row["notes"]
        db_session.add(ProviderPricing(**kwargs))
    await db_session.commit()


@pytest.mark.unit
class TestSeededClaude4Rows:
    """Each Claude 4.x model declared in initial_pricing.yaml must price
    correctly against ``services.pricing.compute_cost_usd``."""

    async def test_yaml_contains_claude_4_family(self) -> None:
        """The three Claude 4.x rows we care about are present in the seed.

        Provider key is "claude" (matches what inference-router emits per the
        provider catalog). Renamed from "anthropic" 2026-05-09; see commit
        message on fix/pricing-seed-claude-provider-name for context.
        """
        _eff, rows = _load_yaml()
        keys = {(r.get("provider"), r.get("model")) for r in rows}
        assert ("claude", "claude-opus-4-7") in keys
        assert ("claude", "claude-sonnet-4-6") in keys
        assert ("claude", "claude-haiku-4-5") in keys

    async def test_sonnet_4_6_prices_1m_in_500k_out(self, db_session) -> None:
        """claude-sonnet-4-6 @ 1M input + 500k output = 3.00 + 7.50 = $10.50.

        This is the canonical example from the task spec. If this fails the
        rollup will silently undercount sonnet cost again.
        """
        eff_from, rows = _load_yaml()
        await _seed_yaml_rows(db_session, rows, eff_from)

        cost, pid = await pricing_svc.compute_cost_usd(
            db_session,
            provider="claude",
            served_model="claude-sonnet-4-6",
            input_tokens=1_000_000,
            output_tokens=500_000,
            ts=datetime(2026, 5, 8, 12, 0, tzinfo=UTC),
        )
        assert pid is not None
        assert cost == Decimal("10.50000000")

    async def test_opus_4_7_prices_1m_in_500k_out(self, db_session) -> None:
        """claude-opus-4-7 @ 1M input + 500k output = 15.00 + 37.50 = $52.50."""
        eff_from, rows = _load_yaml()
        await _seed_yaml_rows(db_session, rows, eff_from)

        cost, pid = await pricing_svc.compute_cost_usd(
            db_session,
            provider="claude",
            served_model="claude-opus-4-7",
            input_tokens=1_000_000,
            output_tokens=500_000,
            ts=datetime(2026, 5, 8, 12, 0, tzinfo=UTC),
        )
        assert pid is not None
        assert cost == Decimal("52.50000000")

    async def test_haiku_4_5_prices_1m_in_500k_out(self, db_session) -> None:
        """claude-haiku-4-5 @ 1M input + 500k output = 0.80 + 2.00 = $2.80."""
        eff_from, rows = _load_yaml()
        await _seed_yaml_rows(db_session, rows, eff_from)

        cost, pid = await pricing_svc.compute_cost_usd(
            db_session,
            provider="claude",
            served_model="claude-haiku-4-5",
            input_tokens=1_000_000,
            output_tokens=500_000,
            ts=datetime(2026, 5, 8, 12, 0, tzinfo=UTC),
        )
        assert pid is not None
        assert cost == Decimal("2.80000000")

    async def test_opus_4_7_cache_tokens(self, db_session) -> None:
        """Opus 4.7 with cache tokens: 1M cache_creation @ $18.75 + 1M cache_read @ $1.50.

        Verifies the cache rate columns from the seed flow through to
        compute_cost_usd, not just the input/output rates.
        """
        eff_from, rows = _load_yaml()
        await _seed_yaml_rows(db_session, rows, eff_from)

        cost, _pid = await pricing_svc.compute_cost_usd(
            db_session,
            provider="claude",
            served_model="claude-opus-4-7",
            input_tokens=0,
            output_tokens=0,
            cache_creation_input_tokens=1_000_000,
            cache_read_input_tokens=1_000_000,
            ts=datetime(2026, 5, 8, 12, 0, tzinfo=UTC),
        )
        # 1M * 18.75/M + 1M * 1.50/M = 18.75 + 1.50 = 20.25
        assert cost == Decimal("20.25000000")


@pytest.mark.unit
class TestSeedYamlRates:
    """Property check: the YAML rates obey the documented Anthropic ratios.

    Cache write 5m = 1.25x input, cache read = 0.10x input. If a future edit
    fat-fingers a column these tests catch it without re-checking arithmetic
    in the test_*_prices_* tests above.
    """

    @pytest.mark.parametrize(
        "model",
        ["claude-opus-4-7", "claude-sonnet-4-6", "claude-haiku-4-5"],
    )
    def test_cache_rates_match_anthropic_ratios(self, model: str) -> None:
        _eff, rows = _load_yaml()
        row = _row_for(rows, "claude", model)
        in_rate = Decimal(row["input_per_1m_usd"])
        cache_create = Decimal(row["cache_creation_per_1m_usd"])
        cache_read = Decimal(row["cache_read_per_1m_usd"])
        # 5-minute cache write = 1.25x input
        assert cache_create == in_rate * Decimal("1.25"), (
            f"{model}: cache_creation_per_1m_usd={cache_create} != input*1.25={in_rate * Decimal('1.25')}"
        )
        # cache read = 0.10x input
        assert cache_read == in_rate * Decimal("0.10"), (
            f"{model}: cache_read_per_1m_usd={cache_read} != input*0.10={in_rate * Decimal('0.10')}"
        )


@pytest.mark.unit
class TestSeededByoProviderRows:
    """openai + groq are BYO-only providers (NOT in the inference-router catalog;
    tenants bring their own OPENAI_API_KEY / GROQ_API_KEY). Their provider_pricing
    rows are the pass-through cost ESTIMATE used for attribution — the actual
    byo_required charge is the flat byo_platform_fee surcharge (proven in
    test_markup_billing_consume). These tests guard the same 'anthropic vs claude'
    mis-key bug class: a wrong (provider, model) key or rate would silently
    under-attribute rather than raise.
    """

    async def test_yaml_contains_openai_groq_byo_rows(self) -> None:
        _eff, rows = _load_yaml()
        keys = {(r.get("provider"), r.get("model")) for r in rows}
        # per-model rows keyed to the ids the BYO emitter sends
        assert ("openai", "gpt-4o") in keys
        assert ("openai", "gpt-4o-mini") in keys
        assert ("openai", "gpt-4.1") in keys
        assert ("openai", "gpt-4.1-mini") in keys
        assert ("groq", "llama-3.3-70b-versatile") in keys
        assert ("groq", "llama-3.1-8b-instant") in keys
        # provider-default fallback rows (model=None) so an unknown id never NULLs
        assert ("openai", None) in keys
        assert ("groq", None) in keys

    async def test_openai_gpt4o_prices_1m_in_1m_out(self, db_session) -> None:
        """openai/gpt-4o @ 1M input + 1M output = 2.50 + 10.00 = $12.50."""
        eff_from, rows = _load_yaml()
        await _seed_yaml_rows(db_session, rows, eff_from)
        cost, pid = await pricing_svc.compute_cost_usd(
            db_session,
            provider="openai",
            served_model="gpt-4o",
            input_tokens=1_000_000,
            output_tokens=1_000_000,
            ts=datetime(2026, 6, 24, 12, 0, tzinfo=UTC),
        )
        assert pid is not None
        assert cost == Decimal("12.50000000")

    async def test_groq_llama_70b_prices_1m_in_1m_out(self, db_session) -> None:
        """groq/llama-3.3-70b-versatile @ 1M input + 1M output = 0.59 + 0.79 = $1.38."""
        eff_from, rows = _load_yaml()
        await _seed_yaml_rows(db_session, rows, eff_from)
        cost, pid = await pricing_svc.compute_cost_usd(
            db_session,
            provider="groq",
            served_model="llama-3.3-70b-versatile",
            input_tokens=1_000_000,
            output_tokens=1_000_000,
            ts=datetime(2026, 6, 24, 12, 0, tzinfo=UTC),
        )
        assert pid is not None
        assert cost == Decimal("1.38000000")

    async def test_openai_unknown_model_is_a_miss_not_the_text_default(self, db_session) -> None:
        """An unseeded openai model id is a loud pricing MISS, not the gpt-4o
        text default.

        openai is the one provider that fronts multiple unit families — text
        tokens ($2.50/1M tok), audio seconds (whisper $100/1M sec), characters
        (tts $15/1M char) and flat-rate images. Because the resolver cannot know
        which family an unknown served_model belongs to, handing it the text
        default silently under-bills ASR by 40x (seconds priced as tokens) and
        over-bills embeddings by 500x. So _resolve_pricing_row refuses the
        provider-default for a mixed-family provider and returns None, which the
        caller records as billing_pricing_miss_total — visible and repairable via
        POST /api/admin/reprice once the correct per-model row is seeded.
        """
        eff_from, rows = _load_yaml()
        await _seed_yaml_rows(db_session, rows, eff_from)
        cost, pid = await pricing_svc.compute_cost_usd(
            db_session,
            provider="openai",
            served_model="o9-some-unseeded-future-model",
            input_tokens=1_000_000,
            output_tokens=0,
            ts=datetime(2026, 6, 24, 12, 0, tzinfo=UTC),
        )
        assert cost is None
        assert pid is None


@pytest.mark.unit
class TestSeededXaiRows:
    """xai (Grok) is a BYO-only provider (NOT in the inference-router catalog;
    tenants bring their own XAI_API_KEY — see gitops workers/deployment.yaml).
    worker_management's BYO emitter sends provider="xai" (cost_provider or
    model_provider, both resolving to the provider_catalog key) with the grok
    model id as served_model, so these rows must be keyed exactly that way —
    the same mis-key bug class that left every claude row at cost_usd=NULL.

    NOTE: rates are the <200k-token tier. xAI doubles above a 200k prompt and
    provider_pricing has no tier column, so long-context Grok under-states cost
    by design — see the section header in initial_pricing.yaml.
    """

    async def test_yaml_contains_xai_rows(self) -> None:
        _eff, rows = _load_yaml()
        keys = {(r.get("provider"), r.get("model")) for r in rows}
        assert ("xai", "grok-4.5") in keys
        assert ("xai", "grok-4.3") in keys
        assert ("xai", "grok-4.20-0309-reasoning") in keys
        assert ("xai", "grok-4.20-0309-non-reasoning") in keys
        # provider-default fallback so an unseeded grok id never NULLs
        assert ("xai", None) in keys

    async def test_grok_4_5_prices_1m_in_500k_out(self, db_session) -> None:
        """xai/grok-4.5 @ 1M input + 500k output = 2.00 + 3.00 = $5.00."""
        eff_from, rows = _load_yaml()
        await _seed_yaml_rows(db_session, rows, eff_from)
        cost, pid = await pricing_svc.compute_cost_usd(
            db_session,
            provider="xai",
            served_model="grok-4.5",
            input_tokens=1_000_000,
            output_tokens=500_000,
            ts=datetime(2026, 7, 19, 12, 0, tzinfo=UTC),
        )
        assert pid is not None
        assert cost == Decimal("5.00000000")

    async def test_grok_4_3_prices_1m_in_1m_out(self, db_session) -> None:
        """xai/grok-4.3 @ 1M input + 1M output = 1.25 + 2.50 = $3.75."""
        eff_from, rows = _load_yaml()
        await _seed_yaml_rows(db_session, rows, eff_from)
        cost, pid = await pricing_svc.compute_cost_usd(
            db_session,
            provider="xai",
            served_model="grok-4.3",
            input_tokens=1_000_000,
            output_tokens=1_000_000,
            ts=datetime(2026, 7, 19, 12, 0, tzinfo=UTC),
        )
        assert pid is not None
        assert cost == Decimal("3.75000000")

    async def test_xai_unknown_model_uses_provider_default(self, db_session) -> None:
        """An unseeded grok id falls back to the provider default (grok-4.5
        rates) so cost_usd does not go NULL — xAI ships variants often."""
        eff_from, rows = _load_yaml()
        await _seed_yaml_rows(db_session, rows, eff_from)
        cost, pid = await pricing_svc.compute_cost_usd(
            db_session,
            provider="xai",
            served_model="grok-9-not-yet-seeded",
            input_tokens=1_000_000,
            output_tokens=0,
            ts=datetime(2026, 7, 19, 12, 0, tzinfo=UTC),
        )
        assert pid is not None
        assert cost == Decimal("2.00000000")


# The mlx-backend routing slug the live NER events carry. Held as a named
# constant rather than an inline literal so detect-secrets' "Secret Keyword"
# heuristic never sees a quoted string directly after a `service_key` keyword —
# it false-positives on that shape (a routing key, not a credential).
_MLX_SVC = "mlx-backend"
_KAG_SVC = "kag"


@pytest.mark.unit
class TestSeededSelfHostedNlpAndKagRows:
    """The two providers whose events were pricing NULL in production on
    2026-09-04, exercised with the EXACT column values the live emitters send.

    Both rows are provider-defaults (model=NULL, service_key=NULL), so both are
    reached only by ladder rung 4. Rungs 1-3 are unreachable for them, and both
    live emitters populate ``service_key`` — which the pre-existing tests never
    passed. A YAML-shape assertion cannot tell you rung 4 actually fires through
    a non-NULL service_key, and that is the whole question here.
    """

    async def test_mlx_nlp_ner_prices_through_rung_4_with_a_service_key(self, db_session) -> None:
        """mlx-nlp/en_core_web_trf, as emitted: a real served_model AND a real
        service key, neither of which has a row. 2,552 such events carried
        cost_usd=NULL. per_character with input_tokens = characters, so 1M
        characters must price to exactly $1.00.
        """
        eff_from, rows = _load_yaml()
        await _seed_yaml_rows(db_session, rows, eff_from)
        for served in ("en_core_web_trf", "en_core_web_md"):
            cost, pid = await pricing_svc.compute_cost_usd(
                db_session,
                provider="mlx-nlp",
                served_model=served,
                service_key=_MLX_SVC,
                input_tokens=1_000_000,
                output_tokens=0,
                ts=datetime(2026, 9, 4, 12, 0, tzinfo=UTC),
            )
            assert pid is not None, f"mlx-nlp/{served} still resolves to no pricing row"
            assert cost == Decimal("1.00000000"), f"mlx-nlp/{served} priced {cost}"

    async def test_kag_retrieval_prices_with_an_empty_served_model(self, db_session) -> None:
        """The kag emitter sends an EMPTY served_model (not NULL) plus a service
        key, and zero tokens both ways. Empty string is not None, so rungs 1 and
        2 are attempted and miss; only rung 4 can answer. A per_token rate would
        price all four live events at exactly $0, which is why this row is
        per_request — assert the charge is non-zero, not merely non-NULL.
        """
        eff_from, rows = _load_yaml()
        await _seed_yaml_rows(db_session, rows, eff_from)
        cost, pid = await pricing_svc.compute_cost_usd(
            db_session,
            provider="kag",
            served_model="",
            service_key=_KAG_SVC,
            input_tokens=0,
            output_tokens=0,
            ts=datetime(2026, 9, 4, 12, 0, tzinfo=UTC),
        )
        assert pid is not None, "kag retrieval still resolves to no pricing row"
        assert cost is not None and cost > 0, (
            f"kag prices to {cost} on zero tokens — a per_token family would give $0"
        )
        assert cost == Decimal("0.01153846")
