"""Coverage guards for scripts/initial_pricing.yaml — no provider may price NULL.

A pricing miss does not raise. ``compute_cost_usd`` returns ``(None, None)``,
``billing_cost_events.cost_usd`` is written NULL, and the rollups
``COALESCE(cost_usd, 0)`` it away — the revenue disappears with no error
anywhere. 17,626 such events existed in prod on 2026-07-19.

Two distinct failure modes produced them, and these tests guard both:

1. **No provider-default row.** The resolver
   (``services.pricing._resolve_pricing_row``) matches on the EXACT model
   string. A provider that has only per-model rows prices NULL the moment a
   vendor ships a model id we haven't seeded — which vendors do constantly.
   Rung 4, ``(provider, model=NULL, service_key=NULL)``, is the only thing that
   catches it.
2. **A provider string no row is keyed to at all.** The
   ``required_providers:`` block in the YAML is the emitter<->pricing contract:
   every string a producer can put in ``cost_events.provider``. That is how the
   ``anthropic`` vs ``claude`` split (both live, both required) and the
   ``inference_router`` override-fallthrough went unnoticed.

``test_unseeded_model_resolves_to_provider_default`` is the load-bearing one:
it seeds the real YAML into the DB and runs the real ``compute_cost_usd``, so
it proves rung 4 actually resolves rather than merely asserting YAML shape.
"""

from __future__ import annotations

from datetime import UTC, datetime
from decimal import Decimal
from typing import Any

import pytest
import yaml

from services import pricing as pricing_svc
from tests.test_initial_pricing_seed import YAML_PATH, _load_yaml, _seed_yaml_rows

# After every effective_from in the seed, so every row is live at this instant.
_TS = datetime(2026, 7, 19, 12, 0, tzinfo=UTC)

# A model id no vendor will ever serve. The point is that the resolver must NOT
# find a (provider, model) row for it and must fall through to rung 4.
_FABRICATED_MODEL = "totally-not-a-real-model-xyz"


def _cost_rows(rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    """Only rate_class='cost' rows — 'list' rows feed the BYO transparency
    estimate (services/list_pricing.py) and deliberately have no defaults."""
    return [r for r in rows if r.get("rate_class", "cost") == "cost"]


def _cost_providers(rows: list[dict[str, Any]]) -> list[str]:
    return sorted({r["provider"] for r in _cost_rows(rows)})


def _mixed_family_providers(rows: list[dict[str, Any]]) -> set[str]:
    """Providers whose cost rows span >1 cost_model family. For these the
    resolver deliberately REFUSES the rung-4 provider-default (it cannot know an
    unknown model's unit), so an unseeded id is a loud pricing_miss rather than a
    silent cross-unit misprice. openai is the canonical case (text tokens + audio
    seconds + characters + flat-rate images). See
    services.pricing._resolve_pricing_row and
    tests/test_pricing.py::TestMixedFamilyDefaultRefusal."""
    by_provider: dict[str, set[str]] = {}
    for r in _cost_rows(rows):
        by_provider.setdefault(r["provider"], set()).add(r["cost_model"])
    return {p for p, fams in by_provider.items() if len(fams) > 1}


@pytest.mark.unit
class TestProviderDefaultCoverage:
    def test_every_provider_has_a_default_row(self) -> None:
        """Every cost-class provider must carry a (model=NULL, service_key=NULL)
        row — the resolver's rung 4, the only backstop against an unseeded
        model id pricing NULL."""
        _eff, rows = _load_yaml()
        defaults = {
            r["provider"]
            for r in _cost_rows(rows)
            if r.get("model") is None and r.get("service_key") is None
        }
        missing = [p for p in _cost_providers(rows) if p not in defaults]
        assert not missing, (
            "providers with cost rows but NO provider-default row (model: null, "
            f"service_key: null): {missing}. Any model id these providers emit that "
            "isn't seeded per-model will write cost_usd=NULL and vanish from billing. "
            "Add a `model: null` row — convention is the MOST EXPENSIVE of that "
            "provider's existing rows, so an unseeded model is never under-billed."
        )

    async def test_unseeded_model_resolves_to_provider_default(self, db_session) -> None:
        """The real guard: seed the real YAML, then ask the real pricing service
        to cost a fabricated model id for every provider. Every one must return
        a non-None cost and a pricing_id, i.e. rung 4 actually fired.

        A YAML-shape assertion alone would not catch a resolver regression, a
        bad rate_class, or an effective_from that hasn't opened yet.
        """
        eff_from, rows = _load_yaml()
        await _seed_yaml_rows(db_session, rows, eff_from)

        # Mixed-family providers are EXCLUDED here and pinned in the sibling test
        # below: for them an unseeded id is an intentional miss, not a leak.
        mixed = _mixed_family_providers(rows)
        failures: list[str] = []
        for provider in _cost_providers(rows):
            if provider in mixed:
                continue
            cost, pricing_id = await pricing_svc.compute_cost_usd(
                db_session,
                provider=provider,
                served_model=_FABRICATED_MODEL,
                input_tokens=1_000_000,
                output_tokens=1_000_000,
                ts=_TS,
            )
            if cost is None or pricing_id is None:
                failures.append(f"{provider} -> cost={cost!r} pricing_id={pricing_id!r}")

        assert not failures, (
            "these single-family providers price an unseeded model id to NULL — "
            f"the money silently leaves the system: {failures}"
        )

    async def test_mixed_family_provider_refuses_default_as_a_loud_miss(self, db_session) -> None:
        """The other half of the guard: a provider whose rows span >1 cost_model
        family must NOT price an unknown id off its default — it must MISS, so
        the cross-unit misprice (openai ASR seconds at the text token rate, a 40x
        under-bill) becomes a visible billing_pricing_miss_total instead."""
        eff_from, rows = _load_yaml()
        await _seed_yaml_rows(db_session, rows, eff_from)

        mixed = _mixed_family_providers(rows)
        # EXCLUSIVITY, not just membership. This used to assert only
        # `"openai" in mixed` while the comment claimed openai was "the (only)"
        # mixed-family provider — so nothing enforced the claim. That gap is a
        # silent whole-provider outage waiting to happen, because the two halves
        # of this guard are complementary: the sibling
        # test_unseeded_model_resolves_to_provider_default SKIPS every provider in
        # `mixed`. So the day someone adds, say, a per_token row to `mlx-nlp`
        # (today per_character only) or a per_character row to `kag` (today
        # per_request only), that provider would (a) start REFUSING its rung-4
        # default, NULLing cost_usd for every event whose model is not explicitly
        # seeded, and (b) drop out of the leak test that would have caught it —
        # both silently, with the suite still green. Pin the set instead: making a
        # provider mixed-family is a deliberate act that must edit this line and
        # seed per-model rows for every id that provider can emit.
        # mlx-api / mlx-api-text joined 2026-10-17 (strata): one mlx_api host
        # serves several capabilities in different units, and every model they
        # emit is seeded per model — see TestStrataLocalProviders below.
        assert mixed == {"openai", "mlx-api", "mlx-api-text"}, (
            "openai, mlx-api and mlx-api-text front rows in several units and must "
            "be the ONLY mixed-family providers in the seed. A newly mixed-family provider "
            "REFUSES its rung-4 provider-default (every unseeded model id prices "
            "NULL) and is simultaneously exempted from "
            "test_unseeded_model_resolves_to_provider_default, so the leak is "
            "invisible. If this is intentional, seed per-model rows for every id "
            f"that provider emits, then add it here. Got {sorted(mixed)}"
        )
        for provider in sorted(mixed):
            cost, pricing_id = await pricing_svc.compute_cost_usd(
                db_session,
                provider=provider,
                served_model=_FABRICATED_MODEL,
                input_tokens=1_000_000,
                output_tokens=1_000_000,
                ts=_TS,
            )
            assert cost is None and pricing_id is None, (
                f"{provider} is mixed-family; an unseeded id must MISS, not resolve "
                f"to a cross-unit default (got cost={cost!r} pricing_id={pricing_id!r})"
            )

    async def test_unseeded_model_default_is_priced_not_zero(self, db_session) -> None:
        """A per_token provider-default with all-zero rates would pass the
        not-None check while still billing nothing. Assert the token-priced
        providers actually produce a positive cost.

        Excluded by design: ``self_hosted_amortized`` providers cost $0 per
        event (the monthly fixed cost is amortized at report time), and the
        zero-output media/KAG rows are per_request (priced below via
        per_request_usd, so > 0 anyway).
        """
        eff_from, rows = _load_yaml()
        await _seed_yaml_rows(db_session, rows, eff_from)

        amortized = {
            r["provider"]
            for r in _cost_rows(rows)
            if r.get("cost_model") == "self_hosted_amortized"
            and r.get("model") is None
            and r.get("service_key") is None
        }
        zero_cost: list[str] = []
        for provider in _cost_providers(rows):
            if provider in amortized:
                continue
            cost, _pid = await pricing_svc.compute_cost_usd(
                db_session,
                provider=provider,
                served_model=_FABRICATED_MODEL,
                input_tokens=1_000_000,
                output_tokens=1_000_000,
                ts=_TS,
            )
            if cost is not None and cost <= Decimal("0"):
                zero_cost.append(provider)

        assert not zero_cost, f"provider-default resolves but bills $0 on 1M+1M tokens: {zero_cost}"


@pytest.mark.unit
class TestRequiredProvidersContract:
    """``required_providers:`` is the emitter<->pricing contract. It is kept in
    THIS repo on purpose — billing-service does not import from
    worker_management, so the seed stays deployable on its own.
    """

    def test_required_providers_are_covered(self) -> None:
        _eff, rows = _load_yaml()
        doc_providers = {r["provider"] for r in rows}
        required = _load_required_providers()
        missing = [p for p in required if p not in doc_providers]
        assert not missing, (
            f"required_providers entries with NO pricing rows: {missing}. These are "
            "provider strings a producer can emit, so every event they generate "
            "prices NULL."
        )

    def test_required_providers_lists_every_seeded_provider(self) -> None:
        """The reverse direction: a provider that has rows but is absent from
        the contract list is a provider nobody is guarding. Keeps the list from
        rotting into a stale subset."""
        _eff, rows = _load_yaml()
        required = set(_load_required_providers())
        unlisted = [p for p in _cost_providers(rows) if p not in required]
        assert not unlisted, f"providers seeded but missing from required_providers: {unlisted}"

    def test_every_mlx_provider_is_seeded_and_listed(self) -> None:
        """Every MLX GPU emitter must have BOTH a pricing row and a
        ``required_providers`` entry — the two halves fail independently.

        The mlx_backend / mlx_ingestion_backend services on mini1 and mini2 each
        emit their own provider string, and the family grows one endpoint at a
        time: ``mlx-nlp`` was unseeded until 2026-09-04 (648 events priced NULL)
        and ``mlx-attention`` until 2026-09-17, when mlx_backend#49 shipped the
        ``/attention`` emitter to a live service with no row waiting for it.
        Both were found only after MeteringPricingMiss fired in prod.

        This asserts the two halves SEPARATELY and on purpose. A row with no
        contract entry is a provider nobody guards (it silently drops out of the
        list's protection the day the row moves); a contract entry with no row
        prices every event NULL. The generic tests above check each direction
        globally, but nothing pins the MLX family itself, so a new mlx-* service
        can ship with neither half and no test names it.
        """
        _eff, rows = _load_yaml()
        seeded = {r["provider"] for r in _cost_rows(rows)}
        required = set(_load_required_providers())
        expected = (
            "mlx-nlp",
            "mlx-whisper",
            "mlx-embed",
            "mlx-clip",
            "mlx-ocr",
            "mlx-rerank",
            "mlx-attention",
        )
        missing_rows = [p for p in expected if p not in seeded]
        assert not missing_rows, (
            f"MLX providers with NO pricing row: {missing_rows}. The service is "
            "live and emitting, so every event it produces writes cost_usd=NULL, "
            "fires MeteringPricingMiss and is skipped by billing.generate_billing()."
        )
        missing_contract = [p for p in expected if p not in required]
        assert not missing_contract, (
            f"MLX providers absent from required_providers: {missing_contract}. "
            "They are unguarded: nothing fails if their row is later removed."
        )

    def test_mlx_attention_prices_the_live_emitter_literal(self) -> None:
        """mlx_backend sends ``model="scaled-dot-product"`` (ml_routes.py), a
        literal we deliberately do NOT pin a row to.

        The op is parameter-free SDPA, so that string names an algorithm rather
        than a checkpoint. The provider-default (model=None) row is what must
        catch it on rung 4 of ``_resolve_pricing_row``. Pinning the literal
        instead would price NULL the day the kernel is renamed, so this test
        asserts the DEFAULT exists and carries a non-zero per_request rate —
        a per_token row here would price every attention call at exactly $0.
        """
        _eff, rows = _load_yaml()
        defaults = [
            r
            for r in _cost_rows(rows)
            if r["provider"] == "mlx-attention" and r.get("model") is None
        ]
        assert len(defaults) == 1, (
            f"expected exactly one mlx-attention provider-default row, got {defaults}"
        )
        row = defaults[0]
        assert row["cost_model"] == "per_request", (
            "mlx-attention emits no token counts (quantity = #key vectors, "
            f"usage_extra.unit=attention_keys), so {row['cost_model']} prices it at $0."
        )
        assert Decimal(row["per_request_usd"]) > 0, "mlx-attention default row is free"

    def test_anthropic_and_claude_are_both_present(self) -> None:
        """Both keys are live and neither may be 'deduplicated' away.

        ``claude`` is what inference-router emits (its provider `name: claude`).
        ``anthropic`` is what worker_management's BYO emitter sends: the
        provider_catalog key `anthropic` has no `_BILLING_PROVIDER_OVERRIDES`
        entry, so `billing_provider_for` returns "anthropic" unchanged. The
        2026-05-09 rename to `claude` fixed router traffic and silently left BYO
        traffic pricing NULL.
        """
        _eff, rows = _load_yaml()
        keys = {(r.get("provider"), r.get("model")) for r in _cost_rows(rows)}
        for model in (
            "claude-sonnet-4-20250514",
            "claude-sonnet-4-6",
            "claude-opus-4-7",
            "claude-haiku-4-5",
        ):
            assert ("anthropic", model) in keys, f"anthropic/{model} missing from seed"
        assert ("anthropic", None) in keys
        assert ("claude", None) in keys


@pytest.mark.unit
class TestStrataLocalProviders:
    """strata-rag / strata-ingest local_gpu events price off the seeded rows.

    Each case is the exact (provider, model, service_key, quantity_unit) the
    emitter sends (strata_rag / strata_ingest ``src/metering.rs`` + the gitops
    ``providers.toml`` endpoints), run through the real resolver against the real
    seed. The expected cost is the copied sibling rate applied to the quantity.
    """

    @pytest.mark.parametrize(
        ("provider", "model", "service_key", "unit", "inp", "out", "expected"),
        [
            # embed: 1M bge-m3 tokens at mlx-embed's $0.10/1M chars x 4 chars/token
            ("mlx-api-text", "BAAI/bge-m3", "strata-rag-embed", "tokens", 1_000_000, 0, "0.40"),
            # rerank: flat per call whatever the passage count (mlx-rerank)
            (
                "mlx-api-text",
                "BAAI/bge-reranker-v2-m3",
                "strata-rag-rerank",
                "passages",
                40,
                0,
                "0.001",
            ),
            # transcribe: 60 s at mlx-whisper's $0.017/min
            (
                "mlx-api",
                "mlx-community/whisper-large-v3-turbo",
                "strata-ingest-transcribe",
                "seconds",
                60,
                0,
                "0.017",
            ),
            # OCR: pages in input_tokens, no quantity_unit; owner $0.0015/page
            (
                "mlx-api",
                "mlx-community/PaddleOCR-VL-1.5-bf16",
                "strata-ingest-ocr",
                None,
                8,
                0,
                "0.012",
            ),
            # ...and the same event if the emitter starts asserting pages
            (
                "mlx-api",
                "mlx-community/PaddleOCR-VL-1.5-bf16",
                "strata-ingest-ocr",
                "pages",
                1,
                0,
                "0.0015",
            ),
            # vision: owner rate 2026-10-18, $0.10/1M in + out; strata_ingest
            # sends total_tokens as input_tokens
            (
                "mlx-api",
                "mlx-community/Qwen2-VL-2B-Instruct-4bit",
                "strata-ingest-vision",
                "tokens",
                1_000_000,
                0,
                "0.10",
            ),
            # chat: pgx-local local-llm $10/$25 per 1M
            ("vllm", "local-llm", "strata-rag-chat", "tokens", 1_000_000, 1_000_000, "35"),
        ],
    )
    async def test_emitted_shape_prices(
        self, db_session, provider, model, service_key, unit, inp, out, expected
    ) -> None:
        eff_from, rows = _load_yaml()
        await _seed_yaml_rows(db_session, rows, eff_from)
        cost, pricing_id = await pricing_svc.compute_cost_usd(
            db_session,
            provider=provider,
            served_model=model,
            input_tokens=inp,
            output_tokens=out,
            service_key=service_key,
            quantity_unit=unit,
            ts=_TS,
        )
        assert pricing_id is not None, f"{provider}/{model} priced NULL"
        assert cost == Decimal(expected), f"{provider}/{model}: {cost} != {expected}"

    async def test_unseeded_mlx_api_model_misses_loudly(self, db_session) -> None:
        """mlx-api is mixed-family, so a model it emits that is NOT seeded must
        MISS rather than be priced off the seconds-rate provider default."""
        eff_from, rows = _load_yaml()
        await _seed_yaml_rows(db_session, rows, eff_from)
        cost, pricing_id = await pricing_svc.compute_cost_usd(
            db_session,
            provider="mlx-api",
            served_model="mlx-community/some-future-model",
            input_tokens=431,
            output_tokens=0,
            quantity_unit="tokens",
            ts=_TS,
        )
        assert cost is None and pricing_id is None


def _load_required_providers() -> list[str]:
    with YAML_PATH.open("r", encoding="utf-8") as fh:
        doc = yaml.safe_load(fh)
    required = doc.get("required_providers")
    assert required, (
        "initial_pricing.yaml has no `required_providers:` block — the "
        "emitter<->pricing contract is missing."
    )
    return list(required)
