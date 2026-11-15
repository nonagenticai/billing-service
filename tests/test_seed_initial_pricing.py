"""Lightweight schema test for scripts/initial_pricing.yaml.

Catches accidental edits — the YAML is the seed-from-empty starting point and
must match the verified rates from 2026-04-26. If you intentionally change the
YAML, update this test alongside it (and ideally retire the YAML in favor of
DB-side audited POSTs going forward).
"""

from __future__ import annotations

from decimal import Decimal
from pathlib import Path

import yaml

YAML_PATH = Path(__file__).resolve().parent.parent / "scripts" / "initial_pricing.yaml"

# The VLM product slug (a routing/product key, not a credential). Held as a
# constant so detect-secrets sees an identifier ref, not a quoted literal after
# a `service_key:` keyword (its "Secret Keyword" heuristic false-positives on the
# inline string).
_VLM_SLUG = "w2v-vlm"  # pragma: allowlist secret -- product slug, not a credential


def _load() -> dict:
    with YAML_PATH.open("r", encoding="utf-8") as fh:
        return yaml.safe_load(fh)


def test_yaml_parses() -> None:
    doc = _load()
    assert isinstance(doc, dict)
    assert doc["version"] == 1
    assert doc["effective_from"] == "2026-04-26T00:00:00Z"
    assert isinstance(doc["rows"], list)
    # 5 original (sonnet-4-20250514 + anthropic-default + deepseek-chat + gemma + mlx_inference)
    # + 3 Claude 4.x family rows (opus-4-7, sonnet-4-6, haiku-4-5) added 2026-05-08
    # + 1 pgx-local per-token row added 2026-06-19
    # + 4 w2v local media rows (kokoro, kitten, mlx-whisper, mflux) added 2026-06-20
    # + 4 MLX GPU rows (mlx-embed, mlx-rerank, mlx-ocr, mlx-clip) added 2026-06-22
    # + 5 BYO openai rows (gpt-4o, gpt-4o-mini, gpt-4.1, gpt-4.1-mini, default)
    # + 3 BYO groq rows (llama-3.3-70b-versatile, llama-3.1-8b-instant, default) added 2026-06-24
    # + 1 video-analyzer per-minute row (model=local) added 2026-06-24 to mirror
    #   live id=32 (originally seeded in-process, missing from this file).
    # + 1 video-compiler per-output-minute row (model=ffmpeg) added 2026-06-25.
    # + 4 KAG-pipeline per_request rows (webscraper, karma, hirag, kag-graph)
    #   added 2026-07-09 — self-hosted stages metered by the P5–P7 cutovers.
    # + 15 external-cloud modality cost rows added 2026-07-09 (managed-BYO /
    #   owner_provided basis): openai tts-1/tts-1-hd/gpt-4o-mini-tts,
    #   whisper-1/gpt-4o-transcribe/gpt-4o-mini-transcribe, gpt-image-1/dall-e-3,
    #   text-embedding-3-small/large (10) + elevenlabs ×3 + deepgram ×2.
    # + 2 VLM cost rows (openai gpt-4o/gpt-4o-mini, service_key=w2v-vlm) added
    #   2026-07-10 for the modality-gateway VLM route.
    # + 17 rate_class='list' rows added 2026-07-10 (Slice-2 BYO transparency):
    #   15 mirroring the modality cost rows (NULL service_key) + 2 VLM
    #   (openai gpt-4o/gpt-4o-mini, service_key=w2v-vlm).
    # + 7 xai/Grok BYO rows added 2026-07-19 (worker_management #130 shipped the
    #   xAI provider): 6 per-model (grok-4.5, grok-4.3, grok-4.20-0309-reasoning,
    #   grok-4.20-0309-non-reasoning, grok-4.20-multi-agent-0309, grok-build-0.1)
    #   + 1 provider-default (model=NULL).
    # + 25 pricing-coverage rows added 2026-07-19 (WS1-A: 17,626 live cost
    #   events had cost_usd=NULL). 5 anthropic (the BYO provider key, a live
    #   mirror of the `claude` block that existed only in the DB) + 3 deepseek
    #   (v4-pro, v4-flash, provider-default) + 3 cohere + 3 google + 6
    #   inference_router (override-fallthrough net) + 5 provider-default-only
    #   rows for providers that had per-model rows but no rung-4 fallback
    #   (pgx-local, video-analyzer, video-compiler, elevenlabs, deepgram).
    # + 4 pricing-drift guard rows added 2026-07-23 (concrete ids that mis-billed
    #   off a provider-default): claude/claude-haiku-4-5-20251001 +
    #   anthropic/claude-haiku-4-5-20251001 (dated haiku snapshot, was Opus 18x) +
    #   inference_router/deepseek-v4-flash + inference_router/deepseek-v4-pro
    #   (raw deepseek ids under the router, were $10/$25 35-70x). See
    #   tests/test_pricing_drift.py.
    # + 1 mlx-nlp provider-default row added 2026-09-04 (per_character, $1.00/1M
    #   chars). MeteringPricingMiss fired on mlx-nlp/en_core_web_trf: 648 NER cost
    #   events carried cost_usd=NULL, so generate_billing() skipped them entirely.
    #   Rate is the three-vendor market floor — AWS Comprehend DetectEntities,
    #   Google Cloud NL Entity Analysis and Azure AI Language NER all list exactly
    #   USD 1.00 per 1M characters at entry tier. See
    #   docs/PRICING_SELF_HOSTED_NLP.md.
    # + 1 kag provider-default row added 2026-09-04 (per_request). The RETRIEVAL
    #   stage emits provider="kag" — a different string from the `kag-graph` BUILD
    #   row — and was never seeded, so its events carried cost_usd=NULL. It sends
    #   input_tokens=output_tokens=0, so only a per_request rate can charge at all.
    # ⚠️ A FLOOR, NOT AN EQUALITY. This read `== 103`, and the comment block
    # above it is the receipt: a chronicle of every row anyone has added, each
    # one having forced a bump of this scalar. That is one shared number every
    # branch contends over — no merge order produces a correct value, and the
    # next author to add a pricing row is failed by a test about a count they
    # had no reason to think about. Growth is the NORMAL case here (2-4 rows a
    # month, per that chronicle).
    #
    # The floor still catches the thing worth catching — the seed file being
    # truncated or a block going missing. What it does not do is charge every
    # future author for adding a row. Coverage is the real gate and it lives
    # elsewhere: `required_providers:` below, plus tests/test_pricing_coverage.py,
    # which assert that every provider a producer can EMIT has a row — a
    # property, where this is a quantity.
    assert len(doc["rows"]) >= 103
    # Every provider a producer can emit is now declared; see the
    # emitter<->pricing contract block in the YAML and tests/test_pricing_coverage.py.
    assert isinstance(doc["required_providers"], list)


def test_byo_openai_rows() -> None:
    """BYO openai rows: 4 LLM per-model + provider-default (2026-06-24), plus the
    external-cloud modality models (tts / asr / image / embedding) added 2026-07-09.

    Scoped to the canonical cost rows (rate_class='cost', NULL service_key); the
    rate_class='list' rows and the w2v-vlm-scoped VLM rows are asserted
    separately below."""
    doc = _load()
    by_model = {
        r.get("model"): r
        for r in doc["rows"]
        if r["provider"] == "openai"
        and r.get("rate_class", "cost") == "cost"
        and r.get("service_key") is None
    }
    assert set(by_model) == {
        # LLM (text)
        "gpt-4o",
        "gpt-4o-mini",
        "gpt-4.1",
        "gpt-4.1-mini",
        None,
        # TTS
        "tts-1",
        "tts-1-hd",
        "gpt-4o-mini-tts",
        # ASR
        "whisper-1",
        "gpt-4o-transcribe",
        "gpt-4o-mini-transcribe",
        # Image
        "gpt-image-1",
        "dall-e-3",
        # Embedding
        "text-embedding-3-small",
        "text-embedding-3-large",
    }
    gpt4o = by_model["gpt-4o"]
    assert gpt4o["cost_model"] == "per_token"
    assert Decimal(gpt4o["input_per_1m_usd"]) == Decimal("2.50")
    assert Decimal(gpt4o["output_per_1m_usd"]) == Decimal("10.00")
    # provider-default mirrors gpt-4o so an unseeded id never NULLs cost_usd.
    assert Decimal(by_model[None]["input_per_1m_usd"]) == Decimal("2.50")


def test_seed_key_disambiguates_rate_class_and_service_key() -> None:
    """The seeder dedup key must match the DB open-row uniqueness
    (rate_class, provider, model, service_key) so a 'list' row does not collide
    with the 'cost' row of the same (provider, model), and a service_key-scoped
    VLM row does not collide with the NULL-service_key LLM row. Every YAML row
    must therefore produce a distinct key."""
    import sys

    sys.path.insert(0, str(YAML_PATH.parent))
    from seed_initial_pricing import (  # ty: ignore[unresolved-import]  # scripts/ is on sys.path only at runtime
        _key,
    )

    doc = _load()
    keys = [_key(r) for r in doc["rows"]]
    assert len(set(keys)) == len(keys), "duplicate seed keys — a list/cost or service_key collision"

    cost = {"provider": "openai", "model": "gpt-4o"}
    lst = {"provider": "openai", "model": "gpt-4o", "rate_class": "list"}
    vlm = {"provider": "openai", "model": "gpt-4o", "service_key": _VLM_SLUG}
    assert _key(cost) != _key(lst) != _key(vlm) != _key(cost)


def test_modality_list_rows_present() -> None:
    """Slice-2 BYO transparency: every modality cost row has a mirroring
    rate_class='list' row so the read-time provider estimate populates (else it
    returns None). 17 list rows total (15 modality NULL-service_key + 2 VLM)."""
    doc = _load()
    list_rows = [r for r in doc["rows"] if r.get("rate_class") == "list"]
    assert len(list_rows) == 17
    # The 15 modality list rows carry NULL service_key so the list resolver
    # matches a modality line via rung 2 (provider, model, NULL).
    modality_list = [r for r in list_rows if r.get("service_key") is None]
    assert {(r["provider"], r["model"]) for r in modality_list} == {
        ("openai", "tts-1"),
        ("openai", "tts-1-hd"),
        ("openai", "gpt-4o-mini-tts"),
        ("elevenlabs", "eleven_multilingual_v2"),
        ("elevenlabs", "eleven_turbo_v2_5"),
        ("elevenlabs", "eleven_flash_v2_5"),
        ("openai", "whisper-1"),
        ("openai", "gpt-4o-transcribe"),
        ("openai", "gpt-4o-mini-transcribe"),
        ("deepgram", "nova-3"),
        ("deepgram", "nova-2"),
        ("openai", "gpt-image-1"),
        ("openai", "dall-e-3"),
        ("openai", "text-embedding-3-small"),
        ("openai", "text-embedding-3-large"),
    }


def test_vlm_rows_are_service_key_scoped() -> None:
    """gpt-4o / gpt-4o-mini are shared with the LLM pricing rows, so the VLM
    cost+list rows MUST carry the w2v-vlm service_key — otherwise they collide
    with (or pollute) the LLM rows of the same (provider, model)."""
    doc = _load()
    vlm = [r for r in doc["rows"] if r.get("service_key") == _VLM_SLUG]
    # 2 cost + 2 list, over gpt-4o and gpt-4o-mini.
    assert {(r["model"], r.get("rate_class", "cost")) for r in vlm} == {
        ("gpt-4o", "cost"),
        ("gpt-4o-mini", "cost"),
        ("gpt-4o", "list"),
        ("gpt-4o-mini", "list"),
    }
    for r in vlm:
        assert r["provider"] == "openai"
        assert r["cost_model"] == "per_token"


def test_byo_groq_rows() -> None:
    """BYO groq rows added 2026-06-24: 2 per-model + 1 provider-default."""
    doc = _load()
    by_model = {r.get("model"): r for r in doc["rows"] if r["provider"] == "groq"}
    assert set(by_model) == {"llama-3.3-70b-versatile", "llama-3.1-8b-instant", None}
    llama70 = by_model["llama-3.3-70b-versatile"]
    assert llama70["cost_model"] == "per_token"
    assert Decimal(llama70["input_per_1m_usd"]) == Decimal("0.59")
    assert Decimal(llama70["output_per_1m_usd"]) == Decimal("0.79")
    # provider-default mirrors the 70b rate.
    assert Decimal(by_model[None]["input_per_1m_usd"]) == Decimal("0.59")


def test_claude_sonnet_4() -> None:
    doc = _load()
    rows = [r for r in doc["rows"] if r["provider"] == "claude" and r.get("model") is not None]
    # Dated rows: claude-sonnet-4-20250514 (the original) + the three Claude 4.x
    # short-IDs added 2026-05-08 + the claude-haiku-4-5-20251001 dated snapshot
    # added 2026-07-23 (pricing-drift guard, see tests/test_pricing_drift.py).
    by_model = {r["model"]: r for r in rows}
    assert set(by_model) == {
        "claude-sonnet-4-20250514",
        "claude-opus-4-7",
        "claude-sonnet-4-6",
        "claude-haiku-4-5",
        "claude-haiku-4-5-20251001",
    }

    sonnet = by_model["claude-sonnet-4-20250514"]
    assert sonnet["cost_model"] == "per_token"
    assert Decimal(sonnet["input_per_1m_usd"]) == Decimal("3.00")
    assert Decimal(sonnet["output_per_1m_usd"]) == Decimal("15.00")
    assert Decimal(sonnet["cache_creation_per_1m_usd"]) == Decimal("3.75")
    assert Decimal(sonnet["cache_read_per_1m_usd"]) == Decimal("0.30")


def test_claude_4_family() -> None:
    """Verify the three Claude 4.x rows added 2026-05-08 carry the canonical rates."""
    doc = _load()
    by_model = {
        r["model"]: r
        for r in doc["rows"]
        if r["provider"] == "claude" and r.get("model") is not None
    }

    opus = by_model["claude-opus-4-7"]
    assert opus["cost_model"] == "per_token"
    assert Decimal(opus["input_per_1m_usd"]) == Decimal("15.00")
    assert Decimal(opus["output_per_1m_usd"]) == Decimal("75.00")
    assert Decimal(opus["cache_creation_per_1m_usd"]) == Decimal("18.75")
    assert Decimal(opus["cache_read_per_1m_usd"]) == Decimal("1.50")

    sonnet = by_model["claude-sonnet-4-6"]
    assert sonnet["cost_model"] == "per_token"
    assert Decimal(sonnet["input_per_1m_usd"]) == Decimal("3.00")
    assert Decimal(sonnet["output_per_1m_usd"]) == Decimal("15.00")
    assert Decimal(sonnet["cache_creation_per_1m_usd"]) == Decimal("3.75")
    assert Decimal(sonnet["cache_read_per_1m_usd"]) == Decimal("0.30")

    haiku = by_model["claude-haiku-4-5"]
    assert haiku["cost_model"] == "per_token"
    assert Decimal(haiku["input_per_1m_usd"]) == Decimal("0.80")
    assert Decimal(haiku["output_per_1m_usd"]) == Decimal("4.00")
    assert Decimal(haiku["cache_creation_per_1m_usd"]) == Decimal("1.00")
    assert Decimal(haiku["cache_read_per_1m_usd"]) == Decimal("0.08")


def test_claude_provider_default() -> None:
    """THE CEILING RULE — DO NOT "FIX" THESE BACK TO SONNET'S $3/$15.

    A provider-default must be at least as expensive as EVERY per-model row of
    that provider, so it is Opus 4.7 rates ($15/$75) — the priciest seeded Claude
    model — with the cache rates scaled at the original ratios (1.25x input =
    18.75 write, 0.10x input = 1.50 read).

    WHY the ceiling and not the "most common" rate: the two errors are not
    symmetric. OVER-billing an unseeded model id self-corrects — seed that id
    with its real rate and the per-model row overrides. UNDER-billing is
    unrecoverable: the revenue was never measured and nothing raises. This row
    sat at Sonnet rates until 2026-07-20, so every unseeded Anthropic id
    (claude-opus-4-8, claude-fable-5) billed at 1/5 of Opus."""
    doc = _load()
    rows = [r for r in doc["rows"] if r["provider"] == "claude" and r.get("model") is None]
    assert len(rows) == 1, "exactly one claude provider-default row expected"
    row = rows[0]
    assert row["cost_model"] == "per_token"
    assert Decimal(row["input_per_1m_usd"]) == Decimal("15.00")
    assert Decimal(row["output_per_1m_usd"]) == Decimal("75.00")
    assert Decimal(row["cache_creation_per_1m_usd"]) == Decimal("18.75")
    assert Decimal(row["cache_read_per_1m_usd"]) == Decimal("1.50")


def test_deepseek_chat() -> None:
    doc = _load()
    # Scoped to the deepseek-chat row specifically: deepseek gained
    # deepseek-v4-flash/-v4-pro per-model rows and a provider-default in the
    # 2026-07-19 pricing-coverage pass, so a bare len()==1 no longer holds.
    rows = [
        r for r in doc["rows"] if r["provider"] == "deepseek" and r.get("model") == "deepseek-chat"
    ]
    assert len(rows) == 1
    row = rows[0]
    assert row["model"] == "deepseek-chat"
    assert row["cost_model"] == "per_token"
    assert Decimal(row["input_per_1m_usd"]) == Decimal("0.14")
    assert Decimal(row["output_per_1m_usd"]) == Decimal("0.28")
    # Cache rates intentionally absent until inference-router emits cache_hit_tokens.
    assert row.get("cache_creation_per_1m_usd") is None
    assert row.get("cache_read_per_1m_usd") is None


def test_pgx_local() -> None:
    """pgx-local: self-hosted local LLM on the DGX, but priced per-token
    (George's assigned internal rate 2026-06-19), unlike gemma/mlx_inference.
    Keyed to the emitted served_model "local-llm" (verified live in
    cost_events)."""
    doc = _load()
    # Scoped to the local-llm row: a pgx-local provider-default (model=null) was
    # added 2026-07-19 so the `default` alias — which _BILLING_PROVIDER_OVERRIDES
    # also maps to pgx-local — stops pricing NULL.
    rows = [
        r for r in doc["rows"] if r["provider"] == "pgx-local" and r.get("model") == "local-llm"
    ]
    assert len(rows) == 1, "exactly one pgx-local/local-llm row expected"
    row = rows[0]
    assert row["model"] == "local-llm"
    assert row["cost_model"] == "per_token"
    assert Decimal(row["input_per_1m_usd"]) == Decimal("10.00")
    assert Decimal(row["output_per_1m_usd"]) == Decimal("25.00")


def test_w2v_local_tts() -> None:
    """w2v local TTS (kokoro, kitten): $3/$15 per_character provider-default rows
    (model=null) added 2026-06-20. input_tokens = characters, output = 0, so
    $3 / 1M characters; per_character records that unit so the rate never
    cross-compares against a token rate. provider-default so any served_model
    matches."""
    doc = _load()
    by_provider = {r["provider"]: r for r in doc["rows"] if r["provider"] in {"kokoro", "kitten"}}
    assert set(by_provider) == {"kokoro", "kitten"}
    for provider, row in by_provider.items():
        assert row.get("model") is None, f"{provider} must be a provider-default row"
        assert row["cost_model"] == "per_character"
        assert Decimal(row["input_per_1m_usd"]) == Decimal("3.00")
        assert Decimal(row["output_per_1m_usd"]) == Decimal("15.00")


def test_asr_per_minute() -> None:
    """ASR mlx-whisper: billed per MINUTE of audio at $0.017/min (re-rated
    2026-06-21). The asr_backend emitter sends input_tokens = round(audio
    seconds), so per_token input rate is 0.017/60 * 1e6 = 283.33333333 / 1M
    seconds; output rate 0 (duration-only). 60 input "tokens" (seconds) must
    price to exactly $0.017."""
    doc = _load()
    rows = [r for r in doc["rows"] if r["provider"] == "mlx-whisper"]
    assert len(rows) == 1, "exactly one mlx-whisper row expected"
    row = rows[0]
    assert row.get("model") is None, "mlx-whisper must be a provider-default row"
    assert row["cost_model"] == "per_second"
    in_rate = Decimal(row["input_per_1m_usd"])
    assert in_rate == Decimal("283.33333333")
    assert Decimal(row["output_per_1m_usd"]) == Decimal("0")
    # 60 seconds of audio -> $0.017 (8-dp exact).
    cost_60s = (Decimal(60) * in_rate / Decimal(1_000_000)).quantize(Decimal("0.00000001"))
    assert cost_60s == Decimal("0.01700000")


def test_video_analyzer_per_minute() -> None:
    """video-analyzer: billed per MINUTE of source video at $0.017/min, same
    shape as ASR. The video_analyzer_service emitter sends provider=
    "video-analyzer", model="local", input_tokens = round(video seconds),
    output_tokens = 0; per_token input rate is 0.017/60 * 1e6 = 283.33333333 /
    1M seconds. Mirrors live provider_pricing id=32 (model=local). 60 input
    "tokens" (seconds) must price to exactly $0.017. Guards against the
    seed-from-empty file silently omitting the row (it was originally seeded
    in-process, not via this YAML) -> a fresh/CI/DR billing DB would pricing_miss
    and bill $0 for every video analysis."""
    doc = _load()
    # Scoped to the model=local row: a video-analyzer provider-default was added
    # 2026-07-19 (same rate) so a non-"local" served_model can't price NULL.
    rows = [
        r for r in doc["rows"] if r["provider"] == "video-analyzer" and r.get("model") == "local"
    ]
    assert len(rows) == 1, "exactly one video-analyzer/local row expected"
    row = rows[0]
    assert row.get("model") == "local", "video-analyzer row must match live id=32 (model=local)"
    assert row["cost_model"] == "per_second"
    in_rate = Decimal(row["input_per_1m_usd"])
    assert in_rate == Decimal("283.33333333")
    assert Decimal(row["output_per_1m_usd"]) == Decimal("0")
    # 60 seconds of video -> $0.017 (8-dp exact).
    cost_60s = (Decimal(60) * in_rate / Decimal(1_000_000)).quantize(Decimal("0.00000001"))
    assert cost_60s == Decimal("0.01700000")


def test_video_compiler_per_output_minute() -> None:
    """video-compiler: billed per MINUTE of OUTPUT media at $0.30/min (Shotstack
    render-API pay-as-you-go benchmark). The video_compiler_service emitter sends
    provider="video-compiler", model="ffmpeg", input_tokens = round(output
    seconds), output_tokens = 0; per_token input rate is 0.30/60 * 1e6 =
    5000.00000000 / 1M seconds. 60 input "tokens" (seconds) must price to exactly
    $0.30. Guards against the seed silently omitting the row -> a fresh/CI/DR
    billing DB would pricing_miss and bill $0 for every render."""
    doc = _load()
    # Scoped to the model=ffmpeg row: a video-compiler provider-default was added
    # 2026-07-19 (same rate) so a non-"ffmpeg" served_model can't price NULL.
    rows = [
        r for r in doc["rows"] if r["provider"] == "video-compiler" and r.get("model") == "ffmpeg"
    ]
    assert len(rows) == 1, "exactly one video-compiler/ffmpeg row expected"
    row = rows[0]
    assert row.get("model") == "ffmpeg", (
        "video-compiler row model must match the emitter served_model"
    )
    assert row["cost_model"] == "per_second"
    in_rate = Decimal(row["input_per_1m_usd"])
    assert in_rate == Decimal("5000.00000000")
    assert Decimal(row["output_per_1m_usd"]) == Decimal("0")
    # 60 seconds of output -> $0.30 (8-dp exact).
    cost_60s = (Decimal(60) * in_rate / Decimal(1_000_000)).quantize(Decimal("0.00000001"))
    assert cost_60s == Decimal("0.30000000")


def test_w2v_image_gen_per_request() -> None:
    """w2v image-gen (mflux): flat per-image fee. input_tokens carries
    megapixels, so a per-token rate would be ~$0/image — priced per_request."""
    doc = _load()
    rows = [r for r in doc["rows"] if r["provider"] == "mflux"]
    assert len(rows) == 1, "exactly one mflux row expected"
    row = rows[0]
    assert row.get("model") is None
    assert row["cost_model"] == "per_request"
    assert Decimal(row["per_request_usd"]) == Decimal("0.01")
    # No per-token rates on a per_request row.
    assert row.get("input_per_1m_usd") is None
    assert row.get("output_per_1m_usd") is None


def test_self_hosted_providers() -> None:
    doc = _load()
    self_hosted = [r for r in doc["rows"] if r["cost_model"] == "self_hosted_amortized"]
    providers = {r["provider"] for r in self_hosted}
    assert providers == {"gemma", "mlx_inference"}
    # Operator-set amortized monthly fixed costs — seeded to match the live
    # effective rows (prod already carries these; the seed was null → diverged).
    expected_fixed = {"gemma": 45.00, "mlx_inference": 20.00}
    for row in self_hosted:
        # Provider-default rows (model=None) for self-hosted.
        assert row.get("model") is None
        assert float(row["monthly_fixed_cost_usd"]) == expected_fixed[row["provider"]]
        # No per-token rates on self-hosted rows.
        assert row.get("input_per_1m_usd") is None
        assert row.get("output_per_1m_usd") is None


def test_every_row_has_notes_with_source() -> None:
    """Every seeded row must carry a notes field — that's the audit breadcrumb
    explaining where the number came from. Without it, future readers can't
    audit the seed."""
    doc = _load()
    for row in doc["rows"]:
        assert row.get("notes"), f"{row['provider']}/{row.get('model')} missing notes"
