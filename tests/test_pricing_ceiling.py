"""The provider-default CEILING rule for scripts/initial_pricing.yaml.

A provider-default row (``model: null``) is what an unseeded model id resolves
to on rung 4 of ``services/pricing.py::_resolve_pricing_row``. The convention
has been written down twice — in the YAML comments and in the assertion MESSAGE
of ``tests/test_pricing_coverage.py::test_every_provider_has_a_default_row``
("convention is the MOST EXPENSIVE of that provider's existing rows, so an
unseeded model is never under-billed") — and asserted nowhere. It duly rotted:
``anthropic`` and ``claude`` sat at Sonnet's $3/$15 while ``claude-opus-4-7``
is $15/$75, so every unseeded Anthropic id billed at 1/5 of Opus.

WHY THE RULE IS "CEILING" AND NOT "TYPICAL": the two errors are not symmetric.
Over-billing an unseeded id is SELF-CORRECTING — seed the id and its per-model
row takes over; the overcharge is visible in the data and refundable.
Under-billing is UNRECOVERABLE — the revenue was never measured, no row records
what should have been charged, and nothing raises.
"""

from __future__ import annotations

from collections import defaultdict
from decimal import Decimal
from typing import Any

import pytest

from tests.test_initial_pricing_seed import _load_yaml, _row_for

# The rate axes a default and a per-model row can be compared on. Deliberately
# excludes monthly_fixed_cost_usd (self_hosted_amortized rows bill $0/event, the
# fixed cost is amortized at report time) and the cache_* columns (a NULL cache
# column falls back to the input rate in services/pricing.py, so a NULL vs
# non-NULL pair is not a comparable ordering).
_RATE_AXES = ("input_per_1m_usd", "output_per_1m_usd", "per_request_usd")

# A provider-default only governs rows the resolver could have reached instead
# of it, so rows are grouped on the full resolution identity, not on provider
# alone. Each element of the key is load-bearing:
#   * provider    — obvious; a default only backstops its own provider.
#   * rate_class  — 'cost' and 'list' are separate price lists (list rows feed
#                   the BYO transparency estimate, services/list_pricing.py) and
#                   deliberately have no defaults. Mixing them would compare a
#                   cost default against a list row.
#   * cost_model  — the unit discriminator. per_request ($/call) is obviously
#                   not comparable to per_token ($/1M); less obviously, per_token
#                   (text), per_second (audio) and per_character (speech) share
#                   the SAME $/1M arithmetic but different units, so grouping on
#                   cost_model keeps an ASR $100/1M-second rate from being
#                   compared against a text $2.50/1M-token default. This replaced
#                   a hand-maintained exemption list (see
#                   TestUnitFamiliesPartitionTheCeiling). It also keeps the
#                   per-request-only providers (kag-graph, karma, hirag,
#                   webscraper) and the self_hosted_amortized ones (gemma,
#                   mlx_inference) out entirely — default + no per-model row, so
#                   their group is skipped.
#   * service_key — a service_key-scoped row (e.g. openai/gpt-4o @ w2v-vlm) is
#                   matched on rung 1 by that service only and never competes
#                   with the NULL-service_key provider-default.
_GroupKey = tuple[str, str, str, str | None]


def _group_key(row: dict[str, Any]) -> _GroupKey:
    return (
        row["provider"],
        row.get("rate_class", "cost"),
        row["cost_model"],
        row.get("service_key"),
    )


def _grouped_rows() -> dict[_GroupKey, list[dict[str, Any]]]:
    _eff, rows = _load_yaml()
    groups: dict[_GroupKey, list[dict[str, Any]]] = defaultdict(list)
    for row in rows:
        groups[_group_key(row)].append(row)
    return groups


@pytest.mark.unit
class TestProviderDefaultIsCeiling:
    """A provider-default must be >= every per-model row it could shadow."""

    def test_provider_default_is_at_least_as_expensive_as_every_per_model_row(self) -> None:
        violations: list[str] = []

        for key, rows in _grouped_rows().items():
            provider, rate_class, cost_model, service_key = key
            defaults = [r for r in rows if r.get("model") is None]
            per_model = [r for r in rows if r.get("model") is not None]
            # Only groups that contain BOTH shapes are constrained. A group with
            # a default and no per-model rows has nothing to be a ceiling over.
            if not defaults or not per_model:
                continue

            for default in defaults:
                for row in per_model:
                    for axis in _RATE_AXES:
                        default_rate = default.get(axis)
                        model_rate = row.get(axis)
                        # Compare only where BOTH sides declare the axis. A NULL
                        # on either side is "not priced on this axis", not zero.
                        if default_rate is None or model_rate is None:
                            continue
                        if Decimal(str(default_rate)) < Decimal(str(model_rate)):
                            violations.append(
                                f"{provider} (rate_class={rate_class}, "
                                f"cost_model={cost_model}, service_key={service_key!r}): "
                                f"provider-default {axis}={default_rate} is CHEAPER than "
                                f"model={row['model']!r} {axis}={model_rate}"
                            )

        assert not violations, (
            "provider-default rows that UNDER-price a per-model row — an unseeded "
            "model id for these providers bills below a model we already know is "
            "more expensive, and that revenue is unrecoverable (over-billing "
            "self-corrects when the id is seeded; under-billing does not):\n  "
            + "\n  ".join(violations)
        )

    def test_deepseek_default_matches_v4_pro(self) -> None:
        """Regression pin: the deepseek default IS the deepseek-v4-pro rate.

        Before deepseek-v4-pro got its own row, both the BYO preset and the
        router's tier-medium alias fell back to a deepseek-chat-rate default and
        UNDER-billed v4-pro by ~3x. The default must track the priciest seeded
        DeepSeek model exactly — a generic ">=" assertion would still pass if
        someone dropped the default back to flash rates *and* deleted v4-pro.
        """
        _eff, rows = _load_yaml()
        default = _row_for(rows, "deepseek", None)
        v4_pro = _row_for(rows, "deepseek", "deepseek-v4-pro")

        for axis in ("input_per_1m_usd", "output_per_1m_usd"):
            assert Decimal(str(default[axis])) == Decimal(str(v4_pro[axis])), (
                f"deepseek provider-default {axis}={default[axis]} != "
                f"deepseek-v4-pro {axis}={v4_pro[axis]}"
            )
        # Belt and braces: the published figures, so a matched-but-wrong pair fails.
        assert Decimal(str(default["input_per_1m_usd"])) == Decimal("0.435")
        assert Decimal(str(default["output_per_1m_usd"])) == Decimal("0.87")


@pytest.mark.unit
class TestUnitFamiliesPartitionTheCeiling:
    """The ceiling groups by cost_model, so units never cross-compare.

    This replaces the old ``_KNOWN_UNIT_MISMATCH_EXEMPTIONS`` escape hatch.
    Previously every ASR/TTS rate lived in the per_token column, so an audio
    per-SECOND rate ($100/1M sec) shared a group with the openai text default
    ($2.50/1M tok) and had to be hand-exempted from the ceiling check. Those
    rows are now cost_model=per_second / per_character, which is already part of
    ``_group_key``, so they land in their own group and the exemption list is
    gone. This test pins the property that made deleting it safe: no group mixes
    an audio-unit rate with a token-unit rate.
    """

    def test_no_group_mixes_token_and_non_token_units(self) -> None:
        # Every group is keyed on cost_model, so by construction a group holds a
        # single unit family. Assert it directly so a future edit that drops a
        # per_second/per_character label back to per_token (re-mixing the units)
        # fails here instead of silently under-comparing.
        _eff, rows = _load_yaml()
        by_provider_family: dict[tuple[str, str], set[str]] = defaultdict(set)
        for row in rows:
            by_provider_family[(row["provider"], row["cost_model"])].add(
                row.get("model") or "<default>"
            )

        # openai is the canary: it is the only provider that fronts text tokens,
        # audio seconds, characters AND flat-rate images. After reclassification
        # each must be its own cost_model.
        openai_families = {fam for (prov, fam) in by_provider_family if prov == "openai"}
        assert {"per_token", "per_second", "per_character", "per_request"} <= openai_families, (
            "openai audio/character rows must carry per_second/per_character "
            f"cost_models, not per_token; found families {sorted(openai_families)}"
        )

    def test_asr_rows_are_per_second_not_per_token(self) -> None:
        # Spot-pin the rows that used to need exemptions: they must now be
        # per_second (audio) or per_character (speech), so the resolver's
        # mixed-family refusal and the ceiling grouping both see them correctly.
        _eff, rows = _load_yaml()
        by_key = {(r["provider"], r.get("model"), r.get("rate_class", "cost")): r for r in rows}
        expect = {
            ("openai", "whisper-1"): "per_second",
            ("openai", "gpt-4o-transcribe"): "per_second",
            ("openai", "gpt-4o-mini-transcribe"): "per_second",
            ("openai", "tts-1"): "per_character",
            ("openai", "tts-1-hd"): "per_character",
            ("openai", "gpt-4o-mini-tts"): "per_character",
        }
        for (provider, model), want in expect.items():
            for rate_class in ("cost", "list"):
                row = by_key.get((provider, model, rate_class))
                if row is None:
                    continue
                assert row["cost_model"] == want, (
                    f"{provider}/{model} ({rate_class}) must be cost_model={want}, "
                    f"got {row['cost_model']} — an audio/speech rate in the token "
                    "column re-opens the 40x under-bill this module guards against."
                )


@pytest.mark.unit
class TestCacheRates:
    """DeepSeek publishes a cache-HIT input rate far below the cache-MISS rate.

    ``services/pricing.py`` falls back to the input rate when
    ``cache_read_per_1m_usd`` is NULL, so an unseeded cache rate silently bills
    every cached token at the full miss rate. Seeding it puts the real figure on
    record ahead of the emitter sending cache_hit_tokens.
    """

    @pytest.mark.parametrize(
        ("model", "expected_cache_read"),
        [
            ("deepseek-v4-pro", Decimal("0.003625")),
            ("deepseek-v4-flash", Decimal("0.0028")),
        ],
    )
    def test_deepseek_cache_read_priced_below_input(
        self, model: str, expected_cache_read: Decimal
    ) -> None:
        _eff, rows = _load_yaml()
        row = _row_for(rows, "deepseek", model)

        cache_read = row.get("cache_read_per_1m_usd")
        assert cache_read is not None, (
            f"deepseek/{model} has no cache_read_per_1m_usd — services/pricing.py "
            "then bills cache-hit tokens at the full cache-MISS input rate."
        )
        cache_read_dec = Decimal(str(cache_read))
        in_rate = Decimal(str(row["input_per_1m_usd"]))

        assert cache_read_dec == expected_cache_read, (
            f"deepseek/{model}: cache_read_per_1m_usd={cache_read_dec} != DeepSeek's "
            f"published cache-hit input rate {expected_cache_read}"
        )
        assert cache_read_dec < in_rate, (
            f"deepseek/{model}: cache_read_per_1m_usd={cache_read_dec} is not below "
            f"input_per_1m_usd={in_rate} — a cache HIT must never cost more than a miss."
        )
