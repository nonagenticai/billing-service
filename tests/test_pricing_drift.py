"""Drift guard: every router-servable CONCRETE model must carry its own pricing
row, so it can never silently mis-bill off the provider-default (rung 4).

This is the mirror image of ``test_pricing_coverage``. That file proves a
provider-DEFAULT exists so an unseeded id never prices NULL. This one guards the
OTHER failure mode: a single-family provider ALWAYS resolves an unknown id — to
its default — and when that default rate differs from the model's true rate the
money silently leaks. Real cases this locks down:

* ``claude/claude-haiku-4-5-20251001`` → hits the claude Opus default → ~18x over.
* ``inference_router/deepseek-v4-{flash,pro}`` → hit the $10/$25 default → 35-70x.

Mixed-family providers (openai) are EXEMPT: their default is refused as a loud
``billing_pricing_miss_total`` (see test_pricing_coverage), not a silent misprice.

The servable list is a hand-synced snapshot; its sources and re-sync recipe are
in the header of ``tests/fixtures/router_servable_models.yaml``.
"""

from __future__ import annotations

from datetime import UTC, datetime
from pathlib import Path
from typing import Any

import pytest
import yaml
from sqlalchemy import select

from models.db import ProviderPricing
from services import pricing as pricing_svc
from tests.test_initial_pricing_seed import _load_yaml, _seed_yaml_rows
from tests.test_pricing_coverage import _cost_rows, _mixed_family_providers

_FIXTURE = Path(__file__).resolve().parent / "fixtures" / "router_servable_models.yaml"

# After every effective_from in the seed, so every seeded row is live.
_TS = datetime(2026, 7, 19, 12, 0, tzinfo=UTC)

# The known over-bill pairs — the snapshot must always contain these so a
# well-meaning edit that empties the fixture cannot make the guard vacuous.
_MUST_CONTAIN = (
    ("claude", "claude-haiku-4-5-20251001"),
    ("inference_router", "deepseek-v4-flash"),
    ("inference_router", "deepseek-v4-pro"),
)


def _load_router_servable_models() -> list[tuple[str, str]]:
    with _FIXTURE.open("r", encoding="utf-8") as fh:
        doc = yaml.safe_load(fh)
    entries = doc.get("router_servable_models")
    assert entries, f"{_FIXTURE.name} has no `router_servable_models:` list — snapshot is empty."
    pairs = [(e["provider"], e["model"]) for e in entries]
    assert len(pairs) == len(set(pairs)), (
        f"duplicate entries in {_FIXTURE.name}: a dup can mask a missing row — {pairs}"
    )
    return pairs


@pytest.mark.unit
class TestRouterServableModelDrift:
    def test_snapshot_contains_the_known_over_bill_pairs(self) -> None:
        """Guard the guard: emptying or trimming the snapshot must not silently
        disarm this test."""
        pairs = set(_load_router_servable_models())
        missing = [f"{p}/{m}" for (p, m) in _MUST_CONTAIN if (p, m) not in pairs]
        assert not missing, f"router-servable snapshot is missing known bug pairs: {missing}"

    def test_every_single_family_servable_model_has_its_own_row(self) -> None:
        """Each servable concrete (provider, model) under a SINGLE-FAMILY provider
        must have an explicit open cost row — else it resolves to the provider-
        default and mis-bills. Fails against the pre-fix seed (the 3 rows absent).
        """
        _eff, rows = _load_yaml()
        seeded = {(r["provider"], r.get("model")) for r in _cost_rows(rows)}
        mixed = _mixed_family_providers(rows)

        missing: list[str] = []
        for provider, model in _load_router_servable_models():
            if provider in mixed:
                continue  # loud-miss protects mixed-family; no silent drift
            if (provider, model) not in seeded:
                missing.append(f"{provider}/{model}")

        assert not missing, (
            "router-servable models with NO explicit (provider, model) pricing row — "
            f"they silently mis-bill off the provider-default: {missing}. Add a per-model "
            "row to scripts/initial_pricing.yaml (correct rate source: the model's own "
            "deepseek/claude row or the vendor price list)."
        )

    async def test_servable_models_resolve_to_their_own_row_not_the_default(
        self, db_session: Any
    ) -> None:
        """The strong form: seed the real YAML and resolve each servable model
        through the real pricing service. The WINNING row must be the model's own
        (model IS NOT NULL) row, not the provider-default — a row can exist in
        YAML yet be shadowed by a resolver bug or an effective_from that hasn't
        opened. This is what actually proves the money doesn't leak.
        """
        eff_from, rows = _load_yaml()
        await _seed_yaml_rows(db_session, rows, eff_from)
        mixed = _mixed_family_providers(rows)

        leaked: list[str] = []
        for provider, model in _load_router_servable_models():
            if provider in mixed:
                continue
            _cost, pricing_id = await pricing_svc.compute_cost_usd(
                db_session,
                provider=provider,
                served_model=model,
                input_tokens=1_000,
                output_tokens=1_000,
                ts=_TS,
            )
            if pricing_id is None:
                leaked.append(f"{provider}/{model} -> MISS (no row resolved)")
                continue
            row = (
                await db_session.execute(
                    select(ProviderPricing).where(ProviderPricing.id == pricing_id)
                )
            ).scalar_one()
            if row.model is None:
                leaked.append(f"{provider}/{model} -> provider-default row (model=NULL)")

        assert not leaked, (
            "servable models resolving to the provider-default instead of their own "
            f"row (silent misbill): {leaked}"
        )
