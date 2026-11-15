"""WHO FUNDS a cost event: ``local_gpu`` / ``platform_external`` / ``tenant_byo``.

``billing_cost_events.funding`` (alembic 0053) answers one question the ledger
could not answer in one column before: whose money (or whose hardware) paid for
this call?

* ``tenant_byo``        -- the tenant brought its own provider key; the vendor
                           bills the tenant directly.
* ``platform_external`` -- an external vendor (OpenAI, Anthropic, ...) billed the
                           PLATFORM: cash left the building on our key.
* ``local_gpu``         -- self-hosted platform compute (pgx vLLM, the mlx
                           fleet, kokoro, mflux, the in-house services). No
                           vendor invoice; the cost is our own hardware.

PRECEDENCE (first rule that answers wins)
-----------------------------------------
1. ``usage_extra.byo`` true           -> ``tenant_byo``
2. ``usage_extra.owner_provided`` true -> ``platform_external``
3. the provider CONTRACT basis -- the resolved ``provider_pricing.cost_basis``
   (alembic 0022), else the emitter's own valid ``cost_basis``:
   ``cash`` -> ``platform_external``, ``internal_transfer`` -> ``local_gpu``.
   ⛔ NOT ``PRICING_MISS_COST_BASIS``: that is a backstop written on a pricing
   MISS, i.e. "nothing says this was cash", which is not evidence of local.
4. ``cost_model == 'self_hosted_amortized'`` -> ``local_gpu``
5. the provider NAME: a known self-hosted provider (incl. any ``mlx*``) ->
   ``local_gpu``; a known external vendor -> ``platform_external``.
6. otherwise ``None`` -- stored NULL and counted on
   ``billing_funding_unknown_total{service_key}``. Never guessed.

An emitter that sends ``funding`` explicitly is honoured verbatim when the value
is in :data:`FUNDING_VALUES`. An explicit value OUTSIDE the set is NOT re-derived:
it is stored NULL and counted, so a broken emitter is visible rather than
silently papered over by the derivation.

⚠️ The SQL twin of this function is the backfill in
``alembic/versions/0053_cost_funding_usage_daily.py``; ``tests/test_funding.py``
runs both over the same inputs and asserts they agree.
"""

from __future__ import annotations

from collections.abc import Mapping
from typing import Any

LOCAL_GPU = "local_gpu"
PLATFORM_EXTERNAL = "platform_external"
TENANT_BYO = "tenant_byo"

FUNDING_VALUES: frozenset[str] = frozenset({LOCAL_GPU, PLATFORM_EXTERNAL, TENANT_BYO})

#: What the rollup stores when the event's funding is NULL (the rollup's unique
#: key cannot hold NULL as one value); the API reports it verbatim.
FUNDING_UNKNOWN = "unknown"

#: Self-hosted providers, by name. Only consulted on a pricing miss (rule 5):
#: every one of these has a ``provider_pricing`` row with ``cost_basis =
#: internal_transfer``, which rule 3 reads first. Any ``mlx*`` name also matches.
LOCAL_PROVIDERS: frozenset[str] = frozenset(
    {
        "pgx-local",
        "vllm",
        "vlm_local",
        "mlx_inference",
        "gemma",
        "kokoro",
        "kitten",
        "mflux",
        "webscraper",
        "kag",
        "kag-graph",
        "karma",
        "hirag",
        "html-render",
        "video-analyzer",
        "video-compiler",
    }
)

#: External vendors that invoice the platform, by name (rule 5, pricing miss only).
EXTERNAL_PROVIDERS: frozenset[str] = frozenset(
    {
        "anthropic",
        "claude",
        "openai",
        "google",
        "gemini",
        "cohere",
        "deepgram",
        "deepseek",
        "elevenlabs",
        "groq",
        "xai",
        "mistral",
        "openrouter",
        "together",
        "fireworks",
        "perplexity",
    }
)

_BASIS_TO_FUNDING: Mapping[str, str] = {
    "cash": PLATFORM_EXTERNAL,
    "internal_transfer": LOCAL_GPU,
}


def _flag(usage_extra: Mapping[str, Any] | None, key: str) -> bool:
    """Same truth test as the rollups' ``usage_extra[key].astext == 'true'``."""
    if not usage_extra:
        return False
    return str(usage_extra.get(key)).lower() == "true"


def parse_wire_funding(raw: Any) -> tuple[bool, str | None]:
    """``(present, value)`` for the event's ``funding`` field.

    ``present`` is False for an absent / empty / null field (derive it).
    ``present`` is True with ``value=None`` for a value outside the set (store
    NULL, count, do NOT derive).
    """
    if raw is None:
        return False, None
    s = str(raw).strip()
    if not s:
        return False, None
    return True, (s if s in FUNDING_VALUES else None)


def derive_funding(
    *,
    usage_extra: Mapping[str, Any] | None,
    provider: str | None,
    contract_basis: str | None,
    cost_model: str | None,
) -> str | None:
    """Derive funding for an event that did not assert one. See the module docstring."""
    if _flag(usage_extra, "byo"):
        return TENANT_BYO
    if _flag(usage_extra, "owner_provided"):
        return PLATFORM_EXTERNAL
    if contract_basis is not None and contract_basis in _BASIS_TO_FUNDING:
        return _BASIS_TO_FUNDING[contract_basis]
    if cost_model == "self_hosted_amortized":
        return LOCAL_GPU
    p = (provider or "").strip().lower()
    if p in LOCAL_PROVIDERS or p.startswith("mlx"):
        return LOCAL_GPU
    if p in EXTERNAL_PROVIDERS:
        return PLATFORM_EXTERNAL
    return None
