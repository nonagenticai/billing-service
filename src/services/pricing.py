"""Centralized pricing — metering's authoritative cost source.

Looks up `provider_pricing` rows by (provider, served_model) effective at the
event timestamp. Falls back to the provider-default row (model IS NULL) when
no per-model row matches.

`cost_model` values:
- `per_token` / `per_second` / `per_character` / `per_page`: input + output +
  cache priced via the per-1M rates. These four share IDENTICAL arithmetic — the
  value only records WHICH UNIT the emitter packed into the token counts (LLM
  tokens, audio-seconds, characters, or document pages), so the ceiling invariant and the resolver can
  tell an audio-second rate ($100/1M sec) apart from a text-token rate
  ($2.50/1M tok) that would otherwise share a column. NULL cache rates fall back
  to `input_per_1m_usd`. ⛔ Identical arithmetic means the unit only constrains
  anything if something CHECKS it: `_cost_from_row` refuses a quantity whose
  `quantity_unit` contradicts the resolved row's family — see
  `_unit_contradicts_cost_model` and `_TOKEN_FAMILY_UNIT_BY_COST_MODEL`.
- `per_request`: flat `per_request_usd` regardless of token counts.
- `self_hosted_amortized`: returns Decimal("0") per event. Amortization across
  the monthly fixed cost happens at billing-report time (not in this phase).
"""

from __future__ import annotations

from datetime import UTC, datetime
from decimal import ROUND_HALF_UP, Decimal
from typing import Any

import structlog
from sqlalchemy import or_, select
from sqlalchemy.ext.asyncio import AsyncSession

import observability.metrics as observability_metrics
from models.db import ProviderPricing
from models.domain import ProviderPricingCreate

logger = structlog.stdlib.get_logger(__name__)

_QUANT = Decimal("0.00000001")
_MILLION = Decimal("1000000")

# cost_models that share the "rate per 1M units" arithmetic. The value records
# the UNIT (LLM tokens / audio-seconds / characters / pages); the math is
# identical, so an older pod that predates per_second/per_character/per_page
# still prices such a row correctly (see _cost_from_row). per_request and
# self_hosted_amortized are handled separately and are NOT in this set.
#
# per_page (2026-10-18): strata-ingest OCR sends the page count in input_tokens
# with no quantity_unit; the owner prices it per page. A per_token row would
# have computed the same number but stamped those events quantity_unit=tokens.
_TOKEN_FAMILY_COST_MODELS = ("per_token", "per_second", "per_character", "per_page")

# ---------------------------------------------------------------------------
# quantity_unit / cost_basis derivation (alembic 0021)
# ---------------------------------------------------------------------------
#
# Every ``cost_model`` value ``ProviderPricingCreate`` accepts maps to exactly
# one unit — this is a TOTAL function over the six live values, not a
# best-effort guess. 'megapixels' and 'passages' (mflux / mlx-rerank) are in
# ``CostEvent.quantity_unit``'s bounded set for a FUTURE emitter to assert
# explicitly, but neither is reachable from a cost_model today: mflux prices
# flat ``per_request`` (see ``ProviderPricingCreate.cost_model``'s docstring —
# "per_megapixel is deliberately absent"), so deriving from cost_model alone
# would call it 'requests', which is technically what it is billed as but not
# what the operator measured the emitted quantity to mean. See
# :func:`quantity_unit_for_provider`, which checks the PROVIDER NAME first for
# exactly this reason, and only falls back to this table.
QUANTITY_UNIT_BY_COST_MODEL: dict[str, str] = {
    "per_token": "tokens",
    "per_second": "seconds",
    "per_character": "characters",
    "per_page": "pages",
    "per_request": "requests",
    # Today self_hosted_amortized has exactly one user (pgx-local, alembic
    # 0021 decision 4) and pgx-local's own quantity is true LLM tokens. If a
    # future self-hosted provider with a different unit adopts this
    # cost_model, its emitter can still assert ``quantity_unit`` on the wire
    # directly — see services.cost_flusher, which honours an explicit value
    # before ever consulting this table.
    "self_hosted_amortized": "tokens",
}

#: The ONE emitted ``quantity_unit`` each TOKEN-FAMILY ``cost_model`` can
#: legitimately price. DERIVED from :data:`QUANTITY_UNIT_BY_COST_MODEL`
#: restricted to :data:`_TOKEN_FAMILY_COST_MODELS` — never re-typed, so the two
#: tables cannot drift apart.
#:
#: ⛔ THIS IS THE ONLY REASON ``cost_model`` CARRIES A UNIT AT ALL. The three
#: token-family models share IDENTICAL arithmetic (see :func:`_cost_from_row`),
#: so a quantity emitted in SECONDS and priced by a ``per_token`` rate is
#: arithmetically indistinguishable from a correct charge: no NULL, no
#: out-of-range value, no exception — nothing to find afterwards. Until this
#: table existed the unit was decorative, validated nowhere.
#:
#: Measured live: ``provider_pricing`` row 32 (video-analyzer /
#: w2v-video-analyzer, ``per_token``, OPEN) shadows rows 117/118
#: (``per_second``, OPEN) for the only ``service_key`` that emits, and the
#: mispricing is invisible only because every rate on all three rows happens to
#: be ``283.33333333``. The next rate change to either side makes it a silent
#: multi-order-of-magnitude error.
#:
#: ⚠️ ``per_request`` and ``self_hosted_amortized`` are DELIBERATELY ABSENT:
#: neither multiplies quantity by a rate, so a unit disagreement against them
#: cannot mis-charge anything. That absence is load-bearing — it is what keeps
#: mflux (``megapixels``) and mlx-rerank (``passages``) priceable, since both
#: units are in ``CostEvent.quantity_unit``'s bounded set and NEITHER is
#: reachable from any ``cost_model`` (see the module comment above).
_TOKEN_FAMILY_UNIT_BY_COST_MODEL: dict[str, str] = {
    cost_model: QUANTITY_UNIT_BY_COST_MODEL[cost_model] for cost_model in _TOKEN_FAMILY_COST_MODELS
}

#: Provider-name overrides, authoritative over the cost_model table above.
#: This is the operator's own measured mapping (2026-09-04 audit) and is what
#: alembic 0021's historical backfill uses too — kept in agreement by
#: ``tests/test_migration_00XX_*`` rather than re-typed independently.
QUANTITY_UNIT_BY_PROVIDER: dict[str, str] = {
    "pgx-local": "tokens",
    "mlx-nlp": "characters",
    "mlx-embed": "characters",
    "kokoro": "characters",
    "video-compiler": "seconds",
    "mflux": "megapixels",
    "mlx-rerank": "passages",
}

#: ⛔ RETIRED (alembic 0022) — kept only as a historical marker of the defect.
#: ``cost_model`` says HOW WE METER (per token / per character / per second /
#: flat), not WHETHER CASH LEAVES THE BUILDING. Deriving ``cost_basis`` from
#: this table treated every self-hosted per_token/per_character provider
#: (pgx-local, mlx-nlp, ...) as 'cash', which is exactly backwards — measured
#: live 2026-09-04, 15-and-growing mislabelled mlx-nlp rows within minutes of
#: 0021's deploy. ``cost_basis`` is now a PROVIDER CONTRACT fact declared on
#: ``provider_pricing.cost_basis`` (see :func:`compute_cost_usd_with_cost_model`,
#: which returns the resolved row's own ``cost_basis`` instead of consulting
#: this table). Not deleted outright so its history stays greppable; nothing
#: in this codebase calls it any more.
COST_BASIS_BY_COST_MODEL: dict[str, str] = {
    "self_hosted_amortized": "internal_transfer",
}

#: Backstop ``cost_basis`` for a PRICING MISS — no row resolved at all, so
#: there is nothing to say this was a real vendor charge. Under-claims cash
#: spend rather than fabricating one; mirrors the ``provider_pricing.cost_basis``
#: column's own default (alembic 0022) and 0021's identical reasoning for
#: ``billing_cost_events.cost_basis``'s column default.
PRICING_MISS_COST_BASIS = "internal_transfer"


def cost_basis_for_cost_model(cost_model: str | None) -> str:
    """⛔ RETIRED (alembic 0022) — do not call this from the write path.

    Kept only so the defect it caused stays reproducible: see
    ``tests/test_pricing.py::TestComputeCostUsdWithCostModelReturnsCostBasis::
    test_mutation_check_cost_model_based_inference_would_be_wrong_here``, which
    calls this directly to prove the OLD derivation disagrees with the
    resolved pricing row's own ``cost_basis`` for a self-hosted provider.
    ``cost_basis`` is a provider-contract fact, not a function of
    ``cost_model`` — use the ``cost_basis`` returned by
    :func:`compute_cost_usd_with_cost_model` instead.
    """
    return COST_BASIS_BY_COST_MODEL.get(cost_model or "", "cash")


def quantity_unit_for_provider(provider: str | None, cost_model: str | None) -> str | None:
    """The best honest ``quantity_unit`` for one event, or ``None``.

    Provider name wins (it is what was actually measured); the resolved
    pricing row's ``cost_model`` is the fallback for any provider not in the
    measured table. Never invents a value: an unrecognised provider with no
    resolved pricing row returns ``None``.
    """
    return QUANTITY_UNIT_BY_PROVIDER.get(provider or "") or QUANTITY_UNIT_BY_COST_MODEL.get(
        cost_model or ""
    )


def _unit_contradicts_cost_model(*, quantity_unit: str | None, cost_model: str | None) -> bool:
    """True when an emitted ``quantity_unit`` cannot be priced by ``cost_model``.

    The LEGITIMATE pairs, all derived from the tables above rather than re-typed:

      * ``('tokens', 'per_token')``, ``('seconds', 'per_second')``,
        ``('characters', 'per_character')``, ``('pages', 'per_page')`` — the token family, the only place a
        quantity is multiplied by a "per 1M of THAT unit" rate.
      * **ANY unit against ``per_request`` or ``self_hosted_amortized``.**
        Neither consults the quantity (flat fee / ``$0`` by design), so a unit
        disagreement there cannot mis-charge anything and refusing it would
        invent an unbilled row out of a correct charge. This is what keeps
        mflux (``megapixels``) and mlx-rerank (``passages``) priceable.
      * **``(None, anything)`` — an ABSENT unit asserts nothing.**
        ``billing_cost_events.quantity_unit`` is nullable by design (alembic
        0021) and every row predating it is NULL, so absence is not a
        contradiction. ``''`` is treated the same as NULL.
      * ANY unit against an UNRECOGNISED ``cost_model``. We cannot say what such
        a row's rates mean, so we cannot say the unit disagrees with them; the
        pre-existing ``pricing_unknown_cost_model`` warning is the signal for
        that shape, and the token-family fall-through is deliberate (it is what
        lets a pod predating a new unit still compute the same number during a
        rollout — see :func:`_cost_from_row`).

    Everything else contradicts.
    """
    if not quantity_unit:
        return False
    expected = _TOKEN_FAMILY_UNIT_BY_COST_MODEL.get(cost_model or "")
    if expected is None:
        return False
    return quantity_unit != expected


def _q(value: Decimal) -> Decimal:
    """Quantize to 8 decimal places — matches the cost_usd column precision."""
    return value.quantize(_QUANT, rounding=ROUND_HALF_UP)


def _refuse_unpriceable_row(
    row: ProviderPricing,
    *,
    cost_model: str | None,
    reason: str,
    **extra: Any,
) -> None:
    """Log + count a row that cannot honestly price the usage handed to it.

    Reuses the existing ``billing_pricing_row_malformed_total`` metric so
    dashboards and alerts keep working unchanged — what changed is the RETURN
    (a refusal instead of a fabricated ``$0``), not the signal.

    ``reason`` discriminates the shapes in the LOG only: the metric's labels are
    ``(provider, model, cost_model)`` and are deliberately left alone, because
    ``MeteringPricingMiss``-style rules already aggregate on them. ⚠️ That means
    a unit mismatch and an unstated rate are one series in Prometheus — see the
    report: splitting them wants a new counter in ``observability.metrics``,
    which is owned elsewhere right now.

    ``**extra`` rides along on the structured log line (never on the metric), so
    a refusal can name the unit it saw and the unit it expected.
    """
    logger.warning(
        "pricing_row_unpriceable",
        pricing_id=row.id,
        provider=row.provider,
        model=row.model,
        cost_model=cost_model,
        reason=reason,
        **extra,
    )
    observability_metrics.pricing_row_malformed_total.labels(
        provider=row.provider or "unknown",
        model=row.model or "default",
        cost_model=cost_model or "unknown",
    ).inc()


def _cost_from_row(
    row: ProviderPricing,
    *,
    input_tokens: int,
    output_tokens: int,
    cache_creation_input_tokens: int = 0,
    cache_read_input_tokens: int = 0,
    quantity_unit: str | None = None,
) -> Decimal | None:
    """Per-row USD cost for a token/request line, or ``None`` to REFUSE — no DB access.

    Shared by the cost hot path (:func:`compute_cost_usd`) and the read-time
    list-price estimator (:mod:`services.list_pricing`) so the per_token /
    per_request / self_hosted math lives in exactly one place.

    ⛔ A row whose rate was NEVER STATED, asked to price REAL quantity, returns
    ``None`` instead of fabricating ``$0``:

      * ``per_request`` with a NULL ``per_request_usd``;
      * a token-family row with NULL ``input_per_1m_usd`` AND NULL
        ``output_per_1m_usd``, when quantity > 0;
      * a token-family row whose unit CONTRADICTS the emitted ``quantity_unit``,
        when quantity > 0 (see below).

    Those are exactly the shapes ``ProviderPricingCreate`` rejects at the door
    (``test_malformed_row_rejected``), so this aligns the RUNTIME with the
    WRITE-TIME contract for legacy rows already sitting in the live
    ``provider_pricing`` table. Previously ``or Decimal("0")`` coerced them into
    a successful ``$0`` charge which — because :mod:`services.reprice` backfills
    on ``cost_usd IS NULL`` — could never be found or repaired, so the spend was
    unbillable forever. Refusing leaves ``cost_usd`` NULL, this repo's existing
    "priced later" idiom: logged, counted, and alerted on as a ``pricing_miss``.

    Two things are deliberately NOT refusals, because refusing them would invent
    a fake "unbilled" row out of a correct charge:

      * **An explicit zero.** A stated ``0`` is a deliberate free tier and the
        validator accepts it on purpose (``test_zero_rate_row_is_accepted``), so
        it still prices at ``$0``. The refusal tests NULL-ness, never value.
      * **Zero quantity.** A genuinely zero-token event costs ``$0`` whatever the
        rate is, so an unstated rate is not consulted and not reported.

    ``self_hosted_amortized`` likewise still returns ``Decimal("0")`` — its
    per-event cost is zero by design, amortized monthly at report time.

    UNIT ENFORCEMENT (``quantity_unit``)
    ------------------------------------
    ``quantity_unit`` is the unit the EMITTER says it packed into the token
    counts (``billing_cost_events.quantity_unit``). Until now nothing in this
    function referenced it: the three token-family ``cost_model`` values share
    identical arithmetic, so seconds priced by a ``per_token`` rate produced a
    number indistinguishable from a correct charge. The label was decorative.

    A contradicting unit is now a REFUSAL, on the same terms as an unstated
    rate — see :func:`_unit_contradicts_cost_model` for the exhaustive list of
    legitimate pairs. Three guards keep the strictness pointing the right way:

      * only the TOKEN FAMILY is checked. ``per_request`` /
        ``self_hosted_amortized`` never multiply quantity by a rate, so mflux
        (``megapixels``) and mlx-rerank (``passages``) keep pricing untouched;
      * an ABSENT (NULL/empty) unit asserts nothing and never refuses — the
        column is nullable and historic rows are NULL;
      * ZERO quantity never refuses, mirroring the rate rules above: a
        zero-quantity event costs ``$0`` in any unit.

    When the caller passes no ``quantity_unit``, the PROVIDER's own measured
    unit (:data:`QUANTITY_UNIT_BY_PROVIDER`, the operator's 2026-09-04 audit and
    alembic 0021's backfill twin) is used instead. ⭐ That fallback is what gives
    the flush hot path coverage without its caller changing: every provider in
    that table agrees with its ``scripts/initial_pricing.yaml`` ``cost_model``
    today (pgx-local/per_token, kokoro|mlx-nlp|mlx-embed/per_character,
    video-compiler/per_second, mflux|mlx-rerank/per_request), so the fallback
    refuses nothing that is currently priced correctly — it fires only when a
    row in the WRONG family shadows one of them. ⚠️ It is deliberately NOT
    extended with new providers here: that dict is the Python twin of alembic
    0021's SQL ``CASE`` and adding an entry would desync the twin.
    """
    cost_model = row.cost_model

    if cost_model == "per_request":
        # Unstated price => refuse. `or Decimal("0")` turned a seeding omission
        # into a permanent $0 charge reprice could never find. An explicit 0 is a
        # deliberate free tier and still prices at 0.
        if row.per_request_usd is None:
            _refuse_unpriceable_row(row, cost_model=cost_model, reason="per_request_usd_unstated")
            return None
        return _q(Decimal(row.per_request_usd))

    if cost_model == "self_hosted_amortized":
        # Per-event cost is zero; monthly fixed cost is amortized at report time.
        return Decimal("0")

    # per_token / per_second / per_character all share this arithmetic — the
    # emitter packs its unit's count (tokens / audio-seconds / characters) into
    # input_tokens/output_tokens and the rate is "per 1M of that unit". Listing
    # them explicitly (rather than treating everything-not-per_request as
    # per_token) is what closes the old silent fall-through: a genuinely unknown
    # cost_model now logs rather than being priced as tokens without a trace.
    # The math stays identical on purpose so an older pod that predates
    # per_second/per_character reads such a row and computes the same number —
    # there is no mispricing window during a rollout, only the write path is
    # version-sensitive (see ProviderPricingCreate).
    if cost_model not in _TOKEN_FAMILY_COST_MODELS:
        logger.warning(
            "pricing_unknown_cost_model",
            pricing_id=row.id,
            provider=row.provider,
            model=row.model,
            cost_model=cost_model,
        )
    # Quantity is hoisted above the rate arithmetic because the refusal below
    # needs it: an unstated rate only matters if there was something to charge
    # for. (The all-zero observability check further down reuses it.)
    total_tokens = (
        input_tokens + output_tokens + cache_creation_input_tokens + cache_read_input_tokens
    )

    # ⛔ WRONG UNIT => REFUSE, BEFORE any rate is consulted. Checked first on
    # purpose: if the quantity is in the wrong unit for this row then the rates
    # are the wrong rates, and reporting "token_rates_unstated" would name a
    # symptom instead of the cause. Evaluated only when quantity > 0 — the same
    # rule the rate refusals use, for the same reason.
    effective_unit = quantity_unit or QUANTITY_UNIT_BY_PROVIDER.get(row.provider or "")
    if total_tokens > 0 and _unit_contradicts_cost_model(
        quantity_unit=effective_unit, cost_model=cost_model
    ):
        _refuse_unpriceable_row(
            row,
            cost_model=cost_model,
            reason="quantity_unit_contradicts_cost_model",
            quantity_unit=effective_unit,
            expected_quantity_unit=_TOKEN_FAMILY_UNIT_BY_COST_MODEL.get(cost_model or ""),
            # Which table produced the unit — an event-asserted unit is the
            # emitter's own measurement; the provider table is the operator's.
            quantity_unit_source="event" if quantity_unit else "provider_measured",
            total_tokens=total_tokens,
        )
        return None

    # Both primary rates UNSTATED while real quantity was billed => refuse. The
    # token-family mirror of the per_request case above, and the same shape
    # ProviderPricingCreate rejects. Tests NULL-ness, never value: a row stating
    # 0 is a legitimate free tier (see the docstring).
    if total_tokens > 0 and row.input_per_1m_usd is None and row.output_per_1m_usd is None:
        _refuse_unpriceable_row(row, cost_model=cost_model, reason="token_rates_unstated")
        return None

    in_rate = row.input_per_1m_usd or Decimal("0")
    out_rate = row.output_per_1m_usd or Decimal("0")
    cache_create_rate = (
        row.cache_creation_per_1m_usd if row.cache_creation_per_1m_usd is not None else in_rate
    )
    cache_read_rate = (
        row.cache_read_per_1m_usd if row.cache_read_per_1m_usd is not None else in_rate
    )

    input_cost = (Decimal(input_tokens) * Decimal(in_rate)) / _MILLION
    output_cost = (Decimal(output_tokens) * Decimal(out_rate)) / _MILLION
    cache_create_cost = (
        Decimal(cache_creation_input_tokens) * Decimal(cache_create_rate)
    ) / _MILLION
    cache_read_cost = (Decimal(cache_read_input_tokens) * Decimal(cache_read_rate)) / _MILLION

    # A per_token row whose entire effective rate set is zero prices real usage
    # at $0.00 and looks like a successful charge — no NULL cost_usd, no
    # pricing_miss. ProviderPricingCreate now rejects such rows at the door, but
    # the validator cannot retroactively catch the malformed rows already sitting
    # in the live provider_pricing table (they were inserted before it existed,
    # and some predate the API entirely). This runtime signal is therefore the
    # part that actually protects revenue today; the validator only stops new ones.
    #
    # Guards deliberately narrow to avoid false positives:
    #   * only fires when tokens were actually billed — a genuinely zero-token
    #     event priced at $0 is correct, not malformed;
    #   * only when EVERY effective rate is zero — a legitimately free axis
    #     (e.g. a promo with cache_read at $0) must not trip this;
    #   * self_hosted_amortized returns above and never reaches here — its $0
    #     per-event cost is correct by design (amortized monthly at report time).
    all_rates_zero = (
        Decimal(in_rate) == 0
        and Decimal(out_rate) == 0
        and Decimal(cache_create_rate) == 0
        and Decimal(cache_read_rate) == 0
    )
    if total_tokens > 0 and all_rates_zero:
        logger.warning(
            "pricing_row_malformed",
            pricing_id=row.id,
            provider=row.provider,
            model=row.model,
            cost_model=cost_model,
            total_tokens=total_tokens,
        )
        observability_metrics.pricing_row_malformed_total.labels(
            provider=row.provider or "unknown",
            model=row.model or "default",
            cost_model=cost_model or "unknown",
        ).inc()

    return _q(input_cost + output_cost + cache_create_cost + cache_read_cost)


async def _resolve_pricing_row(
    session: AsyncSession,
    *,
    provider: str,
    served_model: str | None,
    service_key: str | None = None,
    rate_class: str = "cost",
    ts: datetime,
) -> ProviderPricing | None:
    """Return the effective ProviderPricing row at `ts`.

    Match precedence (T1.4 — service_key adds two new rungs):
      1. (provider, model, service_key) — exact match on all three
      2. (provider, model, NULL)        — model-specific default for any service
      3. (provider, NULL, service_key)  — provider-default for this service_key
      4. (provider, NULL, NULL)         — provider-default for anything

    Walk the candidates in this order and return the first match. When the
    caller passes ``service_key=None``, rungs 1 and 3 are unreachable; the
    fallthrough to rungs 2 and 4 preserves the pre-T1.4 behavior exactly.
    """
    stmt = (
        select(ProviderPricing)
        .where(
            ProviderPricing.rate_class == rate_class,
            ProviderPricing.provider == provider,
            ProviderPricing.effective_from <= ts,
            or_(
                ProviderPricing.effective_until.is_(None),
                ProviderPricing.effective_until > ts,
            ),
        )
        .order_by(ProviderPricing.effective_from.desc())
    )
    result = await session.execute(stmt)
    rows = list(result.scalars().all())
    if not rows:
        return None

    if served_model is not None and service_key is not None:
        for row in rows:
            if row.model == served_model and row.service_key == service_key:
                return row

    if served_model is not None:
        for row in rows:
            if row.model == served_model and row.service_key is None:
                return row

    if service_key is not None:
        for row in rows:
            if row.model is None and row.service_key == service_key:
                return row

    # Rung 4 — provider-default (model IS NULL, service_key IS NULL).
    #
    # provider_pricing has no unit column: a rate is "per 1M X" where X is tokens,
    # audio-seconds, or characters depending on the cost_model family. When a
    # provider carries rows in MORE THAN ONE token-family (openai: per_token text +
    # per_second ASR + per_character TTS + per_request image), an unknown
    # served_model that reaches this rung cannot be safely priced by the default —
    # the default belongs to one family and the event may belong to another.
    # Historically this silently billed openai ASR seconds at the text token rate
    # (40x under) and unseeded openai embeddings at 500x over.
    #
    # Refuse instead: return None so the caller records a loud pricing_miss (NULL
    # cost + billing_pricing_miss_total + the MeteringPricingMiss alert), which is
    # repairable via POST /api/admin/reprice once the correct per-model row is
    # seeded. A silent misprice is neither loud nor repairable. Single-family
    # providers are unaffected — their default is unambiguous, so this is a no-op
    # for every provider except a genuinely mixed-vocabulary one.
    default_row = next((row for row in rows if row.model is None and row.service_key is None), None)
    if default_row is None:
        return None

    distinct_families = {row.cost_model for row in rows}
    if len(distinct_families) > 1:
        logger.warning(
            "pricing_default_refused_mixed_family",
            provider=provider,
            served_model=served_model,
            service_key=service_key,
            rate_class=rate_class,
            families=sorted(distinct_families),
        )
        return None

    return default_row


async def compute_cost_usd(
    session: AsyncSession,
    *,
    provider: str,
    served_model: str | None,
    input_tokens: int,
    output_tokens: int,
    cache_creation_input_tokens: int = 0,
    cache_read_input_tokens: int = 0,
    service_key: str | None = None,
    rate_class: str = "cost",
    quantity_unit: str | None = None,
    ts: datetime,
) -> tuple[Decimal | None, int | None]:
    """Compute the authoritative USD cost for a cost event.

    Returns ``(cost_usd, pricing_id)``. Both ``None`` when no pricing row
    matches at the given timestamp; in that case a structured ``pricing_miss``
    log is emitted so operators can spot uncovered providers/models.

    ``service_key`` is the optional T1.4 dimension — see
    :func:`_resolve_pricing_row` for the 4-rung precedence. ``rate_class``
    stays ``'cost'`` on the authoritative hot path; the read-time BYO estimator
    passes ``'list'`` to resolve the public provider rate independently.

    Thin wrapper over :func:`compute_cost_usd_with_cost_model` that drops the
    third and fourth elements — kept byte-for-byte behaviourally identical
    (same log line, same metric, same arguments) so every existing
    caller/test is unaffected.
    """
    cost, pricing_id, _cost_model, _cost_basis = await compute_cost_usd_with_cost_model(
        session,
        provider=provider,
        served_model=served_model,
        input_tokens=input_tokens,
        output_tokens=output_tokens,
        cache_creation_input_tokens=cache_creation_input_tokens,
        cache_read_input_tokens=cache_read_input_tokens,
        service_key=service_key,
        rate_class=rate_class,
        quantity_unit=quantity_unit,
        ts=ts,
    )
    return cost, pricing_id


async def compute_cost_usd_with_cost_model(
    session: AsyncSession,
    *,
    provider: str,
    served_model: str | None,
    input_tokens: int,
    output_tokens: int,
    cache_creation_input_tokens: int = 0,
    cache_read_input_tokens: int = 0,
    service_key: str | None = None,
    rate_class: str = "cost",
    quantity_unit: str | None = None,
    ts: datetime,
) -> tuple[Decimal | None, int | None, str | None, str | None]:
    """:func:`compute_cost_usd`, plus the resolved row's ``cost_model`` and ``cost_basis``.

    Added for ``services.cost_flusher`` (alembic 0021 — ``quantity_unit``;
    alembic 0022 — ``cost_basis``), which needs the pricing row's own declared
    fields to derive both without a second round trip. ``cost_basis`` is the
    resolved row's PROVIDER-CONTRACT fact (alembic 0022) — it is NOT derived
    from ``cost_model`` here, deliberately: ``cost_model`` says how a rate
    metering, not whether cash left the building (see the module-level
    ``COST_BASIS_BY_COST_MODEL`` comment for why that derivation was wrong).
    All four are ``None`` on a pricing miss — same as ``compute_cost_usd`` —
    because there is nothing to derive from; the caller applies its own
    pricing-miss backstop (see :data:`PRICING_MISS_COST_BASIS`).

    ``quantity_unit`` is the emitter's asserted unit for THIS event, forwarded to
    :func:`_cost_from_row` so a quantity can be refused when it contradicts the
    resolved row's ``cost_model``. Optional and defaulted to ``None`` so every
    pre-existing caller is byte-for-byte unaffected; when it is not supplied the
    provider-measured fallback still applies (see :func:`_cost_from_row`).

    ⛔ A MISS RETURNS ``None``, NEVER ``Decimal("0")``, and that is the contract
    :mod:`services.reprice` relies on (its backfill selects ``cost_usd IS
    NULL``). ⚠️ Absence does NOT survive the whole pipeline: the DAILY rollup
    folds these NULLs with ``coalesce(sum(cost_usd), 0)`` into
    ``billing_cost_daily.total_cost_usd``, which is ``nullable=False`` — so a
    provider with no pricing row at all lands in the rollup as a hard
    ``0.000000`` beside its real token count. That is outside this module; see
    the PR report.
    """
    row = await _resolve_pricing_row(
        session,
        provider=provider,
        served_model=served_model,
        service_key=service_key,
        rate_class=rate_class,
        ts=ts,
    )
    if row is None:
        logger.warning(
            "pricing_miss",
            provider=provider,
            served_model=served_model,
            rate_class=rate_class,
            ts=ts.isoformat(),
        )
        observability_metrics.pricing_miss_total.labels(
            provider=provider or "unknown",
            model=served_model or "unknown",
        ).inc()
        return None, None, None, None

    return (
        _cost_from_row(
            row,
            input_tokens=input_tokens,
            output_tokens=output_tokens,
            cache_creation_input_tokens=cache_creation_input_tokens,
            cache_read_input_tokens=cache_read_input_tokens,
            quantity_unit=quantity_unit,
        ),
        row.id,
        row.cost_model,
        row.cost_basis,
    )


async def list_current_pricing(session: AsyncSession) -> list[ProviderPricing]:
    """All currently-effective rows (effective_until IS NULL).

    Sorted by ``(provider, model NULLS FIRST)`` so the provider-default row
    appears before per-model rows for the same provider.
    """
    stmt = select(ProviderPricing).where(ProviderPricing.effective_until.is_(None))
    result = await session.execute(stmt)
    rows = list(result.scalars().all())
    rows.sort(key=lambda r: (r.provider, r.model is not None, r.model or ""))
    return rows


async def list_pricing_history(
    session: AsyncSession, *, provider: str, model: str | None = None
) -> list[ProviderPricing]:
    """Full history for a (provider, model) pair, newest first."""
    stmt = select(ProviderPricing).where(ProviderPricing.provider == provider)
    if model is None:
        stmt = stmt.where(ProviderPricing.model.is_(None))
    else:
        stmt = stmt.where(ProviderPricing.model == model)
    stmt = stmt.order_by(ProviderPricing.effective_from.desc())
    result = await session.execute(stmt)
    return list(result.scalars().all())


class PricingSupersedeBeforePriorError(RuntimeError):
    """A superseding pricing row began before the open row it would close.

    Carries both instants so the HTTP boundary can report the conflict and a
    caller can correct its ``effective_from`` without re-querying.
    """

    def __init__(
        self,
        *,
        provider: str,
        model: str | None,
        effective_from: datetime,
        prior_effective_from: datetime,
    ) -> None:
        super().__init__(
            f"effective_from {effective_from.isoformat()} precedes the open row it "
            f"would supersede ({prior_effective_from.isoformat()}) for "
            f"provider={provider} model={model}"
        )
        self.provider = provider
        self.model = model
        self.effective_from = effective_from
        self.prior_effective_from = prior_effective_from


async def upsert_pricing(
    session: AsyncSession,
    payload: ProviderPricingCreate,
    *,
    ts: datetime | None = None,
) -> tuple[ProviderPricing, ProviderPricing | None]:
    """Close any open row for the same (provider, model, service_key) and insert a new one.

    The closed (prior) row is returned alongside the new one so callers can
    capture before/after for audit logging.

    T1.4 — the "open row" lookup now scopes by ``service_key`` too, so
    setting (provider='deepseek', model='deepseek-v4-flash', service_key='kag-graph')
    only closes the prior open row for that exact triple — the
    (provider, model, NULL) default keeps charging unchanged for everyone else.
    """
    closed_at = ts or datetime.now(UTC)

    # Find any currently-open row for the same
    # (rate_class, provider, model, service_key). Scoping by rate_class keeps a
    # 'list' upsert from closing the open 'cost' row for the same tuple.
    open_stmt = select(ProviderPricing).where(
        ProviderPricing.rate_class == payload.rate_class,
        ProviderPricing.provider == payload.provider,
        ProviderPricing.effective_until.is_(None),
    )
    if payload.model is None:
        open_stmt = open_stmt.where(ProviderPricing.model.is_(None))
    else:
        open_stmt = open_stmt.where(ProviderPricing.model == payload.model)
    if payload.service_key is None:
        open_stmt = open_stmt.where(ProviderPricing.service_key.is_(None))
    else:
        open_stmt = open_stmt.where(ProviderPricing.service_key == payload.service_key)

    open_result = await session.execute(open_stmt)
    prior = open_result.scalar_one_or_none()

    # ⭐ THE NEW ROW'S START IS COMPUTED FIRST, AND IT IS WHAT CLOSES THE PRIOR
    # ROW. Until now this function read the clock twice: the prior row was closed
    # at ``closed_at`` (a server-side ``datetime.now(UTC)``) while the new row
    # began at ``payload.effective_from``. Whenever the caller supplied an
    # ``effective_from`` earlier than that clock read — which the seeder and
    # ``scripts/reconcile_pricing_units.py`` both do, because they compute the
    # date before the POST — BOTH ROWS WERE VALID for exactly the difference, and
    # which one priced a usage row in that window was decided by row order, not
    # by the timeline.
    #
    # Measured on prod 2026-09-10: 51 pairs of ``provider_pricing`` rows overlap
    # on their full key. The shape is this defect at two scales. Four are
    # sub-second (hirag's window is 566 ms — prior ``effective_until``
    # 11:03:31.618063 against the new row's ``effective_from`` 11:03:31.052081;
    # kag-graph's is 506 ms), which is the clock skew between the caller and the
    # server. The rest are months wide, from a deliberately backdated
    # ``effective_from``: ``mlx-whisper`` id 116 starts 2026-04-26 and overlaps
    # ids 25 and 27 for their entire life, at 3.00 against 283.33 per 1M input —
    # a 94x spread with no determinate answer.
    #
    # Closing the prior row AT the new row's start makes the two ranges abut by
    # construction, from ONE value. There is no second clock read to skew.
    #
    # ⚠️ Realised damage was $0.00 and that is not a reason to leave it. Every
    # overlap window had closed by 2026-07-21 and the oldest surviving event is
    # 2026-09-05 (the table was truncated), so no event ever fell inside one; the
    # latent repricing swing was $1.99. The defect is that the price of a unit was
    # not a function of the timeline, and that is not a quantity that needs to be
    # large before it is worth removing.
    effective_from = payload.effective_from or closed_at

    if prior is not None:
        # ⛔ A SUPERSEDE CANNOT START BEFORE THE ROW IT SUPERSEDES. Closing the
        # prior row at ``effective_from`` would otherwise write
        # ``effective_until < effective_from`` onto it — a row valid over an
        # inverted interval, which every range predicate reads as "valid never".
        # The rate would vanish from the timeline rather than be superseded, and
        # the only symptom would be usage silently pricing against whatever row
        # matched next. Refuse instead: a backdated rate that predates its
        # predecessor is not expressible here, and the caller has to say which
        # row it actually means to replace.
        if effective_from < prior.effective_from:
            raise PricingSupersedeBeforePriorError(
                provider=payload.provider,
                model=payload.model,
                effective_from=effective_from,
                prior_effective_from=prior.effective_from,
            )
        prior.effective_until = effective_from
    new_row = ProviderPricing(
        provider=payload.provider,
        model=payload.model,
        service_key=payload.service_key,
        rate_class=payload.rate_class,
        cost_model=payload.cost_model,
        cost_basis=payload.cost_basis,
        input_per_1m_usd=payload.input_per_1m_usd,
        output_per_1m_usd=payload.output_per_1m_usd,
        cache_creation_per_1m_usd=payload.cache_creation_per_1m_usd,
        cache_read_per_1m_usd=payload.cache_read_per_1m_usd,
        per_request_usd=payload.per_request_usd,
        monthly_fixed_cost_usd=payload.monthly_fixed_cost_usd,
        effective_from=effective_from,
        effective_until=payload.effective_until,
        notes=payload.notes,
    )
    session.add(new_row)
    await session.flush()
    return new_row, prior


def pricing_to_response_dict(row: ProviderPricing) -> dict[str, Any]:
    """Plain dict view of a ProviderPricing row — ready for ProviderPricingResponse."""
    return {
        "id": row.id,
        "provider": row.provider,
        "model": row.model,
        "service_key": row.service_key,
        "rate_class": row.rate_class,
        "cost_model": row.cost_model,
        "cost_basis": row.cost_basis,
        "input_per_1m_usd": row.input_per_1m_usd,
        "output_per_1m_usd": row.output_per_1m_usd,
        "cache_creation_per_1m_usd": row.cache_creation_per_1m_usd,
        "cache_read_per_1m_usd": row.cache_read_per_1m_usd,
        "per_request_usd": row.per_request_usd,
        "monthly_fixed_cost_usd": row.monthly_fixed_cost_usd,
        "effective_from": row.effective_from,
        "effective_until": row.effective_until,
        "notes": row.notes,
    }
