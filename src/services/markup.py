"""Markup resolution — the north-star billing markup engine.

Resolves a per-usage-line markup from the effective-dated ``markup_rules``
table. Mirrors :mod:`services.pricing` (the cost side) but on the *billing*
side: pricing decides what a token COST, markup decides what we CHARGE.

Resolution is **most-specific-wins** with the **consumption axis dominant**
(decided with George). Each set match dimension contributes a distinct
power-of-two weight; the highest-scoring matching rule wins::

    byo         64   (the commercial axis — see the weight block below)
    model       32
    provider    16
    service_key  8   (the "app" dimension)
    license_id   4
    project_id   2   (the kanban-project axis)
    tenant_id    1   (1 = a tenant rule; 0 = the global-default row)

So a fully-specified ``tenant+app+provider+model`` rule (score 57) outranks a
flat ``tenant+license`` rule (score 5) — an expensive model can carry a premium
even under a flat-rate license. Score ties are impossible at a single instant:
the ``ix_markuprules_open`` partial-unique guarantees one effective row per
match-tuple, and every dimension is a distinct bit.

A billed line is then::

    billed = incurred * (1 + (markup_pct or 0)/100)
             + Σ(tokens * surcharge_rate / 1e6)
             + request_count * surcharge_per_request_usd

Consumed per-line by billing: :func:`build_markup_resolver` loads the effective
rules once and :func:`compute_billed_line` applies mode + rule + the legacy
per-tenant fallback to each cost line (``services/billing.generate_billing``,
``routers/billing.compute_billing_overview``, the ``routers/usage`` dashboard
series/breakdown, and ``services/admin_aggregate``).
"""

from __future__ import annotations

from datetime import UTC, datetime
from decimal import ROUND_HALF_UP, Decimal
from typing import Any, NamedTuple
from uuid import UUID

import structlog
from sqlalchemy import or_, select
from sqlalchemy.ext.asyncio import AsyncSession

import observability.metrics as observability_metrics
from models.db import MarkupRule
from models.domain import MarkupRuleCreate

logger = structlog.stdlib.get_logger(__name__)

# --------------------------------------------------------------------------- #
# THE money rounding contract
# --------------------------------------------------------------------------- #
# ⭐ ONE ROUNDING MODE FOR EVERY MONEY FIGURE THIS SERVICE EMITS, named in one
# place so it cannot be half-chosen.
#
# ⛔ A BARE `.quantize(Decimal("0.01"))` IS NOT A DECISION — it silently takes
# `decimal.getcontext().rounding`, which is ROUND_HALF_EVEN (banker's rounding).
# Every money figure in this module already said ROUND_HALF_UP explicitly, while
# `services/billing.py` wrote the invoice totals with the bare form, so the two
# halves of the same invoice disagreed on a half-cent: a line billed 0.025 became
# 0.03 here and the total containing it became 0.02 there. That is not a
# rounding-policy debate, it is two policies in one statement.
#
# HALF_UP is the mode because it is the one the codebase EXPRESSED (every
# `quantize` in this file carried it since the module was written); the bare
# calls were omissions, not a competing choice.
#
# ⚠️ THE ONE DELIBERATE HALF_EVEN IN THE REPO IS NOT A COUNTER-EXAMPLE:
# `services/billing.apply_markup` is bare-quantize and
# `tests/test_admin_usage_system.py:129` pins its banker's-rounding result with a
# comment. That helper has NO caller in `src/` (measured: the only references are
# that test module) so it prices nothing; it is left exactly as it is rather than
# flipped under a test that asserts its current behaviour.
#: Money is carried to cents. Sub-cent precision survives on the *incurred* rollup
#: columns (Numeric(20,6)/(20,8)); it does not survive onto a billed figure.
MONEY_QUANT = Decimal("0.01")
#: The single rounding mode. Import it; never re-spell a mode at a call site.
MONEY_ROUNDING = ROUND_HALF_UP

_MILLION = Decimal("1000000")


def quantize_money(value: Decimal) -> Decimal:
    """Round one money figure to cents under the service's single rounding mode.

    The ONLY sanctioned way to put a Decimal onto an invoice, a statement or a
    response body. Use it instead of ``.quantize(...)`` so the mode is impossible
    to omit — an omission is what produced two rounding modes inside one invoice.

    ⛔ Decimal in, Decimal out. Never pass a float: ``Decimal(0.025)`` is
    0.025000000000000001387778780781445675529539585113525390625 and would round
    UP where the exact Decimal("0.025") is a true half. The whole money path is
    Decimal today (``Decimal(str(pct))`` below is there for the same reason) and
    must stay that way.

    ⭐ CALL IT ONCE PER EMITTED FIGURE — NEVER PER LINE. Rounding is not
    distributive over addition, so quantising each line and then summing is a
    DIFFERENT number from summing and then quantising once. That made the bill a
    function of how the same spend happened to be SPLIT into rows: twenty lines
    of $0.001 incurred **at a 50% markup** rounded to $0.00 twenty times and
    billed **$0.00** where the one line they are worth bills $0.03 — and a rollup
    key gaining a column (as ``project_id`` did) re-splits every tenant's lines
    without touching a rate. ⚠️ The markup is part of the figure, not decoration:
    the same twenty lines at 0% are worth $0.02, and it is
    ``test_twenty_sub_cent_lines_bill_the_money_they_are_worth`` — which applies
    50% — that $0.03 is quoting.
    So no line-level helper in this module quantises any more; the invoice
    totals, the per-licence subtotals and the response fields do, each exactly
    once, at the point the figure becomes a charge.
    """
    return Decimal(value).quantize(MONEY_QUANT, rounding=MONEY_ROUNDING)


# Specificity weights — consumption axis dominant (see module docstring).
#
# ⭐ `byo` OUTRANKS `model`, and that ordering is a commercial decision, not a
# tidiness one. "What we charge on a customer's own provider key" is a term of
# the contract; "what we charge for gpt-4o" is a rate-card line. If a
# model-pinned rule could outrank a byo-scoped one, adding a row for a new model
# would silently re-price a tenant's BYO traffic — a rate-card edit changing a
# contract. Weight 64 > 32 makes that impossible, and
# `test_model_pinned_rule_does_not_outrank_byo_rule` is what holds it.
#
# ⭐ THE 2026-09-15 RESCALE IS ORDER-PRESERVING, AND THAT IS THE WHOLE ARGUMENT
# FOR ITS SAFETY.
#
# `project_id` had to sit strictly between LICENSE and TENANT (a project rule
# overrides the tenant-wide default; a per-licence/app rule still overrides the
# project rule). There is NO INTEGER BETWEEN 2 AND 1, so PROJECT could not be
# inserted into the old ladder. These weights are distinct BITS, so the fix is a
# one-bit LEFT SHIFT of everything above tenant, freeing bit 1 for PROJECT:
#
#   byo 32->64   model 16->32   provider 8->16   service_key 4->8
#   license 2->4   PROJECT = 2 (new, bit 1)   tenant 1->1 (UNCHANGED, bit 0)
#
# ⚠️ IT IS NOT "DOUBLE EVERY WEIGHT" — `_W_TENANT` deliberately STAYS 1. Tenant
# keeps bit 0 because it is the ladder's floor (1 = a tenant rule, 0 = the
# global-default row, which `compute_billed_line_detailed` tests for with
# `_specificity(rule) > 0`); doubling it to 2 would collide with PROJECT.
#
# So the transform is: new(S) = 2*old(S) - [tenant ∈ S]. For any two old
# dimension sets A and B with old(A) > old(B), the scores are integers so
# old(A) - old(B) >= 1, hence
#
#   new(A) - new(B) = 2*(old(A) - old(B)) - ([tenant∈A] - [tenant∈B]) >= 2 - 1 = 1
#
# which is > 0. The subtracted term is bounded by 1 while the doubled gap is at
# least 2, so it can never close a gap, let alone reverse one. Equal old scores
# are impossible between distinct sets (every dimension is a distinct bit), so
# no new tie is introduced either. EVERY pre-existing pairwise ordering is
# preserved exactly: nothing that used to win can now lose, and nothing that
# used to lose can now win. The rescale therefore re-prices NOTHING — it only
# makes a new, strictly-weaker-than-license dimension expressible.
#
# WHAT THE PROOF COVERS — AND THE ONE THING IT DOES NOT.
#
# ⭐ COVERED: every test that asserts WHICH RULE WINS stays green UNMODIFIED,
# above all `test_model_pinned_rule_does_not_outrank_byo_rule`
# (tests/test_markup.py:324), which the paragraph above names as the invariant
# holding BYO over model. Verified exhaustively, not by assertion: all 64 old
# dimension subsets compared pairwise (4096 pairs) give ZERO order inversions
# between the old and new ladders, and all 64 scores stay distinct under both.
# That test's docstring still spells the pre-rescale "32 > 16" — stale PROSE,
# not a stale assertion; it asserts a winner, never a weight. Its mutation
# target still kills: with `_W_BYO = 8` the model rule (1+32=33) outranks the
# byo rule (1+8=9).
#
# ⛔ NOT COVERED, AND DO NOT CLAIM OTHERWISE: the ABSOLUTE score changes, and it
# is a PUBLISHED value, not an internal one. `markup_to_response_dict` emits it
# as `"specificity"` on `MarkupRuleResponse`, and
# `test_byo_dimension_raises_specificity_and_is_reported` exists precisely
# because "an operator comparing two rules reads that number". So a rescale is
# behaviour-preserving for RESOLUTION and wire-visible for DISCLOSURE. Three
# assertions pin the old absolutes and MUST move with this change:
#
#   tests/test_markup.py:85          == 29  ->  57   (tenant+service_key+provider+model)
#   tests/test_markup.py:478         == 33  ->  65   (tenant+byo)
#   tests/test_markup_routes.py:70   == 29  ->  57   (same tuple, over HTTP)
#
# (tests/test_markup.py:49 and :477 and tests/test_markup_routes.py:462 pin 0
# and 1 — the global default and a tenant-only rule — and are UNCHANGED, because
# tenant keeps bit 0 and the empty set keeps score 0.)
#
# The new insertion is verified by arithmetic, not by assertion:
#   project+tenant = 2+1 = 3  <  license = 4   → a per-licence/app rule still
#                                                outranks a project rule ✅
#   project = 2  >  tenant = 1                 → a project rule overrides the
#                                                tenant-wide default ✅
#   every weight below BYO sums to 32+16+8+4+2+1 = 63 < 64 → BYO still dominates
#                                                any combination of the rest ✅
_W_BYO = 64
_W_MODEL = 32
_W_PROVIDER = 16
_W_SERVICE_KEY = 8
_W_LICENSE = 4
_W_PROJECT = 2
_W_TENANT = 1


def _specificity(row: MarkupRule) -> int:
    """Score a rule by how many (and which) match dimensions it pins."""
    return (
        (_W_BYO if row.byo is not None else 0)
        + (_W_MODEL if row.model is not None else 0)
        + (_W_PROVIDER if row.provider is not None else 0)
        + (_W_SERVICE_KEY if row.service_key is not None else 0)
        + (_W_LICENSE if row.license_id is not None else 0)
        + (_W_PROJECT if row.project_id is not None else 0)
        + (_W_TENANT if row.tenant_id is not None else 0)
    )


# ⛔ `project_id` IS REQUIRED AND KEYWORD-ONLY ON EVERY PUBLIC PRICING ENTRY
# POINT BELOW, WITH NO DEFAULT. DO NOT "TIDY" A `= None` BACK IN.
#
# A defaulted `project_id` fails SILENTLY and in the worst direction: a caller
# that forgets it prices the line at the TENANT-WIDE rate — no error, no log, no
# counter, just a wrong number on an invoice. Nothing in CI can catch the
# omission, because the omission is itself a valid call.
#
# That is not hypothetical. FOUR of the six call sites shipped exactly this bug:
#   - services/billing.py          (original 0036 commit)
#   - routers/billing.py           (original 0036 commit)
#   - routers/usage.py             (original 0036 commit)
#   - services/admin_aggregate.py  (landed mid-sweep with `proj_uuid` computed
#                                   and never passed; caught only by ruff F841)
# The seventh caller would have failed identically.
#
# With no default, omitting it is an immediate `TypeError` AT THE CALL SITE
# instead of a wrong price on a bill. A caller that genuinely has no project
# MUST write `project_id=None` explicitly — that explicitness is the entire
# point: it forces a decision and leaves the decision visible in the diff.
# `services/billing.py`'s app-request line is the precedent: it passes `None`
# because `billing_metering_daily` deliberately has no project column
# (entitlement is per licence, not per project).
#
# ⛔ Never substitute the NIL/sentinel uuid for a missing project — `None` is
# the "no project" value and a sentinel would match a project-pinned rule.
#
# Held by tests/test_pricing_requires_explicit_project.py.


def _matches(
    row: MarkupRule,
    *,
    tenant_id: str | None,
    license_id: UUID | None,
    project_id: UUID | None,
    service_key: str | None,
    provider: str | None,
    model: str | None,
    byo: bool | None = None,
) -> bool:
    """A rule matches iff every dimension it pins equals the event's value.

    A NULL rule dimension is a wildcard (matches anything). A pinned rule
    dimension requires the event to carry exactly that value — so a rule that
    pins ``license_id`` cannot match an event with no license (``None``).

    ``project_id`` (the kanban-project axis) is matched by exactly that rule:
    ``project_id IS NULL`` applies to any project (a tenant-wide rule), a set
    ``project_id`` applies only to that project and cannot match a line that
    carries no project.

    ⚠️ ``byo`` is the one dimension whose EVENT value is a bool and never absent
    in the ledger (``billing_cost_daily.byo`` is NOT NULL DEFAULT false). A
    caller that cannot say (the app-request path) passes ``None``, and a
    byo-pinned rule must NOT match such a line: guessing "not BYO" would apply
    the platform-provided rate to BYO spend. The generic
    ``rule_val is None or rule_val == event_val`` test gives exactly that,
    because ``True == None`` is False.
    """
    # ⛔ A PROJECT RULE IS ONLY EVER CONSIDERED UNDER THE TENANT THAT OWNS IT.
    #
    # The generic wildcard test below would let a row with `tenant_id IS NULL`
    # and a set `project_id` match ANY tenant's line carrying that project id.
    # Project ids are minted by kanban, not by this service, so "the same
    # project id under a different tenant" is not a hypothetical — it is a
    # cross-tenant repricing one INSERT away, and the all-wildcard global
    # default row is the shape that makes a NULL tenant_id look routine.
    # A project-pinned rule with no tenant is refused outright rather than
    # matched, so `project_id` alone can never select a rule.
    if row.project_id is not None and row.tenant_id is None:
        return False
    pairs = (
        (row.tenant_id, tenant_id),
        (row.license_id, license_id),
        (row.project_id, project_id),
        (row.service_key, service_key),
        (row.provider, provider),
        (row.model, model),
        (row.byo, byo),
    )
    return all(rule_val is None or rule_val == event_val for rule_val, event_val in pairs)


class MarkupResolver:
    """In-memory resolver over a pre-fetched set of effective markup rules.

    Billing reads many usage lines per request (per day × provider × model on
    the usage dashboard; per (license, app, provider, model) on an invoice).
    Issuing one ``resolve_markup_rule`` DB query per line is wasteful, so the
    caller fetches all candidate rules **once** via :func:`build_markup_resolver`
    and resolves every line in memory. ``resolve`` carries ``tenant_id`` so the
    same resolver can be loaded with all tenants' rules (admin/platform views)
    and still attribute each line to its own tenant — a rule pinned to tenant A
    never matches tenant B's line, while global (NULL-tenant) rules match all.
    """

    def __init__(self, rows: list[MarkupRule]) -> None:
        self._rows = rows

    def resolve(
        self,
        *,
        tenant_id: str | None,
        license_id: UUID | None = None,
        # ⛔ REQUIRED, NO DEFAULT — see the block above `_matches`. Omitting it used to
        # price silently at the tenant-wide rate; a caller with no project writes
        # `project_id=None` explicitly.
        project_id: UUID | None,
        service_key: str | None = None,
        provider: str | None = None,
        model: str | None = None,
        byo: bool | None = None,
    ) -> MarkupRule | None:
        """Highest-specificity matching rule, or ``None`` (logged as a miss)."""
        candidates = [
            r
            for r in self._rows
            if _matches(
                r,
                tenant_id=tenant_id,
                license_id=license_id,
                project_id=project_id,
                service_key=service_key,
                provider=provider,
                model=model,
                byo=byo,
            )
        ]
        if not candidates:
            logger.warning(
                "markup_miss",
                tenant_id=tenant_id,
                provider=provider,
                model=model,
                service_key=service_key,
            )
            observability_metrics.markup_miss_total.labels(
                provider=provider or "unknown",
            ).inc()
            return None
        # Highest specificity wins; newer effective_from breaks any (impossible) tie.
        candidates.sort(key=lambda r: (_specificity(r), r.effective_from), reverse=True)
        return candidates[0]


async def build_markup_resolver(
    session: AsyncSession,
    *,
    tenant_id: str | None = None,
    ts: datetime,
) -> MarkupResolver:
    """Load every rule effective at ``ts`` into a :class:`MarkupResolver`.

    ``tenant_id=None`` loads all tenants' rules (for multi-tenant admin
    aggregation); a value scopes the fetch to that tenant's rules + global ones
    (the common single-invoice / single-dashboard path). Matching on the other
    four dimensions happens in :meth:`MarkupResolver.resolve`.
    """
    stmt = select(MarkupRule).where(
        MarkupRule.effective_from <= ts,
        or_(MarkupRule.effective_until.is_(None), MarkupRule.effective_until > ts),
    )
    if tenant_id is not None:
        stmt = stmt.where(or_(MarkupRule.tenant_id.is_(None), MarkupRule.tenant_id == tenant_id))
    rows = list((await session.execute(stmt)).scalars().all())
    return MarkupResolver(rows)


async def resolve_markup_rule(
    session: AsyncSession,
    *,
    tenant_id: str | None,
    license_id: UUID | None = None,
    # ⛔ REQUIRED, NO DEFAULT — see the block above `_matches`. Omitting it used to
    # price silently at the tenant-wide rate; a caller with no project writes
    # `project_id=None` explicitly.
    project_id: UUID | None,
    service_key: str | None = None,
    provider: str | None = None,
    model: str | None = None,
    byo: bool | None = None,
    ts: datetime,
) -> MarkupRule | None:
    """Return the winning :class:`MarkupRule` for a single usage line at ``ts``.

    Convenience one-shot wrapper over :func:`build_markup_resolver` +
    :meth:`MarkupResolver.resolve` — fetches the tenant-scoped rules effective at
    ``ts`` and resolves one line. Returns the all-wildcard global-default row
    when no more-specific rule matches; ``None`` only when not even a default has
    been seeded (a ``markup_miss`` is logged + counted in that case). Hot paths
    that resolve many lines should build the resolver once instead.
    """
    resolver = await build_markup_resolver(session, tenant_id=tenant_id, ts=ts)
    return resolver.resolve(
        tenant_id=tenant_id,
        license_id=license_id,
        project_id=project_id,
        service_key=service_key,
        provider=provider,
        model=model,
        byo=byo,
    )


def absolute_surcharge_usd(
    row: MarkupRule | None,
    *,
    input_tokens: int = 0,
    output_tokens: int = 0,
    cache_creation_input_tokens: int = 0,
    cache_read_input_tokens: int = 0,
    request_count: int = 0,
) -> Decimal:
    """The ABSOLUTE (non-percentage) component of a rule, UNQUANTISED.

    ``Σ(tokens * surcharge_per_1m / 1e6) + requests * surcharge_per_request``.

    ⭐ This exists so the fee has exactly ONE implementation. It is called both
    by :func:`compute_billed_usd` (which adds it to the relative component) and
    by :func:`compute_billed_line_detailed` (which reports it separately as the
    platform fee). A second copy of this arithmetic would let the number that is
    CHARGED and the number that is DISCLOSED drift apart silently, which is the
    precise failure that made ``platform_fee_usd`` unreportable in the first
    place.

    Returned unquantised on purpose: the caller that bills quantises the SUM
    once, so factoring this out does not move any charged amount.
    """
    if row is None:
        return Decimal("0")

    def _surcharge(tokens: int, rate: Decimal | None) -> Decimal:
        if not tokens or rate is None:
            return Decimal("0")
        return (Decimal(tokens) * Decimal(rate)) / _MILLION

    total = _surcharge(input_tokens, row.surcharge_input_per_1m_usd)
    total += _surcharge(output_tokens, row.surcharge_output_per_1m_usd)
    total += _surcharge(cache_creation_input_tokens, row.surcharge_cache_creation_per_1m_usd)
    total += _surcharge(cache_read_input_tokens, row.surcharge_cache_read_per_1m_usd)
    if request_count and row.surcharge_per_request_usd is not None:
        total += Decimal(request_count) * Decimal(row.surcharge_per_request_usd)
    return total


def compute_billed_usd(
    row: MarkupRule | None,
    *,
    incurred_usd: Decimal,
    input_tokens: int = 0,
    output_tokens: int = 0,
    cache_creation_input_tokens: int = 0,
    cache_read_input_tokens: int = 0,
    request_count: int = 0,
    force_pct_zero: bool = False,
    fee_only: bool = False,
) -> Decimal:
    """Apply a resolved markup rule to one usage line.

    ``billed = incurred*(1+pct/100) + Σ(tokens*surcharge/1e6) + requests*per_req``.
    When ``row`` is ``None`` the incurred cost passes through unchanged.
    ``force_pct_zero`` drops the relative component (used by the live
    ``byo_platform_fee`` mode where BYO token spend is a pass-through estimate
    and only the absolute platform fee is charged).

    ⛔ RETURNS AN UNQUANTISED FIGURE, deliberately — see :func:`quantize_money`.
    The caller sums these and quantises the SUM once.
    """
    incurred = Decimal(incurred_usd)
    if row is None:
        return incurred

    pct = Decimal("0") if force_pct_zero else Decimal(row.markup_pct or 0)
    # ``fee_only`` (``byo_markup_only`` + a BYO line): the customer paid the provider, so the
    # incurred REFERENCE cost is not re-charged — only the percentage of it is.
    billed = incurred * (pct / Decimal("100") if fee_only else Decimal("1") + pct / Decimal("100"))
    billed += absolute_surcharge_usd(
        row,
        input_tokens=input_tokens,
        output_tokens=output_tokens,
        cache_creation_input_tokens=cache_creation_input_tokens,
        cache_read_input_tokens=cache_read_input_tokens,
        request_count=request_count,
    )

    return billed


# Billing modes (mirror tenant_config.billing_mode / models/db/config.py).
BILLING_MODE_STANDARD = "standard_markup"
BILLING_MODE_BYO = "byo_platform_fee"
BILLING_MODE_PASS_THROUGH = "pass_through"
#: ⭐ BYO lines bill ONLY the percentage of their reference cost (+ absolute surcharges); the
#: reference cost itself is NOT re-charged because the customer paid the provider directly.
#: Non-BYO lines behave exactly as ``standard_markup``.
#:
#: ⛔ This is the fee schema ``byo_platform_fee`` cannot express: that mode FORCES the percentage
#: to 0 and charges only an absolute surcharge. A commercial "X% markup on your own LLM spend"
#: needs the percentage and NOT the base, which neither existing mode produces —
#: ``standard_markup`` bills ``incurred × (1 + pct)``, i.e. charges the provider cost a second time.
#:
#: ⚠️ The line's incurred cost is ALSO excluded from ``internal_cost`` in ``generate_billing``:
#: the platform paid nothing for it, and leaving it in reports a negative margin on every invoice.
BILLING_MODE_BYO_MARKUP_ONLY = "byo_markup_only"
# Phase 4 byo_mode gate: internal/demo tenants (byo_mode local_allowed |
# platform_only) are not billed — every line bills $0. Resolved at billing time
# from licenses byo_mode (see services/byo_mode.py), never persisted.
BILLING_MODE_INTERNAL = "internal"


# --------------------------------------------------------------------------- #
# Markup PROVENANCE
# --------------------------------------------------------------------------- #
# ⭐ An invoice reading "markup 30%" that cannot say WHERE the 30 came from is
# not reconcilable: the operator cannot tell an effective-dated commercial rule
# from a code constant nobody decided. Measured 2026-09-08, `markup_rules` holds
# FOUR rows and every one of them is CLOSED (`effective_until` set), so today
# every tenant silently rides `settings.default_markup_pct`. That is precisely
# the state in which the distinction is invisible and therefore worth recording.
#
# These are the only sources a billed percentage can have. Every billed line
# carries exactly one.
MARKUP_SOURCE_RULE_PREFIX = "markup_rule:"
#: `settings.default_markup_pct` — a CODE CONSTANT, not an operator decision.
MARKUP_SOURCE_SETTINGS = "settings_default"
#: `tenant_config.markup_pct` on THIS tenant's own row.
MARKUP_SOURCE_TENANT_CONFIG = "tenant_config"
#: `tenant_config.markup_pct` on the global default-config row.
MARKUP_SOURCE_TENANT_CONFIG_DEFAULT = "tenant_config_default"
#: `pass_through` mode — billed at cost, no markup applied.
MARKUP_SOURCE_PASS_THROUGH = "pass_through"
#: Internal/demo tenant — billed $0, no markup applied.
MARKUP_SOURCE_INTERNAL = "internal"
#: Provenance was not recorded — a snapshot predating this field. NEVER write
#: this for a fresh close; it exists so a legacy row reads as "we do not know"
#: rather than silently claiming a source it never had.
MARKUP_SOURCE_UNKNOWN = "unknown"


def markup_source_for_rule(rule: MarkupRule) -> str:
    """The provenance token naming ``rule`` as the source of a billed pct.

    ⚠️ ``MarkupRule.id`` is a ``bigserial`` (``BigInteger`` + ``autoincrement``,
    ``nextval('markup_rules_id_seq')``) — NOT a UUID. It is therefore stable
    WITHIN ONE DATABASE LINEAGE and meaningless across lineages: after a
    ``pg_dump``/reseed, a logical-replication rebuild, or in a fresh
    environment, the same commercial rule gets a different id, so
    ``markup_rule:37`` on a prod invoice and ``markup_rule:37`` in staging name
    DIFFERENT rules. ``MarkupRule`` carries no UUID and no natural key to fall
    back on.

    ⛔ CONSTRAINT ON FUTURE EDITS — this token is safe to freeze into an
    immutable invoice ONLY because ``services.invoices._capture_inputs``
    serialises the WHOLE matching rule rows into ``inputs["markup_rules"]``, so
    the statement carries the dereference beside the pointer and resolves
    ``markup_rule:37`` from its own frozen document without consulting the live
    table. Anyone who later trims ``inputs["markup_rules"]`` to "just the ids,
    we can look them up" silently converts a self-contained invoice into one
    that CANNOT be reproduced after a reseed — and nothing would fail at the
    time of that edit. Keep the rows whole, or replace this token with the
    match-tuple (tenant_id, license_id, service_key, provider, model) +
    effective_from, which is what ``ix_markuprules_open`` uniquifies.
    """
    return f"{MARKUP_SOURCE_RULE_PREFIX}{rule.id}"


class BilledLine(NamedTuple):
    """One billed usage line, decomposed so the invoice can disclose it.

    ``billed_usd`` is the charge and is IDENTICAL to what the previous 3-tuple
    returned — this type adds disclosure, it does not reprice anything.

    ⛔ EVERY MONEY FIELD HERE IS UNQUANTISED. A line is not a charge; an invoice
    is. Rounding does not distribute over addition, so a per-line quantum made
    the bill depend on how the spend was SPLIT into rows rather than on what it
    cost (twenty $0.001 lines billed $0.00). Whoever turns these into a figure a
    customer sees calls :func:`quantize_money` on the SUM, once.

    ``surcharge_usd`` is the absolute (non-percentage) component actually
    charged. ``platform_fee_usd`` is the subset of that which is the BYO
    PLATFORM FEE — i.e. charged on a line where the relative markup was forced
    to zero because the tenant is on ``byo_platform_fee`` and the line is BYO.

    ⚠️ The two are deliberately NOT the same field. A surcharge on a
    ``standard_markup`` rule is extra markup, not a platform fee; reporting it
    as `platform_fee_usd` would overstate the fee on the statement. Both are
    returned so neither number has to be inferred from the other.
    """

    billed_usd: Decimal
    applied_pct: float
    rule: MarkupRule | None
    surcharge_usd: Decimal
    platform_fee_usd: Decimal
    markup_source: str


def compute_billed_line_detailed(
    resolver: MarkupResolver,
    billing_mode: str,
    *,
    tenant_id: str | None,
    incurred_usd: Decimal,
    fallback_pct: float,
    fallback_source: str = MARKUP_SOURCE_SETTINGS,
    license_id: UUID | None = None,
    # The kanban-project axis. `None` = this line carries no project, and a
    # project-pinned rule must not match it (same containment as license_id).
    # ⛔ REQUIRED, NO DEFAULT — see the block above `_matches`. Omitting it used to
    # price silently at the tenant-wide rate; a caller with no project writes
    # `project_id=None` explicitly.
    project_id: UUID | None,
    service_key: str | None = None,
    provider: str | None = None,
    model: str | None = None,
    # ⭐ `None` MEANS 'THIS LINE HAS NO BYO DIMENSION', and it must be
    # EXPRESSIBLE. `_rule_matches`'s docstring already states the contract —
    # "A caller that cannot say (the app-request path) passes ``None``, and a
    # byo-pinned rule must NOT match such a line" — but `byo: bool = False`
    # made `None` unrepresentable, so the app-request caller silently asserted
    # `byo=False` instead. That is not a harmless default: `_W_BYO = 32` is the
    # HIGHEST specificity weight, so a `byo=False` rule outranks even a
    # service_key-scoped one, and an app-request line (which has no provider and
    # no BYO dimension at all) would take a rate written for platform-provided
    # GPU spend. Measured 2026-09-11: prod holds no `byo=False` rule, so this was
    # latent — one row away, not already wrong.
    byo: bool | None = None,
    input_tokens: int = 0,
    output_tokens: int = 0,
    cache_creation_input_tokens: int = 0,
    cache_read_input_tokens: int = 0,
    request_count: int = 0,
) -> BilledLine:
    """Bill one usage line — the single markup-application primitive.

    Combines the billing mode, the resolved markup rule, and the legacy
    per-tenant ``tenant_config.markup_pct`` (``fallback_pct``) into one billed
    amount, and reports the components that make the charge auditable.

    Resolution policy (revenue-safe transition):
    - ``pass_through`` → bill the incurred cost unchanged.
    - A rule **more specific than the global default** (specificity > 0) takes
      over the line: its ``markup_pct`` + absolute surcharges apply.
    - Otherwise (only the all-wildcard global default matched, or a miss) fall
      back to ``fallback_pct`` — the tenant's configured markup — with no
      surcharge. This preserves per-tenant ``tenant_config.markup_pct`` overrides
      that have not (yet) been migrated into ``markup_rules``.
    - ``byo_platform_fee`` + a BYO line forces the relative % to 0; only the
      resolved absolute surcharge (the platform fee) is charged.

    ``fallback_source`` names where ``fallback_pct`` came from, and the caller
    MUST pass it truthfully — this function cannot see the DB and would
    otherwise have to guess. It defaults to ``settings_default`` because that is
    the only source available to a caller that did not look one up.
    """
    if billing_mode == BILLING_MODE_INTERNAL:
        # Internal/demo tenant (byo_mode local_allowed|platform_only): not a
        # paying customer — bill $0 regardless of incurred cost or markup rules.
        return BilledLine(
            quantize_money(Decimal("0")),
            0.0,
            None,
            quantize_money(Decimal("0")),
            quantize_money(Decimal("0")),
            MARKUP_SOURCE_INTERNAL,
        )
    if billing_mode == BILLING_MODE_PASS_THROUGH:
        return BilledLine(
            Decimal(incurred_usd),
            0.0,
            None,
            quantize_money(Decimal("0")),
            quantize_money(Decimal("0")),
            MARKUP_SOURCE_PASS_THROUGH,
        )

    rule = resolver.resolve(
        tenant_id=tenant_id,
        license_id=license_id,
        project_id=project_id,
        service_key=service_key,
        provider=provider,
        model=model,
        byo=byo,
    )
    # ⭐ A BYO-SCOPED RULE WINS; THE FORCED ZERO SURVIVES ONLY AS THE FALLBACK.
    #
    # Before 0027 this read `billing_mode == BILLING_MODE_BYO and byo`
    # unconditionally, so a `byo_platform_fee` tenant's BYO spend was ALWAYS
    # marked up 0% and there was no row anyone could write to say otherwise —
    # George asked for 20% on BYO lines and the engine could not express it.
    #
    # `byo_rule_applies` is the whole change. Read it as: the operator has
    # deliberately written a rule that applies to BYO lines and only to BYO
    # lines, so that rule's percentage is the answer. Absent such a rule nothing
    # has been decided about BYO and the historical zero stands — which is why
    # every existing expectation in tests/test_markup.py is untouched.
    #
    # ⚠️ `rule.byo is True`, not truthiness: `MarkupRule.byo` has THREE meanings
    # (True = BYO-only rule, False = platform-provided-only, None = applies to
    # both). A `byo=False` rule must NOT lift the zero — it is not about BYO
    # lines at all.
    byo_rule_applies = rule is not None and rule.byo is True
    force_zero = bool(billing_mode == BILLING_MODE_BYO and byo and not byo_rule_applies)
    # The whole charge on a markup-only BYO line IS the platform's fee — there is no base in it.
    fee_only = bool(billing_mode == BILLING_MODE_BYO_MARKUP_ONLY and byo)

    if rule is not None and _specificity(rule) > 0:
        billed = compute_billed_usd(
            rule,
            incurred_usd=incurred_usd,
            input_tokens=input_tokens,
            output_tokens=output_tokens,
            cache_creation_input_tokens=cache_creation_input_tokens,
            cache_read_input_tokens=cache_read_input_tokens,
            request_count=request_count,
            force_pct_zero=force_zero,
            fee_only=fee_only,
        )
        applied_pct = 0.0 if force_zero else float(rule.markup_pct or 0)
        # ⛔ UNQUANTISED, like `billed` above and for the same reason: this is a
        # COMPONENT of that billed figure (`compute_billed_usd` adds the very
        # same surcharge in), so rounding it here and not there would make the
        # disclosure disagree with the charge it is disclosing. The caller sums
        # these across lines and quantises the total once.
        surcharge = absolute_surcharge_usd(
            rule,
            input_tokens=input_tokens,
            output_tokens=output_tokens,
            cache_creation_input_tokens=cache_creation_input_tokens,
            cache_read_input_tokens=cache_read_input_tokens,
            request_count=request_count,
        )
        # Only a BYO line under byo_platform_fee is a PLATFORM FEE. See BilledLine.
        fee = surcharge if force_zero else billed if fee_only else quantize_money(Decimal("0"))
        return BilledLine(billed, applied_pct, rule, surcharge, fee, markup_source_for_rule(rule))

    # Global-default or miss: legacy tenant/default markup, no surcharge.
    pct = 0.0 if force_zero else fallback_pct
    # A billable line charged at a 0% markup is a zero-margin miss — but only
    # when there is actual cost to have earned margin on. A $0-incurred line
    # (nothing to bill) is not a margin event, so gate on incurred_usd > 0 to
    # keep the signal to genuine "paying tenant billed at cost" cases.
    #
    # ⚠️ THIS GATE HAS A BLIND SPOT BY CONSTRUCTION, AND IT IS COVERED
    # ELSEWHERE — do not "fix" it by removing the `> 0`.
    #
    # `routers.billing` and `services.billing` build the per-request app line as
    # `app_rates.get(service_key, Decimal("0"))`. An UNMAPPED service_key
    # therefore arrives here with `incurred_usd == 0` — not because the line is
    # genuinely free, but because no rate existed and the default was
    # substituted. That line is real revenue being invoiced at $0.00, and it is
    # precisely the case this gate cannot report: the fabricated zero suppresses
    # the only alarm that would have fired on it.
    #
    # Widening the gate here CANNOT fix that, because by this point "absent
    # rate" and "rate of zero" are the same number and the distinction is
    # unrecoverable. It would only add noise from every genuinely-free line.
    # The miss is therefore counted at the two lookup sites, where the
    # difference is still knowable, as
    # `billing_app_rate_missing_total{service_key, surface}`. The two signals
    # are complementary and neither subsumes the other: this one means "we
    # priced it and earned nothing", that one means "we never priced it at
    # all".
    if not force_zero and pct == 0 and Decimal(incurred_usd) > 0:
        logger.warning(
            "markup_zero_margin",
            billing_mode=billing_mode,
            tenant_id=tenant_id,
            service_key=service_key,
            provider=provider,
            model=model,
            rule_specificity=(_specificity(rule) if rule is not None else None),
        )
        observability_metrics.markup_zero_margin_total.labels(
            billing_mode=billing_mode, service_key=service_key or "none"
        ).inc()
    # ⛔ UNQUANTISED — see `quantize_money`. The caller rounds the sum, once.
    factor = Decimal(str(pct)) / Decimal("100")
    billed = Decimal(incurred_usd) * (factor if fee_only else Decimal("1") + factor)
    # ⚠️ `rule` is returned but is NOT the provenance here. On this branch the
    # all-wildcard global default may well have MATCHED (specificity 0) and been
    # DECLINED in favour of `fallback_pct`. Naming it as the source would credit
    # the billed percentage to a rule that did not produce it — the exact false
    # attribution this field exists to prevent.
    return BilledLine(
        billed,
        pct,
        rule,
        quantize_money(Decimal("0")),
        billed if fee_only else quantize_money(Decimal("0")),
        fallback_source,
    )


def compute_billed_line(
    resolver: MarkupResolver,
    billing_mode: str,
    *,
    tenant_id: str | None,
    incurred_usd: Decimal,
    fallback_pct: float,
    license_id: UUID | None = None,
    # ⛔ REQUIRED, NO DEFAULT — see the block above `_matches`. Omitting it used to
    # price silently at the tenant-wide rate; a caller with no project writes
    # `project_id=None` explicitly.
    project_id: UUID | None,
    service_key: str | None = None,
    provider: str | None = None,
    model: str | None = None,
    byo: bool | None = None,
    input_tokens: int = 0,
    output_tokens: int = 0,
    cache_creation_input_tokens: int = 0,
    cache_read_input_tokens: int = 0,
    request_count: int = 0,
) -> tuple[Decimal, float, MarkupRule | None]:
    """``(billed_usd, applied_pct, rule)`` — the legacy 3-tuple projection.

    Thin wrapper over :func:`compute_billed_line_detailed`; there is one
    implementation of the arithmetic and this drops the disclosure components
    for the callers (usage dashboard, admin aggregation) that do not need them.
    """
    line = compute_billed_line_detailed(
        resolver,
        billing_mode,
        tenant_id=tenant_id,
        incurred_usd=incurred_usd,
        fallback_pct=fallback_pct,
        license_id=license_id,
        project_id=project_id,
        service_key=service_key,
        provider=provider,
        model=model,
        byo=byo,
        input_tokens=input_tokens,
        output_tokens=output_tokens,
        cache_creation_input_tokens=cache_creation_input_tokens,
        cache_read_input_tokens=cache_read_input_tokens,
        request_count=request_count,
    )
    return line.billed_usd, line.applied_pct, line.rule


async def list_current_markup_rules(session: AsyncSession) -> list[MarkupRule]:
    """All currently-effective rules (effective_until IS NULL), most-specific first."""
    stmt = select(MarkupRule).where(MarkupRule.effective_until.is_(None))
    rows = list((await session.execute(stmt)).scalars().all())
    rows.sort(key=lambda r: _specificity(r), reverse=True)
    return rows


async def list_markup_history(
    session: AsyncSession,
    *,
    tenant_id: str | None = None,
    provider: str | None = None,
    model: str | None = None,
    service_key: str | None = None,
    license_id: UUID | None = None,
    project_id: UUID | None = None,
) -> list[MarkupRule]:
    """Full history for an exact match-tuple, newest first.

    Each supplied dimension is matched exactly, including its NULL form, so the
    history is scoped to one logical rule's effective-dated lineage.

    ⚠️ ``project_id`` is matched in its NULL form too (0036). Omitting it from
    this loop would NOT have left the endpoint unchanged: every project-scoped
    row for a tenant would start appearing inside the TENANT-WIDE lineage, so
    the history of "the tenant's baseline markup" would interleave rates that
    only ever applied to one kanban project. "Each supplied dimension is matched
    exactly, including its NULL form" is the contract — a dimension absent from
    the loop is matched as a WILDCARD, which is the opposite.
    """
    stmt = select(MarkupRule)
    for col, val in (
        (MarkupRule.tenant_id, tenant_id),
        (MarkupRule.provider, provider),
        (MarkupRule.model, model),
        (MarkupRule.service_key, service_key),
        (MarkupRule.license_id, license_id),
        (MarkupRule.project_id, project_id),
    ):
        stmt = stmt.where(col.is_(None) if val is None else col == val)
    stmt = stmt.order_by(MarkupRule.effective_from.desc())
    return list((await session.execute(stmt)).scalars().all())


async def upsert_markup_rule(
    session: AsyncSession,
    payload: MarkupRuleCreate,
    *,
    ts: datetime | None = None,
) -> tuple[MarkupRule, MarkupRule | None]:
    """Close the open row for the same match-tuple and insert a new one.

    Mirrors :func:`services.pricing.upsert_pricing` — the open-row lookup scopes
    by ALL SEVEN match dimensions (each in its NULL or set form) so editing the
    ``(tenant, app, provider, model)`` rule never disturbs the global default or
    any other tuple. The closed prior row is returned for audit before/after.

    ⛔ THE LOOP BELOW MUST MATCH ``ix_markuprules_open`` TERM FOR TERM. That
    index uniquifies seven coalesced dimensions; any dimension present there and
    ABSENT here is a silent close of a rule the writer never named.

    ⚠️ ``byo`` MUST be in the loop below, not only in ``ix_markuprules_open``.
    The partial unique would reject a duplicate either way, but the failure modes
    differ and one of them is SILENT: without ``byo`` here, writing a byo-scoped
    rule first FINDS the byo-agnostic open row, CLOSES it, and inserts — so
    "set BYO to 20%" retires the tenant's general markup rule and changes the
    rate on non-BYO lines too, returning 201 with an audit trail that records it
    as an unrelated rule's creation.

    ⚠️ ``project_id`` (0036) is in the loop for EXACTLY the same reason, and its
    blast radius is wider than ``byo``'s. Without it, writing "project X = 20%"
    FINDS the tenant-wide open row, CLOSES it, and inserts — so pricing ONE
    kanban project silently retires the tenant's baseline markup and re-prices
    EVERY OTHER PROJECT for that tenant, again returning 201 with an audit trail
    that records it as an unrelated rule's creation. Regression-pinned by
    ``tests/test_markup.py::
    test_upsert_project_rule_does_not_close_the_tenant_wide_rule``.
    """
    closed_at = ts or datetime.now(UTC)

    open_stmt = select(MarkupRule).where(MarkupRule.effective_until.is_(None))
    for col, val in (
        (MarkupRule.tenant_id, payload.tenant_id),
        (MarkupRule.license_id, payload.license_id),
        (MarkupRule.service_key, payload.service_key),
        (MarkupRule.provider, payload.provider),
        (MarkupRule.model, payload.model),
        (MarkupRule.byo, payload.byo),
        # ⭐ 0036. APPENDED LAST, mirroring `ix_markuprules_open`'s own term
        # order (models/db/config.py) and alembic 0036's
        # `_CREATE_OPEN_IDX_WITH_PROJECT`, so a reader diffing the loop against
        # the index finds them identical rather than having to prove two
        # orderings equivalent. See the ⚠️ in the docstring for what its
        # absence would silently close.
        (MarkupRule.project_id, payload.project_id),
    ):
        open_stmt = open_stmt.where(col.is_(None) if val is None else col == val)

    prior = (await session.execute(open_stmt)).scalar_one_or_none()
    if prior is not None:
        prior.effective_until = closed_at

    new_row = MarkupRule(
        tenant_id=payload.tenant_id,
        license_id=payload.license_id,
        project_id=payload.project_id,
        service_key=payload.service_key,
        provider=payload.provider,
        model=payload.model,
        byo=payload.byo,
        markup_pct=payload.markup_pct,
        surcharge_input_per_1m_usd=payload.surcharge_input_per_1m_usd,
        surcharge_output_per_1m_usd=payload.surcharge_output_per_1m_usd,
        surcharge_cache_creation_per_1m_usd=payload.surcharge_cache_creation_per_1m_usd,
        surcharge_cache_read_per_1m_usd=payload.surcharge_cache_read_per_1m_usd,
        surcharge_per_request_usd=payload.surcharge_per_request_usd,
        effective_from=payload.effective_from or closed_at,
        effective_until=payload.effective_until,
        notes=payload.notes,
    )
    session.add(new_row)
    await session.flush()
    return new_row, prior


def markup_to_response_dict(row: MarkupRule) -> dict[str, Any]:
    """Plain dict view of a MarkupRule — ready for MarkupRuleResponse."""
    return {
        "id": row.id,
        "tenant_id": row.tenant_id,
        "license_id": row.license_id,
        "project_id": row.project_id,
        "service_key": row.service_key,
        "provider": row.provider,
        "model": row.model,
        "byo": row.byo,
        "markup_pct": row.markup_pct,
        "surcharge_input_per_1m_usd": row.surcharge_input_per_1m_usd,
        "surcharge_output_per_1m_usd": row.surcharge_output_per_1m_usd,
        "surcharge_cache_creation_per_1m_usd": row.surcharge_cache_creation_per_1m_usd,
        "surcharge_cache_read_per_1m_usd": row.surcharge_cache_read_per_1m_usd,
        "surcharge_per_request_usd": row.surcharge_per_request_usd,
        "effective_from": row.effective_from,
        "effective_until": row.effective_until,
        "notes": row.notes,
        "specificity": _specificity(row),
    }
