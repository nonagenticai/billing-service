"""Configuration tables: pricing, per-app cost rates, per-tenant markup."""

from __future__ import annotations

import uuid
from datetime import datetime
from decimal import Decimal

from sqlalchemy import (
    CHAR,
    BigInteger,
    Boolean,
    CheckConstraint,
    DateTime,
    Index,
    Numeric,
    String,
    Text,
    Uuid,
    text,
)
from sqlalchemy.orm import Mapped, mapped_column

from db import Base

from ._common import DBUUID, _new_uuid, _utcnow


class ProviderCost(Base):
    """Cost rates per LLM provider (editable via admin API)."""

    __tablename__ = "billing_provider_costs"

    id: Mapped[uuid.UUID] = mapped_column(DBUUID(), primary_key=True, default=_new_uuid)
    provider: Mapped[str] = mapped_column(String(64), nullable=False, unique=True)
    input_per_1m_usd: Mapped[Decimal] = mapped_column(Numeric(10, 4), nullable=False, default=0)
    output_per_1m_usd: Mapped[Decimal] = mapped_column(Numeric(10, 4), nullable=False, default=0)
    updated_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False, default=_utcnow
    )


class AppCostRate(Base):
    """Per-app-key cost rate for non-LLM services."""

    __tablename__ = "billing_app_cost_rates"

    id: Mapped[uuid.UUID] = mapped_column(DBUUID(), primary_key=True, default=_new_uuid)
    app_key: Mapped[str] = mapped_column(String(64), nullable=False, unique=True)
    cost_per_request_usd: Mapped[Decimal] = mapped_column(Numeric(12, 8), nullable=False, default=0)
    updated_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False, default=_utcnow
    )


class TenantConfig(Base):
    """Per-tenant configuration (markup overrides, etc.)."""

    __tablename__ = "tenant_config"

    id: Mapped[uuid.UUID] = mapped_column(DBUUID(), primary_key=True, default=_new_uuid)
    # tenant_id is a real Postgres `uuid` column (alembic 0011). as_uuid=False is
    # deliberate: the DB type is uuid while Python keeps receiving/binding `str`, so
    # every existing comparison, log line and JSON serialisation keeps working.
    tenant_id: Mapped[str] = mapped_column(Uuid(as_uuid=False), nullable=False, unique=True)
    tenant_name: Mapped[str] = mapped_column(String(255), nullable=False, server_default="")
    # NULL = "this tenant's markup was never configured" (alembic 0044): the
    # money-budget PUT creates this row only to flip budget_unit and must not
    # record a markup nobody chose. Every reader treats NULL exactly like "no
    # row" (services.billing.get_markup_pct_with_source falls through).
    markup_pct: Mapped[Decimal | None] = mapped_column(Numeric(6, 2), nullable=True, default=30)
    # Billing mode — the OUTER gate over a resolved markup_rules row. Plain
    # VARCHAR for forward-compat (same rationale as provider_pricing.cost_model):
    #   'standard_markup'  — billed = incurred*(1+pct) + absolute surcharge.
    #   'byo_platform_fee' — BYO LLM lines bill at the absolute surcharge only
    #                        (pct forced to 0; provider cost is a pass-through
    #                        estimate); non-BYO/platform lines stay standard.
    #   'pass_through'     — markup 0 everywhere (estimate-only invoice).
    # generate_billing acts on this per line via services.markup.compute_billed_line
    # (effective_billing_mode → the mode branches); all three modes are live and
    # covered by tests/test_markup_billing_consume.py + tests/test_byo_mode_gate.py.
    billing_mode: Mapped[str] = mapped_column(
        String(32), nullable=False, server_default="standard_markup"
    )
    # What this tenant's budget is denominated in (alembic 0044). 'tokens' (the
    # default, every pre-0044 tenant) keeps the token_budgets path; 'money'
    # routes settlement of IR cost events carrying usage_extra.setup to
    # tenant_money_budget and SKIPS the token debit. Flipped to 'money' by
    # PUT /api/budget/money/{tenant_id}.
    budget_unit: Mapped[str] = mapped_column(String(8), nullable=False, server_default="tokens")
    notes: Mapped[str | None] = mapped_column(Text, nullable=True)
    updated_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False, default=_utcnow
    )

    __table_args__ = (
        CheckConstraint("budget_unit IN ('tokens', 'money')", name="ck_tenant_config_budget_unit"),
    )


class FxRate(Base):
    """Owner-set, effective-dated FX rate USD -> EUR | CAD (alembic 0044/0045).

    There is never a USD -> USD row: a USD budget prices at exactly 1.

    Resolution is "the row effective at the event time" (newest
    ``effective_from <= ts`` whose ``effective_until`` is NULL or later). No row
    is a REFUSAL (``fx_rate_missing``), never an assumed rate. Written by
    ``PUT /api/config/fx-rates``, which closes the open row at the new row's
    ``effective_from``.
    """

    __tablename__ = "fx_rate"

    id: Mapped[int] = mapped_column(BigInteger, primary_key=True, autoincrement=True)
    base: Mapped[str] = mapped_column(CHAR(3), nullable=False, server_default="USD")
    quote: Mapped[str] = mapped_column(CHAR(3), nullable=False, server_default="EUR")
    rate: Mapped[Decimal] = mapped_column(Numeric(20, 10), nullable=False)
    effective_from: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)
    effective_until: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    set_by: Mapped[str | None] = mapped_column(Text, nullable=True)

    __table_args__ = (
        CheckConstraint("rate > 0", name="ck_fx_rate_positive"),
        # alembic 0045: rates are USD -> X only; USD itself is implicitly 1.
        CheckConstraint("base = 'USD' AND quote IN ('EUR', 'CAD')", name="ck_fx_rate_pair"),
        Index("ix_fx_rate_lookup", "base", "quote", "effective_from"),
    )


class MarkupRule(Base):
    """Effective-dated markup rules — the north-star billing markup source.

    Mirrors :class:`ProviderPricing`'s shape (NULL = wildcard on each match
    dimension, effective-dated, one open row per match-tuple) but resolves on
    the *billing* side rather than the cost side. A usage line is billed as::

        billed = incurred * (1 + (markup_pct or 0)/100)
                 + Σ(tokens * surcharge_rate / 1e6)
                 + request_count * surcharge_per_request_usd

    Resolution is MOST-SPECIFIC-WINS with the **consumption axis dominant**
    (decided with George): specificity score
    ``byo=64 > model=32 > provider=16 > service_key=8 > license_id=4 >
    project_id=2 > tenant_id=1``.
    A fully-specified ``tenant+app+provider+model`` rule therefore outranks a
    flat ``tenant+license`` rule — see :func:`services.markup.resolve_markup_rule`.

    The seed migration inserts one global-default row (all dims NULL) so a
    lookup never misses; ``markup_pct`` is nullable because a rule may carry
    only an absolute surcharge (e.g. a per-token platform fee on a $0 BYO line).
    """

    __tablename__ = "markup_rules"

    id: Mapped[int] = mapped_column(BigInteger, primary_key=True, autoincrement=True)
    # Match dimensions. NULL = wildcard ("applies to any"). tenant_id NULL is
    # the global default (supersedes the legacy '__default__' string sentinel).
    tenant_id: Mapped[str | None] = mapped_column(Uuid(as_uuid=False), nullable=True)
    license_id: Mapped[uuid.UUID | None] = mapped_column(DBUUID(), nullable=True)
    # ⭐ THE KANBAN-PROJECT AXIS (0036). NULL = tenant-wide (applies to every
    # project); a value scopes the rule to that one kanban project.
    #
    # Weighted BETWEEN license_id and tenant_id in
    # :func:`services.markup._specificity` (4 > 2 > 1), which is the operator's
    # decision: a project rule overrides the tenant-wide default, and a
    # per-licence/app rule still overrides the project rule. That slot did not
    # exist before — the old ladder ran 32/16/8/4/2/1 with nothing between
    # LICENSE and TENANT — so every weight was doubled to free it. See the
    # monotonicity note above `_W_BYO` for why that re-prices nothing.
    #
    # ⛔ A project-pinned rule with a NULL tenant_id NEVER MATCHES. Project ids
    # are minted by kanban, not here, so a tenantless project rule would reprice
    # any tenant that happened to carry the same project id;
    # `services.markup._matches` refuses that shape outright.
    project_id: Mapped[uuid.UUID | None] = mapped_column(DBUUID(), nullable=True)
    service_key: Mapped[str | None] = mapped_column(String(64), nullable=True)
    provider: Mapped[str | None] = mapped_column(String(64), nullable=True)
    # Matches cost_events.served_model (authoritative).
    model: Mapped[str | None] = mapped_column(String(128), nullable=True)
    # ⭐ THE COMMERCIAL AXIS. NULL = applies to both BYO and platform-provided
    # lines; True = BYO only; False = platform-provided only. Matches
    # billing_cost_daily.byo / cost_events.usage_extra.byo.
    #
    # Weighted ABOVE `model` in :func:`services.markup._specificity` (64 vs 32):
    # "what we charge on a customer's own key" is a term of the contract, and a
    # model-pinned rate card must not silently outrank it. Before this column a
    # tenant's BYO rate was not expressible AT ALL -- services.markup forced the
    # percentage to 0 on every BYO line under billing_mode byo_platform_fee, so
    # the only BYO knob was the absolute surcharge.
    byo: Mapped[bool | None] = mapped_column(Boolean, nullable=True)
    # Relative markup. Nullable — a surcharge-only rule sets this NULL/0.
    markup_pct: Mapped[Decimal | None] = mapped_column(Numeric(6, 2), nullable=True)
    # Absolute surcharges (the platform-fee component), mirroring the
    # provider_pricing per-1M shape so the math reuses services.pricing helpers.
    surcharge_input_per_1m_usd: Mapped[Decimal | None] = mapped_column(
        Numeric(20, 8), nullable=True
    )
    surcharge_output_per_1m_usd: Mapped[Decimal | None] = mapped_column(
        Numeric(20, 8), nullable=True
    )
    surcharge_cache_creation_per_1m_usd: Mapped[Decimal | None] = mapped_column(
        Numeric(20, 8), nullable=True
    )
    surcharge_cache_read_per_1m_usd: Mapped[Decimal | None] = mapped_column(
        Numeric(20, 8), nullable=True
    )
    surcharge_per_request_usd: Mapped[Decimal | None] = mapped_column(Numeric(20, 8), nullable=True)
    effective_from: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)
    effective_until: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    notes: Mapped[str | None] = mapped_column(Text, nullable=True)

    __table_args__ = (
        Index(
            "ix_markuprules_lookup",
            # tenant_id is uuid-typed (alembic 0011) and `coalesce(uuid, '')` has no
            # type resolution in Postgres, so the index cannot be built. Cast to text
            # first — same treatment license_id already gets below.
            text("coalesce(tenant_id::text, '')"),
            "provider",
            "model",
            effective_from.desc(),
        ),
        # At most one OPEN row per full match-tuple. coalesce the nullable dims
        # to a sentinel so the unique also covers the all-wildcard default row.
        Index(
            "ix_markuprules_open",
            text("coalesce(tenant_id::text, '')"),
            text("coalesce(license_id::text, '')"),
            text("coalesce(service_key, '')"),
            text("coalesce(provider, '')"),
            text("coalesce(model, '')"),
            # `byo` joins the tuple (0027). Without it a byo-scoped rule and the
            # byo-agnostic rule it refines collide on the same open slot, and the
            # upsert would CLOSE the general rule to insert the specific one --
            # silently changing the rate on non-BYO lines too.
            text("coalesce(byo::text, '')"),
            # `project_id` joins the tuple as the 7th term (0036), for the same
            # reason `byo` did in 0027: without it a project-scoped rule and the
            # project-agnostic rule it refines collide on the same open slot, so
            # `services.markup.upsert_markup_rule` would CLOSE the tenant-wide
            # rule in order to insert the project one — silently changing the
            # rate on every other project too.
            #
            # ⚠️ APPENDED LAST, MATCHING alembic 0036's
            # `_CREATE_OPEN_IDX_WITH_PROJECT` TERM-FOR-TERM AND IN ORDER. Term
            # ORDER does not change what the unique enforces (same set of terms
            # = same constraint), but the migration's own comment says it
            # "mirrors models/db/config.py", so a reader diffing the two must
            # find them identical rather than having to prove two different
            # orderings equivalent.
            text("coalesce(project_id::text, '')"),
            unique=True,
            postgresql_where=text("effective_until IS NULL"),
        ),
    )


class TenantAccessCharge(Base):
    """Effective-dated per-(tenant, product) ACCESS charge — the access-billing axis.

    The ACCESS axis, distinct from the USAGE axis (``cost_events`` + ``markup_rules``):
    a fixed recurring/one-time fee a tenant pays for *access* to a product (the "paid
    license" concept), independent of how many tokens they consume. It is billed in
    :func:`services.billing.generate_billing` as a standalone line that folds into
    ``customer_cost`` + ``margin`` — it has **no internal/provider cost behind it**
    (pure access revenue), so it never touches ``internal_cost``.

    Mirrors :class:`MarkupRule`'s effective-dated, one-open-row-per-match-tuple shape,
    but the match tuple is only ``(tenant_id, service_key)`` — an access charge is
    inherently tenant-scoped and product-scoped, not resolved along the consumption
    axis. Templated from ``licenses.Offering.access_price_usd`` and provisioned
    per-tenant, exactly as ``markup_rules`` are provisioned from an offering.

    ``cadence``:
      - ``'monthly'``  — billed once per queried month while the row is effective.
      - ``'one_time'`` — billed only in its ``effective_from`` month.

    ``included_cost_usd`` is the **allowance** column — an included-usage credit that
    offsets incurred usage *before* markup (NULL = no allowance). For ``monthly`` it is
    a recurring credit that resets each billed month; for ``one_time`` it is a durable
    prepaid credit drawn down across months until exhausted (netted in
    :func:`services.billing.generate_billing` via
    :func:`services.access_charge.one_time_remaining_before`).
    """

    __tablename__ = "tenant_access_charge"

    id: Mapped[int] = mapped_column(BigInteger, primary_key=True, autoincrement=True)
    # Always set — an access charge is inherently tenant-scoped.
    tenant_id: Mapped[str] = mapped_column(Uuid(as_uuid=False), nullable=False)
    # NULL = a tenant-wide access charge (all products); a value scopes it to one
    # product, matched against a license's app_key at bill time.
    service_key: Mapped[str | None] = mapped_column(String(64), nullable=True)
    amount_usd: Mapped[Decimal] = mapped_column(Numeric(20, 8), nullable=False)
    # Plain VARCHAR for forward-compat (same rationale as provider_pricing.cost_model).
    cadence: Mapped[str] = mapped_column(String(16), nullable=False, server_default="monthly")
    # Forward-compat allowance (included-usage credit offsetting incurred usage before
    # markup). Unused until the allowance-netting slice.
    included_cost_usd: Mapped[Decimal | None] = mapped_column(Numeric(20, 8), nullable=True)
    effective_from: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)
    effective_until: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    notes: Mapped[str | None] = mapped_column(Text, nullable=True)

    __table_args__ = (
        Index(
            "ix_accesscharge_lookup",
            "tenant_id",
            text("coalesce(service_key, '')"),
            effective_from.desc(),
        ),
        # At most one OPEN row per (tenant, service_key). coalesce the nullable
        # service_key so the tenant-wide (NULL) charge is also covered.
        Index(
            "ix_accesscharge_open",
            "tenant_id",
            text("coalesce(service_key, '')"),
            unique=True,
            postgresql_where=text("effective_until IS NULL"),
        ),
    )


class ProviderPricing(Base):
    """Effective-dated provider pricing — metering's authoritative cost source.

    `cost_model` is a plain VARCHAR for forward-compat; current values are
    'per_token', 'per_request', 'self_hosted_amortized'. Postgres ENUMs are
    painful to evolve so we deliberately use a string.
    """

    __tablename__ = "provider_pricing"

    id: Mapped[int] = mapped_column(BigInteger, primary_key=True, autoincrement=True)
    provider: Mapped[str] = mapped_column(String(64), nullable=False)
    # NULL = default rate for the provider when no per-model row matches.
    model: Mapped[str | None] = mapped_column(String(128), nullable=True)
    # NULL = applies to any service_key for this (provider, model). Set to
    # a specific service_key (e.g. 'kag-graph') to charge a different rate
    # for that consuming service while leaving the (provider, model, NULL)
    # row as the default for everything else. See migration 0009 + the
    # 4-rung precedence in services.pricing._resolve_pricing_row.
    service_key: Mapped[str | None] = mapped_column(String(64), nullable=True)
    # 'cost' = internal COST rate (metering's authoritative hot path — the only
    # class the cost resolver reads). 'list' = public LIST/provider rate, used
    # ONLY to show a BYO tenant an estimated *provider* cost at read time; it
    # never touches the cost hot path. Leading column of both indexes so open-row
    # uniqueness is per (rate_class, provider, model, service_key) — one open
    # 'cost' AND one open 'list' row may coexist per tuple.
    rate_class: Mapped[str] = mapped_column(String(16), nullable=False, server_default="cost")
    cost_model: Mapped[str] = mapped_column(String(32), nullable=False)
    input_per_1m_usd: Mapped[Decimal | None] = mapped_column(Numeric(20, 8), nullable=True)
    output_per_1m_usd: Mapped[Decimal | None] = mapped_column(Numeric(20, 8), nullable=True)
    # Anthropic-style cache pricing. NULL falls back to input_per_1m_usd.
    cache_creation_per_1m_usd: Mapped[Decimal | None] = mapped_column(Numeric(20, 8), nullable=True)
    cache_read_per_1m_usd: Mapped[Decimal | None] = mapped_column(Numeric(20, 8), nullable=True)
    per_request_usd: Mapped[Decimal | None] = mapped_column(Numeric(20, 8), nullable=True)
    monthly_fixed_cost_usd: Mapped[Decimal | None] = mapped_column(Numeric(20, 8), nullable=True)
    # cost_basis: the PROVIDER CONTRACT fact — 'cash' (a real vendor charge) or
    # 'internal_transfer' (a book entry on our own hardware, no cash leaves the
    # business). Deliberately NOT derived from cost_model — per_token/per_
    # second/per_character say HOW WE METER, not WHETHER CASH LEAVES THE
    # BUILDING (alembic 0022; every self-hosted provider metering per_token/
    # per_character is still internal_transfer). Defaults internal_transfer:
    # every provider live today is self-hosted, so silence must not read as
    # "real money" — see 0022's module docstring.
    cost_basis: Mapped[str] = mapped_column(
        String(20), nullable=False, server_default="internal_transfer"
    )
    effective_from: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)
    effective_until: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    notes: Mapped[str | None] = mapped_column(Text, nullable=True)

    __table_args__ = (
        # Lookup index — most-recent-effective first per
        # (rate_class, provider, model). rate_class leads so the cost hot path
        # (rate_class='cost') and list lookups each hit a tight index prefix.
        Index(
            "ix_providerpricing_lookup",
            "rate_class",
            "provider",
            "model",
            effective_from.desc(),
        ),
        # Partial unique: at most one open-ended row per
        # (rate_class, provider, model, service_key). `model` and `service_key`
        # are nullable; coalesce to '' so the unique works for the
        # provider-default + service-default rows too.
        Index(
            "ix_providerpricing_open",
            "rate_class",
            "provider",
            text("coalesce(model, '')"),
            text("coalesce(service_key, '')"),
            unique=True,
            postgresql_where=text("effective_until IS NULL"),
        ),
    )


#: The bounded domain of ``AttributionRule.match_project``. Mirrors the CHECK in
#: alembic 0041 / the model below.
ATTRIBUTION_MATCH_PROJECT_VALUES: tuple[str, ...] = ("any", "with_project", "without_project")


class AttributionRule(Base):
    """Re-attribute cost events from a source tenant to a bill tenant + member (0041).

    Applied on the COST ingest path by ``services.attribution_rules`` (called
    from ``services.cost_flusher``). A rule matches an event whose ``tenant_id``
    is ``source_tenant_id`` and — when set — whose ``subject_id`` is
    ``source_subject_id`` (NULL = any subject). ``match_project`` narrows on the
    RESOLVED project: ``with_project`` only when a real project id is present,
    ``without_project`` only when none is (NULL or ``NULL_PROJECT_ID``), ``any``
    always. Precedence: subject-specific over tenant-wide, then a
    project-specific matcher over ``any``.

    On match the event's ``tenant_id`` becomes ``bill_tenant_id``, its
    ``actor_id`` becomes ``COALESCE(actor_id, rule.actor_id)`` (a real actor is
    never overwritten), and ``metadata`` records ``attributed_from_tenant_id`` +
    ``attribution_rule_id``. Forward-only; no stored row is ever rewritten.
    """

    __tablename__ = "billing_attribution_rules"

    id: Mapped[uuid.UUID] = mapped_column(DBUUID(), primary_key=True, default=_new_uuid)
    source_tenant_id: Mapped[str] = mapped_column(Uuid(as_uuid=False), nullable=False)
    # NULL = any subject under the source tenant.
    source_subject_id: Mapped[uuid.UUID | None] = mapped_column(DBUUID(), nullable=True)
    match_project: Mapped[str] = mapped_column(Text, nullable=False)
    bill_tenant_id: Mapped[str] = mapped_column(Uuid(as_uuid=False), nullable=False)
    # The member to bill as. NULL = re-tenant only, leave actor_id as it arrived.
    actor_id: Mapped[uuid.UUID | None] = mapped_column(DBUUID(), nullable=True)
    reason: Mapped[str] = mapped_column(Text, nullable=False)
    enabled: Mapped[bool] = mapped_column(Boolean, nullable=False, server_default=text("true"))
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False, server_default=text("now()")
    )

    __table_args__ = (
        CheckConstraint(
            "match_project IN ('any', 'with_project', 'without_project')",
            name="ck_attribution_rules_match_project",
        ),
        # ⚠️ Term-for-term the same as alembic 0041's `_CREATE_RULES_UNIQUE`.
        Index(
            "uq_attribution_rules_match",
            "source_tenant_id",
            text("coalesce(source_subject_id, '00000000-0000-0000-0000-000000000000'::uuid)"),
            "match_project",
            unique=True,
        ),
    )
