"""Aggregated rollup tables (permanent).

Hourly + daily request rollups, daily cost rollup, monthly quota tracking.
All have UNIQUE constraints on the (tenant, license, subject, app, plan,
service_key, bucket) tuple so the rollup engine can ON CONFLICT upsert.
"""

from __future__ import annotations

import uuid
from datetime import date, datetime
from decimal import Decimal

from sqlalchemy import (
    BigInteger,
    Boolean,
    Date,
    DateTime,
    Index,
    Integer,
    Numeric,
    String,
    UniqueConstraint,
    Uuid,
    and_,
    text,
)
from sqlalchemy.orm import Mapped, mapped_column

from db import Base

from ._common import (
    ATTRIBUTION_SYSTEM,
    ATTRIBUTION_UNKNOWN,
    ATTRIBUTION_UNLICENSED,
    DBUUID,
    NULL_ACTOR_ID,
    NULL_APP_ID,
    NULL_LICENSE_ID,
    NULL_PLAN_ID,
    NULL_PROJECT_ID,
    NULL_SUBJECT_ID,
    _new_uuid,
)


class MeteringHourly(Base):
    """Hourly rollup of API request events."""

    __tablename__ = "billing_metering_hourly"

    id: Mapped[uuid.UUID] = mapped_column(DBUUID(), primary_key=True, default=_new_uuid)
    # tenant_id is a real Postgres `uuid` column (alembic 0011). as_uuid=False is
    # deliberate: the DB type is uuid while Python keeps receiving/binding `str`, so
    # every existing comparison, log line and JSON serialisation keeps working.
    tenant_id: Mapped[str] = mapped_column(Uuid(as_uuid=False), nullable=False)
    tenant_name: Mapped[str] = mapped_column(String(255), nullable=False, server_default="")
    license_id: Mapped[uuid.UUID] = mapped_column(DBUUID(), nullable=False, default=NULL_LICENSE_ID)
    subject_id: Mapped[uuid.UUID] = mapped_column(
        DBUUID(),
        nullable=False,
        default=NULL_SUBJECT_ID,
        server_default=text("'00000000-0000-0000-0000-000000000003'::uuid"),
    )
    app_id: Mapped[uuid.UUID] = mapped_column(DBUUID(), nullable=False, default=NULL_APP_ID)
    plan_id: Mapped[uuid.UUID] = mapped_column(DBUUID(), nullable=False, default=NULL_PLAN_ID)
    service_key: Mapped[str] = mapped_column(String(64), nullable=False)
    bucket_hour: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)
    request_count: Mapped[int] = mapped_column(Integer, nullable=False, default=0)
    denied_count: Mapped[int] = mapped_column(Integer, nullable=False, default=0)
    budget_denied_count: Mapped[int] = mapped_column(
        Integer, nullable=False, default=0, server_default="0"
    )

    __table_args__ = (
        UniqueConstraint(
            "tenant_id",
            "license_id",
            "subject_id",
            "app_id",
            "plan_id",
            "service_key",
            "bucket_hour",
            name="uq_hourly",
        ),
        Index("ix_metering_hourly_tenant_name", "tenant_name"),
    )


class MeteringDaily(Base):
    """Daily rollup of API request events."""

    __tablename__ = "billing_metering_daily"

    id: Mapped[uuid.UUID] = mapped_column(DBUUID(), primary_key=True, default=_new_uuid)
    tenant_id: Mapped[str] = mapped_column(Uuid(as_uuid=False), nullable=False)
    tenant_name: Mapped[str] = mapped_column(String(255), nullable=False, server_default="")
    license_id: Mapped[uuid.UUID] = mapped_column(DBUUID(), nullable=False, default=NULL_LICENSE_ID)
    subject_id: Mapped[uuid.UUID] = mapped_column(
        DBUUID(),
        nullable=False,
        default=NULL_SUBJECT_ID,
        server_default=text("'00000000-0000-0000-0000-000000000003'::uuid"),
    )
    app_id: Mapped[uuid.UUID] = mapped_column(DBUUID(), nullable=False, default=NULL_APP_ID)
    plan_id: Mapped[uuid.UUID] = mapped_column(DBUUID(), nullable=False, default=NULL_PLAN_ID)
    service_key: Mapped[str] = mapped_column(String(64), nullable=False)
    bucket_date: Mapped[date] = mapped_column(Date, nullable=False)
    request_count: Mapped[int] = mapped_column(Integer, nullable=False, default=0)
    denied_count: Mapped[int] = mapped_column(Integer, nullable=False, default=0)
    budget_denied_count: Mapped[int] = mapped_column(
        Integer, nullable=False, default=0, server_default="0"
    )

    __table_args__ = (
        UniqueConstraint(
            "tenant_id",
            "license_id",
            "subject_id",
            "app_id",
            "plan_id",
            "service_key",
            "bucket_date",
            name="uq_daily",
        ),
        Index("ix_metering_daily_tenant_name", "tenant_name"),
    )


class CostDaily(Base):
    """Daily rollup of cost events."""

    __tablename__ = "billing_cost_daily"

    id: Mapped[uuid.UUID] = mapped_column(DBUUID(), primary_key=True, default=_new_uuid)
    tenant_id: Mapped[str] = mapped_column(Uuid(as_uuid=False), nullable=False)
    tenant_name: Mapped[str] = mapped_column(String(255), nullable=False, server_default="")
    # NOT NULL: application code always substitutes NULL_LICENSE_ID for None
    license_id: Mapped[uuid.UUID] = mapped_column(DBUUID(), nullable=False, default=NULL_LICENSE_ID)
    # NOT NULL: application code substitutes NULL_SUBJECT_ID for None.
    subject_id: Mapped[uuid.UUID] = mapped_column(
        DBUUID(),
        nullable=False,
        default=NULL_SUBJECT_ID,
        server_default=text("'00000000-0000-0000-0000-000000000003'::uuid"),
    )
    # The kanban project this day's spend belongs to, so cost can be priced per
    # project. NOT NULL with a SENTINEL, exactly like `subject_id` above and like
    # `plan_id` on MeteringMonthlyUsage (:341) — and deliberately NOT
    # `nullable=True`, even though the raw source columns
    # (`events.MeteringEvent.project_id`, `events.CostEvent.project_id`) ARE
    # nullable.
    #
    # ⛔ MAKING THIS COLUMN NULLABLE WOULD DISARM THE ONLY DB-SIDE GUARD ON THIS
    # TABLE'S GRAIN. `uq_cost_daily` is a plain UNIQUE constraint and Postgres
    # treats two NULLs as DISTINCT in a unique key (the default, `NULLS
    # DISTINCT`), so a NULL here never matches itself: the constraint would stop
    # refusing duplicate project-less rows from ANY writer, and it is the only
    # object that refuses them. A sentinel uuid collides with itself, so the key
    # keeps enforcing exactly what it enforced before 0036.
    #
    # ⚠️ AND THE OBVIOUS RATIONALE IS THE WRONG ONE — do not restate it. This
    # comment used to say a NULL would make "every rollup cycle INSERT a fresh
    # row instead of updating". It would not, today:
    # `services.rollup_engine.rollup_daily` runs
    # `delete(CostDaily).where(CostDaily.bucket_date == target_date)` under the
    # guard `if cost_rows or cost_covered:` immediately above it, BEFORE the
    # insert, so a re-rollup replaces the whole day and never reaches
    # `ON CONFLICT` at all — that file's own comments on the
    # `on_conflict_do_update` block say so, and no test exercises the conflict
    # path.
    #
    # ⚠️ CITE IT BY SYMBOL AND GUARD, NOT BY LINE. This comment said "~:818";
    # the DELETE had already moved to ~841 and the
    # `on_conflict_do_update(constraint="uq_cost_daily")` to ~905, with the
    # mechanism unchanged. A line number fingerprints one revision — a named
    # guard survives edits.
    #
    # The real reason this column is NOT NULL is
    # defence-in-depth: `uq_cost_daily` IS the declared conflict target
    # (`on_conflict_do_update(constraint="uq_cost_daily")`), it covers the
    # residual concurrent-writer path, and if that DELETE were ever removed or
    # narrowed a NULL would fan the grain out silently, with nothing in the
    # schema refusing it.
    #
    # The sentinel means "no project was asserted for this spend", NOT "the
    # default project" and not any real project — see NULL_PROJECT_ID in
    # models/db/_common.py. Rollup code substitutes it for None, the same way it
    # already does for NULL_LICENSE_ID / NULL_SUBJECT_ID / NULL_APP_ID /
    # NULL_PLAN_ID.
    project_id: Mapped[uuid.UUID] = mapped_column(
        DBUUID(),
        nullable=False,
        default=NULL_PROJECT_ID,
        server_default=text("'00000000-0000-0000-0000-000000000004'::uuid"),
    )
    service_key: Mapped[str] = mapped_column(String(64), nullable=False)
    provider: Mapped[str] = mapped_column(String(64), nullable=False)
    # The model that actually served the request (cost_events.served_model).
    # Carried into the rollup so per-model markup resolves at billing time.
    # Sentinel '__unknown__' for events that predate served_model or emit NULL.
    served_model: Mapped[str] = mapped_column(
        String(128), nullable=False, default="__unknown__", server_default="__unknown__"
    )
    # WHY the row's license_id is (or is not) the sentinel -- see
    # ``_common.classify_cost_attribution``. DERIVED at rollup time from the local
    # licence mirror; no emitter supplies it. Deliberately NOT part of
    # ``uq_cost_daily``: it is functionally determined by columns already in that
    # key (license_id + tenant_id), so adding it would split no group while
    # changing the ON CONFLICT shape of every existing row. 'unknown' is the
    # pre-migration value and is never written by code.
    attribution_status: Mapped[str] = mapped_column(
        String(16),
        nullable=False,
        default=ATTRIBUTION_UNKNOWN,
        server_default=ATTRIBUTION_UNKNOWN,
    )
    # Bring-your-own-key line (cost_events.usage_extra.byo). Gates the
    # byo_platform_fee billing mode. Default false for ordinary platform spend.
    byo: Mapped[bool] = mapped_column(
        Boolean, nullable=False, default=False, server_default=text("false")
    )
    # Managed-BYO / owner-provided line (cost_events.usage_extra.owner_provided):
    # the platform owner supplied the external key, so the line bills as a normal
    # marked-up cost (distinct from a tenant-supplied `byo` line, which the
    # byo_platform_fee mode zeroes the markup on). Rolled up as its own dimension
    # so owner-provided spend is attributable separately.
    owner_provided: Mapped[bool] = mapped_column(
        Boolean, nullable=False, default=False, server_default=text("false")
    )
    cost_source: Mapped[str] = mapped_column(
        String(64), nullable=False, default="unknown", server_default="unknown"
    )
    bucket_date: Mapped[date] = mapped_column(Date, nullable=False)
    total_input_tokens: Mapped[int] = mapped_column(BigInteger, nullable=False, default=0)
    total_output_tokens: Mapped[int] = mapped_column(BigInteger, nullable=False, default=0)
    # Numeric(20, 6): the old Numeric(12, 6) capped a per-tuple daily aggregate
    # at $999,999.999999 — a high-volume tenant's daily total for one group could
    # overflow the uq_cost_daily upsert and fail the day's rollup (the invoice
    # source). Widened to match the scale-8 source (billing_cost_events.cost_usd).
    total_cost_usd: Mapped[Decimal] = mapped_column(Numeric(20, 6), nullable=False, default=0)
    # ⭐ The PROVIDER's own reported charge summed over the grain (alembic 0040), from
    # CostEvent.vendor_cost_usd. NULL = no event in the group reported one — NOT $0, and never the
    # price-card figure. generate_billing prefers it over total_cost_usd when it is > 0.
    total_vendor_cost_usd: Mapped[Decimal | None] = mapped_column(Numeric(20, 8), nullable=True)
    request_count: Mapped[int] = mapped_column(Integer, nullable=False, default=0)
    # How many of this group's source rows carried `cost_usd IS NULL` — a PRICING
    # MISS, which is NOT a free call. `services.pricing` returns None rather than
    # guessing a rate ("Refuse instead: return None so the caller records a loud
    # pricing_miss", pricing.py), and `cost_flusher` stores that refusal as NULL
    # (see `events.CostEvent.cost_usd`: "NO default: NULL is the load-bearing
    # 'no ProviderPricing matched' sentinel").
    #
    # ⛔ WITHOUT THIS COLUMN THAT DISTINCTION DIED HERE, and `total_cost_usd`
    # could not carry it: SQL `sum()` skips NULLs, so the stored total holds only
    # the costs that ARE known, and when EVERY row of a group is unpriced the sum
    # is NULL and `rollup_engine`'s `coalesce(..., 0)` writes a hard $0.000000
    # beside a real token count — indistinguishable from usage an operator priced
    # free on purpose. `services/reprice.py` already names the shape (its
    # docstring: "an entirely unpriced provider/day lands in the rollup as a hard
    # 0.000000 ... Expressing that absence needs a nullable rollup column (or a
    # coverage flag beside it)"). This is that flag.
    #
    # ⚠️ LATENT WHEN ADDED, and it must not be described as more than that:
    # measured 2026-09-11, no row in `billing_cost_events` carries a NULL
    # `cost_usd`, so this reads 0 everywhere and no stored total is presently
    # wrong. It is added now because `billing_invoices` has 0 rows and because
    # the first unpriced provider on a paying customer would otherwise bill $0
    # onto an immutable, gaplessly-numbered invoice line with nothing recording
    # that the number was a guess.
    #
    # ⚠️ A COUNT, NOT A NULLABLE TOTAL, deliberately. Making `total_cost_usd`
    # nullable would express the same fact but move it into every reader —
    # `services.billing`, `routers.billing.compute_billing_overview`,
    # `services.reprice`, `routers.usage` all arithmetic on it today — so the
    # change that was meant to stop a silent $0 would instead spray `or 0`
    # fallbacks, which is the same collapse in more places. A count beside the
    # money keeps the existing contract intact and is strictly additive.
    #
    # Deliberately NOT part of `uq_cost_daily`: it is a property OF the rows in a
    # group, not a dimension of the group, so adding it would split no group
    # while rewriting the ON CONFLICT shape of every existing row (same reasoning
    # as `attribution_status` above, and alembic 0020's "NOT ADDED TO
    # uq_cost_daily" block).
    #
    # ⚠️ ON A ROW THAT PREDATES ALEMBIC 0031, 0 MEANS "NOT MEASURED", not "none
    # unpriced". 0031 ships no backfill on purpose: the only source is
    # `billing_cost_events`, which is RANGE-partitioned with retention and was
    # truncated 2026-09-05, so a figure computed for an older day would be an
    # undercount dressed as a measurement. The days whose source survives are
    # restated by `daily_reconcile_days`' own trailing re-roll.
    unpriced_event_count: Mapped[int] = mapped_column(
        Integer, nullable=False, default=0, server_default="0"
    )

    __table_args__ = (
        # ⚠️ ADDING `project_id` CHANGES THIS KEY'S DEDUP SEMANTICS, and that is
        # INTENDED: spend that previously aggregated into ONE tenant-level daily
        # row now SPLITS into one row per project. Daily row counts rise and any
        # reader that assumed one row per (tenant, license, subject, service,
        # provider, model, byo, owner_provided, cost_source, day) must aggregate
        # across projects to get the old figure. Totals are unchanged; the grain
        # is not.
        #
        # ⚠️ KNOWN GAP, RECORDED NOT FIXED. `uq_cost_daily` is the ONLY
        # database-side object enforcing this table's daily grain, and the
        # property it protects currently rests on ONE application-side
        # `delete(CostDaily).where(CostDaily.bucket_date == target_date)` in
        # `services.rollup_engine.rollup_daily`, itself behind
        # `if cost_rows or cost_covered:`. Nothing exercises the `ON CONFLICT`
        # path in the test suite, so that path's behaviour is UNVERIFIED: if a
        # future change narrows, guards away or removes that DELETE, the grain
        # fans out silently and the green suite says nothing. A totals check
        # cannot catch it either — duplicated grain preserves the sum.
        UniqueConstraint(
            "tenant_id",
            "license_id",
            "subject_id",
            "service_key",
            "provider",
            "served_model",
            "byo",
            "owner_provided",
            "cost_source",
            "bucket_date",
            "project_id",
            name="uq_cost_daily",
        ),
        Index("ix_cost_daily_tenant_name", "tenant_name"),
        # Serves the per-project spend query (a tenant's projects over a date
        # range) — unlike the raw event tables, this rollup IS grouped by
        # project, which is what earns the index here and denies it there.
        Index("ix_cost_daily_tenant_project_date", "tenant_id", "project_id", "bucket_date"),
    )

    @classmethod
    def license_attributed_clause(cls):
        """Canonical SQL predicate selecting only license-attributed cost rows,
        excluding the system/S2S sentinel license (``NULL_LICENSE_ID``).

        System/S2S cost is never attributed to a customer invoice line, so both
        the per-tenant invoice (``services.billing.generate_billing``) and the
        all-tenant overview (``routers.billing.compute_billing_overview``) must
        drop these rows. Shared here so the two paths cannot drift apart.
        """
        return and_(cls.license_id.is_not(None), cls.license_id != NULL_LICENSE_ID)

    @classmethod
    def unlicensed_attributable_clause(cls):
        """Rows the invoice clause above drops that are nonetheless a REAL
        customer's spend: no licence reached billing, but the billed tenant owns
        licences in the local mirror.

        This is the population ``license_attributed_clause`` used to write off
        indistinguishably from system cost. Measured on prod 2026-09-06:
        $3,386.52 across 22 licence-owning tenants (top: $2,138.58 / 250 rows).

        ⛔ NOT an invoice predicate. Nothing that produces a customer invoice line
        may call this -- deciding who PAYS for unlicensed spend is a business
        call, and this PR deliberately does not make it. It exists so the money
        is VISIBLE and attributable to a tenant.
        """
        return and_(
            cls.license_id == NULL_LICENSE_ID,
            cls.attribution_status == ATTRIBUTION_UNLICENSED,
        )

    @classmethod
    def system_cost_clause(cls):
        """Genuine system/S2S cost: no licence, and no customer relationship to
        attribute it to. Stays excluded from every customer aggregate -- this
        clause only makes the exclusion *nameable* and countable, so an operator
        can see that the invoice gate is dropping $266.28 of system cost rather
        than $3,652.80 of unexamined mixture.
        """
        return and_(
            cls.license_id == NULL_LICENSE_ID,
            cls.attribution_status == ATTRIBUTION_SYSTEM,
        )


class MeteringMonthlyUsage(Base):
    """Monthly quota tracking — request counts per license."""

    __tablename__ = "billing_metering_monthly_usage"

    id: Mapped[uuid.UUID] = mapped_column(DBUUID(), primary_key=True, default=_new_uuid)
    license_id: Mapped[uuid.UUID] = mapped_column(DBUUID(), nullable=False)
    # NOT NULL: application code substitutes NULL_SUBJECT_ID for None. Adding
    # subject_id to the upsert key keeps S2S/system rows on a separate row
    # from per-user rows for the same license.
    subject_id: Mapped[uuid.UUID] = mapped_column(
        DBUUID(),
        nullable=False,
        default=NULL_SUBJECT_ID,
        server_default=text("'00000000-0000-0000-0000-000000000003'::uuid"),
    )
    # NOT NULL with a SENTINEL, exactly like `subject_id` above and like
    # `plan_id` on MeteringHourly/MeteringDaily (:64, :104) — and deliberately
    # NOT `nullable=True`.
    #
    # ⛔ MAKING THIS COLUMN NULLABLE WOULD BREAK THE DE-DUPLICATION IT SITS IN.
    # `uq_monthly_usage` is a plain UNIQUE constraint, and Postgres treats two
    # NULLs as DISTINCT in a unique key (the default, `NULLS DISTINCT`). A
    # nullable plan_id would therefore let the flusher's
    # `ON CONFLICT ON CONSTRAINT uq_monthly_usage` MISS every plan-less row, so
    # each flush batch would INSERT a new row instead of incrementing the
    # existing one — the counter would fan out into one row per flush and the
    # readers below would sum duplicates. A sentinel uuid collides with itself,
    # so the upsert keeps working unchanged.
    #
    # The sentinel means "no plan was asserted for this usage", NOT "the free
    # plan" and not any real plan — see NULL_PLAN_ID in models/db/_common.py,
    # which `rollup_engine` already coalesces to for the hourly/daily grain.
    # Rows that DO carry a real plan_id keep their own grain; a mid-month plan
    # change still splits into two rows and is not flattened. The reader
    # (services/quota.py `_get_db_usage`) SUMs across the plan grain, so widening
    # the population of rows changes no aggregate's definition.
    plan_id: Mapped[uuid.UUID] = mapped_column(
        DBUUID(),
        nullable=False,
        default=NULL_PLAN_ID,
        server_default=text("'00000000-0000-0000-0000-000000000002'::uuid"),
    )
    month_start: Mapped[date] = mapped_column(Date, nullable=False)
    request_count: Mapped[int] = mapped_column(Integer, nullable=False, default=0)
    denied_count: Mapped[int] = mapped_column(Integer, nullable=False, default=0)
    budget_denied_count: Mapped[int] = mapped_column(
        Integer, nullable=False, default=0, server_default="0"
    )

    __table_args__ = (
        UniqueConstraint(
            "license_id", "subject_id", "plan_id", "month_start", name="uq_monthly_usage"
        ),
    )


class CostActorDaily(Base):
    """Daily cost per ACTING PERSON (alembic 0040) — the per-user axis that survives raw retention.

    ⭐ WHY A SEPARATE TABLE AND NOT A COLUMN ON ``CostDaily``. ``billing_cost_events.actor_id`` (WHO
    ACTED, the Keycloak sub) is distinct from ``subject_id`` (WHO PAYS). The raw table is retained
    for days, not forever, so an actor dimension that lives only there is lost at the next partition
    drop. Adding ``actor_id`` to ``uq_cost_daily`` would change the grain every invoice, overview and
    reprice reader is written against; a sibling table adds the axis and moves nothing.

    Reconciles to ``CostDaily`` exactly: same filters, same bucket date, so for any
    (tenant, bucket_date) the sums over this table equal the sums over ``CostDaily``.
    ``actor_id = NULL_ACTOR_ID`` is spend no person initiated (or an emitter that sent no actor).
    """

    __tablename__ = "billing_cost_actor_daily"

    id: Mapped[uuid.UUID] = mapped_column(DBUUID(), primary_key=True, default=_new_uuid)
    bucket_date: Mapped[date] = mapped_column(Date, nullable=False)
    tenant_id: Mapped[str] = mapped_column(Uuid(as_uuid=False), nullable=False)
    license_id: Mapped[uuid.UUID] = mapped_column(DBUUID(), nullable=False, default=NULL_LICENSE_ID)
    subject_id: Mapped[uuid.UUID] = mapped_column(DBUUID(), nullable=False, default=NULL_SUBJECT_ID)
    actor_id: Mapped[uuid.UUID] = mapped_column(DBUUID(), nullable=False, default=NULL_ACTOR_ID)
    service_key: Mapped[str] = mapped_column(String(64), nullable=False)
    provider: Mapped[str] = mapped_column(String(64), nullable=False)
    served_model: Mapped[str] = mapped_column(String(128), nullable=False, default="__unknown__")
    byo: Mapped[bool] = mapped_column(Boolean, nullable=False, default=False)
    owner_provided: Mapped[bool] = mapped_column(Boolean, nullable=False, default=False)
    total_input_tokens: Mapped[int] = mapped_column(BigInteger, nullable=False, default=0)
    total_output_tokens: Mapped[int] = mapped_column(BigInteger, nullable=False, default=0)
    total_cost_usd: Mapped[Decimal] = mapped_column(Numeric(20, 6), nullable=False, default=0)
    total_vendor_cost_usd: Mapped[Decimal | None] = mapped_column(Numeric(20, 8), nullable=True)
    request_count: Mapped[int] = mapped_column(Integer, nullable=False, default=0)

    __table_args__ = (
        UniqueConstraint(
            "bucket_date",
            "tenant_id",
            "license_id",
            "subject_id",
            "actor_id",
            "service_key",
            "provider",
            "served_model",
            "byo",
            "owner_provided",
            name="uq_cost_actor_daily",
        ),
        Index("ix_cost_actor_daily_tenant_actor_date", "tenant_id", "actor_id", "bucket_date"),
    )


class CostUsageDaily(Base):
    """The CROSS rollup (alembic 0053): tenant × project × actor × app × funding × provider × model.

    ``CostDaily`` has project but no actor; ``CostActorDaily`` has actor but no project; neither
    has funding or app. This one table carries every axis the ``/api/usage/breakdown`` read groups
    by, so a project × member × funding table is ONE aggregate instead of a join of two rollups
    that do not share a grain.

    Same filters and bucket date as ``CostDaily`` (``excluded_from_billing`` dropped, UTC
    ``cost_event_time`` day), so for any (tenant, bucket_date) its sums equal ``CostDaily``'s.
    Sentinels instead of NULL on the key columns, as in the sibling rollups: ``NULL_PROJECT_ID`` /
    ``NULL_ACTOR_ID`` / ``NULL_APP_ID``, and ``funding = 'unknown'`` for an event stored NULL.
    """

    __tablename__ = "billing_cost_usage_daily"

    id: Mapped[uuid.UUID] = mapped_column(DBUUID(), primary_key=True, default=_new_uuid)
    bucket_date: Mapped[date] = mapped_column(Date, nullable=False)
    tenant_id: Mapped[str] = mapped_column(Uuid(as_uuid=False), nullable=False)
    project_id: Mapped[uuid.UUID] = mapped_column(DBUUID(), nullable=False, default=NULL_PROJECT_ID)
    actor_id: Mapped[uuid.UUID] = mapped_column(DBUUID(), nullable=False, default=NULL_ACTOR_ID)
    app_id: Mapped[uuid.UUID] = mapped_column(DBUUID(), nullable=False, default=NULL_APP_ID)
    funding: Mapped[str] = mapped_column(String(32), nullable=False)
    provider: Mapped[str] = mapped_column(String(64), nullable=False)
    served_model: Mapped[str] = mapped_column(String(128), nullable=False, default="__unknown__")
    event_count: Mapped[int] = mapped_column(Integer, nullable=False, default=0)
    total_input_tokens: Mapped[int] = mapped_column(BigInteger, nullable=False, default=0)
    total_output_tokens: Mapped[int] = mapped_column(BigInteger, nullable=False, default=0)
    total_cost_usd: Mapped[Decimal] = mapped_column(Numeric(20, 6), nullable=False, default=0)

    __table_args__ = (
        UniqueConstraint(
            "bucket_date",
            "tenant_id",
            "project_id",
            "actor_id",
            "app_id",
            "funding",
            "provider",
            "served_model",
            name="uq_cost_usage_daily",
        ),
        Index("ix_cost_usage_daily_tenant_date", "tenant_id", "bucket_date"),
    )
