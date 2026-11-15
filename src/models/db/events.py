"""Raw event tables (90-day retention).

``MeteringEvent`` is the per-request log from license-authz.
``CostEvent`` is the per-call cost record from inference-router (or other
cost-incurring services). Both are flushed in batches by
``services.event_flusher`` / ``services.cost_flusher`` and aggregated
nightly by ``services.rollup_engine``.
"""

from __future__ import annotations

import uuid
from datetime import datetime
from decimal import Decimal

from sqlalchemy import (
    CHAR,
    JSON,
    BigInteger,
    Boolean,
    CheckConstraint,
    DateTime,
    ForeignKey,
    Index,
    Integer,
    Numeric,
    SmallInteger,
    String,
    Uuid,
    text,
)
from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy.orm import Mapped, mapped_column

from db import Base

from ._common import DBUUID, _new_uuid, _utcnow


class MeteringEvent(Base):
    """Raw API request event from license-authz."""

    __tablename__ = "billing_metering_events"

    id: Mapped[uuid.UUID] = mapped_column(DBUUID(), primary_key=True, default=_new_uuid)
    # tenant_id is a real Postgres `uuid` column (alembic 0011). as_uuid=False is
    # deliberate: the DB type is uuid while Python keeps receiving/binding `str`, so
    # every existing comparison, log line and JSON serialisation keeps working.
    tenant_id: Mapped[str] = mapped_column(Uuid(as_uuid=False), nullable=False)
    # The tenant license-authz RETARGETED this request to (alembic 0042,
    # billing-service#235; emitted by license-authz#65 only when a retarget
    # happened, never as ""). Typed exactly like ``tenant_id`` above.
    #
    # NULLABLE, NEVER DEFAULTED: NULL means "no retarget on the wire" (or a
    # malformed value, refused and counted by
    # ``billing_metering_retarget_tenant_invalid_total``). It is NEVER sunk to the
    # nil tenant the way ``tenant_id`` is: that would assert a retarget nobody
    # made. Recorded only; nothing bills on it yet.
    #
    # NO INDEX, same judgement as ``project_id`` below: raw events are read by
    # time, never grouped by this column, and a btree would be paid on every
    # INSERT on the hot ingest path to serve no query.
    retarget_tenant_id: Mapped[str | None] = mapped_column(Uuid(as_uuid=False), nullable=True)
    # WHO ACTED (alembic 0052): the UUID Keycloak ``sub`` license-authz sends as
    # ``actor_id`` (license-authz#73 — the verified JWT ``sub``, or the licence
    # subject's ``subject_keycloak_sub``). Distinct from ``subject_id`` (who pays),
    # same meaning as ``CostEvent.actor_id``. NULLABLE, NEVER DEFAULTED: NULL is
    # "no actor on the wire" or a malformed one (counted on
    # ``billing_actor_malformed_total``). ⛔ No backfill; no index (raw events are
    # read by time, same judgement as ``retarget_tenant_id``).
    actor_id: Mapped[uuid.UUID | None] = mapped_column(DBUUID(), nullable=True)
    # ``""`` is the CANONICAL "the registry could not resolve a name" value — NOT
    # "the name is empty". The column stays NOT NULL deliberately (decided
    # 2026-09-06), so "" is the absence marker rather than NULL. A uuid must
    # NEVER be written here: it asserts more than is known, while "" asserts
    # nothing. Consumers: "" is falsy, so `if (name)` / `{#if name}` are correct;
    # `name ?? id` and `COALESCE(tenant_name, …)` are NOT and must treat
    # empty-or-whitespace as absent. Written by services._parsers.stored_tenant_name,
    # which carries the full contract and the boot-window caveat.
    tenant_name: Mapped[str] = mapped_column(String(255), nullable=False, server_default="")
    license_id: Mapped[uuid.UUID | None] = mapped_column(DBUUID(), nullable=True)
    # Subject UUID (the end-user behind the license). NULL when the call is
    # service-to-service or pre-dates per-user metering.
    subject_id: Mapped[uuid.UUID | None] = mapped_column(DBUUID(), nullable=True)
    app_id: Mapped[uuid.UUID | None] = mapped_column(DBUUID(), nullable=True)
    # The kanban project this request belongs to, so spend can be priced per
    # project. NULLABLE and NEVER DEFAULTED here: a raw event either carried a
    # project on the wire or genuinely had none, and substituting a sentinel
    # would assert an attribution the producer never made. The sentinel lives
    # only on the DAILY ROLLUP (``rollups.CostDaily.project_id``), where it is
    # forced by the ON CONFLICT key — see that column's comment.
    #
    # ⛔ DO NOT ADD AN INDEX. Raw events are read by partition + time, never
    # grouped by project; a bare btree would be paid on every INSERT on the hot
    # ingest path and serve no query. Same judgement as ``reservation_id`` on
    # ``CostEvent``, and deliberately NOT the ``event_ts`` judgement.
    project_id: Mapped[uuid.UUID | None] = mapped_column(DBUUID(), nullable=True)
    plan_id: Mapped[uuid.UUID | None] = mapped_column(DBUUID(), nullable=True)
    plan_code: Mapped[str | None] = mapped_column(String(64), nullable=True)
    method: Mapped[str] = mapped_column(String(10), nullable=False)
    path: Mapped[str] = mapped_column(String(512), nullable=False)
    service_key: Mapped[str] = mapped_column(String(64), nullable=False)
    # Widened 20 -> 64 by alembic 0038 (chained off 0037_billed_while_archived).
    # 20 chars silently discarded real license-authz vocabulary: measured live,
    # ``denied_service_principal`` (24) and ``denied_invalid_tenant`` (21) both
    # exceed it and would truncate to services.event_flusher's `_too_long`
    # sentinel -- which does NOT start with "denied", so a genuinely denied
    # request would misclassify as billable (see that module's
    # `_bound_status_for_storage` for the sentinel and its safety property).
    # 64 matches the sibling `service_key` column's bound and covers every
    # status literal seen live with 2.5x headroom over the longest
    # (``allowed_service_principal``, 25 chars) while still bounding a
    # genuinely hostile/pathological value (an exception message, unbounded).
    status: Mapped[str] = mapped_column(String(64), nullable=False)
    source_ip: Mapped[str | None] = mapped_column(String(45), nullable=True)
    # --- Event time, as asserted by the PRODUCER (alembic 0034) --------------
    # The EXACT mirror of ``CostEvent.event_ts`` below — same type, same
    # nullability, same NULL-is-load-bearing contract, same forward-only rule,
    # same partial-index strategy. Read that column's comment; everything it says
    # applies here, and the two must not be allowed to drift.
    #
    # WHY IT WAS MISSING, AND WHAT THAT COST. Alembic 0028 gave the COST bus a
    # producer clock and left the METERING bus on ingest time, so the two money
    # axes of ONE request were bucketed on TWO different clocks. A request emitted
    # at 23:59:50 on the last day of month M whose cost flushes promptly but whose
    # metering event is ingested after midnight had its LLM cost billed in month M
    # and its per-request surcharge in month M+1. Both invoices are wrong and
    # neither is amendable: invoice rows are immutable and gaplessly numbered.
    #
    # THE VALUE WAS ALREADY ON THE WIRE, exactly as it was for cost. Measured
    # 2026-09-05 on the live streams and recorded in
    # ``services.consumer.resolve_service_key``: ``billing:events`` entries carry
    # "tenant/subject/license/app/plan, method, path, status, source_ip,
    # timestamp, latency". ``timestamp`` arrived intact and was dropped on the
    # floor; this column is where it is to land.
    #
    # ⛔ AND IT DOES NOT LAND HERE YET. As of alembic 0034 NOTHING WRITES THIS
    # COLUMN: ``services/event_flusher.py`` — the ONE writer of this table — does
    # not read the wire ``timestamp``, and that half is a separate change in
    # flight. Until it lands, every new row has ``event_ts IS NULL`` and is
    # bucketed on ``created_at`` exactly as before. Do not read this comment as
    # evidence the split-invoice defect is closed; the schema and the rollup are
    # ready for it, the producer is not. The line needed there is
    # ``"event_ts": parse_event_ts(e.get("timestamp"), received_at=now)``, reusing
    # the same ``services._parsers.parse_event_ts`` ``cost_flusher`` calls.
    #
    # NULLABLE, AND NULL IS LOAD-BEARING: NULL means "no usable producer event
    # time" and every reader falls back to ``created_at``
    # (``rollup_engine.metering_event_time``). ⛔ NOT BACKFILLED, EVER — every row
    # written before 0034 had its true event time discarded at ingest and it
    # exists nowhere; ``event_ts = created_at`` would restate ingest time as event
    # time under a column that promises otherwise.
    #
    # ⚠️ UNLIKE ``CostEvent``, this table is NOT partitioned (``relkind = 'r'`` —
    # measured and recorded in alembic 0023), so ``created_at`` is not a partition
    # key here and retention is a row DELETE (``cleanup_old_events``). Nothing
    # about this column changes that.
    event_ts: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False, default=_utcnow
    )

    __table_args__ = (
        Index("ix_events_tenant_created", "tenant_id", "created_at"),
        Index("ix_events_license_created", "license_id", "created_at"),
        Index("ix_events_subject_created", "subject_id", "created_at"),
        Index("ix_events_created", "created_at"),
        Index("ix_metering_events_tenant_name", "tenant_name"),
        # PARTIAL, for the same reason ``ix_costs_event_ts`` is: ``event_ts`` is
        # NULL for 100% of pre-0034 history and is never backfilled, so a full
        # index would mostly index absence.
        #
        # ⚠️ SAME HONESTY AS THE COST SIBLING about what it serves. It serves a
        # direct range scan on event time (an audit, a restatement by event time).
        # It does NOT accelerate ``rollup_hourly``'s resolved predicate, which
        # reads ``coalesce(event_ts, created_at)`` — a btree on one column cannot
        # answer a COALESCE over two. What keeps that query cheap is still the
        # sargable LOWER bound on ``created_at`` and ``ix_events_created``.
        Index(
            "ix_events_event_ts",
            "event_ts",
            postgresql_where=text("event_ts IS NOT NULL"),
        ),
    )


class CostEvent(Base):
    """Raw cost event from inference-router or other cost-incurring services."""

    __tablename__ = "billing_cost_events"

    id: Mapped[uuid.UUID] = mapped_column(DBUUID(), primary_key=True, default=_new_uuid)
    tenant_id: Mapped[str] = mapped_column(Uuid(as_uuid=False), nullable=False)
    # ``""`` is the CANONICAL "the registry could not resolve a name" value — NOT
    # "the name is empty". The column stays NOT NULL deliberately (decided
    # 2026-09-06), so "" is the absence marker rather than NULL. A uuid must
    # NEVER be written here: it asserts more than is known, while "" asserts
    # nothing. Consumers: "" is falsy, so `if (name)` / `{#if name}` are correct;
    # `name ?? id` and `COALESCE(tenant_name, …)` are NOT and must treat
    # empty-or-whitespace as absent. Written by services._parsers.stored_tenant_name,
    # which carries the full contract and the boot-window caveat.
    tenant_name: Mapped[str] = mapped_column(String(255), nullable=False, server_default="")
    license_id: Mapped[uuid.UUID | None] = mapped_column(DBUUID(), nullable=True)
    # Subject UUID — WHO PAYS. The licence-holder / billable party (the licences
    # Subject UUID). Unchanged meaning; kept distinct from ``actor_id`` below on
    # purpose (alembic 0021) — "who pays" and "who acted" are different
    # questions and neither substitutes for the other. NULL when the call is
    # service-to-service or pre-dates per-user metering.
    subject_id: Mapped[uuid.UUID | None] = mapped_column(DBUUID(), nullable=True)
    # Actor UUID — WHO ACTED. The acting principal (the JWT ``sub``) at the
    # moment of the call, as opposed to ``subject_id`` (who pays). NULLABLE and
    # NEVER DEFAULTED: it is genuinely unknown for every event written before
    # emitters are updated to send it (alembic 0021), and no value here can
    # stand in for "unknown" without asserting more than is known. Measured
    # 2026-09-06: 98.9% of spend maps to two non-human ``subject_id``s, so
    # without this column per-user attribution cannot exist at all — but
    # inventing a value for history would be forgery, not attribution.
    actor_id: Mapped[uuid.UUID | None] = mapped_column(DBUUID(), nullable=True)
    app_id: Mapped[uuid.UUID | None] = mapped_column(DBUUID(), nullable=True)
    # The kanban project this cost belongs to, so spend can be priced per
    # project. The EXACT mirror of ``MeteringEvent.project_id`` above — same
    # type, same nullability, same never-defaulted contract; read that comment,
    # and do not let the two drift.
    #
    # NULLABLE AND NEVER DEFAULTED: a raw cost event either carried a project on
    # the wire or genuinely had none, and a sentinel here would assert an
    # attribution the producer never made. The sentinel exists only on the daily
    # rollup (``rollups.CostDaily.project_id``), where the ON CONFLICT key forces
    # it.
    #
    # ⛔ DO NOT ADD AN INDEX. This table is read by partition + ``created_at``,
    # never grouped by project; a bare btree would be paid on every INSERT on the
    # hot ingest path and serve nothing. Same judgement as ``reservation_id``
    # below, and deliberately NOT ``event_ts``'s — do not add one by analogy.
    project_id: Mapped[uuid.UUID | None] = mapped_column(DBUUID(), nullable=True)
    service_key: Mapped[str] = mapped_column(String(64), nullable=False)
    provider: Mapped[str] = mapped_column(String(64), nullable=False)
    # The client-requested model alias echoed by inference-router (e.g.
    # `deepseek-chat`, `gemma`). Uniform across providers and stream/non-stream
    # since IR PR #62; before that, non-streamed OpenAI/Anthropic calls put the
    # IR provider-config name here instead (see `provider`). Pricing lookup uses
    # `served_model`, not this; `provider` carries the IR provider-config name.
    model: Mapped[str | None] = mapped_column(String(128), nullable=True)
    # The actual model that served the request (e.g. `claude-3-5-sonnet-20241022`).
    # Authoritative for pricing lookup.
    served_model: Mapped[str | None] = mapped_column(String(128), nullable=True)
    input_tokens: Mapped[int] = mapped_column(Integer, nullable=False, default=0)
    output_tokens: Mapped[int] = mapped_column(Integer, nullable=False, default=0)
    # Anthropic-style prompt cache token classes. Default 0 for non-Anthropic.
    cache_creation_input_tokens: Mapped[int] = mapped_column(
        BigInteger, nullable=False, default=0, server_default="0"
    )
    cache_read_input_tokens: Mapped[int] = mapped_column(
        BigInteger, nullable=False, default=0, server_default="0"
    )
    # The UNIT ``input_tokens``/``output_tokens`` actually count (alembic 0021).
    # Those two columns are generic quantity fields whose meaning VARIES by
    # provider — true tokens for pgx-local, characters for mlx-nlp/mlx-embed/
    # kokoro (mlx-embed has recorded input_tokens=1,065,551 on a 512-token
    # model), seconds for video-compiler, megapixels for mflux, passages for
    # mlx-rerank. ``services.pricing.quantity_unit_for_provider`` sources it
    # from the provider name first (the operator's own measured mapping) and
    # falls back to the resolved ``provider_pricing.cost_model`` family
    # otherwise. NULLABLE and left NULL rather than guessed when neither is
    # available — see the module docstring and alembic 0021 for the backfill.
    quantity_unit: Mapped[str | None] = mapped_column(String(16), nullable=True)
    # Authoritative cost computed by the metering aggregator. NULL when no
    # ProviderPricing row matches at the event timestamp — billing skips
    # NULL-cost events.
    # Numeric(20, 8) matches cost_usd_hint + provider_pricing rates; the old
    # Numeric(12, 8) capped a single event at ~$10k and a computed cost >= $10k
    # (e.g. a self_hosted_amortized / monthly_fixed_cost_usd line) overflowed on
    # INSERT, aborting the whole cost_flusher batch → stuck consumer. NO default:
    # NULL is the load-bearing "no ProviderPricing matched" sentinel (billing
    # skips NULL-cost rows); a default of 0 would silently record a pricing miss
    # as "priced at $0" and bypass the pricing_miss signal.
    cost_usd: Mapped[Decimal | None] = mapped_column(Numeric(20, 8), nullable=True)
    # Diagnostic-only — whatever inference-router emitted alongside the event.
    cost_usd_hint: Mapped[Decimal | None] = mapped_column(Numeric(20, 8), nullable=True)
    # cash vs book entry (alembic 0021). ``cost_usd`` keeps its meaning — what we
    # book — regardless of which basis produced it. 'internal_transfer' means
    # cost_usd is an amortized/self-hosted book entry (today: every row priced
    # via a ``self_hosted_amortized`` pricing row, e.g. gemma/mlx_inference —
    # NOT pgx-local, which still bills real per_token dollars and therefore
    # derives to 'cash'; see alembic 0021's docstring for why decision 4's
    # cost_model flip was deliberately NOT applied); 'cash' means it is real
    # money paid to a vendor (or, today, a real self-hosted per-unit rate).
    # NOT NULL with server_default 'internal_transfer' —
    # the SAFE default for a row nothing has classified: it under-claims cash
    # spend rather than fabricating a vendor charge that never happened. Every
    # row up to alembic 0021 is self-hosted, so 'internal_transfer' is also the
    # correct backfilled value for 100% of history, not merely a safe one.
    # ``services.cost_flusher`` always computes this explicitly from the
    # resolved pricing row's ``cost_model`` — the default is a backstop for
    # writers that predate this column, never the flusher's own answer.
    cost_basis: Mapped[str] = mapped_column(
        String(20), nullable=False, server_default="internal_transfer"
    )
    # What the vendor actually charged, when this is a real vendor call (NULL
    # otherwise — never 0, which would assert a free call rather than an
    # unknown one). Distinct from ``cost_usd`` (what we book) so a markup or a
    # negotiated rate can diverge from the invoice line without losing either
    # number. NULL for every row until an external-vendor emitter sends it.
    vendor_cost_usd: Mapped[Decimal | None] = mapped_column(Numeric(20, 8), nullable=True)
    # ISO 4217. Every cost in this table is USD today; the column exists so a
    # future non-USD vendor invoice does not silently get treated as USD.
    currency: Mapped[str] = mapped_column(String(3), nullable=False, server_default="USD")
    # --- EUR consumption for money-budget tenants (alembic 0044) ------------
    # Written by services.cost_flusher ONLY for an event whose usage_extra
    # carries ``setup`` AND whose tenant has tenant_config.budget_unit='money':
    # EUR = fx(USD->EUR @ event time) x base (setup 1 cost price; setup 2
    # list x (1+pct/100); setup 3 list x pct/100). NULL on every token-path row,
    # permanently -- NULL means "not a money settlement", never "0 EUR".
    consumption_amount: Mapped[Decimal | None] = mapped_column(Numeric(20, 8), nullable=True)
    consumption_currency: Mapped[str | None] = mapped_column(CHAR(3), nullable=True)
    # The LLM setup the event was served under (usage_extra.setup), recorded for
    # every event that states a valid one, money tenant or not.
    llm_setup: Mapped[int | None] = mapped_column(
        SmallInteger,
        CheckConstraint("llm_setup IN (1, 2, 3)", name="ck_cost_events_llm_setup"),
        nullable=True,
    )
    # FK to the ProviderPricing row that authoritatively priced this event.
    pricing_id: Mapped[int | None] = mapped_column(
        BigInteger,
        ForeignKey("provider_pricing.id", ondelete="SET NULL"),
        nullable=True,
    )
    # Upstream provider's request id, useful for invoice reconciliation.
    provider_request_id: Mapped[str | None] = mapped_column(String(255), nullable=True)
    # Bag for future reasoning/thinking/image tokens.
    usage_extra: Mapped[dict | None] = mapped_column(JSONB, nullable=True)
    latency_ms: Mapped[int | None] = mapped_column(Integer, nullable=True)
    # Column named "metadata" in DB, renamed here to avoid collision with Base.metadata
    extra_metadata: Mapped[dict | None] = mapped_column("metadata", JSON, nullable=True)
    # End-to-end correlation key, propagated from n8n / fr-backend through
    # worker_management.graph_execution_sessions and into Loki log lines so
    # the exec_quality ETL can join all three sources by a single id.
    correlation_id: Mapped[uuid.UUID | None] = mapped_column(DBUUID(), nullable=True, index=True)
    # Producer-side attribution. Lets dashboards split spend by which service
    # generated the cost (e.g. ``inference-router``, ``worker_management``,
    # ``scientific-processor``) independently of ``service_key``, which names
    # the *consumer*. Defaults to 'unknown' so legacy producers keep flushing.
    cost_source: Mapped[str] = mapped_column(
        String(64), nullable=False, default="unknown", server_default="unknown"
    )
    # WHO FUNDS the call (alembic 0053): local_gpu / platform_external /
    # tenant_byo, or NULL when the emitter sent an invalid value or none could be
    # derived. Set by services.cost_flusher via services.funding.
    funding: Mapped[str | None] = mapped_column(String(32), nullable=True)
    # Producer-minted idempotency key, one per physical metered call. The Redis
    # cost bus is at-least-once and ``ProcessedCostMessage`` dedupes on the Redis
    # message id, which does NOT survive a re-XADD by the producer; this key
    # does. Claimed in ``processed_cost_event_keys`` (a separate, NON-partitioned
    # table) because a UNIQUE constraint cannot live here: this table is
    # RANGE-partitioned on ``created_at``, so any unique index must include it,
    # and ``created_at`` is ingest-time and unstable across redelivery.
    # NULL/absent for producers that predate the key — those keep the old
    # message-id-only guarantee rather than being rejected.
    event_key: Mapped[str | None] = mapped_column(String(64), nullable=True)
    # --- Reversible billing exclusion -------------------------------------
    # Set instead of DELETE. Billing rows are financial records: a duplicate is
    # PARKED, never destroyed, so the decision can be audited and reversed with
    # a single UPDATE. Every aggregate that turns events into money must filter
    # on this; see rollup_engine.rollup_daily, routers/export, routers/costs,
    # services/attribution_audit and services/reprice.
    excluded_from_billing: Mapped[bool] = mapped_column(
        Boolean, nullable=False, default=False, server_default="false"
    )
    # WHY the row is parked, so a later reader can tell one exclusion class from
    # another (and so a reversal can target exactly one class).
    exclusion_reason: Mapped[str | None] = mapped_column(String(64), nullable=True)
    # --- Archived-tenant spend, flagged not refused (alembic 0037) --------
    # `archived_at` is enforced in FOUR other places in the estate (licences'
    # licence-mint path, its token-mint path, and `routers/budget.py`'s
    # pre-spend reservation gate) and NONE of them is this ingestion path,
    # which is reached AFTER the compute already happened (inference-router /
    # mlx-ingestion-backend push cost events east-west, bypassing both the
    # gateway-only Envoy ext_authz check and the budget gate). Refusing here
    # would not stop the spend, only destroy the record of it — a strictly
    # worse outcome than mis-attributing money already spent (same ordering
    # of harms `excluded_from_billing` above states for an unregistered
    # tenant). So this column is a FLAG, never a filter: the row is ALWAYS
    # billed and rolled up exactly as any other row; this only says whether
    # `services.tenant_names.is_tenant_archived` answered True for the row's
    # tenant at flush time.
    #
    # NULLABLE, NEVER BACKFILLED: every row written before this column
    # existed genuinely never had the check run, and there is no way to
    # reconstruct after the fact whether a given historical tenant_id was
    # archived AT THE TIME that row was flushed — the registry only exposes
    # current state. NULL therefore means "never checked", not "checked and
    # clean"; every row flushed after this ships carries an explicit
    # True/False.
    billed_while_archived: Mapped[bool | None] = mapped_column(Boolean, nullable=True)
    # The surviving twin this row duplicates — the audit trail for the park.
    # A plain uuid, not an FK: the referent lives in a partitioned table whose
    # PK is (id, created_at), so an FK on id alone is not expressible.
    duplicate_of_event_id: Mapped[uuid.UUID | None] = mapped_column(DBUUID(), nullable=True)
    # --- Event time, as asserted by the PRODUCER ---------------------------
    # WHAT IT IS: the emitter's own ``timestamp`` wire field — ``datetime.now(UTC)``
    # taken at the moment it finished metering the call. Every emitter already
    # sends it (inference-router services/metering.py, w2v_common
    # metering/cost_bus.py, kag_common metering/__init__.py); billing simply
    # dropped it on the floor until alembic 0028.
    #
    # ⛔ WHAT IT IS NOT: it is NOT request-START time. It is taken at END of
    # request, after the provider answered, so for a 40-minute video compile it
    # sits 40 minutes after the work began. It must never be documented, graphed
    # or billed as "when the request started" — only as "when the producer
    # finished and asserted the cost".
    #
    # WHY IT EXISTS ANYWAY: it is STABLE ACROSS REDELIVERY, and ``created_at``
    # explicitly is not (see ``event_key``'s comment above and
    # ``ProcessedCostMessage``: ``created_at`` is ingest ``now()``, so the same
    # physical call gets a different one on every replay). ``created_at`` is
    # therefore the wrong clock to derive a BUCKET DATE from: a flush that
    # crosses midnight bills the usage into the wrong day, and — at a month
    # boundary — into the wrong INVOICE. Normally the lag is seconds; an observed
    # consumer stall made it 62 hours.
    #
    # NULLABLE, AND NULL IS LOAD-BEARING: NULL means "no usable producer event
    # time", and every reader must fall back to ``created_at`` for that row
    # (``services.rollup_engine`` does, and counts the fallback). Three ways to
    # get NULL, all honest: the wire carried no ``timestamp``; it carried one
    # that would not parse; or it parsed to an instant AFTER the flusher received
    # it (a skewed producer clock — see ``cost_flusher``, which refuses it rather
    # than billing a cost into the future).
    #
    # ⛔ NOT BACKFILLED, EVER. Every row written before 0028 has had its true
    # event time discarded at ingest; it exists nowhere. Deriving one from
    # ``created_at`` would not be a backfill, it would be restating ingest time
    # as event time under a column name that promises otherwise. Forward-only.
    #
    # ⚠️ NOT the partition key and not a substitute for one: ``created_at`` still
    # RANGE-partitions this table, and retention/pruning still runs on it.
    event_ts: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    # --- Budget reservation identity (alembic 0032) -------------------------
    # The per-reservation handle, minted by BILLING when it grants a budget
    # reservation and carried back here by ``inference-router`` on the cost event,
    # so a settlement can only release the reservation it actually holds. Without
    # it the cap fails OPEN — measured 151.6 over-releases/hour
    # (billing-service#178) — because an anonymous release returns headroom that
    # was never held.
    #
    # NULLABLE, AND NULL IS THE CORRECT PERMANENT VALUE — NOT a gap awaiting a
    # backfill. Roughly 90% of debits have no reservation at all, BY DESIGN (they
    # are metered after the fact and never pre-flighted), so NULL means "this
    # debit had no reservation" and settlement has nothing to release for it.
    # Never read it as "reservation lost". Every pre-0032 row is NULL forever:
    # those producers did not carry the handle, so there is nothing to backfill
    # from and inventing one would fabricate an authorisation that never existed.
    #
    # ⛔ DO NOT ADD AN INDEX. This column is written on INSERT and read only
    # in-process, during the same settlement batch that wrote it; it is never the
    # sole predicate of a query. An index would be paid on every cost INSERT (the
    # hot ingest path) and serve nothing. ⚠️ That is deliberately a DIFFERENT
    # judgement from ``event_ts`` above, which earns its partial index by serving
    # direct range scans (reprice windows, audits). Do not add one by analogy.
    reservation_id: Mapped[uuid.UUID | None] = mapped_column(DBUUID(), nullable=True)
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False, default=_utcnow
    )

    __table_args__ = (
        Index("ix_costs_tenant_created", "tenant_id", "created_at"),
        Index("ix_costs_provider_created", "provider", "created_at"),
        Index("ix_costs_license_created", "license_id", "created_at"),
        Index("ix_costs_subject_created", "subject_id", "created_at"),
        # Partial: actor_id is NULL for every row until emitters send it
        # (alembic 0021), so a full index would mostly index absence.
        Index(
            "ix_costs_actor_created",
            "actor_id",
            "created_at",
            postgresql_where=text("actor_id IS NOT NULL"),
        ),
        Index("ix_cost_events_tenant_name", "tenant_name"),
        Index("ix_cost_events_source_created", "cost_source", "created_at"),
        CheckConstraint(
            "funding IN ('local_gpu', 'platform_external', 'tenant_byo')",
            name="ck_cost_events_funding",
        ),
        # PARTIAL, for the same reason ix_costs_actor_created is: ``event_ts`` is
        # NULL for 100% of pre-0028 history and is never backfilled, so a full
        # index would mostly index absence.
        #
        # ⚠️ BE HONEST ABOUT WHAT THIS DOES AND DOES NOT SERVE. It serves a direct
        # range scan on event time (``WHERE event_ts >= … AND event_ts < …`` — a
        # reprice window, an audit, an invoice restated by event time). It does
        # NOT accelerate ``rollup_daily``'s resolved predicate: that one reads
        # ``cast(timezone('UTC', coalesce(event_ts, created_at)) AS date)``, and
        # ``timezone(text, timestamptz)`` is STABLE rather than IMMUTABLE, so the
        # expression is not indexable at all and a plain ``event_ts`` btree cannot
        # answer a COALESCE over two columns either. What prunes that query is
        # still the sargable LOWER bound on ``created_at`` (the partition key) —
        # see the long comment at that predicate for why the lower bound stays
        # sound and the upper bound had to go.
        Index(
            "ix_costs_event_ts",
            "event_ts",
            postgresql_where=text("event_ts IS NOT NULL"),
        ),
    )


class ProcessedCostEventKey(Base):
    """Idempotency inbox keyed on the PRODUCER's per-call event key.

    ``ProcessedCostMessage`` claims the Redis *message* id, which is minted by
    Redis at XADD time. That defeats a redelivery of the same message, but not a
    producer that re-XADDs the same call (a retried emit, a replayed outbox, a
    consumer-group reset that makes the producer resend) — those are different
    message ids carrying the same physical LLM call, and billing had no way to
    tell. worker_management now stamps ``event_key`` (a uuid7 minted once per
    completed call); the flusher claims it here in the SAME transaction as the
    cost_events insert, so a replayed emit is dropped instead of re-billed.

    Separate and NON-partitioned for the same reason ``ProcessedCostMessage``
    is: ``billing_cost_events`` is RANGE-partitioned on ``created_at``, so a
    unique constraint there would have to include ``created_at`` — which is
    ingest ``now()`` and therefore differs between the original and the replay,
    making the constraint vacuous for exactly the case it is meant to catch.

    This does NOT dedupe across producers. Two services metering the same
    physical call mint two different keys; that class is prevented upstream, by
    worker_management refusing to meter a call inference-router already metered.
    """

    __tablename__ = "processed_cost_event_keys"

    event_key: Mapped[str] = mapped_column(String(64), primary_key=True)
    processed_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False, default=_utcnow, server_default="now()"
    )

    __table_args__ = (Index("ix_processed_cost_event_keys_processed_at", "processed_at"),)


class ProcessedCostMessage(Base):
    """Idempotency inbox for the cost-event stream consumer.

    The Redis Streams cost bus is at-least-once: a message can be redelivered if
    the consumer commits the ``cost_events`` insert but crashes before the XACK,
    or if a ``NOGROUP`` recovery recreates the group at id=0 against a *durable*
    stream (see ``services.consumer``). To keep billing exactly-once, the flusher
    claims each Redis stream message id here with ``INSERT … ON CONFLICT DO
    NOTHING`` inside the SAME transaction as the cost_events insert; only newly
    claimed ids are persisted/priced/debited.

    This is a deliberately separate, NON-partitioned table: ``cost_events`` is
    RANGE-partitioned on ``created_at`` (so a unique constraint there would have
    to include ``created_at``, which defaults to ingest ``now()`` and is not
    stable across redelivery). A standalone PK on the immutable Redis id is the
    clean dedupe key. Rows are pruned by ``rollup_engine.cleanup_old_processed_messages``
    well outside the stream's redelivery window.
    """

    __tablename__ = "processed_cost_messages"

    # The Redis Stream message id (``<ms>-<seq>``) — immutable across redelivery.
    stream_msg_id: Mapped[str] = mapped_column(String(64), primary_key=True)
    processed_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False, default=_utcnow, server_default="now()"
    )

    __table_args__ = (Index("ix_processed_cost_messages_processed_at", "processed_at"),)


class ProcessedEventMessage(Base):
    """Idempotency inbox for the usage/metering-event stream consumer.

    The Redis Streams usage bus is at-least-once: a message can be redelivered if
    the consumer commits the ``billing_metering_events`` insert + monthly rollup
    but crashes before the XACK, or if a ``NOGROUP`` recovery recreates the group
    at id=0 against a *durable* stream (see ``services.consumer``). Because the
    monthly rollup UPSERT is *additive* (``request_count = request_count +
    excluded.request_count``), a redelivery would double-increment it — pushing a
    tenant falsely ``over_quota`` and denying legitimate requests. To keep usage
    exactly-once, the flusher claims each Redis stream message id here with
    ``INSERT … ON CONFLICT DO NOTHING`` inside the SAME transaction as the raw
    event insert + rollup; only newly claimed ids are counted.

    This is a deliberately separate, NON-partitioned table on the immutable Redis
    id (mirroring ``ProcessedCostMessage``). Rows are pruned by
    ``rollup_engine.cleanup_old_processed_messages`` well outside the stream's
    redelivery window.
    """

    __tablename__ = "processed_event_messages"

    # The Redis Stream message id (``<ms>-<seq>``) — immutable across redelivery.
    stream_msg_id: Mapped[str] = mapped_column(String(64), primary_key=True)
    processed_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False, default=_utcnow, server_default="now()"
    )

    __table_args__ = (Index("ix_processed_event_messages_processed_at", "processed_at"),)
