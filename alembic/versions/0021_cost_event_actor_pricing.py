"""Four modelling decisions on billing_cost_events: actor grain, unit, cash-vs-book.

Foundational work ahead of the external-vendor rollout (inference-router is
moving from all-self-hosted GPU to a MIX of local GPU, rented GPU, and external
vendors — Claude/OpenAI/DeepSeek/Grok). Every change here is additive and
backward compatible with today's writers: no emitter has to change for this
migration to apply cleanly, and ``services.cost_flusher`` fills every new
column with an honest value (often NULL) when the wire sends nothing.

DECISION 1 — IDENTITY GRAIN: CARRY BOTH
----------------------------------------
``subject_id`` keeps its existing meaning — the licences Subject UUID, i.e. WHO
PAYS. This migration does NOT touch it. ``actor_id`` is new: the acting
principal (the JWT ``sub``), i.e. WHO ACTED. NULLABLE, and NEVER DEFAULTED —
not here (no backfill statement touches it) and not in ``cost_flusher``, which
leaves it None whenever the wire carries nothing. "Who pays" and "who acted"
are different questions and neither substitutes for the other; measured
2026-09-06, 98.9% of spend maps to two non-human ``subject_id``s, so without
``actor_id`` per-user attribution cannot exist at all. Inventing a value for
history would be forgery, not attribution — every existing row stays NULL
forever unless a later, unrelated backfill from an external log can vouch for
one, which this migration does not attempt.

DECISION 2 — quantity_unit: THE UNIT INPUT_TOKENS/OUTPUT_TOKENS PACK
---------------------------------------------------------------------
``input_tokens``/``output_tokens`` are generic quantity fields whose UNIT
varies by provider: true tokens only for pgx-local; characters for
mlx-nlp/mlx-embed/kokoro (mlx-embed has recorded input_tokens=1,065,551 on a
512-token model — proof the field was never really "tokens" there); seconds
for video-compiler; megapixels for mflux; passages for mlx-rerank. 83% of rows
are non-token units, so ``SUM(input_tokens)`` across providers is meaningless
today. NULLABLE (not NOT NULL): the honest backfill leaves a row NULL rather
than guessing when neither the provider name nor a resolved pricing row can
say. See BACKFILL below for exactly what is and is not derivable.

DECISION 3 — CASH VS BOOK ENTRY
---------------------------------
``cost_usd`` keeps its meaning — what we book — regardless of basis.
``cost_basis`` (``cash`` | ``internal_transfer``) says which kind of number
that is. ``vendor_cost_usd`` (nullable) is what the vendor actually charged,
when this is a real vendor call — never defaulted to 0, which would assert a
free call rather than an unknown one. ``currency`` defaults to ``'USD'``: every
cost in this table is USD today, and the column exists so a future non-USD
vendor invoice does not silently get treated as USD.

DECISION 4 — ⛔ NOT APPLIED: UNIMPLEMENTABLE AS STATED WITHOUT A REAL PRICE CHANGE
------------------------------------------------------------------------------------
The operator's decision reads: "set pgx-local's pricing row cost_model to
self_hosted_amortized ... Do NOT change any price." Those two clauses conflict
on THIS row today, and the conflict is measured, not theoretical:

* pgx-local's live OPEN cost-rate-class rows (both ``model='local-llm'`` and
  the ``model=NULL`` provider-default) carry ``cost_model='per_token'`` at
  George's 2026-06-19 rate, $10/$25 per 1M input/output tokens — see
  ``scripts/initial_pricing.yaml``'s ``pgx-local`` section and
  ``scripts/reconcile_pricing_units.py``'s own docstring, which documents this
  exact row being superseded FROM ``self_hosted_amortized`` at $0 TO
  ``per_token`` at $10/$25, operator-approved, precisely so it "bills with real
  numbers".
* ``services.pricing._cost_from_row`` returns ``Decimal("0")`` unconditionally
  for ``cost_model == 'self_hosted_amortized'`` — every branch, no exception —
  with amortization against ``monthly_fixed_cost_usd`` explicitly deferred:
  "Amortization across the monthly fixed cost happens at billing-report time
  (not in this phase)" (that module's own docstring). No amortization exists
  anywhere in this codebase yet; ``observability.metrics.pricing_row_malformed_total``
  documents self_hosted_amortized's $0 as "by design" for the providers that
  already use it (gemma, mlx_inference — never priced any other way).
* Flipping pgx-local's ``cost_model`` to ``self_hosted_amortized`` therefore
  does not relabel a $0 row — it TAKES a real, non-zero, per-event booked cost
  (94% of platform spend, per the operator's own figure) and DROPS it to $0 on
  every future event, with no downstream mechanism to reconstruct it. That is
  a real price change (to $0), it breaks decision 3's own invariant that
  ``cost_usd`` "keeps its meaning: what we book", and it directly contradicts
  this decision's own "do NOT change any price" clause.

Implementing amortization-at-report-time so a self_hosted_amortized row prices
non-zero would be a genuine fifth decision — new pricing-engine behaviour, well
outside "the write path" this migration and ``cost_flusher`` are scoped to —
so it is not improvised here. The pricing row is left untouched: pgx-local
keeps billing at its real $10/$25 per_token rate, and the ``cost_basis``
derivation this migration DOES add
(``services.pricing.cost_basis_for_cost_model``) correctly reads that as
``'cash'`` today, which is the honest answer for a row nothing has re-priced —
NOT a claim that pgx-local's spend is externally vendor-billed cash in the
"leaves the business" sense, only that ``cost_usd`` is a real computed number
rather than a $0 placeholder. Distinguishing "real internal rate, self-hosted"
from "real external vendor invoice" within ``cost_basis``'s two-value domain
is exactly the gap an operator decision on the pricing rate (or a third
``cost_basis`` value) would need to close — flagged here, not decided here.

BACKFILL CHOICES (say what, and why)
-------------------------------------
* ``cost_basis``: every existing row predates the external-vendor rollout, so
  EVERY row is self-hosted. A single ``ADD COLUMN ... NOT NULL DEFAULT
  'internal_transfer'`` is both the correct value for 100% of history and the
  safe backstop for any future writer that predates this column (it
  under-claims cash spend rather than fabricating a vendor charge that never
  happened) — no separate UPDATE is needed because the constant is right for
  every row, not merely a placeholder later corrected.
* ``currency``: identical reasoning, ``NOT NULL DEFAULT 'USD'`` — correct for
  100% of history, no separate backfill.
* ``quantity_unit``: genuinely NOT uniform, so it gets a real per-row backfill:
  first by PROVIDER NAME (the operator's own measured mapping, authoritative
  because it is what was actually observed on the wire), then — for any row
  whose provider is not in that measured set but which resolved to a priced
  row — by the matched ``provider_pricing.pricing_id -> cost_model`` family.
  Anything neither rung reaches (no pricing_id, unrecognised provider) is left
  NULL. Nothing is guessed to make the column "complete".
* ``actor_id`` / ``vendor_cost_usd``: no backfill statement at all. Both stay
  NULL for every existing row — see decisions 1 and 3 above.

WHY quantity_unit CANNOT SIMPLY MIRROR cost_model
----------------------------------------------------
Every value ``ProviderPricingCreate.cost_model`` accepts maps to one unit
(per_token->tokens, per_second->seconds, per_character->characters,
per_request->requests, self_hosted_amortized->tokens — pgx-local's own case
today), and that IS the fallback rung below. But mflux prices flat
``per_request`` (see that model's docstring: "per_megapixel is deliberately
absent... every image-gen row is flat per_request") while its measured
quantity is megapixels — deriving from cost_model alone would silently call it
'requests', which is what it is billed as but not what was actually measured
on the wire. Provider name is checked FIRST for exactly this reason, in both
this migration's SQL and its Python twin,
``services.pricing.quantity_unit_for_provider`` — kept from drifting apart by
``tests/test_migration_0021_cost_event_actor_pricing.py``, the same discipline
alembic 0020 used for its own SQL/Python twin.

PARTITIONED TABLE — WHY THIS SHAPE IS SAFE
---------------------------------------------
``billing_cost_events`` is RANGE-partitioned on ``created_at`` (parent +
monthly partitions). Every statement below targets the PARENT only: ``ADD
COLUMN`` on a partitioned parent cascades to every existing partition and is
inherited by every future one (``create_cost_events_partition_for`` mints
partitions with ``CREATE TABLE ... PARTITION OF``), so no per-partition DDL is
needed and none is issued. No column here is added as a bare NOT NULL with no
default — the two NOT NULL columns (``cost_basis``, ``currency``) both carry a
literal default that is correct for every existing row, which is the
Postgres-11+ single-statement-safe shape 0020 already established for this
same table family; ``actor_id``/``quantity_unit``/``vendor_cost_usd`` stay
nullable and are never forced to NOT NULL. None of this touches
``billing_truncate_guard_trg``, ``billing_unrolled_truncate_guard_trg``,
``billing_truncate_audit_trg`` or ``billing_delete_audit_trg`` on the parent or
any partition — no trigger is dropped, disabled or recreated, and
``tests/test_migration_0021_cost_event_actor_pricing.py`` asserts all four
survive ``tgenabled='O'`` on the parent AND on a partition after this
migration runs.

Revision ID: 0021_cost_event_actor_pricing
Revises: 0020_cost_daily_attribution
"""

from __future__ import annotations

from collections.abc import Sequence

from alembic import op

revision: str = "0021_cost_event_actor_pricing"  # <= 32 chars: alembic_version.version_num
down_revision: str | None = "0020_cost_daily_attribution"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


# The Python twin lives in services.pricing.QUANTITY_UNIT_BY_PROVIDER — kept in
# agreement with this SQL by test_migration_0021_cost_event_actor_pricing.py
# rather than imported (migrations in this repo stay frozen in time; see the
# 0020 module docstring for why).
_QUANTITY_UNIT_BY_PROVIDER_CASE = """
        CASE e.provider
            WHEN 'pgx-local' THEN 'tokens'
            WHEN 'mlx-nlp' THEN 'characters'
            WHEN 'mlx-embed' THEN 'characters'
            WHEN 'kokoro' THEN 'characters'
            WHEN 'video-compiler' THEN 'seconds'
            WHEN 'mflux' THEN 'megapixels'
            WHEN 'mlx-rerank' THEN 'passages'
            ELSE NULL
        END
"""

# Fallback rung: any row not covered above but resolved to a priced row is
# derived from that row's cost_model family. The Python twin is
# services.pricing.QUANTITY_UNIT_BY_COST_MODEL.
_QUANTITY_UNIT_BY_COST_MODEL_CASE = """
        CASE p.cost_model
            WHEN 'per_token' THEN 'tokens'
            WHEN 'per_second' THEN 'seconds'
            WHEN 'per_character' THEN 'characters'
            WHEN 'per_request' THEN 'requests'
            WHEN 'self_hosted_amortized' THEN 'tokens'
            ELSE NULL
        END
"""

_BACKFILL_QUANTITY_UNIT_BY_PROVIDER = f"""
UPDATE billing_cost_events e
SET quantity_unit = ({_QUANTITY_UNIT_BY_PROVIDER_CASE})
WHERE e.quantity_unit IS NULL AND ({_QUANTITY_UNIT_BY_PROVIDER_CASE}) IS NOT NULL
"""

_BACKFILL_QUANTITY_UNIT_BY_PRICING = f"""
UPDATE billing_cost_events e
SET quantity_unit = ({_QUANTITY_UNIT_BY_COST_MODEL_CASE})
FROM provider_pricing p
WHERE e.pricing_id = p.id
  AND e.quantity_unit IS NULL
  AND ({_QUANTITY_UNIT_BY_COST_MODEL_CASE}) IS NOT NULL
"""

# Decision 4 is deliberately NOT implemented as a SQL statement here — see the
# "DECISION 4" section of the module docstring for the measured conflict
# (flipping pgx-local's cost_model to self_hosted_amortized would silently
# zero its real $10/$25 per-event booked cost, with no amortization mechanism
# anywhere in this codebase to reconstruct it). Nothing in provider_pricing is
# touched by this migration.


def upgrade() -> None:
    # --- Decision 1: actor_id -- nullable, no default, no backfill ---------
    op.execute("ALTER TABLE billing_cost_events ADD COLUMN IF NOT EXISTS actor_id UUID")
    op.execute(
        """
        CREATE INDEX IF NOT EXISTS ix_costs_actor_created
            ON billing_cost_events (actor_id, created_at)
            WHERE actor_id IS NOT NULL
        """
    )

    # --- Decision 2: quantity_unit -- nullable, real per-row backfill -------
    op.execute("ALTER TABLE billing_cost_events ADD COLUMN IF NOT EXISTS quantity_unit VARCHAR(16)")
    op.execute(_BACKFILL_QUANTITY_UNIT_BY_PROVIDER)
    op.execute(_BACKFILL_QUANTITY_UNIT_BY_PRICING)

    # --- Decision 3: cost_basis / vendor_cost_usd / currency ----------------
    # cost_basis and currency: single-statement-safe (PG11+ fast ADD COLUMN
    # with a constant default) because the default is the CORRECT value for
    # every existing row, not a placeholder — no follow-up UPDATE needed.
    op.execute(
        "ALTER TABLE billing_cost_events ADD COLUMN IF NOT EXISTS cost_basis VARCHAR(20) "
        "NOT NULL DEFAULT 'internal_transfer'"
    )
    op.execute(
        "ALTER TABLE billing_cost_events ADD COLUMN IF NOT EXISTS vendor_cost_usd NUMERIC(20,8)"
    )
    op.execute(
        "ALTER TABLE billing_cost_events ADD COLUMN IF NOT EXISTS currency VARCHAR(3) "
        "NOT NULL DEFAULT 'USD'"
    )

    # --- Decision 4: NOT applied here. See the module docstring. -----------


def downgrade() -> None:
    op.execute("DROP INDEX IF EXISTS ix_costs_actor_created")
    op.execute("ALTER TABLE billing_cost_events DROP COLUMN IF EXISTS actor_id")
    op.execute("ALTER TABLE billing_cost_events DROP COLUMN IF EXISTS quantity_unit")
    op.execute("ALTER TABLE billing_cost_events DROP COLUMN IF EXISTS cost_basis")
    op.execute("ALTER TABLE billing_cost_events DROP COLUMN IF EXISTS vendor_cost_usd")
    op.execute("ALTER TABLE billing_cost_events DROP COLUMN IF EXISTS currency")
