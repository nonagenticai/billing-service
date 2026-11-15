"""Move cost_basis derivation from the rate's SHAPE to the PROVIDER CONTRACT.

THE DEFECT — measured live, ~30 minutes after 0021 shipped
------------------------------------------------------------
``services.pricing.cost_basis_for_cost_model`` (and the write path in
``services.cost_flusher``) derived ``billing_cost_events.cost_basis`` from the
resolved pricing row's ``cost_model``: only ``self_hosted_amortized`` mapped to
``internal_transfer``; every other family — ``per_token``, ``per_second``,
``per_character``, ``per_request`` — derived to ``cash``.

That is backwards. ``cost_model`` says HOW WE METER (per token, per character,
per second, flat per request); it says nothing about WHETHER CASH LEAVES THE
BUILDING. Every provider in this estate today is self-hosted and uses a real
per-unit rate — pgx-local is ``per_token``, mlx-nlp is ``per_character`` — so
every one of them derived to ``cash``, exactly backwards. Live proof at the
time of this migration: ``cost_basis='cash'`` on 15-and-growing
``billing_cost_events`` rows, all ``mlx-nlp``/``per_character``, all written in
the ~2 minutes after the 0021 deploy. mlx-nlp runs on mini2. It is not cash.

The obvious "fix" — flip providers to ``self_hosted_amortized`` — is closed:
``services.pricing._cost_from_row`` returns ``Decimal("0")`` unconditionally
for that ``cost_model`` with no amortization mechanism (0021's own docstring,
DECISION 4). Flipping ``cost_model`` would zero ~94% of booked cost. Not
re-proposed here.

THE FIX — model it where the fact lives
-----------------------------------------
Cost basis is a property of the PROVIDER CONTRACT, not of the rate's shape.
``provider_pricing`` gets a new ``cost_basis`` column
(``'cash'`` | ``'internal_transfer'``), and ``billing_cost_events.cost_basis``
is now derived from the RESOLVED PRICING ROW's declared value —
``services.pricing.compute_cost_usd_with_cost_model`` now returns the row's
``cost_basis`` alongside its ``cost_model``, and ``cost_flusher`` uses that
instead of calling the (now dead) ``cost_model``-keyed table. A pricing MISS
(no row resolved at all) still backstops to ``internal_transfer`` — there is
nothing to say a miss was a real vendor charge, so it under-claims cash rather
than fabricating one, exactly 0021's existing backstop reasoning.

DEFAULT: internal_transfer, not cash
--------------------------------------
Every provider in the estate today is self-hosted: pgx-local, mlx-nlp,
mlx-embed, mlx-rerank, mlx-ocr, mlx-clip, kokoro, kitten, mlx-whisper,
video-analyzer, video-compiler, mflux, webscraper, karma, hirag, kag-graph,
kag, gemma, mlx_inference, inference_router. Zero external vendors are live.
``internal_transfer`` is therefore correct for 100% of current reality, and
adding a genuine external vendor becomes an explicit, deliberate act (setting
``cost_basis='cash'`` on that provider's pricing row) rather than something
that happens by omission. Defaulting to ``cash`` would make the NEXT person's
silence mean "real money" — the wrong direction to fail; see 0021's own
``cost_basis`` reasoning on ``billing_cost_events``, which this migration is
consistent with.

BACKFILL — the 155 external-vendor rows already seeded (claude, anthropic,
deepseek, openai, groq, xai, cohere, google, elevenlabs, deepgram)
-----------------------------------------------------------------------------
These are not self-hosted rate cards; they are transcribed vendor list prices
(``scripts/initial_pricing.yaml``, each block cites the vendor's public
pricing page and a verification date). They are UNUSED today (0021's own
docstring: "155 pricing rows already exist for them, unused") because nothing
routes to them yet — the external-vendor rollout this migration is foundational
for hasn't shipped. But the row itself plainly represents a real vendor's
price, so it is seeded ``cost_basis='cash'`` now rather than left to default:
the moment routing starts using one of these rows, the event should price as
cash from the first row written, not from whenever someone remembers to flip
it. Every other provider (the self-hosted list above, plus anything not in
the named vendor set) gets the ``internal_transfer`` default — correct for
100% of what runs today.

EVENT BACKFILL — the ~15 already-mislabelled billing_cost_events rows
--------------------------------------------------------------------------
Corrected via a join on ``pricing_id`` to the now-backfilled
``provider_pricing.cost_basis``: any ``billing_cost_events`` row currently
``cost_basis='cash'`` whose resolved pricing row is (now, correctly)
``internal_transfer`` is provably self-hosted and provably mislabelled by the
defect above — flip it. This is not scoped to "the last 30 minutes"; it
corrects every historical row this exact defect could have produced, for as
long as 0021's derivation has been live.

WHAT THIS DOES NOT TOUCH
----------------------------
No price, rate, or existing ``cost_model`` value changes. No change to
``excluded_from_billing`` semantics or any reader (rollup engine, invoice
export, reprice, attribution audit). The four billing_cost_events guard
triggers (``billing_truncate_guard_trg``, ``billing_unrolled_truncate_guard_trg``,
``billing_truncate_audit_trg``, ``billing_delete_audit_trg``) live on that
table's parent + partitions and are untouched by this migration, which does
not DROP/CREATE/DISABLE any trigger anywhere —
``tests/test_migration_0022_provider_pricing_cost_basis.py`` asserts all four
still ``tgenabled='O'`` on the parent AND a partition after this migration
runs, mirroring 0021's own assertion.

``provider_pricing`` is NOT partitioned — this is a plain single-table
``ADD COLUMN ... NOT NULL DEFAULT 'internal_transfer'`` (PG11+ single-statement
safe, correct default for every existing row) followed by one data-dependent
UPDATE for the named external-vendor providers, then one data-dependent UPDATE
on ``billing_cost_events`` joined by ``pricing_id``. ``billing_cost_events`` IS
partitioned, but this migration issues no DDL against it at all (only two
UPDATEs), so partition-parent DDL safety does not apply here.

Revision ID: 0022_provider_pricing_cost_basis
Revises: 0021_cost_event_actor_pricing
"""

from __future__ import annotations

from collections.abc import Sequence

from alembic import op

revision: str = "0022_provider_pricing_cost_basis"  # <= 32 chars: alembic_version.version_num
down_revision: str | None = "0021_cost_event_actor_pricing"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


# The operator's own measured mapping of provider keys that are genuine
# external vendors (real list-price rate cards), transcribed from
# scripts/initial_pricing.yaml. Kept in agreement with the Python twin,
# services.pricing.EXTERNAL_VENDOR_PROVIDERS, by
# tests/test_migration_0022_provider_pricing_cost_basis.py (same discipline
# 0021 used for its own SQL/Python twins) rather than imported — migrations in
# this repo stay frozen in time.
_EXTERNAL_VENDOR_PROVIDERS = (
    "claude",
    "anthropic",
    "deepseek",
    "openai",
    "groq",
    "xai",
    "cohere",
    "google",
    "elevenlabs",
    "deepgram",
)

_EXTERNAL_VENDOR_PROVIDERS_SQL_LIST = ", ".join(f"'{p}'" for p in _EXTERNAL_VENDOR_PROVIDERS)

_BACKFILL_PROVIDER_PRICING_COST_BASIS = f"""
UPDATE provider_pricing
SET cost_basis = 'cash'
WHERE provider IN ({_EXTERNAL_VENDOR_PROVIDERS_SQL_LIST})
  AND cost_basis <> 'cash'
"""

# Join on pricing_id: only flip a billing_cost_events row when its OWN
# resolved pricing row is (now) internal_transfer -- i.e. provably self-hosted
# under the corrected derivation, not a guess based on provider name alone.
_BACKFILL_MISLABELLED_COST_EVENTS = """
UPDATE billing_cost_events e
SET cost_basis = 'internal_transfer'
FROM provider_pricing p
WHERE e.pricing_id = p.id
  AND e.cost_basis = 'cash'
  AND p.cost_basis = 'internal_transfer'
"""


def upgrade() -> None:
    op.execute(
        "ALTER TABLE provider_pricing ADD COLUMN IF NOT EXISTS cost_basis VARCHAR(20) "
        "NOT NULL DEFAULT 'internal_transfer'"
    )
    op.execute(_BACKFILL_PROVIDER_PRICING_COST_BASIS)
    op.execute(_BACKFILL_MISLABELLED_COST_EVENTS)


def downgrade() -> None:
    op.execute("ALTER TABLE provider_pricing DROP COLUMN IF EXISTS cost_basis")
    # billing_cost_events.cost_basis rows corrected by the UPDATE above are NOT
    # reverted to 'cash' -- that would resurrect the defect this migration
    # fixes. The column itself (added by 0021) is untouched.
