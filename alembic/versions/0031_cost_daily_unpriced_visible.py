"""billing_cost_daily.unpriced_event_count — "unknown cost" must not read as $0.

THE GAP, AND ITS HONEST STATUS
------------------------------
⚠️ LATENT, NOT LIVE. Measured 2026-09-11: **zero** rows in ``billing_cost_events``
carry a NULL ``cost_usd``, so no stored ``billing_cost_daily.total_cost_usd`` is
wrong today and no invoice has been mis-billed by this. It is fixed now because
``billing_invoices`` has 0 rows (the change is free while no statement depends on
the shape) and because the first unpriced provider on a paying customer would
otherwise bill **$0.00** onto an immutable, gaplessly-numbered invoice line with
nothing recording that the figure was a substitution.

``billing_cost_events.cost_usd`` is NULLABLE and the NULL is load-bearing:
``services/pricing.py`` deliberately REFUSES to price an event it cannot price
honestly ("Refuse instead: return None so the caller records a loud pricing_miss
(NULL cost + billing_pricing_miss_total + the MeteringPricingMiss alert)") and
``cost_flusher`` stores that refusal as NULL -- ``models/db/events.py`` on the
column: "NO default: NULL is the load-bearing 'no ProviderPricing matched'
sentinel ... a default of 0 would silently record a pricing miss as 'priced at
$0' and bypass the pricing_miss signal."

``rollup_engine`` then aggregated those rows into ``billing_cost_daily`` with
``coalesce(sum(cost_usd), 0)`` written to a ``nullable=False`` column, and the
distinction the write side went to that much trouble to preserve died there.

⭐ WHAT EXACTLY WAS AND WAS NOT WRONG — the sum is not the defect.
SQL ``sum()`` SKIPS NULLs, so ``total_cost_usd`` always held exactly the spend
that IS known; that is correct and this migration does not change it. Two things
follow from the collapse instead:

* a group with SOME unpriced rows stores a known-good total whose
  ``request_count`` silently includes rows that contributed nothing to it;
* a group whose rows are ALL unpriced makes ``sum()`` NULL, the ``coalesce``
  turns it into a hard ``0.000000``, and the row then sits beside a real token
  count reading "this spend was free" rather than "this spend is unknown".

That second shape was already recorded in this repo before this migration, in
``services/reprice.py``'s own docstring -- which named the measured instance
(``provider`` of openai_compatible, ``service_key`` of commonpractice, no
``provider_pricing`` row at all, 2026-08-08 bucket ``total_cost_usd =
0.000000`` against 6,087 tokens), stated the remedy ("Expressing that absence
needs a nullable rollup column (or a coverage flag beside it)") and explicitly
left it alone ("Neither ``rollup_engine`` nor the column is changed by this
module"). ⚠️ That figure is TESTIMONY carried in a comment, not re-measured for
this migration, and it is not consistent with today's zero-NULL reading -- those
events were either repriced or aged out of their partition since. Treat the
present state as the measured one and that paragraph as the history.

A COUNT BESIDE THE MONEY, NOT A NULLABLE TOTAL
----------------------------------------------
The other candidate fix was making ``total_cost_usd`` nullable, which expresses
the same fact and was rejected: ``services/billing.py``,
``routers/billing.compute_billing_overview``, ``services/reprice.py`` and
``routers/usage.py`` all do arithmetic on that column today, so a nullable money
column moves the collapse into four readers as ``or 0`` -- the same silent
substitution, in more places, each of them a money path. A NOT NULL counter is
strictly additive, keeps every existing contract, and follows the pattern this
module already established for ``ts_fallbacks``: count it INSIDE the aggregate
that does the summing, so the number can never describe a different row set than
the one just billed.

NOT ADDED TO uq_cost_daily
--------------------------
Same reasoning as ``attribution_status`` (alembic 0020): the count is a property
OF the rows in a group, not a dimension of the group, so adding it would split no
group while rewriting the ON CONFLICT shape of every existing row.

NO BACKFILL, DELIBERATELY — AND 0 ON AN OLD ROW MEANS "NOT MEASURED"
--------------------------------------------------------------------
⚠️ Every row that predates this migration reads ``0``, and on those rows 0 means
NOT MEASURED, not "nothing was unpriced". A backfill is not offered because the
only source is ``billing_cost_events``, which is monthly RANGE-partitioned with
retention (``drop_old_cost_partitions``) and was TRUNCATED 2026-09-05T07:01:56Z
-- so a figure computed for an older day would be an undercount wearing the
costume of a measurement, which is the defect this migration exists to remove.
The days that can honestly be restated are restated by the rollup itself:
``daily_reconcile_days`` re-rolls its trailing window on every pass and writes
the real count for every day whose source survives.

⛔ COMPANION EDIT REQUIRED BEFORE THIS SHIPS — ``src/db.py``
-----------------------------------------------------------
``rollup_engine``'s cost UPSERT now NAMES ``unpriced_event_count`` in every
INSERT, so a pod carrying this code against a database still on pre-0031 fails
EVERY cost-rollup INSERT with ``UndefinedColumn`` — the whole cost rollup path
(the invoice source), not a degraded subset. That is precisely what
``db.ADDITIVE_MIGRATIONS`` is the safety net for; see its ``event_ts`` block
("⛔ THE OMISSION HERE IS AN INGEST OUTAGE, NOT A COSMETIC GAP ... an additive
column absent from it has no net"). Mirror this migration byte-compatibly, beside
the other ``billing_cost_daily`` entries::

    "ALTER TABLE billing_cost_daily "
    "ADD COLUMN IF NOT EXISTS unpriced_event_count INTEGER NOT NULL DEFAULT 0",

``src/db.py`` is owned by another lane on this branch and is deliberately NOT
touched here.

SAFE ON A LIVE DATABASE
-----------------------
``ADD COLUMN ... NOT NULL DEFAULT 0`` on Postgres 11+ is metadata-only (no table
rewrite, no row-level lock held for the scan), and ``billing_cost_daily`` is not
partitioned. Nothing reads the column until ``rollup_engine`` writes it.

Revision ID: 0031_cost_daily_unpriced_visible
Revises: 0030_draft_to_issued_money_diff
Create Date: 2026-09-11
"""

from __future__ import annotations

import sqlalchemy as sa
from alembic import op

# <= 32 chars: alembic_version.version_num is varchar(32).
revision = "0031_cost_daily_unpriced_visible"
# ⚠️ Chained onto 0030, NOT onto 0029. 0030 is a sibling lane's revision on this
# same branch; declaring 0029 here would give the tree TWO heads off 0029, and
# `alembic upgrade head` refuses a multi-head chain — every pod would fail to
# start rather than fail loudly at review time.
down_revision = "0030_draft_to_issued_money_diff"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.add_column(
        "billing_cost_daily",
        sa.Column(
            "unpriced_event_count",
            sa.Integer(),
            nullable=False,
            server_default="0",
        ),
    )


def downgrade() -> None:
    op.drop_column("billing_cost_daily", "unpriced_event_count")
