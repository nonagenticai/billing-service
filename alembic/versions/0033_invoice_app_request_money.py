"""Carry APP-REQUEST money onto the invoice as its own component pair.

THE DEFECT, AND WHY IT IS CURRENTLY INVISIBLE
---------------------------------------------
``services/billing.py`` builds ``totals.customer_cost_usd`` — the figure
``close_period`` stores as ``total_usd`` and ``subtotal_usd`` — as::

    customer_cost = Σ(app_request_billed + llm_billed) + access_total + seat_fee_total

``billing_invoices`` broke exactly ONE of those two billed streams out into
component columns: ``llm_usage_usd`` (Σ incurred) and ``llm_markup_usd``
(Σ billed − Σ incurred), whose sum is Σ ``llm_billed``. The app-request stream
had no columns at all, so the stored components understate the stored total by
**exactly the app-request billed amount**, permanently, on an immutable row
drawn from a gapless series.

⛔ AND THE TAMPER CHECK CANNOT SEE IT. ``reproduce``'s ``money_matches_snapshot``
re-derives the columns with ``_money_from_snapshot`` — the SAME function that
omits the stream — and compares the result against the columns it produced. Both
sides are missing the same term, so they agree, and the one instrument that
exists to falsify a stored invoice figure reports green over a total its own
components cannot account for. A check whose two halves share a defect is not a
check; that is what these columns, and the close-path identity gate they enable,
close.

⚠️ MEASURED 2026-09-13, AND THE MEASUREMENT IS THE ARGUMENT FOR DOING IT NOW.
``billing_app_cost_rates`` holds **2 rows** (``workers``, ``fr-backend``) and
BOTH are priced ``0.00000000``. So the imbalance is latent only because every
app rate in the estate happens to be zero — a CONFIGURATION STATE an operator can
change with one ``UPDATE``, not a safeguard. The first non-zero rate arms the
defect silently, on statements that are already immutable by the time anyone
looks. This is the same reason 0025 froze ``markup_source`` while the answer was
still the boring one: the distinction is worth recording precisely while it is
invisible, and ``billing_invoices`` is still small.

WHY TWO COLUMNS AND NOT ONE
---------------------------
``app_request_usage_usd`` is what the platform's own per-request rate INCURRED;
``app_request_markup_usd`` is what was charged on top of it. Collapsing them into
a single "app request charge" would mirror the llm pair badly and lose the one
figure margin is reconciled from — and the two are already separate facts in the
snapshot (``costs.app_request_cost_usd`` / ``costs.app_request_billed_usd``).
The pair sums to the billed stream, exactly as ``llm_usage_usd`` +
``llm_markup_usd`` does, which is what makes the close-path identity expressible
as a sum rather than as a special case.

⚠️ ``app_request_markup_usd`` IS SIGNED AND MAY LEGITIMATELY BE NEGATIVE, same as
its llm sibling. Under ``BILLING_MODE_INTERNAL`` every line bills $0 while the
incurred cost is real, so the pair reads ``+X`` / ``−X`` and nets to zero. A
CHECK constraint demanding ``>= 0`` would refuse every internal cost centre's
statement, so none is added.

NOT NULL WITH A ``0`` SERVER DEFAULT
------------------------------------
Matching the component columns 0024 established (``access_charge_usd``,
``llm_usage_usd``, ``seat_fee_usd`` …) rather than the four totals, which carry
NO default deliberately. The reasoning is 0024's and is unchanged: a period with
no app-request spend legitimately has ``app_request_usage_usd = 0``, so ``0`` is
a true value here rather than a fabricated one — unlike a missing TOTAL, where
``0`` would make a failed generator indistinguishable from an empty month.

⚠️ Existing rows stay valid and the backfill is honest for a different reason
than convenience: every row written before this migration was closed while every
app rate in the estate was ``0.00000000``, so its true app-request usage and
markup ARE ``0``. The default states what happened; it does not invent it.

⚠️ ``Numeric(20, 8)``, matching every other money column on this table. See the
``MONEY PRECISION`` section of ``models/db/invoices.py`` for why the narrow
``Numeric(12, x)`` shapes were abandoned, and why this table does not copy
``billing_cost_daily``'s scale of 6.

THE IMMUTABILITY TRIGGER NEEDS NO EDIT, AND THAT IS BY CONSTRUCTION
--------------------------------------------------------------------
``invoice_immutability``'s guard compares ``to_jsonb(OLD)`` minus an ALLOWLIST of
the columns a transition may move against the same projection of ``NEW`` — not a
denylist of the money columns that exist today. Its own docstring states the
consequence: "a new column is guarded the instant it exists, and OPENING a column
requires adding it to the allowlist". So these two are frozen from the moment
this migration lands, with no trigger DDL here and none needed. ⛔ Do not add
them to either allowlist.

This adds COLUMNS to a table that already exists; it creates no table, so 0024's
"every table added after 0018 is born unguarded" hazard does not apply —
``billing_invoices`` keeps the guard it was given at 0024.

⚠️ NOT MIRRORED IN ``src/db.py`` ``ADDITIVE_MIGRATIONS``, and deliberately so:
that tuple carries no ``billing_invoices`` statement at all (0024, 0025 and 0030
added none either). The boot-replay net exists for tables on the hot INGEST path,
where a pod ahead of its schema is an outage; the invoice close is a scheduled
monthly job that fails loudly and retries. Adding a statement for this table
would start a mirror that has to be kept byte-identical forever for no gain.

Revision ID: 0033_invoice_app_request_money
Revises: 0032_cost_event_reservation
Create Date: 2026-09-13
"""

from __future__ import annotations

from collections.abc import Sequence

import sqlalchemy as sa
from alembic import op

revision: str = "0033_invoice_app_request_money"  # <= 32 chars: alembic_version.version_num
down_revision: str | None = "0032_cost_event_reservation"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    # Mirrors models/db/invoices.py BillingInvoice.app_request_usage_usd /
    # .app_request_markup_usd. Two separate add_column calls rather than one
    # batch: alembic emits one ALTER each, both catalogue-only (a NOT NULL column
    # WITH a constant default is metadata-only since PG 11), so neither rewrites
    # the table.
    op.add_column(
        "billing_invoices",
        sa.Column(
            "app_request_usage_usd",
            sa.Numeric(20, 8),
            server_default="0",
            nullable=False,
        ),
    )
    op.add_column(
        "billing_invoices",
        sa.Column(
            "app_request_markup_usd",
            sa.Numeric(20, 8),
            server_default="0",
            nullable=False,
        ),
    )
    # ⛔ NO BACKFILL STATEMENT, AND NONE IS POSSIBLE. The per-licence app-request
    # figures live in `billing_invoices.snapshot` JSONB, so a backfill would have
    # to UPDATE rows that the immutability trigger refuses to move at all — and
    # rightly: an issued statement's money may not be restated in place. It is
    # also unnecessary, per the module docstring: every pre-0033 row was closed
    # against an all-zero rate table, so `0` is the correct historical value.


def downgrade() -> None:
    # ⚠️ Reversible as schema, lossy as data — the same trade 0032 documents.
    # Dropping these destroys the only place the app-request split is carried as
    # a COLUMN; the per-licence figures survive in `snapshot` JSONB, so a
    # re-upgrade could in principle re-derive them, but the close-path identity
    # gate cannot run in between and any statement closed while the columns are
    # absent is one whose components again cannot account for its total.
    op.drop_column("billing_invoices", "app_request_markup_usd")
    op.drop_column("billing_invoices", "app_request_usage_usd")
