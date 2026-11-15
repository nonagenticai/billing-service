"""Record WHERE a frozen invoice's markup percentage came from.

WHY A COLUMN AND NOT JUST THE SNAPSHOT
--------------------------------------
``billing_invoices`` already carries ``markup_pct`` and ``effective_markup_pct``.
Neither says where the rate came from, and there are four possible answers:

* an effective-dated ``markup_rules`` row  -> ``markup_rule:<id>``
* this tenant's ``tenant_config.markup_pct`` -> ``tenant_config``
* the global default ``tenant_config`` row   -> ``tenant_config_default``
* ``settings.default_markup_pct``            -> ``settings_default``

The first three are commercial decisions somebody made and can be produced on
request. The fourth is a CODE CONSTANT that nobody chose for this customer. They
are indistinguishable in ``markup_pct`` alone -- 30% is 30% -- and a statement
that cannot name its own rate card is not reconcilable.

⭐ THIS MATTERS *BECAUSE* THE ANSWER IS CURRENTLY THE BORING ONE. Measured
2026-09-08: ``markup_rules`` holds 4 rows and every one is CLOSED
(``effective_until`` set, including the global default), and ``tenant_config``
holds 0 rows. So today every tenant silently rides the settings constant, and
there is no open effective-dated rule behind any invoice. That is exactly the
state in which the distinction is invisible, which is why it is worth freezing
now, while ``billing_invoices`` still has 0 rows and nothing has frozen wrong.

WHY ``'unknown'`` IS THE DEFAULT
--------------------------------
A row written before this column existed genuinely has no provenance. Defaulting
it to ``settings_default`` -- the value that happens to be right for every
tenant today -- would INVENT a fact and make an unrecorded source
indistinguishable from a recorded one. ``'unknown'`` reads as what it is. The
generator always writes a real source, so no fresh close ever takes the default.

⚠️ NOT NULL is safe here: ``billing_invoices`` has 0 rows (measured 2026-09-08),
so the server_default backfills nothing and the ALTER takes no meaningful lock.

This adds a COLUMN to a table that already exists; it does not create a table,
so 0024's "every table added after 0018 is born unguarded" hazard does not
apply -- ``billing_invoices`` keeps whatever guard it was given at 0024.
"""

from __future__ import annotations

from collections.abc import Sequence

import sqlalchemy as sa
from alembic import op

revision: str = "0025_invoice_markup_source"  # <= 32 chars: alembic_version.version_num
down_revision: str | None = "0024_billing_invoices"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    op.add_column(
        "billing_invoices",
        sa.Column(
            "markup_source",
            sa.Text(),
            server_default="unknown",
            nullable=False,
        ),
    )


def downgrade() -> None:
    op.drop_column("billing_invoices", "markup_source")
