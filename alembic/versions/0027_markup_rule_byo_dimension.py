"""markup_rules.byo — make a tenant's BYO rate expressible at all.

WHY
---
George, 2026-09-10: markup is 20% "globally AND on BYO lines". The second half
was **not expressible**. ``services.markup.resolve_billed_line`` computed

    force_zero = billing_mode == BILLING_MODE_BYO and byo

and fed it to ``compute_billed_usd(force_pct_zero=...)``, so under billing_mode
``byo_platform_fee`` every BYO line carried a FORCED 0% markup and only the
absolute surcharge. There was no row anyone could write to change that, because
``byo`` was not one of the five match dimensions a ``markup_rules`` row scopes on
(tenant_id, license_id, service_key, provider, model).

This migration adds the sixth dimension. ``services.markup`` then honours a
byo-scoped rule when one matches and keeps the forced zero ONLY as the
no-byo-rule fallback, so behaviour is unchanged for every estate that has not
written such a row.

NULL = applies to both BYO and platform-provided lines (the wildcard, matching
every other dimension's convention). TRUE = BYO only. FALSE = platform-provided
only.

THE INDEX REBUILD IS THE LOAD-BEARING HALF
------------------------------------------
``ix_markuprules_open`` is the partial unique that enforces "at most one OPEN row
per match-tuple", and ``services.markup.upsert_markup_rule`` relies on it: it
closes the currently-open row for the same tuple and inserts the new one. If
``byo`` were added to the column but NOT to that index, a byo-scoped rule and the
byo-agnostic rule it refines would occupy the SAME open slot — so writing
"BYO = 20%" would CLOSE the general rule and silently change the rate on
non-BYO lines too. The column without the index is worse than neither.

Both index definitions are recreated from ``models/db/config.py`` verbatim
(``coalesce(byo::text, '')`` — the same text-cast treatment tenant_id and
license_id already need, because ``coalesce(boolean, '')`` has no type
resolution in Postgres).

SAFE AGAINST A LIVE TABLE
-------------------------
Nullable ADD COLUMN takes no table rewrite and no default backfill, and every
pre-existing row becomes ``byo IS NULL`` — the wildcard — which is exactly the
behaviour those rows have today (they match a line regardless of its byo flag).
Measured on prod 2026-09-10 before writing this: ``markup_rules`` held 4 rows,
**0 of them open**, so the unique index is rebuilt over an empty open set.
"""

from __future__ import annotations

from collections.abc import Sequence

from alembic import op

revision: str = "0027_markup_rule_byo"  # <= 32 chars: alembic_version.version_num
down_revision: str | None = "0026_quota_config_status"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


_ADD_COLUMN = "ALTER TABLE markup_rules ADD COLUMN IF NOT EXISTS byo BOOLEAN"
_DROP_COLUMN = "ALTER TABLE markup_rules DROP COLUMN IF EXISTS byo"

_DROP_OPEN_IDX = "DROP INDEX IF EXISTS ix_markuprules_open"

# WITH byo — mirrors models/db/config.py MarkupRule.__table_args__ after 0027.
_CREATE_OPEN_IDX_WITH_BYO = (
    "CREATE UNIQUE INDEX IF NOT EXISTS ix_markuprules_open ON markup_rules ("
    "coalesce(tenant_id::text, ''), "
    "coalesce(license_id::text, ''), "
    "coalesce(service_key, ''), "
    "coalesce(provider, ''), "
    "coalesce(model, ''), "
    "coalesce(byo::text, '')"
    ") WHERE effective_until IS NULL"
)

# WITHOUT byo — the pre-0027 definition, for downgrade.
_CREATE_OPEN_IDX_NO_BYO = (
    "CREATE UNIQUE INDEX IF NOT EXISTS ix_markuprules_open ON markup_rules ("
    "coalesce(tenant_id::text, ''), "
    "coalesce(license_id::text, ''), "
    "coalesce(service_key, ''), "
    "coalesce(provider, ''), "
    "coalesce(model, '')"
    ") WHERE effective_until IS NULL"
)


def upgrade() -> None:
    op.execute(_ADD_COLUMN)
    # Order matters: the column must exist before an index can reference it.
    op.execute(_DROP_OPEN_IDX)
    op.execute(_CREATE_OPEN_IDX_WITH_BYO)


def downgrade() -> None:
    # ⚠️ The index must return to its narrower form BEFORE the column goes, and
    # a downgrade can FAIL here by design: if two open rows differ only in
    # `byo`, the pre-0027 unique cannot be built over them. That is the correct
    # outcome — silently dropping one of two deliberately distinct commercial
    # rules would change what a tenant is billed. Close one of them first.
    op.execute(_DROP_OPEN_IDX)
    op.execute(_CREATE_OPEN_IDX_NO_BYO)
    op.execute(_DROP_COLUMN)
