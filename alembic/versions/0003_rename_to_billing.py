"""Layer-4 internal-table rename: metering_*/cost_* -> billing_*.

In-place ``ALTER TABLE ... RENAME`` (Path B) — metadata-only, preserves all
rows (live DB holds ~349k cost_events + ~731k metering_events). Renames the
eight billing base tables plus the 14 monthly ``cost_events`` partitions to the
``billing_`` prefix, and recreates ``create_cost_events_partition_for`` so newly
minted partitions attach to (and are named after) ``billing_cost_events``.

``processed_cost_messages`` (idempotency inbox) is deliberately left unchanged.
Indexes / constraints / FKs are NOT renamed — their names are already drifted,
purely cosmetic, and functionally inert.

Revision ID: 0003_rename_to_billing
Revises: 0002_processed_cost_messages
Create Date: 2026-07-16
"""

from __future__ import annotations

from alembic import op

revision = "0003_rename_to_billing"
down_revision = "0002_processed_cost_messages"
branch_labels = None
depends_on = None


# Base tables renamed by this migration (billing_ prefix + existing name).
# Partition attachment is by OID (not name), so renaming the parent does not
# detach its children and rename order is not load-bearing.
_BASE_RENAMES: tuple[tuple[str, str], ...] = (
    ("cost_events", "billing_cost_events"),
    ("cost_daily", "billing_cost_daily"),
    ("metering_events", "billing_metering_events"),
    ("metering_hourly", "billing_metering_hourly"),
    ("metering_daily", "billing_metering_daily"),
    ("metering_monthly_usage", "billing_metering_monthly_usage"),
    ("provider_costs", "billing_provider_costs"),
    ("app_cost_rates", "billing_app_cost_rates"),
)

# The 14 monthly RANGE partitions of cost_events (verified against
# 0001_billing_baseline.py: cost_events_2026_04 .. 2026_12, 2027_01 .. 2027_05).
_PARTITION_MONTHS: tuple[str, ...] = (
    "2026_04",
    "2026_05",
    "2026_06",
    "2026_07",
    "2026_08",
    "2026_09",
    "2026_10",
    "2026_11",
    "2026_12",
    "2027_01",
    "2027_02",
    "2027_03",
    "2027_04",
    "2027_05",
)


# create_cost_events_partition_for retargeted at billing_cost_events. Body is
# copied verbatim from 0001_billing_baseline.py with the two name references
# changed: ``PARTITION OF cost_events`` and ``format('cost_events_%s_%s', ...)``.
_FN_BILLING = r"""
CREATE OR REPLACE FUNCTION public.create_cost_events_partition_for(p_year integer, p_month integer)
 RETURNS text
 LANGUAGE plpgsql
AS $function$
        DECLARE
            partition_name TEXT;
            from_date TEXT;
            to_date TEXT;
        BEGIN
            partition_name := format('billing_cost_events_%s_%s',
                lpad(p_year::text, 4, '0'),
                lpad(p_month::text, 2, '0'));
            from_date := format('%s-%s-01',
                lpad(p_year::text, 4, '0'),
                lpad(p_month::text, 2, '0'));
            -- Compute the next month's start by adding 1 month then truncating.
            to_date := to_char(
                (from_date::timestamp + interval '1 month')::date,
                'YYYY-MM-01'
            );

            EXECUTE format(
                'CREATE TABLE IF NOT EXISTS %I PARTITION OF billing_cost_events
                 FOR VALUES FROM (%L) TO (%L)',
                partition_name, from_date, to_date
            );
            RETURN partition_name;
        END
        $function$;
"""

# Original (pre-rename) function body targeting cost_events — used by downgrade.
_FN_LEGACY = r"""
CREATE OR REPLACE FUNCTION public.create_cost_events_partition_for(p_year integer, p_month integer)
 RETURNS text
 LANGUAGE plpgsql
AS $function$
        DECLARE
            partition_name TEXT;
            from_date TEXT;
            to_date TEXT;
        BEGIN
            partition_name := format('cost_events_%s_%s',
                lpad(p_year::text, 4, '0'),
                lpad(p_month::text, 2, '0'));
            from_date := format('%s-%s-01',
                lpad(p_year::text, 4, '0'),
                lpad(p_month::text, 2, '0'));
            -- Compute the next month's start by adding 1 month then truncating.
            to_date := to_char(
                (from_date::timestamp + interval '1 month')::date,
                'YYYY-MM-01'
            );

            EXECUTE format(
                'CREATE TABLE IF NOT EXISTS %I PARTITION OF cost_events
                 FOR VALUES FROM (%L) TO (%L)',
                partition_name, from_date, to_date
            );
            RETURN partition_name;
        END
        $function$;
"""


def upgrade() -> None:
    # Rename the 14 monthly partitions first (children keep their OID-based
    # attachment to the parent across the parent's own rename, so order is not
    # load-bearing — done first only for tidy log ordering).
    for month in _PARTITION_MONTHS:
        op.execute(f"ALTER TABLE public.cost_events_{month} RENAME TO billing_cost_events_{month}")
    # Rename the eight base tables (parent cost_events included).
    for old, new in _BASE_RENAMES:
        op.execute(f"ALTER TABLE public.{old} RENAME TO {new}")
    # Retarget the partition-minting helper at billing_cost_events.
    # create_cost_events_partition_for_next_month() only calls this by name
    # (no hardcoded table), so it needs no change.
    op.execute(_FN_BILLING)


def downgrade() -> None:
    op.execute(_FN_LEGACY)
    for old, new in _BASE_RENAMES:
        op.execute(f"ALTER TABLE public.{new} RENAME TO {old}")
    for month in _PARTITION_MONTHS:
        op.execute(f"ALTER TABLE public.billing_cost_events_{month} RENAME TO cost_events_{month}")
