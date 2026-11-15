"""``ADDITIVE_MIGRATIONS`` must be additive, as its name promises.

WHY THIS EXISTS. ``src/db.py`` replays that tuple on EVERY pod start, and every
statement fails soft -- one bad statement must never take down startup. Those
two properties are fine for additions. They are not fine for a REPLACEMENT.

Until 2026-09-01 the tuple dropped and re-added four UNIQUE constraints and one
UNIQUE index on every boot. Between the DROP and the ADD there is a window with
no uniqueness enforcement, and because the ADD fails soft, a failed re-add left
the constraint simply GONE with the pod started and healthy. On
``uq_cost_daily`` -- the upsert key for cost aggregation -- that is the shape of
a double-billing bug. It never bit (the tables are small, so the window is
milliseconds) but nothing bounded it except table size, which grows.

Replacements belong in the alembic chain, where they run once, in a
transaction, under review -- see ``0015_pricing_cost_daily_shapes``, which is
also written to SKIP an object that already has the target shape so it takes no
lock on a live database.

A ONE-WAY drop is still allowed and is not what this guards: dropping a legacy
constraint that is never recreated (``uq_token_budget``) is idempotent and
convergent -- the second boot is a no-op. Relaxations like ``DROP NOT NULL`` are
the same. The defect is specifically DROP-then-RECREATE of the SAME object.
"""

from __future__ import annotations

import re

from db import ADDITIVE_MIGRATIONS

_DROPS = (
    re.compile(r"DROP\s+CONSTRAINT\s+(?:IF\s+EXISTS\s+)?([a-z_][a-z0-9_]*)", re.I),
    re.compile(r"DROP\s+INDEX\s+(?:IF\s+EXISTS\s+)?([a-z_][a-z0-9_]*)", re.I),
)
_CREATES = (
    re.compile(r"ADD\s+CONSTRAINT\s+([a-z_][a-z0-9_]*)", re.I),
    re.compile(
        r"CREATE\s+(?:UNIQUE\s+)?INDEX\s+(?:CONCURRENTLY\s+)?(?:IF\s+NOT\s+EXISTS\s+)?([a-z_][a-z0-9_]*)",
        re.I,
    ),
)


def _names(patterns: tuple[re.Pattern[str], ...]) -> set[str]:
    found: set[str] = set()
    for stmt in ADDITIVE_MIGRATIONS:
        for pat in patterns:
            found.update(m.lower() for m in pat.findall(stmt))
    return found


def test_no_object_is_dropped_and_recreated_on_every_pod_start():
    dropped = _names(_DROPS)
    created = _names(_CREATES)
    replaced = sorted(dropped & created)

    assert not replaced, (
        "These objects are DROPped and re-created by ADDITIVE_MIGRATIONS, which "
        "src/db.py replays on every pod start with soft-failing statements. That "
        "opens a window on every boot where the object does not exist, and a "
        "failed re-create leaves it gone with the pod healthy. Move the "
        "replacement into an alembic migration (see "
        "0015_pricing_cost_daily_shapes, which skips objects already in the "
        f"target shape so it is a no-op on a live DB): {replaced}"
    )


def test_the_guard_can_actually_see_a_replacement():
    """Non-vacuity: the matchers must detect the shape this test forbids.

    Without this, a regex that silently matches nothing would let the assertion
    above pass forever while the tuple filled back up with replacements.
    """
    sample = (
        "ALTER TABLE billing_cost_daily DROP CONSTRAINT IF EXISTS uq_cost_daily",
        "ALTER TABLE billing_cost_daily ADD CONSTRAINT uq_cost_daily UNIQUE (tenant_id)",
        "DROP INDEX IF EXISTS ix_providerpricing_open",
        "CREATE UNIQUE INDEX IF NOT EXISTS ix_providerpricing_open ON provider_pricing (provider)",
    )
    dropped: set[str] = set()
    created: set[str] = set()
    for stmt in sample:
        for pat in _DROPS:
            dropped.update(m.lower() for m in pat.findall(stmt))
        for pat in _CREATES:
            created.update(m.lower() for m in pat.findall(stmt))
    assert dropped & created == {"uq_cost_daily", "ix_providerpricing_open"}


def test_a_one_way_drop_is_not_flagged():
    """The rule must NOT fire on an idempotent, convergent one-way drop."""
    dropped: set[str] = set()
    created: set[str] = set()
    for stmt in ("ALTER TABLE token_budgets DROP CONSTRAINT IF EXISTS uq_token_budget",):
        for pat in _DROPS:
            dropped.update(m.lower() for m in pat.findall(stmt))
        for pat in _CREATES:
            created.update(m.lower() for m in pat.findall(stmt))
    assert dropped == {"uq_token_budget"}
    assert not (dropped & created)
