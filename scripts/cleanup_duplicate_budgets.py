#!/usr/bin/env python3
"""One-shot reconciler for duplicate token budgets per (tenant, license, period).

Background
----------
Before bug B2 was fixed (see commit history of routers/budget.py and
licenses/main.py:_provision_budget), the auto-provision path on license
issuance used the default ``source='manual'`` while the periodic
sync-from-licenses pull uniquely keyed on ``source='license_sync'``. The two
paths were invisible to each other and each created their own active
TokenBudget row for the same ``(tenant_id, license_id, period)``. The
cascade-drain treated them as stacked, doubling the effective entitlement.

The fix on the metering side routes ``POST /api/budget`` payloads carrying
``source='license_sync'`` to ``upsert_license_sync_budget`` (idempotent).
The fix on the licenses side passes that source explicitly. This script
cleans up rows that were created by the buggy code paths.

What this script does
---------------------
1. Reads every active TokenBudget row directly from PostgreSQL (read-only).
2. Groups by ``(tenant_id, license_id, period)``.
3. For groups with >1 active rows, picks a keeper:
   - Exactly one ``source='manual'`` and one ``source='license_sync'``:
     keep whichever has the higher ``consumed_tokens`` (preserves usage
     history). Tie-breaker: keep the older ``created_at``.
   - Multiple manuals or multiple license_syncs (or other shapes): SKIP,
     log as unresolved — operator must investigate.
4. Soft-deletes each non-keeper via ``DELETE /api/budget/{id}`` so the
   metering audit_log captures the identity / request / before-snapshot.
5. Prints a structured summary; --dry-run shows the plan without acting.

Idempotent
----------
Re-running on a cleaned dataset produces zero changes (no group has >1
active row).

Auth
----
Same shape as ``seed_initial_pricing.py``:
  * ``$METERING_ADMIN_TOKEN`` (Bearer), or
  * Keycloak client_credentials via ``$KEYCLOAK_TOKEN_URL`` +
    ``$SERVICE_CLIENT_ID`` + ``$SERVICE_CLIENT_SECRET``. The client must
    carry ``metering-internal`` (or equivalent) role for DELETE.

DB access
---------
The script needs raw read access to the metering Postgres so it can list
budgets across all tenants (the existing API only exposes per-tenant
list endpoints). Configure via ``$METERING_DATABASE_URL`` — this script
will NOT fall back to the metering service's default DSN, the operator
must point it at the right database explicitly.

How to run (post-deploy of the bug fix)
---------------------------------------
1. Wait for the metering deployment with the source-aware route to roll.
2. Bootstrap a one-shot service account with metering-internal role.
3. Dry-run::

    SERVICE_CLIENT_ID=metering-cleanup SERVICE_CLIENT_SECRET=<...> \\
    METERING_DATABASE_URL=postgresql+psycopg://...  \\
    METERING_BASE_URL=https://metering.example.com     \\
    KEYCLOAK_TOKEN_URL=https://auth.../token        \\
        uv run python scripts/cleanup_duplicate_budgets.py --dry-run

4. Inspect the printed plan, then re-run without --dry-run.

Exit codes
----------
  0  clean run (no duplicates, OR all duplicates resolved cleanly)
  1  one or more groups unresolvable (multiple manuals etc. — operator
     intervention required); cleanup did NOT touch unresolved groups
  2  auth, DB, or HTTP failure mid-run
"""

from __future__ import annotations

import argparse
import logging
import os
import sys
from dataclasses import dataclass
from datetime import datetime
from typing import Any

import httpx
import psycopg

logger = logging.getLogger("cleanup_duplicate_budgets")


# ---------------------------------------------------------------------------
# Pure resolution logic (unit-testable; no I/O)
# ---------------------------------------------------------------------------


@dataclass(frozen=True)
class BudgetRow:
    """Subset of TokenBudget fields the resolver needs.

    Kept as a plain dataclass so the unit test can construct synthetic rows
    without having to spin up SQLAlchemy.
    """

    id: str
    tenant_id: str
    license_id: str
    period: str
    source: str
    consumed_tokens: int
    created_at: datetime
    active: bool = True


@dataclass(frozen=True)
class Resolution:
    """Outcome for one (tenant_id, license_id, period) group."""

    keeper: BudgetRow | None
    delete: tuple[BudgetRow, ...]
    unresolved_reason: str | None  # None means resolved


def resolve_group(rows: list[BudgetRow]) -> Resolution:
    """Decide which budget to keep for a single group.

    Pre-conditions:
      * ``rows`` all share the same (tenant_id, license_id, period).
      * Caller has already filtered to ``active=True``.
      * len(rows) >= 1.

    Behavior:
      * len == 1: nothing to do, that row is the keeper.
      * len >= 2 with exactly one 'manual' and one 'license_sync': keep
        the one with higher consumed_tokens (preserves usage history).
        Tie-break by older created_at. The other becomes the delete target.
      * len >= 2 with any other shape (e.g. 2 manual, 3 license_sync,
        unknown sources): mark unresolved with a human-readable reason.
        The script will skip these — operator must investigate.
    """
    if not rows:
        # Defensive — caller groups by key, so this shouldn't happen.
        return Resolution(keeper=None, delete=(), unresolved_reason="empty group")

    if len(rows) == 1:
        return Resolution(keeper=rows[0], delete=(), unresolved_reason=None)

    sources = [r.source for r in rows]
    manuals = [r for r in rows if r.source == "manual"]
    license_syncs = [r for r in rows if r.source == "license_sync"]
    others = [r for r in rows if r.source not in {"manual", "license_sync"}]

    # The expected B2 shape: exactly one manual + exactly one license_sync.
    if len(manuals) == 1 and len(license_syncs) == 1 and not others and len(rows) == 2:
        a, b = manuals[0], license_syncs[0]
        if a.consumed_tokens > b.consumed_tokens:
            keeper, drop = a, b
        elif b.consumed_tokens > a.consumed_tokens:
            keeper, drop = b, a
        else:
            # Tie on consumed_tokens — keep older created_at.
            keeper, drop = (a, b) if a.created_at <= b.created_at else (b, a)
        return Resolution(keeper=keeper, delete=(drop,), unresolved_reason=None)

    # Anything else is operator territory.
    return Resolution(
        keeper=None,
        delete=(),
        unresolved_reason=(f"unexpected shape: {len(rows)} active rows, sources={sorted(sources)}"),
    )


# ---------------------------------------------------------------------------
# Auth (mirror of seed_initial_pricing.py)
# ---------------------------------------------------------------------------


def acquire_token() -> str | None:
    """Return a bearer token, or None if no auth configured.

    Exits with code 2 if Keycloak is configured but the call fails.
    """
    explicit = os.environ.get("METERING_ADMIN_TOKEN")
    if explicit:
        return explicit

    token_url = os.environ.get("KEYCLOAK_TOKEN_URL")
    client_id = os.environ.get("SERVICE_CLIENT_ID")
    client_secret = os.environ.get("SERVICE_CLIENT_SECRET")
    if not (token_url and client_id and client_secret):
        return None

    try:
        with httpx.Client(timeout=10.0) as client:
            resp = client.post(
                token_url,
                data={
                    "grant_type": "client_credentials",
                    "client_id": client_id,
                    "client_secret": client_secret,
                },
            )
    except httpx.HTTPError as exc:
        print(f"ERROR: keycloak token request failed: {exc}", file=sys.stderr)
        sys.exit(2)
    if resp.status_code != 200:
        print(
            f"ERROR: keycloak token endpoint returned {resp.status_code}: {resp.text}",
            file=sys.stderr,
        )
        sys.exit(2)
    body = resp.json()
    token = body.get("access_token")
    if not isinstance(token, str):
        print("ERROR: keycloak response missing access_token", file=sys.stderr)
        sys.exit(2)
    return token


# ---------------------------------------------------------------------------
# DB read (sync psycopg — script runs once, no event loop needed)
# ---------------------------------------------------------------------------


def fetch_active_budgets(database_url: str) -> list[BudgetRow]:
    """Read every active TokenBudget row across all tenants."""
    # SQLAlchemy-style URLs may have a +driver suffix; psycopg wants plain.
    dsn = database_url.replace("postgresql+psycopg://", "postgresql://", 1)

    rows: list[BudgetRow] = []
    with psycopg.connect(dsn) as conn, conn.cursor() as cur:
        cur.execute(
            """
            SELECT id, tenant_id, license_id, period, source,
                   consumed_tokens, created_at
              FROM token_budgets
             WHERE active = TRUE
            """
        )
        for r in cur.fetchall():
            rid, tenant_id, license_id, period, source, consumed, created_at = r
            rows.append(
                BudgetRow(
                    id=str(rid),
                    # str() like every other id here: since migration 0011 the
                    # driver returns a uuid.UUID for tenant_id. An unwrapped
                    # uuid.UUID never compares equal to the str-wrapped values
                    # used beside it in the group key below, so duplicate groups
                    # would split and the script would silently find nothing.
                    tenant_id=str(tenant_id),
                    license_id=str(license_id),
                    period=period,
                    source=source or "manual",
                    consumed_tokens=int(consumed or 0),
                    created_at=created_at,
                    active=True,
                )
            )
    return rows


# ---------------------------------------------------------------------------
# Grouping
# ---------------------------------------------------------------------------


def group_rows(rows: list[BudgetRow]) -> dict[tuple[str, str, str], list[BudgetRow]]:
    """Group active rows by (tenant_id, license_id, period)."""
    groups: dict[tuple[str, str, str], list[BudgetRow]] = {}
    for r in rows:
        key = (r.tenant_id, r.license_id, r.period)
        groups.setdefault(key, []).append(r)
    return groups


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------


def _format_row(r: BudgetRow) -> str:
    return (
        f"id={r.id} source={r.source} "
        f"consumed={r.consumed_tokens} created={r.created_at.isoformat()}"
    )


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Reconcile duplicate active TokenBudget rows per (tenant, license, period).",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Print the resolution plan without calling DELETE.",
    )
    args = parser.parse_args()

    logging.basicConfig(level=logging.INFO, format="%(message)s")

    database_url = os.environ.get("METERING_DATABASE_URL")
    if not database_url:
        print(
            "ERROR: METERING_DATABASE_URL is required (point at metering's Postgres).",
            file=sys.stderr,
        )
        return 2

    base_url = os.environ.get("METERING_BASE_URL", "http://localhost:8156").rstrip("/")
    token = acquire_token()
    headers: dict[str, str] = {}
    if token:
        headers["Authorization"] = f"Bearer {token}"
    elif not args.dry_run:
        print(
            "ERROR: no admin token; set METERING_ADMIN_TOKEN or "
            "SERVICE_CLIENT_{ID,SECRET}+KEYCLOAK_TOKEN_URL.",
            file=sys.stderr,
        )
        return 2

    try:
        all_rows = fetch_active_budgets(database_url)
    except psycopg.Error as exc:
        print(f"ERROR: fetching budgets from DB failed: {exc}", file=sys.stderr)
        return 2

    groups = group_rows(all_rows)
    multi_groups = {k: v for k, v in groups.items() if len(v) > 1}

    examined = len(groups)
    duplicates_planned = 0
    deleted = 0
    unresolved = 0
    delete_failures = 0

    print(
        f"Examined {examined} groups; {len(multi_groups)} have >1 active rows "
        f"({sum(len(v) for v in multi_groups.values())} rows total in those groups)."
    )

    if not multi_groups:
        print("No duplicates to clean up.")
        return 0

    # Resolve and (optionally) delete.
    with httpx.Client(timeout=15.0, headers=headers) as client:
        for key, rows in sorted(multi_groups.items()):
            tenant_id, license_id, period = key
            res = resolve_group(rows)

            label = f"tenant={tenant_id} license={license_id} period={period}"
            if res.unresolved_reason is not None:
                unresolved += 1
                print(f"UNRESOLVED: {label} — {res.unresolved_reason}")
                for r in rows:
                    print(f"  - {_format_row(r)}")
                continue

            assert res.keeper is not None  # resolved => keeper set
            duplicates_planned += len(res.delete)
            print(f"GROUP: {label}")
            print(f"  KEEP   {_format_row(res.keeper)}")
            for drop in res.delete:
                print(f"  DELETE {_format_row(drop)}")

            if args.dry_run:
                continue

            for drop in res.delete:
                url = f"{base_url}/api/budget/{drop.id}"
                try:
                    resp = client.delete(url)
                except httpx.HTTPError as exc:
                    delete_failures += 1
                    print(f"  ERROR DELETE {drop.id}: {exc}", file=sys.stderr)
                    continue
                if resp.status_code == 204:
                    deleted += 1
                elif resp.status_code == 404:
                    # Already inactive; fine.
                    print(f"  note: DELETE {drop.id} returned 404 (already inactive)")
                else:
                    delete_failures += 1
                    print(
                        f"  ERROR DELETE {drop.id} returned {resp.status_code}: {resp.text}",
                        file=sys.stderr,
                    )

    if args.dry_run:
        print(
            f"\nDry-run summary: {examined} groups examined, "
            f"{len(multi_groups)} duplicate-groups, "
            f"{duplicates_planned} rows would be soft-deleted, "
            f"{unresolved} unresolved (skipped)."
        )
    else:
        print(
            f"\nSummary: {examined} groups examined, "
            f"{len(multi_groups)} duplicate-groups, "
            f"{deleted} rows soft-deleted, "
            f"{unresolved} unresolved (skipped), "
            f"{delete_failures} delete failures."
        )

    if unresolved or delete_failures:
        return 1
    return 0


# Re-export resolve_group for the test module without importing main().
__all__ = [
    "BudgetRow",
    "Resolution",
    "resolve_group",
    "group_rows",
    "main",
]


def _main_for_pytest() -> Any:  # pragma: no cover  - convenience for ad-hoc invocation
    return main()


if __name__ == "__main__":
    sys.exit(main())
