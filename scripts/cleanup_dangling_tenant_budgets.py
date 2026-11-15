#!/usr/bin/env python3
"""Reconcile token budgets left DANGLING after the licenses orphan-tenant cleanup.

Background
----------
The licenses service ran a ``tenant_id`` FK campaign (Stage 2, 2026-07-23) that
HARD-DELETED 39 ``license`` rows across 23 distinct ``tenant_id`` values —
synthetic/ghost subjects (Keycloak ``sub`` UUIDs that exist in no realm), test
tenants, and one revoked real user. The same ``sub``-persisted-as-``tenant_id``
money-bug that produced those orphan licenses also stamped the SAME values into
``metering.token_budgets.tenant_id``. So metering now carries token budgets keyed
by tenant_ids that no longer have any license behind them: dangling budgets.

**licenses did NOT touch metering's DB (service isolation).** This script is
metering's OWN tooling operating on metering's OWN database — that is the correct
side of the isolation boundary to do the cleanup.

What this script does
---------------------
1. Reads every ``token_budgets`` row whose ``tenant_id`` is one of the deleted
   tenant_ids (read-only) directly from PostgreSQL.
2. Classifies them (pure, unit-testable ``plan_cleanup``):
   - ``to_delete``        — ACTIVE and ``consumed_tokens == 0``: a safe,
     reversible soft-delete candidate (no usage/billing history to lose).
   - ``skipped_consumed`` — ACTIVE but ``consumed_tokens > 0``: NEVER
     auto-deleted. A consumed budget carries usage that may still be billable;
     removing it is an operator decision. Reported, and forces a non-zero exit.
   - ``already_inactive``  — inactive rows: nothing to do (already soft-deleted;
     inert history, kept for audit).
3. With ``--apply``, soft-deletes each ``to_delete`` row via
   ``DELETE /api/budget/{id}`` — the SAME audited path a human uses. That sets
   ``active=False`` (reversible), writes an ``audit_log`` row (identity + before
   snapshot), and re-syncs Redis so enforcement drops the budget. It does NOT
   hard-delete: the row survives and can be re-activated if any deletion is
   disputed.

Default is DRY-RUN. Pass ``--apply`` to act.

Reversibility
-------------
Soft-delete only. Every removed budget remains in the table with
``active=False`` and an ``audit_log`` before-snapshot; re-activation is a single
UPDATE. Re-running after a clean apply reports zero ``to_delete`` (idempotent).

Scope
-----
ONLY ``token_budgets``. Not in scope (verified 2026-07-24 to be either empty or
non-budget telemetry for the deleted tenants): ``request_quota_config`` (0 rows),
``budget_overage_alerts`` (0 rows), ``token_usage`` (0 rows), cost tables (0
rows). Request-count telemetry (``billing_metering_events`` / ``_hourly`` /
``_daily``) is retention-pruned or inert and is a separate decision.

Auth (mirrors ``cleanup_duplicate_budgets.py``)
-----------------------------------------------
  * ``$METERING_ADMIN_TOKEN`` (Bearer), or
  * Keycloak client_credentials via ``$KEYCLOAK_TOKEN_URL`` +
    ``$SERVICE_CLIENT_ID`` + ``$SERVICE_CLIENT_SECRET`` (client must carry the
    metering-internal role for DELETE).

DB access
---------
``$METERING_DATABASE_URL`` — raw read access to metering's Postgres. No fallback
to a default DSN: the operator points it at the right database explicitly.

How to run
----------
    METERING_DATABASE_URL=postgresql+psycopg://... \\
    METERING_BASE_URL=https://metering.example.com    \\
    METERING_ADMIN_TOKEN=<...>                      \\
        uv run python scripts/cleanup_dangling_tenant_budgets.py            # dry-run
        uv run python scripts/cleanup_dangling_tenant_budgets.py --apply    # act

Exit codes
----------
  0  clean (no dangling active budgets, OR all deleted cleanly)
  1  one or more ACTIVE budgets carry consumed_tokens > 0 (skipped, operator
     review) and/or one or more DELETE calls failed
  2  auth, DB, or configuration failure
"""

from __future__ import annotations

import argparse
import logging
import os
import sys
from dataclasses import dataclass, field
from datetime import datetime
from pathlib import Path

import httpx
import psycopg

logger = logging.getLogger("cleanup_dangling_tenant_budgets")

# The 23 tenant_ids whose license rows were HARD-DELETED by the licenses FK
# campaign Stage 2 (2026-07-23). Source of truth for the deletion set:
# ~/.claude/plans/metering-dangling-budgets-after-orphan-cleanup-2026-07-23.md
# Override at runtime with --tenant-ids-file (one id per line).
DELETED_TENANT_IDS: tuple[str, ...] = (
    # Mis-keyed: tenant_id == a user's Keycloak sub.
    "0e076e7c-3291-4367-9fa1-a77af40584ad",
    "0fe60a60-7fac-419d-a40e-d2e73d2770a4",
    "2bcf3ae8-2a68-490a-ab6b-0ca8dc661423",
    "4152c0e8-e872-4608-9209-784b9355d99c",
    "5768e740-bbf8-4a86-b4b4-5b5caa5f909d",
    "597272f4-1ee3-4daf-b1c0-96af341d780b",
    "63b005ad-e5e7-4fd6-9e3a-87d6c6b16d85",
    "969279ce-6508-48de-a9ad-7425cc4c06ab",
    "a4823060-3b73-4145-9988-389ccc1f6035",
    "a7cc48ed-9800-4f27-9123-fbd03b059617",
    "d01b3608-e50a-4376-bf97-c1c96ba4f576",
    "dd0513ac-fcbf-4fa9-bf4b-1b3149874b13",
    "f7b0372a-9ce4-4753-be11-2995b72eea71",
    "70640a77-153d-403b-95bc-0f3b3291b2ef",  # Gerard Kanters — real user, revoked
    "32969c16-6ecd-49fe-a106-ba13e6640dc8",  # fr-test (has real tenant 019efaa0)
    # Ghost / test tenants.
    "019ecc3b-b78d-7391-a31f-c1f71ebd3f5a",
    "019f07d2-b18e-77d6-b664-ecbf0a50b3d8",
    "019f091f-506e-734d-81bc-db1c6e462d9a",
    "019f093f-64ca-7661-8558-12841afe676f",
    "019f0954-7ebe-72a9-bedc-9b6bbbf2540c",
    "8382f0b3-c082-4f97-8d47-120ce5a18e05",  # test-user
    "3806d6e5-4698-571e-9bb2-22ffd076ac73",  # smoke-operator
    "00000000-0000-0000-0000-000000000002",  # 0002 sentinel
)


# ---------------------------------------------------------------------------
# Pure classification logic (unit-testable; no I/O)
# ---------------------------------------------------------------------------


@dataclass(frozen=True)
class BudgetRow:
    """Subset of ``token_budgets`` fields the classifier needs.

    Plain dataclass so tests can build synthetic rows without SQLAlchemy.
    """

    id: str
    tenant_id: str
    tenant_name: str
    license_id: str
    source: str
    active: bool
    budget_tokens: int
    consumed_tokens: int
    created_at: datetime


@dataclass(frozen=True)
class CleanupPlan:
    """Classification of the dangling budgets for a run."""

    to_delete: tuple[BudgetRow, ...] = ()
    skipped_consumed: tuple[BudgetRow, ...] = ()
    already_inactive: tuple[BudgetRow, ...] = ()

    @property
    def is_clean(self) -> bool:
        """True when nothing requires operator attention (no consumed budgets)."""
        return not self.skipped_consumed


def plan_cleanup(rows: list[BudgetRow]) -> CleanupPlan:
    """Classify dangling budget rows into delete / skip / already-inactive.

    Pre-condition: every row already belongs to a deleted tenant_id (the caller
    filters by ``tenant_id = ANY(deleted)`` in SQL). This function decides only
    what is *safe* to touch.

    Rules:
      * inactive row              -> already_inactive (no-op; kept for audit).
      * active, consumed == 0     -> to_delete (safe, reversible soft-delete).
      * active, consumed  > 0     -> skipped_consumed (NEVER auto-delete —
        may carry billable usage; operator must decide).
    """
    to_delete: list[BudgetRow] = []
    skipped_consumed: list[BudgetRow] = []
    already_inactive: list[BudgetRow] = []

    for r in rows:
        if not r.active:
            already_inactive.append(r)
        elif r.consumed_tokens > 0:
            skipped_consumed.append(r)
        else:
            to_delete.append(r)

    return CleanupPlan(
        to_delete=tuple(to_delete),
        skipped_consumed=tuple(skipped_consumed),
        already_inactive=tuple(already_inactive),
    )


# ---------------------------------------------------------------------------
# Deleted-tenant set
# ---------------------------------------------------------------------------


def load_deleted_tenant_ids(path: str | None) -> list[str]:
    """Return the deleted tenant_id set — from a file if given, else the built-in."""
    if not path:
        return list(DELETED_TENANT_IDS)
    ids: list[str] = []
    for line in Path(path).read_text(encoding="utf-8").splitlines():
        stripped = line.strip()
        if stripped and not stripped.startswith("#"):
            ids.append(stripped)
    return ids


# ---------------------------------------------------------------------------
# Auth (mirror of cleanup_duplicate_budgets.py)
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


def fetch_dangling_budgets(database_url: str, tenant_ids: list[str]) -> list[BudgetRow]:
    """Read every ``token_budgets`` row (active OR inactive) for the deleted tenants."""
    # SQLAlchemy-style URLs may carry a +driver suffix; psycopg wants plain.
    dsn = database_url.replace("postgresql+psycopg://", "postgresql://", 1)
    dsn = dsn.replace("postgresql+asyncpg://", "postgresql://", 1)

    rows: list[BudgetRow] = []
    with psycopg.connect(dsn, connect_timeout=10) as conn, conn.cursor() as cur:
        cur.execute(
            """
            SELECT id, tenant_id, tenant_name, license_id, source, active,
                   budget_tokens, consumed_tokens, created_at
              FROM token_budgets
             WHERE tenant_id = ANY(%s::uuid[])
             ORDER BY tenant_id, active DESC, created_at
            """,
            # ``tenant_ids`` is a list[str]; psycopg3 adapts that to ``text[]`` and
            # ``uuid = ANY(text[])`` raises "operator does not exist" since
            # migration 0011 made tenant_id a real uuid. The ``::uuid[]`` cast is
            # the fix — do NOT drop it, and do NOT rely on the driver inferring it.
            (tenant_ids,),
        )
        for r in cur.fetchall():
            rows.append(
                BudgetRow(
                    id=str(r[0]),
                    # str() at the boundary: the driver now hands back a
                    # uuid.UUID, but the dataclass field is declared ``str`` and
                    # the value is f-stringed and re-bound downstream.
                    tenant_id=str(r[1]),
                    tenant_name=r[2],
                    license_id=str(r[3]),
                    source=r[4] or "manual",
                    active=bool(r[5]),
                    budget_tokens=int(r[6] or 0),
                    consumed_tokens=int(r[7] or 0),
                    created_at=r[8],
                )
            )
    return rows


# ---------------------------------------------------------------------------
# Reporting helpers
# ---------------------------------------------------------------------------


def _fmt(r: BudgetRow) -> str:
    return (
        f"id={r.id} tenant={r.tenant_id} name={r.tenant_name!r} "
        f"license={r.license_id} source={r.source} active={r.active} "
        f"budget={r.budget_tokens} consumed={r.consumed_tokens}"
    )


@dataclass
class RunStats:
    deleted: int = 0
    delete_failures: int = 0
    notes: list[str] = field(default_factory=list)


def apply_deletions(plan: CleanupPlan, base_url: str, headers: dict[str, str]) -> RunStats:
    """Soft-delete each ``to_delete`` budget via the audited DELETE endpoint."""
    stats = RunStats()
    with httpx.Client(timeout=15.0, headers=headers) as client:
        for r in plan.to_delete:
            url = f"{base_url}/api/budget/{r.id}"
            try:
                resp = client.delete(url)
            except httpx.HTTPError as exc:
                stats.delete_failures += 1
                print(f"  ERROR DELETE {r.id}: {exc}", file=sys.stderr)
                continue
            if resp.status_code == 204:
                stats.deleted += 1
                print(f"  DELETED {r.id}")
            elif resp.status_code == 404:
                # Already inactive between read and write — benign / idempotent.
                print(f"  note: DELETE {r.id} returned 404 (already inactive)")
            else:
                stats.delete_failures += 1
                print(
                    f"  ERROR DELETE {r.id} returned {resp.status_code}: {resp.text}",
                    file=sys.stderr,
                )
    return stats


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------


def main() -> int:
    parser = argparse.ArgumentParser(
        description=(
            "Soft-delete token budgets left dangling by the licenses orphan-tenant "
            "cleanup. Dry-run by default; pass --apply to act."
        ),
    )
    parser.add_argument(
        "--apply",
        action="store_true",
        help="Actually soft-delete the dangling active budgets (default: dry-run).",
    )
    parser.add_argument(
        "--tenant-ids-file",
        default=None,
        help="Path to a newline-delimited tenant_id list (overrides the built-in 23).",
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

    tenant_ids = load_deleted_tenant_ids(args.tenant_ids_file)
    if not tenant_ids:
        print("ERROR: empty deleted-tenant set.", file=sys.stderr)
        return 2

    base_url = os.environ.get("METERING_BASE_URL", "http://localhost:8157").rstrip("/")
    # Auth is a WRITE-path concern. A dry-run is pure read-only, so never touch
    # Keycloak in that mode (the pod carries SERVICE_CLIENT_* env, and reaching
    # the token endpoint from a one-shot exec can fail — that must not block a
    # read-only inventory).
    headers: dict[str, str] = {}
    if args.apply:
        token = acquire_token()
        if token:
            headers["Authorization"] = f"Bearer {token}"
        else:
            print(
                "ERROR: no admin token; set METERING_ADMIN_TOKEN or "
                "SERVICE_CLIENT_{ID,SECRET}+KEYCLOAK_TOKEN_URL.",
                file=sys.stderr,
            )
            return 2

    try:
        rows = fetch_dangling_budgets(database_url, tenant_ids)
    except psycopg.Error as exc:
        print(f"ERROR: fetching budgets from DB failed: {exc}", file=sys.stderr)
        return 2

    plan = plan_cleanup(rows)

    print(
        f"Scanned {len(tenant_ids)} deleted tenant_ids: "
        f"{len(rows)} token_budgets rows found "
        f"({len(plan.to_delete)} active/consumed=0 to delete, "
        f"{len(plan.skipped_consumed)} active/consumed>0 SKIPPED, "
        f"{len(plan.already_inactive)} already inactive)."
    )

    if plan.skipped_consumed:
        print("\nSKIPPED — active budgets carrying consumed_tokens (operator review):")
        for r in plan.skipped_consumed:
            print(f"  ! {_fmt(r)}")

    if not plan.to_delete:
        print("\nNo dangling active budgets to soft-delete.")
        return 1 if plan.skipped_consumed else 0

    print("\nDangling active budgets (consumed=0) to soft-delete:")
    for r in plan.to_delete:
        print(f"  - {_fmt(r)}")

    if not args.apply:
        print(
            f"\nDry-run: {len(plan.to_delete)} budget(s) WOULD be soft-deleted. "
            "Re-run with --apply to act."
        )
        return 1 if plan.skipped_consumed else 0

    print("\nApplying soft-deletes...")
    stats = apply_deletions(plan, base_url, headers)
    print(
        f"\nSummary: {stats.deleted} soft-deleted, "
        f"{stats.delete_failures} delete failures, "
        f"{len(plan.skipped_consumed)} skipped (consumed>0)."
    )

    if stats.delete_failures or plan.skipped_consumed:
        return 1
    return 0


__all__ = [
    "DELETED_TENANT_IDS",
    "BudgetRow",
    "CleanupPlan",
    "apply_deletions",
    "load_deleted_tenant_ids",
    "plan_cleanup",
    "main",
]


if __name__ == "__main__":
    sys.exit(main())
