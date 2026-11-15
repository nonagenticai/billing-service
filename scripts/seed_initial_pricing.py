#!/usr/bin/env python3
"""Seed provider_pricing from scripts/initial_pricing.yaml. Idempotent.

This script POSTs to /api/config/provider-pricing so each insert creates an
audit_log entry — same path a human admin would take. It does NOT talk to
PostgreSQL directly. That's deliberate: the audit log must capture provenance
for every pricing row, including the seed.

Auth:
  - If $METERING_ADMIN_TOKEN is set, use it as Bearer.
  - Else, fetch via Keycloak client_credentials using $KEYCLOAK_TOKEN_URL,
    $SERVICE_CLIENT_ID, $SERVICE_CLIENT_SECRET (client must have
    license-admin role).

Endpoint base:
  - $METERING_BASE_URL or default http://localhost:8156
  - GET  /api/config/provider-pricing  (list current effective rows)
  - POST /api/config/provider-pricing   (insert)

Behavior:
  1. GET current rows.
  2. For each YAML entry, look up by (provider, model):
     - If exists with same prices: skip (logged as "skip: already seeded").
     - If exists with different prices: skip and flag as "diverged" — operator
       must reconcile by POSTing a new row themselves so the audit_log
       captures their identity.
     - If absent: POST it.
  3. Print a summary line.

Exit codes:
  0  all rows present-and-matching or newly-created
  1  one or more rows diverged (operator must reconcile)
  2  auth or network failure
"""

from __future__ import annotations

import os
import sys
from decimal import Decimal
from pathlib import Path
from typing import Any

import httpx
import yaml

SCRIPT_DIR = Path(__file__).resolve().parent
YAML_PATH = SCRIPT_DIR / "initial_pricing.yaml"

# Fields we compare for divergence — anything that affects cost computation.
PRICE_FIELDS = (
    "cost_model",
    "input_per_1m_usd",
    "output_per_1m_usd",
    "cache_creation_per_1m_usd",
    "cache_read_per_1m_usd",
    "per_request_usd",
    "monthly_fixed_cost_usd",
)


# --- YAML loading ---


def load_seed_data(path: Path) -> tuple[str, list[dict[str, Any]]]:
    """Return (effective_from, rows). Raises on schema problems."""
    with path.open("r", encoding="utf-8") as fh:
        doc = yaml.safe_load(fh)
    if not isinstance(doc, dict):
        raise ValueError(f"{path}: expected a top-level mapping")
    if doc.get("version") != 1:
        raise ValueError(f"{path}: only version=1 is supported, got {doc.get('version')!r}")
    effective_from = doc.get("effective_from")
    if not isinstance(effective_from, str):
        raise ValueError(f"{path}: 'effective_from' must be an ISO-8601 string")
    rows = doc.get("rows")
    if not isinstance(rows, list) or not rows:
        raise ValueError(f"{path}: 'rows' must be a non-empty list")
    for i, row in enumerate(rows):
        if not isinstance(row, dict):
            raise ValueError(f"{path}: rows[{i}] is not a mapping")
        if "provider" not in row:
            raise ValueError(f"{path}: rows[{i}] missing 'provider'")
        if "cost_model" not in row:
            raise ValueError(f"{path}: rows[{i}] missing 'cost_model'")
    return effective_from, rows


# --- Auth ---


def acquire_token() -> str | None:
    """Return a bearer token string, or None if no auth configured.

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


# --- Comparison ---


def _to_decimal(v: Any) -> Decimal | None:
    if v is None:
        return None
    return Decimal(str(v))


def prices_match(yaml_row: dict[str, Any], existing: dict[str, Any]) -> bool:
    """True iff every cost-relevant field matches between YAML and DB row."""
    for field in PRICE_FIELDS:
        y = yaml_row.get(field)
        e = existing.get(field)
        if field == "cost_model":
            if (y or None) != (e or None):
                return False
            continue
        if _to_decimal(y) != _to_decimal(e):
            return False
    return True


def _key(row: dict[str, Any]) -> tuple[str, str | None, str, str | None]:
    # Match the DB open-row uniqueness (rate_class, provider, model, service_key)
    # exactly, so the seeder dedup does not (a) collide a 'list' row with the
    # 'cost' row of the same (provider, model), nor (b) collide a service_key-
    # scoped row (e.g. w2v-vlm gpt-4o) with the NULL-service_key LLM row of the
    # same (provider, model).
    return (
        row["provider"],
        row.get("model"),
        row.get("rate_class", "cost"),
        row.get("service_key"),
    )


# --- Main ---


def main() -> int:
    base_url = os.environ.get("METERING_BASE_URL", "http://localhost:8156").rstrip("/")
    token = acquire_token()
    headers: dict[str, str] = {}
    if token:
        headers["Authorization"] = f"Bearer {token}"

    effective_from, yaml_rows = load_seed_data(YAML_PATH)

    list_url = f"{base_url}/api/config/provider-pricing"
    post_url = list_url

    try:
        with httpx.Client(timeout=15.0, headers=headers) as client:
            resp = client.get(list_url)
    except httpx.HTTPError as exc:
        print(f"ERROR: GET {list_url} failed: {exc}", file=sys.stderr)
        return 2
    if resp.status_code == 401 or resp.status_code == 403:
        print(
            f"ERROR: GET {list_url} returned {resp.status_code}; "
            "set METERING_ADMIN_TOKEN or SERVICE_CLIENT_{ID,SECRET}",
            file=sys.stderr,
        )
        return 2
    if resp.status_code != 200:
        print(f"ERROR: GET {list_url} returned {resp.status_code}: {resp.text}", file=sys.stderr)
        return 2
    # /api/config/provider-pricing wraps the list in an Envelope ({"data": [...]}).
    # Older versions of this script assumed a bare list, which silently broke
    # post-Envelope rollout — the script TypeError'd on string-indexing.
    body = resp.json()
    existing_rows: list[dict[str, Any]] = (
        body["data"] if isinstance(body, dict) and "data" in body else body
    )
    existing_by_key = {
        (r["provider"], r.get("model"), r.get("rate_class") or "cost", r.get("service_key")): r
        for r in existing_rows
    }

    created = 0
    skipped = 0
    diverged = 0

    with httpx.Client(timeout=15.0, headers=headers) as client:
        for row in yaml_rows:
            key = _key(row)
            label = f"{key[0]}/{key[1] or '<default>'} [{key[2]}{'/' + key[3] if key[3] else ''}]"
            existing = existing_by_key.get(key)
            if existing is not None:
                if prices_match(row, existing):
                    print(f"skip: already seeded {label}")
                    skipped += 1
                    continue
                diverged_fields = []
                for field in PRICE_FIELDS:
                    y = row.get(field)
                    e = existing.get(field)
                    if field == "cost_model":
                        if (y or None) != (e or None):
                            diverged_fields.append(f"{field}: yaml={y!r} db={e!r}")
                        continue
                    if _to_decimal(y) != _to_decimal(e):
                        diverged_fields.append(f"{field}: yaml={y!r} db={e!r}")
                print(
                    f"DIVERGED: {label} — DB has different prices than YAML; "
                    "operator must reconcile via POST. Fields: " + "; ".join(diverged_fields)
                )
                diverged += 1
                continue

            payload: dict[str, Any] = {
                "provider": row["provider"],
                "model": row.get("model"),
                "rate_class": row.get("rate_class", "cost"),
                "cost_model": row["cost_model"],
                "effective_from": effective_from,
            }
            if row.get("service_key") is not None:
                payload["service_key"] = row["service_key"]
            for field in (
                "input_per_1m_usd",
                "output_per_1m_usd",
                "cache_creation_per_1m_usd",
                "cache_read_per_1m_usd",
                "per_request_usd",
                "monthly_fixed_cost_usd",
            ):
                if row.get(field) is not None:
                    payload[field] = str(row[field])
            if row.get("notes"):
                payload["notes"] = row["notes"]

            try:
                resp = client.post(post_url, json=payload)
            except httpx.HTTPError as exc:
                print(f"ERROR: POST {post_url} for {label} failed: {exc}", file=sys.stderr)
                return 2
            if resp.status_code not in (200, 201):
                print(
                    f"ERROR: POST {post_url} for {label} returned {resp.status_code}: {resp.text}",
                    file=sys.stderr,
                )
                return 2
            print(f"created: {label}")
            created += 1

    print(f"\nSummary: {created} created, {skipped} skipped, {diverged} diverged")
    if diverged:
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(main())
