#!/usr/bin/env python3
"""One-shot reconciliation: supersede live provider_pricing rows that diverge
from scripts/initial_pricing.yaml.

WHY THIS EXISTS
---------------
``seed_initial_pricing.py`` is INSERT-IF-ABSENT by design — it never updates an
existing row, it only reports a DIVERGED row and exits 1 (which reddens the
seed CronJob). That invariant is load-bearing (a seeder that can silently
rewrite a live rate is dangerous), so a deliberate, audited migration needs its
own entry point. This is it.

It was written for the cost-unit reclassification (per_token -> per_second /
per_character for the ASR/TTS rows) plus the pgx-local provider-default
supersede ($0 self_hosted_amortized -> $10/$25 per_token). Both are declared in
initial_pricing.yaml; this script makes prod match by POSTing a SUPERSEDING row
through the same audited ``POST /api/config/provider-pricing`` path the CronJob
uses (upsert_pricing closes the prior open row and inserts the new one), so
every change lands in audit_log exactly as an operator's would.

SAFETY
------
* Dry-run by default. It prints every row it WOULD supersede, with the exact
  field diffs, and POSTs nothing. Pass ``--apply`` to write.
* It only touches rows that already exist in prod AND diverge from the YAML. It
  never creates a brand-new (provider, model) row — that is the seeder's job.
* The unit relabels are cost-NEUTRAL: per_token / per_second / per_character
  share identical arithmetic in services/pricing.py, so a relabel does not
  change any computed cost_usd. The pgx-local default is the one intentional
  rate change (operator-approved 2026-07-20).

USAGE (in the billing pod, post-deploy of the per_second/per_character image)
    python scripts/reconcile_pricing_units.py            # dry-run
    python scripts/reconcile_pricing_units.py --apply     # write

Auth + base URL come from the same env as the seed CronJob (METERING_BASE_URL,
SERVICE_CLIENT_ID/SECRET or METERING_ADMIN_TOKEN).
"""

from __future__ import annotations

import argparse
import sys
from typing import Any

import httpx

# Reuse the seeder's auth, loader, comparison, and key helpers verbatim so this
# script cannot drift from the CronJob's behaviour.
from seed_initial_pricing import (  # type: ignore[import-not-found]
    PRICE_FIELDS,
    YAML_PATH,
    _key,
    _to_decimal,
    acquire_token,
    load_seed_data,
    prices_match,
)

# The base_url import is deferred to main() so a missing env var fails loudly
# there rather than at import time.


def _diverged_fields(yaml_row: dict[str, Any], existing: dict[str, Any]) -> list[str]:
    diffs: list[str] = []
    for field in PRICE_FIELDS:
        y = yaml_row.get(field)
        e = existing.get(field)
        if field == "cost_model":
            if (y or None) != (e or None):
                diffs.append(f"{field}: yaml={y!r} db={e!r}")
            continue
        if _to_decimal(y) != _to_decimal(e):
            diffs.append(f"{field}: yaml={y!r} db={e!r}")
    return diffs


def _payload(row: dict[str, Any], effective_from: str) -> dict[str, Any]:
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
    return payload


def main() -> int:
    import os

    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--apply",
        action="store_true",
        help="Actually POST the superseding rows. Without this it is a dry-run.",
    )
    args = parser.parse_args()

    base_url = os.environ.get("METERING_BASE_URL", "http://localhost:8156").rstrip("/")
    token = acquire_token()
    headers: dict[str, str] = {}
    if token:
        headers["Authorization"] = f"Bearer {token}"

    effective_from, yaml_rows = load_seed_data(YAML_PATH)
    list_url = f"{base_url}/api/config/provider-pricing"

    with httpx.Client(timeout=15.0, headers=headers) as client:
        resp = client.get(list_url)
    if resp.status_code != 200:
        print(f"ERROR: GET {list_url} returned {resp.status_code}: {resp.text}", file=sys.stderr)
        return 2
    body = resp.json()
    existing_rows = body["data"] if isinstance(body, dict) and "data" in body else body
    existing_by_key = {
        (r["provider"], r.get("model"), r.get("rate_class") or "cost", r.get("service_key")): r
        for r in existing_rows
    }

    to_supersede: list[tuple[str, dict[str, Any], list[str]]] = []
    for row in yaml_rows:
        key = _key(row)
        existing = existing_by_key.get(key)
        if existing is None:
            continue  # absent rows are the seeder's job, not ours
        if prices_match(row, existing):
            continue  # already reconciled
        label = f"{key[0]}/{key[1] or '<default>'} [{key[2]}{'/' + key[3] if key[3] else ''}]"
        to_supersede.append((label, row, _diverged_fields(row, existing)))

    if not to_supersede:
        print("Nothing to reconcile — every prod row already matches the YAML.")
        return 0

    print(f"{len(to_supersede)} diverged row(s) to supersede:")
    for label, _row, diffs in to_supersede:
        print(f"  {label}\n      {'; '.join(diffs)}")

    if not args.apply:
        print("\nDRY-RUN — nothing written. Re-run with --apply to POST these superseding rows.")
        return 0

    posted = 0
    failed = 0
    with httpx.Client(timeout=15.0, headers=headers) as client:
        for label, row, _diffs in to_supersede:
            resp = client.post(list_url, json=_payload(row, effective_from))
            if resp.status_code in (200, 201):
                print(f"superseded: {label}")
                posted += 1
            else:
                print(
                    f"ERROR: POST for {label} returned {resp.status_code}: {resp.text}",
                    file=sys.stderr,
                )
                failed += 1

    print(f"\n{posted} superseded, {failed} failed.")
    return 1 if failed else 0


if __name__ == "__main__":
    raise SystemExit(main())
