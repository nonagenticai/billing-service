"""Fail CI when the live OpenAPI spec diverges from ``docs/openapi.json``.

API consumers (license-authz, fr-backend, licenses) generate clients from the
committed snapshot. If a route signature changes without regenerating the
snapshot, those consumers silently break the next time they regenerate. This
test forces the maintainer to run ``uv run python scripts/export-openapi.py``
and commit the diff before merging schema-changing work.
"""

from __future__ import annotations

import json
from pathlib import Path

import pytest

from main import app

EXPECTED_PATH = Path(__file__).resolve().parent.parent / "docs" / "openapi.json"


def _strip_volatile(spec: dict) -> dict:
    """Remove fields that legitimately vary between exports (build tag, etc.)."""
    spec = json.loads(json.dumps(spec))  # deep copy
    info = spec.get("info", {})
    info.pop("version", None)
    return spec


def test_openapi_matches_committed_snapshot():
    if not EXPECTED_PATH.exists():
        pytest.fail(
            f"{EXPECTED_PATH} not found — run "
            "`uv run python scripts/export-openapi.py` and commit the result."
        )

    expected_raw = json.loads(EXPECTED_PATH.read_text())
    expected = _strip_volatile(expected_raw)
    live = _strip_volatile(app.openapi())

    if live == expected:
        return

    # Compose a focused diff so the failure message tells you what to do.
    live_paths = set(live.get("paths", {}).keys())
    expected_paths = set(expected.get("paths", {}).keys())
    added = sorted(live_paths - expected_paths)
    removed = sorted(expected_paths - live_paths)

    msg = [
        "OpenAPI drift detected vs docs/openapi.json.",
        f"  Routes added by code, missing from snapshot: {added or '(none)'}",
        f"  Routes in snapshot, missing from code:       {removed or '(none)'}",
        "",
        "Fix: `uv run python scripts/export-openapi.py` and commit the updated docs/openapi.json.",
    ]
    pytest.fail("\n".join(msg))
