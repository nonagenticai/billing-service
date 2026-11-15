"""Opt-in LIVE probe against the real licences service.

⛔ THIS IS NOT THE CONTRACT GATE. ``tests/test_licences_contract.py`` is — it
validates the same response against a VENDORED licences schema, offline, with no
skip path, on every PR. This module is a supplementary probe you point at a
running licences service by hand.

Why it stopped being the gate
-----------------------------
It claimed above this line that "a nightly cron'd CI workflow sets the env vars
and runs ``uv run pytest -m integration ...``". ⚠️ **CORRECTED 2026-09-13 —
measured on ``origin/main``, no such workflow exists and never did.**
``.github/workflows/ci-cd.yaml`` is a 30-line thin wrapper around
``gitops-production/.github/workflows/python-service.yml``; it has **no
``schedule:``, no cron**, and the string ``LICENSES_INTEGRATION`` appears
**nowhere** in this repository outside this file. So the only contract check on
``GET /internal/licenses`` ran in ZERO environments, for as long as it existed.

⚠️ And it is ONE closed gate, not two, though the ``integration`` marker makes it
look like two. Measured: this repo's ``addopts`` (``pyproject.toml``) carries no
``-m`` expression; the archetype's unit pass is
``pytest ${test_dir} -v --tb=short`` (``python-service.yml:2849``) with no marker
expression; and ``ci-cd.yaml`` sets no ``integration_test_select``. The module is
COLLECTED on every CI run and stopped solely by the ``skipif`` below. Deleting
the marker would therefore have changed nothing — worth knowing before anyone
"fixes" the disarm by touching markers.

⚠️ For contrast the licences repo DOES run a real integration pass
(``integration_test_select: "tests/integration -m integration"``). The gap was
one-sided: licences checks what it sends billing, billing checked nothing of
what it receives.

The ``skipif`` below is retained deliberately — a live probe needs a live URL,
and there is no honest way for it to run without one. It is no longer load-
bearing, because the offline gate now carries the contract.

Run it manually with::

    LICENSES_INTEGRATION_URL=https://licenses.example.com \\
    LICENSES_INTEGRATION_TOKEN=... \\
    uv run pytest tests/test_licenses_integration.py

⛔ WHAT THIS MODULE MUST NEVER ASSERT: *which* licences come back, that the set
is non-empty, or anything expiry-dependent. ``internal_list_licenses`` filters
the stored ``status`` column today, while licences' ``seat-count`` applies a
three-part expiry predicate; an open ``nonagenticai/licenses`` PR collapses the
two onto one expiry-aware predicate, under which an empty active set is a
legitimate answer. An assertion on the set would pin today's looser semantics as
the contract and make that fix read as a regression. Shape only.
"""

from __future__ import annotations

import os

import httpx
import pytest

from tests.support.licences_contract import response_violations

INTEGRATION_URL = os.getenv("LICENSES_INTEGRATION_URL", "")
INTEGRATION_TOKEN = os.getenv("LICENSES_INTEGRATION_TOKEN", "")

pytestmark = [
    pytest.mark.integration,
    pytest.mark.skipif(
        not INTEGRATION_URL,
        reason=(
            "live probe only; set LICENSES_INTEGRATION_URL to run it. "
            "The contract itself is checked offline and unconditionally by "
            "tests/test_licences_contract.py"
        ),
    ),
]


def _headers() -> dict[str, str]:
    """Exactly the headers the production caller sends — and no tenant header.

    ``GET /internal/licenses`` is an admin/service call: it takes NO tenant
    dependency on the licences side. It is authorised purely by the service-
    account bearer (``require_internal_service``) and optionally narrowed by a
    ``?tenant_id=`` query param, which neither the production caller nor this
    contract test sends, because the endpoint reconciles ALL tenants in one pass.

    This used to send ``X-Tenant-Id: "__admin__"``. ``/internal`` is in the
    licences service's ``PROTECTED_PREFIXES`` and its ``AuthMiddleware`` runs
    ``validate_tenant_uuid()`` BEFORE it looks at the Authorization header, so
    that sentinel made every call a 400 — never a contract check. There is no
    admin-sentinel convention to opt into, and substituting an arbitrary tenant
    uuid would be worse: it would silently turn this all-tenants reconciliation
    contract into a single-tenant one. The correct expression of "an admin call
    needs no tenant" is to send no tenant header at all — which is what
    ``routers/budget.py::sync_from_licenses`` does.
    """
    h = {"Accept": "application/json"}
    if INTEGRATION_TOKEN:
        h["Authorization"] = f"Bearer {INTEGRATION_TOKEN}"
    return h


def test_licenses_health_reachable() -> None:
    """Connectivity + the licenses /health endpoint returns 200."""
    with httpx.Client(timeout=10.0, verify=False) as client:  # noqa: S501 -- internal LAN
        resp = client.get(f"{INTEGRATION_URL.rstrip('/')}/health")
    assert resp.status_code == 200, f"got {resp.status_code}: {resp.text[:200]}"


def test_internal_licenses_contract() -> None:
    """``GET /internal/licenses?status=active`` returns the SHAPE metering depends on.

    Shape is delegated to ``tests.support.licences_contract.response_violations``
    — the same validator the offline gate uses against the vendored licences
    schema — so the live probe and the offline gate can never disagree about what
    the contract is.
    """
    # Guard the header contract itself: a tenant header here is not merely
    # unnecessary, it is rejected upstream of auth and would make every
    # assertion below unreachable.
    assert "X-Tenant-Id" not in _headers()

    with httpx.Client(timeout=10.0, verify=False) as client:  # noqa: S501
        resp = client.get(
            f"{INTEGRATION_URL.rstrip('/')}/internal/licenses",
            params={"status": "active"},
            headers=_headers(),
        )

    # ⛔ 401/403 IS A FAILURE, NOT A SKIP. This used to `pytest.skip` whenever no
    # token was configured, which made a rotated or revoked service-account
    # credential — i.e. the outage itself, the state in which billing reads zero
    # licences and invoices nothing — indistinguishable from a green run. If you
    # pointed this probe at a live service, you asked it to authenticate.
    assert resp.status_code == 200, f"got {resp.status_code}: {resp.text[:300]}" + (
        "  (no LICENSES_INTEGRATION_TOKEN set — an unauthenticated service "
        "account is the outage, not a reason to pass)"
        if not INTEGRATION_TOKEN
        else ""
    )

    # Shape only — never cardinality. See the module docstring.
    violations = response_violations(resp.json())
    assert violations == [], "licences response drifted from the vendored contract: " + "; ".join(
        violations
    )
