"""The billing CSV export must carry ``project_id``.

``/api/export/billing.csv`` is the invoice-reconciliation surface — its own
docstring says "it must sum to what the customer is billed". Since markup
prices per ``(tenant, project)`` rule, two cost events byte-identical in every
other exported column can bill different amounts: a tenant-wide 20% rule and a
project-P 50% rule turn two $100 events into $120 and $150. An auditor summing
the export sees $200 incurred against a $270 invoice and has no column that
explains the split.

``project_id`` is the SOLE markup match-tuple dimension that exists on a
``CostEvent`` row (there is no ``byo`` / ``owner_provided`` on the model), which
is what makes its absence reconciliation-breaking rather than cosmetic.

Real DB (per-test schema), real CSV parsing on the streamed response body.
"""

from __future__ import annotations

import csv
import io
import uuid
from datetime import UTC, datetime, timedelta
from decimal import Decimal

import pytest
from sqlalchemy.ext.asyncio import AsyncSession

from models.db import CostEvent
from routers.export import _BILLING_HEADER
from tests.conftest import T

# One fixed instant for both events, so the ONLY difference the DB rows carry
# beyond ``project_id`` is the surrogate ``id`` — which is what lets the
# "distinguishable" assertion below mean what it says.
_TS = datetime.now(UTC) - timedelta(days=1)


def _cost(*, tenant: str, project_id: uuid.UUID | None) -> CostEvent:
    """A cost event whose every exported column is fixed except ``project_id``."""
    return CostEvent(
        tenant_id=tenant,
        tenant_name=tenant,
        license_id=None,
        subject_id=None,
        app_id=None,
        project_id=project_id,
        service_key="test-svc",  # pragma: allowlist secret
        provider="testprov",
        model="testmodel",
        served_model="testmodel",
        input_tokens=100,
        output_tokens=50,
        cache_creation_input_tokens=0,
        cache_read_input_tokens=0,
        cost_usd=Decimal("100.000000"),
        cost_usd_hint=None,
        pricing_id=None,
        provider_request_id=None,
        usage_extra=None,
        latency_ms=200,
        extra_metadata=None,
        correlation_id=None,
        created_at=_TS,
    )


def _parse_csv(body: str) -> list[dict[str, str]]:
    return list(csv.DictReader(io.StringIO(body)))


@pytest.mark.asyncio
async def test_billing_export_header_carries_project_id() -> None:
    """The column exists at all — the cheapest possible failure to read."""
    assert "project_id" in _BILLING_HEADER


@pytest.mark.asyncio
async def test_billing_export_distinguishes_two_projects(
    app_client, db_session: AsyncSession
) -> None:
    """Two events differing ONLY by project must be distinguishable in the CSV.

    This is the reconciliation case verbatim: identical spend, different markup
    rule, and the export is the auditor's only evidence of why one $100 event
    became $120 and the other $150.
    """
    tenant = T("proj-export-t")
    project_a = uuid.uuid4()
    project_b = uuid.uuid4()
    db_session.add_all(
        [
            _cost(tenant=tenant, project_id=project_a),
            _cost(tenant=tenant, project_id=project_b),
        ]
    )
    await db_session.commit()

    r = app_client.get(
        f"/api/export/billing.csv?tenant_id={tenant}",
        headers={"Authorization": "Bearer test-admin-jwt"},
    )
    assert r.status_code == 200
    rows = _parse_csv(r.text)
    assert len(rows) == 2

    # The column is emitted and carries BOTH projects — not one, not a blank.
    assert {row["project_id"] for row in rows} == {str(project_a), str(project_b)}

    # ...and it is the ONLY thing that tells the two apart. Every other exported
    # column except the surrogate ``id`` is equal across the pair, so an export
    # without ``project_id`` leaves an auditor two indistinguishable $100 rows
    # to reconcile against a $270 invoice.
    differing = {col for col in _BILLING_HEADER if rows[0][col] != rows[1][col] and col != "id"}
    assert differing == {"project_id"}, (
        f"expected project_id to be the sole distinguishing column, got {sorted(differing)}"
    )


@pytest.mark.asyncio
async def test_billing_export_null_project_renders_like_other_nullable_uuids(
    app_client, db_session: AsyncSession
) -> None:
    """A NULL project renders exactly as the other nullable uuid columns do: ``""``.

    ``CostEvent.project_id`` is nullable and never defaulted — the event either
    carried a project or genuinely had none. ⛔ No sentinel, and never the
    string ``"unknown"``: that would assert an attribution no producer made.
    ``license_id`` / ``subject_id`` are the reference rendering, and both are
    NULL on this row, so the assertion is a direct comparison rather than a
    restatement of the expected literal.
    """
    tenant = T("proj-export-null")
    db_session.add(_cost(tenant=tenant, project_id=None))
    await db_session.commit()

    r = app_client.get(
        f"/api/export/billing.csv?tenant_id={tenant}",
        headers={"Authorization": "Bearer test-admin-jwt"},
    )
    assert r.status_code == 200
    rows = _parse_csv(r.text)
    assert len(rows) == 1
    row = rows[0]

    assert row["project_id"] == ""
    assert row["project_id"] == row["license_id"] == row["subject_id"]
    # Guard the specific wrong answers, so a future placeholder cannot slip in.
    assert row["project_id"] not in {"unknown", "None", "null", "-"}


@pytest.mark.asyncio
async def test_billing_export_project_column_position_is_appended() -> None:
    """Column ORDER is a published contract for a CSV that leaves the estate.

    ``project_id`` is APPENDED so that all 20 pre-existing columns keep their
    index for any consumer that reads positionally. If a future change wants it
    grouped with the identity uuids, that is a breaking change for those
    consumers and this assertion is the place it must be argued.
    """
    assert _BILLING_HEADER[-1] == "project_id"
    assert _BILLING_HEADER[:20] == [
        "id",
        "created_at",
        "tenant_id",
        "tenant_name",
        "license_id",
        "subject_id",
        "service_key",
        "cost_source",
        "provider",
        "model",
        "served_model",
        "input_tokens",
        "output_tokens",
        "cache_creation_input_tokens",
        "cache_read_input_tokens",
        "cost_usd",
        "pricing_id",
        "provider_request_id",
        "correlation_id",
        "latency_ms",
    ]
