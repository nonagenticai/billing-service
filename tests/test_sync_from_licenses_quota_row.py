"""``POST /api/budget/sync-from-licenses`` writes a ``request_quota_config`` row
for a tenant-bearing licence and skips a tenant-less one without crashing.

Runs against real Postgres (repo standard).
"""

from __future__ import annotations

import uuid
from unittest.mock import patch

import pytest

from tests.conftest import T


def _mock_licenses_client(payload):
    from unittest.mock import AsyncMock, MagicMock

    mock_resp = MagicMock()
    mock_resp.status_code = 200
    mock_resp.json.return_value = payload
    mock_client = AsyncMock()
    mock_client.get.return_value = mock_resp
    mock_client.__aenter__ = AsyncMock(return_value=mock_client)
    mock_client.__aexit__ = AsyncMock(return_value=False)
    return mock_client


class TestSyncFromLicensesWritesConfigRow:
    """``sync-from-licenses`` upserts a config row for a tenant-bearing license and
    SKIPS a tenant-less license without crashing."""

    @pytest.mark.unit
    def test_sync_upserts_config_row_for_tenant_license_and_skips_tenantless(
        self, db_engine, app_client
    ):
        import os

        import psycopg

        good_lic = uuid.uuid4()
        bad_lic = uuid.uuid4()
        payload = [
            {
                "id": str(good_lic),
                "tenant_id": T("tenant-good"),
                "plan": {"id": str(uuid.uuid4()), "quota_monthly": 750},
            },
            {
                # No tenant_id → must be skipped with a warning, no row, no crash.
                "id": str(bad_lic),
                "tenant_id": None,
                "plan": {"id": str(uuid.uuid4()), "quota_monthly": 500},
            },
        ]

        with patch("routers.budget.httpx.AsyncClient", return_value=_mock_licenses_client(payload)):
            r = app_client.post("/api/budget/sync-from-licenses")
        assert r.status_code == 200, r.text
        assert r.json()["quotas_synced"] == 1  # only the tenant-bearing license

        # tenant-bearing license → a durable config row exists with its tenant.
        # tenant-less license → skipped, NO row (would have violated NOT NULL).
        sync_url = os.environ["DATABASE_URL"].replace("+psycopg", "")
        with psycopg.connect(sync_url) as conn, conn.cursor() as cur:
            cur.execute(
                "SELECT tenant_id, quota_monthly FROM request_quota_config WHERE license_id = %s",
                (str(good_lic),),
            )
            good = cur.fetchone()
            cur.execute(
                "SELECT 1 FROM request_quota_config WHERE license_id = %s",
                (str(bad_lic),),
            )
            bad = cur.fetchone()
        assert good is not None
        assert str(good[0]) == T("tenant-good")
        assert good[1] == 750
        assert bad is None
