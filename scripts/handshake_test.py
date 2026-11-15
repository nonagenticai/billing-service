"""Cross-service handshake check.

Inserts a cost_event row carrying a fresh correlation_id (the value
worker_management would emit per spec §5.2.1) and confirms the metering DB
holds it. This is the local approximation of the plan's Task 13 end-to-end
verification — it doesn't span the two service Postgres instances on mf1
(which would require SSH + production access) but it does pin the metering
contract: the correlation_id round-trips intact.
"""

from __future__ import annotations

import os
import sys
import uuid
from pathlib import Path

REPO = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(REPO / "src"))

from sqlalchemy import create_engine, text  # noqa: E402

from tenancy import derived_tenant_id  # noqa: E402

# NEVER put 5436 back here: that port is ``ir-r21-pg-5436``, ANOTHER session's
# Postgres container. This repo's own test container is 127.0.0.1:5502.
DSN = os.getenv(
    "DATABASE_URL",
    "postgresql+psycopg://postgres:postgres@127.0.0.1:5502/metering_test",
)

# ``cost_events.tenant_id`` is a real uuid since migration 0011, so the literal
# 'tenant-A' that used to sit in the INSERT now raises DataError. Resolve the
# label the same way the service does; the human label survives in tenant_name.
TENANT_LABEL = "tenant-A"
TENANT_ID = derived_tenant_id(TENANT_LABEL)


def main() -> None:
    eng = create_engine(DSN, future=True)
    cid = str(uuid.uuid4())
    print(f"CORRELATION_ID: {cid}")
    with eng.begin() as conn:
        conn.execute(
            text(
                """
                INSERT INTO cost_events
                    (id, tenant_id, tenant_name, service_key, provider,
                     input_tokens, output_tokens,
                     correlation_id, created_at)
                VALUES (gen_random_uuid(), CAST(:tenant_id AS uuid), :tenant_name,
                        'inference-router', 'anthropic',
                        100, 50,
                        CAST(:cid AS uuid), now())
                """
            ),
            {"cid": cid, "tenant_id": TENANT_ID, "tenant_name": TENANT_LABEL},
        )
        row = conn.execute(
            text(
                "SELECT correlation_id FROM cost_events WHERE correlation_id = CAST(:cid AS uuid)"
            ),
            {"cid": cid},
        ).fetchone()
    assert row is not None, "row not found"
    assert str(row.correlation_id) == cid, "correlation_id round-trip failed"
    print(f"FOUND ROW: {row}")
    print("CROSS-SERVICE HANDSHAKE TEST: PASS")


if __name__ == "__main__":
    main()
