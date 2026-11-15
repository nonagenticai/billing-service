"""Shared row-builder helpers for the aggregator-split test files.

Imported by:
- test_event_flusher.py
- test_cost_flusher.py
- test_rollup_engine.py
- test_aggregator.py (orchestrator loops + cross-cutting tests)

Leading underscore = internal to tests/, not a public test module.

``tenant_id`` is a REQUIRED keyword on both builders. It used to default to the
readable literal ``"test-tenant"``, which since ``0011_tenant_id_is_a_uuid`` is
not a uuid: ``_normalize_tenant_id`` sinks it to ``NIL_TENANT_ID`` and the row
lands on the nil tenant while every assertion still passes. Making it required
forces each caller to name its tenant explicitly — use ``T("label")`` from
``tests/conftest.py``. Paired with the ``_nil_tenant_tripwire`` fixture there.
"""

import uuid


def _make_event(
    *,
    tenant_id: str,
    license_id: str | None = None,
    app_id: str | None = None,
    plan_id: str | None = None,
    method: str = "GET",
    path: str = "/workers/api/tasks",
    status: str = "allowed",
    source_ip: str = "10.0.0.1",
    service_key: str = "workers",
) -> dict:
    return {
        "tenant_id": tenant_id,
        "license_id": license_id if license_id is not None else str(uuid.uuid4()),
        "app_id": app_id or str(uuid.uuid4()),
        "plan_id": plan_id or str(uuid.uuid4()),
        "method": method,
        "path": path,
        "status": status,
        "source_ip": source_ip,
        "_service_key": service_key,
    }


def _make_cost(
    *,
    tenant_id: str,
    license_id: str | None = None,
    app_id: str = "",
    service_key: str = "inference-router",
    provider: str = "claude",
    model: str = "claude-sonnet-4-20250514",
    input_tokens: str = "1000",
    output_tokens: str = "200",
    cost_usd: str = "0.0165",
    latency_ms: str = "350",
    **extra: str,
) -> dict:
    d: dict = {
        "tenant_id": tenant_id,
        "license_id": license_id if license_id is not None else str(uuid.uuid4()),
        "app_id": app_id,
        "service_key": service_key,
        "provider": provider,
        "model": model,
        "input_tokens": input_tokens,
        "output_tokens": output_tokens,
        "cost_usd": cost_usd,
        "latency_ms": latency_ms,
    }
    d.update(extra)
    return d
