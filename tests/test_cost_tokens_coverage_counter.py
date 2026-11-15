"""Tests for `billing_cost_tokens_by_service_total` (observability.metrics.

cost_tokens_by_service_total), the token-VOLUME counter incremented in
services/cost_flusher.py immediately after `provider = c.get("provider",
"unknown")`, off the same `service_key_label` expression that
`cost_events_by_service_total` already increments earlier in the same loop
iteration. It sums input_tokens + output_tokens + cache_creation_input_tokens
+ cache_read_input_tokens for every schema-valid event taken off the buffer.

The default prometheus registry is session-global and other test modules
increment these series, so every test here reads by DELTA (before/after),
matching the idiom `tests/test_cost_flusher.py` already uses for
`cost_dead_letter_total` / `license_unattributed_total` / etc.
(`<counter>.labels(...)._value.get()`).
"""

import pytest
from sqlalchemy import func, select

from models.db import CostEvent
from services.aggregator import flush_costs
from tenancy import NIL_TENANT_ID
from tests._aggregator_helpers import _make_cost
from tests.conftest import T

pytestmark = pytest.mark.unit


def _tokens(service_key: str, provider: str) -> float:
    import observability.metrics as om

    return om.cost_tokens_by_service_total.labels(
        service_key=service_key, provider=provider
    )._value.get()


def _events(service_key: str) -> float:
    import observability.metrics as om

    return om.cost_events_by_service_total.labels(service_key=service_key)._value.get()


def _dead_letters(reason: str) -> float:
    import observability.metrics as om

    return om.cost_dead_letter_total.labels(reason=reason)._value.get()


async def test_token_counter_sums_all_four_token_fields(db_session_factory):
    """One event, four DISTINCT non-zero prime values.

    Deliberately NOT 1/1/1/1: with all-equal inputs a bug that drops one field
    (summing only 3 of 4) or double-counts another (e.g. input twice instead of
    input+output) still lands on a plausible total and the test would pass on
    the wrong arithmetic. 7/11/13/17 are pairwise distinct, so the counter can
    only read 48 if all four fields were read exactly once each.
    """
    before = _tokens("inference-router", "claude")
    count = await flush_costs(
        [
            _make_cost(
                tenant_id=T("token-sum-tenant"),
                service_key="inference-router",  # pragma: allowlist secret
                provider="claude",
                input_tokens="7",
                output_tokens="11",
                cache_creation_input_tokens="13",
                cache_read_input_tokens="17",
            )
        ],
        db_session_factory,
        None,
    )
    assert count == 1
    assert _tokens("inference-router", "claude") - before == 48


async def test_token_counter_is_labelled_by_service_key_and_provider(db_session_factory):
    """Two events with different (service_key, provider) pairs land on separate
    child series with the right values — not pooled onto one series."""
    alpha_before = _tokens("svc-alpha", "anthropic")
    beta_before = _tokens("svc-beta", "openai")

    count = await flush_costs(
        [
            _make_cost(
                tenant_id=T("labelled-tenant-1"),
                service_key="svc-alpha",  # pragma: allowlist secret
                provider="anthropic",
                input_tokens="3",
                output_tokens="5",
                cache_creation_input_tokens="0",
                cache_read_input_tokens="0",
            ),
            _make_cost(
                tenant_id=T("labelled-tenant-2"),
                service_key="svc-beta",  # pragma: allowlist secret
                provider="openai",
                input_tokens="2",
                output_tokens="4",
                cache_creation_input_tokens="0",
                cache_read_input_tokens="0",
            ),
        ],
        db_session_factory,
        None,
    )

    assert count == 2
    assert _tokens("svc-alpha", "anthropic") - alpha_before == 8
    assert _tokens("svc-beta", "openai") - beta_before == 6
    # Neither event's tokens leaked onto the cross pair this test never touched.
    assert _tokens("svc-alpha", "openai") == 0
    assert _tokens("svc-beta", "anthropic") == 0


async def test_token_counter_and_event_counter_see_the_same_population(db_session_factory):
    """The pairing guarantee: a coverage ratio over the two counters is only
    honest if both see the same rows. Flush a small batch and assert the EVENT
    counter rose by the number of events AND the TOKEN counter rose by the sum
    of their tokens."""
    service_key = "pairing-check-service"  # pragma: allowlist secret
    provider = "claude"
    events_before = _events(service_key)
    tokens_before = _tokens(service_key, provider)

    batch = [
        _make_cost(
            tenant_id=T("pairing-tenant-1"),
            service_key=service_key,
            provider=provider,
            input_tokens="10",
            output_tokens="20",
            cache_creation_input_tokens="0",
            cache_read_input_tokens="0",
        ),
        _make_cost(
            tenant_id=T("pairing-tenant-2"),
            service_key=service_key,
            provider=provider,
            input_tokens="1",
            output_tokens="2",
            cache_creation_input_tokens="3",
            cache_read_input_tokens="4",
        ),
        _make_cost(
            tenant_id=T("pairing-tenant-3"),
            service_key=service_key,
            provider=provider,
            input_tokens="100",
            output_tokens="0",
            cache_creation_input_tokens="0",
            cache_read_input_tokens="0",
        ),
    ]
    count = await flush_costs(batch, db_session_factory, None)

    assert count == 3
    assert _events(service_key) - events_before == 3
    # (10+20) + (1+2+3+4) + (100+0) == 140
    assert _tokens(service_key, provider) - tokens_before == 140


async def test_a_dead_lettered_event_increments_neither_counter(db_session_factory, fake_redis):
    """An event that fails `validate_cost_event` `continue`s before both
    increments (cost_flusher.py: the dead-letter branch is hit, and returns,
    before `service_key_label` is even read) and must move NEITHER counter.

    Uses a nil-uuid `tenant_id` (`NIL_TENANT_ID`), which `_cost_schema.
    validate_cost_event` dead-letters with reason `nil_tenant_id` — verified
    below via the dead-letter counter itself, rather than assumed, so this test
    cannot pass because the "invalid" event was actually accepted.
    """
    service_key = "dead-letter-neither-service"  # pragma: allowlist secret
    provider = "claude"
    dlq = "billing:costs:dead"

    events_before = _events(service_key)
    tokens_before = _tokens(service_key, provider)
    dlq_before = _dead_letters("nil_tenant_id")

    count = await flush_costs(
        [
            _make_cost(
                tenant_id=NIL_TENANT_ID,
                service_key=service_key,
                provider=provider,
                input_tokens="9",
                output_tokens="9",
                cache_creation_input_tokens="9",
                cache_read_input_tokens="9",
                _msg_id="coverage-dlq-1",
            )
        ],
        db_session_factory,
        fake_redis,
        dead_letter_stream=dlq,
    )

    assert count == 0

    # Confirm the event was actually rejected, and rejected for the expected
    # reason — not accepted and silently billed as if it never happened.
    assert _dead_letters("nil_tenant_id") - dlq_before == 1
    entries = await fake_redis.xrange(dlq)
    assert len(entries) == 1
    assert entries[0][1]["_dlq_reason"] == "nil_tenant_id"

    async with db_session_factory() as session:
        n = (await session.execute(select(func.count()).select_from(CostEvent))).scalar()
        assert n == 0

    assert _events(service_key) - events_before == 0
    assert _tokens(service_key, provider) - tokens_before == 0


def test_unknown_service_and_provider_pair_is_born_at_zero():
    """metrics.py births the (unknown service_key, unknown provider) child
    at import time. Must read 0.0 on a FRESH interpreter.

    The default prometheus registry is session-global and other test modules
    (e.g. flush_costs calls elsewhere in the suite that default service_key /
    provider to "unknown") increment this exact child, so a born-at-zero
    assertion cannot be made in-process without becoming order-dependent. Same
    subprocess idiom as tests/test_metrics_endpoint.py's
    `_COLLECT_NEW_SERIES_AT_BOOT`.
    """
    import json
    import os
    import subprocess
    import sys
    from pathlib import Path

    collect_at_boot = """
import json
import observability.metrics as m

print(json.dumps([
    [s.labels["service_key"], s.labels["provider"], s.value]
    for metric in m.cost_tokens_by_service_total.collect()
    for s in metric.samples
    if s.name == "billing_cost_tokens_by_service_total"
]))
"""

    proc = subprocess.run(  # noqa: S603
        [sys.executable, "-c", collect_at_boot],
        capture_output=True,
        text=True,
        check=True,
        cwd=str(Path(__file__).parent.parent),
        timeout=30,
        env={**os.environ, "PYTHONPATH": "src"},
    )
    born = json.loads(proc.stdout.strip().splitlines()[-1])
    assert born == [["unknown", "unknown", 0.0]], (
        f"cost_tokens_by_service_total boot series is not exactly the catch-all at zero: {born}"
    )
