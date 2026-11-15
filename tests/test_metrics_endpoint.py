"""``/metrics`` endpoint exposes both HTTP-level + custom billing counters.

Forces the Prometheus surface to stay stable so the ServiceMonitor + Grafana
dashboard + PrometheusRule alerts in gitops-production can rely on these
specific metric names.
"""

from __future__ import annotations

import json
import os
import subprocess
import sys
from pathlib import Path

from fastapi.testclient import TestClient

import observability.metrics as om
from services import markup
from services._cost_schema import COST_DEAD_LETTER_REASONS

#: Read back from the pre-seed loop in observability.metrics, never restated —
#: a reason added there is covered here without an edit.
_CLOSE_FAILURE_REASONS = (
    "infra_error",
    "unexpected_error",
    "tenant_refused",
    "tenant_error",
)

# Read the reason series out of a FRESHLY imported observability.metrics, in a
# clean interpreter that has incremented nothing. The default prometheus
# registry is module-level state shared by the whole pytest session, and
# tests/test_cost_flusher.py (collected first, alphabetically) really does
# increment three of these reasons — so "the value at birth" cannot be measured
# in-process without becoming an order-dependent assertion.
_COLLECT_AT_BOOT = """
import json
import observability.metrics as m

print(json.dumps({
    s.labels["reason"]: s.value
    for metric in m.cost_dead_letter_total.collect()
    for s in metric.samples
    if s.name == "billing_cost_dead_letter_total"
}))
"""


# Same fresh-interpreter trick for the invoice-close failure reasons. The label
# domain is read back from the pre-seed loop in observability.metrics rather than
# restated here, so a fifth reason added beside the counter is born automatically
# and this test cannot drift from it.
_COLLECT_CLOSE_FAILURES_AT_BOOT = """
import json
import observability.metrics as m

print(json.dumps({
    s.labels["reason"]: s.value
    for metric in m.invoice_close_failures_total.collect()
    for s in metric.samples
    if s.name == "billing_invoice_close_failures_total"
}))
"""

# Same fresh-interpreter trick for the two counters added with the
# unregistered-tenant exclusion and the placeholder census. Both are labelled and
# both have a BOUNDED label domain that is imported, not restated, so a new
# exclusion reason or a new metadata whitelist key is born automatically.
_COLLECT_NEW_SERIES_AT_BOOT = """
import json
import observability.metrics as m

print(json.dumps({
    "unregistered": sorted(
        (s.labels["action"], s.labels["service_key"], s.value)
        for metric in m.cost_events_unregistered_tenant_total.collect()
        for s in metric.samples
        if s.name == "billing_cost_events_unregistered_tenant_total"
    ),
    "placeholder": sorted(
        (s.labels["field"], s.labels["service_key"], s.value)
        for metric in m.placeholder_value_total.collect()
        for s in metric.samples
        if s.name == "billing_placeholder_value_total"
    ),
}))
"""


def test_metrics_endpoint_exposes_custom_billing_counters(app_client: TestClient) -> None:
    body = app_client.get("/metrics").text
    # All custom collectors from observability.metrics must appear in the
    # default registry (even with zero observations they emit the HELP/TYPE
    # lines so dashboards survive a fresh pod's first scrape).
    expected = [
        "billing_events_in_total",
        "billing_events_flushed_total",
        "billing_cost_events_in_total",
        "billing_cost_events_flushed_total",
        "billing_cost_events_priced_total",
        "billing_pricing_miss_total",
        "billing_pricing_row_malformed_total",
        "billing_budget_debits_total",
        # Label-less, so it must be born at zero on import: the benign
        # release-without-reserve split is only readable as a rate if a pod
        # that has not yet settled anything still exports the series.
        "billing_budget_release_without_reserve_total",
        "billing_flush_lag_seconds",
        "billing_cost_events_unregistered_tenant_total",
        # Token VOLUME, the missing half of the coverage ratio: event counts
        # cannot answer "is all LLM usage measured?" because a 300-token call and
        # a 90,000-token call are one increment each.
        "billing_cost_tokens_by_service_total",
        # The WRITE-side sibling of the line above, and a different population:
        # that one counts a money record arriving for an unknown tenant (billed
        # anyway), this one counts a payer row being refused. Listed here so its
        # EXPOSURE is asserted -- a counter absent from this list is exported on
        # nobody's authority.
        "billing_budget_writes_refused_unregistered_tenant_total",
        "billing_placeholder_value_total",
        # The four silent metering failures made countable. Each covers a site
        # whose only previous trace was a log line or nothing at all, so their
        # EXPOSURE is the thing to assert: a counter that never reaches /metrics
        # is exported on nobody's authority.
        #
        # The OUTER guard in event_flusher (`if license_id and plan_id:`) — the
        # inner unknown-status branch was already counted, this one was not, and
        # it is the normal population today (159,085 rows/30d).
        "billing_metering_usage_skipped_total",
        # Confirmed PEL data loss + its denominator. The code comment at the
        # eviction site says it "must be loud"; a structlog warning is not.
        "billing_pel_entries_evicted_total",
        "billing_pel_entries_reclaimed_total",
        # An event_ts accepted up to 90 days back, landing in a day the ~3-day
        # reconcile window no longer revisits.
        "billing_cost_event_ts_backdated_past_rollup_total",
        # Ledger DEPTH, not flow. billing_cost_events was TRUNCATEd on
        # 2026-09-05 and nothing noticed for six days, because every signal this
        # service had measured flow and flow was healthy within the minute.
        "billing_cost_events_oldest_age_seconds",
    ]
    missing = [m for m in expected if m not in body]
    assert not missing, f"missing metrics: {missing}"


def test_cost_dead_letter_reason_series_are_born_at_zero(app_client: TestClient) -> None:
    """THE DEFECT, measured on prod 2026-08-28.

    A labelled prometheus_client Counter exports NO series until its first
    ``.labels(...).inc()``. ``billing_cost_dead_letter_total`` has exactly one
    increment site (``cost_flusher.py:708``) and had no zero-init, so a pod that
    had simply not dead-lettered anything since boot exported nothing at all. An
    operator ran ``sum(increase(billing_cost_dead_letter_total
    {reason="nil_tenant_id"}[6h]))``, got an EMPTY VECTOR, and read it as a
    measured zero — "no dead letters" and "the instrument is gone" were the same
    answer.

    Asserted on the VALUE and on the presence of each labelled sample on
    purpose: a test that only checks the Counter OBJECT exists passes today and
    would have passed that morning too.
    """
    # 1. Live scrape surface: every reason must be PRESENT. Order-independent —
    #    an already-incremented reason still satisfies presence, an unborn one
    #    cannot.
    body = app_client.get("/metrics").text
    unborn = [
        reason
        for reason in COST_DEAD_LETTER_REASONS
        if f'billing_cost_dead_letter_total{{reason="{reason}"}}' not in body
    ]
    assert not unborn, f"dead-letter reason series never born: {unborn}"

    # 2. The value at boot, from a clean interpreter (see _COLLECT_AT_BOOT).
    src_root = Path(om.__file__).resolve().parents[1]
    proc = subprocess.run(
        [sys.executable, "-c", _COLLECT_AT_BOOT],
        capture_output=True,
        text=True,
        check=True,
        # Under the 60s per-test pytest-timeout, so this fails as an assertion
        # rather than as a killed test.
        timeout=30,
        env={**os.environ, "PYTHONPATH": str(src_root)},
    )
    born: dict[str, float] = json.loads(proc.stdout.strip().splitlines()[-1])
    assert set(born) == set(COST_DEAD_LETTER_REASONS), (
        f"boot series {sorted(born)} != reasons {sorted(COST_DEAD_LETTER_REASONS)}"
    )
    assert set(born.values()) == {0.0}, f"a reason series was not born at 0: {born}"


#: The three streams services.rollup_engine.trim_stream is ever called on
#: (services.consumer's DLQ trim block + services.aggregator's leader-gated
#: ingest-stream trim), matching their Settings field defaults
#: (src/settings.py). Hand-copied rather than read from the pre-seed loop, same
#: as _CLOSE_REFUSAL_REASONS below -- see that constant's own comment.
_TRIMMED_STREAM_NAMES = ("billing:costs", "billing:events", "billing:costs:dead")

_COLLECT_DEAD_LETTER_EXPIRED_AT_BOOT = """
import json
import observability.metrics as m

print(json.dumps({
    s.labels["stream"]: s.value
    for metric in m.cost_dead_letter_expired_total.collect()
    for s in metric.samples
    if s.name == "billing_cost_dead_letter_expired_total"
}))
"""


def test_cost_dead_letter_expired_stream_series_are_born_at_zero(app_client: TestClient) -> None:
    """billing_cost_dead_letter_expired_total is the counter that closes the
    gap billing_cost_dead_letter_depth cannot see (see that gauge's own
    module-level comment): the alert BillingCostDeadLetterBacklog watches
    DEPTH while the loss mechanism on billing:costs:dead is AGE
    (dead_letter_stream_max_age_hours, 720h/30d default), so a sub-500
    backlog can expire one entry at a time forever without ever tripping
    that alert.

    Same birth hazard as its DLQ siblings above: a labelled
    prometheus_client Counter exports no series until its first
    ``.labels(...)``. Written on the SAME defect class this branch's
    audit already found twice tonight (invoice_close_refused_total,
    inference-router's identity pre-seed) -- a pre-seed with no test that
    can fail. Mutation: emptying the pre-seed loop in metrics.py must red
    this test.
    """
    # Matched at LINE START, not by a bare substring of the whole body: a
    # HELP-text comment mentioning this metric name as prose (the exact
    # trap test_invoices_closed_status_series_are_born_at_zero documents
    # below) would otherwise be able to fool this check the same way.
    body = app_client.get("/metrics").text
    sample_lines = body.splitlines()
    unborn = [
        stream
        for stream in _TRIMMED_STREAM_NAMES
        if not any(
            line.startswith(f'billing_cost_dead_letter_expired_total{{stream="{stream}"}}')
            for line in sample_lines
        )
    ]
    assert not unborn, f"trim-stream expired series never born: {unborn}"

    src_root = Path(om.__file__).resolve().parents[1]
    proc = subprocess.run(
        [sys.executable, "-c", _COLLECT_DEAD_LETTER_EXPIRED_AT_BOOT],
        capture_output=True,
        text=True,
        check=True,
        timeout=30,
        env={**os.environ, "PYTHONPATH": str(src_root)},
    )
    born: dict[str, float] = json.loads(proc.stdout.strip().splitlines()[-1])
    assert set(born) == set(_TRIMMED_STREAM_NAMES), (
        f"boot series {sorted(born)} != streams {sorted(_TRIMMED_STREAM_NAMES)}"
    )
    assert set(born.values()) == {0.0}, f"a stream series was not born at 0: {born}"


_COLLECT_PRICING_MISS_AT_BOOT = """
import json
import observability.metrics as m

print(json.dumps([
    [s.labels["provider"], s.labels["model"], s.value]
    for metric in m.pricing_miss_total.collect()
    for s in metric.samples
    if s.name == "billing_pricing_miss_total"
]))
"""


def test_pricing_miss_series_is_born_at_zero(app_client: TestClient) -> None:
    """THE DEFECT, measured on prod 2026-09-03.

    ``billing_pricing_miss_total`` is defined (metrics.py) and incremented
    (services/pricing.py, the ``row is None`` branch of ``compute_cost_usd``),
    but a LABELLED prometheus_client Counter exports the HELP/TYPE header and NO
    SAMPLES until its first ``.labels(...)``. Prometheus ingests samples, not
    headers, so a fleet that had not missed a price since boot created no series
    at all::

        /api/v1/series?match[]=billing_pricing_miss_total  ->  []

    That makes the ``MeteringPricingMiss`` alert
    (``sum by (provider, model) (increase(billing_pricing_miss_total[1h])) > 0``)
    structurally unfireable: "no pricing misses" and "the alert is dead" are the
    same empty vector.

    Asserted on a labelled SAMPLE and on its value, not on the Counter object:
    a test that only checks ``hasattr(om, "pricing_miss_total")`` passed on the
    morning of the measurement and would keep passing through a regression.
    """
    # 1. Live scrape surface: at least one labelled sample, so a scrape of a pod
    #    that has never missed a price still creates the series. Order-independent
    #    — a peer test that already incremented a different (provider, model)
    #    cannot satisfy this, only the birthed catch-all can.
    body = app_client.get("/metrics").text
    assert 'billing_pricing_miss_total{model="unknown",provider="unknown"}' in body, (
        "pricing-miss series never born: /metrics carries the HELP/TYPE header "
        "but no sample, so Prometheus records no series and MeteringPricingMiss "
        "can never fire"
    )

    # 2. The value at boot, from a clean interpreter that has priced nothing
    #    (the default registry is session-shared module state, so "at birth"
    #    cannot be measured in-process without an order-dependent assertion).
    src_root = Path(om.__file__).resolve().parents[1]
    proc = subprocess.run(
        [sys.executable, "-c", _COLLECT_PRICING_MISS_AT_BOOT],
        capture_output=True,
        text=True,
        check=True,
        timeout=30,
        env={**os.environ, "PYTHONPATH": str(src_root)},
    )
    born = json.loads(proc.stdout.strip().splitlines()[-1])
    assert born == [["unknown", "unknown", 0.0]], (
        f"pricing-miss boot series is not exactly the catch-all at zero: {born}"
    )


def test_new_counter_series_are_born_at_zero() -> None:
    """An unfired series must read 0, not be absent.

    Absence and zero are the same observation to a dashboard, so a counter that
    has never fired is indistinguishable from one that was never wired up. Both
    of these instruments exist to make a gap VISIBLE — a placeholder census that
    silently exports nothing would be the exact defect it was written to close.

    Run in a fresh interpreter for the reason the dead-letter test above gives:
    the default prometheus registry is session-global and other test modules
    increment these.
    """
    import json
    import subprocess
    import sys

    out = subprocess.run(  # noqa: S603
        [sys.executable, "-c", _COLLECT_NEW_SERIES_AT_BOOT],
        capture_output=True,
        text=True,
        check=True,
        cwd=str(Path(__file__).parent.parent),
        env={**os.environ, "PYTHONPATH": "src"},
    )
    born = json.loads(out.stdout)

    from observability.metrics import UNREGISTERED_TENANT_ACTIONS
    from services._parsers import PLACEHOLDER_COUNTED_FIELDS

    unregistered = {(action, sk): v for action, sk, v in born["unregistered"]}
    for action in UNREGISTERED_TENANT_ACTIONS:
        assert unregistered.get((action, "unknown")) == 0.0, (
            "billing_cost_events_unregistered_tenant_total"
            f"{{action={action!r}}} was not born at zero: {born['unregistered']}"
        )

    placeholder = {(field, sk): v for field, sk, v in born["placeholder"]}
    for field in PLACEHOLDER_COUNTED_FIELDS:
        assert placeholder.get((field, "unknown")) == 0.0, (
            f"billing_placeholder_value_total{{field={field!r}}} was not born at zero: "
            f"{born['placeholder']}"
        )


def test_invoice_close_failure_reason_series_are_born_at_zero(
    app_client: TestClient,
) -> None:
    """THE DEFECT, measured on prod 2026-09-11: this metric had NEVER appeared.

    ``billing_invoice_close_failures_total`` had no zero-init, so a fleet that had
    not yet failed a close exported no series at all — while its siblings
    ``_enabled``, ``_is_leader`` and ``_lag_days`` each carried 2 series from the
    SAME process, which is what proves the absence was birth and not scrape.

    ⛔ WHY IT MATTERS MORE HERE THAN FOR MOST COUNTERS. ``BillingInvoiceCloseErrored``
    (gitops #3806) pages on
    ``increase(billing_invoice_close_failures_total{reason="tenant_error"}[1h]) > 0``.
    On an unborn series that expression has nothing to evaluate, so the FIRST
    close failure — the one that matters — is invisible, and the alert reads green
    for a reason indistinguishable from health. Its sibling
    ``InferenceRouterCostEventsMalformedTenant`` carries an ``unless … offset 1h``
    birth clause; this alert does not, so the fix belongs on this side.

    All four reasons are asserted, not just the paged one: a ``tenant_refused``
    series that appears only on the first refusal makes "no refusals" and "never
    asked" the same answer, and this estate has repeatedly read the second as the
    first.
    """
    # 1. Live scrape surface: every reason PRESENT. Order-independent, so an
    #    already-incremented reason still satisfies presence while an unborn one
    #    cannot.
    body = app_client.get("/metrics").text
    unborn = [
        reason
        for reason in _CLOSE_FAILURE_REASONS
        if f'billing_invoice_close_failures_total{{reason="{reason}"}}' not in body
    ]
    assert not unborn, f"invoice-close failure reason series never born: {unborn}"

    # 2. The value at boot, from a clean interpreter that has incremented nothing.
    src_root = Path(om.__file__).resolve().parents[1]
    proc = subprocess.run(
        [sys.executable, "-c", _COLLECT_CLOSE_FAILURES_AT_BOOT],
        capture_output=True,
        text=True,
        check=True,
        timeout=30,
        env={**os.environ, "PYTHONPATH": str(src_root)},
    )
    born: dict[str, float] = json.loads(proc.stdout.strip().splitlines()[-1])
    assert set(born) == set(_CLOSE_FAILURE_REASONS), (
        f"boot series {sorted(born)} != reasons {sorted(_CLOSE_FAILURE_REASONS)}"
    )
    assert all(v == 0.0 for v in born.values()), f"not born at zero: {born}"


_CLOSE_REFUSAL_REASONS = (
    "tenant_unregistered",
    "tenant_archived",
    "bucket_gap",
    "seat_count_unavailable",
    "byo_mode_unavailable",
    "licenses_unavailable",
)

_COLLECT_CLOSE_REFUSALS_AT_BOOT = """
import json
import observability.metrics as m
from prometheus_client import REGISTRY

out = {}
for mf in REGISTRY.collect():
    if mf.name == "billing_invoice_close_refused":
        for s in mf.samples:
            if s.name.endswith("_total"):
                out[s.labels["reason"]] = s.value
print(json.dumps(out))
"""


def test_invoice_close_refusal_reason_series_are_born_at_zero(
    app_client: TestClient,
) -> None:
    """THE GUARD THE SIBLING THIRTY LINES ABOVE ALREADY HAD, AND THIS ONE DID NOT.

    ⚠️ WRITTEN AFTER AN AUDIT RETURNED **NO**, AND THE NO WAS CORRECT.
    ``billing_invoice_close_failures_total`` is guarded by
    ``test_invoice_close_failure_reason_series_are_born_at_zero`` directly above.
    ``billing_invoice_close_refused_total`` — its sibling, pre-seeded the same way
    in the same module — had **no birth guard at all**. Measured by the auditor:
    deleting the four new label values left 59 tests passing, and deleting the
    ENTIRE pre-seed loop (``grep -c _close_refusal_reason`` → 0) left
    ``-k "metric or invoice or refus"`` at **356 passed**. Nothing reddened.

    ⇒ The estate's recorded shape: a defect class travels further than a defect.
    The birth defect was found and fixed on ``_failures_total`` on 2026-09-11; the
    ``_refused_total`` counter carrying the identical exposure was never swept for.

    ⛔ WHY IT MATTERS HERE SPECIFICALLY. This counter exists so that a DELIBERATE
    refusal (``bucket_gap`` — the period has no basis) is distinguishable from an
    OUTAGE (``licenses_unavailable``). Both land in the aggregate
    ``billing_invoice_close_failures_total{reason="tenant_refused"}`` and are
    indistinguishable there. If the per-reason series is unborn, the FIRST
    ``bucket_gap`` refusal of an armed close is also the first appearance of the
    series — so on the day arming matters, "correctly declined" and "instrument
    never registered" are again the same reading. That is precisely the ambiguity
    this PR exists to close, silently reintroduced by a future edit to the tuple.

    Acceptance, and the mutation that proves it: delete or empty the pre-seed loop
    at ``src/observability/metrics.py`` and THIS test goes red while the rest of
    the suite stays green.
    """
    # 1. Live scrape surface: every reason PRESENT. Order-independent, so an
    #    already-incremented reason still satisfies presence while an unborn one
    #    cannot.
    body = app_client.get("/metrics").text
    unborn = [
        reason
        for reason in _CLOSE_REFUSAL_REASONS
        if f'billing_invoice_close_refused_total{{reason="{reason}"}}' not in body
    ]
    assert not unborn, f"invoice-close refusal reason series never born: {unborn}"

    # 2. The value at boot, from a clean interpreter that has incremented nothing.
    #    ⛔ A subprocess, not an in-process read: ``Counter.labels()`` CREATES the
    #    child on touch, so an in-process helper would manufacture the presence it
    #    claims to measure, and by the time any test runs earlier tests may have
    #    driven real increments through this counter.
    src_root = Path(om.__file__).resolve().parents[1]
    proc = subprocess.run(
        [sys.executable, "-c", _COLLECT_CLOSE_REFUSALS_AT_BOOT],
        capture_output=True,
        text=True,
        check=True,
        timeout=30,
        env={**os.environ, "PYTHONPATH": str(src_root)},
    )
    born: dict[str, float] = json.loads(proc.stdout.strip().splitlines()[-1])
    assert set(born) == set(_CLOSE_REFUSAL_REASONS), (
        f"boot series {sorted(born)} != reasons {sorted(_CLOSE_REFUSAL_REASONS)}"
    )
    assert all(v == 0.0 for v in born.values()), f"not born at zero: {born}"


#: The three status values reachable through invoice_closer.py's three
#: `invoices_closed_total.labels(...)` call sites (measured 2026-09-18:
#: `git grep -n "invoices_closed_total.labels" -- src` finds exactly these
#: three). `draft` is deliberately excluded -- see metrics.py's own comment
#: on the pre-seed loop: close_due_periods never calls close_period with
#: `issue_immediately=False`, so that branch is unreached today and a
#: pre-seeded series for it could only ever read 0.
_CLOSED_STATUSES = ("issued", "unchanged", "skipped")

_COLLECT_INVOICES_CLOSED_AT_BOOT = """
import json
import observability.metrics as m

print(json.dumps({
    s.labels["status"]: s.value
    for metric in m.invoices_closed_total.collect()
    for s in metric.samples
    if s.name == "billing_invoices_closed_total"
}))
"""


def test_invoices_closed_status_series_are_born_at_zero(app_client: TestClient) -> None:
    """gitops PR #4008's BillingInvoiceIssued alert --
    ``sum(increase(billing_invoices_closed_total{status="issued"}[1h])) > 0
    and on() (max(billing_invoice_close_enabled) == 1)`` -- exists to
    announce the FIRST invoice this estate ever mints. A labelled
    prometheus_client Counter exports no series until its first
    ``.labels(...)``, and this counter had ZERO pre-seed before this
    change despite three real increment sites in services/invoice_closer.py
    (measured: ``git grep "invoices_closed_total.labels" -- src`` on
    origin/main). On an unborn series ``increase()`` has nothing to
    evaluate, so the single event the alert exists to catch -- the first
    invoice -- is exactly the event it was least able to see.

    Mutation, as with this counter's siblings above: emptying the
    pre-seed loop for ``invoices_closed_total`` in metrics.py must red
    this test while its neighbours stay green.

    ⚠️ Matched at LINE START, not by a bare substring of the whole body.
    ``invoice_close_skipped_total``'s own HELP text (a few lines below this
    counter's definition) reads "NOT THE SAME AS
    billing_invoices_closed_total{status="skipped"}" as PROSE -- a bare
    ``in body`` check is fooled by that comment into reporting the
    "skipped" series as born when only the HELP line, not a sample, was
    present. Measured while writing this test: with the pre-seed loop
    emptied, a substring check still read "skipped" as present for exactly
    this reason. A real sample line starts at column 0 with the metric
    name; a HELP-text mention never does.
    """
    body = app_client.get("/metrics").text
    sample_lines = body.splitlines()
    unborn = [
        status
        for status in _CLOSED_STATUSES
        if not any(
            line.startswith(f'billing_invoices_closed_total{{status="{status}"}}')
            for line in sample_lines
        )
    ]
    assert not unborn, f"invoices-closed status series never born: {unborn}"

    src_root = Path(om.__file__).resolve().parents[1]
    proc = subprocess.run(
        [sys.executable, "-c", _COLLECT_INVOICES_CLOSED_AT_BOOT],
        capture_output=True,
        text=True,
        check=True,
        timeout=30,
        env={**os.environ, "PYTHONPATH": str(src_root)},
    )
    born: dict[str, float] = json.loads(proc.stdout.strip().splitlines()[-1])
    assert set(born) == set(_CLOSED_STATUSES), (
        f"boot series {sorted(born)} != statuses {sorted(_CLOSED_STATUSES)}"
    )
    assert all(v == 0.0 for v in born.values()), f"not born at zero: {born}"


_COLLECT_MARKUP_ZERO_MARGIN_AT_BOOT = """
import json
import observability.metrics as m
from prometheus_client import REGISTRY

out = {}
for mf in REGISTRY.collect():
    if mf.name == "billing_markup_zero_margin":
        for s in mf.samples:
            if s.name.endswith("_total"):
                out[s.labels["billing_mode"]] = s.value
print(json.dumps(out))
"""


def test_markup_zero_margin_series_are_born_at_zero(app_client: TestClient) -> None:
    """``BillingMarkupZeroMargin`` could not fire, because its series did not exist.

    Measured live 2026-09-13: ``billing_markup_zero_margin_total`` had ZERO series
    in Prometheus, against an increment site that has existed since 2026-07-24
    (``services/markup.py``). The alert is ``increase(...[1h]) > 0``, and over a
    non-existent series that is an EMPTY VECTOR — vacuously green. So "no paying
    tenant is billed at cost" and "this alert is structurally dead" were the SAME
    reading.

    ⛔ THE BIRTH LIST, NOT THE REGISTRY, IS THE NON-VACUOUS ASSERTION. A test that
    only scans the live registry passes the moment any earlier test in the process
    increments the same child, so it would keep passing with the birth block
    deleted. The pinning below is what a deletion cannot survive.
    """
    # 1. The birth list is EXACTLY the modes that can reach the increment.
    #    `compute_billed_line` returns early for internal (markup.py:498) and for
    #    pass_through (markup.py:509), both far above the counter, so birthing
    #    either would pin a series at 0 forever — an instrument that looks like it
    #    is watching a healthy population and is watching nothing.
    assert set(om.MARKUP_ZERO_MARGIN_BIRTH_MODES) == {
        markup.BILLING_MODE_STANDARD,
        markup.BILLING_MODE_BYO,
        markup.BILLING_MODE_BYO_MARKUP_ONLY,
    }
    assert set(om.MARKUP_ZERO_MARGIN_BIRTH_MODES).isdisjoint(
        {markup.BILLING_MODE_INTERNAL, markup.BILLING_MODE_PASS_THROUGH}
    )

    # 2. Live scrape surface: every birthed mode PRESENT.
    body = app_client.get("/metrics").text
    unborn = [
        mode for mode in om.MARKUP_ZERO_MARGIN_BIRTH_MODES if f'billing_mode="{mode}"' not in body
    ]
    assert not unborn, f"markup zero-margin mode series never born: {unborn}"

    # 3. The value at boot, from a clean interpreter that has incremented nothing.
    src_root = Path(om.__file__).resolve().parents[1]
    proc = subprocess.run(
        [sys.executable, "-c", _COLLECT_MARKUP_ZERO_MARGIN_AT_BOOT],
        capture_output=True,
        text=True,
        check=True,
        timeout=30,
        env={**os.environ, "PYTHONPATH": str(src_root)},
    )
    born: dict[str, float] = json.loads(proc.stdout.strip().splitlines()[-1])
    assert set(born) == set(om.MARKUP_ZERO_MARGIN_BIRTH_MODES), (
        f"boot series {sorted(born)} != birth list {sorted(om.MARKUP_ZERO_MARGIN_BIRTH_MODES)}"
    )
    assert all(v == 0.0 for v in born.values()), f"not born at zero: {born}"


_COLLECT_LICENSE_PERSISTED_AT_BOOT = """
import json
import observability.metrics as m

print(json.dumps({
    s.labels["service_key"]: s.value
    for metric in m.license_unattributed_persisted_total.collect()
    for s in metric.samples
    if s.name == "billing_license_unattributed_persisted_total"
}))
"""


def test_license_unattributed_persisted_series_are_born_at_zero(app_client: TestClient) -> None:
    """The ``billing.attribution-volume`` group has nothing to evaluate on a cold pod.

    ⚠️ THIS COUNTER IS NOT DEAD, and saying so is half the point of this test. It
    increments correctly at ``services/cost_flusher.py``'s ``survived`` loop and
    was measured live with 2 series on 2026-09-14. An earlier reading of "zero
    series" was taken at 2026-09-13T19:45Z, inside a window in which the path had
    not executed — a statement about the window, not the instrument. No increment
    was added; a second writer on a live counter would be a defect, not a fix.

    What birth buys is exact and narrow: a labelled Counter exports the HELP/TYPE
    header but NO SAMPLES until its first ``.labels(...)``, and that is per-pod and
    per-boot. Between a roll and the first flush that stores a licence-less row,
    ``BillingUnattributedVolumeSustained`` — ``increase(...[6h])`` over
    ``billing_cost_events_by_service_total`` — has an EMPTY VECTOR for a numerator
    and is vacuously green, so "no spend is stranded off an invoice line" and "this
    pod cannot answer yet" read identically.

    ⛔ THE BIRTH LIST, NOT THE REGISTRY, IS THE NON-VACUOUS ASSERTION. A test that
    only scans the live registry passes the moment any earlier test in the process
    increments the same child — and ``tests/test_cost_flusher.py`` (collected
    first, alphabetically) really does increment this counter. The boot subprocess
    below is what a deleted birth block cannot survive.
    """
    # 1. The birth list is EXACTLY the one value the increment site can produce
    #    without live traffic. `service_key` comes off the wire, so there is no
    #    finite roster to enumerate; what is birthed is the flusher's own
    #    normalisation (`c.get("service_key") or "unknown"`), which the counter's
    #    call site then re-applies. A REAL bucket — an emitter that sends no
    #    service_key lands in it — never a sentinel invented to make a series
    #    exist, which is the failure MARKUP_ZERO_MARGIN_BIRTH_MODES records.
    assert om.LICENSE_UNATTRIBUTED_PERSISTED_BIRTH_SERVICES == ("unknown",)

    # 2. Live scrape surface: the birthed key PRESENT. Order-independent, so an
    #    already-incremented child still satisfies presence while an unborn one
    #    cannot.
    body = app_client.get("/metrics").text
    unborn = [
        key
        for key in om.LICENSE_UNATTRIBUTED_PERSISTED_BIRTH_SERVICES
        if f'billing_license_unattributed_persisted_total{{service_key="{key}"}}' not in body
    ]
    assert not unborn, f"persisted-gap series never born: {unborn}"

    # 3. The value at boot, from a clean interpreter that has incremented nothing.
    src_root = Path(om.__file__).resolve().parents[1]
    proc = subprocess.run(
        [sys.executable, "-c", _COLLECT_LICENSE_PERSISTED_AT_BOOT],
        capture_output=True,
        text=True,
        check=True,
        timeout=30,
        env={**os.environ, "PYTHONPATH": str(src_root)},
    )
    born: dict[str, float] = json.loads(proc.stdout.strip().splitlines()[-1])
    assert set(born) == set(om.LICENSE_UNATTRIBUTED_PERSISTED_BIRTH_SERVICES), (
        f"boot series {sorted(born)} != birth list "
        f"{sorted(om.LICENSE_UNATTRIBUTED_PERSISTED_BIRTH_SERVICES)}"
    )
    assert all(v == 0.0 for v in born.values()), f"not born at zero: {born}"


_COLLECT_LICENSE_RESOLVED_AT_BOOT = """
import json
import observability.metrics as m

print(json.dumps({
    s.labels["service_key"]: s.value
    for metric in m.license_resolved_total.collect()
    for s in metric.samples
    if s.name == "billing_license_resolved_total"
}))
"""


def test_license_resolved_is_not_born_at_import_because_its_domain_is_operator_config() -> None:
    """⛔ THE CONTROL ON WHERE THE BIRTH LIVES, and why it is not in metrics.py.

    ``record_resolved`` is reached from exactly one place — ``cost_flusher``,
    inside ``if app_by_key:`` — and only for a row ``resolve_license`` resolved,
    which requires ``app_by_service_key.get(service_key)`` to have hit. So the
    producible domain is the KEYS of the operator's
    ``cost_attribution_app_by_service_key``, which is not knowable until settings
    resolve. Birthing ``"unknown"`` at import — the way the sibling
    ``billing_license_unattributed_persisted_total`` legitimately does — would pin
    a series at 0 FOREVER: an instrument that reads as though it is watching a
    healthy population while watching nothing.

    A clean interpreter that merely IMPORTS the metrics module must therefore
    export no children for this counter at all.
    """
    src_root = Path(om.__file__).resolve().parents[1]
    proc = subprocess.run(
        [sys.executable, "-c", _COLLECT_LICENSE_RESOLVED_AT_BOOT],
        capture_output=True,
        text=True,
        check=True,
        timeout=30,
        env={**os.environ, "PYTHONPATH": str(src_root)},
    )
    born: dict[str, float] = json.loads(proc.stdout.strip().splitlines()[-1])
    assert born == {}, (
        "billing_license_resolved_total must not be birthed at import: its "
        f"reachable domain is operator config, and {sorted(born)} would be "
        "pinned at 0 forever"
    )
