"""The SLO threshold must not sit on a default bucket edge.

``MeteringQuotaBudgetLatencyHigh`` fires at p95 ``> 0.5``. The
prometheus-fastapi-instrumentator default ``latency_lowr_buckets`` is
``(0.1, 0.5, 1)``, which puts the threshold EXACTLY on a boundary: once the
tail crosses 0.5 the quantile can only interpolate inside ``(0.5, 1.0]`` and
cannot tell 0.51s from 0.99s. The 2026-08-27 reading of p95=0.820 was exactly
``0.5 + (41/64)*0.5`` -- bucket geometry, not a latency.

These tests pin the fix so a future refactor cannot silently restore the
default and turn the SLO number back into an artefact.
"""

import re

from fastapi import FastAPI
from fastapi.testclient import TestClient
from prometheus_client import CollectorRegistry
from prometheus_fastapi_instrumentator import Instrumentator

# Kept in step with src/main.py deliberately: this file asserts the PROPERTY
# (resolution around the threshold), not merely that two literals match.
CONFIGURED = (0.05, 0.1, 0.25, 0.5, 0.75, 1.0, 2.5, 5.0, 10.0)
LIBRARY_DEFAULT = (0.1, 0.5, 1)
SLO_THRESHOLD = 0.5


def _emitted_boundaries(buckets):
    registry = CollectorRegistry()
    app = FastAPI()

    @app.get("/api/budget")
    def _budget():
        return {"ok": True}

    instrumentator = Instrumentator(
        excluded_handlers=["/metrics", "/health", "/health/live", "/health/ready"],
        registry=registry,
    )
    kwargs = {} if buckets is None else {"latency_lowr_buckets": buckets}
    instrumentator.instrument(app, **kwargs).expose(
        app, endpoint="/metrics", include_in_schema=False
    )

    client = TestClient(app)
    client.get("/api/budget")
    body = client.get("/metrics").text
    found = set(
        re.findall(
            r'http_request_duration_seconds_bucket\{[^}]*le="([^"]+)"',
            body,
        )
    )
    return sorted(found, key=float)


def test_default_buckets_put_the_slo_threshold_on_an_edge():
    """Positive control: without the fix the defect is real and reproducible.

    Without this, the test below could pass against a library whose defaults
    had changed, and we would be asserting nothing.
    """
    boundaries = [float(b) for b in _emitted_boundaries(None) if b != "+Inf"]
    assert boundaries == list(LIBRARY_DEFAULT)
    assert SLO_THRESHOLD in boundaries
    # Nothing between the threshold and the next boundary: the blind band.
    above = [b for b in boundaries if SLO_THRESHOLD < b < 1.0]
    assert above == [], f"expected no resolution in (0.5, 1.0), got {above}"


def test_configured_buckets_resolve_around_the_slo_threshold():
    boundaries = [float(b) for b in _emitted_boundaries(CONFIGURED) if b != "+Inf"]
    assert boundaries == list(CONFIGURED)
    # The point of the change: the band the alert lands in is no longer blind.
    assert [b for b in boundaries if SLO_THRESHOLD < b < 1.0], (
        "no boundary between the 0.5 threshold and 1.0 -- a p95 in that band "
        "is still pure interpolation and the alert value stays unactionable"
    )


def test_configured_buckets_are_a_superset_of_the_default():
    """No existing selector may break -- notably the alert's own non-vacuity
    guard, which anchors on ``le="+Inf"``."""
    boundaries = {float(b) for b in _emitted_boundaries(CONFIGURED) if b != "+Inf"}
    missing = set(LIBRARY_DEFAULT) - boundaries
    assert not missing, f"dropped default boundaries {sorted(missing)}"
    assert "+Inf" in _emitted_boundaries(CONFIGURED)


def test_main_module_configures_explicit_buckets():
    """The app itself must pass them -- not just this test's local harness.

    Asserted against the parsed AST, NOT a substring search. The explanatory
    comment above the call in src/main.py contains the words
    ``latency_lowr_buckets``, so a naive ``in source`` check passes even when
    the keyword argument has been deleted -- a test that cannot fail. The AST
    sees only the actual call.
    """
    import ast
    import pathlib

    source = (pathlib.Path(__file__).resolve().parents[1] / "src" / "main.py").read_text()
    tree = ast.parse(source)

    instrument_calls = [
        node
        for node in ast.walk(tree)
        if isinstance(node, ast.Call)
        and isinstance(node.func, ast.Attribute)
        and node.func.attr == "instrument"
    ]
    assert instrument_calls, "no .instrument(...) call found in src/main.py"

    keyworded = [
        call
        for call in instrument_calls
        if any(kw.arg == "latency_lowr_buckets" for kw in call.keywords)
    ]
    assert keyworded, (
        "src/main.py calls .instrument(...) without latency_lowr_buckets; the "
        "SLO number has reverted to library-default bucket geometry, where the "
        "0.5 alert threshold sits on a bucket edge"
    )

    # The value must actually resolve around the threshold, not merely exist.
    kw = next(k for k in keyworded[0].keywords if k.arg == "latency_lowr_buckets")
    assert isinstance(kw.value, (ast.List, ast.Tuple)), (
        f"latency_lowr_buckets is not a literal list/tuple: {ast.dump(kw.value)}"
    )
    configured = [ast.literal_eval(e) for e in kw.value.elts]
    assert [b for b in configured if SLO_THRESHOLD < b < 1.0], (
        f"configured buckets {configured} have no boundary between the 0.5 "
        "threshold and 1.0 -- the blind band this fix exists to close"
    )
