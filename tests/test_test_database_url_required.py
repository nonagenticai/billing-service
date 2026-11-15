"""The test suite must REFUSE to run when no test database is configured.

WHY THIS FILE EXISTS
--------------------
``tests/conftest.py`` used to resolve the DSN as ``TEST_DATABASE_URL`` ->
``DATABASE_URL`` -> a HARDCODED literal. The failure DIRECTION was the whole
defect: a default pointing at nothing yields "connection refused", which is
instantly legible; a default pointing at a live peer's database yields a
PLAUSIBLE red suite indistinguishable from your own regression, while the
session-scoped engine fixture DROP/CREATEs that peer's schema. Three lanes were
corrupted on 2026-09-07 and two of them debugged their own diff first.

HOW THIS AVOIDS BEING SELF-REFERENTIAL
--------------------------------------
The thing under test *configures the run that would test it* — by the time a
test body executes, conftest has already resolved a DSN successfully, so no
assertion made inside this process can observe the unset case.

Two independent layers, neither of which imports conftest:

1. The decision itself lives in ``tests/_production_tripwire.py`` as
   ``resolve_test_database_url(test_url, db_url)``, a pure function taking both
   values as ARGUMENTS. It is exercised directly, with no environment mutation,
   so the running session cannot be disturbed.
2. The wiring — that conftest actually calls it, and that pytest really dies —
   is exercised by launching a CHILD pytest in a subprocess with a SCRUBBED
   environment and asserting on its exit code and stderr. That is the only
   honest way to observe "the suite refuses to start".

Layer 1 alone would pass if conftest stopped calling the guard. Layer 2 alone
would pass on any nonzero exit. Both are required.
"""

from __future__ import annotations

import importlib.util
import os
import re
import subprocess
import sys
from pathlib import Path

import pytest

_REPO_ROOT = Path(__file__).resolve().parent.parent
_CONFTEST = _REPO_ROOT / "tests" / "conftest.py"

# Loaded BY PATH, matching tests/test_truncate_guard.py and conftest.py: `tests`
# is a package rooted at the repo root, and the module is deliberately not
# imported through it so this file exercises the same object conftest does
# without depending on package-import side effects.
_spec = importlib.util.spec_from_file_location(
    "_production_tripwire_dsn_under_test", Path(__file__).parent / "_production_tripwire.py"
)
assert _spec is not None and _spec.loader is not None
_tripwire = importlib.util.module_from_spec(_spec)
_spec.loader.exec_module(_tripwire)

TEST_DSN_EXPORT_TEMPLATE = _tripwire.TEST_DSN_EXPORT_TEMPLATE
TEST_DSN_ENV_VARS = _tripwire.TEST_DSN_ENV_VARS
resolve_test_database_url = _tripwire.resolve_test_database_url

# A DSN that is syntactically fine and can never be connected to during
# --collect-only (no engine is built until a fixture runs). Port 1 is
# unbindable; the database name is not in PRODUCTION_DB_NAMES.
_INERT_DSN = "postgresql+psycopg://u:p@127.0.0.1:1/collect_only_probe"


def _run_child_pytest(env_overrides: dict[str, str | None]) -> subprocess.CompletedProcess[str]:
    """Collect ONE test file in a child pytest with a controlled environment.

    ``--collect-only`` is enough: conftest resolves the DSN at IMPORT time,
    before any test module is imported and before any engine exists, so this
    touches no database at all. ``-o addopts=`` drops the repo's --cov flags,
    which would otherwise fail the child on coverage rather than on the DSN.
    """
    env = dict(os.environ)
    for key, value in env_overrides.items():
        if value is None:
            env.pop(key, None)
        else:
            env[key] = value
    return subprocess.run(
        [
            sys.executable,
            "-m",
            "pytest",
            str(_CONFTEST.parent / "test_test_database_url_required.py"),
            "--collect-only",
            "-q",
            "-p",
            "no:cacheprovider",
            "-o",
            "addopts=",
        ],
        cwd=_REPO_ROOT,
        env=env,
        capture_output=True,
        text=True,
        timeout=180,
    )


# ---------------------------------------------------------------------------
# Layer 1 — the guard function, all four env combinations
# ---------------------------------------------------------------------------


def test_refuses_when_neither_env_var_is_set():
    with pytest.raises(RuntimeError) as exc:
        resolve_test_database_url(None, None)
    message = str(exc.value)
    # Actionable in ONE read: names the variable, shows the export, says why.
    assert "TEST_DATABASE_URL" in message
    assert "DATABASE_URL" in message
    assert "export TEST_DATABASE_URL=" in message
    assert "no default" in message.lower()
    assert "DROP SCHEMA" in message


def test_empty_string_counts_as_unset():
    # A stale `export TEST_DATABASE_URL=` must not select an empty DSN.
    with pytest.raises(RuntimeError):
        resolve_test_database_url("", "")
    with pytest.raises(RuntimeError):
        resolve_test_database_url("   ", None)


def test_test_database_url_is_honored():
    assert resolve_test_database_url(_INERT_DSN, None) == _INERT_DSN


def test_database_url_is_honored_this_is_the_ci_path():
    # The fleet archetype's "Start ephemeral CI database" step exports DATABASE_URL (not
    # TEST_DATABASE_URL) into $GITHUB_ENV. Removing this fallback breaks every
    # build in the fleet, so it is pinned here.
    ci_dsn = "postgresql+psycopg://ci_test:ci_test@127.0.0.1:5432/billing-service_ci"
    assert resolve_test_database_url(None, ci_dsn) == ci_dsn


def test_test_database_url_takes_precedence_over_database_url():
    assert resolve_test_database_url(_INERT_DSN, "postgresql+psycopg://x/other") == _INERT_DSN


def test_precedence_order_is_declared():
    assert TEST_DSN_ENV_VARS == ("TEST_DATABASE_URL", "DATABASE_URL")


def test_a_set_but_unreachable_dsn_is_returned_not_refused():
    # "unset" and "set to something unreachable" must stay DISTINGUISHABLE.
    # Collapsing them makes a real database outage look like a config mistake.
    unreachable = "postgresql+psycopg://u:p@127.0.0.1:1/nothing_listens_here"
    assert resolve_test_database_url(unreachable, None) == unreachable


# ---------------------------------------------------------------------------
# The error text is a FLEET BROADCAST — it must not hand out a working DSN
# ---------------------------------------------------------------------------


def test_export_template_uses_placeholders_not_a_working_value():
    # A copy-pasteable DSN here would be copy-pasted by every lane that hits it,
    # rebuilding the shared-database collision this guard closes — with everyone's
    # explicit blessing. Port and database name are PER-LANE.
    assert "<YOUR_FREE_PORT>" in TEST_DSN_EXPORT_TEMPLATE
    assert "<YOUR_OWN_DB>" in TEST_DSN_EXPORT_TEMPLATE
    # No literal host:port anywhere in the template.
    assert not re.search(r":\d{2,5}/", TEST_DSN_EXPORT_TEMPLATE)


def test_refusal_tells_the_reader_how_to_pick_a_port_and_not_to_share_it():
    message = str(pytest.raises(RuntimeError, resolve_test_database_url, None, None).value)
    assert "lsof" in message  # how to find a free port
    assert "<YOUR_FREE_PORT>" in message
    assert "<YOUR_OWN_DB>" in message
    assert "another lane" in message  # explicit do-not-share
    # Self-explaining at 04:00: says this is not the reader's diff.
    assert "not" in message.lower() and "diff" in message.lower()


# ---------------------------------------------------------------------------
# Layer 2 — conftest carries no default of its own
# ---------------------------------------------------------------------------


def test_conftest_contains_no_hardcoded_dsn_literal():
    # The strongest available control: a default cannot be re-added to conftest
    # in any spelling without a DSN scheme literal appearing in the file.
    source = _CONFTEST.read_text()
    assert "postgresql" not in source, (
        "tests/conftest.py must contain no DSN literal. A hardcoded default does "
        "not fail closed — it connects to a peer's database and DROPs its schema."
    )


def test_conftest_delegates_to_the_guard():
    source = _CONFTEST.read_text()
    assert "resolve_test_database_url(" in source


# ---------------------------------------------------------------------------
# Layer 3 — end to end, in a child process with a scrubbed environment
# ---------------------------------------------------------------------------


@pytest.mark.timeout(200)
def test_child_pytest_refuses_to_start_with_both_env_vars_unset():
    result = _run_child_pytest({"TEST_DATABASE_URL": None, "DATABASE_URL": None})
    combined = result.stdout + result.stderr
    assert result.returncode != 0, f"child pytest should have failed:\n{combined}"
    assert "REFUSING TO RUN: no test database is configured" in combined
    assert "export TEST_DATABASE_URL=" in combined
    assert "<YOUR_FREE_PORT>" in combined


@pytest.mark.timeout(200)
def test_child_pytest_starts_normally_with_test_database_url_set():
    result = _run_child_pytest({"TEST_DATABASE_URL": _INERT_DSN, "DATABASE_URL": None})
    combined = result.stdout + result.stderr
    assert result.returncode == 0, f"child pytest should have collected:\n{combined}"
    assert "REFUSING TO RUN" not in combined


@pytest.mark.timeout(200)
def test_child_pytest_starts_normally_with_only_database_url_set():
    # Proves failing closed does NOT break CI, which supplies DATABASE_URL only.
    result = _run_child_pytest({"TEST_DATABASE_URL": None, "DATABASE_URL": _INERT_DSN})
    combined = result.stdout + result.stderr
    assert result.returncode == 0, f"child pytest should have collected:\n{combined}"
    assert "REFUSING TO RUN" not in combined
