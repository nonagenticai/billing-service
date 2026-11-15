"""Startup refuses ``DASHBOARD_AUTH_ENABLED`` != "true" in production.

The bypass in ``auth._decode_bearer_token`` hands every caller an admin
identity. Nothing used to stop it being set on the live Deployment; Settings()
now raises, which aborts the FastAPI lifespan and the process exits non-zero.
"""

from __future__ import annotations

import os
import subprocess
import sys
from pathlib import Path

import pytest
from pydantic import ValidationError

from settings import Settings

SRC = Path(__file__).resolve().parent.parent / "src"


def _prod_baseline(monkeypatch) -> None:
    """Every OTHER production requirement satisfied, so only the guard can fire."""
    monkeypatch.setenv("ENVIRONMENT", "production")
    monkeypatch.setenv("INTERNAL_SECRET", "any-non-empty-secret")
    monkeypatch.setenv(
        "DATABASE_URL",
        "postgresql+psycopg://metering_user:5tr0ng-r34l-Pa55w0rd@pgbouncer:6432/metering",  # pragma: allowlist secret
    )
    monkeypatch.setenv("REDIS_URL", "redis://redis:6379/2")
    monkeypatch.setenv("LICENSES_URL", "http://licenses:8544")
    monkeypatch.setenv("DIRECT_POSTGRES_HOST", "postgres.example.com:5432")
    monkeypatch.setenv("KEYCLOAK_URL", "https://auth.nonagentic.ai")
    monkeypatch.setenv("OTLP_TRACES_ENDPOINT", "http://alloy:4318/v1/traces")


def test_production_with_auth_enabled_starts(monkeypatch):
    """Control: the same baseline boots when auth is on (the live config)."""
    _prod_baseline(monkeypatch)
    monkeypatch.setenv("DASHBOARD_AUTH_ENABLED", "true")
    assert Settings().environment == "production"


@pytest.mark.parametrize("value", ["false", "False", "0", "1", "no", "", "true "])
def test_production_with_auth_disabled_refused(monkeypatch, value):
    """Every value auth.py reads as DISABLED is refused — including "1", which a
    pydantic bool would have called enabled while auth.py bypasses."""
    _prod_baseline(monkeypatch)
    monkeypatch.setenv("DASHBOARD_AUTH_ENABLED", value)
    with pytest.raises(ValidationError, match="DASHBOARD_AUTH_ENABLED"):
        Settings()


@pytest.mark.parametrize("env", ["Production", " production ", "prod"])
def test_production_tag_variants_refused(monkeypatch, env):
    monkeypatch.setenv("ENVIRONMENT", env)
    monkeypatch.setenv("KEYCLOAK_URL", "http://localhost:8080")
    monkeypatch.setenv("DASHBOARD_AUTH_ENABLED", "false")
    with pytest.raises(ValidationError, match="ENVIRONMENT="):
        Settings()


def test_unset_environment_defaults_to_production_and_is_refused(monkeypatch):
    """ENVIRONMENT defaults to production, so forgetting the tag fails closed."""
    monkeypatch.delenv("ENVIRONMENT", raising=False)
    monkeypatch.setenv("KEYCLOAK_URL", "")
    monkeypatch.setenv("DASHBOARD_AUTH_ENABLED", "false")
    with pytest.raises(ValidationError, match="DASHBOARD_AUTH_ENABLED"):
        Settings()


@pytest.mark.parametrize(
    "url",
    [
        "https://auth.nonagentic.ai",
        "https://AUTH.nonagentic.ai/",
        "https://auth.nonagentic.ai:443/x",
    ],
)
def test_production_keycloak_host_refused_even_when_mistagged(monkeypatch, url):
    """A non-prod ENVIRONMENT tag does not unlock the bypass on a pod that
    verifies real tenants' tokens."""
    monkeypatch.setenv("ENVIRONMENT", "dev")
    monkeypatch.setenv("KEYCLOAK_URL", url)
    monkeypatch.setenv("DASHBOARD_AUTH_ENABLED", "false")
    with pytest.raises(ValidationError, match="production issuer"):
        Settings()


@pytest.mark.parametrize("env", ["test", "dev", "development", "local"])
def test_non_production_with_auth_disabled_allowed(monkeypatch, env):
    monkeypatch.setenv("ENVIRONMENT", env)
    monkeypatch.setenv("KEYCLOAK_URL", "http://localhost:8080")
    monkeypatch.setenv("DASHBOARD_AUTH_ENABLED", "false")
    s = Settings()
    assert s.environment == env
    assert s.dashboard_auth_enabled == "false"


def test_refused_startup_exits_non_zero_with_fatal_message(tmp_path):
    """End to end: a fresh interpreter loading settings the way the lifespan
    does exits non-zero and says why on stderr."""
    env = {k: v for k, v in os.environ.items() if not k.startswith(("ENVIRONMENT", "KEYCLOAK"))}
    env.update(
        ENVIRONMENT="production",
        DASHBOARD_AUTH_ENABLED="false",
        KEYCLOAK_URL="https://auth.nonagentic.ai",
        PYTHONPATH=str(SRC),
    )
    proc = subprocess.run(  # noqa: S603 -- fixed argv, test-only
        [sys.executable, "-c", "from settings import get_settings; get_settings()"],
        env=env,
        cwd=tmp_path,
        capture_output=True,
        text=True,
        timeout=60,
        check=False,
    )
    assert proc.returncode != 0
    assert "FATAL: DASHBOARD_AUTH_ENABLED='false' disables bearer auth" in proc.stderr
