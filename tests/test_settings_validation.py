"""Settings validators — required fields in production, range checks.

The bare ``Settings()`` defaults intentionally permit local dev to construct an
instance with placeholder values. Production must refuse these defaults so a
misconfigured pod fails fast at startup instead of silently routing billing
through a placeholder DSN or accepting requests with an empty
``internal_secret`` that disables the internal endpoints.
"""

from __future__ import annotations

import pytest
from pydantic import ValidationError

from settings import MIN_RETENTION_DAYS, Settings

# Conftest sets ENVIRONMENT=test, so all other tests bypass the
# prod-required validators. We explicitly flip back to "production" per test.


@pytest.fixture(autouse=True)
def _auth_enabled(monkeypatch):
    """Conftest disables dashboard auth; production Settings() now refuses that
    (``_refuse_auth_disabled_in_production``). These tests exercise OTHER prod
    validators, so run them with auth enabled, as the live Deployment does."""
    monkeypatch.setenv("DASHBOARD_AUTH_ENABLED", "true")


def _real_dsn() -> str:
    """A DATABASE_URL that doesn't trip the placeholder check."""
    return "postgresql+psycopg://metering_user:5tr0ng-r34l-Pa55w0rd@pgbouncer:6432/metering"


def test_database_url_required_in_production(monkeypatch):
    monkeypatch.setenv("ENVIRONMENT", "production")
    monkeypatch.setenv("INTERNAL_SECRET", "any-non-empty-secret")
    monkeypatch.delenv("DATABASE_URL", raising=False)
    # The default URL embeds ":password@" — which we treat as a placeholder.
    with pytest.raises(ValidationError, match="database_url"):
        Settings()


def test_database_url_placeholder_password_rejected_in_production(monkeypatch):
    monkeypatch.setenv("ENVIRONMENT", "production")
    monkeypatch.setenv("INTERNAL_SECRET", "any-non-empty-secret")
    monkeypatch.setenv(
        "DATABASE_URL",
        "postgresql+psycopg://metering_user:password@db.example.com:6432/metering",
    )
    with pytest.raises(ValidationError, match="placeholder"):
        Settings()


def test_database_url_real_credential_accepted_in_production(monkeypatch):
    monkeypatch.setenv("ENVIRONMENT", "production")
    monkeypatch.setenv("INTERNAL_SECRET", "any-non-empty-secret")
    monkeypatch.setenv("DATABASE_URL", _real_dsn())
    # Other URLs required-in-prod (added 2026-05-10 config-externalization)
    monkeypatch.setenv("REDIS_URL", "redis://redis:6379/2")
    monkeypatch.setenv("LICENSES_URL", "http://licenses:8544")
    monkeypatch.setenv("DIRECT_POSTGRES_HOST", "postgres.example.com:5432")
    monkeypatch.setenv("KEYCLOAK_URL", "http://keycloak:8080")
    monkeypatch.setenv("OTLP_TRACES_ENDPOINT", "http://alloy:4318/v1/traces")
    s = Settings()
    assert s.environment == "production"
    assert s.database_url == _real_dsn()


def test_internal_secret_required_in_production(monkeypatch):
    monkeypatch.setenv("ENVIRONMENT", "production")
    monkeypatch.setenv("DATABASE_URL", _real_dsn())
    monkeypatch.setenv("INTERNAL_SECRET", "")
    with pytest.raises(ValidationError, match="internal_secret"):
        Settings()


def test_internal_secret_unset_in_production_uses_empty_default_and_fails(monkeypatch):
    monkeypatch.setenv("ENVIRONMENT", "production")
    monkeypatch.setenv("DATABASE_URL", _real_dsn())
    monkeypatch.delenv("INTERNAL_SECRET", raising=False)
    with pytest.raises(ValidationError, match="internal_secret"):
        Settings()


def test_markup_pct_negative_rejected(monkeypatch):
    monkeypatch.setenv("DEFAULT_MARKUP_PCT", "-1")
    with pytest.raises(ValidationError, match="default_markup_pct"):
        Settings()


def test_markup_pct_above_cap_rejected(monkeypatch):
    monkeypatch.setenv("DEFAULT_MARKUP_PCT", "1001")
    with pytest.raises(ValidationError, match="default_markup_pct"):
        Settings()


def test_markup_pct_zero_accepted(monkeypatch):
    monkeypatch.setenv("DEFAULT_MARKUP_PCT", "0")
    s = Settings()
    assert s.default_markup_pct == 0


def test_markup_pct_normal_value_accepted(monkeypatch):
    monkeypatch.setenv("DEFAULT_MARKUP_PCT", "30.0")
    s = Settings()
    assert s.default_markup_pct == 30.0


def test_test_environment_skips_prod_validators(monkeypatch):
    """ENVIRONMENT=test skips the prod-required validators; bare Settings() works.

    Sets ENVIRONMENT explicitly rather than relying on conftest. conftest uses
    ``os.environ.setdefault("ENVIRONMENT", "test")`` (conftest.py:74), which
    LOSES to an ENVIRONMENT already exported by the caller — and CI exports
    ``ENVIRONMENT: development``. So the old docstring's premise ("conftest
    already sets ENVIRONMENT=test") was false in CI and this test failed there
    with ``assert 'development' == 'test'`` while passing on every dev machine.
    Every other test in this file already sets the value it needs; this one now
    matches, which also makes it independent of ambient environment.
    """
    monkeypatch.setenv("ENVIRONMENT", "test")
    monkeypatch.delenv("DATABASE_URL", raising=False)
    monkeypatch.delenv("INTERNAL_SECRET", raising=False)
    s = Settings()  # must NOT raise
    assert s.environment == "test"


def test_consumer_enabled_defaults_true(monkeypatch):
    monkeypatch.delenv("CONSUMER_ENABLED", raising=False)
    assert Settings().consumer_enabled is True


def test_consumer_enabled_can_be_disabled_for_readonly(monkeypatch):
    """Phase 2 read-only deploy sets CONSUMER_ENABLED=false so billing does not
    join the shared consumer group / re-fire overage webhooks."""
    monkeypatch.setenv("CONSUMER_ENABLED", "false")
    assert Settings().consumer_enabled is False


def test_dev_environment_skips_prod_validators(monkeypatch):
    monkeypatch.setenv("ENVIRONMENT", "dev")
    monkeypatch.delenv("DATABASE_URL", raising=False)
    monkeypatch.delenv("INTERNAL_SECRET", raising=False)
    s = Settings()  # must NOT raise
    assert s.environment == "dev"


def _set_prod_required_baseline(monkeypatch) -> None:
    """Set every required-in-prod env var so we can flip one off per test."""
    monkeypatch.setenv("ENVIRONMENT", "production")
    monkeypatch.setenv("DATABASE_URL", _real_dsn())
    monkeypatch.setenv("INTERNAL_SECRET", "any-non-empty-secret")
    monkeypatch.setenv("REDIS_URL", "redis://redis:6379/2")
    monkeypatch.setenv("LICENSES_URL", "http://licenses:8544")
    monkeypatch.setenv("DIRECT_POSTGRES_HOST", "postgres.example.com:5432")
    monkeypatch.setenv("KEYCLOAK_URL", "http://keycloak:8080")
    monkeypatch.setenv("OTLP_TRACES_ENDPOINT", "http://alloy:4318/v1/traces")


def test_all_required_set_accepted_in_production(monkeypatch):
    _set_prod_required_baseline(monkeypatch)
    s = Settings()
    assert s.redis_url == "redis://redis:6379/2"
    assert s.direct_postgres_host == "postgres.example.com:5432"
    assert s.keycloak_url == "http://keycloak:8080"


@pytest.mark.parametrize(
    "var",
    [
        "REDIS_URL",
        "LICENSES_URL",
        "DIRECT_POSTGRES_HOST",
        "KEYCLOAK_URL",
        "OTLP_TRACES_ENDPOINT",
    ],
)
def test_each_required_field_missing_rejected_in_production(monkeypatch, var):
    """Missing any of the required service URLs must crash Settings() in prod."""
    _set_prod_required_baseline(monkeypatch)
    monkeypatch.delenv(var, raising=False)
    with pytest.raises(ValidationError, match="missing required fields in production"):
        Settings()


# --- retention_days floor ---------------------------------------------------
#
# retention_days is not only a row-DELETE window: rollup_engine's
# drop_old_cost_partitions turns it into DROP TABLE against whole cost_events
# partitions. A DROP fires neither the BEFORE TRUNCATE guard nor the AFTER
# DELETE/TRUNCATE audit, so an unsafe value here is unrecoverable financial data
# loss that leaves billing_delete_audit correctly empty. Refuse it at
# construction, where the pod crashes visibly at startup instead.


def test_retention_days_zero_rejected():
    with pytest.raises(ValidationError, match="retention_days must be >="):
        Settings(retention_days=0)


def test_retention_days_negative_rejected():
    with pytest.raises(ValidationError, match="retention_days must be >="):
        Settings(retention_days=-1)


def test_retention_days_below_one_month_rejected():
    with pytest.raises(ValidationError, match="retention_days must be >="):
        Settings(retention_days=MIN_RETENTION_DAYS - 1)


def test_retention_days_env_var_zero_rejected(monkeypatch):
    """The realistic vector is the Deployment env block, not a kwarg."""
    monkeypatch.setenv("RETENTION_DAYS", "0")
    with pytest.raises(ValidationError, match="retention_days must be >="):
        Settings()


def test_retention_days_default_is_unchanged_and_accepted():
    assert Settings().retention_days == 90
    assert Settings(retention_days=MIN_RETENTION_DAYS).retention_days == MIN_RETENTION_DAYS
