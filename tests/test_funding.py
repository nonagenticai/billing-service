"""``billing_cost_events.funding`` (alembic 0053): derivation, ingest, and the backfill twin.

Pins:
1. ``services.funding.derive_funding`` precedence (byo > owner_provided > contract
   basis > self_hosted cost_model > provider name > None).
2. The flusher honours a valid wire ``funding``, stores an INVALID one as NULL
   without re-deriving it, derives an absent one from the RESOLVED pricing row
   (never the pricing-miss backstop), and counts every NULL on
   ``billing_funding_unknown_total{service_key}``.
3. The migration's SQL twin (``FUNDING_CASE_SQL`` via ``backfill_funding``) agrees
   with the Python function on the same rows, only fills NULLs, and is idempotent.
"""

from __future__ import annotations

import importlib.util
import uuid
from datetime import UTC, datetime, timedelta
from decimal import Decimal
from pathlib import Path

import pytest
from prometheus_client import REGISTRY
from sqlalchemy import select, text

from models.db import CostEvent, ProviderPricing
from services.cost_flusher import flush_costs
from services.funding import (
    EXTERNAL_PROVIDERS,
    LOCAL_PROVIDERS,
    derive_funding,
    parse_wire_funding,
)
from tests._aggregator_helpers import _make_cost
from tests.conftest import T

_MIG_PATH = (
    Path(__file__).resolve().parent.parent
    / "alembic"
    / "versions"
    / "0053_cost_funding_usage_daily.py"
)


def _load_migration():
    spec = importlib.util.spec_from_file_location("_mig_0053", _MIG_PATH)
    assert spec is not None and spec.loader is not None
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    return mod


def _unknown(service_key: str) -> float:
    return (
        REGISTRY.get_sample_value("billing_funding_unknown_total", {"service_key": service_key})
        or 0.0
    )


# ---------------------------------------------------------------------------
# 1. Precedence (pure)
# ---------------------------------------------------------------------------


@pytest.mark.unit
@pytest.mark.parametrize(
    ("usage_extra", "provider", "basis", "cost_model", "expected"),
    [
        # byo wins over everything, even a cash contract.
        ({"byo": True}, "openai", "cash", "per_token", "tenant_byo"),
        ({"byo": "true", "owner_provided": True}, "openai", None, None, "tenant_byo"),
        # owner_provided: the platform's own key on an external vendor.
        ({"owner_provided": True}, "pgx-local", "internal_transfer", None, "platform_external"),
        # contract basis beats the provider name.
        ({}, "pgx-local", "cash", None, "platform_external"),
        ({}, "openai", "internal_transfer", None, "local_gpu"),
        # self_hosted cost_model with no basis.
        ({}, "something-new", None, "self_hosted_amortized", "local_gpu"),
        # provider-name fallback (pricing miss).
        (None, "mlx-whatever", None, None, "local_gpu"),
        (None, "kokoro", None, None, "local_gpu"),
        (None, "Anthropic", None, None, "platform_external"),
        # nothing answers -> None, never guessed.
        (None, "brand-new-vendor", None, "per_token", None),
        ({"byo": False}, None, None, None, None),
    ],
)
def test_derive_funding_precedence(usage_extra, provider, basis, cost_model, expected):
    """⚠️ MUTATION TARGET: reorder any two rules in derive_funding -> a row here fails."""
    assert (
        derive_funding(
            usage_extra=usage_extra,
            provider=provider,
            contract_basis=basis,
            cost_model=cost_model,
        )
        == expected
    )


@pytest.mark.unit
@pytest.mark.parametrize(
    ("raw", "expected"),
    [
        (None, (False, None)),
        ("", (False, None)),
        ("  ", (False, None)),
        ("local_gpu", (True, "local_gpu")),
        ("tenant_byo", (True, "tenant_byo")),
        ("LOCAL_GPU", (True, None)),  # case is part of the contract
        ("free", (True, None)),
    ],
)
def test_parse_wire_funding(raw, expected):
    assert parse_wire_funding(raw) == expected


@pytest.mark.unit
def test_the_sql_twin_names_exactly_the_python_provider_lists():
    """The backfill's provider lists are literals; they must not drift from the module's."""
    sql = _load_migration().FUNDING_CASE_SQL
    for p in LOCAL_PROVIDERS | EXTERNAL_PROVIDERS:
        assert f"'{p}'" in sql, p
    local_block = sql.split("THEN 'local_gpu'")[-2]
    for p in EXTERNAL_PROVIDERS:
        assert f"'{p}'" not in local_block, p


# ---------------------------------------------------------------------------
# 2. Ingest
# ---------------------------------------------------------------------------


async def _seed_pricing(factory, provider: str, cost_basis: str, cost_model: str = "per_token"):
    async with factory() as s:
        s.add(
            ProviderPricing(
                provider=provider,
                model=None,
                cost_model=cost_model,
                cost_basis=cost_basis,
                input_per_1m_usd=Decimal("1"),
                output_per_1m_usd=Decimal("1"),
                effective_from=datetime(2026, 1, 1, tzinfo=UTC),
            )
        )
        await s.commit()


async def _stored_funding(factory, tenant: str) -> str | None:
    async with factory() as s:
        return (
            await s.execute(select(CostEvent.funding).where(CostEvent.tenant_id == tenant))
        ).scalar_one()


@pytest.mark.unit
class TestIngest:
    async def test_a_valid_wire_value_is_honoured_verbatim(self, db_session_factory):
        """Even against a cash contract: the emitter knows it was BYO."""
        await _seed_pricing(db_session_factory, "openai", "cash")
        tenant = T("funding-wire-valid")
        buf = [_make_cost(tenant_id=tenant, provider="openai", funding="tenant_byo")]
        assert await flush_costs(buf, db_session_factory, None) == 1
        assert await _stored_funding(db_session_factory, tenant) == "tenant_byo"

    async def test_an_invalid_wire_value_is_stored_null_and_counted_not_derived(
        self, db_session_factory
    ):
        """⚠️ MUTATION TARGET: drop the funding_asserted check -> derives local_gpu here."""
        await _seed_pricing(db_session_factory, "pgx-local", "internal_transfer")
        tenant = T("funding-wire-invalid")
        sk = f"funding-bad-{uuid.uuid4().hex[:8]}"
        before = _unknown(sk)
        buf = [_make_cost(tenant_id=tenant, provider="pgx-local", service_key=sk, funding="gratis")]
        assert await flush_costs(buf, db_session_factory, None) == 1
        assert await _stored_funding(db_session_factory, tenant) is None
        assert _unknown(sk) - before == 1.0

    async def test_absent_funding_is_derived_from_the_resolved_contract(self, db_session_factory):
        await _seed_pricing(db_session_factory, "pgx-local", "internal_transfer")
        await _seed_pricing(db_session_factory, "openai", "cash")
        local, ext = T("funding-derive-local"), T("funding-derive-ext")
        buf = [
            _make_cost(tenant_id=local, provider="pgx-local"),
            _make_cost(tenant_id=ext, provider="openai"),
        ]
        assert await flush_costs(buf, db_session_factory, None) == 2
        assert await _stored_funding(db_session_factory, local) == "local_gpu"
        assert await _stored_funding(db_session_factory, ext) == "platform_external"

    async def test_byo_in_usage_extra_beats_a_cash_contract(self, db_session_factory):
        await _seed_pricing(db_session_factory, "openai", "cash")
        tenant = T("funding-byo")
        buf = [_make_cost(tenant_id=tenant, provider="openai", usage_extra='{"byo": true}')]
        assert await flush_costs(buf, db_session_factory, None) == 1
        assert await _stored_funding(db_session_factory, tenant) == "tenant_byo"

    async def test_a_pricing_miss_is_not_read_as_local(self, db_session_factory):
        """No pricing row, unknown provider: the stored cost_basis is the
        internal_transfer BACKSTOP, which must not derive local_gpu.

        ⚠️ MUTATION TARGET: feed row["cost_basis"] (post-backstop) as the contract
        basis -> this row becomes local_gpu and the counter does not move.
        """
        tenant = T("funding-miss")
        sk = f"funding-miss-{uuid.uuid4().hex[:8]}"
        before = _unknown(sk)
        buf = [_make_cost(tenant_id=tenant, provider="brand-new-vendor", service_key=sk)]
        assert await flush_costs(buf, db_session_factory, None) == 1
        assert await _stored_funding(db_session_factory, tenant) is None
        assert _unknown(sk) - before == 1.0

    async def test_a_derived_value_is_not_counted(self, db_session_factory):
        tenant = T("funding-nocount")
        sk = f"funding-ok-{uuid.uuid4().hex[:8]}"
        before = _unknown(sk)
        buf = [_make_cost(tenant_id=tenant, provider="claude", service_key=sk)]
        assert await flush_costs(buf, db_session_factory, None) == 1
        assert await _stored_funding(db_session_factory, tenant) == "platform_external"
        assert _unknown(sk) == before


# ---------------------------------------------------------------------------
# 3. Backfill twin
# ---------------------------------------------------------------------------

_CASES = [
    # (label, provider, usage_extra, pricing (basis, model) or None)
    ("byo", "openai", {"byo": True}, ("cash", "per_token")),
    ("owner", "pgx-local", {"owner_provided": True}, ("internal_transfer", "per_token")),
    (
        "byo-and-owner",
        "pgx-local",
        {"byo": True, "owner_provided": True},
        ("internal_transfer", "per_token"),
    ),
    ("cash", "openai", {}, ("cash", "per_token")),
    ("internal", "mlx-nlp", {}, ("internal_transfer", "per_character")),
    ("amortized", "gemma", {}, ("internal_transfer", "self_hosted_amortized")),
    ("name-local", "kokoro", None, None),
    ("name-mlx", "mlx-new", None, None),
    ("name-ext", "groq", None, None),
    ("unknown", "brand-new-vendor", None, None),
]


@pytest.mark.unit
async def test_the_backfill_sql_agrees_with_derive_funding_and_only_fills_nulls(
    db_session_factory,
):
    """⚠️ MUTATION TARGETS: swap any two WHEN branches of FUNDING_CASE_SQL, or drop
    ``e.funding IS NULL`` from ``_UPDATE_RANGE`` -> this fails."""
    mig = _load_migration()
    ts = datetime.now(UTC) - timedelta(days=2)
    expected: dict[str, str | None] = {}
    labels: dict[str, str] = {}
    preset = T("funding-bf-preset")
    async with db_session_factory() as s:
        pricing: dict[str, ProviderPricing] = {}
        for label, provider, extra, pr in _CASES:
            pid = None
            if pr is not None:
                row = pricing.get(provider)
                if row is None:
                    row = ProviderPricing(
                        provider=provider,
                        model=None,
                        cost_model=pr[1],
                        cost_basis=pr[0],
                        input_per_1m_usd=Decimal("1"),
                        output_per_1m_usd=Decimal("1"),
                        effective_from=datetime(2026, 1, 1, tzinfo=UTC),
                    )
                    s.add(row)
                    await s.flush()
                    pricing[provider] = row
                pid = row.id
            tenant = T(f"funding-bf-{label}")
            labels[tenant] = label
            s.add(
                CostEvent(
                    tenant_id=tenant,
                    service_key="sk",  # pragma: allowlist secret
                    provider=provider,
                    input_tokens=1,
                    output_tokens=1,
                    usage_extra=extra,
                    pricing_id=pid,
                    cost_source="t",
                    created_at=ts,
                )
            )
            expected[tenant] = derive_funding(
                usage_extra=extra,
                provider=provider,
                contract_basis=pr[0] if pr else None,
                cost_model=pr[1] if pr else None,
            )
        # A row that already carries a value must not be rewritten.
        s.add(
            CostEvent(
                tenant_id=preset,
                service_key="sk",  # pragma: allowlist secret
                provider="openai",
                input_tokens=1,
                output_tokens=1,
                usage_extra={"byo": True},
                funding="local_gpu",
                cost_source="t",
                created_at=ts,
            )
        )
        await s.commit()

    async with db_session_factory() as s:
        conn = await s.connection()
        # batch=2 forces several keyset ranges over 11 rows.
        updated = await conn.run_sync(lambda c: mig.backfill_funding(c, batch=2))
        await s.commit()
    assert updated == sum(1 for v in expected.values() if v is not None)

    async with db_session_factory() as s:
        got = dict(
            (await s.execute(text("SELECT tenant_id::text, funding FROM billing_cost_events")))
            .tuples()
            .all()
        )
    for tenant, want in expected.items():
        assert got[tenant] == want, (labels[tenant], got[tenant], want)
    assert got[preset] == "local_gpu"

    # Idempotent: a second run touches nothing.
    async with db_session_factory() as s:
        conn = await s.connection()
        again = await conn.run_sync(lambda c: mig.backfill_funding(c, batch=3))
        await s.commit()
    assert again == 0
