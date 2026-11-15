"""Schema-only assertions for the stacked-budget / audit-log / pricing phase.

Verifies the ORM model definitions in ``models.db`` carry the new columns,
indexes, and (absence of) constraints that the follow-up service phases
depend on. Pure introspection — no service behavior is exercised.
"""

from __future__ import annotations

from typing import cast

import pytest
from sqlalchemy import Numeric, String, Table, inspect

from db import Base
from models.db import AuditLog, CostEvent, ProviderPricing, TokenBudget

# ---------------------------------------------------------------------------
# TokenBudget
# ---------------------------------------------------------------------------


class TestTokenBudgetSchema:
    @pytest.mark.unit
    def test_legacy_unique_constraint_dropped(self) -> None:
        """The (tenant_id, license_id, period) unique constraint is gone.

        Stacked budgets need multiple rows per (tenant, license, period); the
        old uq_token_budget would refuse them.
        """
        table = cast(Table, TokenBudget.__table__)
        names = {c.name for c in table.constraints if c.name}
        assert "uq_token_budget" not in names

    @pytest.mark.unit
    def test_new_columns_present(self) -> None:
        """All stacking columns exist on the model."""
        cols = TokenBudget.__table__.columns
        for name in ("priority", "starts_at", "expires_at", "consumed_tokens", "source"):
            assert name in cols, f"missing column: {name}"

    @pytest.mark.unit
    def test_priority_not_null_default_100(self) -> None:
        col = TokenBudget.__table__.columns["priority"]
        assert col.nullable is False
        # server_default lives on the column for additive migration safety.
        assert col.server_default is not None
        assert "100" in str(col.server_default.arg)

    @pytest.mark.unit
    def test_expires_at_nullable(self) -> None:
        col = TokenBudget.__table__.columns["expires_at"]
        assert col.nullable is True

    @pytest.mark.unit
    def test_starts_at_timezone_aware(self) -> None:
        col = TokenBudget.__table__.columns["starts_at"]
        assert col.nullable is False
        # SQLAlchemy DateTime(timezone=True) maps to TIMESTAMPTZ on Postgres.
        assert getattr(col.type, "timezone", False) is True

    @pytest.mark.unit
    def test_consumed_tokens_numeric_20_6(self) -> None:
        col = TokenBudget.__table__.columns["consumed_tokens"]
        assert col.nullable is False
        ctype = cast(Numeric, col.type)
        assert ctype.precision == 20
        assert ctype.scale == 6

    @pytest.mark.unit
    def test_source_varchar_64_default_manual(self) -> None:
        col = TokenBudget.__table__.columns["source"]
        assert col.nullable is False
        assert cast(String, col.type).length == 64

    @pytest.mark.unit
    def test_active_drain_index_present(self) -> None:
        idx_names = {ix.name for ix in cast(Table, TokenBudget.__table__).indexes}
        assert "ix_tokenbudget_active_drain" in idx_names

    @pytest.mark.unit
    def test_active_drain_index_columns(self) -> None:
        ix = next(
            i
            for i in cast(Table, TokenBudget.__table__).indexes
            if i.name == "ix_tokenbudget_active_drain"
        )
        assert [c.name for c in ix.columns] == [
            "tenant_id",
            "active",
            "priority",
            "expires_at",
        ]

    @pytest.mark.unit
    def test_active_column_unchanged(self) -> None:
        """`active` stays as-is — soft-delete semantics land in a later phase."""
        cols = TokenBudget.__table__.columns
        assert cols["active"].nullable is False


# ---------------------------------------------------------------------------
# AuditLog
# ---------------------------------------------------------------------------


class TestAuditLogSchema:
    @pytest.mark.unit
    def test_table_registered(self) -> None:
        assert "audit_log" in Base.metadata.tables

    @pytest.mark.unit
    def test_pk_is_bigint_autoincrement(self) -> None:
        col = AuditLog.__table__.columns["id"]
        assert col.primary_key is True
        # BigInteger maps to BIGINT (no precision/length attrs to assert,
        # just the python_type contract).
        assert col.type.python_type is int
        assert col.autoincrement is True

    @pytest.mark.unit
    def test_columns_present(self) -> None:
        cols = AuditLog.__table__.columns
        expected = {
            "id",
            "ts",
            "actor_sub",
            "actor_role",
            "action",
            "resource_type",
            "resource_id",
            "tenant_id",
            "before",
            "after",
            "request_id",
        }
        assert expected.issubset(set(cols.keys()))

    @pytest.mark.unit
    def test_ts_timezone_aware(self) -> None:
        col = AuditLog.__table__.columns["ts"]
        assert col.nullable is False
        assert getattr(col.type, "timezone", False) is True

    @pytest.mark.unit
    def test_jsonb_payload_columns(self) -> None:
        for name in ("before", "after"):
            col = AuditLog.__table__.columns[name]
            # postgresql.JSONB visit_name is 'JSONB'.
            assert col.type.__class__.__name__ == "JSONB"

    @pytest.mark.unit
    def test_required_indexes(self) -> None:
        idx_names = {ix.name for ix in cast(Table, AuditLog.__table__).indexes}
        assert {
            "ix_audit_log_ts",
            "ix_audit_log_tenant_ts",
            "ix_audit_log_action_ts",
        }.issubset(idx_names)

    @pytest.mark.unit
    def test_no_foreign_keys(self) -> None:
        """Audit log must survive even if a referenced row is hard-deleted."""
        for col in AuditLog.__table__.columns:
            assert not col.foreign_keys, f"audit_log.{col.name} has FK; should not"


# ---------------------------------------------------------------------------
# ProviderPricing
# ---------------------------------------------------------------------------


class TestProviderPricingSchema:
    @pytest.mark.unit
    def test_table_registered(self) -> None:
        assert "provider_pricing" in Base.metadata.tables

    @pytest.mark.unit
    def test_columns_present(self) -> None:
        cols = ProviderPricing.__table__.columns
        expected = {
            "id",
            "provider",
            "model",
            "cost_model",
            "input_per_1m_usd",
            "output_per_1m_usd",
            "per_request_usd",
            "monthly_fixed_cost_usd",
            "effective_from",
            "effective_until",
            "notes",
        }
        assert expected.issubset(set(cols.keys()))

    @pytest.mark.unit
    def test_cost_model_is_varchar_not_enum(self) -> None:
        """cost_model is intentionally a plain VARCHAR for forward-compat."""
        col = ProviderPricing.__table__.columns["cost_model"]
        assert col.type.__class__.__name__ == "String"
        assert cast(String, col.type).length == 32
        assert col.nullable is False

    @pytest.mark.unit
    def test_effective_from_not_null(self) -> None:
        assert ProviderPricing.__table__.columns["effective_from"].nullable is False

    @pytest.mark.unit
    def test_effective_until_nullable(self) -> None:
        assert ProviderPricing.__table__.columns["effective_until"].nullable is True

    @pytest.mark.unit
    def test_lookup_index_present(self) -> None:
        idx_names = {ix.name for ix in cast(Table, ProviderPricing.__table__).indexes}
        assert "ix_providerpricing_lookup" in idx_names

    @pytest.mark.unit
    def test_open_partial_unique_index_present(self) -> None:
        """Partial-unique index on open-ended (effective_until IS NULL) rows."""
        ix = next(
            (
                i
                for i in cast(Table, ProviderPricing.__table__).indexes
                if i.name == "ix_providerpricing_open"
            ),
            None,
        )
        assert ix is not None, "ix_providerpricing_open must exist"
        assert ix.unique is True
        # postgresql_where dialect kwarg is stored on the index.
        where_clause = ix.dialect_options.get("postgresql", {}).get("where")
        assert where_clause is not None


# ---------------------------------------------------------------------------
# CostEvent additions
# ---------------------------------------------------------------------------


class TestCostEventSchema:
    @pytest.mark.unit
    def test_cost_usd_hint_added(self) -> None:
        col = CostEvent.__table__.columns["cost_usd_hint"]
        assert col.nullable is True
        ctype = cast(Numeric, col.type)
        assert ctype.precision == 20
        assert ctype.scale == 8

    @pytest.mark.unit
    def test_pricing_id_added_with_fk(self) -> None:
        col = CostEvent.__table__.columns["pricing_id"]
        assert col.nullable is True
        fks = list(col.foreign_keys)
        assert len(fks) == 1
        fk = fks[0]
        assert fk.column.table.name == "provider_pricing"
        assert fk.ondelete == "SET NULL"

    @pytest.mark.unit
    def test_cost_usd_nullable_and_widened(self) -> None:
        """cost_usd is nullable (pricing-miss events persist with NULL
        authoritative cost; the hint is preserved on cost_usd_hint) and widened
        to Numeric(20, 8) — the old Numeric(12, 8) capped a single event at ~$10k
        and overflowed on INSERT, aborting the cost_flusher batch. Also carries
        NO default (0 would mask a miss as priced-at-$0)."""
        col = CostEvent.__table__.columns["cost_usd"]
        assert col.nullable is True
        assert col.default is None  # dropped default=0 — NULL is the miss sentinel
        ctype = cast(Numeric, col.type)
        assert ctype.precision == 20
        assert ctype.scale == 8

    @pytest.mark.unit
    def test_served_model_added(self) -> None:
        col = CostEvent.__table__.columns["served_model"]
        assert col.nullable is True
        assert cast(String, col.type).length == 128

    @pytest.mark.unit
    def test_cache_token_columns_added(self) -> None:
        for name in ("cache_creation_input_tokens", "cache_read_input_tokens"):
            col = CostEvent.__table__.columns[name]
            assert col.nullable is False
            # default 0 (NUMERIC default expressed as Decimal/int via SQLAlchemy)
            assert col.default is not None or col.server_default is not None

    @pytest.mark.unit
    def test_provider_request_id_and_usage_extra_added(self) -> None:
        prid = CostEvent.__table__.columns["provider_request_id"]
        assert prid.nullable is True
        assert cast(String, prid.type).length == 255
        ux = CostEvent.__table__.columns["usage_extra"]
        assert ux.nullable is True

    @pytest.mark.unit
    def test_provider_pricing_cache_columns_added(self) -> None:
        from models.db import ProviderPricing

        for name in ("cache_creation_per_1m_usd", "cache_read_per_1m_usd"):
            col = ProviderPricing.__table__.columns[name]
            assert col.nullable is True


# ---------------------------------------------------------------------------
# Engine reflection — schema actually applies cleanly to a fresh DB
# ---------------------------------------------------------------------------


class TestSchemaAppliesToPostgres:
    @pytest.mark.unit
    async def test_audit_log_table_exists_in_db(self, db_engine) -> None:
        async with db_engine.connect() as conn:
            tables = await conn.run_sync(lambda c: inspect(c).get_table_names())
        assert "audit_log" in tables
        assert "provider_pricing" in tables

    @pytest.mark.unit
    async def test_token_budget_columns_in_db(self, db_engine) -> None:
        async with db_engine.connect() as conn:
            cols = await conn.run_sync(lambda c: inspect(c).get_columns("token_budgets"))
        names = {c["name"] for c in cols}
        for need in ("priority", "starts_at", "expires_at", "consumed_tokens", "source"):
            assert need in names

    @pytest.mark.unit
    async def test_no_uq_token_budget_in_db(self, db_engine) -> None:
        async with db_engine.connect() as conn:
            uq = await conn.run_sync(lambda c: inspect(c).get_unique_constraints("token_budgets"))
        names = {u["name"] for u in uq}
        assert "uq_token_budget" not in names
