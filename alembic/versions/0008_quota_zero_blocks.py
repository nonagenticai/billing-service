"""Relax the request-quota CHECK from ``> 0`` to ``>= 0`` (0 = block-all).

``quota_monthly = 0`` now means **block all requests** — a zero-allowance /
suspended plan — while "unlimited" is expressed by the ABSENCE of a row (a NULL
limit at check time). Before this, ``ck_request_quota_positive CHECK
(quota_monthly > 0)`` made a genuine block-all unstorable, so a 0 could only ever
reach enforcement as a silent cap==0=unlimited signal. Renamed to
``ck_request_quota_non_negative`` so the name no longer claims "positive".

Idempotent / order-independent by construction. The app also runs
``Base.metadata.create_all`` at startup (``src/db.py``), which now emits the
NEW named constraint, so the constraint may already exist (under either name)
when ``alembic upgrade head`` runs in the migrate initContainer. We therefore
``DROP CONSTRAINT IF EXISTS`` for BOTH the old and new names before adding the
new one — a plain ``ADD CONSTRAINT`` (Postgres has no ``ADD ... IF NOT EXISTS``)
then cannot collide with a create_all-produced constraint. Negatives stay
rejected.

Revision ID: 0008_quota_zero_blocks
Revises: 0007_quota_tenant_not_null
Create Date: 2026-07-29

(Revision id kept <=32 chars — ``alembic_version.version_num`` is varchar(32).)
"""

from __future__ import annotations

from alembic import op

revision = "0008_quota_zero_blocks"
down_revision = "0007_quota_tenant_not_null"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.execute(
        "ALTER TABLE request_quota_config DROP CONSTRAINT IF EXISTS ck_request_quota_non_negative"
    )
    op.execute(
        "ALTER TABLE request_quota_config DROP CONSTRAINT IF EXISTS ck_request_quota_positive"
    )
    op.execute(
        "ALTER TABLE request_quota_config "
        "ADD CONSTRAINT ck_request_quota_non_negative CHECK (quota_monthly >= 0)"
    )


def downgrade() -> None:
    # Reverting to ``> 0`` would reject any existing block-all (0) row, so the
    # downgrade drops the non-negative constraint and restores the strict
    # ``> 0`` original ONLY if no 0-rows exist; otherwise it fails loud rather
    # than silently corrupting enforcement by leaving 0-rows unconstrained.
    op.execute(
        "ALTER TABLE request_quota_config DROP CONSTRAINT IF EXISTS ck_request_quota_non_negative"
    )
    op.execute(
        """
        DO $$
        DECLARE
            n bigint;
        BEGIN
            SELECT count(*) INTO n FROM request_quota_config WHERE quota_monthly = 0;
            IF n > 0 THEN
                RAISE EXCEPTION
                    'request_quota_config has % block-all (quota_monthly=0) row(s); '
                    'cannot restore the > 0 CHECK without dropping them — resolve '
                    'first', n;
            END IF;
        END $$
        """
    )
    op.execute(
        "ALTER TABLE request_quota_config "
        "ADD CONSTRAINT ck_request_quota_positive CHECK (quota_monthly > 0)"
    )
