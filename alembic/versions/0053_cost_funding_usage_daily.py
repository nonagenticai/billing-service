"""billing_cost_events.funding + the cross rollup billing_cost_usage_daily, both backfilled.

WHY
---
Owner decisions D1/D2 (2026-10-07): usage must be shown per project and per user,
split by WHO FUNDS it -- local GPU / platform-paid external vendor / tenant BYO.

* ``funding`` was only implied (``usage_extra.byo`` / ``owner_provided`` plus the
  provider), and no rollup carried project AND actor together: ``billing_cost_daily``
  has project but no actor, ``billing_cost_actor_daily`` actor but no project.
* ``billing_cost_usage_daily`` is ONE table at the grain
  (day, tenant, project, actor, app, funding, provider, served_model), maintained by
  ``services.rollup_engine.rollup_daily`` beside the two existing cost rollups and
  read by ``GET /api/usage/breakdown``.

WHAT THIS DOES
--------------
1. ``ADD COLUMN funding VARCHAR(32)`` with a CHECK on the three values (NULL allowed:
   an invalid or underivable value is stored NULL and counted).
2. ``CREATE TABLE billing_cost_usage_daily`` + its (tenant_id, bucket_date) index, and
   arms ``billing_truncate_guard_trg`` on it (0043's lesson: a table born after the
   sweep is born unguarded unless its own migration arms it).
3. BACKFILLS ``funding`` on every existing event with the SAME precedence as
   ``services.funding.derive_funding`` (the SQL twin; ``tests/test_funding.py`` runs
   both over the same rows), in keyset batches of ``_BATCH`` on the (id, created_at)
   primary key. Only rows still NULL are touched, so a re-run is a no-op.
4. BACKFILLS ``billing_cost_usage_daily`` from the events for every COMPLETE UTC day,
   with the same filters / bucket expression / sentinels as ``rollup_daily``. Days
   whose raw events are gone (retention) cannot be rebuilt and stay absent.

⚠️ COMPANION EDIT -- ``src/db.py``: statements 1-2 are mirrored BYTE-IDENTICALLY into
``db.ADDITIVE_MIGRATIONS``: the flusher NAMES ``funding`` in every INSERT and the
rollup writes the new table, so a pod booting before this migration must still work.

Revision ID: 0053_cost_funding_usage_daily
Revises: 0052_metering_actor_id
Create Date: 2026-10-07
"""

from __future__ import annotations

from collections.abc import Sequence

import sqlalchemy as sa
from alembic import op

revision: str = "0053_cost_funding_usage_daily"  # <= 32 chars: alembic_version.version_num
down_revision: str | None = "0052_metering_actor_id"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None

_BATCH = 5000

# --- mirrored byte-identically in src/db.py ADDITIVE_MIGRATIONS ---------------
_ADD_FUNDING = (
    "ALTER TABLE billing_cost_events ADD COLUMN IF NOT EXISTS funding VARCHAR(32) "
    "CONSTRAINT ck_cost_events_funding "
    "CHECK (funding IN ('local_gpu', 'platform_external', 'tenant_byo'))"
)
_CREATE_USAGE_DAILY = (
    "CREATE TABLE IF NOT EXISTS billing_cost_usage_daily ("
    "id UUID PRIMARY KEY, "
    "bucket_date DATE NOT NULL, "
    "tenant_id UUID NOT NULL, "
    "project_id UUID NOT NULL, "
    "actor_id UUID NOT NULL, "
    "app_id UUID NOT NULL, "
    "funding VARCHAR(32) NOT NULL, "
    "provider VARCHAR(64) NOT NULL, "
    "served_model VARCHAR(128) NOT NULL, "
    "event_count INTEGER NOT NULL, "
    "total_input_tokens BIGINT NOT NULL, "
    "total_output_tokens BIGINT NOT NULL, "
    "total_cost_usd NUMERIC(20, 6) NOT NULL, "
    "CONSTRAINT uq_cost_usage_daily UNIQUE (bucket_date, tenant_id, project_id, actor_id, "
    "app_id, funding, provider, served_model))"
)
_CREATE_USAGE_INDEX = (
    "CREATE INDEX IF NOT EXISTS ix_cost_usage_daily_tenant_date "
    "ON billing_cost_usage_daily (tenant_id, bucket_date)"
)
# -----------------------------------------------------------------------------

_ARM_TRUNCATE_GUARD = r"""
DO $do$
BEGIN
    IF to_regprocedure('public.billing_forbid_truncate()') IS NOT NULL
       AND NOT EXISTS (
           SELECT 1 FROM pg_trigger
            WHERE tgrelid = to_regclass('public.billing_cost_usage_daily')
              AND tgname = 'billing_truncate_guard_trg') THEN
        EXECUTE 'CREATE TRIGGER billing_truncate_guard_trg BEFORE TRUNCATE ON '
                'public.billing_cost_usage_daily FOR EACH STATEMENT '
                'EXECUTE FUNCTION public.billing_forbid_truncate()';
    END IF;
END
$do$;
"""

# The SQL twin of services.funding.derive_funding. Provider lists MUST match
# services.funding.LOCAL_PROVIDERS / EXTERNAL_PROVIDERS (tests/test_funding.py
# asserts it). Rule 3 reads the joined pricing row's cost_basis only: for a
# historical row the stored e.cost_basis cannot be told apart from the
# pricing-miss backstop, which is not evidence of anything.
FUNDING_CASE_SQL = """
CASE
    WHEN lower(e.usage_extra->>'byo') = 'true' THEN 'tenant_byo'
    WHEN lower(e.usage_extra->>'owner_provided') = 'true' THEN 'platform_external'
    WHEN p.cost_basis = 'cash' THEN 'platform_external'
    WHEN p.cost_basis = 'internal_transfer' THEN 'local_gpu'
    WHEN p.cost_model = 'self_hosted_amortized' THEN 'local_gpu'
    WHEN lower(e.provider) IN (
        'pgx-local', 'vllm', 'vlm_local', 'mlx_inference', 'gemma', 'kokoro', 'kitten',
        'mflux', 'webscraper', 'kag', 'kag-graph', 'karma', 'hirag', 'html-render',
        'video-analyzer', 'video-compiler'
    ) OR lower(e.provider) LIKE 'mlx%' THEN 'local_gpu'
    WHEN lower(e.provider) IN (
        'anthropic', 'claude', 'openai', 'google', 'gemini', 'cohere', 'deepgram',
        'deepseek', 'elevenlabs', 'groq', 'xai', 'mistral', 'openrouter', 'together',
        'fireworks', 'perplexity'
    ) THEN 'platform_external'
    ELSE NULL
END
"""

_UPDATE_RANGE = f"""
UPDATE billing_cost_events AS t
   SET funding = sub.funding
  FROM (
    SELECT e.id, e.created_at, {FUNDING_CASE_SQL} AS funding
      FROM billing_cost_events AS e
      LEFT JOIN provider_pricing AS p ON p.id = e.pricing_id
     WHERE e.funding IS NULL
       AND (e.id, e.created_at) > (CAST(:lo_id AS uuid), CAST(:lo_ca AS timestamptz))
       AND (e.id, e.created_at) <= (CAST(:hi_id AS uuid), CAST(:hi_ca AS timestamptz))
  ) AS sub
 WHERE t.id = sub.id AND t.created_at = sub.created_at AND sub.funding IS NOT NULL
"""

_NEXT_HI = """
SELECT id, created_at FROM billing_cost_events
 WHERE (id, created_at) > (CAST(:lo_id AS uuid), CAST(:lo_ca AS timestamptz))
 ORDER BY id, created_at
 OFFSET :off LIMIT 1
"""

_MAX_KEY = (
    "SELECT id, created_at FROM billing_cost_events ORDER BY id DESC, created_at DESC LIMIT 1"
)

#: Lower bound below every real key: the nil uuid at -infinity.
_LO0 = ("00000000-0000-0000-0000-000000000000", "-infinity")

# Same filters, bucket expression and sentinels as rollup_engine.rollup_daily's
# cross-axis block. Complete UTC days only (rollup_daily never rolls "today").
BACKFILL_USAGE_DAILY_SQL = """
INSERT INTO billing_cost_usage_daily (
    id, bucket_date, tenant_id, project_id, actor_id, app_id, funding, provider,
    served_model, event_count, total_input_tokens, total_output_tokens, total_cost_usd)
SELECT gen_random_uuid(), d.bucket_date, d.tenant_id, d.project_id, d.actor_id, d.app_id,
       d.funding, d.provider, d.served_model, count(*),
       coalesce(sum(d.input_tokens), 0), coalesce(sum(d.output_tokens), 0),
       coalesce(sum(d.cost_usd), 0)
  FROM (
    SELECT CAST(timezone('UTC', coalesce(e.event_ts, e.created_at)) AS date) AS bucket_date,
           e.tenant_id,
           coalesce(nullif(e.project_id, e.tenant_id),
                    '00000000-0000-0000-0000-000000000004'::uuid) AS project_id,
           coalesce(e.actor_id, '00000000-0000-0000-0000-000000000005'::uuid) AS actor_id,
           coalesce(e.app_id, '00000000-0000-0000-0000-000000000001'::uuid) AS app_id,
           coalesce(e.funding, 'unknown') AS funding,
           e.provider,
           coalesce(e.served_model, '__unknown__') AS served_model,
           e.input_tokens, e.output_tokens, e.cost_usd
      FROM billing_cost_events e
     WHERE e.excluded_from_billing IS FALSE
  ) d
 WHERE d.bucket_date < CAST(timezone('UTC', now()) AS date)
 GROUP BY d.bucket_date, d.tenant_id, d.project_id, d.actor_id, d.app_id, d.funding,
          d.provider, d.served_model
ON CONFLICT ON CONSTRAINT uq_cost_usage_daily DO NOTHING
"""


def backfill_funding(bind: sa.engine.Connection, batch: int = _BATCH) -> int:
    """Keyset-batched funding backfill. Returns rows updated. Idempotent."""
    top = bind.execute(sa.text(_MAX_KEY)).first()
    if top is None:
        return 0
    lo = _LO0
    updated = 0
    while True:
        hi_row = bind.execute(
            sa.text(_NEXT_HI), {"lo_id": lo[0], "lo_ca": lo[1], "off": batch - 1}
        ).first()
        hi = (
            (str(hi_row[0]), hi_row[1].isoformat()) if hi_row else (str(top[0]), top[1].isoformat())
        )
        res = bind.execute(
            sa.text(_UPDATE_RANGE),
            {"lo_id": lo[0], "lo_ca": lo[1], "hi_id": hi[0], "hi_ca": hi[1]},
        )
        updated += int(res.rowcount or 0)
        if hi_row is None:
            return updated
        lo = hi


def upgrade() -> None:
    op.execute("SET LOCAL lock_timeout = '10s'")
    op.execute(_ADD_FUNDING)
    op.execute(_CREATE_USAGE_DAILY)
    op.execute(_CREATE_USAGE_INDEX)
    op.execute(_ARM_TRUNCATE_GUARD)
    bind = op.get_bind()
    backfill_funding(bind)
    bind.execute(sa.text(BACKFILL_USAGE_DAILY_SQL))


def downgrade() -> None:
    # Lossy as data: the cross axis exists nowhere else once raw events age out.
    op.execute("DROP TABLE IF EXISTS billing_cost_usage_daily")
    op.execute("ALTER TABLE billing_cost_events DROP COLUMN IF EXISTS funding")
