-- pg_cron schedules for the continuous-improvement substrate.
--
-- Runs from the `postgres` DB (cron.database_name) and uses
-- `cron.schedule_in_database` to dispatch the rollup CALLs into `metering`
-- where the procedures live. No Postgres restart needed: pg_cron is already
-- in shared_preload_libraries on the Postgres host.
--
-- Prereq: alembic upgrade head against the `metering` DB so
-- sp_rollup_asset_daily_stats_today and sp_compute_asset_baselines exist
-- (migrations 0001-0005).
--
-- Usage — run on the Postgres host, as a role allowed to schedule pg_cron jobs.
-- Credentials come from your own SSH configuration; nothing about the host is
-- needed in this file, and none may be added: this repository is public.
--     ssh <postgres-host> "sudo -u postgres psql -d postgres -f -" \
--       < scripts/install_pg_cron.sql
--
-- Per project memory `cron-metric-convention` we also emit a freshness gauge
-- so alerts can fire when these jobs stop succeeding. The gauge table lives
-- in `postgres` alongside cron.job_run_details so the emit job needs no
-- cross-DB hop; alerting reads MAX(occurred_at) per job_name from there.

-- Hourly rollup of asset_daily_stats from the local exec_quality table.
SELECT cron.schedule_in_database(
    'rollup_asset_daily_stats',
    '0 * * * *',
    $$ CALL sp_rollup_asset_daily_stats_today(); $$,
    'metering'
);

-- Nightly recompute of 30-day baselines.
SELECT cron.schedule_in_database(
    'compute_asset_baselines',
    '15 2 * * *',
    $$ CALL sp_compute_asset_baselines(); $$,
    'metering'
);

-- Cron-metric freshness gauge per project memory (cron-metric-convention).
CREATE TABLE IF NOT EXISTS cron_last_success_event (
    job_name     TEXT PRIMARY KEY,
    occurred_at  TIMESTAMPTZ
);

SELECT cron.schedule(
    'emit_cron_metric_rollups',
    '*/15 * * * *',
    $$
    INSERT INTO cron_last_success_event (job_name, occurred_at)
      VALUES ('rollup_asset_daily_stats',
              (SELECT max(jrd.end_time)
                 FROM cron.job_run_details jrd
                 JOIN cron.job j ON j.jobid = jrd.jobid
                WHERE j.jobname='rollup_asset_daily_stats' AND jrd.status='succeeded'))
    ON CONFLICT (job_name) DO UPDATE SET occurred_at = EXCLUDED.occurred_at;
    INSERT INTO cron_last_success_event (job_name, occurred_at)
      VALUES ('compute_asset_baselines',
              (SELECT max(jrd.end_time)
                 FROM cron.job_run_details jrd
                 JOIN cron.job j ON j.jobid = jrd.jobid
                WHERE j.jobname='compute_asset_baselines' AND jrd.status='succeeded'))
    ON CONFLICT (job_name) DO UPDATE SET occurred_at = EXCLUDED.occurred_at;
    $$
);
