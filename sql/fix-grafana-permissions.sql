-- =============================================================================
-- One-shot fix: grant grafana_ro everything it needs to read the hypertable.
--
-- Why this is needed:
--   docker-entrypoint-initdb.d/ scripts run ONLY when the Postgres data volume
--   is first created. If you've been running this project across rebuilds, the
--   `timescale_data` named volume predates the grafana_ro grants in init.sql,
--   so the role either doesn't exist or has no read permissions.
--
-- TimescaleDB wrinkle:
--   `telemetry` is a hypertable whose chunks live in `_timescaledb_internal`.
--   A plain `GRANT SELECT ON public.telemetry` is not enough — the reader also
--   needs USAGE on `_timescaledb_internal` and SELECT on every chunk, plus
--   default privileges so chunks created in the future are auto-granted.
--
-- Run with:
--   docker exec -i timescaledb psql -U telemetry -d clinical < sql/fix-grafana-permissions.sql
-- =============================================================================

-- 1. Make sure the role exists (idempotent).
DO $$
BEGIN
    IF NOT EXISTS (SELECT FROM pg_catalog.pg_roles WHERE rolname = 'grafana_ro') THEN
        CREATE ROLE grafana_ro LOGIN PASSWORD 'grafana_ro_pw';
    ELSE
        ALTER ROLE grafana_ro WITH LOGIN PASSWORD 'grafana_ro_pw';
    END IF;
END
$$;

-- 2. Database-level + public schema access.
GRANT CONNECT ON DATABASE clinical TO grafana_ro;
GRANT USAGE ON SCHEMA public TO grafana_ro;
GRANT SELECT ON ALL TABLES    IN SCHEMA public TO grafana_ro;
GRANT SELECT ON ALL SEQUENCES IN SCHEMA public TO grafana_ro;

-- 3. Default privileges so future tables in `public` are auto-granted.
ALTER DEFAULT PRIVILEGES IN SCHEMA public
    GRANT SELECT ON TABLES    TO grafana_ro;
ALTER DEFAULT PRIVILEGES IN SCHEMA public
    GRANT SELECT ON SEQUENCES TO grafana_ro;

-- 4. THIS IS THE KEY MISSING PIECE — TimescaleDB chunks live here.
GRANT USAGE ON SCHEMA _timescaledb_internal TO grafana_ro;
GRANT SELECT ON ALL TABLES IN SCHEMA _timescaledb_internal TO grafana_ro;

-- 5. Default privileges so NEW chunks (added every day as the hypertable grows)
--    are auto-granted to grafana_ro. Without this, tomorrow's chunks would be
--    unreadable and the dashboard would silently break again.
ALTER DEFAULT PRIVILEGES IN SCHEMA _timescaledb_internal
    GRANT SELECT ON TABLES TO grafana_ro;

-- 6. Continuous aggregate (telemetry_1min) lives in a materialised view —
--    grant explicitly.
DO $$
BEGIN
    IF EXISTS (SELECT 1 FROM pg_matviews WHERE matviewname = 'telemetry_1min') THEN
        EXECUTE 'GRANT SELECT ON public.telemetry_1min TO grafana_ro';
    END IF;
END
$$;

-- 7. Verification — should print > 0 rows. If this returns 0, the worker
--    isn't currently writing.
SELECT
    'rows in last 5 min as grafana_ro' AS check_name,
    COUNT(*) AS row_count
FROM telemetry
WHERE time > NOW() - INTERVAL '5 minutes';
