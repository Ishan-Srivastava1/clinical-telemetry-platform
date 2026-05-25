-- =============================================================================
-- Clinical Telemetry — TimescaleDB schema bootstrap
-- =============================================================================

CREATE EXTENSION IF NOT EXISTS timescaledb;

-- -----------------------------------------------------------------------------
-- Raw / filtered telemetry samples
-- -----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS telemetry (
    time         TIMESTAMPTZ      NOT NULL,
    patient_id   TEXT             NOT NULL,
    metric       TEXT             NOT NULL,
    value        DOUBLE PRECISION NOT NULL,
    device_id    TEXT             NOT NULL,
    z_score      DOUBLE PRECISION,
    window_mean  DOUBLE PRECISION,
    window_std   DOUBLE PRECISION
);

-- Convert into a hypertable with both time and patient_id partitioning.
SELECT create_hypertable(
    'telemetry',
    'time',
    partitioning_column => 'patient_id',
    number_partitions   => 8,
    chunk_time_interval => INTERVAL '1 day',
    if_not_exists       => TRUE
);

CREATE INDEX IF NOT EXISTS idx_tel_patient_time
    ON telemetry (patient_id, time DESC);

CREATE INDEX IF NOT EXISTS idx_tel_metric_time
    ON telemetry (metric, time DESC);

-- -----------------------------------------------------------------------------
-- Native columnar compression
-- -----------------------------------------------------------------------------
ALTER TABLE telemetry SET (
    timescaledb.compress,
    timescaledb.compress_segmentby = 'patient_id, metric',
    timescaledb.compress_orderby   = 'time DESC'
);

-- Aggressive policy for local dev so you can SEE compression happen
SELECT add_compression_policy('telemetry', INTERVAL '1 hour', if_not_exists => TRUE);

-- -----------------------------------------------------------------------------
-- Continuous aggregate: 1-minute rollups for dashboards
-- -----------------------------------------------------------------------------
CREATE MATERIALIZED VIEW IF NOT EXISTS telemetry_1min
WITH (timescaledb.continuous) AS
SELECT
    time_bucket('1 minute', time) AS bucket,
    patient_id,
    metric,
    AVG(value)        AS avg_value,
    MIN(value)        AS min_value,
    MAX(value)        AS max_value,
    COUNT(*)          AS sample_count,
    AVG(window_std)   AS avg_window_std
FROM telemetry
GROUP BY bucket, patient_id, metric
WITH NO DATA;

SELECT add_continuous_aggregate_policy(
    'telemetry_1min',
    start_offset      => INTERVAL '2 hours',
    end_offset        => INTERVAL '30 seconds',
    schedule_interval => INTERVAL '1 minute',
    if_not_exists     => TRUE
);

-- -----------------------------------------------------------------------------
-- Optional alerts table for sustained-deterioration events
-- -----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS alerts (
    alert_id    BIGSERIAL    PRIMARY KEY,
    time        TIMESTAMPTZ  NOT NULL,
    patient_id  TEXT         NOT NULL,
    metric      TEXT         NOT NULL,
    value       DOUBLE PRECISION,
    z_score     DOUBLE PRECISION,
    description TEXT
);

CREATE INDEX IF NOT EXISTS idx_alerts_patient_time
    ON alerts (patient_id, time DESC);

-- Read-only role for Grafana
DO $$
BEGIN
    IF NOT EXISTS (SELECT FROM pg_catalog.pg_roles WHERE rolname = 'grafana_ro') THEN
        CREATE ROLE grafana_ro LOGIN PASSWORD 'grafana_ro_pw';
    END IF;
END
$$;
GRANT CONNECT ON DATABASE clinical TO grafana_ro;
GRANT USAGE ON SCHEMA public TO grafana_ro;
GRANT SELECT ON ALL TABLES IN SCHEMA public TO grafana_ro;
ALTER DEFAULT PRIVILEGES IN SCHEMA public
    GRANT SELECT ON TABLES TO grafana_ro;