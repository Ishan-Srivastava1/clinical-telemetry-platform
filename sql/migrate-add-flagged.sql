-- ============================================================================
-- Migration: add `flagged` and `reason` columns to the running telemetry table.
-- Idempotent: safe to run more than once.
-- ============================================================================

ALTER TABLE telemetry
    ADD COLUMN IF NOT EXISTS flagged BOOLEAN NOT NULL DEFAULT FALSE,
    ADD COLUMN IF NOT EXISTS reason  TEXT;

CREATE INDEX IF NOT EXISTS idx_tel_flagged_patient_time
    ON telemetry (patient_id, time DESC) WHERE flagged = TRUE;
