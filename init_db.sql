-- ============================================================
-- streaming_pipeline — Jaya Kotagiri
-- PostgreSQL init script
-- Runs automatically on first docker-compose up
-- ============================================================

CREATE TABLE IF NOT EXISTS file_change_events (
    id              SERIAL PRIMARY KEY,
    event_id        UUID        NOT NULL UNIQUE,   -- idempotency key
    detected_at     TIMESTAMPTZ NOT NULL,
    file_path       TEXT        NOT NULL,
    operation       VARCHAR(10) NOT NULL CHECK (operation IN ('INSERT', 'UPDATE', 'DELETE')),
    line_number     INTEGER     NOT NULL,
    old_value       TEXT,
    new_value       TEXT,
    ingested_at     TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

-- Index for fast lookups by file and time range
CREATE INDEX IF NOT EXISTS idx_fce_file_path    ON file_change_events (file_path);
CREATE INDEX IF NOT EXISTS idx_fce_detected_at  ON file_change_events (detected_at DESC);
CREATE INDEX IF NOT EXISTS idx_fce_operation    ON file_change_events (operation);

COMMENT ON TABLE  file_change_events              IS 'Line-level change events produced by Jaya streaming_pipeline watcher';
COMMENT ON COLUMN file_change_events.event_id     IS 'UUID generated per change; used for idempotent upserts';
COMMENT ON COLUMN file_change_events.operation    IS 'INSERT | UPDATE | DELETE — derived from difflib SequenceMatcher output';
