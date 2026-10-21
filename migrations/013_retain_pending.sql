CREATE TABLE IF NOT EXISTS memory_retain_pending (
    session_key TEXT PRIMARY KEY,
    payload JSONB NOT NULL,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
);
CREATE INDEX IF NOT EXISTS idx_retain_pending_updated
    ON memory_retain_pending (updated_at);
