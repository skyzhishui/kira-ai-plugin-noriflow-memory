CREATE TABLE IF NOT EXISTS memory_retain_pending (
    session_key TEXT PRIMARY KEY,
    payload TEXT NOT NULL,
    updated_at TEXT NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_retain_pending_updated
    ON memory_retain_pending (updated_at);
