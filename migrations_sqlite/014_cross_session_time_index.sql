-- Partial index fetching the latest cross-session summaries by occurrence
-- time descending (isomorphic to the PG-side 014; SQLite supports partial
-- indexes, booleans are 0/1 integers, truthiness matches PG semantics).
CREATE INDEX IF NOT EXISTS idx_mcs_cross_session_time
ON memory_chat_summary (occurred_at DESC, id DESC)
WHERE summarized AND kind = 'chat_summary' AND NOT archived;
