-- Partial index fetching the latest cross-session summaries by occurrence time descending.
CREATE INDEX IF NOT EXISTS idx_mcs_cross_session_time
ON memory_chat_summary (occurred_at DESC, id DESC)
WHERE summarized AND kind = 'chat_summary' AND NOT archived;
