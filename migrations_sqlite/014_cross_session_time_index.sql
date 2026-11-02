-- 跨会话摘要按发生时间倒序取最近条目的部分索引（与 PG 侧 014 同构；
-- SQLite 支持部分索引，布尔列为 0/1 整型，真值判定与 PG 语义一致）。
CREATE INDEX IF NOT EXISTS idx_mcs_cross_session_time
ON memory_chat_summary (occurred_at DESC, id DESC)
WHERE summarized AND kind = 'chat_summary' AND NOT archived;
