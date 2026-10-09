-- 011_summary_lifecycle.sql: summary lifecycle (archive state + access
-- reinforcement). Mirrors migrations/011_summary_lifecycle.sql (PG) with
-- SQLite types: timestamps are TEXT in the canonical boundary format
-- (sqlite_format_ts), booleans are INTEGER 0/1.
--
-- Semantics (aligned with the PG migration):
-- - archived/archived_at: archived rows structurally exit recall (search
--   SQL adds NOT archived); the row body is kept, readable by id, and
--   restorable via the maintenance page / memory_correct(reactivate).
-- - last_recall_at: access-reinforcement timestamp refreshed on recall
--   hits (archive pass exempts rows reinforced within the window).
-- - recall_count: cumulative recall hits (hot-memory signal, WebUI
--   observability; the archive predicate does not consume it yet).

-- 刻意不加 ADD COLUMN IF NOT EXISTS：该语法需 SQLite 3.35+，而本地
-- macOS 自带 sqlite（开发者环境）低于此版本会直接语法报错。重复执行
-- 防护由 apply_migrations 的事务原子性承担（脚本语句与版本记录同事务，
-- 异常中断整体回滚不产生半应用状态；手工清版本记录属运维自伤，不在
-- 防护范围）。
ALTER TABLE memory_chat_summary
    ADD COLUMN archived INTEGER NOT NULL DEFAULT 0;
ALTER TABLE memory_chat_summary
    ADD COLUMN archived_at TEXT;
ALTER TABLE memory_chat_summary
    ADD COLUMN last_recall_at TEXT;
ALTER TABLE memory_chat_summary
    ADD COLUMN recall_count INTEGER NOT NULL DEFAULT 0;

-- Archive-pass scan index: unb archived rows scanned by written_at
-- ascending; the scan set keeps shrinking as the archived ratio rises
-- (partial index only contains active rows).
CREATE INDEX IF NOT EXISTS idx_mcs_lifecycle
    ON memory_chat_summary (written_at) WHERE NOT archived;
