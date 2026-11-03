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

-- Deliberately no ADD COLUMN IF NOT EXISTS: that syntax requires SQLite
-- 3.35+, and the sqlite bundled with macOS (developer environments) is
-- older and fails with a syntax error. Re-execution protection is handled
-- by apply_migrations transaction atomicity (script statements and the
-- version record share one transaction; an interrupted run rolls back as a
-- whole and never leaves a half-applied state; manually wiping version
-- records is self-inflicted ops damage, outside this protection).
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
