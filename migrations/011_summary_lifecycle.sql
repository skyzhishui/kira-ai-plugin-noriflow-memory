-- 011_summary_lifecycle.sql: summary memory lifecycle (archive state + access reinforcement counts).
--
-- Background: the summary table previously only grew — recall time decay
-- only re-ranked without filtering, so rows stayed in the candidate pool
-- forever; aged low-value rows kept diluting BM25/vector candidates and
-- inflating the search scan set. This migration introduces three groups of
-- state columns (semantics aligned with the access metadata of the iris
-- forgetting algorithm + the TTL/access reinforcement model of livingmemory
-- memory atoms):
-- - archived/archived_at: archive state. Archived rows structurally exit
--   recall (the search SQL unconditionally excludes archived); the row body
--   is kept, still readable by id, restorable via WebUI — archiving is a
--   state transition, not deletion;
-- - last_recall_at: access reinforcement timestamp. Refreshed whenever a row
--   hits the final recall injection set (life extension: the archive pass
--   exempts rows reinforced within the window);
-- - recall_count: cumulative recall hits (hot-memory signal, for WebUI
--   observability; not consumed by the archive predicate as of v0.10.6,
--   kept as input for later frequency-aware eviction).
--
-- The archive predicate runs in the merge agent archive pass (kv-gated
-- periodically, summary_lifecycle_* config group): written_at older than
-- the archive deadline AND last recall earlier than the reinforcement
-- window. Legacy rows have last_recall_at NULL (treated as never
-- reinforced, judged by written_at).

ALTER TABLE memory_chat_summary
    ADD COLUMN IF NOT EXISTS archived BOOLEAN NOT NULL DEFAULT false;
ALTER TABLE memory_chat_summary
    ADD COLUMN IF NOT EXISTS archived_at TIMESTAMPTZ;
ALTER TABLE memory_chat_summary
    ADD COLUMN IF NOT EXISTS last_recall_at TIMESTAMPTZ;
ALTER TABLE memory_chat_summary
    ADD COLUMN IF NOT EXISTS recall_count INT NOT NULL DEFAULT 0;

-- Archive-pass scan index: non-archived rows scanned by written_at ascending;
-- the scan set keeps shrinking as the archived ratio rises (the partial
-- index only contains active rows).
CREATE INDEX IF NOT EXISTS idx_mcs_lifecycle
    ON memory_chat_summary (written_at)
    WHERE NOT archived;
