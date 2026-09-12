-- 010_review_fixes.sql: 2026-09-11 full review fix batch.
-- P2-11: partial index for the periodic search_text backfill scan. The
-- summary table is the largest table; after the one-off backfill drains,
-- the per-cycle "WHERE search_text IS NULL" scan must read zero rows
-- without a sequential scan (write paths always fill search_text, so the
-- NULL set only shrinks — a partial index has zero maintenance cost).
CREATE INDEX IF NOT EXISTS idx_mcs_search_text_null
    ON memory_chat_summary (id) WHERE search_text IS NULL;

-- P3-i: idx_mea_name (b-tree on name) supports no query path — pure write
-- amplification. Replace it with an owner+recency index that serves the
-- canonical-name lookups (fetch_alias_names_by_owner owner filter).
DROP INDEX IF EXISTS idx_mea_name;
CREATE INDEX IF NOT EXISTS idx_mea_owner_seen
    ON memory_entity_alias (platform, user_id, last_seen DESC);

-- P3-q / P3-t: edge tombstone traceability and a version column for the
-- optimistic supersede lock (manual edits and evidence refreshes bump
-- updated_at; the audit pass only tombstones edges whose updated_at still
-- matches its snapshot).
ALTER TABLE memory_entity_edge
    ADD COLUMN IF NOT EXISTS supersede_reason TEXT NOT NULL DEFAULT '',
    ADD COLUMN IF NOT EXISTS superseded_at TIMESTAMPTZ,
    ADD COLUMN IF NOT EXISTS updated_at TIMESTAMPTZ NOT NULL DEFAULT now();
