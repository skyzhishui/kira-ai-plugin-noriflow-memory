-- 007_backfill_indexes.sql
-- Companion partial indexes for the backfill/pending scans: the periodic
-- scans over embedding IS NULL on three tables and over pending raw
-- extraction (vector_ops.EmbeddingBackfillTask / merge_agent normalization
-- pass) previously had no index support; once the tables grew large, every
-- cycle became a full table scan + sequential sort. Partial indexes only
-- cover rows pending backfill/extraction — after backfill completes the
-- indexes trend toward an empty set, so write overhead is negligible.

-- Summary table embedding backfill scan (WHERE embedding IS NULL ... ORDER BY id)
CREATE INDEX IF NOT EXISTS idx_mcs_backfill
    ON memory_chat_summary (id) WHERE embedding IS NULL;

-- Raw fact table embedding backfill scan (same shape)
CREATE INDEX IF NOT EXISTS idx_mpfr_backfill
    ON memory_persona_fact_raw (id) WHERE embedding IS NULL;

-- Cluster table embedding backfill scan (same shape)
CREATE INDEX IF NOT EXISTS idx_mfc_backfill
    ON memory_fact_cluster (id) WHERE embedding IS NULL;

-- Normalization-pass pending extraction (WHERE extracted_flag = 0 ORDER BY id):
-- the existing idx_mpfr_pending lacks id ordering; an added (id)-keyed
-- partial index removes the periodic sort
CREATE INDEX IF NOT EXISTS idx_mpfr_pending_id
    ON memory_persona_fact_raw (id) WHERE extracted_flag = 0;
