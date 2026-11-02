-- 015_bm25_materialize.sql: materialize the BM25 token column (recall perf P1).
-- (Numbering stays aligned with the nori-core side: 014 is reserved for a
-- future port of nori's 014_cross_session_time_index, so this lands as 015.)
--
-- Problem (measured 2026-11-02 on a 13,515-row corpus): the BM25 leg calls
-- to_tsvector('simple', search_text) twice, once for filtering and once for
-- ordering. The filter can use an expression GIN index (idx_mcs_fts), but
-- ORDER BY ts_rank(to_tsvector(...)) re-tokenizes ~400 characters of body
-- text for every matching row (thousands of rows at 35-69% selectivity):
-- ~160ms for that leg alone, growing linearly with table size. Under rank
-- cost the planner also abandons the GIN index and picks a Parallel Seq
-- Scan. Split measurement: GIN filter alone 24ms, 2.4ms with pre-truncated
-- tokens -- ~135ms was repeated tokenization.
--
-- Fix: a stored generated column (tsv) carries the tokenized form. The
-- two-argument to_tsvector(regconfig, text) is immutable and therefore valid
-- in a generated column (PG12+; this deployment is PG13+ already required by
-- HNSW). Both filtering and ts_rank read the column; the expression GIN
-- index idx_mcs_fts retires and GIN(tsv) takes over. The retrieval SQL
-- switches to the tsv column in the same release (db/postgres.py).
--
-- Rollback compatibility: old code (expression form) still executes without
-- idx_mcs_fts (degrades to a Seq Scan -- slower but correct).
ALTER TABLE memory_chat_summary
    ADD COLUMN IF NOT EXISTS tsv tsvector
    GENERATED ALWAYS AS (to_tsvector('simple', search_text)) STORED;
DROP INDEX IF EXISTS idx_mcs_fts;
CREATE INDEX IF NOT EXISTS idx_mcs_tsv
    ON memory_chat_summary USING GIN (tsv);
ANALYZE memory_chat_summary;
