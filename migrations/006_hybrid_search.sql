-- Hybrid search (plan A): write-side bigram pre-tokenized column + 'simple' full-text index.
-- When retrieving rare entries (person names / game names / slang), fused
-- with the vector leg via RRF (real-corpus benchmark: bigram tsvector
-- rare-probe coverage 100% / 0.6ms vs pg_trgm word_similarity only 9.3%).
-- Legacy rows have search_text NULL: no impact on the vector leg (both legs
-- share the same WHERE; NULL rows only stay out of the BM25 leg), filled in
-- periodically by the search_text backfill pass added with the backfill task.
ALTER TABLE memory_chat_summary ADD COLUMN IF NOT EXISTS search_text TEXT;
CREATE INDEX IF NOT EXISTS idx_mcs_fts ON memory_chat_summary
    USING GIN (to_tsvector('simple', search_text));
