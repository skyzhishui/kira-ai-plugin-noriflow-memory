-- 混合检索（方案 A）：写入侧 bigram 预分词列 + 'simple' 全文索引。
-- 检索稀有条目（人名/游戏名/黑话）时与向量路 RRF 融合（真实语料基准：
-- bigram tsvector 稀有探针覆盖率 100% / 0.6ms，pg_trgm word_similarity 仅 9.3%）。
-- 存量行 search_text 为 NULL：不影响向量路（两路 WHERE 同源，NULL 行仅不进 BM25 路），
-- 由补算任务新增的 search_text 回填遍周期补齐。
ALTER TABLE memory_chat_summary ADD COLUMN IF NOT EXISTS search_text TEXT;
CREATE INDEX IF NOT EXISTS idx_mcs_fts ON memory_chat_summary
    USING GIN (to_tsvector('simple', search_text));
