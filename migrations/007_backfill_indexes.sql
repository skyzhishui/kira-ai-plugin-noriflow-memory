-- 007_backfill_indexes.sql
-- 补算/待处理扫描的配套部分索引：三表 embedding IS NULL 与 raw 待处理
-- 抽取的周期扫描（vector_ops.EmbeddingBackfillTask / merge_agent 归一化遍）
-- 此前无任何索引支撑，存量表大后每周期全表扫 + 顺序排序。
-- 部分索引只覆盖待补算/待处理行——回填完成后索引趋近空集，写入开销可忽略。

-- 摘要表向量补算扫描（WHERE embedding IS NULL ... ORDER BY id）
CREATE INDEX IF NOT EXISTS idx_mcs_backfill
    ON memory_chat_summary (id) WHERE embedding IS NULL;

-- 事实原始表向量补算扫描（同型）
CREATE INDEX IF NOT EXISTS idx_mpfr_backfill
    ON memory_persona_fact_raw (id) WHERE embedding IS NULL;

-- 簇表向量补算扫描（同型）
CREATE INDEX IF NOT EXISTS idx_mfc_backfill
    ON memory_fact_cluster (id) WHERE embedding IS NULL;

-- 归一化遍待处理抽取（WHERE extracted_flag = 0 ORDER BY id）：现有
-- idx_mpfr_pending 不含 id 排序，补 (id) 键的部分索引消除周期性 sort
CREATE INDEX IF NOT EXISTS idx_mpfr_pending_id
    ON memory_persona_fact_raw (id) WHERE extracted_flag = 0;
