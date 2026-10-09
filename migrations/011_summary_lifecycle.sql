-- 011_summary_lifecycle.sql：摘要记忆生命周期（归档态 + 访问强化计数）。
--
-- 背景：摘要表此前只进不出——recall 时间衰减仅改排序不过滤，行永久留在
-- 候选池里，陈年低价值行持续稀释 BM25/向量候选并推高检索扫描集。
-- 本迁移引入三组状态列（语义对齐 iris 遗忘算法的 access 元数据 +
-- livingmemory 记忆原子的 TTL/访问强化模型）：
-- - archived/archived_at：归档态。archived 行结构性退出召回（检索 SQL
--   无条件 NOT archived），原文保留、按 id 仍可读、WebUI 可恢复——
--   归档是状态转移不是删除；
-- - last_recall_at：访问强化时间戳。召回最终注入集命中即刷新（续命：
--   归档遍对强化窗口内的行豁免）；
-- - recall_count：累计被召回次数（热记忆信号，WebUI 观测用；v0.10.6
--   归档判据不消费，留作后续频次感知淘汰的输入）。
--
-- 归档判据由 merge agent 归档遍执行（kv 周期门控，summary_lifecycle_*
-- 配置组）：written_at 距今超过归档时限 且 最近召回早于强化窗口。
-- 存量行 last_recall_at 为 NULL（视作从未被强化，按 written_at 判定）。

ALTER TABLE memory_chat_summary
    ADD COLUMN IF NOT EXISTS archived BOOLEAN NOT NULL DEFAULT false;
ALTER TABLE memory_chat_summary
    ADD COLUMN IF NOT EXISTS archived_at TIMESTAMPTZ;
ALTER TABLE memory_chat_summary
    ADD COLUMN IF NOT EXISTS last_recall_at TIMESTAMPTZ;
ALTER TABLE memory_chat_summary
    ADD COLUMN IF NOT EXISTS recall_count INT NOT NULL DEFAULT 0;

-- 归档遍扫描索引：未归档行按 written_at 升序扫描，归档比例升高后
-- 扫描集持续收敛（partial index 只含活跃行）。
CREATE INDEX IF NOT EXISTS idx_mcs_lifecycle
    ON memory_chat_summary (written_at)
    WHERE NOT archived;
