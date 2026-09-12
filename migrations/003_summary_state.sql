-- 003_summary_state.sql：摘要表增加编码状态列（retain 编码降级原文不入召回）
--
-- 背景：端侧编码 fail-open 降级时，编码输入原文（带信封格式与历史上下文
-- 标记的对话日志）会作为 content 写入本表。原文行被 recall 召回会污染注入
-- 上下文。本迁移引入 summarized 状态列将"未编码原文"与"编码摘要"区分开：
-- summarized=false 的行不参与召回（检索 SQL 过滤），由合并 agent 补编码遍
-- 重编码（summary UPDATE 原行 + facts 入事实表）后翻回 true，数据不丢。
-- bot_self 行设计上即原文直写（原文即终态），恒为 true，不受影响。

ALTER TABLE memory_chat_summary
    ADD COLUMN IF NOT EXISTS summarized BOOLEAN NOT NULL DEFAULT true;

-- 存量清洗：将已入库的降级原文行置 false（等待补编码遍重编码）。
-- 识别特征二选一：
-- a) 含历史上下文分隔标记行（HISTORY_BATCH_SEPARATOR）；
-- b) 行首信封格式（format_history_message 产出：行首 "<msg ts=" 开头的
--    信封行）——覆盖历史窗口为空、无分隔标记的降级行。正常摘要为
--    自然语言转述，不会出现行首信封模式。
UPDATE memory_chat_summary
SET summarized = false
WHERE content LIKE '%以上为历史上下文%'
   OR content ~ '(^|\n)<msg ts="20[0-9]{2}-[0-9]{2}-[0-9]{2} [0-9]{2}:[0-9]{2}:[0-9]{2}"';

-- 补编码遍扫描索引（未摘要行通常少量，部分索引足够）
CREATE INDEX IF NOT EXISTS idx_mcs_unsummarized
    ON memory_chat_summary (id) WHERE NOT summarized;
