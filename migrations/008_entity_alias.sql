-- 008_entity_alias.sql：持久实体别名层（P1，2026-09 关系图谱方案）
-- 名字流（宿主 messages 对账 / 回合批次 upsert）-> 变体拆分后的别名行，
-- 供实体命中匹配（窗口词典之后的持久层）与 recall 实体路 / 画像候选共用。

CREATE TABLE IF NOT EXISTS memory_entity_alias (
    id         BIGSERIAL PRIMARY KEY,
    platform   TEXT NOT NULL DEFAULT '',
    user_id    TEXT NOT NULL,
    name       TEXT NOT NULL,                        -- 变体拆分后的候选名（>=2 词字符）
    first_seen TIMESTAMPTZ NOT NULL DEFAULT now(),   -- 行创建时间（非语义首见时间）
    last_seen  TIMESTAMPTZ NOT NULL DEFAULT now(),   -- 该名字最近被观察到的时间
    source     TEXT NOT NULL DEFAULT 'batch',        -- backfill | reconcile | batch
    UNIQUE (platform, user_id, name)
);
CREATE INDEX IF NOT EXISTS idx_mea_name ON memory_entity_alias (name);
