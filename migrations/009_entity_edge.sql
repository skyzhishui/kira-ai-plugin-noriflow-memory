-- P2：结构化关系边表（memory_entity_edge）。
-- 写入：编码 pass relations 数组零 LLM 结构键合并（P2 只写不查，
-- 审计期达标后由 P3 注入消费）。
-- 状态机：人-人边插入即 active；bot 端点边插入 pending，
-- evidence_count（含本次证据）达 relation_bot_edge_min_evidence 时由
-- upsert 语句内联 CASE 转 active（零 LLM）；superseded 保留状态位但
-- 不自动置位（label 存在多值语义，零 LLM 无法判互斥——决策 8）。
-- 端点名（subject_name/object_name）：P3 注入陈述行的显示名来源，
-- 随每次证据刷新为最新（完整名字史在 memory_entity_alias）。

CREATE TABLE IF NOT EXISTS memory_entity_edge (
    id             BIGSERIAL PRIMARY KEY,
    platform       TEXT NOT NULL DEFAULT '',
    subject_uid    TEXT NOT NULL,
    object_uid     TEXT NOT NULL,
    subject_name   TEXT NOT NULL DEFAULT '',
    object_name    TEXT NOT NULL DEFAULT '',
    relation_label TEXT NOT NULL,
    statement      TEXT NOT NULL,
    status         TEXT NOT NULL DEFAULT 'active',
    confidence     TEXT NOT NULL DEFAULT 'medium',
    evidence_count INT NOT NULL DEFAULT 1,
    evidence_keys  TEXT[] NOT NULL DEFAULT '{}',
    first_seen     TIMESTAMPTZ NOT NULL DEFAULT now(),
    last_seen      TIMESTAMPTZ NOT NULL DEFAULT now(),
    occurred_at    TIMESTAMPTZ NOT NULL DEFAULT now(),
    written_at     TIMESTAMPTZ NOT NULL DEFAULT now(),
    UNIQUE (platform, subject_uid, object_uid, relation_label)
);

CREATE INDEX IF NOT EXISTS idx_mee_subject
    ON memory_entity_edge (platform, subject_uid, status);
CREATE INDEX IF NOT EXISTS idx_mee_object
    ON memory_entity_edge (platform, object_uid, status);
