-- 001_init.sql：本地记忆库初始 schema（4 张表）
-- 幂等：全部 IF NOT EXISTS；由插件 db.py 迁移执行器在事务内应用并登记版本。
-- 向量统一 qwen3-embedding 1024 维、cosine 距离（vector_cosine_ops）、HNSW 索引。

CREATE EXTENSION IF NOT EXISTS vector;

-- 1. 对话摘要表（recall 唯一语料；bot 自触发消息 kind=bot_self）
CREATE TABLE IF NOT EXISTS memory_chat_summary (
    id           BIGSERIAL PRIMARY KEY,
    document_id  TEXT NOT NULL UNIQUE,              -- 幂等键 {session_id}-{md5(content)[:12]}
    kind         TEXT NOT NULL DEFAULT 'chat_summary',  -- chat_summary | bot_self
    platform     TEXT NOT NULL DEFAULT '',
    session_id   TEXT NOT NULL DEFAULT '',
    group_id     TEXT NOT NULL DEFAULT '',
    user_id      TEXT NOT NULL DEFAULT '',          -- trigger 发言者（裸 uid）
    participants TEXT[] DEFAULT '{}',               -- 本轮全部发言者（"platform:uid" 复合键，用户精确召回过滤用）
    content      TEXT NOT NULL,
    occurred_at  TIMESTAMPTZ NOT NULL,              -- 事实发生时间（本轮触发消息时间）
    written_at   TIMESTAMPTZ NOT NULL DEFAULT now(),-- 写入时间
    embedding    vector(1024)
);
CREATE INDEX IF NOT EXISTS idx_mcs_session ON memory_chat_summary (session_id, occurred_at DESC);
CREATE INDEX IF NOT EXISTS idx_mcs_kind    ON memory_chat_summary (kind);
CREATE INDEX IF NOT EXISTS idx_mcs_participants ON memory_chat_summary USING GIN (participants);
CREATE INDEX IF NOT EXISTS idx_mcs_hnsw ON memory_chat_summary
    USING hnsw (embedding vector_cosine_ops) WITH (m = 16, ef_construction = 64);

-- 2. 原始事实表（编码器产出，等待入簇；不参与 recall）
CREATE TABLE IF NOT EXISTS memory_persona_fact_raw (
    id               BIGSERIAL PRIMARY KEY,
    document_id      TEXT NOT NULL UNIQUE,          -- 幂等键：排序uid列表+md5(statement)[:12]
    platform         TEXT NOT NULL DEFAULT '',
    user_id          TEXT NOT NULL,
    related_user_ids TEXT[]  DEFAULT '{}',          -- 关系事实多方归属（裸 uid）
    display_name     TEXT NOT NULL DEFAULT '',
    category         TEXT NOT NULL,                 -- identity|stable|interaction|naming|recent|uncertain
    statement        TEXT NOT NULL,
    confidence       TEXT NOT NULL,                 -- high | medium
    session_id       TEXT NOT NULL DEFAULT '',
    group_id         TEXT NOT NULL DEFAULT '',
    evidence_key     TEXT NOT NULL DEFAULT '',      -- 计分去重键 {session_id}|{occurred_at::date}
    occurred_at      TIMESTAMPTZ NOT NULL,
    written_at       TIMESTAMPTZ NOT NULL DEFAULT now(),
    extracted_flag   SMALLINT NOT NULL DEFAULT 0,   -- 0=未提取 1=已处理（合并 agent 消费后置 1）
    embedding        vector(1024)
);
CREATE INDEX IF NOT EXISTS idx_mpfr_user    ON memory_persona_fact_raw (user_id, category, extracted_flag);
CREATE INDEX IF NOT EXISTS idx_mpfr_pending ON memory_persona_fact_raw (extracted_flag) WHERE extracted_flag = 0;

-- 3. 事实簇表（合并 agent 主体；画像直接来源；死亡/被替代簇即墓碑）
CREATE TABLE IF NOT EXISTS memory_fact_cluster (
    id                  BIGSERIAL PRIMARY KEY,
    platform            TEXT NOT NULL DEFAULT '',
    user_id             TEXT NOT NULL,
    category            TEXT NOT NULL,
    canonical_statement TEXT NOT NULL,              -- 簇的规范陈述（并入时由 LLM 可选改写，默认取首条）
    score               REAL NOT NULL,              -- 确信度分数，封顶 10
    status              TEXT NOT NULL DEFAULT 'active',
        -- active | profiled | pending_uncertain | replaced | dead
    evidence_count      INT    NOT NULL DEFAULT 1,
    evidence_keys       TEXT[] DEFAULT '{}',        -- 已计分的证据键，防同会话灌分
    source_fact_ids     BIGINT[] DEFAULT '{}',      -- 审计：来源 raw 事实 id
    last_evidence_at    TIMESTAMPTZ,                -- 最近正向证据时间
    occurred_at         TIMESTAMPTZ NOT NULL,       -- 首次出现时间
    replaced_by         BIGINT,                     -- 被替代时指向新簇 id
    written_at          TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at          TIMESTAMPTZ NOT NULL DEFAULT now(),
    embedding           vector(1024)
);
CREATE INDEX IF NOT EXISTS idx_mfc_user ON memory_fact_cluster (user_id, category, status);
CREATE INDEX IF NOT EXISTS idx_mfc_hnsw ON memory_fact_cluster
    USING hnsw (embedding vector_cosine_ops) WITH (m = 16, ef_construction = 64);

-- 4. 用户画像表（profiled 簇的投影；与簇双向联动）
CREATE TABLE IF NOT EXISTS memory_user_profile (
    id          BIGSERIAL PRIMARY KEY,
    platform    TEXT NOT NULL DEFAULT '',
    user_id     TEXT NOT NULL,
    category    TEXT NOT NULL,
    cluster_id  BIGINT NOT NULL,                    -- 关联簇
    statement   TEXT NOT NULL,                      -- 冗余 canonical_statement，拼装免 join
    score       REAL NOT NULL,
    created_at  TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at  TIMESTAMPTZ NOT NULL DEFAULT now(),
    UNIQUE (platform, user_id, category, cluster_id)
);
CREATE INDEX IF NOT EXISTS idx_mup_user ON memory_user_profile (platform, user_id, category);
