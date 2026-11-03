-- 001_init.sql: initial schema of the local memory store (4 tables).
-- Idempotent: all IF NOT EXISTS; the plugin db.py migration runner applies it
-- inside a transaction and records the version.
-- Vectors are uniformly qwen3-embedding 1024-dim, cosine distance
-- (vector_cosine_ops), HNSW index.

CREATE EXTENSION IF NOT EXISTS vector;

-- 1. Chat summary table (sole recall corpus; bot self-trigger kind=bot_self)
CREATE TABLE IF NOT EXISTS memory_chat_summary (
    id           BIGSERIAL PRIMARY KEY,
    document_id  TEXT NOT NULL UNIQUE,              -- idempotency key {session_id}-{md5(content)[:12]}
    kind         TEXT NOT NULL DEFAULT 'chat_summary',  -- chat_summary | bot_self
    platform     TEXT NOT NULL DEFAULT '',
    session_id   TEXT NOT NULL DEFAULT '',
    group_id     TEXT NOT NULL DEFAULT '',
    user_id      TEXT NOT NULL DEFAULT '',          -- trigger speaker (bare uid)
    participants TEXT[] DEFAULT '{}',               -- all speakers this round ("platform:uid" composite key, used for per-user exact recall filtering)
    content      TEXT NOT NULL,
    occurred_at  TIMESTAMPTZ NOT NULL,              -- fact occurrence time (trigger message time of this round)
    written_at   TIMESTAMPTZ NOT NULL DEFAULT now(),-- write time
    embedding    vector(1024)
);
CREATE INDEX IF NOT EXISTS idx_mcs_session ON memory_chat_summary (session_id, occurred_at DESC);
CREATE INDEX IF NOT EXISTS idx_mcs_kind    ON memory_chat_summary (kind);
CREATE INDEX IF NOT EXISTS idx_mcs_participants ON memory_chat_summary USING GIN (participants);
CREATE INDEX IF NOT EXISTS idx_mcs_hnsw ON memory_chat_summary
    USING hnsw (embedding vector_cosine_ops) WITH (m = 16, ef_construction = 64);

-- 2. Raw fact table (encoder output, waiting to be clustered; not used by recall)
CREATE TABLE IF NOT EXISTS memory_persona_fact_raw (
    id               BIGSERIAL PRIMARY KEY,
    document_id      TEXT NOT NULL UNIQUE,          -- idempotency key: sorted uid list + md5(statement)[:12]
    platform         TEXT NOT NULL DEFAULT '',
    user_id          TEXT NOT NULL,
    related_user_ids TEXT[]  DEFAULT '{}',          -- multi-party ownership of relation facts (bare uid)
    display_name     TEXT NOT NULL DEFAULT '',
    category         TEXT NOT NULL,                 -- identity|stable|interaction|naming|recent|uncertain
    statement        TEXT NOT NULL,
    confidence       TEXT NOT NULL,                 -- high | medium
    session_id       TEXT NOT NULL DEFAULT '',
    group_id         TEXT NOT NULL DEFAULT '',
    evidence_key     TEXT NOT NULL DEFAULT '',      -- scoring dedup key {session_id}|{occurred_at::date}
    occurred_at      TIMESTAMPTZ NOT NULL,
    written_at       TIMESTAMPTZ NOT NULL DEFAULT now(),
    extracted_flag   SMALLINT NOT NULL DEFAULT 0,   -- 0=not extracted 1=processed (set to 1 after merge agent consumption)
    embedding        vector(1024)
);
CREATE INDEX IF NOT EXISTS idx_mpfr_user    ON memory_persona_fact_raw (user_id, category, extracted_flag);
CREATE INDEX IF NOT EXISTS idx_mpfr_pending ON memory_persona_fact_raw (extracted_flag) WHERE extracted_flag = 0;

-- 3. Fact cluster table (merge agent's main object; direct profile source; dead/replaced clusters are tombstones)
CREATE TABLE IF NOT EXISTS memory_fact_cluster (
    id                  BIGSERIAL PRIMARY KEY,
    platform            TEXT NOT NULL DEFAULT '',
    user_id             TEXT NOT NULL,
    category            TEXT NOT NULL,
    canonical_statement TEXT NOT NULL,              -- cluster canonical statement (optionally rewritten by the LLM on merge, defaults to the first one)
    score               REAL NOT NULL,              -- confidence score, capped at 10
    status              TEXT NOT NULL DEFAULT 'active',
        -- active | profiled | pending_uncertain | replaced | dead
    evidence_count      INT    NOT NULL DEFAULT 1,
    evidence_keys       TEXT[] DEFAULT '{}',        -- evidence keys already scored, guards against same-session score stuffing
    source_fact_ids     BIGINT[] DEFAULT '{}',      -- audit: source raw fact ids
    last_evidence_at    TIMESTAMPTZ,                -- latest positive evidence time
    occurred_at         TIMESTAMPTZ NOT NULL,       -- first occurrence time
    replaced_by         BIGINT,                     -- points to the new cluster id when replaced
    written_at          TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at          TIMESTAMPTZ NOT NULL DEFAULT now(),
    embedding           vector(1024)
);
CREATE INDEX IF NOT EXISTS idx_mfc_user ON memory_fact_cluster (user_id, category, status);
CREATE INDEX IF NOT EXISTS idx_mfc_hnsw ON memory_fact_cluster
    USING hnsw (embedding vector_cosine_ops) WITH (m = 16, ef_construction = 64);

-- 4. User profile table (projection of profiled clusters; two-way linked with the cluster)
CREATE TABLE IF NOT EXISTS memory_user_profile (
    id          BIGSERIAL PRIMARY KEY,
    platform    TEXT NOT NULL DEFAULT '',
    user_id     TEXT NOT NULL,
    category    TEXT NOT NULL,
    cluster_id  BIGINT NOT NULL,                    -- owning cluster
    statement   TEXT NOT NULL,                      -- redundant canonical_statement, saves a join when assembling
    score       REAL NOT NULL,
    created_at  TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at  TIMESTAMPTZ NOT NULL DEFAULT now(),
    UNIQUE (platform, user_id, category, cluster_id)
);
CREATE INDEX IF NOT EXISTS idx_mup_user ON memory_user_profile (platform, user_id, category);
