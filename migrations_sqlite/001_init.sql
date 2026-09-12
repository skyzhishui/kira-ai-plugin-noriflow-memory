-- 001_init.sql: SQLite backend merged initialization (terminal schema).
--
-- Equivalent to the叠加后的 PG migrations 001-010 terminal shape (dual-storage
-- plan §7): fresh SQLite installs land on the final schema in one file, and
-- this file also registers versions 2..10 in _memory_local_migrations so the
-- shared version sequence keeps future paired migrations (011_pg + 011_sqlite)
-- correct.
--
-- Dialect mapping (plan §5.1):
--   BIGSERIAL            -> INTEGER PRIMARY KEY (rowid alias)
--   TIMESTAMPTZ          -> TEXT, fixed "%Y-%m-%dT%H:%M:%S.%f" UTC + "Z"
--                           (6 fractional digits => lexicographic == chrono;
--                           every write path supplies the value from Python —
--                           the STRFTIME default is a 3-digit-ms safety net)
--   TEXT[]               -> TEXT holding a JSON array (written via json.dumps;
--                           membership via json_each EXISTS subqueries)
--   vector(1024)         -> BLOB (float32 LE); dimension is not fixed by the
--                           column, mismatched rows are treated as missing
--   tsvector GIN         -> FTS5 shadow table memory_chat_summary_fts,
--                           kept in sync by the write paths (same txn)
--   now()                -> supplied from Python (no SQLite date functions)
--
-- Indexes: partial indexes cover the periodic backfill scans exactly like the
-- PG originals; GIN/HNSW equivalents are intentionally absent (JSON arrays are
-- scanned inside small row sets; vectors use Python brute-force cosine).

CREATE TABLE IF NOT EXISTS memory_chat_summary (
    id           INTEGER PRIMARY KEY,
    document_id  TEXT NOT NULL UNIQUE,
    kind         TEXT NOT NULL DEFAULT 'chat_summary',
    platform     TEXT NOT NULL DEFAULT '',
    session_id   TEXT NOT NULL DEFAULT '',
    group_id     TEXT NOT NULL DEFAULT '',
    user_id      TEXT NOT NULL DEFAULT '',
    participants TEXT NOT NULL DEFAULT '[]',
    content      TEXT NOT NULL,
    occurred_at  TEXT NOT NULL,
    written_at   TEXT NOT NULL DEFAULT (STRFTIME('%Y-%m-%dT%H:%M:%fZ', 'now')),
    embedding    BLOB,
    summarized   INTEGER NOT NULL DEFAULT 1,
    search_text  TEXT
);
CREATE INDEX IF NOT EXISTS idx_mcs_session
    ON memory_chat_summary (session_id, occurred_at DESC);
CREATE INDEX IF NOT EXISTS idx_mcs_kind ON memory_chat_summary (kind);
CREATE INDEX IF NOT EXISTS idx_mcs_unsummarized
    ON memory_chat_summary (id) WHERE NOT summarized;
CREATE INDEX IF NOT EXISTS idx_mcs_backfill
    ON memory_chat_summary (id) WHERE embedding IS NULL;
CREATE INDEX IF NOT EXISTS idx_mcs_search_text_null
    ON memory_chat_summary (id) WHERE search_text IS NULL;

CREATE TABLE IF NOT EXISTS memory_persona_fact_raw (
    id               INTEGER PRIMARY KEY,
    document_id      TEXT NOT NULL UNIQUE,
    platform         TEXT NOT NULL DEFAULT '',
    user_id          TEXT NOT NULL,
    related_user_ids TEXT NOT NULL DEFAULT '[]',
    display_name     TEXT NOT NULL DEFAULT '',
    category         TEXT NOT NULL,
    statement        TEXT NOT NULL,
    confidence       TEXT NOT NULL,
    session_id       TEXT NOT NULL DEFAULT '',
    group_id         TEXT NOT NULL DEFAULT '',
    evidence_key     TEXT NOT NULL DEFAULT '',
    occurred_at      TEXT NOT NULL,
    written_at       TEXT NOT NULL DEFAULT (STRFTIME('%Y-%m-%dT%H:%M:%fZ', 'now')),
    extracted_flag   INTEGER NOT NULL DEFAULT 0,
    embedding        BLOB
);
CREATE INDEX IF NOT EXISTS idx_mpfr_user
    ON memory_persona_fact_raw (user_id, category, extracted_flag);
CREATE INDEX IF NOT EXISTS idx_mpfr_pending
    ON memory_persona_fact_raw (extracted_flag) WHERE extracted_flag = 0;
CREATE INDEX IF NOT EXISTS idx_mpfr_pending_id
    ON memory_persona_fact_raw (id) WHERE extracted_flag = 0;
CREATE INDEX IF NOT EXISTS idx_mpfr_backfill
    ON memory_persona_fact_raw (id) WHERE embedding IS NULL;

CREATE TABLE IF NOT EXISTS memory_fact_cluster (
    id                  INTEGER PRIMARY KEY,
    platform            TEXT NOT NULL DEFAULT '',
    user_id             TEXT NOT NULL,
    category            TEXT NOT NULL,
    canonical_statement TEXT NOT NULL,
    score               REAL NOT NULL,
    status              TEXT NOT NULL DEFAULT 'active',
    evidence_count      INTEGER NOT NULL DEFAULT 1,
    evidence_keys       TEXT NOT NULL DEFAULT '[]',
    source_fact_ids     TEXT NOT NULL DEFAULT '[]',
    last_evidence_at    TEXT,
    occurred_at         TEXT NOT NULL,
    replaced_by         INTEGER,
    written_at          TEXT NOT NULL DEFAULT (STRFTIME('%Y-%m-%dT%H:%M:%fZ', 'now')),
    updated_at          TEXT NOT NULL DEFAULT (STRFTIME('%Y-%m-%dT%H:%M:%fZ', 'now')),
    embedding           BLOB,
    demoted_at          TEXT,
    contradicted_at     TEXT,
    related_user_ids    TEXT NOT NULL DEFAULT '[]'
);
CREATE INDEX IF NOT EXISTS idx_mfc_user
    ON memory_fact_cluster (user_id, category, status);
CREATE INDEX IF NOT EXISTS idx_mfc_backfill
    ON memory_fact_cluster (id) WHERE embedding IS NULL;

CREATE TABLE IF NOT EXISTS memory_user_profile (
    id          INTEGER PRIMARY KEY,
    platform    TEXT NOT NULL DEFAULT '',
    user_id     TEXT NOT NULL,
    category    TEXT NOT NULL,
    cluster_id  INTEGER NOT NULL,
    statement   TEXT NOT NULL,
    score       REAL NOT NULL,
    created_at  TEXT NOT NULL DEFAULT (STRFTIME('%Y-%m-%dT%H:%M:%fZ', 'now')),
    updated_at  TEXT NOT NULL DEFAULT (STRFTIME('%Y-%m-%dT%H:%M:%fZ', 'now')),
    related_user_ids TEXT NOT NULL DEFAULT '[]',
    UNIQUE (platform, user_id, category, cluster_id)
);
CREATE INDEX IF NOT EXISTS idx_mup_user
    ON memory_user_profile (platform, user_id, category);

CREATE TABLE IF NOT EXISTS _memory_local_kv (
    key        TEXT PRIMARY KEY,
    value      TEXT NOT NULL,
    updated_at TEXT NOT NULL DEFAULT (STRFTIME('%Y-%m-%dT%H:%M:%fZ', 'now'))
);

CREATE TABLE IF NOT EXISTS memory_entity_alias (
    id         INTEGER PRIMARY KEY,
    platform   TEXT NOT NULL DEFAULT '',
    user_id    TEXT NOT NULL,
    name       TEXT NOT NULL,
    first_seen TEXT NOT NULL DEFAULT (STRFTIME('%Y-%m-%dT%H:%M:%fZ', 'now')),
    last_seen  TEXT NOT NULL DEFAULT (STRFTIME('%Y-%m-%dT%H:%M:%fZ', 'now')),
    source     TEXT NOT NULL DEFAULT 'batch',
    UNIQUE (platform, user_id, name)
);
CREATE INDEX IF NOT EXISTS idx_mea_owner_seen
    ON memory_entity_alias (platform, user_id, last_seen DESC);

CREATE TABLE IF NOT EXISTS memory_entity_edge (
    id             INTEGER PRIMARY KEY,
    platform       TEXT NOT NULL DEFAULT '',
    subject_uid    TEXT NOT NULL,
    object_uid     TEXT NOT NULL,
    subject_name   TEXT NOT NULL DEFAULT '',
    object_name    TEXT NOT NULL DEFAULT '',
    relation_label TEXT NOT NULL,
    statement      TEXT NOT NULL,
    status         TEXT NOT NULL DEFAULT 'active',
    confidence     TEXT NOT NULL DEFAULT 'medium',
    evidence_count INTEGER NOT NULL DEFAULT 1,
    evidence_keys  TEXT NOT NULL DEFAULT '[]',
    first_seen     TEXT NOT NULL DEFAULT (STRFTIME('%Y-%m-%dT%H:%M:%fZ', 'now')),
    last_seen      TEXT NOT NULL DEFAULT (STRFTIME('%Y-%m-%dT%H:%M:%fZ', 'now')),
    occurred_at    TEXT NOT NULL DEFAULT (STRFTIME('%Y-%m-%dT%H:%M:%fZ', 'now')),
    written_at     TEXT NOT NULL DEFAULT (STRFTIME('%Y-%m-%dT%H:%M:%fZ', 'now')),
    supersede_reason TEXT NOT NULL DEFAULT '',
    superseded_at  TEXT,
    updated_at     TEXT NOT NULL DEFAULT (STRFTIME('%Y-%m-%dT%H:%M:%fZ', 'now')),
    UNIQUE (platform, subject_uid, object_uid, relation_label)
);
CREATE INDEX IF NOT EXISTS idx_mee_subject
    ON memory_entity_edge (platform, subject_uid, status);
CREATE INDEX IF NOT EXISTS idx_mee_object
    ON memory_entity_edge (platform, object_uid, status);

-- FTS5 shadow table for the BM25 leg (mirror of PG's tsvector GIN on
-- search_text). Kept in sync by the write paths inside their own
-- transaction: one row per summary (summary_id UNINDEXED + token text).
CREATE VIRTUAL TABLE IF NOT EXISTS memory_chat_summary_fts USING fts5(
    summary_id UNINDEXED,
    text
);

-- Register the PG-equivalent merged versions (2..10) so the shared version
-- sequence stays aligned; the runner itself registers version 1 from the
-- filename. Future schema changes must add paired files (011_pg + 011_sqlite).
INSERT OR IGNORE INTO _memory_local_migrations (version, name) VALUES
    (2,  'merged:002_fact_merge'),
    (3,  'merged:003_summary_state'),
    (4,  'merged:004_contradicted_at'),
    (5,  'merged:005_related_user_ids'),
    (6,  'merged:006_hybrid_search'),
    (7,  'merged:007_backfill_indexes'),
    (8,  'merged:008_entity_alias'),
    (9,  'merged:009_entity_edge'),
    (10, 'merged:010_review_fixes');
