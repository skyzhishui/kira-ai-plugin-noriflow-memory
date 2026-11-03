-- P2: structured relation edge table (memory_entity_edge).
-- Writes: zero-LLM structural-key merge of the encoder pass relations array
-- (P2 writes without reading; consumed by P3 injection once the audit-phase
-- threshold is met).
-- State machine: person-person edges are active on insert; bot-endpoint
-- edges insert as pending, and once evidence_count (including this piece of
-- evidence) reaches relation_bot_edge_min_evidence, an inline CASE in the
-- upsert statement promotes them to active (zero LLM); superseded keeps the
-- state slot but is never set automatically (label carries multi-value
-- semantics; zero LLM cannot judge mutual exclusivity — decision 8).
-- Endpoint names (subject_name/object_name): display-name source for P3
-- injected statement rows, refreshed to the latest on every evidence update
-- (full name history lives in memory_entity_alias).

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
