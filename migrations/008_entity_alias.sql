-- 008_entity_alias.sql: persistent entity alias layer (P1, 2026-09 relation graph plan)
-- Name stream (host messages reconciliation / turn-batch upsert) -> alias
-- rows after variant splitting, shared by entity hit matching (the
-- persistent layer after the window dictionary) and the recall entity leg /
-- profile candidates.

CREATE TABLE IF NOT EXISTS memory_entity_alias (
    id         BIGSERIAL PRIMARY KEY,
    platform   TEXT NOT NULL DEFAULT '',
    user_id    TEXT NOT NULL,
    name       TEXT NOT NULL,                        -- candidate name after variant splitting (>=2 word chars)
    first_seen TIMESTAMPTZ NOT NULL DEFAULT now(),   -- row creation time (not the semantic first-seen time)
    last_seen  TIMESTAMPTZ NOT NULL DEFAULT now(),   -- when this name was last observed
    source     TEXT NOT NULL DEFAULT 'batch',        -- backfill | reconcile | batch
    UNIQUE (platform, user_id, name)
);
CREATE INDEX IF NOT EXISTS idx_mea_name ON memory_entity_alias (name);
