-- 002_fact_merge.sql: merge agent support (M4, see dev plan §8)
-- kv table: plugin-internal persistent state (last decay-pass run time etc.), survives process restarts.

CREATE TABLE IF NOT EXISTS _memory_local_kv (
    key        TEXT PRIMARY KEY,
    value      TEXT NOT NULL,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

-- demoted_at: the moment the cluster entered pending_uncertain (written on
-- demotion/expiry, cleared on revival). The hysteresis exit "one more decay
-- cycle with no new evidence -> dead" counts from this point, so a freshly
-- demoted cluster does not die immediately.
ALTER TABLE memory_fact_cluster ADD COLUMN IF NOT EXISTS demoted_at TIMESTAMPTZ;

