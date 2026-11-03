-- 005_related_user_ids.sql: relation facts visible on the read side (multi-party ownership)
-- Adds related_user_ids columns to the cluster and profile tables (inherited
-- from the raw fact snapshot at cluster build time); the read side (cluster
-- candidate search / profile panels / pending info panel) matches with
-- owner OR related — "A and B are roommates" shows up in both A's and B's
-- profile/candidate space, replacing the previous owner-only attribution to
-- a single user_id. related is the snapshot at cluster build time; merge
-- does no union (a same ruling means the same relation, so the sets should
-- match anyway).

ALTER TABLE memory_fact_cluster
    ADD COLUMN IF NOT EXISTS related_user_ids TEXT[] NOT NULL DEFAULT '{}';
ALTER TABLE memory_user_profile
    ADD COLUMN IF NOT EXISTS related_user_ids TEXT[] NOT NULL DEFAULT '{}';

CREATE INDEX IF NOT EXISTS idx_mfc_related ON memory_fact_cluster
    USING GIN (related_user_ids);

-- Legacy backfill: the cluster table takes the first non-empty related set among its source facts (source_fact_ids)
UPDATE memory_fact_cluster c
SET related_user_ids = sub.related
FROM (
    SELECT c2.id AS cid, f.related_user_ids AS related
    FROM memory_fact_cluster c2,
         LATERAL (
             SELECT f0.related_user_ids
             FROM memory_persona_fact_raw f0
             WHERE f0.id = ANY(c2.source_fact_ids) AND f0.related_user_ids <> '{}'
             ORDER BY f0.id LIMIT 1
         ) f
    WHERE c2.related_user_ids = '{}'
) sub
WHERE c.id = sub.cid;

-- Legacy backfill: profile rows align from their owning cluster (the cluster is the single source of truth)
UPDATE memory_user_profile p
SET related_user_ids = c.related_user_ids
FROM memory_fact_cluster c
WHERE p.cluster_id = c.id
  AND p.related_user_ids <> c.related_user_ids;
