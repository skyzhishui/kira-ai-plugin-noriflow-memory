-- 005_related_user_ids.sql：关系事实读侧可见（多方归属）
-- 簇表与画像表补 related_user_ids 列（建簇时从 raw 事实快照继承），读侧
-- （簇候选检索 / 画像分栏 / 待定信息栏）以 owner OR related 匹配——
-- "A 和 B 是室友" 同时出现在 A 与 B 的画像/候选空间，替代此前只归属
-- user_id 单方的行为。related 为建簇时快照，merge 不做并集（same 裁定
-- 意味着同一关系，集合本应一致）。

ALTER TABLE memory_fact_cluster
    ADD COLUMN IF NOT EXISTS related_user_ids TEXT[] NOT NULL DEFAULT '{}';
ALTER TABLE memory_user_profile
    ADD COLUMN IF NOT EXISTS related_user_ids TEXT[] NOT NULL DEFAULT '{}';

CREATE INDEX IF NOT EXISTS idx_mfc_related ON memory_fact_cluster
    USING GIN (related_user_ids);

-- 存量回填：簇表取来源事实（source_fact_ids）中首个非空 related 集合
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

-- 存量回填：画像行从所属簇对齐（簇即唯一真相源）
UPDATE memory_user_profile p
SET related_user_ids = c.related_user_ids
FROM memory_fact_cluster c
WHERE p.cluster_id = c.id
  AND p.related_user_ids <> c.related_user_ids;
