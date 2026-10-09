-- P3 复合键读路径表达式索引（sqlite 方言，对应 postgres 侧 012）：
-- fetch_active_edges 端点匹配为 platform || ':' || uid IN (...) 拼接，
-- 普通 (platform, uid, status) 索引无法下推；建表达式部分索引避免
-- 边表增长后注入读路径退化为全表扫描。

CREATE INDEX IF NOT EXISTS idx_mee_subject_composite
    ON memory_entity_edge (platform || ':' || subject_uid)
    WHERE status = 'active';

CREATE INDEX IF NOT EXISTS idx_mee_object_composite
    ON memory_entity_edge (platform || ':' || object_uid)
    WHERE status = 'active';
