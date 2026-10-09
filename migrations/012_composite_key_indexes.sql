-- P3 复合键读路径表达式索引：fetch_active_edges 的端点匹配为
-- platform || ':' || subject_uid = ANY($1) 形态，普通 (platform, uid)
-- 复合索引无法下推；建表达式部分索引（WHERE status='active' 与查询
-- 常驻条件对齐）避免边表增长后注入读路径退化为全表扫描。

CREATE INDEX IF NOT EXISTS idx_mee_subject_composite
    ON memory_entity_edge ((platform || ':' || subject_uid))
    WHERE status = 'active';

CREATE INDEX IF NOT EXISTS idx_mee_object_composite
    ON memory_entity_edge ((platform || ':' || object_uid))
    WHERE status = 'active';
