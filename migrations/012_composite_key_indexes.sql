-- P3 expression indexes for the composite-key read path: endpoint matching
-- in fetch_active_edges has the form platform || ':' || subject_uid = ANY($1),
-- which plain (platform, uid) composite indexes cannot push down; expression
-- partial indexes (WHERE status='active' aligned with the query's standing
-- condition) keep the inject read path from degrading into full table scans
-- as the edge table grows.

CREATE INDEX IF NOT EXISTS idx_mee_subject_composite
    ON memory_entity_edge ((platform || ':' || subject_uid))
    WHERE status = 'active';

CREATE INDEX IF NOT EXISTS idx_mee_object_composite
    ON memory_entity_edge ((platform || ':' || object_uid))
    WHERE status = 'active';
