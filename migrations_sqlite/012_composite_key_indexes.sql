-- P3 expression indexes for the composite-key read path (SQLite dialect,
-- counterpart of the postgres-side 012): endpoint matching in
-- fetch_active_edges concatenates platform || ':' || uid IN (...), which a
-- plain (platform, uid, status) index cannot push down; expression partial
-- indexes keep the inject read path from degrading into full table scans as
-- the edge table grows.

CREATE INDEX IF NOT EXISTS idx_mee_subject_composite
    ON memory_entity_edge (platform || ':' || subject_uid)
    WHERE status = 'active';

CREATE INDEX IF NOT EXISTS idx_mee_object_composite
    ON memory_entity_edge (platform || ':' || object_uid)
    WHERE status = 'active';
