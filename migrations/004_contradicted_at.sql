-- 004_contradicted_at.sql: contradiction marker column (evidence floor exemption)
-- On merge disposition, timestamps target clusters ruled correction/drift
-- (updated_at untouched, so profile projection/ranking is unaffected); the
-- decay pass exempts marked clusters from the evidence floor, restoring
-- natural decay drain-out. A same re-confirm merge clears the marker
-- (restores stickiness).
ALTER TABLE memory_fact_cluster ADD COLUMN IF NOT EXISTS contradicted_at TIMESTAMPTZ;
