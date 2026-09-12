-- 004_contradicted_at.sql：矛盾标记列（证据地板豁免）
-- 合并处置时对被裁定 correction/drift 的目标簇打时间戳（不动 updated_at，
-- 画像投影/排序不受影响）；衰减遍的证据地板豁免带标记的簇，恢复自然衰减
-- 出清。same 再确认 merge 清除标记（恢复粘性）。
ALTER TABLE memory_fact_cluster ADD COLUMN IF NOT EXISTS contradicted_at TIMESTAMPTZ;
