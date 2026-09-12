-- 002_fact_merge.sql：合并 agent 支撑（M4，见开发方案 §8）
-- kv 表：插件内部持久化状态（衰减遍上次执行时间等），进程重启不丢。

CREATE TABLE IF NOT EXISTS _memory_local_kv (
    key        TEXT PRIMARY KEY,
    value      TEXT NOT NULL,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

-- demoted_at：簇进入 pending_uncertain 的时刻（降级/过期时写入，复活时清空）。
-- 迟滞退出的"再一个衰减周期无新证据 -> dead"以此为起算点，避免刚降级即死亡。
ALTER TABLE memory_fact_cluster ADD COLUMN IF NOT EXISTS demoted_at TIMESTAMPTZ;

