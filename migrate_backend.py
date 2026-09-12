#!/usr/bin/env python3
"""One-shot storage-backend migration: PostgreSQL → SQLite (dual-backend plan §4).

Same-schema, full-fidelity copy — NO re-embedding and NO re-encoding:
original row ids, timestamps, embeddings (pgvector text → float32 BLOB),
array columns (TEXT[] → JSON TEXT), search_text (+ FTS5 shadow rebuild).

Id-collision aware: when the target table already holds rows (e.g. the
service ran on sqlite before the cutover), all source ids of that table are
shifted by the current target max id and every cross-reference is remapped
(cluster.replaced_by, cluster.source_fact_ids, profile.cluster_id,
"backfill|{cid}" edge evidence keys, id-bearing kv keys). With an empty
target the ids are copied verbatim.

Rows whose UNIQUE key already exists in the target (summary/fact
document_id, alias (platform,user_id,name), edge structural key) are
SKIPPED under --force (the target row keeps its place; nothing is
overwritten). The skipped source id maps to the existing target row id so
cross-references stay resolvable. Note: evidence_keys carried only by a
skipped edge row are not merged into the target edge (plain skip semantics).

Safety:
- refuses to run twice (kv marker `_backend_migration_done`, --force clears);
- refuses a non-empty target without --force; with --force it merges
  (offset ids + unique-key skip) instead of failing mid-copy;
- batched BEGIN IMMEDIATE transactions (short write locks, WAL-friendly);
- verification after copy: per-table counts (minus skipped), document_id
  coverage, profile→cluster referential check, FTS row count, embedding
  round-trip (sample dims compared source-vs-target — works for any
  embedding model, not just 1024 dims; non-null vector counts must match);
- --dry-run previews counts/collisions without writing.

The safest window is with the bot idle (no conversation rounds): the copy
takes seconds at personal scale. PG is only read; the sqlite backend's own
migrations are idempotent, so the target may be fresh or live.

Usage example:
  /data/KiraAI/venv/bin/python migrate_backend.py \
    --config /data/KiraAI/data/config/plugins/kira-ai-plugin-noriflow-memory.json \
    --sqlite-path /data/KiraAI/data/plugin_data/kira-ai-plugin-noriflow-memory/memory.sqlite3 \
    [--dry-run] [--batch-size 500] [--force]
"""

from __future__ import annotations

import argparse
import asyncio
import importlib.util
import json
import logging
import struct
import sys
import types
from datetime import datetime, timezone
from pathlib import Path

PLUGIN_DIR = Path(__file__).resolve().parent

# ---------------------------------------------------------------------------
#  standalone bootstrap (stub core.logging_manager + package, load modules)
# ---------------------------------------------------------------------------


def _install_stubs() -> None:
    if "noriflow_memory_pkg" in sys.modules:
        return
    core = types.ModuleType("core")
    logging_mod = types.ModuleType("core.logging_manager")

    logging_mod.get_logger = lambda *args, **kwargs: logging.getLogger("migrate")

    sys.modules["core"] = core
    sys.modules["core.logging_manager"] = logging_mod

    pkg = types.ModuleType("noriflow_memory_pkg")
    pkg.__path__ = [str(PLUGIN_DIR)]
    sys.modules["noriflow_memory_pkg"] = pkg


def _load(mod_name: str):
    _install_stubs()
    target = PLUGIN_DIR / mod_name
    if target.is_dir():
        path = target / "__init__.py"
        kwargs = {"submodule_search_locations": [str(target)]}
    else:
        path = PLUGIN_DIR / f"{mod_name}.py"
        kwargs = {}
    spec = importlib.util.spec_from_file_location(
        f"noriflow_memory_pkg.{mod_name}", path, **kwargs
    )
    mod = importlib.util.module_from_spec(spec)
    sys.modules[f"noriflow_memory_pkg.{mod_name}"] = mod
    spec.loader.exec_module(mod)
    return mod


# ---------------------------------------------------------------------------
#  table specs (terminal schema; explicit columns keep embedding::text clean)
# ---------------------------------------------------------------------------

_TS_KEYS = frozenset({
    "occurred_at", "written_at", "updated_at", "created_at",
    "last_evidence_at", "demoted_at", "contradicted_at", "superseded_at",
    "first_seen", "last_seen",
})
_JSON_KEYS = frozenset({
    "participants", "related_user_ids", "evidence_keys", "source_fact_ids",
})
_INT_KEYS = frozenset({"summarized", "extracted_flag"})

_TABLES: list[dict] = [
    {
        "name": "memory_chat_summary",
        "cols": "id, document_id, kind, platform, session_id, group_id, "
                "user_id, participants, content, occurred_at, written_at, "
                "embedding::text AS embedding, summarized, search_text",
        "fts": True,
        "unique_key": ("document_id",),
    },
    {
        "name": "memory_persona_fact_raw",
        "cols": "id, document_id, platform, user_id, related_user_ids, "
                "display_name, category, statement, confidence, session_id, "
                "group_id, evidence_key, occurred_at, written_at, "
                "extracted_flag, embedding::text AS embedding",
        "unique_key": ("document_id",),
    },
    {
        "name": "memory_fact_cluster",
        "cols": "id, platform, user_id, category, canonical_statement, score, "
                "status, evidence_count, evidence_keys, source_fact_ids, "
                "last_evidence_at, occurred_at, replaced_by, written_at, "
                "updated_at, embedding::text AS embedding, demoted_at, "
                "contradicted_at, related_user_ids",
        "remap": "cluster",
    },
    {
        "name": "memory_user_profile",
        "cols": "id, platform, user_id, category, cluster_id, statement, "
                "score, created_at, updated_at, related_user_ids",
        "remap": "profile",
    },
    {
        "name": "memory_entity_alias",
        "cols": "id, platform, user_id, name, first_seen, last_seen, source",
        "unique_key": ("platform", "user_id", "name"),
    },
    {
        "name": "memory_entity_edge",
        "cols": "id, platform, subject_uid, object_uid, subject_name, "
                "object_name, relation_label, statement, status, confidence, "
                "evidence_count, evidence_keys, first_seen, last_seen, "
                "occurred_at, written_at, supersede_reason, superseded_at, "
                "updated_at",
        "remap": "edge",
        "unique_key": ("platform", "subject_uid", "object_uid",
                       "relation_label"),
    },
]

# kv keys whose values carry ids that must follow the remap
_KV_ID_KEYS = {
    "relation_backfill_watermark": "cluster",   # int cluster id
    "relation_audit_edge_id": "edge",           # int edge id
    "relation_backfill_pending_ids": "cluster", # JSON list of cluster ids
}

_MARKER_KEY = "_backend_migration_done"


def _fmt_ts(value) -> str | None:
    """asyncpg aware datetime / naive / None → fixed-format UTC TEXT."""
    if value is None:
        return None
    if isinstance(value, str):
        return value
    if value.tzinfo is None:
        value = value.astimezone()
    return value.astimezone(timezone.utc).strftime(
        "%Y-%m-%dT%H:%M:%S.%f"
    ) + "Z"


def _json_list(value) -> str:
    """PG TEXT[] (asyncpg list) → JSON TEXT (empty/None → '[]')."""
    return json.dumps(list(value or []), ensure_ascii=False)


def _pack_embedding(text_or_none, warnings: list[str]) -> bytes | None:
    """pgvector text '[...]' (the shape asyncpg returns without a registered
    codec) -> float32 BLOB."""
    if text_or_none is None:
        return None
    try:
        parsed = json.loads(text_or_none)
        vec = [float(x) for x in parsed]
    except (ValueError, TypeError):
        warnings.append(f"embedding 不可解析，按 NULL 迁移: {text_or_none!r:.60}")
        return None
    if not vec:
        return None
    return struct.pack(f"<{len(vec)}f", *vec)


# PG-nullable columns whose SQLite counterparts are NOT NULL with a default
# (passing the source NULL through would violate the constraint). Only
# supersede_reason is affected today — superseded_at / replaced_by /
# demoted_at / contradicted_at are nullable on the SQLite side too.
_NULL_TO_DEFAULT = {"supersede_reason": ""}


def _convert_row(row: dict, warnings: list[str]) -> dict:
    out = {}
    for key, value in row.items():
        if key == "embedding":
            out[key] = _pack_embedding(value, warnings)
        elif key in _TS_KEYS:
            out[key] = _fmt_ts(value)
        elif key in _JSON_KEYS:
            out[key] = _json_list(value)
        elif key in _INT_KEYS:
            out[key] = 1 if value else 0
        elif value is None and key in _NULL_TO_DEFAULT:
            out[key] = _NULL_TO_DEFAULT[key]
        else:
            out[key] = value
    return out


async def _copy_table(
    pg_conn, sdb, spec: dict, offset: int, batch_size: int, warnings: list[str]
) -> tuple[int, int, dict[int, int]]:
    """Copy one table with explicit ids (+offset).

    Rows whose UNIQUE key already exists in the target are skipped (the
    target row wins; the source id maps to the existing target id so
    cross-references stay resolvable).

    Returns:
        (inserted, skipped, id_map).
    """
    name = spec["name"]
    ukey: tuple[str, ...] = spec.get("unique_key", ())

    existing: dict[tuple, int] = {}
    if ukey:
        cols_sql = ", ".join(("id",) + ukey)
        async with sdb.pool.acquire() as conn:
            rows = await conn.fetch(
                f"SELECT {cols_sql} FROM {name}"  # noqa: S608
            )
        for r in rows:
            existing[tuple(str(r[k]) for k in ukey)] = int(r["id"])

    rows = await pg_conn.fetch(f"SELECT {spec['cols']} FROM {name} ORDER BY id")
    id_map: dict[int, int] = {}
    converted = []
    skipped = 0
    skipped_samples: list[str] = []
    for r in rows:
        row = _convert_row(dict(r), warnings)
        old_id = int(row["id"])
        if ukey:
            key = tuple(str(row[k]) for k in ukey)
            hit = existing.get(key)
            if hit is not None:
                # Unique-key collision: skip the source row and point its id
                # at the existing target row so cross-references stay resolvable
                skipped += 1
                id_map[old_id] = hit
                if len(skipped_samples) < 5:
                    skipped_samples.append("/".join(str(v) for v in key))
                continue
        new_id = old_id + offset
        id_map[old_id] = new_id
        row["id"] = new_id
        converted.append(row)
    if skipped:
        warnings.append(
            f"{name}: 跳过唯一键冲突行 {skipped} 条"
            f"（如 {', '.join(skipped_samples)}）"
        )

    cols = list(converted[0].keys()) if converted else None
    placeholders = ", ".join(f"${i + 1}" for i in range(len(cols or [])))
    col_sql = ", ".join(cols or [])
    for start in range(0, len(converted), batch_size):
        batch = converted[start : start + batch_size]
        async with sdb.pool.acquire() as conn:
            async with conn.transaction():
                for row in batch:
                    values = [row[c] for c in cols]
                    await conn.execute(
                        f"INSERT INTO {name} ({col_sql}) "
                        f"VALUES ({placeholders})",
                        *values,
                    )
    return len(converted), skipped, id_map


def _remap_cluster_row(row: dict, cluster_map: dict, raw_map: dict) -> None:
    rb = row.get("replaced_by")
    if rb is not None:
        row["replaced_by"] = cluster_map.get(int(rb), int(rb))
    sfids = row.get("source_fact_ids")
    if sfids:
        try:
            ids = json.loads(sfids)
            row["source_fact_ids"] = json.dumps(
                [raw_map.get(int(i), int(i)) for i in ids]
            )
        except (ValueError, TypeError):
            pass


def _remap_edge_row(row: dict, cluster_map: dict) -> None:
    keys = row.get("evidence_keys")
    if not keys:
        return
    try:
        parsed = json.loads(keys)
    except (ValueError, TypeError):
        return
    out = []
    changed = False
    for k in parsed:
        if str(k).startswith("backfill|"):
            try:
                cid = int(str(k).split("|", 1)[1])
                out.append(f"backfill|{cluster_map.get(cid, cid)}")
                changed = True
                continue
            except ValueError:
                pass
        out.append(k)
    if changed:
        row["evidence_keys"] = json.dumps(out, ensure_ascii=False)


def _remap_kv(value: str, key: str, cluster_map: dict, edge_map: dict):
    table = _KV_ID_KEYS[key]
    id_map = cluster_map if table == "cluster" else edge_map

    def m(i: int) -> int:
        return id_map.get(int(i), int(i))

    if key == "relation_backfill_pending_ids":
        try:
            ids = json.loads(value)
            return json.dumps([m(i) for i in ids if str(i).lstrip("-").isdigit()])
        except (ValueError, TypeError):
            return value
    text = str(value).strip()
    return str(m(int(text))) if text.lstrip("-").isdigit() else value


async def _run_migration(
    pg, sdb, *, force: bool = False, dry_run: bool = False, batch_size: int = 500
) -> int:
    """Copy PG → SQLite with pre-check + verification (main_async's core;
    split out so tests can drive it with a stubbed PG source)."""
    try:
        marker = await sdb.get_kv(_MARKER_KEY)
        if marker and not force:
            print(f"ERROR: 目标库已迁移过（kv {_MARKER_KEY} 存在）；"
                  "如确认要重复执行请加 --force")
            return 2

        # ---- Pre-check: source counts + target state (offset basis) ----
        stats: dict[str, dict] = {}
        target_max: dict[str, int] = {}
        async with sdb.pool.acquire() as conn:
            for spec in _TABLES:
                name = spec["name"]
                pg_n = await pg.fetchval(f"SELECT count(*) FROM {name}")
                t_max = await conn.fetchval(f"SELECT COALESCE(MAX(id), 0) FROM {name}")
                t_n = await conn.fetchval(f"SELECT count(*) FROM {name}")
                target_max[name] = int(t_max or 0)
                stats[name] = {"pg": int(pg_n or 0), "target": int(t_n or 0)}
        kv_rows = await pg.fetch(
            "SELECT key, value, updated_at FROM _memory_local_kv"
        )
        print("== 迁移预检 ==")
        for spec in _TABLES:
            n = stats[spec["name"]]
            print(f"  {spec['name']}: pg={n['pg']} target={n['target']}")
        print(f"  _memory_local_kv: pg={len(kv_rows)}")

        if dry_run:
            print("dry-run：未写入。")
            return 0

        if any(s["target"] > 0 for s in stats.values()) and not force:
            print("ERROR: 目标库已有数据（可能服务已在 sqlite 上运行过）。"
                  "--force 语义：id 整体偏移并重映射引用，唯一键冲突的行"
                  "跳过（保留目标现状）；确认请加 --force 重跑")
            return 2

        # ---- Copy (one bulk transaction per table; empty target keeps offset=0
# for a byte-faithful copy) ----
        maps = {"cluster": {}, "raw": {}, "edge": {}}
        warnings: list[str] = []
        copied: dict[str, int] = {}
        skipped: dict[str, int] = {}
        for spec in _TABLES:
            name = spec["name"]
            offset = target_max[name]  # Empty table = 0 (original ids); non-empty = a uniform offset
            n, skipped_n, id_map = await _copy_table(
                pg, sdb, spec, offset, batch_size, warnings
            )
            copied[name] = n
            skipped[name] = skipped_n
            kind = spec.get("remap")
            if kind == "cluster":
                maps["cluster"] = id_map
            elif kind == "profile":
                # Profile rows are referenced in the other direction (by cluster/source)
# rows; nothing references them
                pass
            elif kind == "edge":
                maps["edge"] = id_map
            # The raw-table id_map is reserved for cluster.source_fact_ids (ready
# before the next loop round)
            if name == "memory_persona_fact_raw":
                maps["raw"] = id_map
            skip_note = f", skipped {skipped_n}" if skipped_n else ""
            print(f"  copied {name}: {n} rows (offset={offset}{skip_note})")

        # Cross-reference remapping (identity when offset=0, safe to re-run)
        for spec in _TABLES:
            if not spec.get("remap"):
                continue
            name = spec["name"]
            async with sdb.pool.acquire() as conn:
                rows = await conn.fetch(f"SELECT * FROM {name}")
                for r in rows:
                    row = dict(r)
                    old = dict(r)
                    if name == "memory_fact_cluster":
                        _remap_cluster_row(row, maps["cluster"], maps["raw"])
                    elif name == "memory_entity_edge":
                        _remap_edge_row(row, maps["cluster"])
                    elif name == "memory_user_profile":
                        cid = row.get("cluster_id")
                        if cid is not None:
                            row["cluster_id"] = maps["cluster"].get(
                                int(cid), int(cid)
                            )
                    else:
                        continue
                    if row == old:
                        continue
                    sets = ", ".join(
                        f"{k} = ${i + 2}" for i, k in enumerate(
                            [k for k in row if k != "id"]
                        )
                    )
                    await conn.execute(
                        f"UPDATE {name} SET {sets} WHERE id = $1",
                        row["id"],
                        *[row[k] for k in row if k != "id"],
                    )

        # kv copy + id-key remapping
        async with sdb.pool.acquire() as conn:
            for r in kv_rows:
                value = r["value"]
                if r["key"] in _KV_ID_KEYS:
                    value = _remap_kv(
                        value, r["key"], maps["cluster"], maps["edge"]
                    )
                await conn.execute(
                    "INSERT OR REPLACE INTO _memory_local_kv "
                    "(key, value, updated_at) VALUES ($1, $2, $3)",
                    r["key"], value, _fmt_ts(r["updated_at"]),
                )
        print(f"  copied _memory_local_kv: {len(kv_rows)} rows")

        # FTS5 shadow-table rebuild (search_text carried over as-is)
        async with sdb.pool.acquire() as conn:
            await conn.execute("DELETE FROM memory_chat_summary_fts")
            await conn.execute(
                "INSERT INTO memory_chat_summary_fts (summary_id, text) "
                "SELECT id, search_text FROM memory_chat_summary "
                "WHERE search_text IS NOT NULL AND search_text <> ''"
            )
            fts_n = await conn.fetchval(
                "SELECT count(*) FROM memory_chat_summary_fts"
            )
        print(f"  FTS shadow rebuilt: {fts_n} rows")

        # ---- Verification ----
        print("== 校验 ==")
        ok = True
        async with sdb.pool.acquire() as conn:
            for spec in _TABLES:
                name = spec["name"]
                pg_n = stats[name]["pg"]
                t_n = await conn.fetchval(f"SELECT count(*) FROM {name}")
                delta = int(t_n) - stats[name]["target"]
                expected = pg_n - skipped.get(name, 0)
                good = delta == expected
                ok &= good
                skip_note = (
                    f", skipped {skipped.get(name, 0)}"
                    if skipped.get(name)
                    else ""
                )
                print(f"  {name}: +{delta} (pg {pg_n}{skip_note}) "
                      f"{'OK' if good else 'MISMATCH'}")
            # Full document_id cross-check against PG (no missed rows)
            pg_ids = await pg.fetch(
                "SELECT document_id FROM memory_chat_summary"
            )
            missing = 0
            for r in pg_ids:
                hit = await conn.fetchval(
                    "SELECT count(*) FROM memory_chat_summary WHERE document_id = $1",
                    r["document_id"],
                )
                if not hit:
                    missing += 1
            ok &= missing == 0
            print(f"  summary document_id 缺失: {missing}")
            orphan = await conn.fetchval(
                "SELECT count(*) FROM memory_user_profile p WHERE NOT EXISTS ("
                "SELECT 1 FROM memory_fact_cluster c WHERE c.id = p.cluster_id)"
            )
            ok &= orphan == 0
            print(f"  profile→cluster 悬挂: {orphan}")
            # Embedding fidelity: compare dims of each side's lowest-id
            # non-null row (the same logical row on a clean copy; under a
            # --force merge a skipped source row may leave the two sides
            # sampling different rows, so this is a dims sanity check, not a
            # per-row guarantee) — works for ANY embedding model (the dim size
            # is not pinned to 1024; deployments may use other models);
            # non-null vector counts must also match (pack failures drop
            # vectors — details are listed in the warnings)
            pg_emb = await pg.fetchval(
                "SELECT embedding::text FROM memory_chat_summary "
                "WHERE embedding IS NOT NULL ORDER BY id LIMIT 1"
            )
            tgt_emb = await conn.fetchval(
                "SELECT embedding FROM memory_chat_summary "
                "WHERE embedding IS NOT NULL ORDER BY id LIMIT 1"
            )
            src_dims = len(json.loads(pg_emb)) if pg_emb else 0
            tgt_dims = len(tgt_emb) // 4 if tgt_emb else 0
            pg_emb_n = await pg.fetchval(
                "SELECT count(*) FROM memory_chat_summary "
                "WHERE embedding IS NOT NULL"
            )
            tgt_emb_n = await conn.fetchval(
                "SELECT count(*) FROM memory_chat_summary "
                "WHERE embedding IS NOT NULL"
            )
            dims_ok = (src_dims == 0 and tgt_dims == 0) or (
                src_dims > 0 and src_dims == tgt_dims
            )
            counts_ok = int(pg_emb_n or 0) == int(tgt_emb_n or 0)
            print(
                f"  embedding: dims pg={src_dims} target={tgt_dims}, "
                f"non-null pg={pg_emb_n} target={tgt_emb_n} "
                f"{'OK' if dims_ok and counts_ok else 'MISMATCH'}"
            )
            ok &= dims_ok and counts_ok

        marker_stats = {
            "migrated_at": _fmt_ts(datetime.now(timezone.utc)),
            "source": "postgres",
            "copied": copied,
            "skipped": skipped,
            "warnings": warnings[:20],
        }
        await sdb.set_kv(_MARKER_KEY, json.dumps(marker_stats, ensure_ascii=False))
        for w in warnings:
            print(f"  WARN {w}")
        print("== 迁移完成 ==" if ok else "== 迁移完成（存在校验告警，请核查）==")
        return 0 if ok else 1
    finally:
        await pg.close()
        await sdb.close()


async def main_async(args: argparse.Namespace) -> int:
    import asyncpg

    config_mod = _load("config")
    _load("alias_store")
    _load("entity_edge")
    db_pkg = _load("db")

    host_cfg = json.loads(Path(args.config).read_text(encoding="utf-8"))
    dsn = host_cfg.get("dsn") or ""
    if not dsn:
        print("ERROR: 插件配置中无 dsn（PG 源不可用）")
        return 2
    sqlite_path = args.sqlite_path or host_cfg.get("sqlite_path") or ""
    if not sqlite_path:
        print("ERROR: 未指定 --sqlite-path 且配置中 sqlite_path 为空")
        return 2

    pg = await asyncpg.connect(dsn)
    sdb = db_pkg.create_backend(
        config_mod.LocalMemoryConfig(
            storage_backend="sqlite", sqlite_path=sqlite_path
        )
    )
    await sdb.connect()
    await sdb.apply_migrations(PLUGIN_DIR / "migrations_sqlite")
    return await _run_migration(
        pg, sdb, force=args.force, dry_run=args.dry_run,
        batch_size=args.batch_size,
    )


def main() -> int:
    parser = argparse.ArgumentParser(
        description="noriflow-memory 存储后端迁移：PostgreSQL → SQLite"
    )
    parser.add_argument("--config", required=True,
                        help="插件配置 JSON 路径（读 dsn）")
    parser.add_argument("--sqlite-path", default="",
                        help="目标 SQLite 库文件（空 = 读配置 sqlite_path，"
                             "仍为空则报错）")
    parser.add_argument("--batch-size", type=int, default=500)
    parser.add_argument("--dry-run", action="store_true",
                        help="只做预检计数，不写入")
    parser.add_argument("--force", action="store_true",
                        help="目标非空时合并写入（id 整体偏移、引用重映射，"
                             "唯一键冲突的行跳过）；同时清除重复执行守卫")
    args = parser.parse_args()
    return asyncio.run(main_async(args))


if __name__ == "__main__":
    sys.exit(main())
