"""Memory tools + summary lifecycle real-SQLite integration harness (python direct-run).

Validates the db/kernel landing points of the nori-side five-tool migration (tempfile real db):
- db five methods: upsert_persona_fact_raw_for_apply / search_fact_clusters /
  fetch_cluster_owner / cluster_status_op (drop/dispute/reactivate + owner
  validation) / restore_summary;
- kernel.write_fact deterministic direct write (create makes a cluster / replace inherits);
- lifecycle: archive_stale_summaries (over-age / reinforce exemption / minimum retention / keyset
  batching / UPDATE recheck), reinforce_summaries renewal, search structurally excludes archived rows,
  include_archived maintenance channel.

Run (plugin dir):
    python tests/test_memory_tools_sqlite.py
"""

from __future__ import annotations

import asyncio
import logging
import sys
import tempfile
import types
from datetime import datetime, timedelta, timezone
from pathlib import Path

PLUGIN_DIR = Path(__file__).resolve().parent.parent

PASS = 0
_FAIL = 0


def check(name: str, cond: bool, detail: str = "") -> None:
    global PASS, _FAIL
    if cond:
        PASS += 1
        print(f"PASS {name}")
    else:
        _FAIL += 1
        print(f"FAIL {name} {detail}")


def _install_stubs() -> None:
    if "noriflow_memory_pkg" in sys.modules:
        return
    core = types.ModuleType("core")
    logging_mod = types.ModuleType("core.logging_manager")
    logging_mod.get_logger = lambda *a, **k: logging.getLogger("stub")
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
    import importlib.util

    spec = importlib.util.spec_from_file_location(
        f"noriflow_memory_pkg.{mod_name}", path, **kwargs
    )
    mod = importlib.util.module_from_spec(spec)
    sys.modules[f"noriflow_memory_pkg.{mod_name}"] = mod
    spec.loader.exec_module(mod)
    return mod


def _vec(*values: float) -> list[float]:
    return list(values)


_UTC = timezone.utc


async def main_async() -> None:
    config_mod = _load("config")
    _load("alias_store")
    _load("entity_edge")
    db_pkg = _load("db")
    vector_ops = _load("vector_ops")

    LocalMemoryConfig = config_mod.LocalMemoryConfig
    SQLiteMemoryDatabase = db_pkg.SQLiteMemoryDatabase

    tmp = tempfile.TemporaryDirectory(prefix="noriflow_tools_")
    db_path = str(Path(tmp.name) / "memory.sqlite3")
    config = LocalMemoryConfig(
        storage_backend="sqlite",
        sqlite_path=db_path,
        embedding_dims=4,
    )
    db = SQLiteMemoryDatabase(config)
    await db.connect()
    await db.apply_migrations(PLUGIN_DIR / "migrations_sqlite")

    now = datetime.now(_UTC)

    async def _raw(text: str, uid: str = "u1", cat: str = "stable") -> int:
        occurred = now - timedelta(days=1)
        row_id = await db.upsert_persona_fact_raw_for_apply(
            document_id=f"doc-{text[:8]}-{uid}",
            platform="napcat", user_id=uid, related_user_ids=[],
            display_name="", category=cat, statement=text,
            confidence="high", session_id="s1", group_id="",
            evidence_key=f"s1|{occurred.strftime('%Y-%m-%d')}",
            occurred_at=occurred, embedding=_vec(1.0, 0.0, 0.0, 0.0),
        )
        assert row_id is not None
        return row_id

    async def _merge_kwargs(fact_id: int) -> dict:
        return dict(
            fact_id=fact_id,
            start_score=3.0, score_cap=10.0,
            promote_threshold=10.0, recent_promote_threshold=4.0,
        )

    # ---- memory_write 直写链：upsert → create 成簇 → replace 替代 ----
    fid1 = await _raw("小周不吃辣")
    out = await db.apply_fact_merge(action="create", **await _merge_kwargs(fid1))
    check("write_create_cluster", out.get("action") == "create" and out.get("cluster_id") > 0, str(out))
    cid1 = out["cluster_id"]

    fid2 = await _raw("小周其实能吃辣")
    out = await db.apply_fact_merge(
        action="replace", cluster_id=cid1, **await _merge_kwargs(fid2)
    )
    cid2 = out.get("cluster_id")
    check("write_replace_inherits", out.get("action") == "replace" and cid2 != cid1, str(out))
    owner = await db.fetch_cluster_owner(cid2)
    check("fetch_cluster_owner_hit", owner == ("napcat", "u1"), str(owner))
    check("fetch_cluster_owner_miss", await db.fetch_cluster_owner(999999) is None)

    # 同 document_id 重复 upsert：DO UPDATE 返回同一行 id（幂等闭环）
    again = await db.upsert_persona_fact_raw_for_apply(
        document_id=f"doc-小周不吃辣-u1",
        platform="napcat", user_id="u1", related_user_ids=[],
        display_name="", category="stable", statement="小周不吃辣",
        confidence="high", session_id="s1", group_id="",
        evidence_key="s1|x", occurred_at=now, embedding=_vec(1.0, 0.0, 0.0, 0.0),
    )
    check("upsert_conflict_returns_id", again == fid1, f"{again} vs {fid1}")

    # 冲突刷新不刷空 display_name（编码路径先解析出真名、工具路径同键
    # 直写恒传 ""——裸 SET 会把名字抹掉；传入非空名仍可刷新）
    async with db.pool.acquire() as conn:
        await conn.execute(
            "UPDATE memory_persona_fact_raw SET display_name = '小周'"
            " WHERE id = $1", fid1,
        )
    await db.upsert_persona_fact_raw_for_apply(
        document_id=f"doc-小周不吃辣-u1",
        platform="napcat", user_id="u1", related_user_ids=[],
        display_name="", category="stable", statement="小周不吃辣",
        confidence="high", session_id="s1", group_id="",
        evidence_key="s1|x", occurred_at=now, embedding=_vec(1.0, 0.0, 0.0, 0.0),
    )
    async with db.pool.acquire() as conn:
        row = await conn.fetchrow(
            "SELECT display_name FROM memory_persona_fact_raw WHERE id = $1",
            fid1,
        )
    check(
        "upsert_empty_name_keeps_existing",
        row["display_name"] == "小周",
        f"display_name={row['display_name']!r}（空串刷掉了既有名字）",
    )
    await db.upsert_persona_fact_raw_for_apply(
        document_id=f"doc-小周不吃辣-u1",
        platform="napcat", user_id="u1", related_user_ids=[],
        display_name="周老板", category="stable", statement="小周不吃辣",
        confidence="high", session_id="s1", group_id="",
        evidence_key="s1|x", occurred_at=now, embedding=_vec(1.0, 0.0, 0.0, 0.0),
    )
    async with db.pool.acquire() as conn:
        row = await conn.fetchrow(
            "SELECT display_name FROM memory_persona_fact_raw WHERE id = $1",
            fid1,
        )
    check(
        "upsert_real_name_refreshes",
        row["display_name"] == "周老板",
        f"display_name={row['display_name']!r}（非空名未刷新）",
    )

    # ---- cluster_status_op：drop/dispute/reactivate + 归属校验 ----
    r = await db.cluster_status_op(cid2, "drop", platform="napcat", user_id="u1")
    check("op_drop", r.get("changed") and r.get("status") == "dead", str(r))
    r = await db.cluster_status_op(cid2, "reactivate", platform="napcat", user_id="u1")
    check("op_reactivate", r.get("changed") and r.get("status") == "active", str(r))
    r = await db.cluster_status_op(cid2, "reactivate", platform="napcat", user_id="u1")
    check("op_reactivate_idempotent_reject", not r.get("changed") and r.get("status") == "active", str(r))
    # 归属不符与不存在同款（不泄露他人簇存在性）
    r = await db.cluster_status_op(cid2, "drop", platform="napcat", user_id="intruder")
    check("op_owner_mismatch_opaque", not r.get("changed") and r.get("status") == "", str(r))
    # replaced 簇不可复活：回读 replaced_by 指向继任
    r = await db.cluster_status_op(cid1, "reactivate", platform="napcat", user_id="u1")
    check(
        "op_replaced_points_successor",
        not r.get("changed") and r.get("status") == "replaced" and r.get("replaced_by") == cid2,
        str(r),
    )
    r = await db.cluster_status_op(cid2, "dispute", platform="napcat", user_id="u1")
    check("op_dispute", r.get("changed"), str(r))
    # 重复 dispute：与 PG 版 RETURNING 未命中分支同款——changed=False 不假报
    r = await db.cluster_status_op(cid2, "dispute", platform="napcat", user_id="u1")
    check(
        "op_dispute_repeat_unchanged",
        not r.get("changed") and r.get("status") == "active",
        str(r),
    )

    # ---- search_fact_clusters：状态过滤 + 归属/维度收窄 ----
    fid3 = await _raw("小周爱爬山", cat="preference")
    out = await db.apply_fact_merge(action="create", **await _merge_kwargs(fid3))
    cid3 = out["cluster_id"]
    rows = await db.search_fact_clusters(
        query_vec=_vec(1.0, 0.0, 0.0, 0.0), limit=10,
        platform="napcat", user_ids=["u1"],
    )
    check(
        "search_clusters_active_only",
        {r["status"] for r in rows} == {"active"} and len(rows) >= 1,
        str([(r["id"], r["status"]) for r in rows]),
    )
    rows_all = await db.search_fact_clusters(
        query_vec=_vec(1.0, 0.0, 0.0, 0.0), limit=10,
        platform="napcat", user_ids=["u1"], include_inactive=True,
    )
    statuses = {r["status"] for r in rows_all}
    check(
        "search_clusters_include_inactive",
        "replaced" in statuses and "active" in statuses,
        str(statuses),
    )
    rows_cat = await db.search_fact_clusters(
        query_vec=_vec(1.0, 0.0, 0.0, 0.0), limit=10,
        platform="napcat", user_ids=["u1"], category="preference",
    )
    check(
        "search_clusters_category_filter",
        len(rows_cat) == 1 and rows_cat[0]["id"] == cid3,
        str([(r["id"], r["category"]) for r in rows_cat]),
    )
    rows_other = await db.search_fact_clusters(
        query_vec=_vec(1.0, 0.0, 0.0, 0.0), limit=10,
        platform="napcat", user_ids=["someone-else"],
    )
    check("search_clusters_owner_scoped", rows_other == [], str(rows_other))
    # only_inactive：SQL 侧只留非激活簇（memory_lookup 深查下推——生效
    # 簇不进相似度候选窗，防固定倍数池假阴性）
    rows_mnt = await db.search_fact_clusters(
        query_vec=_vec(1.0, 0.0, 0.0, 0.0), limit=10,
        platform="napcat", user_ids=["u1"], only_inactive=True,
    )
    mnt_statuses = {r["status"] for r in rows_mnt}
    check(
        "search_clusters_only_inactive",
        mnt_statuses and mnt_statuses <= {"replaced", "dead",
                                          "pending_uncertain"},
        str(mnt_statuses),
    )

    # ---- kernel.write_fact 端到端（真 kernel + 真库）----
    kernel_mod = _load("memory_kernel")
    circuit_mod = _load("circuit_breaker")
    embedding = vector_ops.EmbeddingService(client=None, dims=4)
    breaker = circuit_mod.MemoryDBCircuitBreaker(
        failure_threshold=5, recovery_seconds=1.0
    )
    kernel = kernel_mod.LocalMemoryKernel(
        db=db, embedding_service=embedding,
        circuit_breaker=breaker, config=config, bot_id="bot01",
    )

    async def _fake_vec(_text):
        # client=None 的桩 EmbeddingService 本就 fail-open 返回 None；
        # 注入确定性向量让 create 路径走 embedded=True 分支
        return [0.1, 0.2, 0.3, 0.4]
    kernel.embedding_service.embed_one = _fake_vec
    summary = await kernel.write_fact(
        statement="小周养了一只橘猫", category="stable",
        platform="napcat", session_id="s1", group_id="", user_id="u1",
    )
    check(
        "kernel_write_fact_create",
        summary.get("action") == "create"
        and summary.get("cluster_id", 0) > 0
        and summary.get("embedded") is True,
        str(summary),
    )
    # 向量服务不可用：fail-open 落库（不拒绝——对话滑走即内容永丢），
    # embedded=False 标志交由工具层如实提示
    async def _no_vector(_text):
        return None
    kernel.embedding_service.embed_one = _no_vector
    summary = await kernel.write_fact(
        statement="小周不会骑自行车", category="stable",
        platform="napcat", session_id="s1", group_id="", user_id="u2",
    )
    check(
        "kernel_write_fact_embed_failopen",
        summary.get("action") == "create" and summary.get("embedded") is False,
        str(summary),
    )
    new_cid = summary["cluster_id"]
    summary = await kernel.write_fact(
        statement="小周的猫是奶牛猫", category="stable",
        platform="napcat", session_id="s1", group_id="", user_id="u1",
        replaces_cluster_id=new_cid,
    )
    check(
        "kernel_write_fact_replace",
        summary.get("action") == "replace"
        and summary.get("cluster_id") != new_cid
        and summary.get("score") is not None,
        str(summary),
    )
    # 幂等：同语句同日重复写入 → 乐观锁 skipped
    summary = await kernel.write_fact(
        statement="小周的猫是奶牛猫", category="stable",
        platform="napcat", session_id="s1", group_id="", user_id="u1",
    )
    check(
        "kernel_write_fact_idempotent",
        summary.get("action") in ("skipped", "create"),
        str(summary),
    )

    # ---- 摘要生命周期 ----
    async def _insert_summary(doc_id: str, written_days_ago: float, recalled_days_ago: float | None):
        await db.insert_chat_summary(
            document_id=doc_id, kind="chat_summary", platform="napcat",
            session_id="s1", group_id="s1", user_id="u1",
            participants=["napcat:u1"], content=f"内容-{doc_id}",
            occurred_at=now - timedelta(days=written_days_ago),
            embedding=_vec(0.9, 0.1, 0.0, 0.0), summarized=True,
        )
        async with db.pool.acquire() as conn:
            await conn.execute(
                "UPDATE memory_chat_summary SET written_at = $2"
                " WHERE document_id = $1",
                doc_id,
                db_pkg.sqlite_format_ts(now - timedelta(days=written_days_ago)),
            )
            if recalled_days_ago is not None:
                await conn.execute(
                    "UPDATE memory_chat_summary SET last_recall_at = $2"
                    " WHERE document_id = $1",
                    doc_id,
                    db_pkg.sqlite_format_ts(now - timedelta(days=recalled_days_ago)),
                )

    await _insert_summary("old-stale", 400, None)          # 超龄且从未召回 → 归档
    await _insert_summary("old-hot", 400, 5)               # 超龄但近期召回 → 豁免
    await _insert_summary("fresh", 10, None)               # 最低保留期内 → 豁免

    archived = await db.archive_stale_summaries(
        archive_after_days=300, reinforce_window_days=90, batch_size=2,
    )
    check("archive_pass_stale_only", archived == 1, str(archived))

    # 归档行结构性退出召回；include_archived 维护通道带回标志列
    rows = await db.search_chat_summaries(
        query_vec=_vec(0.9, 0.1, 0.0, 0.0), limit=10,
        scope="session", session_id="s1", platform="napcat",
    )
    docs = {r["document_id"] for r in rows}
    check(
        "recall_excludes_archived",
        "old-stale" not in docs and "old-hot" in docs,
        str(docs),
    )
    rows_mnt = await db.search_chat_summaries(
        query_vec=_vec(0.9, 0.1, 0.0, 0.0), limit=10,
        scope="session", session_id="s1", platform="napcat",
        include_archived=True,
    )
    by_doc = {r["document_id"]: r for r in rows_mnt}
    check(
        "include_archived_returns_flag",
        "old-stale" in by_doc and bool(by_doc["old-stale"].get("archived")),
        str(sorted(by_doc)),
    )
    # only_archived：SQL 侧只留归档行（memory_lookup 深查下推）
    rows_only = await db.search_chat_summaries(
        query_vec=_vec(0.9, 0.1, 0.0, 0.0), limit=10,
        scope="session", session_id="s1", platform="napcat",
        only_archived=True,
    )
    check(
        "only_archived_filters_active",
        {r["document_id"] for r in rows_only} == {"old-stale"},
        str(sorted(r["document_id"] for r in rows_only)),
    )

    # restore_summary：归属校验 + 恢复后重新进入召回
    async def _row_id(doc_id: str) -> int:
        async with db.pool.acquire() as conn:
            row = await conn.fetchrow(
                "SELECT id FROM memory_chat_summary WHERE document_id = $1",
                doc_id,
            )
        return int(row["id"])

    stale_id = await _row_id("old-stale")
    check(
        "restore_owner_mismatch",
        await db.restore_summary(stale_id, "other-session") is False,
    )
    check("restore_hit", await db.restore_summary(stale_id, "s1") is True)
    check("restore_already_active", await db.restore_summary(stale_id, "s1") is False)
    rows = await db.search_chat_summaries(
        query_vec=_vec(0.9, 0.1, 0.0, 0.0), limit=10,
        scope="session", session_id="s1", platform="napcat",
    )
    check(
        "restored_reenters_recall",
        "old-stale" in {r["document_id"] for r in rows},
    )

    # reinforce_summaries：续命 + 计数；随后归档遍豁免
    await _insert_summary("old-cold", 400, 200)            # 超龄且强化早过期
    await db.reinforce_summaries(["old-cold"])
    async with db.pool.acquire() as conn:
        row = await conn.fetchrow(
            "SELECT recall_count FROM memory_chat_summary"
            " WHERE document_id = 'old-cold'"
        )
    check("reinforce_counts", int(row["recall_count"]) == 1, str(dict(row)))
    archived = await db.archive_stale_summaries(
        archive_after_days=300, reinforce_window_days=90, batch_size=2,
    )
    check("reinforce_exempts_archive", archived == 0, str(archived))

    # keyset 分页：多批全量排干（batch_size=1 强制分页）
    for i in range(3):
        await _insert_summary(f"bulk-{i}", 400, None)
    archived = await db.archive_stale_summaries(
        archive_after_days=300, reinforce_window_days=90, batch_size=1,
    )
    check("archive_keyset_batches", archived == 3, str(archived))

    # 判据全下推 SQL（候选 SELECT + UPDATE 复查双防线，对齐 PG 版与
    # idx_mcs_lifecycle partial index）：未超龄行必须在候选层即被排除，
    # UPDATE 复查继续兜候选快照与落地之间的并发改写
    await _insert_summary("young-guard", 10, None)
    archived = await db.archive_stale_summaries(
        archive_after_days=300, reinforce_window_days=90, batch_size=100,
    )
    async with db.pool.acquire() as conn:
        row = await conn.fetchrow(
            "SELECT archived FROM memory_chat_summary"
            " WHERE document_id = 'young-guard'"
        )
    check(
        "archive_update_rechecks_written_at",
        int(row["archived"]) == 0,
        f"archived={row['archived']} (SQL 谓词未拦住未超龄行)",
    )

    await db.close()
    tmp.cleanup()


def main() -> None:
    logging.basicConfig(level=logging.WARNING)
    asyncio.run(main_async())
    print(f"{PASS}/{PASS + _FAIL} checks passed")
    if _FAIL:
        sys.exit(1)


if __name__ == "__main__":
    main()
