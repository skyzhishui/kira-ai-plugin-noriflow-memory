"""SQLite 后端真库集成 harness（python 直跑，无 pytest 依赖）。

双存储后端方案 §9.2 验收：tempfile 起真 SQLite 库，覆盖
- 四条收敛管线：embedding 补算 / search_text 补算 / 补编码遍消费
  summarized=false / pending facts 入簇（含 merge/replace/衰减/晋档）；
- recall 双路（向量 + FTS5 BM25）RRF 融合、近时排除、黑名单、行排除；
- alias / edge 幂等（evidence 去重、bot 边双证据激活、反向回声、墓碑）；
- WebUI 数据层（pg/sqlite 双方言派发的 sqlite 路全函数）。

PG 侧行为回归由既有桩测试覆盖（拆包零变化）；真机 live_check 保持
postgres 路径。本 harness 首次让集成测试不需要 PG 服务。

运行（插件目录）：
    python tests/test_sqlite_backend.py
"""

from __future__ import annotations

import asyncio
import importlib.util
import json
import logging
import sys
import tempfile
import time
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

    logging_mod.get_logger = lambda *args, **kwargs: logging.getLogger("stub")

    sys.modules["core"] = core
    sys.modules["core.logging_manager"] = logging_mod

    pkg = types.ModuleType("noriflow_memory_pkg")
    pkg.__path__ = [str(PLUGIN_DIR)]
    sys.modules["noriflow_memory_pkg"] = pkg

    fastapi = types.ModuleType("fastapi")

    class HTTPException(Exception):
        def __init__(self, status_code, detail=""):
            self.status_code = status_code
            self.detail = detail
            super().__init__(f"{status_code}: {detail}")

    fastapi.HTTPException = HTTPException
    sys.modules["fastapi"] = fastapi


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


def _vec(*values: float) -> list[float]:
    return list(values)


# ---------------------------------------------------------------------------
#  用例
# ---------------------------------------------------------------------------


async def main_async() -> None:
    config_mod = _load("config")
    _load("alias_store")
    _load("entity_edge")
    db_pkg = _load("db")
    webui = _load("webui_store")

    LocalMemoryConfig = config_mod.LocalMemoryConfig
    SQLiteMemoryDatabase = db_pkg.SQLiteMemoryDatabase

    tmp = tempfile.TemporaryDirectory(prefix="noriflow_sqlite_")
    db_path = str(Path(tmp.name) / "memory.sqlite3")
    config = LocalMemoryConfig(
        storage_backend="sqlite",
        sqlite_path=db_path,
        embedding_dims=4,
    )
    check(
        "factory_creates_sqlite_backend",
        type(db_pkg.create_backend(config)).__name__ == "SQLiteMemoryDatabase",
    )

    db = SQLiteMemoryDatabase(config)
    await db.connect()
    applied = await db.apply_migrations(PLUGIN_DIR / "migrations_sqlite")
    check("migrations_applied", applied == [1], str(applied))
    applied_again = await db.apply_migrations(PLUGIN_DIR / "migrations_sqlite")
    check("migrations_idempotent", applied_again == [])

    now = datetime(2026, 9, 12, 12, 0, 0, tzinfo=timezone.utc)

    # ---- 写入链路：摘要 + 幂等 ----
    await db.insert_chat_summary(
        document_id="10086-aaaa1111",
        kind="chat_summary", platform="qq", session_id="10086",
        group_id="10086", user_id="u1", participants=["qq:u1", "qq:u2"],
        content="小明说他最喜欢玩塞尔达传说，每天晚上都玩两个小时",
        occurred_at=now, embedding=_vec(1.0, 0.0, 0.0, 0.0), summarized=True,
    )
    await db.insert_chat_summary(
        document_id="10086-aaaa1111",  # 同幂等键重放
        kind="chat_summary", platform="qq", session_id="10086",
        group_id="10086", user_id="u1", participants=["qq:u1"],
        content="重复内容", occurred_at=now, embedding=None, summarized=True,
    )
    await db.insert_chat_summary(
        document_id="10086-bbbb2222",
        kind="chat_summary", platform="qq", session_id="10086",
        group_id="10086", user_id="u1", participants=["qq:u1"],
        content="小红的猫叫团子，是一只橘猫",
        occurred_at=now + timedelta(minutes=5),
        embedding=_vec(0.0, 1.0, 0.0, 0.0), summarized=True,
    )
    # 降级原文行（summarized=false，待补编码遍消费）
    await db.insert_chat_summary(
        document_id="10086-cccc3333",
        kind="chat_summary", platform="qq", session_id="10086",
        group_id="10086", user_id="u1", participants=["qq:u1"],
        content="<msg ts=\"2026-09-12 11:59:00\" uid=\"u1\">以上为历史上下文</msg>",
        occurred_at=now + timedelta(minutes=6), embedding=None,
        summarized=False,
    )
    row = await db.pool.conn.fetchval(
        "SELECT count(*) FROM memory_chat_summary"
    )
    check("summary_idempotent", int(row) == 3, str(row))

    # ---- 收敛管线 1：embedding 补算 ----
    missing = await db.fetch_missing_embeddings("memory_chat_summary", 10)
    check("backfill_scan_finds_null", [m[0] for m in missing] == [3],
          str(missing))
    await db.update_embedding("memory_chat_summary", 3, _vec(0.0, 0.0, 1.0, 0.0))
    missing = await db.fetch_missing_embeddings("memory_chat_summary", 10)
    check("backfill_converged", missing == [])

    # ---- 收敛管线 2：search_text 补算（写入点已全落，扫应为空）----
    no_st = await db.fetch_missing_search_text(10)
    check("search_text_converged", no_st == [], str(no_st))

    # ---- recall：双路融合 + 近时排除 + 黑名单 + 行排除 ----
    hits = await db.search_chat_summaries(
        query_vec=_vec(1.0, 0.0, 0.0, 0.0), limit=5,
        scope="session", session_id="10086", platform="qq",
    )
    check(
        "recall_vec_top1",
        hits and hits[0]["document_id"] == "10086-aaaa1111",
        str([h["document_id"] for h in hits]),
    )
    check(
        "recall_relevance_is_cosine",
        abs(hits[0]["relevance"] - 1.0) < 1e-6, str(hits[0]["relevance"]),
    )
    check(
        "recall_occurred_at_decoded",
        hits[0]["occurred_at"].tzinfo is not None
        and hits[0]["occurred_at"].year == 2026,
    )

    hybrid = await db.search_chat_summaries(
        query_vec=_vec(1.0, 0.0, 0.0, 0.0), limit=5,
        scope="session", session_id="10086", platform="qq",
        query_text="小红的猫团子", hybrid=True,
    )
    check(
        "recall_hybrid_bm25_rare_first",
        hybrid[0]["document_id"] == "10086-bbbb2222",
        str([(h["document_id"], round(h.get("rrf", 0), 4)) for h in hybrid]),
    )

    excl = await db.search_chat_summaries(
        query_vec=_vec(1.0, 0.0, 0.0, 0.0), limit=5,
        scope="session", session_id="10086", platform="qq",
        exclude_recent_batches=1,
    )
    check(
        "recall_exclude_recent",
        "10086-bbbb2222" not in [h["document_id"] for h in excl]
        and "10086-aaaa1111" in [h["document_id"] for h in excl],
        str([h["document_id"] for h in excl]),
    )

    black = await db.search_chat_summaries(
        query_vec=_vec(1.0, 0.0, 0.0, 0.0), limit=5,
        scope="session", session_id="10086", platform="qq",
        exclude_content_keywords=["塞尔达"],
    )
    check(
        "recall_topic_blacklist",
        "10086-aaaa1111" not in [h["document_id"] for h in black],
    )

    rowex = await db.search_chat_summaries(
        query_vec=_vec(1.0, 0.0, 0.0, 0.0), limit=5,
        scope="session", session_id="10086", platform="qq",
        exclude_document_ids=["10086-aaaa1111"],
    )
    check(
        "recall_row_exclude",
        "10086-aaaa1111" not in [h["document_id"] for h in rowex],
    )

    # 写入侧近重去重的打分路径
    scores = await db.fetch_recent_summary_scores(
        query_vec=_vec(1.0, 0.0, 0.0, 0.0), session_id="10086", limit=2,
        platform="qq",
    )
    check(
        "write_dedup_scores",
        len(scores) == 2
        and [r["id"] for r in scores] == [2, 1]  # occurred_at 倒序（非分数序）
        and abs(scores[0]["score"] - 0.0) < 1e-6
        and abs(scores[1]["score"] - 1.0) < 1e-6,
        str(scores),
    )

    # ---- 收敛管线 3：补编码遍消费 summarized=false ----
    pending_rows = await db.fetch_unsummarized_summaries(10)
    check(
        "reencode_scan_finds_raw",
        len(pending_rows) == 1 and pending_rows[0]["id"] == 3,
        str(pending_rows),
    )
    check(
        "reencode_row_shape",
        pending_rows[0]["participants"] == ["qq:u1"]
        and pending_rows[0]["occurred_at"].tzinfo is not None,
    )
    await db.update_chat_summary_encoded(
        3, "小明深夜在群里聊了历史话题", _vec(0.0, 0.0, 0.0, 1.0),
    )
    pending_rows = await db.fetch_unsummarized_summaries(10)
    check("reencode_converged", pending_rows == [])
    hits = await db.search_chat_summaries(
        query_vec=_vec(0.0, 0.0, 0.0, 1.0), limit=1,
        scope="session", session_id="10086", platform="qq",
    )
    check(
        "reencoded_row_recallable",
        hits and hits[0]["document_id"] == "10086-cccc3333",
    )

    # ---- 参与者扩选 / 滚动补回 ----
    parts = await db.fetch_recent_session_participants(
        session_id="10086", limit=10, platform="qq"
    )
    flat = sorted({p for r in parts for p in r["participants"]})
    check("expansion_participants", flat == ["qq:u1", "qq:u2"], str(flat))

    rollout = await db.fetch_recent_rollout_summaries(
        session_id="10086", skip_batches=2, limit=5, platform="qq"
    )
    check(
        "rollout_skips_window",
        [r["document_id"] for r in rollout] == ["10086-aaaa1111"],
        str([r["document_id"] for r in rollout]),
    )

    # ---- 收敛管线 4：pending facts 入簇（create/merge/replace）----
    await db.insert_persona_fact_raw(
        document_id="u1@10086|2026-09-12-d1",
        platform="qq", user_id="u1", related_user_ids=["u2"],
        display_name="小明", category="stable", statement="小明喜欢玩塞尔达",
        confidence="high", session_id="10086", group_id="10086",
        evidence_key="10086|2026-09-12", occurred_at=now,
        embedding=_vec(1.0, 0.0, 0.0, 0.0),
    )
    await db.insert_persona_fact_raw(
        document_id="u1@10086|2026-09-12-d1",  # 幂等键重放
        platform="qq", user_id="u1", related_user_ids=["u2"],
        display_name="小明", category="stable", statement="重复事实",
        confidence="high", session_id="10086", group_id="10086",
        evidence_key="10086|2026-09-12", occurred_at=now,
        embedding=None,
    )
    facts = await db.fetch_pending_facts(10)
    check("fact_idempotent", len(facts) == 1, str(len(facts)))
    check(
        "fact_row_shape",
        facts[0]["related_user_ids"] == ["u2"]
        and facts[0]["embedding"] is not None
        and facts[0]["occurred_at"].tzinfo is not None,
    )

    fact_id = facts[0]["id"]
    out = await db.apply_fact_merge(
        fact_id=fact_id, action="create", start_score=3.0, score_cap=10.0,
        promote_threshold=10.0, recent_promote_threshold=4.0,
    )
    check(
        "merge_create",
        out["action"] == "create" and out["score"] == 3.0, str(out),
    )
    cluster_id = out["cluster_id"]

    # 同一 fact 重放：乐观锁翻标志失败 -> skipped（与 PG 同语义）
    out = await db.apply_fact_merge(
        fact_id=fact_id, action="merge", cluster_id=cluster_id,
        evidence_key="10086|2026-09-13", start_score=3.0, score_cap=10.0,
        promote_threshold=10.0, recent_promote_threshold=4.0,
        occurred_at=now + timedelta(days=1),
    )
    check("merge_same_fact_skipped", out == {"action": "skipped"}, str(out))

    async def _insert_pending(doc_suffix, statement, evidence_key, embedding):
        await db.insert_persona_fact_raw(
            document_id=f"u1@10086|2026-09-12-{doc_suffix}",
            platform="qq", user_id="u1", related_user_ids=[],
            display_name="小明", category="stable", statement=statement,
            confidence="high", session_id="10086", group_id="10086",
            evidence_key=evidence_key, occurred_at=now,
            embedding=embedding,
        )
        rows = await db.fetch_pending_facts(10)
        return next(r["id"] for r in rows if r["document_id"].endswith(doc_suffix))

    # 新事实 + 新证据键 -> 计分 +1
    fid = await _insert_pending(
        "m1", "小明沉迷塞尔达", "10086|2026-09-13", _vec(1.0, 0.0, 0.0, 0.0),
    )
    out = await db.apply_fact_merge(
        fact_id=fid, action="merge", cluster_id=cluster_id,
        evidence_key="10086|2026-09-13", start_score=3.0, score_cap=10.0,
        promote_threshold=10.0, recent_promote_threshold=4.0,
        occurred_at=now + timedelta(days=1),
    )
    check(
        "merge_scores", out["score"] == 6.0 and out["status"] == "active",
        str(out),
    )
    # 新事实 + 重复证据键 -> 去重不计分
    fid = await _insert_pending(
        "m2", "小明爱玩塞尔达", "10086|2026-09-13", _vec(1.0, 0.0, 0.0, 0.0),
    )
    out = await db.apply_fact_merge(
        fact_id=fid, action="merge", cluster_id=cluster_id,
        evidence_key="10086|2026-09-13", start_score=3.0, score_cap=10.0,
        promote_threshold=10.0, recent_promote_threshold=4.0,
        occurred_at=now + timedelta(days=1),
    )
    check("merge_evidence_key_no_double_count", out["score"] == 6.0, str(out))
    fid = await _insert_pending(
        "m3", "小明每天玩塞尔达", "10086|2026-09-14", _vec(1.0, 0.0, 0.0, 0.0),
    )
    out = await db.apply_fact_merge(
        fact_id=fid, action="merge", cluster_id=cluster_id,
        evidence_key="10086|2026-09-14", start_score=3.0, score_cap=10.0,
        promote_threshold=10.0, recent_promote_threshold=4.0,
        occurred_at=now + timedelta(days=2),
    )
    check("merge_score_cap", out["score"] == 9.0, str(out))

    # 晋档遍
    promoted = await db.promote_pass(
        promote_threshold=9.0, recent_promote_threshold=4.0
    )
    check("promote_pass", promoted == 1, str(promoted))
    sections = await db.fetch_profile_sections("qq", "u1", 5)
    check(
        "profile_sections",
        sections.get("stable") == ["小明喜欢玩塞尔达"], str(sections),
    )
    name = await db.fetch_latest_display_name("qq", "u1")
    check("display_name", name == "小明", str(name))

    # 簇候选检索（向量 + 标量退化）
    cands = await db.search_cluster_candidates(
        platform="qq", user_id="u2", category="stable", related_user_ids=[],
        embedding=_vec(1.0, 0.0, 0.0, 0.0), top_k=5,
    )
    check(
        "cluster_candidates_related_hit",
        cands and cands[0]["id"] == cluster_id
        and abs(cands[0]["similarity"] - 1.0) < 1e-6, str(cands),
    )
    cands = await db.search_cluster_candidates(
        platform="qq", user_id="u1", category="stable",
        embedding=None, top_k=5,
    )
    check(
        "cluster_candidates_scalar_fallback",
        cands and cands[0]["id"] == cluster_id,
    )

    # replace：更正陈述，旧簇半分墓碑，画像投影切换
    await db.insert_persona_fact_raw(
        document_id="u1@10086|2026-09-12-d2",
        platform="qq", user_id="u1", related_user_ids=[],
        display_name="小明", category="stable", statement="小明其实更喜欢空洞骑士",
        confidence="high", session_id="10086", group_id="10086",
        evidence_key="10086|2026-09-15", occurred_at=now,
        embedding=_vec(0.9, 0.1, 0.0, 0.0),
    )
    new_fact_id = await _insert_pending(
        "d2", "小明其实更喜欢空洞骑士", "10086|2026-09-15",
        _vec(0.9, 0.1, 0.0, 0.0),
    )
    out = await db.apply_fact_merge(
        fact_id=new_fact_id, action="replace", cluster_id=cluster_id,
        evidence_key="10086|2026-09-15", start_score=3.0, score_cap=10.0,
        promote_threshold=9.0, recent_promote_threshold=4.0,
        occurred_at=now,
    )
    check(
        "merge_replace",
        out["action"] == "replace"
        and out["replaced_cluster_id"] == cluster_id
        and out["status"] == "profiled", str(out),
    )
    new_cluster = out["cluster_id"]
    sections = await db.fetch_profile_sections("qq", "u1", 5)
    check(
        "replace_profile_projection",
        sections.get("stable") == ["小明其实更喜欢空洞骑士"], str(sections),
    )

    # 衰减遍（缺席冻结 + 证据地板 + 死亡窗口）
    stats = await db.decay_pass(
        decay_factor=0.8, demote_threshold=3.0, pending_dead_days=90,
        recent_expire_days=30, activity_since=now - timedelta(days=1),
        sticky_evidence_count=2, anchor_profile_size=0,
    )
    check(
        "decay_pass_stats",
        set(stats) == {
            "expired_recent", "demoted", "profile_rows_deleted", "deaded",
        }, str(stats),
    )

    # kv
    await db.set_kv("decay_last_run", "2026-09-12T12:00:00.000000Z")
    check(
        "kv_roundtrip",
        await db.get_kv("decay_last_run") == "2026-09-12T12:00:00.000000Z",
    )

    # ---- alias 层 ----
    await db.alias_upsert([
        {"platform": "qq", "user_id": "u1", "name": "小明",
         "last_seen": now, "source": "batch"},
        {"platform": "qq", "user_id": "u2", "name": "小红",
         "last_seen": now + timedelta(minutes=1), "source": "batch"},
    ])
    await db.alias_upsert([  # 更晚 last_seen 翻转 source；更早重放不回退
        {"platform": "qq", "user_id": "u1", "name": "明哥",
         "last_seen": now + timedelta(minutes=2), "source": "reconcile"},
        {"platform": "qq", "user_id": "u2", "name": "小红",
         "last_seen": now - timedelta(minutes=1), "source": "backfill"},
    ])
    alias_all = await db.alias_fetch_all()
    check("alias_upsert_idempotent", len(alias_all) == 3, str(alias_all))
    by_owner = await db.fetch_alias_names_by_owner([("qq", "u2"), ("qq", "u9")])
    check(
        "alias_names_by_owner", by_owner.get(("qq", "u2")) == "小红",
        str(by_owner),
    )
    directory = await db.fetch_alias_directory()
    check("alias_directory", len(directory) == 3)

    # ---- 关系边：幂等/双证据/反向回声/墓碑 ----
    edge_row = {
        "platform": "qq", "subject_uid": "u1", "object_uid": "u2",
        "subject_name": "小明", "object_name": "小红",
        "relation_label": "室友", "statement": "小明的室友是小红",
        "confidence": "high", "occurred_at": now,
        "evidence_key": "10086|2026-09-12", "is_bot_edge": False,
        "min_evidence": 2,
    }
    await db.upsert_entity_edge([dict(edge_row)])
    await db.upsert_entity_edge([dict(edge_row)])  # 同键重放不涨计数
    edges = await db.pool.conn.fetch(
        "SELECT evidence_count, status FROM memory_entity_edge"
    )
    check(
        "edge_idempotent",
        len(edges) == 1 and int(edges[0]["evidence_count"]) == 1
        and edges[0]["status"] == "active",
        str([dict(e) for e in edges]),
    )

    bot_row = dict(edge_row)
    bot_row.update(
        subject_uid="bot", object_uid="u1", subject_name="Kira",
        relation_label="朋友", is_bot_edge=True, min_evidence=2,
        evidence_key="10086|2026-09-12", statement="Kira和小明是朋友",
    )
    await db.upsert_entity_edge([dict(bot_row)])
    edges = await db.pool.conn.fetch(
        "SELECT status, evidence_count FROM memory_entity_edge "
        "WHERE relation_label = '朋友'"
    )
    check(
        "bot_edge_pending_first",
        edges[0]["status"] == "pending"
        and int(edges[0]["evidence_count"]) == 1,
    )
    bot_row2 = dict(bot_row)
    bot_row2["evidence_key"] = "10086|2026-09-16"
    await db.upsert_entity_edge([bot_row2])
    edges = await db.pool.conn.fetch(
        "SELECT status, evidence_count FROM memory_entity_edge "
        "WHERE relation_label = '朋友'"
    )
    check(
        "bot_edge_double_evidence_activates",
        edges[0]["status"] == "active"
        and int(edges[0]["evidence_count"]) == 2,
    )

    active = await db.fetch_active_edges(["qq:u1"], ["qq:bot"])
    check(
        "active_edges_by_node_keys",
        {e["relation_label"] for e in active} == {"室友", "朋友"},
        str([e["relation_label"] for e in active]),
    )
    active = await db.fetch_active_edges(["qq:u1"], "")
    check(
        "active_edges_node_keys_only",
        {e["relation_label"] for e in active} == {"室友", "朋友"},
        str([e["relation_label"] for e in active]),
    )

    # 反向回声：镜像方向已在库，反向新边整行跳过
    echo = dict(edge_row)
    echo.update(
        subject_uid="u2", object_uid="u1", subject_name="小红",
        object_name="小明", statement="小红的室友是小明",
        evidence_key="10086|2026-09-17",
    )
    await db.upsert_entity_edge([echo])
    cnt = await db.pool.conn.fetchval(
        "SELECT count(*) FROM memory_entity_edge WHERE relation_label='室友'"
    )
    check("reverse_echo_skipped", int(cnt) == 1, str(cnt))

    report = await db.relation_integrity_report(pending_stale_days=14)
    check(
        "integrity_report_shape",
        report["evidence_mismatch"] == 0
        and report["placeholder_names"] == 0
        and report["bad_labels"] == 0, str(report),
    )

    audit_rows = await db.fetch_edges_for_audit(0, 10)
    check(
        "edges_for_audit",
        len(audit_rows) == 2 and audit_rows[0]["id"] < audit_rows[1]["id"],
    )
    n = await db.supersede_edges([audit_rows[0]["id"]])
    check("supersede_edges", n == 1)
    n = await db.supersede_edges([audit_rows[0]["id"]])
    check("supersede_edges_idempotent", n == 0)

    sources = await db.fetch_confirmed_relation_sources(0, 10)
    check(
        "confirmed_sources",
        len(sources) == 1 and sources[0]["id"] == new_cluster, str(sources),
    )
    total = await db.count_confirmed_relation_sources(0, exclude_user_id="u9")
    check("confirmed_sources_count", total == 1, str(total))
    by_ids = await db.fetch_confirmed_relation_sources_by_ids([new_cluster])
    check("confirmed_sources_by_ids", len(by_ids) == 1)

    # ---- WebUI 数据层（sqlite 方言派发全函数）----
    # 装配契约回归锁：API 层（main._pool_or_503）必须传 backend 而非裸池
    # ——webui_store 按 backend.dialect 派发方言，裸池会被兜底成 postgres，
    # 把 PG 方言 SQL 打进 SQLite（上线日事故：unrecognized token ":"）
    check(
        "webui_dialect_dispatch_contract",
        webui._dialect(db) == "sqlite"
        and webui._dialect(db.pool) == "postgres"
        and webui._pool(db) is db.pool,
    )
    overview = await webui.fetch_overview(db)
    check(
        "webui_overview",
        overview["summaries"] == 3 and overview["clusters"] == 2
        and overview["users_with_clusters"] == 1, str(overview),
    )

    users = await webui.fetch_users(db, keyword="", page=1, size=10)
    check(
        "webui_users",
        users["total"] == 1 and users["items"][0]["user_id"] == "u1"
        and users["items"][0]["display_name"] == "小明", str(users),
    )
    users = await webui.fetch_users(db, keyword="u1", page=1, size=10)
    check("webui_users_keyword_like", users["total"] == 1)
    users = await webui.fetch_users(db, keyword="u9", page=1, size=10)
    check("webui_users_keyword_miss", users["total"] == 0)

    facts_page = await webui.fetch_facts(db, 1, 10, q="塞尔达")
    check(
        "webui_facts_q",
        facts_page["total"] == 4
        and any(
            i["related_user_ids"] == ["u2"] for i in facts_page["items"]
        ),
        str(facts_page["total"]),
    )

    clusters = await webui.fetch_clusters(db, 1, 10, status="replaced")
    check(
        "webui_clusters_status_filter",
        clusters["total"] == 1
        and clusters["items"][0]["replaced_by"] == new_cluster,
        str(clusters["total"]),
    )
    clusters = await webui.fetch_clusters(db, 1, 10, session_id="10086")
    check(
        "webui_clusters_session_filter", clusters["total"] == 2,
        str(clusters["total"]),
    )

    out = await webui.update_cluster(db, new_cluster, {
        "canonical_statement": "小明最喜欢的是空洞骑士",
        "score": 9.5, "status": "profiled",
    })
    check(
        "webui_update_cluster",
        out["updated"] and out["statement_changed"], str(out),
    )
    sections = await db.fetch_profile_sections("qq", "u1", 5)
    check(
        "webui_update_cluster_projection",
        sections.get("stable") == ["小明最喜欢的是空洞骑士"], str(sections),
    )

    summaries = await webui.fetch_summaries(db, 1, 10, q="猫")
    check(
        "webui_summaries_q",
        summaries["total"] == 1
        and summaries["items"][0]["participants"] == ["qq:u1"],
        str(summaries["total"]),
    )

    out = await webui.update_summary(db, 2, {"content": "小红的猫团子已三岁"})
    check("webui_update_summary", out["updated"])
    fts_hit = await db.pool.conn.fetch(
        "SELECT summary_id FROM memory_chat_summary_fts "
        "WHERE memory_chat_summary_fts MATCH '团子'"
    )
    check(
        "webui_update_summary_fts_sync",
        [int(r["summary_id"]) for r in fts_hit] == [2], str(fts_hit),
    )
    # embedding 置 NULL 后该行退出召回（与 PG 同语义）；补算回填后
    # hybrid 召回命中的是新正文
    await db.update_embedding("memory_chat_summary", 2, _vec(0.0, 1.0, 0.0, 0.0))
    hits = await db.search_chat_summaries(
        query_vec=_vec(0.0, 1.0, 0.0, 0.0), limit=5,
        scope="session", session_id="10086", platform="qq",
        query_text="团子三岁", hybrid=True,
    )
    check(
        "webui_update_summary_recall_new_content",
        hits and hits[0]["document_id"] == "10086-bbbb2222"
        and "三岁" in hits[0]["content"],
        str([(h["document_id"], h["content"][:20]) for h in hits]),
    )

    out = await webui.delete_summary(db, 2)
    check("webui_delete_summary", out["deleted"])
    left = await db.pool.conn.fetchval(
        "SELECT count(*) FROM memory_chat_summary_fts WHERE summary_id = 2"
    )
    check("webui_delete_summary_fts_cleanup", int(left) == 0, str(left))

    graph = await webui.fetch_relation_graph(db, bot_user_id="bot")
    check(
        "webui_relation_graph",
        graph["stats"]["edges"] == 2
        and graph["stats"]["edges_superseded"] == 1
        and any(n["is_bot"] for n in graph["nodes"]), str(graph["stats"]),
    )

    out = await webui.update_relation_edge(
        db, graph["edges"][-1]["id"], {"status": "active"}
    )
    check(
        "webui_update_edge", out["updated"] and out["status"] == "active",
        str(out),
    )

    del_fact_id = await _insert_pending(
        "w1", "待删除事实", "10086|2026-09-18", None,
    )
    out = await webui.delete_fact(db, del_fact_id)
    check("webui_delete_fact", out["deleted"] and not out["was_extracted"])

    out = await webui.delete_user_memories(db, "qq", "u1")
    check(
        "webui_delete_user_memories",
        out["deleted"] and out["clusters"] == 2 and out["profiles"] == 1,
        str(out),
    )
    overview = await webui.fetch_overview(db)
    check(
        "webui_erase_converged",
        overview["clusters"] == 0 and overview["profile_rows"] == 0
        and overview["facts_raw"] == 0 and overview["facts_pending"] == 0,
        str(overview),
    )

    # delete_chat_summary（工具路径）+ 作用域 + FTS 清理
    ok = await db.delete_chat_summary(
        "10086-aaaa1111", scope_session_id="10086", scope_user_id="u1"
    )
    check("tool_delete_summary", ok is True)
    ok = await db.delete_chat_summary("10086-cccc3333", scope_user_id="other")
    check("tool_delete_scope_blocks", ok is False)

    # ---- v1.15.2 review 回归：BM25 where 感知填充 / promote 关联列 / ----
    # ---- decay 参与者门控 / 画像 related_user_ids 启动自修复          ----
    # 自建数据段（上方 webui 段已清空 u1 数据，计数断言不受影响）
    await db.insert_chat_summary(
        document_id="91086-p111",
        kind="chat_summary", platform="qq", session_id="91086",
        group_id="91086", user_id="u7", participants=["qq:u7", "qq:u8"],
        content="小明说他最喜欢玩塞尔达传说",
        occurred_at=now + timedelta(hours=1),
        embedding=_vec(1.0, 0.0, 0.0, 0.0), summarized=True,
    )
    await db.insert_chat_summary(
        document_id="91086-p222",
        kind="chat_summary", platform="qq", session_id="91086",
        group_id="91086", user_id="u7", participants=["qq:u7"],
        content="小红的猫叫团子，是一只橘猫",
        occurred_at=now + timedelta(hours=2),
        embedding=_vec(0.0, 1.0, 0.0, 0.0), summarized=True,
    )
    await db.insert_chat_summary(
        document_id="20001-decoy",
        kind="chat_summary", platform="qq", session_id="20001",
        group_id="20001", user_id="u9", participants=["qq:u9"],
        content="团子团子团子团子团子团子",  # FTS 词频远超 p222
        occurred_at=now + timedelta(hours=3),
        embedding=_vec(0.0, 0.0, 0.0, 1.0), summarized=True,
    )
    # BM25 路必须在 where（会话限定）之后取 top-limit：旧实现先取 FTS 全库
    # top-1（被 decoy 的词频霸占）再与 where 相交，BM25 路欠填为空
    fill = await db.search_chat_summaries(
        query_vec=_vec(1.0, 0.0, 0.0, 0.0), limit=1,
        scope="session", session_id="91086", platform="qq",
        query_text="团子", hybrid=True,
    )
    check(
        "regress_hybrid_bm25_where_fill",
        [h["document_id"] for h in fill] == ["91086-p111", "91086-p222"],
        str([(h["document_id"], round(h.get("rrf", 0), 4)) for h in fill]),
    )

    # promote_pass 画像行 related_user_ids 必须是合法 JSON 数组
    # （v1.15.1 及之前 raw JSON TEXT 被 _json_list 按字符迭代成字符数组）
    await db.insert_persona_fact_raw(
        document_id="u8@91086|2026-09-12-r1",
        platform="qq", user_id="u8", related_user_ids=["u2"],
        display_name="阿八", category="stable", statement="阿八喜欢爬山",
        confidence="high", session_id="91086", group_id="91086",
        evidence_key="91086|2026-09-12", occurred_at=now,
        embedding=None,
    )
    facts = await db.fetch_pending_facts(10)
    rid = next(
        r["id"] for r in facts
        if r["document_id"] == "u8@91086|2026-09-12-r1"
    )
    out = await db.apply_fact_merge(
        fact_id=rid, action="create",
        evidence_key="91086|2026-09-12", start_score=9.0, score_cap=10.0,
        promote_threshold=9.0, recent_promote_threshold=4.0,
        occurred_at=now,
    )
    check("regress_promote_create", out["action"] == "create", str(out))
    promoted = await db.promote_pass(
        promote_threshold=9.0, recent_promote_threshold=4.0
    )
    check("regress_promote_pass", promoted == 1, str(promoted))
    async with db.pool.acquire() as conn:
        prow = await conn.fetchrow(
            "SELECT id, related_user_ids FROM memory_user_profile "
            "WHERE cluster_id = $1",
            out["cluster_id"],
        )
    check(
        "regress_promote_related_ids_text",
        prow is not None and prow["related_user_ids"] == '["u2"]',
        str(dict(prow) if prow else None),
    )

    # decay 活跃度门控必须覆盖仅以参与者身份出现的用户（u8 只在
    # p111 的 participants 里，非任何批次的 user_id）——旧实现迭代
    # JSON TEXT 字符串，参与者键组全丢，u8 簇被错误冻结不衰减
    await db.decay_pass(
        decay_factor=0.8, demote_threshold=3.0, pending_dead_days=90,
        recent_expire_days=30, activity_since=now - timedelta(days=1),
    )
    async with db.pool.acquire() as conn:
        u8row = await conn.fetchrow(
            "SELECT score, status FROM memory_fact_cluster "
            "WHERE platform = 'qq' AND user_id = 'u8'"
        )
    check(
        "regress_decay_participant_gate",
        u8row is not None
        and abs(float(u8row["score"]) - 9.0 * 0.8) < 1e-9,
        str(dict(u8row) if u8row else None),
    )

    # 启动自修复：手工写入历史缺陷形态（字符数组），重跑迁移触发修复；
    # 合法形态（含全单字符 uid 的数组，拼接后非合法 JSON）不得误伤
    corrupted = json.dumps(list('["u2"]'), ensure_ascii=False)
    valid_ambiguous = '["a", "b"]'
    async with db.pool.acquire() as conn:
        await conn.execute(
            "UPDATE memory_user_profile SET related_user_ids = $1 "
            "WHERE id = $2",
            corrupted, int(prow["id"]),
        )
        await conn.execute(
            "INSERT INTO memory_user_profile (platform, user_id, category,"
            " cluster_id, statement, score, related_user_ids)"
            " VALUES ('qq', 'ux', 'stable', 99999, '歧义负样本', 1.0, $1)",
            valid_ambiguous,
        )
    applied_again = await db.apply_migrations(
        PLUGIN_DIR / "migrations_sqlite"
    )
    check("regress_repair_no_new_migrations", applied_again == [],
          str(applied_again))
    async with db.pool.acquire() as conn:
        fixed = await conn.fetchval(
            "SELECT related_user_ids FROM memory_user_profile WHERE id = $1",
            int(prow["id"]),
        )
        kept = await conn.fetchval(
            "SELECT related_user_ids FROM memory_user_profile "
            "WHERE cluster_id = 99999"
        )
    check(
        "regress_profile_related_ids_repaired",
        fixed == '["u2"]', str(fixed),
    )
    check(
        "regress_repair_keeps_valid_rows",
        kept == valid_ambiguous, str(kept),
    )

    await db.close()

    # ---- 持久化：close 后重开数据仍在 ----
    db2 = SQLiteMemoryDatabase(config)
    await db2.connect()
    await db2.apply_migrations(PLUGIN_DIR / "migrations_sqlite")
    left = await db2.get_kv("decay_last_run")
    check("reopen_persists", left == "2026-09-12T12:00:00.000000Z")
    await db2.close()

    # ---- migrate_backend: --force unique-key skip + dim verification ----
    await _migrate_backend_checks()

    tmp.cleanup()


# ---------------------------------------------------------------------------
#  migrate_backend: --force merge semantics (unique-key collision skip) +
#  sampled-dim verification. The PG source is stubbed (fetch/fetchval
#  dispatch on SQL shape, returning canned rows); the target is real SQLite.
# ---------------------------------------------------------------------------


def _emb_text(*values: float) -> str:
    return "[" + ",".join(repr(v) for v in values) + "]"


class _FakePG:
    """asyncpg connection stub: dispatches on SQL shape, returning canned source rows."""

    def __init__(self, tables: dict[str, list[dict]], kv_rows: list[dict]):
        self.tables = tables
        self.kv_rows = kv_rows
        self.closed = False

    async def fetch(self, sql: str, *args):
        if "_memory_local_kv" in sql:
            return self.kv_rows
        if sql.startswith("SELECT document_id FROM memory_chat_summary"):
            return [{"document_id": r["document_id"]}
                    for r in self.tables["memory_chat_summary"]]
        for name, rows in self.tables.items():
            if f"FROM {name} ORDER BY id" in sql:
                return rows
        raise AssertionError("unexpected pg fetch: " + sql[:100])

    async def fetchval(self, sql: str, *args):
        if "WHERE embedding IS NOT NULL ORDER BY id LIMIT 1" in sql:
            rows = self.tables["memory_chat_summary"]
            emb = next((r["embedding"] for r in rows
                        if r.get("embedding") is not None), None)
            return emb
        if "WHERE embedding IS NOT NULL" in sql:
            return sum(1 for r in self.tables["memory_chat_summary"]
                       if r.get("embedding") is not None)
        if "SELECT document_id FROM" in sql or "document_id = $1" in sql:
            return 1  # coverage check: every source doc counts as hit
        if "COALESCE(MAX(id), 0)" in sql:
            name = sql.split("FROM ")[1].split()[0]
            rows = self.tables.get(name, [])
            return max((int(r["id"]) for r in rows), default=0)
        if sql.startswith("SELECT count(*) FROM"):
            name = sql.split("FROM ")[1].split()[0]
            return len(self.tables.get(name, []))
        raise AssertionError("unexpected pg fetchval: " + sql[:100])

    async def close(self):
        self.closed = True


async def _migrate_backend_checks() -> None:
    mig = _load("migrate_backend")
    config_mod = _load("config")
    db_pkg = _load("db")
    LocalMemoryConfig = config_mod.LocalMemoryConfig
    SQLiteMemoryDatabase = db_pkg.SQLiteMemoryDatabase
    now = datetime(2026, 9, 12, 12, 0, 0, tzinfo=timezone.utc)

    with tempfile.TemporaryDirectory(prefix="noriflow_migrate_") as td:
        # Pre-seed the target with rows from "the service already ran on
        # sqlite" (partially colliding with the source)
        target_path = str(Path(td) / "target.sqlite3")
        tgt_cfg = LocalMemoryConfig(
            storage_backend="sqlite", sqlite_path=target_path, embedding_dims=4,
        )
        sdb = SQLiteMemoryDatabase(tgt_cfg)
        await sdb.connect()
        await sdb.apply_migrations(PLUGIN_DIR / "migrations_sqlite")
        try:
            await sdb.insert_chat_summary(
                document_id="s-collide", kind="chat_summary", platform="qq",
                session_id="g1", group_id="g1", user_id="u1",
                participants=["qq:u1"], content="target 版本摘要",
                occurred_at=now, embedding=_vec(0.25, 0.5, 0.75, 1.0),
                summarized=True,
            )
            await sdb.insert_persona_fact_raw(
                document_id="f-collide", platform="qq", user_id="u1",
                related_user_ids=[], display_name="目标名", category="stable",
                statement="target 已有事实", confidence="high", session_id="g1",
                group_id="g1", evidence_key="g1|2026-09-12", occurred_at=now,
                embedding=None,
            )
            await sdb.alias_upsert([
                {"platform": "qq", "user_id": "u1", "name": "旧名",
                 "last_seen": now, "source": "batch"},
            ])
            await sdb.upsert_entity_edge([{
                "platform": "qq", "subject_uid": "u9", "object_uid": "u1",
                "subject_name": "目标九", "object_name": "目标一",
                "relation_label": "朋友", "statement": "目标侧已有边",
                "confidence": "high", "occurred_at": now,
                "evidence_key": "g1|2026-09-12", "is_bot_edge": False,
                "min_evidence": 2,
            }])
            # Pre-seed one target cluster so the cluster/profile/edge-evidence/kv
            # watermark ids all exercise the offset path (the cluster table has no
            # unique key, so it is inserted with a shifted id); occurred_at is
            # NOT NULL and must be provided (the pool shim only translates $n
            # placeholders, so the literal is inlined here)
            async with sdb.pool.acquire() as conn:
                await conn.execute(
                    "INSERT INTO memory_fact_cluster (platform, user_id,"
                    " category, canonical_statement, score, status, occurred_at)"
                    " VALUES ('qq', 'u0', 'stable', 'target 预置簇', 1.0,"
                    " 'active', '2026-09-12T12:00:00.000000Z')"
                )
        finally:
            await sdb.close()

        # PG source data: some rows collide with target unique keys (should
        # be skipped), some are new (should be inserted with shifted ids);
        # the cluster's fact-id references cover both remap paths — skipped
        # -> existing target id, and newly inserted
        pg = _FakePG(
            tables={
                "memory_chat_summary": [
                    {"id": 10, "document_id": "s-collide", "kind": "chat_summary",
                     "platform": "qq", "session_id": "g1", "group_id": "g1",
                     "user_id": "u1", "participants": ["qq:u1"],
                     "content": "source 冲突摘要",
                     "occurred_at": now, "written_at": now,
                     "embedding": _emb_text(0.25, 0.5, 0.75, 1.0),
                     "summarized": True, "search_text": "source 冲突"},
                    {"id": 11, "document_id": "s-new", "kind": "chat_summary",
                     "platform": "qq", "session_id": "g1", "group_id": "g1",
                     "user_id": "u1", "participants": ["qq:u1"],
                     "content": "source 新摘要",
                     "occurred_at": now, "written_at": now,
                     "embedding": _emb_text(1.0, 0.5, 0.25, 0.0),
                     "summarized": True, "search_text": "source 新"},
                ],
                "memory_persona_fact_raw": [
                    {"id": 20, "document_id": "f-collide", "platform": "qq",
                     "user_id": "u1", "related_user_ids": [],
                     "display_name": "源名", "category": "stable",
                     "statement": "source 冲突事实", "confidence": "high",
                     "session_id": "g1", "group_id": "g1",
                     "evidence_key": "g1|2026-09-12", "occurred_at": now,
                     "written_at": now, "extracted_flag": False,
                     "embedding": None},
                    {"id": 21, "document_id": "f-new", "platform": "qq",
                     "user_id": "u1", "related_user_ids": [],
                     "display_name": "源名", "category": "stable",
                     "statement": "source 新事实", "confidence": "high",
                     "session_id": "g1", "group_id": "g1",
                     "evidence_key": "g1|2026-09-13", "occurred_at": now,
                     "written_at": now, "extracted_flag": False,
                     "embedding": None},
                ],
                "memory_fact_cluster": [
                    {"id": 30, "platform": "qq", "user_id": "u1",
                     "category": "stable", "canonical_statement": "source 簇",
                     "score": 3.0, "status": "active", "evidence_count": 1,
                     "evidence_keys": ["g1|2026-09-12"],
                     "source_fact_ids": [20, 21], "last_evidence_at": now,
                     "occurred_at": now, "replaced_by": None,
                     "written_at": now, "updated_at": now, "embedding": None,
                     "demoted_at": None, "contradicted_at": None,
                     "related_user_ids": []},
                ],
                "memory_user_profile": [
                    {"id": 40, "platform": "qq", "user_id": "u1",
                     "category": "stable", "cluster_id": 30,
                     "statement": "source 簇", "score": 3.0,
                     "created_at": now, "updated_at": now,
                     "related_user_ids": []},
                ],
                "memory_entity_alias": [
                    {"id": 50, "platform": "qq", "user_id": "u1",
                     "name": "旧名", "first_seen": now, "last_seen": now,
                     "source": "batch"},
                    {"id": 51, "platform": "qq", "user_id": "u2",
                     "name": "新名", "first_seen": now, "last_seen": now,
                     "source": "backfill"},
                ],
                "memory_entity_edge": [
                    {"id": 60, "platform": "qq", "subject_uid": "u9",
                     "object_uid": "u1", "subject_name": "源九",
                     "object_name": "源一", "relation_label": "朋友",
                     "statement": "source 冲突边", "status": "active",
                     "confidence": "high", "evidence_count": 1,
                     "evidence_keys": ["backfill|30"], "first_seen": now,
                     "last_seen": now, "occurred_at": now, "written_at": now,
                     "supersede_reason": None, "superseded_at": None,
                     "updated_at": now},
                    {"id": 61, "platform": "qq", "subject_uid": "u7",
                     "object_uid": "u8", "subject_name": "源七",
                     "object_name": "源八", "relation_label": "同事",
                     "statement": "source 新边", "status": "active",
                     "confidence": "high", "evidence_count": 1,
                     "evidence_keys": ["backfill|30"], "first_seen": now,
                     "last_seen": now, "occurred_at": now, "written_at": now,
                     "supersede_reason": None, "superseded_at": None,
                     "updated_at": now},
                ],
            },
            kv_rows=[
                {"key": "relation_backfill_watermark", "value": "30",
                 "updated_at": now},
                {"key": "relation_audit_edge_id", "value": "60",
                 "updated_at": now},
            ],
        )

        sdb = SQLiteMemoryDatabase(tgt_cfg)
        await sdb.connect()
        await sdb.apply_migrations(PLUGIN_DIR / "migrations_sqlite")
        code = await mig._run_migration(pg, sdb, force=True, batch_size=100)
        # _run_migration's finally closed the db; re-open a fresh connection for asserts
        check("migrate_force_exit_code", code == 0, str(code))

        vdb = SQLiteMemoryDatabase(tgt_cfg)
        await vdb.connect()
        await vdb.apply_migrations(PLUGIN_DIR / "migrations_sqlite")
        async with vdb.pool.acquire() as conn:
            # colliding summary skipped: the target row was not overwritten; the new summary is inserted with a shifted id
            row = await conn.fetchrow(
                "SELECT content FROM memory_chat_summary "
                "WHERE document_id = 's-collide'"
            )
            check("migrate_skip_summary_keeps_target",
                  row is not None and row["content"] == "target 版本摘要",
                  str(dict(row) if row else None))
            n = await conn.fetchval("SELECT count(*) FROM memory_chat_summary")
            check("migrate_summary_counts", int(n) == 2, str(n))
            # colliding fact skipped; the new fact is inserted with a shifted id (offset = target max id 1 -> id 22)
            frow = await conn.fetchrow(
                "SELECT id, statement FROM memory_persona_fact_raw "
                "WHERE document_id = 'f-new'"
            )
            check("migrate_new_fact_offset",
                  frow is not None and int(frow["id"]) == 22
                  and frow["statement"] == "source 新事实",
                  str(dict(frow) if frow else None))
            # cluster source_fact_ids remap: skipped row -> existing target
            # fact id 1, new row -> shifted id 22
            crow = await conn.fetchrow(
                "SELECT id, source_fact_ids FROM memory_fact_cluster "
                "WHERE canonical_statement = 'source 簇'"
            )
            check("migrate_cluster_remap_mixed",
                  crow is not None and int(crow["id"]) == 31
                  and crow["source_fact_ids"] == "[1, 22]",
                  str(dict(crow) if crow else None))
            # profile cluster_id remapped to the shifted cluster id
            prow = await conn.fetchrow(
                "SELECT cluster_id FROM memory_user_profile "
                "WHERE statement = 'source 簇'"
            )
            check("migrate_profile_remap",
                  prow is not None and int(prow["cluster_id"]) == 31,
                  str(dict(prow) if prow else None))
            # colliding edge skipped (target row kept); the new edge is inserted with its backfill|evidence key remapped
            erow = await conn.fetchrow(
                "SELECT statement FROM memory_entity_edge "
                "WHERE subject_uid = 'u9' AND relation_label = '朋友'"
            )
            check("migrate_skip_edge_keeps_target",
                  erow is not None and erow["statement"] == "目标侧已有边",
                  str(dict(erow) if erow else None))
            nrow = await conn.fetchrow(
                "SELECT id, evidence_keys FROM memory_entity_edge "
                "WHERE relation_label = '同事'"
            )
            check("migrate_new_edge_evidence_remap",
                  nrow is not None and int(nrow["id"]) == 62
                  and nrow["evidence_keys"] == '["backfill|31"]',
                  str(dict(nrow) if nrow else None))
            # colliding alias skipped, new alias inserted
            n_alias = await conn.fetchval(
                "SELECT count(*) FROM memory_entity_alias"
            )
            check("migrate_alias_merge", int(n_alias) == 2, str(n_alias))
            # kv id-key remaps: cluster watermark 30->31 (offset); edge id 60->1 (skipped -> existing row)
            wm = await vdb.get_kv("relation_backfill_watermark")
            check("migrate_kv_cluster_remap", wm == "31", str(wm))
            aid = await vdb.get_kv("relation_audit_edge_id")
            check("migrate_kv_edge_remap_skip_to_existing", aid == "1", str(aid))
            # dim verification is not bound to 1024: source/target both sampled at 4 dims with matching row counts
            marker = await vdb.get_kv("_backend_migration_done")
            stats = json.loads(marker) if marker else {}
            check(
                "migrate_marker_skipped_recorded",
                stats.get("skipped", {}).get("memory_chat_summary") == 1
                and stats.get("skipped", {}).get("memory_entity_edge") == 1
                and stats.get("skipped", {}).get("memory_entity_alias") == 1
                and stats.get("skipped", {}).get("memory_persona_fact_raw") == 1,
                str(stats.get("skipped")),
            )
        await vdb.close()

        # repeat-run guard: a second run without --force must refuse (return 2)
        pg2 = _FakePG(tables={"memory_chat_summary": [],
                              "memory_persona_fact_raw": [],
                              "memory_fact_cluster": [],
                              "memory_user_profile": [],
                              "memory_entity_alias": [],
                              "memory_entity_edge": []},
                      kv_rows=[])
        sdb2 = SQLiteMemoryDatabase(tgt_cfg)
        await sdb2.connect()
        await sdb2.apply_migrations(PLUGIN_DIR / "migrations_sqlite")
        code2 = await mig._run_migration(pg2, sdb2)
        check("migrate_marker_guard", code2 == 2, str(code2))


def main() -> int:
    started = time.time()
    asyncio.run(main_async())
    elapsed = time.time() - started
    total = PASS + _FAIL
    print(f"{total - _FAIL}/{total} checks passed ({elapsed:.2f}s)")
    return 1 if _FAIL else 0


if __name__ == "__main__":
    sys.exit(main())
