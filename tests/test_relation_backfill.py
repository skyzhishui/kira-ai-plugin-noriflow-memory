"""存量关系回填 + 关系图谱数据层测试（python 直跑，无 pytest 依赖）。

覆盖（v1.8.0 关系图谱栏配套，与上游 nori 版测试同构）：
- build_directory：歧义名字整名跳过、bot 显式入目录（通配平台）；
- RelationBackfill.run：批内名字预筛（无可解析名批次跳过 LLM）、
  行构造（evidence_key=backfill|簇id、bot 边判定、min_evidence 透传、
  occurred_at 取簇值）、校验链丢弃（未知簇/subject 锁归属/object 锁
  词典/label 词形锁源陈述/平台一致/label 越界）、同批重复结构键留首条、
  单批失败计数继续、bot 名下簇不作源、进度回调；
- BackfillController：运行中拒绝二次启动、完成后状态翻转、factory
  异常/None 不可用、stop 幂等；
- webui_store.fetch_relation_graph：节点组装（platform:uid 复合键、
  无名回退 uid、bot 标注、degree）+ 统计。

运行（插件目录）：
    python tests/test_relation_backfill.py
"""

from __future__ import annotations

import asyncio
import json
import logging
import sys
import types
from datetime import datetime
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))

from _harness import load_module  # noqa: E402

PLUGIN_DIR = Path(__file__).resolve().parent.parent

_backfill = load_module("relation_backfill")
_entity_edge = load_module("entity_edge")
_webui_store = load_module("webui_store")

RelationBackfill = _backfill.RelationBackfill
BackfillController = _backfill.BackfillController
build_directory = _backfill.build_directory
fetch_relation_graph = _webui_store.fetch_relation_graph


# ---------------------------------------------------------------------------
#  桩件
# ---------------------------------------------------------------------------


class _FakeDB:
    def __init__(self, clusters, alias_rows):
        self.clusters = clusters
        self.alias_rows = alias_rows
        self.edge_rows = []
        self.kv = {}
        self.last_after_id = None

    async def get_kv(self, key):
        return self.kv.get(key)

    async def set_kv(self, key, value):
        self.kv[key] = value

    async def fetch_confirmed_relation_sources(self, after_id=0, limit=None):
        self.last_after_id = after_id
        rows = [c for c in self.clusters if int(c["id"]) > after_id]
        return rows[:limit] if limit is not None else rows

    async def count_confirmed_relation_sources(self, after_id=0, exclude_user_id=""):
        return sum(
            1 for c in self.clusters
            if int(c["id"]) > after_id and c["user_id"] != exclude_user_id
        )

    async def fetch_confirmed_relation_sources_by_ids(self, cluster_ids):
        wanted = {int(i) for i in cluster_ids}
        return [c for c in self.clusters if int(c["id"]) in wanted]

    async def fetch_alias_directory(self):
        return self.alias_rows

    async def upsert_entity_edge(self, rows, **kwargs):
        self.edge_rows.extend(rows)


class _FakeRouter:
    """LLM 出口桩：按序弹出响应（Exception 实例则抛出）。"""

    def __init__(self, responses):
        self.responses = list(responses)
        self.calls = []

    async def __call__(self, system_prompt, user_prompt):
        self.calls.append({"system": system_prompt, "user": user_prompt})
        item = self.responses.pop(0)
        if isinstance(item, Exception):
            raise item
        return item


def _cfg(min_evidence=2):
    return types.SimpleNamespace(
        relation_bot_edge_min_evidence=min_evidence,
        relation_label_stopwords=[],
    )


def _cluster(cid, uid, stmt, platform="qq"):
    return {
        "id": cid,
        "platform": platform,
        "user_id": uid,
        "canonical_statement": stmt,
        "occurred_at": datetime(2026, 9, 1, 12, 0, 0),
    }


def _relation(cid, subject, obj, label="姐姐", statement=None, extra=None):
    payload = {
        "cluster_id": cid,
        "subject_user_id": subject,
        "subject_display_name": "小张",
        "object_user_id": obj,
        "object_display_name": "小李",
        "label": label,
        "statement": statement or "小张说他的姐姐是小李",
        "confidence": "high",
    }
    payload.update(extra or {})
    return payload


def _backfill_of(db, router, batch_size=40):
    return RelationBackfill(
        llm_call=router, db=db, config=_cfg(3),
        bot_user_id="999", bot_nickname="诺里", batch_size=batch_size,
    )


# ---------------------------------------------------------------------------
#  用例
# ---------------------------------------------------------------------------

PASS = 0


def _ok(name):
    global PASS
    PASS += 1
    print(f"  ok {name}")


async def t_directory():
    d = build_directory([("qq", "1", "小王"), ("qq", "2", "小王"), ("qq", "3", "小李")])
    assert "小王" not in d, "歧义名应整名跳过"
    assert d["小李"].uid == "3"
    d2 = build_directory(
        [("qq", "1", "小李"), ("qq", "1", "小李"), ("qq", "1", "李哥")],
        bot_user_id="999", bot_nickname="诺里",
    )
    assert d2["小李"].uid == "1" and d2["李哥"].uid == "1"
    assert d2["诺里"].uid == "999" and d2["诺里"].platform == ""
    assert "诺里" not in build_directory([], bot_user_id="999", bot_nickname="")
    _ok("build_directory 歧义跳过 + bot 条目")


async def t_happy_path():
    db = _FakeDB([_cluster(101, "100", "小张说他的姐姐是小李")], [("qq", "200", "小李")])
    router = _FakeRouter([json.dumps({"relations": [_relation(101, "100", "200")]})])
    progress = []
    summary = await _backfill_of(db, router).run(progress=progress.append)
    assert summary["mode"] == "incremental"
    assert summary["clusters_total"] == 1 and summary["batches_total"] == 1
    assert summary["batches_failed"] == 0
    assert summary["relations_written"] == 1
    assert summary["relations_discarded"] == 0
    [row] = db.edge_rows
    assert row["platform"] == "qq"
    assert row["subject_uid"] == "100" and row["object_uid"] == "200"
    assert row["evidence_key"] == "backfill|101"
    # 回填通道：命中已有边只刷新不计分（线上提取不传，默认照常计分）
    assert row["count_on_conflict"] is False
    assert row["occurred_at"] == datetime(2026, 9, 1, 12, 0, 0)
    assert row["is_bot_edge"] is False and row["min_evidence"] == 3
    assert len(progress) == 1 and progress[0]["done"] == 1
    assert "诺里" in router.calls[0]["system"]
    assert "#101 [qq] uid=100：" in router.calls[0]["user"]
    assert "小李=qq:200" in router.calls[0]["user"]
    _ok("run 正常路径（行构造/进度/提示词）")


async def t_bot_edge_and_owner_skipped():
    db = _FakeDB(
        [_cluster(1, "999", "诺里自己的陈述"), _cluster(2, "100", "我姐姐就是诺里")],
        [("qq", "200", "小李")],
    )
    router = _FakeRouter([json.dumps({"relations": [_relation(
        2, "100", "999", statement="我姐姐就是诺里",
        extra={"object_display_name": "诺里"},
    )]})])
    summary = await _backfill_of(db, router).run()
    assert summary["clusters_total"] == 1, "bot 名下簇不作源"
    [row] = db.edge_rows
    assert row["object_uid"] == "999" and row["is_bot_edge"] is True
    assert row["platform"] == "qq"
    _ok("bot 边判定 + bot 名下簇剔除")


async def t_validation_chain():
    stmt = "小张说他的表姐是小李"
    db = _FakeDB([_cluster(101, "100", stmt)], [("qq", "200", "小李")])
    router = _FakeRouter([json.dumps({"relations": [
        _relation(999, "100", "200", label="表姐", statement=stmt),
        _relation(101, "777", "200", label="表姐", statement=stmt),
        _relation(101, "100", "888", label="表姐", statement=stmt),
        _relation(101, "100", "200", label="姐姐", statement=stmt),
        _relation(101, "100", "200", label="一个超长的关系词", statement=stmt),
        _relation(101, "100", "200", label="表姐", statement=stmt),
    ]})])
    summary = await _backfill_of(db, router).run()
    assert summary["relations_written"] == 1, summary
    assert summary["relations_discarded"] == 5
    assert db.edge_rows[0]["relation_label"] == "表姐"
    _ok("校验链逐条丢弃")


async def t_platform_mismatch():
    db = _FakeDB([_cluster(101, "100", "小张说他的姐姐是小李", platform="web")],
                 [("qq", "200", "小李")])
    router = _FakeRouter([json.dumps({"relations": [_relation(101, "100", "200")]})])
    summary = await _backfill_of(db, router).run()
    assert summary["relations_written"] == 0 and summary["relations_discarded"] == 1
    _ok("平台不一致丢弃（bot 通配除外）")


async def t_dup_struct_key():
    db = _FakeDB([_cluster(101, "100", "小张说他的姐姐是小李")], [("qq", "200", "小李")])
    router = _FakeRouter([json.dumps({"relations": [
        _relation(101, "100", "200"),
        _relation(101, "100", "200", extra={"confidence": "medium"}),
    ]})])
    summary = await _backfill_of(db, router).run()
    assert summary["relations_written"] == 1 and summary["relations_discarded"] == 1
    _ok("同批重复结构键留首条")


async def t_batch_skip_and_fail():
    db = _FakeDB(
        [_cluster(1, "100", "他喜欢打篮球"), _cluster(2, "300", "老王说他的室友是小李")],
        [("qq", "200", "小李")],
    )
    router = _FakeRouter([
        json.dumps({"relations": [_relation(
            2, "300", "200", label="室友", statement="老王说他的室友是小李",
            extra={"subject_display_name": "老王"},
        )]}),
    ])
    summary = await _backfill_of(db, router, batch_size=1).run()
    assert summary["batches_total"] == 2
    assert summary["relations_written"] == 1
    assert len(router.calls) == 1, "无可解析名的批次应跳过 LLM"
    assert db.edge_rows[0]["subject_uid"] == "300"

    db2 = _FakeDB([_cluster(1, "100", "小张说他的姐姐是小李")], [("qq", "200", "小李")])
    router2 = _FakeRouter([RuntimeError("llm down")])
    summary2 = await _backfill_of(db2, router2).run()
    assert summary2["batches_failed"] == 1 and db2.edge_rows == []
    assert db2.kv.get("relation_backfill_watermark") is None, "失败批水位不推进"

    db3 = _FakeDB([_cluster(1, "100", "小张说他的姐姐是小李")], [("qq", "200", "小李")])
    router3 = _FakeRouter(["不是 JSON"])
    summary3 = await _backfill_of(db3, router3).run()
    assert summary3["batches_failed"] == 1 and db3.edge_rows == []
    _ok("批次跳过 LLM / LLM 失败与坏输出计数继续")


async def t_watermark_and_full():
    # 增量：水位止步失败批，重跑自动补；之后空转零 LLM
    db = _FakeDB(
        [
            _cluster(1, "100", "小张说他的姐姐是小李"),
            _cluster(2, "300", "老王说他的室友是小李"),
        ],
        [("qq", "200", "小李")],
    )
    router = _FakeRouter([
        json.dumps({"relations": [_relation(1, "100", "200")]}),
        json.dumps({"relations": [_relation(
            2, "300", "200", label="室友", statement="老王说他的室友是小李",
            extra={"subject_display_name": "老王"},
        )]}),
    ])
    bf = _backfill_of(db, router, batch_size=1)
    summary1 = await bf.run()
    assert summary1["relations_written"] == 2
    assert db.kv["relation_backfill_watermark"] == "2"
    assert len(router.calls) == 2
    # 重跑：水位 2 之后无新簇 → 空转不调 LLM
    summary2 = await bf.run()
    assert summary2["clusters_total"] == 0 and summary2["batches_total"] == 0
    assert db.last_after_id == 2
    assert len(router.calls) == 2, "水位后无新簇：空转不调 LLM"
    # 新簇 3 到来：增量只处理 3
    db.clusters.append(_cluster(3, "500", "小张说他的表姐是小李"))
    router.responses.append(json.dumps({"relations": [_relation(
        3, "500", "200", label="表姐", statement="小张说他的表姐是小李",
        extra={"subject_display_name": "小张"},
    )]}))
    summary3 = await bf.run()
    assert summary3["clusters_total"] == 1 and summary3["relations_written"] == 1
    # 分页流式：增量窗口从水位 2 起（不重取旧簇），批后末次空探测推到 3
    assert db.last_after_id == 3
    assert db.kv["relation_backfill_watermark"] == "3"
    # 全量：忽略水位重取全部（router2 无响应——全量重跑会尝试调 LLM，
    # 这里只验证取簇面，批次在 LLM 前失败即停不影响断言）
    router2 = _FakeRouter([])
    bf2 = _backfill_of(db, router2, batch_size=1)
    summary_full = await bf2.run(force_full=True)
    assert summary_full["mode"] == "full"
    assert summary_full["clusters_total"] == 3
    assert db.last_after_id == 0
    # kv 读取失败退化为全量
    db3 = _FakeDB([_cluster(1, "100", "小张说他的姐姐是小李")], [("qq", "200", "小李")])

    async def _boom(key):
        raise RuntimeError("kv down")

    db3.get_kv = _boom
    router3 = _FakeRouter([json.dumps({"relations": [_relation(1, "100", "200")]})])
    summary4 = await _backfill_of(db3, router3).run()
    assert summary4["mode"] == "full" and summary4["relations_written"] == 1
    _ok("水位增量/全量重跑/kv 失败退化")


async def t_controller():
    db = _FakeDB([_cluster(1, "100", "小张说他的姐姐是小李")], [("qq", "200", "小李")])
    router = _FakeRouter([json.dumps({"relations": [_relation(1, "100", "200")]})])
    ctl = BackfillController(factory=lambda: _backfill_of(db, router))
    assert ctl.available() is True
    started, _ = ctl.start()
    assert started is True and ctl.state["running"] is True
    started2, msg2 = ctl.start()
    assert started2 is False and "运行" in msg2
    await asyncio.sleep(0.05)
    assert ctl.state["running"] is False
    assert ctl.state["relations"] == 1 and ctl.state["error"] == ""
    assert ctl.state["finished_at"]
    await ctl.stop()
    assert ctl.state["running"] is False

    ctl_none = BackfillController(factory=lambda: None)
    assert ctl_none.available() is False
    started3, msg3 = ctl_none.start()
    assert started3 is False and "task_router" in msg3

    def _boom():
        raise RuntimeError("wired wrong")

    assert BackfillController(factory=_boom).available() is False
    _ok("控制器启停/拒绝二次/不可用态")


def _graph_pool(edge_rows, alias_rows):
    """图谱查询桩：按 SQL 目标表分发（边表 / 别名表各一次 fetch）。"""

    class _GraphPool:
        def acquire(self):
            return self

        async def __aenter__(self):
            return self

        async def __aexit__(self, *a):
            return False

        async def fetch(self, sql, *args):
            if "memory_entity_edge" in sql:
                return edge_rows
            assert "memory_entity_alias" in sql
            return alias_rows

        async def fetchval(self, sql, *args):
            # fetch_relation_graph 的全表计数（截断提示的分母）
            assert "count(*)" in sql and "memory_entity_edge" in sql
            return len(edge_rows)

    return _GraphPool()


async def t_graph_assembly():
    edges = [
        {
            "id": 1, "platform": "qq", "subject_uid": "100",
            "object_uid": "999", "subject_name": "小张", "object_name": "",
            "relation_label": "姐姐", "statement": "s", "status": "active",
            "confidence": "high", "evidence_count": 3,
            "last_seen": None, "occurred_at": None,
        },
    ]
    d = await fetch_relation_graph(_graph_pool(edges, []), bot_user_id="999")
    assert d["stats"]["nodes"] == 2 and d["stats"]["edges"] == 1
    assert d["stats"]["edges_shown"] == 1, "截断提示的展示计数"
    assert d["stats"]["edges_active"] == 1 and d["stats"]["edges_pending"] == 0
    by_id = {n["id"]: n for n in d["nodes"]}
    assert by_id["qq:100"]["degree"] == 1
    assert by_id["qq:999"]["is_bot"] is True
    assert by_id["qq:999"]["name"] == "999"
    assert d["edges"][0]["subject"] == "qq:100" and d["edges"][0]["object"] == "qq:999"
    _ok("fetch_relation_graph 节点组装与统计")


async def t_graph_canonical_names():
    """读侧规范名：别名最新 > 边名（非占位）> uid；边级端点名同步规范化。"""
    edges = [
        {
            "id": 1, "platform": "qq", "subject_uid": "1",
            "object_uid": "2", "subject_name": "未知", "object_name": "小李",
            "relation_label": "姐姐", "statement": "s", "status": "active",
            "confidence": "high", "evidence_count": 1,
            "last_seen": None, "occurred_at": None,
        },
        {
            "id": 2, "platform": "qq", "subject_uid": "1",
            "object_uid": "3", "subject_name": "旧名", "object_name": "用户3",
            "relation_label": "姐姐", "statement": "s", "status": "active",
            "confidence": "high", "evidence_count": 1,
            "last_seen": None, "occurred_at": None,
        },
    ]
    alias_rows = [
        {"platform": "qq", "user_id": "1", "name": "张三"},
        {"platform": "qq", "user_id": "3", "name": "未知"},  # 占位名不作规范名
    ]
    d = await fetch_relation_graph(_graph_pool(edges, alias_rows), bot_user_id="")
    by_uid = {n["uid"]: n["name"] for n in d["nodes"]}
    assert by_uid["1"] == "张三"      # 别名表顶替"未知"
    assert by_uid["2"] == "小李"      # 非占位边名保留
    assert by_uid["3"] == "3"        # 别名占位 + 边名占位 -> 回退 uid
    assert d["edges"][1]["object_name"] == "3"
    _ok("fetch_relation_graph 规范名解析（别名>边名>uid，占位名出局）")


async def t_backfill_placeholder_names():
    """回填写侧守卫：LLM 占位端点名用目录规范名顶替，未命中留空。"""

    class _Router:
        def __init__(self, payload):
            self.payload = payload

        async def __call__(self, system_prompt, user_prompt):
            return json.dumps(self.payload)

    # subject/object 都是占位名 -> 双双取目录名
    db = _FakeDB(
        [_cluster(101, "100", "小张说他的姐姐是小李")],
        [("qq", "100", "小张"), ("qq", "200", "小李")],
    )
    router = _Router({"relations": [_relation(
        101, "100", "200",
        extra={"subject_display_name": "未知", "object_display_name": "未知用户"},
    )]})
    bf = _backfill_of(db, router)
    summary = await bf.run()
    assert summary["relations_written"] == 1
    assert db.edge_rows[0]["subject_name"] == "小张"
    assert db.edge_rows[0]["object_name"] == "小李"

    # subject 无目录名 -> 留空（upsert 保旧名），object 正常
    db2 = _FakeDB(
        [_cluster(101, "100", "小张说他的姐姐是小李")],
        [("qq", "200", "小李")],
    )
    router2 = _Router({"relations": [_relation(
        101, "100", "200", extra={"subject_display_name": "未知"},
    )]})
    await _backfill_of(db2, router2).run()
    assert db2.edge_rows[0]["subject_name"] == ""
    assert db2.edge_rows[0]["object_name"] == "小李"

    # 真名原样透传
    db3 = _FakeDB(
        [_cluster(101, "100", "小张说他的姐姐是小李")],
        [("qq", "200", "小李")],
    )
    await _backfill_of(db3, _Router(
        {"relations": [_relation(101, "100", "200")]}
    )).run()
    assert db3.edge_rows[0]["subject_name"] == "小张"
    assert db3.edge_rows[0]["object_name"] == "小李"
    _ok("回填占位名守卫（目录顶替/未命中留空/真名透传）")


async def main() -> int:
    for case in (
        t_directory, t_happy_path, t_bot_edge_and_owner_skipped,
        t_validation_chain, t_platform_mismatch, t_dup_struct_key,
        t_batch_skip_and_fail, t_watermark_and_full, t_controller,
        t_graph_assembly, t_graph_canonical_names, t_backfill_placeholder_names,
    ):
        await case()
    print(f"ALL PASS ({PASS} cases)")
    return 0


if __name__ == "__main__":
    sys.exit(asyncio.run(main()))
