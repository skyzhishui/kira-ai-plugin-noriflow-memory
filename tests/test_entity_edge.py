"""实体关系边（P2 提取落库 / P3 边注入）测试（python 直跑，无 pytest 依赖）。

覆盖（KiraAI 侧，2026-09-09 P2/P3 批）：
- parse_relation 校验（合法/bot 端点/自环/label 越界/statement 超长）；
- select_relation_edges 场景裁决（A 节点+边 / B 退化 / C bot 边门槛 /
  停用词 / 邻居与总行预算 / 边 id 去重）；
- 陈述行组装与邻居画像 uid 收集；
- db.upsert_entity_edge SQL 组装（结构键合并 / evidence 去重 / bot 边
  pending + 内联激活 CASE）+ fetch_active_edges 参数；
- kernel 通道：retain relations 落库（开关/失败上抛/bot 端点行）；
- encoder relations 提示词门控 + 结构化 schema 含 relations；
- build_injection_text 边小节（A/C 注入、开关关闭零变化、主路空时
  小节独立产出、邻居画像、预算上限）；
- main._bot_addressed（At pid 命中 / @all 不算 / Reply 命中 bot 消息
  ID 追踪 / 裸"你"不触发）+ _track_bot_message_ids 有界。

运行（插件目录）：
    python tests/test_entity_edge.py
"""

from __future__ import annotations

import asyncio
import json
import sys
from collections import OrderedDict, deque
from datetime import datetime, timezone
from pathlib import Path
from typing import Optional

sys.path.insert(0, str(Path(__file__).resolve().parent))

from _harness import load_module  # noqa: E402

PLUGIN_DIR = Path(__file__).resolve().parent.parent

_entity_edge = load_module("entity_edge")
_circuit_breaker = load_module("circuit_breaker")
_config = load_module("config")
_db = load_module("db")
_kernel = load_module("memory_kernel")
_encoder = load_module("memory_encoder")
_main = load_module("main")
elements = sys.modules["core.chat.message_elements"]

BOT = "bot-1"

EncodedRelation = _entity_edge.EncodedRelation
parse_relation = _entity_edge.parse_relation
relation_document_id = _entity_edge.relation_document_id
relation_statement_line = _entity_edge.relation_statement_line
select_relation_edges = _entity_edge.select_relation_edges
neighbor_profile_uids = _entity_edge.neighbor_profile_uids
edge_has_bot_endpoint = _entity_edge.edge_has_bot_endpoint
split_reverse_echo_rows = _entity_edge.split_reverse_echo_rows
MemoryDBCircuitBreaker = _circuit_breaker.MemoryDBCircuitBreaker
LocalMemoryConfig = _config.LocalMemoryConfig
MemoryDatabase = _db.MemoryDatabase
LocalMemoryKernel = _kernel.LocalMemoryKernel
RecallHints = _kernel.RecallHints
MemoryEncoder = _encoder.MemoryEncoder
NoriflowMemoryPlugin = _main.NoriflowMemoryPlugin


# ---------------------------------------------------------------------------
#  parse_relation
# ---------------------------------------------------------------------------


def test_parse_relation_valid_and_bot_endpoint() -> None:
    rel = parse_relation({
        "subject_user_id": "u1", "subject_display_name": "小张",
        "object_user_id": BOT, "object_display_name": "Kira",
        "label": "姐姐", "statement": "小张说Kira是他的姐姐",
        "confidence": "high",
    })
    assert rel is not None
    assert rel.subject_user_id == "u1" and rel.object_user_id == BOT
    assert edge_has_bot_endpoint(rel.subject_user_id, rel.object_user_id, BOT)
    rel2 = parse_relation({
        "subject_user_id": "u1", "object_user_id": "u2",
        "label": "室友", "statement": "两人是室友",
    })
    assert rel2 is not None and rel2.confidence == "medium"


def test_parse_relation_invalid_variants() -> None:
    base = {
        "subject_user_id": "u1", "object_user_id": "u2",
        "label": "姐姐", "statement": "陈述", "confidence": "high",
    }
    assert parse_relation(None) is None
    assert parse_relation("x") is None
    assert parse_relation({**base, "subject_user_id": ""}) is None
    assert parse_relation({**base, "object_user_id": "u1"}) is None
    assert parse_relation({**base, "label": "好"}) is None
    assert parse_relation({**base, "label": "这是一个超过八个字的标签"}) is None
    assert parse_relation({**base, "statement": "  "}) is None
    assert parse_relation({**base, "statement": "长" * 201}) is None
    assert parse_relation({**base, "confidence": "low"}) is None


def test_parse_relation_label_with_endpoint_name_rejected() -> None:
    """label 撞端点名（含端点显示名）丢弃——"A的X是X"式陈述行必然破碎。"""
    row = {
        "subject_user_id": "u1", "subject_display_name": "小张",
        "object_user_id": "u2", "object_display_name": "老王",
        "label": "老王", "statement": "小张的老王是老王",
    }
    assert parse_relation(row) is None
    ok = {
        "subject_user_id": "u1", "subject_display_name": "M",
        "object_user_id": "u2", "object_display_name": "小李",
        "label": "姐姐", "statement": "小M的姐姐是小李",
    }
    assert parse_relation(ok) is not None


def test_split_reverse_echo_rows() -> None:
    """镜像在库且正向不在 -> 跳过；正向在库保留；同批双向往先到者胜。"""
    existing = {("qq", "u2", "u1", "主人")}
    rows = [
        {"platform": "qq", "subject_uid": "u1", "object_uid": "u2",
         "relation_label": "主人"},
        {"platform": "qq", "subject_uid": "u2", "object_uid": "u1",
         "relation_label": "主人"},
        {"platform": "qq", "subject_uid": "u3", "object_uid": "u4",
         "relation_label": "同学"},
    ]
    kept, skipped = split_reverse_echo_rows(rows, existing)
    assert skipped == 1
    assert [r["subject_uid"] for r in kept] == ["u2", "u3"]
    batch = [
        {"platform": "qq", "subject_uid": "a", "object_uid": "b",
         "relation_label": "主人"},
        {"platform": "qq", "subject_uid": "b", "object_uid": "a",
         "relation_label": "主人"},
    ]
    kept2, skipped2 = split_reverse_echo_rows(batch, set())
    assert skipped2 == 1 and kept2[0]["subject_uid"] == "a"


def test_relation_document_id_deterministic() -> None:
    r1 = EncodedRelation("u1", "u2", "姐姐", "陈述A")
    r2 = EncodedRelation("u2", "u1", "姐姐", "陈述A")
    ts = datetime(2026, 9, 9, tzinfo=timezone.utc)
    assert relation_document_id(r1, "s1", ts) == relation_document_id(r2, "s1", ts)
    assert relation_document_id(r1, "s1", ts) != relation_document_id(
        EncodedRelation("u1", "u2", "姐姐", "陈述B"), "s1", ts
    )


# ---------------------------------------------------------------------------
#  场景裁决（纯函数）
# ---------------------------------------------------------------------------


def _edge(eid, subject, object_, label, *, sname="", oname=""):
    return {
        "id": eid, "platform": "qq",
        "subject_uid": subject, "object_uid": object_,
        "subject_name": sname, "object_name": oname,
        "relation_label": label, "statement": f"原始陈述{eid}",
        "evidence_count": 1,
        "last_seen": datetime(2026, 9, 7, tzinfo=timezone.utc),
        "occurred_at": datetime(2026, 9, 7, tzinfo=timezone.utc),
    }


_STOP = ["朋友", "认识", "熟人", "网友"]


def _select(edges, text, nodes, *, bot=BOT, addressed=False, neighbors=2,
            lines=3, stopwords=None):
    # 节点匹配为复合键（"platform:uid"）：裸 uid 入参由助手按 _edge 的
    # platform（qq）补前缀，调用侧保持简洁
    node_keys = [n if ":" in n else f"qq:{n}" for n in nodes]
    return select_relation_edges(
        text=text, edges=edges, node_keys=node_keys, bot_uid=bot,
        bot_addressed=addressed, stopwords=stopwords or _STOP,
        max_neighbors=neighbors, max_lines=lines,
    )


def test_scenario_a_node_plus_label() -> None:
    edges = [_edge(1, "u1", "u2", "姐姐", sname="小张", oname="小李")]
    hit = _select(edges, "小张的姐姐最近怎么样", ["u1"])
    assert [e["_scenario"] for e in hit] == ["A"] and hit[0]["_anchor"] == "qq:u1"
    assert _select(edges, "小张最近怎么样", ["u1"]) == []          # 场景 B
    assert _select(edges, "群里谁是姐姐", []) == []                 # 无名字锚


def test_scenario_a_cross_platform_uid_collision() -> None:
    """复合键匹配：同数字 uid 跨平台撞号不得互相命中（P3-p 修复面）。"""
    edge_qq = _edge(21, "u1", "u2", "姐姐", sname="小张")
    # tg:u1 是另一个平台的人：qq 边不作为其节点关系注入
    assert _select([edge_qq], "小张的姐姐", ["tg:u1"]) == []
    # 同平台命中正常
    assert [e["_scenario"] for e in _select([edge_qq], "小张的姐姐", ["qq:u1"])] == ["A"]


def test_scenario_c_bot_edge_hard_gate() -> None:
    bot_edge = _edge(9, "u1", BOT, "姐姐", sname="小张")
    assert _select([bot_edge], "你姐姐是谁", []) == []              # 未 AT/引用
    hit = _select([bot_edge], "@Kira 你姐姐是谁", [], addressed=True)
    assert [e["_scenario"] for e in hit] == ["C"]
    hit2 = _select([bot_edge], "小张的姐姐", ["u1"], addressed=False)
    assert [e["_scenario"] for e in hit2] == ["A"]                  # 真名锚优先


def test_select_budgets_and_stopwords() -> None:
    assert _select([_edge(2, "u1", "u3", "朋友")], "小张的朋友", ["u1"]) == []
    edges = [_edge(3, "u1", "u4", "姐姐"), _edge(4, "u1", "u5", "表哥"),
             _edge(5, "u1", "u6", "同桌")]
    assert len(_select(edges, "小张的姐姐和表哥还有同桌", ["u1"], neighbors=2)) == 2
    edges2 = [_edge(6, "u1", "u4", "姐姐"), _edge(7, "u2", "u5", "表哥"),
              _edge(8, "u3", "u6", "同桌")]
    assert len(_select(edges2, "小张的姐姐小林的表哥小周的同桌",
                       ["u1", "u2", "u3"], lines=2)) == 2


def test_statement_line_and_neighbors() -> None:
    edge = _edge(1, "u1", BOT, "姐姐", sname="小张")
    line = relation_statement_line(edge, bot_uid=BOT, bot_nickname="Kira",
                                   time_qualifier="截至9月7日")
    assert line == f"小张(u1)的姐姐是Kira({BOT})（截至9月7日）"
    edge2 = _edge(2, "u1", "u2", "室友")
    assert relation_statement_line(edge2, bot_uid=BOT, bot_nickname="Kira",
                                   time_qualifier="") == "u1(u1)的室友是u2(u2)"
    hit = _select([edge, edge2], "小张的姐姐和室友", ["u1"])
    assert neighbor_profile_uids(hit, ["qq:u1"], BOT, 2) == [("qq", "u2", "")]
    hit_bot = _select([edge], "@Kira 你姐姐是谁", [], addressed=True)
    assert neighbor_profile_uids(hit_bot, [], BOT, 2) == [("qq", "u1", "小张")]


# ---------------------------------------------------------------------------
#  db 层 SQL
# ---------------------------------------------------------------------------


class FakeConnPool:
    def __init__(self, fetch_rows=None) -> None:
        self.batches: list[tuple[str, list]] = []
        self.queries: list[tuple[str, list]] = []
        self._fetch_rows = fetch_rows or []

    def acquire(self):
        return self

    async def __aenter__(self):
        return self

    async def __aexit__(self, *args):
        return False

    async def executemany(self, sql: str, args) -> None:
        self.batches.append((sql, list(args)))

    async def fetch(self, sql: str, *args):
        self.queries.append((sql, list(args)))
        return self._fetch_rows


def _make_db(pool) -> MemoryDatabase:
    db = MemoryDatabase(LocalMemoryConfig(dsn="postgresql://x"))
    db._pool = pool  # noqa: SLF001
    return db


async def test_upsert_entity_edge_sql_and_params() -> None:
    pool = FakeConnPool()
    db = _make_db(pool)
    await db.upsert_entity_edge([
        {"platform": "qq", "subject_uid": "u1", "object_uid": "u2",
         "subject_name": "小张", "object_name": "小李",
         "relation_label": "姐姐", "statement": "小张说姐姐是小李",
         "confidence": "high", "occurred_at": datetime(2026, 9, 9, 12, 0),
         "evidence_key": "s1|2026-09-09", "is_bot_edge": False,
         "min_evidence": 2},
        {"platform": "qq", "subject_uid": "u1", "object_uid": BOT,
         "subject_name": "小张", "object_name": "",
         "relation_label": "姐姐", "statement": "小张说Kira是他的姐姐",
         "confidence": "medium", "occurred_at": datetime(2026, 9, 9, 12, 0),
         "evidence_key": "s1|2026-09-09", "is_bot_edge": True,
         "min_evidence": 2},
    ])
    sql, payload = pool.batches[0]
    assert "INSERT INTO memory_entity_edge" in sql
    assert "ON CONFLICT (platform, subject_uid, object_uid, relation_label)" in sql
    assert "$11::text = ANY(memory_entity_edge.evidence_keys)" in sql
    assert "array_append(" in sql and sql.count("$11::text") >= 4
    assert "WHEN $12::bool" in sql and ">= $13::int" in sql
    # occurred_at 单调不回退（pending 重放旧批次/回填旧簇不倒退发生时间）；
    # SQL 多行排版，去空白后匹配
    assert (
        "occurred_at=GREATEST(memory_entity_edge.occurred_at,EXCLUDED.occurred_at)"
        in "".join(sql.split())
    )
    assert payload[0][7] == "active" and payload[1][7] == "pending"
    assert payload[1][11] is True and payload[1][12] == 2
    # 端点占位名守卫：冲突路径占位名不覆盖旧名（双侧端点各一段 CASE）；
    # "未知"系为精确形（未知/未知用户+数字），前缀泛匹配误伤真名已废除
    assert sql.count("~* '^(unknown|undefined|用户[0-9]+|[0-9]+|未知(用户)?[0-9]*)$'") == 2
    assert "LIKE '未知%'" not in sql
    pool.batches.clear()
    await db.upsert_entity_edge([])
    assert not pool.batches


async def test_upsert_reverse_echo_skipped() -> None:
    """反向回声：镜像方向键已在库而正向不在 -> 整行跳过；正向已在库照常 upsert。"""
    mirror_row = {"platform": "qq", "subject_uid": "u2", "object_uid": "u1",
                  "relation_label": "主人"}
    pool = FakeConnPool(fetch_rows=[mirror_row])
    db = _make_db(pool)
    await db.upsert_entity_edge([
        {"platform": "qq", "subject_uid": "u1", "object_uid": "u2",
         "subject_name": "小张", "object_name": "小李",
         "relation_label": "主人", "statement": "小张称呼小李为主人",
         "confidence": "medium", "occurred_at": datetime(2026, 9, 9, 12, 0),
         "evidence_key": "s1|2026-09-09", "is_bot_edge": False, "min_evidence": 2},
        {"platform": "qq", "subject_uid": "u8", "object_uid": "u9",
         "subject_name": "小张", "object_name": "小李",
         "relation_label": "姐姐", "statement": "小张的姐姐是小李",
         "confidence": "high", "occurred_at": datetime(2026, 9, 9, 12, 0),
         "evidence_key": "s1|2026-09-09", "is_bot_edge": False, "min_evidence": 2},
    ])
    assert len(pool.batches) == 1 and len(pool.batches[0][1]) == 1
    assert pool.batches[0][1][0][1] == "u8"  # subject_uid 参数序 $2


async def test_fetch_active_edges_params() -> None:
    pool = FakeConnPool()
    db = _make_db(pool)
    await db.fetch_active_edges(["qq:u1", "qq:u2"], "")
    sql, args = pool.queries[0]
    assert "FROM memory_entity_edge" in sql and "status = 'active'" in sql
    assert "platform || ':' || subject_uid" in sql, "复合键匹配（平台感知）"
    # 空入参 -> 空列表（ANY(cardinality>0) 分支不取边）
    assert args == [["qq:u1", "qq:u2"], []]
    await db.fetch_active_edges([], ["qq:bot-1"])
    assert pool.queries[1][1] == [[], ["qq:bot-1"]]
    # 双形态集合：任一形态复合键命中 bot 端点边
    await db.fetch_active_edges([], ["qq:9900000004", "qq:bot-1"])
    assert pool.queries[2][1] == [[], ["qq:9900000004", "qq:bot-1"]]


# ---------------------------------------------------------------------------
#  kernel 写通道
# ---------------------------------------------------------------------------


class FakeWriteDB:
    def __init__(self, fail_edges=False) -> None:
        self.edge_rows: list[dict] = []
        self.fail_edges = fail_edges
        self.summaries: list[dict] = []
        self.facts: list[dict] = []

    async def insert_persona_fact_raw(self, **kwargs) -> None:
        self.facts.append(kwargs)

    async def fetch_recent_summary_scores(self, **kwargs) -> list[dict]:
        return []  # no recent duplicates (stub)

    async def insert_chat_summary(self, **kwargs) -> None:
        self.summaries.append(kwargs)

    async def upsert_entity_edge(self, rows, **kwargs) -> None:
        if self.fail_edges:
            raise RuntimeError("边表写入失败（测试注入）")
        self.edge_rows.extend(rows)


class FakeEmbed:
    async def embed_one(self, text):
        return [0.1]

    async def embed_batch(self, texts):
        return [[0.1] for _ in texts]


class FakeEncodeEncoder:
    def __init__(self, result) -> None:
        self.result = result

    async def encode(self, text, nickname, bot_user_id=""):
        return self.result


_REL_A = EncodedRelation("u1", "u2", "姐姐", "小张说姐姐是小李",
                         confidence="high",
                         subject_display_name="小张", object_display_name="小李")
_REL_BOT = EncodedRelation("u1", BOT, "姐姐", "小张说Kira是他的姐姐",
                           confidence="medium", subject_display_name="小张")


def _write_kernel(db, encoder, **cfg):
    return LocalMemoryKernel(
        db=db,
        embedding_service=FakeEmbed(),
        circuit_breaker=MemoryDBCircuitBreaker(failure_threshold=50, recovery_seconds=1.0),
        config=LocalMemoryConfig(**cfg),
        bot_id=BOT,
        encoder=encoder,
        bot_nickname="Kira",
    )


async def test_retain_relations_channel_and_gating() -> None:
    contracts = load_module("contracts")
    encoder = FakeEncodeEncoder((
        "摘要",
        [contracts.EncodedFact(user_id="u1", statement="小张是学生",
                               display_name="小张", category="identity",
                               confidence="high")],
        [_REL_A, _REL_BOT], True,
    ))
    db = FakeWriteDB()
    kernel = _write_kernel(db, encoder, relation_extract_enabled=True)
    doc_ids = await kernel.retain_encoded("对话", "s1", user_id="u1", platform="qq")
    assert len(db.edge_rows) == 2
    assert db.edge_rows[0]["evidence_key"].startswith("s1|")
    assert db.edge_rows[0]["is_bot_edge"] is False
    assert db.edge_rows[1]["object_uid"] == BOT
    assert db.edge_rows[1]["is_bot_edge"] is True
    assert any(d.startswith("rel-") for d in doc_ids)
    assert len(db.summaries) == 1

    db2 = FakeWriteDB()
    kernel2 = _write_kernel(db2, FakeEncodeEncoder(("摘要", [], [_REL_A], True)))
    await kernel2.retain_encoded("对话", "s1", platform="qq")
    assert db2.edge_rows == []


async def test_retain_relations_failure_no_half_commit() -> None:
    """边表写入失败 -> MemoryDBUnavailable 上抛（kira 侧由 main 回滚水位线，
    本批下轮重编码——evidence_key 幂等保证重试不重复计数）；summary 未写。
    """
    db = FakeWriteDB(fail_edges=True)
    kernel = _write_kernel(
        db, FakeEncodeEncoder(("摘要", [], [_REL_A], True)),
        relation_extract_enabled=True,
    )
    try:
        await kernel.retain_encoded("对话", "s1", platform="qq")
        raise AssertionError("应上抛 MemoryDBUnavailable")
    except _kernel.MemoryDBUnavailable:
        pass
    assert db.summaries == []  # 无半提交


async def test_encoder_relations_prompt_and_schema() -> None:
    class SpyLlm:
        def __init__(self) -> None:
            self.prompts: list[str] = []
            self.schemas: list[dict] = []

        async def run_structured(self, **kwargs):
            self.prompts.append(kwargs["system_prompt"])
            self.schemas.append(kwargs.get("schema"))
            return json.dumps({"summary": "摘要", "facts": [], "relations": []},
                              ensure_ascii=False)

    llm = SpyLlm()
    enc = MemoryEncoder(llm=llm, prompt_dir=PLUGIN_DIR / "prompts")
    await enc.encode("对话", "Kira", BOT)
    assert "关系提取" not in llm.prompts[0]

    llm2 = SpyLlm()
    enc2 = MemoryEncoder(llm=llm2, prompt_dir=PLUGIN_DIR / "prompts",
                         relations_enabled=True)
    _, _, relations, ok = await enc2.encode("对话", "Kira", BOT)
    assert ok is True and relations == []
    assert "关系提取（relations 扩展数组" in llm2.prompts[0]
    # 结构化出口 schema 含 relations 数组（tool calling 强制出口能产出）
    assert "relations" in llm2.schemas[0]["properties"]


# ---------------------------------------------------------------------------
#  build_injection_text 边小节
# ---------------------------------------------------------------------------


class FakeRecallDB:
    def __init__(self, edges=None, rows=None, canonical=None) -> None:
        self.edges = edges or []
        self.rows = rows or []
        self.edge_calls: list[dict] = []
        self.canonical = canonical or {}

    async def search_chat_summaries(self, **kwargs) -> list[dict]:
        return self.rows

    async def alias_fetch_all(self) -> list[dict]:
        return []

    async def fetch_hint_candidates(self, **kwargs) -> list[dict]:
        return []

    async def fetch_recent_session_participants(self, **kwargs) -> list[dict]:
        return []

    async def fetch_active_edges(self, node_keys, bot_keys=""):
        self.edge_calls.append({
            "node_keys": list(node_keys),
            "bot_keys": list(bot_keys) if isinstance(bot_keys, (list, tuple, set)) else bot_keys,
        })
        return self.edges

    async def fetch_alias_names_by_owner(self, owners=None) -> dict:
        if owners:
            return {k: v for k, v in self.canonical.items() if k in set(owners)}
        return self.canonical


class FakePersona:
    def __init__(self, block="# 用户画像-背景信息\n小李档案") -> None:
        self.block = block
        self.calls: list = []

    async def build_multi_profile_text(self, candidates, session_id):
        self.calls.append(candidates)
        return self.block if candidates else ""


def _recall_kernel(db, persona=None, **cfg):
    kernel = LocalMemoryKernel(
        db=db,
        embedding_service=FakeEmbed(),
        circuit_breaker=MemoryDBCircuitBreaker(),
        config=LocalMemoryConfig(**cfg),
        bot_id=BOT,
        bot_nickname="Kira",
    )
    kernel.persona_service = persona
    return kernel


async def test_injection_scenario_a_appends_section() -> None:
    db = FakeRecallDB(
        edges=[_edge(1, "u1", "u2", "姐姐", sname="小张", oname="小李")],
        rows=[{"id": 1, "document_id": "d1", "kind": "chat_summary",
               "session_id": "s1", "user_id": "u1", "content": "上周小张聊过爬山",
               "occurred_at": datetime(2026, 9, 1), "relevance": 0.9}],
    )
    persona = FakePersona()
    kernel = _recall_kernel(db, persona, relation_inject_enabled=True)
    hints = kernel.make_recall_hints(
        entity_user_keys=["qq:u1"], match_text="小张的姐姐最近怎么样",
    )
    text = await kernel.build_injection_text(
        query="小张的姐姐", session_id="s1", top_k=3,
        user_id="u9", platform="qq", hints=hints,
    )
    assert "# 相关长期记忆" in text and "# 相关人物关系" in text
    assert "小张(u1)的姐姐是小李(u2)（截至9月7日）" in text
    assert len(persona.calls) == 1
    assert persona.calls[0][0].user_id == "u2"
    assert persona.calls[0][0].display_name == "小李"


async def test_injection_canonical_name_override() -> None:
    """注入陈述行端点名以别名表最新规范名优先（边表冗余名可能过期）。"""
    db = FakeRecallDB(
        edges=[_edge(1, "u1", "u2", "姐姐", sname="旧昵称", oname="小李")],
        canonical={("qq", "u1"): "新昵称"},
    )
    kernel = _recall_kernel(db, relation_inject_enabled=True)
    hints = kernel.make_recall_hints(
        entity_user_keys=["qq:u1"], match_text="Hy的姐姐最近怎么样",
    )
    text = await kernel.build_injection_text(
        query="Hy的姐姐", session_id="s1", top_k=3,
        user_id="u9", platform="qq", hints=hints,
    )
    assert "新昵称(u1)的姐姐是小李(u2)" in text


async def test_injection_scenario_c_and_gate() -> None:
    edges = [_edge(9, "u1", BOT, "姐姐", sname="小张")]
    db = FakeRecallDB(edges=edges)
    kernel = _recall_kernel(db, relation_inject_enabled=True)
    hints = kernel.make_recall_hints(match_text="@Kira 你姐姐是谁", bot_addressed=True)
    text = await kernel.build_injection_text(
        query="你姐姐是谁", session_id="s1", platform="qq", hints=hints,
    )
    assert text.startswith("# 相关人物关系")
    assert f"小张(u1)的姐姐是Kira({BOT})" in text

    db2 = FakeRecallDB(edges=edges)
    kernel2 = _recall_kernel(db2, relation_inject_enabled=True)
    hints2 = kernel2.make_recall_hints(match_text="你姐姐是谁", bot_addressed=False)
    text2 = await kernel2.build_injection_text(
        query="你姐姐是谁", session_id="s1", platform="qq", hints=hints2,
    )
    assert text2 == "" and db2.edge_calls == []


def test_select_relation_edges_dual_bot_forms() -> None:
    """bot uid 集合语义：会话标识/平台 uid 双形态任一命中即 bot 端点边。"""
    plat_edge = _edge(12, "9900000004", "u1", "姐姐", oname="小张")
    # 双形态集合：平台 uid 端点边照常走场景 C
    hit = _select([plat_edge], "你姐姐是谁", [], bot=[BOT, "9900000004"], addressed=True)
    assert [e["_scenario"] for e in hit] == ["C"]
    assert hit[0]["_anchor"] == "9900000004"
    # 仅会话标识（旧单形态）：平台 uid 端点边不认 -> 不注入
    assert _select([plat_edge], "你姐姐是谁", [], bot=BOT, addressed=True) == []


async def test_injection_scenario_c_platform_uid_form() -> None:
    """场景 C 端到端（双形态）：hints 带平台 uid，平台 uid 端点边命中且
    陈述行以 bot 昵称渲染该端点。"""
    edges = [_edge(11, "9900000004", "u1", "姐姐", oname="小张")]
    db = FakeRecallDB(edges=edges)
    kernel = _recall_kernel(db, relation_inject_enabled=True)
    hints = kernel.make_recall_hints(
        match_text="@Kira 你姐姐是谁", bot_addressed=True, bot_user_id="9900000004",
    )
    text = await kernel.build_injection_text(
        query="你姐姐是谁", session_id="s1", platform="qq", hints=hints,
    )
    assert "Kira(9900000004)的姐姐是小张(u1)" in text
    # 边检索按复合键下发（bot 形态 × 会话平台 qq；显式平台 uid 优先 +
    # bot_id 兜底双形态）
    assert db.edge_calls and db.edge_calls[0]["bot_keys"] == [
        "qq:9900000004", f"qq:{BOT}",
    ]


def test_make_recall_hints_learns_platform_uid_form() -> None:
    """hints 学习式形态备忘：平台 uid 进集合后，写侧 is_bot 判定通吃双形态
    （平台 uid 形态边正确走 pending/双证据门槛）。"""
    kernel = _recall_kernel(FakeRecallDB())
    assert kernel._bot_uid_forms_all() == [BOT]
    kernel.make_recall_hints(match_text="x", bot_user_id="9900000004")
    assert kernel._bot_uid_forms_all() == [BOT, "9900000004"]
    assert edge_has_bot_endpoint("9900000004", "u1", kernel._bot_uid_forms_all())
    assert edge_has_bot_endpoint("u1", BOT, kernel._bot_uid_forms_all())
    assert not edge_has_bot_endpoint("u1", "u2", kernel._bot_uid_forms_all())


async def test_injection_disabled_and_stopwords() -> None:
    edges = [_edge(2, "u1", "u3", "朋友"), _edge(3, "u1", "u4", "表哥")]
    db = FakeRecallDB(edges=edges)
    kernel = _recall_kernel(db, relation_inject_enabled=False)
    hints = kernel.make_recall_hints(
        entity_user_keys=["qq:u1"], match_text="小张的朋友和表哥",
    )
    text = await kernel.build_injection_text(
        query="小张的朋友", session_id="s1", platform="qq", hints=hints,
    )
    assert text == "" and db.edge_calls == []

    db2 = FakeRecallDB(edges=edges)
    kernel2 = _recall_kernel(db2, relation_inject_enabled=True)
    hints2 = kernel2.make_recall_hints(
        entity_user_keys=["qq:u1"], match_text="小张的朋友和表哥",
    )
    text2 = await kernel2.build_injection_text(
        query="小张的朋友", session_id="s1", platform="qq", hints=hints2,
    )
    assert "表哥" in text2 and "朋友" not in text2.replace("# 相关人物关系", "")


async def test_injection_budget_caps_lines() -> None:
    edges = [_edge(1, "u1", "u2", "姐姐"), _edge(2, "u1", "u3", "表哥"),
             _edge(3, "u1", "u4", "同桌")]
    db = FakeRecallDB(edges=edges)
    kernel = _recall_kernel(
        db, relation_inject_enabled=True,
        relation_inject_max_lines=2, relation_inject_max_neighbors=3,
    )
    hints = kernel.make_recall_hints(
        entity_user_keys=["qq:u1"], match_text="小张的姐姐表哥同桌",
    )
    text = await kernel.build_injection_text(
        query="小张的姐姐", session_id="s1", platform="qq", hints=hints,
    )
    assert text.count("\n- ") == 2


# ---------------------------------------------------------------------------
#  main._bot_addressed / _track_bot_message_id
# ---------------------------------------------------------------------------


class _Msg:
    def __init__(self, elements, is_notice=False):
        self.chain = elements
        self.is_notice = is_notice


def _bare_plugin():
    plugin = object.__new__(NoriflowMemoryPlugin)
    plugin._bot_user_id = BOT
    plugin._bot_message_ids = OrderedDict()
    plugin._history = OrderedDict()
    return plugin


def test_bot_addressed_at_and_reply() -> None:
    plugin = _bare_plugin()
    # AT bot（pid 命中）
    assert plugin._bot_addressed(
        [_Msg([elements.Text("你姐姐是谁"), elements.At(pid=BOT, nickname="Kira")])],
        "qq:gm:1",
    ) is True
    # @all 不算
    assert plugin._bot_addressed(
        [_Msg([elements.At(pid="all")])], "qq:gm:1"
    ) is False
    # AT 他人不算；裸"你"不算
    assert plugin._bot_addressed(
        [_Msg([elements.At(pid="u9"), elements.Text("你姐姐是谁")])], "qq:gm:1"
    ) is False
    # Reply 命中 bot 出站消息 ID 追踪集
    plugin._track_bot_message_id("qq:gm:1", "M-100")
    assert plugin._bot_addressed(
        [_Msg([elements.Reply(message_id="M-100")])], "qq:gm:1"
    ) is True
    # Reply 指向他人消息不算
    assert plugin._bot_addressed(
        [_Msg([elements.Reply(message_id="M-999")])], "qq:gm:1"
    ) is False
    # 未学到 bot uid（首条消息前）：恒 False
    plugin2 = _bare_plugin()
    plugin2._bot_user_id = ""
    assert plugin2._bot_addressed(
        [_Msg([elements.At(pid=BOT)])], "qq:gm:1"
    ) is False


def test_track_bot_message_ids_bounded() -> None:
    plugin = _bare_plugin()
    for i in range(120):
        plugin._track_bot_message_id("s1", f"M-{i}")
    assert len(plugin._bot_message_ids["s1"]) == _main._BOT_MESSAGE_ID_LIMIT
    assert "M-119" in plugin._bot_message_ids["s1"]
    # 会话数 LRU 上限
    for i in range(_main._SESSION_CACHE_MAX_SESSIONS + 5):
        plugin._track_bot_message_id(f"s{i}", "M-x")
    assert len(plugin._bot_message_ids) == _main._SESSION_CACHE_MAX_SESSIONS


# ---------------------------------------------------------------------------
#  运行器
# ---------------------------------------------------------------------------


async def main() -> None:
    test_parse_relation_valid_and_bot_endpoint()
    test_parse_relation_invalid_variants()
    test_relation_document_id_deterministic()
    test_scenario_a_node_plus_label()
    test_scenario_c_bot_edge_hard_gate()
    test_select_budgets_and_stopwords()
    test_statement_line_and_neighbors()
    await test_upsert_entity_edge_sql_and_params()
    await test_fetch_active_edges_params()
    await test_retain_relations_channel_and_gating()
    await test_retain_relations_failure_no_half_commit()
    await test_encoder_relations_prompt_and_schema()
    await test_injection_scenario_a_appends_section()
    await test_injection_scenario_c_and_gate()
    await test_injection_disabled_and_stopwords()
    await test_injection_budget_caps_lines()
    test_bot_addressed_at_and_reply()
    test_track_bot_message_ids_bounded()
    print("ALL PASS")


if __name__ == "__main__":
    sys.exit(asyncio.run(main()))
