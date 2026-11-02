"""v1.18.2 ported-feature coverage: cross-session summary injection (nori
#31), naming low-threshold ingestion (nori #32), encoder truncation guard
(nori #26) and the guidance convergence (nori #29).

Coverage:
- cross-session: PG query shape (participants && / session exclusion / age
  window / order), SQLite twin fetch on a real temp DB, kernel memo
  exclusion wiring into the shared retrieval path, block builder budget
  semantics (preamble counted, only rendered rows memo-registered);
- naming: create-with-immediate-promotion (naming start 3.0 >= threshold
  3.0 -> profiled + profile row in the same tx), promote-pass naming arm
  (stable arm untouched), encoder naming section toggle;
- truncation guard: _looks_truncated matrix (reverse whitelist), encode
  retry-once then degrade;
- guidance: no per-tool routing left in the shared :tools block (the
  descriptions are the single source; nori #29 convergence lock).

Run (plugin dir):
    python tests/test_cross_session_naming.py
"""

from __future__ import annotations

import asyncio
import json
import sys
import tempfile
from datetime import datetime, timedelta, timezone
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))

from plugin_env import install_host_stubs, load_modules, load_plugin_module  # noqa: E402

PLUGIN_DIR = Path(__file__).resolve().parent.parent

install_host_stubs()
plugin_main = load_plugin_module()
_config_mod, _db_pkg = load_modules("config", "db")
LocalMemoryConfig = _config_mod.LocalMemoryConfig

_NOW = datetime(2026, 11, 2, 12, 0, 0, tzinfo=timezone.utc)


# ---------------------------------------------------------------------------
#  cross-session: PG query shape
# ---------------------------------------------------------------------------

class _CaptureConn:
    def __init__(self, rows):
        self._rows = rows
        self.queries: list[tuple[str, tuple]] = []

    async def fetch(self, sql, *params):
        self.queries.append((sql, params))
        return self._rows


class _CapturePool:
    def __init__(self, conn):
        self.conn = conn

    def acquire(self):
        return _Ctx(self.conn)


class _Ctx:
    def __init__(self, conn):
        self._conn = conn

    async def __aenter__(self):
        return self._conn

    async def __aexit__(self, *exc):
        return False


async def test_cross_session_pg_sql_shape() -> None:
    MemoryDatabase = _db_pkg.MemoryDatabase
    db = MemoryDatabase.__new__(MemoryDatabase)
    conn = _CaptureConn([{"document_id": "d1", "session_id": "s2",
                          "content": "x", "occurred_at": _NOW}])
    db._pool = _CapturePool(conn)
    rows = await db.fetch_recent_cross_session_summaries(
        current_session_id="s1",
        user_keys=["qq:u1", "qq:u2"],
        limit=2,
        max_age_days=30,
    )
    assert len(rows) == 1
    sql, params = conn.queries[0]
    assert "session_id <> $1" in sql, "排除当前会话"
    assert "participants && $2" in sql, "身份键数组重叠"
    assert "occurred_at >= now() - make_interval(days => $3)" in sql, "陈旧窗口"
    assert sql.rstrip().endswith("ORDER BY occurred_at DESC, id DESC LIMIT $4")
    assert params == ("s1", ["qq:u1", "qq:u2"], 30, 2)
    # 空键过滤（None/空白不进数组绑定）
    conn2 = _CaptureConn([])
    db._pool = _CapturePool(conn2)
    await db.fetch_recent_cross_session_summaries(
        current_session_id="s1",
        user_keys=["", None, "  "],
        limit=2,
    )
    assert conn2.queries == [], "全无效键应直接短路不发查询"
    print("PASS test_cross_session_pg_sql_shape")


# ---------------------------------------------------------------------------
#  cross-session: SQLite twin fetch (real temp DB)
# ---------------------------------------------------------------------------

async def test_cross_session_sqlite_fetch() -> None:
    SQLiteMemoryDatabase = _db_pkg.SQLiteMemoryDatabase
    with tempfile.TemporaryDirectory(prefix="nm_cs_") as tmp:
        config = LocalMemoryConfig(
            storage_backend="sqlite",
            sqlite_path=str(Path(tmp) / "m.db"),
            embedding_dims=4,
        )
        db = await _make_sqlite_db(tmp)
        try:
            old = _NOW - timedelta(days=40)
            for i, (doc, sid, occurred, parts) in enumerate([
                ("d-other", "s2", _NOW - timedelta(hours=2), ["qq:u1"]),
                ("d-other-old", "s3", old, ["qq:u1"]),
                ("d-current", "s1", _NOW - timedelta(hours=1), ["qq:u1"]),
                ("d-stranger", "s4", _NOW - timedelta(hours=1), ["qq:u9"]),
            ]):
                await db.insert_chat_summary(
                    document_id=doc, kind="chat_summary", platform="qq",
                    session_id=sid, group_id="", user_id="u1",
                    participants=parts, content=f"摘要-{doc}",
                    occurred_at=occurred, summarized=True, embedding=None,
                )
            rows = await db.fetch_recent_cross_session_summaries(
                current_session_id="s1", user_keys=["qq:u1"],
                limit=10, max_age_days=30,
            )
            assert [r["document_id"] for r in rows] == ["d-other"], (
                "仅命中其他会话、本人参与、窗口内的行（当前会话/陈旧行/陌生人行排除）"
            )
            rows_all = await db.fetch_recent_cross_session_summaries(
                current_session_id="s1", user_keys=["qq:u1"],
                limit=10, max_age_days=0,
            )
            assert [r["document_id"] for r in rows_all] == ["d-other", "d-other-old"], (
                "max_age_days=0 不限陈旧度，按发生时间新->旧"
            )
        finally:
            await db.close()
    print("PASS test_cross_session_sqlite_fetch")


# ---------------------------------------------------------------------------
#  cross-session: kernel memo exclusion wiring
# ---------------------------------------------------------------------------

class _SearchCaptureDB:
    def __init__(self, rows=None):
        self.rows = rows or []
        self.search_kwargs: dict | None = None

    async def search_chat_summaries(self, **kwargs):
        self.search_kwargs = kwargs
        return [dict(r) for r in self.rows]

    async def fetch_recent_session_participants(self, **kwargs):
        return []


async def test_cross_session_memo_excludes_from_search() -> None:
    """跨会话 memo 并入共享检索排除通道：登记行被排除、清空后恢复。"""
    circuit_breaker_mod = load_modules("circuit_breaker")[0]
    kernel_mod = load_modules("memory_kernel")[0]

    class _StubBreaker(circuit_breaker_mod.MemoryDBCircuitBreaker):
        async def is_available(self) -> bool:
            return True

        async def record_failure(self) -> None:
            pass

        async def record_success(self) -> None:
            pass

    class _StubEmbedding:
        async def embed_one(self, text):
            return [0.1]

    db = _SearchCaptureDB(rows=[])
    kernel = kernel_mod.LocalMemoryKernel(
        db=db,
        embedding_service=_StubEmbedding(),
        circuit_breaker=_StubBreaker(),
        config=LocalMemoryConfig(dsn="postgresql://stub"),
        bot_id="bot",
    )
    kernel.refresh_cross_session_memo("s1", ["doc-a", "doc-b", "doc-a"])
    await kernel.search(query="猫", session_id="s1", top_k=3)
    assert db.search_kwargs["exclude_document_ids"] == ["doc-a", "doc-b"], (
        "memo 行（去重后）应并入检索排除"
    )
    kernel.refresh_cross_session_memo("s1", [])
    await kernel.search(query="猫", session_id="s1", top_k=3)
    assert db.search_kwargs["exclude_document_ids"] is None, (
        "入口清空后不再排除（SQL 不加条件）"
    )
    print("PASS test_cross_session_memo_excludes_from_search")


# ---------------------------------------------------------------------------
#  cross-session: block builder budget semantics
# ---------------------------------------------------------------------------

def test_build_cross_session_block_budget() -> None:
    row_new = {
        "document_id": "d1", "session_id": "s2",
        "occurred_at": _NOW - timedelta(hours=2),
        "content": "用户在另一个会话里聊了爬山计划。",
    }
    row_old = {
        "document_id": "d2", "session_id": "s3",
        "occurred_at": _NOW - timedelta(days=2),
        "content": "更早的一条摘要。",
    }
    block, ids = plugin_main.build_cross_session_block(
        [row_new, row_old], now=_NOW, tz=timezone.utc, budget=1500,
    )
    assert block.startswith("# 该用户在其他会话的近期摘要")
    assert "摘要内容：用户在另一个会话里聊了爬山计划。" in block
    assert ids == ["d1", "d2"], "预算内两行都注入，都登记 memo"
    assert block.index("摘要内容：用户在另一") < block.index("摘要内容：更早"), (
        "新->旧排列"
    )

    # 极小预算：前言占满 -> 无条目（既不注入也不登记，行保持可检索）
    tiny_block, tiny_ids = plugin_main.build_cross_session_block(
        [row_new], now=_NOW, tz=timezone.utc, budget=120,
    )
    assert tiny_block == "" and tiny_ids == []

    # 首条截断后正文残留过短 -> 整条放弃（不注入不登记）
    short_block, short_ids = plugin_main.build_cross_session_block(
        [row_new], now=_NOW, tz=timezone.utc, budget=260,
    )
    assert short_block == "" and short_ids == [], (
        "截断至元数据-only 的条目必须放弃（防注入+登记造成召回盲区）"
    )

    # 空内容行跳过
    empty = dict(row_new, content="   ")
    b, i = plugin_main.build_cross_session_block(
        [empty], now=_NOW, tz=timezone.utc, budget=1500,
    )
    assert b == "" and i == []
    print("PASS test_build_cross_session_block_budget")


# ---------------------------------------------------------------------------
#  naming: create-with-immediate-promotion + promote-pass arm (SQLite)
# ---------------------------------------------------------------------------

async def _make_sqlite_db(tmp: str):
    SQLiteMemoryDatabase = _db_pkg.SQLiteMemoryDatabase
    config = LocalMemoryConfig(
        storage_backend="sqlite",
        sqlite_path=str(Path(tmp) / "m.db"),
        embedding_dims=4,
    )
    db = SQLiteMemoryDatabase(config)
    await db.connect()
    await db.apply_migrations(PLUGIN_DIR / "migrations_sqlite")
    return db


async def _seed_raw(db, *, uid: str, cat: str, statement: str) -> int:
    return await db.upsert_persona_fact_raw_for_apply(
        document_id=f"doc-{uid}-{cat}-{statement[:6]}",
        platform="qq", user_id=uid, related_user_ids=[],
        display_name="", category=cat, statement=statement,
        confidence="high", session_id="s1", group_id="",
        evidence_key=f"s1|{uid}|{cat}", occurred_at=_NOW, embedding=None,
    )


async def test_naming_create_immediate_promotion_sqlite() -> None:
    with tempfile.TemporaryDirectory(prefix="nm_naming_") as tmp:
        db = await _make_sqlite_db(tmp)
        try:
            # naming：high 起步分 3.0 >= naming 阈值 3.0 -> 建簇即晋档 + 画像行
            rid = await _seed_raw(db, uid="u1", cat="naming",
                                  statement="希望被称为“阿青”")
            out = await db.apply_fact_merge(
                fact_id=rid, action="create", start_score=3.0, score_cap=10.0,
                promote_threshold=6.0, recent_promote_threshold=4.0,
                naming_promote_threshold=3.0,
            )
            assert out["status"] == "profiled", f"naming 单次明确表态应即时晋档: {out}"
            async with db.pool.acquire() as conn:
                prow = await conn.fetchrow(
                    "SELECT statement FROM memory_user_profile WHERE cluster_id = $1",
                    out["cluster_id"],
                )
            assert prow is not None, "建簇晋档须同事务写画像行"
            assert prow["statement"] == "希望被称为“阿青”"

            # stable：同样 3.0 < 通用阈值 6.0 -> active，无画像行
            rid2 = await _seed_raw(db, uid="u2", cat="stable", statement="小周不吃辣")
            out2 = await db.apply_fact_merge(
                fact_id=rid2, action="create", start_score=3.0, score_cap=10.0,
                promote_threshold=6.0, recent_promote_threshold=4.0,
                naming_promote_threshold=3.0,
            )
            assert out2["status"] == "active", f"stable 维度不受 naming 低阈值影响: {out2}"
            async with db.pool.acquire() as conn:
                n = await conn.fetchval(
                    "SELECT count(*) FROM memory_user_profile WHERE cluster_id = $1",
                    out2["cluster_id"],
                )
            assert n == 0

            # promote_pass 的 naming 臂：active naming 簇 3.0 -> 晋档；stable 3.0 不动
            async with db.pool.acquire() as conn:
                await conn.execute(
                    "UPDATE memory_fact_cluster SET score = 3.0 WHERE id = $1",
                    out2["cluster_id"],
                )
            promoted = await db.promote_pass(
                promote_threshold=6.0, recent_promote_threshold=4.0,
                naming_promote_threshold=3.0,
            )
            assert promoted == 0, "stable 3.0 不得经 naming 臂晋档"
            rid3 = await _seed_raw(db, uid="u3", cat="naming",
                                   statement="接受被称为“青哥”")
            out3 = await db.apply_fact_merge(
                fact_id=rid3, action="create", start_score=2.0, score_cap=10.0,
                promote_threshold=6.0, recent_promote_threshold=4.0,
                naming_promote_threshold=3.0,
            )
            assert out3["status"] == "active", "2.0 < 3.0 建簇不晋档"
            async with db.pool.acquire() as conn:
                await conn.execute(
                    "UPDATE memory_fact_cluster SET score = 3.0 WHERE id = $1",
                    out3["cluster_id"],
                )
            promoted = await db.promote_pass(
                promote_threshold=6.0, recent_promote_threshold=4.0,
                naming_promote_threshold=3.0,
            )
            assert promoted == 1, "active naming 簇达 3.0 应经 naming 臂晋档"
            await db.close()
        finally:
            await db.close()
    print("PASS test_naming_create_immediate_promotion_sqlite")


# ---------------------------------------------------------------------------
#  truncation guard
# ---------------------------------------------------------------------------

def test_looks_truncated_matrix() -> None:
    encoder_mod = load_modules("memory_encoder")[0]
    f = encoder_mod._looks_truncated
    assert f("【话题】爬山\n显示名: 内容发") is True, "多行末字为汉字 -> 截断"
    assert f("【话题】爬山\n显示名: 内容。") is False, "句号收尾完整"
    assert f("【话题】查ID") is False, "单行纯锚点 + ASCII 收尾合法"
    assert f("【话题】版本 2") is False
    assert f("【话题】摸鱼~") is False, "半角 ~ 收尾不误杀"
    assert f("【话题】爬山，") is True, "逗号族收尾必是半句"
    assert f("【话题】爬山（") is True, "开括号收尾必是半句"
    assert f("【话题】") is True, "空锚点残缺"
    assert f("   ") is True, "空串残缺"
    assert f("【话题】记忆系统操作") is False, "纯锚点形态合法"
    print("PASS test_looks_truncated_matrix")


async def test_encode_truncated_retries_then_degrades() -> None:
    encoder_mod = load_modules("memory_encoder")[0]

    class Scripted:
        def __init__(self, payloads):
            self._payloads = list(payloads)
            self.calls = 0

        async def run_structured(self, **kwargs):
            self.calls += 1
            return json.dumps(self._payloads.pop(0), ensure_ascii=False)

    good = {"summary": "【话题】爬山计划\n显示名: 约定了周末爬山。", "facts": []}
    half = {"summary": "【话题】爬山\n显示名: 约定了周末", "facts": []}

    llm = Scripted([half, good])
    enc = encoder_mod.MemoryEncoder(llm=llm, prompt_dir=PLUGIN_DIR / "prompts")
    summary, facts, rels, ok = await enc.encode("对话", "bot")
    assert ok is True and llm.calls == 2, "疑似截断应重试一次后采用完整摘要"
    assert summary == good["summary"]

    llm2 = Scripted([half, half])
    enc2 = encoder_mod.MemoryEncoder(llm=llm2, prompt_dir=PLUGIN_DIR / "prompts")
    summary2, _, _, ok2 = await enc2.encode("对话", "bot")
    assert ok2 is False and summary2 == "对话", (
        "重试后仍截断应降级原文单通道（summarized=false 不参与召回）"
    )
    assert llm2.calls == 2, "只重试一次，不做三次调用"
    print("PASS test_encode_truncated_retries_then_degrades")


# ---------------------------------------------------------------------------
#  encoder naming section toggle
# ---------------------------------------------------------------------------

async def test_encoder_naming_section_toggled() -> None:
    encoder_mod = load_modules("memory_encoder")[0]

    class Capture:
        def __init__(self):
            self.prompts: list[str] = []

        async def run_structured(self, **kwargs):
            self.prompts.append(kwargs.get("system_prompt") or "")
            return json.dumps(
                {"summary": "【话题】测试\n显示名: 完整摘要。", "facts": []},
                ensure_ascii=False,
            )

    cap = Capture()
    enc = encoder_mod.MemoryEncoder(
        llm=cap, prompt_dir=PLUGIN_DIR / "prompts", naming_enabled=True,
    )
    await enc.encode("对话", "bot")
    assert "称呼偏好专项" in cap.prompts[0], "开关开：提示词含 naming 判定节"

    cap2 = Capture()
    enc2 = encoder_mod.MemoryEncoder(
        llm=cap2, prompt_dir=PLUGIN_DIR / "prompts", naming_enabled=False,
    )
    await enc2.encode("对话", "bot")
    assert "称呼偏好专项" not in cap2.prompts[0], "开关关：提示词不含该段"
    print("PASS test_encoder_naming_section_toggled")


# ---------------------------------------------------------------------------
#  guidance convergence (#29): no per-tool routing in the shared block
# ---------------------------------------------------------------------------

def test_guidance_has_no_tool_routing() -> None:
    guidance = plugin_main._MEMORY_TOOLS_GUIDANCE
    assert "记忆工具使用准则" in guidance
    for tool_name in (
        "memory_search", "memory_lookup", "memory_profile",
        "memory_write", "memory_correct", "target=",
    ):
        assert tool_name not in guidance, (
            f"准则不得复述工具路由（{tool_name}）——描述是唯一口径（nori #29）"
        )
    print("PASS test_guidance_has_no_tool_routing")


async def main() -> None:
    await test_cross_session_pg_sql_shape()
    await test_cross_session_sqlite_fetch()
    await test_cross_session_memo_excludes_from_search()
    test_build_cross_session_block_budget()
    await test_naming_create_immediate_promotion_sqlite()
    test_looks_truncated_matrix()
    await test_encode_truncated_retries_then_degrades()
    await test_encoder_naming_section_toggled()
    test_guidance_has_no_tool_routing()
    print("ALL PASS")


if __name__ == "__main__":
    sys.exit(asyncio.run(main()))
