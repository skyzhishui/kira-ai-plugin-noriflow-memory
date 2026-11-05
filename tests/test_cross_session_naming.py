"""v1.18.2 ported-feature coverage: cross-session summary injection (nori
#31), naming low-threshold ingestion (nori #32), encoder truncation guard
(nori #26) and the guidance convergence (nori #29).

Coverage:
- cross-session: PG query shape (participants && / scoped session exclusion /
  future-timestamp bound / age window / order), SQLite twin fetch on a real
  temp DB (same bare id across session types not mis-excluded), kernel memo
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
        current_platform="qq",
        current_session_type="dm",
    )
    assert len(rows) == 1
    sql, params = conn.queries[0]
    assert (
        "NOT (session_id = $1 AND platform = $3 AND group_id = '')" in sql
    ), "排除当前会话须带 platform + 会话类型（裸 session_id 不唯一）"
    assert "participants && $2" in sql, "身份键数组重叠"
    assert "occurred_at <= now()" in sql, "排除未来时间戳（否则 DESC 抢占 top_k）"
    assert "occurred_at >= now() - make_interval(days => $4)" in sql, "陈旧窗口"
    assert sql.rstrip().endswith("ORDER BY occurred_at DESC, id DESC LIMIT $5")
    assert params == ("s1", ["qq:u1", "qq:u2"], "qq", 30, 2)

    # Group-chat arm: the type discriminator flips (group_id <> '')
    conn_gm = _CaptureConn([])
    db._pool = _CapturePool(conn_gm)
    await db.fetch_recent_cross_session_summaries(
        current_session_id="s1",
        user_keys=["qq:u1"],
        limit=3,
        current_platform="qq",
        current_session_type="gm",
    )
    sql_gm, params_gm = conn_gm.queries[0]
    assert (
        "NOT (session_id = $1 AND platform = $3 AND group_id <> '')" in sql_gm
    ), "群聊会话按 group_id <> '' 排除自身"
    assert "occurred_at <= now()" in sql_gm, "未来时间戳上界与年龄窗口无关"
    assert params_gm == ("s1", ["qq:u1"], "qq", 3)

    # Empty-key filtering (None/blank never enter the array binding)
    conn2 = _CaptureConn([])
    db._pool = _CapturePool(conn2)
    await db.fetch_recent_cross_session_summaries(
        current_session_id="s1",
        user_keys=["", None, "  "],
        limit=2,
        current_platform="qq",
        current_session_type="dm",
    )
    assert conn2.queries == [], "全无效键应直接短路不发查询"
    print("PASS test_cross_session_pg_sql_shape")


# ---------------------------------------------------------------------------
#  cross-session: SQLite twin fetch (real temp DB)
# ---------------------------------------------------------------------------

async def test_cross_session_sqlite_fetch() -> None:
    SQLiteMemoryDatabase = _db_pkg.SQLiteMemoryDatabase
    # Build data relative to the wall clock: the query side computes the
    # max_age_days cutoff from now(), so fixed timestamps rot after ~30 days
    # (the flakiness review flagged)
    wall_now = datetime.now(timezone.utc)
    with tempfile.TemporaryDirectory(prefix="nm_cs_") as tmp:
        config = LocalMemoryConfig(
            storage_backend="sqlite",
            sqlite_path=str(Path(tmp) / "m.db"),
            embedding_dims=4,
        )
        db = await _make_sqlite_db(tmp)
        try:
            old = wall_now - timedelta(days=40)
            for doc, sid, plat, gid, occurred, parts in [
                # another DM session, inside the age window
                ("d-other", "s2", "qq", "", wall_now - timedelta(hours=2),
                 ["qq:u1"]),
                # the current DM session itself
                ("d-current", "s1", "qq", "", wall_now - timedelta(hours=1),
                 ["qq:u1"]),
                # same bare id as the current DM but a group chat: a different
                # session, so a bare-id-only exclusion would wrongly drop it
                ("d-same-id-group", "s1", "qq", "s1",
                 wall_now - timedelta(hours=3), ["qq:u1"]),
                # same bare id + same type, other adapter: another session too
                ("d-same-id-otherplat", "s1", "napcat", "",
                 wall_now - timedelta(hours=4), ["napcat:u1"]),
                # future-dated row (clock skew): must never be injected
                ("d-future", "s5", "qq", "",
                 wall_now + timedelta(days=1), ["qq:u1"]),
                # out of the age window
                ("d-other-old", "s3", "qq", "", old, ["qq:u1"]),
                # someone else's session
                ("d-stranger", "s4", "qq", "", wall_now - timedelta(hours=1),
                 ["qq:u9"]),
            ]:
                await db.insert_chat_summary(
                    document_id=doc, kind="chat_summary", platform=plat,
                    session_id=sid, group_id=gid, user_id="u1",
                    participants=parts, content=f"摘要-{doc}",
                    occurred_at=occurred, summarized=True, embedding=None,
                )
            rows = await db.fetch_recent_cross_session_summaries(
                current_session_id="s1", user_keys=["qq:u1", "napcat:u1"],
                limit=10, max_age_days=30,
                current_platform="qq", current_session_type="dm",
            )
            assert [r["document_id"] for r in rows] == [
                "d-other", "d-same-id-group", "d-same-id-otherplat",
            ], (
                "仅命中其他会话、本人参与、窗口内的行（当前 DM/同裸 ID 群聊外的"
                "会话不误排除；陈旧行/未来行/陌生人行排除）"
            )
            rows_all = await db.fetch_recent_cross_session_summaries(
                current_session_id="s1", user_keys=["qq:u1", "napcat:u1"],
                limit=10, max_age_days=0,
                current_platform="qq", current_session_type="dm",
            )
            assert [r["document_id"] for r in rows_all] == [
                "d-other", "d-same-id-group", "d-same-id-otherplat",
                "d-other-old",
            ], "max_age_days=0 不限陈旧度（未来时间戳仍排除），按发生时间新->旧"
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
    """Cross-session memo joins the shared recall exclusion channel.

    Registered rows are excluded from search; clearing the memo restores
    them.
    """
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


async def test_cross_session_memo_isolated_per_turn() -> None:
    """Concurrent rounds of one session must not clear each other's memo.

    Round B's entry clear / registration only touches its own turn key, so
    round A's already-injected ids stay excluded until A's recall runs.
    """
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
    # Round A registers its injected ids, then round B starts (entry clear)
    # and registers its own before A's recall runs.
    kernel.refresh_cross_session_memo("s1", ["doc-a"], turn_id="turn-a")
    kernel.refresh_cross_session_memo("s1", [], turn_id="turn-b")
    kernel.refresh_cross_session_memo("s1", ["doc-b"], turn_id="turn-b")

    await kernel.search(query="猫", session_id="s1", top_k=3, turn_id="turn-a")
    assert db.search_kwargs["exclude_document_ids"] == ["doc-a"], (
        "B 轮入口清空/登记不得清掉 A 轮的排除集合"
    )
    await kernel.search(query="猫", session_id="s1", top_k=3, turn_id="turn-b")
    assert db.search_kwargs["exclude_document_ids"] == ["doc-b"], (
        "B 轮只排除自己注入的行"
    )
    print("PASS test_cross_session_memo_isolated_per_turn")


async def test_cross_session_memo_isolated_per_context() -> None:
    """Same bare session_id in different platform/session-type contexts must
    keep independent memos.

    The host does not guarantee a session_id is unique across adapters, and a
    DM with a user can share its id with a group chat; without platform +
    session_type in the key, one context's clear/registration would wipe the
    other's exclusions and leak already-injected summaries back into recall.
    """
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
    for platform, stype, doc in (
        ("qq", "dm", "doc-qq-dm"),
        ("web", "dm", "doc-web-dm"),
        ("qq", "gm", "doc-qq-gm"),
    ):
        kernel.refresh_cross_session_memo(
            "s1", [doc], turn_id="t", platform=platform, session_type=stype,
        )

    for platform, stype, expected in (
        ("qq", "dm", "doc-qq-dm"),
        ("web", "dm", "doc-web-dm"),
        ("qq", "gm", "doc-qq-gm"),
    ):
        await kernel.search(
            query="猫", session_id="s1", top_k=3, turn_id="t",
            platform=platform, session_type=stype,
        )
        assert db.search_kwargs["exclude_document_ids"] == [expected], (
            f"{platform}/{stype} 只应排除自己上下文的注入行"
        )

    # Clearing one context must not clear the other contexts sharing the id
    kernel.refresh_cross_session_memo(
        "s1", [], turn_id="t", platform="web", session_type="dm",
    )
    await kernel.search(
        query="猫", session_id="s1", top_k=3, turn_id="t",
        platform="web", session_type="dm",
    )
    assert db.search_kwargs["exclude_document_ids"] is None
    await kernel.search(
        query="猫", session_id="s1", top_k=3, turn_id="t",
        platform="qq", session_type="dm",
    )
    assert db.search_kwargs["exclude_document_ids"] == ["doc-qq-dm"]
    print("PASS test_cross_session_memo_isolated_per_context")


async def test_cross_session_memo_prune_keeps_inflight() -> None:
    """Capacity pruning must never evict a round that may still be executing.

    A plain newest-N cap drops the oldest entries even when their recall has
    not run yet (64 later rounds registering first would silently un-exclude
    the older round's injected summaries). Only entries past the in-flight
    window are capacity candidates; the TTL still bounds the memo lifetime.
    """
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

    def _kernel():
        return kernel_mod.LocalMemoryKernel(
            db=_SearchCaptureDB(rows=[]),
            embedding_service=_StubEmbedding(),
            circuit_breaker=_StubBreaker(),
            config=LocalMemoryConfig(dsn="postgresql://stub"),
            bot_id="bot",
        )

    now = kernel_mod.time.monotonic()
    inflight = kernel_mod.LocalMemoryKernel._CROSS_SESSION_MEMO_INFLIGHT_SECONDS
    max_entries = kernel_mod.LocalMemoryKernel._ROLLOUT_MEMO_MAX_SESSIONS

    # All fresh: over the cap but all in flight -> nothing is evicted
    fresh_only = _kernel()
    fresh_only._cross_session_memo = {
        ("qq", "dm", "s", f"fresh{i}"): ([f"f{i}"], now)
        for i in range(max_entries + 6)
    }
    fresh_only._prune_cross_session_memo()
    assert len(fresh_only._cross_session_memo) == max_entries + 6, (
        "仍在执行的轮次不得被数量上限淘汰"
    )

    # Old (past the in-flight window, within TTL) + fresh: only the oldest
    # old entries beyond the cap are dropped; every fresh entry survives
    kernel = _kernel()
    kernel._cross_session_memo = {
        ("qq", "dm", "s", f"old{i}"): ([f"d{i}"], now - inflight - 100.0)
        for i in range(10)
    }
    for i in range(60):
        kernel._cross_session_memo[("qq", "dm", "s", f"fresh{i}")] = (
            [f"f{i}"], now,
        )
    kernel._prune_cross_session_memo()
    assert len(kernel._cross_session_memo) == max_entries, str(
        len(kernel._cross_session_memo)
    )
    assert all(
        ("qq", "dm", "s", f"fresh{i}") in kernel._cross_session_memo
        for i in range(60)
    ), "fresh（in-flight）轮次必须全部保留"
    assert sum(
        1 for key in kernel._cross_session_memo if key[3].startswith("old")
    ) == 4, "只淘汰超出上限的已完成轮次"

    # TTL sweep still removes expired entries regardless of the cap
    kernel._cross_session_memo[("qq", "dm", "s", "expired")] = (
        ["x"], now - kernel_mod.LocalMemoryKernel._ROLLOUT_MEMO_TTL_SECONDS - 1,
    )
    kernel._prune_cross_session_memo()
    assert ("qq", "dm", "s", "expired") not in kernel._cross_session_memo
    print("PASS test_cross_session_memo_prune_keeps_inflight")


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

    # Tiny budget: the preamble eats it -> no entries (neither injected nor
    # registered, rows stay recallable)
    tiny_block, tiny_ids = plugin_main.build_cross_session_block(
        [row_new], now=_NOW, tz=timezone.utc, budget=120,
    )
    assert tiny_block == "" and tiny_ids == []

    # Budgets are derived from the real entry structure to pin down both
    # truncation branches of the assembler (review B1: the old hardcoded 260
    # and 120 both hit the same room<=0 branch, leaving truncation uncovered)
    preamble = plugin_main._CROSS_SESSION_PREAMBLE
    used = len(preamble) + 2
    entry = plugin_main._format_cross_session_entry(
        row_new, now=_NOW, tz=timezone.utc, limit=1500
    )
    numbered = f"1. {entry}"
    pos = numbered.find(plugin_main._CROSS_SESSION_CONTENT_MARKER)

    # Branch 1 (room > 0 and body residue < 10): drop the entry whole after
    # truncation (neither injected nor registered)
    room_abandon = pos  # cut lands before the content marker -> residue < 10
    b1, i1 = plugin_main.build_cross_session_block(
        [row_new], now=_NOW, tz=timezone.utc,
        budget=used + plugin_main._CROSS_SESSION_TRUNCATE_SUFFIX_LEN + room_abandon,
    )
    assert b1 == "" and i1 == [], (
        "截断至元数据-only 的条目必须放弃（防注入+登记造成召回盲区）"
    )

    # Branch 2 (room > 0 and body residue >= 10): inject the truncated entry
    # and register it
    room_keep = pos + len(plugin_main._CROSS_SESSION_CONTENT_MARKER) + 12
    b2, i2 = plugin_main.build_cross_session_block(
        [row_new], now=_NOW, tz=timezone.utc,
        budget=used + plugin_main._CROSS_SESSION_TRUNCATE_SUFFIX_LEN + room_keep,
    )
    assert b2 != "" and i2 == ["d1"], "截断但仍保留正文切片的条目应注入并登记"
    assert b2.rstrip().endswith("..."), "截断注入的条目应以省略号收尾"

    # Intermediate memo contract: the budget fits only the first entry -> the
    # second is budget-trimmed and not registered (stays recallable)
    entry_old = plugin_main._format_cross_session_entry(
        row_old, now=_NOW, tz=timezone.utc, limit=1500
    )
    numbered_old = f"2. {entry_old}"
    b3, i3 = plugin_main.build_cross_session_block(
        [row_new, row_old], now=_NOW, tz=timezone.utc,
        budget=used + len(numbered) + 2 + len(numbered_old) - 5,
    )
    assert i3 == ["d1"], "预算裁剪的次条不得登记 memo"
    assert "摘要内容：用户在另一" in b3 and "摘要内容：更早" not in b3

    # Empty-content rows are skipped
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
            # naming: high start score 3.0 >= naming threshold 3.0 -> promote
            # at cluster creation + profile row
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

            # stable: same 3.0 < generic threshold 6.0 -> active, no row
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

            # naming arm of promote_pass: an active naming cluster at 3.0 gets
            # promoted; stable at 3.0 is untouched
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
        finally:
            await db.close()
    print("PASS test_naming_create_immediate_promotion_sqlite")


# ---------------------------------------------------------------------------
#  truncation guard
# ---------------------------------------------------------------------------

def test_looks_truncated_matrix() -> None:
    encoder_mod = load_modules("memory_encoder")[0]
    f = encoder_mod._looks_truncated
    assert f("【话题】爬山\n显示名: 内容发") is False, (
        "多行末字为汉字不再判截断（语义完整的短句同样以汉字收尾）"
    )
    assert f("【话题】爬山\n显示名: 内容。") is False, "句号收尾完整"
    assert f("【话题】爬山\n显示名: 内容，") is True, "多行逗号族收尾必是半句"
    assert f("【话题】爬山\n显示名: 内容（") is True, "多行开括号收尾必是半句"
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
    half = {"summary": "【话题】爬山\n显示名: 约定了周末，", "facts": []}

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
    await test_cross_session_memo_isolated_per_turn()
    await test_cross_session_memo_isolated_per_context()
    await test_cross_session_memo_prune_keeps_inflight()
    test_build_cross_session_block_budget()
    await test_naming_create_immediate_promotion_sqlite()
    test_looks_truncated_matrix()
    await test_encode_truncated_retries_then_degrades()
    await test_encoder_naming_section_toggled()
    test_guidance_has_no_tool_routing()
    print("ALL PASS")


if __name__ == "__main__":
    sys.exit(asyncio.run(main()))
