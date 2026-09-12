"""Recall pipeline tests (direct-run, no pytest dependency).

Merged from the former test_recall_quality.py / test_hybrid_rollout.py /
test_recall_hints.py (2026-09-12 review cleanup: the three files shared
identical stub boilerplate, now consolidated by domain; expired tests were
trimmed — the hint-bypass hasattr negative-existence asserts and the
make_recall_hints signature lock, the former being long-stable behavior and
the latter duplicating test_entity_edge's form-learning case).

Coverage:
- near-duplicate dedup: cosine >= threshold dropped / threshold 0 disables /
  missing vectors stay visible / early stop once top_k is filled;
- recency exclusion (batch-count anchored): the window batch count is read
  live via the provider (max_memory_length) and pushed down as
  NOT (session_id AND id IN (latest K batches));
- recall expansion: participants of the latest N summary batches join the
  expanded key set (bot excluded); SQL-side expanded hits are always pinned
  to the current session (independent of the cross-session switch);
- relative time labels: timezone-chain labels appended to injection bullets;
- recall_log: one JSONL line each for search/inject events; nothing is
  written when disabled;
- SQL assembly: placeholder numbering matches params one-to-one, semantic
  clauses present;
- build_search_text tokenization + both write points land search_text;
- hybrid retrieval: dual-leg query, RRF fusion order, single leg when
  disabled, row-exclusion push-down;
- rolling restore block: skip the latest K batches, oldest->newest
  presentation, budget truncation, memo-driven recall row exclusion, no
  exclusion after TTL expiry;
- search_text backfill pass: still fills when the embedding service is
  unavailable;
- _EntityDirectory: build filtering/TTL/degradation, kernel directory
  entry, compound-key projection;
- main injection layer: _entity_match_text / _reply_quote_text /
  _directory_names / _persona_candidates dictionary-level backfill.

Run (plugin dir):
    python tests/test_recall.py
"""

from __future__ import annotations

import asyncio
import json
import re
import sys
import tempfile
import time
from datetime import datetime, timedelta, timezone
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))

from _harness import load_module  # noqa: E402

PLUGIN_DIR = Path(__file__).resolve().parent.parent

_circuit_breaker = load_module("circuit_breaker")
_config = load_module("config")
_db = load_module("db")
_kernel = load_module("memory_kernel")
_recall_log = load_module("recall_log")
_vector_ops = load_module("vector_ops")
_main = load_module("main")

MemoryDBCircuitBreaker = _circuit_breaker.MemoryDBCircuitBreaker
LocalMemoryConfig = _config.LocalMemoryConfig
MemoryDatabase = _db.MemoryDatabase
parse_vector = _db.parse_vector
build_search_text = _db.build_search_text
LocalMemoryKernel = _kernel.LocalMemoryKernel
RecallHints = _kernel.RecallHints
_EntityDirectory = _kernel._EntityDirectory
RecallLogWriter = _recall_log.RecallLogWriter
EmbeddingBackfillTask = _vector_ops.EmbeddingBackfillTask

_NOW = datetime(2026, 9, 1, 12, 0, 0, tzinfo=timezone.utc)

_ROW_SEQ = iter(range(1, 10000))


# ---------------------------------------------------------------------------
#  Shared stubs
# ---------------------------------------------------------------------------


class _FakeBreaker(MemoryDBCircuitBreaker):
    async def is_available(self) -> bool:  # noqa: D102
        return True

    async def record_failure(self) -> None:  # noqa: D102
        pass

    async def record_success(self) -> None:  # noqa: D102
        pass


class _StubEmbedding:
    """embed_one always returns a vector (the real EmbeddingService returns None without a client)."""

    async def embed_one(self, text):
        return [0.1]

    async def embed_batch(self, texts):
        return [[0.1] for _ in texts]


class _CaptureDB:
    """Stub DB capturing search kwargs (including the expansion-participants query)."""

    def __init__(self, rows=None, participants_rows=None) -> None:
        self.rows = rows or []
        self.participants_rows = participants_rows or []
        self.search_kwargs: dict | None = None
        self.participants_calls: list[dict] = []

    async def search_chat_summaries(self, **kwargs):
        self.search_kwargs = kwargs
        return list(self.rows)

    async def fetch_recent_session_participants(self, **kwargs):
        self.participants_calls.append(kwargs)
        return list(self.participants_rows)


class _RolloutDB:
    """Stub for the search/rolling-restore consumer side: captures call kwargs, returns canned rows."""

    def __init__(self, rows=None, rollout_rows=None) -> None:
        self.rows = rows or []
        self.rollout_rows = rollout_rows or []
        self.search_kwargs: dict | None = None
        self.rollout_calls: list[dict] = []

    async def search_chat_summaries(self, **kwargs):
        self.search_kwargs = kwargs
        return list(self.rows)

    async def fetch_recent_session_participants(self, **kwargs):
        return []

    async def fetch_recent_rollout_summaries(self, **kwargs):
        self.rollout_calls.append(kwargs)
        return list(self.rollout_rows)


class FakeHintDB:
    """Stub DB for the recall consumer side (entity directory path): records call kwargs, returns canned rows."""

    def __init__(self, rows=None):
        self.rows = rows or []
        self.calls: list[dict] = []

    async def search_chat_summaries(self, **kwargs) -> list[dict]:
        self.calls.append(kwargs)
        return self.rows

    async def alias_fetch_all(self) -> list[dict]:
        return []  # empty persistent alias table (window-path tests touch no alias rows)

    async def fetch_recent_session_participants(self, **kwargs) -> list[dict]:
        return []


class FakeRerankClient:
    def __init__(self, result=None):
        self.result = result

    async def rerank(self, query: str, documents: list[str], top_k: int = 10):
        return self.result


def _row(doc_id: str, relevance: float, occurred_at, embedding=None) -> dict:
    return {
        "id": next(_ROW_SEQ),
        "document_id": doc_id,
        "kind": "chat_summary",
        "session_id": "s1",
        "user_id": "123",
        "content": f"内容-{doc_id}",
        "occurred_at": occurred_at,
        "relevance": relevance,
        "embedding": embedding,
    }


def _hint_row(row_id: int, content: str, *, relevance: float = 0.9, **extra) -> dict:
    row = {
        "id": row_id,
        "document_id": f"doc-{row_id}",
        "kind": "chat_summary",
        "session_id": "s1",
        "user_id": "u1",
        "content": content,
        "occurred_at": datetime(2026, 8, 18, 20, 0),
        "relevance": relevance,
    }
    row.update(extra)
    return row


def _make_kernel(db, *, window_batches=None, rerank=None, **cfg_over) -> LocalMemoryKernel:
    config = LocalMemoryConfig(dsn="postgresql://stub", **cfg_over)
    provider = None if window_batches is None else (lambda: window_batches)
    kernel = LocalMemoryKernel(
        db=db,
        embedding_service=_StubEmbedding(),
        circuit_breaker=_FakeBreaker(failure_threshold=5, recovery_seconds=1.0),
        config=config,
        bot_id="bot001",
        rerank_client=rerank,
        history_window_provider=provider,
    )
    kernel._now = staticmethod(lambda: _NOW)  # frozen clock so labels/decay are exactly assertable
    return kernel


def _vec_str(vec: list[float]) -> str:
    return "[" + ",".join(repr(v) for v in vec) + "]"


# ----------------------------------------------------------------------
# Near-duplicate dedup
# ----------------------------------------------------------------------

async def test_dedup_drops_near_duplicate() -> None:
    # two rows with cosine=1.0 embeddings (same direction): the second must be dropped
    a = _row("a", 0.9, _NOW - timedelta(days=10), _vec_str([1.0, 0.0, 0.0]))
    b = _row("b", 0.8, _NOW - timedelta(days=11), _vec_str([1.0, 0.0, 0.0]))
    db = _CaptureDB(rows=[a, b])
    kernel = _make_kernel(db)
    items = await kernel.search(query="猫", session_id="s1")
    assert [i.id for i in items] == ["a"], "近重复（cosine=1.0）应被丢弃"
    print("PASS test_dedup_drops_near_duplicate")


async def test_dedup_keeps_dissimilar() -> None:
    a = _row("a", 0.9, _NOW - timedelta(days=10), _vec_str([1.0, 0.0]))
    b = _row("b", 0.8, _NOW - timedelta(days=11), _vec_str([0.0, 1.0]))
    db = _CaptureDB(rows=[a, b])
    kernel = _make_kernel(db)
    items = await kernel.search(query="猫", session_id="s1")
    assert [i.id for i in items] == ["a", "b"], "正交向量不应被去重"
    print("PASS test_dedup_keeps_dissimilar")


async def test_dedup_threshold_boundary() -> None:
    # cosine ~= 0.71: kept at threshold 0.9, dropped at 0.7 (>= semantics)
    a = _row("a", 0.9, _NOW - timedelta(days=10), _vec_str([1.0, 0.0]))
    b = _row("b", 0.8, _NOW - timedelta(days=11), _vec_str([1.0, 1.0]))
    db = _CaptureDB(rows=[a, b])
    items = await _make_kernel(db).search(query="猫", session_id="s1")
    assert len(items) == 2, "cosine≈0.71 < 0.9 不应丢弃"
    db2 = _CaptureDB(rows=[a, b])
    items2 = await _make_kernel(db2, dedup_similarity_threshold=0.7).search(
        query="猫", session_id="s1"
    )
    assert [i.id for i in items2] == ["a"], "cosine≈0.71 ≥ 0.7 应丢弃"
    print("PASS test_dedup_threshold_boundary")


async def test_dedup_disabled_and_missing_vector() -> None:
    a = _row("a", 0.9, _NOW - timedelta(days=10), _vec_str([1.0, 0.0]))
    b = _row("b", 0.8, _NOW - timedelta(days=11), _vec_str([1.0, 0.0]))
    db = _CaptureDB(rows=[a, b])
    items = await _make_kernel(db, dedup_similarity_threshold=0.0).search(
        query="猫", session_id="s1"
    )
    assert len(items) == 2, "阈值 0 应关闭去重"

    c = _row("c", 0.7, _NOW - timedelta(days=12), None)
    d = _row("d", 0.6, _NOW - timedelta(days=13), None)
    db2 = _CaptureDB(rows=[c, d])
    items2 = await _make_kernel(db2).search(query="猫", session_id="s1")
    assert len(items2) == 2, "向量缺失的行保底可见，不应被丢弃"
    print("PASS test_dedup_disabled_and_missing_vector")


async def test_dedup_early_stop_at_top_k() -> None:
    # 5 mutually dissimilar rows with top_k=2: early stop means rows 3+ are not scanned (result still the first 2)
    rows = [
        _row(f"r{i}", 0.9 - i * 0.01, _NOW - timedelta(days=i + 3))
        for i in range(5)
    ]
    # without embeddings the order falls back to relevance; add distinct vectors to exercise the early-stop path
    axes = [[1.0, 0.0, 0.0], [0.0, 1.0, 0.0], [0.0, 0.0, 1.0],
            [1.0, 1.0, 0.0], [0.0, 1.0, 1.0]]
    for r, v in zip(rows, axes):
        r["embedding"] = _vec_str(v)
    db = _CaptureDB(rows=rows)
    items = await _make_kernel(db).search(query="猫", session_id="s1", top_k=2)
    assert [i.id for i in items] == ["r0", "r1"], "top_k 截断优先于去重扫描"
    print("PASS test_dedup_early_stop_at_top_k")


# ----------------------------------------------------------------------
# Recency exclusion (batch-count anchored)
# ----------------------------------------------------------------------

async def test_window_batches_passed_to_search() -> None:
    """The window batch count is read live via the provider and pushed down to SQL (batch-count anchoring = host max_memory_length)."""
    db = _CaptureDB(rows=[_row("a", 0.9, _NOW - timedelta(days=5))])
    kernel = _make_kernel(db, window_batches=30)
    await kernel.search(query="猫", session_id="s1")
    assert db.search_kwargs.get("exclude_recent_batches") == 30, (
        "近时排除下推窗口块数"
    )

    # the provider is read on every search (a live host-side change takes effect immediately)
    db2 = _CaptureDB(rows=[_row("a", 0.9, _NOW - timedelta(days=5))])
    holder = {"k": 30}
    kernel2 = _make_kernel(db2)
    kernel2._history_window_provider = lambda: holder["k"]
    await kernel2.search(query="猫", session_id="s1")
    assert db2.search_kwargs.get("exclude_recent_batches") == 30
    holder["k"] = 10
    await kernel2.search(query="猫", session_id="s1")
    assert db2.search_kwargs.get("exclude_recent_batches") == 10, (
        "热改后下一次检索即时生效"
    )
    print("PASS test_window_batches_passed_to_search")


async def test_window_disabled_or_no_provider() -> None:
    db = _CaptureDB(rows=[_row("a", 0.9, _NOW - timedelta(days=5))])
    kernel = _make_kernel(db)  # no provider -> no exclusion
    await kernel.search(query="猫", session_id="s1")
    assert db.search_kwargs.get("exclude_recent_batches") == 0

    db2 = _CaptureDB(rows=[_row("a", 0.9, _NOW - timedelta(days=5))])
    kernel2 = _make_kernel(db2, window_batches=30, recall_exclude_history_window=False)
    await kernel2.search(query="猫", session_id="s1")
    assert db2.search_kwargs.get("exclude_recent_batches") == 0, "开关关闭时不排除"

    db3 = _CaptureDB(rows=[_row("a", 0.9, _NOW - timedelta(days=5))])
    kernel3 = _make_kernel(db3)
    def _boom():
        raise RuntimeError("宿主配置读取失败（测试注入）")
    kernel3._history_window_provider = _boom
    await kernel3.search(query="猫", session_id="s1")
    assert db3.search_kwargs.get("exclude_recent_batches") == 0, "读取失败回退关闭"
    print("PASS test_window_disabled_or_no_provider")


def test_zero_window_warns_once() -> None:
    """With host max_memory_length=0 ([-0:] = unlimited window) the warning fires exactly once (deduplicated)."""
    import logging

    kernel = _make_kernel(_CaptureDB(), window_batches=0)
    logger = logging.getLogger("stub")
    records: list = []

    class _Handler(logging.Handler):
        def emit(self, record):
            records.append(record)

    handler = _Handler()
    logger.addHandler(handler)
    try:
        assert kernel._derive_window_batches() == 0
        kernel._derive_window_batches()  # second call must not warn again
    finally:
        logger.removeHandler(handler)
    warns = [r for r in records if "max_memory_length" in r.getMessage()]
    assert len(warns) == 1, "零窗口告警恰好一次"
    print("PASS test_zero_window_warns_once")


async def test_platform_passed_through_search() -> None:
    """platform is passed through to search/expansion (cross-adapter isolation for bare session_ids)."""
    db = _CaptureDB(
        rows=[_row("a", 0.9, _NOW - timedelta(days=5))],
        participants_rows=[{"participants": ["napcat:111"], "user_id": "111"}],
    )
    kernel = _make_kernel(db, window_batches=10)
    await kernel.search(
        query="猫", session_id="s1", user_id="123", platform="napcat"
    )
    assert db.search_kwargs.get("platform") == "napcat"
    assert db.participants_calls[0]["platform"] == "napcat", "扩选查询须限定平台"
    print("PASS test_platform_passed_through_search")


# ----------------------------------------------------------------------
# Recall expansion
# ----------------------------------------------------------------------

async def test_expansion_derives_recent_participants() -> None:
    db = _CaptureDB(
        rows=[_row("a", 0.9, _NOW - timedelta(days=5))],
        participants_rows=[
            {"participants": ["qq:111", "qq:bot001"], "user_id": "111"},
            {"participants": ["qq:222"], "user_id": "222"},
            {"participants": ["qq:111"], "user_id": "111"},  # duplicate, must be deduped
        ],
    )
    kernel = _make_kernel(db)
    await kernel.search(query="猫", session_id="s1", user_id="123", platform="qq")
    assert len(db.participants_calls) == 1, "扩选查询恰好一次"
    assert db.participants_calls[0]["session_id"] == "s1"
    kw = db.search_kwargs
    assert sorted(kw["expanded_user_keys"]) == ["qq:111", "qq:222"], (
        "扩展键=最近参与者去重，bot 自身剔除"
    )
    assert sorted(kw["expanded_user_ids"]) == ["111", "222"]
    print("PASS test_expansion_derives_recent_participants")


async def test_expansion_disabled_or_wrong_scope() -> None:
    db = _CaptureDB(rows=[_row("a", 0.9, _NOW - timedelta(days=5))])
    kernel = _make_kernel(db, recall_expansion_enabled=False)
    await kernel.search(query="猫", session_id="s1")
    assert db.participants_calls == [], "关闭时不查参与者"
    assert db.search_kwargs.get("expanded_user_keys") is None

    db2 = _CaptureDB(rows=[_row("a", 0.9, _NOW - timedelta(days=5))])
    kernel2 = _make_kernel(db2)
    await kernel2.search(query="猫", session_id="s1", scope="user", user_id="123")
    assert db2.participants_calls == [], "scope=user 不扩选（无会话隔离诉求）"
    print("PASS test_expansion_disabled_or_wrong_scope")


# ----------------------------------------------------------------------
# Relative time labels
# ----------------------------------------------------------------------

async def test_time_labels_appended() -> None:
    rows = [
        _row("today", 0.9, _NOW - timedelta(hours=2)),
        _row("yest", 0.85, _NOW - timedelta(days=1)),
        _row("d3", 0.8, _NOW - timedelta(days=3)),
        _row("w2", 0.75, _NOW - timedelta(days=18)),
        _row("m6", 0.7, _NOW - timedelta(days=184)),
    ]
    db = _CaptureDB(rows=rows)
    kernel = _make_kernel(db, timezone="UTC")  # fixed timezone so day-granularity labels are assertable
    text = await kernel.build_injection_text(query="猫", session_id="s1", top_k=6)
    assert "（今天）" in text and "（昨天）" in text, "日内/隔日标注"
    assert "（3天前）" in text and "（约2周前）" in text, "天/周粒度标注"
    assert "（约6个月前）" in text, "月粒度标注"
    assert "- 内容-today（今天）" in text, "标注应在 bullet 尾部"
    print("PASS test_time_labels_appended")


async def test_time_label_falls_back_to_host_tz() -> None:
    """When the plugin timezone is unset, fall back to the host locale.TZ (KiraAI timezone chain)."""
    from zoneinfo import ZoneInfo

    rows = [_row("a", 0.9, _NOW - timedelta(days=1))]
    kernel = _make_kernel(_CaptureDB(rows=rows))  # no timezone
    kernel._host_tz_provider = lambda: ZoneInfo("UTC")
    assert kernel._local_tz() is not None
    text = await kernel.build_injection_text(query="猫", session_id="s1")
    assert "（昨天）" in text, "宿主时区承接标注换算"
    print("PASS test_time_label_falls_back_to_host_tz")


async def test_time_label_disabled() -> None:
    rows = [_row("a", 0.9, _NOW - timedelta(days=3))]
    kernel = _make_kernel(_CaptureDB(rows=rows), recall_time_label_enabled=False)
    text = await kernel.build_injection_text(query="猫", session_id="s1")
    assert text.splitlines()[1] == "- 内容-a", "关闭时与 hindsight 逐字一致"
    print("PASS test_time_label_disabled")


# ----------------------------------------------------------------------
# recall_log
# ----------------------------------------------------------------------

async def test_recall_log_writes_search_and_inject() -> None:
    with tempfile.TemporaryDirectory() as tmp:
        log_path = Path(tmp) / "recall.jsonl"
        db = _CaptureDB(rows=[_row("a", 0.9, _NOW - timedelta(days=3))])
        kernel = _make_kernel(db)
        kernel._recall_log = RecallLogWriter(log_path)
        await kernel.build_injection_text(query="猫", session_id="s1")
        lines = [json.loads(x) for x in log_path.read_text(encoding="utf-8").splitlines()]
        assert [e["event"] for e in lines] == ["search", "inject"], "检索+注入两事件"
        assert lines[0]["query"] == "猫" and lines[0]["candidates"] == 1
        assert lines[0]["kept"][0]["id"] == "a"
        assert lines[1]["injected"][0]["id"] == "a"
        assert lines[1]["injected"][0]["label"], "注入事件应带时间标注"

        # disabled (no writer) -> nothing written
        db2 = _CaptureDB(rows=[_row("a", 0.9, _NOW - timedelta(days=3))])
        kernel2 = _make_kernel(db2)
        await kernel2.build_injection_text(query="猫", session_id="s1")
        assert not kernel2._recall_log
    print("PASS test_recall_log_writes_search_and_inject")


# ----------------------------------------------------------------------
# parse_vector / SQL assembly
# ----------------------------------------------------------------------

def test_parse_vector() -> None:
    assert parse_vector("[0.1,0.2]") == [0.1, 0.2]
    assert parse_vector([0.1, 0.2]) == [0.1, 0.2]
    assert parse_vector("not-a-vec") is None
    assert parse_vector(None) is None
    assert parse_vector("[bad]") is None
    print("PASS test_parse_vector")


class _FakeConn:
    def __init__(self) -> None:
        self.queries: list[tuple[str, tuple]] = []

    async def fetch(self, sql, *params):
        self.queries.append((sql, params))
        return []


class _FakeAcquire:
    def __init__(self, conn) -> None:
        self._conn = conn

    async def __aenter__(self):
        return self._conn

    async def __aexit__(self, *exc):
        return False


class _FakePool:
    def __init__(self) -> None:
        self.conn = _FakeConn()

    def acquire(self):
        return _FakeAcquire(self.conn)


async def test_sql_assembly_expansion_and_window() -> None:
    db = MemoryDatabase.__new__(MemoryDatabase)
    db._pool = _FakePool()
    await db.search_chat_summaries(
        query_vec=[0.1, 0.2],
        limit=50,
        scope="session",
        session_id="s1",
        user_keys=["qq:123"],
        user_ids=["123"],
        expanded_user_keys=["qq:111", "qq:222"],
        expanded_user_ids=["111", "222"],
        exclude_recent_batches=30,
    )
    sql, params = db._pool.conn.queries[0]
    # placeholder numbering matches the param count one-to-one (construction order = appearance order = append order)
    placeholders = {int(n) for n in re.findall(r"\$(\d+)", sql)}
    assert placeholders == set(range(1, len(params) + 1)), (
        f"占位符 {sorted(placeholders)} 应与 {len(params)} 个参数一一对应"
    )
    assert "embedding" in sql.split("FROM")[0], "SELECT 应带 embedding（去重用）"
    # the bot_self exemption outlet stays and is narrowed to within-session
    # (v1.9.x privacy stance: bot self-statements from other sessions no
    # longer enter this session's/user's recall)
    assert re.search(
        r"OR \(kind = 'bot_self' AND session_id = \$\d+\)\)", sql
    ), "用户条件须保留会话内 bot_self 豁免出口"
    assert re.search(
        r"AND summarized AND session_id = \$\d+", sql
    ), "扩展命中恒钉死当前会话"
    assert re.search(
        r"NOT \(session_id = \$\d+ AND id IN \(SELECT id FROM memory_chat_summary "
        r"WHERE session_id = \$\d+ AND summarized "
        r"ORDER BY occurred_at DESC, id DESC LIMIT \$\d+\)\)",
        sql,
    ), "近时排除子句（块数锚定子查询）"
    # the primary key group comes before the expanded group ($ numbering reflects construction order)
    assert sql.index("participants && $") < re.search(
        r"AND summarized AND session_id = \$\d+", sql
    ).start(), "主键组先于扩展键组"
    print("PASS test_sql_assembly_expansion_and_window")


async def test_sql_assembly_platform_qualified() -> None:
    """With a non-empty platform, the session-scope condition / recency-exclusion subquery / expansion-pinned block all gain platform qualifiers."""
    db = MemoryDatabase.__new__(MemoryDatabase)
    db._pool = _FakePool()
    await db.search_chat_summaries(
        query_vec=[0.1], limit=10, scope="session", session_id="s1",
        platform="napcat",
        exclude_recent_batches=30,
        expanded_user_keys=["napcat:111"],
    )
    sql, params = db._pool.conn.queries[0]
    placeholders = {int(n) for n in re.findall(r"\$(\d+)", sql)}
    assert placeholders == set(range(1, len(params) + 1)), (
        f"占位符 {sorted(placeholders)} 应与 {len(params)} 个参数一一对应"
    )
    assert "napcat" in params
    assert re.search(r"session_id = \$\d+ AND platform = \$\d+", sql), "会话条件平台限定"
    assert sql.count("AND platform = $") == 5, (
        "会话条件 + bot_self 豁免支路 + 用户组豁免支路 + 扩展钉死块 + 近时排除子查询"
        "五处平台限定"
    )


async def test_sql_assembly_no_user_filter_unchanged() -> None:
    """Without user filters (wide-recall path) no expansion/exclusion clauses appear; semantics unchanged from the old version."""
    db = MemoryDatabase.__new__(MemoryDatabase)
    db._pool = _FakePool()
    await db.search_chat_summaries(
        query_vec=[0.1], limit=10, scope="session", session_id="s1"
    )
    sql, params = db._pool.conn.queries[0]
    placeholders = {int(n) for n in re.findall(r"\$(\d+)", sql)}
    assert placeholders == set(range(1, len(params) + 1))
    assert not re.search(r"AND summarized AND session_id = \$\d+", sql)
    assert not re.search(r"NOT \(session_id", sql)
    # bot_self exemption narrowed to within-session (the no-platform-qualifier shape)
    assert (
        "((session_id = $2 AND summarized) "
        "OR (kind = 'bot_self' AND session_id = $2))" in sql
    )
    print("PASS test_sql_assembly_no_user_filter_unchanged")


# ----------------------------------------------------------------------
# per_user quota (reuses the main path; quota applies at truncation; capability-flag regression, not yet enabled)
# ----------------------------------------------------------------------


def _prow(doc_id: str, user_id: str, participants: list[str],
          relevance: float = 0.9, kind: str = "chat_summary") -> dict:
    """per_user case row (with the participants column, the ownership source)."""
    return {
        "id": next(_ROW_SEQ),
        "document_id": doc_id,
        "kind": kind,
        "session_id": "s1",
        "user_id": user_id,
        "content": f"内容-{doc_id}",
        "occurred_at": _NOW,
        "relevance": relevance,
        "participants": participants,
    }


async def test_per_user_quota_via_main_pipeline() -> None:
    """per_user reuses the main path: a single search_chat_summaries call
    (with the participants column, candidate pool enlarged by user count);
    quota is enforced at truncation by ownership."""
    rows = [
        _prow("doc-1", "u1", ["napcat:u1"], 0.9),
        _prow("doc-2", "u1", ["napcat:u1"], 0.8),
        _prow("doc-3", "u2", ["napcat:u2"], 0.7),
        _prow("doc-4", "", [], 0.6, kind="bot_self"),
    ]
    db = _CaptureDB(rows=rows)
    kernel = _make_kernel(db)
    items = await kernel.search(
        query="Q", top_k=1, scope="user", platform="napcat",
        user_ids=["u1", "u2"], per_user=True,
    )
    kw = db.search_kwargs
    assert kw["user_keys"] == ["napcat:u1", "napcat:u2"]
    assert kw["with_participants"] is True
    # candidate pool enlargement: max(top_k*4, top_k*4*2) = 8
    assert kw["limit"] == 8
    # quota 1/1 + 1 shared slot: doc-1 -> u1, doc-2 dropped (u1 full),
    # doc-3 -> u2, doc-4 unowned -> shared slot — 3 items total, may exceed
    # top_k
    assert sorted(i.id for i in items) == ["doc-1", "doc-3", "doc-4"]
    print("PASS test_per_user_quota_via_main_pipeline")


def test_per_user_quota_truncation_pure() -> None:
    """Quota truncation pure function: ownership via user_id/participants,
    multi-owned rows go to the first owner with quota left, rows whose
    owners are all full are dropped (no shared slot), unowned rows go to
    shared slots."""
    rows = [
        {"id": 1, "user_id": "u1", "participants": []},
        {"id": 2, "user_id": "u1", "participants": []},
        {"id": 3, "user_id": "", "participants": ["napcat:u2"]},
        {"id": 4, "user_id": "", "participants": ["napcat:u1", "napcat:u2"]},
        {"id": 5, "user_id": "", "participants": []},
        {"id": 6, "user_id": "", "participants": []},
    ]
    kept = LocalMemoryKernel._truncate_per_user_quota(
        rows, uids=["u1", "u2"], platform="napcat", top_k=1
    )
    assert [r["id"] for r in kept] == [1, 3, 5]

    kept2 = LocalMemoryKernel._truncate_per_user_quota(
        rows, uids=["u1", "u2"], platform="napcat", top_k=2
    )
    # quota 2/2 + 2 shared slots: 1/2 -> u1; 3 -> u2; 4 dual-owned (u1 full,
    # u2 has room) -> u2; 5/6 unowned -> shared slots (shared capacity
    # equals top_k)
    assert [r["id"] for r in kept2] == [1, 2, 3, 4, 5, 6]
    print("PASS test_per_user_quota_truncation_pure")


# ----------------------------------------------------------------------
# Topic blacklist (candidate-layer SQL push-down)
# ----------------------------------------------------------------------

async def test_search_passes_blacklist_to_db() -> None:
    """A non-empty topic_blacklist passes the exclusion params to the candidate layer; empty passes None (no condition)."""
    db = _CaptureDB(rows=[_row("a", 0.9, _NOW)])
    kernel = _make_kernel(db, topic_blacklist=["小鱼干"])
    await kernel.search(query="Q", session_id="s1")
    assert db.search_kwargs["exclude_content_keywords"] == ["小鱼干"]

    db2 = _CaptureDB(rows=[_row("a", 0.9, _NOW)])
    await _make_kernel(db2).search(query="Q", session_id="s1")
    assert db2.search_kwargs["exclude_content_keywords"] is None
    print("PASS test_search_passes_blacklist_to_db")


async def test_sql_assembly_blacklist_and_participants() -> None:
    """Blacklist SQL exclusion (content NOT LIKE ALL + wildcard escaping)
    and the with_participants column switch."""
    db = MemoryDatabase.__new__(MemoryDatabase)
    db._pool = _FakePool()
    await db.search_chat_summaries(
        query_vec=[0.1], limit=10, scope="session", session_id="s1",
        exclude_content_keywords=["小鱼干", "100%胜率", "a_b", ""],
    )
    sql, params = db._pool.conn.queries[0]
    placeholders = {int(n) for n in re.findall(r"\$(\d+)", sql)}
    assert placeholders == set(range(1, len(params) + 1))
    assert "content NOT LIKE ALL($3)" in sql
    # empty keywords dropped; % _ escaped to literals; the wrapping % means substring containment
    assert params[2] == ["%小鱼干%", "%100\\%胜率%", "%a\\_b%"]

    db2 = MemoryDatabase.__new__(MemoryDatabase)
    db2._pool = _FakePool()
    await db2.search_chat_summaries(
        query_vec=[0.1], limit=10, scope="session", session_id="s1",
        with_participants=True,
    )
    sql2, _ = db2._pool.conn.queries[0]
    assert ", participants, " in sql2
    # not selected by default (saves transfer); the participants && condition in WHERE does not count as a column
    db3 = MemoryDatabase.__new__(MemoryDatabase)
    db3._pool = _FakePool()
    await db3.search_chat_summaries(
        query_vec=[0.1], limit=10, scope="session", session_id="s1",
    )
    sql3, _ = db3._pool.conn.queries[0]
    assert "participants" not in sql3
    print("PASS test_sql_assembly_blacklist_and_participants")


# ----------------------------------------------------------------------
# Tokenization + search_text write points
# ----------------------------------------------------------------------

def test_build_search_text() -> None:
    assert build_search_text("明日方舟") == "明日 日方 方舟"
    assert build_search_text("玩原神 Game5 吗") == "玩原 原神 game5", (
        "单字 CJK 不产 bigram，ASCII 词小写保留"
    )
    assert build_search_text("") == ""
    assert build_search_text("ABC") == "abc"
    print("PASS test_build_search_text")


class _ExecConn:
    def __init__(self) -> None:
        self.executions: list[tuple[str, tuple]] = []

    async def execute(self, sql, *params):
        self.executions.append((sql, params))

    async def fetch(self, sql, *params):
        return []


class _ExecPool:
    def __init__(self) -> None:
        self.conn = _ExecConn()

    def acquire(self):
        return _Ctx(self.conn)


class _Ctx:
    def __init__(self, conn) -> None:
        self._conn = conn

    async def __aenter__(self):
        return self._conn

    async def __aexit__(self, *exc):
        return False


async def test_insert_writes_search_text() -> None:
    db = MemoryDatabase.__new__(MemoryDatabase)
    db._pool = _ExecPool()
    await db.insert_chat_summary(
        document_id="d1", kind="chat_summary", platform="qq", session_id="s1",
        group_id="", user_id="1", participants=["qq:1"], content="柠檬喜欢明日方舟",
        occurred_at=_NOW, embedding=[0.1],
    )
    sql, params = db._pool.conn.executions[0]
    assert "search_text" in sql, "INSERT 须含 search_text 列"
    assert params[-1] == build_search_text("柠檬喜欢明日方舟"), "末参为分词串"
    print("PASS test_insert_writes_search_text")


async def test_update_encoded_rewrites_search_text() -> None:
    db = MemoryDatabase.__new__(MemoryDatabase)
    db._pool = _ExecPool()
    await db.update_chat_summary_encoded(7, "新摘要内容", [0.2])
    sql, params = db._pool.conn.executions[0]
    assert "search_text = $4" in sql
    assert params[3] == build_search_text("新摘要内容")
    print("PASS test_update_encoded_rewrites_search_text")


# ----------------------------------------------------------------------
# Hybrid retrieval dual-leg + RRF
# ----------------------------------------------------------------------

class _FetchSeqConn:
    """fetch stub returning canned results in call order (vector leg -> BM25 leg)."""

    def __init__(self, results: list[list[dict]]) -> None:
        self._results = list(results)
        self.queries: list[tuple[str, tuple]] = []

    async def fetch(self, sql, *params):
        self.queries.append((sql, params))
        return self._results.pop(0) if self._results else []


def _leg_row(row_id: int, doc_id: str, relevance: float) -> dict:
    return {
        "id": row_id, "document_id": doc_id, "kind": "chat_summary",
        "session_id": "s1", "user_id": "1", "content": f"内容-{doc_id}",
        "occurred_at": _NOW, "relevance": relevance,
    }


async def test_hybrid_two_legs_rrf_fusion() -> None:
    db = MemoryDatabase.__new__(MemoryDatabase)
    pool = _ExecPool()
    pool.conn = _FetchSeqConn([
        [_leg_row(1, "vec-only", 0.9), _leg_row(2, "both", 0.8)],
        [_leg_row(3, "bm25-only", 0.5), _leg_row(2, "both", 0.8)],
    ])
    db._pool = pool
    rows = await db.search_chat_summaries(
        query_vec=[0.1], limit=20, scope="session", session_id="s1",
        query_text="明日方舟", hybrid=True, rrf_k=60,
    )
    assert len(pool.conn.queries) == 2, "须发出向量+BM25 两路查询"
    vec_sql, bm25_sql = pool.conn.queries[0][0], pool.conn.queries[1][0]
    assert "ORDER BY embedding <=> $1::vector" in vec_sql
    assert "to_tsvector('simple', search_text)" in bm25_sql
    assert "to_tsquery('simple', $" in bm25_sql, "OR 连接用 to_tsquery"
    assert "plainto_tsquery" not in bm25_sql, "AND 语义（plainto）须废弃"
    for sql_i, params_i in pool.conn.queries:
        # Invariant: placeholders strictly without gaps — asyncpg encodes by
        # position and PG does not infer types for parameter numbers the SQL
        # never references; a gap means IndeterminateDatatypeError (upstream
        # production incident: the BM25 leg skipped the number taken by the
        # vector leg's LIMIT, and recall came back empty after restart)
        phs = {int(n) for n in re.findall(r"\$(\d+)", sql_i)}
        assert phs, "至少引用一个占位符"
        assert phs == set(range(1, len(params_i) + 1)), (
            f"占位符须恰好连续覆盖 1..{len(params_i)}，实得 {sorted(phs)}"
        )
    or_params = [
        v for v in pool.conn.queries[1][1]
        if isinstance(v, str) and " | " in v
    ]
    assert or_params, "BM25 查询词 OR 连接"
    # "both" hit by both legs ranks highest in RRF; vec-only (vector leg #1) next; bm25-only last
    assert [r["document_id"] for r in rows] == ["both", "vec-only", "bm25-only"], (
        "RRF 融合序：双路命中行应置顶"
    )
    assert all("rrf" in r for r in rows)
    print("PASS test_hybrid_two_legs_rrf_fusion")


async def test_hybrid_disabled_single_leg() -> None:
    db = MemoryDatabase.__new__(MemoryDatabase)
    pool = _ExecPool()
    pool.conn = _FetchSeqConn([[_leg_row(1, "a", 0.9)]])
    db._pool = pool
    rows = await db.search_chat_summaries(
        query_vec=[0.1], limit=20, scope="session", session_id="s1",
        query_text="明日方舟", hybrid=False,
    )
    assert len(pool.conn.queries) == 1, "关闭时单路"
    assert "rrf" not in rows[0], "关闭时行无 rrf 字段（纯向量契约不变）"
    print("PASS test_hybrid_disabled_single_leg")


async def test_exclude_document_ids_clause() -> None:
    db = MemoryDatabase.__new__(MemoryDatabase)
    pool = _ExecPool()
    pool.conn = _FetchSeqConn([[_leg_row(1, "a", 0.9)]])
    db._pool = pool
    await db.search_chat_summaries(
        query_vec=[0.1], limit=20, scope="session", session_id="s1",
        exclude_document_ids=["d1", "d2"],
    )
    sql, params = pool.conn.queries[0]
    assert re.search(r"document_id <> ALL\(\$\d+\)", sql), "行排除子句须下推"
    assert ["d1", "d2"] in list(params), "排除列表作为数组参数传入"
    print("PASS test_exclude_document_ids_clause")


class _FailingRerank:
    async def rerank(self, query, docs, top_k):
        raise RuntimeError("rerank 失败（测试注入）")


async def test_hybrid_fallback_threshold_scale_guard() -> None:
    """rerank failure + RRF scale: skip the threshold (never apply an absolute threshold to relative scores)."""
    rows = [
        dict(_leg_row(1, "a", 0.8), rrf=0.02),
        dict(_leg_row(2, "b", 0.95), rrf=0.03),
    ]
    db = _RolloutDB(rows=rows)
    kernel = _make_kernel(db, recall_relevance_threshold=0.9)
    kernel.rerank_client = _FailingRerank()
    items = await kernel.search(query="Q", session_id="s1")
    assert len(items) == 2, "RRF 刻度应跳过阈值，两行均保留"
    print("PASS test_hybrid_fallback_threshold_scale_guard")


# ----------------------------------------------------------------------
# Rolling restore (batch-count anchored: skip the latest K batches)
# ----------------------------------------------------------------------

def _rollout_row(doc_id: str, content: str, age_hours: float) -> dict:
    return {
        "document_id": doc_id, "content": content, "kind": "chat_summary",
        "occurred_at": _NOW - timedelta(hours=age_hours),
    }


async def test_rollout_text_and_recall_exclusion() -> None:
    db = _RolloutDB(
        rows=[_leg_row(1, "recall-hit", 0.9)],
        rollout_rows=[
            _rollout_row("new-batch", "最新一批摘要", 4),
            _rollout_row("mid-batch", "中间一批摘要", 6),
            _rollout_row("old-batch", "最旧一批摘要", 8),
        ],
    )
    kernel = _make_kernel(db, window_batches=10, timezone="UTC")

    text = await kernel.build_recent_rollout_text("s1")
    assert text.startswith("# 更早对话摘要"), "块标题"
    lines = [l for l in text.splitlines() if l.startswith("- ")]
    assert len(lines) == 3
    assert lines[0] == "- 最旧一批摘要（今天）", "呈现顺序须旧→新，且带相对时间标注"
    assert "最新一批摘要" in lines[2], "呈现顺序须旧→新"

    # memo effective: same-session recall carries row exclusion; db receives skip_batches = window batch count
    assert db.rollout_calls[0]["limit"] == 3
    assert db.rollout_calls[0]["skip_batches"] == 10
    await kernel.search(query="Q", session_id="s1")
    assert db.search_kwargs.get("exclude_document_ids") == [
        "old-batch", "mid-batch", "new-batch",
    ], "recall 须排除已注入滚动补回的行"
    print("PASS test_rollout_text_and_recall_exclusion")


async def test_rollout_platform_qualified() -> None:
    """The rolling-restore OFFSET quota is qualified by this adapter (cross-adapter isolation for bare session_ids)."""
    db = _RolloutDB(rollout_rows=[_rollout_row("a", "内容", 4)])
    kernel = _make_kernel(db, window_batches=10)
    await kernel.build_recent_rollout_text("s1", platform="napcat")
    assert db.rollout_calls[0]["platform"] == "napcat"
    print("PASS test_rollout_platform_qualified")


async def test_rollout_budget_truncation_keeps_newest() -> None:
    db = _RolloutDB(
        rollout_rows=[
            _rollout_row("a", "A" * 900, 4),   # newest (the bridging batch next to the host window)
            _rollout_row("b", "B" * 900, 6),   # older
        ]
    )
    kernel = _make_kernel(
        db, window_batches=10, recent_rollout_max_chars=1000,
        recall_time_label_enabled=False,
    )
    text = await kernel.build_recent_rollout_text("s1")
    body = [l for l in text.splitlines() if l.startswith("- ")]
    assert len(body) == 1, "预算 1000 只容得下一批"
    assert body[0].startswith("- " + "A" * 900), "超预算丢弃更旧批次、保留最新桥接批"
    await kernel.search(query="Q", session_id="s1")
    assert db.search_kwargs.get("exclude_document_ids") == ["a"], "只排除实际注入的行"
    print("PASS test_rollout_budget_truncation_keeps_newest")


async def test_rollout_memo_ttl_expiry() -> None:
    db = _RolloutDB(rollout_rows=[_rollout_row("a", "内容", 4)])
    kernel = _make_kernel(db, window_batches=10)
    await kernel.build_recent_rollout_text("s1")
    # wind the memo timestamp back past the TTL manually
    ids, _ = kernel._rollout_memo["s1"]
    kernel._rollout_memo["s1"] = (ids, time.monotonic() - 10000)
    await kernel.search(query="Q", session_id="s1")
    assert db.search_kwargs.get("exclude_document_ids") is None, "过期后不再排除"
    print("PASS test_rollout_memo_ttl_expiry")


async def test_rollout_disabled_or_no_window() -> None:
    db = _RolloutDB(rollout_rows=[_rollout_row("a", "内容", 4)])
    kernel = _make_kernel(db, recent_rollout_enabled=False, window_batches=10)
    assert await kernel.build_recent_rollout_text("s1") == ""
    assert db.rollout_calls == []

    db2 = _RolloutDB(rollout_rows=[_rollout_row("a", "内容", 4)])
    kernel2 = _make_kernel(db2)  # no provider -> window unavailable
    assert await kernel2.build_recent_rollout_text("s1") == ""
    assert db2.rollout_calls == []

    db3 = _RolloutDB(rollout_rows=[_rollout_row("a", "内容", 4)])
    kernel3 = _make_kernel(db3, recall_exclude_history_window=False, window_batches=10)
    assert await kernel3.build_recent_rollout_text("s1") == "", (
        "近时排除开关关闭时窗口锚定不可得（与 recall 同一开关源）"
    )
    assert db3.rollout_calls == []
    print("PASS test_rollout_disabled_or_no_window")


# ----------------------------------------------------------------------
# search_text backfill pass
# ----------------------------------------------------------------------

class _BackfillDB:
    def __init__(self) -> None:
        self.search_updates: list[tuple[int, str]] = []
        self.embedding_scans: list[str] = []

    async def fetch_missing_search_text(self, limit):
        self.limit = limit
        return [(1, "柠檬喜欢明日方舟"), (2, "讨论 GBC")]

    async def update_search_text(self, row_id, text):
        self.search_updates.append((row_id, text))

    async def fetch_missing_embeddings(self, table, limit):
        self.embedding_scans.append(table)
        return []

    async def update_embedding(self, table, row_id, vec):
        pass


class _UnavailableService:
    available = False


async def test_backfill_search_text_without_embedding_service() -> None:
    db = _BackfillDB()
    task = EmbeddingBackfillTask(
        db=db,  # type: ignore[arg-type]
        service=_UnavailableService(),  # type: ignore[arg-type]
        config=LocalMemoryConfig(dsn="postgresql://stub", backfill_batch_size=16),
    )
    filled = await task.run_once()
    assert filled == 2, "search_text 回填不依赖 embedding 服务"
    assert db.search_updates == [
        (1, build_search_text("柠檬喜欢明日方舟")),
        (2, build_search_text("讨论 GBC")),
    ]
    assert db.embedding_scans == [], "embedding 服务不可用时跳过向量遍"
    print("PASS test_backfill_search_text_without_embedding_service")


async def test_with_embedding_false_omits_column() -> None:
    db = MemoryDatabase.__new__(MemoryDatabase)
    pool = _ExecPool()
    pool.conn = _FetchSeqConn([[_leg_row(1, "a", 0.9)]])
    db._pool = pool
    await db.search_chat_summaries(
        query_vec=[0.1], limit=20, scope="session", session_id="s1",
        with_embedding=False,
    )
    sql = pool.conn.queries[0][0]
    assert ", embedding," not in sql and "embedding," not in sql.split(" AS relevance")[0].split("occurred_at")[-1], (
        "with_embedding=False 时 SELECT 不携带 embedding 列"
    )
    assert "<=> $1::vector" in sql, "排序仍用向量（WHERE/ORDER BY 不受影响）"
    print("PASS test_with_embedding_false_omits_column")


# ----------------------------------------------------------------------
# Entity hits (window dictionary + kernel directory entry)
# ----------------------------------------------------------------------

async def test_directory_build_filters_and_match() -> None:
    async def source(session_id: str):
        return [
            ("阿", "qq", "1"),            # <2 chars, skipped
            ("小王", "qq", ""),            # empty uid, skipped
            ("小王", "qq", "100"),
            ("小王", "qq", "100"),         # duplicate, deduped
            ("小王", "web", "200"),        # same name, multiple keys
            ("阿伟", "qq", "300"),
        ]

    d = _EntityDirectory(source)
    hits = await d.match("s1", "今天小王来找我了")
    assert hits == [("小王", "qq", "100"), ("小王", "web", "200")]
    assert await d.match("s1", "随便聊点什么") == []

    async def big_source(session_id: str):
        return [(f"名字{i}", "qq", str(i)) for i in range(6)]

    d2 = _EntityDirectory(big_source)
    text = " ".join(f"名字{i}" for i in range(6))
    assert len(await d2.match("s1", text)) == _EntityDirectory._MAX_HINT_ENTRIES


async def test_directory_ttl_rebuild_and_failure() -> None:
    calls: list[str] = []

    async def source(session_id: str):
        calls.append(session_id)
        return [("小王", "qq", "100")]

    d = _EntityDirectory(source)
    assert await d.match("s1", "提到小王") == [("小王", "qq", "100")]
    await d.match("s1", "又提到小王")
    assert len(calls) == 1
    ts, names = d._cache["s1"]
    d._cache["s1"] = (ts - _EntityDirectory._TTL_SECONDS - 1, names)
    await d.match("s1", "再提到小王")
    assert len(calls) == 2

    async def bad_source(session_id: str):
        raise RuntimeError("名字源不可用（测试注入）")

    assert await _EntityDirectory(bad_source).match("s1", "小王") == []
    assert await _EntityDirectory(None).match("s1", "小王") == []
    assert await _EntityDirectory(source).match("s1", "") == []


async def test_entity_hint_entries_and_keys_projection() -> None:
    async def source(session_id: str):
        return [("小王", "qq", "100"), ("无平台", "", "400")]

    kernel = _make_kernel(FakeHintDB())
    kernel._entity_directory = _EntityDirectory(source)

    entries = await kernel.entity_hint_entries("s1", "小王和无平台都在")
    assert entries == [("小王", "qq", "100"), ("无平台", "", "400")]
    # compound-key projection ("p:u" / bare uid without platform) is done
    # inline by the caller (main), see
    # test_noriflow_memory.test_53k_entity_keys_passed_to_search

    kernel_off = _make_kernel(FakeHintDB(), recall_hint_enabled=False)
    assert await kernel_off.entity_hint_entries("s1", "小王") == []


def test_recall_hints_fields_contract() -> None:
    """RecallHints is the main<->kernel transfer contract: a drifting field
    set silently breaks the chain. hints only carries entity hits / match
    text / bot signals — recall candidate fetching has no bypass (removed
    in v1.7.1; the negative-existence asserts are no longer kept)."""
    import dataclasses
    field_names = {f.name for f in dataclasses.fields(RecallHints)}
    assert field_names == {
        "entity_user_keys", "match_text", "bot_addressed", "bot_user_id",
    }


async def test_search_runs_main_path_only() -> None:
    """Recall runs the main path only: a single search_chat_summaries call, no bypass DB access."""
    db = FakeHintDB(rows=[_hint_row(1, "主路摘要行")])
    kernel = _make_kernel(db)
    items = await kernel.search(query="Q", top_k=5, session_id="s1")
    assert [i.id for i in items] == ["doc-1"]
    assert len(db.calls) == 1  # main path once, no bypass calls


# ----------------------------------------------------------------------
# main injection layer (bypasses plugin __init__; sets instance state only)
# ----------------------------------------------------------------------


def _bare_plugin():
    plugin = object.__new__(_main.NoriflowMemoryPlugin)
    from collections import OrderedDict
    plugin._history = OrderedDict()
    plugin._bot_user_id = "bot-1"
    plugin._config = LocalMemoryConfig()
    return plugin


def _add_line(plugin, sid, key, **kw):
    line = _main._CachedLine(
        line=kw.get("line", "x"),
        uid=kw.get("uid", ""),
        platform=kw.get("platform", "qq"),
        is_bot=kw.get("is_bot", False),
        nickname=kw.get("nickname", ""),
        cardname=kw.get("cardname", ""),
        timestamp=kw.get("timestamp"),
    )
    from collections import OrderedDict
    bucket = plugin._history.setdefault(sid, OrderedDict())
    bucket[key] = line


class _Msg:
    def __init__(self, elements, is_notice=False):
        self.chain = elements
        self.is_notice = is_notice


_ELEMENTS = sys.modules["core.chat.message_elements"]


def test_entity_match_text_includes_at_nicknames() -> None:
    msg = _Msg([
        _ELEMENTS.At(pid="100", nickname="小王"),
        _ELEMENTS.Text(text=" 你上次说的"),
        _ELEMENTS.Reply(message_id="99988777"),
    ])
    text = _main.NoriflowMemoryPlugin._entity_match_text([_Msg([_ELEMENTS.Text(text="纯文本")]), msg])
    assert "小王" in text
    assert "你上次说的" in text
    assert "99988777" not in text, "Reply 元素只含 ID，不进匹配面"


def test_reply_quote_text_cache_lookup() -> None:
    plugin = _bare_plugin()
    _add_line(plugin, "sid-1", "ref-old", uid="100", line="[09-01 10:00][小王]我们聊过的那家店")
    _add_line(plugin, "sid-1", "ref-new", uid="200", line="[09-08 10:00][阿伟]他上次说的话")

    msg = _Msg([_ELEMENTS.Reply(message_id="ref-old"), _ELEMENTS.Reply(message_id="ref-new")])
    quote = plugin._reply_quote_text([msg], "sid-1")
    assert "我们聊过的那家店" in quote and "他上次说的话" in quote

    # miss: the quoted target is not in the cache (or no session cache) -> empty (not merged into the query)
    assert plugin._reply_quote_text([_Msg([_ELEMENTS.Reply(message_id="gone")])], "sid-1") == ""
    assert plugin._reply_quote_text([msg], "sid-other") == ""
    # no Reply elements -> empty
    assert plugin._reply_quote_text([_Msg([_ELEMENTS.Text(text="hi")])], "sid-1") == ""

    # at most 2 per turn: with 3 quoted targets only the first 2 cache hits are merged
    _add_line(plugin, "sid-2", "r1", uid="1", line="文本一")
    _add_line(plugin, "sid-2", "r2", uid="2", line="文本二")
    _add_line(plugin, "sid-2", "r3", uid="3", line="文本三")
    msg3 = _Msg([_ELEMENTS.Reply(message_id="r1"), _ELEMENTS.Reply(message_id="r2"),
                 _ELEMENTS.Reply(message_id="r3")])
    quote3 = plugin._reply_quote_text([msg3], "sid-2")
    assert quote3.count("文本") == 2


async def test_directory_names_skips_bot() -> None:
    plugin = _bare_plugin()
    _add_line(plugin, "sid-1", "m1", uid="100", nickname="小王", cardname="群名片王")
    _add_line(plugin, "sid-1", "m2", uid="", is_bot=True, nickname="Kira")
    _add_line(plugin, "sid-1", "m3", uid="300", nickname="", cardname="")
    pairs = await plugin._directory_names("sid-1")
    assert pairs == [("小王", "qq", "100"), ("群名片王", "qq", "100")]
    assert await plugin._directory_names("nope") == []


def test_persona_candidates_entity_level() -> None:
    plugin = _bare_plugin()

    class _Sender:
        def __init__(self, uid, nickname=""):
            self.user_id = uid
            self.nickname = nickname

    class _PMsg:
        def __init__(self, uid, nickname=""):
            self.sender = _Sender(uid, nickname)

    # senders first; dictionary hits backfill; already-collected uids deduped; bot excluded
    candidates = plugin._persona_candidates(
        [_PMsg("123", "发起人"), _PMsg("123", "发起人")],
        "qq",
        entity_entries=[("发起人", "qq", "123"), ("小王", "qq", "100"), ("bot名", "qq", "bot-1")],
    )
    ids = [c.user_id for c in candidates]
    assert ids == ["123", "100"], "发起人先收（词典同名去重），小王补位，bot 排除"
    assert candidates[1].display_name == "小王"
    assert candidates[1].nickname == "小王"

    # cap truncation: max_persona_profiles defaults to 3 -> sender + 2 dictionary hits
    plugin._config = LocalMemoryConfig(max_persona_profiles=2)
    candidates2 = plugin._persona_candidates(
        [_PMsg("123", "发起人")],
        "qq",
        entity_entries=[("小王", "qq", "100"), ("阿伟", "qq", "200")],
    )
    assert [c.user_id for c in candidates2] == ["123", "100"]


# ---------------------------------------------------------------------------
#  Runner
# ---------------------------------------------------------------------------


async def main() -> None:
    await test_dedup_drops_near_duplicate()
    await test_dedup_keeps_dissimilar()
    await test_dedup_threshold_boundary()
    await test_dedup_disabled_and_missing_vector()
    await test_dedup_early_stop_at_top_k()
    await test_window_batches_passed_to_search()
    await test_window_disabled_or_no_provider()
    test_zero_window_warns_once()
    await test_platform_passed_through_search()
    await test_expansion_derives_recent_participants()
    await test_expansion_disabled_or_wrong_scope()
    await test_time_labels_appended()
    await test_time_label_falls_back_to_host_tz()
    await test_time_label_disabled()
    await test_recall_log_writes_search_and_inject()
    test_parse_vector()
    await test_sql_assembly_expansion_and_window()
    await test_sql_assembly_platform_qualified()
    await test_sql_assembly_no_user_filter_unchanged()
    await test_per_user_quota_via_main_pipeline()
    test_per_user_quota_truncation_pure()
    await test_search_passes_blacklist_to_db()
    await test_sql_assembly_blacklist_and_participants()
    test_build_search_text()
    await test_insert_writes_search_text()
    await test_update_encoded_rewrites_search_text()
    await test_hybrid_two_legs_rrf_fusion()
    await test_hybrid_disabled_single_leg()
    await test_exclude_document_ids_clause()
    await test_hybrid_fallback_threshold_scale_guard()
    await test_rollout_text_and_recall_exclusion()
    await test_rollout_platform_qualified()
    await test_rollout_budget_truncation_keeps_newest()
    await test_rollout_memo_ttl_expiry()
    await test_rollout_disabled_or_no_window()
    await test_backfill_search_text_without_embedding_service()
    await test_with_embedding_false_omits_column()
    await test_directory_build_filters_and_match()
    await test_directory_ttl_rebuild_and_failure()
    await test_entity_hint_entries_and_keys_projection()
    test_recall_hints_fields_contract()
    await test_search_runs_main_path_only()
    test_entity_match_text_includes_at_nicknames()
    test_reply_quote_text_cache_lookup()
    await test_directory_names_skips_bot()
    test_persona_candidates_entity_level()
    print("ALL PASS")


if __name__ == "__main__":
    sys.exit(asyncio.run(main()))
