"""Recall lazy embedding fetch (perf P2) and the BM25 materialization
migration guard.

- Lazy path: when the DB exposes fetch_embeddings_by_ids, the retrieval
  SELECT carries no embedding (with_embedding=False) and the dedup head rows
  get their vectors attached by a precise id fetch;
- Compatibility path: a DB without that method keeps the old behavior
  (with_embedding follows the dedup switch);
- Migration 015 guard: tsv generated column + GIN(tsv) + expression index
  retirement.
"""

from __future__ import annotations

import sys
from datetime import datetime, timedelta, timezone
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))

from _harness import load_module  # noqa: E402

_circuit_breaker = load_module("circuit_breaker")
_config = load_module("config")
_kernel = load_module("memory_kernel")

MemoryDBCircuitBreaker = _circuit_breaker.MemoryDBCircuitBreaker
LocalMemoryConfig = _config.LocalMemoryConfig
LocalMemoryKernel = _kernel.LocalMemoryKernel

_NOW = datetime(2026, 11, 2, 12, 0, 0, tzinfo=timezone.utc)
_ROW_SEQ = iter(range(1, 10000))


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


def _vec_str(vec: list[float]) -> str:
    return "[" + ",".join(repr(v) for v in vec) + "]"


class _LazyCaptureDB:
    """Stub DB supporting lazy fetch: search rows carry no vector, the
    vectors come from fetch_embeddings_by_ids."""

    def __init__(self, rows, embed_by_id=None, fail_fetch: bool = False) -> None:
        self.rows = rows
        self.embed_by_id = embed_by_id or {}
        self.fail_fetch = fail_fetch
        self.search_kwargs: dict | None = None
        self.fetch_calls: list[list[int]] = []

    async def search_chat_summaries(self, **kwargs):
        self.search_kwargs = kwargs
        # Mirror the real SELECT shape: with_embedding=False carries no
        # vector column
        if kwargs.get("with_embedding"):
            return [dict(r) for r in self.rows]
        stubbed = [
            {k: v for k, v in r.items() if k != "embedding"} for r in self.rows
        ]
        return [dict(r) for r in stubbed]

    async def fetch_recent_session_participants(self, **kwargs):
        return []

    async def fetch_embeddings_by_ids(self, *, ids):
        self.fetch_calls.append(list(ids))
        if self.fail_fetch:
            raise RuntimeError("stub fetch failure")
        return dict(self.embed_by_id)


class _FakeBreaker(MemoryDBCircuitBreaker):
    async def is_available(self) -> bool:  # noqa: D102
        return True

    async def record_failure(self) -> None:  # noqa: D102
        pass

    async def record_success(self) -> None:  # noqa: D102
        pass


class _StubEmbedding:
    async def embed_one(self, text):
        return [0.1]

    async def embed_batch(self, texts):
        return [[0.1] for _ in texts]


def _make_kernel(db, **cfg_over) -> LocalMemoryKernel:
    # Keep the legacy relative time-label shape; mode-specific coverage lives
    # in test_time_label.py
    cfg_over.setdefault("recall_time_label_mode", "relative")
    config = LocalMemoryConfig(dsn="postgresql://stub", **cfg_over)
    kernel = LocalMemoryKernel(
        db=db,
        embedding_service=_StubEmbedding(),
        circuit_breaker=_FakeBreaker(failure_threshold=5, recovery_seconds=1.0),
        config=config,
        bot_id="bot001",
    )
    kernel._now = staticmethod(lambda: _NOW)
    return kernel


async def test_lazy_path_requests_no_embedding_and_dedup_works() -> None:
    """Lazy path: with_embedding=False; head-row vectors fetched by id make
    dedup work."""
    a = _row("a", 0.9, _NOW - timedelta(days=10))
    b = _row("b", 0.8, _NOW - timedelta(days=11))
    db = _LazyCaptureDB(
        rows=[a, b],
        # Same direction: cosine=1.0, the second row must be dropped
        embed_by_id={
            a["id"]: _vec_str([1.0, 0.0, 0.0]),
            b["id"]: _vec_str([1.0, 0.0, 0.0]),
        },
    )
    kernel = _make_kernel(db)
    items = await kernel.search(query="猫", session_id="s1")
    assert db.search_kwargs is not None
    assert db.search_kwargs["with_embedding"] is False, (
        "the retrieval SELECT must not carry embedding on the lazy path"
    )
    assert len(db.fetch_calls) == 1, "head-row vectors must be fetched exactly once"
    assert set(db.fetch_calls[0]) == {a["id"], b["id"]}
    assert [i.id for i in items] == ["a"], (
        "near-duplicate with same-direction vector (cosine=1.0) must be dropped"
    )
    print("PASS test_lazy_path_requests_no_embedding_and_dedup_works")


async def test_lazy_path_fetch_failure_fail_open() -> None:
    """Fetch failure fails open: dedup degrades to missing-vector semantics
    and recall is not blocked."""
    a = _row("a", 0.9, _NOW - timedelta(days=10))
    b = _row("b", 0.8, _NOW - timedelta(days=11))
    db = _LazyCaptureDB(rows=[a, b], fail_fetch=True)
    kernel = _make_kernel(db)
    items = await kernel.search(query="猫", session_id="s1")
    assert [i.id for i in items] == ["a", "b"], (
        "on fetch failure, missing-vector semantics apply: not compared and "
        "not dropped"
    )
    print("PASS test_lazy_path_fetch_failure_fail_open")


async def test_lazy_path_missing_vector_keeps_row() -> None:
    """A row missing from the fetched dict stays visible (never dropped)."""
    a = _row("a", 0.9, _NOW - timedelta(days=10))
    b = _row("b", 0.8, _NOW - timedelta(days=11))
    db = _LazyCaptureDB(
        rows=[a, b],
        # Only a's vector is supplied: b is missing but stays visible
        embed_by_id={a["id"]: _vec_str([1.0, 0.0, 0.0])},
    )
    kernel = _make_kernel(db)
    items = await kernel.search(query="猫", session_id="s1")
    assert [i.id for i in items] == ["a", "b"]
    print("PASS test_lazy_path_missing_vector_keeps_row")


async def test_legacy_db_keeps_old_behavior() -> None:
    """Legacy DB (no fetch_embeddings_by_ids): old behavior, rows ship their
    own vectors."""

    class _LegacyDB(_LazyCaptureDB):
        fetch_embeddings_by_ids = None  # type: ignore[assignment]

    a = _row("a", 0.9, _NOW - timedelta(days=10), _vec_str([1.0, 0.0, 0.0]))
    b = _row("b", 0.8, _NOW - timedelta(days=11), _vec_str([1.0, 0.0, 0.0]))
    db = _LegacyDB(rows=[a, b])
    kernel = _make_kernel(db)
    items = await kernel.search(query="猫", session_id="s1")
    assert db.search_kwargs is not None
    assert db.search_kwargs["with_embedding"] is True, (
        "a DB without lazy fetch keeps the old behavior (retrieval carries "
        "vectors)"
    )
    assert [i.id for i in items] == ["a"], (
        "dedup semantics are unchanged on the self-carried-vector path"
    )
    print("PASS test_legacy_db_keeps_old_behavior")


def test_migration_015_materializes_tsv() -> None:
    """Migration 015 guard: tsv generated column + GIN(tsv), expression index
    retired."""
    sql = (
        Path(__file__).resolve().parents[1]
        / "migrations"
        / "015_bm25_materialize.sql"
    ).read_text(encoding="utf-8")
    assert "GENERATED ALWAYS AS (to_tsvector('simple', search_text)) STORED" in sql
    assert "USING GIN (tsv)" in sql
    assert "DROP INDEX IF EXISTS idx_mcs_fts" in sql
    print("PASS test_migration_015_materializes_tsv")
