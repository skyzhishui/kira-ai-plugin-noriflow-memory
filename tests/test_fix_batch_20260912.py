"""2026-09-12 review fix batch regression tests (works both under pytest and python direct-run).

Coverage (keyed to the 2026-09-12 review numbering):
- Bug1 memory_search tool-path entity directory: composite sid passed to the directory + _directory_names
  bare session_id tail lookup fallback (previously the bare id always missed, breaking the window-dictionary path);
- Bug2 _edge_time_qualifier treats naive timestamps per the configured timezone (previously used the server's
  local timezone, drifting boundary dates when the timezone config differs from the server);
- Bug3 relation-backfill revived-cluster todo: precise fetch / batch failure re-registers (previously cleared-then-used,
  clusters below the watermark lost retry chances in incremental mode);
- Bug4 merge normalization pass short-circuits before candidate retrieval when the budget is exhausted (previously
  every fact still ran the HNSW candidate query);
- Bug5 reverse-echo pre-check and write share one connection and one transaction (previously two independent
  acquires, so concurrent retain could interleave mirror edges within the window).

Run (plugin dir):
    python tests/test_fix_batch_20260912.py
"""

from __future__ import annotations

import asyncio
import json
import sys
import unittest
from datetime import datetime, timezone
from pathlib import Path
from types import SimpleNamespace

TESTS_DIR = Path(__file__).resolve().parent
sys.path.insert(0, str(TESTS_DIR))

import test_noriflow_memory as tnm  # noqa: E402  reuses plugin-level stubs

mod = tnm.mod

from plugin_env import load_modules  # noqa: E402

_circuit_breaker, _config, _db, _kernel, _merge, _backfill = load_modules(
    "circuit_breaker", "config", "db", "memory_kernel", "merge_agent",
    "relation_backfill",
)

MemoryDatabase = _db.MemoryDatabase
LocalMemoryConfig = _config.LocalMemoryConfig
MemoryDBCircuitBreaker = _circuit_breaker.MemoryDBCircuitBreaker
LocalMemoryKernel = _kernel.LocalMemoryKernel
FactMergeAgent = _merge.FactMergeAgent
RelationBackfill = _backfill.RelationBackfill


def _try_zoneinfo(name: str):
    """Environments missing tzdata (some minimal containers) skip timezone cases instead of false-failing."""
    from zoneinfo import ZoneInfo

    try:
        return ZoneInfo(name)
    except Exception:  # pragma: no cover - environment-dependent
        return None


# ---------------------------------------------------------------------------
#  Bug1 memory_search entity dictionary keys
# ---------------------------------------------------------------------------


class TestBug1ToolEntityDirectory(unittest.TestCase):
    def test_memory_search_uses_composite_sid_for_directory(self):
        inst, _ = tnm._ready_plugin(mod, cfg={
            "dsn": "postgres://u:p@h/db",
            "allowed_users": ["u1"],
            "tool_scope_locked": True,
        })
        # History rows cache keyed by composite sid (same as observe_message)
        inst._append_history(
            "napcat:gm:10086", "m1", '<msg uid="u9" name="小白">早</msg>',
            uid="u9", platform="napcat", nickname="小白",
        )
        search_calls: dict = {}

        async def fake_search(**kwargs):
            search_calls.update(kwargs)
            return []

        inst._memory_kernel.search = fake_search
        session = tnm.FakeSession(sid="10086", stype="gm", adapter="napcat")
        event = SimpleNamespace(
            session=session, messages=[tnm.make_msg("u1", "小白在吗")],
        )
        result = asyncio.run(inst.memory_search(event, query="小白在吗"))
        self.assertEqual(result, "没有找到相关记忆")
        self.assertEqual(
            search_calls.get("entity_user_keys"), ["napcat:u9"],
            "工具路径实体词典须以复合 sid 命中窗口词典（裸 id 恒 miss 的回归）",
        )

    def test_directory_names_bare_id_tail_lookup(self):
        inst, _ = tnm._ready_plugin(mod, cfg={"dsn": "postgres://u:p@h/db"})
        inst._append_history(
            "napcat:gm:10086", "m1", '<msg uid="u9" name="小白">早</msg>',
            uid="u9", platform="napcat", nickname="小白",
        )
        pairs = asyncio.run(inst._directory_names("10086"))
        self.assertIn(("小白", "napcat", "u9"), pairs, "裸 session_id 尾段反查兜底")


# ---------------------------------------------------------------------------
#  Bug2 edge time qualifier naive-timestamp timezone convention
# ---------------------------------------------------------------------------


class TestBug2EdgeTimeQualifier(unittest.TestCase):
    def _kernel(self, tz: str):
        return LocalMemoryKernel(
            db=None,
            embedding_service=None,
            circuit_breaker=MemoryDBCircuitBreaker(),
            config=LocalMemoryConfig(timezone=tz),
            bot_id="kira",
        )

    def test_naive_timestamp_treated_as_configured_local(self):
        if _try_zoneinfo("Asia/Tokyo") is None:
            self.skipTest("tzdata 不可用")
        kernel = self._kernel("Asia/Tokyo")
        # naive treated as Tokyo wall clock: Sep 1 must not drift to Sep 2
        self.assertEqual(
            kernel._edge_time_qualifier(datetime(2026, 9, 1, 23, 30)),
            "截至9月1日",
        )

    def test_aware_timestamp_converts_to_configured_tz(self):
        if _try_zoneinfo("Asia/Tokyo") is None:
            self.skipTest("tzdata 不可用")
        kernel = self._kernel("Asia/Tokyo")
        # UTC Sep 1 15:00 = Tokyo Sep 2 00:00
        self.assertEqual(
            kernel._edge_time_qualifier(
                datetime(2026, 9, 1, 15, 0, tzinfo=timezone.utc)
            ),
            "截至9月2日",
        )


# ---------------------------------------------------------------------------
#  Bug3 revived-cluster todo re-registers on failure
# ---------------------------------------------------------------------------


class _RequeueDB:
    def __init__(self):
        self.kv = {
            "relation_backfill_watermark": "100",
            "relation_backfill_pending_ids": "[7]",
        }

    async def get_kv(self, key):
        return self.kv.get(key)

    async def set_kv(self, key, value):
        self.kv[key] = value

    async def fetch_confirmed_relation_sources_by_ids(self, ids):
        return [{
            "id": 7, "platform": "qq", "user_id": "100",
            "canonical_statement": "小李是小张的姐姐",
            "occurred_at": datetime(2026, 9, 1, tzinfo=timezone.utc),
        }]

    async def count_confirmed_relation_sources(self, after_id, exclude_user_id=""):
        return 0

    async def fetch_alias_directory(self):
        # matches db.fetch_alias_directory return order: (platform, uid, name)
        return [("qq", "200", "小李")]

    async def fetch_confirmed_relation_sources(self, after_id, limit=None):
        return []


class TestBug3BackfillPendingRequeue(unittest.TestCase):
    def test_failed_revived_batch_requeues_pending(self):
        async def _boom(system_prompt, user_prompt):
            raise RuntimeError("LLM 不可用")

        db = _RequeueDB()
        backfill = RelationBackfill(
            llm_call=_boom, db=db, config=LocalMemoryConfig(dsn="x"),
        )
        summary = asyncio.run(backfill.run())
        self.assertEqual(summary["batches_failed"], 1)
        # The todo was already cleared by take; the failed batch must be
        # re-registered back into kv (below watermark 100 -- without
        # re-registration, incremental mode would never retry extraction)
        self.assertEqual(json.loads(db.kv["relation_backfill_pending_ids"]), [7])

    def test_successful_revived_batch_not_requeued(self):
        async def _ok(system_prompt, user_prompt):
            return '{"relations": []}'

        db = _RequeueDB()
        backfill = RelationBackfill(
            llm_call=_ok, db=db, config=LocalMemoryConfig(dsn="x"),
        )
        asyncio.run(backfill.run())
        self.assertEqual(json.loads(db.kv["relation_backfill_pending_ids"]), [])


# ---------------------------------------------------------------------------
#  Bug4 normalization pass budget short-circuit
# ---------------------------------------------------------------------------


class _BudgetDB:
    def __init__(self):
        self.search_calls = 0

    async def get_kv(self, key):
        return None

    async def set_kv(self, key, value):
        pass

    async def fetch_pending_facts(self, limit):
        return [{
            "id": 1, "document_id": "d", "platform": "qq", "user_id": "u1",
            "related_user_ids": [], "display_name": "", "category": "stable",
            "statement": "我爱猫", "confidence": "high", "session_id": "s1",
            "group_id": "", "evidence_key": "s1|2026-09-12",
            "occurred_at": datetime(2026, 9, 12, tzinfo=timezone.utc),
            "embedding": None,
        }]

    async def search_cluster_candidates(self, **kwargs):
        self.search_calls += 1
        return []

    async def promote_pass(self, **kwargs):
        return 0

    async def relation_integrity_report(self, **kwargs):
        return {}


class TestBug4BudgetShortCircuit(unittest.TestCase):
    def test_exhausted_budget_skips_candidate_search(self):
        db = _BudgetDB()
        agent = FactMergeAgent(
            db=db, llm=SimpleNamespace(),
            config=LocalMemoryConfig(llm_budget_per_cycle=0),
            encoder=None,
        )
        stats = asyncio.run(agent.run_cycle())
        self.assertEqual(stats["deferred"], 1)
        self.assertEqual(
            db.search_calls, 0,
            "预算耗尽须在候选检索前短路（回归：HNSW 查询空转）",
        )
        self.assertEqual(stats["facts"], 0)


# ---------------------------------------------------------------------------
#  Bug5 reverse-echo pre-check and write share one transaction
# ---------------------------------------------------------------------------


class _CountingEdgePool:
    """Single-connection pool stand-in: counts acquire calls (pre-check + write must share one acquire)."""

    def __init__(self, fetch_rows=None):
        self.acquire_count = 0
        self.batches: list = []
        self._fetch_rows = fetch_rows or []

    def acquire(self):
        self.acquire_count += 1
        return self

    async def __aenter__(self):
        return self

    async def __aexit__(self, *args):
        return False

    def transaction(self):
        return self

    async def fetch(self, sql, *args):
        return self._fetch_rows

    async def executemany(self, sql, args):
        self.batches.append((sql, list(args)))


def _edge_row(**overrides):
    row = {
        "platform": "qq", "subject_uid": "u1", "object_uid": "u2",
        "subject_name": "小张", "object_name": "小李",
        "relation_label": "姐姐", "statement": "小张的姐姐是小李",
        "confidence": "high", "occurred_at": datetime(2026, 9, 9, 12, 0),
        "evidence_key": "s1|2026-09-09", "is_bot_edge": False,
        "min_evidence": 2,
    }
    row.update(overrides)
    return row


class TestBug5EchoCheckSameConnection(unittest.TestCase):
    def test_check_and_upsert_share_single_acquire(self):
        pool = _CountingEdgePool()
        db = MemoryDatabase(LocalMemoryConfig(dsn="postgresql://x"))
        db._pool = pool  # noqa: SL001
        asyncio.run(db.upsert_entity_edge([_edge_row()]))
        self.assertEqual(
            pool.acquire_count, 1,
            "镜像探测与写入须同连接（两次 acquire 存在并发互插镜像边窗口）",
        )
        self.assertEqual(len(pool.batches), 1)

    def test_mirror_skipped_still_single_acquire(self):
        mirror = {"platform": "qq", "subject_uid": "u2", "object_uid": "u1",
                  "relation_label": "姐姐"}
        pool = _CountingEdgePool(fetch_rows=[mirror])
        db = MemoryDatabase(LocalMemoryConfig(dsn="postgresql://x"))
        db._pool = pool  # noqa: SL001
        asyncio.run(db.upsert_entity_edge([_edge_row()]))
        self.assertEqual(pool.acquire_count, 1)
        self.assertEqual(pool.batches, [], "镜像方向已在库 -> 整行跳过")


if __name__ == "__main__":
    unittest.main(verbosity=2)
