"""Dedicated tests for retain batching (private-chat N-turn merged encoding).

Covers semantics aligned with nori repo PR #25 (kira architecture
adaptation):

- Batch gating: group chat interval=1 keeps the per-turn path unchanged
  (no pending-table writes, watermark rolled back on failure); private
  chat interval=N merges due batches into a single encode
- Batch content correctness: the merged batch = concatenation of each
  round's batch rows; the history window takes the pre-batch snapshot
- Dual persistence: a snapshot row UPSERTed each turn, deleted after a
  successful encode
- Failure reseating: a failed encode loses no batch (both memory and
  the table row are kept) and retries at the next trigger point
- Watermark semantics: the batching path does not roll back the
  watermark (source of truth already handed to the batch cache/rows)
- Restart recovery: round counts refill from the table, and a refill
  does not encode immediately
- Idle sweep: over-age batches are force-encoded and persisted
- Session eviction coupling: evicting an LRU session holding a batch
  schedules its encode, nothing stranded
- Shutdown closeout: terminate force-encodes unfinished batches

Run: python -m pytest -q tests/test_memory_retain_batch.py
"""

from __future__ import annotations

import asyncio
import json
import sys
import time
import types
import unittest
from datetime import datetime, timedelta, timezone
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))

from test_noriflow_memory import (  # noqa: E402
    FakeCtx,
    FakeDB,
    FakeSession,
    _ready_plugin,
    make_msg,
    mod,
)

DSN = "postgres://u:p@127.0.0.1:5432/db"


def _cfg(private: int = 5, group: int = 1, flush_idle_s: float = 1800.0) -> dict:
    return {
        "dsn": DSN,
        "retain_interval_private": private,
        "retain_interval_group": group,
        "flush_idle_s": flush_idle_s,
    }


async def _turn(inst, ctx, session, text: str, mid: str) -> None:
    """One turn: observe a message, then fire the turn-complete signal."""
    await inst.observe_message(
        types.SimpleNamespace(message=make_msg("u1", text, mid=mid), session=session)
    )
    await ctx.event_bus.fire("session_memory_updated", {"session": session.sid})


def _spy_encode(inst, sink: list) -> None:
    """Capture the encoding input (conversation_text) at the kernel
    boundary."""
    kernel = inst._memory_kernel
    original = kernel.retain_encoded

    async def spy(**kwargs):
        sink.append(kwargs.get("conversation_text", ""))
        return await original(**kwargs)

    kernel.retain_encoded = spy


def _pending_sqls(inst) -> list:
    """Pending-table SQL calls (INSERT / DELETE)."""
    return [
        sql for kind, sql, _args in inst._db.pool.conn.sqls
        if kind == "execute" and "memory_retain_pending" in sql
    ]


class TestRetainBatchGating(unittest.TestCase):
    """Gating: who batches, who encodes per turn."""

    def test_private_batches_five_turns_into_one_encode(self):
        """Private chat merges 5 turns into 1 encoding, batch includes
        all 5 turns' content."""
        inst, ctx = _ready_plugin(mod, cfg=_cfg(private=5))
        dm = FakeSession(sid="10086", stype="dm", adapter="napcat")
        captured: list[str] = []
        _spy_encode(inst, captured)

        async def scenario():
            for i in range(5):
                await _turn(inst, ctx, dm, f"私聊第{i + 1}句", mid=f"q{i + 1}")
                await asyncio.gather(*inst._retain_tasks)
            await asyncio.gather(*inst._retain_tasks)
            return inst._db.inserted_summaries

        summaries = asyncio.run(scenario())
        self.assertEqual(len(captured), 1, "5 轮私聊应合并为 1 次编码")
        self.assertEqual(len(summaries), 1)
        for i in range(5):
            self.assertIn(f"私聊第{i + 1}句", captured[0], "合并批次须含每一轮内容")
        self.assertEqual(summaries[0]["group_id"], "", "私聊 group_id 为空")

    def test_group_interval_one_keeps_per_turn_encoding(self):
        """Group chat defaults to interval=1: per-turn encoding, and no
        pending-table writes (zero behavior change)."""
        inst, ctx = _ready_plugin(mod, cfg=_cfg(group=1))
        gm = FakeSession(sid="778899", stype="gm", adapter="napcat")
        captured: list[str] = []

        async def scenario():
            _spy_encode(inst, captured)
            for i in range(3):
                await _turn(inst, ctx, gm, f"群聊第{i + 1}句", mid=f"g{i + 1}")
                await asyncio.gather(*inst._retain_tasks)
            await asyncio.gather(*inst._retain_tasks)

        asyncio.run(scenario())
        self.assertEqual(len(captured), 3, "群聊逐轮编码，每次一轮")
        self.assertEqual(
            _pending_sqls(inst), [], "群聊逐轮路径不写 pending 表"
        )

    def test_private_interval_one_keeps_per_turn_encoding(self):
        """Private chat with interval=1 also takes the per-turn path
        (rollback switch)."""
        inst, ctx = _ready_plugin(mod, cfg=_cfg(private=1))
        dm = FakeSession(sid="10086", stype="dm", adapter="napcat")
        captured: list[str] = []

        async def scenario():
            _spy_encode(inst, captured)
            for i in range(3):
                await _turn(inst, ctx, dm, f"逐轮第{i + 1}句", mid=f"p{i + 1}")
                await asyncio.gather(*inst._retain_tasks)
            await asyncio.gather(*inst._retain_tasks)

        asyncio.run(scenario())
        self.assertEqual(len(captured), 3)

    def test_batch_keeps_pre_batch_history_snapshot(self):
        """The second batch's history window = the pre-batch consumed
        rows (equivalent to nori's exclude fallback).

        Scenario: the first 5 turns merge-encode as batch 1; turn 6
        opens batch 2, whose history window should contain the first 5
        consumed rows (without re-entering batch content).
        """
        inst, ctx = _ready_plugin(mod, cfg=_cfg(private=5))
        dm = FakeSession(sid="10086", stype="dm", adapter="napcat")
        captured: list[str] = []

        async def scenario():
            _spy_encode(inst, captured)
            for i in range(5):
                await _turn(inst, ctx, dm, f"第{i + 1}句", mid=f"b{i + 1}")
                await asyncio.gather(*inst._retain_tasks)
            # Turn 6 opens a new batch: the history window should fall
            # back to the first 5 turns
            await _turn(inst, ctx, dm, "第6句", mid="b6")
            await asyncio.gather(*inst._retain_tasks)
            # Force-flush batch 2 (sweep over-age)
            inst._retain_pending_at[dm.sid] = time.time() - 9999
            await inst._sweep_retain_pending()
            await asyncio.gather(*inst._retain_tasks)

        asyncio.run(scenario())
        self.assertEqual(len(captured), 2, "应有第 1 批与第 2 批两次编码")
        first, second = captured
        for i in range(5):
            self.assertIn(f"第{i + 1}句", first, "第 1 批含前 5 轮")
            self.assertIn(f"第{i + 1}句", second, "第 2 批历史窗口含前 5 轮")
            self.assertEqual(
                second.count(f"第{i + 1}句"), 1, "历史行不得重复进批次内容"
            )
        self.assertIn("第6句", second)


class TestRetainBatchPersistence(unittest.TestCase):
    """Dual persistence: the table-row snapshot lifecycle."""

    def test_row_written_each_turn_and_deleted_after_encode(self):
        """Each turn UPSERTs a snapshot row; the row is deleted after a
        successful encode."""
        inst, ctx = _ready_plugin(mod, cfg=_cfg(private=5))
        dm = FakeSession(sid="10086", stype="dm", adapter="napcat")

        async def scenario():
            for i in range(5):
                await _turn(inst, ctx, dm, f"第{i + 1}句", mid=f"s{i + 1}")
                await asyncio.gather(*inst._retain_tasks)
            await asyncio.gather(*inst._retain_tasks)

        asyncio.run(scenario())
        sqls = _pending_sqls(inst)
        inserts = [s for s in sqls if s.lstrip().startswith("INSERT")]
        deletes = [s for s in sqls if s.lstrip().startswith("DELETE")]
        self.assertEqual(len(inserts), 5, "每轮写一次快照行")
        self.assertEqual(len(deletes), 1, "编码成功后删行一次")

    def test_failed_encode_reseats_batch_and_keeps_row(self):
        """Encode failure: the batch reseats (memory + table row kept)
        and a later trigger retries successfully."""
        inst, ctx = _ready_plugin(mod, cfg=_cfg(private=5))
        dm = FakeSession(sid="10086", stype="dm", adapter="napcat")
        kernel = inst._memory_kernel
        original = kernel.retain_encoded
        state = {"fail": True}

        async def flaky(**kwargs):
            if state["fail"]:
                raise RuntimeError("编码失败（测试注入）")
            return await original(**kwargs)

        kernel.retain_encoded = flaky
        sid = dm.sid

        async def scenario():
            for i in range(5):
                await _turn(inst, ctx, dm, f"第{i + 1}句", mid=f"f{i + 1}")
                await asyncio.gather(*inst._retain_tasks)
            await asyncio.gather(*inst._retain_tasks)
            return inst._db.inserted_summaries

        summaries = asyncio.run(scenario())
        self.assertEqual(len(summaries), 0, "失败批不落库")
        # Failure reseat: memory still holds the batch, rounds kept
        pending = inst._retain_pending.get(sid)
        self.assertIsNotNone(pending, "失败批次必须回座到内存 pending")
        self.assertEqual(int(getattr(pending, "_retain_rounds", 0)), 5)
        # The table row is still there (not deleted)
        self.assertFalse(
            [s for s in _pending_sqls(inst) if s.lstrip().startswith("DELETE")],
            "失败时不得删行",
        )
        # The next turn triggers a retry that succeeds
        state["fail"] = False
        captured: list[str] = []

        async def retry():
            _spy_encode(inst, captured)
            await _turn(inst, ctx, dm, "重试一轮", mid="retry1")
            await asyncio.gather(*inst._retain_tasks)
            await asyncio.gather(*inst._retain_tasks)
            return inst._db.inserted_summaries

        summaries = asyncio.run(retry())
        self.assertEqual(len(summaries), 1, "回座批次应在下一触发点重试编码")
        self.assertEqual(len(captured), 1, "失败批次应单独编码一次")
        self.assertIn("第5句", captured[0], "重试编码的是失败批次内容")
        self.assertNotIn(
            "重试一轮",
            captured[0],
            "失败批次不得与新轮次混批（混合链会与在途批次交错）",
        )
        pending = inst._retain_pending.get(sid)
        self.assertIsNotNone(pending, "新轮次另开批次")
        self.assertEqual(
            int(getattr(pending, "_retain_rounds", 0)), 1, "新轮次为独立批次"
        )

    def test_batch_failure_does_not_roll_back_watermark(self):
        """A batching-path failure does not roll back the watermark
        (source of truth already handed to the batch cache/table row)."""
        inst, ctx = _ready_plugin(mod, cfg=_cfg(private=5))
        dm = FakeSession(sid="10086", stype="dm", adapter="napcat")
        kernel = inst._memory_kernel

        async def boom(**kwargs):
            raise RuntimeError("编码失败（测试注入）")

        kernel.retain_encoded = boom

        async def scenario():
            for i in range(5):
                await _turn(inst, ctx, dm, f"第{i + 1}句", mid=f"w{i + 1}")
                await asyncio.gather(*inst._retain_tasks)
            await asyncio.gather(*inst._retain_tasks)

        asyncio.run(scenario())
        consumed = inst._consumed.get(dm.sid, set())
        self.assertIn("w1", consumed, "攒批路径失败不应回滚水位线")


class TestRetainBatchRecovery(unittest.TestCase):
    """Restart recovery / sweep / eviction coupling."""

    def test_restore_continues_round_count_without_immediate_encode(self):
        """Table refill: round counts continue, and a refill does not
        encode immediately (keep accumulating rather than persist
        early)."""
        inst, ctx = _ready_plugin(mod, cfg=_cfg(private=5))
        dm = FakeSession(sid="10086", stype="dm", adapter="napcat")
        payload = json.dumps(
            {
                "session_id": "10086",
                "platform": "napcat",
                "group_id": "",
                "user_id": "u1",
                "timestamp": datetime.now().isoformat(),
                "participants": [["napcat", "u1"]],
                "history_lines": ["历史行"],
                "batch_lines": ["攒批第1句", "攒批第2句", "攒批第3句"],
                "bot_lines": [],
                "rounds": 3,
            },
            ensure_ascii=False,
        )
        row = {
            "session_key": dm.sid,
            "payload": payload,
            "updated_at": datetime.now(timezone.utc).isoformat(),
        }

        async def fake_fetch(sql, *args):
            return [row]

        inst._db.pool.conn.fetch = fake_fetch
        captured: list[str] = []

        async def scenario():
            _spy_encode(inst, captured)
            await inst._restore_retain_pending()
            return inst._retain_pending.get(dm.sid)

        pending = asyncio.run(scenario())
        self.assertIsNotNone(pending, "回灌应恢复内存 pending")
        self.assertEqual(int(getattr(pending, "_retain_rounds", 0)), 3, "轮数延续")
        self.assertEqual(captured, [], "回灌不得立即编码未满批")

    def test_restored_batch_encodes_when_interval_reached(self):
        """After a refill, accumulating to the interval triggers the
        encode."""
        inst, ctx = _ready_plugin(mod, cfg=_cfg(private=5))
        dm = FakeSession(sid="10086", stype="dm", adapter="napcat")
        payload = json.dumps(
            {
                "session_id": "10086",
                "platform": "napcat",
                "group_id": "",
                "user_id": "u1",
                "timestamp": datetime.now().isoformat(),
                "participants": [["napcat", "u1"]],
                "history_lines": [],
                "batch_lines": ["攒批第1句", "攒批第2句", "攒批第3句"],
                "bot_lines": [],
                "rounds": 3,
            },
            ensure_ascii=False,
        )
        row = {
            "session_key": dm.sid,
            "payload": payload,
            "updated_at": datetime.now(timezone.utc).isoformat(),
        }

        async def fake_fetch(sql, *args):
            return [row]

        inst._db.pool.conn.fetch = fake_fetch
        captured: list[str] = []

        async def scenario():
            _spy_encode(inst, captured)
            await inst._restore_retain_pending()
            for i in range(2):
                await _turn(inst, ctx, dm, f"续攒第{i + 1}句", mid=f"r{i + 1}")
                await asyncio.gather(*inst._retain_tasks)
            await asyncio.gather(*inst._retain_tasks)

        asyncio.run(scenario())
        self.assertEqual(len(captured), 1, "回灌后续攒到间隔即编码")
        self.assertIn("续攒第1句", captured[0])
        self.assertIn("攒批第1句", captured[0], "回灌批次内容应并入最终批次")

    def test_sweep_flushes_idle_batch(self):
        """Sweep: over-age batches are force-encoded and persisted."""
        inst, ctx = _ready_plugin(mod, cfg=_cfg(private=5, flush_idle_s=60))
        dm = FakeSession(sid="10086", stype="dm", adapter="napcat")
        captured: list[str] = []

        async def scenario():
            _spy_encode(inst, captured)
            for i in range(2):
                await _turn(inst, ctx, dm, f"未满批第{i + 1}句", mid=f"i{i + 1}")
                await asyncio.gather(*inst._retain_tasks)
            # Fake over-age: rewind the last-append time by 10 minutes
            inst._retain_pending_at[dm.sid] = time.time() - 600
            await inst._sweep_retain_pending()
            await asyncio.gather(*inst._retain_tasks)

        asyncio.run(scenario())
        self.assertEqual(len(captured), 1, "超龄批次应被巡检强制编码")

    def test_sweep_keeps_fresh_batch(self):
        """Sweep: batches under the age limit are untouched."""
        inst, ctx = _ready_plugin(mod, cfg=_cfg(private=5, flush_idle_s=1800))
        dm = FakeSession(sid="10086", stype="dm", adapter="napcat")
        captured: list[str] = []

        async def scenario():
            _spy_encode(inst, captured)
            for i in range(2):
                await _turn(inst, ctx, dm, f"新批次第{i + 1}句", mid=f"n{i + 1}")
                await asyncio.gather(*inst._retain_tasks)
            await inst._sweep_retain_pending()
            await asyncio.gather(*inst._retain_tasks)

        asyncio.run(scenario())
        self.assertEqual(captured, [], "未超龄批次不应被强制编码")

    def test_session_eviction_flushes_pending_batch(self):
        """Session eviction coupling: evicting a session with a pending
        batch schedules the encode, nothing stranded."""
        inst, ctx = _ready_plugin(mod, cfg=_cfg(private=5))
        dm = FakeSession(sid="10086", stype="dm", adapter="napcat")
        other = FakeSession(sid="20002", stype="dm", adapter="napcat")
        captured: list[str] = []
        original_limit = mod._SESSION_CACHE_MAX_SESSIONS
        mod._SESSION_CACHE_MAX_SESSIONS = 1

        async def scenario():
            _spy_encode(inst, captured)
            await _turn(inst, ctx, dm, "待逐出的攒批", mid="e1")
            await _turn(inst, ctx, other, "另一个会话", mid="e2")
            await asyncio.gather(*inst._retain_tasks)
            await asyncio.gather(*inst._retain_tasks)

        try:
            asyncio.run(scenario())
        finally:
            mod._SESSION_CACHE_MAX_SESSIONS = original_limit
        self.assertEqual(len(captured), 1, "逐出时应调度该会话的攒批编码")

    def test_terminate_flushes_unfinished_batch(self):
        """Shutdown closeout: unfinished batches are force-encoded at
        terminate."""
        inst, ctx = _ready_plugin(mod, cfg=_cfg(private=5))
        dm = FakeSession(sid="10086", stype="dm", adapter="napcat")
        captured: list[str] = []

        async def scenario():
            _spy_encode(inst, captured)
            for i in range(2):
                await _turn(inst, ctx, dm, f"停机前第{i + 1}句", mid=f"t{i + 1}")
                await asyncio.gather(*inst._retain_tasks)
            await inst.terminate()
            await asyncio.gather(*inst._retain_tasks, return_exceptions=True)

        asyncio.run(scenario())
        self.assertEqual(len(captured), 1, "停机前未满批应被强制编码")


import json  # noqa: E402  (kept at the bottom to keep the header imports tidy)


class TestRetainBatchSQLiteBackend(unittest.TestCase):
    """Real production-path roundtrip (SQLite backend): migration 013 +
    pending-table reads/writes.

    The 10.0.1.91 production machine runs storage_backend=sqlite; this
    group is a release blocker: migrations apply, and
    UPSERT/SELECT/DELETE roundtrip under the sqlite shim.
    """

    def test_sqlite_migration_and_pending_roundtrip(self):
        import tempfile

        with tempfile.TemporaryDirectory(prefix="noriflow_batch_sqlite_") as tmp:
            db_path = str(Path(tmp) / "memory.sqlite3")
            config = mod.LocalMemoryConfig(
                storage_backend="sqlite",
                sqlite_path=db_path,
                embedding_dims=4,
                retain_interval_private=5,
            )
            db = mod.SQLiteMemoryDatabase(config)

            async def scenario():
                await db.connect()
                applied = await db.apply_migrations(
                    Path(__file__).resolve().parent.parent / "migrations_sqlite"
                )
                self.assertIn(13, applied, "013 迁移须被应用")

                inst = mod.NoriflowMemoryPlugin(FakeCtx(), {"dsn": DSN})
                inst._db = db
                inst._backend_name = "sqlite"
                inst._config = config
                sid = "napcat:dm:10086"
                inst._retain_pending[sid] = inst._retain_state_from_payload(
                    {
                        "session_id": "10086",
                        "platform": "napcat",
                        "group_id": "",
                        "user_id": "u1",
                        "timestamp": datetime.now().isoformat(),
                        "participants": [["napcat", "u1"]],
                        "history_lines": ["历史行"],
                        "batch_lines": ["攒批第1句"],
                        "bot_lines": [],
                        "rounds": 2,
                    }
                )
                await inst._save_retain_pending(sid)

                async with db.pool.acquire() as conn:
                    rows = await conn.fetch(
                        "SELECT session_key,payload,updated_at "
                        "FROM memory_retain_pending"
                    )
                self.assertEqual(len(rows), 1, "快照行应写入")
                raw = rows[0]["payload"]
                data = json.loads(raw) if isinstance(raw, str) else raw
                self.assertEqual(data["batch_lines"], ["攒批第1句"])
                self.assertEqual(int(data["rounds"]), 2)

                # Refill read-back
                inst._retain_pending.clear()
                await inst._restore_retain_pending()
                restored = inst._retain_pending.get(sid)
                self.assertIsNotNone(restored, "回灌应恢复 pending")
                self.assertEqual(int(getattr(restored, "_retain_rounds", 0)), 2)

                # Delete the row (cutoff = this row's write timestamp;
                # without a cutoff, skip per the P2 semantics)
                await inst._delete_retain_pending(sid, inst._retain_row_ts.get(sid))
                async with db.pool.acquire() as conn:
                    rows = await conn.fetch(
                        "SELECT session_key FROM memory_retain_pending"
                    )
                self.assertEqual(len(rows), 0, "删除后不应残留行")
                await db.close()

            asyncio.run(scenario())


class TestRetainBatchReviewFixes(unittest.TestCase):
    """Regression locks for the pr-agent review fix batch (PR #16)."""

    def test_f4_first_batch_history_is_not_duplicated(self):
        """F4: when the first batch's history window is legally empty,
        it must not fall back to the new batch's history (duplicate
        content)."""
        inst, ctx = _ready_plugin(mod, cfg=_cfg(private=5))
        dm = FakeSession(sid="10086", stype="dm", adapter="napcat")
        captured: list[str] = []

        async def scenario():
            _spy_encode(inst, captured)
            for i in range(5):
                await _turn(inst, ctx, dm, f"首轮第{i + 1}句", mid=f"d{i + 1}")
                await asyncio.gather(*inst._retain_tasks)
            await asyncio.gather(*inst._retain_tasks)

        asyncio.run(scenario())
        self.assertEqual(len(captured), 1)
        text = captured[0]
        for i in range(5):
            self.assertEqual(
                text.count(f"首轮第{i + 1}句"), 1, "同一句不得既进历史窗口又进批次内容"
            )

    def test_f1_terminate_clears_inflight_registry(self):
        """F1: terminate must clear the in-flight batch registry
        (cancelled tasks bypass _drop_inflight)."""
        inst, ctx = _ready_plugin(mod, cfg=_cfg(private=5))
        dm = FakeSession(sid="10086", stype="dm", adapter="napcat")

        async def scenario():
            await _turn(inst, ctx, dm, "在途批次", mid="inflight1")
            # Simulate the mid-encode state of "popped and registered
            # in-flight"
            inst._retain_inflight[dm.sid] = [inst._retain_pending[dm.sid]]
            await inst.terminate()

        asyncio.run(scenario())
        self.assertEqual(inst._retain_inflight, {}, "terminate 后不得残留在途登记")
        self.assertEqual(inst._retain_pending, {})

    def test_f2_kernel_missing_reseats_batch(self):
        """F2: with a missing kernel the batching path must reseat
        (otherwise this batch is never encoded for the process
        lifetime)."""
        inst, ctx = _ready_plugin(mod, cfg=_cfg(private=5))
        dm = FakeSession(sid="10086", stype="dm", adapter="napcat")

        async def scenario():
            # 4 turns, batch not yet full: still in pending
            for i in range(4):
                await _turn(inst, ctx, dm, f"第{i + 1}句", mid=f"k{i + 1}")
                await asyncio.gather(*inst._retain_tasks)
            await asyncio.gather(*inst._retain_tasks)
            # Due flush with kernel missing: batch popped -> in-flight
            # -> encode fails -> must reseat
            inst._memory_kernel = None
            await _turn(inst, ctx, dm, "第5句", mid="k5")
            await asyncio.gather(*inst._retain_tasks)
            await asyncio.gather(*inst._retain_tasks)

        asyncio.run(scenario())
        self.assertIn(dm.sid, inst._retain_pending, "批次应回座到内存 pending")
        self.assertNotIn(dm.sid, inst._retain_inflight, "在途登记不得泄漏")
        self.assertEqual(
            int(getattr(inst._retain_pending[dm.sid], "_retain_rounds", 0)),
            5,
            "回座后轮数应为 5（不得翻倍）",
        )

    def test_f3_conditional_delete_keeps_newer_row(self):
        """F3: the conditional delete removes only rows no newer than
        the cutoff (a concurrent newer turn's row is not wiped)."""
        import tempfile

        with tempfile.TemporaryDirectory(prefix="noriflow_batch_sqlite_") as tmp:
            db_path = str(Path(tmp) / "memory.sqlite3")
            config = mod.LocalMemoryConfig(
                storage_backend="sqlite",
                sqlite_path=db_path,
                embedding_dims=4,
                retain_interval_private=5,
            )
            db = mod.SQLiteMemoryDatabase(config)

            async def scenario():
                await db.connect()
                await db.apply_migrations(
                    Path(__file__).resolve().parent.parent / "migrations_sqlite"
                )
                inst = mod.NoriflowMemoryPlugin(FakeCtx(), {"dsn": DSN})
                inst._db, inst._backend_name, inst._config = db, "sqlite", config
                sid = "napcat:dm:10086"

                def _state(batch_line: str):
                    return inst._retain_state_from_payload(
                        {
                            "session_id": "10086",
                            "platform": "napcat",
                            "group_id": "",
                            "user_id": "u1",
                            "timestamp": datetime.now().isoformat(),
                            "participants": [["napcat", "u1"]],
                            "history_lines": [],
                            "batch_lines": [batch_line],
                            "bot_lines": [],
                            "rounds": 1,
                        }
                    )

                inst._retain_pending[sid] = _state("批次行")
                await inst._save_retain_pending(sid)
                old_cutoff = inst._retain_row_ts.get(sid)
                self.assertIsNotNone(old_cutoff, "写入须登记行时间戳")

                # Simulate a concurrent newer turn having written a
                # newer row
                inst._retain_pending[sid] = _state("新批次行")
                await inst._save_retain_pending(sid)

                # Delete with the old cutoff: the newer row must survive
                await inst._delete_retain_pending(sid, old_cutoff)
                async with db.pool.acquire() as conn:
                    rows = await conn.fetch(
                        "SELECT session_key FROM memory_retain_pending WHERE session_key=$1",
                        sid,
                    )
                self.assertEqual(len(rows), 1, "更晚写入的行不应被旧 cutoff 删除")

                # Delete with the newest cutoff: the row written now
                # should be deleted
                await inst._delete_retain_pending(sid, inst._retain_row_ts.get(sid))
                async with db.pool.acquire() as conn:
                    rows = await conn.fetch(
                        "SELECT session_key FROM memory_retain_pending WHERE session_key=$1",
                        sid,
                    )
                self.assertEqual(len(rows), 0, "本批快照行应被删除")
                await db.close()

            asyncio.run(scenario())


    def test_r1_delete_failure_does_not_escape_task(self):
        """R1: a delete failure on the success path must not end the
        encode task with an exception (DB jitter fallback)."""
        inst, ctx = _ready_plugin(mod, cfg=_cfg(private=5))
        dm = FakeSession(sid="10086", stype="dm", adapter="napcat")
        original_delete = inst._delete_retain_pending

        async def boom(sid, cutoff=None):
            raise RuntimeError("删行失败（测试注入）")

        inst._delete_retain_pending = boom

        async def scenario():
            results = []
            for i in range(5):
                await _turn(inst, ctx, dm, f"第{i + 1}句", mid=f"r{i + 1}")
                results += await asyncio.gather(
                    *inst._retain_tasks, return_exceptions=True
                )
            results += await asyncio.gather(
                *inst._retain_tasks, return_exceptions=True
            )
            return results

        results = asyncio.run(scenario())
        # The encode itself succeeds (the table row remains, expected:
        # an undeletable row is left for the next flush/restart to
        # overwrite)
        self.assertEqual(len(inst._db.inserted_summaries), 1, "编码仍应成功")
        # Explicit assertion: a delete failure must not end the encode
        # task with an exception (the _retain_tasks done callback
        # removes finished tasks, so the set cannot be inspected for
        # this)
        self.assertFalse(
            [r for r in results if isinstance(r, BaseException)],
            "删行失败不得让编码任务带异常结束",
        )
        inst._delete_retain_pending = original_delete

    def test_i3_cutoff_kept_when_conditional_delete_misses(self):
        """I3: when the conditional delete misses (the newer row still
        exists), the cutoff must not be discarded."""
        inst, _ = _ready_plugin(mod, cfg=_cfg(private=5))
        sid = "napcat:dm:10086"

        async def scenario():
            inst._retain_pending[sid] = inst._retain_state_from_payload(
                {
                    "session_id": "10086",
                    "platform": "napcat",
                    "group_id": "",
                    "user_id": "u1",
                    "timestamp": datetime.now().isoformat(),
                    "participants": [["napcat", "u1"]],
                    "history_lines": [],
                    "batch_lines": ["批次行"],
                    "bot_lines": [],
                    "rounds": 1,
                }
            )
            await inst._save_retain_pending(sid)
            old_cutoff = inst._retain_row_ts.get(sid)
            # The row written later (a concurrent newer turn)
            inst._retain_pending[sid] = inst._retain_state_from_payload(
                {
                    "session_id": "10086",
                    "platform": "napcat",
                    "group_id": "",
                    "user_id": "u1",
                    "timestamp": datetime.now().isoformat(),
                    "participants": [["napcat", "u1"]],
                    "history_lines": [],
                    "batch_lines": ["新批次行"],
                    "bot_lines": [],
                    "rounds": 1,
                }
            )
            await inst._save_retain_pending(sid)
            new_cutoff = inst._retain_row_ts.get(sid)
            self.assertGreater(new_cutoff, old_cutoff)
            # Delete with the old cutoff: it misses the newer row ->
            # the cutoff must be kept
            await inst._delete_retain_pending(sid, old_cutoff)
            self.assertEqual(
                inst._retain_row_ts.get(sid),
                new_cutoff,
                "条件删除未命中时不得丢弃 cutoff（否则下次删除退回 now，抹掉新批次备份）",
            )

        asyncio.run(scenario())


    def test_n1_older_snapshot_does_not_overwrite_newer_row(self):
        """N1: UPSERT timestamp guard -- an older snapshot must not
        overwrite a newer row."""
        import tempfile

        with tempfile.TemporaryDirectory(prefix="noriflow_batch_sqlite_") as tmp:
            db_path = str(Path(tmp) / "memory.sqlite3")
            config = mod.LocalMemoryConfig(
                storage_backend="sqlite",
                sqlite_path=db_path,
                embedding_dims=4,
                retain_interval_private=5,
            )
            db = mod.SQLiteMemoryDatabase(config)

            async def scenario():
                await db.connect()
                await db.apply_migrations(
                    Path(__file__).resolve().parent.parent / "migrations_sqlite"
                )
                inst = mod.NoriflowMemoryPlugin(FakeCtx(), {"dsn": DSN})
                inst._db, inst._backend_name, inst._config = db, "sqlite", config
                sid = "napcat:dm:10086"
                inst._retain_pending[sid] = inst._retain_state_from_payload(
                    {
                        "session_id": "10086",
                        "platform": "napcat",
                        "group_id": "",
                        "user_id": "u1",
                        "timestamp": datetime.now().isoformat(),
                        "participants": [["napcat", "u1"]],
                        "history_lines": [],
                        "batch_lines": ["新内容"],
                        "bot_lines": [],
                        "rounds": 2,
                    }
                )
                await inst._save_retain_pending(sid)
                async with db.pool.acquire() as conn:
                    rows = await conn.fetch(
                        "SELECT payload,updated_at FROM memory_retain_pending "
                        "WHERE session_key=$1",
                        sid,
                    )
                self.assertEqual(len(rows), 1)
                newer_payload = rows[0]["payload"]
                newer_ts = rows[0]["updated_at"]
                # Simulate out-of-order arrival: an older snapshot
                # arrives later (earlier timestamp)
                older = (
                    datetime.now(timezone.utc) - timedelta(minutes=5)
                ).isoformat(timespec="microseconds")
                async with db.pool.acquire() as conn:
                    await conn.execute(
                        "INSERT INTO memory_retain_pending(session_key,payload,updated_at) "
                        "VALUES ($1,$2,$3) ON CONFLICT(session_key) DO UPDATE SET "
                        "payload=excluded.payload, updated_at=excluded.updated_at "
                        "WHERE excluded.updated_at > memory_retain_pending.updated_at",
                        sid, json.dumps({"batch_lines": ["旧内容"]}), older,
                    )
                async with db.pool.acquire() as conn:
                    rows = await conn.fetch(
                        "SELECT payload,updated_at FROM memory_retain_pending "
                        "WHERE session_key=$1",
                        sid,
                    )
                self.assertEqual(rows[0]["payload"], newer_payload, "更旧快照不得覆盖")
                self.assertEqual(rows[0]["updated_at"], newer_ts, "时间戳不得回退")
                await db.close()

            asyncio.run(scenario())

    def test_n2_stopping_blocks_new_tasks_but_keeps_round(self):
        """N2/U1: after the stopping flag is set no new tasks may be
        dispatched, but this turn's content must not be silently
        swallowed."""
        inst, ctx = _ready_plugin(mod, cfg=_cfg(private=5))
        dm = FakeSession(sid="10086", stype="dm", adapter="napcat")

        async def scenario():
            await _turn(inst, ctx, dm, "停机前一轮", mid="stop1")
            await asyncio.gather(*inst._retain_tasks)
            inst._retain_stopping = True
            tasks_before = len(inst._retain_tasks)
            await _turn(inst, ctx, dm, "停机后一轮", mid="stop2")
            await asyncio.gather(*inst._retain_tasks)
            return tasks_before, len(inst._retain_tasks)

        before, after = asyncio.run(scenario())
        self.assertEqual(after, before, "停机后不得派发新 retain 任务")
        # The post-stop turn's delta already entered the watermark --
        # it must be kept (memory pending / table row); it must not be
        # silently dropped just because the gate returns early
        pending = inst._retain_pending.get(dm.sid)
        self.assertIsNotNone(pending, "停机后本轮批次不得丢失")
        self.assertIn(
            "停机后一轮",
            "\n".join(pending.batch_lines),
            "停机后那一轮必须保留在批次里",
        )
        inserts = [
            s for s in _pending_sqls(inst) if s.lstrip().startswith("INSERT")
        ]
        self.assertGreaterEqual(len(inserts), 2, "停机后那一轮应尽力落表行")

    def test_n3_interval_switch_merges_residual_batch(self):
        """N3/X1: when the interval shrinks (hot switch), the residual
        batch merges with this turn into ONE batch for encoding.

        Residual content is not lost and does not duplicate this
        turn's history window -- this turn's state history is produced
        by _history_snapshot, which already contains the residual
        batch's rows; encoding the residual asynchronously on its own
        would encode the same conversation twice.
        """
        inst, ctx = _ready_plugin(mod, cfg=_cfg(private=5))
        dm = FakeSession(sid="10086", stype="dm", adapter="napcat")
        captured: list[str] = []

        async def scenario():
            _spy_encode(inst, captured)
            for i in range(3):
                await _turn(inst, ctx, dm, f"残留第{i + 1}句", mid=f"res{i + 1}")
                await asyncio.gather(*inst._retain_tasks)
            await asyncio.gather(*inst._retain_tasks)
            self.assertEqual(captured, [], "3 轮未满批不应编码")
            # Hot switch: interval 5 -> 1 (the host config page mutates
            # the same config instance in place)
            inst._config.retain_interval_private = 1
            await _turn(inst, ctx, dm, "切换后一轮", mid="sw1")
            await asyncio.gather(*inst._retain_tasks)
            await asyncio.gather(*inst._retain_tasks)

        asyncio.run(scenario())
        self.assertEqual(len(captured), 1, "残留批次与本轮应合并为一次编码")
        text = captured[0]
        for i in range(3):
            self.assertEqual(
                text.count(f"残留第{i + 1}句"),
                1,
                "残留内容不得在历史窗口与批次内容中重复",
            )
        self.assertEqual(text.count("切换后一轮"), 1)
        self.assertNotIn(dm.sid, inst._retain_pending, "编码后不得残留 pending")

    def test_x2_save_skips_stale_row_after_encode_completed(self):
        """X2: if encoding completes while waiting for a connection, the
        save must not write already-encoded content as a residual
        row."""
        inst, _ = _ready_plugin(mod, cfg=_cfg(private=5))
        sid = "napcat:dm:10086"
        waiting = asyncio.Event()
        release = asyncio.Event()

        holder: dict = {}

        class _SlowAcquire:
            async def __aenter__(self):
                waiting.set()
                await release.wait()
                return holder["conn"]

            async def __aexit__(self, *exc):
                return False

        class _SlowPool:
            def acquire(self):
                return _SlowAcquire()

        async def scenario():
            inst._retain_pending[sid] = inst._retain_state_from_payload(
                {
                    "session_id": "10086",
                    "platform": "napcat",
                    "group_id": "",
                    "user_id": "u1",
                    "timestamp": datetime.now().isoformat(),
                    "participants": [["napcat", "u1"]],
                    "history_lines": [],
                    "batch_lines": ["待编码内容"],
                    "bot_lines": [],
                    "rounds": 1,
                }
            )
            original_pool = inst._db.pool
            holder["conn"] = original_pool.conn
            inst._db.pool = _SlowPool()
            try:
                task = asyncio.create_task(inst._save_retain_pending(sid))
                await waiting.wait()
                # Encoding completed while waiting for the connection:
                # unencoded data cleared
                inst._retain_pending.clear()
                inst._retain_inflight.clear()
                release.set()
                await task
            finally:
                inst._db.pool = original_pool

        asyncio.run(scenario())
        inserts = [
            s for s in _pending_sqls(inst) if s.lstrip().startswith("INSERT")
        ]
        self.assertEqual(inserts, [], "无未编码数据时不得写残留行")


    def test_p1_snapshot_write_does_not_block_turn_signal(self):
        """P1: snapshot writes must not block the turn-complete signal
        (a slow DB must not drag the main path)."""
        inst, ctx = _ready_plugin(mod, cfg=_cfg(private=5))
        dm = FakeSession(sid="10086", stype="dm", adapter="napcat")
        original_save = inst._save_retain_pending
        slow = {"release": asyncio.Event()}

        async def slow_save(sid):
            await slow["release"].wait()
            await original_save(sid)

        inst._save_retain_pending = slow_save

        async def scenario():
            await inst.observe_message(
                types.SimpleNamespace(
                    message=make_msg("u1", "慢 DB 的一轮", mid="slow1"), session=dm
                )
            )
            # Timeout fallback: if the signal truly awaited the snapshot
            # write, this times out instead of hanging
            try:
                await asyncio.wait_for(
                    ctx.event_bus.fire("session_memory_updated", {"session": dm.sid}),
                    timeout=2.0,
                )
            except asyncio.TimeoutError:
                raise AssertionError(
                    "回合完成信号不得等待快照写入（快照应走后台任务）"
                ) from None
            # Criterion: when the signal returns the snapshot write task
            # is still NOT done -> the signal did not wait for it
            # (do not just check release being unset: it is only set
            # after fire returns, so that would always hold)
            still_running = [t for t in inst._retain_tasks if not t.done()]
            slow["release"].set()
            await asyncio.gather(*inst._retain_tasks)
            return still_running

        still_running = asyncio.run(scenario())
        self.assertTrue(
            still_running,
            "回合完成信号返回时快照写入任务仍在进行（信号不得等它）",
        )

    def test_p2_delete_without_cutoff_keeps_row(self):
        """P2: skip the delete when the row timestamp is missing
        (keeping the row is safer than deleting a new snapshot by
        mistake)."""
        inst, _ = _ready_plugin(mod, cfg=_cfg(private=5))
        sid = "napcat:dm:10086"

        async def scenario():
            inst._retain_pending[sid] = inst._retain_state_from_payload(
                {
                    "session_id": "10086",
                    "platform": "napcat",
                    "group_id": "",
                    "user_id": "u1",
                    "timestamp": datetime.now().isoformat(),
                    "participants": [["napcat", "u1"]],
                    "history_lines": [],
                    "batch_lines": ["批次行"],
                    "bot_lines": [],
                    "rounds": 1,
                }
            )
            await inst._save_retain_pending(sid)
            # No cutoff: must not delete
            await inst._delete_retain_pending(sid)
            self.assertEqual(
                inst._retain_row_ts.get(sid) is not None,
                True,
                "跳过删除时不得清掉行时间戳",
            )
            self.assertIsNotNone(inst._retain_pending.get(sid))

        asyncio.run(scenario())


    def test_q1_restore_precedes_signal_subscription(self):
        """Q1: snapshot restore must precede turn-complete signal
        subscription (turns arriving during restore are not
        overwritten)."""
        ctx = FakeCtx()
        observed: list[int] = []
        original = mod.NoriflowMemoryPlugin._restore_retain_pending

        async def observing_restore(self):
            # Restore in progress: the signal handler should not be
            # subscribed yet
            observed.append(len(ctx.event_bus.subs.get("session_memory_updated", [])))
            await original(self)

        mod.NoriflowMemoryPlugin._restore_retain_pending = observing_restore
        try:
            _ready_plugin(mod, cfg=_cfg(private=5), ctx=ctx)
        finally:
            mod.NoriflowMemoryPlugin._restore_retain_pending = original

        self.assertEqual(
            observed, [0], "快照恢复期间不得已订阅回合完成信号（恢复会覆盖内存 pending）"
        )
        self.assertTrue(
            ctx.event_bus.subs.get("session_memory_updated"), "恢复完成后必须订阅信号"
        )

    def test_q1_restore_merges_with_existing_pending(self):
        """Q1: restore must not overwrite a batch already held in
        memory (turns arriving during restore already opened a new
        batch)."""
        inst, _ = _ready_plugin(mod, cfg=_cfg(private=5))
        sid = "napcat:dm:10086"
        restored = {
            "session_id": "10086",
            "platform": "napcat",
            "group_id": "",
            "user_id": "u1",
            "timestamp": datetime.now().isoformat(),
            "participants": [["napcat", "u1"]],
            "history_lines": [],
            "batch_lines": ["恢复行"],
            "bot_lines": [],
            "rounds": 2,
        }

        class _StubConn:
            async def fetch(self, sql, *args):
                return [
                    {
                        "session_key": sid,
                        "payload": json.dumps(restored),
                        "updated_at": datetime.now(timezone.utc).isoformat(
                            timespec="microseconds"
                        ),
                    }
                ]

        class _StubAcquire:
            async def __aenter__(self):
                return _StubConn()

            async def __aexit__(self, *exc):
                return False

        class _StubPool:
            def acquire(self):
                return _StubAcquire()

        async def scenario():
            # Simulate a turn that arrived during restore and opened a
            # new batch
            inst._retain_pending[sid] = inst._retain_state_from_payload(
                {**restored, "batch_lines": ["内存行"], "rounds": 1}
            )
            inst._db = types.SimpleNamespace(pool=_StubPool())
            await inst._restore_retain_pending()
            return inst._retain_pending[sid]

        merged = asyncio.run(scenario())
        lines = "\n".join(merged.batch_lines)
        self.assertIn("恢复行", lines, "恢复批次内容不得丢失")
        self.assertIn("内存行", lines, "内存批次内容不得被覆盖")
        self.assertEqual(
            int(getattr(merged, "_retain_rounds", 0)), 3, "两批次轮数应相加"
        )


    def test_s1_snapshot_merges_chronologically_after_older_reseat(self):
        """S1: after an older batch fails and reseats, the table-row
        snapshot merges in content-anchor order (the history window
        takes the earliest batch)."""
        inst, _ = _ready_plugin(mod, cfg=_cfg(private=5))
        sid = "napcat:dm:10086"
        base = datetime.now(timezone.utc)

        def _mk(offset_min, history, batch, rounds):
            stamp = (base + timedelta(minutes=offset_min)).isoformat()
            return inst._retain_state_from_payload(
                {
                    "session_id": "10086",
                    "platform": "napcat",
                    "group_id": "",
                    "user_id": "u1",
                    "timestamp": stamp,
                    "first_ts": stamp,
                    "participants": [["napcat", "u1"]],
                    "history_lines": history,
                    "batch_lines": batch,
                    "bot_lines": [],
                    "rounds": rounds,
                }
            )

        # Older batch A (its history window = the preceding context)
        # and newer batch C (whose history window already contains A's
        # rows)
        older = _mk(0, ["前置上下文"], ["A1", "A2"], 2)
        newer = _mk(10, ["前置上下文", "A1", "A2"], ["C1", "C2"], 2)

        async def scenario():
            # In-flight: A (older) and C (newer) encoding at the same
            # time
            inst._retain_inflight[sid] = [older, newer]
            # A fails and reseats: enters the pending slot (the pending
            # slot is the "newest batch" seat)
            await inst._reseat_pending_batch(sid, older)
            return inst._pending_snapshot_for_row(sid)

        merged = asyncio.run(scenario())
        self.assertEqual(
            merged.batch_lines,
            ["A1", "A2", "C1", "C2"],
            "批次行须按内容锚点的时间序合并",
        )
        self.assertEqual(
            merged.history_lines,
            ["前置上下文"],
            "历史窗口须取最早批次（否则与批次内容重复同一段对话）",
        )

    def test_s1_reseat_keeps_chronological_merge_order(self):
        """S1: when a reseat merges with existing pending, order by
        content anchor (older first)."""
        inst, _ = _ready_plugin(mod, cfg=_cfg(private=5))
        sid = "napcat:dm:10086"
        base = datetime.now(timezone.utc)

        def _mk(offset_min, history, batch):
            stamp = (base + timedelta(minutes=offset_min)).isoformat()
            return inst._retain_state_from_payload(
                {
                    "session_id": "10086",
                    "platform": "napcat",
                    "group_id": "",
                    "user_id": "u1",
                    "timestamp": stamp,
                    "first_ts": stamp,
                    "participants": [["napcat", "u1"]],
                    "history_lines": history,
                    "batch_lines": batch,
                    "bot_lines": [],
                    "rounds": 1,
                }
            )

        # The pending slot holds the older batch (also from a failure
        # reseat) and the reseated batch is newer: the old
        # implementation unconditionally treated the reseated batch as
        # old -> inverted order + wrong history window
        older = _mk(0, ["前置上下文"], ["A1"])
        newer = _mk(10, ["前置上下文", "A1"], ["C1"])

        async def scenario():
            inst._retain_pending[sid] = older
            await inst._reseat_pending_batch(sid, newer)
            return inst._retain_pending[sid]

        merged = asyncio.run(scenario())
        self.assertEqual(merged.batch_lines, ["A1", "C1"], "较旧批次须在前")
        self.assertEqual(merged.history_lines, ["前置上下文"], "历史窗口取较旧批次")


    def test_t1_failed_flag_lands_on_kept_pending_object(self):
        """T1: the failure flag must land on the object actually left
        in the pending slot (merging creates a new object)."""
        inst, ctx = _ready_plugin(mod, cfg=_cfg(private=5))
        dm = FakeSession(sid="10086", stype="dm", adapter="napcat")
        sid = dm.sid
        base = datetime.now(timezone.utc)

        def _mk(offset_min, history, batch):
            stamp = (base + timedelta(minutes=offset_min)).isoformat()
            return inst._retain_state_from_payload(
                {
                    "session_id": "10086",
                    "platform": "napcat",
                    "group_id": "",
                    "user_id": "u1",
                    "timestamp": stamp,
                    "first_ts": stamp,
                    "participants": [["napcat", "u1"]],
                    "history_lines": history,
                    "batch_lines": batch,
                    "bot_lines": [],
                    "rounds": 1,
                }
            )

        failed = _mk(0, ["前置上下文"], ["失败批内容"])
        pending = _mk(10, ["前置上下文", "失败批内容"], ["新批次内容"])
        captured: list[str] = []

        async def scenario():
            _spy_encode(inst, captured)
            # The pending slot already holds a newer batch; the failed
            # batch reseats and merges with it (creating a new object)
            inst._retain_pending[sid] = pending
            await inst._reseat_pending_batch(sid, failed)
            kept = inst._retain_pending[sid]
            self.assertTrue(
                getattr(kept, "_retain_failed", False),
                "失败标记必须落在留在 pending 槽的对象上",
            )
            # Next turn trigger: the failed batch must not mix with the
            # new turn
            await _turn(inst, ctx, dm, "新轮次内容", mid="t1n")
            await asyncio.gather(*inst._retain_tasks)
            await asyncio.gather(*inst._retain_tasks)

        asyncio.run(scenario())
        self.assertEqual(len(captured), 1, "失败批次应单独编码一次")
        self.assertIn("失败批内容", captured[0])
        self.assertIn("新批次内容", captured[0], "合并后的批次内容应一起编码")
        self.assertNotIn("新轮次内容", captured[0], "新轮次不得混进失败批次")

    def test_t3_out_of_range_config_is_clamped(self):
        """T3: out-of-range config values must clamp into the legal
        range (otherwise Pydantic validation fails and memory goes
        offline entirely)."""
        cfg = mod._build_config(
            {
                "dsn": DSN,
                "retain_interval_group": 0,
                "retain_interval_private": 0,
                "flush_idle_s": 0,
            }
        )
        self.assertEqual(cfg.retain_interval_group, 1)
        self.assertEqual(cfg.retain_interval_private, 1)
        self.assertEqual(cfg.flush_idle_s, 1.0)

        cfg_neg = mod._build_config(
            {
                "dsn": DSN,
                "retain_interval_group": -5,
                "retain_interval_private": -1,
                "flush_idle_s": -100.0,
            }
        )
        self.assertEqual(cfg_neg.retain_interval_group, 1)
        self.assertEqual(cfg_neg.retain_interval_private, 1)
        self.assertEqual(cfg_neg.flush_idle_s, 1.0)

    def test_t2_durable_default_is_false(self):
        """T2: _do_retain's durable default must be False (an
        unparameterized call keeps the pre-change semantics)."""
        import inspect

        sig = inspect.signature(mod.NoriflowMemoryPlugin._do_retain)
        self.assertIs(sig.parameters["durable"].default, False)


    def test_w1_restore_failure_triggers_retry_before_save(self):
        """W1: after a restore failure, re-restore before saving;
        otherwise the new snapshot UPSERT overwrites the old row
        (permanent loss)."""
        inst, ctx = _ready_plugin(mod, cfg=_cfg(private=5))
        dm = FakeSession(sid="10086", stype="dm", adapter="napcat")
        sid = dm.sid
        calls = {"n": 0}

        def _restored_state():
            return inst._retain_state_from_payload(
                {
                    "session_id": "10086",
                    "platform": "napcat",
                    "group_id": "",
                    "user_id": "u1",
                    "timestamp": datetime.now().isoformat(),
                    "participants": [["napcat", "u1"]],
                    "history_lines": [],
                    "batch_lines": ["崩溃备份内容"],
                    "bot_lines": [],
                    "rounds": 3,
                }
            )

        async def scenario():
            # First (init-time) restore failure -> flag a re-restore as
            # pending
            inst._retain_restore_failed = True
            restored = _restored_state()

            async def fake_restore():
                calls["n"] += 1
                if calls["n"] == 1:
                    raise RuntimeError("恢复失败（测试注入）")
                inst._retain_pending[sid] = restored
                inst._retain_restore_failed = False

            inst._restore_retain_pending = fake_restore
            # First save: the re-restore still fails -> skip the table
            # write (do not overwrite the un-refilled old row)
            await inst._save_retain_pending(sid)
            first_inserts = len(
                [s for s in _pending_sqls(inst) if s.lstrip().startswith("INSERT")]
            )
            # Second save (DB recovered): the re-restore succeeds ->
            # merge and persist
            await inst._save_retain_pending(sid)
            return calls["n"], first_inserts

        calls_n, first_inserts = asyncio.run(scenario())
        self.assertGreaterEqual(calls_n, 2, "保存前必须补恢复")
        self.assertEqual(first_inserts, 0, "补恢复仍失败时不得写表行")
        # The recovered crash-backup content must have entered the
        # table row (not lost to the new snapshot overwrite)
        payloads = [
            args[1]
            for kind, sql, args in inst._db.pool.conn.sqls
            if kind == "execute" and "memory_retain_pending" in sql and len(args) > 1
        ]
        self.assertTrue(payloads, "应写入 pending 表行")
        self.assertIn(
            "崩溃备份内容",
            payloads[-1],
            "补恢复后的崩溃备份内容必须随快照落表（不得被覆盖丢失）",
        )

    def test_w1_save_skipped_while_restore_still_failing(self):
        """W1: while the re-restore still fails, no table row may be
        written (otherwise the un-refilled crash-backup row is
        overwritten and the old content is lost forever)."""
        inst, _ = _ready_plugin(mod, cfg=_cfg(private=5))
        sid = "napcat:dm:10086"

        async def scenario():
            inst._retain_pending[sid] = inst._retain_state_from_payload(
                {
                    "session_id": "10086",
                    "platform": "napcat",
                    "group_id": "",
                    "user_id": "u1",
                    "timestamp": datetime.now().isoformat(),
                    "participants": [["napcat", "u1"]],
                    "history_lines": [],
                    "batch_lines": ["新批次内容"],
                    "bot_lines": [],
                    "rounds": 1,
                }
            )

            async def always_failing():
                raise RuntimeError("恢复持续失败（测试注入）")

            inst._restore_retain_pending = always_failing
            inst._retain_restore_failed = True
            await inst._save_retain_pending(sid)

        asyncio.run(scenario())
        inserts = [
            s for s in _pending_sqls(inst) if s.lstrip().startswith("INSERT")
        ]
        self.assertEqual(inserts, [], "补恢复失败时不得写表行（会覆盖未回灌的旧行）")
        self.assertTrue(
            inst._retain_restore_failed, "恢复仍失败时标记须保持（下次保存继续补恢复）"
        )

    def test_v1_initialize_resets_stopping_flag(self):
        """V1: assembly unconditionally resets the stopping flag (an
        exception mid-terminate must not leave the flag stuck)."""
        inst, _ = _ready_plugin(mod, cfg=_cfg(private=5))
        inst._retain_stopping = True
        asyncio.run(inst._initialize_services(inst._db, inst._config))
        self.assertFalse(inst._retain_stopping, "装配期必须复位停机标志")


    def test_y1_concurrent_restore_retry_is_serialized(self):
        """Y1: concurrent re-restores must serialize (otherwise the
        same table rows merge twice: doubled content + inflated round
        counts)."""
        inst, _ = _ready_plugin(mod, cfg=_cfg(private=5))
        calls = {"n": 0}
        release = asyncio.Event()

        async def slow_restore():
            calls["n"] += 1
            await release.wait()
            inst._retain_restore_failed = False

        inst._restore_retain_pending = slow_restore
        inst._retain_restore_failed = True

        async def scenario():
            t1 = asyncio.create_task(inst._ensure_retain_restored())
            t2 = asyncio.create_task(inst._ensure_retain_restored())
            await asyncio.sleep(0)
            release.set()
            await asyncio.gather(t1, t2)

        asyncio.run(scenario())
        self.assertEqual(calls["n"], 1, "并发补恢复必须串行（只补一次）")

    def test_y2_nickname_refresh_failure_does_not_leak_inflight(self):
        """Y2: a nickname refresh failure must not leak the durable
        batch's in-flight registration (the batch must reseat).

        Encoding also fails (injected), forcing the failure-reseat
        branch -- otherwise the case would take the success path
        (_drop_inflight) and never lock the "exception escape leaks
        the in-flight registration" scenario.
        """
        inst, ctx = _ready_plugin(mod, cfg=_cfg(private=5))
        dm = FakeSession(sid="10086", stype="dm", adapter="napcat")
        kernel = inst._memory_kernel

        async def failing_encode(**kwargs):
            raise RuntimeError("编码失败（测试注入）")

        async def boom():
            raise RuntimeError("昵称刷新失败（测试注入）")

        inst._refresh_bot_nickname = boom
        kernel.retain_encoded = failing_encode

        async def scenario():
            for i in range(3):
                await _turn(inst, ctx, dm, f"第{i + 1}句", mid=f"y2{i + 1}")
                await asyncio.gather(*inst._retain_tasks)
            await asyncio.gather(*inst._retain_tasks)
            state = inst._retain_pending[dm.sid]
            inst._retain_inflight[dm.sid] = [state]
            await inst._do_retain(state, dm.sid, set(), True)

        asyncio.run(scenario())
        self.assertNotIn(dm.sid, inst._retain_inflight, "在途登记不得泄漏")
        pending = inst._retain_pending.get(dm.sid)
        self.assertIsNotNone(pending, "批次须回座（不得丢失）")
        self.assertEqual(
            int(getattr(pending, "_retain_rounds", 0)),
            3,
            "回座批次轮数须保留",
        )

    def test_y3_restore_retry_does_not_double_merge(self):
        """Y3: a re-restore that failed midway must not merge the same
        row again (doubled content + inflated round counts)."""
        inst, _ = _ready_plugin(mod, cfg=_cfg(private=5))
        sid = "napcat:dm:10086"
        ts = datetime.now(timezone.utc).isoformat(timespec="microseconds")
        payload = {
            "session_id": "10086",
            "platform": "napcat",
            "group_id": "",
            "user_id": "u1",
            "timestamp": datetime.now().isoformat(),
            "participants": [["napcat", "u1"]],
            "history_lines": [],
            "batch_lines": ["崩溃备份内容"],
            "bot_lines": [],
            "rounds": 1,
        }

        class _StubConn:
            async def fetch(self, sql, *args):
                return [
                    {"session_key": sid, "payload": json.dumps(payload), "updated_at": ts}
                ]

        class _StubAcquire:
            async def __aenter__(self):
                return _StubConn()

            async def __aexit__(self, *exc):
                return False

        class _StubPool:
            def acquire(self):
                return _StubAcquire()

        async def scenario():
            inst._db = types.SimpleNamespace(pool=_StubPool())
            await inst._restore_retain_pending()
            first = inst._retain_pending[sid]
            # Run the re-restore again (the retry path after a midway
            # failure)
            await inst._restore_retain_pending()
            return first, inst._retain_pending[sid]

        first, second = asyncio.run(scenario())
        self.assertEqual(len(first.batch_lines), 1, "首次回灌内容")
        self.assertEqual(
            second.batch_lines,
            first.batch_lines,
            "补恢复不得把同一行再合并一遍（内容翻倍）",
        )
        self.assertEqual(
            int(getattr(second, "_retain_rounds", 0)),
            int(getattr(first, "_retain_rounds", 0)),
            "补恢复不得让轮数虚高（会提前落库）",
        )


    def test_z1_slow_call_sentinel_logs_warning(self):
        """Z1: a call exceeding the duration threshold must escalate to
        WARNING (slow-call sentinel, consistent with upstream nori)."""
        import logging

        inst, _ = _ready_plugin(mod, cfg=_cfg(private=5))
        kernel = inst._memory_kernel
        mk_mod = sys.modules["noriflow_memory_pkg.memory_kernel"]
        original_impl = kernel._build_injection_text_impl

        async def slow_impl(**kwargs):
            await asyncio.sleep(0.01)
            return "注入文本"

        kernel._build_injection_text_impl = slow_impl
        # Lower the threshold to 1ms so the 10ms call always crosses it
        # (independent of machine speed)
        threshold = getattr(mk_mod, "SLOW_CALL_THRESHOLD_MS", 500.0)
        mk_mod.SLOW_CALL_THRESHOLD_MS = 1.0
        records: list[logging.LogRecord] = []

        class _Capture(logging.Handler):
            def emit(self, record):
                records.append(record)

        handler = _Capture()
        target = logging.getLogger(mk_mod.logger.name)
        target.addHandler(handler)
        previous_level = target.level
        target.setLevel(logging.DEBUG)
        try:
            asyncio.run(
                kernel.build_injection_text(query="q", session_id="s1", top_k=1)
            )
        finally:
            target.removeHandler(handler)
            target.setLevel(previous_level)
            mk_mod.SLOW_CALL_THRESHOLD_MS = threshold
            kernel._build_injection_text_impl = original_impl

        warnings = [
            r for r in records if r.levelno >= logging.WARNING and "慢调用" in r.getMessage()
        ]
        self.assertTrue(warnings, "超阈值调用必须升级 WARNING（慢查询哨兵）")

    def test_z2_y2_test_covers_reseat_branch(self):
        """Z2: the Y2 regression lock must truly cover the
        failure-reseat branch (otherwise the lock is empty)."""
        import inspect

        src = inspect.getsource(
            TestRetainBatchReviewFixes.test_y2_nickname_refresh_failure_does_not_leak_inflight
        )
        # The reseat branch's trigger condition: encoding must fail
        # (otherwise the success path's _drop_inflight runs)
        self.assertIn("failing_encode", src)
        self.assertIn("retain_encoded = failing_encode", src)


    def test_x3_merged_residual_batch_is_backed_up_to_table(self):
        """X3: the interval<=1 residual-merge path must persist the
        table row first (a crash mid-encode must not lose this turn's
        content)."""
        inst, ctx = _ready_plugin(mod, cfg=_cfg(private=5))
        dm = FakeSession(sid="10086", stype="dm", adapter="napcat")

        async def scenario():
            for i in range(3):
                await _turn(inst, ctx, dm, f"残留第{i + 1}句", mid=f"x3r{i + 1}")
                await asyncio.gather(*inst._retain_tasks)
            await asyncio.gather(*inst._retain_tasks)
            inst._config.retain_interval_private = 1
            await _turn(inst, ctx, dm, "本轮内容", mid="x3now")
            await asyncio.gather(*inst._retain_tasks)
            await asyncio.gather(*inst._retain_tasks)

        asyncio.run(scenario())
        payloads = [
            args[1]
            for kind, sql, args in inst._db.pool.conn.sqls
            if kind == "execute"
            and "memory_retain_pending" in sql
            and sql.lstrip().upper().startswith("INSERT")
            and len(args) > 1
        ]
        self.assertTrue(payloads, "合并路径必须落表行（崩溃恢复备份）")
        self.assertIn(
            "本轮内容",
            payloads[-1],
            "表行快照必须含本轮内容（编码期间崩溃才不丢）",
        )

    def test_aa1_dirty_row_elements_are_coerced_to_str(self):
        """AA1: non-string elements of a dirty table row must not
        poison the pipeline (join would raise TypeError)."""
        inst, _ = _ready_plugin(mod, cfg=_cfg(private=5))
        state = inst._retain_state_from_payload(
            {
                "session_id": "10086",
                "platform": "napcat",
                "group_id": "",
                "user_id": "u1",
                "timestamp": datetime.now().isoformat(),
                "participants": [["napcat", "u1"]],
                "history_lines": [123, None],
                "batch_lines": [456],
                "bot_lines": [None],
                "rounds": 1,
            }
        )
        self.assertTrue(all(isinstance(x, str) for x in state.history_lines))
        self.assertTrue(all(isinstance(x, str) for x in state.batch_lines))
        self.assertTrue(all(isinstance(x, str) for x in state.bot_lines))
        # join must not raise
        self.assertIsInstance("\n".join(state.history_lines), str)

    def test_aa2_restore_keeps_newer_pending_age(self):
        """AA2: refill must not overwrite a newer in-memory batch age
        with the old row's age (the sweep would flush it early)."""
        inst, _ = _ready_plugin(mod, cfg=_cfg(private=5))
        sid = "napcat:dm:10086"
        old_ts = (datetime.now(timezone.utc) - timedelta(hours=2)).isoformat(
            timespec="microseconds"
        )
        payload = {
            "session_id": "10086",
            "platform": "napcat",
            "group_id": "",
            "user_id": "u1",
            "timestamp": datetime.now().isoformat(),
            "participants": [["napcat", "u1"]],
            "history_lines": [],
            "batch_lines": ["旧行内容"],
            "bot_lines": [],
            "rounds": 1,
        }

        class _StubConn:
            async def fetch(self, sql, *args):
                return [
                    {"session_key": sid, "payload": json.dumps(payload), "updated_at": old_ts}
                ]

        class _StubAcquire:
            async def __aenter__(self):
                return _StubConn()

            async def __aexit__(self, *exc):
                return False

        class _StubPool:
            def acquire(self):
                return _StubAcquire()

        async def scenario():
            inst._db = types.SimpleNamespace(pool=_StubPool())
            fresh_at = time.time()
            inst._retain_pending_at[sid] = fresh_at
            await inst._restore_retain_pending()
            return fresh_at, inst._retain_pending_at.get(sid)

        fresh_at, after = asyncio.run(scenario())
        self.assertIsNotNone(after)
        self.assertGreaterEqual(
            after, fresh_at, "回灌不得把更新的批次龄期改旧（会被巡检提前冲刷）"
        )

    def test_aa3_residual_pop_clears_pending_age(self):
        """AA3: when the residual batch leaves the pending slot, its
        age entry is cleared in sync (no dangling keys)."""
        inst, ctx = _ready_plugin(mod, cfg=_cfg(private=5))
        dm = FakeSession(sid="10086", stype="dm", adapter="napcat")

        async def scenario():
            for i in range(3):
                await _turn(inst, ctx, dm, f"残留第{i + 1}句", mid=f"aa3{i + 1}")
                await asyncio.gather(*inst._retain_tasks)
            await asyncio.gather(*inst._retain_tasks)
            self.assertIn(dm.sid, inst._retain_pending_at)
            inst._config.retain_interval_private = 1
            await _turn(inst, ctx, dm, "本轮内容", mid="aa3now")
            await asyncio.gather(*inst._retain_tasks)
            await asyncio.gather(*inst._retain_tasks)

        asyncio.run(scenario())
        self.assertNotIn(
            dm.sid, inst._retain_pending_at, "残留出槽后不得留悬空的龄期键"
        )


    def test_bb1_snapshot_merge_failure_keeps_row(self):
        """BB1: a snapshot-merge exception on the success path must not
        escape the task, and the table row must be conservatively
        kept."""
        inst, _ = _ready_plugin(mod, cfg=_cfg(private=5))
        sid = "napcat:dm:10086"

        async def scenario():
            inst._retain_pending[sid] = inst._retain_state_from_payload(
                {
                    "session_id": "10086",
                    "platform": "napcat",
                    "group_id": "",
                    "user_id": "u1",
                    "timestamp": datetime.now().isoformat(),
                    "participants": [["napcat", "u1"]],
                    "history_lines": [],
                    "batch_lines": ["批次行"],
                    "bot_lines": [],
                    "rounds": 1,
                }
            )
            await inst._save_retain_pending(sid)

            def boom(sid):
                raise TypeError("合并失败（测试注入）")

            inst._pending_snapshot_for_row = boom
            # Must not raise (an exception would become "Task exception
            # was never retrieved")
            await inst._do_retain(inst._retain_pending[sid], sid, set(), True)

        asyncio.run(scenario())
        deletes = [
            s for s in _pending_sqls(inst) if s.lstrip().startswith("DELETE")
        ]
        self.assertEqual(deletes, [], "合并失败时不得删表行（删了会丢未编码内容）")

    def test_bb2_dirty_rounds_is_clamped(self):
        """BB2: a dirty row's 0/negative rounds must clamp to >=1
        (batching cadence unchanged)."""
        inst, _ = _ready_plugin(mod, cfg=_cfg(private=5))
        for raw in (0, -3):
            state = inst._retain_state_from_payload(
                {
                    "session_id": "10086",
                    "platform": "napcat",
                    "group_id": "",
                    "user_id": "u1",
                    "timestamp": datetime.now().isoformat(),
                    "participants": [["napcat", "u1"]],
                    "history_lines": [],
                    "batch_lines": ["行"],
                    "bot_lines": [],
                    "rounds": raw,
                }
            )
            self.assertGreaterEqual(
                int(getattr(state, "_retain_rounds", 0)), 1, "轮数须钳到 >=1"
            )

    def test_bb3_mixed_timezone_anchors_are_comparable(self):
        """BB3: mixing naive and aware anchors must not raise TypeError
        on comparison/sorting."""
        inst, _ = _ready_plugin(mod, cfg=_cfg(private=5))
        sid = "napcat:dm:10086"
        naive = datetime.now()
        aware = datetime.now(timezone.utc)

        def _mk(ts, first_ts, line):
            return inst._retain_state_from_payload(
                {
                    "session_id": "10086",
                    "platform": "napcat",
                    "group_id": "",
                    "user_id": "u1",
                    "timestamp": ts.isoformat(),
                    "first_ts": first_ts.isoformat(),
                    "participants": [["napcat", "u1"]],
                    "history_lines": [],
                    "batch_lines": [line],
                    "bot_lines": [],
                    "rounds": 1,
                }
            )

        older_naive = _mk(naive, naive, "naive 批")
        newer_aware = _mk(aware, aware, "aware 批")

        async def scenario():
            inst._retain_inflight[sid] = [newer_aware]
            inst._retain_pending[sid] = older_naive
            return inst._pending_snapshot_for_row(sid)

        merged = asyncio.run(scenario())
        self.assertIsNotNone(merged, "混合时区不得让快照链路抛异常")
        self.assertEqual(len(merged.batch_lines), 2, "两批内容都在")


    def test_cc1_restore_does_not_rollback_newer_row_ts(self):
        """CC1: refill must not pull a registered newer row timestamp
        back (a regressed cutoff would fail to delete the newer
        row)."""
        inst, _ = _ready_plugin(mod, cfg=_cfg(private=5))
        sid = "napcat:dm:10086"
        newer = datetime.now(timezone.utc)
        older = newer - timedelta(hours=1)

        class _StubConn:
            async def fetch(self, sql, *args):
                return [
                    {
                        "session_key": sid,
                        "payload": json.dumps(
                            {
                                "session_id": "10086",
                                "platform": "napcat",
                                "group_id": "",
                                "user_id": "u1",
                                "timestamp": datetime.now().isoformat(),
                                "participants": [["napcat", "u1"]],
                                "history_lines": [],
                                "batch_lines": ["旧行"],
                                "bot_lines": [],
                                "rounds": 1,
                            }
                        ),
                        "updated_at": older.isoformat(timespec="microseconds"),
                    }
                ]

        class _StubAcquire:
            async def __aenter__(self):
                return _StubConn()

            async def __aexit__(self, *exc):
                return False

        class _StubPool:
            def acquire(self):
                return _StubAcquire()

        async def scenario():
            inst._db = types.SimpleNamespace(pool=_StubPool())
            # A concurrent save already registered the newer timestamp
            inst._retain_row_ts[sid] = newer
            await inst._restore_retain_pending()
            return inst._retain_row_ts.get(sid)

        after = asyncio.run(scenario())
        self.assertEqual(
            after, newer, "回灌不得把较新的行时间戳拉回（cutoff 只前进不后退）"
        )

    def test_cc2_flush_idle_zero_uses_same_clamp_as_build(self):
        """CC2: on a hot switch to flush_idle_s=0, the sweep threshold
        must match assembly time (1.0, not 1800)."""
        inst, _ = _ready_plugin(mod, cfg=_cfg(private=5, flush_idle_s=1800.0))
        inst._config.flush_idle_s = 0
        self.assertEqual(inst._retain_flush_idle_s(), 1.0)
        inst._config.flush_idle_s = -5
        self.assertEqual(inst._retain_flush_idle_s(), 1.0)
        inst._config.flush_idle_s = 60
        self.assertEqual(inst._retain_flush_idle_s(), 60.0)

    def test_cc3_dirty_participants_are_not_split_into_chars(self):
        """CC3: dirty participants must not be split into single-char
        tuples (silent corruption of participant info)."""
        inst, _ = _ready_plugin(mod, cfg=_cfg(private=5))
        state = inst._retain_state_from_payload(
            {
                "session_id": "10086",
                "platform": "napcat",
                "group_id": "",
                "user_id": "u1",
                "timestamp": datetime.now().isoformat(),
                "participants": ["u1", ["napcat", "u2"], ["only"]],
                "history_lines": [],
                "batch_lines": ["行"],
                "bot_lines": [],
                "rounds": 1,
            }
        )
        self.assertEqual(state.participants[0], ("u1", ""), "字符串不得被拆成字符元组")
        self.assertEqual(state.participants[1], ("napcat", "u2"))
        self.assertEqual(state.participants[2], ("only", ""))
        self.assertTrue(all(len(p) == 2 for p in state.participants))

    def test_cc4_merge_preserves_failed_flag(self):
        """1061: merging must propagate _retain_failed (otherwise the
        failed batch mixes with new turns)."""
        inst, _ = _ready_plugin(mod, cfg=_cfg(private=5))
        base = {
            "session_id": "10086",
            "platform": "napcat",
            "group_id": "",
            "user_id": "u1",
            "timestamp": datetime.now().isoformat(),
            "participants": [["napcat", "u1"]],
            "history_lines": [],
            "batch_lines": ["行"],
            "bot_lines": [],
            "rounds": 1,
        }
        older = inst._retain_state_from_payload(base)
        newer = inst._retain_state_from_payload(base)
        older._retain_failed = True
        merged = inst._merge_retain_states(older, newer)
        self.assertTrue(
            getattr(merged, "_retain_failed", False),
            "合并必须传播失败标记（守卫靠它避免混批）",
        )
        merged2 = inst._merge_retain_states(newer, older)
        self.assertTrue(
            getattr(merged2, "_retain_failed", False),
            "任一输入带标记，合并结果都必须带",
        )


    def test_dd1_scalar_container_payload_is_handled(self):
        """DD1: a scalar container field in the payload must not be
        iterated per char or raise (the whole row should not be
        skipped)."""
        inst, _ = _ready_plugin(mod, cfg=_cfg(private=5))
        state = inst._retain_state_from_payload(
            {
                "session_id": "10086",
                "platform": "napcat",
                "group_id": "",
                "user_id": "u1",
                "timestamp": datetime.now().isoformat(),
                # Containers are scalars: a string would iterate per
                # char, an int is not iterable
                "participants": "u1",
                "history_lines": "abc",
                "batch_lines": 123,
                "bot_lines": None,
                "rounds": 1,
            }
        )
        self.assertEqual(state.participants, [], "标量容器按空处理（不得拆成字符）")
        self.assertEqual(state.history_lines, [], "标量字符串不得逐字符迭代")
        self.assertEqual(state.batch_lines, [])
        self.assertEqual(state.bot_lines, [])
        # The whole row was not skipped: the batch content field can
        # still carry valid data
        ok_state = inst._retain_state_from_payload(
            {
                "session_id": "10086",
                "platform": "napcat",
                "group_id": "",
                "user_id": "u1",
                "timestamp": datetime.now().isoformat(),
                "participants": [["napcat", "u1"]],
                "history_lines": [],
                "batch_lines": ["有效内容"],
                "bot_lines": [],
                "rounds": 1,
            }
        )
        self.assertEqual(ok_state.batch_lines, ["有效内容"])


    def test_ee1_missing_timestamp_does_not_drop_row(self):
        """EE1: a payload missing timestamp must not raise KeyError
        (the whole row's content would be skipped)."""
        inst, _ = _ready_plugin(mod, cfg=_cfg(private=5))
        state = inst._retain_state_from_payload(
            {
                "session_id": "10086",
                "platform": "napcat",
                "group_id": "",
                "user_id": "u1",
                # no timestamp
                "participants": [["napcat", "u1"]],
                "history_lines": [],
                "batch_lines": ["内容"],
                "bot_lines": [],
                "rounds": 1,
            }
        )
        self.assertEqual(state.batch_lines, ["内容"], "整行内容不得因缺时间戳被丢")
        self.assertIsInstance(state.timestamp, datetime)

    def test_ee2_row_ts_registered_before_sql(self):
        """EE2: register the row timestamp even when the write fails
        (a conditional delete inside the registration window would
        find no cutoff)."""
        inst, _ = _ready_plugin(mod, cfg=_cfg(private=5))
        sid = "napcat:dm:10086"

        class _BoomConn:
            async def execute(self, sql, *args):
                raise RuntimeError("写入失败（测试注入）")

        class _BoomAcquire:
            async def __aenter__(self):
                return _BoomConn()

            async def __aexit__(self, *exc):
                return False

        class _BoomPool:
            def acquire(self):
                return _BoomAcquire()

        async def scenario():
            inst._retain_pending[sid] = inst._retain_state_from_payload(
                {
                    "session_id": "10086",
                    "platform": "napcat",
                    "group_id": "",
                    "user_id": "u1",
                    "timestamp": datetime.now().isoformat(),
                    "participants": [["napcat", "u1"]],
                    "history_lines": [],
                    "batch_lines": ["行"],
                    "bot_lines": [],
                    "rounds": 1,
                }
            )
            inst._db = types.SimpleNamespace(pool=_BoomPool())
            with self.assertRaises(RuntimeError):
                await inst._save_retain_pending(sid)
            return inst._retain_row_ts.get(sid)

        ts = asyncio.run(scenario())
        self.assertIsNotNone(ts, "写入失败也须登记行时间戳（cutoff 依据）")

    def test_ee3_non_numeric_rounds_does_not_drop_row(self):
        """EE3: dirty non-numeric rounds must not raise ValueError (the
        whole row's content would be skipped)."""
        inst, _ = _ready_plugin(mod, cfg=_cfg(private=5))
        state = inst._retain_state_from_payload(
            {
                "session_id": "10086",
                "platform": "napcat",
                "group_id": "",
                "user_id": "u1",
                "timestamp": datetime.now().isoformat(),
                "participants": [["napcat", "u1"]],
                "history_lines": [],
                "batch_lines": ["内容"],
                "bot_lines": [],
                "rounds": "abc",
            }
        )
        self.assertEqual(state.batch_lines, ["内容"])
        self.assertEqual(int(getattr(state, "_retain_rounds", 0)), 1)

    def test_ee4_terminate_waits_for_inflight_handler(self):
        """EE4: terminate must wait for in-flight turn handlers to
        finish (otherwise the shutdown snapshot write lands on a closed
        pool)."""
        inst, ctx = _ready_plugin(mod, cfg=_cfg(private=5))
        dm = FakeSession(sid="10086", stype="dm", adapter="napcat")
        release = asyncio.Event()
        order: list[str] = []

        async def slow_impl(event):
            await release.wait()
            order.append("handler_done")

        inst._on_session_memory_updated_impl = slow_impl

        async def scenario():
            handler = asyncio.create_task(
                ctx.event_bus.fire("session_memory_updated", {"session": dm.sid})
            )
            await asyncio.sleep(0)  # let the handler enter its wait
            terminate = asyncio.create_task(inst.terminate())
            await asyncio.sleep(0.1)
            self.assertFalse(
                terminate.done(), "terminate 不得在在途 handler 收尾前完成"
            )
            release.set()
            await asyncio.gather(handler, terminate, return_exceptions=True)
            order.append("terminate_done")

        asyncio.run(scenario())
        self.assertEqual(
            order[:2], ["handler_done", "terminate_done"], "terminate 须等 handler 收尾"
        )

    def test_ee5_post_write_recheck_deletes_stale_row(self):
        """EE5: if encoding completes inside the write window, the
        post-write recheck should delete the residual row holding
        already-encoded content."""
        inst, _ = _ready_plugin(mod, cfg=_cfg(private=5))
        sid = "napcat:dm:10086"
        waiting = asyncio.Event()
        release = asyncio.Event()
        holder: dict = {}

        class _SlowConn:
            async def execute(self, sql, *args):
                if sql.lstrip().upper().startswith("INSERT"):
                    waiting.set()
                    await release.wait()
                holder["sqls"].append(("execute", sql, args))

        class _SlowAcquire:
            async def __aenter__(self):
                return _SlowConn()

            async def __aexit__(self, *exc):
                return False

        class _SlowPool:
            def acquire(self):
                return _SlowAcquire()

        async def scenario():
            inst._retain_pending[sid] = inst._retain_state_from_payload(
                {
                    "session_id": "10086",
                    "platform": "napcat",
                    "group_id": "",
                    "user_id": "u1",
                    "timestamp": datetime.now().isoformat(),
                    "participants": [["napcat", "u1"]],
                    "history_lines": [],
                    "batch_lines": ["行"],
                    "bot_lines": [],
                    "rounds": 1,
                }
            )
            holder["sqls"] = []
            inst._db = types.SimpleNamespace(pool=_SlowPool())
            task = asyncio.create_task(inst._save_retain_pending(sid))
            await waiting.wait()
            # Encoding for the same session completed inside the write
            # window: unencoded data cleared
            inst._retain_pending.clear()
            inst._retain_inflight.clear()
            release.set()
            await task
            return holder["sqls"]

        sqls = asyncio.run(scenario())
        deletes = [
            sql for kind, sql, args in sqls if sql.lstrip().upper().startswith("DELETE")
        ]
        self.assertTrue(deletes, "写后复查发现无未编码数据时须删掉残留行")


    def test_ff1_stopping_turn_is_not_dropped_by_ready_gate(self):
        """FF1: a turn arriving during shutdown must reach the stopping
        branch (not be swallowed by the _ready gate)."""
        inst, ctx = _ready_plugin(mod, cfg=_cfg(private=5))
        dm = FakeSession(sid="10086", stype="dm", adapter="napcat")

        async def scenario():
            await inst.observe_message(
                types.SimpleNamespace(
                    message=make_msg("u1", "停机窗口的一轮", mid="ff1m"), session=dm
                )
            )
            # Simulate terminate having set the stopping flag and
            # _ready False
            inst._retain_stopping = True
            inst._ready = False
            await ctx.event_bus.fire("session_memory_updated", {"session": dm.sid})
            await asyncio.gather(*inst._retain_tasks)

        asyncio.run(scenario())
        pending = inst._retain_pending.get(dm.sid)
        self.assertIsNotNone(pending, "停机窗口的回合不得被门控吞掉")
        self.assertIn(
            "停机窗口的一轮",
            "\n".join(pending.batch_lines),
            "停机窗口那一轮的内容必须保留",
        )

    def test_ff2_failed_flag_survives_restart(self):
        """FF2: the failure flag must persist with the payload and be
        read back after restore (no mixing with new turns)."""
        inst, _ = _ready_plugin(mod, cfg=_cfg(private=5))
        state = inst._retain_state_from_payload(
            {
                "session_id": "10086",
                "platform": "napcat",
                "group_id": "",
                "user_id": "u1",
                "timestamp": datetime.now().isoformat(),
                "participants": [["napcat", "u1"]],
                "history_lines": [],
                "batch_lines": ["行"],
                "bot_lines": [],
                "rounds": 2,
                "failed": True,
            }
        )
        self.assertTrue(
            getattr(state, "_retain_failed", False), "恢复后须读回失败标记"
        )
        payload = inst._retain_payload(state)
        self.assertTrue(payload.get("failed"), "payload 须带失败标记")
        # A clean batch does not carry the flag
        plain = inst._retain_state_from_payload(
            {
                "session_id": "10086",
                "platform": "napcat",
                "group_id": "",
                "user_id": "u1",
                "timestamp": datetime.now().isoformat(),
                "participants": [["napcat", "u1"]],
                "history_lines": [],
                "batch_lines": ["行"],
                "bot_lines": [],
                "rounds": 1,
            }
        )
        self.assertFalse(inst._retain_payload(plain).get("failed"))


    def test_gg1_broken_row_does_not_abort_restore_loop(self):
        """GG1: one broken row skips only itself and must not abort the
        whole restore round (later rows would never refill)."""
        inst, _ = _ready_plugin(mod, cfg=_cfg(private=5))
        good_sid = "napcat:dm:good"
        good_payload = {
            "session_id": "good",
            "platform": "napcat",
            "group_id": "",
            "user_id": "u1",
            "timestamp": datetime.now().isoformat(),
            "participants": [["napcat", "u1"]],
            "history_lines": [],
            "batch_lines": ["好行内容"],
            "bot_lines": [],
            "rounds": 1,
        }

        class _BadKey:
            def __str__(self):
                raise RuntimeError("坏键（测试注入）")

        class _StubConn:
            async def fetch(self, sql, *args):
                return [
                    # Broken row: raises on session_key access (merge/
                    # age computation could raise the same way)
                    {"session_key": _BadKey(), "payload": json.dumps(good_payload),
                     "updated_at": datetime.now(timezone.utc).isoformat()},
                    {"session_key": good_sid, "payload": json.dumps(good_payload),
                     "updated_at": datetime.now(timezone.utc).isoformat()},
                ]

        class _StubAcquire:
            async def __aenter__(self):
                return _StubConn()

            async def __aexit__(self, *exc):
                return False

        class _StubPool:
            def acquire(self):
                return _StubAcquire()

        async def scenario():
            inst._db = types.SimpleNamespace(pool=_StubPool())
            await inst._restore_retain_pending()
            return inst._retain_pending.get(good_sid)

        pending = asyncio.run(scenario())
        self.assertIsNotNone(pending, "坏行不得中断整轮恢复（后面的行仍须回灌）")
        self.assertIn("好行内容", pending.batch_lines)

    def test_gg2_nan_flush_idle_falls_back_to_default(self):
        """GG2: NaN/±inf idle thresholds must fall back to the default
        (NaN would make the sweep flush every batch early)."""
        inst, _ = _ready_plugin(mod, cfg=_cfg(private=5, flush_idle_s=1800.0))
        inst._config.flush_idle_s = float("nan")
        self.assertEqual(inst._retain_flush_idle_s(), 1800.0)
        inst._config.flush_idle_s = float("inf")
        self.assertEqual(inst._retain_flush_idle_s(), 1800.0)
        inst._config.flush_idle_s = float("-inf")
        self.assertEqual(inst._retain_flush_idle_s(), 1800.0)
        inst._config.flush_idle_s = 60
        self.assertEqual(inst._retain_flush_idle_s(), 60.0)


    def test_hh1_nan_config_value_does_not_break_assembly(self):
        """HH1: NaN/±inf config values must not fail assembly (a gt/ge
        failure in LocalMemoryConfig takes memory offline)."""
        cfg = mod._build_config(
            {
                "dsn": DSN,
                "flush_idle_s": float("nan"),
                "backfill_interval_seconds": float("inf"),
                "merge_interval_hours": float("-inf"),
            }
        )
        self.assertEqual(cfg.flush_idle_s, 1800.0)
        self.assertEqual(cfg.backfill_interval_seconds, 300.0)
        self.assertEqual(cfg.merge_interval_hours, 6.0)
        # The string form is intercepted the same way
        cfg2 = mod._build_config({"dsn": DSN, "flush_idle_s": "nan"})
        self.assertEqual(cfg2.flush_idle_s, 1800.0)

    def test_hh2_empty_participant_is_skipped(self):
        """HH2: empty participant entries are skipped outright
        (("", "") would pollute the downstream participant list)."""
        inst, _ = _ready_plugin(mod, cfg=_cfg(private=5))
        state = inst._retain_state_from_payload(
            {
                "session_id": "10086",
                "platform": "napcat",
                "group_id": "",
                "user_id": "u1",
                "timestamp": datetime.now().isoformat(),
                "participants": [[], ["napcat", "u1"]],
                "history_lines": [],
                "batch_lines": ["行"],
                "bot_lines": [],
                "rounds": 1,
            }
        )
        self.assertEqual(state.participants, [("napcat", "u1")], "空参与者须跳过")

    def test_hh3_flush_reuses_task_spawn_helper(self):
        """HH3: batch flushing must reuse the unified task-spawn entry
        (the same funnel set as the per-turn path)."""
        import inspect

        src = inspect.getsource(mod.NoriflowMemoryPlugin._flush_pending_batch)
        self.assertIn("_spawn_retain_task", src, "须复用统一的任务创建入口")
        self.assertNotIn(
            "asyncio.create_task", src, "不得再手搓一套建任务逻辑（两处易不同步）"
        )


    def test_ii1_overflow_config_values_do_not_break_assembly(self):
        """II1: OverflowError dirty values must not fail assembly
        (float(10**400)/int(inf))."""
        cfg = mod._build_config(
            {
                "dsn": DSN,
                "flush_idle_s": 10 ** 400,
                "backfill_interval_seconds": float("inf"),
                "pool_max": float("inf"),
            }
        )
        self.assertEqual(cfg.flush_idle_s, 1800.0)
        self.assertEqual(cfg.backfill_interval_seconds, 300.0)
        self.assertEqual(cfg.pool_max, 8)

    def test_ii2_overflow_flush_idle_does_not_kill_sweep(self):
        """II2: runtime dirty thresholds must not kill the sweep
        coroutine (_retain_flush_idle_s does not raise)."""
        inst, _ = _ready_plugin(mod, cfg=_cfg(private=5))
        inst._config.flush_idle_s = 10 ** 400
        self.assertEqual(inst._retain_flush_idle_s(), 1800.0)
        inst._config.flush_idle_s = float("inf")
        self.assertEqual(inst._retain_flush_idle_s(), 1800.0)


    def test_jj1_infinite_interval_does_not_drop_turn(self):
        """JJ1: a hot switch setting the interval to inf makes int()
        raise OverflowError; the whole turn must not be swallowed."""
        inst, ctx = _ready_plugin(mod, cfg=_cfg(private=5))
        dm = FakeSession(sid="10086", stype="dm", adapter="napcat")
        inst._config.retain_interval_private = float("inf")

        async def scenario():
            await _turn(inst, ctx, dm, "脏间隔的一轮", mid="jj1m")
            await asyncio.gather(*inst._retain_tasks)
            await asyncio.gather(*inst._retain_tasks)
            return inst._db.inserted_summaries

        summaries = asyncio.run(scenario())
        self.assertEqual(
            len(summaries), 1, "脏间隔回退为逐轮路径后该轮必须被编码（不得静默丢失）"
        )

    def test_jj3_infinite_rounds_does_not_drop_row(self):
        """JJ3: a dirty row with rounds=Infinity must not have the
        whole row skipped as bad."""
        inst, _ = _ready_plugin(mod, cfg=_cfg(private=5))
        state = inst._retain_state_from_payload(
            {
                "session_id": "10086",
                "platform": "napcat",
                "group_id": "",
                "user_id": "u1",
                "timestamp": datetime.now().isoformat(),
                "participants": [["napcat", "u1"]],
                "history_lines": [],
                "batch_lines": ["内容"],
                "bot_lines": [],
                "rounds": float("inf"),
            }
        )
        self.assertEqual(state.batch_lines, ["内容"], "整行内容不得被丢弃")
        self.assertEqual(int(getattr(state, "_retain_rounds", 0)), 1)

    def test_jj2_alias_upsert_is_bounded(self):
        """JJ2: the alias upsert must not drag the handler into a DB
        timeout (shutdown closeout would cut that turn)."""
        import inspect

        src = inspect.getsource(mod.NoriflowMemoryPlugin._on_session_memory_updated_impl)
        self.assertRegex(
            src,
            r"wait_for\(\s*self\._alias_upsert_batch",
            "别名 upsert 必须有界等待（否则慢 DB 会把 handler 拖到 DB 超时）",
        )


    def test_kk2_dirty_failed_field_does_not_mark_batch_failed(self):
        """KK2: a dirty failed field (the string "false") must not mark
        a healthy batch as failed."""
        inst, _ = _ready_plugin(mod, cfg=_cfg(private=5))
        base = {
            "session_id": "10086",
            "platform": "napcat",
            "group_id": "",
            "user_id": "u1",
            "timestamp": datetime.now().isoformat(),
            "participants": [["napcat", "u1"]],
            "history_lines": [],
            "batch_lines": ["行"],
            "bot_lines": [],
            "rounds": 1,
        }
        for raw in ("false", "False", "0", "", 0, None):
            state = inst._retain_state_from_payload({**base, "failed": raw})
            self.assertFalse(
                getattr(state, "_retain_failed", False),
                f"failed={raw!r} 不得被当作真值",
            )
        for raw in (True, 1, "true", "yes", "1"):
            state = inst._retain_state_from_payload({**base, "failed": raw})
            self.assertTrue(
                getattr(state, "_retain_failed", False),
                f"failed={raw!r} 应识别为真值",
            )

    def test_kk1_shutdown_deadline_uses_monotonic_clock(self):
        """KK1: the shutdown wait budget must use a monotonic clock
        (wall-clock rollback makes the wait timeout uncontrollable)."""
        import inspect

        src = inspect.getsource(mod.NoriflowMemoryPlugin.terminate)
        self.assertIn("time.monotonic()", src, "停机等待须用单调时钟")
        self.assertNotIn(
            "time.time() + 5.0", src, "不得用墙钟做停机等待预算"
        )


    def test_ll1_alias_upsert_handlers_are_split(self):
        """LL1: the alias-upsert timeout (expected path) logs no stack;
        only real exceptions do."""
        import inspect

        src = inspect.getsource(mod.NoriflowMemoryPlugin._on_session_memory_updated_impl)
        self.assertNotIn(
            "except (asyncio.TimeoutError, Exception)",
            src,
            "TimeoutError 是 Exception 子类，元组写法纯属叠甲",
        )
        self.assertIn("except asyncio.TimeoutError:", src, "超时须单独捕获")
        # The timeout branch must not carry exc_info (printing a stack
        # on the expected path would drown real exceptions)
        timeout_branch = src.split("except asyncio.TimeoutError:")[1].split("except")[0]
        self.assertNotIn("exc_info", timeout_branch, "超时分支不应打印堆栈")

    def test_ll2_restore_log_reports_both_counts(self):
        """LL2: the restore log must distinguish "table rows read" from
        "in-memory pending session count"."""
        import inspect

        src = inspect.getsource(mod.NoriflowMemoryPlugin._restore_retain_pending)
        self.assertIn("len(self._retain_pending)", src, "日志须带内存 pending 会话数")
        self.assertNotIn(
            '"恢复 %d 个 retain pending 快照"', src, "旧口径（行数当会话数）已误导"
        )


    def test_mm1_initialize_resets_handler_counter(self):
        """MM1: assembly must reset the in-flight handler count
        (terminate-timeout residue would make the next shutdown wait 5s
        for nothing)."""
        inst, _ = _ready_plugin(mod, cfg=_cfg(private=5))
        inst._retain_handler_active = 3
        asyncio.run(inst._initialize_services(inst._db, inst._config))
        self.assertEqual(inst._retain_handler_active, 0, "装配期必须复位在途计数")


    def test_nn1_snapshot_timestamp_never_goes_backwards(self):
        """NN1: snapshot write timestamps must be monotonic
        (wall-clock rollback / same-microsecond writes must not be
        dropped by the UPSERT guard)."""
        inst, _ = _ready_plugin(mod, cfg=_cfg(private=5))
        sid = "napcat:dm:10086"
        future = datetime.now(timezone.utc) + timedelta(hours=1)

        async def scenario():
            inst._retain_pending[sid] = inst._retain_state_from_payload(
                {
                    "session_id": "10086",
                    "platform": "napcat",
                    "group_id": "",
                    "user_id": "u1",
                    "timestamp": datetime.now().isoformat(),
                    "participants": [["napcat", "u1"]],
                    "history_lines": [],
                    "batch_lines": ["行"],
                    "bot_lines": [],
                    "rounds": 1,
                }
            )
            # Simulate a registered timestamp in the future (one write
            # after wall-clock rollback)
            inst._retain_row_ts[sid] = future
            await inst._save_retain_pending(sid)
            return inst._retain_row_ts.get(sid)

        ts = asyncio.run(scenario())
        self.assertIsNotNone(ts)
        self.assertGreater(ts, future, "写入时间戳必须单调前进（不得被墙钟回拨拉回）")

    def test_nn2_stale_handler_does_not_corrupt_new_generation_count(self):
        """NN2: an old-generation handler finishing must not decrement
        the new generation's count negative (terminate would misjudge
        no in-flight work)."""
        inst, ctx = _ready_plugin(mod, cfg=_cfg(private=5))
        dm = FakeSession(sid="10086", stype="dm", adapter="napcat")
        release = asyncio.Event()

        async def slow_impl(event):
            await release.wait()

        inst._on_session_memory_updated_impl = slow_impl

        async def scenario():
            handler = asyncio.create_task(
                ctx.event_bus.fire("session_memory_updated", {"session": dm.sid})
            )
            await asyncio.sleep(0)
            # Reassemble (count reset + generation bump), then the old
            # handler finishes
            await inst._initialize_services(inst._db, inst._config)
            release.set()
            await handler
            return inst._retain_handler_active

        active = asyncio.run(scenario())
        self.assertGreaterEqual(active, 0, "在途 handler 计数不得为负")
        self.assertEqual(active, 0, "旧代 handler 收尾后新代计数应为 0")


    def test_oo1_out_of_range_field_falls_back_individually(self):
        """OO1: a single out-of-range config key falls back alone
        (other user config unchanged)."""
        cfg = mod._try_build_config(
            {
                "dsn": DSN,
                "storage_backend": "sqlite",
                "pool_min": 0,          # out of range (ge=1)
                "backfill_interval_seconds": 120.0,  # valid value, kept
            }
        )
        self.assertNotEqual(cfg.pool_min, 0, "越界项不得原样生效")
        self.assertEqual(cfg.pool_min, 2, "越界项回退默认值")
        self.assertEqual(
            cfg.backfill_interval_seconds, 120.0, "其余用户配置不得被一并清掉"
        )
        self.assertEqual(cfg.dsn, DSN)

    def test_oo3_reassembly_cancels_old_sweep_task(self):
        """OO3: reassembly must first collect the old sweep coroutine
        (overwriting the handle leaks the old coroutine into the next
        shutdown)."""
        inst, _ = _ready_plugin(mod, cfg=_cfg(private=5))

        async def scenario():
            # Simulate the sweep coroutine left by the previous assembly
            old_sweep = asyncio.create_task(asyncio.sleep(3600))
            inst._retain_sweep_task = old_sweep
            await asyncio.sleep(0)
            await inst._initialize_services(inst._db, inst._config)
            # Must decide before the loop closes: loop teardown cancels
            # leftover tasks, making them indistinguishable then
            return old_sweep.cancelled()

        cancelled = asyncio.run(scenario())
        self.assertTrue(cancelled, "旧巡检协程必须被取消（否则泄漏到下次停机）")
        self.assertIsNotNone(inst._retain_sweep_task, "新巡检协程须正常建立")


    def test_pp1_restore_merge_uses_anchor_order(self):
        """PP1: restore merging must order by content anchor
        (hard-coding "restored rows are older" duplicates content)."""
        inst, _ = _ready_plugin(mod, cfg=_cfg(private=5))
        sid = "napcat:dm:10086"
        base = datetime.now(timezone.utc)
        # Construct a "restored row is actually newer" case: the
        # in-memory pending content is earlier
        older_ts = (base - timedelta(hours=1)).isoformat()
        newer_ts = base.isoformat()

        def _payload(ts, line):
            return {
                "session_id": "10086",
                "platform": "napcat",
                "group_id": "",
                "user_id": "u1",
                "timestamp": ts,
                "first_ts": ts,
                "participants": [["napcat", "u1"]],
                "history_lines": [],
                "batch_lines": [line],
                "bot_lines": [],
                "rounds": 1,
            }

        class _StubConn:
            async def fetch(self, sql, *args):
                return [
                    {
                        "session_key": sid,
                        "payload": json.dumps(_payload(newer_ts, "恢复行")),
                        "updated_at": newer_ts,
                    }
                ]

        class _StubAcquire:
            async def __aenter__(self):
                return _StubConn()

            async def __aexit__(self, *exc):
                return False

        class _StubPool:
            def acquire(self):
                return _StubAcquire()

        async def scenario():
            inst._db = types.SimpleNamespace(pool=_StubPool())
            inst._retain_pending[sid] = inst._retain_state_from_payload(
                _payload(older_ts, "内存行")
            )
            await inst._restore_retain_pending()
            return inst._retain_pending[sid]

        merged = asyncio.run(scenario())
        self.assertEqual(
            merged.batch_lines,
            ["内存行", "恢复行"],
            "较旧批次必须在前（否则历史窗口取到较新批次 → 内容重复）",
        )

    def test_pp2_restore_age_uses_row_timestamp(self):
        """PP2: restore age must be derived from the row timestamp
        (two mismatched clocks make the age look older)."""
        inst, _ = _ready_plugin(mod, cfg=_cfg(private=5))
        sid = "napcat:dm:10086"
        row_ts = datetime.now(timezone.utc) - timedelta(seconds=120)
        payload = {
            "session_id": "10086",
            "platform": "napcat",
            "group_id": "",
            "user_id": "u1",
            "timestamp": datetime.now().isoformat(),
            "participants": [["napcat", "u1"]],
            "history_lines": [],
            "batch_lines": ["行"],
            "bot_lines": [],
            "rounds": 1,
        }

        class _StubConn:
            async def fetch(self, sql, *args):
                return [
                    {
                        "session_key": sid,
                        "payload": json.dumps(payload),
                        "updated_at": row_ts.isoformat(timespec="microseconds"),
                    }
                ]

        class _StubAcquire:
            async def __aenter__(self):
                return _StubConn()

            async def __aexit__(self, *exc):
                return False

        class _StubPool:
            def acquire(self):
                return _StubAcquire()

        async def scenario():
            inst._db = types.SimpleNamespace(pool=_StubPool())
            await inst._restore_retain_pending()
            return inst._retain_pending_at.get(sid)

        restored_at = asyncio.run(scenario())
        self.assertIsNotNone(restored_at)
        age = time.time() - restored_at
        self.assertAlmostEqual(age, 120.0, delta=2.0, msg="龄期须等于行时间戳的年龄")
        # The age must be derived from the PARSED row timestamp (not
        # now - _row_age_seconds: that mixes two clocks plus loop
        # overhead; reading older lets the sweep flush early)
        import inspect

        src = inspect.getsource(mod.NoriflowMemoryPlugin._restore_retain_pending)
        self.assertIn("row_ts.timestamp()", src, "龄期须由行时间戳换算")
        self.assertNotIn("self._row_age_seconds(row)", src, "不得用两套时钟换算龄期")


    def test_qq1_sweep_interval_read_is_guarded(self):
        """QQ1: the sweep-interval read must be inside the try
        (unexpected exceptions must not kill the sweep coroutine)."""
        import inspect

        src = inspect.getsource(mod.NoriflowMemoryPlugin._retain_sweep_loop)
        self.assertIn("interval = max(5.0, min(300.0", src)
        self.assertIn("except asyncio.CancelledError:", src, "取消须原样上抛")
        # The interval read must not be the sleep argument directly
        # (an exception would bypass the try)
        self.assertNotIn(
            "await asyncio.sleep(max(5.0, min(300.0",
            src,
            "间隔读取不得裸在 try 之外（协程会死）",
        )

    def test_qq1_sweep_survives_interval_read_failure(self):
        """QQ1/RR2: when the interval read raises, the sweep CONTINUES
        to the next round (self-healing) instead of leaving the loop.

        The assertion must cover "still running after failure" --
        asserting only a single read would pass even if the loop broke
        after the first round (the vacuous lock the review flagged).
        """
        inst, _ = _ready_plugin(mod, cfg=_cfg(private=5))
        state = {"n": 0}
        original_fallback = mod.RETAIN_SWEEP_FALLBACK_INTERVAL_S
        # Fallback period shortened: after the first read fails, the
        # next round arrives without waiting 300s
        mod.RETAIN_SWEEP_FALLBACK_INTERVAL_S = 0.01

        async def scenario():
            def flaky():
                state["n"] += 1
                if state["n"] == 1:
                    raise RuntimeError("间隔读取失败（测试注入）")
                return 5.0

            inst._retain_flush_idle_s = flaky
            task = asyncio.create_task(inst._retain_sweep_loop())
            for _ in range(300):
                await asyncio.sleep(0.01)
                if state["n"] >= 2:
                    break
            task.cancel()
            try:
                await task
            except asyncio.CancelledError:
                pass
            return state["n"]

        try:
            n = asyncio.run(scenario())
        finally:
            mod.RETAIN_SWEEP_FALLBACK_INTERVAL_S = original_fallback
        self.assertGreaterEqual(n, 2, "间隔读取失败后巡检必须继续下一轮（自愈）")

    def test_rr1_old_sweep_cancelled_before_restore(self):
        """RR1: reassembly must cancel the old sweep FIRST (an old
        sweep running during restore interleaves with it)."""
        inst, _ = _ready_plugin(mod, cfg=_cfg(private=5))
        observed: dict = {}

        async def scenario():
            old_sweep = asyncio.create_task(asyncio.sleep(3600))
            inst._retain_sweep_task = old_sweep
            await asyncio.sleep(0)
            original = inst._restore_retain_pending

            async def checking_restore():
                observed["sweep_cancelled"] = old_sweep.cancelled() or old_sweep.done()
                await original()

            inst._restore_retain_pending = checking_restore
            await inst._initialize_services(inst._db, inst._config)
            return old_sweep

        old_sweep = asyncio.run(scenario())
        self.assertTrue(
            observed.get("sweep_cancelled"),
            "恢复之前必须先取消旧巡检协程（否则旧巡检会与恢复交错，重复编码）",
        )
        self.assertTrue(old_sweep.cancelled() or old_sweep.done())


    def test_ss1_restore_skips_stale_snapshot_of_same_batch(self):
        """SS1: when the table row is an older snapshot of the
        in-memory pending batch, do not re-merge (doubled content and
        rounds)."""
        inst, _ = _ready_plugin(mod, cfg=_cfg(private=5))
        sid = "napcat:dm:10086"
        base = datetime.now(timezone.utc)

        def _payload(ts, line, rounds):
            return {
                "session_id": "10086",
                "platform": "napcat",
                "group_id": "",
                "user_id": "u1",
                "timestamp": ts,
                "first_ts": ts,
                "participants": [["napcat", "u1"]],
                "history_lines": [],
                "batch_lines": [line],
                "bot_lines": [],
                "rounds": rounds,
            }

        ts = base.isoformat()

        class _StubConn:
            async def fetch(self, sql, *args):
                return [
                    {
                        "session_key": sid,
                        # Table row = an older snapshot of the
                        # in-memory pending batch (1 round vs the
                        # memory's 2 rounds)
                        "payload": json.dumps(_payload(ts, "第1句", 1)),
                        "updated_at": ts,
                    }
                ]

        class _StubAcquire:
            async def __aenter__(self):
                return _StubConn()

            async def __aexit__(self, *exc):
                return False

        class _StubPool:
            def acquire(self):
                return _StubAcquire()

        async def scenario():
            inst._db = types.SimpleNamespace(pool=_StubPool())
            # In-memory pending: the same batch but already 2 rounds
            inst._retain_pending[sid] = inst._retain_state_from_payload(
                _payload(ts, "第1句", 2)
            )
            inst._retain_pending[sid].batch_lines = ["第1句", "第2句"]
            await inst._restore_retain_pending()
            return inst._retain_pending[sid]

        merged = asyncio.run(scenario())
        self.assertEqual(
            merged.batch_lines, ["第1句", "第2句"], "旧快照不得重复合并（内容翻倍）"
        )
        self.assertEqual(
            int(getattr(merged, "_retain_rounds", 0)), 2, "轮数不得虚高（会提前编码）"
        )

    def test_ss2_cancelled_encode_batch_is_reseated(self):
        """SS2: a cancelled encode task must reseat under failure
        semantics (otherwise it is never encoded in this process)."""
        inst, _ = _ready_plugin(mod, cfg=_cfg(private=5))
        sid = "napcat:dm:10086"

        async def scenario():
            state = inst._retain_state_from_payload(
                {
                    "session_id": "10086",
                    "platform": "napcat",
                    "group_id": "",
                    "user_id": "u1",
                    "timestamp": datetime.now().isoformat(),
                    "participants": [["napcat", "u1"]],
                    "history_lines": [],
                    "batch_lines": ["在途内容"],
                    "bot_lines": [],
                    "rounds": 1,
                }
            )
            inst._retain_inflight[sid] = [state]
            # A never-ending encode task (simulating a long LLM call)
            task = asyncio.create_task(asyncio.sleep(3600))
            inst._retain_tasks.add(task)
            await inst._stop_retain_backgrounds(task_timeout=0.05)
            return state

        state = asyncio.run(scenario())
        self.assertNotIn(sid, inst._retain_inflight, "在途登记须清空")
        pending = inst._retain_pending.get(sid)
        self.assertIsNotNone(pending, "被取消的批次必须回座到 pending（可重试）")
        self.assertIn("在途内容", "\n".join(pending.batch_lines))
        self.assertTrue(
            getattr(pending, "_retain_failed", False), "回座批次须标记失败（不与新轮次混批）"
        )


    def test_tt1_reassembly_waits_for_old_handler_before_restore(self):
        """TT1: reassembly must first wait for the old-generation
        handler to finish (otherwise it overwrites the just-restored
        data)."""
        inst, ctx = _ready_plugin(mod, cfg=_cfg(private=5))
        dm = FakeSession(sid="10086", stype="dm", adapter="napcat")
        observed: dict = {}
        handler_done: dict = {}
        release = asyncio.Event()

        async def scenario():
            # Simulate an old-generation handler stuck in await (not
            # finished yet)
            async def slow_handler(event):
                await release.wait()
                handler_done["v"] = True

            inst._on_session_memory_updated_impl = slow_handler
            handler = asyncio.create_task(
                ctx.event_bus.fire("session_memory_updated", {"session": dm.sid})
            )
            await asyncio.sleep(0)
            self.assertEqual(inst._retain_handler_active, 1, "旧 handler 应在途")

            original = inst._restore_retain_pending

            async def checking_restore():
                # The criterion uses the handler's ACTUAL completion,
                # not the count (assembly resets the count, and the old
                # implementation would equally "look" zeroed ->
                # false positive)
                observed["old_handler_done"] = bool(handler_done.get("v"))
                await original()

            inst._restore_retain_pending = checking_restore
            # Assembly waits for the old handler (up to 5s) -> release
            # the old handler meanwhile
            task = asyncio.create_task(
                inst._initialize_services(inst._db, inst._config)
            )
            await asyncio.sleep(0.1)
            release.set()
            await asyncio.gather(handler, task, return_exceptions=True)
            return observed

        observed = asyncio.run(scenario())
        self.assertTrue(
            observed.get("old_handler_done"),
            "恢复之前必须等旧代 handler 收尾（否则它会覆盖刚恢复的数据）",
        )

    def test_tt2_reseat_failure_does_not_escape_task(self):
        """TT2: a failure-reseat exception must not escape the task
        (it would become a task exception nobody retrieves)."""
        inst, ctx = _ready_plugin(mod, cfg=_cfg(private=5))
        dm = FakeSession(sid="10086", stype="dm", adapter="napcat")
        kernel = inst._memory_kernel

        async def failing_encode(**kwargs):
            raise RuntimeError("编码失败（测试注入）")

        async def failing_reseat(sid, state):
            raise TypeError("回座失败（测试注入）")

        kernel.retain_encoded = failing_encode
        inst._reseat_pending_batch = failing_reseat

        async def scenario():
            results = []
            # Only 5 turns (= interval) flush and actually hit the
            # encode failure -> reseat path
            for i in range(5):
                await _turn(inst, ctx, dm, f"第{i + 1}句", mid=f"tt2{i + 1}")
                results += await asyncio.gather(
                    *inst._retain_tasks, return_exceptions=True
                )
            results += await asyncio.gather(
                *inst._retain_tasks, return_exceptions=True
            )
            return results

        results = asyncio.run(scenario())
        self.assertTrue(
            results, "用例必须真的触发编码任务（否则断言空跑）"
        )
        self.assertFalse(
            [r for r in results if isinstance(r, BaseException)],
            "失败回座异常不得冒出编码任务（否则是没人 retrieve 的任务异常）",
        )

    def test_tt3_merge_copies_history_lines(self):
        """TT3: merging must copy history lines (mutating the old state
        in place must not affect the merged batch)."""
        inst, _ = _ready_plugin(mod, cfg=_cfg(private=5))
        base = {
            "session_id": "10086",
            "platform": "napcat",
            "group_id": "",
            "user_id": "u1",
            "timestamp": datetime.now().isoformat(),
            "participants": [["napcat", "u1"]],
            "history_lines": ["历史行1"],
            "batch_lines": ["行1"],
            "bot_lines": [],
            "rounds": 1,
        }
        older = inst._retain_state_from_payload(base)
        newer = inst._retain_state_from_payload({**base, "batch_lines": ["行2"]})
        merged = inst._merge_retain_states(older, newer)
        older.history_lines.append("事后追加")
        self.assertEqual(
            merged.history_lines,
            ["历史行1"],
            "合并后的历史窗口必须与旧批次解耦（不得被原地修改牵连）",
        )


class TestMultiReviewFixes(unittest.TestCase):
    def test_concurrent_alias_wait_preserves_batch_order(self):
        inst, ctx = _ready_plugin(mod, cfg=_cfg(private=5))
        dm = FakeSession(sid="10086", stype="dm", adapter="napcat")
        captured = []

        async def scenario():
            entered, release = asyncio.Event(), asyncio.Event()
            calls = 0

            async def alias(delta):
                nonlocal calls
                calls += 1
                if calls == 1:
                    entered.set()
                    await release.wait()

            inst._alias_upsert_batch = alias
            first = asyncio.create_task(_turn(inst, ctx, dm, "先到第一句", "order1"))
            await entered.wait()
            second = asyncio.create_task(_turn(inst, ctx, dm, "后到第二句", "order2"))
            await asyncio.sleep(0)
            release.set()
            await asyncio.gather(first, second)
            pending = inst._retain_pending[dm.sid]
            self.assertIn("先到第一句", pending.batch_lines[0])
            self.assertIn("后到第二句", pending.batch_lines[1])
            self.assertFalse(pending.history_lines)
            _spy_encode(inst, captured)
            inst._flush_pending_batch(dm.sid)
            await asyncio.gather(*inst._retain_tasks)

        asyncio.run(scenario())
        self.assertEqual(captured[0].count("先到第一句"), 1)
        self.assertEqual(captured[0].count("后到第二句"), 1)

    def test_group_batch_keeps_other_members_after_erase(self):
        """Erasing one member redacts only that member's lines; the
        other members' unencoded content in the same batch must not be
        lost."""
        inst, _ = _ready_plugin(mod, cfg=_cfg(group=3))
        sid = "napcat:gm:778899"
        line_u1 = mod.format_history_message(
            speaker_name="u1", content="u1 的私有发言", user_id="u1"
        )
        line_u2 = mod.format_history_message(
            speaker_name="u2", content="u2 的正常发言", user_id="u2"
        )
        state = inst._retain_state_from_payload({
            "session_id": "778899", "platform": "napcat", "group_id": "778899",
            "user_id": "u1", "timestamp": datetime.now().isoformat(),
            "participants": [["napcat", "u1"], ["napcat", "u2"]],
            "history_lines": [], "batch_lines": [line_u1, line_u2],
            "bot_lines": [], "rounds": 2,
        })
        inst._retain_pending[sid] = state

        async def scenario():
            await inst._erase_retain_for_user("napcat", "u1")

        asyncio.run(scenario())
        kept = inst._retain_pending.get(sid)
        self.assertIsNotNone(kept, "其他成员的批次必须保留")
        text = "\n".join(kept.batch_lines)
        self.assertNotIn("u1 的私有发言", text)
        self.assertIn("u2 的正常发言", text)
        self.assertEqual(kept.user_id, "u2")
        self.assertNotIn(("napcat", "u1"), [tuple(p) for p in kept.participants])

    def test_user_erase_group_inflight_leaves_no_orphan_copy(self):
        """Group in-flight batch erasure redaction: the redacted copy
        reseats only into pending, leaving no orphan in-flight
        registration.

        Regression: the original implementation put the redacted copy
        into both kept (written back to _retain_inflight) and reseat
        (merged into pending). The in-flight-side copy has no owning
        task (the original task was cancelled, and CancelledError does
        not go through the _do_retain cleanup branch) while
        _drop_inflight only recognizes the original state object --
        the orphan copy can never be removed: _pending_snapshot_for_row
        merges it with pending into the snapshot, the table-row
        conditional delete is permanently false (the row never
        deletes), and restart restore would encode the already-encoded
        content again.
        """
        inst, _ = _ready_plugin(mod, cfg=_cfg(group=3))
        sid = "napcat:gm:778899"
        line_u1 = mod.format_history_message(
            speaker_name="u1", content="u1 的机密", user_id="u1"
        )
        line_u2 = mod.format_history_message(
            speaker_name="u2", content="u2 的正常发言", user_id="u2"
        )
        state = inst._retain_state_from_payload({
            "session_id": "778899", "platform": "napcat", "group_id": "778899",
            "user_id": "u1", "timestamp": datetime.now().isoformat(),
            "participants": [["napcat", "u1"], ["napcat", "u2"]],
            "history_lines": [], "batch_lines": [line_u1, line_u2],
            "bot_lines": [], "rounds": 2,
        })
        inst._retain_inflight[sid] = [state]

        async def scenario():
            await inst._erase_retain_for_user("napcat", "u1")

        asyncio.run(scenario())
        # The redacted copy must not remain in the in-flight registry
        # (an orphan with no owning task can never be removed)
        self.assertNotIn(sid, inst._retain_inflight)
        # Other members' content reaches pending via reseat (with the
        # failure-reseat flag, retrying at the next trigger point)
        pending = inst._retain_pending.get(sid)
        self.assertIsNotNone(pending, "其他成员的批次必须回座 pending")
        text = "\n".join(pending.batch_lines)
        self.assertNotIn("u1 的机密", text)
        self.assertIn("u2 的正常发言", text)
        self.assertNotIn(("napcat", "u1"), [tuple(p) for p in pending.participants])

    def test_erase_during_alias_wait_drops_stale_turn(self):
        """Erasure during the alias await: the stale turn must not
        enter pending."""
        inst, ctx = _ready_plugin(mod, cfg=_cfg(private=5))
        dm = FakeSession(sid="10086", stype="dm", adapter="napcat")

        async def scenario():
            entered, release = asyncio.Event(), asyncio.Event()

            async def alias(delta):
                entered.set()
                await release.wait()

            inst._alias_upsert_batch = alias
            turn = asyncio.create_task(_turn(inst, ctx, dm, "擦除前发出的一轮", "stale1"))
            await entered.wait()
            await inst._erase_retain_for_user("napcat", "u1")
            release.set()
            await turn
            self.assertNotIn(dm.sid, inst._retain_pending)

        asyncio.run(scenario())

    def test_pending_payload_nul_is_jsonb_safe_without_mutating_live_state(self):
        inst, _ = _ready_plugin(mod, cfg=_cfg())
        state = inst._retain_state_from_payload({
            "session_id": "10086", "platform": "napcat", "group_id": "",
            "user_id": "u1", "timestamp": datetime.now().isoformat(),
            "participants": [["napcat", "u1"]], "history_lines": ["history\x00"],
            "batch_lines": ["body\x00"], "bot_lines": ["bot\x00"], "rounds": 1,
        })
        encoded = json.dumps(inst._retain_payload(state))
        self.assertNotIn("\\u0000", encoded)
        self.assertEqual(state.batch_lines, ["body\x00"])

    def test_successful_batch_delete_failure_is_retried_by_sweep(self):
        inst, ctx = _ready_plugin(mod, cfg=_cfg(private=2))
        dm = FakeSession(sid="10086", stype="dm", adapter="napcat")

        async def scenario():
            original = inst._delete_retain_pending
            calls = 0

            async def flaky(sid, cutoff=None):
                nonlocal calls
                calls += 1
                if calls == 1:
                    raise RuntimeError("temporary delete failure")
                await original(sid, cutoff)

            inst._delete_retain_pending = flaky
            for i in range(2):
                await _turn(inst, ctx, dm, f"delete retry {i}", f"dr{i}")
                await asyncio.gather(*inst._retain_tasks)
            self.assertIn(dm.sid, inst._retain_delete_retry)
            await inst._sweep_retain_pending()
            self.assertNotIn(dm.sid, inst._retain_delete_retry)
            self.assertGreaterEqual(calls, 2)

        asyncio.run(scenario())

    def test_user_erase_cancels_inflight_and_removes_pending_backup(self):
        import tempfile
        inst, ctx = _ready_plugin(mod, cfg=_cfg(private=5))
        dm = FakeSession(sid="10086", stype="dm", adapter="napcat")

        async def scenario(path):
            config = mod.LocalMemoryConfig(storage_backend="sqlite", sqlite_path=path, embedding_dims=4)
            db = mod.SQLiteMemoryDatabase(config)
            await db.connect()
            await db.apply_migrations(Path(__file__).resolve().parent.parent / "migrations_sqlite")
            inst._db, inst._backend_name = db, "sqlite"
            encoded, cancelled = asyncio.Event(), asyncio.Event()

            async def slow_encode(**kwargs):
                encoded.set()
                try:
                    await asyncio.Event().wait()
                finally:
                    cancelled.set()

            inst._memory_kernel.retain_encoded = slow_encode
            try:
                await _turn(inst, ctx, dm, "erase pending", "erase1")
                await asyncio.gather(*inst._retain_tasks)
                inst._flush_pending_batch(dm.sid)
                await encoded.wait()
                response = await inst.api_memory_user_delete("napcat", "u1")
                self.assertTrue(response["deleted"])
                self.assertTrue(cancelled.is_set())
                self.assertNotIn(dm.sid, inst._retain_pending)
                self.assertNotIn(dm.sid, inst._retain_inflight)
                async with db.pool.acquire() as conn:
                    rows = await conn.fetch("SELECT payload FROM memory_retain_pending")
                self.assertEqual(rows, [])
                await inst._restore_retain_pending()
                self.assertNotIn(dm.sid, inst._retain_pending)
                # Erasure invalidates old work, not future conversations.
                await _turn(inst, ctx, dm, "new after erase", "erase2")
                await asyncio.gather(*inst._retain_tasks)
                self.assertIn(dm.sid, inst._retain_pending)
            finally:
                await db.close()

        with tempfile.TemporaryDirectory() as tmp:
            asyncio.run(scenario(str(Path(tmp) / "memory.sqlite3")))


if __name__ == "__main__":
    unittest.main()
