"""retain 攒批（私聊 N 轮合并编码）专项测试。

覆盖与 nori 仓 PR #25 对齐的语义（kira 架构适配版）：

- 攒批门控：群聊 interval=1 逐轮路径零变化（不写 pending 表、失败回滚
  水位线）；私聊 interval=N 攒批到点合并为单批次编码
- 批次内容正确性：合并批次 = 各轮批次行拼接，历史窗口取攒批前快照
- 双持久化：每轮 UPSERT 快照行、编码成功后删行
- 失败回座：编码失败不丢批次（内存 + 表行都保留），下个触发点重试
- 水位线语义：攒批路径不回滚水位线（真源已移交攒批缓存/表行）
- 重启恢复：读表回灌轮数延续，回灌不立即编码
- 空闲巡检：超龄批次强制编码落库
- 会话逐出联动：LRU 逐出带攒批的会话时调度编码，不滞留
- 停机收口：terminate 强制编码未满批

运行：python -m pytest -q tests/test_memory_retain_batch.py
"""

from __future__ import annotations

import asyncio
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
    """一轮对话：观察消息 -> 发出回合完成信号。"""
    await inst.observe_message(
        types.SimpleNamespace(message=make_msg("u1", text, mid=mid), session=session)
    )
    await ctx.event_bus.fire("session_memory_updated", {"session": session.sid})


def _spy_encode(inst, sink: list) -> None:
    """在 kernel 边界抓取编码输入（conversation_text）。"""
    kernel = inst._memory_kernel
    original = kernel.retain_encoded

    async def spy(**kwargs):
        sink.append(kwargs.get("conversation_text", ""))
        return await original(**kwargs)

    kernel.retain_encoded = spy


def _pending_sqls(inst) -> list:
    """pending 表相关 SQL 调用（INSERT / DELETE）。"""
    return [
        sql for kind, sql, _args in inst._db.pool.conn.sqls
        if kind == "execute" and "memory_retain_pending" in sql
    ]


class TestRetainBatchGating(unittest.TestCase):
    """门控：谁攒批、谁逐轮。"""

    def test_private_batches_five_turns_into_one_encode(self):
        """私聊 5 轮合并为 1 次编码，批次含全部 5 轮内容。"""
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
        """群聊默认 interval=1：逐轮编码，且不写 pending 表（行为零变化）。"""
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
        """私聊 interval=1 时同样走逐轮路径（回滚开关）。"""
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
        """第二批的历史窗口 = 攒批前的已消费行（等价 nori 的 exclude 回退）。

        场景：前 5 轮合并编码为第 1 批；第 6 轮开第 2 批，其历史窗口
        应含前 5 轮已消费的行（不重复进批次内容）。
        """
        inst, ctx = _ready_plugin(mod, cfg=_cfg(private=5))
        dm = FakeSession(sid="10086", stype="dm", adapter="napcat")
        captured: list[str] = []

        async def scenario():
            _spy_encode(inst, captured)
            for i in range(5):
                await _turn(inst, ctx, dm, f"第{i + 1}句", mid=f"b{i + 1}")
                await asyncio.gather(*inst._retain_tasks)
            # 第 6 轮开新批：历史窗口应回退到前 5 轮
            await _turn(inst, ctx, dm, "第6句", mid="b6")
            await asyncio.gather(*inst._retain_tasks)
            # 强制冲刷第 2 批（巡检超龄）
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
    """双持久化：表行快照的生命周期。"""

    def test_row_written_each_turn_and_deleted_after_encode(self):
        """每轮 UPSERT 快照行，编码成功后删行。"""
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
        """编码失败：批次回座（内存 + 表行保留），后续触发点重试成功。"""
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
        # 失败回座：内存仍持有该批次，轮数保留
        pending = inst._retain_pending.get(sid)
        self.assertIsNotNone(pending, "失败批次必须回座到内存 pending")
        self.assertEqual(int(getattr(pending, "_retain_rounds", 0)), 5)
        # 表行仍在（未删）
        self.assertFalse(
            [s for s in _pending_sqls(inst) if s.lstrip().startswith("DELETE")],
            "失败时不得删行",
        )
        # 下一轮触发重试并成功
        state["fail"] = False

        async def retry():
            await _turn(inst, ctx, dm, "重试一轮", mid="retry1")
            await asyncio.gather(*inst._retain_tasks)
            await asyncio.gather(*inst._retain_tasks)
            return inst._db.inserted_summaries

        summaries = asyncio.run(retry())
        self.assertEqual(len(summaries), 1, "回座批次应在下一触发点重试编码")
        self.assertIsNone(inst._retain_pending.get(sid), "成功后清空内存 pending")

    def test_batch_failure_does_not_roll_back_watermark(self):
        """攒批路径失败不回滚水位线（真源已移交攒批缓存/表行）。"""
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
    """重启恢复 / 巡检 / 逐出联动。"""

    def test_restore_continues_round_count_without_immediate_encode(self):
        """读表回灌：轮数延续，且回灌不立即编码（续攒而非提前落库）。"""
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
        """回灌后继续攒到间隔即编码。"""
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
        """巡检：超龄批次强制编码落库。"""
        inst, ctx = _ready_plugin(mod, cfg=_cfg(private=5, flush_idle_s=60))
        dm = FakeSession(sid="10086", stype="dm", adapter="napcat")
        captured: list[str] = []

        async def scenario():
            _spy_encode(inst, captured)
            for i in range(2):
                await _turn(inst, ctx, dm, f"未满批第{i + 1}句", mid=f"i{i + 1}")
                await asyncio.gather(*inst._retain_tasks)
            # 伪造超龄：最后追加时间回拨 10 分钟
            inst._retain_pending_at[dm.sid] = time.time() - 600
            await inst._sweep_retain_pending()
            await asyncio.gather(*inst._retain_tasks)

        asyncio.run(scenario())
        self.assertEqual(len(captured), 1, "超龄批次应被巡检强制编码")

    def test_sweep_keeps_fresh_batch(self):
        """巡检：未超龄批次不动。"""
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
        """会话逐出联动：带攒批的会话被逐出时调度编码，不滞留。"""
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
        """停机收口：未满批在 terminate 时强制编码。"""
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


import json  # noqa: E402  (置于末尾以保持文件头导入整洁)


class TestRetainBatchSQLiteBackend(unittest.TestCase):
    """生产路径（SQLite 后端）真实往返：迁移 013 + pending 表读写。

    10.0.1.91 实机生产配置 storage_backend=sqlite，本组用例是发布阻塞项：
    迁移可应用、UPSERT/SELECT/DELETE 在 sqlite shim 下可往返。
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

                # 回灌读回
                inst._retain_pending.clear()
                await inst._restore_retain_pending()
                restored = inst._retain_pending.get(sid)
                self.assertIsNotNone(restored, "回灌应恢复 pending")
                self.assertEqual(int(getattr(restored, "_retain_rounds", 0)), 2)

                # 删行（cutoff = 本行写入时间戳；无 cutoff 时按 P2 语义跳过）
                await inst._delete_retain_pending(sid, inst._retain_row_ts.get(sid))
                async with db.pool.acquire() as conn:
                    rows = await conn.fetch(
                        "SELECT session_key FROM memory_retain_pending"
                    )
                self.assertEqual(len(rows), 0, "删除后不应残留行")
                await db.close()

            asyncio.run(scenario())


class TestRetainBatchReviewFixes(unittest.TestCase):
    """pr-agent review 修复批（PR #16）的回归锁。"""

    def test_f4_first_batch_history_is_not_duplicated(self):
        """F4：首轮批次的历史窗口合法为空时，不得回退到新批历史（内容重复）。"""
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
        """F1：terminate 必须清空在途批次登记（取消任务不经过 _drop_inflight）。"""
        inst, ctx = _ready_plugin(mod, cfg=_cfg(private=5))
        dm = FakeSession(sid="10086", stype="dm", adapter="napcat")

        async def scenario():
            await _turn(inst, ctx, dm, "在途批次", mid="inflight1")
            # 模拟「已弹出并登记在途」的编码中状态
            inst._retain_inflight[dm.sid] = [inst._retain_pending[dm.sid]]
            await inst.terminate()

        asyncio.run(scenario())
        self.assertEqual(inst._retain_inflight, {}, "terminate 后不得残留在途登记")
        self.assertEqual(inst._retain_pending, {})

    def test_f2_kernel_missing_reseats_batch(self):
        """F2：kernel 为空时攒批路径须回座（否则这批运行期不再被编码）。"""
        inst, ctx = _ready_plugin(mod, cfg=_cfg(private=5))
        dm = FakeSession(sid="10086", stype="dm", adapter="napcat")

        async def scenario():
            # 4 轮未满批：批次仍在 pending
            for i in range(4):
                await _turn(inst, ctx, dm, f"第{i + 1}句", mid=f"k{i + 1}")
                await asyncio.gather(*inst._retain_tasks)
            await asyncio.gather(*inst._retain_tasks)
            # kernel 缺席时到点 flush：批次弹出→在途→编码失败→必须回座
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
        """F3：条件删除只删不晚于 cutoff 的行（并发新回合的更新行不被抹掉）。"""
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

                # 模拟并发新回合写入了更新的行
                inst._retain_pending[sid] = _state("新批次行")
                await inst._save_retain_pending(sid)

                # 以旧 cutoff 删行：更新行必须存活
                await inst._delete_retain_pending(sid, old_cutoff)
                async with db.pool.acquire() as conn:
                    rows = await conn.fetch(
                        "SELECT session_key FROM memory_retain_pending WHERE session_key=$1",
                        sid,
                    )
                self.assertEqual(len(rows), 1, "更晚写入的行不应被旧 cutoff 删除")

                # 以最新 cutoff 删行：本次写入的行应被删除
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
        """R1：成功路径删行失败不得让编码任务带异常结束（DB 抖动兜底）。"""
        inst, ctx = _ready_plugin(mod, cfg=_cfg(private=5))
        dm = FakeSession(sid="10086", stype="dm", adapter="napcat")
        original_delete = inst._delete_retain_pending

        async def boom(sid, cutoff=None):
            raise RuntimeError("删行失败（测试注入）")

        inst._delete_retain_pending = boom

        async def scenario():
            for i in range(5):
                await _turn(inst, ctx, dm, f"第{i + 1}句", mid=f"r{i + 1}")
                await asyncio.gather(*inst._retain_tasks)
            await asyncio.gather(*inst._retain_tasks)
            return [t for t in inst._retain_tasks]

        asyncio.run(scenario())
        # 编码本身成功（表行仍在，属预期：删不掉留给下次刷新/重启覆盖）
        self.assertEqual(len(inst._db.inserted_summaries), 1, "编码仍应成功")
        inst._delete_retain_pending = original_delete

    def test_i3_cutoff_kept_when_conditional_delete_misses(self):
        """I3：条件删除未命中（更新行仍在）时不得丢弃 cutoff。"""
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
            # 更晚写入的行（并发新回合）
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
            # 以旧 cutoff 删：未命中更新行 → cutoff 必须保留
            await inst._delete_retain_pending(sid, old_cutoff)
            self.assertEqual(
                inst._retain_row_ts.get(sid),
                new_cutoff,
                "条件删除未命中时不得丢弃 cutoff（否则下次删除退回 now，抹掉新批次备份）",
            )

        asyncio.run(scenario())


    def test_n1_older_snapshot_does_not_overwrite_newer_row(self):
        """N1：UPSERT 时间戳守卫——更旧的快照不得覆盖更新的行。"""
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
                # 模拟乱序：更旧的快照后到（时间戳更早）
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

    def test_n2_stopping_blocks_new_writes_and_tasks(self):
        """N2：停机标志置位后不得再写 pending 表或派发新任务。"""
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
        # 停机前的残留批次仍可在（合法状态），但停机后那一轮不得被并入
        pending = inst._retain_pending.get(dm.sid)
        if pending is not None:
            self.assertNotIn(
                "停机后一轮",
                "\n".join(pending.batch_lines),
                "停机后不得再攒批",
            )
            self.assertEqual(int(getattr(pending, "_retain_rounds", 0)), 1)
        inserts = [
            s for s in _pending_sqls(inst) if s.lstrip().startswith("INSERT")
        ]
        self.assertLessEqual(len(inserts), 1, "停机后不得再写 pending 表")

    def test_n3_interval_switch_flushes_residual_batch(self):
        """N3：interval 调小（热切换）时，残留批次应先冲刷再走逐轮路径。"""
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
            # 热切换：interval 5 -> 1（宿主配置页就地改同一 config 实例）
            inst._config.retain_interval_private = 1
            await _turn(inst, ctx, dm, "切换后一轮", mid="sw1")
            await asyncio.gather(*inst._retain_tasks)
            await asyncio.gather(*inst._retain_tasks)

        asyncio.run(scenario())
        self.assertEqual(len(captured), 2, "残留批次应被冲刷 + 逐轮编码一次")
        self.assertIn("残留第1句", captured[0], "冲刷的应是残留批次")
        self.assertIn("切换后一轮", captured[1], "逐轮路径编码本轮")
        self.assertNotIn(dm.sid, inst._retain_pending, "冲刷后不得残留 pending")


    def test_p1_snapshot_write_does_not_block_turn_signal(self):
        """P1：快照写入不得阻塞回合完成信号（DB 变慢时主链路不被拖住）。"""
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
            started = time.perf_counter()
            await ctx.event_bus.fire("session_memory_updated", {"session": dm.sid})
            elapsed = time.perf_counter() - started
            slow["release"].set()
            await asyncio.gather(*inst._retain_tasks)
            return elapsed

        elapsed = asyncio.run(scenario())
        self.assertLess(
            elapsed, 0.2, "回合完成信号不应等待快照写入（快照走后台任务）"
        )

    def test_p2_delete_without_cutoff_keeps_row(self):
        """P2：缺少行时间戳时跳过删除（留行比误删新快照安全）。"""
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
            # 无 cutoff：不得删除
            await inst._delete_retain_pending(sid)
            self.assertEqual(
                inst._retain_row_ts.get(sid) is not None,
                True,
                "跳过删除时不得清掉行时间戳",
            )
            self.assertIsNotNone(inst._retain_pending.get(sid))

        asyncio.run(scenario())


if __name__ == "__main__":
    unittest.main()
