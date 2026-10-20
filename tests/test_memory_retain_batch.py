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

    def test_n2_stopping_blocks_new_tasks_but_keeps_round(self):
        """N2/U1：停机标志置位后不得派发新任务，但本轮内容不得被静默吞掉。"""
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
        # 停机后那一轮的增量已入水位线——必须被保留（内存 pending / 表行），
        # 不能因为门控直接 return 就静默丢弃
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
        """N3/X1：interval 调小（热切换）时，残留批次与本轮**合并为一批**编码。

        残留内容不丢，且不与本轮的 history 窗口重复——本轮 state 的 history
        是用 _history_snapshot 生成的，其中已含残留批次的行；若残留单独
        异步编码，同一段对话会被编码两次。
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
            # 热切换：interval 5 -> 1（宿主配置页就地改同一 config 实例）
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
        """X2：等待连接期间编码完成 → 保存不得把已编码内容写成残留行。"""
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
                # 等待连接期间编码完成：未编码数据清空
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
            # 超时兜底：若信号真的 await 了快照写入，这里会超时而不是挂死
            try:
                await asyncio.wait_for(
                    ctx.event_bus.fire("session_memory_updated", {"session": dm.sid}),
                    timeout=2.0,
                )
            except asyncio.TimeoutError:
                raise AssertionError(
                    "回合完成信号不得等待快照写入（快照应走后台任务）"
                ) from None
            # 判据：信号返回时快照写入任务**仍未完成** → 信号没有等它
            # （不能只看 release 未置位：那只在 fire 返回后才置位，恒真）
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


    def test_q1_restore_precedes_signal_subscription(self):
        """Q1：快照恢复必须先于回合完成信号订阅（恢复期间的回合不被覆盖）。"""
        ctx = FakeCtx()
        observed: list[int] = []
        original = mod.NoriflowMemoryPlugin._restore_retain_pending

        async def observing_restore(self):
            # 恢复进行中：此时信号处理器应尚未订阅
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
        """Q1：恢复不得覆盖内存里已有的批次（恢复期间到达的回合已开新批）。"""
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
            # 模拟恢复期间到达的回合已开新批
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
        """S1：较旧批次失败回座后，表行快照按内容锚点顺序合并（历史窗口取最早批次）。"""
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

        # 较旧批次 A（其历史窗口 = 前置上下文）与较新批次 C（历史窗口已含 A 的行）
        older = _mk(0, ["前置上下文"], ["A1", "A2"], 2)
        newer = _mk(10, ["前置上下文", "A1", "A2"], ["C1", "C2"], 2)

        async def scenario():
            # 在途：A（较旧）与 C（较新）同时在编码
            inst._retain_inflight[sid] = [older, newer]
            # A 失败回座：进 pending 槽（pending 槽本是「最新批次」位）
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
        """S1：回座与已有 pending 合并时按内容锚点定先后（较旧在前）。"""
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

        # pending 槽里是较旧批次（同样来自失败回座），回座批次更新：
        # 旧实现无条件把回座批次当作 old → 顺序倒置 + 历史窗口取错
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
        """T1：失败标记必须落在真正留在 pending 槽的对象上（合并会生成新对象）。"""
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
            # pending 槽已有较新批次；失败批次回座会与之合并（生成新对象）
            inst._retain_pending[sid] = pending
            await inst._reseat_pending_batch(sid, failed)
            kept = inst._retain_pending[sid]
            self.assertTrue(
                getattr(kept, "_retain_failed", False),
                "失败标记必须落在留在 pending 槽的对象上",
            )
            # 下一轮触发：失败批次不得与新轮次混批
            await _turn(inst, ctx, dm, "新轮次内容", mid="t1n")
            await asyncio.gather(*inst._retain_tasks)
            await asyncio.gather(*inst._retain_tasks)

        asyncio.run(scenario())
        self.assertEqual(len(captured), 1, "失败批次应单独编码一次")
        self.assertIn("失败批内容", captured[0])
        self.assertIn("新批次内容", captured[0], "合并后的批次内容应一起编码")
        self.assertNotIn("新轮次内容", captured[0], "新轮次不得混进失败批次")

    def test_t3_out_of_range_config_is_clamped(self):
        """T3：越界配置值须钳到合法区间（否则 Pydantic 校验失败→记忆整体下线）。"""
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
        """T2：_do_retain 的 durable 默认值须为 False（未显式传参走改动前语义）。"""
        import inspect

        sig = inspect.signature(mod.NoriflowMemoryPlugin._do_retain)
        self.assertIs(sig.parameters["durable"].default, False)


    def test_w1_restore_failure_triggers_retry_before_save(self):
        """W1：恢复失败后必须在保存前补恢复，否则新快照 UPSERT 覆盖旧行（永久丢失）。"""
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
            # 首次（初始化期）恢复失败 → 标记待补恢复
            inst._retain_restore_failed = True
            restored = _restored_state()

            async def fake_restore():
                calls["n"] += 1
                if calls["n"] == 1:
                    raise RuntimeError("恢复失败（测试注入）")
                inst._retain_pending[sid] = restored
                inst._retain_restore_failed = False

            inst._restore_retain_pending = fake_restore
            # 第一次保存：补恢复仍失败 → 跳过写表（不覆盖未回灌的旧行）
            await inst._save_retain_pending(sid)
            first_inserts = len(
                [s for s in _pending_sqls(inst) if s.lstrip().startswith("INSERT")]
            )
            # 第二次保存（DB 恢复后）：补恢复成功 → 合并落表
            await inst._save_retain_pending(sid)
            return calls["n"], first_inserts

        calls_n, first_inserts = asyncio.run(scenario())
        self.assertGreaterEqual(calls_n, 2, "保存前必须补恢复")
        self.assertEqual(first_inserts, 0, "补恢复仍失败时不得写表行")
        # 补回来的崩溃备份内容必须进了表行（不被新快照覆盖丢失）
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
        """W1：补恢复仍失败时不得写表行（否则覆盖未回灌的崩溃备份行→旧内容永久丢失）。"""
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
        """V1：装配期无条件复位停机标志（terminate 中途异常不得让标志粘住）。"""
        inst, _ = _ready_plugin(mod, cfg=_cfg(private=5))
        inst._retain_stopping = True
        asyncio.run(inst._initialize_services(inst._db, inst._config))
        self.assertFalse(inst._retain_stopping, "装配期必须复位停机标志")


    def test_y1_concurrent_restore_retry_is_serialized(self):
        """Y1：并发补恢复必须串行（否则同一批表行被合并两遍：内容翻倍 + 轮数虚高）。"""
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
        """Y2：昵称刷新失败不得让 durable 批次的在途登记泄漏（批次须回座）。

        编码同时失败（注入），确保走的是**失败回座**分支——否则用例只会
        走成功路径（_drop_inflight），锁不到「异常逃逸导致在途登记泄漏」。
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
        """Y3：半途失败的补恢复不得把同一行再合并一遍（内容翻倍 + 轮数虚高）。"""
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
            # 补恢复再跑一次（半途失败后的重试路径）
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
        """Z1：耗时观测超过阈值须升级 WARNING（慢查询哨兵，与上游 nori 口径一致）。"""
        import logging

        inst, _ = _ready_plugin(mod, cfg=_cfg(private=5))
        kernel = inst._memory_kernel
        mk_mod = sys.modules["noriflow_memory_pkg.memory_kernel"]
        original_impl = kernel._build_injection_text_impl

        async def slow_impl(**kwargs):
            await asyncio.sleep(0.01)
            return "注入文本"

        kernel._build_injection_text_impl = slow_impl
        # 阈值降到 1ms，使 10ms 的调用必然越线（不依赖机器速度）
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
        """Z2：Y2 回归锁必须真正覆盖失败回座分支（否则锁是空的）。"""
        import inspect

        src = inspect.getsource(
            TestRetainBatchReviewFixes.test_y2_nickname_refresh_failure_does_not_leak_inflight
        )
        # 回座分支的触发条件：编码必须失败（否则走成功路径的 _drop_inflight）
        self.assertIn("failing_encode", src)
        self.assertIn("retain_encoded = failing_encode", src)


    def test_x3_merged_residual_batch_is_backed_up_to_table(self):
        """X3：interval<=1 的残留合并路径必须先落表行（编码期间崩溃不丢本轮内容）。"""
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
        """AA1：脏表行的非字符串元素不得毒死链路（join 会抛 TypeError）。"""
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
        # join 不应抛
        self.assertIsInstance("\n".join(state.history_lines), str)

    def test_aa2_restore_keeps_newer_pending_age(self):
        """AA2：回灌不得用旧行龄期覆盖内存里更新的批次龄期（会被巡检提前冲刷）。"""
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
        """AA3：残留批次出 pending 槽时同步清掉龄期条目（不留悬空键）。"""
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
        """BB1：成功路径的快照合并异常不得冒出任务，且须保守保留表行。"""
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
            # 不抛异常（异常会变成 "Task exception was never retrieved"）
            await inst._do_retain(inst._retain_pending[sid], sid, set(), True)

        asyncio.run(scenario())
        deletes = [
            s for s in _pending_sqls(inst) if s.lstrip().startswith("DELETE")
        ]
        self.assertEqual(deletes, [], "合并失败时不得删表行（删了会丢未编码内容）")

    def test_bb2_dirty_rounds_is_clamped(self):
        """BB2：脏行的 0/负数轮数须钳到 >=1（不改变攒批节奏）。"""
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
        """BB3：naive 与 aware 锚点混用时比较/排序不得抛 TypeError。"""
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
        """CC1：回灌不得把已登记的较新行时间戳拉回（cutoff 回退会删不掉更新行）。"""
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
            # 并发保存已登记的较新时间戳
            inst._retain_row_ts[sid] = newer
            await inst._restore_retain_pending()
            return inst._retain_row_ts.get(sid)

        after = asyncio.run(scenario())
        self.assertEqual(
            after, newer, "回灌不得把较新的行时间戳拉回（cutoff 只前进不后退）"
        )

    def test_cc2_flush_idle_zero_uses_same_clamp_as_build(self):
        """CC2：热切换 flush_idle_s=0 时，巡检阈值口径须与装配期一致（1.0 而非 1800）。"""
        inst, _ = _ready_plugin(mod, cfg=_cfg(private=5, flush_idle_s=1800.0))
        inst._config.flush_idle_s = 0
        self.assertEqual(inst._retain_flush_idle_s(), 1.0)
        inst._config.flush_idle_s = -5
        self.assertEqual(inst._retain_flush_idle_s(), 1.0)
        inst._config.flush_idle_s = 60
        self.assertEqual(inst._retain_flush_idle_s(), 60.0)

    def test_cc3_dirty_participants_are_not_split_into_chars(self):
        """CC3：脏 participants 不得被拆成单字符元组（参与者信息静默损坏）。"""
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
        """1061：合并必须传播 _retain_failed（否则失败批与新轮次混批）。"""
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
        """DD1：payload 的容器字段是标量时不得逐字符迭代/抛异常（整行不该被跳过）。"""
        inst, _ = _ready_plugin(mod, cfg=_cfg(private=5))
        state = inst._retain_state_from_payload(
            {
                "session_id": "10086",
                "platform": "napcat",
                "group_id": "",
                "user_id": "u1",
                "timestamp": datetime.now().isoformat(),
                # 容器是标量：字符串会被逐字符迭代，int 不可迭代
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
        # 整行未被跳过：批次内容字段仍可承载有效数据
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
        """EE1：payload 缺 timestamp 不得抛 KeyError（整行内容会被跳过）。"""
        inst, _ = _ready_plugin(mod, cfg=_cfg(private=5))
        state = inst._retain_state_from_payload(
            {
                "session_id": "10086",
                "platform": "napcat",
                "group_id": "",
                "user_id": "u1",
                # 无 timestamp
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
        """EE2：写入失败也要登记行时间戳（登记窗口内条件删除拿不到 cutoff）。"""
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
        """EE3：脏 rounds 非数字不得抛 ValueError（整行内容会被跳过）。"""
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
        """EE4：terminate 须等在途回合处理器收尾（否则停机快照写落在已关闭池上）。"""
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
            await asyncio.sleep(0)  # 让 handler 进入等待
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
        """EE5：写入窗口内编码完成 → 写后复查应删掉装有已编码内容的残留行。"""
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
            # 写入窗口内同会话编码完成：未编码数据清空
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
        """FF1：停机期间到达的回合必须走到 stopping 分支（不得被 _ready 门控吞掉）。"""
        inst, ctx = _ready_plugin(mod, cfg=_cfg(private=5))
        dm = FakeSession(sid="10086", stype="dm", adapter="napcat")

        async def scenario():
            await inst.observe_message(
                types.SimpleNamespace(
                    message=make_msg("u1", "停机窗口的一轮", mid="ff1m"), session=dm
                )
            )
            # 模拟 terminate 已置停机标志、_ready 已置 False
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
        """FF2：失败标记须随 payload 落盘并在恢复后读回（不与新轮次混批）。"""
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
        # 未标脏的批次不带该标记
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
        """GG1：一行坏掉只跳过自己，不得中断整轮恢复（后面的行永远回灌不了）。"""
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
                    # 坏行：取 session_key 时抛异常（合并/龄期计算同样可能抛）
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
        """GG2：NaN/±inf 的空闲阈值须回退默认值（NaN 会让巡检把全部批次提前冲刷）。"""
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
        """HH1：NaN/±inf 配置值不得让装配失败（LocalMemoryConfig 的 gt/ge 判失败→记忆下线）。"""
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
        # 字符串形态同样拦下
        cfg2 = mod._build_config({"dsn": DSN, "flush_idle_s": "nan"})
        self.assertEqual(cfg2.flush_idle_s, 1800.0)

    def test_hh2_empty_participant_is_skipped(self):
        """HH2：空参与者项直接跳过（("", "") 会污染下游参与者列表）。"""
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
        """HH3：攒批冲刷须复用统一的任务创建入口（与逐轮路径同一收口集合）。"""
        import inspect

        src = inspect.getsource(mod.NoriflowMemoryPlugin._flush_pending_batch)
        self.assertIn("_spawn_retain_task", src, "须复用统一的任务创建入口")
        self.assertNotIn(
            "asyncio.create_task", src, "不得再手搓一套建任务逻辑（两处易不同步）"
        )


    def test_ii1_overflow_config_values_do_not_break_assembly(self):
        """II1：OverflowError 脏值不得让装配失败（float(10**400)/int(inf)）。"""
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
        """II2：运行期脏阈值不得让巡检协程死掉（_retain_flush_idle_s 不抛）。"""
        inst, _ = _ready_plugin(mod, cfg=_cfg(private=5))
        inst._config.flush_idle_s = 10 ** 400
        self.assertEqual(inst._retain_flush_idle_s(), 1800.0)
        inst._config.flush_idle_s = float("inf")
        self.assertEqual(inst._retain_flush_idle_s(), 1800.0)


if __name__ == "__main__":
    unittest.main()
