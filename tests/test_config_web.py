"""config_web（维护页设置栏：插件配置读写）自包含测试。

运行：python tests/test_config_web.py

覆盖：schema 推导（PydanticUndefined 缺省/restart/敏感键标注）/ 掩码与
哨兵还原 / payload 组装（缺键补默认）/ prepare_save 的 partial 语义与
校验拒绝 / persist_host_config 落盘合并 / GET/PUT handler 编排（宿主
plugin_mgr 桩 + 运行时实例热更新断言）。
"""

from __future__ import annotations

import asyncio
import importlib.util
import json
import sys
import tempfile
try:
    import tomllib  # noqa: F401  # placeholder kept for parity with the upstream nori tests (this side stores config as json)
except ModuleNotFoundError:  # Python 3.10 has no stdlib tomllib; tomli ships via requirements.txt
    import tomli as tomllib  # noqa: F401
import unittest
from pathlib import Path

PLUGIN_DIR = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(PLUGIN_DIR))

# config_web 用包内相对导入：先挂包桩再加载（与主测试 _load_plugin_module 同法）
import types  # noqa: E402

_pkg = types.ModuleType("noriflow_memory_pkg")
_pkg.__path__ = [str(PLUGIN_DIR)]
sys.modules.setdefault("noriflow_memory_pkg", _pkg)

from noriflow_memory_pkg import config_web  # noqa: E402
from noriflow_memory_pkg.config import LocalMemoryConfig  # noqa: E402
from noriflow_memory_pkg.config_web import (  # noqa: E402
    SaveBlocked,
    _MASK,
    build_payload,
    config_schema,
    mask_values,
    persist_host_config,
    prepare_save,
    unmask_values,
)


class TestSchema(unittest.TestCase):
    def test_schema_covers_all_fields_with_annotations(self):
        self.assertEqual(set(config_schema()), set(LocalMemoryConfig.model_fields))
        s = config_schema()
        # restart 标注：装配期展开 vs 运行时读
        self.assertTrue(s["dsn"]["restart"])
        self.assertTrue(s["pool_max"]["restart"])
        self.assertTrue(s["alias_stopwords"]["restart"])
        self.assertFalse(s["recall_top_k"]["restart"])
        self.assertFalse(s["backfill_interval_seconds"]["restart"])  # 运行时读（热加载批）
        self.assertFalse(s["failure_threshold"]["restart"])  # 熔断器运行时读
        self.assertFalse(s["tool_scope_locked"]["restart"])
        # sensitive：dsn 命中；非敏感不受影响
        self.assertTrue(s["dsn"]["sensitive"])
        self.assertFalse(s["recall_top_k"]["sensitive"])
        # 类型映射
        self.assertEqual(s["topic_blacklist"]["type"], "list")
        self.assertEqual(s["recall_relevance_threshold"]["type"], "float")
        self.assertEqual(s["recall_time_decay_enabled"]["type"], "bool")
        # default_factory 字段（topic_blacklist）默认值可 JSON 序列化且非哨兵
        json.dumps(s["topic_blacklist"]["default"])
        self.assertEqual(s["topic_blacklist"]["default"], [])

    def test_groups_cover_every_field(self):
        schema = config_schema()
        seen = set()
        for g in build_payload({})["groups"]:
            seen.update(g["fields"])
        self.assertEqual(seen, set(schema))


class TestMask(unittest.TestCase):
    def test_mask_and_unmask_roundtrip(self):
        vals = {"dsn": "postgres://u:p@h/db", "recall_top_k": 5}
        masked = mask_values(vals)
        self.assertEqual(masked["dsn"], _MASK)
        self.assertEqual(masked["recall_top_k"], 5)
        # 哨兵还原取现值；空串不掩码
        self.assertEqual(mask_values({"dsn": ""})["dsn"], "")
        out = unmask_values(masked, vals)
        self.assertEqual(out["dsn"], "postgres://u:p@h/db")


class TestPayload(unittest.TestCase):
    def test_missing_keys_filled_with_defaults_and_masked(self):
        d = build_payload({"dsn": "postgres://u:p@h/db", "pool_max": 8})
        self.assertEqual(d["values"]["dsn"], _MASK)
        self.assertEqual(d["values"]["pool_max"], 8)
        self.assertEqual(d["values"]["recall_top_k"], 5)  # 缺键补默认
        self.assertEqual(d["groups"][0]["key"], "conn")


class TestPrepareSave(unittest.TestCase):
    HOST = {"dsn": "postgres://u:p@h/db", "pool_max": 8, "recall_top_k": 5}

    def test_partial_submit_keeps_current_values(self):
        r = prepare_save({"recall_top_k": 7}, self.HOST)
        # 未提交字段沿用现值（不落模型默认）
        self.assertEqual(r["validated"].dsn, self.HOST["dsn"])
        self.assertEqual(r["validated"].pool_max, 8)
        self.assertEqual(r["changed"], {"recall_top_k": 7})
        self.assertEqual(r["restart_required"], [])

    def test_validation_failure_raises(self):
        with self.assertRaises(ValueError):
            prepare_save({"recall_top_k": 0}, self.HOST)

    def test_empty_dsn_blocked(self):
        with self.assertRaises(SaveBlocked):
            prepare_save({"dsn": ""}, self.HOST)

    def test_no_change_yields_empty_diff(self):
        r = prepare_save({"dsn": _MASK, "recall_top_k": 5}, self.HOST)
        self.assertEqual(r["changed"], {})

    def test_restart_fields_reported(self):
        r = prepare_save({"pool_max": 16}, self.HOST)
        self.assertEqual(r["restart_required"], ["pool_max"])


class TestPersistHostConfig(unittest.TestCase):
    def test_merge_into_existing_file(self):
        with tempfile.TemporaryDirectory() as td:
            cfg_dir = Path(td)
            (cfg_dir / "kira-ai-plugin-noriflow-memory.json").write_text(
                json.dumps({"dsn": "postgres://old", "enabled": True}, ensure_ascii=False),
                encoding="utf-8",
            )
            current = {"dsn": "postgres://old", "enabled": True, "recall_top_k": 5}
            persist_host_config(cfg_dir, "kira-ai-plugin-noriflow-memory",
                                {"dsn": "postgres://new", "recall_top_k": 7}, current)
            on_disk = json.loads(
                (cfg_dir / "kira-ai-plugin-noriflow-memory.json").read_text(encoding="utf-8")
            )
            self.assertEqual(on_disk["dsn"], "postgres://new")
            self.assertEqual(on_disk["recall_top_k"], 7)
            self.assertTrue(on_disk["enabled"])  # 非配置字段不被触碰


# ---------------- handler 编排（复用主测试文件的桩链路） ----------------


class TestConfigApi(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        base_spec = importlib.util.spec_from_file_location(
            "base_tests", str(Path(__file__).parent / "test_noriflow_memory.py")
        )
        cls.base = importlib.util.module_from_spec(base_spec)
        base_spec.loader.exec_module(cls.base)
        cls.mod = cls.base._load_plugin_module()
        from fastapi import HTTPException  # noqa: F401
        cls.HTTPException = HTTPException

    def setUp(self):
        self._tmp_config_dir = tempfile.TemporaryDirectory()

    def tearDown(self):
        self._tmp_config_dir.cleanup()

    def _make_inst(self, host_cfg):
        mod = self.mod

        class _PM:
            def __init__(self):
                self.plugin_configs = {"kira-ai-plugin-noriflow-memory": dict(host_cfg)}
                self.enabled = {"kira_plugin_simple_memory": True}

            def get_plugin_config(self, pid):
                return dict(self.plugin_configs.get(pid, {}))

            def is_plugin_enabled(self, pid):
                return self.enabled.get(pid, True)

            async def set_plugin_enabled(self, pid, enabled):
                self.enabled[pid] = enabled

        # handler 内延迟 import core.plugin.plugin_registry：测试环境 core
        # 是桩包，预注一个带 PLUGIN_CONFIG_DIR 的桩模块（指向临时目录）
        core_pkg = sys.modules.get("core")
        if core_pkg is not None and "core.plugin.plugin_registry" not in sys.modules:
            reg_stub = types.ModuleType("core.plugin.plugin_registry")
            reg_stub.PLUGIN_CONFIG_DIR = self._tmp_config_dir.name
            sys.modules["core.plugin.plugin_registry"] = reg_stub

        ctx = self.base.FakeCtx()
        ctx.plugin_mgr = _PM()
        inst = mod.NoriflowMemoryPlugin(ctx, dict(host_cfg))
        orig_db = mod.MemoryDatabase
        mod.MemoryDatabase = self.base.FakeDB
        loop = asyncio.new_event_loop()
        try:
            loop.run_until_complete(inst.initialize())

            async def _stop():
                if inst._backfill_task is not None:
                    await inst._backfill_task.stop()
                if inst._merge_agent is not None:
                    await inst._merge_agent.stop()

            loop.run_until_complete(_stop())
        finally:
            mod.MemoryDatabase = orig_db
            loop.close()
        return inst

    def test_get_and_put_roundtrip_hot_swap_and_persist(self):
        host = {"dsn": "postgres://u:p@127.0.0.1:5432/db", "recall_top_k": 5}
        inst = self._make_inst(host)
        pm = inst.ctx.plugin_mgr

        # GET：schema + 掩码值
        d = asyncio.run(inst.api_memory_config_get())
        self.assertEqual(d["groups"][0]["key"], "conn")
        self.assertEqual(d["values"]["dsn"], _MASK)
        self.assertEqual(d["values"]["recall_top_k"], 5)

        # PUT：热字段 + restart 字段
        reg_stub = sys.modules.get("core.plugin.plugin_registry")
        if reg_stub is not None:
            reg_stub.PLUGIN_CONFIG_DIR = Path(self._tmp_config_dir.name)
        try:
            values = dict(d["values"])
            values.update({"recall_top_k": 7, "pool_max": 16})
            r = asyncio.run(inst.api_memory_config_put({"values": values}))
        finally:
            if reg_stub is not None:
                sys.modules.pop("core.plugin.plugin_registry", None)

        self.assertTrue(r["saved"])
        self.assertEqual(r["changes"], 2)
        self.assertEqual(r["restart_required"], ["pool_max"])
        # 运行时实例热更新
        self.assertEqual(inst._config.recall_top_k, 7)
        self.assertEqual(inst._config.pool_max, 16)
        # 宿主真相源（内存 + 磁盘）
        self.assertEqual(pm.plugin_configs["kira-ai-plugin-noriflow-memory"]["recall_top_k"], 7)
        on_disk = json.loads(
            (Path(self._tmp_config_dir.name) / "kira-ai-plugin-noriflow-memory.json")
            .read_text(encoding="utf-8")
        )
        self.assertEqual(on_disk["recall_top_k"], 7)
        self.assertEqual(on_disk["pool_max"], 16)

    def test_put_rejects_bad_values(self):
        inst = self._make_inst({"dsn": "postgres://u:p@h/db"})
        with self.assertRaises(self.HTTPException) as cm:
            asyncio.run(inst.api_memory_config_put({"values": {"recall_top_k": 0}}))
        self.assertEqual(cm.exception.status_code, 422)
        with self.assertRaises(self.HTTPException) as cm2:
            asyncio.run(inst.api_memory_config_put({"values": {"dsn": ""}}))
        self.assertEqual(cm2.exception.status_code, 400)
        # 无变化短路
        host_now = dict(inst.ctx.plugin_mgr.get_plugin_config("kira-ai-plugin-noriflow-memory"))
        r = asyncio.run(inst.api_memory_config_put({"values": host_now}))
        self.assertFalse(r["saved"])


if __name__ == "__main__":
    unittest.main(verbosity=2)
