"""部署环境回环验证：对真实 PostgreSQL（pgvector）验证本插件数据层。

用法（在部署机上）：
    python tests/live_check.py
可选环境变量：
    NORIFLOW_MEMORY_CONFIG  插件配置 JSON 路径
                            （默认 /data/KiraAI/data/config/plugins/kira-ai-plugin-noriflow-memory.json）

检查项：
1. 配置加载与 dsn
2. 连接 + schema 迁移（幂等）
3. 摘要写入 / 按幂等键清理（memory_chat_summary 双向通路）
4. 事实簇/画像/摘要维护查询（webui_store 数据层）
5. 画像拼装（确定性投影，空画像合法）

不依赖 KiraAI 进程；LLM 编码/向量检索不在本脚本范围（需宿主模型配置）。
"""

from __future__ import annotations

import asyncio
import hashlib
import json
import os
import sys
import time
from pathlib import Path

import asyncpg

PLUGIN_DIR = Path(__file__).resolve().parent.parent
DEFAULT_CONFIG = "/data/KiraAI/data/config/plugins/kira-ai-plugin-noriflow-memory.json"

# 让脚本可直接引用插件内模块（包上下文）
sys.path.insert(0, str(PLUGIN_DIR.parent))


def _load_config() -> dict:
    path = os.environ.get("NORIFLOW_MEMORY_CONFIG", DEFAULT_CONFIG)
    with open(path, "rb") as f:
        return json.loads(f.read().decode("utf-8"))


async def main() -> int:
    results: list[tuple[str, bool, str]] = []

    # 1. 配置
    try:
        cfg = _load_config()
        dsn = str(cfg.get("dsn", "") or "").strip()
        assert dsn, "dsn 未配置"
        results.append(("配置加载", True, f"dsn={dsn.split('@')[-1]}"))
    except Exception as exc:
        results.append(("配置加载", False, str(exc)))
        _report(results)
        return 1

    # 动态加载插件模块（包上下文）
    pkg_name = "noriflow_memory_live_pkg"
    import types

    pkg = types.ModuleType(pkg_name)
    pkg.__path__ = [str(PLUGIN_DIR)]
    sys.modules[pkg_name] = pkg
    import importlib.util

    def load(name: str):
        spec = importlib.util.spec_from_file_location(
            f"{pkg_name}.{name}", PLUGIN_DIR / f"{name}.py"
        )
        mod = importlib.util.module_from_spec(spec)
        sys.modules[f"{pkg_name}.{name}"] = mod
        spec.loader.exec_module(mod)
        return mod

    try:
        config_mod = load("config")
        db_mod = load("db")
        envelope_mod = load("envelope")
        webui_store = load("webui_store")
        config_cls = config_mod.LocalMemoryConfig
        results.append(("模块加载", True, "config/db/envelope/webui_store"))
    except Exception as exc:
        results.append(("模块加载", False, f"{exc}（缺依赖？asyncpg/json_repair）"))
        _report(results)
        return 1

    config = config_cls(
        **{k: v for k, v in cfg.items()
           if k in config_cls.model_fields and k != "enabled"}
    )
    db = db_mod.MemoryDatabase(config)

    # 2. 连接 + 迁移（幂等：执行两次）
    try:
        await db.connect()
        migrations = PLUGIN_DIR / "migrations"
        await db.apply_migrations(migrations)
        await db.apply_migrations(migrations)
        results.append(("连接与迁移", True, "迁移可重复执行"))
    except Exception as exc:
        results.append(("连接与迁移", False, str(exc)))
        _report(results)
        return 1

    # 3. 摘要写入 + 清理
    try:
        probe = f"livecheck-{int(time.time())}"
        content = f'<msg ts="2026-01-01 00:00:00" uid="u-live" name="tester">回环测试 {probe}</msg>'
        document_id = f"livecheck-{hashlib.md5(content.encode()).hexdigest()[:12]}"
        async with db.pool.acquire() as conn:
            await conn.execute(
                """
                INSERT INTO memory_chat_summary
                    (document_id, kind, platform, session_id, group_id, user_id,
                     participants, content, occurred_at)
                VALUES ($1,'chat_summary','livecheck','sess-live','',$1,'{livecheck:u-live}',$2,now())
                ON CONFLICT (document_id) DO NOTHING
                """,
                document_id,
                content,
            )
            row = await conn.fetchval(
                "SELECT count(*) FROM memory_chat_summary WHERE document_id = $1",
                document_id,
            )
        ok = row == 1
        results.append(("摘要写入", ok, f"document_id={document_id} rows={row}"))
    except Exception as exc:
        document_id = ""
        results.append(("摘要写入", False, str(exc)))

    try:
        async with db.pool.acquire() as conn:
            deleted = await conn.fetchval(
                "DELETE FROM memory_chat_summary WHERE document_id = $1 RETURNING id",
                document_id,
            )
        results.append(("摘要清理", deleted is not None, f"deleted={document_id}"))
    except Exception as exc:
        results.append(("摘要清理", False, str(exc)))

    # 4. 维护数据层查询
    try:
        overview = await webui_store.fetch_overview(db.pool)
        results.append((
            "维护查询-概览",
            "summaries" in overview,
            f"summaries={overview.get('summaries')} clusters={overview.get('clusters')}",
        ))
    except Exception as exc:
        results.append(("维护查询-概览", False, str(exc)))

    try:
        facts = await webui_store.fetch_facts(db.pool, 1, 5)
        summaries = await webui_store.fetch_summaries(db.pool, 1, 5)
        clusters = await webui_store.fetch_clusters(db.pool, 1, 5)
        ok = all("items" in d for d in (facts, summaries, clusters))
        results.append(("维护查询-列表", ok, "facts/summaries/clusters 分页查询正常"))
    except Exception as exc:
        results.append(("维护查询-列表", False, str(exc)))

    # 5. 画像拼装（无数据时返回空文本属预期）
    try:
        persona_mod = load("persona_service")
        svc = persona_mod.LocalPersonaService(
            db=db, config=config, bot_nickname="tester"
        )
        text = await svc.build_profile_text(user_id="u-nonexist-livecheck", platform="livecheck")
        results.append(("画像拼装", text == "", f"无数据画像为空串（len={len(text)}）"))
    except Exception as exc:
        results.append(("画像拼装", False, str(exc)))

    # 清理：关闭连接池
    try:
        await db.close()
    except Exception:
        pass

    # 信封格式冒烟（不依赖 DB）
    line = envelope_mod.format_history_message(
        speaker_name="t", content="hello",
        user_id="u1", nickname="t", cardname="c",
    )
    results.append(("信封格式", 'uid="u1"' in line, line[:60]))

    ok = _report(results)
    return 0 if ok else 1


def _report(results) -> bool:
    ok = all(r[1] for r in results)
    for name, passed, detail in results:
        print(f"[{'PASS' if passed else 'FAIL'}] {name}: {detail}")
    print(f"\n{sum(1 for r in results if r[1])}/{len(results)} checks passed")
    return ok


if __name__ == "__main__":
    t0 = time.time()
    code = asyncio.run(main())
    print(f"(耗时 {time.time() - t0:.1f}s)")
    sys.exit(code)
