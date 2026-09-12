"""Kira 记忆数据迁入 harness（python 直跑，无 pytest 依赖）。

覆盖（真 SQLite 库 + 临时源目录）：
- 会话映射：KiraAI "seki:dm:UID"/"seki:gm:GID" -> 现网裸 session_id +
  platform 前缀复合 participants；sm/非法会话跳过；
- chunk 组装：信封行（uid/name/self 属性）、内嵌时间戳解析、防伪装
  中性化、无 sender 系统通知不带归属属性；
- TOML 事实/洞察 -> persona_fact_raw（kernel 幂等键同式、importance->
  confidence、群实体归属群 ID、global/self 归属 bot、archive/skills 跳过）；
- profile.json -> 别名行（name/nickname/aliases 去重，source=kira_memory_import）；
- 幂等重跑：二次执行实插量为 0，kv 标记更新；
- 源目录只读性：迁入前后源文件内容与 mtime 不变。

运行（插件目录）：
    python tests/test_kira_memory_import.py
"""

from __future__ import annotations

import asyncio
import importlib.util
import json
import logging
import os
import sys
import tempfile
import types
from datetime import datetime, timezone
from pathlib import Path

PLUGIN_DIR = Path(__file__).resolve().parent.parent

PASS = 0
_FAIL = 0


def check(name: str, cond: bool, detail: str = "") -> None:
    global PASS, _FAIL
    if cond:
        PASS += 1
        print(f"PASS {name}")
    else:
        _FAIL += 1
        print(f"FAIL {name} {detail}")


def _install_stubs() -> None:
    if "noriflow_memory_pkg" in sys.modules:
        return
    core = types.ModuleType("core")
    logging_mod = types.ModuleType("core.logging_manager")

    logging_mod.get_logger = lambda *args, **kwargs: logging.getLogger("stub")

    sys.modules["core"] = core
    sys.modules["core.logging_manager"] = logging_mod

    pkg = types.ModuleType("noriflow_memory_pkg")
    pkg.__path__ = [str(PLUGIN_DIR)]
    sys.modules["noriflow_memory_pkg"] = pkg

    fastapi = types.ModuleType("fastapi")

    class HTTPException(Exception):
        def __init__(self, status_code, detail=""):
            self.status_code = status_code
            self.detail = detail
            super().__init__(f"{status_code}: {detail}")

    fastapi.HTTPException = HTTPException
    sys.modules["fastapi"] = fastapi


def _load(mod_name: str):
    _install_stubs()
    target = PLUGIN_DIR / mod_name
    if target.is_dir():
        path = target / "__init__.py"
        kwargs = {"submodule_search_locations": [str(target)]}
    else:
        path = PLUGIN_DIR / f"{mod_name}.py"
        kwargs = {}
    spec = importlib.util.spec_from_file_location(
        f"noriflow_memory_pkg.{mod_name}", path, **kwargs
    )
    mod = importlib.util.module_from_spec(spec)
    sys.modules[f"noriflow_memory_pkg.{mod_name}"] = mod
    spec.loader.exec_module(mod)
    return mod


# 崩溃兜底：aiosqlite 后台线程非常驻守护线程，断言失败未走 db.close() 时
# 进程会挂着不退——统一在 main() finally 里强制收尾
_DBS: list = []


def _shutdown_dbs() -> None:
    for db in _DBS:
        try:
            asyncio.run(db.close())
        except Exception:
            pass


# ---------------------------------------------------------------------------
#  源目录 fixture
# ---------------------------------------------------------------------------


def _write_toml(path: Path, body: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(body, encoding="utf-8")


def build_source(root: Path) -> None:
    # chat_memory.json：dm（带 sender + 内嵌时间戳）、gm（无 sender）、sm 跳过；
    # 含宿主系统通知（Notice + system_* 伪账号，应跳过）与真实消息转储（保留）
    chat = {
        "seki:dm:10086": {
            "title": "私聊",
            "description": "",
            "memory": [
                [
                    {"role": "user", "content": "[Sep 10 2026 18:44 Thu] 我昨天通宵打塞尔达了",
                     "sender_id": "10086", "sender_name": "小明"},
                    {"role": "assistant", "content": '<msg><text>少熬夜呀</text></msg>'},
                ],
                [
                    {"role": "user", "content": "纯文本无时间戳"},
                    {"role": "user", "content":
                     "[Sep 10 2026 11:00 Thu] Notice [user_id: system_qzone_task] | 【评论任务】请对最近的好友说说进行评论"},
                    {"role": "assistant", "content": "<msg />"},
                ],
                [
                    {"role": "user", "content":
                     "[Sep 10 2026 12:00 Thu] Notice [user_id: system_qzone_task] | 【回复任务】请回复你最近说说下的新评论"},
                    {"role": "assistant", "content": "已回复三条评论"},
                ],
            ],
        },
        "seki:gm:9988": {
            "title": "群",
            "description": "",
            "memory": [
                [
                    {"role": "user", "content":
                     "[Sep 11 2026 09:00 Fri] [message_id: 76168] [user_nickname: Hy, user_id: 10086] | 群通知内容"},
                ],
            ],
        },
        "seki:sm:system": {"title": "", "description": "", "memory": [[{"role": "user", "content": "x"}]]},
    }
    (root / "chat_memory.json").write_text(
        json.dumps(chat, ensure_ascii=False), encoding="utf-8"
    )

    # 实体 TOML：用户事实（importance 8 -> high）、用户洞察、群事实
    _write_toml(
        root / "entities" / "user_seki%3A10086" / "facts" / "hates_css.toml",
        "\n".join([
            'id = "hates_css"',
            'type = "fact"',
            'text = "小明讨厌写 CSS"',
            "importance = 8",
            'tags = ["frontend"]',
            "",
            "[source]",
            'session = "seki:dm:10086"',
            "time = 2026-09-10T18:44:00+08:00",
            "",
            "[meta]",
            "timestamp = 1789128240.0",
        ]) + "\n",
    )
    _write_toml(
        root / "entities" / "user_seki%3A10086" / "reflections" / "insight.toml",
        "\n".join([
            'id = "insight"',
            'type = "reflection"',
            'text = "小明对游戏话题响应最积极"',
            "importance = 5",
        ]) + "\n",
    )
    _write_toml(
        root / "entities" / "group_seki%3A9988" / "facts" / "grouprule.toml",
        "\n".join([
            'id = "grouprule"',
            'type = "fact"',
            'text = "群规：禁止刷屏"',
            "importance = 6",
        ]) + "\n",
    )
    # global/self（bot 自我觉察）+ global/facts
    _write_toml(
        root / "global" / "self" / "facts" / "self1.toml",
        "\n".join(['id = "self1"', 'type = "fact"', 'text = "AI 喜欢用喵结尾"']) + "\n",
    )
    # archive / skills：按语义不迁入
    _write_toml(
        root / "archive" / "user_seki%3A10086__facts__old.toml",
        'id = "old"\ntext = "已遗忘的记忆"\n',
    )
    _write_toml(root / "global" / "skills" / "skill1.toml", 'id = "skill1"\ntext = "技能"\n')

    # 画像
    profile = {
        "entity_id": "seki:10086", "entity_type": "user",
        "name": "小明", "nickname": "明哥", "aliases": ["明哥", "小明同学", ""],
    }
    (root / "entities" / "user_seki%3A10086" / "profile.json").write_text(
        json.dumps(profile, ensure_ascii=False), encoding="utf-8"
    )


def _snapshot_source(root: Path) -> dict[str, tuple[bytes, float]]:
    snap = {}
    for p in sorted(root.rglob("*")):
        if p.is_file():
            st = p.stat()
            snap[str(p.relative_to(root))] = (p.read_bytes(), st.st_mtime)
    return snap


# ---------------------------------------------------------------------------
#  用例
# ---------------------------------------------------------------------------


async def main_async() -> None:
    config_mod = _load("config")
    _load("alias_store")
    _load("entity_edge")
    db_pkg = _load("db")
    ki = _load("kira_memory_import")

    LocalMemoryConfig = config_mod.LocalMemoryConfig
    SQLiteMemoryDatabase = db_pkg.SQLiteMemoryDatabase

    # ---- 纯函数单测 ----
    s = ki.parse_kira_memory_session("seki:dm:10086")
    check("session_dm", s == {"platform": "seki", "session_id": "10086",
                              "group_id": "", "user_id": "10086"}, str(s))
    s = ki.parse_kira_memory_session("seki:gm:9988")
    check("session_gm", s == {"platform": "seki", "session_id": "9988",
                              "group_id": "9988", "user_id": ""}, str(s))
    check("session_sm_rejected", ki.parse_kira_memory_session("seki:sm:sys") is None)
    check("session_bad_rejected", ki.parse_kira_memory_session("no-colon") is None)
    ts = ki.parse_content_timestamp("[Sep 10 2026 18:44 Thu] 内容")
    check("content_ts_parsed", ts is not None and (ts.year, ts.month, ts.day, ts.hour) ==
          (2026, 9, 10, 18), str(ts))
    check("content_ts_absent", ki.parse_content_timestamp("无时间戳") is None)

    content = ki.compose_chunk_content(
        [
            {"role": "user", "content": "[Sep 10 2026 18:44 Thu] 通宵打游戏",
             "sender_id": "10086", "sender_name": "小明"},
            {"role": "assistant", "content": "<msg><text>少熬夜</text></msg>"},
            {"role": "user", "content": "系统通知"},
            {"role": "user", "content": ""},
        ],
        bot_nickname="Kira",
    )
    check("compose_uid_attr", 'uid="10086"' in content and 'name="小明"' in content, content)
    check("compose_ts_attr", 'ts="2026-09-10 18:44:00"' in content, content)
    check("compose_self_attr", 'self="true"' in content and "Kira" in content, content)
    check("compose_mimicry_neutralized", "<msg>" not in content, content)
    check("compose_no_fake_attribution", "系统通知" in content and 'uid=' not in
          content.split("系统通知")[0].rsplit("\n", 1)[-1], content)

    # 系统通知过滤：Notice + system_* 伪账号签名
    check("notice_detected", ki.is_system_notice(
        {"role": "user", "sender_id": "",
         "content": "[Sep 10 2026 11:00 Thu] Notice [user_id: system_qzone_task] | 任务"}))
    check("notice_with_sender_not_filtered", not ki.is_system_notice(
        {"role": "user", "sender_id": "10086",
         "content": "[Sep 10 2026 11:00 Thu] Notice [user_id: system_x] | 任务"}))
    check("real_dump_not_filtered", not ki.is_system_notice(
        {"role": "user", "sender_id": "",
         "content": "[Sep 11 2026 09:00 Fri] [message_id: 76168] "
                    "[user_nickname: Hy, user_id: 10086] | 你有印象吗"}))
    check("real_notice_not_filtered", not ki.is_system_notice(
        {"role": "user", "sender_id": "",
         "content": "[Sep 11 2026 09:00 Fri] Notice [group_id: 9988, user_id: 10086] | 某某 退出了群聊"}))
    notice_content = ki.compose_chunk_content(
        [
            {"role": "user",
             "content": "[Sep 10 2026 11:00 Thu] Notice [user_id: system_qzone_task] | 【评论任务】指令"},
            {"role": "user", "sender_id": "",
             "content": "[Sep 11 2026 09:00 Fri] [message_id: 7] [user_nickname: Hy, user_id: 10086] | 真实消息"},
        ],
    )
    check("compose_drops_notice", "评论任务" not in notice_content
          and "真实消息" in notice_content, notice_content)

    # ---- 起库 + 源目录 ----
    tmp = tempfile.TemporaryDirectory(prefix="noriflow_kira_memory_import_")
    db_path = str(Path(tmp.name) / "memory.sqlite3")
    source = Path(tmp.name) / "kira_memory_source"
    source.mkdir()
    build_source(source)
    snap_before = _snapshot_source(source)

    config = LocalMemoryConfig(storage_backend="sqlite", sqlite_path=db_path,
                               embedding_dims=4)
    db = SQLiteMemoryDatabase(config)
    _DBS.append(db)
    await db.connect()
    await db.apply_migrations(PLUGIN_DIR / "migrations_sqlite")

    preview = ki.scan_source(source)
    check("preview_counts", preview["sessions"] == 2 and preview["chunks"] == 4
          and preview["messages"] == 8 and preview["toml_files"] == 4
          and preview["profiles"] == 1 and preview["archive_files"] == 1,
          str(preview))

    # ---- 第一次迁入 ----
    stats = await ki.run_kira_memory_import(
        db, source_path=source, bot_uid="99999", bot_nickname="Kira",
    )
    check("stats_sessions", stats["sessions"] == 2, str(stats))
    check("stats_chunks_total", stats["chunks_total"] == 4, str(stats))
    check("stats_notice_skipped", stats["system_notice_msgs_skipped"] == 2, str(stats))
    check("stats_bot_only_skipped", stats["chunks_bot_only_skipped"] == 1, str(stats))
    check("stats_summary_rows", stats["summary_rows"] == 3, str(stats))
    check("stats_fact_rows", stats["fact_rows"] == 4, str(stats))  # 2 user + 1 group + 1 self
    check("stats_aliases", stats["aliases_upserted"] == 3, str(stats))
    check("stats_archive_skipped", stats["facts_total"] == 4, str(stats))
    check("stats_errors_clean", stats["parse_errors"] == 0, str(stats["errors"]))

    conn = db.pool.conn

    # 摘要行映射（同会话多 chunk 行，ORDER BY id 取首个=信封最全的 dm 批次）
    dm = dict(await conn.fetchrow(
        "SELECT * FROM memory_chat_summary WHERE session_id='10086' AND group_id='' "
        "ORDER BY id LIMIT 1"))
    check("dm_platform", dm["platform"] == "seki", str(dm["platform"]))
    check("dm_user_id", dm["user_id"] == "10086", str(dm["user_id"]))
    check("dm_participants", dm["participants"] == '["seki:10086"]',
          str(dm["participants"]))
    check("dm_summarized_false", not dm["summarized"], str(dm["summarized"]))
    check("dm_kind", dm["kind"] == "chat_summary", str(dm["kind"]))
    check("dm_document_id_formula",
          dm["document_id"] ==
          f"10086-{__import__('hashlib').md5(dm['content'].encode()).hexdigest()[:12]}",
          dm["document_id"])
    check("dm_content_envelope", 'uid="10086"' in dm["content"]
          and 'self="true"' in dm["content"], dm["content"][:120])
    check("dm_occurred_from_content", dm["occurred_at"] is not None and
          "2026-09-10" in str(dm["occurred_at"]), str(dm["occurred_at"]))

    gm = dict(await conn.fetchrow(
        "SELECT * FROM memory_chat_summary WHERE session_id='9988' ORDER BY id LIMIT 1"))
    check("gm_group_id", gm["group_id"] == "9988", str(gm["group_id"]))
    check("gm_user_id_empty", gm["user_id"] == "", str(gm["user_id"]))
    check("gm_participants_empty", gm["participants"] in ("[]", "", None),
          str(gm["participants"]))

    # 系统通知不落库；真实消息转储（正文带真实 user_id）保留
    check("notice_content_absent", await conn.fetchval(
        "SELECT count(*) FROM memory_chat_summary WHERE content LIKE '%评论任务%'") == 0)
    check("real_dump_kept", await conn.fetchval(
        "SELECT count(*) FROM memory_chat_summary WHERE content LIKE '%群通知内容%'") == 1)

    # 事实行映射
    fact = dict(await conn.fetchrow(
        "SELECT * FROM memory_persona_fact_raw WHERE statement='小明讨厌写 CSS'"))
    check("fact_owner", fact["user_id"] == "10086" and fact["platform"] == "seki",
          str(fact["user_id"]))
    check("fact_category_confidence", fact["category"] == "stable"
          and fact["confidence"] == "high", f"{fact['category']}/{fact['confidence']}")
    check("fact_session_from_source", fact["session_id"] == "10086",
          str(fact["session_id"]))
    from noriflow_memory_pkg.db.base import fact_document_id, sqlite_parse_ts
    expected_doc = (
        f"{fact_document_id(['10086'], '10086', sqlite_parse_ts(fact['occurred_at']))}-"
        f"{__import__('hashlib').md5('小明讨厌写 CSS'.encode()).hexdigest()[:12]}"
    )
    check("fact_document_id_kernel_formula", fact["document_id"] == expected_doc,
          f"{fact['document_id']} != {expected_doc}")

    refl = dict(await conn.fetchrow(
        "SELECT * FROM memory_persona_fact_raw WHERE statement LIKE '小明对游戏%'"))
    check("reflection_imported_medium", refl["confidence"] == "medium", str(refl["confidence"]))

    group_fact = dict(await conn.fetchrow(
        "SELECT * FROM memory_persona_fact_raw WHERE statement='群规：禁止刷屏'"))
    check("group_fact_owner_gid", group_fact["user_id"] == "9988",
          str(group_fact["user_id"]))

    self_fact = dict(await conn.fetchrow(
        "SELECT * FROM memory_persona_fact_raw WHERE statement LIKE 'AI 喜欢用喵%'"))
    check("self_fact_owner_bot", self_fact["user_id"] == "99999"
          and self_fact["platform"] == "seki",
          f"{self_fact['platform']}:{self_fact['user_id']}")

    check("archive_not_imported", await conn.fetchval(
        "SELECT count(*) FROM memory_persona_fact_raw WHERE statement='已遗忘的记忆'") == 0)
    check("skills_not_imported", await conn.fetchval(
        "SELECT count(*) FROM memory_persona_fact_raw WHERE statement='技能'") == 0)

    # 别名行
    aliases = await conn.fetch(
        "SELECT name, source FROM memory_entity_alias WHERE user_id='10086' "
        "ORDER BY name")
    check("alias_rows", [a["name"] for a in aliases] == ["小明", "小明同学", "明哥"],
          str([dict(a) for a in aliases]))
    check("alias_source_tag", all(a["source"] == "kira_memory_import" for a in aliases),
          str([dict(a) for a in aliases]))

    # kv 标记
    last = await ki.read_last_run(db)
    check("kv_marker", last is not None and last["summary_rows"] == 3
          and last["fact_rows"] == 4, str(last))

    # ---- legacy kv marker carry-over (v1.16.1 rename) ----
    async with db.pool.acquire() as conn:
        await conn.execute(
            "DELETE FROM _memory_local_kv WHERE key = $1",
            "kira_memory_import_last_run",
        )
    await db.set_kv("kiraos_import_last_run", json.dumps(
        {"finished_at": "2026-09-12T15:40:00", "summary_rows": 1}))
    await ki.migrate_legacy_import_kv(db)
    carried = await ki.read_last_run(db)
    check("legacy_kv_carried", carried is not None and carried["summary_rows"] == 1,
          str(carried))
    legacy_left = await db.get_kv("kiraos_import_last_run")
    check("legacy_kv_dropped", legacy_left is None, str(legacy_left))

    # ---- 幂等重跑：实插量 0 ----
    stats2 = await ki.run_kira_memory_import(
        db, source_path=source, bot_uid="99999", bot_nickname="Kira",
    )
    check("rerun_summary_rows_zero", stats2["summary_rows"] == 0, str(stats2))
    check("rerum_fact_rows_zero", stats2["fact_rows"] == 0, str(stats2))
    check("rerun_skipped_counts", stats2["summary_skipped"] == 3
          and stats2["facts_skipped"] == 4, str(stats2))
    check("rerun_aliases_stable", stats2["aliases_upserted"] == 3, str(stats2))

    # ---- 源目录只读性 ----
    snap_after = _snapshot_source(source)
    check("source_untouched", snap_before == snap_after,
          str(set(snap_before.items()) ^ set(snap_after.items())))

    # ---- 缺 chat_memory.json 且无 entities/ 的目录拒绝 ----
    bogus = Path(tmp.name) / "bogus"
    bogus.mkdir()
    try:
        await ki.run_kira_memory_import(db, source_path=bogus)
        check("bogus_dir_rejected", False, "no exception")
    except ValueError:
        check("bogus_dir_rejected", True)
    try:
        await ki.run_kira_memory_import(db, source_path=Path(tmp.name) / "missing")
        check("missing_dir_rejected", False, "no exception")
    except FileNotFoundError:
        check("missing_dir_rejected", True)

    await db.close()
    tmp.cleanup()

    print(f"\n==== kira_memory_import harness: {PASS} passed, {_FAIL} failed ====")
    if _FAIL:
        sys.exit(1)


def main() -> None:
    try:
        asyncio.run(main_async())
    finally:
        _shutdown_dbs()


if __name__ == "__main__":
    main()
