"""Persistent entity alias layer (P1) tests (python direct-run, no pytest dependency).

Coverage (KiraAI side, 2026-09-09 alias layer batch):
- Variant splitting/cleaning: bracket trunk/segments, single-char and symbol filtering, "user<digits>" fallback;
- build_alias_rows: same-name keeps later last_seen, per-uid variant cap truncation;
- AliasStore: unique owner hit, same-name ambiguity (window endorsement wins / no endorsement skips),
  stopwords, apply_rows incremental sync;
- db.alias_upsert: SQL assembly and params, empty-list short-circuit;
- kernel two-layer merge: window-first + persistent backfill + pair dedup + cap 4;
- main._alias_upsert_batch: per-batch sender upsert (bot skipped, fail-open).

Run (plugin dir):
    python tests/test_alias_store.py
"""

from __future__ import annotations

import asyncio
import sys
from collections import OrderedDict
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Optional

sys.path.insert(0, str(Path(__file__).resolve().parent))

from _harness import load_module  # noqa: E402

PLUGIN_DIR = Path(__file__).resolve().parent.parent

_alias_store = load_module("alias_store")
_circuit_breaker = load_module("circuit_breaker")
_config = load_module("config")
_db = load_module("db")
_kernel = load_module("memory_kernel")
_main = load_module("main")

AliasStore = _alias_store.AliasStore
build_alias_rows = _alias_store.build_alias_rows
build_fact_code_alias_rows = _alias_store.build_fact_code_alias_rows
clean_alias_name = _alias_store.clean_alias_name
extract_uid_code_names = _alias_store.extract_uid_code_names
is_placeholder_name = _alias_store.is_placeholder_name
normalize_alias_text = _alias_store.normalize_alias_text
split_name_variants = _alias_store.split_name_variants
MemoryDBCircuitBreaker = _circuit_breaker.MemoryDBCircuitBreaker
LocalMemoryConfig = _config.LocalMemoryConfig
MemoryDatabase = _db.MemoryDatabase
LocalMemoryKernel = _kernel.LocalMemoryKernel
_EntityDirectory = _kernel._EntityDirectory

UTC = timezone.utc


# ---------------------------------------------------------------------------
#  变体拆分与清洗
# ---------------------------------------------------------------------------


def test_clean_alias_name_filters() -> None:
    assert clean_alias_name("琳妮特") == "琳妮特"
    assert clean_alias_name("Shizuku") == "Shizuku"
    assert clean_alias_name("  月月猫 ") == "月月猫"
    assert clean_alias_name("白") == ""
    assert clean_alias_name("∞") == ""
    assert clean_alias_name("。。") == ""
    assert clean_alias_name("用户1000001") == ""
    assert clean_alias_name("") == ""


def test_split_name_variants_brackets() -> None:
    assert split_name_variants("琳妮特（笨蛋猫娘）") == [
        "琳妮特（笨蛋猫娘）", "琳妮特", "笨蛋猫娘",
    ]
    assert split_name_variants("梦瑶月（月月猫）[群低性能bot]") == [
        "梦瑶月（月月猫）[群低性能bot]", "梦瑶月", "月月猫", "群低性能bot",
    ]
    variants = split_name_variants("傲娇喵  (猫娘")
    assert "傲娇喵" in variants
    assert split_name_variants("（。。）") == []
    assert split_name_variants("") == []


def test_build_alias_rows_cap_and_dedup() -> None:
    t1 = datetime(2026, 9, 1, tzinfo=UTC)
    t2 = datetime(2026, 9, 5, tzinfo=UTC)
    raw = [
        ("qq", "u1", "琳妮特（笨蛋猫娘）", t1),
        ("qq", "u1", "琳妮特（聪明猫娘）", t2),
    ]
    rows = build_alias_rows(raw, source="batch", variant_cap=3)
    by_name = {r["name"]: r for r in rows}
    assert by_name["琳妮特"]["last_seen"] == t2
    assert len(rows) == 3  # 两个 t2 整串 + 主干（t1 整串被上限截掉）
    assert "琳妮特（笨蛋猫娘）" not in by_name
    assert all(r["source"] == "batch" for r in rows)

    # 变体风暴收敛
    base = datetime(2026, 9, 1, tzinfo=UTC)
    storm = [
        ("qq", "u2", f"透明人（污染程度达到{p}%）", base + timedelta(hours=p))
        for p in (50, 68, 89, 90, 95, 96, 98, 99, 100)
    ]
    rows2 = build_alias_rows(storm, source="backfill", variant_cap=4)
    assert "透明人" in [r["name"] for r in rows2]
    assert len(rows2) <= 4


# ---------------------------------------------------------------------------
#  AliasStore 匹配
# ---------------------------------------------------------------------------


def _store_with(names: dict) -> AliasStore:
    store = AliasStore(db=None, variant_cap=8)
    rows = []
    for name, pairs in names.items():
        for platform, uid in pairs:
            rows.append({
                "platform": platform, "user_id": uid, "name": name,
                "last_seen": datetime(2026, 9, 8, tzinfo=UTC),
            })
    store.apply_rows(rows)
    return store


def test_alias_store_match_basic_and_ambiguity() -> None:
    store = _store_with({
        "琳妮特": [("qq", "u1")],
        "诺里": [("qq", "u2"), ("qq", "u3")],
    })
    hits, skipped = store.match("今天琳妮特说话了吗", set())
    assert hits == [("琳妮特", "qq", "u1")]
    assert skipped == []

    hits, skipped = store.match("诺里是谁", set())
    assert hits == []
    assert skipped == ["诺里"]

    hits, skipped = store.match("诺里是谁", {("qq", "u2")})
    assert hits == [("诺里", "qq", "u2")]
    assert skipped == []


def test_alias_store_stopwords() -> None:
    store = AliasStore(db=None, variant_cap=8, stopwords=["谢谢"])
    store.apply_rows([{
        "platform": "qq", "user_id": "u9", "name": "谢谢",
        "last_seen": datetime(2026, 9, 8, tzinfo=UTC),
    }])
    hits, _ = store.match("谢谢老板", set())
    assert hits == []
    assert store.size == 0


def test_alias_store_apply_rows_last_seen_monotonic() -> None:
    store = AliasStore(db=None, variant_cap=8)
    store.apply_rows([{
        "platform": "qq", "user_id": "u1", "name": "阿明",
        "last_seen": datetime(2026, 9, 1, tzinfo=UTC),
    }])
    store.apply_rows([{
        "platform": "qq", "user_id": "u1", "name": "阿明",
        "last_seen": datetime(2026, 8, 1, tzinfo=UTC),  # 更早：不回退
    }])
    assert len(store._names["阿明"]) == 1


def test_is_placeholder_name() -> None:
    """Placeholder-name check (shared by canonical-name resolution / write-side guard): empty / unknown[user][digit] / unknown / user<digits> / pure digits."""
    assert is_placeholder_name("")
    assert is_placeholder_name("  ")
    assert is_placeholder_name("未知")
    assert is_placeholder_name("未知用户")
    assert is_placeholder_name("未知1")
    assert is_placeholder_name("未知用户12")
    assert is_placeholder_name("unknown")
    assert is_placeholder_name("Undefined")
    assert is_placeholder_name("用户1000002")
    assert is_placeholder_name("1000002")
    # 真名（含数字混排）不误伤；"未知"系真名昵称不误伤（实测群成员
    # 昵称（如"未知旅人"式真名）——前缀泛匹配已废除）
    assert not is_placeholder_name("未知昵称")
    assert not is_placeholder_name("未知旅人")
    assert not is_placeholder_name("未知旅人（某频道）（某头衔）")
    assert not is_placeholder_name("未知学徒")
    assert not is_placeholder_name("浅陌QanMo")
    assert not is_placeholder_name("咪咪喵喵小茂密")


def test_alias_store_name_for_reverse_view() -> None:
    """name_for: uid -> latest non-placeholder name; placeholder names / pure digits never enter the reverse view."""
    store = AliasStore(db=None, variant_cap=8)
    store.apply_rows([
        {"platform": "qq", "user_id": "1", "name": "旧名",
         "last_seen": datetime(2026, 9, 1, tzinfo=UTC)},
        {"platform": "qq", "user_id": "1", "name": "新名",
         "last_seen": datetime(2026, 9, 2, tzinfo=UTC)},
        {"platform": "qq", "user_id": "2", "name": "未知用户",
         "last_seen": datetime(2026, 9, 3, tzinfo=UTC)},
        {"platform": "qq", "user_id": "3", "name": "1000003",
         "last_seen": datetime(2026, 9, 3, tzinfo=UTC)},
    ])
    assert store.name_for("qq", "1") == "新名"  # 最新者胜
    assert store.name_for("qq", "2") == ""      # 占位名不作规范名源
    assert store.name_for("qq", "3") == ""      # 纯数字不作规范名源
    assert store.name_for("qq", "404") == ""


# ---------------------------------------------------------------------------
#  db.alias_upsert SQL
# ---------------------------------------------------------------------------


class FakeConnPool:
    def __init__(self) -> None:
        self.batches: list[tuple[str, list]] = []

    def acquire(self):
        return self

    async def __aenter__(self):
        return self

    async def __aexit__(self, *args):
        return False

    async def executemany(self, sql: str, args) -> None:
        self.batches.append((sql, list(args)))

    async def fetch(self, sql: str, *args):
        return []


def _make_db(pool: FakeConnPool) -> MemoryDatabase:
    db = MemoryDatabase(LocalMemoryConfig(dsn="postgresql://x"))
    db._pool = pool  # noqa: SLF001 -- 注入桩池（对齐现有 db 层桩测姿势）
    return db


async def test_alias_upsert_sql_and_params() -> None:
    pool = FakeConnPool()
    db = _make_db(pool)
    await db.alias_upsert([
        {"platform": "qq", "user_id": "u1", "name": "琳妮特",
         "last_seen": datetime(2026, 9, 8, 12, 0), "source": "batch"},
    ])
    sql, payload = pool.batches[0]
    assert "INSERT INTO memory_entity_alias" in sql
    assert "ON CONFLICT (platform, user_id, name)" in sql
    assert "GREATEST(memory_entity_alias.last_seen, EXCLUDED.last_seen)" in sql
    assert payload[0][2] == "琳妮特"
    assert payload[0][4] == "batch"
    pool.batches.clear()
    await db.alias_upsert([])
    assert not pool.batches


# ---------------------------------------------------------------------------
#  kernel 两层合并
# ---------------------------------------------------------------------------


class _FakeEmbed:
    async def embed_one(self, text: str) -> Optional[list[float]]:
        return [0.1]


def _make_kernel(alias_enabled: bool = True):
    cfg = LocalMemoryConfig(
        dsn="postgresql://x", alias_enabled=alias_enabled, alias_stopwords=[]
    )

    async def directory_source(session_id: str):
        return [("小周", "qq", "u3003")]

    kernel = LocalMemoryKernel(
        db=None,
        embedding_service=_FakeEmbed(),
        circuit_breaker=MemoryDBCircuitBreaker(),
        config=cfg,
        bot_id="bot-1",
        directory_source=directory_source,
    )
    return kernel


async def test_entity_hint_entries_merges_alias_layer() -> None:
    kernel = _make_kernel()
    kernel.alias_store.apply_rows([
        {"platform": "qq", "user_id": "u3003", "name": "小周",
         "last_seen": datetime(2026, 9, 8, tzinfo=UTC)},
        {"platform": "qq", "user_id": "u1001", "name": "琳妮特",
         "last_seen": datetime(2026, 9, 8, tzinfo=UTC)},
    ])
    entries = await kernel.entity_hint_entries("s1", "小周约琳妮特周末爬山")
    pairs = [(p, u) for _, p, u in entries]
    assert ("qq", "u3003") in pairs
    assert ("qq", "u1001") in pairs
    assert pairs.count(("qq", "u3003")) == 1  # 窗口与持久不重复注入


async def test_entity_hint_entries_alias_disabled() -> None:
    kernel = _make_kernel(alias_enabled=False)
    assert kernel.alias_store is None
    entries = await kernel.entity_hint_entries("s1", "小周约爬山")
    assert [u for _, _, u in entries] == ["u3003"]


# ---------------------------------------------------------------------------
#  main._alias_upsert_batch（批次 sender 名字流）
# ---------------------------------------------------------------------------


class _FakeAliasDB:
    def __init__(self) -> None:
        self.upserted: list[dict] = []

    async def alias_upsert(self, rows: list[dict]) -> None:
        self.upserted.extend(rows)


def _bare_plugin(kernel=None, db=None):
    plugin = object.__new__(_main.NoriflowMemoryPlugin)
    plugin._history = OrderedDict()
    plugin._bot_user_id = "bot-1"
    plugin._config = LocalMemoryConfig()
    plugin._memory_kernel = kernel
    plugin._db = db
    return plugin


def _line(key, uid, nickname="", cardname="", platform="qq", is_bot=False,
          timestamp=None):
    return (key, _main._CachedLine(
        line="x", uid=uid, platform=platform, is_bot=is_bot,
        nickname=nickname, cardname=cardname, timestamp=timestamp,
    ))


async def test_alias_upsert_batch_wiring() -> None:
    kernel = _make_kernel()
    db = _FakeAliasDB()
    plugin = _bare_plugin(kernel=kernel, db=db)
    ts = datetime(2026, 9, 8, 10, 0)
    await plugin._alias_upsert_batch([
        _line("m1", uid="u1", nickname="琳妮特（笨蛋猫娘）", cardname="", timestamp=ts),
        _line("m2", uid="bot-1", nickname="bot名", timestamp=ts),   # bot 跳过
        _line("m3", uid="u2", nickname="", cardname="", timestamp=ts),  # 无名跳过
    ])
    names = [r["name"] for r in db.upserted]
    assert "琳妮特" in names and "琳妮特（笨蛋猫娘）" in names
    assert all(r["user_id"] == "u1" for r in db.upserted)
    # 内存视图同步（免整表重载即可命中）
    hits, _ = kernel.alias_store.match("琳妮特在吗", set())
    assert hits == [("琳妮特", "qq", "u1")]

    # fail-open：alias_upsert 抛异常不上抛
    class _BadDB(_FakeAliasDB):
        async def alias_upsert(self, rows):
            raise RuntimeError("db down")

    plugin2 = _bare_plugin(kernel=_make_kernel(), db=_BadDB())
    await plugin2._alias_upsert_batch([
        _line("m1", uid="u1", nickname="阿明", timestamp=ts),
    ])  # 不抛即通过

    # kernel 未装配 / store 关闭：直接返回
    plugin3 = _bare_plugin(kernel=None, db=db)
    await plugin3._alias_upsert_batch([_line("m1", uid="u1", nickname="阿明")])
    assert not db.upserted or all(r["name"] != "阿明" for r in db.upserted)


# ---------------------------------------------------------------------------
#  归一化匹配（NFKC + casefold）与事实代号别名
# ---------------------------------------------------------------------------


def test_normalize_alias_text() -> None:
    # 名片 undefined𝕩𝕩𝕪（𝕩=U+1D569）NFKC 后即 undefinedxxy——含 "xxy"
    assert normalize_alias_text("undefined𝕩𝕩𝕪") == "undefinedxxy"
    # 全角字母/数字/括号转半角；大小写收敛
    assert normalize_alias_text("ＸＹＺ（测试）") == "xyz(测试)"
    assert normalize_alias_text("Shizuku") == "shizuku"
    assert normalize_alias_text("") == ""


def test_alias_store_match_unicode_variants() -> None:
    """Card-name Unicode variants vs ASCII/half-width text forms: substring hit after normalization."""
    store = _store_with({
        "𝕩𝕩𝕪": [("qq", "3429924750")],  # 数学字母名片（NFKC 后即 xxy）
        "Ｓｈｉｚｕｋｕ": [("qq", "u2")],  # 全角名片
        "Shizuku": [("qq", "u3")],  # 与上行归一化后同名（半角异大小写）
    })
    hits, _ = store.match("seki还记得xxy吗", set())
    assert ("𝕩𝕩𝕪", "qq", "3429924750") in hits
    assert all(h[0] != "xxy" for h in hits)  # 展示名保留名片原样
    hits, skipped = store.match("shizuku 在吗", set())
    assert hits == []
    assert skipped in (["Ｓｈｉｚｕｋｕ"], ["Shizuku"])


def test_extract_uid_code_names() -> None:
    """Extraction of "user<uid>(<codename>)" forms: full/half-width brackets, multi-pair dedup, uid length floor."""
    stmt = "与用户3429924750（xxy）关系亲密，同时与用户2374893963(忆熙阿)互动频繁。"
    assert extract_uid_code_names(stmt) == [
        ("3429924750", "xxy"),
        ("2374893963", "忆熙阿"),
    ]
    assert extract_uid_code_names("用户10001（阿明）和用户10001（阿明）") == [
        ("10001", "阿明")
    ]
    assert extract_uid_code_names("用户123（小明）") == []  # uid 不足 4 位
    assert extract_uid_code_names("用户10001 阿明") == []  # 括号缺失
    # 代号保留原文形态（大小写/全角——DB 与展示用原名，归一化只作匹配键）
    assert extract_uid_code_names("用户10001（Kai）是熟人") == [("10001", "Kai")]
    assert extract_uid_code_names("用户10001（Ｋａｉ）是熟人") == [("10001", "Ｋａｉ")]
    assert extract_uid_code_names("用户１０００１（阿明）") == [("10001", "阿明")]  # uid 全角归一


def test_build_fact_code_alias_rows() -> None:
    """Codename row build: cleaning + placeholder filtering + same-key keeps later last_seen."""
    ts_old = datetime(2026, 9, 1, tzinfo=timezone.utc)
    ts_new = datetime(2026, 9, 11, tzinfo=timezone.utc)
    rows = build_fact_code_alias_rows([
        ("qq", "与用户3429924750（xxy）关系亲密。", ts_old),
        ("qq", "用户3429924750（xxy）今天上线了", ts_new),
        ("qq", "用户10002（undefined）说话奇怪", ts_old),
        ("qq", "用户10003（！！）刷屏", ts_old),
        ("", "用户10004（阿明）", ts_old),
    ])
    assert rows == [{
        "platform": "qq",
        "user_id": "3429924750",
        "name": "xxy",
        "last_seen": ts_new,
        "source": "fact",
    }]
    # 同归一化键多形态：行 name 保留首个观测原文，last_seen 取更晚
    rows = build_fact_code_alias_rows([
        ("qq", "用户10001（Kai）今天上线", ts_old),
        ("qq", "用户10001（kai）又上线", ts_new),
    ])
    assert rows == [{
        "platform": "qq",
        "user_id": "10001",
        "name": "Kai",
        "last_seen": ts_new,
        "source": "fact",
    }]
    # last_seen=None（SQLite 标量 MAX/时间戳解析失败路径）不崩，视作最旧
    rows = build_fact_code_alias_rows([
        ("qq", "用户10006（阿明）先出现", None),
        ("qq", "用户10006（阿明）后出现", ts_new),
    ])
    assert rows == [{
        "platform": "qq",
        "user_id": "10006",
        "name": "阿明",
        "last_seen": ts_new,
        "source": "fact",
    }]


def test_alias_store_fact_code_alias_end_to_end() -> None:
    """After fact-codename rows apply: text hit -> (display, platform, uid)."""
    store = AliasStore(db=None, variant_cap=8)
    store.apply_rows([{
        "platform": "qq", "user_id": "3429924750", "name": "xxy",
        "last_seen": datetime(2026, 9, 11, tzinfo=timezone.utc),
        "source": "fact",
    }])
    hits, skipped = store.match("seki还记得xxy吗", set())
    assert hits == [("xxy", "qq", "3429924750")]
    assert skipped == []


def test_alias_store_display_per_pair() -> None:
    """Multiple uids under the same normalized key: each hit carries its own observed original name, no cross-grafting another person's address term."""
    store = AliasStore(db=None, variant_cap=8)
    store.apply_rows([
        {"platform": "qq", "user_id": "20001", "name": "Shizuku",
         "last_seen": datetime(2026, 9, 11, tzinfo=timezone.utc),
         "source": "batch"},
        {"platform": "qq", "user_id": "20002", "name": "Ｓｈｉｚｕｋｕ",
         "last_seen": datetime(2026, 9, 12, tzinfo=timezone.utc),
         "source": "batch"},
    ])
    hits, skipped = store.match(
        "shizuku 在吗", {("qq", "20001"), ("qq", "20002")}
    )
    assert skipped == []
    assert sorted(hits) == sorted([
        ("Shizuku", "qq", "20001"),
        ("Ｓｈｉｚｕｋｕ", "qq", "20002"),
    ])


async def test_entity_directory_display_per_pair() -> None:
    """Session entity directory, multiple uids under the same normalized key: each hit carries its own observed original name, no cross-grafting
    (same semantics as AliasStore._display_by_pair)."""
    async def source(session_id: str):
        return [("Shizuku", "qq", "400"), ("Ｓｈｉｚｕｋｕ", "web", "500")]

    d = _EntityDirectory(source)
    hits = await d.match("s1", "shizuku 在吗")
    assert sorted(hits) == sorted([
        ("Shizuku", "qq", "400"),
        ("Ｓｈｉｚｕｋｕ", "web", "500"),
    ])


class _RefreshDbStub:
    async def alias_fetch_all(self):
        return [
            {"platform": "qq", "user_id": 40001, "name": "Shizuku",
             "last_seen_epoch": 100.0},
            {"platform": "qq", "user_id": "40002", "name": "Ｓｈｉｚｕｋｕ",
             "last_seen_epoch": 200.0},
        ]


async def test_alias_store_refresh_display_by_pair() -> None:
    """The refresh path (TTL full-table reload) shares apply_rows semantics: uids normalized to str,
       same-key multi-uid display names match per-pair — when the DB returns integer uids it never falls back to key-level cross-grafting."""
    store = AliasStore(db=_RefreshDbStub(), variant_cap=8)
    await store.refresh_if_due(force=True)
    hits, skipped = store.match(
        "shizuku 在吗", {("qq", "40001"), ("qq", "40002")}
    )
    assert skipped == []
    assert sorted(hits) == sorted([
        ("Shizuku", "qq", "40001"),
        ("Ｓｈｉｚｕｋｕ", "qq", "40002"),
    ])


# ---------------------------------------------------------------------------
#  运行器
# ---------------------------------------------------------------------------


async def main() -> None:
    test_clean_alias_name_filters()
    test_normalize_alias_text()
    test_alias_store_match_unicode_variants()
    test_extract_uid_code_names()
    test_build_fact_code_alias_rows()
    test_alias_store_fact_code_alias_end_to_end()
    test_alias_store_display_per_pair()
    test_split_name_variants_brackets()
    test_build_alias_rows_cap_and_dedup()
    test_alias_store_match_basic_and_ambiguity()
    test_alias_store_stopwords()
    test_alias_store_apply_rows_last_seen_monotonic()
    test_is_placeholder_name()
    test_alias_store_name_for_reverse_view()
    await test_alias_upsert_sql_and_params()
    await test_entity_hint_entries_merges_alias_layer()
    await test_entity_hint_entries_alias_disabled()
    await test_entity_directory_display_per_pair()
    await test_alias_store_refresh_display_by_pair()
    await test_alias_upsert_batch_wiring()
    print("ALL PASS")


if __name__ == "__main__":
    sys.exit(asyncio.run(main()))
