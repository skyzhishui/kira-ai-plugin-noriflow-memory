"""持久实体别名层（memory_entity_alias 表的内存匹配视图 + 写入辅助）。

职责（P1 别名层，设计见 2026-09 关系图谱方案 P1）：
- 变体拆分与清洗：括号后缀昵称拆主干/内段（"琳妮特（笨蛋猫娘）" ->
  "琳妮特（笨蛋猫娘）"/"琳妮特"/"笨蛋猫娘"），过滤单字/符号名与
  "用户<数字>" fallback 名；
- 内存匹配：name in text 子串包含（等价于对输入跑 %name%，纯内存
  零 DB）；重名歧义时窗口词典命中优先，仍歧义则跳过（确定性优先于
  召回，跳过名单进 debug 日志供审计）；
- apply_rows：写入侧（批次 upsert / 宿主 messages 对账）落库后同步
  内存视图，免整表重载；TTL 整表重载仅作跨写入口的自愈。

名字来源（两侧同构：消息流 -> 本表，均不读宿主用户表）：
- 上游 nori 版：宿主 messages 表回填 + TTL 增量对账（alias_sync.py）；
- KiraAI：回合批次 sender upsert（main.py 回合完成信号处理内）。
"""

from __future__ import annotations

import re
import time
from datetime import datetime, timezone
from typing import Iterable, Optional

try:  # kira host: route through the host logging manager so records reach
    # data/log.log (plain std logging is filtered out by the host's
    # GetLoggerFilter and stays invisible).
    from core.logging_manager import get_logger as _get_logger

    logger = _get_logger("noriflow_memory.alias", "cyan")
except ImportError:  # kira 宿主（模块与上游 nori 版保持同构）
    import logging

    logger = logging.getLogger(__name__)

# 括号对（全角/半角混用均可命中），拆主干与内段
_BRACKETS_OPEN = "（(【〔[《<"
_BRACKETS_CLOSE = "）)】〕]》>"
_FALLBACK_NAME_RE = re.compile(r"^用户\d+$")


def _word_chars(name: str) -> int:
    """CJK + 字母数字字符数（符号/空白/装饰符不计）。"""
    return sum(
        1
        for ch in name
        if ch.isalnum() and not ch.isspace()
    )


def clean_alias_name(name: str) -> str:
    """清洗单个候选名：去空白；无效（<2 个词字符、"用户\\d+" fallback）返回空串。"""
    cleaned = (name or "").strip()
    if len(cleaned) < 2:
        return ""
    if _FALLBACK_NAME_RE.match(cleaned):
        return ""
    if _word_chars(cleaned) < 2:
        return ""
    return cleaned


# 占位名精确形（unknown/undefined 为 LLM 输出泄漏词形）；纯数字 = LLM 拿
# uid 当名字；"未知"系只收 未知/未知用户 及其数字编号变体——前缀泛匹配
# 会误伤真名（实测有群成员的真实昵称即以"未知"开头）
_PLACEHOLDER_EXACT = frozenset({"unknown", "undefined"})
_PLACEHOLDER_RE = re.compile(r"^(?:用户\d+|\d+|未知(?:用户)?\d*)$")

# SQL 单源常量：占位名规则的 POSIX 正则（db 层 upsert 守卫 / 结构自检
# stale_names 三处 SQL 内联副本一律由此常量插值——Python 判定与 SQL 判定
# 永不漂移，这正是本文件 docstring 警示的"双实现失守"面的根治）。
PLACEHOLDER_NAME_SQL = r"^(unknown|undefined|用户[0-9]+|[0-9]+|未知(用户)?[0-9]*)$"


def is_placeholder_name(name: str) -> bool:
    """判定占位名：空/未知[用户][数字]/unknown/undefined/用户\\d+/纯数字。

    与 clean_alias_name 的分工：后者管别名表准入（消息流里真有人这么
    起名），本函数管"能否作实体规范显示名"——"未知"能过清洗但不是
    可用的规范名，故独立判定。读侧规范名解析与写侧守卫共用同一裁决。

    【单源警示】本函数与 PLACEHOLDER_NAME_SQL（db 层 SQL 副本的插值源）
    是同一规则的两态实现，必须同步演化——任一侧新增占位形态时另一侧
    静默失守。
    """
    cleaned = (name or "").strip()
    if not cleaned:
        return True
    if cleaned.lower() in _PLACEHOLDER_EXACT:
        return True
    return bool(_PLACEHOLDER_RE.match(cleaned))


def split_name_variants(name: str) -> list[str]:
    """拆分名字变体：整串 + 括号外主干 + 各括号内段（均过清洗，去重保序）。

    "梦瑶月（月月猫）[群低性能bot]" -> 整串 / "梦瑶月" / "月月猫" /
    "群低性能bot"（主干为全部括号外段拼接——常见单段，多段拼接保语义）。
    """
    raw = (name or "").strip()
    if not raw:
        return []
    variants: list[str] = []
    outer: list[str] = []
    inner: list[str] = []
    buf: list[str] = []
    depth = 0

    def _flush_outer() -> None:
        seg = "".join(buf).strip()
        if seg:
            outer.append(seg)
        buf.clear()

    for ch in raw:
        if ch in _BRACKETS_OPEN:
            if depth == 0:
                _flush_outer()
            depth += 1
        elif ch in _BRACKETS_CLOSE and depth > 0:
            depth -= 1
            if depth == 0:
                seg = "".join(buf).strip()
                if seg:
                    inner.append(seg)
                buf.clear()
        else:
            buf.append(ch)
    if depth > 0:
        # 未闭合括号（截断名片）：缓冲区按外段处理
        _flush_outer()
    else:
        _flush_outer()

    for candidate in [raw, " ".join(outer).replace(" ", ""), *outer, *inner]:
        # 多段外名（"傲娇喵  (猫娘)" 中空格归一）后统一清洗
        cleaned = clean_alias_name(candidate)
        if cleaned and cleaned not in variants:
            variants.append(cleaned)
    return variants


def build_alias_rows(
    raw: Iterable[tuple[str, str, str, datetime]],
    *,
    source: str,
    variant_cap: int,
) -> list[dict]:
    """原始名观测 -> 别名行（变体拆分 + 每用户上限）。

    Args:
        raw: (platform, uid, 原始名, last_seen) 任意顺序、可重复——
            同名取更晚 last_seen。
        source: 写入来源标记（backfill | reconcile | batch）。
        variant_cap: 每 (platform, uid) 保留的变体上限（按 last_seen
            降序截断——状态播报式名片的十几个变体在此收敛）。

    Returns:
        别名行列表（platform/user_id/name/last_seen/source）。
    """
    by_owner: dict[tuple[str, str], dict[str, datetime]] = {}
    for platform, uid, name, last_seen in raw:
        if not platform or not uid or not name:
            continue
        for variant in split_name_variants(name):
            bucket = by_owner.setdefault((platform, uid), {})
            if variant not in bucket or last_seen > bucket[variant]:
                bucket[variant] = last_seen
    rows: list[dict] = []
    for (platform, uid), variants in by_owner.items():
        ranked = sorted(variants.items(), key=lambda kv: kv[1], reverse=True)
        for variant, last_seen in ranked[: max(variant_cap, 1)]:
            rows.append({
                "platform": platform,
                "user_id": uid,
                "name": variant,
                "last_seen": last_seen,
                "source": source,
            })
    return rows


def _to_epoch(ts: object) -> float:
    """datetime/timestamp -> epoch 秒（naive 按 UTC 补齐；缺失返回 0）。"""
    if isinstance(ts, datetime):
        if ts.tzinfo is None:
            ts = ts.replace(tzinfo=timezone.utc)
        return ts.timestamp()
    try:
        return float(ts)
    except (TypeError, ValueError):
        return 0.0


class AliasStore:
    """持久别名内存视图：加载/匹配/写后同步（reply 路径零 DB 依赖）。"""

    _REFRESH_TTL_SECONDS = 600.0

    def __init__(
        self,
        db=None,
        *,
        variant_cap: int = 8,
        stopwords: Optional[Iterable[str]] = None,
    ) -> None:
        """Args: db 为 None 时仅 apply_rows 驱动的内存视图可用（测试用）。"""
        self._db = db
        self._variant_cap = variant_cap
        self._stopwords = {w for w in (stopwords or []) if w}
        # name -> [(platform, uid, last_seen epoch)]
        self._names: dict[str, list[tuple[str, str, float]]] = {}
        # (platform, uid) -> (最新非占位名, epoch)——写侧守卫/读侧规范名
        # 解析的反向视图，与 _names 同源同步（refresh/apply 两口）
        self._by_owner: dict[tuple[str, str], tuple[str, float]] = {}
        self._loaded_at = 0.0

    @property
    def size(self) -> int:
        """内存视图中的名字数（监控/日志用）。"""
        return len(self._names)

    @property
    def variant_cap(self) -> int:
        """每用户变体上限（写入侧构造 build_alias_rows 参数用）。"""
        return self._variant_cap

    async def refresh_if_due(self, force: bool = False) -> None:
        """TTL 整表重载（自愈跨写入口；表小全量拉取，成本毫秒级）。"""
        if self._db is None:
            return
        now = time.monotonic()
        if not force and self._names and now - self._loaded_at < self._REFRESH_TTL_SECONDS:
            return
        rows = await self._db.alias_fetch_all()
        names: dict[str, list[tuple[str, str, float]]] = {}
        by_owner: dict[tuple[str, str], tuple[str, float]] = {}
        for row in rows:
            name = row["name"]
            if not name or name in self._stopwords:
                continue
            epoch = _to_epoch(row.get("last_seen_epoch"))
            names.setdefault(name, []).append(
                (row["platform"], row["user_id"], epoch)
            )
            pair = (row["platform"], str(row["user_id"]))
            if is_placeholder_name(name):
                continue
            prev = by_owner.get(pair)
            if prev is None or epoch > prev[1]:
                by_owner[pair] = (name, epoch)
        self._names = names
        self._by_owner = by_owner
        self._loaded_at = now
        logger.info("持久别名视图已加载: %d 名", len(names))

    def apply_rows(self, rows: list[dict]) -> None:
        """写入侧落库后同步内存（增量，免整表重载）。"""
        for row in rows:
            name = row.get("name") or ""
            if not name or name in self._stopwords:
                continue
            pair = (row.get("platform") or "", str(row.get("user_id") or ""))
            if not pair[1]:
                continue
            epoch = _to_epoch(row.get("last_seen"))
            bucket = self._names.setdefault(name, [])
            for idx, (p, u, ts) in enumerate(bucket):
                if (p, u) == pair:
                    if epoch > ts:
                        bucket[idx] = (p, u, epoch)
                    break
            else:
                bucket.append((pair[0], pair[1], epoch))
            if is_placeholder_name(name):
                continue
            prev = self._by_owner.get(pair)
            if prev is None or epoch > prev[1]:
                self._by_owner[pair] = (name, epoch)

    def name_for(self, platform: str, uid: str) -> str:
        """uid -> 最新非占位别名（反向视图；未命中返回空串）。

        写侧占位名守卫的解析源：LLM 输出"未知"/uid 兜底形时用该 uid 的
        最新可用别名顶替，未命中留空（由 upsert 语句保旧名，读侧图谱
        解析再兜底）。与 _names 同源同步，纯内存零 DB。
        """
        hit = self._by_owner.get((platform or "", str(uid or "")))
        return hit[0] if hit else ""

    def match(
        self, text: str, window_pairs: set[tuple[str, str]]
    ) -> tuple[list[tuple[str, str, str]], list[str]]:
        """子串包含匹配（name in text）。

        歧义裁决：名字对应多个 (platform, uid) 时——窗口词典（会话内
        权威）命中者优先；无窗口背书且仍多义则跳过并记入返回值第二项
        （确定性优先于召回；不按 recency 猜测，跳过名单供 debug 审计）。

        Args:
            text: 本轮匹配文本（含 @昵称 渲染的 plain_text）。
            window_pairs: 窗口词典本轮命中的 (platform, uid) 集合。

        Returns:
            (命中条目 [(名字, platform, uid)], 歧义跳过的名字列表)。
        """
        if not text:
            return [], []
        hits: list[tuple[str, str, str]] = []
        skipped: list[str] = []
        for name, entries in self._names.items():
            if name in self._stopwords or name not in text:
                continue
            pairs: list[tuple[str, str]] = []
            for platform, uid, _ in entries:
                if (platform, uid) not in pairs:
                    pairs.append((platform, uid))
            window_backed = [p for p in pairs if p in window_pairs]
            if window_backed:
                chosen = window_backed
            elif len(pairs) == 1:
                chosen = pairs
            else:
                skipped.append(name)
                continue
            for platform, uid in chosen:
                hits.append((name, platform, uid))
        return hits, skipped
