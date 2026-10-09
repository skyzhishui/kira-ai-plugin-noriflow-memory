"""Persistent entity alias layer (in-memory match view over the memory_entity_alias table plus write helpers).

Responsibilities (P1 alias layer, design per the 2026-09 relation graph plan P1):
- Variant splitting and cleaning: split bracket-suffixed nicknames into stem/inner
  ("Lynette (silly catgirl)" -> "Lynette (silly catgirl)"/"Lynette"/"silly catgirl"); filter out
  single-char/symbol names and "User<digits>" fallback names;
- In-memory matching: name in text substring containment (equivalent to running %name% over the
  input, pure memory, zero DB); on duplicate-name ambiguity the window dictionary hit wins,
  still ambiguous then skip (determinism over recall, skip list goes into debug logs for audit);
- apply_rows: after the write side (batch upsert / host messages reconciliation) persists rows,
  syncs the in-memory view to avoid full-table reloads; TTL full-table reloads only serve as
  self-healing across write entries.

Name sources (homogeneous on both sides: message stream -> this table, neither reads the host user table):
- Upstream nori version: host messages table backfill + TTL incremental reconciliation (alias_sync.py);
- KiraAI: per-round batch sender upsert (inside the main.py round-completion signal handler).
"""

from __future__ import annotations

import re
import time
import unicodedata
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


def normalize_alias_text(text: str) -> str:
    """Match view normalization: NFKC + casefold.

    Group card names often contain Unicode math letters (the 𝕩=U+1D569 in "undefined𝕩𝕩𝕪") and
    full-width forms, while chat text is usually typed in ASCII/half-width ("xxy"): different code
    points mean substring matching always fails. NFKC maps 𝕩->x and full-width->half-width, casefold
    unifies case; both sides (dictionary keys and matched text) pass through this function, so
    byte-level ambiguity disappears in front of matching.

    DB storage keeps the original name (canonical name/persona title shown as-is); normalization
    only happens in the in-memory match view (load and match entries) and is unrelated to the
    alias_upsert write semantics.
    """
    return unicodedata.normalize("NFKC", text or "").casefold()


# 事实陈述中的「用户<uid>（<代号>）」模式（原文直接匹配，括号全角/
# 半角通吃；uid 4 位起防误伤泛指；代号不含括号、长度有界防 LLM 长句
# 误裹）。典型来源：编码提示词要求关系陈述写
# 「用户3429924750（xxy）关系亲密」——括号内代号是群聊真实称呼，
# 与名片可能完全不同形（名片 undefined𝕩𝕩𝕪 vs 群称 xxy）。
_FACT_CODE_RE = re.compile(r"用户(\d{4,})\s*[（(]\s*([^()（）]{1,24}?)\s*[）)]")


def extract_uid_code_names(statement: str) -> list[tuple[str, str]]:
    """Extract (uid, code) pairs matching the "User<uid> (code)" pattern from fact statements.

    Extraction is direct on the original text (full-width/half-width brackets both work); the code
    keeps its original form: DB storage and display use the original name (the normalize_alias_text
    storage convention), normalization only applies to the uid (NFKC unifies half-width digits) and
    to match keys. Same-form repeats are deduplicated preserving order. Raw forms are not validated:
    cleaning and placeholder-name verdicts are unified in build_fact_code_alias_rows.
    """
    pairs: list[tuple[str, str]] = []
    for uid, code in _FACT_CODE_RE.findall(statement or ""):
        uid = normalize_alias_text(uid)
        if (uid, code) not in pairs:
            pairs.append((uid, code))
    return pairs


def _later_ts(a: Optional[datetime], b: Optional[datetime]) -> bool:
    """None-safe chronological comparison: None counts as the oldest.

    Missing/unparseable time values must not make the comparison raise TypeError: SQLite scalar MAX
    returns NULL if any argument is NULL (unlike PG GREATEST, which ignores NULL), and old-format
    TEXT data drift also flows into None via sqlite_parse_ts.
    """
    if a is None:
        return False
    if b is None:
        return True
    return a > b


def build_fact_code_alias_rows(
    sources: Iterable[tuple[str, str, datetime]],
    *,
    source: str = "fact",
) -> list[dict]:
    """Fact statement codes -> alias rows (stream of (platform, statement, last_seen)).

    Extracted codes pass through a double guard: clean_alias_name (cleaning) and is_placeholder_name
    (blocks LLM poison output like unknown/undefined/pure digits, decided on the normalized form);
    deduplicated by (platform, uid, normalized name): the row name keeps the first observed original
    (case/full-width form, shown as-is), last_seen takes the later one (None counts as the oldest,
    see _later_ts).

    Returns:
        List of alias rows (platform/user_id/name/last_seen/source) for db.alias_upsert to
        persist and AliasStore.apply_rows to sync in memory.
    """
    by_key: dict[tuple[str, str, str], tuple[str, Optional[datetime]]] = {}
    for platform, statement, last_seen in sources:
        if not platform or not statement:
            continue
        for uid, code in extract_uid_code_names(statement):
            name = clean_alias_name(code)
            if not name:
                continue
            key = (platform, uid, normalize_alias_text(name))
            if is_placeholder_name(key[2]):
                continue
            prev = by_key.get(key)
            if prev is None:
                by_key[key] = (name, last_seen)
            elif _later_ts(last_seen, prev[1]):
                by_key[key] = (prev[0], last_seen)
    return [
        {
            "platform": platform,
            "user_id": uid,
            "name": name,
            "last_seen": last_seen,
            "source": source,
        }
        for (platform, uid, _), (name, last_seen) in by_key.items()
    ]


def _word_chars(name: str) -> int:
    """Count of CJK plus alphanumeric characters (symbols/whitespace/decorators not counted)."""
    return sum(
        1
        for ch in name
        if ch.isalnum() and not ch.isspace()
    )


def clean_alias_name(name: str) -> str:
    """Clean a single candidate name: strip whitespace; return empty string when invalid (< 2 word chars, "User\\d+" fallback)."""
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
    """Judge placeholder names: empty/"unknown[user][digits]"/unknown/undefined/User\\d+/pure digits.

    Division of labor with clean_alias_name: the latter governs alias table admission (some people in
    the message stream really do use such names), this function governs whether a name can serve as a
    canonical entity display name: "unknown" passes cleaning but is not a usable canonical name, hence
    the separate judgment. Read-side canonical-name resolution and the write-side guard share the same
    verdict.

    [Single-source warning] This function and PLACEHOLDER_NAME_SQL (the interpolation source of the
    db-layer SQL copies) are two stateful implementations of the same rule and must evolve in
    lockstep: when either side adds a placeholder form the other side goes silently stale.
    """
    cleaned = (name or "").strip()
    if not cleaned:
        return True
    if cleaned.lower() in _PLACEHOLDER_EXACT:
        return True
    return bool(_PLACEHOLDER_RE.match(cleaned))


def split_name_variants(name: str) -> list[str]:
    """Split name variants: full string + stem outside brackets + each inner bracketed segment (all pass cleaning, deduplicated preserving order).

    "Yueyao (moon cat) [low-perf group bot]" -> full string / "Yueyao" / "moon cat" /
    "low-perf group bot" (the stem is the concatenation of all outer segments: commonly a single
    segment, multi-segment concatenation preserves semantics).
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
    """Raw name observations -> alias rows (variant splitting + per-user cap).

    Args:
        raw: (platform, uid, original name, last_seen) in any order, repeats allowed:
            the same name takes the later last_seen.
        source: write source marker (backfill | reconcile | batch).
        variant_cap: per (platform, uid) variant cap kept (truncated by last_seen descending:
            the dozen-plus variants of status-broadcast cards converge here).

    Returns:
        List of alias rows (platform/user_id/name/last_seen/source).
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
    """datetime/timestamp -> epoch seconds (naive treated as UTC; missing returns 0)."""
    if isinstance(ts, datetime):
        if ts.tzinfo is None:
            ts = ts.replace(tzinfo=timezone.utc)
        return ts.timestamp()
    try:
        return float(ts)
    except (TypeError, ValueError):
        return 0.0


class AliasStore:
    """Persistent alias in-memory view: load/match/post-write sync (zero DB dependency on the reply path)."""

    _REFRESH_TTL_SECONDS = 600.0

    def __init__(
        self,
        db=None,
        *,
        variant_cap: int = 8,
        stopwords: Optional[Iterable[str]] = None,
    ) -> None:
        """Args: when db is None only the apply_rows-driven in-memory view is available (for tests)."""
        self._db = db
        self._variant_cap = variant_cap
        # 停用词同样以归一化形态比对（视图键是归一化名，口径一致）
        self._stopwords = {normalize_alias_text(w) for w in (stopwords or []) if w}
        # 归一化名 -> [(platform, uid, last_seen epoch)]（匹配键 = NFKC +
        # casefold；DB 与展示保留原名，见 normalize_alias_text）
        self._names: dict[str, list[tuple[str, str, float]]] = {}
        # 归一化名 -> 首个观测原名（命中条目的展示名，画像标题与聊天
        # 称呼一致；同键多原名时保最先入视图者）
        self._display: dict[str, str] = {}
        # (归一化名, platform, uid) -> 该 uid 自己的观测原名——同键多 uid
        # 时命中条目各带各的称呼，不串贴他人原名（_display 仅作键级兜底
        # 与歧义跳过名单展示）
        self._display_by_pair: dict[tuple[str, str, str], str] = {}
        # (platform, uid) -> (最新非占位名, epoch)——写侧守卫/读侧规范名
        # 解析的反向视图，与 _names 同源同步（refresh/apply 两口）
        self._by_owner: dict[tuple[str, str], tuple[str, float]] = {}
        self._loaded_at = 0.0

    @property
    def size(self) -> int:
        """Number of names in the in-memory view (for monitoring/logs)."""
        return len(self._names)

    @property
    def variant_cap(self) -> int:
        """Per-user variant cap (the parameter the write side passes to build_alias_rows)."""
        return self._variant_cap

    async def refresh_if_due(self, force: bool = False) -> None:
        """TTL full-table reload (self-heals across write entries; the table is small so a full fetch costs milliseconds)."""
        if self._db is None:
            return
        now = time.monotonic()
        if not force and self._names and now - self._loaded_at < self._REFRESH_TTL_SECONDS:
            return
        rows = await self._db.alias_fetch_all()
        names: dict[str, list[tuple[str, str, float]]] = {}
        display: dict[str, str] = {}
        display_by_pair: dict[tuple[str, str, str], str] = {}
        by_owner: dict[tuple[str, str], tuple[str, float]] = {}
        for row in rows:
            name = row["name"]
            if not name:
                continue
            key = normalize_alias_text(name)
            if not key or key in self._stopwords:
                continue
            epoch = _to_epoch(row.get("last_seen_epoch"))
            # uid 统一 str（与 apply_rows/_by_owner 及 _display_by_pair 键同
            # 口径）——DB 返回非 str uid（整数列）时 match 以 _names 原值查
            # _display_by_pair 会落空回退键级兜底，串贴复发
            pair = (row["platform"], str(row["user_id"]))
            names.setdefault(key, []).append((pair[0], pair[1], epoch))
            if key not in display:
                display[key] = name
            display_by_pair.setdefault((key, pair[0], pair[1]), name)
            if is_placeholder_name(name):
                continue
            prev = by_owner.get(pair)
            if prev is None or epoch > prev[1]:
                by_owner[pair] = (name, epoch)
        self._names = names
        self._display = display
        self._display_by_pair = display_by_pair
        self._by_owner = by_owner
        self._loaded_at = now
        logger.info("持久别名视图已加载: %d 名", len(names))

    def apply_rows(self, rows: list[dict]) -> None:
        """Sync in-memory state after the write side persists rows (incremental, no full-table reload)."""
        for row in rows:
            name = row.get("name") or ""
            if not name:
                continue
            key = normalize_alias_text(name)
            if not key or key in self._stopwords:
                continue
            pair = (row.get("platform") or "", str(row.get("user_id") or ""))
            if not pair[1]:
                continue
            epoch = _to_epoch(row.get("last_seen"))
            bucket = self._names.setdefault(key, [])
            for idx, (p, u, ts) in enumerate(bucket):
                if (p, u) == pair:
                    if epoch > ts:
                        bucket[idx] = (p, u, epoch)
                    break
            else:
                bucket.append((pair[0], pair[1], epoch))
            self._display.setdefault(key, name)
            self._display_by_pair.setdefault((key, pair[0], pair[1]), name)
            if is_placeholder_name(name):
                continue
            prev = self._by_owner.get(pair)
            if prev is None or epoch > prev[1]:
                self._by_owner[pair] = (name, epoch)

    def name_for(self, platform: str, uid: str) -> str:
        """uid -> latest non-placeholder alias (reverse view; empty string when not found).

        Resolution source for the write-side placeholder-name guard: when the LLM outputs an
        "unknown"/uid fallback form, substitute this uid's latest usable alias; leave empty when not
        found (the upsert statement keeps the old name, and read-side graph resolution falls back
        again). Kept in sync from the same source as _names, pure memory, zero DB.
        """
        hit = self._by_owner.get((platform or "", str(uid or "")))
        return hit[0] if hit else ""

    def match(
        self, text: str, window_pairs: set[tuple[str, str]]
    ) -> tuple[list[tuple[str, str, str]], list[str]]:
        """Substring containment match (normalized name in normalized text).

        Both sides pass through normalize_alias_text (NFKC + casefold): Unicode math letters /
        full-width forms in card names and the ASCII/half-width spellings in chat text differ at the
        byte level yet are semantically the same name: substring matching can only hit after
        normalization (DB original names stay untouched, only the in-memory match view is normalized,
        see normalize_alias_text).

        Ambiguity resolution: when a name maps to multiple (platform, uid) pairs, the window
        dictionary (in-session authority) hit takes precedence; with no window endorsement and still
        ambiguous, the name is skipped and recorded into the second return value (determinism over
        recall; no recency guessing, the skip list is for debug audit).

        Args:
            text: current round match text (includes plain_text rendered for @nickname).
            window_pairs: set of (platform, uid) hits from the window dictionary this round.

        Returns:
            (Hit entries [(name, platform, uid)], names skipped due to ambiguity).
            Names are the display originals for the key (not the normalized form).
        """
        if not text:
            return [], []
        norm_text = normalize_alias_text(text)
        if not norm_text:
            return [], []
        hits: list[tuple[str, str, str]] = []
        skipped: list[str] = []
        for key, entries in self._names.items():
            if key in self._stopwords or key not in norm_text:
                continue
            display = self._display.get(key, key)
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
                skipped.append(display)
                continue
            for platform, uid in chosen:
                pair_display = self._display_by_pair.get(
                    (key, platform, uid), display
                )
                hits.append((pair_display, platform, uid))
        return hits, skipped
