"""LocalPersonaService: deterministic persona assembly from cluster table + profile table (M5, dev plan section 9.3).

Design (essential difference from hindsight):
- persona == a **deterministic projection** of table content: field
  template (8 dimensions + conditional "other names" slot) + fixed ordering
  (score DESC -> updated_at DESC -> id ASC) + per-slot cap; identical table
  content produces byte-identical persona text; no LLM involvement, no
  full rewrite, persona change iff cluster state changes;
- data source: the first five slots come from the profile table (projection
  rows of profiled clusters); the pending-info slot from the cluster table
  (pending_uncertain plus halved replaced scores, so swaying/degraded
  content stays visible to the bot);
- injection triple (header/disclaimer/footer) is verbatim-aligned with
  hindsight_persona; consuming side (MemoryStage -> planner/replyer) switches
  without noticing;
- write-interface semantics adaptation: update_profile / update_name /
  ensure_profile_* are all no-ops (the scoring state machine is the only
  persona change channel, external direct writes would break determinism);
  ensure_profile_model returns None (the local backend has no entity to
  create; the table is the persona).
"""

from __future__ import annotations

from core.logging_manager import get_logger

from datetime import datetime, timezone
from typing import Optional

from .config import LocalMemoryConfig
from .contracts import PersonaCandidate, PersonProfile
from .db import MemoryDatabase
from .time_labels import memory_time_label, resolve_local_tz

logger = get_logger("noriflow_memory.persona", "cyan")

# 注入文本骨架三件套：逐字对齐 hindsight_persona（消费侧跨插件契约）。
INJECTION_HEADER = "# 用户画像-背景信息"
INJECTION_DISCLAIMER = "以下记录属于内部推理素材，回复时请概括转述，不要逐字照读。"
INJECTION_FOOTER = "把它当作理解对方的一份额外参考即可；一旦与当前对话冲突，以当前对话为准。"

# 栏位固定顺序：(category, 栏名, PersonProfile 字段名)。
# 字段名是上游 persona 模块的运行时序列化契约，不可改动。
# preference/commitment（画像 8 维度扩展，对齐 nori 侧）：暂无核心侧
# 专属字段（get_profile 为休眠 API，宿主只消费注入 markdown），序列化
# 时 preference 并入 established_notes、commitment 并入 memory_points
# （记忆要点——语义恰为"要记住的约定"）；核心侧加字段后改为直映射。
_SECTION_ORDER = [
    ("identity", "基本信息", "persona_backdrop"),
    ("naming", "称呼偏好", "addressing_style"),
    ("stable", "已知事实", "established_notes"),
    ("preference", "喜好偏好", "established_notes"),
    ("commitment", "约定承诺", "memory_points"),
    ("interaction", "互动偏好", "rapport_rules"),
    ("recent", "近期动态", "recent_updates"),
]
_UNCERTAIN_TITLE = "待定信息"
_EMPTY_ITEM = "暂无"
_PER_SECTION_LIMIT = 5
# 时效栏：条目带发生时间尾注，口径对齐 recall 记忆块（time_labels）
_TIME_LABELED_CATEGORIES = {"recent"}
# "其他名称"栏：唯一非事实栏（alias 层历史名变体，身份参考元数据），
# 置于基本信息之后；无变体整栏省略（不写"暂无"占位），最多最近 5 个
_ALIAS_SECTION_TITLE = "其他名称"
_ALIAS_NAMES_LIMIT = 5


class LocalPersonaService:
    """User persona service for the local memory backend (deterministic assembly)."""

    def __init__(
        self,
        db: MemoryDatabase,
        config: LocalMemoryConfig,
        bot_nickname: str = "",
        identity_resolver: object | None = None,
        host_tz_provider: object | None = None,
    ) -> None:
        """Initialize.

        Args:
            db: Memory store access layer.
            config: Plugin runtime config (topic blacklist etc.).
            bot_nickname: Bot nickname (kept for base-class constructor
                compatibility; not used in assembly, persona statements are
                produced by the encoder and need no runtime name substitution).
            identity_resolver: Optional identity-mapping resolver; when the
                same person has multiple channels (accounts linking qq/web
                accounts), reads merge personas by identity; persona writes
                still land per source account, merging only happens for read-side injection.
            host_tz_provider: Host timezone live-read callback (passed from
                kernel with the same source), keeping the resolution chain
                "config name > host > server local" aligned with recall injection.
        """
        self._db = db
        self._config = config
        self.bot_nickname = bot_nickname or "AI"
        self._identity_resolver = identity_resolver
        self._host_tz_provider = host_tz_provider
        self._tz_cache: object | None = None

    def _local_tz(self) -> object:
        """Local timezone (lazy-resolved cache; resolution chain same as kernel: config name > host
        provider > server local), preventing an illegal timezone config from
        logging a warning on every persona injection."""
        if self._tz_cache is None:
            self._tz_cache = resolve_local_tz(
                (self._config.timezone or "").strip(), self._host_tz_provider
            )
        return self._tz_cache

    def reset_local_tz_cache(self) -> None:
        """Invalidate the cache after timezone config changes (maintenance-page save path, same as kernel)."""
        self._tz_cache = None

    # ------------------------------------------------------------------
    #  写接口：均为语义适配 no-op（表驱动画像，唯一变更通道是评分状态机）
    # ------------------------------------------------------------------

    async def ensure_profile_model(
        self,
        platform: str,
        user_id: str,
        display_name: str,
        nickname: str = "",
    ) -> Optional[str]:
        """The local backend has no persona entity to create: the table is the persona, returns None (base-class semantics adaptation).

        Args:
            platform: Platform identifier.
            user_id: User ID.
            display_name: Display name (ignored, the display name is taken
                dynamically from the most recent entry in the raw table).
            nickname: User nickname (ignored).

        Returns:
            Always None.
        """
        return None

    async def ensure_profile_exists(
        self, user_id: str, platform: str = "", nickname: str = ""
    ) -> None:
        """No entity to create: an empty persona naturally returns empty text, no pre-creation needed."""
        return None

    async def update_profile(
        self, user_id: str, profile: PersonProfile, platform: str = ""
    ) -> None:
        """The persona is a cluster-table projection and does not accept direct writes (external rewrites would break determinism)."""
        logger.info(
            "update_profile 被忽略：画像由评分状态机驱动（user_id=%s）", user_id
        )

    async def update_name(
        self, user_id: str, new_name: str, reason: str = "", platform: str = ""
    ) -> None:
        """Name changes go through the fact-extraction -> naming cluster -> persona-projection path, not direct rewrites."""
        logger.info(
            "update_name 被忽略：称呼经 naming 簇投影（user_id=%s, new_name=%s）",
            user_id,
            new_name,
        )

    # ------------------------------------------------------------------
    #  读接口：拼装与解析
    # ------------------------------------------------------------------

    async def get_profile(
        self, user_id: str, session_id: str = "", platform: str = ""
    ) -> Optional[PersonProfile]:
        """Fetch the user persona (assembled from table content, then parsed into PersonProfile).

        Args:
            user_id: User ID.
            session_id: Session ID (not used in assembly; kept for contract compatibility).
            platform: Platform identifier (None returned when empty, nothing to look up).

        Returns:
            PersonProfile; None when there is no persona content or platform is missing.
        """
        if not platform:
            logger.warning("get_profile 需要 platform 参数")
            return None
        markdown = await self._assemble_profile_markdown(platform, user_id)
        if not markdown:
            return None
        return self._parse_profile(user_id, markdown)

    async def build_profile_text(
        self, user_id: str, session_id: str = "", platform: str = ""
    ) -> str:
        """Build single-user persona injection text (triple format, verbatim-aligned with hindsight).

        Args:
            user_id: User ID.
            session_id: Session ID (not used in assembly).
            platform: Platform identifier.

        Returns:
            Injection text; empty string when there is no persona content
            (or everything was filtered by the blacklist).
        """
        if not platform:
            return ""
        # 抬头名先取：其他名称栏需以当前称呼做排除
        keys = self._linked_keys(platform, user_id)
        if keys is not None:
            platforms = [p for p, _ in keys]
            uids = [u for _, u in keys]
            display_name = (
                await self._db.fetch_latest_display_name_multi(platforms, uids)
                or user_id
            )
        else:
            display_name = (
                await self._db.fetch_latest_display_name(platform, user_id) or user_id
            )
        markdown = await self._assemble_profile_markdown(
            platform, user_id, current_name=display_name
        )
        if not markdown:
            return ""
        markdown = self._filter_blacklist(markdown)
        if not markdown:
            return ""
        logger.debug(
            "画像注入 session=%s: 1 人 [%s+%s]",
            session_id, user_id, platform,
        )
        return (
            f"{INJECTION_HEADER}\n"
            f"{INJECTION_DISCLAIMER}\n\n"
            f"{display_name}：\n{markdown}\n\n"
            f"{INJECTION_FOOTER}"
        )

    async def build_multi_profile_text(
        self,
        candidates: list[PersonaCandidate],
        session_id: str,
    ) -> str:
        """Build multi-participant persona injection text for group chat.

        Assembles "{display_name}:\n{profile}" blocks per candidate and
        merges them; (platform, user_id) dedup prevents repeated injection;
        the candidate count cap is controlled by the caller (MemoryStage's
        max_persona_profiles). Returns an empty string when no block is valid.

        Args:
            candidates: Multi-participant persona candidate list.
            session_id: Session ID (not used in assembly).

        Returns:
            Merged persona text; empty string when there is no content.
        """
        if not candidates:
            return ""

        blocks: list[str] = []
        injected_names: list[str] = []
        seen: set[tuple[tuple[str, str], ...]] = set()
        for candidate in candidates:
            # 身份级去重：账号键（无映射）或 linked 键组（多渠道同一人）
            # 作为去重键——qq/web 关联账号在候选中同时出现时仅注入一份
            keys = self._linked_keys(candidate.platform, candidate.user_id)
            identity_key = tuple(keys) if keys is not None else (
                (candidate.platform, candidate.user_id),
            )
            if identity_key in seen:
                continue
            seen.add(identity_key)
            # 抬头名先取：其他名称栏需以当前称呼做排除
            display_name = candidate.display_name or candidate.user_id
            markdown = await self._assemble_profile_markdown(
                candidate.platform, candidate.user_id, current_name=display_name
            )
            if not markdown:
                continue
            markdown = self._filter_blacklist(markdown)
            if not markdown:
                continue
            blocks.append(f"{display_name}：\n{markdown}")
            injected_names.append(
                f"[{candidate.user_id}+{display_name}]"
            )

        if not blocks:
            return ""
        logger.debug(
            "画像注入 session=%s: %d 人 %s",
            session_id, len(injected_names), "".join(injected_names),
        )
        return (
            f"{INJECTION_HEADER}\n"
            f"{INJECTION_DISCLAIMER}\n\n"
            + "\n\n".join(blocks)
            + f"\n\n{INJECTION_FOOTER}"
        )

    # ------------------------------------------------------------------
    #  内部：拼装 / 过滤 / 解析
    # ------------------------------------------------------------------

    def _linked_keys(self, platform: str, user_id: str) -> list[tuple[str, str]] | None:
        """Expand identity-linked account keys; None when there is no mapping/no resolver (single-key path).

        When the same person has multiple channels (e.g. qq and web accounts
        linked to one identity), reads merge personas: profile rows from both
        channels inject as one shared profile. The single-key path keeps the
        exact original behavior (no multi query; zero difference for existing deployments).
        """
        if self._identity_resolver is None:
            return None
        keys = self._identity_resolver.linked_accounts(platform, user_id)
        if len(keys) <= 1:
            return None
        return keys

    async def _assemble_profile_markdown(
        self, platform: str, user_id: str, current_name: str = ""
    ) -> str:
        """Deterministically assemble the persona profile in fixed slot order (empty slots write the "no data" placeholder).

        Returns an empty string when there is no content at all (no profile
        rows, no pending clusters, no name variants); the caller uses that to
        conclude "no persona" instead of producing an all-empty profile. When
        identity-linked multiple keys exist, the multi query path is used and
        profile rows across keys merge into one profile (ordering/dedup per
        the db-layer multi methods). The "other names" slot holds historical
        name variants collected by the alias layer (historical nicknames and
        cards, current address current_name excluded), merged across keys and
        sorted by db-layer last_seen descending.

        Args:
            platform: Platform identifier.
            user_id: User ID.
            current_name: The user's current address (block header name),
                exclusion for the other-names slot.

        Returns:
            Persona profile markdown; empty string with no content.
        """
        keys = self._linked_keys(platform, user_id)
        if keys is not None:
            platforms = [p for p, _ in keys]
            uids = [u for _, u in keys]
            sections = await self._db.fetch_profile_sections_multi(
                platforms, uids, _PER_SECTION_LIMIT
            )
            uncertain = await self._db.fetch_uncertain_statements_multi(
                platforms, uids, _PER_SECTION_LIMIT
            )
        else:
            platforms, uids = [platform], [user_id]
            sections = await self._db.fetch_profile_sections(
                platform, user_id, _PER_SECTION_LIMIT
            )
            uncertain = await self._db.fetch_uncertain_statements(
                platform, user_id, _PER_SECTION_LIMIT
            )
        variants_map = await self._db.fetch_alias_variants(platforms, uids)
        alias_names: list[str] = []
        for key in keys or [(platform, user_id)]:
            for name in variants_map.get(key, []):
                if name != current_name and name not in alias_names:
                    alias_names.append(name)
        if not sections and not uncertain and not alias_names:
            return ""

        parts: list[str] = []
        now = self._now()
        local_tz = self._local_tz()
        mode = self._config.recall_time_label_mode

        def _label(ts: object) -> str:
            label = memory_time_label(ts, now=now, local_tz=local_tz, mode=mode)
            return f"（{label}）" if label else ""

        for category, title, _ in _SECTION_ORDER:
            items = sections.get(category, [])
            if category in _TIME_LABELED_CATEGORIES:
                lines = [f"- {s}{l}" if (l := _label(ts)) else f"- {s}"
                    for s, ts in items
                ] or [f"- {_EMPTY_ITEM}"]
            else:
                lines = [f"- {s}" for s, _ in items] or [f"- {_EMPTY_ITEM}"]
            parts.append(f"## {title}\n" + "\n".join(lines))
            if category == "identity" and alias_names:
                parts.append(
                    f"## {_ALIAS_SECTION_TITLE}\n"
                    + f"- {'、'.join(alias_names[:_ALIAS_NAMES_LIMIT])}"
                )
        uncertain_lines = [
            f"- {s}{l}" if (l := _label(ts)) else f"- {s}" for s, ts in uncertain
        ] or [f"- {_EMPTY_ITEM}"]
        parts.append(f"## {_UNCERTAIN_TITLE}\n" + "\n".join(uncertain_lines))
        return "\n\n".join(parts)

    @staticmethod
    def _now() -> datetime:
        """Current UTC time (separate method so tests can pin the clock)."""
        return datetime.now(timezone.utc)

    def _filter_blacklist(self, markdown: str) -> str:
        """Filter persona content line by line against the topic blacklist (aligned with recall filtering).

        Lines containing a blacklist keyword ("- xxx" entries) are dropped;
        "## slot" titles are kept. Returns an empty string when all entries
        are filtered out.

        Args:
            markdown: Persona profile text.

        Returns:
            Filtered text (possibly empty).
        """
        blacklist = self._config.topic_blacklist
        if not blacklist:
            return markdown
        lines = markdown.splitlines()
        filtered = [
            line for line in lines if not any(kw in line for kw in blacklist)
        ]
        if len(filtered) < len(lines):
            logger.info(
                "画像话题黑名单过滤: %d -> %d 行（黑名单: %s）",
                len(lines), len(filtered), blacklist,
            )
        # 有效条目全被滤光（只剩栏目标题与"暂无"占位）视为无内容，
        # 返回空串不注入空壳档案
        has_content = any(
            line.lstrip().startswith("- ") and line.strip() != f"- {_EMPTY_ITEM}"
            for line in filtered
        )
        if not has_content:
            return ""
        return "\n".join(filtered).strip()

    @staticmethod
    def _parse_profile(user_id: str, markdown: str) -> PersonProfile:
        """Parse the assembled profile into a PersonProfile structure (slot name -> field mapping).

        Behavior consistent with the hindsight parser: skips empty items and
        "no data" placeholder lines.

        Args:
            user_id: User ID.
            markdown: Slot-template profile text.

        Returns:
            PersonProfile (field names are the core-side contract, see the
            _SECTION_ORDER comment).
        """
        profile = PersonProfile(user_id=user_id)
        section_map = {title: field for _, title, field in _SECTION_ORDER}
        section_map[_UNCERTAIN_TITLE] = "unverified_notes"

        current_section: Optional[str] = None
        for line in markdown.splitlines():
            stripped = line.strip()
            if stripped.startswith("## "):
                current_section = section_map.get(stripped[3:].strip())
                continue
            if current_section and stripped.startswith("- "):
                item = stripped[2:].strip()
                if item and item != _EMPTY_ITEM:
                    getattr(profile, current_section).append(item)
        return profile
