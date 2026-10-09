"""LocalPersonaService：簇表 + 画像表的确定性画像拼装（M5，开发方案 §9.3）。

设计（与 hindsight 的本质差异）：
- 画像 = 表内容的**确定性投影**：栏位模板（8 维度 + 条件性"其他
  名称"栏）+ 固定排序（score DESC ->
  updated_at DESC -> id ASC）+ 每栏上限，同样的表内容产出逐字节相同的
  画像文本；无 LLM 参与、无全量重写，画像变化 ⇔ 簇状态变化；
- 数据源：前五栏取画像表（profiled 簇的投影行），待定信息栏取簇表
  （pending_uncertain 与 replaced 减半分簇，摇摆/降级内容对 bot 仍可见）；
- 注入三件套（标题/免责声明/尾句）逐字对齐 hindsight_persona，消费侧
  （MemoryStage -> planner/replyer）无感切换；
- 写接口语义适配：update_profile / update_name / ensure_profile_* 均为
  no-op（画像唯一变更通道是评分状态机，杜绝外部直接改写破坏确定性）；
  ensure_profile_model 返回 None（本地后端无实体可建，表即画像）。
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
    """本地记忆后端的用户画像服务（确定性拼装）。"""

    def __init__(
        self,
        db: MemoryDatabase,
        config: LocalMemoryConfig,
        bot_nickname: str = "",
        identity_resolver: object | None = None,
        host_tz_provider: object | None = None,
    ) -> None:
        """初始化。

        Args:
            db: 记忆库访问层。
            config: 插件运行时配置（话题黑名单等）。
            bot_nickname: Bot 昵称（保留对齐基类构造习惯，拼装不使用——
                画像语句由编码器产出，无需运行时再代入称呼）。
            identity_resolver: 可选身份映射解析器——多渠道同一人
                （accounts 关联 qq/web 账号）时读侧按身份合并画像；
                画像写入仍按来源账号键独立落库，仅读取时合并注入。
            host_tz_provider: 宿主时区活读回调（与 kernel 同源传入）——
                时区解析链"配置名 > 宿主 > 服务器本地"与 recall 注入
                保持同一口径。
        """
        self._db = db
        self._config = config
        self.bot_nickname = bot_nickname or "AI"
        self._identity_resolver = identity_resolver
        self._host_tz_provider = host_tz_provider
        self._tz_cache: object | None = None

    def _local_tz(self) -> object:
        """本地时区（懒解析缓存，解析链与 kernel 同款：配置名 > 宿主
        provider > 服务器本地）——防非法 timezone 配置在每次画像注入时
        重复落 warning。"""
        if self._tz_cache is None:
            self._tz_cache = resolve_local_tz(
                (self._config.timezone or "").strip(), self._host_tz_provider
            )
        return self._tz_cache

    def reset_local_tz_cache(self) -> None:
        """时区配置变更后失效缓存（维护页保存路径，与 kernel 同款）。"""
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
        """本地后端无画像实体可建：表即画像，返回 None（基类语义适配）。

        Args:
            platform: 平台标识。
            user_id: 用户 ID。
            display_name: 显示名（忽略——显示名按 raw 表最近登记动态取）。
            nickname: 用户昵称（忽略）。

        Returns:
            恒 None。
        """
        return None

    async def ensure_profile_exists(
        self, user_id: str, platform: str = "", nickname: str = ""
    ) -> None:
        """无实体可建：空画像自然返回空文本，无需预创建。"""
        return None

    async def update_profile(
        self, user_id: str, profile: PersonProfile, platform: str = ""
    ) -> None:
        """画像为簇表投影，不接受直接写入（外部改写会破坏确定性）。"""
        logger.info(
            "update_profile 被忽略：画像由评分状态机驱动（user_id=%s）", user_id
        )

    async def update_name(
        self, user_id: str, new_name: str, reason: str = "", platform: str = ""
    ) -> None:
        """称呼变更走事实提取 -> naming 簇 -> 画像投影链路，不直接改写。"""
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
        """获取用户画像（表内容拼装后解析为 PersonProfile 结构）。

        Args:
            user_id: 用户 ID。
            session_id: 会话 ID（拼装不使用，保留契约兼容）。
            platform: 平台标识（空时返回 None——无归属可查）。

        Returns:
            PersonProfile；无画像内容或 platform 缺失时 None。
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
        """构建单用户画像注入文本（三件套格式，逐字对齐 hindsight）。

        Args:
            user_id: 用户 ID。
            session_id: 会话 ID（拼装不使用）。
            platform: 平台标识。

        Returns:
            注入文本；无画像内容（或全被黑名单过滤）时空字符串。
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
        """批量构建多参与者画像注入文本（群聊场景）。

        逐候选拼装 "{display_name}：\n{档案}" 块后合并；(platform, user_id)
        去重防重复注入；候选数量上限由调用方（MemoryStage 的
        max_persona_profiles）控制。无任何有效块时返回空字符串。

        Args:
            candidates: 多参与者画像候选列表。
            session_id: 会话 ID（拼装不使用）。

        Returns:
            合并后的画像文本；无内容时空字符串。
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
        """展开身份关联账号键；无映射/无 resolver 时返回 None（单键路径）。

        多渠道同一人（如 qq 与 web 账号关联同一 identity）时读侧合并画像：
        两个渠道的画像行共同注入同一份档案。单键路径完全保持原行为
        （不触发 multi 查询，存量部署零差异）。
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
        """按栏位固定顺序确定性拼装画像档案（空栏写"暂无"）。

        完全无内容（无画像行、无待定簇且无名变体）时返回空字符串（调用
        方据此判定"无画像"，不产出全空档案）。身份关联多键时走 multi
        查询，跨键画像行合并为同一份档案（排序/去重见 db 层 multi 方法）。
        "其他名称"栏内容为 alias 层采集的历史名变体（含历史昵称/名片，
        排除当前称呼 current_name），跨键合并按 db 层 last_seen 降序。

        Args:
            platform: 平台标识。
            user_id: 用户 ID。
            current_name: 该用户当前称呼（块抬头名），其他名称栏排除项。

        Returns:
            画像档案 markdown；无任何内容时空字符串。
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
        """当前 UTC 时间（独立方法便于测试固定时钟）。"""
        return datetime.now(timezone.utc)

    def _filter_blacklist(self, markdown: str) -> str:
        """按话题黑名单逐行过滤画像内容（与 recall 过滤对齐）。

        包含黑名单关键词的行（"- xxx" 条目）被丢弃；"## 栏目"标题保留。
        全部条目被过滤时返回空字符串。

        Args:
            markdown: 画像档案文本。

        Returns:
            过滤后的文本（可能为空串）。
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
        """把拼装档案解析为 PersonProfile 结构（栏名 -> 字段映射）。

        与 hindsight 版解析器行为一致：跳过空项与"暂无"占位行。

        Args:
            user_id: 用户 ID。
            markdown: 栏位模板档案文本。

        Returns:
            PersonProfile（字段名是核心侧契约，见 _SECTION_ORDER 注释）。
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
