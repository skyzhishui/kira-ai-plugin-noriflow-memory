"""NoriFlow 长期记忆插件（KiraAI 版，PostgreSQL + pgvector）。

自 nori-core nori_plugin_noriflow_memory 移植。与工具/标签型插件的差异：
本插件是服务型插件，核心价值在自动记忆链路——

- 写入（retain）：监听 @on.im_message / @on.message_sent 维护每会话滚动行
  缓存；订阅核心 session_memory_updated 事件（每对话回合——含工具循环——
  全部发送完成后恰好发出一次）按消息水位线取增量批次组装编码输入（历史
  窗口 + 分隔标记 + 本轮批次 + bot 回复，格式与 memory_encode.prompt 契约
  逐字对齐），经 FastLlmExit（fast 档）端侧编码为「保真摘要 + 人物事实」
  双通道写入；编码失败 fail-open 降级为原文单通道并回滚水位线，DB 熔断
  拒绝期 retain 直接失败并回滚水位线（本批待下轮重编码，不白烧编码与
  向量化调用），内容永不丢。
- 召回（recall）：@on.llm_request 时以批次文本为 query 向量检索 + 重排序，
  注入为独立 Prompt；用户画像由簇表确定性拼装（无 LLM），一并注入。
- 主动工具：memory_search / memory_write / memory_remove（enabled_tools
  多选配置控制启用哪些工具；allowed_users 用户白名单与 allowed_sessions
  会话白名单做代码级拦截——按触发用户或触发会话匹配，任一命中即放行，
  两者皆空 = 全部拒绝；用户条目支持 user_id 或 平台:user_id，会话条目
  支持 session_id、平台:session_id 或完整 sid 平台:类型:session_id）。
- 维护：@register.page 维护页 + 22 个 @register.api 端点（概览/用户/事实/
  簇/画像/摘要/图谱/边编辑/回填/补编码/迁入/设置的浏览与修正），数据层见 webui_store.py。

graceful degradation：dsn 未配置 / DB 连接或迁移失败 / fast LLM 未配置，
均跳过对应能力并记录日志，不阻断宿主启动；kernel 未就绪时自动摘除全部
记忆工具。装配完全就绪后自动禁用内置 kira_plugin_simple_memory（官方
要求使用第三方记忆插件前禁用；仅在本插件就绪后才禁用，避免记忆真空）。

配置默认值不含真实地址；dsn 请在部署侧 WebUI 插件配置中填写。
"""

from __future__ import annotations

import asyncio
import json
from collections import OrderedDict, deque
from dataclasses import dataclass, field
from datetime import datetime
from pathlib import Path
import time
from typing import Any, Optional

from fastapi import Body, HTTPException

from core.chat.message_elements import At, Reply, Text
from core.plugin import (
    BasePlugin,
    PageMenu,
    PluginPage,
    Priority,
    logger,
    on,
    register,
)
from core.prompt_manager import Prompt

from . import config_web, webui_store
from .alias_store import build_alias_rows
from .circuit_breaker import MemoryDBCircuitBreaker
from .clients import FastLlmExit, KiraEmbeddingClient, KiraRerankClient
from .config import LocalMemoryConfig
from .contracts import PersonaCandidate
from .db import MemoryDatabase, SQLiteMemoryDatabase, resolve_backend_name
from .envelope import HISTORY_BATCH_SEPARATOR, format_history_message
from .kira_memory_import import (
    default_source_path,
    migrate_legacy_import_kv,
    read_last_run,
    run_kira_memory_import,
    scan_source,
)
from .memory_encoder import MemoryEncoder
from .memory_kernel import LocalMemoryKernel
from .merge_agent import FactMergeAgent
from .persona_service import LocalPersonaService
from .recall_log import RecallLogWriter
from .relation_backfill import BackfillController, RelationBackfill
from .vector_ops import EmbeddingBackfillTask, EmbeddingService

PLUGIN_ID = "kira-ai-plugin-noriflow-memory"
# 内置文件型记忆插件（官方描述：使用第三方记忆插件前请先禁用）
SIMPLE_MEMORY_PLUGIN_ID = "kira_plugin_simple_memory"

ALL_TOOLS = ["memory_search", "memory_write", "memory_remove"]

# 编码输入中历史窗口行数（与上游 nori 版一致）
_HISTORY_WINDOW_LINES = 10
# 每会话滚动行缓存上限（历史窗口取自这里，留足余量）
_SESSION_CACHE_LIMIT = 30
# 会话缓存 LRU 上限（防长期运行内存膨胀）
_SESSION_CACHE_MAX_SESSIONS = 200
# bot 出站平台消息 ID 每会话追踪上限（引用 bot 判定覆盖窗口）
_BOT_MESSAGE_ID_LIMIT = 100
# recall query 截断长度
_RECALL_QUERY_MAX_CHARS = 200
# 存量关系回填的结构化出口 schema（FastLlmExit.run_structured 工具调用
# 形状约束；字段语义由 fact_relations.prompt 承载）
_RELATION_BACKFILL_SCHEMA = {
    "type": "object",
    "properties": {
        "relations": {
            "type": "array",
            "description": "提取的人际关系列表；无可提取关系时为空数组",
            "items": {
                "type": "object",
                "properties": {
                    "cluster_id": {"type": "integer", "description": "来源簇 ID"},
                    "subject_user_id": {"type": "string", "description": "关系持有方 uid"},
                    "subject_display_name": {"type": "string", "description": "持有方名字"},
                    "object_user_id": {"type": "string", "description": "关系对象 uid"},
                    "object_display_name": {"type": "string", "description": "对象名字"},
                    "label": {"type": "string", "description": "关系词（陈述原文词形）"},
                    "statement": {"type": "string", "description": "一句完整陈述"},
                    "confidence": {"type": "string", "enum": ["high", "medium"]},
                },
                "required": [
                    "cluster_id", "subject_user_id", "object_user_id",
                    "label", "statement",
                ],
            },
        }
    },
    "required": ["relations"],
}
# persona 昵称活读 TTL：宿主 persona 每次请求从 DB 活读（支持热切换）
# 且无变更事件可订阅，以此限频刷新插件的昵称缓存
_PERSONA_REFRESH_TTL_SECONDS = 60.0


def _cfg_str(raw: dict, key: str, default: str = "") -> str:
    value = raw.get(key)
    if value in (None, ""):
        return default
    return str(value).strip()


def _cfg_int(raw: dict, key: str, default: int) -> int:
    try:
        value = raw.get(key)
        return default if value in (None, "") else int(value)
    except (TypeError, ValueError):
        logger.warning("配置项 %s 非法（期望整数），使用默认 %s", key, default)
        return default


def _cfg_float(raw: dict, key: str, default: float) -> float:
    try:
        value = raw.get(key)
        return default if value in (None, "") else float(value)
    except (TypeError, ValueError):
        logger.warning("配置项 %s 非法（期望数值），使用默认 %s", key, default)
        return default


def _cfg_bool(raw: dict, key: str, default: bool) -> bool:
    value = raw.get(key)
    if value in (None, ""):
        return default
    if isinstance(value, bool):
        return value
    return str(value).strip().lower() in ("1", "true", "yes", "on")


def _cfg_strlist(raw: dict, key: str, default: Optional[list[str]] = None) -> list[str]:
    value = raw.get(key)
    if not value:
        return list(default) if default else []
    if isinstance(value, str):
        return [part.strip() for part in value.split(",") if part.strip()]
    if isinstance(value, (list, tuple)):
        return [str(part).strip() for part in value if str(part).strip()]
    return []


def _build_config(raw: dict) -> LocalMemoryConfig:
    """插件配置 dict -> LocalMemoryConfig（逐字段容错，非法值落默认）。"""
    return LocalMemoryConfig(
        storage_backend=_cfg_str(raw, "storage_backend"),
        sqlite_path=_cfg_str(raw, "sqlite_path"),
        dsn=_cfg_str(raw, "dsn"),
        pool_min=_cfg_int(raw, "pool_min", 2),
        pool_max=_cfg_int(raw, "pool_max", 8),
        db_command_timeout=_cfg_int(raw, "db_command_timeout", 60),
        embedding_model=_cfg_str(raw, "embedding_model"),
        embedding_dims=_cfg_int(raw, "embedding_dims", 1024),
        encode_input_max_chars=_cfg_int(raw, "encode_input_max_chars", 30000),
        backfill_interval_seconds=_cfg_float(raw, "backfill_interval_seconds", 300.0),
        backfill_batch_size=_cfg_int(raw, "backfill_batch_size", 64),
        merge_interval_hours=_cfg_float(raw, "merge_interval_hours", 6.0),
        merge_batch_size=_cfg_int(raw, "merge_batch_size", 200),
        candidate_top_k=_cfg_int(raw, "candidate_top_k", 20),
        llm_budget_per_cycle=_cfg_int(raw, "llm_budget_per_cycle", 200),
        score_start_high=_cfg_int(raw, "score_start_high", 3),
        score_start_medium=_cfg_int(raw, "score_start_medium", 2),
        score_cap=_cfg_float(raw, "score_cap", 10.0),
        promote_threshold=_cfg_float(raw, "promote_threshold", 10.0),
        demote_threshold=_cfg_float(raw, "demote_threshold", 3.0),
        decay_factor=_cfg_float(raw, "decay_factor", 0.8),
        decay_interval_days=_cfg_int(raw, "decay_interval_days", 14),
        recent_expire_days=_cfg_int(raw, "recent_expire_days", 30),
        recent_promote_threshold=_cfg_float(raw, "recent_promote_threshold", 4.0),
        decay_requires_activity=_cfg_bool(raw, "decay_requires_activity", True),
        sticky_evidence_count=_cfg_int(raw, "sticky_evidence_count", 4),
        pending_dead_days=_cfg_int(raw, "pending_dead_days", 90),
        anchor_profile_size=_cfg_int(raw, "anchor_profile_size", 5),
        recall_top_k=_cfg_int(raw, "recall_top_k", 5),
        recall_relevance_threshold=_cfg_float(raw, "recall_relevance_threshold", 0.0),
        recall_max_tokens=_cfg_int(raw, "recall_max_tokens", 2048),
        recall_time_decay_enabled=_cfg_bool(
            raw, "recall_time_decay_enabled", False
        ),
        recall_time_decay_half_life_days=_cfg_float(
            raw, "recall_time_decay_half_life_days", 90.0
        ),
        summary_recall_session_scoped=_cfg_bool(
            raw, "summary_recall_session_scoped", True
        ),
        max_persona_profiles=_cfg_int(raw, "max_persona_profiles", 3),
        topic_blacklist=_cfg_strlist(raw, "topic_blacklist"),
        rerank_enabled=_cfg_bool(raw, "rerank_enabled", True),
        rerank_candidates=_cfg_int(raw, "rerank_candidates", 50),
        recall_expansion_enabled=_cfg_bool(raw, "recall_expansion_enabled", True),
        recall_expansion_recent_batches=_cfg_int(
            raw, "recall_expansion_recent_batches", 20
        ),
        recall_exclude_history_window=_cfg_bool(
            raw, "recall_exclude_history_window", True
        ),
        recall_hint_enabled=_cfg_bool(raw, "recall_hint_enabled", True),
        alias_enabled=_cfg_bool(raw, "alias_enabled", True),
        alias_variant_cap=_cfg_int(raw, "alias_variant_cap", 8),
        alias_stopwords=_cfg_strlist(raw, "alias_stopwords"),
        relation_extract_enabled=_cfg_bool(raw, "relation_extract_enabled", False),
        relation_bot_edge_min_evidence=_cfg_int(
            raw, "relation_bot_edge_min_evidence", 2
        ),
        relation_inject_enabled=_cfg_bool(raw, "relation_inject_enabled", False),
        relation_inject_max_neighbors=_cfg_int(
            raw, "relation_inject_max_neighbors", 2
        ),
        relation_inject_max_profiles=_cfg_int(
            raw, "relation_inject_max_profiles", 2
        ),
        relation_inject_max_lines=_cfg_int(raw, "relation_inject_max_lines", 3),
        relation_label_stopwords=_cfg_strlist(
            raw, "relation_label_stopwords", ["朋友", "认识", "熟人", "网友"]
        ),
        relation_audit_enabled=_cfg_bool(raw, "relation_audit_enabled", False),
        relation_audit_batch_size=_cfg_int(raw, "relation_audit_batch_size", 20),
        dedup_similarity_threshold=_cfg_float(raw, "dedup_similarity_threshold", 0.85),
        write_dedup_enabled=_cfg_bool(raw, "write_dedup_enabled", True),
        write_dedup_window=_cfg_int(raw, "write_dedup_window", 8),
        write_dedup_threshold=_cfg_float(raw, "write_dedup_threshold", 0.85),
        recall_time_label_enabled=_cfg_bool(raw, "recall_time_label_enabled", True),
        timezone=_cfg_str(raw, "timezone"),
        recall_log_enabled=_cfg_bool(raw, "recall_log_enabled", False),
        recall_log_path=_cfg_str(raw, "recall_log_path"),
        hybrid_search_enabled=_cfg_bool(raw, "hybrid_search_enabled", True),
        hybrid_rrf_k=_cfg_int(raw, "hybrid_rrf_k", 60),
        recent_rollout_enabled=_cfg_bool(raw, "recent_rollout_enabled", True),
        recent_rollout_batches=_cfg_int(raw, "recent_rollout_batches", 3),
        recent_rollout_max_chars=_cfg_int(raw, "recent_rollout_max_chars", 1500),
        failure_threshold=_cfg_int(raw, "failure_threshold", 5),
        recovery_seconds=_cfg_float(raw, "recovery_seconds", 60.0),
        tool_scope_locked=_cfg_bool(raw, "tool_scope_locked", True),
    )


def _try_build_config(raw: dict) -> LocalMemoryConfig:
    """_build_config 的兜底层：数值越界触发 pydantic 校验异常时回退默认
    参数（仅保留 dsn），与模块"逐字段容错"承诺一致，避免单个配置项
    （如 pool_min=0）让插件整体初始化失败。"""
    try:
        return _build_config(raw)
    except Exception:
        logger.warning("配置项越界（pydantic 校验失败），越界项回退默认值", exc_info=True)
        return LocalMemoryConfig(dsn=_cfg_str(raw, "dsn"))


@dataclass
class _CachedLine:
    """滚动行缓存的一行：信封行 + 归属元数据（供回合信号重建增量批次）。"""

    line: str
    uid: str = ""          # 发言者裸 uid（bot 行为空）
    platform: str = ""
    is_bot: bool = False
    # 实体词典路/引用反查路的缓存内锚点数据（bot 行为空/None）
    nickname: str = ""
    cardname: str = ""
    timestamp: Optional[datetime] = None


@dataclass
class _RetainState:
    """一次对话轮次的 retain 快照（回合完成信号到达时按水位线定格）。"""

    event_id: str
    session_id: str
    platform: str
    group_id: str
    user_id: str
    timestamp: datetime
    participants: list[tuple[str, str]]
    history_lines: list[str]
    batch_lines: list[str]
    bot_lines: list[str] = field(default_factory=list)


class NoriflowMemoryPlugin(BasePlugin):
    """NoriFlow 长期记忆插件：自动 retain/recall + 主动工具 + 维护页。"""

    def __init__(self, ctx, cfg: dict):
        super().__init__(ctx, cfg)
        self._db: Optional[MemoryDatabase] = None
        self._memory_kernel: Optional[LocalMemoryKernel] = None
        self._persona_service: Optional[LocalPersonaService] = None
        self._backfill_task: Optional[EmbeddingBackfillTask] = None
        self._merge_agent: Optional[FactMergeAgent] = None
        self._config: Optional[LocalMemoryConfig] = None
        self._fast_llm: Optional[FastLlmExit] = None
        self._relation_controller: Optional[BackfillController] = None
        self._ready = False
        # bot 身份（persona 名 + 首条消息的 self_id）
        self._bot_nickname = "助手"
        self._bot_user_id = ""
        # persona 昵称上次活读时间（monotonic；TTL 限频见 _refresh_bot_nickname）
        self._persona_checked_at = 0.0
        # 每会话滚动行缓存：sid -> OrderedDict[msg_key -> _CachedLine]（尾部最新）
        self._history: OrderedDict[str, OrderedDict[str, _CachedLine]] = OrderedDict()
        # retain 水位线：sid -> 已编码批次的消息键集合（防同批次重复编码）
        self._consumed: dict[str, set[str]] = {}
        # bot 行序号（缓存键唯一性用，不参与查找）
        self._bot_seq = 0
        # bot 出站消息的平台 message_id 追踪（P3 场景 C 的引用 bot 判定：
        # Reply 元素只带平台消息 ID，而 bot 行缓存键是 bot#N 无法反查；
        # sid -> deque 有界，重启从零积累同滚动缓存语义）
        self._bot_message_ids: OrderedDict[str, deque[str]] = OrderedDict()
        # session_memory_updated 订阅句柄（terminate 退订用）
        self._mem_handler = None
        # 已启动写入任务
        self._retain_tasks: set[asyncio.Task] = set()
        self._retain_semaphore = asyncio.Semaphore(2)

    # ------------------------------------------------------------------
    #  生命周期
    # ------------------------------------------------------------------

    async def initialize(self) -> None:
        """装配：配置 -> DB 连接迁移 -> 客户端适配 -> 内核/服务 -> 后台任务。

        任何一步失败均降级跳过（记录日志），不阻断宿主启动；只有完全
        就绪才置 _ready 并自动禁用内置文件型记忆插件。
        """
        cfg = self.plugin_cfg or {}
        if _cfg_bool(cfg, "enabled", True) is False:
            logger.info("noriflow-memory 跳过：config.enabled=false")
            return

        config = _try_build_config(cfg)
        try:
            backend_name = resolve_backend_name(config)
        except ValueError as exc:
            logger.warning("noriflow-memory 跳过：%s", exc)
            return
        if backend_name == "postgres" and not config.dsn:
            logger.info("noriflow-memory 跳过：dsn 未配置（记忆功能关闭）")
            return

        self._config = config
        self._backend_name = backend_name
        if backend_name == "sqlite":
            # SQLite 后端：向量列是 BLOB（无固定维度），不受 vector(1024)
            # 约束（维度不匹配的向量按缺失处理，换 embedding 模型无需重建
            # 表）；库文件默认插件数据目录/memory.sqlite3
            if not config.sqlite_path:
                config.sqlite_path = str(
                    self._plugin_data_dir() / "memory.sqlite3"
                )
            db = SQLiteMemoryDatabase(config)
            migrations_dir = "migrations_sqlite"
        else:
            # 维度与迁移 DDL（vector(1024)）一致性前置校验：不一致时写入链路
            # 会在落列处反复失败（expected 1024 dimensions），直接拒绝启动
            if config.embedding_dims != 1024:
                logger.warning(
                    "noriflow-memory 跳过：embedding_dims=%s 与迁移 DDL 的 "
                    "vector(1024) 不一致（写入将反复失败），请改回 1024",
                    config.embedding_dims,
                )
                return
            db = MemoryDatabase(config)
            migrations_dir = "migrations"
        try:
            await db.connect()
            await db.apply_migrations(
                Path(__file__).resolve().parent / migrations_dir
            )
        except Exception:
            logger.warning("记忆库连接/迁移失败，记忆功能关闭", exc_info=True)
            await db.close()
            return
        self._db = db

        # 宿主 init_plugin 对 initialize 抛异常的实例不调 terminate（直接
        # pop），本段任何失败必须自行清理已建的池/已启的任务，否则泄漏
        try:
            await self._initialize_services(db, config)
        except Exception:
            logger.warning("noriflow-memory 装配失败，记忆功能关闭", exc_info=True)
            await self.terminate()

    async def _initialize_services(
        self, db: MemoryDatabase, config: LocalMemoryConfig
    ) -> None:
        """装配服务与后台任务（initialize 后半段；异常由调用方统一清理）。"""
        # bot 身份：persona 名（先取，供 kernel/合并 agent 构造使用；
        # 消息 self_id 随首条消息回填）
        try:
            persona = await self.ctx.persona_mgr.get_persona()
            if persona is not None and persona.name:
                self._bot_nickname = persona.name
        except Exception:
            logger.warning("获取 persona 失败，bot 称呼回退「助手」", exc_info=True)

        # embedding 客户端（default_embedding 未配置则服务不可用，行内置 NULL 补算）
        embedding_service = EmbeddingService(
            client=self._build_embedding_client(),
            dims=config.embedding_dims,
        )

        # 共享熔断器：kernel（retain/recall）与合并 agent（周期入口 peek）
        # 同一实例——任一路径记录的 DB 故障都会让另一路径的拒绝期跳过
        # 传 config 引用让阈值/恢复时长随维护页配置保存热生效
        circuit_breaker = MemoryDBCircuitBreaker(
            failure_threshold=config.failure_threshold,
            recovery_seconds=config.recovery_seconds,
            config=config,
        )

        # fast LLM 出口：未配置则编码/裁定不可用（retain 单通道，raw 事实堆积）
        fast_llm = self._probe_fast_llm()
        self._fast_llm = fast_llm
        encoder: Optional[MemoryEncoder] = None
        merge_agent: Optional[FactMergeAgent] = None
        if fast_llm is not None:
            encoder = MemoryEncoder(
                llm=fast_llm,
                prompt_dir=Path(__file__).resolve().parent / "prompts",
                input_max_chars=config.encode_input_max_chars,
                # P2 关系提取：开关只控制提示词扩展节附加与否（关闭时
                # 编码行为与现状逐字一致）；kernel 侧另有提取开关兜底；
                # 传 config 让两项运行时读（维护页配置热生效）
                relations_enabled=config.relation_extract_enabled,
                config=config,
            )
            merge_agent = FactMergeAgent(
                db=db,
                llm=fast_llm,
                config=config,
                prompt_dir=Path(__file__).resolve().parent / "prompts",
                encoder=encoder,
                embedding_service=embedding_service,
                bot_nickname=self._bot_nickname,
                # 占位符：首条消息学到真实 self_id 后经 _propagate_bot_identity 回填
                bot_id="kira",
                circuit_breaker=circuit_breaker,
                # kernel 就绪后经其解析插件/宿主时区（构造期 kernel 尚未建，
                # None 时 _to_local 回退服务器本地时区）
                tz_provider=lambda: (
                    self._memory_kernel._local_tz()
                    if self._memory_kernel is not None
                    else None
                ),
                # Bot multi-form uid set (kernel learns platform-uid side forms
                # at runtime); re-encode relation channel must classify bot
                # endpoints with the same form set as the retain channel.
                bot_forms_provider=lambda: (
                    self._memory_kernel._bot_uid_forms_all(self._bot_user_id)
                    if self._memory_kernel is not None
                    else ([self._bot_user_id] if self._bot_user_id else [])
                ),
                # Placeholder-name guard resolver: kernel's alias view supplies
                # the latest canonical name for a uid (same source as retain).
                alias_name_resolver=lambda platform, uid: (
                    self._memory_kernel.alias_store.name_for(platform, uid)
                    if self._memory_kernel is not None
                    and self._memory_kernel.alias_store is not None
                    else ""
                ),
            )
        else:
            logger.warning(
                "fast LLM 未配置：端侧记忆编码与合并 agent 不可用，"
                "retain 降级为原文单通道（raw 事实将堆积待处理）"
            )

        # 召回评估日志（默认关闭；路径默认插件数据目录下）
        recall_log = None
        if config.recall_log_enabled:
            log_path = config.recall_log_path or str(
                self._plugin_data_dir() / "memory_recall_log.jsonl"
            )
            try:
                log_path = str(Path(log_path).resolve())
                Path(log_path).parent.mkdir(parents=True, exist_ok=True)
                recall_log = RecallLogWriter(log_path)
                logger.info("recall 评估日志已启用: %s", log_path)
            except Exception:
                logger.warning("recall 日志路径初始化失败（日志关闭）", exc_info=True)
                recall_log = None

        self._memory_kernel = LocalMemoryKernel(
            db=db,
            embedding_service=embedding_service,
            circuit_breaker=circuit_breaker,
            config=config,
            # 占位符：首条消息学到真实 self_id 后经 _propagate_bot_identity 回填
            bot_id="kira",
            encoder=encoder,
            bot_nickname=self._bot_nickname,
            rerank_client=self._build_rerank_client(config),
            # 近时排除/滚动补回的窗口锚定：宿主可见窗口块数活读（热改即时生效）
            history_window_provider=self._host_window_batches,
            # 时区回退链：插件 timezone 未配置时取宿主 locale.TZ
            # （getattr 防御：老版本核心无该方法时回退服务器本地时区）
            host_tz_provider=getattr(self.ctx, "get_timezone", None),
            recall_log=recall_log,
            # 实体词典名字源：插件滚动行缓存内的发送者昵称/群名片
            # （宿主已观察到的可靠名字；懒构建 + TTL，见 memory_kernel）
            directory_source=self._directory_names,
        )
        self._persona_service = LocalPersonaService(
            db=db,
            config=config,
            bot_nickname=self._bot_nickname,
            identity_resolver=None,  # KiraAI 无跨渠道身份合并，单键路径
        )
        # P3 边注入的邻居画像句柄（kernel 内消费，独立预算；未回填时只出陈述行）
        self._memory_kernel.persona_service = self._persona_service
        self._merge_agent = merge_agent
        self._backfill_task = EmbeddingBackfillTask(db, embedding_service, config)
        # 存量关系回填控制器（维护页关系图谱栏手动触发；LLM 出口缺失时
        # 控制器仍在但 start 拒绝。状态挂插件实例——@register.api 的
        # self 即本实例，天然跨请求存续。bot 身份占位在装配期，factory
        # 触发时活读 self._bot_user_id/_bot_nickname）
        self._relation_controller = BackfillController(
            factory=(
                self._make_relation_backfill if self._fast_llm is not None else (lambda: None)
            )
        )

        # 后台任务：embedding 补算 + 合并 agent
        self._backfill_task.start()
        if self._merge_agent is not None:
            self._merge_agent.start()

        self._ready = True
        # 订阅核心回合完成信号（核心在每轮对话全部发送结束后恰好发出一次，
        # 含完整 chunk）；retain 由该信号触发，替代按发送段 debounce 的旧机制
        bus = getattr(self.ctx, "event_bus", None)
        if bus is not None:
            self._mem_handler = self._on_session_memory_updated
            try:
                bus.subscribe("session_memory_updated", self._mem_handler)
            except Exception:
                logger.warning("回合完成信号订阅失败（自动 retain 不触发）", exc_info=True)
                self._mem_handler = None
        else:
            logger.warning("ctx.event_bus 不可用：回合完成信号无法订阅，自动 retain 不触发")
        if not self._allowed_user_ids() and not self._allowed_session_ids():
            # fail-closed: both whitelists empty keeps the proactive tools
            # rejected for every caller
            logger.warning(
                "noriflow-memory 已就绪但 allowed_users 与 allowed_sessions"
                "均为空：主动工具（memory_search/write/remove）将被全部拒绝，"
                "请在插件配置中填写允许调用的用户 ID 或会话 ID"
            )
        logger.info(
            "noriflow-memory 已就绪: backend=%s(%s), pool=%d-%d(timeout=%ss), "
            "embedding=%s, rerank=%s, encoder=%s, merge=%s, persona=on(确定性拼装)",
            self._backend_name,
            self._config.sqlite_path
            if self._backend_name == "sqlite" else "dsn",
            config.pool_min,
            config.pool_max,
            config.db_command_timeout,
            "on" if embedding_service.available else "off(行内置NULL待补算)",
            "on" if self._memory_kernel.rerank_client is not None else "off(纯向量序)",
            "on" if encoder is not None else "off(单通道降级)",
            "on" if merge_agent is not None else "off(事实堆积)",
        )
        # one-time carry-over: a pre-v1.16.1 import marker (old kv key) keeps
        # its record under the renamed key; best-effort, failure is non-fatal
        try:
            await migrate_legacy_import_kv(db)
        except Exception:
            logger.warning("旧版迁入标记迁移失败（跳过）", exc_info=True)
        await self._disable_simple_memory()

    async def terminate(self) -> None:
        """清理：退订回合信号 -> flush 在途 retain -> 停后台任务 -> 关池。"""
        self._ready = False
        if self._mem_handler is not None:
            bus = getattr(self.ctx, "event_bus", None)
            if bus is not None:
                try:
                    bus.unsubscribe("session_memory_updated", self._mem_handler)
                except (ValueError, KeyError):
                    pass
            self._mem_handler = None
        self._consumed.clear()

        if self._retain_tasks:
            done, pending = await asyncio.wait(self._retain_tasks, timeout=10.0)
            if pending:
                logger.warning("terminate: %d 个 retain 任务超时未完成，取消", len(pending))
                for task in pending:
                    task.cancel()
                await asyncio.gather(*pending, return_exceptions=True)
        self._retain_tasks.clear()

        # 两个 stop 各自兜底：任一失败不能跳过其后的 db.close()（池泄漏）
        if self._merge_agent is not None:
            try:
                await self._merge_agent.stop()
            except Exception:
                logger.warning("合并 agent 停止异常（继续清理）", exc_info=True)
            self._merge_agent = None
        if self._backfill_task is not None:
            try:
                await self._backfill_task.stop()
            except Exception:
                logger.warning("embedding 补算任务停止异常（继续清理）", exc_info=True)
            self._backfill_task = None
        if self._relation_controller is not None:
            try:
                await self._relation_controller.stop()
            except Exception:
                logger.warning("关系回填任务停止异常（继续清理）", exc_info=True)
            self._relation_controller = None
            self._fast_llm = None
        if self._db is not None:
            try:
                await self._db.close()
            except Exception:
                logger.warning("关闭记忆库连接池异常", exc_info=True)
            self._db = None
        self._memory_kernel = None
        self._persona_service = None

    # ------------------------------------------------------------------
    #  装配辅助
    # ------------------------------------------------------------------

    def _build_embedding_client(self) -> Optional[KiraEmbeddingClient]:
        """embedding_model 覆盖 -> default_embedding -> 插件内适配器。

        embedding_model 格式 provider_id:model_id（与宿主 WebUI 模型标识
        一致，经 get_embedding_client 解析）；解析失败回退 default_embedding
        并告警（fail-open，不阻断装配）。两者均未配置返回 None。
        """
        config = self._config
        override = (
            (config.embedding_model or "").strip() if config is not None else ""
        )
        if override:
            emb = None
            try:
                emb = self.ctx.get_embedding_client(override)
            except Exception as exc:
                logger.warning(
                    "embedding_model=%s 解析异常（%s），回退 default_embedding",
                    override,
                    exc,
                )
            if emb is not None:
                logger.info("embedding 客户端使用 embedding_model 覆盖: %s", override)
                return KiraEmbeddingClient(emb)
            logger.warning(
                "embedding_model=%s 无法解析为已配置的模型，回退 default_embedding",
                override,
            )
        try:
            emb = self.ctx.get_default_embedding_client()
        except Exception as exc:
            logger.warning("embedding 不可用：%s（行内向量置 NULL 待补算）", exc)
            return None
        if emb is None:
            logger.warning("embedding 不可用：default_embedding 未配置")
            return None
        return KiraEmbeddingClient(emb)

    def _build_rerank_client(
        self, config: LocalMemoryConfig
    ) -> Optional[KiraRerankClient]:
        """default_rerank -> 插件内适配器（未配置/未启用返回 None，纯向量序）。"""
        if not config.rerank_enabled:
            return None
        try:
            rr = self.ctx.provider_mgr.get_default_rerank()
        except Exception as exc:
            logger.info("重排序未启用：%s（检索退化为纯向量序）", exc)
            return None
        if rr is None:
            return None
        return KiraRerankClient(rr)

    def _host_window_batches(self) -> int:
        """宿主 LLM 可见历史窗口块数（bot.max_memory_length 活读）。

        与 session_manager 的窗口截断同键同源（热改即时生效）；读取失败
        回退核心默认 10。retain 每轮恰好一批摘要，「最近 K 批」即窗口内
        已可见内容的等价物（见 memory_kernel._derive_window_batches）。
        """
        cfg = getattr(self.ctx, "config", None)
        if cfg is None:
            return 10
        try:
            value = int(cfg.get_config("bot_config.bot.max_memory_length", 10) or 10)
        except (TypeError, ValueError):
            return 10
        return max(value, 0)

    def _plugin_data_dir(self) -> Path:
        """插件数据目录（宿主 plugin_data/<plugin_id>；不可得时回退 ./data）。"""
        try:
            d = self.ctx.get_plugin_data_dir()
            if d is not None:
                return Path(d)
        except Exception:
            logger.warning("插件数据目录获取失败（回退 ./data）", exc_info=True)
        return Path("data")

    async def _refresh_bot_nickname(self) -> None:
        """活读宿主 persona 昵称并传播（TTL 限频）。

        宿主 persona 每次请求从 DB 活读（支持运行期热切换），且没有
        persona 变更事件可订阅——initialize 的一次性缓存会在切换后过期：
        编码 prompt 的「bot 排除规则」仍指向旧名，新人格名下关于 bot
        自身的信息可能被提取进用户画像（信封行靠 is_self 标记仍安全，
        与 bot_id 传播同族的确定性防线）。消费方：编码/补编码 prompt
        排除规则、bot 回复信封行。读取失败保持现值。
        """
        now = time.monotonic()
        if (
            self._persona_checked_at
            and now - self._persona_checked_at < _PERSONA_REFRESH_TTL_SECONDS
        ):
            return
        self._persona_checked_at = now
        try:
            persona = await self.ctx.persona_mgr.get_persona()
        except Exception:
            return
        name = str(getattr(persona, "name", "") or "").strip()
        if not name or name == self._bot_nickname:
            return
        self._bot_nickname = name
        if self._memory_kernel is not None:
            self._memory_kernel.bot_nickname = name
        if self._merge_agent is not None:
            self._merge_agent.bot_nickname = name
        if self._persona_service is not None:
            self._persona_service.bot_nickname = name
        logger.info("persona 昵称已热切换并传播: %s", name)

    def _propagate_bot_identity(self, bot_user_id: str) -> None:
        """真实 bot 平台 ID 传播到 kernel / 合并 agent（构造期是占位符）。

        消费方：retain_encoded/_reingest_facts 的 bot 事实硬过滤、编码
        prompt 的「平台 ID」渲染、召回扩选的 bot 剔除——占位符「kira」
        与真实 uid 永不相等，传播前这层确定性防线是死代码。首见即定型
        （KiraAI 单 bot 部署；跨适配器多账号不在当前范围）。
        """
        if self._memory_kernel is not None:
            self._memory_kernel.bot_id = bot_user_id
        if self._merge_agent is not None:
            self._merge_agent.bot_id = bot_user_id
        logger.info("bot 平台 ID 已学习并传播: %s", bot_user_id)

    def _probe_fast_llm(self) -> Optional[FastLlmExit]:
        """探测 fast LLM 可用性；可用返回出口实例，不可用返回 None。"""
        try:
            client = self.ctx.get_default_fast_llm_client()
        except Exception as exc:
            logger.warning("fast LLM 探测失败：%s", exc)
            return None
        if client is None:
            return None
        return FastLlmExit(self.ctx)

    def _make_relation_backfill(self) -> RelationBackfill:
        """构造存量关系回填器（llm_call 闭包对齐共享模块两侧同构约定）。"""
        fast_llm = self._fast_llm

        async def _call(system_prompt: str, user_prompt: str) -> str:
            return await fast_llm.run_structured(
                system_prompt=system_prompt,
                user_prompt=user_prompt,
                schema=_RELATION_BACKFILL_SCHEMA,
                tool_name="relation_backfill",
            )

        return RelationBackfill(
            llm_call=_call,
            db=self._db,
            config=self._config,
            # bot 身份装配期为占位，点击回填时活读（首条消息后已学习）
            bot_user_id=self._bot_user_id,
            bot_nickname=self._bot_nickname,
            # Bot multi-form uid set: backfill must classify bot endpoints the
            # same way as the retain channel (kernel-learned forms included).
            bot_forms_provider=lambda: (
                self._memory_kernel._bot_uid_forms_all(self._bot_user_id)
                if self._memory_kernel is not None
                else ([self._bot_user_id] if self._bot_user_id else [])
            ),
        )

    async def _disable_simple_memory(self) -> None:
        """自动禁用内置文件型记忆插件（仅在本插件完全就绪后调用）。"""
        pm = getattr(self.ctx, "plugin_mgr", None)
        if pm is None:
            logger.info(
                "plugin_mgr 不可用，请手动在 WebUI 插件管理中禁用 %s",
                SIMPLE_MEMORY_PLUGIN_ID,
            )
            return
        try:
            if pm.is_plugin_enabled(SIMPLE_MEMORY_PLUGIN_ID):
                await pm.set_plugin_enabled(SIMPLE_MEMORY_PLUGIN_ID, False)
                logger.info(
                    "已自动禁用内置记忆插件 %s（本插件完全替代其功能）",
                    SIMPLE_MEMORY_PLUGIN_ID,
                )
        except Exception:
            logger.warning(
                "自动禁用 %s 失败（请手动禁用，两套记忆并存会重复注入）",
                SIMPLE_MEMORY_PLUGIN_ID,
                exc_info=True,
            )

    # ------------------------------------------------------------------
    #  recall 注入 + 工具门控（llm_request）
    # ------------------------------------------------------------------

    @on.llm_request(priority=Priority.MEDIUM)
    async def inject_memory(self, event, req, tag_set, *args, **kwargs):
        """召回注入 + 记忆工具门控。

        - 工具门控：enabled_tools 多选配置，kernel 未就绪时摘除全部记忆
          工具（防调用报错）；allowed_users 用户白名单下，非白名单触发者
          同样摘除（硬拦截仍在各工具入口）。
        - 召回：批次文本为 query 向量检索（session 隔离开关控制跨会话）。
        - 滚动补回：宿主可见窗口外的最近批次摘要注入为独立 Prompt（非
          query 驱动——notice 触发的回合同样需要近期上下文；先于 recall
          构建以让其 memo 就位，recall 同轮排除这些行防重复注入）。
        - 画像：批次发言者候选 -> 簇表确定性拼装（群聊多人 / 私聊
          触发者+实体命中扩员，v1.7.1 起私聊提及他人也注入其画像）。
        """
        # enabled_tools 显式空列表 = 全部禁用（与 schema "取消选择即禁用"
        # 语义一致）；键缺失/None 才回退全启用
        configured = self.plugin_cfg.get("enabled_tools")
        if configured is None:
            configured = ALL_TOOLS
        disabled = set(ALL_TOOLS) - set(configured)
        if not self._ready:
            disabled = set(ALL_TOOLS)
        elif self._proactive_tools_denied(event):
            # user whitelist (allowed_users): hide the memory tools from
            # non-whitelisted trigger users for the whole turn to avoid
            # pointless denied calls; hard enforcement stays at each tool
            # entry (_whitelist_denial)
            disabled = set(ALL_TOOLS)
        if disabled:
            try:
                req.tool_set.remove(*disabled)
            except Exception:
                logger.debug("摘除记忆工具失败（可能未注册）", exc_info=True)

        kernel = self._memory_kernel
        if kernel is None or not self._ready:
            return
        # persona 昵称活读（TTL 限频）：bot 回复信封行的称呼随宿主热切换
        await self._refresh_bot_nickname()
        messages = list(getattr(event, "messages", None) or [])
        if not messages:
            return
        session = getattr(event, "session", None)
        if session is None:
            return

        # 滚动补回：跳过宿主窗口内 K 批后直取最近 N 批同会话摘要（失败
        # 不阻断本轮）；构建先于 recall——memo 就位后 recall 排除已注入行。
        # platform 限定本适配器：入库 session_id 是裸 id，跨适配器同号会话
        # 不得占用 OFFSET 名额（近时排除子查询同口径）
        platform = session.adapter_name
        # 实体词典命中（一次匹配，两处消费：召回提示旁路 + 画像候选补位；
        # 匹配文本同时喂 P3 边注入——label 词形命中与名字命中同一文本）
        entity_entries: list = []
        match_text = ""
        if self._config.recall_hint_enabled:
            match_text = self._entity_match_text(messages)
            try:
                entity_entries = await kernel.entity_hint_entries(
                    session.sid, match_text
                )
            except Exception:
                logger.warning("实体词典匹配失败（忽略）", exc_info=True)
                entity_entries = []

        rollout_text = ""
        if self._config.recent_rollout_enabled:
            try:
                rollout_text = await kernel.build_recent_rollout_text(
                    session.session_id, platform=platform
                )
            except Exception:
                logger.warning("滚动补回摘要获取失败（不阻断本轮）", exc_info=True)

        memory_text = ""
        query = self._build_recall_query(messages)
        # 引用原文并入 query（v1.7.1 设计修订：引用反查旁路移除——被引用
        # 消息文本与批次正文一起走主路语义召回，会话隔离统一由
        # summary_recall_session_scoped 开关管）
        quote_text = self._reply_quote_text(messages, session.sid)
        if quote_text:
            query = (query + " " + quote_text).strip()[:_RECALL_QUERY_MAX_CHARS]
        user_id, _ = self._trigger_identity(messages)
        if query:
            # hints 组装（实体命中复用顶部匹配结果；只承载画像候选/P3
            # 信号，不再驱动召回候选拉取）。P3 场景 C 硬门槛：AT bot /
            # 引用 bot 消息的轮次才可命中 bot 边；私聊（一切非群会话）
            # 恒 True——整场对话就是对 bot 说的
            is_group = getattr(session, "session_type", "dm") == "gm"
            bot_addressed = (
                not is_group or self._bot_addressed(messages, session.sid)
            )
            hints = None
            if entity_entries or bot_addressed:
                hints = kernel.make_recall_hints(
                    entity_user_keys=[
                        f"{p}:{u}" if p else u for _, p, u in entity_entries
                    ],
                    match_text=match_text,
                    bot_addressed=bot_addressed,
                    # bot 平台 uid（首条消息后回填；未学到时空串=单形态降级）
                    bot_user_id=self._bot_user_id,
                )
            try:
                memory_text = await kernel.build_injection_text(
                    query=query,
                    session_id=session.session_id,
                    top_k=self._config.recall_top_k,
                    user_id=user_id,
                    platform=platform,
                    hints=hints,
                    cross_session=not self._config.summary_recall_session_scoped,
                )
            except Exception:
                logger.warning("记忆检索失败（不阻断本轮）", exc_info=True)

        profile_text = ""
        try:
            profile_text = await self._build_profile_text(
                event, session, messages, entity_entries=entity_entries
            )
        except Exception:
            logger.warning("用户画像获取失败（不阻断本轮）", exc_info=True)

        if rollout_text:
            req.system_prompt.append(
                Prompt(
                    content=rollout_text,
                    name=f"{PLUGIN_ID}:rollout",
                    source=PLUGIN_ID,
                )
            )
        if memory_text:
            req.system_prompt.append(
                Prompt(
                    content=memory_text,
                    name=f"{PLUGIN_ID}:recall",
                    source=PLUGIN_ID,
                )
            )
        if profile_text:
            req.system_prompt.append(
                Prompt(
                    content=profile_text,
                    name=f"{PLUGIN_ID}:profile",
                    source=PLUGIN_ID,
                )
            )

    async def _build_profile_text(
        self, event, session, messages, entity_entries: list | None = None
    ) -> str:
        """构建画像注入文本（bot 自身排除；候选 = 发送者 + 实体命中）。

        v1.7.1 设计修订：私聊（一切非群会话）同样消费实体命中——批次
        文本提及他人（别名/词典命中）时其画像并入候选（候选收集与群聊
        同款）；无扩员时保持单用户路径（输出格式与既有私聊一致）。
        """
        persona_service = self._persona_service
        if persona_service is None or not self._config:
            return ""
        platform = session.adapter_name
        is_group = getattr(session, "session_type", "dm") == "gm"
        candidates = self._persona_candidates(
            messages, platform, entity_entries=entity_entries
        )
        if not is_group:
            user_id, _ = self._trigger_identity(messages)
            if any(c.user_id != user_id for c in candidates):
                return await persona_service.build_multi_profile_text(
                    candidates=candidates, session_id=session.session_id
                )
            if not user_id:
                return ""
            return await persona_service.build_profile_text(
                user_id=user_id, session_id=session.session_id, platform=platform
            )
        if not candidates:
            return ""
        return await persona_service.build_multi_profile_text(
            candidates=candidates, session_id=session.session_id
        )

    def _persona_candidates(
        self,
        messages: list,
        platform: str,
        entity_entries: list | None = None,
    ) -> list[PersonaCandidate]:
        """从批次消息收集画像候选（去重保序，bot 自身排除，上限配置）。

        候选优先级：批次发送者 > 实体词典命中（批次文本提及的成员；
        与召回提示旁路共用同一命中结果，命中名直接作 display_name——
        保证画像标题与聊天称呼一致。误命中由上限截断与空画像栏兜底，
        被发送者级先收集的用户经 seen 去重跳过）。
        """
        seen: set[str] = set()
        candidates: list[PersonaCandidate] = []
        for msg in messages:
            sender = getattr(msg, "sender", None)
            uid = str(getattr(sender, "user_id", "") or "")
            if not uid or uid == self._bot_user_id:
                continue
            if uid in seen:
                continue
            seen.add(uid)
            nickname = str(getattr(sender, "nickname", "") or "")
            candidates.append(
                PersonaCandidate(
                    user_id=uid,
                    platform=platform,
                    display_name=nickname or uid,
                    nickname=nickname,
                )
            )
            if len(candidates) >= self._config.max_persona_profiles:
                return candidates
        # 实体词典命中补位（发送者之后；提及即候选）
        for name, ent_platform, ent_uid in entity_entries or []:
            if not ent_uid or ent_uid in seen or ent_uid == self._bot_user_id:
                continue
            seen.add(ent_uid)
            candidates.append(
                PersonaCandidate(
                    user_id=ent_uid,
                    platform=ent_platform or platform,
                    display_name=name or ent_uid,
                    nickname=name,
                )
            )
            if len(candidates) >= self._config.max_persona_profiles:
                break
        return candidates

    @staticmethod
    def _build_recall_query(messages: list) -> str:
        """批次文本拼接为 recall query（截断防向量请求过大）。"""
        texts: list[str] = []
        for msg in messages:
            if getattr(msg, "is_notice", False):
                continue
            text = NoriflowMemoryPlugin._chain_text(getattr(msg, "chain", None))
            if text:
                texts.append(text)
        query = " ".join(texts).strip()
        return query[:_RECALL_QUERY_MAX_CHARS]

    @staticmethod
    def _entity_match_text(messages: list) -> str:
        """实体词典匹配文本：Text 元素 + At 元素昵称（单独构造，不动
        retain 用的 _chain_text）。

        At 昵称进入匹配面是特性——AT 是最强提及信号，「@小王 你上次说的」
        正应命中小王；Reply 元素只含消息 ID 不含名字，不进匹配面。
        """
        parts: list[str] = []
        for msg in messages:
            if getattr(msg, "is_notice", False):
                continue
            chain = getattr(msg, "chain", None)
            if chain is None:
                continue
            try:
                elements = list(chain)
            except TypeError:
                elements = list(getattr(chain, "chain", None) or [])
            for ele in elements:
                if isinstance(ele, Text):
                    text = getattr(ele, "text", "") or ""
                    if text:
                        parts.append(text)
                elif isinstance(ele, At):
                    nick = getattr(ele, "nickname", "") or ""
                    if nick:
                        parts.append(f"@{nick}")
        return " ".join(parts)

    def _track_bot_message_id(self, sid: str, message_id: str) -> None:
        """bot 出站平台消息 ID 入会话追踪集（有界；会话数 LRU）。"""
        bucket = self._bot_message_ids.get(sid)
        if bucket is None:
            bucket = deque(maxlen=_BOT_MESSAGE_ID_LIMIT)
            self._bot_message_ids[sid] = bucket
        else:
            self._bot_message_ids.move_to_end(sid)
        if message_id not in bucket:
            bucket.append(message_id)
        while len(self._bot_message_ids) > _SESSION_CACHE_MAX_SESSIONS:
            self._bot_message_ids.popitem(last=False)

    def _bot_addressed(self, messages: list, sid: str) -> bool:
        """P3 场景 C 硬门槛：本轮输入 AT bot 或引用 bot 消息。

        AT 判定 = At 元素 pid 等于 bot uid（"all" 不算）；引用判定 =
        Reply 目标命中本会话 bot 出站消息 ID 追踪集。裸代词"你"永不
        构成 bot 命中——bot 名不进实体词典/别名表，名字匹配层结构性
        不含 bot，群聊随便一句话不会误触发 bot 边注入。
        """
        bot_uid = self._bot_user_id
        if not bot_uid:
            return False
        bot_mids = self._bot_message_ids.get(sid)
        for msg in messages:
            if getattr(msg, "is_notice", False):
                continue
            chain = getattr(msg, "chain", None)
            if chain is None:
                continue
            try:
                elements = list(chain)
            except TypeError:
                elements = list(getattr(chain, "chain", None) or [])
            for ele in elements:
                if isinstance(ele, At):
                    if str(getattr(ele, "pid", "") or "") == bot_uid:
                        return True
                elif isinstance(ele, Reply) and bot_mids:
                    mid = str(getattr(ele, "message_id", "") or "")
                    if mid and mid in bot_mids:
                        return True
        return False

    def _reply_quote_text(self, messages: list, sid: str) -> str:
        """被引用消息原文（并入 recall query 的引用文本）。

        v1.7.1 设计修订：引用反查旁路（锚点时间窗摘要拉取）移除，改为
        被引用消息原文并入 query 走主路语义召回——引用只能引用本会话
        消息，会话隔离由主路开关统一管。覆盖 = 会话滚动缓存
        （_SESSION_CACHE_LIMIT 行）；更早的引用目标不并入（缓存 miss
        无害，无引用文本）。信封行整行截断使用（含时间/说话人前缀——
        embedding 对前缀噪声不敏感，保留说话人反而有助召回）；
        每条截 200 字、每轮至多 2 条。
        """
        target_ids: set[str] = set()
        for msg in messages:
            if getattr(msg, "is_notice", False):
                continue
            chain = getattr(msg, "chain", None)
            if chain is None:
                continue
            try:
                elements = list(chain)
            except TypeError:
                elements = list(getattr(chain, "chain", None) or [])
            for ele in elements:
                if isinstance(ele, Reply):
                    mid = str(getattr(ele, "message_id", "") or "")
                    if mid:
                        target_ids.add(mid)
        if not target_ids:
            return ""
        bucket = self._history.get(sid)
        if not bucket:
            return ""
        texts: list[str] = []
        for key, cached in bucket.items():
            if key in target_ids and cached.line:
                texts.append(cached.line.strip()[:200])
        return " ".join(texts[:2]).strip()

    async def _directory_names(self, sid: str) -> list[tuple[str, str, str]]:
        """实体词典名字源：会话滚动缓存内观察到的发送者昵称/群名片。

        bot 行跳过（bot 名字会命中全部会话摘要，候选爆炸）；重启后
        缓存从零积累，词典随之自愈（TTL 兜底）。
        """
        bucket = self._history.get(sid)
        if not bucket:
            return []
        pairs: list[tuple[str, str, str]] = []
        for cached in bucket.values():
            if cached.is_bot or not cached.uid:
                continue
            if cached.nickname:
                pairs.append((cached.nickname, cached.platform, cached.uid))
            if cached.cardname:
                pairs.append((cached.cardname, cached.platform, cached.uid))
        return pairs

    async def _alias_upsert_batch(self, user_delta: list) -> None:
        """回合批次 sender 名字 -> 持久别名层 upsert（kira 侧名字流唯一来源）。

        kira 插件不读宿主用户表（两侧同构约定：消息流 -> 插件 alias 表）；
        变体拆分与上限截断在 build_alias_rows 内完成。失败仅记日志。
        """
        kernel = self._memory_kernel
        if (
            kernel is None
            or kernel.alias_store is None
            or self._db is None
            or not user_delta
        ):
            return
        now = datetime.now()
        raw: list[tuple[str, str, str, datetime]] = []
        for _, line in user_delta:
            if not line.uid or line.uid == self._bot_user_id:
                continue
            ts = line.timestamp or now
            for name in (line.nickname, line.cardname):
                if name:
                    raw.append((line.platform, line.uid, name, ts))
        if not raw:
            return
        try:
            rows = build_alias_rows(
                raw, source="batch", variant_cap=kernel.alias_store.variant_cap
            )
            await self._db.alias_upsert(rows)
            kernel.alias_store.apply_rows(rows)
        except Exception:
            logger.warning("持久别名批次 upsert 失败（跳过，不影响 retain）", exc_info=True)

    @staticmethod
    def _trigger_identity(messages: list) -> tuple[str, str]:
        """取批次触发者 (user_id, "")——platform 由调用方按 session 补齐。

        Returns:
            (首个非 notice 消息的发送者 uid，占位空 platform)。
        """
        for msg in messages:
            if getattr(msg, "is_notice", False):
                continue
            uid = str(getattr(getattr(msg, "sender", None), "user_id", "") or "")
            if uid:
                return uid, ""
        return "", ""

    # ------------------------------------------------------------------
    #  消息观察（retain 输入维护）
    # ------------------------------------------------------------------

    @on.im_message(priority=Priority.LOW)
    async def observe_message(self, event, *args, **kwargs):
        """入站消息入滚动行缓存（编码输入的历史窗口来源）。"""
        if not self._ready:
            return
        msg = getattr(event, "message", None)
        session = getattr(event, "session", None)
        if msg is None or session is None or getattr(msg, "is_notice", False):
            return
        text = self._chain_text(getattr(msg, "chain", None))
        if not text:
            return
        sender = getattr(msg, "sender", None)
        uid = str(getattr(sender, "user_id", "") or "")
        nickname = str(getattr(sender, "nickname", "") or "")
        self_id = str(getattr(msg, "self_id", "") or "")
        if self_id and not self._bot_user_id:
            self._bot_user_id = self_id
            self._propagate_bot_identity(self_id)
        line = format_history_message(
            speaker_name=nickname or uid,
            content=text,
            timestamp=self._to_datetime(getattr(msg, "timestamp", None)),
            user_id=uid,
            is_at_bot=bool(getattr(msg, "is_mentioned", False)),
        )
        self._append_history(
            session.sid,
            str(getattr(msg, "message_id", "") or id(msg)),
            line,
            uid=uid,
            platform=str(getattr(session, "adapter_name", "") or ""),
            nickname=nickname,
            cardname=str(
                (getattr(sender, "extra", None) or {}).get("cardname", "") or ""
            ),
            timestamp=self._to_datetime(getattr(msg, "timestamp", None)),
        )

    @on.message_sent(priority=Priority.MEDIUM)
    async def observe_sent(self, event, action, result, *args, **kwargs):
        """bot 发送观察：行入滚动缓存（retain 由回合完成信号触发）。

        不在此处触发编码——按发送段触发会在段间隔超过合并窗口时把同一
        批次重复编码（工具回合拆段/多段回复即中招）；核心的
        session_memory_updated 信号保证每回合恰好编码一次。
        """
        if not self._ready:
            return
        if result is not None and getattr(result, "is_notice", False):
            return  # 主动通知不入记忆（v1 不接 bot_self 通道）
        session = getattr(event, "session", None)
        if session is None:
            return
        text = self._chain_text(getattr(action, "chain", None) or action)
        # bot 出站平台消息 ID 入追踪集（场景 C 引用判定；result 为
        # KiraIMSentResult，message_id 由适配器回传，可能为 None）
        mid = str(getattr(result, "message_id", "") or "")
        if mid:
            self._track_bot_message_id(session.sid, mid)
        if not text:
            return

        now = datetime.now()
        for ln in text.split("\n"):
            if not ln.strip():
                continue
            line = format_history_message(
                speaker_name=self._bot_nickname,
                content=ln,
                timestamp=now,
                is_self_message=True,
            )
            self._bot_seq += 1
            self._append_history(session.sid, f"bot#{self._bot_seq}", line, is_bot=True)

    async def _on_session_memory_updated(self, event) -> None:
        """Round-end signal -> take one unconsumed increment batch and encode it.

        The host emits this exactly once after every conversation round
        (tool loops and multi-segment replies included). We take the
        not-yet-consumed increment from the rolling line cache and assemble
        the encode input from it, so the same batch can never be encoded
        twice by multi-segment sends. Bot-only increments (no new user
        lines) are deliberately not retained; bot lines stay unconsumed and
        ride along with the next round's batch.

        Concurrency: the take-increment -> mark-consumed window below
        contains no await (single-threaded event loop makes it atomic), so
        concurrently arriving signals for the same session can never take
        the same batch twice. The watermark MUST be advanced before the
        first await (the alias upsert performs real DB I/O and can suspend
        for up to db_command_timeout) — see the inline comment.
        """
        if not self._ready:
            return
        payload = getattr(event, "payload", None) or {}
        if not isinstance(payload, dict):
            return
        sid = str(payload.get("session", "") or "")
        parts = sid.split(":", maxsplit=2)
        if len(parts) != 3 or not all(parts) or parts[1] not in ("dm", "gm"):
            return  # 非会话型 session（系统消息等）不编码
        adapter, stype, bare = parts

        bucket = self._history.get(sid)
        if not bucket:
            return
        consumed = self._consumed.setdefault(sid, set())
        delta = [(k, v) for k, v in bucket.items() if k not in consumed]
        # 批次 = 增量中的用户行（bot 出站行走 bot 段；bot 自身入站回声行
        # 两段都不进——无记忆价值，直接标消费丢弃，仅留在缓存 history 里
        # 作后续批次的上下文）
        user_delta = [
            (k, v) for k, v in delta
            if not v.is_bot and v.uid != self._bot_user_id
        ]
        if not user_delta:
            return

        participants: list[tuple[str, str]] = []
        seen: set[str] = set()
        for _, v in user_delta:
            if v.uid and v.uid not in seen:
                seen.add(v.uid)
                participants.append((v.platform or adapter, v.uid))
        # Advance the watermark BEFORE the first await: the alias upsert
        # below is real DB I/O (suspends up to db_command_timeout). If it
        # sat inside the take->mark window, a concurrent signal for the same
        # session could take the exact same unconsumed increment and encode
        # it twice. The alias upsert is fail-open and does not depend on
        # watermark state, so marking first is safe.
        delta_keys = {k for k, _ in delta}
        consumed.update(delta_keys)
        self._prune_consumed(sid)
        # Persistent alias layer batch upsert (fail-open, independent of retain)
        await self._alias_upsert_batch(user_delta)

        state = _RetainState(
            event_id=f"{sid}#{int(datetime.now().timestamp())}",
            session_id=bare,
            platform=adapter,
            group_id=bare if stype == "gm" else "",
            user_id=user_delta[0][1].uid,
            timestamp=datetime.now(),
            participants=participants,
            history_lines=self._history_snapshot(sid, {k for k, _ in delta}),
            batch_lines=[v.line for _, v in user_delta],
            bot_lines=[v.line for _, v in delta if v.is_bot],
        )
        task = asyncio.create_task(self._do_retain(state, sid, delta_keys))
        self._retain_tasks.add(task)
        task.add_done_callback(self._retain_tasks.discard)

    def _prune_consumed(self, sid: str) -> None:
        """水位线只保留仍在缓存里的键（消息键不复用，逐出的可安全丢弃）。"""
        consumed = self._consumed.get(sid)
        bucket = self._history.get(sid)
        if consumed is None:
            return
        if bucket is None:
            self._consumed.pop(sid, None)
            return
        self._consumed[sid] = {k for k in bucket if k in consumed}

    def _rollback_consumed(self, sid: str, delta_keys: set[str]) -> None:
        """编码失败回滚水位线，让下一轮信号重新编码这批消息。"""
        consumed = self._consumed.get(sid)
        if consumed is not None:
            consumed.difference_update(delta_keys)

    async def _do_retain(
        self, state: _RetainState, sid: str = "", delta_keys: Optional[set[str]] = None
    ) -> None:
        """组装编码输入并写入（Semaphore 限流；失败回滚水位线，异常只记录）。"""
        # persona 昵称活读（TTL 限频）：编码 prompt 的 bot 排除规则消费
        await self._refresh_bot_nickname()
        kernel = self._memory_kernel
        if kernel is None:
            if sid:
                self._rollback_consumed(sid, delta_keys or set())
            return
        parts: list[str] = []
        history_text = "\n".join(state.history_lines)
        if history_text:
            parts.append(history_text)
            parts.append(HISTORY_BATCH_SEPARATOR)
        parts.extend(state.batch_lines)
        parts.extend(state.bot_lines)
        conversation = "\n".join(parts)
        async with self._retain_semaphore:
            try:
                doc_ids = await kernel.retain_encoded(
                    conversation_text=conversation,
                    session_id=state.session_id,
                    user_id=state.user_id,
                    platform=state.platform,
                    group_id=state.group_id,
                    timestamp=state.timestamp,
                    participant_user_ids=state.participants,
                )
                logger.info(
                    "retain 完成: session=%s doc_ids=%s",
                    state.session_id,
                    doc_ids,
                )
            except Exception:
                logger.warning(
                    "异步 retain 失败（水位线已回滚，本轮消息待下轮信号重编码）",
                    exc_info=True,
                )
                if sid:
                    self._rollback_consumed(sid, delta_keys or set())

    # ------------------------------------------------------------------
    #  行缓存辅助
    # ------------------------------------------------------------------

    def _append_history(
        self,
        sid: str,
        msg_key: str,
        line: str,
        uid: str = "",
        platform: str = "",
        is_bot: bool = False,
        nickname: str = "",
        cardname: str = "",
        timestamp: Optional[datetime] = None,
    ) -> None:
        """追加一行到会话滚动缓存（LRU 上限防膨胀；会话逐出时同步清水位线）。"""
        bucket = self._history.get(sid)
        if bucket is None:
            bucket = OrderedDict()
            self._history[sid] = bucket
        self._history.move_to_end(sid)
        bucket[msg_key] = _CachedLine(
            line=line, uid=uid, platform=platform, is_bot=is_bot,
            nickname=nickname, cardname=cardname, timestamp=timestamp,
        )
        bucket.move_to_end(msg_key)
        while len(bucket) > _SESSION_CACHE_LIMIT:
            bucket.popitem(last=False)
        while len(self._history) > _SESSION_CACHE_MAX_SESSIONS:
            # 会话逐出 = 缓存与水位线一起丢：该会话再次活跃时其缓存内
            # 旧消息全部按未消费增量整批重编（幂等键吸收大部分重复）
            evicted_sid, _ = self._history.popitem(last=False)
            self._consumed.pop(evicted_sid, None)
            logger.warning(
                "会话缓存超上限（%d），逐出最久未活跃会话 %r"
                "（其再次活跃时旧增量将整批重编码）",
                _SESSION_CACHE_MAX_SESSIONS,
                evicted_sid,
            )

    def _history_snapshot(self, sid: str, exclude_ids: set[str]) -> list[str]:
        """取会话最近 N 行（排除本轮增量消息），时间序（旧 -> 新）。"""
        bucket = self._history.get(sid)
        if not bucket:
            return []
        lines = [
            v.line for k, v in bucket.items() if k and k not in exclude_ids
        ]
        return lines[-_HISTORY_WINDOW_LINES:]

    @staticmethod
    def _chain_text(chain: Any) -> str:
        """从消息链提取纯文本（只取 Text 元素）。"""
        if chain is None:
            return ""
        parts: list[str] = []
        try:
            elements = list(chain)
        except TypeError:
            elements = list(getattr(chain, "chain", None) or [])
        for ele in elements:
            if isinstance(ele, Text):
                parts.append(getattr(ele, "text", "") or "")
        return "".join(parts).strip()

    @staticmethod
    def _to_datetime(ts: Any) -> Optional[datetime]:
        """时间戳 -> datetime（秒/毫秒自适应；非法返回 None）。"""
        if not ts:
            return None
        try:
            value = float(ts)
            if value > 1e12:
                value /= 1000.0
            return datetime.fromtimestamp(value)
        except (TypeError, ValueError, OSError, OverflowError):
            return None

    def _pool_or_503(self):
        """维护 API 公共守卫：kernel 未就绪时 503。

        返回 backend 对象（而非裸池）——webui_store 双后端 SQL 派发按
        ``backend.dialect`` 判定方言，裸池缺该属性会被兜底成 postgres，
        在 SQLite 后端下把 PG 方言 SQL 打进 SQLite（::token 语法错误）。
        webui_store 内部经 ``_pool(backend)`` 取池，调用点无需改动。
        """
        db = self._db
        pool = None
        if db is not None:
            try:
                pool = db.pool
            except RuntimeError:
                pool = None
        if pool is None:
            raise HTTPException(status_code=503, detail="记忆功能未启用或数据库不可用")
        return db

    # ------------------------------------------------------------------
    #  主动记忆工具（enabled_tools 门控见 inject_memory）
    # ------------------------------------------------------------------

    @staticmethod
    def _event_scope(event) -> tuple[str, str, str]:
        """Derive (session_id, user_id, platform) from the triggering event.

        All getattr-based: event may be None (direct/manual calls) or lack
        session/message attributes.
        """
        session = getattr(event, "session", None)
        if session is None:
            return "", "", ""
        sid = str(getattr(session, "session_id", "") or "")
        plat = str(getattr(session, "adapter_name", "") or "")
        msg = getattr(event, "message", None)
        sender = getattr(msg, "sender", None) if msg is not None else None
        uid = str(getattr(sender, "user_id", "") or "") if sender is not None else ""
        return sid, uid, plat

    @staticmethod
    def _trigger_user(event) -> tuple[str, str]:
        """(user_id, platform) of the message that triggered this LLM turn.

        Framework tool dispatch passes a batch event carrying ``messages``
        (no ``message`` attr): the first non-notice sender is the trigger
        user, the same convention recall uses in _trigger_identity. Single
        message events (and manual calls in tests) may only carry
        ``message``. Returns ("", "") when no identity can be derived.
        """
        uid = ""
        messages = list(getattr(event, "messages", None) or [])
        if messages:
            uid, _ = NoriflowMemoryPlugin._trigger_identity(messages)
        if not uid:
            msg = getattr(event, "message", None)
            sender = getattr(msg, "sender", None) if msg is not None else None
            uid = str(getattr(sender, "user_id", "") or "") if sender is not None else ""
        session = getattr(event, "session", None)
        plat = str(getattr(session, "adapter_name", "") or "") if session is not None else ""
        return uid, plat

    def _allowed_user_ids(self) -> list[str]:
        """Normalized allowed_users whitelist, read live from plugin config."""
        return _cfg_strlist(self.plugin_cfg or {}, "allowed_users")

    def _allowed_session_ids(self) -> list[str]:
        """Normalized allowed_sessions whitelist, read live from plugin config."""
        return _cfg_strlist(self.plugin_cfg or {}, "allowed_sessions")

    @staticmethod
    def _session_whitelist_keys(sid: str, platform: str, raw_id: str = "") -> set[str]:
        """All whitelist keys a triggering session can match.

        A session ``napcat:gm:10086`` matches bare ``10086``, platform-
        prefixed ``napcat:10086`` and the full sid ``napcat:gm:10086``;
        keeping bare/platform forms lets one entry cover both dm and gm
        sessions of the same peer without cross-platform collisions.
        ``raw_id`` (the session_id attribute verbatim) is included too so
        entries match regardless of how the host composes sid.
        """
        bare = sid.rsplit(":", 1)[-1] if sid else ""
        keys = {sid} if sid else set()
        if bare and bare != sid:
            keys.add(bare)
        if platform and bare:
            keys.add(f"{platform}:{bare}")
        if raw_id:
            keys.add(raw_id)
            if platform and raw_id not in (bare, sid):
                keys.add(f"{platform}:{raw_id}")
        return keys

    def _session_whitelist_denial(self, event) -> Optional[str]:
        """Session whitelist gate: allow when the triggering session matches.

        Complements the user whitelist: a caller passes when either the
        trigger user is in allowed_users or the trigger session is in
        allowed_sessions (group chats where any member may call).

        Returns None when the session matches the whitelist, the sentinel
        ``"skip"`` when no session whitelist is configured (nothing to
        allow here; the caller then decides via the user whitelist alone),
        otherwise the denial text.
        """
        allowed = self._allowed_session_ids()
        if not allowed:
            # No session whitelist configured: nothing to allow here; the
            # caller decides via the user whitelist (fail-closed when both
            # are empty).
            return "skip"
        session = getattr(event, "session", None)
        sid = str(getattr(session, "sid", "") or "") if session is not None else ""
        raw_id = (
            str(getattr(session, "session_id", "") or "")
            if session is not None
            else ""
        )
        platform = (
            str(getattr(session, "adapter_name", "") or "")
            if session is not None
            else ""
        )
        keys = self._session_whitelist_keys(sid, platform, raw_id)
        if keys & set(allowed):
            return None
        return "权限不足：当前会话不在记忆工具白名单（allowed_sessions）内，无法调用该工具"

    def _whitelist_denial(self, event, tool_name: str = "") -> Optional[str]:
        """Code-level whitelist gate for the proactive memory tools.

        Two complementary dimensions, either one allowing the call:
        - allowed_users, keyed on the triggering user id: entries match a
          bare user id or ``<platform>:<user_id>`` (platform prefix
          disambiguates cross-platform id collisions)
        - allowed_sessions, keyed on the triggering session: entries match
          a bare session id, ``<platform>:<session_id>`` or the full sid
          ``<platform>:<type>:<session_id>``

        Fail-closed: when both lists are empty/missing every caller is
        denied, as is an event whose trigger user and session cannot be
        derived.

        Returns None when allowed, otherwise the denial text for the LLM.
        """
        allowed = self._allowed_user_ids()
        if allowed:
            uid, platform = self._trigger_user(event)
            if uid and (uid in allowed or f"{platform}:{uid}" in allowed):
                return None
        session_denial = self._session_whitelist_denial(event)
        if session_denial is None:
            return None
        if not allowed and session_denial == "skip":
            # neither whitelist configured: fail-closed for everyone
            if tool_name:
                logger.warning(
                    "记忆工具 %s 被白名单拦截（allowed_users 与 allowed_sessions 均为空）",
                    tool_name,
                )
            return "权限不足：当前用户不在记忆工具白名单（allowed_users）内，无法调用该工具"
        if tool_name:
            # Log only real tool invocations; the per-request tool_set
            # removal in inject_memory shares this check and must stay
            # silent (it runs on every turn for non-whitelisted users).
            logger.warning(
                "记忆工具 %s 被白名单拦截（触发用户 %r 不在 allowed_users、"
                "触发会话不在 allowed_sessions 内）",
                tool_name, self._trigger_user(event)[0],
            )
        return "权限不足：当前用户不在记忆工具白名单（allowed_users / allowed_sessions）内，无法调用该工具"

    def _proactive_tools_denied(self, event) -> bool:
        """True when the whitelist hides/removes all proactive memory tools."""
        return self._whitelist_denial(event) is not None

    def _tool_scope_lock_enabled(self) -> bool:
        """工具作用域锁定开关（tool_scope_locked，默认开）。"""
        config = self._config
        return bool(config is not None and config.tool_scope_locked)

    @register.tool(
        name="memory_search",
        description="搜索长期记忆库中的历史记忆（对话摘要）。当需要回忆过去聊过的内容、用户提到「之前说过/上次聊过」时调用。",
        params={
            "type": "object",
            "properties": {
                "query": {"type": "string", "description": "检索内容（自然语言）"},
                "top_k": {"type": "number", "description": "返回条数上限，默认 5"},
                "session_id": {"type": "string", "description": "会话 ID（裸会话 ID，非 platform:type:id 完整格式；提供则按会话检索；不填默认当前会话）"},
                "user_id": {"type": "string", "description": "用户 ID（提供则按用户跨会话检索）"},
            },
            "required": ["query"],
        },
    )
    # The framework invokes tools as func(event, **args): the first parameter
    # after self receives the message event, tool params arrive by keyword.
    async def memory_search(self, event, *_, query: str, top_k: int = 5, session_id: str = "", user_id: str = "") -> str:
        denial = self._whitelist_denial(event, "memory_search")
        if denial:
            return denial
        kernel = self._memory_kernel
        if kernel is None or not self._ready:
            return "记忆功能未启用"
        query = (query or "").strip()
        if not query:
            return "query 不能为空"
        sid = (session_id or "").strip()
        uid = (user_id or "").strip()
        plat = ""
        if self._tool_scope_lock_enabled():
            # 作用域锁定：忽略 LLM 显式传入的 session_id/user_id，钉死为
            # 触发会话/触发者——防白名单命中的调用方借参数横向检索
            # 其他会话/其他用户的记忆
            sid, uid, plat = self._event_scope(event)
        elif not sid and not uid:
            # Neither scope given by the LLM: fall back to the triggering
            # session. Explicit session_id/user_id keep their intended scope
            # (platform then unknown——裸 id 检索不限平台，兼容旧行为).
            sid, uid, plat = self._event_scope(event)
        try:
            top_k = max(1, min(int(top_k or 5), 10))
        except (TypeError, ValueError):
            top_k = 5
        # Asking-about-others recall: match the query against the entity
        # dictionary + persistent aliases; the keys then join the main search
        # path. Isolation posture follows summary_recall_session_scoped via
        # cross_session below (session-pinned entity keys when isolated).
        entity_keys: list[str] = []
        entity_uids: list[str] = []
        if sid and self._config.recall_hint_enabled:
            try:
                entries = await kernel.entity_hint_entries(sid, query)
                entity_keys = [f"{p}:{u}" if p else u for _, p, u in entries]
                entity_uids = [u for _, _, u in entries]
            except Exception:
                logger.warning("memory_search 实体匹配失败（忽略）", exc_info=True)
        try:
            if sid:
                items = await kernel.search(
                    query=query, top_k=top_k, session_id=sid,
                    user_id=uid, platform=plat, scope="session",
                    cross_session=not self._config.summary_recall_session_scoped,
                    entity_user_keys=entity_keys or None,
                    entity_user_ids=entity_uids or None,
                )
            elif uid:
                items = await kernel.search(
                    query=query, top_k=top_k, user_id=uid, scope="user",
                )
            else:
                return "需要提供 session_id 或 user_id 之一（当前会话 ID 见上下文）"
        except Exception:
            logger.warning("memory_search 工具失败", exc_info=True)
            return "记忆检索暂时不可用，请稍后再试"
        if not items:
            return "没有找到相关记忆"
        return "\n".join(
            f"- [{it.id}] ({it.score:.2f}) {it.content}" for it in items
        )

    @register.tool(
        name="memory_write",
        description="把一条信息写入长期记忆库。仅当用户明确要求记住某事时调用。",
        params={
            "type": "object",
            "properties": {
                "text": {"type": "string", "description": "要记住的内容（完整陈述句）"},
                "session_id": {"type": "string", "description": "归属会话 ID（不填默认当前会话）"},
                "user_id": {"type": "string", "description": "归属用户 ID（不填默认当前会话用户）"},
                "platform": {"type": "string", "description": "平台标识（如 napcat；不填自动从当前会话推断）"},
            },
            "required": ["text"],
        },
    )
    async def memory_write(self, event, *_, text: str, session_id: str = "", user_id: str = "", platform: str = "") -> str:
        denial = self._whitelist_denial(event, "memory_write")
        if denial:
            return denial
        kernel = self._memory_kernel
        if kernel is None or not self._ready:
            return "记忆功能未启用"
        text = (text or "").strip()
        if not text:
            return "text 不能为空"
        sid = (session_id or "").strip()
        uid = (user_id or "").strip()
        plat = (platform or "").strip()
        if self._tool_scope_lock_enabled():
            # 作用域锁定：写入归属钉死为触发会话/触发者（防越权写入
            # 他人会话名下伪造记忆）；作用域派生失败时与 memory_remove
            # 同口径显式拒绝，不静默落一行空归属记忆
            sid, uid, plat = self._event_scope(event)
            if not (sid or uid):
                return "作用域锁定开启且无法识别当前会话/用户，拒绝写入"
        elif not (sid and uid and plat):
            ev_sid, ev_uid, ev_plat = self._event_scope(event)
            sid = sid or ev_sid
            uid = uid or ev_uid
            plat = plat or ev_plat
        pairs = [(plat, uid)] if plat and uid else None
        try:
            doc_id = await kernel.ingest(
                content=text,
                session_id=sid,
                user_id=uid,
                platform=plat,
                kind="chat_summary",
                timestamp=datetime.now(),
                participant_user_ids=pairs,
            )
        except Exception:
            logger.warning("memory_write 工具失败", exc_info=True)
            return "写入失败：记忆库暂时不可用，请稍后再试"
        return f"已写入长期记忆（id={doc_id}）"

    @register.tool(
        name="memory_remove",
        description="按 ID 删除一条长期记忆。先用 memory_search 查到记忆条目的 [id] 再调用。",
        params={
            "type": "object",
            "properties": {
                "document_id": {"type": "string", "description": "记忆条目 ID（来自 memory_search 结果）"},
            },
            "required": ["document_id"],
        },
    )
    async def memory_remove(self, event, *_, document_id: str) -> str:
        denial = self._whitelist_denial(event, "memory_remove")
        if denial:
            return denial
        kernel = self._memory_kernel
        if kernel is None or not self._ready:
            return "记忆功能未启用"
        doc_id = (document_id or "").strip()
        if not doc_id:
            return "document_id 不能为空"
        scope_sid = scope_uid = ""
        if self._tool_scope_lock_enabled():
            # 作用域锁定：删除下推触发会话/触发者归属限定——越出作用域的
            # document_id（他人会话/他人名下）静默不可删
            scope_sid, scope_uid, _ = self._event_scope(event)
            if not (scope_sid or scope_uid):
                return "作用域锁定开启且无法识别当前会话/用户，拒绝删除"
        try:
            removed = await kernel.forget_summary(
                doc_id, scope_session_id=scope_sid, scope_user_id=scope_uid
            )
        except Exception:
            logger.warning("memory_remove 工具失败", exc_info=True)
            return "删除失败：记忆库暂时不可用，请稍后再试"
        if removed:
            return "已删除"
        if scope_sid or scope_uid:
            return "记忆条目不存在或不属于当前会话/用户"
        return "记忆条目不存在"

    # ------------------------------------------------------------------
    #  维护页 + API（数据层见 webui_store.py）
    # ------------------------------------------------------------------

    # 注意：页面必须挂在非空子路径（"/dashboard"）而非根路径（"/"）——
    # 根路径挂载经 Starlette Mount rstrip 后存为 /page/plugin/<id>（无尾
    # 斜杠），宿主清理逻辑按带尾斜杠前缀 startswith 匹配不上，热重载后
    # 残留旧挂载遮蔽重新注册的路由（表现为维护页 404）；非空子路径的
    # 挂载可被正常卸载。目录挂载本身是 StaticFiles(html=True)，目录请求
    # 自动回退 index.html 且 content-type 正确，无需 from_html 别名页。
    @register.page(
        "/dashboard",
        menu=PageMenu(label={"zh": "长期记忆", "en": "Long-term Memory"}, icon="Coin", order=85),
    )
    def memory_page(self):
        return PluginPage.from_folder("./web")

    @register.api(method="GET", path="/memory/overview")
    async def api_memory_overview(self):
        return await webui_store.fetch_overview(self._pool_or_503())

    @register.api(method="GET", path="/memory/users")
    async def api_memory_users(self, q: str = "", page: int = 1, size: int = 20):
        return await webui_store.fetch_users(
            self._pool_or_503(), keyword=q or "", page=page, size=size
        )

    @register.api(method="GET", path="/memory/facts")
    async def api_memory_facts(
        self,
        page: int = 1,
        size: int = 20,
        user_id: str = "",
        session_id: str = "",
        category: str = "",
        confidence: str = "",
        extracted: str = "",
        q: str = "",
    ):
        return await webui_store.fetch_facts(
            self._pool_or_503(), page, size,
            user_id=user_id or "", session_id=session_id or "",
            category=category or "", confidence=confidence or "",
            extracted=extracted or "", q=q or "",
        )

    @register.api(method="DELETE", path="/memory/facts/{fact_id}")
    async def api_memory_fact_delete(self, fact_id: int):
        return await webui_store.delete_fact(self._pool_or_503(), fact_id)

    @register.api(method="GET", path="/memory/clusters")
    async def api_memory_clusters(
        self,
        page: int = 1,
        size: int = 20,
        user_id: str = "",
        category: str = "",
        status: str = "",
        session_id: str = "",
        q: str = "",
    ):
        return await webui_store.fetch_clusters(
            self._pool_or_503(), page, size,
            user_id=user_id or "", category=category or "",
            status=status or "", session_id=session_id or "", q=q or "",
        )

    @register.api(method="PUT", path="/memory/clusters/{cluster_id}")
    async def api_memory_cluster_update(self, cluster_id: int, body: dict = Body(...)):
        return await webui_store.update_cluster(self._pool_or_503(), cluster_id, body)

    @register.api(method="GET", path="/memory/profile/{platform}/{user_id}")
    async def api_memory_profile(self, platform: str, user_id: str):
        if self._db is None or self._persona_service is None:
            raise HTTPException(status_code=503, detail="记忆功能未启用或数据库不可用")
        return await webui_store.fetch_profile_preview(
            self._db, self._persona_service, platform, user_id
        )

    @register.api(method="DELETE", path="/memory/users/{platform}/{user_id}")
    async def api_memory_user_delete(self, platform: str, user_id: str):
        """Per-user memory erasure: cascade delete across plugin-owned tables.

        Covers aliases, fact clusters, raw facts, profile projections and
        relation edges owned by (platform, user_id). Chat summaries are
        session-scoped and may involve other members, so they are not
        cascaded — remove individual rows from the summaries tab if needed.
        """
        if self._db is None:
            raise HTTPException(status_code=503, detail="记忆功能未启用或数据库不可用")
        return await webui_store.delete_user_memories(
            self._pool_or_503(), platform, user_id
        )

    @register.api(method="GET", path="/memory/summaries")
    async def api_memory_summaries(
        self,
        page: int = 1,
        size: int = 20,
        session_id: str = "",
        user_id: str = "",
        kind: str = "",
        q: str = "",
    ):
        return await webui_store.fetch_summaries(
            self._pool_or_503(), page, size,
            session_id=session_id or "", user_id=user_id or "",
            kind=kind or "", q=q or "",
        )

    @register.api(method="PUT", path="/memory/summaries/{summary_id}")
    async def api_memory_summary_update(self, summary_id: int, body: dict = Body(...)):
        return await webui_store.update_summary(self._pool_or_503(), summary_id, body)

    @register.api(method="DELETE", path="/memory/summaries/{summary_id}")
    async def api_memory_summary_delete(self, summary_id: int):
        return await webui_store.delete_summary(self._pool_or_503(), summary_id)

    # ------------------------------------------------------------------
    #  关系图谱（P2 边表审计视图 + 存量回填）
    # ------------------------------------------------------------------

    @register.api(method="GET", path="/memory/relations/graph")
    async def api_relations_graph(self):
        data = await webui_store.fetch_relation_graph(
            self._pool_or_503(), bot_user_id=self._bot_user_id
        )
        ctl = self._relation_controller
        data["backfill_available"] = ctl is not None and ctl.available()
        return data

    @register.api(method="PUT", path="/memory/relations/edges/{edge_id}")
    async def api_relations_edge_update(self, edge_id: int, body: dict = Body(...)):
        return await webui_store.update_relation_edge(
            self._pool_or_503(), edge_id, body
        )

    @register.api(method="DELETE", path="/memory/relations/edges/{edge_id}")
    async def api_relations_edge_delete(self, edge_id: int):
        return await webui_store.delete_relation_edge(self._pool_or_503(), edge_id)

    @register.api(method="POST", path="/memory/relations/backfill")
    async def api_relations_backfill(self, full: bool = False):
        """full=True 忽略水位全量重跑（默认增量：只回填上次进度之后的新簇）。"""
        ctl = self._relation_controller
        if ctl is None:
            raise HTTPException(status_code=503, detail="回填控制器未装配")
        started, message = ctl.start(full=full)
        if not started:
            raise HTTPException(status_code=409, detail=message)
        logger.info("[WebUI] 存量关系回填已启动（mode=%s）", "full" if full else "incremental")
        return {"started": True}

    @register.api(method="GET", path="/memory/relations/backfill/status")
    async def api_relations_backfill_status(self):
        ctl = self._relation_controller
        if ctl is None:
            return {"running": False, "available": False}
        state = dict(ctl.state)
        state["available"] = True
        return state

    # ------------------------------------------------------------------
    #  Manual encode kick (summaries-tab button: wake the merge loop now)
    # ------------------------------------------------------------------

    @register.api(method="POST", path="/memory/encode/backfill")
    async def api_encode_backfill_run(self):
        """Kick the merge loop into an immediate cycle.

        The loop stays the only runner: kick() resolves its inter-cycle
        wait early, so a manual cycle can never overlap the periodic one.
        409 = a cycle is executing or the startup grace window is still
        elapsing; 503 = merge pipeline not assembled (fast LLM unset).
        """
        agent = self._merge_agent
        if agent is None:
            raise HTTPException(
                status_code=503, detail="合并管线未装配（fast LLM 未配置时合并关闭）"
            )
        if not agent.kick():
            raise HTTPException(
                status_code=409, detail="合并周期执行中或启动预热未结束，无需重复触发"
            )
        logger.info("[WebUI] 补编码手动触发：合并周期被立即唤醒")
        return {"kicked": True}

    @register.api(method="GET", path="/memory/encode/backfill/status")
    async def api_encode_backfill_status(self):
        """Status for the encode button: backlog size + last cycle result.

        backlog counts summarized=false rows via the same interface the
        re-encode pass consumes (capped at 1000 — counting only, rows are
        not read out); None = the backend probe failed on this call.
        """
        agent = self._merge_agent
        if agent is None:
            return {
                "available": False, "cycle_running": False,
                "backlog": None, "backlog_capped": False, "last_cycle": None,
            }
        backlog = None
        try:
            rows = await self._pool_or_503().fetch_unsummarized_summaries(1000)
            backlog = len(rows)
        except Exception:
            backlog = None
        return {
            "available": True,
            "cycle_running": agent.cycle_running,
            "backlog": backlog,
            "backlog_capped": bool(backlog and backlog >= 1000),
            "last_cycle": agent.last_cycle,
        }

    # ------------------------------------------------------------------
    #  Kira 记忆数据迁入（只读源 + 幂等键写入；维护页设置栏触发）
    # ------------------------------------------------------------------

    def _kira_memory_bot_identity(self) -> tuple[str, str, str]:
        """global 域事实的归属 bot（platform/uid/昵称；装配未就绪时空串）。"""
        platform = ""
        uid = ""
        nickname = ""
        kernel = self._memory_kernel
        if kernel is not None:
            platform = str(getattr(kernel, "bot_platform", "") or "")
            uid = str(getattr(kernel, "bot_id", "") or "")
            nickname = str(getattr(kernel, "bot_nickname", "") or "")
        return platform, uid, nickname

    @register.api(method="GET", path="/memory/kira_memory/import")
    async def api_kira_memory_import_preview(self, source_path: str = ""):
        """迁入预览：源扫描计数 + 最近一次迁入结果（不写任何数据）。"""
        path = source_path.strip() or default_source_path(self._plugin_data_dir())
        preview = scan_source(path)
        backend = self._pool_or_503() if preview["exists"] else None
        last_run = await read_last_run(backend) if backend is not None else None
        return {
            "default_path": default_source_path(self._plugin_data_dir()),
            "source_path": preview["source_path"],
            "preview": preview,
            "last_run": last_run,
        }

    @register.api(method="POST", path="/memory/kira_memory/import")
    async def api_kira_memory_import_run(self, body: dict = Body(None)):
        """执行迁入（源只读；目标端幂等，重复执行自动去重）。

        body 可选 {"source_path": "..."}；缺省用推断的宿主 data/memory。
        """
        if getattr(self, "_kira_memory_import_running", False):
            raise HTTPException(status_code=409, detail="已有一次迁入在执行中")
        backend = self._pool_or_503()
        body = body if isinstance(body, dict) else {}
        path = str(body.get("source_path") or "").strip() or default_source_path(
            self._plugin_data_dir()
        )
        bot_platform, bot_uid, bot_nickname = self._kira_memory_bot_identity()
        self._kira_memory_import_running = True
        try:
            stats = await run_kira_memory_import(
                backend,
                source_path=path,
                bot_platform=bot_platform,
                bot_uid=bot_uid,
                bot_nickname=bot_nickname,
            )
        except FileNotFoundError as exc:
            raise HTTPException(status_code=404, detail=str(exc))
        except ValueError as exc:
            raise HTTPException(status_code=422, detail=str(exc))
        finally:
            self._kira_memory_import_running = False
        logger.info(
            "[WebUI] Kira 记忆数据迁入完成: summary_rows=%s fact_rows=%s aliases=%s",
            stats.get("summary_rows"), stats.get("fact_rows"),
            stats.get("aliases_upserted"),
        )
        return {"ok": True, "stats": stats}

    # ------------------------------------------------------------------
    #  设置栏（插件配置读写：真相源在宿主，运行时实例就地热更新）
    # ------------------------------------------------------------------

    def _plugin_id(self) -> str:
        """manifest.json 的 plugin_id（宿主配置存储键；启动后缓存）。"""
        if getattr(self, "_cached_plugin_id", None):
            return self._cached_plugin_id
        try:
            manifest = json.loads(
                (Path(__file__).resolve().parent / "manifest.json").read_text(
                    encoding="utf-8"
                )
            )
            pid = str(manifest.get("plugin_id") or "")
        except Exception:
            pid = ""
        self._cached_plugin_id = pid
        return pid

    @register.api(method="GET", path="/memory/config")
    async def api_memory_config_get(self):
        pm = self.ctx.plugin_mgr
        if pm is None or self._config is None:
            raise HTTPException(status_code=503, detail="配置端点未装配")
        host_values = pm.get_plugin_config(self._plugin_id()) or {}
        return config_web.build_payload(host_values)

    @register.api(method="PUT", path="/memory/config")
    async def api_memory_config_put(self, body: dict = Body(...)):
        """保存配置：校验 -> 就地热更新运行时实例 -> 同步宿主真相源。

        不走 pm.update_plugin_config（那会 init_plugin 整体重初始化、拆池
        重建）——直接更新宿主内存 dict 并落盘 PLUGIN_CONFIG_DIR/<pid>.json，
        与宿主配置页读内存的口径一致；kernel/merge_agent/encoder/熔断器
        运行时按次读同一 LocalMemoryConfig 实例，保存即生效。装配期展开
        的字段（restart_required）如实返回，需重初始化/重启进程生效。
        """
        pm = self.ctx.plugin_mgr
        if pm is None or self._config is None:
            raise HTTPException(status_code=503, detail="配置端点未装配")
        values = body.get("values") if isinstance(body, dict) else None
        if not isinstance(values, dict):
            raise HTTPException(status_code=422, detail="缺少 values（配置对象）")

        pid = self._plugin_id()
        host_values = dict(pm.get_plugin_config(pid) or {})
        try:
            result = config_web.prepare_save(values, host_values)
        except config_web.SaveBlocked as exc:
            raise HTTPException(status_code=400, detail=str(exc))
        except ValueError as exc:
            raise HTTPException(status_code=422, detail=str(exc))

        changed = result["changed"]
        if not changed:
            return {"saved": False, "changes": 0, "restart_required": []}

        # 1) 运行时实例就地更新（kernel/merge_agent/encoder/熔断器共享引用即刻可见；
        #    validated 已通过模型校验，赋值安全）
        for name in config_web.config_schema():
            setattr(self._config, name, getattr(result["validated"], name))
        # timezone 变更后使内核时区缓存失效：_local_tz_cache 首次解析后不再
        # 失效（相对时间标注/幂等键日期口径共用），不重置则继续用旧时区
        if "timezone" in changed and self._memory_kernel is not None:
            self._memory_kernel.reset_local_tz_cache()

        # 2) 宿主真相源同步：内存 dict（宿主配置页/下次 init_plugin 读）+ 磁盘
        merged_host = dict(host_values)
        merged_host.update(changed)
        pm.plugin_configs[pid] = merged_host
        persistence_error = ""
        try:
            from core.plugin.plugin_registry import PLUGIN_CONFIG_DIR

            config_web.persist_host_config(
                PLUGIN_CONFIG_DIR, pid, changed, host_values
            )
        except Exception as exc:
            persistence_error = str(exc)
            logger.warning(
                "宿主配置文件落盘失败（运行时已热生效，宿主重启后回退旧值）",
                exc_info=True,
            )

        logger.info(
            "[WebUI] 插件配置已保存(维护页设置栏): changes=%s keys=%s restart=%s",
            len(changed), sorted(changed), result["restart_required"] or "无",
        )
        resp = {
            "saved": True,
            "changes": len(changed),
            "restart_required": result["restart_required"],
        }
        if persistence_error:
            resp["persistence_error"] = persistence_error
        return resp
