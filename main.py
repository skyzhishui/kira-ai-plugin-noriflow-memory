"""NoriFlow long-term memory plugin (KiraAI edition, PostgreSQL + pgvector / SQLite dual backend).

Ported from nori-core nori_plugin_noriflow_memory. Difference from tool/label
plugins: this is a service plugin whose core value is the automatic memory
pipeline:

- Write (retain): listen on @on.im_message / @on.message_sent to maintain a
  per-session rolling line cache; subscribe to the core session_memory_updated
  event (emitted exactly once per conversation turn - including tool loops -
  after all messages are sent) and take incremental batches by message
  watermark to assemble encoding input (history window + separator markers +
  current batch + bot reply, byte-aligned with the memory_encode.prompt
  contract), encoded end-side via FastLlmExit (fast tier) and written through
  the dual channel of "fidelity summary + persona facts"; on encoding failure,
  fail-open degrades to the single original-text channel and rolls back the
  watermark; while the DB circuit breaker rejects, retain fails directly and
  rolls back the watermark (the batch waits for re-encoding on the next round,
  without wasting encoding and vectorization calls); content is never lost.
- Recall (recall): on @on.llm_request, vector-retrieve with the batch text as
  the query + rerank, injected as an independent Prompt; the user persona is
  deterministically assembled from the cluster table (no LLM) and injected too.
- Active tools: memory_search / memory_write / memory_remove +
  memory_profile / memory_lookup / memory_correct (all six are controlled
  by the enabled_tools multi-select config; the latter three are
  additionally gated by the memory_tools_enabled master switch; the
  allowed_users user whitelist and allowed_sessions session whitelist do
  code-level interception - matched by trigger user or trigger session,
  any hit is allowed through, both empty = all rejected; user entries
  support user_id or platform:user_id, session entries support session_id,
  platform:session_id or a full sid platform:type:session_id).
- Maintenance: @register.page maintenance page + 24 @register.api endpoints
  (overview/user/facts/cluster/persona/summary/graph/edge edit/backfill/re-
  encoding/import/settings browse and fix), data layer in webui_store.py.

graceful degradation: dsn not configured / DB connection or migration failure
/ fast LLM not configured - all skip the corresponding capability and log,
without blocking host startup; when the kernel is not ready, all memory tools
are removed automatically. Once assembly is fully ready, the built-in
kira_plugin_simple_memory is disabled automatically (officially required
before using third-party memory plugins; disabled only after this plugin is
ready, to avoid a memory vacuum).

Config defaults contain no real addresses; fill in dsn in the deployment-side
WebUI plugin config.
"""

from __future__ import annotations

import asyncio
import itertools
import json
import math
from collections import OrderedDict, deque
from dataclasses import dataclass, field
from datetime import datetime, timezone
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
from .entity_edge import relation_statement_line
from .envelope import HISTORY_BATCH_SEPARATOR, format_history_message
from .kira_memory_import import (
    default_source_path,
    migrate_legacy_import_kv,
    read_host_last_run,
    read_last_run,
    run_host_db_import,
    run_kira_memory_import,
    scan_host_sessions,
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

# 旧三件（enabled_tools 管理；search/write 已升级为 nori 双语义）
_LEGACY_TOOLS = ["memory_search", "memory_write", "memory_remove"]
# 对齐 nori 五件套新增的三件（memory_tools_enabled 总开关管理）
_PARITY_TOOLS = ["memory_profile", "memory_lookup", "memory_correct"]
ALL_TOOLS = _LEGACY_TOOLS + _PARITY_TOOLS

# 主动维度（memory_write 可写；系统维度 recent/uncertain 仅编码器落）
# ——与 nori 侧 tools.py 同值域（模块级：类体 genexpr 查找跳过类作用域）
_ACTIVE_CATEGORIES = (
    "identity", "stable", "preference", "commitment", "interaction", "naming",
)
_SEARCH_CATEGORIES = _ACTIVE_CATEGORIES
_CATEGORY_HINTS = {
    "identity": "他是谁（职业/身份/自我定位）",
    "stable": "既成事实（住址、宠物、经历、习惯）",
    "preference": "喜好厌恶（饮食禁忌、口味、喜欢或反感的事物）",
    "commitment": "带日期的约定（答应的事、约好的活动）",
    "interaction": "怎么和他相处（节奏、边界、忌讳）",
    "naming": "称呼偏好（想被怎么叫、反感什么称呼）",
}
_MAX_TOP_K = 10
_MAX_STATEMENT_CHARS = 200

_MEMORY_TOOLS_GUIDANCE = (
    "## 记忆工具使用准则\n"
    "上下文已注入的记忆简报与画像块中的内容不要重复检索。缺上下文"
    "先检索再答，不得假装记得，没查到就坦诚说记不清。本次真正检"
    "索到的内容当作自己亲身记得的经历自然讲出来，不要说「翻了翻"
    "记忆」「查了下记录」这类话，也不要提及工具名或检索过程。"
    "跨会话记忆注意时间与当事人：别人的经历不等于当前用户的，同"
    "名不等于同一人。被问到某个具体人物（含群友代号/昵称）时，用 "
    "memory_search(target=\"fact\", name=称呼) 查该成员的事实结论，"
    "结果中的【关系】小节就是该成员的确切关系边，直接据此回答谁和谁是什么"
    "关系，不要根据事实簇臆测。"
    "用 memory_profile 查完整画像。memory_search 查对话与事实结论，"
    "memory_lookup 查归档/失效的记忆，memory_profile 按名字查"
    "成员画像，memory_write 仅在用户明确要求记住时调用，答应记"
    "住就自然应下、顺接当前话题，不要说「写进记忆里啦」「已经记"
    "录」这类话，也不要提及写入操作或工具名，"
    "memory_correct 调整记忆状态（必须给 reason）。"
)

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
    except (TypeError, ValueError, OverflowError):
        # OverflowError：int(float('inf')) 一类脏值——不接住会冒泡到
        # _build_config 让整个插件装配失败（与「越界值钳到合法区间」相悖）
        logger.warning("配置项 %s 非法（期望整数），使用默认 %s", key, default)
        return default


def _cfg_float(raw: dict, key: str, default: float) -> float:
    try:
        value = raw.get(key)
        if value in (None, ""):
            return default
        value = float(value)
    except (TypeError, ValueError, OverflowError):
        # OverflowError：float(10**400) 一类脏值——不接住同样会让装配失败
        logger.warning("配置项 %s 非法（期望数值），使用默认 %s", key, default)
        return default
    if not math.isfinite(value):
        # NaN/±inf 必须在此拦下：LocalMemoryConfig 的 gt/ge 约束对 NaN 判
        # 失败 → ValidationError → 装配失败 → 整个记忆功能下线（一次配置
        # 页手滑就是一次静默下线）。统一回退默认值。
        logger.warning("配置项 %s 非有限值（%r），使用默认 %s", key, value, default)
        return default
    return value


def _cfg_bool(raw: dict, key: str, default: bool) -> bool:
    value = raw.get(key)
    if value in (None, ""):
        return default
    if isinstance(value, bool):
        return value
    return str(value).strip().lower() in ("1", "true", "yes", "on")


_TIME_LABEL_MODES = ("relative", "absolute", "both")


def _cfg_time_label_mode(raw: dict) -> str:
    """recall_time_label_mode wiring: host config values outside the Literal
    whitelist fall back to the default both and warn - a mistyped form in a
    hand-edited host json should not blow up the whole plugin assembly
    (consistent with the tolerant handling of _cfg_int/_cfg_float)."""
    value = _cfg_str(raw, "recall_time_label_mode", "both")
    if value not in _TIME_LABEL_MODES:
        logger.warning("配置项 recall_time_label_mode=%r 非法，使用默认 both", value)
        return "both"
    return value


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
    """Plugin config dict -> LocalMemoryConfig (per-field tolerant, invalid values fall back to defaults).

    越界值（如 interval=0 / flush_idle_s=0）在此**钳到合法区间**：LocalMemoryConfig
    的 ge/gt 约束会让 0 触发 ValidationError → 构造失败 → 插件整体不可用
    （一次配置页手滑就是一次静默的「记忆下线」）。
    """
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
        retain_interval_group=max(_cfg_int(raw, "retain_interval_group", 1), 1),
        retain_interval_private=max(_cfg_int(raw, "retain_interval_private", 5), 1),
        flush_idle_s=max(_cfg_float(raw, "flush_idle_s", 1800.0), 1.0),
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
        commitment_expire_days=_cfg_int(raw, "commitment_expire_days", 60),
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
        recall_time_label_mode=_cfg_time_label_mode(raw),
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
        summary_lifecycle_enabled=_cfg_bool(
            raw, "summary_lifecycle_enabled", False
        ),
        summary_lifecycle_grace_days=_cfg_int(
            raw, "summary_lifecycle_grace_days", 30
        ),
        summary_lifecycle_half_life_days=_cfg_float(
            raw, "summary_lifecycle_half_life_days", 90.0
        ),
        summary_lifecycle_reinforce_window_days=_cfg_int(
            raw, "summary_lifecycle_reinforce_window_days", 90
        ),
        summary_lifecycle_interval_days=_cfg_int(
            raw, "summary_lifecycle_interval_days", 7
        ),
        summary_lifecycle_reinforce_on_recall=_cfg_bool(
            raw, "summary_lifecycle_reinforce_on_recall", True
        ),
        memory_tools_enabled=_cfg_bool(raw, "memory_tools_enabled", True),
    )


def _try_build_config(raw: dict) -> LocalMemoryConfig:
    """Fallback layer for _build_config: when out-of-range values trigger a
    pydantic validation exception, fall back to default parameters (keeping
    only dsn), consistent with the module's "per-field tolerant" promise, so a
    single config item (e.g. pool_min=0) cannot fail the whole plugin
    initialization."""
    try:
        return _build_config(raw)
    except Exception:
        logger.warning("配置项越界（pydantic 校验失败），越界项回退默认值", exc_info=True)
        return LocalMemoryConfig(dsn=_cfg_str(raw, "dsn"))


@dataclass
class _CachedLine:
    """One line of the rolling line cache: envelope line + attribution metadata (used by the turn signal to rebuild incremental batches)."""

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
    """Retain snapshot of one conversation turn (frozen at the watermark when the turn completion signal arrives)."""

    session_id: str
    platform: str
    group_id: str
    user_id: str
    timestamp: datetime
    participants: list[tuple[str, str]]
    history_lines: list[str]
    batch_lines: list[str]
    bot_lines: list[str] = field(default_factory=list)
    # 内容锚点：该批次链最早一轮的时间戳（合并时取 min）。与 timestamp
    # （最新一轮）区分——失败回座会把较旧批次放进 pending 槽，批次之间的
    # 真实先后必须按内容锚点判定，否则较新批次会被当作 old 合并：其历史
    # 窗口（含旧批次行）与批次内容重复，且批次行时间序倒置。
    _retain_first_ts: Optional[datetime] = None


class NoriflowMemoryPlugin(BasePlugin):
    """NoriFlow long-term memory plugin: automatic retain/recall + active tools + maintenance page."""

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
        # bot 身份（persona 名 + 首条消息的 self_id 及其适配器平台）
        self._bot_nickname = "助手"
        self._bot_user_id = ""
        # bot 所属适配器平台（与 _bot_user_id 同点一次性学习，传播给
        # kernel.bot_platform 供 Kira 记忆迁入的全局事实归属）
        self._bot_platform = ""
        # persona 昵称上次活读时间（monotonic；TTL 限频见 _refresh_bot_nickname）
        self._persona_checked_at = 0.0
        # 每会话滚动行缓存：sid -> OrderedDict[msg_key -> _CachedLine]（尾部最新）
        self._history: OrderedDict[str, OrderedDict[str, _CachedLine]] = OrderedDict()
        # retain 水位线：sid -> 已编码批次的消息键集合（防同批次重复编码）
        self._consumed: dict[str, set[str]] = {}
        # bot 行序号（缓存键唯一性用，不参与查找）
        self._bot_seq = 0
        # 无平台 message_id 的入站消息键序号（id(msg) 在 GC 后可被复用，
        # 会导致新消息撞上已消费水位键而静默丢失——单调计数器杜绝复用）
        self._anon_msg_seq = itertools.count()
        # bot 出站消息的平台 message_id 追踪（P3 场景 C 的引用 bot 判定：
        # Reply 元素只带平台消息 ID，而 bot 行缓存键是 bot#N 无法反查；
        # sid -> deque 有界，重启从零积累同滚动缓存语义）
        self._bot_message_ids: OrderedDict[str, deque[str]] = OrderedDict()
        # session_memory_updated 订阅句柄（terminate 退订用）
        self._mem_handler = None
        # 已启动写入任务
        self._retain_tasks: set[asyncio.Task] = set()
        self._retain_semaphore = asyncio.Semaphore(2)
        # retain 攒批（私聊 N 轮合并编码）状态：
        # _retain_pending = 内存主真源（未编码的当前批次，sid -> 状态）
        # _retain_pending_at = 每批最后追加时间（巡检超龄判据，wall clock）
        # _retain_inflight = 已弹出、编码在途的批次（sid -> [状态]）：表行快照
        #   需把它一起写入，否则崩溃瞬间在途内容永久丢失
        self._retain_pending: dict[str, _RetainState] = {}
        self._retain_pending_at: dict[str, float] = {}
        self._retain_inflight: dict[str, list[_RetainState]] = {}
        # 表行写入时间戳（sid -> datetime）：条件删除的 cutoff 依据
        self._retain_row_ts: dict[str, datetime] = {}
        self._retain_stopping = False
        # 快照恢复是否失败过（初始化时 DB 抖动 → 表里的崩溃备份行没回灌）：
        # 后续保存前必须补恢复，否则新快照 UPSERT 会覆盖旧行（永久丢失）
        self._retain_restore_failed = False
        # 补恢复串行锁：_restore_retain_pending 内有 conn.fetch 的 await 点，
        # 并发进入会让两个协程把同一批表行各合并一次（内容翻倍 + 轮数虚高）
        self._retain_restore_lock = asyncio.Lock()
        # 已回灌表行的时间戳（仅由恢复写入）：补恢复的幂等判据——半途失败
        # 后重试不得把同一行再合并一遍
        self._retain_restored_ts: dict[str, datetime] = {}
        # 在途回合处理器计数：terminate 需等它们收尾（在途 handler 的停机
        # 快照写必须完成后才能关连接池，否则该轮既没编码也没表行备份）
        self._retain_handler_active = 0
        # 攒批空闲巡检协程句柄（initialize 创建 / terminate 取消）
        self._retain_sweep_task: Optional[asyncio.Task] = None

    # ------------------------------------------------------------------
    #  生命周期
    # ------------------------------------------------------------------

    async def initialize(self) -> None:
        """Assembly: config -> DB connect/migrate -> client adapters -> kernel/services -> background tasks.

        Any step failing degrades and skips (logged), without blocking host
        startup; only when fully ready is _ready set and the built-in
        file-based memory plugin disabled.
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
        """Assemble services and background tasks (second half of initialize; exceptions are cleaned up uniformly by the caller)."""
        # 停机标志在装配期无条件复位：terminate 只在末尾复位，中途抛异常
        # （create_task 失败 / 未预期的 bus 错误）时标志会永久粘住——之后
        # 所有回合只写 pending 不编码，巡检也被 _flush_pending_batch 的
        # 门控挡死，等于静默「记忆下线」到下次停机。
        self._retain_stopping = False
        # 在途 handler 计数一并复位：terminate 等待超时后它可能残留非零
        # （handler 真卡住时的极端情形），下次停机会白等满 5s
        self._retain_handler_active = 0
        # 攒批间隔/空闲 flush 阈值不另存实例属性：宿主配置页保存时是就地改
        # 同一 LocalMemoryConfig 实例（setattr(self._config, ...)），攒批门控
        # 与巡检阈值一律活读 self._config —— 存一份启动期快照既是死代码，
        # 又会让人误以为改它能热生效（实则读不到）。
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
                # alias_store 不在此传值：kernel 尚未构造，此刻取值恒为
                # None（事实代号别名回填遍将永久空转）——kernel 就绪后
                # 在下方统一补设
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
            # 与 kernel 同源宿主时区链：画像尾注与 recall 标注同一时区口径
            host_tz_provider=getattr(self.ctx, "get_timezone", None),
        )
        # P3 边注入的邻居画像句柄（kernel 内消费，独立预算；未回填时只出陈述行）
        self._memory_kernel.persona_service = self._persona_service
        # 事实代号别名回填遍的别名句柄：FactMergeAgent 构造先于 kernel，
        # 只能在 kernel 就绪后补设（None 当别名层被禁用时保持跳过语义）
        if merge_agent is not None:
            merge_agent._alias_store = self._memory_kernel.alias_store  # noqa: SL001
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
        # 先恢复在途攒批快照（读表回灌内存 pending），再订阅回合完成信号：
        # 恢复过程有 await 点（conn.fetch）。若此时信号处理器已生效，恢复
        # 期间到达的回合会开新批并 UPSERT 新表行，随后恢复循环用读到的
        # 旧表行无条件覆盖内存 pending——该回合批次从主真源消失；随后旧
        # 批次编码成功时的条件删除还会把新表行一并删掉，回合消息既不在
        # pending 也没被编码落库。
        # 订阅前到达的回合不丢：其增量仍在滚动缓存里未消费，下个回合的
        # 增量会带上它（增量按水位线取，不按回合切分）。
        await self._restore_retain_pending()
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
        self._retain_sweep_task = asyncio.create_task(self._retain_sweep_loop())
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
        """Cleanup: unsubscribe turn signal -> flush in-flight retain -> stop background tasks -> close pools."""
        # 停机标志先置、_ready 后置：顺序反了的话（_ready 先 False）此后到达的
        # 回合会被门控直接挡掉，停机分支（保留本轮内容）根本进不去——那轮
        # 消息既不编码也不进表行（水位线未推进，但滚动缓存随进程退出丢失）
        self._retain_stopping = True
        self._ready = False
        if self._mem_handler is not None:
            bus = getattr(self.ctx, "event_bus", None)
            if bus is not None:
                try:
                    bus.unsubscribe("session_memory_updated", self._mem_handler)
                except (ValueError, KeyError):
                    pass
            self._mem_handler = None
        # 攒批巡检先停：防 flush 期间巡检再投新任务
        if self._retain_sweep_task is not None:
            sweep = self._retain_sweep_task
            self._retain_sweep_task = None
            try:
                sweep.cancel()
                await asyncio.gather(sweep, return_exceptions=True)
            except RuntimeError as exc:
                # 任务归属的事件循环已关闭（跨 loop 收口场景）：无法取消，
                # 直接丢弃句柄——协程随旧 loop 一同销毁，不影响收口
                logger.warning("retain 攒批巡检任务取消失败（事件循环已关闭）: %s", exc)
        # 等在途回合处理器收尾（最多 5s）：它们可能正在写停机快照或已被
        # 停机标志挡下——不等就关连接池，那次写入会跑在已关闭的池上（数据
        # 不落库），该轮消息既没编码也没表行备份，重启无法恢复
        # 预算取 5s 而非 db_command_timeout+余量：宿主 kira 用户单元
        # TimeoutStopSec=10s——等 60s+ 只会被 SIGKILL 抢先（收口更糟）。
        # 根治方式是让 handler 自身不被慢 DB 拖住（别名 upsert 2s + 停机
        # 快照写 3s ⇒ handler ≤5s），所以 5s 的等待足以覆盖它。
        # 单调时钟：墙钟回拨（NTP 校正/手动改时间）会让等待超时失控，
        # 挤占后续 db.close() 的预算
        deadline = time.monotonic() + 5.0
        while self._retain_handler_active > 0 and time.monotonic() < deadline:
            await asyncio.sleep(0.05)
        if self._retain_handler_active > 0:
            logger.warning(
                "terminate: %d 个在途回合处理器未收尾（继续停机）",
                self._retain_handler_active,
            )
        # 未满批的攒批：停机前强制编码（表行快照已落库，重启也能恢复；
        # 此处编码让数据在停机前落库，避免重启前存在未编码窗口）
        for sid in list(self._retain_pending):
            self._flush_pending_batch(sid, force=True)
        if self._retain_tasks:
            # 4s：与上面的 handler 等待（5s）合计 ≤ 9s，留出余量给宿主
            # kira 用户单元的 TimeoutStopSec=10s——超时会 SIGKILL，收口反而
            # 更糟（未完成的编码任务被砍，其内容仍在表行快照里可恢复）
            done, pending = await asyncio.wait(self._retain_tasks, timeout=4.0)
            if pending:
                logger.warning("terminate: %d 个 retain 任务超时未完成，取消", len(pending))
                for task in pending:
                    task.cancel()
            await asyncio.gather(*pending, return_exceptions=True)
        self._retain_tasks.clear()
        self._consumed.clear()

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
        self._retain_pending.clear()
        self._retain_pending_at.clear()
        # 在途登记必须一起清：超时被 cancel 的任务抛 CancelledError（不经过
        # _do_retain 的 except 分支），_drop_inflight 不会执行——残留状态会
        # 在下次 initialize 后被并进快照行（行删不掉 + 重启恢复重复编码）
        self._retain_inflight.clear()
        self._retain_row_ts.clear()
        self._retain_restored_ts.clear()
        self._retain_stopping = False

    # ------------------------------------------------------------------
    #  装配辅助
    # ------------------------------------------------------------------

    def _build_embedding_client(self) -> Optional[KiraEmbeddingClient]:
        """embedding_model override -> default_embedding -> in-plugin adapter.

        embedding_model format is provider_id:model_id (consistent with the
        host WebUI model identifier, resolved via get_embedding_client); on
        resolve failure, fall back to default_embedding and warn (fail-open,
        does not block assembly). Returns None if neither is configured.
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
        """default_rerank -> in-plugin adapter (returns None when not configured/disabled; pure vector order)."""
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
        """Number of history window batches visible to the host LLM (live-read bot.max_memory_length).

        Same key and source as the session_manager window truncation (hot
        changes take effect immediately); on read failure, fall back to the
        core default of 10. retain produces exactly one summary batch per
        turn, so "the most recent K batches" is the equivalent of the content
        already visible in the window (see
        memory_kernel._derive_window_batches).
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
        """Plugin data directory (host plugin_data/<plugin_id>; falls back to ./data when unavailable)."""
        try:
            d = self.ctx.get_plugin_data_dir()
            if d is not None:
                return Path(d)
        except Exception:
            logger.warning("插件数据目录获取失败（回退 ./data）", exc_info=True)
        return Path("data")

    async def _refresh_bot_nickname(self) -> None:
        """Live-read the host persona nickname and propagate it (TTL rate-limited).

        The host persona is live-read from the DB on every request (supports
        hot switching at runtime), and there is no persona change event to
        subscribe to - the one-shot cache in initialize goes stale after a
        switch: the "bot exclusion rule" in the encoding prompt still points
        to the old name, and under the new persona name, information about the
        bot itself may be extracted into the user persona (the envelope line
        is still safe via the is_self marker, a deterministic defense in the
        same family as bot_id propagation). Consumers: the encoding /
        re-encoding prompt exclusion rule and bot reply envelope lines. On
        read failure, keep the current value.
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
        """Propagate the real bot platform ID to the kernel / merge agent (the placeholder is used at construction time).

        Consumers: the bot fact hard-filter in retain_encoded/_reingest_facts,
        the "platform ID" rendering in the encoding prompt, and bot exclusion
        in recall expansion - the "kira" placeholder never equals a real uid,
        so this deterministic defense is dead code before propagation. Fixed
        on first sight (KiraAI single-bot deployment; multi-account across
        adapters is out of current scope). Also propagates the learned
        adapter platform (kernel.bot_platform) so the Kira-memory import can
        attribute global facts to the right platform.
        """
        if self._memory_kernel is not None:
            self._memory_kernel.bot_id = bot_user_id
            self._memory_kernel.bot_platform = self._bot_platform
        if self._merge_agent is not None:
            self._merge_agent.bot_id = bot_user_id
        logger.info("bot 平台 ID 已学习并传播: %s", bot_user_id)

    def _probe_fast_llm(self) -> Optional[FastLlmExit]:
        """Probe fast LLM availability; return the exit instance if available, None otherwise."""
        try:
            client = self.ctx.get_default_fast_llm_client()
        except Exception as exc:
            logger.warning("fast LLM 探测失败：%s", exc)
            return None
        if client is None:
            return None
        return FastLlmExit(self.ctx)

    def _make_relation_backfill(self) -> RelationBackfill:
        """Construct the legacy relation backfiller (the llm_call closure aligns the isomorphic convention on both sides of the shared module)."""
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
        """Automatically disable the built-in file-based memory plugin (called only after this plugin is fully ready)."""
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
        """Recall injection + memory tool gating.

        - Tool gating: enabled_tools multi-select config; when the kernel is
          not ready, all memory tools are removed (to prevent call errors);
          under the allowed_users user whitelist, non-whitelisted trigger
          users are also removed (hard interception still lives at each tool
          entry point).
        - Recall: vector retrieval with the batch text as the query (the
          session isolation switch controls cross-session behavior).
        - Rolling rollout: the recent batch summaries outside the host-visible
          window are injected as an independent Prompt (not query-driven -
          notice-triggered turns also need recent context; built before
          recall so its memo is in place, and recall excludes these lines in
          the same turn to avoid duplicate injection).
        - Persona: batch speaker candidates -> deterministically assembled
          from the cluster table (multi-user in group chat / trigger user +
          entity hits in private chat; since v1.7.1, naming others in private
          chat also injects their persona).
        - Injection position: dynamic blocks (rolling rollout / recall /
          persona) are written to the head of the user prompt, and the host's
          assemble_prompt places them inside the latest user message, right
          before the user input - so the system prompt and history messages
          form a stable prefix and can hit the prompt cache; persist=False,
          effective for this turn only, not written back to history.
        """
        # enabled_tools 显式空列表 = 全部禁用（与 schema "取消选择即禁用"
        # 语义一致）；键缺失/None 才回退全启用（plugin_cfg 可能为 None，
        # 与 451/1872/1876 处的 or {} 同口径）
        configured = (self.plugin_cfg or {}).get("enabled_tools")
        if configured is None:
            configured = ALL_TOOLS
        disabled = set(ALL_TOOLS) - set(configured)
        if (
            self._config is None
            or not self._config.memory_tools_enabled
        ):
            # 新三件（profile/lookup/correct）由 memory_tools_enabled 总
            # 开关管理；旧三件不受其管辖
            disabled |= set(_PARITY_TOOLS)
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
        # 动态块注入 user prompt 头部（宿主 assemble_prompt 将其排进最新一条
        # user 消息、紧邻用户输入）：system prompt 与历史消息由此构成稳定
        # 前缀可命中提示词缓存，随轮次变化的召回/画像块落在尾部不再打断
        # 前缀缓存。persist=False：仅本轮生效，不写回会话历史。
        inject_prompts: list[Prompt] = []
        if self._config.recent_rollout_enabled:
            try:
                rollout_text = await kernel.build_recent_rollout_text(
                    session.session_id, platform=platform
                )
            except Exception:
                logger.warning("滚动补回摘要获取失败（不阻断本轮）", exc_info=True)
            else:
                # 对齐 kernel 两阶段契约：先把补回块真正 append 进本轮
                # prompt pieces，再 commit 排除 memo，最后跑 recall（recall
                # 依据 memo 同轮排除已注入行）。append 与 commit 之间仅隔
                # 纯内存 list.append——本轮装配若被丢弃，pending 不入 memo，
                # 行保持 recall 可见，而非补回/召回两通道同时不可见一个
                # TTL。hasattr 守卫兼容未实现两阶段的测试桩 kernel。
                if rollout_text:
                    inject_prompts.append(
                        Prompt(
                            content=rollout_text,
                            name=f"{PLUGIN_ID}:rollout",
                            source=PLUGIN_ID,
                            persist=False,
                        )
                    )
                if kernel is not None and hasattr(kernel, "commit_rollout_memo"):
                    kernel.commit_rollout_memo(session.session_id)

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

        # （rollout 块已在 recall 之前 append 进 inject_prompts——两阶段
        # commit 契约要求，见上方；注入顺序 rollout → recall → profile 不变）
        if memory_text:
            inject_prompts.append(
                Prompt(
                    content=memory_text,
                    name=f"{PLUGIN_ID}:recall",
                    source=PLUGIN_ID,
                    persist=False,
                )
            )
        if profile_text:
            inject_prompts.append(
                Prompt(
                    content=profile_text,
                    name=f"{PLUGIN_ID}:profile",
                    source=PLUGIN_ID,
                    persist=False,
                )
            )
        if inject_prompts:
            user_prompt = getattr(req, "user_prompt", None)
            if isinstance(user_prompt, list):
                user_prompt[:0] = inject_prompts
            else:
                # 旧宿主无 user_prompt（或类型不可变）时回退 system prompt，
                # 注入不丢
                req.system_prompt.extend(inject_prompts)
        # 记忆工具准则：本轮至少一件记忆工具可用时注入（工具全被门控摘除
        # 时不占 prompt 预算）。静态内容留在 system prompt 尾部——不随轮次
        # 变化，仍是可缓存前缀的一部分，故不随动态块下移。
        if set(ALL_TOOLS) - disabled:
            req.system_prompt.append(
                Prompt(
                    content=_MEMORY_TOOLS_GUIDANCE,
                    name=f"{PLUGIN_ID}:tools",
                    source=PLUGIN_ID,
                )
            )

    async def _build_profile_text(
        self, event, session, messages, entity_entries: list | None = None
    ) -> str:
        """Build the persona injection text (bot itself excluded; candidates = sender + entity hits).

        v1.7.1 design revision: private chat (any non-group session) also
        consumes entity hits - when the batch text mentions others (alias /
        dictionary hit), their persona joins the candidates (candidate
        collection same as group chat); with no expansion, keep the
        single-user path (output format consistent with existing private
        chat).
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
        """Collect persona candidates from batch messages (dedup preserving order, bot itself excluded, capped by config).

        Candidate priority: batch sender > entity dictionary hits (members
        mentioned in the batch text; shares the same hit results as the recall
        hint bypass, and the hit name is used directly as display_name -
        ensuring the persona title matches the chat appellation. False hits
        are guarded by cap truncation and empty persona slots; users already
        collected at the sender level are skipped via the seen dedup).
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
        """Concatenate the batch text into the recall query (truncated to prevent oversized vector requests)."""
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
        """Entity dictionary match text: Text elements + At element nicknames (built separately, does not touch the _chain_text used by retain).

        At nicknames being part of the match surface is a feature - AT is the
        strongest mention signal, and "@XiaoWang, what you said last time"
        should rightly match XiaoWang; Reply elements only carry the message
        ID without a name, so they are not part of the match surface.
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
        """Track bot outbound platform message IDs in the per-session set (bounded; sessions LRU)."""
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
        """P3 scenario C hard gate: this turn's input ATs the bot or quotes a bot message.

        AT determination = At element pid equals the bot uid ("all" does not
        count); quote determination = the Reply target hits this session's bot
        outbound message ID tracking set. A bare pronoun "you" never
        constitutes a bot hit - the bot name is not in the entity
        dictionary/alias table, the name matching layer structurally excludes
        the bot, so a random group chat sentence cannot falsely trigger bot
        edge injection.
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
        """Original text of the quoted message (quote text merged into the recall query).

        v1.7.1 design revision: the quote reverse-lookup bypass (anchor
        time-window summary pull) was removed; instead the quoted message
        original text joins the query and goes through the main semantic
        recall path - a quote can only reference messages in this session, and
        session isolation is uniformly governed by the main-path switch.
        Coverage = session rolling cache (_SESSION_CACHE_LIMIT lines); older
        quote targets are not merged (a cache miss is harmless, no quote
        text). The envelope line is used truncated in full (including the
        time/speaker prefix - embedding is insensitive to prefix noise, and
        keeping the speaker actually helps recall); each quote is truncated to
        200 characters, at most 2 per turn.
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
        """Entity dictionary name sources: sender nicknames / group cards observed in the session rolling cache.

        Bot lines are skipped (the bot name would hit all session summaries
        and explode candidates); after restart the cache accumulates from zero
        and the dictionary heals itself (TTL fallback).

        Bare session_id compatibility: the cache is keyed by composite sid
        (platform:type:id), while callers such as build_injection_text's
        fallback self-match pass a bare id - looked up by trailing segment.
        For same-number sessions across adapters, take the most recently
        active one: names are only a candidate source, and mismatches are
        guarded by reranking and empty persona slots.
        """
        bucket = self._history.get(sid)
        if bucket is None and sid and ":" not in sid:
            # LRU OrderedDict：最新活跃会话在尾部，倒序扫描让最近活跃者
            # 胜出（正序首个命中是最久未活跃会话，与 docstring 相悖）
            for key, candidate in reversed(list(self._history.items())):
                if key.rsplit(":", 1)[-1] == sid:
                    bucket = candidate
                    break
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
        """Upsert turn batch sender names into the persistent alias layer (the only name-stream source on the kira side).

        The kira plugin does not read the host user table (isomorphic
        convention on both sides: message stream -> plugin alias table);
        variant splitting and the cap truncation happen inside
        build_alias_rows. Failures are only logged.
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
        """Get the batch trigger user (user_id, "") - platform is filled in by the caller from the session.

        Returns:
            (uid of the first non-notice message sender, placeholder empty
            platform).
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
        """Add an inbound message to the rolling line cache (the history window source for encoding input)."""
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
        # 平台学习与 uid 学习解耦：首条 self_id 消息可能尚无
        # adapter_name，此后任何一条消息补学到平台即重传播——消除
        # 「平台永久空串」窗口，Kira 迁入的全局事实归属不再依赖推断回退
        if not self._bot_platform:
            adapter = str(getattr(session, "adapter_name", "") or "")
            if adapter:
                self._bot_platform = adapter
                if self._bot_user_id:
                    self._propagate_bot_identity(self._bot_user_id)
        line = format_history_message(
            speaker_name=nickname or uid,
            content=text,
            timestamp=self._to_datetime(getattr(msg, "timestamp", None)),
            user_id=uid,
            is_at_bot=bool(getattr(msg, "is_mentioned", False)),
        )
        self._append_history(
            session.sid,
            str(
                getattr(msg, "message_id", "")
                or f"_anon{next(self._anon_msg_seq)}"
            ),
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
        """Bot sent observation: line into the rolling cache (retain is triggered by the turn completion signal).

        Encoding is not triggered here - triggering per sent segment would
        re-encode the same batch when the segment gap exceeds the merge window
        (tool-turn splits / multi-segment replies hit this); the core
        session_memory_updated signal guarantees exactly one encoding per
        turn.
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
        """回合完成信号入口：登记在途计数（terminate 需等它收尾）。

        在途 handler 可能正卡在 await（别名 upsert / 快照写）上；terminate
        若不等它就关连接池，它后续的停机快照写会跑在已关闭的池上（数据不
        落库）——这一轮既没编码也没表行备份，重启无法恢复。
        """
        # 停机期间仍放行：让回合走到 _retain_stopping 分支保留本轮内容
        # （直接 return 会让这一轮既不编码也不进表行）
        if not self._ready and not self._retain_stopping:
            return
        self._retain_handler_active += 1
        try:
            await self._on_session_memory_updated_impl(event)
        finally:
            self._retain_handler_active -= 1

    async def _on_session_memory_updated_impl(self, event) -> None:
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
        if not self._ready and not self._retain_stopping:
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
        # Persistent alias layer batch upsert (fail-open, independent of retain).
        # 有界等待：这是**旁路**写入（别名层），不该把回合处理链拖住——
        # 无界等待时慢 DB 会让 handler 卡到 db_command_timeout（默认 60s），
        # 停机收口（宿主 kira 单元 TimeoutStopSec=10s）必然切在它前面：
        # 水位线已推进、批次还没进 pending，这一轮静默丢失。超时按 fail-open
        # 跳过（别名层可有缺口，回落原始文本即可）。
        try:
            await asyncio.wait_for(self._alias_upsert_batch(user_delta), timeout=2.0)
        except asyncio.TimeoutError:
            # 超时是预期路径：不带堆栈，避免真异常被噪音淹没
            logger.warning("别名层 upsert 超时（旁路写入，跳过）")
        except Exception:
            logger.warning("别名层 upsert 失败（旁路写入，跳过）", exc_info=True)

        state = _RetainState(
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
        # 内容锚点：新批次链的最早一轮 = 本轮
        state._retain_first_ts = state.timestamp
        # P2: coalesce private-chat turns (and persist the frozen snapshot).
        # 停机门控：terminate 期间不再派新任务——收口的 asyncio.wait 只
        # 覆盖调用瞬间的任务集合，之后新建的任务不会被等待，而随后
        # db.close() 会让它们跑在已关闭的连接池上（数据不落库 + 未捕获异常）。
        # 但本轮增量此刻已写入水位线（消费在首个 await 之前完成），直接
        # return 会静默吞掉这一轮：滚动缓存随进程退出丢失，又没进表行。
        # 因此停机分支仍保留批次（内存 pending，terminate 的 force 冲刷
        # 会编码它）并尽力落表行（重启可恢复）。
        if self._retain_stopping:
            existing = self._retain_pending.get(sid)
            if existing is None:
                state._retain_rounds = 1
                self._retain_pending[sid] = state
            elif existing is not state:
                state = self._merge_retain_states(existing, state)
                self._retain_pending[sid] = state
            self._retain_pending_at[sid] = time.time()
            try:
                # 有界等待：停机路径上 terminate 可能已推进到 db.close()，
                # 无界 await（内部还含 _ensure_retain_restored 的锁 + fetch）
                # 会把回合完成信号拖死在 DB I/O 上。超时只损失表行备份，
                # 内存 pending 仍会被 terminate 的 force 冲刷编码
                await asyncio.wait_for(
                    self._save_retain_pending(sid), timeout=3.0
                )
            except Exception:
                logger.warning(
                    "停机期间 retain pending 快照保存失败（本轮可能丢失）: session=%s",
                    sid,
                )
            return
        # 攒批门控（活读配置）：群聊 retain_interval_group、私聊
        # retain_interval_private；间隔 <=1 走原逐轮路径（群聊默认 1，
        # 行为与改动前零变化：不写 pending 表，编码失败回滚水位线）。
        interval = self._retain_interval_for(stype)
        if interval <= 1:
            # 攒批残留（热切换把 interval 调小、或重启恢复的批次）与本轮
            # **合并为一批**编码（不是分别编码）：残留内容不丢，也不会与
            # 本轮的 history 窗口重复——本轮 state 的 history 是用
            # _history_snapshot 生成的，其中已含残留批次的行；若残留单独
            # 异步编码，同一段对话会既进残留批次的 batch_lines、又进本轮的
            # history 窗口（被编码两次）。合并后历史窗口取残留批次的历史
            # （攒批前快照），内容按时间序拼接。
            residual = self._retain_pending.pop(sid, None)
            self._retain_pending_at.pop(sid, None)
            durable = False
            if residual is not None:
                state = self._merge_retain_states(residual, state)
                # 残留内容并入本轮后，失败时按攒批语义回座（内容不丢）；
                # 无残留时保持逐轮路径的既有契约（失败回滚水位线）
                durable = True
                # 合并后的批次已离开 pending 槽（表行仍只有残留批次），
                # 本轮内容只活在任务闭包里：编码是长耗时 LLM 调用，期间
                # 崩溃或 terminate 超时取消（CancelledError 不走失败回座）
                # → 本轮内容既不在内存也不在表行，重启后永久丢失。故先
                # 登记在途 + 落表行（与攒批主路径一致），再调度编码。
                self._retain_inflight.setdefault(sid, []).append(state)
                self._spawn_pending_save(sid)
            self._spawn_retain_task(sid, state, delta_keys=delta_keys, durable=durable)
            return

        # 攒批路径：内存 pending 为主真源，DB 表行为重启恢复备份。
        # 取出即转移真源——水位线不回滚（回滚会让下一轮信号重取已入批的
        # 行造成双份）；未编码数据的保管责任移交攒批缓存/表行。
        pending = self._retain_pending.get(sid)
        if pending is not None and getattr(pending, "_retain_failed", False):
            # 失败回座的批次不与新轮次混批：单独冲刷（重试），新轮次另开
            # 批次——混合链（失败批 + 新轮次）会与在途批次交错，表行快照
            # 的合并顺序与历史窗口都会失真
            self._flush_pending_batch(sid)
            pending = None
        if pending is not None:
            state = self._merge_retain_states(pending, state)
        else:
            state._retain_rounds = 1
        self._retain_pending[sid] = state
        self._retain_pending_at[sid] = time.time()
        # 快照写入放后台任务：快照是崩溃恢复备份，不是主链路——DB 变慢或
        # 不可达时不应把回合完成信号拖住（原逐轮路径也是 fire-and-forget；
        # 写失败只记 warning，内存 pending 是主真源，下轮追加自愈）
        self._spawn_pending_save(sid)
        if int(getattr(state, "_retain_rounds", 1)) < interval:
            return
        self._flush_pending_batch(sid)

    # ------------------------------------------------------------------
    #  retain 攒批（私聊 N 轮合并编码）
    # ------------------------------------------------------------------

    def _retain_interval_for(self, stype: str) -> int:
        """攒批间隔（活读配置）：群聊 retain_interval_group / 私聊
        retain_interval_private；<=1 表示逐轮编码（现状默认路径）。"""
        cfg = self._config
        raw = (
            getattr(cfg, "retain_interval_group", 1)
            if stype == "gm"
            else getattr(cfg, "retain_interval_private", 5)
        )
        try:
            return max(int(raw or 1), 1)
        except (TypeError, ValueError, OverflowError):
            # OverflowError：int(float('inf')) 一类脏值——不接住会在这行之后
            # 抛出（水位线已推进、批次还没进 pending），整轮对话静默丢失
            return 1

    def _retain_flush_idle_s(self) -> float:
        """攒批空闲 flush 阈值（活读配置，秒）：会话攒不满间隔时由巡检
        强制编码落库，防末批长期滞留不进记忆。"""
        try:
            # 与装配期钳制保持同一口径：0（或负数）→ 1.0，而不是 `or 1800.0`
            # 悄悄变成 1800（同一配置两条路径两种语义，末批滞留半小时）
            value = float(getattr(self._config, "flush_idle_s", 1800.0))
        except (TypeError, ValueError, OverflowError):
            # OverflowError：热切换塞进来 10**400 一类脏值时 float() 会抛，
            # 而本方法在巡检循环里于 try 之外调用——不接住会让巡检协程直接
            # 死掉（不再自愈）
            return 1800.0
        if math.isnan(value) or math.isinf(value):
            # NaN/±inf（宿主 setattr 热切换可绕过 Pydantic 校验）：NaN 会让
            # `now - updated_at <= idle_s` 恒为 False → 巡检把全部批次误判
            # 超龄提前冲刷；±inf 则永不超龄。统一回退默认值。
            return 1800.0
        return max(value, 1.0)

    def _spawn_retain_task(
        self,
        sid: str,
        state: _RetainState,
        *,
        delta_keys: set[str],
        durable: bool,
    ) -> asyncio.Task:
        """创建并追踪 retain 编码任务（逐轮与攒批共用同一收口）。"""
        task = asyncio.create_task(self._do_retain(state, sid, delta_keys, durable))
        task._retain_sid = sid  # type: ignore[attr-defined]
        self._retain_tasks.add(task)
        task.add_done_callback(self._retain_tasks.discard)
        return task

    def _spawn_pending_save(self, sid: str) -> None:
        """后台写攒批快照（不阻塞回合完成信号）。

        与编码任务同一收口集合（terminate 统一等待/取消）。
        """
        task = asyncio.create_task(self._save_retain_pending_guarded(sid))
        task._retain_sid = sid  # type: ignore[attr-defined]
        self._retain_tasks.add(task)
        task.add_done_callback(self._retain_tasks.discard)

    async def _save_retain_pending_guarded(self, sid: str) -> None:
        """快照写入的兜底包装：失败只记 warning（内存 pending 是主真源）。"""
        try:
            await self._save_retain_pending(sid)
        except Exception:
            logger.warning("retain pending 快照保存失败（保留内存快照）", exc_info=True)

    def _flush_pending_batch(self, sid: str, *, force: bool = False) -> None:
        """弹出攒批批次并调度合并编码（到点 / 巡检超龄 / 会话逐出共用入口）。

        弹出即从内存 pending 移除，后续同会话新轮次开新批次（与在途编码
        无共享可变状态）；在途批次登记进 _retain_inflight，表行快照会把
        在途批次一并写入（崩溃恢复不丢在途内容）。

        force=True 仅供 terminate 自身的收口冲刷使用（此时停机标志已置位）；
        停机期间巡检/回合侧的新冲刷一律跳过（收口只等已登记的任务）。
        """
        if self._retain_stopping and not force:
            return
        state = self._retain_pending.pop(sid, None)
        if state is None:
            return
        self._retain_pending_at.pop(sid, None)
        # 复用统一的任务创建入口（与逐轮路径同一收口集合，避免两处逻辑
        # 将来不同步）；在途登记紧随其后（其间无 await，顺序安全）
        self._spawn_retain_task(sid, state, delta_keys=set(), durable=True)
        self._retain_inflight.setdefault(sid, []).append(state)

    def _drop_inflight(self, sid: str, state: _RetainState) -> None:
        """摘除在途批次登记（成功/失败路径共用，幂等）。"""
        states = self._retain_inflight.get(sid)
        if not states:
            return
        try:
            states.remove(state)
        except ValueError:
            pass
        if not states:
            self._retain_inflight.pop(sid, None)

    def _pending_snapshot_for_row(self, sid: str) -> Optional[_RetainState]:
        """表行快照 = 在途批次 ∪ 当前 pending（合并为单批）。

        崩溃恢复要求：任意时刻崩溃，表行都要能恢复全部未编码数据——
        在途批次（已编码但可能未完成）也必须在内，否则崩溃瞬间在途
        内容永久丢失。
        """
        states = list(self._retain_inflight.get(sid, []))
        current = self._retain_pending.get(sid)
        if current is not None:
            states.append(current)
        if not states:
            return None
        # 按内容锚点（批次链最早一轮的时间）排序再折叠：列表顺序不等于
        # 时间序——失败回座会把较旧批次放进 pending 槽（pending 槽本是
        # 「最新批次」的位置），不排序就会把较新批次当作 old：其历史窗口
        # （已含旧批次的行）与批次内容重复，同一段对话既进历史又进内容，
        # 且批次行时间序倒置。
        states.sort(key=self._state_sort_key)
        merged = states[0]
        for nxt in states[1:]:
            merged = self._merge_retain_states(merged, nxt)
        return merged

    async def _sweep_retain_pending(self) -> None:
        """巡检单轮：超龄的攒批会话强制编码（攒不满间隔的末批不滞留）。

        判据 = 内存 pending 的最后追加时间距今超过 flush_idle_s（活读）；
        巡检周期取 min(300s, flush_idle_s)，故实际落库延迟上界约
        flush_idle_s + min(300s, flush_idle_s)。
        """
        idle_s = self._retain_flush_idle_s()
        # 补恢复：恢复曾失败时表里还有未回灌的崩溃备份行——先回灌，
        # 本轮巡检才能把它们按龄冲刷落库（否则一直躺在表里直到下次重启）
        await self._ensure_retain_restored()
        # 取时必须在恢复之后：补恢复含 DB fetch 的 await 点，提前取时会把
        # 恢复耗时从「龄期」里扣掉，让刚超龄的批次被误判为未超龄、推迟一轮
        now = time.time()
        for sid, state in list(self._retain_pending.items()):
            updated_at = self._retain_pending_at.get(sid, now)
            if now - updated_at <= idle_s:
                continue
            logger.info(
                "retain 攒批空闲超龄 flush: session=%s rounds=%d age=%.0fs",
                state.session_id,
                int(getattr(state, "_retain_rounds", 1)),
                now - updated_at,
            )
            self._flush_pending_batch(sid)

    async def _retain_sweep_loop(self) -> None:
        """攒批巡检协程主循环（initialize 创建 / terminate 取消）。

        单轮异常全捕获（含 DB 不可达）：巡检是自愈型后台任务，单轮失败
        不终止循环；CancelledError 原样上抛配合 terminate 的取消收口。
        """
        while True:
            # 下限 5s：flush_idle_s 误配成 0 会被钳到 1.0，无下限时巡检
            # 退化成 1Hz 空转；上限仍是 300s，落库延迟上界基本不变
            await asyncio.sleep(max(5.0, min(300.0, self._retain_flush_idle_s())))
            try:
                await self._sweep_retain_pending()
            except asyncio.CancelledError:
                raise
            except Exception:
                logger.warning("retain 攒批巡检失败（下轮重试）", exc_info=True)

    def _prune_consumed(self, sid: str) -> None:
        """The watermark keeps only keys still in the cache (message keys are never reused, evicted ones can be safely dropped)."""
        consumed = self._consumed.get(sid)
        bucket = self._history.get(sid)
        if consumed is None:
            return
        if bucket is None:
            self._consumed.pop(sid, None)
            return
        self._consumed[sid] = {k for k in bucket if k in consumed}

    def _rollback_consumed(self, sid: str, delta_keys: set[str]) -> None:
        """On encoding failure, roll back the watermark so the next turn's signal re-encodes this batch of messages."""
        consumed = self._consumed.get(sid)
        if consumed is not None:
            consumed.difference_update(delta_keys)

    async def _do_retain(
        self, state: _RetainState, sid: str = "", delta_keys: Optional[set[str]] = None,
        durable: bool = False,
    ) -> None:
        """Assemble the encoding input and write (Semaphore rate-limited).

        durable=False（逐轮路径，间隔<=1）：失败回滚水位线，下轮信号重取
        重编——与改动前逐字等价。
        durable=True（攒批路径）：失败不碰水位线（真源已移交攒批缓存/表
        行），改为失败回座——批次合并回内存 pending 并重写表行快照，待下
        一触发点（到点/巡检超龄/停机 flush）重试，消息不因失败丢失。
        """
        # persona 昵称活读（TTL 限频）：编码 prompt 的 bot 排除规则消费。
        # 该 await 在编码 try 之外：异常逃逸会让 durable 批次的在途登记永久
        # 残留（本轮不再被编码、之后每次快照都把它并进去、重启后重复入库）
        # ——就地兜住，昵称刷新失败不影响批次归属。
        try:
            await self._refresh_bot_nickname()
        except Exception:
            logger.warning("刷新 bot 昵称失败（不影响 retain 编码）", exc_info=True)
        kernel = self._memory_kernel
        if kernel is None:
            if not durable:
                if sid:
                    self._rollback_consumed(sid, delta_keys or set())
            else:
                # 攒批路径：批次已从 pending 弹出并登记在途，直接 return 会
                # 让这批数据运行期不再被编码（只能等重启从表行恢复）——回座
                # 交回 pending（表行备份仍在），等下一触发点重试
                await self._reseat_pending_batch(sid, state)
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
                    "异步 retain 失败（pending 快照保留，后续恢复/巡检重试）"
                    if durable
                    else "异步 retain 失败（水位线已回滚，本轮消息待下轮信号重编码）",
                    exc_info=True,
                )
                if durable:
                    await self._reseat_pending_batch(sid, state)
                elif sid:
                    self._rollback_consumed(sid, delta_keys or set())
                return
        if not durable:
            return
        # 成功路径：摘除在途登记；同会话仍有未编码数据（新轮次攒批）时
        # 改写表行为剩余数据的快照，全部编码完毕才删行。
        self._drop_inflight(sid, state)
        try:
            remaining = self._pending_snapshot_for_row(sid)
        except Exception:
            # 合并失败（脏行 / 时区混用等）时保守保留表行：删了会丢未编码
            # 内容，留行重启还能重放；也不让异常冒出任务（"Task exception
            # was never retrieved"）
            logger.warning(
                "retain pending 快照合并失败（保留表行待重放）: session=%s", sid,
                exc_info=True,
            )
            return
        if remaining is None:
            # 条件删除（cutoff = 本批快照行写入时间戳）：判定与删除之间的
            # await 窗口内并发新回合写入的更新行不受影响，不会被误删。
            # 删除走 DB I/O，异常不外抛：外抛会让编码任务以未捕获异常结束
            #（"Task exception was never retrieved"），且残留行会在重启后
            # 被重放编码。删不掉就留给下次刷新/重启覆盖。
            try:
                await self._delete_retain_pending(sid, self._retain_row_ts.get(sid))
            except Exception:
                logger.warning("retain pending 快照删除失败（重启恢复时重放）", exc_info=True)
        else:
            try:
                await self._save_retain_pending(sid)
            except Exception:
                logger.warning("retain pending 快照刷新失败（重启恢复时重放）", exc_info=True)

    async def _reseat_pending_batch(self, sid: str, state: _RetainState) -> None:
        """失败回座：批次合并回内存 pending（同步，无 await 点）+ 重写表行。

        缓存 miss（无新代）：原批次放回；已有新代：按内容锚点定先后合并
        （较旧在前），轮数相加。

        龄期语义：回座时把 _retain_pending_at 重置为 now——这是**重试
        退避**：编码端点持续失败时，巡检不会每 300s 就热重试一次；该会话
        若不再有回合，重试最迟发生在 flush_idle_s 之后（内容不会丢，表行
        备份仍在）。下一轮有新回合时，失败批次会被**单独冲刷重试**，
        不必等巡检。

        回座批次打 _retain_failed 标记：它不得与后续新轮次混批（混合链会
        与在途批次交错，表行快照的合并顺序与历史窗口都会失真）——下一轮
        触发时单独冲刷重试。
        """
        current = self._retain_pending.get(sid)
        if current is None or current is state:
            merged = state
        elif self._state_sort_key(state) <= self._state_sort_key(current):
            # 不同对象按内容锚点定先后（较旧在前，其历史窗口作为合并后的历史窗口）
            merged = self._merge_retain_states(state, current)
        else:
            merged = self._merge_retain_states(current, state)
        # 标记必须落在真正留在 pending 槽的对象上：合并会生成**新对象**，
        # 给被丢弃的 state 打标会让下一轮守卫读不到标记（失败批被混进新
        # 轮次），正是本方法要避免的交错/历史窗口失真
        merged._retain_failed = True
        self._retain_pending[sid] = merged
        self._retain_pending_at[sid] = time.time()
        self._drop_inflight(sid, state)
        try:
            await self._save_retain_pending(sid)
        except Exception:
            logger.warning("retain pending 失败回座快照保存失败（内存保留）", exc_info=True)

    @staticmethod
    def _merge_retain_states(old: _RetainState, new: _RetainState) -> _RetainState:
        """合并两个批次为单批（攒批主路径）。

        历史窗口取较旧批次（old）的 history_lines——它正是「攒批之前」的
        快照，等价于 nori 侧 exclude_ids 机制回退到攒批前的历史窗口；
        批次内容按时间序拼接（old 在前），轮数相加。

        不回退到 new.history_lines：old 的历史窗口合法为空（会话首轮就是
        此情形），而 new 的历史窗口含 old 批次的行——回退会让同一句既进
        历史窗口又进批次内容，编码输入里重复一遍。
        """
        participants = list(old.participants)
        seen = set(participants)
        for participant in new.participants:
            if participant not in seen:
                seen.add(participant)
                participants.append(participant)
        state = _RetainState(
            session_id=new.session_id, platform=new.platform,
            group_id=new.group_id, user_id=old.user_id or new.user_id,
            timestamp=new.timestamp, participants=participants,
            history_lines=old.history_lines,
            batch_lines=old.batch_lines + new.batch_lines,
            bot_lines=old.bot_lines + new.bot_lines,
        )
        state._retain_rounds = int(getattr(old, "_retain_rounds", 1)) + int(
            getattr(new, "_retain_rounds", 1)
        )
        # 失败标记必须随合并传播：合并返回新对象，不带过去的话标记就丢了
        # ——下一轮守卫读不到 _retain_failed，失败批会与新轮次混成一批
        # （正是该守卫要避免的混合链）
        state._retain_failed = bool(
            getattr(old, "_retain_failed", False)
            or getattr(new, "_retain_failed", False)
        )
        # 内容锚点取两批最早者（old 已是较早的一方）
        state._retain_first_ts = NoriflowMemoryPlugin._state_first_ts(old)
        return state

    @staticmethod
    def _state_first_ts(state: _RetainState) -> datetime:
        """批次的内容锚点（最早一轮的时间戳）；缺失时退回 timestamp。"""
        value = getattr(state, "_retain_first_ts", None)
        if isinstance(value, datetime):
            return value
        return state.timestamp

    @classmethod
    def _state_sort_key(cls, state: _RetainState) -> datetime:
        """批次锚点的比较键（统一为 aware UTC）。

        恢复行（ISO 带时区）与内存新批次（naive datetime.now()）混在一起
        时，直接比较/排序会抛 TypeError，毒死快照链路。这里只用于**比较**，
        不改写锚点本身（naive 一律按 UTC 解释：生产路径的锚点全是 naive，
        口径一致，先后关系不受影响）。
        """
        value = cls._state_first_ts(state)
        if value.tzinfo is None:
            return value.replace(tzinfo=timezone.utc)
        return value.astimezone(timezone.utc)

    def _retain_payload(self, state: _RetainState) -> dict[str, Any]:
        return {
            "session_id": state.session_id, "platform": state.platform,
            "group_id": state.group_id, "user_id": state.user_id,
            "timestamp": state.timestamp.isoformat(),
            "first_ts": self._state_first_ts(state).isoformat(),
            "participants": state.participants,
            "history_lines": state.history_lines,
            "batch_lines": state.batch_lines, "bot_lines": state.bot_lines,
            "rounds": int(getattr(state, "_retain_rounds", 1)),
            # 失败标记也落盘：重启恢复后仍能识别「这是回座待重试的批次」，
            # 不会当成普通 pending 与新轮次混批
            "failed": bool(getattr(state, "_retain_failed", False)),
        }

    @staticmethod
    def _retain_participants_from_payload(raw: Any) -> list[tuple[str, str]]:
        """参与者兜底解析：只接受可迭代二元组，其余降级为 (str(item), "")。

        两层都要兜：
        - **容器**：raw 本身若是标量字符串（"u1"），逐字符迭代会产出
          ("u","")、("1","") 这类静默损坏的参与者；若是 int 等不可迭代值，
          直接 TypeError 会让整行被当坏行跳过（该行未编码内容不恢复）。
          非序列一律按空处理——丢参与者信息好过丢整批内容。
        - **元素**：字符串也是 iterable，tuple("u1") 会得到 ('u','1')。
        """
        if not isinstance(raw, (list, tuple)):
            return []
        result: list[tuple[str, str]] = []
        for item in raw:
            if isinstance(item, (list, tuple)):
                if len(item) >= 2:
                    result.append((str(item[0]), str(item[1])))
                elif len(item) == 1:
                    result.append((str(item[0]), ""))
                else:
                    # 空项直接跳过：("", "") 会污染下游参与者列表（画像/prompt）
                    continue
            else:
                result.append((str(item), ""))
        return result

    @staticmethod
    def _retain_lines_from_payload(raw: Any) -> list[str]:
        """行内容兜底解析：非序列按空处理，元素统一转 str。

        标量字符串会被逐字符迭代（"abc" → ['a','b','c']），属静默损坏。
        """
        if not isinstance(raw, (list, tuple)):
            return []
        return [str(line) for line in raw]

    @staticmethod
    def _parse_retain_ts(raw: Any) -> datetime:
        """时间戳兜底解析：缺失/非法一律回退当前时间。

        直接 `payload["timestamp"]` 会在字段缺失时抛 KeyError，整行未编码
        内容被逐行 except 当坏行跳过（永不恢复）——回退当前时间只是让排序
        略有偏差，比丢整批内容好。
        """
        if raw:
            try:
                return datetime.fromisoformat(str(raw))
            except ValueError:
                pass
        return datetime.now()

    @staticmethod
    def _retain_state_from_payload(payload: dict[str, Any]) -> _RetainState:
        state = _RetainState(
            session_id=str(payload.get("session_id", "")),
            platform=str(payload.get("platform", "")),
            group_id=str(payload.get("group_id", "")),
            user_id=str(payload.get("user_id", "")),
            timestamp=NoriflowMemoryPlugin._parse_retain_ts(payload.get("timestamp")),
            # 参与者只接受可迭代二元组：脏行里若是字符串，tuple(item) 不抛
            # 异常而是拆成单字符元组（参与者信息静默损坏），长度不足 2 时
            # 下游解包还可能炸——统一走兜底解析
            participants=NoriflowMemoryPlugin._retain_participants_from_payload(
                payload.get("participants", [])
            ),
            # 行内容统一转 str：脏行含非字符串时，"\n".join 会在编码 try 之外
            # 抛 TypeError——durable 批次的在途登记永远摘不掉（表行删不掉、
            # 重启重复编码）。一行脏数据不该毒死整条链路；容器非序列时按空
            # 处理（标量字符串会被逐字符迭代，属静默损坏）。
            history_lines=NoriflowMemoryPlugin._retain_lines_from_payload(
                payload.get("history_lines", [])
            ),
            batch_lines=NoriflowMemoryPlugin._retain_lines_from_payload(
                payload.get("batch_lines", [])
            ),
            bot_lines=NoriflowMemoryPlugin._retain_lines_from_payload(
                payload.get("bot_lines", [])
            ),
        )
        # 脏行的 0/负数轮数会改变攒批节奏（门控 rounds < interval）——钳到 >=1；
        # 非数字脏值也不能抛（ValueError 会让整行被当坏行跳过 → 内容不恢复）
        try:
            state._retain_rounds = max(int(payload.get("rounds", 1) or 1), 1)
        except (TypeError, ValueError, OverflowError):
            # OverflowError：rounds=Infinity 一类脏值——不接住会让整行被
            # 当坏行跳过（该行未编码内容永不恢复）
            state._retain_rounds = 1
        # 失败标记（回座待重试）：恢复后仍按「不与新轮次混批」处理。
        # 只接受明确真值——bool("false") 是 True，脏行里的字符串 "false"
        # 会把正常批次误标为失败批（被单独冲刷，攒批节奏被打乱）。
        failed_raw = payload.get("failed", False)
        state._retain_failed = (
            failed_raw is True
            or failed_raw == 1
            or (
                isinstance(failed_raw, str)
                and failed_raw.strip().lower() in ("1", "true", "yes")
            )
        )
        # 内容锚点：恢复时保留批次链的最早时间（缺失则退回 timestamp）
        first_raw = payload.get("first_ts")
        if first_raw:
            try:
                state._retain_first_ts = datetime.fromisoformat(str(first_raw))
            except ValueError:
                state._retain_first_ts = state.timestamp
        else:
            state._retain_first_ts = state.timestamp
        return state

    async def _save_retain_pending(self, sid: str) -> None:
        """把该会话「全部未编码数据」的合并快照写入表（重启恢复备份）。

        快照 = 在途批次 ∪ 当前 pending（见 _pending_snapshot_for_row）——
        崩溃恢复时能恢复全部未编码数据，包括编码在途的批次。

        行时间戳由客户端统一生成（双后端同源）：删除走条件删除（cutoff），
        需要「本次写入的确切时间戳」才能判定新行，故在此登记到
        _retain_row_ts。
        """
        if self._db is None:
            raise RuntimeError("memory db unavailable")
        # 保存前补恢复：初始化时恢复失败过的话，表里还有未回灌的崩溃备份
        # 行——不补恢复，本行的 UPSERT 会把它们覆盖掉（永久丢失）
        await self._ensure_retain_restored()
        if self._retain_restore_failed:
            # 补恢复仍失败：本次**不写表行**——写了会用只含新批次的快照
            # 覆盖表里未回灌的崩溃备份行（旧内容立即永久丢失）。内存
            # pending 仍持有全部未编码数据，DB 恢复后的下一次保存会补齐。
            logger.warning(
                "retain pending 快照保存跳过（补恢复失败，避免覆盖未回灌行）: session=%s",
                sid,
            )
            return
        async with self._db.pool.acquire() as conn:
            # 取连接**之后**再取快照与时间戳：等待连接期间同会话的编码可能
            # 已完成（批次已摘除、表行已删），早先算好的 payload 会把已编码
            # 内容又写成一行——重启后被恢复并重放编码（且该行在会话不再活跃
            # 时不自愈）。此刻无未编码数据则直接不写。
            state = self._pending_snapshot_for_row(sid)
            if state is None:
                return
            payload = json.dumps(self._retain_payload(state), ensure_ascii=False)
            now = datetime.now(timezone.utc)
            # 先登记本次写入时间戳：编码成功路径的条件删除会读它——否则
            # 「SQL 已执行、尚未登记」的窗口里删除会拿到旧/None cutoff，
            # 刚写入的行删不掉（重启后被重放编码）
            registered = self._retain_row_ts.get(sid)
            if registered is None or now > registered:
                self._retain_row_ts[sid] = now
            if getattr(self, "_backend_name", "postgres") == "sqlite":
                await conn.execute(
                    "INSERT INTO memory_retain_pending(session_key,payload,updated_at) "
                    "VALUES ($1,$2,$3) ON CONFLICT(session_key) DO UPDATE SET "
                    "payload=excluded.payload, updated_at=excluded.updated_at "
                    # 时间戳守卫：同一 sid 的保存可能并发（回合保存 / 成功刷新 /
                    # 失败回座），后算出的快照可能先执行——无守卫时更旧的
                    # payload 会覆盖更新的行（重启备份丢最新内容 + cutoff 回退）
                    "WHERE excluded.updated_at > memory_retain_pending.updated_at",
                    sid, payload, now.isoformat(timespec="microseconds"),
                )
            else:
                await conn.execute(
                    "INSERT INTO memory_retain_pending(session_key,payload,updated_at) "
                    "VALUES ($1,$2::jsonb,$3) ON CONFLICT(session_key) DO UPDATE SET "
                    "payload=EXCLUDED.payload, updated_at=EXCLUDED.updated_at "
                    "WHERE EXCLUDED.updated_at > memory_retain_pending.updated_at",
                    sid, payload, now,
                )
        # 登记行时间戳只前进不后退（并发保存乱序时不得让 cutoff 回退；
        # 已在 SQL 前登记过，这里只做兜底）
        registered = self._retain_row_ts.get(sid)
        if registered is None or now > registered:
            self._retain_row_ts[sid] = now
        # 写后复查：写入窗口内同会话编码可能已完成（剩余快照变空），此时
        # 本行装的是「已编码内容」——用刚写入的 now 做条件删除，避免残留
        # 行在会话不再活跃时不自愈（重启后被重放编码）
        try:
            if self._pending_snapshot_for_row(sid) is None:
                await self._delete_retain_pending(sid, now)
        except Exception:
            logger.warning(
                "retain pending 快照写后复查失败（可能残留行）: session=%s", sid,
                exc_info=True,
            )

    async def _delete_retain_pending(self, sid: str, cutoff: Optional[datetime] = None) -> None:
        """删除攒批表行（条件删除：只删不晚于 cutoff 的行）。

        cutoff = 本批编码完成时快照行的写入时间戳（_retain_row_ts）。
        「判定无剩余数据」与「执行删除」之间有 await 窗口（pool.acquire /
        conn.execute），并发新回合可能已写入更新的快照行——条件删除只删
        updated_at <= cutoff 的行，新行（更晚时间戳）不受影响，避免把新
        批次的重启备份抹掉。
        """
        if self._db is None:
            return
        if cutoff is None:
            # 无 cutoff 不删：退回 now 会在「判定无剩余数据」与删除之间的
            # await 窗口内把并发写入的新快照行一并删掉（重启备份丢失）——
            # 留行比误删安全（残留行重启后重放编码，写入幂等键吸收）
            logger.warning(
                "retain pending 快照删除跳过（缺少行时间戳）: session=%s", sid
            )
            return
        limit = cutoff
        async with self._db.pool.acquire() as conn:
            if getattr(self, "_backend_name", "postgres") == "sqlite":
                await conn.execute(
                    "DELETE FROM memory_retain_pending "
                    "WHERE session_key=$1 AND updated_at <= $2",
                    sid, limit.isoformat(timespec="microseconds"),
                )
            else:
                await conn.execute(
                    "DELETE FROM memory_retain_pending "
                    "WHERE session_key=$1 AND updated_at <= $2",
                    sid, limit,
                )
        # 只在「本次删除确实覆盖到已登记的那一行」时清 cutoff：条件删除
        # 未命中（表里是更晚写入的并发行）时保留它——否则下次删除会退回
        # cutoff=now，把新批次的重启备份一并抹掉（条件删除本要防的竞态）
        registered = self._retain_row_ts.get(sid)
        if cutoff is not None and (registered is None or registered <= limit):
            self._retain_row_ts.pop(sid, None)

    async def _ensure_retain_restored(self) -> None:
        """补恢复：初始化时恢复失败（DB 抖动）后，表里的崩溃备份行还没回灌。

        不补恢复会造成**不可逆的数据丢失**：该会话再来新回合时，
        _save_retain_pending 用只含新批次的快照 UPSERT 覆盖旧行（新时间戳
        必过守卫）——旧批次内容既不在内存也不在表里，永远不会被编码。
        """
        if not self._retain_restore_failed:
            return
        # 补恢复必须串行：_restore_retain_pending 内有 conn.fetch 的 await 点，
        # 并发进入会各自把同一批表行合并进 pending（内容翻倍 + 轮数虚高，
        # 直接提前落库 + 多编码一遍）
        async with self._retain_restore_lock:
            # 双检：等锁期间可能已被别的协程补恢复成功
            if not self._retain_restore_failed:
                return
            logger.warning("retain pending 快照恢复曾失败，补恢复中")
            try:
                await self._restore_retain_pending()
            except Exception:
                self._retain_restore_failed = True
                logger.warning("retain pending 快照补恢复失败", exc_info=True)

    async def _restore_retain_pending(self) -> None:
        """读表回灌内存 pending（重启恢复，轮数延续）。

        回灌只恢复状态，不立即编码：未满批的批次按原轮数续攒（到点或
        巡检超龄才编码），避免重启把半批内容提前落库。坏行（非法 JSON
        / 缺字段）跳过不阻断其余行恢复。
        """
        if self._db is None:
            return
        try:
            async with self._db.pool.acquire() as conn:
                rows = await conn.fetch(
                    "SELECT session_key,payload,updated_at FROM memory_retain_pending "
                    "ORDER BY updated_at ASC"
                )
            now = time.time()
            for row in rows:
                # 整段行处理都在 try 内：合并 / 龄期计算 / 键读取任一抛异常，
                # 只跳过**这一行**——放到 try 外会中断整个恢复循环，排在它
                # 后面的表行永远回灌不了（且每次重试都会在同一行再炸）
                try:
                    raw = row["payload"]
                    if isinstance(raw, str):
                        raw = json.loads(raw)
                    if not isinstance(raw, dict):
                        continue
                    state = self._retain_state_from_payload(raw)
                    sid = str(row["session_key"])
                    row_ts = self._row_timestamp(row)
                    if row_ts is not None and row_ts == self._retain_restored_ts.get(sid):
                        # 幂等：该行已由恢复回灌过——半途失败的补恢复不得重复
                        # 合并，否则同批内容翻倍、轮数虚高导致提前落库 + 多编码
                        # 一遍。（判据用**仅由恢复写入**的登记，不能用
                        # _retain_row_ts：保存路径也会写它，会误跳过正常恢复）
                        continue
                    existing = self._retain_pending.get(sid)
                    if existing is None:
                        self._retain_pending[sid] = state
                    else:
                        # 恢复不得覆盖内存里已有的批次（恢复期间到达的回合
                        # 可能已开新批）：按时间序合并——旧=恢复行（攒批前快照），
                        # 新=内存批次，轮数相加，两边内容都不丢
                        self._retain_pending[sid] = self._merge_retain_states(
                            state, existing
                        )
                    # 保留表行的真实龄期（不重置超时钟）：重启后仍按原时间戳
                    # 参与巡检超龄判定；但内存已有更晚批次时取较晚者——否则
                    # 旧行的龄期会让合并后的新批次被巡检提前冲刷（违背
                    # 「续攒」语义）
                    restored_at = now - self._row_age_seconds(row)
                    existing_at = self._retain_pending_at.get(sid)
                    self._retain_pending_at[sid] = (
                        restored_at
                        if existing_at is None
                        else max(existing_at, restored_at)
                    )
                    # 登记行时间戳（条件删除的 cutoff 依据）：只前进不后退——
                    # 补恢复期间并发保存写入的更晚行不能被旧行拉回（cutoff
                    # 回退会让后续条件删除删不掉更新行，只能留行等重启重放）
                    if row_ts is not None:
                        registered = self._retain_row_ts.get(sid)
                        if registered is None or row_ts > registered:
                            self._retain_row_ts[sid] = row_ts
                        # 恢复登记（幂等判据）：该行已回灌过
                        self._retain_restored_ts[sid] = row_ts
                except Exception:
                    logger.warning("retain pending 快照行损坏（跳过该行）", exc_info=True)
                    continue
            if rows:
                # 口径修正：len(rows) 是**读到的表行数**（含坏行与幂等跳过的
                # 行），不等于实际回灌的会话数——两个数一起打，排查不跑偏
                logger.info(
                    "读取 %d 个 retain pending 快照行（内存 pending 现有 %d 个会话）",
                    len(rows),
                    len(self._retain_pending),
                )
            self._retain_restore_failed = False
        except Exception:
            # 标记待补恢复：表里的崩溃备份行还没回灌内存——后续保存前
            # 必须补恢复，否则会被新快照 UPSERT 覆盖（永久丢失）
            self._retain_restore_failed = True
            logger.warning(
                "retain pending 快照恢复失败（保存前会补恢复）", exc_info=True
            )

    @staticmethod
    def _row_age_seconds(row: Any) -> float:
        """表行 updated_at 距今的秒数（PG 返回 datetime / SQLite 返回 ISO 字符串）。"""
        value = None
        try:
            value = row["updated_at"]
        except (KeyError, IndexError, TypeError):
            return 0.0
        if value is None:
            return 0.0
        if isinstance(value, datetime):
            dt = value
        else:
            try:
                dt = datetime.fromisoformat(str(value))
            except ValueError:
                return 0.0
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        return max((datetime.now(timezone.utc) - dt).total_seconds(), 0.0)

    @staticmethod
    def _row_timestamp(row: Any) -> Optional[datetime]:
        """表行 updated_at 的 datetime（PG 返回 datetime / SQLite 返回 ISO 字符串）。"""
        try:
            value = row["updated_at"]
        except (KeyError, IndexError, TypeError):
            return None
        if value is None:
            return None
        if isinstance(value, datetime):
            dt = value
        else:
            try:
                dt = datetime.fromisoformat(str(value))
            except ValueError:
                return None
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        return dt

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
        """Append a line to the session rolling cache (LRU cap prevents bloat; watermarks are cleared in sync when a session is evicted)."""
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
            # 攒批联动：会话逐出会丢行缓存与水位线，但攒批批次是自包含
            # 快照（history/batch/bot 行都在状态里），不依赖缓存存活——
            # 立即调度编码，避免该批次在表行里长期滞留到巡检超时
            if evicted_sid in self._retain_pending:
                self._flush_pending_batch(evicted_sid)
            logger.warning(
                "会话缓存超上限（%d），逐出最久未活跃会话 %r"
                "（其再次活跃时旧增量将整批重编码）",
                _SESSION_CACHE_MAX_SESSIONS,
                evicted_sid,
            )

    def _history_snapshot(self, sid: str, exclude_ids: set[str]) -> list[str]:
        """Take the most recent N lines of a session (excluding this turn's incremental messages), in time order (old -> new)."""
        bucket = self._history.get(sid)
        if not bucket:
            return []
        lines = [
            v.line for k, v in bucket.items() if k and k not in exclude_ids
        ]
        return lines[-_HISTORY_WINDOW_LINES:]

    @staticmethod
    def _chain_text(chain: Any) -> str:
        """Extract plain text from a message chain (Text elements only)."""
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
        """Timestamp -> datetime (seconds/milliseconds auto-detected; returns None for invalid input)."""
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
        """Common guard for maintenance APIs: 503 when the kernel is not ready.

        Returns the backend object (not the bare pool) - webui_store's
        dual-backend SQL dispatch determines the dialect from
        ``backend.dialect``; a bare pool lacks this attribute and would fall
        back to postgres, pushing PG-dialect SQL into SQLite under the SQLite
        backend (::token syntax errors). webui_store fetches the pool
        internally via ``_pool(backend)``, so call sites need no changes.
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
    #  主动记忆工具（enabled_tools 门控见 inject_memory；search/write 为
    #  nori 双语义版，profile/lookup/correct 由 memory_tools_enabled 管；
    #  常量与准则文本在模块级）
    # ------------------------------------------------------------------

    async def _resolve_tool_name(
        self, session_id: str, name: str
    ) -> tuple[Optional[tuple[str, str]], str]:
        """Resolve a name to a unique (platform, uid) (reuses the entity-hit match mechanism).

        On ambiguity/miss, the second item is hint text for the model; the
        same resolution used for persona candidates / P3 relation nodes
        (window dictionary + persistent alias layer).
        """
        kernel = self._memory_kernel
        if kernel is None:
            return None, "名字解析暂时不可用，请稍后再试。"
        try:
            entries = await kernel.entity_hint_entries(session_id, name)
        except Exception:
            logger.warning("记忆工具名字解析失败", exc_info=True)
            return None, "名字解析暂时不可用，请稍后再试。"
        pairs: list[tuple[str, str]] = []
        labels: dict[tuple[str, str], str] = {}
        for entry_name, plat, uid in entries:
            if (plat, uid) not in pairs:
                pairs.append((plat, uid))
                labels[(plat, uid)] = entry_name
        if not pairs:
            return None, (
                f"没有找到叫「{name}」的成员。名字需要与他在对话中出现过"
                "的称呼一致（支持历史昵称变体）。"
            )
        if len(pairs) > 1:
            candidates = "、".join(
                f"{labels[(p, u)]}[{p}:{u}]" for p, u in pairs[:6]
            )
            return None, (
                f"「{name}」命中多位成员：{candidates}。"
                "请用更完整的名字或加上平台区分后重试。"
            )
        return pairs[0], ""

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
        """Tool scope lock switch (tool_scope_locked, on by default)."""
        config = self._config
        return bool(config is not None and config.tool_scope_locked)

    @register.tool(
        name="memory_search",
        description=(
            "检索长期记忆。两种目标：summary=对话摘要（每轮对话压缩为"
            "一段保真记录，带时间与参与者，是「当时聊了什么」的证据级"
            "材料，陈年记录会归档不再出现在此检索）；fact=事实簇（从"
            "多轮对话合并计分出的人物结论，score 为确信度，仅返回生效"
            "中的簇，是「这个人怎样」的结论级材料）。查「我们聊过/发生"
            "过什么」用 summary；查「某人有什么特点/偏好/约定」用 fact。"
            "查某个成员时，fact 结果会一并返回该成员的确切人际关系边。"
            "query 用简洁关键词，不要复述整句用户消息；第一次不够就换"
            "关键词再查。"
        ),
        params={
            "type": "object",
            "properties": {
                "query": {
                    "type": "string",
                    "description": (
                        "检索关键词（简洁短语或实体名）；target=fact 且传 name 时可省略，"
                        "将按成员称呼检索"
                    ),
                },
                "target": {
                    "type": "string",
                    "enum": ["summary", "fact"],
                    "description": (
                        "检索目标，默认 summary。查某个具体人物"
                        "的特点/偏好/约定/身份或关系时必须选 "
                        "fact，并同时传 name（成员称呼）——"
                        "不传 name 时 fact 只查触发者本人"
                    ),
                },
                "category": {
                    "type": "string",
                    "enum": list(_SEARCH_CATEGORIES),
                    "description": (
                        "仅 target=fact：按维度过滤。"
                        + "；".join(
                            f"{k}={v}"
                            for k, v in _CATEGORY_HINTS.items()
                        )
                        + "。不传则全维度"
                    ),
                },
                "name": {
                    "type": "string",
                    "description": (
                        "仅 target=fact：成员称呼（支持历史昵称与"
                        "群内代号，如 xxy）。查某人的事实结论必传"
                        "；查某人时其人际关系会随 fact 附带返回"
                    ),
                },
                "top_k": {"type": "number", "description": "返回条数上限，默认 5，最大 10"},
                "session_id": {"type": "string", "description": "会话 ID（裸会话 ID；提供则按会话检索；不填默认当前会话）"},
                "user_id": {"type": "string", "description": "用户 ID（提供则按用户跨会话检索）"},
            },
            "required": [],
        },
    )
    # The framework invokes tools as func(event, **args): the first parameter
    # after self receives the message event, tool params arrive by keyword.
    async def memory_search(
        self,
        event,
        *_,
        query: str = "",
        target: str = "summary",
        category: str = "",
        name: str = "",
        top_k: int = 5,
        session_id: str = "",
        user_id: str = "",
    ) -> str:
        denial = self._whitelist_denial(event, "memory_search")
        if denial:
            return denial
        kernel = self._memory_kernel
        if kernel is None or not self._ready:
            return "记忆功能未启用"
        query = (query or "").strip()
        target = (target or "summary").strip()
        category = (category or "").strip()
        name = (name or "").strip()
        if target not in ("summary", "fact"):
            return "错误：target 只能是 summary 或 fact。"
        if category and category not in _SEARCH_CATEGORIES:
            return f"错误：category 只能是 {'/'.join(_SEARCH_CATEGORIES)}。"
        if not query and not (target == "fact" and name):
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
            top_k = max(1, min(int(top_k or 5), _MAX_TOP_K))
        except (TypeError, ValueError):
            top_k = 5
        # EntityDirectory is keyed by the composite session.sid, while the
        # tool scope intentionally uses the bare session_id for DB queries.
        # Keep both forms so name-based fact lookup uses the same key as the
        # recall/entity-hint path.
        entity_sid = str(
            getattr(getattr(event, "session", None), "sid", "") or ""
        ) or sid
        if target == "fact":
            # 批次事件（框架工具分发形态）_event_scope 取不到 uid；fact
            # 通道要求触发者锚定，回退 _trigger_user（与召回同款约定）。
            # 仅作用于事实路——summary 会话检索的 user_id 传参口径不变
            trigger_uid, session_platform = self._trigger_user(event)
            if not uid:
                uid = trigger_uid
            return await self._tool_search_facts(
                kernel, query, top_k, category, name, sid, uid, plat,
                entity_sid=entity_sid,
                session_platform=session_platform,
                trigger_uid=trigger_uid,
            )
        # Asking-about-others recall: match the query against the entity
        # dictionary + persistent aliases; the keys then join the main search
        # path. Isolation posture follows summary_recall_session_scoped via
        # cross_session below (session-pinned entity keys when isolated).
        # 实体词典路用复合 sid：历史行缓存/窗口词典以 session.sid（复合键）
        # 为键，_event_scope 返回的裸 session_id 在词典路恒 miss——与
        # inject_memory 同源口径；无会话对象（手工调用）时回退原值
        entity_keys: list[str] = []
        entity_uids: list[str] = []
        if sid and self._config.recall_hint_enabled:
            try:
                entries = await kernel.entity_hint_entries(entity_sid, query)
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

    def _owner_label(self, platform: str, uid: str) -> str:
        """Attribution label: the newest non-placeholder alias (uid); the dynamic name avoids hard-coding nickname drift."""
        name = ""
        kernel = self._memory_kernel
        alias_store = getattr(kernel, "alias_store", None) if kernel is not None else None
        if alias_store is not None:
            try:
                name = str(alias_store.name_for(platform, uid) or "").strip()
            except Exception:
                logger.warning("归属别名查询失败（回退 uid）", exc_info=True)
                name = ""
        if len(name) > 24:
            name = name[:23] + "…"
        return f"{name}（{uid}）" if name else f"（{uid}）"

    def _format_edge_time(self, kernel, value: object) -> str:
        """Format an edge timestamp through the kernel's configured timezone."""
        helper = getattr(kernel, "_edge_time_qualifier", None)
        if callable(helper):
            try:
                return str(helper(value) or "")
            except Exception:
                logger.warning("关系边时间格式化失败（回退无时限）", exc_info=True)
        else:
            logger.debug("kernel 无关系边时间 helper（回退无时限）")
        return ""

    async def _tool_search_facts(
        self,
        kernel,
        query: str,
        top_k: int,
        category: str,
        name: str,
        sid: str,
        uid: str,
        plat: str,
        entity_sid: str = "",
        session_platform: str = "",
        trigger_uid: str = "",
    ) -> str:
        """The fact channel of memory_search with attached active relation edges.

        Scope lock: with a name, narrow to that member via entity resolution;
        without a name, anchor to the trigger user, and refuse retrieval when
        the user cannot be located (prevents cross-session fact leakage).
        """
        db = self._db
        if db is None:
            return "记忆功能未启用"
        resolved: Optional[tuple[str, str]] = None
        if name:
            if not sid:
                return "错误：缺少会话定位，无法解析名字。"
            resolved, hint = await self._resolve_tool_name(
                entity_sid or sid, name
            )
            if resolved is None:
                return hint
        elif not uid:
            return "错误：缺少用户定位，拒绝检索（作用域锁定）。"
        try:
            query_vec = await kernel.embedding_service.embed_one(query or name)
        except Exception:
            logger.warning("检索向量化失败", exc_info=True)
            return "检索向量化暂不可用，请稍后再试。"
        if not query_vec:
            return "检索向量化暂不可用，请稍后再试。"
        try:
            rows = await db.search_fact_clusters(
                query_vec=query_vec,
                limit=top_k,
                platform=resolved[0] if resolved else plat,
                user_ids=[resolved[1]] if resolved else [uid],
                category=category if category else "",
            )
        except Exception:
            logger.warning("事实簇检索失败", exc_info=True)
            return "记忆检索暂时不可用，请稍后再试"
        target_platform = resolved[0] if resolved else plat
        target_uid = resolved[1] if resolved else uid
        relation_scope_note = ""
        if not target_platform:
            row_platforms = {
                str(row.get("platform") or "") for row in rows
                if str(row.get("user_id") or "") == target_uid and row.get("platform")
            }
            if target_uid == trigger_uid and session_platform:
                target_platform = session_platform
                other_platforms = row_platforms - {session_platform}
                if other_platforms:
                    relation_scope_note = (
                        f"关系边仅限当前会话平台 {session_platform}；"
                        f"未查询事实涉及的其他平台：{'、'.join(sorted(other_platforms))}。"
                    )
            else:
                if len(row_platforms) == 1:
                    target_platform = next(iter(row_platforms))
        relation_scope_missing = not target_platform
        fact_lines: list[str] = []
        for r in rows:
            # user_id 与 platform 同口径 .get 兜底（防缺列行 KeyError 带走
            # 整次检索）；缺 uid 不输出归属段（（空）占位无信息量）
            row_uid = r.get("user_id") or ""
            owner = (
                f"{self._owner_label(r.get('platform') or target_platform, row_uid)} "
                if row_uid
                else ""
            )
            fact_lines.append(
                f"- [簇{r['id']}] ({float(r['score']):.1f}分) "
                f"{owner}"
                f"{str(r['canonical_statement'] or '')[:400]}"
                f"（{r['category']}，{r['status']}，证据{r['evidence_count']}次）"
            )

        edges: list[dict] = []
        relation_lookup_failed = False
        if not relation_scope_missing:
            try:
                edges = [
                    dict(e) for e in await db.fetch_active_edges(
                        [f"{target_platform}:{target_uid}"]
                    )
                ]
            except Exception:
                relation_lookup_failed = True
                logger.warning("memory_search 关系边查询失败（跳过关系小节）", exc_info=True)

        if edges:
            owners: set[tuple[str, str]] = {(target_platform, target_uid)}
            for edge in edges:
                eplat = str(edge.get("platform") or target_platform)
                for side in ("subject", "object"):
                    endpoint = str(edge.get(f"{side}_uid") or "")
                    if endpoint:
                        owners.add((eplat, endpoint))
            try:
                names = await db.fetch_alias_names_by_owner(sorted(owners)) or {}
                for edge in edges:
                    eplat = str(edge.get("platform") or target_platform)
                    for side in ("subject", "object"):
                        endpoint = str(edge.get(f"{side}_uid") or "")
                        if names.get((eplat, endpoint)):
                            edge[f"{side}_name"] = names[(eplat, endpoint)]
            except Exception:
                logger.warning("memory_search 关系边规范名解析失败（回退快照名）", exc_info=True)

        sections: list[str] = []
        if fact_lines:
            sections.append("【事实】\n" + "\n".join(fact_lines))
        if edges:
            relation_lines: list[str] = []
            bot_uid: object = self._bot_user_id or getattr(kernel, "bot_id", "")
            bot_uid_forms = getattr(kernel, "_bot_uid_forms_all", None)
            if callable(bot_uid_forms):
                try:
                    bot_uid = bot_uid_forms(bot_uid)
                except Exception:
                    logger.warning("关系边 bot 标识解析失败（回退主 uid）", exc_info=True)
            for edge in edges[:top_k]:
                try:
                    relation_lines.append(
                        "- " + str(relation_statement_line(
                            edge,
                            bot_uid=bot_uid,
                            session_platform=session_platform or plat,
                            bot_nickname=self._bot_nickname,
                            time_qualifier=self._format_edge_time(
                                kernel, edge.get("last_seen")
                            ),
                        ))[:400]
                    )
                except Exception:
                    logger.warning("memory_search 关系边渲染失败（跳过该边）", exc_info=True)
            if relation_lines:
                sections.append("【关系】\n" + "\n".join(relation_lines))
        if relation_scope_missing:
            sections.append("【关系】\n- 关系边暂不可查询：缺少平台上下文。")
        elif relation_lookup_failed:
            sections.append("【关系】\n- 关系边暂不可查询，请稍后再试。")
        if relation_scope_note and not relation_lookup_failed:
            sections.append(relation_scope_note)
        return "\n\n".join(sections) if sections else "没有找到相关事实或关系。"

    @register.tool(
        name="memory_write",
        description=(
            "把一条用户明确要求记住的人物事实直接写入事实库并立即"
            "生效（无需等待后台合并）。仅当用户明确说「记住/记下」时"
            "调用；对话中自然流露的信息不要用此工具（后台会自动提取）。"
            "text 必须是绑定主体的第三人称完整陈述句。仅当用户更正一条"
            "既有记忆时传 replaces_cluster_id（来自 memory_search 的 "
            "fact 检索结果，新旧说法在单次调用内原子替换）。写入归属"
            "锁定为当前用户，不可代他人写入。"
        ),
        params={
            "type": "object",
            "properties": {
                "text": {
                    "type": "string",
                    "description": (
                        "完整陈述句（第三人称绑定主体，如「小周不吃辣」；"
                        "commitment 须含具体日期，如「9 月 12 日和小林去爬山」）"
                    ),
                },
                "category": {
                    "type": "string",
                    "enum": list(_ACTIVE_CATEGORIES),
                    "description": (
                        "六选一："
                        + "；".join(
                            f"{k}={v}"
                            for k, v in _CATEGORY_HINTS.items()
                            if k in _ACTIVE_CATEGORIES
                        )
                        + "。拿不准归 stable"
                        + "。relation 不在值域：人际关系类内容"
                        "（谁是谁的什么人）不支持此参数，勿传入"
                    ),
                },
                "confidence": {
                    "type": "string",
                    "enum": ["high", "medium"],
                    "description": "置信度（默认 high：用户明确指令）",
                },
                "replaces_cluster_id": {
                    "type": "integer",
                    "description": (
                        "被更正的旧簇 id（仅更正场景传；新簇继承旧簇"
                        "分数，旧说法转为已替代）"
                    ),
                },
                "session_id": {
                    "type": "string",
                    "description": (
                        "归属会话 ID（裸会话 ID；仅宿主关闭 "
                        "tool_scope_locked 时生效，默认钉死为当前"
                        "会话，通常不填）"
                    ),
                },
                "user_id": {
                    "type": "string",
                    "description": (
                        "归属用户 ID（仅宿主关闭 tool_scope_locked 时"
                        "生效，默认钉死为触发者，通常不填）"
                    ),
                },
                "platform": {
                    "type": "string",
                    "description": (
                        "归属平台（与 user_id 配套；仅宿主关闭 "
                        "tool_scope_locked 时生效）"
                    ),
                },
            },
            "required": ["text", "category"],
        },
    )
    async def memory_write(
        self,
        event,
        *_,
        text: str,
        category: str = "",
        confidence: str = "high",
        replaces_cluster_id=None,
        session_id: str = "",
        user_id: str = "",
        platform: str = "",
    ) -> str:
        denial = self._whitelist_denial(event, "memory_write")
        if denial:
            return denial
        kernel = self._memory_kernel
        db = self._db
        if kernel is None or db is None or not self._ready:
            return "记忆功能未启用"
        text = (text or "").strip()
        category = (category or "").strip()
        confidence = (confidence or "high").strip()
        if not text:
            return "text 不能为空"
        if len(text) > _MAX_STATEMENT_CHARS:
            return f"错误：text 超过 {_MAX_STATEMENT_CHARS} 字，请精简为一句陈述。"
        if category not in _ACTIVE_CATEGORIES:
            return (
                "错误：category 六选一（identity/stable/preference/"
                "commitment/interaction/naming）。"
            )
        if confidence not in ("high", "medium"):
            return "错误：confidence 只能是 high 或 medium。"
        sid = (session_id or "").strip()
        uid = (user_id or "").strip()
        plat = (platform or "").strip()
        if self._tool_scope_lock_enabled():
            # 作用域锁定：写入归属钉死为触发会话/触发者（防越权写入
            # 他人名下伪造记忆）；作用域派生失败时显式拒绝
            sid, uid, plat = self._event_scope(event)
            # 批次事件（框架工具分发形态）无 message 属性，_event_scope
            # 取不到 uid——回退 _trigger_user（与召回同款触发者约定）
            if not uid:
                uid, _ = self._trigger_user(event)
            if not (sid or uid):
                return "作用域锁定开启且无法识别当前会话/用户，拒绝写入"
        elif not (sid and uid and plat):
            ev_sid, ev_uid, ev_plat = self._event_scope(event)
            sid = sid or ev_sid
            uid = uid or ev_uid
            plat = plat or ev_plat
        if not (plat and uid):
            return "错误：缺少用户定位，拒绝写入（作用域锁定）。"
        replaces: Optional[int] = None
        raw_replaces: Optional[int] = None
        if (
            replaces_cluster_id is not None
            and str(replaces_cluster_id).strip() != ""
        ):
            try:
                raw_replaces = int(str(replaces_cluster_id).strip())
            except (TypeError, ValueError):
                return "错误：replaces_cluster_id 必须是整数（memory_search 结果里的簇 id）。"
        if raw_replaces is not None:
            replaces = raw_replaces
            # 作用域锁定（replace 目标校验，对齐 nori PR#4 安全修复）：
            # 模型可能把 memory_search(name=他人) 拿到的簇 id 误作更正
            # 目标——跨用户 replace 会把他人簇置 replaced 并删其画像
            # 投影。目标簇必须属于触发者
            try:
                owner = await db.fetch_cluster_owner(replaces)
            except Exception:
                logger.warning("replace 目标归属校验失败", exc_info=True)
                return "写入失败，请稍后再试。"
            # 不存在与属于他人同款输出：簇 id 自增可枚举，双文案即存在性
            # 预言机（cluster_status_op / restore_summary 均已同款化，此处
            # 对齐；引导语义并入同一条文本）
            if owner is None or owner != (plat, uid):
                return (
                    "错误：更正目标不存在或不属于当前用户（作用域锁定）；"
                    "他人的说法变化请作为新观察记录，不要替代其原有簇，"
                    "自己的簇请先用 memory_lookup 确认 id。"
                )
        try:
            summary = await kernel.write_fact(
                statement=text,
                category=category,
                confidence=confidence,
                platform=plat,
                session_id=sid,
                group_id="",
                user_id=uid,
                display_name="",
                replaces_cluster_id=replaces,
            )
        except Exception:
            logger.warning("memory_write 工具失败", exc_info=True)
            return "写入失败：记忆库暂时不可用，请稍后再试"
        if summary.get("action") == "skipped":
            if replaces is not None:
                # 更正路径换幂等键重试后仍 skipped = 同一更正当日已执行过
                return "这条更正今天已经执行过了（幂等跳过，未重复执行）。"
            return "这条事实今天已经记录过了（幂等跳过，未重复计分）。"
        cluster_id = summary.get("cluster_id")
        embedded_note = (
            ""
            if summary.get("embedded", True)
            else "（向量服务暂不可用，数分钟内自动补算后才能被检索到）"
        )
        if summary.get("action") == "replace":
            return (
                f"已更正：新记忆已生效（簇 {cluster_id}，替代旧簇 "
                f"{replaces}，分数继承 {summary.get('score')}）。{embedded_note}"
            )
        return (
            f"已记住（簇 {cluster_id}，当前状态 {summary.get('status')}）。"
            f"{embedded_note}"
        )

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
        # 契约：True=已删除，False=查证不存在，None=DB 故障/熔断开启——
        # None 须与"不存在"区分回报（否则用户会误以为 ID 写错了）
        if removed is None:
            return "记忆服务暂时不可用，请稍后再试"
        if removed:
            return "已删除"
        if scope_sid or scope_uid:
            return "记忆条目不存在或不属于当前会话/用户"
        return "记忆条目不存在"

    @register.tool(
        name="memory_profile",
        description=(
            "按名字查询一位成员的画像（基本信息/称呼/事实/喜好/约定/"
            "互动偏好/近期动态/待定信息）。上下文画像块中已出现的人"
            "不要再查；仅当想了解的人不在画像块中时使用（名字需与他在"
            "对话中出现过的称呼一致，支持历史昵称变体）。"
        ),
        params={
            "type": "object",
            "properties": {
                "name": {
                    "type": "string",
                    "description": "成员名字（对话中出现过的称呼）",
                },
            },
            "required": ["name"],
        },
    )
    async def memory_profile(self, event, *_, name: str) -> str:
        denial = self._whitelist_denial(event, "memory_profile")
        if denial:
            return denial
        kernel = self._memory_kernel
        persona = self._persona_service
        if kernel is None or persona is None or not self._ready:
            return "记忆功能未启用"
        name = (name or "").strip()
        if not name:
            return "错误：name 不能为空。"
        sid, _uid, _plat = self._event_scope(event)
        if not sid:
            return "错误：缺少会话定位，无法解析名字。"
        entity_sid = str(
            getattr(getattr(event, "session", None), "sid", "") or ""
        ) or sid
        resolved, hint = await self._resolve_tool_name(entity_sid, name)
        if resolved is None:
            return hint
        platform, uid = resolved
        try:
            text = await persona.build_profile_text(
                uid, session_id=sid, platform=platform
            )
        except Exception:
            logger.warning("memory_profile 工具失败", exc_info=True)
            return "画像查询失败，请稍后再试。"
        if not text:
            return f"「{name}」暂无画像内容（还没有足够的对话事实沉淀）。"
        return text

    @register.tool(
        name="memory_lookup",
        description=(
            "维护性深查：检索**非激活状态**的记忆——已归档的对话摘要"
            "与已失效/被替代/待定的事实簇（memory_search 查不到它们）。"
            "结果带状态标注与 id；用户提起「很久以前的事」而常规检索"
            "为空、或想恢复某条被淡忘的记忆时使用，随后可用 "
            "memory_correct(reactivate) 重新激活。"
        ),
        params={
            "type": "object",
            "properties": {
                "query": {"type": "string", "description": "检索关键词"},
                "target": {
                    "type": "string",
                    "enum": ["summary", "fact"],
                    "description": "检索目标，默认 summary",
                },
                "top_k": {
                    "type": "integer",
                    "description": "返回条数上限，默认 5，最大 10",
                },
            },
            "required": ["query"],
        },
    )
    async def memory_lookup(
        self, event, *_, query: str, target: str = "summary", top_k: int = 5
    ) -> str:
        denial = self._whitelist_denial(event, "memory_lookup")
        if denial:
            return denial
        kernel = self._memory_kernel
        db = self._db
        if kernel is None or db is None or not self._ready:
            return "记忆功能未启用"
        query = (query or "").strip()
        if not query:
            return "错误：query 不能为空。"
        target = (target or "summary").strip()
        if target not in ("summary", "fact"):
            return "错误：target 只能是 summary 或 fact。"
        try:
            top_k = max(1, min(int(top_k or 5), _MAX_TOP_K))
        except (TypeError, ValueError):
            top_k = 5
        sid, uid, plat = self._event_scope(event)
        # 批次事件（框架工具分发形态）无 message 属性，_event_scope 取不到
        # uid——回退 _trigger_user（与召回同款触发者约定）；uid 仅 fact
        # 通道消费，summary 通道只看 sid，此处统一回退无副作用
        if not uid:
            uid, _ = self._trigger_user(event)
        try:
            query_vec = await kernel.embedding_service.embed_one(query)
            if not query_vec:
                return "检索向量化暂不可用，请稍后再试。"
            if target == "summary":
                if not sid:
                    # 防止 db 侧退化全库召回（与 search 同款拒绝）
                    return "错误：缺少会话定位，拒绝检索（作用域锁定）。"
                rows = await db.search_chat_summaries(
                    query_vec=query_vec,
                    limit=top_k,
                    scope="session",
                    session_id=sid,
                    # session_id 存的是裸 id：不带平台过滤时其他适配器上的
                    # 同号会话归档摘要会漏进来（跨平台泄漏），与
                    # memory_search 会话路口径对齐
                    platform=plat,
                    only_archived=True,
                    query_text=query,
                    hybrid=self._config.hybrid_search_enabled,
                    with_embedding=False,
                )
                # 契约：工具描述承诺只检索非激活记忆——only_archived 把
                # 过滤下推进 SQL，生效行不进相似度候选窗，数量再多也挤
                # 不出归档行（固定倍数池 + 调用方过滤会在生效行超池时
                # 假阴性）；db 侧行序即相似度序
                if not rows:
                    return "没有找到已归档的相关记忆。"
                lines = []
                for r in rows:
                    lines.append(
                        f"- [id:{r['id']}][{r['document_id']}][已归档] "
                        f"({float(r['relevance']):.2f}) · "
                        f"{str(r['content'] or '')[:400]}"
                    )
                return "\n".join(lines)
            if not uid:
                # 作用域锁定：维护检索钉死触发者，防翻出其他用户事实簇
                return "错误：缺少用户定位，拒绝检索（作用域锁定）。"
            rows = await db.search_fact_clusters(
                query_vec=query_vec,
                limit=top_k,
                platform=plat,
                user_ids=[uid],
                only_inactive=True,
            )
            # 契约同上：only_inactive 在 SQL 侧只留 replaced/dead/
            # pending_uncertain，active/profiled 的生效簇走 memory_search
            # 的既有通道，不进此处候选窗
            if not rows:
                return "没有找到已失效/被替代/待定的相关事实簇。"
            markers = {
                "replaced": "[已替代]",
                "dead": "[已失效]",
                "pending_uncertain": "[待定]",
            }
            lines = []
            for r in rows:
                marker = markers.get(str(r["status"]), "")
                # user_id 与 platform 同口径 .get 兜底（见 memory_search）；
                # 行内变量名 row_uid 防遮蔽函数级 uid 作用域锚点
                row_uid = r.get("user_id") or ""
                owner = (
                    f"{self._owner_label(r.get('platform') or plat, row_uid)} "
                    if row_uid
                    else ""
                )
                lines.append(
                    f"- [簇{r['id']}]{marker} {owner}"
                    f"{str(r['canonical_statement'] or '')[:400]}"
                    f"（{r['category']}，证据{r['evidence_count']}次）"
                )
            return "\n".join(lines)
        except Exception:
            logger.warning("memory_lookup 工具失败", exc_info=True)
            return "维护检索失败，请稍后再试。"

    @register.tool(
        name="memory_correct",
        description=(
            "调整记忆状态（不修改内容本身）。action：drop=彻底否定"
            "（簇转失效并退出画像）；dispute=存疑降权（打矛盾标记，"
            "自然衰减淡出，适合拿不准或部分过时）；reactivate=重新"
            "激活（归档摘要恢复召回 / 失效簇复活，与 memory_lookup "
            "配合）。更正内容本身请用 memory_write(replaces_cluster_id)"
            " 原子替换。id 来自 memory_search / memory_lookup 的结果。"
        ),
        params={
            "type": "object",
            "properties": {
                "target": {
                    "type": "string",
                    "enum": ["summary", "fact"],
                    "description": "操作对象：对话摘要 / 事实簇",
                },
                "id": {
                    "type": "integer",
                    "description": (
                        "行 id（fact=簇 id；summary 为摘要行 id——"
                        "取 memory_lookup 结果行的 [id:N] 前缀，"
                        "非 document_id）"
                    ),
                },
                "action": {
                    "type": "string",
                    "enum": ["drop", "dispute", "reactivate"],
                    "description": "drop/dispute 仅 fact；reactivate 两者皆可",
                },
                "reason": {
                    "type": "string",
                    "description": "操作原因（用户原话或语境，审计留痕）",
                },
            },
            "required": ["target", "id", "action", "reason"],
        },
    )
    async def memory_correct(
        self, event, *_, target: str, id: int, action: str, reason: str
    ) -> str:
        denial = self._whitelist_denial(event, "memory_correct")
        if denial:
            return denial
        db = self._db
        if db is None or not self._ready:
            return "记忆功能未启用"
        target = (target or "").strip()
        action = (action or "").strip()
        reason = (reason or "").strip()
        if not reason:
            return "错误：reason 必填（审计留痕）。"
        try:
            row_id = int(id)
        except (TypeError, ValueError):
            return "错误：id 必须是整数。"
        if target not in ("summary", "fact"):
            return "错误：target 只能是 summary 或 fact。"
        sid, uid, plat = self._event_scope(event)
        # 批次事件（框架工具分发形态）无 message 属性，_event_scope 取不到
        # uid——回退 _trigger_user（与召回同款触发者约定）；uid 仅 fact
        # 通道消费，summary 通道只看 sid，此处统一回退无副作用
        if not uid:
            uid, _ = self._trigger_user(event)
        try:
            if target == "summary":
                # 作用域锁定：恢复归属钉死当前会话（自增 id 可被诱导
                # 传入他人行 id，db 侧 session 校验拦截跨用户篡改）
                if action != "reactivate":
                    return "错误：摘要只支持 reactivate（删除请走 memory_remove）。"
                if not sid:
                    return "错误：缺少会话定位，拒绝调整（作用域锁定）。"
                restored = await db.restore_summary(row_id, sid)
                if not restored:
                    return "未变更：本会话没有该归档摘要。"
                logger.info(
                    "[记忆工具] 归档摘要已恢复: id=%s reason=%s", row_id, reason
                )
                return "已恢复：该摘要重新进入召回。"
            if action not in ("drop", "dispute", "reactivate"):
                return "错误：fact 支持 drop / dispute / reactivate。"
            if not (plat and uid):
                return "错误：缺少用户定位，拒绝调整（作用域锁定）。"
            result = await db.cluster_status_op(
                row_id, action, platform=plat, user_id=uid
            )
            if result.get("changed"):
                logger.info(
                    "[记忆工具] 簇状态调整: id=%s action=%s reason=%s",
                    row_id, action, reason,
                )
                verb = {
                    "drop": "已否定（转失效并退出画像，可再激活）",
                    "dispute": "已标记存疑（降权淡出，再被提及时自动恢复确信）",
                    "reactivate": "已重新激活",
                }[action]
                return verb + "。"
            status = str(result.get("status") or "")
            if not status:
                return "未变更：簇不存在。"
            if action == "reactivate" and status == "replaced":
                successor = result.get("replaced_by")
                return (
                    f"该说法已有更正版本（簇 {successor}）。如更正本身有误，"
                    f"请对继任簇 {successor} 执行 dispute，或用 memory_write 带 "
                    f"replaces_cluster_id={successor} 重新更正。"
                )
            return f"未变更：簇当前状态为 {status}，不适用 {action}。"
        except Exception:
            logger.warning("memory_correct 工具失败", exc_info=True)
            return "记忆状态调整失败，请稍后再试。"

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
        """full=True ignores the watermark and reruns in full (default incremental: only refills clusters newer than the last progress)."""
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
        """The owning bot of global-scope facts (platform/uid/nickname; empty strings when assembly is not ready)."""
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
        """Import preview: source scan counts + the most recent import result (writes no data)."""
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
        """Run the import (source read-only; target idempotent, repeated runs are auto-deduped).

        body is optional {"source_path": "..."}; defaults to the inferred host
        data/memory.
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

    @register.api(method="GET", path="/memory/kira_memory/import_host/preview")
    async def api_kira_memory_import_host_preview(self):
        """Preview the host-DB import source (KiraAI v3 keeps session memory in its database)."""
        session_mgr = getattr(self.ctx, "session_mgr", None)
        if session_mgr is None:
            raise HTTPException(status_code=503, detail="宿主 session_mgr 不可用")
        backend = self._pool_or_503()
        sessions = await session_mgr.get_sessions(include_content=True)
        return {
            "source_path": "host_db",
            "preview": scan_host_sessions(sessions),
            "last_run": await read_host_last_run(backend),
        }

    @register.api(method="POST", path="/memory/kira_memory/import_host")
    async def api_kira_memory_import_host_run(self):
        """Import the host database's sessions (conversation summaries only).

        v3 writes session context to the database and stops updating
        data/memory/chat_memory.json, so this entry point covers memory
        created after the upgrade; the file-based import stays for the
        legacy v2 tree.
        """
        if getattr(self, "_kira_memory_import_running", False):
            raise HTTPException(status_code=409, detail="已有一次迁入在执行中")
        backend = self._pool_or_503()
        session_mgr = getattr(self.ctx, "session_mgr", None)
        if session_mgr is None:
            raise HTTPException(status_code=503, detail="宿主 session_mgr 不可用")
        _, _, bot_nickname = self._kira_memory_bot_identity()
        self._kira_memory_import_running = True
        try:
            sessions = await session_mgr.get_sessions(include_content=True)
            stats = await run_host_db_import(
                backend, sessions=sessions, bot_nickname=bot_nickname,
            )
        finally:
            self._kira_memory_import_running = False
        logger.info(
            "[WebUI] 宿主数据库记忆迁入完成: sessions=%s summary_rows=%s",
            stats.get("sessions"), stats.get("summary_rows"),
        )
        return {"ok": True, "stats": stats}

    # ------------------------------------------------------------------
    #  设置栏（插件配置读写：真相源在宿主，运行时实例就地热更新）
    # ------------------------------------------------------------------

    def _plugin_id(self) -> str:
        """The plugin_id from manifest.json (the host config storage key; cached after startup)."""
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
        pm = getattr(self.ctx, "plugin_mgr", None)
        if pm is None or self._config is None:
            raise HTTPException(status_code=503, detail="配置端点未装配")
        host_values = pm.get_plugin_config(self._plugin_id()) or {}
        return config_web.build_payload(host_values)

    @register.api(method="PUT", path="/memory/config")
    async def api_memory_config_put(self, body: dict = Body(...)):
        """Save config: validate -> hot-update the runtime instance in place -> sync the host source of truth.

        Does not use pm.update_plugin_config (that would re-initialize the
        whole plugin via init_plugin and tear down/recreate pools) - instead
        it directly updates the host in-memory dict and persists
        PLUGIN_CONFIG_DIR/<pid>.json, consistent with the host config page
        reading from memory; kernel/merge_agent/encoder/circuit breaker read
        the same LocalMemoryConfig instance at runtime, so the save takes
        effect immediately. Fields expanded at assembly time
        (restart_required) are returned as-is; they need a re-initialization
        or process restart to take effect.
        """
        pm = getattr(self.ctx, "plugin_mgr", None)
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
        # timezone 变更后使 kernel 与 persona 两处时区缓存失效：缓存首次
        # 解析后不再失效（相对时间标注/幂等键日期口径/画像尾注共用），
        # 不重置则继续用旧时区
        if "timezone" in changed:
            if self._memory_kernel is not None:
                self._memory_kernel.reset_local_tz_cache()
            if self._persona_service is not None:
                self._persona_service.reset_local_tz_cache()

        # 2) 宿主真相源同步：内存 dict（宿主配置页/下次 init_plugin 读）+ 磁盘
        merged_host = dict(host_values)
        merged_host.update(changed)
        # Do NOT use pm.update_plugin_config: it calls init_plugin and tears down
        # the running plugin (pools/background tasks); sync the in-memory dict
        # defensively instead, degrading to disk-only + warning if it is absent.
        if isinstance(getattr(pm, "plugin_configs", None), dict):
            pm.plugin_configs[pid] = merged_host
        else:
            logger.warning(
                "宿主 PluginManager 未暴露 plugin_configs，内存配置未同步"
                "（磁盘写入仍会执行，宿主重启后生效）"
            )
        persistence_error = ""
        try:
            # v3: plugin_registry was removed; PLUGIN_CONFIG_DIR moved to core.plugin.manager
            from core.plugin.manager import PLUGIN_CONFIG_DIR

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
