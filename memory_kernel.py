"""LocalMemoryKernel：基于 PostgreSQL + pgvector 的本地记忆内核。

写入链路（M2）：
- ingest: 摘要/bot_self 原文写入 memory_chat_summary（document_id 幂等，
  写入时计算 embedding，失败置 NULL 待补算）；
- retain_encoded: 端侧编码多通道——facts 批量写入 memory_persona_fact_raw
  （extracted_flag=0，附 evidence_key 计分去重键，等待 M4 合并 agent 消费）、
  relations 结构化三元组零 LLM 合并落 memory_entity_edge（P2 通道）、
  summary 走 ingest 同路径殿后（失败方向安全，见 retain_encoded docstring）。

召回链路（M3，见开发方案 §9.2）：
- search: query 实时向量化 -> scope 过滤 + cosine top-N 候选 ->
  RerankClient 重排序（可配关闭，失败退化纯向量序）-> 相关度阈值 ->
  top_k 截断（per_user 模式复用同一管线，仅截断阶段换用户配额 +
  公共位；能力位暂不启用，见 search docstring）；
- scope 模式：session（默认，语义对齐 hindsight tag 组合）/ user（用户精确，
  participants 数组过滤天然跨会话）/ user_session（用户 + 会话收紧）；
- build_injection_text: 话题黑名单二次防线 + token 预算截断 + 注入格式
  逐字对齐 hindsight 版（主排除在 search 候选层 SQL 结构性完成）；
- persona_fact 对 recall 不可见由结构保证（事实/簇表不参与检索）。

DB 故障语义：recall 读取经熔断器守卫，失败记 warning 返回空（fail-open，
不阻断注入链路，与 hindsight 后端行为对齐）；retain 写路径相反——熔断
拒绝期/写入执行失败上抛 MemoryDBUnavailable，由 retain_encoded 包装层
捕获入 pending 重试队列（宿主无重试机制，插件内队列等价实现「内容不丢」）；
合并 agent（merge_agent）的周期任务另行在周期入口 peek 熔断状态
（拒绝期整周期跳过，不烧 LLM 预算）。
"""

from __future__ import annotations

from core.logging_manager import get_logger

import asyncio
import hashlib
import math
import time
from collections import OrderedDict
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Awaitable, Callable, Optional, TypeVar
from zoneinfo import ZoneInfo

from .alias_store import AliasStore, is_placeholder_name
from .circuit_breaker import MemoryDBCircuitBreaker
from .clients import KiraRerankClient
from .config import LocalMemoryConfig
from .contracts import (
    EncodedFact,
    KnowledgeType,
    MemoryItem,
    PersonaCandidate,
)
from .db import MemoryDatabase, fact_document_id, parse_vector
from .entity_edge import (
    RELATION_HEADER,
    EncodedRelation,
    edge_has_bot_endpoint,
    neighbor_profile_uids,
    relation_document_id,
    relation_statement_line,
    select_relation_edges,
)
from .memory_encoder import MemoryEncoder
from .recall_log import RecallLogWriter
from .vector_ops import EmbeddingService

logger = get_logger("noriflow_memory.kernel", "cyan")

_T = TypeVar("_T")


class MemoryDBUnavailable(RuntimeError):
    """记忆库不可用（熔断拒绝期或写入执行失败）。

    retain 路径以此上抛：调用方（_do_retain）回滚水位线，本批消息留给
    下一轮信号重编码（内容不丢）；memory_write 等工具路径由入口捕获，
    向 LLM 回报写入失败。替代旧 fail-open 契约（拒绝后仍返回
    document_id）——那会让批次被标已消费而实际未落库，静默丢失。
    """

# scope 合法值
_VALID_SCOPES = frozenset({"session", "user", "user_session"})


@dataclass
class RecallHints:
    """实体命中/P3 关系注入的传输载体（不再驱动召回候选拉取）。

    v0.5.1 设计修订：召回提示旁路（实体词典路 + 引用反查路）整体
    移除——实体路的恒跨会话拉取与会话隔离基线（summary_recall_
    session_scoped）冲突，引用路的时间窗反查被「引用原文并入
    recall query 走主路」取代（适配层把被引用消息文本拼进 query，
    语义召回天然受会话隔离开关管）。hints 仅承载：

    - entity_user_keys: 实体命中复合键（"platform:uid"）——画像候选
      与 P3 关系注入节点匹配的输入；
    - match_text: 实体词典同源匹配文本（含 @昵称 渲染）——P3 边注入
      的 label 词形命中面（与名字匹配同一文本，适配层组装）；
    - bot_addressed: 本轮输入是否 AT bot 或引用 bot 消息（场景 C 硬门槛，
      适配层判定；私聊恒 True——整场对话就是对 bot 说的。False 时
      bot 端点边不参与注入）。
    - bot_user_id: bot 平台 uid（如 QQ 号），适配层从平台适配器解析。
      与宿主注入的 bot_id（会话标识）是同一 bot 的两种 uid 形态——
      边表 bot 端点两种形态都存在（提取提示词主形态=bot_id，@段
      词典渗入副形态=平台 uid），注入按集合匹配通吃（bot_uid_set）。
    """

    entity_user_keys: list[str] = field(default_factory=list)
    match_text: str = ""
    bot_addressed: bool = False
    bot_user_id: str = ""


class _EntityDirectory:
    """会话级实体词典（名字 → 参与者 (platform, uid) 列表），懒构建 + TTL + LRU。

    名字源由适配层注入（directory_source 回调）：KiraAI 侧取插件滚动行
    缓存（宿主已观察到的发送者昵称/群名片）——均为可靠名字，不从摘要
    文本抽数（误抽风险留 v2）。TTL 到期自愈重建；改名/账号合并在 TTL
    窗口内不感知，属旁路可接受的延迟（词典只是候选来源，误命中由
    重排与画像空栏兜底）。
    """

    _TTL_SECONDS = 600.0
    _MAX_SESSIONS = 256
    _MIN_NAME_LEN = 2  # 单字名（"阿"/"好"）误命中率过高，不入词典
    _MAX_HINT_ENTRIES = 4  # 单轮命中条目上限（防重名/常用词昵称候选风暴）

    def __init__(self, source: Optional[Callable[[str], Awaitable[list[tuple[str, str, str]]]]]):
        self._source = source
        self._cache: OrderedDict[str, tuple[float, dict[str, list[tuple[str, str]]]]] = OrderedDict()

    async def match(self, session_id: str, text: str) -> list[tuple[str, str, str]]:
        """返回文本命中条目 [(名字, platform, uid)]（去重保序，含重名多键）。

        结构化返回：召回提示路投影为复合键，画像候选路直接消费
        （命中的名字即 display_name，保证画像标题与聊天称呼一致）。
        """
        if not text or not session_id or self._source is None:
            return []
        now = time.monotonic()
        entry = self._cache.get(session_id)
        if entry is None or now - entry[0] > self._TTL_SECONDS:
            try:
                pairs = await self._source(session_id)
            except Exception:
                logger.warning(
                    "实体词典构建失败（本轮词典路跳过）session=%s", session_id,
                    exc_info=True,
                )
                return []
            names: dict[str, list[tuple[str, str]]] = {}
            for raw_name, platform, uid in pairs or []:
                name = (raw_name or "").strip()
                if len(name) < self._MIN_NAME_LEN or not uid:
                    continue
                pair = (platform or "", str(uid))
                bucket = names.setdefault(name, [])
                if pair not in bucket:
                    bucket.append(pair)
            entry = (now, names)
            self._cache[session_id] = entry
            self._cache.move_to_end(session_id)
            while len(self._cache) > self._MAX_SESSIONS:
                self._cache.popitem(last=False)
        matched: list[tuple[str, str, str]] = []
        seen_pairs: set[tuple[str, str]] = set()
        for name, pairs in entry[1].items():
            if name not in text:
                continue
            for platform, uid in pairs:
                if (platform, uid) not in seen_pairs:
                    seen_pairs.add((platform, uid))
                    matched.append((name, platform, uid))
        return matched[: self._MAX_HINT_ENTRIES]


class LocalMemoryKernel:
    """本地记忆内核（PostgreSQL + pgvector）。

    职责（自 nori-core noriflow 插件平移）：
    - ingest: 写入 memory_chat_summary（chat_summary / bot_self 两类 kind）
    - retain_encoded: 端侧编码后双通道写入（chat_summary 表 + fact 原始表），
      encoder 未装配或编码降级时退化为单通道 ingest（原文走摘要表标记
      summarized=false，不参与召回，由合并 agent 补编码遍重编码）
    - search / build_injection_text: M3 占位（返回空，不阻断注入链路）
    """

    def __init__(
        self,
        db: MemoryDatabase,
        embedding_service: EmbeddingService,
        circuit_breaker: MemoryDBCircuitBreaker,
        config: LocalMemoryConfig,
        bot_id: str,
        encoder: Optional[MemoryEncoder] = None,
        bot_nickname: str = "",
        rerank_client: KiraRerankClient | None = None,
        history_window_provider: Callable[[], int] | None = None,
        host_tz_provider: Callable[[], object] | None = None,
        recall_log: RecallLogWriter | None = None,
        directory_source: Optional[Callable[[str], Awaitable[list[tuple[str, str, str]]]]] = None,
    ) -> None:
        """初始化记忆内核。

        Args:
            db: 记忆库访问层（已 connect + 迁移完成）。
            embedding_service: 向量计算服务（不可用时行内向量置 NULL）。
            circuit_breaker: DB 熔断器。
            config: 插件运行时配置。
            bot_id: 当前 bot 唯一标识（bot_self 归属）。
            encoder: 端侧记忆编码器；None 时 retain_encoded 走单通道降级。
            bot_nickname: Bot 昵称（编码器提示词排除项）。
            rerank_client: 重排序客户端（None 或 config.rerank_enabled=false
                时检索退化为纯向量序）。
            history_window_provider: 宿主历史窗口块数读取器（近时排除/
                滚动补回的锚定用；返回 max_memory_length，活读使热改即时
                生效；None 时两能力关闭）。
            host_tz_provider: 宿主时区读取器（locale.TZ；插件 timezone
                未配置时的回退源）。
            recall_log: 召回评估日志写器（None 关闭日志）。
        directory_source: 实体词典名字源回调（session_id -> [(名字,
            platform, uid)]，见 _EntityDirectory；None 关闭词典路）。
        """
        self.db = db
        self.embedding_service = embedding_service
        self.circuit_breaker = circuit_breaker
        self.config = config
        self.bot_id = bot_id
        # bot uid 形态备忘（含平台 uid 副形态；make_recall_hints 学习式
        # 回填——写侧 is_bot_edge 与读侧场景 C 匹配共用，见 bot_uid_set）
        self._bot_uid_forms: set[str] = set()
        self.encoder = encoder
        self.bot_nickname = bot_nickname
        self.rerank_client = rerank_client
        self._history_window_provider = history_window_provider
        self._host_tz_provider = host_tz_provider
        self._recall_log = recall_log
        # 实体词典（实体命中用——画像候选/P3 节点匹配；source 为 None 时恒空）
        self._entity_directory = _EntityDirectory(directory_source)
        # 持久实体别名层（窗口词典之后的持久命中来源；alias_enabled 关闭时恒空）
        self._alias_store: AliasStore | None = (
            AliasStore(
                db,
                variant_cap=config.alias_variant_cap,
                stopwords=config.alias_stopwords,
            )
            if config.alias_enabled
            else None
        )
        # P3 边注入的邻居画像服务（main 装配后回填；None 时边注入只出
        # 陈述行、不拼画像——画像预算独立于宿主画像注入）
        self.persona_service = None
        self._local_tz_cache = None
        # 滚动补回去重 memo：{session_id: (已注入 document_id 列表, monotonic 时间)}
        # 同轮 recall/主动检索据此排除已注入行；TTL 覆盖单轮 pipeline 时长
        self._rollout_memo: dict[str, tuple[list[str], float]] = {}
        # 宿主窗口块数为 0 的一次性告警旗标（见 _derive_window_batches）
        self._window_zero_warned = False

    # ------------------------------------------------------------------
    #  写入链路
    # ------------------------------------------------------------------

    async def ingest(
        self,
        content: str,
        session_id: str,
        user_id: str = "",
        memory_category: KnowledgeType = KnowledgeType.EPISODIC,
        platform: str = "",
        group_id: str = "",
        kind: str = "chat_summary",
        timestamp: Optional[datetime] = None,
        bot_id: str = "",
        participant_user_ids: Optional[list[tuple[str, str]]] = None,
        summarized: bool = True,
        apply_write_dedup: bool = False,
    ) -> str:
        """摄入记忆到摘要表（memory_chat_summary）。

        kind 语义（对齐 hindsight tag 策略）：
        - chat_summary：会话对话摘要，participants 存本轮全部发言者
          （"platform:uid" 复合键，用户精确召回过滤用）；
        - bot_self：bot 自触发批次原文，participants 留空（召回按 kind 全局命中）。

        document_id 策略（幂等去重，沿用 hindsight 哈希策略）：
        - chat_summary：{session_id}-{md5(content)[:12]}（每轮独立，不覆盖历史）
        - bot_self：bot-self-{md5(content)[:12]}（相同内容幂等）
        - 其他：{kind}-{session_id}-{md5(content)[:12]}

        Args:
            content: 记忆内容（摘要正文 / bot_self 原文）。
            session_id: 会话 ID。
            user_id: 用户 ID（trigger 发言者，裸 uid 入库）。
            memory_category: 记忆分类（本地表以 kind 列区分用途，此参数仅兼容契约）。
            platform: 平台标识（如 "qq"）。
            group_id: 群 ID（空表示私聊）。
            kind: 记忆类型（chat_summary / bot_self）。
            timestamp: 事件时间（None 用 now；入 occurred_at 列）。
            bot_id: 当前 bot ID（覆盖 self.bot_id，可选）。
            participant_user_ids: 批次所有发言者的 (platform, user_id) 列表
                （写入 participants 列；None/空时回退 trigger 单人）。
            summarized: 编码状态（false=编码降级写入的对话原文，不参与召回，
                由合并 agent 补编码遍重编码；bot_self 原文直写恒 true）。
            apply_write_dedup: 是否执行写入侧近重去重（仅 retain 编码摘要
                路径传 True；工具显式写入/降级原文/bot_self 不参与）。命中
                时跳过插入但仍返回 document_id（与内容哈希幂等跳过同语义）。

        Returns:
            document_id。

        Raises:
            MemoryDBUnavailable: 熔断拒绝期或写入执行失败（retain 调用方
                回滚水位线待重试；工具入口捕获回报失败）。
        """
        occurred_at = timestamp or datetime.now()
        content_hash = hashlib.md5(content.encode()).hexdigest()[:12]
        document_id = self._build_document_id(
            kind=kind, session_id=session_id, content_hash=content_hash
        )
        participants = self._build_participants(
            kind=kind,
            platform=platform,
            user_id=user_id,
            participant_user_ids=participant_user_ids,
        )

        # 熔断明确拒绝期直接失败（peek 非消费式，恢复探测名额留给下方
        # 真实的写入）：旧契约拒绝后仍返回 document_id，retain 批次会被
        # 标已消费而实际未落库（静默丢失）；且 embed_one 在拒绝前执行
        # 会白烧一次向量化调用
        if not await self.circuit_breaker.peek_available():
            raise MemoryDBUnavailable("记忆库熔断拒绝期，写入被拒绝")

        # 写入时向量化（fail-open：失败置 NULL，补算任务回填）
        embedding = await self.embedding_service.embed_one(content)

        # 写入侧近重去重（仅 retain 编码摘要路径）：与同会话最近
        # write_dedup_window 批已编码摘要比对，cosine ≥ 阈值跳过写入——
        # 抑制历史上下文泄漏进摘要导致的同事件重复行（6705/6707 类）。
        # 窗口限制同会话近期，久远/跨会话相似事件不误杀；查询失败
        # fail-open 继续写入（宁重复不丢失）
        if (
            apply_write_dedup
            and summarized
            and kind == "chat_summary"
            and self.config.write_dedup_enabled
            and self.config.write_dedup_threshold > 0
            and embedding is not None
        ):
            try:
                recent = await self.db.fetch_recent_summary_scores(
                    query_vec=embedding,
                    session_id=session_id,
                    limit=self.config.write_dedup_window,
                    platform=platform,
                )
            except Exception:
                logger.warning(
                    "写入侧近重去重查询失败（跳过去重继续写入）", exc_info=True
                )
                recent = []
            best = max((float(r["score"]) for r in recent), default=0.0)
            if best >= self.config.write_dedup_threshold:
                logger.info(
                    "写入侧近重去重: session=%s 跳过摘要写入"
                    "（与最近批次 cosine=%.3f ≥ 阈值 %.2f）",
                    session_id, best, self.config.write_dedup_threshold,
                )
                return document_id

        ok = await self._guarded(
            lambda: self.db.insert_chat_summary(
                document_id=document_id,
                kind=kind,
                platform=platform,
                session_id=session_id,
                group_id=group_id,
                user_id=user_id,
                participants=participants,
                content=content,
                occurred_at=occurred_at,
                embedding=embedding,
                summarized=summarized,
            ),
            description=f"摘要写入 {document_id}",
        )
        if not ok:
            raise MemoryDBUnavailable(f"摘要写入失败: {document_id}")
        return document_id

    async def retain_encoded(
        self,
        conversation_text: str,
        session_id: str,
        user_id: str = "",
        platform: str = "",
        group_id: str = "",
        timestamp: Optional[datetime] = None,
        bot_id: str = "",
        participant_user_ids: Optional[list[tuple[str, str]]] = None,
    ) -> list[str]:
        """摄入原始对话文本（端侧编码后双通道写入）。

        签名与上游 nori 版 retain_encoded 契约保持一致
      （便于经验/数据互通）。

        通道 1（chat_summary 表）：编码产出的保真摘要（仅覆盖本轮批次），
          document_id/participants 策略与单通道 ingest 一致，可召回；
        通道 2（fact 原始表）：每条 EncodedFact 独立写入 memory_persona_fact_raw，
          extracted_flag=0 等待合并 agent（M4）入簇计分，不参与 recall；
          evidence_key={session_id}|{occurred_at:日期} 是后续计分去重键，
          document_id 幂等粒度与之对齐（同会话同日去重、跨会话/跨日复现
          作为独立证据入库计分）。

        编码降级链（graceful degradation）：
        - encoder 为 None（未装配）-> 原文原样走 chat_summary 单通道；
        - encode() 抛异常或输出不可解析 -> 内部 fail-open 返回 (原文, [], False)，
          即单通道行为，不阻断 retain；
        - 降级写入的原文行 summarized=false：不参与 recall（检索过滤），由
          合并 agent 补编码遍在 LLM 恢复后重编码（summary 回写原行 + facts
          入事实表），数据不丢、上下文不污染。

        Args:
            conversation_text: 带时间戳与发言者标识（含 uid）的对话文本，
                含历史上下文/本轮批次分隔标记行（信封组装）。
            其余参数语义同 ingest。

        Returns:
            document_id 列表（summary 一个 + 每条 fact 各一个）。
        """
        if not conversation_text:
            # Guard before the encoder-None branch: an empty input must be a
            # no-op for BOTH paths (the degraded single-channel ingest below
            # would otherwise persist an empty-content summary row).
            return []  # 无编码输入，无内容可写

        if self.encoder is None:
            logger.debug(
                "retain_encoded: encoder 未装配，原文按 chat_summary 单通道写入"
                "（summarized=false，待补编码）"
            )
            return [await self.ingest(
                content=conversation_text,
                session_id=session_id,
                user_id=user_id,
                memory_category=KnowledgeType.EPISODIC,
                platform=platform,
                group_id=group_id,
                kind="chat_summary",
                timestamp=timestamp,
                bot_id=bot_id or self.bot_id,
                participant_user_ids=participant_user_ids,
                summarized=False,
            )]

        # 熔断明确拒绝期：写路径全拒，原文直写是空转且会静默丢批（ingest
        # 旧契约拒绝后仍返回 document_id，批次被标已消费即永久丢失）。
        # 直接失败让调用方（_do_retain）回滚水位线，本批消息留给下一轮
        # 信号重编码（内容不丢）；编码 LLM 与向量化调用均不白烧。
        # peek 非消费式——恢复探测名额由恢复后的真实写入消费
        if not await self.circuit_breaker.peek_available():
            raise MemoryDBUnavailable(
                "记忆库熔断拒绝期：retain 失败（调用方回滚水位线，待下轮重编码）"
            )

        # 端侧编码（fail-open：任何失败返回 (conversation_text, [], False)）；
        # 此处再兜一层，保证 encoder 实现意外抛异常时同样降级为原文单通道
        effective_bot_id = (bot_id or self.bot_id).strip()
        try:
            summary, facts, relations, encoded_ok = await self.encoder.encode(
                conversation_text,
                self.bot_nickname,
                bot_user_id=effective_bot_id,
            )
        except Exception:
            logger.warning(
                "retain_encoded: 编码器异常，降级为原文单通道", exc_info=True
            )
            summary, facts, relations, encoded_ok = conversation_text, [], [], False

        # 确定性防线：bot 自身不得成为画像主体。编码提示词排除项已约束，
        # 但用户发言中 @bot / 提及 bot 名称与号码时 LLM 仍可能把 bot 提取为
        # 事实主体（user_id 填 bot 的平台 ID，形成 bot 画像）——此处按
        # bot_id 硬过滤兜底，发生在向量化之前（被丢弃条目不消耗 embedding）。
        if effective_bot_id and facts:
            kept = []
            dropped = 0
            for f in facts:
                if f.user_id.strip() == effective_bot_id:
                    dropped += 1
                    continue
                # related 集合同步剔除 bot（关系事实参与方含 bot 时，
                # 其复合键会随簇传播进候选检索匹配）
                if f.related_user_ids:
                    cleaned = [
                        r for r in f.related_user_ids
                        if (r or "").strip() != effective_bot_id
                    ]
                    if len(cleaned) != len(f.related_user_ids):
                        f.related_user_ids = cleaned
                kept.append(f)
            if dropped:
                logger.info(
                    "retain_encoded: 丢弃 %d 条 bot 自身事实（user_id=%s）",
                    dropped,
                    effective_bot_id,
                )
            facts = kept

        doc_ids: list[str] = []

        # 通道顺序（facts -> relations -> summary，summary 殿后——失败方向
        # 安全，对齐合并 agent 补编码遍的写入顺序）：summary 落库前任何失败
        # 上抛 → retain_encoded 包装层入 pending 队列，DB 恢复后从零重放，
        # 无半提交残留；若 summary 先行，facts/relations 失败入队后重编码
        # 产出不同摘要文本会落成第二行（document_id 含内容哈希，文本漂移
        # 即重复行）——facts/relations 幂等键则保证重试不重复（同语句
        # ON CONFLICT 跳过/合并）。
        if facts:
            doc_ids.extend(
                await self._ingest_facts(
                    facts=facts,
                    platform=platform,
                    session_id=session_id,
                    group_id=group_id,
                    timestamp=timestamp,
                )
            )

        # 通道 1.5（P2 关系边）：结构化关系三元组零 LLM 合并落边表；
        # 提取开关关闭时丢弃（encoder 未附加扩展节，正常恒空——此处
        # 兜底防旧提示词缓存/手改配置的残余输出）
        if relations and self.config.relation_extract_enabled:
            doc_ids.extend(
                await self._ingest_relations(
                    relations=relations,
                    platform=platform,
                    session_id=session_id,
                    timestamp=timestamp,
                    bot_user_id=effective_bot_id,
                )
            )

        # 通道 2：summary 走 chat_summary（复用 ingest 的幂等/participants 策略）；
        # 编码降级时（encoded_ok=False，content 为原文）标记 summarized=false
        # 不参与召回，由合并 agent 补编码遍重编码；编码摘要启用写入侧近重去重
        doc_ids.append(await self.ingest(
            content=summary or conversation_text,
            session_id=session_id,
            user_id=user_id,
            memory_category=KnowledgeType.EPISODIC,
            platform=platform,
            group_id=group_id,
            kind="chat_summary",
            timestamp=timestamp,
            bot_id=bot_id or self.bot_id,
            participant_user_ids=participant_user_ids,
            summarized=encoded_ok,
            apply_write_dedup=True,
        ))
        return doc_ids

    async def _ingest_facts(
        self,
        facts: list[EncodedFact],
        platform: str,
        session_id: str,
        group_id: str,
        timestamp: Optional[datetime],
    ) -> list[str]:
        """将编码产出的事实批量写入 memory_persona_fact_raw。

        statements 一次性批量向量化（单次 embeddings 请求），随后逐条插入；
        任一条插入失败（含熔断）立即上抛 MemoryDBUnavailable——本方法在
        retain_encoded 中先于 summary 通道执行，调用方回滚水位线后下轮
        从零重编码，无半提交残留；document_id 幂等保证重试不产生重复行
        （同语句 ON CONFLICT 跳过）。旧"跳过该条继续"语义会在熔断期
        静默永久丢失整批事实（批次已被标消费，违反内容不丢契约）。

        Args:
            facts: 编码产出的人物事实列表。
            platform: 平台标识。
            session_id: 提取来源会话 ID（证据键组成部分）。
            group_id: 提取来源群 ID。
            timestamp: 事实发生时间（None 用 now）。

        Returns:
            成功入队的 document_id 列表。

        Raises:
            MemoryDBUnavailable: 任一条插入失败（调用方回滚水位线待重试）。
        """
        occurred_at = self._to_local(timestamp or datetime.now())
        evidence_key = self._build_evidence_key(session_id, occurred_at)

        # 批量向量化（fail-open：失败整批 None，补算任务回填）
        vectors = await self.embedding_service.embed_batch(
            [fact.statement for fact in facts]
        )

        doc_ids: list[str] = []
        for fact, embedding in zip(facts, vectors):
            content_hash = hashlib.md5(fact.statement.encode()).hexdigest()[:12]
            uids = sorted(
                set([fact.user_id] + [r for r in fact.related_user_ids if r])
            )
            document_id = f"{fact_document_id(uids, session_id, occurred_at)}-{content_hash}"
            ok = await self._guarded(
                lambda: self.db.insert_persona_fact_raw(
                    document_id=document_id,
                    platform=platform,
                    user_id=fact.user_id,
                    related_user_ids=fact.related_user_ids,
                    display_name=fact.display_name,
                    category=fact.category,
                    statement=fact.statement,
                    confidence=fact.confidence,
                    session_id=session_id,
                    group_id=group_id,
                    evidence_key=evidence_key,
                    occurred_at=occurred_at,
                    embedding=embedding,
                ),
                description=f"事实写入 {document_id}",
            )
            if not ok:
                # 上抛而非跳过：summary 通道尚未写入（facts 先行），回滚
                # 重试无半提交、无重复；旧语义在熔断期会永久丢失整批事实
                raise MemoryDBUnavailable(f"事实写入失败: {document_id}")
            doc_ids.append(document_id)
        return doc_ids

    async def _ingest_relations(
        self,
        relations: list[EncodedRelation],
        platform: str,
        session_id: str,
        timestamp: Optional[datetime],
        bot_user_id: str = "",
    ) -> list[str]:
        """将编码产出的关系三元组零 LLM 合并写入 memory_entity_edge。

        单条 executemany 批量提交（结构键合并 + evidence_key 去重计数 +
        bot 边 pending→active 内联转写在同语句完成，见 db.upsert_entity_edge）；
        任一失败（含熔断）上抛 MemoryDBUnavailable——本方法在 retain_encoded
        中位于 facts 之后、summary 之前，调用方入 pending 队列后 DB 恢复时
        从零重放，无半提交；evidence_key 幂等保证重试不重复计数。
        document_id 不入库（边表自增主键），仅作返回值/日志可观测。

        Args:
            relations: 编码产出并校验后的关系三元组列表。
            platform: 平台标识。
            session_id: 提取来源会话 ID（证据键组成部分）。
            timestamp: 关系发生时间（None 用 now）。
            bot_user_id: bot 平台 uid（bot 端点边 pending/激活门槛判定）。

        Returns:
            边幂等键列表（可观测用）。

        Raises:
            MemoryDBUnavailable: 批量写入失败（调用方入 pending 队列待重放）。
        """
        occurred_at = self._to_local(timestamp or datetime.now())
        evidence_key = self._build_evidence_key(session_id, occurred_at)
        min_evidence = self.config.relation_bot_edge_min_evidence
        alias = self._alias_store

        def _endpoint_name(uid: str, llm_name: str) -> str:
            # 占位名守卫：LLM 偶发输出"未知"/uid 兜底形，用别名视图的
            # 最新名顶替；未命中留空（upsert 语句保旧名，读侧图谱解析
            # 再兜底显示）
            if not is_placeholder_name(llm_name):
                return llm_name
            return alias.name_for(platform, uid) if alias is not None else ""

        rows: list[dict] = []
        doc_ids: list[str] = []
        for rel in relations:
            doc_ids.append(relation_document_id(rel, session_id, occurred_at))
            rows.append(
                {
                    "platform": platform,
                    "subject_uid": rel.subject_user_id,
                    "object_uid": rel.object_user_id,
                    "subject_name": _endpoint_name(
                        rel.subject_user_id, rel.subject_display_name
                    ),
                    "object_name": _endpoint_name(
                        rel.object_user_id, rel.object_display_name
                    ),
                    "relation_label": rel.label,
                    "statement": rel.statement,
                    "confidence": rel.confidence,
                    "occurred_at": occurred_at,
                    "evidence_key": evidence_key,
                    "is_bot_edge": edge_has_bot_endpoint(
                        rel.subject_user_id, rel.object_user_id, self._bot_uid_forms_all(bot_user_id)
                    ),
                    "min_evidence": min_evidence,
                }
            )
        ok = await self._guarded(
            lambda: self.db.upsert_entity_edge(
                rows, label_stopwords=self.config.relation_label_stopwords
            ),
            description=f"关系边写入 session={session_id} n={len(rows)}",
        )
        if not ok:
            raise MemoryDBUnavailable(f"关系边写入失败: session={session_id}")
        return doc_ids

    # ------------------------------------------------------------------
    #  recall 检索（M3，管线见开发方案 §9.2.2）
    # ------------------------------------------------------------------

    async def search(
        self,
        query: str,
        top_k: int = 5,
        session_id: str = "",
        user_id: str = "",
        platform: str = "",
        bot_id: str = "",
        cross_session: bool = False,
        exclude_kinds: Optional[list[str]] = None,
        scope: str = "session",
        user_ids: Optional[list[str]] = None,
        per_user: bool = False,
        entity_user_keys: Optional[list[str]] = None,
        entity_user_ids: Optional[list[str]] = None,
    ) -> list[MemoryItem]:
        """搜索记忆（向量 + 重排序管线）。

        管线：query 实时向量化（与扩选键/近时排除边界并行）->
        scope 过滤 + cosine top-N 候选（N = max(rerank_candidates, top_k*4)；
        重排序关闭时 N = top_k*4；扩展键命中恒钉死当前会话，近时排除
        剔除宿主窗口内最近 K 批本会话摘要；topic_blacklist 非空时黑名单行
        在 SQL 候选层结构性排除——不占 top_k 名额，注入层另留二次防线）->
        重排序（rerank_enabled 且客户端
        可用；失败退化纯向量序）-> 相关度阈值过滤（作用于 rerank 分数或
        cosine 相似度）-> 时间衰减重排（recall_time_decay_enabled 时
        score × 2^(-age/H)，仅改排序不过滤）-> 近重复去重（最终序贪心，
        与已保留条目 cosine ≥ dedup_similarity_threshold 的丢弃）->
        top_k 截断。

        v0.5.1 设计修订：召回提示旁路（实体词典/引用反查两路候选
        拉取）已整体移除——实体命中只喂画像/P3 节点匹配，引用原文由
        适配层并入 query 走本主路（会话隔离统一由隔离开关管）。

        问及他人召回（entity_user_keys/entity_user_ids，仅 scope=session
        消费）：query 实体命中的 (platform, uid) 键组——跨会话开放
        （cross_session=True）时并入主键组（问及者任何会话的摘要，含其
        与 bot 的私聊，均可被召回）；会话隔离时钉死当前会话（仅实体
        本会话摘要）。隐私口径由调用方的隔离配置决定，落点见
        db.search_chat_summaries 实体键组条目。

        scope 模式：
        - session（默认，框架 MemoryStage/planner 路径）：会话 + bot_self；
          platform+user_id 非空时 AND 追加用户过滤组（对齐 hindsight tag 组合，
          planner 主动召回不传 platform -> 全会话）；
        - user：用户精确召回（participants 数组过滤，天然跨会话）；
        - user_session：user 基础上 AND session_id 收紧。

        多用户（user_ids 列表）与 per_user 配额：
        - per_user=false（默认）：OR 混合单池，单次向量检索，全局 top_k；
        - per_user=true：复用主路完整管线（扩选/近时排除/混合检索/相关度
          阈值/时间衰减/近重复去重/滚动补回行排除——候选池按用户数放大），
          仅截断阶段换配额逻辑：每用户至多 top_k 条（归属判定按行 user_id
          与 participants 复合键，命中多用户取先有配额者），不可归属行
          （bot_self 原文/扩选命中）进公共位（合计至多 top_k）。

        【暂不启用·能力位】per_user 当前无调用方接线（宿主记忆召回
        均走单池路；它不是 config 开关，是本方法的形参级能力）。
        后续按需接线，接线前须知：
        1) 跨平台身份——user_ids 裸 uid 共用单一 platform 前缀，linked
           accounts（同人多适配器）需 (platform, uid) 对形态；
        2) identity 级配额——同一人多账号应合桶共享一份配额，否则
           一人经多账号拿到多份配额；
        3) 候选池占满——单池按相关度排序，池被单用户行占满时配额只能
           池内再分配（已按用户数放大候选缓解，非保底——这是与旧分桶
           实现「每桶 LIMIT 保底」的语义差异，属有意取舍：不再维护
           第二条检索管线）；
        4) scope 语义收紧——旧分桶路对 scope="session" 不收紧会话
           （仅 user_session 传 tighten），单池路 scope 条件全量生效；
           以 session + per_user 接线时行为从「跨会话配额」变为
           「会话内配额」，接线前须确认预期口径。

        Args:
            query: 检索查询文本。
            top_k: 返回最大条数（per_user 模式为每用户配额 + 公共位）。
            session_id: 会话 ID（scope=session/user_session 消费）。
            user_id: 用户 ID（裸 uid；platform 非空时启用用户过滤组）。
            platform: 平台标识。
            bot_id: 当前 bot ID（兼容契约，本地 kind 过滤已覆盖 bot_self）。
            cross_session: 跨会话放宽（仅 scope=session 生效）。
            exclude_kinds: 排除的 kind 列表（兼容契约；本地表无 person_fact，
                事实表结构性不参与 recall，通常无需传入）。
            scope: session | user | user_session。
            user_ids: 多用户召回的裸 uid 列表（None 时回落单个 user_id）。
            per_user: 多用户召回时是否每用户保底配额（暂不启用，见上）。
            entity_user_keys: 问及他人召回的实体命中复合键列表（仅
                scope=session 消费）。
            entity_user_ids: 同上的裸 uid 列表。

        Returns:
            记忆条目列表（最多 top_k 条；per_user 模式每用户至多 top_k 条
            另加公共位至多 top_k 条）。
        """
        if scope not in _VALID_SCOPES:
            raise ValueError(f"不支持的检索 scope: {scope}")
        if not query.strip():
            return []

        # query 向量化是 HTTP 调用（检索延迟大头），扩选键/近时排除边界
        # 在其飞行期间并行完成（一条索引查询 + 一次宿主配置读取）
        embed_task = asyncio.create_task(self.embedding_service.embed_one(query))
        try:
            window_batches = self._derive_window_batches()
            expanded_keys, expanded_uids = await self._derive_expanded_users(
                scope, session_id, platform
            )
        except BaseException:
            # Search aborted: cancel the in-flight vectorization request. An
            # orphaned task whose exception is never retrieved triggers a
            # "never retrieved" warning (cancellation does not), and the
            # useless HTTP request is terminated too. The normal path
            # harvests the result via the await below.
            embed_task.cancel()
            raise
        # query 实时向量化（fail-open：失败返回空，不阻断注入链路）
        query_vec = await embed_task
        if query_vec is None:
            logger.warning("recall 查询向量化失败（embedding 不可用），返回空结果")
            return []

        use_rerank = self.rerank_client is not None and self.config.rerank_enabled
        n_candidates = max(
            self.config.rerank_candidates if use_rerank else 0, top_k * 4
        )

        # 用户参数归一：uid 列表 + 对应复合键。
        # scope=session（框架路径）对齐 hindsight 语义：platform 与 user 均非空才
        # 启用用户过滤组——planner 主动召回不传 platform，应全会话检索；
        # user/user_session 模式以用户过滤为主体，允许裸 uid 回退
        uids = [u for u in (user_ids or ([user_id] if user_id else [])) if u]
        keys = [f"{platform}:{u}" for u in uids] if platform else []
        if scope == "session" and not platform:
            uids = []

        is_per_user = bool(per_user and uids)
        if is_per_user:
            # 候选池按用户数放大：配额截断作用于重排后的单池，池被单一
            # 用户的行占满时配额无法凭空补位（放大缓解，非保底）
            n_candidates = max(n_candidates, top_k * 4 * len(uids))

        # 单池检索（per_user 同路——完整管线过滤/扩选/排除/混合在此生效，
        # 配额只在截断阶段引入；with_participants 供归属判定）
        rows = await self._guarded_call(
            lambda: self.db.search_chat_summaries(
                query_vec=query_vec,
                limit=n_candidates,
                scope=scope,
                session_id=session_id,
                platform=platform,
                cross_session=cross_session,
                user_keys=keys or None,
                user_ids=uids or None,
                exclude_kinds=exclude_kinds,
                exclude_content_keywords=self.config.topic_blacklist or None,
                expanded_user_keys=expanded_keys or None,
                expanded_user_ids=expanded_uids or None,
                entity_user_keys=entity_user_keys if scope == "session" else None,
                entity_user_ids=entity_user_ids if scope == "session" else None,
                exclude_recent_batches=window_batches,
                query_text=query,
                hybrid=self.config.hybrid_search_enabled,
                rrf_k=self.config.hybrid_rrf_k,
                exclude_document_ids=self._rollout_excluded_ids(session_id),
                with_embedding=self.config.dedup_similarity_threshold > 0,
                with_participants=is_per_user,
            ),
            description="recall 检索",
        ) or []

        candidates_n = len(rows)
        if not rows:
            self._log_search(
                session_id=session_id, query=query, scope=scope, top_k=top_k,
                candidates_n=0, exclude_batches=window_batches,
                expansion_n=len(expanded_keys) + len(expanded_uids),
                threshold_dropped=0, dedup_dropped=0, rows=[],
            )
            return []

        # 重排序（失败退化纯向量序；rerank 分数即最终相关度）
        score_is_absolute = False
        if use_rerank:
            try:
                ranked = await self.rerank_client.rerank(
                    query, [r["content"] for r in rows], top_k=len(rows)
                )
                score_by_index = dict(ranked)
                for idx, row in enumerate(rows):
                    # 未返回的候选置 -1 排到队尾（保留候选不静默丢弃）
                    row["score"] = score_by_index.get(idx, -1.0)
                rows.sort(key=lambda r: float(r["score"]), reverse=True)
                score_is_absolute = True
            except Exception:
                logger.warning("重排序失败，退化为纯向量序", exc_info=True)
                rows = self._sort_by_relevance(rows)
        else:
            rows = self._sort_by_relevance(rows)

        # 相关度阈值过滤（rerank 分数或 cosine 相似度，统一作用于 score）。
        # 仅混合检索的降级序（rerank 关闭/失败且行携带 RRF 融合分）跳过：
        # RRF 是 ~0.01-0.03 量级的相对排名分，套用绝对分阈值会全量误杀；
        # 纯向量降级序（cosine）刻度不变，照常过滤
        threshold_dropped = 0
        threshold = self.config.recall_relevance_threshold
        score_is_rrf = (
            not score_is_absolute and bool(rows) and "rrf" in rows[0]
        )
        if score_is_rrf and threshold > 0:
            logger.info(
                "混合降级序（RRF 相对分）跳过相关度阈值 %.2f（刻度不适用）",
                threshold,
            )
            threshold = 0.0
        if threshold > 0:
            kept_threshold = [
                r for r in rows
                if float(r["score"]) >= threshold
            ]
            threshold_dropped = len(rows) - len(kept_threshold)
            rows = kept_threshold

        # 时间衰减（可选）：语义阈值过滤之后、截断之前叠加
        # score × 2^(-age/H) 并重排——只改排序不过滤；occurred_at 缺失
        # 视为未知年龄不降权（系数 1.0）；未来时间戳（时钟偏移）钳为 0
        if self.config.recall_time_decay_enabled and rows:
            half_life = self.config.recall_time_decay_half_life_days
            now = self._now()
            for row in rows:
                ts = row.get("occurred_at")
                if ts is None:
                    continue
                age_days = max((now - ts).total_seconds() / 86400.0, 0.0)
                row["score"] = float(row["score"]) * (2.0 ** (-age_days / half_life))
            rows.sort(key=lambda r: float(r["score"]), reverse=True)

        # 近重复去重（最终序贪心扫描）：与已保留条目 embedding cosine ≥ 阈值
        # 的丢弃——相邻轮次摘要高度重叠，不去重时 top_k 会被同一事件的连续
        # 快照占满。凑满 top_k 即停（后续行反正进不了截断结果，省点积
        # 开销）；per_user 无提前停——配额截断在去重之后，会被配额丢弃的
        # 行同样占用去重扫描位，提前停会把队尾的公共位行（bot_self 等）
        # 错杀在去重阶段。向量缺失的行不参与比对也不会被丢弃（保底可见）。
        # per_user 同样参与去重（完整管线）——跨用户近重复（同一场对话的
        # 双视角摘要）保留其一即可。
        dedup_dropped = 0
        sim_threshold = self.config.dedup_similarity_threshold
        keep_bound = len(rows) if is_per_user else top_k
        if sim_threshold > 0 and rows:
            kept_rows: list[dict] = []
            kept_unit_vecs: list[list[float]] = []
            for row in rows:
                if len(kept_rows) >= keep_bound:
                    break
                vec = parse_vector(row.get("embedding"))
                is_dup = False
                if vec:
                    norm = math.sqrt(sum(x * x for x in vec))
                    if norm > 0:
                        unit = [x / norm for x in vec]
                        for kept_vec in kept_unit_vecs:
                            cosine = sum(a * b for a, b in zip(unit, kept_vec))
                            if cosine >= sim_threshold:
                                is_dup = True
                                break
                    else:
                        unit = None
                else:
                    unit = None
                if is_dup:
                    dedup_dropped += 1
                    continue
                kept_rows.append(row)
                if unit is not None:
                    kept_unit_vecs.append(unit)
            if dedup_dropped:
                logger.info(
                    "近重复去重: %d -> %d 条（cosine 阈值 %.2f）",
                    len(rows), len(kept_rows), sim_threshold,
                )
            rows = kept_rows

        # 截断：per_user 模式按用户配额 + 公共位（重排/去重之后），否则全局 top_k
        if is_per_user:
            rows = self._truncate_per_user_quota(
                rows, uids=uids, platform=platform, top_k=top_k
            )
        else:
            rows = rows[:top_k]

        self._log_search(
            session_id=session_id, query=query, scope=scope, top_k=top_k,
            candidates_n=candidates_n, exclude_batches=window_batches,
            expansion_n=len(expanded_keys) + len(expanded_uids),
            threshold_dropped=threshold_dropped, dedup_dropped=dedup_dropped,
            rows=rows,
        )

        return [
            MemoryItem(
                id=str(r["document_id"]),
                content=r["content"],
                memory_category=KnowledgeType.EPISODIC,
                session_id=r["session_id"],
                user_id=r["user_id"],
                timestamp=r["occurred_at"] or datetime.now(),
                metadata={"kind": r["kind"], "relevance": float(r["relevance"])},
                score=float(r["score"]),
            )
            for r in rows
        ]

    async def entity_hint_entries(
        self, session_id: str, text: str
    ) -> list[tuple[str, str, str]]:
        """实体命中条目 [(名字, platform, uid)]（窗口词典 + 持久别名两层合并）。

        优先级：窗口词典（会话内发言者，最权威、实时）> 持久别名层
        （memory_entity_alias，覆盖久未发言/历史改名成员）。持久层重名
        歧义时窗口命中者优先、无背书则跳过（确定性优先于召回）；合并去重
        按 (platform, uid)，窗口条目优先保留。召回提示路与画像候选路共用
        一次命中结果（注入入口一次匹配、两处消费）；recall_hint_enabled
        关闭或未注入名字源时恒空。
        """
        if not self.config.recall_hint_enabled:
            return []
        window = await self._entity_directory.match(session_id, text)
        entries = list(window)
        window_pairs = {(p, u) for _, p, u in window}
        if self._alias_store is not None:
            try:
                await self._alias_store.refresh_if_due()
                alias_hits, skipped = self._alias_store.match(text, window_pairs)
            except Exception:
                # 持久层失败不拖累窗口路（fail-open）
                logger.warning(
                    "持久别名层匹配失败（本轮仅窗口词典生效）", exc_info=True
                )
                alias_hits, skipped = [], []
            fresh = [e for e in alias_hits if (e[1], e[2]) not in window_pairs]
            entries.extend(fresh)
            if skipped:
                logger.debug(
                    "实体别名歧义跳过 session=%s: %s",
                    session_id, ",".join(sorted(set(skipped))),
                )
        if window or len(entries) > len(window):
            logger.debug(
                "实体命中 session=%s: 窗口=%d 持久=%d | %s",
                session_id, len(window), len(entries) - len(window),
                " ".join(f"{n}[{p}:{u}]" for n, p, u in entries),
            )
        return entries[: _EntityDirectory._MAX_HINT_ENTRIES]

    @property
    def alias_store(self) -> AliasStore | None:
        """持久别名层句柄（批次 upsert 落库后 apply_rows 同步内存用）。"""
        return self._alias_store

    def make_recall_hints(
        self,
        entity_user_keys: Optional[list[str]] = None,
        match_text: str = "",
        bot_addressed: bool = False,
        bot_user_id: str = "",
    ) -> RecallHints:
        """RecallHints 工厂（适配层调用；kira 侧由 main.py inject_memory 组装）。

        v0.5.1 起 hints 只承载实体命中（画像候选/P3 节点匹配）与
        match_text/bot_addressed（P3 边注入信号）——不再驱动召回候选
        拉取（旁路已移除），anchor 形参已删除。bot_user_id 为 bot 平台
        uid（场景 C 双形态匹配的副形态）；非空时顺手学习进形态备忘，
        写侧 is_bot_edge 判定同样受益（平台 uid 形态边正确走 pending/
        双证据门槛）。
        """
        uid = (bot_user_id or "").strip()
        if uid:
            self._bot_uid_forms.add(uid)
        return RecallHints(
            entity_user_keys=list(entity_user_keys or []),
            match_text=match_text or "",
            bot_addressed=bool(bot_addressed),
            bot_user_id=uid,
        )

    def _bot_uid_forms_all(self, explicit: str = "") -> list[str]:
        """bot uid 全形态有序去重：显式值 -> bot_id -> 学习备忘。

        写侧 is_bot_edge 与读侧场景 C 的统一供给（bot_uid_set 语义）。
        """
        forms: list[str] = []
        for uid in (explicit, self.bot_id, *sorted(self._bot_uid_forms)):
            uid = (uid or "").strip()
            if uid and uid not in forms:
                forms.append(uid)
        return forms

    async def build_injection_text(
        self,
        query: str,
        session_id: str,
        top_k: int = 3,
        user_id: str = "",
        platform: str = "",
        bot_id: str = "",
        cross_session: bool = False,
        exclude_person_facts: bool = False,
        scope: str = "session",
        user_ids: Optional[list[str]] = None,
        per_user: bool = False,
        hints: Optional[RecallHints] = None,
    ) -> str:
        """构建注入 LLM 的记忆文本。

        返回格式（对齐 hindsight 版，时间标注为本插件扩展）：
            # 相关长期记忆（仅供背景参考，不要提及记忆来源，不要逐字复述）
            - {memory_text_1}（约2周前）
            - {memory_text_2}（今天）
            ...

        recall_time_label_enabled 开启时每条尾部追加相对时间标注
        （本地时区按日粒度）；关闭时与 hindsight 逐字一致。

        无记忆时返回空字符串。exclude_person_facts 为 hindsight 存量数据
        防御参数：本地事实表结构性不参与 recall，无需等价操作（接受并忽略）。

        问及他人召回：hints.entity_user_keys（适配层按批次文本匹配的
        实体命中）并入主路检索；hints 未携带键时对 query 自匹配一次
        兜底（planner 主动检索不传 hints 的场景）。跨会话开放时实体键
        并入主键组，会话隔离时钉死当前会话（落点见 search 实体键参数）。

        Args:
            query: 检索查询文本。
            session_id: 会话 ID。
            top_k: 返回最大条数。
            user_id: 用户 ID（platform 非空时启用用户过滤组）。
            platform: 平台标识。
            bot_id: 当前 bot ID（兼容契约）。
            cross_session: 是否跨 session 召回（仅 scope=session 生效）。
            exclude_person_facts: 兼容契约（本地结构性满足，忽略）。
            scope/user_ids/per_user: 语义同 search。
            hints: 召回提示（entity_user_keys 消费为问及他人召回键组）。
        """
        # Asking-about-others recall keys: explicit hints win (adapter matched
        # the batch text); otherwise self-match once on the query (in-memory
        # substrings + alias view, negligible cost) to cover planner active
        # recall which passes no hints. Master-switched by recall_hint_enabled;
        # matching failure is fail-open and never blocks the main path.
        entity_keys = list(hints.entity_user_keys) if hints is not None else []
        if (
            not entity_keys
            and self.config.recall_hint_enabled
            and query.strip()
        ):
            try:
                entries = await self.entity_hint_entries(session_id, query)
                entity_keys = [f"{p}:{u}" if p else u for _, p, u in entries]
            except Exception:
                logger.warning("问及他人召回：query 实体匹配失败（忽略）", exc_info=True)
                entity_keys = []
        # Derive bare uids from "platform:uid" composite keys (platform-less
        # bare keys go straight into the uid list)
        entity_uids = [k.rsplit(":", 1)[-1] for k in entity_keys if ":" in k]
        entity_uids += [k for k in entity_keys if ":" not in k]
        items = await self.search(
            query=query,
            top_k=top_k,
            session_id=session_id,
            user_id=user_id,
            platform=platform,
            bot_id=bot_id,
            cross_session=cross_session,
            exclude_kinds=None,
            scope=scope,
            user_ids=user_ids,
            per_user=per_user,
            entity_user_keys=entity_keys or None,
            entity_user_ids=entity_uids or None,
        )

        # P3 边注入（场景 A/C）：关系陈述行小节，独立于 recall 主路——
        # 主路无命中时小节仍可独立产出（fail-open，不影响既有注入）
        relation_section = ""
        if hints is not None and self.config.relation_inject_enabled:
            try:
                relation_section = await self._build_relation_section(
                    hints,
                    session_id=session_id,
                    platform=platform,
                    bot_id=bot_id or self.bot_id,
                )
            except Exception:
                logger.warning("关系边注入失败（本轮跳过边小节）", exc_info=True)
                relation_section = ""

        if not items:
            return relation_section

        # 话题黑名单二次防线：主排除在 search 候选层（SQL content NOT LIKE
        # ALL，黑名单行不占 top_k 名额）；此处兜桩件/词形差异漏网（正常
        # 恒 0 命中），防止单一话题污染 prompt
        blacklist = self.config.topic_blacklist
        blacklist_dropped = 0
        if blacklist:
            filtered = [
                item for item in items
                if not any(kw in item.content for kw in blacklist)
            ]
            blacklist_dropped = len(items) - len(filtered)
            if blacklist_dropped:
                logger.info(
                    "话题黑名单过滤: %d -> %d 条记忆（黑名单: %s）",
                    len(items), len(filtered), blacklist,
                )
            items = filtered

        if not items:
            self._log_inject(
                session_id=session_id, query=query, top_k=top_k,
                blacklist_dropped=blacklist_dropped, injected=[], truncated=False,
            )
            return relation_section

        # token 预算截断：按字符数保守估算（CJK 近似 1 字符=1 token）；
        # 首条允许超预算（避免单条超长时返回全空），其后累加超限即止
        budget = self.config.recall_max_tokens
        # bot 名自指锚定：记忆以第三人称记录 bot 言行（query 端亦恒为第三人称，
        # 库内保留 bot 名原文保字面检索命中），注入时显式告知 LLM 该名即自己，
        # 防把记忆中 bot 的言行当成第三方成员的事
        identity_note = (
            f"；文中“{self.bot_nickname}”即你自己" if self.bot_nickname else ""
        )
        lines = [
            "# 相关长期记忆（仅供背景参考，不要提及记忆来源，不要逐字复述"
            f"{identity_note}）"
        ]
        used = 0
        appended = 0
        injected: list[dict] = []
        previews: list[str] = []
        truncated = False
        # 相对时间标注（本地时区）：让 LLM 可区分"昨天"与"三个月前"，
        # 避免把久远记忆当作近况使用
        label_enabled = self.config.recall_time_label_enabled
        for item in items:
            content = item.content.strip()
            if not content:
                continue
            cost = len(content)
            if appended > 0 and used + cost > budget:
                logger.info(
                    "token 预算截断: 保留 %d 条（预算 %d）", appended, budget
                )
                truncated = True
                break
            label = self._relative_time_label(item.timestamp) if label_enabled else ""
            lines.append(f"- {content}（{label}）" if label else f"- {content}")
            injected.append({"id": item.id, "label": label})
            # debug 注入明细：id + 每条截断 100 字
            previews.append(f"{item.id}:{content[:100]}")
            used += cost
            appended += 1
        if previews:
            logger.debug(
                "recall 注入 session=%s: %d 条 | %s",
                session_id, len(previews), " ; ".join(previews),
            )
        self._log_inject(
            session_id=session_id, query=query, top_k=top_k,
            blacklist_dropped=blacklist_dropped, injected=injected,
            truncated=truncated,
        )
        text = "\n".join(lines)
        if relation_section:
            text = f"{text}\n\n{relation_section}"
        return text

    async def _build_relation_section(
        self,
        hints: RecallHints,
        *,
        session_id: str,
        platform: str,
        bot_id: str = "",
    ) -> str:
        """P3 边注入：场景 A/C 关系陈述行 + 邻居画像（独立预算）。

        匹配文本 = hints.match_text（两侧适配层均为实体词典同源文本，含
        @昵称 渲染）；label 词形命中 = label in text（与名字匹配同路数，
        确定性子串）。节点匹配为 (platform, uid) 复合键——实体命中键本就
        带平台前缀，多适配器数字 uid 撞号时不会把另一平台的人的边/画像
        错配进来（与画像主路同口径）。画像仅补注「未命中节点的对端」
        （命中者已走实体候选路），bot 端点无画像；persona_service 未装配
        时只出陈述行。
        """
        text = (hints.match_text or "").strip()
        if not text:
            return ""
        # Composite node keys ("platform:uid") — kept whole for matching.
        node_keys = [k for k in hints.entity_user_keys if k]
        # 场景 C 双形态匹配：bot 端点 uid 在边表有会话标识/平台 uid 两种
        # 形态（bot_uid_set 语义），复合键 = 形态 × 当前会话平台；
        # 门槛 = AT/引用 bot 的轮次才取 bot 端点边
        bot_uids = self._bot_uid_forms_all(
            hints.bot_user_id or (bot_id or self.bot_id)
        )
        session_platform = platform or ""
        bot_lookup = (
            [f"{session_platform}:{u}" for u in bot_uids]
            if hints.bot_addressed
            else []
        )
        if not node_keys and not bot_lookup:
            return ""
        edges = await self._guarded_call(
            lambda: self.db.fetch_active_edges(node_keys, bot_lookup),
            description="关系边检索",
        ) or []
        if not edges:
            return ""
        cfg = self.config
        selected = select_relation_edges(
            text=text,
            edges=edges,
            node_keys=node_keys,
            bot_uid=bot_uids,
            bot_addressed=hints.bot_addressed,
            stopwords=cfg.relation_label_stopwords,
            max_neighbors=cfg.relation_inject_max_neighbors,
            max_lines=cfg.relation_inject_max_lines,
        )
        if not selected:
            return ""
        # 端点名规范解析：边表冗余名是"最后一次证据时"的名字快照，会过期
        # （存量审计：57/65 active 边端点名 ≠ 别名表最新规范名）。
        # 别名表最新非占位名优先（按选中边端点过滤，(platform, uid) 索引
        # 命中而非全表扫），边名兜底；bot 端点不入别名表，由
        # relation_statement_line 内部回退 bot 昵称。
        owners: set[tuple[str, str]] = set()
        for edge in selected:
            eplat = str(edge.get("platform") or "")
            for side in ("subject", "object"):
                uid = str(edge.get(f"{side}_uid") or "")
                if uid:
                    owners.add((eplat, uid))
        canonical_names = await self._guarded_call(
            lambda: self.db.fetch_alias_names_by_owner(sorted(owners)),
            description="关系边端点规范名解析",
        ) or {}
        for edge in selected:
            eplat = str(edge.get("platform") or "")
            for _side in ("subject", "object"):
                _resolved = canonical_names.get(
                    (eplat, str(edge.get(f"{_side}_uid") or ""))
                )
                if _resolved:
                    edge[f"{_side}_name"] = _resolved
        lines = [RELATION_HEADER]
        for edge in selected:
            lines.append(
                "- "
                + relation_statement_line(
                    edge,
                    bot_uid=bot_uids,
                    bot_nickname=self.bot_nickname,
                    time_qualifier=self._edge_time_qualifier(edge.get("last_seen")),
                )
            )
        section_text = "\n".join(lines)
        if self.persona_service is not None:
            neighbors = neighbor_profile_uids(
                selected,
                node_keys=node_keys,
                bot_uid=bot_uids,
                max_profiles=cfg.relation_inject_max_profiles,
            )
            if neighbors:
                # Neighbor platform comes from each edge (cross-platform alias
                # hits resolve to the edge's own platform, not the session's).
                candidates = [
                    PersonaCandidate(
                        user_id=uid,
                        platform=plat or session_platform,
                        display_name=name or uid,
                        source="relation_edge",
                        nickname=name,
                    )
                    for plat, uid, name in neighbors
                ]
                try:
                    profile_block = (
                        await self.persona_service.build_multi_profile_text(
                            candidates=candidates, session_id=session_id
                        )
                    )
                except Exception:
                    logger.warning("关系边邻居画像拼装失败（跳过画像）", exc_info=True)
                    profile_block = ""
                if profile_block:
                    section_text = f"{section_text}\n\n{profile_block}"
        logger.debug(
            "图谱命中 session=%s: %s",
            session_id,
            " ".join(
                "%s:%s->%s(%s)"
                % (
                    e.get("_scenario"),
                    e.get("relation_label"),
                    e.get("subject_uid") if e.get("_scenario") == "C"
                    else e.get("_anchor"),
                    "bot" if e.get("_scenario") == "C" else "node",
                )
                for e in selected
            ),
        )
        return section_text

    def _edge_time_qualifier(self, ts: Optional[datetime]) -> str:
        """边时间限定：截至M月D日（last_seen 本地时区）。

        label 存在多值语义（决策 8：不做自动互斥），注入带时间限定让
        LLM 可自行裁决新旧；naive 时间戳按本地时区补齐。
        """
        if ts is None:
            return ""
        if ts.tzinfo is None:
            ts = ts.astimezone()
        try:
            local = ts.astimezone(self._local_tz())
        except (ValueError, OSError, OverflowError):
            return ""
        return f"截至{local.month}月{local.day}日"

    # ------------------------------------------------------------------
    #  内部工具
    # ------------------------------------------------------------------

    @staticmethod
    def _now() -> datetime:
        """当前 UTC 时间（衰减计算用；独立方法便于测试固定时钟）。"""
        return datetime.now(timezone.utc)

    def _derive_window_batches(self) -> int:
        """近时排除/滚动补回的窗口锚定：宿主 LLM 可见历史窗口的块数。

        KiraAI 宿主窗口按「块」截断（session memory 每 chunk = 1 轮，仅
        保留最近 max_memory_length 块；窗口内容是 OpenAIMessage 字典，
        不带时间戳，无法按时间锚定边界）。retain 每轮恰好产出一批摘要
        （回合完成信号触发一次编码），故「最近 K 批已编码摘要」即
        「窗口内已可见内容」的等价物。经 provider 活读宿主配置（热改
        即时生效）；失败/未装配返回 0（该轮关闭近时排除与滚动补回）。

        Returns:
            窗口块数（0 = 关闭）。
        """
        if not self.config.recall_exclude_history_window:
            return 0
        if self._history_window_provider is None:
            return 0
        try:
            value = int(self._history_window_provider() or 0)
        except Exception:
            logger.warning("宿主历史窗口块数读取失败（跳过近时排除）", exc_info=True)
            return 0
        if value <= 0 and not self._window_zero_warned:
            # 宿主截断 memory[-0:] 等于全列表——max_memory_length=0 实为
            # 无限窗口（全部历史可见），而插件按 0=锚定不可得关闭了近时
            # 排除与滚动补回，当前会话近期摘要可能与窗口内容成对注入。
            # 根治在宿主（对 0 取 max(...,1) 或启动期校验拒绝）
            self._window_zero_warned = True
            logger.warning(
                "宿主 bot.max_memory_length=%s：窗口锚定为 0，近时排除/滚动"
                "补回已关闭（宿主 [-0:] 截断语义=无限窗口，近期摘要可能与"
                "窗口内容重复注入；请将宿主配置设为 >=1）",
                value,
            )
        return max(value, 0)

    async def _derive_expanded_users(
        self, scope: str, session_id: str, platform: str = ""
    ) -> tuple[list[str], list[str]]:
        """召回扩选：取会话最近 N 批摘要的参与者作为扩展键组。

        扩展键命中在 SQL 侧恒钉死当前会话（见 search_chat_summaries），
        与 cross_session 开关无关——"问及未在场成员"可命中其参与过的
        同会话摘要，但不会带出该成员任何跨会话（含私聊）记忆。
        bot 自身剔除（bot 参与过几乎所有批次，混入等于对本会话取消
        用户过滤）。仅 scope=session 生效（user/user_session 本就无
        会话隔离诉求）。
        """
        if not (
            self.config.recall_expansion_enabled
            and scope == "session"
            and session_id
        ):
            return [], []
        rows = await self._guarded_call(
            lambda: self.db.fetch_recent_session_participants(
                session_id=session_id,
                limit=self.config.recall_expansion_recent_batches,
                platform=platform,
            ),
            description="召回扩选参与者查询",
        )
        if not rows:
            return [], []
        bot_id = self.bot_id.strip()
        keys: list[str] = []
        uids: list[str] = []
        for r in rows:
            for key in (r.get("participants") or []):
                k = key.strip() if isinstance(key, str) else ""
                if not k or k in keys:
                    continue
                if bot_id and k.endswith(f":{bot_id}"):
                    continue
                keys.append(k)
            uid = (r.get("user_id") or "").strip()
            if uid and uid not in uids and uid != bot_id:
                uids.append(uid)
        return keys, uids

    # 滚动补回去重 memo 的 TTL：覆盖单轮 pipeline 时长（MemoryStage 装配
    # → planner 主动检索 → replyer 注入），并留余量给超长 agent 委派轮
    # （委派期间不产生新轮刷新 memo，过短会双注入）
    _ROLLOUT_MEMO_TTL_SECONDS = 1800.0
    # memo 容量上限（超出时清理过期项；防长期运行慢累积）
    _ROLLOUT_MEMO_MAX_SESSIONS = 64

    def _rollout_excluded_ids(self, session_id: str) -> list[str] | None:
        """取本会话已注入滚动补回块的 document_id（TTL 内；否则 None）。

        Args:
            session_id: 会话 ID。

        Returns:
            待排除的 document_id 列表；无有效 memo 时 None（SQL 不加条件）。
        """
        if not session_id:
            return None
        entry = self._rollout_memo.get(session_id)
        if not entry:
            return None
        ids, at = entry
        if time.monotonic() - at > self._ROLLOUT_MEMO_TTL_SECONDS:
            self._rollout_memo.pop(session_id, None)
            return None
        return ids or None

    async def build_recent_rollout_text(
        self, session_id: str, platform: str = ""
    ) -> str:
        """构建"窗口外最近批次摘要"注入块（滚动出 history 窗口的会话补回）。

        背景：记忆来源只有上下文窗口 + recall 摘要 + 画像，滚动出轮数的
        会话全靠 recall 补回——但 recall 是 query 驱动的语义检索，刚滚出
        窗口的近期上下文若无人提起就永久不可见。本方法跳过最近 K 批
        （K = 宿主窗口块数，与 recall 近时排除同源）直取更早的 N 批，供
        llm_request 注入为独立 Prompt（时间线置于记忆召回块之前）。

        去重契约：成功注入后 memoize 这些 document_id（会话级 + TTL），
        同轮的 recall / planner 主动检索经 search() 的行排除跳过这些行，
        避免"注入块 + recall 块"双份呈现。

        Args:
            session_id: 会话 ID（裸 id；跨适配器同号会话靠 platform 区分）。
            platform: 平台标识（非空时限定本适配器——OFFSET 名额与近时
                排除子查询同口径）。

        Returns:
            注入块文本（无可用内容/开关关闭/窗口边界不可得时返回空串）。
        """
        if not (self.config.recent_rollout_enabled and session_id):
            return ""
        skip_batches = self._derive_window_batches()
        if skip_batches <= 0:
            return ""
        rows = await self._guarded_call(
            lambda: self.db.fetch_recent_rollout_summaries(
                session_id=session_id,
                skip_batches=skip_batches,
                limit=self.config.recent_rollout_batches,
                platform=platform,
            ),
            description="滚动补回摘要查询",
        )
        if not rows:
            return ""

        # 预算挑选新→旧（rows 已按 occurred_at DESC）：最新批次是紧邻
        # history 窗口的桥接批，超预算时丢弃更旧批次；单批超预算时截断
        # 该批（避免空块）。呈现顺序旧→新（衔接 history 时间线）
        budget = self.config.recent_rollout_max_chars
        label_enabled = self.config.recall_time_label_enabled
        kept_rows: list[dict] = []
        used = 0
        for row in rows:
            content = (row.get("content") or "").strip()
            if not content:
                continue
            if used + len(content) > budget:
                if kept_rows:
                    break  # 已有批次：丢弃更旧批次
                row = {**row, "content": content[:budget] + "…"}  # 单批超预算截断
                content = row["content"]
            kept_rows.append(row)
            used += len(content)

        lines = [
            "# 更早对话摘要（已滚动出当前可见历史，仅供背景参考，"
            "不要提及记忆来源，不要逐字复述）"
        ]
        injected_ids: list[str] = []
        for row in reversed(kept_rows):
            content = (row.get("content") or "").strip()
            label = (
                self._relative_time_label(row.get("occurred_at"))
                if label_enabled
                else ""
            )
            lines.append(f"- {content}（{label}）" if label else f"- {content}")
            injected_ids.append(row["document_id"])
        if len(lines) == 1:
            return ""

        self._rollout_memo[session_id] = (injected_ids, time.monotonic())
        if len(self._rollout_memo) > self._ROLLOUT_MEMO_MAX_SESSIONS:
            now_m = time.monotonic()
            self._rollout_memo = {
                k: v
                for k, v in self._rollout_memo.items()
                if now_m - v[1] <= self._ROLLOUT_MEMO_TTL_SECONDS
            }
        logger.debug(
            "滚动补回: session=%s | 注入 %d 批（跳过窗口内 %d 批）",
            session_id, len(injected_ids), skip_batches,
        )
        return "\n".join(lines)

    def _local_tz(self):
        """本地时区（插件配置 timezone > 宿主 locale.TZ > 服务器本地）。"""
        if self._local_tz_cache is None:
            name = (self.config.timezone or "").strip()
            if name:
                try:
                    self._local_tz_cache = ZoneInfo(name)
                except Exception:
                    logger.warning(
                        "timezone 配置无法解析（%s），回退宿主/服务器本地时区", name
                    )
                    self._local_tz_cache = self._host_tz_or_local()
            else:
                self._local_tz_cache = self._host_tz_or_local()
        return self._local_tz_cache

    def reset_local_tz_cache(self) -> None:
        """Invalidate the cached local timezone (maintenance-page save path).

        The cache is resolved once and never expires; after a timezone config
        change the relative-time labels and evidence-key date derivations must
        pick up the new zone on the next call instead of after a restart.
        """
        self._local_tz_cache = None

    def _host_tz_or_local(self):
        """宿主 locale.TZ（provider 活读；不可用回退服务器本地时区）。"""
        if self._host_tz_provider is not None:
            try:
                tz = self._host_tz_provider()
            except Exception:
                logger.warning("宿主时区读取失败，回退服务器本地时区", exc_info=True)
                tz = None
            if tz is not None:
                return tz
        return datetime.now().astimezone().tzinfo

    def _to_local(self, dt: datetime) -> datetime:
        """幂等键日期口径归一：aware 入参转本地时区，naive 视为已是本地口径。

        evidence_key / fact_document_id / relation_document_id 取日期必须
        用本地口径（宿主传 naive 本地时间戳时原样透传）。任何通路传 aware
        （UTC）值时直接 strftime 会与 merge agent 补编码路径
        （merge_agent._to_local 归一）产生跨路径幂等键失配——UTC+8 的
        00:00-07:59 差一天，同批事实重复入库计分。astimezone 只换表示
        不变时刻，转出的值随后写库无副作用。
        """
        if dt.tzinfo is None:
            return dt
        return dt.astimezone(self._local_tz())

    def _relative_time_label(self, ts: datetime | None) -> str:
        """相对时间标注（本地时区按日粒度）：今天/昨天/N天前/约N周前/…。

        Args:
            ts: 记忆发生时间（aware；None 返回空串）。

        Returns:
            标注文本（空串表示不加标注）。
        """
        if ts is None:
            return ""
        try:
            local = ts.astimezone(self._local_tz())
            now_local = self._now().astimezone(self._local_tz())
        except (ValueError, OSError, OverflowError):
            return ""
        days = (now_local.date() - local.date()).days
        if days <= 0:
            return "今天"
        if days == 1:
            return "昨天"
        if days < 7:
            return f"{days}天前"
        if days < 30:
            return f"约{max(days // 7, 1)}周前"
        if days < 365:
            return f"约{max(days // 30, 1)}个月前"
        return f"约{max(days // 365, 1)}年前"

    def _log_search(
        self,
        *,
        session_id: str,
        query: str,
        scope: str,
        top_k: int,
        candidates_n: int,
        exclude_batches: int,
        expansion_n: int,
        threshold_dropped: int,
        dedup_dropped: int,
        rows: list[dict],
    ) -> None:
        """落 search 事件（未启用日志时为空操作）。"""
        if self._recall_log is None:
            return
        self._recall_log.write({
            "event": "search",
            "session_id": session_id,
            "query": query,
            "scope": scope,
            "top_k": top_k,
            "candidates": candidates_n,
            "exclude_batches": exclude_batches,
            "expansion_keys": expansion_n,
            "threshold_dropped": threshold_dropped,
            "dedup_dropped": dedup_dropped,
            "time_decay": self.config.recall_time_decay_enabled,
            "kept": [
                {
                    "id": r.get("document_id"),
                    "kind": r.get("kind"),
                    "score": round(float(r.get("score", 0.0)), 6),
                    "occurred_at": r.get("occurred_at"),
                }
                for r in rows
            ],
        })

    def _log_inject(
        self,
        *,
        session_id: str,
        query: str,
        top_k: int,
        blacklist_dropped: int,
        injected: list[dict],
        truncated: bool,
    ) -> None:
        """落 inject 事件（未启用日志时为空操作）。"""
        if self._recall_log is None:
            return
        self._recall_log.write({
            "event": "inject",
            "session_id": session_id,
            "query": query,
            "top_k": top_k,
            "blacklist_dropped": blacklist_dropped,
            "injected": injected,
            "budget_truncated": truncated,
        })

    @staticmethod
    def _truncate_per_user_quota(
        rows: list[dict],
        *,
        uids: list[str],
        platform: str,
        top_k: int,
    ) -> list[dict]:
        """per_user 配额截断（纯函数，供单测直调；rows 须已按最终序排好）。

        归属判定与主路用户过滤组同口径：行 user_id == uid，或
        "platform:uid" ∈ 行 participants；命中多用户时取先有配额者
        （顺序即 uids 序）。不可归属行（bot_self 原文 / 扩选命中的
        非列名用户行）进公共位——bot_self 在旧分桶实现中随每桶计入
        某个用户的配额，独立成公共位语义更干净（bot 自身记忆属于
        全场，不占任何人的名额）。

        【暂不启用】per_user 无调用方接线（见 search docstring 的
        接线前须知：跨平台身份对 / identity 级合桶 / 候选池占满 /
        scope 语义收紧）。
        """
        quota = {u: top_k for u in uids}
        shared_left = top_k
        kept: list[dict] = []
        for row in rows:
            participants = row.get("participants") or []
            matched = [
                u for u in uids
                if (row.get("user_id") or "") == u
                or f"{platform}:{u}" in participants
            ]
            if matched:
                # 可归属行：归属者中取先有配额者；全部配额已满则丢弃
                # （不占公共位——公共位是给无归属行的，不是溢出区）
                owner = next(
                    (u for u in matched if quota.get(u, 0) > 0), None
                )
                if owner is not None:
                    quota[owner] -= 1
                    kept.append(row)
            elif shared_left > 0:
                shared_left -= 1
                kept.append(row)
        return kept

    @staticmethod
    def _sort_by_relevance(rows: list[dict]) -> list[dict]:
        """纯向量序：按 cosine 相关度降序显式排序并回填 score。

        SQL 已按 cosine 排序返回，此处再排一次消除对返回序的隐式依赖
        （分桶去重/桩件等中转可能打乱顺序）。混合检索开启时行携带
        "rrf"（两路融合分），降级序以融合分为准（否则 BM25 路捞回的
        稀有条目会被 cosine 序埋没）。

        Args:
            rows: 候选行列表（含 relevance 字段，hybrid 时含 rrf）。

        Returns:
            排序后的行列表（score 字段已回填）。
        """
        for row in rows:
            row["score"] = row.get("rrf", row["relevance"])
        rows.sort(key=lambda r: float(r["score"]), reverse=True)
        return rows

    async def _guarded_call(
        self, operation: Callable[[], Awaitable[_T]], description: str
    ) -> _T | None:
        """熔断守卫的 DB 读取（fail-open：任何失败记日志返回 None，不上抛）。

        Args:
            operation: 无参协程工厂（DB 读取操作，返回结果）。
            description: 日志描述。

        Returns:
            操作结果；熔断跳过或执行失败时返回 None。
        """
        if not await self.circuit_breaker.is_available():
            logger.warning("记忆库熔断中，跳过: %s", description)
            return None
        try:
            result = await operation()
        except Exception:
            await self.circuit_breaker.record_failure()
            logger.warning("记忆库读取失败（fail-open 返回空）: %s", description, exc_info=True)
            return None
        await self.circuit_breaker.record_success()
        return result

    async def _guarded(self, operation: Callable[[], Awaitable[None]], description: str) -> bool:
        """熔断守卫的 DB 操作执行（fail-open：任何失败记日志跳过，不上抛）。

        Args:
            operation: 无参协程工厂（DB 写入操作）。
            description: 日志描述（含 document_id）。

        Returns:
            True 表示执行成功；False 表示熔断跳过或执行失败。
        """
        if not await self.circuit_breaker.is_available():
            logger.warning("记忆库熔断中，跳过: %s", description)
            return False
        try:
            await operation()
        except Exception:
            await self.circuit_breaker.record_failure()
            logger.warning("记忆库操作失败（fail-open 跳过）: %s", description, exc_info=True)
            return False
        await self.circuit_breaker.record_success()
        return True

    @staticmethod
    def _build_participants(
        kind: str,
        platform: str,
        user_id: str,
        participant_user_ids: Optional[list[tuple[str, str]]],
    ) -> list[str]:
        """构造 participants 复合键列表（"platform:uid"，去重保序）。

        Args:
            kind: 记忆类型（bot_self 留空——召回按 kind 全局命中，无需用户过滤）。
            platform: 平台标识。
            user_id: trigger 发言者（participant 列表为空时的回退）。
            participant_user_ids: 批次所有发言者 (platform, user_id) 列表。

        Returns:
            复合键列表；bot_self 或无任何发言者时为空列表。
        """
        if kind == "bot_self":
            return []

        seen: set[str] = set()
        participants: list[str] = []
        pairs = participant_user_ids or ([(platform, user_id)] if user_id else [])
        for p, uid in pairs:
            if not p or not uid:
                continue
            key = f"{p}:{uid}"
            if key not in seen:
                seen.add(key)
                participants.append(key)
        return participants

    @staticmethod
    def _build_document_id(kind: str, session_id: str, content_hash: str) -> str:
        """构造摘要表 document_id（幂等键，策略对齐 hindsight 插件）。

        Args:
            kind: 记忆类型（chat_summary / bot_self / 其他）。
            session_id: 会话 ID。
            content_hash: 内容 MD5 哈希（前 12 位）。

        Returns:
            document_id 字符串。
        """
        if kind == "chat_summary":
            # session_id 已含 bot_id（如 group-{gid}-{bot_id}），无需重复
            return f"{session_id}-{content_hash}"
        if kind == "bot_self":
            return f"bot-self-{content_hash}"
        return f"{kind}-{session_id}-{content_hash}"

    @staticmethod
    def _build_evidence_key(session_id: str, occurred_at: datetime) -> str:
        """构造计分证据键 "{session_id}|{occurred_at:日期}"。

        同一会话同一天的重复提取（历史窗口重叠）天然落在同一键上，
        被 M4 合并 agent 的 evidence_keys 去重消除。

        Args:
            session_id: 提取来源会话 ID。
            occurred_at: 事实发生时间。

        Returns:
            证据键字符串。
        """
        return f"{session_id}|{occurred_at.strftime('%Y-%m-%d')}"

    async def forget_summary(
        self,
        document_id: str,
        *,
        scope_session_id: str = "",
        scope_user_id: str = "",
    ) -> bool:
        """按幂等键删除摘要记忆（memory_remove 维护工具的内核入口）。

        Args:
            document_id: 摘要幂等键。
            scope_session_id: 作用域锁定非空时追加会话归属限定（下传 DAL）。
            scope_user_id: 作用域锁定非空时追加用户归属限定（下传 DAL）。

        Returns:
            是否删除了行；熔断中或执行失败返回 False。
        """
        return bool(await self._guarded_call(
            lambda: self.db.delete_chat_summary(
                document_id,
                scope_session_id=scope_session_id,
                scope_user_id=scope_user_id,
            ),
            description=f"删除摘要 {document_id}",
        ))
