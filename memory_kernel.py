"""LocalMemoryKernel: local memory kernel based on PostgreSQL + pgvector.

Write path (M2):
- ingest: summary/bot_self raw text written to memory_chat_summary (document_id
  idempotent; embedding computed at write time, set to NULL on failure pending
  backfill);
- retain_encoded: on-endpoint encoded multi-channel - facts batch-written to
  memory_persona_fact_raw (extracted_flag=0, carrying evidence_key scoring
  dedup key, awaiting M4 merge-agent consumption), relations as structured
  triples landed zero-LLM-merge into memory_entity_edge (P2 channel), summary
  written last through the same ingest path (failure-direction safe, see the
  retain_encoded docstring).

Recall path (M3, see dev plan section 9.2):
- search: query vectorized in real time -> scope filter + cosine top-N
  candidates -> RerankClient rerank (configurable off; on failure degrades to
  plain vector order) -> relevance threshold -> top_k truncation (per_user
  mode reuses the same pipeline, only the truncation stage swaps to per-user
  quota + shared slots; the capability slot is not wired yet, see the search
  docstring);
- scope modes: session (default, semantically aligned with the hindsight tag
  combination) / user (user-exact, participants array filter naturally
  cross-session) / user_session (user + session tightened);
- build_injection_text: topic blacklist second line of defense + token budget
  truncation + injection format verbatim-aligned with the hindsight version
  (the main exclusion is done structurally in SQL at the search candidate
  layer);
- persona_fact invisibility to recall is guaranteed structurally (fact/cluster
  tables never participate in retrieval).

DB failure semantics: recall reads are guarded by the circuit breaker, on
failure they log a warning and return empty (fail-open, does not block the
injection path, aligned with hindsight backend behavior); the retain write
path is the opposite - during circuit-open rejection or write execution
failure it raises MemoryDBUnavailable, caught by the retain_encoded wrapper
into a pending retry queue (the host has no retry mechanism; the in-plugin
queue is the equivalent way to keep content loss-free); the merge agent
(merge_agent) periodic task separately peeks the circuit state at cycle entry
(skips the whole cycle during the rejection period, no burned LLM budget).
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

from . import time_labels
from .alias_store import (
    AliasStore,
    build_fact_code_alias_rows,
    is_placeholder_name,
    normalize_alias_text,
)
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
    """Memory database unavailable (during a circuit-open rejection period or
    a write execution failure).

    The retain path raises this: the caller (_do_retain) rolls back the
    watermark and leaves this batch of messages to be re-encoded on the next
    signal round (content is not lost); tool paths such as memory_write are
    caught at the entry point, which reports the write failure to the LLM.
    It replaces the old fail-open contract (which still returned
    document_id after a rejection) - that let a batch be marked consumed
    while never actually being persisted, a silent loss.
    """

# scope 合法值
_VALID_SCOPES = frozenset({"session", "user", "user_session"})


@dataclass
class RecallHints:
    """Transport carrier for entity hits / P3 relation injection (no longer
    drives recall candidate fetching).

    v0.5.1 design revision: the recall-hint bypass (entity-directory route +
    reference reverse-lookup route) was removed entirely - the entity route's
    always-cross-session fetching conflicted with the session-isolation
    baseline (summary_recall_session_scoped), and the reference route's
    time-window reverse lookup was replaced by "merge the referenced raw text
    into the recall query and go through the main route" (the adapter appends
    the referenced message text to the query; semantic recall is naturally
    governed by the session-isolation switch). hints only carry:

    - entity_user_keys: entity-hit composite keys ("platform:uid") - the
      input for persona-candidate and P3 relation-injection node matching;
    - match_text: same-source matching text from the entity directory
      (including @nickname rendering) - the label word-form hit surface for
      P3 edge injection (same text used for name matching, assembled by the
      adapter);
    - bot_addressed: whether this round's input ATs the bot or references a
      bot message (hard gate for scenario C, decided by the adapter; always
      True in private chats - the whole conversation is addressed to the
      bot. When False, bot-endpoint edges do not participate in injection).
    - bot_user_id: bot platform uid (e.g. a QQ number), resolved by the
      adapter from the platform adapter. Together with the host-injected
      bot_id (session identifier) these are the two uid forms of the same
      bot - both forms exist for bot endpoints in the edge table (the main
      form in extraction prompts = bot_id, the secondary form leaked from
      the @-segment dictionary = platform uid), and injection matches by
      union of the sets (bot_uid_set).
    """

    entity_user_keys: list[str] = field(default_factory=list)
    match_text: str = ""
    bot_addressed: bool = False
    bot_user_id: str = ""


class _EntityDirectory:
    """Session-level entity directory (name -> list of participant
    (platform, uid)), lazy-built with TTL + LRU.

    The name source is injected by the adapter (directory_source callback):
    the KiraAI side reads the plugin's rolling row cache (sender
    nicknames / group cards the host has already observed) - all reliable
    names, never scraped from summary text (mis-extraction risk deferred to
    v2). TTL expiry triggers a self-healing rebuild; name changes / account
    merges are invisible within the TTL window, an acceptable latency for a
    bypass (the directory is only a candidate source; false hits are caught
    by rerank and the persona blank-slot fallback).
    """

    _TTL_SECONDS = 600.0
    _MAX_SESSIONS = 256
    _MIN_NAME_LEN = 2  # 单字名（"阿"/"好"）误命中率过高，不入词典
    _MAX_HINT_ENTRIES = 4  # 单轮命中条目上限（防重名/常用词昵称候选风暴）

    def __init__(self, source: Optional[Callable[[str], Awaitable[list[tuple[str, str, str]]]]]):
        self._source = source
        self._cache: OrderedDict[str, tuple[float, dict[str, dict[tuple[str, str], str]]]] = OrderedDict()

    async def match(self, session_id: str, text: str) -> list[tuple[str, str, str]]:
        """Return text-hit entries [(name, platform, uid)] (deduped,
        order-preserving, including multi-key for duplicate names).

        Structured return: the recall-hint route projects it to composite
        keys, and the persona-candidate route consumes it directly (the hit
        name is the display_name, guaranteeing persona titles match chat
        addressing).
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
            # 键 -> {pair: 该 pair 自己的观测原名}——同归一化键多 uid 时
            # 命中条目各带各的称呼，不串贴他人原名（与 AliasStore.
            # _display_by_pair 同款口径）
            names: dict[str, dict[tuple[str, str], str]] = {}
            for raw_name, platform, uid in pairs or []:
                name = (raw_name or "").strip()
                key = normalize_alias_text(name)
                if len(key) < self._MIN_NAME_LEN or not uid:
                    continue
                pair = (platform or "", str(uid))
                names.setdefault(key, {}).setdefault(pair, name)
            entry = (now, names)
            self._cache[session_id] = entry
            self._cache.move_to_end(session_id)
            while len(self._cache) > self._MAX_SESSIONS:
                self._cache.popitem(last=False)
        matched: list[tuple[str, str, str]] = []
        seen_pairs: set[tuple[str, str]] = set()
        norm_text = normalize_alias_text(text)
        for key, pair_displays in entry[1].items():
            if key not in norm_text:
                continue
            for (platform, uid), display in pair_displays.items():
                if (platform, uid) not in seen_pairs:
                    seen_pairs.add((platform, uid))
                    matched.append((display, platform, uid))
        return matched[: self._MAX_HINT_ENTRIES]


class LocalMemoryKernel:
    """Local memory kernel (PostgreSQL + pgvector).

    Responsibilities (ported from the nori-core noriflow plugin):
    - ingest: writes to memory_chat_summary (chat_summary / bot_self kinds)
    - retain_encoded: on-endpoint dual-channel write after encoding
      (chat_summary table + fact raw table); when the encoder is not
      assembled or encoding degrades, falls back to single-channel ingest
      (raw text goes to the summary table marked summarized=false, out of
      recall, re-encoded by the merge agent's backfill pass)
    - search / build_injection_text: M3 recall pipeline (vector + BM25 dual
      route -> RRF fusion -> rerank -> relevance threshold -> time decay ->
      near-duplicate dedup -> truncation; P3 relation section assembled
      independently)
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
        """Initialize the memory kernel.

        Args:
            db: Memory database access layer (already connected + migrated).
            embedding_service: Embedding service (in-row vectors set to NULL
                when unavailable).
            circuit_breaker: DB circuit breaker.
            config: Plugin runtime configuration.
            bot_id: Unique identifier of the current bot (bot_self
                ownership).
            encoder: On-endpoint memory encoder; None makes
                retain_encoded degrade to the single channel.
            bot_nickname: Bot nickname (excluded in the encoder prompt).
            rerank_client: Rerank client (None or
                config.rerank_enabled=false degrades retrieval to plain
                vector order).
            history_window_provider: Host history-window block count reader
                (anchor for recent-range exclusion / rolling fill-back;
                returns max_memory_length, live-read so hot config changes
                take effect immediately; None disables both features).
            host_tz_provider: Host timezone reader (locale.TZ; the fallback
                source when the plugin timezone is not configured).
            recall_log: Recall evaluation log writer (None disables
                logging).
        directory_source: Entity-directory name source callback
            (session_id -> [(name, platform, uid)], see _EntityDirectory;
            None disables the directory route).
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
        # 召回访问强化在飞任务（011 生命周期）：持引用防 GC 中途取消；
        # 正常路径任务自清，_drain_reinforcement 供测试等待落定
        self._reinforce_tasks: set = set()
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
        """Ingest a memory into the summary table (memory_chat_summary).

        kind semantics (aligned with the hindsight tag strategy):
        - chat_summary: conversation dialogue summary; participants stores
          all speakers of this round ("platform:uid" composite keys, used by
          user-exact recall filtering);
        - bot_self: bot self-triggered batch raw text; participants is left
          empty (recall matches globally by kind).

        document_id strategy (idempotent dedup, reusing the hindsight hash
        strategy):
        - chat_summary: {session_id}-{md5(content)[:12]} (unique per round,
          never overwrites history)
        - bot_self: bot-self-{md5(content)[:12]} (identical content is
          idempotent)
        - others: {kind}-{session_id}-{md5(content)[:12]}

        Args:
            content: Memory content (summary body / bot_self raw text).
            session_id: Session ID.
            user_id: User ID (the trigger speaker, stored as a bare uid).
            memory_category: Memory category (the local table distinguishes
                purposes via the kind column; this parameter only keeps the
                contract).
            platform: Platform identifier (e.g. "qq").
            group_id: Group ID (empty means a private chat).
            kind: Memory kind (chat_summary / bot_self).
            timestamp: Event time (None uses now; stored into the
                occurred_at column).
            bot_id: Current bot ID (overrides self.bot_id, optional).
            participant_user_ids: (platform, user_id) list of all speakers in
                the batch (written into the participants column; None/empty
                falls back to the trigger speaker alone).
            summarized: Encoding status (false = conversation raw text
                written by degraded encoding, does not participate in recall,
                re-encoded by the merge agent's backfill pass; bot_self raw
                text is always written with true).
            apply_write_dedup: Whether to run write-side near-duplicate dedup
                (only the retain encoded-summary path passes True; explicit
                tool writes / degraded raw text / bot_self do not
                participate). On a hit the insert is skipped but document_id
                is still returned (same semantics as the content-hash
                idempotent skip).

        Returns:
            document_id.

        Raises:
            MemoryDBUnavailable: during a circuit-open rejection period or a
                write execution failure (the retain caller rolls back its
                watermark to retry; tool entry points catch it and report the
                failure).
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
        """Ingest raw conversation text (on-endpoint dual-channel write after
        encoding).

        The signature stays consistent with the upstream nori
        retain_encoded contract
      (for experience/data interoperability).

        Channel 1 (chat_summary table): the fidelity summary produced by
        encoding (covers only this round's batch),
          document_id/participants strategy is the same as single-channel
          ingest, recallable;
        Channel 2 (fact raw table): each EncodedFact is written independently
        to memory_persona_fact_raw,
          extracted_flag=0 awaiting the merge agent (M4) to cluster and
          score, does not participate in recall;
          evidence_key={session_id}|{occurred_at:date} is the later scoring
          dedup key, document_id idempotency granularity aligns with it
          (same-session same-day dedup; cross-session / cross-day recurrence
          is stored as an independent piece of evidence for scoring).

        Encoding degradation chain (graceful degradation):
        - encoder is None (not assembled) -> raw text goes through the
          chat_summary single channel as-is;
        - encode() raises or the output is unparseable -> internal fail-open
          returns (raw text, [], False),
          i.e. single-channel behavior, does not block retain;
        - degraded raw rows written with summarized=false: do not participate
          in recall (filtered at retrieval), re-encoded by the
          merge agent's backfill pass after the LLM recovers (summary
          rewritten into the same row + facts
          into the fact table), data not lost, context not polluted.

        Args:
            conversation_text: conversation text with timestamps and speaker
                identifiers (including uid),
                containing the history-context / this-round-batch separator
                marker lines (envelope assembly).
            All remaining parameters have the same semantics as ingest.

        Returns:
            document_id list (one summary + one per fact).
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

    async def write_fact(
        self,
        *,
        statement: str,
        category: str,
        confidence: str = "high",
        platform: str,
        session_id: str,
        group_id: str,
        user_id: str,
        display_name: str = "",
        replaces_cluster_id: Optional[int] = None,
        occurred_at: Optional[datetime] = None,
    ) -> dict:
        """Active fact write (the deterministic direct-cluster path of the
        memory_write tool).

        Complementary to retain's encoded-extraction path: no encoding LLM,
        no waiting for the merge agent's cycle - after inserting the raw row
        it immediately applies apply_fact_merge to form a cluster (create, or
        replace to supersede the old cluster); an explicit instruction is the
        highest-ranked evidence. Idempotency loop: document_id granularity =
        owning uid + session + date + statement hash; duplicate writes on the
        same day land on the same raw row; when the row is already consumed
        (flag=1) the apply optimistic-lock rejects and returns
        {"action": "skipped"}. Encoding-extraction of the same fact by
        retain in the same round is adjudicated same into the cluster by the
        merge agent; evidence_key dedups on the same key without double
        scoring.

        Semantic constraints (caller-guaranteed): category is limited to the
        active six dimensions (system dimensions recent/uncertain do not go
        through this channel); ownership is pinned to the parameter user_id
        (the tool layer locks the scope to the trigger user, preventing
        unauthorized proxy writes).

        Raises:
            MemoryDBUnavailable: circuit-open rejection or any step failure
                (the tool layer converts it into an error text).
        """
        if not await self.circuit_breaker.peek_available():
            raise MemoryDBUnavailable("记忆库熔断拒绝期，主动事实写入被拒绝")
        occurred = self._to_local(occurred_at or datetime.now())
        evidence_key = self._build_evidence_key(session_id, occurred)
        embedding = await self.embedding_service.embed_one(statement)
        base_prefix = fact_document_id([user_id], session_id, occurred)
        content_hash = hashlib.md5(statement.encode()).hexdigest()[:12]
        document_id = f"{base_prefix}-{content_hash}"
        if embedding is None:
            # fail-open 落库等补算回填（与 retain 编码路径同哲学：主动
            # 写入因向量故障被拒 = 对话滑走后内容永丢）；embedded 标志让
            # 工具层如实告知「稍后才可检索」，不静默违约立即生效承诺
            logger.warning(
                "主动事实写入向量化失败（embedding 不可用），"
                "置 NULL 落库等补算回填: %s", document_id
            )

        cfg = self.config
        start_score = (
            cfg.score_start_high
            if confidence == "high"
            else cfg.score_start_medium
        )

        async def _write(doc_id: str) -> dict:
            """Land a raw row under the given idempotent key and run
            cluster/replace, returning the merge summary."""
            row_id: Optional[int] = None

            async def _insert() -> None:
                nonlocal row_id
                row_id = await self.db.upsert_persona_fact_raw_for_apply(
                    document_id=doc_id,
                    platform=platform,
                    user_id=user_id,
                    related_user_ids=[],
                    display_name=display_name,
                    category=category,
                    statement=statement,
                    confidence=confidence,
                    session_id=session_id,
                    group_id=group_id,
                    evidence_key=evidence_key,
                    occurred_at=occurred,
                    embedding=embedding,
                )

            if not await self._guarded(
                _insert, description=f"主动事实写入 {doc_id}"
            ) or row_id is None:
                raise MemoryDBUnavailable(f"主动事实写入失败: {doc_id}")

            summary: dict = {}

            async def _apply() -> None:
                nonlocal summary
                summary = await self.db.apply_fact_merge(
                    fact_id=row_id,
                    action=(
                        "replace" if replaces_cluster_id is not None
                        else "create"
                    ),
                    cluster_id=replaces_cluster_id,
                    evidence_key=evidence_key,
                    occurred_at=occurred,
                    start_score=start_score,
                    score_cap=cfg.score_cap,
                    promote_threshold=cfg.promote_threshold,
                    recent_promote_threshold=cfg.recent_promote_threshold,
                )

            if not await self._guarded(
                _apply, description=f"主动事实成簇 {doc_id}"
            ) or not summary:
                raise MemoryDBUnavailable(f"主动事实成簇失败: {doc_id}")
            return summary

        summary = await _write(document_id)
        if (
            summary.get("action") == "skipped"
            and replaces_cluster_id is not None
        ):
            # 同日改回旧说法：原语句当日已写入且行已消费，乐观锁拒绝
            # 再次成簇——更正语义优先于同日重复去重。换更正作用域的
            # 幂等键（语句+目标簇）重落一行完成替换；该键也已消费
            # （同一更正当日重复执行）时维持 skipped，工具层如实报告
            correct_hash = hashlib.md5(
                f"{statement}#r{replaces_cluster_id}".encode()
            ).hexdigest()[:12]
            summary = await _write(f"{base_prefix}-{correct_hash}")
        summary["embedded"] = embedding is not None
        await self._register_fact_code_aliases([statement], platform, occurred)
        return summary

    async def _ingest_facts(
        self,
        facts: list[EncodedFact],
        platform: str,
        session_id: str,
        group_id: str,
        timestamp: Optional[datetime],
    ) -> list[str]:
        """Batch-write encoded facts into memory_persona_fact_raw.

        statements are vectorized at once (a single embeddings request),
        then inserted one by one;
        any insert failure (including circuit-open) immediately raises
        MemoryDBUnavailable - this method runs before the summary channel in
        retain_encoded; after the caller rolls back the watermark, the next
        round re-encodes from scratch, leaving no half-committed residue;
        document_id idempotency guarantees retries do not create duplicate
        rows
        (same statement ON CONFLICT skips). The old "skip this row and
        continue" semantics would silently and permanently lose the whole
        batch of facts during the rejection period (the batch is already
        marked consumed, violating the content-loss-free contract).

        Args:
            facts: List of encoded person facts.
            platform: Platform identifier.
            session_id: Source session ID of extraction (part of the
                evidence key).
            group_id: Source group ID of extraction.
            timestamp: Fact occurrence time (None uses now).

        Returns:
            List of document_ids successfully queued.

        Raises:
            MemoryDBUnavailable: any single insert failure (caller rolls back
                the watermark to retry).
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
        await self._register_fact_code_aliases(
            [fact.statement for fact in facts], platform, occurred_at
        )
        return doc_ids

    async def _register_fact_code_aliases(
        self, statements: list[str], platform: str, last_seen: datetime
    ) -> None:
        """Register the fact-statement "user<uid> (<alias-code>)" as an alias for
        that uid (bypass-side gain).

        In group chats, how a member is addressed is often completely
        different in form from their card name (card undefinedxxxy vs group
        name xxy), and message-stream alias upsert can never learn such code
        names - only the fact statement's LLM-written "user3429924750 (xxy)"
        binds them to the uid explicitly. After registration, entity hits
        (persona candidate / ask-about-others recall / memory tool name
        resolution) work immediately.

        Bypass positioning: on failure only a warning is logged, never
        raised - aliases are a bonus, not a contract; the retry semantics of
        the main writes (raw/summary) are unaffected; existing clusters are
        covered by the merge agent's periodic pass (the alias backfill pass
        of run_cycle), this method only handles real-time increments.
        """
        if self._alias_store is None or not statements:
            return
        try:
            rows = build_fact_code_alias_rows(
                [(platform, s, last_seen) for s in statements]
            )
            if not rows:
                return
            await self.db.alias_upsert(rows)
            self._alias_store.apply_rows(rows)
            logger.debug("事实代号别名登记: %d 行（source=fact）", len(rows))
        except Exception:
            logger.warning("事实代号别名登记失败（下轮事实继续）", exc_info=True)

    async def _ingest_relations(
        self,
        relations: list[EncodedRelation],
        platform: str,
        session_id: str,
        timestamp: Optional[datetime],
        bot_user_id: str = "",
    ) -> list[str]:
        """Write encoded relation triples into memory_entity_edge with zero LLM
        merging.

        A single executemany batch commit (structural-key merge +
        evidence_key dedup counting + bot edge pending to active inline
        transition, all in the same statement, see db.upsert_entity_edge);
        any failure (including circuit-open) raises
        MemoryDBUnavailable - this method sits after facts and before
        summary in retain_encoded; once the caller enqueues to the pending
        queue, the DB replays from scratch on recovery without half-commit;
        evidence_key idempotency guarantees retries do not repeat counting.
        document_id is not persisted (the edge table uses an auto-increment
        primary key), it only serves the return value / log observability.

        Args:
            relations: List of validated encoded relation triples.
            platform: Platform identifier.
            session_id: Source session ID of extraction (part of the
                evidence key).
            timestamp: Relation occurrence time (None uses now).
            bot_user_id: bot platform uid (for the pending/activation gate
                of bot-endpoint edges).

        Returns:
            List of edge idempotent keys (for observability).

        Raises:
            MemoryDBUnavailable: batch write failure (caller enqueues to the
                pending queue for replay).
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
        """Search memory (vector + rerank pipeline).

        Pipeline: query vectorized in real time (in parallel with the
        expansion-key / recent-range exclusion boundary)->
        scope filter + cosine top-N candidates (N = max(rerank_candidates,
        top_k*4);
        when rerank is off N = top_k*4; expansion-key hits are always pinned
        to the current session, recent-range exclusion
        removes the most recent K batches of this session's summaries for the
        host window; when topic_blacklist is non-empty blacklist rows
        are structurally excluded at the SQL candidate layer - they do not
        consume top_k slots, and the injection layer keeps a second line of
        defense)->
        rerank (rerank_enabled with a usable
        client; on failure it degrades to plain vector order) -> relevance
        threshold filter (applied to rerank scores or
        cosine similarity) -> time-decay reorder (when
        recall_time_decay_enabled,
        score x 2^(-age/H), only reorders, never filters) -> near-duplicate
        dedup (greedy on the final order,
        drops anything with cosine >= dedup_similarity_threshold against a
        kept entry)->
        top_k truncation.

        v0.5.1 design revision: the recall-hint bypass (entity-directory /
        reference reverse-lookup candidate
        fetching) has been removed entirely - entity hits only feed persona /
        P3 node matching, and referenced raw text is
        merged into query by the adapter to go through this main route
        (session isolation is uniformly governed by the isolation switch).

        Ask-about-others recall (entity_user_keys/entity_user_ids, consumed
        only by scope=session): the (platform, uid) key group hit by the
        query entities - when cross-session is open
        (cross_session=True) they merge into the primary key group (summaries
        from any session of the asked-about person, including their
        private chats with the bot, can be recalled); under session isolation
        they are pinned to the current session (only the entity's
        summaries in this session). The privacy posture is determined by the
        caller's isolation config; the landing point is the entity key group
        entry of db.search_chat_summaries.

        scope modes:
        - session (default, framework MemoryStage/planner path): session +
          bot_self;
          when platform+user_id are non-empty an AND user-filter group is
          appended (aligned with the hindsight tag combination,
          planner active recall passes no platform -> whole session);
        - user: user-exact recall (participants array filter, naturally
          cross-session);
        - user_session: user plus AND session_id tightening.

        Multi-user (user_ids list) and per_user quota:
        - per_user=false (default): OR-mixed single pool, one vector search,
          global top_k;
        - per_user=true: reuses the full main pipeline (expansion / recent
          exclusion / hybrid retrieval / relevance
          threshold / time decay / near-dedup / rolling fill-back row
          exclusion - the candidate pool is scaled by the user count),
          only the truncation stage swaps to quota logic: at most top_k rows
          per user (ownership decided by the row's user_id
          and participants composite key, earlier-quota owner wins on hits to
          multiple users), unattributable rows
          (bot_self raw text / expansion hits) go to the shared slots (at
          most top_k in total).

        [Not wired yet - capability slot] per_user currently has no caller
        (host memory recall
        all goes through the single-pool path; it is not a config switch but
        a capability at the parameter level of this method).
        Wire it later as needed; before wiring, note:
        1) cross-platform identity - user_ids bare uids share a single
           platform prefix, linked
           accounts (same person across adapters) need the (platform, uid)
           pair form;
        2) identity-level quota - one person with multiple accounts should
           share one bucket of quota, otherwise
           one person gets multiple quotas through many accounts;
        3) candidate pool saturation - the single pool is sorted by
           relevance; when the pool is filled by one user's rows, quota can
           only be redistributed inside the pool (candidate scaling by user
           count mitigates, not a guarantee - this is the semantic
           difference from the old bucketed
           implementation's "per-bucket LIMIT guarantee", an intentional
           trade-off: no second
           retrieval pipeline is maintained);
        4) scope semantics tightening - the old bucketed route does not
           tighten sessions for scope="session"
           (only user_session passes tighten), while the single-pool route
           applies all scope conditions;
           wiring with session + per_user changes behavior from
           "cross-session quota" to
           "in-session quota", the expected posture must be confirmed before
           wiring.

        Args:
            query: Query text for retrieval.
            top_k: Maximum number of returned rows (per_user mode means
                per-user quota + shared slots).
            session_id: Session ID (consumed by scope=session/user_session).
            user_id: User ID (bare uid; enables the user-filter group when
                platform non-empty).
            platform: Platform identifier.
            bot_id: Current bot ID (compatibility contract; local kind
                filtering already covers bot_self).
            cross_session: Cross-session relaxation (only effective for
                scope=session).
            exclude_kinds: List of kind values to exclude (compatibility
                contract; the local table has no person_fact,
                fact tables structurally never join recall, usually no need
                to pass).
            scope: session | user | user_session.
            user_ids: List of bare uids for multi-user recall (None falls
                back to a single user_id).
            per_user: Whether to give each user a guaranteed quota in
                multi-user recall (not wired yet, see above).
            entity_user_keys: List of entity-hit composite keys for
                ask-about-others recall (consumed only by scope=session).
            entity_user_ids: Corresponding list of bare uids.

        Returns:
            List of memory items (at most top_k rows; per_user mode at most
                top_k rows per user plus up to top_k shared rows).
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

        # 访问强化（011 生命周期，对齐 iris batch_update_access 语义）：
        # 只对最终注入集（截断后）刷新——这一批是真正喂给模型的记忆，
        # 被截掉的候选行不算"被想起"。fire-and-forget：写库挂后台任务，
        # 失败仅告警，绝不拖慢/阻断召回返回。
        self._fire_reinforcement(rows)

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
        """Entity-hit entries [(name, platform, uid)] (window dictionary +
        persistent alias layers merged).

        Priority: window dictionary (speakers in the session, most
        authoritative and real-time) > persistent alias layer
        (memory_entity_alias, covering long-silent / historically-renamed
        members). On ambiguous names in the persistent layer, the window hit
        wins, otherwise the entry is skipped (determinism beats recall); dedup
        is by (platform, uid), window entries are kept first. The recall-hint
        route and the persona-candidate route share
        one hit result (one match at the injection entry, consumed in two
        places); always empty when recall_hint_enabled
        is off or no name source is injected.
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
        """Handle to the persistent alias layer (used to sync memory via
        apply_rows after a batch upsert)."""
        return self._alias_store

    def make_recall_hints(
        self,
        entity_user_keys: Optional[list[str]] = None,
        match_text: str = "",
        bot_addressed: bool = False,
        bot_user_id: str = "",
    ) -> RecallHints:
        """RecallHints factory (called by the adapter; on the kira side assembled
        by inject_memory in main.py).

        Since v0.5.1 hints only carry entity hits (persona candidate / P3
        node matching) and
        match_text/bot_addressed (P3 edge-injection signals) - they no longer
        drive recall candidate
        fetching (the bypass was removed), and the anchor parameter was
        deleted. bot_user_id is the bot's platform
        uid (the secondary form matched by scenario C's dual-form matching);
        when non-empty it is opportunistically learned into the form memo,
        and the write-side is_bot_edge judgment also benefits
        (platform-uid-form edges correctly go through the pending /
        double-evidence gate).
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
        """All bot uid forms, deduped in order: explicit value -> bot_id ->
        learned memo.

        The unified supply for the write-side is_bot_edge and the read-side
        scenario C (bot_uid_set semantics).
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
        """Build the memory text to inject into the LLM.

        Returned format (time labels plus the attribution/tense preamble are
        plugin extensions):
            # Related long-term memory (background reference only; "{bot_nickname}" in the text is you)
            All of the following are group-chat records from sometime in the
            past: the speaker actions in each entry happened at the time
            labeled at the end of the entry, unrelated to the current
            message's speaker; do not treat what others said or did in memory
            as happening right now.
            When referring to old matters, summarize and paraphrase with a
            sense of timing (e.g. "earlier", "around August"),
            do not mention sources such as "memory/retrieval", and do not
            quote verbatim.
            - {memory_text_1} (about 2 weeks ago)
            - {memory_text_2} (today)
            ...

        All preamble goes at the front (no trailing notes) - the planner's
        active recall appends a bullet to the
        end of the existing memory_context, so a rule placed at the end
        would be clipped out of alignment by the appended line.

        When recall_time_label_enabled is on, each entry gets a relative time
        label appended (local timezone, day
        granularity); when off, the entry lines carry no label and the time
        reference in the preamble
        changes to "earlier events" (no longer "labeled at the end of the
        entry").

        Returns an empty string when there is no memory.
        exclude_person_facts is a defensive parameter for hindsight legacy
        data: the local fact table structurally never joins recall, so no
        equivalent operation is needed (accepted and ignored).

        Ask-about-others recall: hints.entity_user_keys (entity hits matched
        by the adapter against the batch text)
        merge into the main route retrieval; when hints carry no keys,
        self-match once against query as a
        fallback (the case where planner active recall passes no hints). When
        cross-session is open the entity keys
        merge into the primary key group, under session isolation they are
        pinned to the current session (landing point is the search
        entity-key parameter).

        Args:
            query: Query text for retrieval.
            session_id: Session ID.
            top_k: Maximum number of returned rows.
            user_id: User ID (enables the user-filter group when platform
                non-empty).
            platform: Platform identifier.
            bot_id: Current bot ID (compatibility contract).
            cross_session: Whether to recall across sessions (only effective
                for scope=session).
            exclude_person_facts: Compatibility contract (locally satisfied
                structurally, ignored).
            scope/user_ids/per_user: Same semantics as search.
            hints: Recall hints (entity_user_keys consumed as the
                ask-about-others recall key group).
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
        # 归因/时态导语全部前置（不用尾注）：planner 主动检索会向既有
        # memory_context 末尾追加 bullet，规则若置尾会被追加行截断错位；
        # 前置保证追加后规则恒在列表上方、结构不破。
        # 时间指代按标注开关动态选措辞：关闭时条目行无尾部时间标注，
        # 导语若仍称"条目末尾标注的时间"会指挥模型找不存在的标注。
        time_phrase = (
            "各条目中发言者的言行都发生在条目末尾标注的时间，"
            if self.config.recall_time_label_enabled
            else "各条目都是更早发生的事，"
        )
        lines = [
            f"# 相关长期记忆（仅供背景参考{self._identity_note()}）",
            "以下均为过去某时的群聊记录：" + time_phrase
            + "与当前消息的发言者无关，不要把记忆中他人的言行当成眼前正在发生的事。",
            "提及旧事时请概括转述、带时效感（如“之前”“8月那会儿”），"
            "不要提及“记忆/检索”等来源，也不要逐字复述。",
        ]
        used = len("\n".join(lines))
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
        """P3 edge injection: scenario A/C relation statement lines + neighbor
        persona (independent budget).

        Matching text = hints.match_text (both adapter sides use the
        same-source text from the entity directory, including
        @nickname rendering); label word-form hit = label in text (same
        mechanism as name matching, deterministic substring). Node matching
        uses the (platform, uid) composite key - entity-hit keys already
        carry a platform prefix, so a numeric-uid collision across adapters
        never mismatches another platform's person's edge/persona
        in (same semantics as the main persona path). Persona is only
        appended for the "counterpart of an unhit node"
        (the hit one already went through the entity candidate route), bot
        endpoints have no persona; when persona_service is not assembled,
        only statement lines are emitted.
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
            session_platform=session_platform,
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
                    session_platform=session_platform,
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
                session_platform=session_platform,
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
        """Edge time qualifier: "as of {month}/{day}" (last_seen in local
        timezone).

        The label carries multi-value semantics (decision 8: no automatic
        mutual exclusion), so the injection carries a time qualifier letting
        the LLM adjudicate old vs new itself; naive timestamps are treated as
        already local (+ the configured timezone, same semantics as
        _to_local), never via the server's local timezone - no deviation when
        the configured timezone and the server timezone differ.
        """
        if ts is None:
            return ""
        try:
            if ts.tzinfo is None:
                ts = ts.replace(tzinfo=self._local_tz())
            local = ts.astimezone(self._local_tz())
        except (ValueError, OSError, OverflowError):
            return ""
        return f"截至{local.month}月{local.day}日"

    # ------------------------------------------------------------------
    #  内部工具
    # ------------------------------------------------------------------

    @staticmethod
    def _now() -> datetime:
        """Current UTC time (for decay computation; a standalone method lets
        tests fix the clock)."""
        return datetime.now(timezone.utc)

    def _derive_window_batches(self) -> int:
        """Window anchor for recent-range exclusion / rolling fill-back: the host
        LLM-visible history window length in blocks.

        The KiraAI host window truncates by "blocks" (session memory has 1
        chunk = 1 round; only the most recent max_memory_length blocks are
        kept; the window contents are OpenAIMessage dicts without
        timestamps, so boundaries cannot be anchored by time). retain
        produces exactly one summary batch per round (a round-completion
        signal triggers one encoding), so "the most recent K encoded
        summary batches" is the equivalent of "the contents already visible
        in the window". Live-read from the provider makes hot config changes
        effective immediately; on failure / when unassembled it returns 0
        (recent-range exclusion and rolling fill-back are off for that round).

        Returns:
            Window block count (0 = disabled).
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
        """Recall query expansion: take the participants of the session's most
        recent N summary batches as the expansion key group.

        Expansion-key hits are always pinned to the current session on the
        SQL side (see search_chat_summaries),
        independent of the cross_session switch - "asking about a member not
        present" can hit summaries they
        participated in within the same session, but never surfaces any
        cross-session (including private) memories of that member.
        The bot itself is excluded (the bot participates in almost every
        batch; keeping it in would effectively cancel the user filter for
        this session). Only effective for scope=session (user/user_session
        have no session-isolation requirement anyway).
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
        """Get this session's already-injected rolling-fill-back block
        document_ids (within TTL; otherwise None).

        Args:
            session_id: Session ID.

        Returns:
            List of document_ids to exclude; None when there is no valid memo
            (SQL applies no condition).
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
        """Build the "most recent session summary batches outside the window"
        injection block (fill back for conversations rolled out of the
        history window).

        Background: memory sources are only the context window + recall
        summaries + persona, and conversations that roll out of the round
        count depend entirely on recall to be refilled - but recall is a
        query-driven semantic search, so recent context just rolled out of
        the window stays permanently invisible if nobody brings it up. This
        method skips the most recent K batches (K = host window block count,
        same source as recall recent-range exclusion) and fetches the earlier
        N batches for the llm_request to inject as a standalone Prompt
        (timeline placed before the memory-recall block).

        Dedup contract: after a successful injection, memoize these
        document_ids (session-level + TTL),
        so the same round's recall / planner active recall exclude these rows
        via the search() row exclusion,
        avoiding a double-presented "injection block + recall block".

        Args:
            session_id: Session ID (bare id; same-numbered sessions across
                adapters are distinguished by platform).
            platform: Platform identifier (when non-empty, restrict to this
                adapter - same convention as the OFFSET quota and the
                recent-range exclusion subquery).

        Returns:
            The injection block text (empty string when there is no usable
            content / the switch is off / the window boundary is
            unavailable).
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
            "不要提及记忆来源，不要逐字复述"
            f"{self._identity_note()}）"
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
        """Local timezone (plugin config timezone > host locale.TZ > server
        local)."""
        if self._local_tz_cache is None:
            self._local_tz_cache = time_labels.resolve_local_tz(
                (self.config.timezone or "").strip(), self._host_tz_provider
            )
        return self._local_tz_cache

    def _identity_note(self) -> str:
        """Bot-name self-reference anchor (shared by the recall block and the
        rolling-fill-back block, avoiding wording drift between the two).

        Memory records the bot's actions in the third person (the library
        keeps the bot name verbatim so literal search still matches);
        when injecting, explicitly tell the LLM that this name is itself,
        preventing the bot's actions in memory from being taken as another
        member's.
        """
        return (
            f"；文中“{self.bot_nickname}”即你自己" if self.bot_nickname else ""
        )

    def reset_local_tz_cache(self) -> None:
        """Invalidate the cached local timezone (maintenance-page save path).

        The cache is resolved once and never expires; after a timezone config
        change the relative-time labels and evidence-key date derivations must
        pick up the new zone on the next call instead of after a restart.
        """
        self._local_tz_cache = None

    def _to_local(self, dt: datetime) -> datetime:
        """Idempotent-key date convention normalization: aware inputs are
        converted to the local timezone, naive ones are treated as already
        local.

        evidence_key / fact_document_id / relation_document_id must take the
        date in local convention (host-passed naive local timestamps are
        passed through as-is). strftime directly on an aware (UTC) value from
        any path would mismatch the merge agent's backfill-encoding path
        (normalized by merge_agent._to_local) - UTC+8's 00:00-07:59 spans a
        day difference, and the same batch of facts would be re-inserted and
        double-scored. astimezone only changes the representation, never the
        instant; the converted value written to the DB has no side effects.
        """
        if dt.tzinfo is None:
            return dt
        return dt.astimezone(self._local_tz())

    def _relative_time_label(self, ts: datetime | None) -> str:
        """Injection time label (local timezone): the form is decided by
        recall_time_label_mode.

        The implementation delegates to the shared module
        time_labels.memory_time_label - the persona service's
        freshness column needs labels identical to the recall memory block
        word for word; a single implementation prevents drift between the
        two. Calibration details live in the time_labels module docstring.

        Args:
            ts: Memory occurrence time (aware; None or a future timestamp
                returns an empty string).

        Returns:
            The label text (empty string means no label).
        """
        return time_labels.memory_time_label(
            ts,
            now=self._now(),
            local_tz=self._local_tz(),
            mode=self.config.recall_time_label_mode,
        )

    def _relative_part(self, days: int) -> str:
        """Relative time distance (day granularity): delegates to the shared
        implementation (calibration in time_labels)."""
        return time_labels.relative_time_part(days)

    def _absolute_part(self, local: datetime, days: int, now_local: datetime) -> str:
        """Absolute time anchor (layered precision): delegates to the shared
        implementation (calibration in time_labels)."""
        return time_labels.absolute_time_part(local, days, now_local)

    def _fire_reinforcement(self, rows: list) -> None:
        """Recall reinforcement dispatch (011 lifecycle): asynchronously refresh
        the reinforcement timestamps of the final injected set.

        Semantics aligned with iris's batch_update_access, but narrowed to
        only refreshing the final injected set -
        candidate rows cut by top_k do not count as "recalled".
        fire-and-forget: run in a background task, any exception is only
        warned about (not via _guarded_call - a reinforcement failure should
        not count toward the circuit breaker,
        it is not part of the recall main path). Task handles are held on the
        instance set (the event loop only holds weak references to tasks,
        so without holding a reference they may be GC-cancelled midway).

        No dispatch when summary_lifecycle_reinforce_on_recall is off; the
        lifecycle
        master switch does not gate this hook (reinforcement counts
        accumulate independently, for the first-pass criterion once enabled).
        """
        if not self.config.summary_lifecycle_reinforce_on_recall or not rows:
            return
        doc_ids = [str(r["document_id"]) for r in rows if r.get("document_id")]
        if not doc_ids:
            return

        async def _reinforce() -> None:
            task = asyncio.current_task()
            try:
                await self.db.reinforce_summaries(doc_ids)
            except Exception:
                logger.warning("召回访问强化写入失败（不影响召回结果）", exc_info=True)
            finally:
                if task is not None:
                    self._reinforce_tasks.discard(task)

        try:
            task = asyncio.get_running_loop().create_task(_reinforce())
        except RuntimeError:
            # 无运行中的事件循环（同步上下文直接调 search 的场景）：
            # 放弃异步派发，强化是尽力而为语义
            return
        self._reinforce_tasks.add(task)

    async def _drain_reinforcement(self) -> None:
        """Wait for in-flight reinforcement tasks to settle (test hook: ensures
        the writes have happened before assertions)."""
        pending = [t for t in self._reinforce_tasks if not t.done()]
        if pending:
            await asyncio.gather(*pending, return_exceptions=True)

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
        """Write a search event (a no-op when logging is not enabled)."""
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
        """Write an inject event (a no-op when logging is not enabled)."""
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
        """per_user quota truncation (pure function, directly testable; rows must
        already be sorted in the final order).

        Ownership judgment uses the same convention as the main route's
        user-filter group: row user_id == uid, or
        "platform:uid" in the row's participants; on hits to multiple users
        the earlier-quota owner wins (order follows the uids order).
        Unattributable rows (bot_self raw text / expansion hits to
        non-listed users) go to the shared slots - in the old bucketed
        implementation bot_self was counted into some user's quota per
        bucket; a standalone shared slot is semantically cleaner (the bot's
        own memory belongs to everyone, occupying no one's quota).

        [Not wired yet] per_user has no caller (see the search docstring's
        pre-wiring notes: cross-platform identity pairs / identity-level
        bucket sharing / candidate pool saturation /
        scope semantics tightening).
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
        """Plain vector order: explicitly sort by descending cosine relevance and
        backfill score.

        SQL already returns rows sorted by cosine; sorting here again
        removes the implicit dependency on the return order
        (transit through bucket dedup / stubs may shuffle the order). When
        hybrid search is on, rows carry
        "rrf" (the two-route fused score), so the degraded order follows the
        fused score (otherwise the sparse entries retrieved by the BM25
        route would be buried by the cosine order).

        Args:
            rows: Candidate rows (carrying the relevance field, plus rrf when
                hybrid).

        Returns:
            The sorted row list (the score field is backfilled).
        """
        for row in rows:
            row["score"] = row.get("rrf", row["relevance"])
        rows.sort(key=lambda r: float(r["score"]), reverse=True)
        return rows

    async def _guarded_call(
        self, operation: Callable[[], Awaitable[_T]], description: str
    ) -> _T | None:
        """Circuit-guarded DB read (fail-open: any failure logs and returns None,
        never raises).

        Args:
            operation: zero-argument coroutine factory (a DB read operation,
                returns the result).
            description: log description.

        Returns:
            The operation result; None when skipped by circuit-open or the
            execution failed.
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
        """Circuit-guarded DB operation execution (fail-open: any failure logs and
        skips, never raises).

        Args:
            operation: zero-argument coroutine factory (a DB write
                operation).
            description: log description (including document_id).

        Returns:
            True when the execution succeeded; False when skipped by
            circuit-open or the execution failed.
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
        """Build the participants composite-key list ("platform:uid", deduped and
        order-preserving).

        Args:
            kind: Memory kind (bot_self leaves it empty - recall matches
                globally by kind, no user filter needed).
            platform: Platform identifier.
            user_id: trigger speaker (fallback when the participant list is
                empty).
            participant_user_ids: (platform, user_id) list of all speakers in
                the batch.

        Returns:
            The composite-key list; empty for bot_self or when there are no
            speakers at all.
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
        """Build the summary-table document_id (idempotent key, strategy aligned
        with the hindsight plugin).

        Args:
            kind: Memory kind (chat_summary / bot_self / others).
            session_id: Session ID.
            content_hash: content MD5 hash (first 12 characters).

        Returns:
            The document_id string.
        """
        if kind == "chat_summary":
            # session_id 已含 bot_id（如 group-{gid}-{bot_id}），无需重复
            return f"{session_id}-{content_hash}"
        if kind == "bot_self":
            return f"bot-self-{content_hash}"
        return f"{kind}-{session_id}-{content_hash}"

    @staticmethod
    def _build_evidence_key(session_id: str, occurred_at: datetime) -> str:
        """Build the scoring evidence key "{session_id}|{occurred_at:date}".

        Repeated extractions on the same session and day (overlapping
        history windows) naturally land on the same key,
        deduped by the M4 merge agent's evidence_keys.

        Args:
            session_id: Source session ID of the extraction.
            occurred_at: Fact occurrence time.

        Returns:
            The evidence-key string.
        """
        return f"{session_id}|{occurred_at.strftime('%Y-%m-%d')}"

    async def forget_summary(
        self,
        document_id: str,
        *,
        scope_session_id: str = "",
        scope_user_id: str = "",
    ) -> bool:
        """Delete a summary memory by idempotent key (the kernel entry of the
        memory_remove maintenance tool).

        Args:
            document_id: summary idempotent key.
            scope_session_id: when scope locking is non-empty, appends a
                session-ownership qualifier (passed down to the DAL).
            scope_user_id: when scope locking is non-empty, appends a
                user-ownership qualifier (passed down to the DAL).

        Returns:
            Whether a row was deleted; False when circuit-open or the
            execution failed.
        """
        return bool(await self._guarded_call(
            lambda: self.db.delete_chat_summary(
                document_id,
                scope_session_id=scope_session_id,
                scope_user_id=scope_user_id,
            ),
            description=f"删除摘要 {document_id}",
        ))
