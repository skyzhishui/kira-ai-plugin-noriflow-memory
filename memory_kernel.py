"""LocalMemoryKernel: local memory kernel based on a dual storage backend
(PostgreSQL + pgvector / SQLite).

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
failure it raises MemoryDBUnavailable, caught by main._do_retain which
rolls back the message watermark, so the same batch of messages is
re-encoded on the next signal round (the host has no retry mechanism; the
watermark-rollback replay is the equivalent way to keep content loss-free);
the merge agent (merge_agent) periodic task separately peeks the circuit
state at cycle entry (skips the whole cycle during the rejection period,
no burned LLM budget).
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
from .db import (
    MemoryDatabase,
    evidence_key,
    fact_document_id,
    parse_vector,
    summary_document_id,
)
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

# Slow-call sentinel threshold (milliseconds): elapsed-time observations
# above it escalate from DEBUG to WARNING (same convention as upstream
# nori: 500ms)
SLOW_CALL_THRESHOLD_MS = 500.0

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

# Valid scope values
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
    _MIN_NAME_LEN = 2  # single-char names ("阿"/"好") mis-hit too often,
                       # not admitted into the directory
    _MAX_HINT_ENTRIES = 4  # per-round hit-entry cap (guards candidate
                           # storms from duplicate names / common-word
                           # nicknames)

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
            # key -> {pair: that pair's own observed raw name} -- with
            # multiple uids under one normalized key, hit entries each
            # carry their own addressing and never paste someone else's
            # raw name (same convention as AliasStore._display_by_pair)
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
    """Local memory kernel (PostgreSQL + pgvector / SQLite dual backend).

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
        # The platform (adapter name) the bot runs on, propagated by the
        # plugin layer via plain attribute assignment
        # (kernel.bot_platform = <adapter name>) once it learns the bot
        # identity; empty string when unknown.
        self.bot_platform: str = ""
        # bot uid form memo (includes the platform-uid secondary form;
        # learned/backfilled by make_recall_hints -- shared by the
        # write-side is_bot_edge and the read-side scenario C matching,
        # see bot_uid_set)
        self._bot_uid_forms: set[str] = set()
        self.encoder = encoder
        self.bot_nickname = bot_nickname
        self.rerank_client = rerank_client
        self._history_window_provider = history_window_provider
        self._host_tz_provider = host_tz_provider
        self._recall_log = recall_log
        # Entity directory (for entity hits -- persona candidates / P3
        # node matching; always empty when source is None)
        self._entity_directory = _EntityDirectory(directory_source)
        # Persistent entity alias layer (persistent hit source after the
        # window dictionary; always empty when alias_enabled is off)
        self._alias_store: AliasStore | None = (
            AliasStore(
                db,
                variant_cap=config.alias_variant_cap,
                stopwords=config.alias_stopwords,
            )
            if config.alias_enabled
            else None
        )
        # Neighbor profile service for P3 edge injection (backfilled once
        # main assembles it; when None, edge injection emits statement
        # lines only and assembles no profiles -- the profile budget is
        # independent of host profile injection)
        self.persona_service = None
        self._local_tz_cache = None
        # Recall access-reinforcement in-flight tasks (011 lifecycle):
        # references are held to keep GC from cancelling them midway; on
        # the normal path tasks clean themselves up (discarded in finally)
        self._reinforce_tasks: set = set()
        # Rolling rollout fill-back dedup memo:
        # {session_id: (list of injected document_ids, monotonic time)};
        # same-round recall / active retrieval excludes injected rows by
        # it; the TTL covers one pipeline round
        self._rollout_memo: dict[str, tuple[list[str], float]] = {}
        # Cross-session injection dedup memo (same shape):
        # {session_id: (injected document_ids, monotonic time)}
        # Shares the retrieval exclusion channel with rolling fill-back; the
        # entry point always clears it so no early return leaves the previous
        # turn's exclusions behind (wrong exclusion within the TTL blinds
        # recall)
        self._cross_session_memo: dict[str, tuple[list[str], float]] = {}
        # Two-phase commit staging area (written by
        # build_recent_rollout_text, committed by commit_rollout_memo):
        # prompt assembly may be dropped between build and final
        # injection; uncommitted ids must never enter the memo (else they
        # would no longer appear in the fill-back while still being
        # excluded by recall -- invisible in both channels for the whole
        # TTL)
        self._rollout_pending: dict[str, tuple[list[str], float]] = {}
        # One-shot warn flag for a host window block count of 0 (see
        # _derive_window_batches)
        self._window_zero_warned = False

    # ------------------------------------------------------------------
    #  Write path
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
        document_id = self._build_document_id(
            kind=kind, session_id=session_id, content=content
        )
        participants = self._build_participants(
            kind=kind,
            platform=platform,
            user_id=user_id,
            participant_user_ids=participant_user_ids,
        )

        # Fail directly during a circuit-open rejection period (peek is
        # non-consuming; the recovery-probe quota is left for the real
        # write below): the old contract still returned document_id after
        # a rejection, so retain batches got marked consumed without ever
        # being persisted (a silent loss); and embed_one running ahead of
        # the rejection would burn one vectorization call for nothing
        if not await self.circuit_breaker.peek_available():
            raise MemoryDBUnavailable("记忆库熔断拒绝期，写入被拒绝")

        # Vectorize at write time (fail-open: on failure set NULL, the
        # backfill task fills it in)
        embedding = await self.embedding_service.embed_one(content)

        # Write-side near-duplicate dedup (retain encoded-summary path
        # only): compare against the last write_dedup_window batches of
        # encoded summaries in the same session; a cosine >= threshold
        # skips the write -- suppressing same-event duplicate rows caused
        # by history context leaking into summaries (the 6705/6707 class).
        # The window is limited to this session's recent batches, so
        # distant / cross-session similar events are not wrongly killed; a
        # query failure is fail-open and writing continues (prefer
        # duplication over loss)
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
                logger.warning(
                    "写入侧近重去重命中: document_id=%s session=%s 跳过摘要写入"
                    "（与最近 %d 批最高 cosine=%.3f ≥ 阈值 %.2f）",
                    document_id, session_id, self.config.write_dedup_window,
                    best, self.config.write_dedup_threshold,
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
            return []  # no encoding input, nothing to write

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

        # Circuit-open rejection period: the write path rejects outright;
        # writing the raw text anyway is busywork and would silently lose
        # the batch (the ingest old contract still returned document_id
        # after a rejection; once the batch is marked consumed the loss is
        # permanent). Failing directly lets the caller (_do_retain) roll
        # back the watermark, leaving this batch of messages to be
        # re-encoded on the next signal round (content is not lost); the
        # encoding LLM and vectorization calls burn nothing. peek is
        # non-consuming -- the recovery-probe quota is consumed by the
        # real write after recovery
        if not await self.circuit_breaker.peek_available():
            raise MemoryDBUnavailable(
                "记忆库熔断拒绝期：retain 失败（调用方回滚水位线，待下轮重编码）"
            )

        # On-endpoint encoding (fail-open: any failure returns
        # (conversation_text, [], False)); one more fallback layer here so
        # an unexpected exception from the encoder implementation also
        # degrades to the raw-text single channel
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

        # Deterministic defense line: the bot itself must never become a
        # profile subject. The encoding prompt's exclusion clause already
        # constrains this, but when user speech @-mentions the bot /
        # mentions its name and number, the LLM may still extract the bot
        # as a fact subject (user_id filled with the bot's platform ID,
        # forming a bot profile) -- this hard filter by bot_id is the
        # fallback, and it runs before vectorization (dropped entries
        # consume no embedding).
        if effective_bot_id and facts:
            kept = []
            dropped = 0
            for f in facts:
                if f.user_id.strip() == effective_bot_id:
                    dropped += 1
                    continue
                # Strip the bot from the related set in sync (when a bot
                # participates in a relation fact, its composite key would
                # propagate into candidate retrieval matching along with
                # the cluster)
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

        # Channel order (facts -> relations -> summary, summary last --
        # failure-direction safe, aligned with the merge agent backfill
        # pass's write order): any failure before the summary lands
        # propagates up -> the caller (_do_retain) rolls back the message
        # watermark, and this batch is re-encoded and replayed from zero on
        # the next signal round with no half-committed residue; if summary
        # went first, a facts/relations failure after the watermark
        # rollback would re-encode into different summary text and land a
        # second row (document_id embeds a content hash -- text drift means
        # a duplicate row) -- while the facts/relations idempotency keys
        # guarantee retries do not duplicate (same-statement ON CONFLICT
        # skip/merge).
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

        # Channel 1.5 (P2 relation edges): structured relation triples land
        # in the edge table with zero LLM merging; dropped when the
        # extraction switch is off (the encoder appends no extension
        # section, so normally always empty -- this fallback guards against
        # residual output from a stale prompt cache / hand-edited config)
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

        # Channel 2: summary goes through chat_summary (reusing ingest's
        # idempotency/participants strategy); on encoding degradation
        # (encoded_ok=False, content is the raw text) it is marked
        # summarized=false, stays out of recall, and is re-encoded by the
        # merge agent's backfill pass; encoded summaries enable write-side
        # near-duplicate dedup
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
        active dimensions (system dimensions recent/uncertain do not go
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
        # Timestamp clamping: future tool input is untrusted, and the recent
        # section orders by occurred_at first, so a future value would pin
        # itself to the top until the wall clock catches up. The idempotency
        # key (evidence_key / document_id prefix) keeps the original input
        # time -- using the clamped now in the key would give two calls with
        # the same future timestamp different keys across days, breaking
        # idempotent dedup and double-scoring. The clamped value is used only
        # for storage and profile ordering (naive-local and aware-local paths
        # each compare against a now of the same kind)
        key_occurred = occurred
        now_local = (
            datetime.now(tz=self._local_tz())
            if occurred.tzinfo is not None
            else datetime.now()
        )
        if occurred > now_local:
            occurred = now_local
        evidence_key = self._build_evidence_key(session_id, key_occurred)
        embedding = await self.embedding_service.embed_one(statement)
        base_prefix = fact_document_id([user_id], session_id, key_occurred)
        content_hash = hashlib.md5(statement.encode()).hexdigest()[:12]
        document_id = f"{base_prefix}-{content_hash}"
        if embedding is None:
            # fail-open persist, waiting for the backfill task's fill-in
            # (same philosophy as the retain encoded path: rejecting an
            # active write over a vector failure = content lost forever
            # once the conversation scrolls away); the embedded flag lets
            # the tool layer honestly report "searchable later" instead of
            # silently defaulting on the immediate-effect promise
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
                    naming_promote_threshold=cfg.naming_promote_threshold,
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
            # Same-day correction back to an older statement: the original
            # statement was written today and its row is already consumed,
            # so the optimistic lock rejects re-clustering -- correction
            # semantics take precedence over same-day duplicate dedup.
            # Re-land one row under a correction-scoped idempotency key
            # (statement + target cluster) to complete the replace; when
            # that key is also consumed (the same correction executed
            # repeatedly in one day), stay skipped and let the tool layer
            # report it honestly
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

        # Batch vectorization (fail-open: on failure the whole batch is
        # None, the backfill task fills it in)
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
                # Raise instead of skipping: the summary channel has not
                # been written yet (facts go first), so rollback + retry
                # leaves no half-commit and no duplicates; the old
                # semantics would permanently lose the whole batch of facts
                # during a circuit-open period
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
        summary in retain_encoded; the caller (main._do_retain) catches it,
        rolls back its message watermark, and the whole batch is re-encoded
        on the next signal round without half-commit;
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
            MemoryDBUnavailable: batch write failure (the caller rolls back
                its message watermark and the whole batch is re-encoded on
                the next signal round).
        """
        occurred_at = self._to_local(timestamp or datetime.now())
        evidence_key = self._build_evidence_key(session_id, occurred_at)
        min_evidence = self.config.relation_bot_edge_min_evidence
        alias = self._alias_store

        def _endpoint_name(uid: str, llm_name: str) -> str:
            # Placeholder-name guard: the LLM occasionally outputs
            # "未知"/uid fallback forms; substitute the alias view's latest
            # name, or leave empty on no hit (the upsert statement keeps
            # the old name, and read-side graph resolution falls back for
            # display)
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
    #  recall retrieval (M3; pipeline in dev plan section 9.2.2)
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

        # Query vectorization is an HTTP call (the bulk of retrieval
        # latency); the expansion keys / recent-range exclusion boundary
        # are computed in parallel during its flight (one index query + one
        # host config read)
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
        # Real-time query vectorization (fail-open: on failure return
        # empty, never blocking the injection path)
        query_vec = await embed_task
        if query_vec is None:
            logger.warning("recall 查询向量化失败（embedding 不可用），返回空结果")
            return []

        use_rerank = self.rerank_client is not None and self.config.rerank_enabled
        n_candidates = max(
            self.config.rerank_candidates if use_rerank else 0, top_k * 4
        )

        # Normalize user params: uid list + corresponding composite keys.
        # scope=session (framework path) aligns with hindsight semantics:
        # the user filter group is enabled only when platform and user are
        # both non-empty -- planner active recall passes no platform and
        # should search the whole session; user/user_session modes are
        # user-filter-centric and allow a bare-uid fallback
        uids = [u for u in (user_ids or ([user_id] if user_id else [])) if u]
        keys = [f"{platform}:{u}" for u in uids] if platform else []
        if scope == "session" and not platform:
            uids = []

        is_per_user = bool(per_user and uids)
        if is_per_user:
            # Scale the candidate pool by the user count: quota truncation
            # acts on the single post-rerank pool, and when the pool is
            # filled by one user's rows the quota cannot conjure slots out
            # of thin air (scaling mitigates, it is no guarantee)
            n_candidates = max(n_candidates, top_k * 4 * len(uids))

        # Lazy embedding fetch (perf P2): when not per_user and the DB exposes
        # fetch_embeddings_by_ids, the retrieval SELECT carries no embedding --
        # dedup consumes vectors only for the rows it actually visits, fetched
        # precisely by id in paged chunks (head chunk up front, further chunks
        # on demand; avoiding transfer + JSON parsing of 2 x n_candidates rows
        # x ~18KB of vector text). Older DB implementations/test stubs without
        # this method keep the old behavior (rows ship their own embedding) for
        # cross-version compatibility.
        fetch_embeddings_by_id = getattr(self.db, "fetch_embeddings_by_ids", None)
        lazy_embeddings = not is_per_user and fetch_embeddings_by_id is not None

        # Single-pool retrieval (per_user shares the route -- the full
        # pipeline of filtering/expansion/exclusion/hybrid applies here,
        # quota is introduced only at the truncation stage;
        # with_participants supports ownership judgment)
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
                exclude_document_ids=self._injected_excluded_ids(session_id),
                with_embedding=(
                    self.config.dedup_similarity_threshold > 0
                    and not lazy_embeddings
                ),
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

        # Rerank (on failure degrade to plain vector order; the rerank
        # score is the final relevance)
        score_is_absolute = False
        if use_rerank:
            try:
                ranked = await self.rerank_client.rerank(
                    query, [r["content"] for r in rows], top_k=len(rows)
                )
                score_by_index = dict(ranked)
                for idx, row in enumerate(rows):
                    # Candidates not returned get -1 and queue at the tail
                    # (candidates are kept, never silently dropped)
                    row["score"] = score_by_index.get(idx, -1.0)
                rows.sort(key=lambda r: float(r["score"]), reverse=True)
                score_is_absolute = True
            except Exception:
                logger.warning("重排序失败，退化为纯向量序", exc_info=True)
                rows = self._sort_by_relevance(rows)
        else:
            rows = self._sort_by_relevance(rows)

        # Relevance threshold filter (rerank score or cosine similarity,
        # uniformly applied to score). Skipped only for the hybrid
        # retrieval's degraded order (rerank off/failed and rows carry RRF
        # fused scores): RRF is a relative ranking score of ~0.01-0.03
        # magnitude, and an absolute-score threshold would kill everything;
        # the pure-vector degraded order (cosine) keeps the same scale and
        # is filtered as usual
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

        # Time decay (optional): applied after the semantic threshold
        # filter and before truncation, score x 2^(-age/H) then reorder --
        # reorders only, never filters; a missing occurred_at counts as
        # unknown age and is not down-weighted (factor 1.0); future
        # timestamps (clock skew) are clamped to 0
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

        # Near-duplicate dedup (greedy scan in the final order): drop rows
        # whose embedding cosine against a kept entry is >= the threshold
        # -- adjacent-round summaries overlap heavily, and without dedup
        # top_k would be filled by consecutive snapshots of the same event.
        # Stop once top_k is reached (later rows cannot make the truncation
        # result anyway, saving dot-product work); per_user has no early
        # stop -- quota truncation runs after dedup, and rows that quota
        # would drop also occupy dedup scan slots, so an early stop would
        # wrongly kill the shared-slot rows at the queue tail (bot_self
        # etc.) during the dedup stage. Rows with a missing vector neither
        # join the comparison nor get dropped (guaranteed visible). per_user
        # joins dedup too (full pipeline) -- cross-user near-duplicates
        # (dual-perspective summaries of one conversation) need only keep
        # one.
        dedup_dropped = 0
        sim_threshold = self.config.dedup_similarity_threshold
        # Lazy embedding loading (P2, paged): the retrieval SELECT carries no
        # embedding; dedup fetches vectors by id only for the rows it actually
        # consumes, chunk by chunk (chunk = top_k*4, min 16). The head chunk
        # is loaded up front; dropped rows do not count toward top_k, so a
        # long near-duplicate run can push the loop past the loaded boundary
        # -- further chunks are then fetched on demand, so beyond-window
        # near-duplicates still get deduped instead of silently kept. Rows
        # whose vector is missing or failed to fetch follow the existing
        # "missing vector" semantics (not compared, never dropped); fetch
        # failures fail open via the shared guarded call (breaker accounting
        # same as the retrieval leg) and do not block recall.
        dedup_chunk = max(top_k * 4, 16)
        loaded_upto = 0
        # Stop paging on the first failed chunk fetch (dedup_fetch_dead):
        # failed chunk fetches happen back-to-back with no retrieval success
        # in between, so without this latch a pool spanning many chunks with
        # a persistently failing fetch would accumulate breaker failures
        # within one round and trip the shared breaker, silently killing the
        # whole recall (review). With the latch, one round counts at most one
        # fetch failure (the next round's retrieval success resets the
        # counter), and the un-fetched rows keep missing-vector semantics.
        dedup_fetch_dead = False
        if lazy_embeddings and sim_threshold > 0 and rows:
            # A failed FIRST chunk cannot trigger paging in any reachable
            # state: the lazy path only runs on the non-per-user branch
            # (keep_bound = top_k < dedup_chunk) and vector-less rows are
            # always kept, so kept_rows reaches top_k before idx crosses
            # the chunk boundary. The latch below therefore only ever
            # engages for genuine mid-pool paging failures.
            await self._load_dedup_vectors(
                rows, 0, dedup_chunk, fetch_embeddings_by_id
            )
            loaded_upto = min(len(rows), dedup_chunk)
        keep_bound = len(rows) if is_per_user else top_k
        if sim_threshold > 0 and rows:
            kept_rows: list[dict] = []
            kept_unit_vecs: list[list[float]] = []
            for idx, row in enumerate(rows):
                if len(kept_rows) >= keep_bound:
                    break
                if (
                    lazy_embeddings
                    and not dedup_fetch_dead
                    and idx >= loaded_upto
                ):
                    fetched = await self._load_dedup_vectors(
                        rows,
                        loaded_upto,
                        idx + dedup_chunk,
                        fetch_embeddings_by_id,
                    )
                    if not fetched:
                        dedup_fetch_dead = True
                    loaded_upto = min(idx + dedup_chunk, len(rows))
                vec = parse_vector(row.get("embedding"))
                is_dup = False
                if vec:
                    norm = math.sqrt(sum(x * x for x in vec))
                    if norm > 0:
                        unit = [x / norm for x in vec]
                        for kept_vec in kept_unit_vecs:
                            # Dimension-mismatched vectors are treated as
                            # non-duplicates: the SQLite backend enforces no
                            # fixed vector dimension, so legacy rows stored
                            # under a different embedding model can carry a
                            # different length; zip() would silently
                            # truncate and produce a meaningless score whose
                            # spurious >=threshold hits drop recall rows
                            # (same contract as db.sqlite._cosine returning
                            # None on length mismatch).
                            if len(unit) != len(kept_vec):
                                continue
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

        # Truncation: per_user mode applies per-user quota + shared slots
        # (after rerank/dedup), otherwise global top_k
        if is_per_user:
            rows = self._truncate_per_user_quota(
                rows, uids=uids, platform=platform, top_k=top_k
            )
        else:
            rows = rows[:top_k]

        # Access reinforcement (011 lifecycle, semantics aligned with iris
        # batch_update_access): refresh only the final injected set (after
        # truncation) -- this batch is what is actually fed to the model;
        # candidate rows cut by truncation do not count as "recalled".
        # fire-and-forget: the write runs as a background task, failure
        # only warns, never slowing/blocking the recall return.
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
                # A persistent-layer failure must not drag down the window
                # route (fail-open)
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
        started = time.perf_counter()
        try:
            return await self._build_injection_text_impl(
                query=query,
                session_id=session_id,
                top_k=top_k,
                user_id=user_id,
                platform=platform,
                bot_id=bot_id,
                cross_session=cross_session,
                exclude_person_facts=exclude_person_facts,
                scope=scope,
                user_ids=user_ids,
                per_user=per_user,
                hints=hints,
            )
        finally:
            elapsed_ms = (time.perf_counter() - started) * 1000
            logger.debug(
                "build_injection_text 耗时 %.3f ms",
                elapsed_ms,
            )
            if elapsed_ms > SLOW_CALL_THRESHOLD_MS:
                # Slow-query sentinel (same convention as upstream nori:
                # >500ms escalates to WARNING)
                logger.warning(
                    "build_injection_text 慢调用 %.3f ms（session=%s）",
                    elapsed_ms,
                    session_id,
                )

    async def _build_injection_text_impl(
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

        # P3 edge injection (scenarios A/C): the relation statement-line
        # section, independent of the recall main route -- the section can
        # still be produced on its own when the main route has no hits
        # (fail-open, existing injection unaffected)
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

        # Topic blacklist second line of defense: the primary exclusion
        # sits at the search candidate layer (SQL content NOT LIKE ALL;
        # blacklist rows do not consume top_k slots); this catches leaks
        # from fragment/word-form differences (normally always 0 hits),
        # preventing a single topic from polluting the prompt
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

        # Token budget truncation: conservatively estimated by character
        # count (CJK approximated as 1 char = 1 token); the first entry may
        # exceed the budget (avoids returning all-empty when a single entry
        # is over-long); afterwards, stop once the running sum overflows
        budget = self.config.recall_max_tokens
        # The attribution/tense preamble always goes up front (no trailing
        # note): the planner's active retrieval appends a bullet to the end
        # of the existing memory_context, and a rule placed at the tail
        # would be clipped out of alignment by the appended line; being up
        # front keeps the rule above the list after appending, structure
        # intact.
        # Time references pick their wording dynamically by the labeling
        # switch: when off, entry lines carry no trailing time label, and a
        # preamble still saying "the time labeled at the end of the entry"
        # would direct the model to look for labels that do not exist.
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
        # Relative time labels (local timezone): let the LLM tell "yesterday"
        # from "three months ago", so distant memories are not used as
        # recent happenings
        label_enabled = self.config.recall_time_label_enabled
        for item in items:
            content = item.content.strip()
            if not content:
                continue
            label = self._relative_time_label(item.timestamp) if label_enabled else ""
            # Budget on the rendered line: the time label (15-25 chars in
            # "both" mode) is part of what the model actually sees, so
            # costing only the bare content systematically overshoots
            # recall_max_tokens.
            line = f"- {content}（{label}）" if label else f"- {content}"
            cost = len(line)
            if appended > 0 and used + cost > budget:
                logger.info(
                    "token 预算截断: 保留 %d 条（预算 %d）", appended, budget
                )
                truncated = True
                break
            lines.append(line)
            injected.append({"id": item.id, "label": label})
            # debug injection detail: id + each entry truncated to 100 chars
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
        # Scenario C dual-form matching: a bot endpoint uid exists in the
        # edge table in two forms (session identifier / platform uid,
        # bot_uid_set semantics); composite key = form x current session
        # platform; gate = only rounds that AT/reference the bot fetch
        # bot-endpoint edges
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
        # Canonical endpoint-name resolution: the edge table's redundant
        # name is a snapshot of the name "at the time of the last evidence"
        # and goes stale (stock audit: 57/65 active edge endpoint names !=
        # the alias table's latest canonical name). The alias table's
        # latest non-placeholder name wins (filtered by the selected edges'
        # endpoints, hitting the (platform, uid) index instead of a full
        # scan), with the edge name as fallback; bot endpoints are not in
        # the alias table, and relation_statement_line falls back to the
        # bot nickname internally.
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
    #  Internal helpers
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
            # The host truncating memory[-0:] equals the whole list --
            # max_memory_length=0 is effectively an infinite window (all
            # history visible), while the plugin treats 0 = anchor
            # unavailable and disables recent-range exclusion and rolling
            # rollout fill-back, so this session's recent summaries could be
            # injected in pairs with window content. The real fix lives in
            # the host (clamp 0 via max(..., 1) or reject at startup
            # validation)
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
        # Dual-form bot exclusion (bot_uid_set semantics, same source as
        # _bot_uid_forms_all): the bot may appear in participants under its
        # session-id form (bot_id) or its platform-uid form (learned memo,
        # see RecallHints.bot_user_id) - excluding only one form lets the
        # other leak bot keys into the expansion group and effectively
        # cancel the user filter for this session.
        bot_forms = self._bot_uid_forms_all()
        keys: list[str] = []
        uids: list[str] = []
        for r in rows:
            for key in (r.get("participants") or []):
                k = key.strip() if isinstance(key, str) else ""
                if not k or k in keys:
                    continue
                if any(k.endswith(f":{form}") for form in bot_forms):
                    continue
                keys.append(k)
            uid = (r.get("user_id") or "").strip()
            if uid and uid not in uids and uid not in bot_forms:
                uids.append(uid)
        return keys, uids

    # TTL of the rolling rollout fill-back dedup memo: covers one pipeline
    # round (MemoryStage assembly -> planner active retrieval -> replyer
    # injection), with headroom for extra-long agent delegation rounds
    # (delegation produces no new round to refresh the memo; too short a
    # TTL would double-inject)
    _ROLLOUT_MEMO_TTL_SECONDS = 1800.0
    # memo capacity cap (expired entries are pruned beyond it; guards slow
    # accumulation over long runs)
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

    def refresh_cross_session_memo(self, session_id: str, document_ids: list[str]) -> None:
        """Replace this round's cross-session injected ids; empty clears.

        Synchronous (no await). The injection hook calls it unconditionally
        at entry (clear -- so no early return can leave stale exclusions)
        and again after a *successful* injection (register only the ids
        actually rendered; budget-dropped rows stay retrievable). With the
        feature disabled the register call never fires -- the clear at entry
        is the only per-round call. Mirrors the rollout memo TTL/capacity
        semantics.
        """
        if not session_id or not document_ids:
            self._cross_session_memo.pop(session_id, None)
            return
        unique = list(dict.fromkeys(str(doc_id) for doc_id in document_ids if doc_id))
        if not unique:
            self._cross_session_memo.pop(session_id, None)
            return
        self._cross_session_memo[session_id] = (unique, time.monotonic())
        if len(self._cross_session_memo) > self._ROLLOUT_MEMO_MAX_SESSIONS:
            now_m = time.monotonic()
            self._cross_session_memo = {
                key: value
                for key, value in self._cross_session_memo.items()
                if now_m - value[1] <= self._ROLLOUT_MEMO_TTL_SECONDS
            }

    def _cross_session_excluded_ids(self, session_id: str) -> list[str] | None:
        """Cross-session memo ids within TTL (None when absent/expired)."""
        if not session_id:
            return None
        entry = self._cross_session_memo.get(session_id)
        if not entry:
            return None
        ids, at = entry
        if time.monotonic() - at > self._ROLLOUT_MEMO_TTL_SECONDS:
            self._cross_session_memo.pop(session_id, None)
            return None
        return ids or None

    def _injected_excluded_ids(self, session_id: str) -> list[str] | None:
        """Union of every already-injected memo (rollout fill-back +
        cross-session summaries) for this session, deduped.

        The shared retrieval path (recall + active memory_search) consumes
        this: rows already presented to the model this round are excluded so
        the same summary cannot be both injected and recalled (double
        exposure), while budget-dropped rows stay searchable.
        """
        ids = [
            *(self._rollout_excluded_ids(session_id) or []),
            *(self._cross_session_excluded_ids(session_id) or []),
        ]
        return list(dict.fromkeys(ids)) or None

    async def fetch_recent_cross_session_summaries(
        self,
        *,
        current_session_id: str,
        user_keys: list[str],
        limit: int,
        max_age_days: int = 0,
        current_platform: str = "",
        current_session_type: str = "",
    ) -> list[dict]:
        """Fetch recent cross-session summaries through the shared guarded
        call (fail-open: a DB failure logs and returns []).

        Args:
            current_session_id: bare session id to exclude.
            user_keys: composite identity keys ("platform:uid") of the user.
            limit: max rows.
            max_age_days: drop rows older than this (0 = unlimited).
            current_platform: platform tag of the current session (scopes the
                current-session exclusion).
            current_session_type: "dm" / "gm" of the current session (scopes
                the current-session exclusion: the bare id alone collides
                between a DM with a user and a group chat).

        Returns:
            Dict rows (document_id / session_id / content / occurred_at).
        """
        rows = await self._guarded_call(
            lambda: self.db.fetch_recent_cross_session_summaries(
                current_session_id=current_session_id,
                user_keys=user_keys,
                limit=limit,
                max_age_days=max_age_days,
                current_platform=current_platform,
                current_session_type=current_session_type,
            ),
            description="跨会话摘要查询",
        )
        return rows or []

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

        Dedup contract (two-phase commit): this method only STAGES the
        injected document_ids into _rollout_pending; the caller must call
        commit_rollout_memo(session_id) once the rollout text has actually
        been appended into the final prompt pieces and BEFORE the recall
        search runs. Commit moves the staged ids into the live
        _rollout_memo (session-level + TTL), which the same round's
        recall / planner active recall then exclude via the search() row
        exclusion, avoiding a double-presented "injection block + recall
        block". If prompt assembly is discarded, nothing is committed and
        the rows stay recall-visible (writing the memo at build time would
        have made them invisible in both channels for a full TTL).

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

        # Budget selection newest -> oldest (rows already sorted by
        # occurred_at DESC): the newest batch is the bridge batch adjacent
        # to the history window; over budget, drop older batches; when a
        # single batch exceeds the budget, truncate it (avoids an empty
        # block). Presentation order oldest -> newest (splices into the
        # history timeline)
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
                    break  # already have batches: drop older ones
                row = {**row, "content": content[:budget] + "…"}  # batch cut
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

        # Stage only (two-phase commit): the ids enter the live memo when
        # the caller commits after actually using the text (see
        # commit_rollout_memo).
        self._rollout_pending[session_id] = (injected_ids, time.monotonic())
        if len(self._rollout_pending) > self._ROLLOUT_MEMO_MAX_SESSIONS:
            self._rollout_pending = self._prune_rollout_memo(
                self._rollout_pending
            )
        logger.debug(
            "滚动补回: session=%s | 注入 %d 批（跳过窗口内 %d 批）",
            session_id, len(injected_ids), skip_batches,
        )
        return "\n".join(lines)

    @classmethod
    def _prune_rollout_memo(
        cls, memo: dict[str, tuple[list[str], float]]
    ) -> dict[str, tuple[list[str], float]]:
        """Drop TTL-expired entries from a rollout memo/pending dict
        (capacity bounding for both phases)."""
        now_m = time.monotonic()
        return {
            k: v
            for k, v in memo.items()
            if now_m - v[1] <= cls._ROLLOUT_MEMO_TTL_SECONDS
        }

    def commit_rollout_memo(self, session_id: str) -> None:
        """Two-phase commit of the rolling-fill-back dedup memo.

        build_recent_rollout_text only stages the injected document_ids in
        _rollout_pending; this method moves the staged entry into the live
        _rollout_memo, preserving the build-time monotonic timestamp (the
        same TTL semantics as a single-phase write). The caller (plugin
        layer) must invoke this right after the rollout text has been
        appended into the final prompt pieces and BEFORE the recall search
        runs - if prompt assembly is discarded, the ids never enter the
        memo and the rows stay recall-visible instead of being invisible
        in both the rollout and recall channels for a full TTL.

        A no-op when there is no pending entry for the session; a second
        build before commit overwrites the pending entry.
        """
        entry = self._rollout_pending.pop(session_id, None)
        if entry is None:
            return
        self._rollout_memo[session_id] = entry
        if len(self._rollout_memo) > self._ROLLOUT_MEMO_MAX_SESSIONS:
            self._rollout_memo = self._prune_rollout_memo(self._rollout_memo)

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
            # No running event loop (the case of calling search directly
            # from a sync context): give up async dispatch; reinforcement
            # is best-effort semantics
            return
        self._reinforce_tasks.add(task)

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
                # Attributable row: take the first owner that still has
                # quota; when every quota is full, drop it (it does not
                # take a shared slot -- shared slots are for unattributable
                # rows, not an overflow area)
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

    async def _load_dedup_vectors(
        self,
        rows: list[dict],
        start: int,
        end: int,
        fetch_embeddings_by_id: Callable[..., Awaitable[dict[int, object]]] | None,
    ) -> bool:
        """Attach embeddings to rows[start:end] in place, fetched by primary
        key in one batch (lazy dedup loading, perf P2).

        Fail-open: the fetch goes through the shared guarded call; a failure
        (circuit-open included, which returns None) or an id missing from the
        returned mapping leaves the row vector-less -- "missing vector"
        semantics (not compared in dedup, never dropped), recall unblocked.
        Rows with a None id cannot be fetched and stay vector-less.

        Args:
            rows: recall candidate rows (mutated in place).
            start/end: half-open row index range to load (end is clamped to
                len(rows); a degenerate range is a no-op).
            fetch_embeddings_by_id: the backend's fetch_embeddings_by_ids
                coroutine (the caller only invokes this helper on the lazy
                path, where it is not None).

        Returns:
            True when the guarded fetch succeeded (rows attached, the mapping
            itself may be empty); False on failure or circuit-open skip -- the
            caller stops paging for the rest of the round, so one recall
            round counts at most one breaker failure and the paging loop
            cannot trip the shared breaker on its own.
        """
        end = min(end, len(rows))
        if end <= start:
            return True
        ids = [r["id"] for r in rows[start:end] if r.get("id") is not None]
        if not ids:
            return True
        embed_by_id = await self._guarded_call(
            lambda: fetch_embeddings_by_id(ids=ids),
            description="去重向量取回",
        )
        if embed_by_id is None:
            return False
        for row in rows[start:end]:
            embedding = embed_by_id.get(row.get("id")) if embed_by_id else None
            if embedding is not None:
                row["embedding"] = embedding
        return True

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
    def _build_document_id(kind: str, session_id: str, content: str) -> str:
        """Build the summary-table document_id (idempotent key, strategy aligned
        with the hindsight plugin).

        The chat_summary branch delegates to db.summary_document_id (single
        source of truth); all branch outputs are byte-identical to the
        historical inline formulas - the id must never drift, a different
        string would orphan every already-stored summary row.

        Args:
            kind: Memory kind (chat_summary / bot_self / others).
            session_id: Session ID.
            content: Memory content body (hashed into the id).

        Returns:
            The document_id string.
        """
        content_hash = hashlib.md5(content.encode()).hexdigest()[:12]
        if kind == "chat_summary":
            # session_id already embeds bot_id (e.g. group-{gid}-{bot_id});
            # no need to repeat it
            return summary_document_id(session_id, content)
        if kind == "bot_self":
            return f"bot-self-{content_hash}"
        return f"{kind}-{session_id}-{content_hash}"

    @staticmethod
    def _build_evidence_key(session_id: str, occurred_at: datetime) -> str:
        """Build the scoring evidence key "{session_id}|{occurred_at:date}".

        Repeated extractions on the same session and day (overlapping
        history windows) naturally land on the same key,
        deduped by the M4 merge agent's evidence_keys.

        Delegates to db.evidence_key (single source of truth); the caller is
        responsible for local-timezone normalization first
        (kernel._to_local / merge_agent._to_local - strftime on a raw
        aware-UTC value can be a day off from the other path).

        Args:
            session_id: Source session ID of the extraction.
            occurred_at: Fact occurrence time (already localized; only the
                date part is used).

        Returns:
            The evidence-key string.
        """
        return evidence_key(session_id, occurred_at)

    async def forget_summary(
        self,
        document_id: str,
        *,
        scope_session_id: str = "",
        scope_user_id: str = "",
    ) -> Optional[bool]:
        """Delete a summary memory by idempotent key (the kernel entry of the
        memory_remove maintenance tool).

        Args:
            document_id: summary idempotent key.
            scope_session_id: when scope locking is non-empty, appends a
                session-ownership qualifier (passed down to the DAL).
            scope_user_id: when scope locking is non-empty, appends a
                user-ownership qualifier (passed down to the DAL).

        Returns:
            True when a row was verified deleted; False when the id is
            verified absent (no matching row under the given scope);
            None when the DB was unavailable (circuit-open rejection or
            the delete execution failed) - a temporary-unavailable signal,
            callers must not report it as "not found".
        """
        deleted = await self._guarded_call(
            lambda: self.db.delete_chat_summary(
                document_id,
                scope_session_id=scope_session_id,
                scope_user_id=scope_user_id,
            ),
            description=f"删除摘要 {document_id}",
        )
        if deleted is None:
            return None
        return bool(deleted)
