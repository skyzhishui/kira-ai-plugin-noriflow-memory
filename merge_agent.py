"""Merge agent: four-pass scan + LLM four-way adjudication + scoring state
machine orchestration (dev plan section 8).

One run_cycle runs per merge_interval_hours:
1. Normalization pass: raw table facts with flag=0 -> candidate clusters for
   the same (platform, user, category), using vector top-K (including
   tombstone clusters; scalar fallback when a fact has no vector) -> LLM
   four-way adjudication (same/drift/correction/unrelated) -> single
   transaction disposition + flag flip;
2. Decay pass: once per decay_interval_days (last-run time persisted in kv,
   survives restarts; the first run only initializes the timestamp, it does
   not decay);
3. Promotion pass: active clusters reaching the threshold -> profiled +
   profile table upsert (recent uses a lower threshold);
4. Encode-backfill pass: dialogue raw rows written into the summary table
   during the encode-degradation window (summarized=false, excluded from
   recall) are re-run through on-device encoding; summary is written back to
   the original row (status flipped, embedding recomputed), facts go through
   the normal fact raw table channel (evidence_key reuses the row's
   occurred_at);
5. Relation pass: structural self-check (a zero-LLM sentinel, violations
   counted into logs) + semantic audit (gated by relation_audit_enabled,
   LLM incrementally audits active/pending edges by id watermark, edges
   judged bad are superseded directly).

Reliability:
- LLM call budget is throttled by llm_budget_per_cycle, overshoot is carried
  over to the next cycle;
- unsure verdicts / LLM failures / disposition errors never flip the flag
  (reprocessed next cycle; idempotence is backed by evidence_key dedup plus
  optimistic locking);
- Encode-backfill pass: encode or write failures do not flip status (rows
  stay summarized=false and retry next cycle; fact/edge idempotence keys
  prevent duplicate inserts), 3 consecutive ok=False results treat the LLM
  as unavailable and end the pass early without burning the remaining
  budget;
- Semantic audit pass: a batch failure/unsure stops the pass (the watermark
  stays at the last successful batch, the same batch re-audits next cycle;
  3 consecutive failures/unsures on one batch skip it and advance the
  watermark so a poison batch cannot pin the watermark), each cycle has an
  independent call cap, exhaustion is carried over;
- All disposition scoring is pushed down to db-layer SQL (see
  db.apply_fact_merge); this layer only interprets verdicts and picks
  actions.
"""

from __future__ import annotations

from core.logging_manager import get_logger

import asyncio
import hashlib
import json
from datetime import datetime, timedelta, timezone, tzinfo
from pathlib import Path
from typing import Callable, Optional

from .clients import FastLlmExit
from .alias_store import AliasStore, build_fact_code_alias_rows, is_placeholder_name
from .config import LocalMemoryConfig
from .db import MemoryDatabase, fact_document_id
from .entity_edge import EncodedRelation, edge_has_bot_endpoint
from .json_utils import safe_parse_llm_json
from .memory_encoder import MemoryEncoder
from .prompt_loader import PromptLoader
from .relation_backfill import mark_backfill_pending
from .vector_ops import EmbeddingService

logger = get_logger("noriflow_memory.merge", "cyan")

# 裁定/审计 payload 的陈述截断上限（对齐关系通道 _STATEMENT_MAX_CHARS 的
# 量级：防极端长陈述放大簇 canonical_statement 与 prompt 体积；存储侧由
# memory_encoder._parse_fact 同步截断）
_PAYLOAD_STATEMENT_MAX_CHARS = 300

# 裁定提示词模板名（不含 .prompt 后缀）
_ADJUDICATE_PROMPT_NAME = "fact_adjudicate"
# 四分类值域
_VALID_VERDICTS = frozenset({"same", "drift", "correction", "unrelated"})

# 裁定结果 JSON Schema（tool calling 强制出口；与提示词输出格式及
# _parse_verdicts 校验字段一致——校验仍以 _parse_verdicts 为准）
_ADJUDICATE_RESULT_SCHEMA = {
    "type": "object",
    "properties": {
        "verdicts": {
            "type": "array",
            "description": "对输入 candidates 中每一个簇各给一条裁定",
            "items": {
                "type": "object",
                "properties": {
                    "cluster_id": {"type": "integer", "description": "候选簇 ID（必须来自输入列表）"},
                    "verdict": {
                        "type": "string",
                        "enum": ["same", "drift", "correction", "unrelated"],
                    },
                    "reason": {"type": "string", "description": "一句话理由（审计用）"},
                },
                "required": ["cluster_id", "verdict"],
            },
        },
        "unsure": {
            "type": "boolean",
            "description": "证据不足时 true 且 verdicts 置空数组（系统跳过本条，留待下周期重审）",
        },
    },
    "required": ["verdicts", "unsure"],
}

# 强制调用的提交工具名（LLM 将裁定结果放入其 arguments）
_ADJUDICATE_TOOL_NAME = "submit_fact_verdicts"

# 关系边语义审计提示词模板名与边判定值域
_RELATION_AUDIT_PROMPT_NAME = "relation_audit"
_EDGE_VERDICTS = frozenset({"ok", "bad"})
# 关系边审计结果 JSON Schema（tool calling 强制出口；校验仍以解析层为准）
_RELATION_AUDIT_RESULT_SCHEMA = {
    "type": "object",
    "properties": {
        "edges": {
            "type": "array",
            "description": "对输入中每一条边各给一条判定",
            "items": {
                "type": "object",
                "properties": {
                    "id": {"type": "integer", "description": "边编号（必须来自输入列表）"},
                    "verdict": {
                        "type": "string",
                        "enum": ["ok", "bad"],
                    },
                    "reason": {"type": "string", "description": "一句话理由（审计日志用）"},
                },
                "required": ["id", "verdict"],
            },
        },
        "unsure": {
            "type": "boolean",
            "description": "证据不足时 true 且 edges 置空数组（系统跳过本批，不降级任何边）",
        },
    },
    "required": ["edges", "unsure"],
}
_RELATION_AUDIT_TOOL_NAME = "submit_edge_verdicts"
# 语义审计遍水位 kv 键（值为已判定批的最大边 id；清空即全量重扫）
_RELATION_AUDIT_KV_KEY = "relation_audit_edge_id"
# 毒丸批防护：同一水位批连续失败/unsure 达此数即跳批推水位（内容型毒丸
# 边不得永久钉死水位——其后所有边永无审计之日且每周期白烧 LLM 调用；
# 跳批记 warning，人工全量重审入口 = 清空 _RELATION_AUDIT_KV_KEY）
_RELATION_AUDIT_MAX_BATCH_FAILS = 3
_RELATION_AUDIT_FAIL_MARK_KEY = "relation_audit_fail_mark"
_RELATION_AUDIT_FAIL_COUNT_KEY = "relation_audit_fail_count"
# 语义审计遍每周期 LLM 调用上限（独立于事实合并预算，防互相挤占；
# 边增量为 trickle，常态一轮即清完积压）
_RELATION_AUDIT_MAX_CALLS = 3
# 结构自检的 pending 边超龄阈值（天；bot 边长期未达双证据门槛观测）
_PENDING_STALE_DAYS = 14

# 衰减遍上次执行时间的 kv 键
_DECAY_KV_KEY = "last_decay_at"
# 摘要归档遍上次执行时间的 kv 键（011 生命周期；与衰减遍独立周期）
_SUMMARY_LIFECYCLE_KV_KEY = "last_summary_lifecycle_at"
# 补编码遍连续失败熔断阈值（视为 LLM 整体不可用，提前结束本遍）
_REENCODE_FAIL_LIMIT = 3
# 毒丸行重试上限（差分计数：仅当同一周期/遍内存在其他成功处理时才累计
# ——排除 LLM/DB 整体不可用期的误伤；超限进入跳过名单不再重试，
# 防 ORDER BY id 队首钉死饿死后续行）
_ADJUD_MAX_ATTEMPTS = 5
_REENCODE_MAX_ATTEMPTS = 5
# 跳过名单容量上限（超出对半 FIFO 截断，防 kv 值无界增长）
_SKIP_STATE_MAX_ENTRIES = 500
# 跳过状态 kv 键（JSON：{"<行id>": 连续失败次数, ...}）
_ADJUD_SKIP_KV_KEY = "adjudicate_skip_state"
_REENCODE_SKIP_KV_KEY = "reencode_skip_state"
# 冷启动首周期延迟（秒）：启动缓冲后先跑一轮（新装库尽早建簇；衰减遍
# 首次运行只初始化时间戳不衰减，存量数据安全）
_INITIAL_DELAY_SECONDS = 60.0


def _bump_skip(state: dict[str, int], row_id: str) -> None:
    """Increment the poison-row failure count; once the cap is reached, the
    caller filters the row out of fetches."""
    state[row_id] = state.get(row_id, 0) + 1


class FactAdjudicator:
    """LLM four-way adjudicator: a new fact vs a list of candidate clusters
    yields a per-cluster verdict.

    fail-soft: an LLM call failure or an unparseable output returns None,
    letting the caller skip this fact (the flag is not flipped, re-audited
    next cycle), and it never blindly creates a cluster.

    Attributes:
        llm: FastLlmExit instance (run_structured exit).
        prompt_dir: directory of adjudication prompt templates.
    """

    def __init__(
        self,
        llm: FastLlmExit,
        prompt_dir: Optional[str | Path] = None,
    ) -> None:
        """Initialize the adjudicator.

        Args:
            llm: FastLlmExit instance (fast LLM text exit).
            prompt_dir: prompt directory; None defaults to this plugin's
                prompts/ directory.
        """
        self.llm = llm
        self.prompt_dir = Path(
            prompt_dir or (Path(__file__).resolve().parent / "prompts")
        )
        self._loader = PromptLoader(self.prompt_dir)

    async def adjudicate(
        self, fact: dict, candidates: list[dict]
    ) -> Optional[dict]:
        """Adjudicate the relation between a new fact and all candidate clusters
        (one LLM call).

        Args:
            fact: raw fact row (produced by fetch_pending_facts).
            candidates: candidate cluster list (produced by
                search_cluster_candidates).

        Returns:
            {"verdicts": [(cluster_id, verdict), ...], "unsure": bool};
            None on LLM failure or unparseable output (the caller skips
            this fact).
        """
        payload = {
            "new_fact": {
                "statement": (fact["statement"] or "")[:_PAYLOAD_STATEMENT_MAX_CHARS],
                "category": fact["category"],
                "confidence": fact["confidence"],
            },
            "candidates": [
                {
                    "cluster_id": c["id"],
                    "canonical_statement": (c["canonical_statement"] or "")[
                        :_PAYLOAD_STATEMENT_MAX_CHARS
                    ],
                    "status": c["status"],
                }
                for c in candidates
            ],
        }
        try:
            system_prompt = self._loader.render(_ADJUDICATE_PROMPT_NAME)
            raw = await self.llm.run_structured(
                system_prompt=system_prompt,
                user_prompt=json.dumps(payload, ensure_ascii=False),
                schema=_ADJUDICATE_RESULT_SCHEMA,
                tool_name=_ADJUDICATE_TOOL_NAME,
            )
        except Exception:
            logger.warning("事实裁定 LLM 调用失败（本条顺延下周期）", exc_info=True)
            return None

        parsed = safe_parse_llm_json(raw)
        if not isinstance(parsed, dict):
            logger.warning(
                "事实裁定输出无法解析为 JSON 对象（本条顺延下周期）: %r",
                (raw or "")[:200],
            )
            return None

        if parsed.get("unsure"):
            return {"verdicts": [], "unsure": True}

        raw_verdicts = parsed.get("verdicts")
        if not isinstance(raw_verdicts, list):
            logger.warning("事实裁定输出缺少 verdicts 数组（本条顺延下周期）")
            return None

        valid_ids = {c["id"] for c in candidates}
        verdicts: list[tuple[int, str]] = []
        dropped = 0
        for item in raw_verdicts:
            if (
                not isinstance(item, dict)
                or item.get("cluster_id") not in valid_ids
                or item.get("verdict") not in _VALID_VERDICTS
            ):
                dropped += 1
                continue
            verdicts.append((int(item["cluster_id"]), str(item["verdict"])))
        if dropped:
            logger.warning(
                "事实裁定丢弃 %d 条非法条目（候选共 %d 条）", dropped, len(candidates)
            )
        return {"verdicts": verdicts, "unsure": False}


class RelationAuditor:
    """Relation edge semantic auditor: a batch of active/pending edges yields
    a per-edge ok/bad judgement.

    fail-soft: an LLM call failure, an unparseable output, or an unsure
    result returns None or an unsure marker, letting the caller stop this
    pass (the watermark is not advanced, the same batch re-audits next
    cycle), and it never blindly downgrades.

    Attributes:
        llm: FastLlmExit instance (run_structured exit).
        prompt_dir: directory of audit prompt templates.
    """

    def __init__(
        self,
        llm: FastLlmExit,
        prompt_dir: Optional[str | Path] = None,
    ) -> None:
        """Initialize the auditor.

        Args:
            llm: FastLlmExit instance (fast LLM text exit).
            prompt_dir: prompt directory; None defaults to this plugin's
                prompts/ directory.
        """
        self.llm = llm
        self.prompt_dir = Path(
            prompt_dir or (Path(__file__).resolve().parent / "prompts")
        )
        self._loader = PromptLoader(self.prompt_dir)

    async def audit(self, edges: list[dict]) -> Optional[dict]:
        """Judge whether a batch of edges are valid relation edges (one LLM call).

        Args:
            edges: edge rows awaiting audit (produced by
                fetch_edges_for_audit).

        Returns:
            {"edges": [(edge_id, verdict, reason), ...], "unsure": bool};
            None on LLM failure or unparseable output (the caller stops
            this pass).
        """
        payload = {
            "edges": [
                {
                    "id": e["id"],
                    "endpoints": (
                        f"{e['subject_name'] or e['subject_uid']}({e['subject_uid']})"
                        f" -> {e['object_name'] or e['object_uid']}({e['object_uid']})"
                    ),
                    "label": e["relation_label"],
                    # Edge statements are capped at 200 by parse_relation;
                    # truncate defensively for audit payload size as well.
                    "statement": (e["statement"] or "")[:200],
                    "status": e["status"],
                    "evidence_count": e["evidence_count"],
                }
                for e in edges
            ]
        }
        try:
            system_prompt = self._loader.render(_RELATION_AUDIT_PROMPT_NAME)
            raw = await self.llm.run_structured(
                system_prompt=system_prompt,
                user_prompt=json.dumps(payload, ensure_ascii=False),
                schema=_RELATION_AUDIT_RESULT_SCHEMA,
                tool_name=_RELATION_AUDIT_TOOL_NAME,
            )
        except Exception:
            logger.warning("关系边审计 LLM 调用失败（本批顺延下周期）", exc_info=True)
            return None

        parsed = safe_parse_llm_json(raw)
        if not isinstance(parsed, dict):
            logger.warning(
                "关系边审计输出无法解析为 JSON 对象（本批顺延下周期）: %r",
                (raw or "")[:200],
            )
            return None
        if parsed.get("unsure"):
            return {"edges": [], "unsure": True}

        valid_ids = {e["id"] for e in edges}
        verdicts: list[tuple[int, str, str]] = []
        dropped = 0
        items = parsed.get("edges")
        if not isinstance(items, list):
            logger.warning("关系边审计输出缺少 edges 数组（本批顺延下周期）")
            return None
        for item in items:
            if (
                not isinstance(item, dict)
                or item.get("id") not in valid_ids
                or item.get("verdict") not in _EDGE_VERDICTS
            ):
                dropped += 1
                continue
            verdicts.append(
                (int(item["id"]), str(item["verdict"]), str(item.get("reason") or ""))
            )
        if dropped:
            logger.warning(
                "关系边审计丢弃 %d 条非法条目（送审共 %d 条）", dropped, len(edges)
            )
        return {"edges": verdicts, "unsure": False}


class FactMergeAgent:
    """Four-pass scan merge agent (a background asyncio task whose lifecycle
    belongs to the plugin's initialize/terminate).

    Depends on FastLlmExit (LLM adjudication): do not start this agent when
    it is missing. Blind cluster creation would cause cluster explosion,
    while raw facts keep flag=0 and pile up waiting (no data loss).
    """

    def __init__(
        self,
        db: MemoryDatabase,
        llm: FastLlmExit,
        config: LocalMemoryConfig,
        prompt_dir: Optional[str | Path] = None,
        encoder: Optional[MemoryEncoder] = None,
        embedding_service: Optional[EmbeddingService] = None,
        bot_nickname: str = "",
        bot_id: str = "",
        circuit_breaker=None,
        tz_provider: Optional[Callable[[], tzinfo]] = None,
        bot_forms_provider: Optional[Callable[[], list[str]]] = None,
        alias_name_resolver: Optional[Callable[[str, str], str]] = None,
        alias_store: Optional[AliasStore] = None,
    ) -> None:
        """Initialize (does not start the task).

        Args:
            db: memory database access layer.
            llm: FastLlmExit instance (LLM adjudication exit).
            config: merge agent and scoring state-machine configuration.
            prompt_dir: adjudication prompt directory; None defaults to
                this plugin's prompts/ directory.
            encoder: on-device memory encoder (consumed by the encode-backfill
                pass; None skips that pass, degraded raw rows keep
                summarized=false and stay out of recall).
            embedding_service: embedding computation service (vectorizes
                encode-backfill facts on insert).
            bot_nickname: bot nickname (encoder prompt exclusion item).
            bot_id: bot platform ID (encoder prompt exclusion item plus a
                facts hard-filter fallback).
            circuit_breaker: memory DB circuit breaker (None disables the
                pre-cycle breaker check, so the periodic task just idles
                and retries during a DB outage; when provided, an open
                period skips the whole cycle without burning LLM budget).
            tz_provider: local timezone reader (same source as
                kernel._local_tz; aligns the encode-backfill idempotency
                key date basis with the retain path. None falls back to
                the server's local timezone).
            bot_forms_provider: all-form bot uid set reader (same
                semantics as kernel._bot_uid_forms_all; aligns the bot
                endpoint detection of the encode-backfill relation channel
                with the retain channel; None degrades to the single form
                self._bot_id).
            alias_name_resolver: (platform, uid) -> latest non-placeholder
                alias (the replacement source for the write-side
                placeholder-name guard, same source as
                kernel._endpoint_name; None leaves blank names to the
                upsert, which keeps the old name).
            alias_store: persistent alias in-memory view (the apply_rows
                sync channel for the fact-code alias backfill pass; None
                skips that pass, the degraded form when the alias layer is
                disabled).
        """
        self._db = db
        self._adjudicator = FactAdjudicator(llm, prompt_dir)
        self._relation_auditor = RelationAuditor(llm, prompt_dir)
        self._encoder = encoder
        self._embedding_service = embedding_service
        self._bot_nickname = bot_nickname
        self._bot_id = (bot_id or "").strip()
        self._config = config
        self._circuit_breaker = circuit_breaker
        self._tz_provider = tz_provider
        self._bot_forms_provider = bot_forms_provider
        self._alias_name_resolver = alias_name_resolver
        self._alias_store = alias_store
        # 周期参数运行时读 self._config（维护页保存配置后热生效，无需重启）
        self._task: Optional[asyncio.Task] = None
        self._last_decay_at: Optional[datetime] = None
        self._last_summary_lifecycle_at: Optional[datetime] = None
        # WebUI manual-trigger state: the inter-cycle wait doubles as a
        # kickable event so a manual request short-circuits the interval.
        # The loop stays the only runner — a kicked cycle can never
        # overlap the periodic one.
        self._kick_event: Optional[asyncio.Event] = None
        self._in_cycle = False
        self._last_cycle: Optional[dict] = None

    def _bot_forms(self) -> list[str]:
        """Full-form bot uid set (provider first; degrades to the single form
        self._bot_id)."""
        if self._bot_forms_provider is not None:
            try:
                forms = [f for f in (self._bot_forms_provider() or []) if f]
            except Exception:
                forms = []
            if forms:
                return forms
        return [self._bot_id] if self._bot_id else []

    def _alias_name_for(self, platform: str, uid: str) -> str:
        """Placeholder-name replacement source: the alias view's latest
        available name for this uid (empty string when there is no hit)."""
        if self._alias_name_resolver is None:
            return ""
        try:
            return self._alias_name_resolver(platform or "", uid or "") or ""
        except Exception:
            return ""

    @property
    def bot_id(self) -> str:
        """Bot platform ID (backfilled at runtime once the host plugin learns
        the real self_id)."""
        return self._bot_id

    @bot_id.setter
    def bot_id(self, value: str) -> None:
        """Backfill the real bot platform ID (consumed by the facts hard
        filter and the encode-backfill prompt rendering)."""
        self._bot_id = (value or "").strip()

    @property
    def bot_nickname(self) -> str:
        """Bot nickname (backfilled at runtime after the host persona
        hot-swaps)."""
        return self._bot_nickname

    @bot_nickname.setter
    def bot_nickname(self, value: str) -> None:
        """Backfill the nickname (consumed by the encode-backfill prompt's
        bot exclusion rule)."""
        self._bot_nickname = (value or "").strip()

    @property
    def running(self) -> bool:
        """Whether the task is running."""
        return self._task is not None and not self._task.done()

    @property
    def cycle_running(self) -> bool:
        """True while a merge cycle is executing (WebUI status surface)."""
        return self._in_cycle

    @property
    def last_cycle(self) -> Optional[dict]:
        """Snapshot of the last finished cycle ({"finished_at", "stats"})."""
        return self._last_cycle

    def start(self) -> None:
        """Start the background task (idempotent: a no-op when already
        running)."""
        if self.running:
            return
        self._task = asyncio.create_task(self._run(), name="memory-fact-merge")
        logger.info(
            "合并 agent 已启动: interval=%sh batch=%d budget=%d",
            self._config.merge_interval_hours,
            self._config.merge_batch_size,
            self._config.llm_budget_per_cycle,
        )

    async def stop(self) -> None:
        """Stop the background task (idempotent; waits for the current cycle
        to exit)."""
        if self._task is None:
            return
        self._task.cancel()
        try:
            await self._task
        except asyncio.CancelledError:
            pass
        self._task = None
        # A leftover event would let a post-stop kick() report success
        # while nobody is waiting on it.
        self._kick_event = None
        logger.info("合并 agent 已停止")

    async def _run(self) -> None:
        """Task main loop: run one cycle after the startup grace window, then
        run per interval; exceptions are only logged, never fatal."""
        await asyncio.sleep(_INITIAL_DELAY_SECONDS)
        while True:
            self._in_cycle = True
            try:
                stats = await self.run_cycle()
                self._last_cycle = {
                    "finished_at": datetime.now(timezone.utc).isoformat(),
                    "stats": stats,
                }
                logger.info("合并周期完成: %s", stats)
            except Exception:
                logger.warning("合并周期执行失败（顺延下周期）", exc_info=True)
            finally:
                self._in_cycle = False
            await self._wait_interval()

    async def _wait_interval(self) -> None:
        """Inter-cycle wait; kick() resolves it early (WebUI manual trigger)."""
        self._kick_event = asyncio.Event()
        try:
            await asyncio.wait_for(
                self._kick_event.wait(),
                timeout=self._config.merge_interval_hours * 3600.0,
            )
        except asyncio.TimeoutError:
            pass
        self._kick_event = None

    def kick(self) -> bool:
        """Wake the merge loop into an immediate cycle (WebUI manual trigger).

        Returns False when the loop cannot be woken right now: a cycle is
        executing, or the startup grace window is still elapsing (the
        first automatic cycle is imminent anyway).
        """
        if self._in_cycle or self._kick_event is None:
            return False
        self._kick_event.set()
        return True

    async def run_cycle(self) -> dict:
        """Run a single merge cycle (four-pass scan).

        Returns:
            stats dict: facts/merged/created/replaced/skipped_unsure/
            deferred/llm_calls/promoted/reencoded.
        """
        cfg = self._config
        stats = {
            "facts": 0,
            "merged": 0,
            "created": 0,
            "replaced": 0,
            "skipped_unsure": 0,
            "deferred": 0,
            "llm_calls": 0,
            "promoted": 0,
            "reencoded": 0,
            "edges_audited": 0,
            "edges_superseded": 0,
        }

        # 熔断拒绝期整周期跳过（peek 非消费式，不占恢复探测名额）：DB
        # 故障期行状态不翻、下周期自动重试，避免先烧 LLM 预算再全部
        # 写失败的白空转
        if (
            self._circuit_breaker is not None
            and not await self._circuit_breaker.peek_available()
        ):
            logger.warning("记忆库熔断中，跳过本合并周期")
            return stats

        # 补编码遍预算保留：存在降级原文积压时预留 1/4 预算——归一化遍
        # 持续满载会把补编码遍饿死为恒 0（summarized=false 行不参与召回，
        # 无界期无法恢复）；无积压时不预留，预算全给归一化遍。预留上限
        # budget-1：极小预算（1-3）时归一化遍至少保 1 次调用，不被 floor(1)
        # 的预留吃成恒 0
        reserve = 0
        if self._encoder is not None:
            try:
                backlog = await self._db.fetch_unsummarized_summaries(1)
            except Exception:
                backlog = []
            if backlog:
                reserve = min(
                    max(1, cfg.llm_budget_per_cycle // 4),
                    max(0, cfg.llm_budget_per_cycle - 1),
                )
        budget = cfg.llm_budget_per_cycle - reserve

        # ---- 归一化遍 ----
        # 超采样 + 跳过名单过滤：fetch 按 id 升序拉 3 倍候选，剔除已达
        # 重试上限的 unsure 毒丸行后再截回批量——防毒丸行占据队首使后续
        # 新事实永远进不了处理窗口
        skip_state = await self._load_skip_state(_ADJUD_SKIP_KV_KEY)
        facts = await self._db.fetch_pending_facts(cfg.merge_batch_size * 3)
        facts = [
            f for f in facts
            if skip_state.get(str(f["id"]), 0) < _ADJUD_MAX_ATTEMPTS
        ][: cfg.merge_batch_size]
        llm_seen_ok = False
        for fact in facts:
            # 预算耗尽在候选检索前短路：省掉无谓的 HNSW 候选查询；同时
            # 不得走无裁定的建簇路径——本条可能存在候选簇，跳过检索直接
            # create 等于盲目建簇，违反模块契约（顺延下周期重审）
            if budget <= 0:
                stats["deferred"] += 1
                continue
            candidates = await self._db.search_cluster_candidates(
                platform=fact["platform"],
                user_id=fact["user_id"],
                category=fact["category"],
                related_user_ids=fact.get("related_user_ids") or [],
                embedding=fact["embedding"],
                top_k=cfg.candidate_top_k,
            )
            verdicts: Optional[list[tuple[int, str]]] = None
            if candidates:
                budget -= 1
                stats["llm_calls"] += 1
                result = await self._adjudicator.adjudicate(fact, candidates)
                if result is None or result["unsure"]:
                    stats["skipped_unsure"] += 1
                    # 差分计数：本周期裁定器已有有效输出（LLM 健康），
                    # 该行对确定性输入仍反复 unsure 才计为毒丸尝试
                    if llm_seen_ok:
                        _bump_skip(skip_state, str(fact["id"]))
                    continue
                if not result["verdicts"]:
                    # Parsed output carried zero valid entries (all dropped
                    # by validation): the model produced nothing usable —
                    # treat as unsure and defer. Never fall through to
                    # _choose_action, which would blindly create a cluster.
                    stats["skipped_unsure"] += 1
                    if llm_seen_ok:
                        _bump_skip(skip_state, str(fact["id"]))
                    continue
                llm_seen_ok = True
                verdicts = result["verdicts"]

            action, cluster_id = self._choose_action(fact, candidates, verdicts)
            # 矛盾标记目标：correction/drift 裁定簇 + same 命中 replaced 簇的
            # 继任簇（重申旧说法 = 推翻此前的更正，继任簇豁免证据地板出清）
            contradict_ids = [
                cid for cid, verdict in (verdicts or [])
                if verdict in ("correction", "drift")
            ]
            contradict_ids.extend(
                self._same_replaced_successors(candidates, verdicts)
            )
            try:
                summary = await self._db.apply_fact_merge(
                    fact_id=fact["id"],
                    action=action,
                    cluster_id=cluster_id,
                    evidence_key=fact["evidence_key"],
                    occurred_at=fact["occurred_at"],
                    start_score=self._start_score(fact),
                    score_cap=cfg.score_cap,
                    promote_threshold=cfg.promote_threshold,
                    recent_promote_threshold=cfg.recent_promote_threshold,
                    contradict_cluster_ids=contradict_ids or None,
                )
            except Exception:
                # 合并侧 DB 失败同样喂熔断器（learn merge-only failure modes;
                # the periodic peek alone never records them）
                await self._record_db_failure()
                logger.warning(
                    "事实合并处置失败（fact_id=%s，顺延下周期）",
                    fact["id"],
                    exc_info=True,
                )
                continue
            await self._record_db_success()
            stats["facts"] += 1
            skip_state.pop(str(fact["id"]), None)
            # 复活簇登记回填待办（P2-9）：merge 复活 pending/dead 簇后，其
            # 存量陈述未经关系回填（水位已推过）——登记待办让下轮增量回填
            # 按id 精取补提取（backfill|{id} evidence_key 幂等，重复无害）
            if (
                summary.get("action") == "merge"
                and cluster_id is not None
                and any(
                    c["id"] == cluster_id
                    and c.get("status") in ("pending_uncertain", "dead")
                    for c in candidates
                )
            ):
                try:
                    await mark_backfill_pending(self._db, [cluster_id])
                except Exception:
                    logger.warning(
                        "复活簇回填待办登记失败（cluster_id=%s）",
                        cluster_id,
                        exc_info=True,
                    )
            key = {"merge": "merged", "create": "created", "replace": "replaced"}.get(
                summary.get("action", "")
            )
            if key is not None:
                stats[key] += 1
            logger.debug("事实处置: fact_id=%s -> %s", fact["id"], summary)

        # ---- 衰减遍 + 晋档遍 ----
        await self._maybe_decay()
        stats["promoted"] = await self._db.promote_pass(
            promote_threshold=cfg.promote_threshold,
            recent_promote_threshold=cfg.recent_promote_threshold,
        )

        # ---- 摘要归档遍（011 生命周期；周期/开关门控见方法内）----
        # fail-open（与 _fact_code_alias_pass 同款）：归档遍是可选功能且
        # 默认关闭，其异常不应中断本周期后续各遍（补编码/关系自检/语义
        # 审计/别名回填）——失败仅告警，下周期重试
        try:
            stats["summaries_archived"] = await self._maybe_summary_lifecycle()
        except Exception:
            logger.warning("摘要归档遍失败（跳过，下周期重试）", exc_info=True)
            stats["summaries_archived"] = 0

        # ---- 补编码遍（剩余预算 + 保留份额内处理降级原文行）----
        stats["reencoded"] = await self._reencode_pass(budget + reserve)
        await self._save_skip_state(_ADJUD_SKIP_KV_KEY, skip_state)

        # ---- 关系遍：结构自检（零 LLM）+ 语义审计（开关，LLM）----
        await self._relation_integrity_check()
        audited, superseded = await self._relation_audit_pass()
        stats["edges_audited"] = audited
        stats["edges_superseded"] = superseded

        # ---- 事实代号别名回填遍（零 LLM；retain 通道的周期兜底）----
        stats["fact_alias"] = await self._fact_code_alias_pass()
        return stats

    async def _fact_code_alias_pass(self) -> int:
        """Register "user<uid> (<code name>)" of active fact clusters as uid
        aliases.

        The message-flow alias upsert can only learn card-shaped names; a
        group chat's real form of address (code name / abbreviation), when
        it differs from the card (card "undefined𝕩𝕩𝕪" vs group "xxy"), is
        never learned. The "user3429924750 (xxy)" form the LLM writes into
        fact statements is the explicit uid-to-code-name binding. The
        retain channel registers increments in real time (kernel.
        _register_fact_code_aliases); this pass is the full backstop:
        existing clusters (including pre-deployment history) are filled in
        from the next cycle on, and the upsert is idempotent and
        re-entrant. The whole pass is skipped when the alias layer is
        disabled.
        """
        if self._alias_store is None:
            return 0
        try:
            sources = await self._db.fetch_fact_code_sources()
        except Exception:
            logger.warning("事实代号别名回填读取失败（下周期重试）", exc_info=True)
            return 0
        try:
            rows = build_fact_code_alias_rows(sources)
        except Exception:
            logger.warning("事实代号别名回填构建失败（下周期重试）", exc_info=True)
            return 0
        if not rows:
            return 0
        try:
            await self._db.alias_upsert(rows)
            self._alias_store.apply_rows(rows)
        except Exception:
            logger.warning("事实代号别名回填写入失败（下周期重试）", exc_info=True)
            return 0
        logger.info("事实代号别名回填完成: %d 行（source=fact）", len(rows))
        return len(rows)

    async def _reencode_pass(self, budget: int) -> int:
        """Encode-backfill pass: re-run on-device encoding on dialogue raw rows
        that were written through encode degradation.

        chat_summary rows with summarized=false do not participate in
        recall (a context-pollution guard); this pass re-encodes them once
        the LLM recovers: facts are written first (failure-direction safe:
        after the UPDATE the row is no longer scanned, so facts landing
        first matters; document_id idempotence guards duplicates), then
        the raw row is updated (summary written back, embedding recomputed
        from the summary, status flipped).

        Failure semantics: an encode downgrade (ok=False), a failure to
        write facts/relations, or a write-back error never flips status;
        the row stays summarized=false and retries next cycle (the
        fact/edge idempotency keys guarantee retries do not duplicate
        inserts). _REENCODE_FAIL_LIMIT consecutive downgrades treat the
        LLM as fully unavailable and end this pass early. The budget is
        shared with the normalization pass, deducted per-row before each
        call (mirroring the normalization pass), and stops when exhausted.

        Args:
            budget: remaining LLM call budget after the normalization pass.

        Returns:
            Number of rows successfully re-encoded (status flipped) in
            this pass.
        """
        if self._encoder is None or budget <= 0:
            return 0
        # 熔断拒绝期跳过本遍（与周期入口同判——补编码遍写库同样吃池）
        if (
            self._circuit_breaker is not None
            and not await self._circuit_breaker.peek_available()
        ):
            logger.warning("记忆库熔断中，补编码遍跳过")
            return 0
        # 超采样 + 跳过名单过滤（对齐归一化遍）：防超长降级行反复失败
        # 占据 ORDER BY id 队首，阻塞其后正常行的重编码
        skip_state = await self._load_skip_state(_REENCODE_SKIP_KV_KEY)
        target = min(budget, self._config.merge_batch_size)
        rows = await self._db.fetch_unsummarized_summaries(target * 3)
        rows = [
            r for r in rows
            if skip_state.get(str(r["id"]), 0) < _REENCODE_MAX_ATTEMPTS
        ][:target]
        if not rows:
            return 0
        logger.info("补编码遍开始: 待处理 %d 行", len(rows))

        done = 0
        fail_streak = 0
        encode_failed_ids: list[str] = []
        for row in rows:
            if budget <= 0:
                logger.info("补编码遍预算耗尽（本周期顺延）")
                break
            budget -= 1  # 调用即扣（异常路径同样消耗了 LLM 配额）
            try:
                summary, facts, relations, encoded_ok = await self._encoder.encode(
                    row["content"], self._bot_nickname, bot_user_id=self._bot_id
                )
            except Exception:
                logger.warning(
                    "补编码 LLM 调用异常（row_id=%s，顺延下周期）",
                    row["id"],
                    exc_info=True,
                )
                encode_failed_ids.append(str(row["id"]))
                continue
            if not encoded_ok:
                # LLM 仍不可用：不翻状态，连续失败达阈值则结束本遍。降级行
                # 同样计入毒丸名单（差分计数见函数尾——本遍有成功行时才累计；
                # encoder 是 fail-open 的，持续解析失败的行不会走异常路径）
                fail_streak += 1
                encode_failed_ids.append(str(row["id"]))
                if fail_streak >= _REENCODE_FAIL_LIMIT:
                    logger.warning(
                        "补编码连续 %d 次降级（LLM 不可用），本遍提前结束"
                        "（剩余行顺延下周期）",
                        fail_streak,
                    )
                    break
                continue

            fail_streak = 0
            # facts -> relations -> 回写 summary，与 retain 路径同序（summary
            # 殿后，失败方向安全）：任一写入失败即跳过回写，行保持
            # summarized=false 由下周期重试再产出——facts/边表幂等键保证
            # 重试不重复，事实不永久丢失
            try:
                await self._reingest_facts(facts, row)
                # P2 关系通道（提取开关开启时随补编码恢复）
                if relations and self._config.relation_extract_enabled:
                    await self._reingest_relations(relations, row)
            except Exception:
                logger.warning(
                    "补编码事实/关系写入失败（row_id=%s，行保持未摘要，顺延下周期）",
                    row["id"],
                    exc_info=True,
                )
                continue
            try:
                await self._db.update_chat_summary_encoded(
                    row_id=row["id"],
                    content=summary,
                    embedding=await self._embedding_service.embed_one(summary)
                    if self._embedding_service is not None
                    else None,
                )
            except Exception:
                logger.warning(
                    "补编码回写失败（row_id=%s，行保持未摘要，顺延下周期）",
                    row["id"],
                    exc_info=True,
                )
                continue
            done += 1
            skip_state.pop(str(row["id"]), None)
            logger.info("补编码完成: row_id=%s，摘要 %d 字，事实 %d 条",
                        row["id"], len(summary), len(facts))
        # 差分计数：本遍存在成功行（LLM 健康）时，失败行才计为毒丸尝试；
        # 回写失败（DB 侧）不计——那是瞬时故障不是内容问题
        if done > 0:
            for row_id in encode_failed_ids:
                _bump_skip(skip_state, row_id)
        await self._save_skip_state(_REENCODE_SKIP_KV_KEY, skip_state)
        return done

    async def _reingest_facts(self, facts: list, row: dict) -> None:
        """Batch-write the encode-backfill facts into the raw fact table
        (aligned with kernel channel semantics).

        evidence_key reuses the raw row's session_id, and the date is
        taken after local-timezone conversion (aligned with the retain
        path's naive local date basis; see the _to_local note). Facts
        about the bot itself are hard-filtered as a fallback (aligned
        with retain_encoded). Any insert failure is raised immediately
        (aligned with kernel._ingest_facts failure semantics), so the
        caller skips the summary write-back; the row stays
        summarized=false and the next cycle's encode-backfill outputs it
        again, so facts are never permanently lost (document_id
        idempotence guarantees retries do not re-insert).

        Args:
            facts: encoded persona fact list (EncodedFact) produced by
                encoding.
            row: raw summary-table row (produced by
                fetch_unsummarized_summaries; carries
                session_id/group_id/platform/occurred_at).
        """
        if not facts:
            return
        # 确定性防线：bot 自身不得成为画像主体，related 集合同步剔除 bot
        # （对齐 retain_encoded——bot uid 经 related 进簇表后会参与候选
        # 检索的关系感知归属匹配面）
        if self._bot_id:
            facts = [
                f for f in facts if f.user_id.strip() != self._bot_id
            ]
            for f in facts:
                if f.related_user_ids:
                    f.related_user_ids = [
                        r for r in f.related_user_ids
                        if (r or "").strip() != self._bot_id
                    ]
        if not facts:
            return

        if self._embedding_service is not None:
            vectors = await self._embedding_service.embed_batch(
                [f.statement for f in facts]
            )
        else:
            vectors = [None] * len(facts)

        # 幂等键日期口径与 retain 路径对齐（本地时区）：asyncpg 对
        # timestamptz 恒返回 UTC aware 值，直接 strftime 在 UTC+8 的
        # 00:00-07:59 会与 kernel 侧 naive 本地日期差一天，跨路径
        # 幂等键失配（同语句重复入库/簇重复计分）。列值仍写原 instant。
        occurred_local = self._to_local(row["occurred_at"])
        evidence_key = (
            f"{row['session_id']}|{occurred_local.strftime('%Y-%m-%d')}"
        )
        for fact, embedding in zip(facts, vectors):
            content_hash = hashlib.md5(fact.statement.encode()).hexdigest()[:12]
            uids = sorted(
                set([fact.user_id] + [r for r in fact.related_user_ids if r])
            )
            document_id = f"{fact_document_id(uids, row['session_id'], occurred_local)}-{content_hash}"
            await self._db.insert_persona_fact_raw(
                document_id=document_id,
                platform=row["platform"],
                user_id=fact.user_id,
                related_user_ids=fact.related_user_ids,
                display_name=fact.display_name,
                category=fact.category,
                statement=fact.statement,
                confidence=fact.confidence,
                session_id=row["session_id"],
                group_id=row["group_id"],
                evidence_key=evidence_key,
                occurred_at=row["occurred_at"],
                embedding=embedding,
            )

    async def _reingest_relations(
        self, relations: list[EncodedRelation], row: dict
    ) -> None:
        """Merge the relation triples produced by encode-backfill into the edge
        table (aligned with kernel channel semantics).

        evidence_key reuses the raw row's session_id plus the local date
        (same basis as _reingest_facts; re-encoding or replaying the same
        batch does not double-count). The write-side invariants align with
        the retain path: label stopword interception (label_stopwords),
        multi-form bot endpoint detection (forms learned by the kernel are
        bridged through the provider), and the endpoint placeholder-name
        guard (the alias view's latest name replaces placeholders; blank
        names are left to the upsert, which keeps the old name). The whole
        batch upserts in a single call; a failure is raised, so the caller
        skips the summary write-back and the row stays unsummarized for a
        retry next cycle (the edge-table evidence_key is idempotent, so a
        retry does not double-count).

        Args:
            relations: validated relation triples produced by encoding.
            row: raw summary-table row (carries
                session_id/platform/occurred_at).
        """
        if not relations:
            return
        occurred_local = self._to_local(row["occurred_at"])
        evidence_key = (
            f"{row['session_id']}|{occurred_local.strftime('%Y-%m-%d')}"
        )
        platform = row["platform"]
        bots = self._bot_forms()

        def _endpoint_name(uid: str, llm_name: str) -> str:
            if not is_placeholder_name(llm_name):
                return llm_name
            return self._alias_name_for(platform, uid)

        await self._db.upsert_entity_edge(
            [
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
                    "occurred_at": row["occurred_at"],
                    "evidence_key": evidence_key,
                    "is_bot_edge": edge_has_bot_endpoint(
                        rel.subject_user_id, rel.object_user_id, bots
                    ),
                    "min_evidence": self._config.relation_bot_edge_min_evidence,
                }
                for rel in relations
            ],
            label_stopwords=self._config.relation_label_stopwords,
        )
        logger.debug(
            "补编码关系入库: row_id=%s 边 %d 条",
            row["id"], len(relations),
        )

    def _to_local(self, dt: datetime) -> datetime:
        """Convert an aware datetime to the local timezone (naive values are
        returned as-is, treated as already in local basis).

        The timezone resolution chain shares its source with
        kernel._local_tz (injected via tz_provider); it falls back to the
        server's local timezone when the provider is unavailable.
        """
        if dt.tzinfo is None:
            return dt
        tz = None
        if self._tz_provider is not None:
            try:
                tz = self._tz_provider()
            except Exception:
                tz = None
        if tz is None:
            tz = datetime.now().astimezone().tzinfo
        return dt.astimezone(tz)

    async def _record_db_failure(self) -> None:
        """Feed a merge-side DB failure into the shared circuit breaker.

        The periodic-entry peek alone never records failures, so merge-only
        fault patterns would never trip the breaker without this.
        """
        if self._circuit_breaker is None:
            return
        try:
            await self._circuit_breaker.record_failure()
        except Exception:
            pass

    async def _record_db_success(self) -> None:
        """Record a merge-side DB success (resets the breaker's failure run)."""
        if self._circuit_breaker is None:
            return
        try:
            await self._circuit_breaker.record_success()
        except Exception:
            pass

    async def _load_skip_state(self, key: str) -> dict[str, int]:
        """Load the poison-row skip state ({"<row id>": consecutive failure
        count}; a failed read is treated as empty)."""
        try:
            raw = await self._db.get_kv(key)
            state = json.loads(raw) if raw else {}
            return state if isinstance(state, dict) else {}
        except Exception:
            logger.warning("跳过名单读取失败（%s，按空处理）", key, exc_info=True)
            return {}

    async def _save_skip_state(self, key: str, state: dict[str, int]) -> None:
        """Persist the skip state (over-capacity is trimmed by half-FIFO;
        failures are only logged)."""
        try:
            if len(state) > _SKIP_STATE_MAX_ENTRIES:
                state = dict(list(state.items())[_SKIP_STATE_MAX_ENTRIES // 2:])
            await self._db.set_kv(key, json.dumps(state, ensure_ascii=False))
        except Exception:
            logger.warning("跳过名单持久化失败（%s）", key, exc_info=True)

    @staticmethod
    def _same_replaced_successors(
        candidates: list[dict],
        verdicts: Optional[list[tuple[int, str]]],
    ) -> list[int]:
        """When a same verdict lands on a replaced cluster, return the id list
        of its successors (used for contradiction marking).

        A new fact restating an old claim already superseded by a
        correction semantically negates the successor cluster (the
        corrected claim); the successor gets contradicted_at and is
        exempt from the evidence floor, flushed out by decay. Marking a
        successor that is already dead/replaced is harmless (the mark only
        acts on the decay path and is cleared on revival).

        Args:
            candidates: candidate cluster list (carries the replaced_by
                field).
            verdicts: adjudication result.

        Returns:
            Successor id list (deduplicated, may be empty).
        """
        by_id = {c["id"]: c for c in candidates}
        successors: list[int] = []
        for cid, verdict in (verdicts or []):
            cluster = by_id.get(cid)
            if (
                verdict == "same"
                and cluster is not None
                and cluster.get("status") == "replaced"
                and cluster.get("replaced_by")
                and cluster["replaced_by"] not in successors
            ):
                successors.append(cluster["replaced_by"])
        return successors

    @staticmethod
    def _choose_action(
        fact: dict,
        candidates: list[dict],
        verdicts: Optional[list[tuple[int, str]]],
    ) -> tuple[str, Optional[int]]:
        """Derive the disposition action from the adjudication result
        (priority: correction > same > drift > create a new cluster).

        - correction with confidence=high and a fact not older than the
          cluster's first appearance: replace (fast track, supersedes the
          old cluster);
        - correction that misses the fast track (medium confidence or a
          time-order inversion): downgrade to creating a new cluster;
        - same: merge into the highest-similarity same cluster (candidate
          order is similarity order; scalar fallback uses last-update
          order). A fact joins exactly one cluster, preventing duplicate
          scoring across clusters;
        - drift / unrelated: create a new cluster with independent
          scoring;
        - no candidates (verdicts=None, no LLM call): creating a new
          cluster is the only path. "Candidates exist but every verdict
          was invalid" is intercepted by run_cycle as unsure (it never
          reaches this method: empty verdicts arriving here would equal
          blind cluster creation, violating the never-blindly-create
          contract);
        - a replaced cluster is never a disposition target (neither merge
          nor replace points at it): it has an explicit successor, and a
          same revival would coexist in contradiction with the new
          cluster. When a same verdict hits a replaced cluster, the caller
          marks its successor as contradicted and this path creates a new
          cluster. dead/pending_uncertain clusters can be normally revived
          by a same merge (decay-death has no semantic successor).

        Args:
            fact: raw fact row.
            candidates: candidate cluster list.
            verdicts: adjudication result (None means there were no
                candidate clusters, so no LLM call ran).

        Returns:
            (action, cluster_id) tuple (action: merge|create|replace).
        """
        if not verdicts:
            return "create", None

        cluster_map = {c["id"]: c for c in candidates}
        for cid, verdict in verdicts:
            if verdict != "correction":
                continue
            cluster = cluster_map.get(cid)
            if cluster is None or cluster.get("status") == "replaced":
                # 对已替代墓碑的更正无处置意义（说法已被推翻），跳过目标
                continue
            if fact["confidence"] == "high" and fact["occurred_at"] >= cluster["occurred_at"]:
                return "replace", cid
            logger.info(
                "更正裁定不满足快速通道（confidence=%s，fact.occurred_at=%s，"
                "cluster.occurred_at=%s），降级为建新簇: cluster_id=%s",
                fact["confidence"],
                fact["occurred_at"],
                cluster["occurred_at"],
                cid,
            )

        same_ids = [
            cid for cid, verdict in verdicts
            if verdict == "same"
            and cluster_map.get(cid, {}).get("status") != "replaced"
        ]
        if same_ids:
            order = {c["id"]: i for i, c in enumerate(candidates)}
            best = min(same_ids, key=lambda cid: order.get(cid, len(order)))
            return "merge", best
        return "create", None

    def _start_score(self, fact: dict) -> float:
        """Pick the start score from the fact's confidence (high/medium).

        Args:
            fact: raw fact row.

        Returns:
            The start score (score_start_high / score_start_medium).
        """
        if fact["confidence"] == "high":
            return float(self._config.score_start_high)
        return float(self._config.score_start_medium)

    async def _relation_integrity_check(self) -> None:
        """Relation pass, structural self-check (layer 1, a zero-LLM sentinel):
        invariant violations are counted and logged.

        Observation only, no disposition. Structural violations (an
        evidence count mismatch / a bidirectional mirror both active /
        word-length overflow) usually signal a pipeline bug or migration
        residue; silently fixing the numbers would hide the root cause.
        stale_names is a known observational item (the read side has a
        canonical-name resolution fallback, not an alert surface) and is
        excluded from the alert decision.
        """
        try:
            report = await self._db.relation_integrity_report(
                pending_stale_days=_PENDING_STALE_DAYS
            )
        except Exception:
            logger.warning("关系边结构自检失败（下周期重试）", exc_info=True)
            return
        actionable = {
            k: v for k, v in report.items() if k != "stale_names" and v
        }
        if actionable:
            logger.warning("关系边结构自检发现违例: %s（全量: %s）", actionable, report)
        else:
            logger.debug("关系边结构自检通过: %s", report)

    async def _relation_audit_pass(self) -> tuple[int, int]:
        """Relation pass, semantic audit (layer 2, gated by
        relation_audit_enabled, LLM).

        Audit active/pending edges incrementally by the id watermark (kv
        relation_audit_edge_id): one LLM call per batch of
        relation_audit_batch_size edges; edges judged bad (interaction
        description / compound form of address / statement-endpoint
        mismatch / direction contradiction) are marked superseded directly
        (a tombstone, not physically deleted, humanly recoverable) with
        the downgrade reason written into supersede_reason for the record.
        Downgrades carry an optimistic lock (the tombstone is only set
        when updated_at still matches the submitted snapshot; edges
        manually edited or given new evidence while the audit was in
        flight are skipped). The watermark only advances to the max id of
        a judged batch; an LLM failure / unsure / disposition error stops
        this pass, and the next cycle re-audits from the same batch (the
        advanced part does not roll back). Poison-batch protection: the
        same watermark batch failing/unsure for
        _RELATION_AUDIT_MAX_BATCH_FAILS consecutive times skips the batch,
        advances the watermark, and logs a warning (a manual full
        re-audit means clearing that kv value). Same entry point for a
        full re-audit.

        Budget is independent of the fact merge (at most
        _RELATION_AUDIT_MAX_CALLS batches per cycle; exhaustion defers to
        the next cycle. Edge increments are a trickle, so one normal
        cycle clears the backlog).

        Returns:
            (number of edges submitted this cycle, number of edges
            superseded).
        """
        if not self._config.relation_audit_enabled:
            return 0, 0
        if (
            self._circuit_breaker is not None
            and not await self._circuit_breaker.peek_available()
        ):
            logger.warning("记忆库熔断中，关系语义审计遍跳过")
            return 0, 0
        try:
            stored = await self._db.get_kv(_RELATION_AUDIT_KV_KEY)
            watermark = int(stored) if stored else 0
        except Exception:
            logger.warning("关系审计水位读取失败（本遍跳过）", exc_info=True)
            return 0, 0
        if watermark < 0:
            watermark = 0

        audited = 0
        superseded = 0
        calls = 0
        while calls < _RELATION_AUDIT_MAX_CALLS:
            try:
                edges = await self._db.fetch_edges_for_audit(
                    watermark, self._config.relation_audit_batch_size
                )
            except Exception:
                logger.warning(
                    "关系审计送审边拉取失败（本遍停止）", exc_info=True
                )
                break
            if not edges:
                break
            calls += 1
            result = await self._relation_auditor.audit(edges)
            if result is None or result["unsure"]:
                skip = await self._bump_audit_batch_fail(watermark)
                if skip:
                    # Poison batch: bump the watermark past it so later edges
                    # still get audited; log loudly for a manual full re-run.
                    logger.warning(
                        "关系审计批连续 %d 次失败/unsure（水位 %d 起）——跳过该批并"
                        "推水位；该批边未经审计，人工全量重审请清空 kv %s",
                        _RELATION_AUDIT_MAX_BATCH_FAILS,
                        watermark,
                        _RELATION_AUDIT_KV_KEY,
                    )
                    watermark = max(e["id"] for e in edges)
                    audited += len(edges)
                    try:
                        await self._db.set_kv(_RELATION_AUDIT_KV_KEY, str(watermark))
                    except Exception:
                        logger.warning("关系审计水位持久化失败（跳批后）", exc_info=True)
                else:
                    logger.info(
                        "关系边语义审计批拿不准/失败（水位 %d 起，顺延下周期）",
                        watermark,
                    )
                break
            await self._clear_audit_batch_fail()
            bad = [
                (eid, reason)
                for eid, verdict, reason in result["edges"]
                if verdict == "bad"
            ]
            if bad:
                try:
                    superseded += await self._db.supersede_edges(
                        [eid for eid, _ in bad],
                        expected_updated_at={
                            e["id"]: e["updated_at"] for e in edges
                        },
                        reasons={eid: reason for eid, reason in bad},
                    )
                except Exception:
                    await self._record_db_failure()
                    logger.warning(
                        "关系边降级写入失败（本遍停止，水位回退到 %d）",
                        watermark,
                        exc_info=True,
                    )
                    break
                await self._record_db_success()
                for eid, reason in bad:
                    logger.info(
                        "关系边语义审计降级: edge_id=%s reason=%s", eid, reason
                    )
            watermark = max(e["id"] for e in edges)
            audited += len(edges)
            try:
                await self._db.set_kv(_RELATION_AUDIT_KV_KEY, str(watermark))
            except Exception:
                logger.warning(
                    "关系审计水位持久化失败（本遍停止）", exc_info=True
                )
                break
        return audited, superseded

    async def _bump_audit_batch_fail(self, watermark: int) -> bool:
        """Count a failed/unsure batch at this watermark; True = skip it now.

        The mark identifies the batch by its starting watermark: a different
        batch resets the counter; reaching _RELATION_AUDIT_MAX_BATCH_FAILS
        consecutive failures for the same batch tells the caller to advance
        the watermark past it (poison-pill protection).
        """
        mark = str(watermark)
        try:
            prev_mark = await self._db.get_kv(_RELATION_AUDIT_FAIL_MARK_KEY) or ""
            count = int(await self._db.get_kv(_RELATION_AUDIT_FAIL_COUNT_KEY) or 0)
            if prev_mark != mark:
                count = 0
            count += 1
            await self._db.set_kv(_RELATION_AUDIT_FAIL_MARK_KEY, mark)
            await self._db.set_kv(_RELATION_AUDIT_FAIL_COUNT_KEY, str(count))
        except Exception:
            logger.warning("关系审计失败计数读写失败（跳批保护降级）", exc_info=True)
            return False
        return count >= _RELATION_AUDIT_MAX_BATCH_FAILS

    async def _clear_audit_batch_fail(self) -> None:
        """Reset the poison-batch counter after a successful batch."""
        try:
            await self._db.set_kv(_RELATION_AUDIT_FAIL_COUNT_KEY, "0")
        except Exception:
            pass

    async def _maybe_decay(self) -> None:
        """Decay-pass trigger decision: skip when less than one interval has
        passed since the last run.

        The last run time is persisted in the kv table (survives process
        restarts); the first run (no record) only initializes the
        timestamp and does not decay, keeping fresh/migrated data intact
        for one full interval. With decay_requires_activity enabled, the
        activity window is [last decay, now).
        """
        now = datetime.now().astimezone()
        if self._last_decay_at is None:
            stored = await self._db.get_kv(_DECAY_KV_KEY)
            if stored:
                try:
                    self._last_decay_at = datetime.fromisoformat(stored)
                except ValueError:
                    logger.warning("衰减时间戳损坏（忽略并重置）: %r", stored)
            if self._last_decay_at is None:
                await self._db.set_kv(_DECAY_KV_KEY, now.isoformat())
                self._last_decay_at = now
                return

        if now - self._last_decay_at < timedelta(days=self._config.decay_interval_days):
            return

        cfg = self._config
        stats = await self._db.decay_pass(
            decay_factor=cfg.decay_factor,
            demote_threshold=cfg.demote_threshold,
            pending_dead_days=cfg.pending_dead_days,
            recent_expire_days=cfg.recent_expire_days,
            commitment_expire_days=cfg.commitment_expire_days,
            activity_since=(
                self._last_decay_at if cfg.decay_requires_activity else None
            ),
            sticky_evidence_count=cfg.sticky_evidence_count,
            anchor_profile_size=cfg.anchor_profile_size,
        )
        self._last_decay_at = now
        await self._db.set_kv(_DECAY_KV_KEY, now.isoformat())
        logger.info("衰减遍完成: %s", stats)

    async def _maybe_summary_lifecycle(self) -> int:
        """Summary-archive-pass trigger decision (011 lifecycle): interval
        gating plus the criteria handed down to the db.

        Same freshness semantics as the decay pass: the last run time is
        kv-persisted. The first pass is deferred past the reinforce
        window. Legacy rows have no reinforcement record before deploy
        (last_recall_at is always NULL, so the criteria treat them as
        "never recalled"); if the first pass fired at the regular
        interval, high-age rows recalled within the window before the
        upgrade would be mis-archived. The initial timestamp is therefore
        pushed forward (the pass actually runs max(window, interval) days
        after the first-run checkpoint), by which point rows recalled
        within the window already carry reinforcement records and are
        exempt. Archive deadline = minimum retention + 3x half-life (the
        decay weight falls below 12.5%); rows recalled within the
        reinforce window are exempt (the criteria implementation lives in
        the db layer).

        The whole pass is skipped when summary_lifecycle_enabled is off
        (already-archived rows do not flow back; reinforcement counts
        still accumulate independently on the recall side).

        Returns:
            Number of rows archived this run (0 when the interval has not
            elapsed, the feature is off, or it is the first-run
            checkpoint).
        """
        if not self._config.summary_lifecycle_enabled:
            return 0

        now = datetime.now().astimezone()
        if self._last_summary_lifecycle_at is None:
            stored = await self._db.get_kv(_SUMMARY_LIFECYCLE_KV_KEY)
            if stored:
                try:
                    self._last_summary_lifecycle_at = datetime.fromisoformat(
                        stored
                    )
                except ValueError:
                    logger.warning(
                        "摘要归档遍时间戳损坏（忽略并重置）: %r", stored
                    )
            if self._last_summary_lifecycle_at is None:
                # 首遍延后：初始时间戳前推，首跑打点后 max(window, interval)
                # - interval 天才实际执行（见 docstring）
                first_delay_days = max(
                    self._config.summary_lifecycle_reinforce_window_days,
                    self._config.summary_lifecycle_interval_days,
                )
                first_at = now + timedelta(
                    days=first_delay_days
                    - self._config.summary_lifecycle_interval_days
                )
                await self._db.set_kv(
                    _SUMMARY_LIFECYCLE_KV_KEY, first_at.isoformat()
                )
                self._last_summary_lifecycle_at = first_at
                logger.info(
                    "摘要归档遍首次运行：初始化时间戳（首遍延后 %d 天，"
                    "存量行强化预积累），本周期不归档",
                    first_delay_days,
                )
                return 0

        interval = timedelta(days=self._config.summary_lifecycle_interval_days)
        if now - self._last_summary_lifecycle_at < interval:
            return 0

        cfg = self._config
        archive_after = int(
            cfg.summary_lifecycle_grace_days
            + 3.0 * cfg.summary_lifecycle_half_life_days
        )
        total = await self._db.archive_stale_summaries(
            archive_after_days=archive_after,
            reinforce_window_days=cfg.summary_lifecycle_reinforce_window_days,
            batch_size=200,
        )
        self._last_summary_lifecycle_at = now
        await self._db.set_kv(_SUMMARY_LIFECYCLE_KV_KEY, now.isoformat())
        logger.info("摘要归档遍完成: 归档 %d 行", total)
        return total
