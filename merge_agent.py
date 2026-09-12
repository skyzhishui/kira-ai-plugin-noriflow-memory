"""合并 agent：四遍扫描 + LLM 四分类裁定 + 评分状态机编排（开发方案 §8）。

每 merge_interval_hours 周期执行一次 run_cycle：
1. 归一化遍：raw 表 flag=0 事实 -> 同 (platform, user, category) 簇候选
   （向量 top-K，含墓碑簇；事实无向量时标量退化）-> LLM 四分类裁定
   （same/drift/correction/unrelated）-> 单事务处置 + 翻 flag；
2. 衰减遍：每 decay_interval_days 一次（上次执行时间 kv 持久化，重启不丢；
   首次运行只初始化时间戳不衰减）；
3. 晋档遍：达阈值 active 簇 -> profiled + 画像表 upsert（recent 低阈值）；
4. 补编码遍：编码降级期写入摘要表的对话原文行（summarized=false，不参与
   召回）重跑端侧编码——summary 回写原行（翻状态、向量重算），facts 走
   fact 原始表正常通道（evidence_key 沿用原行 occurred_at）；
5. 关系遍：结构自检（零 LLM 哨兵，违例计数记日志）+ 语义审计（开关
   relation_audit_enabled，LLM 按 id 水位增量审计 active/pending 边，
   判定 bad 直接置 superseded）。

可靠性：
- LLM 调用预算 llm_budget_per_cycle 限流，超出顺延下周期；
- 裁定 unsure / LLM 失败 / 处置异常均不翻 flag（下周期重处理，幂等由
  evidence_key 去重 + 乐观锁双重兜底）；
- 补编码遍：编码或写入失败不翻状态（行保持 summarized=false 下周期
  重试，facts/边表幂等键保证重试不重复入库），连续 3 次 ok=False 视为
  LLM 整体不可用，提前结束本遍不烧剩余预算；
- 语义审计遍：批失败/unsure 即停（水位停在最后成功批，下周期同批重审；
  同一批连续 3 次失败/unsure 跳批推水位——毒丸批不得钉死水位），
  每周期调用次数独立上限，耗尽顺延；
- 处置数值计算全部下沉 db 层 SQL（见 db.apply_fact_merge），本层只做
  裁定解读与动作选择。
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
from .alias_store import is_placeholder_name
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
    """毒丸行失败计数 +1（达到上限后调用方过滤不再拉取该行）。"""
    state[row_id] = state.get(row_id, 0) + 1


class FactAdjudicator:
    """LLM 四分类裁定器：新事实 vs 候选簇列表 -> 逐簇 verdict。

    fail-soft：LLM 调用失败 / 输出不可解析时返回 None，由调用方跳过
    本条事实（不翻 flag，下周期重审），绝不盲目建簇。

    Attributes:
        llm: FastLlmExit 实例（run_structured 出口）。
        prompt_dir: 裁定提示词模板目录。
    """

    def __init__(
        self,
        llm: FastLlmExit,
        prompt_dir: Optional[str | Path] = None,
    ) -> None:
        """初始化裁定器。

        Args:
            llm: FastLlmExit 实例（fast LLM 文本出口）。
            prompt_dir: 提示词目录；None 时默认本插件 prompts/ 目录。
        """
        self.llm = llm
        self.prompt_dir = Path(
            prompt_dir or (Path(__file__).resolve().parent / "prompts")
        )
        self._loader = PromptLoader(self.prompt_dir)

    async def adjudicate(
        self, fact: dict, candidates: list[dict]
    ) -> Optional[dict]:
        """裁定新事实与全部候选簇的关系（一次 LLM 调用）。

        Args:
            fact: raw 事实行（fetch_pending_facts 产出）。
            candidates: 候选簇列表（search_cluster_candidates 产出）。

        Returns:
            {"verdicts": [(cluster_id, verdict), ...], "unsure": bool}；
            LLM 失败 / 输出不可解析时 None（调用方跳过本条）。
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
    """关系边语义审计器：一批 active/pending 边 -> 逐边 ok/bad 判定。

    fail-soft：LLM 调用失败 / 输出不可解析 / unsure 时返回 None 或
    unsure 标记，由调用方停止本遍（不推水位，下周期从同批重审），
    绝不盲目降级。

    Attributes:
        llm: FastLlmExit 实例（run_structured 出口）。
        prompt_dir: 审计提示词模板目录。
    """

    def __init__(
        self,
        llm: FastLlmExit,
        prompt_dir: Optional[str | Path] = None,
    ) -> None:
        """初始化审计器。

        Args:
            llm: FastLlmExit 实例（fast LLM 文本出口）。
            prompt_dir: 提示词目录；None 时默认本插件 prompts/ 目录。
        """
        self.llm = llm
        self.prompt_dir = Path(
            prompt_dir or (Path(__file__).resolve().parent / "prompts")
        )
        self._loader = PromptLoader(self.prompt_dir)

    async def audit(self, edges: list[dict]) -> Optional[dict]:
        """判定一批边是否为合格的关系边（一次 LLM 调用）。

        Args:
            edges: 待审边行（fetch_edges_for_audit 产出）。

        Returns:
            {"edges": [(edge_id, verdict, reason), ...], "unsure": bool}；
            LLM 失败 / 输出不可解析时 None（调用方停遍）。
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
    """四遍扫描合并 agent（后台 asyncio 任务，生命周期归插件 initialize/terminate）。

    依赖 FastLlmExit（LLM 裁定）：缺失时不应启动本 agent——盲目建簇会造成
    簇爆炸，raw 事实保持 flag=0 堆积等待（数据不丢）。
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
    ) -> None:
        """初始化（不启动任务）。

        Args:
            db: 记忆库访问层。
            llm: FastLlmExit 实例（LLM 裁定出口）。
            config: 合并 agent 与评分状态机配置。
            prompt_dir: 裁定提示词目录；None 时默认本插件 prompts/ 目录。
            encoder: 端侧记忆编码器（补编码遍消费；None 时补编码遍跳过，
                降级原文行保持 summarized=false 不参与召回）。
            embedding_service: 向量计算服务（补编码 facts 入表向量化用）。
            bot_nickname: Bot 昵称（编码器提示词排除项）。
            bot_id: Bot 平台 ID（编码器提示词排除项 + facts 硬过滤兜底）。
            circuit_breaker: 记忆库熔断器（None 时周期任务不做事前熔断
                检查——DB 故障期每周期空转重试；传入时拒绝期整周期跳过，
                不烧 LLM 预算）。
            tz_provider: 本地时区读取器（kernel._local_tz 同源；补编码
                幂等键的日期口径对齐 retain 路径用。None 回退服务器本地）。
            bot_forms_provider: bot uid 全形态集合读取器（kernel.
                _bot_uid_forms_all 语义——补编码关系通道的 bot 端点判定
                与 retain 通道对齐；None 退化为单形态 self._bot_id）。
            alias_name_resolver: (platform, uid) -> 最新非占位别名（写侧
                占位名守卫的顶替源，与 kernel._endpoint_name 同源；
                None 时空名留给 upsert 保旧名）。
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
        # 周期参数运行时读 self._config（维护页保存配置后热生效，无需重启）
        self._task: Optional[asyncio.Task] = None
        self._last_decay_at: Optional[datetime] = None
        # WebUI manual-trigger state: the inter-cycle wait doubles as a
        # kickable event so a manual request short-circuits the interval.
        # The loop stays the only runner — a kicked cycle can never
        # overlap the periodic one.
        self._kick_event: Optional[asyncio.Event] = None
        self._in_cycle = False
        self._last_cycle: Optional[dict] = None

    def _bot_forms(self) -> list[str]:
        """Bot uid 全形态集合（provider 优先；退化为单形态 self._bot_id）。"""
        if self._bot_forms_provider is not None:
            try:
                forms = [f for f in (self._bot_forms_provider() or []) if f]
            except Exception:
                forms = []
            if forms:
                return forms
        return [self._bot_id] if self._bot_id else []

    def _alias_name_for(self, platform: str, uid: str) -> str:
        """占位名顶替源：别名视图该 uid 的最新可用名（未命中返回空串）。"""
        if self._alias_name_resolver is None:
            return ""
        try:
            return self._alias_name_resolver(platform or "", uid or "") or ""
        except Exception:
            return ""

    @property
    def bot_id(self) -> str:
        """Bot 平台 ID（宿主插件学到真实 self_id 后运行期回填）。"""
        return self._bot_id

    @bot_id.setter
    def bot_id(self, value: str) -> None:
        """回填真实 Bot 平台 ID（facts 硬过滤 + 补编码 prompt 渲染消费）。"""
        self._bot_id = (value or "").strip()

    @property
    def bot_nickname(self) -> str:
        """Bot 昵称（宿主 persona 热切换后运行期回填）。"""
        return self._bot_nickname

    @bot_nickname.setter
    def bot_nickname(self, value: str) -> None:
        """回填昵称（补编码 prompt 的 bot 排除规则消费）。"""
        self._bot_nickname = (value or "").strip()

    @property
    def running(self) -> bool:
        """任务是否在运行。"""
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
        """启动后台任务（幂等：已运行时跳过）。"""
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
        """停止后台任务（幂等，等待当前周期退出）。"""
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
        """任务主循环：启动缓冲后先跑一轮，此后按周期执行，异常只记录不退出。"""
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
        """执行单个合并周期（四遍扫描）。

        Returns:
            统计 dict：facts/merged/created/replaced/skipped_unsure/
            deferred/llm_calls/promoted/reencoded。
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
                if budget <= 0:
                    stats["deferred"] += 1
                    continue
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

        # ---- 补编码遍（剩余预算 + 保留份额内处理降级原文行）----
        stats["reencoded"] = await self._reencode_pass(budget + reserve)
        await self._save_skip_state(_ADJUD_SKIP_KV_KEY, skip_state)

        # ---- 关系遍：结构自检（零 LLM）+ 语义审计（开关，LLM）----
        await self._relation_integrity_check()
        audited, superseded = await self._relation_audit_pass()
        stats["edges_audited"] = audited
        stats["edges_superseded"] = superseded
        return stats

    async def _reencode_pass(self, budget: int) -> int:
        """补编码遍：对编码降级写入的对话原文行重跑端侧编码。

        summarized=false 的 chat_summary 行不参与召回（防上下文污染），
        由本遍在 LLM 恢复后重编码：先写 facts（失败方向安全——UPDATE 后
        该行不再被扫描，先保 facts 落库；document_id 幂等兜底重复），再
        UPDATE 原行（summary 回写 + 向量按摘要重算 + 翻状态）。

        失败语义：encode 降级（ok=False）、facts/关系写入失败或回写异常均
        不翻状态，行保持 summarized=false 下周期重试（facts/边表幂等键
        保证重试不重复入库）；连续 _REENCODE_FAIL_LIMIT 次降级视为
        LLM 整体不可用，提前结束本遍。预算与归一化遍共享，逐行调用前扣减
        （对齐归一化遍模式），耗尽即止。

        Args:
            budget: 归一化遍消耗后剩余的 LLM 调用预算。

        Returns:
            本遍成功重编码（翻状态）的行数。
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
        """补编码产出的事实批量写入 fact 原始表（对齐 kernel 通道语义）。

        evidence_key 沿用原行 session_id，日期按本地时区换算后取值（与
        retain 路径的 naive 本地日期口径对齐，见 _to_local 注释）；bot
        自身事实硬过滤兜底（对齐 retain_encoded）；任一条插入失败立即
        上抛（对齐 kernel._ingest_facts 的失败语义）——调用方据此跳过
        summary 回写，行保持 summarized=false 由下周期补编码重试再产出，
        事实不永久丢失（document_id 幂等保证重试不重复入表）。

        Args:
            facts: 编码产出的人物事实列表（EncodedFact）。
            row: 摘要表原行（fetch_unsummarized_summaries 产出，含
                session_id/group_id/platform/occurred_at）。
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
        """补编码产出的关系三元组合并落边表（对齐 kernel 通道语义）。

        evidence_key 沿用原行 session_id + 本地日期（与 _reingest_facts
        同口径——同批重编码/重放不重复计数）；写侧三项不变式与 retain 路
        对齐——label 停用表拦截（label_stopwords）、bot 端点多形态集合
        判定（kernel 学习形态经 provider 桥接）、端点占位名守卫（别名
        视图最新名顶替，空名留给 upsert 保旧名）。整批单次 upsert，失败
        上抛——调用方跳过 summary 回写，行保持未摘要由下周期重试（边表
        evidence_key 幂等，重试不重复计数）。

        Args:
            relations: 编码产出并校验后的关系三元组列表。
            row: 摘要表原行（含 session_id/platform/occurred_at）。
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
        """aware 时间转本地时区（naive 原样返回，视为已是本地口径）。

        时区解析链与 kernel._local_tz 同源（tz_provider 注入），provider
        不可用时回退服务器本地时区。
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
        """读取毒丸行跳过状态（{"<行id>": 连续失败次数}；读取失败视为空）。"""
        try:
            raw = await self._db.get_kv(key)
            state = json.loads(raw) if raw else {}
            return state if isinstance(state, dict) else {}
        except Exception:
            logger.warning("跳过名单读取失败（%s，按空处理）", key, exc_info=True)
            return {}

    async def _save_skip_state(self, key: str, state: dict[str, int]) -> None:
        """持久化跳过状态（超容量对半 FIFO 截断；失败只记日志）。"""
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
        """same 裁定命中 replaced 簇时，返回其继任簇 id 列表（打矛盾标记用）。

        新事实重申已被更正替代的旧说法，语义上是对继任簇（更正后的说法）
        的否定——继任簇打 contradicted_at 豁免证据地板，随衰减出清；即使
        继任簇已 dead/replaced 再被标记也无害（标记只作用于衰减路径，且
        复活时会被清除）。

        Args:
            candidates: 候选簇列表（含 replaced_by 字段）。
            verdicts: 裁定结果。

        Returns:
            继任簇 id 列表（去重，可能为空）。
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
        """从裁定结果派生处置动作（优先级 correction > same > drift > 建新簇）。

        - correction 且 confidence=high 且新事实不早于簇首次出现：
          replace（快速通道，替代旧簇）；
        - correction 不满足快速通道（medium 置信或时序倒置）：降级建新簇；
        - same：并入相似度最高的 same 簇（候选序即相似度序，标量退化时为
          最近更新序）——一条事实只入一个簇，防多簇重复计分；
        - drift / unrelated：建新簇独立计分；
        - 候选为空（verdicts=None，未走 LLM）：建新簇是唯一出路；
          「有候选但裁定全非法」由 run_cycle 拦截为 unsure（不落入本
          方法——空 verdicts 直达此处等于盲目建簇，违反绝不盲目建簇契约）；
        - replaced 簇不作为任何处置目标（merge/replace 都不指向它）：它有
          明确继任簇，same 复活会与新簇矛盾并存；same 命中 replaced 时
          由调用方对继任簇打矛盾标记，本条走建新簇。dead/pending_uncertain
          簇可正常 same 入簇复活（衰减死亡无语义继任者）。

        Args:
            fact: raw 事实行。
            candidates: 候选簇列表。
            verdicts: 裁定结果（None 表示无候选簇，未走 LLM）。

        Returns:
            (action, cluster_id) 二元组（action: merge|create|replace）。
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
        """按事实置信度取起步分（high/medium）。

        Args:
            fact: raw 事实行。

        Returns:
            起步分（score_start_high / score_start_medium）。
        """
        if fact["confidence"] == "high":
            return float(self._config.score_start_high)
        return float(self._config.score_start_medium)

    async def _relation_integrity_check(self) -> None:
        """关系遍·结构自检（第一层，零 LLM 哨兵）：不变式违例计数记日志。

        只观测不处置——结构性违例（证据计数错位/双向镜像 active/词长
        越界）通常意味着管线 bug 或迁移遗留，静默修数会掩盖根因。
        stale_names 是已知观测项（读侧有规范名解析兜底、非告警面），
        不计入告警判定。
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
        """关系遍·语义审计（第二层，开关 relation_audit_enabled，LLM）。

        按 id 水位（kv relation_audit_edge_id）增量审计 active/pending
        边：每批 relation_audit_batch_size 条一次 LLM 调用，判定 bad
        （互动描述/称呼复合词/陈述与端点错位/方向矛盾）的边直接置
        superseded（墓碑不物理删，可人工恢复），降级原因写入
        supersede_reason 留痕。降级带乐观锁（updated_at 与送审快照一致
        才置墓碑——审计飞行期间被人工改过/收到新证据的边跳过）。水位
        只推到已判定批的最大 id；LLM 失败 / unsure / 处置异常即停本遍，
        下周期从同批重审（已推部分不回退）。毒丸批防护：同一水位批连续
        _RELATION_AUDIT_MAX_BATCH_FAILS 次失败/unsure 即跳批推水位并记
        warning（需人工全量重审 = 清空该 kv 值）。全量重审入口同。

        预算独立于事实合并（每周期最多 _RELATION_AUDIT_MAX_CALLS 批，
        耗尽顺延；边增量为 trickle，常态一轮即清完积压）。

        Returns:
            (本周期送审边数, 降级 superseded 边数)。
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
        """衰减遍触发判定：距上次执行不足一个周期则跳过。

        上次执行时间经 kv 表持久化（进程重启不丢）；首次运行（无记录）
        只初始化时间戳不衰减——新装/迁移数据保真一个完整周期。
        开启 decay_requires_activity 时活跃窗口取 [上次衰减, now)。
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
            activity_since=(
                self._last_decay_at if cfg.decay_requires_activity else None
            ),
            sticky_evidence_count=cfg.sticky_evidence_count,
            anchor_profile_size=cfg.anchor_profile_size,
        )
        self._last_decay_at = now
        await self._db.set_kv(_DECAY_KV_KEY, now.isoformat())
        logger.info("衰减遍完成: %s", stats)
