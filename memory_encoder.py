"""On-device memory encoder (MemoryEncoder), vendored from the hindsight plugin.

Encodes conversation text into three parts: "faithful summary + trustworthy
person facts + optional structured relations" (one LLM call), consumed by
LocalMemoryKernel.retain_encoded's multi-channel write:
- summary goes to the memory_chat_summary table (recallable raw material);
- each fact is written independently to memory_persona_fact_raw (consumed by
  the merge agent; not part of recall);
- relations (P2, when relations_enabled is on): structured relation
  triples are written to memory_entity_edge with zero LLM merging (the bot
  may be an endpoint; isolated from the facts channel's hard bot filter).
  The extension section is a standalone prompt file memory_relations.prompt
  appended after the base prompt, isomorphic with nori-core upstream; when
  disabled, encoding behavior is verbatim-identical to the current state.

[Common contract] prompts/memory_encode.prompt follows the upstream nori
version of the same file (the upstream authoritative copy; this plugin is a
ported copy): the encoding input is isomorphic with it (including the
"history context / current batch" separator marker line); the separator
description, current-batch scope restriction, summary length cap, and the
hard rule excluding the bot's own info are common clauses; after upstream
changes, this copy must stay verbatim-synced (synced 2026-09-03: all bare
"bot" tokens in the prompt replaced with the {bot_nickname} placeholder,
both copies synced).

fail-open design: any failure in encode/parse/validate degrades to
(conversation_text, [], [], False); the caller takes the single-channel old
behavior (writing the raw text to the summary table marked summarized=false,
not participating in recall), without blocking retain.
"""

from __future__ import annotations

from core.logging_manager import get_logger

from pathlib import Path
from typing import TYPE_CHECKING, Optional

from .clients import FastLlmExit
from .contracts import EncodedFact
from .entity_edge import EncodedRelation, parse_relation
from .json_utils import safe_parse_llm_json
from .prompt_loader import PromptLoader

if TYPE_CHECKING:
    from .config import LocalMemoryConfig

logger = get_logger("noriflow_memory.encoder", "cyan")

# category 值域（画像 8 维度：主动 6（identity/stable/preference/commitment/
# interaction/naming）+ 系统 2（recent/uncertain，编码器按证据强度/时间性落））
_VALID_CATEGORIES = frozenset(
    {
        "identity", "stable", "preference", "commitment",
        "interaction", "naming", "recent", "uncertain",
    }
)
# confidence 值域
_VALID_CONFIDENCES = frozenset({"high", "medium"})
# 事实陈述长度上限（截断保留，对照关系通道 ≤200 的量级；簇
# canonical_statement 直接继承该值，不设限会放大簇表与裁定 prompt 体积）
_FACT_STATEMENT_MAX_CHARS = 200

# 解码提示词模板文件（不含 .prompt 后缀）
_ENCODE_PROMPT_NAME = "memory_encode"
# P2 关系提取扩展节模板（relations_enabled 时附加在编码提示词尾部）
_RELATIONS_PROMPT_NAME = "memory_relations"

# 编码结果 JSON Schema（tool calling 强制出口；与提示词输出格式及
# _parse_payload 校验字段一致——校验仍以 _parse_payload 为准）
_ENCODE_RESULT_SCHEMA = {
    "type": "object",
    "properties": {
        "summary": {
            "type": "string",
            "description": "压缩后的对话摘要（仅覆盖本轮批次，含 ## 氛围摘要 小节，全文不超过 300 字）",
        },
        "facts": {
            "type": "array",
            "description": "提取的人物事实列表；无可提取事实时为空数组",
            "items": {
                "type": "object",
                "properties": {
                    "user_id": {"type": "string", "description": "平台用户ID"},
                    "display_name": {"type": "string", "description": "显示名"},
                    "category": {
                        "type": "string",
                        "enum": [
                            "identity", "stable", "preference", "commitment",
                            "interaction", "naming", "recent", "uncertain",
                        ],
                    },
                    "statement": {"type": "string", "description": "简短中文陈述句"},
                    "confidence": {"type": "string", "enum": ["high", "medium"]},
                    "related_user_ids": {
                        "type": "array",
                        "items": {"type": "string"},
                        "description": "关系事实的其他参与方 user_id 列表；单用户事实留空数组",
                    },
                },
                "required": ["user_id", "category", "statement", "confidence"],
            },
        },
        "relations": {
            "type": "array",
            "description": "提取的人际关系三元组列表；无可提取关系时为空数组",
            "items": {
                "type": "object",
                "properties": {
                    "subject_user_id": {"type": "string", "description": "关系持有方平台用户ID"},
                    "subject_display_name": {"type": "string", "description": "关系持有方显示名"},
                    "object_user_id": {"type": "string", "description": "关系对象平台用户ID"},
                    "object_display_name": {"type": "string", "description": "关系对象显示名"},
                    "label": {"type": "string", "description": "2-8字中文关系短语，原文词形"},
                    "statement": {"type": "string", "description": "一句完整中文陈述句"},
                    "confidence": {"type": "string", "enum": ["high", "medium"]},
                },
                "required": ["subject_user_id", "object_user_id", "label", "statement"],
            },
        },
    },
    "required": ["summary", "facts"],
}

# 强制调用的提交工具名（LLM 将编码结果放入其 arguments）
_ENCODE_TOOL_NAME = "submit_memory_encoding"


class MemoryEncoder:
    """On-device memory encoder: conversation text -> (summary, facts).

    Loads the encoding prompt template via PromptLoader (the constructor
    accepts any directory), and does the encoding in a single call through
    FastLlmExit (run_structured exit).

    Attributes:
        llm: FastLlmExit instance (run_structured structured exit).
        prompt_dir: Encoding prompt template directory.
    """

    def __init__(
        self,
        llm: FastLlmExit,
        prompt_dir: Optional[str | Path] = None,
        input_max_chars: int = 0,
        relations_enabled: bool = False,
        config: Optional["LocalMemoryConfig"] = None,
    ) -> None:
        """Initialize the encoder.

        Args:
            llm: FastLlmExit instance (fast LLM text exit).
            prompt_dir: Prompt template directory; None defaults to this
                plugin's prompts/ directory (Path(__file__).parent / "prompts").
            input_max_chars: Encoding input char cap (0 = no truncation).
                Over-long input fed directly to the LLM will most likely
                fail/time out; after degrading to raw-text storage the
                backfill pass reruns the same poison-pill row every cycle
                and keeps failing (head-of-queue blocking), so truncation is
                the floor that keeps encoding always completable.
            relations_enabled: P2 relation-extraction switch, when on,
                appends the memory_relations extension section after the
                encoding prompt tail (an independent relations array); when
                off, the prompt is verbatim-identical to existing behavior
                (grayscale regression guarantee).
            config: Plugin runtime config (optional): when provided,
                input_max_chars / relations_enabled read this instance per
                call at runtime, taking effect immediately after saving config
                on the maintenance page; constructor params are the default
                fallback.
        """
        self.llm = llm
        self.prompt_dir = Path(prompt_dir or (Path(__file__).resolve().parent / "prompts"))
        self._config = config
        self._static_input_max_chars = max(int(input_max_chars or 0), 0)
        self._static_relations_enabled = bool(relations_enabled)
        self._loader = PromptLoader(self.prompt_dir)

    @property
    def input_max_chars(self) -> int:
        """Encoding input char cap (read at runtime from config when provided, hot-effective)."""
        if self._config is not None:
            return max(int(self._config.encode_input_max_chars or 0), 0)
        return self._static_input_max_chars

    @property
    def relations_enabled(self) -> bool:
        """P2 relation-extraction switch (read at runtime from config when provided, hot-effective)."""
        if self._config is not None:
            return bool(self._config.relation_extract_enabled)
        return self._static_relations_enabled

    async def encode(
        self,
        conversation_text: str,
        bot_nickname: str,
        bot_user_id: str = "",
    ) -> tuple[str, list[EncodedFact], list[EncodedRelation], bool]:
        """Encode conversation text as (summary, facts, relations, encoded_ok).

        Returns ("", [], [], False) directly without calling the LLM when
        the encoding input is an empty string. Any exception (LLM call
        failure / unparseable output / validation failure) is fail-open:
        return (conversation_text, [], [], False) with a warning logged, and
        the caller takes the single-channel old behavior (raw text written
        to the summary table marked summarized=false, not participating in
        recall). encoded_ok=False lets the caller distinguish "encoded
        product" from "degraded raw text": degraded-raw rows get re-encoded
        by the merge agent's backfill pass after the LLM recovers (relations
        are lost alongside facts and restored on backfill).

        Args:
            conversation_text: Conversation text with timestamps and speaker
                markers (including userid), assembled uniformly by the
                caller (main.py retain orchestration, envelope module),
                including the history-context/current-batch separator marker line.
            bot_nickname: Bot nickname (used as prompt exclusion, identifies self="true" lines).
            bot_user_id: Bot platform id (used as prompt exclusion, to
                prevent bot info from being extracted as person facts when
                user messages @-mention the bot / mention its name and
                number; in the relations channel the bot may only be an endpoint).

        Returns:
            A four-tuple (summary, facts, relations, encoded_ok): summary is
            the faithful condensed summary, facts the validated fact list,
            relations the validated relation-triple list (always empty when
            relations_enabled=False), encoded_ok=False means this output is
            degraded raw text (summary is the input text verbatim,
            facts/relations empty).
        """
        if not conversation_text:
            return "", [], [], False

        # 超长输入截断（尾部丢弃 + 标记行告知 LLM）：防用户粘贴超长文本
        # 造成编码必败 → 降级原文入库 → 补编码遍毒丸行队首阻塞
        if self.input_max_chars and len(conversation_text) > self.input_max_chars:
            dropped = len(conversation_text) - self.input_max_chars
            conversation_text = (
                conversation_text[: self.input_max_chars]
                + f"\n[系统注：输入超长，已截断丢弃末尾 {dropped} 字符]"
            )
            logger.warning(
                "编码输入超长已截断: %d -> %d 字符",
                self.input_max_chars + dropped,
                self.input_max_chars,
            )

        try:
            system_prompt = self._loader.render(
                _ENCODE_PROMPT_NAME,
                bot_nickname=bot_nickname,
                bot_user_id=bot_user_id or "未知",
            )
            if self.relations_enabled:
                system_prompt += "\n\n" + self._loader.render(
                    _RELATIONS_PROMPT_NAME,
                    bot_nickname=bot_nickname,
                    bot_user_id=bot_user_id or "未知",
                )
            raw = await self.llm.run_structured(
                system_prompt=system_prompt,
                user_prompt=conversation_text,
                schema=_ENCODE_RESULT_SCHEMA,
                tool_name=_ENCODE_TOOL_NAME,
            )
        except Exception:
            logger.warning("记忆编码 LLM 调用失败，降级为原文单通道", exc_info=True)
            return conversation_text, [], [], False

        parsed = safe_parse_llm_json(raw)
        if not isinstance(parsed, dict):
            logger.warning(
                "记忆编码输出无法解析为 JSON 对象，降级为原文单通道: %r",
                (raw or "")[:200],
            )
            return conversation_text, [], [], False

        summary, facts, relations = self._parse_payload(parsed)
        if not summary:
            # summary 缺失时降级为原文（chat_summary 通道永不丢原材料）
            logger.warning("记忆编码未产出 summary，降级为原文单通道")
            return conversation_text, [], [], False
        return summary, facts, relations, True

    def _parse_payload(
        self, payload: dict
    ) -> tuple[str, list[EncodedFact], list[EncodedRelation]]:
        """Parse and validate the payload from the encoding LLM output.

        Validates each fact: user_id/statement non-empty, category/confidence
        legal values; illegal entries are dropped and counted in a log (rather
        nothing than garbage). relations follow the same validation of each
        entry (both endpoint uids non-empty and distinct, label 2-8 chars,
        statement non-empty).

        Args:
            payload: dict successfully parsed by safe_parse_llm_json.

        Returns:
            (summary, facts, relations); summary is an empty string when
            missing/not a string, facts/relations are the validated lists.
        """
        summary = payload.get("summary")
        summary_text = summary.strip() if isinstance(summary, str) else ""

        raw_facts = payload.get("facts")
        if not isinstance(raw_facts, list):
            raw_facts = []

        facts: list[EncodedFact] = []
        discarded = 0
        for item in raw_facts:
            fact = self._parse_fact(item)
            if fact is None:
                discarded += 1
            else:
                facts.append(fact)
        if discarded:
            logger.warning("记忆编码丢弃 %d 条非法事实（共 %d 条）", discarded, len(raw_facts))

        raw_relations = payload.get("relations")
        if not isinstance(raw_relations, list):
            raw_relations = []
        relations: list[EncodedRelation] = []
        rel_discarded = 0
        for item in raw_relations:
            rel = parse_relation(item)
            if rel is None:
                rel_discarded += 1
            else:
                relations.append(rel)
        if rel_discarded:
            logger.warning(
                "记忆编码丢弃 %d 条非法关系（共 %d 条）", rel_discarded, len(raw_relations)
            )
        return summary_text, facts, relations

    @staticmethod
    def _parse_fact(item: object) -> Optional[EncodedFact]:
        """Parse a single fact dict into EncodedFact; return None for invalid fields.

        Statements longer than _FACT_STATEMENT_MAX_CHARS are truncated and
        kept (not discarded, a truncated fact is still usable; contrast the
        relation channel's parse_relation which rejects over-long entries
        wholesale). The fact channel truncates because the cluster
        canonical_statement inherits this value directly, and extreme-length
        statements would bloat cluster tables and arbitration/injection prompt size.

        Args:
            item: element of the facts array (expected dict).

        Returns:
            EncodedFact; None when user_id/statement missing, category/
            confidence out of range, or a field has the wrong type.
        """
        if not isinstance(item, dict):
            return None

        user_id = item.get("user_id")
        statement = item.get("statement")
        if not user_id or not statement:
            return None
        user_id = str(user_id).strip()
        statement = str(statement).strip()
        if not user_id or not statement:
            return None
        if len(statement) > _FACT_STATEMENT_MAX_CHARS:
            statement = statement[:_FACT_STATEMENT_MAX_CHARS]

        category = str(item.get("category") or "").strip()
        confidence = str(item.get("confidence") or "").strip()
        if category not in _VALID_CATEGORIES:
            return None
        if confidence not in _VALID_CONFIDENCES:
            return None

        display_name_raw = item.get("display_name")
        display_name = str(display_name_raw).strip() if display_name_raw else ""

        related_ids: list[str] = []
        related_raw = item.get("related_user_ids")
        if isinstance(related_raw, list):
            for uid in related_raw:
                s = str(uid).strip()
                if s and s not in related_ids:
                    related_ids.append(s)

        return EncodedFact(
            user_id=user_id,
            statement=statement,
            display_name=display_name,
            category=category,
            confidence=confidence,
            related_user_ids=related_ids,
        )
