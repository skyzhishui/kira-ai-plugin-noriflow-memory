"""端侧记忆编码器（MemoryEncoder）——自 hindsight 插件 vendor。

把对话文本编码为「保真摘要 + 可信人物事实 + 可选结构化关系」三部分
（一次 LLM 调用），供 LocalMemoryKernel.retain_encoded 多通道写入消费：
- summary 走 memory_chat_summary 表（可召回原材料）；
- facts 每条独立写入 memory_persona_fact_raw 表（合并 agent 消费，不参与 recall）；
- relations（P2，relations_enabled 开启时）：结构化关系三元组零 LLM
  合并落 memory_entity_edge 表（bot 可作端点，与 facts 的 bot 硬过滤隔离）。
  扩展节为独立提示词文件 memory_relations.prompt 附加在基础提示词尾部——
  与 nori-core 上游同构；关闭时编码行为与现状逐字一致。

【通用契约】prompts/memory_encode.prompt 以上游 nori 版同名文件为准
（上游权威副本，本插件为移植副本）：
编码输入与之同构（含"历史上下文/本轮批次"分隔标记行），分隔标记说明、
本轮范围限定、摘要长度上限与 bot 自身信息排除硬规则属于通用条款，
上游修改后本副本必须同步保持逐字一致（2026-09-03 已同步：
提示词内裸 "bot" 字样全部替换为 {bot_nickname} 占位符，两副本同步修改）。

fail-open 设计：编码/解析/校验任何环节失败均降级返回
(conversation_text, [], [], False)，由调用方走单通道旧行为（原文写入摘要表并
标记 summarized=false 不参与召回），不阻断 retain。
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

# category 值域（固定对应画像 6 维度：identity/stable/interaction/naming/recent/uncertain）
_VALID_CATEGORIES = frozenset(
    {"identity", "stable", "interaction", "naming", "recent", "uncertain"}
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
                        "enum": ["identity", "stable", "interaction", "naming", "recent", "uncertain"],
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
    """端侧记忆编码器：对话文本 -> (summary, facts)。

    使用 PromptLoader 加载编码提示词模板（构造器支持任意目录），
    通过 FastLlmExit（run_structured 出口）单次调用完成编码。

    Attributes:
        llm: FastLlmExit 实例（run_structured 结构化出口）。
        prompt_dir: 编码提示词模板目录。
    """

    def __init__(
        self,
        llm: FastLlmExit,
        prompt_dir: Optional[str | Path] = None,
        input_max_chars: int = 0,
        relations_enabled: bool = False,
        config: Optional["LocalMemoryConfig"] = None,
    ) -> None:
        """初始化编码器。

        Args:
            llm: FastLlmExit 实例（fast LLM 文本出口）。
            prompt_dir: 提示词模板目录；None 时默认本插件 prompts/ 目录
                （Path(__file__).parent / "prompts"）。
            input_max_chars: 编码输入字符上限（0 = 不截断）。超长输入直送
                LLM 大概率失败/超时，降级原文入库后补编码遍每周期对同一
                毒丸行重跑再失败（队首阻塞）——截断保底让编码始终可完成。
            relations_enabled: P2 关系提取开关——开启时在编码提示词尾部
                附加 memory_relations 扩展节（relations 独立数组），关闭
                时提示词与既有行为逐字一致（灰度回归保证）。
            config: 插件运行时配置（可选）：提供时 input_max_chars /
                relations_enabled 运行时按次读该实例——维护页保存配置后
                即刻生效，无需重启；构造参数作为缺省回退。
        """
        self.llm = llm
        self.prompt_dir = Path(prompt_dir or (Path(__file__).resolve().parent / "prompts"))
        self._config = config
        self._static_input_max_chars = max(int(input_max_chars or 0), 0)
        self._static_relations_enabled = bool(relations_enabled)
        self._loader = PromptLoader(self.prompt_dir)

    @property
    def input_max_chars(self) -> int:
        """编码输入字符上限（config 提供时运行时读，支持热生效）。"""
        if self._config is not None:
            return max(int(self._config.encode_input_max_chars or 0), 0)
        return self._static_input_max_chars

    @property
    def relations_enabled(self) -> bool:
        """P2 关系提取开关（config 提供时运行时读，支持热生效）。"""
        if self._config is not None:
            return bool(self._config.relation_extract_enabled)
        return self._static_relations_enabled

    async def encode(
        self,
        conversation_text: str,
        bot_nickname: str,
        bot_user_id: str = "",
    ) -> tuple[str, list[EncodedFact], list[EncodedRelation], bool]:
        """编码对话文本为 (summary, facts, relations, encoded_ok)。

        编码输入为空串时直接返回 ("", [], [], False)，不走 LLM。
        任何异常（LLM 调用失败 / 输出不可解析 / 校验失败）均 fail-open：
        返回 (conversation_text, [], [], False) 并记录 warning，由调用方走单通道
        旧行为（原文写入摘要表并标记 summarized=false，不参与召回）。
        encoded_ok=False 供调用方区分"编码产物"与"降级原文"——降级原文行
        由合并 agent 补编码遍在 LLM 恢复后重编码（relations 随 facts 一并
        丢失并在补编码时恢复）。

        Args:
            conversation_text: 带时间戳与发言者标识（含 userid）的对话文本，
                格式由调用方（main.py retain 编排）统一构造（envelope 模块），
                含历史上下文/本轮批次分隔标记行。
            bot_nickname: Bot 昵称（用于提示词排除项，识别 self="true" 行）。
            bot_user_id: Bot 平台 ID（用于提示词排除项——用户发言中 @bot /
                提及 bot 名称与号码时，防止 bot 自身信息被提取为人物事实；
                relations 通道中 bot 仅可作端点）。

        Returns:
            (summary, facts, relations, encoded_ok) 四元组；summary 为保真
            压缩摘要，facts 为校验后的事实列表，relations 为校验后的关系
            三元组列表（relations_enabled=False 时恒空），encoded_ok=False
            表示本次产出为降级原文（summary 即输入原文、facts/relations
            为空）。
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
        """解析并校验编码 LLM 输出的 payload。

        逐条校验 facts：user_id/statement 非空、category/confidence 合法值，
        非法条目丢弃并计数记录日志（宁缺毋滥）。relations 同口径逐条校验
        （两端 uid 非空且互异、label 2-8 字、statement 非空）。

        Args:
            payload: safe_parse_llm_json 成功解析的 dict。

        Returns:
            (summary, facts, relations)；summary 缺失/非字符串时为空串，
            facts/relations 为校验后列表。
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
        """解析单条事实 dict 为 EncodedFact；字段非法返回 None。

        statement 超过 _FACT_STATEMENT_MAX_CHARS 时截断保留（不丢弃——
        截断的事实仍可用；对照关系通道 parse_relation 对超长是整条拒绝，
        事实通道选择截断是因为簇 canonical_statement 直接继承该值，
        极端长陈述会放大簇表与裁定/注入 prompt 体积）。

        Args:
            item: facts 数组元素（期望 dict）。

        Returns:
            EncodedFact；user_id/statement 缺失、category/confidence 越界
            或字段类型错误时返回 None。
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
