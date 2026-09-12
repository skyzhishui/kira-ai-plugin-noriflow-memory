"""自包含数据契约（自 nori-core 记忆/画像扩展模块 vendor）。

KiraAI 无对应的核心契约层，本插件把用到的数据结构自带：
- KnowledgeType / MemoryItem：记忆条目分类与统一模型（kernel 检索返回值）；
- EncodedFact：端侧编码产出的人物事实单元（编码器 -> kernel 双通道写入）；
- PersonProfile / PersonaCandidate：画像结构与群聊多参与者候选
  （persona_service 拼装消费）。

字段语义与上游 nori 版逐字一致，便于未来数据/经验互通。
"""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime
from enum import Enum


class KnowledgeType(str, Enum):
    """记忆条目分类（认知科学记忆类型学）。

    本插件实际只使用 EPISODIC（摘要/事实通道均为情景记忆），
    其余值保留枚举完整性以对齐上游 nori 版语义。
    """

    EPISODIC = "episodic"
    SEMANTIC = "semantic"
    ATTRIBUTED = "attributed"
    VERBATIM = "verbatim"


@dataclass
class MemoryItem:
    """记忆条目统一模型。

    Attributes:
        id: 记忆后端分配的唯一标识（document_id）。
        content: 记忆正文。
        memory_category: 记忆分类，默认情景记忆。
        session_id: 归属会话 ID（空字符串表示无会话归属）。
        user_id: 关联用户 ID。
        timestamp: 记忆发生时间。
        metadata: 扩展元数据（召回结果携带的 kind、相关度等）。
        score: 检索相关性分数（召回后填充，0.0 表示未评分）。
    """

    id: str
    content: str
    memory_category: KnowledgeType = KnowledgeType.EPISODIC
    session_id: str = ""
    user_id: str = ""
    timestamp: datetime = field(default_factory=datetime.now)
    metadata: dict = field(default_factory=dict)
    score: float = 0.0


@dataclass
class EncodedFact:
    """编码产出的人物事实（retain 双通道写入的 persona_fact 单元）。

    由记忆编码器（端侧 LLM 一次调用）从对话批次提取：
    - 归属明确的长期事实才允许产出（宁缺毋滥）；
    - user_id 为平台用户 ID（从编码输入的 uid 属性引用）；
    - category 值域对应画像 6 维度：identity(基本信息)/stable(已知事实)/
      interaction(互动偏好)/naming(称呼偏好)/recent(近期动态)/uncertain(待定信息)；
    - confidence 取值 high/medium。
    """

    user_id: str
    statement: str
    display_name: str = ""
    category: str = ""
    confidence: str = "medium"
    related_user_ids: list[str] = field(default_factory=list)


@dataclass
class PersonProfile:
    """人物画像数据（六栏 + 扩展栏，persona_service 拼装/解析消费）。"""

    user_id: str
    primary_name: str = ""
    aliases: list[str] = field(default_factory=list)
    persona_backdrop: list[str] = field(default_factory=list)  # 基本信息
    addressing_style: list[str] = field(default_factory=list)  # 称呼偏好
    established_notes: list[str] = field(default_factory=list)  # 已知事实
    rapport_rules: list[str] = field(default_factory=list)  # 互动偏好
    recent_updates: list[str] = field(default_factory=list)  # 近期动态
    unverified_notes: list[str] = field(default_factory=list)  # 待定信息
    memory_points: list[str] = field(default_factory=list)  # 记忆要点


@dataclass
class PersonaCandidate:
    """群聊多参与者画像候选。

    Attributes:
        user_id: 用户 ID。
        platform: 平台标识。
        display_name: 显示名称（主称呼）。
        source: 候选来源（自由字符串，仅作日志语境，画像后端不分支依赖）。
        nickname: 用户昵称（取不到为空）。
    """

    user_id: str
    platform: str
    display_name: str = ""
    source: str = ""
    nickname: str = ""
