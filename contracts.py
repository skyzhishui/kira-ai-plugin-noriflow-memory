"""Self-contained data contracts (vendored from the nori-core memory/persona extension modules).

KiraAI has no corresponding core contract layer; this plugin carries the
data structures it uses:
- KnowledgeType / MemoryItem: memory entry classification and unified model
  (kernel retrieval return values);
- EncodedFact: person-fact units produced by on-device encoding
  (encoder -> kernel dual-channel write);
- PersonProfile / PersonaCandidate: persona structure and group-chat
  multi-participant candidates (assembled/consumed by persona_service).

Field semantics are verbatim-consistent with the upstream nori version, so
future data/experience interchange stays straightforward.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime
from enum import Enum


class KnowledgeType(str, Enum):
    """Memory entry classification (cognitive-science memory typology).

    This plugin only actually uses EPISODIC (summary/fact channels are both
    episodic memory); the remaining values are kept for enum completeness to
    align with the upstream nori version semantics.
    """

    EPISODIC = "episodic"
    SEMANTIC = "semantic"
    ATTRIBUTED = "attributed"
    VERBATIM = "verbatim"


@dataclass
class MemoryItem:
    """Unified model for memory entries.

    Attributes:
        id: Unique identifier assigned by the memory backend (document_id).
        content: Memory body text.
        memory_category: Memory category, defaults to episodic.
        session_id: Owning session ID (empty string means no session).
        user_id: Linked user ID.
        timestamp: Time the memory occurred.
        metadata: Extended metadata (kind, relevance carried by recall results).
        score: Retrieval relevance score (filled after recall; 0.0 means unscored).
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
    """Person fact produced by encoding (the persona_fact unit of the retain dual-channel write).

    Extracted by the memory encoder (a single on-device LLM call) from a
    conversation batch:
    - only long-term facts with clear attribution may be produced (rather
      nothing than garbage);
    - user_id is the platform user id (referenced from the uid attribute of
      the encoding input);
    - category values map to the persona 8 dimensions: identity (basic
      info)/stable (established facts)/preference (preferences)/
      commitment (promises)/interaction (interaction style)/naming (address
      style)/recent (recent updates)/uncertain (pending info);
    - confidence takes high/medium.
    """

    user_id: str
    statement: str
    display_name: str = ""
    category: str = ""
    confidence: str = "medium"
    related_user_ids: list[str] = field(default_factory=list)


@dataclass
class PersonProfile:
    """Person profile data (8 dimension slots + pending slot, assembled/parsed by persona_service)."""

    user_id: str
    primary_name: str = ""
    aliases: list[str] = field(default_factory=list)
    persona_backdrop: list[str] = field(default_factory=list)  # 基本信息
    addressing_style: list[str] = field(default_factory=list)  # 称呼偏好
    established_notes: list[str] = field(default_factory=list)  # 已知事实（preference 并入）
    memory_points: list[str] = field(default_factory=list)  # 记忆要点（commitment 并入）
    rapport_rules: list[str] = field(default_factory=list)  # 互动偏好
    recent_updates: list[str] = field(default_factory=list)  # 近期动态
    unverified_notes: list[str] = field(default_factory=list)  # 待定信息


@dataclass
class PersonaCandidate:
    """Group-chat multi-participant persona candidate.

    Attributes:
        user_id: User ID.
        platform: Platform identifier.
        display_name: Display name (primary address).
        source: Candidate source (free-form string, log-context only; the
            persona backend does not branch on it).
        nickname: User nickname (empty when unavailable).
    """

    user_id: str
    platform: str
    display_name: str = ""
    source: str = ""
    nickname: str = ""
