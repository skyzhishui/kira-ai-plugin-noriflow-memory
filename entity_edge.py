"""Entity relation edges (memory_entity_edge): extraction parsing, injection matching, and statement line assembly.

P2 (write): the relations array output by the encoding pass is validated into EncodedRelation via
parse_relation and persisted with zero-LLM structural-key merging (db.upsert_entity_edge);
P3 (inject): when the input hits "node + edge" (scenario A) or "bot edge + AT/reference gate"
(scenario C), assembles the relation statement line (payload body) plus neighbor profiles.

Scenario definitions (docs/plans/noriflow-entity-graph-roadmap.md §6):
- A (node + edge): entity hits on both layers, node N plus an edge of N whose relation_label
  expression appears in the matched text -> statement line + neighbor profiles (only for the
  counterpart that missed the node: one that hit already feeds profile injection via the entity
  candidate path, double injection wastes budget);
- B (node only): no edge injection (profiles go the existing entity candidate path, empty profiles
  are not injected);
- C (bot edge): bot-endpoint edge + this round ATs bot / references a bot message (hard gate,
  judged by the adapter layer as RecallHints.bot_addressed) + label expression hit -> statement
  line + counterpart profile; the bare pronoun "you" is never matched as a bot name (bot names are
  not in the dictionary/alias table, the name-match layer structurally excludes bots).

label stopword table (relation_label_stopwords): general relation words ("friend" etc.) do not
trigger edge expansion: even high-frequency spoken words plus dual node hits cannot suppress the
misfire surface (non-relational uses like "Let me tell you, sister"). label expression matching =
label in text, same approach as name matching (deterministic substring, no fuzziness).

This module keeps byte-level consistency on both sides: it does not import host types (profile
candidate assembly is done by each side's kernel with its own PersonaCandidate, see
_build_relation_section).
"""

from __future__ import annotations

import hashlib
from dataclasses import dataclass
from datetime import datetime
from typing import Optional

# label 词长约束（2-8 字中文短语，从消息原文取词形）
_LABEL_MIN_CHARS = 2
_LABEL_MAX_CHARS = 8
# statement 上限（防 LLM 把整段塞进陈述行）
_STATEMENT_MAX_CHARS = 200
_VALID_CONFIDENCES = frozenset({"high", "medium"})

# P3 注入小节标题（对齐 "# 相关长期记忆" 的免责口径）
RELATION_HEADER = "# 相关人物关系（仅供背景参考，不要提及记忆来源，不要逐字复述）"


def bot_uid_set(bot_user_id: object) -> set[str]:
    """Normalize bot uids: str | iterable -> stripped set.

    Bot uids at edge endpoints are persisted in two forms: the host-injected bot_id (session
    identifier, the main form specified by the extraction prompt rules) and the platform uid (e.g. a
    QQ number, the secondary form that leaks in through the @-segment dictionary). is_bot judgment
    (write-side pending/activation gate, read-side scenario C) uniformly matches against a set
    through this function, covering both forms.
    """
    if bot_user_id is None:
        return set()
    items = [bot_user_id] if isinstance(bot_user_id, str) else list(bot_user_id)
    return {u.strip() for u in items if str(u or "").strip()}


@dataclass
class EncodedRelation:
    """Structured relation triple (an element of the encoding pass relations array).

    Bot endpoint relaxation is limited to this channel: subject/object may be bot platform uids,
    isolated from the facts channel's hard bot filter (the encoding prompt exclusions for facts stay
    unchanged; the bot's own utterances still never count as evidence on any channel).
    """

    subject_user_id: str
    object_user_id: str
    label: str
    statement: str
    confidence: str = "medium"
    subject_display_name: str = ""
    object_display_name: str = ""


def parse_relation(item: object) -> Optional[EncodedRelation]:
    """Parse a single relations element; return None when fields are invalid (better missing than wrong).

    Validation: both endpoint uids non-empty and distinct (self-loops are meaningless); label 2-8
    chars; statement non-empty and <= 200 chars; confidence in {high, medium}; display_name optional;
    label must not contain either endpoint's display name (when >= 2 chars): if an endpoint name is
    wholly used as the label the statement line is inevitably broken (a tautology like "A's X is X"),
    reject outright.
    """
    if not isinstance(item, dict):
        return None
    subject = str(item.get("subject_user_id") or "").strip()
    object_ = str(item.get("object_user_id") or "").strip()
    if not subject or not object_ or subject == object_:
        return None
    subject_display = str(item.get("subject_display_name") or "").strip()
    object_display = str(item.get("object_display_name") or "").strip()
    label = str(item.get("label") or "").strip()
    if not (_LABEL_MIN_CHARS <= len(label) <= _LABEL_MAX_CHARS):
        return None
    for name in (subject_display, object_display):
        if len(name) >= 2 and name in label:
            return None
    statement = str(item.get("statement") or "").strip()
    if not statement or len(statement) > _STATEMENT_MAX_CHARS:
        return None
    confidence = str(item.get("confidence") or "").strip() or "medium"
    if confidence not in _VALID_CONFIDENCES:
        return None
    return EncodedRelation(
        subject_user_id=subject,
        object_user_id=object_,
        label=label,
        statement=statement,
        confidence=confidence,
        subject_display_name=subject_display,
        object_display_name=object_display,
    )


def edge_has_bot_endpoint(subject_uid: str, object_uid: str, bot_user_id: object) -> bool:
    """Whether either endpoint is a bot uid (the focus of the persist-time pending judgment and the activation gate).

    bot_user_id accepts a single uid or a set (bot_uid_set semantics: either the session identifier
    or the platform uid form mapping marks the edge as a bot edge).
    """
    bots = bot_uid_set(bot_user_id)
    if not bots:
        return False
    return bool(bots & {(subject_uid or "").strip(), (object_uid or "").strip()})


def split_reverse_echo_rows(
    rows: list[dict], existing_keys: set[tuple[str, str, str, str]]
) -> tuple[list[dict], int]:
    """Reverse-echo filtering (pure function): skip the whole row when its mirrored-direction key exists but the forward-direction key does not.

    The same relation is often stated once by each party ("A calls B master", one line from each
    perspective): the structural key (platform, subject, object, label) carries a direction, so
    direct persistence would produce both an A->B and a B->A active edge, and one direction must be
    wrong (2026-09-11 relation graph audit found 2). Rules:

    - The mirrored key (platform, object, subject, label) is already in the DB and the forward key
      is not -> skip that row (the mirrored row is the registered fact for this relation);
    - The forward key is already in the DB -> upsert as usual (refresh/scoring semantics unchanged);
    - Both directions exist within the same batch (neither in the DB) -> first come first served,
      the later one is skipped as a mirror.

    Args:
        rows: edge rows to persist (platform/subject_uid/object_uid/relation_label).
        existing_keys: structural keys already in the DB (including forward and mirror probe results).

    Returns:
        (kept rows list, skipped count).
    """
    kept: list[dict] = []
    skipped = 0
    seen: set[tuple[str, str, str, str]] = set()
    for row in rows or []:
        platform = str(row.get("platform") or "")
        subject = str(row.get("subject_uid") or "")
        object_ = str(row.get("object_uid") or "")
        label = str(row.get("relation_label") or "")
        forward = (platform, subject, object_, label)
        mirror = (platform, object_, subject, label)
        if mirror in existing_keys or mirror in seen:
            if forward not in existing_keys:
                skipped += 1
                continue
        seen.add(forward)
        kept.append(row)
    return kept, skipped


def filter_stopword_rows(
    rows: list[dict], stopwords: Optional[list[str]]
) -> tuple[list[dict], int]:
    """Write-side stopword label interception (pure function): rows whose label hits the stopword table are dropped whole.

    The stopword table is maintained manually by scenario on the maintenance page (general relation
    words / interaction-description words); previously it only took effect on the injection side (no
    edge expansion triggered): hit rows still persisted and accumulated evidence until manually
    demoted. After wiring to the write side: new edges hitting a stopword label are not persisted,
    and new evidence for existing edges (including active ones) no longer refreshes that
    structural-key row (stale cleanup goes through the semantic audit pass or manual editing). Like
    split_reverse_echo_rows, it is a pre-filter at the upsert entry; the two have no ordering
    dependency.
    """
    stopped = {w for w in (stopwords or []) if w}
    kept: list[dict] = []
    skipped = 0
    for row in rows or []:
        if str(row.get("relation_label") or "") in stopped:
            skipped += 1
            continue
        kept.append(row)
    return kept, skipped


def relation_document_id(
    rel: EncodedRelation, session_id: str, occurred_at: datetime
) -> str:
    """Edge idempotency key: sorted uid pair + session + date + statement hash.

    Cooperates with the edge UNIQUE(platform, subject, object, label) merge semantics: replaying the
    same evidence_key does not inflate the count; different statements under the same structural key
    still merge into one row (statement refreshes to the latest evidence value). document_id is only
    for the retain return value / log observability, not persisted (the edge table's primary key is
    an autoincrement id).
    """
    pair = "-".join(sorted([rel.subject_user_id, rel.object_user_id]))
    date = occurred_at.strftime("%Y%m%d")
    digest = hashlib.md5(rel.statement.encode()).hexdigest()[:12]
    return f"rel-{pair}-{session_id}-{date}-{digest}"


def select_relation_edges(
    *,
    text: str,
    edges: list[dict],
    node_keys: list[str],
    bot_uid: object,
    session_platform: str,
    bot_addressed: bool,
    stopwords: Optional[list[str]],
    max_neighbors: int,
    max_lines: int,
) -> list[dict]:
    """Scenario A/C edge selection (pure function, for direct injection and test calls).

    Node matching uses composite keys ("platform:uid", isomorphic to entity-hit keys): when numeric
    uids collide across adapters, a cross-platform alias hit will not take another platform's
    same-number user's edge as this platform node's relation. edges are expected sorted by
    evidence_count DESC, last_seen DESC (db-layer ORDER BY, the selection layer's deterministic
    order). Rules:
    - Human-human edge: either endpoint composite key in node_keys and label in text -> scenario A
      (anchor = the hit endpoint composite key);
    - Bot-endpoint edge: the counterpart composite key in node_keys and label in text -> scenario A
      (anchor = the counterpart; a real-name-anchored bot relation is safe and not bound by the C gate);
      when the counterpart misses the node, bot_addressed is required -> scenario C (anchor = that
      edge's bot endpoint composite key; "@bot, who is your sister" has no name anchor, the bot
      endpoint itself is the anchor);
    - Stopword labels are skipped in both scenarios; dedup by edge id; per-anchor neighbors <=
      max_neighbors; total lines <= max_lines.

    bot_uid accepts a single uid or a set (bot_uid_set semantics: either the session identifier or
    platform uid form mapping marks a bot-endpoint edge). Bot endpoint judgment uses composite keys:
    the form x session_platform key is compared against the edge endpoint composite key: another
    platform's same-number user's edge will not be misjudged as a bot edge by a bare-uid collision
    (with the db layer no longer filtering by platform, this is the python-layer platform check bit).
    session_platform is required: omitting it raises TypeError, silently falling back to a bare-uid
    judgment is not allowed.

    Returns:
        Selected edge rows (original dict copies annotated with _scenario/_anchor), possibly empty.
    """
    node_set = {k for k in (node_keys or []) if k}
    bot_keys = {
        f"{session_platform}:{u}" for u in bot_uid_set(bot_uid)
    }
    stopped = {w for w in (stopwords or []) if w}
    selected: list[dict] = []
    seen_ids: set = set()
    neighbor_count: dict[str, int] = {}
    for edge in edges or []:
        if len(selected) >= max_lines:
            break
        eid = edge.get("id")
        if eid is not None and eid in seen_ids:
            continue
        label = str(edge.get("relation_label") or "")
        if not label or label in stopped or label not in text:
            continue
        platform = str(edge.get("platform") or "")
        subject = str(edge.get("subject_uid") or "")
        object_ = str(edge.get("object_uid") or "")
        subject_key = f"{platform}:{subject}"
        object_key = f"{platform}:{object_}"
        subject_bot = subject_key in bot_keys
        object_bot = object_key in bot_keys
        if subject_bot or object_bot:
            if subject_bot and object_bot:
                continue  # 两端都是 bot 形态：无对端，无意义
            other_key = object_key if subject_bot else subject_key
            if other_key in node_set:
                scenario, anchor = "A", other_key
            elif bot_addressed:
                scenario = "C"
                anchor = subject_key if subject_bot else object_key
            else:
                continue
        elif subject_key in node_set:
            scenario, anchor = "A", subject_key
        elif object_key in node_set:
            scenario, anchor = "A", object_key
        else:
            continue
        if neighbor_count.get(anchor, 0) >= max_neighbors:
            continue
        neighbor_count[anchor] = neighbor_count.get(anchor, 0) + 1
        row = dict(edge)
        row["_scenario"] = scenario
        row["_anchor"] = anchor
        selected.append(row)
        if eid is not None:
            seen_ids.add(eid)
    return selected


def relation_statement_line(
    edge: dict, bot_uid: object, session_platform: str,
    bot_nickname: str, time_qualifier: str,
) -> str:
    """Statement line: `Xiao Zhang(u1001)'s sister is Xiao Li(u2002) (as of Sep 7)`.

    Endpoint names come from the edge table's redundant names (refreshed to the latest on each
    evidence refresh), falling back to the uid when missing; bot endpoints fall back to
    bot_nickname: bot judgment uses the composite key (bot_uid form x session_platform, same
    convention as select_relation_edges / neighbor_profile_uids, so a same-number real person on
    another platform is not rendered as a bot nickname). The time qualifier is generated by the
    caller in last_seen's local timezone: label has multi-value semantics (decision 8: no automatic
    mutual exclusion), the LLM decides old vs new by the time itself.
    """
    subject = str(edge.get("subject_uid") or "")
    object_ = str(edge.get("object_uid") or "")
    label = str(edge.get("relation_label") or "")
    sname = str(edge.get("subject_name") or "").strip()
    oname = str(edge.get("object_name") or "").strip()
    platform = str(edge.get("platform") or "")
    bot_keys = {
        f"{session_platform}:{u}" for u in bot_uid_set(bot_uid)
    }
    if subject and f"{platform}:{subject}" in bot_keys and bot_nickname:
        sname = bot_nickname
    if object_ and f"{platform}:{object_}" in bot_keys and bot_nickname:
        oname = bot_nickname
    s = f"{sname or subject}({subject})" if subject else (sname or "?")
    o = f"{oname or object_}({object_})" if object_ else (oname or "?")
    line = f"{s}的{label}是{o}"
    return f"{line}（{time_qualifier}）" if time_qualifier else line


def neighbor_profile_uids(
    selected: list[dict],
    node_keys: list[str],
    bot_uid: object,
    session_platform: str,
    max_profiles: int,
) -> list[tuple[str, str, str]]:
    """List of counterparts (platform, uid, name) needing profile top-ups (independent budget).

    Only counterparts that missed the node are taken: members that hit a node already go into
    profile injection via the entity candidate path; bot endpoints are skipped (bots have no
    profiles; bot endpoint judgment uses the composite key: bot_uid form x session_platform, same
    convention as select_relation_edges: a same-number real person on another platform is not
    wrongly excluded by a bare-uid collision). platform is the edge's own platform (a counterpart
    hit via a cross-platform alias looks up the profile by its edge's platform, not the session
    platform). Dedup by (platform, uid), preserving the edge selection order.
    """
    node_set = {k for k in (node_keys or []) if k}
    bot_keys = {
        f"{session_platform}:{u}" for u in bot_uid_set(bot_uid)
    }
    seen: set[tuple[str, str]] = set()
    out: list[tuple[str, str, str]] = []
    for edge in selected or []:
        platform = str(edge.get("platform") or "")
        for uid, name in (
            (str(edge.get("subject_uid") or ""), str(edge.get("subject_name") or "")),
            (str(edge.get("object_uid") or ""), str(edge.get("object_name") or "")),
        ):
            if (
                not uid
                or f"{platform}:{uid}" in bot_keys
                or (platform, uid) in seen
            ):
                continue
            if f"{platform}:{uid}" in node_set:
                continue
            seen.add((platform, uid))
            out.append((platform, uid, name.strip()))
            if len(out) >= max_profiles:
                return out
    return out
