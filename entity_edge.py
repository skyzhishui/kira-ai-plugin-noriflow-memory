"""实体关系边（memory_entity_edge）——提取解析、注入匹配与陈述行组装。

P2（写入）：编码 pass 输出的 relations 数组经 parse_relation 校验为
EncodedRelation，零 LLM 结构键合并落库（db.upsert_entity_edge）；
P3（注入）：输入命中「节点+边」（场景 A）或「bot 边 + AT/引用门槛」
（场景 C）时组装关系陈述行（payload 主体）+ 邻居画像。

场景定义（docs/plans/noriflow-entity-graph-roadmap.md §6）：
- A（节点+边）：两层实体命中节点 N + N 的某条边 relation_label 词形
  出现在匹配文本 -> 陈述行 + 邻居画像（仅未命中节点的对端——命中者
  已走实体候选路进画像注入，重复注入浪费预算）；
- B（仅节点）：无边注入（画像走既有实体候选路，空画像不注入）；
- C（bot 边）：bot 端点边 + 本轮 AT bot/引用 bot 消息（硬门槛，由
  适配层判定为 RecallHints.bot_addressed）+ label 词形命中 -> 陈述行
  + 对端画像；裸代词"你"永不作为 bot 名匹配（bot 名不入词典/别名表，
  名字匹配层结构性不含 bot）。

label 停用表（relation_label_stopwords）：泛关系词（"朋友"等）不触发
边扩展——高频口语词与节点双命中也压不住误触发面（"姐姐我告诉你"式
非关系用法）。label 词形匹配 = label in text，与名字匹配同路数
（确定性子串，无模糊）。

本模块保持两侧字节级一致：不 import 宿主类型（画像候选组装由各侧
kernel 用自己的 PersonaCandidate 完成，见 _build_relation_section）。
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
    """bot uid 归一：str | 可迭代 -> 去空白集合。

    边端点的 bot uid 落库存在两种形态——宿主注入的 bot_id（会话标识，
    提取提示词规则明示的主形态）与平台 uid（如 QQ 号，经 @段词典渗入
    提取的副形态）。is_bot 判定（写侧 pending/激活门槛、读侧场景 C）
    统一经本函数按集合匹配，两种形态通吃。
    """
    if bot_user_id is None:
        return set()
    items = [bot_user_id] if isinstance(bot_user_id, str) else list(bot_user_id)
    return {u.strip() for u in items if str(u or "").strip()}


@dataclass
class EncodedRelation:
    """结构化关系三元组（编码 pass relations 数组元素）。

    bot 端点放开仅限本通道：subject/object 可为 bot 平台 uid——与
    facts 通道的 bot 硬过滤隔离（编码提示词排除项对 facts 不变，
    bot 自身发言仍不作任何通道的证据）。
    """

    subject_user_id: str
    object_user_id: str
    label: str
    statement: str
    confidence: str = "medium"
    subject_display_name: str = ""
    object_display_name: str = ""


def parse_relation(item: object) -> Optional[EncodedRelation]:
    """解析单条 relations 元素；字段非法返回 None（宁缺毋滥）。

    校验：两端 uid 非空且互异（自环无意义）；label 2-8 字；statement
    非空且 ≤200 字；confidence ∈ {high, medium}；display_name 可缺省；
    label 不得包含任一端点显示名（≥2 字时）——端点名被整体当作
    label 时陈述行必然破碎（"A的X是X"式同语反复），直接拒。
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
    """两端点任一为 bot uid（落库 pending 判定与激活门槛适用对象）。

    bot_user_id 接受单 uid 或集合（bot_uid_set 语义：会话标识/平台
    uid 双形态任一命中即 bot 边）。
    """
    bots = bot_uid_set(bot_user_id)
    if not bots:
        return False
    return bool(bots & {(subject_uid or "").strip(), (object_uid or "").strip()})


def split_reverse_echo_rows(
    rows: list[dict], existing_keys: set[tuple[str, str, str, str]]
) -> tuple[list[dict], int]:
    """反向回声过滤（纯函数）：镜像方向键已存在而正向键不存在时整行跳过。

    同一条关系常被双方各自陈述一次（"A 称呼 B 为主人"由双方各自视角各出一条）——结构键 (platform, subject, object, label) 带方向，直接
    落库会产出 A→B 与 B→A 两条 active 边，方向必有一错（2026-09-11 关系
    谱图审计发现 2）。规则：

    - 镜像键 (platform, object, subject, label) 已在库且正向键不在库 ->
      跳过该行（镜像行才是这条关系的在册事实）；
    - 正向键已在库 -> 照常 upsert（刷新/计分语义不变）；
    - 同批内两方向并存（库里都没有）-> 先到者胜，后者按镜像跳过。

    Args:
        rows: 待落库边行（platform/subject_uid/object_uid/relation_label）。
        existing_keys: 库中已存在的结构键集合（含正向与镜像探测结果）。

    Returns:
        (保留行列表, 跳过数)。
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
    """写侧停用 label 拦截（纯函数）：label 命中停用表的行整行丢弃。

    停用表由维护页按场景人工维护（泛关系词/互动描述词），此前只在
    注入侧生效（不触发边扩展）——命中行照常落库积累证据，直到人工
    降级。接写侧后：命中停用 label 的新边不落库，已有边（含 active）
    的新证据也不再刷新该结构键行（存量清理走语义审计遍或人工编辑）。
    与 split_reverse_echo_rows 同为 upsert 入口前置过滤，先后无耦合。
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
    """边幂等键：uid 对排序 + 会话 + 日期 + 陈述哈希。

    与边的 UNIQUE(platform, subject, object, label) 合并语义配合——同
    evidence_key 重放不涨计数；同结构键不同陈述仍并入同一行（statement
    刷新为最新证据值）。document_id 仅作 retain 返回值/日志可观测用，
    不入库（边表主键是自增 id）。
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
    bot_addressed: bool,
    stopwords: Optional[list[str]],
    max_neighbors: int,
    max_lines: int,
) -> list[dict]:
    """场景 A/C 边选择（纯函数，供注入与测试直调）。

    节点匹配为复合键（"platform:uid"，与实体命中键同构）——多适配器
    数字 uid 撞号时，跨平台别名命中不会把另一平台同号者的边当作本
    平台节点的关系。edges 预期按 evidence_count DESC, last_seen DESC
    排序（db 层 ORDER BY，选择层的确定性序）。规则：
    - 人-人边：任一端点复合键 ∈ node_keys 且 label ∈ text -> 场景 A
      （锚 = 命中端点复合键）；
    - bot 端点边：对端复合键 ∈ node_keys 且 label ∈ text -> 场景 A
      （锚 = 对端；真名锚定的 bot 关系问题安全，不受 C 门槛约束）；
      对端未命中节点时须 bot_addressed 才 -> 场景 C（锚 = 该边的 bot
      端点 uid，"@bot 你姐姐是谁"无任何名字锚点，bot 端点自身即锚）；
    - 停用 label 两场景均跳过；按边 id 去重；每锚点邻居 ≤
      max_neighbors；总行数 ≤ max_lines。

    bot_uid 接受单 uid 或集合（bot_uid_set 语义：会话标识/平台 uid
    双形态任一命中即 bot 端点边——端点是否 bot 按裸 uid 判定，复合键
    匹配在 db 层已完成）。

    Returns:
        选中边行（原 dict 副本 + _scenario/_anchor 标注），可能为空。
    """
    node_set = {k for k in (node_keys or []) if k}
    bots = bot_uid_set(bot_uid)
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
        bot_hits = bots & {subject, object_}
        if bot_hits:
            if subject in bot_hits and object_ in bot_hits:
                continue  # 两端都是 bot 形态：无对端，无意义
            other_key = object_key if subject in bot_hits else subject_key
            if other_key in node_set:
                scenario, anchor = "A", other_key
            elif bot_addressed:
                scenario = "C"
                anchor = subject if subject in bot_hits else object_
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
    edge: dict, bot_uid: object, bot_nickname: str, time_qualifier: str
) -> str:
    """陈述行：`小张(u1001)的姐姐是小李(u2002)（截至9月7日）`。

    端点名取边表冗余名（每次证据刷新为最新），缺省回退 uid；bot 端点
    回退 bot_nickname（bot_uid 为 bot_uid_set 语义集合，双形态任一
    命中即用昵称渲染）。时间限定由调用方按 last_seen 本地时区生成——
    label 存在多值语义（决策 8：不做自动互斥），LLM 依据时间自行裁决
    新旧。
    """
    subject = str(edge.get("subject_uid") or "")
    object_ = str(edge.get("object_uid") or "")
    label = str(edge.get("relation_label") or "")
    sname = str(edge.get("subject_name") or "").strip()
    oname = str(edge.get("object_name") or "").strip()
    bots = bot_uid_set(bot_uid)
    if subject and subject in bots and bot_nickname:
        sname = bot_nickname
    if object_ and object_ in bots and bot_nickname:
        oname = bot_nickname
    s = f"{sname or subject}({subject})" if subject else (sname or "?")
    o = f"{oname or object_}({object_})" if object_ else (oname or "?")
    line = f"{s}的{label}是{o}"
    return f"{line}（{time_qualifier}）" if time_qualifier else line


def neighbor_profile_uids(
    selected: list[dict],
    node_keys: list[str],
    bot_uid: object,
    max_profiles: int,
) -> list[tuple[str, str, str]]:
    """需要画像补注的对端 (platform, uid, name) 列表（独立预算）。

    只取「未命中节点的对端」——命中节点的成员已走实体候选路进画像
    注入；bot 端点跳过（bot 无画像；bot_uid 为 bot_uid_set 语义集合）。
    platform 取该边自己的平台（跨平台别名命中的对端经其所在边平台
    查画像，而非会话平台）。按 (platform, uid) 去重，保边选择序。
    """
    node_set = {k for k in (node_keys or []) if k}
    bots = bot_uid_set(bot_uid)
    seen: set[tuple[str, str]] = set()
    out: list[tuple[str, str, str]] = []
    for edge in selected or []:
        platform = str(edge.get("platform") or "")
        for uid, name in (
            (str(edge.get("subject_uid") or ""), str(edge.get("subject_name") or "")),
            (str(edge.get("object_uid") or ""), str(edge.get("object_name") or "")),
        ):
            if not uid or uid in bots or (platform, uid) in seen:
                continue
            if f"{platform}:{uid}" in node_set:
                continue
            seen.add((platform, uid))
            out.append((platform, uid, name.strip()))
            if len(out) >= max_profiles:
                return out
    return out
