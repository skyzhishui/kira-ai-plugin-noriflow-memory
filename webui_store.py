"""记忆维护 API 的共享数据访问层（双后端拆分，方案 §6）。

本模块保留与方言无关的部分：常量/白名单、分页与过滤参数校验、行装配
（_dt 时间序列化等纯 Python 逻辑），以及方言完全一致的 SQL；方言分歧的
SQL 体下沉到 webui_store_pg.py / webui_store_sqlite.py（函数一一对应），
按 ``backend.dialect`` 派发。查询/分页/修正 SQL 与 nori 上游版逐字一致
（PG 路）；错误经 fastapi HTTPException 上抛。
"""

from __future__ import annotations

from core.logging_manager import get_logger

from datetime import datetime
from typing import Optional

from fastapi import HTTPException

from .alias_store import is_placeholder_name
from .db import build_search_text
from .relation_backfill import mark_backfill_pending

logger = get_logger("noriflow_memory.webui", "cyan")

# 画像六维（原始事实/簇的 category 值域）
_CATEGORIES = frozenset(
    {"identity", "stable", "interaction", "naming", "recent", "uncertain"}
)
# 簇状态机值域（WebUI 可手工设置的终态；replaced 为合并管线专属墓碑，
# 只读可见不可手设——手迁出会破坏继任链 replaced_by 语义）
_CLUSTER_STATUSES = frozenset({"active", "profiled", "pending_uncertain", "dead"})
# 簇状态过滤白名单（查询侧；含 replaced——待定栏数据源含 replaced 陈述，
# 管理员需能按状态筛到这些行）
_CLUSTER_STATUS_FILTER = frozenset({"active", "profiled", "pending_uncertain", "dead", "replaced"})
# 关系边状态值域（人工可改的全部三态；置 superseded 后新证据不再自动激活
# ——upsert 墓碑保护，恢复走人工置回）
_EDGE_STATUSES = frozenset({"active", "pending", "superseded"})

# 画像投影 upsert（updated_at 由方言分支注入：PG now() / SQLite $8 参数）
_PROFILE_UPSERT_SQL = """
INSERT INTO memory_user_profile
    (platform, user_id, category, cluster_id, statement,
     score, related_user_ids)
VALUES ($1, $2, $3, $4, $5, $6, $7)
ON CONFLICT (platform, user_id, category, cluster_id)
DO UPDATE SET statement = excluded.statement,
              score = excluded.score,
              related_user_ids = excluded.related_user_ids,
              updated_at = {updated_at}
"""


def _dialect(backend) -> str:
    """后端方言（测试桩无 dialect 属性时按 postgres 处理）。"""
    return getattr(backend, "dialect", "postgres")


def _pool(backend):
    """连接池解析：backend 对象取 .pool；裸池（旧调用方/测试桩）原样。"""
    return backend.pool if hasattr(backend, "pool") else backend


def _impl(backend):
    """按后端方言取 SQL 体实现模块（惰性导入避免环）。"""
    if _dialect(backend) == "sqlite":
        from . import webui_store_sqlite as impl

        return impl
    from . import webui_store_pg as impl

    return impl


def _decode_backend_rows(backend, rows) -> list[dict]:
    """SQLite 行统一解码（时间/JSON/布尔/向量），PG 行 dict 原样。"""
    if _dialect(backend) == "sqlite":
        from .db.sqlite import _decode_rows

        return _decode_rows(rows)
    return [dict(r) for r in rows]


def _like_op(backend) -> str:
    """关键词匹配运算符（SQLite LIKE 对 ASCII 恒不区分大小写）。"""
    return "LIKE" if _dialect(backend) == "sqlite" else "ILIKE"


def _escape_like(keyword: str) -> str:
    """LIKE 模式元字符转义（\\ % _ → 字面量；默认转义符为 \\）。"""
    return keyword.replace("\\", "\\\\").replace("%", "\\%").replace("_", "\\_")


def _ilike(keyword: str) -> str:
    """ILIKE/LIKE 模式串：元字符转义后前后通配。"""
    return f"%{_escape_like(keyword)}%"


def _dt(value: Optional[datetime]) -> str:
    """时间序列化（None -> 空串；展示前统一转服务器本地时区）。

    asyncpg 读 timestamptz 恒返回 UTC aware 值，直接 isoformat 输出的
    是 UTC 墙钟；维护页前端按前缀截断展示（不解析偏移），会把 UTC 当
    本地时间显示。转本地后输出形如 "...+08:00"，前端截断即得本地墙钟。
    naive 值视为已是本地口径原样输出。
    """
    if not value:
        return ""
    if value.tzinfo is not None:
        value = value.astimezone()
    return value.isoformat(sep=" ", timespec="seconds")


def _page(page: int, size: int) -> tuple[int, int]:
    page = max(1, page)
    size = min(max(1, size), 100)
    return page, size


async def fetch_overview(backend) -> dict:
    """概览 KPI：各表行数/待处理量/状态分布/kv 任务状态。"""
    row, kv = await _impl(backend).fetch_overview(backend)
    return {
        **row,
        "kv": [
            {"key": r["key"], "value": r["value"], "updated_at": _dt(r["updated_at"])}
            for r in kv
        ],
    }


async def fetch_users(
    backend, keyword: str, page: int, size: int
) -> dict:
    """有事实/簇的用户清单（画像页用户选择器数据源）。"""
    page, size = _page(page, size)
    data = await _impl(backend).fetch_users_data(backend, keyword, page, size)
    rows = data["rows"]
    name_map = {
        (r["platform"], r["user_id"]): r["display_name"] for r in data["names"]
    }
    return {
        "total": data["total"],
        "page": page,
        "size": size,
        "items": [
            {
                "platform": r["platform"],
                "user_id": r["user_id"],
                "display_name": name_map.get((r["platform"], r["user_id"]), ""),
                "active": r["active"],
                "profiled": r["profiled"],
                "pending": r["pending"],
                "max_score": r["max_score"],
                "updated_at": _dt(r["updated_at"]),
            }
            for r in rows
        ],
    }


async def fetch_facts(
    backend,
    page: int,
    size: int,
    user_id: str = "",
    session_id: str = "",
    category: str = "",
    confidence: str = "",
    extracted: str = "",
    q: str = "",
) -> dict:
    """原始事实分页浏览（多维过滤；SQL 与方言无关，ILIKE 按后端切换）。"""
    page, size = _page(page, size)
    like_op = _like_op(backend)
    conditions: list[str] = []
    params: list = []

    def add(cond: str, value) -> None:
        params.append(value)
        conditions.append(cond.format(n=len(params)))

    if user_id.strip():
        add("user_id = ${n}", user_id.strip())
    if session_id.strip():
        add("session_id = ${n}", session_id.strip())
    if category.strip() in _CATEGORIES:
        add("category = ${n}", category.strip())
    if confidence.strip() in ("high", "medium"):
        add("confidence = ${n}", confidence.strip())
    if extracted.strip() in ("0", "1"):
        add("extracted_flag = ${n}", int(extracted.strip()))
    if q.strip():
        add(f"statement {like_op} ${{n}}", _ilike(q.strip()))

    where = ("WHERE " + " AND ".join(conditions)) if conditions else ""
    async with _pool(backend).acquire() as conn:
        total = await conn.fetchval(
            f"SELECT count(*) FROM memory_persona_fact_raw {where}", *params
        )
        rows = await conn.fetch(
            f"""
            SELECT id, platform, user_id, related_user_ids, display_name, category,
                   statement, confidence, session_id, evidence_key,
                   extracted_flag, occurred_at, written_at,
                   (embedding IS NOT NULL) AS has_embedding
            FROM memory_persona_fact_raw {where}
            ORDER BY id DESC
            LIMIT ${len(params)+2} OFFSET ${len(params)+1}
            """,
            *params,
            (page - 1) * size,
            size,
        )
    return {
        "total": total or 0,
        "page": page,
        "size": size,
        "items": [
            {
                "id": r["id"],
                "platform": r["platform"],
                "user_id": r["user_id"],
                "related_user_ids": list(r["related_user_ids"] or []),
                "display_name": r["display_name"],
                "category": r["category"],
                "statement": r["statement"],
                "confidence": r["confidence"],
                "session_id": r["session_id"],
                "evidence_key": r["evidence_key"],
                "extracted": bool(r["extracted_flag"]),
                "occurred_at": _dt(r["occurred_at"]),
                "written_at": _dt(r["written_at"]),
                "has_embedding": r["has_embedding"],
            }
            for r in _decode_backend_rows(backend, rows)
        ],
    }


async def delete_fact(backend, fact_id: int) -> dict:
    """删除原始事实（误提取清理）。

    已入簇事实的评分状态机不受影响（score/evidence_count 是裁定结果），
    但同步把该 id 从相关簇的 source_fact_ids 引用中移除——否则引用悬挂、
    簇页「证据」计数（cardinality(source_fact_ids)）虚高。删除与清理
    同一事务，回滚时两不落单。
    """
    row = await _impl(backend).delete_fact(backend, fact_id)
    logger.info(
        "[WebUI] 原始事实已删除: id=%s user=%s extracted=%s statement=%s",
        row["id"], row["user_id"], row["extracted_flag"], row["statement"][:60],
    )
    return {"deleted": True, "was_extracted": bool(row["extracted_flag"])}


async def fetch_clusters(
    backend,
    page: int,
    size: int,
    user_id: str = "",
    category: str = "",
    status: str = "",
    session_id: str = "",
    q: str = "",
) -> dict:
    """事实簇分页浏览（画像来源，含确信度分值）。"""
    page, size = _page(page, size)
    filters = {
        "user_id": user_id.strip(),
        "category": category.strip() if category.strip() in _CATEGORIES else "",
        "status": status.strip() if status.strip() in _CLUSTER_STATUS_FILTER else "",
        "session_id": session_id.strip(),
        # 簇无 session 列：按证据键 {session}|{date} 前缀匹配（元字符转义
        # 而非剥离——含 _/% 的会话 ID 也能如实匹配）
        "session_evidence_prefix": _escape_like(session_id.strip()) + "|%",
        "q": q.strip(),
        "q_pattern": _ilike(q.strip()) if q.strip() else "",
    }
    data = await _impl(backend).fetch_clusters_data(backend, page, size, filters)
    return {
        "total": data["total"],
        "page": page,
        "size": size,
        "items": [
            {
                "id": r["id"],
                "platform": r["platform"],
                "user_id": r["user_id"],
                "category": r["category"],
                "statement": r["canonical_statement"],
                "score": r["score"],
                "status": r["status"],
                "evidence_count": r["evidence_count"],
                "evidence_keys": list(r["evidence_keys"] or []),
                "source_fact_count": r["source_fact_count"],
                "last_evidence_at": _dt(r["last_evidence_at"]),
                "occurred_at": _dt(r["occurred_at"]),
                "updated_at": _dt(r["updated_at"]),
                "replaced_by": r["replaced_by"],
                "demoted_at": _dt(r["demoted_at"]),
                "has_embedding": r["has_embedding"],
            }
            for r in data["rows"]
        ],
    }


async def update_cluster(
    backend, cluster_id: int, body: dict
) -> dict:
    """修正簇：陈述/分数/状态；画像表投影同步（保持表即画像的一致性）。

    状态为「保持现值」的提交（弹窗恒带 status 字段）视同未提交状态——
    replaced 墓碑簇只改陈述/分数的保存因此可放行（真正的迁出仍被拒）。
    手工置 replaced/dead 时联动下线其回填来源边；手工复活
    pending/dead 簇时登记关系回填待办（复活簇的存量陈述需补提取）。
    事务内簇行读取/修正 UPDATE 按方言派发（FOR UPDATE / now()）。
    """
    new_statement = body.get("canonical_statement")
    new_score = body.get("score")
    new_status = body.get("status")
    if new_score is not None:
        try:
            new_score = max(0.0, min(10.0, float(new_score)))
        except (TypeError, ValueError):
            raise HTTPException(status_code=422, detail="score 必须是 0~10 的数值")
    if new_statement is not None and not str(new_statement).strip():
        raise HTTPException(status_code=422, detail="canonical_statement 不能为空")

    impl = _impl(backend)
    async with _pool(backend).acquire() as conn:
        async with conn.transaction():
            cur = await impl.select_cluster_for_update(conn, cluster_id)
            if cur is None:
                raise HTTPException(status_code=404, detail="簇不存在")
            if new_status is not None and new_status == cur["status"]:
                # "Keep current status" submissions (the modal always sends
                # status) are equivalent to not submitting a status at all.
                new_status = None
            if new_status is not None and new_status not in _CLUSTER_STATUSES:
                raise HTTPException(
                    status_code=422,
                    detail=f"非法状态: {new_status}（可选 {sorted(_CLUSTER_STATUSES)}）",
                )
            if (
                cur["status"] == "replaced"
                and new_status is not None
                and new_status != "replaced"
            ):
                # 墓碑簇只读：手迁出会使 replaced_by 指向的继任链断裂
                raise HTTPException(
                    status_code=422,
                    detail="replaced 是合并管线墓碑状态，不可手工迁出"
                    "（如需恢复该说法，请编辑其继任簇或新建簇）",
                )
            superseded_edges = 0
            if (
                new_status in ("replaced", "dead")
                and cur["status"] not in ("replaced", "dead")
            ):
                # Manual kill: retire backfill-sourced edges whose only
                # evidence is this cluster (keep edges with live evidence).
                superseded_edges = await backend.supersede_backfill_edges_of(
                    conn, [cluster_id]
                )
            revived = (
                new_status in ("active", "profiled")
                and cur["status"] in ("pending_uncertain", "dead")
            )

            statement_changed = (
                new_statement is not None
                and str(new_statement) != cur["canonical_statement"]
            )
            final_statement = (
                str(new_statement) if statement_changed else cur["canonical_statement"]
            )
            final_score = new_score if new_score is not None else cur["score"]
            final_status = new_status if new_status is not None else cur["status"]

            await impl.update_cluster_row(
                conn, cluster_id, final_statement, final_score, final_status,
                statement_changed,
            )

            # 画像表投影同步（related_user_ids 随簇对齐——迁移 005 的
            # "簇即唯一真相源"投影不变量，与 promote/replace 路径同型）
            profiled_now = final_status == "profiled"
            if profiled_now:
                if _dialect(backend) == "sqlite":
                    # SQLite：时间戳由 Python 供给
                    await conn.execute(
                        _PROFILE_UPSERT_SQL.format(updated_at="$8"),
                        cur["platform"],
                        cur["user_id"],
                        cur["category"],
                        cluster_id,
                        final_statement,
                        final_score,
                        cur["related_user_ids"] or [],
                        _now_ts(backend),
                    )
                else:
                    await conn.execute(
                        _PROFILE_UPSERT_SQL.format(updated_at="now()"),
                        cur["platform"],
                        cur["user_id"],
                        cur["category"],
                        cluster_id,
                        final_statement,
                        final_score,
                        cur["related_user_ids"] or [],
                    )
            else:
                await conn.execute(
                    "DELETE FROM memory_user_profile WHERE cluster_id = $1",
                    cluster_id,
                )

    logger.info(
        "[WebUI] 簇已修正: id=%s user=%s statement_changed=%s score=%s status=%s "
        "edges_superseded=%s revived=%s",
        cluster_id, cur["user_id"], statement_changed, final_score, final_status,
        superseded_edges, revived,
    )
    if revived:
        # Revived clusters sit below the backfill watermark: register them as
        # pending sources so the next incremental backfill re-extracts them.
        try:
            await mark_backfill_pending(backend, [cluster_id])
        except Exception:
            logger.warning(
                "复活簇回填待办登记失败（cluster_id=%s，可手动全量重跑回补）",
                cluster_id,
                exc_info=True,
            )
    return {
        "updated": True,
        "statement_changed": statement_changed,
        "embedding_reset": statement_changed,
        "status": final_status,
        "score": final_score,
        "edges_superseded": superseded_edges,
    }


def _now_ts(backend) -> str:
    """画像投影 upsert 的 updated_at（SQLite 由 Python 供给；PG 由 SQL
    now() 生成——此处值仅 SQLite 路消费，PG 路忽略该参数）。"""
    if _dialect(backend) == "sqlite":
        from .db.sqlite import _now_ts as _sqlite_now

        return _sqlite_now()
    return ""


async def fetch_profile_preview(
    db,
    persona_service,
    platform: str,
    user_id: str,
) -> dict:
    """画像预览（复用 LocalPersonaService 拼装：所见即注入）。

    Args:
        db: MemoryBackend（拼装数据源与 persona_service 同源）。
        persona_service: LocalPersonaService 实例。
        platform: 平台标识。
        user_id: 用户 ID。
    """
    injection_text = await persona_service.build_profile_text(
        user_id=user_id, platform=platform
    )
    sections = await db.fetch_profile_sections(platform, user_id, 5)
    uncertain = await db.fetch_uncertain_statements(platform, user_id, 5)
    display_name = (
        await db.fetch_latest_display_name(platform, user_id) or user_id
    )
    return {
        "platform": platform,
        "user_id": user_id,
        "display_name": display_name,
        "injection_text": injection_text,
        "sections": sections,
        "uncertain": uncertain,
    }


async def fetch_summaries(
    backend,
    page: int,
    size: int,
    session_id: str = "",
    user_id: str = "",
    kind: str = "",
    q: str = "",
) -> dict:
    """对话摘要分页浏览（recall 语料 = 注入事实来源）。"""
    page, size = _page(page, size)
    like_op = _like_op(backend)
    conditions: list[str] = []
    params: list = []

    def add(cond: str, value) -> None:
        params.append(value)
        conditions.append(cond.format(n=len(params)))

    if session_id.strip():
        add("session_id = ${n}", session_id.strip())
    if user_id.strip():
        add("user_id = ${n}", user_id.strip())
    if kind.strip() in ("chat_summary", "bot_self"):
        add("kind = ${n}", kind.strip())
    if q.strip():
        add(f"content {like_op} ${{n}}", _ilike(q.strip()))

    where = ("WHERE " + " AND ".join(conditions)) if conditions else ""
    async with _pool(backend).acquire() as conn:
        total = await conn.fetchval(
            f"SELECT count(*) FROM memory_chat_summary {where}", *params
        )
        rows = await conn.fetch(
            f"""
            SELECT id, document_id, kind, platform, session_id, user_id,
                   participants, content, occurred_at, written_at,
                   (embedding IS NOT NULL) AS has_embedding,
                   (NOT summarized) AS unsummarized
            FROM memory_chat_summary {where}
            ORDER BY occurred_at DESC, id DESC
            LIMIT ${len(params)+2} OFFSET ${len(params)+1}
            """,
            *params,
            (page - 1) * size,
            size,
        )
    return {
        "total": total or 0,
        "page": page,
        "size": size,
        "items": [
            {
                "id": r["id"],
                "document_id": r["document_id"],
                "kind": r["kind"],
                "platform": r["platform"],
                "session_id": r["session_id"],
                "user_id": r["user_id"],
                "participants": list(r["participants"] or []),
                "content": r["content"],
                "occurred_at": _dt(r["occurred_at"]),
                "written_at": _dt(r["written_at"]),
                "has_embedding": r["has_embedding"],
                "unsummarized": r["unsummarized"],
            }
            for r in _decode_backend_rows(backend, rows)
        ],
    }


async def update_summary(backend, summary_id: int, body: dict) -> dict:
    """修正摘要内容（embedding 置 NULL 待重算；search_text 同步重分词）。

    管理员修正视为内容终态：summarized 翻 true——修正行立即重新参与
    召回，且不被合并 agent 补编码遍再编码（防手工内容被覆盖）。
    search_text 必须随新正文重算：补算任务只扫 search_text IS NULL 的
    行，不重算会带着旧正文的 BM25 分词（混合检索按旧内容命中、注入
    的却是新内容）。SQL 与方言无关；SQLite 侧 FTS5 影子表由触发器
    语义的补算遍兜底（search_text 置新值不经过本路径时由回填遍收敛）。
    """
    content = str(body.get("content", "")).strip()
    if not content:
        raise HTTPException(status_code=422, detail="content 不能为空")

    async with _pool(backend).acquire() as conn:
        row = await conn.fetchrow(
            """
            UPDATE memory_chat_summary
            SET content = $2, embedding = NULL, summarized = true,
                search_text = $3
            WHERE id = $1
            RETURNING id, session_id
            """,
            summary_id,
            content,
            build_search_text(content),
        )
        if row is not None and _dialect(backend) == "sqlite":
            # FTS5 影子表同步（BM25 路与新正文对齐）
            await conn.execute(
                "DELETE FROM memory_chat_summary_fts WHERE summary_id = $1",
                int(row["id"]),
            )
            search_text = build_search_text(content)
            if search_text:
                await conn.execute(
                    "INSERT INTO memory_chat_summary_fts"
                    " (summary_id, text) VALUES ($1, $2)",
                    int(row["id"]), search_text,
                )
    if row is None:
        raise HTTPException(status_code=404, detail="摘要不存在")
    logger.info(
        "[WebUI] 摘要已修正: id=%s session=%s（embedding 置 NULL 待重算，"
        "search_text 已重分词，summarized=true）",
        summary_id, row["session_id"],
    )
    return {"updated": True, "embedding_reset": True}


async def delete_summary(backend, summary_id: int) -> dict:
    """删除摘要（错误/敏感轮次从 recall 语料移除）。"""
    async with _pool(backend).acquire() as conn:
        head_expr = (
            "substr(content, 1, 60)" if _dialect(backend) == "sqlite"
            else "left(content, 60)"
        )
        row = await conn.fetchrow(
            "DELETE FROM memory_chat_summary WHERE id = $1 "
            f"RETURNING id, session_id, {head_expr} AS head",
            summary_id,
        )
        if row is not None and _dialect(backend) == "sqlite":
            await conn.execute(
                "DELETE FROM memory_chat_summary_fts WHERE summary_id = $1",
                int(row["id"]),
            )
    if row is None:
        raise HTTPException(status_code=404, detail="摘要不存在")
    logger.info(
        "[WebUI] 摘要已删除: id=%s session=%s head=%s",
        summary_id, row["session_id"], row["head"],
    )
    return {"deleted": True}


async def fetch_relation_graph(backend, bot_user_id: str = "") -> dict:
    """关系图谱数据：全量边（含 pending）+ 组装节点 + 统计。

    与上游 nori 版关系图谱接口同构（节点 platform:uid 复合键、
    规范名解析、bot 节点标注、degree 统计）；回填可用性由 main 层 API
    处理器并入（控制器状态挂插件实例）。

    端点显示名经规范名解析统一（按 uid 唯一）：别名表最新名 > 边上
    名字（非占位）> uid。边表结构键本就是 uid，不存在实体分裂，此层
    只解决"同一 uid 多个显示名"的展示一致性问题。（上游 nori 版另有
    身份映射最高优先级层，kira 宿主无该概念，此层缺省。）
    """
    data = await _impl(backend).fetch_relation_graph_data(backend)
    rows = data["rows"]
    edges = rows
    # uid -> 最新非占位别名（last_seen 降序扫描首见即最新）
    names: dict[tuple[str, str], str] = {}
    for r in data["alias_rows"]:
        nm = str(r["name"] or "").strip()
        if not nm or is_placeholder_name(nm):
            continue
        names.setdefault((r["platform"], str(r["user_id"])), nm)

    def _canonical(platform: str, uid: str, edge_name: str) -> str:
        alias = names.get((platform, uid))
        if alias:
            return alias
        if edge_name and not is_placeholder_name(edge_name):
            return edge_name
        return uid

    bot = (bot_user_id or "").strip()
    nodes: dict[str, dict] = {}
    for e in edges:
        platform = e["platform"] or ""
        for side in ("subject", "object"):
            uid = str(e[f"{side}_uid"] or "")
            if not uid:
                continue
            key = f"{platform}:{uid}"
            if key not in nodes:
                nodes[key] = {
                    "id": key,
                    "platform": platform,
                    "uid": uid,
                    "name": _canonical(platform, uid, str(e[f"{side}_name"] or "")),
                    "is_bot": bool(bot) and uid == bot,
                    "degree": 0,
                }
            nodes[key]["degree"] += 1
    return {
        "nodes": list(nodes.values()),
        "edges": [
            {
                "id": e["id"],
                "platform": e["platform"],
                "subject": f"{e['platform'] or ''}:{e['subject_uid']}",
                "object": f"{e['platform'] or ''}:{e['object_uid']}",
                "subject_name": _canonical(
                    e["platform"] or "",
                    str(e["subject_uid"] or ""),
                    str(e["subject_name"] or ""),
                ),
                "object_name": _canonical(
                    e["platform"] or "",
                    str(e["object_uid"] or ""),
                    str(e["object_name"] or ""),
                ),
                "label": e["relation_label"],
                "statement": e["statement"],
                "status": e["status"],
                "confidence": e["confidence"],
                "evidence_count": e["evidence_count"],
                "last_seen": _dt(e["last_seen"]),
                "occurred_at": _dt(e["occurred_at"]),
                "supersede_reason": e.get("supersede_reason") or "",
            }
            for e in edges
        ],
        "stats": {
            "nodes": len(nodes),
            # edges = full-table truth (truncation notice rendered by the
            # frontend when edges_shown < edges); edges_shown = page size.
            "edges": int(data["total"] or 0),
            "edges_shown": len(edges),
            "edges_active": sum(1 for e in edges if e["status"] == "active"),
            "edges_pending": sum(1 for e in edges if e["status"] == "pending"),
            "edges_superseded": sum(
                1 for e in edges if e["status"] == "superseded"
            ),
            "relation_extract_hint": "边由 P2 提取/回填写入，P3 注入只消费 active",
        },
    }


async def update_relation_edge(backend, edge_id: int, body: dict) -> dict:
    """人工修正边状态（active/pending/superseded）。

    墓碑保护语义：置 superseded 后新证据不再自动激活该边（恢复走本
    端点人工置回）；statement/端点/label 不可编辑（结构键与陈述是
    证据快照，改等于删旧建新）。人工置墓碑写入 supersede_reason 留痕
    （可追溯），updated_at 同步推进（在途审计批的乐观锁据此跳过）。
    """
    new_status = body.get("status")
    if new_status not in _EDGE_STATUSES:
        raise HTTPException(
            status_code=400,
            detail=f"非法状态: {new_status}（可选 {sorted(_EDGE_STATUSES)}）",
        )
    row = await _impl(backend).update_relation_edge(backend, edge_id, new_status)
    logger.info(
        "[WebUI] 关系边已修正: id=%s %s->%s %s(%s)->%s(%s) label=%s",
        row["id"], row["pre_status"], new_status,
        row["subject_name"], row["subject_uid"],
        row["object_name"], row["object_uid"],
        row["relation_label"],
    )
    return {"updated": True, "id": row["id"], "status": new_status}


async def delete_relation_edge(backend, edge_id: int) -> dict:
    """物理删除关系边（误提取清理；日常下线用状态修正置 superseded）。"""
    async with _pool(backend).acquire() as conn:
        row = await conn.fetchrow(
            """
            DELETE FROM memory_entity_edge WHERE id = $1
            RETURNING id, subject_uid, object_uid, relation_label,
                      status, statement
            """,
            edge_id,
        )
    if row is None:
        raise HTTPException(status_code=404, detail="边不存在")
    logger.info(
        "[WebUI] 关系边已删除: id=%s status=%s %s->%s label=%s statement=%s",
        row["id"], row["status"], row["subject_uid"], row["object_uid"],
        row["relation_label"], str(row["statement"])[:60],
    )
    return {"deleted": True, "id": row["id"]}


async def delete_user_memories(
    backend, platform: str, user_id: str
) -> dict:
    """Per-user erasure: cascade delete plugin-owned memory rows (one txn).

    Covers profile projections, fact clusters, raw facts, aliases and
    relation edges owned by (platform, user_id). Backfill-sourced edges of
    the user's clusters are retired (superseded) rather than deleted only
    when they carry other evidence — same propagation rule as cluster
    replace. Chat summaries are session-scoped and may involve other
    members, so they are intentionally not cascaded. SQL 与方言无关
    （execute 状态串解析依赖池 shim 的 asyncpg 兼容形态）。
    """
    platform = (platform or "").strip()
    user_id = (user_id or "").strip()
    if not platform or not user_id:
        raise HTTPException(status_code=422, detail="platform 与 user_id 均不能为空")
    async with _pool(backend).acquire() as conn:
        async with conn.transaction():
            cluster_ids = [
                r["id"]
                for r in await conn.fetch(
                    "SELECT id FROM memory_fact_cluster "
                    "WHERE platform = $1 AND user_id = $2",
                    platform,
                    user_id,
                )
            ]
            edges_retired = await backend.supersede_backfill_edges_of(
                conn, cluster_ids
            )
            counts: dict[str, int] = {}
            wait_del = await conn.execute(
                "DELETE FROM memory_user_profile "
                "WHERE platform = $1 AND user_id = $2",
                platform,
                user_id,
            )
            counts["profiles"] = int(str(wait_del).rsplit(" ", 1)[-1])
            wait_del = await conn.execute(
                "DELETE FROM memory_fact_cluster "
                "WHERE platform = $1 AND user_id = $2",
                platform,
                user_id,
            )
            counts["clusters"] = int(str(wait_del).rsplit(" ", 1)[-1])
            wait_del = await conn.execute(
                "DELETE FROM memory_persona_fact_raw "
                "WHERE platform = $1 AND user_id = $2",
                platform,
                user_id,
            )
            counts["facts"] = int(str(wait_del).rsplit(" ", 1)[-1])
            wait_del = await conn.execute(
                "DELETE FROM memory_entity_alias "
                "WHERE platform = $1 AND user_id = $2",
                platform,
                user_id,
            )
            counts["aliases"] = int(str(wait_del).rsplit(" ", 1)[-1])
            wait_del = await conn.execute(
                "DELETE FROM memory_entity_edge "
                "WHERE platform = $1 AND (subject_uid = $2 OR object_uid = $2)",
                platform,
                user_id,
            )
            counts["edges"] = int(str(wait_del).rsplit(" ", 1)[-1])
    logger.warning(
        "[WebUI] 用户记忆已级联删除: %s:%s %s（另有 %d 条回填来源边置墓碑）",
        platform, user_id, counts, edges_retired,
    )
    return {"deleted": True, "platform": platform, "user_id": user_id, **counts}
