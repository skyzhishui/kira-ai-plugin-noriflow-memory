"""Shared data-access layer for the memory maintenance API (dual-backend split, §6).

This module keeps the dialect-agnostic parts: constants and whitelists,
pagination and filter parameter validation, row assembly (pure Python
logic like _dt datetime serialization), and dialect-identical SQL; the
dialect-divergent SQL bodies live in webui_store_pg.py / webui_store_sqlite.py
(functions map one-to-one), dispatched by ``backend.dialect``.
Query/pagination/correction SQL matches the upstream nori version verbatim
(PG path); errors surface as fastapi HTTPException.
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
    """Backend dialect (test stubs without a dialect attribute are treated as postgres)."""
    return getattr(backend, "dialect", "postgres")


def _pool(backend):
    """Resolve the connection pool: take .pool from a backend object; pass a bare pool (legacy callers/test stubs) through unchanged."""
    return backend.pool if hasattr(backend, "pool") else backend


def _impl(backend):
    """Return the SQL-body implementation module for the backend dialect (lazy import to avoid cycles)."""
    if _dialect(backend) == "sqlite":
        from . import webui_store_sqlite as impl

        return impl
    from . import webui_store_pg as impl

    return impl


def _decode_backend_rows(backend, rows) -> list[dict]:
    """Uniform decoding for SQLite rows (time/JSON/bool/vector); PG rows stay as dicts."""
    if _dialect(backend) == "sqlite":
        from .db.sqlite import _decode_rows

        return _decode_rows(rows)
    return [dict(r) for r in rows]


def _like_op(backend) -> str:
    """Keyword-match operator (SQLite LIKE is always case-insensitive for ASCII)."""
    return "LIKE" if _dialect(backend) == "sqlite" else "ILIKE"


def _like_suffix(backend) -> str:
    """LIKE suffix: SQLite has no default escape character, so backslash
    escape patterns require an explicit ESCAPE clause (PG's LIKE/ILIKE
    default escape is backslash, no clause needed)."""
    return " ESCAPE '\\'" if _dialect(backend) == "sqlite" else ""


def _escape_like(keyword: str) -> str:
    """Escapes LIKE pattern metacharacters (\\ % _ become literals; the default escape is \\)."""
    return keyword.replace("\\", "\\\\").replace("%", "\\%").replace("_", "\\_")


def _ilike(keyword: str) -> str:
    """ILIKE/LIKE pattern: metacharacters escaped, then wrapped with leading and trailing wildcards."""
    return f"%{_escape_like(keyword)}%"


def _dt(value: Optional[datetime]) -> str:
    """Datetime serialization (None -> empty string; converted to the server
    local timezone before display).

    asyncpg always returns UTC-aware timestamptz values, so a direct isoformat
    output is the UTC wall clock; the maintenance-page frontend truncates the
    prefix for display (it does not parse the offset) and would show UTC as
    local time. After conversion the output looks like "...+08:00", and
    truncation yields the local wall clock. Naive values are treated as
    already local and passed through unchanged.
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
    """Overview KPIs: per-table row counts, pending amounts, status distribution, kv task status."""
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
    """Listing of users that have facts/clusters (data source for the persona-page user picker)."""
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
    """Paginated raw-fact browsing (multi-dimension filtering; SQL is dialect-agnostic, ILIKE toggled by backend)."""
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
        add(f"statement {like_op} ${{n}}{_like_suffix(backend)}", _ilike(q.strip()))

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
    """Delete a raw fact (miscapture cleanup).

    The scoring state machine of facts already merged into clusters is
    unaffected (score/evidence_count are adjudication results), but the id is
    also removed from the source_fact_ids references of related clusters;
    otherwise references go stale and the cluster page "evidence" count
    (cardinality(source_fact_ids)) would be inflated. Deletion and cleanup
    run in the same transaction, so a rollback never leaves one done without
    the other.
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
    """Paginated fact-cluster browsing (persona source, including confidence scores)."""
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
    """Correct a cluster: statement/score/status; the persona-table projection
    stays in sync (the table remains identical to the persona).

    A submission whose status is "keep current value" (the modal always sends
    the status field) counts as not submitting a status, so a replaced
    tombstone cluster can still save statement/score changes (a real
    migration out remains rejected). Manually setting replaced/dead also
    retires the cluster's backfill-sourced edges; manually reviving a
    pending_uncertain/dead cluster registers a relation-backfill pending job
    (existing statements of the revived cluster need re-extraction). Within
    the transaction the cluster row read and correction UPDATE are dispatched
    by dialect (FOR UPDATE / now()).
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
    """updated_at for the persona-projection upsert (SQLite supplies it from
    Python; PG generates it via SQL now(). This value is consumed only on the
    SQLite path; the PG path ignores it)."""
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
    """Persona preview (reuses LocalPersonaService assembly: what you see is
    what gets injected).

    Args:
        db: MemoryBackend (assembly data source, same source as persona_service).
        persona_service: LocalPersonaService instance.
        platform: Platform identifier.
        user_id: User ID.
    """
    injection_text = await persona_service.build_profile_text(
        user_id=user_id, platform=platform
    )
    sections = await db.fetch_profile_sections(platform, user_id, 5)
    uncertain = await db.fetch_uncertain_statements(platform, user_id, 5)
    display_name = (
        await db.fetch_latest_display_name(platform, user_id) or user_id
    )

    def _entry(statement: str, occurred_at) -> dict:
        return {"statement": statement, "occurred_at": _dt(occurred_at)}

    return {
        "platform": platform,
        "user_id": user_id,
        "display_name": display_name,
        "injection_text": injection_text,
        # sections/uncertain 维持既有字符串结构（消费方兼容）；
        # 发生时间经 *_meta 附加，不破坏旧契约
        "sections": {
            category: [s for s, _ in items]
            for category, items in sections.items()
        },
        "sections_meta": {
            category: [_entry(s, ts) for s, ts in items]
            for category, items in sections.items()
        },
        "uncertain": [s for s, _ in uncertain],
        "uncertain_meta": [_entry(s, ts) for s, ts in uncertain],
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
    """Paginated conversation-summary browsing (recall corpus, the injected fact source)."""
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
        add(f"content {like_op} ${{n}}{_like_suffix(backend)}", _ilike(q.strip()))

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
    """Correct a summary's content (embedding set to NULL pending recompute; search_text re-tokenized in sync).

    An admin correction is treated as the content's final state: summarized
    flips to true, so the corrected row immediately rejoins recall and is
    not re-encoded by the merge agent's backfill pass (protects manual edits
    from being overwritten). search_text must be recomputed with the new
    body: the backfill job only scans rows where search_text IS NULL, so not
    recomputing would leave the old body's BM25 tokens (hybrid retrieval
    matches on stale content while the injected text is new). SQL is
    dialect-independent; on the SQLite side the FTS5 shadow table is kept
    consistent by the trigger-based backfill pass (when a new search_text is
    set without going through this path, the backfill pass converges it).
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
    """Delete a summary (removes an erroneous or sensitive turn from the recall corpus)."""
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
    """Relation-graph data: all edges (including pending), assembled nodes, and statistics.

    Isomorphic to the upstream nori relation-graph endpoint (platform:uid
    composite node keys, canonical-name resolution, bot-node marking, degree
    stats); backfill availability is folded in by the main-layer API handler
    (controller state hangs off the plugin instance).

    Endpoint display names are unified via canonical-name resolution (unique
    per uid): alias-table latest name > edge name (non-placeholder) > uid.
    Edge-table structural keys are already uids, so no entity split exists;
    this layer only fixes the "one uid, multiple display names" display
    consistency issue. (The upstream nori version has an extra identity-map
    layer at top priority; the kira host has no such concept, so this layer
    omits it.)
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
    """Manually correct an edge's status (active/pending/superseded).

    Tombstone-protection semantics: once superseded, new evidence no longer
    reactivates the edge (recovery goes through this endpoint's manual reset);
    statement/endpoints/label are not editable (structural keys and the
    statement are an evidence snapshot; changing them equals delete-and-recreate).
    Manually setting the tombstone writes supersede_reason for traceability,
    and updated_at advances in step (in-flight audit batches skip it via the
    optimistic lock).
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
    """Physically delete a relation edge (miscapture cleanup; for routine takedowns, mark superseded via status correction)."""
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
    """Per-user erasure: cascade-delete plugin-owned memory rows (one txn).

    Covers profile projections, fact clusters, raw facts, aliases and
    relation edges owned by (platform, user_id). Backfill-sourced edges of
    the user's clusters are retired (superseded) rather than deleted only
    when they carry other evidence, the same propagation rule as cluster
    replace. Chat summaries are session-scoped and may involve other
    members, so they are intentionally not cascaded. SQL is dialect-
    independent (parsing the execute status string relies on the pool
    shim's asyncpg-compatible form).
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
