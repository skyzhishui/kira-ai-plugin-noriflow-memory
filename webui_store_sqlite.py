"""记忆维护 API 数据层——SQLite SQL 体（webui_store_pg 的方言对应实现）。

与 webui_store_pg.py 函数一一对应；方言改写要点：
- ILIKE -> LIKE（SQLite LIKE 对 ASCII 恒不区分大小写；转义口径与 PG 一致）；
- unnest(evidence_keys) -> json_each EXISTS；cardinality -> json_array_length；
- array_remove/ANY -> 事务内读-改-写（JSON 数组 Python 侧维护）；
- DISTINCT ON -> row_number() 窗口；row 值构造去掉 ::text 强转；
- FOR UPDATE -> 普通 SELECT（单写者，BEGIN IMMEDIATE 已串行化）；
- now() -> Python 供给参数；
- 行读出统一经 db.sqlite._decode_row 还原（时间/JSON/布尔/向量），与
  asyncpg 行为一致。
"""

from __future__ import annotations

import json

from fastapi import HTTPException

from .db.sqlite import _decode_rows
from .webui_store import _ilike


def _pool(backend):
    """连接池解析：backend 对象取 .pool；裸池（旧调用方/测试桩）原样。"""
    return backend.pool if hasattr(backend, "pool") else backend


async def fetch_overview(backend) -> tuple[dict, list]:
    """概览 KPI 原始查询（users_with_clusters 以 GROUP BY 子查询等价
    PG 的 count(DISTINCT (platform, user_id))——SQLite 无行构造 DISTINCT）。"""
    async with _pool(backend).acquire() as conn:
        row = await conn.fetchrow(
            """
            SELECT
              (SELECT count(*) FROM memory_chat_summary
                 WHERE summarized OR kind = 'bot_self') AS summaries,
              (SELECT count(*) FROM memory_chat_summary WHERE NOT summarized) AS summaries_unsummarized,
              (SELECT count(*) FROM memory_chat_summary WHERE embedding IS NULL) AS summaries_no_vec,
              (SELECT count(*) FROM memory_persona_fact_raw) AS facts_raw,
              (SELECT count(*) FROM memory_persona_fact_raw WHERE extracted_flag = 0) AS facts_pending,
              (SELECT count(*) FROM memory_fact_cluster) AS clusters,
              (SELECT count(*) FROM memory_fact_cluster WHERE status = 'active') AS clusters_active,
              (SELECT count(*) FROM memory_fact_cluster WHERE status = 'profiled') AS clusters_profiled,
              (SELECT count(*) FROM memory_fact_cluster WHERE status = 'pending_uncertain') AS clusters_pending,
              (SELECT count(*) FROM memory_fact_cluster WHERE status = 'dead') AS clusters_dead,
              (SELECT count(*) FROM memory_user_profile) AS profile_rows,
              (SELECT count(*) FROM (
                 SELECT 1 FROM memory_fact_cluster
                 WHERE status IN ('active','profiled')
                 GROUP BY platform, user_id
               )) AS users_with_clusters
            """
        )
        kv = await conn.fetch(
            "SELECT key, value, updated_at FROM _memory_local_kv ORDER BY key"
        )
    return dict(row), _decode_rows(kv)


async def fetch_users_data(backend, keyword: str, page: int, size: int) -> dict:
    """有事实/簇的用户清单——原始查询（DISTINCT ON 改窗口函数）。"""
    params: list = []
    where = ""
    if keyword:
        params.append(_ilike(keyword))
        where = "WHERE c.user_id LIKE $1"
    async with _pool(backend).acquire() as conn:
        total = await conn.fetchval(
            f"""
            SELECT count(*) FROM (
              SELECT 1 FROM memory_fact_cluster c {where}
              GROUP BY c.platform, c.user_id
            ) t
            """,
            *params,
        )
        rows = await conn.fetch(
            f"""
            SELECT c.platform, c.user_id,
                   count(*) FILTER (WHERE c.status = 'active') AS active,
                   count(*) FILTER (WHERE c.status = 'profiled') AS profiled,
                   count(*) FILTER (WHERE c.status = 'pending_uncertain') AS pending,
                   max(c.score) AS max_score,
                   max(c.updated_at) AS updated_at
            FROM memory_fact_cluster c {where}
            GROUP BY c.platform, c.user_id
            ORDER BY max(c.updated_at) DESC
            LIMIT ${len(params)+2} OFFSET ${len(params)+1}
            """,
            *params,
            (page - 1) * size,
            size,
        )
        # 名字映射只查当前页用户；DISTINCT ON 等价改写：row_number 取
        # (platform, user_id) 分组内 occurred_at DESC, id DESC 首行
        pairs = [(r["platform"], r["user_id"]) for r in rows]
        names = []
        if pairs:
            from .db.sqlite import _Params

            p = _Params(None)
            pair_sql = ", ".join(
                f"({p.add(a)}, {p.add(b)})" for a, b in pairs
            )
            raw_names = await conn.fetch(
                f"""
                SELECT platform, user_id, display_name FROM (
                    SELECT f.platform, f.user_id, f.display_name,
                           row_number() OVER (
                               PARTITION BY f.platform, f.user_id
                               ORDER BY f.occurred_at DESC, f.id DESC
                           ) AS rn
                    FROM memory_persona_fact_raw f
                    WHERE f.display_name <> ''
                      AND (f.platform, f.user_id) IN ({pair_sql})
                ) t WHERE rn = 1
                """,
                *p.values,
            )
            names = [dict(r) for r in raw_names]
    return {
        "total": total or 0,
        "rows": _decode_rows(rows),
        "names": [dict(r) for r in names],
    }


async def delete_fact(backend, fact_id: int) -> dict:
    """删除原始事实并同步清理簇表 source_fact_ids 引用（同一事务）。

    array_remove 语义改写：事务内取受影响簇 -> Python 维护 JSON 数组 ->
    逐行写回（BEGIN IMMEDIATE 串行化，无丢更新）。
    """
    async with _pool(backend).acquire() as conn:
        async with conn.transaction():
            row = await conn.fetchrow(
                "DELETE FROM memory_persona_fact_raw WHERE id = $1 "
                "RETURNING id, user_id, statement, extracted_flag",
                fact_id,
            )
            if row is None:
                raise HTTPException(status_code=404, detail="事实不存在")
            affected = await conn.fetch(
                "SELECT id, source_fact_ids FROM memory_fact_cluster "
                "WHERE EXISTS (SELECT 1 FROM json_each(source_fact_ids) je "
                "WHERE je.value = $1)",
                int(fact_id),
            )
            for cluster in affected:
                try:
                    ids = json.loads(cluster["source_fact_ids"] or "[]")
                except (ValueError, TypeError):
                    ids = []
                ids = [int(i) for i in ids if int(i) != int(fact_id)]
                await conn.execute(
                    "UPDATE memory_fact_cluster SET source_fact_ids = $2 "
                    "WHERE id = $1",
                    int(cluster["id"]),
                    json.dumps(ids),
                )
    return dict(row)


async def fetch_clusters_data(
    backend, page: int, size: int, filters: dict
) -> dict:
    """事实簇分页浏览——原始查询（unnest EXISTS -> json_each）。"""
    conditions: list[str] = []
    params: list = []

    def add(cond: str, value) -> None:
        params.append(value)
        conditions.append(cond.format(n=len(params)))

    if filters["user_id"]:
        add("user_id = ${n}", filters["user_id"])
    if filters["category"]:
        add("category = ${n}", filters["category"])
    if filters["status"]:
        add("status = ${n}", filters["status"])
    if filters["session_id"]:
        add(
            "EXISTS (SELECT 1 FROM json_each(evidence_keys) e "
            "WHERE e.value LIKE ${n} ESCAPE '\\')",
            filters["session_evidence_prefix"],
        )
    if filters["q"]:
        add("canonical_statement LIKE ${n}", filters["q_pattern"])

    where = ("WHERE " + " AND ".join(conditions)) if conditions else ""
    async with _pool(backend).acquire() as conn:
        total = await conn.fetchval(
            f"SELECT count(*) FROM memory_fact_cluster {where}", *params
        )
        rows = await conn.fetch(
            f"""
            SELECT id, platform, user_id, category, canonical_statement, score,
                   status, evidence_count, evidence_keys, last_evidence_at,
                   occurred_at, updated_at, replaced_by, demoted_at,
                   (embedding IS NOT NULL) AS has_embedding,
                   (SELECT count(*) FROM json_each(source_fact_ids))
                       AS source_fact_count
            FROM memory_fact_cluster {where}
            ORDER BY score DESC, updated_at DESC, id ASC
            LIMIT ${len(params)+2} OFFSET ${len(params)+1}
            """,
            *params,
            (page - 1) * size,
            size,
        )
    return {"total": total or 0, "rows": _decode_rows(rows)}


async def select_cluster_for_update(conn, cluster_id: int):
    """簇行读取（SQLite 单写者，无需 FOR UPDATE）。"""
    return await conn.fetchrow(
        "SELECT * FROM memory_fact_cluster WHERE id = $1", cluster_id
    )


async def update_cluster_row(
    conn, cluster_id: int, statement: str, score, status: str,
    statement_changed: bool,
) -> None:
    """簇修正 UPDATE（SQLite：时间戳由 Python 供给，覆盖 pg 版同签名）。"""
    from .db.sqlite import _now_ts

    now = _now_ts()
    await conn.execute(
        """
        UPDATE memory_fact_cluster
        SET canonical_statement = $2, score = $3, status = $4,
            updated_at = $6,
            embedding = CASE WHEN $5 THEN NULL ELSE embedding END,
            demoted_at = CASE
                WHEN $4 = 'pending_uncertain' AND status <> 'pending_uncertain'
                    THEN $6
                WHEN $4 <> 'pending_uncertain' THEN NULL
                ELSE demoted_at
            END
        WHERE id = $1
        """,
        cluster_id,
        statement,
        score,
        status,
        statement_changed,
        now,
    )


async def fetch_relation_graph_data(backend) -> dict:
    """关系图谱原始查询（别名 join unnest -> 行值 IN）。"""
    async with _pool(backend).acquire() as conn:
        rows = await conn.fetch(
            """
            SELECT id, platform, subject_uid, object_uid, subject_name,
                   object_name, relation_label, statement, status,
                   confidence, evidence_count, last_seen, occurred_at,
                   supersede_reason
            FROM memory_entity_edge
            ORDER BY evidence_count DESC, last_seen DESC
            LIMIT $1
            """,
            500,
        )
        total = await conn.fetchval("SELECT count(*) FROM memory_entity_edge")
        owners: set[tuple[str, str]] = set()
        for r in rows:
            for col_uid in ("subject_uid", "object_uid"):
                uid = str(r[col_uid] or "")
                if uid:
                    owners.add((str(r["platform"] or ""), uid))
        alias_rows: list = []
        if owners:
            from .db.sqlite import _Params

            p = _Params(None)
            pair_sql = ", ".join(
                f"({p.add(a)}, {p.add(b)})" for a, b in sorted(owners)
            )
            alias_rows = [
                dict(r)
                for r in await conn.fetch(
                    "SELECT a.platform, a.user_id, a.name "
                    "FROM memory_entity_alias a "
                    f"WHERE (a.platform, a.user_id) IN ({pair_sql}) "
                    "ORDER BY a.last_seen DESC, a.name ASC",
                    *p.values,
                )
            ]
    return {
        "rows": _decode_rows(rows),
        "total": total or 0,
        "alias_rows": alias_rows,
    }


async def update_relation_edge(backend, edge_id: int, new_status: str) -> dict:
    """人工修正边状态（时间戳由 Python 供给）。"""
    from .db.sqlite import _now_ts

    now = _now_ts()
    async with _pool(backend).acquire() as conn:
        cur = await conn.fetchrow(
            "SELECT status FROM memory_entity_edge WHERE id = $1", edge_id
        )
        if cur is None:
            raise HTTPException(status_code=404, detail="边不存在")
        row = await conn.fetchrow(
            """
            UPDATE memory_entity_edge
            SET status = $2,
                updated_at = $3,
                superseded_at = CASE WHEN $2 = 'superseded' THEN $3
                                     ELSE superseded_at END,
                supersede_reason = CASE WHEN $2 = 'superseded'
                                        THEN 'manual:status_edit'
                                        ELSE supersede_reason END
            WHERE id = $1
            RETURNING id, platform, subject_uid, object_uid,
                      subject_name, object_name, relation_label, status
            """,
            edge_id,
            new_status,
            now,
        )
    if row is None:
        raise HTTPException(status_code=404, detail="边不存在")
    return {**dict(row), "pre_status": cur["status"]}
