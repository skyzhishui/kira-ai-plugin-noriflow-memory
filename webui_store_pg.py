"""记忆维护 API 数据层——PostgreSQL SQL 体（自 webui_store.py 下沉）。

双后端拆分（docs/plans/noriflow-dual-storage-backend-plan.md §6）：
webui_store.py 保留共享层（行装配/分页校验/常量），本模块持有 PG 方言
SQL；SQL 与拆分前逐字一致，行为零变化。SQLite 方言的对应实现见
webui_store_sqlite.py（函数一一对应）。

约定：函数首个参数为 backend（MemoryDatabase / SQLiteMemoryDatabase），
``backend.supersede_backfill_edges_of(conn, ids)`` 派发双后端墓碑传播。
"""

from __future__ import annotations

from fastapi import HTTPException



def _pool(backend):
    """连接池解析：backend 对象取 .pool；裸池（旧调用方/测试桩）原样。"""
    return backend.pool if hasattr(backend, "pool") else backend


async def fetch_overview(backend) -> tuple[dict, list]:
    """概览 KPI 原始查询：各表行数/待处理量/状态分布 + kv 任务状态。"""
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
              (SELECT count(DISTINCT (platform, user_id)) FROM memory_fact_cluster
                 WHERE status IN ('active','profiled')) AS users_with_clusters
            """
        )
        kv = await conn.fetch(
            "SELECT key, value, updated_at FROM _memory_local_kv ORDER BY key"
        )
    return dict(row), [dict(r) for r in kv]


async def fetch_users_data(backend, keyword: str, page: int, size: int) -> dict:
    """有事实/簇的用户清单（画像页用户选择器数据源）——原始查询。"""
    params: list = []
    where = ""
    if keyword:
        params.append(_ilike(keyword))
        where = "WHERE c.user_id ILIKE $1"
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
            OFFSET ${len(params)+1} LIMIT ${len(params)+2}
            """,
            *params,
            (page - 1) * size,
            size,
        )
        # 名字映射只查当前页用户（原全表 DISTINCT ON 在大库上是与分页
        # 查询不成比例的整表扫描）
        pairs = [(r["platform"], r["user_id"]) for r in rows]
        names = []
        if pairs:
            values_sql = ", ".join(
                f"(${i * 2 + 1}::text, ${i * 2 + 2}::text)"
                for i in range(len(pairs))
            )
            flat = [v for pair in pairs for v in pair]
            names = await conn.fetch(
                f"""
                SELECT DISTINCT ON (platform, user_id) platform, user_id, display_name
                FROM memory_persona_fact_raw
                WHERE display_name <> ''
                  AND (platform, user_id) IN ({values_sql})
                ORDER BY platform, user_id, occurred_at DESC, id DESC
                """,
                *flat,
            )
    return {
        "total": total or 0,
        "rows": [dict(r) for r in rows],
        "names": [dict(r) for r in names],
    }


async def delete_fact(backend, fact_id: int) -> dict:
    """删除原始事实并同步清理簇表 source_fact_ids 引用（同一事务）。"""
    async with _pool(backend).acquire() as conn:
        async with conn.transaction():
            row = await conn.fetchrow(
                "DELETE FROM memory_persona_fact_raw WHERE id = $1 "
                "RETURNING id, user_id, statement, extracted_flag",
                fact_id,
            )
            if row is None:
                raise HTTPException(status_code=404, detail="事实不存在")
            await conn.execute(
                "UPDATE memory_fact_cluster "
                "SET source_fact_ids = array_remove(source_fact_ids, $1::bigint) "
                "WHERE $1::bigint = ANY(source_fact_ids)",
                fact_id,
            )
    return dict(row)


async def fetch_clusters_data(
    backend, page: int, size: int, filters: dict
) -> dict:
    """事实簇分页浏览（画像来源，含确信度分值）——原始查询。

    filters 为 webui_store 层白名单校验后的取值（user_id/category/status/
    session_id/q）。
    """
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
        # 簇无 session 列：按证据键 {session}|{date} 前缀匹配（元字符转义
        # 而非剥离——含 _/% 的会话 ID 也能如实匹配）
        add(
            "EXISTS (SELECT 1 FROM unnest(evidence_keys) e WHERE e LIKE ${n})",
            filters["session_evidence_prefix"],
        )
    if filters["q"]:
        add("canonical_statement ILIKE ${n}", filters["q_pattern"])

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
                   cardinality(source_fact_ids) AS source_fact_count
            FROM memory_fact_cluster {where}
            ORDER BY score DESC, updated_at DESC, id ASC
            OFFSET ${len(params)+1} LIMIT ${len(params)+2}
            """,
            *params,
            (page - 1) * size,
            size,
        )
    return {"total": total or 0, "rows": [dict(r) for r in rows]}


async def select_cluster_for_update(conn, cluster_id: int):
    """簇行读取并加行锁（PG：FOR UPDATE，防并发修正互踩）。"""
    return await conn.fetchrow(
        "SELECT * FROM memory_fact_cluster WHERE id = $1 FOR UPDATE",
        cluster_id,
    )


async def update_cluster_row(
    conn, cluster_id: int, statement: str, score, status: str,
    statement_changed: bool,
) -> None:
    """簇修正 UPDATE（PG：now() 生成时间戳）。"""
    await conn.execute(
        """
        UPDATE memory_fact_cluster
        SET canonical_statement = $2, score = $3, status = $4,
            updated_at = now(),
            embedding = CASE WHEN $5 THEN NULL ELSE embedding END,
            demoted_at = CASE
                WHEN $4 = 'pending_uncertain' AND status <> 'pending_uncertain'
                    THEN now()
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
    )


async def fetch_relation_graph_data(backend) -> dict:
    """关系图谱原始查询：全量边（含 pending）+ 别名行（当前页 uid 过滤）。"""
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
        # Alias lookup filtered by the uids appearing in this page of edges
        # (full-table scans of the alias layer scale with history, not graph).
        owners: set[tuple[str, str]] = set()
        for r in rows:
            for col_uid in ("subject_uid", "object_uid"):
                uid = str(r[col_uid] or "")
                if uid:
                    owners.add((str(r["platform"] or ""), uid))
        alias_rows: list = []
        if owners:
            alias_rows = await conn.fetch(
                """
                SELECT a.platform, a.user_id, a.name
                FROM memory_entity_alias a
                JOIN unnest($1::text[], $2::text[]) AS k(platform, user_id)
                  ON a.platform = k.platform AND a.user_id = k.user_id
                ORDER BY a.last_seen DESC, a.name ASC
                """,
                [p for p, _ in owners],
                [u for _, u in owners],
            )
    return {
        "rows": [dict(r) for r in rows],
        "total": total or 0,
        "alias_rows": [dict(r) for r in alias_rows],
    }


async def update_relation_edge(backend, edge_id: int, new_status: str) -> dict:
    """人工修正边状态（墓碑保护语义见 webui_store.update_relation_edge）。"""
    async with _pool(backend).acquire() as conn:
        # Capture the pre-update status first: RETURNING only exposes the NEW
        # row, logging "old->new" from it would print new->new.
        cur = await conn.fetchrow(
            "SELECT status FROM memory_entity_edge WHERE id = $1", edge_id
        )
        if cur is None:
            raise HTTPException(status_code=404, detail="边不存在")
        row = await conn.fetchrow(
            """
            UPDATE memory_entity_edge
            SET status = $2,
                updated_at = now(),
                superseded_at = CASE WHEN $2 = 'superseded' THEN now()
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
        )
    if row is None:
        raise HTTPException(status_code=404, detail="边不存在")
    return {**dict(row), "pre_status": cur["status"]}

