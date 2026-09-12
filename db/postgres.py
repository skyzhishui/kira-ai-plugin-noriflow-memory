"""Local memory store access layer — PostgreSQL backend (asyncpg pool +
migration runner + write operations).

After the db package split (dual-storage plan §3/§5) this module is the PG
backend implementation, byte-identical in SQL and behavior to the pre-split
code; dialect-agnostic helpers/table constants/interface contract live in
base.py, the SQLite backend in sqlite.py, the assembly factory in
factory.py.

Responsibilities:
- pool lifecycle (connect/close) and schema migrations (versioned SQL in
  migrations/, independent of the repository's alembic — that one targets
  the MySQL business database);
- write-path operations: summary/raw-fact inserts (document_id unique-key
  idempotent via ON CONFLICT DO NOTHING, so retain retries/overlaps never
  duplicate rows);
- merge-agent support (M4): pending raw batch fetch / cluster-candidate
  search / single-transaction disposal (join-score-create-replace +
  flag-flip optimistic lock) / decay pass / promotion pass / kv state;
- embedding backfill support: scan embedding IS NULL rows per table.

The caller (LocalMemoryKernel) owns circuit-breaking and fail-open
semantics; this layer lets exceptions propagate unchanged.
"""

from __future__ import annotations

from datetime import datetime
from pathlib import Path

import asyncpg

from core.logging_manager import get_logger

from ..alias_store import PLACEHOLDER_NAME_SQL, is_placeholder_name
from ..config import LocalMemoryConfig
from ..entity_edge import (
    filter_stopword_rows,
    split_reverse_echo_rows,
)
from .base import (
    CHAT_SUMMARY_TABLE,
    FACT_CLUSTER_TABLE,
    FACT_RAW_TABLE,
    MemoryBackend,
    _MIGRATION_NAME_RE,
    _Params,
    _ensure_tz,
    _like_contains_pattern,
    _rrf_fuse,
    build_search_text,
    vector_literal,
)

logger = get_logger("noriflow_memory.db", "cyan")

_MIGRATIONS_TABLE_DDL = """
CREATE TABLE IF NOT EXISTS _memory_local_migrations (
    version    INT PRIMARY KEY,
    name       TEXT NOT NULL,
    applied_at TIMESTAMPTZ NOT NULL DEFAULT now()
)
"""


# Migration-executor mutual-exclusion lock key (pg_advisory_lock,
# session-scoped): any fixed bigint, used only to serialize this plugin's
# migration runner across instances
_MIGRATION_LOCK_KEY = 830_210_005


async def supersede_backfill_edges_of(conn, cluster_ids: list[int]) -> int:
    """Retire backfill-sourced edges of dead/replaced clusters (same txn).

    An edge is retired only when its whole evidence set consists of
    ``backfill|{cluster_id}`` keys AND every referenced cluster is in
    ``cluster_ids`` — edges that also carry online evidence (``session|date``
    keys) or a living backfill source stay active. Used by the merge replace
    path, manual cluster kills and per-user erasure so a corrected/replaced
    cluster ("表姐不是姐姐") stops injecting its old relation edges.

    Args:
        conn: asyncpg connection (caller owns the transaction).
        cluster_ids: ids of clusters that just became replaced/dead/deleted.

    Returns:
        Number of edges set to superseded.
    """
    dead = {int(i) for i in cluster_ids or [] if int(i) > 0}
    if not dead:
        return 0
    rows = await conn.fetch(
        """
        SELECT id, evidence_keys FROM memory_entity_edge
        WHERE status = 'active'
          AND EXISTS (SELECT 1 FROM unnest(evidence_keys) k
                      WHERE k LIKE 'backfill|%')
        """
    )
    doomed: list[int] = []
    for r in rows:
        sources: set[int] = set()
        has_online_evidence = False
        for key in list(r["evidence_keys"] or []):
            if str(key).startswith("backfill|"):
                try:
                    sources.add(int(str(key).split("|", 1)[1]))
                except ValueError:
                    continue
            else:
                has_online_evidence = True
        if has_online_evidence:
            continue
        if sources and sources <= dead:
            doomed.append(r["id"])
    if doomed:
        await conn.execute(
            """
            UPDATE memory_entity_edge
            SET status = 'superseded', superseded_at = now(),
                supersede_reason = 'source cluster removed', updated_at = now()
            WHERE id = ANY($1::bigint[])
            """,
            doomed,
        )
    return len(doomed)


class MemoryDatabase(MemoryBackend):
    """Local memory store (PostgreSQL + pgvector) access layer.

    Attributes:
        dsn: connection string (contains the password; never log it).
        pool: asyncpg connection pool (available after connect).
    """

    def __init__(self, config: LocalMemoryConfig) -> None:
        """Initialize (no connection is made yet).

        Args:
            config: runtime plugin config (dsn/pool_min/pool_max/
                db_command_timeout).
        """
        self.dsn = config.dsn
        self._pool_min = config.pool_min
        self._pool_max = config.pool_max
        # Per-statement timeout in seconds (0 = disabled): keeps a hung
        # query from pinning pool slots forever and starving the whole
        # memory subsystem (recall/merge/WebUI share this pool)
        self._command_timeout = float(config.db_command_timeout or 0)
        self._pool: asyncpg.Pool | None = None

    @property
    def pool(self) -> asyncpg.Pool:
        """Connection pool (raises RuntimeError before connect)."""
        if self._pool is None:
            raise RuntimeError("MemoryDatabase 未连接（connect 先于任何操作）")
        return self._pool

    # Backend dialect tag (webui_store dispatches SQL per backend)
    dialect = "postgres"

    async def supersede_backfill_edges_of(self, conn, cluster_ids: list[int]) -> int:
        """Backend method form (webui_store dispatches via the backend object)."""
        return await supersede_backfill_edges_of(conn, cluster_ids)

    async def connect(self) -> None:
        """Create the pool and verify with a ping (idempotent: skips when
        already connected).

        Raises:
            RuntimeError: connection failure (bad DSN/unreachable service);
            the caller degrades gracefully.
        """
        if self._pool is not None:
            return
        self._pool = await asyncpg.create_pool(
            self.dsn,
            min_size=self._pool_min,
            max_size=self._pool_max,
            command_timeout=self._command_timeout or None,
        )
        try:
            async with self.pool.acquire() as conn:
                await conn.execute("SELECT 1")
        except Exception:
            # On ping failure close the half-initialized pool before
            # re-raising: otherwise a connect() retry would return early
            # because _pool is already set, breaking the "connected means
            # verified" invariant
            await self.close()
            raise
        logger.info(
            "本地记忆库连接成功: pool=%d-%d", self._pool_min, self._pool_max
        )

    async def close(self) -> None:
        """Close the pool (idempotent; exceptions are logged, not raised)."""
        if self._pool is not None:
            try:
                await self._pool.close()
            except Exception:
                logger.warning("关闭记忆库连接池时发生异常", exc_info=True)
            self._pool = None

    # ------------------------------------------------------------------
    #  schema migrations
    # ------------------------------------------------------------------

    async def apply_migrations(self, migrations_dir: str | Path) -> list[int]:
        """Apply pending migration scripts in version order (one
        transaction per script; the version is recorded after applying).

        Args:
            migrations_dir: migration script directory (plugin migrations/).

        Returns:
            Versions applied in this run (empty when up to date).

        Raises:
            FileNotFoundError: directory missing.
            asyncpg exceptions: DDL failure (transaction rolled back;
            propagated for the caller to degrade gracefully).
        """
        directory = Path(migrations_dir)
        if not directory.is_dir():
            raise FileNotFoundError(f"迁移目录不存在: {directory}")

        applied: list[int] = []
        async with self.pool.acquire() as conn:
            # Cross-instance mutual exclusion: serializes migration runs
            # when several instances start together (no contention in
            # single-instance deployments; PG releases the session advisory
            # lock automatically when the connection drops)
            await conn.execute("SELECT pg_advisory_lock($1)", _MIGRATION_LOCK_KEY)
            try:
                await conn.execute(_MIGRATIONS_TABLE_DDL)
                rows = await conn.fetch(
                    "SELECT version FROM _memory_local_migrations"
                )
                done = {r["version"] for r in rows}

                for path in sorted(directory.glob("*.sql")):
                    match = _MIGRATION_NAME_RE.match(path.name)
                    if match is None:
                        logger.warning("跳过不合规的迁移文件名: %s", path.name)
                        continue
                    version = int(match.group(1))
                    if version in done:
                        continue
                    sql = path.read_text(encoding="utf-8")
                    async with conn.transaction():
                        await conn.execute(sql)
                        await conn.execute(
                            "INSERT INTO _memory_local_migrations (version, name) "
                            "VALUES ($1, $2) ON CONFLICT (version) DO NOTHING",
                            version,
                            path.name,
                        )
                    applied.append(version)
                    logger.info("记忆库迁移已应用: %s", path.name)
            finally:
                # Unlock failures are logged only: on a broken connection a
                # secondary exception here would mask the real migration
                # failure (PG auto-releases the session lock on disconnect,
                # so nothing leaks)
                try:
                    await conn.execute(
                        "SELECT pg_advisory_unlock($1)", _MIGRATION_LOCK_KEY
                    )
                except Exception:
                    logger.warning("迁移 advisory lock 解锁失败（忽略）", exc_info=True)

        if not applied:
            logger.info("记忆库 schema 已是最新（无待应用迁移）")
        return applied

    # ------------------------------------------------------------------
    #  write path (M2)
    # ------------------------------------------------------------------

    async def insert_chat_summary(
        self,
        *,
        document_id: str,
        kind: str,
        platform: str,
        session_id: str,
        group_id: str,
        user_id: str,
        participants: list[str],
        content: str,
        occurred_at: datetime,
        embedding: list[float] | None,
        summarized: bool = True,
    ) -> None:
        """Insert a chat summary (or bot_self raw text); document_id
        conflicts are silently skipped (idempotent).

        Args:
            document_id: idempotency key ({session_id}-{md5(content)[:12]} /
                bot-self-{hash}).
            kind: chat_summary | bot_self.
            platform: platform tag (e.g. "qq").
            session_id: session id.
            group_id: group id (empty = private chat).
            user_id: trigger speaker (bare uid).
            participants: composite keys of every speaker this round
                ("platform:uid").
            content: summary body (or bot_self raw text).
            occurred_at: occurrence time (this round's trigger message time).
            embedding: vector computed at write time; None writes NULL (the
                backfill task fills it later).
            summarized: encoding state (false = raw conversation text from a
                degraded encode, excluded from recall until the merge
                agent's catch-up pass re-encodes it back to true; bot_self
                raw text is by-design and always passes true).
        """
        async with self.pool.acquire() as conn:
            await conn.execute(
                """
                INSERT INTO memory_chat_summary
                    (document_id, kind, platform, session_id, group_id, user_id,
                     participants, content, occurred_at, embedding, summarized,
                     search_text)
                VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10::vector, $11, $12)
                ON CONFLICT (document_id) DO NOTHING
                """,
                document_id,
                kind,
                platform,
                session_id,
                group_id,
                user_id,
                participants,
                content,
                _ensure_tz(occurred_at),
                vector_literal(embedding),
                summarized,
                build_search_text(content),
            )

    async def insert_persona_fact_raw(
        self,
        *,
        document_id: str,
        platform: str,
        user_id: str,
        related_user_ids: list[str],
        display_name: str,
        category: str,
        statement: str,
        confidence: str,
        session_id: str,
        group_id: str,
        evidence_key: str,
        occurred_at: datetime,
        embedding: list[float] | None,
    ) -> None:
        """Insert a raw persona fact (extracted_flag=0, awaiting the merge
        agent).

        Args:
            document_id: idempotency key (fact_document_id prefix +
                md5(statement)[:12]; granularity = ownership uid set +
                session + day).
            platform: platform tag.
            user_id: primary owner of the fact (bare uid).
            related_user_ids: other parties of a relation fact (bare uids).
            display_name: display name at extraction time.
            category: one of the six dimensions.
            statement: the fact statement as extracted.
            confidence: high | medium.
            session_id: source session id (part of the scoring evidence key).
            group_id: source group id.
            evidence_key: scoring dedup key "{session_id}|{occurred_at:date}".
            occurred_at: occurrence time (this round's trigger message time).
            embedding: statement vector; None writes NULL (backfilled later).
        """
        async with self.pool.acquire() as conn:
            await conn.execute(
                """
                INSERT INTO memory_persona_fact_raw
                    (document_id, platform, user_id, related_user_ids, display_name,
                     category, statement, confidence, session_id, group_id,
                     evidence_key, occurred_at, embedding)
                VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13::vector)
                ON CONFLICT (document_id) DO NOTHING
                """,
                document_id,
                platform,
                user_id,
                related_user_ids,
                display_name,
                category,
                statement,
                confidence,
                session_id,
                group_id,
                evidence_key,
                _ensure_tz(occurred_at),
                vector_literal(embedding),
            )

    # ------------------------------------------------------------------
    #  recall search (M3)
    # ------------------------------------------------------------------

    async def search_chat_summaries(
        self,
        *,
        query_vec: list[float],
        limit: int,
        scope: str = "session",
        session_id: str = "",
        platform: str = "",
        cross_session: bool = False,
        user_keys: list[str] | None = None,
        user_ids: list[str] | None = None,
        exclude_kinds: list[str] | None = None,
        exclude_content_keywords: list[str] | None = None,
        expanded_user_keys: list[str] | None = None,
        expanded_user_ids: list[str] | None = None,
        entity_user_keys: list[str] | None = None,
        entity_user_ids: list[str] | None = None,
        exclude_recent_batches: int = 0,
        query_text: str = "",
        hybrid: bool = False,
        rrf_k: int = 60,
        exclude_document_ids: list[str] | None = None,
        with_embedding: bool = True,
        with_participants: bool = False,
    ) -> list[dict]:
        """Vector search over the summary table (optional BM25 hybrid;
        scope filtering + cosine ranking, single-pool mode).

        Condition-assembly semantics (AND composition aligned with the
        hindsight tag_groups design):
        - Session condition: scope=session -> (session_id=sid AND summarized)
          OR (kind='bot_self' AND session_id=sid) — the bot_self exemption
          has been session-scoped since v1.9.x (same privacy posture as
          session-isolated summary recall; bot_self rows have empty
          participants so the explicit OR is required to hit them in their
          own session); cross_session=True widens it to summarized AND
          kind IN ('chat_summary','bot_self'); scope=user_session ->
          session_id=sid AND summarized (tighter; bot_self stays reachable
          via the user-group OR);
        - scope=user -> no session condition (user filtering is inherently
          cross-session, and bot_self never enters user-domain recall —
          without a session context there is no basis for the exemption);
        - User condition (primary key group; user_keys/user_ids non-empty):
          ((any composite key = ANY(participants) OR user_id = ANY(bare
          uid list)) AND summarized OR (kind='bot_self' AND
          session_id=sid)) — the participants array guarantees precise
          recall for users who took part in a round without triggering
          retain;
        - Expansion key group (expanded_user_keys/expanded_user_ids,
          recall widening): hit conditions match the primary group but are
          always AND-pinned to session_id=sid — the "asking about an absent
          member" scenario can hit that member's summaries from the current
          session, while the member's cross-session (including private)
          memories never leak in regardless of the cross_session switch
          (structural leak prevention, no reliance on caller guards);
        - Entity key group (entity_user_keys/entity_user_ids,
          asking-about-others recall): (platform, uid) pairs hit by query
          entities (window dictionary / persistent alias layer). With
          cross_session=True they merge into the primary group
          (cross-session — any summary of the mentioned person, including
          their private chats with the bot, becomes recallable; the privacy
          posture is the caller's isolation config); with
          cross_session=False they are session-pinned exactly like the
          expansion group (only the entity's current-session summaries);
        - Recency exclusion (exclude_recent_batches, batch-count anchored):
          NOT (session_id=sid AND id IN (latest K encoded summary batches))
          — the host window truncates by blocks and its content carries no
          timestamps, so "latest K batches" equals "already visible inside
          the window" (exactly one batch per round); those rows leave the
          recall set and free slots for information outside the window;
        - Topic blacklist (exclude_content_keywords): content NOT LIKE
          ALL(pattern list) — structural exclusion at the SQL candidate
          layer; blacklisted rows neither enter the candidate set nor
          consume top_k slots (truncation happens at the pipeline end); the
          injection layer keeps a second Python defense line;
        - Row exclusion (exclude_document_ids): rows already injected by the
          rolling-restore block stay out of the candidate set (no double
          injection within a round);
        - summarized=false (raw conversation text from a degraded encode)
          is never recalled, bot_self exempt (raw text is by design); bot_self
          rows are always summarized=true, so the exemption clause is a
          defensive fallback;
        - exclude_kinds: kind <> ALL(list) (exclude the given kinds).

        Hybrid search (hybrid=True with non-empty query_text, plan A):
        - Vector leg: cosine order LIMIT limit (the original path);
        - BM25 leg: same where + to_tsvector('simple', search_text) @@
          to_tsquery('simple', OR-joined token query) (AND-joining a whole
          sentence's bigrams would only match near-verbatim repeats), in
          ts_rank order LIMIT limit; legacy rows with NULL search_text
          simply skip the BM25 leg (no impact until the backfill pass fills
          them);
        - The two legs fuse via RRF (score = Σ 1/(rrf_k + rank), merged and
          deduplicated by id); the fused score lands in the "rrf" field and
          rows return in its descending order. With rerank enabled the fused
          order is overridden by absolute scores — its value is the
          candidate-pool composition (rare entries the vector top-N missed
          enter via BM25) and the final order when rerank degrades.

        Args:
            query_vec: query vector.
            limit: candidate cap (how much reaches rerank).
            scope: session | user | user_session.
            session_id: session id (consumed by scope=session/user_session;
                stored bare; platform disambiguates same-numbered sessions
                across adapters).
            platform: platform tag (non-empty appends platform = $ to the
                session-domain condition so same-numbered sessions from
                other adapters cannot mix in or pollute the recency-exclusion
                anchor; empty = unrestricted, for legacy callers).
            cross_session: cross-session widening (only effective for
                scope=session).
            user_keys: participant composite keys ("platform:uid"; primary
                key group).
            user_ids: bare uid list (completes the primary-group user filter
                together with user_keys).
            exclude_kinds: kinds to exclude.
            exclude_content_keywords: topic blacklist keywords (substring
                semantics; SQL candidate-layer exclusion — blacklisted rows
                consume no top_k slots).
            expanded_user_keys: expansion participant composite keys (always
                session-pinned).
            expanded_user_ids: expansion participant bare uids (as above).
            entity_user_keys: entity-hit composite keys (merged into the
                primary group cross-session when cross_session=True;
                otherwise session-pinned, see above).
            entity_user_ids: entity-hit bare uids (as above).
            exclude_recent_batches: how many recent batches to exclude
                (= host window block count; 0 disables).
            query_text: raw query text (BM25 leg tokenization when hybrid).
            hybrid: enable hybrid search.
            rrf_k: RRF fusion constant.
            exclude_document_ids: document_ids to exclude.
            with_embedding: include the embedding column in SELECT (saves
                ~10KB/row transfer when dedup is off).
            with_participants: include the participants column (per_user
                quota attribution; the single-pool default path omits it to
                save transfer).

        Returns:
            Dict rows (id/document_id/kind/session_id/user_id/content/
            occurred_at/embedding/relevance, plus rrf when hybrid; embedding
            feeds the caller's near-duplicate dedup, participants feed
            per_user attribution).
        """
        if scope not in ("session", "user", "user_session"):
            raise ValueError(f"不支持的检索 scope: {scope}")

        p = _Params(vector_literal(query_vec))
        conds: list[str] = []

        if scope == "session":
            if cross_session:
                conds.append(
                    "(summarized AND kind IN ('chat_summary', 'bot_self')"
                    " OR kind = 'bot_self')"
                )
            elif session_id:
                sess_cond = f"session_id = {p.add(session_id)}"
                if platform:
                    sess_cond += f" AND platform = {p.add(platform)}"
                # bot_self exemption is session-scoped (same privacy posture
                # as v1.9.0 session isolation): bot_self rows have empty
                # participants, so they need the explicit OR to stay
                # recallable in their own session — but they must not leak
                # other sessions' bot self-talk into this session's recall.
                conds.append(
                    f"(({sess_cond} AND summarized)"
                    f" OR (kind = 'bot_self' AND {sess_cond}))"
                )
        elif scope == "user_session" and session_id:
            sess_cond = f"session_id = {p.add(session_id)}"
            if platform:
                sess_cond += f" AND platform = {p.add(platform)}"
            conds.append(f"({sess_cond} AND summarized)")

        user_blocks: list[str] = []
        primary_parts: list[str] = []
        keys = [k for k in (user_keys or []) if k]
        uids = [u for u in (user_ids or []) if u]
        if keys:
            # Array overlap (GIN-indexable; the core filter of the shared
            # multi-user pool model)
            primary_parts.append(f"participants && {p.add(keys)}")
        if uids:
            primary_parts.append(f"user_id = ANY({p.add(uids)})")
        # Entity key group (asking-about-others recall): with cross-session
        # recall open, entity hits join the primary key group (OR-pooled with
        # the current speaker; any session of the mentioned person becomes
        # recallable); under session isolation they land in the session-pinned
        # block below (that person's current-session summaries only)
        ent_keys = [k for k in (entity_user_keys or []) if k]
        ent_uids = [u for u in (entity_user_ids or []) if u]
        ent_parts: list[str] = []
        if ent_keys:
            ent_parts.append(f"participants && {p.add(ent_keys)}")
        if ent_uids:
            ent_parts.append(f"user_id = ANY({p.add(ent_uids)})")
        if ent_parts and cross_session:
            primary_parts.extend(ent_parts)
        if primary_parts:
            # User-hit rows must be encoded (summarized); bot_self raw
            # text is exempt by design
            user_blocks.append("(" + " OR ".join(primary_parts) + ") AND summarized")

        exp_parts: list[str] = []
        exp_keys = [k for k in (expanded_user_keys or []) if k]
        exp_uids = [u for u in (expanded_user_ids or []) if u]
        if exp_keys:
            exp_parts.append(f"participants && {p.add(exp_keys)}")
        if exp_uids:
            exp_parts.append(f"user_id = ANY({p.add(exp_uids)})")
        if exp_parts and session_id:
            # Expansion hits are always pinned to the current session (see
            # docstring; independent of the cross_session switch)
            exp_pin = f"session_id = {p.add(session_id)}"
            if platform:
                exp_pin += f" AND platform = {p.add(platform)}"
            user_blocks.append(
                "(" + " OR ".join(exp_parts) + ") AND summarized"
                f" AND {exp_pin}"
            )
        if ent_parts and not cross_session and session_id:
            # Entity-key landing spot under session isolation (see docstring):
            # same session-pinned shape as the expansion block
            ent_pin = f"session_id = {p.add(session_id)}"
            if platform:
                ent_pin += f" AND platform = {p.add(platform)}"
            user_blocks.append(
                "(" + " OR ".join(ent_parts) + ") AND summarized"
                f" AND {ent_pin}"
            )
        if user_blocks:
            # bot_self rows have empty participants; scope their exemption to
            # the current session (see the scope=session branch). Without a
            # session context (scope=user) there is no justification for
            # pulling bot self-talk into a user's recall at all.
            if session_id:
                bot_self_cond = f"kind = 'bot_self' AND session_id = {p.add(session_id)}"
                if platform:
                    bot_self_cond += f" AND platform = {p.add(platform)}"
                conds.append(
                    "(" + " OR ".join(user_blocks) + f" OR ({bot_self_cond}))"
                )
            else:
                conds.append("(" + " OR ".join(user_blocks) + ")")

        if exclude_kinds:
            conds.append(f"kind <> ALL({p.add(list(exclude_kinds))})")

        # Topic blacklist (structural exclusion at the SQL candidate
        # layer): blacklisted rows never enter the candidate set, and the
        # top_k truncation happens at the end of the pipeline, so filtered
        # rows no longer waste slots. The injection layer keeps a second
        # Python defense line (memory_kernel.build_injection_text).
        if exclude_content_keywords:
            patterns = [
                _like_contains_pattern(kw)
                for kw in exclude_content_keywords
                if kw
            ]
            if patterns:
                conds.append(f"content NOT LIKE ALL({p.add(patterns)})")

        # Recency exclusion (batch-count anchored): the latest K encoded
        # summary batches equal what the host window already shows; the
        # sub-query ordering matches fetch_recent_rollout_summaries exactly
        # (the restore side converts OFFSET by "K - bot_self batches inside
        # the window" to land exactly on the exclusion boundary). Every
        # kind counts (bot_self included — it is visible in the window too).
        if exclude_recent_batches > 0 and session_id:
            sub_where = f"session_id = {p.add(session_id)}"
            if platform:
                sub_where += f" AND platform = {p.add(platform)}"
            sub_sql = (
                f"SELECT id FROM {CHAT_SUMMARY_TABLE} "
                f"WHERE {sub_where} AND summarized "
                f"ORDER BY occurred_at DESC, id DESC "
                f"LIMIT {p.add(int(exclude_recent_batches))}"
            )
            conds.append(
                f"NOT (session_id = {p.add(session_id)} AND id IN ({sub_sql}))"
            )

        # Row exclusion: rows already injected by the rolling-restore block
        # stay out of the candidate set (no double injection in one round)
        if exclude_document_ids:
            conds.append(
                f"document_id <> ALL({p.add(list(exclude_document_ids))})"
            )

        # Fallback filter: when conds ends up empty (e.g. scope=session with
        # no session_id and cross_session off — the wide-recall path),
        # un-encoded fallback rows are excluded the same way, bot_self
        # exempt; an empty non-cross_session condition means the caller
        # omitted a required argument, so warn to make the over-broad
        # surface easier to diagnose
        if not conds and not cross_session:
            logger.warning(
                "search_chat_summaries 无过滤条件（scope=%s, session_id=%r），"
                "退化为全库召回",
                scope,
                session_id or "",
            )
        where = (
            " AND ".join(conds) if conds else "(summarized OR kind = 'bot_self')"
        )
        select_cols = (
            "id, document_id, kind, session_id, user_id, content, "
            "occurred_at"
            + (", participants" if with_participants else "")
            + (", embedding" if with_embedding else "")
            + ", 1 - (embedding <=> $1::vector) AS relevance"
        )
        async with self.pool.acquire() as conn:
            # The LIMIT placeholder number is shared by both SQL strings
            # (same value) — asyncpg forbids numbering gaps: an unreferenced
            # param number has no type at PREPARE time and positional
            # encoding raises IndeterminateDatatypeError (the upstream host
            # once hit an incident with this exact root cause)
            limit_ph = p.add(limit)
            vec_sql = (
                f"SELECT {select_cols} "
                f"FROM {CHAT_SUMMARY_TABLE} "
                f"WHERE embedding IS NOT NULL AND ({where}) "
                f"ORDER BY embedding <=> $1::vector LIMIT {limit_ph}"
            )
            vec_rows = [dict(r) for r in await conn.fetch(vec_sql, *p.values)]

            # BM25 leg (plan A): same where + full-text match on the
            # tokenized column, ts_rank order. Query tokens join with OR
            # (to_tsquery ' | '): production queries are whole sentences
            # (a dozen-plus bigrams) and AND would require the document to
            # contain every bigram — a near-verbatim repeat; OR + ts_rank
            # naturally favors documents matching more tokens. Token shapes
            # (CJK bigrams / ASCII alphanumerics) contain no to_tsquery
            # metacharacters, so no escaping is needed
            tokens = build_search_text(query_text).split() if hybrid else []
            if not tokens:
                return vec_rows
            q_ph = p.add(" | ".join(tokens))
            bm25_sql = (
                f"SELECT {select_cols} "
                f"FROM {CHAT_SUMMARY_TABLE} "
                f"WHERE embedding IS NOT NULL AND ({where}) "
                f"AND to_tsvector('simple', search_text) @@ "
                f"to_tsquery('simple', {q_ph}) "
                f"ORDER BY ts_rank(to_tsvector('simple', search_text), "
                f"to_tsquery('simple', {q_ph})) DESC LIMIT {limit_ph}"
            )
            bm25_rows = [dict(r) for r in await conn.fetch(bm25_sql, *p.values)]
        return _rrf_fuse(vec_rows, bm25_rows, rrf_k)

    async def fetch_recent_session_participants(
        self, *, session_id: str, limit: int, platform: str = ""
    ) -> list[dict]:
        """Participants of the session's latest N summary batches (source
        of expansion keys for recall widening).

        Samples in reverse via the (session_id, occurred_at DESC) index; the
        caller merges/dedups the returned participants (composite keys) and
        user_id (bare uid), drops the bot itself, and passes the result to
        search_chat_summaries as the expansion key group.

        Args:
            session_id: session id (bare; platform disambiguates
                same-numbered sessions across adapters).
            limit: how many recent summary batches to sample.
            platform: platform tag (non-empty appends platform = $).

        Returns:
            Dict rows (participants / user_id).
        """
        async with self.pool.acquire() as conn:
            rows = await conn.fetch(
                f"SELECT participants, user_id FROM {CHAT_SUMMARY_TABLE} "  # noqa: S608 -- whitelist constant table names
                "WHERE session_id = $1 AND summarized "
                + ("AND platform = $3 " if platform else "")
                + "ORDER BY occurred_at DESC LIMIT $2",
                session_id,
                limit,
                *([platform] if platform else []),
            )
        return [dict(r) for r in rows]

    async def fetch_recent_summary_scores(
        self, *, query_vec: list[float], session_id: str, limit: int, platform: str = ""
    ) -> list[dict]:
        """Cosine similarity between the session's latest N encoded
        summaries and a given vector (write-side near-duplicate check).

        Takes the most recent limit encoded chat_summary rows by
        (occurred_at DESC, id DESC) (embedding non-null, same order as the
        recency-exclusion sub-query); returns id/content/score rows
        (score = 1 - cosine distance, higher = more similar).

        Args:
            query_vec: embedding of the new summary content.
            session_id: session id (bare; platform disambiguates
                same-numbered sessions across adapters).
            limit: window size (recent batch count).
            platform: platform tag (non-empty appends platform = $).

        Returns:
            Dict rows (id / content / score) in occurred_at-descending
            order; rows with NULL embedding inside the window do not
            participate (nothing to compare — never a false kill).
        """
        async with self.pool.acquire() as conn:
            rows = await conn.fetch(
                f"SELECT id, content, 1 - (embedding <=> $1::vector) AS score "
                f"FROM {CHAT_SUMMARY_TABLE} "  # noqa: S608 -- whitelist constant table names
                "WHERE session_id = $2 AND kind = 'chat_summary' AND summarized "
                "AND embedding IS NOT NULL "
                + ("AND platform = $4 " if platform else "")
                + "ORDER BY occurred_at DESC, id DESC LIMIT $3",
                vector_literal(query_vec),
                session_id,
                limit,
                *([platform] if platform else []),
            )
        return [dict(r) for r in rows]

    # ------------------------------------------------------------------
    #  embedding backfill (consumed by the vector_ops background task)
    # ------------------------------------------------------------------

    async def fetch_missing_embeddings(
        self, table: str, limit: int
    ) -> list[tuple[int, str]]:
        """Scan rows with embedding IS NULL in the given table (id
        ascending, capped).

        The two tables use different text column names (summary content /
        fact statement); this returns uniform (id, text) tuples so callers
        need not care.

        Args:
            table: table name (whitelisted: summary/fact/cluster table).
            limit: batch size.

        Returns:
            (row id, text) tuple list.

        Raises:
            ValueError: table name outside the whitelist.
        """
        if table not in _BACKFILL_TABLES:
            raise ValueError(f"补算扫描不支持表: {table}")
        text_col = _BACKFILL_TEXT_COLUMNS[table]
        async with self.pool.acquire() as conn:
            rows = await conn.fetch(
                f"SELECT id, {text_col} AS text FROM {table} "  # noqa: S608 -- whitelist constant table/column names
                "WHERE embedding IS NULL ORDER BY id LIMIT $1",
                limit,
            )
        return [(r["id"], r["text"]) for r in rows]

    async def update_embedding(
        self, table: str, row_id: int, embedding: list[float]
    ) -> None:
        """Backfill one row's embedding.

        Args:
            table: table name (whitelisted: summary/fact/cluster table).
            row_id: row id.
            embedding: vector (never None — caller guarantees it).

        Raises:
            ValueError: table name outside the whitelist.
        """
        if table not in _BACKFILL_TABLES:
            raise ValueError(f"补算回填不支持表: {table}")
        async with self.pool.acquire() as conn:
            await conn.execute(
                f"UPDATE {table} SET embedding = $2::vector WHERE id = $1",  # noqa: S608
                row_id,
                vector_literal(embedding),
            )

    # ------------------------------------------------------------------
    #  merge agent (M4, dev plan §8)
    # ------------------------------------------------------------------

    async def fetch_unsummarized_summaries(self, limit: int) -> list[dict]:
        """Fetch a batch of un-encoded degraded-text rows (summarized=false,
        id ascending).

        Consumed by the encode catch-up pass: bot_self rows are always
        summarized=true and fall outside the scan (the condition excludes
        them defensively); only the fields the catch-up needs are selected,
        and occurred_at doubles as part of the facts' evidence_key.

        Args:
            limit: batch size.

        Returns:
            Dict rows (id/session_id/group_id/platform/participants/user_id/
            content/occurred_at).
        """
        async with self.pool.acquire() as conn:
            rows = await conn.fetch(
                """
                SELECT id, session_id, group_id, platform, participants,
                       user_id, content, occurred_at
                FROM memory_chat_summary
                WHERE NOT summarized AND kind <> 'bot_self'
                ORDER BY id
                LIMIT $1
                """,
                limit,
            )
        return [dict(r) for r in rows]

    async def update_chat_summary_encoded(
        self, row_id: int, content: str, embedding: list[float] | None
    ) -> None:
        """Encode catch-up write-back: UPDATE the summary body/embedding
        and flip the encoded flag (document_id unchanged).

        content becomes the faithful summary produced by encoding, embedding
        is recomputed from the summary text, and search_text is re-tokenized
        from the new body in sync (the old value tokenized the raw text and
        would skew the BM25 leg); after summarized=false -> true the row
        rejoins recall. On failure the caller simply does not call this
        method (the row stays false and retries next cycle).

        Args:
            row_id: summary row id.
            content: faithful summary from the encode pass.
            embedding: vector computed from the summary; None writes NULL
                (backfilled later).
        """
        async with self.pool.acquire() as conn:
            await conn.execute(
                """
                UPDATE memory_chat_summary
                SET content = $2, embedding = $3::vector, summarized = true,
                    search_text = $4
                WHERE id = $1
                """,
                row_id,
                content,
                vector_literal(embedding),
                build_search_text(content),
            )

    async def fetch_missing_search_text(self, limit: int) -> list[tuple[int, str]]:
        """Scan summary rows with search_text IS NULL (legacy backfill
        tokenization pass).

        Args:
            limit: batch size.

        Returns:
            (row id, summary body) tuple list.
        """
        async with self.pool.acquire() as conn:
            rows = await conn.fetch(
                f"SELECT id, content FROM {CHAT_SUMMARY_TABLE} "  # noqa: S608 -- whitelist constant table names
                "WHERE search_text IS NULL ORDER BY id LIMIT $1",
                limit,
            )
        return [(r["id"], r["content"]) for r in rows]

    async def update_search_text(self, row_id: int, search_text: str) -> None:
        """Backfill one row's search_text (tokenization pass).

        Args:
            row_id: summary row id.
            search_text: token string (build_search_text output).
        """
        async with self.pool.acquire() as conn:
            await conn.execute(
                f"UPDATE {CHAT_SUMMARY_TABLE} SET search_text = $2 WHERE id = $1",  # noqa: S608 -- whitelist constant table names
                row_id,
                search_text,
            )

    async def fetch_recent_rollout_summaries(
        self, *, session_id: str, skip_batches: int, limit: int, platform: str = ""
    ) -> list[dict]:
        """Fetch the latest N session-summary batches that rolled out of
        the host window (occurred_at-descending with OFFSET).

        Only this session's encoded chat_summary rows (bot_self is the bot's
        self-triggered raw batch and is out of scope for conversation
        rolling restore). The ordering matches the recency-exclusion
        sub-query exactly, and OFFSET converts on the same terms as the
        exclusion anchor — the anchor takes the latest K rows over all kinds
        (bot_self included), so the window actually covers K - (bot_self
        batches inside the window) chat_summary batches, and restoring must
        offset by that many in the chat sub-sequence to land exactly on the
        exclusion boundary (the previous fixed OFFSET K silently skipped
        that many batches whenever the window mixed in bot_self rows).

        Args:
            session_id: session id (bare; platform disambiguates
                same-numbered sessions across adapters).
            skip_batches: recent batches to skip (= host window block count,
                same source as the recency exclusion).
            limit: batches to fetch.
            platform: platform tag (non-empty appends platform = $ — the
                OFFSET budget shares its terms with the recency-exclusion
                sub-query and must not be taken up by another adapter's
                same-numbered session).

        Returns:
            Dict rows (document_id / content / occurred_at / kind).
        """
        async with self.pool.acquire() as conn:
            rows = await conn.fetch(
                f"SELECT document_id, content, occurred_at, kind FROM {CHAT_SUMMARY_TABLE} "  # noqa: S608 -- whitelist constant table names
                "WHERE session_id = $1 AND summarized AND kind = 'chat_summary' "
                + ("AND platform = $4 " if platform else "")
                + "ORDER BY occurred_at DESC, id DESC LIMIT $2 "
                "OFFSET GREATEST(0, $3 - (SELECT count(*) FROM ("
                f"SELECT kind FROM {CHAT_SUMMARY_TABLE} "
                "WHERE session_id = $1 "
                + ("AND platform = $4 " if platform else "")
                + "AND summarized ORDER BY occurred_at DESC, id DESC LIMIT $3"
                ") w WHERE w.kind = 'bot_self'))",
                session_id,
                limit,
                max(int(skip_batches), 0),
                *([platform] if platform else []),
            )
        return [dict(r) for r in rows]

    async def fetch_pending_facts(self, limit: int) -> list[dict]:
        """Fetch a batch of unprocessed raw facts (extracted_flag=0, id
        ascending).

        embedding is selected with an explicit ::text cast (asyncpg has no
        built-in vector decoder) for reuse in the cluster-candidate <=>
        ordering; None (pending backfill) takes the scalar-degraded path.

        Args:
            limit: batch size (merge_batch_size).

        Returns:
            Dict rows (all fields; embedding is a pgvector text literal or
            None).
        """
        async with self.pool.acquire() as conn:
            rows = await conn.fetch(
                """
                SELECT id, document_id, platform, user_id, related_user_ids,
                       display_name, category, statement, confidence,
                       session_id, group_id, evidence_key, occurred_at,
                       embedding::text AS embedding
                FROM memory_persona_fact_raw
                WHERE extracted_flag = 0
                ORDER BY id
                LIMIT $1
                """,
                limit,
            )
        return [dict(r) for r in rows]

    async def search_cluster_candidates(
        self,
        *,
        platform: str,
        user_id: str,
        category: str,
        related_user_ids: list[str] | None = None,
        embedding: str | list[float] | None,
        top_k: int,
    ) -> list[dict]:
        """Search cluster candidates under the same (platform, category)
        whose ownership relates to the new fact (tombstones included).

        Ownership matching is a relation-aware symmetric condition (the
        read side folds related ids into the candidate keys): cluster
        primary-owner hit (c.user_id = fact.user_id), the fact's primary
        owner inside the cluster's related set (a "A and I are roommates"
        fact attributed to B still hits A's primary cluster), the cluster's
        primary owner inside the fact's related set, or the two related
        sets overlapping (different-ownership extractions of the same
        relation become mutual candidates, preventing relation clusters
        from splitting into duplicates per owner).

        Vector path: cosine top-K (HNSW); when embedding is None (pending
        backfill) it degrades to a most-recently-updated top-K — the LLM
        verdict can still run, only candidate-ranking quality drops.

        Args:
            platform: platform tag.
            user_id: primary owner of the fact (bare uid).
            category: one of the six dimensions.
            related_user_ids: other parties of the fact (bare uids; an
                empty list matches on primary owner only).
            embedding: fact vector (pgvector text literal or float list;
                None takes the scalar-degraded path).
            top_k: candidate cap (candidate_top_k).

        Returns:
            Dict rows (id/canonical_statement/status/score/evidence_count/
            evidence_keys/last_evidence_at/occurred_at/replaced_by/
            similarity).
        """
        related = [r for r in (related_user_ids or []) if r]
        # $1 is always the related array (reused by owner_match; with an
        # empty array the array conditions are trivially false)
        p = _Params(related)
        uid_ph = p.add(user_id)
        plat_ph = p.add(platform)
        cat_ph = p.add(category)
        owner_match = (
            f"(c.user_id = {uid_ph} OR c.related_user_ids @> ARRAY[{uid_ph}] "
            f"OR c.user_id = ANY($1) OR $1 && c.related_user_ids)"
        )
        if embedding is None:
            top_ph = p.add(top_k)
            sql = (
                f"SELECT c.id, c.canonical_statement, c.status, c.score, "
                f"c.evidence_count, c.evidence_keys, c.last_evidence_at, "
                f"c.occurred_at, c.replaced_by "
                f"FROM {FACT_CLUSTER_TABLE} c "
                f"WHERE c.platform = {plat_ph} AND c.category = {cat_ph} "
                f"AND {owner_match} "
                f"ORDER BY c.updated_at DESC LIMIT {top_ph}"
            )
        else:
            vec = (
                embedding
                if isinstance(embedding, str)
                else vector_literal(embedding)
            )
            vec_ph = p.add(vec)
            top_ph = p.add(top_k)
            sql = (
                f"SELECT c.id, c.canonical_statement, c.status, c.score, "
                f"c.evidence_count, c.evidence_keys, c.last_evidence_at, "
                f"c.occurred_at, c.replaced_by, "
                f"1 - (c.embedding <=> {vec_ph}::vector) AS similarity "
                f"FROM {FACT_CLUSTER_TABLE} c "
                f"WHERE c.platform = {plat_ph} AND c.category = {cat_ph} "
                f"AND c.embedding IS NOT NULL AND {owner_match} "
                f"ORDER BY c.embedding <=> {vec_ph}::vector LIMIT {top_ph}"
            )
        async with self.pool.acquire() as conn:
            rows = await conn.fetch(sql, *p.values)
        return [dict(r) for r in rows]

    async def apply_fact_merge(
        self,
        *,
        fact_id: int,
        action: str,
        cluster_id: int | None = None,
        evidence_key: str = "",
        occurred_at: datetime | None = None,
        start_score: float,
        score_cap: float,
        promote_threshold: float,
        recent_promote_threshold: float,
        contradict_cluster_ids: list[int] | None = None,
    ) -> dict:
        """Dispose of one raw fact in a single transaction
        (join/create/replace cluster) and flip extracted_flag.

        Disposal and flag flip share the transaction: on commit the flag is
        flipped; on rollback it is not and the fact is reprocessed next
        cycle (same-join evidence_key dedup is the second idempotent
        backstop). The flag flip carries an optimistic lock
        (AND extracted_flag = 0) so concurrent consumers never double-score.

        All state-machine arithmetic (LEAST cap / evidence_key dedup /
        inheritance / halving) happens atomically inside SQL — no
        read-modify-write race window. The fact's ownership/dimension/
        statement/vector flow straight from the raw row via
        INSERT ... SELECT, eliminating Python-side field drift.

        Clusters in contradict_cluster_ids get a contradiction mark in the
        same transaction (contradicted_at, updated_at untouched — profile
        projection/ordering unaffected): old clusters ruled corrected or
        evolved are exempt from the evidence floor and return to natural
        decay; a same-join counts as reconfirmation and clears the mark.

        Replaced clusters never revive (the merge-revival CASE only covers
        pending_uncertain/dead): a replaced cluster has an explicit
        successor (replaced_by) and reviving it alongside the successor
        would keep contradictory statements alive; the caller excludes
        replaced targets at the verdict-interpretation layer and marks the
        successor with a contradiction.

        Args:
            fact_id: raw fact row id.
            action: merge (same-join + revival) | create (new cluster) |
                replace (new cluster superseding an old one — the fast lane
                for explicit corrections).
            cluster_id: target cluster id (required for merge/replace; must
                not be a replaced cluster).
            evidence_key: scoring dedup key (merge scoring; the create path
                takes it straight from the raw row).
            occurred_at: fact occurrence time (advances last_evidence_at on
                merge).
            start_score: starting score (already set by fact confidence:
                high/medium).
            score_cap: score cap.
            promote_threshold: profile threshold (immediate promotion check
                for replace-created clusters).
            recent_promote_threshold: recent-dimension profile threshold.
            contradict_cluster_ids: cluster ids ruled correction/drift.

        Returns:
            Disposal summary dict: action/score/status etc. ("skipped"
            means a concurrent consumer already handled it).

        Raises:
            ValueError: invalid action, or merge/replace without cluster_id.
            RuntimeError: target row missing (transaction rolled back;
            retried next cycle).
        """
        if action not in ("merge", "create", "replace"):
            raise ValueError(f"不支持的合并处置动作: {action}")
        if action in ("merge", "replace") and cluster_id is None:
            raise ValueError(f"action={action} 需要 cluster_id")

        async with self.pool.acquire() as conn:
            async with conn.transaction():
                flagged = await conn.execute(
                    "UPDATE memory_persona_fact_raw SET extracted_flag = 1 "
                    "WHERE id = $1 AND extracted_flag = 0",
                    fact_id,
                )
                if not flagged.endswith(" 1"):
                    # Optimistic lock: a concurrent consumer already took
                    # this fact — this transaction writes nothing
                    return {"action": "skipped"}

                if contradict_cluster_ids:
                    # Contradiction marks leave updated_at alone so profile
                    # projection/ordering is unaffected
                    await conn.execute(
                        """
                        UPDATE memory_fact_cluster
                        SET contradicted_at = now()
                        WHERE id = ANY($1::bigint[])
                        """,
                        contradict_cluster_ids,
                    )

                if action == "merge":
                    row = await conn.fetchrow(
                        """
                        UPDATE memory_fact_cluster c SET
                            score = CASE WHEN $1 = ANY(c.evidence_keys) THEN c.score
                                         ELSE LEAST($3, c.score + $2) END,
                            evidence_count = CASE WHEN $1 = ANY(c.evidence_keys)
                                                  THEN c.evidence_count
                                                  ELSE c.evidence_count + 1 END,
                            evidence_keys = CASE WHEN $1 = ANY(c.evidence_keys)
                                                 THEN c.evidence_keys
                                                 ELSE array_append(c.evidence_keys, $1) END,
                            last_evidence_at = GREATEST(
                                COALESCE(c.last_evidence_at, c.occurred_at), $4),
                            occurred_at = CASE WHEN c.category = 'recent'
                                               THEN GREATEST(c.occurred_at, $4)
                                               ELSE c.occurred_at END,
                            status = CASE WHEN $1 = ANY(c.evidence_keys) THEN c.status
                                          WHEN c.status IN ('pending_uncertain', 'dead')
                                          THEN 'active' ELSE c.status END,
                            demoted_at = CASE WHEN $1 = ANY(c.evidence_keys)
                                              THEN c.demoted_at ELSE NULL END,
                            contradicted_at = CASE WHEN $1 = ANY(c.evidence_keys)
                                                    THEN c.contradicted_at
                                                    ELSE NULL END,
                            updated_at = now()
                        WHERE c.id = $5
                        RETURNING id, score, status
                        """,
                        evidence_key,
                        start_score,
                        score_cap,
                        # GREATEST ignores NULL: a None occurred_at keeps the
                        # stored timestamps unchanged instead of crashing in
                        # _ensure_tz (latent guard — callers always pass a value).
                        _ensure_tz(occurred_at) if occurred_at is not None else None,
                        cluster_id,
                    )
                    if row is None:
                        raise RuntimeError(f"入簇目标簇不存在: cluster_id={cluster_id}")
                    # Profile-row sync: for already-profiled clusters the
                    # score copy follows the cluster score (statement is
                    # immutable — canonical_statement never changes after
                    # creation), removing projection-ordering lag; when the
                    # score is unchanged (pure replay hitting the
                    # evidence-key dedup) updated_at stays put
                    await conn.execute(
                        "UPDATE memory_user_profile SET score = $2, updated_at = "
                        "CASE WHEN score IS DISTINCT FROM $2 THEN now() ELSE updated_at END "
                        "WHERE cluster_id = $1",
                        cluster_id,
                        row["score"],
                    )
                    return {"action": "merge", "cluster_id": row["id"],
                            "score": row["score"], "status": row["status"]}

                if action == "create":
                    row = await conn.fetchrow(
                        """
                        INSERT INTO memory_fact_cluster
                            (platform, user_id, category, canonical_statement,
                             score, status, evidence_count, evidence_keys,
                             source_fact_ids, last_evidence_at, occurred_at,
                             related_user_ids, embedding)
                        SELECT f.platform, f.user_id, f.category, f.statement,
                               $2, 'active', 1, ARRAY[f.evidence_key], ARRAY[f.id],
                               f.occurred_at, f.occurred_at, f.related_user_ids,
                               f.embedding
                        FROM memory_persona_fact_raw f
                        WHERE f.id = $1
                        RETURNING id, score, status
                        """,
                        fact_id,
                        start_score,
                    )
                    if row is None:
                        raise RuntimeError(f"建簇来源事实不存在: fact_id={fact_id}")
                    return {"action": "create", "cluster_id": row["id"],
                            "score": row["score"], "status": row["status"]}

                # action == "replace": the new cluster inherits the old
                # cluster's score and the old one demotes to replaced
                row = await conn.fetchrow(
                    """
                    INSERT INTO memory_fact_cluster
                        (platform, user_id, category, canonical_statement,
                         score, status, evidence_count, evidence_keys,
                         source_fact_ids, last_evidence_at, occurred_at,
                         related_user_ids, embedding)
                    SELECT f.platform, f.user_id, f.category, f.statement,
                           LEAST($3, GREATEST($2, c.score))::real,
                           CASE WHEN LEAST($3, GREATEST($2, c.score))
                                      >= CASE WHEN f.category = 'recent'
                                              THEN $5::real ELSE $4::real END
                                THEN 'profiled' ELSE 'active' END,
                           1, ARRAY[f.evidence_key], ARRAY[f.id],
                           f.occurred_at, f.occurred_at, f.related_user_ids,
                           f.embedding
                    FROM memory_persona_fact_raw f
                    JOIN memory_fact_cluster c ON c.id = $6
                    WHERE f.id = $1
                    RETURNING id, score, status
                    """,
                    fact_id,
                    start_score,
                    score_cap,
                    promote_threshold,
                    recent_promote_threshold,
                    cluster_id,
                )
                if row is None:
                    raise RuntimeError(
                        f"替代建簇失败（事实或旧簇不存在）: "
                        f"fact_id={fact_id}, cluster_id={cluster_id}"
                    )
                old = await conn.fetchrow(
                    """
                    UPDATE memory_fact_cluster
                    SET status = 'replaced', score = score * 0.5,
                        replaced_by = $2, updated_at = now()
                    WHERE id = $1 AND status <> 'replaced'
                    RETURNING score
                    """,
                    cluster_id,
                    row["id"],
                )
                if old is None:
                    # Concurrent writer already tombstoned the cluster: no
                    # score to inherit-report (defensive — old["score"] would
                    # crash; the caller's successor chain is still consistent).
                    raise RuntimeError(
                        f"替代目标簇已不存在或已 replaced: cluster_id={cluster_id}"
                    )
                # Relation propagation (P2-10): edges whose ONLY evidence is
                # the backfill of this now-replaced cluster stop injecting —
                # corrected facts must not keep their old relation edges alive.
                await supersede_backfill_edges_of(conn, [cluster_id])
                # Atomic profile-entry switch: delete the old row; insert a
                # new row if the new cluster meets the profile threshold
                await conn.execute(
                    "DELETE FROM memory_user_profile WHERE cluster_id = $1",
                    cluster_id,
                )
                await conn.execute(
                    """
                    INSERT INTO memory_user_profile
                        (platform, user_id, category, cluster_id, statement,
                         score, related_user_ids)
                    SELECT platform, user_id, category, id, canonical_statement,
                           score, related_user_ids
                    FROM memory_fact_cluster
                    WHERE id = $1 AND status = 'profiled'
                    ON CONFLICT (platform, user_id, category, cluster_id) DO UPDATE
                    SET statement = EXCLUDED.statement,
                        score = EXCLUDED.score,
                        related_user_ids = EXCLUDED.related_user_ids,
                        updated_at = now()
                    """,
                    row["id"],
                )
                return {"action": "replace", "cluster_id": row["id"],
                        "score": row["score"], "status": row["status"],
                        "replaced_cluster_id": cluster_id,
                        "replaced_score": old["score"]}

    async def decay_pass(
        self,
        *,
        decay_factor: float,
        demote_threshold: float,
        pending_dead_days: int,
        recent_expire_days: int,
        activity_since: datetime | None = None,
        sticky_evidence_count: int = 0,
        anchor_profile_size: int = 0,
    ) -> dict:
        """Decay pass (single transaction, dev plan §8.2).

        Order: recent-dimension expiry demotion -> non-recent score decay
        -> profiled demotion check (on decayed scores) -> pending_uncertain
        death after pending_dead_days (counted from demoted_at). Every
        demotion cascades into profile-row deletion (data-modifying CTE).

        Activity gating (when activity_since is not None): the decay/
        demotion/dead steps apply only to users seen (triggering or
        participating) in memory_chat_summary since activity_since; absent
        users freeze entirely — profiles are never emptied by silence
        alone. Recent expiry is calendar semantics and stays ungated
        (demotes into the uncertain section; the cluster body stays
        protected by the dead gate and can revive on return).

        Evidence floor (when sticky_evidence_count > 0): clusters whose
        evidence count reached the threshold never decay below
        demote_threshold — repeatedly confirmed facts do not drop out of
        the profile just because the topic stopped recurring; clusters
        carrying a contradiction mark (contradicted_at, ruled
        correction/drift) are exempt from the floor and return to natural
        decay (a same reconfirmation clears the mark).

        Profile anchoring (when anchor_profile_size > 0): per dimension,
        the top N profile rows — same order as injection reads
        (score DESC -> updated_at DESC -> id ASC) — become anchors; anchored
        clusters are exempt from decay and demotion — profile members do
        not die from missing new evidence, they only return to natural
        decay once displaced out of the top N by newly confirmed facts (or
        contradiction-marked); the recent dimension is never anchored
        (calendar semantics belong to the expiry step alone).

        Args:
            decay_factor: per-cycle score factor (×0.8).
            demote_threshold: profile demotion threshold (profiled clusters
                below it demote).
            pending_dead_days: pending_uncertain death window in days,
                decoupled from the decay cycle.
            recent_expire_days: recent-dimension expiry days.
            activity_since: activity window start (None = no gating, decay
                everything).
            sticky_evidence_count: evidence-floor threshold (0 = disabled).
            anchor_profile_size: profile anchor count (0 = disabled).

        Returns:
            Stats dict: expired_recent/demoted/profile_rows_deleted/deaded.
        """
        gate = ""
        if sticky_evidence_count > 0:
            score_expr = (
                "CASE WHEN c.evidence_count >= $2 AND c.contradicted_at IS NULL "
                "AND c.score * $1 < $3 THEN $3 ELSE c.score * $1 END"
            )
            decay_params: tuple = (decay_factor, sticky_evidence_count, demote_threshold)
        else:
            score_expr = "c.score * $1"
            decay_params = (decay_factor,)
        async with self.pool.acquire() as conn:
            async with conn.transaction():
                if activity_since is not None:
                    await conn.execute(
                        """
                        CREATE TEMP TABLE _decay_active_pairs ON COMMIT DROP AS
                        SELECT DISTINCT platform, user_id FROM (
                            SELECT s.platform, s.user_id
                            FROM memory_chat_summary s
                            WHERE s.kind <> 'bot_self' AND s.user_id <> ''
                              AND s.occurred_at > $1
                            UNION
                            SELECT split_part(p, ':', 1), split_part(p, ':', 2)
                            FROM memory_chat_summary s,
                                 unnest(s.participants) AS p
                            WHERE s.kind <> 'bot_self'
                              AND s.occurred_at > $1
                        ) t
                        WHERE user_id <> ''
                        """,
                        _ensure_tz(activity_since),
                    )
                    gate = (
                        " AND EXISTS (SELECT 1 FROM _decay_active_pairs a "
                        "WHERE a.platform = c.platform AND a.user_id = c.user_id)"
                    )
                anchor_gate = ""
                if anchor_profile_size > 0:
                    await conn.execute(
                        """
                        CREATE TEMP TABLE _decay_anchors ON COMMIT DROP AS
                        SELECT platform, user_id, cluster_id FROM (
                            SELECT p.platform, p.user_id, p.cluster_id,
                                   row_number() OVER (
                                       PARTITION BY p.platform, p.user_id, p.category
                                       ORDER BY p.score DESC, p.updated_at DESC, p.id ASC
                                   ) AS rn
                            FROM memory_user_profile p
                            JOIN memory_fact_cluster c ON c.id = p.cluster_id
                            WHERE p.category <> 'recent'
                              AND c.contradicted_at IS NULL
                        ) t WHERE rn <= $1
                        """,
                        anchor_profile_size,
                    )
                    anchor_gate = (
                        " AND NOT EXISTS (SELECT 1 FROM _decay_anchors x "
                        "WHERE x.platform = c.platform AND x.user_id = c.user_id "
                        "AND x.cluster_id = c.id)"
                    )
                recent = await conn.fetchrow(
                    """
                    WITH expired AS (
                        UPDATE memory_fact_cluster
                        SET status = 'pending_uncertain', demoted_at = now(),
                            updated_at = now()
                        WHERE category = 'recent' AND status IN ('active', 'profiled')
                          AND occurred_at < now() - make_interval(days => $1)
                        RETURNING id
                    ), cleaned AS (
                        DELETE FROM memory_user_profile
                        WHERE cluster_id IN (SELECT id FROM expired)
                        RETURNING cluster_id
                    )
                    SELECT (SELECT count(*) FROM expired) AS expired,
                           (SELECT count(*) FROM cleaned) AS cleaned
                    """,
                    recent_expire_days,
                )
                await conn.execute(
                    f"""
                    UPDATE memory_fact_cluster c
                    SET score = {score_expr}, updated_at = now()
                    WHERE c.category <> 'recent' AND c.status IN ('active', 'profiled')
                    {gate}{anchor_gate}
                    """,
                    *decay_params,
                )
                demoted = await conn.fetchrow(
                    f"""
                    WITH demoted AS (
                        UPDATE memory_fact_cluster c
                        SET status = 'pending_uncertain', demoted_at = now(),
                            updated_at = now()
                        WHERE c.status = 'profiled' AND c.score < $1
                        {gate}{anchor_gate}
                        RETURNING c.id
                    ), cleaned AS (
                        DELETE FROM memory_user_profile
                        WHERE cluster_id IN (SELECT id FROM demoted)
                        RETURNING cluster_id
                    )
                    SELECT (SELECT count(*) FROM demoted) AS demoted,
                           (SELECT count(*) FROM cleaned) AS cleaned
                    """,
                    demote_threshold,
                )
                deaded = await conn.execute(
                    f"""
                    UPDATE memory_fact_cluster c
                    SET status = 'dead', updated_at = now()
                    WHERE c.status = 'pending_uncertain'
                      AND COALESCE(c.demoted_at, c.last_evidence_at, c.occurred_at)
                          < now() - make_interval(days => $1)
                    {gate}
                    """,
                    pending_dead_days,
                )
        return {
            "expired_recent": recent["expired"],
            "demoted": demoted["demoted"],
            "profile_rows_deleted": recent["cleaned"] + demoted["cleaned"],
            "deaded": int(deaded.rsplit(" ", 1)[-1]),
        }

    async def promote_pass(
        self, *, promote_threshold: float, recent_promote_threshold: float
    ) -> int:
        """Promotion pass: active clusters reaching the threshold become
        profiled with a profile-table upsert (single-transaction CTE).

        The recent dimension uses its own lower threshold
        (recent_promote_threshold).

        Args:
            promote_threshold: profile threshold.
            recent_promote_threshold: recent-dimension profile threshold.

        Returns:
            Clusters promoted in this run.
        """
        async with self.pool.acquire() as conn:
            async with conn.transaction():
                row = await conn.fetchrow(
                    """
                    WITH promoted AS (
                        UPDATE memory_fact_cluster c
                        SET status = 'profiled', updated_at = now()
                        WHERE c.status = 'active'
                          AND c.score >= CASE WHEN c.category = 'recent'
                                              THEN $2::real ELSE $1::real END
                        RETURNING c.id, c.platform, c.user_id, c.category,
                                  c.canonical_statement, c.score,
                                  c.related_user_ids
                    ), upserted AS (
                        INSERT INTO memory_user_profile
                            (platform, user_id, category, cluster_id, statement,
                             score, related_user_ids)
                        SELECT platform, user_id, category, id,
                               canonical_statement, score, related_user_ids
                        FROM promoted
                        ON CONFLICT (platform, user_id, category, cluster_id) DO UPDATE
                        SET statement = EXCLUDED.statement,
                            score = EXCLUDED.score,
                            related_user_ids = EXCLUDED.related_user_ids,
                            updated_at = now()
                        RETURNING cluster_id
                    )
                    SELECT (SELECT count(*) FROM promoted) AS promoted
                    """,
                    promote_threshold,
                    recent_promote_threshold,
                )
        return row["promoted"]

    async def get_kv(self, key: str) -> str | None:
        """Read an internal plugin kv value (e.g. the decay pass's last
        run time).

        Args:
            key: state key.

        Returns:
            The stored value; None when absent.
        """
        async with self.pool.acquire() as conn:
            row = await conn.fetchrow(
                "SELECT value FROM _memory_local_kv WHERE key = $1", key
            )
        return row["value"] if row else None

    async def set_kv(self, key: str, value: str) -> None:
        """Write an internal plugin kv value (upsert).

        Args:
            key: state key.
            value: state value (ISO timestamp string etc.).
        """
        async with self.pool.acquire() as conn:
            await conn.execute(
                """
                INSERT INTO _memory_local_kv (key, value)
                VALUES ($1, $2)
                ON CONFLICT (key) DO UPDATE
                SET value = EXCLUDED.value, updated_at = now()
                """,
                key,
                value,
            )

    # ------------------------------------------------------------------
    #  persistent entity alias layer (P1, memory_entity_alias table)
    # ------------------------------------------------------------------

    async def alias_upsert(self, rows: list[dict]) -> None:
        """Batch upsert of alias rows (unique key platform+user_id+name).

        last_seen takes the later value (GREATEST) — monotonic, never
        regressing under concurrent write channels; first_seen keeps its
        column default (row creation time, not a semantic time). source
        flips only with a "genuinely later observation" (replaying older
        observations or re-running the backfill must not rewrite an
        existing row's source back and forth between batch/backfill — that
        would drift the observation semantics). Empty list returns
        immediately.

        Args:
            rows: alias rows (platform/user_id/name/last_seen/source, see
                alias_store.build_alias_rows).
        """
        if not rows:
            return
        payload = [
            (
                r["platform"],
                r["user_id"],
                r["name"],
                _ensure_tz(r["last_seen"]),
                r.get("source") or "batch",
            )
            for r in rows
        ]
        async with self.pool.acquire() as conn:
            await conn.executemany(
                """
                INSERT INTO memory_entity_alias
                    (platform, user_id, name, last_seen, source)
                VALUES ($1, $2, $3, $4, $5)
                ON CONFLICT (platform, user_id, name) DO UPDATE SET
                    last_seen = GREATEST(memory_entity_alias.last_seen, EXCLUDED.last_seen),
                    source = CASE WHEN EXCLUDED.last_seen > memory_entity_alias.last_seen
                                  THEN EXCLUDED.source
                                  ELSE memory_entity_alias.source END
                """,
                payload,
            )

    async def alias_fetch_all(self) -> list[dict]:
        """All alias rows (full in-memory view reload; the table is bounded
        at ~tens of thousands of rows, millisecond cost).

        Returns:
            Row list (platform/user_id/name/last_seen_epoch).
        """
        async with self.pool.acquire() as conn:
            rows = await conn.fetch(
                """
                SELECT platform, user_id, name,
                       EXTRACT(EPOCH FROM last_seen) AS last_seen_epoch
                FROM memory_entity_alias
                """
            )
        return [dict(r) for r in rows]

    async def fetch_alias_names_by_owner(
        self, owners: list[tuple[str, str]] | None = None
    ) -> dict[tuple[str, str], str]:
        """(platform, uid) -> latest non-placeholder alias (canonical-name
        resolution source for the graph read side).

        Scans last_seen-descending and takes the first non-placeholder name
        per uid (that uid's latest usable name); the secondary key name ASC
        keeps the pick deterministic on same-second ties. Placeholder names
        (未知*/用户\\d+/digits-only etc.) never serve as canonical names;
        the predicate is the same source as the write-side guard
        (alias_store.is_placeholder_name).

        Args:
            owners: optional (platform, uid) filter set — the injection path
                resolves only the selected edges' endpoints (the owner
                filter rides the (platform, user_id) index instead of a
                full scan); None = whole table (the established semantics
                for graph write-side directory builds etc.).
        """
        async with self.pool.acquire() as conn:
            if owners:
                pairs = [(str(p or ""), str(u or "")) for p, u in owners if u]
                if not pairs:
                    return {}
                rows = await conn.fetch(
                    """
                    SELECT a.platform, a.user_id, a.name
                    FROM memory_entity_alias a
                    JOIN unnest($1::text[], $2::text[]) AS k(platform, user_id)
                      ON a.platform = k.platform AND a.user_id = k.user_id
                    ORDER BY a.last_seen DESC, a.name ASC
                    """,
                    [p for p, _ in pairs],
                    [u for _, u in pairs],
                )
            else:
                rows = await conn.fetch(
                    """
                    SELECT platform, user_id, name
                    FROM memory_entity_alias
                    ORDER BY last_seen DESC, name ASC
                    """
                )
        out: dict[tuple[str, str], str] = {}
        for r in rows:
            name = str(r["name"] or "").strip()
            if not name or is_placeholder_name(name):
                continue
            out.setdefault((r["platform"], str(r["user_id"])), name)
        return out

    # ------------------------------------------------------------------
    #  entity relation edges (P2 write / P3 inject, memory_entity_edge)
    # ------------------------------------------------------------------

    async def upsert_entity_edge(
        self, rows: list[dict], *, label_stopwords: list[str] | None = None
    ) -> None:
        """Batch zero-LLM merge of edge rows (structural key
        platform+subject+object+label).

        Write-side stopword interception (before merging): rows whose label
        hits the stopword table (label_stopwords, maintained per scenario by
        hand on the admin page) are dropped whole — no new edge lands, and
        new evidence no longer refreshes an existing row under that
        structural key (legacy cleanup goes through the semantic audit pass
        or manual edits); the pure-rule predicate is
        entity_edge.filter_stopword_rows.

        Reverse-echo pre-check (before merging): rows whose mirrored
        direction already exists in the table while the forward one does
        not are skipped whole — when both sides state the same relation
        once each, this prevents A→B/B→A dual active edges (one direction
        must be wrong; 2026-09-11 audit finding 2); the pure-rule predicate
        is entity_edge.split_reverse_echo_rows.

        evidence_key dedup counting: replaying the same key (retain
        retries / encode-catch-up idempotency) does not raise
        evidence_count; cross-session/cross-day recurrence adds 1.
        Bot-endpoint edges insert as pending; with min_evidence=1 the first
        evidence already meets the bar and the new row is active outright
        (with >1 the insert stays pending, and reaching the count — this
        evidence included — flips it active inline in the conflict path);
        confidence only ever rises; statement refreshes to the latest
        evidence value, and occurred_at takes the later value (GREATEST,
        monotonic like last_seen — replaying older batches from the pending
        queue or backfilling older clusters must not rewind occurrence
        time); endpoint names refresh too, except placeholder names
        (empty / 未知* / 用户\\d+ / digits-only / unknown / undefined; the
        rule is dual-implemented in alias_store.is_placeholder_name and the
        two copies must stay in sync) never overwrite an existing name —
        the LLM occasionally emits placeholders and a good name must not be
        flushed away; the display layer is backstopped by read-side
        canonical-name resolution (no automatic superseded — label is
        multi-valued, decision 8).

        Tombstone protection: superseded edges never auto-reactivate — the
        bot-edge activation CASE carries status <> 'superseded', so an edge
        demoted by hand or by audit stays down even after piling up double
        evidence (restoration is a manual flip on the admin page);
        human-human edges keep their status in the ELSE branch — they never
        had an auto-revival path anyway, and both sides now share the
        "superseded never auto-revives" semantics.

        Optional row key count_on_conflict (default True): with False, the
        conflict path on an existing row only refreshes without scoring
        (evidence_count/evidence_keys untouched) — reserved for the legacy
        backfill channel (preventing "online extraction +1 then backfill
        echo +1" double counting, and backfill echoes from topping up a bot
        edge's double-evidence bar); new-row inserts still count this
        evidence as 1 (backfill-guided output stays grounded). Only the
        backfill passes False; online extraction and encode-catch-up never
        do.

        Args:
            rows: edge rows (platform/subject_uid/object_uid/subject_name/
                object_name/relation_label/statement/confidence/
                occurred_at/evidence_key/is_bot_edge/min_evidence
                [/count_on_conflict], see memory_kernel._ingest_relations
                and relation_backfill._to_row).
            label_stopwords: write-side stopword label table
                (config.relation_label_stopwords; None/empty = no
                interception, for legacy callers).
        """
        if not rows:
            return
        rows, stopped = filter_stopword_rows(rows, label_stopwords)
        if stopped:
            logger.info(
                "关系边写侧停用拦截 %d 条（label 命中停用表）", stopped
            )
        if not rows:
            return
        async with self.pool.acquire() as conn:
            existing_keys = await self._fetch_existing_edge_keys(conn, rows)
            rows, skipped = split_reverse_echo_rows(rows, existing_keys)
            if skipped:
                logger.info(
                    "关系边反向回声跳过 %d 条（镜像方向已在库/同批先到）", skipped
                )
        if not rows:
            return
        payload = [
            (
                r["platform"],
                r["subject_uid"],
                r["object_uid"],
                r["subject_name"],
                r["object_name"],
                r["relation_label"],
                r["statement"],
                (
                    "pending"
                    if r["is_bot_edge"] and int(r["min_evidence"]) > 1
                    else "active"
                ),
                r["confidence"],
                _ensure_tz(r["occurred_at"]),
                r["evidence_key"],
                bool(r["is_bot_edge"]),
                int(r["min_evidence"]),
                bool(r.get("count_on_conflict", True)),
            )
            for r in rows
        ]
        # Placeholder-name pattern is the single-source constant from
        # alias_store (the Python predicate and both SQL copies must not drift).
        async with self.pool.acquire() as conn:
            await conn.executemany(
                f"""
                INSERT INTO memory_entity_edge (
                    platform, subject_uid, object_uid, subject_name, object_name,
                    relation_label, statement, status, confidence,
                    evidence_count, evidence_keys, first_seen, last_seen,
                    occurred_at
                )
                VALUES (
                    $1, $2, $3, $4, $5, $6, $7, $8, $9,
                    1, ARRAY[$11::text], $10, $10, $10
                )
                ON CONFLICT (platform, subject_uid, object_uid, relation_label)
                DO UPDATE SET
                    subject_name = CASE
                        WHEN EXCLUDED.subject_name = ''
                            OR EXCLUDED.subject_name ~* '{PLACEHOLDER_NAME_SQL}'
                        THEN memory_entity_edge.subject_name
                        ELSE EXCLUDED.subject_name END,
                    object_name = CASE
                        WHEN EXCLUDED.object_name = ''
                            OR EXCLUDED.object_name ~* '{PLACEHOLDER_NAME_SQL}'
                        THEN memory_entity_edge.object_name
                        ELSE EXCLUDED.object_name END,
                    statement = EXCLUDED.statement,
                    last_seen = GREATEST(
                        memory_entity_edge.last_seen, EXCLUDED.last_seen
                    ),
                    occurred_at = GREATEST(
                        memory_entity_edge.occurred_at, EXCLUDED.occurred_at
                    ),
                    updated_at = now(),
                    confidence = CASE WHEN EXCLUDED.confidence = 'high'
                                      THEN 'high'
                                      ELSE memory_entity_edge.confidence END,
                    evidence_count = CASE
                        WHEN NOT $14::bool THEN memory_entity_edge.evidence_count
                        WHEN $11::text = ANY(memory_entity_edge.evidence_keys)
                        THEN memory_entity_edge.evidence_count
                        ELSE memory_entity_edge.evidence_count + 1 END,
                    evidence_keys = CASE
                        WHEN NOT $14::bool THEN memory_entity_edge.evidence_keys
                        WHEN $11::text = ANY(memory_entity_edge.evidence_keys)
                        THEN memory_entity_edge.evidence_keys
                        ELSE array_append(
                            memory_entity_edge.evidence_keys, $11::text
                        ) END,
                    status = CASE
                        WHEN $12::bool
                             AND memory_entity_edge.status <> 'superseded'
                             AND (
                            CASE WHEN NOT $14::bool
                                 THEN memory_entity_edge.evidence_count
                                 WHEN $11::text = ANY(memory_entity_edge.evidence_keys)
                                 THEN memory_entity_edge.evidence_count
                                 ELSE memory_entity_edge.evidence_count + 1 END
                        ) >= $13::int
                        THEN 'active'
                        ELSE memory_entity_edge.status END
                """,
                payload,
            )

    async def _fetch_existing_edge_keys(
        self, conn, rows: list[dict]
    ) -> set[tuple[str, str, str, str]]:
        """Batch-probe which edge structural keys (forward + mirrored
        direction) already exist in the table (input for reverse-echo
        filtering).

        superseded tombstones do not count as "existing": once the audit
        pass corrects a direction and tombstones the edge, restating the
        same relation in the correct direction must not be blocked by the
        mirrored tombstone (otherwise the direction-correction workflow
        deadlocks itself). No dual-active tombstone risk exists — the
        forward side is a newly inserted row and the mirrored tombstone
        stays superseded, joining neither injection nor mirror counting.
        """
        forward = [
            (
                str(r["platform"]),
                str(r["subject_uid"]),
                str(r["object_uid"]),
                str(r["relation_label"]),
            )
            for r in rows
        ]
        mirror = [(p, o, s, l) for (p, s, o, l) in forward]
        all_keys = forward + mirror
        recs = await conn.fetch(
            """
            SELECT DISTINCT k.p AS platform, k.s AS subject_uid,
                   k.o AS object_uid, k.l AS relation_label
            FROM unnest($1::text[], $2::text[], $3::text[], $4::text[])
                 AS k(p, s, o, l)
            JOIN memory_entity_edge e
              ON e.platform = k.p AND e.subject_uid = k.s
             AND e.object_uid = k.o AND e.relation_label = k.l
             AND e.status <> 'superseded'
            """,
            [k[0] for k in all_keys],
            [k[1] for k in all_keys],
            [k[2] for k in all_keys],
            [k[3] for k in all_keys],
        )
        return {
            (r["platform"], r["subject_uid"], r["object_uid"], r["relation_label"])
            for r in recs
        }

    async def fetch_active_edges(
        self, node_keys: list[str], bot_keys: object = ""
    ) -> list[dict]:
        """Injection candidate edges: active with an endpoint hitting the
        node set (bot-endpoint edges included when bot_keys is non-empty).

        Endpoint matching uses (platform, uid) composite keys
        ("platform:uid") — when the same numeric uid collides across
        adapters, another platform's person's edges/profiles no longer leak
        into injection (entity-hit keys are platform-prefixed already, same
        semantics as the profile main path; the previous bare-uid +
        session-platform matching mis-hit/missed on cross-platform alias
        hits). Bot endpoints match by composite key as well (bot identity ×
        current session platform).

        Ordering evidence_count DESC -> last_seen DESC (the deterministic
        order of the injection selection layer, aligned with
        select_relation_edges' consumption); capped at 64 rows (node sets
        are bounded; keeps a homonym storm from pulling the whole table).

        Args:
            node_keys: this round's entity-hit node composite keys
                ("platform:uid", scenario A anchors).
            bot_keys: bot composite keys ("platform:bot_uid", bot_uid_set
                forms × session platform); non-empty = scenario C gate open
                (AT/quote of the bot), empty = no bot-endpoint edges.

        Returns:
            Edge rows (id/platform/endpoints/names/label/statement/
            evidence_count/last_seen/occurred_at).
        """
        keys = [str(k or "") for k in (node_keys or []) if k]
        bots = sorted(str(k or "") for k in (bot_keys or []) if k)
        async with self.pool.acquire() as conn:
            rows = await conn.fetch(
                """
                SELECT id, platform, subject_uid, object_uid, subject_name,
                       object_name, relation_label, statement,
                       evidence_count, last_seen, occurred_at
                FROM memory_entity_edge
                WHERE status = 'active'
                  AND (
                      platform || ':' || subject_uid = ANY($1::text[])
                      OR platform || ':' || object_uid = ANY($1::text[])
                      OR (cardinality($2::text[]) > 0
                          AND (platform || ':' || subject_uid = ANY($2::text[])
                               OR platform || ':' || object_uid = ANY($2::text[])))
                  )
                ORDER BY evidence_count DESC, last_seen DESC
                LIMIT 64
                """,
                keys,
                bots,
            )
        return [dict(r) for r in rows]

    async def fetch_edges_for_audit(self, after_id: int, limit: int) -> list[dict]:
        """Edges submitted to the semantic audit pass: id above the
        watermark and not tombstoned (active/pending).

        Ordering id ASC keeps batch partitioning stable (watermark
        semantics: advance only to the max id of a judged batch; rows
        before a failed/unsure batch are never skipped over). Pending bot
        edges are included — sub-threshold edges are exposed in the graph
        too, so junk pending edges go under audit as well. updated_at rides
        along (the optimistic-lock input for supersede: edges touched by
        hand or refreshed by new evidence while the audit LLM call is in
        flight skip demotion, never tombstoned on a stale snapshot).

        Args:
            after_id: watermark (only edges with id above it; 0 = all).
            limit: batch cap (relation_audit_batch_size).

        Returns:
            Edge rows (id/platform/endpoint uids and names/label/statement/
            status/confidence/evidence_count/last_seen/updated_at).
        """
        async with self.pool.acquire() as conn:
            rows = await conn.fetch(
                """
                SELECT id, platform, subject_uid, object_uid, subject_name,
                       object_name, relation_label, statement, status,
                       confidence, evidence_count, last_seen, updated_at
                FROM memory_entity_edge
                WHERE id > $1 AND status IN ('active', 'pending')
                ORDER BY id ASC
                LIMIT $2
                """,
                int(after_id),
                int(limit),
            )
        return [dict(r) for r in rows]

    async def supersede_edges(
        self,
        edge_ids: list[int],
        *,
        expected_updated_at: dict[int, datetime] | None = None,
        reasons: dict[int, str] | None = None,
    ) -> int:
        """Batch-mark edges superseded (shared by the semantic audit pass
        and the admin page; idempotent, returns affected rows).

        No physical deletes (the graph stays traceable and manually
        restorable); once tombstoned, new evidence no longer reactivates
        the row (upsert tombstone protection), while statement/last_seen
        still refresh with new evidence under the same structural key
        (state semantics untouched, audit observability keeps advancing).
        The demotion reason lands in supersede_reason/superseded_at for the
        record (tombstones stay traceable and batch-reviewable).

        When expected_updated_at is non-empty, demotion is optimistic-locked:
        a row is tombstoned only if its updated_at still equals the
        submitted snapshot (the audit LLM call yields the event loop for
        seconds; edges manually flipped back to active or refreshed by new
        evidence in that window are not tombstoned on a stale verdict).

        Args:
            edge_ids: edge id list.
            expected_updated_at: {edge_id: updated_at at submission} —
                the optimistic-lock snapshot.
            reasons: {edge_id: demotion reason} (the audit LLM's verdict).

        Returns:
            Rows actually tombstoned (counted via RETURNING id — an UPDATE
            without RETURNING reads None through fetchval and skews the
            statistics).
        """
        ids = [int(i) for i in edge_ids if int(i) > 0]
        if not ids:
            return 0
        reason_map = {int(k): str(v)[:200] for k, v in (reasons or {}).items()}
        reason_arr = [reason_map.get(i, "") for i in ids]
        async with self.pool.acquire() as conn:
            if expected_updated_at:
                ts_arr = [expected_updated_at.get(i) for i in ids]
                rows = await conn.fetch(
                    """
                    UPDATE memory_entity_edge e SET status = 'superseded',
                        superseded_at = now(), updated_at = now(),
                        supersede_reason = COALESCE(NULLIF(v.reason, ''), e.supersede_reason)
                    FROM unnest($1::bigint[], $2::text[], $3::timestamptz[])
                         AS v(id, reason, ts)
                    WHERE e.id = v.id AND e.updated_at = v.ts
                      AND e.status <> 'superseded'
                    RETURNING e.id
                    """,
                    ids,
                    reason_arr,
                    ts_arr,
                )
            else:
                rows = await conn.fetch(
                    """
                    UPDATE memory_entity_edge e SET status = 'superseded',
                        superseded_at = now(), updated_at = now(),
                        supersede_reason = COALESCE(NULLIF(v.reason, ''), e.supersede_reason)
                    FROM unnest($1::bigint[], $2::text[]) AS v(id, reason)
                    WHERE e.id = v.id AND e.status <> 'superseded'
                    RETURNING e.id
                    """,
                    ids,
                    reason_arr,
                )
        return len(rows)

    async def relation_integrity_report(self, *, pending_stale_days: int = 14) -> dict:
        """First-layer structural self-check (zero LLM, rule sentinels):
        violation counts per invariant.

        All queries are read-only counters; the merge agent logs them for
        observation (no automated remediation — a structural violation
        usually means a pipeline bug and needs human diagnosis, not silent
        data munging):
        - evidence_mismatch: evidence_count ≠ evidence_keys length (always
          0 in a healthy table);
        - mirror_active_pairs: bidirectional mirror-active pairs under the
          same label (the write-side reverse-echo pre-check blocks new
          ones; any recurrence in the existing stock means the sentinel fired);
        - placeholder_names: edges with placeholder/empty endpoint names
          (observability);
        - bad_labels: labels out of length bounds or containing an endpoint
          name (write-side parse_relation blocks new data; sentinel for
          historical dirt);
        - pending_stale: overdue pending edges (bot edges stuck below the
          double-evidence bar);
        - stale_names: active edges whose endpoint names differ from the
          alias table's latest canonical names (observability; read-side
          injection already backstops with canonical-name resolution);
        - edges_dead_source: active edges whose every backfill source
          cluster is replaced/dead with no online evidence (cluster-death
          propagation should have tombstoned them — non-zero means a
          propagation gap).
        """
        async with self.pool.acquire() as conn:
            mismatch = await conn.fetchval(
                """
                SELECT count(*) FROM memory_entity_edge
                WHERE evidence_count IS DISTINCT FROM
                      COALESCE(array_length(evidence_keys, 1), 0)
                """
            )
            mirror = await conn.fetchval(
                """
                SELECT count(*) FROM memory_entity_edge a
                JOIN memory_entity_edge b
                  ON b.platform = a.platform
                 AND b.subject_uid = a.object_uid
                 AND b.object_uid = a.subject_uid
                 AND b.relation_label = a.relation_label
                 AND b.id > a.id
                WHERE a.status = 'active' AND b.status = 'active'
                """
            )
            placeholder = await conn.fetchval(
                f"""
                SELECT count(*) FROM memory_entity_edge
                WHERE subject_name = '' OR object_name = ''
                   OR subject_name ~* '{PLACEHOLDER_NAME_SQL}'
                   OR object_name ~* '{PLACEHOLDER_NAME_SQL}'
                """
            )
            bad_label = await conn.fetchval(
                """
                SELECT count(*) FROM memory_entity_edge
                WHERE char_length(relation_label) NOT BETWEEN 2 AND 8
                   OR (char_length(subject_name) >= 2
                       AND position(subject_name in relation_label) > 0)
                   OR (char_length(object_name) >= 2
                       AND position(object_name in relation_label) > 0)
                """
            )
            pending_stale = await conn.fetchval(
                """
                SELECT count(*) FROM memory_entity_edge
                WHERE status = 'pending'
                  AND last_seen < now() - ($1::int * interval '1 day')
                """,
                int(pending_stale_days),
            )
            stale_names = await conn.fetchval(
                f"""
                WITH canon AS (
                  SELECT DISTINCT ON (platform, user_id) platform, user_id, name
                  FROM memory_entity_alias
                  WHERE name <> ''
                    AND name !~* '{PLACEHOLDER_NAME_SQL}'
                  ORDER BY platform, user_id, last_seen DESC
                )
                SELECT count(*) FROM memory_entity_edge e
                LEFT JOIN canon cs
                  ON cs.platform = e.platform AND cs.user_id = e.subject_uid
                LEFT JOIN canon co
                  ON co.platform = e.platform AND co.user_id = e.object_uid
                WHERE e.status = 'active'
                  AND (e.subject_name <> COALESCE(cs.name, e.subject_name)
                       OR e.object_name <> COALESCE(co.name, e.object_name))
                """
            )
            dead_source = await conn.fetchval(
                """
                SELECT count(*) FROM memory_entity_edge e
                WHERE e.status = 'active'
                  AND EXISTS (SELECT 1 FROM unnest(e.evidence_keys) k
                              WHERE k LIKE 'backfill|%')
                  AND NOT EXISTS (SELECT 1 FROM unnest(e.evidence_keys) k
                                  WHERE k NOT LIKE 'backfill|%')
                  AND EXISTS (
                      SELECT 1 FROM memory_fact_cluster c
                      WHERE ('backfill|' || c.id::text) = ANY(e.evidence_keys)
                        AND c.status IN ('replaced', 'dead')
                  )
                """
            )
        return {
            "evidence_mismatch": int(mismatch or 0),
            "mirror_active_pairs": int(mirror or 0),
            "placeholder_names": int(placeholder or 0),
            "bad_labels": int(bad_label or 0),
            "pending_stale": int(pending_stale or 0),
            "stale_names": int(stale_names or 0),
            "edges_dead_source": int(dead_source or 0),
        }

    async def fetch_confirmed_relation_sources(
        self, after_id: int = 0, limit: int | None = None
    ) -> list[dict]:
        """Confirmed fact clusters (active/profiled) — the input face of
        the legacy relation backfill.

        pending_uncertain/replaced/dead are unconfirmed/tombstoned states
        and never backfill sources. after_id drives watermark increments
        (only clusters beyond the previous backfill progress); ordering
        id ASC keeps batch partitioning stable (idempotent re-runs). When
        limit is provided the query pages by id window — a full load has a
        non-trivial memory peak on large tables, so the backfill side
        advances the watermark window by window.

        Args:
            after_id: only clusters with id above this value (0 = all).
            limit: window size (None = unbounded, for legacy callers).

        Returns:
            Cluster rows (id/platform/user_id/canonical_statement/
            occurred_at).
        """
        sql = (
            """
            SELECT id, platform, user_id, canonical_statement, occurred_at
            FROM memory_fact_cluster
            WHERE status IN ('active', 'profiled')
              AND canonical_statement <> ''
              AND id > $1
            ORDER BY id
            """
        )
        params: list = [int(after_id)]
        if limit is not None:
            sql += " LIMIT $2"
            params.append(int(limit))
        async with self.pool.acquire() as conn:
            rows = await conn.fetch(sql, *params)
        return [dict(r) for r in rows]

    async def count_confirmed_relation_sources(
        self, after_id: int = 0, exclude_user_id: str = ""
    ) -> int:
        """Total confirmed backfill-source clusters (progress denominator
        of the paged backfill; the cluster table is small enough to count).

        When exclude_user_id is non-empty that owner is excluded (clusters
        owned by the bot are not backfill sources).
        """
        async with self.pool.acquire() as conn:
            value = await conn.fetchval(
                """
                SELECT count(*) FROM memory_fact_cluster
                WHERE status IN ('active', 'profiled')
                  AND canonical_statement <> ''
                  AND id > $1
                  AND user_id <> $2
                """,
                int(after_id),
                exclude_user_id or "",
            )
        return int(value or 0)

    async def fetch_confirmed_relation_sources_by_ids(
        self, cluster_ids: list[int]
    ) -> list[dict]:
        """Fetch confirmed clusters by exact ids (consumed by the
        revived-cluster backfill todo; same filtering semantics as the
        watermark variant)."""
        ids = [int(i) for i in cluster_ids or [] if int(i) > 0]
        if not ids:
            return []
        async with self.pool.acquire() as conn:
            rows = await conn.fetch(
                """
                SELECT id, platform, user_id, canonical_statement, occurred_at
                FROM memory_fact_cluster
                WHERE id = ANY($1::bigint[])
                  AND status IN ('active', 'profiled')
                  AND canonical_statement <> ''
                ORDER BY id
                """,
                ids,
            )
        return [dict(r) for r in rows]

    async def fetch_alias_directory(self) -> list[tuple[str, str, str]]:
        """Full alias table (name -> (platform, uid) directory; the
        backfill's object-resolution source).

        Ambiguity (one name, several uids) is skipped by the backfill
        service — this method returns every row untouched; the arbitration
        logic lives in the pure-function layer (unit-testable).

        Returns:
            (platform, uid, name) tuple list.
        """
        async with self.pool.acquire() as conn:
            rows = await conn.fetch(
                "SELECT platform, user_id, name FROM memory_entity_alias"
            )
        return [(r["platform"], r["user_id"], r["name"]) for r in rows]

    # ------------------------------------------------------------------
    #  profile assembly queries (M5, dev plan §9.3)
    # ------------------------------------------------------------------

    async def fetch_profile_sections(
        self, platform: str, user_id: str, per_section_limit: int
    ) -> dict[str, list[str]]:
        """Per-section profile rows (data source for the first five
        sections).

        Ownership matching is owner-only (related ids do not join —
        relation-fact statements are written from the owner's viewpoint, so
        injecting the related side's profile would drop the subject and
        misattribute; the related side's profile is covered by facts
        previously extracted about them from the owner's viewpoint). The
        ordering is fixed: score DESC -> updated_at DESC -> id ASC (the id
        tiebreaker guarantees identical output order for identical table
        contents — part of profile determinism), capped per section.

        Args:
            platform: platform tag.
            user_id: user id (bare uid).
            per_section_limit: per-section cap.

        Returns:
            category -> statement list (ordered within a section); empty
            dict when the user has no profile rows.
        """
        async with self.pool.acquire() as conn:
            rows = await conn.fetch(
                """
                SELECT category, statement FROM (
                    SELECT category, statement,
                           row_number() OVER (
                               PARTITION BY category
                               ORDER BY score DESC, updated_at DESC, id ASC
                           ) AS rn
                    FROM memory_user_profile
                    WHERE platform = $1
                      AND user_id = $2
                ) t
                WHERE rn <= $3
                ORDER BY category, rn
                """,
                platform,
                user_id,
                per_section_limit,
            )
        sections: dict[str, list[str]] = {}
        for r in rows:
            sections.setdefault(r["category"], []).append(r["statement"])
        return sections

    async def fetch_uncertain_statements(
        self, platform: str, user_id: str, limit: int
    ) -> list[str]:
        """Uncertain-info section source: pending_uncertain and replaced
        (halved-score) clusters.

        Ownership matching is owner-only (same as the profile sections;
        related ids do not join injection).

        Args:
            platform: platform tag.
            user_id: user id (bare uid).
            limit: cap.

        Returns:
            Canonical statement list (score DESC -> updated_at DESC ->
            id ASC).
        """
        async with self.pool.acquire() as conn:
            rows = await conn.fetch(
                """
                SELECT canonical_statement
                FROM memory_fact_cluster
                WHERE platform = $1
                  AND user_id = $2
                  AND status IN ('pending_uncertain', 'replaced')
                ORDER BY score DESC, updated_at DESC, id ASC
                LIMIT $3
                """,
                platform,
                user_id,
                limit,
            )
        return [r["canonical_statement"] for r in rows]

    async def fetch_profile_sections_multi(
        self, platforms: list[str], user_ids: list[str], per_section_limit: int
    ) -> dict[str, list[str]]:
        """Per-section profile rows with multi-account keys merged
        (identity-linked reads, e.g. the same person on qq/web).

        Keys pair up as parallel arrays (platforms[i], user_ids[i]);
        ownership matching is owner-only (same as the single-key version;
        related ids do not join injection); ordering and per-section caps
        match the single-key version (global score DESC -> updated_at DESC
        -> id ASC ordering, then per-section caps), and identical statement
        texts across keys dedup order-preservingly.

        Args:
            platforms: platform tag array (paired 1:1 with user_ids).
            user_ids: user id array.
            per_section_limit: per-section cap.

        Returns:
            category -> statement list (ordered within a section); empty
            dict when no profile rows exist.
        """
        async with self.pool.acquire() as conn:
            rows = await conn.fetch(
                """
                SELECT category, statement FROM (
                    SELECT m.category, m.statement,
                           row_number() OVER (
                               PARTITION BY m.category
                               ORDER BY m.score DESC, m.updated_at DESC, m.id ASC
                           ) AS rn
                    FROM memory_user_profile m
                    JOIN unnest($1::text[], $2::text[]) AS k(platform, user_id)
                      ON m.platform = k.platform
                     AND m.user_id = k.user_id
                ) t
                WHERE rn <= $3
                ORDER BY category, rn
                """,
                platforms,
                user_ids,
                per_section_limit,
            )
        sections: dict[str, list[str]] = {}
        for r in rows:
            items = sections.setdefault(r["category"], [])
            if r["statement"] not in items:  # order-preserving dedup across keys
                items.append(r["statement"])
        return sections

    async def fetch_uncertain_statements_multi(
        self, platforms: list[str], user_ids: list[str], limit: int
    ) -> list[str]:
        """Uncertain-info rows with multi-account keys merged (ordering/
        dedup/ownership semantics identical to the single-key version).

        Args:
            platforms: platform tag array (paired 1:1 with user_ids).
            user_ids: user id array.
            limit: cap.

        Returns:
            Canonical statement list (score DESC -> updated_at DESC ->
            id ASC, deduplicated across keys).
        """
        async with self.pool.acquire() as conn:
            rows = await conn.fetch(
                """
                SELECT m.canonical_statement
                FROM memory_fact_cluster m
                JOIN unnest($1::text[], $2::text[]) AS k(platform, user_id)
                  ON m.platform = k.platform
                 AND m.user_id = k.user_id
                WHERE m.status IN ('pending_uncertain', 'replaced')
                ORDER BY m.score DESC, m.updated_at DESC, m.id ASC
                LIMIT $3
                """,
                platforms,
                user_ids,
                limit,
            )
        statements: list[str] = []
        for r in rows:
            if r["canonical_statement"] not in statements:
                statements.append(r["canonical_statement"])
        return statements

    async def fetch_latest_display_name_multi(
        self, platforms: list[str], user_ids: list[str]
    ) -> str | None:
        """Most recently registered display name across multi-account keys
        (profile header; globally latest across keys).

        Args:
            platforms: platform tag array (paired 1:1 with user_ids).
            user_ids: user id array.

        Returns:
            The display name; None when absent or always empty (the caller
            falls back to user_id).
        """
        async with self.pool.acquire() as conn:
            value = await conn.fetchval(
                """
                SELECT f.display_name
                FROM memory_persona_fact_raw f
                JOIN unnest($1::text[], $2::text[]) AS k(platform, user_id)
                  ON f.platform = k.platform AND f.user_id = k.user_id
                WHERE f.display_name <> ''
                ORDER BY f.occurred_at DESC, f.id DESC
                LIMIT 1
                """,
                platforms,
                user_ids,
            )
        return value

    async def fetch_latest_display_name(
        self, platform: str, user_id: str
    ) -> str | None:
        """The display name registered at the user's latest fact extraction
        (profile header).

        Args:
            platform: platform tag.
            user_id: user id (bare uid).

        Returns:
            The display name; None when absent or always empty (the caller
            falls back to user_id).
        """
        async with self.pool.acquire() as conn:
            value = await conn.fetchval(
                """
                SELECT display_name FROM memory_persona_fact_raw
                WHERE platform = $1 AND user_id = $2 AND display_name <> ''
                ORDER BY occurred_at DESC, id DESC
                LIMIT 1
                """,
                platform,
                user_id,
            )
        return value

    async def delete_chat_summary(
        self,
        document_id: str,
        *,
        scope_session_id: str = "",
        scope_user_id: str = "",
    ) -> bool:
        """Delete a summary memory by idempotency key (memory_remove
        maintenance tool).

        Args:
            document_id: summary idempotency key.
            scope_session_id: when non-empty, adds a session-ownership
                restriction.
            scope_user_id: when non-empty, adds a user-ownership restriction
                (OR-combined with the session one — only rows inside the
                trigger's session or owned by them can be deleted, so a
                whitelisted caller cannot delete someone else's memory just
                by knowing the document_id).

        Returns:
            Whether a row was deleted (False when missing or out of scope).
        """
        sql = "DELETE FROM memory_chat_summary WHERE document_id = $1"
        params: list = [document_id]
        if scope_session_id or scope_user_id:
            parts: list[str] = []
            if scope_session_id:
                params.append(scope_session_id)
                parts.append(f"session_id = ${len(params)}")
            if scope_user_id:
                params.append(scope_user_id)
                parts.append(f"user_id = ${len(params)}")
            sql += " AND (" + " OR ".join(parts) + ")"
        sql += " RETURNING id"
        async with self.pool.acquire() as conn:
            row = await conn.fetchrow(sql, *params)
        return row is not None
