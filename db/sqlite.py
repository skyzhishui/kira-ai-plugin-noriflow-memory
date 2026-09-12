"""Local memory store access layer — SQLite backend (SQLiteMemoryDatabase,
plan §5/§7/§8).

Implements the MemoryBackend contract with the same signatures as
postgres.MemoryDatabase; dialect mappings (TIMESTAMPTZ -> fixed-format TEXT /
TEXT[] -> JSON TEXT / vector -> float32 BLOB / tsvector -> FTS5 shadow
table) are documented in the migrations_sqlite/001_init.sql header. Key
points:

- vector search pulls in no sqlite-vec: scope conditions bound the row set
  in SQL first, Python/numpy brute-force cosine picks top-K, the BM25 leg
  goes through the FTS5 shadow table, and ``base._rrf_fuse`` fuses them
  (acceptable at personal scale of ~10⁵ rows; ceiling declared in plan
  §5.3);
- time reads/writes funnel through base.sqlite_format_ts / sqlite_parse_ts
  (always 6 fractional digits UTC, lexicographic = chronological); reads
  restore aware datetimes uniformly so upper-layer signatures stay the
  same;
- JSON array columns decode back to lists (matching asyncpg TEXT[]
  behavior) with json.dumps on write; legacy vectors with mismatched
  dimensions count as missing (cosine skips them — switching embedding
  models needs no table rebuild);
- array semantics (ANY / && / array_append / unnest join) are rewritten
  per plan §5.2 into json_each EXISTS / IN lists / in-transaction
  read-modify-write (BEGIN IMMEDIATE already serializes writers);
- REGEXP is a Python function registered by connect() (the equivalent of
  PG ``~*``, reusing the single-source placeholder guard constant
  PLACEHOLDER_NAME_SQL).

The caller (LocalMemoryKernel) owns circuit-breaking and fail-open
semantics; this layer lets exceptions propagate unchanged.
"""

from __future__ import annotations

import json
import re
import sqlite3
import struct
from datetime import datetime, timedelta, timezone
from pathlib import Path

from core.logging_manager import get_logger

from ..alias_store import PLACEHOLDER_NAME_SQL, is_placeholder_name
from ..config import LocalMemoryConfig
from ..entity_edge import (
    filter_stopword_rows,
    split_reverse_echo_rows,
)
from ._sqlite_pool import _SingleConnPool
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
    parse_vector,
    sqlite_format_ts,
    sqlite_parse_ts,
)

logger = get_logger("noriflow_memory.sqlite", "cyan")

# Brute-force cosine accelerator: numpy is optional; without it we fall
try:  # back to pure Python (acceptable at personal scale)
    import numpy as _np
except ImportError:  # pragma: no cover
    _np = None

# Column sets restored on read (aligned with asyncpg behavior)
_TS_COLUMNS = frozenset({
    "occurred_at", "written_at", "updated_at", "created_at",
    "last_evidence_at", "demoted_at", "contradicted_at", "superseded_at",
    "first_seen", "last_seen",
})
_JSON_COLUMNS = frozenset({
    "participants", "related_user_ids", "evidence_keys", "source_fact_ids",
})
_BOOL_COLUMNS = frozenset({"summarized", "unsummarized", "has_embedding"})

# Migration version registry DDL (same structure as the PG side;
# applied_at uses the SQLite time format)
_MIGRATIONS_TABLE_DDL = """
CREATE TABLE IF NOT EXISTS _memory_local_migrations (
    version    INTEGER PRIMARY KEY,
    name       TEXT NOT NULL,
    applied_at TEXT NOT NULL DEFAULT (STRFTIME('%Y-%m-%dT%H:%M:%fZ', 'now'))
)
"""


def _regexp(pattern: str, value) -> int:
    """REGEXP function implementation (PG ``~*`` equivalent:
    case-insensitive unanchored search)."""
    if value is None:
        return 0
    try:
        return 1 if re.search(pattern, str(value), re.IGNORECASE) else 0
    except re.error:
        return 0


def _now_ts() -> str:
    """Current time as fixed-format UTC text — every now() equivalent is
    supplied by Python."""
    return sqlite_format_ts(datetime.now(timezone.utc))


def _pack_vector(vec: list[float] | None, dims: int) -> bytes | None:
    """Vector -> float32 little-endian BLOB; mismatched dimensions count
    as missing (NULL, self-healed by the backfill pass)."""
    if vec is None:
        return None
    if dims and len(vec) != dims:
        logger.warning(
            "embedding 维度不一致（%d != %d），按缺失向量写入（待补算回填）",
            len(vec), dims,
        )
        return None
    return struct.pack(f"<{len(vec)}f", *vec)


def _json_list(value: list | None) -> str:
    """list -> JSON TEXT (TEXT[] column mapping)."""
    return json.dumps(list(value or []), ensure_ascii=False)


def _decode_row(row) -> dict:
    """sqlite3.Row -> dict: timestamp/JSON/bool/vector columns restored to
    their asyncpg shapes."""
    out = {key: row[key] for key in row.keys()}
    for key in out:
        if key in _TS_COLUMNS:
            out[key] = sqlite_parse_ts(out[key])
        elif key in _JSON_COLUMNS:
            raw = out[key]
            if raw is None or raw == "":
                out[key] = []
            else:
                try:
                    parsed = json.loads(raw)
                    out[key] = parsed if isinstance(parsed, list) else []
                except (ValueError, TypeError):
                    out[key] = []
        elif key in _BOOL_COLUMNS:
            out[key] = bool(out[key])
        elif key == "embedding":
            out[key] = parse_vector(out[key])
    return out


def _decode_rows(rows) -> list[dict]:
    return [_decode_row(r) for r in rows]


def _cosine(a: list[float], b: list[float]) -> float | None:
    """Cosine similarity (numpy first, pure-Python fallback; zero vectors
    return None)."""
    if len(a) != len(b) or not a:
        return None
    if _np is not None:
        va = _np.asarray(a, dtype="<f4")
        vb = _np.asarray(b, dtype="<f4")
        na = float(_np.linalg.norm(va))
        nb = float(_np.linalg.norm(vb))
        if na == 0.0 or nb == 0.0:
            return None
        return float(_np.dot(va, vb) / (na * nb))
    dot = 0.0
    na = 0.0
    nb = 0.0
    for x, y in zip(a, b):
        dot += x * y
        na += x * x
        nb += y * y
    if na == 0.0 or nb == 0.0:
        return None
    return dot / ((na ** 0.5) * (nb ** 0.5))


def _overlap_cond(p, col: str, keys: list[str]) -> str | None:
    """Array-overlap semantics (PG ``col && $n``) -> json_each member
    EXISTS."""
    keys = [k for k in keys if k]
    if not keys:
        return None
    phs = ", ".join(p.add(k) for k in keys)
    return f"EXISTS (SELECT 1 FROM json_each({col}) je WHERE je.value IN ({phs}))"


def _in_cond(p, col: str, values: list) -> str | None:
    """ANY($n) semantics -> IN list (empty list returns None; the caller
    skips the condition)."""
    values = [v for v in values if v]
    if not values:
        return None
    phs = ", ".join(p.add(v) for v in values)
    return f"{col} IN ({phs})"


def _json_array_of(raw) -> list:
    """TEXT JSON column -> list (defensive against malformed values)."""
    if raw is None or raw == "":
        return []
    try:
        parsed = json.loads(raw)
        return parsed if isinstance(parsed, list) else []
    except (ValueError, TypeError):
        return []


def _split_sql_statements(script: str) -> list[str]:
    """Split a migration script into statements (quote/comment aware;
    migration files are plain DDL, no trigger-level parsing needed)."""
    statements: list[str] = []
    buf: list[str] = []
    i = 0
    n = len(script)
    while i < n:
        ch = script[i]
        if ch in ("'", '"'):
            quote = ch
            buf.append(ch)
            i += 1
            while i < n:
                buf.append(script[i])
                if script[i] == quote:
                    # Doubled quotes ('' / "") are escapes - keep going
                    if i + 1 < n and script[i + 1] == quote:
                        buf.append(script[i + 1])
                        i += 2
                        continue
                    i += 1
                    break
                i += 1
            continue
        if script.startswith("--", i):
            j = script.find("\n", i)
            i = n if j < 0 else j
            continue
        if script.startswith("/*", i):
            j = script.find("*/", i + 2)
            i = n if j < 0 else j + 2
            continue
        if ch == ";":
            stmt = "".join(buf).strip()
            if stmt:
                statements.append(stmt)
            buf = []
            i += 1
            continue
        buf.append(ch)
        i += 1
    tail = "".join(buf).strip()
    if tail:
        statements.append(tail)
    return statements


class SQLiteMemoryDatabase(MemoryBackend):
    """Local memory store (SQLite + FTS5, brute-force cosine) access layer.

    Attributes:
        sqlite_path: database file path (default resolved at main assembly
            time).
        pool: _SingleConnPool shim (acquire surface identical to an asyncpg
            pool).
    """

    # Backend dialect tag (webui_store dispatches SQL per backend)
    dialect = "sqlite"

    def __init__(self, config: LocalMemoryConfig) -> None:
        """Initialize (no connection is made yet). pool_min/pool_max/
        db_command_timeout are ignored under the SQLite backend (single
        connection + WAL, plan §4/§8)."""
        self.sqlite_path = config.sqlite_path
        self._embedding_dims = int(config.embedding_dims or 0)
        self._pool: _SingleConnPool | None = None

    @property
    def pool(self) -> _SingleConnPool:
        """Pool shim (raises RuntimeError before connect)."""
        if self._pool is None:
            raise RuntimeError("SQLiteMemoryDatabase 未连接（connect 先于任何操作）")
        return self._pool

    async def connect(self) -> None:
        """Open the database file and set up PRAGMAs/REGEXP (idempotent:
        skips when already connected).

        Raises:
            RuntimeError: SQLite too old (< 3.35, no RETURNING) or open
            failure; the caller degrades gracefully.
        """
        if self._pool is not None:
            return
        if sqlite3.sqlite_version_info < (3, 35, 0):
            raise RuntimeError(
                f"SQLite 后端需要 sqlite >= 3.35（当前 {sqlite3.sqlite_version}，"
                "RETURNING 语法不可用）；请升级 Python 官方包自带版本"
            )
        path = Path(self.sqlite_path)
        path.parent.mkdir(parents=True, exist_ok=True)
        import aiosqlite

        try:
            conn = await aiosqlite.connect(
                str(path), isolation_level=None  # autocommit: this layer manages transactions explicitly
            )
        except Exception:
            raise
        try:
            conn.row_factory = sqlite3.Row
            await conn.execute("PRAGMA journal_mode=WAL")
            await conn.execute("PRAGMA busy_timeout=5000")
            await conn.execute("PRAGMA synchronous=NORMAL")
            await conn.create_function("regexp", 2, _regexp, deterministic=True)
            await conn.execute("SELECT 1")
        except Exception:
            await conn.close()
            raise
        self._pool = _SingleConnPool(conn)
        logger.info("本地记忆库(SQLite)连接成功: %s", path)

    async def close(self) -> None:
        """Close the connection (idempotent; exceptions are logged, not raised)."""
        if self._pool is not None:
            try:
                await self._pool.close()
            except Exception:
                logger.warning("关闭记忆库连接时发生异常", exc_info=True)
            self._pool = None

    # ------------------------------------------------------------------
    #  schema migrations
    # ------------------------------------------------------------------

    async def apply_migrations(self, migrations_dir: str | Path) -> list[int]:
        """Apply pending migration scripts in version order (each file in
        one BEGIN IMMEDIATE transaction).

        SQLite has a single writer, so no pg_advisory_lock mutual exclusion
        is needed; the version registry matches the PG-side structure
        (001_init.sql already folds versions 2..10 into its registration;
        the version sequence is shared).

        Args:
            migrations_dir: migration script directory (plugin
                migrations_sqlite/).

        Returns:
            Versions applied in this run (empty when up to date).
        """
        directory = Path(migrations_dir)
        if not directory.is_dir():
            raise FileNotFoundError(f"迁移目录不存在: {directory}")

        applied: list[int] = []
        async with self.pool.acquire() as conn:
            await conn.execute(_MIGRATIONS_TABLE_DDL)
            rows = await conn.fetch(
                "SELECT version FROM _memory_local_migrations"
            )
            done = {int(r["version"]) for r in rows}
            for path in sorted(directory.glob("*.sql")):
                match = _MIGRATION_NAME_RE.match(path.name)
                if match is None:
                    logger.warning("跳过不合规的迁移文件名: %s", path.name)
                    continue
                version = int(match.group(1))
                if version in done:
                    continue
                script = path.read_text(encoding="utf-8")
                async with conn.transaction():
                    for statement in _split_sql_statements(script):
                        await conn.execute(statement)
                    await conn.execute(
                        "INSERT OR IGNORE INTO _memory_local_migrations "
                        "(version, name) VALUES ($1, $2)",
                        version,
                        path.name,
                    )
                applied.append(version)
                logger.info("记忆库迁移已应用: %s", path.name)

        if not applied:
            logger.info("记忆库 schema 已是最新（无待应用迁移）")
        await self._repair_profile_related_ids()
        return applied

    async def _repair_profile_related_ids(self) -> int:
        """Self-repair of profile rows whose related_user_ids was corrupted
        by promote_pass in v1.15.1 and earlier.

        Historical defect: promote_pass fed a raw row's JSON TEXT into
        _json_list, which iterated the string character by character and
        stored e.g. ["[", "\"", "u", "2", "\"", "]"] (a char array).
        Repair signature: the value parses as a non-empty list, every
        element is a single-character string, and the elements joined back
        parse as a JSON array themselves — legitimate rows hold
        multi-character uids (joined, not valid JSON) or empty arrays
        (length 0), so neither hits the signature; a corrupted row joined
        back always re-parses, making this repair injury-free, naturally
        idempotent, and safe to repeat at every startup (the table scan is
        negligible).
        """
        async with self.pool.acquire() as conn:
            rows = await conn.fetch(
                "SELECT id, related_user_ids FROM memory_user_profile"
            )
            repairs: list[tuple[str, int]] = []
            for r in rows:
                try:
                    parsed = json.loads(r["related_user_ids"])
                except (ValueError, TypeError):
                    continue
                if (
                    not isinstance(parsed, list)
                    or not parsed
                    or not all(
                        isinstance(e, str) and len(e) == 1 for e in parsed
                    )
                ):
                    continue
                joined = "".join(parsed)
                try:
                    if not isinstance(json.loads(joined), list):
                        continue
                except (ValueError, TypeError):
                    continue
                repairs.append((joined, int(r["id"])))
            if not repairs:
                return 0
            async with conn.transaction():
                await conn.executemany(
                    "UPDATE memory_user_profile SET related_user_ids = $1 "
                    "WHERE id = $2",
                    repairs,
                )
        logger.info(
            "画像表 related_user_ids 自修复完成：重写 %d 行"
            "（v1.15.1 promote_pass 字符数组缺陷）",
            len(repairs),
        )
        return len(repairs)

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
        summarized: bool,
    ) -> None:
        """Insert a chat summary (document_id conflicts silently skipped,
        idempotent).

        Syncs the FTS5 shadow table in the same transaction (BM25 leg);
        parameter semantics in the postgres namesake.
        """
        occurred = sqlite_format_ts(_ensure_tz(occurred_at))
        written = _now_ts()
        search_text = build_search_text(content)
        blob = _pack_vector(embedding, self._embedding_dims)
        async with self.pool.acquire() as conn:
            async with conn.transaction():
                row = await conn.fetchrow(
                    """
                    INSERT INTO memory_chat_summary
                        (document_id, kind, platform, session_id, group_id,
                         user_id, participants, content, occurred_at,
                         written_at, embedding, summarized, search_text)
                    VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11,
                            $12, $13)
                    ON CONFLICT (document_id) DO NOTHING
                    RETURNING id
                    """,
                    document_id, kind, platform, session_id, group_id,
                    user_id, _json_list(participants), content, occurred,
                    written, blob, 1 if summarized else 0, search_text,
                )
                if row is not None and search_text:
                    await conn.execute(
                        "INSERT INTO memory_chat_summary_fts (summary_id, text)"
                        " VALUES ($1, $2)",
                        row["id"], search_text,
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
        agent)."""
        async with self.pool.acquire() as conn:
            await conn.execute(
                """
                INSERT INTO memory_persona_fact_raw
                    (document_id, platform, user_id, related_user_ids,
                     display_name, category, statement, confidence,
                     session_id, group_id, evidence_key, occurred_at,
                     written_at, embedding)
                VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12,
                        $13, $14)
                ON CONFLICT (document_id) DO NOTHING
                """,
                document_id, platform, user_id,
                _json_list(related_user_ids), display_name, category,
                statement, confidence, session_id, group_id, evidence_key,
                sqlite_format_ts(_ensure_tz(occurred_at)), _now_ts(),
                _pack_vector(embedding, self._embedding_dims),
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
        """Vector(+BM25) search over the summary table (condition-assembly
        semantics aligned verbatim with the PG version — see its docstring).

        SQLite implementation differences (plan §5.2/§5.3):
        - Vector leg: scope conditions bound the row set in SQL, then Python
          cosine sorts and keeps top-limit;
        - BM25 leg: same where + FTS5 shadow-table MATCH (tokens OR-joined),
          bm25 order;
        - The two legs fuse via ``base._rrf_fuse`` (pure Python, shared by
          both backends).
        """
        if scope not in ("session", "user", "user_session"):
            raise ValueError(f"不支持的检索 scope: {scope}")

        p = _Params(None)  # first-slot placeholder (cosine runs in Python; SQL never consumes it)
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
        keys = [k for k in (user_keys or []) if k]
        uids = [u for u in (user_ids or []) if u]
        if keys:
            cond = _overlap_cond(p, "participants", keys)
            if cond:
                user_blocks.append(cond)
        if uids:
            cond = _in_cond(p, "user_id", uids)
            if cond:
                user_blocks.append(cond)
        ent_keys = [k for k in (entity_user_keys or []) if k]
        ent_uids = [u for u in (entity_user_ids or []) if u]
        ent_parts: list[str] = []
        if ent_keys:
            cond = _overlap_cond(p, "participants", ent_keys)
            if cond:
                ent_parts.append(cond)
        if ent_uids:
            cond = _in_cond(p, "user_id", ent_uids)
            if cond:
                ent_parts.append(cond)
        if ent_parts and cross_session:
            user_blocks.extend(ent_parts)
        primary_parts = user_blocks[:]
        if primary_parts:
            user_blocks = ["(" + " OR ".join(primary_parts) + ") AND summarized"]

        exp_parts: list[str] = []
        exp_keys = [k for k in (expanded_user_keys or []) if k]
        exp_uids = [u for u in (expanded_user_ids or []) if u]
        if exp_keys:
            cond = _overlap_cond(p, "participants", exp_keys)
            if cond:
                exp_parts.append(cond)
        if exp_uids:
            cond = _in_cond(p, "user_id", exp_uids)
            if cond:
                exp_parts.append(cond)
        if exp_parts and session_id:
            exp_pin = f"session_id = {p.add(session_id)}"
            if platform:
                exp_pin += f" AND platform = {p.add(platform)}"
            user_blocks.append(
                "(" + " OR ".join(exp_parts) + ") AND summarized"
                f" AND {exp_pin}"
            )
        if ent_parts and not cross_session and session_id:
            ent_pin = f"session_id = {p.add(session_id)}"
            if platform:
                ent_pin += f" AND platform = {p.add(platform)}"
            user_blocks.append(
                "(" + " OR ".join(ent_parts) + ") AND summarized"
                f" AND {ent_pin}"
            )
        if user_blocks:
            if session_id:
                bot_self_cond = (
                    f"kind = 'bot_self' AND session_id = {p.add(session_id)}"
                )
                if platform:
                    bot_self_cond += f" AND platform = {p.add(platform)}"
                conds.append(
                    "(" + " OR ".join(user_blocks) + f" OR ({bot_self_cond}))"
                )
            else:
                conds.append("(" + " OR ".join(user_blocks) + ")")

        if exclude_kinds:
            phs = ", ".join(p.add(v) for v in exclude_kinds)
            conds.append(f"kind NOT IN ({phs})")

        # Topic blacklist (structural exclusion at the SQL candidate layer):
        # blacklisted rows never enter the candidate set and top_k truncation
        # happens at the pipeline end, so filtered rows waste no slots. The
        # injection layer keeps a second Python defense line
        # (memory_kernel.build_injection_text).
        if exclude_content_keywords:
            patterns = [
                _like_contains_pattern(kw)
                for kw in exclude_content_keywords
                if kw
            ]
            for pattern in patterns:
                conds.append(
                    f"content NOT LIKE {p.add(pattern)} ESCAPE '\\'"
                )

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

        if exclude_document_ids:
            phs = ", ".join(p.add(d) for d in exclude_document_ids)
            conds.append(f"document_id NOT IN ({phs})")

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
            + ", embedding"
        )

        async with self.pool.acquire() as conn:
            rows = _decode_rows(
                await conn.fetch(
                    f"SELECT {select_cols} FROM {CHAT_SUMMARY_TABLE} "  # noqa: S608
                    f"WHERE embedding IS NOT NULL AND ({where})",
                    *p.values,
                )
            )
            # Vector leg: Python cosine sorts and keeps top-limit (rows
            # with missing/mismatched-dimension vectors are skipped)
            vec_rows: list[dict] = []
            for row in rows:
                emb = row.get("embedding")
                if not emb:
                    continue
                sim = _cosine(emb, query_vec)
                if sim is None:
                    continue
                row["relevance"] = sim
                vec_rows.append(row)
            vec_rows.sort(key=lambda r: r["relevance"], reverse=True)
            vec_rows = vec_rows[:limit]

            tokens = build_search_text(query_text).split() if hybrid else []
            if not tokens:
                if not with_embedding:
                    for row in vec_rows:
                        row.pop("embedding", None)
                return vec_rows

            # BM25 leg: FTS5 shadow table JOINs the main table in one
            # statement (MATCH + same where + bm25 order + LIMIT in a single
            # pass). Taking the FTS top-limit first and intersecting with
            # the where afterwards would under-fill the BM25 leg whenever
            # the where is highly selective (session isolation/blacklist) —
            # the whole top-limit can be filtered out (the PG version
            # applies where before LIMIT and has no such gap)
            q_ph = p.add(" OR ".join(tokens))
            limit_ph = p.add(limit)
            bm25_rows = _decode_rows(
                await conn.fetch(
                    f"SELECT {select_cols} FROM {CHAT_SUMMARY_TABLE} "  # noqa: S608
                    "JOIN memory_chat_summary_fts ON "
                    "memory_chat_summary_fts.summary_id = "
                    f"{CHAT_SUMMARY_TABLE}.id "
                    f"WHERE memory_chat_summary_fts MATCH {q_ph} "
                    f"AND embedding IS NOT NULL AND ({where}) "
                    f"ORDER BY bm25(memory_chat_summary_fts) LIMIT {limit_ph}",
                    *p.values,
                )
            )
            bm25_filtered: list[dict] = []
            for row in bm25_rows:
                # SQL guarantees embedding non-null; this only drops rows
                # with mismatched dimensions / unparseable vectors
                emb = row.get("embedding")
                if not emb:
                    continue
                sim = _cosine(emb, query_vec)
                if sim is None:
                    continue
                row["relevance"] = sim
                bm25_filtered.append(row)

        fused = _rrf_fuse(vec_rows, bm25_filtered, rrf_k)
        if not with_embedding:
            for row in fused:
                row.pop("embedding", None)
        return fused

    async def fetch_recent_session_participants(
        self, *, session_id: str, limit: int, platform: str = ""
    ) -> list[dict]:
        """Participants of the session's latest N summary batches (source
        of expansion keys for recall widening)."""
        sql = (
            f"SELECT participants, user_id FROM {CHAT_SUMMARY_TABLE} "  # noqa: S608
            "WHERE session_id = $1 AND summarized "
            + ("AND platform = $3 " if platform else "")
            + "ORDER BY occurred_at DESC LIMIT $2"
        )
        params = [session_id, limit] + ([platform] if platform else [])
        async with self.pool.acquire() as conn:
            rows = await conn.fetch(sql, *params)
        return _decode_rows(rows)

    async def fetch_recent_summary_scores(
        self, *, query_vec: list[float], session_id: str, limit: int, platform: str = ""
    ) -> list[dict]:
        """Cosine of the session's latest N encoded summaries against a
        given vector (write-side near-duplicate check)."""
        sql = (
            f"SELECT id, content, embedding FROM {CHAT_SUMMARY_TABLE} "  # noqa: S608
            "WHERE session_id = $1 AND kind = 'chat_summary' AND summarized "
            "AND embedding IS NOT NULL "
            + ("AND platform = $3 " if platform else "")
            + "ORDER BY occurred_at DESC, id DESC LIMIT $2"
        )
        params = [session_id, limit] + ([platform] if platform else [])
        async with self.pool.acquire() as conn:
            rows = _decode_rows(await conn.fetch(sql, *params))
        out: list[dict] = []
        for row in rows:
            emb = row.get("embedding")
            if not emb:
                continue
            sim = _cosine(emb, query_vec)
            if sim is None:
                continue
            out.append(
                {"id": row["id"], "content": row["content"], "score": sim}
            )
        return out

    # ------------------------------------------------------------------
    #  embedding backfill (consumed by the vector_ops background task)
    # ------------------------------------------------------------------

    async def fetch_missing_embeddings(
        self, table: str, limit: int
    ) -> list[tuple[int, str]]:
        """Scan rows with embedding IS NULL in the given table (id
        ascending, capped)."""
        from .base import _BACKFILL_TABLES, _BACKFILL_TEXT_COLUMNS

        if table not in _BACKFILL_TABLES:
            raise ValueError(f"补算扫描不支持表: {table}")
        text_col = _BACKFILL_TEXT_COLUMNS[table]
        async with self.pool.acquire() as conn:
            rows = await conn.fetch(
                f"SELECT id, {text_col} AS text FROM {table} "  # noqa: S608
                "WHERE embedding IS NULL ORDER BY id LIMIT $1",
                limit,
            )
        return [(int(r["id"]), r["text"]) for r in rows]

    async def update_embedding(
        self, table: str, row_id: int, embedding: list[float]
    ) -> None:
        """Backfill one row's embedding."""
        from .base import _BACKFILL_TABLES

        if table not in _BACKFILL_TABLES:
            raise ValueError(f"补算回填不支持表: {table}")
        async with self.pool.acquire() as conn:
            await conn.execute(
                f"UPDATE {table} SET embedding = $2 WHERE id = $1",  # noqa: S608
                row_id,
                _pack_vector(embedding, self._embedding_dims),
            )

    # ------------------------------------------------------------------
    #  merge agent (M4)
    # ------------------------------------------------------------------

    async def fetch_unsummarized_summaries(self, limit: int) -> list[dict]:
        """Fetch a batch of un-encoded degraded-text rows (summarized=false,
        id ascending)."""
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
        return _decode_rows(rows)

    async def update_chat_summary_encoded(
        self, row_id: int, content: str, embedding: list[float] | None
    ) -> None:
        """Encode catch-up write-back (body/vector/encoded flag/search_text
        + FTS5 shadow sync)."""
        search_text = build_search_text(content)
        blob = _pack_vector(embedding, self._embedding_dims)
        async with self.pool.acquire() as conn:
            async with conn.transaction():
                await conn.execute(
                    """
                    UPDATE memory_chat_summary
                    SET content = $2, embedding = $3, summarized = 1,
                        search_text = $4
                    WHERE id = $1
                    """,
                    row_id, content, blob, search_text,
                )
                await conn.execute(
                    "DELETE FROM memory_chat_summary_fts WHERE summary_id = $1",
                    row_id,
                )
                if search_text:
                    await conn.execute(
                        "INSERT INTO memory_chat_summary_fts"
                        " (summary_id, text) VALUES ($1, $2)",
                        row_id, search_text,
                    )

    async def fetch_missing_search_text(self, limit: int) -> list[tuple[int, str]]:
        """Scan summary rows with search_text IS NULL (legacy backfill
        tokenization pass)."""
        async with self.pool.acquire() as conn:
            rows = await conn.fetch(
                f"SELECT id, content FROM {CHAT_SUMMARY_TABLE} "  # noqa: S608
                "WHERE search_text IS NULL ORDER BY id LIMIT $1",
                limit,
            )
        return [(int(r["id"]), r["content"]) for r in rows]

    async def update_search_text(self, row_id: int, search_text: str) -> None:
        """Backfill one row's search_text (tokenization pass; FTS5 shadow
        sync)."""
        async with self.pool.acquire() as conn:
            async with conn.transaction():
                await conn.execute(
                    f"UPDATE {CHAT_SUMMARY_TABLE} SET search_text = $2 "  # noqa: S608
                    "WHERE id = $1",
                    row_id, search_text,
                )
                await conn.execute(
                    "DELETE FROM memory_chat_summary_fts WHERE summary_id = $1",
                    row_id,
                )
                if search_text:
                    await conn.execute(
                        "INSERT INTO memory_chat_summary_fts"
                        " (summary_id, text) VALUES ($1, $2)",
                        row_id, search_text,
                    )

    async def fetch_recent_rollout_summaries(
        self, *, session_id: str, skip_batches: int, limit: int, platform: str = ""
    ) -> list[dict]:
        """Fetch the latest N session-summary batches that rolled out of
        the host window (OFFSET conversion on the same terms as PG).

        The PG version inlines the bot_self-count sub-query into OFFSET;
        SQLite does it in two steps (count then query within the same
        connection session — consistent under the single-writer model).
        """
        async with self.pool.acquire() as conn:
            count_sql = (
                "SELECT count(*) FROM ("
                f"SELECT kind FROM {CHAT_SUMMARY_TABLE} "  # noqa: S608
                "WHERE session_id = $1 "
                + ("AND platform = $2 " if platform else "")
                + "AND summarized ORDER BY occurred_at DESC, id DESC LIMIT $3"
                ") w WHERE w.kind = 'bot_self'"
            )
            count_params = [session_id] + (
                [platform] if platform else []
            ) + [int(skip_batches)]
            bot_in_window = int(
                await conn.fetchval(count_sql, *count_params) or 0
            )
            offset = max(0, int(skip_batches) - bot_in_window)
            rows = await conn.fetch(
                f"SELECT document_id, content, occurred_at, kind "  # noqa: S608
                f"FROM {CHAT_SUMMARY_TABLE} "
                "WHERE session_id = $1 AND summarized AND kind = 'chat_summary' "
                + ("AND platform = $4 " if platform else "")
                + "ORDER BY occurred_at DESC, id DESC LIMIT $2 OFFSET $5",
                session_id,
                limit,
                int(skip_batches),
                platform,
                offset,
            )
        return _decode_rows(rows)

    async def fetch_pending_facts(self, limit: int) -> list[dict]:
        """Fetch a batch of unprocessed raw facts (extracted_flag=0, id
        ascending).

        embedding decodes back to a float list (the PG version returns a
        pgvector text literal via ``embedding::text`` —
        ``search_cluster_candidates`` accepts both shapes).
        """
        async with self.pool.acquire() as conn:
            rows = await conn.fetch(
                """
                SELECT id, document_id, platform, user_id, related_user_ids,
                       display_name, category, statement, confidence,
                       session_id, group_id, evidence_key, occurred_at,
                       embedding
                FROM memory_persona_fact_raw
                WHERE extracted_flag = 0
                ORDER BY id
                LIMIT $1
                """,
                limit,
            )
        return _decode_rows(rows)

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

        Ownership matching is the relation-aware symmetric condition (the
        read side folds related ids into the candidate keys), matching the
        PG version; the vector path is Python cosine (None degrades to a
        most-recently-updated top-K).
        """
        related = [r for r in (related_user_ids or []) if r]
        query_vec: list[float] | None = None
        if embedding is not None:
            query_vec = (
                embedding if isinstance(embedding, list) else parse_vector(embedding)
            )
            if query_vec is not None and len(query_vec) != self._embedding_dims:
                # Dimension mismatch (legacy rows from another embedding
                # model): degrade to the scalar path as if missing
                query_vec = None
        async with self.pool.acquire() as conn:
            if query_vec is None:
                rows = _decode_rows(
                    await conn.fetch(
                        f"""
                        SELECT id, canonical_statement, status, score,
                               evidence_count, evidence_keys, last_evidence_at,
                               occurred_at, replaced_by
                        FROM {FACT_CLUSTER_TABLE}
                        WHERE platform = $1 AND category = $2
                          AND (user_id = $3
                               OR EXISTS (SELECT 1 FROM json_each(related_user_ids) je
                                          WHERE je.value = $3)
                               OR user_id IN (SELECT value FROM json_each($4))
                               OR EXISTS (SELECT 1 FROM json_each(related_user_ids) je
                                          WHERE je.value IN
                                                (SELECT value FROM json_each($4))))
                        ORDER BY updated_at DESC LIMIT $5
                        """,  # noqa: S608
                        platform, category, user_id, _json_list(related), top_k,
                    )
                )
                return rows
            rows = _decode_rows(
                await conn.fetch(
                    f"""
                    SELECT id, canonical_statement, status, score,
                           evidence_count, evidence_keys, last_evidence_at,
                           occurred_at, replaced_by, embedding
                    FROM {FACT_CLUSTER_TABLE}
                    WHERE platform = $1 AND category = $2
                      AND embedding IS NOT NULL
                      AND (user_id = $3
                           OR EXISTS (SELECT 1 FROM json_each(related_user_ids) je
                                      WHERE je.value = $3)
                           OR user_id IN (SELECT value FROM json_each($4))
                           OR EXISTS (SELECT 1 FROM json_each(related_user_ids) je
                                      WHERE je.value IN
                                            (SELECT value FROM json_each($4))))
                    """,  # noqa: S608
                    platform, category, user_id, _json_list(related),
                )
            )
        candidates: list[dict] = []
        for row in rows:
            emb = row.get("embedding")
            if not emb:
                continue
            sim = _cosine(emb, query_vec)
            if sim is None:
                continue
            row["similarity"] = sim
            candidates.append(row)
        candidates.sort(key=lambda r: r["similarity"], reverse=True)
        return candidates[:top_k]

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

        Array columns (evidence_keys/source_fact_ids) become an
        in-transaction read-modify-write under SQLite (BEGIN IMMEDIATE
        already serializes writers, so no lost-update risk, plan §5.2); the
        optimistic-lock flag flip / disposal-and-flip sharing one
        transaction / contradiction marks leaving updated_at alone all
        match the PG version verbatim.
        """
        if action not in ("merge", "create", "replace"):
            raise ValueError(f"不支持的合并处置动作: {action}")
        if action in ("merge", "replace") and cluster_id is None:
            raise ValueError(f"action={action} 需要 cluster_id")

        now = _now_ts()
        occurred_ts = (
            sqlite_format_ts(_ensure_tz(occurred_at))
            if occurred_at is not None
            else None
        )
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
                    ids = [int(i) for i in contradict_cluster_ids if int(i) > 0]
                    if ids:
                        phs = ", ".join(f"${i + 2}" for i in range(len(ids)))
                        await conn.execute(
                            "UPDATE memory_fact_cluster SET contradicted_at = $1 "
                            f"WHERE id IN ({phs})",
                            now,
                            *ids,
                        )

                if action == "merge":
                    row = await conn.fetchrow(
                        f"""
                        SELECT id, score, evidence_count, evidence_keys,
                               status, demoted_at, contradicted_at, category,
                               last_evidence_at, occurred_at
                        FROM {FACT_CLUSTER_TABLE} WHERE id = $1
                        """,  # noqa: S608
                        cluster_id,
                    )
                    if row is None:
                        raise RuntimeError(
                            f"入簇目标簇不存在: cluster_id={cluster_id}"
                        )
                    # In-transaction read-modify-write (BEGIN IMMEDIATE
                    # serializes writers, no race window, plan §5.2); time
                    # columns stay TEXT (fixed ISO format, lexicographic =
                    # chronological)
                    keys = _json_array_of(row["evidence_keys"])
                    dup = evidence_key in keys
                    score = float(row["score"])
                    count = int(row["evidence_count"])
                    status = row["status"]
                    demoted_at = row["demoted_at"]
                    contradicted_at = row["contradicted_at"]
                    if not dup:
                        score = min(float(score_cap), score + float(start_score))
                        count += 1
                        keys = keys + [evidence_key]
                        if status in ("pending_uncertain", "dead"):
                            status = "active"
                        demoted_at = None
                        contradicted_at = None
                    last_evidence = row["last_evidence_at"] or row["occurred_at"]
                    if occurred_ts is not None and (
                        last_evidence is None or occurred_ts > last_evidence
                    ):
                        last_evidence = occurred_ts
                    occurred = row["occurred_at"]
                    if (
                        row["category"] == "recent"
                        and occurred_ts is not None
                        and (occurred is None or occurred_ts > occurred)
                    ):
                        occurred = occurred_ts
                    await conn.execute(
                        f"""
                        UPDATE {FACT_CLUSTER_TABLE} SET
                            score = $2, evidence_count = $3,
                            evidence_keys = $4, status = $5,
                            demoted_at = $6, contradicted_at = $7,
                            last_evidence_at = $8, occurred_at = $9,
                            updated_at = $10
                        WHERE id = $1
                        """,  # noqa: S608
                        cluster_id, score, count, _json_list(keys), status,
                        demoted_at, contradicted_at, last_evidence, occurred,
                        now,
                    )
                    # Profile-row sync: for already-profiled clusters the
                    # score copy follows the cluster score (pure replays
                    # with an unchanged score leave updated_at alone; score
                    # is NOT NULL so <> is synonymous with the PG version's
                    # IS DISTINCT FROM)
                    await conn.execute(
                        "UPDATE memory_user_profile SET score = $2, "
                        "updated_at = CASE WHEN score <> $2 THEN $3 "
                        "ELSE updated_at END WHERE cluster_id = $1",
                        cluster_id, score, now,
                    )
                    return {"action": "merge", "cluster_id": cluster_id,
                            "score": score, "status": status}

                raw = await conn.fetchrow(
                    f"""
                    SELECT platform, user_id, category, statement,
                           evidence_key, occurred_at, related_user_ids,
                           embedding
                    FROM memory_persona_fact_raw WHERE id = $1
                    """,  # noqa: S608
                    fact_id,
                )
                if raw is None:
                    raise RuntimeError(f"建簇来源事实不存在: fact_id={fact_id}")

                if action == "create":
                    row = await conn.fetchrow(
                        f"""
                        INSERT INTO {FACT_CLUSTER_TABLE}
                            (platform, user_id, category, canonical_statement,
                             score, status, evidence_count, evidence_keys,
                             source_fact_ids, last_evidence_at, occurred_at,
                             related_user_ids, embedding, written_at,
                             updated_at)
                        VALUES ($1, $2, $3, $4, $5, 'active', 1, $6, $7,
                                $8, $8, $9, $10, $11, $11)
                        RETURNING id
                        """,  # noqa: S608
                        raw["platform"], raw["user_id"], raw["category"],
                        raw["statement"], float(start_score),
                        _json_list([raw["evidence_key"]]),
                        _json_list([int(fact_id)]),
                        raw["occurred_at"], raw["related_user_ids"],
                        raw["embedding"], now,
                    )
                    return {"action": "create", "cluster_id": int(row["id"]),
                            "score": float(start_score), "status": "active"}

                # action == "replace": the new cluster inherits the old
                # cluster's score and the old one demotes to replaced
                old = await conn.fetchrow(
                    f"SELECT score, status FROM {FACT_CLUSTER_TABLE} "  # noqa: S608
                    "WHERE id = $1",
                    cluster_id,
                )
                if old is None or old["status"] == "replaced":
                    raise RuntimeError(
                        f"替代目标簇已不存在或已 replaced: cluster_id={cluster_id}"
                    )
                new_score = min(
                    float(score_cap),
                    max(float(start_score), float(old["score"])),
                )
                threshold = (
                    float(recent_promote_threshold)
                    if raw["category"] == "recent"
                    else float(promote_threshold)
                )
                new_status = "profiled" if new_score >= threshold else "active"
                row = await conn.fetchrow(
                    f"""
                    INSERT INTO {FACT_CLUSTER_TABLE}
                        (platform, user_id, category, canonical_statement,
                         score, status, evidence_count, evidence_keys,
                         source_fact_ids, last_evidence_at, occurred_at,
                         related_user_ids, embedding, written_at, updated_at)
                    VALUES ($1, $2, $3, $4, $5, $6, 1, $7, $8, $9, $9, $10,
                            $11, $12, $12)
                    RETURNING id
                    """,  # noqa: S608
                    raw["platform"], raw["user_id"], raw["category"],
                    raw["statement"], new_score, new_status,
                    _json_list([raw["evidence_key"]]),
                    _json_list([int(fact_id)]),
                    raw["occurred_at"], raw["related_user_ids"],
                    raw["embedding"], now,
                )
                new_id = int(row["id"])
                demoted = await conn.execute(
                    f"UPDATE {FACT_CLUSTER_TABLE} SET status = 'replaced', "  # noqa: S608
                    "score = score * 0.5, replaced_by = $2, updated_at = $3 "
                    "WHERE id = $1 AND status <> 'replaced'",
                    cluster_id, new_id, now,
                )
                if not demoted.endswith(" 1"):
                    # Concurrent writer already tombstoned the cluster: fail
                    # loud to keep the successor chain consistent (same
                    # semantics as the PG version).
                    raise RuntimeError(
                        f"替代目标簇已不存在或已 replaced: cluster_id={cluster_id}"
                    )
                # Relation propagation (P2-10): corrected clusters must not
                # keep their backfill-only relation edges alive.
                await self.supersede_backfill_edges_of(conn, [cluster_id])
                # Atomic profile-entry switch: delete the old row; insert a
                # new row if the new cluster meets the profile threshold
                await conn.execute(
                    "DELETE FROM memory_user_profile WHERE cluster_id = $1",
                    cluster_id,
                )
                if new_status == "profiled":
                    await conn.execute(
                        """
                        INSERT INTO memory_user_profile
                            (platform, user_id, category, cluster_id,
                             statement, score, related_user_ids,
                             created_at, updated_at)
                        VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $8)
                        ON CONFLICT (platform, user_id, category, cluster_id)
                        DO UPDATE SET statement = excluded.statement,
                                      score = excluded.score,
                                      related_user_ids = excluded.related_user_ids,
                                      updated_at = excluded.updated_at
                        """,
                        raw["platform"], raw["user_id"], raw["category"],
                        new_id, raw["statement"], new_score,
                        raw["related_user_ids"], now,
                    )
                return {"action": "replace", "cluster_id": new_id,
                        "score": new_score, "status": new_status,
                        "replaced_cluster_id": cluster_id,
                        "replaced_score": float(old["score"]) * 0.5}

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
        """Decay pass (single transaction; activity gating / evidence
        floor / profile anchoring semantics match the PG version).

        The PG version's data-modifying CTEs are rewritten as an
        in-transaction "collect id set first -> UPDATE/DELETE" flow; the
        split_part semantics (splitting participants into platform/uid)
        happen on the Python side.
        """
        now = _now_ts()
        async with self.pool.acquire() as conn:
            async with conn.transaction():
                gate = ""
                anchor_gate = ""
                if activity_since is not None:
                    cutoff = sqlite_format_ts(_ensure_tz(activity_since))
                    rows = await conn.fetch(
                        "SELECT platform, user_id, participants FROM "
                        f"{CHAT_SUMMARY_TABLE} "  # noqa: S608
                        "WHERE kind <> 'bot_self' AND occurred_at > $1",
                        cutoff,
                    )
                    pairs: set[tuple[str, str]] = set()
                    for r in rows:
                        if r["user_id"]:
                            pairs.add((r["platform"], r["user_id"]))
                        # participants is a raw JSON TEXT column here (rows
                        # bypass _decode_rows); parse before iterating —
                        # iterating the string directly yields characters
                        for key in _json_array_of(r["participants"]):
                            parts = str(key).split(":")
                            if len(parts) >= 2 and parts[1]:
                                pairs.add((parts[0], parts[1]))
                    pairs = {
                        (p or "", u or "") for p, u in pairs if u
                    }
                    await conn.execute(
                        "DROP TABLE IF EXISTS temp._decay_active_pairs"
                    )
                    await conn.execute(
                        "CREATE TEMP TABLE _decay_active_pairs "
                        "(platform TEXT, user_id TEXT)"
                    )
                    if pairs:
                        await conn.executemany(
                            "INSERT INTO temp._decay_active_pairs "
                            "VALUES ($1, $2)",
                            sorted(pairs),
                        )
                    gate = (
                        " AND EXISTS (SELECT 1 FROM temp._decay_active_pairs a "
                        "WHERE a.platform = memory_fact_cluster.platform "
                        "AND a.user_id = memory_fact_cluster.user_id)"
                    )
                if anchor_profile_size > 0:
                    await conn.execute(
                        "DROP TABLE IF EXISTS temp._decay_anchors"
                    )
                    await conn.execute(
                        "CREATE TEMP TABLE _decay_anchors "
                        "(platform TEXT, user_id TEXT, cluster_id INTEGER)"
                    )
                    anchor_rows = await conn.fetch(
                        """
                        SELECT platform, user_id, cluster_id FROM (
                            SELECT p.platform, p.user_id, p.cluster_id,
                                   row_number() OVER (
                                       PARTITION BY p.platform, p.user_id,
                                       p.category
                                       ORDER BY p.score DESC, p.updated_at DESC,
                                       p.id ASC
                                   ) AS rn
                            FROM memory_user_profile p
                            JOIN memory_fact_cluster c ON c.id = p.cluster_id
                            WHERE p.category <> 'recent'
                              AND c.contradicted_at IS NULL
                        ) t WHERE rn <= $1
                        """,
                        anchor_profile_size,
                    )
                    if anchor_rows:
                        await conn.executemany(
                            "INSERT INTO temp._decay_anchors VALUES ($1, $2, $3)",
                            [
                                (r["platform"], r["user_id"], int(r["cluster_id"]))
                                for r in anchor_rows
                            ],
                        )
                    anchor_gate = (
                        " AND NOT EXISTS (SELECT 1 FROM temp._decay_anchors x "
                        "WHERE x.platform = memory_fact_cluster.platform "
                        "AND x.user_id = memory_fact_cluster.user_id "
                        "AND x.cluster_id = memory_fact_cluster.id)"
                    )

                # 1) Recent-dimension expiry demotion (calendar semantics,
                # ungated) + profile-row cascade
                recent_cutoff = sqlite_format_ts(
                    datetime.now(timezone.utc) - timedelta(days=recent_expire_days)
                )
                expired_ids = [
                    int(r["id"])
                    for r in await conn.fetch(
                        f"SELECT id FROM {FACT_CLUSTER_TABLE} "  # noqa: S608
                        "WHERE category = 'recent' "
                        "AND status IN ('active', 'profiled') "
                        "AND occurred_at < $1",
                        recent_cutoff,
                    )
                ]
                expired_cleaned = 0
                if expired_ids:
                    phs = ", ".join(f"${i + 2}" for i in range(len(expired_ids)))
                    await conn.execute(
                        f"UPDATE {FACT_CLUSTER_TABLE} SET "  # noqa: S608
                        f"status = 'pending_uncertain', demoted_at = $1, "
                        f"updated_at = $1 WHERE id IN ({phs})",
                        now, *expired_ids,
                    )
                    expired_cleaned = int(
                        await conn.execute(
                            "DELETE FROM memory_user_profile WHERE cluster_id "
                            f"IN ({phs})",
                            *expired_ids,
                        ).rsplit(" ", 1)[-1]
                    )

                # 2) Non-recent score decay (evidence floor inside the SQL
                # CASE, same shape as PG)
                if sticky_evidence_count > 0:
                    score_set = (
                        "score = CASE WHEN evidence_count >= $2 "
                        "AND contradicted_at IS NULL AND score * $1 < $3 "
                        "THEN $3 ELSE score * $1 END, updated_at = $4"
                    )
                    decay_params: list = [
                        decay_factor, sticky_evidence_count, demote_threshold, now,
                    ]
                else:
                    score_set = "score = score * $1, updated_at = $2"
                    decay_params = [decay_factor, now]
                await conn.execute(
                    f"UPDATE {FACT_CLUSTER_TABLE} SET {score_set} "  # noqa: S608
                    "WHERE category <> 'recent' "
                    "AND status IN ('active', 'profiled')"
                    f"{gate}{anchor_gate}",
                    *decay_params,
                )

                # 3) Profiled demotion check (on decayed scores) +
                # profile-row cascade
                demoted_ids = [
                    int(r["id"])
                    for r in await conn.fetch(
                        f"SELECT id FROM {FACT_CLUSTER_TABLE} WHERE "  # noqa: S608
                        "status = 'profiled' AND score < $1"
                        f"{gate}{anchor_gate}",
                        demote_threshold,
                    )
                ]
                demoted_cleaned = 0
                if demoted_ids:
                    phs = ", ".join(f"${i + 2}" for i in range(len(demoted_ids)))
                    await conn.execute(
                        f"UPDATE {FACT_CLUSTER_TABLE} SET "  # noqa: S608
                        f"status = 'pending_uncertain', demoted_at = $1, "
                        f"updated_at = $1 WHERE id IN ({phs})",
                        now, *demoted_ids,
                    )
                    demoted_cleaned = int(
                        await conn.execute(
                            "DELETE FROM memory_user_profile WHERE cluster_id "
                            f"IN ({phs})",
                            *demoted_ids,
                        ).rsplit(" ", 1)[-1]
                    )

                # 4) pending_uncertain death after pending_dead_days
                # (gated)
                dead_cutoff = sqlite_format_ts(
                    datetime.now(timezone.utc) - timedelta(days=pending_dead_days)
                )
                dead_rowcount = await conn.execute(
                    f"UPDATE {FACT_CLUSTER_TABLE} SET status = 'dead', "  # noqa: S608
                    "updated_at = $1 WHERE status = 'pending_uncertain' "
                    "AND COALESCE(demoted_at, last_evidence_at, occurred_at) < $2"
                    f"{gate}",
                    now, dead_cutoff,
                )

                await conn.execute("DROP TABLE IF EXISTS temp._decay_anchors")
                await conn.execute("DROP TABLE IF EXISTS temp._decay_active_pairs")

        return {
            "expired_recent": len(expired_ids),
            "demoted": len(demoted_ids),
            "profile_rows_deleted": expired_cleaned + demoted_cleaned,
            "deaded": int(dead_rowcount.rsplit(" ", 1)[-1]),
        }

    async def promote_pass(
        self, *, promote_threshold: float, recent_promote_threshold: float
    ) -> int:
        """Promotion pass: active clusters reaching the threshold become
        profiled with a profile-table upsert (single transaction)."""
        now = _now_ts()
        async with self.pool.acquire() as conn:
            async with conn.transaction():
                promoted = await conn.fetch(
                    f"SELECT id, platform, user_id, category, "  # noqa: S608
                    "canonical_statement, score, related_user_ids FROM "
                    f"{FACT_CLUSTER_TABLE} "
                    "WHERE status = 'active' AND score >= CASE "
                    "WHEN category = 'recent' THEN $2 ELSE $1 END",
                    promote_threshold,
                    recent_promote_threshold,
                )
                if promoted:
                    ids = [int(r["id"]) for r in promoted]
                    phs = ", ".join(f"${i + 2}" for i in range(len(ids)))
                    await conn.execute(
                        f"UPDATE {FACT_CLUSTER_TABLE} SET "  # noqa: S608
                        f"status = 'profiled', updated_at = $1 WHERE id IN ({phs})",
                        now, *ids,
                    )
                    await conn.executemany(
                        """
                        INSERT INTO memory_user_profile
                            (platform, user_id, category, cluster_id,
                             statement, score, related_user_ids,
                             created_at, updated_at)
                        VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $8)
                        ON CONFLICT (platform, user_id, category, cluster_id)
                        DO UPDATE SET statement = excluded.statement,
                                      score = excluded.score,
                                      related_user_ids = excluded.related_user_ids,
                                      updated_at = excluded.updated_at
                        """,
                        [
                            (
                                r["platform"], r["user_id"], r["category"],
                                int(r["id"]), r["canonical_statement"],
                                float(r["score"]),
                                # JSON TEXT -> JSON TEXT pass-through (the
                                # column stores the same encoding; feeding it
                                # through _json_list would iterate the string
                                # char-by-char and corrupt it)
                                r["related_user_ids"] or "[]", now,
                            )
                            for r in promoted
                        ],
                    )
        return len(promoted)

    async def get_kv(self, key: str) -> str | None:
        """Read an internal plugin kv value (e.g. the decay pass's last
        run time)."""
        async with self.pool.acquire() as conn:
            row = await conn.fetchrow(
                "SELECT value FROM _memory_local_kv WHERE key = $1", key
            )
        return row["value"] if row else None

    async def set_kv(self, key: str, value: str) -> None:
        """Write an internal plugin kv value (upsert)."""
        async with self.pool.acquire() as conn:
            await conn.execute(
                """
                INSERT INTO _memory_local_kv (key, value, updated_at)
                VALUES ($1, $2, $3)
                ON CONFLICT (key) DO UPDATE
                SET value = excluded.value, updated_at = excluded.updated_at
                """,
                key, value, _now_ts(),
            )

    # ------------------------------------------------------------------
    #  persistent entity alias layer (P1)
    # ------------------------------------------------------------------

    async def alias_upsert(self, rows: list[dict]) -> None:
        """Batch upsert of alias rows (unique key platform+user_id+name;
        last_seen monotonic)."""
        if not rows:
            return
        payload = [
            (
                r["platform"],
                r["user_id"],
                r["name"],
                sqlite_format_ts(_ensure_tz(r["last_seen"])),
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
                    last_seen = MAX(memory_entity_alias.last_seen, excluded.last_seen),
                    source = CASE
                        WHEN excluded.last_seen > memory_entity_alias.last_seen
                        THEN excluded.source
                        ELSE memory_entity_alias.source END
                """,
                payload,
            )

    async def alias_fetch_all(self) -> list[dict]:
        """All alias rows (full in-memory view reload; the table is
        bounded at ~tens of thousands of rows, millisecond cost)."""
        async with self.pool.acquire() as conn:
            rows = await conn.fetch(
                "SELECT platform, user_id, name, last_seen "
                "FROM memory_entity_alias"
            )
        out = []
        for r in rows:
            seen = sqlite_parse_ts(r["last_seen"])
            out.append(
                {
                    "platform": r["platform"],
                    "user_id": r["user_id"],
                    "name": r["name"],
                    "last_seen_epoch": seen.timestamp() if seen else 0.0,
                }
            )
        return out

    async def fetch_alias_names_by_owner(
        self, owners: list[tuple[str, str]] | None = None
    ) -> dict[tuple[str, str], str]:
        """(platform, uid) -> latest non-placeholder alias (canonical-name
        resolution source for the graph read side)."""
        async with self.pool.acquire() as conn:
            if owners:
                pairs = [(str(p or ""), str(u or "")) for p, u in owners if u]
                if not pairs:
                    return {}
                p = _Params(None)
                pair_sql = ", ".join(
                    f"({p.add(a)}, {p.add(b)})" for a, b in pairs
                )
                rows = await conn.fetch(
                    "SELECT a.platform, a.user_id, a.name "
                    "FROM memory_entity_alias a "
                    f"WHERE (a.platform, a.user_id) IN ({pair_sql}) "
                    "ORDER BY a.last_seen DESC, a.name ASC",
                    *p.values,
                )
            else:
                rows = await conn.fetch(
                    "SELECT platform, user_id, name FROM memory_entity_alias "
                    "ORDER BY last_seen DESC, name ASC"
                )
        out: dict[tuple[str, str], str] = {}
        for r in rows:
            name = str(r["name"] or "").strip()
            if not name or is_placeholder_name(name):
                continue
            out.setdefault((r["platform"], str(r["user_id"])), name)
        return out

    # ------------------------------------------------------------------
    #  entity relation edges (P2 write / P3 inject)
    # ------------------------------------------------------------------

    async def upsert_entity_edge(
        self, rows: list[dict], *, label_stopwords: list[str] | None = None
    ) -> None:
        """Batch zero-LLM merge of edge rows (structural-key upsert;
        semantics identical to the PG version).

        Dialect rewrites: ``= ANY(evidence_keys)`` -> json_each EXISTS;
        ``array_append`` -> ``json_insert '$[#]'``; ``~*`` -> REGEXP (the
        Python function registered at connect, with the pattern still
        coming from the single-source constant PLACEHOLDER_NAME_SQL);
        ``now()`` -> supplied by Python ($15).
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
        now = _now_ts()
        payload = [
            (
                r["platform"],
                r["subject_uid"],
                r["object_uid"],
                r["subject_name"],
                r["object_name"],
                r["relation_label"],
                r["statement"],
                "pending" if r["is_bot_edge"] and int(r["min_evidence"]) > 1
                else "active",
                r["confidence"],
                sqlite_format_ts(_ensure_tz(r["occurred_at"])),
                r["evidence_key"],
                1 if r["is_bot_edge"] else 0,
                int(r["min_evidence"]),
                1 if r.get("count_on_conflict", True) else 0,
                now,
            )
            for r in rows
        ]
        async with self.pool.acquire() as conn:
            await conn.executemany(
                f"""
                INSERT INTO memory_entity_edge (
                    platform, subject_uid, object_uid, subject_name,
                    object_name, relation_label, statement, status,
                    confidence, evidence_count, evidence_keys, first_seen,
                    last_seen, occurred_at
                )
                VALUES (
                    $1, $2, $3, $4, $5, $6, $7, $8, $9,
                    1, json_array($11), $10, $10, $10
                )
                ON CONFLICT (platform, subject_uid, object_uid, relation_label)
                DO UPDATE SET
                    subject_name = CASE
                        WHEN excluded.subject_name = ''
                            OR REGEXP('{PLACEHOLDER_NAME_SQL}',
                                      excluded.subject_name)
                        THEN memory_entity_edge.subject_name
                        ELSE excluded.subject_name END,
                    object_name = CASE
                        WHEN excluded.object_name = ''
                            OR REGEXP('{PLACEHOLDER_NAME_SQL}',
                                      excluded.object_name)
                        THEN memory_entity_edge.object_name
                        ELSE excluded.object_name END,
                    statement = excluded.statement,
                    last_seen = MAX(memory_entity_edge.last_seen, excluded.last_seen),
                    occurred_at = MAX(memory_entity_edge.occurred_at, excluded.occurred_at),
                    updated_at = $15,
                    confidence = CASE WHEN excluded.confidence = 'high'
                                      THEN 'high'
                                      ELSE memory_entity_edge.confidence END,
                    evidence_count = CASE
                        WHEN NOT $14 THEN memory_entity_edge.evidence_count
                        WHEN EXISTS (SELECT 1 FROM json_each(memory_entity_edge.evidence_keys) je
                                     WHERE je.value = $11)
                        THEN memory_entity_edge.evidence_count
                        ELSE memory_entity_edge.evidence_count + 1 END,
                    evidence_keys = CASE
                        WHEN NOT $14 THEN memory_entity_edge.evidence_keys
                        WHEN EXISTS (SELECT 1 FROM json_each(memory_entity_edge.evidence_keys) je
                                     WHERE je.value = $11)
                        THEN memory_entity_edge.evidence_keys
                        ELSE json_insert(memory_entity_edge.evidence_keys, '$[#]', $11) END,
                    status = CASE
                        WHEN $12
                             AND memory_entity_edge.status <> 'superseded'
                             AND (
                            CASE WHEN NOT $14
                                 THEN memory_entity_edge.evidence_count
                                 WHEN EXISTS (SELECT 1 FROM json_each(memory_entity_edge.evidence_keys) je
                                              WHERE je.value = $11)
                                 THEN memory_entity_edge.evidence_count
                                 ELSE memory_entity_edge.evidence_count + 1 END
                        ) >= $13
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

        superseded tombstones do not count as "existing" (same semantics as
        the PG version).
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
        if not all_keys:
            return set()
        p = _Params(None)
        tuple_sql = ", ".join(
            f"({p.add(k[0])}, {p.add(k[1])}, {p.add(k[2])}, {p.add(k[3])})"
            for k in all_keys
        )
        recs = await conn.fetch(
            "SELECT DISTINCT e.platform, e.subject_uid, e.object_uid, "
            "e.relation_label FROM memory_entity_edge e "
            f"WHERE (e.platform, e.subject_uid, e.object_uid, e.relation_label) "
            f"IN ({tuple_sql}) AND e.status <> 'superseded'",
            *p.values,
        )
        return {
            (r["platform"], r["subject_uid"], r["object_uid"], r["relation_label"])
            for r in recs
        }

    async def fetch_active_edges(
        self, node_keys: list[str], bot_keys: object = ""
    ) -> list[dict]:
        """Injection candidate edges: active with an endpoint hitting the
        node set (bot-endpoint edges included when bot_keys is non-empty)."""
        keys = [str(k or "") for k in (node_keys or []) if k]
        bots = sorted(str(k or "") for k in (bot_keys or []) if k)
        if not keys and not bots:
            return []
        p = _Params(None)
        subj_phs = ", ".join(p.add(k) for k in keys) if keys else None
        obj_phs = ", ".join(p.add(k) for k in keys) if keys else None
        cond_parts: list[str] = []
        if keys:
            cond_parts.append(
                f"platform || ':' || subject_uid IN ({subj_phs})"
                f" OR platform || ':' || object_uid IN ({obj_phs})"
            )
        if bots:
            bot_subj = ", ".join(p.add(k) for k in bots)
            bot_obj = ", ".join(p.add(k) for k in bots)
            bot_cond = (
                f"platform || ':' || subject_uid IN ({bot_subj})"
                f" OR platform || ':' || object_uid IN ({bot_obj})"
            )
            cond_parts.append(bot_cond if not keys else f"({bot_cond})")
        async with self.pool.acquire() as conn:
            rows = await conn.fetch(
                "SELECT id, platform, subject_uid, object_uid, subject_name, "
                "object_name, relation_label, statement, "
                "evidence_count, last_seen, occurred_at "
                "FROM memory_entity_edge "
                f"WHERE status = 'active' AND ({' OR '.join(cond_parts)}) "
                "ORDER BY evidence_count DESC, last_seen DESC LIMIT 64",
                *p.values,
            )
        return _decode_rows(rows)

    async def fetch_edges_for_audit(self, after_id: int, limit: int) -> list[dict]:
        """Edges submitted to the semantic audit pass: id above the
        watermark and not tombstoned (active/pending)."""
        async with self.pool.acquire() as conn:
            rows = await conn.fetch(
                "SELECT id, platform, subject_uid, object_uid, subject_name, "
                "object_name, relation_label, statement, status, "
                "confidence, evidence_count, last_seen, updated_at "
                "FROM memory_entity_edge "
                "WHERE id > $1 AND status IN ('active', 'pending') "
                "ORDER BY id ASC LIMIT $2",
                int(after_id),
                int(limit),
            )
        return _decode_rows(rows)

    async def supersede_edges(
        self,
        edge_ids: list[int],
        *,
        expected_updated_at: dict[int, datetime] | None = None,
        reasons: dict[int, str] | None = None,
    ) -> int:
        """Batch-mark edges superseded (shared by the semantic audit pass
        and the admin page; idempotent, returns affected rows).

        The PG version's ``UPDATE ... FROM unnest(...)`` becomes a
        per-id UPDATE loop inside one transaction (batch size ≤ the audit
        batch size, few rows; the optimistic-lock semantics are row-for-row
        equivalent).
        """
        ids = [int(i) for i in edge_ids if int(i) > 0]
        if not ids:
            return 0
        reason_map = {int(k): str(v)[:200] for k, v in (reasons or {}).items()}
        now = _now_ts()
        count = 0
        async with self.pool.acquire() as conn:
            async with conn.transaction():
                for edge_id in ids:
                    reason = reason_map.get(edge_id, "")
                    if expected_updated_at:
                        ts = expected_updated_at.get(edge_id)
                        ts_text = (
                            sqlite_format_ts(_ensure_tz(ts)) if ts else None
                        )
                        if ts_text is None:
                            continue
                        row = await conn.fetchrow(
                            "UPDATE memory_entity_edge SET status = 'superseded', "
                            "superseded_at = $2, updated_at = $2, "
                            "supersede_reason = CASE WHEN $3 <> '' THEN $3 "
                            "ELSE supersede_reason END "
                            "WHERE id = $1 AND updated_at = $4 "
                            "AND status <> 'superseded' RETURNING id",
                            edge_id, now, reason, ts_text,
                        )
                    else:
                        row = await conn.fetchrow(
                            "UPDATE memory_entity_edge SET status = 'superseded', "
                            "superseded_at = $2, updated_at = $2, "
                            "supersede_reason = CASE WHEN $3 <> '' THEN $3 "
                            "ELSE supersede_reason END "
                            "WHERE id = $1 AND status <> 'superseded' "
                            "RETURNING id",
                            edge_id, now, reason,
                        )
                    if row is not None:
                        count += 1
        return count

    async def supersede_backfill_edges_of(self, conn, cluster_ids: list[int]) -> int:
        """Tombstone propagation to backfill-source edges (caller owns the
        transaction).

        Same Python decision skeleton as the PG version; the array scan is
        rewritten with json_each: ``unnest(evidence_keys)`` ->
        ``json_each(evidence_keys)``.
        """
        dead = {int(i) for i in cluster_ids or [] if int(i) > 0}
        if not dead:
            return 0
        rows = await conn.fetch(
            """
            SELECT id, evidence_keys FROM memory_entity_edge
            WHERE status = 'active'
              AND EXISTS (SELECT 1 FROM json_each(evidence_keys) k
                          WHERE k.value LIKE 'backfill|%' ESCAPE '\\')
            """
        )
        doomed: list[int] = []
        for r in rows:
            keys = r["evidence_keys"]
            if isinstance(keys, str):
                try:
                    keys = json.loads(keys)
                except (ValueError, TypeError):
                    keys = []
            sources: set[int] = set()
            has_online_evidence = False
            for key in list(keys or []):
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
                doomed.append(int(r["id"]))
        if doomed:
            phs = ", ".join(f"${i + 2}" for i in range(len(doomed)))
            await conn.execute(
                "UPDATE memory_entity_edge SET status = 'superseded', "
                "superseded_at = $1, supersede_reason = 'source cluster "
                "removed', updated_at = $1 "
                f"WHERE id IN ({phs})",
                _now_ts(), *doomed,
            )
        return len(doomed)

    async def relation_integrity_report(self, *, pending_stale_days: int = 14) -> dict:
        """First-layer structural self-check (zero-LLM rule sentinels;
        invariants identical to the PG version)."""
        async with self.pool.acquire() as conn:
            mismatch = await conn.fetchval(
                """
                SELECT count(*) FROM memory_entity_edge
                WHERE evidence_count IS NOT
                      COALESCE(json_array_length(evidence_keys), 0)
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
                   OR REGEXP('{PLACEHOLDER_NAME_SQL}', subject_name)
                   OR REGEXP('{PLACEHOLDER_NAME_SQL}', object_name)
                """
            )
            bad_label = await conn.fetchval(
                """
                SELECT count(*) FROM memory_entity_edge
                WHERE length(relation_label) NOT BETWEEN 2 AND 8
                   OR (length(subject_name) >= 2
                       AND instr(relation_label, subject_name) > 0)
                   OR (length(object_name) >= 2
                       AND instr(relation_label, object_name) > 0)
                """
            )
            stale_cutoff = sqlite_format_ts(
                datetime.now(timezone.utc) - timedelta(days=int(pending_stale_days))
            )
            pending_stale = await conn.fetchval(
                "SELECT count(*) FROM memory_entity_edge "
                "WHERE status = 'pending' AND last_seen < $1",
                stale_cutoff,
            )
            stale_names = await conn.fetchval(
                f"""
                WITH canon AS (
                  SELECT platform, user_id, name FROM (
                    SELECT platform, user_id, name, row_number() OVER (
                        PARTITION BY platform, user_id
                        ORDER BY last_seen DESC
                    ) AS rn
                    FROM memory_entity_alias
                    WHERE name <> ''
                      AND NOT REGEXP('{PLACEHOLDER_NAME_SQL}', name)
                  ) t WHERE rn = 1
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
                  AND EXISTS (SELECT 1 FROM json_each(e.evidence_keys) k
                              WHERE k.value LIKE 'backfill|%' ESCAPE '\\')
                  AND NOT EXISTS (SELECT 1 FROM json_each(e.evidence_keys) k
                                  WHERE k.value NOT LIKE 'backfill|%' ESCAPE '\\')
                  AND EXISTS (
                      SELECT 1 FROM memory_fact_cluster c
                      WHERE EXISTS (SELECT 1 FROM json_each(e.evidence_keys) k
                                    WHERE k.value = 'backfill|' || c.id)
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
        the legacy relation backfill."""
        sql = (
            f"SELECT id, platform, user_id, canonical_statement, occurred_at "  # noqa: S608
            f"FROM {FACT_CLUSTER_TABLE} "
            "WHERE status IN ('active', 'profiled') "
            "AND canonical_statement <> '' AND id > $1 ORDER BY id"
        )
        params: list = [int(after_id)]
        if limit is not None:
            sql += " LIMIT $2"
            params.append(int(limit))
        async with self.pool.acquire() as conn:
            rows = await conn.fetch(sql, *params)
        return _decode_rows(rows)

    async def count_confirmed_relation_sources(
        self, after_id: int = 0, exclude_user_id: str = ""
    ) -> int:
        """Total confirmed backfill-source clusters (progress denominator
        of the paged backfill)."""
        async with self.pool.acquire() as conn:
            value = await conn.fetchval(
                f"SELECT count(*) FROM {FACT_CLUSTER_TABLE} "  # noqa: S608
                "WHERE status IN ('active', 'profiled') "
                "AND canonical_statement <> '' AND id > $1 AND user_id <> $2",
                int(after_id),
                exclude_user_id or "",
            )
        return int(value or 0)

    async def fetch_confirmed_relation_sources_by_ids(
        self, cluster_ids: list[int]
    ) -> list[dict]:
        """Fetch confirmed clusters by exact ids (consumed by the
        revived-cluster backfill todo)."""
        ids = [int(i) for i in cluster_ids or [] if int(i) > 0]
        if not ids:
            return []
        phs = ", ".join(f"${i + 1}" for i in range(len(ids)))
        async with self.pool.acquire() as conn:
            rows = await conn.fetch(
                f"SELECT id, platform, user_id, canonical_statement, "  # noqa: S608
                f"occurred_at FROM {FACT_CLUSTER_TABLE} "
                f"WHERE id IN ({phs}) "
                "AND status IN ('active', 'profiled') "
                "AND canonical_statement <> '' ORDER BY id",
                *ids,
            )
        return _decode_rows(rows)

    async def fetch_alias_directory(self) -> list[tuple[str, str, str]]:
        """Full alias table (name -> (platform, uid) directory; the
        backfill's object-resolution source)."""
        async with self.pool.acquire() as conn:
            rows = await conn.fetch(
                "SELECT platform, user_id, name FROM memory_entity_alias"
            )
        return [(r["platform"], r["user_id"], r["name"]) for r in rows]

    # ------------------------------------------------------------------
    #  profile assembly queries (M5)
    # ------------------------------------------------------------------

    async def fetch_profile_sections(
        self, platform: str, user_id: str, per_section_limit: int
    ) -> dict[str, list[str]]:
        """Per-section profile rows (data source for the first five
        sections; owner-only, ordering identical to the PG version)."""
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
                    WHERE platform = $1 AND user_id = $2
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
        (halved-score) clusters."""
        async with self.pool.acquire() as conn:
            rows = await conn.fetch(
                f"SELECT canonical_statement FROM {FACT_CLUSTER_TABLE} "  # noqa: S608
                "WHERE platform = $1 AND user_id = $2 "
                "AND status IN ('pending_uncertain', 'replaced') "
                "ORDER BY score DESC, updated_at DESC, id ASC LIMIT $3",
                platform,
                user_id,
                limit,
            )
        return [r["canonical_statement"] for r in rows]

    async def fetch_profile_sections_multi(
        self, platforms: list[str], user_ids: list[str], per_section_limit: int
    ) -> dict[str, list[str]]:
        """Per-section profile rows with multi-account keys merged (key
        semantics/ordering/dedup identical to the PG version)."""
        pairs = list(zip(platforms, user_ids))
        async with self.pool.acquire() as conn:
            if not pairs:
                return {}
            p = _Params(None)
            pair_sql = ", ".join(
                f"({p.add(a)}, {p.add(b)})" for a, b in pairs
            )
            rows = await conn.fetch(
                f"""
                SELECT category, statement FROM (
                    SELECT m.category, m.statement,
                           row_number() OVER (
                               PARTITION BY m.category
                               ORDER BY m.score DESC, m.updated_at DESC, m.id ASC
                           ) AS rn
                    FROM memory_user_profile m
                    WHERE (m.platform, m.user_id) IN ({pair_sql})
                ) t
                WHERE rn <= {p.add(per_section_limit)}
                ORDER BY category, rn
                """,
                *p.values,
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
        dedup/ownership semantics as in the single-key version)."""
        pairs = list(zip(platforms, user_ids))
        async with self.pool.acquire() as conn:
            if not pairs:
                return []
            p = _Params(None)
            pair_sql = ", ".join(
                f"({p.add(a)}, {p.add(b)})" for a, b in pairs
            )
            rows = await conn.fetch(
                f"""
                SELECT m.canonical_statement
                FROM {FACT_CLUSTER_TABLE} m
                WHERE (m.platform, m.user_id) IN ({pair_sql})
                  AND m.status IN ('pending_uncertain', 'replaced')
                ORDER BY m.score DESC, m.updated_at DESC, m.id ASC
                LIMIT {p.add(limit)}
                """,  # noqa: S608
                *p.values,
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
        (profile header; globally latest across keys)."""
        pairs = list(zip(platforms, user_ids))
        async with self.pool.acquire() as conn:
            if not pairs:
                return None
            p = _Params(None)
            pair_sql = ", ".join(
                f"({p.add(a)}, {p.add(b)})" for a, b in pairs
            )
            value = await conn.fetchval(
                "SELECT f.display_name FROM memory_persona_fact_raw f "
                f"WHERE (f.platform, f.user_id) IN ({pair_sql}) "
                "AND f.display_name <> '' "
                "ORDER BY f.occurred_at DESC, f.id DESC LIMIT 1",
                *p.values,
            )
        return value

    async def fetch_latest_display_name(
        self, platform: str, user_id: str
    ) -> str | None:
        """The display name registered at the user's latest fact
        extraction (profile header)."""
        async with self.pool.acquire() as conn:
            value = await conn.fetchval(
                "SELECT display_name FROM memory_persona_fact_raw "
                "WHERE platform = $1 AND user_id = $2 AND display_name <> '' "
                "ORDER BY occurred_at DESC, id DESC LIMIT 1",
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
        maintenance tool; FTS5 shadow sync)."""
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
            async with conn.transaction():
                row = await conn.fetchrow(sql, *params)
                if row is not None:
                    await conn.execute(
                        "DELETE FROM memory_chat_summary_fts WHERE summary_id = $1",
                        int(row["id"]),
                    )
        return row is not None
