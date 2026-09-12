"""MemoryBackend abstract interface + pure helpers shared by both backends
(db package split, 2026-09-12).

Storage seam design (docs/plans/noriflow-dual-storage-backend-plan.md §3):
- postgres.py / sqlite.py each implement the same `MemoryBackend` method
  set; layers above (kernel / merge_agent / persona_service / webui_store /
  main) depend only on that interface and the `.pool` attribute;
- this module holds only dialect-agnostic pure functions and constants;
  PG-specific SQL stays in postgres.py (supersede_backfill_edges_of etc.),
  SQLite-specific rewrites live in sqlite.py;
- time format (SQLite side): TEXT in fixed ``%Y-%m-%dT%H:%M:%S.%f`` UTC +
  ``Z`` (always 6 fractional digits, so lexicographic order = chronological
  order), with reads/writes funneled through the sqlite_format_ts /
  sqlite_parse_ts boundary functions.

The caller (LocalMemoryKernel) owns circuit-breaking and fail-open
semantics; this layer lets exceptions propagate unchanged.
"""

from __future__ import annotations

import json
import re
from abc import ABC, abstractmethod
from datetime import datetime, timezone

# Table-name constants: whitelist for dynamic SQL (anti-injection — table
# names cannot be parameterized)
CHAT_SUMMARY_TABLE = "memory_chat_summary"
FACT_RAW_TABLE = "memory_persona_fact_raw"
FACT_CLUSTER_TABLE = "memory_fact_cluster"
_BACKFILL_TABLES = frozenset(
    {CHAT_SUMMARY_TABLE, FACT_RAW_TABLE, FACT_CLUSTER_TABLE}
)
# Column holding the recomputation text per table (summary content /
# fact statement / cluster canonical_statement)
_BACKFILL_TEXT_COLUMNS = {
    CHAT_SUMMARY_TABLE: "content",
    FACT_RAW_TABLE: "statement",
    FACT_CLUSTER_TABLE: "canonical_statement",
}

# Migration file names: three-digit version prefix (001_init.sql -> version=1)
_MIGRATION_NAME_RE = re.compile(r"^(\d{3})_[a-z0-9_]+\.sql$")


def fact_document_id(
    uids: list[str], session_id: str, occurred_at: datetime
) -> str:
    """Build the ownership prefix of the fact-table idempotency key
    "{uids}-{md5(statement)[:12]}".

    document_id = {sorted deduped uid list}@{session_id}|{date}-{content_hash}:
    the idempotency granularity aligns with evidence_key ({session}|{date}) —
    duplicate extractions within the same session and day (double writes /
    history-window overlap) still land on the same key and get deduplicated,
    while verbatim recurrence across sessions/days is independent evidence
    that must be stored and counted (the old statement-level global
    idempotency silently swallowed those, systematically undercounting
    evidence_count).

    Args:
        uids: sorted deduped ownership uid list (user_id + related_user_ids).
        session_id: session the fact was extracted from.
        occurred_at: fact occurrence time (date part used; the caller is
            responsible for local-timezone normalization —
            kernel._to_local / merge_agent._to_local; strftime on a raw
            aware-UTC value can be a day off from the other path).

    Returns:
        The document_id prefix (caller appends "-{content_hash}").
    """
    return f"{'-'.join(uids)}@{session_id}|{occurred_at.strftime('%Y-%m-%d')}"


def vector_literal(vec: list[float] | None) -> str | None:
    """Convert a float vector to a pgvector text literal ('[0.1,0.2,...]');
    None passes through unchanged.

    With asyncpg and no registered vector codec, pgvector columns are read
    and written as text with a ::vector cast on the SQL side (PG backend
    only — the SQLite backend stores float32 BLOBs, see sqlite.py; this
    function keeps its original signature for the PG path and test stubs).

    Args:
        vec: embedding vector; None means missing (writes NULL).

    Returns:
        pgvector text literal, or None.
    """
    if vec is None:
        return None
    return "[" + ",".join(repr(v) for v in vec) + "]"


def parse_vector(value) -> list[float] | None:
    """Parse an embedding read from the DB into a float list (for
    dedup-similarity computation).

    Three shapes (depending on backend / wiring):
    - asyncpg without a registered vector codec: pgvector text
      '[0.1,0.2,...]' (which happens to be a valid JSON array);
    - test stubs / registered codec: list/tuple;
    - SQLite backend: float32 little-endian BLOB (struct unpack, no numpy).
    Unparseable shapes return None (callers treat it as missing: rows
    without a vector neither join dedup comparison nor get dropped).

    Args:
        value: embedding value read from the DB (str / list / bytes / other).

    Returns:
        float list, or None.
    """
    if isinstance(value, (list, tuple)):
        try:
            return [float(x) for x in value]
        except (TypeError, ValueError):
            return None
    if isinstance(value, (bytes, bytearray, memoryview)):
        raw = bytes(value)
        if len(raw) % 4 != 0:
            return None
        import struct

        try:
            return list(struct.unpack(f"<{len(raw) // 4}f", raw))
        except struct.error:
            return None
    if isinstance(value, str) and value.strip().startswith("["):
        try:
            parsed = json.loads(value)
            return [float(x) for x in parsed] if isinstance(parsed, list) else None
        except (ValueError, TypeError):
            return None
    return None


# BM25 pre-tokenization (plan A): CJK runs are split into bigrams (2-char
# sliding window, no dictionary, deterministic); ASCII alphanumeric runs
# keep whole words (lowercased). Write side and query side must share this
# exact function — a tokenization mismatch silently defeats the BM25 path.
_CJK_RUN_RE = re.compile(r"[\u4e00-\u9fff]+")
_ASCII_TOKEN_RE = re.compile(r"[A-Za-z0-9]+")


def build_search_text(text: str) -> str:
    """Turn raw text into a space-separated retrieval token string (written
    to search_text / used to build BM25 queries).

    Pure Python and shared by both backends: PG (tsvector 'simple') and
    SQLite (FTS5 shadow table) tokenize identically by construction, each
    engine is self-contained, and no cross-engine consistency issue exists.

    Args:
        text: raw text (summary body / recall query).

    Returns:
        Space-separated token string (CJK bigrams + ASCII words; empty text
        yields an empty string).
    """
    tokens: list[str] = []
    for run in _CJK_RUN_RE.findall(text or ""):
        tokens.extend(run[i : i + 2] for i in range(len(run) - 1))
    tokens.extend(t.lower() for t in _ASCII_TOKEN_RE.findall(text or ""))
    return " ".join(tokens)


def _rrf_fuse(
    vec_rows: list[dict], bm25_rows: list[dict], rrf_k: int
) -> list[dict]:
    """Fuse the vector/BM25 candidate lists with RRF: score = Σ 1/(k + rank),
    merged and deduplicated by id.

    Both legs SELECT the same columns (same where), so rows sharing an id
    have equal fields and the first one wins; the fused score is stored in
    the "rrf" field and rows are returned in its descending order (rerank
    overrides it with absolute scores when enabled — this order mainly
    decides the candidate pool composition and the final order when rerank
    degrades to disabled).

    Args:
        vec_rows: vector-leg candidates (cosine order).
        bm25_rows: BM25-leg candidates (ts_rank order).
        rrf_k: RRF constant (larger = the two rankings weigh more evenly).

    Returns:
        Fused row list (with rrf field, descending).
    """
    merged: dict[int, dict] = {}
    for rank, row in enumerate(vec_rows):
        entry = merged.setdefault(row["id"], {**row, "rrf": 0.0})
        entry["rrf"] += 1.0 / (rrf_k + rank)
    for rank, row in enumerate(bm25_rows):
        entry = merged.setdefault(row["id"], {**row, "rrf": 0.0})
        entry["rrf"] += 1.0 / (rrf_k + rank)
    return sorted(merged.values(), key=lambda r: r["rrf"], reverse=True)


def _like_contains_pattern(keyword: str) -> str:
    """Keyword -> LIKE contains pattern (escape % _ \\ then wrap in %,
    semantics of substring containment).

    Same semantics as webui_store._ilike; kept as its own function here so
    the db layer does not depend backwards on the web layer.
    """
    escaped = (
        keyword.replace("\\", "\\\\").replace("%", "\\%").replace("_", "\\_")
    )
    return f"%{escaped}%"


def _ensure_tz(dt: datetime) -> datetime:
    """Attach the local timezone to naive datetimes (asyncpg interprets
    naive inputs to timestamptz as UTC, so passing them through directly
    shifts times by 8 hours; the "later wins" ordering of occurred_at is
    insensitive to a consistent offset, but the tz fix keeps decay windows
    correct when compared against now()).

    Args:
        dt: time to normalize.

    Returns:
        tz-aware datetime (naive inputs interpreted in the local timezone).
    """
    if dt.tzinfo is None:
        return dt.astimezone()
    return dt


# ---------------------------------------------------------------------------
#  SQLite time boundary (fixed TEXT format, lexicographic = chronological)
# ---------------------------------------------------------------------------

_SQLITE_TS_FORMAT = "%Y-%m-%dT%H:%M:%S.%f"  # always 6 fractional digits (strftime %f)


def sqlite_format_ts(dt: datetime | None) -> str | None:
    """datetime -> SQLite TEXT (UTC, always 6 fractional digits + ``Z``);
    None passes through unchanged.

    Naive inputs are interpreted in the local timezone (same policy as
    _ensure_tz) then converted to UTC so the timeline matches the write
    path; aware inputs convert to UTC directly.

    Args:
        dt: time to format; None (nullable column) passes through.

    Returns:
        "YYYY-MM-DDTHH:MM:SS.ffffffZ", or None.
    """
    if dt is None:
        return None
    if dt.tzinfo is None:
        dt = dt.astimezone()
    return dt.astimezone(timezone.utc).strftime(_SQLITE_TS_FORMAT) + "Z"


def sqlite_parse_ts(value) -> datetime | None:
    """SQLite TEXT -> aware datetime (UTC). None/empty pass through.

    Args:
        value: column value (expected to be sqlite_format_ts output;
            malformed values map to None — callers handle them on the same
            path as PG-side NULLs).

    Returns:
        tz-aware datetime (UTC), or None.
    """
    if value is None or value == "":
        return None
    if isinstance(value, datetime):
        return value if value.tzinfo else value.astimezone()
    text = str(value)
    try:
        return datetime.strptime(text, _SQLITE_TS_FORMAT + "Z").replace(
            tzinfo=timezone.utc
        )
    except ValueError:
        pass
    # Defensive: non-canonical values (hand edits / older tools) fall back
    # to ISO parsing
    try:
        dt = datetime.fromisoformat(text.replace("Z", "+00:00"))
        return dt if dt.tzinfo else dt.astimezone()
    except ValueError:
        return None


class _Params:
    """asyncpg numbered-placeholder collector: add appends a param value
    and returns its $n placeholder.

    The SQLite backend reuses the same collector (the sqlite connection
    proxy translates $n to ? and reorders params by first appearance, see
    _sqlite_pool).
    """

    def __init__(self, first) -> None:
        """Initialize with the first param (usually the query vector text)."""
        self.values = [first]

    def add(self, value) -> str:
        """Append a param and return its placeholder ($n, n counting from 1
        in append order)."""
        self.values.append(value)
        return f"${len(self.values)}"


class MemoryBackend(ABC):
    """Unified interface for both backends (§3): postgres.MemoryDatabase
    and sqlite.SQLiteMemoryDatabase each implement it; layers above depend
    only on this interface and `.pool`.

    `.pool` compatibility attribute: returns an asyncpg.Pool on the PG side;
    on the SQLite side a _SingleConnPool shim (acquire()/execute/fetch/
    fetchrow/fetchval/executemany surface identical to an asyncpg pool, so
    call sites stay unchanged).
    """

    @property
    @abstractmethod
    def pool(self):
        """Connection pool / pool shim (raises RuntimeError before connect)."""

    @abstractmethod
    async def connect(self) -> None:
        """Connect and validate (idempotent)."""

    @abstractmethod
    async def close(self) -> None:
        """Close the connection (idempotent; exceptions are logged, not raised)."""

    @abstractmethod
    async def apply_migrations(self, migrations_dir) -> list[int]:
        """Apply pending migration scripts in version order."""

    @abstractmethod
    async def insert_chat_summary(self, **kwargs) -> None:
        """Insert a chat summary (document_id idempotent)."""

    @abstractmethod
    async def insert_persona_fact_raw(self, **kwargs) -> None:
        """Insert a raw persona fact (document_id idempotent)."""

    @abstractmethod
    async def search_chat_summaries(self, **kwargs) -> list[dict]:
        """Vector(+BM25) search over the summary table."""

    @abstractmethod
    async def fetch_recent_session_participants(self, **kwargs) -> list[dict]:
        """Participants of the session's latest N summary batches (recall widening)."""

    @abstractmethod
    async def fetch_recent_summary_scores(self, **kwargs) -> list[dict]:
        """Cosine of the session's latest N summaries against a vector (write-side near-dup check)."""

    @abstractmethod
    async def fetch_missing_embeddings(self, table: str, limit: int) -> list[tuple]:
        """Scan rows with embedding IS NULL in the given table."""

    @abstractmethod
    async def update_embedding(self, table: str, row_id: int, embedding: list) -> None:
        """Backfill one row's embedding."""

    @abstractmethod
    async def fetch_unsummarized_summaries(self, limit: int) -> list[dict]:
        """Fetch a batch of un-encoded fallback-text rows (encode catch-up pass)."""

    @abstractmethod
    async def update_chat_summary_encoded(
        self, row_id: int, content: str, embedding
    ) -> None:
        """Write back a completed encode catch-up."""

    @abstractmethod
    async def fetch_missing_search_text(self, limit: int) -> list[tuple]:
        """Scan summary rows with search_text IS NULL."""

    @abstractmethod
    async def update_search_text(self, row_id: int, search_text: str) -> None:
        """Backfill one row's search_text."""

    @abstractmethod
    async def fetch_recent_rollout_summaries(self, **kwargs) -> list[dict]:
        """Latest N session-summary batches that rolled out of the host window (rolling restore)."""

    @abstractmethod
    async def fetch_pending_facts(self, limit: int) -> list[dict]:
        """Fetch a batch of unprocessed raw facts (merge-agent normalization pass)."""

    @abstractmethod
    async def search_cluster_candidates(self, **kwargs) -> list[dict]:
        """Search cluster candidates with matching ownership (tombstones included)."""

    @abstractmethod
    async def apply_fact_merge(self, **kwargs) -> dict:
        """Dispose of one raw fact in a single transaction (join/create/replace cluster) and flip extracted_flag."""

    @abstractmethod
    async def decay_pass(self, **kwargs) -> dict:
        """Decay pass (single transaction)."""

    @abstractmethod
    async def promote_pass(self, **kwargs) -> int:
        """Promotion pass."""

    @abstractmethod
    async def get_kv(self, key: str):
        """Read an internal plugin kv value."""

    @abstractmethod
    async def set_kv(self, key: str, value: str) -> None:
        """Write an internal plugin kv value (upsert)."""

    @abstractmethod
    async def alias_upsert(self, rows: list[dict]) -> None:
        """Batch upsert of alias rows."""

    @abstractmethod
    async def alias_fetch_all(self) -> list[dict]:
        """All alias rows (full in-memory view reload)."""

    @abstractmethod
    async def fetch_alias_names_by_owner(self, owners=None) -> dict:
        """(platform, uid) -> latest non-placeholder alias."""

    @abstractmethod
    async def upsert_entity_edge(self, rows: list[dict], **kwargs) -> None:
        """Batch zero-LLM merge of edge rows (structural-key upsert)."""

    @abstractmethod
    async def fetch_active_edges(self, node_keys: list, bot_keys=object) -> list[dict]:
        """Injection candidate edges (active with an endpoint in the node set)."""

    @abstractmethod
    async def fetch_edges_for_audit(self, after_id: int, limit: int) -> list[dict]:
        """Edges to submit to the semantic audit pass (id-watermark incremental)."""

    @abstractmethod
    async def supersede_edges(self, edge_ids: list, **kwargs) -> int:
        """Batch-mark edges superseded (idempotent; returns affected rows)."""

    @abstractmethod
    async def supersede_backfill_edges_of(self, conn, cluster_ids: list) -> int:
        """Tombstone propagation to backfill-source edges (caller owns the transaction)."""

    @abstractmethod
    async def relation_integrity_report(self, **kwargs) -> dict:
        """First-layer structural self-check (zero-LLM rule sentinels)."""

    @abstractmethod
    async def fetch_confirmed_relation_sources(
        self, after_id: int = 0, limit=None
    ) -> list[dict]:
        """Confirmed fact clusters (input for legacy relation backfill)."""

    @abstractmethod
    async def count_confirmed_relation_sources(
        self, after_id: int = 0, exclude_user_id: str = ""
    ) -> int:
        """Total confirmed backfill-source clusters."""

    @abstractmethod
    async def fetch_confirmed_relation_sources_by_ids(
        self, cluster_ids: list
    ) -> list[dict]:
        """Fetch confirmed clusters by exact ids (revived-cluster backfill todo consumption)."""

    @abstractmethod
    async def fetch_alias_directory(self) -> list[tuple]:
        """Full alias table (name -> (platform, uid) directory)."""

    @abstractmethod
    async def fetch_profile_sections(
        self, platform: str, user_id: str, per_section_limit: int
    ) -> dict:
        """Per-section profile rows (data source for the first five sections)."""

    @abstractmethod
    async def fetch_uncertain_statements(
        self, platform: str, user_id: str, limit: int
    ) -> list[str]:
        """Data source for the uncertain-info section."""

    @abstractmethod
    async def fetch_profile_sections_multi(
        self, platforms: list, user_ids: list, per_section_limit: int
    ) -> dict:
        """Per-section profile rows, multi-account keys merged."""

    @abstractmethod
    async def fetch_uncertain_statements_multi(
        self, platforms: list, user_ids: list, limit: int
    ) -> list[str]:
        """Uncertain-info rows, multi-account keys merged."""

    @abstractmethod
    async def fetch_latest_display_name_multi(
        self, platforms: list, user_ids: list
    ):
        """Most recently registered display name across multi-account keys."""

    @abstractmethod
    async def fetch_latest_display_name(self, platform: str, user_id: str):
        """Display name registered at the user's latest fact extraction."""

    @abstractmethod
    async def delete_chat_summary(self, document_id: str, **kwargs) -> bool:
        """Delete a summary memory by idempotency key (memory_remove maintenance tool)."""
