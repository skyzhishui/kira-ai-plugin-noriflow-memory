"""Storage access layer with two interchangeable backends (PostgreSQL/SQLite).

Package layout (docs/plans/noriflow-dual-storage-backend-plan.md §3):
- base.py       MemoryBackend interface contract + pure helpers shared by
                both backends;
- postgres.py   PostgreSQL backend (the former MemoryDatabase; SQL and
                behavior byte-identical to the pre-split module);
- sqlite.py     SQLite backend (SQLiteMemoryDatabase, same-signature
                reimplementation);
- factory.py    create_backend / resolve_backend_name assembly factory.

This __init__ re-exports every public name of the former db.py so callers
(main / kernel / merge_agent / persona_service / relation_backfill /
vector_ops / webui_store) and tests keep their ``from ..db import X``
imports unchanged.
"""

from .base import (
    CHAT_SUMMARY_TABLE,
    FACT_CLUSTER_TABLE,
    FACT_RAW_TABLE,
    MemoryBackend,
    _ensure_tz,
    _like_contains_pattern,
    _rrf_fuse,
    build_search_text,
    fact_document_id,
    parse_vector,
    sqlite_format_ts,
    sqlite_parse_ts,
    vector_literal,
)
from .factory import create_backend, resolve_backend_name
from .postgres import MemoryDatabase, supersede_backfill_edges_of

__all__ = [
    "CHAT_SUMMARY_TABLE",
    "FACT_CLUSTER_TABLE",
    "FACT_RAW_TABLE",
    "MemoryBackend",
    "MemoryDatabase",
    "SQLiteMemoryDatabase",
    "_ensure_tz",
    "_like_contains_pattern",
    "_rrf_fuse",
    "build_search_text",
    "create_backend",
    "fact_document_id",
    "parse_vector",
    "resolve_backend_name",
    "sqlite_format_ts",
    "sqlite_parse_ts",
    "supersede_backfill_edges_of",
    "vector_literal",
]


def __getattr__(name):
    # Lazy export of the SQLite backend: SQLiteMemoryDatabase is only needed
    # when the sqlite backend is selected, so pg-only deployments never load
    # aiosqlite (optional dependency, installed alongside asyncpg only for
    # sqlite-backend deployments).
    if name == "SQLiteMemoryDatabase":
        from .sqlite import SQLiteMemoryDatabase

        return SQLiteMemoryDatabase
    raise AttributeError(f"module {__name__!r} has no attribute {name!r}")
