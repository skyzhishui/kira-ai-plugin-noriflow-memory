"""Single-connection SQLite pool shim exposing an asyncpg-compatible
connection surface (plan §8).

Callers above (kernel / merge_agent / webui_store / this package's
sqlite.py) only know ``pool.acquire()`` + ``conn.execute/fetch/fetchrow/
fetchval/executemany/transaction()``; this module translates that surface
onto a single aiosqlite connection:

- **Placeholders**: callers keep asyncpg-style ``$n`` (reuse/out-of-order
  included); ``_translate`` reorders them by first appearance into SQLite
  ``?`` marks;
- **Session isolation**: ``acquire()`` takes exclusive ownership of the
  whole "connection session" (reentrant within the same task), so
  uncommitted writes inside a merge-agent transaction are never visible to
  a concurrent recall (with a single connection this equals the isolation
  semantics of asyncpg callers each holding their own connection);
- **Transactions**: ``transaction()`` = ``BEGIN IMMEDIATE`` (write
  transactions grab the write lock up front, making read-modify-write
  patterns immune to lost updates) / COMMIT / ROLLBACK;
- **Status tags**: ``execute`` returns asyncpg-style command tags
  ("UPDATE 1" / "DELETE 3") — ``apply_fact_merge``'s optimistic-lock
  ``endswith(" 1")`` and the webui delete-count parsing depend on this
  shape.

Connection PRAGMAs (set by sqlite.py at connect time): journal_mode=WAL +
busy_timeout=5000 + isolation_level=None (explicit transaction control).
"""

from __future__ import annotations

import asyncio
import re
from contextlib import asynccontextmanager

import aiosqlite

from core.logging_manager import get_logger

logger = get_logger("noriflow_memory.sqlite_pool", "cyan")

_PARAM_RE = re.compile(r"\$(\d+)")


def _translate(sql: str, params: tuple) -> tuple[str, list]:
    """Rewrite $n placeholders to ? and reorder params by first appearance
    (an $n reused expands to a repeated value).

    Args:
        sql: SQL containing $n placeholders.
        params: positional params ($1 maps to params[0]).

    Returns:
        (translated SQL, reordered value list).

    Raises:
        IndexError: a placeholder number outside the params range.
    """
    order: list[int] = []

    def _repl(match: re.Match) -> str:
        n = int(match.group(1))
        if n < 1 or n > len(params):
            raise IndexError(f"占位符 ${n} 超出参数范围（共 {len(params)} 个）")
        order.append(n)
        return "?"

    new_sql = _PARAM_RE.sub(_repl, sql)
    return new_sql, [params[n - 1] for n in order]


def _status_tag(sql: str, rowcount: int) -> str:
    """Build an asyncpg-style command status tag (execute return shape)."""
    verb = sql.lstrip().split(None, 1)[0].upper() if sql.strip() else ""
    if verb == "INSERT":
        return f"INSERT 0 {rowcount}"
    return f"{verb} {rowcount}"


class _SingleConn:
    """Single-connection proxy: asyncpg surface -> aiosqlite."""

    def __init__(self, conn: aiosqlite.Connection) -> None:
        self._conn = conn
        self._in_txn = False

    async def execute(self, sql: str, *params) -> str:
        new_sql, values = _translate(sql, params)
        cur = await self._conn.execute(new_sql, values)
        return _status_tag(new_sql, cur.rowcount or 0)

    async def fetch(self, sql: str, *params) -> list:
        new_sql, values = _translate(sql, params)
        cur = await self._conn.execute(new_sql, values)
        return await cur.fetchall()

    async def fetchrow(self, sql: str, *params):
        new_sql, values = _translate(sql, params)
        cur = await self._conn.execute(new_sql, values)
        return await cur.fetchone()

    async def fetchval(self, sql: str, *params):
        new_sql, values = _translate(sql, params)
        cur = await self._conn.execute(new_sql, values)
        row = await cur.fetchone()
        return row[0] if row is not None else None

    async def executemany(self, sql: str, args_seq) -> None:
        translated = [_translate(sql, tuple(args)) for args in args_seq]
        if not translated:
            return
        sql2 = translated[0][0]
        values = [v for _, v in translated]
        await self._conn.executemany(sql2, values)

    @asynccontextmanager
    async def transaction(self):
        """BEGIN IMMEDIATE transaction (no nesting — the current pipeline never nests)."""
        if self._in_txn:
            raise RuntimeError("SQLite 后端不支持嵌套事务")
        self._in_txn = True
        try:
            await self._conn.execute("BEGIN IMMEDIATE")
        except Exception:
            self._in_txn = False
            raise
        try:
            yield
        except BaseException:
            try:
                await self._conn.rollback()
            except Exception:
                logger.warning("SQLite 事务回滚失败", exc_info=True)
            raise
        else:
            await self._conn.commit()
        finally:
            self._in_txn = False

    async def close(self) -> None:
        await self._conn.close()


class _SingleConnPool:
    """Single-connection pool: acquire() owns the session exclusively
    (reentrant within one task); close() is idempotent."""

    def __init__(self, conn: aiosqlite.Connection) -> None:
        self._conn = _SingleConn(conn)
        self._lock = asyncio.Lock()
        self._owner: asyncio.Task | None = None
        self._depth = 0
        self._closed = False

    @property
    def conn(self) -> _SingleConn:
        return self._conn

    @asynccontextmanager
    async def acquire(self):
        if self._closed:
            raise RuntimeError("SQLite 连接已关闭")
        task = asyncio.current_task()
        if self._owner is task:
            # Same-task reentrancy (nested acquire is legal under PG pool semantics)
            self._depth += 1
            try:
                yield self._conn
            finally:
                self._depth -= 1
            return
        async with self._lock:
            self._owner = task
            self._depth = 1
            try:
                yield self._conn
            finally:
                self._owner = None
                self._depth = 0

    async def close(self) -> None:
        if self._closed:
            return
        self._closed = True
        async with self._lock:
            try:
                await self._conn.close()
            except Exception:
                logger.warning("关闭 SQLite 连接时发生异常", exc_info=True)
