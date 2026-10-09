"""Embedding service: batch computation on write (fail-open) + background backfill task.

Design (see dev plan section 9.1):
- on write: inside the retain async path, compute once for the single
  summary and once for the facts batch, stored alongside the rows; compute
  failure does not block the write (the in-row embedding is set to NULL; a
  row without a vector skips semantic retrieval but keeps scalar search);
- backfill task: a plugin background asyncio periodic task scans rows with
  embedding IS NULL in both tables and backfills;
- server side: reuses the KiraAI provider system default_embedding (OpenAI
  compatible /v1/embeddings; dimension validated by plugin config embedding_dims).
"""

from __future__ import annotations

from core.logging_manager import get_logger

import asyncio
from typing import TYPE_CHECKING, Optional

from .config import LocalMemoryConfig
from .db import (
    CHAT_SUMMARY_TABLE,
    FACT_CLUSTER_TABLE,
    FACT_RAW_TABLE,
    MemoryDatabase,
    build_search_text,
)

if TYPE_CHECKING:
    from .clients import KiraEmbeddingClient

logger = get_logger("noriflow_memory.vector", "cyan")


class EmbeddingService:
    """Vector computation service (fail-open semantics on the write path).

    Attributes:
        client: KiraEmbeddingClient instance; None means embedding is
            unavailable (default_embedding not configured); every computation
            returns None and the in-row vector is set to NULL.
        dims: Vector dimension (for write validation).
    """

    def __init__(
        self,
        client: Optional["KiraEmbeddingClient"],
        dims: int = 1024,
    ) -> None:
        """Initialize.

        Args:
            client: Embedding client (may be None, meaning the service is unavailable).
            dims: Vector dimension (must match the schema vector column).
        """
        self.client = client
        self.dims = dims

    @property
    def available(self) -> bool:
        """Whether the embedding service is available."""
        return self.client is not None

    async def embed_one(self, text: str) -> list[float] | None:
        """Vectorize a single text (fail-open: any failure returns None, never raises).

        Args:
            text: Text to encode (empty string returns None directly without a request).

        Returns:
            Vector; None when the service is unavailable / the request fails /
            the dimension mismatches.
        """
        if self.client is None or not text.strip():
            return None
        try:
            vec = await self.client.embed(text)
        except Exception:
            logger.warning("embedding 计算失败（行内向量置 NULL，待补算）", exc_info=True)
            return None
        return self._validate(vec)

    async def embed_batch(self, texts: list[str]) -> list[Optional[list[float]]]:
        """Vectorize texts in batch (fail-open: on call failure the whole batch returns None placeholders).

        One embeddings request carries the whole batch (per retain turn the
        facts are typically 0-5 plus one summary, so the batch size is
        controllable); on request failure the whole batch is set to None and
        covered by the backfill task, never blocking the write.

        Args:
            texts: Text list to encode (empty elements return None placeholders).

        Returns:
            A list of the same length as the input; unavailable/failed elements are None.
        """
        if self.client is None:
            return [None] * len(texts)

        results: list[Optional[list[float]]] = [None] * len(texts)
        indexes = [i for i, t in enumerate(texts) if t.strip()]
        if not indexes:
            return results

        try:
            vectors = await self.client.embed_batch(
                [texts[i] for i in indexes]
            )
        except Exception:
            logger.warning(
                "embedding 批量计算失败（%d 条置 NULL，待补算）", len(indexes), exc_info=True
            )
            return results

        # 返回形态防御（同样按 fail-open 处理，对齐上方异常语义）：部分
        # OpenAI 兼容网关异常时会返回条数不齐或含 null 的列表——后处理
        # 在 try 外，越界/None 若不在此拦截会炸掉调用链（补算任务整轮
        # 停摆、retain 批次上抛），且服务端响应形态不变时每周期复现
        if not isinstance(vectors, list) or len(vectors) != len(indexes):
            logger.warning(
                "embedding 批量返回条数不符（期望 %d 实得 %s），整批置 NULL 待补算",
                len(indexes),
                len(vectors) if isinstance(vectors, list) else type(vectors).__name__,
            )
            return results
        for pos, i in enumerate(indexes):
            results[i] = self._validate(vectors[pos])
        return results

    def _validate(self, vec: list[float]) -> list[float] | None:
        """Dim check (mismatch or None -> None so the vector column never
        rejects an insert and blocks the write path).

        Non-list/tuple shapes (a misbehaving gateway response) also map to
        None — this method must never raise (fail-open contract); a
        TypeError from len() would otherwise break the embed_one /
        embed_batch callers.
        """
        if vec is None or not isinstance(vec, (list, tuple)):
            return None
        if len(vec) != self.dims:
            logger.warning(
                "embedding 维度不符（期望 %d 实得 %d），行内置 NULL",
                self.dims, len(vec),
            )
            return None
        return vec


class EmbeddingBackfillTask:
    """Background embedding backfill task: periodically scans NULL-vector rows in both tables and backfills.

    Lifecycle owned by the plugin (on_start starts it / on_stop stops it);
    a single-batch failure only logs a warning and defers to the next cycle;
    the task loop itself never exits.
    """

    def __init__(
        self,
        db: MemoryDatabase,
        service: EmbeddingService,
        config: LocalMemoryConfig,
    ) -> None:
        """Initialize.

        Args:
            db: Memory store access layer.
            service: Embedding service (the task just idles when unavailable).
            config: Backfill interval/batch-size config.
        """
        self._db = db
        self._service = service
        self._config = config
        self._task: asyncio.Task | None = None

    @property
    def running(self) -> bool:
        """Whether the task is running."""
        return self._task is not None and not self._task.done()

    def start(self) -> None:
        """Start the background task (idempotent: skipped when already running)."""
        if self.running:
            return
        self._task = asyncio.create_task(self._run(), name="memory-embedding-backfill")
        logger.info(
            "embedding 补算任务已启动: interval=%ss batch=%d",
            self._config.backfill_interval_seconds, self._config.backfill_batch_size,
        )

    async def stop(self) -> None:
        """Stop the background task (idempotent, waits for the current loop to exit)."""
        if self._task is None:
            return
        self._task.cancel()
        try:
            await self._task
        except asyncio.CancelledError:
            pass
        self._task = None
        logger.info("embedding 补算任务已停止")

    async def _run(self) -> None:
        """Task main loop: periodically runs backfill; exceptions only log, never exit."""
        while True:
            await asyncio.sleep(self._config.backfill_interval_seconds)
            try:
                await self.run_once()
            except Exception:
                logger.warning("embedding 补算周期执行失败（顺延下周期）", exc_info=True)

    async def run_once(self) -> int:
        """Run one backfill round (search_text tokenization pass + vector backfill on three tables).

        The search_text pass runs first and does not depend on the embedding
        service (pure Python tokenization + UPDATE; it must be backfilled
        even when the service is unavailable, the BM25 path's availability
        should not be held hostage by the vector service state); the cluster
        table runs last: new clusters inherit vectors from raw facts when
        created, so backfilling raw before cluster lets the cluster backfill
        cover the window where a raw vector was still missing at cluster
        creation (otherwise that cluster stays NULL forever, invisible to
        candidate retrieval, and synonymous facts would keep creating clusters).

        Returns:
            Number of rows successfully backfilled this round.
        """
        filled = 0
        try:
            filled += await self._backfill_search_text()
        except Exception:
            logger.warning("search_text 分词回填失败（顺延下周期）", exc_info=True)

        if not self._service.available:
            return filled

        for table in (CHAT_SUMMARY_TABLE, FACT_RAW_TABLE, FACT_CLUSTER_TABLE):
            rows = await self._db.fetch_missing_embeddings(
                table, self._config.backfill_batch_size)
            if not rows:
                continue
            vectors = await self._service.embed_batch([text for _, text in rows])
            table_filled = 0
            for (row_id, _), vec in zip(rows, vectors):
                if vec is None:
                    continue
                await self._db.update_embedding(table, row_id, vec)
                table_filled += 1
            filled += table_filled
            if table_filled:
                logger.info(
                    "embedding 补算: %s 扫描 %d 行，回填 %d 行",
                    table, len(rows), table_filled,
                )
        return filled

    async def _backfill_search_text(self) -> int:
        """Scan summary-table rows with search_text IS NULL and backfill their tokenization (legacy migration 006 data).

        Pure Python tokenization + UPDATE (no API calls); the scan batch is
        amplified 8x (default 64->512/cycle; the embedding pass keeps its
        original batch constrained by API throughput).

        Returns:
            Number of rows backfilled this round.
        """
        scan_limit = self._config.backfill_batch_size * 8
        rows = await self._db.fetch_missing_search_text(scan_limit)
        if not rows:
            return 0
        filled = 0
        for row_id, text in rows:
            await self._db.update_search_text(row_id, build_search_text(text or ""))
            filled += 1
        logger.info(
            "search_text 分词回填: 扫描 %d 行，回填 %d 行", len(rows), filled
        )
        return filled
