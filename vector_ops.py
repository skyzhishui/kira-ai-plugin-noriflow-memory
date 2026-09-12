"""embedding 服务：写入时批量计算（fail-open）+ 后台补算任务。

设计（见开发方案 §9.1）：
- 写入时：retain 异步链路内对 summary 单条、facts 批量各算一次，随行存储；
  计算失败不阻塞写入（行内 embedding 置 NULL，无向量的行不参与语义检索
  但保留标量检索能力）；
- 补算任务：插件后台 asyncio 周期任务，扫描两表 embedding IS NULL 的行回填；
- 服务端：复用 KiraAI provider 体系 default_embedding（OpenAI 兼容
  /v1/embeddings，维度由插件配置 embedding_dims 校验）。
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
    """向量计算服务（写入链路 fail-open 语义）。

    Attributes:
        client: KiraEmbeddingClient 实例；None 表示 embedding 不可用
            （default_embedding 未配置时），所有计算返回 None，行内向量置 NULL。
        dims: 向量维度（写入校验用）。
    """

    def __init__(
        self,
        client: Optional["KiraEmbeddingClient"],
        dims: int = 1024,
    ) -> None:
        """初始化。

        Args:
            client: embedding 客户端（可为 None，表示服务不可用）。
            dims: 向量维度（与 schema 向量列一致）。
        """
        self.client = client
        self.dims = dims

    @property
    def available(self) -> bool:
        """embedding 服务是否可用。"""
        return self.client is not None

    async def embed_one(self, text: str) -> list[float] | None:
        """单条文本向量化（fail-open：任何失败返回 None，不抛异常）。

        Args:
            text: 待编码文本（空串直接返回 None，不发起请求）。

        Returns:
            向量；服务不可用 / 请求失败 / 维度不符时返回 None。
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
        """批量文本向量化（fail-open：调用失败整批返回 None 占位）。

        单次 embeddings 请求承载整批（retain 每轮 facts 通常 0-5 条 + summary 1 条，
        批量规模可控）；请求失败时整批置 None 由补算任务兜底，绝不阻塞写入。

        Args:
            texts: 待编码文本列表（空元素返回 None 占位）。

        Returns:
            与输入等长的列表；不可用/失败的元素为 None。
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
    """embedding 补算后台任务：周期扫描两表 NULL 向量行并回填。

    生命周期归插件（on_start 启动 / on_stop 停止）；单批失败只记 warning
    顺延下周期，任务循环本身不退出。
    """

    def __init__(
        self,
        db: MemoryDatabase,
        service: EmbeddingService,
        config: LocalMemoryConfig,
    ) -> None:
        """初始化。

        Args:
            db: 记忆库访问层。
            service: embedding 服务（不可用时任务直接空转）。
            config: 补算周期/批量配置。
        """
        self._db = db
        self._service = service
        self._config = config
        self._task: asyncio.Task | None = None

    @property
    def running(self) -> bool:
        """任务是否在运行。"""
        return self._task is not None and not self._task.done()

    def start(self) -> None:
        """启动后台任务（幂等：已运行时跳过）。"""
        if self.running:
            return
        self._task = asyncio.create_task(self._run(), name="memory-embedding-backfill")
        logger.info(
            "embedding 补算任务已启动: interval=%ss batch=%d",
            self._config.backfill_interval_seconds, self._config.backfill_batch_size,
        )

    async def stop(self) -> None:
        """停止后台任务（幂等，等待当前循环退出）。"""
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
        """任务主循环：周期执行补算，异常只记录不退出。"""
        while True:
            await asyncio.sleep(self._config.backfill_interval_seconds)
            try:
                await self.run_once()
            except Exception:
                logger.warning("embedding 补算周期执行失败（顺延下周期）", exc_info=True)

    async def run_once(self) -> int:
        """执行单轮补算（search_text 分词遍 + 三张表向量补算）。

        search_text 遍在最前且不依赖 embedding 服务（纯 Python 分词 + UPDATE，
        服务不可用时也要回填——BM25 路可用性不应被向量服务状态绑架）；
        簇表排在最后：新簇建簇时从 raw 事实继承向量，先补 raw 再补簇，
        让簇的回填能覆盖「建簇时 raw 向量仍缺失」的窗口期残留（否则该簇
        永久 NULL 向量、对候选检索不可见，同义事实会重复建簇）。

        Returns:
            本轮回填成功的行数。
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
        """扫描摘要表 search_text IS NULL 的行并回填分词（存量迁移 006 数据）。

        纯 Python 分词 + UPDATE（无 API 调用），扫描批量放大 8 倍
        （默认 64→512/周期；embedding 遍受 API 吞吐约束维持原批量）。

        Returns:
            本轮回填行数。
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
