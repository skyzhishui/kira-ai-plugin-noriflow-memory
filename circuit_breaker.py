"""本地记忆库（PostgreSQL）调用熔断器。

实现状态机：CLOSED -> OPEN -> HALF_OPEN -> CLOSED
连续失败达到阈值后进入熔断状态，熔断期间拒绝所有 DB 操作（recall 返回空；
retain 路径上抛 MemoryDBUnavailable，由插件 pending 队列延迟重放——均不
阻断主流程）。熔断到期后允许一次探测调用（HALF_OPEN），
成功则重置，失败则继续熔断。

与 hindsight 插件的 HindsightCircuitBreaker 同参数同语义（对齐现有降级行为），
覆盖对象从 HTTP 服务换成 PG 连接池。
"""

from __future__ import annotations

import asyncio
import time
from typing import TYPE_CHECKING, Literal

if TYPE_CHECKING:
    from .config import LocalMemoryConfig


class MemoryDBCircuitBreaker:
    """记忆库调用熔断器。

    连续 failure_threshold 次失败后，进入熔断状态（recovery_seconds 秒）。
    熔断期间所有 DB 操作直接拒绝，不实际建连。
    熔断到期后允许一次探测调用，成功则重置，失败则继续熔断。

    config 提供时阈值/恢复时长运行时读该实例（维护页保存配置后热生效），
    构造参数作为 config 缺省时的回退（测试/独立构造场景）。
    """

    def __init__(
        self,
        failure_threshold: int = 5,
        recovery_seconds: float = 60.0,
        config: "LocalMemoryConfig | None" = None,
    ) -> None:
        """初始化熔断器。

        Args:
            failure_threshold: 连续失败阈值（达到即熔断）。
            recovery_seconds: 熔断恢复等待时长（秒）。
            config: 插件运行时配置（可选）：提供时运行时按次读其
                failure_threshold/recovery_seconds。
        """
        self._config = config
        self._static_failure_threshold = failure_threshold
        self._static_recovery_seconds = recovery_seconds
        self._failure_count = 0
        self._state: Literal["CLOSED", "OPEN", "HALF_OPEN"] = "CLOSED"
        self._trip_until = 0.0
        self._probe_inflight = False  # HALF_OPEN 探测请求是否在进行中
        self._lock = asyncio.Lock()

    @property
    def _failure_threshold(self) -> int:
        if self._config is not None:
            return self._config.failure_threshold
        return self._static_failure_threshold

    @property
    def _recovery_seconds(self) -> float:
        if self._config is not None:
            return self._config.recovery_seconds
        return self._static_recovery_seconds

    async def is_available(self) -> bool:
        """判断当前是否允许发起请求（加锁，保证 HALF_OPEN 只允许一次探测）。

        CLOSED 状态：允许请求。
        OPEN 状态：检查是否已过 recovery_seconds，若已过期则转为 HALF_OPEN 并允许一次探测；否则拒绝。
        HALF_OPEN 状态：仅允许一次探测请求，其余拒绝（防并发打满恢复中的服务）。

        Returns:
            bool: True 表示允许请求，False 表示熔断中拒绝。
        """
        async with self._lock:
            if self._state == "CLOSED":
                return True
            if self._state == "HALF_OPEN":
                if self._probe_inflight:
                    return False
                self._probe_inflight = True
                return True
            # OPEN 状态
            if time.time() >= self._trip_until:
                # 熔断到期，允许一次探测（转为 HALF_OPEN）
                self._state = "HALF_OPEN"
                self._probe_inflight = True
                return True
            return False

    async def peek_available(self) -> bool:
        """非消费式探测：当前是否允许新请求（不迁移状态、不占用探测名额）。

        供调用方在昂贵前置操作（如 retain 编码 LLM 调用）前判断：熔断
        明确拒绝时跳过前置操作直接走降级路径，避免白烧 LLM/embedding。
        与 is_available 的差异：不触发 OPEN -> HALF_OPEN 迁移、不置
        probe_inflight——探测名额仍由后续真正的 DB 操作（is_available）
        消费，不会出现「查询即占坑」导致探测永久卡死。

        Returns:
            bool: True 表示当前放行（CLOSED，或 OPEN 到期/HALF_OPEN 空闲
            即将有探测机会）；False 表示明确拒绝中。
        """
        async with self._lock:
            if self._state == "CLOSED":
                return True
            if self._state == "HALF_OPEN":
                return not self._probe_inflight
            return time.time() >= self._trip_until

    async def record_success(self) -> None:
        """记录一次成功调用，重置失败计数，状态回到 CLOSED。"""
        async with self._lock:
            self._failure_count = 0
            self._state = "CLOSED"
            self._probe_inflight = False

    async def record_failure(self) -> None:
        """记录一次失败调用，递增失败计数。

        若达到阈值则进入 OPEN 状态，并记录熔断到期时间。
        """
        async with self._lock:
            self._failure_count += 1
            self._probe_inflight = False
            if self._failure_count >= self._failure_threshold:
                self._state = "OPEN"
                self._trip_until = time.time() + self._recovery_seconds
