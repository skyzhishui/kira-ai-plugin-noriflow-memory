"""Circuit breaker for local memory store (PostgreSQL) calls.

Implements the state machine: CLOSED -> OPEN -> HALF_OPEN -> CLOSED.
After consecutive failures reach the threshold it enters the tripped state;
while tripped, all DB operations are rejected (recall returns empty; the
retain path raises MemoryDBUnavailable, deferred-replayed by the plugin
pending queue, neither of which blocks the main flow). When the trip expires
one probe call is allowed (HALF_OPEN); success resets the breaker, failure
keeps it tripped.

Shares the same parameters and semantics as the HindsightCircuitBreaker of
the hindsight plugin (aligned with the existing degradation behavior), with
the covered object switched from an HTTP service to a PG connection pool.
"""

from __future__ import annotations

import asyncio
import time
from typing import TYPE_CHECKING, Literal

if TYPE_CHECKING:
    from .config import LocalMemoryConfig


class MemoryDBCircuitBreaker:
    """Circuit breaker for memory store calls.

    After failure_threshold consecutive failures, enters the tripped state
    for recovery_seconds seconds. While tripped, all DB operations are
    rejected without actually establishing a connection. When the trip
    expires, one probe call is allowed; success resets it, failure keeps it
    tripped.

    When config is provided, threshold/recovery values are read from that
    instance at runtime (hot-effective after saving config on the
    maintenance page); constructor parameters serve as the fallback when
    config is absent (testing / standalone construction scenarios).
    """

    def __init__(
        self,
        failure_threshold: int = 5,
        recovery_seconds: float = 60.0,
        config: "LocalMemoryConfig | None" = None,
    ) -> None:
        """Initialize the circuit breaker.

        Args:
            failure_threshold: Consecutive failure threshold (trips when reached).
            recovery_seconds: Trip recovery wait duration (seconds).
            config: Plugin runtime config (optional): when provided, its
                failure_threshold/recovery_seconds are read per call at runtime.
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
        """Return whether a request may be sent now (locked, so HALF_OPEN allows only one probe).

        CLOSED state: requests allowed.
        OPEN state: check whether recovery_seconds has elapsed; if expiry has
        passed, switch to HALF_OPEN and allow one probe; otherwise reject.
        HALF_OPEN state: only one probe request is allowed, the rest rejected
        (to prevent concurrent requests from saturating a recovering service).

        Returns:
            bool: True allows the request, False means rejected while tripped.
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
        """Non-consuming probe: whether new requests are allowed now (no state transition, no probe slot consumed).

        Lets callers decide before an expensive pre-step (such as the retain
        encoding LLM call): when the breaker explicitly rejects, skip the
        pre-step and take the degraded path directly, avoiding wasted
        LLM/embedding calls. Difference from is_available: it does not
        trigger the OPEN -> HALF_OPEN transition nor set probe_inflight, so
        the probe slot is still consumed by the subsequent real DB operation
        (is_available), and a "query claims the slot" situation that
        permanently stalls probing cannot occur.

        Returns:
            bool: True means currently allowed (CLOSED, or OPEN expired /
            HALF_OPEN idle with a probe opportunity coming); False means
            explicitly rejected.
        """
        async with self._lock:
            if self._state == "CLOSED":
                return True
            if self._state == "HALF_OPEN":
                return not self._probe_inflight
            return time.time() >= self._trip_until

    async def record_success(self) -> None:
        """Record a successful call, reset the failure count, and return to CLOSED."""
        async with self._lock:
            self._failure_count = 0
            self._state = "CLOSED"
            self._probe_inflight = False

    async def record_failure(self) -> None:
        """Record a failed call and increment the failure count.

        If the threshold is reached, enter the OPEN state and record the trip expiry time.
        """
        async with self._lock:
            self._failure_count += 1
            self._probe_inflight = False
            if self._failure_count >= self._failure_threshold:
                self._state = "OPEN"
                self._trip_until = time.time() + self._recovery_seconds
