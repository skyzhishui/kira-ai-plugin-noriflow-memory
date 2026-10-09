"""Injection time-label (_relative_time_label) layered-precision and mode tests.

Aligned with nori-side PR#7 (recall_time_label_mode: relative/absolute/both):
fixed clock + fixed timezone (Asia/Shanghai), covering all three modes, absolute-part precision layering
(within 7 days carries time-of-day / older goes date-only / cross-year keeps the year / cross-year recent items keep the year),
full-timestamp guard for future timestamps, naive/UTC timezone conversion, the Literal assembly-time whitelist,
and the runtime fallback for unknown modes.

Run (plugin dir):
    python tests/test_time_label.py
"""

from __future__ import annotations

import asyncio
import sys
from datetime import datetime, timedelta, timezone as dt_timezone
from pathlib import Path
from zoneinfo import ZoneInfo

sys.path.insert(0, str(Path(__file__).resolve().parent))

from _harness import load_module  # noqa: E402

_config = load_module("config")
_kernel = load_module("memory_kernel")
LocalMemoryConfig = _config.LocalMemoryConfig
LocalMemoryKernel = _kernel.LocalMemoryKernel

_NOW = datetime(2026, 10, 1, 22, 16, tzinfo=ZoneInfo("Asia/Shanghai"))


class _FakeBreaker:
    def peek_available(self) -> bool:  # noqa: D102
        return True

    async def record_success(self) -> None:  # noqa: D102
        pass


def _make_kernel(**cfg_over) -> LocalMemoryKernel:
    config = LocalMemoryConfig(
        dsn="postgresql://stub", timezone="Asia/Shanghai", **cfg_over
    )
    kernel = LocalMemoryKernel(
        db=object(),
        embedding_service=object(),
        circuit_breaker=_FakeBreaker(),
        config=config,
        bot_id="bot001",
    )
    kernel._now = staticmethod(lambda: _NOW)
    return kernel


def test_relative_mode_unchanged() -> None:
    """relative-mode regression: output identical to the old version, character for character."""
    kernel = _make_kernel(recall_time_label_mode="relative")
    cases = [
        (_NOW - timedelta(hours=2), "今天"),
        (_NOW - timedelta(days=1), "昨天"),
        (_NOW - timedelta(days=3), "3天前"),
        (_NOW - timedelta(days=14), "约2周前"),
        (_NOW - timedelta(days=45), "约1个月前"),
        (_NOW.replace(year=2025), "约1年前"),
    ]
    for ts, expect in cases:
        assert kernel._relative_time_label(ts) == expect, ts


def test_both_mode_layered_precision() -> None:
    """both mode: within 7 days carries time-of-day, older goes date-only, cross-year keeps the year."""
    kernel = _make_kernel(recall_time_label_mode="both")
    d3 = _NOW - timedelta(days=3)
    assert kernel._relative_time_label(d3) == "3天前 · 9月28日 22:16"
    assert kernel._relative_time_label(_NOW - timedelta(hours=2)) == (
        "今天 · 10月1日 20:16"
    )
    d10 = _NOW - timedelta(days=10)
    assert kernel._relative_time_label(d10) == "约1周前 · 9月21日"
    d45 = _NOW - timedelta(days=45)
    assert kernel._relative_time_label(d45) == "约1个月前 · 8月17日"
    year_ago = _NOW.replace(year=2025)
    assert kernel._relative_time_label(year_ago) == "约1年前 · 2025年10月1日"


def test_absolute_mode_no_relative() -> None:
    """absolute mode: only the absolute anchor."""
    kernel = _make_kernel(recall_time_label_mode="absolute")
    d3 = _NOW - timedelta(days=3)
    assert kernel._relative_time_label(d3) == "9月28日 22:16"
    assert kernel._relative_time_label(_NOW) == "10月1日 22:16"


def test_none_ts_and_mode_switch() -> None:
    """ts=None always yields an empty string; dynamic mode switching takes effect immediately (config hot-reload path)."""
    kernel = _make_kernel(recall_time_label_mode="relative")
    assert kernel._relative_time_label(None) == ""
    ts = _NOW - timedelta(days=3)
    assert kernel._relative_time_label(ts) == "3天前"
    kernel.config.recall_time_label_mode = "both"
    assert kernel._relative_time_label(ts) == "3天前 · 9月28日 22:16"


def test_naive_ts_treated_as_local() -> None:
    """Naive timestamps are treated as local time in the configured timezone (same convention as kernel._to_local).

    The old assertion pinned astimezone's fallback semantics (naive interpreted in the server timezone) — when the
    configured timezone differs from the server's, date and "today/yesterday" determinations drift, and it contradicts
    the naive convention shared by kernel._to_local and _edge_time_qualifier. The label layer now pins the configured
    timezone before converting; the assertion checks naive input equals an aware input explicitly stamped with the
    configured timezone — it does not depend on the running machine's system timezone and holds on UTC runners.
    """
    kernel = _make_kernel(recall_time_label_mode="both")
    naive = datetime(2026, 9, 28, 22, 16)
    equivalent_aware = naive.replace(tzinfo=ZoneInfo("Asia/Shanghai"))
    assert kernel._relative_time_label(naive) == (
        kernel._relative_time_label(equivalent_aware)
    )


def test_utc_ts_converts_to_local() -> None:
    """Aware UTC timestamps convert to the local timezone before labeling (cross-day boundaries use the local date)."""
    kernel = _make_kernel(recall_time_label_mode="both")
    utc = datetime(2026, 9, 28, 22, 16, tzinfo=dt_timezone.utc)
    # UTC 22:16 = 上海 9月29日 06:16 → days=2
    assert kernel._relative_time_label(utc) == "2天前 · 9月29日 06:16"


def test_cross_year_near_past_keeps_year() -> None:
    """Cross-year recent items (days<7 and crossing the year): the absolute part keeps the year, not dropped by the time-of-day short-circuit."""
    kernel = _make_kernel(recall_time_label_mode="both")
    kernel._now = staticmethod(
        lambda: datetime(2027, 1, 1, 12, 0, tzinfo=ZoneInfo("Asia/Shanghai"))
    )
    ts = datetime(2026, 12, 29, 15, 30, tzinfo=ZoneInfo("Asia/Shanghai"))
    assert kernel._relative_time_label(ts) == "3天前 · 2026年12月29日 15:30"


def test_future_ts_no_label() -> None:
    """Future timestamps (clock skew / batch pre-set): empty string, no label, consistent across all modes.

    Comparison uses the full timestamp: future instants within the same day (not crossing midnight) are intercepted too.
    """
    kernel = _make_kernel(recall_time_label_mode="both")
    assert kernel._relative_time_label(_NOW + timedelta(days=2)) == ""
    # _NOW=22:16，+1h=23:16 仍是同一天——只比日期差会漏掉这种未来时刻
    assert kernel._relative_time_label(_NOW + timedelta(hours=1)) == ""
    kernel.config.recall_time_label_mode = "absolute"
    assert kernel._relative_time_label(_NOW + timedelta(hours=3)) == ""


def test_invalid_mode_rejected_at_config() -> None:
    """Invalid modes are rejected at assembly time by the Literal whitelist, no longer silently degrading."""
    import pydantic

    # 断言收窄到具体错误位/类型：dsn 等字段均有默认值（零参构造成功），
    # 当前唯一错误即 mode 的 literal_error；宽泛 except ValidationError
    # 在未来新增必填字段时会误把它们当通过依据（假阳性）
    try:
        LocalMemoryConfig(recall_time_label_mode="hmm")
    except pydantic.ValidationError as exc:
        errors = exc.errors()
        assert ("recall_time_label_mode",) in [e["loc"] for e in errors]
        assert "literal_error" in [e["type"] for e in errors]
    else:
        raise AssertionError("非法 mode 应被 Literal 拒绝")


def test_unknown_mode_warns_falls_back_relative() -> None:
    """Bypassing pydantic and assigning an unknown mode directly (hot-switch path): falls back to relative without blowing up."""
    kernel = _make_kernel(recall_time_label_mode="relative")
    ts = _NOW - timedelta(days=3)
    kernel.config.recall_time_label_mode = "weird"  # type: ignore[assignment]
    assert kernel._relative_time_label(ts) == "3天前"


def test_resolve_local_tz_chain() -> None:
    """Timezone resolution chain (sole entry for persona/memory injection): config name > host provider >
    server local; invalid names and missing host fall back level by level."""
    tl = load_module("time_labels")
    utc = ZoneInfo("UTC")
    assert tl.resolve_local_tz("Asia/Tokyo", lambda: utc) == ZoneInfo("Asia/Tokyo")
    assert tl.resolve_local_tz("Not/AZone", lambda: utc) == utc
    assert tl.resolve_local_tz("", lambda: utc) == utc
    for tz in (tl.resolve_local_tz(""), tl.resolve_local_tz("", lambda: None)):
        assert hasattr(tz, "utcoffset")


async def main() -> None:
    test_relative_mode_unchanged()
    test_both_mode_layered_precision()
    test_absolute_mode_no_relative()
    test_none_ts_and_mode_switch()
    test_naive_ts_treated_as_local()
    test_utc_ts_converts_to_local()
    test_cross_year_near_past_keeps_year()
    test_future_ts_no_label()
    test_invalid_mode_rejected_at_config()
    test_unknown_mode_warns_falls_back_relative()
    test_resolve_local_tz_chain()
    print("ALL PASS")


if __name__ == "__main__":
    sys.exit(asyncio.run(main()))
