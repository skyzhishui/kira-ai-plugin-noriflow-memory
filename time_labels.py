"""Shared injection time-labeling module (recall memory blocks and persona profiles use the same convention).

Extracted from memory_kernel's _relative_time_label/_relative_part/
_absolute_part into module-level functions: the persona profile
(persona_service) recent-updates/pending-info slots need time labels
verbatim-identical to recall memory blocks (relative+absolute side by
side), to avoid implementation drift handing the LLM two different
time-distance semantics.

Convention (consistent with recall injection):
- relative=today/yesterday/N days ago/about N weeks ago... (zero-inference time-distance feel);
- absolute=Sep 28 14:30 (year included across years; the LLM anchors via current time);
- both=both side by side (3 days ago · Sep 28 14:30).
- absolute part layered precision: within 7 days includes time (recent
  events distinguishable within the batch); older only reaches the date,
  since occurred_at is the batch-insert time, not the true event time, and
  minute precision on old events would induce wrong cross-entry temporal reasoning.
"""

from __future__ import annotations

from datetime import datetime
from zoneinfo import ZoneInfo

from core.logging_manager import get_logger

logger = get_logger("noriflow_memory.time_labels", "cyan")

# 未知 mode 按值去重只告警一次：标注按注入记忆/画像条目逐条执行，
# 逐条告警会日志洪泛
_WARNED_MODES: set[str] = set()


def resolve_local_tz(timezone_name: str, host_tz_provider=None) -> object:
    """Resolve the local timezone (the only entry point shared by persona/memory injection).

    Resolution chain: config name > host provider (live read) > server local.

    Args:
        timezone_name: Configured timezone name (empty string skips the config tier).
        host_tz_provider: Host timezone live-read callback (None skips the host tier).

    Returns:
        tzinfo (ZoneInfo / host tzinfo / server local tzinfo).
    """
    if timezone_name:
        try:
            return ZoneInfo(timezone_name)
        except Exception:
            logger.warning(
                "配置时区 %r 解析失败，回退宿主/服务器本地时区", timezone_name
            )
    if host_tz_provider is not None:
        try:
            tz = host_tz_provider()
        except Exception:
            logger.warning("宿主时区读取失败，回退服务器本地时区", exc_info=True)
            tz = None
        if tz is not None:
            return tz
    return datetime.now().astimezone().tzinfo


def relative_time_part(days: int) -> str:
    """Relative time-distance (day granularity): today/yesterday/N days ago/about N weeks ago/about N months ago/about N years ago."""
    if days <= 0:
        return "今天"
    if days == 1:
        return "昨天"
    if days < 7:
        return f"{days}天前"
    if days < 30:
        return f"约{max(days // 7, 1)}周前"
    if days < 365:
        return f"约{max(days // 30, 1)}个月前"
    return f"约{max(days // 365, 1)}年前"


def absolute_time_part(local: datetime, days: int, now_local: datetime) -> str:
    """Absolute time anchor (layered precision): within 7 days include time, older only the date, cross-year includes the year."""
    # 年份前缀先算：days<7 的近事分支也可能跨年（元旦回溯上月），
    # 不能让时分短路把年份丢掉
    year_part = f"{local.year}年" if local.year != now_local.year else ""
    if days < 7:
        return f"{year_part}{local.month}月{local.day}日 {local:%H:%M}"
    return f"{year_part}{local.month}月{local.day}日"


def memory_time_label(
    ts: datetime | None,
    *,
    now: datetime,
    local_tz: object,
    mode: str,
) -> str:
    """Injection time label (local timezone): form decided by recall_time_label_mode.

    Args:
        ts: Memory/fact occurrence time (aware; naive treated as local_tz
            local time, SQLite backend/host may deliver naive values; None
            or future timestamps return an empty string).
        now: Current time anchor (aware).
        local_tz: Local timezone (product of resolve_local_tz).
        mode: Label form (relative/absolute/both).

    Returns:
        Label text (empty string means no label).
    """
    if ts is None:
        return ""
    try:
        if ts.tzinfo is None:
            # naive 约定为配置时区的本地时间（SQLite 后端/宿主可能传来）：
            # 直接 astimezone 会按服务器本地时区解释，配置时区不一致时
            # 日期与"今天/昨天"判定漂移——转换前先钉上 local_tz
            ts = ts.replace(tzinfo=local_tz)  # type: ignore[arg-type]
        local = ts.astimezone(local_tz)  # type: ignore[arg-type]
        now_local = now.astimezone(local_tz)  # type: ignore[arg-type]
    except (ValueError, OSError, OverflowError):
        return ""
    if local > now_local:
        # 将来时间戳（时钟偏差/批次预置）：按完整时刻比较，同日内的未来
        # 时刻一并拦截——相对锚与绝对日期会自相矛盾（今天 · 未来某刻），
        # 宁可不标注，避免诱导错误时序推理
        return ""
    days = (now_local.date() - local.date()).days
    relative = relative_time_part(days)
    absolute = absolute_time_part(local, days, now_local)
    if mode == "absolute":
        return absolute
    if mode == "both" and absolute:
        return f"{relative} · {absolute}"
    if mode != "relative":
        # Literal 校验外的兜底：直赋值改 config 不经 pydantic（如热切换），
        # 未知取值按 relative 处理但落日志，不让手滑静默退化
        if mode not in _WARNED_MODES:
            _WARNED_MODES.add(mode)
            logger.warning("未知 recall_time_label_mode=%r，按 relative 处理", mode)
    return relative
