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

# Unknown mode values warn only once per value (deduped by value):
# labeling runs per injected memory/profile entry, so per-entry warnings
# would flood the log
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


def relative_time_precise(occurred_at: datetime | None, now: datetime) -> str:
    """Fine-grained relative label (N seconds/minutes/hours/days ago) for
    near-horizon events (day-granularity relative_time_part is too coarse
    there); None or future timestamps return an empty string."""
    if occurred_at is None:
        return ""
    delta = (now - occurred_at).total_seconds()
    if delta < 0:
        return ""
    seconds = int(delta)
    if seconds < 60:
        return f"{seconds}秒前"
    minutes, seconds = divmod(seconds, 60)
    if minutes < 60:
        return f"{minutes}分前" if seconds == 0 else f"{minutes}分{seconds}秒前"
    hours, minutes = divmod(minutes, 60)
    if hours < 24:
        return f"{hours}小时前" if minutes == 0 else f"{hours}小时{minutes}分前"
    days, hours = divmod(hours, 24)
    return f"{days}天前" if hours == 0 else f"{days}天{hours}小时前"


def absolute_time_part(local: datetime, days: int, now_local: datetime) -> str:
    """Absolute time anchor (layered precision): within 7 days include time, older only the date, cross-year includes the year."""
    # Compute the year prefix first: the days<7 recent branch can still
    # cross a year boundary (New Year's Day looking back into last
    # month), so the hour:minute early return must not drop the year
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
            # naive values are by convention local times in the configured
            # timezone (the SQLite backend / host may deliver them): a
            # direct astimezone interprets them in the server's local
            # timezone, so when the configured timezone differs the date
            # and the "today/yesterday" verdict drift -- pin local_tz on
            # before converting
            ts = ts.replace(tzinfo=local_tz)  # type: ignore[arg-type]
        local = ts.astimezone(local_tz)  # type: ignore[arg-type]
        now_local = now.astimezone(local_tz)  # type: ignore[arg-type]
    except (ValueError, OSError, OverflowError):
        return ""
    if local > now_local:
        # Future timestamps (clock skew / batch pre-dating): compare by the
        # full instant and intercept same-day future moments too -- the
        # relative anchor and the absolute date would contradict each
        # other ("today · a future moment"); prefer no label over
        # inducing wrong temporal reasoning
        return ""
    days = (now_local.date() - local.date()).days
    relative = relative_time_part(days)
    absolute = absolute_time_part(local, days, now_local)
    if mode == "absolute":
        return absolute
    if mode == "both" and absolute:
        return f"{relative} · {absolute}"
    if mode != "relative":
        # Fallback outside Literal validation: direct config assignment
        # bypasses pydantic (e.g. hot swap); unknown values are handled as
        # relative but logged, so a slip never degrades silently
        if mode not in _WARNED_MODES:
            _WARNED_MODES.add(mode)
            logger.warning("未知 recall_time_label_mode=%r，按 relative 处理", mode)
    return relative
