"""注入时间标注共享模块（recall 记忆块与画像档案共用同一口径）。

从 memory_kernel 的 _relative_time_label/_relative_part/_absolute_part
提取为模块级函数——画像档案（persona_service）的近期动态/待定信息栏
需要与 recall 记忆块逐字一致的时间标注（相对+绝对并列），避免两处
实现漂移导致 LLM 收到两套时距语义。

口径（与 recall 注入一致）：
- relative=今天/昨天/N天前/约N周前…（零推理时距感）；
- absolute=9月28日 14:30（跨年带年份，LLM 经当前时间锚换算）；
- both=两者并列（3天前 · 9月28日 14:30）。
- 绝对部分分层精度：7 天内带时分（近事可辨批内时序），更久只到
  日期——occurred_at 是批次落库时间而非事件真实时刻，远期精确到
  分钟会诱导跨条错误时序推理。
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
    """解析本地时区（画像/记忆注入共用的唯一入口）。

    解析链：配置名 > 宿主 provider（活读）> 服务器本地。

    Args:
        timezone_name: 配置时区名（空串跳过配置档）。
        host_tz_provider: 宿主时区活读回调（None 时跳过宿主档）。

    Returns:
        tzinfo（ZoneInfo / 宿主 tzinfo / 服务器本地 tzinfo）。
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
    """相对时距（按日粒度）：今天/昨天/N天前/约N周前/约N个月前/约N年前。"""
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
    """绝对时间锚（分层精度）：7 天内带时分，更久只到日期，跨年带年份。"""
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
    """注入时间标注（本地时区）：形态由 recall_time_label_mode 决定。

    Args:
    Args:
        ts: 记忆/事实发生时间（aware；naive 视为 local_tz 本地时间——
            SQLite 后端/宿主可能传来 naive 值；None 或将来时间戳返回空串）。
        now: 当前时间锚（aware）。
        local_tz: 本地时区（resolve_local_tz 产物）。
        mode: 标注形态（relative/absolute/both）。

    Returns:
        标注文本（空串表示不加标注）。
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
