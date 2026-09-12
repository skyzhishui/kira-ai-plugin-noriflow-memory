"""召回评估日志（recall_log）：检索与注入两事件各落一条 JSONL。

用途：召回质量度量闭环的唯一数据面——离线脚本据此统计
query/候选/最终注入的 document_id 与得分分布，支撑阈值
（去重相似度/相关度）、近时排除窗口、扩选范围等参数的实调。

设计约束：
- 纯插件侧，开箱即走文件（默认插件 data_dir/memory_recall_log.jsonl），
  不建表不加迁移（热路径零写放大，评估期开关，默认关闭）；
- fail-open：写入失败仅记 warning，任何情况下不影响召回链路；
- 每事件一行 JSON（ensure_ascii=False 保留中文可读性），追加打开、
  写完即关（每轮 1-2 条，频次下同步 append 的句柄开销可忽略，
  换取崩溃安全的落盘语义）。

事件结构：
- search：一次 search() 的完整决策快照（query/scope/候选数/各过滤器
  丢弃数/最终保留的 id+score+occurred_at）；
- inject：一次 build_injection_text() 的注入结果（黑名单/预算截断后
  最终进入 prompt 的条目与时间标注）。
"""

from __future__ import annotations

import json
from datetime import datetime, timezone
from pathlib import Path

from core.logging_manager import get_logger

logger = get_logger("noriflow_memory.recall_log", "cyan")


class RecallLogWriter:
    """JSONL 追加写器（线程/协程安全性依赖追加写的原子性，评估场景足够）。"""

    def __init__(self, path: str | Path) -> None:
        """初始化。

        Args:
            path: 日志文件路径（父目录由构造方确保存在）。
        """
        self._path = Path(path)

    @property
    def path(self) -> Path:
        """日志文件路径（诊断用）。"""
        return self._path

    def write(self, event: dict) -> None:
        """追加一条事件（自动补时间戳；任何异常吞掉只记 warning）。

        Args:
            event: 事件字段（须含 event 类型键；datetime 值经 default=str 兜底）。
        """
        record = {"ts": datetime.now(timezone.utc).isoformat(), **event}
        try:
            line = json.dumps(record, ensure_ascii=False, default=str)
            with self._path.open("a", encoding="utf-8") as f:
                f.write(line + "\n")
        except Exception:
            logger.warning("recall_log 写入失败（fail-open，不影响召回）", exc_info=True)
