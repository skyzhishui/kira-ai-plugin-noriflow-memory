"""Recall evaluation log (recall_log): one JSONL entry per search and injection event.

Purpose: the only data surface closing the recall-quality measurement loop;
offline scripts aggregate document_id and score distributions of
query/candidates/final injection from it, supporting live tuning of the
thresholds (dedup similarity / relevance), recent-exclusion window, and
expansion scope.

Design constraints:
- purely plugin-side, working out of the box against a file (default plugin
  data_dir/memory_recall_log.jsonl), no table, no migration (zero write
  amplification on the hot path; an evaluation-period switch, off by default);
- fail-open: a write failure only logs a warning and never affects the
  recall path;
- one JSON line per event (ensure_ascii=False keeps Chinese readable),
  append-opened, closed right after writing (1-2 lines per turn; at this
  frequency the handle overhead of sync append is negligible, traded for
  crash-safe persistence semantics).

Event structure:
- search: a full decision snapshot of one search() (query/scope/candidate
  count/drop counts per filter/final kept id+score+occurred_at);
- inject: the injection result of one build_injection_text() (entries that
  finally made it into the prompt after blacklist/budget truncation, with
  their time labels).
"""

from __future__ import annotations

import json
from datetime import datetime, timezone
from pathlib import Path

from core.logging_manager import get_logger

logger = get_logger("noriflow_memory.recall_log", "cyan")


class RecallLogWriter:
    """JSONL append writer (thread/coroutine safety relies on append atomicity; adequate for evaluation scenarios)."""

    def __init__(self, path: str | Path) -> None:
        """Initialize.

        Args:
            path: Log file path (the parent directory is ensured by the constructor side).
        """
        self._path = Path(path)

    @property
    def path(self) -> Path:
        """Log file path (for diagnostics)."""
        return self._path

    def write(self, event: dict) -> None:
        """Append one event (timestamp auto-added; any exception is swallowed and only logged as a warning).

        Args:
            event: Event fields (must include the event type key; datetime
                values fall back via default=str).
        """
        record = {"ts": datetime.now(timezone.utc).isoformat(), **event}
        try:
            line = json.dumps(record, ensure_ascii=False, default=str)
            with self._path.open("a", encoding="utf-8") as f:
                f.write(line + "\n")
        except Exception:
            logger.warning("recall_log 写入失败（fail-open，不影响召回）", exc_info=True)
