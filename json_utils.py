"""Fault-tolerant JSON parsing utilities for LLM output (vendored from the nori-core same-name utility).

Staged repair (each stage only runs when the previous one fails to parse):
strips markdown fences -> direct json.loads (valid JSON is returned as-is,
never rewritten) -> extracts the JSON body + repairs Python literals and
trailing commas -> json.loads retry -> json_repair fallback -> returns None
when everything fails (never raises).
"""

from __future__ import annotations

from core.logging_manager import get_logger

import json
import re
from typing import Any

import json_repair

logger = get_logger("noriflow_memory.json", "cyan")

# json_repair 版本差异：旧版本导出 repair()，新版本为 repair_json()
_JSON_REPAIR = getattr(json_repair, "repair", None) or getattr(
    json_repair, "repair_json", None
)


def safe_parse_llm_json(text: str) -> dict[str, Any] | list[Any] | None:
    """Safely parse JSON text returned by an LLM (staged repair).

    Stages (each later stage only runs when the previous one fails):
        1. strip markdown fences and attempt json.loads directly — perfectly
           valid JSON is returned untouched (the regex repairs below would
           silently corrupt legitimate string content like ", }" or
           "note: True" inside otherwise-valid JSON);
        2. extract the JSON body (array/object bracket slicing, unchanged
           semantics) and apply the regex repairs for Python literals /
           trailing commas, then retry json.loads;
        3. json_repair fallback (truncated text, single quotes, missing
           quotes), exactly as before.

    Args:
        text: Raw LLM output text (may contain markdown fences, explanatory
            text, or malformed JSON).

    Returns:
        The parsed dict or list; None when nothing can be parsed.
    """
    if not text:
        return None

    cleaned = text.strip()
    # 去除 markdown 代码块标记
    cleaned = re.sub(r"^```(?:json)?\s*", "", cleaned, flags=re.IGNORECASE)
    cleaned = re.sub(r"\s*```$", "", cleaned)
    # Stage 1: perfectly valid JSON parses as-is; the regex repairs must
    # never rewrite string content of valid JSON (e.g. a stored statement
    # legitimately containing ", }" or "note: True").
    try:
        return json.loads(cleaned)
    except json.JSONDecodeError:
        pass
    # Stage 2: 提取 JSON 主体：整体是数组时优先按数组提取（避免内层对象遮蔽），
    # 否则优先对象，其次数组
    cleaned_body = cleaned.strip()
    if cleaned_body.startswith("[") and cleaned_body.endswith("]"):
        arr_start = cleaned_body.find("[")
        arr_end = cleaned_body.rfind("]")
        if arr_end > arr_start:
            cleaned = cleaned_body[arr_start : arr_end + 1]
    else:
        obj_start = cleaned.find("{")
        obj_end = cleaned.rfind("}")
        arr_start = cleaned.find("[")
        arr_end = cleaned.rfind("]")
        if obj_start != -1 and obj_end != -1 and obj_end > obj_start:
            cleaned = cleaned[obj_start : obj_end + 1]
        elif arr_start != -1 and arr_end != -1 and arr_end > arr_start:
            cleaned = cleaned_body[arr_start : arr_end + 1]
    # 修复常见错误
    cleaned = re.sub(r",\s*}", "}", cleaned)
    cleaned = re.sub(r",\s*]", "]", cleaned)
    cleaned = re.sub(r":\s*True\b", ": true", cleaned)
    cleaned = re.sub(r":\s*False\b", ": false", cleaned)
    cleaned = re.sub(r":\s*None\b", ": null", cleaned)
    try:
        return json.loads(cleaned)
    except json.JSONDecodeError:
        pass

    # json_repair 兜底：修复截断、单引号、缺失引号等 json.loads 无法处理的情况
    if _JSON_REPAIR is None:
        logger.warning("json_repair 不可用，无法兜底修复: %s", text[:200])
        return None
    try:
        repaired = _JSON_REPAIR(text, return_objects=True)
    except Exception as e:
        logger.warning("json_repair 修复失败: %s, raw=%s", e, text[:200])
        return None
    if isinstance(repaired, (dict, list)):
        return repaired
    return None
