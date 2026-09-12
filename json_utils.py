"""LLM 输出 JSON 解析容错工具（自 nori-core 同名工具 vendor）。

剥离 markdown 围栏 -> 提取 JSON 主体 -> 修复 Python 字面量与尾逗号 ->
json.loads -> json_repair 兜底 -> 全部失败返回 None（不抛异常）。
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
    """安全解析 LLM 返回的 JSON 文本。

    Args:
        text: LLM 原始输出文本（可能含 markdown 围栏、说明文字、畸形 JSON）。

    Returns:
        解析成功的 dict 或 list；完全无法解析时 None。
    """
    if not text:
        return None

    cleaned = text.strip()
    # 去除 markdown 代码块标记
    cleaned = re.sub(r"^```(?:json)?\s*", "", cleaned, flags=re.IGNORECASE)
    cleaned = re.sub(r"\s*```$", "", cleaned)
    # 提取 JSON 主体：整体是数组时优先按数组提取（避免内层对象遮蔽），
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
            cleaned = cleaned[arr_start : arr_end + 1]
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
