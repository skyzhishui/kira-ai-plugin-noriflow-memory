"""Read/write core logic for the "Settings" tab of the maintenance page (adapted from the config endpoints of nori-core nori_plugin_noriflow_memory web_api.py).

Key semantics isomorphic to the upstream nori version:
- schema is derived from LocalMemoryConfig.model_fields (single source of truth);
- sensitive-key mask sentinel matches the host WebUI;
- save = validate -> diff -> write host source of truth -> update the runtime
  LocalMemoryConfig instance in place (kernel/merge_agent/encoder/breaker
  read the same instance per call, so the save takes effect immediately);
- fields expanded at assembly time (connection pool / embedding client /
  alias layer assembly etc.) are marked restart, honestly prompting that a
  re-initialize / process restart is required.

Differences from the upstream nori version (KiraAI host semantics):
- the config source of truth lives in the host (plugin_mgr.plugin_configs +
  PLUGIN_CONFIG_DIR/<pid>.json); save syncs the host but does not call
  update_plugin_config, which would trigger a full init_plugin re-initialization
  (pool teardown/rebuild) in conflict with the hot-effect goal; instead it
  directly updates the host in-memory dict and persists to disk, consistent
  with how the host config page reads memory.
"""

from __future__ import annotations

import json
import math
import re
import types
from typing import TYPE_CHECKING, Any, Literal, Union, get_args, get_origin

from pydantic_core import PydanticUndefined

from .config import LocalMemoryConfig

if TYPE_CHECKING:
    from pathlib import Path

# Mask sentinel and sensitive-key judgment (isomorphic to the KiraAI
# host WebUI / upstream nori version)
_MASK = "••••••"
_SENSITIVE_KEY_RE = re.compile(r"password|api_key|apikey|token|secret|dsn|cookie", re.I)

# Fields expanded at assembly time (values read when components are
# constructed): changing them requires re-initializing the plugin or
# restarting the process. Other fields are read per operation at runtime
# by kernel/merge_agent/encoder/circuit breaker from the same
# LocalMemoryConfig instance, so saving takes effect immediately
# (2026-09-11 hot-reload batch rework).
_RESTART_FIELDS = frozenset({
    "storage_backend", "sqlite_path",
    "dsn", "pool_min", "pool_max", "db_command_timeout",
    "embedding_model", "embedding_dims",
    "recall_log_enabled", "recall_log_path",
    "alias_enabled", "alias_variant_cap", "alias_stopwords",
    # Assembled at startup: the kernel does read this switch per operation
    # at runtime, but flipping off->on still requires re-initializing the
    # client -- it looks like a hot-reloadable field but is not.
    "rerank_enabled",
})

# Settings-panel groups (aligned with the upstream nori version;
# KiraAI-specific fields merged into the matching group)
_CONFIG_GROUPS: list[tuple[str, str, tuple[str, ...]]] = [
    ("conn", "连接与存储", (
        "storage_backend", "sqlite_path", "dsn", "pool_min", "pool_max",
        "db_command_timeout",
    )),
    ("embed", "Embedding 与补算", (
        "embedding_model", "embedding_dims", "encode_input_max_chars",
        "backfill_interval_seconds", "backfill_batch_size",
    )),
    ("retain", "写入触发", (
        "retain_interval_group", "retain_interval_private", "flush_idle_s",
    )),
    ("merge", "合并 agent", (
        "merge_interval_hours", "merge_batch_size", "candidate_top_k",
        "llm_budget_per_cycle",
    )),
    ("scoring", "评分状态机", (
        "score_start_high", "score_start_medium", "score_cap",
        "promote_threshold", "demote_threshold", "decay_factor",
        "decay_interval_days", "recent_expire_days", "recent_promote_threshold",
        "naming_promote_threshold", "naming_prompt_enabled",
        "commitment_expire_days",
        "decay_requires_activity", "sticky_evidence_count", "pending_dead_days",
        "anchor_profile_size",
    )),
    ("recall", "召回", (
        "recall_top_k", "recall_relevance_threshold", "recall_max_tokens",
        "recall_time_decay_enabled", "recall_time_decay_half_life_days",
        "summary_recall_session_scoped", "topic_blacklist",
        "rerank_enabled", "rerank_candidates",
    )),
    ("quality", "召回质量与画像", (
        "recall_hint_enabled", "recall_expansion_enabled",
        "recall_expansion_recent_batches", "recall_exclude_history_window",
        "dedup_similarity_threshold", "write_dedup_enabled",
        "write_dedup_window", "write_dedup_threshold",
        "recall_time_label_enabled", "recall_time_label_mode", "timezone",
        "max_persona_profiles", "recall_log_enabled", "recall_log_path",
    )),
    ("alias", "实体别名", (
        "alias_enabled", "alias_variant_cap", "alias_stopwords",
    )),
    ("relation", "关系提取与注入", (
        "relation_extract_enabled", "relation_bot_edge_min_evidence",
        "relation_inject_enabled", "relation_inject_max_neighbors",
        "relation_inject_max_profiles", "relation_inject_max_lines",
        "relation_label_stopwords", "relation_audit_enabled",
        "relation_audit_batch_size",
    )),
    ("hybrid", "混合检索", ("hybrid_search_enabled", "hybrid_rrf_k")),
    ("rollout", "滚动补回", (
        "recent_rollout_enabled", "recent_rollout_batches",
        "recent_rollout_max_chars",
    )),
    ("cross_session", "跨会话摘要", (
        "cross_session_summary_enabled", "cross_session_summary_top_k",
        "cross_session_summary_max_chars",
        "cross_session_summary_max_age_days",
    )),
    ("lifecycle", "摘要生命周期", (
        "summary_lifecycle_enabled", "summary_lifecycle_grace_days",
        "summary_lifecycle_half_life_days",
        "summary_lifecycle_reinforce_window_days",
        "summary_lifecycle_interval_days",
        "summary_lifecycle_reinforce_on_recall",
    )),
    ("breaker", "DB 熔断", ("failure_threshold", "recovery_seconds")),
    ("tools", "主动工具", ("tool_scope_locked", "memory_tools_enabled")),
]


# Union origins to strip when resolving an Optional annotation: `Optional[X]`
# and `X | None` are both valid spellings (typing.Union / PEP 604
# types.UnionType), and config.py may use either. getattr keeps the import
# working on a Python without types.UnionType (3.9 and older).
_UNION_ORIGINS: tuple = (Union, getattr(types, "UnionType", Union))


def _unwrap_optional(annotation):
    """Strip ``Optional[X]`` / ``Union[X, None]`` wrappers down to ``X``.

    Fields whose "unset" state is meaningful (e.g. ``naming_promote_threshold``,
    derived from score_start_high when None) are declared ``Optional[...]``.
    Without unwrapping, such an annotation matches no scalar branch of
    ``_field_type`` and falls through to the "list" fallback: it would be
    rendered as a multi-line textarea whose empty value is submitted as
    ``[]`` and 422-rejected by pydantic whenever the host config does not
    carry the key yet (every save would be refused).

    Nested wrappers (``Optional[Optional[X]]``, ``X | None | None``) are
    stripped in a loop. A union with more than one non-None member is
    returned untouched -- it has no single control type to map to.
    """
    while get_origin(annotation) in _UNION_ORIGINS:
        members = [a for a in get_args(annotation) if a is not type(None)]
        if len(members) != 1:
            return annotation
        annotation = members[0]
    return annotation


def _field_type(annotation) -> str:
    """pydantic annotation -> frontend control type (Optional[X] is classified as X)."""
    annotation = _unwrap_optional(annotation)
    if annotation is bool:
        return "bool"
    if annotation is int:
        return "int"
    if annotation is float:
        return "float"
    if annotation is str:
        return "str"
    if get_origin(annotation) is Literal:
        # Literal["a", "b"] is an enum string, not a list — rendered as a
        # str control; options are exposed via config_schema (see
        # _literal_options)
        return "str"
    return "list"


def _literal_options(annotation) -> list[str] | None:
    """Optional values of a Literal annotation (None for non-Literal annotations).

    ``Optional[Literal[...]]`` is unwrapped as well (None excluded from the
    options): an enum field that may be unset still exposes its choices.
    """
    annotation = _unwrap_optional(annotation)
    if get_origin(annotation) is Literal:
        return [str(v) for v in get_args(annotation)]
    return None


def _numeric_bounds(field) -> tuple[Any, Any]:
    """Extract (min, max) from pydantic numeric constraints, if any.

    ``gt``/``lt`` are EXCLUSIVE bounds: a value equal to the bound is
    422-rejected by pydantic, so the inclusive HTML ``min``/``max`` must be
    nudged strictly inside the valid range — ints by ±1, floats by one ULP
    via ``math.nextafter`` (min = nextafter(gt, inf); max = nextafter(lt,
    -inf)). Inclusive ``ge``/``le`` bounds pass through verbatim.
    """
    lo: Any = None
    hi: Any = None
    # Unwrap Optional so an Optional[int] field still gets the ±1 int nudge
    # instead of the float ULP nudge
    is_int = _unwrap_optional(field.annotation) is int
    for meta in field.metadata:
        ge = getattr(meta, "ge", None)
        gt = getattr(meta, "gt", None)
        le = getattr(meta, "le", None)
        lt = getattr(meta, "lt", None)
        candidate = None
        if ge is not None:
            candidate = ge
        elif gt is not None:
            candidate = gt + 1 if is_int else math.nextafter(gt, math.inf)
        if candidate is not None and (lo is None or candidate > lo):
            lo = candidate
        candidate = None
        if le is not None:
            candidate = le
        elif lt is not None:
            candidate = lt - 1 if is_int else math.nextafter(lt, -math.inf)
        if candidate is not None and (hi is None or candidate < hi):
            hi = candidate
    return lo, hi


def config_schema() -> dict:
    """Derive the frontend schema from LocalMemoryConfig.model_fields (single source of truth)."""
    schema: dict[str, dict[str, Any]] = {}
    for name, f in LocalMemoryConfig.model_fields.items():
        if f.default_factory is not None:
            default = f.default_factory()
        elif f.default is not PydanticUndefined:
            default = f.default
        else:
            default = None
        entry: dict[str, Any] = {
            "type": _field_type(f.annotation),
            "default": default,
            "description": f.description or "",
            "restart": name in _RESTART_FIELDS,
            "sensitive": bool(_SENSITIVE_KEY_RE.search(name)),
        }
        literal_options = _literal_options(f.annotation)
        if literal_options is not None:
            entry["options"] = literal_options
        if entry["type"] in ("int", "float"):
            lo, hi = _numeric_bounds(f)
            if lo is not None:
                entry["min"] = lo
            if hi is not None:
                entry["max"] = hi
        schema[name] = entry
    return schema


def mask_values(values: dict) -> dict:
    """Mask string values of sensitive keys."""
    return {
        k: (_MASK if (_SENSITIVE_KEY_RE.search(k) and isinstance(v, str) and v) else v)
        for k, v in values.items()
    }


def unmask_values(new: dict, current: dict) -> dict:
    """Restore mask sentinels in submitted values to current values (unmodified sensitive fields are not refilled with the mask).

    Collision guard: a submitted mask is only treated as "unchanged" when
    the stored value is itself not the mask string, if the real secret
    literally equals the sentinel, submitting it must be honored as a value
    (otherwise that secret could never be set).
    """
    out: dict = {}
    for k, v in new.items():
        if v == _MASK and current.get(k) != _MASK:
            out[k] = current.get(k, v)
        else:
            out[k] = v
    return out


def build_payload(host_values: dict) -> dict:
    """Assemble the data section of GET /memory/config (schema + groups + masked current values)."""
    schema = config_schema()
    current = {k: v for k, v in host_values.items() if k in schema}
    values = {k: f["default"] for k, f in schema.items()}
    values.update(current)
    return {
        "groups": [
            {"key": key, "label": label, "fields": list(fields)}
            for key, label, fields in _CONFIG_GROUPS
        ],
        "schema": schema,
        "values": mask_values(values),
    }


def prepare_save(submitted: dict, host_values: dict) -> dict:
    """Validate and diff submitted values.

    Returns:
        {"merged", "validated", "changed", "restart_required"}.

    Raises:
        ValueError: validation failed (message includes pydantic details).
        SaveBlocked: business-level rejection such as clearing dsn.
    """
    schema = config_schema()
    merged = unmask_values(
        {k: v for k, v in submitted.items() if k in schema}, host_values
    )
    # Non-empty dsn guard (dual-backend semantics): the postgres backend
    # must keep its dsn; under auto, a non-empty current dsn means we are
    # actually running on postgres — clearing it would silently switch to
    # sqlite, so it is blocked too. Only the sqlite backend (explicitly
    # declared, or auto with no dsn in the first place) passes.
    effective_backend = str(
        merged.get("storage_backend")
        if merged.get("storage_backend") not in (None, "")
        else host_values.get("storage_backend", "auto")
        or "auto"
    ).strip().lower()
    host_dsn = str(host_values.get("dsn") or "").strip()
    dsn_guard = effective_backend == "postgres" or (
        effective_backend == "auto" and bool(host_dsn)
    )
    if dsn_guard and "dsn" in merged and not str(merged.get("dsn") or "").strip():
        raise SaveBlocked("dsn 不能清空（当前存储后端以数据库连接为存在前提；"
                          "停用请用插件总开关，或切换 storage_backend=sqlite）")

    # Validation / hot-update scope = current values ∪ defaults for
    # missing keys, then overlaid with submitted values (partial
    # semantics: unsubmitted fields keep current values; model defaults
    # must not override host and runtime state)
    baseline = {k: f["default"] for k, f in schema.items()}
    baseline.update({k: v for k, v in host_values.items() if k in schema})
    try:
        validated = LocalMemoryConfig(**{**baseline, **merged})
    except Exception as exc:
        raise ValueError(f"配置校验失败：{exc}") from exc

    # Persist the VALIDATED value, not the raw submitted one: direct API
    # callers may submit strings for numeric fields; storing those would
    # pollute the host config JSON (runtime survives via int() fallbacks,
    # but the file should stay type-clean).
    changed = {}
    for k, v in merged.items():
        if k in schema and baseline.get(k) != v:
            changed[k] = getattr(validated, k)
    return {
        "merged": merged,
        "validated": validated,
        "changed": changed,
        "restart_required": sorted(k for k in changed if k in _RESTART_FIELDS),
    }


def persist_host_config(config_dir: "Path", plugin_id: str, changed: dict,
                        current_host: dict) -> None:
    """Merge changes into the host config file (PLUGIN_CONFIG_DIR/<pid>.json).

    Consistent with the host update_plugin_config disk behavior (read the
    current file -> merge -> write back), but does not trigger init_plugin
    (hot effect is handled by the caller updating the runtime instance in
    place).
    """
    config_dir.mkdir(parents=True, exist_ok=True)
    config_path = config_dir / f"{plugin_id}.json"
    on_disk: dict = {}
    if config_path.is_file():
        try:
            on_disk = json.loads(config_path.read_text(encoding="utf-8"))
        except Exception:
            on_disk = {}
    on_disk.update(changed)
    # Merge direction: host in-memory config is the session source of truth,
    # the file only exists to survive a host restart. Latest values win —
    # keys just changed are taken from on_disk (already overlaid with the
    # new values), keys known to host memory keep the in-memory value (a
    # previously failed persist must not silently roll back later saves),
    # and disk-only keys unknown to host memory (e.g. written by an older
    # version) are preserved.
    merged = {
        k: (on_disk[k] if (k in changed or k not in current_host) else v)
        for k, v in current_host.items()
    }
    for k, v in on_disk.items():
        merged.setdefault(k, v)
    # Atomic write (temp file + replace): a crash mid-write must not leave a
    # truncated JSON that the next load would silently fall back to {}.
    tmp_path = config_path.with_suffix(".json.tmp")
    tmp_path.write_text(
        json.dumps(merged, indent=4, ensure_ascii=False), encoding="utf-8"
    )
    tmp_path.replace(config_path)


class SaveBlocked(Exception):
    """Business-level save rejection (e.g. clearing dsn), HTTP 400."""
