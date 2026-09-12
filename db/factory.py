"""Backend assembly factory (docs/plans/noriflow-dual-storage-backend-plan.md
§3/§4).

resolve_backend_name picks the effective backend; create_backend builds the
matching implementation (external consumers such as the KiraAI->noriflow
import tool also go through this factory — interface constraints in §11).
"""

from __future__ import annotations

from .base import MemoryBackend
from .postgres import MemoryDatabase

# Accepted storage_backend config values.
_BACKEND_CHOICES = ("postgres", "sqlite", "auto")


def resolve_backend_name(config) -> str:
    """Resolve the storage_backend config into a concrete backend name.

    "auto" means postgres when dsn is set and sqlite otherwise — existing
    deployments (storage_backend unset, dsn configured) keep the exact same
    behavior.

    Args:
        config: LocalMemoryConfig (storage_backend/dsn fields).

    Returns:
        "postgres" or "sqlite".

    Raises:
        ValueError: storage_backend holds an invalid value.
    """
    backend = (getattr(config, "storage_backend", "auto") or "auto").strip().lower()
    if backend not in _BACKEND_CHOICES:
        raise ValueError(
            f"storage_backend 取值非法: {backend!r}（可选 postgres | sqlite | auto）"
        )
    if backend == "auto":
        return "postgres" if (config.dsn or "").strip() else "sqlite"
    return backend


def create_backend(config) -> MemoryBackend:
    """Build the storage backend selected by config (no connection is made —
    the caller owns connect/apply_migrations).

    Args:
        config: LocalMemoryConfig.

    Returns:
        A MemoryBackend instance (MemoryDatabase or SQLiteMemoryDatabase).
    """
    name = resolve_backend_name(config)
    if name == "sqlite":
        from .sqlite import SQLiteMemoryDatabase

        return SQLiteMemoryDatabase(config)
    return MemoryDatabase(config)
