from __future__ import annotations

from dataclasses import dataclass


@dataclass
class ObjectTypeConfig:
    """Configuration for a single UC object type migration worker."""

    worker_notebook: str
    phase: str
    batch_size: int = 50
    parallel_threads: int = 4


OBJECT_TYPES: dict[str, ObjectTypeConfig] = {
    "managed_table": ObjectTypeConfig(worker_notebook="managed_table_worker", phase="parallel_1"),
    "external_table": ObjectTypeConfig(worker_notebook="external_table_worker", phase="parallel_1"),
    "volume": ObjectTypeConfig(worker_notebook="volume_worker", phase="parallel_1"),
    "function": ObjectTypeConfig(worker_notebook="functions_worker", phase="parallel_2"),
    "view": ObjectTypeConfig(worker_notebook="views_worker", phase="parallel_3"),
    "grant": ObjectTypeConfig(worker_notebook="grants_worker", phase="parallel_4"),
}


def get_types_by_phase(phase: str) -> dict[str, ObjectTypeConfig]:
    """Return all object types belonging to the given phase."""
    return {name: cfg for name, cfg in OBJECT_TYPES.items() if cfg.phase == phase}
