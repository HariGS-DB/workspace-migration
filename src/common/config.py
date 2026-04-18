from __future__ import annotations

from dataclasses import dataclass, field
from pathlib import Path

DEFAULT_CONFIG_PATH = "/Workspace/Shared/cp_migration/config.yaml"

REQUIRED_FIELDS: tuple[str, ...] = (
    "source_workspace_url",
    "target_workspace_url",
    "spn_client_id",
    "spn_secret_scope",
    "spn_secret_key",
)


def _coerce_list(raw: object) -> list[str]:
    """Accept list, comma-separated string, or None; return list[str]."""
    if raw is None or raw == "":
        return []
    if isinstance(raw, list):
        return [str(x).strip() for x in raw if str(x).strip()]
    if isinstance(raw, str):
        return [p.strip() for p in raw.split(",") if p.strip()]
    return []


def _coerce_bool(raw: object) -> bool:
    if isinstance(raw, bool):
        return raw
    if raw is None:
        return False
    return str(raw).strip().lower() in ("true", "1", "yes")


@dataclass
class MigrationConfig:
    """Configuration for a Unity Catalog migration run."""

    source_workspace_url: str
    target_workspace_url: str
    spn_client_id: str
    spn_secret_scope: str
    spn_secret_key: str
    catalog_filter: list[str] = field(default_factory=list)
    schema_filter: list[str] = field(default_factory=list)
    tracking_catalog: str = "migration_tracking"
    tracking_schema: str = "cp_migration"
    dry_run: bool = False
    batch_size: int = 50
    # Hive (Phase 2) — unused in Phase 1 notebooks but fields exist so the
    # dataclass matches the full config file schema.
    migrate_hive_dbfs_root: bool = False
    hive_dbfs_target_path: str = ""
    hive_target_catalog: str = "hive_upgraded"

    @classmethod
    def from_workspace_file(cls, path: str = DEFAULT_CONFIG_PATH) -> MigrationConfig:
        """Load migration config from a YAML file at a workspace path.

        On Databricks, ``/Workspace/...`` paths resolve to local FUSE-mounted
        files that are readable with ``open()``. Required fields are validated;
        missing ones raise a clear error so pre-check can surface them.
        """
        import yaml

        content = Path(path).read_text()
        raw = yaml.safe_load(content) or {}
        if not isinstance(raw, dict):
            msg = f"Config file at {path} must be a YAML mapping, got {type(raw).__name__}"
            raise ValueError(msg)

        missing = [k for k in REQUIRED_FIELDS if not raw.get(k)]
        if missing:
            msg = (
                f"Required config fields missing or empty in {path}: {missing}. "
                f"Edit the file and re-run."
            )
            raise ValueError(msg)

        return cls(
            source_workspace_url=str(raw["source_workspace_url"]),
            target_workspace_url=str(raw["target_workspace_url"]),
            spn_client_id=str(raw["spn_client_id"]),
            spn_secret_scope=str(raw["spn_secret_scope"]),
            spn_secret_key=str(raw["spn_secret_key"]),
            catalog_filter=_coerce_list(raw.get("catalog_filter")),
            schema_filter=_coerce_list(raw.get("schema_filter")),
            tracking_catalog=str(raw.get("tracking_catalog", "migration_tracking")),
            tracking_schema=str(raw.get("tracking_schema", "cp_migration")),
            dry_run=_coerce_bool(raw.get("dry_run")),
            batch_size=int(raw.get("batch_size", 50)),
            migrate_hive_dbfs_root=_coerce_bool(raw.get("migrate_hive_dbfs_root")),
            hive_dbfs_target_path=str(raw.get("hive_dbfs_target_path", "")),
            hive_target_catalog=str(raw.get("hive_target_catalog", "hive_upgraded")),
        )
