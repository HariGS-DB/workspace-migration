from __future__ import annotations

from dataclasses import dataclass, field


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

    @classmethod
    def from_job_params(cls, dbutils: object) -> MigrationConfig:
        """Build a MigrationConfig from Databricks job widgets."""
        get = dbutils.widgets.get  # type: ignore[attr-defined]

        raw_catalog = get("catalog_filter")
        catalog_filter = [c.strip() for c in raw_catalog.split(",") if c.strip()] if raw_catalog else []

        raw_schema = get("schema_filter")
        schema_filter = [s.strip() for s in raw_schema.split(",") if s.strip()] if raw_schema else []

        dry_run = get("dry_run").strip().lower() in ("true", "1", "yes")

        return cls(
            source_workspace_url=get("source_workspace_url"),
            target_workspace_url=get("target_workspace_url"),
            spn_client_id=get("spn_client_id"),
            spn_secret_scope=get("spn_secret_scope"),
            spn_secret_key=get("spn_secret_key"),
            catalog_filter=catalog_filter,
            schema_filter=schema_filter,
            dry_run=dry_run,
        )
