from __future__ import annotations

from dataclasses import dataclass, field
from pathlib import Path

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



def _resolve_bundle_config_path() -> str:
    """Resolve config.yaml path from the running notebook's own workspace path.

    config.yaml is synced by DAB into the bundle's ``files/`` directory on
    every deploy, so it sits at ``${workspace.file_path}/config.yaml``. We
    derive that path by splitting the running notebook's own workspace path
    on ``/files/`` (the same trick the sys.path bootstrap at the top of each
    notebook uses) and re-joining under the ``/Workspace`` FUSE mount.

    Raises:
        RuntimeError: when not running under a Databricks notebook context
            (no ``dbutils`` available). Callers should pass an explicit path
            to :meth:`MigrationConfig.from_workspace_file` in that case.
    """
    dbutils = None
    try:
        import IPython  # type: ignore[import-not-found]

        ip = IPython.get_ipython()
        if ip is not None:
            dbutils = ip.user_ns.get("dbutils")
    except Exception:  # noqa: BLE001
        dbutils = None

    if dbutils is None:
        msg = (
            "Cannot auto-resolve config.yaml path: no dbutils available. "
            "Pass an explicit path to MigrationConfig.from_workspace_file()."
        )
        raise RuntimeError(msg)

    ctx = dbutils.notebook.entry_point.getDbutils().notebook().getContext()
    nb_path = ctx.notebookPath().get()
    # nb_path looks like /Shared/cp_migration/<target>/files/<...>/notebook.
    # The config lives at <bundle_root>/files/config.yaml on the FUSE mount.
    bundle_root = nb_path.split("/files/")[0]
    return f"/Workspace{bundle_root}/files/config.yaml"


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
    # Scope: which source metastore domains to discover/migrate. Default is UC
    # only — Hive enablement is opt-in.
    include_uc: bool = True
    include_hive: bool = False
    # Hive (Phase 2) — unused in Phase 1 notebooks but fields exist so the
    # dataclass matches the full config file schema.
    migrate_hive_dbfs_root: bool = False
    hive_dbfs_target_path: str = ""
    hive_target_catalog: str = "hive_upgraded"

    @classmethod
    def from_workspace_file(cls, path: str | None = None) -> MigrationConfig:
        """Load migration config from a YAML file at a workspace path.

        When ``path`` is ``None`` (the default for workflow notebooks), resolve
        the config.yaml shipped alongside the current bundle deployment at
        ``${workspace.file_path}/config.yaml``. The path is discovered from
        the running notebook's own workspace location — the same trick the
        sys.path bootstrap at the top of each notebook uses — so the config
        file travels with the bundle and every ``databricks bundle deploy``
        re-syncs it from the local checkout.

        Tests and ad-hoc callers can pass an explicit ``path``.

        On Databricks, ``/Workspace/...`` paths resolve to local FUSE-mounted
        files that are readable with ``open()``. Required fields are validated;
        missing ones raise a clear error so pre-check can surface them.
        """
        import yaml

        resolved = path if path is not None else _resolve_bundle_config_path()
        content = Path(resolved).read_text()
        raw = yaml.safe_load(content) or {}
        if not isinstance(raw, dict):
            msg = f"Config file at {resolved} must be a YAML mapping, got {type(raw).__name__}"
            raise ValueError(msg)

        missing = [k for k in REQUIRED_FIELDS if not raw.get(k)]
        if missing:
            msg = (
                f"Required config fields missing or empty in {resolved}: {missing}. "
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
            include_uc=_coerce_bool((raw.get("scope") or {}).get("include_uc", True)),
            include_hive=_coerce_bool((raw.get("scope") or {}).get("include_hive", False)),
            migrate_hive_dbfs_root=_coerce_bool(raw.get("migrate_hive_dbfs_root")),
            hive_dbfs_target_path=str(raw.get("hive_dbfs_target_path", "")),
            hive_target_catalog=str(raw.get("hive_target_catalog", "hive_upgraded")),
        )
