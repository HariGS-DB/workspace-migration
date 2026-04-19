# Databricks notebook source

# COMMAND ----------

# Bootstrap: put the bundle's `src/` dir on sys.path so `from common...` imports resolve
import sys  # noqa: E402
try:
    _ctx = dbutils.notebook.entry_point.getDbutils().notebook().getContext()  # noqa: F821
    _nb = _ctx.notebookPath().get()
    _src = "/Workspace" + _nb.split("/files/")[0] + "/files/src"
    if _src not in sys.path:
        sys.path.insert(0, _src)
except NameError:
    pass  # not running under a Databricks notebook (e.g. pytest)

# COMMAND ----------

# Discovery: unified entry point for UC and Hive discovery.
#
# Both domains write to a single `discovery_inventory` table; rows are
# distinguished by the `source_type` column ('uc' or 'hive'). Scope is
# controlled via config.include_uc / config.include_hive.

import contextlib
from collections import Counter
from datetime import datetime, timezone

from common.auth import AuthManager
from common.catalog_utils import CatalogExplorer
from common.config import MigrationConfig
from common.tracking import TrackingManager, discovery_row, discovery_schema

# COMMAND ----------


def _is_notebook() -> bool:
    try:
        _ = dbutils  # type: ignore[name-defined]  # noqa: F821
        return True
    except NameError:
        return False


# COMMAND ----------


_MIGRATION_SHARE = "cp_migration_share"
_MIGRATION_RECIPIENT_PREFIX = "cp_migration_recipient_"


def _discover_uc(config, explorer, now) -> tuple[list[dict], int]:
    """Discover UC objects. Returns (rows, dlt_count)."""
    rows: list[dict] = []
    dlt_count = 0
    all_table_fqns: list[str] = []  # for workspace-level monitor enumeration

    catalogs = explorer.list_catalogs(filter_list=config.catalog_filter or None)
    print(f"[uc] Discovered {len(catalogs)} catalog(s): {catalogs}")

    for catalog in catalogs:
        schemas = explorer.list_schemas(catalog)
        if config.schema_filter:
            schemas = [s for s in schemas if s in config.schema_filter]
        print(f"  [uc] Catalog '{catalog}': {len(schemas)} schema(s)")

        for schema in schemas:
            # --- Tables and views ---
            tables = explorer.classify_tables(catalog, schema)
            for tbl in tables:
                fqn = tbl["fqn"]
                obj_type = tbl["object_type"]

                if obj_type == "view":
                    is_dlt, pipeline_id = False, None
                else:
                    is_dlt, pipeline_id = explorer.detect_dlt_managed(fqn)
                    # MVs / STs always have a pipeline_id (auto-provisioned or
                    # DLT-defined). Only flag as DLT-managed if the underlying
                    # pipeline is user-owned (non-empty spec.libraries). That
                    # distinction is deferred to mv_st_worker.
                    if obj_type in ("mv", "st"):
                        is_dlt = False  # distinguish later via pipelines.get()
                    if is_dlt:
                        dlt_count += 1

                row_count = 0
                size_bytes = 0
                create_stmt = ""
                table_format: str | None = None

                # MV / ST row counts via SELECT COUNT(*) can be expensive or
                # block on auto-refresh; skip them and let the target do its
                # own validation after REFRESH.
                if obj_type not in ("view", "mv", "st"):
                    with contextlib.suppress(Exception):
                        row_count = explorer.get_table_row_count(fqn)
                    with contextlib.suppress(Exception):
                        size_bytes = explorer.get_table_size_bytes(fqn)
                    with contextlib.suppress(Exception):
                        table_format = explorer.get_table_format(fqn)

                with contextlib.suppress(Exception):
                    create_stmt = explorer.get_create_statement(fqn)

                rows.append(discovery_row(
                    source_type="uc",
                    object_type=obj_type,
                    object_name=fqn,
                    catalog_name=catalog,
                    schema_name=schema,
                    discovered_at=now,
                    row_count=row_count,
                    size_bytes=size_bytes,
                    is_dlt_managed=is_dlt,
                    pipeline_id=pipeline_id,
                    create_statement=create_stmt,
                    format=table_format,
                ))
                if obj_type in ("managed_table", "external_table"):
                    all_table_fqns.append(fqn)

            # --- Functions ---
            for func_fqn in explorer.list_functions(catalog, schema):
                ddl = ""
                with contextlib.suppress(Exception):
                    ddl = explorer.get_function_ddl(func_fqn)

                rows.append(discovery_row(
                    source_type="uc",
                    object_type="function",
                    object_name=func_fqn,
                    catalog_name=catalog,
                    schema_name=schema,
                    discovered_at=now,
                    create_statement=ddl,
                ))

            # --- Volumes ---
            for vol in explorer.list_volumes(catalog, schema):
                rows.append(discovery_row(
                    source_type="uc",
                    object_type="volume",
                    object_name=vol["fqn"],
                    catalog_name=catalog,
                    schema_name=schema,
                    discovered_at=now,
                    table_type=vol.get("volume_type"),  # MANAGED or EXTERNAL
                    storage_location=vol.get("storage_location"),
                ))

            # --- Phase 3 governance: per-schema objects ---
            for tag in explorer.list_tags(catalog, schema):
                rows.append(discovery_row(
                    source_type="uc",
                    object_type="tag",
                    object_name=(
                        f"{tag['securable_fqn']}:{tag.get('column_name','')}:"
                        f"{tag['tag_name']}"
                    ).rstrip(":"),
                    catalog_name=catalog,
                    schema_name=schema,
                    discovered_at=now,
                    metadata=tag,
                ))

            for rf in explorer.list_row_filters(catalog, schema):
                rows.append(discovery_row(
                    source_type="uc",
                    object_type="row_filter",
                    object_name=rf["table_fqn"],
                    catalog_name=catalog,
                    schema_name=schema,
                    discovered_at=now,
                    metadata=rf,
                ))

            for cm in explorer.list_column_masks(catalog, schema):
                rows.append(discovery_row(
                    source_type="uc",
                    object_type="column_mask",
                    object_name=f"{cm['table_fqn']}.{cm['column_name']}",
                    catalog_name=catalog,
                    schema_name=schema,
                    discovered_at=now,
                    metadata=cm,
                ))

            for m in explorer.list_registered_models(catalog, schema):
                rows.append(discovery_row(
                    source_type="uc",
                    object_type="registered_model",
                    object_name=m["model_fqn"],
                    catalog_name=catalog,
                    schema_name=schema,
                    discovered_at=now,
                    storage_location=m.get("storage_location"),
                    metadata=m,
                ))

    # --- Phase 3 governance: workspace-level objects ---
    # Monitors are per-table; enumerate over every discovered table.
    for mon in explorer.list_monitors(all_table_fqns):
        rows.append(discovery_row(
            source_type="uc",
            object_type="monitor",
            object_name=mon["table_fqn"],
            catalog_name=None,
            schema_name=None,
            discovered_at=now,
            metadata=mon,
        ))

    for p in explorer.list_policies():
        rows.append(discovery_row(
            source_type="uc",
            object_type="policy",
            object_name=p["policy_name"] or f"policy_{p.get('securable_fqn', '?')}",
            catalog_name=None,
            schema_name=None,
            discovered_at=now,
            metadata=p,
        ))

    for c in explorer.list_connections():
        rows.append(discovery_row(
            source_type="uc",
            object_type="connection",
            object_name=c["connection_name"],
            catalog_name=None,
            schema_name=None,
            discovered_at=now,
            metadata=c,
        ))

    for fc in explorer.list_foreign_catalogs():
        rows.append(discovery_row(
            source_type="uc",
            object_type="foreign_catalog",
            object_name=fc["catalog_name"],
            catalog_name=fc["catalog_name"],
            schema_name=None,
            discovered_at=now,
            metadata=fc,
        ))

    for ot in explorer.list_online_tables():
        rows.append(discovery_row(
            source_type="uc",
            object_type="online_table",
            object_name=ot["online_table_fqn"],
            catalog_name=None,
            schema_name=None,
            discovered_at=now,
            metadata=ot,
        ))

    exclude_shares = frozenset({_MIGRATION_SHARE})
    for s in explorer.list_shares(exclude_names=exclude_shares):
        rows.append(discovery_row(
            source_type="uc",
            object_type="share",
            object_name=s["share_name"],
            catalog_name=None,
            schema_name=None,
            discovered_at=now,
            metadata=s,
        ))

    for r in explorer.list_recipients(exclude_prefix=_MIGRATION_RECIPIENT_PREFIX):
        rows.append(discovery_row(
            source_type="uc",
            object_type="recipient",
            object_name=r["recipient_name"],
            catalog_name=None,
            schema_name=None,
            discovered_at=now,
            metadata=r,
        ))

    for p in explorer.list_providers():
        rows.append(discovery_row(
            source_type="uc",
            object_type="provider",
            object_name=p["provider_name"],
            catalog_name=None,
            schema_name=None,
            discovered_at=now,
            metadata=p,
        ))

    return rows, dlt_count


def _discover_hive(config, explorer, now) -> list[dict]:
    """Discover Hive objects. Returns rows list."""
    rows: list[dict] = []
    databases = explorer.list_hive_databases()
    print(f"[hive] Discovered {len(databases)} database(s): {databases}")

    for database in databases:
        # --- Tables and views ---
        for tbl in explorer.classify_hive_tables(database):
            row_count = 0
            size_bytes = 0
            if tbl["object_type"] == "hive_table":
                with contextlib.suppress(Exception):
                    row_count = explorer.get_table_row_count(tbl["fqn"])
                with contextlib.suppress(Exception):
                    size_bytes = explorer.get_table_size_bytes(tbl["fqn"])

            rows.append(discovery_row(
                source_type="hive",
                object_type=tbl["object_type"],
                object_name=tbl["fqn"],
                catalog_name="hive_metastore",
                schema_name=database,
                discovered_at=now,
                row_count=row_count,
                size_bytes=size_bytes,
                data_category=tbl["data_category"],
                table_type=tbl["table_type"],
                provider=tbl["provider"],
                storage_location=tbl["storage_location"],
            ))

        # --- Functions ---
        for func_fqn in explorer.list_hive_functions(database):
            rows.append(discovery_row(
                source_type="hive",
                object_type="hive_function",
                object_name=func_fqn,
                catalog_name="hive_metastore",
                schema_name=database,
                discovered_at=now,
                data_category="hive_function",
                table_type="",
                provider="",
                storage_location="",
            ))

    return rows


# COMMAND ----------


def run(dbutils, spark):  # noqa: D103
    config = MigrationConfig.from_workspace_file()
    auth = AuthManager(config, dbutils)
    tracker = TrackingManager(spark, config)
    explorer = CatalogExplorer(spark, auth)

    tracker.init_tracking_tables()

    if not (config.include_uc or config.include_hive):
        print("Neither scope.include_uc nor scope.include_hive is enabled — nothing to discover.")
        return []

    now = datetime.now(tz=timezone.utc)
    inventory: list[dict] = []
    dlt_count = 0

    if config.include_uc:
        uc_rows, dlt_count = _discover_uc(config, explorer, now)
        inventory.extend(uc_rows)
    else:
        print("[uc] Skipped (scope.include_uc = false)")

    if config.include_hive:
        inventory.extend(_discover_hive(config, explorer, now))
    else:
        print("[hive] Skipped (scope.include_hive = false)")

    print(f"\nTotal objects discovered: {len(inventory)}")

    if inventory:
        df = spark.createDataFrame(inventory, schema=discovery_schema())
        tracker.write_discovery_inventory(df)
        print("Discovery inventory written to tracking table.")
    else:
        print("WARNING: No objects discovered. Check catalog/schema/scope filters.")

    # Summary by (source_type, object_type)
    type_counts = Counter((obj["source_type"], obj["object_type"]) for obj in inventory)
    print(f"\n{'Source':<8} {'Object Type':<20} {'Count':>8}")
    print("-" * 40)
    for (src, obj_type), count in sorted(type_counts.items()):
        print(f"{src:<8} {obj_type:<20} {count:>8}")
    print("-" * 40)
    print(f"{'TOTAL':<28} {len(inventory):>8}")

    if dlt_count > 0:
        print(f"\n** {dlt_count} DLT-managed table(s) detected. These require special handling during migration. **")

    return inventory


# COMMAND ----------

if _is_notebook():
    run(dbutils, spark)  # type: ignore[name-defined]  # noqa: F821
