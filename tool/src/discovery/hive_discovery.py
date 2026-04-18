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

# Hive Discovery: scan the legacy `hive_metastore` catalog and build an inventory
# of Hive objects (tables, views, functions) classified by migration category.
#
# Writes to migration_tracking.cp_migration.hive_discovery_inventory.

from collections import Counter
from datetime import datetime, timezone

from common.auth import AuthManager
from common.catalog_utils import CatalogExplorer
from common.config import MigrationConfig
from common.tracking import TrackingManager

# COMMAND ----------


def _is_notebook() -> bool:
    try:
        _ = dbutils  # type: ignore[name-defined]  # noqa: F821
        return True
    except NameError:
        return False


# COMMAND ----------


def run(dbutils, spark):  # noqa: D103
    config = MigrationConfig.from_workspace_file()
    auth = AuthManager(config, dbutils)
    tracker = TrackingManager(spark, config)
    explorer = CatalogExplorer(spark, auth)

    tracker.init_tracking_tables()

    inventory: list[dict] = []
    databases = explorer.list_hive_databases()
    print(f"Discovered {len(databases)} Hive database(s): {databases}")

    for database in databases:
        now = datetime.now(tz=timezone.utc)

        # --- Tables and views ---
        tables = explorer.classify_hive_tables(database)
        for tbl in tables:
            row_count = 0
            size_bytes = 0
            if tbl["object_type"] == "hive_table":
                try:
                    row_count = explorer.get_table_row_count(tbl["fqn"])
                except Exception:  # noqa: BLE001
                    pass
                try:
                    size_bytes = explorer.get_table_size_bytes(tbl["fqn"])
                except Exception:  # noqa: BLE001
                    pass

            inventory.append({
                "object_name": tbl["fqn"],
                "object_type": tbl["object_type"],
                "catalog_name": "hive_metastore",
                "schema_name": database,
                "data_category": tbl["data_category"],
                "table_type": tbl["table_type"],
                "provider": tbl["provider"],
                "storage_location": tbl["storage_location"],
                "row_count": row_count,
                "size_bytes": size_bytes,
                "discovered_at": now,
            })

        # --- Functions ---
        for func_fqn in explorer.list_hive_functions(database):
            inventory.append({
                "object_name": func_fqn,
                "object_type": "hive_function",
                "catalog_name": "hive_metastore",
                "schema_name": database,
                "data_category": "hive_function",
                "table_type": "",
                "provider": "",
                "storage_location": "",
                "row_count": 0,
                "size_bytes": 0,
                "discovered_at": now,
            })

    print(f"\nTotal Hive objects discovered: {len(inventory)}")

    if inventory:
        from pyspark.sql.types import (
            LongType, StringType, StructField, StructType, TimestampType,
        )
        schema = StructType([
            StructField("object_name", StringType(), True),
            StructField("object_type", StringType(), True),
            StructField("catalog_name", StringType(), True),
            StructField("schema_name", StringType(), True),
            StructField("data_category", StringType(), True),
            StructField("table_type", StringType(), True),
            StructField("provider", StringType(), True),
            StructField("storage_location", StringType(), True),
            StructField("row_count", LongType(), True),
            StructField("size_bytes", LongType(), True),
            StructField("discovered_at", TimestampType(), True),
        ])
        df = spark.createDataFrame(inventory, schema=schema)
        df.write.mode("overwrite").saveAsTable(  # type: ignore[attr-defined]
            f"{config.tracking_catalog}.{config.tracking_schema}.hive_discovery_inventory"
        )
        print("Hive discovery inventory written to tracking table.")
    else:
        print("No Hive objects discovered.")

    # Summary by category
    cat_counts = Counter(obj["data_category"] for obj in inventory)
    print(f"\n{'Category':<30} {'Count':>8}")
    print("-" * 40)
    for cat, count in sorted(cat_counts.items()):
        print(f"{cat:<30} {count:>8}")
    print("-" * 40)
    print(f"{'TOTAL':<30} {len(inventory):>8}")

    return inventory


# COMMAND ----------

if _is_notebook():
    run(dbutils, spark)  # type: ignore[name-defined]  # noqa: F821
