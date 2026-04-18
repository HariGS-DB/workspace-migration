# Databricks notebook source

# COMMAND ----------

# Bootstrap: put the bundle's `src/` dir on sys.path so `from common...` imports resolve
import sys  # noqa: E402
_ctx = dbutils.notebook.entry_point.getDbutils().notebook().getContext()  # noqa: F821
_nb = _ctx.notebookPath().get()
_src = "/Workspace" + _nb.split("/files/")[0] + "/files/src"
if _src not in sys.path:
    sys.path.insert(0, _src)

# COMMAND ----------

# Discovery: scan source workspace catalogs/schemas and build a full inventory of UC objects.

import contextlib
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
    config = MigrationConfig.from_job_params(dbutils)
    auth = AuthManager(config, dbutils)
    tracker = TrackingManager(spark, config)
    explorer = CatalogExplorer(spark, auth)


    tracker.init_tracking_tables()


    inventory: list[dict] = []
    dlt_count = 0

    catalogs = explorer.list_catalogs(filter_list=config.catalog_filter or None)
    print(f"Discovered {len(catalogs)} catalog(s): {catalogs}")


    for catalog in catalogs:
        schemas = explorer.list_schemas(catalog)
        if config.schema_filter:
            schemas = [s for s in schemas if s in config.schema_filter]
        print(f"  Catalog '{catalog}': {len(schemas)} schema(s)")

        for schema in schemas:
            now = datetime.now(tz=timezone.utc)

            # --- Tables and views ---
            tables = explorer.classify_tables(catalog, schema)
            for tbl in tables:
                fqn = tbl["fqn"]
                obj_type = tbl["object_type"]

                if obj_type == "view":
                    is_dlt, pipeline_id = False, None
                else:
                    is_dlt, pipeline_id = explorer.detect_dlt_managed(fqn)
                    if is_dlt:
                        dlt_count += 1

                row_count = 0
                size_bytes = 0
                create_stmt = ""

                if obj_type != "view":
                    with contextlib.suppress(Exception):
                        row_count = explorer.get_table_row_count(fqn)
                    with contextlib.suppress(Exception):
                        size_bytes = explorer.get_table_size_bytes(fqn)

                with contextlib.suppress(Exception):
                    create_stmt = explorer.get_create_statement(fqn)

                inventory.append(
                    {
                        "object_name": fqn,
                        "object_type": obj_type,
                        "catalog_name": catalog,
                        "schema_name": schema,
                        "row_count": row_count,
                        "size_bytes": size_bytes,
                        "is_dlt_managed": is_dlt,
                        "pipeline_id": pipeline_id,
                        "create_statement": create_stmt,
                        "discovered_at": now,
                    }
                )

            # --- Functions ---
            functions = explorer.list_functions(catalog, schema)
            for func_fqn in functions:
                ddl = ""
                with contextlib.suppress(Exception):
                    ddl = explorer.get_function_ddl(func_fqn)

                inventory.append(
                    {
                        "object_name": func_fqn,
                        "object_type": "function",
                        "catalog_name": catalog,
                        "schema_name": schema,
                        "row_count": 0,
                        "size_bytes": 0,
                        "is_dlt_managed": False,
                        "pipeline_id": None,
                        "create_statement": ddl,
                        "discovered_at": now,
                    }
                )

            # --- Volumes ---
            volumes = explorer.list_volumes(catalog, schema)
            for vol in volumes:
                inventory.append(
                    {
                        "object_name": vol["fqn"],
                        "object_type": "volume",
                        "catalog_name": catalog,
                        "schema_name": schema,
                        "row_count": 0,
                        "size_bytes": 0,
                        "is_dlt_managed": False,
                        "pipeline_id": None,
                        "create_statement": "",
                        "discovered_at": now,
                    }
                )


    print(f"\nTotal objects discovered: {len(inventory)}")

    if inventory:
        from pyspark.sql.types import (
            BooleanType, LongType, StringType, StructField, StructType, TimestampType,
        )
        schema = StructType([
            StructField("object_name", StringType(), True),
            StructField("object_type", StringType(), True),
            StructField("catalog_name", StringType(), True),
            StructField("schema_name", StringType(), True),
            StructField("row_count", LongType(), True),
            StructField("size_bytes", LongType(), True),
            StructField("is_dlt_managed", BooleanType(), True),
            StructField("pipeline_id", StringType(), True),
            StructField("create_statement", StringType(), True),
            StructField("discovered_at", TimestampType(), True),
        ])
        df = spark.createDataFrame(inventory, schema=schema)
        tracker.write_discovery_inventory(df)
        print("Discovery inventory written to tracking table.")
    else:
        print("WARNING: No objects discovered. Check catalog/schema filters.")


    # Print summary by object type
    type_counts = Counter(obj["object_type"] for obj in inventory)
    print(f"\n{'Object Type':<20} {'Count':>8}")
    print("-" * 30)
    for obj_type, count in sorted(type_counts.items()):
        print(f"{obj_type:<20} {count:>8}")
    print("-" * 30)
    print(f"{'TOTAL':<20} {len(inventory):>8}")

    if dlt_count > 0:
        print(f"\n** {dlt_count} DLT-managed table(s) detected. These require special handling during migration. **")

    return inventory


# COMMAND ----------

if _is_notebook():
    run(dbutils, spark)  # type: ignore[name-defined]  # noqa: F821
