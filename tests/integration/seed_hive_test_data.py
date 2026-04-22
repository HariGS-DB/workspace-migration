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
    pass

# COMMAND ----------

# Seed Hive test data: managed DBFS-root table + view + function under
# hive_metastore.integration_test_hive. Also grants the migration SPN the
# target-side permissions needed to register UC external tables during the
# DBFS-root migration path.

from common.auth import AuthManager  # noqa: E402
from common.config import MigrationConfig
from common.sql_utils import execute_and_poll, find_warehouse

config = MigrationConfig.from_workspace_file()

# COMMAND ----------

spark.sql("CREATE DATABASE IF NOT EXISTS hive_metastore.integration_test_hive")  # noqa: F821

# Managed Delta table on DBFS root — default for Hive CREATE TABLE
spark.sql(  # noqa: F821
    """
    CREATE TABLE IF NOT EXISTS hive_metastore.integration_test_hive.managed_orders (
        order_id INT,
        amount DOUBLE
    ) USING DELTA
    """
)
spark.sql(  # noqa: F821
    """
    INSERT OVERWRITE TABLE hive_metastore.integration_test_hive.managed_orders VALUES
        (1, 10.0),
        (2, 20.0),
        (3, 30.0)
    """
)

spark.sql(  # noqa: F821
    """
    CREATE OR REPLACE VIEW hive_metastore.integration_test_hive.big_orders AS
    SELECT * FROM hive_metastore.integration_test_hive.managed_orders WHERE amount > 15
    """
)

spark.sql(  # noqa: F821
    """
    CREATE OR REPLACE FUNCTION hive_metastore.integration_test_hive.triple(x DOUBLE)
    RETURNS DOUBLE
    RETURN x * 3
    """
)

# COMMAND ----------

# --- Hive external + managed non-DBFS fixtures ---
#
# Phase 2 has dedicated workers for these categories:
#   hive_external            — EXTERNAL table on any storage
#   hive_managed_nondbfs     — MANAGED table whose location is off DBFS root
#                              (legacy pattern; migrate preserving MANAGED on target)
#
# Before this fixture, both workers ran with empty input on every
# integration test — zero coverage. We seed one row per category here
# so both workflows exercise at least the happy path.
#
# The ADLS path reuses the external location used by DBFS-root
# migration, gated on the same config fields.

_hive_external_location: str | None = None
_hive_nondbfs_location: str | None = None
if config.migrate_hive_dbfs_root and config.hive_dbfs_target_path:
    _base = config.hive_dbfs_target_path.rstrip("/")
    _hive_external_location = f"{_base}/hive_external_invoices"
    _hive_nondbfs_location = f"{_base}/hive_nondbfs_sales"

_has_hive_external = False
_has_hive_nondbfs = False

if _hive_external_location:
    try:
        # EXTERNAL table — CREATE TABLE ... LOCATION makes it external in
        # hive_metastore (the LOCATION clause flips table_type to EXTERNAL).
        spark.sql(  # noqa: F821
            f"""
            CREATE TABLE IF NOT EXISTS hive_metastore.integration_test_hive.external_invoices (
                invoice_id INT,
                amount DOUBLE
            )
            USING DELTA
            LOCATION '{_hive_external_location}'
            """
        )
        spark.sql(  # noqa: F821
            """
            INSERT OVERWRITE TABLE hive_metastore.integration_test_hive.external_invoices VALUES
                (101, 100.0),
                (102, 200.0)
            """
        )
        _has_hive_external = True
        print(f"Created Hive external table at {_hive_external_location}.")
    except Exception as _exc:  # noqa: BLE001
        print(f"Skipped Hive external seed: {_exc}")

if _hive_nondbfs_location:
    try:
        # MANAGED non-DBFS — in hive_metastore, a managed table with an
        # explicit LOCATION off DBFS root. Covers the hive_managed_nondbfs
        # worker's path (legacy mount / non-default cluster config).
        spark.sql(  # noqa: F821
            f"""
            CREATE TABLE IF NOT EXISTS hive_metastore.integration_test_hive.nondbfs_sales (
                sale_id INT,
                amount DOUBLE
            )
            USING DELTA
            LOCATION '{_hive_nondbfs_location}'
            """
        )
        spark.sql(  # noqa: F821
            """
            INSERT OVERWRITE TABLE hive_metastore.integration_test_hive.nondbfs_sales VALUES
                (201, 50.0),
                (202, 75.0),
                (203, 90.0)
            """
        )
        _has_hive_nondbfs = True
        print(f"Created Hive managed non-DBFS table at {_hive_nondbfs_location}.")
    except Exception as _exc:  # noqa: BLE001
        print(f"Skipped Hive managed-nondbfs seed: {_exc}")

dbutils.jobs.taskValues.set(  # type: ignore[name-defined]  # noqa: F821
    key="has_hive_external", value="true" if _has_hive_external else "false"
)
dbutils.jobs.taskValues.set(  # type: ignore[name-defined]  # noqa: F821
    key="has_hive_nondbfs", value="true" if _has_hive_nondbfs else "false"
)

# COMMAND ----------

# --- Hive grants fixture ---
# Legacy hive_metastore supports GRANT / REVOKE on tables. Seed a grant on
# managed_orders so hive_grants_worker has something to migrate and the
# assertion in test_hive_end_to_end can verify it replays on target.

_has_hive_grant = False
try:
    spark.sql(  # noqa: F821
        "GRANT SELECT ON SCHEMA hive_metastore.integration_test_hive TO `account users`"
    )
    _has_hive_grant = True
    print("Granted SELECT on Hive schema to `account users`.")
except Exception as _exc:  # noqa: BLE001
    # Some clusters have Hive ACLs disabled (table ACLs on legacy); tolerate.
    print(f"Skipped Hive grant seed (table ACLs may be disabled): {_exc}")
dbutils.jobs.taskValues.set(  # type: ignore[name-defined]  # noqa: F821
    key="has_hive_grant", value="true" if _has_hive_grant else "false"
)

# COMMAND ----------

# Grant the SPN CREATE EXTERNAL TABLE on the target's external location that
# hosts hive_dbfs_target_path. Without this, the DBFS-root worker's
# CREATE TABLE ... LOCATION '<hive_dbfs_target_path>/...' fails with
# PERMISSION_DENIED on the target side.
if config.migrate_hive_dbfs_root and config.hive_dbfs_target_path and config.spn_client_id:
    auth = AuthManager(config, dbutils)  # noqa: F821
    wh_id = find_warehouse(auth)
    # Find the external location covering hive_dbfs_target_path on target.
    locs = list(auth.target_client.external_locations.list())
    matching = [loc for loc in locs if config.hive_dbfs_target_path.startswith(loc.url)]
    if matching:
        loc_name = matching[0].name
        grant_sql = (
            f"GRANT CREATE EXTERNAL TABLE, READ FILES, WRITE FILES "
            f"ON EXTERNAL LOCATION `{loc_name}` TO `{config.spn_client_id}`"
        )
        res = execute_and_poll(auth, wh_id, grant_sql)
        if res["state"] == "SUCCEEDED":
            print(f"Granted CREATE EXTERNAL TABLE on '{loc_name}' to SPN.")
        else:
            print(f"WARNING: grant on external location '{loc_name}' failed: {res.get('error')}")
    else:
        print(
            f"WARNING: No external location on target covers {config.hive_dbfs_target_path!r}. "
            f"DBFS-root migration will likely fail with PERMISSION_DENIED."
        )

# COMMAND ----------

print(
    "Hive seed data created successfully: "
    "managed_orders (DBFS-root), big_orders (view), triple (function). "
    f"DBFS-root migration will {'run' if config.migrate_hive_dbfs_root else 'be SKIPPED (migrate_hive_dbfs_root=false)'}."
)
