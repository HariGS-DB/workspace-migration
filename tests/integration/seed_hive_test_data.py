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
