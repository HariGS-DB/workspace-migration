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

# End-to-end integration test: verify migration tracking table shows all objects validated.

from common.config import MigrationConfig
from common.tracking import TrackingManager

# COMMAND ----------

config = MigrationConfig.from_job_params(dbutils)  # noqa: F821
tracker = TrackingManager(spark, config)  # noqa: F821

# COMMAND ----------

status_df = tracker.get_latest_migration_status()
total = status_df.count()
print(f"Total migrated objects: {total}")
assert total > 0, "No migration status records found -- migration did not run."

error_messages: list[str] = []

# Report per-status breakdown
status_counts = {row["status"]: row["n"] for row in status_df.groupBy("status").count().withColumnRenamed("count", "n").collect()}
print(f"Status breakdown: {status_counts}")

# Any explicit failures are test failures
failures_df = status_df.filter("status IN ('failed','validation_failed')")
if failures_df.count() > 0:
    for row in failures_df.select("object_name", "object_type", "status", "error_message").collect():
        msg = f"{row.object_type} '{row.object_name}' [{row.status}]: {row.error_message}"
        error_messages.append(msg)
        print(f"FAILURE: {msg}")

# Verify managed_orders was migrated with matching row count (recorded by worker)
managed_rows = status_df.filter("object_type = 'managed_table'").collect()
if not managed_rows:
    error_messages.append("No managed_table records in migration_status.")
else:
    for row in managed_rows:
        if row["source_row_count"] != row["target_row_count"]:
            error_messages.append(
                f"Row count mismatch for {row['object_name']}: src={row['source_row_count']} tgt={row['target_row_count']}"
            )
        else:
            print(f"{row['object_name']}: rows={row['source_row_count']} validated")

# COMMAND ----------

if error_messages:
    detail = "\n".join(error_messages)
    raise AssertionError(f"Integration test failed with {len(error_messages)} error(s):\n{detail}")

print("All integration tests passed.")
