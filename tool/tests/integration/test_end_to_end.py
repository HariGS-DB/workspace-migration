# Databricks notebook source

# COMMAND ----------

# End-to-end integration test: verify that migration completed and data landed correctly
# on the target catalog.

from common.config import MigrationConfig
from common.tracking import TrackingManager

# COMMAND ----------

config = MigrationConfig.from_job_params(dbutils)  # noqa: F821
tracker = TrackingManager(spark, config)  # noqa: F821

# COMMAND ----------

# Get the latest migration status for all objects
status_df = tracker.get_latest_migration_status()
total = status_df.count()
print(f"Total migrated objects: {total}")

assert total > 0, "No migration status records found -- migration did not run."

# COMMAND ----------

# Check for failures
failures_df = status_df.filter("status = 'failed'")
failure_count = failures_df.count()

error_messages: list[str] = []

if failure_count > 0:
    failed_rows = failures_df.select("object_name", "object_type", "error_message").collect()
    for row in failed_rows:
        msg = f"{row.object_type} '{row.object_name}': {row.error_message}"
        error_messages.append(msg)
        print(f"FAILURE: {msg}")

# COMMAND ----------

# Verify managed_orders row count on target
target_catalog = "integration_test_tgt"
target_count = (
    spark.sql(  # noqa: F821
        f"SELECT COUNT(*) AS cnt FROM {target_catalog}.test_schema.managed_orders"
    )
    .first()
    .cnt
)
print(f"Target managed_orders row count: {target_count}")

if target_count != 2:
    error_messages.append(f"managed_orders row count mismatch: expected 2, got {target_count}")

# COMMAND ----------

# Verify view exists on target
try:
    spark.sql(  # noqa: F821
        f"SELECT * FROM {target_catalog}.test_schema.high_value_orders LIMIT 1"
    )
    print("View high_value_orders exists on target.")
except Exception as e:
    error_messages.append(f"View high_value_orders not found on target: {e}")

# COMMAND ----------

# Verify function works on target
try:
    result = (
        spark.sql(  # noqa: F821
            f"SELECT {target_catalog}.test_schema.double_amount(5.0) AS val"
        )
        .first()
        .val
    )
    print(f"double_amount(5.0) = {result}")
    if result != 10.0:
        error_messages.append(f"double_amount returned {result}, expected 10.0")
except Exception as e:
    error_messages.append(f"Function double_amount failed on target: {e}")

# COMMAND ----------

# Final verdict
if error_messages:
    detail = "\n".join(error_messages)
    raise AssertionError(f"Integration test failed with {len(error_messages)} error(s):\n{detail}")

print("All integration tests passed.")
