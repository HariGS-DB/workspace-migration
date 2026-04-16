# Databricks notebook source

# COMMAND ----------

# Teardown: clean up all integration test artifacts (catalogs, schemas, shares).

from databricks.sdk import WorkspaceClient

# COMMAND ----------

# Drop test catalogs and tracking schema
spark.sql("DROP CATALOG IF EXISTS integration_test_tgt CASCADE")  # noqa: F821
spark.sql("DROP CATALOG IF EXISTS integration_test_src CASCADE")  # noqa: F821
spark.sql("DROP SCHEMA IF EXISTS migration_tracking.cp_migration_test CASCADE")  # noqa: F821

print("Dropped test catalogs and tracking schema.")

# COMMAND ----------

# Clean up delta shares via SDK
w = WorkspaceClient()

share_names = ["cp_migration_share"]
for share_name in share_names:
    try:
        w.shares.delete(share_name)
        print(f"Deleted share '{share_name}'.")
    except Exception as e:  # noqa: BLE001
        print(f"Share '{share_name}' cleanup skipped: {e}")

# Clean up recipients matching integration test pattern
try:
    for recipient in w.recipients.list():
        if recipient.name and "cp_migration_recipient_" in recipient.name:
            try:
                w.recipients.delete(recipient.name)
                print(f"Deleted recipient '{recipient.name}'.")
            except Exception as e:  # noqa: BLE001
                print(f"Recipient '{recipient.name}' cleanup skipped: {e}")
except Exception as e:  # noqa: BLE001
    print(f"Recipient listing skipped: {e}")

# COMMAND ----------

print("Teardown complete.")
