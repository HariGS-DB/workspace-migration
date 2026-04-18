# Databricks notebook source

# COMMAND ----------

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

from databricks.sdk import WorkspaceClient

# Drop UC test catalogs + tracking test schema
spark.sql("DROP CATALOG IF EXISTS integration_test_tgt CASCADE")  # noqa: F821
spark.sql("DROP CATALOG IF EXISTS integration_test_src CASCADE")  # noqa: F821
spark.sql("DROP SCHEMA IF EXISTS migration_tracking.cp_migration_test CASCADE")  # noqa: F821
print("Dropped UC test catalogs.")

# COMMAND ----------

# Clean up Delta Share created during UC migration
w = WorkspaceClient()
for share_name in ("cp_migration_share",):
    try:
        w.shares.delete(share_name)
        print(f"Deleted share '{share_name}'.")
    except Exception as e:  # noqa: BLE001
        print(f"Share '{share_name}' cleanup skipped: {e}")

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

print("UC teardown complete.")
