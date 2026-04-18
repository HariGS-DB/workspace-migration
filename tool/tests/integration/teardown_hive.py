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

# Hive teardown: drop the source Hive database and the UC-target upgrade catalog.

spark.sql("DROP DATABASE IF EXISTS hive_metastore.integration_test_hive CASCADE")  # noqa: F821

from common.config import MigrationConfig
config = MigrationConfig.from_workspace_file()
spark.sql(f"DROP CATALOG IF EXISTS `{config.hive_target_catalog}` CASCADE")  # noqa: F821

print("Hive teardown complete.")
