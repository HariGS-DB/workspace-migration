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
from common.auth import AuthManager  # noqa: E402
from common.sql_utils import execute_and_poll, find_warehouse  # noqa: E402

config = MigrationConfig.from_workspace_file()
spark.sql(f"DROP CATALOG IF EXISTS `{config.hive_target_catalog}` CASCADE")  # noqa: F821

# Also drop hive_target_catalog on TARGET — hive migration creates it
# there too and a stale copy breaks the next run.
try:
    auth = AuthManager(config, dbutils)  # noqa: F821
    wh_id = find_warehouse(auth)
    res = execute_and_poll(
        auth, wh_id, f"DROP CATALOG IF EXISTS `{config.hive_target_catalog}` CASCADE"
    )
    print(f"Target drop `{config.hive_target_catalog}`: {res.get('state')}")
except Exception as _exc:  # noqa: BLE001
    print(f"Target hive catalog cleanup skipped: {_exc}")

# COMMAND ----------

# Restore the pre-test config.yaml (setup_test_config saved a backup at
# the start of the workflow).

import os  # noqa: E402
import shutil  # noqa: E402
from common.config import _resolve_bundle_config_path  # type: ignore[import-not-found]  # noqa: E402

config_path = _resolve_bundle_config_path()
backup_path = config_path + ".pre-integration-test.bak"
if os.path.exists(backup_path):
    shutil.move(backup_path, config_path)
    print(f"Restored {config_path} from {backup_path}.")
else:
    print("No pre-integration-test backup found; config.yaml left as-is.")

print("Hive teardown complete.")
