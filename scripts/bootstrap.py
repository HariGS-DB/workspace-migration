# Databricks notebook source

# COMMAND ----------

from __future__ import annotations  # noqa: E402
# Bootstrap: seed /Shared/cp_migration/config.yaml from the shipped template
# on first deploy. Idempotent — does nothing if the config file already exists.
import os
import shutil
import sys

CONFIG_DIR = "/Workspace/Shared/cp_migration"
CONFIG_PATH = f"{CONFIG_DIR}/config.yaml"

# The bundle deploys itself into /Shared/cp_migration/${bundle.target}/files,
# so the template path is derived from this notebook's own location.
try:
    _ctx = dbutils.notebook.entry_point.getDbutils().notebook().getContext()  # noqa: F821
    _nb = _ctx.notebookPath().get()
    # _nb looks like: /Shared/cp_migration/<target>/files/scripts/bootstrap
    TEMPLATE_PATH = "/Workspace" + _nb.split("/scripts/")[0] + "/config.template.yaml"
except NameError:
    print("Not running under a Databricks notebook — exiting.")
    sys.exit(0)

# COMMAND ----------

if os.path.exists(CONFIG_PATH):
    print(f"✅ Config already exists at {CONFIG_PATH} — nothing to do.")
    print(f"   Edit the file in the Databricks UI, then run the migration workflows.")
else:
    os.makedirs(CONFIG_DIR, exist_ok=True)
    shutil.copyfile(TEMPLATE_PATH, CONFIG_PATH)
    print(f"🎉 Created {CONFIG_PATH} from template.")
    print()
    print("Next steps:")
    print(f"  1. Open {CONFIG_PATH} in the Databricks UI")
    print("     (Workspace → Shared → cp_migration → config.yaml)")
    print("  2. Fill in source_workspace_url, target_workspace_url, spn_client_id, etc.")
    print("  3. Run the `pre_check` workflow to validate.")
