# Databricks notebook source

# COMMAND ----------

# Setup test config.
#
# Overrides the workspace copy of config.yaml with values appropriate for
# the invoking integration workflow (UC vs Hive). Back up the file first
# so ``teardown`` can restore it — we don't want a failed integration
# run to leave config.yaml in a "test toggles on" state that an operator
# would then trip over during a real migration.
#
# Each integration workflow passes its desired toggles as task
# parameters — that way the repo ships ``config.yaml`` with neutral
# placeholder defaults (placeholders for URLs + SPN + paths), and the
# workflows themselves carry the test-specific behavior flags.
#
# Parameters consumed (all strings; "true"/"false" for booleans):
#   include_uc               — scope.include_uc
#   include_hive             — scope.include_hive
#   iceberg_strategy         — "" or "ddl_replay"
#   rls_cm_strategy          — "" (skip) only; drop_and_restore isn't
#                              implemented yet so we refuse here to keep
#                              operators from setting it accidentally.
#   migrate_hive_dbfs_root   — "true" / "false"
#   hive_dbfs_target_path    — ADLS URL for Hive DBFS-root migration; may
#                              be empty when ``migrate_hive_dbfs_root`` is
#                              false. Typically provided by an operator-
#                              set BUNDLE_VAR or left at the workspace
#                              config.yaml's value for subsequent reads.
#   batch_size               — integer ≥ 1 overriding batch_size in
#                              config.yaml. Empty → leave existing value.
#                              Hive integration passes "10" so its 12-
#                              table fixture exercises > 1 batch.
#   catalog_filter           — comma-separated allow-list of UC catalogs
#                              to discover. Empty → unchanged. Hive
#                              integration passes "integration_test_src"
#                              so UC discovery ignores the parallel
#                              ``integration_test_hive_ucref`` fixture
#                              seeded for the cross-catalog view test.

import shutil

# COMMAND ----------
# Bootstrap so we can reuse MigrationConfig's resolver for the config
# path (keeps "where does config.yaml live" in one place).
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

import yaml  # noqa: E402

from common.config import _resolve_bundle_config_path  # type: ignore[import-not-found]  # noqa: E402

config_path = _resolve_bundle_config_path()
backup_path = config_path + ".pre-integration-test.bak"

# Back up the current config exactly once. If the backup already exists
# (e.g. a previous run crashed before teardown), keep the older backup
# — it represents the pre-test "real" config.
import os  # noqa: E402

if not os.path.exists(backup_path):
    shutil.copy2(config_path, backup_path)
    print(f"Backed up {config_path} -> {backup_path}")
else:
    print(f"Backup already exists at {backup_path}; reusing.")

# COMMAND ----------

dbutils.widgets.text("include_uc", "true")  # noqa: F821
dbutils.widgets.text("include_hive", "false")  # noqa: F821
dbutils.widgets.text("iceberg_strategy", "")  # noqa: F821
dbutils.widgets.text("rls_cm_strategy", "")  # noqa: F821
dbutils.widgets.text("migrate_hive_dbfs_root", "false")  # noqa: F821
dbutils.widgets.text("hive_dbfs_target_path", "")  # noqa: F821
dbutils.widgets.text("batch_size", "")  # noqa: F821
dbutils.widgets.text("catalog_filter", "")  # noqa: F821


def _get_bool(key: str, default: str) -> bool:
    return str(dbutils.widgets.get(key) or default).strip().lower() == "true"  # type: ignore[name-defined]  # noqa: F821


def _get_str(key: str, default: str = "") -> str:
    return str(dbutils.widgets.get(key) or default).strip()  # type: ignore[name-defined]  # noqa: F821


include_uc = _get_bool("include_uc", "true")
include_hive = _get_bool("include_hive", "false")
iceberg_strategy = _get_str("iceberg_strategy", "")
rls_cm_strategy = _get_str("rls_cm_strategy", "")
migrate_hive_dbfs_root = _get_bool("migrate_hive_dbfs_root", "false")
hive_dbfs_target_path = _get_str("hive_dbfs_target_path", "")
batch_size_raw = _get_str("batch_size", "")
catalog_filter_raw = _get_str("catalog_filter", "")

# Guard against drop_and_restore here too (setup_sharing also gates it,
# but catching it at config-setup keeps us from doing a bunch of I/O
# before failing).
if rls_cm_strategy.lower() == "drop_and_restore":
    raise NotImplementedError("rls_cm_strategy='drop_and_restore' is not yet implemented — see README.")

# COMMAND ----------

# Load → override in memory → write back.
with open(config_path) as f:
    cfg = yaml.safe_load(f) or {}

scope = cfg.setdefault("scope", {})
scope["include_uc"] = include_uc
scope["include_hive"] = include_hive
cfg["iceberg_strategy"] = iceberg_strategy
cfg["rls_cm_strategy"] = rls_cm_strategy
cfg["migrate_hive_dbfs_root"] = migrate_hive_dbfs_root
if hive_dbfs_target_path:
    cfg["hive_dbfs_target_path"] = hive_dbfs_target_path
# If hive_dbfs_target_path is not provided but migrate_hive_dbfs_root is
# true, leave the existing value in place (operator-configured pre-test).
if batch_size_raw:
    try:
        cfg["batch_size"] = max(1, int(batch_size_raw))
    except ValueError as _exc:
        raise ValueError(f"batch_size must be an integer, got {batch_size_raw!r}") from _exc
if catalog_filter_raw:
    cfg["catalog_filter"] = [x.strip() for x in catalog_filter_raw.split(",") if x.strip()]

with open(config_path, "w") as f:
    yaml.safe_dump(cfg, f, sort_keys=False)

print(
    f"Overrode {config_path} for this integration test run:\n"
    f"  scope.include_uc         = {include_uc}\n"
    f"  scope.include_hive       = {include_hive}\n"
    f"  iceberg_strategy         = {iceberg_strategy!r}\n"
    f"  rls_cm_strategy          = {rls_cm_strategy!r}\n"
    f"  migrate_hive_dbfs_root   = {migrate_hive_dbfs_root}\n"
    f"  hive_dbfs_target_path    = {cfg.get('hive_dbfs_target_path', '')!r}\n"
    f"  batch_size               = {cfg.get('batch_size', '(unchanged)')}\n"
    f"  catalog_filter           = {cfg.get('catalog_filter', '(unchanged)')}\n"
)
