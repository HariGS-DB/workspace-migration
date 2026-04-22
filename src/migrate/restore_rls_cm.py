# Databricks notebook source

# COMMAND ----------

from __future__ import annotations  # noqa: E402

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
# Restore RLS / CM (Phase 3 P.1).
#
# Reads rls_cm_manifest for rows with restored_at=NULL and re-applies each
# policy on the source workspace. Skipped entirely unless
# rls_cm_strategy=drop_and_restore + rls_cm_maintenance_window_confirmed=true.
# Runs idempotently — a crash here leaves manifest rows with restored_at=NULL
# so the next execution picks them up.

import logging

from common.auth import AuthManager
from common.config import MigrationConfig
from common.sql_utils import find_warehouse
from common.tracking import TrackingManager
from migrate.rls_cm import reapply_policies_to_source

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("restore_rls_cm")


def _is_notebook() -> bool:
    try:
        _ = dbutils  # type: ignore[name-defined]  # noqa: F821
        return True
    except NameError:
        return False


def run(dbutils, spark) -> None:
    config = MigrationConfig.from_workspace_file()
    if not config.include_uc:
        logger.info("Skipping restore_rls_cm: scope.include_uc=false.")
        return
    if config.rls_cm_strategy != "drop_and_restore":
        logger.info(
            "Skipping restore_rls_cm: rls_cm_strategy=%r (not drop_and_restore).",
            config.rls_cm_strategy,
        )
        return

    auth = AuthManager(config, dbutils)
    tracker = TrackingManager(spark, config)
    tracker.init_rls_cm_manifest()
    wh_id = find_warehouse(auth)

    results = reapply_policies_to_source(
        spark, auth, tracker, wh_id=wh_id, dry_run=config.dry_run,
    )

    # Emit a single migration_status summary row so operators can see the
    # restore worker ran; individual row status is captured in the manifest.
    validated = sum(1 for r in results if r["status"] == "validated")
    failed = sum(1 for r in results if r["status"] == "failed")
    summary = {
        "object_name": "RLS_CM_RESTORE",
        "object_type": "rls_cm_restore",
        "status": "validated" if failed == 0 else "validation_failed",
        "error_message": (
            f"Reapplied {validated} policies, {failed} failed. "
            f"Unrestored manifest rows remain: {len(tracker.get_unrestored_manifest())}"
        ),
        "duration_seconds": 0.0,
    }
    tracker.append_migration_status([summary])
    logger.info(summary["error_message"])


if _is_notebook():
    run(dbutils, spark)  # type: ignore[name-defined]  # noqa: F821
