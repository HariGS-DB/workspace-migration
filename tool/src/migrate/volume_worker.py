# Databricks notebook source

# COMMAND ----------

from __future__ import annotations  # noqa: E402
# Bootstrap: put the bundle's `src/` dir on sys.path so `from common...` imports resolve
import sys  # noqa: E402
try:
    _ctx = dbutils.notebook.entry_point.getDbutils().notebook().getContext()  # noqa: F821
    _nb = _ctx.notebookPath().get()
    _src = "/Workspace" + _nb.split("/files/")[0] + "/files/src"
    if _src not in sys.path:
        sys.path.insert(0, _src)
except NameError:
    pass  # not running under a Databricks notebook (e.g. pytest)

# COMMAND ----------
# Volume Worker: recreates volumes on the target workspace.
# External volumes get metadata recreated; managed volumes get created empty
# and flagged for manual data copy.

import json
import logging
import time

from common.auth import AuthManager
from common.config import MigrationConfig
from common.sql_utils import execute_and_poll, find_warehouse
from common.tracking import TrackingManager

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("volume_worker")


# COMMAND ----------


def _is_notebook() -> bool:
    """Return True when running inside a Databricks notebook."""
    try:
        _ = dbutils  # type: ignore[name-defined]  # noqa: F821
        return True
    except NameError:
        return False


# COMMAND ----------
# Migrate a single volume


def migrate_volume(
    vol_info: dict,
    *,
    config: MigrationConfig,
    auth: AuthManager,
    tracker: TrackingManager,
    wh_id: str,
) -> dict:
    """Recreate a volume on the target workspace."""
    obj_name = vol_info["object_name"]
    # Volume type stored from discovery: "MANAGED" or "EXTERNAL"
    volume_type = vol_info.get("volume_type", "MANAGED").upper()

    tracker.append_migration_status(
        [
            {
                "object_name": obj_name,
                "object_type": "volume",
                "status": "in_progress",
                "error_message": None,
                "job_run_id": None,
                "task_run_id": None,
                "source_row_count": None,
                "target_row_count": None,
                "duration_seconds": None,
            }
        ]
    )

    start = time.time()

    if volume_type == "EXTERNAL":
        # External volume: get the storage location from source and recreate
        storage_location = vol_info.get("storage_location", "")
        if storage_location:
            sql = f"CREATE EXTERNAL VOLUME IF NOT EXISTS {obj_name} LOCATION '{storage_location}'"
        else:
            # Attempt to read create statement; fall back to basic create
            sql = f"CREATE EXTERNAL VOLUME IF NOT EXISTS {obj_name}"
    else:
        # Managed volume: create empty, data must be copied manually
        sql = f"CREATE VOLUME IF NOT EXISTS {obj_name}"

    if config.dry_run:
        duration = time.time() - start
        logger.info("[DRY RUN] Would execute: %s", sql)
        return {
            "object_name": obj_name,
            "object_type": "volume",
            "status": "skipped",
            "error_message": "dry_run",
            "duration_seconds": duration,
        }

    logger.info("Executing: %s", sql)
    result = execute_and_poll(auth, wh_id, sql)
    duration = time.time() - start

    if result["state"] != "SUCCEEDED":
        return {
            "object_name": obj_name,
            "object_type": "volume",
            "status": "failed",
            "error_message": result.get("error", result["state"]),
            "duration_seconds": duration,
        }

    status = "validated"
    error_message = None
    if volume_type == "MANAGED":
        status = "validated"
        error_message = "Managed volume created empty; manual data copy required."
        logger.warning("Managed volume %s created empty. Manual data copy required.", obj_name)

    return {
        "object_name": obj_name,
        "object_type": "volume",
        "status": status,
        "error_message": error_message,
        "duration_seconds": duration,
    }


# COMMAND ----------
# Notebook execution


def run(dbutils, spark) -> None:
    """Entry point when running as a Databricks notebook."""
    config = MigrationConfig.from_job_params(dbutils)
    auth = AuthManager(config, dbutils)
    spark_session = spark
    tracker = TrackingManager(spark_session, config)

    # Parse batch from widget
    batch_json = dbutils.widgets.get("batch")
    batch: list[dict] = json.loads(batch_json)
    logger.info("Received batch of %d volumes.", len(batch))

    wh_id = find_warehouse(auth)

    # Process batch sequentially (volumes are lightweight metadata operations)

    results: list[dict] = []

    for vol in batch:
        try:
            res = migrate_volume(vol, config=config, auth=auth, tracker=tracker, wh_id=wh_id)
        except Exception as exc:  # noqa: BLE001
            res = {
                "object_name": vol["object_name"],
                "object_type": "volume",
                "status": "failed",
                "error_message": str(exc),
                "duration_seconds": 0.0,
            }
        results.append(res)
        logger.info("Volume %s -> %s", res["object_name"], res["status"])

    # Record final statuses

    tracker.append_migration_status(results)
    logger.info(
        "Volume worker complete. %d succeeded, %d failed.",
        sum(1 for r in results if r["status"] == "validated"),
        sum(1 for r in results if r["status"] == "failed"),
    )


# COMMAND ----------

if _is_notebook():
    run(dbutils, spark)  # type: ignore[name-defined]  # noqa: F821
