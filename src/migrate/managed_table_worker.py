# Databricks notebook source

# COMMAND ----------
# Managed Table Worker: deep-clones managed tables from delta share to target.

from __future__ import annotations

import json
import logging
import time
from concurrent.futures import ThreadPoolExecutor, as_completed

from common.auth import AuthManager
from common.catalog_utils import CatalogExplorer
from common.config import MigrationConfig
from common.sql_utils import execute_and_poll, find_warehouse
from common.tracking import TrackingManager
from common.validation import Validator

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("managed_table_worker")

MAX_WORKERS = 4
SHARE_NAME = "cp_migration_share"


# COMMAND ----------


def _is_notebook() -> bool:
    """Return True when running inside a Databricks notebook."""
    try:
        _ = dbutils  # type: ignore[name-defined]  # noqa: F821
        return True
    except NameError:
        return False


# COMMAND ----------
# Clone a single managed table


def clone_table(
    table_info: dict,
    *,
    config: MigrationConfig,
    auth: AuthManager,
    tracker: TrackingManager,
    validator: Validator,
    wh_id: str,
    share_name: str,
) -> dict:
    """Deep clone a single managed table from delta share to target."""
    obj_name = table_info["object_name"]
    parts = obj_name.strip("`").split("`.`")
    if len(parts) != 3:
        return {
            "object_name": obj_name,
            "object_type": "managed_table",
            "status": "failed",
            "error_message": f"Malformed FQN: {obj_name}",
            "duration_seconds": 0.0,
        }

    _catalog, schema, table = parts
    target_fqn = obj_name  # same FQN on target
    share_table_ref = f"{schema}.{table}"

    # Record in-progress
    tracker.append_migration_status(
        [
            {
                "object_name": obj_name,
                "object_type": "managed_table",
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

    sql = f"CREATE OR REPLACE TABLE {target_fqn} DEEP CLONE delta_sharing.`{share_name}`.`{share_table_ref}`"
    logger.info("Executing DEEP CLONE for %s", obj_name)

    if config.dry_run:
        duration = time.time() - start
        logger.info("[DRY RUN] Would execute: %s", sql)
        return {
            "object_name": obj_name,
            "object_type": "managed_table",
            "status": "skipped",
            "error_message": "dry_run",
            "duration_seconds": duration,
        }

    result = execute_and_poll(auth, wh_id, sql)
    duration = time.time() - start

    if result["state"] != "SUCCEEDED":
        return {
            "object_name": obj_name,
            "object_type": "managed_table",
            "status": "failed",
            "error_message": result.get("error", result["state"]),
            "duration_seconds": duration,
        }

    # Validate row count
    try:
        validation = validator.validate_row_count(obj_name, target_fqn)
        status = "validated" if validation["match"] else "validation_failed"
        return {
            "object_name": obj_name,
            "object_type": "managed_table",
            "status": status,
            "error_message": None
            if validation["match"]
            else (f"Row count mismatch: source={validation['source_count']}, target={validation['target_count']}"),
            "source_row_count": validation["source_count"],
            "target_row_count": validation["target_count"],
            "duration_seconds": duration,
        }
    except Exception as exc:  # noqa: BLE001
        return {
            "object_name": obj_name,
            "object_type": "managed_table",
            "status": "validation_failed",
            "error_message": f"Validation error: {exc}",
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

    source_explorer = CatalogExplorer(spark_session, auth)
    target_explorer = CatalogExplorer(spark_session, auth)
    validator = Validator(source_explorer, target_explorer)

    # Parse batch from widget
    batch_json = dbutils.widgets.get("batch")
    batch: list[dict] = json.loads(batch_json)
    logger.info("Received batch of %d managed tables.", len(batch))

    wh_id = find_warehouse(auth)

    # COMMAND ----------
    # Process batch with thread pool

    results: list[dict] = []

    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as pool:
        futures = {
            pool.submit(
                clone_table,
                tbl,
                config=config,
                auth=auth,
                tracker=tracker,
                validator=validator,
                wh_id=wh_id,
                share_name=SHARE_NAME,
            ): tbl
            for tbl in batch
        }
        for future in as_completed(futures):
            tbl_info = futures[future]
            try:
                res = future.result()
            except Exception as exc:  # noqa: BLE001
                res = {
                    "object_name": tbl_info["object_name"],
                    "object_type": "managed_table",
                    "status": "failed",
                    "error_message": str(exc),
                    "duration_seconds": 0.0,
                }
            results.append(res)
            logger.info("Table %s -> %s", res["object_name"], res["status"])

    # COMMAND ----------
    # Record final statuses

    tracker.append_migration_status(results)
    logger.info(
        "Managed table worker complete. %d succeeded, %d failed.",
        sum(1 for r in results if r["status"] == "validated"),
        sum(1 for r in results if r["status"] in ("failed", "validation_failed")),
    )


# COMMAND ----------

if _is_notebook():
    run(dbutils, spark)  # type: ignore[name-defined]  # noqa: F821
