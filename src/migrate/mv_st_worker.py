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
# MV / ST Worker.
#
# Migrates SQL-created materialized views and streaming tables via DDL replay.
# DLT-defined MVs/STs are explicitly out of scope for this tool (handled by a
# separate pipeline migration tool) and are skipped with status
# `skipped_by_pipeline_migration`.
#
# Distinguishing signal: call `w.pipelines.get(pipeline_id)` on source and
# inspect `spec.libraries`. Empty → SQL-created (auto-provisioned backing
# pipeline). Non-empty → user-owned DLT pipeline → skip.
#
# Streaming-table caveat: source state (Kafka offsets, Auto Loader
# checkpoints, Delta CDF cursors) does NOT transfer — target restarts from the
# source's current position. Pre-check surfaces this as a warning.

import json
import logging
import time

from common.auth import AuthManager
from common.config import MigrationConfig
from common.sql_utils import execute_and_poll, find_warehouse
from common.tracking import TrackingManager

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("mv_st_worker")


# COMMAND ----------


def _is_notebook() -> bool:
    """Return True when running inside a Databricks notebook."""
    try:
        _ = dbutils  # type: ignore[name-defined]  # noqa: F821
        return True
    except NameError:
        return False


# COMMAND ----------
# Detection: is this MV/ST SQL-created (in scope) or DLT-defined (skip)?


def _is_sql_created(auth: AuthManager, pipeline_id: str) -> tuple[bool, str]:
    """Return (is_sql_created, diagnostic) for a given pipeline.

    Empty ``spec.libraries`` → SQL-created (Databricks auto-provisioned the
    backing pipeline from CREATE MATERIALIZED VIEW / CREATE STREAMING TABLE).
    Non-empty → user-owned DLT pipeline (libraries point at notebook/file
    entries). Missing pipeline (404) → treat as SQL-created so DDL replay is
    attempted; the target's own REFRESH will surface issues.
    """
    source = auth.source_client
    try:
        pipeline = source.pipelines.get(pipeline_id)
    except Exception as exc:  # noqa: BLE001
        logger.warning(
            "Could not fetch pipeline %s (%s); assuming SQL-created.", pipeline_id, exc
        )
        return True, f"pipeline lookup failed: {exc}"

    spec = getattr(pipeline, "spec", None)
    libraries = getattr(spec, "libraries", None) if spec is not None else None
    if not libraries:
        return True, "empty spec.libraries"
    return False, f"non-empty libraries ({len(libraries)})"


# COMMAND ----------
# SQL-created DDL replay


def _replay_mv_st_ddl(
    obj_info: dict,
    *,
    auth: AuthManager,
    wh_id: str,
    dry_run: bool,
) -> tuple[str, str | None]:
    """Replay the CREATE statement on target and issue an initial REFRESH.

    Returns (status, error_message). ``status`` is one of ``validated`` /
    ``failed`` / ``skipped`` (for dry_run).
    """
    obj_name = obj_info["object_name"]
    obj_type = obj_info["object_type"]  # "mv" or "st"
    create_stmt = obj_info.get("create_statement") or ""
    if not create_stmt:
        return "failed", "Missing create_statement in discovery row"

    refresh_keyword = "MATERIALIZED VIEW" if obj_type == "mv" else "STREAMING TABLE"
    refresh_sql = f"REFRESH {refresh_keyword} {obj_name}"

    if dry_run:
        logger.info("[DRY RUN] Would execute: %s", create_stmt)
        logger.info("[DRY RUN] Would execute: %s", refresh_sql)
        return "skipped", "dry_run"

    logger.info("Creating %s on target: %s", obj_type, obj_name)
    result = execute_and_poll(auth, wh_id, create_stmt)
    already_existed = False
    if result["state"] != "SUCCEEDED":
        err_text = str(result.get("error", result["state"])).lower()
        # Idempotency: MV/ST CREATE does not support OR REPLACE. On retry the
        # object may already exist — skip CREATE and proceed to REFRESH so a
        # resumed run still validates state instead of marking it failed.
        if "already exists" in err_text or "already_exists" in err_text:
            already_existed = True
            logger.info("%s %s already exists on target; proceeding to REFRESH.", obj_type, obj_name)
        else:
            return "failed", result.get("error", result["state"])

    # REFRESH is best-effort — the target auto-pipeline may already have
    # started its own refresh. Log but don't fail the migration on REFRESH
    # errors; surface them in error_message for visibility.
    logger.info("Refreshing %s on target: %s", obj_type, obj_name)
    refresh = execute_and_poll(auth, wh_id, refresh_sql)
    if refresh["state"] != "SUCCEEDED":
        return (
            "validated",
            f"{'already existed on target; ' if already_existed else ''}"
            f"{'REFRESH failed' if not already_existed else 'REFRESH failed'}: "
            f"{refresh.get('error', refresh['state'])}",
        )
    return "validated", ("already existed on target" if already_existed else None)


# COMMAND ----------
# Migrate a single MV or ST


def migrate_mv_st(
    obj_info: dict,
    *,
    config: MigrationConfig,
    auth: AuthManager,
    tracker: TrackingManager,
    wh_id: str,
) -> dict:
    """Migrate one materialized view or streaming table."""
    obj_name = obj_info["object_name"]
    obj_type = obj_info["object_type"]  # "mv" or "st"
    pipeline_id = obj_info.get("pipeline_id")

    tracker.append_migration_status([{
        "object_name": obj_name,
        "object_type": obj_type,
        "status": "in_progress",
        "error_message": None,
        "job_run_id": None,
        "task_run_id": None,
        "source_row_count": None,
        "target_row_count": None,
        "duration_seconds": None,
    }])

    start = time.time()

    if not pipeline_id:
        return {
            "object_name": obj_name,
            "object_type": obj_type,
            "status": "failed",
            "error_message": "Missing pipeline_id in discovery row (MV/ST always has one)",
            "duration_seconds": time.time() - start,
        }

    sql_created, diagnostic = _is_sql_created(auth, pipeline_id)
    if not sql_created:
        logger.info(
            "Skipping DLT-owned %s %s (%s) — handled by pipeline migration.",
            obj_type, obj_name, diagnostic,
        )
        return {
            "object_name": obj_name,
            "object_type": obj_type,
            "status": "skipped_by_pipeline_migration",
            "error_message": f"DLT-defined pipeline {pipeline_id}; {diagnostic}",
            "duration_seconds": time.time() - start,
        }

    status, err = _replay_mv_st_ddl(
        obj_info, auth=auth, wh_id=wh_id, dry_run=config.dry_run,
    )
    return {
        "object_name": obj_name,
        "object_type": obj_type,
        "status": status,
        "error_message": err,
        "duration_seconds": time.time() - start,
    }


# COMMAND ----------
# Notebook execution


def run(dbutils, spark) -> None:
    """Entry point when running as a Databricks notebook."""
    config = MigrationConfig.from_workspace_file()
    auth = AuthManager(config, dbutils)
    tracker = TrackingManager(spark, config)

    dbutils.widgets.text("batch", "[]")
    batch: list[dict] = json.loads(dbutils.widgets.get("batch"))
    logger.info("Received batch of %d MV/ST objects.", len(batch))

    wh_id = find_warehouse(auth)
    results: list[dict] = []

    for obj in batch:
        try:
            res = migrate_mv_st(
                obj, config=config, auth=auth, tracker=tracker, wh_id=wh_id,
            )
        except Exception as exc:  # noqa: BLE001
            res = {
                "object_name": obj["object_name"],
                "object_type": obj.get("object_type", "mv"),
                "status": "failed",
                "error_message": str(exc),
                "duration_seconds": 0.0,
            }
        results.append(res)
        logger.info("%s %s -> %s", res["object_type"], res["object_name"], res["status"])

    tracker.append_migration_status(results)
    logger.info(
        "MV/ST worker complete. %d validated, %d skipped_by_pipeline_migration, %d failed.",
        sum(1 for r in results if r["status"] == "validated"),
        sum(1 for r in results if r["status"] == "skipped_by_pipeline_migration"),
        sum(1 for r in results if r["status"] == "failed"),
    )


# COMMAND ----------

if _is_notebook():
    run(dbutils, spark)  # type: ignore[name-defined]  # noqa: F821
