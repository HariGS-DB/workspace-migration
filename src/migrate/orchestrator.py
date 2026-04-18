# Databricks notebook source

# COMMAND ----------

# Bootstrap: put the bundle's `src/` dir on sys.path so `from common...` imports resolve
import sys  # noqa: E402
_ctx = dbutils.notebook.entry_point.getDbutils().notebook().getContext()  # noqa: F821
_nb = _ctx.notebookPath().get()
_src = "/Workspace" + _nb.split("/files/")[0] + "/files/src"
if _src not in sys.path:
    sys.path.insert(0, _src)

# COMMAND ----------
# Orchestrator: reads discovery inventory, builds batches per object type,
# and publishes them as task values for downstream workers.

from __future__ import annotations

import json
import logging

from common.config import MigrationConfig
from common.tracking import TrackingManager

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("orchestrator")


# COMMAND ----------
# Batching helper — importable for unit tests


def build_batches(objects: list[dict], batch_size: int) -> list[str]:
    """Split a list of object dicts into JSON-encoded batch strings.

    Each returned string is a JSON array of dicts, with at most *batch_size*
    elements.
    """
    batches: list[str] = []
    for i in range(0, len(objects), batch_size):
        chunk = objects[i : i + batch_size]
        batches.append(json.dumps(chunk, default=str))
    return batches


# COMMAND ----------
# Notebook execution — guarded so build_batches can be imported in tests.


def _is_notebook() -> bool:
    """Return True when running inside a Databricks notebook."""
    try:
        _ = dbutils  # type: ignore[name-defined] # noqa: F821
        return True
    except NameError:
        return False


if _is_notebook():
    config = MigrationConfig.from_job_params(dbutils)  # type: ignore[name-defined] # noqa: F821
    spark_session = spark  # type: ignore[name-defined] # noqa: F821
    tracker = TrackingManager(spark_session, config)

    # Read discovery inventory and collect pending objects per type
    BATCHED_TYPES = ("managed_table", "external_table", "volume")
    LIST_TYPES = ("function", "view")

    batch_output: dict[str, list[str]] = {}
    list_output: dict[str, str] = {}

    for obj_type in BATCHED_TYPES:
        pending = tracker.get_pending_objects(obj_type)
        logger.info("Pending %s: %d objects", obj_type, len(pending))
        batches = build_batches(pending, config.batch_size)
        batch_output[f"{obj_type}_batches"] = batches

    for obj_type in LIST_TYPES:
        pending = tracker.get_pending_objects(obj_type)
        logger.info("Pending %s: %d objects", obj_type, len(pending))
        list_output[f"{obj_type}_list"] = json.dumps(pending, default=str)

    # Publish task values for downstream workers
    for key, batches in batch_output.items():
        dbutils.jobs.taskValues.set(key=key, value=json.dumps(batches))  # type: ignore[name-defined] # noqa: F821
        logger.info("Published %s: %d batches", key, len(batches))

    for key, value in list_output.items():
        dbutils.jobs.taskValues.set(key=key, value=value)  # type: ignore[name-defined] # noqa: F821
        logger.info("Published %s", key)

    logger.info("Orchestrator complete.")
