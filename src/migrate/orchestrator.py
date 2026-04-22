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
# UC Orchestrator: reads discovery inventory (source_type='uc'), builds
# batches per object type, and publishes them as task values for downstream
# workers. Skipped when config.scope.include_uc is false.

import json
import logging

from common.config import MigrationConfig
from common.tracking import TrackingManager

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("orchestrator")


# COMMAND ----------
# Batching helpers live in ``migrate.batching`` (plain module — notebooks
# can't import other notebooks). Re-exported here for back-compat so
# existing ``from migrate.orchestrator import build_batches`` callers
# (unit tests, downstream code) keep working.

from migrate.batching import (  # noqa: E402, F401
    MAX_BATCH_BYTES,
    _strip_heavy_fields,
    build_batches,
)


# COMMAND ----------


def _is_notebook() -> bool:
    """Return True when running inside a Databricks notebook."""
    try:
        _ = dbutils  # type: ignore[name-defined] # noqa: F821
        return True
    except NameError:
        return False


# Keys published by this orchestrator — declared up top so we can emit empty
# task values when scope.include_uc is false (downstream for_each tasks
# consume these and would block otherwise).
_BATCH_KEYS = (
    "managed_table_batches",
    "external_table_batches",
    "volume_batches",
    "mv_batches",
    "st_batches",
)
_LIST_KEYS = (
    "function_list",
    "view_list",
    # Phase 3 Tier A
    "tag_list",
    "row_filter_list",
    "column_mask_list",
    "policy_list",
    # Phase 3 Tier B
    "comment_list",
    # Phase 3 Tier C
    "monitor_list",
    "registered_model_list",
    "connection_list",
    "foreign_catalog_list",
    # Phase 3 Tier D
    "share_list",
    "recipient_list",
    "provider_list",
    "online_table_list",
)


def _publish_empty_task_values(dbutils) -> None:
    for key in _BATCH_KEYS:
        dbutils.jobs.taskValues.set(key=key, value=json.dumps([]))
    for key in _LIST_KEYS:
        dbutils.jobs.taskValues.set(key=key, value=json.dumps([]))


# COMMAND ----------
# Notebook execution

if _is_notebook():
    config = MigrationConfig.from_workspace_file()  # type: ignore[name-defined] # noqa: F821
    if not config.include_uc:
        logger.info("Skipping UC orchestrator: scope.include_uc=false.")
        _publish_empty_task_values(dbutils)  # type: ignore[name-defined] # noqa: F821
    else:
        spark_session = spark  # type: ignore[name-defined] # noqa: F821
        tracker = TrackingManager(spark_session, config)

        # Read discovery inventory and collect pending objects per type
        BATCHED_TYPES = ("managed_table", "external_table", "volume", "mv", "st")
        LIST_TYPES = (
            "function",
            "view",
            # Phase 3 governance object types — published even when counts
            # are zero so downstream worker tasks always have a valid JSON
            # payload to consume.
            "tag",
            "row_filter",
            "column_mask",
            "policy",
            "comment",
            "monitor",
            "registered_model",
            "connection",
            "foreign_catalog",
            "share",
            "recipient",
            "provider",
            "online_table",
        )

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
            # Strip heavy fields for the same reason as batched types — the
            # aggregated list is also subject to the 3000-byte task-value limit.
            list_output[f"{obj_type}_list"] = json.dumps(_strip_heavy_fields(pending), default=str)

        # Publish task values for downstream workers
        for key, batches in batch_output.items():
            dbutils.jobs.taskValues.set(key=key, value=json.dumps(batches))  # type: ignore[name-defined] # noqa: F821
            logger.info("Published %s: %d batches", key, len(batches))

        for key, value in list_output.items():
            dbutils.jobs.taskValues.set(key=key, value=value)  # type: ignore[name-defined] # noqa: F821
            logger.info("Published %s", key)

        logger.info("Orchestrator complete.")
