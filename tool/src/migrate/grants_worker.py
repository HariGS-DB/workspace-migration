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
# Grants Worker: replays catalog and schema grants from source to target.
# Skips OWNER grants (those must be set separately).

import logging
import time

from common.auth import AuthManager
from common.catalog_utils import CatalogExplorer
from common.config import MigrationConfig
from common.sql_utils import execute_and_poll, find_warehouse
from common.tracking import TrackingManager

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("grants_worker")


# COMMAND ----------


def _is_notebook() -> bool:
    """Return True when running inside a Databricks notebook."""
    try:
        _ = dbutils  # type: ignore[name-defined]  # noqa: F821
        return True
    except NameError:
        return False


# COMMAND ----------
# Replay grants


def replay_grants(
    securable_type: str,
    securable_fqn: str,
    grants: list[dict],
    *,
    auth: AuthManager,
    wh_id: str,
    dry_run: bool = False,
) -> list[dict]:
    """Replay a list of grants on the target, skipping OWNER grants."""
    results: list[dict] = []
    for grant in grants:
        principal = grant["principal"]
        action_type = grant["action_type"]

        # Skip OWNER grants -- ownership is set differently
        if action_type.upper() == "OWN":
            logger.info("Skipping OWNER grant for %s on %s %s.", principal, securable_type, securable_fqn)
            continue

        sql = f"GRANT {action_type} ON {securable_type} {securable_fqn} TO `{principal}`"
        obj_key = f"GRANT_{action_type}_{securable_type}_{securable_fqn}_{principal}"

        if dry_run:
            logger.info("[DRY RUN] Would execute: %s", sql)
            results.append(
                {
                    "object_name": obj_key,
                    "object_type": "grant",
                    "status": "skipped",
                    "error_message": "dry_run",
                    "duration_seconds": 0.0,
                }
            )
            continue

        start = time.time()
        logger.info("Executing: %s", sql)
        result = execute_and_poll(auth, wh_id, sql)
        duration = time.time() - start

        if result["state"] == "SUCCEEDED":
            results.append(
                {
                    "object_name": obj_key,
                    "object_type": "grant",
                    "status": "validated",
                    "error_message": None,
                    "duration_seconds": duration,
                }
            )
        else:
            results.append(
                {
                    "object_name": obj_key,
                    "object_type": "grant",
                    "status": "failed",
                    "error_message": result.get("error", result["state"]),
                    "duration_seconds": duration,
                }
            )

    return results


# COMMAND ----------
# Notebook execution


def run(dbutils, spark) -> None:
    """Entry point when running as a Databricks notebook."""
    config = MigrationConfig.from_workspace_file()
    auth = AuthManager(config, dbutils)
    spark_session = spark
    tracker = TrackingManager(spark_session, config)
    explorer = CatalogExplorer(spark_session, auth)

    # Read discovery inventory for catalogs and schemas
    inventory_df = spark_session.sql(
        f"SELECT DISTINCT catalog_name, schema_name "
        f"FROM {config.tracking_catalog}.{config.tracking_schema}.discovery_inventory"
    )
    inventory_rows = inventory_df.collect()

    catalogs: set[str] = set()
    schemas: set[str] = set()  # set of "catalog.schema" FQNs

    for row in inventory_rows:
        cat = row.catalog_name
        sch = row.schema_name
        if cat:
            catalogs.add(cat)
        if cat and sch:
            schemas.add(f"`{cat}`.`{sch}`")

    logger.info("Found %d catalogs and %d schemas to process grants.", len(catalogs), len(schemas))

    wh_id = find_warehouse(auth)

    # Process catalog grants

    all_results: list[dict] = []

    for catalog_name in sorted(catalogs):
        logger.info("Processing grants for CATALOG %s", catalog_name)
        try:
            grants = explorer.list_grants("CATALOG", f"`{catalog_name}`")
            grant_results = replay_grants(
                "CATALOG", f"`{catalog_name}`", grants, auth=auth, wh_id=wh_id, dry_run=config.dry_run
            )
            all_results.extend(grant_results)
        except Exception as exc:  # noqa: BLE001
            logger.error("Failed to process grants for CATALOG %s: %s", catalog_name, exc)
            all_results.append(
                {
                    "object_name": f"CATALOG_GRANTS_{catalog_name}",
                    "object_type": "grant",
                    "status": "failed",
                    "error_message": str(exc),
                    "duration_seconds": 0.0,
                }
            )

    # Process schema grants

    for schema_fqn in sorted(schemas):
        logger.info("Processing grants for SCHEMA %s", schema_fqn)
        try:
            grants = explorer.list_grants("SCHEMA", schema_fqn)
            grant_results = replay_grants("SCHEMA", schema_fqn, grants, auth=auth, wh_id=wh_id, dry_run=config.dry_run)
            all_results.extend(grant_results)
        except Exception as exc:  # noqa: BLE001
            logger.error("Failed to process grants for SCHEMA %s: %s", schema_fqn, exc)
            all_results.append(
                {
                    "object_name": f"SCHEMA_GRANTS_{schema_fqn}",
                    "object_type": "grant",
                    "status": "failed",
                    "error_message": str(exc),
                    "duration_seconds": 0.0,
                }
            )

    # Record final statuses

    if all_results:
        tracker.append_migration_status(all_results)

    logger.info(
        "Grants worker complete. %d succeeded, %d failed.",
        sum(1 for r in all_results if r["status"] == "validated"),
        sum(1 for r in all_results if r["status"] == "failed"),
    )


# COMMAND ----------

if _is_notebook():
    run(dbutils, spark)  # type: ignore[name-defined]  # noqa: F821
