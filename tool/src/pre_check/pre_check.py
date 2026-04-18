# Databricks notebook source

# COMMAND ----------

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

# Pre-Check: validate connectivity, permissions, and prerequisites before migration.

from common.auth import AuthManager
from common.catalog_utils import CatalogExplorer
from common.config import MigrationConfig
from common.tracking import TrackingManager

# COMMAND ----------


def _is_notebook() -> bool:
    try:
        _ = dbutils  # type: ignore[name-defined]  # noqa: F821
        return True
    except NameError:
        return False


# COMMAND ----------


def run(dbutils, spark):  # noqa: D103
    config = MigrationConfig.from_workspace_file()
    auth = AuthManager(config, dbutils)
    tracker = TrackingManager(spark, config)
    explorer = CatalogExplorer(spark, auth)


    tracker.init_tracking_tables()


    results: list[dict] = []

    def _add(check_name: str, status: str, message: str, action_required: str = "") -> None:
        results.append(
            {
                "check_name": check_name,
                "status": status,
                "message": message,
                "action_required": action_required,
            }
        )


    # 1. check_source_auth
    try:
        connectivity = auth.test_connectivity()
        if connectivity["source"]:
            _add("check_source_auth", "PASS", "Source workspace authentication succeeded.")
        else:
            _add(
                "check_source_auth",
                "FAIL",
                "Source workspace authentication failed.",
                "Verify SPN credentials and source workspace URL.",
            )
    except Exception as e:
        _add(
            "check_source_auth",
            "FAIL",
            f"Source auth error: {e}",
            "Verify SPN credentials and source workspace URL.",
        )


    # 2. check_target_auth
    try:
        if connectivity["target"]:
            _add("check_target_auth", "PASS", "Target workspace authentication succeeded.")
        else:
            _add(
                "check_target_auth",
                "FAIL",
                "Target workspace authentication failed.",
                "Verify SPN credentials and target workspace URL.",
            )
    except Exception as e:
        _add(
            "check_target_auth",
            "FAIL",
            f"Target auth error: {e}",
            "Verify SPN credentials and target workspace URL.",
        )


    # 3. check_source_metastore
    try:
        row = spark.sql("SELECT current_metastore() AS ms").first()
        _add("check_source_metastore", "PASS", f"Source metastore: {row.ms}")
    except Exception as e:
        _add(
            "check_source_metastore",
            "FAIL",
            f"Cannot query source metastore: {e}",
            "Ensure the workspace is attached to a Unity Catalog metastore.",
        )


    # 4. check_target_metastore
    try:
        ms = auth.target_client.metastores.summary()
        _add("check_target_metastore", "PASS", f"Target metastore: {ms.name}")
    except Exception as e:
        _add(
            "check_target_metastore",
            "FAIL",
            f"Cannot reach target metastore: {e}",
            "Ensure target workspace has a UC metastore assigned.",
        )


    # 5. check_source_sharing
    try:
        list(auth.source_client.shares.list())
        _add("check_source_sharing", "PASS", "Source Delta Sharing provider is accessible.")
    except Exception as e:
        _add(
            "check_source_sharing",
            "WARN",
            f"Source sharing check failed: {e}",
            "Delta Sharing may not be enabled on source workspace.",
        )


    # 6. check_target_sharing
    try:
        list(auth.target_client.shares.list())
        _add("check_target_sharing", "PASS", "Target Delta Sharing provider is accessible.")
    except Exception as e:
        _add(
            "check_target_sharing",
            "WARN",
            f"Target sharing check failed: {e}",
            "Delta Sharing may not be enabled on target workspace.",
        )


    # 7. check_catalog_filter
    try:
        available = explorer.list_catalogs()
        if config.catalog_filter:
            missing = [c for c in config.catalog_filter if c not in available]
            if missing:
                _add(
                    "check_catalog_filter",
                    "FAIL",
                    f"Catalogs not found: {missing}",
                    "Check catalog_filter parameter for typos.",
                )
            else:
                _add(
                    "check_catalog_filter",
                    "PASS",
                    f"All filtered catalogs exist: {config.catalog_filter}",
                )
        else:
            _add(
                "check_catalog_filter",
                "PASS",
                f"No catalog filter set. Found {len(available)} catalogs.",
            )
    except Exception as e:
        _add(
            "check_catalog_filter",
            "FAIL",
            f"Cannot list catalogs: {e}",
            "Ensure SPN has catalog-level permissions on source.",
        )


    # 8. check_storage_credentials
    try:
        creds = list(auth.target_client.storage_credentials.list())
        _add(
            "check_storage_credentials",
            "PASS",
            f"Target has {len(creds)} storage credential(s).",
        )
    except Exception as e:
        _add(
            "check_storage_credentials",
            "WARN",
            f"Cannot list storage credentials: {e}",
            "SPN may lack permission to list storage credentials on target.",
        )


    # 9. check_external_locations
    try:
        locs = list(auth.target_client.external_locations.list())
        _add(
            "check_external_locations",
            "PASS",
            f"Target has {len(locs)} external location(s).",
        )
    except Exception as e:
        _add(
            "check_external_locations",
            "WARN",
            f"Cannot list external locations: {e}",
            "SPN may lack permission to list external locations on target.",
        )


    # 10. check_tracking_schema
    try:
        spark.sql(f"DESCRIBE SCHEMA {config.tracking_catalog}.{config.tracking_schema}")
        _add(
            "check_tracking_schema",
            "PASS",
            f"Tracking schema {config.tracking_catalog}.{config.tracking_schema} exists.",
        )
    except Exception as e:
        _add(
            "check_tracking_schema",
            "FAIL",
            f"Tracking schema not found: {e}",
            "Run init_tracking_tables or check tracking_catalog/tracking_schema params.",
        )


    # Persist results
    tracker.append_pre_check_results(results)


    # Print summary table
    print(f"\n{'Check':<30} {'Status':<8} {'Message'}")
    print("-" * 100)
    for r in results:
        print(f"{r['check_name']:<30} {r['status']:<8} {r['message']}")
        if r["action_required"]:
            print(f"{'':>30} -> {r['action_required']}")
    print("-" * 100)

    fail_count = sum(1 for r in results if r["status"] == "FAIL")
    warn_count = sum(1 for r in results if r["status"] == "WARN")
    print(f"\nSummary: {len(results)} checks, {fail_count} FAIL, {warn_count} WARN")

    if fail_count > 0:
        msg = f"Pre-check failed: {fail_count} check(s) returned FAIL. See details above."
        raise Exception(msg)

    return results


# COMMAND ----------

if _is_notebook():
    run(dbutils, spark)  # type: ignore[name-defined]  # noqa: F821
