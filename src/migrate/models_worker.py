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
# Registered Models Worker (Phase 3 Task 34).
#
# Two-phase: (a) create model shell + version metadata via SDK,
# (b) replay aliases per version. Artifact file copy is NOT covered here —
# a proper implementation requires cross-metastore ABFSS copy (similar to
# volume_worker's share+dbutils.fs.cp pattern). That work is flagged
# explicitly in the status row's error_message so operators know to run
# a separate artifact sync before the model is fully usable on target.

import json
import logging
import time

from common.auth import AuthManager
from common.config import MigrationConfig
from common.tracking import TrackingManager

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("models_worker")


def _is_notebook() -> bool:
    try:
        _ = dbutils  # type: ignore[name-defined] # noqa: F821
        return True
    except NameError:
        return False


def _parse_fqn(fqn: str) -> tuple[str, str, str]:
    parts = fqn.strip("`").split(".")
    if len(parts) != 3:
        raise ValueError(f"Malformed model FQN: {fqn}")
    return parts[0], parts[1], parts[2]


def apply_model(
    model: dict, *, auth: AuthManager, dry_run: bool,
) -> list[dict]:
    """Create the registered model + each version + each alias on target."""
    model_fqn = model.get("model_fqn", "")
    obj_key = f"MODEL_{model_fqn}"
    results: list[dict] = []

    start = time.time()
    if dry_run:
        results.append({
            "object_name": obj_key, "object_type": "registered_model",
            "status": "skipped", "error_message": "dry_run",
            "duration_seconds": time.time() - start,
        })
        return results

    try:
        catalog, schema, name = _parse_fqn(model_fqn)
    except ValueError as exc:
        results.append({
            "object_name": obj_key, "object_type": "registered_model",
            "status": "failed", "error_message": str(exc),
            "duration_seconds": time.time() - start,
        })
        return results

    client = auth.target_client
    # 1. Create the model shell
    try:
        client.registered_models.create(
            catalog_name=catalog, schema_name=schema, name=name,
            comment=model.get("comment"),
            storage_location=model.get("storage_location"),
        )
    except Exception as exc:  # noqa: BLE001
        # IDEMPOTENT: if model already exists, continue to versions + aliases
        if "already" not in str(exc).lower() or "exists" not in str(exc).lower():
            results.append({
                "object_name": obj_key, "object_type": "registered_model",
                "status": "failed", "error_message": str(exc),
                "duration_seconds": time.time() - start,
            })
            return results

    # 2. Versions — metadata only; artifacts deferred (see module docstring)
    version_errors: list[str] = []
    for v in model.get("versions", []):
        try:
            client.model_versions.create(
                catalog_name=catalog, schema_name=schema, model_name=name,
                source=v.get("source") or v.get("storage_location") or "",
                run_id=None,
            )
        except Exception as exc:  # noqa: BLE001
            if "already" not in str(exc).lower() or "exists" not in str(exc).lower():
                version_errors.append(f"v{v.get('version','?')}: {exc}")

        # 3. Aliases for this version
        for alias in v.get("aliases") or []:
            try:
                client.registered_models.set_alias(
                    full_name=f"{catalog}.{schema}.{name}",
                    alias=alias,
                    version_num=int(v["version"]),
                )
            except Exception as exc:  # noqa: BLE001
                version_errors.append(f"alias '{alias}' v{v['version']}: {exc}")

    duration = time.time() - start
    status_msg = "Artifact files not copied — run post-migration artifact sync."
    if version_errors:
        status_msg += " Errors: " + "; ".join(version_errors[:5])
    results.append({
        "object_name": obj_key, "object_type": "registered_model",
        "status": "validated" if not version_errors else "validation_failed",
        "error_message": status_msg,
        "duration_seconds": duration,
    })
    return results


def run(dbutils, spark) -> None:
    config = MigrationConfig.from_workspace_file()
    if not config.include_uc:
        logger.info("Skipping models_worker: scope.include_uc=false.")
        return
    auth = AuthManager(config, dbutils)
    tracker = TrackingManager(spark, config)

    rows_json = dbutils.jobs.taskValues.get(taskKey="orchestrator", key="registered_model_list")
    rows: list[dict] = json.loads(rows_json)
    logger.info("Received %d model records.", len(rows))

    results: list[dict] = []
    for r in rows:
        meta = r.get("metadata_json")
        if isinstance(meta, str):
            try:
                meta = json.loads(meta)
            except Exception:  # noqa: BLE001
                continue
        if not isinstance(meta, dict):
            continue
        results.extend(apply_model(meta, auth=auth, dry_run=config.dry_run))

    if results:
        tracker.append_migration_status(results)
    logger.info(
        "Models worker complete. %d validated, %d validation_failed, %d failed.",
        sum(1 for r in results if r["status"] == "validated"),
        sum(1 for r in results if r["status"] == "validation_failed"),
        sum(1 for r in results if r["status"] == "failed"),
    )


if _is_notebook():
    run(dbutils, spark)  # type: ignore[name-defined]  # noqa: F821
