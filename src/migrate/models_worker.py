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
# (b) replay aliases per version, (c) best-effort artifact copy from the
# source version's storage_location to the target version's storage_location
# via dbutils.fs.cp when an ambient dbutils is available. Byte count and
# file count from the copy are surfaced in the status row's error_message
# so operators can confirm artifacts reached target without opening a
# separate artifact-sync job.

import json
import logging
import time

from common.auth import AuthManager
from common.config import MigrationConfig
from common.tracking import TrackingManager

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("models_worker")


def _copy_artifact_files(src: str, dst: str, dbutils) -> tuple[int, int]:
    """Recursively copy all files under *src* to *dst* using dbutils.fs.cp.

    Returns (bytes_copied, file_count). Missing src directories return (0, 0).
    Exceptions bubble up so the caller can record a partial status.
    """
    total_bytes = 0
    total_files = 0
    try:
        entries = dbutils.fs.ls(src)
    except Exception:  # noqa: BLE001
        # No artifacts at src (yet) — not an error; the model shell still
        # migrated successfully.
        return 0, 0
    try:
        dbutils.fs.mkdirs(dst)
    except Exception:  # noqa: BLE001
        pass
    for f in entries:
        target = dst.rstrip("/") + "/" + f.name.rstrip("/")
        if f.isDir():
            sub_bytes, sub_files = _copy_artifact_files(f.path, target, dbutils)
            total_bytes += sub_bytes
            total_files += sub_files
        else:
            dbutils.fs.cp(f.path, target)
            total_bytes += f.size
            total_files += 1
    return total_bytes, total_files


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
    model: dict, *, auth: AuthManager, dry_run: bool, dbutils=None,
) -> list[dict]:
    """Create the registered model + each version + each alias on target.

    When *dbutils* is provided, also attempt a best-effort artifact copy
    from each source version's storage_location to the target version's
    storage_location. Counts are surfaced in the status row's error_message.
    """
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

    # 2. Versions — metadata + best-effort artifact copy (when dbutils is
    #    available — i.e. running in a notebook).
    version_errors: list[str] = []
    total_bytes = 0
    total_files = 0
    for v in model.get("versions", []):
        try:
            created = client.model_versions.create(
                catalog_name=catalog, schema_name=schema, model_name=name,
                source=v.get("source") or v.get("storage_location") or "",
                run_id=None,
            )
        except Exception as exc:  # noqa: BLE001
            created = None
            if "already" not in str(exc).lower() or "exists" not in str(exc).lower():
                version_errors.append(f"v{v.get('version','?')}: {exc}")

        # 2b. Artifact copy (best-effort; requires dbutils + accessible paths).
        src_loc = v.get("storage_location") or ""
        dst_loc = getattr(created, "storage_location", None) if created is not None else None
        if dbutils is not None and src_loc and dst_loc:
            try:
                copied_bytes, copied_files = _copy_artifact_files(src_loc, dst_loc, dbutils)
                total_bytes += copied_bytes
                total_files += copied_files
                logger.info(
                    "Artifact copy v%s: %d bytes, %d files (%s -> %s)",
                    v.get("version", "?"), copied_bytes, copied_files, src_loc, dst_loc,
                )
            except Exception as exc:  # noqa: BLE001
                version_errors.append(
                    f"v{v.get('version','?')} artifact copy failed: {exc}"
                )

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
    # Surface copy stats so operators (and integration assertions) can verify
    # artifacts made it across without digging into storage URIs.
    status_msg = (
        f"Artifact copy: {total_bytes} bytes, {total_files} file(s)."
        if dbutils is not None
        else "Artifact files not copied — run post-migration artifact sync."
    )
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
        # Pass dbutils so apply_model can run the artifact copy path.
        results.extend(apply_model(
            meta, auth=auth, dry_run=config.dry_run, dbutils=dbutils,
        ))

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
