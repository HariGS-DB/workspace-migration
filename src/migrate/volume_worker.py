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
# Volume Worker.
#
# EXTERNAL volumes: recreate on target at the same storage_location (zero-copy).
# MANAGED volumes: preserve MANAGED type on target by
#   1. Adding source volume to cp_migration_share via SQL ALTER SHARE
#   2. Creating a fresh managed volume on target (UC allocates new storage path)
#   3. Submitting a target-side notebook run that dbutils.fs.cp's bytes from the
#      share-consumer path (/Volumes/cp_migration_share_consumer/<schema>/<vol>)
#      to the new target volume path
#   4. Removing the volume from the share
# Bytes copied + file count are recorded in migration_status.

import base64
import json
import logging
import time

from common.auth import AuthManager
from common.config import MigrationConfig
from common.sql_utils import execute_and_poll, find_warehouse
from common.tracking import TrackingManager

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("volume_worker")

SHARE_NAME = "cp_migration_share"
CONSUMER_CATALOG = f"{SHARE_NAME}_consumer"
# Path on target workspace where we upload the copy notebook on first use
TARGET_COPY_NOTEBOOK_PATH = "/Shared/cp_migration_runtime/_volume_copy"


# COMMAND ----------


def _is_notebook() -> bool:
    """Return True when running inside a Databricks notebook."""
    try:
        _ = dbutils  # type: ignore[name-defined]  # noqa: F821
        return True
    except NameError:
        return False


# Target-side notebook source — uploaded once per migration run and reused for
# every managed volume. Emits JSON with bytes_copied/file_count via notebook exit.
_TARGET_COPY_NOTEBOOK = '''# Databricks notebook source
# Target-side helper: recursively copies files from a source volume path to a
# destination volume path. Invoked by volume_worker for each MANAGED volume.
import json

dbutils.widgets.text("src", "")
dbutils.widgets.text("dst", "")
src = dbutils.widgets.get("src")
dst = dbutils.widgets.get("dst")

total_bytes = 0
total_files = 0

def _copy_recursive(s, d):
    global total_bytes, total_files
    for f in dbutils.fs.ls(s):
        target = d.rstrip("/") + "/" + f.name.rstrip("/")
        if f.isDir():
            dbutils.fs.mkdirs(target)
            _copy_recursive(f.path, target)
        else:
            dbutils.fs.cp(f.path, target)
            total_bytes += f.size
            total_files += 1

_copy_recursive(src, dst)
dbutils.notebook.exit(json.dumps({"bytes_copied": total_bytes, "file_count": total_files}))
'''


def _parse_fqn(fqn: str) -> tuple[str, str, str]:
    """Parse a backtick-wrapped catalog.schema.object FQN into its three parts."""
    parts = fqn.strip("`").split("`.`")
    if len(parts) != 3:
        msg = f"Cannot parse FQN {fqn!r}: expected `cat`.`schema`.`name`"
        raise ValueError(msg)
    return parts[0], parts[1], parts[2]


# COMMAND ----------
# Share add/remove (source-side SQL)


def add_volume_to_share(
    source_spark, share_name: str, volume_fqn: str, *, dry_run: bool = False
) -> None:
    """Add a volume to the migration share on source via SQL."""
    sql = f"ALTER SHARE {share_name} ADD VOLUME {volume_fqn}"
    if dry_run:
        logger.info("[DRY RUN] %s", sql)
        return
    try:
        source_spark.sql(sql)
    except Exception as exc:  # noqa: BLE001
        # Tolerate "already shared" errors — ALTER SHARE ADD is not idempotent
        if "already" in str(exc).lower():
            logger.info("Volume %s already in share; continuing.", volume_fqn)
        else:
            raise


def remove_volume_from_share(
    source_spark, share_name: str, volume_fqn: str, *, dry_run: bool = False
) -> None:
    """Remove a volume from the migration share on source. Best-effort."""
    sql = f"ALTER SHARE {share_name} REMOVE VOLUME {volume_fqn}"
    if dry_run:
        logger.info("[DRY RUN] %s", sql)
        return
    try:
        source_spark.sql(sql)
    except Exception as exc:  # noqa: BLE001
        logger.warning("Could not remove volume %s from share: %s", volume_fqn, exc)


# COMMAND ----------
# Target-side notebook upload + job submission


def _ensure_copy_notebook_on_target(auth: AuthManager) -> None:
    """Idempotently upload the copy-helper notebook to the target workspace."""
    target = auth.target_client
    # Ensure parent dir exists
    try:
        target.workspace.mkdirs("/Shared/cp_migration_runtime")
    except Exception:  # noqa: BLE001
        pass  # mkdirs is idempotent; swallow errors
    target.workspace.import_(
        path=TARGET_COPY_NOTEBOOK_PATH,
        content=base64.b64encode(_TARGET_COPY_NOTEBOOK.encode()).decode(),
        format="SOURCE",  # type: ignore[arg-type]
        language="PYTHON",  # type: ignore[arg-type]
        overwrite=True,
    )


def _run_target_volume_copy(
    auth: AuthManager, src_path: str, dst_path: str, run_name: str, *, timeout_s: int = 3600
) -> dict:
    """Submit the copy notebook on target as a serverless job and wait for it.

    Returns a dict with ``bytes_copied`` and ``file_count`` on success.
    Raises RuntimeError on failure or timeout.
    """
    target = auth.target_client
    submit_tasks = [
        {
            "task_key": "copy",
            "notebook_task": {
                "notebook_path": TARGET_COPY_NOTEBOOK_PATH,
                "base_parameters": {"src": src_path, "dst": dst_path},
            },
            # Serverless: no new_cluster / existing_cluster_id — Databricks picks
            # serverless compute automatically when neither is supplied.
            "environment_key": "default",
        }
    ]
    environments = [{"environment_key": "default", "spec": {"client": "2"}}]

    run = target.jobs.submit(
        run_name=run_name,
        tasks=submit_tasks,  # type: ignore[arg-type]
        environments=environments,  # type: ignore[arg-type]
    )
    run_id = run.run_id  # may be None on the submit response; re-fetch by waiter
    # Poll
    deadline = time.time() + timeout_s
    while time.time() < deadline:
        run_obj = target.jobs.get_run(run_id)
        life_cycle = getattr(run_obj.state, "life_cycle_state", None)
        if life_cycle and str(life_cycle).endswith(("TERMINATED", "SKIPPED", "INTERNAL_ERROR")):
            break
        time.sleep(5)
    else:
        raise RuntimeError(f"Target volume-copy job timed out after {timeout_s}s (run_id={run_id})")

    result_state = getattr(run_obj.state, "result_state", None)
    if str(result_state).endswith("SUCCESS"):
        # Pull notebook exit value
        task_run_id = run_obj.tasks[0].run_id if run_obj.tasks else run_id
        out = target.jobs.get_run_output(task_run_id)
        payload = out.notebook_output.result if out.notebook_output else "{}"
        return json.loads(payload or "{}")
    msg = getattr(run_obj.state, "state_message", "") or str(result_state)
    raise RuntimeError(f"Target volume-copy job failed: {msg}")


# COMMAND ----------
# Migrate a single volume


def migrate_volume(
    vol_info: dict,
    *,
    config: MigrationConfig,
    auth: AuthManager,
    tracker: TrackingManager,
    wh_id: str,
    source_spark,
    notebook_uploaded: bool,
) -> tuple[dict, bool]:
    """Recreate a volume on the target workspace. Returns (record, notebook_uploaded)."""
    obj_name = vol_info["object_name"]
    # table_type stores the volume_type for volume rows ("MANAGED" or "EXTERNAL")
    volume_type = (vol_info.get("table_type") or "MANAGED").upper()
    storage_location = vol_info.get("storage_location") or ""

    src_cat, src_sch, src_vol = _parse_fqn(obj_name)
    target_fqn = obj_name  # same catalog.schema.name on target

    tracker.append_migration_status([{
        "object_name": obj_name,
        "object_type": "volume",
        "status": "in_progress",
        "error_message": None,
        "job_run_id": None,
        "task_run_id": None,
        "source_row_count": None,
        "target_row_count": None,
        "duration_seconds": None,
    }])

    start = time.time()

    # --- EXTERNAL: zero-copy, same storage_location ---
    if volume_type == "EXTERNAL":
        if not storage_location:
            duration = time.time() - start
            return {
                "object_name": obj_name,
                "object_type": "volume",
                "status": "failed",
                "error_message": "EXTERNAL volume missing storage_location in discovery",
                "duration_seconds": duration,
            }, notebook_uploaded

        sql = f"CREATE EXTERNAL VOLUME IF NOT EXISTS {target_fqn} LOCATION '{storage_location}'"
        if config.dry_run:
            logger.info("[DRY RUN] %s", sql)
            return {
                "object_name": obj_name,
                "object_type": "volume",
                "status": "skipped",
                "error_message": "dry_run",
                "duration_seconds": time.time() - start,
            }, notebook_uploaded

        logger.info("Creating external volume on target: %s", target_fqn)
        result = execute_and_poll(auth, wh_id, sql)
        duration = time.time() - start
        if result["state"] != "SUCCEEDED":
            return {
                "object_name": obj_name,
                "object_type": "volume",
                "status": "failed",
                "error_message": result.get("error", result["state"]),
                "duration_seconds": duration,
            }, notebook_uploaded
        return {
            "object_name": obj_name,
            "object_type": "volume",
            "status": "validated",
            "error_message": None,
            "duration_seconds": duration,
        }, notebook_uploaded

    # --- MANAGED: add to share, create on target, copy files ---
    if config.dry_run:
        logger.info(
            "[DRY RUN] Would share, create, and copy-files for managed volume %s",
            obj_name,
        )
        return {
            "object_name": obj_name,
            "object_type": "volume",
            "status": "skipped",
            "error_message": "dry_run",
            "duration_seconds": time.time() - start,
        }, notebook_uploaded

    # Lazily upload the copy helper notebook on first managed volume
    if not notebook_uploaded:
        _ensure_copy_notebook_on_target(auth)
        notebook_uploaded = True

    try:
        # 1. Share the source volume
        add_volume_to_share(source_spark, SHARE_NAME, obj_name)

        # 2. Create a managed volume on target (UC allocates new storage path)
        create_sql = f"CREATE VOLUME IF NOT EXISTS {target_fqn}"
        logger.info("Creating managed volume on target: %s", target_fqn)
        result = execute_and_poll(auth, wh_id, create_sql)
        if result["state"] != "SUCCEEDED":
            raise RuntimeError(
                f"CREATE VOLUME failed: {result.get('error', result['state'])}"
            )

        # 3. Submit target-side copy job
        shared_path = f"/Volumes/{CONSUMER_CATALOG}/{src_sch}/{src_vol}"
        target_path = f"/Volumes/{src_cat}/{src_sch}/{src_vol}"
        run_name = f"cp_migration_volcopy_{src_cat}_{src_sch}_{src_vol}"
        logger.info("Copying volume data: %s -> %s", shared_path, target_path)
        try:
            copy_result = _run_target_volume_copy(auth, shared_path, target_path, run_name)
        except Exception:
            # Copy failed mid-execution — drop the partially-populated target volume
            # so a re-run can recreate it cleanly. Best-effort: swallow cleanup errors.
            try:
                auth.target_client.volumes.delete(name=target_fqn.strip("`").replace("`.`", "."))
                logger.info("Dropped partial target volume %s after copy failure", target_fqn)
            except Exception as cleanup_exc:  # noqa: BLE001
                logger.warning(
                    "Could not drop partial target volume %s after copy failure: %s",
                    target_fqn,
                    cleanup_exc,
                )
            raise

        duration = time.time() - start
        return {
            "object_name": obj_name,
            "object_type": "volume",
            "status": "validated",
            "error_message": None,
            "duration_seconds": duration,
            "source_row_count": copy_result.get("file_count"),
            "target_row_count": copy_result.get("file_count"),
        }, notebook_uploaded
    except Exception as exc:  # noqa: BLE001
        logger.exception("Managed volume migration failed for %s", obj_name)
        return {
            "object_name": obj_name,
            "object_type": "volume",
            "status": "failed",
            "error_message": str(exc),
            "duration_seconds": time.time() - start,
        }, notebook_uploaded
    finally:
        # Always remove the volume from the share
        remove_volume_from_share(source_spark, SHARE_NAME, obj_name)


# COMMAND ----------
# Notebook execution


def run(dbutils, spark) -> None:
    """Entry point when running as a Databricks notebook."""
    config = MigrationConfig.from_workspace_file()
    auth = AuthManager(config, dbutils)
    tracker = TrackingManager(spark, config)

    dbutils.widgets.text("batch", "[]")
    batch: list[dict] = json.loads(dbutils.widgets.get("batch"))
    logger.info("Received batch of %d volumes.", len(batch))

    wh_id = find_warehouse(auth)
    notebook_uploaded = False
    results: list[dict] = []

    for vol in batch:
        try:
            res, notebook_uploaded = migrate_volume(
                vol,
                config=config,
                auth=auth,
                tracker=tracker,
                wh_id=wh_id,
                source_spark=spark,
                notebook_uploaded=notebook_uploaded,
            )
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

    tracker.append_migration_status(results)
    logger.info(
        "Volume worker complete. %d succeeded, %d failed.",
        sum(1 for r in results if r["status"] == "validated"),
        sum(1 for r in results if r["status"] == "failed"),
    )


# COMMAND ----------

if _is_notebook():
    run(dbutils, spark)  # type: ignore[name-defined]  # noqa: F821
