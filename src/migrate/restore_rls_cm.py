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
# Post-migrate RLS/CM restore task.
#
# Runs at the end of the migrate workflow when ``rls_cm_strategy='drop_and_restore'``.
# Reads ``rls_cm_manifest`` for rows still ``restored_at IS NULL``, re-applies
# the row filter + column masks on the source table via ALTER TABLE SQL, and
# stamps ``restored_at``. Per-table continue-on-failure: a single bad restore
# doesn't block the rest.
#
# Idempotent:
# - Already-restored rows are ignored (WHERE restored_at IS NULL).
# - If UC rejects the SET because the policy is already applied (partial-
#   restore re-run), we catch that specific error and treat it as success.

import logging

from common.auth import AuthManager
from common.config import MigrationConfig
from common.tracking import TrackingManager
from migrate.rls_cm import restore_rls_cm

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("restore_rls_cm")


def _is_notebook() -> bool:
    try:
        _ = dbutils  # type: ignore[name-defined] # noqa: F821
        return True
    except NameError:
        return False


def run(dbutils, spark) -> None:  # noqa: ARG001
    config = MigrationConfig.from_workspace_file()
    if (config.rls_cm_strategy or "").strip().lower() != "drop_and_restore":
        logger.info("rls_cm_strategy is not 'drop_and_restore'; nothing to restore.")
        return
    if not config.include_uc:
        logger.info("scope.include_uc=false; skipping restore_rls_cm.")
        return

    auth = AuthManager(config, dbutils)  # noqa: F841 — kept for symmetry + future needs
    tracker = TrackingManager(spark, config)

    pending = tracker.get_unrestored_rls_cm_manifest()
    if not pending:
        logger.info("No RLS/CM manifest rows pending restore — nothing to do.")
        return
    logger.info("Restoring RLS/CM on source for %d table(s).", len(pending))

    restored = 0
    failed: list[tuple[str, str]] = []
    for row in pending:
        table_fqn = row["table_fqn"]
        captured = {
            "filter_fn_fqn": row.get("filter_fn_fqn"),
            "filter_columns": row.get("filter_columns") or [],
            "masks": row.get("masks") or [],
        }
        try:
            restore_rls_cm(spark, table_fqn, captured)
            tracker.mark_rls_cm_restored(table_fqn)
            restored += 1
            logger.info("Restored RLS/CM on %s.", table_fqn)
        except Exception as exc:  # noqa: BLE001
            err_text = str(exc)
            # ``already`` covers ``ROW_FILTER_ALREADY_SET`` / ``MASK_ALREADY_SET``
            # — treat as success for idempotent re-runs after partial crashes.
            if "already" in err_text.lower():
                tracker.mark_rls_cm_restored(table_fqn)
                restored += 1
                logger.info("Already restored on %s; stamped manifest.", table_fqn)
                continue
            tracker.mark_rls_cm_restore_failed(table_fqn, err_text)
            failed.append((table_fqn, err_text))
            logger.error("Restore failed for %s: %s", table_fqn, exc, exc_info=True)

    logger.info(
        "restore_rls_cm done. %d restored, %d failed.",
        restored,
        len(failed),
    )
    if failed:
        # Raise so the operator sees the workflow task fail — they must
        # manually re-apply the policies or fix the root cause and
        # re-run the task.
        lines = "\n".join(f"  {fqn}: {err[:200]}" for fqn, err in failed)
        raise RuntimeError(
            f"{len(failed)} table(s) failed RLS/CM restore — source side is "
            f"STILL UNPROTECTED for these. Fix the underlying error (see "
            f"rls_cm_manifest.restore_error) and re-run this task. Tables:\n"
            f"{lines}"
        )


# COMMAND ----------

if _is_notebook():
    run(dbutils, spark)  # type: ignore[name-defined]  # noqa: F821
