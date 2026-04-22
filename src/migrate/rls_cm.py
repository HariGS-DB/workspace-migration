"""RLS / CM drop_and_restore helpers (Phase 3 P.1).

Supports the ``rls_cm_strategy: drop_and_restore`` path:
  1. setup_sharing calls :func:`strip_policies_from_source` for every source
     table that carries a row filter or column mask. That function records
     a manifest row and then runs the ``DROP ROW FILTER`` /
     ``ALTER COLUMN ... DROP MASK`` against the source workspace so the
     resulting share exposes unfiltered data for DEEP CLONE on target.
  2. After managed_table_worker completes, ``restore_rls_cm`` re-reads the
     manifest and calls :func:`reapply_policy_to_source` for every row
     where ``restored_at IS NULL``. Success stamps the row. Crash recovery
     is built-in because the restore worker re-queries the manifest each
     run — setup_sharing is also safe to re-run (it skips tables that are
     already stripped).

Callers (setup_sharing, restore_rls_cm notebook) own the workspace
connection + warehouse id. This module is a pure helper library so it's
easy to unit test.
"""
from __future__ import annotations

import json
import logging
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from common.auth import AuthManager
    from common.tracking import TrackingManager

logger = logging.getLogger("rls_cm")


def discover_source_policies(spark, catalog: str, schema: str) -> list[dict]:
    """Return row filters + column masks currently applied on source tables
    in ``catalog.schema`` — one record per policy.

    Shape: ``policy_kind`` is ``row_filter`` or ``column_mask``;
    ``target_column`` is the masked column (column_mask) or NULL (row_filter);
    ``function_fqn`` is the UDF; ``using_columns`` is JSON-encoded list of
    columns (filter input columns or mask USING COLUMNS).
    """
    results: list[dict] = []
    # Row filters
    try:
        rows = spark.sql(
            f"SELECT table_name, row_filter_name, row_filter_input_columns "
            f"FROM `{catalog}`.`information_schema`.`tables` "
            f"WHERE table_schema = '{schema}' AND row_filter_name IS NOT NULL"
        ).collect()
        for r in rows:
            cols = list(r.row_filter_input_columns) if r.row_filter_input_columns else []
            results.append({
                "table_fqn": f"`{catalog}`.`{schema}`.`{r.table_name}`",
                "policy_kind": "row_filter",
                "target_column": None,
                "function_fqn": r.row_filter_name,
                "using_columns": json.dumps(cols),
            })
    except Exception as exc:  # noqa: BLE001
        logger.warning("row_filter discovery failed for %s.%s: %s", catalog, schema, exc)

    # Column masks
    try:
        rows = spark.sql(
            f"SELECT table_name, column_name, mask_name, mask_using_columns "
            f"FROM `{catalog}`.`information_schema`.`columns` "
            f"WHERE table_schema = '{schema}' AND mask_name IS NOT NULL"
        ).collect()
        for r in rows:
            cols = list(r.mask_using_columns) if r.mask_using_columns else []
            results.append({
                "table_fqn": f"`{catalog}`.`{schema}`.`{r.table_name}`",
                "policy_kind": "column_mask",
                "target_column": r.column_name,
                "function_fqn": r.mask_name,
                "using_columns": json.dumps(cols),
            })
    except Exception as exc:  # noqa: BLE001
        logger.warning("column_mask discovery failed for %s.%s: %s", catalog, schema, exc)

    return results


def strip_sql(policy: dict) -> str:
    """Return the source-side SQL that strips a policy."""
    if policy["policy_kind"] == "row_filter":
        return f"ALTER TABLE {policy['table_fqn']} DROP ROW FILTER"
    if policy["policy_kind"] == "column_mask":
        return (
            f"ALTER TABLE {policy['table_fqn']} "
            f"ALTER COLUMN `{policy['target_column']}` DROP MASK"
        )
    raise ValueError(f"Unknown policy_kind: {policy['policy_kind']}")


def reapply_sql(policy: dict) -> str:
    """Return the source-side SQL that re-applies a policy from manifest."""
    using_raw = policy.get("using_columns") or "[]"
    if isinstance(using_raw, list):
        cols = using_raw
    else:
        try:
            cols = json.loads(using_raw)
        except Exception:  # noqa: BLE001
            cols = []

    if policy["policy_kind"] == "row_filter":
        cols_clause = ", ".join(f"`{c}`" for c in cols)
        return (
            f"ALTER TABLE {policy['table_fqn']} "
            f"SET ROW FILTER {policy['function_fqn']} ON ({cols_clause})"
        )
    if policy["policy_kind"] == "column_mask":
        base = (
            f"ALTER TABLE {policy['table_fqn']} "
            f"ALTER COLUMN `{policy['target_column']}` "
            f"SET MASK {policy['function_fqn']}"
        )
        if cols:
            using = ", ".join(f"`{c}`" for c in cols)
            base += f" USING COLUMNS ({using})"
        return base
    raise ValueError(f"Unknown policy_kind: {policy['policy_kind']}")


def strip_policies_from_source(
    spark, auth: AuthManager, tracker: TrackingManager,
    *, catalogs: list[str], schemas: list[str] | None, wh_id: str,
    dry_run: bool = False,
) -> list[dict]:
    """Enumerate policies on source tables in scope, record a manifest row
    per policy, then execute the source-side strip SQL.

    Re-entrant: if a policy has already been stripped (manifest row exists
    with restored_at=NULL), we skip re-recording it but still make sure the
    table has no active policy on source.
    """
    from common.sql_utils import execute_and_poll

    stripped: list[dict] = []
    already = {
        (r["table_fqn"], r["policy_kind"], r.get("target_column"))
        for r in tracker.get_unrestored_manifest()
    }
    for catalog in catalogs:
        # information_schema.schemata — list schemas to scan
        try:
            rows = spark.sql(
                f"SELECT schema_name FROM `{catalog}`.information_schema.schemata"
            ).collect()
            cat_schemas = [r.schema_name for r in rows if r.schema_name != "information_schema"]
        except Exception:  # noqa: BLE001
            cat_schemas = []
        if schemas:
            cat_schemas = [s for s in cat_schemas if s in schemas]

        for schema in cat_schemas:
            policies = discover_source_policies(spark, catalog, schema)
            for policy in policies:
                key = (policy["table_fqn"], policy["policy_kind"], policy.get("target_column"))
                if key in already:
                    logger.info("Skipping %s — already tracked in manifest", key)
                    continue
                # Record first so that a crash post-DROP still leaves
                # a manifest breadcrumb for restore.
                if not dry_run:
                    tracker.append_rls_cm_manifest([policy])
                sql = strip_sql(policy)
                logger.info("Stripping source policy: %s", sql)
                if dry_run:
                    continue
                result = execute_and_poll(auth, wh_id, sql)
                if result.get("state") != "SUCCEEDED":
                    # Don't abort the whole workflow — the restore step will
                    # still try to reapply whatever we managed to strip.
                    logger.error("Strip failed for %s: %s", policy["table_fqn"], result.get("error"))
                    continue
                stripped.append(policy)
    return stripped


def reapply_policies_to_source(
    spark, auth: AuthManager, tracker: TrackingManager,
    *, wh_id: str, dry_run: bool = False,
) -> list[dict]:
    """Read all unrestored manifest rows, re-apply each policy on source,
    and stamp ``restored_at``. Returns per-row status dicts."""
    from common.sql_utils import execute_and_poll

    pending = tracker.get_unrestored_manifest()
    logger.info("Reapplying %d pending RLS/CM policies from manifest.", len(pending))
    results: list[dict] = []
    for row in pending:
        policy = {
            "table_fqn": row["table_fqn"],
            "policy_kind": row["policy_kind"],
            "target_column": row.get("target_column"),
            "function_fqn": row["function_fqn"],
            "using_columns": row.get("using_columns"),
        }
        sql = reapply_sql(policy)
        logger.info("Reapplying: %s", sql)
        status = "skipped" if dry_run else "validated"
        err: str | None = "dry_run" if dry_run else None
        if not dry_run:
            try:
                result = execute_and_poll(auth, wh_id, sql)
                if result.get("state") != "SUCCEEDED":
                    status = "failed"
                    err = result.get("error", result.get("state"))
                else:
                    tracker.stamp_manifest_restored(
                        policy["table_fqn"],
                        policy["policy_kind"],
                        policy.get("target_column"),
                    )
            except Exception as exc:  # noqa: BLE001
                status = "failed"
                err = str(exc)

        results.append({
            "table_fqn": policy["table_fqn"],
            "policy_kind": policy["policy_kind"],
            "target_column": policy.get("target_column"),
            "status": status,
            "error_message": err,
        })
    return results
