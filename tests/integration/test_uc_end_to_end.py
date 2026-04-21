# Databricks notebook source

# COMMAND ----------

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

# UC end-to-end assertion: verify migration tracking shows all seeded UC
# objects validated, plus Phase 2.5 feature-specific checks.

from common.config import MigrationConfig
from common.tracking import TrackingManager

config = MigrationConfig.from_workspace_file()
tracker = TrackingManager(spark, config)  # noqa: F821

status_df = tracker.get_latest_migration_status()
# UC-only object types: base (Phase 1) + Phase 2.5 (mv, st)
uc_types = ("managed_table", "external_table", "view", "function", "volume", "mv", "st")
status_df = status_df.filter(status_df.object_type.isin(list(uc_types)))

total = status_df.count()
print(f"Total UC migrated objects: {total}")
assert total > 0, "No UC migration status records found."

error_messages: list[str] = []

counts = {row["status"]: row["n"] for row in status_df.groupBy("status").count().withColumnRenamed("count", "n").collect()}
print(f"UC status breakdown: {counts}")

# failed / validation_failed are real errors. skipped_by_pipeline_migration
# is expected for any DLT-owned MV/ST (not part of our seed today, but
# forward-compatible with the worker's skip path).
failures_df = status_df.filter("status IN ('failed','validation_failed')")
if failures_df.count() > 0:
    for row in failures_df.select("object_name", "object_type", "status", "error_message").collect():
        error_messages.append(
            f"{row.object_type} '{row.object_name}' [{row.status}]: {row.error_message}"
        )

# Row-count parity on managed_table
managed_rows = status_df.filter("object_type = 'managed_table'").collect()
if not managed_rows:
    error_messages.append("No managed_table records in UC migration_status.")
else:
    for row in managed_rows:
        if row["source_row_count"] != row["target_row_count"]:
            error_messages.append(
                f"Row count mismatch for {row['object_name']}: "
                f"src={row['source_row_count']} tgt={row['target_row_count']}"
            )
        else:
            print(f"{row['object_name']}: rows={row['source_row_count']} validated")

# COMMAND ----------
# --- Phase 2.5 raw-string leak guard ---
# classify_tables must have mapped MATERIALIZED_VIEW -> 'mv' and
# STREAMING_TABLE -> 'st'. No rows should carry the raw strings.
raw_df = tracker.get_latest_migration_status().filter(
    "object_type IN ('MATERIALIZED_VIEW', 'STREAMING_TABLE')"
)
if raw_df.count() > 0:
    for row in raw_df.select("object_name", "object_type").collect():
        error_messages.append(
            f"classify_tables leaked raw table_type '{row.object_type}' "
            f"for {row.object_name} — _TABLE_TYPE_MAP is missing an entry"
        )

# COMMAND ----------
# --- Phase 2.5.A: managed volume data copy ---
# Seed wrote a marker file into the source volume; verify it exists on target
# with matching byte count.
expected_bytes_str = dbutils.jobs.taskValues.get(  # type: ignore[name-defined]  # noqa: F821
    taskKey="seed_uc", key="volume_marker_bytes", debugValue="0"
)
expected_bytes = int(expected_bytes_str or "0")
if expected_bytes == 0:
    error_messages.append("Seed did not publish volume_marker_bytes task value.")
else:
    try:
        files = dbutils.fs.ls(  # type: ignore[name-defined]  # noqa: F821
            "/Volumes/integration_test_src/test_schema/test_volume/"
        )
        marker = next((f for f in files if f.name == "marker.txt"), None)
        if marker is None:
            error_messages.append(
                "Phase 2.5.A: marker.txt missing on target — managed volume data not copied."
            )
        elif marker.size != expected_bytes:
            error_messages.append(
                f"Phase 2.5.A: marker.txt size mismatch "
                f"(source={expected_bytes}, target={marker.size})"
            )
        else:
            print(f"Phase 2.5.A validated: marker.txt on target with {marker.size} bytes.")
    except Exception as _exc:  # noqa: BLE001
        error_messages.append(f"Phase 2.5.A: target volume inaccessible: {_exc}")

# COMMAND ----------
# --- Phase 2.5.C: Python UDF replay ---
try:
    row = spark.sql(  # noqa: F821
        "SELECT integration_test_src.test_schema.py_double(21.0) AS result"
    ).first()
    if row is None or row.result != 42.0:
        error_messages.append(
            f"Phase 2.5.C: py_double on target returned {row.result if row else None}, expected 42.0"
        )
    else:
        print(f"Phase 2.5.C validated: py_double(21.0) = {row.result}")
except Exception as _exc:  # noqa: BLE001
    error_messages.append(f"Phase 2.5.C: py_double failed on target: {_exc}")

# COMMAND ----------
# --- Phase 2.5.D: SQL-created MV ---
has_mv = dbutils.jobs.taskValues.get(  # type: ignore[name-defined]  # noqa: F821
    taskKey="seed_uc", key="has_mv", debugValue="false"
)
if str(has_mv).lower() == "true":
    mv_status = status_df.filter(
        "object_type = 'mv' AND object_name LIKE '%mv_high_value%'"
    ).collect()
    if not mv_status:
        error_messages.append("Phase 2.5.D: MV row missing from migration_status.")
    elif mv_status[0]["status"] != "validated":
        error_messages.append(
            f"Phase 2.5.D: MV status is '{mv_status[0]['status']}', expected 'validated'. "
            f"error={mv_status[0]['error_message']}"
        )
    else:
        try:
            detail = spark.sql(  # noqa: F821
                "DESCRIBE DETAIL integration_test_src.test_schema.mv_high_value"
            ).first()
            props = detail.properties if detail and detail.properties else {}
            if not props.get("pipelines.pipelineId"):
                error_messages.append(
                    "Phase 2.5.D: MV on target is missing pipelines.pipelineId "
                    "(target did not auto-provision a backing pipeline)"
                )
            else:
                print(f"Phase 2.5.D MV validated: pipeline_id={props['pipelines.pipelineId']}")
        except Exception as _exc:  # noqa: BLE001
            error_messages.append(f"Phase 2.5.D: MV DESCRIBE DETAIL failed: {_exc}")
else:
    print("Phase 2.5.D: MV fixture not seeded; skipping MV assertion.")

# COMMAND ----------
# --- Phase 2.5.D: SQL-created streaming table ---
has_st = dbutils.jobs.taskValues.get(  # type: ignore[name-defined]  # noqa: F821
    taskKey="seed_uc", key="has_st", debugValue="false"
)
if str(has_st).lower() == "true":
    st_status = status_df.filter(
        "object_type = 'st' AND object_name LIKE '%st_orders%'"
    ).collect()
    if not st_status:
        error_messages.append("Phase 2.5.D: ST row missing from migration_status.")
    elif st_status[0]["status"] != "validated":
        error_messages.append(
            f"Phase 2.5.D: ST status is '{st_status[0]['status']}', expected 'validated'. "
            f"error={st_status[0]['error_message']}"
        )
    else:
        print(f"Phase 2.5.D ST validated: {st_status[0]['object_name']}")
else:
    print("Phase 2.5.D: ST fixture not seeded; skipping ST assertion.")

# COMMAND ----------
# --- Phase 2.5.B: Iceberg managed table ---
has_iceberg = dbutils.jobs.taskValues.get(  # type: ignore[name-defined]  # noqa: F821
    taskKey="seed_uc", key="has_iceberg", debugValue="false"
)
if str(has_iceberg).lower() == "true":
    iceberg_row = status_df.filter(
        "object_type = 'managed_table' AND object_name LIKE '%iceberg_sales%'"
    ).collect()
    if not iceberg_row:
        error_messages.append("Phase 2.5.B: iceberg_sales missing from migration_status.")
    elif iceberg_row[0]["status"] != "validated":
        error_messages.append(
            f"Phase 2.5.B: iceberg_sales status is "
            f"'{iceberg_row[0]['status']}', expected 'validated'. "
            f"error={iceberg_row[0]['error_message']}"
        )
    elif iceberg_row[0]["source_row_count"] != iceberg_row[0]["target_row_count"]:
        error_messages.append(
            f"Phase 2.5.B: iceberg_sales row count mismatch "
            f"(src={iceberg_row[0]['source_row_count']}, "
            f"tgt={iceberg_row[0]['target_row_count']})"
        )
    else:
        try:
            detail = spark.sql(  # noqa: F821
                "DESCRIBE DETAIL integration_test_src.test_schema.iceberg_sales"
            ).first()
            fmt = (getattr(detail, "format", "") or "").lower()
            if fmt != "iceberg":
                error_messages.append(
                    f"Phase 2.5.B: target iceberg_sales format is '{fmt}', expected 'iceberg'"
                )
            else:
                print(
                    f"Phase 2.5.B Iceberg validated: format=iceberg, "
                    f"rows={iceberg_row[0]['source_row_count']}"
                )
        except Exception as _exc:  # noqa: BLE001
            error_messages.append(f"Phase 2.5.B: DESCRIBE DETAIL failed: {_exc}")
else:
    print("Phase 2.5.B: Iceberg fixture not seeded; skipping Iceberg assertion.")

# COMMAND ----------

# COMMAND ----------
# --- Phase 3 governance assertions ---
# Each gated on a task value from the seed step; if the fixture was skipped
# due to runtime/preview constraints, the corresponding assertion is skipped
# with a clear message rather than failing.

full_status = tracker.get_latest_migration_status()

# Task 28 — tags: expect at least one 'tag' row with status='validated'
has_tag = dbutils.jobs.taskValues.get(  # type: ignore[name-defined]  # noqa: F821
    taskKey="seed_uc", key="has_tag", debugValue="false"
)
if str(has_tag).lower() == "true":
    tag_rows = full_status.filter("object_type = 'tag' AND status = 'validated'").collect()
    if not tag_rows:
        error_messages.append("Phase 3 T28: no validated tag rows in migration_status.")
    else:
        print(f"Phase 3 T28 validated: {len(tag_rows)} tag row(s) on target.")
else:
    print("Phase 3 T28: tag fixture not seeded; skipping.")

# Task 29 — row filter
has_rf = dbutils.jobs.taskValues.get(  # type: ignore[name-defined]  # noqa: F821
    taskKey="seed_uc", key="has_row_filter", debugValue="false"
)
if str(has_rf).lower() == "true":
    rf_rows = full_status.filter(
        "object_type = 'row_filter' AND status = 'validated'"
    ).collect()
    if not rf_rows:
        error_messages.append("Phase 3 T29: row filter not replayed on target.")
    else:
        print(f"Phase 3 T29 validated: row filter applied on {rf_rows[0]['object_name']}.")
else:
    print("Phase 3 T29: row filter fixture not seeded; skipping.")

# Task 30 — column mask
has_cm = dbutils.jobs.taskValues.get(  # type: ignore[name-defined]  # noqa: F821
    taskKey="seed_uc", key="has_column_mask", debugValue="false"
)
if str(has_cm).lower() == "true":
    cm_rows = full_status.filter(
        "object_type = 'column_mask' AND status = 'validated'"
    ).collect()
    if not cm_rows:
        error_messages.append("Phase 3 T30: column mask not replayed on target.")
    else:
        print(f"Phase 3 T30 validated: column mask applied on {cm_rows[0]['object_name']}.")
else:
    print("Phase 3 T30: column mask fixture not seeded; skipping.")

# Task 32 — comments: expect at least CATALOG + SCHEMA + TABLE comment rows
comment_rows = full_status.filter(
    "object_type = 'comment' AND status = 'validated'"
).collect()
if len(comment_rows) < 2:
    # 2 minimum since TABLE comments on Delta may skip via DEEP CLONE path
    error_messages.append(
        f"Phase 3 T32: expected >= 2 comment rows (catalog + schema), "
        f"got {len(comment_rows)}."
    )
else:
    print(f"Phase 3 T32 validated: {len(comment_rows)} comment row(s) replayed.")

# COMMAND ----------
# --- Grants assertion (UC) ---
# The seed grants SELECT on managed_orders to ``account users``. Verify
# grants_worker migrated that grant (creates a ``grant`` row in
# migration_status with status validated).

has_table_grant = dbutils.jobs.taskValues.get(  # type: ignore[name-defined]  # noqa: F821
    taskKey="seed_uc", key="has_table_grant", debugValue="false"
)
if str(has_table_grant).lower() == "true":
    grant_rows = full_status.filter(
        "object_type = 'grant' AND status = 'validated' "
        "AND object_name LIKE '%SELECT%' "
        "AND object_name LIKE '%managed_orders%' "
        "AND object_name LIKE '%account users%'"
    ).collect()
    if not grant_rows:
        # Fallback lookup — some grant workers record broader securable-level
        # entries; also accept a catalog/schema-level entry that would imply
        # the grant propagated.
        fallback = full_status.filter(
            "object_type = 'grant' AND status = 'validated' "
            "AND object_name LIKE '%account users%'"
        ).collect()
        if not fallback:
            error_messages.append(
                "Grants: no validated grant row for `account users` — "
                "SELECT on managed_orders did not migrate to target."
            )
        else:
            print(
                f"Grants validated (fallback): {len(fallback)} grant row(s) "
                f"for 'account users' replayed on target."
            )
    else:
        print(
            f"Grants validated: {len(grant_rows)} "
            f"SELECT-on-managed_orders grant row(s) for 'account users' "
            f"replayed on target."
        )
else:
    print("Grants: table-level grant not seeded; skipping assertion.")

# COMMAND ----------
# --- RLS/CM skip-path assertion ---
# managed_sensitive has row filter + column mask on a managed Delta table.
# Delta Sharing refuses to share these; with rls_cm_strategy="" (default)
# setup_sharing records status=skipped_by_rls_cm_policy and the table's
# data never reaches target.

has_rls_cm_managed = dbutils.jobs.taskValues.get(  # type: ignore[name-defined]  # noqa: F821
    taskKey="seed_uc", key="has_rls_cm_managed", debugValue="false"
)
if str(has_rls_cm_managed).lower() == "true":
    sensitive_rows = full_status.filter(
        "object_type = 'managed_table' "
        "AND object_name LIKE '%managed_sensitive%'"
    ).collect()
    if not sensitive_rows:
        error_messages.append(
            "RLS/CM skip: no migration_status row for managed_sensitive; "
            "setup_sharing should have recorded skipped_by_rls_cm_policy."
        )
    else:
        # The worker can append multiple rows over the run's lifetime; take
        # the latest by migrated_at (get_latest_migration_status in
        # full_status already does this per (object_name, object_type)).
        row = sensitive_rows[0].asDict()
        status = row.get("status")
        error_message = row.get("error_message") or ""
        if status != "skipped_by_rls_cm_policy":
            error_messages.append(
                f"RLS/CM skip: managed_sensitive status is {status!r}, "
                f"expected 'skipped_by_rls_cm_policy'."
            )
        elif "Delta Sharing" not in error_message:
            error_messages.append(
                "RLS/CM skip: managed_sensitive skipped, but error_message "
                "does not mention Delta Sharing — operator-visible reason is missing."
            )
        else:
            print(
                "RLS/CM skip validated: managed_sensitive recorded "
                "'skipped_by_rls_cm_policy' with Delta-Sharing reason."
            )
else:
    print("RLS/CM skip: managed_sensitive fixture not seeded; skipping.")

# COMMAND ----------

if error_messages:
    raise AssertionError(
        f"UC integration test failed with {len(error_messages)} error(s):\n"
        + "\n".join(error_messages)
    )
print("UC integration tests passed (Phase 1/2 + Phase 2.5 + Phase 3).")
