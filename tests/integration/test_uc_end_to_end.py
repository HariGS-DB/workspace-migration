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

if error_messages:
    raise AssertionError(
        f"UC integration test failed with {len(error_messages)} error(s):\n"
        + "\n".join(error_messages)
    )
print("UC integration tests passed (Phase 1/2 + Phase 2.5).")
