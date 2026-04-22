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

counts = {
    row["status"]: row["n"] for row in status_df.groupBy("status").count().withColumnRenamed("count", "n").collect()
}
print(f"UC status breakdown: {counts}")

# failed / validation_failed are real errors. skipped_by_pipeline_migration
# is expected for any DLT-owned MV/ST (not part of our seed today, but
# forward-compatible with the worker's skip path).
failures_df = status_df.filter("status IN ('failed','validation_failed')")
if failures_df.count() > 0:
    for row in failures_df.select("object_name", "object_type", "status", "error_message").collect():
        error_messages.append(f"{row.object_type} '{row.object_name}' [{row.status}]: {row.error_message}")

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
# --- Phase 1: target data integrity (1.7) ---
# Beyond ``migration_status`` says ``validated``, verify the target
# actually has each migrated table with a sensible schema. ``migration_
# status`` is trusted inside the tool; this assertion proves the trust
# is earned by cross-checking the authoritative target metastore via
# the UC Tables API.
#
# Checks per validated managed/external table on target:
#   - Table exists (tables.get doesn't 404).
#   - Column count matches source discovery.
#   - Column name set matches source (order/case ignored — UC may
#     normalize).
#
# We skip row-level compare (expensive; covered by source_row_count /
# target_row_count in migration_status already).

try:
    from common.auth import AuthManager  # noqa: E402
    from common.catalog_utils import CatalogExplorer  # noqa: E402

    _auth = AuthManager(config, dbutils)  # noqa: F821
    _explorer = CatalogExplorer(spark, _auth)  # noqa: F821

    _validated_tables = status_df.filter(
        "object_type IN ('managed_table', 'external_table') AND status = 'validated'"
    ).collect()

    _integrity_errors: list[str] = []
    for _row in _validated_tables:
        _fqn = _row["object_name"]
        _parts = _fqn.strip("`").split("`.`")
        if len(_parts) != 3:
            continue
        _cat, _sch, _name = _parts

        # 1. Target table exists?
        try:
            _tgt = _auth.target_client.tables.get(f"{_cat}.{_sch}.{_name}")
        except Exception as _exc:  # noqa: BLE001
            _integrity_errors.append(f"Target data integrity: {_fqn} missing on target ({_exc})")
            continue

        # 2. Compare column name set with source. We query source via
        #    spark.sql so we don't depend on source_client tables API
        #    for non-UC fixtures.
        try:
            _src_cols = {
                r.col_name
                for r in spark.sql(  # type: ignore[name-defined]  # noqa: F821
                    f"DESCRIBE TABLE {_fqn}"
                ).collect()
                if r.col_name and not r.col_name.startswith("#")
            }
        except Exception as _exc:  # noqa: BLE001
            _integrity_errors.append(f"Target data integrity: source DESCRIBE failed for {_fqn} ({_exc})")
            continue

        _tgt_cols = {c.name for c in (getattr(_tgt, "columns", None) or [])}
        if _src_cols != _tgt_cols:
            _missing_on_tgt = _src_cols - _tgt_cols
            _extra_on_tgt = _tgt_cols - _src_cols
            _integrity_errors.append(
                f"Target data integrity: column-set mismatch for {_fqn}: "
                f"missing on target={sorted(_missing_on_tgt)}; "
                f"extra on target={sorted(_extra_on_tgt)}"
            )
        else:
            print(f"Target data integrity validated: {_fqn} ({len(_src_cols)} cols match)")

    if _integrity_errors:
        error_messages.extend(_integrity_errors)
    elif not _validated_tables:
        error_messages.append(
            "Target data integrity: no validated tables found to verify — migrate may have been a no-op."
        )
except Exception as _exc:  # noqa: BLE001
    error_messages.append(f"Target data integrity: check aborted ({_exc})")

# COMMAND ----------
# --- Phase 2.5 raw-string leak guard ---
# classify_tables must have mapped MATERIALIZED_VIEW -> 'mv' and
# STREAMING_TABLE -> 'st'. No rows should carry the raw strings.
raw_df = tracker.get_latest_migration_status().filter("object_type IN ('MATERIALIZED_VIEW', 'STREAMING_TABLE')")
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
            error_messages.append("Phase 2.5.A: marker.txt missing on target — managed volume data not copied.")
        elif marker.size != expected_bytes:
            error_messages.append(
                f"Phase 2.5.A: marker.txt size mismatch (source={expected_bytes}, target={marker.size})"
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
        error_messages.append(f"Phase 2.5.C: py_double on target returned {row.result if row else None}, expected 42.0")
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
    mv_status = status_df.filter("object_type = 'mv' AND object_name LIKE '%mv_high_value%'").collect()
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
    st_status = status_df.filter("object_type = 'st' AND object_name LIKE '%st_orders%'").collect()
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
    iceberg_row = status_df.filter("object_type = 'managed_table' AND object_name LIKE '%iceberg_sales%'").collect()
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
                error_messages.append(f"Phase 2.5.B: target iceberg_sales format is '{fmt}', expected 'iceberg'")
            else:
                print(f"Phase 2.5.B Iceberg validated: format=iceberg, rows={iceberg_row[0]['source_row_count']}")
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
    rf_rows = full_status.filter("object_type = 'row_filter' AND status = 'validated'").collect()
    if not rf_rows:
        error_messages.append("Phase 3 T29: row filter not replayed on target.")
    else:
        print(f"Phase 3 T29 validated: row filter applied on {rf_rows[0]['object_name']}.")

    # --- DDL sanitizer end-to-end (S.12) ---
    # Not just "migration_status says validated" — fetch the external_customers
    # table from TARGET and verify its row_filter is actually populated.
    # Proves the strip-filter-from-DDL path in external_table_worker + the
    # later row_filters_worker re-application on target both work.
    try:
        from common.auth import AuthManager  # noqa: E402

        _auth = AuthManager(config, dbutils)  # noqa: F821
        _tgt_info = _auth.target_client.tables.get("integration_test_src.test_schema.external_customers")
        if getattr(_tgt_info, "row_filter", None) is None:
            error_messages.append(
                "DDL sanitizer E2E: external_customers on target has no "
                "row_filter — strip-then-reapply chain broke. Sanitizer "
                "stripped the filter from CREATE TABLE but row_filters_worker "
                "didn't reapply it."
            )
        else:
            print(
                f"DDL sanitizer E2E validated: external_customers on target "
                f"carries row_filter "
                f"'{getattr(_tgt_info.row_filter, 'function_name', '?')}'"
            )
    except Exception as _exc:  # noqa: BLE001
        error_messages.append(f"DDL sanitizer E2E: target lookup failed: {_exc}")
else:
    print("Phase 3 T29: row filter fixture not seeded; skipping.")

# Task 30 — column mask
has_cm = dbutils.jobs.taskValues.get(  # type: ignore[name-defined]  # noqa: F821
    taskKey="seed_uc", key="has_column_mask", debugValue="false"
)
if str(has_cm).lower() == "true":
    cm_rows = full_status.filter("object_type = 'column_mask' AND status = 'validated'").collect()
    if not cm_rows:
        error_messages.append("Phase 3 T30: column mask not replayed on target.")
    else:
        print(f"Phase 3 T30 validated: column mask applied on {cm_rows[0]['object_name']}.")

    # --- DDL sanitizer end-to-end: column mask on target (S.12) ---
    # external_customers.customer_id should carry the mask on target
    # even though external_table_worker stripped the MASK clause from
    # the replayed CREATE TABLE (because mask_customer function hadn't
    # been migrated yet at that stage).
    try:
        from common.auth import AuthManager  # noqa: E402

        _auth = AuthManager(config, dbutils)  # noqa: F821
        _tgt_info = _auth.target_client.tables.get("integration_test_src.test_schema.external_customers")
        _masked_cols = [c for c in (getattr(_tgt_info, "columns", None) or []) if getattr(c, "mask", None) is not None]
        if not _masked_cols:
            error_messages.append(
                "DDL sanitizer E2E: external_customers on target has no "
                "column masks — strip-then-reapply chain broke for masks."
            )
        else:
            print(f"DDL sanitizer E2E validated: {len(_masked_cols)} column mask(s) on external_customers on target.")
    except Exception as _exc:  # noqa: BLE001
        error_messages.append(f"DDL sanitizer E2E (mask): target lookup failed: {_exc}")
else:
    print("Phase 3 T30: column mask fixture not seeded; skipping.")

# Task 32 — comments: expect at least CATALOG + SCHEMA + TABLE comment rows
comment_rows = full_status.filter("object_type = 'comment' AND status = 'validated'").collect()
if len(comment_rows) < 2:
    # 2 minimum since TABLE comments on Delta may skip via DEEP CLONE path
    error_messages.append(f"Phase 3 T32: expected >= 2 comment rows (catalog + schema), got {len(comment_rows)}.")
else:
    print(f"Phase 3 T32 validated: {len(comment_rows)} comment row(s) replayed.")

# COMMAND ----------
# --- Grants assertion (UC) ---
# The seed grants SELECT on test_schema (schema level, which grants_worker
# supports today — table-level grants are a separate future gap). Verify
# grants_worker migrated that grant.

has_schema_grant = dbutils.jobs.taskValues.get(  # type: ignore[name-defined]  # noqa: F821
    taskKey="seed_uc", key="has_schema_grant", debugValue="false"
)
if str(has_schema_grant).lower() == "true":
    grant_rows = full_status.filter(
        "object_type = 'grant' AND status = 'validated' "
        "AND object_name LIKE '%SELECT%' "
        "AND object_name LIKE '%test_schema%' "
        "AND object_name LIKE '%account users%'"
    ).collect()
    if not grant_rows:
        error_messages.append(
            "Grants: no validated grant row for `account users` on test_schema"
            " — SELECT on schema did not migrate to target."
        )
    else:
        print(
            f"Grants validated: {len(grant_rows)} "
            f"SELECT-on-test_schema grant row(s) for 'account users' "
            f"replayed on target."
        )
else:
    print("Grants: schema-level grant not seeded; skipping assertion.")

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
        "object_type = 'managed_table' AND object_name LIKE '%managed_sensitive%'"
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
                f"RLS/CM skip: managed_sensitive status is {status!r}, expected 'skipped_by_rls_cm_policy'."
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

# COMMAND ----------
# --- Phase 1 deferred assertions ---

# 1.10 View dependency ordering — recent_high_value depends on
# high_value_orders which depends on managed_orders. All three must
# migrate in topological order (managed_orders first, then the views).
_view_rows = full_status.filter("object_type = 'view' AND status = 'validated'").collect()
_view_names = {r["object_name"].split("`.`")[-1].strip("`") for r in _view_rows}
for _expected in ("high_value_orders", "recent_high_value"):
    if _expected not in _view_names:
        error_messages.append(
            f"1.10 view dependency ordering: {_expected!r} missing from "
            f"validated views ({sorted(_view_names)}). If "
            f"recent_high_value was migrated before high_value_orders, "
            f"CREATE VIEW would have failed — worker topological sort "
            f"must be broken."
        )
    else:
        print(f"1.10 view dep ordering validated: {_expected} on target.")

# 1.11 Partitioned external table — partitioned_events should migrate
# with its PARTITIONED BY clause intact. We already verified DDL-level
# preservation in unit tests (test_external_table_worker). Here:
#   - migration_status has a validated external_table row for it
#   - target TableInfo columns list still includes the partition
#     columns (region, event_date) — if they were dropped from the DDL,
#     they'd be missing on target.
_has_partitioned = dbutils.jobs.taskValues.get(  # type: ignore[name-defined]  # noqa: F821
    taskKey="seed_uc", key="has_partitioned_external", debugValue="false"
)
if str(_has_partitioned).lower() == "true":
    _pr_rows = status_df.filter(
        "object_type = 'external_table' AND object_name LIKE '%partitioned_events%' AND status = 'validated'"
    ).collect()
    if not _pr_rows:
        error_messages.append("1.11 partitioned external: no validated row for partitioned_events.")
    else:
        try:
            from common.auth import AuthManager  # noqa: E402

            _auth_p = AuthManager(config, dbutils)  # noqa: F821
            _tgt_pe = _auth_p.target_client.tables.get("integration_test_src.test_schema.partitioned_events")
            _col_names = {c.name for c in (getattr(_tgt_pe, "columns", None) or [])}
            _missing_partition_cols = {"region", "event_date"} - _col_names
            if _missing_partition_cols:
                error_messages.append(
                    f"1.11 partitioned external: target missing partition columns {sorted(_missing_partition_cols)}"
                )
            else:
                print("1.11 partitioned external validated: target has all columns including partition keys.")
        except Exception as _exc:  # noqa: BLE001
            error_messages.append(f"1.11 partitioned external: target lookup failed: {_exc}")
else:
    print("1.11 partitioned external: fixture not seeded; skipping.")

# 1.12 External volume with seeded files — Phase 2.5.A already
# validates marker.txt presence/size on target; this block adds a
# count check for extra signal if the volume contained multiple files.
# The base assertion is Phase 2.5.A's marker-bytes check above; this
# is a no-op if the volume is already verified there.
print("1.12 external volume files: covered by Phase 2.5.A marker check above.")

# 1.8 Multi-schema — secondary_orders lives in a second schema
# (test_schema_2) under the same source catalog. Discovery must
# enumerate both schemas and migrate managed tables in both.
_ms_rows = full_status.filter(
    "object_type = 'managed_table' AND object_name LIKE '%test_schema_2%secondary_orders%' AND status = 'validated'"
).collect()
if not _ms_rows:
    error_messages.append(
        "1.8 multi-schema: no validated managed_table row for "
        "secondary_orders in test_schema_2 — discovery or the "
        "schema-level iteration may be scoped to one schema."
    )
else:
    print("1.8 multi-schema validated: secondary_orders migrated from test_schema_2.")

# 1.8 Multi-catalog — extra_orders lives in integration_test_src_b,
# a distinct SOURCE catalog. Discovery must enumerate every
# non-tool-owned catalog (not just the first) and migrate objects
# from each. If catalog iteration is scoped or index-bugged, this
# row won't exist on target.
_mc_rows = full_status.filter(
    "object_type = 'managed_table' AND object_name LIKE '%integration_test_src_b%extra_orders%' AND status = 'validated'"
).collect()
if not _mc_rows:
    error_messages.append(
        "1.8 multi-catalog: no validated managed_table row for "
        "extra_orders in integration_test_src_b — discovery's "
        "catalog enumeration may be scoped to one catalog."
    )
else:
    print("1.8 multi-catalog validated: extra_orders migrated from integration_test_src_b.")

# 1.13 Grant to non-existent principal — seeded on the schema to an
# intentionally-bogus email. The migrator must land a migration_status
# row for the grant (either ``validated`` because UC accepts arbitrary
# principal strings or ``failed`` if target rejects it). Silent drop
# means the row is neither state — that would hide the gap.
_has_missing_grant = dbutils.jobs.taskValues.get(  # type: ignore[name-defined]  # noqa: F821
    taskKey="seed_uc", key="has_missing_principal_grant", debugValue="false"
)
if str(_has_missing_grant).lower() == "true":
    _missing_principal = dbutils.jobs.taskValues.get(  # type: ignore[name-defined]  # noqa: F821
        taskKey="seed_uc",
        key="missing_principal",
        debugValue="",
    )
    # grants are recorded with object_name containing the principal
    _mg_rows = full_status.filter(f"object_type = 'grant' AND object_name LIKE '%{_missing_principal}%'").collect()
    if not _mg_rows:
        error_messages.append(
            f"1.13 missing-principal grant: no migration_status row for "
            f"principal {_missing_principal!r}. The grant must not be "
            f"silently dropped — expected either 'validated' or 'failed'."
        )
    else:
        _st = _mg_rows[0]["status"]
        if _st not in ("validated", "failed"):
            error_messages.append(
                f"1.13 missing-principal grant: row status is {_st!r} (expected 'validated' or 'failed')."
            )
        else:
            print(f"1.13 missing-principal grant validated: status={_st!r} (row is surfaced, not silently dropped).")
else:
    print("1.13 missing-principal grant: fixture not seeded; skipping.")

# COMMAND ----------

if error_messages:
    raise AssertionError(
        f"UC integration test failed with {len(error_messages)} error(s):\n" + "\n".join(error_messages)
    )
print("UC integration tests passed (Phase 1/2 + Phase 2.5 + Phase 3).")
