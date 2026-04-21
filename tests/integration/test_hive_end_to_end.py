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

# Hive end-to-end assertion: verify all seeded Hive objects migrated to UC
# target under {hive_target_catalog}.integration_test_hive.

from common.config import MigrationConfig
from common.tracking import TrackingManager

config = MigrationConfig.from_workspace_file()
tracker = TrackingManager(spark, config)  # noqa: F821

status_df = tracker.get_latest_migration_status()
hive_types = ("hive_view", "hive_function", "hive_managed_dbfs_root", "hive_managed_nondbfs", "hive_external", "hive_grant")
status_df = status_df.filter(status_df.object_type.isin(list(hive_types)))

total = status_df.count()
print(f"Total Hive migrated objects: {total}")
assert total > 0, "No Hive migration status records found."

error_messages: list[str] = []

counts = {row["status"]: row["n"] for row in status_df.groupBy("status").count().withColumnRenamed("count", "n").collect()}
print(f"Hive status breakdown: {counts}")

# Expected object types from seed_hive_test_data
expected_types = ["hive_view", "hive_function"]
if config.migrate_hive_dbfs_root:
    expected_types.append("hive_managed_dbfs_root")

for htype in expected_types:
    rows = status_df.filter(f"object_type = '{htype}'").collect()
    if not rows:
        error_messages.append(f"No {htype} records in migration_status.")
        continue
    accepted = ("validated",)
    if htype == "hive_managed_dbfs_root" and not config.migrate_hive_dbfs_root:
        accepted = ("validated", "skipped_by_config")
    failed = [r for r in rows if r["status"] not in accepted]
    if failed:
        for row in failed:
            error_messages.append(
                f"Hive {htype}: {row['object_name']} [{row['status']}]: {row['error_message']}"
            )
    else:
        print(f"Hive {htype}: {len(rows)} object(s) OK")

# Data-level row-count check: rely on migration_status (worker records source
# row count + target row count after DEEP CLONE / data copy) — we can't query
# the target catalog directly from this source-side notebook since the target
# metastore isn't visible here.
if config.migrate_hive_dbfs_root:
    dbfs_rows = status_df.filter("object_type = 'hive_managed_dbfs_root' AND status = 'validated'").collect()
    if not dbfs_rows:
        error_messages.append("No validated hive_managed_dbfs_root records (DBFS-root migration did not run or did not validate).")
    else:
        for row in dbfs_rows:
            if row["source_row_count"] != row["target_row_count"]:
                error_messages.append(
                    f"DBFS-root row mismatch for {row['object_name']}: "
                    f"src={row['source_row_count']} tgt={row['target_row_count']}"
                )
            else:
                print(f"{row['object_name']}: src/tgt rows match ({row['source_row_count']})")

# COMMAND ----------
# --- Hive external + managed non-DBFS assertions ---
# Both workers run on every Hive migration but had zero input before the
# corresponding seed was added. Now we assert each seeded category is
# validated and its row count matches across source and target.

has_hive_external = dbutils.jobs.taskValues.get(  # type: ignore[name-defined]  # noqa: F821
    taskKey="seed_hive", key="has_hive_external", debugValue="false"
)
if str(has_hive_external).lower() == "true":
    ext_rows = status_df.filter(
        "object_type = 'hive_external' AND object_name LIKE '%external_invoices%'"
    ).collect()
    if not ext_rows:
        error_messages.append(
            "hive_external: no migration_status row for external_invoices."
        )
    else:
        row = ext_rows[0]
        if row["status"] != "validated":
            error_messages.append(
                f"hive_external: external_invoices status is {row['status']!r} "
                f"(expected 'validated'); error={row['error_message']}"
            )
        elif row["source_row_count"] != row["target_row_count"]:
            error_messages.append(
                f"hive_external: external_invoices row mismatch "
                f"(src={row['source_row_count']}, tgt={row['target_row_count']})"
            )
        else:
            print(
                f"hive_external validated: external_invoices rows match "
                f"({row['source_row_count']})"
            )
else:
    print(
        "hive_external: fixture not seeded (requires migrate_hive_dbfs_root + "
        "hive_dbfs_target_path); skipping assertion."
    )

has_hive_nondbfs = dbutils.jobs.taskValues.get(  # type: ignore[name-defined]  # noqa: F821
    taskKey="seed_hive", key="has_hive_nondbfs", debugValue="false"
)
if str(has_hive_nondbfs).lower() == "true":
    nd_rows = status_df.filter(
        "object_type = 'hive_managed_nondbfs' AND object_name LIKE '%nondbfs_sales%'"
    ).collect()
    if not nd_rows:
        error_messages.append(
            "hive_managed_nondbfs: no migration_status row for nondbfs_sales."
        )
    else:
        row = nd_rows[0]
        if row["status"] != "validated":
            error_messages.append(
                f"hive_managed_nondbfs: nondbfs_sales status is {row['status']!r} "
                f"(expected 'validated'); error={row['error_message']}"
            )
        elif row["source_row_count"] != row["target_row_count"]:
            error_messages.append(
                f"hive_managed_nondbfs: nondbfs_sales row mismatch "
                f"(src={row['source_row_count']}, tgt={row['target_row_count']})"
            )
        else:
            print(
                f"hive_managed_nondbfs validated: nondbfs_sales rows match "
                f"({row['source_row_count']})"
            )
else:
    print(
        "hive_managed_nondbfs: fixture not seeded (requires migrate_hive_dbfs_root "
        "+ hive_dbfs_target_path); skipping assertion."
    )

# COMMAND ----------
# --- Hive grants assertion ---
# Seed grants SELECT on managed_orders to ``account users``. Verify
# hive_grants_worker migrated that grant.

has_hive_grant = dbutils.jobs.taskValues.get(  # type: ignore[name-defined]  # noqa: F821
    taskKey="seed_hive", key="has_hive_grant", debugValue="false"
)
if str(has_hive_grant).lower() == "true":
    full_status_hive = tracker.get_latest_migration_status()
    hg_rows = full_status_hive.filter(
        "object_type = 'hive_grant' AND status = 'validated' "
        "AND object_name LIKE '%account users%'"
    ).collect()
    if not hg_rows:
        error_messages.append(
            "hive_grants: no validated hive_grant row for `account users` — "
            "SELECT on Hive schema did not migrate to target."
        )
    else:
        print(
            f"hive_grants validated: {len(hg_rows)} hive_grant row(s) for "
            f"'account users' replayed."
        )
else:
    print("hive_grants: grant fixture not seeded; skipping assertion.")

# COMMAND ----------

if error_messages:
    raise AssertionError(
        f"Hive integration test failed with {len(error_messages)} error(s):\n"
        + "\n".join(error_messages)
    )
print("Hive integration tests passed.")
