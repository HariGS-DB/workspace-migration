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

# Seed UC test data: create a small source catalog with table, view, function, volume.
# Pre-seed hygiene checks (S.14/S.15) were removed after a test run showed
# signs they introduced side-effects — the assertions they offered are
# covered by the unit tests for teardown behavior instead.

spark.sql("CREATE CATALOG IF NOT EXISTS integration_test_src")  # noqa: F821
spark.sql("CREATE SCHEMA IF NOT EXISTS integration_test_src.test_schema")  # noqa: F821
# --- 1.8 Multi-schema fixture ---
# A second schema under the same catalog with a distinct managed table.
# Discovery should enumerate both schemas; managed_table_worker should
# clone both. The assertion in test_uc_end_to_end verifies the target
# sees a validated row for the second-schema table — if discovery or
# the schema-level filter accidentally scopes to one schema, the row
# won't exist.
spark.sql("CREATE SCHEMA IF NOT EXISTS integration_test_src.test_schema_2")  # noqa: F821

# COMMAND ----------

spark.sql(  # noqa: F821
    """
    CREATE OR REPLACE TABLE integration_test_src.test_schema_2.secondary_orders (
        order_id INT,
        amount DOUBLE
    ) USING DELTA
    """
)
spark.sql(  # noqa: F821
    """
    INSERT INTO integration_test_src.test_schema_2.secondary_orders VALUES
        (10, 10.0),
        (11, 20.0)
    """
)

# COMMAND ----------

spark.sql(  # noqa: F821
    """
    CREATE OR REPLACE TABLE integration_test_src.test_schema.managed_orders (
        order_id INT,
        customer_id INT,
        amount DOUBLE,
        order_date DATE
    ) USING DELTA
    """
)
spark.sql(  # noqa: F821
    """
    INSERT INTO integration_test_src.test_schema.managed_orders VALUES
        (1, 100, 250.00, '2024-01-15'),
        (2, 200, 75.50, '2024-01-16')
    """
)

# COMMAND ----------

spark.sql(  # noqa: F821
    """
    CREATE OR REPLACE VIEW integration_test_src.test_schema.high_value_orders AS
    SELECT * FROM integration_test_src.test_schema.managed_orders
    WHERE amount > 100
    """
)

# --- View-over-view (Phase 1 integration 1.10) ---
# View dependency ordering: ``recent_high_value`` references
# ``high_value_orders``, which references ``managed_orders``. The
# views worker must migrate them in topological order (leaves first)
# so each view's upstream exists on target when it's CREATE'd.
spark.sql(  # noqa: F821
    """
    CREATE OR REPLACE VIEW integration_test_src.test_schema.recent_high_value AS
    SELECT * FROM integration_test_src.test_schema.high_value_orders
    WHERE order_date > '2024-01-01'
    """
)

# COMMAND ----------

spark.sql(  # noqa: F821
    """
    CREATE OR REPLACE FUNCTION integration_test_src.test_schema.double_amount(x DOUBLE)
    RETURNS DOUBLE
    RETURN x * 2
    """
)

# COMMAND ----------

spark.sql(  # noqa: F821
    "CREATE VOLUME IF NOT EXISTS integration_test_src.test_schema.test_volume"
)

# --- Partitioned external Delta table (Phase 1 integration 1.11) ---
# Source-side PARTITIONED BY must survive DDL replay + arrive on target
# with partition metadata intact. The unit test for the sanitizer +
# rewrite_ddl locks in the DDL shape; this integration verifies the
# round-trip on a live target.
_partitioned_location = (
    "abfss://external-data@stextsourcemig36cd38.dfs.core.windows.net/"
    "partitioned_events"
)
_has_partitioned_external = False
try:
    spark.sql(  # noqa: F821
        f"""
        CREATE OR REPLACE TABLE integration_test_src.test_schema.partitioned_events (
            event_id INT,
            region STRING,
            event_date DATE,
            payload STRING
        )
        USING DELTA
        PARTITIONED BY (region, event_date)
        LOCATION '{_partitioned_location}'
        """
    )
    spark.sql(  # noqa: F821
        "INSERT OVERWRITE TABLE integration_test_src.test_schema.partitioned_events VALUES "
        "(1, 'US', DATE'2024-01-15', 'a'), "
        "(2, 'UK', DATE'2024-01-15', 'b'), "
        "(3, 'US', DATE'2024-01-16', 'c')"
    )
    _has_partitioned_external = True
    print("Created partitioned external table partitioned_events with 3 rows.")
except Exception as _exc:  # noqa: BLE001
    print(f"Skipped partitioned external seed: {_exc}")
dbutils.jobs.taskValues.set(  # type: ignore[name-defined]  # noqa: F821
    key="has_partitioned_external",
    value="true" if _has_partitioned_external else "false",
)

# Seed a small file into the managed volume so Phase 2.5.A data-copy can be
# verified end-to-end. Byte count is surfaced as a task value so the assertion
# step knows what to expect on target.
_VOLUME_FILE_CONTENT = b"phase-2.5 integration-test marker file\n"
dbutils.fs.put(  # type: ignore[name-defined]  # noqa: F821
    "/Volumes/integration_test_src/test_schema/test_volume/marker.txt",
    _VOLUME_FILE_CONTENT.decode(),
    overwrite=True,
)
dbutils.jobs.taskValues.set(  # type: ignore[name-defined]  # noqa: F821
    key="volume_marker_bytes", value=str(len(_VOLUME_FILE_CONTENT))
)

# COMMAND ----------

# --- Phase 2.5.C: Python UDF ---
spark.sql(  # noqa: F821
    """
    CREATE OR REPLACE FUNCTION integration_test_src.test_schema.py_double(x DOUBLE)
    RETURNS DOUBLE
    LANGUAGE PYTHON
    AS $$
def handler(x):
    return x * 2

return handler(x)
    $$
    """
)

# COMMAND ----------

# --- Phase 2.5.D: SQL-created materialized view ---
# Wrapped in try/except because MV support requires a compatible DBR.
_has_mv = False
try:
    spark.sql(  # noqa: F821
        """
        CREATE OR REPLACE MATERIALIZED VIEW integration_test_src.test_schema.mv_high_value
        AS SELECT * FROM integration_test_src.test_schema.managed_orders WHERE amount > 100
        """
    )
    _has_mv = True
    print("Created materialized view mv_high_value.")
except Exception as _exc:  # noqa: BLE001
    print(f"Skipped MV seed (unsupported on this runtime): {_exc}")

dbutils.jobs.taskValues.set(  # type: ignore[name-defined]  # noqa: F821
    key="has_mv", value="true" if _has_mv else "false"
)

# COMMAND ----------

# --- Phase 2.5.D: SQL-created streaming table ---
# Requires a streaming source. Wrapped because not all runtimes support
# CREATE STREAMING TABLE from a warehouse / notebook context.
_has_st = False
try:
    spark.sql(  # noqa: F821
        """
        CREATE OR REPLACE STREAMING TABLE integration_test_src.test_schema.st_orders
        AS SELECT * FROM STREAM(integration_test_src.test_schema.managed_orders)
        """
    )
    _has_st = True
    print("Created streaming table st_orders.")
except Exception as _exc:  # noqa: BLE001
    print(f"Skipped ST seed (unsupported on this runtime): {_exc}")

dbutils.jobs.taskValues.set(  # type: ignore[name-defined]  # noqa: F821
    key="has_st", value="true" if _has_st else "false"
)

# COMMAND ----------

# --- Phase 2.5.B: UC-managed Iceberg table ---
# UC-managed native Iceberg is preview-feature-gated in some workspaces;
# guard so the seed doesn't fail the whole test when unavailable.
_has_iceberg = False
try:
    spark.sql(  # noqa: F821
        """
        CREATE OR REPLACE TABLE integration_test_src.test_schema.iceberg_sales (
            sale_id INT,
            customer_id INT,
            amount DOUBLE
        ) USING ICEBERG
        """
    )
    spark.sql(  # noqa: F821
        """
        INSERT INTO integration_test_src.test_schema.iceberg_sales VALUES
            (1, 100, 42.00),
            (2, 200, 19.95),
            (3, 300, 7.50)
        """
    )
    _has_iceberg = True
    print("Created Iceberg table iceberg_sales with 3 rows.")
except Exception as _exc:  # noqa: BLE001
    print(f"Skipped Iceberg seed (unsupported on this runtime): {_exc}")

dbutils.jobs.taskValues.set(  # type: ignore[name-defined]  # noqa: F821
    key="has_iceberg", value="true" if _has_iceberg else "false"
)

# COMMAND ----------

# --- Phase 3 governance fixtures ---

# Task 28 — Tags on a table and on a column
_has_tag = False
try:
    spark.sql(  # noqa: F821
        "ALTER TABLE integration_test_src.test_schema.managed_orders "
        "SET TAGS ('env' = 'test', 'phase' = '3')"
    )
    spark.sql(  # noqa: F821
        "ALTER TABLE integration_test_src.test_schema.managed_orders "
        "ALTER COLUMN customer_id SET TAGS ('pii' = 'true')"
    )
    _has_tag = True
    print("Applied tags to managed_orders (table + column).")
except Exception as _exc:  # noqa: BLE001
    print(f"Skipped tag seed (unsupported on this runtime): {_exc}")
dbutils.jobs.taskValues.set(  # type: ignore[name-defined]  # noqa: F821
    key="has_tag", value="true" if _has_tag else "false"
)

# COMMAND ----------

# Task 29 / 30 — Row filter + column mask applied to an EXTERNAL table.
#
# Delta Sharing refuses tables that carry row-level security or column masks
# (InvalidParameterValue), so applying these to a managed table breaks
# setup_sharing. External tables are migrated via DDL replay (not Delta
# Sharing), so they can carry filters/masks without blocking the share.
# The migration tool's setup_sharing → filter/mask workers chain covers
# external tables fine; discovery reads filter/mask metadata from
# information_schema regardless of table_type.

_external_customers_location = (
    "abfss://external-data@stextsourcemig36cd38.dfs.core.windows.net/external_customers"
)

_has_row_filter = False
_has_column_mask = False
try:
    spark.sql(  # noqa: F821
        f"""
        CREATE OR REPLACE TABLE integration_test_src.test_schema.external_customers (
            customer_id INT,
            name STRING,
            region STRING
        )
        USING DELTA
        LOCATION '{_external_customers_location}'
        """
    )
    spark.sql(  # noqa: F821
        "INSERT INTO integration_test_src.test_schema.external_customers VALUES "
        "(1, 'Alice', 'US'), (2, 'Bob', 'UK')"
    )
    spark.sql(  # noqa: F821
        """
        CREATE OR REPLACE FUNCTION integration_test_src.test_schema.region_filter(region STRING)
        RETURNS BOOLEAN
        RETURN region = 'US' OR is_account_group_member('admins')
        """
    )
    spark.sql(  # noqa: F821
        "ALTER TABLE integration_test_src.test_schema.external_customers "
        "SET ROW FILTER integration_test_src.test_schema.region_filter ON (region)"
    )
    _has_row_filter = True
    print("Applied row filter region_filter on external_customers.region.")

    spark.sql(  # noqa: F821
        """
        CREATE OR REPLACE FUNCTION integration_test_src.test_schema.mask_customer(cid INT)
        RETURNS INT
        RETURN CASE WHEN is_account_group_member('admins') THEN cid ELSE -1 END
        """
    )
    spark.sql(  # noqa: F821
        "ALTER TABLE integration_test_src.test_schema.external_customers "
        "ALTER COLUMN customer_id SET MASK integration_test_src.test_schema.mask_customer"
    )
    _has_column_mask = True
    print("Applied column mask mask_customer on external_customers.customer_id.")
except Exception as _exc:  # noqa: BLE001
    print(f"Skipped row filter / column mask seed: {_exc}")

dbutils.jobs.taskValues.set(  # type: ignore[name-defined]  # noqa: F821
    key="has_row_filter", value="true" if _has_row_filter else "false"
)
dbutils.jobs.taskValues.set(  # type: ignore[name-defined]  # noqa: F821
    key="has_column_mask", value="true" if _has_column_mask else "false"
)

# COMMAND ----------

# --- RLS/CM skip-path fixture ---
# A managed Delta table with row filter AND column mask. Delta Sharing
# refuses to share these, so with rls_cm_strategy="" (default) the tool
# must skip the table and record status=skipped_by_rls_cm_policy. The
# companion assertion in test_uc_end_to_end.py verifies that.

_has_rls_cm_managed = False
try:
    spark.sql(  # noqa: F821
        """
        CREATE OR REPLACE TABLE integration_test_src.test_schema.managed_sensitive (
            record_id INT,
            account_id INT,
            region STRING
        ) USING DELTA
        """
    )
    spark.sql(  # noqa: F821
        "INSERT INTO integration_test_src.test_schema.managed_sensitive VALUES "
        "(1, 100, 'US'), (2, 200, 'UK'), (3, 300, 'US')"
    )
    # Reuse region_filter + mask_customer from above (both were created
    # in integration_test_src.test_schema).
    spark.sql(  # noqa: F821
        "ALTER TABLE integration_test_src.test_schema.managed_sensitive "
        "SET ROW FILTER integration_test_src.test_schema.region_filter ON (region)"
    )
    spark.sql(  # noqa: F821
        "ALTER TABLE integration_test_src.test_schema.managed_sensitive "
        "ALTER COLUMN account_id SET MASK integration_test_src.test_schema.mask_customer"
    )
    _has_rls_cm_managed = True
    print("Applied row filter + column mask on managed_sensitive (managed table).")
except Exception as _exc:  # noqa: BLE001
    print(f"Skipped managed RLS/CM seed: {_exc}")
dbutils.jobs.taskValues.set(  # type: ignore[name-defined]  # noqa: F821
    key="has_rls_cm_managed", value="true" if _has_rls_cm_managed else "false"
)

# COMMAND ----------

# Task 32 — Comments on catalog/schema/table
try:
    spark.sql(  # noqa: F821
        "COMMENT ON CATALOG integration_test_src IS 'Phase 3 integration test catalog'"
    )
    spark.sql(  # noqa: F821
        "COMMENT ON SCHEMA integration_test_src.test_schema IS 'Phase 3 test schema'"
    )
    spark.sql(  # noqa: F821
        "COMMENT ON TABLE integration_test_src.test_schema.managed_orders "
        "IS 'Orders fixture — carries tags, row filter, and column mask'"
    )
    print("Applied comments on catalog/schema/table.")
except Exception as _exc:  # noqa: BLE001
    print(f"Skipped comments seed: {_exc}")

# COMMAND ----------

# Grant the migration SPN permissions to read the source catalog.
from common.config import MigrationConfig  # noqa: E402
config = MigrationConfig.from_workspace_file()
if config.spn_client_id:
    spark.sql(  # noqa: F821
        f"GRANT USE CATALOG, USE SCHEMA, SELECT, EXECUTE, READ VOLUME ON CATALOG integration_test_src TO `{config.spn_client_id}`"
    )
    print(f"Granted migration SPN {config.spn_client_id} perms on integration_test_src.")

# Schema-level grant to a well-known principal (``account users``) so
# test_uc_end_to_end can assert a specific grant replays on target.
# ``account users`` exists on every Databricks account, so the grant is
# portable across test environments.
# Note: table-level grants are out of scope for grants_worker today
# (only CATALOG + SCHEMA grants migrate). Seeding at schema level so
# the assertion exercises a path the tool actually supports.
_has_schema_grant = False
try:
    spark.sql(  # noqa: F821
        "GRANT SELECT ON SCHEMA integration_test_src.test_schema "
        "TO `account users`"
    )
    _has_schema_grant = True
    print("Granted SELECT on test_schema to `account users`.")
except Exception as _exc:  # noqa: BLE001
    print(f"Skipped schema-level grant seed: {_exc}")
dbutils.jobs.taskValues.set(  # type: ignore[name-defined]  # noqa: F821
    key="has_schema_grant", value="true" if _has_schema_grant else "false"
)

# COMMAND ----------

# --- 1.13 Grant to non-existent principal ---
# Seed a grant to a principal that is unlikely to exist on target
# (a deterministic bogus name). The grants_worker should either:
#   (a) migrate it successfully (UC accepts arbitrary string identifiers
#       without validating principal existence at grant time), or
#   (b) record status=failed with an operator-readable error.
# Either is acceptable; the assertion in test_uc_end_to_end verifies the
# row lands in migration_status and the status reflects one of the two,
# never silently disappearing.

_has_missing_principal_grant = False
_missing_principal = "nonexistent-group-integration-test-1a2b3c4d@example.invalid"
try:
    spark.sql(  # noqa: F821
        f"GRANT SELECT ON SCHEMA integration_test_src.test_schema "
        f"TO `{_missing_principal}`"
    )
    _has_missing_principal_grant = True
    print(f"Seeded grant to missing principal {_missing_principal!r}.")
except Exception as _exc:  # noqa: BLE001
    # Some source metastores reject unknown principals at grant time;
    # leave the task value false so the assertion skips.
    print(
        f"Skipped missing-principal grant seed (source rejected): {_exc}"
    )
dbutils.jobs.taskValues.set(  # type: ignore[name-defined]  # noqa: F821
    key="has_missing_principal_grant",
    value="true" if _has_missing_principal_grant else "false",
)
dbutils.jobs.taskValues.set(  # type: ignore[name-defined]  # noqa: F821
    key="missing_principal", value=_missing_principal
)

# COMMAND ----------

print("UC seed data created successfully.")
