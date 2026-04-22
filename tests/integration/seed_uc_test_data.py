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

spark.sql("CREATE CATALOG IF NOT EXISTS integration_test_src")  # noqa: F821
spark.sql("CREATE SCHEMA IF NOT EXISTS integration_test_src.test_schema")  # noqa: F821

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

# Task 29 — Row filter function + ALTER TABLE SET ROW FILTER
_has_row_filter = False
try:
    spark.sql(  # noqa: F821
        """
        CREATE OR REPLACE FUNCTION integration_test_src.test_schema.region_filter(region STRING)
        RETURNS BOOLEAN
        RETURN region = 'US' OR is_account_group_member('admins')
        """
    )
    # Row filters need a column to filter on — add one and populate
    spark.sql(  # noqa: F821
        "ALTER TABLE integration_test_src.test_schema.managed_orders "
        "ADD COLUMN IF NOT EXISTS region STRING"
    )
    spark.sql(  # noqa: F821
        "UPDATE integration_test_src.test_schema.managed_orders SET region = 'US' WHERE order_id = 1"
    )
    spark.sql(  # noqa: F821
        "UPDATE integration_test_src.test_schema.managed_orders SET region = 'UK' WHERE order_id = 2"
    )
    spark.sql(  # noqa: F821
        "ALTER TABLE integration_test_src.test_schema.managed_orders "
        "SET ROW FILTER integration_test_src.test_schema.region_filter ON (region)"
    )
    _has_row_filter = True
    print("Applied row filter region_filter on managed_orders.region.")
except Exception as _exc:  # noqa: BLE001
    print(f"Skipped row filter seed: {_exc}")
dbutils.jobs.taskValues.set(  # type: ignore[name-defined]  # noqa: F821
    key="has_row_filter", value="true" if _has_row_filter else "false"
)

# COMMAND ----------

# Task 30 — Column mask function + ALTER COLUMN SET MASK
_has_column_mask = False
try:
    spark.sql(  # noqa: F821
        """
        CREATE OR REPLACE FUNCTION integration_test_src.test_schema.mask_customer(cid INT)
        RETURNS INT
        RETURN CASE WHEN is_account_group_member('admins') THEN cid ELSE -1 END
        """
    )
    spark.sql(  # noqa: F821
        "ALTER TABLE integration_test_src.test_schema.managed_orders "
        "ALTER COLUMN customer_id SET MASK integration_test_src.test_schema.mask_customer"
    )
    _has_column_mask = True
    print("Applied column mask mask_customer on managed_orders.customer_id.")
except Exception as _exc:  # noqa: BLE001
    print(f"Skipped column mask seed: {_exc}")
dbutils.jobs.taskValues.set(  # type: ignore[name-defined]  # noqa: F821
    key="has_column_mask", value="true" if _has_column_mask else "false"
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

# --- 3.16 ABAC policy fixture ---
# ABAC policies are a Unity Catalog preview feature with evolving SQL. If
# the workspace doesn't have the preview enabled, SET ABAC POLICY returns
# a syntax error; we catch it and skip gracefully so the assertion step
# can skip too.
_has_abac = False
_abac_skip_reason = ""
try:
    # First create the user-defined filter function the policy references.
    spark.sql(  # noqa: F821
        """
        CREATE OR REPLACE FUNCTION integration_test_src.test_schema.abac_region_filter(region STRING)
        RETURNS BOOLEAN
        RETURN region = 'US' OR is_account_group_member('admins')
        """
    )
    # SQL shape per 2025-03 preview docs: ALTER TABLE ... SET ABAC POLICY <name>
    # FILTER USING <function>(<col>). Older shape: CREATE POLICY <name> ON <t>
    # FILTER USING <fn>(<col>). Try the newer ALTER form first, then fall back
    # to CREATE POLICY if the parser rejects it.
    try:
        spark.sql(  # noqa: F821
            """
            ALTER TABLE integration_test_src.test_schema.managed_orders
            SET ABAC POLICY abac_region_policy FILTER USING
            integration_test_src.test_schema.abac_region_filter(region)
            """
        )
    except Exception as _inner:  # noqa: BLE001
        spark.sql(  # noqa: F821
            """
            CREATE POLICY abac_region_policy ON integration_test_src.test_schema.managed_orders
            FILTER USING integration_test_src.test_schema.abac_region_filter(region)
            """
        )
    _has_abac = True
    print("Applied ABAC policy abac_region_policy.")
except Exception as _exc:  # noqa: BLE001
    _abac_skip_reason = str(_exc)[:300]
    print(
        f"Skipped ABAC seed — preview not available on this workspace. "
        f"Reason: {_abac_skip_reason}"
    )
dbutils.jobs.taskValues.set(  # type: ignore[name-defined]  # noqa: F821
    key="has_abac", value="true" if _has_abac else "false"
)
dbutils.jobs.taskValues.set(  # type: ignore[name-defined]  # noqa: F821
    key="abac_skip_reason", value=_abac_skip_reason
)

# COMMAND ----------

# --- 3.20 Registered model with an artifact file ---
# Create a registered model, register a version, and upload a tiny file to
# the version's storage_location so models_worker has bytes to copy.
_has_model_artifact = False
_model_bytes = 0
_model_version_num = 0
try:
    from common.auth import AuthManager  # noqa: E402
    from common.config import MigrationConfig as _MigrationConfig  # noqa: E402

    _model_cfg = _MigrationConfig.from_workspace_file()
    _model_auth = AuthManager(_model_cfg, dbutils)  # noqa: F821
    _model_client = _model_auth.source_client

    # Create (or re-use) the registered model
    _model_fqn = "integration_test_src.test_schema.integration_model"
    try:
        _model_client.registered_models.create(
            catalog_name="integration_test_src",
            schema_name="test_schema",
            name="integration_model",
            comment="Integration-test registered model (3.20)",
        )
    except Exception as _exc:  # noqa: BLE001
        if "already" not in str(_exc).lower():
            raise

    # Create a model version via SDK so UC allocates a storage_location
    _version = _model_client.model_versions.create(
        catalog_name="integration_test_src",
        schema_name="test_schema",
        model_name="integration_model",
        source="integration-test-seed",  # placeholder source URI
    )
    _model_version_num = int(_version.version)
    _storage_location = getattr(_version, "storage_location", None)
    if not _storage_location:
        raise RuntimeError("Model version has no storage_location")

    # Upload a tiny artifact file into the version's storage_location
    _REQS = b"# integration-test marker\npandas==2.2.0\n"
    _marker_path = _storage_location.rstrip("/") + "/requirements.txt"
    dbutils.fs.put(_marker_path, _REQS.decode(), overwrite=True)  # type: ignore[name-defined]  # noqa: F821
    _model_bytes = len(_REQS)
    _has_model_artifact = True
    print(
        f"Seeded registered model {_model_fqn} v{_model_version_num} with "
        f"{_model_bytes} bytes at {_storage_location}"
    )
except Exception as _exc:  # noqa: BLE001
    print(f"Skipped model artifact seed: {_exc}")

dbutils.jobs.taskValues.set(  # type: ignore[name-defined]  # noqa: F821
    key="has_model_artifact", value="true" if _has_model_artifact else "false"
)
dbutils.jobs.taskValues.set(  # type: ignore[name-defined]  # noqa: F821
    key="model_artifact_bytes", value=str(_model_bytes)
)

# COMMAND ----------

# --- P.1 drop_and_restore RLS/CM fixture ---
# The row filter and column mask created above (tasks 29 / 30) are the
# fixture. drop_and_restore only activates when config.rls_cm_strategy is
# set; when that's the case setup_sharing will strip them, managed_table
# DEEP CLONE runs, then restore_rls_cm reapplies on source. The assertion
# step inspects rls_cm_manifest + re-checks source tables.
_has_rls_cm_fixture = bool(_has_row_filter and _has_column_mask)
dbutils.jobs.taskValues.set(  # type: ignore[name-defined]  # noqa: F821
    key="has_rls_cm_fixture", value="true" if _has_rls_cm_fixture else "false"
)
# Capture the active rls_cm_strategy so the assertion step can know whether
# to run the manifest checks. (Note: the strategy is read from config.yaml
# at workflow runtime; this just mirrors it for debugging.)
try:
    from common.config import MigrationConfig as _RlsMigrationConfig  # noqa: E402

    _rls_cfg = _RlsMigrationConfig.from_workspace_file()
    dbutils.jobs.taskValues.set(  # type: ignore[name-defined]  # noqa: F821
        key="rls_cm_strategy", value=_rls_cfg.rls_cm_strategy or ""
    )
except Exception:  # noqa: BLE001
    pass

# COMMAND ----------

# Grant the migration SPN permissions to read the source catalog.
from common.config import MigrationConfig  # noqa: E402

config = MigrationConfig.from_workspace_file()
if config.spn_client_id:
    spark.sql(  # noqa: F821
        f"GRANT USE CATALOG, USE SCHEMA, SELECT, EXECUTE, READ VOLUME ON CATALOG integration_test_src TO `{config.spn_client_id}`"
    )
    print(f"Granted migration SPN {config.spn_client_id} perms on integration_test_src.")

# COMMAND ----------

print("UC seed data created successfully.")
