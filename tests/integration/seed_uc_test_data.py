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
