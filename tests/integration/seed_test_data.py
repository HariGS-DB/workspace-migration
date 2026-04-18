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

# Seed test data: create a small source catalog with tables, views, functions, and volumes
# for integration testing of the migration tool.

# COMMAND ----------

spark.sql("CREATE CATALOG IF NOT EXISTS integration_test_src")  # noqa: F821
spark.sql("CREATE SCHEMA IF NOT EXISTS integration_test_src.test_schema")  # noqa: F821

# COMMAND ----------

# Managed table with sample data
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

# View filtering high-value orders
spark.sql(  # noqa: F821
    """
    CREATE OR REPLACE VIEW integration_test_src.test_schema.high_value_orders AS
    SELECT * FROM integration_test_src.test_schema.managed_orders
    WHERE amount > 100
    """
)

# COMMAND ----------

# SQL function
spark.sql(  # noqa: F821
    """
    CREATE OR REPLACE FUNCTION integration_test_src.test_schema.double_amount(x DOUBLE)
    RETURNS DOUBLE
    RETURN x * 2
    """
)

# COMMAND ----------

# Managed volume
spark.sql(  # noqa: F821
    """
    CREATE VOLUME IF NOT EXISTS integration_test_src.test_schema.test_volume
    """
)

# COMMAND ----------

# Grant the migration SPN permissions to read the source catalog
from common.config import MigrationConfig  # noqa: E402
config = MigrationConfig.from_workspace_file()
if config.spn_client_id:
    spark.sql(  # noqa: F821
        f"GRANT USE CATALOG, USE SCHEMA, SELECT, EXECUTE, READ VOLUME ON CATALOG integration_test_src TO `{config.spn_client_id}`"
    )
    print(f"Granted migration SPN {config.spn_client_id} perms on integration_test_src.")

# COMMAND ----------

print("Seed data created successfully.")
