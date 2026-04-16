# Databricks notebook source

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

print("Seed data created successfully.")
