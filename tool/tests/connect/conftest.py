"""Fixtures for Databricks Connect tests using databricks-sql-connector.

Uses the Statement Execution REST API (HTTPS) instead of Spark Connect (gRPC),
which works through corporate VPNs that block HTTP/2.

Requires:
  - databricks-sql-connector installed
  - Valid auth (ARM_TENANT_ID, ARM_CLIENT_ID, ARM_CLIENT_SECRET env vars)
  - DATABRICKS_HOST env var
  - A SQL warehouse on the workspace (auto-discovered)
"""
from __future__ import annotations

import os

import pytest
from databricks import sql as dbsql
from databricks.sdk import WorkspaceClient


@pytest.fixture(scope="session")
def workspace_client():
    """Authenticated WorkspaceClient using env vars."""
    return WorkspaceClient()


@pytest.fixture(scope="session")
def warehouse_id(workspace_client):
    """Find the first available SQL warehouse."""
    warehouses = list(workspace_client.warehouses.list())
    for wh in warehouses:
        if wh.state and wh.state.value in ("RUNNING", "STARTING"):
            return wh.id
    if warehouses:
        return warehouses[0].id
    pytest.skip("No SQL warehouse available on workspace")


@pytest.fixture(scope="session")
def sql_connection(warehouse_id):
    """SQL connection via databricks-sql-connector."""
    host = os.environ["DATABRICKS_HOST"].replace("https://", "")
    conn = dbsql.connect(
        server_hostname=host,
        http_path=f"/sql/1.0/warehouses/{warehouse_id}",
    )
    yield conn
    conn.close()


@pytest.fixture(scope="session")
def sql(sql_connection):
    """Helper that executes SQL and returns rows as list of dicts."""

    def _exec(statement: str) -> list[dict]:
        cursor = sql_connection.cursor()
        cursor.execute(statement)
        if cursor.description:
            columns = [desc[0] for desc in cursor.description]
            rows = [dict(zip(columns, row)) for row in cursor.fetchall()]
        else:
            rows = []
        cursor.close()
        return rows

    return _exec


@pytest.fixture(scope="session")
def test_catalog():
    return "connect_test_src"


@pytest.fixture(scope="session")
def test_schema():
    return "test_schema"


@pytest.fixture(scope="session", autouse=True)
def setup_test_data(sql, test_catalog, test_schema):
    """Create test catalog, schema, and objects."""
    sql(f"CREATE CATALOG IF NOT EXISTS {test_catalog}")
    sql(f"CREATE SCHEMA IF NOT EXISTS {test_catalog}.{test_schema}")

    # Managed table
    sql(f"DROP TABLE IF EXISTS {test_catalog}.{test_schema}.orders")
    sql(f"""
        CREATE TABLE {test_catalog}.{test_schema}.orders (
            order_id INT, customer_id INT, amount DOUBLE
        ) USING DELTA
    """)
    sql(f"""
        INSERT INTO {test_catalog}.{test_schema}.orders VALUES
        (1, 100, 99.99), (2, 101, 149.99), (3, 102, 50.00)
    """)

    # View
    sql(f"""
        CREATE OR REPLACE VIEW {test_catalog}.{test_schema}.high_value_orders AS
        SELECT * FROM {test_catalog}.{test_schema}.orders WHERE amount > 100
    """)

    # Function
    sql(f"""
        CREATE OR REPLACE FUNCTION {test_catalog}.{test_schema}.double_it(x DOUBLE)
        RETURNS DOUBLE
        RETURN x * 2
    """)

    yield

    # Teardown
    sql(f"DROP CATALOG IF EXISTS {test_catalog} CASCADE")
