"""Test tracking table operations against real Delta tables via SQL connector."""

from __future__ import annotations

import uuid


class TestTrackingManagerConnect:
    def test_init_tables_and_query(self, sql, test_catalog):
        """Test creating tracking tables and querying them via SQL."""
        suffix = uuid.uuid4().hex[:8]
        schema = f"tracking_{suffix}"

        try:
            sql(f"CREATE SCHEMA IF NOT EXISTS {test_catalog}.{schema}")

            # Create the 3 tracking tables
            sql(f"""
                CREATE TABLE IF NOT EXISTS {test_catalog}.{schema}.discovery_inventory (
                    object_name STRING, object_type STRING, catalog_name STRING,
                    schema_name STRING, row_count LONG, size_bytes LONG,
                    is_dlt_managed BOOLEAN, pipeline_id STRING,
                    create_statement STRING, discovered_at TIMESTAMP
                ) USING DELTA
            """)
            sql(f"""
                CREATE TABLE IF NOT EXISTS {test_catalog}.{schema}.migration_status (
                    object_name STRING, object_type STRING, status STRING,
                    error_message STRING, job_run_id STRING, task_run_id STRING,
                    source_row_count LONG, target_row_count LONG,
                    duration_seconds DOUBLE, migrated_at TIMESTAMP
                ) USING DELTA
            """)
            sql(f"""
                CREATE TABLE IF NOT EXISTS {test_catalog}.{schema}.pre_check_results (
                    check_name STRING, status STRING, message STRING,
                    action_required STRING, checked_at TIMESTAMP
                ) USING DELTA
            """)

            # Verify tables exist
            rows = sql(f"SHOW TABLES IN {test_catalog}.{schema}")
            table_names = [r["tableName"] for r in rows]

            assert "discovery_inventory" in table_names
            assert "migration_status" in table_names
            assert "pre_check_results" in table_names

            # Insert and read back
            sql(f"""
                INSERT INTO {test_catalog}.{schema}.migration_status VALUES
                ('test.schema.table1', 'managed_table', 'validated', NULL,
                 'test-run', NULL, 100, 100, 1.5, current_timestamp())
            """)

            rows = sql(f"SELECT * FROM {test_catalog}.{schema}.migration_status")
            assert len(rows) == 1
            assert rows[0]["object_name"] == "test.schema.table1"
            assert rows[0]["status"] == "validated"
            assert rows[0]["source_row_count"] == 100

        finally:
            sql(f"DROP SCHEMA IF EXISTS {test_catalog}.{schema} CASCADE")
