from __future__ import annotations

from datetime import datetime, timezone
from unittest.mock import MagicMock

from common.tracking import TrackingManager, discovery_row, discovery_schema


class TestTrackingManager:
    """Tests for the TrackingManager class."""

    def test_init_tracking_tables_creates_schema(self, mock_spark, mock_config):
        mgr = TrackingManager(mock_spark, mock_config)
        mgr.init_tracking_tables()

        sql_calls = [c.args[0] for c in mock_spark.sql.call_args_list]

        assert any("CREATE CATALOG IF NOT EXISTS migration_tracking" in s for s in sql_calls)
        assert any("CREATE SCHEMA IF NOT EXISTS migration_tracking.cp_migration" in s for s in sql_calls)
        fqn = "migration_tracking.cp_migration"
        assert any(f"CREATE TABLE IF NOT EXISTS {fqn}.discovery_inventory" in s for s in sql_calls)
        assert any(f"CREATE TABLE IF NOT EXISTS {fqn}.migration_status" in s for s in sql_calls)
        assert any(f"CREATE TABLE IF NOT EXISTS {fqn}.pre_check_results" in s for s in sql_calls)

        # Unified discovery_inventory includes source_type and metadata_json columns
        discovery_ddl = next(s for s in sql_calls if "discovery_inventory" in s)
        assert "source_type STRING" in discovery_ddl
        assert "metadata_json STRING" in discovery_ddl
        assert "format STRING" in discovery_ddl

    def test_append_migration_status(self, mock_spark, mock_config):
        mgr = TrackingManager(mock_spark, mock_config)

        mock_df = MagicMock()
        mock_spark.createDataFrame.return_value = mock_df
        mock_df.withColumn.return_value = mock_df

        records = [
            {
                "object_name": "catalog.schema.table1",
                "object_type": "managed_table",
                "status": "migrated",
                "error_message": None,
                "job_run_id": "123",
                "task_run_id": "456",
                "source_row_count": 100,
                "target_row_count": 100,
                "duration_seconds": 5.0,
            }
        ]
        mgr.append_migration_status(records)

        # Verify createDataFrame was called, but don't lock in the exact args —
        # the implementation now normalizes records and passes an explicit schema
        # (both needed to avoid the `CANNOT_DETERMINE_TYPE` integration failure).
        mock_spark.createDataFrame.assert_called_once()
        assert "schema" in mock_spark.createDataFrame.call_args.kwargs, (
            "createDataFrame must be called with an explicit schema kwarg to avoid "
            "type inference failures on all-None columns"
        )
        mock_df.withColumn.assert_called_once()
        assert mock_df.withColumn.call_args[0][0] == "migrated_at"
        mock_df.write.mode.assert_called_once_with("append")
        mock_df.write.mode.return_value.saveAsTable.assert_called_once_with(
            "migration_tracking.cp_migration.migration_status"
        )

    def test_get_pending_objects_filters_completed(self, mock_spark, mock_config):
        mgr = TrackingManager(mock_spark, mock_config)

        # Simulate two rows returned by the SQL query: one pending and one validated
        mock_row_pending = MagicMock()
        mock_row_pending.asDict.return_value = {
            "object_name": "catalog.schema.table1",
            "object_type": "managed_table",
            "catalog_name": "catalog",
            "schema_name": "schema",
        }

        mock_result = MagicMock()
        mock_result.collect.return_value = [mock_row_pending]
        mock_spark.sql.return_value = mock_result

        result = mgr.get_pending_objects("managed_table")

        # Verify the SQL query contains the correct filtering logic
        sql_arg = mock_spark.sql.call_args[0][0]
        assert "LEFT JOIN" in sql_arg
        assert "NOT IN ('validated', 'skipped')" in sql_arg
        assert "managed_table" in sql_arg

        # Verify the result is a list of dicts from collect()
        assert len(result) == 1
        assert result[0]["object_name"] == "catalog.schema.table1"


class TestDiscoveryRowHelpers:
    """Tests for the module-level discovery_row() and discovery_schema() helpers."""

    def test_discovery_row_uc(self):
        now = datetime.now(tz=timezone.utc)
        row = discovery_row(
            source_type="uc",
            object_type="managed_table",
            object_name="cat.sch.t1",
            catalog_name="cat",
            schema_name="sch",
            discovered_at=now,
            row_count=10,
            size_bytes=100,
            is_dlt_managed=False,
            pipeline_id=None,
            create_statement="CREATE TABLE ...",
        )
        assert row["source_type"] == "uc"
        assert row["object_type"] == "managed_table"
        assert row["data_category"] is None
        assert row["metadata_json"] is None

    def test_discovery_row_hive(self):
        now = datetime.now(tz=timezone.utc)
        row = discovery_row(
            source_type="hive",
            object_type="hive_table",
            object_name="hive_metastore.db.t",
            catalog_name="hive_metastore",
            schema_name="db",
            discovered_at=now,
            data_category="hive_external",
            table_type="EXTERNAL",
            provider="DELTA",
            storage_location="abfss://...",
        )
        assert row["source_type"] == "hive"
        assert row["is_dlt_managed"] is None
        assert row["data_category"] == "hive_external"
        assert row["storage_location"] == "abfss://..."

    def test_discovery_row_metadata_json_encoded(self):
        now = datetime.now(tz=timezone.utc)
        row = discovery_row(
            source_type="uc",
            object_type="mv",
            object_name="cat.sch.mv1",
            catalog_name="cat",
            schema_name="sch",
            discovered_at=now,
            metadata={"pipeline_id": "abc123", "is_sql_created": True},
        )
        import json
        assert json.loads(row["metadata_json"]) == {"pipeline_id": "abc123", "is_sql_created": True}

    def test_discovery_schema_is_callable(self):
        # pyspark.sql.types is mocked in unit tests (see conftest.py); verify
        # the function executes without error and returns the mocked StructType.
        # Real schema-field coverage is exercised by the DDL assertion in
        # test_init_tracking_tables_creates_schema above.
        assert discovery_schema() is not None
