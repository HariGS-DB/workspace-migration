from __future__ import annotations

from unittest.mock import MagicMock

from common.tracking import TrackingManager


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

        mock_spark.createDataFrame.assert_called_once_with(records)
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
