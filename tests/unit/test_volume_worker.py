from __future__ import annotations

from unittest.mock import MagicMock, patch


class TestMigrateVolume:
    """Tests for the volume_worker.migrate_volume function."""

    def _make_deps(self, *, dry_run: bool = False) -> dict:
        config = MagicMock()
        config.dry_run = dry_run
        auth = MagicMock()
        tracker = MagicMock()
        source_spark = MagicMock()
        return {
            "config": config,
            "auth": auth,
            "tracker": tracker,
            "wh_id": "wh-789",
            "source_spark": source_spark,
            "notebook_uploaded": True,  # avoid the workspace.import_ path in most tests
        }

    @patch("migrate.volume_worker.time")
    @patch("migrate.volume_worker.execute_and_poll")
    def test_migrate_external_volume(self, mock_execute, mock_time):
        from migrate.volume_worker import migrate_volume

        mock_time.time.side_effect = [100.0, 105.0]
        mock_execute.return_value = {"state": "SUCCEEDED", "statement_id": "s-1"}

        deps = self._make_deps()
        vol_info = {
            "object_name": "`cat`.`sch`.`ext_vol`",
            "table_type": "EXTERNAL",
            "storage_location": "abfss://container@storage.dfs.core.windows.net/path",
        }
        result, _ = migrate_volume(vol_info, **deps)

        assert result["status"] == "validated"
        assert result["object_type"] == "volume"
        assert result["error_message"] is None
        called_sql = mock_execute.call_args[0][2]
        assert "CREATE EXTERNAL VOLUME" in called_sql
        assert "IF NOT EXISTS" in called_sql
        assert "LOCATION 'abfss://container@storage.dfs.core.windows.net/path'" in called_sql

    @patch("migrate.volume_worker.time")
    @patch("migrate.volume_worker.execute_and_poll")
    def test_external_volume_missing_location_fails(self, mock_execute, mock_time):
        from migrate.volume_worker import migrate_volume

        mock_time.time.side_effect = [100.0, 100.1]

        deps = self._make_deps()
        vol_info = {
            "object_name": "`cat`.`sch`.`ext_vol`",
            "table_type": "EXTERNAL",
            "storage_location": "",  # missing
        }
        result, _ = migrate_volume(vol_info, **deps)

        assert result["status"] == "failed"
        assert "storage_location" in result["error_message"]
        mock_execute.assert_not_called()

    @patch("migrate.volume_worker._run_target_volume_copy")
    @patch("migrate.volume_worker.time")
    @patch("migrate.volume_worker.execute_and_poll")
    def test_migrate_managed_volume_copies_data(self, mock_execute, mock_time, mock_copy):
        from migrate.volume_worker import migrate_volume

        mock_time.time.side_effect = [100.0, 130.0]
        mock_execute.return_value = {"state": "SUCCEEDED", "statement_id": "s-2"}
        mock_copy.return_value = {"bytes_copied": 12345, "file_count": 7}

        deps = self._make_deps()
        deps["source_spark"].sql = MagicMock()

        vol_info = {
            "object_name": "`cat`.`sch`.`mgd_vol`",
            "table_type": "MANAGED",
        }
        result, _ = migrate_volume(vol_info, **deps)

        assert result["status"] == "validated"
        assert result["error_message"] is None
        # target-side copy was invoked with the correct share consumer path
        mock_copy.assert_called_once()
        args, kwargs = mock_copy.call_args
        src_path = args[1] if len(args) > 1 else kwargs.get("src_path")
        dst_path = args[2] if len(args) > 2 else kwargs.get("dst_path")
        assert src_path == "/Volumes/cp_migration_share_consumer/sch/mgd_vol"
        assert dst_path == "/Volumes/cat/sch/mgd_vol"
        # Share add + remove SQL were emitted
        sql_calls = [c.args[0] for c in deps["source_spark"].sql.call_args_list]
        assert any("ALTER SHARE cp_migration_share ADD VOLUME" in s for s in sql_calls)
        assert any("ALTER SHARE cp_migration_share REMOVE VOLUME" in s for s in sql_calls)
        # Target CREATE VOLUME (not EXTERNAL)
        called_sqls = [c.args[0] if len(c.args) == 1 else c.args[2] for c in mock_execute.call_args_list]
        assert any("CREATE VOLUME IF NOT EXISTS `cat`.`sch`.`mgd_vol`" in s for s in called_sqls)
        # File count surfaced via source/target_row_count
        assert result["source_row_count"] == 7
        assert result["target_row_count"] == 7

    @patch("migrate.volume_worker._run_target_volume_copy")
    @patch("migrate.volume_worker.time")
    @patch("migrate.volume_worker.execute_and_poll")
    def test_managed_volume_removes_from_share_on_copy_failure(
        self, mock_execute, mock_time, mock_copy
    ):
        from migrate.volume_worker import migrate_volume

        mock_time.time.side_effect = [100.0, 105.0]
        mock_execute.return_value = {"state": "SUCCEEDED", "statement_id": "s-2"}
        mock_copy.side_effect = RuntimeError("copy timed out")

        deps = self._make_deps()
        deps["source_spark"].sql = MagicMock()

        vol_info = {"object_name": "`cat`.`sch`.`mgd_vol`", "table_type": "MANAGED"}
        result, _ = migrate_volume(vol_info, **deps)

        assert result["status"] == "failed"
        assert "copy timed out" in result["error_message"]
        # REMOVE VOLUME still called
        sql_calls = [c.args[0] for c in deps["source_spark"].sql.call_args_list]
        assert any("REMOVE VOLUME" in s for s in sql_calls)

    @patch("migrate.volume_worker.time")
    @patch("migrate.volume_worker.execute_and_poll")
    def test_migrate_dry_run(self, mock_execute, mock_time):
        from migrate.volume_worker import migrate_volume

        mock_time.time.side_effect = [100.0, 100.1]

        deps = self._make_deps(dry_run=True)
        vol_info = {
            "object_name": "`cat`.`sch`.`vol1`",
            "table_type": "MANAGED",
        }
        result, _ = migrate_volume(vol_info, **deps)

        assert result["status"] == "skipped"
        assert result["error_message"] == "dry_run"
        mock_execute.assert_not_called()

    @patch("migrate.volume_worker.time")
    @patch("migrate.volume_worker.execute_and_poll")
    def test_migrate_failure(self, mock_execute, mock_time):
        from migrate.volume_worker import migrate_volume

        mock_time.time.side_effect = [100.0, 108.0]
        mock_execute.return_value = {
            "state": "FAILED",
            "error": "PERMISSION_DENIED",
            "statement_id": "s-3",
        }

        deps = self._make_deps()
        vol_info = {
            "object_name": "`cat`.`sch`.`vol_fail`",
            "table_type": "EXTERNAL",
            "storage_location": "s3://bucket/path",
        }
        result, _ = migrate_volume(vol_info, **deps)

        assert result["status"] == "failed"
        assert "PERMISSION_DENIED" in result["error_message"]
