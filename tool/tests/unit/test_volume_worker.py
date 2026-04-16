from __future__ import annotations

from unittest.mock import MagicMock, patch


class TestMigrateVolume:
    """Tests for the volume_worker.migrate_volume function."""

    def _make_deps(self, *, dry_run: bool = False) -> dict:
        config = MagicMock()
        config.dry_run = dry_run
        auth = MagicMock()
        tracker = MagicMock()
        return {
            "config": config,
            "auth": auth,
            "tracker": tracker,
            "wh_id": "wh-789",
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
            "volume_type": "EXTERNAL",
            "storage_location": "abfss://container@storage.dfs.core.windows.net/path",
        }
        result = migrate_volume(vol_info, **deps)

        assert result["status"] == "validated"
        assert result["object_type"] == "volume"
        assert result["error_message"] is None
        # Verify the SQL contains CREATE EXTERNAL VOLUME
        called_sql = mock_execute.call_args[0][2]
        assert "CREATE EXTERNAL VOLUME" in called_sql
        assert "IF NOT EXISTS" in called_sql
        assert "LOCATION" in called_sql

    @patch("migrate.volume_worker.time")
    @patch("migrate.volume_worker.execute_and_poll")
    def test_migrate_managed_volume(self, mock_execute, mock_time):
        from migrate.volume_worker import migrate_volume

        mock_time.time.side_effect = [100.0, 105.0]
        mock_execute.return_value = {"state": "SUCCEEDED", "statement_id": "s-2"}

        deps = self._make_deps()
        vol_info = {
            "object_name": "`cat`.`sch`.`mgd_vol`",
            "volume_type": "MANAGED",
        }
        result = migrate_volume(vol_info, **deps)

        assert result["status"] == "validated"
        assert result["error_message"] is not None
        assert "manual data copy" in result["error_message"]
        # Verify the SQL uses CREATE VOLUME (not EXTERNAL)
        called_sql = mock_execute.call_args[0][2]
        assert "CREATE VOLUME IF NOT EXISTS" in called_sql
        assert "EXTERNAL" not in called_sql

    @patch("migrate.volume_worker.time")
    @patch("migrate.volume_worker.execute_and_poll")
    def test_migrate_dry_run(self, mock_execute, mock_time):
        from migrate.volume_worker import migrate_volume

        mock_time.time.side_effect = [100.0, 100.1]

        deps = self._make_deps(dry_run=True)
        vol_info = {
            "object_name": "`cat`.`sch`.`vol1`",
            "volume_type": "MANAGED",
        }
        result = migrate_volume(vol_info, **deps)

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
            "volume_type": "EXTERNAL",
            "storage_location": "s3://bucket/path",
        }
        result = migrate_volume(vol_info, **deps)

        assert result["status"] == "failed"
        assert "PERMISSION_DENIED" in result["error_message"]
