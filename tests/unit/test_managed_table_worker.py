from __future__ import annotations

from unittest.mock import MagicMock, patch


class TestCloneTable:
    """Tests for the managed_table_worker.clone_table function."""

    def _make_deps(self, *, dry_run: bool = False) -> dict:
        config = MagicMock()
        config.dry_run = dry_run
        auth = MagicMock()
        tracker = MagicMock()
        validator = MagicMock()
        return {
            "config": config,
            "auth": auth,
            "tracker": tracker,
            "validator": validator,
            "wh_id": "wh-123",
            "share_name": "cp_migration_share",
        }

    @patch("migrate.managed_table_worker.time")
    @patch("migrate.managed_table_worker.execute_and_poll")
    def test_clone_table_success(self, mock_execute, mock_time):
        from migrate.managed_table_worker import clone_table

        mock_time.time.side_effect = [100.0, 105.0, 110.0]
        mock_execute.return_value = {"state": "SUCCEEDED", "statement_id": "s-1"}

        deps = self._make_deps()
        deps["validator"].validate_row_count.return_value = {
            "match": True,
            "source_count": 42,
            "target_count": 42,
        }

        table_info = {"object_name": "`cat`.`sch`.`tbl`"}
        result = clone_table(table_info, **deps)

        assert result["status"] == "validated"
        assert result["object_type"] == "managed_table"
        assert result["source_row_count"] == 42
        assert result["target_row_count"] == 42
        assert result["error_message"] is None
        deps["tracker"].append_migration_status.assert_called_once()
        mock_execute.assert_called_once()

    @patch("migrate.managed_table_worker.time")
    @patch("migrate.managed_table_worker.execute_and_poll")
    def test_clone_table_dry_run(self, mock_execute, mock_time):
        from migrate.managed_table_worker import clone_table

        mock_time.time.side_effect = [100.0, 100.1]

        deps = self._make_deps(dry_run=True)
        table_info = {"object_name": "`cat`.`sch`.`tbl`"}
        result = clone_table(table_info, **deps)

        assert result["status"] == "skipped"
        assert result["error_message"] == "dry_run"
        mock_execute.assert_not_called()

    @patch("migrate.managed_table_worker.time")
    @patch("migrate.managed_table_worker.execute_and_poll")
    def test_clone_table_clone_failure(self, mock_execute, mock_time):
        from migrate.managed_table_worker import clone_table

        mock_time.time.side_effect = [100.0, 115.0]
        mock_execute.return_value = {
            "state": "FAILED",
            "error": "TABLE_NOT_FOUND",
            "statement_id": "s-2",
        }

        deps = self._make_deps()
        table_info = {"object_name": "`cat`.`sch`.`tbl`"}
        result = clone_table(table_info, **deps)

        assert result["status"] == "failed"
        assert "TABLE_NOT_FOUND" in result["error_message"]

    @patch("migrate.managed_table_worker.time")
    @patch("migrate.managed_table_worker.execute_and_poll")
    def test_clone_table_validation_mismatch(self, mock_execute, mock_time):
        from migrate.managed_table_worker import clone_table

        mock_time.time.side_effect = [100.0, 105.0, 110.0]
        mock_execute.return_value = {"state": "SUCCEEDED", "statement_id": "s-3"}

        deps = self._make_deps()
        deps["validator"].validate_row_count.return_value = {
            "match": False,
            "source_count": 100,
            "target_count": 50,
        }

        table_info = {"object_name": "`cat`.`sch`.`tbl`"}
        result = clone_table(table_info, **deps)

        assert result["status"] == "validation_failed"
        assert "Row count mismatch" in result["error_message"]
        assert result["source_row_count"] == 100
        assert result["target_row_count"] == 50

    def test_clone_table_malformed_fqn(self):
        from migrate.managed_table_worker import clone_table

        deps = self._make_deps()
        table_info = {"object_name": "just_a_table_name"}
        result = clone_table(table_info, **deps)

        assert result["status"] == "failed"
        assert "Malformed FQN" in result["error_message"]
        assert result["duration_seconds"] == 0.0
