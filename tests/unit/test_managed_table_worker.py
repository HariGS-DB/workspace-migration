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


class TestIcebergManagedTable:
    """Tests for the Iceberg Option A branch of clone_table."""

    def _make_deps(self, *, dry_run: bool = False, iceberg_strategy: str = "") -> dict:
        config = MagicMock()
        config.dry_run = dry_run
        config.iceberg_strategy = iceberg_strategy
        auth = MagicMock()
        tracker = MagicMock()
        validator = MagicMock()
        return {
            "config": config,
            "auth": auth,
            "tracker": tracker,
            "validator": validator,
            "wh_id": "wh-ice",
            "share_name": "cp_migration_share",
        }

    @patch("migrate.managed_table_worker.time")
    @patch("migrate.managed_table_worker.execute_and_poll")
    def test_iceberg_without_opt_in_is_skipped(self, mock_execute, mock_time):
        from migrate.managed_table_worker import clone_table

        mock_time.time.side_effect = [100.0, 100.1]

        deps = self._make_deps(iceberg_strategy="")  # not opted in
        table_info = {
            "object_name": "`cat`.`sch`.`ice_tbl`",
            "format": "iceberg",
            "create_statement": "CREATE TABLE ...",
        }
        result = clone_table(table_info, **deps)

        assert result["status"] == "skipped"
        assert "iceberg_strategy" in result["error_message"]
        mock_execute.assert_not_called()

    @patch("migrate.managed_table_worker.time")
    @patch("migrate.managed_table_worker.execute_and_poll")
    def test_iceberg_ddl_replay_success(self, mock_execute, mock_time):
        from migrate.managed_table_worker import clone_table

        mock_time.time.side_effect = [100.0, 130.0, 160.0]
        mock_execute.return_value = {"state": "SUCCEEDED", "statement_id": "s"}

        deps = self._make_deps(iceberg_strategy="ddl_replay")
        deps["validator"].validate_row_count.return_value = {
            "match": True,
            "source_count": 10,
            "target_count": 10,
        }

        table_info = {
            "object_name": "`cat`.`sch`.`ice_tbl`",
            "format": "iceberg",
            "create_statement": (
                "CREATE TABLE `cat`.`sch`.`ice_tbl` USING ICEBERG AS SELECT 1"
            ),
        }
        result = clone_table(table_info, **deps)

        assert result["status"] == "validated"
        assert result["source_row_count"] == 10
        sqls = [c.args[2] for c in mock_execute.call_args_list]
        assert any("USING ICEBERG" in s for s in sqls)  # CREATE executed
        assert any("INSERT INTO" in s for s in sqls)  # Re-ingest executed
        assert any(
            "FROM `cp_migration_share_consumer`.`sch`.`ice_tbl`" in s for s in sqls
        )

    @patch("migrate.managed_table_worker.time")
    @patch("migrate.managed_table_worker.execute_and_poll")
    def test_iceberg_missing_create_statement_fails(self, mock_execute, mock_time):
        from migrate.managed_table_worker import clone_table

        mock_time.time.side_effect = [100.0, 100.1]

        deps = self._make_deps(iceberg_strategy="ddl_replay")
        table_info = {
            "object_name": "`cat`.`sch`.`ice_tbl`",
            "format": "iceberg",
            "create_statement": "",
        }
        result = clone_table(table_info, **deps)

        assert result["status"] == "failed"
        assert "create_statement" in result["error_message"]

    @patch("migrate.managed_table_worker.time")
    @patch("migrate.managed_table_worker.execute_and_poll")
    def test_delta_format_still_uses_deep_clone(self, mock_execute, mock_time):
        """Regression: explicit format='delta' should behave like no format set."""
        from migrate.managed_table_worker import clone_table

        mock_time.time.side_effect = [100.0, 110.0, 115.0]
        mock_execute.return_value = {"state": "SUCCEEDED", "statement_id": "s"}

        deps = self._make_deps()
        deps["validator"].validate_row_count.return_value = {
            "match": True,
            "source_count": 5,
            "target_count": 5,
        }

        table_info = {
            "object_name": "`cat`.`sch`.`tbl`",
            "format": "delta",
        }
        result = clone_table(table_info, **deps)

        assert result["status"] == "validated"
        sqls = [c.args[2] for c in mock_execute.call_args_list]
        assert any("DEEP CLONE" in s for s in sqls)
        assert not any("INSERT INTO" in s for s in sqls)
