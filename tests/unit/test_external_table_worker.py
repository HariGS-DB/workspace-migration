from __future__ import annotations

from unittest.mock import MagicMock, patch


class TestMigrateExternalTable:
    """Tests for the external_table_worker.migrate_external_table function."""

    def _make_deps(self, *, dry_run: bool = False) -> dict:
        config = MagicMock()
        config.dry_run = dry_run
        auth = MagicMock()
        tracker = MagicMock()
        explorer = MagicMock()
        validator = MagicMock()
        return {
            "config": config,
            "auth": auth,
            "tracker": tracker,
            "explorer": explorer,
            "validator": validator,
            "wh_id": "wh-456",
        }

    @patch("migrate.external_table_worker.time")
    @patch("migrate.external_table_worker.rewrite_ddl")
    @patch("migrate.external_table_worker.execute_and_poll")
    def test_migrate_success(self, mock_execute, mock_rewrite, mock_time):
        from migrate.external_table_worker import migrate_external_table

        mock_time.time.side_effect = [100.0, 105.0, 110.0]
        mock_rewrite.return_value = (
            "CREATE TABLE IF NOT EXISTS `cat`.`sch`.`ext_tbl` USING DELTA LOCATION 's3://bucket'"
        )
        mock_execute.return_value = {"state": "SUCCEEDED", "statement_id": "s-1"}

        deps = self._make_deps()
        deps[
            "explorer"
        ].get_create_statement.return_value = "CREATE TABLE `cat`.`sch`.`ext_tbl` USING DELTA LOCATION 's3://bucket'"
        deps["validator"].validate_row_count.return_value = {
            "match": True,
            "source_count": 200,
            "target_count": 200,
        }

        table_info = {"object_name": "`cat`.`sch`.`ext_tbl`"}
        result = migrate_external_table(table_info, **deps)

        assert result["status"] == "validated"
        assert result["object_type"] == "external_table"
        assert result["source_row_count"] == 200
        assert result["error_message"] is None
        deps["tracker"].append_migration_status.assert_called_once()

    @patch("migrate.external_table_worker.time")
    @patch("migrate.external_table_worker.rewrite_ddl")
    @patch("migrate.external_table_worker.execute_and_poll")
    def test_migrate_dry_run(self, mock_execute, mock_rewrite, mock_time):
        from migrate.external_table_worker import migrate_external_table

        mock_time.time.side_effect = [100.0, 100.1]
        mock_rewrite.return_value = "CREATE TABLE IF NOT EXISTS `cat`.`sch`.`ext_tbl`"

        deps = self._make_deps(dry_run=True)
        deps["explorer"].get_create_statement.return_value = "CREATE TABLE `cat`.`sch`.`ext_tbl`"

        table_info = {"object_name": "`cat`.`sch`.`ext_tbl`"}
        result = migrate_external_table(table_info, **deps)

        assert result["status"] == "skipped"
        assert result["error_message"] == "dry_run"
        mock_execute.assert_not_called()

    @patch("migrate.external_table_worker.time")
    def test_migrate_ddl_failure(self, mock_time):
        from migrate.external_table_worker import migrate_external_table

        mock_time.time.side_effect = [100.0, 100.5]

        deps = self._make_deps()
        deps["explorer"].get_create_statement.side_effect = RuntimeError("catalog unavailable")

        table_info = {"object_name": "`cat`.`sch`.`ext_tbl`"}
        result = migrate_external_table(table_info, **deps)

        assert result["status"] == "failed"
        assert "Failed to get DDL" in result["error_message"]
        assert "catalog unavailable" in result["error_message"]

    @patch("migrate.external_table_worker.time")
    @patch("migrate.external_table_worker.rewrite_ddl")
    @patch("migrate.external_table_worker.execute_and_poll")
    def test_migrate_ddl_rewrite(self, mock_execute, mock_rewrite, mock_time):
        from migrate.external_table_worker import migrate_external_table

        mock_time.time.side_effect = [100.0, 105.0, 110.0]
        rewritten = "CREATE TABLE IF NOT EXISTS `cat`.`sch`.`ext_tbl` USING DELTA"
        mock_rewrite.return_value = rewritten
        mock_execute.return_value = {"state": "SUCCEEDED", "statement_id": "s-2"}

        deps = self._make_deps()
        deps["explorer"].get_create_statement.return_value = "CREATE TABLE `cat`.`sch`.`ext_tbl` USING DELTA"
        deps["validator"].validate_row_count.return_value = {
            "match": True,
            "source_count": 10,
            "target_count": 10,
        }

        table_info = {"object_name": "`cat`.`sch`.`ext_tbl`"}
        migrate_external_table(table_info, **deps)

        # Verify rewrite_ddl was called with the CREATE TABLE pattern
        mock_rewrite.assert_called_once_with(
            "CREATE TABLE `cat`.`sch`.`ext_tbl` USING DELTA",
            r"CREATE\s+TABLE\b",
            "CREATE TABLE IF NOT EXISTS",
        )
        # Verify the rewritten DDL was passed to execute_and_poll
        mock_execute.assert_called_once_with(deps["auth"], "wh-456", rewritten)
