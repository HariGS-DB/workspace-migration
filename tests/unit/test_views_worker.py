from __future__ import annotations

from unittest.mock import MagicMock, patch


class TestMigrateView:
    """Tests for the views_worker.migrate_view function."""

    def _make_deps(self, *, dry_run: bool = False) -> dict:
        config = MagicMock()
        config.dry_run = dry_run
        auth = MagicMock()
        tracker = MagicMock()
        explorer = MagicMock()
        return {
            "config": config,
            "auth": auth,
            "tracker": tracker,
            "explorer": explorer,
            "wh_id": "wh-vw-1",
        }

    @patch("migrate.views_worker.time")
    @patch("migrate.views_worker.rewrite_ddl")
    @patch("migrate.views_worker.execute_and_poll")
    def test_migrate_success(self, mock_execute, mock_rewrite, mock_time):
        from migrate.views_worker import migrate_view

        mock_time.time.side_effect = [100.0, 105.0, 110.0]
        mock_rewrite.return_value = "CREATE OR REPLACE VIEW `cat`.`sch`.`v1` AS SELECT * FROM `cat`.`sch`.`tbl`"
        mock_execute.return_value = {"state": "SUCCEEDED", "statement_id": "s-1"}

        deps = self._make_deps()
        deps[
            "explorer"
        ].get_create_statement.return_value = "CREATE VIEW `cat`.`sch`.`v1` AS SELECT * FROM `cat`.`sch`.`tbl`"

        view_info = {"object_name": "`cat`.`sch`.`v1`"}
        result = migrate_view(view_info, **deps)

        assert result["status"] == "validated"
        assert result["object_type"] == "view"
        assert result["error_message"] is None
        deps["tracker"].append_migration_status.assert_called_once()
        mock_execute.assert_called_once()

    @patch("migrate.views_worker.time")
    @patch("migrate.views_worker.rewrite_ddl")
    @patch("migrate.views_worker.execute_and_poll")
    def test_migrate_dry_run(self, mock_execute, mock_rewrite, mock_time):
        from migrate.views_worker import migrate_view

        mock_time.time.side_effect = [100.0, 100.1]
        mock_rewrite.return_value = "CREATE OR REPLACE VIEW `cat`.`sch`.`v2` AS SELECT 1"

        deps = self._make_deps(dry_run=True)
        deps["explorer"].get_create_statement.return_value = "CREATE VIEW `cat`.`sch`.`v2` AS SELECT 1"

        view_info = {"object_name": "`cat`.`sch`.`v2`"}
        result = migrate_view(view_info, **deps)

        assert result["status"] == "skipped"
        assert result["error_message"] == "dry_run"
        mock_execute.assert_not_called()

    @patch("migrate.views_worker.time")
    @patch("migrate.views_worker.rewrite_ddl")
    @patch("migrate.views_worker.execute_and_poll")
    def test_migrate_ddl_rewrite(self, mock_execute, mock_rewrite, mock_time):
        from migrate.views_worker import migrate_view

        mock_time.time.side_effect = [100.0, 105.0, 110.0]
        rewritten = "CREATE OR REPLACE VIEW `cat`.`sch`.`v3` AS SELECT id FROM `cat`.`sch`.`tbl`"
        mock_rewrite.return_value = rewritten
        mock_execute.return_value = {"state": "SUCCEEDED", "statement_id": "s-3"}

        deps = self._make_deps()
        original_ddl = "CREATE VIEW `cat`.`sch`.`v3` AS SELECT id FROM `cat`.`sch`.`tbl`"
        deps["explorer"].get_create_statement.return_value = original_ddl

        view_info = {"object_name": "`cat`.`sch`.`v3`"}
        migrate_view(view_info, **deps)

        # Verify rewrite_ddl was called with the CREATE VIEW pattern
        mock_rewrite.assert_called_once_with(
            original_ddl,
            r"CREATE\s+VIEW\b",
            "CREATE OR REPLACE VIEW",
        )
        # Verify the rewritten DDL (containing OR REPLACE) was passed to execute_and_poll
        mock_execute.assert_called_once_with(deps["auth"], "wh-vw-1", rewritten)
        assert "CREATE OR REPLACE VIEW" in rewritten
