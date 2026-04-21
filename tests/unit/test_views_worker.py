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


class TestViewsWorkerComplexDdls:
    """Views referencing multiple tables, CTEs, or other views get replayed
    verbatim (modulo CREATE → CREATE OR REPLACE). The worker shouldn't
    rewrite the body — only the CREATE keyword. If it ever starts mangling
    query bodies, these tests catch it."""

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
            "wh_id": "wh-complex",
        }

    @patch("migrate.views_worker.time")
    @patch("migrate.views_worker.execute_and_poll")
    def test_multi_table_join_view(self, mock_execute, mock_time):
        """View body joining two tables — rewrite should not touch the
        JOIN, just the CREATE keyword."""
        from migrate.views_worker import migrate_view

        mock_time.time.side_effect = [100.0, 105.0, 110.0]
        mock_execute.return_value = {"state": "SUCCEEDED", "statement_id": "s-j"}

        deps = self._make_deps()
        original = (
            "CREATE VIEW `cat`.`sch`.`orders_with_customers` AS "
            "SELECT o.order_id, o.amount, c.name "
            "FROM `cat`.`sch`.`orders` o "
            "JOIN `cat`.`sch`.`customers` c ON o.customer_id = c.id"
        )
        deps["explorer"].get_create_statement.return_value = original

        migrate_view({"object_name": "`cat`.`sch`.`orders_with_customers`"}, **deps)

        replayed = mock_execute.call_args[0][2]
        assert replayed.startswith("CREATE OR REPLACE VIEW"), (
            f"View DDL not replaced with CREATE OR REPLACE: {replayed[:120]}"
        )
        # Body must be preserved verbatim — join / aliases / etc.
        assert "JOIN `cat`.`sch`.`customers` c ON o.customer_id = c.id" in replayed
        assert "o.order_id, o.amount, c.name" in replayed

    @patch("migrate.views_worker.time")
    @patch("migrate.views_worker.execute_and_poll")
    def test_cte_view(self, mock_execute, mock_time):
        """View body with a WITH clause (CTE) — preserved verbatim."""
        from migrate.views_worker import migrate_view

        mock_time.time.side_effect = [100.0, 105.0, 110.0]
        mock_execute.return_value = {"state": "SUCCEEDED", "statement_id": "s-cte"}

        deps = self._make_deps()
        original = (
            "CREATE VIEW `cat`.`sch`.`top_customers` AS "
            "WITH top AS (SELECT customer_id, SUM(amount) total "
            "FROM `cat`.`sch`.`orders` GROUP BY customer_id "
            "ORDER BY total DESC LIMIT 10) "
            "SELECT * FROM top"
        )
        deps["explorer"].get_create_statement.return_value = original

        migrate_view({"object_name": "`cat`.`sch`.`top_customers`"}, **deps)
        replayed = mock_execute.call_args[0][2]
        assert replayed.startswith("CREATE OR REPLACE VIEW")
        assert "WITH top AS (" in replayed
        assert "ORDER BY total DESC LIMIT 10" in replayed

    @patch("migrate.views_worker.time")
    @patch("migrate.views_worker.execute_and_poll")
    def test_view_referencing_another_view(self, mock_execute, mock_time):
        """View referencing another view in the same schema — no special
        handling in the worker (topological ordering is the
        orchestrator's job), but the DDL body must be preserved."""
        from migrate.views_worker import migrate_view

        mock_time.time.side_effect = [100.0, 105.0, 110.0]
        mock_execute.return_value = {"state": "SUCCEEDED", "statement_id": "s-nested"}

        deps = self._make_deps()
        original = (
            "CREATE VIEW `cat`.`sch`.`high_value_recent` AS "
            "SELECT * FROM `cat`.`sch`.`high_value_orders` "
            "WHERE order_date > '2024-01-01'"
        )
        deps["explorer"].get_create_statement.return_value = original

        migrate_view({"object_name": "`cat`.`sch`.`high_value_recent`"}, **deps)
        replayed = mock_execute.call_args[0][2]
        assert replayed.startswith("CREATE OR REPLACE VIEW")
        assert "FROM `cat`.`sch`.`high_value_orders`" in replayed

    @patch("migrate.views_worker.time")
    @patch("migrate.views_worker.execute_and_poll")
    def test_view_with_qualifiers(self, mock_execute, mock_time):
        """View with WINDOW / PARTITION BY / ORDER BY — common analytics
        pattern. All preserved."""
        from migrate.views_worker import migrate_view

        mock_time.time.side_effect = [100.0, 105.0, 110.0]
        mock_execute.return_value = {"state": "SUCCEEDED", "statement_id": "s-win"}

        deps = self._make_deps()
        original = (
            "CREATE VIEW `cat`.`sch`.`rank_orders` AS "
            "SELECT customer_id, order_id, amount, "
            "ROW_NUMBER() OVER (PARTITION BY customer_id ORDER BY amount DESC) AS rnk "
            "FROM `cat`.`sch`.`orders`"
        )
        deps["explorer"].get_create_statement.return_value = original

        migrate_view({"object_name": "`cat`.`sch`.`rank_orders`"}, **deps)
        replayed = mock_execute.call_args[0][2]
        assert "ROW_NUMBER() OVER (PARTITION BY customer_id ORDER BY amount DESC)" in replayed
