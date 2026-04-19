from __future__ import annotations

from unittest.mock import MagicMock, patch


class TestMvStWorker:
    """Tests for mv_st_worker.migrate_mv_st covering SQL-created vs DLT-defined branches."""

    def _make_deps(self, *, dry_run: bool = False) -> dict:
        config = MagicMock()
        config.dry_run = dry_run
        auth = MagicMock()
        tracker = MagicMock()
        return {
            "config": config,
            "auth": auth,
            "tracker": tracker,
            "wh_id": "wh-42",
        }

    def test_is_sql_created_empty_libraries(self):
        from migrate.mv_st_worker import _is_sql_created

        auth = MagicMock()
        pipeline = MagicMock()
        pipeline.spec.libraries = []
        auth.source_client.pipelines.get.return_value = pipeline

        ok, diag = _is_sql_created(auth, "pip-123")
        assert ok is True
        assert "empty" in diag.lower()

    def test_is_sql_created_populated_libraries(self):
        from migrate.mv_st_worker import _is_sql_created

        auth = MagicMock()
        pipeline = MagicMock()
        pipeline.spec.libraries = [MagicMock(), MagicMock()]
        auth.source_client.pipelines.get.return_value = pipeline

        ok, diag = _is_sql_created(auth, "pip-123")
        assert ok is False
        assert "non-empty" in diag.lower()

    def test_is_sql_created_pipeline_lookup_fails(self):
        from migrate.mv_st_worker import _is_sql_created

        auth = MagicMock()
        auth.source_client.pipelines.get.side_effect = RuntimeError("not found")

        ok, diag = _is_sql_created(auth, "pip-123")
        # Fallback: treat as SQL-created so DDL replay is attempted
        assert ok is True
        assert "lookup failed" in diag

    @patch("migrate.mv_st_worker.time")
    @patch("migrate.mv_st_worker.execute_and_poll")
    def test_migrate_sql_created_mv(self, mock_execute, mock_time):
        from migrate.mv_st_worker import migrate_mv_st

        mock_time.time.side_effect = [100.0, 125.0]
        # First call (CREATE), second call (REFRESH) both succeed
        mock_execute.return_value = {"state": "SUCCEEDED", "statement_id": "s"}

        deps = self._make_deps()
        pipeline = MagicMock()
        pipeline.spec.libraries = []  # SQL-created
        deps["auth"].source_client.pipelines.get.return_value = pipeline

        obj_info = {
            "object_name": "`cat`.`sch`.`mv1`",
            "object_type": "mv",
            "pipeline_id": "pip-abc",
            "create_statement": "CREATE MATERIALIZED VIEW `cat`.`sch`.`mv1` AS SELECT 1",
        }
        result = migrate_mv_st(obj_info, **deps)

        assert result["status"] == "validated"
        assert result["error_message"] is None
        # Verify CREATE and REFRESH SQL were both sent
        sqls = [c.args[2] for c in mock_execute.call_args_list]
        assert any("CREATE MATERIALIZED VIEW" in s for s in sqls)
        assert any("REFRESH MATERIALIZED VIEW" in s for s in sqls)

    @patch("migrate.mv_st_worker.time")
    @patch("migrate.mv_st_worker.execute_and_poll")
    def test_migrate_sql_created_st_uses_streaming_refresh(self, mock_execute, mock_time):
        from migrate.mv_st_worker import migrate_mv_st

        mock_time.time.side_effect = [100.0, 125.0]
        mock_execute.return_value = {"state": "SUCCEEDED", "statement_id": "s"}

        deps = self._make_deps()
        pipeline = MagicMock()
        pipeline.spec.libraries = []
        deps["auth"].source_client.pipelines.get.return_value = pipeline

        obj_info = {
            "object_name": "`cat`.`sch`.`st1`",
            "object_type": "st",
            "pipeline_id": "pip-xyz",
            "create_statement": "CREATE STREAMING TABLE `cat`.`sch`.`st1` AS SELECT ...",
        }
        result = migrate_mv_st(obj_info, **deps)

        assert result["status"] == "validated"
        sqls = [c.args[2] for c in mock_execute.call_args_list]
        assert any("REFRESH STREAMING TABLE" in s for s in sqls)

    @patch("migrate.mv_st_worker.time")
    def test_migrate_dlt_defined_is_skipped(self, mock_time):
        from migrate.mv_st_worker import migrate_mv_st

        mock_time.time.side_effect = [100.0, 100.5]
        deps = self._make_deps()
        pipeline = MagicMock()
        pipeline.spec.libraries = [MagicMock()]  # non-empty -> DLT-defined
        deps["auth"].source_client.pipelines.get.return_value = pipeline

        obj_info = {
            "object_name": "`cat`.`sch`.`dlt_mv`",
            "object_type": "mv",
            "pipeline_id": "pip-dlt",
            "create_statement": "...",
        }
        result = migrate_mv_st(obj_info, **deps)

        assert result["status"] == "skipped_by_pipeline_migration"
        assert "pip-dlt" in result["error_message"]

    @patch("migrate.mv_st_worker.time")
    def test_migrate_missing_pipeline_id_fails(self, mock_time):
        from migrate.mv_st_worker import migrate_mv_st

        mock_time.time.side_effect = [100.0, 100.1]
        deps = self._make_deps()
        obj_info = {
            "object_name": "`cat`.`sch`.`x`",
            "object_type": "mv",
            "pipeline_id": None,
            "create_statement": "CREATE MATERIALIZED VIEW ...",
        }
        result = migrate_mv_st(obj_info, **deps)

        assert result["status"] == "failed"
        assert "pipeline_id" in result["error_message"]

    @patch("migrate.mv_st_worker.time")
    @patch("migrate.mv_st_worker.execute_and_poll")
    def test_refresh_failure_still_validates(self, mock_execute, mock_time):
        """Target auto-pipeline may race; a REFRESH failure shouldn't fail migration."""
        from migrate.mv_st_worker import migrate_mv_st

        mock_time.time.side_effect = [100.0, 125.0]
        # CREATE succeeds, REFRESH fails
        mock_execute.side_effect = [
            {"state": "SUCCEEDED", "statement_id": "s1"},
            {"state": "FAILED", "error": "pipeline busy", "statement_id": "s2"},
        ]

        deps = self._make_deps()
        pipeline = MagicMock()
        pipeline.spec.libraries = []
        deps["auth"].source_client.pipelines.get.return_value = pipeline

        obj_info = {
            "object_name": "`cat`.`sch`.`mv1`",
            "object_type": "mv",
            "pipeline_id": "pip-abc",
            "create_statement": "CREATE MATERIALIZED VIEW ...",
        }
        result = migrate_mv_st(obj_info, **deps)

        assert result["status"] == "validated"
        assert "REFRESH failed" in result["error_message"]
        assert "pipeline busy" in result["error_message"]
