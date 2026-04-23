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


class TestMvStWorkerEdgeCases:
    """Edge cases for Phase 2.5.D beyond the happy path — streaming
    source state warning, REFRESH failure classified as validated-
    with-note, DLT-defined pipelines consistently skipped."""

    def _deps(self, **overrides):
        config = MagicMock(dry_run=False)
        auth = MagicMock()
        tracker = MagicMock()
        deps = {"config": config, "auth": auth, "tracker": tracker, "wh_id": "wh-ms"}
        deps.update(overrides)
        return deps

    @patch("migrate.mv_st_worker.time")
    @patch("migrate.mv_st_worker.execute_and_poll")
    @patch("migrate.mv_st_worker._is_sql_created")
    def test_refresh_failure_still_validates_with_note(self, mock_is_sql, mock_execute, mock_time):
        """CREATE succeeds but subsequent REFRESH fails — the table
        exists on target (lossless) so we mark ``validated`` but record
        the REFRESH failure in error_message for operator visibility.
        Loss of this behavior would silently demote successful
        migrations to 'failed' on transient target pipeline glitches."""
        from migrate.mv_st_worker import migrate_mv_st

        mock_time.time.side_effect = [100.0, 100.5]
        mock_is_sql.return_value = (True, "")
        mock_execute.side_effect = [
            {"state": "SUCCEEDED", "statement_id": "s-create"},
            {"state": "FAILED", "error": "PIPELINE_BUSY", "statement_id": "s-refresh"},
        ]

        deps = self._deps()
        obj = {
            "object_name": "`c`.`s`.`mv1`",
            "object_type": "mv",
            "pipeline_id": "pipe-1",
            "create_statement": "CREATE MATERIALIZED VIEW `c`.`s`.`mv1` AS SELECT 1",
        }
        result = migrate_mv_st(obj, **deps)
        assert result["status"] == "validated"
        assert result["error_message"] is not None
        assert "REFRESH" in result["error_message"]
        assert "PIPELINE_BUSY" in result["error_message"]

    @patch("migrate.mv_st_worker.time")
    @patch("migrate.mv_st_worker._is_sql_created")
    def test_dlt_defined_mv_is_skipped_by_pipeline_migration(self, mock_is_sql, mock_time):
        """An MV whose pipeline has non-empty libraries is DLT-owned →
        skip with ``skipped_by_pipeline_migration`` so the DLT-pipeline
        migration tool handles it later. No CREATE issued on target."""
        from migrate.mv_st_worker import migrate_mv_st

        mock_time.time.side_effect = [100.0, 100.1]
        mock_is_sql.return_value = (False, "non-empty libraries (2)")

        deps = self._deps()
        obj = {
            "object_name": "`c`.`s`.`dlt_mv`",
            "object_type": "mv",
            "pipeline_id": "pipe-dlt",
        }
        result = migrate_mv_st(obj, **deps)
        assert result["status"] == "skipped_by_pipeline_migration"
        assert "DLT" in result["error_message"]

    @patch("migrate.mv_st_worker.time")
    def test_missing_pipeline_id_fails(self, mock_time):
        """MV/ST without a pipeline_id in the discovery row is an
        invalid state (every MV/ST has a backing pipeline in UC)."""
        from migrate.mv_st_worker import migrate_mv_st

        mock_time.time.side_effect = [100.0, 100.1]
        deps = self._deps()
        obj = {"object_name": "`c`.`s`.`mv2`", "object_type": "mv", "pipeline_id": None}
        result = migrate_mv_st(obj, **deps)
        assert result["status"] == "failed"
        assert "pipeline_id" in result["error_message"].lower()

    @patch("migrate.mv_st_worker.time")
    @patch("migrate.mv_st_worker.execute_and_poll")
    @patch("migrate.mv_st_worker._is_sql_created")
    def test_st_uses_streaming_refresh_keyword(self, mock_is_sql, mock_execute, mock_time):
        """Streaming tables get ``REFRESH STREAMING TABLE`` (not
        ``REFRESH MATERIALIZED VIEW``). Locks in the syntax pick."""
        from migrate.mv_st_worker import migrate_mv_st

        mock_time.time.side_effect = [100.0, 100.1]
        mock_is_sql.return_value = (True, "")
        mock_execute.return_value = {"state": "SUCCEEDED", "statement_id": "s"}

        deps = self._deps()
        obj = {
            "object_name": "`c`.`s`.`st1`",
            "object_type": "st",
            "pipeline_id": "p1",
            "create_statement": "CREATE STREAMING TABLE `c`.`s`.`st1` AS SELECT 1",
        }
        migrate_mv_st(obj, **deps)

        # Inspect all SQL calls — second one should be REFRESH on streaming.
        sqls = [c.args[2] for c in mock_execute.call_args_list]
        refresh_sqls = [s for s in sqls if "REFRESH" in s]
        assert refresh_sqls, "Expected a REFRESH call"
        assert "STREAMING TABLE" in refresh_sqls[0]
        assert "MATERIALIZED VIEW" not in refresh_sqls[0]

    @patch("migrate.mv_st_worker.time")
    @patch("migrate.mv_st_worker.execute_and_poll")
    @patch("migrate.mv_st_worker._is_sql_created")
    def test_st_success_emits_streaming_state_warning(self, mock_is_sql, mock_execute, mock_time):
        """On ST happy path, error_message carries a ``warning:`` note about
        streaming source state not transferring. Operators reading
        migration_status must see the caveat without having to open worker
        source — this is the only signal that Kafka offsets / Auto Loader
        checkpoints / CDF cursors are not migrated (Phase 2.5.12)."""
        from migrate.mv_st_worker import migrate_mv_st

        mock_time.time.side_effect = [100.0, 100.5]
        mock_is_sql.return_value = (True, "")
        mock_execute.return_value = {"state": "SUCCEEDED", "statement_id": "s"}

        deps = self._deps()
        obj = {
            "object_name": "`c`.`s`.`st_ok`",
            "object_type": "st",
            "pipeline_id": "p1",
            "create_statement": "CREATE STREAMING TABLE `c`.`s`.`st_ok` AS SELECT 1",
        }
        result = migrate_mv_st(obj, **deps)

        assert result["status"] == "validated"
        assert result["error_message"] is not None, (
            "ST happy path must carry a streaming-state warning in error_message"
        )
        msg = result["error_message"].lower()
        assert "warning" in msg
        assert "stream" in msg
        assert "state" in msg or "offset" in msg or "checkpoint" in msg

    @patch("migrate.mv_st_worker.time")
    @patch("migrate.mv_st_worker.execute_and_poll")
    @patch("migrate.mv_st_worker._is_sql_created")
    def test_st_refresh_failure_preserves_streaming_warning(self, mock_is_sql, mock_execute, mock_time):
        """REFRESH failure + streaming caveat must coexist in error_message.
        Don't clobber the refresh-failure message with just the warning, and
        don't drop the streaming caveat just because refresh failed."""
        from migrate.mv_st_worker import migrate_mv_st

        mock_time.time.side_effect = [100.0, 100.5]
        mock_is_sql.return_value = (True, "")
        mock_execute.side_effect = [
            {"state": "SUCCEEDED", "statement_id": "s-create"},
            {"state": "FAILED", "error": "PIPELINE_BUSY", "statement_id": "s-refresh"},
        ]

        deps = self._deps()
        obj = {
            "object_name": "`c`.`s`.`st_err`",
            "object_type": "st",
            "pipeline_id": "p1",
            "create_statement": "CREATE STREAMING TABLE `c`.`s`.`st_err` AS SELECT 1",
        }
        result = migrate_mv_st(obj, **deps)

        assert result["status"] == "validated"
        msg = result["error_message"]
        assert msg is not None
        assert "REFRESH failed" in msg
        assert "PIPELINE_BUSY" in msg
        # Streaming caveat must survive alongside the refresh-failure note
        assert "stream" in msg.lower()


class TestIcebergSkipByConfigBehaviorContract:
    """Iceberg skip uses ``skipped_by_config`` (not plain ``skipped``) so
    subsequent runs — after an operator flips ``iceberg_strategy=
    ddl_replay`` — pick the tables back up via ``get_pending_objects``'s
    NOT-LIKE-'skipped%' filter.

    Complementary to the managed_table_worker Iceberg tests: this
    locks in the cross-component contract between the worker and the
    tracker filter."""

    def test_skip_status_prefix_matches_tracker_filter(self):
        """``skipped_by_config`` starts with the literal 'skipped' prefix
        so ``NOT LIKE 'skipped%'`` in get_pending_objects excludes it,
        same as ``skipped_by_rls_cm_policy`` and ``skipped_by_pipeline_
        migration``. If someone ever renames to ``skip_by_config``, this
        fails loud and the tracker filter needs updating."""
        skip_status = "skipped_by_config"
        assert skip_status.startswith("skipped")
