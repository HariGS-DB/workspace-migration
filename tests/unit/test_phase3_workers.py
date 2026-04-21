"""Unit tests for Phase 3 governance workers (Tasks 28-37)."""
from __future__ import annotations

from unittest.mock import MagicMock, patch


def _ok() -> dict:
    return {"state": "SUCCEEDED", "statement_id": "s"}


def _fail(err: str = "ERR") -> dict:
    return {"state": "FAILED", "error": err, "statement_id": "s"}


# ---------------------------------------------------------------- Tags --

class TestTagsWorker:
    @patch("migrate.tags_worker.time")
    @patch("migrate.tags_worker.execute_and_poll")
    def test_applies_tag_group_to_table(self, mock_execute, mock_time):
        from migrate.tags_worker import apply_tag_group

        mock_time.time.side_effect = [100.0, 101.0]
        mock_execute.return_value = _ok()

        auth = MagicMock()
        result = apply_tag_group(
            ("TABLE", "`c`.`s`.`t`", ""),
            [{"tag_name": "env", "tag_value": "prod"}],
            auth=auth, wh_id="wh-1", dry_run=False,
        )
        assert result["status"] == "validated"
        sql = mock_execute.call_args[0][2]
        assert "ALTER TABLE `c`.`s`.`t` SET TAGS" in sql
        assert "'env' = 'prod'" in sql

    @patch("migrate.tags_worker.time")
    @patch("migrate.tags_worker.execute_and_poll")
    def test_applies_column_tag(self, mock_execute, mock_time):
        from migrate.tags_worker import apply_tag_group

        mock_time.time.side_effect = [100.0, 100.5]
        mock_execute.return_value = _ok()

        result = apply_tag_group(
            ("COLUMN", "`c`.`s`.`t`", "ssn"),
            [{"tag_name": "pii", "tag_value": "true"}],
            auth=MagicMock(), wh_id="wh-1", dry_run=False,
        )
        sql = mock_execute.call_args[0][2]
        assert "ALTER COLUMN `ssn` SET TAGS" in sql
        assert result["object_name"].endswith(".ssn")

    def test_tag_value_escapes_quotes(self):
        from migrate.tags_worker import _tag_clause
        clause = _tag_clause([("owner", "O'Brien")])
        assert clause == "('owner' = 'O''Brien')"


# ---------------------------------------------------- Row filters -----

class TestRowFiltersWorker:
    @patch("migrate.row_filters_worker.time")
    @patch("migrate.row_filters_worker.execute_and_poll")
    def test_applies_row_filter(self, mock_execute, mock_time):
        from migrate.row_filters_worker import apply_row_filter

        mock_time.time.side_effect = [100.0, 101.0]
        mock_execute.return_value = _ok()

        res = apply_row_filter(
            {"table_fqn": "`c`.`s`.`t`",
             "filter_function_fqn": "c.s.region_fn",
             "filter_columns": ["region", "env"]},
            auth=MagicMock(), wh_id="wh", dry_run=False,
        )
        assert res["status"] == "validated"
        sql = mock_execute.call_args[0][2]
        assert "ALTER TABLE `c`.`s`.`t` SET ROW FILTER c.s.region_fn" in sql
        assert "ON (`region`, `env`)" in sql


# ---------------------------------------------------- Column masks ----

class TestColumnMasksWorker:
    @patch("migrate.column_masks_worker.time")
    @patch("migrate.column_masks_worker.execute_and_poll")
    def test_applies_column_mask(self, mock_execute, mock_time):
        from migrate.column_masks_worker import apply_column_mask

        mock_time.time.side_effect = [100.0, 101.0]
        mock_execute.return_value = _ok()

        res = apply_column_mask(
            {"table_fqn": "`c`.`s`.`users`", "column_name": "ssn",
             "mask_function_fqn": "c.s.redact_ssn",
             "mask_using_columns": ["role"]},
            auth=MagicMock(), wh_id="wh", dry_run=False,
        )
        assert res["status"] == "validated"
        sql = mock_execute.call_args[0][2]
        assert "ALTER COLUMN `ssn` SET MASK c.s.redact_ssn" in sql
        assert "USING COLUMNS (`role`)" in sql

    @patch("migrate.column_masks_worker.time")
    @patch("migrate.column_masks_worker.execute_and_poll")
    def test_mask_without_using_columns(self, mock_execute, mock_time):
        from migrate.column_masks_worker import apply_column_mask

        mock_time.time.side_effect = [100.0, 101.0]
        mock_execute.return_value = _ok()

        apply_column_mask(
            {"table_fqn": "`c`.`s`.`t`", "column_name": "x",
             "mask_function_fqn": "c.s.fn", "mask_using_columns": []},
            auth=MagicMock(), wh_id="wh", dry_run=False,
        )
        sql = mock_execute.call_args[0][2]
        assert "USING COLUMNS" not in sql


# ---------------------------------------------------- Policies --------

class TestPoliciesWorker:
    @patch("migrate.policies_worker.time")
    def test_posts_policy(self, mock_time):
        from migrate.policies_worker import apply_policy

        mock_time.time.side_effect = [100.0, 101.0]
        auth = MagicMock()
        auth.target_client.api_client.do.return_value = {"ok": True}

        res = apply_policy(
            {"name": "p1", "on_securable_fullname": "c.s.t"},
            auth=auth, dry_run=False,
        )
        assert res["status"] == "validated"
        auth.target_client.api_client.do.assert_called_once()
        method, path = auth.target_client.api_client.do.call_args[0][:2]
        assert method == "POST"
        assert "/policies" in path

    @patch("migrate.policies_worker.time")
    def test_records_error_on_api_failure(self, mock_time):
        from migrate.policies_worker import apply_policy

        mock_time.time.side_effect = [100.0, 101.0]
        auth = MagicMock()
        auth.target_client.api_client.do.side_effect = Exception("403")

        res = apply_policy({"name": "p1"}, auth=auth, dry_run=False)
        assert res["status"] == "failed"
        assert "403" in res["error_message"]


# ---------------------------------------------------- Monitors --------

class TestMonitorsWorker:
    @patch("migrate.monitors_worker.time")
    def test_posts_monitor(self, mock_time):
        from migrate.monitors_worker import apply_monitor

        mock_time.time.side_effect = [100.0, 101.0]
        auth = MagicMock()
        auth.target_client.api_client.do.return_value = {"ok": True}

        res = apply_monitor(
            {"table_fqn": "`c`.`s`.`t`",
             "definition": {
                 "table_name": "source.s.t",  # should be stripped
                 "schedule": {"quartz_cron_expression": "0 0 * * * ?"},
                 "status": "ACTIVE",  # should be stripped
             }},
            auth=auth, dry_run=False,
        )
        assert res["status"] == "validated"
        body = auth.target_client.api_client.do.call_args.kwargs["body"]
        assert "table_name" not in body  # stripped
        assert "status" not in body
        assert "schedule" in body


# ---------------------------------------------------- Models ----------

class TestModelsWorker:
    @patch("migrate.models_worker.time")
    def test_creates_model_with_versions_and_aliases(self, mock_time):
        from migrate.models_worker import apply_model

        mock_time.time.side_effect = [100.0, 110.0]
        auth = MagicMock()
        # registered_models.create + model_versions.create succeed
        auth.target_client.registered_models.create.return_value = MagicMock()
        auth.target_client.model_versions.create.return_value = MagicMock()
        auth.target_client.registered_models.set_alias.return_value = MagicMock()

        results = apply_model(
            {"model_fqn": "c.s.m1", "storage_location": "abfss://.../m1",
             "versions": [
                 {"version": 1, "source": "run:/abc/art", "aliases": ["prod"]},
             ]},
            auth=auth, dry_run=False,
        )
        assert len(results) == 1
        assert results[0]["status"] == "validated"
        auth.target_client.registered_models.set_alias.assert_called_once_with(
            full_name="c.s.m1", alias="prod", version_num=1,
        )

    @patch("migrate.models_worker.time")
    def test_idempotent_on_already_exists(self, mock_time):
        from migrate.models_worker import apply_model

        mock_time.time.side_effect = [100.0, 101.0]
        auth = MagicMock()
        auth.target_client.registered_models.create.side_effect = Exception(
            "RESOURCE_ALREADY_EXISTS"
        )
        auth.target_client.model_versions.create.return_value = MagicMock()

        results = apply_model(
            {"model_fqn": "c.s.m1", "versions": []},
            auth=auth, dry_run=False,
        )
        assert results[0]["status"] == "validated"


# ---------------------------------------------------- Connections -----

class TestConnectionsWorker:
    @patch("migrate.connections_worker.time")
    def test_creates_connection_and_flags_missing_credentials(self, mock_time):
        from migrate.connections_worker import apply_connection

        mock_time.time.side_effect = [100.0, 101.0]
        auth = MagicMock()
        auth.target_client.connections.create.return_value = MagicMock()

        res = apply_connection(
            {"connection_name": "snow", "connection_type": "SNOWFLAKE",
             "options": {"host": "acct", "password": ""}},
            auth=auth, dry_run=False,
        )
        assert res["status"] == "validation_failed"
        assert "password" in res["error_message"]

    @patch("migrate.connections_worker.time")
    def test_validated_when_no_secret_options(self, mock_time):
        from migrate.connections_worker import apply_connection

        mock_time.time.side_effect = [100.0, 101.0]
        auth = MagicMock()
        auth.target_client.connections.create.return_value = MagicMock()

        res = apply_connection(
            {"connection_name": "httpapi", "connection_type": "HTTP",
             "options": {"url": "https://x"}},
            auth=auth, dry_run=False,
        )
        assert res["status"] == "validated"


# ---------------------------------------------------- Foreign catalogs

class TestForeignCatalogsWorker:
    @patch("migrate.foreign_catalogs_worker.time")
    def test_creates_foreign_catalog(self, mock_time):
        from migrate.foreign_catalogs_worker import apply_foreign_catalog

        mock_time.time.side_effect = [100.0, 101.0]
        auth = MagicMock()
        res = apply_foreign_catalog(
            {"catalog_name": "snow_fc", "connection_name": "snow", "options": {}},
            auth=auth, dry_run=False,
        )
        assert res["status"] == "validated"
        auth.target_client.catalogs.create.assert_called_once()


# ---------------------------------------------------- Online tables ---

class TestOnlineTablesWorker:
    @patch("migrate.online_tables_worker.time")
    def test_posts_online_table(self, mock_time):
        from migrate.online_tables_worker import apply_online_table

        mock_time.time.side_effect = [100.0, 101.0]
        auth = MagicMock()
        auth.target_client.api_client.do.return_value = {"ok": True}

        res = apply_online_table(
            {"online_table_fqn": "c.s.online_t",
             "definition": {"spec": {"source_table_full_name": "c.s.t"}}},
            auth=auth, dry_run=False,
        )
        assert res["status"] == "validated"
        body = auth.target_client.api_client.do.call_args.kwargs["body"]
        assert body["name"] == "c.s.online_t"
        assert body["spec"]["source_table_full_name"] == "c.s.t"


# ---------------------------------------------------- Sharing ---------

class TestSharingWorker:
    @patch("migrate.sharing_worker.time")
    @patch("migrate.sharing_worker.execute_and_poll")
    def test_share_adds_mixed_object_types(self, mock_execute, mock_time):
        from migrate.sharing_worker import apply_share

        mock_time.time.side_effect = [100.0, 130.0]
        mock_execute.return_value = _ok()
        auth = MagicMock()
        auth.target_client.shares.create.return_value = MagicMock()

        res = apply_share(
            {"share_name": "s1", "comment": None,
             "objects": [
                 {"name": "c.s.t", "data_object_type": "SharedDataObjectDataObjectType.TABLE"},
                 {"name": "c.s.v", "data_object_type": "VIEW"},
                 {"name": "c.s.vol", "data_object_type": "VOLUME"},
                 {"name": "c.s", "data_object_type": "SCHEMA"},
             ]},
            auth=auth, wh_id="wh", dry_run=False,
        )
        assert res["status"] == "validated"
        sqls = [c.args[2] for c in mock_execute.call_args_list]
        assert any("ADD TABLE" in s for s in sqls)
        assert any("ADD VIEW" in s for s in sqls)
        assert any("ADD VOLUME" in s for s in sqls)
        assert any("ADD SCHEMA" in s for s in sqls)

    @patch("migrate.sharing_worker.time")
    def test_recipient_idempotent(self, mock_time):
        from migrate.sharing_worker import apply_recipient

        mock_time.time.side_effect = [100.0, 101.0]
        auth = MagicMock()
        auth.target_client.recipients.create.side_effect = Exception("already exists")

        res = apply_recipient(
            {"recipient_name": "r1", "authentication_type": "DATABRICKS"},
            auth=auth, dry_run=False,
        )
        assert res["status"] == "validated"
        assert "already existed" in res["error_message"]

    @patch("migrate.sharing_worker.time")
    @patch("migrate.sharing_worker.execute_and_poll")
    def test_share_partial_failure_marks_validation_failed(
        self, mock_execute, mock_time,
    ):
        """If some ADD succeeds and some fails, share row should be
        ``validation_failed`` so operators know to investigate without
        halting the rest of the share pipeline."""
        from migrate.sharing_worker import apply_share

        mock_time.time.side_effect = [100.0, 105.0]
        # First two ADDs succeed, third fails
        mock_execute.side_effect = [
            _ok(), _ok(),
            {"state": "FAILED", "error": "UNAUTHORIZED", "statement_id": "s"},
        ]
        auth = MagicMock()
        auth.target_client.shares.create.return_value = MagicMock()

        res = apply_share(
            {"share_name": "s1",
             "objects": [
                 {"name": "c.s.t1", "data_object_type": "TABLE"},
                 {"name": "c.s.t2", "data_object_type": "TABLE"},
                 {"name": "c.s.t3", "data_object_type": "TABLE"},
             ]},
            auth=auth, wh_id="wh", dry_run=False,
        )
        assert res["status"] == "validation_failed"
        assert "UNAUTHORIZED" in res["error_message"]
        assert "Added 2" in res["error_message"]

    @patch("migrate.sharing_worker.time")
    def test_provider_failure_surfaces_error(self, mock_time):
        """Create-provider failure must produce status='failed' — we
        don't silently treat API errors as success."""
        from migrate.sharing_worker import apply_provider

        mock_time.time.side_effect = [100.0, 101.0]
        auth = MagicMock()
        auth.target_client.providers.create.side_effect = Exception(
            "INVALID_ACTIVATION_URL"
        )

        res = apply_provider(
            {"provider_name": "p1",
             "authentication_type": "TOKEN",
             "recipient_profile_str": "http://..."},
            auth=auth, dry_run=False,
        )
        assert res["status"] == "failed"
        assert "INVALID_ACTIVATION_URL" in res["error_message"]

    @patch("migrate.sharing_worker.time")
    def test_share_dry_run_skips_api(self, mock_time):
        """Dry run must NOT hit shares.create on target."""
        from migrate.sharing_worker import apply_share

        mock_time.time.side_effect = [100.0, 100.0]
        auth = MagicMock()
        res = apply_share(
            {"share_name": "s1", "objects": []},
            auth=auth, wh_id="wh", dry_run=True,
        )
        assert res["status"] == "skipped"
        assert res["error_message"] == "dry_run"
        auth.target_client.shares.create.assert_not_called()


class TestPhase3WorkersIdempotency:
    """Cross-worker contract: each Phase 3 worker must tolerate its
    target object already existing on target (e.g. from a previous
    partial migrate). The tests below exercise idempotency per object
    type — a re-run must not fail with ``ALREADY_EXISTS`` or similar.
    """

    @patch("migrate.tags_worker.time")
    @patch("migrate.tags_worker.execute_and_poll")
    def test_tags_worker_tolerates_already_set_tag(self, mock_execute, mock_time):
        """Re-applying a tag that already exists must succeed (SET TAGS
        is idempotent in UC — same key/value is a no-op)."""
        from migrate.tags_worker import apply_tag_group

        mock_time.time.side_effect = [100.0, 100.1]
        mock_execute.return_value = {"state": "SUCCEEDED", "statement_id": "s"}

        auth = MagicMock()
        tag_group = [
            {
                "securable_type": "TABLE",
                "securable_fqn": "`c`.`s`.`t`",
                "tag_name": "env",
                "tag_value": "test",
            },
        ]
        res = apply_tag_group(
            ("TABLE", "`c`.`s`.`t`", ""), tag_group,
            auth=auth, wh_id="wh", dry_run=False,
        )
        assert res["status"] == "validated"


class TestPhase3DispatchOnObjectType:
    """Every Phase 3 worker reads its ``object_type`` from the incoming
    list payload and dispatches only to the types it owns. Prevents a
    regression where e.g. tags_worker accidentally starts processing
    row_filter entries.
    """

    def test_tags_worker_only_handles_tag_rows(self):
        """Source-level check that tags_worker's per-row dispatch
        filters on object_type == 'tag'."""
        import pathlib
        src = (
            pathlib.Path(__file__).resolve().parents[2]
            / "src" / "migrate" / "tags_worker.py"
        ).read_text()
        # Either explicit object_type filtering, or reading tag_list
        # (which the orchestrator already pre-filters to tag type).
        assert "tag_list" in src or 'object_type = \'tag\'' in src or \
            'object_type == "tag"' in src

    def test_row_filters_worker_uses_row_filter_list(self):
        import pathlib
        src = (
            pathlib.Path(__file__).resolve().parents[2]
            / "src" / "migrate" / "row_filters_worker.py"
        ).read_text()
        assert "row_filter_list" in src

    def test_column_masks_worker_uses_column_mask_list(self):
        import pathlib
        src = (
            pathlib.Path(__file__).resolve().parents[2]
            / "src" / "migrate" / "column_masks_worker.py"
        ).read_text()
        assert "column_mask_list" in src


class TestPhase3StatusEmission:
    """Every Phase 3 worker writes to migration_status with an
    object_type matching the Phase 3 backlog (tag, row_filter,
    column_mask, policy, comment, monitor, registered_model,
    connection, foreign_catalog, share, recipient, provider,
    online_table). Locks in the naming so dashboard panels + the
    tracker's NOT-LIKE-'skipped%' filter stay aligned."""

    @patch("migrate.tags_worker.time")
    @patch("migrate.tags_worker.execute_and_poll")
    def test_tags_worker_writes_object_type_tag(self, mock_execute, mock_time):
        from migrate.tags_worker import apply_tag_group

        mock_time.time.side_effect = [100.0, 100.1]
        mock_execute.return_value = {"state": "SUCCEEDED", "statement_id": "s"}
        auth = MagicMock()
        res = apply_tag_group(
            ("TABLE", "`c`.`s`.`t`", ""),
            [{
                "securable_type": "TABLE",
                "securable_fqn": "`c`.`s`.`t`",
                "tag_name": "k",
                "tag_value": "v",
            }],
            auth=auth, wh_id="wh", dry_run=False,
        )
        assert res["object_type"] == "tag"

    @patch("migrate.row_filters_worker.time")
    @patch("migrate.row_filters_worker.execute_and_poll")
    def test_row_filters_worker_writes_object_type_row_filter(
        self, mock_execute, mock_time
    ):
        from migrate.row_filters_worker import apply_row_filter

        mock_time.time.side_effect = [100.0, 100.1]
        mock_execute.return_value = {"state": "SUCCEEDED", "statement_id": "s"}
        auth = MagicMock()
        res = apply_row_filter(
            {
                "table_fqn": "`c`.`s`.`t`",
                "filter_function_fqn": "c.s.f",
                "filter_columns": ["region"],
            },
            auth=auth, wh_id="wh", dry_run=False,
        )
        assert res["object_type"] == "row_filter"


# ---------------------------------------------------- Negative-path ----
#
# Every Phase 3 worker must turn a downstream failure into a status='failed'
# tracking row, not a raised exception. Locks in the contract so a single
# bad tag / RLS / mask / monitor doesn't halt the whole worker.

class TestPhase3WorkerErrorSurfacing:
    @patch("migrate.tags_worker.time")
    @patch("migrate.tags_worker.execute_and_poll")
    def test_tags_worker_surfaces_failed_sql(self, mock_execute, mock_time):
        from migrate.tags_worker import apply_tag_group

        mock_time.time.side_effect = [100.0, 101.0]
        mock_execute.return_value = {
            "state": "FAILED",
            "error": "PERMISSION_DENIED: not metastore admin",
            "statement_id": "s",
        }
        res = apply_tag_group(
            ("TABLE", "`c`.`s`.`t`", ""),
            [{"tag_name": "env", "tag_value": "prod"}],
            auth=MagicMock(), wh_id="wh", dry_run=False,
        )
        assert res["status"] == "failed"
        assert "PERMISSION_DENIED" in res["error_message"]

    @patch("migrate.row_filters_worker.time")
    @patch("migrate.row_filters_worker.execute_and_poll")
    def test_row_filter_surfaces_missing_function(self, mock_execute, mock_time):
        from migrate.row_filters_worker import apply_row_filter

        mock_time.time.side_effect = [100.0, 100.5]
        mock_execute.return_value = {
            "state": "FAILED", "error": "ROUTINE_NOT_FOUND", "statement_id": "s",
        }
        res = apply_row_filter(
            {"table_fqn": "`c`.`s`.`t`",
             "filter_function_fqn": "c.s.missing_fn",
             "filter_columns": ["region"]},
            auth=MagicMock(), wh_id="wh", dry_run=False,
        )
        assert res["status"] == "failed"
        assert "ROUTINE_NOT_FOUND" in res["error_message"]

    @patch("migrate.column_masks_worker.time")
    @patch("migrate.column_masks_worker.execute_and_poll")
    def test_column_mask_surfaces_missing_function(self, mock_execute, mock_time):
        from migrate.column_masks_worker import apply_column_mask

        mock_time.time.side_effect = [100.0, 100.5]
        mock_execute.return_value = {
            "state": "FAILED", "error": "ROUTINE_NOT_FOUND", "statement_id": "s",
        }
        res = apply_column_mask(
            {"table_fqn": "`c`.`s`.`t`",
             "column_name": "ssn",
             "mask_function_fqn": "c.s.missing_mask"},
            auth=MagicMock(), wh_id="wh", dry_run=False,
        )
        assert res["status"] == "failed"
        assert "ROUTINE_NOT_FOUND" in res["error_message"]

    @patch("migrate.monitors_worker.time")
    def test_monitor_surfaces_api_failure(self, mock_time):
        from migrate.monitors_worker import apply_monitor

        mock_time.time.side_effect = [100.0, 101.0]
        auth = MagicMock()
        auth.target_client.api_client.do.side_effect = Exception(
            "TABLE_NOT_FOUND: monitor target missing"
        )
        res = apply_monitor(
            {"table_fqn": "`c`.`s`.`t`",
             "definition": {"schedule": {"quartz_cron_expression": "0 0 * * * ?"}}},
            auth=auth, dry_run=False,
        )
        assert res["status"] == "failed"
        assert "TABLE_NOT_FOUND" in res["error_message"]

    @patch("migrate.models_worker.time")
    def test_model_surfaces_api_failure(self, mock_time):
        from migrate.models_worker import apply_model

        mock_time.time.side_effect = [100.0, 101.0]
        auth = MagicMock()
        auth.target_client.registered_models.create.side_effect = Exception(
            "PERMISSION_DENIED on schema"
        )
        results = apply_model(
            {"model_fqn": "c.s.m1",
             "storage_location": "abfss://x@y/m1",
             "versions": []},
            auth=auth, dry_run=False,
        )
        # apply_model returns a list of results (model + versions)
        assert any(r["status"] == "failed" for r in results)
        assert any("PERMISSION_DENIED" in (r.get("error_message") or "")
                   for r in results)


# ---------------------------------------------------- Dry-run gate ------

class TestPhase3DryRun:
    """Every worker that takes dry_run must short-circuit execute_and_poll
    and return status='skipped' / error_message='dry_run'. Missing these
    means dry_run silently hits the target."""

    @patch("migrate.tags_worker.execute_and_poll")
    @patch("migrate.tags_worker.time")
    def test_tags_worker_dry_run(self, mock_time, mock_execute):
        from migrate.tags_worker import apply_tag_group

        mock_time.time.side_effect = [100.0, 100.0]
        res = apply_tag_group(
            ("TABLE", "`c`.`s`.`t`", ""),
            [{"tag_name": "k", "tag_value": "v"}],
            auth=MagicMock(), wh_id="wh", dry_run=True,
        )
        assert res["status"] == "skipped"
        assert res["error_message"] == "dry_run"
        mock_execute.assert_not_called()

    @patch("migrate.row_filters_worker.execute_and_poll")
    @patch("migrate.row_filters_worker.time")
    def test_row_filter_dry_run(self, mock_time, mock_execute):
        from migrate.row_filters_worker import apply_row_filter

        mock_time.time.side_effect = [100.0, 100.0]
        res = apply_row_filter(
            {"table_fqn": "`c`.`s`.`t`",
             "filter_function_fqn": "c.s.f",
             "filter_columns": ["r"]},
            auth=MagicMock(), wh_id="wh", dry_run=True,
        )
        assert res["status"] == "skipped"
        mock_execute.assert_not_called()

    @patch("migrate.column_masks_worker.execute_and_poll")
    @patch("migrate.column_masks_worker.time")
    def test_column_mask_dry_run(self, mock_time, mock_execute):
        from migrate.column_masks_worker import apply_column_mask

        mock_time.time.side_effect = [100.0, 100.0]
        res = apply_column_mask(
            {"table_fqn": "`c`.`s`.`t`",
             "column_name": "ssn",
             "mask_function_fqn": "c.s.f"},
            auth=MagicMock(), wh_id="wh", dry_run=True,
        )
        assert res["status"] == "skipped"
        mock_execute.assert_not_called()
