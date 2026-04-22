"""Unit tests for Phase 3 P.1 RLS/CM drop_and_restore helpers."""
from __future__ import annotations

import json
from unittest.mock import MagicMock, patch


class TestStripAndReapplySQL:
    def test_strip_sql_row_filter(self):
        from migrate.rls_cm import strip_sql
        p = {
            "table_fqn": "`c`.`s`.`t`",
            "policy_kind": "row_filter",
            "target_column": None,
            "function_fqn": "c.s.fn",
            "using_columns": json.dumps(["region"]),
        }
        assert strip_sql(p) == "ALTER TABLE `c`.`s`.`t` DROP ROW FILTER"

    def test_strip_sql_column_mask(self):
        from migrate.rls_cm import strip_sql
        p = {
            "table_fqn": "`c`.`s`.`t`",
            "policy_kind": "column_mask",
            "target_column": "ssn",
            "function_fqn": "c.s.mask_ssn",
            "using_columns": "[]",
        }
        assert strip_sql(p) == "ALTER TABLE `c`.`s`.`t` ALTER COLUMN `ssn` DROP MASK"

    def test_strip_sql_rejects_unknown_kind(self):
        import pytest

        from migrate.rls_cm import strip_sql
        with pytest.raises(ValueError, match="Unknown policy_kind"):
            strip_sql({"policy_kind": "other", "table_fqn": "x"})

    def test_reapply_sql_row_filter(self):
        from migrate.rls_cm import reapply_sql
        sql = reapply_sql({
            "table_fqn": "`c`.`s`.`t`",
            "policy_kind": "row_filter",
            "target_column": None,
            "function_fqn": "c.s.fn",
            "using_columns": json.dumps(["region", "env"]),
        })
        assert "SET ROW FILTER c.s.fn ON (`region`, `env`)" in sql

    def test_reapply_sql_column_mask_with_using_cols(self):
        from migrate.rls_cm import reapply_sql
        sql = reapply_sql({
            "table_fqn": "`c`.`s`.`t`",
            "policy_kind": "column_mask",
            "target_column": "ssn",
            "function_fqn": "c.s.mask_ssn",
            "using_columns": json.dumps(["role"]),
        })
        assert "ALTER COLUMN `ssn` SET MASK c.s.mask_ssn" in sql
        assert "USING COLUMNS (`role`)" in sql

    def test_reapply_sql_column_mask_without_using_cols(self):
        from migrate.rls_cm import reapply_sql
        sql = reapply_sql({
            "table_fqn": "`c`.`s`.`t`",
            "policy_kind": "column_mask",
            "target_column": "ssn",
            "function_fqn": "c.s.mask_ssn",
            "using_columns": "[]",
        })
        assert "USING COLUMNS" not in sql

    def test_reapply_sql_handles_list_using_columns(self):
        """using_columns may arrive as either a JSON string or a real list."""
        from migrate.rls_cm import reapply_sql
        sql = reapply_sql({
            "table_fqn": "`c`.`s`.`t`",
            "policy_kind": "row_filter",
            "target_column": None,
            "function_fqn": "c.s.fn",
            "using_columns": ["region"],  # list, not JSON
        })
        assert "ON (`region`)" in sql


class TestReapplyPoliciesToSource:
    @patch("common.sql_utils.execute_and_poll")
    def test_reapplies_all_unrestored(self, mock_execute):
        from migrate.rls_cm import reapply_policies_to_source
        mock_execute.return_value = {"state": "SUCCEEDED"}

        tracker = MagicMock()
        tracker.get_unrestored_manifest.return_value = [
            {
                "table_fqn": "`c`.`s`.`t`",
                "policy_kind": "row_filter",
                "target_column": None,
                "function_fqn": "c.s.fn",
                "using_columns": json.dumps(["region"]),
            },
            {
                "table_fqn": "`c`.`s`.`t`",
                "policy_kind": "column_mask",
                "target_column": "ssn",
                "function_fqn": "c.s.mask_ssn",
                "using_columns": "[]",
            },
        ]
        spark = MagicMock()
        auth = MagicMock()

        results = reapply_policies_to_source(spark, auth, tracker, wh_id="wh-1")
        assert len(results) == 2
        assert all(r["status"] == "validated" for r in results)
        assert tracker.stamp_manifest_restored.call_count == 2

    @patch("common.sql_utils.execute_and_poll")
    def test_records_failure_when_sql_fails(self, mock_execute):
        from migrate.rls_cm import reapply_policies_to_source
        mock_execute.return_value = {"state": "FAILED", "error": "boom"}

        tracker = MagicMock()
        tracker.get_unrestored_manifest.return_value = [{
            "table_fqn": "`c`.`s`.`t`",
            "policy_kind": "row_filter",
            "target_column": None,
            "function_fqn": "c.s.fn",
            "using_columns": "[]",
        }]
        results = reapply_policies_to_source(
            MagicMock(), MagicMock(), tracker, wh_id="wh-1"
        )
        assert results[0]["status"] == "failed"
        assert results[0]["error_message"] == "boom"
        tracker.stamp_manifest_restored.assert_not_called()

    def test_dry_run_emits_skipped_rows(self):
        from migrate.rls_cm import reapply_policies_to_source
        tracker = MagicMock()
        tracker.get_unrestored_manifest.return_value = [{
            "table_fqn": "`c`.`s`.`t`",
            "policy_kind": "row_filter",
            "target_column": None,
            "function_fqn": "c.s.fn",
            "using_columns": "[]",
        }]
        results = reapply_policies_to_source(
            MagicMock(), MagicMock(), tracker, wh_id="wh-1", dry_run=True,
        )
        assert results[0]["status"] == "skipped"
        tracker.stamp_manifest_restored.assert_not_called()


class TestModelArtifactCopy:
    def test_apply_model_reports_bytes_in_status_message(self):
        from migrate.models_worker import apply_model

        auth = MagicMock()
        created = MagicMock()
        created.storage_location = "abfss://tgt/m1/v1"
        auth.target_client.registered_models.create.return_value = MagicMock()
        auth.target_client.model_versions.create.return_value = created
        auth.target_client.registered_models.set_alias.return_value = MagicMock()

        # Fake dbutils.fs.ls + cp — mirror the real dbutils FileInfo contract.
        dbu = MagicMock()
        entry = MagicMock()
        entry.name = "requirements.txt"
        entry.path = "abfss://src/m1/v1/requirements.txt"
        entry.size = 42
        entry.isDir.return_value = False
        dbu.fs.ls.return_value = [entry]

        with patch("migrate.models_worker.time") as mock_time:
            mock_time.time.side_effect = [0.0, 1.0]
            results = apply_model(
                {
                    "model_fqn": "c.s.m1",
                    "storage_location": "abfss://src/m1",
                    "versions": [
                        {"version": 1, "source": "run:/abc", "aliases": [],
                         "storage_location": "abfss://src/m1/v1"},
                    ],
                },
                auth=auth, dry_run=False, dbutils=dbu,
            )
        assert len(results) == 1
        assert results[0]["status"] == "validated"
        assert "42 bytes" in results[0]["error_message"]
        assert "1 file" in results[0]["error_message"]
        dbu.fs.cp.assert_called_once()

    def test_apply_model_without_dbutils_keeps_legacy_message(self):
        from migrate.models_worker import apply_model

        auth = MagicMock()
        auth.target_client.registered_models.create.return_value = MagicMock()
        auth.target_client.model_versions.create.return_value = MagicMock()

        with patch("migrate.models_worker.time") as mock_time:
            mock_time.time.side_effect = [0.0, 1.0]
            results = apply_model(
                {"model_fqn": "c.s.m1", "versions": []},
                auth=auth, dry_run=False, dbutils=None,
            )
        assert "post-migration artifact sync" in results[0]["error_message"]
