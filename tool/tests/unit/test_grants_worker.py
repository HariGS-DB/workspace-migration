from __future__ import annotations

from unittest.mock import MagicMock, patch


class TestReplayGrants:
    """Tests for the grants_worker.replay_grants function."""

    @patch("migrate.grants_worker.time")
    @patch("migrate.grants_worker.execute_and_poll")
    def test_replay_success(self, mock_execute, mock_time):
        from migrate.grants_worker import replay_grants

        mock_time.time.side_effect = [100.0, 102.0]
        mock_execute.return_value = {"state": "SUCCEEDED", "statement_id": "s-1"}

        auth = MagicMock()
        grants = [{"principal": "data_team", "action_type": "SELECT"}]

        results = replay_grants(
            "CATALOG",
            "`my_catalog`",
            grants,
            auth=auth,
            wh_id="wh-gr-1",
            dry_run=False,
        )

        assert len(results) == 1
        assert results[0]["status"] == "validated"
        assert results[0]["object_type"] == "grant"
        assert results[0]["error_message"] is None
        mock_execute.assert_called_once()
        # Verify the SQL includes the GRANT statement
        called_sql = mock_execute.call_args[0][2]
        assert "GRANT SELECT ON CATALOG" in called_sql
        assert "`data_team`" in called_sql

    @patch("migrate.grants_worker.execute_and_poll")
    def test_replay_skips_own(self, mock_execute):
        from migrate.grants_worker import replay_grants

        auth = MagicMock()
        grants = [{"principal": "admin_user", "action_type": "OWN"}]

        results = replay_grants(
            "SCHEMA",
            "`cat`.`sch`",
            grants,
            auth=auth,
            wh_id="wh-gr-2",
            dry_run=False,
        )

        assert len(results) == 0
        mock_execute.assert_not_called()

    @patch("migrate.grants_worker.execute_and_poll")
    def test_replay_dry_run(self, mock_execute):
        from migrate.grants_worker import replay_grants

        auth = MagicMock()
        grants = [{"principal": "analysts", "action_type": "USAGE"}]

        results = replay_grants(
            "CATALOG",
            "`prod`",
            grants,
            auth=auth,
            wh_id="wh-gr-3",
            dry_run=True,
        )

        assert len(results) == 1
        assert results[0]["status"] == "skipped"
        assert results[0]["error_message"] == "dry_run"
        mock_execute.assert_not_called()

    @patch("migrate.grants_worker.time")
    @patch("migrate.grants_worker.execute_and_poll")
    def test_replay_failure(self, mock_execute, mock_time):
        from migrate.grants_worker import replay_grants

        mock_time.time.side_effect = [100.0, 103.0]
        mock_execute.return_value = {
            "state": "FAILED",
            "error": "PRINCIPAL_NOT_FOUND",
            "statement_id": "s-4",
        }

        auth = MagicMock()
        grants = [{"principal": "unknown_group", "action_type": "SELECT"}]

        results = replay_grants(
            "SCHEMA",
            "`cat`.`sch`",
            grants,
            auth=auth,
            wh_id="wh-gr-4",
            dry_run=False,
        )

        assert len(results) == 1
        assert results[0]["status"] == "failed"
        assert "PRINCIPAL_NOT_FOUND" in results[0]["error_message"]
