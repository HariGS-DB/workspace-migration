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


class TestGrantsWorkerKnownLimitations:
    """Contract tests that lock in the tool's current CURRENT behavior
    around grants — NOT its aspirational behavior. These exist so that a
    future fix for the table-level gap is visible as "tests changed" in
    code review rather than silently extending coverage.

    Known gap (see README + follow-up list): grants_worker only enumerates
    CATALOG + SCHEMA grants. Table, view, volume, function grants are NOT
    migrated today. Documented via test here.
    """

    def test_run_only_processes_catalog_and_schema_grants(self):
        """Inspect the source of grants_worker.run() to verify it only
        iterates catalogs + schemas — never tables/views/volumes. If a
        future refactor extends this, the test fails loudly and the gap
        documentation needs updating."""
        import pathlib
        src = (
            pathlib.Path(__file__).resolve().parents[2]
            / "src" / "migrate" / "grants_worker.py"
        ).read_text()
        # These SHOULD appear — current behavior:
        assert 'list_grants("CATALOG"' in src, (
            "grants_worker must process CATALOG grants."
        )
        assert 'list_grants("SCHEMA"' in src, (
            "grants_worker must process SCHEMA grants."
        )
        # These SHOULD NOT appear today. If they do, either:
        #   a) someone extended grants_worker (update this test + README), or
        #   b) they added a pattern that only LOOKS like table-level grant
        #      processing and this test needs tightening.
        for unsupported in ('list_grants("TABLE"', 'list_grants("VIEW"',
                            'list_grants("VOLUME"', 'list_grants("FUNCTION"'):
            assert unsupported not in src, (
                f"grants_worker now appears to process {unsupported!r}. "
                f"If that's intentional, update the README's known-gap "
                f"list and this test to reflect the new behavior."
            )

    def test_replay_grants_skips_owner_grants(self):
        """OWNER grants are set differently (ALTER ... OWNER TO) — the
        grants_worker has an explicit skip for them. Lock this in."""
        from migrate.grants_worker import replay_grants

        auth = MagicMock()
        grants = [
            {
                "action_type": "SELECT",
                "principal": "user1",
                "grantable": False,
            },
            {
                "action_type": "OWN",
                "principal": "user2",
                "grantable": False,
            },
        ]
        with patch("migrate.grants_worker.execute_and_poll") as mock_exec:
            mock_exec.return_value = {"state": "SUCCEEDED", "statement_id": "s"}
            results = replay_grants(
                "SCHEMA",
                "`cat`.`sch`",
                grants,
                auth=auth,
                wh_id="wh-owner",
                dry_run=False,
            )
        # Only SELECT was replayed; OWN was skipped (no result row).
        assert len(results) == 1
        assert "SELECT" in results[0]["object_name"]
        assert "user1" in results[0]["object_name"]
        sql_sent = mock_exec.call_args[0][2]
        assert sql_sent.startswith("GRANT SELECT")
        assert mock_exec.call_count == 1
