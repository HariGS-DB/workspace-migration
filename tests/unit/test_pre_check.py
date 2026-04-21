from unittest.mock import MagicMock, patch

import pytest

from common.config import MigrationConfig
from pre_check.pre_check import run


def _make_config(**overrides) -> MigrationConfig:
    defaults = dict(
        source_workspace_url="https://source.test",
        target_workspace_url="https://target.test",
        spn_client_id="test-id",
        spn_secret_scope="scope",
        spn_secret_key="key",
    )
    defaults.update(overrides)
    return MigrationConfig(**defaults)


class TestPreCheck:
    @patch("pre_check.pre_check.MigrationConfig.from_workspace_file")
    @patch("pre_check.pre_check.AuthManager")
    @patch("pre_check.pre_check.TrackingManager")
    @patch("pre_check.pre_check.CatalogExplorer")
    def test_all_checks_pass(
        self, mock_explorer_cls, mock_tracker_cls, mock_auth_cls, mock_from_file
    ):
        dbutils = MagicMock()
        spark = MagicMock()
        mock_from_file.return_value = _make_config()

        mock_auth = mock_auth_cls.return_value
        mock_auth.test_connectivity.return_value = {"source": True, "target": True}
        mock_auth.source_client.shares.list.return_value = []
        mock_auth.target_client.shares.list.return_value = []
        mock_auth.target_client.metastores.summary.return_value = MagicMock(name="test-metastore")
        mock_auth.target_client.storage_credentials.list.return_value = [MagicMock()]
        mock_auth.target_client.external_locations.list.return_value = [MagicMock()]

        mock_explorer_cls.return_value.list_catalogs.return_value = ["cat_a"]
        spark.sql.return_value.first.return_value = MagicMock(ms="test-metastore-id")

        # Phase 2 additions need a source_client that supports cluster/warehouse listing
        mock_auth.source_client.clusters.list.return_value = []
        mock_auth.source_client.warehouses.list.return_value = []
        # Hive enumeration used by check_hive_dbfs_root_config — succeed with no DBFs
        mock_explorer_cls.return_value.list_hive_databases.return_value = []
        mock_explorer_cls.return_value.classify_hive_tables.return_value = []

        results = run(dbutils, spark)

        # 10 core UC checks + 3 Hive/external-metastore checks = 13
        assert len(results) == 13
        assert all(r["status"] in ("PASS", "WARN") for r in results)
        assert sum(1 for r in results if r["status"] == "FAIL") == 0

    @patch("pre_check.pre_check.MigrationConfig.from_workspace_file")
    @patch("pre_check.pre_check.AuthManager")
    @patch("pre_check.pre_check.TrackingManager")
    @patch("pre_check.pre_check.CatalogExplorer")
    def test_auth_failure_raises(
        self, mock_explorer_cls, mock_tracker_cls, mock_auth_cls, mock_from_file
    ):
        dbutils = MagicMock()
        spark = MagicMock()
        mock_from_file.return_value = _make_config()

        mock_auth = mock_auth_cls.return_value
        mock_auth.test_connectivity.return_value = {"source": False, "target": True}
        mock_auth.target_client.metastores.summary.return_value = MagicMock(name="ms")
        mock_auth.source_client.shares.list.return_value = []
        mock_auth.target_client.shares.list.return_value = []
        mock_auth.target_client.storage_credentials.list.return_value = []
        mock_auth.target_client.external_locations.list.return_value = []
        mock_explorer_cls.return_value.list_catalogs.return_value = []

        with pytest.raises(Exception, match="Pre-check failed"):
            run(dbutils, spark)


def _base_mocks(mock_auth_cls, mock_tracker_cls, mock_explorer_cls, spark):
    """Wire up the default 'all green' mocks. Individual tests override one
    piece to assert that specific check FAILs cleanly."""
    mock_auth = mock_auth_cls.return_value
    mock_auth.test_connectivity.return_value = {"source": True, "target": True}
    mock_auth.source_client.shares.list.return_value = []
    mock_auth.target_client.shares.list.return_value = []
    mock_auth.target_client.metastores.summary.return_value = MagicMock(name="ms")
    mock_auth.target_client.storage_credentials.list.return_value = [MagicMock()]
    mock_auth.target_client.external_locations.list.return_value = [MagicMock()]
    mock_auth.source_client.clusters.list.return_value = []
    mock_auth.source_client.warehouses.list.return_value = []

    mock_explorer_cls.return_value.list_catalogs.return_value = ["cat_a"]
    mock_explorer_cls.return_value.list_hive_databases.return_value = []
    mock_explorer_cls.return_value.classify_hive_tables.return_value = []

    spark.sql.return_value.first.return_value = MagicMock(ms="metastore-id")
    return mock_auth


class TestPreCheckIndividualFailures:
    """Per-check failure tests — each one isolates one precondition so the
    FAIL path for that check is specifically exercised. Complements the
    happy path + generic-auth-failure tests above."""

    @patch("pre_check.pre_check.MigrationConfig.from_workspace_file")
    @patch("pre_check.pre_check.AuthManager")
    @patch("pre_check.pre_check.TrackingManager")
    @patch("pre_check.pre_check.CatalogExplorer")
    def test_target_auth_failure(
        self, mock_explorer_cls, mock_tracker_cls, mock_auth_cls, mock_from_file
    ):
        """If target connectivity fails, ``check_target_auth`` FAILs and
        ``run()`` raises (because at least one check failed)."""
        dbutils = MagicMock()
        spark = MagicMock()
        mock_from_file.return_value = _make_config()
        mock_auth = _base_mocks(mock_auth_cls, mock_tracker_cls, mock_explorer_cls, spark)
        mock_auth.test_connectivity.return_value = {"source": True, "target": False}

        with pytest.raises(Exception, match="Pre-check failed"):
            run(dbutils, spark)

    @patch("pre_check.pre_check.MigrationConfig.from_workspace_file")
    @patch("pre_check.pre_check.AuthManager")
    @patch("pre_check.pre_check.TrackingManager")
    @patch("pre_check.pre_check.CatalogExplorer")
    def test_source_metastore_lookup_failure(
        self, mock_explorer_cls, mock_tracker_cls, mock_auth_cls, mock_from_file
    ):
        """spark.sql(SELECT current_metastore()) raising bubbles into
        check_source_metastore FAILing with the action-required hint."""
        dbutils = MagicMock()
        spark = MagicMock()
        mock_from_file.return_value = _make_config()
        _base_mocks(mock_auth_cls, mock_tracker_cls, mock_explorer_cls, spark)

        # Make only the metastore query fail.
        def _sql_side_effect(query, *args, **kwargs):
            r = MagicMock()
            if "current_metastore()" in query:
                raise RuntimeError("metastore unreachable")
            r.first.return_value = MagicMock()
            return r

        spark.sql.side_effect = _sql_side_effect

        with pytest.raises(Exception, match="Pre-check failed"):
            run(dbutils, spark)

    @patch("pre_check.pre_check.MigrationConfig.from_workspace_file")
    @patch("pre_check.pre_check.AuthManager")
    @patch("pre_check.pre_check.TrackingManager")
    @patch("pre_check.pre_check.CatalogExplorer")
    def test_target_metastore_lookup_failure(
        self, mock_explorer_cls, mock_tracker_cls, mock_auth_cls, mock_from_file
    ):
        """auth.target_client.metastores.summary raising is caught by
        check_target_metastore's try/except."""
        dbutils = MagicMock()
        spark = MagicMock()
        mock_from_file.return_value = _make_config()
        mock_auth = _base_mocks(mock_auth_cls, mock_tracker_cls, mock_explorer_cls, spark)
        mock_auth.target_client.metastores.summary.side_effect = RuntimeError(
            "target metastore lookup failed"
        )

        with pytest.raises(Exception, match="Pre-check failed"):
            run(dbutils, spark)

    @patch("pre_check.pre_check.MigrationConfig.from_workspace_file")
    @patch("pre_check.pre_check.AuthManager")
    @patch("pre_check.pre_check.TrackingManager")
    @patch("pre_check.pre_check.CatalogExplorer")
    def test_source_catalog_enumeration_failure(
        self, mock_explorer_cls, mock_tracker_cls, mock_auth_cls, mock_from_file
    ):
        """list_catalogs raising → check_source_catalogs FAILs."""
        dbutils = MagicMock()
        spark = MagicMock()
        mock_from_file.return_value = _make_config()
        _base_mocks(mock_auth_cls, mock_tracker_cls, mock_explorer_cls, spark)
        mock_explorer_cls.return_value.list_catalogs.side_effect = RuntimeError(
            "permission denied on metastore"
        )

        with pytest.raises(Exception, match="Pre-check failed"):
            run(dbutils, spark)

    @patch("pre_check.pre_check.MigrationConfig.from_workspace_file")
    @patch("pre_check.pre_check.AuthManager")
    @patch("pre_check.pre_check.TrackingManager")
    @patch("pre_check.pre_check.CatalogExplorer")
    def test_all_checks_recorded_even_when_one_fails(
        self, mock_explorer_cls, mock_tracker_cls, mock_auth_cls, mock_from_file
    ):
        """One check failing must not abort the rest — run() should
        continue through all checks and record each so the operator sees
        the full picture in one report."""
        dbutils = MagicMock()
        spark = MagicMock()
        mock_from_file.return_value = _make_config()
        mock_auth = _base_mocks(mock_auth_cls, mock_tracker_cls, mock_explorer_cls, spark)
        mock_auth.target_client.metastores.summary.side_effect = RuntimeError("oops")

        # Check the tracker.append_pre_check_results got ALL checks,
        # not just the first few before the failure.
        tracker = mock_tracker_cls.return_value
        try:
            run(dbutils, spark)
        except Exception:
            pass  # raise is expected once at least one check failed
        # At least one call with >=5 entries (confirms we ran the full
        # battery; exact count varies with enabled checks).
        recorded = tracker.append_pre_check_results.call_args_list
        assert recorded, "No pre-check results were written."
        total_rows = sum(len(c.args[0]) for c in recorded)
        assert total_rows >= 5, (
            f"Only {total_rows} pre-check result(s) recorded — one "
            f"check failing should not abort the rest."
        )
