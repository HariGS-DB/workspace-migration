from unittest.mock import MagicMock, patch

from pre_check.pre_check import run


class TestPreCheck:
    def _make_dbutils(self):
        dbutils = MagicMock()
        dbutils.widgets.get.side_effect = lambda key: {
            "source_workspace_url": "https://source.test",
            "target_workspace_url": "https://target.test",
            "spn_client_id": "test-id",
            "spn_secret_scope": "scope",
            "spn_secret_key": "key",
            "catalog_filter": "",
            "schema_filter": "",
            "dry_run": "false",
        }[key]
        dbutils.secrets.get.return_value = "fake-secret"
        return dbutils

    @patch("pre_check.pre_check.AuthManager")
    @patch("pre_check.pre_check.TrackingManager")
    @patch("pre_check.pre_check.CatalogExplorer")
    def test_all_checks_pass(self, mock_explorer_cls, mock_tracker_cls, mock_auth_cls):
        dbutils = self._make_dbutils()
        spark = MagicMock()

        # Mock auth
        mock_auth = mock_auth_cls.return_value
        mock_auth.test_connectivity.return_value = {"source": True, "target": True}
        mock_auth.source_client.shares.list.return_value = []
        mock_auth.target_client.shares.list.return_value = []
        mock_auth.target_client.metastores.summary.return_value = MagicMock(name="test-metastore")
        mock_auth.target_client.storage_credentials.list.return_value = [MagicMock()]
        mock_auth.target_client.external_locations.list.return_value = [MagicMock()]

        # Mock explorer
        mock_explorer_cls.return_value.list_catalogs.return_value = ["cat_a"]

        # Mock spark
        spark.sql.return_value.first.return_value = MagicMock(ms="test-metastore-id")

        results = run(dbutils, spark)

        assert len(results) == 10
        assert all(r["status"] in ("PASS", "WARN") for r in results)
        fail_count = sum(1 for r in results if r["status"] == "FAIL")
        assert fail_count == 0

    @patch("pre_check.pre_check.AuthManager")
    @patch("pre_check.pre_check.TrackingManager")
    @patch("pre_check.pre_check.CatalogExplorer")
    def test_auth_failure_raises(self, mock_explorer_cls, mock_tracker_cls, mock_auth_cls):
        dbutils = self._make_dbutils()
        spark = MagicMock()

        mock_auth = mock_auth_cls.return_value
        mock_auth.test_connectivity.return_value = {"source": False, "target": True}
        mock_auth.target_client.metastores.summary.return_value = MagicMock(name="ms")
        mock_auth.source_client.shares.list.return_value = []
        mock_auth.target_client.shares.list.return_value = []
        mock_auth.target_client.storage_credentials.list.return_value = []
        mock_auth.target_client.external_locations.list.return_value = []
        mock_explorer_cls.return_value.list_catalogs.return_value = []

        import pytest

        with pytest.raises(Exception, match="Pre-check failed"):
            run(dbutils, spark)
