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

        results = run(dbutils, spark)

        assert len(results) == 10
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
