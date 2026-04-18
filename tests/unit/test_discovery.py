from unittest.mock import MagicMock, patch

from common.config import MigrationConfig
from discovery.discovery import run


def _make_config(**overrides) -> MigrationConfig:
    defaults = dict(
        source_workspace_url="https://source.test",
        target_workspace_url="https://target.test",
        spn_client_id="test-id",
        spn_secret_scope="scope",
        spn_secret_key="key",
        catalog_filter=["test_catalog"],
    )
    defaults.update(overrides)
    return MigrationConfig(**defaults)


class TestDiscovery:
    @patch("discovery.discovery.MigrationConfig.from_workspace_file")
    @patch("discovery.discovery.AuthManager")
    @patch("discovery.discovery.TrackingManager")
    @patch("discovery.discovery.CatalogExplorer")
    def test_discovers_objects(
        self, mock_explorer_cls, mock_tracker_cls, mock_auth_cls, mock_from_file
    ):
        dbutils = MagicMock()
        spark = MagicMock()
        mock_from_file.return_value = _make_config()

        explorer = mock_explorer_cls.return_value
        explorer.list_catalogs.return_value = ["test_catalog"]
        explorer.list_schemas.return_value = ["schema_a"]
        explorer.classify_tables.return_value = [
            {
                "fqn": "`test_catalog`.`schema_a`.`t1`",
                "object_type": "managed_table",
                "table_type": "MANAGED",
                "data_source_format": "DELTA",
            },
        ]
        explorer.detect_dlt_managed.return_value = (False, None)
        explorer.get_table_row_count.return_value = 100
        explorer.get_table_size_bytes.return_value = 5000
        explorer.get_create_statement.return_value = "CREATE TABLE t1 (...)"
        explorer.list_functions.return_value = ["`test_catalog`.`schema_a`.`fn1`"]
        explorer.get_function_ddl.return_value = "CREATE FUNCTION fn1..."
        explorer.list_volumes.return_value = [{"fqn": "`test_catalog`.`schema_a`.`vol1`", "volume_type": "MANAGED"}]

        inventory = run(dbutils, spark)

        assert len(inventory) == 3
        types = {obj["object_type"] for obj in inventory}
        assert types == {"managed_table", "function", "volume"}

        tracker = mock_tracker_cls.return_value
        tracker.init_tracking_tables.assert_called_once()
        tracker.write_discovery_inventory.assert_called_once()

    @patch("discovery.discovery.MigrationConfig.from_workspace_file")
    @patch("discovery.discovery.AuthManager")
    @patch("discovery.discovery.TrackingManager")
    @patch("discovery.discovery.CatalogExplorer")
    def test_empty_catalog(
        self, mock_explorer_cls, mock_tracker_cls, mock_auth_cls, mock_from_file
    ):
        dbutils = MagicMock()
        spark = MagicMock()
        mock_from_file.return_value = _make_config()

        explorer = mock_explorer_cls.return_value
        explorer.list_catalogs.return_value = ["test_catalog"]
        explorer.list_schemas.return_value = []

        inventory = run(dbutils, spark)
        assert inventory == []
