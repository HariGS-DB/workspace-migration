from unittest.mock import MagicMock, patch

from discovery.discovery import run


class TestDiscovery:
    def _make_dbutils(self):
        dbutils = MagicMock()
        dbutils.widgets.get.side_effect = lambda key: {
            "source_workspace_url": "https://source.test",
            "target_workspace_url": "https://target.test",
            "spn_client_id": "test-id",
            "spn_secret_scope": "scope",
            "spn_secret_key": "key",
            "catalog_filter": "test_catalog",
            "schema_filter": "",
            "tracking_catalog": "migration_tracking",
            "tracking_schema": "cp_migration",
            "dry_run": "false",
            "batch_size": "50",
        }.get(key, "")
        dbutils.secrets.get.return_value = "fake-secret"
        return dbutils

    @patch("discovery.discovery.AuthManager")
    @patch("discovery.discovery.TrackingManager")
    @patch("discovery.discovery.CatalogExplorer")
    def test_discovers_objects(self, mock_explorer_cls, mock_tracker_cls, mock_auth_cls):
        dbutils = self._make_dbutils()
        spark = MagicMock()

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

        assert len(inventory) == 3  # 1 table + 1 function + 1 volume
        types = {obj["object_type"] for obj in inventory}
        assert types == {"managed_table", "function", "volume"}

        # Verify tracker was called
        tracker = mock_tracker_cls.return_value
        tracker.init_tracking_tables.assert_called_once()
        tracker.write_discovery_inventory.assert_called_once()

    @patch("discovery.discovery.AuthManager")
    @patch("discovery.discovery.TrackingManager")
    @patch("discovery.discovery.CatalogExplorer")
    def test_empty_catalog(self, mock_explorer_cls, mock_tracker_cls, mock_auth_cls):
        dbutils = self._make_dbutils()
        spark = MagicMock()

        explorer = mock_explorer_cls.return_value
        explorer.list_catalogs.return_value = ["test_catalog"]
        explorer.list_schemas.return_value = []

        inventory = run(dbutils, spark)
        assert inventory == []
