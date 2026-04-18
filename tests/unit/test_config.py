from unittest.mock import MagicMock

from common.config import MigrationConfig


class TestMigrationConfig:
    def test_from_job_params(self, mock_dbutils):
        config = MigrationConfig.from_job_params(mock_dbutils)

        assert config.source_workspace_url == "https://source.azuredatabricks.net"
        assert config.target_workspace_url == "https://target.azuredatabricks.net"
        assert config.spn_client_id == "test-client-id"
        assert config.spn_secret_scope == "migration-scope"
        assert config.spn_secret_key == "spn-secret"
        assert config.catalog_filter == ["catalog_a", "catalog_b"]
        assert config.schema_filter == []
        assert config.dry_run is False

    def test_from_job_params_with_dry_run(self):
        dbutils = MagicMock()
        dbutils.widgets.get.side_effect = lambda key: {
            "source_workspace_url": "https://source.azuredatabricks.net",
            "target_workspace_url": "https://target.azuredatabricks.net",
            "spn_client_id": "test-client-id",
            "spn_secret_scope": "migration-scope",
            "spn_secret_key": "spn-secret",
            "catalog_filter": "",
            "schema_filter": "",
            "tracking_catalog": "migration_tracking",
            "tracking_schema": "cp_migration",
            "dry_run": "true",
            "batch_size": "50",
        }.get(key, "")

        config = MigrationConfig.from_job_params(dbutils)

        assert config.dry_run is True
        assert config.catalog_filter == []

    def test_defaults(self):
        config = MigrationConfig(
            source_workspace_url="https://src.azuredatabricks.net",
            target_workspace_url="https://tgt.azuredatabricks.net",
            spn_client_id="client-id",
            spn_secret_scope="scope",
            spn_secret_key="key",
        )

        assert config.catalog_filter == []
        assert config.schema_filter == []
        assert config.tracking_catalog == "migration_tracking"
        assert config.tracking_schema == "cp_migration"
        assert config.dry_run is False
        assert config.batch_size == 50
