from unittest.mock import MagicMock

import pytest
from common.config import MigrationConfig


@pytest.fixture
def mock_config() -> MigrationConfig:
    return MigrationConfig(
        source_workspace_url="https://source.azuredatabricks.net",
        target_workspace_url="https://target.azuredatabricks.net",
        spn_client_id="test-client-id",
        spn_secret_scope="migration-scope",
        spn_secret_key="spn-secret",
        catalog_filter=["catalog_a", "catalog_b"],
        tracking_catalog="migration_tracking",
        tracking_schema="cp_migration",
    )


@pytest.fixture
def mock_dbutils():
    dbutils = MagicMock()
    _widget_values = {
        "source_workspace_url": "https://source.azuredatabricks.net",
        "target_workspace_url": "https://target.azuredatabricks.net",
        "spn_client_id": "test-client-id",
        "spn_secret_scope": "migration-scope",
        "spn_secret_key": "spn-secret",
        "catalog_filter": "catalog_a, catalog_b",
        "schema_filter": "",
        "tracking_catalog": "migration_tracking",
        "tracking_schema": "cp_migration",
        "dry_run": "false",
        "batch_size": "50",
    }
    # Use .get with default so new widgets (added via `dbutils.widgets.text`) that
    # aren't pre-seeded here fall back cleanly rather than raising KeyError.
    dbutils.widgets.get.side_effect = lambda key: _widget_values.get(key, "")
    dbutils.widgets.text.side_effect = lambda name, default, *args, **kwargs: _widget_values.setdefault(name, default)
    dbutils.secrets.get.return_value = "fake-secret"
    return dbutils


@pytest.fixture
def mock_spark():
    spark = MagicMock()
    spark.sql.return_value = MagicMock()
    return spark


@pytest.fixture
def mock_workspace_client():
    client = MagicMock()
    client.current_user.me.return_value = MagicMock(user_name="test@databricks.com")
    return client
