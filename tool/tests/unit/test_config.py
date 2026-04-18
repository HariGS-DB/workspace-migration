from __future__ import annotations

from pathlib import Path

import pytest

from common.config import MigrationConfig


def _write(tmp_path: Path, body: str) -> Path:
    p = tmp_path / "config.yaml"
    p.write_text(body)
    return p


class TestMigrationConfig:
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
        assert config.migrate_hive_dbfs_root is False
        assert config.hive_dbfs_target_path == ""
        assert config.hive_target_catalog == "hive_upgraded"

    def test_from_workspace_file_minimal(self, tmp_path):
        path = _write(tmp_path, """
source_workspace_url: https://src.azuredatabricks.net
target_workspace_url: https://tgt.azuredatabricks.net
spn_client_id: client-id
spn_secret_scope: migration
spn_secret_key: spn-secret
""")
        config = MigrationConfig.from_workspace_file(str(path))
        assert config.source_workspace_url == "https://src.azuredatabricks.net"
        assert config.spn_client_id == "client-id"
        # Defaults applied for missing optional fields
        assert config.tracking_catalog == "migration_tracking"
        assert config.dry_run is False
        assert config.migrate_hive_dbfs_root is False

    def test_from_workspace_file_full(self, tmp_path):
        path = _write(tmp_path, """
source_workspace_url: https://src.azuredatabricks.net
target_workspace_url: https://tgt.azuredatabricks.net
spn_client_id: client-id
spn_secret_scope: migration
spn_secret_key: spn-secret
catalog_filter: "cat_a, cat_b"
schema_filter: ""
tracking_catalog: custom_tracking
tracking_schema: custom_schema
dry_run: true
batch_size: 25
migrate_hive_dbfs_root: true
hive_dbfs_target_path: abfss://hive@acct.dfs.core.windows.net/upgraded/
hive_target_catalog: legacy_hive
""")
        config = MigrationConfig.from_workspace_file(str(path))
        assert config.catalog_filter == ["cat_a", "cat_b"]
        assert config.schema_filter == []
        assert config.dry_run is True
        assert config.batch_size == 25
        assert config.migrate_hive_dbfs_root is True
        assert config.hive_dbfs_target_path.startswith("abfss://")
        assert config.hive_target_catalog == "legacy_hive"

    def test_from_workspace_file_catalog_filter_as_list(self, tmp_path):
        """YAML list syntax for catalog_filter also works."""
        path = _write(tmp_path, """
source_workspace_url: https://src.azuredatabricks.net
target_workspace_url: https://tgt.azuredatabricks.net
spn_client_id: client-id
spn_secret_scope: migration
spn_secret_key: spn-secret
catalog_filter:
  - cat_a
  - cat_b
""")
        config = MigrationConfig.from_workspace_file(str(path))
        assert config.catalog_filter == ["cat_a", "cat_b"]

    def test_from_workspace_file_raises_on_missing_required(self, tmp_path):
        path = _write(tmp_path, """
source_workspace_url: https://src.azuredatabricks.net
target_workspace_url: https://tgt.azuredatabricks.net
# spn_client_id omitted
spn_secret_scope: migration
spn_secret_key: spn-secret
""")
        with pytest.raises(ValueError, match="spn_client_id"):
            MigrationConfig.from_workspace_file(str(path))

    def test_from_workspace_file_raises_on_empty_required(self, tmp_path):
        path = _write(tmp_path, """
source_workspace_url: https://src.azuredatabricks.net
target_workspace_url: https://tgt.azuredatabricks.net
spn_client_id: ""
spn_secret_scope: migration
spn_secret_key: spn-secret
""")
        with pytest.raises(ValueError, match="spn_client_id"):
            MigrationConfig.from_workspace_file(str(path))

    def test_from_workspace_file_raises_on_missing_file(self, tmp_path):
        with pytest.raises(FileNotFoundError):
            MigrationConfig.from_workspace_file(str(tmp_path / "does-not-exist.yaml"))
