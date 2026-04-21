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


class TestToolOwnedCatalogs:
    """_tool_owned_catalogs ensures the migration tool never tries to
    migrate its own tracking / share-consumer state."""

    def test_includes_tracking_catalog(self):
        from discovery.discovery import _tool_owned_catalogs
        config = MagicMock()
        config.tracking_catalog = "migration_tracking"
        excluded = _tool_owned_catalogs(config)
        assert "migration_tracking" in excluded

    def test_includes_share_consumer(self):
        from discovery.discovery import _tool_owned_catalogs
        config = MagicMock()
        config.tracking_catalog = "migration_tracking"
        excluded = _tool_owned_catalogs(config)
        # share-consumer is named ``{cp_migration_share}_consumer`` — hardcoded.
        assert "cp_migration_share_consumer" in excluded

    def test_respects_custom_tracking_catalog_name(self):
        """If the operator configures a non-default tracking_catalog,
        excludes that one instead of the default."""
        from discovery.discovery import _tool_owned_catalogs
        config = MagicMock()
        config.tracking_catalog = "custom_tracking"
        excluded = _tool_owned_catalogs(config)
        assert "custom_tracking" in excluded


class TestWarnRlsCmTables:
    """_warn_rls_cm_tables prints a prominent banner listing tables with
    row filter or column mask. Operators see this in the discovery run log
    and decide whether to migrate RLS/CM governance to ABAC or accept skip."""

    def test_silent_when_no_rls_cm_rows(self, capsys):
        from discovery.discovery import _warn_rls_cm_tables
        config = MagicMock()
        config.rls_cm_strategy = ""
        rows = [
            {"object_type": "managed_table", "object_name": "`c`.`s`.`t`"},
            {"object_type": "view", "object_name": "`c`.`s`.`v`"},
        ]
        _warn_rls_cm_tables(rows, config)
        captured = capsys.readouterr()
        assert "TABLES WITH ROW FILTER" not in captured.out
        assert "managed_sensitive" not in captured.out

    def test_fires_on_row_filter(self, capsys):
        from discovery.discovery import _warn_rls_cm_tables
        config = MagicMock()
        config.rls_cm_strategy = ""
        rows = [
            {
                "object_type": "row_filter",
                "object_name": "`c`.`s`.`t`",
                "metadata_json": None,
            },
        ]
        _warn_rls_cm_tables(rows, config)
        out = capsys.readouterr().out
        assert "TABLES WITH ROW FILTER" in out
        assert "`c`.`s`.`t`" in out
        assert "skipped_by_rls_cm_policy" in out

    def test_fires_on_column_mask_via_metadata(self, capsys):
        """column_mask rows have ``object_name`` = ``<fqn>.<col>`` so the
        clean table_fqn must be pulled from metadata_json."""
        import json as _json
        from discovery.discovery import _warn_rls_cm_tables
        config = MagicMock()
        config.rls_cm_strategy = ""
        rows = [
            {
                "object_type": "column_mask",
                "object_name": "`c`.`s`.`t`.ssn",
                "metadata_json": _json.dumps({"table_fqn": "`c`.`s`.`t`"}),
            },
        ]
        _warn_rls_cm_tables(rows, config)
        out = capsys.readouterr().out
        assert "TABLES WITH ROW FILTER / COLUMN MASK" in out
        assert "`c`.`s`.`t`" in out

    def test_deduplicates_table_across_rf_and_cm_rows(self, capsys):
        """Same table listed once even if it has both filter and mask."""
        import json as _json
        from discovery.discovery import _warn_rls_cm_tables
        config = MagicMock()
        config.rls_cm_strategy = ""
        rows = [
            {"object_type": "row_filter", "object_name": "`c`.`s`.`t`"},
            {
                "object_type": "column_mask",
                "object_name": "`c`.`s`.`t`.ssn",
                "metadata_json": _json.dumps({"table_fqn": "`c`.`s`.`t`"}),
            },
        ]
        _warn_rls_cm_tables(rows, config)
        out = capsys.readouterr().out
        # Table should appear exactly once in the listed bullets.
        assert out.count("- `c`.`s`.`t`") == 1

    def test_calls_out_drop_and_restore_when_flagged(self, capsys):
        """If operator set rls_cm_strategy=drop_and_restore, surface the
        ``NOT YET IMPLEMENTED`` warning so they know setup_sharing will fail."""
        from discovery.discovery import _warn_rls_cm_tables
        config = MagicMock()
        config.rls_cm_strategy = "drop_and_restore"
        rows = [{"object_type": "row_filter", "object_name": "`c`.`s`.`t`"}]
        _warn_rls_cm_tables(rows, config)
        out = capsys.readouterr().out
        assert "NOT YET IMPLEMENTED" in out

    def test_tolerates_malformed_metadata_json(self, capsys):
        from discovery.discovery import _warn_rls_cm_tables
        config = MagicMock()
        config.rls_cm_strategy = ""
        rows = [
            {
                "object_type": "column_mask",
                "object_name": "`c`.`s`.`t`.ssn",
                "metadata_json": "not-json",
            },
            {"object_type": "row_filter", "object_name": "`c`.`s`.`other`"},
        ]
        # Shouldn't raise, and still surfaces the row_filter row.
        _warn_rls_cm_tables(rows, config)
        out = capsys.readouterr().out
        assert "`c`.`s`.`other`" in out
