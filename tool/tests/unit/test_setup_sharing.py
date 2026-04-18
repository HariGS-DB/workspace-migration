from unittest.mock import MagicMock

from migrate.setup_sharing import (
    add_tables_to_share,
    ensure_target_catalogs_and_schemas,
    get_or_create_recipient,
    get_or_create_share,
)


class TestSetupSharing:
    def test_get_or_create_share_exists(self):
        auth = MagicMock()
        auth.source_client.shares.get.return_value = MagicMock(name="test_share")

        get_or_create_share(auth, "test_share")
        auth.source_client.shares.create.assert_not_called()

    def test_get_or_create_share_creates(self):
        auth = MagicMock()
        auth.source_client.shares.get.side_effect = Exception("Not found")
        auth.source_client.shares.create.return_value = MagicMock(name="new_share")

        get_or_create_share(auth, "new_share")
        auth.source_client.shares.create.assert_called_once_with(name="new_share")

    def test_get_or_create_share_dry_run(self):
        auth = MagicMock()
        auth.source_client.shares.get.side_effect = Exception("Not found")

        result = get_or_create_share(auth, "test_share", dry_run=True)
        assert result == "test_share"
        auth.source_client.shares.create.assert_not_called()

    def test_get_or_create_recipient_exists(self):
        auth = MagicMock()
        auth.source_client.recipients.get.return_value = MagicMock(name="cp_migration_recipient_ms123")

        get_or_create_recipient(auth, "ms123")
        auth.source_client.recipients.create.assert_not_called()

    def test_get_or_create_recipient_creates(self):
        auth = MagicMock()
        auth.source_client.recipients.get.side_effect = Exception("Not found")
        auth.source_client.recipients.create.return_value = MagicMock(name="cp_migration_recipient_ms123")

        get_or_create_recipient(auth, "ms123")
        auth.source_client.recipients.create.assert_called_once_with(
            name="cp_migration_recipient_ms123",
            authentication_type="DATABRICKS",
            data_recipient_global_metastore_id="ms123",
        )

    def test_get_or_create_recipient_dry_run(self):
        auth = MagicMock()
        auth.source_client.recipients.get.side_effect = Exception("Not found")

        result = get_or_create_recipient(auth, "ms123", dry_run=True)
        assert result == "cp_migration_recipient_ms123"
        auth.source_client.recipients.create.assert_not_called()

    def test_add_tables_to_share_dry_run(self):
        auth = MagicMock()
        tables = [
            {"object_name": "`cat_a`.`sch_1`.`t1`"},
            {"object_name": "`cat_a`.`sch_1`.`t2`"},
        ]

        add_tables_to_share(auth, "test_share", tables, dry_run=True)
        auth.source_client.shares.update.assert_not_called()

    def test_add_tables_to_share_calls_update(self):
        auth = MagicMock()
        tables = [
            {"object_name": "`cat_a`.`sch_1`.`t1`"},
        ]

        add_tables_to_share(auth, "test_share", tables)
        auth.source_client.shares.update.assert_called_once()

    def test_add_tables_skips_malformed_fqn(self):
        auth = MagicMock()
        tables = [
            {"object_name": "bad_fqn"},
        ]

        add_tables_to_share(auth, "test_share", tables)
        auth.source_client.shares.update.assert_not_called()

    def test_ensure_target_catalogs_dry_run(self):
        auth = MagicMock()
        tables = [
            {"catalog_name": "cat_a", "schema_name": "sch_1"},
            {"catalog_name": "cat_a", "schema_name": "sch_2"},
        ]

        ensure_target_catalogs_and_schemas(auth, tables, dry_run=True)
        auth.target_client.catalogs.create.assert_not_called()
        auth.target_client.schemas.create.assert_not_called()

    def test_ensure_target_catalogs_creates_missing(self):
        auth = MagicMock()
        auth.target_client.catalogs.get.side_effect = Exception("Not found")
        auth.target_client.schemas.get.side_effect = Exception("Not found")

        tables = [
            {"catalog_name": "cat_a", "schema_name": "sch_1"},
        ]

        ensure_target_catalogs_and_schemas(auth, tables)
        auth.target_client.catalogs.create.assert_called_once_with(name="cat_a")
        auth.target_client.schemas.create.assert_called_once_with(name="sch_1", catalog_name="cat_a")

    def test_ensure_target_catalogs_skips_existing(self):
        auth = MagicMock()
        # catalogs.get and schemas.get succeed (already exist)

        tables = [
            {"catalog_name": "cat_a", "schema_name": "sch_1"},
        ]

        ensure_target_catalogs_and_schemas(auth, tables)
        auth.target_client.catalogs.create.assert_not_called()
        auth.target_client.schemas.create.assert_not_called()
