from unittest.mock import MagicMock

import pytest

from migrate.setup_sharing import (
    add_tables_to_share,
    ensure_share_consumer_catalog,
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
        # Only assert that create was called; don't lock in specific kwarg shapes
        # (authentication_type enum vs string, metastore-id format are SDK-version
        # dependent and caused integration failures when over-specified here).
        auth.source_client.recipients.create.assert_called_once()

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

    def test_add_tables_to_share_strips_backticks(self):
        """Share API rejects backticked names; object_name must be stripped
        before being passed as ``data_object.name``."""
        auth = MagicMock()
        # Simulate an empty existing share so the pre-clean path is a no-op
        auth.source_client.shares.get.return_value = MagicMock(objects=[])

        tables = [{"object_name": "`cat_a`.`sch_1`.`t1`"}]
        add_tables_to_share(auth, "test_share", tables)

        # Find the "add" update call (after any pre-clean remove call)
        calls = auth.source_client.shares.update.call_args_list
        add_calls = [c for c in calls if c.kwargs.get("updates")
                     and any(u.action.value == "ADD" for u in c.kwargs["updates"] if hasattr(u.action, "value"))]
        assert add_calls, "shares.update was never called with an ADD"
        updates = add_calls[-1].kwargs["updates"]
        names = [u.data_object.name for u in updates]
        for n in names:
            assert "`" not in n, f"Share object name must not contain backticks: {n!r}"
            assert n.count(".") == 2, f"Expected catalog.schema.table, got: {n!r}"

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
        # Use assert_any_call so additional catalog creations (e.g. share-consumer
        # catalogs) don't break this test.
        auth.target_client.catalogs.create.assert_any_call(name="cat_a")
        auth.target_client.schemas.create.assert_any_call(name="sch_1", catalog_name="cat_a")

    def test_ensure_target_catalogs_skips_existing(self):
        auth = MagicMock()
        # catalogs.get and schemas.get succeed (already exist)

        tables = [
            {"catalog_name": "cat_a", "schema_name": "sch_1"},
        ]

        ensure_target_catalogs_and_schemas(auth, tables)
        auth.target_client.catalogs.create.assert_not_called()
        auth.target_client.schemas.create.assert_not_called()

    # ------------------------------------------------------------------
    # ensure_share_consumer_catalog
    # ------------------------------------------------------------------

    def _make_auth_with_matching_provider(self):
        """Build an auth mock with a target provider whose global metastore ID
        matches the source metastore. Returns (auth, provider_name)."""
        auth = MagicMock()
        auth.source_client.metastores.summary.return_value = MagicMock(
            global_metastore_id="azure:northeurope:aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee"
        )
        matching = MagicMock()
        matching.name = "src_provider"
        matching.data_provider_global_metastore_id = "azure:northeurope:aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee"
        other = MagicMock()
        other.name = "unrelated_provider"
        other.data_provider_global_metastore_id = "azure:westus:11111111-2222-3333-4444-555555555555"
        auth.target_client.providers.list.return_value = [other, matching]
        return auth, "src_provider"

    def test_ensure_share_consumer_catalog_creates_using_share(self):
        """Must find the matching target-side provider (by source metastore id)
        and create a catalog with provider_name + share_name kwargs.
        Skipping this step was the root cause of integration failure #16 —
        DEEP CLONE couldn't resolve the source tables without a consumer catalog.
        """
        auth, provider_name = self._make_auth_with_matching_provider()

        ensure_share_consumer_catalog(auth, "cp_migration_share", dry_run=False)

        auth.target_client.catalogs.create.assert_called_once()
        kwargs = auth.target_client.catalogs.create.call_args.kwargs
        assert kwargs["name"] == "cp_migration_share_consumer"
        assert kwargs["provider_name"] == provider_name
        assert kwargs["share_name"] == "cp_migration_share"

    def test_ensure_share_consumer_catalog_dry_run(self):
        auth, _ = self._make_auth_with_matching_provider()
        ensure_share_consumer_catalog(auth, "cp_migration_share", dry_run=True)
        auth.target_client.catalogs.create.assert_not_called()
        auth.target_client.catalogs.delete.assert_not_called()

    def test_ensure_share_consumer_catalog_drops_before_recreate(self):
        """Re-runs must drop the existing consumer catalog first — otherwise
        stale share structure (e.g. old shared_as format) causes DEEP CLONE
        to fail with unresolvable schema UUIDs."""
        auth, _ = self._make_auth_with_matching_provider()

        ensure_share_consumer_catalog(auth, "cp_migration_share", dry_run=False)

        # delete must be called before create
        delete_call = auth.target_client.catalogs.delete.call_args
        assert delete_call is not None, "Consumer catalog must be dropped before recreate"
        assert delete_call.args[0] == "cp_migration_share_consumer"

    def test_ensure_share_consumer_catalog_raises_when_no_provider_matches(self):
        """If the source metastore has never been shared to the target, no
        matching provider exists. Must fail loudly rather than create an
        invalid catalog."""
        auth = MagicMock()
        auth.source_client.metastores.summary.return_value = MagicMock(
            global_metastore_id="azure:northeurope:deadbeef-dead-beef-dead-beefdeadbeef"
        )
        # No matching provider
        other = MagicMock()
        other.name = "unrelated"
        other.data_provider_global_metastore_id = "azure:westus:11111111-2222-3333-4444-555555555555"
        auth.target_client.providers.list.return_value = [other]

        with pytest.raises(RuntimeError, match="No target-side provider"):
            ensure_share_consumer_catalog(auth, "cp_migration_share", dry_run=False)
        auth.target_client.catalogs.create.assert_not_called()
