"""Tests for Phase 3 Task 27 discovery helpers.

Covers the 11 new list_* methods on CatalogExplorer. Each helper mocks
either spark.sql or the source Databricks SDK client; REST-based helpers
assert the try/except safety net returns [] on failure.
"""
from __future__ import annotations

from unittest.mock import MagicMock

from common.catalog_utils import CatalogExplorer


def _row(**kwargs):
    row = MagicMock()
    for k, v in kwargs.items():
        setattr(row, k, v)
    return row


def _explorer(spark, auth):
    return CatalogExplorer(spark, auth)


class TestListTags:
    def test_aggregates_all_securable_tag_tables(self, mock_spark):
        def sql_side_effect(query):
            result = MagicMock()
            if "catalog_tags" in query:
                result.collect.return_value = [_row(
                    catalog_name="c", tag_name="env", tag_value="prod",
                )]
            elif "schema_tags" in query:
                result.collect.return_value = [_row(
                    schema_name="s", tag_name="team", tag_value="data",
                )]
            elif "table_tags" in query:
                result.collect.return_value = [_row(
                    table_name="t", tag_name="owner", tag_value="alice",
                )]
            elif "column_tags" in query:
                result.collect.return_value = [_row(
                    table_name="t", column_name="email",
                    tag_name="pii", tag_value="true",
                )]
            elif "volume_tags" in query:
                result.collect.return_value = [_row(
                    volume_name="v", tag_name="purpose", tag_value="raw",
                )]
            else:
                result.collect.return_value = []
            return result

        mock_spark.sql.side_effect = sql_side_effect
        explorer = _explorer(mock_spark, MagicMock())
        tags = explorer.list_tags("c", "s")

        types = {t["securable_type"] for t in tags}
        assert types == {"CATALOG", "SCHEMA", "TABLE", "COLUMN", "VOLUME"}
        col_tag = next(t for t in tags if t["securable_type"] == "COLUMN")
        assert col_tag["column_name"] == "email"
        assert col_tag["tag_name"] == "pii"

    def test_tolerates_missing_tag_tables(self, mock_spark):
        """If system.information_schema.* isn't readable, return [] not raise."""
        mock_spark.sql.side_effect = Exception("table not found")
        explorer = _explorer(mock_spark, MagicMock())
        assert explorer.list_tags("c", "s") == []


class TestListRowFilters:
    """list_row_filters uses the UC Tables API (source_client.tables.list)
    for authoritative row-filter detection — information_schema doesn't
    reliably surface ``row_filter_name`` on every runtime."""

    def test_returns_table_and_filter_metadata(self, mock_spark):
        auth = MagicMock()
        t = MagicMock()
        t.name = "orders"
        t.row_filter.function_name = "c.s.region_filter"
        t.row_filter.input_column_names = ["region"]
        auth.source_client.tables.list.return_value = [t]
        explorer = _explorer(mock_spark, auth)
        rfs = explorer.list_row_filters("c", "s")

        assert len(rfs) == 1
        assert rfs[0]["table_fqn"] == "`c`.`s`.`orders`"
        assert rfs[0]["filter_function_fqn"] == "c.s.region_filter"
        assert rfs[0]["filter_columns"] == ["region"]

    def test_handles_missing_input_columns(self, mock_spark):
        auth = MagicMock()
        t = MagicMock()
        t.name = "orders"
        t.row_filter.function_name = "c.s.f"
        t.row_filter.input_column_names = None
        auth.source_client.tables.list.return_value = [t]
        explorer = _explorer(mock_spark, auth)
        rfs = explorer.list_row_filters("c", "s")
        assert rfs[0]["filter_columns"] == []

    def test_skips_tables_without_filter(self, mock_spark):
        """Tables whose .row_filter is None are not returned."""
        auth = MagicMock()
        t = MagicMock()
        t.name = "orders"
        t.row_filter = None
        auth.source_client.tables.list.return_value = [t]
        explorer = _explorer(mock_spark, auth)
        assert explorer.list_row_filters("c", "s") == []


class TestListColumnMasks:
    def test_returns_column_mask_metadata(self, mock_spark):
        auth = MagicMock()
        t = MagicMock()
        t.name = "users"
        col = MagicMock()
        col.name = "ssn"
        col.mask.function_name = "c.s.redact_ssn"
        col.mask.using_column_names = ["role"]
        t.columns = [col]
        auth.source_client.tables.list.return_value = [t]
        explorer = _explorer(mock_spark, auth)
        masks = explorer.list_column_masks("c", "s")

        assert len(masks) == 1
        assert masks[0]["table_fqn"] == "`c`.`s`.`users`"
        assert masks[0]["column_name"] == "ssn"
        assert masks[0]["mask_function_fqn"] == "c.s.redact_ssn"


class TestListPolicies:
    def test_parses_rest_response(self):
        auth = MagicMock()
        auth.source_client.api_client.do.return_value = {
            "policies": [
                {"name": "p1", "on_securable_fullname": "c.s.t"},
                {"name": "p2", "on_securable_fullname": "c.s"},
            ]
        }
        explorer = _explorer(MagicMock(), auth)
        policies = explorer.list_policies()

        assert len(policies) == 2
        assert policies[0]["policy_name"] == "p1"
        assert policies[0]["definition"]["on_securable_fullname"] == "c.s.t"

    def test_returns_empty_on_404(self):
        auth = MagicMock()
        auth.source_client.api_client.do.side_effect = Exception("404 Not Found")
        explorer = _explorer(MagicMock(), auth)
        assert explorer.list_policies() == []


class TestListMonitors:
    def test_skips_tables_without_monitor(self):
        auth = MagicMock()

        def do(method, path):
            if "orders" in path:
                return {"table_name": "c.s.orders", "schedule": {}}
            raise Exception("404")

        auth.source_client.api_client.do.side_effect = do
        explorer = _explorer(MagicMock(), auth)

        monitors = explorer.list_monitors(
            ["`c`.`s`.`orders`", "`c`.`s`.`no_monitor`"]
        )
        assert len(monitors) == 1
        assert monitors[0]["table_fqn"] == "`c`.`s`.`orders`"


class TestListRegisteredModels:
    def test_captures_versions_and_aliases(self):
        auth = MagicMock()

        model = MagicMock()
        model.full_name = "c.s.model_a"
        model.owner = "alice"
        model.storage_location = "abfss://model@acct.dfs.core.windows.net/ma"
        model.comment = "demo"
        auth.source_client.registered_models.list.return_value = [model]

        v1 = MagicMock()
        v1.version = 1
        v1.source = "run:/abc/artifact"
        v1.storage_location = "abfss://.../v1"
        v1.status = "READY"
        alias = MagicMock()
        alias.alias_name = "prod"
        v1.aliases = [alias]

        auth.source_client.model_versions.list.return_value = [v1]
        explorer = _explorer(MagicMock(), auth)

        models = explorer.list_registered_models("c", "s")
        assert len(models) == 1
        assert models[0]["model_fqn"] == "c.s.model_a"
        assert models[0]["versions"][0]["version"] == 1
        assert models[0]["versions"][0]["aliases"] == ["prod"]


class TestListConnections:
    def test_returns_connections_without_secrets(self):
        auth = MagicMock()

        c = MagicMock()
        c.name = "snowflake_conn"
        c.connection_type = "SNOWFLAKE"
        c.options = {"host": "acct.snowflakecomputing.com"}
        c.comment = None
        auth.source_client.connections.list.return_value = [c]

        explorer = _explorer(MagicMock(), auth)
        conns = explorer.list_connections()

        assert conns[0]["connection_name"] == "snowflake_conn"
        assert conns[0]["options"]["host"] == "acct.snowflakecomputing.com"


class TestListForeignCatalogs:
    def test_filters_to_foreign_only(self):
        auth = MagicMock()

        def _cat(name, ctype):
            c = MagicMock()
            c.name = name
            c.catalog_type = ctype
            c.connection_name = "snowflake_conn" if "snow" in name else None
            c.options = {}
            c.comment = None
            return c

        auth.source_client.catalogs.list.return_value = [
            _cat("regular_cat", "MANAGED_CATALOG"),
            _cat("snow_foreign", "FOREIGN_CATALOG"),
        ]
        explorer = _explorer(MagicMock(), auth)
        foreign = explorer.list_foreign_catalogs()

        assert len(foreign) == 1
        assert foreign[0]["catalog_name"] == "snow_foreign"
        assert foreign[0]["connection_name"] == "snowflake_conn"


class TestListOnlineTables:
    def test_returns_online_tables_via_rest(self):
        auth = MagicMock()
        auth.source_client.api_client.do.return_value = {
            "online_tables": [
                {
                    "name": "c.s.online_orders",
                    "spec": {"source_table_full_name": "c.s.orders"},
                }
            ]
        }
        explorer = _explorer(MagicMock(), auth)
        online = explorer.list_online_tables()

        assert len(online) == 1
        assert online[0]["online_table_fqn"] == "c.s.online_orders"
        assert online[0]["source_table_fqn"] == "c.s.orders"


class TestListShares:
    def test_excludes_migration_share(self):
        auth = MagicMock()

        def _share(name):
            s = MagicMock()
            s.name = name
            return s

        auth.source_client.shares.list.return_value = [
            _share("cp_migration_share"),
            _share("customer_share_a"),
        ]
        full = MagicMock()
        full.objects = []
        auth.source_client.shares.get.return_value = full

        explorer = _explorer(MagicMock(), auth)
        shares = explorer.list_shares(exclude_names=frozenset({"cp_migration_share"}))

        assert len(shares) == 1
        assert shares[0]["share_name"] == "customer_share_a"


class TestListRecipients:
    def test_excludes_migration_recipients(self):
        auth = MagicMock()

        def _recipient(name):
            r = MagicMock()
            r.name = name
            r.authentication_type = "DATABRICKS"
            r.data_recipient_global_metastore_id = "gm-1"
            r.comment = None
            return r

        auth.source_client.recipients.list.return_value = [
            _recipient("cp_migration_recipient_abc"),
            _recipient("partner_recipient_1"),
        ]

        explorer = _explorer(MagicMock(), auth)
        recipients = explorer.list_recipients()

        assert len(recipients) == 1
        assert recipients[0]["recipient_name"] == "partner_recipient_1"


class TestListProviders:
    def test_returns_providers(self):
        auth = MagicMock()

        p = MagicMock()
        p.name = "vendor_x"
        p.authentication_type = "TOKEN"
        p.comment = None
        auth.source_client.providers.list.return_value = [p]

        explorer = _explorer(MagicMock(), auth)
        providers = explorer.list_providers()
        assert providers[0]["provider_name"] == "vendor_x"
