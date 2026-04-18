"""Test catalog operations against real Unity Catalog via SQL connector."""

from __future__ import annotations


class TestCatalogExplorerConnect:
    def test_list_catalogs(self, sql):
        rows = sql("SHOW CATALOGS")
        catalogs = [r["catalog"] for r in rows]

        assert len(catalogs) > 0
        assert "connect_test_src" in catalogs

    def test_list_catalogs_excludes_system(self, sql):
        """Verify our CatalogExplorer exclusion logic is correct."""
        rows = sql("SHOW CATALOGS")
        catalogs = [r["catalog"] for r in rows]

        system_catalogs = {"system", "hive_metastore", "__databricks_internal", "samples"}
        filtered = [c for c in catalogs if c not in system_catalogs]

        assert "connect_test_src" in filtered
        assert "system" not in filtered

    def test_list_schemas(self, sql, test_catalog):
        rows = sql(f"SHOW SCHEMAS IN {test_catalog}")
        schemas = [r["databaseName"] for r in rows]

        assert "test_schema" in schemas

    def test_classify_tables(self, sql, test_catalog, test_schema):
        rows = sql(f"""
            SELECT table_name, table_type, data_source_format
            FROM {test_catalog}.information_schema.tables
            WHERE table_schema = '{test_schema}'
        """)

        table_map = {r["table_name"]: r["table_type"] for r in rows}

        assert "orders" in table_map
        assert table_map["orders"] == "MANAGED"
        assert "high_value_orders" in table_map
        assert table_map["high_value_orders"] == "VIEW"

    def test_get_table_row_count(self, sql, test_catalog, test_schema):
        rows = sql(f"SELECT COUNT(*) AS cnt FROM {test_catalog}.{test_schema}.orders")

        assert rows[0]["cnt"] == 3

    def test_get_create_statement(self, sql, test_catalog, test_schema):
        rows = sql(f"SHOW CREATE TABLE {test_catalog}.{test_schema}.orders")

        ddl = rows[0]["createtab_stmt"]
        assert "CREATE TABLE" in ddl
        assert "orders" in ddl

    def test_list_functions(self, sql, test_catalog, test_schema):
        rows = sql(f"""
            SELECT routine_name
            FROM {test_catalog}.information_schema.routines
            WHERE routine_schema = '{test_schema}'
        """)

        func_names = [r["routine_name"] for r in rows]
        assert "double_it" in func_names

    def test_function_works(self, sql, test_catalog, test_schema):
        rows = sql(f"SELECT {test_catalog}.{test_schema}.double_it(5.0) AS result")

        assert rows[0]["result"] == 10.0

    def test_view_returns_correct_data(self, sql, test_catalog, test_schema):
        rows = sql(f"SELECT * FROM {test_catalog}.{test_schema}.high_value_orders")

        assert len(rows) == 1
        assert rows[0]["amount"] == 149.99
