from unittest.mock import MagicMock

from common.catalog_utils import CatalogExplorer


def _row(**kwargs):
    """Create a MagicMock that behaves like a Spark Row with named attributes."""
    row = MagicMock()
    for k, v in kwargs.items():
        setattr(row, k, v)
    return row


class TestCatalogUtils:
    """Tests for CatalogExplorer."""

    # ------------------------------------------------------------------
    # list_catalogs
    # ------------------------------------------------------------------

    def test_list_catalogs_filters(self, mock_spark):
        mock_spark.sql.return_value.collect.return_value = [
            _row(catalog="catalog_a"),
            _row(catalog="catalog_b"),
            _row(catalog="catalog_c"),
        ]
        explorer = CatalogExplorer(mock_spark, MagicMock())

        result = explorer.list_catalogs(filter_list=["catalog_a", "catalog_c"])
        assert result == ["catalog_a", "catalog_c"]

    def test_list_catalogs_excludes_system(self, mock_spark):
        mock_spark.sql.return_value.collect.return_value = [
            _row(catalog="my_catalog"),
            _row(catalog="system"),
            _row(catalog="hive_metastore"),
            _row(catalog="__databricks_internal"),
            _row(catalog="samples"),
        ]
        explorer = CatalogExplorer(mock_spark, MagicMock())

        result = explorer.list_catalogs()
        assert result == ["my_catalog"]

    def test_list_catalogs_no_filter(self, mock_spark):
        mock_spark.sql.return_value.collect.return_value = [
            _row(catalog="cat_x"),
            _row(catalog="cat_y"),
        ]
        explorer = CatalogExplorer(mock_spark, MagicMock())

        result = explorer.list_catalogs()
        assert result == ["cat_x", "cat_y"]

    # ------------------------------------------------------------------
    # classify_tables
    # ------------------------------------------------------------------

    def test_classify_tables(self, mock_spark):
        mock_spark.sql.return_value.collect.return_value = [
            _row(table_name="orders", table_type="MANAGED", data_source_format="DELTA"),
            _row(table_name="ext_events", table_type="EXTERNAL", data_source_format="PARQUET"),
            _row(table_name="v_summary", table_type="VIEW", data_source_format=None),
        ]
        explorer = CatalogExplorer(mock_spark, MagicMock())

        result = explorer.classify_tables("my_cat", "my_schema")

        assert len(result) == 3
        assert result[0] == {
            "fqn": "`my_cat`.`my_schema`.`orders`",
            "object_type": "managed_table",
            "table_type": "MANAGED",
            "data_source_format": "DELTA",
        }
        assert result[1]["object_type"] == "external_table"
        assert result[2]["object_type"] == "view"

    # ------------------------------------------------------------------
    # detect_dlt_managed
    # ------------------------------------------------------------------

    def test_detect_dlt_managed(self, mock_spark):
        detail_row = _row(properties={"pipelines.pipelineId": "abc-123"})
        mock_spark.sql.return_value.first.return_value = detail_row

        explorer = CatalogExplorer(mock_spark, MagicMock())

        is_dlt, pid = explorer.detect_dlt_managed("`cat`.`sch`.`tbl`")
        assert is_dlt is True
        assert pid == "abc-123"

    def test_detect_dlt_managed_not_dlt(self, mock_spark):
        detail_row = _row(properties={})
        mock_spark.sql.return_value.first.return_value = detail_row

        explorer = CatalogExplorer(mock_spark, MagicMock())

        is_dlt, pid = explorer.detect_dlt_managed("`cat`.`sch`.`tbl`")
        assert is_dlt is False
        assert pid is None

    # ------------------------------------------------------------------
    # resolve_view_dependency_order
    # ------------------------------------------------------------------

    def test_resolve_view_dependency_order(self, mock_spark):
        """v_c depends on v_b, v_b depends on v_a -> order: v_a, v_b, v_c."""
        views = ["`cat`.`sch`.`v_c`", "`cat`.`sch`.`v_b`", "`cat`.`sch`.`v_a`"]

        # Map each view to its dependencies returned by information_schema
        deps = {
            "v_c": [
                _row(
                    view_catalog="cat",
                    view_schema="sch",
                    view_name="v_c",
                    table_catalog="cat",
                    table_schema="sch",
                    table_name="v_b",
                )
            ],
            "v_b": [
                _row(
                    view_catalog="cat",
                    view_schema="sch",
                    view_name="v_b",
                    table_catalog="cat",
                    table_schema="sch",
                    table_name="v_a",
                )
            ],
            "v_a": [],
        }

        def sql_side_effect(query):
            mock_result = MagicMock()
            for view_name, dep_rows in deps.items():
                if f"view_name = '{view_name}'" in query:
                    mock_result.collect.return_value = dep_rows
                    return mock_result
            mock_result.collect.return_value = []
            return mock_result

        mock_spark.sql.side_effect = sql_side_effect

        explorer = CatalogExplorer(mock_spark, MagicMock())
        result = explorer.resolve_view_dependency_order(views)

        # v_a must come before v_b, v_b must come before v_c
        assert result.index("`cat`.`sch`.`v_a`") < result.index("`cat`.`sch`.`v_b`")
        assert result.index("`cat`.`sch`.`v_b`") < result.index("`cat`.`sch`.`v_c`")

    def test_resolve_view_dependency_order_with_cycle(self, mock_spark):
        """v_a -> v_b -> v_a (cycle). All views should still appear in output."""
        views = ["`cat`.`sch`.`v_a`", "`cat`.`sch`.`v_b`"]

        deps = {
            "v_a": [
                _row(
                    view_catalog="cat",
                    view_schema="sch",
                    view_name="v_a",
                    table_catalog="cat",
                    table_schema="sch",
                    table_name="v_b",
                )
            ],
            "v_b": [
                _row(
                    view_catalog="cat",
                    view_schema="sch",
                    view_name="v_b",
                    table_catalog="cat",
                    table_schema="sch",
                    table_name="v_a",
                )
            ],
        }

        def sql_side_effect(query):
            mock_result = MagicMock()
            for view_name, dep_rows in deps.items():
                if f"view_name = '{view_name}'" in query:
                    mock_result.collect.return_value = dep_rows
                    return mock_result
            mock_result.collect.return_value = []
            return mock_result

        mock_spark.sql.side_effect = sql_side_effect

        explorer = CatalogExplorer(mock_spark, MagicMock())
        result = explorer.resolve_view_dependency_order(views)

        # Both views must be present despite the cycle
        assert set(result) == set(views)
        assert len(result) == 2
