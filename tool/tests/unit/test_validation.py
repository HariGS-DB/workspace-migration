from __future__ import annotations

from unittest.mock import MagicMock

import pytest

from common.validation import Validator


@pytest.fixture
def source_explorer():
    explorer = MagicMock()
    return explorer


@pytest.fixture
def target_explorer():
    explorer = MagicMock()
    return explorer


@pytest.fixture
def validator(source_explorer, target_explorer):
    return Validator(source_explorer, target_explorer)


def _describe_row(col_name: str, data_type: str, comment: str = "") -> MagicMock:
    """Create a mock Row that behaves like a DESCRIBE TABLE result row."""
    row = MagicMock()
    row.asDict.return_value = {"col_name": col_name, "data_type": data_type, "comment": comment}
    return row


# --------------------------------------------------------------------------- #
#  validate_row_count
# --------------------------------------------------------------------------- #


class TestValidateRowCount:
    def test_row_count_match(self, validator, source_explorer, target_explorer):
        source_explorer.get_table_row_count.return_value = 100
        target_explorer.get_table_row_count.return_value = 100

        result = validator.validate_row_count("src.db.tbl", "tgt.db.tbl")

        assert result["match"] is True
        assert result["source_count"] == 100
        assert result["target_count"] == 100

    def test_row_count_mismatch(self, validator, source_explorer, target_explorer):
        source_explorer.get_table_row_count.return_value = 100
        target_explorer.get_table_row_count.return_value = 95

        result = validator.validate_row_count("src.db.tbl", "tgt.db.tbl")

        assert result["match"] is False
        assert result["source_count"] == 100
        assert result["target_count"] == 95

    def test_row_count_zero(self, validator, source_explorer, target_explorer):
        source_explorer.get_table_row_count.return_value = 0
        target_explorer.get_table_row_count.return_value = 0

        result = validator.validate_row_count("src.db.tbl", "tgt.db.tbl")

        assert result["match"] is True
        assert result["source_count"] == 0
        assert result["target_count"] == 0


# --------------------------------------------------------------------------- #
#  validate_schema_match
# --------------------------------------------------------------------------- #


class TestValidateSchemaMatch:
    def test_schema_match(self, validator, source_explorer, target_explorer):
        columns = [_describe_row("id", "int"), _describe_row("name", "string")]
        source_explorer.spark.sql.return_value.collect.return_value = columns
        target_explorer.spark.sql.return_value.collect.return_value = columns

        result = validator.validate_schema_match("src.db.tbl", "tgt.db.tbl")

        assert result["match"] is True
        assert result["mismatches"] == []

    def test_schema_missing_in_target(self, validator, source_explorer, target_explorer):
        source_explorer.spark.sql.return_value.collect.return_value = [
            _describe_row("id", "int"),
            _describe_row("extra", "string"),
        ]
        target_explorer.spark.sql.return_value.collect.return_value = [
            _describe_row("id", "int"),
        ]

        result = validator.validate_schema_match("src.db.tbl", "tgt.db.tbl")

        assert result["match"] is False
        assert len(result["mismatches"]) == 1
        assert result["mismatches"][0]["column"] == "extra"
        assert result["mismatches"][0]["issue"] == "missing_in_target"

    def test_schema_missing_in_source(self, validator, source_explorer, target_explorer):
        source_explorer.spark.sql.return_value.collect.return_value = [
            _describe_row("id", "int"),
        ]
        target_explorer.spark.sql.return_value.collect.return_value = [
            _describe_row("id", "int"),
            _describe_row("new_col", "double"),
        ]

        result = validator.validate_schema_match("src.db.tbl", "tgt.db.tbl")

        assert result["match"] is False
        assert len(result["mismatches"]) == 1
        assert result["mismatches"][0]["column"] == "new_col"
        assert result["mismatches"][0]["issue"] == "missing_in_source"

    def test_schema_type_mismatch(self, validator, source_explorer, target_explorer):
        source_explorer.spark.sql.return_value.collect.return_value = [
            _describe_row("id", "int"),
        ]
        target_explorer.spark.sql.return_value.collect.return_value = [
            _describe_row("id", "bigint"),
        ]

        result = validator.validate_schema_match("src.db.tbl", "tgt.db.tbl")

        assert result["match"] is False
        assert len(result["mismatches"]) == 1
        mismatch = result["mismatches"][0]
        assert mismatch["column"] == "id"
        assert mismatch["issue"] == "type_mismatch"
        assert mismatch["source"] == {"col_name": "id", "data_type": "int", "comment": ""}
        assert mismatch["target"] == {"col_name": "id", "data_type": "bigint", "comment": ""}


# --------------------------------------------------------------------------- #
#  validate_object_exists
# --------------------------------------------------------------------------- #


class TestValidateObjectExists:
    def test_object_exists(self, validator, target_explorer):
        target_explorer.spark.sql.return_value.collect.return_value = []

        assert validator.validate_object_exists("tgt.db.tbl") is True

    def test_object_not_exists(self, validator, target_explorer):
        target_explorer.spark.sql.side_effect = Exception("TABLE_OR_VIEW_NOT_FOUND")

        assert validator.validate_object_exists("tgt.db.tbl") is False
