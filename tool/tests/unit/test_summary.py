from __future__ import annotations

from unittest.mock import MagicMock

from migrate.summary import (
    aggregate_by_status,
    get_failed_objects,
    print_failures,
    print_object_type_table,
    print_status_table,
)

# --------------------------------------------------------------------------- #
#  print_status_table
# --------------------------------------------------------------------------- #


class TestPrintStatusTable:
    def test_print_status_table(self, capsys):
        rows = [
            {"status": "validated", "total": 5},
            {"status": "failed", "total": 2},
        ]

        print_status_table(rows)

        captured = capsys.readouterr().out
        assert "validated" in captured
        assert "5" in captured
        assert "failed" in captured
        assert "2" in captured
        assert "TOTAL" in captured
        assert "7" in captured


# --------------------------------------------------------------------------- #
#  print_object_type_table
# --------------------------------------------------------------------------- #


class TestPrintObjectTypeTable:
    def test_print_object_type_table(self, capsys):
        rows = [
            {
                "object_type": "managed_table",
                "total": 10,
                "validated": 8,
                "failed": 1,
                "validation_failed": 0,
                "skipped": 1,
                "in_progress": 0,
            },
        ]

        print_object_type_table(rows)

        captured = capsys.readouterr().out
        assert "managed_table" in captured
        assert "10" in captured
        assert "8" in captured
        assert "OBJECT TYPE" in captured.upper()


# --------------------------------------------------------------------------- #
#  print_failures
# --------------------------------------------------------------------------- #


class TestPrintFailures:
    def test_print_failures_empty(self, capsys):
        print_failures([])

        captured = capsys.readouterr().out
        assert "No failures detected" in captured

    def test_print_failures_with_items(self, capsys):
        failed = [
            {
                "object_name": "catalog.schema.bad_table",
                "object_type": "managed_table",
                "status": "failed",
                "error_message": "Permission denied",
            },
            {
                "object_name": "catalog.schema.bad_view",
                "object_type": "view",
                "status": "validation_failed",
                "error_message": "Row count mismatch",
            },
        ]

        print_failures(failed)

        captured = capsys.readouterr().out
        assert "catalog.schema.bad_table" in captured
        assert "Permission denied" in captured
        assert "catalog.schema.bad_view" in captured
        assert "Row count mismatch" in captured
        assert "2 object(s) failed" in captured


# --------------------------------------------------------------------------- #
#  aggregate_by_status
# --------------------------------------------------------------------------- #


class TestAggregateByStatus:
    def test_aggregate_by_status(self):
        mock_df = MagicMock()
        mock_row = MagicMock()
        mock_row.asDict.return_value = {"status": "validated", "total": 5}
        mock_df.groupBy.return_value.agg.return_value.orderBy.return_value.collect.return_value = [mock_row]

        result = aggregate_by_status(mock_df)

        assert len(result) == 1
        assert result[0] == {"status": "validated", "total": 5}
        mock_df.groupBy.assert_called_once_with("status")


# --------------------------------------------------------------------------- #
#  get_failed_objects
# --------------------------------------------------------------------------- #


class TestGetFailedObjects:
    def test_get_failed_objects(self):
        mock_df = MagicMock()
        mock_row = MagicMock()
        mock_row.asDict.return_value = {
            "object_name": "catalog.schema.tbl",
            "object_type": "managed_table",
            "status": "failed",
            "error_message": "timeout",
        }
        mock_df.filter.return_value.select.return_value.orderBy.return_value.collect.return_value = [mock_row]

        result = get_failed_objects(mock_df)

        assert len(result) == 1
        assert result[0]["object_name"] == "catalog.schema.tbl"
        assert result[0]["error_message"] == "timeout"
        mock_df.filter.assert_called_once()
