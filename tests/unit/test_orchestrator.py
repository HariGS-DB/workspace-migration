from __future__ import annotations

import json

from migrate.orchestrator import build_batches


class TestOrchestrator:
    """Tests for the orchestrator batching logic."""

    def test_batch_building(self):
        """Verify objects are split into correct batch sizes."""
        objects = [{"object_name": f"table_{i}", "object_type": "managed_table"} for i in range(7)]
        batches = build_batches(objects, batch_size=3)

        assert len(batches) == 3
        batch_0 = json.loads(batches[0])
        batch_1 = json.loads(batches[1])
        batch_2 = json.loads(batches[2])

        assert len(batch_0) == 3
        assert len(batch_1) == 3
        assert len(batch_2) == 1

        # Verify content
        assert batch_0[0]["object_name"] == "table_0"
        assert batch_0[2]["object_name"] == "table_2"
        assert batch_1[0]["object_name"] == "table_3"
        assert batch_2[0]["object_name"] == "table_6"

    def test_batch_building_exact_fit(self):
        """Verify exact multiples produce correct number of batches."""
        objects = [{"object_name": f"t_{i}"} for i in range(6)]
        batches = build_batches(objects, batch_size=3)

        assert len(batches) == 2
        assert len(json.loads(batches[0])) == 3
        assert len(json.loads(batches[1])) == 3

    def test_batch_building_single_batch(self):
        """Verify objects smaller than batch size produce one batch."""
        objects = [{"object_name": f"t_{i}"} for i in range(2)]
        batches = build_batches(objects, batch_size=50)

        assert len(batches) == 1
        assert len(json.loads(batches[0])) == 2

    def test_empty_inventory(self):
        """Verify no batches are created for empty input."""
        batches = build_batches([], batch_size=50)
        assert batches == []

    def test_batch_json_roundtrip(self):
        """Verify batches are valid JSON that can be round-tripped."""
        objects = [
            {"object_name": "`cat`.`sch`.`tbl`", "object_type": "managed_table", "row_count": 100},
            {"object_name": "`cat`.`sch`.`tbl2`", "object_type": "managed_table", "row_count": 200},
        ]
        batches = build_batches(objects, batch_size=10)

        assert len(batches) == 1
        parsed = json.loads(batches[0])
        assert parsed == objects

    def test_batch_strips_create_statement(self):
        """create_statement is stripped to keep task-value payloads under
        Jobs' 3000-byte for_each limit; workers re-query the full row."""
        objects = [
            {
                "object_name": "`cat`.`sch`.`tbl`",
                "object_type": "managed_table",
                "create_statement": "CREATE TABLE ...  (... very long DDL ...)",
                "row_count": 100,
            },
        ]
        batches = build_batches(objects, batch_size=10)

        parsed = json.loads(batches[0])
        assert "create_statement" not in parsed[0]
        # other fields preserved
        assert parsed[0]["object_name"] == "`cat`.`sch`.`tbl`"
        assert parsed[0]["row_count"] == 100
