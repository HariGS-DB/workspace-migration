from __future__ import annotations

import json
import logging

from migrate.batching import MAX_BATCH_BYTES, build_batches


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


class TestBuildBatchesByteCap:
    """Byte-size ceiling is enforced alongside ``batch_size`` so the
    Databricks Jobs for_each 3000-byte per-parameter limit is never hit."""

    @staticmethod
    def _obj(i: int, storage_location_len: int = 200) -> dict:
        return {
            "object_name": f"`catalog_long_name`.`schema_long_name`.`object_name_{i:04d}`",
            "object_type": "hive_managed_dbfs_root",
            "catalog_name": "catalog_long_name",
            "schema_name": "schema_long_name",
            "data_category": "hive_managed_dbfs_root",
            "table_type": "MANAGED",
            "provider": "delta",
            "storage_location": "x" * storage_location_len,
        }

    def test_byte_cap_splits_before_count_cap(self):
        """Each object ≈ 300 bytes. With ``batch_size=50`` the count cap
        wouldn't trigger, but 10+ such objects would blow past
        ``MAX_BATCH_BYTES``; the orchestrator must close the batch early."""
        objects = [self._obj(i) for i in range(20)]
        batches = build_batches(objects, batch_size=50)

        assert len(batches) > 1, "Byte cap should have forced multiple batches."
        for b in batches:
            assert len(b.encode("utf-8")) <= MAX_BATCH_BYTES, (
                f"batch size {len(b.encode('utf-8'))} exceeds MAX_BATCH_BYTES={MAX_BATCH_BYTES}"
            )

        # Round-trip: every object must appear exactly once across all batches.
        flattened = [o for b in batches for o in json.loads(b)]
        assert len(flattened) == len(objects)
        assert {o["object_name"] for o in flattened} == {o["object_name"] for o in objects}

    def test_count_cap_still_wins_when_under_byte_cap(self):
        """If objects are tiny, count cap is the binding constraint."""
        objects = [{"object_name": f"t_{i}"} for i in range(10)]
        batches = build_batches(objects, batch_size=3)

        assert len(batches) == 4  # 3 + 3 + 3 + 1
        for b in batches:
            assert len(json.loads(b)) <= 3

    def test_single_huge_object_emits_warning_but_is_batched(self, caplog):
        """An object whose own JSON is > ``MAX_BATCH_BYTES`` is still emitted
        (alone) — dropping it silently would be worse than an operator-
        visible for_each failure with the warning pointing at the culprit."""
        huge = {
            "object_name": "`cat`.`sch`.`huge_object`",
            "storage_location": "x" * (MAX_BATCH_BYTES + 1000),
        }
        with caplog.at_level(logging.WARNING, logger="orchestrator"):
            batches = build_batches([huge], batch_size=50)

        assert len(batches) == 1
        parsed = json.loads(batches[0])
        assert parsed[0]["object_name"] == "`cat`.`sch`.`huge_object`"
        assert any("Single object encoded to" in r.message for r in caplog.records)
        assert any("huge_object" in r.message for r in caplog.records)

    def test_huge_object_is_flushed_into_its_own_batch(self):
        """A huge object after normal ones must close the prior batch and
        land alone in a new batch — not be appended past the byte cap."""
        tiny = [{"object_name": f"t_{i}"} for i in range(3)]
        huge = {"object_name": "huge", "payload": "x" * (MAX_BATCH_BYTES + 500)}
        objects = tiny + [huge]
        batches = build_batches(objects, batch_size=50)

        # 1 batch of 3 tiny + 1 batch of huge alone.
        parsed = [json.loads(b) for b in batches]
        assert len(parsed) == 2
        assert {o["object_name"] for o in parsed[0]} == {"t_0", "t_1", "t_2"}
        assert parsed[1][0]["object_name"] == "huge"
