from __future__ import annotations

from typing import Any


class Validator:
    """Validates migrated objects between source and target workspaces."""

    def __init__(self, source_explorer: Any, target_explorer: Any) -> None:
        self.source_explorer = source_explorer
        self.target_explorer = target_explorer

    def validate_row_count(self, source_fqn: str, target_fqn: str) -> dict[str, Any]:
        """Compare row counts between source and target tables."""
        source_count = self.source_explorer.get_table_row_count(source_fqn)
        target_count = self.target_explorer.get_table_row_count(target_fqn)
        return {
            "source_count": source_count,
            "target_count": target_count,
            "match": source_count == target_count,
        }

    def validate_schema_match(self, source_fqn: str, target_fqn: str) -> dict[str, Any]:
        """Compare column schemas between source and target tables."""
        source_columns = [
            row.asDict() for row in self.source_explorer.spark.sql(f"DESCRIBE TABLE {source_fqn}").collect()
        ]
        target_columns = [
            row.asDict() for row in self.target_explorer.spark.sql(f"DESCRIBE TABLE {target_fqn}").collect()
        ]

        mismatches: list[dict[str, Any]] = []
        source_map = {col["col_name"]: col for col in source_columns}
        target_map = {col["col_name"]: col for col in target_columns}

        all_names = set(source_map.keys()) | set(target_map.keys())
        for name in sorted(all_names):
            if name not in source_map:
                mismatches.append({"column": name, "issue": "missing_in_source"})
            elif name not in target_map:
                mismatches.append({"column": name, "issue": "missing_in_target"})
            elif source_map[name] != target_map[name]:
                mismatches.append(
                    {
                        "column": name,
                        "issue": "type_mismatch",
                        "source": source_map[name],
                        "target": target_map[name],
                    }
                )

        return {
            "source_columns": source_columns,
            "target_columns": target_columns,
            "match": len(mismatches) == 0,
            "mismatches": mismatches,
        }

    def validate_object_exists(self, target_fqn: str) -> bool:
        """Check whether an object exists in the target workspace."""
        try:
            self.target_explorer.spark.sql(f"DESCRIBE TABLE {target_fqn}").collect()
        except Exception:
            return False
        return True
