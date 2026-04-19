from __future__ import annotations

from collections import defaultdict, deque

from common.auth import AuthManager

SYSTEM_CATALOGS = frozenset({"system", "hive_metastore", "__databricks_internal", "samples"})
EXCLUDED_SCHEMAS = frozenset({"default", "information_schema"})

HIVE_CATALOG = "hive_metastore"

# Categories returned by CatalogExplorer.categorize_hive_table — drive which worker
# handles each table in Phase 2.
HIVE_CATEGORIES = frozenset({
    "hive_view",
    "hive_external",
    "hive_managed_dbfs_root",
    "hive_managed_nondbfs",
})

_TABLE_TYPE_MAP = {
    "MANAGED": "managed_table",
    "EXTERNAL": "external_table",
    "VIEW": "view",
}


class CatalogExplorer:
    """Utility for discovering and classifying Unity Catalog objects."""

    def __init__(self, spark: object, auth_manager: AuthManager) -> None:
        self.spark = spark
        self.auth_manager = auth_manager

    # ------------------------------------------------------------------
    # Catalogs & schemas
    # ------------------------------------------------------------------

    def list_catalogs(self, filter_list: list[str] | None = None) -> list[str]:
        """Return non-system catalogs, optionally filtered by *filter_list*."""
        rows = self.spark.sql("SHOW CATALOGS").collect()  # type: ignore[attr-defined]
        catalogs = [row.catalog for row in rows if row.catalog not in SYSTEM_CATALOGS]
        if filter_list:
            allowed = set(filter_list)
            catalogs = [c for c in catalogs if c in allowed]
        return catalogs

    def list_schemas(self, catalog: str) -> list[str]:
        """Return non-default schemas within *catalog*."""
        rows = self.spark.sql(f"SHOW SCHEMAS IN `{catalog}`").collect()  # type: ignore[attr-defined]
        return [row.databaseName for row in rows if row.databaseName not in EXCLUDED_SCHEMAS]

    # ------------------------------------------------------------------
    # Tables / views
    # ------------------------------------------------------------------

    def classify_tables(self, catalog: str, schema: str) -> list[dict]:
        """Query information_schema.tables and classify each object."""
        query = (
            f"SELECT table_name, table_type, data_source_format "
            f"FROM `{catalog}`.`information_schema`.`tables` "
            f"WHERE table_schema = '{schema}'"
        )
        rows = self.spark.sql(query).collect()  # type: ignore[attr-defined]
        results: list[dict] = []
        for row in rows:
            object_type = _TABLE_TYPE_MAP.get(row.table_type, row.table_type)
            results.append(
                {
                    "fqn": f"`{catalog}`.`{schema}`.`{row.table_name}`",
                    "object_type": object_type,
                    "table_type": row.table_type,
                    "data_source_format": row.data_source_format,
                }
            )
        return results

    def detect_dlt_managed(self, table_fqn: str) -> tuple[bool, str | None]:
        """Check whether a table is managed by a DLT pipeline. Safe to call on views (returns False)."""
        try:
            row = self.spark.sql(f"DESCRIBE DETAIL {table_fqn}").first()  # type: ignore[attr-defined]
        except Exception:
            return (False, None)
        properties: dict = row.properties if row.properties else {}
        pipeline_id = properties.get("pipelines.pipelineId")
        return (pipeline_id is not None, pipeline_id)

    def get_table_row_count(self, table_fqn: str) -> int:
        """Return the row count of a table."""
        row = self.spark.sql(f"SELECT COUNT(*) as cnt FROM {table_fqn}").first()  # type: ignore[attr-defined]
        return row.cnt  # type: ignore[union-attr]

    def get_table_size_bytes(self, table_fqn: str) -> int:
        """Return the sizeInBytes from DESCRIBE DETAIL."""
        row = self.spark.sql(f"DESCRIBE DETAIL {table_fqn}").first()  # type: ignore[attr-defined]
        return row.sizeInBytes  # type: ignore[union-attr]

    def get_table_format(self, table_fqn: str) -> str | None:
        """Return the table format from DESCRIBE DETAIL (e.g. 'delta', 'iceberg').

        Returns None on any error — callers treat a missing format as delta
        for backward compatibility with pre-Iceberg-support tools.
        """
        try:
            row = self.spark.sql(f"DESCRIBE DETAIL {table_fqn}").first()  # type: ignore[attr-defined]
        except Exception:  # noqa: BLE001
            return None
        fmt = getattr(row, "format", None)
        return fmt.lower() if isinstance(fmt, str) else None

    # ------------------------------------------------------------------
    # DDL / create statements
    # ------------------------------------------------------------------

    def get_create_statement(self, object_fqn: str) -> str:
        """Return a CREATE OR REPLACE VIEW/TABLE statement for the given object.

        For views, prefer information_schema.views (view_definition) so we get
        the original SQL body without any SHOW CREATE quirks.
        """
        parts = object_fqn.strip("`").split("`.`")
        if len(parts) == 3:
            catalog, schema, name = parts
            try:
                row = self.spark.sql(  # type: ignore[attr-defined]
                    f"""
                    SELECT view_definition
                    FROM `{catalog}`.`information_schema`.`views`
                    WHERE table_schema = '{schema}' AND table_name = '{name}'
                    LIMIT 1
                    """
                ).first()
                if row is not None and row.view_definition:
                    return f"CREATE OR REPLACE VIEW `{catalog}`.`{schema}`.`{name}` AS {row.view_definition}"
            except Exception:  # noqa: BLE001
                pass  # fall through to SHOW CREATE
        row = self.spark.sql(f"SHOW CREATE TABLE {object_fqn}").first()  # type: ignore[attr-defined]
        return row.createtab_stmt  # type: ignore[union-attr]

    def get_function_ddl(self, function_fqn: str) -> str:
        """Return the full CREATE OR REPLACE FUNCTION statement.

        Queries information_schema.routines + parameters to reconstruct the DDL,
        since DESCRIBE FUNCTION only returns the body.
        """
        parts = function_fqn.strip("`").split("`.`")
        if len(parts) != 3:
            raise ValueError(f"Malformed function FQN: {function_fqn}")
        catalog, schema, name = parts

        routine = self.spark.sql(  # type: ignore[attr-defined]
            f"""
            SELECT specific_name, data_type, routine_body, routine_definition, external_language
            FROM `{catalog}`.`information_schema`.`routines`
            WHERE routine_schema = '{schema}' AND routine_name = '{name}'
            LIMIT 1
            """
        ).first()
        if routine is None:
            raise ValueError(f"Function not found in information_schema: {function_fqn}")

        params = self.spark.sql(  # type: ignore[attr-defined]
            f"""
            SELECT parameter_name, data_type, ordinal_position
            FROM `{catalog}`.`information_schema`.`parameters`
            WHERE specific_schema = '{schema}' AND specific_name = '{routine.specific_name}'
              AND parameter_mode = 'IN'
            ORDER BY ordinal_position
            """
        ).collect()
        param_sig = ", ".join(f"{p.parameter_name} {p.data_type}" for p in params)

        body = (routine.routine_definition or "").strip()
        lang_clause = ""
        if routine.routine_body and routine.routine_body.upper() == "EXTERNAL" and routine.external_language:
            lang_clause = f" LANGUAGE {routine.external_language}"

        return (
            f"CREATE OR REPLACE FUNCTION {function_fqn}({param_sig}) "
            f"RETURNS {routine.data_type}{lang_clause} "
            f"RETURN {body}"
        )

    # ------------------------------------------------------------------
    # Functions & volumes
    # ------------------------------------------------------------------

    def list_functions(self, catalog: str, schema: str) -> list[str]:
        """List UDFs in a schema via information_schema.routines."""
        query = (
            f"SELECT routine_name FROM `{catalog}`.`information_schema`.`routines` WHERE routine_schema = '{schema}'"
        )
        rows = self.spark.sql(query).collect()  # type: ignore[attr-defined]
        return [f"`{catalog}`.`{schema}`.`{row.routine_name}`" for row in rows]

    def list_volumes(self, catalog: str, schema: str) -> list[dict]:
        """List volumes in a schema via information_schema.volumes."""
        query = (
            f"SELECT volume_name, volume_type "
            f"FROM `{catalog}`.`information_schema`.`volumes` "
            f"WHERE volume_schema = '{schema}'"
        )
        rows = self.spark.sql(query).collect()  # type: ignore[attr-defined]
        return [
            {
                "fqn": f"`{catalog}`.`{schema}`.`{row.volume_name}`",
                "volume_type": row.volume_type,
            }
            for row in rows
        ]

    # ------------------------------------------------------------------
    # View dependency ordering
    # ------------------------------------------------------------------

    def resolve_view_dependency_order(self, views: list[str]) -> list[str]:
        """Topological sort of views using Kahn's algorithm.

        Dependencies come from ``information_schema.view_table_usage``.
        If cycles are detected, remaining views are appended at the end.
        """
        view_set = set(views)

        # Build adjacency: edge from dependency -> view (dependency must come first)
        in_degree: dict[str, int] = {v: 0 for v in views}
        dependents: dict[str, list[str]] = defaultdict(list)

        for view in views:
            parts = view.strip("`").split("`.`")
            if len(parts) != 3:
                continue
            catalog, schema, _name = parts
            query = (
                f"SELECT view_catalog, view_schema, view_name, "
                f"table_catalog, table_schema, table_name "
                f"FROM `{catalog}`.`information_schema`.`view_table_usage` "
                f"WHERE view_catalog = '{catalog}' "
                f"AND view_schema = '{schema}' "
                f"AND view_name = '{_name}'"
            )
            try:
                rows = self.spark.sql(query).collect()  # type: ignore[attr-defined]
            except Exception:
                # information_schema.view_table_usage may be unavailable (e.g. shared catalog).
                # Skip dependency resolution for this view; it will be migrated in input order.
                rows = []
            for row in rows:
                dep_fqn = f"`{row.table_catalog}`.`{row.table_schema}`.`{row.table_name}`"
                if dep_fqn in view_set and dep_fqn != view:
                    dependents[dep_fqn].append(view)
                    in_degree[view] += 1

        # Kahn's algorithm
        queue: deque[str] = deque(v for v in views if in_degree[v] == 0)
        ordered: list[str] = []

        while queue:
            node = queue.popleft()
            ordered.append(node)
            for dependent in dependents.get(node, []):
                in_degree[dependent] -= 1
                if in_degree[dependent] == 0:
                    queue.append(dependent)

        # Cycle handling – append any remaining views that weren't resolved
        if len(ordered) < len(views):
            remaining = [v for v in views if v not in set(ordered)]
            ordered.extend(remaining)

        return ordered

    # ------------------------------------------------------------------
    # Grants
    # ------------------------------------------------------------------

    def list_grants(self, securable_type: str, securable_fqn: str) -> list[dict]:
        """Return grants on a securable object."""
        rows = self.spark.sql(  # type: ignore[attr-defined]
            f"SHOW GRANTS ON {securable_type} {securable_fqn}"
        ).collect()
        return [
            {
                "principal": row.Principal,
                "action_type": row.ActionType,
                "securable_type": row.ObjectType,
                "securable_fqn": row.ObjectKey,
            }
            for row in rows
        ]

    # ------------------------------------------------------------------
    # Legacy Hive Metastore (Phase 2)
    # ------------------------------------------------------------------

    def list_hive_databases(self) -> list[str]:
        """Return non-default databases under hive_metastore."""
        rows = self.spark.sql(f"SHOW DATABASES IN {HIVE_CATALOG}").collect()  # type: ignore[attr-defined]
        # The first column can be `databaseName` or `namespace` depending on DBR.
        return [
            getattr(row, "databaseName", None) or getattr(row, "namespace", None)
            for row in rows
            if (getattr(row, "databaseName", None) or getattr(row, "namespace", None)) not in EXCLUDED_SCHEMAS
        ]

    def _describe_hive_table(self, database: str, table: str) -> dict:
        """Parse ``DESCRIBE EXTENDED hive_metastore.db.t`` into a summary dict.

        Returns a dict with: ``table_type`` (MANAGED/EXTERNAL/VIEW),
        ``storage_location`` (raw), ``provider`` (delta/parquet/...),
        ``comment`` if present.
        """
        rows = self.spark.sql(  # type: ignore[attr-defined]
            f"DESCRIBE EXTENDED `{HIVE_CATALOG}`.`{database}`.`{table}`"
        ).collect()
        info: dict = {}
        in_detail = False
        for row in rows:
            col = (row.col_name or "").strip()
            data = (row.data_type or "").strip() if row.data_type is not None else ""
            if col == "# Detailed Table Information":
                in_detail = True
                continue
            if not in_detail:
                continue
            key = col.rstrip(":").lower().replace(" ", "_")
            if not key:
                continue
            info[key] = data
        return {
            "table_type": info.get("type", "").upper(),
            "storage_location": info.get("location", ""),
            "provider": (info.get("provider") or "").lower(),
            "comment": info.get("comment", ""),
        }

    def classify_hive_tables(self, database: str) -> list[dict]:
        """List all tables in a Hive database and classify each into a migration category.

        Returns records with: ``fqn``, ``object_type`` (hive_table / hive_view),
        ``table_type``, ``storage_location``, ``provider``, ``data_category``
        (hive_managed_dbfs_root / hive_managed_nondbfs / hive_external / hive_view).
        """
        rows = self.spark.sql(  # type: ignore[attr-defined]
            f"SHOW TABLES IN `{HIVE_CATALOG}`.`{database}`"
        ).collect()
        results: list[dict] = []
        for row in rows:
            table_name = row.tableName
            try:
                details = self._describe_hive_table(database, table_name)
            except Exception:  # noqa: BLE001
                # Some tables (e.g. temp views from other sessions) may fail to describe.
                continue
            fqn = f"`{HIVE_CATALOG}`.`{database}`.`{table_name}`"
            data_category = self.categorize_hive_table(details)
            obj_type = "hive_view" if details["table_type"] == "VIEW" else "hive_table"
            results.append({
                "fqn": fqn,
                "object_type": obj_type,
                "table_type": details["table_type"],
                "storage_location": details["storage_location"],
                "provider": details["provider"],
                "data_category": data_category,
            })
        return results

    @staticmethod
    def categorize_hive_table(details: dict) -> str:
        """Map DESCRIBE output to a migration category.

        Categories drive which Phase 2 worker handles the table.
        """
        table_type = (details.get("table_type") or "").upper()
        location = (details.get("storage_location") or "").lower()

        if table_type == "VIEW":
            return "hive_view"
        if table_type == "EXTERNAL":
            return "hive_external"
        if table_type == "MANAGED":
            # DBFS root = /user/hive/warehouse/... or /user/... under dbfs:/
            if location.startswith("dbfs:/user/hive/warehouse/") or location.startswith("dbfs:/user/spark-warehouse/"):
                return "hive_managed_dbfs_root"
            if location.startswith("dbfs:/mnt/") or location.startswith("abfss://") or location.startswith("s3://") or location.startswith("gs://") or location.startswith("wasbs://"):
                return "hive_managed_nondbfs"
            # Fallback: unknown dbfs path, treat as dbfs_root (safer — triggers data copy)
            if location.startswith("dbfs:/"):
                return "hive_managed_dbfs_root"
            return "hive_managed_nondbfs"
        # Unknown type (shouldn't happen for Hive) — classify as external to be safe.
        return "hive_external"

    def list_hive_functions(self, database: str) -> list[str]:
        """Return user-defined functions in a Hive database (fully-qualified)."""
        try:
            rows = self.spark.sql(  # type: ignore[attr-defined]
                f"SHOW USER FUNCTIONS IN `{HIVE_CATALOG}`.`{database}`"
            ).collect()
        except Exception:  # noqa: BLE001
            return []
        out = []
        for row in rows:
            # `function` column name varies by DBR version — probe common ones.
            name = getattr(row, "function", None) or getattr(row, "name", None)
            if not name:
                continue
            # Skip built-ins that leak into SHOW USER FUNCTIONS on some versions.
            if "." not in name:
                continue
            out.append(f"`{HIVE_CATALOG}`.`{database}`.`{name.split('.')[-1]}`")
        return out
