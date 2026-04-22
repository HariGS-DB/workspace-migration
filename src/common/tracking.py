from __future__ import annotations

from typing import TYPE_CHECKING

from common.config import MigrationConfig

if TYPE_CHECKING:
    from pyspark.sql import DataFrame, SparkSession
    from pyspark.sql.types import StructType


def discovery_row(
    *,
    source_type: str,
    object_type: str,
    object_name: str,
    catalog_name: str | None,
    schema_name: str | None,
    discovered_at,
    row_count: int = 0,
    size_bytes: int = 0,
    is_dlt_managed: bool | None = None,
    pipeline_id: str | None = None,
    create_statement: str = "",
    data_category: str | None = None,
    table_type: str | None = None,
    provider: str | None = None,
    storage_location: str | None = None,
    format: str | None = None,
    metadata: dict | None = None,
) -> dict:
    """Build a unified discovery_inventory row. metadata is JSON-encoded into metadata_json."""
    import json

    return {
        "object_name": object_name,
        "object_type": object_type,
        "source_type": source_type,
        "catalog_name": catalog_name,
        "schema_name": schema_name,
        "row_count": row_count,
        "size_bytes": size_bytes,
        "is_dlt_managed": is_dlt_managed,
        "pipeline_id": pipeline_id,
        "create_statement": create_statement,
        "data_category": data_category,
        "table_type": table_type,
        "provider": provider,
        "storage_location": storage_location,
        "format": format,
        "metadata_json": json.dumps(metadata) if metadata else None,
        "discovered_at": discovered_at,
    }


def discovery_schema() -> StructType:
    """StructType used when writing to discovery_inventory."""
    from pyspark.sql.types import (
        BooleanType,
        LongType,
        StringType,
        StructField,
        StructType,
        TimestampType,
    )
    return StructType([
        StructField("object_name", StringType(), True),
        StructField("object_type", StringType(), True),
        StructField("source_type", StringType(), True),
        StructField("catalog_name", StringType(), True),
        StructField("schema_name", StringType(), True),
        StructField("row_count", LongType(), True),
        StructField("size_bytes", LongType(), True),
        StructField("is_dlt_managed", BooleanType(), True),
        StructField("pipeline_id", StringType(), True),
        StructField("create_statement", StringType(), True),
        StructField("data_category", StringType(), True),
        StructField("table_type", StringType(), True),
        StructField("provider", StringType(), True),
        StructField("storage_location", StringType(), True),
        StructField("format", StringType(), True),
        StructField("metadata_json", StringType(), True),
        StructField("discovered_at", TimestampType(), True),
    ])


class TrackingManager:
    """Manages migration tracking tables for discovery, status, and pre-checks."""

    def __init__(self, spark: SparkSession, config: MigrationConfig) -> None:
        self.spark = spark
        self.config = config
        self._catalog = config.tracking_catalog
        self._schema = config.tracking_schema

    @property
    def _fqn(self) -> str:
        return f"{self._catalog}.{self._schema}"

    def init_tracking_tables(self) -> None:
        """Create the tracking catalog, schema, and tables if they do not exist."""
        self.spark.sql(f"CREATE CATALOG IF NOT EXISTS {self._catalog}")
        self.spark.sql(f"CREATE SCHEMA IF NOT EXISTS {self._fqn}")

        self.spark.sql(f"""
            CREATE TABLE IF NOT EXISTS {self._fqn}.discovery_inventory (
                object_name STRING,
                object_type STRING,
                source_type STRING,
                catalog_name STRING,
                schema_name STRING,
                row_count LONG,
                size_bytes LONG,
                is_dlt_managed BOOLEAN,
                pipeline_id STRING,
                create_statement STRING,
                data_category STRING,
                table_type STRING,
                provider STRING,
                storage_location STRING,
                format STRING,
                metadata_json STRING,
                discovered_at TIMESTAMP
            ) USING DELTA
        """)

        self.spark.sql(f"""
            CREATE TABLE IF NOT EXISTS {self._fqn}.migration_status (
                object_name STRING,
                object_type STRING,
                status STRING,
                error_message STRING,
                job_run_id STRING,
                task_run_id STRING,
                source_row_count LONG,
                target_row_count LONG,
                duration_seconds DOUBLE,
                migrated_at TIMESTAMP
            ) USING DELTA
        """)

        self.spark.sql(f"""
            CREATE TABLE IF NOT EXISTS {self._fqn}.pre_check_results (
                check_name STRING,
                status STRING,
                message STRING,
                action_required STRING,
                checked_at TIMESTAMP
            ) USING DELTA
        """)

        # Phase 3 P.1: RLS / CM drop_and_restore manifest. One row per
        # (table, policy_kind, target_col_or_null) pair stripped from
        # source during setup_sharing; stamped with restored_at when
        # re-applied post-migration. Null restored_at => unfinished
        # restoration — the restore worker will re-run.
        self.spark.sql(f"""
            CREATE TABLE IF NOT EXISTS {self._fqn}.rls_cm_manifest (
                table_fqn STRING,
                policy_kind STRING,
                target_column STRING,
                function_fqn STRING,
                using_columns STRING,
                stripped_at TIMESTAMP,
                restored_at TIMESTAMP
            ) USING DELTA
        """)

    def write_discovery_inventory(self, df: DataFrame) -> None:
        """Overwrite the discovery inventory table with the given DataFrame."""
        df.write.mode("overwrite").option("mergeSchema", "true").saveAsTable(
            f"{self._fqn}.discovery_inventory"
        )

    def append_migration_status(self, records: list[dict]) -> None:
        """Append migration status records with a current timestamp."""
        from pyspark.sql.functions import current_timestamp
        from pyspark.sql.types import DoubleType, LongType, StringType, StructField, StructType

        schema = StructType([
            StructField("object_name", StringType(), True),
            StructField("object_type", StringType(), True),
            StructField("status", StringType(), True),
            StructField("error_message", StringType(), True),
            StructField("job_run_id", StringType(), True),
            StructField("task_run_id", StringType(), True),
            StructField("source_row_count", LongType(), True),
            StructField("target_row_count", LongType(), True),
            StructField("duration_seconds", DoubleType(), True),
        ])
        # Only keep known fields; coerce missing to None
        field_names = [f.name for f in schema.fields]
        normalized = [{k: r.get(k) for k in field_names} for r in records]
        df = self.spark.createDataFrame(normalized, schema=schema)
        df = df.withColumn("migrated_at", current_timestamp())
        df.write.mode("append").saveAsTable(f"{self._fqn}.migration_status")

    def append_pre_check_results(self, records: list[dict]) -> None:
        """Append pre-check result records with a current timestamp."""
        from pyspark.sql.functions import current_timestamp

        df = self.spark.createDataFrame(records)
        df = df.withColumn("checked_at", current_timestamp())
        df.write.mode("append").saveAsTable(f"{self._fqn}.pre_check_results")

    def get_latest_migration_status(self) -> DataFrame:
        """Return the latest migration status per object (by object_name + object_type)."""
        return self.spark.sql(f"""
            SELECT *
            FROM (
                SELECT *,
                       ROW_NUMBER() OVER (
                           PARTITION BY object_name, object_type
                           ORDER BY migrated_at DESC
                       ) AS rn
                FROM {self._fqn}.migration_status
            )
            WHERE rn = 1
        """)

    # ------------------------------------------------------------------
    # RLS / CM manifest (Phase 3 P.1)
    # ------------------------------------------------------------------

    def init_rls_cm_manifest(self) -> None:
        """Ensure the rls_cm_manifest table exists — idempotent helper so
        setup_sharing can call this without running the full tracking init.
        """
        self.spark.sql(f"CREATE CATALOG IF NOT EXISTS {self._catalog}")
        self.spark.sql(f"CREATE SCHEMA IF NOT EXISTS {self._fqn}")
        self.spark.sql(f"""
            CREATE TABLE IF NOT EXISTS {self._fqn}.rls_cm_manifest (
                table_fqn STRING,
                policy_kind STRING,
                target_column STRING,
                function_fqn STRING,
                using_columns STRING,
                stripped_at TIMESTAMP,
                restored_at TIMESTAMP
            ) USING DELTA
        """)

    def append_rls_cm_manifest(self, records: list[dict]) -> None:
        """Append manifest rows with stripped_at=now, restored_at=NULL."""
        from pyspark.sql.functions import current_timestamp, lit
        from pyspark.sql.types import StringType, StructField, StructType

        schema = StructType([
            StructField("table_fqn", StringType(), True),
            StructField("policy_kind", StringType(), True),
            StructField("target_column", StringType(), True),
            StructField("function_fqn", StringType(), True),
            StructField("using_columns", StringType(), True),
        ])
        field_names = [f.name for f in schema.fields]
        normalized = [{k: r.get(k) for k in field_names} for r in records]
        df = self.spark.createDataFrame(normalized, schema=schema)
        df = df.withColumn("stripped_at", current_timestamp())
        df = df.withColumn("restored_at", lit(None).cast("timestamp"))
        df.write.mode("append").saveAsTable(f"{self._fqn}.rls_cm_manifest")

    def get_unrestored_manifest(self) -> list[dict]:
        """Return manifest rows where restored_at IS NULL (oldest first)."""
        rows = self.spark.sql(f"""
            SELECT table_fqn, policy_kind, target_column,
                   function_fqn, using_columns, stripped_at
            FROM {self._fqn}.rls_cm_manifest
            WHERE restored_at IS NULL
            ORDER BY stripped_at ASC
        """).collect()
        return [row.asDict() for row in rows]

    def stamp_manifest_restored(
        self, table_fqn: str, policy_kind: str, target_column: str | None,
    ) -> None:
        """Mark a manifest row restored_at=now. Matches by composite key."""
        col_clause = (
            f"target_column = '{target_column}'"
            if target_column is not None
            else "target_column IS NULL"
        )
        self.spark.sql(f"""
            UPDATE {self._fqn}.rls_cm_manifest
            SET restored_at = current_timestamp()
            WHERE table_fqn = '{table_fqn}'
              AND policy_kind = '{policy_kind}'
              AND {col_clause}
              AND restored_at IS NULL
        """)

    def get_pending_objects(self, object_type: str) -> list[dict]:
        """Return discovery inventory objects that have not been validated or skipped."""
        rows = self.spark.sql(f"""
            WITH latest_status AS (
                SELECT *
                FROM (
                    SELECT *,
                           ROW_NUMBER() OVER (
                               PARTITION BY object_name, object_type
                               ORDER BY migrated_at DESC
                           ) AS rn
                    FROM {self._fqn}.migration_status
                )
                WHERE rn = 1
            )
            SELECT d.*
            FROM {self._fqn}.discovery_inventory d
            LEFT JOIN latest_status s
                ON d.object_name = s.object_name AND d.object_type = s.object_type
            WHERE d.object_type = '{object_type}'
              AND (s.status IS NULL OR s.status NOT IN ('validated', 'skipped'))
        """).collect()
        return [row.asDict() for row in rows]
