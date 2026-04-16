from __future__ import annotations

from typing import TYPE_CHECKING

from common.config import MigrationConfig

if TYPE_CHECKING:
    from pyspark.sql import DataFrame


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
                catalog_name STRING,
                schema_name STRING,
                row_count LONG,
                size_bytes LONG,
                is_dlt_managed BOOLEAN,
                pipeline_id STRING,
                create_statement STRING,
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

    def write_discovery_inventory(self, df: DataFrame) -> None:
        """Overwrite the discovery inventory table with the given DataFrame."""
        df.write.mode("overwrite").saveAsTable(f"{self._fqn}.discovery_inventory")

    def append_migration_status(self, records: list[dict]) -> None:
        """Append migration status records with a current timestamp."""
        from pyspark.sql.functions import current_timestamp

        df = self.spark.createDataFrame(records)
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
