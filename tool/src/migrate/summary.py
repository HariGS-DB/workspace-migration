# Databricks notebook source

# COMMAND ----------
# Summary: aggregates migration tracking data and prints a final report.

from __future__ import annotations

import logging
from typing import TYPE_CHECKING

from common.config import MigrationConfig
from common.tracking import TrackingManager

if TYPE_CHECKING:
    from pyspark.sql import DataFrame

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("summary")


# COMMAND ----------
# Helpers — importable for unit tests


def aggregate_by_status(df: DataFrame) -> list[dict]:
    """Aggregate migration counts by status."""
    from pyspark.sql.functions import count

    rows = df.groupBy("status").agg(count("*").alias("total")).orderBy("status").collect()
    return [row.asDict() for row in rows]


def aggregate_by_object_type(df: DataFrame) -> list[dict]:
    """Aggregate migration counts by object_type, pivoted by status."""
    from pyspark.sql.functions import col, count, when
    from pyspark.sql.functions import sum as spark_sum

    rows = (
        df.groupBy("object_type")
        .agg(
            count("*").alias("total"),
            spark_sum(when(col("status") == "validated", 1).otherwise(0)).alias("validated"),
            spark_sum(when(col("status") == "failed", 1).otherwise(0)).alias("failed"),
            spark_sum(when(col("status") == "validation_failed", 1).otherwise(0)).alias("validation_failed"),
            spark_sum(when(col("status") == "skipped", 1).otherwise(0)).alias("skipped"),
            spark_sum(when(col("status") == "in_progress", 1).otherwise(0)).alias("in_progress"),
        )
        .orderBy("object_type")
        .collect()
    )
    return [row.asDict() for row in rows]


def get_failed_objects(df: DataFrame) -> list[dict]:
    """Return objects with failed or validation_failed status and their error messages."""
    from pyspark.sql.functions import col

    rows = (
        df.filter(col("status").isin("failed", "validation_failed"))
        .select("object_name", "object_type", "status", "error_message")
        .orderBy("object_type", "object_name")
        .collect()
    )
    return [row.asDict() for row in rows]


def print_status_table(status_rows: list[dict]) -> None:
    """Print a summary table of counts by status."""
    print("\n" + "=" * 60)
    print("  MIGRATION STATUS SUMMARY")
    print("=" * 60)
    print(f"  {'Status':<25} {'Count':>10}")
    print("  " + "-" * 37)
    total = 0
    for row in status_rows:
        print(f"  {row['status']:<25} {row['total']:>10}")
        total += row["total"]
    print("  " + "-" * 37)
    print(f"  {'TOTAL':<25} {total:>10}")
    print("=" * 60)


def print_object_type_table(type_rows: list[dict]) -> None:
    """Print a summary table of counts by object type."""
    print("\n" + "=" * 90)
    print("  MIGRATION BY OBJECT TYPE")
    print("=" * 90)
    header = f"  {'Object Type':<20} {'Total':>7} {'OK':>7} {'Fail':>7} {'ValFail':>7} {'Skip':>7} {'InProg':>7}"
    print(header)
    print("  " + "-" * 64)
    for row in type_rows:
        line = (
            f"  {row['object_type']:<20} {row['total']:>7} {row['validated']:>7} "
            f"{row['failed']:>7} {row['validation_failed']:>7} {row['skipped']:>7} "
            f"{row['in_progress']:>7}"
        )
        print(line)
    print("=" * 90)


def print_failures(failed: list[dict]) -> None:
    """Print details of failed objects."""
    if not failed:
        print("\nNo failures detected.")
        return

    print("\n" + "=" * 90)
    print("  WARNING: FAILED OBJECTS")
    print("=" * 90)
    for obj in failed:
        error = obj.get("error_message") or "Unknown error"
        print(f"  [{obj['status']}] {obj['object_type']}: {obj['object_name']}")
        print(f"           Error: {error}")
    print("=" * 90)
    print(f"\n  WARNING: {len(failed)} object(s) failed. Review errors above.")
    print("  Migration is best-effort for non-critical objects; no exception raised.\n")


# COMMAND ----------
# Notebook execution — guarded so helpers can be imported in tests.


def _is_notebook() -> bool:
    """Return True when running inside a Databricks notebook."""
    try:
        _ = dbutils  # type: ignore[name-defined] # noqa: F821
        return True
    except NameError:
        return False


if _is_notebook():
    config = MigrationConfig.from_job_params(dbutils)  # type: ignore[name-defined] # noqa: F821
    spark_session = spark  # type: ignore[name-defined] # noqa: F821
    tracker = TrackingManager(spark_session, config)

    logger.info("Fetching latest migration status...")
    latest_df = tracker.get_latest_migration_status()

    # Aggregate and print
    status_rows = aggregate_by_status(latest_df)
    type_rows = aggregate_by_object_type(latest_df)
    failed = get_failed_objects(latest_df)

    print_status_table(status_rows)
    print_object_type_table(type_rows)
    print_failures(failed)

    logger.info("Summary complete.")
