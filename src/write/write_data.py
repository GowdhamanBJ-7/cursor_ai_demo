"""Write helpers for Delta outputs."""

from __future__ import annotations

import logging
from typing import Iterable

from pyspark.sql import DataFrame, SparkSession


logger = logging.getLogger(__name__)


def ensure_catalog_and_schema(spark: SparkSession, catalog: str, schema: str) -> None:
    """Create catalog/schema if they are missing."""
    try:
        logger.info("Ensuring catalog/schema exist. catalog=%s schema=%s", catalog, schema)
        spark.sql(f"CREATE CATALOG IF NOT EXISTS {catalog}")
        spark.sql(f"CREATE SCHEMA IF NOT EXISTS {catalog}.{schema}")
        logger.info("Catalog/schema ready.")
    except Exception as exc:
        logger.exception("Failed creating catalog/schema.")
        raise RuntimeError("Unable to initialize target Unity Catalog objects.") from exc


def write_delta_table(
    df: DataFrame,
    table_fqn: str,
    mode: str = "overwrite",
    partition_cols: Iterable[str] | None = None,
) -> None:
    """Persist DataFrame as a managed Delta table."""
    try:
        logger.info("Write started for table=%s mode=%s", table_fqn, mode)
        writer = df.write.format("delta").mode(mode)
        if partition_cols:
            writer = writer.partitionBy(*partition_cols)
        writer.saveAsTable(table_fqn)
        logger.info("Write succeeded for table=%s", table_fqn)
    except Exception as exc:
        logger.exception("Write failed for table=%s", table_fqn)
        raise RuntimeError(f"Failed writing table {table_fqn}") from exc
