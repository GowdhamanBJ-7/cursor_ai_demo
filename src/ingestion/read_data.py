"""Read and prepare Bronze layer data."""

from __future__ import annotations

import logging

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import current_timestamp, input_file_name
from pyspark.sql.types import StructType


logger = logging.getLogger(__name__)


def read_raw_data(
    spark: SparkSession,
    source_path: str,
    source_format: str,
    schema: StructType,
) -> DataFrame:
    """Read NYC taxi source files with strict schema enforcement."""
    try:
        logger.info("Bronze read started. path=%s format=%s", source_path, source_format)
        reader = spark.read.format(source_format).schema(schema)

        if source_format == "csv":
            reader = reader.option("header", "true").option("mode", "FAILFAST")

        df = reader.load(source_path)
        logger.info("Bronze read succeeded. record_count=%s", df.count())
        return df
    except Exception as exc:
        logger.exception("Bronze read failed.")
        raise RuntimeError(f"Failed to read source data from {source_path}") from exc


def add_audit_columns(df: DataFrame) -> DataFrame:
    """Add ingestion metadata columns for data lineage and traceability."""
    try:
        logger.info("Adding Bronze audit columns.")
        enriched_df = (
            df.withColumn("ingestion_time", current_timestamp())
            .withColumn("source_file", input_file_name())
        )
        logger.info("Audit columns added successfully.")
        return enriched_df
    except Exception as exc:
        logger.exception("Failed to add Bronze audit columns.")
        raise RuntimeError("Failed to add audit columns to Bronze dataframe.") from exc
