"""Read and prepare Bronze layer data."""

from __future__ import annotations

import logging

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import current_timestamp, input_file_name, col, lit
from pyspark.sql.types import StructType


logger = logging.getLogger(__name__)


def _resolve_source_format(source_path: str, source_format: str) -> str:
    """Resolve effective source format for mixed-file folders."""
    if source_format != "auto":
        return source_format

    lowered = source_path.lower()
    if lowered.endswith(".parquet") or "parquet" in lowered:
        return "parquet"
    return "csv"


def read_raw_data(
    spark: SparkSession,
    source_path: str,
    source_format: str,
    schema: StructType,
) -> DataFrame:
    """Read NYC taxi source files with strict schema enforcement."""
    try:
        effective_format = _resolve_source_format(source_path, source_format)
        logger.info(
            "Bronze read started. path=%s format=%s (requested=%s)",
            source_path,
            effective_format,
            source_format,
        )
        reader = (
            spark.read.format(effective_format)
            .schema(schema)
            .option("recursiveFileLookup", "true")
            .option("ignoreCorruptFiles", "true")
        )

        if effective_format == "csv":
            reader = (
                reader.option("header", "true")
                .option("mode", "PERMISSIVE")
                .option("pathGlobFilter", "*.csv*")
            )
        else:
            reader = reader.option("pathGlobFilter", "*.parquet")

        df = reader.load(source_path)
        logger.info("Bronze read succeeded. columns=%s", ", ".join(df.columns))
        return df
    except Exception as exc:
        logger.exception("Bronze read failed.")
        raise RuntimeError(f"Failed to read source data from {source_path}") from exc


def add_audit_columns(df: DataFrame) -> DataFrame:
    """Add ingestion metadata columns for data lineage and traceability."""
    try:
        logger.info("Adding Bronze audit columns.")

        if "_metadata" in df.columns:
            enriched_df = (
                df.withColumn("ingestion_time", current_timestamp())
                  .withColumn("source_file", col("_metadata.file_path"))
            )
        else:
            enriched_df = (
                df.withColumn("ingestion_time", current_timestamp())
                  .withColumn("source_file", lit("unknown"))
            )

        logger.info("Audit columns added successfully.")

        return enriched_df

    except Exception as exc:
        logger.exception("Failed to add Bronze audit columns.")
        raise RuntimeError("Failed to add audit columns to Bronze dataframe.") from exc