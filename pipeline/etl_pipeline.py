"""End-to-end NYC Taxi ETL pipeline using Medallion architecture."""

from __future__ import annotations

import logging
import sys
import os

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

from pyspark.sql import SparkSession

from config.databricks_config import parse_args
from config.schema_config import NYC_TAXI_SCHEMA, REQUIRED_SILVER_COLUMNS
from src.ingestion.read_data import add_audit_columns, read_raw_data
from src.transformation.transform_data import (
    build_gold_distance_bands,
    build_gold_pickup_zone,
    build_gold_trip_hour,
    run_silver_quality_checks,
    transform_to_silver,
)
from src.write.write_data import ensure_catalog_and_schema, write_delta_table


def setup_logging() -> None:
    """Initialize unified pipeline logging."""
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s | %(levelname)s | %(name)s | %(message)s",
    )


def run_pipeline() -> None:
    """Run Bronze -> Silver -> Gold ETL flow."""
    setup_logging()
    logger = logging.getLogger(__name__)

    spark = (
        SparkSession.builder.appName("nyc-taxi-medallion-etl")
        .config("spark.sql.shuffle.partitions", "200")
        .getOrCreate()
    )

    try:
        logger.info("ETL pipeline started.")

        config = parse_args()

        ensure_catalog_and_schema(spark, config.catalog, config.schema)

       
        logger.info("Starting Bronze layer...")

        bronze_raw_df = read_raw_data(
            spark=spark,
            source_path=config.source_path,
            source_format=config.source_format,
            schema=NYC_TAXI_SCHEMA,
        )

        bronze_df = add_audit_columns(bronze_raw_df)

        write_delta_table(
            bronze_df,
            config.bronze_table_fqn,
            mode="overwrite",
        )

        logger.info("Bronze layer completed.")

        # =========================
        # ⚪ Silver Layer
        # =========================
        logger.info("Starting Silver layer...")

        silver_df = transform_to_silver(
            bronze_df,
            REQUIRED_SILVER_COLUMNS,
        )

        run_silver_quality_checks(silver_df)

        write_delta_table(
            silver_df,
            config.silver_table_fqn,
            mode="overwrite",
            partition_cols=["pickup_date"],
        )

        logger.info("Silver layer completed.")

       
        logger.info("Starting Gold layer...")

        gold_zone_df = build_gold_pickup_zone(silver_df)
        gold_hour_df = build_gold_trip_hour(silver_df)
        gold_distance_df = build_gold_distance_bands(silver_df)

        write_delta_table(
            gold_zone_df,
            config.gold_zone_table_fqn,
            mode="overwrite",
            partition_cols=["pickup_date"],
        )

        write_delta_table(
            gold_hour_df,
            config.gold_hour_table_fqn,
            mode="overwrite",
            partition_cols=["pickup_date", "pickup_hour"],
        )

        write_delta_table(
            gold_distance_df,
            config.gold_distance_table_fqn,
            mode="overwrite",
            partition_cols=["pickup_date", "distance_band"],
        )

        logger.info("Gold layer completed.")
        logger.info("ETL pipeline completed successfully.")

    except Exception as e:
        logger.exception("ETL pipeline failed due to error: %s", str(e))
        raise

    finally:
        spark.stop()
        logger.info("Spark session stopped.")


if __name__ == "__main__":
    try:
        run_pipeline()
    except Exception:
        sys.exit(1)