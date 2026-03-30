"""Transformation logic for Silver and Gold layers."""

from __future__ import annotations

import logging

from pyspark.sql import DataFrame
from pyspark.sql.functions import (
    col,
    count,
    date_format,
    hour,
    lit,
    to_date,
    when,
)
from pyspark.sql.types import DoubleType, IntegerType, TimestampType


logger = logging.getLogger(__name__)


def transform_to_silver(df: DataFrame, required_columns: list[str]) -> DataFrame:
    """Apply cleansing, typing, enrichment, and quality checks for Silver layer."""
    try:
        logger.info("Silver transformation started.")

        silver_df = (
            df.dropDuplicates(
                [
                    "VendorID",
                    "tpep_pickup_datetime",
                    "tpep_dropoff_datetime",
                    "PULocationID",
                    "DOLocationID",
                    "trip_distance",
                    "total_amount",
                ]
            )
            .dropna(subset=required_columns)
            .fillna(
                {
                    "passenger_count": 1.0,
                    "tip_amount": 0.0,
                    "airport_fee": 0.0,
                    "congestion_surcharge": 0.0,
                }
            )
            .withColumn("VendorID", col("VendorID").cast(IntegerType()))
            .withColumn("PULocationID", col("PULocationID").cast(IntegerType()))
            .withColumn("DOLocationID", col("DOLocationID").cast(IntegerType()))
            .withColumn("trip_distance", col("trip_distance").cast(DoubleType()))
            .withColumn("total_amount", col("total_amount").cast(DoubleType()))
            .withColumn("tpep_pickup_datetime", col("tpep_pickup_datetime").cast(TimestampType()))
            .withColumn("tpep_dropoff_datetime", col("tpep_dropoff_datetime").cast(TimestampType()))
            .withColumn(
                "trip_duration_minutes",
                (
                    col("tpep_dropoff_datetime").cast("long") - col("tpep_pickup_datetime").cast("long")
                )
                / lit(60),
            )
            .withColumn("pickup_date", to_date("tpep_pickup_datetime"))
            .withColumn("pickup_hour", hour("tpep_pickup_datetime"))
            .withColumn(
                "pickup_ts_formatted",
                date_format("tpep_pickup_datetime", "yyyy-MM-dd HH:mm:ss"),
            )
            .withColumn(
                "dropoff_ts_formatted",
                date_format("tpep_dropoff_datetime", "yyyy-MM-dd HH:mm:ss"),
            )
        )

        silver_df = silver_df.filter(
            (col("trip_duration_minutes") >= 0)
            & (col("trip_distance") >= 0)
            & (col("total_amount") >= 0)
        )
        logger.info("Silver transformation succeeded. record_count=%s", silver_df.count())
        return silver_df
    except Exception as exc:
        logger.exception("Silver transformation failed.")
        raise RuntimeError("Failed while transforming Bronze to Silver.") from exc


def run_silver_quality_checks(df: DataFrame) -> None:
    """Execute lightweight, blocking quality checks for Silver data."""
    try:
        logger.info("Silver quality checks started.")
        invalid_count = df.filter(
            (col("trip_duration_minutes") < 0)
            | (col("trip_distance") < 0)
            | (col("total_amount") < 0)
        ).count()
        if invalid_count > 0:
            raise ValueError(f"Silver quality checks failed: {invalid_count} invalid records found.")
        logger.info("Silver quality checks passed.")
    except Exception as exc:
        logger.exception("Silver quality checks failed.")
        raise RuntimeError("Silver quality checks failed.") from exc


def build_gold_pickup_zone(df: DataFrame) -> DataFrame:
    """Aggregate trip KPIs by pickup zone."""
    try:
        logger.info("Gold pickup zone aggregation started.")
        result_df = (
            df.groupBy("PULocationID", "pickup_date")
            .agg(
                count("*").alias("trip_count"),
                {"trip_distance": "avg", "total_amount": "avg"},
            )
            .withColumnRenamed("avg(trip_distance)", "avg_trip_distance")
            .withColumnRenamed("avg(total_amount)", "avg_total_amount")
        )
        logger.info("Gold pickup zone aggregation succeeded.")
        return result_df
    except Exception as exc:
        logger.exception("Gold pickup zone aggregation failed.")
        raise RuntimeError("Failed to build Gold pickup zone aggregation.") from exc


def build_gold_trip_hour(df: DataFrame) -> DataFrame:
    """Aggregate trip metrics by hour."""
    try:
        logger.info("Gold hourly aggregation started.")
        result_df = (
            df.groupBy("pickup_date", "pickup_hour")
            .agg(
                count("*").alias("trip_count"),
                {"trip_distance": "avg", "total_amount": "sum"},
            )
            .withColumnRenamed("avg(trip_distance)", "avg_trip_distance")
            .withColumnRenamed("sum(total_amount)", "total_revenue")
        )
        logger.info("Gold hourly aggregation succeeded.")
        return result_df
    except Exception as exc:
        logger.exception("Gold hourly aggregation failed.")
        raise RuntimeError("Failed to build Gold hourly aggregation.") from exc


def build_gold_distance_bands(df: DataFrame) -> DataFrame:
    """Aggregate trips by distance buckets for analytical consumption."""
    try:
        logger.info("Gold distance band aggregation started.")
        bucketed_df = df.withColumn(
            "distance_band",
            when(col("trip_distance") < 2, lit("short"))
            .when((col("trip_distance") >= 2) & (col("trip_distance") < 5), lit("medium"))
            .when((col("trip_distance") >= 5) & (col("trip_distance") < 15), lit("long"))
            .otherwise(lit("very_long")),
        )
        result_df = bucketed_df.groupBy("pickup_date", "distance_band").agg(
            count("*").alias("trip_count"),
            {"total_amount": "avg"},
        )
        result_df = result_df.withColumnRenamed("avg(total_amount)", "avg_total_amount")
        logger.info("Gold distance band aggregation succeeded.")
        return result_df
    except Exception as exc:
        logger.exception("Gold distance band aggregation failed.")
        raise RuntimeError("Failed to build Gold distance band aggregation.") from exc
