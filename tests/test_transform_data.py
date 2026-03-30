"""Unit tests for Silver and Gold transformation logic."""

from datetime import datetime

from pyspark.sql import SparkSession

from src.transformation.transform_data import (
    build_gold_distance_bands,
    build_gold_pickup_zone,
    build_gold_trip_hour,
    transform_to_silver,
)


def _spark() -> SparkSession:
    return SparkSession.builder.master("local[*]").appName("nyc-taxi-tests").getOrCreate()


def test_transform_to_silver_adds_derived_columns() -> None:
    spark = _spark()
    input_rows = [
        (
            1,
            datetime(2024, 1, 1, 10, 0, 0),
            datetime(2024, 1, 1, 10, 30, 0),
            1.0,
            3.2,
            1,
            2,
            15.0,
        )
    ]
    columns = [
        "VendorID",
        "tpep_pickup_datetime",
        "tpep_dropoff_datetime",
        "passenger_count",
        "trip_distance",
        "PULocationID",
        "DOLocationID",
        "total_amount",
    ]
    df = spark.createDataFrame(input_rows, columns)
    result = transform_to_silver(
        df,
        required_columns=["tpep_pickup_datetime", "tpep_dropoff_datetime", "trip_distance", "total_amount"],
    )

    assert "trip_duration_minutes" in result.columns
    assert "pickup_date" in result.columns
    assert result.count() == 1
    spark.stop()


def test_gold_aggregations_return_rows() -> None:
    spark = _spark()
    input_rows = [
        (
            1,
            datetime(2024, 1, 1, 10, 0, 0),
            datetime(2024, 1, 1, 10, 30, 0),
            1.0,
            3.2,
            10,
            2,
            15.0,
        ),
        (
            1,
            datetime(2024, 1, 1, 11, 0, 0),
            datetime(2024, 1, 1, 11, 40, 0),
            1.0,
            8.1,
            10,
            3,
            20.0,
        ),
    ]
    columns = [
        "VendorID",
        "tpep_pickup_datetime",
        "tpep_dropoff_datetime",
        "passenger_count",
        "trip_distance",
        "PULocationID",
        "DOLocationID",
        "total_amount",
    ]
    df = spark.createDataFrame(input_rows, columns)
    silver = transform_to_silver(
        df,
        required_columns=["tpep_pickup_datetime", "tpep_dropoff_datetime", "trip_distance", "total_amount"],
    )

    assert build_gold_pickup_zone(silver).count() >= 1
    assert build_gold_trip_hour(silver).count() >= 1
    assert build_gold_distance_bands(silver).count() >= 1
    spark.stop()
