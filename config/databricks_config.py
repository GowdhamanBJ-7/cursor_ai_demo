"""Databricks runtime configuration for the ETL pipeline."""

from __future__ import annotations

import argparse
import logging
from dataclasses import dataclass


logger = logging.getLogger(__name__)


@dataclass(frozen=True)
class PipelineConfig:
    """Runtime configuration values consumed by the ETL pipeline."""

    catalog: str
    schema: str
    source_path: str
    source_format: str
    bronze_table: str
    silver_table: str
    gold_zone_table: str
    gold_hour_table: str
    gold_distance_table: str
    checkpoint_root: str

    @property
    def bronze_table_fqn(self) -> str:
        return f"{self.catalog}.{self.schema}.{self.bronze_table}"

    @property
    def silver_table_fqn(self) -> str:
        return f"{self.catalog}.{self.schema}.{self.silver_table}"

    @property
    def gold_zone_table_fqn(self) -> str:
        return f"{self.catalog}.{self.schema}.{self.gold_zone_table}"

    @property
    def gold_hour_table_fqn(self) -> str:
        return f"{self.catalog}.{self.schema}.{self.gold_hour_table}"

    @property
    def gold_distance_table_fqn(self) -> str:
        return f"{self.catalog}.{self.schema}.{self.gold_distance_table}"


def parse_args() -> PipelineConfig:
    """Parse CLI arguments for configurable ETL execution."""
    parser = argparse.ArgumentParser(description="NYC Taxi Medallion ETL Pipeline")
    parser.add_argument("--catalog", default="main", help="Unity Catalog catalog name")
    parser.add_argument("--schema", default="nyc_taxi", help="Unity Catalog schema name")
    parser.add_argument(
        "--source-path",
        default="/databricks-datasets/nyctaxi/tripdata/yellow/",
        help="Source path for NYC Taxi input files",
    )
    parser.add_argument(
        "--source-format",
        default="parquet",
        choices=["parquet", "csv"],
        help="Input file format",
    )
    parser.add_argument("--bronze-table", default="taxi_trips_bronze")
    parser.add_argument("--silver-table", default="taxi_trips_silver")
    parser.add_argument("--gold-zone-table", default="taxi_trips_by_pickup_zone_gold")
    parser.add_argument("--gold-hour-table", default="taxi_trips_by_hour_gold")
    parser.add_argument("--gold-distance-table", default="taxi_trips_distance_bands_gold")
    parser.add_argument(
        "--checkpoint-root",
        default="/tmp/nyc_taxi/checkpoints",
        help="Checkpoint root path (reserved for future streaming support)",
    )

    args = parser.parse_args()
    config = PipelineConfig(
        catalog=args.catalog,
        schema=args.schema,
        source_path=args.source_path,
        source_format=args.source_format,
        bronze_table=args.bronze_table,
        silver_table=args.silver_table,
        gold_zone_table=args.gold_zone_table,
        gold_hour_table=args.gold_hour_table,
        gold_distance_table=args.gold_distance_table,
        checkpoint_root=args.checkpoint_root,
    )
    logger.info("Pipeline configuration parsed successfully.")
    return config
