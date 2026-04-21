"""Scalable test data generation for benchmarks.

Leverages the existing testdata module for realistic data patterns.
"""

import os
from typing import Optional

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as f
from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    IntegerType,
    LongType,
)

# Schema matching the existing testdata events
BENCHMARK_SCHEMA = StructType(
    [
        StructField("event_id", StringType(), False),
        StructField("event_type", StringType(), False),
        StructField("ts", StringType(), False),
        StructField("ts_seconds", LongType(), False),
        StructField("location_id", IntegerType(), True),
        StructField("order_id", StringType(), False),
        StructField("sequence", IntegerType(), False),
        StructField("body", StringType(), True),
    ]
)


def generate_benchmark_data(
    spark: SparkSession,
    row_count: int,
    seed: int = 42,
    use_existing: bool = True,
) -> DataFrame:
    """Generate benchmark data at specified scale.

    Args:
        spark: SparkSession
        row_count: Number of rows to generate
        seed: Random seed for reproducibility
        use_existing: If True, sample from existing test data if available

    Returns:
        DataFrame with benchmark data
    """
    # Try to use existing test data for realistic patterns
    project_root = os.path.dirname(os.path.dirname(os.path.dirname(__file__)))
    existing_data_path = os.path.join(project_root, "data/events/orders_90d.parquet")

    if use_existing and os.path.exists(existing_data_path):
        base_df = spark.read.parquet(existing_data_path)
        base_count = base_df.count()

        if row_count <= base_count:
            # Sample from existing data
            fraction = row_count / base_count
            return base_df.sample(
                withReplacement=False,
                fraction=min(1.0, fraction * 1.1),  # Slight oversample
                seed=seed,
            ).limit(row_count)
        else:
            # Need to replicate data to reach target size
            replications = (row_count // base_count) + 1
            dfs = []
            for i in range(replications):
                df_copy = base_df.withColumn(
                    "event_id", f.concat(f.col("event_id"), f.lit(f"_{i}"))
                ).withColumn("order_id", f.concat(f.col("order_id"), f.lit(f"_{i}")))
                dfs.append(df_copy)

            from functools import reduce

            combined = reduce(lambda a, b: a.union(b), dfs)
            return combined.limit(row_count)

    # Generate synthetic data if existing data not available
    return _generate_synthetic_data(spark, row_count, seed)


def _generate_synthetic_data(
    spark: SparkSession,
    row_count: int,
    seed: int,
) -> DataFrame:
    """Generate purely synthetic benchmark data."""
    import random
    from datetime import datetime, timedelta

    random.seed(seed)

    event_types = [
        "order_created",
        "kitchen_started",
        "kitchen_finished",
        "order_ready",
        "driver_arrived",
        "driver_picked_up",
        "delivered",
    ]

    base_time = datetime(2024, 1, 1)

    rows = []
    for i in range(row_count):
        event_time = base_time + timedelta(seconds=i * 10)
        rows.append(
            (
                f"evt_{i:08d}",  # event_id
                random.choice(event_types),  # event_type
                event_time.isoformat(),  # ts
                int(event_time.timestamp()),  # ts_seconds
                random.randint(1, 4),  # location_id
                f"ORD{i // 8:06d}",  # order_id
                i % 8,  # sequence
                '{"brand_id": 1, "total": 25.99}',  # body
            )
        )

    return spark.createDataFrame(rows, BENCHMARK_SCHEMA)


def get_data_size_mb(df: DataFrame) -> float:
    """Estimate DataFrame size in MB (approximate)."""
    # This is a rough estimate based on row count and average row size
    row_count = df.count()
    # Estimate ~200 bytes per row for our schema
    estimated_bytes = row_count * 200
    return estimated_bytes / (1024 * 1024)
