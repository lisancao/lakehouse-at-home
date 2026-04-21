"""OverArchitected SDP Demo — Clean Medallion Pipeline
=====================================================

Shows Spark Declarative Pipelines building silver and gold layers
on top of existing Iceberg bronze tables + live Kafka streaming.

Bronze (already exists):
  - iceberg.bronze.orders          — batch order events (loaded earlier)
  - iceberg.bronze.dim_locations   — delivery cities
  - iceberg.bronze.dim_brands      — ghost kitchen brands

This pipeline adds:
  - bronze.orders_streaming        — live Kafka ingest (@dp.table)
  - silver.orders_enriched         — cleaned, joined with locations
  - gold.hourly_metrics            — order counts + revenue by city/hour
  - gold.brand_summary             — brand-level KPIs

Run:
    spark-pipelines run --spec scripts/demos/overarchitected/sdp-pipeline.yml

    # Or dry-run to see the dependency graph:
    spark-pipelines dry-run --spec scripts/demos/overarchitected/sdp-pipeline.yml

Prerequisites:
    ./lakehouse start all
    ./lakehouse testdata generate --days 7
    ./lakehouse testdata load       # populates bronze tables
    ./lakehouse producer            # (optional) start Kafka stream
"""

from pyspark import pipelines as dp
from pyspark.sql import SparkSession, functions as f
from pyspark.sql.types import (
    StructType, StructField, StringType, IntegerType, DoubleType,
)


def get_spark():
    """Get the active SparkSession (provided by SDP framework)."""
    return SparkSession.getActiveSession()


# =============================================================================
# BRONZE — Streaming ingest from Kafka
# =============================================================================

@dp.table(name="bronze.orders_streaming")
def orders_streaming():
    """Live order events from Kafka. Runs continuously."""
    schema = StructType([
        StructField("event_id", StringType()),
        StructField("event_type", StringType()),
        StructField("ts", StringType()),
        StructField("ts_seconds", IntegerType()),
        StructField("order_id", StringType()),
        StructField("location_id", IntegerType()),
        StructField("sequence", IntegerType()),
        StructField("body", StringType()),
    ])

    return (
        get_spark().readStream
        .format("kafka")
        .option("kafka.bootstrap.servers", "localhost:9092")
        .option("subscribe", "orders")
        .option("startingOffsets", "latest")
        .load()
        .select(
            f.from_json(f.col("value").cast("string"), schema).alias("e"),
            f.col("timestamp").alias("kafka_timestamp"),
        )
        .select(
            "e.event_id",
            "e.event_type",
            f.coalesce(
                f.try_to_timestamp("e.ts", f.lit("yyyy-MM-dd'T'HH:mm:ss.SSSSSS")),
                f.try_to_timestamp("e.ts", f.lit("yyyy-MM-dd'T'HH:mm:ss")),
            ).alias("event_timestamp"),
            "e.order_id",
            "e.location_id",
            "e.sequence",
            "e.body",
            "kafka_timestamp",
        )
    )


# =============================================================================
# SILVER — Enrichment
# =============================================================================

@dp.materialized_view(name="silver.orders_enriched")
def orders_enriched():
    """Orders enriched with city name and parsed order details.

    Reads from batch bronze table. Joins with dim_locations.
    Extracts brand_id and total from JSON body.
    """
    orders = get_spark().table("iceberg.bronze.orders")
    locations = get_spark().table("iceberg.bronze.dim_locations").select(
        f.col("id").alias("location_id"),
        f.col("city"),
    )

    return (
        orders
        .filter(f.col("event_id").isNotNull())
        .join(locations, on="location_id", how="left")
        .select(
            "event_id",
            "event_type",
            "event_timestamp",
            "order_id",
            "location_id",
            "city",
            "sequence",
            f.get_json_object("body", "$.brand_id").cast("int").alias("brand_id"),
            f.get_json_object("body", "$.total").cast("double").alias("order_total"),
            f.to_date("event_timestamp").alias("event_date"),
            f.hour("event_timestamp").alias("event_hour"),
        )
    )


# =============================================================================
# GOLD — Aggregations
# =============================================================================

@dp.materialized_view(name="gold.hourly_metrics")
def hourly_metrics():
    """Hourly order volume and revenue by city."""
    return (
        get_spark().table("iceberg.silver.orders_enriched")
        .filter(f.col("event_type") == "order_created")
        .groupBy("event_date", "event_hour", "city")
        .agg(
            f.count("order_id").alias("order_count"),
            f.sum("order_total").alias("total_revenue"),
            f.avg("order_total").alias("avg_order_value"),
        )
    )


@dp.materialized_view(name="gold.brand_summary")
def brand_summary():
    """Brand-level summary: orders, revenue, reach."""
    orders = get_spark().table("iceberg.silver.orders_enriched")
    brands = get_spark().table("iceberg.bronze.dim_brands").select(
        f.col("id").alias("brand_id"),
        f.col("name").alias("brand_name"),
    )

    return (
        orders
        .filter(f.col("event_type") == "order_created")
        .groupBy("brand_id")
        .agg(
            f.count("order_id").alias("total_orders"),
            f.sum("order_total").alias("total_revenue"),
            f.avg("order_total").alias("avg_order_value"),
            f.countDistinct("city").alias("cities_served"),
        )
        .join(brands, on="brand_id", how="left")
        .select(
            "brand_id", "brand_name",
            "total_orders", "total_revenue", "avg_order_value", "cities_served",
        )
    )
