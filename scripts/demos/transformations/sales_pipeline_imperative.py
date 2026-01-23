"""Imperative Version - Order Pipeline.

Same logic as sales_pipeline.py but using traditional Spark APIs
instead of Spark Declarative Pipelines.
"""

from pyspark.sql import SparkSession, functions as f
from pyspark.sql.streaming.query import StreamingQuery
from pyspark.sql.types import StructType, StructField, StringType, IntegerType


def create_spark_session() -> SparkSession:
    """Create or get existing Spark session."""
    return (
        SparkSession.builder
        .appName("SalesPipelineImperative")
        .getOrCreate()
    )


def run_order_stream(spark: SparkSession) -> StreamingQuery:
    """Stream order events from Kafka to silver.order_stream table.

    Reads from Kafka, parses JSON, filters for order_created events,
    and writes to an Iceberg table using streaming.
    """
    schema = StructType([
        StructField("event_id", StringType()),
        StructField("event_type", StringType()),
        StructField("order_id", StringType()),
        StructField("location_id", IntegerType()),
        StructField("ts", StringType()),
    ])

    df = (
        spark.readStream
        .format("kafka")
        .option("kafka.bootstrap.servers", "localhost:9092")
        .option("subscribe", "orders")
        .option("startingOffsets", "latest")
        .load()
        .select(f.from_json(f.col("value").cast("string"), schema).alias("data"))
        .select("data.*")
        .filter(f.col("event_type") == "order_created")
    )

    query = (
        df.writeStream
        .format("iceberg")
        .outputMode("append")
        .option("checkpointLocation", "/tmp/checkpoints/order_stream")
        .toTable("iceberg.silver.order_stream")
    )

    return query


def run_brand_summary(spark: SparkSession) -> None:
    """Create brand summary from Iceberg bronze tables.

    Aggregates order totals per brand and writes to gold.brand_summary table.
    """
    orders = spark.table("iceberg.bronze.orders")
    brands = spark.table("iceberg.bronze.dim_brands")

    df = (
        orders
        .filter(f.col("event_type") == "order_created")
        .groupBy(f.get_json_object("body", "$.brand_id").cast("int").alias("brand_id"))
        .agg(f.count("order_id").alias("total_orders"))
        .join(brands, f.col("brand_id") == brands["id"])
        .select("brand_id", brands["name"].alias("brand_name"), "total_orders")
    )

    # Write to Iceberg table (overwrite for materialized view semantics)
    (
        df.writeTo("iceberg.gold.brand_summary")
        .using("iceberg")
        .createOrReplace()
    )

    print(f"Brand summary written with {df.count()} rows")


def main():
    """Run the pipeline."""
    spark = create_spark_session()

    # Run batch materialized view
    print("Running brand summary...")
    run_brand_summary(spark)

    # Start streaming (blocks until terminated)
    print("Starting order stream from Kafka...")
    query = run_order_stream(spark)
    query.awaitTermination()


if __name__ == "__main__":
    main()
