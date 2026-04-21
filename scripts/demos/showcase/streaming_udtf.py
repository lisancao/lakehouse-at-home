#!/usr/bin/env python3
"""
Streaming + Python UDTF
================================================

Demonstrates:
1. Kafka → Spark Structured Streaming → Iceberg (batch mode for demo)
2. Python UDTF to explode order lifecycle into event rows

Run:
    docker exec spark-master-41 /opt/spark/bin/spark-submit \
        --packages org.apache.spark:spark-sql-kafka-0-10_2.13:4.1.0 \
        /scripts/demos/showcase/streaming_udtf.py

Prerequisites:
    ./lakehouse start all
    ./lakehouse testdata generate --days 1
    ./lakehouse testdata load
    # Optional: python -m scripts.testdata --kafka (to produce to Kafka)
"""

from pyspark.sql import SparkSession
from pyspark.sql import functions as f
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType, TimestampType

def main():
    spark = SparkSession.builder \
        .appName("lakehouse-demo") \
        .config("spark.sql.streaming.schemaInference", "true") \
        .getOrCreate()
    spark.sparkContext.setLogLevel("WARN")

    print("\n" + "=" * 60)
    print("Streaming + Python UDTF")
    print("=" * 60)
    print(f"Spark version: {spark.version}")

    # Part A: Use batch orders as source (streaming requires Kafka producer)
    # For live demo: switch to readStream.format("kafka") when producer is running
    try:
        orders_df = spark.read.parquet("/data/events/orders_90d.parquet").limit(500)
    except Exception:
        # Create minimal sample
        from pyspark.sql.types import StructType, StructField
        schema = StructType([
            StructField("event_id", StringType()),
            StructField("event_type", StringType()),
            StructField("ts", StringType()),
            StructField("order_id", StringType()),
            StructField("location_id", IntegerType()),
            StructField("sequence", IntegerType()),
            StructField("body", StringType()),
        ])
        sample = [
            ("e1", "order_created", "2024-01-15T12:00:00", "ORD001", 1, 0, '{"brand_id":1,"total":25.99}'),
            ("e2", "delivered", "2024-01-15T12:45:00", "ORD001", 1, 7, '{"total_mins":45.0}'),
        ]
        orders_df = spark.createDataFrame(sample * 100, schema)

    orders_df = orders_df.withColumn(
        "event_timestamp", f.to_timestamp(f.regexp_replace("ts", "T", " "))
    )

    # Build order_lifecycle (pivot) - need silver.order_lifecycle or create from orders
    # For demo: create minimal lifecycle from orders
    from pyspark.sql.types import StructType, StructField, IntegerType, DoubleType
    body_schema = StructType([
        StructField("brand_id", IntegerType(), True),
        StructField("total", DoubleType(), True),
    ])
    enriched = orders_df.withColumn("body_parsed", f.from_json("body", body_schema)).select(
        "order_id", "event_type", "event_timestamp", "location_id",
        f.col("body_parsed.total").alias("order_total"),
    )

    # Pivot to get created_at, delivered_at per order
    lifecycle = enriched.groupBy("order_id", "location_id").pivot(
        "event_type", ["order_created", "delivered"]
    ).agg(f.min("event_timestamp").alias("ts"))

    lifecycle = lifecycle.select(
        "order_id",
        f.col("order_created").alias("created_at"),
        f.col("delivered").alias("delivered_at"),
        "location_id",
        f.lit("San Francisco").alias("city_name"),
    ).filter(f.col("delivered_at").isNotNull())

    # Ensure we have data
    if lifecycle.count() == 0:
        # Create synthetic lifecycle
        from datetime import datetime, timedelta
        base = datetime(2024, 1, 15, 12, 0, 0)
        rows = [
            (f"ORD{i:03d}", base + timedelta(minutes=i), base + timedelta(minutes=i+45), 1, "San Francisco")
            for i in range(20)
        ]
        lifecycle = spark.createDataFrame(rows, ["order_id", "created_at", "delivered_at", "location_id", "city_name"])

    lifecycle.createOrReplaceTempView("order_lifecycle")

    # Part B: Python UDTF - explode order into event rows
    from pyspark.sql.functions import udtf

    from pyspark.sql.types import StructType, StructField, StringType, TimestampType, DoubleType, IntegerType as IT

    @udtf(returnType=StructType([
        StructField("order_id", StringType()),
        StructField("event_type", StringType()),
        StructField("event_ts", TimestampType()),
        StructField("duration_mins", DoubleType()),
        StructField("location_id", IT()),
        StructField("city_name", StringType()),
    ]))
    class OrderLifecycleExploder:
        def eval(self, order_id: str, created_at, delivered_at, location_id: int, city_name: str):
            if created_at is None:
                return
            total_mins = (delivered_at - created_at).total_seconds() / 60 if delivered_at else None
            yield (order_id, "order_created", created_at, None, location_id, city_name)
            if delivered_at:
                yield (order_id, "delivered", delivered_at, total_mins, location_id, city_name)

    spark.udtf.register("order_lifecycle_explode", OrderLifecycleExploder)

    print("\n[UDTF] Registered order_lifecycle_explode()")
    print("  Invoking: SELECT * FROM order_lifecycle_explode(SELECT ... FROM order_lifecycle)")

    # Spark 4.1: UDTF with table argument
    try:
        exploded = spark.sql("""
            SELECT * FROM order_lifecycle_explode(
                TABLE order_lifecycle
            )
        """)
    except Exception:
        # Fallback: call row-by-row via lateral join or collect
        try:
            exploded = spark.sql("""
                SELECT o.order_id, o.created_at, o.delivered_at, o.location_id, o.city_name
                FROM order_lifecycle o
                LIMIT 10
            """)
            # Simulate explode: create two rows per order
            from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType
            rows = []
            for row in exploded.collect():
                rows.append((row.order_id, "order_created", row.created_at, None, row.location_id, row.city_name))
                if row.delivered_at:
                    mins = (row.delivered_at - row.created_at).total_seconds() / 60
                    rows.append((row.order_id, "delivered", row.delivered_at, mins, row.location_id, row.city_name))
            exploded = spark.createDataFrame(rows, ["order_id", "event_type", "event_ts", "duration_mins", "location_id", "city_name"])
            print("  [FALLBACK] Simulated UDTF output (TABLE syntax not supported)")
        except Exception as e:
            print(f"  [SKIP] UDTF not available: {e}")
            exploded = lifecycle.limit(5)

    print("\n  Exploded order lifecycle (sample):")
    exploded.show(15, truncate=False)

    # Write to Iceberg
    spark.sql("CREATE NAMESPACE IF NOT EXISTS iceberg.gold")
    exploded.write.mode("overwrite").saveAsTable("iceberg.gold.order_events_exploded")
    print("\n[Iceberg] Created iceberg.gold.order_events_exploded")

    print("\n" + "=" * 60)
    print("Demo 2 complete!")
    print("=" * 60)
    spark.stop()


if __name__ == "__main__":
    main()
