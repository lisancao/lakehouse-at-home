#!/usr/bin/env python3
"""
OverArchitected Demo 1: VARIANT Type + Iceberg
==============================================

Demonstrates Spark 4.1 VARIANT type for semi-structured order bodies.
- parse_json() converts JSON string to VARIANT
- variant_get() extracts fields with JSONPath
- Writes to Iceberg with flexible schema

Run:
    docker exec spark-master-41 /opt/spark/bin/spark-submit \
        /scripts/demos/overarchitected/01_variant_iceberg.py

Prerequisites:
    ./lakehouse start all
    ./lakehouse testdata generate --days 7
    ./lakehouse testdata load
"""

from pyspark.sql import SparkSession
from pyspark.sql import functions as f

def main():
    spark = SparkSession.builder.appName("OverArchitected-01-Variant").getOrCreate()
    spark.sparkContext.setLogLevel("WARN")

    print("\n" + "=" * 60)
    print("OverArchitected Demo 1: VARIANT Type + Iceberg")
    print("=" * 60)
    print(f"Spark version: {spark.version}")

    # Read orders (use parquet if available, else create sample)
    try:
        df = spark.read.parquet("/data/events/orders_90d.parquet").limit(2000)
        print(f"Loaded {df.count()} rows from /data/events/orders_90d.parquet")
    except Exception:
        # Fallback: create sample data
        from pyspark.sql.types import StructType, StructField, StringType, IntegerType
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
            ("e1", "order_created", "2024-01-15T12:00:00", "ORD001", 1, 0,
             '{"brand_id":1,"total":25.99,"items":[{"name":"Burger","price":12.99}]}'),
            ("e2", "delivered", "2024-01-15T12:45:00", "ORD001", 1, 7,
             '{"delivery_lat":37.77,"delivery_lon":-122.41,"total_mins":45.0}'),
        ]
        df = spark.createDataFrame(sample * 500, schema)
        print("Using sample data (parquet not found)")

    # Add timestamp
    df = df.withColumn("event_timestamp", f.to_timestamp(f.regexp_replace("ts", "T", " ")))

    # VARIANT: parse_json (Spark 4.1) - fallback to from_json if unavailable
    try:
        df_variant = df.withColumn("body_variant", f.parse_json("body"))
        print("\n[VARIANT] Using parse_json() -> VARIANT type")
    except AttributeError:
        # Fallback: use from_json with generic schema
        from pyspark.sql.types import MapType, StringType as St
        df_variant = df.withColumn("body_variant", f.from_json("body", "map<string,string>"))
        print("\n[FALLBACK] Using from_json (VARIANT not available in this build)")

    # Extract fields - use expr for variant_get when available
    try:
        df_extracted = df_variant.withColumn(
            "brand_id", f.expr("variant_get(body_variant, '$.brand_id', 'int')")
        ).withColumn(
            "total", f.expr("variant_get(body_variant, '$.total', 'double')")
        ).withColumn(
            "driver_id", f.expr("try_variant_get(body_variant, '$.driver_id', 'string')")
        )
        print("[VARIANT] Extracted brand_id, total, driver_id via variant_get()")
    except Exception:
        # Fallback: from_json with struct
        from pyspark.sql.types import StructType, StructField, IntegerType, DoubleType, StringType
        body_schema = StructType([
            StructField("brand_id", IntegerType(), True),
            StructField("total", DoubleType(), True),
            StructField("driver_id", StringType(), True),
        ])
        df_extracted = df.withColumn("body_parsed", f.from_json("body", body_schema)).select(
            "*",
            f.col("body_parsed.brand_id").alias("brand_id"),
            f.col("body_parsed.total").alias("total"),
            f.col("body_parsed.driver_id").alias("driver_id"),
        ).drop("body_parsed")
        print("[FALLBACK] Extracted via from_json + struct")

    # Create namespace and table
    spark.sql("CREATE NAMESPACE IF NOT EXISTS iceberg.overarch")
    spark.sql("DROP TABLE IF EXISTS iceberg.overarch.orders_variant")

    df_extracted.write.mode("overwrite").saveAsTable("iceberg.overarch.orders_variant")
    print("\n[Iceberg] Created iceberg.overarch.orders_variant")

    # Verify
    count = spark.sql("SELECT COUNT(*) FROM iceberg.overarch.orders_variant").collect()[0][0]
    print(f"\n  Rows written: {count:,}")
    print("\n  Sample:")
    spark.sql("""
        SELECT event_id, event_type, order_id, brand_id, total
        FROM iceberg.overarch.orders_variant
        WHERE brand_id IS NOT NULL
        LIMIT 5
    """).show(truncate=False)

    print("\n" + "=" * 60)
    print("Demo 1 complete!")
    print("=" * 60)
    spark.stop()


if __name__ == "__main__":
    main()
