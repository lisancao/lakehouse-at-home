#!/usr/bin/env python3
"""
Full Over-Architected Pipeline
======================================================

Combines multiple Spark 4.1 features in one script:
1. VARIANT/parse_json for flexible body parsing
2. Recursive CTE for event chain traversal
3. Collation for case-insensitive brand search (when available)
4. SDP-style declarative table definitions
5. Iceberg writes with partitioning

Run:
    docker exec spark-master-41 /opt/spark/bin/spark-submit \
        /scripts/demos/showcase/spark41_feature_tour.py

Prerequisites:
    ./lakehouse start all
    ./lakehouse testdata generate --days 7
    ./lakehouse testdata load
"""

from pyspark.sql import SparkSession
from pyspark.sql import functions as f
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType

def main():
    spark = SparkSession.builder.appName("lakehouse-demo").getOrCreate()
    spark.sparkContext.setLogLevel("WARN")

    print("\n" + "=" * 60)
    print("Full Pipeline")
    print("=" * 60)
    print(f"Spark version: {spark.version}")

    # Ensure we have data - run pipeline_spark41 first or use sample
    try:
        orders = spark.read.parquet("/data/events/orders_90d.parquet")
        dim_locations = spark.read.parquet("/data/dimensions/locations.parquet")
        dim_brands = spark.read.parquet("/data/dimensions/brands.parquet")
        print("Loaded parquet sources")
    except Exception:
        print("Parquet not found - ensure testdata is loaded")
        spark.stop()
        return

    # Create namespaces
    spark.sql("CREATE NAMESPACE IF NOT EXISTS iceberg.gold")

    # Step 1: VARIANT body (or from_json fallback)
    orders_ts = orders.withColumn(
        "event_timestamp", f.to_timestamp(f.regexp_replace("ts", "T", " "))
    ).limit(5000)

    try:
        orders_variant = orders_ts.withColumn("body_variant", f.try_parse_json("body"))
        orders_extracted = orders_variant.withColumn(
            "brand_id", f.expr("try_variant_get(body_variant, '$.brand_id', 'int')")
        ).withColumn(
            "order_total", f.expr("try_variant_get(body_variant, '$.total', 'double')")
        )
        print("\n[1] VARIANT: Parsed body with try_parse_json + try_variant_get")
    except Exception:
        body_schema = StructType([
            StructField("brand_id", IntegerType(), True),
            StructField("total", DoubleType(), True),
        ])
        orders_extracted = orders_ts.withColumn("body_parsed", f.from_json("body", body_schema)).select(
            "*", f.col("body_parsed.brand_id").alias("brand_id"),
            f.col("body_parsed.total").alias("order_total"),
        ).drop("body_parsed")
        print("\n[1] FALLBACK: Parsed body with from_json")

    # Drop VARIANT column before Iceberg write (Iceberg v2 doesn't support VARIANT type)
    if "body_variant" in orders_extracted.columns:
        orders_extracted = orders_extracted.drop("body_variant")

    # Join locations
    loc_lookup = dim_locations.select(
        f.col("id").alias("location_id"), f.col("city").alias("city_name")
    )
    enriched = orders_extracted.join(f.broadcast(loc_lookup), on="location_id", how="left")

    # Step 2: Recursive CTE for event chain
    enriched.createOrReplaceTempView("order_events")

    try:
        chain_df = spark.sql("""
            WITH RECURSIVE event_chain AS (
                SELECT order_id, event_id, event_type, event_timestamp, sequence, 1 AS depth
                FROM order_events WHERE sequence = 0
                UNION ALL
                SELECT e.order_id, e.event_id, e.event_type, e.event_timestamp, e.sequence, c.depth + 1
                FROM order_events e
                JOIN event_chain c ON e.order_id = c.order_id AND e.sequence = c.sequence + 1
            )
            SELECT order_id, event_type, event_timestamp, depth
            FROM event_chain
            WHERE order_id IN (SELECT DISTINCT order_id FROM order_events LIMIT 10)
            ORDER BY order_id, depth
        """)
        print("\n[2] Recursive CTE: Event chain traversal")
        chain_df.show(20, truncate=False)
    except Exception as e:
        print(f"\n[2] Recursive CTE skipped: {e}")

    # Step 3: Collation (case-insensitive brand search)
    dim_brands.createOrReplaceTempView("brands")
    try:
        collation_df = spark.sql("""
            SELECT name, name COLLATE utf8_lcase AS name_lower
            FROM brands
            WHERE name COLLATE utf8_lcase LIKE '%pizza%' OR name COLLATE utf8_lcase LIKE '%burger%'
        """)
        print("\n[3] Collation: Case-insensitive brand search")
        collation_df.show(truncate=False)
    except Exception as e:
        print(f"\n[3] Collation skipped: {e}")
        collation_df = spark.sql("SELECT name FROM brands WHERE LOWER(name) LIKE '%pizza%' OR LOWER(name) LIKE '%burger%'")
        collation_df.show(truncate=False)

    # Step 4: Gold aggregations (SDP-style logic)
    gold_hourly = enriched.filter(f.col("event_type") == "order_created").groupBy(
        f.to_date("event_timestamp").alias("event_date"),
        f.hour("event_timestamp").alias("event_hour"),
        "location_id", "city_name"
    ).agg(
        f.count("order_id").alias("order_count"),
        f.sum("order_total").alias("total_revenue"),
        f.avg("order_total").alias("avg_order_value"),
    )
    print("\n[4] Gold: hourly_metrics aggregation")

    gold_brand = enriched.filter(f.col("event_type") == "order_created").groupBy("brand_id").agg(
        f.count("order_id").alias("total_orders"),
        f.sum("order_total").alias("total_revenue"),
    ).join(
        dim_brands.select(f.col("id").alias("brand_id"), "name"),
        on="brand_id", how="left"
    )
    print("     brand_summary aggregation")

    # Step 5: Write to Iceberg
    spark.sql("DROP TABLE IF EXISTS iceberg.gold.gold_hourly")
    spark.sql("DROP TABLE IF EXISTS iceberg.gold.gold_brand")

    gold_hourly.write.mode("overwrite").saveAsTable("iceberg.gold.gold_hourly")
    gold_brand.write.mode("overwrite").saveAsTable("iceberg.gold.gold_brand")

    print("\n[5] Iceberg: Written iceberg.gold.gold_hourly, iceberg.gold.gold_brand")

    # Summary
    h_count = spark.table("iceberg.gold.gold_hourly").count()
    b_count = spark.table("iceberg.gold.gold_brand").count()
    print(f"\n  gold_hourly: {h_count} rows")
    print(f"  gold_brand: {b_count} rows")

    print("\n  Sample gold_hourly:")
    spark.table("iceberg.gold.gold_hourly").orderBy(f.desc("total_revenue")).show(5, truncate=False)

    print("\n" + "=" * 60)
    print("Demo 3 complete! All features wired together.")
    print("=" * 60)
    spark.stop()


if __name__ == "__main__":
    main()
