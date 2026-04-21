#!/usr/bin/env python3
"""
"We Need Compute" — Spark 4.1 Setup + Features
======================================================================

Now they need a compute engine.
This script walks through Spark 4.1 configuration and demonstrates
the headline features on Casper's Kitchen data.

Demonstrates:
  1. Spark configuration walkthrough (what goes in spark-defaults.conf)
  2. VARIANT type — parse_json, try_variant_get on order bodies
  3. Recursive CTE — event chain traversal
  4. Collation — case-insensitive brand search
  5. Spark Connect — thin client introduction

Run:
    docker exec spark-master-41 /opt/spark/bin/spark-submit \
        /scripts/demos/showcase/spark_cluster_diagnostic.py

Prerequisites:
    ./lakehouse start all
    ./lakehouse testdata generate --days 7
    ./lakehouse testdata load
"""

from pyspark.sql import SparkSession
from pyspark.sql import functions as f
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType


def section(title):
    print(f"\n{'─' * 60}")
    print(f"  {title}")
    print(f"{'─' * 60}")


def act3_config_walkthrough(spark):
    """Step 1: What does spark-defaults.conf actually look like?"""
    section("STEP 1: Spark Configuration (The Confusing Part)")

    print(f"""
  A user asks: "What config do I actually need?"

  Spark version: {spark.version}

  spark-defaults.conf for Iceberg + S3-compatible storage:

  # ─── Catalog (tells Spark where tables live) ───
  spark.sql.catalog.iceberg                       org.apache.iceberg.spark.SparkCatalog

  # Option A: JDBC catalog (direct to PostgreSQL)
  spark.sql.catalog.iceberg.type                  jdbc
  spark.sql.catalog.iceberg.uri                   jdbc:postgresql://localhost:5432/iceberg_catalog
  spark.sql.catalog.iceberg.jdbc.user             iceberg
  spark.sql.catalog.iceberg.jdbc.password         iceberg_password

  # Option B: REST catalog (Unity Catalog)
  spark.sql.catalog.iceberg.catalog-impl          org.apache.iceberg.rest.RESTCatalog
  spark.sql.catalog.iceberg.uri                   http://localhost:8081/api/2.1/unity-catalog/iceberg

  # ─── Storage (S3-compatible, points to SeaweedFS) ───
  spark.sql.catalog.iceberg.warehouse             s3a://lakehouse/warehouse
  spark.hadoop.fs.s3a.endpoint                    http://localhost:8333
  spark.hadoop.fs.s3a.access.key                  admin
  spark.hadoop.fs.s3a.secret.key                  admin_password
  spark.hadoop.fs.s3a.path.style.access           true

  # ─── JARs (the real pain point) ───
  spark.jars                                      /opt/spark/jars-extra/iceberg-spark-runtime-4.0_2.13-1.10.0.jar,
                                                  /opt/spark/jars-extra/aws-bundle-*.jar,
                                                  /opt/spark/jars-extra/postgresql-*.jar

  A user asks: "That's... a lot of config."
  User: "Welcome to open source."

  Key versions (don't change without testing):
    - Iceberg: 1.10.0
    - AWS SDK v2: 2.24.6 (exact for Hadoop 3.4.1)
    - Spark: 4.1.0 (Scala 2.13, Java 21)
    """)


def act3_variant_type(spark):
    """Step 2: VARIANT type — flexible schema for JSON bodies."""
    section("STEP 2: VARIANT Type (Spark 4.1)")

    print("""
  The order event `body` field is a JSON string with different schemas
  per event type. VARIANT lets us parse it without declaring a schema upfront.

  Old way: from_json(body, explicit_schema) — breaks if schema changes
  New way: parse_json(body) → VARIANT — schema-on-read, always works
    """)

    orders = spark.read.parquet("/data/events/orders_90d.parquet").limit(3000)
    orders = orders.withColumn(
        "event_timestamp", f.to_timestamp(f.regexp_replace("ts", "T", " "))
    )

    # try_parse_json handles malformed JSON gracefully (returns null instead of error)
    try:
        df = orders.withColumn("body_variant", f.try_parse_json("body"))
        print("  Using try_parse_json() → VARIANT type")

        # Extract fields with try_variant_get (safe — returns null on missing path)
        df_extracted = df.withColumn(
            "brand_id", f.expr("try_variant_get(body_variant, '$.brand_id', 'int')")
        ).withColumn(
            "total", f.expr("try_variant_get(body_variant, '$.total', 'double')")
        ).withColumn(
            "driver_id", f.expr("try_variant_get(body_variant, '$.driver_id', 'string')")
        ).withColumn(
            "delivery_lat", f.expr("try_variant_get(body_variant, '$.delivery_lat', 'double')")
        )
        print("  Extracted: brand_id, total, driver_id, delivery_lat via try_variant_get()")

        # Show different event types extracting different fields
        print("\n  order_created events (brand_id + total populated):")
        df_extracted.filter(
            (f.col("event_type") == "order_created") & f.col("brand_id").isNotNull()
        ).select("order_id", "event_type", "brand_id", "total").show(5, truncate=False)

        print("  delivered events (delivery_lat populated):")
        df_extracted.filter(
            (f.col("event_type") == "delivered") & f.col("delivery_lat").isNotNull()
        ).select("order_id", "event_type", "delivery_lat", "total").show(5, truncate=False)

        print("  Key insight: same column, same query, different fields per event type.")
        print("  No schema explosion. No union of 8 different schemas.")

    except (AttributeError, Exception) as e:
        print(f"  VARIANT not available in this build: {e}")
        print("  Falling back to from_json with explicit schema...")
        body_schema = StructType([
            StructField("brand_id", IntegerType(), True),
            StructField("total", DoubleType(), True),
            StructField("driver_id", StringType(), True),
        ])
        df_extracted = orders.withColumn(
            "body_parsed", f.from_json("body", body_schema)
        ).select(
            "*",
            f.col("body_parsed.brand_id").alias("brand_id"),
            f.col("body_parsed.total").alias("total"),
        ).drop("body_parsed")
        df_extracted.filter(f.col("brand_id").isNotNull()).select(
            "order_id", "event_type", "brand_id", "total"
        ).show(5, truncate=False)

    return df_extracted


def act3_recursive_cte(spark, orders_df):
    """Step 3: Recursive CTE — traverse order event chains."""
    section("STEP 3: Recursive CTE (Spark 4.1)")

    print("""
  Each order has a lifecycle: created → kitchen → ready → driver → delivered.
  Events are linked by order_id + sequence number.
  Recursive CTEs let us traverse this chain in SQL.
    """)

    orders_df.createOrReplaceTempView("order_events")

    try:
        chain_df = spark.sql("""
            WITH RECURSIVE event_chain AS (
                -- Anchor: start with order_created events (sequence 0)
                SELECT order_id, event_id, event_type, event_timestamp, sequence,
                       1 AS depth
                FROM order_events
                WHERE sequence = 0 AND event_type = 'order_created'

                UNION ALL

                -- Recursive: follow the chain by incrementing sequence
                SELECT e.order_id, e.event_id, e.event_type, e.event_timestamp,
                       e.sequence, c.depth + 1
                FROM order_events e
                JOIN event_chain c
                  ON e.order_id = c.order_id
                  AND e.sequence = c.sequence + 1
            )
            SELECT order_id, event_type, event_timestamp, depth
            FROM event_chain
            WHERE order_id IN (
                SELECT DISTINCT order_id FROM order_events
                WHERE sequence = 0 LIMIT 3
            )
            ORDER BY order_id, depth
        """)

        print("  Order lifecycle chains (via recursive CTE):")
        chain_df.show(25, truncate=False)

        print("  Before Spark 4.1: this required multiple self-joins or")
        print("  collect() + Python iteration. Now it's one SQL query.")

    except Exception as e:
        print(f"  Recursive CTE not available: {e}")
        print("  Falling back to sequential join approach...")
        spark.sql("""
            SELECT order_id, event_type, event_timestamp, sequence
            FROM order_events
            WHERE order_id IN (SELECT DISTINCT order_id FROM order_events LIMIT 3)
            ORDER BY order_id, sequence
        """).show(25, truncate=False)


def act3_collation(spark):
    """Step 4: Collation — case-insensitive search."""
    section("STEP 4: Collation (Spark 4.1)")

    print("""
  A user asks: "Find all pizza brands."
  Problem: brand names have mixed case — "Pizza Palace", "PIZZA Express", etc.
  Old way: WHERE LOWER(name) LIKE '%pizza%'  (can't use index)
  New way: WHERE name COLLATE utf8_lcase LIKE '%pizza%'  (collation-aware)
    """)

    try:
        brands = spark.read.parquet("/data/dimensions/brands.parquet")
        brands.createOrReplaceTempView("brands")

        result = spark.sql("""
            SELECT name, cuisine_type,
                   name COLLATE utf8_lcase AS name_normalized
            FROM brands
            WHERE name COLLATE utf8_lcase LIKE '%burger%'
               OR name COLLATE utf8_lcase LIKE '%pizza%'
               OR name COLLATE utf8_lcase LIKE '%wok%'
        """)

        print("  Case-insensitive brand search (collation):")
        result.show(truncate=False)

    except Exception as e:
        print(f"  Collation not available: {e}")
        print("  Falling back to LOWER():")
        brands = spark.read.parquet("/data/dimensions/brands.parquet")
        brands.createOrReplaceTempView("brands")
        spark.sql("""
            SELECT name, cuisine_type
            FROM brands
            WHERE LOWER(name) LIKE '%burger%'
               OR LOWER(name) LIKE '%pizza%'
               OR LOWER(name) LIKE '%wok%'
        """).show(truncate=False)


def act3_spark_connect_intro():
    """Step 5: Spark Connect — thin client introduction."""
    section("STEP 5: Spark Connect (Preview)")

    print("""
  A user asks: "Do I need a full Spark install on my laptop?"

  No. Spark Connect separates client from server.

  SERVER (your cluster — already running):
    /opt/spark/sbin/start-connect-server.sh --master spark://spark-master-41:7077
    # Listens on port 15002 (gRPC + Arrow)

  CLIENT (your laptop — 1.5 MB):
    pip install pyspark-client  # No JVM required!

    from pyspark.sql import SparkSession
    spark = SparkSession.builder.remote("sc://cluster:15002").getOrCreate()
    spark.sql("SELECT * FROM iceberg.bronze.orders").show()

  Same DataFrame API. Same SQL. Zero JVM on client.
  Protocol: gRPC for queries, Apache Arrow for results.

  The progression:
    Level 1: spark.master("local[*]")              # laptop, single JVM
    Level 2: spark.master("spark://host:7078")      # standalone cluster
    Level 3: spark.remote("sc://host:15002")        # thin client, remote cluster
    Level 4: spark.remote("sc://k8s-lb:15002")      # thin client, K8s cluster

  Your DataFrame code is IDENTICAL at every level.
  Only the session builder changes.

  We'll demo this fully in Act 5 (Enterprise Scaling).
    """)


def main():
    spark = SparkSession.builder \
        .appName("lakehouse-demo") \
        .getOrCreate()
    spark.sparkContext.setLogLevel("WARN")

    print("\n" + "=" * 60)
    print("  WE NEED COMPUTE")
    print("  Spark 4.1 — setup, config, and the new features.")
    print("=" * 60)
    print(f"  Spark version: {spark.version}")

    # Load data first
    try:
        spark.read.parquet("/data/events/orders_90d.parquet").limit(1)
    except Exception:
        print("\n  ERROR: No test data found.")
        print("  Run: ./lakehouse testdata generate --days 7 && ./lakehouse testdata load")
        spark.stop()
        return

    act3_config_walkthrough(spark)

    orders_df = act3_variant_type(spark)
    act3_recursive_cte(spark, orders_df)
    act3_collation(spark)
    act3_spark_connect_intro()

    print("\n" + "=" * 60)
    print("  Done.")
    print("  Next: we need PIPELINES to automate this.")
    print("=" * 60 + "\n")
    spark.stop()


if __name__ == "__main__":
    main()
