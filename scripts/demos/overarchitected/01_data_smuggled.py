#!/usr/bin/env python3
"""
OverArchitected Act 1: "We Have Data" — OTF Portability
========================================================

Holly and Nick quit Databricks. They smuggled out their data in Open Table Formats.
This script proves that OTF data is fully portable — no vendor lock-in.

Demonstrates:
  1. Read raw parquet files (dimensions + events) — works anywhere
  2. Show schemas, row counts, sample data
  3. Read Iceberg tables if catalog is configured
  4. Prove the data is complete and usable outside any platform

Run:
    docker exec spark-master-41 /opt/spark/bin/spark-submit \
        /scripts/demos/overarchitected/01_data_smuggled.py

Prerequisites:
    ./lakehouse start all
    ./lakehouse testdata generate --days 7
    ./lakehouse testdata load
"""

from pyspark.sql import SparkSession
from pyspark.sql import functions as f


def section(title):
    print(f"\n{'─' * 60}")
    print(f"  {title}")
    print(f"{'─' * 60}")


def main():
    spark = SparkSession.builder \
        .appName("OverArchitected-01-DataSmuggled") \
        .getOrCreate()
    spark.sparkContext.setLogLevel("WARN")

    print("\n" + "=" * 60)
    print("  ACT 1: WE HAVE DATA")
    print("  Open Table Formats — portable, vendor-neutral, ours.")
    print("=" * 60)
    print(f"  Spark version: {spark.version}")

    # ─── Dimension Tables ───────────────────────────────────────
    section("DIMENSION TABLES (Parquet)")

    dims = {
        "locations":  "/data/dimensions/locations.parquet",
        "brands":     "/data/dimensions/brands.parquet",
        "items":      "/data/dimensions/items.parquet",
        "categories": "/data/dimensions/categories.parquet",
    }

    dim_dfs = {}
    for name, path in dims.items():
        try:
            df = spark.read.parquet(path)
            dim_dfs[name] = df
            cols = ", ".join([f"{c.name}:{c.dataType.simpleString()}" for c in df.schema])
            print(f"\n  {name}: {df.count():,} rows")
            print(f"    Schema: {cols}")
        except Exception as e:
            print(f"\n  {name}: NOT FOUND ({e})")

    # Show a taste of the data
    if "brands" in dim_dfs:
        print("\n  Sample brands:")
        dim_dfs["brands"].select("id", "name", "cuisine_type", "momentum").show(5, truncate=False)

    if "locations" in dim_dfs:
        print("  Sample locations:")
        dim_dfs["locations"].select("id", "city", "lat", "lon").show(5, truncate=False)

    # ─── Events Table ───────────────────────────────────────────
    section("EVENT DATA (Parquet)")

    events_path = "/data/events/"
    try:
        # Find available parquet files
        events_df = None
        for days in ["orders_90d", "orders_30d", "orders_7d", "orders_1d"]:
            try:
                events_df = spark.read.parquet(f"{events_path}{days}.parquet")
                print(f"  Found: {events_path}{days}.parquet")
                break
            except Exception:
                continue

        if events_df is None:
            print("  No event parquet files found. Run: ./lakehouse testdata generate --days 7")
            spark.stop()
            return

        total = events_df.count()
        print(f"  Total events: {total:,}")

        cols = ", ".join([f"{c.name}:{c.dataType.simpleString()}" for c in events_df.schema])
        print(f"  Schema: {cols}")

        # Event type distribution
        print("\n  Event type distribution:")
        events_df.groupBy("event_type") \
            .agg(f.count("*").alias("count")) \
            .orderBy(f.desc("count")) \
            .show(truncate=False)

        # Order lifecycle sample
        print("  Sample order lifecycle (single order):")
        sample_order = events_df.select("order_id").limit(1).collect()[0][0]
        events_df.filter(f.col("order_id") == sample_order) \
            .select("sequence", "event_type", "ts") \
            .orderBy("sequence") \
            .show(truncate=False)

        # Data quality check — the chaos injection
        print("  Data quality (chaos injection in test data):")
        null_location = events_df.filter(f.col("location_id").isNull()).count()
        null_body = events_df.filter(f.col("body").isNull()).count()
        null_order = events_df.filter(f.col("order_id").isNull()).count()
        print(f"    Null location_id: {null_location:,} ({100*null_location/total:.1f}%)")
        print(f"    Null body:        {null_body:,} ({100*null_body/total:.1f}%)")
        print(f"    Null order_id:    {null_order:,} ({100*null_order/total:.1f}%)")

        # Parse a JSON body to show the richness
        print("  Sample event body (order_created):")
        events_df.filter(
            (f.col("event_type") == "order_created") & f.col("body").isNotNull()
        ).select("body").limit(1).show(truncate=False)

    except Exception as e:
        print(f"  Error reading events: {e}")

    # ─── Iceberg Tables (if catalog configured) ────────────────
    section("ICEBERG TABLES (if loaded)")

    iceberg_tables = [
        "iceberg.bronze.orders",
        "iceberg.bronze.dim_locations",
        "iceberg.bronze.dim_brands",
        "iceberg.bronze.dim_items",
        "iceberg.bronze.dim_categories",
    ]

    for table in iceberg_tables:
        try:
            count = spark.sql(f"SELECT COUNT(*) FROM {table}").collect()[0][0]
            print(f"  {table}: {count:,} rows")
        except Exception:
            print(f"  {table}: not found (run ./lakehouse testdata load)")

    # ─── The Point ──────────────────────────────────────────────
    section("THE POINT")
    print("""
  This data was created on any Spark cluster. It lives in:
    - Parquet files (open columnar format, readable by anything)
    - Iceberg tables (open table format, ACID, time travel)

  No Databricks. No vendor lock-in. No proprietary formats.
  Just open standards on object storage.

  Tools that can read this right now:
    - Apache Spark (any version >= 3.x)
    - DuckDB
    - Trino / Presto
    - Dremio
    - Snowflake (external tables)
    - Polars
    - pandas + pyarrow

  Next: we need a CATALOG to govern this data.
    """)

    print("=" * 60)
    print("  Act 1 complete.")
    print("=" * 60 + "\n")
    spark.stop()


if __name__ == "__main__":
    main()
