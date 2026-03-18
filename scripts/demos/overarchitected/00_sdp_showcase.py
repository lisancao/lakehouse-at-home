#!/usr/bin/env python3
"""
OverArchitected Demo 0: Spark Declarative Pipelines (SDP) Showcase
==================================================================

THE headline demo. Shows the paradigm shift from imperative to declarative
pipeline development in Spark 4.1.

Three acts:
  1. "The Old Way" — imperative pipeline with manual ordering
  2. "The New Way" — SDP with @dp.materialized_view, auto-dependency resolution
  3. "Live Extension" — add a new gold table with zero execution logic changes

This script simulates SDP behavior for live demo purposes. The real SDP pipeline
lives at scripts/pipelines/pipeline_sdp.py and runs via:
    spark-pipelines run --spec scripts/pipelines/spark-pipeline.yml

Run:
    docker exec spark-master-41 /opt/spark/bin/spark-submit \
        /scripts/demos/overarchitected/00_sdp_showcase.py

Prerequisites:
    ./lakehouse start all
    ./lakehouse testdata generate --days 7
    ./lakehouse testdata load
"""

from pyspark.sql import SparkSession
from pyspark.sql import functions as f
from pyspark.sql.types import (
    StructType, StructField, StringType, IntegerType, DoubleType,
)
from functools import wraps
from typing import Callable, Dict, List, Set
import re
import textwrap


# =============================================================================
# ACT 1: "The Old Way" (Imperative)
# =============================================================================

def act1_imperative(spark):
    """Show the imperative approach — manual ordering, explicit writes."""
    print("\n" + "=" * 70)
    print("  ACT 1: THE OLD WAY (Imperative)")
    print("  You manage execution order. You write to tables. You handle deps.")
    print("=" * 70)

    print(textwrap.dedent("""
    # What imperative code looks like:
    #
    #   def load_orders(spark):
    #       df = spark.read.parquet("/data/orders.parquet")
    #       df.write.mode("overwrite").saveAsTable("bronze.orders")  # explicit write
    #
    #   def enrich_orders(spark):
    #       orders = spark.table("bronze.orders")      # must run AFTER load_orders
    #       locations = spark.table("dim_locations")
    #       enriched = orders.join(locations, "location_id")
    #       enriched.write.mode("overwrite").saveAsTable("silver.orders")  # explicit write
    #
    #   # YOU manage execution order:
    #   load_orders(spark)      # 1st
    #   enrich_orders(spark)    # 2nd — wrong order = crash
    #   daily_metrics(spark)    # 3rd
    """))

    df = spark.read.parquet("/data/events/orders_90d.parquet").limit(2000)
    df = df.withColumn("event_timestamp", f.to_timestamp(f.regexp_replace("ts", "T", " ")))

    spark.sql("CREATE NAMESPACE IF NOT EXISTS iceberg.demo_imperative")
    spark.sql("DROP TABLE IF EXISTS iceberg.demo_imperative.bronze_orders")
    df.write.mode("overwrite").saveAsTable("iceberg.demo_imperative.bronze_orders")

    count = spark.table("iceberg.demo_imperative.bronze_orders").count()
    print(f"  [imperative] Wrote {count:,} rows to bronze_orders")
    print("  Problems: manual ordering, explicit writes, fragile dependencies\n")


# =============================================================================
# ACT 2: "The New Way" (Spark Declarative Pipelines)
# =============================================================================

class DeclarativePipeline:
    """Mini SDP framework for live demo. Production uses: from pyspark import pipelines as dp"""

    def __init__(self, name: str, catalog: str = "iceberg"):
        self.name = name
        self.catalog = catalog
        self.tables: Dict[str, dict] = {}
        self._spark: SparkSession = None

    def set_spark(self, spark: SparkSession):
        self._spark = spark

    @property
    def spark(self) -> SparkSession:
        return self._spark

    def materialized_view(self, name: str):
        """Register a table definition — just like @dp.materialized_view."""
        def decorator(func: Callable):
            import inspect
            source = inspect.getsource(func)
            deps = set(re.findall(r'spark\.table\(["\']([^"\']+)["\']\)', source))

            self.tables[name] = {'func': func, 'deps': deps}

            @wraps(func)
            def wrapper():
                return func()
            return wrapper
        return decorator

    def _topo_sort(self) -> List[str]:
        visited: Set[str] = set()
        order: List[str] = []

        def visit(name: str):
            if name in visited:
                return
            visited.add(name)
            if name in self.tables:
                for dep in self.tables[name]['deps']:
                    short = dep.replace(f"{self.catalog}.", "")
                    visit(short)
            order.append(name)

        for t in self.tables:
            visit(t)
        return order

    def run(self) -> Dict[str, int]:
        results = {}
        order = self._topo_sort()

        print(f"\n  Execution order (AUTO-RESOLVED): {order}")
        print(f"  Tables registered: {len(self.tables)}")

        for name in order:
            if name not in self.tables:
                continue
            info = self.tables[name]
            full_name = f"{self.catalog}.{name}"
            deps = info['deps']

            layer = name.split(".")[0].upper()
            dep_str = f" <- {deps}" if deps else " (no deps)"
            print(f"    [{layer}] {name}{dep_str}")

            df = info['func']()
            self._spark.sql(f"CREATE NAMESPACE IF NOT EXISTS {self.catalog}.{name.split('.')[0]}")
            self._spark.sql(f"DROP TABLE IF EXISTS {full_name}")
            df.write.mode("overwrite").saveAsTable(full_name)

            count = self._spark.sql(f"SELECT COUNT(*) FROM {full_name}").collect()[0][0]
            results[name] = count
            print(f"           -> {count:,} rows")

        return results


pipeline = DeclarativePipeline("overarchitected_sdp", catalog="iceberg")


def act2_declarative(spark):
    """Show the SDP approach — define WHAT, Spark handles WHEN and HOW."""
    print("\n" + "=" * 70)
    print("  ACT 2: THE NEW WAY (Spark Declarative Pipelines)")
    print("  Define WHAT each table contains. Spark handles execution order.")
    print("=" * 70)

    print(textwrap.dedent("""
    # What SDP code looks like:
    #
    #   from pyspark import pipelines as dp
    #
    #   @dp.materialized_view(name="bronze.orders")
    #   def orders():
    #       return spark.read.parquet("/data/orders.parquet")  # just RETURN
    #
    #   @dp.materialized_view(name="silver.orders_enriched")
    #   def orders_enriched():
    #       orders = spark.table("iceberg.bronze.orders")       # dep auto-detected!
    #       locations = spark.table("iceberg.bronze.dim_locations")
    #       return orders.join(broadcast(locations), "location_id")  # just RETURN
    #
    #   # Run: spark-pipelines run --spec pipeline.yml
    #   # No manual ordering. No explicit writes. Framework handles everything.
    """))

    pipeline.set_spark(spark)

    # Register tables — ORDER DOES NOT MATTER (that's the point)
    @pipeline.materialized_view(name="bronze.dim_locations")
    def dim_locations():
        return spark.read.parquet("/data/dimensions/locations.parquet")

    @pipeline.materialized_view(name="bronze.orders")
    def orders():
        df = spark.read.parquet("/data/events/orders_90d.parquet").limit(3000)
        return df.withColumn("event_timestamp", f.to_timestamp(f.regexp_replace("ts", "T", " ")))

    body_schema = StructType([
        StructField("brand_id", IntegerType(), True),
        StructField("total", DoubleType(), True),
        StructField("driver_id", StringType(), True),
    ])

    @pipeline.materialized_view(name="silver.orders_enriched")
    def orders_enriched():
        orders = spark.table("iceberg.bronze.orders")
        locations = spark.table("iceberg.bronze.dim_locations").select(
            f.col("id").alias("location_id"), f.col("city").alias("city_name")
        )

        enriched = orders.withColumn("body_parsed", f.from_json("body", body_schema))
        enriched = enriched.select(
            "*",
            f.col("body_parsed.brand_id").alias("brand_id"),
            f.col("body_parsed.total").alias("order_total"),
        ).drop("body_parsed")
        enriched = enriched.withColumns({
            "event_hour": f.hour("event_timestamp"),
            "event_date": f.to_date("event_timestamp"),
        })
        return enriched.join(f.broadcast(locations), on="location_id", how="left")

    @pipeline.materialized_view(name="gold.hourly_metrics")
    def hourly_metrics():
        orders = spark.table("iceberg.silver.orders_enriched")
        return orders.filter(f.col("event_type") == "order_created").groupBy(
            "event_date", "event_hour", "city_name"
        ).agg(
            f.count("order_id").alias("order_count"),
            f.sum("order_total").alias("total_revenue"),
            f.avg("order_total").alias("avg_order_value"),
        )

    print("  Running pipeline (framework resolves order automatically)...\n")
    results = pipeline.run()

    print(f"\n  Pipeline complete: {len(results)} tables, zero manual ordering")
    print("\n  Sample gold.hourly_metrics:")
    spark.table("iceberg.gold.hourly_metrics").orderBy(f.desc("total_revenue")).show(5, truncate=False)


# =============================================================================
# ACT 3: "Live Extension" (Add a table — no execution logic changes)
# =============================================================================

def act3_live_extension(spark):
    """The money shot: add a new gold table live. Zero changes to execution logic."""
    print("\n" + "=" * 70)
    print("  ACT 3: LIVE EXTENSION")
    print("  Add a new gold table. No changes to pipeline runner. Just define it.")
    print("=" * 70)

    @pipeline.materialized_view(name="gold.city_leaderboard")
    def city_leaderboard():
        """Which city orders the most? Added live on the show."""
        orders = spark.table("iceberg.silver.orders_enriched")
        return orders.filter(f.col("event_type") == "order_created").groupBy(
            "city_name"
        ).agg(
            f.count("order_id").alias("total_orders"),
            f.sum("order_total").alias("total_revenue"),
            f.avg("order_total").alias("avg_order_value"),
        ).orderBy(f.desc("total_orders"))

    print("\n  Registered gold.city_leaderboard — re-running pipeline...\n")
    results = pipeline.run()

    print(f"\n  Pipeline complete: {len(results)} tables (was 4, now 5)")
    print("\n  NEW TABLE — gold.city_leaderboard:")
    spark.table("iceberg.gold.city_leaderboard").show(10, truncate=False)

    print(textwrap.dedent("""
    KEY TAKEAWAY:
      - Imperative: add table → update execution order → test order → pray
      - SDP: add @dp.materialized_view → done. Framework handles the rest.

    REAL SDP COMMANDS:
      spark-pipelines dry-run --spec pipeline.yml   # validate
      spark-pipelines run --spec pipeline.yml        # execute
      spark-pipelines graph --spec pipeline.yml      # visualize DAG
    """))


# =============================================================================
# MAIN
# =============================================================================

def main():
    spark = SparkSession.builder.appName("OverArchitected-00-SDP-Showcase").getOrCreate()
    spark.sparkContext.setLogLevel("WARN")

    print("\n" + "#" * 70)
    print("#  SPARK DECLARATIVE PIPELINES (SDP) — THE HEADLINE FEATURE")
    print(f"#  Spark {spark.version}")
    print("#" * 70)

    act1_imperative(spark)
    act2_declarative(spark)
    act3_live_extension(spark)

    print("\n" + "#" * 70)
    print("#  SDP Showcase complete!")
    print("#  Real pipeline: scripts/pipelines/pipeline_sdp.py")
    print("#  Real config:   scripts/pipelines/spark-pipeline.yml")
    print("#" * 70 + "\n")
    spark.stop()


if __name__ == "__main__":
    main()
