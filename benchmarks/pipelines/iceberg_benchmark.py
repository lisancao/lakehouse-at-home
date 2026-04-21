"""Iceberg pipeline benchmark: Imperative vs Declarative.

Compares READ and WRITE operations for Iceberg tables using:
1. Imperative: Traditional PySpark with explicit write statements
2. Declarative: SDP-style with pure transformation functions

Uses a local Hadoop catalog (no external dependencies).
"""

import json
import time
import uuid
import tempfile
import os
import shutil
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Dict, List, Optional, Any

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as f
from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    IntegerType,
    LongType,
    DoubleType,
)


@dataclass
class IcebergBenchmarkResult:
    """Result of an Iceberg benchmark run."""

    pipeline_type: str  # "imperative" or "declarative"
    operation: str  # "read" or "write"
    rows_processed: int
    duration_seconds: float
    throughput_rows_per_sec: float
    file_size_bytes: int = 0
    tables_created: int = 0
    timestamp: datetime = field(default_factory=lambda: datetime.now(timezone.utc))
    run_id: str = ""

    def to_dict(self) -> Dict[str, Any]:
        return {
            "pipeline_type": self.pipeline_type,
            "operation": self.operation,
            "rows_processed": self.rows_processed,
            "duration_seconds": round(self.duration_seconds, 4),
            "throughput_rows_per_sec": round(self.throughput_rows_per_sec, 2),
            "file_size_bytes": self.file_size_bytes,
            "tables_created": self.tables_created,
            "timestamp": self.timestamp.isoformat(),
            "run_id": self.run_id,
        }


# Schema for order events
ORDER_SCHEMA = StructType([
    StructField("event_id", StringType(), False),
    StructField("event_type", StringType(), False),
    StructField("ts", StringType(), False),
    StructField("ts_seconds", LongType(), False),
    StructField("order_id", StringType(), False),
    StructField("location_id", IntegerType(), True),
    StructField("sequence", IntegerType(), False),
    StructField("body", StringType(), True),
])


class IcebergPipelineBenchmark:
    """Benchmark Iceberg/Parquet pipelines: imperative vs declarative.

    Uses Parquet tables locally (no Iceberg JARs needed).
    Use --use-iceberg when running in Docker with proper JARs.
    """

    def __init__(
        self,
        spark: SparkSession,
        warehouse_dir: Optional[str] = None,
        use_iceberg: bool = False,
    ):
        self.warehouse_dir = warehouse_dir or tempfile.mkdtemp(prefix="table_bench_")
        self.run_id = str(uuid.uuid4())[:8]
        self.use_iceberg = use_iceberg
        self.catalog_name = "bench_catalog"
        self.namespace = "benchmark"
        self.spark = spark

        os.makedirs(self.warehouse_dir, exist_ok=True)

        if use_iceberg:
            self._configure_iceberg()

    def _configure_iceberg(self) -> None:
        """Configure Spark session for Iceberg with Hadoop catalog."""
        try:
            self.spark.conf.set(
                f"spark.sql.catalog.{self.catalog_name}",
                "org.apache.iceberg.spark.SparkCatalog"
            )
            self.spark.conf.set(
                f"spark.sql.catalog.{self.catalog_name}.type",
                "hadoop"
            )
            self.spark.conf.set(
                f"spark.sql.catalog.{self.catalog_name}.warehouse",
                self.warehouse_dir
            )
            self.spark.sql(f"CREATE NAMESPACE IF NOT EXISTS {self.catalog_name}.{self.namespace}")
        except Exception as e:
            print(f"Warning: Iceberg not available, falling back to Parquet: {e}")
            self.use_iceberg = False

    def _get_table_name(self, name: str) -> str:
        """Get fully qualified table name (for Iceberg) or path (for Parquet)."""
        clean_name = name.replace("-", "_").replace(".", "_")
        if self.use_iceberg:
            return f"{self.catalog_name}.{self.namespace}.{clean_name}"
        return clean_name  # Just the name for Parquet

    def _get_table_path(self, name: str) -> str:
        """Get filesystem path for table."""
        clean_name = name.replace("-", "_").replace(".", "_")
        return os.path.join(self.warehouse_dir, clean_name)

    def _write_table(self, df: DataFrame, name: str) -> None:
        """Write DataFrame to table (Iceberg or Parquet)."""
        if self.use_iceberg:
            table_name = self._get_table_name(name)
            df.writeTo(table_name).using("iceberg").createOrReplace()
        else:
            path = self._get_table_path(name)
            df.write.mode("overwrite").parquet(path)

    def _read_table(self, name: str) -> DataFrame:
        """Read table (Iceberg or Parquet)."""
        if self.use_iceberg:
            return self.spark.table(self._get_table_name(name))
        else:
            return self.spark.read.parquet(self._get_table_path(name))

    def _count_table(self, name: str) -> int:
        """Count rows in table."""
        return self._read_table(name).count()

    def _get_dir_size(self, path: str) -> int:
        """Get total size of directory."""
        if not os.path.exists(path):
            return 0
        total = 0
        for dirpath, _, filenames in os.walk(path):
            for fname in filenames:
                total += os.path.getsize(os.path.join(dirpath, fname))
        return total

    def generate_test_data(self, row_count: int) -> DataFrame:
        """Generate test data for benchmarks."""
        base_ts = int(time.time())
        event_types = [
            "order_created", "kitchen_started", "kitchen_finished",
            "order_ready", "driver_arrived", "driver_picked_up", "delivered"
        ]

        rows = []
        for i in range(row_count):
            order_id = f"ORD{i // 7:06d}"
            event_type = event_types[i % 7]
            ts_seconds = base_ts + (i * 10)

            rows.append({
                "event_id": f"evt_{uuid.uuid4().hex[:8]}",
                "event_type": event_type,
                "ts": datetime.fromtimestamp(ts_seconds).isoformat(),
                "ts_seconds": ts_seconds,
                "order_id": order_id,
                "location_id": (i % 4) + 1,
                "sequence": i % 7,
                "body": json.dumps({"brand_id": (i % 10) + 1, "total": 25.99 + (i % 50)}),
            })

        return self.spark.createDataFrame(rows, ORDER_SCHEMA)

    # =========================================================================
    # WRITE BENCHMARKS
    # =========================================================================

    def run_imperative_write(self, df: DataFrame, table_suffix: str = "imp") -> IcebergBenchmarkResult:
        """Run imperative-style write pipeline."""
        print(f"\n{'='*60}")
        print(f"IMPERATIVE WRITE PIPELINE ({'Iceberg' if self.use_iceberg else 'Parquet'})")
        print(f"{'='*60}")

        tables_created = 0
        start_time = time.time()

        # Bronze: Raw events with timestamp parsing
        bronze_name = f"bronze_orders_{table_suffix}"
        print(f"\n  Writing bronze table: {bronze_name}")

        bronze_df = df.withColumn(
            "event_timestamp",
            f.to_timestamp(f.regexp_replace("ts", "T", " "))
        )
        self._write_table(bronze_df, bronze_name)
        tables_created += 1
        bronze_count = self._count_table(bronze_name)
        print(f"    -> {bronze_count:,} rows")

        # Silver: Enriched with parsed body and time features
        silver_name = f"silver_orders_{table_suffix}"
        print(f"\n  Writing silver table: {silver_name}")

        # Read from bronze (imperative - explicit dependency)
        bronze_data = self._read_table(bronze_name)

        # Parse body
        body_schema = StructType([
            StructField("brand_id", IntegerType(), True),
            StructField("total", DoubleType(), True),
        ])

        silver_df = bronze_data.withColumn(
            "body_parsed", f.from_json("body", body_schema)
        ).withColumns({
            "brand_id": f.col("body_parsed.brand_id"),
            "order_total": f.col("body_parsed.total"),
            "event_hour": f.hour("event_timestamp"),
            "event_date": f.to_date("event_timestamp"),
        }).drop("body_parsed")

        self._write_table(silver_df, silver_name)
        tables_created += 1
        silver_count = self._count_table(silver_name)
        print(f"    -> {silver_count:,} rows")

        # Gold: Aggregations
        gold_name = f"gold_hourly_{table_suffix}"
        print(f"\n  Writing gold table: {gold_name}")

        # Read from silver (imperative - explicit dependency)
        silver_data = self._read_table(silver_name)

        gold_df = silver_data.filter(
            f.col("event_type") == "order_created"
        ).groupBy(
            "event_date", "event_hour", "location_id"
        ).agg(
            f.count("order_id").alias("order_count"),
            f.sum("order_total").alias("total_revenue"),
            f.avg("order_total").alias("avg_order_value"),
        )

        self._write_table(gold_df, gold_name)
        tables_created += 1
        gold_count = self._count_table(gold_name)
        print(f"    -> {gold_count:,} rows")

        total_time = time.time() - start_time
        total_rows = bronze_count + silver_count + gold_count

        # Get file sizes
        file_size = sum(
            self._get_dir_size(self._get_table_path(t))
            for t in [bronze_name, silver_name, gold_name]
        )

        return IcebergBenchmarkResult(
            pipeline_type="imperative",
            operation="write",
            rows_processed=total_rows,
            duration_seconds=total_time,
            throughput_rows_per_sec=total_rows / total_time if total_time > 0 else 0,
            file_size_bytes=file_size,
            tables_created=tables_created,
            run_id=self.run_id,
        )

    def run_declarative_write(self, df: DataFrame, table_suffix: str = "dec") -> IcebergBenchmarkResult:
        """Run declarative-style write pipeline."""
        print(f"\n{'='*60}")
        print(f"DECLARATIVE WRITE PIPELINE ({'Iceberg' if self.use_iceberg else 'Parquet'})")
        print(f"{'='*60}")

        # Declarative: Define pure transformation functions

        def bronze_orders(raw_df: DataFrame) -> DataFrame:
            """Bronze layer: Parse timestamps."""
            return raw_df.withColumn(
                "event_timestamp",
                f.to_timestamp(f.regexp_replace("ts", "T", " "))
            )

        def silver_orders_enriched(bronze_df: DataFrame) -> DataFrame:
            """Silver layer: Enrich with parsed body and time features."""
            body_schema = StructType([
                StructField("brand_id", IntegerType(), True),
                StructField("total", DoubleType(), True),
            ])

            return bronze_df.withColumn(
                "body_parsed", f.from_json("body", body_schema)
            ).withColumns({
                "brand_id": f.col("body_parsed.brand_id"),
                "order_total": f.col("body_parsed.total"),
                "event_hour": f.hour("event_timestamp"),
                "event_date": f.to_date("event_timestamp"),
            }).drop("body_parsed")

        def gold_hourly_metrics(silver_df: DataFrame) -> DataFrame:
            """Gold layer: Hourly aggregations."""
            return silver_df.filter(
                f.col("event_type") == "order_created"
            ).groupBy(
                "event_date", "event_hour", "location_id"
            ).agg(
                f.count("order_id").alias("order_count"),
                f.sum("order_total").alias("total_revenue"),
                f.avg("order_total").alias("avg_order_value"),
            )

        tables_created = 0
        start_time = time.time()

        # Compose pipeline declaratively
        bronze_df = bronze_orders(df)
        silver_df = silver_orders_enriched(bronze_df)
        gold_df = gold_hourly_metrics(silver_df)

        # Materialize all tables
        bronze_name = f"bronze_orders_{table_suffix}"
        silver_name = f"silver_orders_{table_suffix}"
        gold_name = f"gold_hourly_{table_suffix}"

        print(f"\n  Writing bronze table: {bronze_name}")
        self._write_table(bronze_df, bronze_name)
        tables_created += 1
        bronze_count = self._count_table(bronze_name)
        print(f"    -> {bronze_count:,} rows")

        print(f"\n  Writing silver table: {silver_name}")
        self._write_table(silver_df, silver_name)
        tables_created += 1
        silver_count = self._count_table(silver_name)
        print(f"    -> {silver_count:,} rows")

        print(f"\n  Writing gold table: {gold_name}")
        self._write_table(gold_df, gold_name)
        tables_created += 1
        gold_count = self._count_table(gold_name)
        print(f"    -> {gold_count:,} rows")

        total_time = time.time() - start_time
        total_rows = bronze_count + silver_count + gold_count

        file_size = sum(
            self._get_dir_size(self._get_table_path(t))
            for t in [bronze_name, silver_name, gold_name]
        )

        return IcebergBenchmarkResult(
            pipeline_type="declarative",
            operation="write",
            rows_processed=total_rows,
            duration_seconds=total_time,
            throughput_rows_per_sec=total_rows / total_time if total_time > 0 else 0,
            file_size_bytes=file_size,
            tables_created=tables_created,
            run_id=self.run_id,
        )

    # =========================================================================
    # READ BENCHMARKS
    # =========================================================================

    def run_imperative_read(self, table_suffix: str = "imp") -> IcebergBenchmarkResult:
        """Run imperative-style read pipeline."""
        print(f"\n{'='*60}")
        print(f"IMPERATIVE READ PIPELINE ({'Iceberg' if self.use_iceberg else 'Parquet'})")
        print(f"{'='*60}")

        start_time = time.time()
        total_rows = 0

        # Read bronze
        bronze_name = f"bronze_orders_{table_suffix}"
        print(f"\n  Reading bronze table: {bronze_name}")
        bronze_df = self._read_table(bronze_name)
        bronze_count = bronze_df.count()
        total_rows += bronze_count
        print(f"    -> {bronze_count:,} rows")

        # Read silver with filter (imperative - step by step)
        silver_name = f"silver_orders_{table_suffix}"
        print(f"\n  Reading silver table with filter: {silver_name}")
        silver_df = self._read_table(silver_name)
        filtered_silver = silver_df.filter(f.col("event_type") == "order_created")
        silver_count = filtered_silver.count()
        total_rows += silver_count
        print(f"    -> {silver_count:,} rows (order_created only)")

        # Read gold with aggregation
        gold_name = f"gold_hourly_{table_suffix}"
        print(f"\n  Reading gold table with aggregation: {gold_name}")
        gold_df = self._read_table(gold_name)
        summary = gold_df.agg(
            f.sum("order_count").alias("total_orders"),
            f.sum("total_revenue").alias("total_revenue"),
        ).collect()[0]
        gold_count = gold_df.count()
        total_rows += gold_count
        print(f"    -> {gold_count:,} rows, {summary['total_orders']} orders, ${summary['total_revenue']:.2f} revenue")

        total_time = time.time() - start_time

        return IcebergBenchmarkResult(
            pipeline_type="imperative",
            operation="read",
            rows_processed=total_rows,
            duration_seconds=total_time,
            throughput_rows_per_sec=total_rows / total_time if total_time > 0 else 0,
            run_id=self.run_id,
        )

    def run_declarative_read(self, table_suffix: str = "dec") -> IcebergBenchmarkResult:
        """Run declarative-style read pipeline."""
        print(f"\n{'='*60}")
        print(f"DECLARATIVE READ PIPELINE ({'Iceberg' if self.use_iceberg else 'Parquet'})")
        print(f"{'='*60}")

        # Declarative: Define read operations as composable functions
        table_suffix_local = table_suffix

        def read_bronze() -> DataFrame:
            """Read bronze orders."""
            return self._read_table(f"bronze_orders_{table_suffix_local}")

        def read_silver_filtered() -> DataFrame:
            """Read silver orders filtered to order_created."""
            return self._read_table(f"silver_orders_{table_suffix_local}").filter(
                f.col("event_type") == "order_created"
            )

        def read_gold_summary() -> DataFrame:
            """Read gold with summary aggregation."""
            return self._read_table(f"gold_hourly_{table_suffix_local}").agg(
                f.sum("order_count").alias("total_orders"),
                f.sum("total_revenue").alias("total_revenue"),
            )

        start_time = time.time()
        total_rows = 0

        # Compose reads
        print(f"\n  Reading bronze table")
        bronze_df = read_bronze()
        bronze_count = bronze_df.count()
        total_rows += bronze_count
        print(f"    -> {bronze_count:,} rows")

        print(f"\n  Reading silver table (filtered)")
        silver_df = read_silver_filtered()
        silver_count = silver_df.count()
        total_rows += silver_count
        print(f"    -> {silver_count:,} rows")

        print(f"\n  Reading gold summary")
        gold_summary = read_gold_summary().collect()[0]
        gold_count = self._read_table(f"gold_hourly_{table_suffix}").count()
        total_rows += gold_count
        print(f"    -> {gold_count:,} rows, {gold_summary['total_orders']} orders, ${gold_summary['total_revenue']:.2f} revenue")

        total_time = time.time() - start_time

        return IcebergBenchmarkResult(
            pipeline_type="declarative",
            operation="read",
            rows_processed=total_rows,
            duration_seconds=total_time,
            throughput_rows_per_sec=total_rows / total_time if total_time > 0 else 0,
            run_id=self.run_id,
        )

    # =========================================================================
    # RUN FULL COMPARISON
    # =========================================================================

    def run_comparison(self, row_count: int = 10000) -> Dict[str, IcebergBenchmarkResult]:
        """Run full comparison: imperative vs declarative for read and write."""
        table_format = "Iceberg" if self.use_iceberg else "Parquet"
        print(f"\n{'='*70}")
        print(f"{table_format.upper()} PIPELINE BENCHMARK: IMPERATIVE vs DECLARATIVE")
        print(f"{'='*70}")
        print(f"Run ID: {self.run_id}")
        print(f"Table Format: {table_format}")
        print(f"Warehouse: {self.warehouse_dir}")
        print(f"Row count: {row_count:,}")

        # Generate test data
        print("\nGenerating test data...")
        df = self.generate_test_data(row_count)
        df.cache()
        df.count()  # Materialize
        print(f"Generated {row_count:,} rows")

        results = {}

        # Write benchmarks
        results["imperative_write"] = self.run_imperative_write(df, "imp")
        results["declarative_write"] = self.run_declarative_write(df, "dec")

        # Read benchmarks (after tables exist)
        results["imperative_read"] = self.run_imperative_read("imp")
        results["declarative_read"] = self.run_declarative_read("dec")

        df.unpersist()

        # Print comparison
        self._print_comparison(results)

        return results

    def _print_comparison(self, results: Dict[str, IcebergBenchmarkResult]) -> None:
        """Print comparison table."""
        print(f"\n{'='*70}")
        print("BENCHMARK RESULTS COMPARISON")
        print(f"{'='*70}")

        # Write comparison
        print(f"\n{'WRITE OPERATIONS':-^70}")
        print(f"\n{'Metric':<25} {'Imperative':<20} {'Declarative':<20}")
        print("-" * 70)

        imp_w = results.get("imperative_write")
        dec_w = results.get("declarative_write")

        if imp_w and dec_w:
            print(f"{'Rows Processed':<25} {imp_w.rows_processed:<20,} {dec_w.rows_processed:<20,}")
            print(f"{'Duration (s)':<25} {imp_w.duration_seconds:<20.2f} {dec_w.duration_seconds:<20.2f}")
            print(f"{'Throughput (rows/s)':<25} {imp_w.throughput_rows_per_sec:<20.1f} {dec_w.throughput_rows_per_sec:<20.1f}")
            print(f"{'File Size (MB)':<25} {imp_w.file_size_bytes/1024/1024:<20.2f} {dec_w.file_size_bytes/1024/1024:<20.2f}")
            print(f"{'Tables Created':<25} {imp_w.tables_created:<20} {dec_w.tables_created:<20}")

            if dec_w.throughput_rows_per_sec > imp_w.throughput_rows_per_sec:
                speedup = dec_w.throughput_rows_per_sec / imp_w.throughput_rows_per_sec
                print(f"\nWrite: Declarative is {speedup:.2f}x faster")
            else:
                speedup = imp_w.throughput_rows_per_sec / dec_w.throughput_rows_per_sec
                print(f"\nWrite: Imperative is {speedup:.2f}x faster")

        # Read comparison
        print(f"\n{'READ OPERATIONS':-^70}")
        print(f"\n{'Metric':<25} {'Imperative':<20} {'Declarative':<20}")
        print("-" * 70)

        imp_r = results.get("imperative_read")
        dec_r = results.get("declarative_read")

        if imp_r and dec_r:
            print(f"{'Rows Processed':<25} {imp_r.rows_processed:<20,} {dec_r.rows_processed:<20,}")
            print(f"{'Duration (s)':<25} {imp_r.duration_seconds:<20.2f} {dec_r.duration_seconds:<20.2f}")
            print(f"{'Throughput (rows/s)':<25} {imp_r.throughput_rows_per_sec:<20.1f} {dec_r.throughput_rows_per_sec:<20.1f}")

            if dec_r.throughput_rows_per_sec > imp_r.throughput_rows_per_sec:
                speedup = dec_r.throughput_rows_per_sec / imp_r.throughput_rows_per_sec
                print(f"\nRead: Declarative is {speedup:.2f}x faster")
            else:
                speedup = imp_r.throughput_rows_per_sec / dec_r.throughput_rows_per_sec
                print(f"\nRead: Imperative is {speedup:.2f}x faster")

    def cleanup(self) -> None:
        """Clean up benchmark artifacts."""
        if os.path.exists(self.warehouse_dir):
            shutil.rmtree(self.warehouse_dir, ignore_errors=True)
