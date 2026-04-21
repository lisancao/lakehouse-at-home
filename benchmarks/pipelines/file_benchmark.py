"""File-based pipeline benchmark: Imperative vs Declarative.

Uses file streaming (no external dependencies) to compare approaches.
Good for local testing without Kafka.
"""

import json
import time
import uuid
import tempfile
import os
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
)


@dataclass
class PipelineResult:
    """Result of a pipeline benchmark run."""

    pipeline_type: str
    source: str
    rows_processed: int
    duration_seconds: float
    throughput_rows_per_sec: float
    batches_processed: int
    timestamp: datetime = field(default_factory=lambda: datetime.now(timezone.utc))
    run_id: str = ""

    def to_dict(self) -> Dict[str, Any]:
        return {
            "pipeline_type": self.pipeline_type,
            "source": self.source,
            "rows_processed": self.rows_processed,
            "duration_seconds": round(self.duration_seconds, 4),
            "throughput_rows_per_sec": round(self.throughput_rows_per_sec, 2),
            "batches_processed": self.batches_processed,
            "timestamp": self.timestamp.isoformat(),
            "run_id": self.run_id,
        }


EVENT_SCHEMA = StructType([
    StructField("event_id", StringType(), False),
    StructField("event_type", StringType(), False),
    StructField("ts", StringType(), False),
    StructField("ts_seconds", LongType(), False),
    StructField("order_id", StringType(), False),
    StructField("location_id", IntegerType(), True),
    StructField("sequence", IntegerType(), False),
    StructField("body", StringType(), True),
])


class FilePipelineBenchmark:
    """Benchmark file-based pipelines: imperative vs declarative."""

    def __init__(
        self,
        spark: SparkSession,
        output_dir: Optional[str] = None,
    ):
        self.spark = spark
        self.output_dir = output_dir or tempfile.mkdtemp(prefix="file_bench_")
        self.run_id = str(uuid.uuid4())[:8]
        self.input_dir = os.path.join(self.output_dir, "input")

        os.makedirs(self.input_dir, exist_ok=True)
        os.makedirs(self.output_dir, exist_ok=True)

    def generate_test_data(self, row_count: int, batch_count: int = 5) -> None:
        """Generate test data files for streaming."""
        rows_per_batch = row_count // batch_count
        base_ts = int(time.time())

        event_types = [
            "order_created", "kitchen_started", "kitchen_finished",
            "order_ready", "driver_arrived", "driver_picked_up", "delivered"
        ]

        for batch in range(batch_count):
            rows = []
            for i in range(rows_per_batch):
                idx = batch * rows_per_batch + i
                order_id = f"ORD{idx // 7:06d}"
                event_type = event_types[idx % 7]
                ts_seconds = base_ts + (idx * 10)

                rows.append({
                    "event_id": f"evt_{uuid.uuid4().hex[:8]}",
                    "event_type": event_type,
                    "ts": datetime.fromtimestamp(ts_seconds).isoformat(),
                    "ts_seconds": ts_seconds,
                    "order_id": order_id,
                    "location_id": (idx % 4) + 1,
                    "sequence": idx % 7,
                    "body": json.dumps({"brand_id": (idx % 10) + 1, "total": 25.99}),
                })

            # Write batch as JSON
            df = self.spark.createDataFrame(rows, EVENT_SCHEMA)
            batch_path = os.path.join(self.input_dir, f"batch_{batch:03d}")
            df.write.mode("overwrite").json(batch_path)

        print(f"Generated {row_count} rows in {batch_count} batches")

    def run_imperative_pipeline(self, duration_seconds: int = 30) -> PipelineResult:
        """Run imperative-style batch pipeline (not streaming for reliability)."""
        print(f"\n{'='*60}")
        print("IMPERATIVE PIPELINE (Traditional PySpark)")
        print(f"{'='*60}")

        output_path = os.path.join(self.output_dir, "imperative_output")

        batch_times = []
        row_counts = []

        start_time = time.time()

        # Process each batch file imperatively
        batch_dirs = sorted([
            d for d in os.listdir(self.input_dir)
            if os.path.isdir(os.path.join(self.input_dir, d))
        ])

        for batch_id, batch_dir in enumerate(batch_dirs):
            batch_start = time.time()
            batch_path = os.path.join(self.input_dir, batch_dir)

            # Read batch
            batch_df = self.spark.read.schema(EVENT_SCHEMA).json(batch_path)

            # Imperative style: explicit step-by-step transformations

            # Step 1: Parse timestamp
            step1 = batch_df.withColumn(
                "event_timestamp",
                f.to_timestamp(f.regexp_replace("ts", "T", " "))
            )

            # Step 2: Add derived columns
            step2 = step1.withColumns({
                "event_hour": f.hour("event_timestamp"),
                "event_date": f.to_date("event_timestamp"),
                "is_weekend": f.when(
                    f.dayofweek("event_timestamp").isin(1, 7), True
                ).otherwise(False),
            })

            # Step 3: Parse JSON body
            body_schema = StructType([
                StructField("brand_id", IntegerType(), True),
                StructField("total", LongType(), True),
            ])
            step3 = step2.withColumn(
                "body_parsed", f.from_json("body", body_schema)
            ).withColumn(
                "brand_id", f.col("body_parsed.brand_id")
            ).withColumn(
                "order_total", f.col("body_parsed.total")
            ).drop("body_parsed")

            # Step 4: Write output (imperative - explicit write)
            step3.write.mode("append").parquet(output_path)

            count = step3.count()
            row_counts.append(count)
            batch_times.append(time.time() - batch_start)

            print(f"  Batch {batch_id}: {count} rows, {batch_times[-1]:.2f}s")

        total_time = time.time() - start_time
        total_rows = sum(row_counts)

        return PipelineResult(
            pipeline_type="imperative",
            source="file",
            rows_processed=total_rows,
            duration_seconds=total_time,
            throughput_rows_per_sec=total_rows / total_time if total_time > 0 else 0,
            batches_processed=len(batch_times),
            run_id=self.run_id,
        )

    def run_declarative_pipeline(self, duration_seconds: int = 30) -> PipelineResult:
        """Run declarative-style batch pipeline."""
        print(f"\n{'='*60}")
        print("DECLARATIVE PIPELINE (SDP-Style)")
        print(f"{'='*60}")

        output_path = os.path.join(self.output_dir, "declarative_output")

        batch_times = []
        row_counts = []

        # Declarative style: Pure transformation functions that return DataFrames

        def bronze_events(raw_df: DataFrame) -> DataFrame:
            """Bronze layer: Add event timestamp."""
            return raw_df.withColumn(
                "event_timestamp",
                f.to_timestamp(f.regexp_replace("ts", "T", " "))
            )

        def silver_events_enriched(bronze_df: DataFrame) -> DataFrame:
            """Silver layer: Add time features and parse body."""
            body_schema = StructType([
                StructField("brand_id", IntegerType(), True),
                StructField("total", LongType(), True),
            ])

            return bronze_df.withColumns({
                "event_hour": f.hour("event_timestamp"),
                "event_date": f.to_date("event_timestamp"),
                "is_weekend": f.when(
                    f.dayofweek("event_timestamp").isin(1, 7), True
                ).otherwise(False),
                "body_parsed": f.from_json("body", body_schema),
            }).withColumns({
                "brand_id": f.col("body_parsed.brand_id"),
                "order_total": f.col("body_parsed.total"),
            }).drop("body_parsed")

        start_time = time.time()

        # Process each batch file declaratively
        batch_dirs = sorted([
            d for d in os.listdir(self.input_dir)
            if os.path.isdir(os.path.join(self.input_dir, d))
        ])

        for batch_id, batch_dir in enumerate(batch_dirs):
            batch_start = time.time()
            batch_path = os.path.join(self.input_dir, batch_dir)

            # Read batch
            batch_df = self.spark.read.schema(EVENT_SCHEMA).json(batch_path)

            # Declarative: Compose transformations (single pipeline)
            result = silver_events_enriched(bronze_events(batch_df))

            # Single materialization point
            result.write.mode("append").parquet(output_path)

            count = result.count()
            row_counts.append(count)
            batch_times.append(time.time() - batch_start)

            print(f"  Batch {batch_id}: {count} rows, {batch_times[-1]:.2f}s")

        total_time = time.time() - start_time
        total_rows = sum(row_counts)

        return PipelineResult(
            pipeline_type="declarative",
            source="file",
            rows_processed=total_rows,
            duration_seconds=total_time,
            throughput_rows_per_sec=total_rows / total_time if total_time > 0 else 0,
            batches_processed=len(batch_times),
            run_id=self.run_id,
        )

    def run_comparison(
        self,
        row_count: int = 10000,
        batch_count: int = 5,
    ) -> Dict[str, PipelineResult]:
        """Run both pipelines and compare results."""
        print(f"\n{'='*70}")
        print("FILE PIPELINE BENCHMARK: IMPERATIVE vs DECLARATIVE")
        print(f"{'='*70}")
        print(f"Run ID: {self.run_id}")
        print(f"Total rows: {row_count}")
        print(f"Batches: {batch_count}")

        # Generate test data
        print("\nGenerating test data...")
        self.generate_test_data(row_count, batch_count)

        results = {}

        # Run imperative
        results["imperative"] = self.run_imperative_pipeline()

        # Clean input for second run (re-generate to reset stream)
        import shutil
        shutil.rmtree(self.input_dir, ignore_errors=True)
        os.makedirs(self.input_dir, exist_ok=True)
        self.generate_test_data(row_count, batch_count)

        # Run declarative
        results["declarative"] = self.run_declarative_pipeline()

        # Print comparison
        self._print_comparison(results)

        return results

    def _print_comparison(self, results: Dict[str, PipelineResult]) -> None:
        """Print comparison table."""
        print(f"\n{'='*70}")
        print("BENCHMARK RESULTS COMPARISON")
        print(f"{'='*70}")

        print(f"\n{'Metric':<25} {'Imperative':<20} {'Declarative':<20}")
        print("-" * 70)

        imp = results.get("imperative")
        dec = results.get("declarative")

        if imp and dec:
            print(f"{'Rows Processed':<25} {imp.rows_processed:<20,} {dec.rows_processed:<20,}")
            print(f"{'Duration (s)':<25} {imp.duration_seconds:<20.2f} {dec.duration_seconds:<20.2f}")
            print(f"{'Throughput (rows/s)':<25} {imp.throughput_rows_per_sec:<20.1f} {dec.throughput_rows_per_sec:<20.1f}")
            print(f"{'Batches Processed':<25} {imp.batches_processed:<20} {dec.batches_processed:<20}")

            if imp.throughput_rows_per_sec > 0 and dec.throughput_rows_per_sec > 0:
                if imp.throughput_rows_per_sec > dec.throughput_rows_per_sec:
                    speedup = imp.throughput_rows_per_sec / dec.throughput_rows_per_sec
                    print(f"\nImperative is {speedup:.2f}x faster")
                elif dec.throughput_rows_per_sec > imp.throughput_rows_per_sec:
                    speedup = dec.throughput_rows_per_sec / imp.throughput_rows_per_sec
                    print(f"\nDeclarative is {speedup:.2f}x faster")
                else:
                    print("\nBoth pipelines have equal throughput")

    def cleanup(self) -> None:
        """Clean up benchmark artifacts."""
        import shutil
        if os.path.exists(self.output_dir):
            shutil.rmtree(self.output_dir, ignore_errors=True)
