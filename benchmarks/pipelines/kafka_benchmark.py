"""Kafka pipeline benchmark: Imperative vs Declarative.

Compares two approaches for Kafka streaming pipelines:
1. Imperative: Traditional PySpark with explicit write statements
2. Declarative: SDP-style with decorators and returned DataFrames

Metrics:
- End-to-end latency (message publish to processed)
- Throughput (messages/second)
- Processing time per batch
- Resource utilization (if available)
"""

import json
import time
import uuid
import tempfile
import os
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Dict, List, Optional, Any
from threading import Thread

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as f
from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    IntegerType,
    LongType,
    TimestampType,
)


@dataclass
class PipelineResult:
    """Result of a pipeline benchmark run."""

    pipeline_type: str  # "imperative" or "declarative"
    source: str  # "kafka"
    messages_processed: int
    duration_seconds: float
    throughput_msg_per_sec: float
    avg_latency_ms: float
    p95_latency_ms: float
    batches_processed: int
    timestamp: datetime = field(default_factory=lambda: datetime.now(timezone.utc))
    run_id: str = ""

    def to_dict(self) -> Dict[str, Any]:
        return {
            "pipeline_type": self.pipeline_type,
            "source": self.source,
            "messages_processed": self.messages_processed,
            "duration_seconds": round(self.duration_seconds, 4),
            "throughput_msg_per_sec": round(self.throughput_msg_per_sec, 2),
            "avg_latency_ms": round(self.avg_latency_ms, 2),
            "p95_latency_ms": round(self.p95_latency_ms, 2),
            "batches_processed": self.batches_processed,
            "timestamp": self.timestamp.isoformat(),
            "run_id": self.run_id,
        }


# Event schema for Kafka messages
EVENT_SCHEMA = StructType([
    StructField("event_id", StringType(), False),
    StructField("event_type", StringType(), False),
    StructField("ts", StringType(), False),
    StructField("ts_seconds", LongType(), False),
    StructField("order_id", StringType(), False),
    StructField("location_id", IntegerType(), True),
    StructField("sequence", IntegerType(), False),
    StructField("body", StringType(), True),
    StructField("benchmark_ts", LongType(), True),  # For latency measurement
])


class KafkaPipelineBenchmark:
    """Benchmark Kafka pipelines: imperative vs declarative."""

    def __init__(
        self,
        spark: SparkSession,
        kafka_bootstrap_servers: str = "localhost:9092",
        topic: str = "benchmark_orders",
        output_dir: Optional[str] = None,
    ):
        self.spark = spark
        self.kafka_bootstrap_servers = kafka_bootstrap_servers
        self.topic = topic
        self.output_dir = output_dir or tempfile.mkdtemp(prefix="kafka_bench_")
        self.run_id = str(uuid.uuid4())[:8]

        os.makedirs(self.output_dir, exist_ok=True)

    def generate_test_messages(self, count: int) -> List[Dict]:
        """Generate test messages for Kafka."""
        messages = []
        base_ts = int(time.time())

        event_types = [
            "order_created", "kitchen_started", "kitchen_finished",
            "order_ready", "driver_arrived", "driver_picked_up", "delivered"
        ]

        for i in range(count):
            order_id = f"ORD{i // 7:06d}"
            event_type = event_types[i % 7]
            ts_seconds = base_ts + (i * 10)

            msg = {
                "event_id": f"evt_{uuid.uuid4().hex[:8]}",
                "event_type": event_type,
                "ts": datetime.fromtimestamp(ts_seconds).isoformat(),
                "ts_seconds": ts_seconds,
                "order_id": order_id,
                "location_id": (i % 4) + 1,
                "sequence": i % 7,
                "body": json.dumps({"brand_id": (i % 10) + 1, "total": 25.99}),
                "benchmark_ts": int(time.time() * 1000),  # Publish timestamp ms
            }
            messages.append(msg)

        return messages

    def publish_to_kafka(self, messages: List[Dict]) -> None:
        """Publish messages to Kafka topic."""
        from kafka import KafkaProducer

        producer = KafkaProducer(
            bootstrap_servers=self.kafka_bootstrap_servers,
            value_serializer=lambda v: json.dumps(v).encode("utf-8"),
        )

        for msg in messages:
            msg["benchmark_ts"] = int(time.time() * 1000)  # Update publish time
            producer.send(self.topic, value=msg)

        producer.flush()
        producer.close()

    def run_imperative_pipeline(
        self,
        duration_seconds: int = 30,
        trigger_interval: str = "5 seconds",
    ) -> PipelineResult:
        """Run imperative-style Kafka pipeline."""
        print(f"\n{'='*60}")
        print("IMPERATIVE PIPELINE (Traditional PySpark)")
        print(f"{'='*60}")

        output_path = os.path.join(self.output_dir, "imperative_output")
        checkpoint_path = os.path.join(self.output_dir, "imperative_checkpoint")

        # Metrics collection
        batch_times = []
        message_counts = []
        latencies = []

        def process_batch(batch_df: DataFrame, batch_id: int) -> None:
            """Process a micro-batch imperatively."""
            batch_start = time.time()

            if batch_df.isEmpty():
                return

            # Parse Kafka value
            parsed = batch_df.select(
                f.from_json(f.col("value").cast("string"), EVENT_SCHEMA).alias("event"),
                f.col("timestamp").alias("kafka_timestamp"),
            ).select("event.*", "kafka_timestamp")

            # Add processing timestamp and calculate latency
            processed = parsed.withColumn(
                "process_ts", f.lit(int(time.time() * 1000))
            ).withColumn(
                "latency_ms", f.col("process_ts") - f.col("benchmark_ts")
            )

            # Transform: Add derived columns (imperative style - explicit transformations)
            enriched = processed.withColumns({
                "event_timestamp": f.to_timestamp(f.regexp_replace("ts", "T", " ")),
                "event_hour": f.hour(f.to_timestamp(f.regexp_replace("ts", "T", " "))),
            })

            # Write output (imperative - explicit write)
            enriched.write.mode("append").parquet(output_path)

            # Collect metrics
            count = enriched.count()
            message_counts.append(count)
            batch_times.append(time.time() - batch_start)

            # Collect latencies
            lat_stats = enriched.agg(
                f.avg("latency_ms").alias("avg_lat"),
                f.percentile_approx("latency_ms", 0.95).alias("p95_lat"),
            ).collect()[0]

            if lat_stats["avg_lat"]:
                latencies.append((lat_stats["avg_lat"], lat_stats["p95_lat"]))

            print(f"  Batch {batch_id}: {count} messages, {batch_times[-1]:.2f}s")

        # Create streaming query
        kafka_df = (
            self.spark.readStream
            .format("kafka")
            .option("kafka.bootstrap.servers", self.kafka_bootstrap_servers)
            .option("subscribe", self.topic)
            .option("startingOffsets", "latest")
            .load()
        )

        query = (
            kafka_df.writeStream
            .foreachBatch(process_batch)
            .option("checkpointLocation", checkpoint_path)
            .trigger(processingTime=trigger_interval)
            .start()
        )

        print(f"Running for {duration_seconds} seconds...")
        start_time = time.time()

        try:
            query.awaitTermination(timeout=duration_seconds * 1000)
        except Exception:
            pass
        finally:
            query.stop()

        total_time = time.time() - start_time
        total_messages = sum(message_counts)

        avg_latency = sum(l[0] for l in latencies) / len(latencies) if latencies else 0
        p95_latency = max(l[1] for l in latencies) if latencies else 0

        return PipelineResult(
            pipeline_type="imperative",
            source="kafka",
            messages_processed=total_messages,
            duration_seconds=total_time,
            throughput_msg_per_sec=total_messages / total_time if total_time > 0 else 0,
            avg_latency_ms=avg_latency,
            p95_latency_ms=p95_latency,
            batches_processed=len(batch_times),
            run_id=self.run_id,
        )

    def run_declarative_pipeline(
        self,
        duration_seconds: int = 30,
        trigger_interval: str = "5 seconds",
    ) -> PipelineResult:
        """Run declarative-style Kafka pipeline."""
        print(f"\n{'='*60}")
        print("DECLARATIVE PIPELINE (SDP-Style)")
        print(f"{'='*60}")

        output_path = os.path.join(self.output_dir, "declarative_output")
        checkpoint_path = os.path.join(self.output_dir, "declarative_checkpoint")

        # Metrics collection
        batch_times = []
        message_counts = []
        latencies = []

        # Declarative style: Define transformations as pure functions
        # that return DataFrames (no side effects)

        def bronze_orders(kafka_df: DataFrame) -> DataFrame:
            """Bronze layer: Parse raw Kafka messages."""
            return kafka_df.select(
                f.from_json(f.col("value").cast("string"), EVENT_SCHEMA).alias("event"),
                f.col("timestamp").alias("kafka_timestamp"),
            ).select("event.*", "kafka_timestamp")

        def silver_orders_enriched(bronze_df: DataFrame) -> DataFrame:
            """Silver layer: Enrich orders with derived columns."""
            return bronze_df.withColumns({
                "event_timestamp": f.to_timestamp(f.regexp_replace("ts", "T", " ")),
                "event_hour": f.hour(f.to_timestamp(f.regexp_replace("ts", "T", " "))),
                "process_ts": f.lit(int(time.time() * 1000)),
            }).withColumn(
                "latency_ms", f.col("process_ts") - f.col("benchmark_ts")
            )

        def process_batch_declarative(batch_df: DataFrame, batch_id: int) -> None:
            """Process batch using declarative composition."""
            batch_start = time.time()

            if batch_df.isEmpty():
                return

            # Declarative: Compose transformations
            bronze = bronze_orders(batch_df)
            silver = silver_orders_enriched(bronze)

            # Final write (single point of materialization)
            silver.write.mode("append").parquet(output_path)

            # Collect metrics
            count = silver.count()
            message_counts.append(count)
            batch_times.append(time.time() - batch_start)

            lat_stats = silver.agg(
                f.avg("latency_ms").alias("avg_lat"),
                f.percentile_approx("latency_ms", 0.95).alias("p95_lat"),
            ).collect()[0]

            if lat_stats["avg_lat"]:
                latencies.append((lat_stats["avg_lat"], lat_stats["p95_lat"]))

            print(f"  Batch {batch_id}: {count} messages, {batch_times[-1]:.2f}s")

        # Create streaming query
        kafka_df = (
            self.spark.readStream
            .format("kafka")
            .option("kafka.bootstrap.servers", self.kafka_bootstrap_servers)
            .option("subscribe", self.topic)
            .option("startingOffsets", "latest")
            .load()
        )

        query = (
            kafka_df.writeStream
            .foreachBatch(process_batch_declarative)
            .option("checkpointLocation", checkpoint_path)
            .trigger(processingTime=trigger_interval)
            .start()
        )

        print(f"Running for {duration_seconds} seconds...")
        start_time = time.time()

        try:
            query.awaitTermination(timeout=duration_seconds * 1000)
        except Exception:
            pass
        finally:
            query.stop()

        total_time = time.time() - start_time
        total_messages = sum(message_counts)

        avg_latency = sum(l[0] for l in latencies) / len(latencies) if latencies else 0
        p95_latency = max(l[1] for l in latencies) if latencies else 0

        return PipelineResult(
            pipeline_type="declarative",
            source="kafka",
            messages_processed=total_messages,
            duration_seconds=total_time,
            throughput_msg_per_sec=total_messages / total_time if total_time > 0 else 0,
            avg_latency_ms=avg_latency,
            p95_latency_ms=p95_latency,
            batches_processed=len(batch_times),
            run_id=self.run_id,
        )

    def run_comparison(
        self,
        message_count: int = 1000,
        duration_seconds: int = 30,
        trigger_interval: str = "5 seconds",
    ) -> Dict[str, PipelineResult]:
        """Run both pipelines and compare results."""
        print(f"\n{'='*70}")
        print("KAFKA PIPELINE BENCHMARK: IMPERATIVE vs DECLARATIVE")
        print(f"{'='*70}")
        print(f"Run ID: {self.run_id}")
        print(f"Topic: {self.topic}")
        print(f"Messages to publish: {message_count}")
        print(f"Duration: {duration_seconds}s per pipeline")

        # Generate test messages
        print("\nGenerating test messages...")
        messages = self.generate_test_messages(message_count)

        # Start message publisher in background
        def publish_messages():
            time.sleep(2)  # Let pipelines start
            self.publish_to_kafka(messages)
            print(f"\nPublished {len(messages)} messages to Kafka")

        results = {}

        # Run imperative pipeline
        publisher = Thread(target=publish_messages)
        publisher.start()
        results["imperative"] = self.run_imperative_pipeline(
            duration_seconds=duration_seconds,
            trigger_interval=trigger_interval,
        )
        publisher.join()

        # Wait for cleanup
        time.sleep(5)

        # Run declarative pipeline
        publisher = Thread(target=publish_messages)
        publisher.start()
        results["declarative"] = self.run_declarative_pipeline(
            duration_seconds=duration_seconds,
            trigger_interval=trigger_interval,
        )
        publisher.join()

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
            print(f"{'Messages Processed':<25} {imp.messages_processed:<20,} {dec.messages_processed:<20,}")
            print(f"{'Duration (s)':<25} {imp.duration_seconds:<20.2f} {dec.duration_seconds:<20.2f}")
            print(f"{'Throughput (msg/s)':<25} {imp.throughput_msg_per_sec:<20.1f} {dec.throughput_msg_per_sec:<20.1f}")
            print(f"{'Avg Latency (ms)':<25} {imp.avg_latency_ms:<20.1f} {dec.avg_latency_ms:<20.1f}")
            print(f"{'P95 Latency (ms)':<25} {imp.p95_latency_ms:<20.1f} {dec.p95_latency_ms:<20.1f}")
            print(f"{'Batches Processed':<25} {imp.batches_processed:<20} {dec.batches_processed:<20}")

            # Determine winner
            if imp.throughput_msg_per_sec > dec.throughput_msg_per_sec:
                speedup = imp.throughput_msg_per_sec / dec.throughput_msg_per_sec
                print(f"\nImperative is {speedup:.1f}x faster in throughput")
            elif dec.throughput_msg_per_sec > imp.throughput_msg_per_sec:
                speedup = dec.throughput_msg_per_sec / imp.throughput_msg_per_sec
                print(f"\nDeclarative is {speedup:.1f}x faster in throughput")
            else:
                print("\nBoth pipelines have equal throughput")

    def cleanup(self) -> None:
        """Clean up benchmark artifacts."""
        import shutil
        if os.path.exists(self.output_dir):
            shutil.rmtree(self.output_dir, ignore_errors=True)
