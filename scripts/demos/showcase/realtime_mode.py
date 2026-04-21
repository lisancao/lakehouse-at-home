#!/usr/bin/env python3
"""
Real-Time Mode (RTM) + SDP Streaming
==============================================================

streaming in Spark 4.1 Structured Streaming. 

Demonstrates:
  1. BEFORE: Micro-batch with trigger(processingTime='10 seconds') — fixed scheduling
  2. AFTER: Real-Time Mode with trigger(realTime='5 minutes') — sub-second latency
  3. Latency comparison via Foreach sink that prints metrics
  4. SDP + Streaming: how @dp.table() works with streaming sources

RTM in OSS Spark 4.1: stateless, single-stage, Kafka source, Kafka/Foreach sinks.
Python support may be limited — includes fallback to processingTime if RTM unavailable.

Run:
    docker exec spark-master-41 /opt/spark/bin/spark-submit \
        --packages org.apache.spark:spark-sql-kafka-0-10_2.13:4.1.0 \
        /scripts/demos/showcase/realtime_mode.py

Prerequisites:
    ./lakehouse start all
    ./lakehouse testdata load
    ./lakehouse producer   # In another terminal — produces to Kafka "orders" topic
"""

from pyspark.sql import SparkSession
from pyspark.sql import functions as f
from pyspark.sql.types import (
    StructType, StructField, StringType, IntegerType,
)

# Order event schema (ghost kitchen food delivery)
ORDER_SCHEMA = StructType([
    StructField("event_id", StringType()),
    StructField("event_type", StringType()),
    StructField("ts", StringType()),
    StructField("order_id", StringType()),
    StructField("location_id", IntegerType()),
    StructField("sequence", IntegerType()),
    StructField("body", StringType()),
])

KAFKA_BOOTSTRAP = "localhost:9092"
KAFKA_TOPIC = "orders"
CHECKPOINT_MICRO = "/tmp/checkpoints/rtm_demo_micro"
CHECKPOINT_RTM = "/tmp/checkpoints/rtm_demo_rtm"


class LatencyMetricsWriter:
    """ForeachWriter that prints latency metrics for live demo."""

    def __init__(self, mode_name: str):
        self.mode_name = mode_name
        self.count = 0
        self.latencies_ms = []

    def open(self, partition_id, epoch_id):
        return True

    def process(self, row):
        self.count += 1
        try:
            from datetime import datetime
            proc_ts = datetime.now()
            event_ts = row["event_timestamp"]
            if event_ts:
                latency_ms = (proc_ts - event_ts).total_seconds() * 1000
                self.latencies_ms.append(latency_ms)
                if self.count <= 5 or self.count % 10 == 0:
                    print(f"  [{self.mode_name}] row {self.count}: order_id={row['order_id']} "
                          f"event_type={row['event_type']} latency={latency_ms:.0f}ms")
        except Exception as e:
            print(f"  [{self.mode_name}] row {self.count}: (latency calc skipped: {e})")

    def close(self, error):
        if error:
            print(f"  [{self.mode_name}] ForeachWriter closed with error: {error}")
        elif self.latencies_ms:
            avg = sum(self.latencies_ms) / len(self.latencies_ms)
            p99 = sorted(self.latencies_ms)[int(len(self.latencies_ms) * 0.99)] if len(self.latencies_ms) > 10 else max(self.latencies_ms)
            print(f"  [{self.mode_name}] Batch complete: {self.count} rows, avg_latency={avg:.0f}ms, p99≈{p99:.0f}ms")


def create_kafka_stream(spark):
    """Create Kafka readStream for orders topic."""
    return (
        spark.readStream.format("kafka")
        .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP)
        .option("subscribe", KAFKA_TOPIC)
        .option("startingOffsets", "latest")
        .option("failOnDataLoss", "false")
        .load()
    )


def parse_and_add_latency(stream_df):
    """Parse JSON, add event_timestamp and latency placeholder."""
    parsed = (
        stream_df.select(
            f.from_json(f.col("value").cast("string"), ORDER_SCHEMA).alias("data")
        )
        .select("data.*")
        .withColumn("event_timestamp", f.to_timestamp(f.regexp_replace("ts", "T", " ")))
    )
    return parsed


def run_micro_batch_demo(spark, run_seconds: int = 15):
    """BEFORE: Micro-batch with processingTime trigger."""
    print("\n" + "=" * 70)
    print("  BEFORE: MICRO-BATCH MODE")
    print("  trigger(processingTime='10 seconds') — fixed scheduling, higher latency")
    print("=" * 70)

    stream = create_kafka_stream(spark)
    parsed = parse_and_add_latency(stream)

    writer = LatencyMetricsWriter("micro-batch")
    try:
        query = (
            parsed.writeStream
            .foreach(writer)
            .outputMode("update")
            .option("checkpointLocation", CHECKPOINT_MICRO)
            .trigger(processingTime="10 seconds")
            .start()
        )
        print("  Started micro-batch query. Waiting {} seconds for events...".format(run_seconds))
        print("  (Ensure ./lakehouse producer is running in another terminal)")
        query.awaitTermination(run_seconds)
        query.stop()
    except Exception as e:
        print(f"  Micro-batch demo error: {e}")
        raise


def run_realtime_mode_demo(spark, run_seconds: int = 15):
    """AFTER: Real-Time Mode with realTime trigger (or fallback)."""
    print("\n" + "=" * 70)
    print("  AFTER: REAL-TIME MODE (RTM)")
    print("  trigger(realTime='5 minutes') — sub-second latency, streaming shuffle")
    print("=" * 70)

    stream = create_kafka_stream(spark)
    parsed = parse_and_add_latency(stream)

    writer = LatencyMetricsWriter("RTM")
    rtm_available = False

    try:
        # Try RTM trigger first (OSS Spark 4.1 may not have it)
        query = (
            parsed.writeStream
            .foreach(writer)
            .outputMode("update")
            .option("checkpointLocation", CHECKPOINT_RTM)
            .trigger(realTime="5 minutes")
            .start()
        )
        rtm_available = True
        print("  Real-Time Mode trigger accepted! Running RTM query...")
    except (TypeError, AttributeError) as e:
        print("  Real-Time Mode trigger not available in this Spark build.")
        print("  (RTM is GA in Databricks Runtime 16.4+; OSS Spark 4.1 has limited support)")
        print("  Falling back to trigger(processingTime='1 second') for demo.")
        query = (
            parsed.writeStream
            .foreach(writer)
            .outputMode("update")
            .option("checkpointLocation", CHECKPOINT_RTM)
            .trigger(processingTime="1 second")
            .start()
        )

    print("  Waiting {} seconds for events...".format(run_seconds))
    try:
        query.awaitTermination(run_seconds)
    finally:
        query.stop()

    return rtm_available


def main():
    spark = (
        SparkSession.builder
        .appName("lakehouse-demo")
        .config("spark.sql.streaming.schemaInference", "true")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")

    print("\n" + "#" * 70)
    print("#  REAL-TIME MODE (RTM) — CO-HEADLINER WITH SDP")
    print("#  Spark 4.1 Structured Streaming: micro-batch vs real-time")
    print("#" * 70)
    print(f"\n  Spark version: {spark.version}")
    print("  Kafka: {} topic '{}'".format(KAFKA_BOOTSTRAP, KAFKA_TOPIC))

    # Act 1: Micro-batch (BEFORE)
    run_micro_batch_demo(spark, run_seconds=12)

    # Act 2: Real-Time Mode (AFTER)
    rtm_worked = run_realtime_mode_demo(spark, run_seconds=12)

    # Summary
    print("\n" + "=" * 70)
    print("  SUMMARY")
    print("=" * 70)
    print("  Micro-batch: Fixed 10s intervals. Latency = scheduling delay + processing.")
    print("  Real-Time Mode: Events processed as they arrive. p99 in single-digit ms.")
    if rtm_worked:
        print("  RTM enabled! Same API, one line change: .trigger(realTime='5 minutes')")
    else:
        print("  RTM not in this build. On Databricks Runtime 16.4+: full RTM support.")
    print("  Flink killer: RTM outperformed Flink by up to 92% in benchmarks.")
    print("  SDP connection: In SDP, streaming is @dp.table. With RTM, that runs sub-second.")
    print("=" * 70)
    print("\n" + "#" * 70)
    print("#  RTM Demo complete!")
    print("#" * 70 + "\n")

    spark.stop()


if __name__ == "__main__":
    main()
