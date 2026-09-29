"""Shared helpers for the Spark 4.3 Real-Time Mode (RTM) experiments.

Every experiment reads the quickstart producer's JSON events from Kafka and
writes JSON with a ``produced_at`` field back to Kafka, so the same
``latency_consumer.py`` measures end-to-end latency for all of them
(consumer wall clock minus ``produced_at``).

Environment:
    KAFKA_BOOTSTRAP_SERVERS  default localhost:9092
    RTM_CHECKPOINT_ROOT      default /tmp/rtm43-checkpoints
    RTM_BATCH_DURATION       default "5 minutes" (RTM checkpoint interval)
    RTM_CONTROL_MICROBATCH   if set, use processingTime="1 second" instead (control runs)
"""

import os
import shutil

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import col, from_json
from pyspark.sql.types import IntegerType, LongType, StringType, StructField, StructType

KAFKA_BOOTSTRAP = os.environ.get("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
CHECKPOINT_ROOT = os.environ.get("RTM_CHECKPOINT_ROOT", "/tmp/rtm43-checkpoints")
BATCH_DURATION = os.environ.get("RTM_BATCH_DURATION", "5 minutes")

# Same schema as the 4.1 quickstart producer (data_generator/producer.py).
EVENT_SCHEMA = StructType(
    [
        StructField("event_id", StringType()),
        StructField("event_type", StringType()),
        StructField("key", StringType()),
        StructField("value", IntegerType()),
        StructField("produced_at", LongType()),
    ]
)


def session(app_name: str, **conf: str) -> SparkSession:
    builder = SparkSession.builder.appName(app_name)
    for k, v in conf.items():
        builder = builder.config(k, v)
    spark = builder.getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    return spark


def read_events(spark: SparkSession, topic: str = "rtm-input") -> DataFrame:
    """Kafka source, parsed into the quickstart event columns."""
    raw = (
        spark.readStream.format("kafka")
        .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP)
        .option("subscribe", topic)
        .option("startingOffsets", "latest")
        .load()
    )
    return (
        raw.selectExpr("CAST(value AS STRING) AS json_str")
        .select(from_json(col("json_str"), EVENT_SCHEMA).alias("event"))
        .select("event.*")
    )


def fresh_checkpoint(name: str) -> str:
    path = os.path.join(CHECKPOINT_ROOT, name)
    shutil.rmtree(path, ignore_errors=True)
    return path


def start_rtm_to_kafka(df: DataFrame, topic: str, name: str, **options: str):
    """Kafka sink with the native PySpark real-time trigger (PySpark 4.2+)."""
    writer = (
        df.writeStream.queryName(name)
        .format("kafka")
        .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP)
        .option("topic", topic)
        .option("checkpointLocation", fresh_checkpoint(name))
        .outputMode("update")
    )
    for k, v in options.items():
        writer = writer.option(k, v)
    if os.environ.get("RTM_CONTROL_MICROBATCH"):
        # Control run: same query, plain micro-batch trigger.
        return writer.trigger(processingTime="1 second").start()
    return writer.trigger(realTime=BATCH_DURATION).start()
