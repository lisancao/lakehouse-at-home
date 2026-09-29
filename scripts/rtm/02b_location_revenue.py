"""Experiment b2: stateful RTM on the ghost-kitchen order stream.

Reads the lakehouse test-data stream (``./lakehouse testdata stream --topic orders``),
drops duplicate events by event_id (the generator's chaos injects ~2%), and keeps
running revenue per location from order_created events.

    orders -> dropDuplicates(event_id) -> filter(order_created)
           -> groupBy(location_id).agg(count, sum(total)) -> Kafka rtm-location-revenue

The Kafka record timestamp of the input (set by the producer at send time) is
carried as produced_at so latency_consumer.py can measure it.
"""

import os

from pyspark.sql.functions import (
    col,
    count,
    from_json,
    get_json_object,
    struct,
    to_json,
)
from pyspark.sql.functions import max as max_
from pyspark.sql.functions import round as round_
from pyspark.sql.functions import sum as sum_
from pyspark.sql.types import IntegerType, StringType, StructField, StructType
from rtm_common import KAFKA_BOOTSTRAP, session, start_rtm_to_kafka

SHUFFLE = os.environ.get("RTM_SHUFFLE_PARTITIONS", "4")
ORDER_SCHEMA = StructType(
    [
        StructField("event_id", StringType()),
        StructField("event_type", StringType()),
        StructField("location_id", IntegerType()),
        StructField("order_id", StringType()),
        StructField("body", StringType()),
    ]
)

spark = session(
    "rtm43-b2-location-revenue", **{"spark.sql.shuffle.partitions": SHUFFLE}
)

orders = (
    spark.readStream.format("kafka")
    .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP)
    .option("subscribe", os.environ.get("ORDERS_TOPIC", "orders"))
    .option("startingOffsets", "latest")
    .load()
    .select(
        from_json(col("value").cast("string"), ORDER_SCHEMA).alias("e"),
        (col("timestamp").cast("double") * 1000).cast("long").alias("produced_at"),
    )
    .select("e.*", "produced_at")
)

revenue = (
    orders.dropDuplicates(["event_id"])
    .where(col("event_type") == "order_created")
    .withColumn("total", get_json_object("body", "$.total").cast("double"))
    .groupBy("location_id")
    .agg(
        count("*").alias("orders"),
        round_(sum_("total"), 2).alias("revenue"),
        max_("produced_at").alias("produced_at"),
    )
)

out = revenue.select(
    col("location_id").cast("string").alias("key"),
    to_json(struct("location_id", "orders", "revenue", "produced_at")).alias("value"),
)

q = start_rtm_to_kafka(out, "rtm-location-revenue", "b2_location_revenue")
print(f"[b2] location revenue RTM running, {SHUFFLE} shuffle partitions", flush=True)
q.awaitTermination()
