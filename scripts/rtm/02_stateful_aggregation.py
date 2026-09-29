"""Experiment b: stateful RTM (new in 4.3, SPARK-58635): running aggregate per key.

Kafka rtm-input -> groupBy(key).agg(count, sum(value), max(produced_at)) -> Kafka rtm-agg.
Output mode update emits each key as it changes. max(produced_at) is the
timestamp of the newest event folded into the row, so the latency consumer
measures "newest input -> updated aggregate visible downstream".

Needs source partitions + shuffle partitions free cores (16 + 16 here).
"""

import os

from pyspark.sql.functions import count, struct, to_json
from pyspark.sql.functions import max as max_
from pyspark.sql.functions import sum as sum_
from rtm_common import BATCH_DURATION, read_events, session, start_rtm_to_kafka

SHUFFLE = os.environ.get("RTM_SHUFFLE_PARTITIONS", "16")
spark = session("rtm43-b-aggregation", **{"spark.sql.shuffle.partitions": SHUFFLE})

agg = (
    read_events(spark)
    .groupBy("key")
    .agg(
        count("*").alias("events"),
        sum_("value").alias("total_value"),
        max_("produced_at").alias("produced_at"),
    )
)

out = agg.select(
    agg.key.alias("key"),
    to_json(struct("key", "events", "total_value", "produced_at")).alias("value"),
)

q = start_rtm_to_kafka(out, "rtm-agg", "b_aggregation")
print(
    f"[b] stateful aggregation RTM running, {SHUFFLE} shuffle partitions, "
    f"batch duration {BATCH_DURATION}",
    flush=True,
)
q.awaitTermination()
