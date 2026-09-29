"""Experiment c: dropDuplicates in RTM (new in 4.3).

Kafka rtm-input -> dropDuplicates("event_id") -> Kafka rtm-dedup.
Run the producer with --dup-rate so it re-sends some events with the same
event_id. Count distinct vs total on rtm-dedup afterwards to check that no
duplicate made it through.
"""

import os

from pyspark.sql.functions import struct, to_json
from rtm_common import BATCH_DURATION, read_events, session, start_rtm_to_kafka

SHUFFLE = os.environ.get("RTM_SHUFFLE_PARTITIONS", "16")
spark = session("rtm43-c-dedup", **{"spark.sql.shuffle.partitions": SHUFFLE})

deduped = read_events(spark).dropDuplicates(["event_id"])
out = deduped.select(
    deduped.key.alias("key"),
    to_json(struct("event_id", "key", "event_type", "produced_at")).alias("value"),
)

q = start_rtm_to_kafka(out, "rtm-dedup", "c_dedup")
print(
    f"[c] dropDuplicates RTM running, {SHUFFLE} shuffle partitions, "
    f"batch duration {BATCH_DURATION}",
    flush=True,
)
q.awaitTermination()
