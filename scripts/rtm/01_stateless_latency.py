"""Experiment a: stateless RTM, same query as the 4.1 quickstart's pipeline 05.

Kafka rtm-input -> from_json -> to_json(key, event_type, produced_at) -> Kafka rtm-latency.
The only change from 4.1 is the trigger: 4.1 needed a Py4J shim
(RealTimeTrigger.apply via the JVM gateway); 4.2+ has trigger(realTime=...).
"""

from pyspark.sql.functions import col, struct, to_json
from rtm_common import BATCH_DURATION, read_events, session, start_rtm_to_kafka

spark = session("rtm43-a-stateless", **{"spark.sql.shuffle.partitions": "20"})

out = read_events(spark).select(
    col("key"),
    to_json(struct("key", "event_type", "produced_at")).alias("value"),
)

q = start_rtm_to_kafka(out, "rtm-latency", "a_stateless")
print(f"[a] stateless RTM running, batch duration {BATCH_DURATION}", flush=True)
q.awaitTermination()
