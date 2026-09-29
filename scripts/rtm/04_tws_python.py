"""Experiment d (Python): transformWithState from PySpark under an RTM trigger.

The 4.3 RTM docs say only the JVM (Scala/Java) transformWithState runs in
Real-Time Mode; transformWithState and transformWithStateInPandas from
PySpark are not supported. This script tries both and prints the exact error.

    spark-submit 04_tws_python.py pandas   # transformWithStateInPandas
    spark-submit 04_tws_python.py row      # transformWithState (Row API)
"""

import sys

import pandas as pd
from pyspark.sql import Row
from pyspark.sql.streaming import StatefulProcessor, StatefulProcessorHandle
from pyspark.sql.types import LongType, StringType, StructField, StructType
from rtm_common import read_events, session, start_rtm_to_kafka

MODE = sys.argv[1] if len(sys.argv) > 1 else "pandas"
OUT_SCHEMA = StructType(
    [
        StructField("key", StringType()),
        StructField("value", StringType()),
    ]
)
STATE_SCHEMA = StructType([StructField("total", LongType())])


class RunningTotalPandas(StatefulProcessor):
    def init(self, handle: StatefulProcessorHandle) -> None:
        self.total = handle.getValueState("total", STATE_SCHEMA)

    def handleInputRows(self, key, rows, timerValues):
        prev = self.total.get()[0] if self.total.exists() else 0
        new = prev + sum(int(pdf["value"].sum()) for pdf in rows)
        self.total.update((new,))
        yield pd.DataFrame({"key": [key[0]], "value": [str(new)]})

    def close(self) -> None:
        pass


class RunningTotalRow(StatefulProcessor):
    def init(self, handle: StatefulProcessorHandle) -> None:
        self.total = handle.getValueState("total", STATE_SCHEMA)

    def handleInputRows(self, key, rows, timerValues):
        prev = self.total.get()[0] if self.total.exists() else 0
        new = prev + sum(int(r.value) for r in rows)
        self.total.update((new,))
        yield Row(key=key[0], value=str(new))

    def close(self) -> None:
        pass


spark = session(
    f"rtm43-d-tws-python-{MODE}",
    **{
        "spark.sql.shuffle.partitions": "16",
        "spark.sql.streaming.stateStore.providerClass": "org.apache.spark.sql.execution.streaming.state.RocksDBStateStoreProvider",
    },
)

grouped = read_events(spark).select("key", "value").groupBy("key")
if MODE == "pandas":
    out = grouped.transformWithStateInPandas(
        RunningTotalPandas(), OUT_SCHEMA, "Update", "None"
    )
else:
    out = grouped.transformWithState(RunningTotalRow(), OUT_SCHEMA, "Update", "None")

try:
    q = start_rtm_to_kafka(out, "rtm-tws", f"d_tws_python_{MODE}")
    q.awaitTermination(60)
    print(
        f"[d-{MODE}] RESULT: query still active={q.isActive}, exception={q.exception()}"
    )
except Exception as e:  # noqa: BLE001 - we want the exact error text
    print(f"[d-{MODE}] RESULT: {type(e).__name__}: {e}")
