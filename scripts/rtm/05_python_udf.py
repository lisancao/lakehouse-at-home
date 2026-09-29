"""Experiment e: Python UDFs in an RTM query.

The 4.3 RTM docs list "scalar user-defined functions (UDFs)" as supported
projections, but the operator allowlist (RealTimeModeAllowlist.scala) has no
BatchEvalPythonExec / ArrowEvalPythonExec. This script tries each kind and
prints the exact error.

    spark-submit 05_python_udf.py udf     # plain @udf  (BatchEvalPythonExec)
    spark-submit 05_python_udf.py arrow   # @udf(useArrow=True) (ArrowEvalPythonExec)
    spark-submit 05_python_udf.py pandas  # @pandas_udf (ArrowEvalPythonExec)
"""

import sys

import pandas as pd
from pyspark.sql.functions import col, pandas_udf, struct, to_json, udf
from rtm_common import read_events, session, start_rtm_to_kafka

MODE = sys.argv[1] if len(sys.argv) > 1 else "udf"
spark = session(f"rtm43-e-python-udf-{MODE}")


@udf("string")
def shout(s):
    return s.upper() if s else s


@udf("string", useArrow=True)
def shout_arrow(s):
    return s.upper() if s else s


@pandas_udf("string")
def shout_pandas(s: pd.Series) -> pd.Series:
    return s.str.upper()


fn = {"udf": shout, "arrow": shout_arrow, "pandas": shout_pandas}[MODE]
events = read_events(spark).withColumn("event_type", fn(col("event_type")))
out = events.select(
    col("key"), to_json(struct("key", "event_type", "produced_at")).alias("value")
)

try:
    q = start_rtm_to_kafka(out, "rtm-udf", f"e_python_udf_{MODE}")
    q.awaitTermination(60)
    print(
        f"[e-{MODE}] RESULT: query still active={q.isActive}, exception={q.exception()}"
    )
except Exception as e:  # noqa: BLE001 - we want the exact error text
    print(f"[e-{MODE}] RESULT: {type(e).__name__}: {e}")
