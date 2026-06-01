"""End-to-end proof that Apache Spark's AUTO CDC -> SCD Type 1 works.

Requires a SOURCE BUILD of Apache Spark master (currently 5.0.0-SNAPSHOT).
`create_auto_cdc_flow` / `stored_as_scd_type` landed AFTER the 4.2.0-preview5
cut, so no released artifact (preview5 image, pyspark 4.2.0.dev5) contains it.

Why a driver script (vs the `spark-pipelines` CLI):
    Both work. This script drives registration + start_run directly against a
    *standalone* Connect server so we pin the exact server version + port. (The
    CLI connects to the default sc://localhost:15002; if an older server happens
    to own that port it rejects the AutoCDC command with RELTYPE_NOT_SET, so an
    explicit standalone server avoids that ambiguity.) See README.md.

What it proves (the classic SCD1 cases):
    - id=1  INSERT NY (seq1) -> UPDATE Boston (seq3) -> stale UPDATE Chicago (seq2)
            => Boston   (highest seq wins; stale out-of-order event ignored)
    - id=2  INSERT LA (seq1) -> DELETE (seq2)            => removed
    - id=3  INSERT San Francisco (seq1)                  => unchanged

Target table requirements (discovered empirically):
    AUTO CDC writes via row-level MERGE and maintains a durable auxiliary state
    table, so the target MUST be a connector that supports row-level operations
    AND survives streaming restarts. A plain Spark-managed parquet table fails
    with AUTOCDC_TARGET_DOES_NOT_SUPPORT_MERGE. Here the target lives in
    `cat`, Spark's own SharedTablesInMemoryRowLevelOperationTableCatalog (a test
    connector), which is the minimal pure-Spark way to satisfy this. In
    production the target would be Iceberg or Delta.

Run via run.sh (which starts a standalone Connect server first), or manually:
    python generate_cdc_data.py
    python autocdc_scd1_proof.py
"""

import sys

from pyspark.sql import SparkSession
from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    IntegerType,
    LongType,
)
from pyspark.pipelines.spark_connect_pipeline import (
    create_dataflow_graph,
    start_run,
    handle_pipeline_events,
)
from pyspark.pipelines.spark_connect_graph_element_registry import (
    SparkConnectGraphElementRegistry,
)
from pyspark.pipelines.graph_element_registry import graph_element_registration_context
from pyspark import pipelines as dp

import os

REMOTE = "sc://localhost:15432"
CDC_GLOB = "file:///tmp/autocdc-scd1/cdc-events/*.json"  # bare dir is rejected; glob works
STORAGE = "file:///tmp/autocdc-scd1/storage"
# Fully-qualified target in a MERGE-capable catalog. Defaults to Iceberg (the
# stack path); set AUTOCDC_TARGET=cat.cdc.scd1_customers for the pure-Spark
# in-memory catalog variant. The namespace is auto-created below.
TARGET = os.environ.get("AUTOCDC_TARGET", "ice.cdc.scd1_customers")
_NS = TARGET.rsplit(".", 1)[0]  # catalog.namespace

CDC_SCHEMA = StructType(
    [
        StructField("id", IntegerType()),
        StructField("name", StringType()),
        StructField("city", StringType()),
        StructField("op", StringType()),  # "UPSERT" / "DELETE"
        StructField("seq", LongType()),  # sequencing column
    ]
)

EXPECTED = {(1, "Alice", "Boston"), (3, "Carol", "San Francisco")}


def main() -> int:
    spark = (
        SparkSession.builder.remote(REMOTE)
        .config("spark.sql.connect.serverStacktrace.enabled", "true")
        .getOrCreate()
    )
    spark.sql(f"CREATE NAMESPACE IF NOT EXISTS {_NS}")

    # Source flow stays in the Hive session catalog (spark_catalog); only the
    # AutoCDC target is qualified into the MERGE-capable catalog.
    gid = create_dataflow_graph(
        spark, default_catalog="spark_catalog", default_database="default", sql_conf={}
    )
    registry = SparkConnectGraphElementRegistry(spark, gid)

    with graph_element_registration_context(registry):

        @dp.table(name="cdc_customers_source")
        def cdc_customers_source():
            return spark.readStream.schema(CDC_SCHEMA).json(CDC_GLOB)

        dp.create_streaming_table(TARGET)

        dp.create_auto_cdc_flow(
            target=TARGET,
            source="cdc_customers_source",
            keys=["id"],
            sequence_by="seq",
            apply_as_deletes="op = 'DELETE'",
            except_column_list=["op", "seq"],
            stored_as_scd_type=1,
        )

    print(">> graph registered; starting run", flush=True)
    events = start_run(
        spark,
        gid,
        full_refresh=None,
        full_refresh_all=False,
        refresh=None,
        dry=False,
        storage=STORAGE,
    )
    try:
        handle_pipeline_events(events)
    except Exception as e:  # noqa: BLE001
        print(f">> run raised: {type(e).__name__}: {str(e)[:400]}", flush=True)
        spark.stop()
        return 2

    print(">> run complete; SCD1 target state:", flush=True)
    # Streaming writes commit a new snapshot; refresh so this session reads it
    # (Iceberg/DSv2 catalogs cache the table at the pre-write snapshot otherwise).
    spark.sql(f"REFRESH TABLE {TARGET}")
    df = spark.table(TARGET).select("id", "name", "city").orderBy("id")
    rows = {(r["id"], r["name"], r["city"]) for r in df.collect()}
    df.show(truncate=False)

    ok = rows == EXPECTED
    if ok:
        print("PASS: AUTO CDC SCD Type 1 semantics confirmed")
        print("  - id=1 overwritten to Boston (newest seq wins, no history)")
        print("  - stale out-of-order Chicago event (seq=2) ignored")
        print("  - id=2 removed via apply_as_deletes")
        print("  - id=3 unchanged")
    else:
        print(f"FAIL: expected {sorted(EXPECTED)}, got {sorted(rows)}")

    spark.stop()
    return 0 if ok else 1


if __name__ == "__main__":
    sys.exit(main())
