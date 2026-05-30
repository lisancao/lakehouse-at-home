"""AUTO CDC consuming a REAL Debezium Postgres CDC feed from Kafka -> SCD1 Iceberg.

Parses the Debezium envelope (op c/u/d/r, before/after, source.lsn) into the AUTO
CDC contract: key from after|before, op 'd' -> DELETE else UPSERT, sequence_by =
source.lsn. Topic produced by a live Debezium Postgres connector (see COVERAGE).

Env: FRESH=1 drops the target + checkpoint (full re-read); otherwise incremental.
     EXPECTED='id,name,city;id,name,city;...' to assert the SCD1 end state.
"""
import os, sys
from pyspark.sql import SparkSession, functions as F
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, LongType
from pyspark.pipelines.spark_connect_pipeline import (
    create_dataflow_graph, start_run, handle_pipeline_events)
from pyspark.pipelines.spark_connect_graph_element_registry import SparkConnectGraphElementRegistry
from pyspark.pipelines.graph_element_registry import graph_element_registration_context
from pyspark import pipelines as dp

TOPIC = "dbz.public.customers"
TARGET = "ice.dbz.customers_current"
CKPT = "file:///tmp/autocdc-dbz/ck"
ROW = StructType([StructField("id", IntegerType()), StructField("name", StringType()), StructField("city", StringType())])
ENVELOPE = StructType([
    StructField("before", ROW), StructField("after", ROW),
    StructField("op", StringType()),
    StructField("source", StructType([StructField("lsn", LongType()), StructField("ts_ms", LongType())])),
    StructField("ts_ms", LongType()),
])

spark = SparkSession.builder.remote("sc://localhost:15432").getOrCreate()
spark.sql("CREATE NAMESPACE IF NOT EXISTS ice.dbz")
if os.environ.get("FRESH") == "1":
    spark.sql(f"DROP TABLE IF EXISTS {TARGET} PURGE")

gid = create_dataflow_graph(spark, default_catalog="spark_catalog", default_database="default", sql_conf={})
reg = SparkConnectGraphElementRegistry(spark, gid)
with graph_element_registration_context(reg):
    @dp.table(name="dbz_cdc_source")
    def dbz_cdc_source():
        raw = (spark.readStream.format("kafka")
               .option("kafka.bootstrap.servers", "localhost:9092")
               .option("subscribe", TOPIC)
               .option("startingOffsets", "earliest").load())
        e = raw.select(F.from_json(F.col("value").cast("string"), ENVELOPE).alias("e")).select("e.*")
        e = e.filter(F.col("op").isNotNull())   # drop any tombstone / unparseable
        return e.select(
            F.coalesce(F.col("after.id"), F.col("before.id")).alias("id"),     # delete: after is null -> before
            F.col("after.name").alias("name"),
            F.col("after.city").alias("city"),
            F.when(F.col("op") == "d", F.lit("DELETE")).otherwise(F.lit("UPSERT")).alias("op_cdc"),
            F.col("source.lsn").alias("seq"),                                  # monotonic WAL offset
        )
    dp.create_streaming_table(TARGET)
    dp.create_auto_cdc_flow(
        target=TARGET, source="dbz_cdc_source", keys=["id"], sequence_by="seq",
        apply_as_deletes="op_cdc = 'DELETE'", except_column_list=["op_cdc", "seq"],
        stored_as_scd_type=1)

print(">> starting run (debezium source)", flush=True)
try:
    handle_pipeline_events(start_run(spark, gid, full_refresh=None, full_refresh_all=False,
                                     refresh=None, dry=False, storage=CKPT))
except Exception as ex:
    print(f">> run raised: {type(ex).__name__}: {str(ex)[:300]}", flush=True); spark.stop(); sys.exit(2)

spark.sql(f"REFRESH TABLE {TARGET}")
rows = {(r["id"], r["name"], r["city"]) for r in spark.table(TARGET).select("id","name","city").collect()}
spark.table(TARGET).orderBy("id").show(truncate=False)
exp_env = os.environ.get("EXPECTED")
if exp_env:
    exp = set()
    for t in exp_env.split(";"):
        i,n,c = t.split(","); exp.add((int(i), n, c))
    print(">> RESULT:", "PASS" if rows == exp else f"FAIL exp={sorted(exp)} got={sorted(rows)}", flush=True)
    sys.exit(0 if rows == exp else 1)
print(">> state:", sorted(rows), flush=True)
