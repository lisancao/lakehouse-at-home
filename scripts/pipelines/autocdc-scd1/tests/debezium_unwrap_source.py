"""AUTO CDC against the FLATTENED Debezium format (ExtractNewRecordState SMT).

The most common real Debezium consumption shape: the envelope is unwrapped to the
`after` row plus `__op`/`__deleted`/`__lsn` metadata fields. On a delete (rewrite
mode) the value's business columns are null, so the key comes from the Kafka
message key. Topic: dbzf.public.customers.
"""
import os, sys
from pyspark.sql import SparkSession, functions as F
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, LongType
from pyspark.pipelines.spark_connect_pipeline import (
    create_dataflow_graph, start_run, handle_pipeline_events)
from pyspark.pipelines.spark_connect_graph_element_registry import SparkConnectGraphElementRegistry
from pyspark.pipelines.graph_element_registry import graph_element_registration_context
from pyspark import pipelines as dp

TOPIC = "dbzf.public.customers"
TARGET = "ice.dbzf.customers_current"
CKPT = "file:///tmp/autocdc-dbz/ckf"
VAL = StructType([
    StructField("id", IntegerType()), StructField("name", StringType()), StructField("city", StringType()),
    StructField("__deleted", StringType()), StructField("__op", StringType()), StructField("__lsn", LongType()),
])
KEY = StructType([StructField("id", IntegerType())])

spark = SparkSession.builder.remote("sc://localhost:15432").getOrCreate()
spark.sql("CREATE NAMESPACE IF NOT EXISTS ice.dbzf")
if os.environ.get("FRESH") == "1":
    spark.sql(f"DROP TABLE IF EXISTS {TARGET} PURGE")

gid = create_dataflow_graph(spark, default_catalog="spark_catalog", default_database="default", sql_conf={})
reg = SparkConnectGraphElementRegistry(spark, gid)
with graph_element_registration_context(reg):
    @dp.table(name="dbzf_cdc_source")
    def dbzf_cdc_source():
        raw = (spark.readStream.format("kafka")
               .option("kafka.bootstrap.servers", "localhost:9092")
               .option("subscribe", TOPIC).option("startingOffsets", "earliest").load())
        d = raw.select(
            F.from_json(F.col("key").cast("string"), KEY).alias("k"),
            F.from_json(F.col("value").cast("string"), VAL).alias("v"))
        d = d.filter(F.col("v").isNotNull())
        return d.select(
            F.coalesce(F.col("v.id"), F.col("k.id")).alias("id"),   # delete: value cols null -> key
            F.col("v.name").alias("name"), F.col("v.city").alias("city"),
            F.when((F.col("v.__deleted") == "true") | (F.col("v.__op") == "d"),
                   F.lit("DELETE")).otherwise(F.lit("UPSERT")).alias("op_cdc"),
            F.col("v.__lsn").alias("seq"))
    dp.create_streaming_table(TARGET)
    dp.create_auto_cdc_flow(target=TARGET, source="dbzf_cdc_source", keys=["id"], sequence_by="seq",
                            apply_as_deletes="op_cdc = 'DELETE'", except_column_list=["op_cdc", "seq"],
                            stored_as_scd_type=1)

print(">> starting run (debezium unwrap/flattened)", flush=True)
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
    exp = {(int(i), n, c) for i, n, c in (t.split(",") for t in exp_env.split(";"))}
    print(">> RESULT:", "PASS" if rows == exp else f"FAIL exp={sorted(exp)} got={sorted(rows)}", flush=True)
    sys.exit(0 if rows == exp else 1)
print(">> state:", sorted(rows), flush=True)
