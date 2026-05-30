"""AUTO CDC consuming Confluent-Avro Debezium messages via Spark from_avro.

Production-standard wire format: each Kafka value is [magic 0x00][4-byte schema id]
[Avro payload]. We strip the 5-byte header and from_avro() with the value schema
fetched from the Schema Registry. Topic: dbza.public.customers (registry :8085).
"""
import json, os, sys, urllib.request
from pyspark.sql import SparkSession, functions as F
from pyspark.sql.avro.functions import from_avro
from pyspark.pipelines.spark_connect_pipeline import (
    create_dataflow_graph, start_run, handle_pipeline_events)
from pyspark.pipelines.spark_connect_graph_element_registry import SparkConnectGraphElementRegistry
from pyspark.pipelines.graph_element_registry import graph_element_registration_context
from pyspark import pipelines as dp

REGISTRY = "http://localhost:8085"
TOPIC = "dbza.public.customers"
TARGET = "ice.dbza.customers_current"
CKPT = "file:///tmp/autocdc-dbz/cka"
AVRO_SCHEMA = json.loads(urllib.request.urlopen(
    f"{REGISTRY}/subjects/{TOPIC}-value/versions/latest", timeout=10).read())["schema"]

spark = SparkSession.builder.remote("sc://localhost:15432").getOrCreate()
spark.sql("CREATE NAMESPACE IF NOT EXISTS ice.dbza")
if os.environ.get("FRESH") == "1":
    spark.sql(f"DROP TABLE IF EXISTS {TARGET} PURGE")

gid = create_dataflow_graph(spark, default_catalog="spark_catalog", default_database="default", sql_conf={})
reg = SparkConnectGraphElementRegistry(spark, gid)
with graph_element_registration_context(reg):
    @dp.table(name="dbza_cdc_source")
    def dbza_cdc_source():
        raw = (spark.readStream.format("kafka")
               .option("kafka.bootstrap.servers", "localhost:9092")
               .option("subscribe", TOPIC).option("startingOffsets", "earliest").load())
        # strip the 5-byte Confluent wire header, then decode Avro
        payload = F.expr("substring(value, 6, length(value) - 5)")
        e = raw.select(from_avro(payload, AVRO_SCHEMA).alias("e")).select("e.*")
        e = e.filter(F.col("op").isNotNull())
        return e.select(
            F.coalesce(F.col("after.id"), F.col("before.id")).alias("id"),
            F.col("after.name").alias("name"), F.col("after.city").alias("city"),
            F.when(F.col("op") == "d", F.lit("DELETE")).otherwise(F.lit("UPSERT")).alias("op_cdc"),
            F.col("source.lsn").alias("seq"))
    dp.create_streaming_table(TARGET)
    dp.create_auto_cdc_flow(target=TARGET, source="dbza_cdc_source", keys=["id"], sequence_by="seq",
                            apply_as_deletes="op_cdc = 'DELETE'", except_column_list=["op_cdc", "seq"],
                            stored_as_scd_type=1)

print(">> starting run (debezium Avro / schema registry)", flush=True)
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
