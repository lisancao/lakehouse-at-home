"""AUTO CDC with an ICEBERG TABLE as the streaming CDC source (lakehouse-to-lakehouse).

A bronze Iceberg table `ice.l2l.cdc_feed` is append-fed with change-event rows
(id, name, city, op, seq). AUTO CDC reads it as a STREAMING source and maintains
the SCD1 current state in `ice.l2l.scd1_customers`.

Note (a real finding): Iceberg's `_change_type` CHANGELOG (deletes/updates as
change rows, via SparkChangelogTable / create_changelog_view) is BATCH-only in OSS;
the streaming source is append-only. So you cannot stream an Iceberg changelog
directly into AUTO CDC — but an append-stream of CDC rows (this test) works.
"""
import os, sys
from pyspark.sql import SparkSession, functions as F
from pyspark.pipelines.spark_connect_pipeline import (
    create_dataflow_graph, start_run, handle_pipeline_events)
from pyspark.pipelines.spark_connect_graph_element_registry import SparkConnectGraphElementRegistry
from pyspark.pipelines.graph_element_registry import graph_element_registration_context
from pyspark import pipelines as dp

FEED = "ice.l2l.cdc_feed"
TARGET = "ice.l2l.scd1_customers"
CKPT = "file:///tmp/autocdc-l2l/ck"

spark = SparkSession.builder.remote("sc://localhost:15432").getOrCreate()
spark.sql("CREATE NAMESPACE IF NOT EXISTS ice.l2l")
spark.sql(f"DROP TABLE IF EXISTS {FEED} PURGE")
spark.sql(f"DROP TABLE IF EXISTS {TARGET} PURGE")

# bronze CDC-feed Iceberg table, append-fed with change events
spark.sql(f"CREATE TABLE {FEED} (id INT, name STRING, city STRING, op STRING, seq BIGINT) USING iceberg")
spark.sql(f"""INSERT INTO {FEED} VALUES
  (1,'Alice','New York','UPSERT',1),(2,'Bob','Los Angeles','UPSERT',1),
  (3,'Carol','San Francisco','UPSERT',1),(1,'Alice','Boston','UPSERT',3),
  (2,'Bob','Los Angeles','DELETE',2),(1,'Alice','Chicago','UPSERT',2)""")  # last = stale

gid = create_dataflow_graph(spark, default_catalog="spark_catalog", default_database="default", sql_conf={})
reg = SparkConnectGraphElementRegistry(spark, gid)
with graph_element_registration_context(reg):
    @dp.table(name="ice_cdc_source")
    def ice_cdc_source():
        # Iceberg streaming source over the bronze feed table (append stream).
        return (spark.readStream.format("iceberg")
                .option("stream-from-timestamp", "1")   # read from the first snapshot
                .load(FEED))
    dp.create_streaming_table(TARGET)
    dp.create_auto_cdc_flow(target=TARGET, source="ice_cdc_source", keys=["id"], sequence_by="seq",
                            apply_as_deletes="op = 'DELETE'", except_column_list=["op", "seq"],
                            stored_as_scd_type=1)

print(">> starting run (iceberg streaming source)", flush=True)
try:
    handle_pipeline_events(start_run(spark, gid, full_refresh=None, full_refresh_all=False,
                                     refresh=None, dry=False, storage=CKPT))
except Exception as ex:
    print(f">> run raised: {type(ex).__name__}: {str(ex)[:300]}", flush=True); spark.stop(); sys.exit(2)

spark.sql(f"REFRESH TABLE {TARGET}")
rows = {(r["id"], r["name"], r["city"]) for r in spark.table(TARGET).select("id","name","city").collect()}
spark.table(TARGET).orderBy("id").show(truncate=False)
exp = {(1,"Alice","Boston"),(3,"Carol","San Francisco")}
print(">> RESULT:", "PASS" if rows == exp else f"FAIL exp={sorted(exp)} got={sorted(rows)}", flush=True)
sys.exit(0 if rows == exp else 1)
