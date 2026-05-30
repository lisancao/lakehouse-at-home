import sys
from pyspark.sql import SparkSession, functions as F
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, LongType
from pyspark.pipelines.spark_connect_pipeline import (
    create_dataflow_graph, start_run, handle_pipeline_events)
from pyspark.pipelines.spark_connect_graph_element_registry import SparkConnectGraphElementRegistry
from pyspark.pipelines.graph_element_registry import graph_element_registration_context
from pyspark import pipelines as dp

spark = SparkSession.builder.remote("sc://localhost:15432").getOrCreate()
spark.sql("CREATE NAMESPACE IF NOT EXISTS ice.kafka")
spark.sql("DROP TABLE IF EXISTS ice.kafka.scd1_customers PURGE")

CDC = StructType([StructField("id",IntegerType()),StructField("name",StringType()),
    StructField("city",StringType()),StructField("op",StringType()),StructField("seq",LongType())])

gid = create_dataflow_graph(spark, default_catalog="spark_catalog", default_database="default", sql_conf={})
reg = SparkConnectGraphElementRegistry(spark, gid)
with graph_element_registration_context(reg):
    @dp.table(name="kafka_cdc_source")
    def kafka_cdc_source():
        raw = (spark.readStream.format("kafka")
               .option("kafka.bootstrap.servers", "localhost:9092")
               .option("subscribe", "autocdc-cdc")
               .option("startingOffsets", "earliest").load())
        return raw.select(F.from_json(F.col("value").cast("string"), CDC).alias("e")).select("e.*")
    dp.create_streaming_table("ice.kafka.scd1_customers")
    dp.create_auto_cdc_flow(
        target="ice.kafka.scd1_customers", source="kafka_cdc_source",
        keys=["id"], sequence_by="seq", apply_as_deletes="op = 'DELETE'",
        except_column_list=["op", "seq"], stored_as_scd_type=1)

print(">> starting run (kafka source)", flush=True)
try:
    handle_pipeline_events(start_run(spark, gid, full_refresh=None, full_refresh_all=False,
                                     refresh=None, dry=False, storage="file:///tmp/autocdc-kafka/ck"))
except Exception as e:
    print(f">> run raised: {type(e).__name__}: {str(e)[:300]}", flush=True); spark.stop(); sys.exit(2)

spark.sql("REFRESH TABLE ice.kafka.scd1_customers")
rows = {(r["id"],r["name"],r["city"]) for r in spark.table("ice.kafka.scd1_customers").select("id","name","city").collect()}
exp = {(1,"Alice","Boston"),(3,"Carol","San Francisco")}
spark.table("ice.kafka.scd1_customers").orderBy("id").show(truncate=False)
print(">> RESULT:", "PASS" if rows==exp else f"FAIL exp={sorted(exp)} got={sorted(rows)}", flush=True)
sys.exit(0 if rows==exp else 1)
