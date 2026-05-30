"""AUTO CDC behavior-dimension tests on Hadoop-Iceberg (Spark 5.0).

Covers: composite keys, incremental re-run (streaming checkpoint picks up new
files), full-refresh, and value-level schema evolution.
"""
import json, os, shutil, sys
from pyspark.sql import SparkSession, functions as F
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, LongType
from pyspark.pipelines.spark_connect_pipeline import (
    create_dataflow_graph, start_run, handle_pipeline_events)
from pyspark.pipelines.spark_connect_graph_element_registry import SparkConnectGraphElementRegistry
from pyspark.pipelines.graph_element_registry import graph_element_registration_context
from pyspark import pipelines as dp

ROOT = "/tmp/autocdc-behavior"
spark = SparkSession.builder.remote("sc://localhost:15432").getOrCreate()
spark.sql("CREATE NAMESPACE IF NOT EXISTS ice.beh")

def write_batches(src, batches):
    shutil.rmtree(src, ignore_errors=True); os.makedirs(src)
    for i, b in enumerate(batches, 1):
        with open(f"{src}/b{i:02d}.json", "w") as fh:
            for e in b: fh.write(json.dumps(e) + "\n")

def add_batch(src, idx, rows):
    with open(f"{src}/b{idx:02d}.json", "w") as fh:
        for e in rows: fh.write(json.dumps(e) + "\n")

def register(target, schema, glob, keys, **kw):
    def reg():
        def mk():
            return spark.readStream.schema(schema).json(glob)
        dp.table(name=f"src_{target.split('.')[-1]}")(mk)
        dp.create_streaming_table(target)
        dp.create_auto_cdc_flow(target=target, source=f"src_{target.split('.')[-1]}",
                                keys=keys, sequence_by="seq",
                                apply_as_deletes="op = 'DELETE'", stored_as_scd_type=1, **kw)
    return reg

def run(target, reg, storage_tag, full_refresh_all=False):
    gid = create_dataflow_graph(spark, default_catalog="spark_catalog", default_database="default", sql_conf={})
    r = SparkConnectGraphElementRegistry(spark, gid)
    with graph_element_registration_context(r):
        reg()
    handle_pipeline_events(start_run(spark, gid, full_refresh=None, full_refresh_all=full_refresh_all,
                                     refresh=None, dry=False, storage=f"file://{ROOT}-ck/{storage_tag}"))

results = []
def check(name, target, expected, cols):
    spark.sql(f"REFRESH TABLE {target}")
    got = {tuple(r[c] for c in cols) for r in spark.table(target).select(*cols).collect()}
    ok = got == expected
    results.append((name, ok, got if not ok else None))
    print(f"[{name}] {'PASS' if ok else 'FAIL'}" + ("" if ok else f"  exp={sorted(expected)} got={sorted(got)}"))

U, D = "UPSERT", "DELETE"

# ---- 1. Composite keys (region, id) ----
S_COMP = StructType([StructField("region",StringType()),StructField("id",IntegerType()),
    StructField("name",StringType()),StructField("city",StringType()),StructField("op",StringType()),StructField("seq",LongType())])
src = f"{ROOT}/composite"
write_batches(src, [[
    {"region":"W","id":1,"name":"A","city":"NY","op":U,"seq":1},
    {"region":"W","id":1,"name":"A","city":"Boston","op":U,"seq":2},   # W,1 -> Boston
    {"region":"E","id":1,"name":"B","city":"Miami","op":U,"seq":1},    # E,1 distinct -> Miami
]])
T="ice.beh.composite"; spark.sql(f"DROP TABLE IF EXISTS {T} PURGE")
run(T, register(T, S_COMP, f"file://{src}/*.json", ["region","id"]), "composite")
check("composite_keys", T, {("W",1,"Boston"),("E",1,"Miami")}, ["region","id","city"])

# ---- 2. Incremental re-run: same target, add data, re-run (no full refresh) ----
S = StructType([StructField("id",IntegerType()),StructField("name",StringType()),
    StructField("city",StringType()),StructField("op",StringType()),StructField("seq",LongType())])
src = f"{ROOT}/incr"
write_batches(src, [[{"id":1,"name":"A","city":"NY","op":U,"seq":1},
                     {"id":2,"name":"B","city":"LA","op":U,"seq":1}]])
T="ice.beh.incr"; spark.sql(f"DROP TABLE IF EXISTS {T} PURGE")
run(T, register(T, S, f"file://{src}/*.json", ["id"]), "incr")
check("incremental_run1", T, {(1,"A","NY"),(2,"B","LA")}, ["id","name","city"])
# add new events; re-run WITHOUT full refresh -> streaming checkpoint should pick up only the new file
add_batch(src, 2, [{"id":1,"name":"A","city":"Boston","op":U,"seq":3},   # update existing
                   {"id":3,"name":"C","city":"SF","op":U,"seq":1}])       # new key
run(T, register(T, S, f"file://{src}/*.json", ["id"]), "incr")  # same storage tag = same checkpoint
check("incremental_run2", T, {(1,"A","Boston"),(2,"B","LA"),(3,"C","SF")}, ["id","name","city"])

# ---- 3. Full refresh: re-run with full_refresh_all on the same target ----
T="ice.beh.fullref"; src=f"{ROOT}/fullref"; spark.sql(f"DROP TABLE IF EXISTS {T} PURGE")
write_batches(src, [[{"id":1,"name":"A","city":"NY","op":U,"seq":1},
                     {"id":2,"name":"B","city":"LA","op":U,"seq":1}]])
run(T, register(T, S, f"file://{src}/*.json", ["id"]), "fullref")
check("fullref_initial", T, {(1,"A","NY"),(2,"B","LA")}, ["id","name","city"])
run(T, register(T, S, f"file://{src}/*.json", ["id"]), "fullref_fr", full_refresh_all=True)
check("fullref_recompute", T, {(1,"A","NY"),(2,"B","LA")}, ["id","name","city"])

# ---- 4. Value-level schema evolution: a column null early, populated later ----
S_EVO = StructType([StructField("id",IntegerType()),StructField("name",StringType()),
    StructField("city",StringType()),StructField("tier",StringType()),StructField("op",StringType()),StructField("seq",LongType())])
T="ice.beh.evo"; src=f"{ROOT}/evo"; spark.sql(f"DROP TABLE IF EXISTS {T} PURGE")
write_batches(src, [
    [{"id":1,"name":"A","city":"NY","tier":None,"op":U,"seq":1}],          # tier null
    [{"id":1,"name":"A","city":"NY","tier":"gold","op":U,"seq":2}],        # tier populated
])
run(T, register(T, S_EVO, f"file://{src}/*.json", ["id"]), "evo")
check("schema_value_evolution", T, {(1,"A","NY","gold")}, ["id","name","city","tier"])

print("\n==== BEHAVIOR SUMMARY ====")
for n, ok, d in results:
    print(f"  {'PASS' if ok else 'FAIL'}  {n}" + (f"  {d}" if d else ""))
print(f"\n  {sum(1 for _,ok,_ in results if ok)}/{len(results)} passed")
sys.exit(0 if all(ok for _,ok,_ in results) else 1)
