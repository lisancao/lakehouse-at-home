"""Adversarial SCD1 correctness battery for AUTO CDC (Spark 5.0 + Iceberg).

Each scenario runs an isolated pipeline into a fresh Iceberg table (Hadoop catalog
`ice`) and asserts the end state. `expected=None` = probe (record behavior).
Run against a standalone Connect server whose SPARK_CONF_DIR registers an `ice`
Hadoop-Iceberg catalog (see COVERAGE.md §4 for the config pattern).
"""
import json, os, shutil, sys
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, LongType
from pyspark.pipelines.spark_connect_pipeline import (
    create_dataflow_graph, start_run, handle_pipeline_events)
from pyspark.pipelines.spark_connect_graph_element_registry import SparkConnectGraphElementRegistry
from pyspark.pipelines.graph_element_registry import graph_element_registration_context
from pyspark import pipelines as dp

ROOT = "/tmp/autocdc-stress"
SCHEMA = StructType([
    StructField("id", IntegerType()), StructField("name", StringType()),
    StructField("city", StringType()), StructField("op", StringType()),
    StructField("seq", LongType()),
])
spark = SparkSession.builder.remote("sc://localhost:15432").getOrCreate()
spark.sql("CREATE NAMESPACE IF NOT EXISTS ice.stress")
U, D = "UPSERT", "DELETE"
def ev(i, n, c, op, s): return {"id": i, "name": n, "city": c, "op": op, "seq": s}

def run_scenario(idx, name, batches, expected, note=""):
    tag = f"s{idx:02d}_{name}"; src = f"{ROOT}/{tag}"
    shutil.rmtree(src, ignore_errors=True); os.makedirs(src)
    for i, b in enumerate(batches, 1):
        with open(f"{src}/b{i:02d}.json", "w") as fh:
            for e in b: fh.write(json.dumps(e) + "\n")
    target = f"ice.stress.{tag}"; spark.sql(f"DROP TABLE IF EXISTS {target} PURGE")
    try:
        gid = create_dataflow_graph(spark, default_catalog="spark_catalog",
                                    default_database="default", sql_conf={})
        reg = SparkConnectGraphElementRegistry(spark, gid)
        with graph_element_registration_context(reg):
            def mk(g=f"file://{src}/*.json"):
                return spark.readStream.schema(SCHEMA).json(g)
            dp.table(name=f"src_{tag}")(lambda: mk())
            dp.create_streaming_table(target)
            dp.create_auto_cdc_flow(target=target, source=f"src_{tag}", keys=["id"],
                                    sequence_by="seq", apply_as_deletes="op = 'DELETE'",
                                    except_column_list=["op", "seq"], stored_as_scd_type=1)
        handle_pipeline_events(start_run(spark, gid, full_refresh=None, full_refresh_all=False,
                                         refresh=None, dry=False, storage=f"file://{ROOT}-ck/{tag}"))
    except Exception as e:
        print(f"[{tag}] ERROR: {str(e).splitlines()[0][:120]}"); return ("ERROR", name)
    spark.sql(f"REFRESH TABLE {target}")
    rows = {(r["id"], r["name"], r["city"])
            for r in spark.table(target).select("id", "name", "city").collect()}
    if expected is None:
        print(f"[{tag}] PROBE -> {sorted(rows)}  ({note})"); return ("PROBE", name)
    ok = rows == expected
    print(f"[{tag}] {'PASS' if ok else 'FAIL'} {note}" + ("" if ok else f"  exp={sorted(expected)} got={sorted(rows)}"))
    return ("PASS" if ok else "FAIL", name)

SCENARIOS = [
    ("delete_then_reinsert", [[ev(1,"A","NY",U,1)],[ev(1,"A","NY",D,2)],[ev(1,"A","LA",U,3)]],
     {(1,"A","LA")}, "insert->delete->reinsert => present"),
    ("out_of_order_delete", [[ev(1,"A","NY",U,1)],[ev(1,"A","Boston",U,3)],[ev(1,"A","NY",D,2)]],
     {(1,"A","Boston")}, "delete@2 < update@3 => stays"),
    ("stale_insert_after_delete", [[ev(1,"A","NY",U,3)],[ev(1,"A","NY",D,5)],[ev(1,"A","Old",U,2)]],
     set(), "insert@2 after delete@5 => deleted"),
    ("delete_before_insert", [[ev(1,"A","NY",D,5)],[ev(1,"A","NY",U,2)]],
     set(), "delete@5 then insert@2 => deleted"),
    ("upsert_nonexistent", [[ev(9,"Z","Reno",U,1)]], {(9,"Z","Reno")}, "upsert-only => insert"),
    ("multi_update_one_batch", [[ev(1,"A","c1",U,1),ev(1,"A","c2",U,2),ev(1,"A","c3",U,3)]],
     {(1,"A","c3")}, "3 updates one batch => highest seq"),
    ("delete_nonexistent", [[ev(7,"Q","X",D,1)]], set(), "delete missing key => no-op"),
    ("seq_tie", [[ev(1,"A","Boston",U,5),ev(1,"A","Denver",U,5)]], None, "equal seq => non-deterministic"),
]

def main():
    res = [run_scenario(i, n, b, e, note) for i,(n,b,e,note) in enumerate(SCENARIOS,1)]
    # volume: 500 keys, out-of-order, 100 deleted -> 400 survive
    keys=list(range(1,501)); allev=[]
    for k in keys:
        allev += [ev(k,f"n{k}","init",U,1), ev(k,f"n{k}","updated",U,3), ev(k,f"n{k}","stale",U,2)]
    for k in keys[:100]: allev.append(ev(k,f"n{k}","x",D,5))
    batches=[[],[],[],[],[]]
    for j,e in enumerate(allev): batches[(j*7)%5].append(e)
    res.append(run_scenario(99,"volume_500keys",batches,{(k,f"n{k}","updated") for k in keys[100:]},
                            "500 keys oo, 100 deleted => 400 'updated'"))
    print("\n==== STRESS SUMMARY ====")
    for s,n in res: print(f"  {s:5} {n}")
    fails=[n for s,n in res if s in ("FAIL","ERROR")]
    print(f"\n  {sum(1 for s,_ in res if s=='PASS')} pass, {len(fails)} fail/error, "
          f"{sum(1 for s,_ in res if s=='PROBE')} probe")
    return 1 if fails else 0

sys.exit(main())
