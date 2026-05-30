"""Holistic, data-grounded AUTO CDC e2e on the ghost-kitchen domain.

Answers "where does AUTO CDC get used?" with two real scenarios on the stack's
existing test data (see spark_content/autocdc_real_world_scenarios.md):

  Scenario A  gold.orders_live           -- the append-only order LIFECYCLE event
              (event stream -> current    stream IS a change feed for each order's
               state)                      current state. AUTO CDC collapses it to
                                           one row/order (latest status), resolves
                                           chaos-injected out-of-order events, and
                                           removes cancellations via apply_as_deletes.

  Scenario B  silver.dim_locations_current-- an operational dimension changes over
              (SCD Type 1 dimension)       time (relocation, rename, closure). A CDC
                                           feed keeps the lakehouse dimension current.

Correctness is proven by an INDEPENDENT full-recompute that must match AUTO CDC.

Run against a standalone Connect server with an Iceberg `ice` catalog (see run.sh
/ conf for the setup). Reads data/events/orders_1d.parquet + data/dimensions/.
"""
import json
import os
import shutil
import sys
from pathlib import Path

from pyspark.sql import SparkSession, functions as F, Window
from pyspark.sql.types import (
    StructType, StructField, StringType, IntegerType, LongType, DoubleType)
from pyspark.pipelines.spark_connect_pipeline import (
    create_dataflow_graph, start_run, handle_pipeline_events)
from pyspark.pipelines.spark_connect_graph_element_registry import (
    SparkConnectGraphElementRegistry)
from pyspark.pipelines.graph_element_registry import graph_element_registration_context
from pyspark import pipelines as dp

# Repo data dir (…/lakehouse-stack/data), overridable via DATA_DIR.
REPO = Path(__file__).resolve().parents[3]
DATA_DIR = os.environ.get("DATA_DIR", f"{REPO}/data")
# Prefer the cancellation-enabled dataset (generated with --cancel-rate, which
# emits real `order_cancelled` lifecycle events); fall back to the standard 1d set.
_DEMO = f"{DATA_DIR}/events/orders_cdc_demo.parquet"
EVENTS_PARQUET = (f"file://{_DEMO}" if os.path.exists(_DEMO)
                  else f"file://{DATA_DIR}/events/orders_1d.parquet")
LOCATIONS_PARQUET = f"file://{DATA_DIR}/dimensions/locations.parquet"
RW = "/tmp/autocdc-scd1/realworld"
N_ORDERS = int(os.environ.get("N_ORDERS", "5000"))  # sample of orders (all their events)

spark = SparkSession.builder.remote("sc://localhost:15432").getOrCreate()
spark.sql("CREATE NAMESPACE IF NOT EXISTS ice.silver")
spark.sql("CREATE NAMESPACE IF NOT EXISTS ice.gold")

ORDER_SCHEMA = StructType([
    StructField("order_id", StringType()), StructField("status", StringType()),
    StructField("location_id", IntegerType()), StructField("brand_id", IntegerType()),
    StructField("order_total", DoubleType()), StructField("event_ts", LongType()),
    StructField("op", StringType()), StructField("seq", LongType()),
])
DIM_SCHEMA = StructType([
    StructField("id", LongType()), StructField("city", StringType()),
    StructField("op", StringType()), StructField("seq", LongType())])


def build_order_cdc_feed():
    """Project the raw lifecycle events into a current-state CDC feed."""
    shutil.rmtree(f"{RW}/order_cdc", ignore_errors=True)
    raw = spark.read.parquet(EVENTS_PARQUET)

    # Deterministic sample: order by the UNIQUE order_id (orderBy(hash).limit is
    # non-deterministic under ties and re-evaluates per DAG reference, which would
    # union several different samples into the feed). Materialize so every
    # downstream reference uses the identical set.
    sampled = (raw.select("order_id").distinct()
               .orderBy("order_id").limit(N_ORDERS).select("order_id").cache())
    sampled.count()  # force materialization
    ev = raw.join(F.broadcast(sampled), "order_id")

    # brand_id + total live only on order_created -> forward onto every event so
    # each SCD1 row carries the full current-state payload.
    body = StructType([StructField("brand_id", IntegerType()), StructField("total", DoubleType())])
    created = (ev.filter("event_type = 'order_created'")
               .withColumn("b", F.from_json("body", body))
               .select("order_id", F.col("b.brand_id").alias("brand_id"),
                       F.col("b.total").alias("order_total")))

    # A real `order_cancelled` lifecycle event (generated with --cancel-rate) is the
    # delete vector: it is the order's terminal, highest-sequence event, so AUTO CDC
    # removes the order from the live table. No synthetic injection.
    feed = (ev.join(F.broadcast(created), "order_id", "left").select(
        F.col("order_id"),
        F.col("event_type").alias("status"),
        F.col("location_id").cast("int").alias("location_id"),
        F.col("brand_id"), F.col("order_total"),
        F.col("ts_seconds").alias("event_ts"),
        F.when(F.col("event_type") == "order_cancelled", F.lit("DELETE"))
         .otherwise(F.lit("UPSERT")).alias("op"),
        # unique per-key ordering: event time, broken by per-order sequence
        (F.col("ts_seconds") * F.lit(100) + F.col("sequence")).alias("seq")))

    feed.coalesce(4).write.mode("overwrite").json(f"{RW}/order_cdc")
    n_orders = sampled.count()
    n_cancel = ev.filter("event_type = 'order_cancelled'").select("order_id").distinct().count()
    n_events = feed.count()
    print(f"[feed] {n_events} CDC events for {n_orders} orders ({n_cancel} real cancellations)")
    return n_orders, n_cancel


def _run(register):
    gid = create_dataflow_graph(spark, default_catalog="spark_catalog",
                                default_database="default", sql_conf={})
    reg = SparkConnectGraphElementRegistry(spark, gid)
    with graph_element_registration_context(reg):
        register()
    handle_pipeline_events(start_run(
        spark, gid, full_refresh=None, full_refresh_all=False, refresh=None,
        dry=False, storage=f"file://{RW}-ckpt/{id(register)}"))


def scenario_a():
    print("\n########## SCENARIO A: orders_live (event stream -> current state) ##########")
    n_orders, n_cancel = build_order_cdc_feed()
    spark.sql("DROP TABLE IF EXISTS ice.gold.orders_live PURGE")

    def reg():
        @dp.table(name="order_cdc_source")
        def order_cdc_source():
            return spark.readStream.schema(ORDER_SCHEMA).json(f"file://{RW}/order_cdc/*.json")
        dp.create_streaming_table("ice.gold.orders_live")
        dp.create_auto_cdc_flow(
            target="ice.gold.orders_live", source="order_cdc_source",
            keys=["order_id"], sequence_by="seq", apply_as_deletes="op = 'DELETE'",
            except_column_list=["op", "seq"], stored_as_scd_type=1)
    _run(reg)

    spark.sql("REFRESH TABLE ice.gold.orders_live")
    live = spark.table("ice.gold.orders_live")
    n_live = live.count()
    print(f"[A] orders_live rows = {n_live}")
    live.groupBy("status").count().orderBy(F.desc("count")).show(20, False)

    # Independent full-recompute: latest non-deleted event_type per order.
    feed = spark.read.schema(ORDER_SCHEMA).json(f"file://{RW}/order_cdc/*.json")
    w = Window.partitionBy("order_id").orderBy(F.desc("seq"))
    naive = (feed.withColumn("rn", F.row_number().over(w)).filter("rn = 1")
             .filter("op != 'DELETE'").select("order_id", "status"))
    pairs = live.select("order_id", "status")
    match = (naive.subtract(pairs).count() == 0 and pairs.subtract(naive).count() == 0
             and n_live == naive.count())
    print(f"[A] AUTO CDC vs full-recompute: match={match} "
          f"(orders_live={n_live}, recompute={naive.count()}, sampled={n_orders}, cancelled={n_cancel})")
    return match


def scenario_b():
    print("\n########## SCENARIO B: dim_locations_current (SCD1 dimension) ##########")
    shutil.rmtree(f"{RW}/dim_cdc", ignore_errors=True); os.makedirs(f"{RW}/dim_cdc")
    base = [{"id": r["id"], "city": r["city"], "op": "UPSERT", "seq": 1}
            for r in spark.read.parquet(LOCATIONS_PARQUET).select("id", "city").collect()]
    changes = [
        {"id": 2, "city": "Los Angeles - Arts District", "op": "UPSERT", "seq": 5},
        {"id": 1, "city": "San Francisco - SoMa",        "op": "UPSERT", "seq": 4},
        {"id": 3, "city": "Chicago",                      "op": "DELETE", "seq": 6},
        {"id": 2, "city": "Los Angeles (STALE)",          "op": "UPSERT", "seq": 3},
    ]
    for name, rows in [("b01", base), ("b02", changes)]:
        with open(f"{RW}/dim_cdc/{name}.json", "w") as fh:
            for e in rows: fh.write(json.dumps(e) + "\n")
    spark.sql("DROP TABLE IF EXISTS ice.silver.dim_locations_current PURGE")

    def reg():
        @dp.table(name="dim_loc_source")
        def dim_loc_source():
            return spark.readStream.schema(DIM_SCHEMA).json(f"file://{RW}/dim_cdc/*.json")
        dp.create_streaming_table("ice.silver.dim_locations_current")
        dp.create_auto_cdc_flow(
            target="ice.silver.dim_locations_current", source="dim_loc_source",
            keys=["id"], sequence_by="seq", apply_as_deletes="op = 'DELETE'",
            except_column_list=["op", "seq"], stored_as_scd_type=1)
    _run(reg)

    spark.sql("REFRESH TABLE ice.silver.dim_locations_current")
    dim = {(r["id"], r["city"]) for r in spark.table("ice.silver.dim_locations_current").collect()}
    print(f"[B] dim_locations_current -> {sorted(dim)}")
    ok = (all("STALE" not in c for _, c in dim)            # stale ignored
          and 3 not in {i for i, _ in dim}                  # closure deleted
          and (1, "San Francisco - SoMa") in dim            # rename applied
          and (2, "Los Angeles - Arts District") in dim)
    print(f"[B] rename applied + closure(delete) honored + stale ignored: {ok}")
    return ok


def main():
    a, b = scenario_a(), scenario_b()
    print(f"\n>>> HOLISTIC E2E RESULT: {'PASS' if (a and b) else 'FAIL'}")
    return 0 if (a and b) else 1


if __name__ == "__main__":
    sys.exit(main())
