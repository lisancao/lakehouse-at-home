# OverArchitected Show — Lakehouse-at-Home Scaffold

**Purpose:** Build open-source lakehouse architecture live on the Databricks "OverArchitected" show. This document provides heavy Spark 4.1 components, demo scripts, and improv-ready challenges for Lisa's appearance.

**Stack:** Spark 4.0/4.1 | Iceberg 1.10 | Kafka 3.6 | Airflow 3.1 | PostgreSQL 16 | SeaweedFS | Unity Catalog OSS 0.3.1 | Docker Compose

**Domain:** Ghost kitchen / food delivery (orders, brands, locations, items)

---

## 1. Advanced Spark Architecture Components (5–7 Heavy Components)

### Component 1: VARIANT Type for Semi-Structured Order Bodies

**What:** Store the polymorphic `body` JSON (different schema per event type) as Spark 4.1's native VARIANT type instead of `from_json` + fixed schema. Enables schema evolution and shredding.

**Why impressive:** VARIANT is GA in Spark 4.1. Shredding extracts hot paths into Parquet columns for fast reads. No more brittle `StructType` for evolving JSON.

**Runnable PySpark:**

```python
from pyspark.sql import SparkSession
from pyspark.sql import functions as f

spark = SparkSession.builder.appName("VariantDemo").getOrCreate()
spark.sparkContext.setLogLevel("WARN")

# Read orders with body as string
df = spark.read.parquet("/data/events/orders_90d.parquet").limit(1000)

# Convert body to VARIANT (Spark 4.1)
df_variant = df.withColumn("body_variant", f.parse_json("body"))

# Extract fields using variant_get (JSONPath)
df_extracted = df_variant.withColumn(
    "brand_id", f.expr("variant_get(body_variant, '$.brand_id', 'int')")
).withColumn(
    "total", f.expr("variant_get(body_variant, '$.total', 'double')")
).withColumn(
    "driver_id", f.expr("try_variant_get(body_variant, '$.driver_id', 'string')")
)

# Write to Iceberg with VARIANT column
spark.sql("CREATE NAMESPACE IF NOT EXISTS iceberg.overarch")
spark.sql("DROP TABLE IF EXISTS iceberg.overarch.orders_variant")
df_extracted.write.mode("overwrite").saveAsTable("iceberg.overarch.orders_variant")
print("VARIANT table created: iceberg.overarch.orders_variant")
```

**Connection:** Feeds into `silver.orders_enriched`; replaces fixed `body_schema` with flexible VARIANT for event-type-specific bodies.

---

### Component 2: Python UDTF for Order Lifecycle Explosion

**What:** User-Defined Table Function that takes one row per order and explodes it into one row per event type (order_created, kitchen_started, …) with computed durations.

**Why impressive:** UDTFs return tables; they're invoked in `FROM` and enable complex row-to-multi-row logic that's hard to express in pure SQL.

**Runnable PySpark:**

```python
from pyspark.sql import SparkSession
from pyspark.sql import functions as f
from pyspark.sql.udtf import UDTF
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType, TimestampType

spark = SparkSession.builder.appName("UDTFDemo").getOrCreate()
spark.sparkContext.setLogLevel("WARN")

@UDTF
class OrderLifecycleExploder:
    def eval(self, order_id: str, created_at, delivered_at, location_id: int, city_name: str):
        if created_at is None or delivered_at is None:
            return
        from datetime import datetime
        total_mins = (delivered_at - created_at).total_seconds() / 60 if delivered_at else None
        yield (order_id, "order_created", created_at, None, location_id, city_name)
        yield (order_id, "delivered", delivered_at, total_mins, location_id, city_name)

# Register UDTF
spark.udtf.register("order_lifecycle_explode", OrderLifecycleExploder)

# Use in SQL
lifecycle = spark.table("iceberg.silver.order_lifecycle")
spark.sql("""
    SELECT * FROM order_lifecycle_explode(
        (SELECT order_id, created_at, delivered_at, location_id, city_name
         FROM iceberg.silver.order_lifecycle LIMIT 10)
    )
""").show(20, truncate=False)
```

**Connection:** Complements `silver.order_lifecycle`; can be used for event-sourced analytics or audit trails.

---

### Component 3: Approximate Sketches (KLL / Theta) for Percentiles

**What:** Spark 4.1 adds `kll_sketch` and `theta_sketch` for approximate aggregations. Use KLL for order total percentiles without full sort.

**Why impressive:** Sub-linear memory; good for streaming and large datasets. New in Spark 4.1.

**Runnable PySpark:**

```python
from pyspark.sql import SparkSession
from pyspark.sql import functions as f

spark = SparkSession.builder.appName("KLLDemo").getOrCreate()
spark.sparkContext.setLogLevel("WARN")

orders = spark.table("iceberg.silver.orders_enriched").filter(
    f.col("event_type") == "order_created"
)

# KLL sketch for approximate percentiles (Spark 4.1)
from pyspark.sql.functions import kll_sketch, kll_sketch_merge, kll_quantile

sketches = orders.agg(
    kll_sketch("order_total", 0.01).alias("order_total_sketch")
).collect()[0]["order_total_sketch"]

# Or use percentile_approx (available in 4.0/4.1) for simpler demo
p50 = orders.agg(f.percentile_approx("order_total", 0.5).alias("p50")).collect()[0]["p50"]
p95 = orders.agg(f.percentile_approx("order_total", 0.95).alias("p95")).collect()[0]["p95"]
print(f"Order total percentiles: p50={p50:.2f}, p95={p95:.2f}")
```

**Note:** `kll_sketch` may require `spark.sql.adaptive.enabled` and specific config. Fallback: `percentile_approx` is well-supported and still impressive.

**Connection:** Powers `gold.delivery_performance` p95 metrics; can replace exact percentiles for scale.

---

### Component 4: Recursive CTE for Order Event Chain

**What:** Spark 4.1 has recursive CTEs. Model order lifecycle as a graph: each event points to the next by sequence.

**Why impressive:** Recursive CTEs are GA in Spark 4.1; great for graph-like data (event chains, hierarchies).

**Runnable PySpark:**

```python
from pyspark.sql import SparkSession
from pyspark.sql import functions as f

spark = SparkSession.builder.appName("RecursiveCTE").getOrCreate()
spark.sparkContext.setLogLevel("WARN")

# Create temp view of order events
orders = spark.table("iceberg.silver.orders_enriched")
orders.createOrReplaceTempView("order_events")

# Recursive CTE: chain events by (order_id, sequence)
spark.sql("""
    WITH RECURSIVE event_chain AS (
        SELECT order_id, event_id, event_type, event_timestamp, sequence, 1 AS depth
        FROM order_events
        WHERE sequence = 0
        UNION ALL
        SELECT e.order_id, e.event_id, e.event_type, e.event_timestamp, e.sequence, c.depth + 1
        FROM order_events e
        JOIN event_chain c ON e.order_id = c.order_id AND e.sequence = c.sequence + 1
    )
    SELECT order_id, event_type, event_timestamp, depth
    FROM event_chain
    WHERE order_id IN (SELECT order_id FROM order_events LIMIT 5)
    ORDER BY order_id, depth
""").show(30, truncate=False)
```

**Connection:** Alternative to pivot in `order_lifecycle`; useful for path analysis and anomaly detection.

---

### Component 5: Structured Streaming with Iceberg Fanout + Real-Time Mode (RTM)

**What:** Kafka → Spark Structured Streaming → Iceberg (or Foreach) with micro-batch or **Real-Time Mode**. RTM is a new trigger type in Spark 4.1 that processes events as they arrive with p99 latency in single-digit milliseconds.

**Why impressive:** Combines real-time ingestion, Iceberg table format, and checkpointing. **RTM outperformed Apache Flink by up to 92%** on low-latency benchmarks — same Spark API, no second engine. Streaming shuffle passes data between stages immediately.

**BEFORE (micro-batch):**
```python
query = parsed.writeStream \
    .format("iceberg") \
    .outputMode("append") \
    .option("checkpointLocation", "/tmp/checkpoints/orders_stream") \
    .option("fanout-enabled", "true") \
    .trigger(processingTime="10 seconds") \
    .toTable("iceberg.bronze.orders_streaming")
```

**AFTER (Real-Time Mode):**
```python
query = parsed.writeStream \
    .format("kafka") \  # or foreach for OSS; Iceberg RTM support in Databricks
    .outputMode("update") \
    .option("checkpointLocation", "/tmp/checkpoints/orders_rtm") \
    .trigger(realTime="5 minutes") \
    .start()
```

**Key talking points:**
- OSS Spark 4.1: stateless, single-stage, Kafka source, Kafka/Foreach sinks
- Databricks Runtime 16.4+: stateful, multi-stage, broader sink support
- One line change: `.trigger(realTime='5 minutes')` — duration is micro-batch window

**Connection:** Matches `pipeline_sdp.py` `orders_streaming`; unifies batch + streaming. In SDP, streaming is `@dp.table`; with RTM, that runs at sub-second latency.

---

### Component 6: Collation Support for Case-Insensitive String Comparison

**What:** Spark 4.1 collation allows `COLLATE` for locale-aware and case-insensitive string operations.

**Why impressive:** New in Spark 4.1; important for i18n and data quality (e.g., "Pizza" vs "pizza").

**Runnable PySpark:**

```python
from pyspark.sql import SparkSession
from pyspark.sql import functions as f

spark = SparkSession.builder.appName("CollationDemo").getOrCreate()
spark.sparkContext.setLogLevel("WARN")

brands = spark.table("iceberg.bronze.dim_brands")

# Collation for case-insensitive comparison (Spark 4.1)
# Syntax: col COLLATE collation_name
result = spark.sql("""
    SELECT name, name COLLATE utf8_lcase AS name_lower
    FROM iceberg.bronze.dim_brands
    WHERE name COLLATE utf8_lcase LIKE '%pizza%'
""")
result.show(truncate=False)
```

**Connection:** Use in `silver`/`gold` filters when matching brand names, item names, or city names.

---

### Component 7: Spark Declarative Pipelines (SDP) with `@dp.materialized_view`

**What:** Official SDP API: `from pyspark import pipelines as dp` with `@dp.materialized_view` and `@dp.table`. Spark infers DAG and execution order.

**Why impressive:** Shift from imperative to declarative; Spark handles parallelism, retries, checkpoints.

**Runnable PySpark (from existing pipeline_sdp.py):**

```python
from pyspark import pipelines as dp
from pyspark.sql import functions as f

@dp.materialized_view(name="gold.overarch_metrics")
def overarch_metrics():
    orders = spark.table("iceberg.silver.orders_enriched")
    return orders.filter(f.col("event_type") == "order_created").groupBy(
        "event_date", "city_name"
    ).agg(
        f.count("order_id").alias("orders"),
        f.sum("order_total").alias("revenue"),
    )
```

**Connection:** Extends existing SDP pipeline; add new gold tables without touching execution logic.

---

## 2. Spark 4.1 Features to Showcase (Prioritized)

| Feature | Show Value | Demo Complexity |
|---------|------------|-----------------|
| **VARIANT type** | High — semi-structured, shredding | Medium |
| **Spark Declarative Pipelines (SDP)** | High — declarative DAG | Low (already in repo) |
| **Python UDTFs** | High — table-returning functions | Medium |
| **Recursive CTEs** | Medium — graph/chain analytics | Medium |
| **Collation** | Medium — i18n, case-insensitive | Low |
| **Structured Streaming + Iceberg** | High — real-time lakehouse | Medium |
| **Approximate sketches (KLL/Theta)** | Medium — scale-friendly percentiles | Medium |
| **SQL Scripting GA** | Low — improved error handling | N/A |
| **Arrow UDF/UDTF** | High — performance | Advanced |

**Recommended order for show:** SDP → VARIANT → Streaming→Iceberg → UDTF → Recursive CTE → Collation.

---

## 3. Over-Architected Architecture Diagram (ASCII)

```
┌─────────────────────────────────────────────────────────────────────────────────────────┐
│                    LAKEHOUSE-AT-HOME: OVER-ARCHITECTED EDITION                           │
│                    (Every Feature, Wired Together)                                       │
└─────────────────────────────────────────────────────────────────────────────────────────┘

  ┌──────────────┐     ┌──────────────┐     ┌──────────────────────────────────────────┐
  │   Kafka      │     │  Parquet     │     │  Unity Catalog OSS (REST)                  │
  │   :9092      │     │  /data/*     │     │  :8081 (optional)                          │
  └──────┬───────┘     └──────┬───────┘     └──────────────────┬─────────────────────────┘
         │                    │                                │
         │  orders topic       │  dimensions + events            │  catalog metadata
         ▼                    ▼                                ▼
  ┌──────────────────────────────────────────────────────────────────────────────────────┐
  │                         SPARK 4.1 (port 7078, UI 8082)                               │
  │  ┌─────────────────────────────────────────────────────────────────────────────────┐ │
  │  │  Spark Declarative Pipelines (SDP)                                               │ │
  │  │  @dp.materialized_view / @dp.table                                               │ │
  │  └─────────────────────────────────────────────────────────────────────────────────┘ │
  │           │                                                                          │
  │           ▼                                                                          │
  │  ┌─────────────┐   ┌─────────────┐   ┌─────────────┐   ┌─────────────────────────┐  │
  │  │   BRONZE    │   │   BRONZE    │   │   BRONZE    │   │  VARIANT body column    │  │
  │  │  (streaming)│   │  (batch)    │   │  (dimensions)│   │  parse_json → shredding │  │
  │  └──────┬──────┘   └──────┬──────┘   └──────┬──────┘   └───────────┬───────────────┘  │
  │         │                │                 │                       │                   │
  │         └────────────────┴─────────────────┴─────────────────────┘                   │
  │                                    │                                                   │
  │                                    ▼                                                   │
  │  ┌─────────────────────────────────────────────────────────────────────────────────┐  │
  │  │  SILVER: orders_enriched (COLLATION for brand names)                             │  │
  │  │  SILVER: order_lifecycle (Recursive CTE alternative)                             │  │
  │  │  Python UDTF: order_lifecycle_explode()                                          │  │
  │  └─────────────────────────────────────────────────────────────────────────────────┘  │
  │                                    │                                                   │
  │                                    ▼                                                   │
  │  ┌─────────────────────────────────────────────────────────────────────────────────┐  │
  │  │  GOLD: hourly_metrics, delivery_performance (KLL/percentile_approx)              │  │
  │  │  GOLD: brand_summary (collation-aware)                                           │  │
  │  └─────────────────────────────────────────────────────────────────────────────────┘  │
  └──────────────────────────────────────────────────────────────────────────────────────┘
                                         │
                                         ▼
  ┌──────────────────────────────────────────────────────────────────────────────────────┐
  │  Iceberg 1.10 Tables (PostgreSQL catalog :5432, SeaweedFS S3 :8333)                    │
  │  iceberg.bronze.* | iceberg.silver.* | iceberg.gold.*                                 │
  └──────────────────────────────────────────────────────────────────────────────────────┘
                                         │
                                         ▼
  ┌──────────────────────────────────────────────────────────────────────────────────────┐
  │  Airflow 3.1 DAGs → spark-submit → Spark 4.1                                          │
  │  (Orchestrates batch pipelines, maintenance)                                          │
  └──────────────────────────────────────────────────────────────────────────────────────┘
```

---

## 4. Live Demo Script Scaffolds (4 Scripts)

### Demo 0b: Real-Time Mode (RTM) — BEFORE vs AFTER

**File:** `scripts/demos/overarchitected/00b_realtime_mode.py`

**Goal:** Show micro-batch (`processingTime='10 seconds'`) vs Real-Time Mode (`realTime='5 minutes'`). Kafka → parse → Foreach sink with latency metrics. Fallback if RTM unavailable in OSS build.

**Run:** `docker exec spark-master-41 /opt/spark/bin/spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.13:4.1.0 /scripts/demos/overarchitected/00b_realtime_mode.py`

**Prerequisite:** `./lakehouse producer` in another terminal.

---

### Demo 1: Foundation (VARIANT + Iceberg) — ~60 lines

**File:** `scripts/demos/overarchitected/01_variant_iceberg.py`

**Goal:** Ingest orders, convert `body` to VARIANT, extract fields, write to Iceberg.

**Run:** `docker exec spark-master-41 /opt/spark/bin/spark-submit /scripts/demos/overarchitected/01_variant_iceberg.py`

---

### Demo 2: Streaming + UDTF — ~80 lines

**File:** `scripts/demos/overarchitected/02_streaming_udtf.py`

**Goal:** Kafka → Streaming → Iceberg; then UDTF to explode order lifecycle.

**Run:** `docker exec spark-master-41 /opt/spark/bin/spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.13:4.1.0 /scripts/demos/overarchitected/02_streaming_udtf.py`

---

### Demo 3: Full Over-Architected Pipeline — ~100 lines

**File:** `scripts/demos/overarchitected/03_full_overarchitected.py`

**Goal:** VARIANT + Recursive CTE + Collation + SDP-style gold tables. All features in one script.

**Run:** `docker exec spark-master-41 /opt/spark/bin/spark-submit /scripts/demos/overarchitected/03_full_overarchitected.py`

---

## 5. Improv-Ready "Challenge" Ideas

### RTM-Specific Talking Points (use when RTM comes up)

| Curveball | Your move |
|-----------|-----------|
| **"Can you make this lower latency?"** | RTM. One line change: `.trigger(realTime='5 minutes')` |
| **"How does this compare to Flink?"** | "Same API, same engine, 92% faster in benchmarks. No second system to manage." |
| **"What about stateful streaming?"** | "Stateless is GA in OSS Spark 4.1. Stateful is coming — already in preview on Databricks." |

### Challenge 1: "Can you add real-time streaming to this?"

**Solution:** Use existing `test-streaming-iceberg.py` pattern. Kafka source → parse JSON → watermark → Iceberg sink with `fanout-enabled`. Start producer in background. For sub-second latency: add RTM — `.trigger(realTime='5 minutes')`.

**Talking point:** "We'll add a Kafka source, parse the JSON, apply a 10-minute watermark for late data, and write to the same Iceberg table. Exactly-once via checkpointing. For sub-second latency? One line: `.trigger(realTime='5 minutes')`."

---

### Challenge 2: "What if the order body schema changes?"

**Solution:** VARIANT type. `parse_json(body)` stores any JSON; `variant_get(body, '$.new_field', 'string')` extracts new fields without migration.

**Talking point:** "With VARIANT, we don't need a fixed schema. New event types or fields just work. Shredding keeps hot paths fast."

---

### Challenge 3: "Can you add case-insensitive brand search?"

**Solution:** Collation. `WHERE name COLLATE utf8_lcase LIKE '%pizza%'` matches "Pizza Planet", "pizza", etc.

**Talking point:** "Spark 4.1 collation. One keyword change and we get locale-aware, case-insensitive matching."

---

### Challenge 4: "Show me the full event chain for an order."

**Solution:** Recursive CTE joining on `(order_id, sequence)` to walk from event 0 → 1 → 2 → ….

**Talking point:** "Recursive CTE—new in Spark 4.1. We treat events as a graph and traverse the chain."

---

### Challenge 5: "Can you make this declarative instead of imperative?"

**Solution:** SDP. Show `@dp.materialized_view` decorators, `spark.table()` dependencies, and `spark-pipelines run`. No explicit write order.

**Talking point:** "Spark Declarative Pipelines. We define WHAT each table contains. Spark figures out HOW and in what order to run."

---

## Quick Reference: Ports & Commands

| Service | Port | Command |
|---------|------|---------|
| Spark 4.1 Master | 7078 | `docker exec spark-master-41 /opt/spark/bin/spark-submit ...` |
| Spark 4.1 UI | 8082 | http://localhost:8082 |
| Kafka | 9092 | `localhost:9092` |
| SeaweedFS S3 | 8333 | `s3a://lakehouse/warehouse` |
| PostgreSQL | 5432 | `localhost:5432` |

**Prerequisites:**
```bash
./lakehouse start all
./lakehouse testdata generate --days 7
./lakehouse testdata load
```
