# OverArchitected Show — Technical Reference

**Premise:** Holly and Nick quit Databricks. They smuggled out their data in OTFs and want to rebuild the platform — for free, using open source. This doc is the technical scaffold behind each act.

**Stack:** Spark 4.1 | Iceberg 1.10 | Kafka 3.6 | Airflow 3.1 | UC OSS 0.4.0 | MLflow 3.1 | PostgreSQL 16 | SeaweedFS | Docker Compose

**Domain:** Ghost kitchen food delivery — orders, brands, locations, items, 7 lifecycle events per order.

---

## Act 1: "We Have Data" — OTF Portability

**Point:** Open Table Format data is portable. No vendor lock-in.

**Script:** `01_data_smuggled.py`

- Reads raw parquet files (dimensions + events) — proves they work outside any platform
- Shows schemas, row counts, sample data
- Reads Iceberg tables if catalog is configured
- The "smuggled data" premise: if you built on OTFs, your data goes with you

**Key line:** "We left Databricks but we took our Iceberg tables. They work anywhere."

---

## Act 2: "We Need a Catalog" — Unity Catalog OSS 0.4.0

**Point:** Governance isn't optional. UC OSS gives you a REST catalog with real features.

**Script:** `02_unity_catalog_setup.py`

**What's new in UC 0.4.0:**
| Feature | Description |
|---------|-------------|
| Catalog-managed commits | UC coordinates multi-engine writes — no more write conflicts |
| Credential vending | External locations API, storage credentials (AWS IAM role support) |
| Managed storage | `storage_root` on catalogs and schemas |
| REST Iceberg catalog | `http://localhost:8080/api/2.1/unity-catalog/iceberg` |
| Multi-engine access | Same tables from Spark, DuckDB, Trino, Polars, Dremio |

**Config snippet:**
```
spark.sql.catalog.unity = org.apache.iceberg.spark.SparkCatalog
spark.sql.catalog.unity.uri = http://localhost:8080/api/2.1/unity-catalog/iceberg
spark.sql.catalog.unity.type = rest
```

**Key line:** "Catalog-managed commits in 0.4.0 — UC coordinates writes from multiple engines. That was a Databricks-only feature until now."

---

## Act 3: "We Need Compute" — Spark 4.1 Features

**Point:** Spark 4.1 is stacked with new capabilities.

**Script:** `03_spark_setup.py`

### VARIANT Type
```python
df_variant = df.withColumn("body_variant", f.parse_json("body"))
df_extracted = df_variant.withColumn(
    "brand_id", f.expr("variant_get(body_variant, '$.brand_id', 'int')")
).withColumn(
    "total", f.expr("variant_get(body_variant, '$.total', 'double')")
)
```
**Key line:** "Nobody's JSON is consistent. VARIANT means you stop pretending it is."

### Recursive CTEs
```sql
WITH RECURSIVE event_chain AS (
    SELECT order_id, event_type, sequence, 1 AS depth
    FROM order_events WHERE sequence = 0
    UNION ALL
    SELECT e.order_id, e.event_type, e.sequence, c.depth + 1
    FROM order_events e
    JOIN event_chain c ON e.order_id = c.order_id AND e.sequence = c.sequence + 1
)
SELECT * FROM event_chain ORDER BY order_id, depth
```
**Key line:** "Graph queries in Spark. Walk the full order lifecycle as a chain."

### Collation
```sql
SELECT name FROM brands WHERE name COLLATE utf8_lcase LIKE '%pizza%'
```
**Key line:** "One keyword. Case-insensitive. Locale-aware. Done."

---

## Act 4: "We Need Pipelines" — SDP + RTM

### Act 4a: Spark Declarative Pipelines (SDP)

**Script:** `04a_sdp_showcase.py` (three-act structure within the act)

**The paradigm shift:**

| | Imperative (old) | SDP (new) |
|---|---|---|
| Execution order | Manual | Automatic (from `spark.table()` calls) |
| Writing data | Explicit `.write()` | Framework handles it |
| Adding a table | Update execution order, test, pray | Add `@dp.materialized_view`, re-run |
| Streaming | Separate `writeStream` logic | `@dp.table` — same framework |
| Running | `python script.py` | `spark-pipelines run --spec pipeline.yml` |

**Core API:**
```python
from pyspark import pipelines as dp

@dp.materialized_view(name="gold.hourly_metrics")
def hourly_metrics():
    orders = spark.table("iceberg.silver.orders_enriched")
    return orders.filter(f.col("event_type") == "order_created").groupBy(
        "event_date", "event_hour", "city_name"
    ).agg(f.count("order_id").alias("order_count"))
```

**Key lines:**
- "Define WHAT each table contains. Spark handles WHEN and HOW."
- "DLT for everyone. Open source. Runs anywhere."
- "I can add a new gold table live — zero changes to the pipeline runner."

### Act 4b: Real-Time Mode (RTM)

**Script:** `04b_rtm_streaming.py`

**BEFORE vs AFTER:**
```python
# BEFORE: micro-batch — fixed 10s scheduling delay
.trigger(processingTime="10 seconds")

# AFTER: Real-Time Mode — sub-second p99
.trigger(realTime="5 minutes")
```

**Key stats:**
- p99 latency: single-digit milliseconds (stateless)
- Outperformed Flink by 92% on low-latency benchmarks
- OSS Spark 4.1: stateless, Kafka source, Kafka/Foreach sinks
- Databricks Runtime 16.4+: stateful, multi-stage, broader sinks

**SDP connection:** `@dp.table` (streaming) + RTM trigger = declarative sub-second pipelines.

**Key line:** "Same API. One line change. No second engine. Flink is looking over its shoulder."

---

## Act 5: "We Need to Scale" — Airflow + SDP + Connect + K8s

**Four scaling dimensions. Each one solves a different problem.**

| Dimension | Component | What it scales |
|-----------|-----------|---------------|
| Logic | SDP | Pipeline definitions — declarative, auto-deps, 1→100 tables |
| Orchestration | Airflow | Everything else — scheduling, preflight, verification, maintenance, agents |
| Access | Spark Connect | Who can use Spark — thin gRPC clients, no JVM, remote access |
| Infrastructure | K8s | Where it runs — same code, Docker Compose → Kubernetes |

### Act 5a: Airflow — The Orchestration Backbone

**Script:** `05a_airflow_sdp.py` | **DAG:** `dags/sdp_pipeline.py`

**Airflow orchestrates the entire stack, not just SDP:**
1. **SDP pipelines** — `spark-pipelines run --spec spark-pipeline.yml`
2. **Preflight checks** — verify Spark cluster and data sources are accessible
3. **Post-run verification** — check output tables have data, row counts
4. **Iceberg maintenance** — compaction, snapshot expiry, orphan cleanup
5. **Streaming jobs** — start/monitor RTM streaming queries
6. **Agent runs** — trigger MLflow Guardian, Analyst, Autopilot on schedule

**Key line:** "SDP declares the pipeline. Airflow orchestrates everything around it — preflight, verification, maintenance, agent runs. Airflow is the glue."

**Why Airflow, not cron or a shell script:**
- Dependency-aware DAGs — if preflight fails, nothing runs
- Built-in retry/alerting — PagerDuty, Slack integration
- UI for monitoring — you can see every run, every task, every log
- Extensible — custom operators for Spark, Iceberg, MLflow

### Act 5b: Spark Connect — Thin Client Access

**Script:** `05b_spark_connect.py`

**The progression:**
```bash
# 1. Classic: fat client, JVM on every machine
docker exec spark-master-41 spark-submit my_script.py

# 2. Spark Connect: thin gRPC client
pip install pyspark-client
python -c "from pyspark.sql import SparkSession; spark = SparkSession.builder.remote('sc://localhost:15002').getOrCreate()"

# 3. Start the server:
docker exec spark-master-41 /opt/spark/sbin/start-connect-server.sh --master spark://spark-master-41:7078
```

**Why this matters for scaling:**
- No JVM on the client → lightweight access for data scientists, analysts, notebooks
- gRPC protocol → language-agnostic (Python, Go, Rust clients exist)
- Decouple client from cluster → multiple remote users, one cluster

**Key line:** "Connect scales who can use Spark. No fat JVM on every laptop."

### Act 5c: Kubernetes (Reference)

**Script:** `05c_spark_k8s.sh`

Same spark-submit, different master:
```bash
spark-submit --master k8s://https://<k8s-api>:443 \
  --deploy-mode cluster \
  --conf spark.kubernetes.container.image=apache/spark:4.1.0 \
  /scripts/pipelines/pipeline_spark41.py
```

**Key line:** "Same pipeline code. Docker Compose for dev. Kubernetes for prod."

### The Four-Part Pitch (tie it together)

> "SDP scales your logic — declarative pipelines, auto-deps.
> Airflow scales your orchestration — it's the glue that runs everything.
> Connect scales your access — thin clients, no JVM.
> K8s scales your infrastructure — same code, bigger cluster.
> All four together? That's how you go from a Docker Compose demo to production."

---

## Act 6: "We're Lazy" — MLflow AI Agents

**Point:** If we've built all this infrastructure, can AI manage it for us?

**Stack:** MLflow 3.1 + AI Gateway + Tracing (OpenTelemetry) + Agent Serving

### Act 6a: Guardian Agent

**Script:** `06a_mlflow_guardian.py`

**Architecture:** MLflow `ChatAgent` → ResponsesAgent → Tools:
- `inspect_table` — row counts, snapshots, file counts
- `check_quality` — null rates, freshness, duplicate detection
- `run_maintenance` — trigger compaction, snapshot expiry
- `query_table` — arbitrary SQL for investigation

**Every action traced via MLflow Tracing.** LLM calls routed through AI Gateway.

**Key line:** "An agent that runs Iceberg compaction for you. Every tool call traced."

### Act 6b: Analyst Agent

**Script:** `06b_mlflow_analyst.py`

Natural language → SQL → results. "What's the busiest city for orders?" → generates and executes the query.

**Key line:** "Ask a question in English. Get SQL and results."

### Act 6c: Autopilot Agent

**Script:** `06c_mlflow_autopilot.py`

Autonomous monitoring loop: detect data drift → investigate → compact → alert. Runs continuously.

```bash
python 06c_mlflow_autopilot.py --monitor --interval 30
```

**Key line:** "Set it and forget it. The lakehouse manages itself."

### MLflow LLM Configuration
```bash
# Anthropic (default)
export ANTHROPIC_API_KEY=sk-...

# Or local Ollama
export LLM_PROVIDER=openai
export LLM_MODEL=qwen2.5:14b
export OPENAI_BASE_URL=http://localhost:11434/v1
```

---

## Improv Quick Reference

| Curveball | Act | Response |
|-----------|-----|----------|
| "Add real-time streaming" | 4b | Kafka → watermark → Iceberg. For sub-second: `.trigger(realTime='5 minutes')` |
| "Schema changes" | 3 | VARIANT. `parse_json` + `variant_get`. No migration. |
| "Make it declarative" | 4a | SDP. `@dp.materialized_view`. Add table, re-run. Zero execution changes. |
| "Case-insensitive search" | 3 | `COLLATE utf8_lcase`. One keyword. |
| "Event chain" | 3 | Recursive CTE. Walk `(order_id, sequence)`. |
| "Schedule this" | 5a | Airflow DAG wraps `spark-pipelines run`. |
| "Remote access" | 5b | Spark Connect. `pip install pyspark-client`. gRPC, no JVM. |
| "Scale to prod" | 5c | Same code, `--master k8s://`. |
| "Can AI manage it?" | 6a | MLflow Guardian. Inspects, checks quality, runs maintenance. |
| "Query in English" | 6b | MLflow Analyst. NL → SQL → results. |
| "Run itself?" | 6c | MLflow Autopilot. Continuous monitoring loop. |
| "What LLM?" | 6 | AI Gateway → Anthropic, OpenAI, or local Ollama. |
| "Flink?" | 4b | "Same API, 92% faster. No second engine." |
| "Unity Catalog?" | 2 | UC 0.4.0. Catalog-managed commits. Multi-engine. |

---

## Quick Reference: Ports & Commands

| Service | Port |
|---------|------|
| Spark 4.1 Master | 7078 (UI: 8082) |
| Spark Connect | 15002 |
| Kafka | 9092 |
| PostgreSQL | 5432 |
| SeaweedFS S3 | 8333 |
| Unity Catalog | 8080 |
| Airflow | 8085 |
| MLflow | 5000 |
| Ollama | 11434 |

## Companion Guides

Deep-dive references in `docs/guides/overarchitected/`:

| Guide | Acts |
|-------|------|
| `companion_guide_otf_portability.md` | 1 |
| `companion_guide_unity_catalog_oss.md` | 2 |
| `companion_guide_spark41_features.md` | 3 |
| `companion_guide_sdp_rtm.md` | 4a, 4b |
| `companion_guide_enterprise_scaling.md` | 5a, 5b, 5c |
| `companion_guide_mlflow_agents.md` | 6a, 6b, 6c |
