# OverArchitected Show Prep — Lisa's Guest Appearance

**Date:** Wednesday, March 18, 2026 (morning)
**Show:** Over Architected with Nick & Holly (Databricks)
**Role:** Guest — open source lakehouse builder
**Repo:** [lakehouse-at-home](https://github.com/lisancao/lakehouse-at-home)

---

## THE SHOW — What You're Walking Into

### Format
- **Hosted by:** Nick Karpov and Holly Smith, Staff Developer Advocates at Databricks
- **Style:** Monthly YouTube show covering Databricks product updates. They take the latest features and intentionally attempt to integrate ALL of them into a single architecture — in "over-engineered" ways for both humor and education
- **Tone:** Technical comedy. Part live coding, part improv. They explain features while demonstrating absurdly complex architectures that use everything at once
- **Cadence:** Monthly episodes (Feb 2025, March 2025, etc.) + live recording at Data+AI Summit
- **Audience:** Databricks practitioners, data engineers, platform engineers. Mix of beginners and senior folks who appreciate the humor

### What Makes This Show Different
1. **Not a tutorial.** It's a "what happens when you use EVERY feature at once" show
2. **Improv energy.** Hosts riff off each other. Expect them to say "okay but what if we ALSO add X?"
3. **Practical foundation.** Despite the humor, they genuinely demonstrate how features work
4. **Intentional over-engineering.** The point is to push things to the limit, then laugh about it

### What They'll Probably Do With You
- Introduce you and lakehouse-at-home as the "open source foundation" they're building on
- Walk through your stack, then start adding Spark 4.1 features one by one
- Keep escalating: "okay we have Iceberg, but what about VARIANT? And streaming? And recursive CTEs?"
- The punchline is always "we built something absurd but it actually works"

---

## YOUR ANGLE — Why You're the Right Guest

### Your Unique Value
1. **You built a fully open-source lakehouse that runs on a laptop.** That's the ultimate "overarchitected home lab" — production-grade infra on Docker Compose
2. **Dual Spark versions (4.0 + 4.1).** You can show the before/after of every Spark 4.1 feature
3. **Real medallion architecture.** Bronze → Silver → Gold with Iceberg — not a toy
4. **Streaming + batch unified.** Kafka → Spark → Iceberg, same tables
5. **You're a Databricks SA who builds open source.** Bridge between managed platform and OSS community

### Your Opening Pitch (30 seconds)
> "I built lakehouse-at-home because I wanted to learn data engineering with real tools, not toy examples. It's Spark 4.x, Iceberg, Kafka, Airflow — all running on Docker Compose on my laptop. And the thing I'm most excited about is Spark Declarative Pipelines — it's DLT for everyone, open source, and it completely changes how you build data pipelines. Today we're going to overarchitect the hell out of it."

---

## THE STACK — Know It Cold

| Component | Version | What It Does | Port |
|-----------|---------|-------------|------|
| Apache Spark | 4.0 / 4.1 | Compute engine | 7077/7078 |
| Apache Iceberg | 1.10 | ACID table format | via catalog |
| Apache Kafka | 3.6 | Event streaming | 9092 |
| Apache Airflow | 3.1 | Workflow orchestration | 8085 |
| PostgreSQL | 16 | Catalog metadata | 5432 |
| SeaweedFS | — | S3-compatible storage | 8333 |
| Unity Catalog OSS | 0.3.1 | REST catalog (optional) | 8081 |

### Data Domain: Ghost Kitchen Food Delivery
- **Orders** flow through 7 lifecycle events: order_created → kitchen_started → kitchen_finished → order_ready → driver_arrived → driver_picked_up → delivered
- **Dimensions:** brands (ghost kitchens), items (menu), locations (cities), categories
- **90 days of generated test data** — realistic enough to demo real analytics

---

## SPARK 4.1 FEATURES — Your Arsenal

Ranked by "show value" — lead with the ones that get the biggest reaction:

### THE HEADLINER: Spark Declarative Pipelines (SDP)
| | |
|---|---|
| **What** | `from pyspark import pipelines as dp` with `@dp.materialized_view` and `@dp.table` |
| **One-liner** | "Define WHAT each table contains. Spark figures out WHEN and HOW to run." |
| **Audience reaction** | "It's like dbt but native to Spark — and it handles streaming too" |
| **Your killer demo** | Side-by-side before/after, then add a new gold table live with zero execution logic changes |
| **Key talking points** | Auto-dependency resolution from `spark.table()` calls. No `.write()`. Unified batch+streaming. `dry-run` validates before execution. |
| **Databricks connection** | "This is DLT for everyone — open source, runs anywhere, no vendor lock-in" |

### CO-HEADLINER: Real-Time Mode (RTM)

| | |
|---|---|
| **What** | New trigger type: `.trigger(realTime='5 minutes')` — processes events as they arrive |
| **One-liner** | "Same Spark API. Sub-second latency. No second engine. Outperformed Flink by 92%." |
| **Audience reaction** | "You mean I don't need Flink for low-latency streaming?" |
| **Your killer demo** | BEFORE: `trigger(processingTime='10 seconds')` vs AFTER: `trigger(realTime='5 minutes')` — latency comparison |
| **Key talking points** | p99 latency in single-digit ms. Streaming shuffle. Kafka source + Foreach/Kafka sinks. Stateless GA in OSS 4.1; stateful in Databricks Runtime 16.4+. |
| **Flink killer** | RTM outperformed Apache Flink by up to 92% on low-latency benchmarks. Same Spark API, no second engine to manage. |
| **SDP connection** | "In SDP, streaming is just @dp.table. With RTM, that @dp.table now runs at sub-second latency." |

### Tier 1: Layer On Top of SDP
| Feature | One-Liner | Reaction |
|---------|-----------|----------|
| **VARIANT type** | `parse_json(body)` + `variant_get(body, '$.brand_id', 'int')` — no fixed schema needed | "Wait, you don't need a StructType anymore?" |
| **Structured Streaming → Iceberg** | Kafka → watermark → Iceberg with `fanout-enabled` + exactly-once. In SDP: just `@dp.table` | Core lakehouse streaming |

### Tier 2: Add When They Say "More"
| Feature | One-Liner | Reaction |
|---------|-----------|----------|
| **Python UDTFs** | Table-returning functions in `FROM` clause | "You can return a whole table from a function?" |
| **Recursive CTEs** | `WITH RECURSIVE` — traverse order event chains | "Graph queries in Spark?" |
| **Collation** | `COLLATE utf8_lcase` — case-insensitive string matching | Quick win, easy to demo |

### Tier 3: If Time Allows
| Feature | One-Liner |
|---------|-----------|
| **KLL Sketches** | Approximate percentiles with sub-linear memory |
| **SQL Scripting** | Multi-statement SQL blocks |
| **Arrow UDFs** | Zero-copy Python UDFs |

---

## DEMO SCRIPTS — Ready to Run

All scripts are in `scripts/demos/overarchitected/` and pre-loaded.

**Lead with Demo 0 (SDP). It's the headline. Everything else layers on top.**

### Prerequisites
```bash
./lakehouse start all
./lakehouse testdata generate --days 7
./lakehouse testdata load
```

### Demo 0: SDP Showcase (THE HEADLINE — RUN THIS FIRST)
```bash
docker exec spark-master-41 /opt/spark/bin/spark-submit \
  /scripts/demos/overarchitected/00_sdp_showcase.py
```
**Three acts:**
- **Act 1:** "The Old Way" — imperative pipeline, manual ordering, explicit writes
- **Act 2:** "The New Way" — `@dp.materialized_view`, auto-dependency resolution, just return DataFrames
- **Act 3:** "Live Extension" — add a new gold table with ZERO changes to execution logic

**Why this leads:** It's the biggest paradigm shift in Spark 4.1. Audience will immediately get it. Nick and Holly will love the before/after. And it sets up every other demo — once you have SDP, you can add VARIANT, streaming, CTEs as new tables without touching the pipeline runner.

**The real SDP pipeline** (show this after the demo):
- Code: `scripts/pipelines/pipeline_sdp.py` — 11 tables, full medallion, batch + streaming
- Config: `scripts/pipelines/spark-pipeline.yml`
- Run: `spark-pipelines run --spec scripts/pipelines/spark-pipeline.yml`
- Validate: `spark-pipelines dry-run --spec scripts/pipelines/spark-pipeline.yml`

### Demo 0b: Real-Time Mode (RTM) — CO-HEADLINER
```bash
docker exec spark-master-41 /opt/spark/bin/spark-submit \
  --packages org.apache.spark:spark-sql-kafka-0-10_2.13:4.1.0 \
  /scripts/demos/overarchitected/00b_realtime_mode.py
```
**Shows:** BEFORE (micro-batch `processingTime='10 seconds'`) vs AFTER (RTM `realTime='5 minutes'`). Kafka → parse → Foreach sink with latency metrics. Fallback if RTM trigger unavailable in OSS build. **Prerequisite:** Run `./lakehouse producer` in another terminal.

### Demo 1: VARIANT + Iceberg
```bash
docker exec spark-master-41 /opt/spark/bin/spark-submit \
  /scripts/demos/overarchitected/01_variant_iceberg.py
```
**Shows:** parse_json → variant_get → Iceberg write. "What if the JSON body schema changes? VARIANT means you never migrate again."

### Demo 2: Streaming + UDTF
```bash
docker exec spark-master-41 /opt/spark/bin/spark-submit \
  --packages org.apache.spark:spark-sql-kafka-0-10_2.13:4.1.0 \
  /scripts/demos/overarchitected/02_streaming_udtf.py
```
**Shows:** Kafka → watermark → Iceberg sink + Python UDTF for lifecycle explosion. "In SDP, this is just `@dp.table` instead of `@dp.materialized_view`."

### Demo 3: Full Over-Architected Pipeline
```bash
docker exec spark-master-41 /opt/spark/bin/spark-submit \
  /scripts/demos/overarchitected/03_full_overarchitected.py
```
**Shows:** VARIANT + Recursive CTE + Collation + gold aggregations. Every feature in one pipeline.

---

## IMPROV PLAYBOOK — When They Throw Curveballs

### "Can you make this lower latency?"
**Your move:** RTM. "One line change: `.trigger(realTime='5 minutes')`. Same API, sub-second p99. No second engine."

### "How does this compare to Flink?"
**Your move:** "Same API, same engine, 92% faster in benchmarks. No second system to manage."

### "What about stateful streaming?"
**Your move:** "Stateless is GA in OSS Spark 4.1. Stateful is coming — already in preview on Databricks Runtime 16.4+."

### "Can you add real-time streaming?"
**Your move:** Kafka source → parse JSON → 10-minute watermark → Iceberg sink with `fanout-enabled`. "Exactly-once via checkpointing. Same Iceberg tables as batch." For sub-second: "Add RTM — `.trigger(realTime='5 minutes')`."

### "What if the order body schema changes?"
**Your move:** VARIANT. "parse_json stores any JSON. variant_get extracts new fields without migration. Shredding keeps hot paths fast."

### "Can you do case-insensitive search?"
**Your move:** Collation. `WHERE name COLLATE utf8_lcase LIKE '%pizza%'`. "One keyword. Locale-aware matching. Spark 4.1."

### "Show me the full lifecycle of an order."
**Your move:** Recursive CTE. "We treat events as a graph. Walk from sequence 0 → 1 → 2 → delivered. New in Spark 4.1."

### "Make it declarative."
**Your move:** Pull up `pipeline_sdp.py`. "@dp.materialized_view. Define WHAT each table contains. Spark figures out the rest."

### "What about Unity Catalog?"
**Your move:** "Already in the repo. Docker Compose up, REST catalog at 8081. Multi-engine — same tables readable from DuckDB, Trino, Dremio."

### "Can you deploy this to the cloud?"
**Your move:** "Terraform in the repo. AWS or Databricks. Same pipeline code, different infra."

---

## CONVERSATION STARTERS — Things to Bring Up Naturally

1. **"SDP changes how you think about pipelines. You stop writing execution code and start defining data."** (Lead with this)
2. **"The before/after is wild — my imperative pipeline is 360 lines with manual ordering. The SDP version is the same logic but the framework handles execution, retries, and parallelism."**
3. **"What makes SDP special is that streaming is just a decorator change — `@dp.table` instead of `@dp.materialized_view`. Same pipeline, same graph, batch and streaming unified."**
4. **"Spark 4.1 SDP is basically Databricks DLT for everyone — open source, runs anywhere."** (The line that will land hardest)
5. **"VARIANT is a game-changer for real-world data. Nobody's JSON is actually consistent."**
6. **"We run Spark 4.0 AND 4.1 side by side. Same data, different features. Perfect for migration testing."**
7. **"The whole thing runs on 8GB RAM. Over-architected, but efficient."**

### SDP-Specific Lines (for when you're deep in the demo)
- "Notice there's no `if __name__` block. No `.write()`. The function just returns a DataFrame."
- "I can add a new gold table right now — watch, zero changes to the pipeline runner."
- "Dependencies? Automatic. If this function calls `spark.table('iceberg.silver.orders')`, the framework knows silver must run first."
- "dry-run catches circular dependencies, missing tables, and schema issues before you run anything."

---

## ARCHITECTURE DIAGRAM — The Full Over-Architected Picture

```
┌─────────────────────────────────────────────────────────────────────────┐
│              LAKEHOUSE-AT-HOME: OVER-ARCHITECTED EDITION                │
└─────────────────────────────────────────────────────────────────────────┘

  Kafka (:9092)          Parquet /data/*         Unity Catalog (:8081)
  orders topic           dimensions + events     REST catalog
       │                      │                       │
       ▼                      ▼                       ▼
┌─────────────────────────────────────────────────────────────────────────┐
│                    SPARK 4.1 (port 7078, UI 8082)                       │
│  ┌────────────────────────────────────────────────────────────────────┐ │
│  │  Spark Declarative Pipelines (SDP)                                 │ │
│  │  @dp.materialized_view / @dp.table                                 │ │
│  └────────────────────────────────────────────────────────────────────┘ │
│         │                                                               │
│         ▼                                                               │
│  ┌─────────────┐  ┌─────────────┐  ┌─────────────┐  ┌──────────────┐  │
│  │   BRONZE    │  │   BRONZE    │  │   BRONZE    │  │   VARIANT    │  │
│  │ (streaming) │  │  (batch)    │  │ (dimensions)│  │  body col    │  │
│  └──────┬──────┘  └──────┬──────┘  └──────┬──────┘  └──────┬───────┘  │
│         └────────────────┴─────────────────┴────────────────┘          │
│                                  │                                      │
│                                  ▼                                      │
│  ┌────────────────────────────────────────────────────────────────────┐ │
│  │  SILVER: orders_enriched (COLLATION for brand names)               │ │
│  │  SILVER: order_lifecycle (Recursive CTE alternative)               │ │
│  │  Python UDTF: order_lifecycle_explode()                            │ │
│  └────────────────────────────────────────────────────────────────────┘ │
│                                  │                                      │
│                                  ▼                                      │
│  ┌────────────────────────────────────────────────────────────────────┐ │
│  │  GOLD: hourly_metrics, delivery_performance (percentile_approx)    │ │
│  │  GOLD: brand_summary (collation-aware)                             │ │
│  └────────────────────────────────────────────────────────────────────┘ │
└─────────────────────────────────────────────────────────────────────────┘
                                   │
                                   ▼
┌─────────────────────────────────────────────────────────────────────────┐
│  Iceberg 1.10 (PostgreSQL catalog :5432, SeaweedFS S3 :8333)            │
│  iceberg.bronze.*  |  iceberg.silver.*  |  iceberg.gold.*               │
└─────────────────────────────────────────────────────────────────────────┘
                                   │
                                   ▼
┌─────────────────────────────────────────────────────────────────────────┐
│  Airflow 3.1 → spark-submit → Spark 4.1 (batch orchestration)          │
└─────────────────────────────────────────────────────────────────────────┘
```

---

## CHECKLIST — Before You Go On

- [ ] `./lakehouse start all` is running and healthy
- [ ] `./lakehouse testdata generate --days 7 && ./lakehouse testdata load` completed
- [ ] **SDP demo tested first:** `00_sdp_showcase.py` runs clean (this is your opener)
- [ ] **RTM demo tested:** `00b_realtime_mode.py` (run `./lakehouse producer` in another terminal first)
- [ ] Other demos tested: `01_variant_iceberg.py`, `02_streaming_udtf.py`, `03_full_overarchitected.py`
- [ ] Spark 4.1 UI accessible at http://localhost:8082
- [ ] Know your opening pitch (30 seconds — leads with SDP)
- [ ] Can explain the before/after: imperative → declarative (see `docs/sdp-before-after.md`)
- [ ] Reviewed improv playbook above
- [ ] Have the GitHub repo URL ready: `github.com/lisancao/lakehouse-at-home`

---

## KEY NUMBERS — For When They Ask

| Metric | Value |
|--------|-------|
| Docker images | 6 containers (Spark master + worker, Kafka, Zookeeper, PostgreSQL, SeaweedFS) |
| Minimum RAM | 8 GB (16 GB recommended) |
| Test data | 90 days of order lifecycle events across multiple cities |
| Medallion layers | 3 (Bronze, Silver, Gold) |
| Spark versions | 2 (4.0.1 + 4.1.0, run side-by-side) |
| Pipeline tables | 11 total (4 dims + 2 fact bronze, 2 silver, 3 gold) |
| Lines of pipeline code | ~350 (SDP), ~360 (imperative) |
| Setup time | ~5 minutes from `git clone` to first query |
