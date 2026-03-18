# OverArchitected Show Prep — Lisa's Guest Appearance

**Date:** Wednesday, March 18, 2026 (morning)
**Show:** Over Architected with Nick & Holly (Databricks)
**Role:** Guest — open source lakehouse builder
**Repo:** [lakehouse-at-home](https://github.com/lisancao/lakehouse-at-home)

---

## THE SHOW — What You're Walking Into

### Format
- **Hosted by:** Nick Karpov and Holly Smith, Staff Developer Advocates at Databricks
- **Style:** Monthly YouTube show. They take the latest features and intentionally integrate ALL of them into a single architecture — over-engineered for both humor and education
- **Tone:** Technical comedy. Part live coding, part improv. They explain features while building absurdly complex architectures that use everything at once
- **Audience:** Databricks practitioners, data engineers, platform engineers. Mix of beginners and senior folks who appreciate the humor

### This Episode's Premise
> Holly and Nick quit Databricks. They smuggled out their data in Open Table Formats. Now they want to rebuild the entire platform from scratch — for free, using open source.

Your repo is the canvas. Each act adds a layer. By the end, you've rebuilt Databricks with open source.

---

## THE NARRATIVE — 6 Acts

The show follows a story arc. Each act builds on the last.

| Act | Title | What You're Proving | Key Tech |
|-----|-------|---------------------|----------|
| **1** | "We Have Data" | OTF data is portable — no vendor lock-in | Parquet, Iceberg |
| **2** | "We Need a Catalog" | Stand up governance from scratch | Unity Catalog OSS 0.4.0, credential vending |
| **3** | "We Need Compute" | Spark 4.1 is loaded with new features | VARIANT, Recursive CTEs, Collation |
| **4a/b** | "We Need Pipelines" | Declarative pipelines + real-time streaming | **SDP**, **RTM** |
| **5a/b/c** | "We Need to Scale" | SDP orchestrated, thin client, K8s-ready | **Airflow+SDP**, **Spark Connect**, K8s |
| **6a/b/c** | "We're Lazy" | Let AI manage the lakehouse | **MLflow agents** — Guardian, Analyst, Autopilot |

### The Scaling Story (Acts 4 → 5)
Four components, each scaling a different dimension:
- **SDP** scales your *pipeline logic*: declarative, auto-dependency, no manual execution order. 1 table to 100 without changing how you run things.
- **Airflow** scales your *orchestration*: schedule SDP, preflight checks, verification, Iceberg maintenance. Airflow is the glue — it orchestrates pipelines, streaming jobs, agent runs, everything.
- **Spark Connect** scales your *access*: thin gRPC client, no JVM on the client side. Multiple remote users, one cluster.
- **K8s** scales your *infrastructure*: same spark-submit, same pipeline code, `--master k8s://`. Docker Compose for dev, Kubernetes for prod.

Together: declarative pipelines, orchestrated by Airflow, accessible via Connect, deployed on K8s. That's the full production story.

---

## YOUR ANGLE — Why You're the Right Guest

### Your Unique Value
1. **Built a production-grade lakehouse that runs on a laptop.** Docker Compose, no cloud account.
2. **Dual Spark versions (4.0 + 4.1).** Before/after for every feature.
3. **Full open-source stack.** Spark, Iceberg, Kafka, Airflow, Unity Catalog, MLflow — all OSS.
4. **Real pipelines, real data.** Medallion architecture with 90 days of ghost kitchen delivery data.
5. **Databricks SA who builds open source.** Bridge between managed platform and OSS community.

### Your Opening Pitch (30 seconds)
> "I built lakehouse-at-home because I wanted real tools, not toy examples. Spark 4.x, Iceberg, Kafka, Airflow, Unity Catalog, MLflow — all running on Docker Compose. Today we're going to see how far open source takes us in rebuilding the whole Databricks platform. Spoiler: pretty far."

---

## THE STACK — Know It Cold

| Component | Version | Purpose | Port |
|-----------|---------|---------|------|
| Apache Spark | 4.0 / 4.1 | Compute engine | 7077/7078 |
| Apache Iceberg | 1.10 | ACID table format | via catalog |
| Apache Kafka | 3.6 | Event streaming | 9092 |
| Apache Airflow | 3.1 | Workflow orchestration | 8085 |
| PostgreSQL | 16 | Catalog + MLflow metadata | 5432 |
| SeaweedFS | — | S3-compatible storage | 8333 |
| Unity Catalog OSS | 0.4.0 | REST catalog, credential vending, catalog-managed commits | 8080 |
| MLflow | 3.1 | Tracking, AI Gateway, agent serving | 5000 |
| Spark Connect | (4.1) | Thin gRPC client | 15002 |
| Ollama | (optional) | Local LLM fallback for agents | 11434 |

### Data Domain: Ghost Kitchen Food Delivery
- **Orders** flow through 7 lifecycle events: order_created → kitchen_started → kitchen_finished → order_ready → driver_arrived → driver_picked_up → delivered
- **Dimensions:** brands (ghost kitchens), items (menu), locations (cities), categories
- **90 days of generated test data** — realistic enough to demo real analytics

---

## FEATURE HIGHLIGHTS — By Act

### Act 1-2: Foundation (Data + Catalog)
- OTF portability: parquet and Iceberg work anywhere, no vendor needed
- Unity Catalog 0.4.0: catalog-managed commits, credential vending, multi-engine access (DuckDB, Trino, Polars)

### Act 3: Spark 4.1 Compute Features
| Feature | One-Liner | Demo Line |
|---------|-----------|-----------|
| **VARIANT** | `parse_json(body)` + `variant_get()` — no fixed schema | "Nobody's JSON is consistent. VARIANT means you stop pretending it is." |
| **Recursive CTEs** | `WITH RECURSIVE` for event chain traversal | "Graph queries in Spark. Walk order lifecycle as a chain." |
| **Collation** | `COLLATE utf8_lcase` for case-insensitive matching | "One keyword. Locale-aware. Done." |

### Act 4: Pipelines (SDP + RTM)
| Feature | One-Liner | Demo Line |
|---------|-----------|-----------|
| **SDP** | `@dp.materialized_view` — define WHAT, Spark handles WHEN/HOW | "DLT for everyone. Open source. Runs anywhere." |
| **RTM** | `.trigger(realTime='5 minutes')` — sub-second p99 | "Same API. One line change. Outperformed Flink by 92%." |
| **SDP + RTM** | `@dp.table` with RTM trigger | "Declarative streaming at sub-second latency." |

### Act 5: Scaling (SDP + Connect + Airflow + K8s)
| Feature | One-Liner | Demo Line |
|---------|-----------|-----------|
| **Airflow + SDP** | Preflight → `spark-pipelines run` → verify → maintain | "Orchestrate SDP like any Spark job. Airflow schedules, SDP declares." |
| **Spark Connect** | Thin gRPC client, no JVM on client | "pip install pyspark-client. Remote cluster access. No fat JARs." |
| **K8s** | spark-submit to Kubernetes | "Same pipeline code. Docker → K8s. Cluster mode." |

### Act 6: MLflow Agents ("We're Lazy")
| Agent | What It Does | Demo Line |
|-------|-------------|-----------|
| **Guardian** (6a) | Inspects table health, checks data quality, triggers maintenance | "An agent that runs Iceberg compaction for you." |
| **Analyst** (6b) | Natural language → SQL, queries the lakehouse | "Ask 'what's the busiest city?' and it writes the SQL." |
| **Autopilot** (6c) | Autonomous monitoring loop — detect drift, fix, alert | "Set it and forget it. The lakehouse manages itself." |

**MLflow stack:** MLflow 3.1 tracking server + AI Gateway (routes to Anthropic/OpenAI/Ollama) + tracing (OpenTelemetry) + agent serving. All on Docker Compose.

---

## DEMO SCRIPTS — Ready to Run

All scripts in `scripts/demos/overarchitected/`. Follow the act order.

### Prerequisites
```bash
./lakehouse start all
./lakehouse testdata generate --days 7
./lakehouse testdata load
./lakehouse start unity-catalog     # Act 2
./lakehouse start airflow           # Act 5a
docker compose -f docker-compose-mlflow.yml up -d  # Act 6
```

| Act | Script | Run Command |
|-----|--------|-------------|
| 1 | `01_data_smuggled.py` | `docker exec spark-master-41 spark-submit /scripts/demos/overarchitected/01_data_smuggled.py` |
| 2 | `02_unity_catalog_setup.py` | `docker exec spark-master-41 spark-submit /scripts/demos/overarchitected/02_unity_catalog_setup.py` |
| 3 | `03_spark_setup.py` | `docker exec spark-master-41 spark-submit /scripts/demos/overarchitected/03_spark_setup.py` |
| 4a | `04a_sdp_showcase.py` | `docker exec spark-master-41 spark-submit /scripts/demos/overarchitected/04a_sdp_showcase.py` |
| 4b | `04b_rtm_streaming.py` | `docker exec spark-master-41 spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.13:4.1.0 /scripts/demos/overarchitected/04b_rtm_streaming.py` |
| 5a | `05a_airflow_sdp.py` | `docker exec spark-master-41 spark-submit /scripts/demos/overarchitected/05a_airflow_sdp.py` |
| 5b | `05b_spark_connect.py` | `docker exec spark-master-41 spark-submit /scripts/demos/overarchitected/05b_spark_connect.py` |
| 6a | `06a_mlflow_guardian.py` | `python scripts/demos/overarchitected/06a_mlflow_guardian.py` |
| 6b | `06b_mlflow_analyst.py` | `python scripts/demos/overarchitected/06b_mlflow_analyst.py` |
| 6c | `06c_mlflow_autopilot.py` | `python scripts/demos/overarchitected/06c_mlflow_autopilot.py` |

**E2E test runner:** `./scripts/demos/overarchitected/run_e2e_test.sh`

**Backup scripts** (original standalone demos): `00_sdp_showcase.py`, `00b_realtime_mode.py`, `01_variant_iceberg.py`, `02_streaming_udtf.py`, `03_full_overarchitected.py`

---

## IMPROV PLAYBOOK — When They Throw Curveballs

### Pipelines & Scaling
| Curveball | Your Move |
|-----------|-----------|
| "Make it declarative" | SDP. `@dp.materialized_view`. Define WHAT, Spark handles the rest. |
| "Can you add a table live?" | Add `@dp.materialized_view`, re-run. Zero changes to execution logic. |
| "How do you schedule this?" | Airflow DAG wraps `spark-pipelines run`. Preflight → run → verify → maintain. |
| "Can remote users query this?" | Spark Connect. `pip install pyspark-client`. gRPC to the cluster. No JVM. |
| "Does this scale to Kubernetes?" | Same spark-submit. `--master k8s://`. Pipeline code doesn't change. |

### Streaming & Latency
| Curveball | Your Move |
|-----------|-----------|
| "Add real-time streaming" | Kafka → watermark → Iceberg with `fanout-enabled`. Exactly-once via checkpointing. |
| "Make it lower latency" | RTM. One line: `.trigger(realTime='5 minutes')`. Sub-second p99. |
| "How does this compare to Flink?" | "Same API, 92% faster in benchmarks. No second system." |
| "What about stateful?" | "Stateless GA in OSS 4.1. Stateful in Databricks Runtime 16.4+ preview." |

### Data & Compute
| Curveball | Your Move |
|-----------|-----------|
| "Schema changes?" | VARIANT. `parse_json` stores any JSON. `variant_get` extracts new fields. No migration. |
| "Case-insensitive search?" | Collation. `COLLATE utf8_lcase`. One keyword. |
| "Event chain traversal?" | Recursive CTE. Walk order lifecycle as a graph. |
| "What about Unity Catalog?" | UC 0.4.0. Docker Compose. Catalog-managed commits, credential vending, multi-engine. |

### AI & Agents
| Curveball | Your Move |
|-----------|-----------|
| "Can AI manage this?" | MLflow Guardian agent. Inspects tables, checks quality, runs Iceberg maintenance. |
| "Can I ask questions in English?" | MLflow Analyst agent. NL → SQL → results. "What's the busiest city?" |
| "Can it run itself?" | MLflow Autopilot. Continuous monitoring loop. Detect drift, compact, alert. |
| "What LLM does it use?" | MLflow AI Gateway routes to Anthropic, OpenAI, or local Ollama. Your choice. |
| "Is the agent traced?" | MLflow Tracing. Every tool call, every LLM interaction. OpenTelemetry native. |

---

## CONVERSATION STARTERS — Drop These Naturally

### The Scaling Story (your throughline)
1. **"Four things scale you from laptop to production: SDP scales your logic, Airflow scales your orchestration, Connect scales your access, K8s scales your infrastructure. Same pipeline code through all of it."**
2. **"SDP is DLT for everyone. Open source. Runs anywhere."**
3. **"Airflow is the glue. It doesn't just schedule SDP — it orchestrates preflight checks, verification, Iceberg maintenance, agent runs. Everything flows through Airflow."**
4. **"Connect means your data scientists don't need a fat JVM client. K8s means your infra team deploys the same code at scale. SDP means your pipeline code doesn't change between any of these."**

### RTM
4. **"RTM beat Flink by 92%. Same Spark API. One line change."**
5. **"In SDP, streaming is `@dp.table`. With RTM, that `@dp.table` runs at sub-second latency."**

### MLflow Agents
6. **"We got lazy. So we built three agents — one monitors table health, one answers questions in English, one runs the whole thing autonomously."**
7. **"MLflow 3.1 + AI Gateway + tracing. The agent calls Iceberg maintenance as a tool. Every action is traced."**

### Open Source
8. **"The whole thing runs on a laptop. 8 GB RAM. Every component is open source. No cloud account."**
9. **"We upgraded Unity Catalog to 0.4.0 — catalog-managed commits, credential vending. The gap between OSS and managed is shrinking fast."**

### SDP Deep-Dive Lines
- "Notice there's no `if __name__` block. No `.write()`. The function just returns a DataFrame."
- "I can add a new gold table right now — zero changes to the pipeline runner."
- "Dependencies are automatic. `spark.table('iceberg.silver.orders')` → framework knows silver runs first."
- "dry-run catches circular deps, missing tables, schema issues before execution."

---

## ARCHITECTURE DIAGRAMS

Three SVG diagrams are in `docs/graphics/` — open in browser or use in presentation:

| Diagram | File | What it shows |
|---------|------|---------------|
| **Full Architecture** | [`01_full_architecture.svg`](graphics/01_full_architecture.svg) | All components: Kafka, Spark 4.1 (SDP/RTM/Connect), Iceberg, UC, Airflow, MLflow, K8s |
| **Scaling Story** | [`02_scaling_story.svg`](graphics/02_scaling_story.svg) | Four-part scaling: SDP (logic) + Airflow (orchestration) + Connect (access) + K8s (infra) |
| **Show Flow** | [`03_show_flow.svg`](graphics/03_show_flow.svg) | 6-act narrative progression with timing, tech per act, HEADLINERS badge on Act 4 |

Quick reference — component map:

| Layer | Components | Ports |
|-------|-----------|-------|
| **Ingestion** | Kafka 3.6, REST APIs, Files | 9092 |
| **Compute** | Spark 4.1 (SDP, RTM, Connect, VARIANT, CTE, Collation, UDTFs) | 7078, 8082, 15002 |
| **Storage** | Iceberg 1.10, SeaweedFS, PostgreSQL 16 | 8333, 5432 |
| **Catalog** | Unity Catalog OSS 0.4.0 | 8080 |
| **Orchestration** | Airflow 3.1 (the glue) | 8085 |
| **Intelligence** | MLflow 3.1 (Gateway, Tracing, Agents) | 5000 |
| **Infrastructure** | Docker Compose (dev) / Kubernetes (prod) | — |

---

## CHECKLIST — Before You Go On

- [ ] `./lakehouse start all` running and healthy
- [ ] `./lakehouse testdata generate --days 7 && ./lakehouse testdata load`
- [ ] `./lakehouse start unity-catalog` running (Act 2)
- [ ] `./lakehouse start airflow` running (Act 5)
- [ ] `docker compose -f docker-compose-mlflow.yml up -d` running (Act 6)
- [ ] `ANTHROPIC_API_KEY` exported (or Ollama running for local LLM)
- [ ] Acts 1-4 tested: `01_data_smuggled.py` through `04b_rtm_streaming.py`
- [ ] Act 5 tested: `05a_airflow_sdp.py`, `05b_spark_connect.py`
- [ ] Act 6 tested: `06a_mlflow_guardian.py` (at minimum)
- [ ] Spark 4.1 UI at http://localhost:8082
- [ ] Airflow UI at http://localhost:8085
- [ ] MLflow UI at http://localhost:5000
- [ ] Know your opening pitch (30 seconds)
- [ ] Reviewed improv playbook above
- [ ] GitHub URL ready: `github.com/lisancao/lakehouse-at-home`

---

## KEY NUMBERS — For When They Ask

| Metric | Value |
|--------|-------|
| Docker services | 10+ (Spark, Kafka, ZK, PostgreSQL, SeaweedFS, UC, Airflow, MLflow) |
| Minimum RAM | 8 GB (16 GB recommended) |
| Test data | 90 days of order lifecycle events across multiple cities |
| Medallion layers | 3 (Bronze, Silver, Gold) |
| Spark versions | 2 (4.0.1 + 4.1.0, side-by-side) |
| Pipeline tables | 11 (4 dims + 2 fact bronze, 2 silver, 3 gold) |
| MLflow agents | 3 (Guardian, Analyst, Autopilot) |
| Demo scripts | 11 (Acts 1-6c) |
| Companion guides | 6 deep-dive docs in `docs/guides/overarchitected/` |
| Setup time | ~5 minutes from `git clone` to first query |

---

## FUTURE EXPLORATION

| Branch | Idea | Notes |
|--------|------|-------|
| `feat/neon-postgres-replacement` | Replace PostgreSQL 16 with Neon | Serverless Postgres (Apache 2.0 OSS). Adds autoscaling, database branching (copy-on-write for catalog/MLflow testing), scale-to-zero. Self-hosting requires pageserver + safekeeper + compute. Best fit: cloud deployment or "serverless lakehouse" episode. Not worth the complexity for Docker Compose local dev — vanilla Postgres is simpler and UC/MLflow just need a JDBC endpoint. |
