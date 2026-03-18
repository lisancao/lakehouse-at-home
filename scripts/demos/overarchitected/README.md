# OverArchitected Show Demos

**Premise:** Holly and Nick quit Databricks. They smuggled out their data in OTFs and want to rebuild the platform themselves — for free. Using open source.

**Format:** 60 min recorded (cut to 30). April Fool's 2026. Highly technical audience familiar with Databricks but not OSS setup.

## Prerequisites

```bash
# Core stack
./lakehouse start all
./lakehouse testdata generate --days 7
./lakehouse testdata load

# Unity Catalog (Act 2)
./lakehouse start unity-catalog

# Airflow (Act 5a)
./lakehouse start airflow

# MLflow (Act 6)
docker compose -f docker-compose-mlflow.yml up -d
pip install mlflow>=3.1 anthropic openai

# Ollama (OSS LLM fallback for Act 6)
# curl -fsSL https://ollama.com/install.sh | sh
# ollama pull qwen2.5:14b
```

## Show Flow

| Act | Script | Title | Time | Key Tech |
|-----|--------|-------|------|----------|
| 1 | `01_data_smuggled.py` | "We Have Data" | ~3 min | OTF portability, parquet, Iceberg |
| 2 | `02_unity_catalog_setup.py` | "We Need a Catalog" | ~10 min | UC 0.4.0, REST catalog, credential vending |
| 3 | `03_spark_setup.py` | "We Need Compute" | ~10 min | Spark 4.1 config, VARIANT, CTE, Collation, Connect |
| 4a | `04a_sdp_showcase.py` | "We Need Pipelines — SDP" | ~8 min | Declarative pipelines, 3-act before/after |
| 4b | `04b_rtm_streaming.py` | "We Need Pipelines — RTM" | ~5 min | Real-Time Mode, micro-batch vs RTM |
| 5a | `05a_airflow_sdp.py` | "Scale: Orchestration" | ~5 min | Airflow is the glue — orchestrates SDP, streaming, agents, maintenance |
| 5b | `05b_spark_connect.py` | "Scale: Access" | ~5 min | Spark Connect — thin gRPC client, no JVM, remote users |
| 5c | `05c_spark_k8s.sh` | "Scale: Infrastructure" | ~3 min | Same code, Docker Compose → `--master k8s://` |
| 6a | `06a_mlflow_guardian.py` | "We're Lazy — Guardian" | ~5 min | MLflow agent, table health, maintenance |
| 6b | `06b_mlflow_analyst.py` | "We're Lazy — Analyst" | ~5 min | NL → SQL agent, data Q&A |
| 6c | `06c_mlflow_autopilot.py` | "We're Lazy — Autopilot" | ~5 min | Autonomous monitoring loop |

**Backup scripts** (original numbering): `00_sdp_showcase.py`, `00b_realtime_mode.py`, `01_variant_iceberg.py`, `02_streaming_udtf.py`, `03_full_overarchitected.py`

## Run Commands

### Act 1: Data Smuggled
```bash
docker exec spark-master-41 /opt/spark/bin/spark-submit \
    /scripts/demos/overarchitected/01_data_smuggled.py
```

### Act 2: Unity Catalog
```bash
# With UC running:
docker exec spark-master-41 /opt/spark/bin/spark-submit \
    /scripts/demos/overarchitected/02_unity_catalog_setup.py

# With UC as Spark catalog:
docker exec spark-master-41 /opt/spark/bin/spark-submit \
    --properties-file /opt/spark/conf/spark-defaults-uc.conf \
    /scripts/demos/overarchitected/02_unity_catalog_setup.py
```

### Act 3: Spark Setup
```bash
docker exec spark-master-41 /opt/spark/bin/spark-submit \
    /scripts/demos/overarchitected/03_spark_setup.py
```

### Act 4a: SDP Showcase
```bash
docker exec spark-master-41 /opt/spark/bin/spark-submit \
    /scripts/demos/overarchitected/04a_sdp_showcase.py
```

### Act 4b: RTM + Streaming
```bash
# Start Kafka producer first:
./lakehouse producer &

docker exec spark-master-41 /opt/spark/bin/spark-submit \
    --packages org.apache.spark:spark-sql-kafka-0-10_2.13:4.1.0 \
    /scripts/demos/overarchitected/04b_rtm_streaming.py
```

### Act 5a: Airflow — The Orchestration Backbone
```bash
docker exec spark-master-41 /opt/spark/bin/spark-submit \
    /scripts/demos/overarchitected/05a_airflow_sdp.py

# Check Airflow UI: http://localhost:8085 (admin/admin)
# DAG: lakehouse_sdp_pipeline
#
# Airflow orchestrates the full stack:
#   - SDP pipelines (spark-pipelines run)
#   - Preflight checks (cluster health, data source connectivity)
#   - Post-run verification (table row counts, freshness)
#   - Iceberg maintenance (compaction, snapshot expiry)
#   - Streaming job lifecycle (start/monitor RTM queries)
#   - MLflow agent runs (Guardian, Analyst, Autopilot on schedule)
```

### Act 5b: Spark Connect
```bash
# On cluster (classic mode):
docker exec spark-master-41 /opt/spark/bin/spark-submit \
    /scripts/demos/overarchitected/05b_spark_connect.py

# Start Connect server:
docker exec spark-master-41 /opt/spark/sbin/start-connect-server.sh \
    --master spark://spark-master-41:7078

# Thin client from host:
pip install pyspark-client
python scripts/demos/overarchitected/05b_spark_connect.py --remote sc://localhost:15002
```

### Act 5c: Kubernetes
```bash
# Reference commands (run individually):
bash scripts/demos/overarchitected/05c_spark_k8s.sh
```

### Act 6a: MLflow Guardian
```bash
export ANTHROPIC_API_KEY=sk-...
python scripts/demos/overarchitected/06a_mlflow_guardian.py

# Or with Ollama:
export LLM_PROVIDER=openai
export LLM_MODEL=qwen2.5:14b
export OPENAI_BASE_URL=http://localhost:11434/v1
python scripts/demos/overarchitected/06a_mlflow_guardian.py
```

### Act 6b: MLflow Analyst
```bash
export ANTHROPIC_API_KEY=sk-...
python scripts/demos/overarchitected/06b_mlflow_analyst.py
```

### Act 6c: MLflow Autopilot
```bash
export ANTHROPIC_API_KEY=sk-...
python scripts/demos/overarchitected/06c_mlflow_autopilot.py

# Continuous monitoring mode:
python scripts/demos/overarchitected/06c_mlflow_autopilot.py --monitor --interval 30
```

## Architecture

SVG diagrams in `docs/graphics/`:

| Diagram | File | Description |
|---------|------|-------------|
| Full Stack | [`01_full_architecture.svg`](../../../docs/graphics/01_full_architecture.svg) | All components, connections, and ports |
| Scaling Story | [`02_scaling_story.svg`](../../../docs/graphics/02_scaling_story.svg) | SDP + Airflow + Connect + K8s |
| Show Flow | [`03_show_flow.svg`](../../../docs/graphics/03_show_flow.svg) | 6-act narrative with timing |

**Components:** Spark 4.1 (SDP, RTM, Connect) | Iceberg 1.10 | Kafka 3.6 | Airflow 3.1 | Unity Catalog OSS 0.4.0 | MLflow 3.1 (Gateway, Tracing, Agents) | PostgreSQL 16 | SeaweedFS | Kubernetes (reference)

## Ports

| Service | Port |
|---------|------|
| PostgreSQL | 5432 |
| SeaweedFS | 8333 |
| Spark Master 4.1 | 7078 (UI: 8082) |
| Spark Connect | 15002 |
| Unity Catalog | 8080 |
| Kafka | 9092 |
| Airflow | 8085 |
| MLflow | 5000 |
| Ollama | 11434 |

## Companion Guides

Each act has a companion guide in `docs/guides/overarchitected/`:

| Guide | Topic |
|-------|-------|
| `companion_guide_otf_portability.md` | Open Table Formats, Iceberg, data portability |
| `companion_guide_unity_catalog_oss.md` | UC setup, capabilities, honest gaps |
| `companion_guide_spark41_features.md` | VARIANT, CTE, Collation, config walkthrough |
| `companion_guide_sdp_rtm.md` | Declarative Pipelines + Real-Time Mode |
| `companion_guide_enterprise_scaling.md` | Airflow, Spark Connect, Kubernetes |
| `companion_guide_mlflow_agents.md` | MLflow AI agents, Gateway, tracing |
| `companion_guide_overarchitected_full.md` | Complete show reference |
