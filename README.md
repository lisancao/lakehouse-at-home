# Lakehouse at Home

A composable open-source reference architecture for a Databricks-equivalent data lakehouse. Everything runs locally on Docker, scales to AWS via Terraform, and is deliberately made up of swappable parts — Iceberg **and** Delta, Unity Catalog **or** PostgreSQL JDBC, SeaweedFS today **and** Apache Ozone tomorrow.

[![CI](https://github.com/lisancao/lakehouse-at-home/actions/workflows/ci.yml/badge.svg)](https://github.com/lisancao/lakehouse-at-home/actions/workflows/ci.yml)
[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](https://opensource.org/licenses/MIT)
[![Spark](https://img.shields.io/badge/Spark-4.0%20%7C%204.1-E25A1C?logo=apachespark&logoColor=white)](https://spark.apache.org/)
[![Iceberg](https://img.shields.io/badge/Iceberg-1.10-blue)](https://iceberg.apache.org/)
[![Delta](https://img.shields.io/badge/Delta-4.0-00ADD4)](https://delta.io/)
[![Unity Catalog](https://img.shields.io/badge/Unity%20Catalog-0.4.0-FF3621)](https://www.unitycatalog.io/)
[![MLflow](https://img.shields.io/badge/MLflow-3.1-0194E2?logo=mlflow)](https://mlflow.org/)

## Why this exists

Most "run Spark locally" projects stop at a single-node Spark + a Parquet file. Most Databricks tutorials stop at the Databricks boundary. This repo sits in the middle: a **composable OSS stack** that exposes the same primitives Databricks gives you — open table formats, a governance catalog, declarative pipelines, orchestration, ML lifecycle — assembled from upstream Apache projects so you can:

- **Learn** the lakehouse paradigm on tools that will still exist without any one vendor
- **Prototype** pipelines that will run identically on EMR, Databricks, or any Kubernetes cluster
- **Benchmark** different combinations (Iceberg vs Delta, UC vs JDBC catalog, micro-batch vs RTM)
- **Migrate** data in or out — Iceberg/Delta metadata is portable by design

## Stack

| Layer | Component | Version | Alternatives on this stack |
|-------|-----------|---------|----------------------------|
| Compute | Apache Spark | 4.0 / 4.1 | Spark 3.5 via compose variant |
| Table format | Apache Iceberg | 1.10 | Delta Lake 4.0 side-by-side |
| Streaming | Apache Kafka | 3.6 | Direct-to-Spark via Structured Streaming |
| Orchestration | Apache Airflow | 3.1 | SparkSubmitOperator DAGs in `dags/` |
| Catalog | Unity Catalog OSS | 0.4.0 | PostgreSQL JDBC catalog |
| ML lifecycle | MLflow + AI Gateway | 3.1 | Anthropic + Ollama configured out of the box |
| Metadata DB | PostgreSQL | 16 | Managed RDS in AWS terraform |
| Object store | SeaweedFS | latest | S3 in AWS, Apache Ozone on the roadmap |

Every component ships with an example config (`config/spark/spark-defaults-{uc,delta}.conf.example`) and a compose file (`docker-compose-*.yml`). Turn individual services on and off via the `lakehouse` CLI.

## Requirements

| Resource | Minimum | Recommended |
|----------|---------|-------------|
| RAM | 8 GB | 16 GB |
| Disk | 20 GB | 50 GB |
| CPU | 4 cores | 8 cores |

**Software**: Docker, Java 17+ (21 for Spark 4.1), Python 3.10+, Poetry 2.1+

## Getting started

```bash
git clone https://github.com/lisancao/lakehouse-at-home.git
cd lakehouse-at-home

./lakehouse setup            # validate prereqs, download JARs, install deps
nano .env                    # credentials
nano config/spark/spark-defaults.conf

./lakehouse start all        # Spark + Kafka + Postgres + SeaweedFS
./lakehouse test             # connectivity checks
```

Optional services:

```bash
./lakehouse start unity-catalog   # UC OSS 0.4.0 on :8081
./lakehouse start airflow         # Airflow 3.1 on :8085
./lakehouse start mlflow          # MLflow + AI Gateway
```

See [docs/getting-started/installation.md](docs/getting-started/installation.md) for OS-specific setup and [docs/guides/cli-reference.md](docs/guides/cli-reference.md) for all CLI commands.

### AI-assisted setup

If you're using Claude Code, Cursor, or a similar agent:

```
Clone https://github.com/lisancao/lakehouse-at-home and follow CLAUDE.md to set up locally.
```

The agent will read `CLAUDE.md` and `AGENTS.md` (a compressed Spark 4.1 reference) for context.

## Demos

`scripts/demos/` contains three progressively deeper suites:

| Suite | What you get |
|-------|--------------|
| [`showcase/`](scripts/demos/showcase/README.md) | 13 one-script-per-component demos — OTF portability, VARIANT, UC bootstrap, SDP, Real-Time Mode, Airflow, Spark Connect, Spark on K8s |
| [`mlflow-agents/`](scripts/demos/mlflow-agents/README.md) | Three reference MLflow agents (guardian, analyst, autopilot) that manage the lakehouse using the AI Gateway + Tracing + Model Registry |
| [`transformations/` + root](scripts/demos/README.md) | 5-minute Spark Declarative Pipelines walkthrough with voiceover |

Benchmarks live under [`benchmarks/`](benchmarks/) — a runnable harness across file formats (CSV/Parquet/ORC/JSON), table formats (Iceberg/Delta), and workloads (UDF, Kafka, Iceberg).

## Test data

```bash
./lakehouse testdata generate --days 7    # ghost-kitchen order data
./lakehouse testdata load                 # load to Iceberg
./lakehouse testdata stream --speed 60    # replay to Kafka at 60×
```

See [docs/guides/test-data.md](docs/guides/test-data.md).

## Architecture

```
┌───────────────────────────────────────────────────────────────────────────────────┐
│                          QUERIES / DASHBOARDS / AGENTS                            │
│     Spark SQL · Time Travel · Reports · MLflow-served agents (Guardian/Analyst)   │
└───────────────────────────────────────────────────────────────────────────────────┘
                                         ▲
                                         │
┌─────────────────────┐    ┌───────────────────────────────────────────────────────┐
│  STREAMING          │    │                   COMPUTE: Spark 4.x                  │
│  Kafka  :9092       │───▶│  ┌─────────┐  ┌─────────┐  ┌─────────┐                │
│  Zookeeper :2181    │    │  │ BRONZE  │─▶│ SILVER  │─▶│  GOLD   │   SDP + RTM   │
└─────────────────────┘    │  └─────────┘  └─────────┘  └─────────┘                │
                           │  Spark 4.0 :7077  UI :8080                            │
┌─────────────────────┐    │  Spark 4.1 :7078  UI :8082                            │
│  ORCHESTRATION      │───▶└──────────────────┬───────────────────┬────────────────┘
│  Airflow :8085      │                       │                   │
│  DAGs → SparkSubmit │                       │ Iceberg API       │ Delta API
└─────────────────────┘                       │                   │
                                              ▼                   ▼
┌─────────────────────┐                ┌─────────────────────────────────────────┐
│  ML LIFECYCLE       │                │        CATALOG (pick one or both)        │
│  MLflow Tracking    │───────────────▶│  Unity Catalog  :8081 │ PostgreSQL :5432 │
│  AI Gateway         │   model ref    │  REST, multi-engine  │ JDBC, Spark-only │
│  Anthropic + Ollama │                └──────────────────────┬──────────────────┘
└─────────────────────┘                                       │
                                                              ▼
                                ┌──────────────────────────────────────────────┐
                                │        STORAGE (S3-compatible)                │
                                │   SeaweedFS :8333   →   Apache Ozone (future) │
                                │   s3://lakehouse/warehouse/{bronze,silver,gold}│
                                └──────────────────────────────────────────────┘
```

**Dual-OTF by design.** Iceberg and Delta run against the same Spark cluster and the same object store. Pick one per table or run both; UC speaks Iceberg REST, Spark speaks both natively.

**Catalog options.**

| | PostgreSQL JDBC | Unity Catalog OSS |
|--|---|---|
| Protocol | Direct SQL | REST |
| Clients | Spark only | Spark, DuckDB, Trino, Dremio |
| Governance | Coarse (DB ACLs) | Fine-grained, credential vending |
| Setup | Simpler | More flexible |

## Ports

| Service | Port | UI |
|---------|------|----|
| PostgreSQL | 5432 | - |
| SeaweedFS | 8333 | - |
| Spark 4.0 master | 7077 | http://localhost:8080 |
| Spark 4.1 master | 7078 | http://localhost:8082 |
| Kafka | 9092 | - |
| Zookeeper | 2181 | - |
| Unity Catalog | 8081 | REST API |
| Airflow | 8085 | http://localhost:8085 |
| MLflow Tracking | 5000 | http://localhost:5000 |
| MLflow AI Gateway | 5001 | http://localhost:5001 |
| JupyterLab (optional) | 8889 | http://localhost:8889 |

## Cloud deployment

```bash
# AWS: VPC + S3 + RDS + optional EMR
cd terraform
cp terraform.tfvars.example terraform.tfvars
terraform init && terraform apply
```

See [docs/deployment/aws.md](docs/deployment/aws.md) | Estimated: $50–500/month

```bash
# Databricks (if you want a managed destination for the same Iceberg tables)
cd terraform-databricks
cp terraform.tfvars.example terraform.tfvars
terraform init && terraform apply
```

See [docs/deployment/databricks.md](docs/deployment/databricks.md) | Estimated: $100–800/month

## Testing

```bash
poetry install --with dev,test

poetry run pytest tests/ -v                             # everything
poetry run pytest tests/ --ignore=tests/integration/    # unit only
poetry run pytest tests/integration/ -v                 # integration
poetry run pytest -m sdp -v                             # SDP suite
poetry run pytest -m security -v                        # security
./scripts/connectivity/test-spark-versions.sh -v 4.0 -v 4.1 -t all
```

## Documentation

| Guide | Description |
|-------|-------------|
| [Quickstart](docs/getting-started/quickstart.md) | 5-minute setup |
| [Installation](docs/getting-started/installation.md) | macOS, Ubuntu, Windows |
| [Configuration](docs/getting-started/configuration.md) | Env + Spark config |
| [CLI Reference](docs/guides/cli-reference.md) | All commands |
| [Streaming](docs/guides/streaming.md) | Kafka + Structured Streaming |
| [SDP data sources](docs/guides/sdp-data-sources.md) | Spark Declarative Pipelines compatibility matrix |
| [Airflow](docs/guides/airflow.md) | Orchestration patterns |
| [Multi-Version Spark](docs/guides/multi-version.md) | Run 4.0 and 4.1 together |
| [Unity Catalog](docs/guides/unity-catalog.md) | UC OSS setup & migration |
| [Architecture](docs/architecture.md) | Detailed system design |
| [AWS Deployment](docs/deployment/aws.md) | EMR + S3 + RDS |
| [Databricks Deployment](docs/deployment/databricks.md) | Managed destination |
| [Troubleshooting](docs/troubleshooting.md) | Common issues |
| [Security](SECURITY.md) | Contributor guidelines |
| [AGENTS.md](AGENTS.md) | Spark 4.1 reference for AI coding assistants |

## Roadmap

- **Apache Ozone** as an S3-compatible object store alternative to SeaweedFS (stronger consistency, larger-scale deployments)
- **Kubernetes** deployment path (Helm charts + the existing `spark_on_k8s.sh` demo as the starting point)
- **Trino / DuckDB** query engines wired into Unity Catalog for multi-engine access
- **Dagster** as an alternative orchestrator alongside Airflow
- **Apache Polaris** as a UC alternative catalog
- **OpenLineage** across the whole stack for lineage tracking

## Security

```bash
pre-commit install
pre-commit run --all-files
poetry run pytest -m security -v
```

See [SECURITY.md](SECURITY.md).

## Contributing

1. Open an issue first to discuss non-trivial changes
2. Work off `develop` (CI/CD expects `feat/* → develop → master`)
3. Install pre-commit hooks and run tests before pushing

## License

MIT
