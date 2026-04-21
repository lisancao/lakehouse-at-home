# CLAUDE.md - Agent Guide for lakehouse-stack

## Project Overview

Composable OSS lakehouse reference architecture. Spark 4.x + (Iceberg 1.10 **and** Delta 4.0) + Kafka 3.6 + PostgreSQL + SeaweedFS + Unity Catalog OSS 0.4.0 + MLflow 3.1 + Airflow 3.1. Runs locally on Docker, deploys to AWS via Terraform. See `README.md` for positioning and roadmap (Apache Ozone, Kubernetes).

## Documentation

| Document | Purpose |
|----------|---------|
| `docs/getting-started/` | Installation, quickstart, configuration |
| `docs/guides/` | CLI reference, streaming, test data, multi-version Spark, Airflow orchestration |
| `docs/guides/unity-catalog.md` | Unity Catalog OSS setup and migration |
| `docs/guides/pipelines.md` | Data pipelines (imperative vs declarative) |
| `docs/guides/airflow.md` | Apache Airflow orchestration |
| `docs/deployment/` | Local and AWS deployment |
| `docs/architecture.md` | System design |
| `docs/troubleshooting.md` | Common issues |
| `docs/DEV_WORKFLOW.md` | Development workflow: always work from develop, test locally |
| `SECURITY.md` | Security guidelines for contributors |
| `.claude/skills/` | AI assistant skill files (see below) |

## Quick Commands

```bash
# Setup and validation
./lakehouse setup          # Validate prereqs, download JARs, create DB
./lakehouse check-config   # Validate credential consistency
./lakehouse preflight      # Test service connectivity

# Service management
./lakehouse start all      # Start Spark + Kafka
./lakehouse stop all       # Stop all services
./lakehouse status         # Human-readable status
./lakehouse status --json  # Machine-readable status
./lakehouse test           # Connectivity tests (returns exit code)
./lakehouse logs <service> # View logs (spark-master, kafka, etc.)

# Unity Catalog (optional)
./lakehouse start unity-catalog  # Start Unity Catalog REST server
./lakehouse stop unity-catalog   # Stop Unity Catalog
./lakehouse logs unity-catalog   # View Unity Catalog logs

# Airflow (optional - requires Airflow 3.x)
./lakehouse start airflow   # Start Airflow scheduler and API server
./lakehouse stop airflow    # Stop Airflow
./lakehouse logs airflow    # View Airflow logs

# Database migrations
./lakehouse migrate        # Apply schema migrations
./lakehouse migrate --dry-run  # Preview migrations
```

## Testing

```bash
# Install test dependencies
poetry install --with dev,test

# Run all tests
poetry run pytest tests/ -v

# Run by category
poetry run pytest tests/ --ignore=tests/integration/     # Unit only
poetry run pytest tests/integration/ -v                   # Integration only
poetry run pytest -m security -v                          # Security only
poetry run pytest -m spark41 -v                           # Spark 4.1 only

# Multi-version Spark testing
./scripts/connectivity/test-spark-versions.sh                    # Default (Spark 4.1)
./scripts/connectivity/test-spark-versions.sh -v 4.0 -v 4.1     # Both versions
./scripts/connectivity/test-spark-versions.sh -t integration    # Integration tests
```

## Test Data

```bash
./lakehouse testdata generate --days 7   # Generate 7 days of order data
./lakehouse testdata load                # Load to Iceberg tables
./lakehouse testdata stream --speed 60   # Stream to Kafka at 60x speed
```

## Key Files

| Path | Purpose |
|------|---------|
| `lakehouse` | CLI script (bash) |
| `.env` | Credentials (from .env.example) - **NOT in git** |
| `config/spark/spark-defaults.conf` | Spark config - **NOT in git** |
| `config/spark/spark-defaults-uc.conf` | Spark config for Unity Catalog - **NOT in git** |
| `docker-compose-spark41.yml` | Spark 4.1 cluster (default) |
| `docker-compose.yml` | Spark 4.0 cluster |
| `docker-compose-kafka.yml` | Kafka + Zookeeper |
| `docker-compose-unity-catalog.yml` | Unity Catalog OSS server |
| `docker-compose-mlflow.yml` | MLflow tracking + AI Gateway |
| `docker-compose-airflow.yml` | Apache Airflow orchestration |
| `docker-compose-notebooks.yml` | JupyterLab (Spark 4.1 via `docker/jupyter/Dockerfile`) |
| `config/mlflow/gateway-config.yml` | MLflow AI Gateway routes (Anthropic + Ollama) |
| `config/spark/spark-defaults-{uc,delta}.conf.example` | Spark config for UC / Delta |
| `dags/` | Airflow DAG definitions (incl. `sdp_pipeline.py`) |
| `jars/` | Required JARs (~860MB, gitignored) |
| `scripts/quickstarts/` | Tutorials (01-04) |
| `scripts/connectivity/` | Integration test scripts (run via CLI) |
| `scripts/pipelines/` | Spark pipeline scripts (SDP, Spark 4.0/4.1) |
| `scripts/demos/showcase/` | One-script-per-stack-component demos |
| `scripts/demos/mlflow-agents/` | MLflow AI Gateway reference agents |
| `scripts/demos/transformations/` | SDP walkthrough pipelines |
| `scripts/tools/` | Utility scripts (download-jars, worktree, etc.) |
| `scripts/testdata/` | Test data generator |
| `benchmarks/` | Runnable perf harness (file/table formats, workloads) |
| `tests/` | Test suite (`integration/sdp/` is SDP-specific) |
| `schemas/` | Database migrations |
| `terraform/` | AWS self-hosted deployment |
| `terraform-databricks/` | Databricks managed destination |
| `AGENTS.md` | Spark 4.1 reference for AI assistants |
| `.pre-commit-config.yaml` | Security hooks |

## Architecture

```
Spark 4.x  →  Iceberg 1.10 / Delta 4.0 (dual-OTF)  →  Catalog  →  SeaweedFS (S3 API)
     ↑                                                   ↑
   Kafka 3.6                            PostgreSQL JDBC  |  Unity Catalog OSS (0.4.0)
     ↑
   Airflow 3.1                          MLflow 3.1 + AI Gateway (Anthropic + Ollama)
```

**Catalog Options:**
- **PostgreSQL JDBC** (default) - Direct SQL, Spark-only
- **Unity Catalog OSS** (optional) - REST API, multi-client (DuckDB, Trino, etc.), 0.4.0 catalog-managed commits

**OTF Options (mix freely):**
- **Iceberg** for the default bronze/silver/gold path
- **Delta** runs on the same cluster via `config/spark/spark-defaults-delta.conf.example`

**Namespaces (Medallion):**
- `iceberg.bronze.*` / `delta.bronze.*` - Raw
- `iceberg.silver.*` / `delta.silver.*` - Cleaned
- `iceberg.gold.*` / `delta.gold.*` - Aggregated

## Ports

| Service | Port |
|---------|------|
| PostgreSQL | 5432 |
| SeaweedFS | 8333 |
| Spark 4.0 | 7077 (UI: 8080) |
| Spark 4.1 | 7078 (UI: 8082) |
| Kafka | 9092 |
| Zookeeper | 2181 |
| Unity Catalog | 8081 (when running with Spark) |
| Airflow | 8085 |
| MLflow Tracking | 5000 |
| MLflow AI Gateway | 5001 |
| JupyterLab (optional) | 8889 |

## Code Style

- **Python:** 3.10+, Black (88 chars), Ruff
- **PySpark:** `from pyspark.sql import functions as f`
- **Shell:** ShellCheck compliant

## Critical Versions

Do not change without testing:
- AWS SDK v2: **2.24.6** (exact for Hadoop 3.4.1)
- Iceberg: **1.10.0**
- Spark: **4.0.1** or **4.1.0** (Scala 2.13)
- Airflow: **3.1.6** (breaking changes from 2.x - see `docs/guides/airflow.md`)
- Poetry: **2.1.0**

**Java versions** (don't change - these are set by official images):
- Spark 4.0 container: Java 17 (from `apache/spark:4.0.1-scala2.13-java17-*`)
- Spark 4.1 container: Java 21 (from `apache/spark:4.1.0-scala2.13-java21-*`)
- Airflow container: Java 17 (sufficient for scheduling; Spark jobs run in Spark containers)

## Security

**NEVER commit credentials.** See `SECURITY.md` for full guidelines.

```bash
# Install pre-commit hooks
pre-commit install

# Run security checks manually
pre-commit run --all-files

# Run security tests
poetry run pytest -m security -v
```

**Pre-commit hooks enforce:**
- No hardcoded secrets (detect-secrets)
- No private keys
- Python security (bandit)
- Shell security (shellcheck)

## CI/CD

GitHub Actions workflow (`.github/workflows/ci.yml`):

| Stage | Description |
|-------|-------------|
| Lint & Validate | Ruff, Black, shell syntax, compose validation |
| Unit Tests | pytest (non-integration) |
| Container Startup | Verify Spark 4.0/4.1 images |
| Integration Tests | PostgreSQL, Kafka connectivity |
| Spark Matrix | Parallel Spark 4.0 + 4.1 tests |
| Airflow Validation | DAG validation and container tests |
| E2E Pipeline | Full stack test (master only) |

## Common Tasks

```bash
# Submit Spark job
docker exec spark-master-41 /opt/spark/bin/spark-submit /scripts/quickstarts/01-basics.py

# Download JARs (with retry)
./scripts/tools/download-jars.sh
./scripts/tools/download-jars.sh --verify-only  # Check existing JARs

# Format and lint
poetry run black scripts/ tests/
poetry run ruff check scripts/ tests/ --fix

# Create PR
gh pr create --base develop --title "feat: description"
```

## Troubleshooting

See `docs/troubleshooting.md` for full guide.

```bash
./lakehouse test              # Test all services
./lakehouse status --json     # Check health
./lakehouse check-config      # Validate credentials
docker logs spark-master-41   # Spark logs
docker logs kafka             # Kafka logs
docker logs airflow-webserver # Airflow logs
```

## For AI Agents

**Golden rule: Always work from `develop`. Always test locally before pushing.**

```bash
git pull origin develop              # Start here
# ... make changes ...
poetry run pytest tests/ -v          # Test locally
git push origin develop              # Only after tests pass
```

See `docs/DEV_WORKFLOW.md` for full workflow details.

## AI Skills Reference

Two top-level references for AI assistants:

| File | Topic |
|------|-------|
| `AGENTS.md` | Compressed Spark 4.1 reference (always in context, no skill invocation) |
| `.claude/skills/SDP.md` | Spark Declarative Pipelines — full reference, patterns, common errors |

**When to use:**
- `AGENTS.md` is compact enough to load up-front — read it before writing any PySpark
- `.claude/skills/SDP.md` is deeper — read before designing or debugging an SDP pipeline
