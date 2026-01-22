# CLAUDE.md - Agent Guide for lakehouse-stack

## Project Overview

Self-hostable data lakehouse: Spark 4.x + Iceberg 1.10 + Kafka 3.6 + PostgreSQL + SeaweedFS.

## Documentation

| Document | Purpose |
|----------|---------|
| `docs/getting-started/` | Installation, quickstart, configuration |
| `docs/guides/` | CLI reference, streaming, test data, multi-version Spark |
| `docs/guides/unity-catalog.md` | Unity Catalog OSS setup and migration |
| `docs/guides/pipelines.md` | Data pipelines (imperative vs declarative) |
| `docs/deployment/` | Local and AWS deployment |
| `docs/architecture.md` | System design |
| `docs/troubleshooting.md` | Common issues |
| `docs/DEV_WORKFLOW.md` | Branch state and integration plan for agents |
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
./scripts/test-spark-versions.sh                    # Default (Spark 4.1)
./scripts/test-spark-versions.sh -v 4.0 -v 4.1     # Both versions
./scripts/test-spark-versions.sh -t integration    # Integration tests
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
| `jars/` | Required JARs (~860MB) |
| `scripts/` | PySpark examples |
| `tests/` | Test suite |
| `schemas/` | Database migrations |
| `.pre-commit-config.yaml` | Security hooks |

## Architecture

```
Spark 4.x → Iceberg 1.10 → PostgreSQL (metadata) + SeaweedFS (data)
                ↑               ↑
            Kafka 3.6      Unity Catalog (optional REST catalog)
```

**Catalog Options:**
- **PostgreSQL JDBC** (default) - Direct SQL, Spark-only
- **Unity Catalog OSS** (optional) - REST API, multi-client (DuckDB, Trino, etc.)

**Namespaces (Medallion):**
- `iceberg.bronze.*` - Raw data
- `iceberg.silver.*` - Cleaned
- `iceberg.gold.*` - Aggregated

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

## Code Style

- **Python:** 3.10+, Black (88 chars), Ruff
- **PySpark:** `from pyspark.sql import functions as f`
- **Shell:** ShellCheck compliant

## Critical Versions

Do not change without testing:
- AWS SDK v2: **2.24.6** (exact for Hadoop 3.4.1)
- Iceberg: **1.10.0**
- Spark: **4.0.1** or **4.1.0** (Scala 2.13)
- Poetry: **2.1.0**

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
| E2E Pipeline | Full stack test (master only) |

## Common Tasks

```bash
# Submit Spark job
docker exec spark-master-41 /opt/spark/bin/spark-submit /scripts/01-basics.py

# Download JARs (with retry)
./scripts/download-jars.sh
./scripts/download-jars.sh --verify-only  # Check existing JARs

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
```

## For AI Agents

See `docs/DEV_WORKFLOW.md` for:
- Current branch state and relationships
- Integration plan and phase status
- Commands for branch analysis
- Guidelines for feature work

## AI Skills Reference

The `.claude/skills/` directory contains detailed reference guides for AI assistants:

| Skill File | Topic |
|------------|-------|
| `SDP.md` | Spark Declarative Pipelines - complete reference from basics to production |

**When to use skills files:**
- When writing Spark pipelines, read `.claude/skills/SDP.md` first
- Skills files contain patterns, common errors, and lakehouse-specific examples
- They are tested with AI assistants to ensure clarity and completeness
