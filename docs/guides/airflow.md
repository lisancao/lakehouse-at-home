# Airflow Orchestration Guide

Orchestrate Spark jobs, Kafka sensors, and Iceberg maintenance with Apache Airflow 3.x.

## Version

This setup uses **Airflow 3.1.6** with Python 3.12. See [Airflow 3.x Notes](#airflow-3x-notes) for breaking changes from 2.x.

## Architecture

```
┌──────────────────────────────────────────────────────────────┐
│                      Airflow (port 8085)                     │
│  ┌─────────────┐  ┌─────────────┐  ┌─────────────────────┐   │
│  │ API Server  │  │  Scheduler  │  │     Triggerer       │   │
│  └─────────────┘  └─────────────┘  └─────────────────────┘   │
└──────────────────────────────────────────────────────────────┘
         │                   │                    │
         │ docker exec       │ Kafka sensors      │ Airflow DB
         │ spark-submit      │ (wait for data)    │ (task state)
         ▼                   ▼                    ▼
┌─────────────┐      ┌─────────────┐      ┌─────────────┐
│    Spark    │      │    Kafka    │      │  PostgreSQL │
│  4.0 / 4.1  │      │   Broker    │      │ (Airflow's) │
└─────────────┘      └─────────────┘      └─────────────┘
         │
         │ Spark talks to
         │ Iceberg catalog
         ▼
┌─────────────────────────────────────────┐
│              Iceberg Tables             │
│  bronze.* → silver.* → gold.*           │
└─────────────────────────────────────────┘
```

**Key point**: Airflow orchestrates Spark jobs via `docker exec spark-submit`. Airflow does not talk directly to Iceberg - Spark handles all Iceberg operations.

## Quick Start

```bash
# 1. Start prerequisites (Spark + Kafka + PostgreSQL)
./lakehouse start all

# 2. Start Airflow
./lakehouse start airflow

# 3. Access UI
open http://localhost:8085
# Login with credentials from your .env file (AIRFLOW_ADMIN_USER/AIRFLOW_ADMIN_PASSWORD)
```

## Configuration

### Environment Variables

Add to `.env` (see `.env.example`):

```bash
# Airflow admin credentials
AIRFLOW_ADMIN_USER=admin
AIRFLOW_ADMIN_PASSWORD=your-secure-password

# Fernet key for encryption (generate with: python -c "from cryptography.fernet import Fernet; print(Fernet.generate_key().decode())")
AIRFLOW_FERNET_KEY=your-fernet-key
```

### Connections

Connections are pre-configured in the Docker setup. To customize:

```bash
# Run setup script inside Airflow container
docker exec airflow-webserver /opt/airflow/config/setup_connections.sh
```

Pre-configured connections:
| Connection ID | Type | Description |
|---------------|------|-------------|
| `kafka_default` | Kafka | Broker at localhost:9092 |
| `spark_41` | Spark | Spark 4.1 master (port 7078) |
| `spark_40` | Spark | Spark 4.0 master (port 7077) |
| `postgres_iceberg` | PostgreSQL | Iceberg catalog metadata |

### Variables

Set Spark version for DAGs:

```bash
# Via CLI
docker exec airflow-webserver airflow variables set spark_version "4.1"

# Or in UI: Admin → Variables
```

## Included DAGs

### 1. Medallion Pipeline (`lakehouse_medallion_pipeline`)

Orchestrates the bronze → silver → gold data pipeline.

**Schedule:** Daily
**Tasks:**
1. `wait_for_kafka_data` - Kafka sensor (optional, soft-fail)
2. `choose_spark_version` - Branch based on `spark_version` variable
3. `run_pipeline_spark41` / `run_pipeline_spark40` - Execute Spark job
4. `verify_tables` - Validate row counts

**Manual trigger:**
```bash
docker exec airflow-webserver airflow dags trigger lakehouse_medallion_pipeline
```

### 2. Iceberg Maintenance (`iceberg_maintenance`)

Performs routine Iceberg table maintenance.

**Schedule:** Daily at 3 AM
**Tasks per table:**
1. `expire_snapshots_*` - Remove snapshots older than 7 days
2. `remove_orphans_*` - Clean orphan files older than 3 days
3. `compact_files_*` - Rewrite small files (target: 128MB)

**Tables maintained:**
- `iceberg.bronze.orders`
- `iceberg.silver.orders_clean`
- `iceberg.gold.daily_summary`

### 3. On-Demand Compaction (`iceberg_compact_on_demand`)

Manual compaction for specific tables.

**Schedule:** None (manual trigger only)
**Parameters:**
- `table`: Table to compact (default: `iceberg.bronze.orders`)
- `target_size_mb`: Target file size in MB (default: 128)

**Trigger with parameters:**
```bash
docker exec airflow-webserver airflow dags trigger iceberg_compact_on_demand \
  --conf '{"table": "iceberg.silver.orders_clean", "target_size_mb": 256}'
```

## CLI Commands

```bash
# Start Airflow
./lakehouse start airflow

# Stop Airflow
./lakehouse stop airflow

# View logs
./lakehouse logs airflow-webserver
./lakehouse logs airflow-scheduler
./lakehouse logs airflow-triggerer

# Check status
./lakehouse status
```

## Writing Custom DAGs

Place DAG files in `dags/` directory. They auto-sync to Airflow.

### Basic DAG Template (Airflow 3.x)

```python
from datetime import datetime, timedelta
from airflow import DAG
from airflow.providers.standard.operators.bash import BashOperator
from airflow.models import Variable

default_args = {
    "owner": "lakehouse",
    "depends_on_past": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

SPARK_VERSION = Variable.get("spark_version", default_var="4.1")
SPARK_CONTAINER = "spark-master-41" if SPARK_VERSION == "4.1" else "spark-master"

with DAG(
    dag_id="my_custom_dag",
    default_args=default_args,
    schedule="@daily",  # Note: 'schedule' not 'schedule_interval' in Airflow 3.x
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=["lakehouse", "custom"],
) as dag:

    run_spark_job = BashOperator(
        task_id="run_spark_job",
        bash_command=f"""
            docker exec {SPARK_CONTAINER} /opt/spark/bin/spark-submit \
                /scripts/pipelines/pipeline_spark41.py
        """,
    )
```

### Using Kafka Sensors

```python
from airflow.providers.apache.kafka.sensors.kafka import AwaitMessageSensor

wait_for_data = AwaitMessageSensor(
    task_id="wait_for_data",
    topics=["orders"],
    kafka_config_id="kafka_default",
    timeout=300,
    soft_fail=True,  # Don't fail DAG if no messages
)
```

### Iceberg Maintenance Tasks

```python
from airflow.providers.standard.operators.bash import BashOperator

# Expire old snapshots
expire_snapshots = BashOperator(
    task_id="expire_snapshots",
    bash_command=f"""
        docker exec {SPARK_CONTAINER} /opt/spark/bin/spark-sql -e "
            CALL iceberg.system.expire_snapshots(
                table => 'iceberg.bronze.orders',
                older_than => TIMESTAMP '$(date -d '7 days ago' '+%Y-%m-%d %H:%M:%S')',
                retain_last => 5
            )
        "
    """,
)
```

## Monitoring

### Web UI

- **DAGs**: http://localhost:8085/dags
- **Task logs**: Click task instance → Logs
- **Connections**: Admin → Connections
- **Variables**: Admin → Variables

### Health Checks

```bash
# API server health (Airflow 3.x)
curl http://localhost:8085/api/v2/monitor/health

# Scheduler health
curl http://localhost:8974/health

# Via CLI
./lakehouse status --json | jq '.airflow'
```

### DAG Run Status

```bash
# List recent DAG runs
docker exec airflow-webserver airflow dags list-runs -d lakehouse_medallion_pipeline

# Get task states
docker exec airflow-webserver airflow tasks states-for-dag-run \
  lakehouse_medallion_pipeline <execution_date>
```

## Troubleshooting

### DAGs Not Appearing

```bash
# Check for import errors
docker exec airflow-webserver airflow dags list-import-errors

# Manually trigger DAG parsing
docker exec airflow-webserver airflow dags reserialize
```

### Task Failures

```bash
# View task logs
./lakehouse logs airflow-scheduler

# Get specific task log
docker exec airflow-webserver airflow tasks logs \
  lakehouse_medallion_pipeline run_pipeline_spark41 2024-01-01
```

### Connection Issues

```bash
# Test Kafka connection
docker exec airflow-webserver airflow connections test kafka_default

# Test PostgreSQL connection
docker exec airflow-webserver airflow connections test postgres_iceberg

# Re-run connection setup
docker exec airflow-webserver /opt/airflow/config/setup_connections.sh
```

### Database Issues

```bash
# Check Airflow database
docker exec airflow-webserver airflow db check

# Reset database (WARNING: deletes all history)
docker exec airflow-webserver airflow db reset
```

### Spark Job Failures

```bash
# Check Spark cluster is running
./lakehouse test

# View Spark logs
./lakehouse logs spark-master

# Test Spark submit manually
docker exec spark-master-41 /opt/spark/bin/spark-submit --version
```

### Airflow 3.x Migration Issues

**"Command airflow webserver has been removed"**
- Airflow 3.x renamed `webserver` to `api-server`
- Update docker-compose to use `command: api-server`

**"DAG.__init__() got an unexpected keyword argument 'schedule_interval'"**
- Airflow 3.x renamed `schedule_interval` to `schedule`
- Update DAGs: `schedule_interval="@daily"` → `schedule="@daily"`

**"Import error: airflow.operators.bash"**
- Operators moved to providers in Airflow 3.x
- Update: `from airflow.operators.bash import BashOperator`
- To: `from airflow.providers.standard.operators.bash import BashOperator`

**Health check returns error about /health endpoint**
- Airflow 3.x changed health endpoint
- Old: `/health`
- New: `/api/v2/monitor/health`

## Ports

| Service | Port |
|---------|------|
| Airflow Webserver | 8085 |
| Airflow Scheduler Health | 8974 |
| Spark 4.1 UI | 8082 |
| Spark 4.0 UI | 8080 |
| Kafka | 9092 |

## File Locations

| Path | Description |
|------|-------------|
| `dags/` | DAG definitions (auto-synced) |
| `logs/airflow/` | Airflow logs |
| `config/airflow/` | Configuration scripts |
| `docker/airflow/Dockerfile` | Custom Airflow image |
| `docker-compose-airflow.yml` | Docker Compose config |

## Airflow 3.x Notes

This setup uses Airflow 3.1.6 which has breaking changes from 2.x:

### Command Changes
| Old (2.x) | New (3.x) |
|-----------|-----------|
| `airflow webserver` | `airflow api-server` |

### DAG Parameter Changes
| Old (2.x) | New (3.x) |
|-----------|-----------|
| `schedule_interval="@daily"` | `schedule="@daily"` |
| `schedule_interval=None` | `schedule=None` |

### Import Changes
| Old (2.x) | New (3.x) |
|-----------|-----------|
| `from airflow.operators.bash import BashOperator` | `from airflow.providers.standard.operators.bash import BashOperator` |
| `from airflow.operators.python import PythonOperator` | `from airflow.providers.standard.operators.python import PythonOperator` |
| `from airflow.operators.python import BranchPythonOperator` | `from airflow.providers.standard.operators.python import BranchPythonOperator` |

### API Changes
| Old (2.x) | New (3.x) |
|-----------|-----------|
| `/health` | `/api/v2/monitor/health` |
| `AIRFLOW__WEBSERVER__WEB_SERVER_PORT` | `AIRFLOW__API__PORT` |

### Docker Image Notes
- Base image: `apache/airflow:3.1.6-python3.12`
- Java: **17** (for local Spark client operations)
- Spark client: 4.1.0 included for potential local spark-submit

**Note on Java versions:** The Airflow container uses Java 17 because:
1. Spark jobs run via `docker exec spark-master-41 spark-submit`, so they use the Spark container's JVM (Java 21 for Spark 4.1)
2. Java 17 is available in the Airflow base image (Java 21 is not)
3. Java 17 is sufficient for Airflow's needs (scheduling, API server)

## See Also

- [Streaming Guide](streaming.md) - Kafka and Spark Streaming
- [Pipelines Guide](pipelines.md) - Medallion architecture
- [CLI Reference](cli-reference.md) - All CLI commands
- [Apache Airflow Docs](https://airflow.apache.org/docs/)
