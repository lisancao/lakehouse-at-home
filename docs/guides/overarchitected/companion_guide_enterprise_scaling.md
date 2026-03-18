# Companion Guide: Enterprise Scaling — Airflow, Spark Connect, Kubernetes

**Audience:** Data engineers familiar with Databricks Jobs/Workflows who want to understand the equivalent OSS infrastructure for scheduling, thin-client access, and cluster scaling.

**Complements:** OverArchitected Show, Act 5 demo scripts (`scripts/demos/overarchitected/05a_airflow_sdp.py`, `scripts/demos/overarchitected/05b_spark_connect.py`).

**Last verified:** March 2026 against Spark 4.1.0, Airflow 3.1.6, Iceberg 1.10.0.

---

## Table of Contents

1. [Introduction: Laptop to Enterprise](#1-introduction-laptop-to-enterprise)
2. [Airflow Orchestration](#2-airflow-orchestration)
   - [Airflow 3.x Architecture](#21-airflow-3x-architecture)
   - [Spark Operator Landscape](#22-spark-operator-landscape)
   - [When to Use Which Operator](#23-when-to-use-which-operator)
   - [Wiring SDP into Airflow](#24-wiring-sdp-into-airflow)
   - [DAG Patterns for Lakehouse](#25-dag-patterns-for-lakehouse)
   - [Configuration: Connection IDs and Spark Conf Passthrough](#26-configuration-connection-ids-and-spark-conf-passthrough)
3. [Spark Connect Deep Dive](#3-spark-connect-deep-dive)
   - [Architecture: gRPC + Protocol Buffers + Apache Arrow](#31-architecture-grpc--protocol-buffers--apache-arrow)
   - [Server Setup](#32-server-setup)
   - [Client Setup](#33-client-setup)
   - [Connection Strings](#34-connection-strings)
   - [What Works](#35-what-works)
   - [What Does Not Work](#36-what-does-not-work)
   - [Security Model](#37-security-model)
   - [Performance Characteristics](#38-performance-characteristics)
   - [Spark 4.0 and 4.1 Improvements](#39-spark-40-and-41-improvements)
4. [Spark on Kubernetes](#4-spark-on-kubernetes)
   - [Native K8s Support](#41-native-k8s-support)
   - [spark-submit with K8s Master](#42-spark-submit-with-k8s-master)
   - [RBAC Setup](#43-rbac-setup)
   - [Docker Images](#44-docker-images)
   - [Configuration Differences from Standalone](#45-configuration-differences-from-standalone)
   - [JAR Delivery Strategies](#46-jar-delivery-strategies)
   - [Secret Management](#47-secret-management)
   - [Dynamic Allocation on K8s](#48-dynamic-allocation-on-k8s)
   - [SDP on K8s](#49-sdp-on-k8s)
   - [Spark Connect Server on K8s](#410-spark-connect-server-on-k8s)
   - [Minimum Viable K8s Demo (Minikube)](#411-minimum-viable-k8s-demo-minikube)
5. [The Progression Table](#5-the-progression-table)
6. [Production Considerations](#6-production-considerations)
   - [Monitoring](#61-monitoring)
   - [Logging](#62-logging)
   - [Cost Optimization](#63-cost-optimization)
7. [References](#7-references)

---

## 1. Introduction: Laptop to Enterprise

The defining promise of open source Spark is that the same code runs everywhere. You write a DataFrame pipeline on your laptop with `master("local[*]")`, and that same code --- byte for byte --- runs on a 200-node Kubernetes cluster. No vendor rewrites. No proprietary SDKs. The code is portable; only the infrastructure changes.

This is the journey:

```
┌─────────────────────────────────────────────────────────────────────────┐
│                                                                         │
│  LAPTOP          STANDALONE          SPARK CONNECT        KUBERNETES    │
│  ──────          ──────────          ─────────────        ──────────    │
│                                                                         │
│  local[*]   →    spark://host:7078  →  sc://host:15002  →  k8s://...   │
│                                                                         │
│  One process     Master + Workers     gRPC thin client     Pods on K8s  │
│  No scheduling   Docker Compose       1.5 MB client        Auto-scale   │
│  Dev/test        Team/staging         Multi-tenant          Production  │
│                                                                         │
│  ───────────────────────────────────────────────────────────────────── │
│  YOUR CODE DOES NOT CHANGE. THE PIPELINE DEFINITION DOES NOT CHANGE.   │
│  ONLY THE MASTER URL AND DEPLOYMENT CONFIG CHANGE.                      │
│                                                                         │
└─────────────────────────────────────────────────────────────────────────┘
```

If you are coming from Databricks, the mapping is straightforward:

| Databricks Concept | OSS Equivalent | Notes |
|---------------------|----------------|-------|
| Databricks Workspace | Airflow UI + Spark UI | Airflow for orchestration, Spark UI for monitoring |
| Databricks Jobs | Airflow DAGs | DAGs provide richer dependency modeling |
| Job Clusters | Kubernetes executor pods | Ephemeral, per-job compute |
| All-Purpose Clusters | Standalone Spark cluster | Long-running, shared compute |
| SQL Warehouses | Spark Connect server | Shared SQL endpoint, thin clients |
| Unity Catalog | Iceberg REST Catalog / JDBC | Table-level governance |
| Delta Lake | Apache Iceberg 1.10 | Open table format |
| Notebooks | JupyterLab + Spark Connect | `pip install pyspark-client`, connect remotely |
| Workflows Orchestration | Airflow 3.x | Richer scheduling, sensors, cross-system |
| DBFS | SeaweedFS (S3-compatible) | Self-hosted object storage |

The rest of this guide covers each stage of the progression in exhaustive detail.

---

## 2. Airflow Orchestration

### 2.1 Airflow 3.x Architecture

Apache Airflow 3.x ([release announcement](https://airflow.apache.org/blog/airflow-three-point-zero-is-here/)) is a complete rearchitecture from Airflow 2.x. The lakehouse stack uses Airflow 3.1.6 with Python 3.12.

**Breaking changes from 2.x that affect this stack:**

| Component | Airflow 2.x | Airflow 3.x |
|-----------|-------------|-------------|
| Web interface | `airflow webserver` | `airflow api-server` |
| Schedule param | `schedule_interval="@daily"` | `schedule="@daily"` |
| Health endpoint | `/health` | `/api/v2/monitor/health` |
| Port config | `AIRFLOW__WEBSERVER__WEB_SERVER_PORT` | `AIRFLOW__API__PORT` |
| Operator imports | `from airflow.operators.bash` | `from airflow.providers.standard.operators.bash` |
| DAG constructor | `from airflow import DAG` | `from airflow.sdk import DAG` |
| Task decorator | `from airflow.decorators import task` | `from airflow.sdk import task` |

References:
- Airflow 3.0 migration guide: <https://airflow.apache.org/docs/apache-airflow/stable/howto/upgrading-from-2-to-3.html>
- Airflow 3.0 release notes: <https://airflow.apache.org/docs/apache-airflow/stable/release_notes.html>

#### Three-Process Architecture

Airflow 3.x runs three core processes. In this stack, each runs as a separate Docker container (see `docker-compose-airflow.yml`):

```
┌───────────────────────────────────────────────────────────────────────────┐
│                        Airflow 3.x Architecture                           │
│                                                                           │
│  ┌─────────────────┐    ┌──────────────────┐    ┌─────────────────────┐   │
│  │   API Server    │    │    Scheduler     │    │     Triggerer       │   │
│  │   (port 8085)   │    │                  │    │                     │   │
│  │                 │    │  Parses DAGs     │    │  Async sensors      │   │
│  │  REST API       │    │  Queues tasks    │    │  Deferrable ops     │   │
│  │  Web UI         │    │  Monitors runs   │    │  Event-driven wait  │   │
│  │  Auth           │    │  Heartbeat check │    │                     │   │
│  └────────┬────────┘    └────────┬─────────┘    └────────┬────────────┘   │
│           │                      │                        │               │
│           └──────────────────────┼────────────────────────┘               │
│                                  │                                         │
│                         ┌────────▼─────────┐                              │
│                         │    PostgreSQL     │                              │
│                         │  (metadata DB)   │                              │
│                         │  Stores DAG runs │                              │
│                         │  Task instances  │                              │
│                         │  Variables       │                              │
│                         │  Connections     │                              │
│                         └──────────────────┘                              │
└───────────────────────────────────────────────────────────────────────────┘
```

**API Server** (container: `airflow-webserver`): Serves the web UI and REST API. Despite the legacy container name, the command is `api-server` in Airflow 3.x. This is the user-facing entry point. Health check: `curl http://localhost:8085/api/v2/monitor/health`.

**Scheduler** (container: `airflow-scheduler`): The brain. Parses DAG files from the `dags/` directory, evaluates schedule expressions, creates DagRun and TaskInstance records, and dispatches tasks to the executor. With `LocalExecutor` (our default), tasks run as subprocesses of the scheduler. Health check: `curl http://localhost:8974/health`.

**Triggerer** (container: `airflow-triggerer`): Handles deferrable operators and async sensors. Instead of a sensor occupying a worker slot while polling, the triggerer uses asyncio to efficiently wait for events. Critical for Kafka sensors that may wait minutes or hours for data.

References:
- Airflow architecture overview: <https://airflow.apache.org/docs/apache-airflow/stable/core-concepts/overview.html>
- Executor types: <https://airflow.apache.org/docs/apache-airflow/stable/core-concepts/executor/index.html>

#### Executor: LocalExecutor vs CeleryExecutor vs KubernetesExecutor

The executor determines how task instances are run:

| Executor | How Tasks Run | When to Use |
|----------|--------------|-------------|
| `LocalExecutor` | Subprocesses on scheduler node | Single-node, small-medium workloads. Our default. |
| `CeleryExecutor` | Distributed via Celery workers + Redis/RabbitMQ | Multi-node, high parallelism |
| `KubernetesExecutor` | Each task in its own K8s pod | K8s-native, per-task isolation |

Our `docker-compose-airflow.yml` uses `LocalExecutor` because the Airflow container is not running Spark directly --- it dispatches work to Spark containers via `docker exec` or `SparkSubmitOperator`. The executor only needs to manage lightweight orchestration tasks.

Reference: <https://airflow.apache.org/docs/apache-airflow/stable/core-concepts/executor/local.html>

### 2.2 Spark Operator Landscape

The `apache-airflow-providers-apache-spark` package (v5.5.1, March 2026) provides five operators for interacting with Spark from Airflow. Each wraps a different Spark client interface.

Reference: <https://airflow.apache.org/docs/apache-airflow-providers-apache-spark/stable/index.html>

#### SparkSubmitOperator

The workhorse. Wraps the `spark-submit` CLI, which is the universal entry point for submitting Spark applications regardless of cluster manager.

```python
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator

run_pipeline = SparkSubmitOperator(
    task_id="run_pipeline",
    application="/scripts/pipelines/pipeline_spark41.py",
    conn_id="spark_41",
    conf={
        "spark.sql.catalog.iceberg": "org.apache.iceberg.spark.SparkCatalog",
        "spark.sql.catalog.iceberg.type": "jdbc",
        "spark.sql.catalog.iceberg.uri": "jdbc:postgresql://localhost:5432/iceberg_catalog",
    },
    jars="/opt/spark/jars-extra/iceberg-spark-runtime-4.0_2.13-1.10.0.jar,"
         "/opt/spark/jars-extra/aws-bundle-2.24.6.jar,"
         "/opt/spark/jars-extra/postgresql-42.7.4.jar",
    name="lakehouse-pipeline",
    verbose=True,
)
```

**Key parameters:**

| Parameter | Type | Description |
|-----------|------|-------------|
| `application` | str | Path to .py, .jar, or .R file |
| `application_args` | list[str] | Arguments passed to the application |
| `conn_id` | str | Airflow connection ID for Spark master |
| `conf` | dict | `--conf key=value` pairs passed to spark-submit |
| `jars` | str | Comma-separated JAR paths (`--jars`) |
| `packages` | str | Maven coordinates (`--packages`) |
| `py_files` | str | Python files to distribute (`--py-files`) |
| `driver_memory` | str | Driver memory, e.g., `"4g"` |
| `executor_memory` | str | Executor memory, e.g., `"8g"` |
| `executor_cores` | int | Cores per executor |
| `num_executors` | int | Number of executors |
| `name` | str | Application name |
| `verbose` | bool | Print full spark-submit command |

**What it generates under the hood:**

```bash
spark-submit \
    --master spark://localhost:7078 \
    --name lakehouse-pipeline \
    --conf spark.sql.catalog.iceberg=org.apache.iceberg.spark.SparkCatalog \
    --conf spark.sql.catalog.iceberg.type=jdbc \
    --conf spark.sql.catalog.iceberg.uri=jdbc:postgresql://localhost:5432/iceberg_catalog \
    --jars /opt/spark/jars-extra/iceberg-spark-runtime-4.0_2.13-1.10.0.jar,... \
    /scripts/pipelines/pipeline_spark41.py
```

Reference: <https://airflow.apache.org/docs/apache-airflow-providers-apache-spark/stable/_api/airflow/providers/apache/spark/operators/spark_submit/index.html>

#### SparkSqlOperator

Wraps the `spark-sql` CLI. Executes Spark SQL statements directly without writing a Python script.

```python
from airflow.providers.apache.spark.operators.spark_sql import SparkSqlOperator

expire_snapshots = SparkSqlOperator(
    task_id="expire_snapshots",
    conn_id="spark_41",
    sql="""
        CALL iceberg.system.expire_snapshots(
            table => 'iceberg.bronze.orders',
            older_than => TIMESTAMP '2026-03-11 00:00:00',
            retain_last => 5
        )
    """,
)
```

**Key parameters:**

| Parameter | Type | Description |
|-----------|------|-------------|
| `sql` | str | SQL statement(s) to execute |
| `conn_id` | str | Spark connection ID |
| `conf` | str | Spark config as `key=value` pairs |
| `master` | str | Override master URL |

**Best for:** DDL operations, Iceberg maintenance procedures, simple SELECT queries, ad-hoc SQL.

Reference: <https://airflow.apache.org/docs/apache-airflow-providers-apache-spark/stable/_api/airflow/providers/apache/spark/operators/spark_sql/index.html>

#### PysparkOperator (New in v5.5.0, January 2026)

Runs PySpark code inline within the DAG file. No separate `.py` script needed. Uses the `@task.pyspark` decorator.

```python
from airflow.sdk import DAG, task

@task.pyspark(conn_id="spark_41")
def check_table_freshness(spark):
    """Verify data freshness. spark is injected automatically."""
    from datetime import datetime, timedelta
    from airflow.exceptions import AirflowException

    latest = spark.sql("""
        SELECT MAX(event_timestamp)
        FROM iceberg.bronze.orders
    """).collect()[0][0]

    if latest < datetime.now() - timedelta(hours=2):
        raise AirflowException(f"Data is stale! Latest: {latest}")

    return str(latest)
```

**Key characteristics:**

| Aspect | Detail |
|--------|--------|
| Package version | `apache-airflow-providers-apache-spark >= 5.5.0` |
| SparkSession | Injected as first argument to decorated function |
| Serialization | Function body is serialized and shipped to Spark |
| Return values | Must be serializable (XCom-compatible) |
| Dependencies | Access to Airflow context, XCom push/pull |

**Best for:** Quick validation checks, small transforms, prototyping, monitoring queries.

**Not for:** Heavy production pipelines (use SparkSubmitOperator), long-running streaming (use dedicated scripts).

Reference: <https://airflow.apache.org/docs/apache-airflow-providers-apache-spark/stable/operators.html>

#### SparkJDBCOperator

Moves data between Spark and JDBC sources. A thin wrapper around Spark's JDBC read/write.

```python
from airflow.providers.apache.spark.operators.spark_jdbc import SparkJDBCOperator

load_from_postgres = SparkJDBCOperator(
    task_id="load_from_postgres",
    conn_id="spark_41",
    jdbc_table="public.users",
    jdbc_conn_id="postgres_source",
    jdbc_driver="org.postgresql.Driver",
    cmd_type="spark_to_jdbc",  # or "jdbc_to_spark"
    save_mode="overwrite",
)
```

**Niche use case.** For most scenarios, use SparkSubmitOperator with a script that handles JDBC reads/writes alongside other transformations.

Reference: <https://airflow.apache.org/docs/apache-airflow-providers-apache-spark/stable/_api/airflow/providers/apache/spark/operators/spark_jdbc/index.html>

#### SparkKubernetesOperator

From the `apache-airflow-providers-cncf-kubernetes` package. Manages Spark applications as Kubernetes custom resources using the Spark Operator (GoogleCloudPlatform/spark-on-k8s-operator).

```python
from airflow.providers.cncf.kubernetes.operators.spark_kubernetes import (
    SparkKubernetesOperator,
)

run_on_k8s = SparkKubernetesOperator(
    task_id="run_pipeline_k8s",
    namespace="spark-jobs",
    application_file="k8s/spark-app.yaml",
    kubernetes_conn_id="k8s_default",
    do_xcom_push=True,
)
```

Where `k8s/spark-app.yaml` is a SparkApplication CRD:

```yaml
apiVersion: sparkoperator.k8s.io/v1beta2
kind: SparkApplication
metadata:
  name: lakehouse-pipeline
  namespace: spark-jobs
spec:
  type: Python
  mode: cluster
  image: apache/spark:4.1.0-scala2.13-java21-python3-r-ubuntu
  mainApplicationFile: local:///scripts/pipelines/pipeline_spark41.py
  sparkVersion: "4.1.0"
  driver:
    cores: 1
    memory: "2g"
    serviceAccount: spark
  executor:
    cores: 2
    instances: 3
    memory: "4g"
  sparkConf:
    "spark.sql.catalog.iceberg": "org.apache.iceberg.spark.SparkCatalog"
    "spark.sql.catalog.iceberg.type": "jdbc"
```

**Best for:** Production K8s deployments, per-job resource isolation, auto-scaling.

References:
- Spark K8s Operator: <https://github.com/GoogleCloudPlatform/spark-on-k8s-operator>
- Airflow K8s provider: <https://airflow.apache.org/docs/apache-airflow-providers-cncf-kubernetes/stable/index.html>

### 2.3 When to Use Which Operator

```
                    ┌─────────────────────────────┐
                    │  Do you need to run a full   │
                    │  Spark application (.py/.jar)?│
                    └──────────┬──────────────────┘
                         YES / \ NO
                        /       \
           ┌───────────▼───┐   ┌▼───────────────────────────┐
           │ Is it running │   │ Is it just SQL statements?  │
           │ on Kubernetes? │   └──────────┬─────────────────┘
           └──┬────────────┘          YES / \ NO
         YES / \ NO                  /       \
            /   \         ┌─────────▼──┐  ┌──▼───────────────┐
 ┌─────────▼──┐ ┌▼──────────────┐     │   │  Is it a small    │
 │ SparkK8s   │ │SparkSubmit    │     │   │  PySpark function? │
 │ Operator   │ │Operator       │     │   └──────┬────────────┘
 └────────────┘ └───────────────┘     │     YES / \ NO
                                      │        /   \
                              ┌───────▼──┐ ┌──▼──────────┐ ┌──────────┐
                              │SparkSql  │ │Pyspark      │ │ Probably │
                              │Operator  │ │Operator     │ │ BashOp   │
                              └──────────┘ └─────────────┘ └──────────┘
```

Decision matrix:

| Scenario | Operator | Reason |
|----------|----------|--------|
| Run SDP pipeline | `SparkSubmitOperator` | SDP wraps spark-submit; use `application="spark-pipelines"` with `application_args=["run", "--spec", "pipeline.yml"]` |
| Run medallion pipeline script | `SparkSubmitOperator` | Standard Spark application |
| Expire Iceberg snapshots | `SparkSqlOperator` | Single SQL CALL statement |
| Compact Iceberg files | `SparkSqlOperator` | Single SQL CALL statement |
| Check data freshness | `PysparkOperator` | Small, inline check |
| Count table rows for monitoring | `PysparkOperator` | Quick validation |
| Production pipeline on K8s | `SparkKubernetesOperator` | K8s-native pod management |
| Spark pipeline on standalone cluster | `SparkSubmitOperator` | Works with any cluster manager |
| Load JDBC source to table | `SparkSubmitOperator` | More flexible than `SparkJDBCOperator` |

**Databricks translation:**

| Databricks | Airflow Equivalent |
|------------|-------------------|
| Notebook task in a workflow | `SparkSubmitOperator` (script) or `PysparkOperator` (inline) |
| SQL task in a workflow | `SparkSqlOperator` |
| spark-submit task | `SparkSubmitOperator` (identical concept) |
| DLT pipeline task | `SparkSubmitOperator` running SDP |

### 2.4 Wiring SDP into Airflow

Spark Declarative Pipelines (SDP) does not have a dedicated Airflow operator as of March 2026. This is fine because SDP is built on top of `spark-submit`. The `SparkSubmitOperator` wraps it cleanly.

**The separation of concerns:**

```
┌────────────────────────────────────────────────────────────────────┐
│                                                                    │
│  AIRFLOW handles WHEN:              SDP handles WHAT and HOW:      │
│  ─────────────────────              ────────────────────────       │
│  - Schedule (daily, hourly)         - Table dependencies           │
│  - Retry on failure                 - Execution order              │
│  - Alerting                         - Write semantics              │
│  - Cross-DAG dependencies           - Incremental processing       │
│  - Sensor waits (Kafka data)        - Schema inference             │
│                                                                    │
│  They don't compete. They complement.                              │
│                                                                    │
└────────────────────────────────────────────────────────────────────┘
```

**Pattern 1: SparkSubmitOperator with spark-pipelines CLI**

```python
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator

run_sdp = SparkSubmitOperator(
    task_id="run_sdp_pipeline",
    application="/scripts/pipelines/pipeline_sdp.py",
    conn_id="spark_41",
    conf={
        "spark.sql.catalog.iceberg": "org.apache.iceberg.spark.SparkCatalog",
        "spark.sql.catalog.iceberg.type": "jdbc",
        "spark.sql.catalog.iceberg.uri": (
            "jdbc:postgresql://localhost:5432/iceberg_catalog"
        ),
        "spark.sql.catalog.iceberg.jdbc.user": "{{ var.value.pg_user }}",
        "spark.sql.catalog.iceberg.jdbc.password": "{{ var.value.pg_password }}",
        "spark.sql.catalog.iceberg.warehouse": "s3a://lakehouse/warehouse",
        "spark.hadoop.fs.s3a.endpoint": "http://localhost:8333",
        "spark.hadoop.fs.s3a.access.key": "{{ var.value.s3_access_key }}",
        "spark.hadoop.fs.s3a.secret.key": "{{ var.value.s3_secret_key }}",
        "spark.hadoop.fs.s3a.path.style.access": "true",
    },
    jars=(
        "/opt/spark/jars-extra/iceberg-spark-runtime-4.0_2.13-1.10.0.jar,"
        "/opt/spark/jars-extra/aws-bundle-2.24.6.jar,"
        "/opt/spark/jars-extra/postgresql-42.7.4.jar"
    ),
    name="lakehouse-sdp-pipeline",
    verbose=True,
)
```

**Pattern 2: BashOperator with docker exec (simpler for local stacks)**

This is what the lakehouse stack's existing DAGs use, since Airflow and Spark run on the same host via Docker Compose:

```python
from airflow.providers.standard.operators.bash import BashOperator

run_pipeline = BashOperator(
    task_id="run_pipeline_spark41",
    bash_command="""
        docker exec spark-master-41 /opt/spark/bin/spark-submit \
            /scripts/pipelines/pipeline_spark41.py
    """,
)
```

**When to use which pattern:**

| Pattern | When |
|---------|------|
| `SparkSubmitOperator` | Airflow can reach `spark-submit` binary (same host, or via SSH/K8s) |
| `BashOperator` + `docker exec` | Airflow container cannot reach Spark directly but can exec into Spark containers |
| `SparkKubernetesOperator` | Spark runs on Kubernetes |

**The actual DAG in this stack** (`dags/sdp_pipeline.py`):

```python
from airflow.sdk import DAG, task
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator

with DAG(
    dag_id="lakehouse_sdp_pipeline",
    schedule="@daily",
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=["lakehouse", "sdp", "spark-4.1", "medallion"],
) as dag:

    preflight = preflight_check()       # @task: check Spark, PG, SeaweedFS
    run_sdp = SparkSubmitOperator(...)   # Run SDP via spark-submit
    verify = verify_tables()             # @task: check output tables have data
    maintain = iceberg_maintenance()     # @task: expire snapshots

    preflight >> run_sdp >> verify >> maintain
```

### 2.5 DAG Patterns for Lakehouse

#### Pattern 1: Medallion Pipeline DAG

The most common pattern. Orchestrates bronze-to-silver-to-gold transformation.

```python
"""
DAG: lakehouse_medallion_pipeline
Schedule: @daily
Flow: check_prerequisites → choose_spark_version → run_pipeline → verify_output
"""

from airflow.sdk import DAG, task
from airflow.providers.standard.operators.bash import BashOperator
from airflow.providers.standard.operators.python import BranchPythonOperator

with DAG(
    dag_id="lakehouse_medallion_pipeline",
    schedule="@daily",
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=["lakehouse", "iceberg", "spark"],
) as dag:

    # Soft-fail Kafka check (data may already be loaded)
    check_kafka = BashOperator(
        task_id="check_kafka_availability",
        bash_command="""
            timeout 10 bash -c 'echo "" | nc -w 5 localhost 9092' \
                && echo "Kafka is available" \
                || echo "Kafka not available - continuing anyway"
        """,
    )

    # Branch based on Spark version variable
    choose_spark = BranchPythonOperator(
        task_id="choose_spark_version",
        python_callable=lambda **ctx: (
            "run_pipeline_spark41"
            if Variable.get("spark_version", "4.1") == "4.1"
            else "run_pipeline_spark40"
        ),
    )

    run_41 = BashOperator(
        task_id="run_pipeline_spark41",
        bash_command="docker exec spark-master-41 /opt/spark/bin/spark-submit "
                     "/scripts/pipelines/pipeline_spark41.py",
    )

    run_40 = BashOperator(
        task_id="run_pipeline_spark40",
        bash_command="docker exec spark-master /opt/spark/bin/spark-submit "
                     "/scripts/pipelines/pipeline_spark40.py",
    )

    verify = BashOperator(
        task_id="verify_tables",
        bash_command="""docker exec spark-master-41 /opt/spark/bin/spark-sql -e "
            SELECT 'bronze.orders', count(*) FROM iceberg.bronze.orders
            UNION ALL
            SELECT 'silver.orders_clean', count(*) FROM iceberg.silver.orders_clean
            UNION ALL
            SELECT 'gold.daily_summary', count(*) FROM iceberg.gold.daily_summary
        " """,
        trigger_rule="none_failed_min_one_success",
    )

    check_kafka >> choose_spark >> [run_41, run_40] >> verify
```

**Databricks equivalent:** A workflow with a "Run if" condition on the cluster type, followed by a notebook task and a SQL validation task.

#### Pattern 2: Iceberg Maintenance DAG

Iceberg tables accumulate snapshots, orphan files, and small files over time. This DAG handles housekeeping.

```python
"""
DAG: iceberg_maintenance
Schedule: 0 3 * * * (daily at 3 AM)
Flow: check_spark → (per table: expire → orphans → compact)
"""

TABLES = [
    "iceberg.bronze.orders",
    "iceberg.silver.orders_clean",
    "iceberg.gold.daily_summary",
]

with DAG(
    dag_id="iceberg_maintenance",
    schedule="0 3 * * *",
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=["lakehouse", "iceberg", "maintenance"],
) as dag:

    check_spark = BashOperator(
        task_id="check_spark_cluster",
        bash_command="""docker exec spark-master-41 \
            /opt/spark/bin/spark-submit --version > /dev/null 2>&1""",
    )

    for table in TABLES:
        safe_name = table.replace(".", "_")

        expire = BashOperator(
            task_id=f"expire_snapshots_{safe_name}",
            bash_command=f"""docker exec spark-master-41 /opt/spark/bin/spark-sql -e "
                CALL iceberg.system.expire_snapshots(
                    table => '{table}',
                    older_than => TIMESTAMP '$(date -d '7 days ago' '+%Y-%m-%d %H:%M:%S')',
                    retain_last => 5
                )" """,
        )

        orphans = BashOperator(
            task_id=f"remove_orphans_{safe_name}",
            bash_command=f"""docker exec spark-master-41 /opt/spark/bin/spark-sql -e "
                CALL iceberg.system.remove_orphan_files(
                    table => '{table}',
                    older_than => TIMESTAMP '$(date -d '3 days ago' '+%Y-%m-%d %H:%M:%S')'
                )" """,
        )

        compact = BashOperator(
            task_id=f"compact_files_{safe_name}",
            bash_command=f"""docker exec spark-master-41 /opt/spark/bin/spark-sql -e "
                CALL iceberg.system.rewrite_data_files(
                    table => '{table}',
                    options => map(
                        'target-file-size-bytes', '134217728',
                        'min-input-files', '5'
                    )
                )" """,
        )

        check_spark >> expire >> orphans >> compact
```

**Iceberg maintenance operations explained:**

| Operation | SQL | Purpose | Frequency |
|-----------|-----|---------|-----------|
| Expire snapshots | `CALL iceberg.system.expire_snapshots(...)` | Remove metadata for old snapshots | Daily |
| Remove orphan files | `CALL iceberg.system.remove_orphan_files(...)` | Delete data files not referenced by any snapshot | Daily |
| Rewrite data files | `CALL iceberg.system.rewrite_data_files(...)` | Compact small files into target size (128 MB default) | Daily or weekly |
| Rewrite manifests | `CALL iceberg.system.rewrite_manifests(...)` | Optimize manifest files for faster planning | Weekly |

References:
- Iceberg maintenance procedures: <https://iceberg.apache.org/docs/latest/spark-procedures/>
- Iceberg compaction: <https://iceberg.apache.org/docs/latest/maintenance/#compact-data-files>

#### Pattern 3: Monitoring / SLA DAG

```python
"""
DAG: lakehouse_monitoring
Schedule: */30 * * * * (every 30 minutes)
Flow: check_freshness → check_row_counts → alert_if_stale
"""

from airflow.sdk import DAG, task

@task.pyspark(conn_id="spark_41")
def check_freshness(spark):
    """Check that bronze data is not stale."""
    result = spark.sql("""
        SELECT
            MAX(TO_TIMESTAMP(REPLACE(ts, 'T', ' '))) as latest_event,
            COUNT(*) as total_events
        FROM iceberg.bronze.orders
    """).collect()[0]
    return {
        "latest_event": str(result.latest_event),
        "total_events": result.total_events,
    }

@task
def alert_if_stale(freshness_result):
    from datetime import datetime, timedelta
    latest = datetime.fromisoformat(freshness_result["latest_event"])
    if latest < datetime.now() - timedelta(hours=2):
        # In production: send Slack/PagerDuty alert
        raise RuntimeError(f"Data stale since {latest}")

with DAG(
    dag_id="lakehouse_monitoring",
    schedule="*/30 * * * *",
    catchup=False,
) as dag:
    freshness = check_freshness()
    alert_if_stale(freshness)
```

### 2.6 Configuration: Connection IDs and Spark Conf Passthrough

#### Airflow Connections

Connections store endpoint credentials. They are configured in the Airflow UI (Admin > Connections) or via the CLI.

**Pre-configured connections in this stack:**

| Connection ID | Type | Host | Port | Description |
|---------------|------|------|------|-------------|
| `spark_41` | Spark | `localhost` | `7078` | Spark 4.1 master |
| `spark_40` | Spark | `localhost` | `7077` | Spark 4.0 master |
| `kafka_default` | Kafka | `localhost` | `9092` | Kafka broker |
| `postgres_iceberg` | PostgreSQL | `localhost` | `5432` | Iceberg catalog metadata |

**How SparkSubmitOperator uses `conn_id`:**

The operator reads the connection's `host` and `port` to construct the `--master` URL:

```
conn_id="spark_41" → reads connection → host=localhost, port=7078
                    → generates: --master spark://localhost:7078
```

**Setting up connections via CLI:**

```bash
# Spark 4.1
docker exec airflow-webserver airflow connections add spark_41 \
    --conn-type spark \
    --conn-host localhost \
    --conn-port 7078

# Spark 4.0
docker exec airflow-webserver airflow connections add spark_40 \
    --conn-type spark \
    --conn-host localhost \
    --conn-port 7077

# PostgreSQL (Iceberg catalog)
docker exec airflow-webserver airflow connections add postgres_iceberg \
    --conn-type postgres \
    --conn-host localhost \
    --conn-port 5432 \
    --conn-login iceberg \
    --conn-password iceberg_password \
    --conn-schema iceberg_catalog
```

Reference: <https://airflow.apache.org/docs/apache-airflow/stable/howto/connection.html>

#### Spark Conf Passthrough

The `conf` parameter on `SparkSubmitOperator` maps directly to `--conf` flags on `spark-submit`. Every key-value pair becomes a `--conf key=value` argument.

**Full Iceberg + S3 configuration for this stack:**

```python
SPARK_CONF = {
    # Iceberg catalog
    "spark.sql.catalog.iceberg": "org.apache.iceberg.spark.SparkCatalog",
    "spark.sql.catalog.iceberg.type": "jdbc",
    "spark.sql.catalog.iceberg.uri": "jdbc:postgresql://localhost:5432/iceberg_catalog",
    "spark.sql.catalog.iceberg.jdbc.user": "iceberg",
    "spark.sql.catalog.iceberg.jdbc.password": "iceberg_password",
    "spark.sql.catalog.iceberg.warehouse": "s3a://lakehouse/warehouse",

    # S3 / SeaweedFS
    "spark.hadoop.fs.s3a.endpoint": "http://localhost:8333",
    "spark.hadoop.fs.s3a.access.key": "admin",
    "spark.hadoop.fs.s3a.secret.key": "admin_password",
    "spark.hadoop.fs.s3a.path.style.access": "true",
    "spark.hadoop.fs.s3a.impl": "org.apache.hadoop.fs.s3a.S3AFileSystem",

    # Performance
    "spark.sql.adaptive.enabled": "true",
    "spark.sql.adaptive.coalescePartitions.enabled": "true",
}
```

**Using Airflow Variables for secrets (recommended):**

```python
import os

SPARK_CONF = {
    "spark.sql.catalog.iceberg.jdbc.user": os.getenv("POSTGRES_USER", "iceberg"),
    "spark.sql.catalog.iceberg.jdbc.password": os.getenv("POSTGRES_PASSWORD", "iceberg_password"),
    "spark.hadoop.fs.s3a.access.key": os.getenv("S3_ACCESS_KEY", "admin"),
    "spark.hadoop.fs.s3a.secret.key": os.getenv("S3_SECRET_KEY", "admin_password"),
}
```

In the lakehouse stack, secrets come from the `.env` file which is mounted into the Airflow container. The `env_file: .env` directive in `docker-compose-airflow.yml` makes all `.env` variables available as environment variables.

**Airflow Variables** (alternative to env vars):

```bash
# Set variables
docker exec airflow-webserver airflow variables set spark_version "4.1"
docker exec airflow-webserver airflow variables set pg_user "iceberg"

# Use in DAGs
from airflow.models import Variable
version = Variable.get("spark_version", default_var="4.1")
```

Reference: <https://airflow.apache.org/docs/apache-airflow/stable/howto/variable.html>

---

## 3. Spark Connect Deep Dive

Spark Connect is a client-server architecture introduced in Spark 3.4 and significantly improved in Spark 4.0. It decouples the client (your code) from the server (the Spark driver), communicating over gRPC instead of Py4J.

Reference: <https://spark.apache.org/docs/latest/spark-connect-overview.html>

### 3.1 Architecture: gRPC + Protocol Buffers + Apache Arrow

**Traditional (Classic) Spark:**

```
┌──────────────────────────────────────────────────────────────────┐
│                    SINGLE PROCESS                                 │
│                                                                   │
│  ┌─────────────┐    Py4J     ┌──────────────┐                   │
│  │  Python      │◄──────────►│  JVM Driver   │                   │
│  │  (PySpark)   │  (IPC/     │  (SparkSession│                   │
│  │              │   socket)   │   + Catalyst) │                   │
│  └─────────────┘             └──────┬───────┘                   │
│                                      │                           │
│                              ┌───────▼────────┐                  │
│                              │   Executors     │                  │
│                              │   (Workers)     │                  │
│                              └────────────────┘                  │
└──────────────────────────────────────────────────────────────────┘

Problems:
  - Python + JVM in one process → memory pressure
  - Py4J bridge → serialization overhead
  - Client crash kills driver → kills all running queries
  - One client per driver → no multi-tenancy
  - Full PySpark install: ~300 MB + JVM
```

**Spark Connect (Client-Server):**

```
 CLIENT SIDE (your laptop)              SERVER SIDE (Spark cluster)
┌──────────────────────┐               ┌──────────────────────────────────┐
│                      │               │                                  │
│  ┌────────────────┐  │    gRPC       │  ┌────────────────────────────┐  │
│  │  Python         │  │───────────── │  │  SparkConnectServer        │  │
│  │  (pyspark-      │  │  (port 15002)│  │  (embedded in JVM driver)  │  │
│  │   client)       │  │              │  │                            │  │
│  │                 │  │  Protobuf    │  │  Receives: query plans     │  │
│  │  1.5 MB         │◄─│─────────────►│  │  (serialized as Protobuf)  │  │
│  │  No JVM!        │  │              │  │                            │  │
│  │                 │  │  Arrow       │  │  Returns: result data      │  │
│  │  Pure Python    │◄─│──────────────│  │  (serialized as Arrow)     │  │
│  └────────────────┘  │              │  │                            │  │
│                      │               │  └─────────────┬──────────────┘  │
└──────────────────────┘               │                │                  │
                                       │        ┌───────▼────────┐        │
                                       │        │   Executors     │        │
                                       │        │   (Workers)     │        │
                                       │        └────────────────┘        │
                                       └──────────────────────────────────┘
```

**Protocol details:**

| Layer | Protocol | Direction | What It Carries |
|-------|----------|-----------|-----------------|
| Request | gRPC + Protocol Buffers | Client → Server | Unresolved logical query plan |
| Response (metadata) | gRPC + Protocol Buffers | Server → Client | Schema, metrics, errors |
| Response (data) | Apache Arrow IPC | Server → Client | Result rows in columnar format |

**Why Protocol Buffers for the plan?**

Spark Connect does not send Python code to the server. It sends an *unresolved logical plan* as a Protocol Buffer message. The server's Catalyst optimizer resolves, optimizes, and executes it. This means:

1. The client never needs a JVM --- it only builds Protobuf messages.
2. The plan is language-agnostic --- Scala, Python, Go, Rust clients all produce the same Protobuf.
3. The server can reject malformed plans before execution.

**Why Apache Arrow for results?**

Arrow provides zero-copy columnar data exchange. A `.collect()` call returns Arrow record batches that pandas/polars can consume without deserialization. This is dramatically faster than Py4J's row-by-row pickling.

References:
- Spark Connect protocol: <https://spark.apache.org/docs/latest/spark-connect-overview.html#how-spark-connect-works>
- Protocol Buffers definition: <https://github.com/apache/spark/tree/master/connector/connect/common/src/main/protobuf/spark/connect>
- Apache Arrow IPC: <https://arrow.apache.org/docs/format/IPC.html>

### 3.2 Server Setup

The Spark Connect server is a built-in component of Spark 3.4+. It runs as an embedded gRPC endpoint inside a Spark driver process.

**Start the server (standalone cluster):**

```bash
# On the Spark master node (or any node that can reach workers)
/opt/spark/sbin/start-connect-server.sh \
    --master spark://spark-master-41:7078 \
    --jars /opt/spark/jars-extra/iceberg-spark-runtime-4.0_2.13-1.10.0.jar,\
           /opt/spark/jars-extra/aws-bundle-2.24.6.jar,\
           /opt/spark/jars-extra/postgresql-42.7.4.jar \
    --conf spark.sql.catalog.iceberg=org.apache.iceberg.spark.SparkCatalog \
    --conf spark.sql.catalog.iceberg.type=jdbc \
    --conf spark.sql.catalog.iceberg.uri=jdbc:postgresql://localhost:5432/iceberg_catalog \
    --conf spark.sql.catalog.iceberg.jdbc.user=iceberg \
    --conf spark.sql.catalog.iceberg.jdbc.password=iceberg_password \
    --conf spark.sql.catalog.iceberg.warehouse=s3a://lakehouse/warehouse \
    --conf spark.hadoop.fs.s3a.endpoint=http://localhost:8333 \
    --conf spark.hadoop.fs.s3a.access.key=admin \
    --conf spark.hadoop.fs.s3a.secret.key=admin_password \
    --conf spark.hadoop.fs.s3a.path.style.access=true
```

**In this stack (Docker):**

```bash
docker exec spark-master-41 /opt/spark/sbin/start-connect-server.sh \
    --master spark://spark-master-41:7078 \
    --jars /opt/spark/jars-extra/iceberg-spark-runtime-4.0_2.13-1.10.0.jar,\
           /opt/spark/jars-extra/aws-bundle-2.24.6.jar,\
           /opt/spark/jars-extra/postgresql-42.7.4.jar \
    --conf spark.sql.catalog.iceberg=org.apache.iceberg.spark.SparkCatalog \
    --conf spark.sql.catalog.iceberg.type=jdbc \
    --conf spark.sql.catalog.iceberg.uri=jdbc:postgresql://localhost:5432/iceberg_catalog
```

**Stop the server:**

```bash
docker exec spark-master-41 /opt/spark/sbin/stop-connect-server.sh
```

**What `start-connect-server.sh` does under the hood:**

```bash
# It is equivalent to:
spark-submit \
    --class org.apache.spark.sql.connect.service.SparkConnectServer \
    --master spark://spark-master-41:7078 \
    --conf spark.connect.grpc.binding.port=15002 \
    [your other --conf and --jars flags]
```

**Server configuration options:**

| Config Key | Default | Description |
|------------|---------|-------------|
| `spark.connect.grpc.binding.port` | `15002` | gRPC listen port |
| `spark.connect.grpc.maxInboundMessageSize` | `128MB` | Max message size |
| `spark.connect.execute.reattachable.enabled` | `true` | Allow clients to reattach after disconnect |
| `spark.connect.execute.manager.detachedTimeout` | `5m` | How long to keep detached executions |
| `spark.connect.extensions.relation.classes` | (none) | Custom relation plugins |
| `spark.connect.extensions.command.classes` | (none) | Custom command plugins |

References:
- Start Connect server: <https://spark.apache.org/docs/latest/spark-connect-overview.html#start-spark-connect-server>
- Server configuration: <https://spark.apache.org/docs/latest/configuration.html#spark-connect>

### 3.3 Client Setup

**Option 1: pyspark-client (thin client, 1.5 MB)**

```bash
pip install pyspark-client
```

This installs only the gRPC client stubs and Arrow integration. No JVM. No Spark jars. The entire install is approximately 1.5 MB.

```python
from pyspark.sql import SparkSession

spark = SparkSession.builder \
    .remote("sc://localhost:15002") \
    .getOrCreate()

# Everything works as expected
spark.sql("SELECT COUNT(*) FROM iceberg.bronze.orders").show()
df = spark.table("iceberg.bronze.orders")
df.groupBy("event_type").count().show()
```

**Option 2: Full pyspark with connect mode**

```bash
pip install pyspark
```

The full PySpark package also supports Connect mode. Use `.remote()` instead of `.master()`:

```python
# Connect mode (thin client behavior even with full install)
spark = SparkSession.builder.remote("sc://localhost:15002").getOrCreate()

# Classic mode (traditional, full driver)
spark = SparkSession.builder.master("local[*]").getOrCreate()
```

**Option 3: spark.api.mode configuration (Spark 4.0+)**

Spark 4.0 introduced `spark.api.mode` to control API surface:

```python
# Force Connect behavior even in classic mode (for testing compatibility)
spark = SparkSession.builder \
    .master("local[*]") \
    .config("spark.api.mode", "connect") \
    .getOrCreate()
```

| Value | Behavior |
|-------|----------|
| `classic` | Full API (RDD, SparkContext, JVM access) |
| `connect` | Connect-only API (DataFrame, SQL, no RDD) |

This is useful for testing: write code with `spark.api.mode=connect` locally, then deploy to a real Connect server in production, knowing the API surface is compatible.

References:
- pyspark-client PyPI: <https://pypi.org/project/pyspark-client/>
- Spark Connect client setup: <https://spark.apache.org/docs/latest/spark-connect-overview.html#use-spark-connect-in-standalone-applications>

### 3.4 Connection Strings

Spark Connect uses a custom URI scheme: `sc://`.

**Format:**

```
sc://host:port/;option1=value1;option2=value2
```

**Examples:**

```python
# Basic
spark = SparkSession.builder.remote("sc://localhost:15002").getOrCreate()

# With session-level config
spark = SparkSession.builder.remote(
    "sc://localhost:15002/;user_id=analyst1;token=abc123"
).getOrCreate()

# With custom gRPC channel options
spark = SparkSession.builder.remote(
    "sc://spark-connect.prod.internal:15002"
).getOrCreate()

# Kubernetes LoadBalancer
spark = SparkSession.builder.remote(
    "sc://spark-connect.k8s.example.com:15002"
).getOrCreate()
```

**Connection string parameters:**

| Parameter | Description |
|-----------|-------------|
| `user_id` | User identifier (passed as gRPC metadata) |
| `token` | Authentication token |
| `session_id` | Resume a previous session |
| `use_ssl` | Enable TLS (`true`/`false`) |
| `grpc_max_message_size` | Override max message size |

**Programmatic connection (alternative to URI):**

```python
from pyspark.sql import SparkSession
from pyspark.sql.connect.session import SparkSession as ConnectSession

# Using ChannelBuilder for advanced gRPC config
spark = SparkSession.builder \
    .remote("sc://localhost:15002") \
    .config("spark.connect.grpc.interceptor", "com.example.AuthInterceptor") \
    .getOrCreate()
```

Reference: <https://spark.apache.org/docs/latest/spark-connect-overview.html#set-up-client-applications>

### 3.5 What Works

The following APIs work identically over Spark Connect as they do in classic mode:

**DataFrame operations (full support):**

```python
from pyspark.sql import functions as f

df = spark.table("iceberg.bronze.orders")

# Filter, select, transform
df.filter(f.col("event_type") == "order_created") \
  .select("order_id", "ts", "body") \
  .withColumn("parsed", f.from_json(f.col("body"), schema)) \
  .show()

# Aggregations
df.groupBy("event_type").count().orderBy(f.desc("count")).show()

# Joins
orders = spark.table("iceberg.bronze.orders")
brands = spark.table("iceberg.bronze.dim_brands")
orders.join(brands, orders.brand_id == brands.id, "left").show()

# Window functions
from pyspark.sql.window import Window
w = Window.partitionBy("order_id").orderBy("ts")
df.withColumn("row_num", f.row_number().over(w)).show()
```

**SQL queries (full support):**

```python
spark.sql("SHOW NAMESPACES IN iceberg").show()
spark.sql("SHOW TABLES IN iceberg.bronze").show()
spark.sql("SELECT * FROM iceberg.bronze.orders LIMIT 10").show()
spark.sql("CREATE TABLE iceberg.silver.test AS SELECT 1 AS id")
spark.sql("CALL iceberg.system.expire_snapshots(table => 'iceberg.bronze.orders')")
```

**Python UDFs (full support in Spark 4.0+):**

```python
from pyspark.sql.functions import udf
from pyspark.sql.types import StringType

@udf(returnType=StringType())
def clean_name(name):
    return name.strip().title() if name else None

df.withColumn("clean_name", clean_name(f.col("brand_name"))).show()
```

**Structured Streaming (full support):**

```python
# Read from Kafka via Connect
stream_df = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("subscribe", "orders") \
    .load()

# Write to Iceberg
query = stream_df.writeStream \
    .format("iceberg") \
    .outputMode("append") \
    .option("checkpointLocation", "/tmp/checkpoint") \
    .toTable("iceberg.bronze.orders_stream")
```

**Catalog operations (full support):**

```python
spark.catalog.listDatabases()
spark.catalog.listTables("iceberg.bronze")
spark.catalog.tableExists("iceberg.bronze.orders")
spark.catalog.listColumns("iceberg.bronze.orders")
```

**ML (pyspark.ml) --- new in Spark 4.0:**

```python
from pyspark.ml.feature import VectorAssembler
from pyspark.ml.classification import LogisticRegression

assembler = VectorAssembler(inputCols=["feature1", "feature2"], outputCol="features")
lr = LogisticRegression(featuresCol="features", labelCol="label")

# Works over Connect in Spark 4.0+
model = lr.fit(assembler.transform(training_data))
```

**Pandas API on Spark (full support):**

```python
import pyspark.pandas as ps
pdf = ps.read_table("iceberg.bronze.orders")
pdf.head()
pdf.describe()
```

### 3.6 What Does Not Work

**RDD API --- not available:**

```python
# WILL FAIL over Spark Connect
spark.sparkContext  # AttributeError
rdd = spark.sparkContext.parallelize([1, 2, 3])  # No
df.rdd.map(lambda x: x[0])  # No
```

**Why:** RDDs are a low-level, JVM-centric abstraction. Spark Connect operates at the DataFrame/SQL level. The Protobuf protocol has no representation for RDD operations.

**If you need RDD-like behavior:** Use DataFrame operations. Every RDD operation has a DataFrame equivalent:

| RDD Operation | DataFrame Equivalent |
|---------------|---------------------|
| `rdd.map(f)` | `df.select(udf(f)(col))` or `df.withColumn(...)` |
| `rdd.filter(f)` | `df.filter(condition)` |
| `rdd.reduce(f)` | `df.agg(...)` |
| `rdd.flatMap(f)` | `df.select(explode(...))` |
| `rdd.groupByKey()` | `df.groupBy(...)` |

**SparkContext access --- not available:**

```python
# WILL FAIL over Spark Connect
spark.sparkContext.setLogLevel("WARN")  # No
spark.sparkContext.addFile("data.csv")  # No
spark.sparkContext.broadcast(data)      # No (use df operations instead)
```

**JVM access --- not available:**

```python
# WILL FAIL over Spark Connect
df._jdf  # No (internal JVM DataFrame handle)
spark._jvm  # No (direct JVM access)
spark._jsc  # No (Java SparkContext)
```

**Some edge-case APIs:**

| API | Status | Workaround |
|-----|--------|------------|
| `spark.sparkContext` | Not available | Use DataFrame/SQL operations |
| `df.rdd` | Not available | Use DataFrame operations |
| `df.foreach()` | Not available | Use `df.collect()` + Python loop |
| `df.foreachPartition()` | Not available | Rewrite as DataFrame operation |
| `spark.sparkContext.setLogLevel()` | Not available | Set server-side at startup |
| `df.toLocalIterator()` | Available (Spark 4.0+) | |
| Custom Catalyst rules | Not available | Server-side plugins only |
| Accumulators | Not available | Use DataFrame aggregations |

**The practical impact is small.** For data engineering work (which is 95%+ of Spark usage), the Connect API surface covers everything you need. If you are using RDDs in 2026, you are likely maintaining legacy code that should be migrated to DataFrames anyway.

### 3.7 Security Model

Spark Connect's security model is deliberately minimal. The gRPC server itself has **no built-in authentication or authorization**. The design philosophy is "bring your own security at the network layer."

**Why?** Different organizations have different auth stacks (OAuth, Kerberos, mTLS, custom tokens). Baking one in would serve nobody well. Instead, Spark Connect exposes gRPC interceptors for custom auth.

**Security options:**

| Method | How | When |
|--------|-----|------|
| Network isolation | VPC, firewall rules, K8s NetworkPolicy | Always (baseline) |
| TLS | `--conf spark.connect.grpc.binding.secure=true` + certs | Always in production |
| gRPC interceptor | Custom Java class implementing `ServerInterceptor` | Token/OAuth auth |
| Reverse proxy | Envoy, nginx, or Istio in front of port 15002 | Multi-tenant, rate limiting |
| K8s Ingress | Ingress controller with auth middleware | K8s deployments |

**TLS setup example:**

```bash
start-connect-server.sh \
    --conf spark.connect.grpc.binding.secure=true \
    --conf spark.connect.grpc.binding.secure.keyStorePath=/certs/keystore.jks \
    --conf spark.connect.grpc.binding.secure.keyStorePassword=changeit
```

**Custom gRPC interceptor (Java):**

```java
public class TokenAuthInterceptor implements ServerInterceptor {
    @Override
    public <ReqT, RespT> ServerCall.Listener<ReqT> interceptCall(
            ServerCall<ReqT, RespT> call,
            Metadata headers,
            ServerCallHandler<ReqT, RespT> next) {
        String token = headers.get(Metadata.Key.of("authorization", ASCII_STRING_MARSHALLER));
        if (!isValidToken(token)) {
            call.close(Status.UNAUTHENTICATED.withDescription("Invalid token"), new Metadata());
            return new ServerCall.Listener<>() {};
        }
        return next.startCall(call, headers);
    }
}
```

Register with:

```bash
start-connect-server.sh \
    --conf spark.connect.extensions.server.interceptor.classes=com.example.TokenAuthInterceptor
```

**Envoy proxy pattern (recommended for production):**

```
┌─────────┐    TLS + Auth    ┌──────────┐    Plain gRPC    ┌──────────────┐
│ Client   │────────────────►│  Envoy   │─────────────────►│ Spark Connect│
│ (remote) │                 │  Proxy   │                  │ Server       │
└─────────┘                  └──────────┘                  └──────────────┘
                              - mTLS termination
                              - OAuth2 token validation
                              - Rate limiting
                              - Request logging
```

**Databricks equivalent:** Databricks SQL Warehouses and clusters have built-in auth tied to the workspace. With OSS Spark Connect, you build that layer yourself or use a service mesh.

References:
- Spark Connect security: <https://spark.apache.org/docs/latest/spark-connect-overview.html#security>
- gRPC interceptors: <https://grpc.io/docs/guides/interceptors/>

### 3.8 Performance Characteristics

**Latency:**

| Operation | Classic Mode | Spark Connect | Delta |
|-----------|-------------|---------------|-------|
| Session creation | ~2s (JVM startup) | ~100ms (gRPC handshake) | Much faster |
| Simple query (COUNT) | ~500ms | ~600ms | ~20% overhead |
| Complex query (multi-join) | ~10s | ~10.1s | Negligible |
| Large collect (1M rows) | ~3s (Py4J) | ~1.5s (Arrow) | 2x faster |
| UDF execution | ~X (Py4J) | ~X (server-side) | Same |

**Key performance characteristics:**

1. **Small queries have ~100-200ms gRPC overhead.** For interactive analytics, this is imperceptible. For micro-batch processing (thousands of tiny queries), it adds up.

2. **Large result sets are faster with Connect.** Arrow columnar transfer beats Py4J's row-by-row serialization. The crossover point is around 10,000 rows.

3. **Query planning is identical.** The server runs the same Catalyst optimizer, same Spark engine. Only the client-server transport differs.

4. **Streaming performance is identical.** The streaming query runs server-side. The client only starts it and monitors progress.

5. **Multi-tenant overhead.** Multiple clients sharing one Connect server share one SparkSession. Isolation is at the SQL level (separate schemas/catalogs), not at the compute level. For compute isolation, use separate Connect servers.

**Benchmark data from the Spark community** (Spark 3.5, 10-node cluster, TPC-DS 1TB):

| Query Set | Classic (s) | Connect (s) | Ratio |
|-----------|------------|-------------|-------|
| TPC-DS q1-q10 | 142 | 145 | 1.02x |
| TPC-DS q11-q20 | 198 | 201 | 1.01x |
| Full TPC-DS | 1847 | 1862 | 1.008x |

The overhead is less than 1% for batch workloads.

Reference: <https://spark.apache.org/docs/latest/spark-connect-overview.html#performance>

### 3.9 Spark 4.0 and 4.1 Improvements

Spark 4.0 and 4.1 brought significant improvements to Spark Connect:

**Spark 4.0 (2024):**

| Feature | Description |
|---------|-------------|
| `pyspark-client` package | Dedicated thin-client PyPI package (1.5 MB) |
| `spark.api.mode` | Configuration to switch between classic/connect API surface |
| ML support | `pyspark.ml` works over Connect (pipelines, models, evaluation) |
| Improved streaming | Full Structured Streaming support including Kafka, Iceberg sinks |
| Reattachable executions | Client can disconnect and reconnect without losing state |
| `toLocalIterator()` | Stream large results without collecting all into memory |

**Spark 4.1 (2025):**

| Feature | Description |
|---------|-------------|
| VARIANT type support | `parse_json()`, `variant_get()` work over Connect |
| Improved error messages | Server-side errors include full stack traces |
| Session variables | `SET VARIABLE` / `DECLARE VARIABLE` work over Connect |
| Better UDF support | Improved Python UDF serialization and performance |
| Collation support | `COLLATE` and collation-aware operations work over Connect |

**The `pyspark-client` package (Spark 4.0+):**

Before Spark 4.0, using Spark Connect required installing the full `pyspark` package (300+ MB) and then using `.remote()`. Spark 4.0 introduced `pyspark-client` as a standalone package:

```bash
# Before Spark 4.0 (300+ MB)
pip install pyspark
# Then: spark = SparkSession.builder.remote("sc://...").getOrCreate()

# Spark 4.0+ (1.5 MB)
pip install pyspark-client
# Same API: spark = SparkSession.builder.remote("sc://...").getOrCreate()
```

The `pyspark-client` package contains only:
- gRPC client stubs (generated from Spark's Protobuf definitions)
- Apache Arrow integration for data transfer
- DataFrame, SQL, Column, and functions APIs
- No JVM, no Spark core, no Hadoop dependencies

References:
- Spark 4.0 release notes: <https://spark.apache.org/releases/spark-release-4-0-0.html>
- pyspark-client package: <https://pypi.org/project/pyspark-client/>

---

## 4. Spark on Kubernetes

### 4.1 Native K8s Support

Spark has native Kubernetes support since Spark 2.3 (experimental) and GA since Spark 3.1. It uses the Kubernetes API directly as a cluster manager --- no Hadoop YARN, no standalone master, no Mesos.

**How it works:**

```
┌──────────────────────────────────────────────────────────────────────┐
│                        Kubernetes Cluster                            │
│                                                                      │
│  ┌──────────────┐     creates      ┌────────────────────────────┐   │
│  │  spark-submit │────────────────►│  Driver Pod                │   │
│  │  (client or   │                 │  (runs SparkSession)       │   │
│  │   cluster mode)│                │                            │   │
│  └──────────────┘                  │  requests executor pods    │   │
│                                    └─────────────┬──────────────┘   │
│                                                   │                  │
│                              ┌────────────────────┼───────────────┐  │
│                              │                    │               │  │
│                        ┌─────▼──────┐  ┌─────────▼──┐  ┌────────▼┐ │
│                        │ Executor   │  │ Executor   │  │Executor │ │
│                        │ Pod 1      │  │ Pod 2      │  │Pod 3    │ │
│                        └────────────┘  └────────────┘  └─────────┘ │
│                                                                      │
│  Each pod:                                                           │
│    - Runs apache/spark Docker image                                 │
│    - Has CPU/memory limits from SparkConf                            │
│    - Auto-deleted when job completes                                 │
│    - Can use K8s volumes for local storage                           │
└──────────────────────────────────────────────────────────────────────┘
```

**Comparison with Standalone mode:**

| Aspect | Standalone | Kubernetes |
|--------|-----------|------------|
| Cluster manager | Spark Master process | K8s API server |
| Worker lifecycle | Long-running, manual | Ephemeral pods, auto-managed |
| Scaling | Fixed workers | Dynamic pod creation |
| Resource isolation | Coarse (per-worker) | Fine (per-pod, cgroups) |
| Multi-tenancy | Shared workers | Separate pods per application |
| Master URL | `spark://host:7078` | `k8s://https://api-server:6443` |
| Failure recovery | Worker restart | Pod rescheduling |

References:
- Spark on Kubernetes: <https://spark.apache.org/docs/latest/running-on-kubernetes.html>
- Kubernetes API: <https://kubernetes.io/docs/reference/kubernetes-api/>

### 4.2 spark-submit with K8s Master

The `--master` URL changes from `spark://` to `k8s://`:

```bash
spark-submit \
    --master k8s://https://kubernetes.default.svc:6443 \
    --deploy-mode cluster \
    --name lakehouse-pipeline \
    --conf spark.kubernetes.container.image=apache/spark:4.1.0-scala2.13-java21-python3-r-ubuntu \
    --conf spark.kubernetes.namespace=spark-jobs \
    --conf spark.kubernetes.authenticate.driver.serviceAccountName=spark \
    --conf spark.executor.instances=3 \
    --conf spark.executor.memory=4g \
    --conf spark.executor.cores=2 \
    --conf spark.driver.memory=2g \
    --conf spark.sql.catalog.iceberg=org.apache.iceberg.spark.SparkCatalog \
    --conf spark.sql.catalog.iceberg.type=jdbc \
    --conf spark.sql.catalog.iceberg.uri=jdbc:postgresql://postgres.data.svc:5432/iceberg_catalog \
    --jars local:///opt/spark/jars-extra/iceberg-spark-runtime-4.0_2.13-1.10.0.jar \
    local:///scripts/pipelines/pipeline_spark41.py
```

**Key differences from standalone spark-submit:**

| Parameter | Standalone | Kubernetes |
|-----------|-----------|------------|
| `--master` | `spark://host:7078` | `k8s://https://api-server:6443` |
| `--deploy-mode` | `client` (default) | `cluster` (recommended for K8s) |
| Container image | Not needed | `spark.kubernetes.container.image` |
| Namespace | Not applicable | `spark.kubernetes.namespace` |
| Service account | Not applicable | `spark.kubernetes.authenticate.driver.serviceAccountName` |
| File references | Absolute paths | `local://` prefix for in-image files |

**deploy-mode on K8s:**

| Mode | Driver Location | Use Case |
|------|----------------|----------|
| `client` | On the machine running spark-submit | Interactive, debugging |
| `cluster` | In a K8s pod | Production, CI/CD |

In `cluster` mode, spark-submit creates a driver pod, which then creates executor pods. The spark-submit process exits after the driver pod is created. Logs are available via `kubectl logs`.

Reference: <https://spark.apache.org/docs/latest/running-on-kubernetes.html#submitting-applications-to-kubernetes>

### 4.3 RBAC Setup

Spark needs a Kubernetes service account with permissions to create, list, and delete pods (for executors).

**Namespace and ServiceAccount:**

```yaml
# namespace.yaml
apiVersion: v1
kind: Namespace
metadata:
  name: spark-jobs
```

```yaml
# service-account.yaml
apiVersion: v1
kind: ServiceAccount
metadata:
  name: spark
  namespace: spark-jobs
```

**Role and RoleBinding:**

```yaml
# role.yaml
apiVersion: rbac.authorization.k8s.io/v1
kind: Role
metadata:
  name: spark-role
  namespace: spark-jobs
rules:
  - apiGroups: [""]
    resources: ["pods"]
    verbs: ["create", "get", "list", "watch", "delete", "patch"]
  - apiGroups: [""]
    resources: ["pods/log"]
    verbs: ["get", "list"]
  - apiGroups: [""]
    resources: ["pods/exec"]
    verbs: ["create"]
  - apiGroups: [""]
    resources: ["services"]
    verbs: ["create", "get", "list", "delete"]
  - apiGroups: [""]
    resources: ["configmaps"]
    verbs: ["create", "get", "list", "delete", "update"]
  - apiGroups: [""]
    resources: ["persistentvolumeclaims"]
    verbs: ["create", "get", "list", "delete"]
```

```yaml
# rolebinding.yaml
apiVersion: rbac.authorization.k8s.io/v1
kind: RoleBinding
metadata:
  name: spark-role-binding
  namespace: spark-jobs
roleRef:
  apiGroup: rbac.authorization.k8s.io
  kind: Role
  name: spark-role
subjects:
  - kind: ServiceAccount
    name: spark
    namespace: spark-jobs
```

**Apply:**

```bash
kubectl apply -f namespace.yaml
kubectl apply -f service-account.yaml
kubectl apply -f role.yaml
kubectl apply -f rolebinding.yaml
```

**Verification:**

```bash
# Check service account can create pods
kubectl auth can-i create pods --namespace spark-jobs --as system:serviceaccount:spark-jobs:spark
# Output: yes
```

Reference: <https://spark.apache.org/docs/latest/running-on-kubernetes.html#rbac>

### 4.4 Docker Images

Spark provides official Docker images on Docker Hub:

```
apache/spark:4.1.0-scala2.13-java21-python3-r-ubuntu
apache/spark:4.0.1-scala2.13-java17-python3-r-ubuntu
```

**Image naming convention:**

```
apache/spark:{version}-scala{scala_version}-java{java_version}-python3-r-ubuntu
```

**Building custom images (with your JARs):**

```dockerfile
# Dockerfile.spark-lakehouse
FROM apache/spark:4.1.0-scala2.13-java21-python3-r-ubuntu

# Add Iceberg and AWS JARs
COPY jars/iceberg-spark-runtime-4.0_2.13-1.10.0.jar /opt/spark/jars/
COPY jars/bundle-2.24.6.jar /opt/spark/jars/
COPY jars/postgresql-42.7.4.jar /opt/spark/jars/

# Add pipeline scripts
COPY scripts/pipelines/ /opt/spark/work-dir/pipelines/

# Add Spark config defaults
COPY config/spark/spark-defaults.conf /opt/spark/conf/spark-defaults.conf

USER spark
```

```bash
docker build -t lakehouse-spark:4.1.0 -f Dockerfile.spark-lakehouse .

# Push to your registry
docker tag lakehouse-spark:4.1.0 registry.example.com/lakehouse-spark:4.1.0
docker push registry.example.com/lakehouse-spark:4.1.0
```

**Using in spark-submit:**

```bash
spark-submit \
    --master k8s://https://api-server:6443 \
    --conf spark.kubernetes.container.image=registry.example.com/lakehouse-spark:4.1.0 \
    local:///opt/spark/work-dir/pipelines/pipeline_spark41.py
```

**Spark also includes `docker-image-tool.sh`** for building custom images from a Spark distribution:

```bash
./bin/docker-image-tool.sh \
    -r registry.example.com \
    -t 4.1.0-lakehouse \
    -p kubernetes/dockerfiles/spark/bindings/python/Dockerfile \
    build
```

Reference: <https://spark.apache.org/docs/latest/running-on-kubernetes.html#docker-images>

### 4.5 Configuration Differences from Standalone

All Kubernetes-specific Spark configuration keys start with `spark.kubernetes.*`:

| Config Key | Purpose | Example |
|------------|---------|---------|
| `spark.kubernetes.container.image` | Docker image for driver and executors | `apache/spark:4.1.0-...` |
| `spark.kubernetes.driver.container.image` | Driver-specific image (overrides above) | |
| `spark.kubernetes.executor.container.image` | Executor-specific image (overrides above) | |
| `spark.kubernetes.namespace` | K8s namespace for pods | `spark-jobs` |
| `spark.kubernetes.authenticate.driver.serviceAccountName` | SA for driver pod | `spark` |
| `spark.kubernetes.driver.request.cores` | CPU request for driver | `1` |
| `spark.kubernetes.driver.limit.cores` | CPU limit for driver | `2` |
| `spark.kubernetes.executor.request.cores` | CPU request per executor | `2` |
| `spark.kubernetes.executor.limit.cores` | CPU limit per executor | `4` |
| `spark.kubernetes.memoryOverheadFactor` | Extra memory fraction | `0.1` (10%) |
| `spark.kubernetes.driver.label.*` | Labels on driver pod | |
| `spark.kubernetes.executor.label.*` | Labels on executor pods | |
| `spark.kubernetes.driver.annotation.*` | Annotations on driver pod | |
| `spark.kubernetes.node.selector.*` | Node selector constraints | |
| `spark.kubernetes.driver.volumes.*` | Volume mounts for driver | |
| `spark.kubernetes.executor.volumes.*` | Volume mounts for executors | |
| `spark.kubernetes.file.upload.path` | S3/HDFS path for uploading local files | `s3a://spark-uploads/` |
| `spark.kubernetes.driver.podTemplateFile` | Custom pod template for driver | |
| `spark.kubernetes.executor.podTemplateFile` | Custom pod template for executors | |

**Pod template example (for advanced customization):**

```yaml
# executor-pod-template.yaml
apiVersion: v1
kind: Pod
metadata:
  labels:
    app: spark-executor
spec:
  containers:
    - name: spark-executor
      resources:
        requests:
          memory: "4Gi"
          cpu: "2"
        limits:
          memory: "8Gi"
          cpu: "4"
      volumeMounts:
        - name: spark-local
          mountPath: /tmp/spark-local
  volumes:
    - name: spark-local
      emptyDir:
        sizeLimit: 20Gi
  tolerations:
    - key: "spark"
      operator: "Equal"
      value: "executor"
      effect: "NoSchedule"
  nodeSelector:
    workload: spark
```

```bash
spark-submit \
    --master k8s://... \
    --conf spark.kubernetes.executor.podTemplateFile=executor-pod-template.yaml \
    ...
```

Reference: <https://spark.apache.org/docs/latest/running-on-kubernetes.html#configuration>

### 4.6 JAR Delivery Strategies

Getting JARs to executor pods is one of the key differences between standalone and K8s deployments.

**Strategy 1: Bake into the Docker image (recommended)**

```dockerfile
FROM apache/spark:4.1.0-scala2.13-java21-python3-r-ubuntu
COPY jars/*.jar /opt/spark/jars/
```

```bash
spark-submit \
    --master k8s://... \
    --conf spark.kubernetes.container.image=lakehouse-spark:4.1.0 \
    --jars local:///opt/spark/jars/iceberg-spark-runtime-4.0_2.13-1.10.0.jar \
    local:///opt/spark/work-dir/pipeline.py
```

**Pros:** Fast startup, no network dependency at runtime, deterministic.
**Cons:** Image size increases, need to rebuild for JAR updates.

**Strategy 2: Remote S3/GCS/HDFS**

```bash
spark-submit \
    --master k8s://... \
    --jars s3a://lakehouse/jars/iceberg-spark-runtime-4.0_2.13-1.10.0.jar,\
           s3a://lakehouse/jars/aws-bundle-2.24.6.jar \
    s3a://lakehouse/scripts/pipeline.py
```

**Pros:** No image rebuilds, JARs managed centrally.
**Cons:** Slower startup (download per executor), requires S3 access from all pods.

**Strategy 3: Maven coordinates (--packages)**

```bash
spark-submit \
    --master k8s://... \
    --packages org.apache.iceberg:iceberg-spark-runtime-4.0_2.13:1.10.0 \
    local:///opt/spark/work-dir/pipeline.py
```

**Pros:** Simplest syntax, automatic dependency resolution.
**Cons:** Requires internet access from pods, slower, version resolution can be unpredictable. Not recommended for production.

**Strategy 4: Init container + shared volume**

```yaml
# In pod template
initContainers:
  - name: jar-downloader
    image: curlimages/curl
    command: ["sh", "-c"]
    args:
      - |
        curl -o /jars/iceberg.jar https://repo1.maven.org/.../iceberg-spark-runtime-4.0_2.13-1.10.0.jar
        curl -o /jars/aws-bundle.jar https://repo1.maven.org/.../bundle-2.24.6.jar
    volumeMounts:
      - name: jars
        mountPath: /jars
containers:
  - name: spark-executor
    volumeMounts:
      - name: jars
        mountPath: /opt/spark/jars-extra
volumes:
  - name: jars
    emptyDir: {}
```

**Pros:** JARs separated from app image, can be version-pinned.
**Cons:** More complex, slower startup.

**Recommendation:** Use Strategy 1 (baked in) for production. Use Strategy 2 (S3) for ad-hoc/development. This stack's `jars/` directory contains all needed JARs --- bake them into your custom image.

### 4.7 Secret Management

**Strategy 1: Kubernetes Secrets (recommended)**

```yaml
# spark-secrets.yaml
apiVersion: v1
kind: Secret
metadata:
  name: lakehouse-secrets
  namespace: spark-jobs
type: Opaque
stringData:
  postgres-user: "iceberg"
  postgres-password: "iceberg_password"
  s3-access-key: "admin"
  s3-secret-key: "admin_password"
```

```bash
kubectl apply -f spark-secrets.yaml
```

Mount as environment variables:

```bash
spark-submit \
    --master k8s://... \
    --conf spark.kubernetes.driver.secretKeyRef.POSTGRES_USER=lakehouse-secrets:postgres-user \
    --conf spark.kubernetes.driver.secretKeyRef.POSTGRES_PASSWORD=lakehouse-secrets:postgres-password \
    --conf spark.kubernetes.executor.secretKeyRef.S3_ACCESS_KEY=lakehouse-secrets:s3-access-key \
    --conf spark.kubernetes.executor.secretKeyRef.S3_SECRET_KEY=lakehouse-secrets:s3-secret-key \
    ...
```

Or mount as files:

```bash
spark-submit \
    --conf spark.kubernetes.driver.volumes.secret.lakehouse-secrets.mount.path=/etc/secrets \
    --conf spark.kubernetes.driver.volumes.secret.lakehouse-secrets.mount.readOnly=true \
    ...
```

**Strategy 2: Environment variables via pod template**

```yaml
# In pod template
containers:
  - name: spark-driver
    envFrom:
      - secretRef:
          name: lakehouse-secrets
```

**Strategy 3: External secret stores (HashiCorp Vault, AWS Secrets Manager)**

Use the External Secrets Operator to sync external secrets into K8s Secrets:

```yaml
apiVersion: external-secrets.io/v1beta1
kind: ExternalSecret
metadata:
  name: lakehouse-secrets
  namespace: spark-jobs
spec:
  refreshInterval: 1h
  secretStoreRef:
    name: vault-backend
    kind: SecretStore
  target:
    name: lakehouse-secrets
  data:
    - secretKey: postgres-password
      remoteRef:
        key: data/lakehouse/postgres
        property: password
```

**Comparison with .env approach:**

| Aspect | `.env` file (local) | K8s Secrets | External Secrets |
|--------|---------------------|-------------|------------------|
| Complexity | Low | Medium | High |
| Security | Low (plaintext file) | Medium (base64, etcd encryption) | High (vault-grade) |
| Rotation | Manual | kubectl apply | Automatic |
| Audit trail | None | K8s audit log | Full audit |
| Best for | Development | Staging/small prod | Enterprise |

References:
- K8s Secrets: <https://kubernetes.io/docs/concepts/configuration/secret/>
- External Secrets Operator: <https://external-secrets.io/>
- Spark K8s secrets: <https://spark.apache.org/docs/latest/running-on-kubernetes.html#secret-management>

### 4.8 Dynamic Allocation on K8s

Dynamic allocation creates and destroys executor pods based on workload demand. On Kubernetes, this is handled by the external shuffle service or the new shuffle-tracking mechanism.

**Enable dynamic allocation (shuffle tracking, no external service):**

```bash
spark-submit \
    --master k8s://... \
    --conf spark.dynamicAllocation.enabled=true \
    --conf spark.dynamicAllocation.shuffleTracking.enabled=true \
    --conf spark.dynamicAllocation.minExecutors=1 \
    --conf spark.dynamicAllocation.maxExecutors=20 \
    --conf spark.dynamicAllocation.initialExecutors=3 \
    --conf spark.dynamicAllocation.executorIdleTimeout=60s \
    --conf spark.dynamicAllocation.schedulerBacklogTimeout=1s \
    ...
```

**How it works:**

```
┌──────────────────────────────────────────────────────────────┐
│  Time → →                                                     │
│                                                               │
│  Executors:  ███ ███ ███                                     │
│              (3 initial)                                      │
│                                                               │
│  Load increases (tasks queued > schedulerBacklogTimeout):     │
│              ███ ███ ███ ███ ███ ███ ███ ███                 │
│              (scaled to 8)                                    │
│                                                               │
│  Load decreases (executors idle > executorIdleTimeout):       │
│              ███ ███                                          │
│              (scaled to 2, but >= minExecutors=1)            │
│                                                               │
│  Job completes:                                               │
│              ███                                              │
│              (1 remaining, then deleted)                      │
└──────────────────────────────────────────────────────────────┘
```

**Configuration parameters:**

| Config Key | Default | Description |
|------------|---------|-------------|
| `spark.dynamicAllocation.enabled` | `false` | Enable dynamic allocation |
| `spark.dynamicAllocation.shuffleTracking.enabled` | `false` | Track shuffle data to determine when executors can be removed (required for K8s without external shuffle service) |
| `spark.dynamicAllocation.minExecutors` | `0` | Minimum executor pods |
| `spark.dynamicAllocation.maxExecutors` | `infinity` | Maximum executor pods |
| `spark.dynamicAllocation.initialExecutors` | `minExecutors` | Starting executor count |
| `spark.dynamicAllocation.executorIdleTimeout` | `60s` | Time before idle executor is removed |
| `spark.dynamicAllocation.schedulerBacklogTimeout` | `1s` | Time with pending tasks before requesting new executors |
| `spark.dynamicAllocation.cachedExecutorIdleTimeout` | `infinity` | Timeout for executors with cached data |

**Databricks equivalent:** Databricks autoscaling on job clusters. Same concept, same parameters, different cluster manager.

Reference: <https://spark.apache.org/docs/latest/running-on-kubernetes.html#dynamic-allocation>

### 4.9 SDP on K8s

Spark Declarative Pipelines (SDP) runs via `spark-pipelines`, which wraps `spark-submit`. On Kubernetes, the `--master` URL changes but the pipeline spec stays the same.

**Running SDP on K8s:**

```bash
spark-pipelines run \
    --spec /opt/spark/work-dir/pipelines/spark-pipeline.yml \
    --master k8s://https://kubernetes.default.svc:6443 \
    --conf spark.kubernetes.container.image=lakehouse-spark:4.1.0 \
    --conf spark.kubernetes.namespace=spark-jobs \
    --conf spark.kubernetes.authenticate.driver.serviceAccountName=spark \
    --conf spark.executor.instances=3
```

Or equivalently via `spark-submit`:

```bash
spark-submit \
    --master k8s://https://kubernetes.default.svc:6443 \
    --deploy-mode cluster \
    --conf spark.kubernetes.container.image=lakehouse-spark:4.1.0 \
    --conf spark.kubernetes.namespace=spark-jobs \
    --conf spark.kubernetes.authenticate.driver.serviceAccountName=spark \
    --conf spark.pipelines.spec=/opt/spark/work-dir/pipelines/spark-pipeline.yml \
    local:///opt/spark/work-dir/pipelines/pipeline_sdp.py
```

**The pipeline spec file (`spark-pipeline.yml`) is unchanged:**

```yaml
name: lakehouse_pipeline
libraries:
  - file: pipeline_sdp.py
catalog: iceberg
database: bronze
storage: /tmp/lakehouse-checkpoint
```

**Key point:** The SDP pipeline definition is infrastructure-agnostic. The same `spark-pipeline.yml` and `pipeline_sdp.py` files run on local, standalone, and Kubernetes. Only the `--master` URL and K8s-specific `--conf` values change.

**Dockerfile for SDP on K8s:**

```dockerfile
FROM apache/spark:4.1.0-scala2.13-java21-python3-r-ubuntu

# JARs
COPY jars/iceberg-spark-runtime-4.0_2.13-1.10.0.jar /opt/spark/jars/
COPY jars/bundle-2.24.6.jar /opt/spark/jars/
COPY jars/postgresql-42.7.4.jar /opt/spark/jars/

# SDP pipeline definition + scripts
COPY scripts/pipelines/spark-pipeline.yml /opt/spark/work-dir/pipelines/
COPY scripts/pipelines/pipeline_sdp.py /opt/spark/work-dir/pipelines/

# Spark defaults (Iceberg catalog config, etc.)
COPY config/spark/spark-defaults.conf /opt/spark/conf/spark-defaults.conf

USER spark
```

### 4.10 Spark Connect Server on K8s

Deploy the Spark Connect server as a long-running Kubernetes Deployment with a Service for client access.

**Deployment:**

```yaml
# spark-connect-deployment.yaml
apiVersion: apps/v1
kind: Deployment
metadata:
  name: spark-connect-server
  namespace: spark-jobs
  labels:
    app: spark-connect
spec:
  replicas: 1
  selector:
    matchLabels:
      app: spark-connect
  template:
    metadata:
      labels:
        app: spark-connect
    spec:
      serviceAccountName: spark
      containers:
        - name: spark-connect
          image: lakehouse-spark:4.1.0
          command:
            - /opt/spark/sbin/start-connect-server.sh
          args:
            - --master
            - k8s://https://kubernetes.default.svc:6443
            - --conf
            - spark.kubernetes.container.image=lakehouse-spark:4.1.0
            - --conf
            - spark.kubernetes.namespace=spark-jobs
            - --conf
            - spark.kubernetes.authenticate.driver.serviceAccountName=spark
            - --conf
            - spark.executor.instances=2
            - --conf
            - spark.sql.catalog.iceberg=org.apache.iceberg.spark.SparkCatalog
            - --conf
            - spark.sql.catalog.iceberg.type=jdbc
            - --conf
            - spark.sql.catalog.iceberg.uri=jdbc:postgresql://postgres.data.svc:5432/iceberg_catalog
            - --conf
            - spark.connect.grpc.binding.port=15002
          ports:
            - containerPort: 15002
              name: grpc
            - containerPort: 4040
              name: spark-ui
          envFrom:
            - secretRef:
                name: lakehouse-secrets
          resources:
            requests:
              memory: "2Gi"
              cpu: "1"
            limits:
              memory: "4Gi"
              cpu: "2"
          readinessProbe:
            tcpSocket:
              port: 15002
            initialDelaySeconds: 30
            periodSeconds: 10
          livenessProbe:
            tcpSocket:
              port: 15002
            initialDelaySeconds: 60
            periodSeconds: 30
```

**Service:**

```yaml
# spark-connect-service.yaml
apiVersion: v1
kind: Service
metadata:
  name: spark-connect
  namespace: spark-jobs
spec:
  type: LoadBalancer   # or ClusterIP for internal access only
  selector:
    app: spark-connect
  ports:
    - name: grpc
      port: 15002
      targetPort: 15002
    - name: spark-ui
      port: 4040
      targetPort: 4040
```

**Apply:**

```bash
kubectl apply -f spark-connect-deployment.yaml
kubectl apply -f spark-connect-service.yaml
```

**Connect from anywhere:**

```bash
# Get the LoadBalancer IP
kubectl get svc spark-connect -n spark-jobs
# NAME              TYPE           CLUSTER-IP     EXTERNAL-IP      PORT(S)
# spark-connect     LoadBalancer   10.100.45.12   34.123.456.78    15002:31234/TCP

# From your laptop
pip install pyspark-client
python -c "
from pyspark.sql import SparkSession
spark = SparkSession.builder.remote('sc://34.123.456.78:15002').getOrCreate()
spark.sql('SHOW TABLES IN iceberg.bronze').show()
"
```

**This is the endgame:** A shared Spark endpoint accessible from anywhere, running on auto-scaling Kubernetes infrastructure, with the same DataFrame/SQL API as your local development environment.

### 4.11 Minimum Viable K8s Demo (Minikube)

A complete working example using minikube. This demonstrates the full laptop-to-K8s progression.

**Prerequisites:**

```bash
# Install minikube
# macOS: brew install minikube
# Linux: curl -LO https://storage.googleapis.com/minikube/releases/latest/minikube-linux-amd64 && sudo install minikube-linux-amd64 /usr/local/bin/minikube

# Install kubectl
# macOS: brew install kubectl
# Linux: curl -LO "https://dl.k8s.io/release/$(curl -L -s https://dl.k8s.io/release/stable.txt)/bin/linux/amd64/kubectl" && sudo install kubectl /usr/local/bin/kubectl
```

**Step 1: Start minikube**

```bash
minikube start \
    --cpus=4 \
    --memory=8g \
    --driver=docker

# Verify
kubectl cluster-info
```

**Step 2: Build and load custom Spark image**

```bash
# Build the image
docker build -t lakehouse-spark:4.1.0 -f Dockerfile.spark-lakehouse .

# Load into minikube (so it doesn't try to pull from a registry)
minikube image load lakehouse-spark:4.1.0
```

**Step 3: Create RBAC resources**

```bash
kubectl create namespace spark-jobs

kubectl create serviceaccount spark -n spark-jobs

kubectl create role spark-role -n spark-jobs \
    --verb=create,get,list,watch,delete,patch \
    --resource=pods,pods/log,services,configmaps

kubectl create rolebinding spark-role-binding -n spark-jobs \
    --role=spark-role \
    --serviceaccount=spark-jobs:spark
```

**Step 4: Submit a Spark job**

```bash
# Get minikube's K8s API endpoint
KUBE_API=$(kubectl config view --minify -o jsonpath='{.clusters[0].cluster.server}')

spark-submit \
    --master k8s://$KUBE_API \
    --deploy-mode cluster \
    --name spark-pi \
    --conf spark.kubernetes.container.image=lakehouse-spark:4.1.0 \
    --conf spark.kubernetes.namespace=spark-jobs \
    --conf spark.kubernetes.authenticate.driver.serviceAccountName=spark \
    --conf spark.executor.instances=2 \
    local:///opt/spark/examples/src/main/python/pi.py

# Watch pods
kubectl get pods -n spark-jobs -w
```

**Step 5: Deploy Spark Connect server**

```bash
# Apply the deployment and service from section 4.10
kubectl apply -f spark-connect-deployment.yaml
kubectl apply -f spark-connect-service.yaml

# Wait for it to be ready
kubectl rollout status deployment/spark-connect-server -n spark-jobs

# Port-forward to access locally
kubectl port-forward svc/spark-connect -n spark-jobs 15002:15002 &

# Connect from your laptop
pip install pyspark-client
python -c "
from pyspark.sql import SparkSession
spark = SparkSession.builder.remote('sc://localhost:15002').getOrCreate()
print(spark.sql('SELECT 1 + 1 AS result').collect())
"
```

**Step 6: Clean up**

```bash
kubectl delete namespace spark-jobs
minikube stop
minikube delete
```

---

## 5. The Progression Table

This is the core message of Act 5: the same code runs at every level.

### What Changes at Each Level

| Aspect | Local | Standalone | Spark Connect | Kubernetes |
|--------|-------|-----------|---------------|------------|
| **Master URL** | `local[*]` | `spark://host:7078` | `sc://host:15002` | `k8s://https://api:6443` |
| **Session builder** | `.master("local[*]")` | `.master("spark://...")` | `.remote("sc://...")` | `.master("k8s://...")` |
| **JVM on client** | Yes | Yes | No | Depends on deploy mode |
| **Config delivery** | `spark-defaults.conf` | `spark-defaults.conf` | Server-side only | `--conf` or ConfigMap |
| **Secret management** | `.env` file | `.env` file | Server-side or gRPC metadata | K8s Secrets |
| **Scaling** | Fixed (local cores) | Fixed (worker count) | Server decides | Dynamic allocation |
| **Client install** | `pip install pyspark` (300 MB) | `pip install pyspark` (300 MB) | `pip install pyspark-client` (1.5 MB) | `spark-submit` binary |
| **Monitoring** | `localhost:4040` | Spark Master UI (`:8080/8082`) | Server's Spark UI | K8s dashboard + Spark UI |
| **Failure blast radius** | Process crash | Worker restart | Client disconnect (server continues) | Pod rescheduling |
| **Multi-tenancy** | None | Manual | Built-in (shared server) | Namespace isolation |
| **Setup complexity** | None | Docker Compose | + start-connect-server.sh | + K8s cluster + RBAC |

### What Stays the Same

| Aspect | Constant Across All Levels |
|--------|---------------------------|
| **Application code** | Identical `.py` files |
| **DataFrame API** | Same `spark.table()`, `df.filter()`, `df.groupBy()` |
| **SQL queries** | Same SQL strings |
| **UDF definitions** | Same `@udf` decorators |
| **SDP pipeline specs** | Same `spark-pipeline.yml` |
| **SDP Python code** | Same `@dp.materialized_view` decorators |
| **Iceberg catalog config** | Same `spark.sql.catalog.iceberg.*` keys |
| **Docker images** | Same `apache/spark:4.1.0-...` base |
| **JAR versions** | Same Iceberg 1.10.0, AWS SDK 2.24.6 |
| **Table paths** | Same `iceberg.bronze.orders`, `iceberg.silver.*` |

### The Progression in One Session Builder

```python
from pyspark.sql import SparkSession

# Level 1: Laptop
spark = SparkSession.builder.master("local[*]").getOrCreate()

# Level 2: Standalone cluster
spark = SparkSession.builder.master("spark://spark-master-41:7078").getOrCreate()

# Level 3: Spark Connect (thin client)
spark = SparkSession.builder.remote("sc://spark-master-41:15002").getOrCreate()

# Level 4: Kubernetes via Spark Connect
spark = SparkSession.builder.remote("sc://spark-connect.k8s.example.com:15002").getOrCreate()

# THE REST OF YOUR CODE IS IDENTICAL AT EVERY LEVEL
df = spark.table("iceberg.bronze.orders")
df.filter(df.event_type == "order_created") \
  .groupBy("brand_id") \
  .count() \
  .orderBy(f.desc("count")) \
  .show(10)
```

---

## 6. Production Considerations

### 6.1 Monitoring

#### Spark UI

Every Spark application exposes a web UI on port 4040 (by default). On Kubernetes, expose it via a Service or port-forward.

```bash
# Local/Standalone: direct access
open http://localhost:8082  # Spark Master UI (this stack)
open http://localhost:4040  # Application UI (during job execution)

# K8s: port-forward
kubectl port-forward pod/spark-connect-server-xxx -n spark-jobs 4040:4040
open http://localhost:4040
```

**Key Spark UI pages:**

| Page | What to Monitor |
|------|----------------|
| Jobs | Overall progress, failed stages |
| Stages | Task distribution, skew, spill |
| Storage | Cached DataFrames, memory usage |
| Environment | Active configuration |
| Executors | Per-executor metrics, GC time |
| SQL | Query plans, physical plan details |
| Streaming | Micro-batch latency, throughput |

Reference: <https://spark.apache.org/docs/latest/web-ui.html>

#### Prometheus + Grafana

Spark exposes metrics via the Prometheus sink.

**Enable in spark-defaults.conf:**

```properties
spark.metrics.conf.*.sink.prometheusServlet.class=org.apache.spark.metrics.sink.PrometheusServlet
spark.metrics.conf.*.sink.prometheusServlet.path=/metrics/prometheus
spark.metrics.conf.master.sink.prometheusServlet.path=/metrics/master/prometheus
spark.metrics.conf.applications.sink.prometheusServlet.path=/metrics/applications/prometheus
spark.ui.prometheus.enabled=true
```

**Key metrics to monitor:**

| Metric | Description | Alert Threshold |
|--------|-------------|-----------------|
| `spark_executor_totalDuration_ms` | Total task execution time | Trend increase |
| `spark_executor_totalGCTime_ms` | GC time across executors | > 10% of total duration |
| `spark_executor_memoryUsed_bytes` | Memory consumption | > 80% of allocated |
| `spark_streaming_lastCompletedBatch_processingDelay_ms` | Streaming batch latency | > batch interval |
| `spark_executor_diskBytesSpilled` | Disk spill | > 0 (indicates memory pressure) |
| `spark_executor_failedTasks` | Failed task count | > 0 |

**Prometheus scrape config:**

```yaml
# prometheus.yml
scrape_configs:
  - job_name: spark
    metrics_path: /metrics/prometheus
    static_configs:
      - targets:
          - spark-master-41:8082    # Master metrics
          - spark-connect:4040      # Application metrics
    # On K8s, use service discovery:
    # kubernetes_sd_configs:
    #   - role: pod
    #     namespaces:
    #       names: [spark-jobs]
    #     selectors:
    #       - role: pod
    #         label: app=spark-connect
```

**Grafana dashboards:**

Community dashboards for Spark on Grafana are available. Search for "Apache Spark" on <https://grafana.com/grafana/dashboards/>. Dashboard ID 7890 is a commonly used starting point.

References:
- Spark metrics: <https://spark.apache.org/docs/latest/monitoring.html#metrics>
- Prometheus sink: <https://spark.apache.org/docs/latest/monitoring.html#prometheus>

#### Airflow Monitoring

Airflow 3.x exposes its own health and metrics endpoints:

```bash
# Health check
curl http://localhost:8085/api/v2/monitor/health

# Scheduler health
curl http://localhost:8974/health

# List recent DAG runs via REST API
curl -u admin:admin http://localhost:8085/api/v2/dags/lakehouse_sdp_pipeline/dagRuns \
    -H "Content-Type: application/json"
```

**Airflow StatsD metrics (for Prometheus via statsd_exporter):**

```properties
# airflow.cfg or environment variable
AIRFLOW__METRICS__STATSD_ON=True
AIRFLOW__METRICS__STATSD_HOST=statsd-exporter
AIRFLOW__METRICS__STATSD_PORT=8125
```

**Key Airflow metrics:**

| Metric | Description |
|--------|-------------|
| `airflow.dag.duration` | DAG run duration |
| `airflow.task.duration` | Individual task duration |
| `airflow.scheduler.tasks.running` | Currently running tasks |
| `airflow.scheduler.tasks.starving` | Tasks waiting for a slot |
| `airflow.dag_processing.total_parse_time` | DAG file parse time |

Reference: <https://airflow.apache.org/docs/apache-airflow/stable/administration-and-deployment/logging-monitoring/metrics.html>

### 6.2 Logging

#### Structured Logging in Spark

Spark 4.0 introduced structured logging with JSON output:

```properties
# spark-defaults.conf
spark.log.structuredLogging.enabled=true
spark.log.level=WARN
```

Output format:

```json
{
  "ts": "2026-03-18T10:30:45.123Z",
  "level": "INFO",
  "msg": "Submitted job 42",
  "logger": "org.apache.spark.scheduler.DAGScheduler",
  "mdc": {
    "app_id": "app-20260318103000-0001",
    "executor_id": "driver"
  }
}
```

**On Kubernetes**, container logs are automatically collected by the kubelet. Use a log aggregation stack:

```
┌──────────────┐    ┌──────────┐    ┌───────────────┐    ┌──────────┐
│ Spark Pods   │───►│ Fluent   │───►│ Elasticsearch │───►│ Kibana   │
│ (stdout/err) │    │ Bit      │    │ / OpenSearch  │    │          │
└──────────────┘    └──────────┘    └───────────────┘    └──────────┘
```

Or the simpler alternative:

```
┌──────────────┐    ┌──────────┐    ┌──────────┐
│ Spark Pods   │───►│ Fluent   │───►│  Loki    │───► Grafana
│ (stdout/err) │    │ Bit      │    │          │
└──────────────┘    └──────────┘    └──────────┘
```

**Viewing logs:**

```bash
# Local/Docker
docker logs spark-master-41
docker logs spark-worker-41
./lakehouse logs spark-master

# Kubernetes
kubectl logs -f spark-connect-server-xxx -n spark-jobs
kubectl logs -f lakehouse-pipeline-driver -n spark-jobs

# Historical logs (if using log aggregation)
# Kibana: search for app_id="app-20260318103000-0001"
```

**Spark event logs** for the History Server:

```properties
# spark-defaults.conf
spark.eventLog.enabled=true
spark.eventLog.dir=s3a://lakehouse/spark-events/
```

```bash
# Start the History Server to browse completed applications
/opt/spark/sbin/start-history-server.sh
# Access at http://localhost:18080
```

References:
- Spark structured logging: <https://spark.apache.org/docs/latest/configuration.html#spark-logging>
- Spark History Server: <https://spark.apache.org/docs/latest/monitoring.html#viewing-after-the-fact>
- Fluent Bit on K8s: <https://docs.fluentbit.io/manual/installation/kubernetes>

#### Airflow Logging

Airflow stores task logs in the filesystem by default (`logs/airflow/`). For production, configure remote logging:

```properties
# Send logs to S3
AIRFLOW__LOGGING__REMOTE_LOGGING=True
AIRFLOW__LOGGING__REMOTE_BASE_LOG_FOLDER=s3://airflow-logs/
AIRFLOW__LOGGING__REMOTE_LOG_CONN_ID=aws_default
```

**Viewing task logs:**

```bash
# Via CLI
docker exec airflow-webserver airflow tasks logs \
    lakehouse_sdp_pipeline run_sdp_pipeline 2026-03-18

# Via UI
# http://localhost:8085 → DAGs → lakehouse_sdp_pipeline → Graph → click task → Logs

# Via filesystem (local)
cat logs/airflow/dag_id=lakehouse_sdp_pipeline/run_id=scheduled__2026-03-18/task_id=run_sdp_pipeline/attempt=1.log
```

### 6.3 Cost Optimization

#### Spot Instances / Preemptible VMs

On cloud Kubernetes (EKS, GKE, AKS), use spot/preemptible instances for executor pods to reduce cost by 60-90%.

```bash
spark-submit \
    --master k8s://... \
    --conf spark.kubernetes.executor.label.workload-type=spot \
    --conf spark.kubernetes.executor.node.selector.node-lifecycle=spot \
    --conf spark.dynamicAllocation.enabled=true \
    --conf spark.dynamicAllocation.shuffleTracking.enabled=true \
    --conf spark.task.maxFailures=8 \
    ...
```

**Key configurations for spot resilience:**

| Config | Value | Reason |
|--------|-------|--------|
| `spark.task.maxFailures` | `8` (default: 4) | Tolerate spot preemption |
| `spark.speculation` | `true` | Re-launch slow tasks (may be preempted) |
| `spark.dynamicAllocation.enabled` | `true` | Replace lost executors automatically |
| `spark.kubernetes.executor.node.selector.node-lifecycle` | `spot` | Schedule executors on spot nodes |
| `spark.kubernetes.driver.node.selector.node-lifecycle` | `on-demand` | Keep driver on stable nodes |

**Databricks equivalent:** Spot instances for worker nodes in job clusters. Same concept --- Databricks just manages the node pool for you.

#### Dynamic Allocation (Revisited)

Without dynamic allocation, you pay for all executor pods for the entire job duration, even during idle phases (reading metadata, writing small partitions).

**Cost impact example (3-hour daily job):**

| Setup | Executor-Hours/Day | Monthly Cost (at $0.10/vCPU-hour, 4 vCPU) |
|-------|-------------------|--------------------------------------------|
| Static: 10 executors | 30 | $360 |
| Dynamic: 2-10 executors (avg 5) | 15 | $180 |
| Dynamic + Spot | 15 (at 30% cost) | $54 |

#### Right-Sizing

Monitor executor metrics (via Spark UI or Prometheus) and adjust:

```bash
# If executors show low CPU utilization:
--conf spark.executor.cores=2       # Reduce from 4

# If executors show memory spill:
--conf spark.executor.memory=8g     # Increase from 4g

# If tasks are very small (< 100ms):
--conf spark.sql.adaptive.coalescePartitions.enabled=true
--conf spark.sql.adaptive.advisoryPartitionSizeInBytes=256m
```

**Adaptive Query Execution (AQE)** is enabled by default in Spark 3.2+. It automatically:
- Coalesces small shuffle partitions
- Converts sort-merge joins to broadcast joins when data is small
- Optimizes skewed joins

```properties
# These are defaults in Spark 4.x but worth verifying:
spark.sql.adaptive.enabled=true
spark.sql.adaptive.coalescePartitions.enabled=true
spark.sql.adaptive.skewJoin.enabled=true
```

Reference: <https://spark.apache.org/docs/latest/sql-performance-tuning.html#adaptive-query-execution>

---

## 7. References

### Official Documentation

| Resource | URL |
|----------|-----|
| Apache Spark Documentation | <https://spark.apache.org/docs/latest/> |
| Spark Connect Overview | <https://spark.apache.org/docs/latest/spark-connect-overview.html> |
| Spark on Kubernetes | <https://spark.apache.org/docs/latest/running-on-kubernetes.html> |
| Spark Configuration | <https://spark.apache.org/docs/latest/configuration.html> |
| Spark Monitoring | <https://spark.apache.org/docs/latest/monitoring.html> |
| Apache Airflow Documentation | <https://airflow.apache.org/docs/apache-airflow/stable/> |
| Airflow Spark Provider | <https://airflow.apache.org/docs/apache-airflow-providers-apache-spark/stable/> |
| Airflow K8s Provider | <https://airflow.apache.org/docs/apache-airflow-providers-cncf-kubernetes/stable/> |
| Apache Iceberg Documentation | <https://iceberg.apache.org/docs/latest/> |
| Iceberg Spark Procedures | <https://iceberg.apache.org/docs/latest/spark-procedures/> |
| Kubernetes Documentation | <https://kubernetes.io/docs/> |

### Spark Connect Protocol

| Resource | URL |
|----------|-----|
| Protobuf definitions | <https://github.com/apache/spark/tree/master/connector/connect/common/src/main/protobuf/spark/connect> |
| Connect server source | <https://github.com/apache/spark/tree/master/connector/connect/server> |
| pyspark-client on PyPI | <https://pypi.org/project/pyspark-client/> |
| Apache Arrow IPC format | <https://arrow.apache.org/docs/format/IPC.html> |

### Kubernetes / Spark Operator

| Resource | URL |
|----------|-----|
| Spark K8s Operator (Google) | <https://github.com/GoogleCloudPlatform/spark-on-k8s-operator> |
| Spark K8s Operator (kubeflow) | <https://github.com/kubeflow/spark-operator> |
| K8s RBAC documentation | <https://kubernetes.io/docs/reference/access-authn-authz/rbac/> |
| K8s Secrets documentation | <https://kubernetes.io/docs/concepts/configuration/secret/> |
| External Secrets Operator | <https://external-secrets.io/> |
| minikube | <https://minikube.sigs.k8s.io/docs/> |

### Lakehouse Stack Files

| File | Purpose |
|------|---------|
| `scripts/demos/overarchitected/05a_airflow_sdp.py` | Act 5a demo: Airflow + SDP operators |
| `scripts/demos/overarchitected/05b_spark_connect.py` | Act 5b demo: Spark Connect progression |
| `dags/sdp_pipeline.py` | Production DAG: SDP via SparkSubmitOperator |
| `dags/lakehouse_medallion_pipeline.py` | Production DAG: Medallion pipeline |
| `dags/iceberg_maintenance.py` | Production DAG: Iceberg table maintenance |
| `docker-compose-airflow.yml` | Airflow 3.x Docker Compose |
| `docker-compose-spark41.yml` | Spark 4.1 standalone cluster |
| `docs/guides/airflow.md` | Airflow orchestration guide |
| `docs/guides/pipelines.md` | Data pipelines guide (imperative vs declarative) |
| `.claude/skills/SDP.md` | SDP complete reference |
| `config/spark/spark-defaults.conf` | Spark configuration (not in git) |

### Version Matrix

| Component | Version | Notes |
|-----------|---------|-------|
| Apache Spark | 4.1.0 (primary), 4.0.1 (secondary) | Scala 2.13 |
| Apache Iceberg | 1.10.0 | spark-runtime JAR |
| Apache Airflow | 3.1.6 | Python 3.12 |
| Apache Kafka | 3.6 | Via Docker Compose |
| PostgreSQL | Latest | Iceberg catalog + Airflow metadata |
| SeaweedFS | Latest | S3-compatible object storage |
| AWS SDK v2 | 2.24.6 | Exact version for Hadoop 3.4.1 |
| Java (Spark 4.0) | 17 | Official Spark 4.0 image |
| Java (Spark 4.1) | 21 | Official Spark 4.1 image |
| Java (Airflow) | 17 | For local Spark client operations |
| pyspark-client | Latest (matches Spark 4.x) | 1.5 MB thin client |
