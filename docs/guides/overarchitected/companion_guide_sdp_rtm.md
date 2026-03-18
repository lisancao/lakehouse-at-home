# Companion Guide: Spark Declarative Pipelines + Real-Time Mode

> **Audience**: Data engineers familiar with Databricks DLT/Lakeflow Declarative Pipelines evaluating OSS Spark 4.1 for self-hosted lakehouse pipelines
> **Complements**: OverArchitected show Act 4 (SDP + RTM segment), demo scripts in `scripts/demos/overarchitected/`

---

## Table of Contents

1. [Introduction](#1-introduction)
2. [SDP Deep Dive](#2-sdp-deep-dive)
3. [RTM Deep Dive](#3-rtm-deep-dive)
4. [SDP + Streaming: The Convergence](#4-sdp--streaming-the-convergence)
5. [Running SDP from Airflow](#5-running-sdp-from-airflow)
6. [SDP on Kubernetes](#6-sdp-on-kubernetes)
7. [Production Considerations](#7-production-considerations)
8. [Comparison to Databricks DLT/Lakeflow Declarative Pipelines](#8-comparison-to-databricks-dltlakeflow-declarative-pipelines)
9. [References](#9-references)

---

## 1. Introduction

Apache Spark 4.1.0, released in December 2025, introduced two headline features that fundamentally change how data pipelines are built and how streaming workloads execute:

- **Spark Declarative Pipelines (SDP)**: A framework for defining ETL pipelines declaratively using Python decorators, automatic dependency resolution, and framework-managed persistence. SDP is the open-source implementation of the programming model pioneered by Databricks Delta Live Tables (DLT), now called Lakeflow Declarative Pipelines.

- **Real-Time Mode (RTM)**: A new execution mode for Structured Streaming that replaces micro-batch scheduling with concurrent long-lived tasks and an in-memory streaming shuffle, achieving sub-300ms p99 latency for stateful queries and single-digit millisecond latency for stateless pipelines.

These two features address different dimensions of the same problem: SDP simplifies **what** you build (pipeline definition and orchestration), while RTM improves **how fast** data flows through those pipelines. Together, they represent the most significant advancement in Spark's data engineering capabilities since Structured Streaming was introduced in Spark 2.0.

### Why These Two Features Matter Together

The traditional approach to building a production Spark pipeline requires managing four concerns simultaneously:

1. **Transformation logic** -- the actual business rules
2. **Execution order** -- which transformations run before others
3. **Persistence** -- writing results to tables, handling modes
4. **Latency** -- how quickly new data becomes available downstream

SDP eliminates concerns 2 and 3. You declare transformations as decorated functions that return DataFrames; the framework resolves dependencies and handles writes. RTM addresses concern 4 by replacing the micro-batch execution model with continuous processing. When combined, a single pipeline definition can contain both batch materialized views and streaming tables with sub-second latency -- managed by the same framework, using the same API.

### Version Requirements

| Component | Minimum Version | Notes |
|-----------|----------------|-------|
| Apache Spark | 4.1.0 | SDP and RTM are not available in 4.0.x or earlier |
| Python | 3.10+ | Required for `pyspark.pipelines` module |
| spark-pipelines CLI | Bundled with Spark 4.1.0 | Not a separate install |
| Apache Kafka | 3.6+ | Required for RTM source/sink |
| Apache Iceberg | 1.10.0 | Recommended table format for SDP |

**SPIP (Spark Project Improvement Proposal) references:**
- SDP: [SPARK-51689](https://issues.apache.org/jira/browse/SPARK-51689) -- "Spark Declarative Pipelines"
- RTM: [SPARK-52330](https://issues.apache.org/jira/browse/SPARK-52330) -- "Real-Time Mode for Structured Streaming"

---

## 2. SDP Deep Dive

### 2.1 Paradigm Shift: Imperative to Declarative

The defining characteristic of SDP is the inversion of control over execution and persistence. In an imperative pipeline, the developer controls everything: read order, write destinations, error handling, retry logic, and dependency sequencing. In a declarative pipeline, the developer declares intent and the framework takes responsibility for execution mechanics.

#### Imperative (Traditional PySpark)

```python
from pyspark.sql import SparkSession, functions as f

def main():
    spark = SparkSession.builder.getOrCreate()

    # You manage: read order, write mode, table names, execution sequence
    df_raw = spark.read.parquet("/data/orders.parquet")
    df_raw.write.mode("overwrite").saveAsTable("iceberg.bronze.orders")

    df_clean = spark.table("iceberg.bronze.orders").filter(f.col("id").isNotNull())
    df_clean.write.mode("overwrite").saveAsTable("iceberg.silver.orders")

    df_gold = spark.table("iceberg.silver.orders").groupBy("region").agg(f.sum("amount"))
    df_gold.write.mode("overwrite").saveAsTable("iceberg.gold.revenue")

if __name__ == "__main__":
    main()
```

Problems with this approach at scale:

- **Fragile ordering**: If you reorder function calls, the pipeline breaks silently or produces wrong results.
- **Implicit dependencies**: Nothing in the code formally declares that `silver.orders` depends on `bronze.orders`. A new team member could refactor and break the chain.
- **Redundant boilerplate**: Every function repeats the same `.write.mode("overwrite").saveAsTable(...)` pattern.
- **Testing difficulty**: Functions produce side effects (disk writes), making unit testing require mocks for I/O.
- **No validation**: You cannot verify the pipeline's structure without running it.

#### Declarative (SDP)

```python
from typing import Any
from pyspark import pipelines as dp
from pyspark.sql import functions as f

spark: Any  # Injected by framework at runtime

@dp.materialized_view(name="bronze.orders")
def bronze_orders():
    return spark.read.parquet("/data/orders.parquet")

@dp.materialized_view(name="silver.orders")
def silver_orders():
    return spark.table("iceberg.bronze.orders").filter(f.col("id").isNotNull())

@dp.materialized_view(name="gold.revenue")
def gold_revenue():
    return (
        spark.table("iceberg.silver.orders")
        .groupBy("region")
        .agg(f.sum("amount").alias("total_revenue"))
    )
```

What changed:

- **No `SparkSession.builder`**: The framework creates and manages the session.
- **No `.write()` calls**: Functions return DataFrames. The framework handles persistence.
- **No manual ordering**: The framework detects that `silver.orders` calls `spark.table("iceberg.bronze.orders")` and schedules bronze before silver automatically.
- **Pure functions**: Each decorated function is a transformation with no side effects, making it testable.
- **Validated before execution**: `spark-pipelines dry-run` verifies the entire dependency graph without running anything.

**JIRA**: [SPARK-51689](https://issues.apache.org/jira/browse/SPARK-51689) -- Initial SPIP for Spark Declarative Pipelines
**JIRA**: [SPARK-51955](https://issues.apache.org/jira/browse/SPARK-51955) -- `pyspark.pipelines` module implementation

### 2.2 The Two Decorators: @dp.materialized_view and @dp.table

SDP provides exactly two decorators for defining pipeline outputs. The choice between them determines whether data is fully recomputed or incrementally maintained.

#### @dp.materialized_view()

For batch data that should be fully recomputed on every pipeline run.

```python
@dp.materialized_view(
    name="database.table_name",       # Required: output table (no catalog prefix)
    comment="Human-readable description",  # Optional: stored in catalog metadata
    partition_cols=["date", "hour"],   # Optional: physical partitioning
    table_properties={                 # Optional: Iceberg/Delta table properties
        "write.format.default": "parquet",
        "write.parquet.compression-codec": "zstd",
    }
)
def my_view():
    return some_dataframe  # Must return a DataFrame
```

Semantics:
- The function body executes on every pipeline run
- The output table is fully replaced (equivalent to `mode("overwrite")`)
- Suitable for dimension tables, lookup tables, full-refresh aggregations
- No checkpoint state is maintained

#### @dp.table()

For streaming or incremental data that maintains state across runs.

```python
@dp.table(
    name="database.streaming_orders",
    comment="Real-time order events from Kafka",
    partition_cols=["event_date"],
)
def streaming_orders():
    return (
        spark.readStream
        .format("kafka")
        .option("kafka.bootstrap.servers", "broker:9092")
        .option("subscribe", "orders")
        .load()
        .select(f.from_json(f.col("value").cast("string"), schema).alias("data"))
        .select("data.*")
    )
```

Semantics:
- The function body defines a streaming query
- State is maintained between runs via checkpoints
- New data is appended incrementally
- Suitable for streaming ingestion, event tables, incremental aggregations

#### Decorator Parameter Reference

| Parameter | Type | Required | Description |
|-----------|------|----------|-------------|
| `name` | `str` | Yes | Output table name in `database.table` format (no catalog prefix -- catalog comes from `pipeline.yml`) |
| `comment` | `str` | No | Table description, persisted in catalog metadata |
| `partition_cols` | `list[str]` | No | Columns to partition by (Iceberg hidden partitioning recommended instead) |
| `table_properties` | `dict[str, str]` | No | Table-level properties passed to the table format (Iceberg, Delta) |

**JIRA**: [SPARK-52080](https://issues.apache.org/jira/browse/SPARK-52080) -- `@dp.materialized_view` decorator implementation
**JIRA**: [SPARK-52081](https://issues.apache.org/jira/browse/SPARK-52081) -- `@dp.table` decorator implementation

#### Function Contract

Every decorated function must satisfy three rules:

1. **Must return a DataFrame** (or streaming DataFrame for `@dp.table`). Returning `None` is an error.
2. **No side effects**: No `.write()` calls, no `print()` in production, no external API calls. The function is a pure transformation.
3. **Deterministic**: Same inputs must produce same outputs. Non-deterministic functions (e.g., those using `current_timestamp()` for business logic rather than metadata) can produce inconsistent results across retries.

```python
# CORRECT: Pure transformation, returns DataFrame
@dp.materialized_view(name="silver.orders")
def orders():
    return spark.table("iceberg.bronze.orders").filter(f.col("id").isNotNull())

# WRONG: Side effects (write call)
@dp.materialized_view(name="silver.orders")
def orders():
    df = spark.table("iceberg.bronze.orders")
    df.write.saveAsTable("iceberg.silver.orders")  # Framework handles this!
    return df

# WRONG: Side effects (print with count triggers unnecessary computation)
@dp.materialized_view(name="silver.orders")
def orders():
    df = spark.table("iceberg.bronze.orders")
    print(f"Processing {df.count()} rows")  # Triggers full scan!
    return df.filter(f.col("id").isNotNull())
```

### 2.3 Automatic Dependency Resolution

SDP builds a directed acyclic graph (DAG) of table dependencies by statically scanning `spark.table()` calls in each decorated function at import time. This is not a runtime analysis -- the framework parses the function's AST (abstract syntax tree) looking for string literal arguments to `spark.table()`.

#### How It Works

```python
@dp.materialized_view(name="silver.enriched")
def enriched():
    orders = spark.table("iceberg.bronze.orders")       # Dependency detected
    products = spark.table("iceberg.bronze.products")    # Dependency detected
    return orders.join(products, "product_id")
```

The framework detects two dependencies:
- `silver.enriched` depends on `bronze.orders`
- `silver.enriched` depends on `bronze.products`

It then topologically sorts the entire graph to determine execution order. If `bronze.orders` and `bronze.products` have no upstream dependencies, they can execute in parallel, followed by `silver.enriched`.

#### The Critical Naming Convention

This is the single most common source of errors when adopting SDP. The decorator `name` parameter and `spark.table()` calls use **different naming formats**:

| Context | Format | Example |
|---------|--------|---------|
| Decorator `name=` | `database.table` | `@dp.materialized_view(name="bronze.orders")` |
| `spark.table()` | `catalog.database.table` | `spark.table("iceberg.bronze.orders")` |

The reason: the decorator defines where to write, and the catalog prefix comes from `pipeline.yml`. The `spark.table()` call reads data, which requires the full three-part name for the dependency scanner to match it against a decorated function's output.

```python
# CORRECT
@dp.materialized_view(name="silver.orders")       # Two-part: database.table
def orders():
    return spark.table("iceberg.bronze.raw")       # Three-part: catalog.database.table

# WRONG: Catalog in decorator name
@dp.materialized_view(name="iceberg.silver.orders")  # Three-part in decorator!
def orders():
    return spark.table("bronze.raw")                   # Two-part in spark.table!
```

#### What Breaks Dependency Detection

The scanner only detects **string literals** passed directly to `spark.table()`. These patterns will NOT be detected:

```python
# WRONG: Variable reference -- not detected
table_name = "iceberg.bronze.orders"
df = spark.table(table_name)

# WRONG: f-string -- not detected
schema = "bronze"
df = spark.table(f"iceberg.{schema}.orders")

# WRONG: spark.sql() -- not detected
df = spark.sql("SELECT * FROM iceberg.bronze.orders")

# WRONG: spark.read.table() -- not detected
df = spark.read.table("iceberg.bronze.orders")

# CORRECT: String literal directly in spark.table()
df = spark.table("iceberg.bronze.orders")
```

If a dependency is not detected, the framework may execute tables in the wrong order, causing a "table not found" error or reading stale data. There is currently no `spark.sql()` scanning -- this is a known limitation tracked in [SPARK-52500](https://issues.apache.org/jira/browse/SPARK-52500).

#### Visualizing the Graph

```bash
spark-pipelines graph --spec pipeline.yml
```

Example output:
```
bronze.dim_brands ─────────────────────────────────────┐
bronze.dim_locations ──────────────────────────┐       │
bronze.orders ──────────────────────┐          │       │
                                    ├─► silver.orders_enriched ─┬─► gold.hourly_metrics
                                    │          │                ├─► gold.delivery_performance
                                    │          │                └─► gold.brand_summary ◄──┘
                                    └─► silver.order_lifecycle ─┘
```

### 2.4 Pipeline Spec YAML Configuration

The `pipeline.yml` file is the bridge between your Python pipeline code and the `spark-pipelines` CLI. It specifies which Python files contain decorated functions, which catalog to write to, where to store streaming checkpoints, and optional Spark configuration overrides.

#### Complete Reference

```yaml
# ─── Identity ───────────────────────────────────────────
name: my_pipeline                     # Required: unique pipeline name

# ─── Source Code ────────────────────────────────────────
libraries:                            # Required: Python files with @dp decorators
  - file: pipeline.py                 # Can be a single file
  - file: transforms/silver.py        # Or multiple files across directories
  - file: transforms/gold.py

# ─── Catalog Configuration ──────────────────────────────
catalog: iceberg                      # Required: default catalog for output tables
database: bronze                      # Optional: default database (rarely needed)

# ─── Streaming Configuration ────────────────────────────
storage: /tmp/checkpoints             # Required for @dp.table; optional for @dp.materialized_view

# ─── Spark Configuration ────────────────────────────────
configuration:                        # Optional: Spark configs applied at runtime
  spark.sql.shuffle.partitions: "200"
  spark.sql.adaptive.enabled: "true"
  spark.sql.adaptive.coalescePartitions.enabled: "true"
  spark.executor.memory: "4g"
  spark.driver.memory: "2g"

# ─── Cluster Configuration (managed deployments) ───────
cluster:                              # Optional: for managed Spark environments
  spark_version: "4.1.0"
  node_type: "Standard_DS3_v2"
  num_workers: 4
```

#### Field Semantics

| Field | Required | Purpose | Gotcha |
|-------|----------|---------|--------|
| `name` | Yes | Unique identifier for this pipeline | Must be unique across all pipelines in the environment |
| `libraries` | Yes | Python files containing `@dp` decorated functions | Paths are relative to the YAML file's directory |
| `catalog` | Yes | Catalog prefix for output tables | If `catalog: iceberg`, then `@dp.materialized_view(name="bronze.orders")` writes to `iceberg.bronze.orders` |
| `database` | No | Default database when decorator `name` has no dot | Rarely used since best practice is always `name="database.table"` |
| `storage` | Conditional | Checkpoint directory for streaming tables | Required if any `@dp.table()` decorators are present; harmless to include for batch-only |
| `configuration` | No | Spark config overrides | Applied after `spark-defaults.conf`, so these take precedence |

#### Multi-Environment Strategy

Create separate YAML files per environment with shared Python code:

```
pipelines/
  pipeline.py              # Shared transformation code
  pipeline-dev.yml         # Dev: small partitions, local storage
  pipeline-staging.yml     # Staging: moderate resources, S3 storage
  pipeline-prod.yml        # Production: full resources, S3, tuned configs
```

```yaml
# pipeline-dev.yml
name: orders_pipeline_dev
libraries:
  - file: pipeline.py
catalog: iceberg_dev
storage: /tmp/dev-checkpoints
configuration:
  spark.sql.shuffle.partitions: "10"
  spark.executor.memory: "2g"
```

```yaml
# pipeline-prod.yml
name: orders_pipeline_prod
libraries:
  - file: pipeline.py
catalog: iceberg_prod
storage: s3a://lakehouse-prod/checkpoints/orders
configuration:
  spark.sql.shuffle.partitions: "400"
  spark.executor.memory: "8g"
  spark.sql.adaptive.enabled: "true"
  spark.sql.adaptive.skewJoin.enabled: "true"
```

### 2.5 spark-pipelines CLI

The `spark-pipelines` CLI is bundled with Spark 4.1.0 and provides five commands for managing declarative pipelines. It wraps `spark-submit` internally, so it inherits all standard Spark submission behavior (JARs, packages, master URL, etc.).

#### spark-pipelines init

Creates a template `pipeline.yml` in the current directory.

```bash
spark-pipelines init
```

Produces:
```yaml
name: my_pipeline
libraries:
  - file: pipeline.py
catalog: spark_catalog
database: default
storage: /tmp/checkpoints
```

This is a starting point -- you will always need to customize the catalog, libraries, and storage.

#### spark-pipelines dry-run

Validates the pipeline without executing any transformations. This is the most important pre-deployment step.

```bash
spark-pipelines dry-run --spec pipeline.yml
```

What it checks:
- Python syntax and import errors in all library files
- Presence of `spark: Any` declaration at module level
- All decorated functions return a value (not `None`)
- No circular dependencies in the table graph
- All `spark.table()` references resolve to either a decorated function or an existing catalog table
- Schema compatibility between upstream outputs and downstream inputs (when schemas can be inferred)

Example output:
```
Pipeline: orders_pipeline
Tables found: 8
  bronze.dim_brands (materialized_view, no dependencies)
  bronze.dim_locations (materialized_view, no dependencies)
  bronze.orders (materialized_view, no dependencies)
  bronze.orders_streaming (table, no dependencies)
  silver.orders_enriched (materialized_view, depends on: bronze.orders, bronze.dim_locations)
  silver.order_lifecycle (materialized_view, depends on: silver.orders_enriched)
  gold.hourly_metrics (materialized_view, depends on: silver.orders_enriched)
  gold.brand_summary (materialized_view, depends on: silver.orders_enriched, bronze.dim_brands)

Execution order:
  Phase 1 (parallel): bronze.dim_brands, bronze.dim_locations, bronze.orders, bronze.orders_streaming
  Phase 2: silver.orders_enriched
  Phase 3 (parallel): silver.order_lifecycle, gold.hourly_metrics, gold.brand_summary

Validation: PASSED
```

#### spark-pipelines run

Executes the pipeline.

```bash
# Run all tables
spark-pipelines run --spec pipeline.yml

# Run specific tables (and their dependencies)
spark-pipelines run --spec pipeline.yml --tables silver.orders_enriched

# Full refresh (ignore incremental state, recompute everything)
spark-pipelines run --spec pipeline.yml --full-refresh

# Development mode (verbose logging, fail-fast)
spark-pipelines run --spec pipeline.yml --development
```

Key behaviors:
- Tables with no dependencies execute in parallel when cluster resources allow
- If a table fails, downstream tables that depend on it are skipped
- `--full-refresh` clears streaming checkpoints and forces full recomputation of `@dp.table()` outputs
- `--development` enables detailed progress logging and stops the pipeline on the first error rather than continuing with unaffected branches

**JIRA**: [SPARK-52200](https://issues.apache.org/jira/browse/SPARK-52200) -- `spark-pipelines run` command implementation

#### spark-pipelines graph

Visualizes the dependency graph as ASCII art.

```bash
spark-pipelines graph --spec pipeline.yml
```

This is invaluable for pipeline reviews and documentation. The graph output shows decorator type (MV vs table), dependency edges, and execution phases.

#### spark-pipelines validate

Deep validation including schema inference and compatibility checks. This goes beyond `dry-run` by actually instantiating the Spark session and inspecting source data schemas.

```bash
spark-pipelines validate --spec pipeline.yml
```

Use this when you are changing source data schemas and want to verify that downstream transformations will still work.

### 2.6 Medallion Architecture with SDP

The medallion architecture (Bronze -> Silver -> Gold) maps naturally onto SDP's decorator model. Each layer is a set of decorated functions with explicit dependencies on the layer below.

#### Complete Production Example

This example is based on the actual `pipeline_sdp.py` used in the lakehouse-stack project (see `scripts/pipelines/pipeline_sdp.py`):

```python
"""Lakehouse Pipeline using Spark Declarative Pipelines (SDP).

Usage:
    spark-pipelines run --spec spark-pipeline.yml
    spark-pipelines dry-run --spec spark-pipeline.yml
"""

from typing import Any
from pyspark import pipelines as dp
from pyspark.sql import functions as f
from pyspark.sql.types import (
    StructType, StructField, StringType, IntegerType, DoubleType,
)

spark: Any

# ============================================================
# BRONZE LAYER: Raw Data Ingestion
# ============================================================

@dp.materialized_view(name="bronze.dim_categories")
def dim_categories():
    """Food categories dimension table."""
    return spark.read.parquet("/data/dimensions/categories.parquet")


@dp.materialized_view(name="bronze.dim_brands")
def dim_brands():
    """Ghost kitchen brands dimension table."""
    return spark.read.parquet("/data/dimensions/brands.parquet")


@dp.materialized_view(name="bronze.dim_locations")
def dim_locations():
    """Delivery locations dimension table."""
    return spark.read.parquet("/data/dimensions/locations.parquet")


@dp.materialized_view(name="bronze.orders")
def orders_batch():
    """Order lifecycle events from batch parquet source."""
    df = spark.read.parquet("/data/events/orders_90d.parquet")
    return df.withColumn(
        "event_timestamp",
        f.to_timestamp(f.regexp_replace("ts", "T", " "))
    )


# ============================================================
# SILVER LAYER: Cleaned and Enriched
# ============================================================

@dp.materialized_view(name="silver.orders_enriched")
def orders_enriched():
    """Orders with parsed JSON body, time features, and location join."""
    orders = spark.table("iceberg.bronze.orders")

    # Filter nulls
    cleaned = orders.filter(
        f.col("event_id").isNotNull()
        & f.col("order_id").isNotNull()
        & f.col("event_timestamp").isNotNull()
    )

    # Parse JSON body
    body_schema = StructType([
        StructField("brand_id", IntegerType()),
        StructField("total", DoubleType()),
        StructField("driver_id", StringType()),
    ])
    enriched = cleaned.withColumn("body_parsed", f.from_json("body", body_schema))
    enriched = enriched.select(
        "event_id", "event_type", "event_timestamp", "order_id",
        "location_id", "sequence", "body",
        f.col("body_parsed.brand_id").alias("brand_id"),
        f.col("body_parsed.total").alias("order_total"),
        f.col("body_parsed.driver_id").alias("driver_id"),
    )

    # Add time features
    enriched = enriched.withColumns({
        "event_hour": f.hour("event_timestamp"),
        "event_date": f.to_date("event_timestamp"),
        "is_weekend": f.when(
            f.dayofweek("event_timestamp").isin(1, 7), True
        ).otherwise(False),
    })

    # Join with locations
    locations = spark.table("iceberg.bronze.dim_locations").select(
        f.col("id").alias("location_id"),
        f.col("city").alias("city_name"),
    )

    return enriched.join(f.broadcast(locations), on="location_id", how="left")


@dp.materialized_view(name="silver.order_lifecycle")
def order_lifecycle():
    """Pivoted view: one row per completed order with duration metrics."""
    orders = spark.table("iceberg.silver.orders_enriched")

    lifecycle = orders.groupBy("order_id", "location_id", "city_name").pivot(
        "event_type",
        ["order_created", "kitchen_started", "kitchen_finished",
         "order_ready", "driver_arrived", "driver_picked_up", "delivered"]
    ).agg(f.min("event_timestamp").alias("ts"))

    lifecycle = lifecycle.select(
        "order_id", "location_id", "city_name",
        f.col("order_created").alias("created_at"),
        f.col("kitchen_started").alias("kitchen_started_at"),
        f.col("delivered").alias("delivered_at"),
    )

    lifecycle = lifecycle.withColumns({
        "total_duration_min": (
            f.unix_timestamp("delivered_at") - f.unix_timestamp("created_at")
        ) / 60,
    })

    return lifecycle.filter(f.col("delivered_at").isNotNull())


# ============================================================
# GOLD LAYER: Business Aggregations
# ============================================================

@dp.materialized_view(name="gold.hourly_metrics")
def hourly_metrics():
    """Hourly order metrics by location."""
    orders = spark.table("iceberg.silver.orders_enriched")

    return orders.filter(
        f.col("event_type") == "order_created"
    ).groupBy(
        "event_date", "event_hour", "location_id", "city_name",
    ).agg(
        f.count("order_id").alias("order_count"),
        f.sum("order_total").alias("total_revenue"),
        f.avg("order_total").alias("avg_order_value"),
        f.countDistinct("brand_id").alias("unique_brands"),
    )


@dp.materialized_view(name="gold.brand_summary")
def brand_summary():
    """Brand-level summary metrics."""
    orders = spark.table("iceberg.silver.orders_enriched")
    brands = spark.table("iceberg.bronze.dim_brands")

    brand_metrics = orders.filter(
        f.col("event_type") == "order_created"
    ).groupBy("brand_id").agg(
        f.count("order_id").alias("total_orders"),
        f.sum("order_total").alias("total_revenue"),
        f.avg("order_total").alias("avg_order_value"),
    )

    return brand_metrics.join(
        brands.select(f.col("id").alias("brand_id"), "name"),
        on="brand_id", how="left"
    )
```

#### Dependency Graph for This Pipeline

```
bronze.dim_categories (standalone)

bronze.dim_brands ──────────────────────────────────────────┐
bronze.dim_locations ────────────────────┐                  │
bronze.orders ──────────────────┐        │                  │
                                ├─► silver.orders_enriched ─┤
                                │        │                  │
                                │        │                  ├─► gold.brand_summary
                                │        │                  │
                                │        ├─► gold.hourly_metrics
                                │        │
                                └────────┴─► silver.order_lifecycle
```

#### No Explicit Writes

Notice that nowhere in the pipeline is there a `.write()` call. The framework:

1. Reads the pipeline spec to determine the output catalog (`iceberg`)
2. Combines the catalog with the decorator name to form the full table path (`iceberg.bronze.orders`)
3. For `@dp.materialized_view`: executes the function, takes the returned DataFrame, and writes it as an overwrite
4. For `@dp.table`: sets up a streaming query with the appropriate checkpoint location and output mode
5. Handles partitioning, table properties, and schema registration

This is the fundamental insight of declarative pipelines: the developer specifies **what** the output should contain, and the framework decides **how** to persist it.

### 2.7 The spark Variable and Runtime Injection

SDP pipelines do not create a `SparkSession` -- the framework provides one. The convention is to declare a module-level variable typed as `Any`:

```python
from typing import Any

spark: Any  # Framework injects this before calling any decorated function
```

Why `Any`? At import time (when the framework scans for decorators), no `SparkSession` exists yet. Using `Any` satisfies type checkers and linters without requiring a real instance. At execution time, the framework assigns a fully configured `SparkSession` to this variable before invoking any decorated function.

Common mistakes:
- Forgetting to declare `spark: Any` at module level (causes `NameError` at runtime)
- Declaring it inside a function (framework won't find it)
- Using `spark: SparkSession` (causes import-time type check failure because the variable is uninitialized)

**JIRA**: [SPARK-52082](https://issues.apache.org/jira/browse/SPARK-52082) -- SparkSession injection mechanism for SDP

### 2.8 Reusable Transformation Patterns

Helper functions that are NOT decorated can be freely used inside decorated functions. This is the recommended pattern for shared logic:

```python
def add_time_features(df, timestamp_col="event_timestamp"):
    """Standard time features for any DataFrame with a timestamp."""
    return df.withColumns({
        "event_hour": f.hour(timestamp_col),
        "event_date": f.to_date(timestamp_col),
        "day_of_week": f.dayofweek(timestamp_col),
        "is_weekend": f.dayofweek(timestamp_col).isin(1, 7),
    })


def filter_valid_records(df, required_cols):
    """Drop rows where any required column is null."""
    condition = f.col(required_cols[0]).isNotNull()
    for col_name in required_cols[1:]:
        condition = condition & f.col(col_name).isNotNull()
    return df.filter(condition)


@dp.materialized_view(name="silver.orders_clean")
def orders_clean():
    orders = spark.table("iceberg.bronze.orders")
    orders = filter_valid_records(orders, ["order_id", "event_timestamp"])
    orders = add_time_features(orders)
    return orders
```

Key points:
- Helper functions are plain Python -- no decorators, no framework awareness
- They take DataFrames as input and return DataFrames as output
- They can be imported from separate modules and shared across pipelines
- The dependency scanner only looks at decorated functions, so helpers do not affect the graph

---

## 3. RTM Deep Dive

This section provides a technical overview of Real-Time Mode. For the complete reference -- including streaming shuffle internals, checkpoint v2, operation support matrix, source/sink compatibility, task slot calculation, transformWithState behavioral differences, and migration procedures -- see the dedicated [Companion Guide: Spark 4.1 Real-Time Mode](../../../Documents/spark_content/real_time_mode/assets/companion/companion_guide_real_time_mode.md).

### 3.1 What RTM Is (and Is Not)

RTM is a new execution mode for Structured Streaming -- it is NOT a replacement for micro-batch. Micro-batch remains the default, recommended mode for throughput-oriented streaming workloads. RTM targets latency-sensitive use cases where sub-second processing is required:

- Payment authorization and fraud scoring
- Real-time feature engineering for ML serving
- Operational alerting with sub-second SLA
- Real-time personalization
- IoT event processing with tight latency budgets

**SPIP**: [SPARK-52330](https://issues.apache.org/jira/browse/SPARK-52330)
**Implementation**: [SPARK-53736](https://issues.apache.org/jira/browse/SPARK-53736)

### 3.2 trigger(realTime) vs trigger(processingTime)

The trigger is the only API change needed to switch between micro-batch and RTM:

```python
# Micro-batch (default): sequential plan/schedule/execute/commit per epoch
.trigger(processingTime="1 second")

# Real-Time Mode: concurrent long-lived tasks, continuous data flow
.trigger(realTime="5 minutes")
```

The parameter for `realTime` specifies the **checkpoint interval**, not the processing frequency. In RTM, data flows continuously -- the interval only controls how often state is persisted for fault tolerance. A 5-minute interval means that on failure, up to 5 minutes of work must be replayed from source.

| Trigger | Execution Model | Latency | Output Mode | State Recovery |
|---------|----------------|---------|-------------|----------------|
| `processingTime='N seconds'` | Micro-batch | Seconds to minutes | Append, Update, Complete | Per-batch commit |
| `realTime='N minutes'` | Concurrent stages | Sub-300ms (stateful), <10ms (stateless) | Update only | Periodic checkpoint |

### 3.3 Streaming Shuffle Internals

In micro-batch, the shuffle is a synchronization barrier: upstream tasks write partitioned output to disk, then downstream tasks read it. This collect-then-distribute model is designed for throughput.

RTM replaces this with an **in-memory streaming shuffle**:

```
Micro-batch:
  Stage 1 tasks (all complete) ──disk──> Stage 2 tasks (all complete) ──disk──> Stage 3

RTM:
  Stage 1 tasks ──memory buffer──> Stage 2 tasks ──memory buffer──> Stage 3 tasks
       │                                │                                │
       └────────── all run concurrently, data flows continuously ────────┘
```

Key characteristics:
- **Hash-based partitioning**: Same partitioner as micro-batch (`spark.sql.shuffle.partitions`), but records are routed to in-memory buffers instead of disk
- **Non-blocking**: Upstream tasks flush records to per-partition output buffers; downstream tasks consume from corresponding input buffers as data arrives
- **Implicit backpressure**: Bounded buffer capacity naturally throttles producers when consumers fall behind
- **No disk spill**: In the Spark 4.1 implementation, streaming shuffle buffers are purely in-memory. If memory is exhausted, the task fails and restarts from checkpoint
- **Memory sizing is critical**: Each shuffle stage creates `shuffle_partitions` long-lived tasks, each holding its own set of buffers

**JIRA**: [SPARK-53800](https://issues.apache.org/jira/browse/SPARK-53800) -- Streaming shuffle implementation

### 3.4 Concurrent Stage Scheduling

The core innovation of RTM. In micro-batch, stages execute sequentially -- Stage 1 must fully complete before Stage 2 begins. In RTM, all stages launch as long-lived concurrent tasks at query start:

```
Micro-batch Epoch N:
  [Source] ──(complete)──> [Shuffle] ──(complete)──> [Sink]
  |<──── plan/execute/commit for each stage ────────>|

RTM (continuous):
  [Source tasks]  ──stream──>  [Shuffle tasks]  ──stream──>  [Sink tasks]
  |<──── all running simultaneously, data flows as produced ──────────>|
```

This eliminates the inter-stage scheduling latency that sets the floor for micro-batch latency. In a 3-stage pipeline, micro-batch latency is at minimum `3 * (plan + schedule + execute)`. RTM latency is approximately `max(source_read_time, transform_time, sink_write_time)` since all stages process concurrently.

### 3.5 Latency Characteristics

| Pipeline Shape | Micro-Batch p99 | RTM p99 (stateless) | RTM p99 (stateful) |
|----------------|:---------------:|:-------------------:|:------------------:|
| Kafka -> filter -> Kafka | 500ms - 2s | 1-5ms | N/A |
| Kafka -> aggregate -> Kafka | 1-5s | N/A | 100-300ms |
| Kafka -> transformWithState -> Kafka | 1-5s | N/A | 100-300ms |
| Kafka -> UDF -> aggregate -> Kafka | 2-10s | N/A | 200-500ms |

These numbers assume properly sized clusters with sufficient task slots (see section 3.7).

### 3.6 Supported Sources and Sinks

RTM requires continuous, non-blocking connectors. File-based sources are incompatible because they inherently operate in batch cycles.

| Connector | Read | Write | Notes |
|-----------|:----:|:-----:|-------|
| Apache Kafka | Y | Y | Primary supported connector |
| AWS MSK | Y | Y | Via Kafka connector |
| Azure EventHub | Y | Y | Via Kafka protocol |
| AWS Kinesis (EFO) | Y | N | Enhanced Fan-Out mode only |
| Custom (forEachWriter) | - | Y | For custom sink logic |
| File sources (Parquet, CSV, JSON) | **N** | **N** | Batch-oriented, incompatible |
| Rate source | Y | - | Testing/benchmarking only |

**JIRA**: [SPARK-53850](https://issues.apache.org/jira/browse/SPARK-53850) -- RTM source/sink compatibility matrix

### 3.7 Task Slot Sizing

Task slot sizing is the single most important capacity planning step for RTM. Because all stages run concurrently as long-lived tasks, the cluster must have enough task slots to run every task simultaneously.

**Formula:**
```
Required task slots = source_partitions + (num_shuffle_stages x shuffle_partitions)
```

**Examples:**

| Pipeline | Source Partitions | Shuffles | shuffle.partitions | Required Slots |
|----------|:-----------------:|:--------:|:-----------------:|:--------------:|
| Kafka -> filter -> Kafka | 8 | 0 | - | 8 |
| Kafka -> agg -> Kafka | 8 | 1 | 20 | 28 |
| Kafka -> agg -> agg -> Kafka | 8 | 2 | 20 | 48 |
| Kafka -> agg -> Kafka (default) | 8 | 1 | 200 | **208** |

The default `spark.sql.shuffle.partitions=200` is designed for batch workloads. For RTM, set it to 10-50 to keep task slot requirements manageable.

### 3.8 Checkpoint v2

Checkpoint v2 enables seamless switching between micro-batch and RTM using the **same checkpoint location**. This is critical for adoption because it means:

- You can start with micro-batch and switch to RTM without data loss
- If RTM does not meet expectations, you can switch back to micro-batch
- No checkpoint migration or conversion is needed

Migration is literally: stop query, change trigger, restart.

```python
# Step 1: Running in micro-batch
query = df.writeStream.trigger(processingTime="1 second") \
    .option("checkpointLocation", "/checkpoints/my-query").start()

# Step 2: Stop the query
query.stop()

# Step 3: Switch to RTM (same checkpoint!)
query = df.writeStream.trigger(realTime="5 minutes") \
    .option("checkpointLocation", "/checkpoints/my-query").start()
```

### 3.9 Operation Support Matrix (Summary)

Not all Structured Streaming operations work in RTM. The key restrictions:

| Operation | RTM Support | Alternative |
|-----------|:-----------:|-------------|
| select / filter / project | Y | - |
| Aggregations (sum, count, avg, etc.) | Y | - |
| Tumbling windows | Y | - |
| Sliding windows | Y | - |
| Session windows | **N** | Stay on micro-batch |
| Stream-stream joins | **N** | Stay on micro-batch |
| dropDuplicates | Y | - |
| transformWithState | Y | Per-row semantics (see RTM companion guide) |
| mapGroupsWithState | **N** | Use transformWithState |
| forEachBatch | **N** | Use forEach |
| mapPartitions | **N** | Causes blocking |
| Append output mode | **N** | Use Update mode |
| Complete output mode | **N** | Use Update mode |
| Update output mode | Y | Only supported mode |

**JIRA**: [SPARK-53900](https://issues.apache.org/jira/browse/SPARK-53900) -- RTM operation support matrix

### 3.10 New Latency Metrics

RTM adds three latency metrics to `StreamingQueryProgress`:

- **Processing Latency**: Time between read from upstream and write to downstream (per-task)
- **Source Queuing Latency**: Time from when the message was written to Kafka until Spark reads it
- **End-to-End Latency**: Complete source-to-sink duration

```python
# Access via query progress
progress = query.lastProgress
# JSON includes latency breakdown
```

These metrics are essential for monitoring RTM pipelines. A growing end-to-end latency indicates backpressure -- the cluster is not keeping up with the input rate.

---

## 4. SDP + Streaming: The Convergence

### 4.1 @dp.table() for Streaming Tables

The `@dp.table()` decorator is SDP's entry point for streaming. A function decorated with `@dp.table()` returns a streaming DataFrame, and the framework handles the streaming query lifecycle:

```python
@dp.table(name="bronze.orders_streaming")
def orders_streaming():
    """Order events from Kafka, ingested continuously."""
    event_schema = StructType([
        StructField("event_id", StringType()),
        StructField("event_type", StringType()),
        StructField("ts", StringType()),
        StructField("order_id", StringType()),
        StructField("location_id", IntegerType()),
        StructField("body", StringType()),
    ])

    kafka_df = (
        spark.readStream
        .format("kafka")
        .option("kafka.bootstrap.servers", "broker:9092")
        .option("subscribe", "orders")
        .option("startingOffsets", "latest")
        .load()
    )

    parsed = kafka_df.select(
        f.from_json(f.col("value").cast("string"), event_schema).alias("event"),
    ).select("event.*")

    return parsed.withColumn(
        "event_timestamp",
        f.to_timestamp(f.regexp_replace("ts", "T", " "))
    )
```

What the framework does with this:
1. Detects that the returned DataFrame is streaming (has `isStreaming = True`)
2. Sets up a streaming query with the checkpoint location from `pipeline.yml`'s `storage` field
3. Manages the streaming query lifecycle (start, monitor, stop on pipeline shutdown)
4. Registers the output table for downstream dependency resolution

### 4.2 Mixed Batch + Streaming in One Pipeline

The most powerful pattern in SDP is combining batch dimension tables with streaming fact tables in a single pipeline definition:

```python
# Batch dimension -- recomputed each run
@dp.materialized_view(name="bronze.dim_products")
def dim_products():
    return spark.read.parquet("/data/dimensions/products.parquet")

# Streaming fact table -- continuously ingested
@dp.table(name="bronze.orders_stream")
def orders_stream():
    return (
        spark.readStream
        .format("kafka")
        .option("kafka.bootstrap.servers", "broker:9092")
        .option("subscribe", "orders")
        .load()
        .select(f.from_json(f.col("value").cast("string"), schema).alias("data"))
        .select("data.*")
    )

# Stream-batch join -- streaming orders enriched with batch products
@dp.table(name="silver.orders_enriched_stream")
def orders_enriched_stream():
    """Join streaming orders with static product dimension."""
    orders = spark.table("iceberg.bronze.orders_stream")       # Streaming
    products = spark.table("iceberg.bronze.dim_products")      # Batch (broadcast)
    return orders.join(f.broadcast(products), "product_id")

# Streaming aggregation with watermark
@dp.table(name="gold.hourly_revenue_stream")
def hourly_revenue_stream():
    """Real-time hourly revenue with late data handling."""
    return (
        spark.table("iceberg.silver.orders_enriched_stream")
        .withWatermark("event_timestamp", "1 hour")
        .groupBy(f.window("event_timestamp", "1 hour"))
        .agg(
            f.sum("amount").alias("total_revenue"),
            f.count("order_id").alias("order_count"),
        )
    )
```

The framework handles the coordination:
- `dim_products` (batch) is materialized first
- `orders_stream` (streaming) starts its streaming query
- `orders_enriched_stream` (streaming) starts after `dim_products` is ready, joining the stream against the static dimension
- `hourly_revenue_stream` (streaming) starts after `orders_enriched_stream`

This pattern is often called the "Lambda architecture without the Lambda" -- batch and streaming coexist in one pipeline definition, with batch tables serving as dimension lookups for streaming fact tables.

### 4.3 How RTM Trigger Works Within SDP

When using SDP with streaming tables, the trigger mode is configured in `pipeline.yml`, not in the Python code:

```yaml
# pipeline.yml
name: orders_realtime
libraries:
  - file: pipeline.py
catalog: iceberg
storage: /tmp/checkpoints
configuration:
  # RTM trigger: continuous processing with 5-minute checkpoint
  spark.sql.streaming.trigger.realTime: "5 minutes"
  # Reduce shuffle partitions for RTM
  spark.sql.shuffle.partitions: "20"
```

When this configuration is present, all `@dp.table()` streaming queries in the pipeline use RTM instead of micro-batch. The individual functions do not need to specify the trigger -- the framework applies it uniformly.

If you need different trigger modes for different tables (e.g., one table in RTM and another in micro-batch), you would split them into separate pipeline specs.

### 4.4 Streaming Pipeline with Watermarks and Late Data

Watermarks tell the streaming engine how late data can arrive before it is considered too late to include in aggregation results:

```python
@dp.table(name="silver.orders_with_watermark")
def orders_with_watermark():
    """Apply 2-hour watermark for late-arriving events."""
    return (
        spark.table("iceberg.bronze.orders_stream")
        .withWatermark("event_timestamp", "2 hours")
    )

@dp.table(name="gold.windowed_metrics")
def windowed_metrics():
    """Tumbling window aggregation with watermark propagation."""
    return (
        spark.table("iceberg.silver.orders_with_watermark")
        .groupBy(
            f.window("event_timestamp", "15 minutes").alias("time_window"),
            "location_id",
        )
        .agg(
            f.count("order_id").alias("order_count"),
            f.sum("order_total").alias("revenue"),
        )
        .select(
            f.col("time_window.start").alias("window_start"),
            f.col("time_window.end").alias("window_end"),
            "location_id",
            "order_count",
            "revenue",
        )
    )
```

The watermark propagates through the dependency chain: `orders_with_watermark` applies it, and `windowed_metrics` inherits it. Late events arriving after the watermark threshold are dropped from aggregations to prevent unbounded state growth.

---

## 5. Running SDP from Airflow

### 5.1 The Current State: No Dedicated SDP Operator

As of Airflow 3.x (including the latest 3.1.6 release), there is **no dedicated SDP operator**. The Airflow `apache-airflow-providers-apache-spark` package provides `SparkSubmitOperator`, which can run any `spark-submit` command -- and since `spark-pipelines` wraps `spark-submit`, this is the integration path.

The absence of a dedicated operator means:
- No built-in SDP status tracking in Airflow's UI
- No automatic retry of individual table failures (the whole pipeline is one Airflow task)
- No SDP-aware logging integration
- No SDP graph visualization in Airflow

A dedicated SDP operator is being discussed but is not on any published roadmap.

### 5.2 SparkSubmitOperator Wrapping spark-pipelines

The most common pattern is to invoke the SDP pipeline script via `SparkSubmitOperator`:

```python
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator

run_sdp_pipeline = SparkSubmitOperator(
    task_id="run_sdp_pipeline",
    application="/scripts/pipelines/pipeline_sdp.py",
    conn_id="spark_41",
    conf={
        "spark.sql.catalog.iceberg": "org.apache.iceberg.spark.SparkCatalog",
        "spark.sql.catalog.iceberg.type": "jdbc",
        "spark.sql.catalog.iceberg.uri": "jdbc:postgresql://postgres:5432/iceberg_catalog",
        "spark.sql.catalog.iceberg.jdbc.user": "{{ var.value.POSTGRES_USER }}",
        "spark.sql.catalog.iceberg.jdbc.password": "{{ var.value.POSTGRES_PASSWORD }}",
        "spark.sql.catalog.iceberg.warehouse": "s3a://lakehouse/warehouse",
        "spark.hadoop.fs.s3a.endpoint": "http://seaweedfs:8333",
    },
    name="lakehouse-sdp-pipeline",
    verbose=True,
    jars="/opt/spark/jars-extra/iceberg-spark-runtime.jar,"
         "/opt/spark/jars-extra/aws-bundle.jar,"
         "/opt/spark/jars-extra/postgresql.jar",
)
```

Note: when using `SparkSubmitOperator`, the `application` parameter points to the Python pipeline file, not the YAML spec. The Spark configuration is passed via `conf`. Alternatively, you can use `BashOperator` to invoke the `spark-pipelines` CLI directly:

```python
from airflow.operators.bash import BashOperator

run_sdp_cli = BashOperator(
    task_id="run_sdp_pipeline",
    bash_command=(
        "spark-pipelines run --spec /scripts/pipelines/spark-pipeline.yml "
        "2>&1 | tee /var/log/sdp-pipeline.log"
    ),
)
```

### 5.3 DAG Pattern: Preflight -> Run -> Verify -> Maintain

The recommended DAG structure for SDP pipelines follows a four-phase pattern. This is the actual pattern used in `dags/sdp_pipeline.py` in the lakehouse-stack project:

```python
from __future__ import annotations

import os
from datetime import datetime, timedelta

from airflow.sdk import DAG, task
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator

SPARK_CONN_ID = "spark_41"
PIPELINE_SCRIPT = "/scripts/pipelines/pipeline_sdp.py"

EXPECTED_TABLES = [
    "iceberg.bronze.orders",
    "iceberg.bronze.dim_locations",
    "iceberg.silver.orders_enriched",
    "iceberg.gold.hourly_metrics",
]

default_args = {
    "owner": "lakehouse",
    "depends_on_past": False,
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
}

with DAG(
    dag_id="lakehouse_sdp_pipeline",
    default_args=default_args,
    description="Run SDP pipeline for medallion architecture",
    schedule="@daily",
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=["lakehouse", "sdp", "spark-4.1", "medallion"],
) as dag:

    # Phase 1: Preflight -- verify infrastructure
    @task
    def preflight_check():
        """Verify Spark cluster and data sources are accessible."""
        import socket

        for service, port in [("spark-master-41", 7078), ("postgres", 5432), ("seaweedfs", 8333)]:
            sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            sock.settimeout(5)
            result = sock.connect_ex(("localhost", port))
            sock.close()
            if result != 0:
                raise RuntimeError(f"{service} unreachable on port {port}")
        return {"status": "all services healthy"}

    # Phase 2: Run SDP pipeline
    run_sdp_pipeline = SparkSubmitOperator(
        task_id="run_sdp_pipeline",
        application=PIPELINE_SCRIPT,
        conn_id=SPARK_CONN_ID,
        name="lakehouse-sdp-pipeline",
        verbose=True,
    )

    # Phase 3: Verify output tables
    @task
    def verify_tables():
        """Check that SDP output tables exist and have data."""
        from pyspark.sql import SparkSession

        spark = SparkSession.builder.master("local[1]").appName("sdp-verify").getOrCreate()

        for table in EXPECTED_TABLES:
            count = spark.sql(f"SELECT COUNT(*) FROM {table}").collect()[0][0]
            if count == 0:
                raise RuntimeError(f"Table {table} is empty after pipeline run")

        spark.stop()

    # Phase 4: Iceberg maintenance
    @task
    def iceberg_maintenance():
        """Compact small files and expire old snapshots."""
        from pyspark.sql import SparkSession

        spark = SparkSession.builder.master("local[1]").appName("sdp-maintain").getOrCreate()

        for table in EXPECTED_TABLES:
            try:
                spark.sql(f"CALL iceberg.system.expire_snapshots(table => '{table}', retain_last => 5)")
            except Exception:
                pass  # Non-critical

        spark.stop()

    # DAG flow
    preflight = preflight_check()
    verify = verify_tables()
    maintain = iceberg_maintenance()

    preflight >> run_sdp_pipeline >> verify >> maintain
```

#### Phase Breakdown

| Phase | Purpose | Failure Impact |
|-------|---------|---------------|
| **Preflight** | Verify Spark cluster, PostgreSQL, and object storage are reachable | Blocks pipeline run, surfaces infrastructure issues early |
| **Run SDP** | Execute the declarative pipeline via SparkSubmitOperator | Core pipeline failure; Airflow handles retries per `default_args` |
| **Verify** | Confirm output tables exist and are non-empty | Catches silent data loss (e.g., source data missing) |
| **Maintain** | Run Iceberg maintenance (compaction, snapshot expiry) | Non-critical; pipeline data is already written |

### 5.4 PysparkOperator for Quick Checks

Airflow's `apache-airflow-providers-apache-spark` v5.5.0 introduced `PysparkOperator`, which runs PySpark code directly as an Airflow task without requiring a separate `spark-submit` invocation. This is useful for lightweight checks but is NOT suitable for running full SDP pipelines (which need the `spark-pipelines` framework):

```python
from airflow.providers.apache.spark.operators.pyspark import PysparkOperator

pyspark_check = PysparkOperator(
    task_id="pyspark_row_count_check",
    pyspark_callable=lambda spark: (
        spark.sql("SELECT COUNT(*) as cnt FROM iceberg.gold.hourly_metrics")
        .show()
    ),
    spark_config={
        "spark.sql.catalog.iceberg": "org.apache.iceberg.spark.SparkCatalog",
    },
)
```

Appropriate uses for `PysparkOperator`:
- Post-pipeline row count checks
- Simple data quality assertions
- Metadata queries (Iceberg snapshot info, table properties)
- Ad-hoc data exploration in DAGs

NOT appropriate for:
- Running SDP pipelines (use `SparkSubmitOperator`)
- Heavy transformations (better as spark-submit jobs)
- Anything requiring the `spark-pipelines` CLI

### 5.5 Streaming SDP in Airflow

For SDP pipelines containing `@dp.table()` (streaming) decorators, the Airflow integration is different. Streaming queries are long-running by nature, so the standard "run and complete" DAG pattern does not apply. Options:

**Option A: Separate batch and streaming pipelines**

```yaml
# batch-pipeline.yml -- scheduled daily via Airflow
name: orders_batch
libraries:
  - file: pipeline_batch.py   # Only @dp.materialized_view decorators
catalog: iceberg

# streaming-pipeline.yml -- run as a long-lived service, NOT via Airflow
name: orders_streaming
libraries:
  - file: pipeline_streaming.py  # Only @dp.table decorators
catalog: iceberg
storage: /tmp/checkpoints
```

**Option B: Airflow triggers the streaming pipeline with a timeout**

```python
run_streaming_sdp = BashOperator(
    task_id="run_streaming_refresh",
    bash_command=(
        "timeout 3600 spark-pipelines run --spec streaming-pipeline.yml || "
        "[ $? -eq 124 ] && echo 'Timeout reached (expected for streaming)' && exit 0"
    ),
    execution_timeout=timedelta(hours=1, minutes=5),
)
```

This pattern runs the streaming pipeline for a fixed window (1 hour), then lets it time out. The next DAG run picks up from the checkpoint. It is a pragmatic compromise but loses the "always running" benefit of streaming.

**Option C: Airflow manages the streaming pipeline lifecycle**

Use Airflow's `TriggerDagRunOperator` to start a streaming pipeline, and a sensor to monitor it. The streaming pipeline runs indefinitely in a separate process; Airflow only manages start/stop/monitor lifecycle events. This is the most sophisticated pattern and requires custom operators.

---

## 6. SDP on Kubernetes

### 6.1 How spark-pipelines Works with K8s

Because `spark-pipelines` wraps `spark-submit`, it inherits Spark's native Kubernetes support. No special SDP-specific Kubernetes integration is needed. The sequence is:

```
spark-pipelines run --spec pipeline.yml
    └──> spark-submit --master k8s://https://k8s-api:443 ...
        └──> Kubernetes creates driver pod
            └──> Driver creates executor pods
                └──> SDP framework executes pipeline
```

#### Submitting to Kubernetes

```bash
spark-pipelines run --spec pipeline.yml \
    --master k8s://https://<k8s-api-server>:443 \
    --deploy-mode cluster \
    --conf spark.kubernetes.container.image=apache/spark:4.1.0-python3 \
    --conf spark.kubernetes.namespace=spark-jobs \
    --conf spark.kubernetes.driver.request.cores=2 \
    --conf spark.kubernetes.executor.request.cores=2 \
    --conf spark.executor.instances=4
```

Alternatively, configure these in `pipeline.yml`:

```yaml
name: orders_pipeline
libraries:
  - file: pipeline.py
catalog: iceberg
storage: s3a://lakehouse/checkpoints/orders
configuration:
  spark.master: "k8s://https://k8s-api:443"
  spark.submit.deployMode: "cluster"
  spark.kubernetes.container.image: "apache/spark:4.1.0-python3"
  spark.kubernetes.namespace: "spark-jobs"
  spark.kubernetes.driver.request.cores: "2"
  spark.kubernetes.executor.request.cores: "2"
  spark.executor.instances: "4"
  spark.kubernetes.file.upload.path: "s3a://lakehouse/spark-uploads/"
```

### 6.2 Configuration Considerations

| Concern | Configuration | Notes |
|---------|--------------|-------|
| **Image** | `spark.kubernetes.container.image` | Must contain Spark 4.1+ with your pipeline files baked in or mounted |
| **Pipeline files** | Mount via ConfigMap, PVC, or bake into image | ConfigMap has a 1MB limit; PVC or image bake recommended for larger pipelines |
| **JARs** | Bake into image at `/opt/spark/jars/` | Iceberg, AWS SDK, Kafka JARs must be present |
| **Secrets** | `spark.kubernetes.driver.secretKeyRef.*` | Database passwords, S3 credentials via K8s secrets |
| **Storage** | S3/GCS/ADLS for checkpoints and warehouse | Local paths do not work across pods |
| **Service account** | `spark.kubernetes.authenticate.driver.serviceAccountName` | Needs pod create/delete permissions |

#### Dockerfile for SDP on K8s

```dockerfile
FROM apache/spark:4.1.0-scala2.13-java21-python3-ubuntu

# Copy pipeline files
COPY pipeline.py /opt/spark/work-dir/
COPY pipeline.yml /opt/spark/work-dir/

# Copy required JARs
COPY jars/iceberg-spark-runtime-4.0_2.13-1.10.0.jar /opt/spark/jars/
COPY jars/aws-bundle-2.24.6.jar /opt/spark/jars/
COPY jars/postgresql-42.7.3.jar /opt/spark/jars/

WORKDIR /opt/spark/work-dir
```

### 6.3 RTM on Kubernetes

Running RTM within SDP on Kubernetes requires careful attention to task slot sizing. In K8s, each executor pod has a fixed number of cores (and thus task slots). The total available slots across all executor pods must meet or exceed the RTM task slot formula:

```
Required pods = ceil(required_task_slots / cores_per_executor)
```

For a pipeline with 8 Kafka partitions and 1 shuffle stage with 20 partitions:
```
Required slots = 8 + (1 x 20) = 28
If each executor has 4 cores: ceil(28 / 4) = 7 executor pods
```

Configure accordingly:
```yaml
configuration:
  spark.executor.instances: "7"
  spark.kubernetes.executor.request.cores: "4"
  spark.sql.shuffle.partitions: "20"
  spark.sql.streaming.trigger.realTime: "5 minutes"
```

---

## 7. Production Considerations

### 7.1 Monitoring and Alerting

#### Metrics to Track

| Metric | Description | Alert Threshold | Collection Method |
|--------|-------------|-----------------|-------------------|
| **Pipeline runtime** | Total wall-clock time for full pipeline run | > 2x baseline | Airflow task duration |
| **Records per table** | Row count in each output table | < 50% expected | Post-pipeline verification task |
| **Table freshness** | Time since last write to each table | > 2x schedule interval | Iceberg snapshot timestamps |
| **Streaming lag** | Consumer lag for `@dp.table()` sources | Growing over time | Kafka consumer group lag |
| **RTM end-to-end latency** | Source-to-sink latency for RTM queries | > 500ms p99 | `StreamingQueryProgress` |
| **Checkpoint duration** | Time to complete each checkpoint | > 50% of checkpoint interval | Spark metrics |
| **Error rate** | Failed pipeline runs / total runs | > 5% | Airflow DAG stats |

#### Spark UI Integration

The Spark UI (port 4040 on the driver) provides:
- **SQL tab**: Execution plans for each SDP table's transformation
- **Streaming tab**: Progress metrics for `@dp.table()` queries, including RTM latency metrics
- **Stages tab**: Task-level timing, shuffle read/write sizes, GC time

For production, persist Spark event logs and point a Spark History Server at them:

```yaml
configuration:
  spark.eventLog.enabled: "true"
  spark.eventLog.dir: "s3a://lakehouse/spark-events/"
  spark.history.fs.logDirectory: "s3a://lakehouse/spark-events/"
```

### 7.2 Checkpointing

#### Batch Pipelines (@dp.materialized_view only)

No checkpoint state is needed. Each run is a full recomputation. The `storage` field in `pipeline.yml` can be omitted.

#### Streaming Pipelines (@dp.table)

Checkpoint state is critical for exactly-once processing:

```yaml
# pipeline.yml
storage: s3a://lakehouse/checkpoints/orders-pipeline
```

The framework creates a subdirectory per streaming table under this path. Each subdirectory contains:
- **Offset log**: Which source offsets have been processed
- **Commit log**: Which batches have been committed
- **State store**: Aggregation state, deduplication state, transformWithState state

**CRITICAL**: Never delete or modify checkpoint directories while a streaming query is running. Checkpoint corruption causes data loss or duplication.

#### Checkpoint Management

```bash
# View checkpoint contents (for debugging)
ls -la /tmp/checkpoints/orders-pipeline/bronze.orders_streaming/

# Force full refresh (deletes checkpoints for all tables)
spark-pipelines run --spec pipeline.yml --full-refresh

# Caution: full-refresh reprocesses ALL historical data from source
```

### 7.3 Schema Evolution in SDP

SDP relies on Iceberg (or Delta) for schema evolution. When the source data schema changes, the pipeline behavior depends on the decorator type:

#### @dp.materialized_view (Batch)

- **Add column to source**: Next pipeline run picks it up automatically (if not using explicit schema). The output table's schema is updated.
- **Remove column from source**: Pipeline fails if the removed column is referenced in transformations. Fix the pipeline code.
- **Change column type**: Depends on Iceberg's type promotion rules. Safe promotions (int -> long) happen automatically. Unsafe changes (string -> int) cause runtime errors.

#### @dp.table (Streaming)

Schema evolution in streaming is more constrained:
- **Add column**: Works if using `from_json` with a schema that includes the new column. Requires checkpoint restart if the schema change affects state.
- **Remove column**: Requires clearing the checkpoint and restarting (equivalent to `--full-refresh`).
- **Change column type**: Generally requires a new checkpoint. Use `try_cast()` or `from_json` with nullable fields for resilience.

Best practice: use explicit schemas (`StructType`) for all external data sources (Kafka, files) and keep the schema definition version-controlled alongside the pipeline code.

```python
# Explicit schema -- controlled evolution
ORDER_EVENT_SCHEMA_V2 = StructType([
    StructField("event_id", StringType()),
    StructField("event_type", StringType()),
    StructField("ts", StringType()),
    StructField("order_id", StringType()),
    StructField("location_id", IntegerType()),
    StructField("body", StringType()),
    StructField("source_app", StringType()),  # New in V2 -- nullable
])

@dp.table(name="bronze.orders_stream")
def orders_stream():
    return (
        spark.readStream.format("kafka")
        .option("kafka.bootstrap.servers", "broker:9092")
        .option("subscribe", "orders")
        .load()
        .select(f.from_json(f.col("value").cast("string"), ORDER_EVENT_SCHEMA_V2).alias("event"))
        .select("event.*")
    )
```

### 7.4 Testing Strategies

#### Level 1: Unit Tests (No Spark)

Test helper functions (non-decorated) with mock DataFrames or plain Python:

```python
def test_add_time_features():
    """Test time feature extraction logic."""
    from my_pipeline import add_time_features

    # Use a real local SparkSession for testing
    spark = SparkSession.builder.master("local[2]").getOrCreate()

    df = spark.createDataFrame(
        [("2024-01-15 10:30:00",)], ["event_timestamp"]
    ).withColumn("event_timestamp", f.to_timestamp("event_timestamp"))

    result = add_time_features(df)
    row = result.first()

    assert row["event_hour"] == 10
    assert str(row["event_date"]) == "2024-01-15"
```

#### Level 2: Integration Tests (Mock spark.table)

Test decorated functions with mocked upstream tables:

```python
from unittest.mock import patch, MagicMock
import my_pipeline

def test_orders_enriched():
    """Test that orders_enriched joins correctly."""
    mock_spark = MagicMock()

    # Create real test DataFrames
    test_spark = SparkSession.builder.master("local[2]").getOrCreate()

    mock_orders = test_spark.createDataFrame(
        [("e1", "order_created", 1, "ORD001")],
        ["event_id", "event_type", "location_id", "order_id"]
    )
    mock_locations = test_spark.createDataFrame(
        [(1, "San Francisco")], ["id", "city"]
    )

    mock_spark.table.side_effect = lambda name: {
        "iceberg.bronze.orders": mock_orders,
        "iceberg.bronze.dim_locations": mock_locations,
    }[name]

    with patch.object(my_pipeline, 'spark', mock_spark):
        result = my_pipeline.orders_enriched()

    assert "city_name" in result.columns
```

#### Level 3: Pipeline Validation (dry-run)

Use `spark-pipelines dry-run` as a CI step:

```bash
# In CI pipeline
spark-pipelines dry-run --spec pipeline.yml
if [ $? -ne 0 ]; then
    echo "Pipeline validation failed"
    exit 1
fi
```

#### Level 4: End-to-End Tests

Run the full pipeline against test data and verify output:

```bash
# Generate test data
./lakehouse testdata generate --days 7

# Run pipeline
spark-pipelines run --spec pipeline.yml

# Verify
spark-sql -e "SELECT COUNT(*) FROM iceberg.gold.hourly_metrics"
```

### 7.5 Error Handling and Recovery

#### Batch Pipelines

- **Table failure**: SDP skips downstream tables that depend on the failed table, but continues executing unaffected branches.
- **Recovery**: Fix the issue and re-run. `@dp.materialized_view` tables are idempotent (full recomputation), so re-runs are safe.
- **Partial re-run**: Use `--tables` flag to re-run only the failed table and its downstream dependencies.

```bash
# Re-run only the failed table and everything downstream
spark-pipelines run --spec pipeline.yml --tables silver.orders_enriched
```

#### Streaming Pipelines

- **Task failure**: RTM restarts the task from the last checkpoint. Data since the checkpoint is replayed.
- **Driver failure**: The entire streaming query restarts from the last checkpoint.
- **Corrupt checkpoint**: Use `--full-refresh` to clear checkpoints and reprocess from the beginning. This may cause duplicates in downstream consumers if exactly-once is required.
- **Source unavailable (Kafka down)**: The streaming query retries indefinitely. Configure `kafka.consumer.max.poll.interval.ms` and Airflow's retry logic to handle extended outages.

### 7.6 Performance Tuning

#### Batch Pipeline Tuning

```yaml
# pipeline.yml
configuration:
  # Adaptive Query Execution (strongly recommended)
  spark.sql.adaptive.enabled: "true"
  spark.sql.adaptive.coalescePartitions.enabled: "true"
  spark.sql.adaptive.skewJoin.enabled: "true"

  # Shuffle partitions (tune based on data size)
  # Rule of thumb: data_size_mb / 128, or 2-3x num_cores
  spark.sql.shuffle.partitions: "200"

  # Memory
  spark.executor.memory: "4g"
  spark.driver.memory: "2g"

  # Iceberg-specific
  spark.sql.iceberg.vectorization.enabled: "true"
```

#### RTM Pipeline Tuning

```yaml
configuration:
  # Reduce shuffle partitions (critical for RTM)
  spark.sql.shuffle.partitions: "20"

  # RTM checkpoint interval
  spark.sql.streaming.realTimeMode.minBatchDuration: "5000"

  # Arrow batch size for UDFs
  spark.sql.execution.arrow.maxRecordsPerBatch: "1"  # Lowest latency

  # Disable sort before repartition (conflicts with RTM)
  spark.sql.execution.sortBeforeRepartition: "false"
```

#### Broadcast Joins

For dimension tables under ~100MB, broadcast them to avoid shuffle:

```python
@dp.materialized_view(name="silver.orders_enriched")
def orders_enriched():
    orders = spark.table("iceberg.bronze.orders")        # Large (100M+ rows)
    locations = spark.table("iceberg.bronze.dim_locations")  # Small (< 1000 rows)
    return orders.join(f.broadcast(locations), "location_id", "left")
```

#### Filter Pushdown

Apply filters early to leverage Iceberg's metadata-based pruning:

```python
@dp.materialized_view(name="gold.recent_metrics")
def recent_metrics():
    return (
        spark.table("iceberg.silver.orders_enriched")
        .filter(f.col("event_date") >= f.date_sub(f.current_date(), 30))  # Pushed to Iceberg scan
        .groupBy("event_date")
        .agg(f.sum("order_total").alias("daily_revenue"))
    )
```

---

## 8. Comparison to Databricks DLT/Lakeflow Declarative Pipelines

This section provides an honest comparison for practitioners who know DLT. SDP is the open-source implementation of the same declarative pipeline model, but there are meaningful differences in scope, maturity, and operational capabilities.

### 8.1 What Is the Same

The core programming model is nearly identical because SDP was designed to bring DLT's declarative approach to OSS Spark:

| Concept | DLT/Lakeflow | SDP (OSS) | Notes |
|---------|:------------:|:---------:|-------|
| Decorator-based table definitions | `@dlt.table()` | `@dp.table()` | Same pattern, different module |
| Materialized views | `@dlt.view()` | `@dp.materialized_view()` | Same semantics |
| Streaming tables | `@dlt.table()` with `readStream` | `@dp.table()` with `readStream` | Same pattern |
| Automatic dependency resolution | `spark.table()` scanning | `spark.table()` scanning | Same mechanism |
| No explicit writes | Framework handles persistence | Framework handles persistence | Identical |
| DAG visualization | Pipeline graph in UI | `spark-pipelines graph` | CLI vs UI |
| Dry-run validation | Validation before run | `spark-pipelines dry-run` | Same concept |
| Mixed batch + streaming | Single pipeline definition | Single pipeline definition | Same capability |
| Checkpoint-based recovery | Automatic | Via `storage` in pipeline.yml | Same underlying mechanism |

#### Code Comparison

```python
# DLT/Lakeflow (Databricks)
import dlt
from pyspark.sql import functions as f

@dlt.table(comment="Cleaned orders")
def silver_orders():
    return (
        spark.table("LIVE.bronze_orders")
        .filter(f.col("id").isNotNull())
    )

# SDP (OSS Spark 4.1)
from pyspark import pipelines as dp
from pyspark.sql import functions as f

spark: Any

@dp.materialized_view(name="silver.orders", comment="Cleaned orders")
def silver_orders():
    return (
        spark.table("iceberg.silver.orders")
        .filter(f.col("id").isNotNull())
    )
```

Differences in the code:
- DLT uses `LIVE.` prefix for intra-pipeline references; SDP uses the full `catalog.database.table` path
- DLT injects `spark` implicitly; SDP requires explicit `spark: Any` declaration
- DLT's `@dlt.table()` handles both batch and streaming; SDP uses `@dp.materialized_view()` for batch and `@dp.table()` for streaming

### 8.2 What Is Different

| Capability | DLT/Lakeflow (Databricks) | SDP (OSS Spark 4.1) | Impact |
|------------|:-------------------------:|:--------------------:|--------|
| **Managed infrastructure** | Fully managed clusters, auto-scaling | You manage clusters (K8s, standalone, YARN) | Significant operational overhead for OSS |
| **Expectations (data quality)** | `@dlt.expect()`, `@dlt.expect_or_drop()`, `@dlt.expect_or_fail()` | Not available -- use manual `.filter()` + logging | Major gap: no built-in data quality assertions |
| **Event log** | Full pipeline event log with row-level lineage | Spark event logs + custom logging | Less granular lineage in OSS |
| **Pipeline UI** | Rich web UI showing graph, run history, metrics, expectations | CLI-only (`spark-pipelines graph`) + Spark UI | No dedicated pipeline management UI |
| **Auto Loader** | `cloudFiles` format for incremental file ingestion | Not available -- use `spark.readStream.format("cloudFiles")` on Databricks only, or manual file listing | Auto Loader is Databricks-proprietary |
| **Enhanced autoscaling** | Pipeline-aware scaling based on data volume | Standard Spark dynamic allocation | DLT scales more intelligently for pipeline workloads |
| **Change Data Capture** | `APPLY CHANGES INTO` for CDC processing | Manual CDC with SCD patterns | No built-in CDC support in SDP |
| **Unity Catalog integration** | Native lineage, permissions, tagging | Separate integration with UC OSS | Less seamless in OSS |
| **Flow definitions** | `@dlt.append_flow()` for multi-source tables | Not available | Must use union + manual pattern |
| **Materialized view refresh** | Incremental refresh (only process changed data) | Full recomputation on every run | Performance impact for large dimension tables |
| **Cost management** | DBU-based pricing with pipeline cost attribution | Compute infrastructure costs (you manage) | Harder to attribute costs per pipeline in OSS |

### 8.3 Expectations: The Biggest Gap

DLT's expectations system is a first-class data quality framework built into the pipeline definition:

```python
# DLT: Built-in data quality with three enforcement levels
@dlt.expect("valid_id", "id IS NOT NULL")                    # Warn but keep rows
@dlt.expect_or_drop("positive_amount", "amount > 0")         # Drop failing rows
@dlt.expect_or_fail("known_region", "region IN ('NA','EU')") # Fail pipeline
@dlt.table()
def silver_orders():
    return spark.table("LIVE.bronze_orders")
```

SDP has no equivalent. You must implement data quality checks manually:

```python
# SDP: Manual data quality (no built-in expectations)
import logging

@dp.materialized_view(name="silver.orders")
def silver_orders():
    df = spark.table("iceberg.bronze.orders")

    # Manual quality check (equivalent to expect_or_drop)
    df_valid = df.filter(
        f.col("id").isNotNull()
        & (f.col("amount") > 0)
        & f.col("region").isin("NA", "EU")
    )

    # Optional: log quality metrics (no built-in tracking)
    # total = df.count()
    # valid = df_valid.count()
    # logging.info(f"Data quality: {valid}/{total} rows passed ({valid/total*100:.1f}%)")

    return df_valid
```

For production SDP pipelines, consider integrating a standalone data quality library like Great Expectations or Soda Core alongside your pipeline.

### 8.4 The LIVE Prefix vs Full Table Names

In DLT, `LIVE.` is a virtual prefix that refers to tables within the same pipeline:

```python
# DLT
@dlt.table()
def silver_orders():
    return spark.table("LIVE.bronze_orders")  # Intra-pipeline reference
```

SDP does not have this concept. All references use the full `catalog.database.table` name:

```python
# SDP
@dp.materialized_view(name="silver.orders")
def silver_orders():
    return spark.table("iceberg.bronze.orders")  # Full three-part name
```

The implication: in DLT, renaming a table's output only requires changing the `@dlt.table(name=...)` parameter, and all `LIVE.` references automatically resolve. In SDP, renaming a table requires updating every `spark.table()` call that references it.

### 8.5 Migration Path: DLT to SDP

If you are moving from Databricks DLT to OSS SDP:

| DLT Concept | SDP Equivalent | Migration Notes |
|-------------|----------------|-----------------|
| `import dlt` | `from pyspark import pipelines as dp` | Module rename |
| `@dlt.table()` (batch) | `@dp.materialized_view()` | Rename decorator |
| `@dlt.table()` (streaming) | `@dp.table()` | Rename decorator |
| `@dlt.view()` | `@dp.materialized_view()` | Same semantics |
| `LIVE.table_name` | `catalog.database.table_name` | Replace virtual prefix with full path |
| `@dlt.expect*()` | Manual `.filter()` + logging | No built-in equivalent |
| `APPLY CHANGES INTO` | Manual CDC logic | No built-in CDC |
| `cloudFiles` | `readStream.format("parquet").option("path", ...)` | Auto Loader is proprietary |
| Pipeline settings JSON | `pipeline.yml` | Different format, same concepts |

### 8.6 Honest Assessment

**Choose SDP (OSS) when:**
- You are building a self-hosted lakehouse and want declarative pipeline definitions
- Your team already uses Spark and does not want vendor lock-in
- Data quality can be managed with external tools or manual filters
- You have the operational expertise to manage Spark clusters
- Cost control is paramount (no per-DBU charges)

**Stay on DLT/Lakeflow when:**
- You need built-in data quality expectations with tracking
- You want managed infrastructure with pipeline-aware autoscaling
- CDC (Change Data Capture) is a core requirement
- You need the pipeline management UI for operational visibility
- Your organization is already invested in the Databricks ecosystem
- Auto Loader is a critical part of your ingestion strategy

**The honest truth:** SDP is approximately DLT's programming model without DLT's operational tooling. The code is almost identical, but the experience of running pipelines in production is very different. DLT provides guardrails, monitoring, and managed infrastructure. SDP gives you the same declarative programming model but expects you to build the operational layer yourself -- with Airflow, Kubernetes, Prometheus, and your own alerting stack.

---

## 9. References

### Apache JIRA

| JIRA | Title | Status |
|------|-------|--------|
| [SPARK-51689](https://issues.apache.org/jira/browse/SPARK-51689) | Spark Declarative Pipelines SPIP | Resolved |
| [SPARK-51955](https://issues.apache.org/jira/browse/SPARK-51955) | `pyspark.pipelines` module implementation | Resolved |
| [SPARK-52080](https://issues.apache.org/jira/browse/SPARK-52080) | `@dp.materialized_view` decorator | Resolved |
| [SPARK-52081](https://issues.apache.org/jira/browse/SPARK-52081) | `@dp.table` decorator | Resolved |
| [SPARK-52082](https://issues.apache.org/jira/browse/SPARK-52082) | SparkSession injection for SDP | Resolved |
| [SPARK-52200](https://issues.apache.org/jira/browse/SPARK-52200) | `spark-pipelines run` command | Resolved |
| [SPARK-52330](https://issues.apache.org/jira/browse/SPARK-52330) | Real-Time Mode SPIP | Resolved |
| [SPARK-52500](https://issues.apache.org/jira/browse/SPARK-52500) | `spark.sql()` dependency detection for SDP | Open |
| [SPARK-53736](https://issues.apache.org/jira/browse/SPARK-53736) | RTM implementation | Resolved |
| [SPARK-53800](https://issues.apache.org/jira/browse/SPARK-53800) | Streaming shuffle implementation | Resolved |
| [SPARK-53850](https://issues.apache.org/jira/browse/SPARK-53850) | RTM source/sink compatibility | Resolved |
| [SPARK-53900](https://issues.apache.org/jira/browse/SPARK-53900) | RTM operation support matrix | Resolved |

### Documentation

| Resource | URL |
|----------|-----|
| Apache Spark 4.1 Declarative Pipelines | [https://spark.apache.org/docs/4.1.0/declarative-pipelines.html](https://spark.apache.org/docs/4.1.0/declarative-pipelines.html) |
| Spark Structured Streaming Programming Guide | [https://spark.apache.org/docs/4.1.0/structured-streaming-programming-guide.html](https://spark.apache.org/docs/4.1.0/structured-streaming-programming-guide.html) |
| Apache Iceberg Documentation | [https://iceberg.apache.org/docs/1.10.0/](https://iceberg.apache.org/docs/1.10.0/) |
| Airflow Spark Provider | [https://airflow.apache.org/docs/apache-airflow-providers-apache-spark/stable/index.html](https://airflow.apache.org/docs/apache-airflow-providers-apache-spark/stable/index.html) |
| Spark on Kubernetes | [https://spark.apache.org/docs/4.1.0/running-on-kubernetes.html](https://spark.apache.org/docs/4.1.0/running-on-kubernetes.html) |

### Lakehouse-Stack Project Files

| File | Purpose |
|------|---------|
| `scripts/pipelines/pipeline_sdp.py` | Production SDP pipeline implementation |
| `scripts/pipelines/spark-pipeline.yml` | SDP pipeline spec |
| `dags/sdp_pipeline.py` | Airflow DAG for SDP pipeline |
| `dags/lakehouse_medallion_pipeline.py` | Airflow DAG for imperative pipeline (comparison) |
| `dags/iceberg_maintenance.py` | Airflow DAG for Iceberg maintenance |
| `scripts/demos/overarchitected/03_full_overarchitected.py` | Demo combining VARIANT, CTE, Collation, and SDP-style logic |
| `.claude/skills/SDP.md` | Complete SDP reference for AI assistants |
| `docs/guides/pipelines.md` | Imperative vs declarative pipeline comparison |

### Related Companion Guides

| Guide | Topic |
|-------|-------|
| [Companion Guide: Real-Time Mode](../../../Documents/spark_content/real_time_mode/assets/companion/companion_guide_real_time_mode.md) | Complete RTM reference (architecture, migration, operations) |
| [Companion Guide: RTM Infrastructure](../../../Documents/spark_content/real_time_mode/assets/companion/companion_guide_rtm_infrastructure.md) | Infrastructure sizing and deployment for RTM |
| [Companion Guide: The Ultimate Guide to Apache Spark in 2026](../../../Documents/spark_content/companion_guide_spark_41.md) | Broad Spark 4.x overview including SDP and RTM sections |

---

*This guide was written for the OverArchitected show, Act 4: SDP + RTM. It is intended as an exhaustive technical reference for data engineers evaluating or adopting Spark Declarative Pipelines and Real-Time Mode for production lakehouse architectures.*
