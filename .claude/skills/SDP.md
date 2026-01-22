# Spark Declarative Pipelines (SDP) - Complete Reference

A comprehensive guide for working with Spark 4.1+ Declarative Pipelines, from first principles to production deployment.

---

## Table of Contents

1. [What is SDP?](#what-is-sdp)
2. [Prerequisites](#prerequisites)
3. [Core Concepts](#core-concepts)
4. [Schema Discovery](#schema-discovery)
5. [Your First Pipeline](#your-first-pipeline)
6. [The SDP API](#the-sdp-api)
7. [Pipeline Configuration](#pipeline-configuration)
8. [CLI Reference](#cli-reference)
9. [Patterns & Best Practices](#patterns--best-practices)
10. [Streaming Pipelines](#streaming-pipelines)
11. [Performance Tuning](#performance-tuning)
12. [Testing Strategies](#testing-strategies)
13. [Production Operations](#production-operations)
14. [Migration from Imperative](#migration-from-imperative)
15. [Troubleshooting](#troubleshooting)
16. [When NOT to Use SDP](#when-not-to-use-sdp)
17. [Quick Reference](#quick-reference)

---

## What is SDP?

### The Problem SDP Solves

Traditional PySpark pipelines require you to:
- Manually manage execution order
- Explicitly write data to tables
- Handle dependencies yourself
- Coordinate between batch and streaming

```python
# Traditional approach - lots of boilerplate
def bronze_orders(spark):
    df = spark.read.parquet("/data/orders.parquet")
    df.write.mode("overwrite").saveAsTable("bronze.orders")

def silver_orders(spark):
    df = spark.table("bronze.orders")  # Must run after bronze_orders!
    df = df.filter(col("id").isNotNull())
    df.write.mode("overwrite").saveAsTable("silver.orders")

# You manage execution order
bronze_orders(spark)
silver_orders(spark)
```

### The SDP Solution

SDP lets you declare **what** you want, not **how** to do it:

```python
# SDP approach - declare intent, framework handles execution
@dp.materialized_view(name="bronze.orders")
def bronze_orders():
    return spark.read.parquet("/data/orders.parquet")

@dp.materialized_view(name="silver.orders")
def silver_orders():
    return spark.table("iceberg.bronze.orders").filter(col("id").isNotNull())

# Framework figures out order and handles writes
```

### Key Benefits

| Benefit | Description |
|---------|-------------|
| **Automatic dependency resolution** | Framework detects dependencies from `spark.table()` calls |
| **No explicit writes** | Return DataFrames; framework handles persistence |
| **Unified batch/streaming** | Same patterns for both; switch with decorator change |
| **Built-in validation** | `dry-run` catches errors before execution |
| **Incremental by default** | Streaming tables maintain state automatically |

### Mental Model

Think of SDP like SQL views with superpowers:

```
SQL View:     CREATE VIEW silver.orders AS SELECT * FROM bronze.orders WHERE ...
SDP:          @dp.materialized_view(name="silver.orders")
              def silver_orders(): return spark.table("...").filter(...)
```

The decorator says "I want a table called X". The function body says "here's the data". The framework handles everything else.

---

## Prerequisites

### Required Knowledge

- **Python basics**: Functions, decorators, type hints
- **PySpark fundamentals**: DataFrames, transformations (filter, select, join, groupBy)
- **SQL concepts**: Tables, views, joins, aggregations

### Environment Requirements

- **Spark 4.1.0+** (SDP not available in earlier versions)
- **Python 3.10+**
- **spark-pipelines CLI** (bundled with Spark 4.1+)

### Verify Installation

```bash
# Check Spark version
spark-submit --version
# Should show 4.1.0 or higher

# Check CLI available
spark-pipelines --help
```

---

## Core Concepts

### 1. Decorators Define Tables

Every table in SDP is defined by a decorated function:

```python
from pyspark import pipelines as dp

@dp.materialized_view(name="my_database.my_table")
def my_table():
    return some_dataframe
```

### 2. spark is Injected

You don't create SparkSession—the framework provides it:

```python
from typing import Any

spark: Any  # Declare at module level; framework injects at runtime

@dp.materialized_view(name="bronze.data")
def data():
    return spark.read.parquet("/data/file.parquet")  # spark is available
```

**Why `Any` type?** The variable isn't assigned until runtime. Using `Any` satisfies type checkers without requiring a real SparkSession at import time.

### 3. Dependencies are Inferred

The framework builds a dependency graph by detecting `spark.table()` calls:

```python
@dp.materialized_view(name="silver.enriched")
def enriched():
    orders = spark.table("iceberg.bronze.orders")      # Dependency!
    products = spark.table("iceberg.bronze.products")  # Dependency!
    return orders.join(products, "product_id")
```

Dependency graph: `bronze.orders` → `silver.enriched` ← `bronze.products`

### 4. Two Decorator Types

| Decorator | Use Case | Behavior |
|-----------|----------|----------|
| `@dp.materialized_view()` | Batch data, dimension tables | Fully recomputed each run |
| `@dp.table()` | Streaming, incremental | Maintains state, appends new data |

### 5. The Pipeline Spec

A YAML file ties everything together:

```yaml
name: my_pipeline
libraries:
  - file: pipeline.py
catalog: iceberg
database: bronze
```

### 6. Critical: Table Naming Convention

> **This is the #1 source of confusion.** Decorator names and `spark.table()` calls use DIFFERENT formats.

| Context | Format | Example |
|---------|--------|---------|
| Decorator `name=` | `database.table` | `@dp.materialized_view(name="bronze.orders")` |
| `spark.table()` | `catalog.database.table` | `spark.table("iceberg.bronze.orders")` |

**Why the difference?**
- The **decorator** defines where to write. The catalog comes from `pipeline.yml`.
- The **`spark.table()`** reads data. It needs the full path so dependencies resolve correctly.

```python
# CORRECT
@dp.materialized_view(name="silver.orders")  # No catalog prefix!
def orders():
    return spark.table("iceberg.bronze.raw")  # Full path with catalog!

# WRONG - catalog in decorator
@dp.materialized_view(name="iceberg.silver.orders")  # NO!
def orders():
    return spark.table("bronze.raw")  # Missing catalog - dependency won't resolve!
```

---

## Schema Discovery

Before writing a pipeline, understand your source data:

### Explore Schema Interactively

```bash
# In a Docker-based lakehouse, use spark-shell inside the container:
docker exec -it spark-master-41 /opt/spark/bin/spark-shell

# Then in Scala:
val df = spark.read.parquet("/data/events/orders_90d.parquet")
df.printSchema()
df.show(5, false)

# Or use PySpark:
docker exec -it spark-master-41 /opt/spark/bin/pyspark
```

```python
# In PySpark shell or notebook (NOT in SDP pipeline code)
df = spark.read.parquet("/data/events/orders_90d.parquet")
df.printSchema()
df.show(5, truncate=False)

# Check for a specific column
df.select("ts").show(3)  # Does 'ts' exist? What format?
```

### Alternative: Check Existing Pipelines

If similar pipelines already exist, check their column references:

```bash
# Search for how others handled the same data
grep -n "orders_90d" scripts/*.py
grep -n "\.ts" scripts/pipeline_sdp.py
```

### Common Schema Patterns

When the actual column name differs from what you expect:

```python
# Source has 'ts' but you want 'event_timestamp'
@dp.materialized_view(name="bronze.orders")
def orders():
    return (
        spark.read.parquet("/data/events/orders.parquet")
        .withColumn("event_timestamp", f.to_timestamp("ts"))
    )
```

### ISO 8601 Timestamp Handling

Many data sources use ISO 8601 format with 'T' separator (`2024-01-15T10:30:00`). Spark's `to_timestamp` expects a space:

```python
# WRONG - to_timestamp may fail or return null
.withColumn("event_ts", f.to_timestamp("ts"))

# CORRECT - Replace 'T' with space first
.withColumn("event_ts", f.to_timestamp(f.regexp_replace("ts", "T", " ")))
```

### Event-Sourced Data

When working with event tables that have multiple rows per entity (e.g., order lifecycle events), filter by event type to avoid double-counting:

```python
@dp.materialized_view(name="gold.daily_orders")
def daily_orders():
    return (
        spark.table("iceberg.silver.orders")
        .filter(f.col("event_type") == "order_created")  # Only count creation events
        .groupBy("event_date")
        .agg(f.countDistinct("order_id").alias("order_count"))
    )
```

### Define Explicit Schemas for External Data

For production pipelines, always define schemas explicitly:

```python
from pyspark.sql.types import StructType, StructField, StringType, IntegerType

ORDER_SCHEMA = StructType([
    StructField("order_id", StringType()),
    StructField("ts", StringType()),  # Use actual column name from source
    StructField("event_type", StringType()),
    StructField("location_id", IntegerType()),
])

@dp.materialized_view(name="bronze.orders")
def orders():
    return spark.read.schema(ORDER_SCHEMA).parquet("/data/events/orders.parquet")
```

### Don't Assume Column Names

**Common mistake:** Assuming `order_timestamp` when the actual column is `ts` or `created_at`.

**Best practice:** Always run `printSchema()` on source data first, or check existing pipelines that use the same data.

---

## Your First Pipeline

### Step 1: Create the Pipeline File

`my_first_pipeline.py`:

```python
"""My first SDP pipeline."""

from typing import Any
from pyspark import pipelines as dp
from pyspark.sql import functions as f

spark: Any

# Bronze: Load raw data
@dp.materialized_view(name="bronze.users")
def users():
    """Raw user data from CSV."""
    return spark.read.csv("/data/users.csv", header=True, inferSchema=True)

# Silver: Clean the data
@dp.materialized_view(name="silver.users_clean")
def users_clean():
    """Users with valid emails only."""
    return (
        spark.table("iceberg.bronze.users")
        .filter(f.col("email").isNotNull())
        .filter(f.col("email").contains("@"))
    )

# Gold: Business metrics
@dp.materialized_view(name="gold.users_by_country")
def users_by_country():
    """User counts per country."""
    return (
        spark.table("iceberg.silver.users_clean")
        .groupBy("country")
        .agg(f.count("*").alias("user_count"))
    )
```

### Step 2: Create the Config

`pipeline.yml`:

```yaml
name: my_first_pipeline
libraries:
  - file: my_first_pipeline.py
catalog: iceberg
database: bronze
storage: /tmp/checkpoints
```

### Step 3: Initialize (Optional)

```bash
# Creates a template pipeline.yml if you don't have one
spark-pipelines init
```

### Step 4: Validate

```bash
# Check for errors without running
spark-pipelines dry-run --spec pipeline.yml
```

Output shows:
- Syntax errors
- Missing dependencies
- Circular dependencies
- Execution plan

### Step 5: Run

```bash
spark-pipelines run --spec pipeline.yml
```

### Step 6: Query Results

```python
spark.table("iceberg.gold.users_by_country").show()
```

---

## The SDP API

### Import Statement

```python
from pyspark import pipelines as dp
```

### @dp.materialized_view()

For batch/static data that's fully recomputed each run.

```python
@dp.materialized_view(
    name="database.table_name",      # Required: output table name
    comment="Description",            # Optional: table comment
    partition_cols=["date", "hour"],  # Optional: partition columns
    table_properties={                # Optional: Iceberg/Delta properties
        "write.format.default": "parquet"
    }
)
def my_view():
    return dataframe
```

### @dp.table()

For streaming/incremental data that maintains state.

```python
@dp.table(
    name="database.table_name",
    comment="Streaming orders from Kafka",
    partition_cols=["event_date"],
)
def my_streaming_table():
    return spark.readStream.format("kafka")...
```

### Decorator Parameters Reference

| Parameter | Type | Description |
|-----------|------|-------------|
| `name` | str | **Required.** Output table name as `database.table` |
| `comment` | str | Table description (shows in catalog) |
| `partition_cols` | list[str] | Columns to partition by |
| `table_properties` | dict | Table-level properties for Iceberg/Delta |

### Function Requirements

1. **Must return a DataFrame** (or streaming DataFrame for `@dp.table`)
2. **No side effects** (no writes, no prints for production)
3. **Deterministic** (same input → same output)

```python
# CORRECT
@dp.materialized_view(name="silver.orders")
def orders():
    return spark.table("iceberg.bronze.orders").filter(...)

# WRONG - has side effects
@dp.materialized_view(name="silver.orders")
def orders():
    df = spark.table("iceberg.bronze.orders")
    print(f"Processing {df.count()} rows")  # Side effect!
    df.write.saveAsTable("iceberg.silver.orders")  # Side effect!
    return df
```

---

## Pipeline Configuration

### Full pipeline.yml Reference

```yaml
# Pipeline identity
name: my_pipeline                    # Required: unique pipeline name

# Source code
libraries:                           # Required: Python files with decorators
  - file: pipeline.py
  - file: transforms/silver.py       # Can list multiple files
  - file: transforms/gold.py

# Catalog configuration
catalog: iceberg                     # Default catalog for output tables
database: bronze                     # Default database (only used if decorator name has no dot)

# Streaming configuration
storage: /tmp/checkpoints            # Checkpoint location (required for streaming, optional for batch)

# Spark configuration
configuration:                       # Optional: Spark configs
  spark.sql.shuffle.partitions: "200"
  spark.sql.adaptive.enabled: "true"
  spark.executor.memory: "4g"

# Cluster configuration (for managed deployments)
cluster:
  spark_version: "4.1.0"
  node_type: "Standard_DS3_v2"
  num_workers: 4
```

### Understanding YAML Fields

| Field | Required | Purpose |
|-------|----------|---------|
| `name` | Yes | Unique pipeline identifier |
| `libraries` | Yes | Python files containing decorated functions |
| `catalog` | Yes | Catalog prefix for output tables (e.g., `iceberg`) |
| `database` | No | Default database when decorator `name` has no dot (rarely used) |
| `storage` | Streaming: Yes, Batch: No | Checkpoint directory for streaming state |
| `configuration` | No | Spark config overrides |

**Clarifications:**

- **`catalog`**: Prepended to decorator names. If `catalog: iceberg` and `name="bronze.orders"`, output table is `iceberg.bronze.orders`.

- **`database`**: Only used when decorator `name` has no dot (e.g., `name="orders"`). Since best practice is to always include database in decorator (`name="bronze.orders"`), this field is rarely needed.

- **`storage`**: Required only if you use `@dp.table()` (streaming). For batch-only pipelines with `@dp.materialized_view()`, you can omit it. Including it doesn't hurt.

### Multi-Environment Configs

Create separate files for each environment:

```
config/
├── pipeline-dev.yml
├── pipeline-staging.yml
└── pipeline-prod.yml
```

`pipeline-dev.yml`:
```yaml
name: my_pipeline_dev
libraries:
  - file: pipeline.py
catalog: iceberg_dev
storage: /tmp/dev-checkpoints
configuration:
  spark.sql.shuffle.partitions: "10"  # Smaller for dev
```

`pipeline-prod.yml`:
```yaml
name: my_pipeline_prod
libraries:
  - file: pipeline.py
catalog: iceberg_prod
storage: s3://my-bucket/checkpoints
configuration:
  spark.sql.shuffle.partitions: "400"  # Larger for prod
```

---

## CLI Reference

### spark-pipelines init

Creates a template `pipeline.yml` in the current directory.

```bash
spark-pipelines init
# Creates: pipeline.yml with example structure
```

### spark-pipelines dry-run

Validates the pipeline without executing. **Always run this first.**

```bash
spark-pipelines dry-run --spec pipeline.yml
```

What it checks:
- Python syntax errors
- Import errors
- Missing `spark: Any` declaration
- Circular dependencies
- Invalid table references
- Schema compatibility (when possible)

Example output:
```
Pipeline: my_pipeline
Tables found: 5
Execution order:
  1. bronze.users (no dependencies)
  2. bronze.products (no dependencies)
  3. silver.users_clean (depends on: bronze.users)
  4. silver.orders_enriched (depends on: bronze.orders, bronze.products)
  5. gold.daily_metrics (depends on: silver.orders_enriched)

Validation: PASSED
```

### spark-pipelines run

Executes the pipeline.

```bash
# Run all tables
spark-pipelines run --spec pipeline.yml

# Run specific tables (and their dependencies)
spark-pipelines run --spec pipeline.yml --tables silver.orders_enriched

# Run with full refresh (ignore incremental state)
spark-pipelines run --spec pipeline.yml --full-refresh

# Run in development mode (more logging)
spark-pipelines run --spec pipeline.yml --development
```

### spark-pipelines graph

Visualizes the dependency graph.

```bash
spark-pipelines graph --spec pipeline.yml
```

Output (ASCII art):
```
bronze.users ──────┐
                   ├──► silver.users_clean ──► gold.user_metrics
bronze.products ───┘
```

### spark-pipelines validate

Deep validation including schema checks.

```bash
spark-pipelines validate --spec pipeline.yml
```

---

## Patterns & Best Practices

### Pattern 1: Medallion Architecture

The standard Bronze → Silver → Gold pattern:

```python
# ============ BRONZE: Raw Ingestion ============
@dp.materialized_view(name="bronze.orders_raw")
def orders_raw():
    """Raw orders exactly as received."""
    return spark.read.parquet("/data/orders/")

@dp.materialized_view(name="bronze.dim_products")
def dim_products():
    """Product dimension table."""
    return spark.read.parquet("/data/products/")

# ============ SILVER: Cleaned & Enriched ============
@dp.materialized_view(name="silver.orders_clean")
def orders_clean():
    """Orders with nulls removed and types fixed."""
    return (
        spark.table("iceberg.bronze.orders_raw")
        .filter(f.col("order_id").isNotNull())
        .withColumn("amount", f.col("amount").cast("decimal(10,2)"))
        .withColumn("order_date", f.to_date("order_timestamp"))
    )

@dp.materialized_view(name="silver.orders_enriched")
def orders_enriched():
    """Orders joined with product details."""
    orders = spark.table("iceberg.silver.orders_clean")
    products = spark.table("iceberg.bronze.dim_products")
    return orders.join(f.broadcast(products), "product_id", "left")

# ============ GOLD: Business Aggregations ============
@dp.materialized_view(name="gold.daily_revenue")
def daily_revenue():
    """Revenue aggregated by day."""
    return (
        spark.table("iceberg.silver.orders_enriched")
        .groupBy("order_date")
        .agg(
            f.sum("amount").alias("total_revenue"),
            f.count("order_id").alias("order_count"),
            f.countDistinct("customer_id").alias("unique_customers"),
        )
    )
```

### Pattern 2: Fact and Dimension Tables

Separate slowly-changing dimensions from fast-moving facts:

```python
# Dimensions - loaded once or daily
@dp.materialized_view(name="bronze.dim_customers")
def dim_customers():
    return spark.read.parquet("/data/customers/")

@dp.materialized_view(name="bronze.dim_locations")
def dim_locations():
    return spark.read.parquet("/data/locations/")

# Facts - high volume, partitioned
@dp.materialized_view(
    name="silver.fact_orders",
    partition_cols=["order_date"]
)
def fact_orders():
    orders = spark.table("iceberg.bronze.orders_raw")
    customers = spark.table("iceberg.bronze.dim_customers")
    locations = spark.table("iceberg.bronze.dim_locations")

    return (
        orders
        .join(f.broadcast(customers), "customer_id", "left")
        .join(f.broadcast(locations), "location_id", "left")
        .withColumn("order_date", f.to_date("order_timestamp"))
    )
```

### Pattern 3: Reusable Transformations

Extract common logic into helper functions:

```python
def add_time_features(df, timestamp_col="event_timestamp"):
    """Add standard time-based features."""
    return df.withColumns({
        "event_hour": f.hour(timestamp_col),
        "event_date": f.to_date(timestamp_col),
        "day_of_week": f.dayofweek(timestamp_col),
        "is_weekend": f.dayofweek(timestamp_col).isin(1, 7),
    })

def filter_valid_records(df, required_cols):
    """Remove records with nulls in required columns."""
    condition = f.col(required_cols[0]).isNotNull()
    for col_name in required_cols[1:]:
        condition = condition & f.col(col_name).isNotNull()
    return df.filter(condition)

# Use in pipeline
@dp.materialized_view(name="silver.orders_enriched")
def orders_enriched():
    orders = spark.table("iceberg.bronze.orders")
    orders = filter_valid_records(orders, ["order_id", "customer_id"])
    orders = add_time_features(orders, "order_timestamp")
    return orders
```

### Pattern 4: Handling Schema Evolution

Use explicit schemas for external data:

```python
from pyspark.sql.types import StructType, StructField, StringType, DoubleType

ORDER_SCHEMA = StructType([
    StructField("order_id", StringType(), nullable=False),
    StructField("customer_id", StringType(), nullable=False),
    StructField("amount", DoubleType(), nullable=True),
    StructField("order_timestamp", StringType(), nullable=True),
])

@dp.materialized_view(name="bronze.orders")
def orders():
    return spark.read.schema(ORDER_SCHEMA).parquet("/data/orders/")
```

### Pattern 5: Conditional Logic

Handle different scenarios cleanly:

```python
@dp.materialized_view(name="silver.orders_categorized")
def orders_categorized():
    return (
        spark.table("iceberg.bronze.orders")
        .withColumn(
            "order_size",
            f.when(f.col("amount") < 50, "small")
             .when(f.col("amount") < 200, "medium")
             .otherwise("large")
        )
        .withColumn(
            "is_high_value",
            f.col("amount") >= 500
        )
    )
```

---

## Streaming Pipelines

### Basic Kafka Ingestion

```python
from pyspark.sql.types import StructType, StructField, StringType, DoubleType

@dp.table(name="bronze.orders_stream")
def orders_stream():
    """Ingest orders from Kafka in real-time."""
    schema = StructType([
        StructField("order_id", StringType()),
        StructField("customer_id", StringType()),
        StructField("amount", DoubleType()),
        StructField("event_time", StringType()),
    ])

    return (
        spark.readStream
        .format("kafka")
        .option("kafka.bootstrap.servers", "localhost:9092")
        .option("subscribe", "orders")
        .option("startingOffsets", "latest")
        .load()
        .select(
            f.from_json(f.col("value").cast("string"), schema).alias("data")
        )
        .select("data.*")
        .withColumn("event_timestamp", f.to_timestamp("event_time"))
    )
```

### Streaming Aggregations with Watermarks

```python
@dp.table(name="gold.hourly_revenue_stream")
def hourly_revenue_stream():
    """Real-time hourly revenue with late data handling."""
    return (
        spark.table("iceberg.bronze.orders_stream")
        .withWatermark("event_timestamp", "1 hour")  # Handle late data
        .groupBy(
            f.window("event_timestamp", "1 hour").alias("time_window")
        )
        .agg(
            f.sum("amount").alias("total_revenue"),
            f.count("order_id").alias("order_count"),
        )
        .select(
            f.col("time_window.start").alias("window_start"),
            f.col("time_window.end").alias("window_end"),
            "total_revenue",
            "order_count",
        )
    )
```

### Mixed Batch and Streaming

```python
# Batch dimension table
@dp.materialized_view(name="bronze.dim_products")
def dim_products():
    return spark.read.parquet("/data/products/")

# Streaming fact table
@dp.table(name="bronze.orders_stream")
def orders_stream():
    return spark.readStream.format("kafka")...

# Join streaming with batch
@dp.table(name="silver.orders_enriched_stream")
def orders_enriched_stream():
    orders = spark.table("iceberg.bronze.orders_stream")      # Streaming
    products = spark.table("iceberg.bronze.dim_products")     # Batch (broadcast)
    return orders.join(f.broadcast(products), "product_id")
```

### Watermarks and Late Data

```python
@dp.table(name="silver.orders_with_watermark")
def orders_with_watermark():
    """Handle late-arriving data up to 2 hours."""
    return (
        spark.table("iceberg.bronze.orders_stream")
        # Events arriving more than 2 hours late are dropped
        .withWatermark("event_timestamp", "2 hours")
    )
```

---

## Performance Tuning

### Partitioning Strategy

```python
# Partition by date for time-series data
@dp.materialized_view(
    name="silver.orders",
    partition_cols=["order_date"]
)
def orders():
    return (
        spark.table("iceberg.bronze.orders_raw")
        .withColumn("order_date", f.to_date("order_timestamp"))
    )

# Multi-level partitioning for large datasets
@dp.materialized_view(
    name="silver.events",
    partition_cols=["event_date", "event_hour"]
)
def events():
    return (
        spark.table("iceberg.bronze.events_raw")
        .withColumn("event_date", f.to_date("event_timestamp"))
        .withColumn("event_hour", f.hour("event_timestamp"))
    )
```

### Broadcast Joins

```python
@dp.materialized_view(name="silver.orders_enriched")
def orders_enriched():
    orders = spark.table("iceberg.bronze.orders")  # Large: 100M rows
    products = spark.table("iceberg.bronze.products")  # Small: 10K rows

    # Broadcast the small table to all executors
    return orders.join(f.broadcast(products), "product_id")
```

### Shuffle Optimization

In `pipeline.yml`:

```yaml
configuration:
  # Adaptive query execution (recommended)
  spark.sql.adaptive.enabled: "true"
  spark.sql.adaptive.coalescePartitions.enabled: "true"

  # Shuffle partitions (tune based on data size)
  # Rule of thumb: 2-3x number of cores, or data_size_mb / 128
  spark.sql.shuffle.partitions: "200"

  # For skewed data
  spark.sql.adaptive.skewJoin.enabled: "true"
```

### Caching for Reused DataFrames

```python
@dp.materialized_view(name="silver.orders_base")
def orders_base():
    """Base orders used by multiple downstream tables."""
    return (
        spark.table("iceberg.bronze.orders")
        .filter(f.col("status") == "completed")
        .cache()  # Cache if used multiple times in same run
    )
```

### Column Pruning

Only select columns you need:

```python
@dp.materialized_view(name="gold.customer_summary")
def customer_summary():
    # Select only needed columns early
    return (
        spark.table("iceberg.silver.orders")
        .select("customer_id", "amount", "order_date")  # Prune early!
        .groupBy("customer_id")
        .agg(f.sum("amount").alias("total_spend"))
    )
```

### Filter Pushdown

Apply filters as early as possible:

```python
@dp.materialized_view(name="gold.recent_metrics")
def recent_metrics():
    return (
        spark.table("iceberg.silver.orders")
        # Filter early - Iceberg pushes this to file scan
        .filter(f.col("order_date") >= f.date_sub(f.current_date(), 30))
        .groupBy("order_date")
        .agg(f.sum("amount").alias("daily_revenue"))
    )
```

---

## Testing Strategies

### Unit Testing Individual Functions

```python
# test_pipeline.py
import pytest
from unittest.mock import MagicMock, patch
from pyspark.sql import SparkSession

# Create test SparkSession
@pytest.fixture(scope="session")
def spark():
    return SparkSession.builder.master("local[2]").getOrCreate()

def test_add_time_features(spark):
    """Test helper function with real Spark."""
    from my_pipeline import add_time_features

    # Create test data
    df = spark.createDataFrame([
        ("2024-01-15 10:30:00",),
        ("2024-01-16 14:45:00",),
    ], ["event_timestamp"])

    df = df.withColumn("event_timestamp", f.to_timestamp("event_timestamp"))
    result = add_time_features(df)

    assert "event_hour" in result.columns
    assert "event_date" in result.columns
    assert result.filter(f.col("event_hour") == 10).count() == 1
```

### Testing Decorated Functions with Mocks

```python
def test_orders_enriched_joins_correctly():
    """Test that orders_enriched performs expected join."""
    import my_pipeline

    # Create mock DataFrames
    mock_orders = MagicMock()
    mock_products = MagicMock()
    mock_result = MagicMock()

    mock_orders.join.return_value = mock_result

    # Mock spark.table to return our mocks
    mock_spark = MagicMock()
    mock_spark.table.side_effect = lambda name: {
        "iceberg.bronze.orders": mock_orders,
        "iceberg.bronze.products": mock_products,
    }[name]

    # Inject mock
    with patch.object(my_pipeline, 'spark', mock_spark):
        result = my_pipeline.orders_enriched()

    # Verify join was called
    mock_orders.join.assert_called_once()
```

### Integration Testing

```python
def test_full_pipeline_integration(spark):
    """Integration test with real data."""
    # Create test tables
    test_orders = spark.createDataFrame([
        ("order_1", "cust_1", 100.0),
        ("order_2", "cust_1", 200.0),
    ], ["order_id", "customer_id", "amount"])

    test_orders.write.mode("overwrite").saveAsTable("test.bronze_orders")

    # Run pipeline function
    from my_pipeline import customer_summary

    with patch.object(my_pipeline, 'spark', spark):
        # Modify to use test tables
        result = customer_summary()

    # Verify results
    assert result.count() == 1
    assert result.filter(f.col("customer_id") == "cust_1").first()["total_spend"] == 300.0
```

### Schema Testing

```python
def test_output_schema(spark):
    """Verify output schema matches expectations."""
    expected_columns = {
        "order_id": "string",
        "amount": "decimal(10,2)",
        "order_date": "date",
    }

    result = my_pipeline.orders_clean()

    for col_name, col_type in expected_columns.items():
        assert col_name in result.columns, f"Missing column: {col_name}"
        actual_type = dict(result.dtypes)[col_name]
        assert col_type in actual_type, f"Wrong type for {col_name}"
```

---

## Production Operations

### Deployment Patterns

#### Pattern 1: Git-Based Deployment

```bash
# CI/CD pipeline
git push origin main
→ CI validates with dry-run
→ CD deploys to cluster
→ Scheduler triggers run
```

#### Pattern 2: Docker Container

```dockerfile
FROM apache/spark:4.1.0-python3

COPY pipeline.py /app/
COPY pipeline.yml /app/

WORKDIR /app
CMD ["spark-pipelines", "run", "--spec", "pipeline.yml"]
```

### Scheduling

#### With Airflow

```python
from airflow import DAG
from airflow.operators.bash import BashOperator

dag = DAG('sdp_pipeline', schedule_interval='@hourly')

run_pipeline = BashOperator(
    task_id='run_pipeline',
    bash_command='spark-pipelines run --spec /path/to/pipeline.yml',
    dag=dag,
)
```

#### With Cron

```bash
# /etc/cron.d/sdp-pipeline
0 * * * * spark-user spark-pipelines run --spec /app/pipeline.yml >> /var/log/pipeline.log 2>&1
```

### Monitoring

#### Health Checks

```bash
# Validate before each run
spark-pipelines dry-run --spec pipeline.yml || exit 1
spark-pipelines run --spec pipeline.yml
```

#### Metrics to Track

| Metric | Description | Alert Threshold |
|--------|-------------|-----------------|
| Runtime | Total pipeline duration | > 2x baseline |
| Records processed | Rows written per table | < 50% expected |
| Error rate | Failed runs / total runs | > 5% |
| Data freshness | Time since last update | > 2x schedule interval |

### Error Handling & Recovery

```bash
# Retry with exponential backoff
for i in 1 2 4 8; do
    spark-pipelines run --spec pipeline.yml && break
    echo "Attempt failed, retrying in ${i}s..."
    sleep $i
done

# Full refresh if incremental fails
spark-pipelines run --spec pipeline.yml --full-refresh
```

### Backfill Strategies

```bash
# Run for specific date range (if your pipeline supports it)
spark-pipelines run --spec pipeline.yml \
    --tables gold.daily_metrics \
    --start-date 2024-01-01 \
    --end-date 2024-01-31
```

---

## Migration from Imperative

### Step-by-Step Migration

#### Before (Imperative)

```python
def create_bronze_orders(spark):
    df = spark.read.parquet("/data/orders.parquet")
    df.write.mode("overwrite").saveAsTable("iceberg.bronze.orders")

def create_silver_orders(spark):
    df = spark.table("iceberg.bronze.orders")
    df = df.filter(col("id").isNotNull())
    df.write.mode("overwrite").saveAsTable("iceberg.silver.orders")

if __name__ == "__main__":
    spark = SparkSession.builder.getOrCreate()
    create_bronze_orders(spark)
    create_silver_orders(spark)
```

#### After (Declarative)

```python
from typing import Any
from pyspark import pipelines as dp
from pyspark.sql import functions as f

spark: Any

@dp.materialized_view(name="bronze.orders")
def bronze_orders():
    return spark.read.parquet("/data/orders.parquet")

@dp.materialized_view(name="silver.orders")
def silver_orders():
    return (
        spark.table("iceberg.bronze.orders")
        .filter(f.col("id").isNotNull())
    )
```

### Migration Checklist

- [ ] Remove all `SparkSession.builder` code
- [ ] Add `from pyspark import pipelines as dp`
- [ ] Add `spark: Any` at module level
- [ ] Add decorator to each function
- [ ] Remove all `.write.saveAsTable()` calls
- [ ] Change function to `return` DataFrame instead
- [ ] Update table references to use full qualified names (`catalog.database.table`)
- [ ] Remove manual execution order (`if __name__` block)
- [ ] Create `pipeline.yml` configuration
- [ ] Test with `spark-pipelines dry-run`

### Hybrid Approach (Gradual Migration)

Migrate one layer at a time:

```python
# Already migrated to SDP
@dp.materialized_view(name="bronze.orders")
def bronze_orders():
    return spark.read.parquet("/data/orders.parquet")

# Still imperative (will migrate later)
def create_silver_orders(spark):
    df = spark.table("iceberg.bronze.orders")
    df.write.mode("overwrite").saveAsTable("iceberg.silver.orders")
```

---

## Troubleshooting

### Error: "NameError: name 'spark' is not defined"

**Cause:** Missing module-level `spark` declaration.

```python
# FIX: Add at top of file
from typing import Any
spark: Any
```

### Error: "Table 'X' not found" during dependency resolution

**Cause:** Using short table names or wrong pattern.

```python
# WRONG
df = spark.table("bronze.orders")           # Missing catalog
df = spark.read.table("iceberg.bronze.orders")  # Wrong method
df = spark.sql("SELECT * FROM iceberg.bronze.orders")  # SQL not detected

# CORRECT
df = spark.table("iceberg.bronze.orders")
```

### Error: "Circular dependency detected"

**Cause:** Two tables reference each other.

```python
# WRONG
@dp.materialized_view(name="silver.a")
def a(): return spark.table("iceberg.silver.b")

@dp.materialized_view(name="silver.b")
def b(): return spark.table("iceberg.silver.a")  # Circular!

# FIX: Break the cycle by restructuring
@dp.materialized_view(name="silver.a")
def a(): return spark.table("iceberg.bronze.raw")

@dp.materialized_view(name="silver.b")
def b(): return spark.table("iceberg.silver.a")  # Linear now
```

### Error: "Cannot write to table" or write-related errors

**Cause:** Calling `.write()` in a decorated function.

```python
# WRONG
@dp.materialized_view(name="silver.orders")
def orders():
    df = spark.table("iceberg.bronze.orders")
    df.write.saveAsTable("iceberg.silver.orders")  # Remove!
    return df

# FIX: Just return
@dp.materialized_view(name="silver.orders")
def orders():
    return spark.table("iceberg.bronze.orders")
```

### Tables Running in Wrong Order

**Cause:** Dependency not detected (variable table name, wrong method).

```python
# WRONG - Variable not detected
table_name = "iceberg.bronze.orders"
df = spark.table(table_name)

# CORRECT - Literal string detected
df = spark.table("iceberg.bronze.orders")
```

### Streaming Table Not Starting

Check that:
1. Using `@dp.table()` not `@dp.materialized_view()`
2. Using `spark.readStream` not `spark.read`
3. Checkpoint location exists and is writable
4. Kafka/source is running and accessible

### Out of Memory Errors

```yaml
# pipeline.yml - increase memory
configuration:
  spark.executor.memory: "8g"
  spark.driver.memory: "4g"
  spark.sql.shuffle.partitions: "400"
```

### Debugging Tips

```python
# Add logging (remove for production)
import logging
logging.basicConfig(level=logging.INFO)

@dp.materialized_view(name="silver.orders")
def orders():
    df = spark.table("iceberg.bronze.orders")
    logging.info(f"Input rows: {df.count()}")  # Debug only!
    return df.filter(...)
```

---

## When NOT to Use SDP

SDP isn't always the right choice:

| Scenario | Use Instead |
|----------|-------------|
| One-off analysis/exploration | Interactive notebooks |
| Complex control flow (if/else at pipeline level) | Imperative scripts |
| Non-Spark processing steps | Airflow/Dagster |
| Spark < 4.1 | Imperative PySpark |
| Real-time with sub-second latency | Kafka Streams, Flink |
| Very simple ETL (one table) | Plain spark-submit |
| Heavy use of `spark.sql()` | Imperative (SQL not detected for deps) |

### Signs SDP Might Not Fit

- You need complex branching logic between tables
- Tables have runtime-determined dependencies
- You're doing more `spark.sql()` than DataFrame API
- You need custom write modes per table
- Your team isn't comfortable with decorators

---

## Quick Reference

### Minimal Template

```python
from typing import Any
from pyspark import pipelines as dp
from pyspark.sql import functions as f

spark: Any

@dp.materialized_view(name="bronze.my_table")
def my_table():
    return spark.read.parquet("/data/file.parquet")

@dp.materialized_view(name="silver.my_table_clean")
def my_table_clean():
    return spark.table("iceberg.bronze.my_table").filter(f.col("id").isNotNull())
```

### Minimal pipeline.yml

```yaml
name: my_pipeline
libraries:
  - file: pipeline.py
catalog: iceberg
database: bronze
storage: /tmp/checkpoints
```

### CLI Quick Reference

```bash
spark-pipelines init                    # Create template config
spark-pipelines dry-run --spec X.yml    # Validate without running
spark-pipelines run --spec X.yml        # Execute pipeline
spark-pipelines graph --spec X.yml      # Show dependency graph
```

### Pre-Run Checklist

- [ ] `from pyspark import pipelines as dp`
- [ ] `spark: Any` declared at module level
- [ ] All decorated functions return DataFrames
- [ ] No `.write()` calls in decorated functions
- [ ] All `spark.table()` use full names: `catalog.database.table`
- [ ] No circular dependencies
- [ ] `pipeline.yml` points to correct files

### Common Imports

```python
from typing import Any
from pyspark import pipelines as dp
from pyspark.sql import functions as f
from pyspark.sql.types import (
    StructType, StructField,
    StringType, IntegerType, DoubleType, TimestampType, DateType,
    ArrayType, MapType
)

spark: Any
```

---

## References

- [Apache Spark 4.1 Documentation - Declarative Pipelines](https://spark.apache.org/docs/4.1.0/declarative-pipelines.html)
- [Lakehouse Stack Pipelines Guide](../../docs/guides/pipelines.md)
- Example Implementation: `scripts/pipeline_sdp.py`
