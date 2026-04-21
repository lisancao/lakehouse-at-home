# Apache Spark 4.1 Documentation Index

> Compressed reference for AI agents. Always in context - no skill invocation required.

## Spark 4.1 Requirements (CRITICAL)

| Dependency | Minimum | Notes |
|------------|---------|-------|
| Python | 3.10+ | **Dropped 3.9** |
| JDK | 17+ | **Dropped 8/11** |
| Pandas | 2.2.0+ | For pandas API |
| PyArrow | 15.0.0+ | Arrow-optimized UDFs |

### Breaking Changes (Spark 4.x)

- **ANSI mode ON by default**: Division by zero and invalid casts raise errors
- **Safe alternatives**: `try_divide()`, `try_cast()`, `try_to_date()`
- **Removed**: `DataFrame.append()` → use `ps.concat()`

## Standard Imports

```python
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as f
from pyspark.sql import types as t
from pyspark.sql.window import Window

spark = SparkSession.builder.appName("job").getOrCreate()
```

## PySpark DataFrame Quick Reference

| Task | Code |
|------|------|
| Filter | `df.filter(f.col("x") > 0)` |
| Select | `df.select("a", "b")` |
| Add column | `df.withColumn("new", expr)` |
| Batch columns (4.1) | `df.withColumns({"a": expr1, "b": expr2})` |
| Conditional | `f.when(cond, val).otherwise(default)` |
| Aggregate | `df.groupBy("x").agg(f.sum("y"))` |
| Join | `df1.join(df2, "key", "left")` |
| Broadcast join | `df1.join(f.broadcast(df_small), "key")` |
| Window | `f.row_number().over(Window.partitionBy("x").orderBy("y"))` |
| Dedupe latest | `df.withColumn("rn", f.row_number().over(w)).filter(f.col("rn")==1)` |
| Order by | `df.orderBy(f.col("x").desc())` or `df.orderBy(f.desc("x"))` |
| Safe divide | `f.expr("try_divide(a, b)")` |
| Safe cast | `df.selectExpr("try_cast(x as int)")` |

## Aggregation Functions

| Task | Code |
|------|------|
| Count | `f.count("*")`, `f.count("col")` |
| Count distinct | `f.countDistinct("col")` |
| Sum/Avg/Min/Max | `f.sum("col")`, `f.avg("col")`, `f.min("col")`, `f.max("col")` |
| Percentile | `f.percentile_approx("col", 0.5)` (median) |
| Percentiles array | `f.percentile_approx("col", [0.25, 0.5, 0.75])` (quartiles) |
| Collect to list | `f.collect_list("col")`, `f.collect_set("col")` |
| First/Last | `f.first("col")`, `f.last("col")` |

## Pivot & Unpivot

```python
# Pivot: rows to columns
df.groupBy("id").pivot("category", ["A", "B", "C"]).agg(f.sum("value"))

# Result: id | A | B | C (with summed values)

# Unpivot: columns to rows (SQL)
spark.sql("""
    SELECT id, category, value
    FROM table
    UNPIVOT (value FOR category IN (A, B, C))
""")

## Null Handling

| Task | Code |
|------|------|
| Filter nulls | `df.filter(f.col("x").isNull())` / `.isNotNull()` |
| First non-null | `f.coalesce("col1", "col2", f.lit("default"))` |
| Fill nulls | `df.na.fill(0)` or `df.na.fill({"col1": 0, "col2": "N/A"})` |
| Drop null rows | `df.na.drop()` or `df.na.drop(subset=["col1", "col2"])` |
| Null-safe equal | `df.filter(f.col("a").eqNullSafe(f.col("b")))` |

## Date/Time Functions

| Task | Code |
|------|------|
| Current | `f.current_date()`, `f.current_timestamp()` |
| Parse date | `f.to_date("col", "yyyy-MM-dd")` |
| Parse timestamp | `f.to_timestamp("col", "yyyy-MM-dd HH:mm:ss")` |
| Format | `f.date_format("col", "yyyy-MM")` |
| Extract | `f.year("col")`, `f.month("col")`, `f.dayofweek("col")` |
| Arithmetic | `f.date_add("col", 7)`, `f.date_sub("col", 7)` |
| Difference | `f.datediff("end", "start")`, `f.months_between("end", "start")` |
| Truncate | `f.date_trunc("month", "col")`, `f.date_trunc("week", "col")` |
| Unix timestamp | `f.unix_timestamp("col")` → seconds since epoch |
| Duration (sec) | `f.unix_timestamp("end") - f.unix_timestamp("start")` |

## String Functions

| Task | Code |
|------|------|
| Case | `f.lower("col")`, `f.upper("col")`, `f.initcap("col")` |
| Trim | `f.trim("col")`, `f.ltrim("col")`, `f.rtrim("col")` |
| Concat | `f.concat("a", "b")`, `f.concat_ws("-", "a", "b", "c")` |
| Substring | `f.substring("col", 1, 5)` (1-indexed) |
| Split | `f.split("col", ",")` → array |
| Regex replace | `f.regexp_replace("col", r"\d+", "X")` |
| Regex extract | `f.regexp_extract("col", r"(\d+)", 1)` |
| Contains | `f.col("col").contains("text")` |
| Like | `f.col("col").like("%pattern%")` |

## Schema Types

```python
from pyspark.sql.types import (
    StructType, StructField, StringType, IntegerType,
    LongType, DoubleType, BooleanType, TimestampType,
    DateType, ArrayType, MapType, DecimalType
)

# Define schema
schema = StructType([
    StructField("id", StringType(), nullable=False),
    StructField("amount", DecimalType(10, 2)),
    StructField("tags", ArrayType(StringType())),
    StructField("metadata", MapType(StringType(), StringType())),
])

# Apply to read
df = spark.read.schema(schema).json("/path")
```

| Type | Python | Example |
|------|--------|---------|
| String | `StringType()` | `"hello"` |
| Integer | `IntegerType()` | `42` |
| Long | `LongType()` | `9999999999` |
| Double | `DoubleType()` | `3.14` |
| Decimal | `DecimalType(10,2)` | `123.45` |
| Boolean | `BooleanType()` | `True` |
| Date | `DateType()` | `2024-01-15` |
| Timestamp | `TimestampType()` | `2024-01-15 10:30:00` |
| Array | `ArrayType(StringType())` | `["a", "b"]` |
| Map | `MapType(StringType(), IntegerType())` | `{"a": 1}` |

## Array & Map Operations

| Task | Code |
|------|------|
| Array size | `f.size("arr")` |
| Array contains | `f.array_contains("arr", "value")` |
| Explode to rows | `df.select(f.explode("arr").alias("item"))` |
| Collect to array | `f.collect_list("col")`, `f.collect_set("col")` |
| Array element | `f.element_at("arr", 1)` (1-indexed) |
| Map keys/values | `f.map_keys("map")`, `f.map_values("map")` |
| Map lookup | `f.col("map")["key"]` or `f.element_at("map", "key")` |
| Struct field | `f.col("struct.field")` or `f.col("struct")["field"]` |
| JSON parse | `f.from_json("json_col", schema)` |
| JSON create | `f.to_json(f.struct("col1", "col2"))` |

## Spark SQL Quick Reference

| Task | SQL |
|------|-----|
| Window rank | `ROW_NUMBER() OVER (PARTITION BY x ORDER BY y)` |
| Filter window | `QUALIFY ROW_NUMBER() OVER (...) = 1` |
| CTE | `WITH cte AS (...) SELECT * FROM cte` |
| Recursive CTE (4.1) | `WITH RECURSIVE r AS (base UNION ALL recursive) SELECT ...` |
| Merge/upsert | `MERGE INTO target USING source ON ... WHEN MATCHED/NOT MATCHED` |
| Time travel | `SELECT * FROM table TIMESTAMP AS OF '2024-01-15'` |
| VARIANT (4.1) | `event_data:field::STRING` for JSON field access |

## Structured Streaming

```python
# Kafka source
df = spark.readStream.format("kafka") \
    .option("kafka.bootstrap.servers", "host:9092") \
    .option("subscribe", "topic").load()

# Watermark for late data
df.withWatermark("event_time", "10 minutes")

# Window aggregation
df.groupBy(f.window("event_time", "5 minutes")).agg(...)

# Iceberg sink
df.writeStream.format("iceberg") \
    .option("checkpointLocation", "/path") \
    .toTable("catalog.db.table")
```

| Trigger | Use Case |
|---------|----------|
| `processingTime="10 seconds"` | Micro-batch interval |
| `availableNow=True` | Process all, then stop |
| `continuous="1 second"` | Sub-second latency (limited ops) |

| Output Mode | Use Case | Aggregations |
|-------------|----------|--------------|
| `append` | New rows only, most sinks | Only with watermark |
| `update` | Changed rows, dashboards | Yes |
| `complete` | Full result each batch | Yes (small results only) |

## Spark Declarative Pipelines (SDP)

```python
from typing import Any
from pyspark import pipelines as dp

spark: Any  # Framework injects at runtime

@dp.materialized_view(name="silver.orders")  # No catalog prefix!
def orders():
    return spark.table("iceberg.bronze.raw")  # Full path with catalog!
```

**Critical naming:**
- Decorator: `name="database.table"` (no catalog)
- spark.table(): `"catalog.database.table"` (full path)

**CLI:**
```bash
spark-pipelines dry-run --spec pipeline.yml  # Validate
spark-pipelines run --spec pipeline.yml      # Execute
```

## Performance Tuning

### AQE (Enabled by Default)
```python
spark.conf.set("spark.sql.adaptive.enabled", "true")
spark.conf.set("spark.sql.adaptive.coalescePartitions.enabled", "true")
spark.conf.set("spark.sql.adaptive.skewJoin.enabled", "true")
```

### Key Configs
| Config | Default | Recommendation |
|--------|---------|----------------|
| `shuffle.partitions` | 200 | data_gb * 2-4 |
| `autoBroadcastJoinThreshold` | 10m | Up to 100m for larger dims |
| `advisoryPartitionSizeInBytes` | 64m | 128m-256m for large data |

### Join Hints
```sql
SELECT /*+ BROADCAST(small) */ * FROM large JOIN small ...
SELECT /*+ MERGE(t1) */ * FROM t1 JOIN t2 ...
SELECT /*+ LEADING(t1, t2, t3) */ * ...  -- Force join order (4.1)
```

## Spark Connect (Client-Server)

```python
# Remote connection
spark = SparkSession.builder.remote("sc://host:15002").getOrCreate()

# All DataFrame operations work; RDD/SparkContext do not
```

## Spark on Kubernetes

```bash
spark-submit \
    --master k8s://https://apiserver:6443 \
    --deploy-mode cluster \
    --conf spark.kubernetes.container.image=apache/spark:4.1.0 \
    --conf spark.kubernetes.authenticate.driver.serviceAccountName=spark \
    --conf spark.executor.instances=5 \
    local:///opt/spark/app.jar
```

## Common Patterns

### Read → Transform → Write
```python
(spark.read.table("iceberg.bronze.events")
    .filter(f.col("date") >= "2024-01-01")
    .groupBy("type").agg(f.count("*").alias("cnt"))
    .write.mode("overwrite")
    .saveAsTable("iceberg.gold.summary"))
```

### Window: Latest per Key
```python
w = Window.partitionBy("customer_id").orderBy(f.col("updated_at").desc())
df.withColumn("rn", f.row_number().over(w)).filter(f.col("rn") == 1).drop("rn")
```

### ISO Timestamp Handling
```python
# Spark expects space separator, not 'T'
.withColumn("ts", f.to_timestamp(f.regexp_replace("ts", "T", " ")))
```

## Testing Patterns

```python
# Create DataFrame from tuples
df = spark.createDataFrame([
    ("alice", 100),
    ("bob", 200),
], ["name", "amount"])

# Create DataFrame from dicts
df = spark.createDataFrame([
    {"name": "alice", "amount": 100},
    {"name": "bob", "amount": 200},
])

# With explicit schema
from pyspark.sql.types import StructType, StructField, StringType, IntegerType
schema = StructType([
    StructField("name", StringType()),
    StructField("amount", IntegerType()),
])
df = spark.createDataFrame([("alice", 100)], schema)

# Empty DataFrame with schema
df = spark.createDataFrame([], schema)
```

## New in Spark 4.1

| Feature | Description |
|---------|-------------|
| Arrow-Native UDFs | `@udf` with PyArrow arrays, bypasses Pandas |
| IN Subquery | `f.col("id").isin(subquery_df)` |
| Parameterized SQL | `spark.sql("...WHERE x = :val", args={"val": 1})` |
| VARIANT type | Semi-structured JSON with `:` field access |
| SQL Scripting GA | DECLARE, IF/ELSE, WHILE, error handlers |
| Recursive CTEs | `WITH RECURSIVE` for hierarchies |
| Join order hints | `/*+ LEADING(t1, t2) */` |
| approx_top_k | Efficient top-K frequent items |
| transformWithState | Custom stateful streaming processors |

## Common Errors

| Error | Cause | Fix |
|-------|-------|-----|
| `ArithmeticException: divide by zero` | ANSI mode | Use `try_divide()` |
| `NumberFormatException` | ANSI invalid cast | Use `try_cast()` |
| `Column cannot be resolved` | Typo or missing | Check `df.printSchema()` |
| `OOMError` | Data skew / collect() | Repartition, avoid collect |
| `Table not found` (SDP) | Wrong path format | Use full `catalog.db.table` in spark.table() |

---

# Production Patterns (Scale)

> For teams running Spark at petabyte scale on Kubernetes.

## Data Skew Detection & Resolution

### Identify Skew
```python
# Check partition sizes
from pyspark.sql.functions import spark_partition_id
df.groupBy(spark_partition_id()).count().orderBy("count", ascending=False).show()

# Check key distribution (find hot keys)
df.groupBy("join_key").count().orderBy(f.desc("count")).show(20)
```

**Spark UI indicators:**
- Max task duration >> 75th percentile (50%+ difference = skew)
- Few tasks stuck while others complete
- "Spill (Memory)" or "Spill (Disk)" in task metrics

### Salting for Skewed Joins
```python
# Salt the skewed side
SALT_BUCKETS = 10
df_skewed = df_large.withColumn("salt", (f.rand() * SALT_BUCKETS).cast("int"))
df_skewed = df_skewed.withColumn(
    "salted_key", f.concat(f.col("join_key"), f.lit("_"), f.col("salt"))
)

# Explode the small side with all salt values
df_small_exploded = df_small.crossJoin(
    spark.range(SALT_BUCKETS).withColumnRenamed("id", "salt")
).withColumn(
    "salted_key", f.concat(f.col("join_key"), f.lit("_"), f.col("salt"))
)

# Join on salted key
result = df_skewed.join(df_small_exploded, "salted_key").drop("salt", "salted_key")
```

### AQE Skew Handling (Preferred)
```python
spark.conf.set("spark.sql.adaptive.enabled", "true")
spark.conf.set("spark.sql.adaptive.skewJoin.enabled", "true")
spark.conf.set("spark.sql.adaptive.skewJoin.skewedPartitionFactor", "5.0")
spark.conf.set("spark.sql.adaptive.skewJoin.skewedPartitionThresholdInBytes", "256m")
```

## Shuffle Optimization at Scale

### Key Configurations
```python
# Partition sizing (rule: 128MB-256MB per partition)
spark.conf.set("spark.sql.shuffle.partitions", "auto")  # or calculate: data_size_gb * 4
spark.conf.set("spark.sql.adaptive.advisoryPartitionSizeInBytes", "128m")
spark.conf.set("spark.sql.adaptive.coalescePartitions.minPartitionSize", "32m")

# Push-based shuffle (Spark 3.2+, requires external shuffle service)
spark.conf.set("spark.shuffle.push.enabled", "true")
spark.conf.set("spark.shuffle.push.minShuffleSizeToWait", "50m")
```

### Reduce Shuffle Data
```python
# Filter and select BEFORE joins/aggregations
df_filtered = (df
    .filter(f.col("date") >= "2024-01-01")  # Filter early
    .select("id", "amount", "key")           # Select only needed columns
    .groupBy("key").agg(f.sum("amount"))
)

# Use broadcast for dimension tables < 100MB
spark.conf.set("spark.sql.autoBroadcastJoinThreshold", "100m")
df_large.join(f.broadcast(df_dim), "key")
```

## Iceberg Maintenance (Critical for Production)

### Compaction
```sql
-- Bin-pack compaction (default, fast)
CALL iceberg.system.rewrite_data_files(
    table => 'catalog.db.table',
    strategy => 'binpack',
    options => map('target-file-size-bytes', '536870912')  -- 512MB
);

-- Sort compaction (better query performance)
CALL iceberg.system.rewrite_data_files(
    table => 'catalog.db.table',
    strategy => 'sort',
    sort_order => 'event_date ASC NULLS LAST, event_hour ASC'
);

-- Z-order compaction (multi-column filter optimization)
CALL iceberg.system.rewrite_data_files(
    table => 'catalog.db.table',
    strategy => 'sort',
    sort_order => 'zorder(customer_id, product_id)'
);
```

### Snapshot & Metadata Cleanup
```sql
-- Expire snapshots older than 7 days (REQUIRED for streaming tables)
CALL iceberg.system.expire_snapshots(
    table => 'catalog.db.table',
    older_than => TIMESTAMP '2024-01-01 00:00:00',
    retain_last => 100
);

-- Remove orphan files (failed writes)
CALL iceberg.system.remove_orphan_files(
    table => 'catalog.db.table',
    older_than => TIMESTAMP '2024-01-01 00:00:00'
);

-- Rewrite manifests (query planning optimization)
CALL iceberg.system.rewrite_manifests('catalog.db.table');
```

### Streaming Table Settings
```sql
-- Auto-cleanup metadata for high-frequency commits
ALTER TABLE catalog.db.table SET TBLPROPERTIES (
    'write.metadata.delete-after-commit.enabled' = 'true',
    'write.metadata.previous-versions-max' = '100'
);
```

## Data Quality (Production)

### Deequ (Spark-Native)
```python
from pydeequ.checks import Check, CheckLevel
from pydeequ.verification import VerificationSuite, VerificationResult

check = Check(spark, CheckLevel.Error, "Data Quality Checks") \
    .isComplete("order_id") \
    .isUnique("order_id") \
    .isNonNegative("amount") \
    .isContainedIn("status", ["pending", "completed", "cancelled"]) \
    .satisfies("amount < 10000", "amount_reasonable")

result = VerificationSuite(spark) \
    .onData(df) \
    .addCheck(check) \
    .run()

# Fail pipeline if checks fail
if result.status != "Success":
    raise ValueError(f"Data quality check failed: {result}")
```

### Inline Validation Pattern
```python
def validate_and_write(df, table_name, checks):
    """Validate before writing - fail fast."""
    # Row-level validation
    df_valid = df.filter(
        f.col("id").isNotNull() &
        f.col("amount").isNotNull() &
        (f.col("amount") >= 0)
    )

    # Quarantine bad records
    df_invalid = df.subtract(df_valid)
    if df_invalid.count() > 0:
        df_invalid.write.mode("append").saveAsTable(f"{table_name}_quarantine")

    # Write valid records
    df_valid.write.mode("append").saveAsTable(table_name)
    return df_valid.count(), df_invalid.count()
```

## Streaming Production Patterns

### Checkpoint Best Practices
```python
# Use durable storage (S3, HDFS, ABFS)
checkpoint_path = "s3a://bucket/checkpoints/pipeline-name/query-name"

query = (df.writeStream
    .format("iceberg")
    .option("checkpointLocation", checkpoint_path)
    .option("fanout-enabled", "true")  # For partitioned tables
    .outputMode("append")
    .toTable("catalog.db.table"))

# CRITICAL: Never share checkpoints between queries
# CRITICAL: Never delete checkpoints while query is running
# CRITICAL: Treat checkpoint dirs as critical infrastructure
```

### Backpressure & Rate Limiting
```python
# Kafka rate limiting
df = (spark.readStream
    .format("kafka")
    .option("kafka.bootstrap.servers", "broker:9092")
    .option("subscribe", "topic")
    .option("maxOffsetsPerTrigger", 100000)      # Max records per batch
    .option("minOffsetsPerTrigger", 10000)       # Min records (wait for more)
    .option("maxTriggerDelay", "5 minutes")      # Max wait time
    .load())

# File source rate limiting
df = (spark.readStream
    .format("parquet")
    .option("maxFilesPerTrigger", 100)
    .option("maxBytesPerTrigger", "1g")
    .load(path))
```

### Exactly-Once Requirements
```python
# 1. Idempotent sink (Delta, Iceberg with merge)
# 2. Checkpoint on durable storage
# 3. Transactional commit support

# For Kafka sink (exactly-once)
df.writeStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "broker:9092") \
    .option("topic", "output") \
    .option("kafka.transactional.id", "spark-producer-1")  # Enable transactions \
    .option("checkpointLocation", checkpoint_path) \
    .start()
```

## Spark UI Diagnostics

### What to Check

| Symptom | UI Location | Likely Cause | Fix |
|---------|-------------|--------------|-----|
| One slow task | Stages → Task metrics | Data skew | Salt keys, enable AQE skew |
| All tasks slow | Executors → GC Time | Memory pressure | Increase memory, reduce partition size |
| Spill to disk | Stages → Spill | Insufficient memory | Increase `spark.memory.fraction` |
| Shuffle read high | Stages → Shuffle Read | Wide transformation | Filter early, broadcast small tables |
| Long GC pauses | Executors → GC Time | Too much cached data | Use `MEMORY_AND_DISK`, tune `storageFraction` |

### Key Metrics to Monitor
```python
# In production, export these to Prometheus/Grafana
# - spark.executor.runTime
# - spark.executor.gcTime
# - spark.shuffle.write.bytesWritten
# - spark.shuffle.read.bytesRead
# - spark.task.maxResultSize
```

## Kubernetes at Scale

### Resource Configuration
```yaml
# Driver
--conf spark.driver.cores=4
--conf spark.driver.memory=8g
--conf spark.driver.memoryOverhead=2g
--conf spark.kubernetes.driver.request.cores=2
--conf spark.kubernetes.driver.limit.cores=4

# Executors
--conf spark.executor.instances=50
--conf spark.executor.cores=4
--conf spark.executor.memory=16g
--conf spark.executor.memoryOverhead=4g

# Dynamic allocation (recommended)
--conf spark.dynamicAllocation.enabled=true
--conf spark.dynamicAllocation.minExecutors=10
--conf spark.dynamicAllocation.maxExecutors=100
--conf spark.dynamicAllocation.executorIdleTimeout=60s
--conf spark.dynamicAllocation.shuffleTracking.enabled=true
```

### Node Affinity & Priority
```yaml
# Pod template for executors
spec:
  nodeSelector:
    node-type: spark-compute
    instance-type: memory-optimized
  tolerations:
  - key: "spark-workload"
    operator: "Equal"
    value: "true"
    effect: "NoSchedule"
  priorityClassName: spark-production  # Preempt lower-priority pods
```

### Spot/Preemptible Instances
```python
# Graceful decommissioning for spot termination
spark.conf.set("spark.decommission.enabled", "true")
spark.conf.set("spark.storage.decommission.enabled", "true")
spark.conf.set("spark.storage.decommission.shuffleBlocks.enabled", "true")
```

## Cost Optimization

| Strategy | Impact | Implementation |
|----------|--------|----------------|
| Right-size executors | 20-40% | Profile jobs, match memory to workload |
| Spot instances | 60-80% | Enable decommissioning, use on executors only |
| Partition pruning | 50-90% | Partition by query filters (date, region) |
| Column pruning | 30-60% | Select only needed columns early |
| Compaction | 20-40% | Z-order on filter columns |
| Cache strategically | Variable | Cache only reused DataFrames |

---

## Detailed Skills

For comprehensive documentation, see:
- [PySpark.md](PySpark.md) - DataFrame API
- [Spark-SQL.md](Spark-SQL.md) - SQL patterns
- [Structured-Streaming.md](Structured-Streaming.md) - Real-time processing
- [SDP.md](SDP.md) - Declarative Pipelines
- [Performance-Tuning.md](Performance-Tuning.md) - Optimization
- [Spark-Connect.md](Spark-Connect.md) - Client-server
- [Spark-Kubernetes.md](Spark-Kubernetes.md) - K8s deployment
