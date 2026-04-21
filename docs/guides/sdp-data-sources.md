# Spark Declarative Pipelines (SDP) - Data Source Coverage Map

> Complete reference for data sources, sinks, and formats compatible with SDP in Spark 4.1+

---

## Coverage Matrix Overview

| Category | Source Type | Batch (`@dp.materialized_view`) | Streaming (`@dp.table`) | Notes |
|----------|-------------|:-------------------------------:|:-----------------------:|-------|
| **File Formats** | Parquet | ✅ | ✅ | Native, recommended |
| | JSON | ✅ | ✅ | Schema required for streaming |
| | CSV | ✅ | ✅ | Schema required for streaming |
| | ORC | ✅ | ✅ | |
| | Avro | ✅ | ✅ | Requires spark-avro |
| | Text | ✅ | ✅ | Single column output |
| **Open Table Formats** | Apache Iceberg | ✅ | ✅ | Full support |
| | Delta Lake | ✅ | ✅ | Requires delta-spark |
| | Apache Hudi | ✅ | ✅ | Requires hudi-spark |
| | Paimon | ✅ | ✅ | Requires paimon-spark |
| **Message Buses** | Apache Kafka | ❌ | ✅ | Primary streaming source |
| | Amazon Kinesis | ❌ | ✅ | Requires kinesis-spark |
| | Google Pub/Sub | ❌ | ✅ | Requires spark-pubsub |
| | Azure EventHub | ❌ | ✅ | Requires eventhubs-spark |
| | Apache Pulsar | ❌ | ✅ | Requires pulsar-spark |
| **Cloud Storage** | Amazon S3 | ✅ | ✅ | Via s3a:// protocol |
| | Azure ADLS Gen2 | ✅ | ✅ | Via abfss:// protocol |
| | Google Cloud Storage | ✅ | ✅ | Via gs:// protocol |
| | HDFS | ✅ | ✅ | Via hdfs:// protocol |
| | Local filesystem | ✅ | ✅ | Via file:// protocol |
| **Databases** | JDBC (PostgreSQL, MySQL, etc.) | ✅ | ❌* | Via foreachBatch |
| | MongoDB | ✅ | ❌* | Via foreachBatch |
| **Testing** | Rate source | ❌ | ✅ | Generates test data |
| | Socket source | ❌ | ✅ | Development only |
| | Memory sink | N/A | ✅ | Unit testing |

*Can write via `foreachBatch` pattern

---

## 1. Python API - File Formats

### Parquet (Recommended)

```python
from typing import Any
from pyspark import pipelines as dp
from pyspark.sql import functions as f

spark: Any

# Batch read
@dp.materialized_view(name="bronze.orders")
def orders():
    return spark.read.parquet("/data/orders/")

# With explicit schema (recommended for production)
@dp.materialized_view(name="bronze.orders_typed")
def orders_typed():
    from pyspark.sql.types import StructType, StructField, StringType, DoubleType
    schema = StructType([
        StructField("order_id", StringType()),
        StructField("amount", DoubleType()),
    ])
    return spark.read.schema(schema).parquet("/data/orders/")

# Streaming from directory
@dp.table(name="bronze.orders_stream")
def orders_stream():
    return (spark.readStream
        .format("parquet")
        .schema(ORDER_SCHEMA)  # Required for streaming
        .option("maxFilesPerTrigger", 100)
        .load("/data/incoming/"))
```

### JSON

```python
# Batch
@dp.materialized_view(name="bronze.events")
def events():
    return spark.read.json("/data/events/")

# With options
@dp.materialized_view(name="bronze.events_multiline")
def events_multiline():
    return (spark.read
        .option("multiLine", True)
        .option("mode", "PERMISSIVE")
        .json("/data/events/"))

# Streaming
@dp.table(name="bronze.events_stream")
def events_stream():
    return (spark.readStream
        .format("json")
        .schema(EVENT_SCHEMA)
        .load("/data/incoming/events/"))
```

### CSV

```python
# Batch with header
@dp.materialized_view(name="bronze.customers")
def customers():
    return (spark.read
        .option("header", True)
        .option("inferSchema", True)
        .csv("/data/customers/"))

# Production: explicit schema
@dp.materialized_view(name="bronze.customers_typed")
def customers_typed():
    return (spark.read
        .schema(CUSTOMER_SCHEMA)
        .option("header", True)
        .option("dateFormat", "yyyy-MM-dd")
        .csv("/data/customers/"))
```

### Avro

```python
# Requires: spark.jars.packages=org.apache.spark:spark-avro_2.13:4.1.0

@dp.materialized_view(name="bronze.users")
def users():
    return spark.read.format("avro").load("/data/users/")

# With schema from registry
@dp.materialized_view(name="bronze.users_registry")
def users_registry():
    return (spark.read
        .format("avro")
        .option("avroSchema", avro_schema_json)
        .load("/data/users/"))
```

### ORC

```python
@dp.materialized_view(name="bronze.logs")
def logs():
    return spark.read.orc("/data/logs/")
```

---

## 2. Open Table Formats

### Apache Iceberg

Full batch and streaming support. This is the default for lakehouse-stack.

```python
# Read from Iceberg table
@dp.materialized_view(name="silver.orders_clean")
def orders_clean():
    return (spark.table("iceberg.bronze.orders")
        .filter(f.col("order_id").isNotNull()))

# Time travel (specific snapshot)
@dp.materialized_view(name="silver.orders_historical")
def orders_historical():
    return (spark.read
        .option("snapshot-id", 10963874102873)
        .table("iceberg.bronze.orders"))

# Incremental reads (changes only)
@dp.materialized_view(name="silver.orders_changes")
def orders_changes():
    return (spark.read
        .option("start-snapshot-id", 10963874102873)
        .option("end-snapshot-id", 10963874102874)
        .table("iceberg.bronze.orders"))

# Streaming from Iceberg
@dp.table(name="silver.orders_stream")
def orders_stream():
    return (spark.readStream
        .format("iceberg")
        .option("stream-from-timestamp", "2024-01-01T00:00:00")
        .load("iceberg.bronze.orders"))

# Write to Iceberg (via foreachBatch for streaming)
# Note: SDP handles writes automatically for decorated functions
```

**Iceberg-Specific Features in SDP:**

| Feature | Support | Example |
|---------|---------|---------|
| Time travel | ✅ | `.option("snapshot-id", id)` |
| Incremental reads | ✅ | `.option("start-snapshot-id", id)` |
| Schema evolution | ✅ | Automatic |
| Partition evolution | ✅ | Via `partition_cols` |
| Hidden partitioning | ✅ | Iceberg handles automatically |
| Row-level deletes | ✅ | Standard Spark SQL |
| Merge-on-read | ✅ | Table property |

### Delta Lake

```python
# Requires: spark.jars.packages=io.delta:delta-spark_2.13:3.2.0
# Config: spark.sql.extensions=io.delta.sql.DeltaSparkSessionExtension
#         spark.sql.catalog.spark_catalog=org.apache.spark.sql.delta.catalog.DeltaCatalog

# Read from Delta table
@dp.materialized_view(name="silver.orders")
def orders():
    return spark.read.format("delta").load("/data/delta/orders/")

# Time travel by version
@dp.materialized_view(name="silver.orders_v10")
def orders_v10():
    return (spark.read
        .format("delta")
        .option("versionAsOf", 10)
        .load("/data/delta/orders/"))

# Time travel by timestamp
@dp.materialized_view(name="silver.orders_historical")
def orders_historical():
    return (spark.read
        .format("delta")
        .option("timestampAsOf", "2024-01-01")
        .load("/data/delta/orders/"))

# Streaming from Delta
@dp.table(name="bronze.orders_stream")
def orders_stream():
    return (spark.readStream
        .format("delta")
        .option("ignoreChanges", True)  # For updates/deletes
        .load("/data/delta/orders/"))

# Change Data Feed (CDC)
@dp.table(name="silver.orders_cdc")
def orders_cdc():
    return (spark.readStream
        .format("delta")
        .option("readChangeFeed", True)
        .option("startingVersion", 0)
        .load("/data/delta/orders/"))
```

**Delta Lake-Specific Features:**

| Feature | Support | Notes |
|---------|---------|-------|
| Time travel | ✅ | By version or timestamp |
| Schema evolution | ✅ | `mergeSchema` option |
| Change Data Feed | ✅ | `readChangeFeed` option |
| Z-ordering | ✅ | Via OPTIMIZE command |
| UniForm (Iceberg compat) | ✅ | Table property |

### Apache Hudi

```python
# Requires: spark.jars.packages=org.apache.hudi:hudi-spark3.5-bundle_2.12:0.15.0
# Config: spark.serializer=org.apache.spark.serializer.KryoSerializer
#         spark.sql.extensions=org.apache.spark.sql.hudi.HoodieSparkSessionExtension

# Read from Hudi table
@dp.materialized_view(name="silver.orders")
def orders():
    return spark.read.format("hudi").load("/data/hudi/orders/")

# Incremental query (changes since commit)
@dp.materialized_view(name="silver.orders_incremental")
def orders_incremental():
    return (spark.read
        .format("hudi")
        .option("hoodie.datasource.query.type", "incremental")
        .option("hoodie.datasource.read.begin.instanttime", "20240101000000")
        .load("/data/hudi/orders/"))

# Streaming from Hudi
@dp.table(name="bronze.orders_stream")
def orders_stream():
    return (spark.readStream
        .format("hudi")
        .option("hoodie.datasource.query.type", "incremental")
        .load("/data/hudi/orders/"))
```

**Hudi-Specific Features:**

| Feature | Support | Notes |
|---------|---------|-------|
| Copy-on-Write (CoW) | ✅ | Best for read-heavy |
| Merge-on-Read (MoR) | ✅ | Best for write-heavy |
| Incremental queries | ✅ | Efficient CDC |
| Record-level indexing | ✅ | Fast upserts |
| Clustering | ✅ | Optimize file sizes |

### Feature Comparison Matrix

| Feature | Iceberg | Delta | Hudi |
|---------|:-------:|:-----:|:----:|
| SDP batch read | ✅ | ✅ | ✅ |
| SDP streaming read | ✅ | ✅ | ✅ |
| Time travel | ✅ | ✅ | ✅ |
| Schema evolution | ✅ | ✅ | ✅ |
| Partition evolution | ✅ | ❌ | ❌ |
| Hidden partitioning | ✅ | ❌ | ❌ |
| Incremental/CDC | ✅ | ✅ | ✅ |
| ACID transactions | ✅ | ✅ | ✅ |
| Row-level operations | ✅ | ✅ | ✅ |
| Multi-engine support | ✅✅✅ | ✅✅ | ✅✅ |

---

## 3. Structured Streaming Sources

### Apache Kafka (Primary)

```python
from pyspark.sql.types import StructType, StructField, StringType, DoubleType

@dp.table(name="bronze.orders_stream")
def orders_stream():
    """Ingest orders from Kafka."""
    schema = StructType([
        StructField("order_id", StringType()),
        StructField("customer_id", StringType()),
        StructField("amount", DoubleType()),
        StructField("event_time", StringType()),
    ])

    return (spark.readStream
        .format("kafka")
        .option("kafka.bootstrap.servers", "localhost:9092")
        .option("subscribe", "orders")
        .option("startingOffsets", "latest")
        .option("maxOffsetsPerTrigger", 10000)  # Rate limiting
        .load()
        .select(
            f.from_json(f.col("value").cast("string"), schema).alias("data")
        )
        .select("data.*")
        .withColumn("event_timestamp", f.to_timestamp("event_time")))
```

**Kafka Options Reference:**

| Option | Description | Example |
|--------|-------------|---------|
| `kafka.bootstrap.servers` | Broker addresses | `"broker1:9092,broker2:9092"` |
| `subscribe` | Topic(s) to read | `"topic1,topic2"` |
| `subscribePattern` | Topic pattern | `"orders-.*"` |
| `startingOffsets` | Where to start | `"earliest"`, `"latest"` |
| `maxOffsetsPerTrigger` | Rate limit | `10000` |
| `kafka.group.id` | Consumer group | `"my-consumer-group"` |
| `kafka.security.protocol` | Security | `"SASL_SSL"` |

### Amazon Kinesis

```python
# Requires: spark.jars.packages=com.amazon.kinesis:spark-sql-kinesis_2.13:1.0.0

@dp.table(name="bronze.events_stream")
def events_stream():
    return (spark.readStream
        .format("kinesis")
        .option("streamName", "my-stream")
        .option("region", "us-east-1")
        .option("startingPosition", "TRIM_HORIZON")
        .option("awsAccessKeyId", aws_access_key)
        .option("awsSecretKey", aws_secret_key)
        .load())
```

### Google Pub/Sub

```python
# Requires: spark.jars.packages=com.google.cloud.spark:spark-pubsub_2.13:1.0.0

@dp.table(name="bronze.events_stream")
def events_stream():
    return (spark.readStream
        .format("pubsub")
        .option("projectId", "my-project")
        .option("subscriptionId", "my-subscription")
        .load())
```

### Azure Event Hubs

```python
# Requires: spark.jars.packages=com.microsoft.azure:azure-eventhubs-spark_2.13:2.3.22

@dp.table(name="bronze.events_stream")
def events_stream():
    connection_string = "Endpoint=sb://..."
    return (spark.readStream
        .format("eventhubs")
        .option("eventhubs.connectionString", connection_string)
        .option("eventhubs.consumerGroup", "$Default")
        .load())
```

### File-Based Streaming (Auto-Ingest)

```python
# Watch directory for new files
@dp.table(name="bronze.orders_stream")
def orders_stream():
    return (spark.readStream
        .format("parquet")  # or json, csv, orc
        .schema(ORDER_SCHEMA)
        .option("maxFilesPerTrigger", 100)
        .option("latestFirst", False)
        .load("/data/incoming/"))
```

### Testing Sources

```python
# Rate source - generates test data
@dp.table(name="test.rate_stream")
def rate_stream():
    return (spark.readStream
        .format("rate")
        .option("rowsPerSecond", 100)
        .option("numPartitions", 4)
        .load())
# Produces: timestamp (Timestamp), value (Long)

# Socket source - development only
@dp.table(name="test.socket_stream")
def socket_stream():
    return (spark.readStream
        .format("socket")
        .option("host", "localhost")
        .option("port", 9999)
        .load())
```

---

## 4. Streaming Aggregations with Watermarks

### Tumbling Windows

```python
@dp.table(name="gold.hourly_revenue")
def hourly_revenue():
    return (spark.table("iceberg.bronze.orders_stream")
        .withWatermark("event_timestamp", "10 minutes")
        .groupBy(
            f.window("event_timestamp", "1 hour")
        )
        .agg(
            f.sum("amount").alias("total_revenue"),
            f.count("order_id").alias("order_count")
        )
        .select(
            f.col("window.start").alias("window_start"),
            f.col("window.end").alias("window_end"),
            "total_revenue",
            "order_count"
        ))
```

### Session Windows

```python
@dp.table(name="gold.user_sessions")
def user_sessions():
    return (spark.table("iceberg.bronze.events_stream")
        .withWatermark("event_timestamp", "30 minutes")
        .groupBy(
            f.session_window("event_timestamp", "10 minutes"),
            "user_id"
        )
        .agg(
            f.count("*").alias("events_in_session"),
            f.min("event_timestamp").alias("session_start"),
            f.max("event_timestamp").alias("session_end")
        ))
```

### Stream-Stream Joins

```python
@dp.table(name="silver.orders_with_payments")
def orders_with_payments():
    orders = (spark.table("iceberg.bronze.orders_stream")
        .withWatermark("order_timestamp", "10 minutes"))

    payments = (spark.table("iceberg.bronze.payments_stream")
        .withWatermark("payment_timestamp", "10 minutes"))

    return orders.join(
        payments,
        f.expr("""
            order_id = payment_order_id AND
            payment_timestamp >= order_timestamp AND
            payment_timestamp <= order_timestamp + interval 1 hour
        """),
        "leftOuter"
    )
```

### Stream-Static Joins (Dimension Enrichment)

```python
# Batch dimension
@dp.materialized_view(name="bronze.dim_products")
def dim_products():
    return spark.read.parquet("/data/products/")

# Streaming fact with dimension join
@dp.table(name="silver.orders_enriched")
def orders_enriched():
    orders = spark.table("iceberg.bronze.orders_stream")
    products = spark.table("iceberg.bronze.dim_products")
    return orders.join(f.broadcast(products), "product_id", "left")
```

---

## 5. Output Modes and Sinks

### SDP Output Modes

| Mode | Use Case | Works With |
|------|----------|------------|
| **append** | New rows only | Aggregations with watermark, non-aggregated |
| **update** | Changed rows only | Aggregations |
| **complete** | Full result set | Small, bounded aggregations |

SDP automatically selects the appropriate output mode based on your transformations.

### Custom Sinks via foreachBatch

For destinations not directly supported:

```python
@dp.table(name="silver.orders_processed")
def orders_processed():
    """Stream to Iceberg with side-effect to PostgreSQL."""

    def write_to_postgres(batch_df, batch_id):
        (batch_df.write
            .format("jdbc")
            .option("url", "jdbc:postgresql://localhost:5432/mydb")
            .option("dbtable", "order_metrics")
            .option("user", "user")
            .option("password", "pass")
            .mode("append")
            .save())

    return (spark.table("iceberg.bronze.orders_stream")
        .writeStream
        .foreachBatch(write_to_postgres)
        .outputMode("append")
        .option("checkpointLocation", "/checkpoints/postgres-sink"))
```

---

## 6. Cloud Storage Protocols

| Cloud | Protocol | Example Path |
|-------|----------|--------------|
| AWS S3 | `s3a://` | `s3a://my-bucket/data/orders/` |
| Azure ADLS Gen2 | `abfss://` | `abfss://container@account.dfs.core.windows.net/data/` |
| Azure Blob | `wasbs://` | `wasbs://container@account.blob.core.windows.net/data/` |
| Google Cloud | `gs://` | `gs://my-bucket/data/orders/` |
| HDFS | `hdfs://` | `hdfs://namenode:8020/data/orders/` |
| Local | `file://` | `file:///data/orders/` |

```python
# S3 example
@dp.materialized_view(name="bronze.s3_orders")
def s3_orders():
    return spark.read.parquet("s3a://my-bucket/data/orders/")

# ADLS Gen2 example
@dp.materialized_view(name="bronze.adls_orders")
def adls_orders():
    return spark.read.parquet("abfss://container@account.dfs.core.windows.net/orders/")
```

---

## 7. What's NOT Supported in SDP

### Dependency Detection Limitations

| Pattern | Detected? | Workaround |
|---------|:---------:|------------|
| `spark.table("literal.name")` | ✅ | Use this pattern |
| `spark.table(variable)` | ❌ | Use literal strings |
| `spark.sql("SELECT ...")` | ❌ | Use DataFrame API |
| `spark.read.table(variable)` | ❌ | Use `spark.table()` |

### When NOT to Use SDP

| Scenario | Alternative |
|----------|-------------|
| One-off analysis | Jupyter notebooks |
| Complex control flow | Imperative scripts |
| Sub-second latency | Kafka Streams, Flink |
| Spark < 4.1 | Imperative PySpark |
| Heavy `spark.sql()` usage | Imperative (SQL not detected) |
| Runtime-determined dependencies | Imperative scripts |

---

## References

- [Spark Declarative Pipelines Programming Guide](https://spark.apache.org/docs/latest/declarative-pipelines-programming-guide.html)
- [Databricks Load Data in Pipelines](https://docs.databricks.com/aws/en/ldp/load)
- [Apache Iceberg Documentation](https://iceberg.apache.org/docs/latest/)
- [Delta Lake Documentation](https://delta.io/)
- [Apache Hudi Documentation](https://hudi.apache.org/)
- [Spark Structured Streaming Guide](https://spark.apache.org/docs/latest/structured-streaming-programming-guide.html)
- [Open Table Formats Comparison](https://lakefs.io/blog/hudi-iceberg-and-delta-lake-data-lake-table-formats-compared/)
