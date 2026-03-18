# Companion Guide: Spark 4.1 Features for Lakehouse

**Purpose:** Exhaustive technical reference for Act 3 of the OverArchitected show ("We Need Compute"). Covers every Spark 4.1 feature demonstrated in the show, with configuration walkthroughs, code examples, JIRA citations, and practical guidance for running OSS Spark on a self-hosted lakehouse stack.

**Audience:** Engineers familiar with Databricks Runtime (DBR) but not OSS Spark configuration. You know what a cluster is; you may not know where `spark-defaults.conf` lives or why you need five JARs to read an Iceberg table.

**Complements:** [03_spark_setup.py](/scripts/demos/overarchitected/03_spark_setup.py) (Act 3 demo script), [01_variant_iceberg.py](/scripts/demos/overarchitected/01_variant_iceberg.py) (VARIANT demo), [02_streaming_udtf.py](/scripts/demos/overarchitected/02_streaming_udtf.py) (UDTF demo), [03_full_overarchitected.py](/scripts/demos/overarchitected/03_full_overarchitected.py) (combined demo), [05b_spark_connect.py](/scripts/demos/overarchitected/05b_spark_connect.py) (Connect demo)

---

## Table of Contents

1. [Introduction: The Spark 4.x Era](#1-introduction-the-spark-4x-era)
2. [VARIANT Type Deep Dive](#2-variant-type-deep-dive)
3. [Recursive CTEs](#3-recursive-ctes)
4. [Collation Support](#4-collation-support)
5. [Spark Configuration for Lakehouse](#5-spark-configuration-for-lakehouse)
6. [Spark Connect](#6-spark-connect)
7. [Python UDTF (User-Defined Table Functions)](#7-python-udtf-user-defined-table-functions)
8. [Other Notable Spark 4.1 Features](#8-other-notable-spark-41-features)
9. [Migration from Spark 3.x to 4.x](#9-migration-from-spark-3x-to-4x)
10. [References](#10-references)

---

## 1. Introduction: The Spark 4.x Era

### The Largest Major Release in a Decade

Apache Spark 4.0.0 shipped on **May 28, 2025** -- the first major version bump since Spark 3.0 (June 2020). Spark 4.1.0 followed on **December 16, 2025**, bringing many 4.0 previews to GA status.

The combined 4.0 + 4.1 releases represent the most significant set of changes since the DataFrame API was introduced in Spark 1.3 (March 2015). The headline themes are:

| Theme | What Changed |
|-------|-------------|
| **SQL standards compliance** | ANSI mode on by default; 77+ new SQL functions in 4.1 |
| **Semi-structured data** | VARIANT type with shredding (binary JSON, schema-on-read) |
| **Declarative pipelines** | Spark Declarative Pipelines (SDP) -- DLT donated to OSS |
| **Low-latency streaming** | Real-Time Mode replaces deprecated Continuous Processing |
| **Client-server architecture** | Spark Connect GA with JDBC driver, thin Python client |
| **Python-first** | Arrow UDFs, UDTFs with TABLE args, Unix Domain Sockets |
| **Modern runtime** | Java 17+ (21 for 4.1), Scala 2.13 only, Python 3.10+ |

### Spark 4.0 vs 4.1: What Landed Where

Not everything shipped at once. Several 4.0 features were previews that became GA in 4.1:

| Feature | Spark 4.0 (May 2025) | Spark 4.1 (Dec 2025) |
|---------|----------------------|----------------------|
| ANSI mode default | **GA** | -- |
| VARIANT type | **GA** (basic) | **GA** (shredding) |
| Declarative Pipelines (SDP) | -- | **GA** |
| Recursive CTEs | Preview | **GA** |
| SQL Scripting | Preview | **GA** ([SPARK-54499](https://issues.apache.org/jira/browse/SPARK-54499)) |
| String Collation | **GA** | Expanded |
| Spark Connect | **GA** (Python, Scala) | JDBC driver, ML on Connect |
| Real-Time Mode | -- | **GA** ([SPARK-52330](https://issues.apache.org/jira/browse/SPARK-52330)) |
| Python UDTFs | **GA** (basic) | TABLE argument, Arrow UDTFs |
| Arrow UDFs (`arrow_udf`) | -- | **GA** ([SPARK-53014](https://issues.apache.org/jira/browse/SPARK-53014)) |
| K8s Operator | **GA** ([SPARK-45923](https://issues.apache.org/jira/browse/SPARK-45923)) | Improvements |

### Why This Matters for Lakehouse Self-Hosting

On Databricks Runtime (DBR), these features "just work" -- the platform handles configuration, JAR management, catalog setup, and version pinning. When you self-host, you inherit all of that responsibility:

- You choose the Iceberg runtime JAR and ensure it matches your Spark version
- You configure the catalog (JDBC or REST) and the storage backend (S3, HDFS, MinIO, SeaweedFS)
- You manage JAR conflicts (AWS SDK v1 vs v2 is a notorious pain point)
- You decide which JVM version to run (Java 17 for 4.0, Java 21 for 4.1)

This guide walks through each feature with the configuration context that DBR normally hides from you.

**Citations:** [Apache Spark 4.0.0 Release Notes](https://spark.apache.org/releases/spark-release-4-0-0.html), [Apache Spark 4.1.0 Release Notes](https://spark.apache.org/releases/spark-release-4.1.0.html)

---

## 2. VARIANT Type Deep Dive

### The Problem: Semi-Structured Data in Spark

Every event-driven system has the same problem: event payloads have different schemas per event type, but you want to store them in the same table.

Consider a food delivery platform (the OverArchitected demo's "Casper's Kitchen" data). The `orders` table has a `body` column containing JSON:

```json
// order_created event
{"brand_id": 1, "total": 25.99, "items": [{"name": "Burger", "price": 12.99}]}

// delivered event
{"delivery_lat": 37.77, "delivery_lon": -122.41, "total_mins": 45.0, "driver_id": "D42"}

// cancelled event
{"reason": "customer_request", "refund_amount": 25.99}
```

Before Spark 4.0, you had two options:

**Option A: Store as STRING, parse with `from_json()` on every read**

```python
# You must declare the schema upfront
body_schema = StructType([
    StructField("brand_id", IntegerType()),
    StructField("total", DoubleType()),
    StructField("driver_id", StringType()),
    # ... every possible field from every event type
])

# Parse on every query -- expensive and brittle
df.withColumn("parsed", from_json(col("body"), body_schema))
```

Problems:
1. Schema must be known upfront. New fields are silently dropped.
2. JSON is re-parsed on every read. No caching.
3. Union of all event schemas creates dozens of mostly-NULL columns.

**Option B: Separate tables per event type**

```python
spark.read.json("orders_created.json").write.saveAsTable("orders_created")
spark.read.json("orders_delivered.json").write.saveAsTable("orders_delivered")
# ... 8 event types = 8 tables
```

Problems:
1. Schema explosion (8 tables instead of 1).
2. Cross-event queries require UNION ALL across all tables.
3. Maintaining consistency is a nightmare.

### Enter VARIANT

**JIRA:** [SPARK-45891](https://issues.apache.org/jira/browse/SPARK-45891) -- "Support Variant data type"
**Creator:** Chenhao Li (Databricks), November 10, 2023
**Shipped:** Spark 4.0.0 (basic), Spark 4.1.0 (shredding GA)
**Parent SPIP:** [SPARK-46905](https://issues.apache.org/jira/browse/SPARK-46905) -- "Semi-Structured Data Processing with VARIANT type"

VARIANT is a new Spark SQL data type that stores semi-structured data in a **compact binary format**. It supports direct field access without re-parsing and does not require a schema declaration.

```sql
-- One type, any shape
CREATE TABLE events (id INT, payload VARIANT);

-- Parse JSON string to VARIANT
INSERT INTO events
VALUES (1, parse_json('{"user_id": 42, "event": "click"}'));

-- Access fields directly (no from_json, no schema)
SELECT payload.user_id, payload.event FROM events;
```

### Core Functions

#### `parse_json(string) -> VARIANT`

Strict parsing. Converts a JSON string to VARIANT binary format. **Throws an error** if the input is not valid JSON.

```sql
SELECT parse_json('{"brand_id": 1, "total": 25.99}');
-- Returns: VARIANT value (binary)

SELECT parse_json('not valid json');
-- ERROR: SparkRuntimeException: Cannot parse as JSON: 'not valid json'
```

**When to use:** When you control the data source and can guarantee valid JSON. Pipeline ingestion from validated API responses.

#### `try_parse_json(string) -> VARIANT`

Safe parsing. Returns **NULL** instead of throwing an error on invalid JSON.

```sql
SELECT try_parse_json('{"brand_id": 1}');
-- Returns: VARIANT value

SELECT try_parse_json('not valid json');
-- Returns: NULL

SELECT try_parse_json(NULL);
-- Returns: NULL

SELECT try_parse_json('{"trailing": "comma",}');
-- Returns: NULL (strict JSON, no trailing commas)
```

**When to use:** When processing data from external sources where JSON validity is not guaranteed. Kafka topics, third-party webhooks, user-generated content. **This is what we use in the OverArchitected demos.**

```python
# From 01_variant_iceberg.py and 03_spark_setup.py
df_variant = orders.withColumn("body_variant", f.try_parse_json("body"))
```

#### `variant_get(variant, path, type) -> typed_value`

Strict field extraction. Extracts a field from a VARIANT value using JSONPath syntax. **Throws an error** if the field exists but cannot be cast to the requested type.

```sql
SELECT variant_get(parse_json('{"total": 25.99}'), '$.total', 'double');
-- Returns: 25.99

SELECT variant_get(parse_json('{"total": "not_a_number"}'), '$.total', 'double');
-- ERROR: Cannot cast 'not_a_number' to DOUBLE

SELECT variant_get(parse_json('{"total": 25.99}'), '$.missing', 'double');
-- Returns: NULL (missing path returns NULL, not an error)
```

#### `try_variant_get(variant, path, type) -> typed_value`

Safe field extraction. Returns **NULL** instead of throwing an error on type cast failure.

```sql
SELECT try_variant_get(parse_json('{"total": 25.99}'), '$.total', 'double');
-- Returns: 25.99

SELECT try_variant_get(parse_json('{"total": "not_a_number"}'), '$.total', 'double');
-- Returns: NULL (no error)

SELECT try_variant_get(parse_json('{"items": [1,2,3]}'), '$.items[0]', 'int');
-- Returns: 1

SELECT try_variant_get(parse_json('{"nested": {"deep": 42}}'), '$.nested.deep', 'int');
-- Returns: 42
```

**When to use:** Whenever you are extracting fields from VARIANT in production code. The `try_` variant is strictly better for ETL because a single malformed record will not crash your entire pipeline.

```python
# From 03_spark_setup.py
df_extracted = df.withColumn(
    "brand_id", f.expr("try_variant_get(body_variant, '$.brand_id', 'int')")
).withColumn(
    "total", f.expr("try_variant_get(body_variant, '$.total', 'double')")
).withColumn(
    "driver_id", f.expr("try_variant_get(body_variant, '$.driver_id', 'string')")
).withColumn(
    "delivery_lat", f.expr("try_variant_get(body_variant, '$.delivery_lat', 'double')")
)
```

### JSONPath Syntax Reference

| Pattern | Meaning | Example |
|---------|---------|---------|
| `$.field` | Top-level field | `$.brand_id` |
| `$.nested.field` | Nested field | `$.address.city` |
| `$.array[n]` | Array index (0-based) | `$.items[0]` |
| `$.array[*]` | All array elements | `$.items[*].name` |

### Additional VARIANT Functions

| Function | Description | Example |
|----------|-------------|---------|
| `is_variant_null(v)` | Check if VARIANT value is JSON null | `is_variant_null(parse_json('null'))` -> `true` |
| `variant_explode(v)` | Explode VARIANT object/array into rows | Rows of (key, value) pairs |
| `schema_of_variant(v)` | Return the inferred schema | `schema_of_variant(parse_json('{"a":1}'))` -> `OBJECT<a: BIGINT>` |
| `schema_of_variant_agg(v)` | Aggregate schema across many values | Union of schemas across all rows |
| `variant_get(v, path)` | Without type: returns VARIANT | For nested extraction |

### Schema-on-Read Philosophy

VARIANT implements **schema-on-read** -- the data is stored without a fixed schema, and the schema is applied when you query it. This is the opposite of traditional relational databases (schema-on-write) where data must conform to a schema at write time.

The practical benefit:

```
Schema-on-write (from_json):
  1. Define schema before writing
  2. Schema changes require ALTER TABLE or new table
  3. Unknown fields are dropped silently
  4. JSON re-parsed on every read

Schema-on-read (VARIANT):
  1. Write any valid JSON, no schema needed
  2. Schema changes are transparent
  3. All fields preserved in binary
  4. Extract only what you need at query time
```

### The Binary Encoding

A VARIANT value is physically stored as two byte arrays:

1. **Metadata**: A dictionary of field name strings shared across all values in a column. Contains a version byte, dictionary size, offsets, and UTF-8 encoded field names.

2. **Value**: The encoded data with a type byte prefix:
   - `basic_type` 0: Primitive (null, boolean, int8-64, float, double, decimal, date, timestamp, string, binary, UUID -- 21 subtypes)
   - `basic_type` 1: Short string (up to 63 bytes, inline)
   - `basic_type` 2: Object (nested fields with dictionary-indexed field IDs)
   - `basic_type` 3: Array (ordered list of variant values)

This encoding is more compact than JSON (no repeated field names, no quoting, no escape characters) and supports O(1) field access through offset tables for random access into objects and arrays.

### Variant Shredding (GA in Spark 4.1)

**The problem:** Even with VARIANT's compact binary format, reading a single field from a VARIANT column requires loading the entire binary blob from disk. A query like `SELECT payload.user_id FROM events` reads the entire `payload` blob.

**The solution:** Shredding automatically extracts frequently-accessed fields and stores them as **separate typed Parquet columns**:

```
Physical Parquet layout with shredding:

event (VARIANT group)
+-- metadata (BYTE_ARRAY)        -- field name dictionary
+-- value (BYTE_ARRAY)           -- fallback for unshredded values
+-- typed_value (GROUP)          -- shredded fields
    +-- user_id (INT64)          -- direct typed column
    +-- event (STRING)           -- direct typed column
    +-- page (STRING)            -- direct typed column
```

When a query accesses `payload.user_id`, Parquet reads just the `user_id` column -- same I/O pattern as a regular typed column.

**Performance benchmarks:**

| Approach | Read Speed (vs JSON string) | Write Overhead |
|----------|----------------------------|----------------|
| JSON string + `from_json()` | 1x (baseline) | None |
| VARIANT without shredding | **8x faster** | Minimal |
| VARIANT with shredding | **30x faster** | 20-50% slower writes |

The shredding specification was adopted into the [Apache Parquet format](https://parquet.apache.org/docs/file-format/types/variantencoding/) with Parquet-Java 1.16.0.

### VARIANT vs from_json Performance

For a practical comparison on the OverArchitected demo data (5,000 rows, 8 event types):

```python
# OLD WAY: from_json with explicit schema
# Must know all possible fields across all 8 event types
body_schema = StructType([
    StructField("brand_id", IntegerType()),
    StructField("total", DoubleType()),
    StructField("driver_id", StringType()),
    StructField("delivery_lat", DoubleType()),
    StructField("delivery_lon", DoubleType()),
    StructField("total_mins", DoubleType()),
    StructField("reason", StringType()),
    StructField("refund_amount", DoubleType()),
    # ... more fields per event type
])
df.withColumn("parsed", from_json(col("body"), body_schema))
# Parse cost: O(n) string parsing per query
# Schema maintenance: manual, error-prone

# NEW WAY: VARIANT
df.withColumn("body_v", try_parse_json("body"))
  .withColumn("brand_id", expr("try_variant_get(body_v, '$.brand_id', 'int')"))
  .withColumn("total", expr("try_variant_get(body_v, '$.total', 'double')"))
# Parse cost: one-time binary encoding, O(1) field access thereafter
# Schema maintenance: none -- query what you need
```

At scale (millions of rows), the difference is dramatic. VARIANT avoids the repeated JSON parsing that `from_json()` performs on every query.

### Iceberg VARIANT Support Status

**Critical limitation for the lakehouse stack:** As of Iceberg 1.10.0 (the version used in this project), the Iceberg v2 table format does **not** natively support the VARIANT data type. This means you cannot write a VARIANT column directly to an Iceberg table.

The workaround used in the OverArchitected demos:

```python
# From 01_variant_iceberg.py
# Drop VARIANT column before Iceberg write
if "body_variant" in df_extracted.columns:
    df_extracted = df_extracted.drop("body_variant")

# Write the extracted (typed) columns instead
df_extracted.write.mode("overwrite").saveAsTable("iceberg.overarch.orders_variant")
```

**Workflow pattern:**

```
1. Read JSON body as STRING from source (Kafka, Parquet)
2. Parse to VARIANT in-memory: try_parse_json(body)
3. Extract typed fields: try_variant_get(body_variant, '$.field', 'type')
4. Drop the VARIANT column
5. Write typed columns to Iceberg
```

This gives you VARIANT's schema-on-read benefits during transformation while maintaining Iceberg compatibility for storage. Once Iceberg supports VARIANT natively (expected in a future spec version), you can store the VARIANT column directly and skip the extraction step.

**Parquet support:** VARIANT is supported natively in Parquet files (Parquet-Java 1.16.0+). If you are writing to Parquet directly (not through Iceberg), VARIANT columns work without issue. The limitation is specifically in the Iceberg metadata layer.

**Delta Lake:** Delta Lake added VARIANT support in delta-spark 4.0. If you are using Delta instead of Iceberg, VARIANT columns can be stored directly.

**Citations:** [SPARK-45891](https://issues.apache.org/jira/browse/SPARK-45891), [SPARK-46905](https://issues.apache.org/jira/browse/SPARK-46905), [Parquet Variant Encoding Specification](https://parquet.apache.org/docs/file-format/types/variantencoding/), [Databricks: Introducing Open Variant Data Type](https://www.databricks.com/blog/introducing-open-variant-data-type-delta-lake-and-apache-spark)

---

## 3. Recursive CTEs

### What Is a Recursive CTE?

A **Common Table Expression (CTE)** is a temporary named result set defined within a SQL statement. A **recursive CTE** is a CTE that references itself, enabling iterative computation in pure SQL.

**JIRA:** [SPARK-24497](https://issues.apache.org/jira/browse/SPARK-24497) -- "Support recursive CTE"
**Status:** Preview in Spark 4.0, **GA in Spark 4.1**

Before Spark 4.0, Spark SQL did not support `WITH RECURSIVE`. This was a significant gap compared to PostgreSQL, Oracle, SQL Server, and Snowflake, all of which have supported recursive CTEs for years. The workaround was to either:
- Write iterative Python/Scala loops calling Spark SQL repeatedly
- Use self-joins (limited to a known maximum depth)
- Collect data to the driver and process in Python

### Syntax

```sql
WITH RECURSIVE cte_name AS (
    -- Anchor member: the starting point (non-recursive)
    SELECT ...
    FROM base_table
    WHERE starting_condition

    UNION ALL

    -- Recursive member: references cte_name
    SELECT ...
    FROM source_table
    JOIN cte_name ON join_condition
)
SELECT * FROM cte_name;
```

The engine executes this as follows:

1. **Evaluate the anchor member** -- produces the initial result set
2. **Evaluate the recursive member** using the previous iteration's result
3. **Repeat step 2** until the recursive member produces no new rows
4. **UNION ALL** all intermediate results into the final output

### Use Case 1: Order Event Chain Traversal

The OverArchitected demo data models food delivery orders as a sequence of events linked by `order_id` and `sequence` number:

```
order_created (seq 0) -> kitchen_confirmed (seq 1) -> preparing (seq 2) ->
ready (seq 3) -> driver_assigned (seq 4) -> picked_up (seq 5) ->
en_route (seq 6) -> delivered (seq 7)
```

A recursive CTE traverses this chain:

```sql
WITH RECURSIVE event_chain AS (
    -- Anchor: start with order_created events (sequence 0)
    SELECT order_id, event_id, event_type, event_timestamp, sequence,
           1 AS depth
    FROM order_events
    WHERE sequence = 0 AND event_type = 'order_created'

    UNION ALL

    -- Recursive: follow the chain by incrementing sequence
    SELECT e.order_id, e.event_id, e.event_type, e.event_timestamp,
           e.sequence, c.depth + 1
    FROM order_events e
    JOIN event_chain c
      ON e.order_id = c.order_id
      AND e.sequence = c.sequence + 1
)
SELECT order_id, event_type, event_timestamp, depth
FROM event_chain
WHERE order_id IN (
    SELECT DISTINCT order_id FROM order_events
    WHERE sequence = 0 LIMIT 3
)
ORDER BY order_id, depth;
```

Output:

```
+--------+-------------------+-------------------+-----+
|order_id|event_type         |event_timestamp    |depth|
+--------+-------------------+-------------------+-----+
|ORD001  |order_created      |2024-01-15 12:00:00|    1|
|ORD001  |kitchen_confirmed  |2024-01-15 12:02:00|    2|
|ORD001  |preparing          |2024-01-15 12:05:00|    3|
|ORD001  |ready              |2024-01-15 12:18:00|    4|
|ORD001  |driver_assigned    |2024-01-15 12:20:00|    5|
|ORD001  |picked_up          |2024-01-15 12:25:00|    6|
|ORD001  |en_route           |2024-01-15 12:30:00|    7|
|ORD001  |delivered          |2024-01-15 12:45:00|    8|
+--------+-------------------+-------------------+-----+
```

### Use Case 2: Organizational Hierarchy

The classic recursive CTE use case -- traversing a tree structure stored as parent-child references:

```sql
WITH RECURSIVE org_hierarchy AS (
    -- Anchor: top-level managers (no manager)
    SELECT id, name, manager_id, 1 AS level,
           CAST(name AS STRING) AS path
    FROM employees
    WHERE manager_id IS NULL

    UNION ALL

    -- Recursive: employees reporting to someone in the previous level
    SELECT e.id, e.name, e.manager_id, h.level + 1,
           CONCAT(h.path, ' > ', e.name) AS path
    FROM employees e
    JOIN org_hierarchy h ON e.manager_id = h.id
)
SELECT level, name, path FROM org_hierarchy ORDER BY path;
```

### Use Case 3: Bill of Materials Explosion

Manufacturing/assembly hierarchies where parts contain sub-parts:

```sql
WITH RECURSIVE bom AS (
    -- Anchor: top-level product
    SELECT part_id, part_name, parent_part_id, quantity, 1 AS level
    FROM parts
    WHERE part_id = 'BIKE-001'

    UNION ALL

    -- Recursive: sub-components
    SELECT p.part_id, p.part_name, p.parent_part_id,
           p.quantity * b.quantity AS total_quantity,
           b.level + 1
    FROM parts p
    JOIN bom b ON p.parent_part_id = b.part_id
)
SELECT * FROM bom ORDER BY level, part_name;
```

### Use Case 4: Graph Shortest Path

Finding paths between nodes in a graph (social network, network topology):

```sql
WITH RECURSIVE paths AS (
    SELECT source, target, 1 AS hops,
           ARRAY(source, target) AS path
    FROM edges
    WHERE source = 'node_A'

    UNION ALL

    SELECT p.source, e.target, p.hops + 1,
           ARRAY_UNION(p.path, ARRAY(e.target))
    FROM paths p
    JOIN edges e ON p.target = e.source
    WHERE NOT ARRAY_CONTAINS(p.path, e.target)  -- prevent cycles
      AND p.hops < 10                           -- depth limit
)
SELECT * FROM paths WHERE target = 'node_Z' ORDER BY hops LIMIT 1;
```

### Performance Considerations

**Max recursion depth:** Spark does not have a built-in `MAXRECURSION` hint like SQL Server. You must implement depth limiting yourself:

```sql
-- Add a depth counter and filter in the recursive member
WITH RECURSIVE cte AS (
    SELECT ..., 1 AS depth FROM ...
    UNION ALL
    SELECT ..., c.depth + 1
    FROM ... JOIN cte c ON ...
    WHERE c.depth < 100  -- prevent infinite recursion
)
```

Without a depth limit, a cycle in your data (A -> B -> A) will cause infinite recursion and eventually an OOM error.

**Performance characteristics:**

| Factor | Impact |
|--------|--------|
| Depth of recursion | Each level adds a join + shuffle. Deep recursions (>50 levels) are expensive. |
| Width per level | Fanout (many children per parent) multiplies rows at each level exponentially. |
| Data skew | If one parent has millions of children, that partition dominates. |
| Join strategy | Spark uses broadcast join if the recursive result is small enough; otherwise sort-merge. |

**Best practices:**
1. Always include a depth limit in the recursive member
2. Filter early (restrict the anchor to relevant starting points)
3. Use `UNION ALL` (not `UNION`) -- `UNION` deduplicates at each level, adding overhead
4. Watch for data skew in deep hierarchies -- consider pre-computing frequently accessed paths

**Comparison: Before and After Recursive CTEs**

```python
# BEFORE (Spark 3.x): Python loop with repeated SQL
result = spark.sql("SELECT * FROM events WHERE sequence = 0")
for depth in range(1, max_depth):
    next_level = spark.sql(f"""
        SELECT e.* FROM events e
        JOIN result_view r ON e.order_id = r.order_id
        AND e.sequence = r.sequence + 1
    """)
    result = result.union(next_level)
    if next_level.count() == 0:
        break
# Problems: N round-trips to Spark, no query plan optimization across iterations

# AFTER (Spark 4.1): One SQL statement
result = spark.sql("""
    WITH RECURSIVE event_chain AS (
        SELECT * FROM events WHERE sequence = 0
        UNION ALL
        SELECT e.* FROM events e
        JOIN event_chain c ON e.order_id = c.order_id
          AND e.sequence = c.sequence + 1
    )
    SELECT * FROM event_chain
""")
# Single optimized plan, no Python round-trips
```

**Citations:** [SPARK-24497](https://issues.apache.org/jira/browse/SPARK-24497), [SQL:1999 Standard (ISO/IEC 9075-2)](https://www.iso.org/standard/26197.html), [PostgreSQL: WITH Queries](https://www.postgresql.org/docs/current/queries-with.html)

---

## 4. Collation Support

### What Is Collation?

A **collation** defines the rules for comparing and sorting strings -- whether comparisons are case-sensitive, accent-sensitive, and what locale-specific ordering rules apply.

**JIRA:** [SPARK-46830](https://issues.apache.org/jira/browse/SPARK-46830) -- "Support string collation"
**Introduced:** Spark 4.0, expanded in Spark 4.1

Before Spark 4.0, all string comparisons were **binary** -- byte-for-byte comparison of UTF-8 encoded strings. `"Pizza"` and `"pizza"` were always different values. This matched MySQL's `utf8_bin` collation but differed from PostgreSQL (case-sensitive by default but with collation support) and SQL Server (case-insensitive by default).

### The Problem

```sql
-- Find all pizza brands
SELECT * FROM brands WHERE name LIKE '%pizza%';
-- Misses: "Pizza Palace", "PIZZA Express", "Deep Dish Pizza"

-- Common workaround: LOWER() everywhere
SELECT * FROM brands WHERE LOWER(name) LIKE '%pizza%';
-- Works, but:
--   1. LOWER() creates a temporary copy of every string
--   2. Cannot use indexes (full table scan required)
--   3. Must remember to add LOWER() to every comparison
--   4. Performance degrades with table size
```

### Available Collations

| Collation | Behavior | Performance | Use Case |
|-----------|----------|-------------|----------|
| `UTF8_BINARY` | Byte-level comparison (default) | Fastest | Exact matching, hashes, IDs |
| `UTF8_LCASE` | Case-insensitive (lowercase comparison) | ~22x faster than `LOWER()` | Names, labels, search |
| ICU collations | Full locale-aware sorting | Slower (ICU library overhead) | International text, legal/financial |

### Using Collation

**Inline collation (per-expression):**

```sql
-- Case-insensitive comparison without LOWER()
SELECT name, cuisine_type
FROM brands
WHERE name COLLATE UTF8_LCASE LIKE '%pizza%'
   OR name COLLATE UTF8_LCASE LIKE '%burger%';
```

This is the approach used in the OverArchitected demo ([03_spark_setup.py](/scripts/demos/overarchitected/03_spark_setup.py), [03_full_overarchitected.py](/scripts/demos/overarchitected/03_full_overarchitected.py)).

**Column-level collation (at table creation):**

```sql
-- Set collation at table creation
CREATE TABLE users (
    id INT,
    name STRING COLLATE UTF8_LCASE,
    email STRING COLLATE UTF8_LCASE
);

-- All comparisons on name/email are now case-insensitive automatically
SELECT * FROM users WHERE name = 'john';
-- Matches: 'John', 'JOHN', 'john', 'jOhN'

SELECT * FROM users WHERE email = 'USER@EXAMPLE.COM';
-- Matches: 'user@example.com', 'User@Example.Com', etc.
```

**Session-level default collation:**

```sql
SET spark.sql.session.collation = 'UTF8_LCASE';
-- All new string comparisons in this session use UTF8_LCASE
```

### Performance: UTF8_LCASE vs LOWER()

`UTF8_LCASE` performs case-insensitive comparison up to **22x faster** than `LOWER()` because:

1. **No temporary allocation:** `LOWER()` creates a new lowercase copy of every string. `UTF8_LCASE` compares characters in place.
2. **Short-circuit evaluation:** `UTF8_LCASE` can bail out early when characters differ. `LOWER()` must lowercase the entire string first.
3. **Index compatibility:** A column defined with `COLLATE UTF8_LCASE` can potentially be indexed for case-insensitive lookups (index support is engine-dependent).

### Collation and Equality

Collation affects all comparison operators:

```sql
-- With UTF8_BINARY (default):
SELECT 'Pizza' = 'pizza';           -- false
SELECT 'Pizza' < 'pizza';           -- true (uppercase letters sort before lowercase in UTF-8)

-- With UTF8_LCASE:
SELECT 'Pizza' COLLATE UTF8_LCASE = 'pizza' COLLATE UTF8_LCASE;  -- true
SELECT 'Pizza' COLLATE UTF8_LCASE < 'pizza' COLLATE UTF8_LCASE;  -- false (equal)
```

### Collation and GROUP BY / DISTINCT

```sql
-- Without collation: "Pizza" and "pizza" are different groups
SELECT name, COUNT(*) FROM orders GROUP BY name;
-- Pizza Palace  -> 100
-- pizza palace  -> 3
-- PIZZA PALACE  -> 1

-- With collation: they merge into one group
SELECT name COLLATE UTF8_LCASE AS name, COUNT(*)
FROM orders
GROUP BY name COLLATE UTF8_LCASE;
-- pizza palace  -> 104
```

### ICU Collations for International Text

For locale-aware sorting (German umlauts, Turkish dotted-i, CJK ordering), Spark supports ICU (International Components for Unicode) collations:

```sql
-- German: a with umlaut sorts after a, not after z
SELECT name FROM products ORDER BY name COLLATE 'de';

-- Turkish: dotted I (I) and dotless i (i) are different letters
SELECT * FROM users WHERE name COLLATE 'tr' = 'istanbul';
```

ICU collations are significantly slower than `UTF8_BINARY` or `UTF8_LCASE` because they invoke the ICU library for every comparison. Use them only when locale-specific behavior is required.

### Practical Advice for Lakehouse

1. **Default to `UTF8_BINARY`** for IDs, hashes, and machine-generated strings
2. **Use `UTF8_LCASE`** for human-readable text columns (names, labels, descriptions)
3. **Apply collation at the column level** (`CREATE TABLE`) rather than inline (`COLLATE` in every query) -- this avoids the "forgot to add LOWER()" class of bugs
4. **Be aware of join implications:** If you join two tables on a string column and they have different collations, Spark will error. Ensure both sides use the same collation.

**Citations:** [SPARK-46830](https://issues.apache.org/jira/browse/SPARK-46830), [Unicode Technical Standard #10 (Unicode Collation Algorithm)](https://unicode.org/reports/tr10/), [ICU Project](https://icu.unicode.org/)

---

## 5. Spark Configuration for Lakehouse

### The Configuration Problem

On Databricks, you create a cluster and it works. You select "Iceberg" as your table format and Databricks handles catalog registration, JAR management, S3 credentials, and version compatibility.

On OSS Spark, you configure all of this yourself. The configuration file is `spark-defaults.conf` (or command-line `--conf` flags), and getting it right is the single biggest friction point for self-hosting.

### spark-defaults.conf: Full Walkthrough

Here is the complete configuration for the lakehouse stack, annotated section by section.

#### Section 1: Iceberg Catalog

```properties
# Register Iceberg as a named catalog
spark.sql.extensions                org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions
spark.sql.catalog.iceberg           org.apache.iceberg.spark.SparkCatalog
```

`spark.sql.extensions` loads Iceberg's SQL extensions (stored procedures like `CALL iceberg.system.rewrite_data_files()`, DDL support, etc.). `spark.sql.catalog.iceberg` registers a catalog named `iceberg` backed by Iceberg's `SparkCatalog` class.

After this, you access tables as `iceberg.namespace.table` (e.g., `iceberg.bronze.orders`).

#### Section 2: Catalog Backend (Choose One)

**Option A: JDBC Catalog (Direct to PostgreSQL)**

```properties
spark.sql.catalog.iceberg.type                  jdbc
spark.sql.catalog.iceberg.uri                   jdbc:postgresql://localhost:5432/iceberg_catalog
spark.sql.catalog.iceberg.jdbc.user             iceberg
spark.sql.catalog.iceberg.jdbc.password         iceberg_password
spark.sql.catalog.iceberg.jdbc.schema-version   V1
```

The JDBC catalog stores Iceberg metadata directly in PostgreSQL tables. This is the **simplest** option -- no additional services needed beyond the PostgreSQL container you are already running.

**Pros:** Minimal infrastructure, no extra services, works immediately.
**Cons:** Spark-only (other engines like DuckDB, Trino cannot share the catalog), no credential vending.

**Option B: REST Catalog (Unity Catalog)**

```properties
spark.sql.catalog.iceberg.catalog-impl    org.apache.iceberg.rest.RESTCatalog
spark.sql.catalog.iceberg.uri             http://localhost:8080/api/2.1/unity-catalog/iceberg
spark.sql.catalog.iceberg.warehouse       unity
spark.sql.catalog.iceberg.token           not_used
```

The REST catalog communicates with Unity Catalog OSS over HTTP. Unity Catalog serves as a shared metadata layer that multiple engines can access.

**Pros:** Multi-engine (DuckDB, Trino, Dremio can all query your tables), credential vending (no hardcoded S3 keys in Spark config), access control.
**Cons:** Additional service to run and maintain, more moving parts.

**Which to choose:** Start with JDBC for simplicity. Move to REST when you need multi-engine access or credential vending.

#### Section 3: Storage (S3-Compatible)

```properties
spark.sql.catalog.iceberg.warehouse             s3a://lakehouse/warehouse
spark.hadoop.fs.s3a.endpoint                    http://localhost:8333
spark.hadoop.fs.s3a.access.key                  admin
spark.hadoop.fs.s3a.secret.key                  admin_password
spark.hadoop.fs.s3a.path.style.access           true
spark.hadoop.fs.s3a.impl                        org.apache.hadoop.fs.s3a.S3AFileSystem
spark.hadoop.fs.s3a.connection.ssl.enabled       false
```

This configures Spark to use SeaweedFS (or MinIO, or any S3-compatible object store) as the storage backend.

Key parameters:

| Parameter | Meaning | Notes |
|-----------|---------|-------|
| `fs.s3a.endpoint` | Object store URL | SeaweedFS: port 8333. MinIO: port 9000. AWS S3: omit (uses default) |
| `fs.s3a.path.style.access` | Use `endpoint/bucket/key` instead of `bucket.endpoint/key` | Required for SeaweedFS/MinIO. AWS S3: `false` |
| `fs.s3a.connection.ssl.enabled` | HTTPS | `false` for local development. `true` for production/AWS |
| `fs.s3a.impl` | Hadoop filesystem implementation | Always `org.apache.hadoop.fs.s3a.S3AFileSystem` |

**AWS S3 (real):** Remove the `endpoint` and `path.style.access` lines. Use IAM roles instead of access keys when possible.

#### Section 4: JARs (The Pain Point)

```properties
spark.jars  /opt/spark/jars-extra/iceberg-spark-runtime-4.0_2.13-1.10.0.jar,\
            /opt/spark/jars-extra/hadoop-aws-3.4.1.jar,\
            /opt/spark/jars-extra/aws-java-sdk-bundle-1.12.780.jar,\
            /opt/spark/jars-extra/bundle-2.24.6.jar,\
            /opt/spark/jars-extra/postgresql-42.7.4.jar
```

This is the part that causes the most pain when self-hosting. Each JAR has version constraints and compatibility requirements:

| JAR | Version | Size | Purpose | Version Constraint |
|-----|---------|------|---------|-------------------|
| `iceberg-spark-runtime-4.0_2.13` | 1.10.0 | ~90 MB | Iceberg integration | Must match Spark major version (4.0 runtime works with 4.1) |
| `hadoop-aws` | 3.4.1 | ~1 MB | S3A filesystem | Must match Hadoop version bundled with Spark |
| `aws-java-sdk-bundle` | 1.12.780 | ~350 MB | AWS SDK v1 | Required by `hadoop-aws` |
| `bundle` (AWS SDK v2) | 2.24.6 | ~400 MB | AWS SDK v2 | **Exact version required for Hadoop 3.4.1** |
| `postgresql` | 42.7.4 | ~1 MB | JDBC driver | Any 42.x works |

**Why exact versions matter:** The AWS SDK v2 bundle version must be **exactly 2.24.6** for Hadoop 3.4.1. A newer version (e.g., 2.25.x) will cause `NoSuchMethodError` at runtime because Hadoop 3.4.1's `S3AFileSystem` was compiled against the 2.24.6 API. This is the most common "it works on Databricks but breaks on OSS" issue.

**Total JAR size:** ~860 MB. This is why the download script (`scripts/tools/download-jars.sh`) has retry logic and size verification.

**Downloading JARs:**

```bash
# Download all required JARs
./scripts/tools/download-jars.sh

# Verify existing JARs (CI mode)
./scripts/tools/download-jars.sh --verify-only
```

#### Section 5: Performance Tuning

```properties
spark.driver.memory                 4g
spark.executor.memory               8g
spark.executor.cores                2
spark.sql.warehouse.dir             /opt/spark-data/warehouse
```

For local development, 4 GB driver and 8 GB executor memory is sufficient. Production deployments should be sized based on data volume.

#### Section 6: Monitoring

```properties
spark.eventLog.enabled              true
spark.eventLog.dir                  /opt/spark-data/logs
spark.history.fs.logDirectory       /opt/spark-data/logs
```

Event logs enable the Spark History Server to display completed job details. In Spark 4.0+, `spark.eventLog.rolling.enabled` defaults to `true` (log files are rotated automatically).

### Critical Version Pins

These versions are pinned in the lakehouse stack and should not be changed without thorough testing:

| Component | Pinned Version | Why |
|-----------|---------------|-----|
| **Iceberg** | 1.10.0 | Latest stable. Runtime JAR must match Spark major version. |
| **AWS SDK v2** | 2.24.6 | Exact match required for Hadoop 3.4.1 compatibility |
| **Spark** | 4.0.1 or 4.1.0 | Scala 2.13 builds. 4.0.1 uses Java 17, 4.1.0 uses Java 21. |
| **Hadoop** | 3.4.1 (4.0) / 3.4.2 (4.1) | Bundled with Spark. Determines AWS SDK version. |
| **PostgreSQL JDBC** | 42.7.4 | Any 42.x works, but pinned for reproducibility. |

### Configuration Comparison: DBR vs OSS

| Aspect | Databricks Runtime | OSS Spark + Lakehouse Stack |
|--------|-------------------|----------------------------|
| Catalog setup | Click "Iceberg" in UI | 6-8 lines in spark-defaults.conf |
| Storage | Managed by workspace | Configure S3 endpoint + credentials |
| JARs | Pre-installed | Download ~860 MB, pin versions |
| Credentials | Unity Catalog vending | Hardcoded or env vars |
| Monitoring | Built-in Ganglia/metrics | Event logs + History Server |
| Version management | DBR version = everything | Pin Iceberg, AWS SDK, Hadoop independently |

### Environment Variables vs Config File

The lakehouse stack supports both approaches:

```bash
# .env file (used by docker-compose)
ICEBERG_CATALOG_URI=jdbc:postgresql://localhost:5432/iceberg_catalog
S3_ENDPOINT=http://localhost:8333
S3_ACCESS_KEY=admin
S3_SECRET_KEY=admin_password

# spark-defaults.conf (used by Spark)
# References the same values but in Spark's property format
```

The `lakehouse` CLI script bridges these: `./lakehouse check-config` validates that `.env` and `spark-defaults.conf` are consistent.

**Citations:** [Iceberg Spark Configuration](https://iceberg.apache.org/docs/latest/spark-configuration/), [Hadoop S3A Configuration](https://hadoop.apache.org/docs/current/hadoop-aws/tools/hadoop-aws/index.html), [Spark Configuration Documentation](https://spark.apache.org/docs/latest/configuration.html)

---

## 6. Spark Connect

### What Is Spark Connect?

Spark Connect is a **client-server protocol** that decouples your application from the Spark driver JVM. Instead of embedding the full Spark runtime (300+ MB, including a JVM) in your Python process, you send queries to a remote Spark server over gRPC and receive results as Apache Arrow streams.

**SPIP:** [SPARK-39375](https://issues.apache.org/jira/browse/SPARK-39375) -- "SPIP: Spark Connect - A client and server interface for Apache Spark"
**Creator:** Martin Grund (Databricks), June 3, 2022
**Shipped:** Spark 3.4 (experimental), Spark 4.0 (GA), Spark 4.1 (JDBC driver, ML GA)

### Architecture

```
Traditional Spark (Classic Mode):
+------------------------------------------+
| Your Code + SparkSession + JVM Driver    | <-- Everything in one process
| (PySpark uses Py4J to talk to JVM)       |
|         |                                |
|    Executors on workers                  |
+------------------------------------------+

Spark Connect (Client-Server Mode):
+--------------+        +--------------------------+
| Your Laptop  |  gRPC  | SparkConnectServer       |
| (Python)     |------->| (embedded in driver)     |
| No JVM!      |<-------| (runs on cluster)        |
| 1.5 MB       | Arrow  | Executors on workers     |
+--------------+        +--------------------------+
```

**Protocol details:**

| Direction | Protocol | Format | Purpose |
|-----------|----------|--------|---------|
| Client -> Server | gRPC (HTTP/2) | Protocol Buffers | Query plans, config, artifacts |
| Server -> Client | gRPC (HTTP/2) | Apache Arrow IPC | Result data |

**Default port:** 15002
**Connection string:** `sc://host:15002` (with optional parameters: `sc://host:15002/;token=xyz;user_agent=myapp`)

### The Connection Progression

This is the key insight from the OverArchitected demo: **your DataFrame code is identical at every level.** Only the session builder changes.

```python
# Level 1: Local (laptop, single JVM)
spark = SparkSession.builder \
    .master("local[*]") \
    .getOrCreate()

# Level 2: Standalone Cluster (spark-submit to cluster)
spark = SparkSession.builder \
    .master("spark://spark-master-41:7078") \
    .getOrCreate()

# Level 3: Spark Connect (thin client to cluster)
spark = SparkSession.builder \
    .remote("sc://spark-master-41:15002") \
    .getOrCreate()

# Level 4: Kubernetes + Spark Connect
spark = SparkSession.builder \
    .remote("sc://k8s-loadbalancer:15002") \
    .getOrCreate()

# SAME CODE AT EVERY LEVEL:
df = spark.table("iceberg.bronze.orders")
result = df.filter(col("event_type") == "order_created") \
    .groupBy("location_id") \
    .agg(count("*").alias("order_count"))
result.show()
```

### Server Setup

```bash
# Start the Spark Connect server on your cluster
/opt/spark/sbin/start-connect-server.sh \
    --master spark://spark-master-41:7078 \
    --jars /opt/spark/jars-extra/iceberg-spark-runtime-4.0_2.13-1.10.0.jar,\
           /opt/spark/jars-extra/bundle-2.24.6.jar \
    --conf spark.sql.catalog.iceberg=org.apache.iceberg.spark.SparkCatalog \
    --conf spark.sql.catalog.iceberg.type=jdbc \
    --conf spark.sql.catalog.iceberg.uri=jdbc:postgresql://postgres:5432/iceberg_catalog

# Stop it
/opt/spark/sbin/stop-connect-server.sh
```

The Connect server is a regular Spark driver process with an embedded gRPC server. It accepts the same `--conf` flags as `spark-submit`.

**Docker example (lakehouse stack):**

```bash
# Start Connect server inside the Spark master container
docker exec spark-master-41 /opt/spark/sbin/start-connect-server.sh \
    --master spark://spark-master-41:7078

# Port 15002 is exposed in docker-compose-spark41.yml
```

### Client Setup

**Thin client (1.5 MB):**

```bash
# Install ONLY the client -- no JVM, no Spark runtime
pip install pyspark-client

# Connect to the server
python -c "
from pyspark.sql import SparkSession
spark = SparkSession.builder.remote('sc://localhost:15002').getOrCreate()
spark.sql('SELECT COUNT(*) FROM iceberg.bronze.orders').show()
"
```

The `pyspark-client` package is ~1.5 MB and contains only the gRPC client stub and Arrow deserialization logic. Compare this to the full `pyspark` package (300+ MB including a bundled JVM).

**Full PySpark install also works:**

```bash
# Full PySpark can also use Connect mode
pip install pyspark
python -c "
from pyspark.sql import SparkSession
spark = SparkSession.builder.remote('sc://localhost:15002').getOrCreate()
# Same API, same behavior
"
```

### What Works and What Does Not

| Feature | Classic Mode | Spark Connect |
|---------|-------------|---------------|
| DataFrame operations | Yes | Yes |
| SQL queries | Yes | Yes |
| Python UDFs | Yes | Yes |
| Pandas UDFs | Yes | Yes |
| Structured Streaming | Yes | Yes |
| Catalog operations (SHOW TABLES, etc.) | Yes | Yes |
| MLlib (pyspark.ml) | Yes | Yes (GA in 4.1) |
| **RDD API** | Yes | **No** |
| **SparkContext access** | Yes | **No** |
| **df._jdf (JVM internals)** | Yes | **No** |
| **Custom Catalyst rules** | Yes | **No** |
| **Py4J direct access** | Yes | **No** |
| **setLogLevel** | Yes | May not work |

**The 95% rule:** For 95% of data engineering and analytics work (DataFrames, SQL, streaming, ML), Spark Connect is functionally identical to classic mode. The 5% that does not work is low-level RDD manipulation and direct JVM access -- things you should not be doing in modern PySpark anyway.

### gRPC Protocol Details

The Spark Connect protocol defines these RPC calls:

| RPC | Purpose |
|-----|---------|
| `ExecutePlan` | Send an unresolved logical plan for execution; results stream back as Arrow |
| `AnalyzePlan` | Analyze a plan without executing (schema inference, explain) |
| `Config` | Get/Set Spark configuration |
| `AddArtifacts` | Upload JARs, Python files, archives |
| `Interrupt` | Cancel a running operation |
| `ReattachExecute` | Reconnect to a running execution after disconnect |

**How a query flows:**

```
1. Client: spark.table("orders").filter(col("total") > 50)
2. Client translates to an unresolved logical plan (Protobuf)
3. Client sends ExecutePlan RPC via gRPC
4. Server: receives plan, resolves table names, runs Catalyst optimizer
5. Server: executes physical plan (distributed across executors)
6. Server: streams Arrow-encoded RecordBatches back over gRPC
7. Client: reconstructs DataFrame / collects results
```

### Spark Connect Timeline

| Date | Milestone | JIRA |
|------|-----------|------|
| June 3, 2022 | SPIP proposal created | [SPARK-39375](https://issues.apache.org/jira/browse/SPARK-39375) |
| June 16, 2022 | SPIP vote passes unanimously | -- |
| April 2023 | **Spark 3.4**: Python client ships (experimental) | -- |
| September 2023 | **Spark 3.5**: Scala client, Go client, streaming support | -- |
| May 2025 | **Spark 4.0**: `pyspark-client` (1.5 MB), full Java client, ML support | -- |
| December 2025 | **Spark 4.1**: JDBC driver, ML on Connect GA, Zstd-compressed plans | [SPARK-53484](https://issues.apache.org/jira/browse/SPARK-53484) |

### The JDBC Driver (Spark 4.1)

**JIRA:** [SPARK-53484](https://issues.apache.org/jira/browse/SPARK-53484)

Spark 4.1 adds a SQL-standard JDBC driver for Spark Connect. Any JDBC-compatible tool (DBeaver, DataGrip, Tableau, custom Java applications) can now connect to Spark without Spark-specific libraries:

```
JDBC URL: jdbc:spark://host:15002
Driver class: org.apache.spark.sql.jdbc.SparkDriver
```

This opens Spark to the entire BI/Java ecosystem without requiring PySpark or the Scala client.

### Security Considerations

Spark Connect does **not** include built-in authentication or authorization. In production:

1. **Network isolation:** Run the Connect server behind a VPN or private network
2. **gRPC proxy:** Use Envoy or nginx as a gRPC proxy for TLS termination and token-based auth
3. **Spark ACLs:** Enable `spark.acls.enable=true` for user-level access control
4. **Token authentication:** Use `sc://host:15002/;token=YOUR_TOKEN` connection strings with a custom authenticator plugin

### Why Not Py4J? (The Old Model)

For context on what Connect replaces: traditional PySpark uses **Py4J** to bridge Python and the JVM. When a PySpark application starts:

1. `spark-submit` launches a JVM containing a `PythonGatewayServer`
2. Python connects to the JVM via a local TCP socket
3. Every Python API call (`df.filter(...)`) translates to a JVM method invocation over the socket
4. Py4J is synchronous and single-threaded

Problems with Py4J:
- If Python crashes, the JVM dies (and vice versa)
- Each PySpark process needs a multi-GB JVM heap
- Only languages with JVM bridges (Python, R) can use Spark
- User code runs with full driver privileges

Spark Connect solves all of these by putting the JVM on a separate server.

**Citations:** [SPARK-39375](https://issues.apache.org/jira/browse/SPARK-39375), [SPARK-53484](https://issues.apache.org/jira/browse/SPARK-53484), [Spark Connect Overview](https://spark.apache.org/docs/latest/spark-connect-overview.html), [Databricks: Introducing Spark Connect (July 2022)](https://www.databricks.com/blog/2022/07/07/introducing-spark-connect-the-power-of-apache-spark-everywhere.html)

---

## 7. Python UDTF (User-Defined Table Functions)

### What Is a UDTF?

A **User-Defined Table Function (UDTF)** is a function that returns a table (zero or more rows) instead of a single value. It is used in the `FROM` clause of SQL, not in column expressions.

**JIRA:** [SPARK-43797](https://issues.apache.org/jira/browse/SPARK-43797) -- "Python User-defined Table Functions"
**Shipped:** Spark 3.5 (basic), Spark 4.0 (TABLE argument), Spark 4.1 (Arrow UDTFs)

**UDF vs UDTF:**

| Aspect | UDF | UDTF |
|--------|-----|------|
| Input | One or more column values | One or more values (or a TABLE) |
| Output | One value | Zero or more rows (a table) |
| SQL position | `SELECT udf(col)` | `SELECT * FROM udtf(args)` |
| Python type | Function | Class |
| Use case | Transform a value | Generate/explode rows |

### Basic UDTF Syntax

```python
from pyspark.sql.functions import udtf
from pyspark.sql.types import StructType, StructField, StringType, IntegerType

@udtf(returnType=StructType([
    StructField("number", IntegerType()),
    StructField("squared", IntegerType()),
]))
class SquareRange:
    """Generate a table of numbers and their squares."""
    def eval(self, start: int, end: int):
        for n in range(start, end + 1):
            yield (n, n * n)

# Usage in SQL
spark.sql("SELECT * FROM SquareRange(1, 5)").show()
# +------+-------+
# |number|squared|
# +------+-------+
# |     1|      1|
# |     2|      4|
# |     3|      9|
# |     4|     16|
# |     5|     25|
# +------+-------+
```

### UDTF Class Methods

| Method | Required | Called When | Purpose |
|--------|----------|------------|---------|
| `__init__(self)` | No | Once per partition | Setup (load models, open connections) |
| `eval(self, ...)` | Yes | Once per input row | Process input, yield output rows |
| `terminate(self)` | No | After all input rows | Emit final rows (summaries, cleanup) |
| `analyze(self, ...)` | No | During planning | Dynamic schema inference |

### The TABLE Argument (Spark 4.0+)

The most powerful UDTF feature: passing an **entire table** as an argument. This enables per-row processing of a full DataFrame within SQL.

```python
@udtf(returnType=StructType([
    StructField("order_id", StringType()),
    StructField("event_type", StringType()),
    StructField("event_ts", TimestampType()),
    StructField("duration_mins", DoubleType()),
    StructField("location_id", IntegerType()),
    StructField("city_name", StringType()),
]))
class OrderLifecycleExploder:
    """Explode a single order lifecycle row into multiple event rows."""
    def eval(self, order_id: str, created_at, delivered_at, location_id: int, city_name: str):
        if created_at is None:
            return
        yield (order_id, "order_created", created_at, None, location_id, city_name)
        if delivered_at:
            total_mins = (delivered_at - created_at).total_seconds() / 60
            yield (order_id, "delivered", delivered_at, total_mins, location_id, city_name)

# Register for SQL use
spark.udtf.register("order_lifecycle_explode", OrderLifecycleExploder)

# TABLE argument: pass an entire view as input
result = spark.sql("""
    SELECT * FROM order_lifecycle_explode(
        TABLE order_lifecycle
    )
""")
```

This is the pattern used in the OverArchitected [02_streaming_udtf.py](/scripts/demos/overarchitected/02_streaming_udtf.py) demo.

### TABLE Argument Syntax Details

```sql
-- Pass a view/table as input
SELECT * FROM my_udtf(TABLE my_table);

-- Pass a subquery as input
SELECT * FROM my_udtf(TABLE (
    SELECT * FROM orders WHERE event_type = 'order_created'
));

-- Pass a table with partitioning (process groups independently)
SELECT * FROM my_udtf(TABLE my_table PARTITION BY location_id);

-- Pass a table with partitioning and ordering
SELECT * FROM my_udtf(
    TABLE my_table
    PARTITION BY location_id
    ORDER BY event_timestamp
);
```

The `PARTITION BY` clause tells Spark to group rows by the specified columns and call `eval()` for each row within a group. This is similar to `applyInPandas` but works in pure SQL.

### Dynamic Schema with `analyze()`

For UDTFs where the output schema depends on the input:

```python
@udtf
class FlexibleParser:
    @staticmethod
    def analyze(json_col: AnalyzeArgument) -> AnalyzeResult:
        # Infer schema from the first value
        schema = infer_schema(json_col)
        return AnalyzeResult(schema=schema)

    def eval(self, json_str: str):
        parsed = json.loads(json_str)
        yield tuple(parsed.values())
```

### Arrow UDTFs (Spark 4.1)

**JIRA:** [SPARK-53014](https://issues.apache.org/jira/browse/SPARK-53014)

Spark 4.1 introduces `arrow_udtf`, which receives and returns PyArrow arrays instead of Python objects. This eliminates the Python-per-row overhead:

```python
from pyspark.sql.functions import arrow_udtf
import pyarrow as pa

@arrow_udtf(returnType="id: int, value: double")
class BatchProcessor:
    def eval(self, id_array: pa.Array, value_array: pa.Array):
        # Process entire batches at once using PyArrow compute
        import pyarrow.compute as pc
        doubled = pc.multiply(value_array, pa.scalar(2.0))
        yield id_array, doubled
```

### Practical Use Cases

| Use Case | Description | Why UDTF |
|----------|-------------|----------|
| **Exploding events** | One lifecycle row -> multiple event rows | One-to-many mapping |
| **Generating test data** | Parameters -> table of synthetic rows | Table generation from parameters |
| **API enrichment** | Row -> enriched rows from external API | May return 0 or N results per input |
| **ML inference** | Features -> predictions (batched) | Model may produce multiple outputs |
| **Parsing complex formats** | One blob -> multiple structured records | Unpack nested structures |
| **Time series expansion** | Date range -> row per interval | Generate regular intervals |

### UDTF vs Alternatives

| Approach | When to Use |
|----------|------------|
| `explode()` / `inline()` | Exploding arrays/structs already in the DataFrame |
| `Pandas UDF (grouped map)` | Per-group DataFrame -> DataFrame transformation |
| UDTF | Custom one-to-many logic with full control over output rows |
| `applyInPandas` | GroupBy + apply, but need Pandas API |

**Citations:** [SPARK-43797](https://issues.apache.org/jira/browse/SPARK-43797), [SPARK-53014](https://issues.apache.org/jira/browse/SPARK-53014), [PySpark UDTF Guide](https://spark.apache.org/docs/latest/api/python/user_guide/udfandudtf.html)

---

## 8. Other Notable Spark 4.1 Features

### Java 21 Support

Spark 4.0 requires Java 17 minimum. Spark 4.1 images ship with **Java 21** (the latest LTS):

```
Spark 4.0 container: Java 17 (apache/spark:4.0.1-scala2.13-java17-*)
Spark 4.1 container: Java 21 (apache/spark:4.1.0-scala2.13-java21-*)
```

Java 21 brings:
- **Virtual threads (Project Loom):** Lightweight threads that improve Spark Connect server throughput
- **ZGC generational mode:** Better garbage collection for large heaps
- **Pattern matching:** Cleaner Scala/Java code in Spark internals
- **Foreign Function & Memory API (preview):** Potential future performance improvements for native code integration

**Practical note:** You do not need to change your PySpark code. Java version is a server-side concern. But if you are building custom JVM-based UDFs or extensions, ensure they compile against Java 21.

### ANSI SQL Compliance

**JIRA:** [SPARK-44444](https://issues.apache.org/jira/browse/SPARK-44444) -- "Use ANSI SQL mode by default"

The single most impactful behavior change in Spark 4.0. `spark.sql.ansi.enabled` now defaults to `true`:

| Operation | Spark 3.x Default | Spark 4.x Default |
|-----------|-------------------|-------------------|
| `CAST('abc' AS INT)` | Returns `NULL` | Throws `SparkNumberFormatException` |
| `2147483647 + 1` | Returns `-2147483648` (wraparound) | Throws `SparkArithmeticException` |
| `1 / 0` | Returns `NULL` | Throws `SparkArithmeticException` |
| `array[out_of_bounds]` | Returns `NULL` | Throws `SparkArrayIndexOutOfBoundsException` |

**The `TRY_` function family** provides safe fallbacks:

```sql
SELECT try_cast('abc' AS INT);          -- NULL (no error)
SELECT try_add(2147483647, 1);          -- NULL
SELECT try_divide(1, 0);               -- NULL
SELECT try_to_timestamp('bad_date');   -- NULL
SELECT try_element_at(array, 999);     -- NULL
```

**Migration strategy:** Enable ANSI mode on your 3.x cluster, run your test suite, and replace every failing expression with the appropriate `TRY_` variant.

### SQL Scripting (GA in 4.1)

**JIRA:** [SPARK-54499](https://issues.apache.org/jira/browse/SPARK-54499)

SQL Scripting turns Spark SQL into a procedural language:

```sql
BEGIN
    DECLARE batch_date DATE DEFAULT CURRENT_DATE;
    DECLARE counter INT DEFAULT 0;

    WHILE counter < 7 DO
        INSERT INTO gold.daily_summary
        SELECT DATE_SUB(batch_date, counter) as report_date,
               COUNT(*) as order_count
        FROM silver.orders_enriched
        WHERE event_date = DATE_SUB(batch_date, counter);

        SET counter = counter + 1;
    END WHILE;
END;
```

Supports: `DECLARE`, `SET`, `IF/ELSEIF/ELSE`, `WHILE`, `LOOP`, `REPEAT`, `FOR`, `LEAVE`, `ITERATE`, `CONTINUE HANDLER`.

### Pipe Syntax

An alternative SQL syntax for top-to-bottom readability:

```sql
-- Traditional SQL (inside-out)
SELECT city, COUNT(*) as orders
FROM (SELECT * FROM orders WHERE status = 'completed') AS completed
WHERE total > 50
GROUP BY city
ORDER BY orders DESC;

-- Pipe syntax (top-to-bottom, like DataFrame chains)
SELECT * FROM orders
|> WHERE status = 'completed'
|> WHERE total > 50
|> GROUP BY city
|> SELECT city, COUNT(*) as orders
|> ORDER BY orders DESC;
```

Introduced in Spark 4.0. Particularly useful for complex multi-stage transformations.

### Approximate Data Sketches

Probabilistic data structures for fast approximate aggregations at scale:

```sql
-- Approximate top-K frequent items
SELECT approx_top_k(product, 10) FROM orders;

-- Theta sketches for approximate distinct counts
SELECT theta_sketch_estimate(theta_sketch_agg(user_id)) FROM events;

-- Set operations between sketches
SELECT theta_sketch_estimate(
    theta_sketch_intersect(sketch_a, sketch_b)
) FROM combined;
```

**JIRA:** [SPARK-52515](https://issues.apache.org/jira/browse/SPARK-52515)

### Arrow UDFs

**JIRA:** [SPARK-53014](https://issues.apache.org/jira/browse/SPARK-53014)

The fastest Python UDF type. Works directly with `pyarrow.Array` objects, avoiding the Arrow-to-Pandas conversion overhead of Pandas UDFs:

```python
import pyarrow as pa
import pyarrow.compute as pc
from pyspark.sql.functions import arrow_udf

@arrow_udf("double")
def add_tax(price: pa.Array) -> pa.Array:
    return pc.multiply(price, pa.scalar(1.1))

df.select("name", add_tax("price").alias("price_with_tax"))
```

**Performance hierarchy (fastest to slowest):**

```
Built-in functions     Catalyst-optimized, compiled
SQL UDFs               Catalyst-transparent, inlined
Scala/Java UDFs        JVM-native, opaque to Catalyst
Arrow UDFs (4.1)       Zero-copy columnar, PyArrow compute
Pandas UDFs            Vectorized, Arrow-to-Pandas overhead
Arrow-opt Python UDFs  Arrow serialization, per-row Python
Pickle Python UDFs     Per-row, pickle serialization
```

### Unix Domain Socket for JVM-Python Communication

**JIRA:** [SPARK-51688](https://issues.apache.org/jira/browse/SPARK-51688) -- "Use Unix Domain Socket between Python and JVM communication"

Before Spark 4.1, all JVM-Python communication used TCP loopback sockets (127.0.0.1). Even though local-only, this involves the full TCP/IP stack: SYN/ACK, checksums, window management.

Spark 4.1 supports Unix Domain Sockets (UDS) for this communication, avoiding TCP overhead entirely. Improves throughput by ~15% for small messages and up to ~3x for bulk data transfer.

```properties
spark.python.unix.domain.socket.enabled  true
```

Disabled by default in 4.1.0 for safety. Expected to become default in a future version.

### 77+ New SQL Functions in 4.1

Spark 4.1 added 77+ new built-in SQL functions. Highlights:

| Category | Functions |
|----------|----------|
| String | `try_to_date`, `try_to_timestamp`, `try_parse_url` |
| Array | `array_prepend`, `array_flatten_n` |
| Map | `map_contains_key` |
| Date/Time | `date_diff` (enhanced), `timestamp_diff` |
| Math | `sec`, `csc`, `cot` |
| VARIANT | `try_parse_json`, `try_variant_get`, `variant_explode`, `schema_of_variant_agg` |
| Sketch | `approx_top_k`, `theta_sketch_agg`, `theta_sketch_estimate` |

### Streaming: Real-Time Mode

**JIRA:** [SPARK-52330](https://issues.apache.org/jira/browse/SPARK-52330)

Sub-second latency streaming without the limitations of the deprecated Continuous Processing mode:

```python
# Same Structured Streaming API, just change the trigger
query = df.writeStream \
    .trigger(realTime="5 minutes") \
    .start()
```

Current scope in 4.1: stateless/single-stage Scala queries only. Kafka source and sink. Python support and stateful queries in future releases.

### Declarative Pipelines (SDP)

**JIRA:** [SPARK-51727](https://issues.apache.org/jira/browse/SPARK-51727)

DLT (Delta Live Tables) donated to OSS as Spark Declarative Pipelines:

```python
from pyspark import pipelines as dp

@dp.materialized_view(name="silver.orders_clean")
def orders_clean():
    return spark.table("bronze.orders").filter(col("id").isNotNull())

@dp.table(name="silver.orders_stream")
def orders_stream():
    return spark.readStream.table("bronze.orders_raw")
```

Covered in detail in the companion guide for Act 4 (`companion_guide_sdp_rtm.md`).

**Citations:** [SPARK-44444](https://issues.apache.org/jira/browse/SPARK-44444), [SPARK-54499](https://issues.apache.org/jira/browse/SPARK-54499), [SPARK-53014](https://issues.apache.org/jira/browse/SPARK-53014), [SPARK-51688](https://issues.apache.org/jira/browse/SPARK-51688), [SPARK-52330](https://issues.apache.org/jira/browse/SPARK-52330), [SPARK-51727](https://issues.apache.org/jira/browse/SPARK-51727), [SPARK-52515](https://issues.apache.org/jira/browse/SPARK-52515)

---

## 9. Migration from Spark 3.x to 4.x

### Breaking Changes Overview

Spark 4.0 is the first major version bump in 5 years. Several backwards-incompatible changes require attention.

### Runtime Requirements

| Component | Spark 3.5 | Spark 4.0 | Spark 4.1 |
|-----------|-----------|-----------|-----------|
| Java | 8, 11, 17 | **17+** | 17+ (21 recommended) |
| Scala | 2.12, 2.13 | **2.13 only** | 2.13 |
| Python | 3.8+ | **3.9+** | **3.10+** |
| PyArrow | 10.0+ | 11.0+ | **15.0.0+** |
| Pandas | 1.x+ | **2.x** | 2.x |
| Hadoop | 3.3.x | **3.4.1** | **3.4.2** |

### Default Behavior Changes

| Configuration | 3.x Default | 4.x Default | Impact |
|---------------|-------------|-------------|--------|
| `spark.sql.ansi.enabled` | `false` | **`true`** | **High** -- queries that silently returned NULL now throw errors |
| `spark.shuffle.service.db.backend` | `LEVELDB` | **`ROCKSDB`** | Low -- performance improvement, transparent |
| `spark.ui.prometheus.enabled` | `false` | **`true`** | Low -- Prometheus metrics endpoint enabled |
| `spark.eventLog.rolling.enabled` | `false` | **`true`** | Low -- log files auto-rotate |
| `spark.checkpoint.compress` | `false` | **`true`** (4.1) | Low -- smaller checkpoints |
| Arrow UDF optimization | `false` | **`true`** (4.0) | Medium -- existing UDFs may behave differently with Arrow serialization |

### Removed Components

| Component | Removed In | Replacement |
|-----------|-----------|-------------|
| Mesos support | Spark 4.0 ([SPARK-44442](https://issues.apache.org/jira/browse/SPARK-44442)) | Kubernetes |
| Hive < 2.0 support | Spark 4.0 | Hive 3.x |
| Scala 2.12 builds | Spark 4.0 | Scala 2.13 |
| Java 8, 11 | Spark 4.0 | Java 17+ |
| SparkR | Deprecated in 4.0 | PySpark |
| GraphX | Deprecated in 4.0 | GraphFrames |

### Deprecated APIs

| API | Status | Replacement |
|-----|--------|-------------|
| `javax.*` packages | Migrated to `jakarta.*` | Update all imports |
| `SparkConf.setMaster()` with Mesos URLs | Removed | Use Kubernetes |
| `DStream` API | Long deprecated | Structured Streaming |
| `SQLContext` | Long deprecated | `SparkSession` |
| Continuous Processing trigger | Deprecated in 3.4 | Real-Time Mode (4.1) |

### JAR Compatibility

This is the most common migration failure. Key changes:

**Iceberg runtime JAR naming:**

```
3.x: iceberg-spark-runtime-3.5_2.12-1.5.2.jar    (Spark 3.5, Scala 2.12)
4.x: iceberg-spark-runtime-4.0_2.13-1.10.0.jar   (Spark 4.0, Scala 2.13)
```

Note: The Iceberg runtime for Spark 4.0 works with Spark 4.1. There is no separate `4.1` runtime at this time.

**AWS SDK version change:**

```
Hadoop 3.3.x (Spark 3.x):  AWS SDK v2 2.20.x
Hadoop 3.4.1 (Spark 4.0):  AWS SDK v2 2.24.6  (exact version required)
Hadoop 3.4.2 (Spark 4.1):  AWS SDK v2 2.24.6  (same)
```

### Migration Checklist

```
PRE-MIGRATION (on your 3.x cluster)
[ ] Enable spark.sql.ansi.enabled=true and run full test suite
[ ] Fix ANSI failures with try_cast/try_add/try_divide/etc.
[ ] Upgrade Java to 17+ (21 recommended for 4.1)
[ ] Upgrade Scala builds to 2.13
[ ] Upgrade Python to 3.10+
[ ] Upgrade PyArrow to 15.0.0+
[ ] Upgrade Pandas to 2.x
[ ] Audit javax.* imports -> jakarta.*
[ ] Test Hadoop 3.4.2 compatibility
[ ] Verify all JARs are compatible with Scala 2.13

DEPLOY 4.x
[ ] Update spark-defaults.conf (verify catalog config)
[ ] Download new JARs (Iceberg 4.0 runtime, AWS SDK 2.24.6)
[ ] Start with non-critical batch workloads
[ ] Validate Iceberg/table format compatibility
[ ] Monitor shuffle behavior (RocksDB is default)
[ ] Verify Prometheus metrics (enabled by default)

ADOPT NEW FEATURES (incremental)
[ ] Prototype VARIANT for semi-structured columns
[ ] Try recursive CTEs for hierarchical queries
[ ] Add collation to human-readable text columns
[ ] Evaluate Spark Connect for client applications
[ ] Migrate highest-volume UDFs to arrow_udf
[ ] Evaluate SDP for new pipelines
[ ] Test Real-Time Mode for low-latency streaming
```

### ANSI Migration: Practical Example

The most common migration issue. Here is a real-world example from a food delivery pipeline:

```python
# Spark 3.x: This works (returns NULL for non-numeric totals)
df = spark.sql("""
    SELECT order_id, CAST(body_total AS DOUBLE) as total
    FROM raw_orders
""")

# Spark 4.x: This throws SparkNumberFormatException if body_total is 'N/A'
# Fix: use try_cast
df = spark.sql("""
    SELECT order_id, TRY_CAST(body_total AS DOUBLE) as total
    FROM raw_orders
""")
```

```python
# Spark 3.x: Division by zero returns NULL
df = spark.sql("""
    SELECT order_id, revenue / order_count as avg_revenue
    FROM daily_metrics
""")

# Spark 4.x: Division by zero throws SparkArithmeticException
# Fix: use try_divide or NULLIF
df = spark.sql("""
    SELECT order_id, TRY_DIVIDE(revenue, order_count) as avg_revenue
    FROM daily_metrics
""")
-- OR
df = spark.sql("""
    SELECT order_id, revenue / NULLIF(order_count, 0) as avg_revenue
    FROM daily_metrics
""")
```

### Compatibility with Lakehouse Stack Components

| Component | 3.x Compatibility | 4.x Compatibility | Notes |
|-----------|-------------------|-------------------|-------|
| Iceberg 1.10.0 | Yes (with 3.x runtime) | Yes (with 4.0 runtime) | Runtime JAR must match Spark major |
| PostgreSQL JDBC | Yes | Yes | Same JAR works |
| Kafka connector | Yes | Yes | Update Scala version in artifact ID: `spark-sql-kafka-0-10_2.13` |
| SeaweedFS (S3) | Yes | Yes | Same S3A config |
| Unity Catalog OSS | Yes (limited) | Yes (full REST catalog) | UC was designed for 4.x |
| Airflow | Yes | Yes | Airflow calls spark-submit; transparent |

**Citations:** [Spark SQL Migration Guide](https://spark.apache.org/docs/latest/sql-migration-guide.html), [Spark 4.0.0 Release Notes](https://spark.apache.org/releases/spark-release-4-0-0.html), [SPARK-44444](https://issues.apache.org/jira/browse/SPARK-44444), [SPARK-44442](https://issues.apache.org/jira/browse/SPARK-44442)

---

## 10. References

### JIRA Issues (Cited in This Guide)

| JIRA | Title | Section |
|------|-------|---------|
| [SPARK-46905](https://issues.apache.org/jira/browse/SPARK-46905) | Semi-Structured Data Processing with VARIANT type | 2 |
| [SPARK-45891](https://issues.apache.org/jira/browse/SPARK-45891) | Support Variant data type | 2 |
| [SPARK-24497](https://issues.apache.org/jira/browse/SPARK-24497) | Support recursive CTE | 3 |
| [SPARK-46830](https://issues.apache.org/jira/browse/SPARK-46830) | Support string collation | 4 |
| [SPARK-39375](https://issues.apache.org/jira/browse/SPARK-39375) | SPIP: Spark Connect | 6 |
| [SPARK-53484](https://issues.apache.org/jira/browse/SPARK-53484) | Spark Connect JDBC driver | 6 |
| [SPARK-43797](https://issues.apache.org/jira/browse/SPARK-43797) | Python User-defined Table Functions | 7 |
| [SPARK-53014](https://issues.apache.org/jira/browse/SPARK-53014) | Native Arrow UDF/UDTF | 7, 8 |
| [SPARK-44444](https://issues.apache.org/jira/browse/SPARK-44444) | Use ANSI SQL mode by default | 8, 9 |
| [SPARK-54499](https://issues.apache.org/jira/browse/SPARK-54499) | SQL Scripting GA | 8 |
| [SPARK-51688](https://issues.apache.org/jira/browse/SPARK-51688) | Unix Domain Socket for Python-JVM | 8 |
| [SPARK-52330](https://issues.apache.org/jira/browse/SPARK-52330) | Real-Time Mode | 8 |
| [SPARK-51727](https://issues.apache.org/jira/browse/SPARK-51727) | SPIP: Declarative Pipelines | 8 |
| [SPARK-52515](https://issues.apache.org/jira/browse/SPARK-52515) | Approximate data sketches | 8 |
| [SPARK-44442](https://issues.apache.org/jira/browse/SPARK-44442) | Remove Mesos support | 9 |
| [SPARK-48516](https://issues.apache.org/jira/browse/SPARK-48516) | Arrow UDF optimization on by default | 8 |
| [SPARK-48730](https://issues.apache.org/jira/browse/SPARK-48730) | SQL UDFs (persistent) | 8 |

### Official Documentation

- [Apache Spark 4.1.0 Release Notes](https://spark.apache.org/releases/spark-release-4.1.0.html)
- [Apache Spark 4.0.0 Release Notes](https://spark.apache.org/releases/spark-release-4-0-0.html)
- [Spark SQL Migration Guide](https://spark.apache.org/docs/latest/sql-migration-guide.html)
- [Spark ANSI Compliance](https://spark.apache.org/docs/latest/sql-ref-ansi-compliance.html)
- [Spark Connect Overview](https://spark.apache.org/docs/latest/spark-connect-overview.html)
- [Spark Configuration](https://spark.apache.org/docs/latest/configuration.html)
- [PySpark UDF and UDTF Guide](https://spark.apache.org/docs/latest/api/python/user_guide/udfandudtf.html)
- [Iceberg Spark Configuration](https://iceberg.apache.org/docs/latest/spark-configuration/)
- [Hadoop S3A Configuration](https://hadoop.apache.org/docs/current/hadoop-aws/tools/hadoop-aws/index.html)
- [Parquet Variant Encoding Specification](https://parquet.apache.org/docs/file-format/types/variantencoding/)
- [Running Spark on Kubernetes](https://spark.apache.org/docs/latest/running-on-kubernetes.html)

### Blog Posts and External Resources

- [Databricks: Introducing Spark Connect (July 2022)](https://www.databricks.com/blog/2022/07/07/introducing-spark-connect-the-power-of-apache-spark-everywhere.html)
- [Databricks: Introducing Open Variant Data Type](https://www.databricks.com/blog/introducing-open-variant-data-type-delta-lake-and-apache-spark)
- [Databricks: Arrow-optimized Python UDFs in Spark 3.5](https://www.databricks.com/blog/arrow-optimized-python-udfs-apache-sparktm-35)
- [Unicode Technical Standard #10 (Unicode Collation Algorithm)](https://unicode.org/reports/tr10/)

### Lakehouse Stack Files Referenced

| File | Purpose |
|------|---------|
| [`config/spark/spark-defaults.conf.example`](/config/spark/spark-defaults.conf.example) | JDBC catalog config template |
| [`config/spark/spark-defaults-uc.conf.example`](/config/spark/spark-defaults-uc.conf.example) | REST (Unity Catalog) config template |
| [`config/spark/spark-defaults-hms.conf`](/config/spark/spark-defaults-hms.conf) | Hive Metastore config template |
| [`scripts/tools/download-jars.sh`](/scripts/tools/download-jars.sh) | JAR download script with retry/verify |
| [`scripts/demos/overarchitected/03_spark_setup.py`](/scripts/demos/overarchitected/03_spark_setup.py) | Act 3 demo script |
| [`scripts/demos/overarchitected/01_variant_iceberg.py`](/scripts/demos/overarchitected/01_variant_iceberg.py) | VARIANT demo |
| [`scripts/demos/overarchitected/02_streaming_udtf.py`](/scripts/demos/overarchitected/02_streaming_udtf.py) | UDTF demo |
| [`scripts/demos/overarchitected/03_full_overarchitected.py`](/scripts/demos/overarchitected/03_full_overarchitected.py) | Combined features demo |
| [`scripts/demos/overarchitected/05b_spark_connect.py`](/scripts/demos/overarchitected/05b_spark_connect.py) | Spark Connect demo |

---

*This companion guide is part of the OverArchitected show series. For the full show flow and other companion guides, see the [OverArchitected README](/scripts/demos/overarchitected/README.md).*
