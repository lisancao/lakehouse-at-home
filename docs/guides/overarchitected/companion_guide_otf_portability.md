# Companion Guide: Open Table Formats & Data Portability

**Audience:** Data engineers and architects familiar with Databricks who want to understand the open-source internals of the table formats they already use -- Iceberg, Delta Lake, and Hudi -- at the specification level. You know what a medallion architecture is. You may not know what a manifest file looks like on disk.

**Complements:** OverArchitected Show, Act 1 ("We Have Data") -- `scripts/demos/overarchitected/01_data_smuggled.py`

**Purpose:** Exhaustive technical reference for every claim made in Act 1. Every architectural decision is explained, every tradeoff is stated honestly, and every claim is cited with a link to a specification, JIRA issue, or primary source. This is the guide you hand to the engineer who asks "but how does it actually work?"

---

## Table of Contents

1. [What Are Open Table Formats?](#1-what-are-open-table-formats)
2. [Iceberg Architecture Deep Dive](#2-iceberg-architecture-deep-dive)
3. [Iceberg vs Delta Lake vs Hudi](#3-iceberg-vs-delta-lake-vs-hudi)
4. [The Iceberg REST Catalog Protocol](#4-the-iceberg-rest-catalog-protocol)
5. [Parquet as the Foundation](#5-parquet-as-the-foundation)
6. [Data Portability in Practice](#6-data-portability-in-practice)
7. [Schema Evolution](#7-schema-evolution)
8. [Partition Evolution](#8-partition-evolution)
9. [Time Travel](#9-time-travel)
10. [The lakehouse-stack Implementation](#10-the-lakehouse-stack-implementation)
11. [Migration Considerations](#11-migration-considerations)
12. [References](#12-references)

---

## 1. What Are Open Table Formats?

### The Problem They Solve

A data lake stores files (Parquet, ORC, CSV) in object storage. That gives you cheap, scalable storage. What it does not give you is:

- **Atomicity**: If a Spark job writes 200 Parquet files and crashes after file 187, you have a corrupted dataset. Readers see partial writes.
- **Consistency**: Two concurrent writers can produce conflicting results with no mechanism to detect or resolve the conflict.
- **Isolation**: A reader scanning a directory mid-write sees a mix of old and new files.
- **Schema enforcement**: Nothing prevents writing a Parquet file with a different schema into the same directory.
- **Time travel**: Once you overwrite a file, the previous version is gone.
- **Efficient reads**: To find rows matching `WHERE date = '2025-01-15'`, a query engine must list every file in the directory (an O(n) operation on object stores where listing is slow and paginated at 1000 keys per request [1]).

These problems are not theoretical. They are the daily reality of anyone who has operated a Hive-style data lake. The Hive metastore tracks partitions as directory paths (`s3://bucket/table/date=2025-01-15/`), but within each partition, there is no transaction log, no file-level tracking, and no isolation.

Open Table Formats (OTFs) solve all of these problems by adding a **metadata layer** between the query engine and the data files. The three major OTFs are:

| Format | Created | Origin | License | Current Version |
|--------|---------|--------|---------|-----------------|
| Apache Iceberg | 2017 (Netflix), ASF TLP 2020 | Netflix | Apache 2.0 | 1.10.0 (March 2025) [2] |
| Delta Lake | 2019 (Databricks), Linux Foundation 2024 | Databricks | Apache 2.0 | 4.0.0 (Nov 2024) [3] |
| Apache Hudi | 2016 (Uber), ASF TLP 2020 | Uber | Apache 2.0 | 1.0.1 (Feb 2025) [4] |

All three store data in Parquet (or ORC) files. All three provide ACID transactions, schema evolution, and time travel. They differ in metadata structure, concurrency control, and ecosystem support. We will examine those differences in depth in Section 3.

### Why OTFs Matter for Data Portability

The defining property of an OTF is that the format is **open and engine-agnostic**. The table's metadata and data files are stored in a well-documented format on object storage. Any engine that implements the specification can read (and write) the table.

This is fundamentally different from a proprietary format where the storage layout is an internal implementation detail of a single vendor's product. With an OTF:

1. **Your data is not locked in.** You can read an Iceberg table written by Spark using DuckDB, Trino, Dremio, Snowflake, BigQuery, Polars, or raw PyArrow. The files are Parquet. The metadata is JSON and Avro. There is no proprietary binary format.

2. **Your metadata is not locked in.** The catalog interface (especially the Iceberg REST Catalog Protocol) means you can swap catalog implementations without changing your query engine or rewriting data.

3. **Your compute is not locked in.** Because any engine can read the table, you can use Spark for heavy ETL, DuckDB for ad-hoc analytics, Trino for federated queries, and Polars for single-node DataFrame work -- all against the same table, concurrently, with snapshot isolation.

This is the premise of Act 1 of the OverArchitected show: Holly and Nick quit Databricks, but they take their data with them. The data is stored in open formats. It does not belong to the platform.

### A Note on "Open"

"Open" is a spectrum, not a binary. Some considerations:

- **Iceberg** has been an Apache Software Foundation top-level project since May 2020. Its specification is developed in the open on GitHub. The Iceberg table spec is a formal document with versioned format definitions [5]. Any person or company can implement it.

- **Delta Lake** was initially developed by Databricks and open-sourced under the Apache 2.0 license. In November 2024, it moved to the Linux Foundation. However, some features historically required the Databricks runtime (deletion vectors, liquid clustering, UniForm). The Delta Universal Format (UniForm) generates Iceberg-compatible metadata alongside Delta metadata, which is itself an acknowledgment that multi-engine access matters [6].

- **Apache Hudi** has been an ASF top-level project since May 2020. It has a broader scope than Iceberg or Delta, including built-in indexing, record-level metadata, and a timeline-based architecture. This broader scope makes it more complex to implement from scratch.

All three are genuinely open-source. The practical question is: how many engines have production-quality implementations? We address this in Section 3.

---

## 2. Iceberg Architecture Deep Dive

Iceberg's architecture is a **tree of immutable files** rooted at a catalog entry. Understanding this tree is essential for understanding every Iceberg feature -- time travel, schema evolution, partition evolution, and concurrent writes all follow directly from this structure.

### The Metadata Tree

```
Catalog
  │
  │  (pointer to current metadata file)
  │
  ▼
Metadata File (v3.metadata.json)
  │
  ├── Table schema (column IDs, types, names)
  ├── Partition spec (field-id → transform)
  ├── Sort order
  ├── Properties (table-level config)
  ├── Snapshot log (ordered list of snapshots)
  │
  └── Current Snapshot
        │
        ▼
      Manifest List (snap-<id>-<attempt>.avro)
        │
        ├── Manifest 1 metadata (partition range, added/deleted counts)
        ├── Manifest 2 metadata
        └── ...
              │
              ▼
            Manifest File (*.avro)
              │
              ├── Data file 1 (path, format, partition, record count,
              │                 column-level min/max/null stats)
              ├── Data file 2
              └── ...
                    │
                    ▼
                  Data Files (*.parquet)
                    │
                    └── Actual row data
```

Each level in this tree is **immutable**. When you commit a new snapshot, Iceberg does not modify the previous metadata file. It writes a new metadata file, a new manifest list, and (usually) new manifest files. The catalog pointer is atomically updated from the old metadata file to the new one. This is the fundamental mechanism for snapshot isolation.

### Level 1: The Catalog Entry

The catalog is the only mutable state in the entire system. It stores exactly one thing per table: a pointer to the current metadata file location.

For the lakehouse-stack's PostgreSQL JDBC catalog, this is a row in the `iceberg_tables` table:

```sql
SELECT catalog_name, table_namespace, table_name, metadata_location
FROM iceberg_tables
WHERE table_namespace = 'bronze' AND table_name = 'orders';
```

```
catalog_name | table_namespace | table_name | metadata_location
─────────────┼─────────────────┼────────────┼──────────────────────────────────────────
iceberg      | bronze          | orders     | s3a://lakehouse/warehouse/bronze/orders/
             |                 |            |   metadata/v12.metadata.json
```

The atomic update of this pointer is what makes Iceberg commits atomic. For JDBC catalogs, it is a SQL `UPDATE` with a `WHERE metadata_location = <expected>` clause (optimistic concurrency). For REST catalogs, it is an HTTP `POST` with a `requirements` block that asserts the expected state. For Hadoop catalogs, it is a filesystem rename (which is NOT atomic on S3 -- this is why the Hadoop catalog is not recommended for production use on object stores) [7].

### Level 2: Metadata Files

A metadata file is a JSON document that describes the entire table state at a point in time. Key fields (from the Iceberg Table Spec v2) [5]:

```json
{
  "format-version": 2,
  "table-uuid": "bf289591-dcc0-4234-a4f0-6a1a3e410921",
  "location": "s3a://lakehouse/warehouse/bronze/orders",
  "last-sequence-number": 12,
  "last-updated-ms": 1710000000000,
  "last-column-id": 8,
  "current-schema-id": 1,
  "schemas": [
    {
      "schema-id": 0,
      "type": "struct",
      "fields": [
        {"id": 1, "name": "order_id", "required": false, "type": "string"},
        {"id": 2, "name": "event_type", "required": false, "type": "string"},
        {"id": 3, "name": "ts", "required": false, "type": "timestamp"}
      ]
    },
    {
      "schema-id": 1,
      "type": "struct",
      "fields": [
        {"id": 1, "name": "order_id", "required": false, "type": "string"},
        {"id": 2, "name": "event_type", "required": false, "type": "string"},
        {"id": 3, "name": "ts", "required": false, "type": "timestamp"},
        {"id": 9, "name": "brand_id", "required": false, "type": "int"}
      ]
    }
  ],
  "default-spec-id": 0,
  "partition-specs": [
    {
      "spec-id": 0,
      "fields": [
        {"source-id": 3, "field-id": 1000, "name": "ts_day", "transform": "day"}
      ]
    }
  ],
  "current-snapshot-id": 7035700953559098903,
  "snapshots": [
    {
      "snapshot-id": 7035700953559098903,
      "timestamp-ms": 1710000000000,
      "summary": {
        "operation": "append",
        "added-data-files": "4",
        "added-records": "15000",
        "total-records": "150000",
        "total-data-files": "48"
      },
      "manifest-list": "s3a://lakehouse/warehouse/bronze/orders/metadata/snap-7035700953559098903-0.avro"
    }
  ],
  "snapshot-log": [
    {"timestamp-ms": 1709000000000, "snapshot-id": 2381234871234987},
    {"timestamp-ms": 1710000000000, "snapshot-id": 7035700953559098903}
  ]
}
```

Important design decisions visible here:

1. **Column IDs, not names.** Every column has a unique integer ID that never changes. When you rename `ts` to `event_timestamp`, the ID remains `3`. This is how Iceberg decouples the logical schema from the physical Parquet files -- old Parquet files written with the name `ts` can still be read using the new name `event_timestamp` because the mapping is by ID [5, Schema Evolution section].

2. **Multiple schemas.** The metadata file stores every schema the table has ever had. Each snapshot references a specific schema-id. This enables time travel to work correctly even when the schema has changed.

3. **Partition specs are versioned.** Like schemas, partition specs are identified by spec-id. When you evolve the partition scheme, old data files retain their original partition spec. New files use the new spec. No data rewriting required.

4. **Snapshots are append-only.** The `snapshots` array grows with each commit. Each snapshot has a unique ID, a timestamp, an operation summary, and a pointer to its manifest list. The `snapshot-log` provides the ordered history.

### Level 3: Manifest Lists

A manifest list is an Avro file that enumerates the manifest files belonging to a snapshot. Each entry includes summary statistics:

```
Manifest List (snap-7035700953559098903-0.avro)
┌────────────────────────────────────────────────────────────────────┐
│ manifest_path         | manifest_length | added_files | partition_ │
│                       |                 |             | summary    │
├────────────────────────────────────────────────────────────────────┤
│ .../metadata/m0.avro  |          12,480 |           4 | ts_day:    │
│                       |                 |             | [2025-01-15│
│                       |                 |             |  ..01-16]  │
├────────────────────────────────────────────────────────────────────┤
│ .../metadata/m1.avro  |          45,200 |           0 | ts_day:    │
│                       |                 |             | [2025-01-01│
│                       |                 |             |  ..01-14]  │
└────────────────────────────────────────────────────────────────────┘
```

The partition summary in the manifest list enables **manifest-level pruning**: if a query filters on `WHERE ts >= '2025-01-15'`, Iceberg can skip reading manifest `m1.avro` entirely because its partition range (`2025-01-01` to `2025-01-14`) does not overlap the filter. This is a critical optimization for large tables with thousands of manifest files.

The `added_files` count of 0 for `m1.avro` means that manifest was carried forward from a previous snapshot without modification. Iceberg reuses manifests across snapshots when the files they track have not changed. This is how snapshot overhead stays proportional to the size of the change, not the size of the table.

### Level 4: Manifest Files

A manifest file is an Avro file that lists individual data files with per-file and per-column statistics:

```
Manifest File (m0.avro)
┌──────────────────────────────────────────────────────────────────────────┐
│ file_path                    | file_format | record_count | column_sizes │
│                              |             |              |              │
│ .../data/ts_day=2025-01-15/  | PARQUET     |        3,750 | {1: 28800,   │
│   part-00000-abc.parquet     |             |              |  2: 15200,   │
│                              |             |              |  3: 30000}   │
├──────────────────────────────────────────────────────────────────────────┤
│ lower_bounds                 | upper_bounds               | null_count   │
│                              |                            |              │
│ {1: "ORD-00100",            | {1: "ORD-04850",          | {1: 0,       │
│  2: "delivered",             |  2: "order_created",       |  2: 0,       │
│  3: 1705276800000}           |  3: 1705363199000}         |  3: 12}      │
└──────────────────────────────────────────────────────────────────────────┘
```

These per-file column statistics enable **file-level pruning** (also called "data skipping" or "min/max pruning"). If a query includes `WHERE order_id = 'ORD-99999'`, Iceberg checks the `upper_bounds` for column 1 (`order_id`). If `ORD-99999` is greater than the upper bound of `ORD-04850`, this file can be skipped entirely without opening the Parquet file.

This three-level pruning (manifest list -> manifest file -> data file) is what makes Iceberg fast on tables with hundreds of thousands of files. A query on a well-partitioned table might:
1. Read the metadata file (one JSON file, typically < 1 MB)
2. Read the manifest list (one Avro file, typically < 100 KB)
3. Skip 95% of manifests via partition summary pruning
4. Read the remaining manifests (a few Avro files)
5. Skip 80% of data files via column statistics pruning
6. Read only the remaining Parquet files

Compare this to a Hive-style table where step 1 is "list all objects in the S3 prefix" -- an operation that takes O(n/1000) API calls and returns no statistics.

### Level 5: Data Files

The actual data is stored in Parquet files (or ORC or Avro, though Parquet is the default and near-universal choice). See Section 5 for a deep dive on Parquet internals.

### Snapshot Isolation: How Concurrent Reads and Writes Work

Iceberg provides **serializable isolation** for writes and **snapshot isolation** for reads [5].

**Reads** are lock-free. When a reader opens a table, it reads the current metadata file pointer from the catalog. From that point forward, the reader works entirely from immutable files. Even if a writer commits a new snapshot while the reader is scanning, the reader continues to see the snapshot it started with. There is no locking, no coordination, and no possibility of dirty reads.

**Writes** use optimistic concurrency control. A writer:
1. Reads the current metadata file pointer from the catalog
2. Plans its changes (which files to add, which to delete)
3. Writes new data files to object storage
4. Writes new manifest files
5. Writes a new manifest list
6. Writes a new metadata file
7. Attempts to atomically update the catalog pointer from the old metadata file to the new one

Step 7 is where conflicts are detected. If another writer has already updated the pointer, the commit fails. The writer must then re-read the current state and determine whether its changes conflict with the intervening commit. Iceberg's conflict resolution is operation-aware: two appends to different partitions do not conflict, even though they both update the metadata pointer [8].

```
Writer A                           Writer B
────────                           ────────
Read metadata v5                   Read metadata v5
Plan: append to partition Jan-15   Plan: append to partition Jan-16
Write data files                   Write data files
Write manifests                    Write manifests
Write metadata v6                  Write metadata v6'
CAS(v5 → v6): SUCCESS             CAS(v5 → v6'): FAIL
                                   Retry: read v6
                                   Detect: no conflict (different partitions)
                                   Rebase onto v6
                                   Write metadata v7
                                   CAS(v6 → v7): SUCCESS
```

This is fundamentally different from Delta Lake's approach, which uses a linear transaction log where writers must acquire the next sequential log entry (see Section 3).

### Iceberg Table Spec Format Versions

Iceberg has two table format versions, with a third in development:

| Version | Introduced | Key Features |
|---------|-----------|-------------|
| v1 | Iceberg 0.x | Core table format, snapshots, schema evolution, partition evolution |
| v2 | Iceberg 0.14 (2022) | Row-level deletes (position delete files, equality delete files), sequence numbers for ordering [9] |
| v3 | In progress | Multi-arg transforms, default values, nanosecond timestamps, row lineage [10] |

**Format v2** is the current production standard. Its most important addition is **row-level deletes**. In v1, deleting a row required rewriting the entire data file that contained it. In v2, Iceberg can write a separate "delete file" that marks specific rows as deleted. This makes MERGE, UPDATE, and DELETE operations much more efficient.

There are two types of delete files:

- **Position deletes**: A file that says "in data file X, rows at positions [4, 17, 231] are deleted." Fast to apply (just skip those positions when reading) but requires knowing the exact file and position.
- **Equality deletes**: A file that says "any row where `order_id = 'ORD-001'` is deleted." More flexible but slower to apply (requires checking every row against the predicate).

Position deletes are the common path for MERGE operations. Equality deletes are used less frequently and will likely be deprecated in favor of deletion vectors in v3.

---

## 3. Iceberg vs Delta Lake vs Hudi

This comparison is technically accurate as of March 2026. The three formats are converging in capability, but they differ materially in architecture, ecosystem support, and philosophy. Stating one is "better" than another without context is not useful; each was designed for different priorities.

### Metadata Architecture

This is the most fundamental difference:

```
ICEBERG: Tree of immutable files
──────────────────────────────────
Catalog → Metadata JSON → Manifest List (Avro) → Manifest (Avro) → Data (Parquet)

DELTA LAKE: Linear transaction log
──────────────────────────────────
_delta_log/00000000000000000000.json  (commit 0)
_delta_log/00000000000000000001.json  (commit 1)
_delta_log/00000000000000000002.json  (commit 2)
...
_delta_log/00000000000000000010.checkpoint.parquet  (periodic checkpoint)

HUDI: Timeline + file groups
──────────────────────────────────
.hoodie/
  hoodie.properties
  20250115120000.commit          (instant)
  20250115130000.deltacommit     (instant)
  ...
<partition>/
  <file-group-id>_<write-token>_<instant>.parquet  (base file)
  .<file-group-id>_<instant>.log                   (log file)
```

**Iceberg** uses a tree structure where the catalog points to a metadata file, which points to manifest lists, which point to manifests, which point to data files. Metadata is in JSON and Avro. The tree can be pruned at every level using partition and column statistics.

**Delta Lake** uses a flat, ordered transaction log stored in a `_delta_log/` directory alongside the data. Each commit is a JSON file. Every 10 commits (configurable), a checkpoint Parquet file is written that summarizes the full table state. To read the current state, an engine reads the latest checkpoint and replays subsequent JSON commits [11].

**Hudi** uses a timeline of "instants" (commits, compactions, cleanings) stored in a `.hoodie/` directory. Data is organized into "file groups" -- each file group has at most one base file and zero or more log files. This architecture is optimized for upserts: a MERGE operation appends to the log file; a later compaction merges the log into the base file [4].

### Detailed Technical Comparison

| Feature | Iceberg | Delta Lake | Hudi |
|---------|---------|------------|------|
| **Metadata format** | JSON + Avro (tree) | JSON + Parquet (log) | Timeline + Avro |
| **Data formats** | Parquet, ORC, Avro | Parquet only | Parquet, ORC |
| **Catalog requirement** | Required (pluggable) | None (self-describing `_delta_log/`) | None (self-describing `.hoodie/`) |
| **Concurrency control** | Optimistic (CAS on catalog pointer) | Optimistic (sequential log entries) | Optimistic (timeline ordering) |
| **Conflict resolution** | Operation-aware (appends to different partitions don't conflict) | File-level (any overlapping file modifications conflict) | Key-level for MoR tables |
| **Partition evolution** | First-class: change scheme without rewriting [5] | Requires data rewrite | Requires data rewrite |
| **Schema evolution** | Column IDs (rename/reorder without rewrite) [5] | Column names (rename requires rewrite or mapping) [12] | Column names |
| **Hidden partitioning** | Yes (partition transforms are metadata-only) [5] | No (partitions are explicit columns) | No |
| **Row-level deletes** | Position deletes + equality deletes (v2) [9] | Deletion vectors (since 3.1) [13] | MoR log files + CoW rewrites |
| **Merge-on-read** | v2 delete files | Deletion vectors | Native (file groups + log files) |
| **Time travel** | By snapshot ID or timestamp | By version number or timestamp | By instant time |
| **Incremental reads** | `since-snapshot-id` option | `startingVersion` / CDF | Incremental pull via timeline |
| **Branching/tagging** | Yes (since 1.2, SPARK-40510) [14] | Not natively (manual) | Not natively |
| **Statistics** | Per-column min/max/null in manifests | Per-file stats in log entries | Per-file stats in timeline |
| **Spec stability** | Formal versioned spec (v1, v2, v3) [5] | Protocol + reader/writer versions [11] | No formal spec document |
| **Governance body** | Apache Software Foundation | Linux Foundation (since Nov 2024) | Apache Software Foundation |

### Engine Support

This is where the practical differences are most visible. As of March 2026:

| Engine | Iceberg | Delta Lake | Hudi |
|--------|---------|------------|------|
| Apache Spark | Full read/write (native since 4.0) | Full read/write (native since 4.0) | Full read/write |
| Trino | Full read/write [15] | Read/write [16] | Read/write |
| DuckDB | Read (via `iceberg` extension) [17] | Read (via `delta` extension) | Limited |
| Polars | Read (via `scan_iceberg`) | Read (via `scan_delta`) | No |
| Snowflake | Read (Iceberg tables, external catalog) [18] | Read (via sharing protocol) | No |
| BigQuery | Read/write (BigLake Iceberg tables) [19] | No native support | No |
| Dremio | Full read/write | Read | No |
| Databricks | Read/write (via UniForm or native) | Full read/write (native) | Read |
| Flink | Full read/write | Read/write | Full read/write |
| Presto | Read/write | Read | Read/write |
| Athena | Read/write [20] | Read | Read |
| pandas/PyArrow | Read (via pyiceberg) [21] | Read (via deltalake) | Limited |
| Rust (datafusion) | Read (via iceberg-rust) | Read (via delta-rs) | No |

The trend is clear: **Iceberg has the broadest multi-engine support**. This is not because Iceberg is technically superior in every dimension (it is not), but because:

1. The Iceberg REST Catalog Protocol (Section 4) provides a standard API that any engine can implement once.
2. Iceberg's tree-structured metadata is amenable to lightweight clients -- you can implement a reader without a JVM.
3. The Apache Software Foundation governance means no single company controls the spec.

Delta Lake's strength is its deep integration with Databricks and Spark. If your entire stack is Spark, Delta Lake works well. The Delta Kernel project [22] is working toward better multi-engine support, and UniForm [6] bridges the gap by writing Iceberg-compatible metadata alongside Delta metadata.

Hudi's strength is record-level upserts and incremental processing. If your primary workload is CDC (change data capture) with high-frequency updates, Hudi's file-group architecture is purpose-built for this.

### An Honest Assessment

**Choose Iceberg if** you need multi-engine access, partition evolution, or are building an open lakehouse that may need to serve data to non-Spark consumers. This is the direction the industry is moving -- Google Cloud, AWS, Snowflake, and Cloudera have all standardized on Iceberg as their external table format.

**Choose Delta Lake if** your entire analytics stack runs on Databricks or Spark, you want the simplest possible setup (no external catalog needed), and you value the tight integration with Databricks features (Unity Catalog on Databricks, Photon, liquid clustering).

**Choose Hudi if** you have a CDC-heavy workload with millions of upserts per hour and need record-level indexing. Hudi's architecture was designed for Uber's ride-tracking use case, and it shows.

**The convergence reality**: All three formats are adopting each other's best features. Delta adopted deletion vectors (similar to Iceberg's position deletes). Iceberg is adding row lineage (similar to Hudi's record-level tracking). Hudi 1.0 added a new "lake storage" format that simplifies its architecture. The differences are shrinking, but the ecosystem and governance differences remain significant.

---

## 4. The Iceberg REST Catalog Protocol

### Why Catalogs Matter

An Iceberg table is a metadata file on object storage. But how does an engine find that metadata file? It asks the catalog. The catalog is the "phone book" that maps table names to metadata file locations.

Without a standard catalog protocol, every engine needs a custom integration with every catalog implementation. Spark uses a Java `Catalog` interface. Trino uses its own connector. DuckDB uses its own extension. If you add a new catalog implementation, you need to write a plugin for every engine.

The Iceberg REST Catalog Protocol [23] solves this by defining a standard HTTP API that any catalog can implement and any engine can consume.

### The Protocol

The REST Catalog Protocol is defined in the Iceberg spec under `rest-catalog` [23]. It is a RESTful HTTP API with JSON request/response bodies. Key endpoints:

```
GET    /v1/config
       Returns catalog configuration (defaults, overrides).

GET    /v1/namespaces
       List all namespaces.

POST   /v1/namespaces
       Create a namespace.

GET    /v1/namespaces/{namespace}
       Load namespace metadata.

GET    /v1/namespaces/{namespace}/tables
       List tables in a namespace.

POST   /v1/namespaces/{namespace}/tables
       Create a table.

GET    /v1/namespaces/{namespace}/tables/{table}
       Load a table. Returns metadata file location and table metadata.

POST   /v1/namespaces/{namespace}/tables/{table}
       Commit a table update (atomic metadata swap).

POST   /v1/namespaces/{namespace}/tables/{table}/metrics
       Report scan metrics back to the catalog.
```

### The Critical Endpoint: Load Table

When an engine wants to read a table, it calls `GET /v1/namespaces/{namespace}/tables/{table}`. The response includes:

```json
{
  "metadata-location": "s3a://lakehouse/warehouse/bronze/orders/metadata/v12.metadata.json",
  "metadata": {
    "format-version": 2,
    "table-uuid": "bf289591-dcc0-4234-a4f0-6a1a3e410921",
    "location": "s3a://lakehouse/warehouse/bronze/orders",
    "current-snapshot-id": 7035700953559098903,
    "schemas": [...],
    "partition-specs": [...],
    "snapshots": [...]
  },
  "config": {
    "s3.access-key-id": "<vended-credential>",
    "s3.secret-access-key": "<vended-credential>",
    "s3.session-token": "<vended-credential>",
    "s3.region": "us-east-1"
  }
}
```

Note the `config` block. This is **credential vending** -- the catalog provides short-lived storage credentials scoped to this specific table. This is a powerful security pattern: the client engine never needs long-lived storage credentials. The catalog acts as a credential broker, issuing time-limited tokens with least-privilege access [24].

### The Commit Endpoint: Atomic Updates

When an engine wants to commit changes (append data, update schema, etc.), it calls `POST /v1/namespaces/{namespace}/tables/{table}` with a request body that includes:

```json
{
  "requirements": [
    {"type": "assert-current-snapshot-id", "snapshot-id": 7035700953559098903}
  ],
  "updates": [
    {
      "action": "add-snapshot",
      "snapshot": {
        "snapshot-id": 8192345678901234567,
        "timestamp-ms": 1710100000000,
        "summary": {"operation": "append", "added-records": "5000"},
        "manifest-list": "s3a://lakehouse/.../snap-819234.avro"
      }
    },
    {
      "action": "set-current-snapshot-id",
      "snapshot-id": 8192345678901234567
    }
  ]
}
```

The `requirements` array is the optimistic concurrency check. The catalog verifies that the current snapshot ID matches the expected value before applying the updates. If it doesn't match (because another writer committed in the meantime), the commit is rejected with a 409 Conflict response, and the client must retry.

### Catalog Implementations

Several catalog implementations support the REST protocol:

| Implementation | Operator | Notes |
|----------------|----------|-------|
| **Unity Catalog OSS** | Self-hosted | Databricks-originated, now open-source. Implements Iceberg REST + credential vending [25] |
| **Nessie** | Self-hosted | Git-like catalog with branches and tags. Project Nessie [26] |
| **Polaris (Apache, incubating)** | Self-hosted | Originally from Snowflake, donated to ASF. Full REST catalog + RBAC [27] |
| **AWS Glue** | AWS-managed | Supports Iceberg REST protocol since 2024 [20] |
| **Gravitino** | Self-hosted | Apache incubating project, multi-catalog federation [28] |
| **Tabular (now part of Databricks)** | SaaS | Acquired by Databricks in 2024 |

In the lakehouse-stack, we support two catalog options:

1. **PostgreSQL JDBC Catalog** (default) -- Direct SQL-based catalog. Simple, Spark-only. Does not implement the REST protocol.
2. **Unity Catalog OSS** (optional) -- REST-based catalog that implements the Iceberg REST protocol. Enables multi-engine access (DuckDB, Trino, etc.).

The UC OSS implementation is configured in `docker-compose-unity-catalog.yml` and accessed at `http://localhost:8080/api/2.1/unity-catalog/iceberg`.

### How Multi-Engine Access Works via REST

```
                     ┌──────────────────────────┐
                     │   Unity Catalog OSS      │
                     │   (REST Catalog)         │
                     │   http://localhost:8080   │
                     └─────────┬────────────────┘
                               │
              ┌────────────────┼────────────────┐
              │                │                │
              ▼                ▼                ▼
       ┌─────────────┐ ┌─────────────┐  ┌─────────────┐
       │   Spark     │ │   DuckDB    │  │   Trino     │
       │   4.1       │ │   1.x       │  │   4xx       │
       │             │ │             │  │             │
       │ RESTCatalog │ │ iceberg ext │  │ iceberg     │
       │ (Java)      │ │ (REST)      │  │ connector   │
       └──────┬──────┘ └──────┬──────┘  └──────┬──────┘
              │                │                │
              └────────────────┼────────────────┘
                               │
                               ▼
                     ┌──────────────────────────┐
                     │     SeaweedFS (S3)       │
                     │  s3://lakehouse/warehouse │
                     └──────────────────────────┘
```

Each engine talks to the same REST catalog, gets the same metadata file location, reads the same Parquet files from the same object storage. The data is written once and read by many engines.

---

## 5. Parquet as the Foundation

All three OTFs store their actual row data in Apache Parquet files. Parquet is the substrate. Understanding its internals is necessary for understanding OTF performance characteristics.

### What Is Parquet?

Apache Parquet is a **columnar storage format** originally developed at Twitter and Cloudera, inspired by Google's Dremel paper (2010) [29]. It is now an Apache top-level project. The current format version is Parquet format v2.10 (supported via parquet-mr 1.15.x and parquet-rs) [30].

### Columnar vs Row-Oriented Storage

```
ROW-ORIENTED (CSV, JSON, Avro):
┌────────────┬─────────────┬──────────┬──────────┐
│ order_id   │ event_type  │ ts       │ total    │
├────────────┼─────────────┼──────────┼──────────┤
│ ORD-001    │ created     │ 10:00:00 │ 25.99    │  ← Row 1 (contiguous)
│ ORD-002    │ created     │ 10:01:00 │ 18.50    │  ← Row 2 (contiguous)
│ ORD-003    │ delivered   │ 10:45:00 │ 42.00    │  ← Row 3 (contiguous)
└────────────┴─────────────┴──────────┴──────────┘

COLUMNAR (Parquet):
┌────────────────────────────────────────────────┐
│ order_id:   [ORD-001, ORD-002, ORD-003]       │  ← Column 1 (contiguous)
│ event_type: [created, created, delivered]       │  ← Column 2 (contiguous)
│ ts:         [10:00:00, 10:01:00, 10:45:00]     │  ← Column 3 (contiguous)
│ total:      [25.99, 18.50, 42.00]              │  ← Column 4 (contiguous)
└────────────────────────────────────────────────┘
```

Columnar storage matters for analytics because:

1. **Column pruning**: `SELECT order_id, total FROM orders` reads only two columns. In a row-oriented format, you read all four columns for every row and discard the ones you don't need. In Parquet, you read only the bytes for `order_id` and `total`. For wide tables (50+ columns), this can be a 10-25x I/O reduction.

2. **Compression**: Values in the same column tend to be similar (same data type, similar range, repeated values). This makes columnar data much more compressible than row-oriented data. A column of `event_type` values with only 8 distinct values compresses dramatically with dictionary encoding.

3. **Vectorized processing**: Modern CPUs process data faster when operating on arrays of the same type (SIMD instructions, cache-line efficiency). Columnar data is naturally structured for vectorized execution.

### Parquet File Layout

```
┌──────────────────────────────────────────────────────────────┐
│                    PARQUET FILE                                │
│                                                               │
│  ┌─────────────────────────────────────────────────────────┐ │
│  │ Magic: PAR1 (4 bytes)                                    │ │
│  └─────────────────────────────────────────────────────────┘ │
│                                                               │
│  ┌─────────────────────────────────────────────────────────┐ │
│  │ ROW GROUP 0                                              │ │
│  │ ┌─────────────┐ ┌─────────────┐ ┌─────────────┐        │ │
│  │ │ Column Chunk│ │ Column Chunk│ │ Column Chunk│  ...    │ │
│  │ │ (order_id)  │ │ (event_type)│ │ (ts)        │        │ │
│  │ │             │ │             │ │             │        │ │
│  │ │ ┌─────────┐│ │ ┌─────────┐│ │ ┌─────────┐│        │ │
│  │ │ │ Page 0  ││ │ │ Page 0  ││ │ │ Page 0  ││        │ │
│  │ │ │(dict pg)││ │ │(dict pg)││ │ │(data pg)││        │ │
│  │ │ ├─────────┤│ │ ├─────────┤│ │ ├─────────┤│        │ │
│  │ │ │ Page 1  ││ │ │ Page 1  ││ │ │ Page 1  ││        │ │
│  │ │ │(data pg)││ │ │(data pg)││ │ │(data pg)││        │ │
│  │ │ ├─────────┤│ │ └─────────┘│ │ └─────────┘│        │ │
│  │ │ │ Page 2  ││ │            │ │             │        │ │
│  │ │ │(data pg)││ │            │ │             │        │ │
│  │ │ └─────────┘│ └─────────────┘ └─────────────┘        │ │
│  │ └─────────────┘                                         │ │
│  └─────────────────────────────────────────────────────────┘ │
│                                                               │
│  ┌─────────────────────────────────────────────────────────┐ │
│  │ ROW GROUP 1                                              │ │
│  │  (same structure)                                        │ │
│  └─────────────────────────────────────────────────────────┘ │
│                                                               │
│  ┌─────────────────────────────────────────────────────────┐ │
│  │ FOOTER                                                   │ │
│  │ • File schema (Thrift-encoded)                           │ │
│  │ • Row group metadata:                                    │ │
│  │   - Column chunk offsets and sizes                       │ │
│  │   - Page offsets within column chunks                    │ │
│  │   - Column statistics (min, max, null_count)             │ │
│  │ • Key-value metadata (e.g., Iceberg schema, Arrow IPC)  │ │
│  │ • Footer length (4 bytes, little-endian)                 │ │
│  │ • Magic: PAR1 (4 bytes)                                  │ │
│  └─────────────────────────────────────────────────────────┘ │
└──────────────────────────────────────────────────────────────┘
```

### Key Concepts

**Row Groups** are horizontal partitions of the data. A typical Parquet file has one row group (Spark default: 128 MB per row group). Row groups enable parallel reading -- different threads or executors can process different row groups.

**Column Chunks** are the data for one column within one row group. Each column chunk is a contiguous byte range in the file, enabling efficient I/O: reading column `total` means seeking to its column chunk offset and reading its bytes.

**Pages** are the unit of compression and encoding within a column chunk. Default page size is 1 MB (configurable via `parquet.page.size`). Page types:

| Page Type | Purpose |
|-----------|---------|
| Dictionary page | Maps integer codes to values. Present if dictionary encoding is used. |
| Data page (v1 or v2) | Encoded column values. Repetition/definition levels for nested data. |

### Encoding and Compression

Parquet applies two layers of size reduction:

1. **Encoding** (logical): Converts values to a more compact representation.

| Encoding | How It Works | Best For |
|----------|-------------|----------|
| Dictionary | Replace values with integer codes into a dictionary | Low-cardinality columns (< ~60K distinct values) |
| Plain | Raw values, no encoding | Fallback when dictionary overflows |
| Run-Length (RLE) | Store runs of repeated values as (value, count) | Sorted or clustered columns |
| Delta Binary Packed | Store deltas between consecutive integers | Timestamps, monotonic IDs |
| Byte Stream Split | Interleave bytes of IEEE 754 floats | Floating-point columns [31] |

2. **Compression** (physical): Byte-level compression of the encoded data.

| Codec | Compression Ratio | Speed | Notes |
|-------|-------------------|-------|-------|
| Snappy | ~2-4x | Very fast | Spark default. Good balance. |
| ZSTD | ~3-7x | Fast | Better ratio than Snappy. Becoming the new standard. |
| Gzip | ~3-6x | Slower | Legacy. No advantage over ZSTD. |
| LZ4 | ~2-3x | Fastest | CPU-light workloads. |
| Brotli | ~4-8x | Slow | Rarely used for analytics. |
| Uncompressed | 1x | N/A | When CPU is the bottleneck. |

In the lakehouse-stack, Spark defaults to Snappy compression. You can change it:

```python
spark.conf.set("spark.sql.parquet.compression.codec", "zstd")
```

### Page-Level Statistics and Column Indexes

Parquet 1.11+ (the version used by Spark 4.x) supports **page-level column indexes** [32]. This is a significant optimization that many engineers are unaware of.

Without column indexes, the finest granularity of statistics is per-column-chunk (i.e., per-row-group). If a row group has 1 million rows, and you filter `WHERE total > 100.00`, you must read the entire column chunk even if only 1% of pages contain matching values.

With column indexes, the footer contains per-page min/max statistics:

```
Column Index for "total" in Row Group 0:
  Page 0: min=0.99,   max=24.99,   null_count=0
  Page 1: min=12.50,  max=89.99,   null_count=0
  Page 2: min=5.00,   max=199.99,  null_count=0
  Page 3: min=100.00, max=450.00,  null_count=0

Offset Index for "total" in Row Group 0:
  Page 0: offset=1024,   length=65536,   first_row_index=0
  Page 1: offset=66560,  length=65536,   first_row_index=10000
  Page 2: offset=132096, length=65536,   first_row_index=20000
  Page 3: offset=197632, length=65536,   first_row_index=30000
```

For `WHERE total > 100.00`, the reader checks the column index and determines that only pages 2 and 3 could contain matching values (page 0 max is 24.99, page 1 max is 89.99). It skips pages 0 and 1 entirely.

Spark enables page-level column indexes by default since Spark 3.2 (`spark.sql.parquet.columnIndex.enabled=true`). Iceberg manifests store per-file statistics; Parquet column indexes provide per-page statistics within files. Together, they create a three-level pruning hierarchy: **manifest → file → page**.

### Why Parquet Matters for OTF Portability

Parquet is the universal data currency of the analytics world. Every major engine reads Parquet natively:

- Spark, Trino, Presto, Hive (JVM, via parquet-mr)
- DuckDB (C++, via native reader)
- Polars, pandas (Python/Rust, via PyArrow or native)
- Arrow/DataFusion (Rust, via parquet-rs)
- Snowflake, BigQuery, Redshift Spectrum (internal readers)

Because OTFs store data in Parquet, the data files are inherently portable. Even without the OTF metadata layer, you can read the raw Parquet files with any engine. The OTF layer adds ACID, time travel, and schema management -- but the data itself is never locked in.

---

## 6. Data Portability in Practice

This section demonstrates what Act 1 of the OverArchitected show proves on stage: the same data, stored once, read by multiple engines.

### Reading from Spark

This is the native path. Spark has built-in Iceberg support since Spark 4.0 (previously via the `iceberg-spark-runtime` JAR).

```python
from pyspark.sql import SparkSession

spark = SparkSession.builder \
    .appName("ReadIceberg") \
    .config("spark.sql.catalog.iceberg", "org.apache.iceberg.spark.SparkCatalog") \
    .config("spark.sql.catalog.iceberg.type", "jdbc") \
    .config("spark.sql.catalog.iceberg.uri",
            "jdbc:postgresql://localhost:5432/iceberg_catalog") \
    .config("spark.sql.catalog.iceberg.warehouse", "s3a://lakehouse/warehouse") \
    .getOrCreate()

# Query via SQL
spark.sql("SELECT * FROM iceberg.bronze.orders WHERE event_type = 'delivered' LIMIT 10").show()

# Time travel
spark.sql("""
    SELECT * FROM iceberg.bronze.orders
    TIMESTAMP AS OF '2025-01-15 12:00:00'
    LIMIT 10
""").show()

# Metadata queries
spark.sql("SELECT * FROM iceberg.bronze.orders.snapshots").show()
spark.sql("SELECT * FROM iceberg.bronze.orders.manifests").show()
spark.sql("SELECT * FROM iceberg.bronze.orders.files").show()
```

### Reading from DuckDB

DuckDB can read Iceberg tables via its `iceberg` extension, which supports the REST catalog protocol [17].

```sql
-- Install and load extension
INSTALL iceberg;
LOAD iceberg;

-- Attach REST catalog (e.g., Unity Catalog OSS)
CREATE SECRET (
    TYPE ICEBERG,
    ENDPOINT 'http://localhost:8080/api/2.1/unity-catalog/iceberg',
    TOKEN 'not_used'
);

ATTACH 'unity' AS unity (TYPE ICEBERG);

-- Query
SELECT event_type, COUNT(*) as cnt
FROM unity.bronze.orders
GROUP BY event_type
ORDER BY cnt DESC;
```

Or read Parquet files directly (no catalog needed):

```sql
-- Direct Parquet read (no OTF metadata, but fully portable)
SELECT *
FROM read_parquet('s3://lakehouse/warehouse/bronze/orders/data/**/*.parquet',
                  hive_partitioning=true)
LIMIT 10;
```

### Reading from Trino

Trino has a mature Iceberg connector that supports the REST catalog protocol [15].

```sql
-- In Trino, configure the connector in etc/catalog/iceberg.properties:
-- connector.name=iceberg
-- iceberg.catalog.type=rest
-- iceberg.rest-catalog.uri=http://localhost:8080/api/2.1/unity-catalog/iceberg

-- Then query:
SELECT
    date_trunc('day', ts) AS order_date,
    COUNT(*) AS order_count,
    SUM(total) AS revenue
FROM iceberg.bronze.orders
WHERE event_type = 'order_created'
GROUP BY 1
ORDER BY 1;
```

### Reading from Polars

Polars can read Iceberg tables via its `scan_iceberg` function (requires the `iceberg` extra) or read Parquet files directly [33].

```python
import polars as pl

# Direct Parquet read (always works, no catalog dependency)
df = pl.scan_parquet(
    "s3://lakehouse/warehouse/bronze/orders/data/**/*.parquet",
    hive_partitioning=True,
    storage_options={
        "aws_access_key_id": "...",
        "aws_secret_access_key": "...",
        "aws_endpoint_url": "http://localhost:8333",
    }
).collect()

print(df.head(10))
print(f"Rows: {df.shape[0]:,}")
print(f"Event types: {df['event_type'].unique().to_list()}")
```

### Reading from pandas + PyArrow

The `pyiceberg` library [21] provides a Python-native Iceberg client that can connect to REST catalogs.

```python
from pyiceberg.catalog import load_catalog
import pyarrow as pa

catalog = load_catalog(
    "unity",
    type="rest",
    uri="http://localhost:8080/api/2.1/unity-catalog/iceberg",
    token="not_used",
    **{
        "s3.endpoint": "http://localhost:8333",
        "s3.access-key-id": "...",
        "s3.secret-access-key": "..."
    }
)

# Load table
table = catalog.load_table("bronze.orders")

# Read as Arrow table (zero-copy)
arrow_table = table.scan(
    row_filter="event_type == 'delivered'",
    selected_fields=("order_id", "ts", "total")
).to_arrow()

# Convert to pandas
df = arrow_table.to_pandas()
print(f"Delivered orders: {len(df):,}")
print(df.describe())

# Inspect metadata
print(f"Current snapshot: {table.current_snapshot()}")
print(f"Schema: {table.schema()}")
print(f"Partition spec: {table.spec()}")
```

### The Portability Hierarchy

There are multiple levels of portability, from most universal to most feature-rich:

```
Level 1: Raw Parquet files
─────────────────────────
  Any tool that reads Parquet can access the data.
  No schema evolution, no time travel, no ACID.
  Works everywhere. Always.

Level 2: Parquet + Hive-style partitioning
──────────────────────────────────────────
  Directory structure encodes partitions (date=2025-01-15/).
  Partition pruning works. Schema embedded in Parquet footer.
  Most tools support this pattern.

Level 3: Iceberg metadata (local/Hadoop catalog)
─────────────────────────────────────────────────
  Full OTF features: ACID, time travel, schema evolution.
  Requires reading metadata files from object storage.
  pyiceberg, DuckDB iceberg extension, Spark all support this.

Level 4: Iceberg REST Catalog
─────────────────────────────
  Standard HTTP API for table discovery and credential vending.
  Multi-engine, multi-tenant, governed access.
  Unity Catalog OSS, Polaris, Nessie, AWS Glue implement this.
```

The lakehouse-stack supports all four levels. Act 1 demonstrates levels 1-3; Act 2 (Unity Catalog setup) demonstrates level 4.

---

## 7. Schema Evolution

Schema evolution is the ability to change a table's schema (add, rename, drop, or reorder columns) without rewriting existing data files. This is one of Iceberg's strongest features, and the design is worth understanding in detail.

### How Iceberg Does It: Column IDs

The key insight is that Iceberg assigns a **unique integer ID** to every column at creation time. These IDs are immutable -- they never change, even if the column is renamed or moved. The ID-to-name mapping is stored in the metadata file, not in the Parquet files.

```
Metadata v1 (schema-id: 0):
  id=1  name="order_id"    type=string
  id=2  name="event_type"  type=string
  id=3  name="ts"          type=timestamp

Metadata v2 (schema-id: 1):
  id=1  name="order_id"    type=string
  id=2  name="event_type"  type=string
  id=3  name="event_time"  type=timestamp     ← RENAMED (id unchanged)
  id=9  name="brand_id"    type=int           ← ADDED (new id)
```

When Spark reads a Parquet file written under schema-id 0, it uses the ID mapping to resolve columns:

1. Read the Parquet footer to get the file's schema (columns named `order_id`, `event_type`, `ts`)
2. Look up each Parquet column in the Iceberg field-ID metadata embedded in the Parquet file's key-value metadata (`iceberg.schema`)
3. Map field ID 3 to the current name `event_time`
4. Column `brand_id` (id=9) is not in the Parquet file, so it returns NULL for all rows

No data rewrite. No backfill. The old Parquet file is read correctly with the new schema.

### Supported Schema Changes

| Operation | Requires Rewrite? | How It Works |
|-----------|-------------------|-------------|
| Add column | No | New column ID assigned. Old files return NULL for the new column. |
| Drop column | No | Column ID removed from current schema. Old files still contain the data but it is not projected. |
| Rename column | No | Column ID unchanged, name updated in metadata. Old Parquet files still use old name but mapping is by ID. |
| Reorder columns | No | Projection order changed in metadata. Physical file layout unchanged. |
| Widen type (int → long) | No | Metadata type updated. Parquet reader handles promotion. [5, Type Promotion] |
| Narrow type (long → int) | Not allowed | Would lose precision. Iceberg forbids this. |
| Change type (string → int) | Not allowed | Incompatible types. Requires creating a new column. |
| Make required → optional | No | Metadata change only. |
| Make optional → required | Not allowed (for existing data) | Existing NULLs would violate the constraint. |

### Comparison with Delta Lake and Hudi

**Delta Lake** uses column **names** (not IDs) for schema mapping. This means:

- Renaming a column requires enabling column mapping mode (`delta.columnMapping.mode = name`), which was added in Delta Lake 2.0 [12].
- Without column mapping mode, renaming a column requires rewriting all data files.
- Column mapping mode adds a `delta.columnMapping.id` to each column in the Delta log, similar to Iceberg's approach -- but it is opt-in rather than the default.

**Hudi** also uses column names by default. Schema evolution support varies by table type (Copy-on-Write vs Merge-on-Read) and storage format.

This is one of the clearest technical advantages of Iceberg's design: schema evolution was a first-class concern from the beginning, not a feature added later.

### Practical Example in the Lakehouse-Stack

```python
from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("SchemaEvolution").getOrCreate()

# Add a column
spark.sql("""
    ALTER TABLE iceberg.bronze.orders
    ADD COLUMNS (brand_id INT COMMENT 'Brand identifier from dimension table')
""")

# Rename a column
spark.sql("""
    ALTER TABLE iceberg.bronze.orders
    RENAME COLUMN ts TO event_time
""")

# Widen a type
spark.sql("""
    ALTER TABLE iceberg.bronze.orders
    ALTER COLUMN sequence TYPE BIGINT
""")

# Drop a column
spark.sql("""
    ALTER TABLE iceberg.bronze.orders
    DROP COLUMN body
""")

# Verify: read old data with new schema
spark.sql("SELECT order_id, event_time, brand_id FROM iceberg.bronze.orders LIMIT 5").show()
# brand_id will be NULL for rows written before the column was added
# event_time maps to the original ts column via field ID
```

---

## 8. Partition Evolution

Partition evolution is the ability to change how a table is partitioned without rewriting existing data. This is arguably Iceberg's most unique feature -- neither Delta Lake nor Hudi support this natively.

### The Problem with Traditional Partitioning

In Hive-style tables, partitions are physical directory paths:

```
s3://bucket/orders/year=2025/month=01/day=15/part-00000.parquet
s3://bucket/orders/year=2025/month=01/day=16/part-00000.parquet
```

If you decide to change from daily to hourly partitioning, you must:
1. Create a new table with the hourly partition scheme
2. Rewrite all data from the old table into the new one
3. Update all downstream consumers to point to the new table
4. Drop the old table

For a 10 TB table, this is a multi-hour, expensive, error-prone operation that requires downtime.

### How Iceberg Solves It: Hidden Partitioning

Iceberg partitions are defined as **transforms** on source columns, not as physical directory structures. The partition spec is metadata:

```json
{
  "spec-id": 0,
  "fields": [
    {"source-id": 3, "field-id": 1000, "name": "ts_day", "transform": "day"}
  ]
}
```

This says: "Partition by the `day` transform applied to column `ts` (field ID 3)." Users never see the partition column in their queries. They write:

```sql
SELECT * FROM orders WHERE ts >= '2025-01-15'
```

Iceberg automatically applies partition pruning using the `day(ts)` transform. The user does not need to know that the table is partitioned by day. This is "hidden partitioning" [5].

### Partition Evolution in Action

Now suppose your table grows and you need hourly partitioning for recent data. With Iceberg, you simply update the partition spec:

```python
# Via Spark SQL (Iceberg 1.4+)
spark.sql("""
    ALTER TABLE iceberg.bronze.orders
    SET PARTITION SPEC (hour(event_time))
""")
```

Or via the Iceberg Java API:

```java
table.updateSpec()
    .removeField("ts_day")
    .addField(Transforms.hour("event_time"))
    .commit();
```

What happens internally:

1. A new partition spec (spec-id: 1) is added to the metadata file.
2. The old partition spec (spec-id: 0) is preserved.
3. Existing data files retain their original partition spec. Files partitioned by day remain partitioned by day.
4. New writes use the new partition spec. New files are partitioned by hour.

```
Before evolution:
  metadata.partition-specs[0] = day(ts)
  manifest-m0: file-A (ts_day=2025-01-15), file-B (ts_day=2025-01-16)

After evolution:
  metadata.partition-specs[0] = day(ts)
  metadata.partition-specs[1] = hour(event_time)
  metadata.default-spec-id = 1

  manifest-m0: file-A (ts_day=2025-01-15), file-B (ts_day=2025-01-16)
  manifest-m1: file-C (event_time_hour=2025-01-17-08), file-D (event_time_hour=2025-01-17-09)
```

When a query scans the table, Iceberg applies the appropriate pruning logic for each manifest based on its partition spec. Old manifests use day-level pruning; new manifests use hour-level pruning. Both coexist in the same table.

### Available Partition Transforms

| Transform | Input Type | Output | Example |
|-----------|-----------|--------|---------|
| `identity` | Any | Same value | `identity(region)` -- partition by exact region value |
| `year` | timestamp/date | Integer year | `year(ts)` -- `2025` |
| `month` | timestamp/date | Integer year-month | `month(ts)` -- `2025-01` (stored as months from epoch) |
| `day` | timestamp/date | Integer date | `day(ts)` -- `2025-01-15` (stored as days from epoch) |
| `hour` | timestamp | Integer date-hour | `hour(ts)` -- `2025-01-15-08` |
| `bucket[N]` | Any | Integer 0..N-1 | `bucket[16](order_id)` -- hash mod 16 |
| `truncate[W]` | string/int/long | Truncated value | `truncate[4](zip_code)` -- `"9410"` for `"94107"` |
| `void` | Any | Always null | Used to remove a partition field without rewrite |

The `bucket` and `truncate` transforms are particularly useful for high-cardinality columns where identity partitioning would create too many partitions (the "small files problem").

### Why Delta Lake and Hudi Cannot Do This

**Delta Lake** encodes partitions as physical directory paths, just like Hive. Changing the partition scheme requires rewriting data. Delta's "liquid clustering" feature (Databricks-only as of 2025, open-sourced in delta-spark 4.0 [34]) provides a different approach: instead of traditional partitioning, it uses Z-order clustering within data files, which can be incrementally reorganized. This is a valid alternative, but it is a fundamentally different mechanism, not partition evolution.

**Hudi** also uses directory-based partitioning. Changing the partition scheme requires data rewriting.

Iceberg's partition evolution works because the partition spec is metadata that is interpreted at query time, not a physical directory layout that must be materialized on disk. This decoupling of logical partition semantics from physical file organization is a direct consequence of Iceberg's manifest-based file tracking.

---

## 9. Time Travel

Time travel is the ability to query a table as it existed at a previous point in time. All three OTFs support this, but the mechanisms differ.

### How Iceberg Time Travel Works

Every commit to an Iceberg table creates a new snapshot. Each snapshot is immutable and references a specific set of data files via its manifest list. Snapshots are retained according to the table's `history.expire.max-snapshot-age-ms` property (default: 5 days) and `history.expire.min-snapshots-to-keep` (default: 1) [5].

```
Snapshot Timeline:
  snap-001 (2025-01-15 08:00) → manifest-list-A → [file-1, file-2]
  snap-002 (2025-01-15 12:00) → manifest-list-B → [file-1, file-2, file-3]
  snap-003 (2025-01-16 08:00) → manifest-list-C → [file-2, file-3, file-4]
                                                     (file-1 compacted away)
  snap-004 (2025-01-16 12:00) → manifest-list-D → [file-2, file-3, file-4, file-5]

Current snapshot: snap-004
```

### Query Syntax

```python
# By snapshot ID
spark.read.option("snapshot-id", 7035700953559098903) \
    .table("iceberg.bronze.orders") \
    .show()

# By timestamp (returns the snapshot that was current at that time)
spark.read.option("as-of-timestamp", "1705305600000") \
    .table("iceberg.bronze.orders") \
    .show()

# SQL syntax
spark.sql("""
    SELECT COUNT(*), MAX(total)
    FROM iceberg.bronze.orders
    TIMESTAMP AS OF '2025-01-15 12:00:00'
""").show()

spark.sql("""
    SELECT COUNT(*), MAX(total)
    FROM iceberg.bronze.orders
    VERSION AS OF 7035700953559098903
""").show()
```

### Snapshot Metadata Queries

Iceberg exposes metadata tables that let you explore the snapshot history:

```sql
-- List all snapshots
SELECT snapshot_id, committed_at, operation, summary
FROM iceberg.bronze.orders.snapshots
ORDER BY committed_at;

-- List all data files in a specific snapshot
SELECT file_path, file_format, record_count, file_size_in_bytes
FROM iceberg.bronze.orders.files
ORDER BY file_size_in_bytes DESC;

-- List all manifest files
SELECT path, length, added_data_files_count, partition_summaries
FROM iceberg.bronze.orders.manifests;

-- History of snapshot changes
SELECT * FROM iceberg.bronze.orders.history;

-- List all metadata log entries
SELECT * FROM iceberg.bronze.orders.metadata_log_entries;
```

### Rollback

Time travel is read-only. If you want to actually revert the table to a previous state, use rollback:

```sql
-- Rollback to a specific snapshot
CALL iceberg.system.rollback_to_snapshot('bronze.orders', 7035700953559098903);

-- Rollback to a timestamp
CALL iceberg.system.rollback_to_timestamp('bronze.orders', TIMESTAMP '2025-01-15 12:00:00');
```

Rollback creates a new snapshot whose manifest list points to the same files as the target snapshot. It does not delete any files. The intervening snapshots remain in the history.

### Incremental Reads

Iceberg supports reading only the changes between two snapshots. This is essential for incremental ETL pipelines:

```python
# Read only rows added between two snapshots
spark.read \
    .option("start-snapshot-id", snap_001_id) \
    .option("end-snapshot-id", snap_004_id) \
    .table("iceberg.bronze.orders") \
    .show()

# In Spark SQL (Iceberg 1.4+)
spark.sql(f"""
    SELECT * FROM iceberg.bronze.orders
    CHANGES BETWEEN {snap_001_id} AND {snap_004_id}
""").show()
```

Incremental reads work by examining the manifest lists of the start and end snapshots, finding manifests and files that were added in between, and reading only those files. This is much more efficient than a full scan with a timestamp filter.

### Branching and Tagging

Since Iceberg 1.2 (SPARK-40510) [14], tables support branches and tags:

```sql
-- Create a tag (named reference to a snapshot, immutable)
ALTER TABLE iceberg.bronze.orders CREATE TAG `release-2025-01-15`
    AS OF VERSION 7035700953559098903;

-- Create a branch (mutable reference, like a git branch)
ALTER TABLE iceberg.bronze.orders CREATE BRANCH `staging`
    AS OF VERSION 7035700953559098903;

-- Write to a branch
spark.conf.set("spark.wap.branch", "staging")
-- ... writes go to the staging branch ...

-- Read from a branch
spark.read.option("branch", "staging") \
    .table("iceberg.bronze.orders") \
    .show()

-- Merge branch (fast-forward)
CALL iceberg.system.fast_forward('bronze.orders', 'main', 'staging');
```

This enables **Write-Audit-Publish (WAP)** workflows where data is written to a staging branch, validated, and then promoted to production by advancing the main branch pointer.

### Snapshot Maintenance

Snapshots accumulate over time and must be cleaned up to avoid unbounded metadata growth and storage consumption:

```sql
-- Expire old snapshots (deletes metadata, marks data files for removal)
CALL iceberg.system.expire_snapshots(
    table => 'bronze.orders',
    older_than => TIMESTAMP '2025-01-10 00:00:00',
    retain_last => 5
);

-- Remove orphan files (data files not referenced by any snapshot)
CALL iceberg.system.remove_orphan_files(
    table => 'bronze.orders',
    older_than => TIMESTAMP '2025-01-10 00:00:00'
);

-- Rewrite data files (compaction -- merge small files into larger ones)
CALL iceberg.system.rewrite_data_files(
    table => 'bronze.orders',
    strategy => 'binpack',
    options => map('target-file-size-bytes', '134217728')
);

-- Rewrite manifests (merge small manifests)
CALL iceberg.system.rewrite_manifests('bronze.orders');
```

In the lakehouse-stack, these maintenance operations are automated via Airflow DAGs (see `dags/iceberg_maintenance.py`).

---

## 10. The lakehouse-stack Implementation

This section describes how the OverArchitected show's "Casper's Kitchen" data is stored in the lakehouse-stack.

### Architecture Overview

```
┌────────────────────────────────────────────────────────────────────────┐
│                        LAKEHOUSE-STACK                                  │
│                                                                         │
│  ┌───────────────┐    ┌───────────────────────────────────────────┐   │
│  │  Test Data    │    │            Spark 4.1                      │   │
│  │  Generator    │    │                                           │   │
│  │              │───▶│  spark.read.parquet("/data/events/...")    │   │
│  │  Dimensions: │    │  df.writeTo("iceberg.bronze.orders")      │   │
│  │  - locations │    │             .append()                      │   │
│  │  - brands    │    │                                           │   │
│  │  - items     │    └──────────────┬────────────────────────────┘   │
│  │  - categories│                   │                                 │
│  │              │                   │ Iceberg commit                  │
│  │  Events:     │                   ▼                                 │
│  │  - orders    │    ┌───────────────────────────────────────────┐   │
│  └───────────────┘   │        PostgreSQL JDBC Catalog             │   │
│                       │                                           │   │
│                       │  iceberg_tables:                          │   │
│                       │    bronze.orders →                        │   │
│                       │      s3a://lakehouse/warehouse/           │   │
│                       │        bronze/orders/metadata/v12.json    │   │
│                       │    bronze.dim_locations → ...              │   │
│                       │    bronze.dim_brands → ...                 │   │
│                       └──────────────┬────────────────────────────┘   │
│                                      │                                 │
│                                      │ metadata pointer               │
│                                      ▼                                 │
│                       ┌───────────────────────────────────────────┐   │
│                       │         SeaweedFS (S3-compatible)          │   │
│                       │         http://localhost:8333              │   │
│                       │                                           │   │
│                       │  s3://lakehouse/warehouse/                │   │
│                       │  ├── bronze/                              │   │
│                       │  │   ├── orders/                         │   │
│                       │  │   │   ├── metadata/                   │   │
│                       │  │   │   │   ├── v1.metadata.json        │   │
│                       │  │   │   │   ├── v12.metadata.json       │   │
│                       │  │   │   │   ├── snap-*.avro             │   │
│                       │  │   │   │   └── *.avro (manifests)      │   │
│                       │  │   │   └── data/                       │   │
│                       │  │   │       ├── ts_day=2025-01-15/      │   │
│                       │  │   │       │   └── *.parquet           │   │
│                       │  │   │       └── ts_day=2025-01-16/      │   │
│                       │  │   │           └── *.parquet           │   │
│                       │  │   ├── dim_locations/                  │   │
│                       │  │   ├── dim_brands/                     │   │
│                       │  │   ├── dim_items/                      │   │
│                       │  │   └── dim_categories/                 │   │
│                       │  ├── silver/                              │   │
│                       │  └── gold/                                │   │
│                       └───────────────────────────────────────────┘   │
└────────────────────────────────────────────────────────────────────────┘
```

### The Casper's Kitchen Dataset

The test data generator (`./lakehouse testdata generate`) creates a simulated food delivery platform with:

**Dimension tables** (Parquet files at `/data/dimensions/`):

| Table | Rows | Description |
|-------|------|-------------|
| `locations` | ~50 | Restaurant locations with lat/lon, city, capacity |
| `brands` | ~20 | Restaurant brands with cuisine type, momentum score |
| `items` | ~200 | Menu items with price, category, dietary flags |
| `categories` | ~15 | Menu categories |

**Event data** (Parquet at `/data/events/`):

| File | Description |
|------|-------------|
| `orders_7d.parquet` | 7 days of order lifecycle events |
| `orders_30d.parquet` | 30 days |
| `orders_90d.parquet` | 90 days |

Each order goes through a lifecycle of events:

```
order_created → confirmed → preparing → ready → picked_up → in_transit → delivered
                    └──────────────────────────────────────────────────→ cancelled
```

Each event has a `body` field containing JSON with event-specific data:

```json
// order_created
{"brand_id": 3, "total": 42.99, "items": [{"name": "Pad Thai", "price": 15.99}, ...]}

// delivered
{"delivery_lat": 37.77, "delivery_lon": -122.41, "total_mins": 38.5, "driver_id": "D-042"}

// cancelled
{"reason": "customer_request", "refund_amount": 42.99}
```

The test data generator intentionally injects data quality issues (null locations, malformed JSON, missing order IDs) to make the demo realistic. This chaos injection is visible in Act 1's data quality check.

### Why This Architecture

The lakehouse-stack makes specific architectural choices that reflect the OSS Iceberg ecosystem:

**PostgreSQL JDBC catalog** (default) because:
- Simplest possible catalog -- a single SQL table mapping table names to metadata file paths
- No additional services to run
- Atomic commits via SQL transactions
- Limitation: only Spark can use it directly (no REST API)

**SeaweedFS** as object storage because:
- S3-compatible API (works with Hadoop's `s3a://` filesystem)
- Single binary, trivial to deploy
- No cloud dependency -- fully local development
- Limitation: not production-grade for petabyte-scale (use actual S3/GCS/ADLS for that)

**Unity Catalog OSS** (optional) for multi-engine access because:
- Implements the Iceberg REST Catalog Protocol
- Credential vending for S3 access
- Enables DuckDB, Trino, Polars to query the same tables Spark writes
- Limitation: UC OSS 0.3.1 lacks some features of the Databricks-managed version (no fine-grained access control, no lineage tracking)

This stack proves that you can build a functional lakehouse with zero cloud dependency and zero vendor lock-in. The tradeoff is operational complexity -- you are responsible for maintaining every component.

---

## 11. Migration Considerations

If you are moving from a proprietary platform (Databricks, Snowflake, BigQuery) to an open lakehouse, or from Hive tables to Iceberg, here are the practical considerations.

### Migrating from Hive Tables to Iceberg

Iceberg supports in-place migration from Hive tables via the `migrate` procedure [35]:

```sql
-- In-place migration: converts Hive table metadata to Iceberg
-- Does NOT rewrite data files. Creates Iceberg metadata pointing to existing Parquet files.
CALL iceberg.system.migrate('db.hive_table');

-- Snapshot migration: creates an Iceberg table that points to the Hive data
-- Original Hive table is preserved.
CALL iceberg.system.snapshot('db.hive_table', 'iceberg.db.migrated_table');

-- Add files from a Hive-style directory to an existing Iceberg table
CALL iceberg.system.add_files(
    table => 'iceberg.bronze.orders',
    source_table => 'hive.bronze.orders'
);
```

The `migrate` procedure works by:
1. Reading the Hive metastore for the table's schema, partition spec, and file locations
2. Creating Iceberg metadata files (metadata JSON, manifest list, manifest files) that reference the existing Parquet data files
3. Updating the catalog to point to the new Iceberg metadata

**No data is copied or rewritten.** The Parquet files stay where they are. Only new metadata is created. This makes migration fast (minutes, not hours) regardless of table size.

Limitations of in-place migration:
- The Hive table must use Parquet or ORC format (CSV/JSON cannot be migrated in-place)
- Partition columns in Hive are physical columns; Iceberg treats them as transforms. Identity partitions are preserved; non-identity transforms require creating a new partition spec.
- After migration, the table is managed by Iceberg. Hive-style writes (direct file drops) will bypass Iceberg metadata and cause inconsistency.

### Migrating from Delta Lake to Iceberg

There is no built-in `migrate` procedure for Delta-to-Iceberg. Options:

1. **CTAS (Create Table As Select)**: Read the Delta table and write it as Iceberg. This rewrites all data.

```sql
CREATE TABLE iceberg.bronze.orders
USING iceberg
AS SELECT * FROM delta.`s3://bucket/delta/orders`;
```

2. **Delta UniForm**: If you are on Databricks or using delta-spark 4.0+, enable UniForm to automatically generate Iceberg metadata alongside Delta metadata [6]. This is not a migration -- it is a compatibility layer that lets Iceberg readers access Delta tables.

3. **Parquet-level migration**: Since both Delta and Iceberg store data in Parquet, you can use Iceberg's `add_files` procedure to import the Parquet files from a Delta table's data directory, then build Iceberg metadata on top. This avoids rewriting data but requires careful handling of deletion vectors and transaction log state.

### Migrating from Databricks to OSS

This is the scenario dramatized in the OverArchitected show. Practical considerations:

**What you keep:**
- All data files (Parquet) -- fully portable
- Table schemas -- encoded in Parquet footers and Delta/Iceberg metadata
- Partition structure -- directory layout is standard
- Query logic -- PySpark code is the same OSS Spark code

**What you lose:**
- Unity Catalog (Databricks-managed) -- replace with UC OSS or Polaris
- Photon engine -- replace with Spark's Tungsten/Whole-Stage Code Generation
- Serverless compute -- replace with self-managed Spark clusters or Kubernetes
- Delta Sharing -- replace with Iceberg REST Catalog + credential vending
- ML model serving -- replace with MLflow OSS
- Workflows (Databricks Jobs) -- replace with Airflow or similar
- Data lineage and governance UI -- no direct OSS equivalent with full feature parity
- Cluster auto-scaling -- implement via Kubernetes Horizontal Pod Autoscaler or YARN

**Honest assessment of the gap:**
The OSS stack provides ~80% of the Databricks platform's functionality. The missing 20% is primarily operational tooling: cluster management UI, cost optimization, serverless elasticity, and integrated governance. These are real gaps that require engineering effort to fill. The OverArchitected show's premise -- that two engineers can rebuild the platform themselves -- is intentionally absurd. The point is not that it is easy, but that it is possible, and that data portability makes it feasible.

### Migration Checklist

1. **Inventory your tables**: List all tables, their formats (Delta, Parquet, CSV), sizes, and access patterns. Prioritize large, frequently-queried tables.

2. **Set up the target catalog**: Deploy an Iceberg catalog (PostgreSQL JDBC for simple setups, REST catalog for multi-engine access).

3. **Configure object storage**: Ensure your Spark cluster can access the target storage (S3, GCS, ADLS, MinIO, SeaweedFS) via the appropriate Hadoop filesystem connector.

4. **Migrate metadata first**: Use in-place migration (`migrate`, `snapshot`, `add_files`) where possible. Avoid data rewrites.

5. **Validate**: Compare row counts, column statistics, and sample data between source and target.

6. **Update consumers**: Point downstream jobs, dashboards, and ad-hoc tools to the new Iceberg tables.

7. **Set up maintenance**: Configure snapshot expiration, orphan file removal, and compaction. These are not optional for production Iceberg tables.

8. **Monitor**: Track table growth, snapshot count, manifest count, and small file accumulation. Iceberg provides metadata tables for all of these.

---

## 12. References

[1] AWS S3 ListObjectsV2 documentation. Lists up to 1,000 objects per request, requiring pagination for larger prefixes. https://docs.aws.amazon.com/AmazonS3/latest/API/API_ListObjectsV2.html

[2] Apache Iceberg 1.10.0 release. March 2025. https://iceberg.apache.org/releases/

[3] Delta Lake 4.0.0 release. November 2024. Linux Foundation project. https://github.com/delta-io/delta/releases/tag/v4.0.0

[4] Apache Hudi 1.0.1 release. February 2025. https://hudi.apache.org/releases/

[5] Iceberg Table Spec v2. Formal specification covering table metadata, schemas, partition specs, snapshots, manifest lists, manifest files, and data files. https://iceberg.apache.org/spec/

[6] Delta Universal Format (UniForm). Generates Iceberg-compatible metadata alongside Delta metadata for multi-engine reads. https://docs.delta.io/latest/delta-uniform.html

[7] Iceberg Reliability documentation. Discusses why filesystem-based catalog implementations (Hadoop catalog) are not safe for production on object stores due to non-atomic rename operations. https://iceberg.apache.org/docs/latest/reliability/

[8] Iceberg Conflict Resolution. Documentation on how Iceberg handles concurrent write conflicts with operation-aware retry. https://iceberg.apache.org/docs/latest/reliability/#concurrent-write-operations

[9] Iceberg Format v2: Row-Level Deletes. Added position delete files and equality delete files. https://iceberg.apache.org/spec/#row-level-deletes

[10] Iceberg Format v3 (in progress). Tracking issue for v3 features including row lineage, default values, nanosecond timestamps. https://github.com/apache/iceberg/issues/6437

[11] Delta Lake Protocol specification. Defines the transaction log format, checkpoint files, and reader/writer protocol versions. https://github.com/delta-io/delta/blob/master/PROTOCOL.md

[12] Delta Lake Column Mapping. Feature added in Delta 2.0 to support column rename and drop without data rewrite. https://docs.delta.io/latest/delta-column-mapping.html

[13] Delta Lake Deletion Vectors. Feature added in Delta 3.1 for efficient row-level deletes without full file rewrite. https://github.com/delta-io/delta/blob/master/PROTOCOL.md#deletion-vectors

[14] Iceberg Branching and Tagging. Added in Iceberg 1.2. SPARK-40510 for Spark integration. https://iceberg.apache.org/docs/latest/branching/

[15] Trino Iceberg Connector. Full read/write support with REST, Hive, Glue, and Nessie catalogs. https://trino.io/docs/current/connector/iceberg.html

[16] Trino Delta Lake Connector. https://trino.io/docs/current/connector/delta-lake.html

[17] DuckDB Iceberg Extension. Read-only support for Iceberg tables via REST catalog or metadata files. https://duckdb.org/docs/extensions/iceberg.html

[18] Snowflake Iceberg Tables. External catalog support for reading Iceberg tables. https://docs.snowflake.com/en/user-guide/tables-iceberg

[19] Google BigQuery BigLake Iceberg Tables. Managed Iceberg tables with multi-engine access. https://cloud.google.com/bigquery/docs/iceberg-tables

[20] AWS Athena Iceberg support. https://docs.aws.amazon.com/athena/latest/ug/querying-iceberg.html

[21] PyIceberg. Python-native Iceberg client library for reading/writing Iceberg tables without a JVM. https://py.iceberg.apache.org/

[22] Delta Kernel. A library for building Delta Lake connectors without depending on Spark. https://github.com/delta-io/delta/tree/master/kernel

[23] Iceberg REST Catalog Protocol specification. Defines the HTTP API for catalog operations, table loading, and commit. https://iceberg.apache.org/spec/#rest-catalog

[24] Iceberg REST Catalog credential vending. Part of the REST catalog spec, where the catalog provides short-lived storage credentials scoped to specific tables. https://iceberg.apache.org/docs/latest/rest-catalog/#credential-vending

[25] Unity Catalog OSS. Open-source universal catalog supporting Iceberg REST protocol. https://github.com/unitycatalog/unitycatalog

[26] Project Nessie. Git-like catalog for data lakes with branch/merge semantics. https://projectnessie.org/

[27] Apache Polaris (Incubating). Open-source catalog service originally developed by Snowflake. https://polaris.apache.org/

[28] Apache Gravitino (Incubating). Multi-catalog federation. https://gravitino.apache.org/

[29] Dremel: Interactive Analysis of Web-Scale Datasets. Melnik et al., VLDB 2010. The paper that inspired Parquet's columnar encoding with repetition and definition levels. https://research.google/pubs/pub36632/

[30] Apache Parquet Format specification. Defines the file layout, page types, encodings, and compression codecs. https://parquet.apache.org/documentation/latest/

[31] Parquet BYTE_STREAM_SPLIT encoding. Optimized for floating-point data. PARQUET-1622. https://issues.apache.org/jira/browse/PARQUET-1622

[32] Parquet Column Index. Page-level min/max statistics for fine-grained predicate pushdown. PARQUET-1201. https://issues.apache.org/jira/browse/PARQUET-1201

[33] Polars Iceberg integration. https://docs.pola.rs/user-guide/io/iceberg/

[34] Delta Lake Liquid Clustering. Z-order-based clustering that replaces traditional partitioning. Originally Databricks-only, open-sourced in delta-spark 4.0. https://docs.delta.io/latest/delta-clustering.html

[35] Iceberg Migrate procedure. In-place migration from Hive tables to Iceberg without data rewrite. https://iceberg.apache.org/docs/latest/spark-procedures/#migrate

---

*This companion guide is part of the OverArchitected show documentation. For the full show flow and other act guides, see `scripts/demos/overarchitected/README.md`.*
