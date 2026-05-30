# AUTO CDC → SCD Type 1 — Test Coverage & Results

What was tested for Apache Spark's declarative-pipelines **AUTO CDC** API
(`create_auto_cdc_flow`, SCD Type 1), and the results. Everything here was run,
not assumed.

**Build under test:** Apache Spark `master` `5.0.0-SNAPSHOT` @ `06ffa273`
(source-built, Scala 2.13.18, Java 17), driven over a standalone Spark Connect
server. Iceberg via a runtime **ported to Spark 5.0** (see `iceberg-port/`).
Date: 2026-05-29/30.

---

## 1. Catalog compatibility — does AUTO CDC work against catalog X?

AUTO CDC requires the target to implement the DSv2 `SupportsRowLevelOperations`
contract (MERGE/UPDATE/DELETE) **and** a durable catalog for the target + its
auxiliary state table.

| # | Catalog | Storage | Result | Notes |
|---|---------|---------|:---:|-------|
| 1 | Iceberg **Hadoop** (filesystem) | local file | ✅ PASS | baseline |
| 2 | Iceberg **in-memory** (`SharedTablesInMemoryRowLevelOperationTableCatalog`) | in-JVM | ✅ PASS | pure-Spark control (no external deps) |
| 3 | Iceberg **JDBC / PostgreSQL** | local file | ✅ PASS | metadata commits in Postgres |
| 4 | Iceberg **JDBC / PostgreSQL** | **SeaweedFS S3** | ✅ **PASS** | **stack production default — full fidelity** |
| 5 | Iceberg **Hive Metastore** (thrift) | local file | ✅ PASS | S3 warehouse needs the HMS *server* to have S3 access (see §4) |
| 6 | **Gravitino** Iceberg REST (writable) | SeaweedFS S3 (via s3a) | ✅ PASS | the writable multi-engine alternative to UC |
| 7 | Iceberg **Glue Catalog** | S3 (via moto S3) | ✅ PASS | tested against **moto** (mock Glue+S3); real AWS Glue needs an account, LocalStack Glue is Pro-only |
| 8 | **Unity Catalog OSS** v0.4.1 — Iceberg REST | — | ❌ FAIL | REST endpoint is **read-only** (no `POST .../tables`) |
| 9 | **Unity Catalog OSS** / **Delta** | — | ⛔ UNSUPPORTED | Delta doesn't implement `SupportsRowLevelOperations` (see §7) |

**Sources tested:**
- File-stream (JSON glob) ✅
- **Kafka** — `readStream.format("kafka")`, AvailableNow, *synthetic* hand-produced feed ✅
- **Debezium → Kafka (real Postgres CDC), full envelope** ✅ — a live Debezium Postgres connector (logical replication, `pgoutput`) on a `customers` table; AUTO CDC parses the real envelope (`op` `r`/`c`/`u`/`d`, `before`/`after`, `source.lsn` as `sequence_by`, `op='d'`→`apply_as_deletes` with the key from `before`). Validated across **snapshot** (`r`), **initial streaming** (`u`/`d`/`c`), and **live incremental** DML re-runs (same checkpoint picks up only new offsets). `tests/debezium_source.py`.
- **Debezium *unwrapped* (ExtractNewRecordState SMT, flattened)** ✅ — the most common production consumption shape: the envelope is flattened to the `after` row + `__op`/`__deleted`/`__lsn` fields; on delete the key is taken from the Kafka message key. Live snapshot + streaming DML. `tests/debezium_unwrap_source.py`.
- **Debezium *Avro* (Confluent Schema Registry)** ✅ — the production-standard wire format: strip the 5-byte Confluent header + `from_avro` with the registry-fetched value schema; nullable-union fields decode cleanly. Live snapshot + streaming DML. `tests/debezium_avro_source.py`.
- **Iceberg table (streaming source)** ✅ — *lakehouse-to-lakehouse*: a bronze Iceberg table append-fed with CDC rows, read as a streaming source → SCD1 Iceberg target. `tests/iceberg_source.py`. (Note: Iceberg's `_change_type` *changelog* is **batch-only** in OSS — see §7 — so you stream the appends, not the changelog.)

### Behavior dimensions (Hadoop-Iceberg) — all pass
| Behavior | Result |
|----------|:---:|
| Composite keys (`keys=[region, id]`) — distinct row per key tuple | ✅ |
| Incremental re-run — streaming checkpoint picks up newly-added files, applies only new changes | ✅ |
| Full refresh (`full_refresh_all=True`) — recompute path | ✅ |
| Value-level schema evolution (column null early, populated later) | ✅ |

**Bottom line:** the stack's production catalog (Iceberg + Postgres + SeaweedFS)
works end-to-end. Among multi-client/REST catalogs, **Gravitino works, UC does
not** (read-only Iceberg REST). Delta is architecturally out (§7).

---

## 2. SCD Type 1 correctness — adversarial battery

Each case runs an isolated pipeline and asserts the end state. **All pass.**

| Case | Expectation | Result |
|------|-------------|:---:|
| Insert → update (newer seq) | latest value wins | ✅ |
| Out-of-order update (stale lower-seq arrives later) | stale ignored | ✅ |
| Multiple updates to one key in a single micro-batch | highest seq wins | ✅ |
| Delete (`apply_as_deletes`) | row removed | ✅ |
| Out-of-order delete (delete seq < latest update seq) | row **stays** (delete is stale) | ✅ |
| Stale insert after a newer delete | stays deleted | ✅ |
| Delete-before-insert across micro-batches | resolves to deleted | ✅ |
| Delete → re-insert (resurrection) | row comes back at new value | ✅ |
| UPSERT to a non-existent key | inserts | ✅ |
| DELETE of a non-existent key | no-op | ✅ |
| 500 keys, shuffled out-of-order, 100 deleted | exactly 400 survive | ✅ |
| Null `sequence_by` | hard fail `AUTOCDC_MICROBATCH_VALIDATION.NULL_SEQUENCE` | ✅ (fails loud, no silent corruption) |
| Tie on `sequence_by` (equal seq, same key) | non-deterministic | ⚠️ documented — sequence on a unique column |

---

## 3. Real-world end-to-end (ghost-kitchen data)

Driven from real test data; AUTO CDC output cross-checked **byte-for-byte against
an independent full recompute** (`realworld_e2e.py`).

| Scenario | What it proves | Result |
|----------|----------------|:---:|
| `gold.orders_live` — order **lifecycle events → current status per order** | append-only log distilled to current state; out-of-order events resolved; cancellations (`order_cancelled`) removed | ✅ PASS (4,691 live = recompute, 308 real cancellations removed) |
| `silver.dim_locations_current` — **operational dimension SCD1** | rename/relocate applied, closed location deleted, stale edit ignored | ✅ PASS |

Cancellations are **real** `order_cancelled` lifecycle events from the test
generator's `--cancel-rate` (not synthetic). Idempotent across re-runs.

---

## 4. Storage / FileIO findings

- **S3A works on Spark 5.0** against SeaweedFS once you match Hadoop 3.5.0:
  `hadoop-aws-3.5.0` + AWS SDK `bundle-2.35.4` + `analyticsaccelerator-s3-1.3.1`
  (a new Hadoop-3.5 S3A dependency).
- **Prefer Hadoop s3a (`HadoopFileIO`) over Iceberg `S3FileIO` on SeaweedFS.**
  S3FileIO failed reading an aux-state snapshot avro (`Failed to open file …
  snap-….avro`); routing `s3://` through `S3AFileSystem` fixed it.
- **HMS + S3 warehouse:** Iceberg HiveCatalog namespace creation fails unless the
  **HMS server itself** can write to S3 (it `mkdir`s the namespace dir). The basic
  `apache/hive:4.0.0` container has no S3 FS → configure it with hadoop-aws+creds,
  or use a local warehouse. (Catalog mechanism itself is fine — §1 row 5.)

---

## 5. Requirements & edge-case behaviors (for users)

- **Target must implement DSv2 `SupportsRowLevelOperations`** (Iceberg ✅). Plain
  Parquet/managed tables → `AUTOCDC_TARGET_DOES_NOT_SUPPORT_MERGE`.
- **Target catalog must be durable** — AUTO CDC keeps a `__spark_autocdc_aux_state_<target>`
  table next to the target; an ephemeral/non-shared catalog loses it on restart.
- **`sequence_by` must be non-null and should be unique** (LSN/offset, not a coarse timestamp).
- **Source must be streaming**; a streaming file source needs a **glob** (`dir/*.json`),
  not a bare directory (SDP analysis → `PATH_NOT_FOUND`).
- **OSS scope:** SCD **Type 1** only; `apply_as_truncates` / ignore-null-update
  lists are declared but not yet honored (SPARK-57092 / SPARK-57093 TODOs).
- **Hidden metadata column:** AUTO CDC adds a `__spark_autocdc_metadata:
  struct<deleteSequence, upsertSequence>` column to the target (its per-row
  sequencing state). It shows up in `SELECT *` — project explicit columns for
  a clean current-state view.

---

## 6. Known issue (filed separately)

`spark-pipelines run` (the CLI) fails to register an AUTO CDC flow:
`InvalidPlanInput: [INTERNAL_ERROR] … RELTYPE_NOT_SET`. The `@dp.table` source and
`create_streaming_table` register fine; only the AUTO CDC `DefineFlow` fails. The
identical graph works when driven programmatically (`create_dataflow_graph` +
`start_run`) against a standalone Connect server — isolated to the CLI's embedded
server. See `BUG-spark-pipelines-autocdc-RELTYPE_NOT_SET.md` + `bug-repro-cli/`.
All testing here therefore uses the programmatic driver.

---

## 7. Not covered / unsupported

- **Delta — architecturally unsupported as an AUTO CDC target.** AUTO CDC needs
  `SupportsRowLevelOperations`; Delta's `DeltaTableV2` does **not** implement it
  (Delta does MERGE via its own `MergeIntoCommand`/analyzer rules, not the DSv2
  V2-connector contract). So no catalog choice or Spark-5.0 Delta build would
  help — AUTO CDC rejects Delta regardless. (Separately, delta-spark 4.0.1 doesn't
  even load on Spark 5.0: `NoSuchMethodError CatalogStorageFormat.copy`; Delta
  master's newest spec is 4.2.0-preview5, which predates AUTO CDC.)
- **UC OSS as a write target** — read-only Iceberg REST; Delta path moot per above.
- **Iceberg `_change_type` changelog as a *streaming* source** — batch-only in OSS
  (`SparkChangelogTable` / `create_changelog_view`); the Iceberg streaming source
  is append-only, so a true changelog-stream (deletes/updates as change rows)
  can't feed AUTO CDC directly. The append-stream-of-CDC-rows pattern works (§Sources).
- **`create_auto_cdc_from_snapshot_flow`** (snapshot-diff CDC) — not in OSS; only
  `create_auto_cdc_flow` (feed-based) is exported. (Databricks Lakeflow only.)
- **SCD Type 2** — not in OSS yet.
- **HMS + S3 warehouse end-to-end** — needs the HMS container configured with S3
  (validated the catalog with a local warehouse instead).
