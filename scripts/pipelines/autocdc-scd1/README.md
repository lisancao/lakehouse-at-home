# AUTO CDC → SCD Type 1 on Apache Spark master (→ Iceberg)

End-to-end proof that Apache Spark's new declarative-pipelines **AUTO CDC** API
(`create_auto_cdc_flow` with `stored_as_scd_type=1`) produces correct **Slowly
Changing Dimension Type 1** behavior, writing into an **Iceberg** table.

## TL;DR

```bash
# In Docker (canonical):
docker compose -f docker-compose-spark42.yml --profile demo run --rm autocdc-demo

# Locally against the source build:
SPARK_HOME=~/spark-src/dist \
ICEBERG_RUNTIME_JAR=~/iceberg-src/spark/v4.1/spark-runtime/build/libs/iceberg-spark-runtime-4.1_2.13-*.jar \
PYSPARK_PYTHON=~/spark-src/.venv-cdc/bin/python ./run.sh
# => PASS: AUTO CDC SCD Type 1 semantics confirmed
```

## Why this needs source builds (no RC exists)

As of 2026-05-29 there is **no Spark 4.2.0 RC** — the 4.2.0 line is in preview
(`4.2.0-preview5`; latest stable `4.1.2`). `create_auto_cdc_flow` landed on
`apache/spark` **master after preview5 was cut**, so no released artifact has it:

| Artifact | AUTO CDC SCD1? |
|----------|----------------|
| `4.2.0-preview5` image / `pyspark==4.2.0.dev5` | ❌ |
| `apache/spark` master source build (5.0.0-SNAPSHOT) | ✅ used here |

And AUTO CDC needs a **row-level MERGE** target → Iceberg/Delta. But Iceberg has
**no Spark 4.2/5.0 module** and its 4.1 runtime won't load on Spark 5.0 (removed
`connector.catalog.View`). So Iceberg is **ported from source** too — see
`iceberg-port/`.

## Build prerequisites

1. **Spark** (≈30–45 min):
   ```bash
   git clone --depth 1 https://github.com/apache/spark.git ~/spark-src && cd ~/spark-src
   export MAVEN_OPTS="-Xss128m -Xmx6g -XX:ReservedCodeCacheSize=1g"
   ./dev/make-distribution.sh --name lakehouse-cdc --pip --tgz --connect -Phive -Phive-thriftserver
   python3 -m venv .venv-cdc
   .venv-cdc/bin/pip install pyarrow "pandas<3.0.0" numpy grpcio grpcio-status \
     googleapis-common-protos "zstandard>=0.25.0" pyyaml
   ```
2. **Iceberg port** → see `iceberg-port/README.md` (publishes Spark to `~/.m2`,
   applies the patch, builds the runtime jar).
3. **Docker image** → see header of `docker-compose-spark42.yml`.

## The change feed (`generate_cdc_data.py`)

| id | events | SCD1 expectation |
|----|--------|------------------|
| 1 | INSERT NY (seq 1) → UPDATE Boston (seq 3) → stale UPDATE Chicago (seq 2) | **Boston** (highest seq wins; stale ignored) |
| 2 | INSERT LA (seq 1) → DELETE (seq 2) | **removed** |
| 3 | INSERT San Francisco (seq 1) | **San Francisco** |

Exercises upsert, in-place overwrite (Type 1 = no history), delete, and
out-of-order resolution via `sequence_by`.

## Files

| File | Purpose |
|------|---------|
| `generate_cdc_data.py` | Writes the crafted CDC feed to `/tmp/autocdc-scd1/cdc-events` |
| `autocdc_scd1_proof.py` | Registers the graph (source + streaming table + AUTO CDC flow), runs it, asserts the SCD1 end state. Target via `AUTOCDC_TARGET` env (default `ice.cdc.scd1_customers`) |
| `conf/spark-defaults.conf.template` | Static Connect-server config (Hive source + Iceberg target); `run.sh` materializes it |
| `run.sh` | Local: generate data → start standalone Connect server → run proof |
| `conf-docker/` + `run-in-docker.sh` | In-container variant (Iceberg jar baked into the image) |
| `realworld_e2e.py` | Holistic e2e on the real ghost-kitchen data: `gold.orders_live` (event stream → current state) + `silver.dim_locations_current` (SCD1 dimension), cross-checked against a full recompute. See `spark_content/autocdc_real_world_scenarios.md` |
| `iceberg-port/` | The Iceberg→Spark-5.0 source port (patch + instructions) |

## Hardening (stress + real-world e2e)

Run `realworld_e2e.py` against a standalone server (Iceberg conf) for the two
realistic scenarios above — both verified equal to an independent full-recompute.

An adversarial correctness battery (delete→reinsert, out-of-order deletes,
delete-before-insert across micro-batches, stale inserts, multi-update-per-batch,
500-key shuffled volume) **all pass**. Real-cancellation e2e: AUTO CDC output is
byte-for-byte equal to an independent full-recompute. Things to know:
- **Null `sequence_by`** fails the micro-batch loudly with
  `AUTOCDC_MICROBATCH_VALIDATION.NULL_SEQUENCE` (no silent corruption).
- **Exact ties on `sequence_by`** are non-deterministic — sequence on something
  unique (commit LSN / log offset), not a coarse timestamp. The real-world e2e
  uses `ts_seconds*100 + per_order_sequence` to guarantee a unique order.
- **Test hygiene (not an engine issue), found while hardening:** (1) sample with
  `orderBy(unique_col).limit`, never `orderBy(hash).limit` — the latter is
  non-deterministic and re-evaluates per DAG reference, unioning several samples
  into one feed; (2) re-runs must `DROP TABLE … PURGE` the Iceberg target, else
  stale data files accumulate and results stop being idempotent.

Real `order_cancelled` events (the delete vector) come from the generator's new
`--cancel-rate` flag — orders cancel mid-lifecycle and emit a terminal
`order_cancelled` event. Generate the demo dataset with:
`python -m scripts.testdata generate --days 1 --orders-per-day 800 --cancel-rate 0.06 --name orders_cdc_demo.parquet`

## Findings / gotchas (discovered empirically)

1. **Target must support row-level MERGE + restart-durability.** Plain
   Spark-managed parquet → `AUTOCDC_TARGET_DOES_NOT_SUPPORT_MERGE`. AUTO CDC
   writes via MERGE and keeps a durable aux state table
   (`__spark_autocdc_aux_state_<target>`). Iceberg satisfies this.
   - Pure-Spark alternative (no Iceberg): Spark's own
     `SharedTablesInMemoryRowLevelOperationTableCatalog` test connector. Run with
     `AUTOCDC_TARGET=cat.cdc.scd1_customers` and a catalog config registering
     `cat` + the catalyst/core `test-classes` on the classpath.
2. **The `spark-pipelines` CLI does not work for AUTO CDC on this build** — its
   embedded Connect server fails serializing the command (`RELTYPE_NOT_SET`).
   Driving registration + `start_run` against a *standalone* Connect server works
   (that's what `autocdc_scd1_proof.py` does).
3. **Streaming file source needs a glob** (`dir/*.json`); a bare directory →
   `[PATH_NOT_FOUND]` during SDP analysis.
4. **Static configs** (warehouse, metastore, Connect port, catalogs, classpath)
   must be set at server startup via `SPARK_CONF_DIR`, not a spec's runtime block.
5. **Mixed catalogs:** source streaming table lives in `spark_catalog` (Hive);
   only the AUTO CDC target is qualified into the MERGE-capable catalog.
6. **Snapshot caching:** the writing session caches the target at the pre-write
   snapshot — `REFRESH TABLE` (or a fresh session) before reading.

## Expected output

```
Flow spark_catalog.default.cdc_customers_source has COMPLETED.
Flow ice.cdc.scd1_customers has COMPLETED.
Run is COMPLETED.
+---+-----+-------------+
|id |name |city         |
|1  |Alice|Boston       |
|3  |Carol|San Francisco|
+---+-----+-------------+
PASS: AUTO CDC SCD Type 1 semantics confirmed
```
