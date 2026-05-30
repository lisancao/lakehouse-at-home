# Iceberg runtime ported to Spark 5.0-SNAPSHOT

AUTO CDC / SCD1 exists only on Apache Spark `master` (5.0.0-SNAPSHOT), but
Iceberg has no Spark 4.2/5.0 module (it stops at `spark/v4.1`) and the released
4.1 runtime **fails to load on Spark 5.0** — Spark removed the DSv2 `View`
connector interface (replaced by `ViewInfo` + `TableViewCatalog`).

`iceberg-spark5.0-port.patch` repoints Iceberg's `spark/v4.1` module to Spark
5.0 and adapts it to the API drift. It is a **tables-only** port: Iceberg's view
DDL support is removed (AUTO CDC needs only tables + row-level MERGE), which is
the bulk of the diff (+54 / −534).

## What the patch changes

- `gradle/libs.versions.toml`: `spark41` → `5.0.0-SNAPSHOT` (resolved from `mavenLocal`).
- `spark/v4.1/build.gradle`: drop the `spark-extensions` dependency from the runtime
  jar (the extensions module is Iceberg view-DDL SQL, all built on the removed `View` API).
- **View removal** (Spark 5.0 deleted `connector.catalog.View`): strip `ViewCatalog`
  from `BaseCatalog`; remove view methods from `SparkCatalog`/`SparkSessionCatalog`;
  delete `SparkView.java`, `SupportsReplaceView.java`.
- **New abstract methods**: `Reducer.resultType()` (BucketFunction, HoursFunction);
  `SpecializedGetters.getTimestampLTZNanos/NTZNanos` (StructInternalRow,
  SparkParquetReaders) — return `IntegerType` / throw for nanos.
- **Unrelated-defaults clash**: both `StagedTable` and `TruncatableTable` now declare
  a `default reportDriverMetrics()`, so `RollbackStagedTable`/`StagedSparkTable`
  override it explicitly.

## Reproduce

```bash
# 1. Publish the Spark 5.0 build to ~/.m2 so Iceberg's gradle can resolve it
cd ~/spark-src && ./build/mvn install -DskipTests -Phive -Phive-thriftserver

# 2. Clone Iceberg and apply the patch
git clone --depth 1 https://github.com/apache/iceberg.git ~/iceberg-src
cd ~/iceberg-src
git apply /path/to/lakehouse-stack/scripts/pipelines/autocdc-scd1/iceberg-port/iceberg-spark5.0-port.patch

# 3. Build the runtime jar (tables-only, no extensions)
./gradlew -DsparkVersions=4.1 -DscalaVersion=2.13 -DhiveVersions= -DflinkVersions= \
  :iceberg-spark:iceberg-spark-runtime-4.1_2.13:shadowJar -x test
# => spark/v4.1/spark-runtime/build/libs/iceberg-spark-runtime-4.1_2.13-*.jar
```

Patch generated against Iceberg `main` @ `8f28a86`.

## Caveats

- Tables-only: no Iceberg view DDL, no Iceberg SQL extensions (CALL procedures,
  `spark.sql.extensions` is intentionally unset). Table reads/writes and **native
  DSv2 row-level MERGE** (what AUTO CDC SCD1 uses) work.
- Throwaway: this exists only because Iceberg has no Spark 4.2/5.0 release yet.
  Replace with the official `iceberg-spark-runtime-4.2`/`5.0` once published.
- The artifact keeps the `4.1_2.13` name (module wiring reused); it is built
  against Spark 5.0.0-SNAPSHOT.
