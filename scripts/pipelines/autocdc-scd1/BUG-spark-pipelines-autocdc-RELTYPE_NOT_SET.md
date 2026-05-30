# Bug: `spark-pipelines run` fails to register an AUTO CDC flow — `RELTYPE_NOT_SET`

**Component:** Spark Declarative Pipelines / Spark Connect (AUTO CDC, `create_auto_cdc_flow`)
**Affected version:** Apache Spark `master` `5.0.0-SNAPSHOT`, commit `06ffa273` (built with `--connect -Phive`)
**Environment:** Linux, Java 17 (OpenJDK 17.0.19), Scala 2.13.18, Python 3.12; PySpark Connect client deps installed (`pyarrow`, `pandas<3`, `grpcio`, `grpcio-status`, `googleapis-common-protos`, `zstandard`, `pyyaml`).
**Severity:** High — the documented `spark-pipelines run` workflow does not work for AUTO CDC flows.

---

## Summary

Running a Spark Declarative Pipeline that contains an **AUTO CDC** flow via the
`spark-pipelines` CLI fails during graph registration with:

```
pyspark.errors.exceptions.connect.InvalidPlanInput:
  [INTERNAL_ERROR] This oneOf field in spark.connect.Relation is not set: RELTYPE_NOT_SET  SQLSTATE: XX000
```

The error is raised by `create_auto_cdc_flow(...)` while sending the
`PipelineCommand.DefineFlow` (with `auto_cdc_flow_details`) to the **embedded
Connect server** that the CLI spawns. The two preceding commands in the same
pipeline — the `@dp.table` streaming source and `create_streaming_table` — register
successfully, so the failure is isolated to the AUTO CDC `DefineFlow` command path.

**The same flow registers and runs correctly** when the graph is built
programmatically (`create_dataflow_graph` + `start_run`) against a *standalone*
Connect server (`sbin/start-connect-server.sh`). This points to the CLI /
embedded-server path rather than the API or the client serialization.

---

## Steps to reproduce

Requires a `master` build with Connect (`./dev/make-distribution.sh --pip --tgz --connect -Phive`)
and a Python env with the Connect client deps.

```bash
# 1. tiny CDC feed (a glob is required; a bare directory fails with PATH_NOT_FOUND)
mkdir -p /tmp/cli_repro/cdc
cat > /tmp/cli_repro/cdc/b01.json <<'JSON'
{"id":1,"name":"Alice","city":"NY","op":"UPSERT","seq":1}
{"id":1,"name":"Alice","city":"Boston","op":"UPSERT","seq":3}
JSON

# 2. the pipeline
cat > /tmp/cli_repro/pipeline.py <<'PY'
from typing import Any
from pyspark import pipelines as dp
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, LongType
spark: Any
SCHEMA = StructType([StructField("id",IntegerType()),StructField("name",StringType()),
                     StructField("city",StringType()),StructField("op",StringType()),
                     StructField("seq",LongType())])
@dp.table(name="cdc_src")
def cdc_src():
    return spark.readStream.schema(SCHEMA).json("file:///tmp/cli_repro/cdc/*.json")
dp.create_streaming_table("scd1")
dp.create_auto_cdc_flow(target="scd1", source="cdc_src", keys=["id"],
                        sequence_by="seq", stored_as_scd_type=1)
PY

# 3. the spec (default spark_catalog — no extra catalog config needed)
cat > /tmp/cli_repro/spark-pipeline.yml <<'YML'
name: autocdc_cli_repro
catalog: spark_catalog
database: default
storage: file:///tmp/cli_repro/storage
libraries:
  - glob:
      include: pipeline.py
YML

# 4. run via the CLI
export SPARK_HOME=/path/to/spark/dist
export PYSPARK_PYTHON=/path/to/venv/bin/python
cd /tmp/cli_repro
"$SPARK_HOME/bin/spark-pipelines" run --spec spark-pipeline.yml
```

(The same files are committed at `scripts/pipelines/autocdc-scd1/bug-repro-cli/`.)

## Expected result

The pipeline registers all three graph elements and runs, applying the CDC feed
to `scd1` as SCD Type 1 (final state: `id=1, city=Boston`).

## Actual result

Registration of the AUTO CDC flow fails:

```
2026-... : Registering graph elements...
2026-... : Importing /tmp/cli_repro/pipeline.py...
Traceback (most recent call last):
  ...
  File ".../pyspark/pipelines/api.py", line 677, in create_auto_cdc_flow
    get_active_graph_element_registry().register_auto_cdc_flow(flow)
  File ".../pyspark/pipelines/spark_connect_graph_element_registry.py", line 168, in register_auto_cdc_flow
    self._client.execute_command(command)
  File ".../pyspark/sql/connect/client/core.py", line 1218, in execute_command
  ...
pyspark.errors.exceptions.connect.InvalidPlanInput:
  [INTERNAL_ERROR] This oneOf field in spark.connect.Relation is not set: RELTYPE_NOT_SET  SQLSTATE: XX000
```

## Isolation / additional findings

- **Only the AUTO CDC command fails.** The `@dp.table` source (a `DefineFlow` with
  `relation_flow_details`) and `create_streaming_table` (a `DefineOutput`) register
  without error. The failure is specific to the `DefineFlow` carrying
  `auto_cdc_flow_details`.
- **Works against a standalone Connect server.** Building the identical graph
  programmatically and registering it against `sbin/start-connect-server.sh`
  succeeds, and `start_run` produces the correct SCD1 result. So the API,
  client-side serialization, and server-side AUTO CDC engine are all fine in
  isolation — the regression appears to be in the **`spark-pipelines` CLI /
  embedded `SparkPipelines` server** handling of the AUTO CDC `DefineFlow`.
- The error mentions an unset `spark.connect.Relation` oneOf (`RELTYPE_NOT_SET`),
  but `AutoCdcFlowDetails` carries only `Expression` fields (keys, sequence_by,
  etc.), not a `Relation` — suggesting an empty/default `Relation` is being read
  on the server side for this command in the embedded path.
- **Possibly related (secondary):** when the spec's default `catalog` is a custom
  v2 catalog rather than `spark_catalog`, the CLI's embedded server fails earlier
  with `Cannot initialize HadoopCatalog because warehousePath must not be null or
  empty`, even though the catalog's `warehouse` is set via `SPARK_CONF_DIR`. This
  may indicate the embedded server does not fully pick up catalog configuration —
  potentially the same root cause (embedded-server config/plan handling).

## Workaround

Drive the pipeline programmatically against a standalone Connect server instead of
the CLI:

```python
from pyspark.sql import SparkSession
from pyspark.pipelines.spark_connect_pipeline import create_dataflow_graph, start_run, handle_pipeline_events
from pyspark.pipelines.spark_connect_graph_element_registry import SparkConnectGraphElementRegistry
from pyspark.pipelines.graph_element_registry import graph_element_registration_context
from pyspark import pipelines as dp

spark = SparkSession.builder.remote("sc://localhost:15432").getOrCreate()
gid = create_dataflow_graph(spark, default_catalog="spark_catalog", default_database="default", sql_conf={})
reg = SparkConnectGraphElementRegistry(spark, gid)
with graph_element_registration_context(reg):
    ...  # @dp.table source + create_streaming_table + create_auto_cdc_flow
handle_pipeline_events(start_run(spark, gid, full_refresh=None, full_refresh_all=False,
                                 refresh=None, dry=False, storage="file:///tmp/.../storage"))
```

## Pointers for triage

- Client: `pyspark/pipelines/spark_connect_graph_element_registry.py::register_auto_cdc_flow`
  builds `PipelineCommand.DefineFlow` with `auto_cdc_flow_details`.
- Server: `org.apache.spark.sql.connect.pipelines.PipelinesHandler::defineFlow` →
  `buildAutoCdcFlow` (branch `DetailsCase.AUTO_CDC_FLOW_DETAILS`).
- Compare the embedded `org.apache.spark.deploy.SparkPipelines` server path vs a
  standard Connect server for how the `DefineFlow` oneOf is decoded.
