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

## Root cause analysis

The failing command is the AUTO CDC `DefineFlow`. The server-side stack trace
(captured by flipping `serverStacktrace.enabled` back on in `cli.py`) is:

```
org.apache.spark.sql.connect.common.InvalidPlanInput$.apply(InvalidPlanInput.scala:46)
org.apache.spark.sql.connect.planner.InvalidInputErrors$.invalidOneOfField(InvalidInputErrors.scala:130)
org.apache.spark.sql.connect.planner.SparkConnectPlanner.$anonfun$transformRelation$1(SparkConnectPlanner.scala:237)
org.apache.spark.sql.connect.service.SessionHolder.usePlanCache(SessionHolder.scala:588)
org.apache.spark.sql.connect.planner.SparkConnectPlanner.transformRelation(SparkConnectPlanner.scala:146/132)
org.apache.spark.sql.connect.pipelines.PipelinesHandler$.defineFlow(PipelinesHandler.scala:375)   ← here
org.apache.spark.sql.connect.pipelines.PipelinesHandler$.handlePipelinesCommand(PipelinesHandler.scala:102)
```

The decisive observation: `defineFlow` only calls `transformRelation` in **one**
branch — `case DetailsCase.RELATION_FLOW_DETAILS =>` (`transformRelationFunc(relationFlowDetails.getRelation)`).
The `AUTO_CDC_FLOW_DETAILS` branch (`buildAutoCdcFlow`) **never** calls
`transformRelation` (verified by source inspection — it builds an
`UnresolvedRelation` from the source *name* and transforms only the
`keys`/`sequence_by`/`apply_as_deletes` **expressions**). So the stack proves the
server entered the **`RELATION_FLOW_DETAILS` branch** for this command and
transformed its `relation` sub-field, which is a default/empty `Relation`
(`RELTYPE_NOT_SET`).

But the client provably sent `auto_cdc_flow_details`, not `relation_flow_details`:

1. **Not stale jars.** `SPARK_PRINT_LAUNCH_COMMAND=1 spark-pipelines run` shows
   *both* JVM launches (the `SparkPipelines` launcher and the embedded
   `SparkSubmit … pyspark-shell` Connect server) use `-cp <dist>/jars/*` from the
   same `06ffa273` build. No old jars on either classpath.
2. **Not a client dispatch bug.** Runtime instrumentation of
   `SparkConnectGraphElementRegistry` during the *actual CLI run* shows the source
   goes through `register_flow` (`type=Flow`) and the AUTO CDC flow goes through
   **`register_auto_cdc_flow` (`type=AutoCdcFlow`)** — i.e. the client builds
   `DefineFlow.auto_cdc_flow_details`, the correct field.
3. **Not a proto field-number skew.** Both the client `pipelines_pb2` and the
   server `pipelines.proto` agree on the `details` oneof: `relation_flow_details = 7`,
   `auto_cdc_flow_details = 10`. The client `.py` and the unzipped/`pyspark.zip`
   copies are byte-identical for `api.py`, the registry, and `pipelines_pb2.py`.

**Conclusion:** the server's `flow.getDetailsCase` returns `RELATION_FLOW_DETAILS`
(field 7) for a `DefineFlow` the client serialized as `auto_cdc_flow_details`
(field 10) — the `details` **oneof discriminator is mis-read**, so the (absent)
`relation` is transformed and throws `RELTYPE_NOT_SET`. This reproduces **only**
through the `spark-pipelines` CLI's embedded server, never against a standalone
`start-connect-server.sh` with the *same* dist jars and protos. The only config
difference in the embedded server is artifact isolation
(`spark.sql.artifact.isolation.enabled=true` +
`spark.sql.artifact.isolation.alwaysApplyClassloader=true`, set automatically by
the embedded `pyspark-shell` driver) combined with the plan cache (`usePlanCache`
is in the failing stack). Enabling those confs *alone* on a standalone server did
**not** reproduce it, so the trigger is the embedded path's specific
classloader/plan-cache interaction during oneof decoding — not the wire bytes.

### Why a coworker may not reproduce it
The bug needs the **exact** `spark-pipelines run` embedded-server path. Any
hand-built standalone Connect server (even one started with the artifact-isolation
confs, or hosting the Connect plugin in a `pyspark-shell` driver) does **not**
reproduce it. Reproduce by running the committed `bug-repro-cli/` verbatim via
`spark-pipelines run --spec spark-pipeline.yml`, not a programmatic driver.

### Secondary (possibly same root cause)
When the spec's default `catalog` is a custom v2 catalog rather than
`spark_catalog`, the CLI's embedded server fails earlier with `Cannot initialize
HadoopCatalog because warehousePath must not be null or empty`, even though the
catalog's `warehouse` is set via `SPARK_CONF_DIR` — consistent with the embedded
server not fully honoring config in the same path.

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
  builds `PipelineCommand.DefineFlow` with `auto_cdc_flow_details` (field 10) —
  confirmed at runtime under the CLI.
- Server: `org.apache.spark.sql.connect.pipelines.PipelinesHandler::defineFlow`
  (`PipelinesHandler.scala:382` `flow.getDetailsCase match`). The crash is at the
  `RELATION_FLOW_DETAILS` branch (`transformRelationFunc(relationFlowDetails.getRelation)`),
  so `getDetailsCase` is returning the wrong oneof case for this command.
- **Next instrumentation (server-side):** in the *running embedded server*, log
  `flow.getDetailsCase` and the raw `DefineFlow` bytes at the top of `defineFlow`,
  and confirm whether the oneof is decoded as `RELATION_FLOW_DETAILS` vs
  `AUTO_CDC_FLOW_DETAILS`. The mis-decode appears only under the embedded server's
  artifact-isolation classloader + plan cache (`usePlanCache` in the stack), so the
  suspect is how the protobuf-generated `DefineFlow`/oneof classes resolve under the
  isolated classloader, or a poisoned `usePlanCache` entry — not the wire bytes,
  which are identical to the standalone (passing) path.
- Reproduce diagnostics with `serverStacktrace.enabled=true` (the CLI hard-codes it
  to `false` in `cli.py` ~line 322; flip it to capture the server stack above).
