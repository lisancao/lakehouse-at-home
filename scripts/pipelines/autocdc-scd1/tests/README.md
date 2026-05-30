# AUTO CDC SCD1 — test harnesses

Reproducible tests behind the results in [`../COVERAGE.md`](../COVERAGE.md). Each
script drives AUTO CDC over a **standalone Spark Connect server** (Spark 5.0
source build) and asserts outcomes.

| Script | What it covers |
|--------|----------------|
| `stress_scd1.py` | Adversarial SCD1 battery: delete→reinsert, out-of-order delete, stale-insert-after-delete, delete-before-insert, upsert/delete of non-existent keys, multi-update-per-batch, `sequence_by` tie (probe), 500-key shuffled volume. |
| `behavior_dimensions.py` | Composite keys, incremental re-run (checkpoint picks up new files), `full_refresh_all`, value-level schema evolution. |
| `kafka_source.py` | AUTO CDC with a **Kafka** streaming source (`readStream.format("kafka")`) instead of files. |

## Running

These need a standalone Connect server whose `SPARK_CONF_DIR` registers the
target catalog. **No secrets are committed** — credentials come from the stack's
own `config/spark/spark-defaults.conf`. See `COVERAGE.md` §4 for the catalog
config patterns. Sketch:

```bash
export SPARK_HOME=~/spark-src/dist            # Spark 5.0 source build (--connect)
export SPARK_CONF_DIR=/path/to/conf           # registers `ice` (Hadoop or other Iceberg catalog)
export PYTHONPATH="$SPARK_HOME/python:$SPARK_HOME/python/lib/py4j-0.10.9.9-src.zip"
"$SPARK_HOME/sbin/start-connect-server.sh"
python tests/stress_scd1.py        # or behavior_dimensions.py
"$SPARK_HOME/sbin/stop-connect-server.sh"
```

`stress_scd1.py` and `behavior_dimensions.py` use a Hadoop-Iceberg catalog `ice`
(no external services). `kafka_source.py` additionally needs the Kafka connector
jars on the server classpath and a populated `autocdc-cdc` topic (see COVERAGE).

> The `spark-pipelines` CLI can't run AUTO CDC on this build (see
> `../BUG-spark-pipelines-autocdc-RELTYPE_NOT_SET.md`), so these drive the graph
> programmatically (`create_dataflow_graph` + `start_run`).
