# Stack Showcase Demos

Focused scripts that each exercise one part of the stack. Pick one based on
what you want to see; they do not need to run in order.

## Running

All scripts assume the stack is up:

```bash
./lakehouse start all
./lakehouse testdata generate --days 7
./lakehouse testdata load
```

Python demos run via `spark-submit` inside the Spark 4.1 container:

```bash
docker exec spark-master-41 /opt/spark/bin/spark-submit \
    /scripts/demos/showcase/<demo>.py
```

The shell demo runs from the host.

## What's here

| Demo | Shows | Needs |
|------|-------|-------|
| `otf_portability.py` | Reading Iceberg/Parquet from outside any managed platform | Iceberg catalog |
| `iceberg_variant.py` | Spark 4.1 VARIANT type over semi-structured Iceberg rows | Spark 4.1 + Iceberg |
| `unity_catalog_setup.py` | End-to-end UC OSS bootstrap and multi-client access | Unity Catalog |
| `spark_cluster_diagnostic.py` | Cluster introspection, config tour, common pitfalls | Spark 4.1 |
| `spark41_feature_tour.py` | VARIANT + recursive CTE + collation + SDP + Iceberg in one pass | Spark 4.1 |
| `streaming_udtf.py` | Python UDTFs on a streaming DataFrame | Kafka + Spark 4.1 |
| `sdp_showcase.py` | Imperative vs declarative pipeline side-by-side | Spark 4.1 |
| `sdp_demo.py` | Clean medallion pipeline driven by `sdp-pipeline.yml` | Spark 4.1 |
| `realtime_mode.py` | Structured Streaming Real-Time Mode vs micro-batch | Kafka + Spark 4.1 |
| `airflow_sdp.py` | Airflow DAG patterns for running SDP on a schedule | Airflow + Spark |
| `spark_connect.py` | Running jobs against a remote Spark driver via Spark Connect | Spark Connect |
| `spark_on_k8s.sh` | Submitting a Spark job to a Kubernetes cluster | `kubectl` + K8s |

## sdp-pipeline.yml

Config for `sdp_demo.py`. Run with:

```bash
docker exec spark-master-41 /opt/spark/bin/spark-pipelines run \
    --spec /scripts/demos/showcase/sdp-pipeline.yml
```
