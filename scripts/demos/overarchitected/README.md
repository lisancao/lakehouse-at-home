# OverArchitected Show Demos

Live demo scripts for the Databricks "OverArchitected" show. Each script demonstrates increasingly complex Spark 4.1 features with the lakehouse-at-home food delivery domain.

## Prerequisites

```bash
./lakehouse start all
./lakehouse testdata generate --days 7
./lakehouse testdata load
```

## Demo Scripts

| Script | Features | Run Command |
|--------|----------|-------------|
| **00b_realtime_mode** | Real-Time Mode (RTM), micro-batch vs RTM, Kafka, Foreach | `docker exec spark-master-41 /opt/spark/bin/spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.13:4.1.0 /scripts/demos/overarchitected/00b_realtime_mode.py` |
| **01_variant_iceberg** | VARIANT type, parse_json, variant_get, Iceberg | `docker exec spark-master-41 /opt/spark/bin/spark-submit /scripts/demos/overarchitected/01_variant_iceberg.py` |
| **02_streaming_udtf** | Python UDTF, order lifecycle explosion | `docker exec spark-master-41 /opt/spark/bin/spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.13:4.1.0 /scripts/demos/overarchitected/02_streaming_udtf.py` |
| **03_full_overarchitected** | VARIANT + Recursive CTE + Collation + Gold tables | `docker exec spark-master-41 /opt/spark/bin/spark-submit /scripts/demos/overarchitected/03_full_overarchitected.py` |

## Run All

```bash
for script in 01_variant_iceberg 02_streaming_udtf 03_full_overarchitected; do
  echo "=== Running $script ==="
  docker exec spark-master-41 /opt/spark/bin/spark-submit \
    /scripts/demos/overarchitected/${script}.py 2>&1 | tail -40
done
```

For demo 02, add `--packages org.apache.spark:spark-sql-kafka-0-10_2.13:4.1.0` if using Kafka.

## Notes

- **Demo 01** uses `parse_json` (Spark 4.1) with fallback to `from_json` if VARIANT is unavailable.
- **Demo 02** uses batch data as source; for true streaming, run the Kafka producer and switch to `readStream.format("kafka")`.
- **Demo 03** requires parquet data; run `./lakehouse testdata load` first.
