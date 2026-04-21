#!/usr/bin/env bash
# Act 5: Streaming + RTM
# Start producer, show Kafka data, run streaming ingest
#
# NOTE: Run the producer in a separate terminal first:
#   cd ~/lakehouse-stack && poetry run python scripts/tools/kafka-producer.py

set -e

echo "=== Kafka topic — check for order events ==="
docker exec kafka kafka-console-consumer \
    --bootstrap-server localhost:9092 \
    --topic orders \
    --from-beginning \
    --max-messages 3 \
    --timeout-ms 5000 2>/dev/null | while IFS= read -r line; do echo "$line" | python3 -m json.tool 2>/dev/null || echo "$line"; done

echo ""
echo "=== Streaming ingest — Kafka → Iceberg ==="
echo "(This runs for ~30 seconds to show data arriving, then stops)"
echo ""

timeout 30 docker exec spark-master-41 /opt/spark/bin/spark-submit \
    --jars /opt/spark/jars-extra/spark-sql-kafka-0-10_2.13-4.1.0.jar,/opt/spark/jars-extra/spark-token-provider-kafka-0-10_2.13-4.1.0.jar,/opt/spark/jars-extra/kafka-clients-3.6.1.jar \
    /scripts/demos/overarchitected/04b_rtm_streaming.py 2>&1 | grep -v "^$" | grep -v "INFO\|DEBUG" | tail -20 || true

echo ""
echo "=== RTM trigger — the one-line change ==="
echo ""
echo "  # Before: micro-batch (10s latency floor)"
echo "  .trigger(processingTime='10 seconds')"
echo ""
echo "  # After: Real-Time Mode (sub-second latency)"
echo "  .trigger(realTime='5 minutes')"
echo ""
echo "  Same API. Same DataFrame code. One line."
