#!/usr/bin/env bash
# Act 4: Spark Declarative Pipelines
# Show the config, the code, dry-run, then full run

set -e

echo "=== SDP Pipeline Config (8 lines) ==="
cat /home/lnc/lakehouse-stack/scripts/demos/overarchitected/sdp-pipeline.yml

echo ""
echo "=== SDP Pipeline Code ==="
cat /home/lnc/lakehouse-stack/scripts/demos/overarchitected/sdp_demo.py

echo ""
echo "=== Dry Run — show the dependency graph ==="
docker exec spark-master-41 /opt/spark/bin/spark-pipelines dry-run \
    --spec /scripts/demos/overarchitected/sdp-pipeline.yml 2>&1 | grep -v "^$" | grep -v "WARN\|INFO\|SLF4J\|UserWarning"

echo ""
echo "=== Full Run ==="
docker exec spark-master-41 /opt/spark/bin/spark-pipelines run \
    --spec /scripts/demos/overarchitected/sdp-pipeline.yml 2>&1 | grep -v "^$" | grep -v "WARN\|INFO\|SLF4J\|UserWarning\|Stage"

echo ""
echo "=== Verify — Silver table ==="
docker exec spark-master-41 /opt/spark/bin/spark-sql \
    -e "SELECT event_type, city, brand_id, order_total FROM iceberg.silver.orders_enriched LIMIT 10" 2>/dev/null

echo ""
echo "=== Verify — Gold hourly metrics ==="
docker exec spark-master-41 /opt/spark/bin/spark-sql \
    -e "SELECT * FROM iceberg.gold.hourly_metrics ORDER BY order_count DESC LIMIT 10" 2>/dev/null

echo ""
echo "=== Verify — Gold brand summary ==="
docker exec spark-master-41 /opt/spark/bin/spark-sql \
    -e "SELECT brand_name, total_orders, total_revenue, cities_served FROM iceberg.gold.brand_summary ORDER BY total_revenue DESC LIMIT 10" 2>/dev/null
