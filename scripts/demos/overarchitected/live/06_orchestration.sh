#!/usr/bin/env bash
# Act 6: Orchestration — Airflow + Spark Connect + K8s reference
# Airflow API, Connect thin client, deployment progression

set -e

echo "=== Airflow — Health Check ==="
curl -s -u admin:admin http://localhost:8085/api/v2/monitor/health | python3 -m json.tool

echo ""
echo "=== Airflow — List DAGs ==="
curl -s -u admin:admin http://localhost:8085/api/v2/dags \
    | python3 -c "
import sys, json
data = json.load(sys.stdin)
for dag in data.get('dags', []):
    status = 'active' if not dag.get('is_paused') else 'paused'
    print(f\"  {dag['dag_id']:40s} [{status}]  schedule: {dag.get('schedule_interval', 'none')}\")
" 2>/dev/null || echo "  (No DAGs found or Airflow not responding)"

echo ""
echo "=== Airflow — DAG details ==="
curl -s -u admin:admin http://localhost:8085/api/v2/dags/lakehouse_medallion_pipeline 2>/dev/null \
    | python3 -m json.tool 2>/dev/null || echo "  (DAG not found)"

echo ""
echo "=== Spark Connect — Start server ==="
docker exec spark-master-41 /opt/spark/sbin/start-connect-server.sh \
    --master spark://localhost:7078 2>&1 || true
echo "Waiting for Connect server..."
sleep 15

echo ""
echo "=== Spark Connect — Thin client query (no JVM, 1.5MB install) ==="
python3 -c "
from pyspark.sql import SparkSession

spark = SparkSession.builder.remote('sc://localhost:15002').getOrCreate()
print('Connected via Spark Connect')
print(f'Spark version: {spark.version}')

# Same query, thin client — no JVM on this machine
df = spark.sql('SELECT count(*) as total FROM iceberg.bronze.orders')
df.show()

spark.stop()
print('Done — same code, no JVM')
" 2>/dev/null

echo ""
echo "=== Deployment Progression ==="
echo ""
echo "  LAPTOP        STANDALONE       SPARK CONNECT      KUBERNETES"
echo "  local[*]      spark://...      sc://...           k8s://..."
echo "  One process   Master+Workers   gRPC thin client   Pods, auto-scale"
echo "  Dev/test      Team/staging     Multi-tenant       Production"
echo ""
echo "  Same pipeline code. Same SDP spec. Only the master URL changes."
echo ""
echo "  K8s deployment (reference — not running live):"
echo "    spark-submit --master k8s://https://api-server:6443 \\"
echo "      --conf spark.kubernetes.container.image=apache/spark:4.1.0 \\"
echo "      --conf spark.kubernetes.authenticate.driver.serviceAccountName=spark \\"
echo "      spark-pipelines run --spec pipeline.yml"
