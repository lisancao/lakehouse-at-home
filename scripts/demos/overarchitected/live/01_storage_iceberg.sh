#!/usr/bin/env bash
# Act 1: Storage + Iceberg
# Show where data lives and how Iceberg organizes it

set -e

echo "=== SeaweedFS Buckets (S3-compatible) ==="
aws --endpoint-url http://localhost:8333 s3 ls

echo ""
echo "=== Warehouse contents ==="
aws --endpoint-url http://localhost:8333 s3 ls s3://lakehouse/warehouse/ --recursive 2>/dev/null | head -20

echo ""
echo "=== Iceberg Namespaces ==="
docker exec spark-master-41 /opt/spark/bin/spark-sql \
    -e "SHOW NAMESPACES IN iceberg" 2>/dev/null

echo ""
echo "=== Bronze Tables ==="
docker exec spark-master-41 /opt/spark/bin/spark-sql \
    -e "SHOW TABLES IN iceberg.bronze" 2>/dev/null

echo ""
echo "=== Orders table — schema ==="
docker exec spark-master-41 /opt/spark/bin/spark-sql \
    -e "DESCRIBE TABLE iceberg.bronze.orders" 2>/dev/null

echo ""
echo "=== Orders table — row count ==="
docker exec spark-master-41 /opt/spark/bin/spark-sql \
    -e "SELECT count(*) as total_events FROM iceberg.bronze.orders" 2>/dev/null

echo ""
echo "=== Orders table — sample data ==="
docker exec spark-master-41 /opt/spark/bin/spark-sql \
    -e "SELECT event_id, event_type, event_timestamp, order_id FROM iceberg.bronze.orders LIMIT 5" 2>/dev/null

echo ""
echo "=== Iceberg snapshots (time travel) ==="
docker exec spark-master-41 /opt/spark/bin/spark-sql \
    -e "SELECT snapshot_id, committed_at, operation, summary FROM iceberg.bronze.orders.snapshots" 2>/dev/null

echo ""
echo "=== Iceberg data files ==="
docker exec spark-master-41 /opt/spark/bin/spark-sql \
    -e "SELECT file_path, record_count, file_size_in_bytes FROM iceberg.bronze.orders.files" 2>/dev/null

echo ""
echo "=== Iceberg manifests ==="
docker exec spark-master-41 /opt/spark/bin/spark-sql \
    -e "SELECT path, added_data_files_count, existing_data_files_count FROM iceberg.bronze.orders.manifests" 2>/dev/null
