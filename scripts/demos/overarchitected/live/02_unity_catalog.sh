#!/usr/bin/env bash
# Act 2: Unity Catalog
# Show the catalog REST API, explore tables, create new ones

set -e

echo "=== UC Health Check ==="
curl -s http://localhost:8081/api/2.1/unity-catalog/catalogs | python3 -m json.tool

echo ""
echo "=== List Schemas ==="
curl -s "http://localhost:8081/api/2.1/unity-catalog/schemas?catalog_name=unity" | python3 -m json.tool

echo ""
echo "=== List Tables in Bronze ==="
curl -s "http://localhost:8081/api/2.1/unity-catalog/tables?catalog_name=unity&schema_name=bronze" | python3 -m json.tool

echo ""
echo "=== Iceberg REST — Namespaces ==="
curl -s http://localhost:8081/api/2.1/unity-catalog/iceberg/v1/namespaces | python3 -m json.tool

echo ""
echo "=== Iceberg REST — Tables in Bronze ==="
curl -s http://localhost:8081/api/2.1/unity-catalog/iceberg/v1/namespaces/bronze/tables | python3 -m json.tool

echo ""
echo "=== Create a new schema via REST ==="
curl -s -X POST http://localhost:8081/api/2.1/unity-catalog/schemas \
    -H "Content-Type: application/json" \
    -d '{"name": "demo", "catalog_name": "unity", "comment": "Live demo schema"}' | python3 -m json.tool

echo ""
echo "=== Create a table via Spark SQL (through UC) ==="
docker exec spark-master-41 /opt/spark/bin/spark-sql -e "
    CREATE TABLE IF NOT EXISTS iceberg.demo.city_order_counts AS
    SELECT location_id, count(*) as order_count
    FROM iceberg.bronze.orders
    WHERE event_type = 'order_created'
    GROUP BY location_id
" 2>/dev/null

echo ""
echo "=== Verify new table exists in UC ==="
curl -s "http://localhost:8081/api/2.1/unity-catalog/tables?catalog_name=unity&schema_name=demo" | python3 -m json.tool

echo ""
echo "=== Query the new table ==="
docker exec spark-master-41 /opt/spark/bin/spark-sql \
    -e "SELECT * FROM iceberg.demo.city_order_counts ORDER BY order_count DESC" 2>/dev/null
