#!/usr/bin/env bash
# Act 3: Spark 4.1 Features
# VARIANT, Collation, Recursive CTEs — each in a few lines

set -e
SPARK_SQL="docker exec spark-master-41 /opt/spark/bin/spark-sql"

echo "=== Spark Version ==="
docker exec spark-master-41 /opt/spark/bin/spark-submit --version 2>&1 | grep "version"

echo ""
echo "=== Spark Config (catalog + S3) ==="
docker exec spark-master-41 cat /opt/spark/conf/spark-defaults.conf 2>/dev/null | grep -v "^#" | grep -v "^$"

echo ""
echo "=== VARIANT — Parse JSON body without a fixed schema ==="
$SPARK_SQL -e "
    SELECT
        order_id,
        parse_json(body) as body_variant,
        variant_get(parse_json(body), '\$.brand_id', 'int') as brand_id,
        variant_get(parse_json(body), '\$.total', 'double') as order_total
    FROM iceberg.bronze.orders
    WHERE event_type = 'order_created'
    LIMIT 5
" 2>/dev/null

echo ""
echo "=== VARIANT — Safe extraction (null on missing field) ==="
$SPARK_SQL -e "
    SELECT
        order_id,
        try_variant_get(parse_json(body), '\$.driver_id', 'string') as driver_id,
        try_variant_get(parse_json(body), '\$.nonexistent_field', 'string') as missing_field
    FROM iceberg.bronze.orders
    LIMIT 5
" 2>/dev/null

echo ""
echo "=== Collation — Case-insensitive without LOWER() ==="
$SPARK_SQL -e "
    SELECT name, id
    FROM iceberg.bronze.dim_brands
    WHERE name COLLATE utf8_lcase LIKE '%burger%'
" 2>/dev/null

echo ""
echo "=== Recursive CTE — Walk the order lifecycle ==="
$SPARK_SQL -e "
    WITH RECURSIVE event_chain AS (
        SELECT order_id, event_type, sequence, event_timestamp, 1 AS depth
        FROM iceberg.bronze.orders
        WHERE sequence = 0 AND order_id = (
            SELECT order_id FROM iceberg.bronze.orders
            WHERE event_type = 'order_created' LIMIT 1
        )
        UNION ALL
        SELECT e.order_id, e.event_type, e.sequence, e.event_timestamp, c.depth + 1
        FROM iceberg.bronze.orders e
        JOIN event_chain c ON e.order_id = c.order_id AND e.sequence = c.depth
    )
    SELECT order_id, event_type, sequence, event_timestamp
    FROM event_chain
    ORDER BY sequence
" 2>/dev/null
