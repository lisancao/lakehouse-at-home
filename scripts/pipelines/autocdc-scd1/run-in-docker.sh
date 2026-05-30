#!/usr/bin/env bash
# Runs INSIDE the lakehouse/spark:5.0.0-snapshot-cdc container (Iceberg jar baked in).
set -euo pipefail
HERE="$(cd "$(dirname "$0")" && pwd)"
export SPARK_HOME=/opt/spark
export SPARK_CONF_DIR="$HERE/conf-docker"
export PYTHONPATH="$SPARK_HOME/python:$SPARK_HOME/python/lib/py4j-0.10.9.9-src.zip"
rm -rf /tmp/autocdc-scd1/storage /tmp/autocdc-scd1/warehouse /tmp/autocdc-scd1/metastore_db /tmp/autocdc-scd1/iceberg-wh
python3 "$HERE/generate_cdc_data.py"
"$SPARK_HOME/sbin/start-connect-server.sh" >/dev/null 2>&1
sleep 18
python3 "$HERE/autocdc_scd1_proof.py"
