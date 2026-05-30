#!/usr/bin/env bash
# Prove AUTO CDC -> SCD Type 1 into an Iceberg table, against a source build of
# Apache Spark master (5.0.0-SNAPSHOT).
#
#   SPARK_HOME=~/spark-src/dist \
#   ICEBERG_RUNTIME_JAR=~/iceberg-src/spark/v4.1/spark-runtime/build/libs/iceberg-spark-runtime-4.1_2.13-*.jar \
#   PYSPARK_PYTHON=~/spark-src/.venv-cdc/bin/python ./run.sh
#
# - SPARK_HOME          : Spark dist built with --connect
# - ICEBERG_RUNTIME_JAR : the Iceberg runtime jar ported to Spark 5.0 (see iceberg-port/)
# - PYSPARK_PYTHON      : venv with pyarrow pandas<3 grpcio grpcio-status
#                         googleapis-common-protos zstandard pyyaml
set -euo pipefail

HERE="$(cd "$(dirname "$0")" && pwd)"
: "${SPARK_HOME:?set SPARK_HOME to the source-built Spark dist}"
: "${ICEBERG_RUNTIME_JAR:?set ICEBERG_RUNTIME_JAR to the Spark-5.0-ported Iceberg runtime jar}"
: "${PYSPARK_PYTHON:=python3}"
export SPARK_HOME PYSPARK_PYTHON

RUN_CONF="/tmp/autocdc-scd1/conf-run"
mkdir -p "$RUN_CONF"
sed "s#__ICEBERG_JAR__#${ICEBERG_RUNTIME_JAR}#g" "$HERE/conf/spark-defaults.conf.template" \
  > "$RUN_CONF/spark-defaults.conf"
export SPARK_CONF_DIR="$RUN_CONF"
export PYTHONPATH="$SPARK_HOME/python:$SPARK_HOME/python/lib/py4j-0.10.9.9-src.zip"

echo "==> Cleaning previous run state"
rm -rf /tmp/autocdc-scd1/storage /tmp/autocdc-scd1/warehouse \
       /tmp/autocdc-scd1/metastore_db /tmp/autocdc-scd1/iceberg-wh

echo "==> Generating CDC change feed"
"$PYSPARK_PYTHON" "$HERE/generate_cdc_data.py"

echo "==> Starting standalone Spark Connect server (port 15432)"
"$SPARK_HOME/sbin/stop-connect-server.sh" >/dev/null 2>&1 || true
sleep 2
"$SPARK_HOME/sbin/start-connect-server.sh" >/dev/null 2>&1
trap '"$SPARK_HOME/sbin/stop-connect-server.sh" >/dev/null 2>&1 || true' EXIT
sleep 20

echo "==> Running AUTO CDC -> SCD1 proof (target: ${AUTOCDC_TARGET:-ice.cdc.scd1_customers})"
set +e
"$PYSPARK_PYTHON" "$HERE/autocdc_scd1_proof.py"
rc=$?
set -e
echo "==> proof exit code: $rc"
exit $rc
