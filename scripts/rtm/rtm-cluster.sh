#!/usr/bin/env bash
# Local Spark standalone cluster (1 master + 1 worker) from a Spark 4.3 dist,
# for the Real-Time Mode experiments.
#
# Why not a container: there is no published 4.3 image, and the stack's
# compose files mount config/spark (which holds real credentials) into every
# session. This uses a private SPARK_CONF_DIR under $RTM_WORK instead.
#
#   SPARK_HOME=~/spark-4.3-src/dist scripts/rtm/rtm-cluster.sh start
#   scripts/rtm/rtm-cluster.sh submit 01_stateless_latency.py
#   scripts/rtm/rtm-cluster.sh shell -i 04_tws_scala.scala
#   scripts/rtm/rtm-cluster.sh stop
#
# Ports (checked with nc -z before use): master 7177, master UI 8180,
# worker UI 8181, driver UI from 4050. 15002 (Spark Connect) is left alone.
set -euo pipefail

HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
: "${SPARK_HOME:=$HOME/spark-4.3-src/dist}"
: "${SPARK_SRC:=$HOME/spark-4.3-src}"
: "${RTM_WORK:=/tmp/rtm43}"
# RTM holds one core per Kafka partition plus one per shuffle partition for the
# whole batch; 16 partitions + 16 shuffle partitions = 32. Over-provision a bit.
: "${RTM_WORKER_CORES:=40}"
: "${RTM_WORKER_MEMORY:=16g}"
# Python with pandas, pyarrow, protobuf and kafka-python-ng (driver and executors).
: "${RTM_PYTHON:=$(command -v python3)}"
MASTER_PORT=7177 MASTER_UI=8180 WORKER_UI=8181
MASTER_URL="spark://localhost:${MASTER_PORT}"

export SPARK_HOME
export SPARK_CONF_DIR="$RTM_WORK/conf"
export SPARK_LOG_DIR="$RTM_WORK/logs"
export SPARK_PID_DIR="$RTM_WORK/pids"
export SPARK_WORKER_DIR="$RTM_WORK/work"
export SPARK_LOCAL_IP=127.0.0.1

kafka_jars() {
  # The Kafka connector is not part of the dist's jars/; take it from the build
  # tree plus its runtime deps from the local Maven repo.
  local v; v=$(cd "$SPARK_HOME/jars" && ls spark-core_2.13-*.jar | sed 's/spark-core_2.13-\(.*\)\.jar/\1/')
  local m2="$HOME/.m2/repository"
  local kc; kc=$(grep -m1 '<kafka.version>' "$SPARK_SRC/pom.xml" | sed 's/.*>\(.*\)<.*/\1/')
  local pool; pool=$(cd "$SPARK_HOME/jars" && ls commons-pool2-*.jar 2>/dev/null | head -1 || true)
  local jars=(
    "$SPARK_SRC/connector/kafka-0-10-sql/target/spark-sql-kafka-0-10_2.13-$v.jar"
    "$SPARK_SRC/connector/kafka-0-10-token-provider/target/spark-token-provider-kafka-0-10_2.13-$v.jar"
    "$m2/org/apache/kafka/kafka-clients/$kc/kafka-clients-$kc.jar"
  )
  if [ -z "$pool" ]; then
    jars+=("$(ls "$m2"/org/apache/commons/commons-pool2/*/commons-pool2-*.jar | sort -V | tail -1)")
  fi
  local IFS=,; echo "${jars[*]}"
}

check_ports() {
  for p in "$MASTER_PORT" "$MASTER_UI" "$WORKER_UI"; do
    if nc -z localhost "$p" 2>/dev/null; then echo "port $p is taken, aborting" >&2; exit 1; fi
  done
}

case "${1:-}" in
  start)
    check_ports
    mkdir -p "$SPARK_CONF_DIR" "$SPARK_LOG_DIR" "$SPARK_PID_DIR" "$SPARK_WORKER_DIR"
    cat > "$SPARK_CONF_DIR/spark-defaults.conf" <<EOF
spark.master                      $MASTER_URL
spark.driver.memory               4g
spark.executor.memory             $RTM_WORKER_MEMORY
spark.ui.port                     4050
spark.pyspark.python              $RTM_PYTHON
spark.pyspark.driver.python       $RTM_PYTHON
spark.jars                        $(kafka_jars)
EOF
    "$SPARK_HOME/sbin/start-master.sh" --host localhost --port "$MASTER_PORT" --webui-port "$MASTER_UI"
    "$SPARK_HOME/sbin/start-worker.sh" "$MASTER_URL" --cores "$RTM_WORKER_CORES" \
      --memory "$RTM_WORKER_MEMORY" --webui-port "$WORKER_UI"
    ;;
  stop)
    "$SPARK_HOME/sbin/stop-worker.sh" || true
    "$SPARK_HOME/sbin/stop-master.sh" || true
    ;;
  submit)
    shift
    cd "$HERE"
    exec "$SPARK_HOME/bin/spark-submit" --py-files "$HERE/rtm_common.py" "$@"
    ;;
  shell)
    shift
    cd "$HERE"
    exec "$SPARK_HOME/bin/spark-shell" "$@"
    ;;
  jars) kafka_jars ;;
  *) echo "usage: $0 start|stop|submit <script> [args]|shell [args]|jars" >&2; exit 2 ;;
esac
