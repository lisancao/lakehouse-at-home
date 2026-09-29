#!/usr/bin/env bash
# Run one RTM experiment end to end with the quickstart harness:
#   1. submit the pipeline to the standalone cluster (rtm-cluster.sh start first)
#   2. wait until the query prints its "running" line
#   3. start producer.py (500 events/s, 50 keys unless overridden)
#   4. run latency_consumer.py on the output topic for DURATION seconds
#   5. stop producer and pipeline
#
#   scripts/rtm/run-experiment.sh 01_stateless_latency.py rtm-latency a
#   PRODUCER_ARGS="--rate 500 --keys 50 --dup-rate 0.05" CONSUMER_ARGS="--id-field event_id" \
#     scripts/rtm/run-experiment.sh 03_dedup.py rtm-dedup c
#
# Env: RTM_OUT (results dir, default /tmp/rtm43/runs), RTM_PYTHON, DURATION (70), WARMUP (10),
#      SETTLE (5, seconds between query start and producer start)
set -euo pipefail

HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
SCRIPT=$1 TOPIC=$2 LABEL=$3
: "${RTM_OUT:=/tmp/rtm43/runs}" "${RTM_PYTHON:=$(command -v python3)}"
: "${DURATION:=70}" "${WARMUP:=10}" "${PRODUCER_ARGS:=--rate 500 --keys 50}" "${CONSUMER_ARGS:=}"
mkdir -p "$RTM_OUT"
DRIVER_LOG="$RTM_OUT/$LABEL.driver.log"

if [[ "$SCRIPT" == *.scala ]]; then
  "$HERE/rtm-cluster.sh" shell -i "$HERE/$SCRIPT" < /dev/null > "$DRIVER_LOG" 2>&1 &
else
  "$HERE/rtm-cluster.sh" submit "$SCRIPT" > "$DRIVER_LOG" 2>&1 &
fi
DRIVER_PID=$!
cleanup() {
  [ -n "${PRODUCER_PID:-}" ] && kill "$PRODUCER_PID" 2>/dev/null || true
  # spark-shell does not exec into the JVM, so kill its children as well.
  pkill -P "$DRIVER_PID" 2>/dev/null || true
  kill "$DRIVER_PID" 2>/dev/null || true
  wait 2>/dev/null || true
}
trap cleanup EXIT

for _ in $(seq 1 120); do
  grep -q -E '\] .*running|RESULT:|Traceback' "$DRIVER_LOG" && break
  kill -0 "$DRIVER_PID" 2>/dev/null || break
  sleep 1
done
if ! grep -q -E '\] .*running' "$DRIVER_LOG"; then
  echo "pipeline did not start; tail of $DRIVER_LOG:" >&2
  tail -40 "$DRIVER_LOG" >&2
  exit 1
fi
echo "[$(date -Is)] $LABEL: query running"
# start() returns before the first batch resolves startingOffsets=latest; events
# produced in that gap are skipped. Give it a moment (lost the first 500 events
# of the first dedup run without this).
sleep "${SETTLE:-5}"

# shellcheck disable=SC2086
"$RTM_PYTHON" "$HERE/producer.py" $PRODUCER_ARGS > "$RTM_OUT/$LABEL.producer.log" 2>&1 &
PRODUCER_PID=$!
# shellcheck disable=SC2086
"$RTM_PYTHON" "$HERE/latency_consumer.py" --topic "$TOPIC" --duration "$DURATION" \
  --warmup "$WARMUP" --json-out "$RTM_OUT/$LABEL.json" $CONSUMER_ARGS \
  | tee "$RTM_OUT/$LABEL.consumer.log"
kill -INT "$PRODUCER_PID" 2>/dev/null || true
wait "$PRODUCER_PID" 2>/dev/null || true
tail -1 "$RTM_OUT/$LABEL.producer.log"
if grep -q -E 'StreamingQueryException|terminated with error' "$DRIVER_LOG"; then
  echo "WARNING: the query reported an error, see $DRIVER_LOG" >&2
fi
