#!/usr/bin/env bash
# OverArchitected E2E Test Runner
# ================================
# Runs each demo script with cleanup between sessions.
# Ensures predictable state for each act.
#
# Usage:
#   ./scripts/demos/overarchitected/run_e2e_test.sh          # Run all
#   ./scripts/demos/overarchitected/run_e2e_test.sh 01       # Run specific act
#   ./scripts/demos/overarchitected/run_e2e_test.sh 01 03    # Run multiple

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
LOG_DIR="/tmp/overarchitected-test-$(date +%Y%m%d-%H%M%S)"
mkdir -p "$LOG_DIR"

SPARK_CONTAINER="spark-master-41"
SPARK_SUBMIT="docker exec $SPARK_CONTAINER /opt/spark/bin/spark-submit"
KAFKA_PACKAGES="--packages org.apache.spark:spark-sql-kafka-0-10_2.13:4.1.0"

PASS=0
FAIL=0
SKIP=0

# Colors
GREEN='\033[0;32m'
RED='\033[0;31m'
YELLOW='\033[0;33m'
NC='\033[0m'

log() { echo -e "${GREEN}[TEST]${NC} $*"; }
err() { echo -e "${RED}[FAIL]${NC} $*"; }
warn() { echo -e "${YELLOW}[SKIP]${NC} $*"; }

# ─── Cleanup function ───────────────────────────────────────
cleanup_iceberg_namespaces() {
    log "Cleaning up test namespaces..."
    docker exec "$SPARK_CONTAINER" /opt/spark/bin/spark-sql \
        -e "DROP TABLE IF EXISTS iceberg.overarch.orders_variant;
            DROP TABLE IF EXISTS iceberg.overarch.order_events_exploded;
            DROP TABLE IF EXISTS iceberg.overarch.gold_hourly;
            DROP TABLE IF EXISTS iceberg.overarch.gold_brand;
            DROP TABLE IF EXISTS iceberg.demo_imperative.bronze_orders;
            DROP TABLE IF EXISTS iceberg.bronze.orders;
            DROP TABLE IF EXISTS iceberg.silver.orders_enriched;
            DROP TABLE IF EXISTS iceberg.gold.hourly_metrics;
            DROP TABLE IF EXISTS iceberg.gold.city_leaderboard;" \
        2>/dev/null || true
    log "Cleanup done."
}

# ─── Test runner ─────────────────────────────────────────────
run_test() {
    local name="$1"
    local cmd="$2"
    local logfile="$LOG_DIR/${name}.log"

    log "Running: $name"
    log "  Log: $logfile"

    if eval "$cmd" > "$logfile" 2>&1; then
        # Check for errors in output (some scripts catch exceptions internally)
        if grep -qi "ERROR\|Traceback\|Exception" "$logfile" 2>/dev/null; then
            # Filter out expected "errors" (like table-not-found during cleanup)
            local real_errors
            real_errors=$(grep -i "ERROR\|Traceback\|Exception" "$logfile" | \
                grep -iv "not found\|already exists\|not available\|skipped\|FALLBACK\|SKIP\|expected\|HTTP Error\|Bad Request\|AirflowException\|raise \|URLError\|reachable\|code example\|What .* looks like\|#.*Exception\|inline" | \
                head -5)
            if [ -n "$real_errors" ]; then
                err "$name — completed but with errors:"
                echo "$real_errors" | head -3
                FAIL=$((FAIL + 1))
                return 1
            fi
        fi
        log "$name — PASSED"
        PASS=$((PASS + 1))
        return 0
    else
        err "$name — FAILED (exit code $?)"
        tail -20 "$logfile"
        FAIL=$((FAIL + 1))
        return 1
    fi
}

# ─── Preflight ───────────────────────────────────────────────
log "OverArchitected E2E Test Suite"
log "=============================="
log "Log directory: $LOG_DIR"

# Check Spark is running
if ! docker exec "$SPARK_CONTAINER" echo "ok" >/dev/null 2>&1; then
    err "Spark container '$SPARK_CONTAINER' not running. Run: ./lakehouse start all"
    exit 1
fi
log "Spark 4.1 container: running"

# Check test data
if ! docker exec "$SPARK_CONTAINER" test -f /data/events/orders_90d.parquet 2>/dev/null; then
    err "Test data not found. Run: ./lakehouse testdata generate --days 90 && ./lakehouse testdata load"
    exit 1
fi
log "Test data: available"

# Determine which tests to run
TESTS_TO_RUN=("${@:-01 02 03 04a 04b 05a 05b}")

# ─── Run Tests ───────────────────────────────────────────────
for test_id in "${TESTS_TO_RUN[@]}"; do
    echo ""
    echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"

    case "$test_id" in
        01)
            run_test "act01_data_smuggled" \
                "$SPARK_SUBMIT /scripts/demos/overarchitected/01_data_smuggled.py" || true
            ;;
        02)
            # UC may or may not be running — script handles both cases
            run_test "act02_unity_catalog" \
                "$SPARK_SUBMIT /scripts/demos/overarchitected/02_unity_catalog_setup.py" || true
            ;;
        03)
            run_test "act03_spark_setup" \
                "$SPARK_SUBMIT /scripts/demos/overarchitected/03_spark_setup.py" || true
            ;;
        04a)
            cleanup_iceberg_namespaces
            run_test "act04a_sdp_showcase" \
                "$SPARK_SUBMIT /scripts/demos/overarchitected/04a_sdp_showcase.py" || true
            ;;
        04b)
            # RTM needs Kafka — check first
            if docker exec "$SPARK_CONTAINER" bash -c "echo > /dev/tcp/localhost/9092" 2>/dev/null; then
                run_test "act04b_rtm_streaming" \
                    "$SPARK_SUBMIT $KAFKA_PACKAGES /scripts/demos/overarchitected/04b_rtm_streaming.py" || true
            else
                warn "act04b_rtm_streaming — Kafka not reachable, skipping"
                SKIP=$((SKIP + 1))
            fi
            ;;
        05a)
            run_test "act05a_airflow_sdp" \
                "$SPARK_SUBMIT /scripts/demos/overarchitected/05a_airflow_sdp.py" || true
            ;;
        05b)
            run_test "act05b_spark_connect" \
                "$SPARK_SUBMIT /scripts/demos/overarchitected/05b_spark_connect.py" || true
            ;;
        05c)
            warn "act05c_spark_k8s — reference script, not executable test"
            SKIP=$((SKIP + 1))
            ;;
        06a|06b|06c)
            warn "act${test_id}_mlflow — requires API key, run manually"
            SKIP=$((SKIP + 1))
            ;;
        all)
            # Recursive call with all tests
            exec "$0" 01 02 03 04a 04b 05a 05b 05c 06a 06b 06c
            ;;
        *)
            warn "Unknown test: $test_id"
            SKIP=$((SKIP + 1))
            ;;
    esac
done

# ─── Summary ─────────────────────────────────────────────────
echo ""
echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
log "E2E Test Summary"
echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
echo -e "  ${GREEN}PASSED:${NC} $PASS"
echo -e "  ${RED}FAILED:${NC} $FAIL"
echo -e "  ${YELLOW}SKIPPED:${NC} $SKIP"
echo "  Logs:   $LOG_DIR/"
echo ""

if [ "$FAIL" -gt 0 ]; then
    err "Some tests failed. Check logs for details."
    exit 1
fi

log "All runnable tests passed!"
