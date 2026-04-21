#!/bin/bash

# Download required JARs for lakehouse-stack
# Supports --verify-only flag for CI validation

set -e  # Exit on error

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
JARS_DIR="${SCRIPT_DIR}/../../jars"
mkdir -p "${JARS_DIR}"
JARS_DIR="$(cd "${JARS_DIR}" && pwd)"

VERIFY_ONLY=false
MAX_RETRIES=3
RETRY_DELAY=5

# Colors
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m'

# Parse arguments
for arg in "$@"; do
    case $arg in
        --verify-only) VERIFY_ONLY=true ;;
    esac
done

# JAR definitions (simpler format for compatibility)
# Format: "filename|url|min_size_bytes"
JAR_LIST=(
    "iceberg-spark-runtime-4.0_2.13-1.10.0.jar|https://repo1.maven.org/maven2/org/apache/iceberg/iceberg-spark-runtime-4.0_2.13/1.10.0/iceberg-spark-runtime-4.0_2.13-1.10.0.jar|40000000"
    "hadoop-aws-3.4.1.jar|https://repo1.maven.org/maven2/org/apache/hadoop/hadoop-aws/3.4.1/hadoop-aws-3.4.1.jar|800000"
    "aws-java-sdk-bundle-1.12.780.jar|https://repo1.maven.org/maven2/com/amazonaws/aws-java-sdk-bundle/1.12.780/aws-java-sdk-bundle-1.12.780.jar|350000000"
    "bundle-2.24.6.jar|https://repo1.maven.org/maven2/software/amazon/awssdk/bundle/2.24.6/bundle-2.24.6.jar|400000000"
    "postgresql-42.7.4.jar|https://repo1.maven.org/maven2/org/postgresql/postgresql/42.7.4/postgresql-42.7.4.jar|1000000"
    "delta-spark_2.13-4.0.1.jar|https://repo1.maven.org/maven2/io/delta/delta-spark_2.13/4.0.1/delta-spark_2.13-4.0.1.jar|7000000"
    "delta-storage-4.0.1.jar|https://repo1.maven.org/maven2/io/delta/delta-storage/4.0.1/delta-storage-4.0.1.jar|70000"
)

# Get file size (cross-platform)
get_file_size() {
    local file=$1
    if [[ "$OSTYPE" == "darwin"* ]]; then
        stat -f%z "$file" 2>/dev/null || echo 0
    else
        stat -c%s "$file" 2>/dev/null || echo 0
    fi
}

# Verify file size
verify_size() {
    local file=$1
    local min_size=$2

    if [ ! -f "$file" ]; then
        return 1
    fi

    local actual_size=$(get_file_size "$file")
    if [ "$actual_size" -ge "$min_size" ]; then
        return 0
    else
        echo -e "   ${RED}File too small: ${actual_size} bytes (expected >= ${min_size})${NC}"
        return 1
    fi
}

# Download with retry
download_with_retry() {
    local url=$1
    local output=$2
    local attempt=1

    while [ $attempt -le $MAX_RETRIES ]; do
        echo -e "   Attempt $attempt/$MAX_RETRIES..."

        if command -v wget >/dev/null 2>&1; then
            if wget -q -O "$output.tmp" "$url" 2>&1; then
                mv "$output.tmp" "$output"
                return 0
            fi
        elif command -v curl >/dev/null 2>&1; then
            if curl -sSL -o "$output.tmp" "$url" 2>&1; then
                mv "$output.tmp" "$output"
                return 0
            fi
        else
            echo -e "   ${RED}Neither wget nor curl available${NC}"
            return 1
        fi

        echo -e "   ${YELLOW}Download failed, retrying in ${RETRY_DELAY}s...${NC}"
        rm -f "$output.tmp"
        sleep $((RETRY_DELAY * attempt))
        attempt=$((attempt + 1))
    done

    echo -e "   ${RED}Download failed after $MAX_RETRIES attempts${NC}"
    return 1
}

# Verify-only mode
if [ "$VERIFY_ONLY" = true ]; then
    echo "Verifying JARs in ${JARS_DIR}..."
    has_errors=0

    for jar_entry in "${JAR_LIST[@]}"; do
        IFS='|' read -r jar_name url min_size <<< "$jar_entry"
        jar_path="${JARS_DIR}/${jar_name}"

        if [ ! -f "$jar_path" ]; then
            echo -e "${RED}✗${NC} $jar_name (missing)"
            has_errors=1
        elif verify_size "$jar_path" "$min_size"; then
            echo -e "${GREEN}✓${NC} $jar_name (OK)"
        else
            echo -e "${RED}✗${NC} $jar_name (invalid size)"
            has_errors=1
        fi
    done

    exit $has_errors
fi

# Download mode
echo "Downloading JARs to ${JARS_DIR}..."
cd "${JARS_DIR}"

total=${#JAR_LIST[@]}
current=0
failed=0

for jar_entry in "${JAR_LIST[@]}"; do
    current=$((current + 1))
    IFS='|' read -r jar_name url min_size <<< "$jar_entry"

    echo -e "\n${YELLOW}[$current/$total]${NC} $jar_name"

    # Skip if already exists and valid
    if [ -f "$jar_name" ] && verify_size "$jar_name" "$min_size"; then
        echo -e "   ${GREEN}Already exists and valid${NC}"
        continue
    fi

    # Download
    echo -e "   Downloading from Maven Central..."
    if download_with_retry "$url" "$jar_name"; then
        if verify_size "$jar_name" "$min_size"; then
            echo -e "   ${GREEN}✓${NC} Download complete"
        else
            echo -e "   ${RED}✗${NC} Downloaded file appears corrupt"
            rm -f "$jar_name"
            failed=$((failed + 1))
        fi
    else
        failed=$((failed + 1))
    fi
done

echo ""
echo "================================"

if [ $failed -eq 0 ]; then
    echo -e "${GREEN}All JARs downloaded successfully!${NC}"
    echo ""
    echo "Total size:"
    du -sh "${JARS_DIR}"
    exit 0
else
    echo -e "${RED}$failed JAR(s) failed to download${NC}"
    echo "Run script again to retry failed downloads"
    exit 1
fi
