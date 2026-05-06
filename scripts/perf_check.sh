#!/bin/bash
# Performance regression test with hard baselines
# Usage: ./scripts/perf_check.sh <platform> <baseline_file>

set -e

PLATFORM=$1
BASELINE_FILE=$2
THRESHOLD=${PERF_THRESHOLD:-10} # Default 10% tolerance
DATA_DIR="benchmarks/data"
PERF_DIR=".perf"
mkdir -p "$PERF_DIR"

if [ -z "$PLATFORM" ] || [ -z "$BASELINE_FILE" ]; then
    echo "Usage: $0 <platform> <baseline_file>"
    exit 1
fi

if [ ! -f "$BASELINE_FILE" ]; then
    echo "Error: Baseline file $BASELINE_FILE not found"
    exit 1
fi

# Helper to run timed command and return time in milliseconds
run_timed() {
    local cmd="$@"
    local start=$(date +%s%N)
    eval "$cmd" > /dev/null 2>&1
    local end=$(date +%s%N)
    local ms=$(( (end - start) / 1000000 ))
    echo "$ms"
}

echo "═══════════════════════════════════════════════════════════════"
echo "MAR Performance Regression Check: $PLATFORM"
echo "Baseline: $BASELINE_FILE"
echo "Tolerance: $THRESHOLD%"
echo "═══════════════════════════════════════════════════════════════"

# Generate some test data if it doesn't exist
if [ ! -d "$DATA_DIR" ]; then
    mkdir -p "$DATA_DIR"
    echo "Generating synthetic test data..."
    dd if=/dev/urandom of="$DATA_DIR/test_large.bin" bs=1M count=50 2>/dev/null
fi

# Load baselines
declare -A BASELINES
while IFS='=' read -r key value; do
    if [[ ! $key =~ ^# ]] && [ -n "$key" ]; then
        BASELINES[$key]=$value
    fi
done < "$BASELINE_FILE"

REGRESSIONS=0

check_perf() {
    local op=$1
    local cmd=$2
    local baseline=${BASELINES[$op]}
    
    if [ -z "$baseline" ]; then
        echo "Skipping $op: No baseline found"
        return
    fi

    echo -n "Checking $op... "
    local current=$(run_timed "$cmd")
    
    local diff=$(( current - baseline ))
    local limit=$(( baseline * THRESHOLD / 100 ))
    
    if [ "$diff" -gt "$limit" ]; then
        local percent=$(( diff * 100 / baseline ))
        echo "⚠️  REGRESSED: ${current}ms (Baseline: ${baseline}ms, +${percent}%)"
        REGRESSIONS=$(( REGRESSIONS + 1 ))
    else
        echo "✓ OK: ${current}ms"
    fi
}

# Define operations to check
TEMP_ARCHIVE=".perf/test.mar"
TEMP_EXTRACT=".perf/extract"

check_perf "CREATE" "./mar create -f $TEMP_ARCHIVE $DATA_DIR"
check_perf "VALIDATE" "./mar validate $TEMP_ARCHIVE"
check_perf "EXTRACT" "mkdir -p $TEMP_EXTRACT && ./mar extract -f -o $TEMP_EXTRACT $TEMP_ARCHIVE"

rm -rf "$TEMP_ARCHIVE" "$TEMP_EXTRACT"

if [ "$REGRESSIONS" -gt 0 ]; then
    echo ""
    echo "Summary: $REGRESSIONS performance regression(s) detected."
    # We exit with 0 because the user requested this to be a warning, not an error
    exit 0
else
    echo ""
    echo "Summary: All performance checks passed."
    exit 0
fi
