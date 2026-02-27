#!/bin/bash
# Performance smoke test - detects regressions in create/extract/list/validate operations
# Generates .perf/previous_run.txt with baseline for future regression detection

set -e

# Configuration
PERF_DIR="${PERF_DIR:-.perf}"
PREV_RUN="previous_run.txt"
THRESHOLD=${PERF_THRESHOLD:-5}  # % threshold for regression warning
THREADS="${THREADS:-}"
THREAD_ARG=""
if [ -n "$THREADS" ]; then
    THREAD_ARG="--threads $THREADS"
fi
TEMP_ARCHIVE=$(mktemp)
TEMP_EXTRACT=$(mktemp -d)

# Ensure build directory exists
mkdir -p "$PERF_DIR"

# Cleanup on exit
cleanup() {
    rm -f "$TEMP_ARCHIVE"
    rm -rf "$TEMP_EXTRACT"
}
trap cleanup EXIT

# Helper to run timed command and return time in milliseconds
run_timed() {
    local cmd="$@"
    local start=$(date +%s%N)
    eval "$cmd" > /dev/null 2>&1
    local end=$(date +%s%N)
    local ms=$(( (end - start) / 1000000 ))
    echo "$ms"
}

# Helper to calculate percentage difference
calc_diff_percent() {
    local prev=$1
    local curr=$2
    if [ "$prev" -eq 0 ]; then
        echo 0
    else
        echo $(( (curr - prev) * 100 / prev ))
    fi
}

# Helper to format time
format_time() {
    local ms=$1
    if [ "$ms" -ge 1000 ]; then
        echo "$((ms / 1000)).$((ms % 1000 / 100))s"
    else
        echo "${ms}ms"
    fi
}

# Helper to format diff with color
format_diff() {
    local diff=$1
    local abs_diff=${diff#-}
    
    if [ "$abs_diff" -gt "$THRESHOLD" ]; then
        if [ "$diff" -gt 0 ]; then
            echo "⚠ +${diff}% (SLOWER)"
        else
            echo "✓ ${diff}% (faster)"
        fi
    else
        echo "  ${diff}%"
    fi
}

# Header
echo "═══════════════════════════════════════════════════════════════"
echo "MAR Performance Smoke Test"
echo "═══════════════════════════════════════════════════════════════"
echo ""
echo "Testing on available data..."
echo ""

# Find test data
DATA_DIR="${DATA_DIR:-benchmarks/data}"
if [ ! -d "$DATA_DIR" ]; then
    echo "Error: $DATA_DIR not found"
    echo "You can set DATA_DIR to point to your benchmark data:"
    echo "  DATA_DIR=/path/to/data ./scripts/perf_smoke_test.sh"
    exit 1
fi

# Select test datasets (prefer larger ones for reliable timing)
TEST_SETS=()
if [ -d "$DATA_DIR/linux-6.18.5" ]; then
    TEST_SETS+=("linux-6.18.5:Linux kernel")
fi
if [ -d "$DATA_DIR/numpy-2.4.1" ]; then
    TEST_SETS+=("numpy-2.4.1:NumPy")
fi
if [ -f "$DATA_DIR/dickens" ]; then
    TEST_SETS+=("dickens:Dickens")
fi

if [ ${#TEST_SETS[@]} -eq 0 ]; then
    echo "Error: No test data found in $DATA_DIR"
    exit 1
fi

# Load previous results if available
declare -A PREV_TIMES
LAST_DATASET=""
if [ -f "$PERF_DIR/$PREV_RUN" ]; then
    # Parse previous run results - extract dataset name and operation times
    # Format in file is: "## Dataset: Linux kernel (linux-6.18.5)" then "CREATE:3004" etc
    while IFS= read -r line; do
        # Check for dataset header
        if [[ $line =~ "## Dataset: ".*"("(.+)")" ]]; then
            LAST_DATASET="${BASH_REMATCH[1]}"
        # Check for operation:time lines
        elif [[ $line =~ ^(CREATE|LIST|HASH|VALIDATE|EXTRACT):([0-9]+)$ ]]; then
            op="${BASH_REMATCH[1]}"
            time="${BASH_REMATCH[2]}"
            if [ -n "$LAST_DATASET" ]; then
                PREV_TIMES["${LAST_DATASET}_${op}"]=$time
            fi
        fi
    done < "$PERF_DIR/$PREV_RUN"
    echo "Comparing against previous run..."
    echo ""
fi

# Initialize report
REPORT="$PERF_DIR/$PREV_RUN"
report_header() {
    {
        echo "# MAR Performance Baseline"
        echo "# Generated: $(date -u +%Y-%m-%dT%H:%M:%SZ)"
        echo "# Threshold: ${THRESHOLD}%"
        echo ""
    } > "$REPORT"
}

report_header

# Run tests
total_tests=0
regression_count=0
improvement_count=0

# Store current run times for summary section
declare -A CURR_TIMES

for dataset_spec in "${TEST_SETS[@]}"; do
    IFS=':' read -r dataset_name dataset_label <<< "$dataset_spec"
    
    if [ ! -e "$DATA_DIR/$dataset_name" ]; then
        continue
    fi
    
    echo "Testing: $dataset_label"
    echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
    
    {
        echo "## Dataset: $dataset_label ($dataset_name)"
        echo ""
    } >> "$REPORT"
    
    # Test 1: CREATE
    echo -n "  create... "
    create_time=$(run_timed "./mar create $THREAD_ARG -f '$TEMP_ARCHIVE' '$DATA_DIR/$dataset_name'")
    archive_size=$(stat -f%z "$TEMP_ARCHIVE" 2>/dev/null || stat -c%s "$TEMP_ARCHIVE" 2>/dev/null || echo "?")
    echo "$(format_time $create_time)"
    
    # Check for regression - use dataset_name for consistent key matching
    create_key="${dataset_name}_CREATE"
    CURR_TIMES[$create_key]=$create_time
    if [ -n "${PREV_TIMES[$create_key]}" ]; then
        diff=$(calc_diff_percent ${PREV_TIMES[$create_key]} $create_time)
        echo "             $(format_diff $diff)"
        if [ "$diff" -gt "$THRESHOLD" ]; then
            regression_count=$((regression_count + 1))
        elif [ "$diff" -lt "-$THRESHOLD" ]; then
            improvement_count=$((improvement_count + 1))
        fi
    fi
    
    {
        echo "### CREATE"
        echo "- Time: $(format_time $create_time) (${create_time}ms)"
        echo "- Archive size: $archive_size bytes"
        echo "CREATE:$create_time"
        echo ""
    } >> "$REPORT"
    total_tests=$((total_tests + 1))
    
    # Test 2: LIST
    echo -n "  list... "
    list_time=$(run_timed "./mar list '$TEMP_ARCHIVE'")
    echo "$(format_time $list_time)"
    
    # Check for regression - use dataset_name for consistent key matching
    list_key="${dataset_name}_LIST"
    CURR_TIMES[$list_key]=$list_time
    if [ -n "${PREV_TIMES[$list_key]}" ]; then
        diff=$(calc_diff_percent ${PREV_TIMES[$list_key]} $list_time)
        echo "             $(format_diff $diff)"
        if [ "$diff" -gt "$THRESHOLD" ]; then
            regression_count=$((regression_count + 1))
        elif [ "$diff" -lt "-$THRESHOLD" ]; then
            improvement_count=$((improvement_count + 1))
        fi
    fi
    
    {
        echo "### LIST"
        echo "- Time: $(format_time $list_time) (${list_time}ms)"
        echo "LIST:$list_time"
        echo ""
    } >> "$REPORT"
    total_tests=$((total_tests + 1))
    
    # Test 3: HASH
    echo -n "  hash... "
    hash_time=$(run_timed "./mar hash '$TEMP_ARCHIVE'")
    echo "$(format_time $hash_time)"

    hash_key="${dataset_name}_HASH"
    CURR_TIMES[$hash_key]=$hash_time
    if [ -n "${PREV_TIMES[$hash_key]}" ]; then
        diff=$(calc_diff_percent ${PREV_TIMES[$hash_key]} $hash_time)
        echo "             $(format_diff $diff)"
        if [ "$diff" -gt "$THRESHOLD" ]; then
            regression_count=$((regression_count + 1))
        elif [ "$diff" -lt "-$THRESHOLD" ]; then
            improvement_count=$((improvement_count + 1))
        fi
    fi

    {
        echo "### HASH"
        echo "- Time: $(format_time $hash_time) (${hash_time}ms)"
        echo "HASH:$hash_time"
        echo ""
    } >> "$REPORT"
    total_tests=$((total_tests + 1))

    # Test 4: VALIDATE
    echo -n "  validate... "
    validate_time=$(run_timed "./mar validate $THREAD_ARG '$TEMP_ARCHIVE'")
    echo "$(format_time $validate_time)"
    
    # Check for regression - use dataset_name for consistent key matching
    validate_key="${dataset_name}_VALIDATE"
    CURR_TIMES[$validate_key]=$validate_time
    if [ -n "${PREV_TIMES[$validate_key]}" ]; then
        diff=$(calc_diff_percent ${PREV_TIMES[$validate_key]} $validate_time)
        echo "             $(format_diff $diff)"
        if [ "$diff" -gt "$THRESHOLD" ]; then
            regression_count=$((regression_count + 1))
        elif [ "$diff" -lt "-$THRESHOLD" ]; then
            improvement_count=$((improvement_count + 1))
        fi
    fi
    
    {
        echo "### VALIDATE"
        echo "- Time: $(format_time $validate_time) (${validate_time}ms)"
        echo "VALIDATE:$validate_time"
        echo ""
    } >> "$REPORT"
    total_tests=$((total_tests + 1))
    
    # Test 5: EXTRACT
    echo -n "  extract... "
    rm -rf "$TEMP_EXTRACT"
    mkdir -p "$TEMP_EXTRACT"
    extract_time=$(run_timed "./mar extract $THREAD_ARG -o '$TEMP_EXTRACT' '$TEMP_ARCHIVE'")
    echo "$(format_time $extract_time)"
    
    # Check for regression - use dataset_name for consistent key matching
    extract_key="${dataset_name}_EXTRACT"
    CURR_TIMES[$extract_key]=$extract_time
    if [ -n "${PREV_TIMES[$extract_key]}" ]; then
        diff=$(calc_diff_percent ${PREV_TIMES[$extract_key]} $extract_time)
        echo "             $(format_diff $diff)"
        if [ "$diff" -gt "$THRESHOLD" ]; then
            regression_count=$((regression_count + 1))
        elif [ "$diff" -lt "-$THRESHOLD" ]; then
            improvement_count=$((improvement_count + 1))
        fi
    fi
    
    {
        echo "### EXTRACT"
        echo "- Time: $(format_time $extract_time) (${extract_time}ms)"
        echo "EXTRACT:$extract_time"
        echo ""
    } >> "$REPORT"
    total_tests=$((total_tests + 1))
    
    echo ""
done

# Summary
{
    echo "## Summary"
    echo "- Total operations: $total_tests"
    echo "- Regression threshold: ${THRESHOLD}%"
    echo ""
} >> "$REPORT"

echo ""
echo "═══════════════════════════════════════════════════════════════"
echo "Performance Summary"
echo "═══════════════════════════════════════════════════════════════"
echo ""

# Check if we have previous results to compare against
HAS_PREV=false
if [ ${#PREV_TIMES[@]} -gt 0 ]; then
    HAS_PREV=true
fi

if [ "$HAS_PREV" = true ]; then
    echo "Run-on-Run Comparison (vs Previous Baseline):"
    echo "───────────────────────────────────────────────────────────"
    echo ""
    
    # Organize by operation type
    for op in CREATE LIST HASH VALIDATE EXTRACT; do
        echo "$op Operations:"
        
        any_data=false
        for dataset_spec in "${TEST_SETS[@]}"; do
            IFS=':' read -r dataset_name dataset_label <<< "$dataset_spec"
            # Use dataset_name for consistency with stored keys
            key="${dataset_name}_${op}"
            
            if [ -n "${CURR_TIMES[$key]}" ] && [ -n "${PREV_TIMES[$key]}" ]; then
                any_data=true
                curr=${CURR_TIMES[$key]}
                prev=${PREV_TIMES[$key]}
                diff=$(calc_diff_percent $prev $curr)
                prev_fmt=$(format_time $prev)
                curr_fmt=$(format_time $curr)
                diff_fmt=$(format_diff $diff)
                
                printf "  %-20s  %8s → %8s  %s\n" \
                    "$dataset_label:" "$prev_fmt" "$curr_fmt" "$diff_fmt"
            fi
        done
        
        if [ "$any_data" = false ]; then
            echo "  (no previous data)"
        fi
        echo ""
    done
    
    echo "───────────────────────────────────────────────────────────"
    echo ""
    
    if [ "$regression_count" -gt 0 ]; then
        echo "⚠️  REGRESSION DETECTED"
        echo "    $regression_count operation(s) slower than ${THRESHOLD}%"
    fi
    
    if [ "$improvement_count" -gt 0 ]; then
        echo "✓  IMPROVEMENTS FOUND"
        echo "    $improvement_count operation(s) improved by more than ${THRESHOLD}%"
    fi
    
    if [ "$regression_count" -eq 0 ] && [ "$improvement_count" -eq 0 ]; then
        echo "✓  STABLE"
        echo "    All operations within ${THRESHOLD}% (no significant changes)"
    fi
else
    echo "📊 First Run - No Previous Baseline"
    echo "───────────────────────────────────────────────────────────"
    echo ""
    echo "Current Results:"
    echo ""
    
    for op in CREATE LIST HASH VALIDATE EXTRACT; do
        echo "$op Operations:"
        
        any_data=false
        for dataset_spec in "${TEST_SETS[@]}"; do
            IFS=':' read -r dataset_name dataset_label <<< "$dataset_spec"
            # Use dataset_name for consistency with stored keys
            key="${dataset_name}_${op}"
            
            if [ -n "${CURR_TIMES[$key]}" ]; then
                any_data=true
                curr=${CURR_TIMES[$key]}
                curr_fmt=$(format_time $curr)
                printf "  %-20s  %8s\n" "$dataset_label:" "$curr_fmt"
            fi
        done
        
        if [ "$any_data" = false ]; then
            echo "  (no data)"
        fi
        echo ""
    done
    
    echo "───────────────────────────────────────────────────────────"
    echo "This baseline will be used for future comparisons."
fi

echo ""
echo "═══════════════════════════════════════════════════════════════"
echo "Full report saved to: $REPORT"
echo ""
echo "View report:"
echo "  cat $REPORT"
echo ""
if [ "$HAS_PREV" = true ]; then
    echo "View changes from previous run:"
    echo "  diff -u <(head -100 $REPORT) <(cat $REPORT)"
fi
echo ""

