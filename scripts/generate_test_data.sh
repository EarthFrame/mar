#!/bin/bash
#
# Test Data Generator for MAR Integration Tests
#
# Generates deterministic test data for reproducible testing.
#
# Usage: SEED=42 generate_test_data.sh <type> <output_dir> [param1] [param2] ...

set -euo pipefail

SEED="${SEED:-42}"
DATA_TYPE="${1:-basic}"
OUTPUT_DIR="${2:-.}"
PARAM1="${3:-}"
PARAM2="${4:-}"

# Seed random number generator for reproducibility
RANDOM=$SEED

# =============================================================================
# Utility Functions
# =============================================================================

ensure_dir() {
    mkdir -p "$1"
}

random_content() {
    local size=$1
    local seed=$((SEED + RANDOM))
    head -c "$size" /dev/urandom | base64 | head -c "$size"
}

# =============================================================================
# Data Generators
# =============================================================================

generate_basic() {
    local dir="$OUTPUT_DIR"
    ensure_dir "$dir"
    
    echo "Hello, World!" > "$dir/hello.txt"
    echo "Test data content" > "$dir/data.txt"
    echo '{"config": "value"}' > "$dir/config.json"
    echo "Line 1" > "$dir/file1.txt"
    echo "Line 2" > "$dir/file2.txt"
}

generate_compressible() {
    local dir="$OUTPUT_DIR"
    ensure_dir "$dir"
    
    # Highly compressible content
    for i in {1..100}; do
        echo "This line repeats many times. This is predictable content for compression testing." >> "$dir/repeated.txt"
    done
    
    # Semi-compressible
    for i in {1..50}; do
        echo "Line $i: $RANDOM $RANDOM $RANDOM"
    done > "$dir/semi_random.txt"
    
    # Binary file
    dd if=/dev/urandom of="$dir/binary.bin" bs=1K count=100 2>/dev/null
}

generate_small_text() {
    local dir="$OUTPUT_DIR"
    local count="${PARAM1:-50}"
    ensure_dir "$dir"
    
    for i in $(seq 1 "$count"); do
        echo "File $i content" > "$dir/file_${i}.txt"
    done
}

generate_deep_dirs() {
    local dir="$OUTPUT_DIR"
    ensure_dir "$dir"
    
    # Create deep nesting (5 levels)
    mkdir -p "$dir/level_1/level_2/level_3/level_4/level_5"
    echo "Deep file" > "$dir/level_1/level_2/level_3/level_4/level_5/file.txt"
    echo "Level 3 file" > "$dir/level_1/level_2/level_3/file_1.txt"
    
    # Create wide structure (10 branches)
    for i in {1..10}; do
        mkdir -p "$dir/branch_$i"
        for j in {1..5}; do
            echo "Branch $i file $j" > "$dir/branch_$i/file_$j.txt"
        done
    done
}

generate_special_names() {
    local dir="$OUTPUT_DIR"
    ensure_dir "$dir"
    
    echo "spaces" > "$dir/file with spaces.txt"
    echo "dashes" > "$dir/file-with-dashes.txt"
    echo "dots" > "$dir/file.multiple.dots.txt"
    echo "underscore" > "$dir/file_underscore.txt"
    echo "mixed-case-FILE.TxT" > "$dir/mixed-case-FILE.TxT"
    echo "numbers" > "$dir/file123.txt"
}

generate_empty_files() {
    local dir="$OUTPUT_DIR"
    ensure_dir "$dir"
    
    # Mix of empty and non-empty files
    touch "$dir/empty.txt"
    touch "$dir/empty2.txt"
    echo "not empty" > "$dir/nonempty.txt"
    touch "$dir/empty3.txt"
    echo "also not empty" > "$dir/nonempty2.txt"
}

# =============================================================================
# Main
# =============================================================================

case "$DATA_TYPE" in
    basic)
        generate_basic
        ;;
    compressible)
        generate_compressible
        ;;
    small_text)
        generate_small_text
        ;;
    deep_dirs)
        generate_deep_dirs
        ;;
    special_names)
        generate_special_names
        ;;
    empty_files)
        generate_empty_files
        ;;
    *)
        echo "Unknown data type: $DATA_TYPE" >&2
        echo "Available types: basic, compressible, small_text, deep_dirs, special_names, empty_files" >&2
        exit 1
        ;;
esac

echo "Generated $DATA_TYPE test data in $OUTPUT_DIR"
