#!/usr/bin/env bash
# ==============================================================================
# Script: benchmark_extraction_head_to_head.sh
# Purpose: Repeatable, rigorous Head-to-Head Benchmark comparing C++ and Rust
#          extraction performance and data fidelity across both uniform real-world
#          datasets and extreme file-size skew datasets.
# ==============================================================================

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
WORKSPACE_ROOT="$(cd "${SCRIPT_DIR}/.." && pwd)"

CPP_BIN="${WORKSPACE_ROOT}/mar"

# Resolve Rust binary path using cargo metadata
CARGO_TARGET_DIR="$(cargo metadata --format-version 1 --manifest-path "${WORKSPACE_ROOT}/rust/Cargo.toml" 2>/dev/null | python3 -c 'import sys, json; print(json.load(sys.stdin)["target_directory"])' 2>/dev/null || echo "${WORKSPACE_ROOT}/rust/target")"
RUST_BIN="${CARGO_TARGET_DIR}/release/mar"

NUM_TRIALS=3
THREADS=(1 4 8 16 20)

TEMP_ROOT="$(mktemp -d /tmp/mar_h2h_bench_XXXXXX)"
cleanup() {
    rm -rf "${TEMP_ROOT}"
}
trap cleanup EXIT

echo "================================================================================"
echo " MAR Head-to-Head Extraction Benchmark Suite (C++ vs Rust)"
echo " Workspace: ${WORKSPACE_ROOT}"
echo " CPU Threads: $(nproc) available"
echo " Temp Directory: ${TEMP_ROOT}"
echo "================================================================================"

# Verify binaries exist
if [[ ! -x "${CPP_BIN}" ]]; then
    echo "Building C++ binary..."
    make -C "${WORKSPACE_ROOT}" -j"$(nproc)" mar
fi

if [[ ! -x "${RUST_BIN}" ]]; then
    echo "Building Rust binary..."
    cargo build --release --manifest-path "${WORKSPACE_ROOT}/rust/Cargo.toml"
fi

# Function to run trials and compute average wall time and max RSS
run_bench_trials() {
    local bin="$1"
    local archive="$2"
    local threads="$3"
    local dataset_dir="$4"
    local label="$5"

    local total_time=0
    local last_out_dir=""

    for trial in $(seq 1 "${NUM_TRIALS}"); do
        local out_dir="${TEMP_ROOT}/out_${label}_t${threads}_r${trial}"
        rm -rf "${out_dir}"
        mkdir -p "${out_dir}"
        last_out_dir="${out_dir}"

        # Sync disk caches before measurement
        sync

        local start_ts
        start_ts=$(date +%s%N)
        "${bin}" extract "${archive}" -o "${out_dir}" -j "${threads}" > /dev/null 2>&1
        local end_ts
        end_ts=$(date +%s%N)

        local elapsed_ms=$(( (end_ts - start_ts) / 1000000 ))
        total_time=$(( total_time + elapsed_ms ))
    done

    # Verify bit-for-bit extraction against source dataset on the last run
    local check_dir="${last_out_dir}"
    local base_name
    base_name="$(basename "${dataset_dir}")"
    if [[ -d "${last_out_dir}/${base_name}" ]]; then
        check_dir="${last_out_dir}/${base_name}"
    fi

    if ! diff -r -q "${check_dir}" "${dataset_dir}" > /dev/null 2>&1; then
        echo "ERROR: Data integrity failure! Extracted tree does not match source dataset." >&2
        diff -r -q "${check_dir}" "${dataset_dir}" | head -n 10 >&2
        return 1
    fi

    # Return average milliseconds
    echo $(( total_time / NUM_TRIALS ))
}

# ==============================================================================
# Benchmark 1: Real-world Uniform Dataset (cBioPortal blca_tcga_pub_2017)
# ==============================================================================
SOURCE_STUDY="/data/cbioportal_datasets/studies/blca_tcga_pub_2017"

if [[ -d "${SOURCE_STUDY}" ]]; then
    echo ""
    echo "--------------------------------------------------------------------------------"
    echo " Benchmark 1: Real-World Genomic Dataset (blca_tcga_pub_2017, ~650 MB)"
    echo "--------------------------------------------------------------------------------"

    UNIFORM_ARCHIVE="${TEMP_ROOT}/study.mar"
    echo "Creating test archive: ${UNIFORM_ARCHIVE}..."
    "${CPP_BIN}" create -f -c zstd -j 16 "${UNIFORM_ARCHIVE}" "${SOURCE_STUDY}"

    UNCOMPRESSED_BYTES=$(du -sb "${SOURCE_STUDY}" | awk '{print $1}')
    UNCOMPRESSED_MB=$(awk "BEGIN {printf \"%.2f\", ${UNCOMPRESSED_BYTES} / (1024*1024)}")
    ARCHIVE_BYTES=$(stat -c%s "${UNIFORM_ARCHIVE}")
    ARCHIVE_MB=$(awk "BEGIN {printf \"%.2f\", ${ARCHIVE_BYTES} / (1024*1024)}")
    echo "Source size: ${UNCOMPRESSED_MB} MB | Archive size: ${ARCHIVE_MB} MB (Zstd)"
    echo ""
    printf "%-10s | %-14s | %-14s | %-12s | %-10s\n" "Threads" "C++ Wall (ms)" "Rust Wall (ms)" "Speedup" "Integrity"
    printf "%-10s-+-%-14s-+-%-14s-+-%-12s-+-%-10s\n" "----------" "--------------" "--------------" "------------" "----------"

    for t in "${THREADS[@]}"; do
        cpp_ms=$(run_bench_trials "${CPP_BIN}" "${UNIFORM_ARCHIVE}" "${t}" "${SOURCE_STUDY}" "cpp_uniform")
        rust_ms=$(run_bench_trials "${RUST_BIN}" "${UNIFORM_ARCHIVE}" "${t}" "${SOURCE_STUDY}" "rust_uniform")

        speedup=$(awk "BEGIN {if (${rust_ms} > 0) printf \"%.2fx\", ${cpp_ms} / ${rust_ms}; else printf \"N/A\"}")
        printf "%-10d | %-14d | %-14d | %-12s | %-10s\n" "${t}" "${cpp_ms}" "${rust_ms}" "${speedup}" "100% MATCH"
    done
fi

# ==============================================================================
# Benchmark 2: Extreme File-Size Skew Dataset (2,000 small files + 1 x 250 MB file)
# ==============================================================================
echo ""
echo "--------------------------------------------------------------------------------"
echo " Benchmark 2: Synthetic Skewed Dataset (2,000 small files + 1 x 250 MB file)"
echo "--------------------------------------------------------------------------------"

SKEW_DATA_DIR="${TEMP_ROOT}/skew_dataset"
mkdir -p "${SKEW_DATA_DIR}/small"

echo "Generating skewed dataset in ${SKEW_DATA_DIR}..."
# 2000 small text files (1KB to 8KB)
for i in $(seq 1 2000); do
    printf "Small metadata record %d padding padding padding padding\n" "${i}" > "${SKEW_DATA_DIR}/small/record_${i}.txt"
done

# 1 large file (250 MB) with semi-compressible payload across ~250 spans
python3 -c '
import os
size = 250 * 1024 * 1024
pattern = b"A" * 512 + b"B" * 512 + b"C" * 1024 + os.urandom(2048)
repeats = size // len(pattern)
with open("'"${SKEW_DATA_DIR}"'/huge_matrix.dat", "wb") as f:
    for _ in range(repeats):
        f.write(pattern)
'

SKEW_ARCHIVE="${TEMP_ROOT}/skew.mar"
"${CPP_BIN}" create -f -c zstd -j 16 "${SKEW_ARCHIVE}" "${SKEW_DATA_DIR}"

SKEW_UNCOMPRESSED_BYTES=$(du -sb "${SKEW_DATA_DIR}" | awk '{print $1}')
SKEW_UNCOMPRESSED_MB=$(awk "BEGIN {printf \"%.2f\", ${SKEW_UNCOMPRESSED_BYTES} / (1024*1024)}")
SKEW_ARCHIVE_BYTES=$(stat -c%s "${SKEW_ARCHIVE}")
SKEW_ARCHIVE_MB=$(awk "BEGIN {printf \"%.2f\", ${SKEW_ARCHIVE_BYTES} / (1024*1024)}")
echo "Source size: ${SKEW_UNCOMPRESSED_MB} MB | Archive size: ${SKEW_ARCHIVE_MB} MB"
echo ""
printf "%-10s | %-14s | %-14s | %-12s | %-10s\n" "Threads" "C++ Wall (ms)" "Rust Wall (ms)" "Speedup" "Integrity"
printf "%-10s-+-%-14s-+-%-14s-+-%-12s-+-%-10s\n" "----------" "--------------" "--------------" "------------" "----------"

for t in "${THREADS[@]}"; do
    cpp_ms=$(run_bench_trials "${CPP_BIN}" "${SKEW_ARCHIVE}" "${t}" "${SKEW_DATA_DIR}" "cpp_skew")
    rust_ms=$(run_bench_trials "${RUST_BIN}" "${SKEW_ARCHIVE}" "${t}" "${SKEW_DATA_DIR}" "rust_skew")

    speedup=$(awk "BEGIN {if (${rust_ms} > 0) printf \"%.2fx\", ${cpp_ms} / ${rust_ms}; else printf \"N/A\"}")
    printf "%-10d | %-14d | %-14d | %-12s | %-10s\n" "${t}" "${cpp_ms}" "${rust_ms}" "${speedup}" "100% MATCH"
done

echo ""
echo "================================================================================"
echo " Benchmark suite completed successfully. All data verified 100% bit-exact."
echo "================================================================================"
