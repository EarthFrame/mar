# MAR Benchmarking Design

The MAR benchmarking suite is designed to provide high-precision, low-overhead performance measurements for the `mar` archival tool, comparing it against standard `tar` baselines.

## Core Components

1.  **`benchmarks/Makefile`**: The primary entry point for running benchmarks. It defines high-level targets like `flash-purge` and `full-purge`.
2.  **`benchmarks/run_easy.py`**: The main orchestration script. It:
    *   Discovers datasets in `benchmarks/data/`.
    *   Generates per-operation bash scripts (`case_*.sh`) for each benchmark case.
    *   Generates a master driver script (`run_easy.sh`) to execute the cases.
    *   Aggregates results and computes medians.
    *   Generates `results.csv` and `summary.md`.
3.  **`benchmarks/scripts/get_data.py`**: Handles downloading and preparing benchmark datasets from Earthframe's public cache.
4.  **`benchmarks/scripts/plot_results.R`**: Generates Tufte-style visualizations from the `results.csv` data.

## Timing Strategy

To ensure maximum accuracy and minimal overhead from the Python interpreter, timing is performed at the subprocess level within the generated bash driver script. It uses the native bash `date +%s%N` command for nanosecond-precision wall-clock timing.

## Cache Management

Accurate benchmarking of archival tools requires controlling for filesystem cache effects. The suite supports "cold cache" benchmarking via the `--purge-cache` flag (used in `flash-purge` and `full-purge`), which executes `sudo purge` (macOS) or drops caches via `/proc/sys/vm/drop_caches` (Linux) before each timed run.

## Benchmark Targets

### `make flash-purge` (Default)
*   **Purpose**: Fast iteration with accurate "cold cache" timing.
*   **Datasets**: `webster` (file) and `numpy-2.4.1` (directory).
*   **Repeats**: 1.
*   **Runtime**: ~1-2 minutes.
*   **Command**: `python3 run_easy.py --overwrite --datasets webster numpy-2.4.1 --repeats 1 --purge-cache`

### `make full-purge`
*   **Purpose**: Comprehensive, publishable benchmark results.
*   **Datasets**: All discovered datasets (Linux kernel, NumPy, CASP15, etc.).
*   **Repeats**: 3 (median reported).
*   **Runtime**: 30+ minutes.
*   **Command**: `python3 run_easy.py --overwrite --purge-cache --mar-create-matrix`

## Datasets

Datasets are stored in `benchmarks/data/` and include:
*   **Linux Kernel**: Large directory of source files.
*   **NumPy**: Python package directory.
*   **CASP15**: Protein structure files (PDB/CIF).
*   **Webster/Dickens**: Single-file text corpora.
*   **hg38**: Human genome reference data.
