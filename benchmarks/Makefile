# MAR Benchmarks Makefile
#
# Two main workflows:
# 1. Flash-purge (default): fast, cold-cache benchmark for quick iteration
# 2. Full-purge:           comprehensive, cold-cache, all datasets

PYTHON = python3
RSCRIPT = Rscript
SCRIPT_DIR = scripts
CONFIG_DIR = configs/default
RESULTS_CSV = results.csv
VISUALS_DIR = visuals

# Threading: 0 = "use MAR defaults" (typically CPU cores)
THREADS ?= 0

.PHONY: flash-purge full-purge plot clean help setup-data

# ============================================================================
# DEFAULT: flash-purge (fast, accurate, good for iteration)
# ============================================================================
# Fast benchmarking with cache purge: 2 datasets, 1 repeat, cold cache
# Purpose: Quick iteration with accurate timing (cache doesn't affect results)
# Runtime: ~1-2 minutes on modern hardware
#

all: flash-purge

flash-purge: setup-data
	@echo "Running flash-purge: fast iteration with cold cache..."
	$(PYTHON) run_easy.py --overwrite --datasets webster numpy-2.4.1 --repeats 1 --get-count 10 --purge-cache --threads $(THREADS)

# ============================================================================
# FULL: full-purge (comprehensive, cold-cache, all datasets)
# ============================================================================
# Complete benchmarking: all datasets, 3 repeats, cold cache, matrix variants
# Purpose: Publishable benchmark results (most accurate)
# Runtime: 30+ minutes depending on dataset sizes
#

full-purge: setup-data
	@echo "Running full benchmarks with cache purging: comprehensive benchmark with cold cache..."
	$(PYTHON) run_easy.py --overwrite --purge-cache --mar-create-matrix --threads $(THREADS)


setup-data:
	@echo "Preparing benchmark data (may take up to one hour) ..."
	@$(PYTHON) $(SCRIPT_DIR)/get_data.py --check-only || $(PYTHON) $(SCRIPT_DIR)/get_data.py

# ============================================================================
# PLOTTING AND UTILITIES
# ============================================================================

plot: check-r-packages
	@$(RSCRIPT) $(SCRIPT_DIR)/plot_results.R $(RESULTS_CSV) $(VISUALS_DIR)

# Check and install R packages if needed (non-fatal - plotting will fail with clear error if packages missing)
check-r-packages:
	@$(RSCRIPT) $(SCRIPT_DIR)/install_r_packages.R || true

parity:
	$(PYTHON) $(SCRIPT_DIR)/metadata_parity.py --write-doc

clean:
	rm -rf profile_output/
	rm -f $(RESULTS_CSV)
	rm -f $(SCRIPT_DIR)/run_internal.sh
	rm -rf $(VISUALS_DIR)/*.png
	rm -rf scratch_easy/
	rm -f summary.md

# ============================================================================
# HELP
# ============================================================================

help:
	@echo "MAR Benchmarking Makefile"
	@echo ""
	@echo "PRIMARY TARGETS:"
	@echo ""
	@echo "  make                 - Run flash-purge (default: fast iteration with cold cache)"
	@echo "  make flash-purge     - Fast iteration: 2 datasets, 1 repeat, cold cache (~1-2 min)"
	@echo "  make full-purge      - Comprehensive: all datasets, 3 repeats, cold cache (~30+ min)"
	@echo ""
	@echo "TUNING:"
	@echo "  make THREADS=4 flash-purge   - Use 4 threads (default: 0 = MAR defaults)"
	@echo ""
	@echo "UTILITIES:"
	@echo "  make setup-data      - Extract and prepare benchmark datasets"
	@echo "  make plot            - (Re)generate plots from results.csv"
	@echo "  make parity          - Write docs/METADATA_PARITY.md"
	@echo "  make clean           - Remove all benchmark results and plots"
	@echo "  make help            - Show this help message"
	@echo ""
	@echo "NOTES:"
	@echo "  • flash-purge/full-purge require 'sudo' (for cache purging)"
	@echo "  • Cold cache (-purge) gives accurate timing, not affected by warm state"
	@echo ""
	@echo "R PACKAGE DEPENDENCIES:"
	@echo "  • Plotting requires: tidyverse, scales"
	@echo "  • Auto-installed via 'make plot' (requires internet)"
	@echo ""

