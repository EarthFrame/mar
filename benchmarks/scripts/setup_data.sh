#!/bin/bash
# MAR Benchmark Data Setup
# Automates the extraction and preparation of datasets.

set -e

# Get repo root
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "$SCRIPT_DIR/../.." && pwd)"
DATA_DIR="$REPO_ROOT/benchmarks/data"

echo "Setting up benchmark data in $DATA_DIR..."

mkdir -p "$DATA_DIR"
cd "$DATA_DIR"

# 1. Linux Kernel
if [ -f "linux-6.18.5.tar.xz" ] && [ ! -d "linux-6.18.5" ]; then
    echo "Extracting Linux Kernel..."
    tar -xf linux-6.18.5.tar.xz
fi

# 2. Dickens and Webster
if [ -f "dickens.bz2" ] && [ ! -f "dickens" ]; then
    echo "Decompressing Dickens..."
    bunzip2 -k dickens.bz2
fi
if [ -f "webster.bz2" ] && [ ! -f "webster" ]; then
    echo "Decompressing Webster..."
    bunzip2 -k webster.bz2
fi

# 3. hg38
if [ -f "Homo_sapiens_assembly38.tar" ] && [ ! -d "hg38" ]; then
    echo "Extracting hg38..."
    mkdir -p hg38
    tar -xf Homo_sapiens_assembly38.tar -C hg38
fi

# 4. NumPy
if [ -f "numpy-2.4.1.tar.gz" ] && [ ! -d "numpy-2.4.1" ]; then
    echo "Extracting NumPy..."
    tar -xf numpy-2.4.1.tar.gz
fi

# 5. Makonin
if [ -f "dataverse_files.zip" ]; then
    echo "Preparing Makonin..."
    mkdir -p makonin
    mv dataverse_files.zip makonin/
    cd makonin
    unzip -q dataverse_files.zip || true
    cd ..
elif [ -d "makonin" ]; then
    echo "Makonin directory already exists."
fi

# 6. CASP15 (handled by separate python script but ensuring directory exists)
echo "Ensuring CASP15 is ready..."
if [ ! -d "$REPO_ROOT/casp15" ] || [ -z "$(ls -A "$REPO_ROOT/casp15")" ]; then
    echo "Running CASP15 download script..."
    python3 "$SCRIPT_DIR/download_casp15.py"
fi

echo "Data setup complete."
