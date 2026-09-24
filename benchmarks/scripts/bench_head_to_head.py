#!/usr/bin/env python3
"""
Comprehensive Head-to-Head Benchmark & Verification Suite:
Compares C++ CLI (`./mar`), Native Rust Engine (`mar-bench-rust`), and Python Package (`pymar` via PyO3).

Evaluates:
1. Creation (Throughput MB/s, Compression Ratio)
2. Whole Archive Extraction (Throughput MB/s)
3. Random Lookup via `mar get` (Microsecond Latency, Requests/sec)
4. Integrity Validation via `mar validate` (Throughput MB/s, Verification parity)
5. Fast Hashing via `mar hash` (XXHash64 & BLAKE3 Throughput, Bit-exact verification)
6. Cross-verification across implementations
"""

import os
import sys
import time
import random
import tempfile
import subprocess
from pathlib import Path

WORKSPACE_ROOT = Path(__file__).resolve().parents[2]
PYMAR_DIR = WORKSPACE_ROOT / "python"
CPP_BIN = WORKSPACE_ROOT / "mar"

def find_rust_bench_bin() -> Path:
    candidates = [
        WORKSPACE_ROOT / "target" / "release" / "mar-bench-rust",
        WORKSPACE_ROOT / "rust" / "target" / "release" / "mar-bench-rust",
        WORKSPACE_ROOT / "pymar" / "target" / "release" / "mar-bench-rust",
    ]
    if "CARGO_TARGET_DIR" in os.environ:
        candidates.insert(0, Path(os.environ["CARGO_TARGET_DIR"]) / "release" / "mar-bench-rust")
    try:
        res = subprocess.run(
            ["cargo", "metadata", "--format-version", "1", "--manifest-path", str(WORKSPACE_ROOT / "rust" / "Cargo.toml")],
            capture_output=True, text=True, check=False
        )
        if res.returncode == 0:
            import json
            meta = json.loads(res.stdout)
            candidates.insert(0, Path(meta["target_directory"]) / "release" / "mar-bench-rust")
    except Exception:
        pass

    for c in candidates:
        if c.exists():
            return c
    return candidates[0]

RUST_BIN = find_rust_bench_bin()

import pymar
import pymar._mar as _mar

def generate_dataset(target_dir: Path, target_total_mb: int = 100):
    print(f"\n[1/6] Generating synthetic mixed dataset (~{target_total_mb} MB) at {target_dir}...")
    target_dir.mkdir(parents=True, exist_ok=True)
    
    total_bytes = 0
    target_bytes = target_total_mb * 1024 * 1024
    
    file_idx = 0
    sizes = [
        4 * 1024,        # 4 KB (small)
        16 * 1024,       # 16 KB (small/mol)
        64 * 1024,       # 64 KB (medium)
        256 * 1024,      # 256 KB (medium)
        1024 * 1024,     # 1 MB (larger)
        5 * 1024 * 1024, # 5 MB (bulk)
    ]
    
    sample_text = (b"ATOM      1  N   ALA A   1      -1.250   1.400  -0.500  1.00 20.00           N\n"
                   b"ATOM      2  CA  ALA A   1       0.000   2.000   0.000  1.00 20.00           C\n"
                   b"ATOM      3  C   ALA A   1       1.200   1.200  -0.500  1.00 20.00           C\n"
                   b"HETATM   42  O1  ATP A 999       5.430  12.110   4.230  1.00 15.00           O\n"
                   b"REMARK 999 BENCHMARK DATASET SYNTHETIC PAYLOAD FOR OKF AND MAR ARCHIVE TESTS\n")
    
    while total_bytes < target_bytes:
        size = sizes[file_idx % len(sizes)]
        subdir = target_dir / f"bucket_{file_idx % 20}"
        subdir.mkdir(parents=True, exist_ok=True)
        file_path = subdir / f"data_{file_idx:05d}.dat"
        
        repeats = (size // len(sample_text)) + 1
        content = (sample_text * repeats)[:size]
        file_path.write_bytes(content)
        
        total_bytes += size
        file_idx += 1
        
    print(f"      Created {file_idx} files totaling {total_bytes / (1024*1024):.2f} MB.")
    return file_idx, total_bytes

def bench_creation_and_extraction(dataset_dir: Path, work_dir: Path, all_files: list, raw_mb: float):
    print("\n[2/6] Benchmarking Archive Creation and Extraction...")
    results = {}
    
    # 1. C++
    cpp_mar = work_dir / "cpp.mar"
    cpp_ext = work_dir / "cpp_ext"
    cpp_ext.mkdir(parents=True, exist_ok=True)
    
    t0 = time.perf_counter()
    # Pass relative paths from dataset_dir so archive entry names match dataset_dir relative paths
    subprocess.run([str(CPP_BIN), "create", "-c", "zstd", str(cpp_mar)] + [str(f.relative_to(dataset_dir)) for f in all_files], cwd=str(dataset_dir), check=True, capture_output=True)
    cpp_create_time = time.perf_counter() - t0
    cpp_size = cpp_mar.stat().st_size
    
    t0 = time.perf_counter()
    subprocess.run([str(CPP_BIN), "extract", str(cpp_mar), "-o", str(cpp_ext)], check=True, capture_output=True)
    cpp_extract_time = time.perf_counter() - t0
    results["C++"] = (cpp_create_time, cpp_size, cpp_extract_time)

    # 2. Rust
    rust_mar = work_dir / "rust.mar"
    rust_ext = work_dir / "rust_ext"
    rust_ext.mkdir(parents=True, exist_ok=True)
    
    res = subprocess.run([str(RUST_BIN), "create", str(rust_mar), str(dataset_dir)], check=True, capture_output=True, text=True)
    rust_create_time = float(res.stdout.strip())
    rust_size = rust_mar.stat().st_size
    
    res = subprocess.run([str(RUST_BIN), "extract", str(rust_mar), str(rust_ext)], check=True, capture_output=True, text=True)
    rust_extract_time = float(res.stdout.strip())
    results["Rust"] = (rust_create_time, rust_size, rust_extract_time)

    # 3. Python
    py_mar = work_dir / "python.mar"
    py_ext = work_dir / "py_ext"
    py_ext.mkdir(parents=True, exist_ok=True)
    
    t0 = time.perf_counter()
    opts = _mar.WriteOptions()
    opts.compression = _mar.CompressionAlgo.ZSTD
    opts.compression_level = 3
    writer = _mar.MarWriter(str(py_mar), opts)
    for f in all_files:
        rel = f.relative_to(dataset_dir)
        writer.add_file(str(f), str(rel))
    writer.finish()
    py_create_time = time.perf_counter() - t0
    py_size = py_mar.stat().st_size
    
    t0 = time.perf_counter()
    reader = pymar.open(str(py_mar))
    reader.extract(str(py_ext))
    py_extract_time = time.perf_counter() - t0
    results["Python"] = (py_create_time, py_size, py_extract_time)

    return results, cpp_mar, rust_mar, py_mar

def bench_random_lookups(cpp_mar: Path, rust_mar: Path, dataset_dir: Path, all_files: list, work_dir: Path, num_lookups: int = 500):
    print(f"\n[3/6] Benchmarking Random Lookups (mar get / reader.read_file) with {num_lookups} queries...")
    
    # Pick random files
    rel_names = [str(f.relative_to(dataset_dir)) for f in all_files]
    random.seed(42)
    sample_queries = [random.choice(rel_names) for _ in range(num_lookups)]
    
    query_file = work_dir / "query_list.txt"
    query_file.write_text("\n".join(sample_queries) + "\n")
    
    # 1. C++ CLI `mar get -c` (individual random lookups via pipe/sink on cpp_mar)
    t0 = time.perf_counter()
    for name in sample_queries[:min(50, len(sample_queries))]:  # test sample subset for process-spawn overhead
        subprocess.run([str(CPP_BIN), "get", "-c", str(cpp_mar), name], check=True, capture_output=True)
    cpp_cli_per_file = (time.perf_counter() - t0) / min(50, len(sample_queries)) * 1_000_000 # us

    # 2. Rust CLI `mar-bench-rust get` (in-process random lookups on rust_mar)
    res = subprocess.run([str(RUST_BIN), "get", str(rust_mar), str(query_file)], check=True, capture_output=True, text=True)
    rust_time_sec = float(res.stdout.split()[0])
    rust_us_per_lookup = (rust_time_sec / num_lookups) * 1_000_000

    # 3. Python in-process lookup via pymar (on rust_mar)
    archive = pymar.open(str(rust_mar))
    t0 = time.perf_counter()
    for name in sample_queries:
        data = archive[name]
    py_us_per_lookup = (time.perf_counter() - t0) / num_lookups * 1_000_000

    return {
        "C++ CLI (mar get process)": cpp_cli_per_file,
        "Rust Engine (in-process)": rust_us_per_lookup,
        "Python pymar (in-process)": py_us_per_lookup,
    }

def bench_validation(cpp_mar: Path, rust_mar: Path, py_mar: Path):
    print("\n[4/6] Benchmarking Integrity Validation (mar validate)...")
    res_val = {}

    # C++ CLI validate on cpp.mar
    t0 = time.perf_counter()
    subprocess.run([str(CPP_BIN), "validate", str(cpp_mar)], check=True, capture_output=True)
    res_val["C++ on cpp.mar"] = (time.perf_counter() - t0, True)

    # Rust engine validate on rust.mar
    res = subprocess.run([str(RUST_BIN), "validate", str(rust_mar)], check=True, capture_output=True, text=True)
    rust_val_time = float(res.stdout.strip())
    res_val["Rust on rust.mar"] = (rust_val_time, True)

    # Python validate on py_mar
    t0 = time.perf_counter()
    py_valid = pymar.mar_validate(str(py_mar))
    res_val["Python on python.mar"] = (time.perf_counter() - t0, py_valid)

    # Cross-validation: C++ on rust.mar
    t0 = time.perf_counter()
    res = subprocess.run([str(CPP_BIN), "validate", str(rust_mar)], check=True, capture_output=True)
    res_val["C++ cross-val on rust.mar"] = (time.perf_counter() - t0, True)

    # Cross-validation: C++ on python.mar
    t0 = time.perf_counter()
    res = subprocess.run([str(CPP_BIN), "validate", str(py_mar)], check=True, capture_output=True)
    res_val["C++ cross-val on python.mar"] = (time.perf_counter() - t0, True)

    # Cross-validation: Rust on cpp.mar
    res = subprocess.run([str(RUST_BIN), "validate", str(cpp_mar)], check=True, capture_output=True, text=True)
    rust_cross_time = float(res.stdout.strip())
    res_val["Rust cross-val on cpp.mar"] = (rust_cross_time, True)

    # Cross-validation: Python on cpp.mar
    t0 = time.perf_counter()
    cross_py_valid = pymar.mar_validate(str(cpp_mar))
    res_val["Python cross-val on cpp.mar"] = (time.perf_counter() - t0, cross_py_valid)

    return res_val

def bench_hashing(cpp_mar: Path, rust_mar: Path, py_mar: Path):
    print("\n[5/6] Benchmarking Fast Hash Algorithms (xxhash64 & blake3)...")
    hash_results = {}

    for algo in ["xxhash64", "blake3"]:
        # C++ CLI
        t0 = time.perf_counter()
        res = subprocess.run([str(CPP_BIN), "hash", "-a", algo, str(cpp_mar)], check=True, capture_output=True, text=True)
        cpp_hash = res.stdout.strip()
        cpp_time = time.perf_counter() - t0

        # Rust CLI
        res = subprocess.run([str(RUST_BIN), "hash", str(cpp_mar), algo], check=True, capture_output=True, text=True)
        rust_time, rust_hash = res.stdout.split()
        rust_time = float(rust_time)

        # Python
        t0 = time.perf_counter()
        py_hash = pymar.mar_hash(str(cpp_mar), algo=algo)
        py_time = time.perf_counter() - t0

        match = (cpp_hash == rust_hash == py_hash)
        hash_results[algo] = {
            "C++": (cpp_time, cpp_hash),
            "Rust": (rust_time, rust_hash),
            "Python": (py_time, py_hash),
            "Match": match
        }

    return hash_results

def print_summary(raw_mb, create_extract, lookups, validations, hashes):
    print("\n" + "=" * 90)
    print("COMPREHENSIVE HEAD-TO-HEAD SPEED & VERIFICATION RESULTS")
    print("=" * 90)
    print(f"Payload Tested: {raw_mb:.2f} MB across mixed small, medium, and bulk files")
    
    print("\n1. CREATION & EXTRACTION BENCHMARKS")
    print("-" * 90)
    print(f"{'Engine':<24} | {'Create Time':<13} | {'Create Rate':<13} | {'Extract Time':<13} | {'Extract Rate'}")
    print("-" * 90)
    for engine, (c_time, size, e_time) in create_extract.items():
        print(f"{engine:<24} | {c_time:>9.3f} s  | {raw_mb / c_time:>9.2f} MB/s | {e_time:>9.3f} s  | {raw_mb / e_time:>9.2f} MB/s")

    print("\n2. RANDOM LOOKUP LATENCY (mar get / reader.read_file)")
    print("-" * 90)
    print(f"{'Access Method':<36} | {'Latency per file':<20} | {'Throughput (lookups/sec)'}")
    print("-" * 90)
    for method, us in lookups.items():
        print(f"{method:<36} | {us:>14.2f} µs | {1_000_000 / us:>18.1f} files/s")

    print("\n3. ARCHIVE VALIDATION BENCHMARKS (mar validate)")
    print("-" * 90)
    print(f"{'Validation Target':<36} | {'Time':<12} | {'Validation Status'}")
    print("-" * 90)
    for target, (vtime, valid) in validations.items():
        status = "PASSED (OK)" if valid else "FAILED"
        print(f"{target:<36} | {vtime:>9.4f} s | {status}")

    print("\n4. FAST HASH VERIFICATION & BENCHMARKS (mar hash)")
    print("-" * 90)
    for algo, data in hashes.items():
        print(f"Algorithm: {algo.upper()} (Verified bit-exact parity across C++, Rust, and Python: {data['Match']})")
        print(f"  - Digest: {data['Python'][1]}")
        print(f"  - C++ Speed:    {data['C++'][0]*1000:.3f} ms")
        print(f"  - Rust Speed:   {data['Rust'][0]*1000:.3f} ms")
        print(f"  - Python Speed: {data['Python'][0]*1000:.3f} ms")
    print("=" * 90 + "\n")

def main():
    with tempfile.TemporaryDirectory() as tmp_root:
        tmp_path = Path(tmp_root)
        dataset_dir = tmp_path / "dataset"
        work_dir = tmp_path / "work"
        work_dir.mkdir(parents=True, exist_ok=True)
        
        file_count, raw_bytes = generate_dataset(dataset_dir, target_total_mb=20)
        raw_mb = raw_bytes / (1024 * 1024)
        all_files = [p for p in dataset_dir.rglob("*") if p.is_file()]
        
        create_extract, cpp_mar, rust_mar, py_mar = bench_creation_and_extraction(dataset_dir, work_dir, all_files, raw_mb)
        lookups = bench_random_lookups(cpp_mar, rust_mar, dataset_dir, all_files, work_dir, num_lookups=500)
        validations = bench_validation(cpp_mar, rust_mar, py_mar)
        hashes = bench_hashing(cpp_mar, rust_mar, py_mar)
        
        print_summary(raw_mb, create_extract, lookups, validations, hashes)

if __name__ == "__main__":
    main()
