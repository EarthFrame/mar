#!/usr/bin/env python3
"""
Direct Head-to-Head Speed Benchmark: C++ CLI (`./mar`) vs. Rust CLI (`./mar-rust`).

Measures wall-clock time and throughput across:
1. `create` (zstd, lz4, uncompressed)
2. `extract` (whole archive)
3. `list` (table, json)
4. `validate` (integrity check across all blocks)
5. `header`
6. `diff` (compare archives)
7. `hash` (xxhash64, blake3)
8. `get` (single-file random access)
9. Cross-compatibility verification
"""

import os
import sys
import time
import shutil
import tempfile
import subprocess
from pathlib import Path

WORKSPACE = Path(__file__).resolve().parents[2]
CPP_BIN = WORKSPACE / "mar"
RUST_BIN = WORKSPACE / "mar-rust"

assert CPP_BIN.exists(), f"Missing C++ CLI at {CPP_BIN}"
assert RUST_BIN.exists(), f"Missing Rust CLI at {RUST_BIN}"


def generate_benchmark_dataset(dataset_dir: Path, target_mb: int = 50) -> tuple[int, int]:
    """Generate a realistic dataset with small text, medium JSON/data, and larger binary files."""
    dataset_dir.mkdir(parents=True, exist_ok=True)
    total_bytes = 0
    target_bytes = target_mb * 1024 * 1024
    
    file_idx = 0
    file_configs = [
        (4 * 1024, b"ATOM %5d  CA  ALA A%4d      %8.3f%8.3f%8.3f  1.00 20.00           C\n"),
        (16 * 1024, b'{"id": %d, "measurement": 42.123, "description": "synthetic benchmark payload data line"}\n'),
        (64 * 1024, b"GENE_%06d ATCGATCGATCGATCGATCGATCGATCGATCGATCGATCGATCGATCGATCGATCGATCGATCGATCG\n"),
        (256 * 1024, b"LOG entry %d: transaction completed successfully with status 200 OK timestamp\n"),
        (1024 * 1024, b"BINARY_BLOCK_CHUNK_%08d_PADDING_REPEATED_DATA_TO_SIMULATE_LARGER_ARRAYS_XYZ\n"),
    ]

    while total_bytes < target_bytes:
        size, template = file_configs[file_idx % len(file_configs)]
        subdir = dataset_dir / f"bucket_{file_idx % 16}"
        subdir.mkdir(parents=True, exist_ok=True)
        file_path = subdir / f"file_{file_idx:05d}.dat"
        
        chunk = template % ((file_idx,) * template.count(b"%"))
        repeats = (size // len(chunk)) + 1
        data = (chunk * repeats)[:size]
        file_path.write_bytes(data)
        
        total_bytes += len(data)
        file_idx += 1

    return file_idx, total_bytes


def run_timed(cmd: list[str], runs: int = 3) -> tuple[float, bytes, bytes]:
    """Run command multiple times, return minimum elapsed time (best-of-N to minimize OS jitter)."""
    best_time = float("inf")
    stdout, stderr = b"", b""
    for _ in range(runs):
        t0 = time.perf_counter()
        proc = subprocess.run(cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
        elapsed = time.perf_counter() - t0
        if proc.returncode != 0:
            raise RuntimeError(f"Command failed ({proc.returncode}): {' '.join(cmd)}\nstderr: {proc.stderr.decode('utf-8', errors='replace')}")
        if elapsed < best_time:
            best_time = elapsed
            stdout, stderr = proc.stdout, proc.stderr
    return best_time, stdout, stderr


def main():
    print("=" * 84)
    print("      MAR CLI HEAD-TO-HEAD SPEED TEST: C++ (`./mar`) vs Pure Rust (`./mar-rust`)")
    print("=" * 84)

    with tempfile.TemporaryDirectory() as tmp_root:
        tmp_dir = Path(tmp_root)
        data_dir = tmp_dir / "dataset"
        out_dir = tmp_dir / "out"
        out_dir.mkdir(parents=True, exist_ok=True)

        target_mb = 50
        print(f"[*] Generating test dataset (~{target_mb} MB mixed files)...")
        file_count, total_bytes = generate_benchmark_dataset(data_dir, target_mb=target_mb)
        raw_mb = total_bytes / (1024 * 1024)
        print(f"    Created {file_count} files across 16 directories ({raw_mb:.2f} MB total).\n")

        results = []

        # ---------------------------------------------------------------------
        # 1. CREATE BENCHMARKS
        # ---------------------------------------------------------------------
        print("[1/6] Benchmarking `mar create` across compression codecs...")
        for comp in ["zstd", "lz4", "none"]:
            cpp_mar = out_dir / f"cpp_{comp}.mar"
            rust_mar = out_dir / f"rust_{comp}.mar"

            cmd_cpp = [str(CPP_BIN), "create", "-f", "-c", comp, str(cpp_mar), str(data_dir)]
            cmd_rust = [str(RUST_BIN), "create", "-f", "-c", comp, str(rust_mar), str(data_dir)]

            t_cpp, _, _ = run_timed(cmd_cpp, runs=3)
            t_rust, _, _ = run_timed(cmd_rust, runs=3)

            rate_cpp = raw_mb / t_cpp
            rate_rust = raw_mb / t_rust
            speedup = t_cpp / t_rust if t_rust > 0 else 0

            results.append({
                "category": "Create",
                "test": f"create (-c {comp})",
                "cpp_time": t_cpp,
                "rust_time": t_rust,
                "cpp_metric": f"{rate_cpp:.1f} MB/s",
                "rust_metric": f"{rate_rust:.1f} MB/s",
                "speedup": speedup,
                "size_cpp": cpp_mar.stat().st_size,
                "size_rust": rust_mar.stat().st_size,
            })
            print(f"    - create (-c {comp:<4}): C++ {t_cpp*1000:>6.1f} ms ({rate_cpp:>6.1f} MB/s) | Rust {t_rust*1000:>6.1f} ms ({rate_rust:>6.1f} MB/s) => {speedup:.2f}x")

        # ---------------------------------------------------------------------
        # 2. EXTRACT BENCHMARKS
        # ---------------------------------------------------------------------
        print("\n[2/6] Benchmarking `mar extract`...")
        for comp in ["zstd", "lz4"]:
            src_archive = out_dir / f"rust_{comp}.mar"
            ext_cpp = out_dir / f"ext_cpp_{comp}"
            ext_rust = out_dir / f"ext_rust_{comp}"

            def do_extract(bin_path: Path, dst: Path):
                best = float("inf")
                for _ in range(3):
                    if dst.exists():
                        shutil.rmtree(dst)
                    dst.mkdir(parents=True, exist_ok=True)
                    t0 = time.perf_counter()
                    subprocess.run([str(bin_path), "extract", str(src_archive), "-o", str(dst)], check=True, stdout=subprocess.DEVNULL)
                    el = time.perf_counter() - t0
                    if el < best:
                        best = el
                return best

            t_cpp = do_extract(CPP_BIN, ext_cpp)
            t_rust = do_extract(RUST_BIN, ext_rust)

            rate_cpp = raw_mb / t_cpp
            rate_rust = raw_mb / t_rust
            speedup = t_cpp / t_rust

            results.append({
                "category": "Extract",
                "test": f"extract ({comp})",
                "cpp_time": t_cpp,
                "rust_time": t_rust,
                "cpp_metric": f"{rate_cpp:.1f} MB/s",
                "rust_metric": f"{rate_rust:.1f} MB/s",
                "speedup": speedup,
            })
            print(f"    - extract ({comp:<4}): C++ {t_cpp*1000:>6.1f} ms ({rate_cpp:>6.1f} MB/s) | Rust {t_rust*1000:>6.1f} ms ({rate_rust:>6.1f} MB/s) => {speedup:.2f}x")

        # ---------------------------------------------------------------------
        # 3. VALIDATE BENCHMARKS (Cross & Native)
        # ---------------------------------------------------------------------
        print("\n[3/6] Benchmarking `mar validate` (integrity verification)...")
        zstd_archive = out_dir / "rust_zstd.mar"
        t_cpp, _, _ = run_timed([str(CPP_BIN), "validate", str(zstd_archive)], runs=5)
        t_rust, _, _ = run_timed([str(RUST_BIN), "validate", str(zstd_archive)], runs=5)

        speedup = t_cpp / t_rust
        results.append({
            "category": "Validate",
            "test": "validate (native)",
            "cpp_time": t_cpp,
            "rust_time": t_rust,
            "cpp_metric": f"{raw_mb / t_cpp:.1f} MB/s",
            "rust_metric": f"{raw_mb / t_rust:.1f} MB/s",
            "speedup": speedup,
        })
        print(f"    - validate:         C++ {t_cpp*1000:>6.1f} ms | Rust {t_rust*1000:>6.1f} ms => {speedup:.2f}x")

        # Cross validate: C++ on Rust, Rust on C++
        cpp_archive = out_dir / "cpp_zstd.mar"
        t_cpp_on_rust, _, _ = run_timed([str(CPP_BIN), "validate", str(zstd_archive)], runs=3)
        t_rust_on_cpp, _, _ = run_timed([str(RUST_BIN), "validate", str(cpp_archive)], runs=3)
        print(f"    - cross-val:       C++ on rust.mar: {t_cpp_on_rust*1000:>5.1f} ms (OK) | Rust on cpp.mar: {t_rust_on_cpp*1000:>5.1f} ms (OK)")

        # ---------------------------------------------------------------------
        # 4. FAST HASH BENCHMARKS (`mar hash`)
        # ---------------------------------------------------------------------
        print("\n[4/6] Benchmarking `mar hash` (archive hashing)...")
        for algo in ["xxhash64", "blake3"]:
            cmd_cpp = [str(CPP_BIN), "hash", "-a", algo, str(zstd_archive)]
            cmd_rust = [str(RUST_BIN), "hash", "-a", algo, str(zstd_archive)]
            t_cpp, out_c, _ = run_timed(cmd_cpp, runs=5)
            t_rust, out_r, _ = run_timed(cmd_rust, runs=5)

            assert out_c.strip() == out_r.strip(), f"Hash mismatch! C++: {out_c} vs Rust: {out_r}"
            speedup = t_cpp / t_rust
            results.append({
                "category": "Hash",
                "test": f"hash (-a {algo})",
                "cpp_time": t_cpp,
                "rust_time": t_rust,
                "cpp_metric": f"{t_cpp*1000:.2f} ms",
                "rust_metric": f"{t_rust*1000:.2f} ms",
                "speedup": speedup,
            })
            print(f"    - hash ({algo:<8}): C++ {t_cpp*1000:>5.2f} ms | Rust {t_rust*1000:>5.2f} ms => {speedup:.2f}x (Bit-exact: {out_c.strip().decode()})")

        # ---------------------------------------------------------------------
        # 5. METADATA COMMANDS (`list`, `header`, `diff`)
        # ---------------------------------------------------------------------
        print("\n[5/6] Benchmarking metadata and lookup commands (`list`, `header`, `diff`)...")
        # List
        t_cpp, _, _ = run_timed([str(CPP_BIN), "list", str(zstd_archive)], runs=5)
        t_rust, _, _ = run_timed([str(RUST_BIN), "list", str(zstd_archive)], runs=5)
        results.append({
            "category": "Metadata",
            "test": "list (standard)",
            "cpp_time": t_cpp,
            "rust_time": t_rust,
            "cpp_metric": f"{t_cpp*1000:.2f} ms",
            "rust_metric": f"{t_rust*1000:.2f} ms",
            "speedup": t_cpp / t_rust,
        })
        print(f"    - list:             C++ {t_cpp*1000:>5.2f} ms | Rust {t_rust*1000:>5.2f} ms => {t_cpp / t_rust:.2f}x")

        # Header
        t_cpp, _, _ = run_timed([str(CPP_BIN), "header", str(zstd_archive)], runs=10)
        t_rust, _, _ = run_timed([str(RUST_BIN), "header", str(zstd_archive)], runs=10)
        results.append({
            "category": "Metadata",
            "test": "header",
            "cpp_time": t_cpp,
            "rust_time": t_rust,
            "cpp_metric": f"{t_cpp*1000:.2f} ms",
            "rust_metric": f"{t_rust*1000:.2f} ms",
            "speedup": t_cpp / t_rust,
        })
        print(f"    - header:           C++ {t_cpp*1000:>5.2f} ms | Rust {t_rust*1000:>5.2f} ms => {t_cpp / t_rust:.2f}x")

        # Diff (identical archives)
        t_cpp, _, _ = run_timed([str(CPP_BIN), "diff", str(zstd_archive), str(zstd_archive)], runs=5)
        t_rust, _, _ = run_timed([str(RUST_BIN), "diff", str(zstd_archive), str(zstd_archive)], runs=5)
        results.append({
            "category": "Metadata",
            "test": "diff (identical)",
            "cpp_time": t_cpp,
            "rust_time": t_rust,
            "cpp_metric": f"{t_cpp*1000:.2f} ms",
            "rust_metric": f"{t_rust*1000:.2f} ms",
            "speedup": t_cpp / t_rust,
        })
        print(f"    - diff:             C++ {t_cpp*1000:>5.2f} ms | Rust {t_rust*1000:>5.2f} ms => {t_cpp / t_rust:.2f}x")

        # ---------------------------------------------------------------------
        # 6. RANDOM LOOKUPS (`mar get`)
        # ---------------------------------------------------------------------
        print("\n[6/6] Benchmarking random file extraction (`mar get`)...")
        # In both archives created with `data_dir`, paths inside the archive include data/ prefix
        all_files = [f"dataset/{p.relative_to(data_dir)}" for p in data_dir.rglob("*") if p.is_file()][:25]
        t0 = time.perf_counter()
        for f in all_files:
            subprocess.run([str(CPP_BIN), "get", "-c", str(zstd_archive), f], check=True, stdout=subprocess.DEVNULL)
        cpp_get_total = time.perf_counter() - t0

        t0 = time.perf_counter()
        for f in all_files:
            subprocess.run([str(RUST_BIN), "get", "-c", str(zstd_archive), f], check=True, stdout=subprocess.DEVNULL)
        rust_get_total = time.perf_counter() - t0

        cpp_get_per_file = (cpp_get_total / len(all_files)) * 1000
        rust_get_per_file = (rust_get_total / len(all_files)) * 1000
        speedup = cpp_get_total / rust_get_total

        results.append({
            "category": "Lookup",
            "test": f"get (x{len(all_files)} random files)",
            "cpp_time": cpp_get_total,
            "rust_time": rust_get_total,
            "cpp_metric": f"{cpp_get_per_file:.2f} ms/file",
            "rust_metric": f"{rust_get_per_file:.2f} ms/file",
            "speedup": speedup,
        })
        print(f"    - get (CLI process): C++ {cpp_get_per_file:>5.2f} ms/file | Rust {rust_get_per_file:>5.2f} ms/file => {speedup:.2f}x")

        # ---------------------------------------------------------------------
        # SUMMARY TABLE
        # ---------------------------------------------------------------------
        print("\n" + "=" * 84)
        print("                           FINAL BENCHMARK SUMMARY")
        print("=" * 84)
        print(f"{'Operation / Test':<25} | {'C++ CLI':<16} | {'Rust CLI':<16} | {'Speedup':<10} | {'Faster Engine'}")
        print("-" * 84)
        for r in results:
            speedup_str = f"{r['speedup']:.2f}x"
            if r['speedup'] >= 1.05:
                winner = f"Rust ({r['speedup']:.2f}x)"
            elif r['speedup'] <= 0.95:
                winner = f"C++ ({1.0/r['speedup']:.2f}x)"
            else:
                winner = "Parity (±5%)"

            col_cpp = f"{r['cpp_time']*1000:>6.1f} ms" if "MB/s" not in r['cpp_metric'] else r['cpp_metric']
            col_rust = f"{r['rust_time']*1000:>6.1f} ms" if "MB/s" not in r['rust_metric'] else r['rust_metric']
            print(f"{r['test']:<25} | {col_cpp:<16} | {col_rust:<16} | {speedup_str:<10} | {winner}")
        print("=" * 84)


if __name__ == "__main__":
    main()
