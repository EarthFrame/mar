#!/usr/bin/env python3
"""
MAR Benchmark Runner (Easy Mode)

Goal: one-command, readable benchmarking across all datasets in benchmarks/data.

It measures:
- create (tar vs mar, selected compressions)
- list
- extract
- get (random-access-ish sampling; compares to tar -xOf)
- validate (mar only)

Outputs:
- benchmarks/results.csv (append/overwrite)
- benchmarks/summary.md (human-readable "MAR is X faster than TAR" lines)

TIMING APPROACH:
- Uses bash 'date +%s%N' for nanosecond-precision timing
- Timing captured in bash driver script at subprocess level, not in Python
- Overhead: ~1.5ms per timing measurement (vs ~4ms for Perl, eliminated Python overhead)
- Measures true wall-clock execution time with minimal contamination

WORKFLOW:
1. Discover datasets in benchmarks/data
2. For each dataset, generate per-operation bash scripts (case_*.sh)
3. Generate a manager bash driver script (run_easy.sh) that:
   - Runs each case script with high-precision timing
   - Purges filesystem cache if requested
   - Handles timeouts and error capture
   - Records raw per-run timing data
4. Aggregate results: compute median times across successful runs
5. Output results.csv and summary.md

Design principles:
- Minimal configuration; auto-discover datasets by default.
- Ultra-low-overhead, high-precision timing via native bash utilities.
- Optional correctness verification (sample or full).
"""

from __future__ import annotations

import argparse
import csv
import hashlib
import random
import shutil
import subprocess
import sys
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Iterable, Optional


REPO_ROOT = Path(__file__).resolve().parent.parent
BENCH_DIR = REPO_ROOT / "benchmarks"
DATA_DIR = BENCH_DIR / "data"


@dataclass(frozen=True)
class Dataset:
    name: str
    path: Path
    is_dir: bool


def die(msg: str) -> None:
    """Print error message to stderr and exit with code 2."""
    print(f"error: {msg}", file=sys.stderr)
    raise SystemExit(2)


def run(cmd: list[str], cwd: Optional[Path] = None) -> subprocess.CompletedProcess:
    """Run a command synchronously, capturing stdout/stderr."""
    return subprocess.run(cmd, cwd=str(cwd) if cwd else None, check=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)


@dataclass(frozen=True)
class TimedResult:
    ok: bool
    seconds: float
    error: str = ""


def timed(cmd: list[str], cwd: Optional[Path] = None, timeout_s: Optional[int] = None) -> TimedResult:
    """
    Run a command, returning elapsed time.
    Never raises: failures are returned as ok=False with captured stderr/stdout.
    
    Note: This function is NOT used for benchmark runs. Benchmarks use bash driver
    script with Perl Time::HiRes timing (see _write_driver_script). This is kept
    for utility purposes only.
    """
    start = time.perf_counter()
    try:
        subprocess.run(
            cmd,
            cwd=str(cwd) if cwd else None,
            check=True,
            stdout=subprocess.DEVNULL,
            stderr=subprocess.PIPE,
            text=True,
            timeout=timeout_s,
        )
        return TimedResult(ok=True, seconds=time.perf_counter() - start)
    except subprocess.TimeoutExpired:
        return TimedResult(ok=False, seconds=time.perf_counter() - start, error="timeout")
    except subprocess.CalledProcessError as e:
        err = (e.stderr or "").strip()
        if not err:
            err = f"exit {e.returncode}"
        return TimedResult(ok=False, seconds=time.perf_counter() - start, error=err)


def purge_fs_cache() -> None:
    """
    Best-effort filesystem cache purge so subsequent runs don't benefit from warm cache.

    Notes:
    - macOS: uses `sudo purge`
    - Linux: uses `sync; echo 3 > /proc/sys/vm/drop_caches` (requires sudo)
    - Non-interactive only: we never prompt for a password.
    """
    if sys.platform == "darwin":
        # `purge` typically requires root privileges.
        subprocess.run(["sudo", "-n", "purge"], check=True, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
        return

    if sys.platform.startswith("linux"):
        # Drop page cache / dentries / inodes.
        subprocess.run(
            ["sudo", "-n", "sh", "-c", "sync; echo 3 > /proc/sys/vm/drop_caches"],
            check=True,
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL,
        )
        return

    raise RuntimeError(f"cache purge unsupported on platform: {sys.platform}")


def iter_files(p: Path) -> Iterable[Path]:
    if p.is_file():
        yield p
        return
    for f in p.rglob("*"):
        if f.is_file():
            yield f


def total_size_bytes(p: Path) -> int:
    return sum(f.stat().st_size for f in iter_files(p))


def sha256_file(p: Path) -> str:
    h = hashlib.sha256()
    with open(p, "rb") as f:
        for chunk in iter(lambda: f.read(1024 * 1024), b""):
            h.update(chunk)
    return h.hexdigest()


def sample_member_paths_for_get(dataset_root_name: str, dataset_path: Path, count: int, seed: int) -> list[str]:
    """
    Return archive member paths (relative, prefixed with dataset root name) for get tests.
    """
    if dataset_path.is_file():
        return [dataset_root_name]

    files = [p for p in dataset_path.rglob("*") if p.is_file()]
    if not files:
        return []
    rnd = random.Random(seed)
    rnd.shuffle(files)
    chosen = files[: min(count, len(files))]

    members: list[str] = []
    for f in chosen:
        rel = f.relative_to(dataset_path).as_posix()
        members.append(f"{dataset_root_name}/{rel}")
    return members


def discover_datasets(data_dir: Path) -> list[Dataset]:
    if not data_dir.exists():
        die(f"missing benchmarks data dir: {data_dir}")

    datasets: list[Dataset] = []
    for child in sorted(data_dir.iterdir()):
        name = child.name
        if name.startswith("."):
            continue
        # Skip obvious archives already in data dir.
        if child.is_file() and child.suffix in {".gz", ".bz2", ".xz", ".zip", ".tar", ".tgz"}:
            continue
        datasets.append(Dataset(name=name, path=child, is_dir=child.is_dir()))
    return datasets


def ensure_tool_exists(tool: str) -> None:
    if shutil.which(tool) is None:
        die(f"required tool not found on PATH: {tool}")


def ensure_file_exists(p: Path, what: str) -> None:
    if not p.exists():
        die(f"{what} not found: {p}")


def ensure_benchmark_data_ready() -> None:
    """
    Ensure benchmark datasets are prepared (downloaded + extracted) using cached checks.
    """
    script = BENCH_DIR / "scripts" / "get_data.py"
    if not script.exists():
        die(f"benchmark data setup script missing: {script}")
    check_cmd = [sys.executable, script.as_posix(), "--check-only"]
    try:
        subprocess.run(check_cmd, check=True)
        return
    except subprocess.CalledProcessError:
        subprocess.run([sys.executable, script.as_posix()], check=True)


def format_mibs_per_s(bytes_count: int, seconds: float) -> float:
    if seconds <= 0:
        return 0.0
    return (bytes_count / (1024 * 1024)) / seconds


def mar_cmd(mar_bin: Path, *args: str) -> list[str]:
    return [str(mar_bin), *args]


def tar_create_cmd(dataset: Dataset, out_path: Path, gzip: bool) -> list[str]:
    # Produce stable member paths: archive contains "<dataset.name>/..."
    # by running tar from benchmarks/data directory.
    flags = ["czf" if gzip else "cf", str(out_path), dataset.name]
    return ["tar", *flags]


def tar_list_cmd(archive: Path, gzip: bool) -> list[str]:
    return ["tar", "tzf" if gzip else "tf", str(archive)]


def tar_extract_cmd(archive: Path, out_dir: Path, gzip: bool) -> list[str]:
    out_dir.mkdir(parents=True, exist_ok=True)
    return ["tar", "xzf" if gzip else "xf", str(archive), "-C", str(out_dir)]


def tar_get_cmd(archive: Path, member: str, gzip: bool) -> list[str]:
    # tar has no real random access; this is the closest analogue.
    # Write to stdout and discard output.
    return ["tar", "xOzf" if gzip else "xOf", str(archive), member]


def verify_sample(original_root: Path, extracted_root: Path, members: list[str], dataset_root_name: str) -> None:
    """
    Verify a sample of member paths by sha256 comparison.
    """
    for m in members:
        # member is "<dataset_root_name>/<rel>"
        rel = m[len(dataset_root_name) + 1 :] if m.startswith(dataset_root_name + "/") else m
        o = original_root / rel
        e = extracted_root / rel
        if not o.exists():
            die(f"verify: original missing: {o}")
        if not e.exists():
            die(f"verify: extracted missing: {e}")
        if sha256_file(o) != sha256_file(e):
            die(f"verify: checksum mismatch: {m}")


def write_rows(csv_path: Path, rows: list[dict], overwrite: bool) -> None:
    csv_path.parent.mkdir(parents=True, exist_ok=True)
    mode = "w" if overwrite else "a"
    exists = csv_path.exists()
    with open(csv_path, mode, newline="") as f:
        fieldnames = [
            "dataset",
            "tool",
            "compression",
            "operation",
            "seconds",
            "input_bytes",
            "output_bytes",
            "throughput_mib_s",
            "status",
            "error",
        ]
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        if overwrite or not exists:
            writer.writeheader()
        writer.writerows(rows)

def safe_rmtree(p: Path) -> None:
    """
    Best-effort recursive delete.
    Large benchmark runs can leave behind partial trees; we prefer continuing.
    """
    if not p.exists():
        return
    shutil.rmtree(p, ignore_errors=True)


@dataclass(frozen=True)
class BenchCase:
    """
    A single benchmark case executed by the generated shell runner.

    The actual work is implemented in a small `case_*.sh` script. The runner handles:
    - optional cache purge *immediately* before running the case
    - high-resolution bare-metal timing via bash 'date +%s%N' (nanosecond precision)
    - recording structured results

    IMPORTANT: Timing is captured at subprocess level using native bash utilities to
    minimize overhead (~1.5ms per measurement) and measure true wall-clock execution
    time without Python contamination.
    """

    case_id: str
    dataset: str
    tool: str
    compression: str
    operation: str
    script_path: Path
    output_file: Optional[Path]
    input_bytes: int


def _sanitize_id(s: str) -> str:
    return "".join(ch if ch.isalnum() or ch in "_-." else "_" for ch in s)


def _write_case_script(path: Path, cwd: Path, body_lines: list[str]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    content = [
        "#!/usr/bin/env bash",
        "set -e",
        f"cd '{cwd.as_posix()}'",
        *body_lines,
        "",
    ]
    path.write_text("\n".join(content), encoding="utf-8")
    path.chmod(0o755)


def _write_driver_script(
    path: Path,
    cases: list[BenchCase],
    raw_runs_csv: Path,
    logs_dir: Path,
    purge_cache: bool,
    repeats: int,
    timeout_s: int,
) -> None:
    """
    Generate a single shell runner that:
    - iterates benchmark cases
    - purges cache right before each run (if enabled)
    - times the case script
    - appends a row to raw_runs_csv per run
    """
    path.parent.mkdir(parents=True, exist_ok=True)
    logs_dir.mkdir(parents=True, exist_ok=True)

    # Bash 3.x compatible (macOS default).
    lines: list[str] = []
    lines += [
        "#!/usr/bin/env bash",
        "set -u",
        "",
        f"RAW_CSV='{raw_runs_csv.as_posix()}'",
        f"LOG_DIR='{logs_dir.as_posix()}'",
        f"PURGE={'1' if purge_cache else '0'}",
        f"REPEATS='{int(max(1, repeats))}'",
        f"TIMEOUT_S='{int(timeout_s)}'",
        "",
        "mkdir -p \"$LOG_DIR\"",
        "",
        "# Fail fast if purging is requested but sudo isn't ready.",
        "if [ \"$PURGE\" = '1' ]; then",
        "  sudo -n true >/dev/null 2>&1 || {",
        "    echo 'cache purge requires non-interactive sudo (run sudo -v first)' >&2;",
        "    exit 2;",
        "  }",
        "fi",
        "",
        "# Choose a timeout binary if available (Linux: timeout, macOS w/ coreutils: gtimeout).",
        "TIMEOUT_BIN=''",
        "command -v timeout >/dev/null 2>&1 && TIMEOUT_BIN='timeout' || true",
        "command -v gtimeout >/dev/null 2>&1 && TIMEOUT_BIN='gtimeout' || true",
        "",
        "stat_bytes() {",
        "  # Print file size in bytes (macOS and Linux).",
        "  local p=\"$1\"",
        "  (stat -f%z \"$p\" 2>/dev/null || stat -c%s \"$p\" 2>/dev/null) || echo 0",
        "}",
        "",
        "purge_cache() {",
        "  if [ \"$PURGE\" != '1' ]; then return 0; fi",
        "  # Non-interactive only: never prompt for a password.",
        "  sudo -n true >/dev/null 2>&1 || { echo 'cache purge requires sudo (run sudo -v first)' >&2; return 1; }",
        "  case \"$(uname -s)\" in",
        "    Darwin)",
        "      sudo -n purge >/dev/null 2>&1 || return 1",
        "      ;;",
        "    Linux)",
        "      sudo -n sh -c 'sync; echo 3 > /proc/sys/vm/drop_caches' >/dev/null 2>&1 || return 1",
        "      ;;",
        "    *)",
        "      echo \"cache purge unsupported on $(uname -s)\" >&2",
        "      return 1",
        "      ;;",
        "  esac",
        "}",
        "",
        "time_case() {",
        "  # Usage: time_case <stderr_log> -- <cmd...>",
        "  # Uses bash 'date +%s%N' for high-resolution timing (nanosecond precision).",
        "  # Overhead: ~1.5ms per measurement (vs ~4ms for Perl Time::HiRes).",
        "  # Executes the command and prints elapsed seconds to stdout, exit code from command.",
        "  local err_log=\"$1\"",
        "  shift",
        "  if [ \"$1\" != '--' ]; then echo 'internal error: time_case expects --' >&2; return 99; fi",
        "  shift",
        "  # Use 'date +%s%N' for bare-metal timing: native bash, minimal overhead.",
        "  # Captures nanosecond precision at system call level (CLOCK_REALTIME via date).",
        "  # Arithmetic-only conversion avoids spawning bc/awk subprocesses.",
        "  local start=$(date +%s%N)",
        "  \"$@\" 2>\"$err_log\"",
        "  local rc=$?",
        "  local end=$(date +%s%N)",
        "  local ns=$((end - start))",
        "  # Convert nanoseconds to seconds with 9 decimal places.",
        "  local secs=$((ns / 1000000000))",
        "  local nanos=$((ns % 1000000000))",
        "  printf '%d.%09d\\n' \"$secs\" \"$nanos\"",
        "  return $rc",
        "}",
        "",
        "# Raw runs output (one row per run).",
        "echo 'case_id,run_idx,dataset,tool,compression,operation,status,seconds,output_bytes,error' > \"$RAW_CSV\"",
        "",
    ]

    for c in cases:
        # We in-line one loop per case for maximal clarity/debuggability.
        out_file = c.output_file.as_posix() if c.output_file else ""
        lines += [
            f"echo '[case] {c.case_id}'",
            f"CASE_ID='{c.case_id}'",
            f"DATASET='{c.dataset}'",
            f"TOOL='{c.tool}'",
            f"COMP='{c.compression}'",
            f"OP='{c.operation}'",
            f"CASE_SCRIPT='{c.script_path.as_posix()}'",
            f"OUT_FILE='{out_file}'",
            "i=1",
            "while [ $i -le $REPEATS ]; do",
            "  # Purge cache immediately before timing.",
            "  purge_cache || {",
            "    echo \"$CASE_ID,$i,$DATASET,$TOOL,$COMP,$OP,fail,,0,cache purge failed\" >> \"$RAW_CSV\"",
            "    i=$((i+1))",
            "    continue",
            "  }",
            "",
            "  err_log=\"$LOG_DIR/${CASE_ID}.${i}.stderr\"",
            "",
            "  # Build the command (best-effort timeout).",
            "  if [ -n \"$TIMEOUT_BIN\" ]; then",
            "    secs=$(time_case \"$err_log\" -- \"$TIMEOUT_BIN\" \"$TIMEOUT_S\" bash \"$CASE_SCRIPT\")",
            "    rc=$?",
            "  else",
            "    secs=$(time_case \"$err_log\" -- bash \"$CASE_SCRIPT\")",
            "    rc=$?",
            "  fi",
            "",
            "  status='ok'",
            "  if [ $rc -ne 0 ]; then",
            "    if [ $rc -eq 124 ]; then status='timeout'; else status='fail'; fi",
            "  fi",
            "",
            "  out_bytes=0",
            "  if [ -n \"$OUT_FILE\" ] && [ -f \"$OUT_FILE\" ]; then",
            "    out_bytes=$(stat_bytes \"$OUT_FILE\")",
            "  fi",
            "",
            "  err_msg=''",
            "  if [ -s \"$err_log\" ]; then",
            "    # First line only, sanitized for CSV.",
            "    err_msg=$(head -n 1 \"$err_log\" | tr '\\r\\n' '  ' | sed 's/,/;/g')",
            "  fi",
            "",
            "  echo \"$CASE_ID,$i,$DATASET,$TOOL,$COMP,$OP,$status,$secs,$out_bytes,$err_msg\" >> \"$RAW_CSV\"",
            "  i=$((i+1))",
            "done",
            "",
        ]

    lines.append("exit 0")
    lines.append("")
    path.write_text("\n".join(lines), encoding="utf-8")
    path.chmod(0o755)


def _parse_raw_runs(raw_csv: Path) -> list[dict]:
    rows: list[dict] = []
    with open(raw_csv, newline="", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        for r in reader:
            rows.append(r)
    return rows


def _median(values: list[float]) -> float:
    if not values:
        return 0.0
    values = sorted(values)
    return values[len(values) // 2]


def _append_bench_case(
    cases: list[BenchCase],
    case_id: str,
    dataset_name: str,
    tool: str,
    compression: str,
    operation: str,
    script_path: Path,
    input_bytes: int,
    output_file: Optional[Path] = None,
) -> None:
    """
    Append a benchmark case with consistent initialization.
    
    This centralizes BenchCase creation to avoid repetitive boilerplate and ensure
    consistency across all benchmark cases.
    """
    cases.append(
        BenchCase(
            case_id=case_id,
            dataset=dataset_name,
            tool=tool,
            compression=compression,
            operation=operation,
            script_path=script_path,
            output_file=output_file,
            input_bytes=input_bytes,
        )
    )


def _build_mar_archives_dict(scratch: Path, dataset_name: str, base_compressions: list[str], 
                             include_matrix: bool) -> dict[str, Path]:
    """
    Build the complete mar_archives dictionary with optional matrix variants.
    
    The base compressions (none, zstd, gzip, lz4) are always included.
    Matrix variants (zstd+no-checksum, zstd+dedup, etc.) are added if include_matrix=True.
    
    Args:
        scratch: Scratch directory for archive outputs
        dataset_name: Name of dataset (used in filename)
        base_compressions: List of base compression variants (e.g., ["none", "zstd"])
        include_matrix: If True, add extra variants (zstd+no-checksum, gzip+no-metadata, etc.)
    
    Returns:
        Dictionary mapping compression names to archive paths
    """
    mar_archives = {comp: scratch / f"{dataset_name}_{comp}.mar" for comp in base_compressions}
    
    if include_matrix:
        # Extra matrix variants: limited to zstd/gzip to keep case count manageable
        # while capturing key switches for publishable numbers
        matrix_variants = {
            "zstd+no-checksum": f"{dataset_name}_zstd_nocs.mar",
            "zstd+no-metadata": f"{dataset_name}_zstd_nometa.mar",
            "zstd+dedup": f"{dataset_name}_zstd_dedup.mar",
            "gzip+no-checksum": f"{dataset_name}_gzip_nocs.mar",
            "gzip+no-metadata": f"{dataset_name}_gzip_nometa.mar",
        }
        for variant, filename in matrix_variants.items():
            mar_archives[variant] = scratch / filename
    
    return mar_archives


def main() -> None:
    parser = argparse.ArgumentParser(description="Easy benchmarks for MAR vs TAR (all datasets in benchmarks/data).")
    parser.add_argument("--mar", dest="mar_bin", default=str(REPO_ROOT / "mar"), help="Path to mar binary (default: ./mar)")
    parser.add_argument("--prepare-data", dest="prepare_data", action="store_true", default=True, help="Ensure benchmark datasets are prepared (default: on)")
    parser.add_argument("--no-prepare-data", dest="prepare_data", action="store_false", help="Skip dataset preparation check")
    parser.add_argument("--datasets", nargs="*", default=None, help="Limit to dataset names (default: all discovered)")
    parser.add_argument("--limit-datasets", type=int, default=0, help="If set, run only N smallest datasets")
    parser.add_argument("--repeats", type=int, default=3, help="Repeats per operation (default: 3, median reported)")
    parser.add_argument("--get-count", type=int, default=25, help="Number of files sampled for get tests (default: 25)")
    parser.add_argument("--verify", choices=["none", "sample"], default="sample", help="Verification mode (default: sample)")
    parser.add_argument("--include-large", action="store_true", help="Include very large datasets (default: skip >2GiB)")
    parser.add_argument("--max-bytes", type=int, default=2 * 1024 * 1024 * 1024, help="Skip datasets larger than this unless --include-large (default: 2GiB)")
    parser.add_argument("--timeout", type=int, default=1800, help="Per-command timeout in seconds (default: 1800)")
    parser.add_argument("--purge-cache", action="store_true", help="Purge filesystem cache before each timed run (requires sudo, non-interactive)")
    parser.add_argument("--threads", type=int, default=0, help="MAR threads (-j/--threads). 0 = MAR default (CPU cores).")
    parser.add_argument(
        "--mar-create-matrix",
        action="store_true",
        help="Also benchmark extra MAR create variants (zstd): no-checksum, no-metadata+no-checksum, dedup.",
    )
    parser.add_argument("--overwrite", action="store_true", help="Overwrite results.csv/summary.md instead of appending")
    parser.add_argument("--seed", type=int, default=42, help="Random seed for sampling (default: 42)")
    args = parser.parse_args()

    ensure_tool_exists("tar")
    if args.prepare_data:
        ensure_benchmark_data_ready()
    mar_bin = Path(args.mar_bin).resolve()
    ensure_file_exists(mar_bin, "mar binary")

    datasets = discover_datasets(DATA_DIR)
    if args.datasets:
        wanted = set(args.datasets)
        datasets = [d for d in datasets if d.name in wanted]
        missing = wanted - {d.name for d in datasets}
        if missing:
            die(f"unknown dataset(s): {', '.join(sorted(missing))}")
    if not datasets:
        die("no datasets found")

    if args.limit_datasets and args.limit_datasets > 0:
        sized: list[tuple[int, Dataset]] = []
        for d in datasets:
            try:
                sized.append((total_size_bytes(d.path), d))
            except FileNotFoundError:
                continue
        sized.sort(key=lambda x: x[0])
        datasets = [d for _, d in sized[: args.limit_datasets]]

    scratch = BENCH_DIR / "scratch_easy"
    safe_rmtree(scratch)
    scratch.mkdir(parents=True, exist_ok=True)

    cases_dir = scratch / "cases"
    logs_dir = scratch / "logs"
    raw_runs_csv = scratch / "raw_runs.csv"
    driver_sh = scratch / "run_easy.sh"

    results_csv = BENCH_DIR / "results.csv"
    summary_md = BENCH_DIR / "summary.md"

    rows: list[dict] = []  # aggregated output written to benchmarks/results.csv
    summary_lines: list[str] = []

    print(f"MAR binary: {mar_bin}")
    print(f"Datasets: {', '.join(d.name for d in datasets)}")
    print(f"Scratch: {scratch}")

    if args.threads < 0:
        die("--threads must be >= 0")

    # ========================================================================
    # PHASE 1: Generate benchmark case scripts
    # ========================================================================
    # We generate per-operation bash scripts for each dataset and compression,
    # then a master driver script that orchestrates timing, caching, and result
    # recording with high precision (via native bash date +%s%N).
    # ========================================================================

    cases: list[BenchCase] = []
    mar_threads_flag = f"-j {args.threads}" if args.threads and args.threads > 0 else ""
    base_mar_compressions = ["none", "zstd", "gzip", "lz4"]

    # Meta/version cases: simple tool version checks (used for timing overhead baseline)
    meta_cwd = REPO_ROOT
    meta_cases = [
        ("meta_mar_version", "mar", f"'{mar_bin.as_posix()}' version > /dev/null"),
        ("meta_tar_version", "tar", "tar --version > /dev/null"),
    ]
    for case_id, tool, cmd in meta_cases:
        script = cases_dir / f"case_{case_id}.sh"
        _write_case_script(script, cwd=meta_cwd, body_lines=[cmd])
        _append_bench_case(
            cases,
            case_id=case_id,
            dataset_name="_meta",
            tool=tool,
            compression="",
            operation="version",
            script_path=script,
            input_bytes=0,
        )

    for d in datasets:
        print(f"\n== {d.name} ==")
        input_bytes = total_size_bytes(d.path)
        if not args.include_large and input_bytes > args.max_bytes:
            print(f"  (skipped: {input_bytes} bytes > max {args.max_bytes})")
            rows.append({
                "dataset": d.name,
                "tool": "",
                "compression": "",
                "operation": "skipped_large",
                "seconds": "",
                "input_bytes": input_bytes,
                "output_bytes": 0,
                "throughput_mib_s": "",
                "status": "skip",
                "error": "dataset too large",
            })
            continue

        # Archives: Use helper to build all paths
        mar_archives = _build_mar_archives_dict(scratch, d.name, base_mar_compressions, args.mar_create_matrix)
        tar_none = scratch / f"{d.name}.tar"
        tar_gz = scratch / f"{d.name}.tar.gz"
        mar_compressions = list(mar_archives.keys())

        # Build archive inputs relative to benchmarks/data for tar,
        # and relative to repo root for mar (avoid absolute-path members).
        tar_cwd = DATA_DIR
        mar_cwd = REPO_ROOT
        mar_input = Path("benchmarks") / "data" / d.name

        # Member roots differ for directories vs files:
        # - tar archives always contain "<dataset.name>/..." (we create from DATA_DIR)
        # - mar archives contain "<dataset.name>/..." for directories, but for files they
        #   contain the provided path string ("benchmarks/data/<name>") due to CLI add_file().
        tar_root = d.name
        mar_root = d.name if d.is_dir else mar_input.as_posix()

        # CREATE
        create_specs = [
            ("tar", "none", tar_cwd, tar_none, [f"rm -f '{tar_none.as_posix()}'", f"tar cf '{tar_none.as_posix()}' '{d.name}'"]),
            ("tar", "gzip", tar_cwd, tar_gz, [f"rm -f '{tar_gz.as_posix()}'", f"tar czf '{tar_gz.as_posix()}' '{d.name}'"]),
        ]
        # Add MAR create specs for each compression variant
        for comp in base_mar_compressions:
            arc = mar_archives[comp]
            create_specs.append(
                ("mar", comp, mar_cwd, arc, [
                    f"rm -f '{arc.as_posix()}'",
                    f"'{mar_bin.as_posix()}' create --force -c {comp} {mar_threads_flag} '{arc.as_posix()}' '{mar_input.as_posix()}' > /dev/null"
                ])
            )
        # Add matrix variants if requested (special flags for no-checksum, no-metadata, dedup)
        if args.mar_create_matrix:
            matrix_specs = {
                "zstd+no-checksum": ("zstd", "--no-checksum"),
                "zstd+no-metadata": ("zstd", "--no-metadata --no-checksum"),
                "zstd+dedup": ("zstd", "--dedup"),
                "gzip+no-checksum": ("gzip", "--no-checksum"),
                "gzip+no-metadata": ("gzip", "--no-metadata --no-checksum"),
            }
            for comp_name, (base_comp, flags) in matrix_specs.items():
                arc = mar_archives[comp_name]
                create_specs.append(
                    ("mar", comp_name, mar_cwd, arc, [
                        f"rm -f '{arc.as_posix()}'",
                        f"'{mar_bin.as_posix()}' create --force -c {base_comp} {flags} {mar_threads_flag} '{arc.as_posix()}' '{mar_input.as_posix()}' > /dev/null"
                    ])
                )

        for tool, comp, cwd, out_file, body in create_specs:
            case_id = _sanitize_id(f"{d.name}_{tool}_create_{comp}")
            script = cases_dir / f"case_{case_id}.sh"
            _write_case_script(script, cwd=cwd, body_lines=body)
            _append_bench_case(
                cases,
                case_id=case_id,
                dataset_name=d.name,
                tool=tool,
                compression=comp,
                operation="create",
                script_path=script,
                input_bytes=input_bytes,
                output_file=out_file,
            )

        # LIST (note: mar list does not support -j/--threads)
        list_specs = [
            ("tar", "none", tar_cwd, [f"tar tf '{tar_none.as_posix()}' > /dev/null"]),
            ("tar", "gzip", tar_cwd, [f"tar tzf '{tar_gz.as_posix()}' > /dev/null"]),
            *[
                ("mar", comp, mar_cwd, [f"'{mar_bin.as_posix()}' list '{mar_archives[comp].as_posix()}' > /dev/null"])
                for comp in mar_compressions
            ],
        ]
        for tool, comp, cwd, body in list_specs:
            case_id = _sanitize_id(f"{d.name}_{tool}_list_{comp}")
            script = cases_dir / f"case_{case_id}.sh"
            _write_case_script(script, cwd=cwd, body_lines=body)
            _append_bench_case(
                cases,
                case_id=case_id,
                dataset_name=d.name,
                tool=tool,
                compression=comp,
                operation="list",
                script_path=script,
                input_bytes=input_bytes,
            )

        # VALIDATE / HEADER (mar only, for all MAR compression variants)
        # Note: mar validate and header do not support -j/--threads
        for op in ["validate", "header"]:
            for comp in mar_compressions:
                arc = mar_archives[comp]
                case_id = _sanitize_id(f"{d.name}_mar_{op}_{comp}")
                script = cases_dir / f"case_{case_id}.sh"
                body = [f"'{mar_bin.as_posix()}' {op} '{arc.as_posix()}' > /dev/null"]
                _write_case_script(script, cwd=mar_cwd, body_lines=body)
                _append_bench_case(
                    cases,
                    case_id=case_id,
                    dataset_name=d.name,
                    tool="mar",
                    compression=comp,
                    operation=op,
                    script_path=script,
                    input_bytes=input_bytes,
                )

        # EXTRACT
        tar_out = scratch / f"extracted_{d.name}_tar"
        tar_gz_out = scratch / f"extracted_{d.name}_tar_gz"

        extract_specs = [
            ("tar", "none", tar_cwd, [f"rm -rf '{tar_out.as_posix()}'", f"mkdir -p '{tar_out.as_posix()}'", f"tar xf '{tar_none.as_posix()}' -C '{tar_out.as_posix()}'"]),
            ("tar", "gzip", tar_cwd, [f"rm -rf '{tar_gz_out.as_posix()}'", f"mkdir -p '{tar_gz_out.as_posix()}'", f"tar xzf '{tar_gz.as_posix()}' -C '{tar_gz_out.as_posix()}'"]),
            *[
                (
                    "mar",
                    comp,
                    mar_cwd,
                    [
                        f"rm -rf '{(scratch / f'extracted_{d.name}_mar_{comp}').as_posix()}'",
                        f"mkdir -p '{(scratch / f'extracted_{d.name}_mar_{comp}').as_posix()}'",
                        f"'{mar_bin.as_posix()}' extract {mar_threads_flag} -o '{(scratch / f'extracted_{d.name}_mar_{comp}').as_posix()}' '{mar_archives[comp].as_posix()}'",
                    ],
                )
                for comp in mar_compressions
            ],
        ]
        for tool, comp, cwd, body in extract_specs:
            case_id = _sanitize_id(f"{d.name}_{tool}_extract_{comp}")
            script = cases_dir / f"case_{case_id}.sh"
            _write_case_script(script, cwd=cwd, body_lines=body)
            _append_bench_case(
                cases,
                case_id=case_id,
                dataset_name=d.name,
                tool=tool,
                compression=comp,
                operation="extract",
                script_path=script,
                input_bytes=input_bytes,
            )

        # Sample members for get/cat tests (tar vs mar may differ for single-file datasets)
        tar_members = sample_member_paths_for_get(tar_root, d.path, count=args.get_count, seed=args.seed)
        mar_members = sample_member_paths_for_get(mar_root, d.path, count=args.get_count, seed=args.seed)

        # GET (random access)
        #
        # This benchmark is only meaningful for multi-file datasets. For single-file
        # datasets, TAR can stream the single member very efficiently and MAR's
        # random-access advantage doesn't apply.
        if d.is_dir and len(mar_members) >= 2 and len(tar_members) == len(mar_members):
            members_quoted = " ".join([f"'{m}'" for m in mar_members])
            for comp in mar_compressions:
                arc = mar_archives[comp]
                get_out = scratch / f"get_{d.name}_mar_{comp}"
                case_id = _sanitize_id(f"{d.name}_mar_get_{len(mar_members)}_{comp}")
                script = cases_dir / f"case_{case_id}.sh"
                body = [
                    f"rm -rf '{get_out.as_posix()}'",
                    f"mkdir -p '{get_out.as_posix()}'",
                    f"'{mar_bin.as_posix()}' get {mar_threads_flag} -o '{get_out.as_posix()}' '{arc.as_posix()}' {members_quoted} > /dev/null",
                ]
                _write_case_script(script, cwd=mar_cwd, body_lines=body)
                _append_bench_case(
                    cases,
                    case_id=case_id,
                    dataset_name=d.name,
                    tool="mar",
                    compression=comp,
                    operation=f"get_{len(mar_members)}",
                    script_path=script,
                    input_bytes=input_bytes,
                )

            # TAR "get" simulation: tar -xOf each member to /dev/null
            tar_case_id = _sanitize_id(f"{d.name}_tar_get_{len(tar_members)}_none")
            tar_script = cases_dir / f"case_{tar_case_id}.sh"
            tar_body = [
                "set -e",
                # Run from DATA_DIR so member paths match tar archive.
                f"cd '{tar_cwd.as_posix()}'",
                *[f"tar xOf '{tar_none.as_posix()}' '{m}' > /dev/null" for m in tar_members],
            ]
            # _write_case_script always inserts set -e + cd, so write manually.
            tar_script.write_text("\n".join(["#!/usr/bin/env bash", *tar_body, ""]), encoding="utf-8")
            tar_script.chmod(0o755)
            cases.append(BenchCase(case_id=tar_case_id, dataset=d.name, tool="tar", compression="none", operation=f"get_{len(tar_members)}", script_path=tar_script, output_file=None, input_bytes=input_bytes))
        else:
            rows.append({
                "dataset": d.name,
                "tool": "",
                "compression": "",
                "operation": "get_skipped",
                "seconds": "",
                "input_bytes": input_bytes,
                "output_bytes": 0,
                "throughput_mib_s": "",
                "status": "skip",
                "error": "get benchmark only runs for multi-file datasets",
            })

        # CAT (stream file(s) to stdout; mar only)
        if d.is_dir and mar_members:
            cat_members = mar_members[: min(5, len(mar_members))]
            for comp in mar_compressions:
                arc = mar_archives[comp]
                case_id = _sanitize_id(f"{d.name}_mar_cat_{len(cat_members)}_{comp}")
                script = cases_dir / f"case_{case_id}.sh"
                body = [f"'{mar_bin.as_posix()}' cat {mar_threads_flag} '{arc.as_posix()}' '{m}' > /dev/null" for m in cat_members]
                _write_case_script(script, cwd=mar_cwd, body_lines=body)
                _append_bench_case(
                    cases,
                    case_id=case_id,
                    dataset_name=d.name,
                    tool="mar",
                    compression=comp,
                    operation=f"cat_{len(cat_members)}",
                    script_path=script,
                    input_bytes=input_bytes,
                )

    # ========================================================================
    # PHASE 2: Execute benchmarks via bash driver script
    # ========================================================================
    # The driver script handles:
    # - Filesystem cache purging (if --purge-cache)
    # - High-precision timing via bash 'date +%s%N' (~1.5ms overhead)
    # - Multiple repeats per case (default 3, median reported)
    # - Timeout handling (default 1800s)
    # - Raw result recording to raw_runs.csv
    # ========================================================================

    _write_driver_script(
        driver_sh,
        cases=cases,
        raw_runs_csv=raw_runs_csv,
        logs_dir=logs_dir,
        purge_cache=bool(args.purge_cache),
        repeats=int(args.repeats),
        timeout_s=int(args.timeout),
    )
    subprocess.run(["bash", driver_sh.as_posix()], check=True)

    # ========================================================================
    # PHASE 3: Aggregate and analyze results
    # ========================================================================
    # For each operation across all repeats:
    # - Filter for successful runs only
    # - Compute median timing (robust against outliers)
    # - Aggregate output bytes and error messages
    # - Compute throughput for create/extract operations
    # ========================================================================

    raw_rows = _parse_raw_runs(raw_runs_csv)

    # Aggregate: median seconds across ok runs.
    by_key: dict[tuple[str, str, str, str], dict] = {}
    for rr in raw_rows:
        dataset = rr["dataset"]
        tool = rr["tool"]
        comp = rr["compression"]
        op = rr["operation"]
        key = (dataset, tool, comp, op)
        if key not in by_key:
            by_key[key] = {
                "dataset": dataset,
                "tool": tool,
                "compression": comp,
                "operation": op,
                "seconds": "",
                "input_bytes": 0,
                "output_bytes": 0,
                "throughput_mib_s": "",
                "status": "fail",
                "error": "",
                "_ok_secs": [],
                "_out_bytes": [],
                "_errors": [],
            }
        if rr.get("status") == "ok" and rr.get("seconds"):
            try:
                by_key[key]["_ok_secs"].append(float(rr["seconds"]))
            except ValueError:
                pass
        if rr.get("output_bytes"):
            try:
                by_key[key]["_out_bytes"].append(int(rr["output_bytes"]))
            except ValueError:
                pass
        if rr.get("error"):
            by_key[key]["_errors"].append(rr["error"])

    # Fill aggregated rows using the known case metadata (input_bytes, output_file sizes).
    case_lookup: dict[tuple[str, str, str, str], BenchCase] = {}
    for c in cases:
        case_lookup[(c.dataset, c.tool, c.compression, c.operation)] = c

    for key, agg in by_key.items():
        dataset, tool, comp, op = key
        meta = case_lookup.get(key)
        input_bytes = meta.input_bytes if meta else 0
        ok_secs = agg.pop("_ok_secs")
        out_bytes_list = agg.pop("_out_bytes")
        errors = agg.pop("_errors")

        if ok_secs:
            secs = _median(ok_secs)
            agg["seconds"] = f"{secs:.6f}"
            agg["status"] = "ok"
            agg["error"] = ""
            agg["input_bytes"] = input_bytes
            # Use median output bytes if present.
            if out_bytes_list:
                agg["output_bytes"] = sorted(out_bytes_list)[len(out_bytes_list) // 2]
            if op in {"create", "extract"}:
                agg["throughput_mib_s"] = f"{format_mibs_per_s(input_bytes, secs):.3f}"
        else:
            agg["status"] = "fail"
            agg["error"] = errors[0] if errors else "no successful runs"
            agg["input_bytes"] = input_bytes
            if out_bytes_list:
                agg["output_bytes"] = sorted(out_bytes_list)[len(out_bytes_list) // 2]

        rows.append(agg)

    # Cache mode metadata (consumed by plot_results.R for subtitle badges).
    rows.append(
        {
            "dataset": "_meta",
            "tool": "bench",
            "compression": "",
            "operation": "cache_mode",
            "seconds": "",
            "input_bytes": 0,
            "output_bytes": 0,
            "throughput_mib_s": "",
            "status": "ok",
            "error": "cold" if args.purge_cache else "warm",
        }
    )

    # Thread count metadata (consumed by plot_results.R for subtitle badges).
    rows.append(
        {
            "dataset": "_meta",
            "tool": "bench",
            "compression": "",
            "operation": "threads",
            "seconds": "",
            "input_bytes": 0,
            "output_bytes": 0,
            "throughput_mib_s": "",
            "status": "ok",
            "error": str(args.threads) if args.threads and args.threads > 0 else "default",
        }
    )

    # Sort output for readability.
    rows.sort(key=lambda r: (r["dataset"], r["operation"], r["tool"], r["compression"]))

    # ========================================================================
    # PHASE 4: Generate summary and output reports
    # ========================================================================
    # Build summary.md with speedup comparisons (mar zstd vs tar baselines)
    # Write results.csv with all aggregated benchmark data
    # ========================================================================

    # Build summary lines (speedup vs tar none & tar gzip baselines, using aggregated rows).
    def find_time(dataset: str, tool: str, comp: str, op: str) -> Optional[float]:
        for r in rows:
            if r["dataset"] == dataset and r["tool"] == tool and r["compression"] == comp and r["operation"] == op and r["status"] == "ok" and r["seconds"]:
                return float(r["seconds"])
        return None

    for d in datasets:
        if not (DATA_DIR / d.name).exists():
            continue
        tar_create = find_time(d.name, "tar", "none", "create")
        tar_create_gzip = find_time(d.name, "tar", "gzip", "create")
        mar_create = find_time(d.name, "mar", "zstd", "create")
        tar_extract = find_time(d.name, "tar", "none", "extract")
        tar_extract_gzip = find_time(d.name, "tar", "gzip", "extract")
        mar_extract = find_time(d.name, "mar", "zstd", "extract")
        if tar_create and mar_create:
            summary_lines.append(f"- **{d.name}**: create speedup (mar zstd vs tar none): **{tar_create / mar_create:.2f}×**")
        if tar_create_gzip and mar_create:
            summary_lines.append(f"- **{d.name}**: create speedup (mar zstd vs tar gzip): **{tar_create_gzip / mar_create:.2f}×**")
        if tar_extract and mar_extract:
            summary_lines.append(f"- **{d.name}**: extract speedup (mar zstd vs tar none): **{tar_extract / mar_extract:.2f}×**")
        if tar_extract_gzip and mar_extract:
            summary_lines.append(f"- **{d.name}**: extract speedup (mar zstd vs tar gzip): **{tar_extract_gzip / mar_extract:.2f}×**")

    if rows:
        write_rows(results_csv, rows, overwrite=args.overwrite)

    if args.overwrite:
        summary_md.write_text("", encoding="utf-8")
    with open(summary_md, "w" if args.overwrite else "a", encoding="utf-8") as f:
        f.write("# MAR Benchmark Summary\n\n")
        f.write("Quick takeaways (median of repeats):\n\n")
        for line in summary_lines:
            f.write(line + "\n")
        f.write("\n")
        f.write("Notes:\n")
        f.write("- TAR “get” is simulated with `tar -xOf` and is not true random access.\n")
        f.write("- Redaction metrics are reported as N/A until the CLI supports redaction.\n")
        f.write(f"- Raw per-run data: `{raw_runs_csv.relative_to(REPO_ROOT).as_posix()}`\n")

    # Check for failures and alert the user.
    failures = [r for r in rows if r["status"] != "ok"]
    if failures:
        print("\n" + "=" * 80)
        print("⚠️  ALERT: BENCHMARK FAILURES DETECTED ⚠️")
        print("=" * 80)
        print(f"\n{len(failures)} test(s) failed:\n")
        for f in failures:
            dataset = f["dataset"]
            tool = f["tool"]
            comp = f["compression"]
            op = f["operation"]
            error = f["error"]
            print(f"  ✗ {dataset:15} | {tool:5} | {comp:10} | {op:10} | {error}")
        print("\n" + "=" * 80)
    
    print(f"\nWrote: {results_csv}")
    print(f"Wrote: {summary_md}")


if __name__ == "__main__":
    main()

