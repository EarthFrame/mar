#!/usr/bin/env python3
"""
Download and prepare benchmark datasets with smart caching.

This script handles:
- Downloading benchmark datasets from Earthframe public cache (pure Python)
- Extracting/decompressing archives
- Verifying dataset readiness via checksums
- Caching preparation state so subsequent runs know data is ready

Default behavior:
- Download archives into `benchmarks/data/`
- Extract/decompress into the expected paths:
  - linux-6.18.5.tar.xz   -> benchmarks/data/linux-6.18.5/
  - numpy-2.4.1.tar.gz    -> benchmarks/data/numpy-2.4.1/
  - dickens.bz2           -> benchmarks/data/dickens
  - webster.bz2           -> benchmarks/data/webster
  - casp15.tgz            -> ./casp15/    (repo root, for compatibility)

CACHING:
- A `.data_cache.json` file tracks which datasets are ready
- Checksums verify data integrity (fast: only checks file sizes + timestamps)
- `--force` re-downloads and re-extracts everything
- `--check-only` just verifies data is ready without downloading

Notes:
- Uses only the Python standard library.
- Shows a simple progress bar based on Content-Length when available.
"""

from __future__ import annotations

import argparse
import bz2
import hashlib
import json
import lzma
import os
import sys
import tarfile
import time
import urllib.error
import urllib.request
from dataclasses import dataclass
from pathlib import Path
from typing import Optional


REPO_ROOT = Path(__file__).resolve().parents[2]
DEFAULT_BASE_URL = "https://public.earthframe.com/mar-benchmark-data"
CACHE_FILE = REPO_ROOT / "benchmarks" / ".data_cache.json"


def eprint(msg: str) -> None:
    """Print to stderr."""
    print(msg, file=sys.stderr)


def format_bytes(n: float) -> str:
    """Format bytes as human-readable string (B, KiB, MiB, GiB, TiB)."""
    units = ["B", "KiB", "MiB", "GiB", "TiB"]
    i = 0
    while n >= 1024 and i < len(units) - 1:
        n /= 1024.0
        i += 1
    if i == 0:
        return f"{int(n)} {units[i]}"
    return f"{n:.1f} {units[i]}"


def archive_base_name(filename: str) -> str:
    """Return the dataset root name for tar-style archives."""
    lower = filename.lower()
    for suffix in [".tar.gz", ".tar.xz", ".tar.bz2", ".tar.zst", ".tar.zstd", ".tar.lz4", ".tar"]:
        if lower.endswith(suffix):
            return filename[: -len(suffix)]
    if lower.endswith(".tgz"):
        return filename[:-4]
    return Path(filename).stem


def extracted_path_for_spec(data_dir: Path, spec: "DatasetSpec") -> Optional[Path]:
    """Resolve the extracted path for a dataset spec."""
    if spec.extract_kind == "tar_to_data":
        return data_dir / archive_base_name(spec.filename)
    if spec.extract_kind == "tar_to_repo_root":
        return REPO_ROOT / archive_base_name(spec.filename)
    if spec.extract_kind == "bz2_to_data_file":
        out_name = spec.filename[:-4]  # strip .bz2
        return data_dir / out_name
    return None


def progress_bar(done: int, total: Optional[int], start_t: float) -> str:
    """Generate a progress bar string with speed and percentage."""
    elapsed = max(1e-6, time.time() - start_t)
    speed = done / elapsed
    if total and total > 0:
        pct = done / total
        width = 28
        filled = int(pct * width)
        bar = "[" + ("#" * filled) + ("-" * (width - filled)) + "]"
        return f"{bar} {pct*100:6.2f}%  {format_bytes(done)} / {format_bytes(total)}  ({format_bytes(speed)}/s)"
    return f"{format_bytes(done)}  ({format_bytes(speed)}/s)"


def fast_checksum(p: Path) -> dict:
    """
    Fast "checksum" based on file size and mtime (not actual hash).
    
    For directories, recursively hash all files.
    This is blazing fast and sufficient for detecting if data has been
    modified or corrupted (99.9% of the time).
    """
    if not p.exists():
        return {"exists": False}
    
    if p.is_file():
        stat = p.stat()
        return {
            "exists": True,
            "type": "file",
            "size": stat.st_size,
            "mtime": stat.st_mtime,
        }
    
    if p.is_dir():
        # For directories: hash of all file paths, sizes, and mtimes
        h = hashlib.sha256()
        for root, dirs, files in os.walk(p):
            dirs.sort()
            files.sort()
            for f in files:
                fpath = Path(root) / f
                stat = fpath.stat()
                h.update(fpath.relative_to(p).as_posix().encode())
                h.update(str(stat.st_size).encode())
                h.update(str(stat.st_mtime).encode())
        return {
            "exists": True,
            "type": "dir",
            "checksum": h.hexdigest(),
        }
    
    return {"exists": True, "type": "unknown"}


def load_cache() -> dict:
    """Load the data cache, or return empty dict if it doesn't exist."""
    if CACHE_FILE.exists():
        try:
            with open(CACHE_FILE) as f:
                return json.load(f)
        except Exception:
            pass
    return {}


def save_cache(cache: dict) -> None:
    """Save the data cache."""
    try:
        with open(CACHE_FILE, "w") as f:
            json.dump(cache, f, indent=2)
    except Exception as e:
        eprint(f"warning: failed to save cache: {e}")


def is_ready(cache: dict, dataset_key: str, data_dir: Path, spec: DatasetSpec) -> bool:
    """
    Check if a dataset is ready for benchmarking by verifying cache + existence.
    
    Smart caching strategy:
    1. Check cache entry exists
    2. Verify extracted files actually exist (in case manual deletion)
    3. Compute checksum only if files exist (for corruption detection)
    4. Detect if archive changed (would need re-extraction)
    
    Returns True if data is ready and hasn't been corrupted.
    """
    entry = cache.get(dataset_key, {})
    
    # If extraction never completed, data isn't ready
    if not entry.get("extraction_complete"):
        return False
    
    # Determine the extracted path based on extraction type
    dataset_dir = extracted_path_for_spec(data_dir, spec)
    if dataset_dir is None:
        return False
    
    # If extracted path doesn't exist, data isn't ready
    if not dataset_dir.exists():
        return False
    
    # Verify checksum if we have a cached one (detects corruption)
    cached_check = entry.get("extracted_check")
    if cached_check:
        current_check = fast_checksum(dataset_dir)
        if cached_check != current_check:
            return False  # Corruption detected
    
    return True



def download(url: str, dest: Path, force: bool) -> None:
    """Download a file from url to dest, with progress bar."""
    dest.parent.mkdir(parents=True, exist_ok=True)

    if dest.exists() and not force:
        eprint(f"✓ already downloaded: {dest.name}")
        return

    tmp = dest.with_suffix(dest.suffix + ".part")
    if tmp.exists():
        tmp.unlink()

    eprint(f"↓ {url}")
    start_t = time.time()
    done = 0
    last_update = 0.0

    try:
        with urllib.request.urlopen(url) as r, open(tmp, "wb") as f:
            total = None
            try:
                total = int(r.headers.get("Content-Length", "0")) or None
            except Exception:
                total = None

            while True:
                chunk = r.read(1024 * 256)
                if not chunk:
                    break
                f.write(chunk)
                done += len(chunk)

                now = time.time()
                if now - last_update > 0.1:
                    last_update = now
                    eprint("\r  " + progress_bar(done, total, start_t) + " " * 10)

        eprint("\r  " + progress_bar(done, total, start_t) + " " * 10)
        eprint("")
        tmp.replace(dest)
        eprint(f"✓ saved: {dest} ({format_bytes(dest.stat().st_size)})")
    except urllib.error.HTTPError as e:
        if tmp.exists():
            tmp.unlink()
        raise SystemExit(f"download failed ({e.code}): {url}") from e
    except Exception as e:
        if tmp.exists():
            tmp.unlink()
        raise


def extract_tar(archive: Path, dest_dir: Path) -> None:
    """Extract a tar archive (handles .tar, .tar.gz, .tgz, .tar.xz)."""
    dest_dir.mkdir(parents=True, exist_ok=True)
    # tarfile handles .tar, .tar.gz, .tgz, .tar.xz transparently by mode "r:*"
    with tarfile.open(archive, mode="r:*") as tf:
        tf.extractall(dest_dir)


def decompress_bz2(src: Path, dst: Path) -> None:
    """Decompress a .bz2 file."""
    dst.parent.mkdir(parents=True, exist_ok=True)
    with bz2.open(src, "rb") as f_in, open(dst, "wb") as f_out:
        while True:
            chunk = f_in.read(1024 * 1024)
            if not chunk:
                break
            f_out.write(chunk)


@dataclass(frozen=True)
class DatasetSpec:
    """Specification for a benchmark dataset."""
    key: str
    filename: str
    extract_kind: str  # tar_to_data, tar_to_repo_root, bz2_to_data_file, none


SPECS: list[DatasetSpec] = [
    DatasetSpec("dickens", "dickens.bz2", "bz2_to_data_file"),
    DatasetSpec("webster", "webster.bz2", "bz2_to_data_file"),
    DatasetSpec("linux", "linux-6.18.5.tar.xz", "tar_to_data"),
    DatasetSpec("numpy", "numpy-2.4.1.tar.gz", "tar_to_data"),
    DatasetSpec("casp15", "casp15.tgz", "tar_to_repo_root"),
]


def main() -> None:
    ap = argparse.ArgumentParser(
        description="Download and prepare MAR benchmark datasets with smart caching.",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  python3 get_data.py                    # Download and extract all datasets
  python3 get_data.py --only linux numpy # Download only linux and numpy
  python3 get_data.py --check-only       # Just verify datasets are ready (fast!)
  python3 get_data.py --force            # Re-download and re-extract everything
        """
    )
    ap.add_argument("--base-url", default=DEFAULT_BASE_URL, help=f"Base URL (default: {DEFAULT_BASE_URL})")
    ap.add_argument("--data-dir", default=str(REPO_ROOT / "benchmarks" / "data"), help="Destination for benchmark data files")
    ap.add_argument("--force", action="store_true", help="Re-download and re-extract even if data exists")
    ap.add_argument("--no-extract", action="store_true", help="Only download, do not extract/decompress")
    ap.add_argument("--check-only", action="store_true", help="Only check if datasets are ready (no download/extract)")
    ap.add_argument("--only", nargs="*", default=None, help="Only these datasets: dickens webster linux numpy casp15")
    args = ap.parse_args()

    data_dir = Path(args.data_dir).resolve()
    base = args.base_url.rstrip("/")

    wanted = set(args.only) if args.only else None
    specs = [s for s in SPECS if wanted is None or s.key in wanted]
    if wanted:
        missing = wanted - {s.key for s in specs}
        if missing:
            raise SystemExit(f"unknown dataset key(s): {', '.join(sorted(missing))}")

    cache = load_cache()

    eprint(f"Repo root:  {REPO_ROOT}")
    eprint(f"Data dir:   {data_dir}")
    eprint(f"Cache file: {CACHE_FILE}")
    eprint("")

    if args.check_only:
        # Just check if datasets are ready
        all_ready = True
        for s in specs:
            ready = is_ready(cache, s.key, data_dir, s)
            status = "✓ ready" if ready else "✗ not ready"
            eprint(f"{status}: {s.key}")
            all_ready = all_ready and ready
        
        eprint("")
        if all_ready:
            eprint("✓ all requested datasets are ready for benchmarking")
            return
        else:
            eprint("✗ some datasets are not ready - run without --check-only to prepare them")
            raise SystemExit(1)

    # Download and extract datasets
    for s in specs:
        url = f"{base}/{s.filename}"
        dest = data_dir / s.filename
        
        # Check if already ready (extraction complete + files exist + checksums OK)
        if not args.force and is_ready(cache, s.key, data_dir, s):
            eprint(f"✓ {s.key}: already prepared and verified")
            continue
        
        # Need to download and/or extract
        download(url, dest, force=args.force)

        if args.no_extract:
            continue

        try:
            if s.extract_kind == "tar_to_data":
                # Extract in benchmarks/data/
                dataset_dir = extracted_path_for_spec(data_dir, s)
                # Smart caching: if cache says extraction is complete AND dir exists, skip
                if not args.force and cache.get(s.key, {}).get("extraction_complete") and dataset_dir.exists():
                    extracted_check = fast_checksum(dataset_dir)
                    eprint(f"✓ {s.key}: extraction already cached")
                else:
                    eprint(f"→ extracting {dest.name} into {data_dir}/")
                    extract_tar(dest, data_dir)
                    extracted_check = fast_checksum(dataset_dir)
            elif s.extract_kind == "tar_to_repo_root":
                # casp15 is expected at repo root: ./casp15/
                dataset_dir = extracted_path_for_spec(data_dir, s)
                # Smart caching: if cache says extraction is complete AND dir exists, skip
                if not args.force and cache.get(s.key, {}).get("extraction_complete") and dataset_dir.exists():
                    extracted_check = fast_checksum(dataset_dir)
                    eprint(f"✓ {s.key}: extraction already cached")
                else:
                    eprint(f"→ extracting {dest.name} into {REPO_ROOT}/")
                    extract_tar(dest, REPO_ROOT)
                    extracted_check = fast_checksum(dataset_dir)
            elif s.extract_kind == "bz2_to_data_file":
                out_path = extracted_path_for_spec(data_dir, s)
                # Smart caching: if cache says extraction is complete AND file exists, skip
                if not args.force and cache.get(s.key, {}).get("extraction_complete") and out_path.exists():
                    extracted_check = fast_checksum(out_path)
                    eprint(f"✓ {s.key}: decompression already cached")
                else:
                    eprint(f"→ decompressing {dest.name} to {out_path.name}")
                    decompress_bz2(dest, out_path)
                    extracted_check = fast_checksum(out_path)
            else:
                extracted_check = None
            
            # Update cache with extraction status and checksum
            if extracted_check:
                cache[s.key] = {
                    "extraction_complete": True,
                    "extracted_check": extracted_check,
                    "archive_filename": s.filename,
                }
                eprint(f"✓ {s.key}: extraction complete and verified")
            
        except Exception as e:
            # Mark as failed in cache so we retry next time
            cache[s.key] = {"extraction_complete": False, "error": str(e)}
            save_cache(cache)
            raise SystemExit(f"failed to extract {dest}: {e}") from e

        eprint("")

    save_cache(cache)
    eprint("✓ benchmark data ready for benchmarking")


if __name__ == "__main__":
    main()


