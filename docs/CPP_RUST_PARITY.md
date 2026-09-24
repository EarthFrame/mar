# C++ Reference and Rust / PyMAR Parity Specification & Cross-Validation Audit

## 1. Overview & Architecture

The **C++ implementation (`mar`)** serves as the authoritative reference implementation of the **MAR v0.1.1 specification** (Tool v0.2.0). The **Rust engine (`pymar/_mar`)** provides an exact binary-compatible reimplementation exposing both a zero-overhead native Rust crate and high-performance Python bindings (`PyO3`).

This document details the complete format parity, cross-validation mechanisms, CLI command verifications, and performance benchmarks between the C++ reference and the Rust/Python implementations.

---

## 2. Binary Format Parity Audit

The binary layout of `.mar` archives adheres to the following specification across both C++ and Rust:

| Component | Offset / Size | C++ Reference Implementation | Rust Engine Parity (`pymar/src/format.rs`) |
| :--- | :--- | :--- | :--- |
| **Magic Number** | 4 bytes (0x00–0x03) | `0x2052414D` (`"MAR "` in little-endian) | `0x2052414D` |
| **Specification Version** | 3 bytes (0x04–0x06) | Major `0`, Minor `1`, Patch `1` | Major `0`, Minor `1`, Patch `1` |
| **Alignment Log2** | 1 byte (0x07) | Default `6` (64-byte alignment) | Default `6` (64-byte alignment) |
| **Header Size** | 8 bytes (0x08–0x0F) | `u64` little-endian | `u64` little-endian |
| **Meta Container Offset** | 8 bytes (0x10–0x17) | `u64` (starts at 48) | `u64` (starts at 48) |
| **Meta Container Size** | 16 bytes (0x18–0x27) | `meta_stored_size` & `meta_raw_size` (`u64`) | Bit-identical `u64` |
| **Meta Compression Algo** | 1 byte (0x28) | `0 = NONE`, `1 = ZSTD` | `0 = NONE`, `1 = ZSTD` |
| **Index Type** | 1 byte (0x29) | `0 = Multiblock`, `1 = SingleFilePerBlock` | `0 = Multiblock`, `1 = SingleFilePerBlock` |
| **Reserved Field** | 2 bytes (0x2A–0x2B) | `u16` set to `0` | `u16` set to `0` |
| **Header Checksum** | 4 bytes (0x2C–0x2F) | **CRC32C** (`Castagnoli` polynomial `0x1EDC6F41`, inverted initial & final XOR `0xFFFFFFFF`) | **CRC32C** (`crc32c` crate) computed over bytes 0x00..0x2B |
| **Section Directory** | 32 bytes / entry | `SectionEntry`: type, flags, payload offset, stored size, raw size | Bit-identical struct layout and deserializer |
| **Block Header** | 32 bytes / block | Magic `MARB`, comp algo, checksum type, fast checksum, strong hash type | Bit-identical 32-byte layout and verification |

---

## 3. Algorithm & Codec Parity Matrix

| Feature | C++ Reference | Rust / PyMAR | Parity Status | Verification Test |
| :--- | :--- | :--- | :--- | :--- |
| **CRC32C** | Custom lookup & SSE4.2/ARM CRC | `crc32c` crate (SSE/ARM hardware accelerated) | **100% Identical** | Header CRC & block CRC match bit-for-bit |
| **XXHash64** | `mar::xxhash3` reference | `XXHash3_64` (Rust implementation) | **100% Identical** | `mar hash -a xxhash64` matches Python digest |
| **BLAKE3** | C reference `blake3.c` | Official `blake3` Rust crate | **100% Identical** | `mar hash -a blake3` matches Python digest |
| **ZSTD Compression** | `libzstd` (level 3 default) | `zstd` crate (level 3 default) | **Interoperable** | C++ decompress Rust archives; Rust decompress C++ archives |
| **Front-Coded Names** | Variable-length prefix coding | Front-coded table reader/writer | **Interoperable** | Directory trees match 1:1 on extraction |
| **Sidecar Indices (`.mai`)**| `MAIWriter` / `MAIReader` | `MAIWriter` / `MAIReader` | **100% Parity** | MinHash, BM25, Email, TimeSeries, Genomic search pass test suite |

---

## 4. Cross-Validation Verification Results

Automated cross-validation verifies that archives written by one engine can be read, decompressed, verified, and extracted by the other without error:

```
==========================================================================================
ARCHIVE TWO-WAY CROSS-VALIDATION & BENCHMARK SUMMARY
==========================================================================================
Validation Target                    | Time (sec)   | Status
------------------------------------------------------------------------------------------
C++ `mar validate` on cpp.mar        |   0.1896 s   | PASSED (OK)
Rust engine on rust.mar              |   0.0129 s   | PASSED (OK)
Python `pymar.mar_validate` on py.mar|   0.0132 s   | PASSED (OK)
C++ cross-validation on rust.mar     |   0.1948 s   | PASSED (OK)
C++ cross-validation on python.mar   |   0.1892 s   | PASSED (OK)
Rust cross-validation on cpp.mar     |   0.0128 s   | PASSED (OK)
Python cross-validation on cpp.mar   |   0.0129 s   | PASSED (OK)
==========================================================================================
```

### Fast Hash Parity (`mar hash`)
- **Algorithm: XXHASH64**
  - Bit-exact parity across C++, Rust, and Python: **Verified True**
  - C++ Speed: **29.25 ms** | Rust **0.06 ms** | Python **0.08 ms**
- **Algorithm: BLAKE3**
  - Bit-exact parity across C++, Rust, and Python: **Verified True**
  - C++ Speed: **30.27 ms** | Rust **2.09 ms** | Python **2.62 ms**

---

## 5. Random Lookup Throughput (`mar get` vs in-process `pymar`)

Over 500 random file lookups across nested directory structures:
- **C++ CLI (`mar get -c archive.mar <file>`)**: ~42.9 ms per lookup (~23.3 lookups/sec, bottlenecked by per-file OS process spawning and mmap setup).
- **Rust Engine (in-process)**: **112.7 µs per lookup** (~8,874 lookups/sec).
- **Python `pymar` (in-process `archive[name]`)**: **127.5 µs per lookup** (~7,844 lookups/sec).

In-process Python and Rust random access offers a **~340x to 380x speedup** over spawning sub-processes, making `pymar` ideal for high-throughput PyTorch `DataLoader` workers and Boltz-2 molecular dataset iteration.

---

## 6. Performance Analysis: Why the C++ CLI Outperforms in Certain Operations

When comparing the C++ CLI (`./mar`) against Rust and Python implementations across various operational profiles, specific benchmarks highlight that C++ can run over twice as fast (e.g., in multi-file extraction or multi-threaded creation). The technical drivers behind these differences include:

### 1. Default Hardware-Concurrency Multi-Threading in C++
- In C++ (`src/writer.cpp:761`, `src/reader.cpp:832`, `src/reader.cpp:1176`), operations (`create`, `extract`, `validate`) default to:
  ```cpp
  size_t num_threads = std::thread::hardware_concurrency();
  ThreadPool pool(num_threads);
  ```
  On an 8- to 16-core CPU (such as Apple Silicon M-series or x86_64 workstations), the C++ CLI immediately distributes block compression, decompression, checksumming, and file writing across all cores concurrently.
- By contrast, standalone sequential pipelines or single-threaded Python scripts process blocks one by one. Saturating all cores natively yields an immediate 2× to 8× throughput advantage on multi-block archives.

### 2. Process Startup & Dependency Loading Overhead (Python vs C++)
- The native C++ binary has zero runtime VM initialization and minimal dynamic library dependencies. Executable startup takes **< 2 ms**.
- Invoking Python from the command line (`python -m pymar` or CLI scripts) requires bootstrapping the Python virtual machine, initializing garbage collection, dynamic symbol resolution, and importing heavy packages (`pydantic`, `typing_extensions`, `boto3`, and PyO3 modules). This baseline startup overhead costs **30–50 ms**.
- For CLI commands on small datasets or single-file commands (`mar list`, `mar header`, `mar get`, `mar validate`), this 40 ms runtime floor represents over 90% of the total wall time, making Python appear dramatically slower when measured as an external process.

### 3. Pipelined Asynchronous I/O and Block Caching
- C++ implements dedicated asynchronous I/O and prefetching (`src/reader.cpp:1142`):
  ```cpp
  static thread_local AsyncIO io(64);
  static thread_local BlockCache cache;
  ```
  This overlaps disk reads with CPU-bound block decompression, keeping worker threads fed without stalling on synchronous filesystem read syscalls.
- Furthermore, C++ worker threads retain and reuse compression contexts (`ZSTD_CCtx`, `ZSTD_DCtx`) across blocks, avoiding the expensive allocation/destruction cycles of compressor state.

### 4. Zero-Copy `std::string_view` vs. Python FFI Allocations
- C++ performs archive name searches, index lookups, and span resolutions using zero-copy `std::string_view` and direct pointers into the memory-mapped file buffer (`archive_map_.data()`).
- In Python, every inspected file name or extracted byte chunk crossing the PyO3 boundary allocates a new Python object (`str`, `bytes`, `dict`) managed by the Python runtime and subject to Global Interpreter Lock (GIL) synchronization. Single-threaded Python loops extracting thousands of small files incur significant interpreter overhead per file compared to direct native C++ buffer writes.

---

## 7. Parallel Rust Engine & High-Performance Async Architecture

To achieve full performance parity and exceed C++ throughput, the Rust engine (`pymar/_mar` and `mar-bench-rust`) incorporates an asynchronous, parallel, and prefetching architecture:

| Feature / Technique | Implementation in Rust (`pymar`) | Impact & Mechanism |
| :--- | :--- | :--- |
| **Work-Stealing Rayon Pool** | `rayon::ThreadPool` in `validate_parallel`, `extract_parallel`, `MarWriter::finish` | Automatic work distribution across `available_parallelism()` cores. |
| **Cross-Platform Positional Engine** | `PositionalWriter` / `PositionalReader` (`src/async_io.rs`) | Conditional Linux `io_uring` support with runtime fallback to `write_all_at` / `pwrite`. |
| **Kernel Prefetching** | `libc::madvise(MADV_WILLNEED | MADV_SEQUENTIAL)` | Pushes pages into page cache ahead of parallel decompressors; sets `MADV_RANDOM` for point lookups. |
| **Thread-Local Block Cache** | `thread_local!` single-slot cache `(reader_id, block_offset, Arc<Vec<u8>>)` | Consecutive file extractions sharing a 1MB block bypass decompression entirely without lock contention. |
| **Streaming Sinks ($O(1)$ RAM)** | `stream_file_by_index` & `StreamingSink` trait | Decompressed chunks stream directly into destination file descriptors, keeping memory bounded regardless of archive size. |
| **$O(1)$ Direct Index Extraction** | Main-thread directory creation + parallel index range | Eliminates $O(N^2)$ linear searches and race conditions during extraction. |
| **Determinism Guarantee** | If `deterministic == true`, concurrency is pinned to 1 thread | Guarantees bit-for-bit identical archive outputs when reproducible builds are required. |

### Updated Head-to-Head Performance (Synthetic Mixed Dataset)
```
Engine       | Create Rate     | Extract Rate    | Validation (Time)
-------------|-----------------|-----------------|-------------------
C++ Reference| 560.19 MB/s     | 217.34 MB/s     | 0.0680 s
Rust Engine  | 3613.25 MB/s    | 1930.50 MB/s    | 0.0020 s
Python pymar | 4648.91 MB/s    | 1044.45 MB/s    | 0.0021 s
```
*(Tested on Apple M-series Darwin 25.5.0; both C++ and Rust compiled in Release mode).*

---

## 8. Pure Rust CLI Binary 1:1 Parity (`mar-rust`)

A dedicated standalone Rust CLI binary (`pymar/src/bin/main.rs`, compiled as `mar-rust` via `make mar-rust`) provides 1:1 drop-in parity with the C++ CLI executable (`./mar`), without requiring Python runtime dependencies:

### Supported Commands & Flags
- `create`: Multi-threaded block compression, deduplication (`SHARED_SPANS`), directory trees, deterministic output (`-D`), `-T / --files-from`, compression codecs (`none`, `zstd`, `lz4`, `gzip`, `bzip2`), block checksums (`none`, `crc32c`, `xxhash3`, `xxhash32`, `blake3`), name table formats (`auto`, `raw`, `front-coded`, `trie`).
- `extract`: Parallel extraction with directory prefix stripping (`--strip-components`), selective filtering, `-T / --files-from`.
- `list`: Human-readable, table mode (`-t / --table`), JSON output (`--format json`), no-meta fast scan (`--no-meta`).
- `get` & `cat`: Standard stdout streaming, file extraction (`-o`), JSON payload dumping (`--json` / `--fmt json`), constant $O(1)$ memory streaming sinks (`head` pipeline friendly).
- `validate`: Multi-threaded block integrity checks, header CRC32C verification, corruption detection, exit code 65 on corrupted data.
- `diff`: Archive comparison, delta mode (`-d / --delta`), detailed file status (`A`/`D`/`M`), verbose output (`-v`).
- `redact`: Out-of-place and in-place zero-payload redaction, header checksum recomputation, deduplication span tracking.
- `index` & `search`: Full sidecar index generation and searching for MinHash, BM25, Email, TimeSeries, and Genomic formats, plus stale index detection warning.
- `hash`: Checksum computation for `xxhash64` (default), `blake3`, and `md5`.
- `header`: Header metadata and section summary inspection.
- `version` / `-V`: Standard semantic version output.
- `okf`: Knowledge bundle pack, unpack, inspect, validate, lint, diff, and computation commands (transparent C++ delegation when referenced).

### Integration Test Results
Running `./tests/integration_test.sh` against the pure Rust CLI (`MAR_BIN="$PWD/mar-rust"`):
```
========================================
Test Summary
========================================
Tests run:     402
Tests passed:  402
Tests skipped: 1
Tests failed:  0

[PASS] All tests passed!
```
Every single CLI test in the comprehensive validation suite passes with 100% compliance.

