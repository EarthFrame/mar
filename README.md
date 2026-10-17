# Mar - Universal Data Container & Archive Toolset

## Overview

> ### *"Water is the new oil."*

Mar is an open-source **Universal Data Container** and high-performance archive toolset built from the ground up for modern computing architectures and networked data pipelines.

In resource economics, people often say "water is the new oil." In that spirit, Mar—a homonym for the Spanish word for "sea"—is the modern counterpart to Tar. Where Unix Tar produces "tarballs," Mar produces **"marbles"**: compact, durable, and structured data containers engineered to hold any data type across local NVMe storage, distributed filesystems, or cloud object stores.

Written in pure Rust with seamless Python bindings (`pymar`), Mar delivers random file access, multi-threaded streaming, modern compression codecs, cryptographic integrity checks, and network byte-range querying without requiring the extraction of an entire archive.

---

## What is a Universal Data Container?

A **Universal Data Container** is a single-file archive that packages arbitrary, heterogeneous data—directory trees, documents, tabular files, images, or model weights—into a portable collection of data entities.

Where traditional formats force a compromise between packaging convenience and access efficiency, Mar provides both:

- **Single-file portability**: Eliminates the overhead of managing millions of loose files across filesystems, cloud buckets, and OS boundaries.
- **True random access**: A deterministic header and metadata index allow any file to be retrieved immediately without scanning or decompressing the rest of the container.
- **Selective cloud streaming**: Over HTTP, S3, or R2, tools like `pymar` fetch the archive index in two small requests and download only the specific compressed blocks required, cutting network egress by up to 99%.

### How Mar Compares

- **vs. Folders**: Loose directory trees are hard to transfer across file systems, cause inode exhaustion, can slow cloud transfers, and risk filesystem path issues (especially when moving between machines). Mar encapsulates the entire structure into a single verified container with zero loss of directory hierarchy or POSIX metadata.
- **vs. Tar (`.tar.gz`, `.tar.zst`)**: Tar formats store data sequentially. Extracting a file requires decompressing all preceding bytes, and querying a remote tarball requires downloading the full file. Mar isolates data into independent compressed blocks, enabling direct \(O(1)\) access to individual files, even over a network connection.
- **vs. Zip (`.zip`)**: Standard Zip compresses files individually, yielding poor compression on small files, or relies on legacy algorithms like Deflate. Mar groups files into configurable block sizes using modern Zstandard or LZ4 compression, balancing high compression density with fast random reads.
- **vs. Parquet (`.parquet`)**: Parquet is a specialized columnar format for structured tables. It cannot store directory trees, unstructured documents, or binary assets. Mar provides a more general container and can store Parquet tables alongside the code, configuration, raw text, and model weights that accompany them.

---

## Features and Use Cases

- **Blazing Fast**: Pure Rust core (`mar-core`) with multi-threaded parallel compression, decompression, and block checksum verification.
- **True Random Access**: Inspect or extract individual files directly in \(O(1)\) time without decompressing preceding blocks or unpacking the entire container.
- **Cloud & Egress Optimization**: Query and slice remote archives on S3, R2, B2, or HTTP servers via byte-range requests. Download only the target blocks, reducing egress bandwidth by 95% to 99%+.
- **Zero-Copy Tar Ingestion**: Stream compressed or uncompressed tarballs (`.tar`, `.tar.gz`, `.tar.zst`, `.tar.bz2`, `.tar.lz4`) directly into Mar format with \(O(1)\) memory overhead without writing intermediate files to disk.
- **Flexible Codecs & Integrity**: Built-in support for Zstandard (default), LZ4, Gzip, Bzip2, or uncompressed storage. Block and file-level integrity powered by XXHash3, XXHash32, CRC32C, and BLAKE3.
- **POSIX Fidelity & Reproducibility**: Preserves file permissions, ownership (UID/GID), and modification times. Supports a `--deterministic` flag to generate byte-identical reproducible marbles across builds.
- **Sidecar Indexing & Native RAG**: Our commercial version of Mar supports sidecar indices (`.mai` files) for vector embeddings, MinHash similarity, and keyword search (BM25), enabling direct Retrieval-Augmented Generation (RAG) over raw archives without importing data into an external database.
- **Automation Ready**: Predictable CLI return codes, comprehensive `--json` and table outputs, and an intuitive syntax that automated pipelines and agentic models can drive reliably out of the box.

---

## Quick Start

### Installation

Mar is distributed as a standalone Rust CLI binary, a Python package (`pymar`), and prebuilt packages for standard platforms.

#### Python Package (`pymar`)

```bash
# Install via PyPI
pip install pymar

# Or install from source
cd python && pip install .
```

#### Prebuilt Binaries & Package Managers

Prebuilt binaries for Linux (x86_64, aarch64) and macOS (Apple Silicon, Intel) are available on GitHub Releases, as well as through Homebrew and Debian package repositories:

```bash
# Verify the installed binary
mar version
```

### Dependencies & Toolchains

- **Rust (1.80+)**: Required to build the standalone `mar` CLI binary, `mar-core` library, and `pymar` PyO3 extension.
- **Python (3.8+)**: Optional, required for the `pymar` library and high-level remote cloud CLI features.

#### Building from Source

```bash
# Build the release CLI binary (./mar)
make

# Build the Python PyO3 extension module
make python

# Run Rust and CLI test suites
make test
make integration-test

# Run Python test suite
make test-python

# Run performance smoke test
make perf-smoke
```

---

## Basic Usage

The standard structure for `mar` commands is:

```bash
mar [global options] <command> [command options] [arguments]
```

### Global Options

| Option | Description |
|--------|-------------|
| `-h`, `--help` | Display general help or command-specific help |
| `-q`, `--quiet` | Suppress non-error output (returns standard exit codes) |
| `-v`, `--verbose` | Increase logging verbosity (`-v`, `-vv`) |
| `--stopwatch` | Print execution time to stderr upon completion |

---

### Creating a Marble

Create a new container from individual files, trees, or pipelines:

```bash
# Create a marble from a directory using default settings (zstd, xxhash3, 1MB blocks)
mar create dataset.mar ./data/

# Create using LZ4 compression for ultra-fast throughput
mar create -c lz4 fast_cache.mar ./cache/

# Uncompressed container for raw high-speed random access
mar create -c none raw_media.mar ./media/

# Byte-for-byte reproducible build (normalized timestamps, sorted order)
mar create --deterministic release.mar ./dist/

# Deduplicate identical files across the directory tree
mar create --hashes --dedup deduplicated.mar ./artifacts/
```

---

### Importing Tar Archives (Zero-Copy)

Convert legacy `.tar`, `.tar.gz`, `.tgz`, `.tar.zst`, or `.tar.bz2` files directly into Mar containers without unpacking files to disk:

```bash
# Import a compressed tarball
mar import -o dataset.mar dataset.tar.gz

# Combine multiple tar archives into a single unified marble
mar import -c zstd -o combined.mar part1.tar.gz part2.tar.zst part3.tar

# Stream directly from a network pipe with zero intermediate disk writes
curl -sL https://example.com/checkpoints.tar.zst | mar import -f -o checkpoints.mar -
```

---

### Listing and Inspecting Contents

Inspect container metadata, file listings, and block structure:

```bash
# Standard path listing
mar list dataset.mar

# Tabular view with file sizes, compression details, and permissions
mar list --table dataset.mar

# Machine-readable JSON output for automated pipelines
mar list --json dataset.mar

# Inspect archive fixed headers, metadata offsets, and format version
mar header dataset.mar
```

---

### Extracting Files

Extract the entire container or target paths:

```bash
# Extract all files into the current directory
mar extract dataset.mar

# Extract into a target directory
mar extract -o /path/to/destination dataset.mar

# Strip leading directory components (equivalent to tar --strip-components)
mar extract -o ./output --strip-components 2 dataset.mar
```

---

### Random Access & Targeted Retrieval

Retrieve specific files immediately without decompressing the full container:

```bash
# Stream a single file directly to stdout
mar get -c dataset.mar metadata/manifest.json

# Extract selected files into a specific folder
mar get -o /tmp/target dataset.mar file1.csv config/settings.yaml
```

---

### Slicing Marbles (Local and Remote)

The `slice` command extracts a subset of files from a source archive into a new, self-contained marble without decompressing unselected entries.

#### Local Slicing

```bash
# Method 1: Glob pattern matching
mar slice input.mar -o filtered.mar "documents/**/*.pdf"

# Method 2: Algebraic include and exclude rules
mar slice input.mar -o subset.mar -i "src/**" -x "**/*.tmp"

# Method 3: Pipeline with grep and stdin file list
mar list input.mar | grep "2026-Q3" | mar slice input.mar -o q3_report.mar -T -

# Method 4: Read paths from an explicit list file
mar slice input.mar -o selected.mar -T target_files.txt
```

#### Remote Cloud Slicing via HTTP Range Requests

Using `pymar`, remote archives on S3, Cloudflare R2, Backblaze B2, or HTTP servers can be sliced directly over the network. The remote index is retrieved in two small reads and cached locally. Only the compressed blocks matching the query are downloaded:

```bash
# Download only matching files from a remote 100 GB marble into a 20 MB local marble
pymar slice --verbose -o local_subset.mar https://data.example.com/archive.mar "**Business**"
```

Because only the required blocks are transferred, cloud egress and transfer time are reduced by up to 99% compared to downloading the entire archive.

---

### Verifying Archive Integrity

Verify the physical structure and per-block checksums of a marble:

```bash
# Silent check for scripts and CI (exit code 0 indicates valid archive)
mar validate -q dataset.mar

# Detailed block-by-block integrity audit
mar validate -v dataset.mar
```

---

## Indexing & Search (Mar Enterprise)

The core Mar format and CLI are open-source under the Apache 2.0 license. For specialized information retrieval, Mar supports standalone index sidecar files (`.mai`) that attach structured indices to an existing marble without modifying the underlying archive:

- **Semantic Vector (HNSW)**: Dense vector embeddings for similarity lookup
- **Semantic MinHash**: Fast locality-sensitive hashing for document duplicate and similarity detection
- **Keyword (BM25)**: Full-text lexical search
- **Domain Indexes**: Time-series, email (MIME), and genomics (FASTA)

Sidecar indices allow native Retrieval-Augmented Generation (RAG) and search queries to run directly against Mar archives without loading content into an external vector database.

For index creation and query documentation, see the EarthFrame documentation at [https://docs.earthframe.com](https://docs.earthframe.com).

---

## Format Specification Summary

Mar implements format specification v0.1.0 with 64-byte block alignment for maximum I/O throughput on NVMe and flash media:

- **48-Byte Fixed Header**: Contains format magic bytes (`MAR\x01`), version numbers, flags, and absolute 64-bit offsets to the metadata section.
- **Metadata Section**: Structured sections including `NAME_TABLE` (raw, front-coded, or trie-compressed paths), `FILE_TABLE` (sizes, timestamps, permissions, and block references), and `FILE_SPANS`.
- **Data Blocks**: Each block features a 32-byte header recording codec identifiers, uncompressed/compressed sizes, and block-level checksums.
- **Flexible Block Modes**:
  - `shared`: Multiple files are packed into standardized blocks (e.g., 1 MB or 4 MB) for high compression ratios.
  - `single`: Each file occupies its own block for zero-overhead random access.
  - `multi`: Large files stream across sequential block boundaries.

Detailed byte specifications are documented in [`specs/mar-0.1.0.md`](specs/mar-0.1.0.md).

---

## Exit Codes

| Code | Meaning | Description |
|------|---------|-------------|
| 0 | Success | Operation completed successfully |
| 1 | General Error | I/O failure, unexpected EOF, or file missing |
| 2 | Usage Error | Invalid command-line arguments or invalid syntax |
| 65 | Integrity Error | Block checksum mismatch or corrupted container structure |
| 69 | Unavailable Feature | Requested codec or capability is not compiled into binary |

---

## Documentation & Community

- **Full CLI Manual**: [`CLI.md`](CLI.md)
- **Binary Format Specification**: [`specs/mar-0.1.0.md`](specs/mar-0.1.0.md)
- **Official Documentation**: [https://docs.earthframe.com](https://docs.earthframe.com)
- **Contributing Guidelines**: [`CONTRIBUTING.md`](CONTRIBUTING.md)

---

## License

Mar is open-source software licensed under the Apache License, Version 2.0. See [`LICENSE.md`](LICENSE.md) for details.

---

## Citation

When using Mar in research or production publications, please cite:

```bibtex
@misc{dawson2026mar,
  author = {Dawson, Eric T.},
  title = {MAR: A Universal Data Container for Modern Computing},
  year = {2026},
  publisher = {EarthFrame},
  url = {https://github.com/earthframe/mar}
}
```
