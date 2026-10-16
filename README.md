# mar - A modern archive format and toolset

## Overview

> ### *"Water is the new oil."*

MAR is an archive format and a data archive toolset designed for modern computing.

Computing hardware today looks very different than it did in the past, with the proliferation of accelerated, multi-core and multi-threaded processing, solid state storage devices, larger / faster RAM, and new  compression algorithms. In addition, the ways in which users interact with their data has changed. While the terminal persists, much of our interaction with our data occurs in the browser or via AI models.

MAR is designed to leverage advances in modern hardware and support new interfaces to data archives that mirror modern usage. MAR builds on the decades of knowledge embedded in other archive and data storage formats and tools to build a **m**odern **ar**chiver ready for the next decade of computing.

## Features

- **Fast**: Optimized for modern hardware with efficient I/O patterns on NVMe and SATA SSDs, hard drives, and accelerated/multi-core/multi-thread architectures.
- **Random Access**: Want just one file from your archive? MAR supports getting it without decompressing the entire archive.
- **Multiple compression**: Support for ZSTD (default), LZ4, Gzip, Bzip2, or no compression. Flexibility enables tuning for your use case.
- **Integrity verification**: Built-in integrity checking via XXHash, CRC32, or BLAKE3 checksums.
- **POSIX metadata**: Optionally preserves permissions, ownership, and timestamps, just like `tar`.
- **Flexibly file/block relationship**: Files can be stored in their own block, in a block shared with other files, or across multiple blocks, allowing you to tune the compression ratio vs. access time of your archive.
- **Parallel ready**: MAR, as a format and a tool, is designed to make use of available hardware parallelism wherever possible.
- **Modern tools for modern computing**: Is your data on S3? Do you want to talk to your data via MCP? Want to browse it like it's a simple website? MAR is designed to support these use cases.

## Quick Start

This README provides some basic usage - for more detailed usage, see the EarthFrame docs at [https://docs.earthframe.com](https://docs.earthframe.com).

### Installation

Package manager installation and static builds are available via Homebrew, Debian packages, prebuilt binaries on GitHub Releases, and PyPI:

```bash
# Python package and CLI
pip install pymar

# Or install from source:
cd python && pip install .
```

### Dependencies & Toolchains

- **Rust toolchain (1.80+)**: Required to build the primary `mar` CLI binary (`./mar`), `mar-core` crate, and the `pymar` Python extension.
- **Python (3.8+)**: Optional, for the Python package (`pymar`).

*Note*: An in-tree C++ reference implementation (`./mar-cpp`) is also available for benchmarking and format validation; see [`cpp/README.md`](cpp/README.md) for its compiler and dependency requirements.

#### Building

```bash
# Build the primary CLI tool (pure Rust binary: ./mar)
make

# Build Python PyO3 extension (python/pymar/_mar.so)
make python

# Run Rust unit tests
make test

# Run CLI integration tests
make integration-test

# Run Python unit tests
make test-python

# Performance smoke-test against saved baselines
make perf-smoke
```

## Usage

For the complete command-line reference covering every subcommand and option, see [`CLI.md`](CLI.md).

### Basics

The basic form of mar commands is as follows:

```bash
mar <subcommand> [options] <archive> ... [File Paths]
```

Mar archive names *should* end with the ".mar" extension, though this is not a requirement.

You can get the help for `mar`, or any `mar` command, by passing `-h` or `--help`:

```bash
## Get the help for mar, including all the subcommands and common options.
mar -h

## Get the help for a specific mar subcommand (in this case, "create," used to create an archive):
mar create --help
## or
mar create -h
```

`mar` has several common options:

```bash
Common options:
  -h, --help      Display this help message
  -q, --quiet     Suppress non-error output
  -v, --verbose   Enable verbose output (use multiple times for more detail)
  --stopwatch     Report command execution time
```

### Create an archive

```bash
# Create archive from files
mar create archive.mar file1.txt file2.txt

# Create archive from directory
mar create archive.mar mydir/

# Use LZ4 compression (faster)
mar create -c lz4 archive.mar mydir/

# Use no compression
mar create -c none archive.mar mydir/

# Deterministic output (sorted, fixed timestamps)
mar create --deterministic archive.mar mydir/
```

### Extract an archive

```bash
# Extract all files to current directory
mar extract archive.mar

# Extract to specific directory
mar extract -o /tmp/output archive.mar

# Extract with path stripping
mar extract --strip-components 1 archive.mar
```

### List contents

```bash
# Simple list
mar list archive.mar

# Table format with metadata
mar list --table archive.mar

# JSON output
mar list --json archive.mar

# Verbose with summary
mar list -v archive.mar
```

### Get specific files

```bash
# Extract to stdout
mar get -c archive.mar path/to/file.txt

# Extract to directory
mar get -o /tmp archive.mar file1.txt file2.txt
```

### Slice an archive (subset extraction)

Extract a subset of files from a source archive into a new, compact `.mar` archive. Works with local archives and remote cloud archives (`s3://` or `http://`) with recursive globs and algebraic include/exclude sets:

```bash
# Slice by glob pattern
mar slice input.mar -o subset.mar "*.pdb"

# Algebraic include and exclude rules
mar slice input.mar -o filtered.mar -i "structures/**" -x "*.json"

# Read file list from file
mar slice input.mar -o targets.mar -T target_ids.txt

# Remote cloud slicing (S3 / HTTPS) via pymar CLI with batch block coalescing:
pymar slice s3://alphafold-db-v4/proteome.mar -o local_targets.mar -T 2000_targets.txt
pymar slice https://cdn.example.com/proteome.mar -o local_targets.mar -T 2000_targets.txt
```

For more details and tutorials, see the EarthFrame documentation at [https://docs.earthframe.com](https://docs.earthframe.com).

## Indexing and Search

MAR supports sidecar indices (`.mai` files) for advanced search capabilities like structural similarity (MinHash) and semantic search (Vector/HNSW).

**Note**: In the standalone open-source `mar` CLI binary, `mar index` and `mar search` delegate to `mar-index` / `mar-enterprise` when available.

For documentation on creating and searching MAR indices, see the EarthFrame documentation at [https://docs.earthframe.com](https://docs.earthframe.com).

## Format Specification

This implementation follows the MAR format specification v0.1.0. Key features:

- **48-byte fixed header** with magic number, version, and metadata offsets
- **Section-based metadata container** with NAME_TABLE, FILE_TABLE, FILE_SPANS, etc.
- **32-byte block headers** with compression type and checksum
- **64-byte block alignment** for efficient I/O

See `specs/mar-0.1.0.md` for the complete specification.

## Exit Codes

| Code | Meaning |
|------|---------|
| 0 | Success |
| 1 | General error |
| 2 | Usage error |
| 65 | Integrity error (checksum mismatch) |
| 69 | Unavailable feature |

## License

See the LICENSE.md file in the project root.

## Citation

When using MAR, please cite:

> Dawson, Eric T. MAR: The NVMe Archive Format. https://github.com/earthframe/mar. 2026.

