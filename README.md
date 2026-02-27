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

Package manager installation and static builds are on the way. For now, if you want to try `mar`, you can build it from source.

### Dependencies

Required:

- C++17 compatible compiler (GCC 8+, Clang 7+)
- zlib (for gzip support)

```bash
## Apple Mac OS X
brew install g++

## Debian / Ubuntu
sudo apt-get install g++
```

Beyond the *very* basics, we recommend the following to enable all of mar's features:

- libzstd (for ZSTD compression - default)
- liblz4 (for LZ4 compression)
- libbz2 (for BZIP2 compression)
- libblake3 (for accelerated BLAKE3 - has builtin fallback)
- libdeflate (faster DEFLATE implementation)

On macOS with Homebrew:

```bash
brew install zstd lz4 bzip2 xxh3 blake3 libdeflate
```

On Ubuntu/Debian:

```bash
sudo apt update
sudo apt install build-essential cmake libzstd-dev liblz4-dev libbz2-dev zlib1g-dev libdeflate-dev
```

#### Building

```bash
# Build the tool
make

# Run unit tests
make test

# Run integration tests
make integration-test

# Check available compression libraries
make check-deps

## Run a quick performance smoke-test to help flag regressions.
make perf-smoke

### Performance Smoke Test
## The performance smoke test (`scripts/perf_smoke_test.sh`) compares current performance against a saved baseline in `.perf/previous_run.txt`.
## By default, it looks for test data in `benchmarks/data`. You can point it to a different directory using the `DATA_DIR` environment variable.
## The test will look for specific datasets like `linux-6.18.5`, `numpy-2.4.1`, or a `dickens` file within that directory.

# Run with a custom data directory
DATA_DIR=/path/to/my/benchmarks ./scripts/perf_smoke_test.sh

```

## Usage

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

## Indexing and Search

MAR supports sidecar indices (`.mai` files) for advanced search capabilities like structural similarity (MinHash) and semantic search (Vector/HNSW).

**Note**: Indexing and search are under heavy development. For now, consider these features in beta. The API and CLI are relatively stable but are subject to change. Complete documentation will be available soon.

### Building with Indexing Support

The indexing infrastructure is built into the core `mar` tool. To build:

```bash
make
```

### Using MAR Indexes and MAR Search

#### 1. Create an Archive

```bash
mar create -f data.mar ./my_docs/
```

#### 2. Generate a MinHash Index

MinHash indices allow finding files with similar content.
```bash
mar index -i data.mar --type minhash --with bit_width=32 --with hashes=256 --with threads=4
```

#### 3. Search for Similar Files

```bash
mar search -i data.mar --index data.mar.minhash.mai --type similarity --with file=report.txt
```

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

See LICENSE file in the project root.

## Citation

When using MAR, please cite:

> Dawson, Eric T. MAR: The NVMe Archive Format. https://github.com/earthframe/mar. 2026.

