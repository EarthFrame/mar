# MAR C++ Reference Implementation

Authoritative reference implementation of the MAR (Multi-format Archival) format v0.1.1 (tool v0.3.0), used for format validation, benchmarking, and cross-implementation verification.

## Dependencies & Requirements

- **C++17 compiler**: GCC 8+ or Clang 7+
- **Compression libraries & development headers**:
  - `libzstd` (Zstandard compression)
  - `zlib` / `libdeflate` (Gzip/Deflate compression)
  - `liblz4` (LZ4 compression)
  - `libbz2` (Bzip2 compression)
  - `libblake3` (Vendored in `deps/`, no system install required)

### Installing Dependencies

**macOS (Homebrew):**
```bash
brew install gcc zstd lz4 bzip2 libdeflate
```

**Debian / Ubuntu / Linux Mint:**
```bash
sudo apt update
sudo apt install build-essential cmake libzstd-dev liblz4-dev libbz2-dev zlib1g-dev libdeflate-dev
```

**Fedora / RHEL / CentOS:**
```bash
sudo dnf install gcc-c++ cmake zstd-devel lz4-devel bzip2-devel zlib-devel libdeflate-devel
```

**Arch Linux:**
```bash
sudo pacman -S gcc cmake zstd lz4 bzip2 zlib libdeflate
```

## Layout

- `include/mar/`: Public C++ headers (`format.hpp`, `reader.hpp`, `writer.hpp`, etc.)
- `src/`: C++ implementation files
- `tests/`: C++ unit test suite (`test_main.cpp`)

## Build

From this directory:
```bash
# Build C++ reference binary (../mar-cpp)
make

# Build with Clang
make CXX=clang++

# Check available compression libraries
make check-deps

# Run C++ unit test suite
make test
```

From the repository root:
```bash
# Build C++ reference binary (./mar-cpp)
make cpp

# Run C++ reference unit tests
make test-cpp
```

