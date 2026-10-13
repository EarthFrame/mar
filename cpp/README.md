# MAR C++ Reference Implementation

Authoritative reference implementation of the MAR (Multi-format Archival) format v0.1.1 (tool v0.3.0).

## Layout

- `include/mar/`: Public C++ headers (`format.hpp`, `reader.hpp`, `writer.hpp`, etc.)
- `src/`: C++ implementation files
- `tests/`: C++ unit test suite (`test_main.cpp`)

## Build

From this directory:
```bash
# Build C++ reference binary (../mar-cpp)
make

# Run C++ unit test suite
make test
```

From the repository root:
```bash
# Build C++ reference binary (./mar-cpp)
make cpp

# Run C++ unit test suite
make test-cpp
```
