# MAR C++ Reference Implementation

Authoritative reference implementation of the MAR (Multi-format Archival) format v0.1.1 (tool v0.2.0).

## Layout

- `include/mar/`: Public C++ headers (`format.hpp`, `reader.hpp`, `writer.hpp`, etc.)
- `src/`: C++ implementation files
- `tests/`: Test suite (`test_main.cpp`, `integration_test.sh`)

## Build

```bash
# Build release binary (./mar)
make

# Run C++ test suite
make test
```
