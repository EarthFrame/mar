# pymar

High-performance Python bindings for the MAR archive format.

`pymar` provides a Pythonic interface to the core MAR C++ library, enabling efficient creation, indexing, and searching of large-scale archival data. It is designed for high performance, perfect equivalence with the C++ implementation, and seamless integration with tool-calling LLMs.

## Features

- **High Performance**: Powered by the MAR C++ backend via `pybind11`.
- **Full Functionality**: Supports creation, extraction, indexing (MinHash, Vector, etc.), semantic search, validation, and metadata inspection.
- **LLM-Friendly**: Includes structured tool-calling wrappers with Pydantic models and comprehensive docstrings.
- **Zero-Copy Reads**: Leverages memory-mapped I/O for efficient data access.
- **Parallel Processing**: Supports multi-threaded compression, extraction, and validation.

## Installation

### Prerequisites

- Python 3.8+
- C++17 compatible compiler (GCC 7+, Clang 5+, or MSVC 2017+)
- `pybind11` and `pydantic`
- System libraries: `libzstd`, `liblz4`, `libdeflate`, `zlib`, `libbz2`

### Install from Source

```bash
cd pymar
pip install .
```

For development:
```bash
pip install -e .
```

## Quick Start

### Basic Operations

```python
import pymar

# Create an archive
pymar.mar_create("data.mar", ["src/", "README.md"], compression="zstd")

# List files
files = pymar.mar_list("data.mar")
print(f"Files: {files}")

# Get file content
content = pymar.mar_get("data.mar", "README.md")
print(content)

# Get archive header
header = pymar.mar_header("data.mar")
print(f"Version: {header['version']}, Files: {header['files']}")

# Validate integrity
if pymar.mar_validate("data.mar"):
    print("Archive is valid!")
```

### Indexing and Search

`pymar` supports several indexing strategies for fast retrieval:
- **MinHash**: Fuzzy matching and near-duplicate detection.
- **Vector**: Semantic search using embeddings (requires an external server).
- **BM25**: Keyword-based retrieval (used for hybrid search).
- **Genomic/Email/TimeSeries**: Specialized indices for domain-specific data.

```python
# Create a MinHash index
pymar.mar_index("data.mar", "minhash")

# Search the archive
results = pymar.mar_search("data.mar", "data.mar.minhash.mai", "search query")
```

## Updating Bindings

If you update the core MAR C++ library, you can refresh the Python bindings by running:

```bash
cd pymar
pip install -e .
```

See [IMPLEMENTATION.md](developer_docs/IMPLEMENTATION.md) for detailed instructions on adding new C++ files or exposing new API features.

## Tool-Calling for LLMs

`pymar.tools` provides functions specifically designed for LLM tool-calling. These functions return structured data (dictionaries or Pydantic models) and have clear, descriptive docstrings that help LLMs understand when and how to use them.

Available tools:
- `mar_create`: Create archives.
- `mar_list`: List contents.
- `mar_get`: Retrieve file data.
- `mar_search`: Perform similarity search.
- `mar_hash`: Compute deterministic hashes.
- `mar_validate`: Check integrity.
- `mar_header`: Inspect metadata.
- `mar_version`: Get version info.

## Testing

`pymar` uses `pytest` for its test suite. To run the tests:

```bash
cd pymar
pytest tests/test_pymar.py
```

The tests cover all core functionalities, including creation, listing, reading, indexing, searching, and validation across various compression algorithms and edge cases.

## Documentation

- [Developer Docs](developer_docs/DESIGN.md): Design and implementation details for contributors.
- [LLM Tool Calling](developer_docs/LLM_TOOL_CALLING.md): Guide for integrating `pymar` with AI agents.
- [MAR Format Spec](../specs/mar-format-spec.md): Technical specification of the underlying binary format.


## Contributing

## License

Licensed under the MIT License.