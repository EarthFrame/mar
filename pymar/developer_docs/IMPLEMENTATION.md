# pymar Implementation Details

This document provides technical details on how `pymar` is implemented and how to extend it.

## Index Type Support

`pymar` supports several indexing strategies out of the box. Each index type is implemented in C++ and exposed via the `mar_index` and `mar_search` tools.

| Index Type | Search Type | Status | Notes |
| :--- | :--- | :--- | :--- |
| **MinHash** | Fuzzy/Jaccard | Full | Great for finding near-duplicate files. |
| **Vector** | Semantic | Full | Requires an external embedding server. |
| **BM25** | Keyword | Index Only | Currently used for hybrid search in the C++ core. |
| **Genomic** | Sequence | Full | Optimized for large-scale FASTA/FASTQ data. |
| **Email** | Metadata/Body | Full | Specialized for `.eml` and mbox formats. |
| **TimeSeries** | Pattern | Full | For efficient similarity search in numerical data. |

## Updating the Bindings

If you make changes to the core MAR C++ library, follow these steps to update the Python bindings:

### 1. Recompile the Extension
If you have only modified the implementation of existing C++ functions, you just need to re-run the build:

```bash
cd pymar
pip install -e .
```

The `setup.py` script automatically pulls the latest source files from `../src/` and recompiles the `_mar` extension.

### 2. Adding New C++ Source Files
If you add a new `.cpp` file to the MAR core library (e.g., `src/new_feature.cpp`):
1. Open `pymar/setup.py`.
2. Add the path `os.path.join(src_dir, "new_feature.cpp")` to the `sources` list.
3. Run `pip install -e .`.

### 3. Exposing New API Features
To expose new C++ classes or methods to Python:
1. **Update `_mar.cpp`**: Use `pybind11` to map the new C++ symbols.
2. **Update `core.py`**: Add high-level Pythonic wrappers and Pydantic models.
3. **Update `tools.py`**: Add tool-calling wrappers for LLM integration.

---

## Build System

`pymar` uses a hybrid build system:
- **`pyproject.toml`**: Defines the build requirements (`setuptools`, `pybind11`).
- **`setup.py`**: A custom build script that:
    1.  Detects system libraries (`libzstd`, etc.) using `pkg-config`.
    2.  Configures compiler flags (C++17, optimization levels).
    3.  Collects C++ source files from the parent `src/` directory.
    4.  Links against the pre-built `libblake3.a` if available.
    5.  Compiles the `_mar` extension.

### Adding New C++ Files
If you add a new `.cpp` file to the MAR core library that needs to be included in the Python bindings, add it to the `sources` list in `pymar/setup.py`.

## C++ to Python Mapping

### Enums
C++ enums are mapped using `py::enum_`. For example:
```cpp
py::enum_<CompressionAlgo>(m, "CompressionAlgo")
    .value("ZSTD", CompressionAlgo::Zstd)
    .export_values();
```

### Data Conversion
- `std::vector<u8>` is converted to Python `bytes` using custom lambda wrappers in `_mar.cpp` to ensure efficiency.
- `std::vector<std::string>` and `std::map<std::string, std::string>` are handled automatically by `pybind11/stl.h`.

### Error Handling
C++ exceptions (e.g., `std::runtime_error`, `mar::MarError`) are automatically translated to Python `RuntimeError` or similar by `pybind11`.

## Extending the API

### 1. Update `_mar.cpp`
Expose the new C++ function or class. If it's a new class, define its methods and properties.
```cpp
py::class_<NewClass>(m, "NewClass")
    .def(py::init<int>())
    .def("do_something", &NewClass::do_something);
```

### 2. Update `core.py`
Add a high-level Python wrapper. Use Pydantic models for any new structured return types.
```python
class NewResult(BaseModel):
    status: str

def do_new_thing(arg: int) -> NewResult:
    res = _mar.NewClass(arg).do_something()
    return NewResult(status=res)
```

### 3. Update `tools.py`
If the new functionality is useful for LLMs, add a tool-calling wrapper with a clear docstring.
```python
def mar_new_thing(arg: int) -> Dict:
    """Description for LLM."""
    return do_new_thing(arg).model_dump()
```

## Testing

Tests are located in the `tests/` directory. Run them using:
```bash
python3 tests/test_pymar.py
```
Always ensure that `pip install -e .` has been run after making changes to C++ source or the extension code.
