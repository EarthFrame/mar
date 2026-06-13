# pymar Design Document

This document outlines the design principles and architecture of the `pymar` Python package.

## Goals

1.  **Perfect Equivalence**: The Python bindings must behave exactly like the C++ implementation.
2.  **High Performance**: Minimal overhead when calling C++ functions; leverage zero-copy and multi-threading where possible.
3.  **Pythonic Interface**: Provide a natural Python API while maintaining access to low-level C++ features.
4.  **LLM Integration**: Expose a structured, well-documented API suitable for tool-calling by Large Language Models.
5.  **Maintainability**: Minimize code duplication by linking directly against the MAR C++ source.

## Architecture

`pymar` is structured in three layers:

### 1. C++ Extension (`_mar.cpp`)
The core of the package is a compiled C++ extension built using `pybind11`. This layer:
- Wraps core MAR classes (`MarReader`, `MarWriter`, `MAIReader`, `MAIWriter`).
- Exposes enums and option structures.
- Handles the conversion between Python types (e.g., `bytes`, `list`, `dict`) and C++ types (e.g., `std::vector<u8>`, `std::vector<std::string>`, `std::map`).
- Provides high-level entry points for complex operations like `search`.

### 2. Python Core (`core.py`)
A Pythonic wrapper around the compiled extension. This layer:
- Provides the `MarArchive` class for a more object-oriented experience.
- Uses `pydantic` models for structured data validation and serialization.
- Handles default parameters and high-level logic that doesn't require C++ performance.

### 3. Tool-Calling Layer (`tools.py`)
A set of flat functions designed for LLM integration. This layer:
- Features descriptive docstrings for LLM discovery.
- Returns simple Python dictionaries (via Pydantic's `model_dump()`) for easy parsing by LLMs.
- Simplifies the API surface to the most common use cases.

## Wrapping Strategy: pybind11

We chose `pybind11` because it:
- Is header-only and easy to integrate into the build process.
- Provides excellent support for C++11/14/17 features.
- Handles automatic conversion of STL containers.
- Allows for fine-grained control over memory management and the Global Interpreter Lock (GIL).

## Design Principles

- **Explicit is better than implicit**: Options are passed via structured objects or keyword arguments.
- **Fail fast**: Use Pydantic and C++ exceptions to catch invalid inputs early.
- **Resource Management**: The `MarArchive` class (and underlying C++ classes) ensure that file handles and memory maps are properly closed.
- **Performance by Default**: Multi-threading is enabled for heavy operations (compression, validation) by default, matching the C++ CLI behavior.
