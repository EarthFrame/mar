# Versioning and Release Guide

This document explains how the MAR project is versioned and the steps required to update the version numbers for a new release.

## Versioning Scheme

MAR uses two independent version numbers:

1.  **Specification Version**: Reflects the on-disk format of `.mar` and `.mai` files. This version only changes when a breaking change is made to the format or a new mandatory feature is added to the specification.
2.  **Tool Version**: Reflects the version of the `mar` CLI and the `pymar` library. This follows semantic versioning (SemVer) and changes with new features, bug fixes, or performance improvements.

## Where to Update the Version

When releasing a new version, you must update the following files:

### 1. C++ Constants (`include/mar/constants.hpp`)
This is the primary source of truth for the version numbers within the C++ codebase.

```cpp
// Format specification version numbers
constexpr u8 MAR_SPEC_MAJOR = 0;
constexpr u8 MAR_SPEC_MINOR = 1;
constexpr u8 MAR_SPEC_PATCH = 1;

// Tool version numbers
constexpr u8 TOOL_VERSION_MAJOR = 0;
constexpr u8 TOOL_VERSION_MINOR = 2;
constexpr u8 TOOL_VERSION_PATCH = 0;
```

### 2. Makefile (`Makefile`)
The Makefile uses these variables for package naming and to pass version information to the compiler if needed (though the headers are preferred).

```makefile
# Versioning
MAR_SPEC_MAJOR ?= 0
MAR_SPEC_MINOR ?= 1
MAR_SPEC_PATCH ?= 1

TOOL_VERSION_MAJOR ?= 0
TOOL_VERSION_MINOR ?= 2
TOOL_VERSION_PATCH ?= 0
```

### 3. Python Package (`pymar/pyproject.toml`)
The `pymar` package version should be synchronized with the Tool Version.

```toml
[project]
name = "pymar"
version = "0.2.0"
```

### 4. Debian Changelog (`debian/changelog`)
Add a new entry at the top of the file with the new version and a summary of changes.

```text
mar (0.2.0-1) unstable; urgency=low

  * Summary of changes...

 -- Author Name <email@example.com>  Sat, 20 Jun 2026 12:00:00 +0000
```

### 5. Homebrew Formula (`.homebrew/mar.rb`)
Update the URL to point to the new tag. The SHA256 should be updated after the tag is pushed and the archive is available.

```ruby
  url "https://github.com/earthframe/mar/archive/refs/tags/v0.2.0.tar.gz"
```

### 6. Integration Test Script (`tests/integration_test.sh`)
The integration test script contains a version header for logging purposes.

```bash
# Version: 0.2.0
```

## Release Process

1.  **Update Files**: Modify all the files listed above with the new version numbers.
2.  **Verify Build**: Run `make clean && make` to ensure the C++ tool builds and reports the correct version via `./mar version`.
3.  **Verify Python**: Reinstall the python package and check the version:
    ```bash
    cd pymar
    pip install -e .
    python -c "import pymar; print(pymar.get_version())"
    ```
4.  **Commit and Tag**:
    ```bash
    git add .
    git commit -m "Release v0.2.0"
    git tag -a v0.2.0 -m "Version 0.2.0"
    ```
5.  **Push**:
    ```bash
    git push origin main --tags
    ```
