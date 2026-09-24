# Source Code Organization

## Directory Structure

```
mar/
├── cpp/                  # C++ reference implementation
│   ├── include/mar/      # Public headers (31 files)
│   ├── src/              # Core implementation (20+ files)
│   └── tests/            # C++ test suite (test_main.cpp)
│
├── rust/                 # Pure Rust MAR engine & native CLI
│   ├── Cargo.toml        # mar-core crate definition
│   ├── src/lib.rs        # mar_core library
│   ├── src/              # Reader, writer, codecs, indices, diff, redact
│   └── src/bin/          # CLI executable (mar) and benchmark (mar-bench-rust)
│
├── python/               # Python bindings & package (pymar)
│   ├── Cargo.toml        # PyO3 native module (_mar)
│   ├── pyproject.toml    # Maturin configuration
│   ├── src/              # PyO3 FFI bridge
│   ├── pymar/            # Python package (core, remote, tar, torch)
│   └── tests/            # pytest test suite
│
├── include/mar/          # Root C++ header path
├── src/                  # Root C++ implementation files
├── tests/                # Test suites & cross-implementation integration tests
│   ├── test_main.cpp     # C++ unit tests
│   └── integration_test.sh # Comprehensive 400+ CLI test suite
│
├── deps/                 # Vendored C++ dependencies (tracked)
│   ├── httplib.h         # HTTP client library
│   ├── nlohmann/         # JSON library
│   ├── hnswlib/          # HNSW indexing
│   ├── simde/            # SIMD emulation
│   └── BLAKE3/           # Hash algorithm
│
├── benchmarks/           # Unified benchmarking suite
│   ├── scripts/          # Head-to-head scripts & data generators
│   └── configs/          # Benchmark configurations
│
├── scripts/              # Build and utility scripts
├── docs/                 # Documentation & tutorials
├── Cargo.toml            # Root Cargo workspace orchestrator
└── Makefile              # Root build orchestrator (make cpp, make rust, make python)
```

## Tracking Policy

### Always Tracked ✅

- **include/mar/** - All public headers (31 files)
- **src/** - All implementation files
- **tests/** - Test suites
- **deps/** - Vendored dependencies
- **scripts/** - Build scripts
- **docs/** - All documentation

### Never Tracked ❌

Built artifacts:
- `build/` - Compiler output
- `*.o` - Object files
- `dist/` - Distribution binaries
- `mar` executable
- `test_mar` executable

Test artifacts:
- `*.mar` - Test archives (too large, regenerated)
- `*.mai` - Test indices (too large, regenerated)

### Line Endings

All source files use **LF line endings** (Unix style) enforced via `.gitattributes`:
- `*.hpp`, `*.cpp`, `*.h` → LF
- `*.py`, `*.md` → LF
- `*.mar`, `*.mai` → Binary (no line ending conversion)

## Adding New Headers

When adding a new header file:

1. **Create** in `include/mar/` with name matching functionality:
   ```bash
   touch include/mar/my_feature.hpp
   ```

2. **Add** includes and inline documentation:
   ```cpp
   #pragma once
   
   namespace mar {
   
   /// Brief description
   class MyFeature {
   public:
       // ...
   };
   
   }  // namespace mar
   ```

3. **Commit** - It will be automatically tracked:
   ```bash
   git add include/mar/my_feature.hpp
   git commit -m "Add MyFeature header"
   ```

4. **Verify** it's tracked:
   ```bash
   git ls-files include/mar/ | grep my_feature
   ```

## Why Headers Are Important

- **API Contract** - Define what's public vs private
- **Documentation** - Comments explain the interface
- **Build Dependency** - Needed to compile client code
- **Git History** - Tracks API evolution
- **Discovery** - Easy to find available functions

## Ensuring Headers Stay Tracked

The `.gitattributes` file ensures:

1. ✅ Consistent line endings across platforms
2. ✅ Binary files (archives) don't get diffs
3. ✅ Source files always use LF (no CRLF on Windows)

The `.gitignore` file ensures:

1. ✅ Build artifacts are never tracked
2. ✅ Only source code and tests are committed
3. ✅ Binaries are generated, not stored

## Checking Status

```bash
# View all tracked headers
git ls-files include/

# Check for untracked headers
git status include/

# See which files are ignored
git check-ignore -v include/mar/*

# Verify line endings
git ls-files --stage include/mar/*.hpp | head -5
```

## If Headers Get Accidentally Ignored

```bash
# Remove from git cache but keep locally
git rm --cached include/mar/forgotten_header.hpp

# Add and commit
git add include/mar/forgotten_header.hpp
git commit -m "Re-track forgotten header"

# Verify it's now tracked
git ls-files include/ | grep forgotten_header
```

---

See also: `.gitignore`, `.gitattributes`, `Makefile`
