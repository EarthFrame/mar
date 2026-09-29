# Staging and Commit Plan

This document outlines the logical breakdown of pending changes across the repository, followed by ready-to-run Git staging and commit commands with clean, simple one-line commit messages.

---

## Change Review & Logical Groups

1. **Ignore Patterns**:
   - Ignore `/private_docs/` and `/private_benchmarks/` in `.gitignore`.

2. **Docker Support (Distroless Multi-Arch)**:
   - Add `.dockerignore`.
   - Add `docker/Dockerfile` (multi-arch Rust builder + static distroless runtime).
   - Add `docker-build*` targets to `Makefile`.

3. **C++ Slice Command & Integration Tests**:
   - Implement `mar slice` CLI command in `src/main.cpp` with algebraic filtering and S3/HTTP remote delegation.
   - Add integration tests for `slice` in `tests/integration_test.sh`.

4. **Rust Core Filter & Slice Implementation**:
   - Add `rust/src/filter.rs` with `glob_match` and `AlgebraicFilter`.
   - Wire `filter` module in `rust/src/lib.rs`.
   - Add `cmd_slice` to `rust/src/bin/main.rs`.
   - Update `rust/src/reader.rs` block validation.

5. **Python (`python/`) Slice Feature & CLI**:
   - Add algebraic glob filtering and slice methods to `python/pymar/core.py`.
   - Add remote range slicing in `python/pymar/remote.py`.
   - Add tool wrapper `mar_slice` in `python/pymar/tools.py`.
   - Expose `slice_archive` in `python/pymar/__init__.py`.
   - Add CLI support with `python/pymar/cli.py` and `python/pymar/__main__.py`.
   - Register console script in `python/pyproject.toml`.
   - Expose bindings in `python/src/py_bindings.rs`.
   - Add unit and integration tests in `python/tests/test_slice.py`.
   - Update `python/README.md`.

6. **Pymar Mirror Package Sync (`pymar/`)**:
   - Sync all slice features, CLI entrypoints, Rust crate filter module, and tests into `pymar/`.
   - Update `pymar/developer_docs/LLM_TOOL_CALLING.md`.

7. **Documentation & Tutorials**:
   - Add `docs/tutorials/slice.md` (tutorial and AlphaFold / S3 case study).
   - Update `docs/tutorials/README.md` and `docs/tutorials/cloud_storage_costs.md`.
   - Update root `README.md` with `mar slice` overview.

---

## Staging and Commit Script

Run the following commands in order from the repository root:

```bash
# 1. Ignore private docs and benchmarks
git add .gitignore
git commit -m "build: ignore private documentation and benchmark directories"

# 2. Distroless multi-arch Docker image and Makefile targets
git add .dockerignore docker/ Makefile
git commit -m "docker: add multi-arch distroless Dockerfile and Makefile targets"

# 3. C++ slice CLI implementation and integration test suite
git add src/main.cpp tests/integration_test.sh
git commit -m "feat(cli): add C++ mar slice command and integration tests"

# 4. Rust core filter module and CLI slice command
git add rust/src/filter.rs rust/src/lib.rs rust/src/bin/main.rs rust/src/reader.rs
git commit -m "feat(rust): implement algebraic filter and slice command in Rust core"

# 5. Python package slice functionality and CLI
git add python/
git commit -m "feat(python): add slice API, remote slicing, and pymar CLI"

# 6. Pymar crate and Python mirror synchronization
git add pymar/
git commit -m "sync(pymar): synchronize slice features, Rust filter, and CLI"

# 7. Documentation and slice tutorials
git add README.md docs/
git commit -m "docs: add slice tutorial, cloud egress guide, and README examples"
```
