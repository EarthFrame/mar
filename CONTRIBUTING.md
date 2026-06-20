# Contributing to MAR

Thank you for your interest in contributing. This document covers everything you need to get a working development environment, run tests, lint your code, and submit a pull request.

---

## Prerequisites

### Build dependencies

These are required to compile `mar`. See the [README](README.md) for per-distro install commands, or run:

```bash
make system-deps
```

| Dependency | Purpose |
|---|---|
| GCC 9+ or Clang 7+ | C++17 compiler |
| zlib | Gzip compression |
| libzstd | Zstandard compression (default) |
| liblz4 | LZ4 compression |
| libbz2 | Bzip2 compression |
| libdeflate | Fast Gzip/Deflate implementation |
| libblake3 | BLAKE3 checksums (vendored in `deps/`, no install needed) |

### Developer tools

These are needed for linting and building portable static release binaries. They are **not** required to build and test the project for day-to-day development.

```bash
make dev-deps
```

Run `make check-dev-deps` at any time to confirm what is installed:

```bash
make check-dev-deps
```

| Tool | Version | Purpose |
|---|---|---|
| `bear` | any recent | Generates `compile_commands.json` for clang-tidy |
| `clang-tidy` | **21.x** | Static analysis |
| `clang-format` | **21.x** (must match clang-tidy) | Code formatting |
| `zig` | 0.16.0 | musl-libc cross-compilation for portable Linux binaries |

> **Version matters for `clang-format`**: different versions format some constructs differently, causing false failures in CI. The required version is **21.x**. `brew install llvm` currently ships 21.x on macOS. On Linux: `apt-get install clang-format-21 clang-tidy-21` (requires the [LLVM apt repository](https://apt.llvm.org)). CI installs LLVM 21 explicitly for the same reason.

> **macOS note**: `make dev-deps` installs `clang-tidy` and `clang-format` via `brew install llvm`. Homebrew does not add these to `PATH` by default to avoid shadowing Apple's tools. Either add `$(brew --prefix llvm)/bin` to your `PATH`, or pass the paths explicitly when running lint:
> ```bash
> make lint CLANG_TIDY=$(brew --prefix llvm)/bin/clang-tidy \
>           CLANG_FORMAT=$(brew --prefix llvm)/bin/clang-format
> ```
> Or add the export to your shell profile so you don't have to repeat it.

> **Linux zig note**: `make dev-deps` installs `zig` to `~/.local/bin`. Make sure `~/.local/bin` is in your `PATH`. Add `export PATH="$HOME/.local/bin:$PATH"` to your `~/.bashrc` or `~/.profile` if it isn't already.

---

## Building

```bash
# Standard release build
make

# Debug build (with symbols, no optimization)
make debug

# Run unit tests
make test

# Run integration tests (uses the built binary at ./mar)
make integration-test

# Check which optional libraries were found
make check-deps
```

---

## Linting

> **Status**: The `make lint` target is not yet implemented. It is tracked in [docs/progress/2026-04-28-ci-lint-static.md](docs/progress/2026-04-28-ci-lint-static.md). This section documents the intended workflow for when it is ready.

Before linting, generate a compilation database so `clang-tidy` knows your exact build flags and include paths:

```bash
bear -- make
```

This produces `compile_commands.json` at the repo root (already gitignored). You only need to re-run this when source files are added or removed, or when your build flags change.

Then run the lint checks:

```bash
# Check formatting and run static analysis (read-only, safe to run any time)
make lint

# Auto-apply clang-format fixes and clang-tidy suggestions in place
make lint-fix
```

`make lint` exits non-zero if any file has formatting violations or clang-tidy warnings. It is run in CI on every PR.

---

## Building Portable Static Binaries

CI produces Linux release binaries by building inside an **Alpine 3.20 Docker container**. Alpine uses musl natively, so `STATIC=1` produces a fully static binary with no glibc dependency — no zig required.

To reproduce the CI build locally (requires Docker):

```bash
mkdir -p dist
docker run --rm -v "$(pwd):/work" -w /work alpine:3.20 sh -c "
  apk add --no-cache build-base linux-headers pkgconf cmake \
    zstd-dev zstd-static lz4-dev lz4-static \
    bzip2-dev bzip2-static zlib-dev zlib-static \
    libdeflate-dev libdeflate-static liburing-dev
  make clean
  make STATIC=1 BUILD=release \
    ARCH_FLAGS=\"-march=x86-64 -mtune=generic\" \
    TARGET_NAME=dist/mar-linux-x86_64-musl \
    all-internal
"
```

For macOS and standard glibc-linked builds:

```bash
make dist-macos-universal     # ARM64 + x86_64 universal binary (macOS only)
make dist-linux-x86_64        # glibc-linked, SSE2 baseline
make dist-linux-x86_64-sse42  # glibc-linked, SSE4.2 (2008+ CPUs)
```

The zig-based musl targets (`make dist-linux-x86_64-musl`) still work locally if you have zig installed (`make dev-deps`), but CI uses the Alpine approach for reliability.

---

## Pull Request Process

1. Fork the repo and create a branch from `main`.
2. Make your changes. If you add or change a feature, update the relevant documentation.
3. Ensure `make test` and `make integration-test` both pass locally.
4. Ensure `make lint` passes with no warnings (`bear -- make` first if needed).
5. Open a PR against `main` and ask a maintainer to run CI.
6. Address any review feedback. CI must be green before merge.

---

## CI Trigger

CI on PRs is manually gated — it does not run automatically on every push from every contributor. A maintainer must trigger it via a PR comment. This prevents untrusted code from running in the CI environment.

### Commands

Post these as comments on any PR:

| Comment | Effect |
|---|---|
| `run tests` | Starts the full CI pipeline for the PR's current head commit |
| `cancel ci` | Cancels all in-progress CI runs for the PR's current head commit |
| `stop ci` | Same as `cancel ci` |

When CI is triggered, the bot immediately posts a 👀 reaction on your comment and then posts a link to the workflow run. All subsequent status updates appear in that run.

### Who can trigger CI

Only users listed in `.github/ci-approvers` can use these commands. Comments from anyone else are silently ignored.

#### Adding an approved user

1. Open `.github/ci-approvers` and add their GitHub username on a new line.
2. Open a PR with that change. A maintainer merges it.
3. The new user can trigger CI immediately on their next comment — no deploy step needed.

#### Removing an approved user

1. Open `.github/ci-approvers` and delete their line.
2. Open a PR with that change. A maintainer merges it.
3. The user's future trigger comments will be silently ignored.

### Security model

The `issue_comment` trigger runs against the **default branch** (`main`), not the PR branch. This means:

- The trigger code (`.github/workflows/trigger.yml`) cannot be modified by a PR — it always runs the version on `main`.
- The approvers list (`.github/ci-approvers`) is always read from `main` — a PR author cannot add themselves to it via their PR.
- The PR head SHA is fetched explicitly via the GitHub API and passed to the CI workflow as an input, so the CI run always targets the correct commit.

### How it wires together

```
PR comment
    │
    ▼
trigger.yml (issue_comment, runs on main)
    │
    ├─ reads .github/ci-approvers from main
    ├─ checks commenter login + command phrase
    ├─ posts 👀 reaction + status comment
    ├─ fetches PR head SHA via API
    │
    ▼
ci.yml (workflow_call, checks out PR head SHA)
    │
    ├─ lint (ubuntu-24.04)
    ├─ test × 3 (ubuntu-22.04, ubuntu-24.04, macos)
    ├─ dist × 2 (ubuntu-22.04 musl, macos universal)
    ├─ integration-test × 2 (tests the dist binaries)
    └─ release (tag pushes only)
```

CI also runs automatically (without a comment) on direct pushes to `main` and on `v*` tag pushes.

---

### Commit messages

Use the imperative mood in the subject line. Keep it under 72 characters. Add a body if the change needs context beyond what the diff shows.

```
Add SSE4.2-optimized checksum path for x86_64

The scalar fallback was bottlenecking archive creation on large files.
This path is selected at runtime via CPUID and falls back to the
existing implementation on older hardware.
```

---

## Code Style

- C++17. No C++20 features yet.
- 4-space indent, no tabs.
- Column limit: 100 characters.
- Formatting is enforced by `clang-format` (`.clang-format` at the repo root). Run `make lint-fix` to auto-format.
- Prefer `const&` over copies for function parameters that aren't modified.
- Use `override` on all virtual method overrides.
- No `NULL` — use `nullptr`.
- Comments should explain *why*, not *what*. If the code needs a comment to explain what it does, consider simplifying the code instead.

---

## Repo Layout

```
src/              Library and CLI source files
include/mar/      Public headers
tests/            Unit tests (test_main.cpp) and integration tests (integration_test.sh)
deps/             Vendored third-party libraries (BLAKE3, hnswlib, nlohmann/json, etc.)
docs/             Tutorials, release notes, and progress notes
scripts/          Helper scripts (perf checks, test data generation)
benchmarks/       Benchmark baselines and tooling
.github/          CI workflow definitions
```
