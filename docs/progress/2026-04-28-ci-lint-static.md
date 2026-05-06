# CI, Lint, and Static Builds — Plan
**Date**: 2026-04-28

---

## Scope

Two workstreams, implemented in order:

1. **Lint & static analysis** — `clang-format` + `clang-tidy`, usable locally via `make lint`, then added to CI
2. **Zig-based static Linux builds** — truly portable Linux binaries via `zig c++` targeting musl libc

A broader CI restructure (comment-gated trigger, job graph, dependency caching) is planned but deferred until these two are solid.

---

## Workstream 1: Lint & Static Analysis

### Goal

`make lint` catches real bugs and enforces consistent formatting. It works locally with no extra setup beyond installing `clang-tidy` and `clang-format`. CI runs it in parallel with the build job so it doesn't add to the critical path.

---

### Step 1.1 — `compile_commands.json`

`clang-tidy` needs to know the include paths and compile flags for each file.

**Decision**: use `bear`. It wraps any build command, intercepts compiler invocations, and writes `compile_commands.json` in one shot — no Makefile changes needed, no `jq` dependency, and it works with any compiler.

```bash
bear -- make
```

Developers run this once after a clean checkout, or whenever source files are added or removed. `compile_commands.json` is already covered by the `*.json` entry in `.gitignore`.

`bear` is installed via `make dev-deps`. See `CONTRIBUTING.md` for details.

The alternative (`-MJ` compiler flags + `jq` merge) is cleaner for CI automation and keeps generation inside the Makefile, but requires `jq` and adds complexity to the compile rule. For CI, the lint job simply runs `bear -- make` before invoking `clang-tidy`. The tradeoff is documented in the conversation history for future reference if the approach needs to change.

---

### Step 1.2 — `.clang-format`

A `.clang-format` file at the repo root. Base style: `Google`. Key overrides to match the existing code:

- `ColumnLimit: 100`
- `IndentWidth: 4` (the codebase uses 4-space indent)
- `SortIncludes: true` with `IncludeBlocks: Regroup` — groups system headers, third-party, and local headers separately

`make lint` checks formatting without modifying files (exits non-zero if any file would change).  
`make lint-fix` applies formatting in place.

Files to format: `src/*.cpp`, `include/mar/*.hpp`, `tests/test_main.cpp`.  
Files to exclude: `deps/`, `include/mar/xxhash3.h` (third-party, not ours).

---

### Step 1.3 — `.clang-tidy`

A `.clang-tidy` file at the repo root. The check set is deliberate — not "enable everything".

**Enabled check groups:**

| Group | Rationale |
|---|---|
| `bugprone-*` | Catches genuine bugs: use-after-move, suspicious string ops, integer overflow casts |
| `modernize-use-nullptr` | Replace `NULL` and `0` used as pointers |
| `modernize-use-override` | Enforce `override` on virtual method overrides |
| `modernize-loop-convert` | Replace index-based loops with range-for where safe |
| `performance-unnecessary-copy-initialization` | Catches copies that should be `const&` |
| `performance-move-const-arg` | `std::move` on a const value does nothing |
| `readability-redundant-smartptr-get` | `.get()` calls that aren't needed |

**Explicitly disabled:**

- `modernize-use-trailing-return-type` — stylistic, not a bug
- `readability-identifier-naming` — would require a naming audit of the whole codebase; defer
- `cppcoreguidelines-*` — too broad and noisy at first; revisit after the initial pass

**Header filter**: Only report on `src/` and `include/mar/`. Suppress all findings from `deps/` and system headers. Configure via `HeaderFilterRegex: '^(src|include/mar)/.*'`.

**Initial run will produce warnings.** The first pass should be run with `--warnings-as-errors` disabled so we can triage and fix incrementally rather than being blocked. Once the codebase is clean, promote warnings to errors in CI.

---

### Step 1.4 — Makefile targets

Add the following targets, also listed in `make help`:

```
make compile-commands   Generate compile_commands.json (needed once before lint)
make lint               Check formatting + run clang-tidy (read-only, CI-safe)
make lint-fix           Apply clang-format fixes + clang-tidy --fix in place
```

`lint` should print a clear summary: files checked, warnings found, whether it passed. Exit code 0 = clean, non-zero = issues found.

Add `compile_commands.json` and `build/*.json` to `.gitignore`.

---

### Step 1.5 — CI lint job

Add a `lint` job to the workflow:

- Runs on `ubuntu-latest` only (no need to lint on all platforms)
- Installs `clang-tidy` and `clang-format` at a **pinned version** (e.g. `clang-tidy-18`) — floating versions cause spurious diffs as defaults change
- Installs apt dependencies (same list as the build job — reuse a composite action or job step template)
- Runs `make compile-commands && make lint`
- Runs **in parallel with `build`** — it does not gate the build job, saving critical path time
- Both `lint` and `build` must pass for a PR to be mergeable (enforced via GitHub branch protection required status checks)

---

### Open questions for Workstream 1

- [ ] Do we want `clang-tidy` to run with `--warnings-as-errors` in CI from day one, or triage first? (Recommendation: triage first — initial run will have warnings, fix in a dedicated pass, then promote to errors.)
- [x] `compile_commands.json` approach: **`bear`** chosen. See step 1.1.
- [ ] Should `make lint` be part of the default `make all` target or strictly opt-in? (Recommendation: opt-in — not everyone has the dev tools installed.)

---

## Workstream 2: Zig-Based Static Linux Builds

### Goal

Linux release binaries that are truly portable — no glibc version dependency. Run on any Linux system from glibc 2.17 (RHEL 7) onward, and on musl-based systems like Alpine.

---

### The problem with current static builds

The Makefile's `STATIC=1` flag adds `-static -static-libgcc -static-libstdc++`. This statically links the C++ standard library, but the binary is still built on Ubuntu and therefore linked against the Ubuntu glibc. The resulting binary requires glibc ≥ 2.35 (Ubuntu 22.04) or ≥ 2.39 (Ubuntu 24.04). A user on RHEL 8 (glibc 2.28) cannot run it.

---

### Solution: `zig c++` as a drop-in compiler

`zig` ships its own musl toolchain and can compile directly to `x86_64-linux-musl` or `aarch64-linux-musl` from any host — no Docker, no cross-compilation toolchain install required. It is used as a `CXX` replacement:

```
CXX="zig c++ -target x86_64-linux-musl" make STATIC=1 BUILD=release all-internal
```

The build system, include paths, and link flags all stay the same. Only the compiler changes.

**Zig version**: pin to a specific release (e.g. 0.13.0). Zig is distributed as a single tarball with no install step — download, extract, add to `PATH`.

---

### Step 2.1 — New Makefile targets

Add the following targets alongside the existing `dist-linux-*` targets:

```
dist-linux-x86_64-musl       # x86_64, musl, SSE2 baseline
dist-linux-x86_64-sse42-musl # x86_64, musl, SSE4.2
dist-linux-arm64-musl        # aarch64, musl
```

Pinned zig version: **0.16.0** (released 2026-04-14). Update `ZIG_VERSION` in the Makefile when bumping.

Each mirrors its glibc counterpart but overrides `CXX` to use `zig c++` with the appropriate target triple and adds `STATIC=1`.

Update `dist-all` to build the musl variants on Linux in addition to (or instead of) the glibc-static variants. Decision: **musl variants become the primary Linux release artifacts**; glibc-static variants are retained but not uploaded to releases by default.

---

### Step 2.2 — Local development experience

Add `make zig-check` that verifies `zig` is on `PATH` and prints its version. If `zig` is not found, the musl dist targets print a clear error and exit rather than failing mysteriously.

Document the zig install in the README under a "Building release binaries" section:

```
# Download zig (one-time setup)
curl -L https://ziglang.org/download/0.13.0/zig-linux-x86_64-0.13.0.tar.xz | tar xJ
export PATH=$PWD/zig-linux-x86_64-0.13.0:$PATH

# Build musl static binary
make dist-linux-x86_64-musl
```

---

### Step 2.3 — CI integration

Add a `dist-musl` job to the workflow:

- Runs on `ubuntu-22.04` (the musl binary built here runs on any Linux regardless)
- Installs `zig` from the pinned tarball URL (no package manager needed — fast and deterministic)
- Installs apt dependencies as usual (needed for compilation headers even when targeting musl)
- Runs `make dist-linux-x86_64-musl dist-linux-x86_64-sse42-musl`
- Uploads `dist/mar-linux-*-musl` as a job artifact
- Depends on `unit-test` passing (same as the glibc dist job)

---

### Step 2.4 — Integration test coverage for musl binaries

Run the integration test suite a second time against the musl binary (still on the Ubuntu runner — the musl binary is self-contained and runs fine there):

```
MAR_BIN=dist/mar-linux-x86_64-musl ./tests/integration_test.sh
```

This is the critical check: the musl binary is the release artifact, so it must pass the same integration tests as the dev build.

Add this as a separate job (`integration-test-musl`) that depends on `dist-musl` and runs in parallel with the regular `integration-test` job.

---

### Step 2.5 — Release artifact naming

With musl and glibc variants both available, make the naming explicit in the release assets:

| Filename | Description |
|---|---|
| `mar-linux-x86_64` | glibc-linked, requires glibc ≥ 2.35 |
| `mar-linux-x86_64-musl` | musl-static, runs on any Linux |
| `mar-linux-x86_64-sse42-musl` | musl-static, SSE4.2 optimized |
| `mar-linux-arm64-musl` | musl-static, ARM64 |
| `mar-macos-universal` | macOS Universal Binary (ARM64 + x86_64) |

The release notes (generated by `generate_release_notes: true`) should include a brief download guide pointing users to `mar-linux-x86_64-musl` as the recommended Linux download.

---

### Open questions for Workstream 2

- [ ] Should the glibc-static `dist-linux-*` targets still be uploaded to releases, or only musl? (Recommendation: musl only, keep glibc targets in Makefile for developers who need them)
- [x] Zig version pinned in Makefile as `ZIG_VERSION = 0.16.0`. CI and `make dev-deps` both use this variable.
- [ ] ARM64 musl build in CI: `zig c++ -target aarch64-linux-musl` should work from an x86_64 runner transparently. Verify with a test build before committing to the full CI job.

---

## Deferred: Full CI Restructure

The following items are captured but not in scope for the current two workstreams. They will be planned once the above is stable.

- Comment-gated PR trigger (`issue_comment` → `workflow_call`)
- Job dependency graph (build → unit-test → dist → integration-test → release)
- Dependency caching (apt + Homebrew)
- Matrix expansion (ubuntu-22.04, ubuntu-24.04, macos-latest)
- Custom Docker images for pre-baked dependency layers

See conversation history for full design notes on each of these.
