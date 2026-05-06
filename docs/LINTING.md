# Addressing Linting Issues

This guide explains how to systematically address linting issues in the `mar` project.

## Quick Start

### Generate a Structured Report

```bash
make lint-report
```

This generates two files:
- **LINT_REPORT.md**: Human-readable report organized by package
- **LINT_REPORT.json**: Machine-readable format for LLM processing

### View the Report

```bash
cat LINT_REPORT.md
# or for JSON:
cat LINT_REPORT.json | jq
```

## Understanding the Report

### Report Organization

Issues are organized by **package** (component):

- **Indexing**: Issues in index-related code
- **Archive**: Issues in archive creation/reading
- **Compression**: Compression-related issues
- **Cryptography**: Crypto implementation issues
- **Utilities**: Helper/utility code
- **CLI**: Command-line interface code
- **Format**: File format implementation
- **Span Management**: Data span handling
- **Block Management**: Block-level operations
- **Core/General**: Other issues

Within each package, issues are grouped by **check type** (e.g., `bugprone-use-after-move`, `modernize-use-nullptr`).

### Report Format

Each issue includes:

```
📄 src/index_registry.cpp
   Package: Indexing | Issues: 3
   ───────────────────────────────────────────────────────────────────────────
   Line   45, Col  12: WARNING
   → bugprone-unused-return-value
      Ignoring return value of function declared with 'nodiscard' attribute
```

## Processing Issues with an LLM

The JSON report is designed to be easily parsed by language models:

```bash
cat LINT_REPORT.json | jq '.by_package.Indexing'
```

This returns all issues for the Indexing package, ready for an LLM to process:

```json
{
  "bugprone-use-after-move": [
    {
      "file": "src/index_registry.cpp",
      "line": 45,
      "col": 12,
      "severity": "warning",
      "message": "..."
    }
  ]
}
```

### Workflow for LLM-Assisted Fixes

1. **Extract one package at a time**:
   ```bash
   cat LINT_REPORT.json | jq '.by_package.Indexing' > indexing-issues.json
   ```

2. **Provide to LLM with context**:
   ```
   Here are the linting issues for the Indexing package. 
   Please suggest fixes for the following issues:
   [paste the JSON]
   
   The relevant source files are in src/index*.cpp and include/mar/index.h
   ```

3. **Review and apply suggestions** manually or via patch

4. **Re-run linting**:
   ```bash
   make lint-report
   ```

## System Header Errors

The lint report **automatically filters out system header errors** (from LLVM, libc++, etc.). This keeps the report focused on actionable issues in your codebase.

If you see issues like:
- `'mutex' file not found`
- `FP_NAN undeclared`
- Symbol conflicts in system headers

These are **not** included in the report and don't require action.

## Common Check Types

| Check | Category | Description |
|-------|----------|-------------|
| `bugprone-use-after-move` | Correctness | Using a variable after std::move |
| `bugprone-unused-return-value` | Correctness | Ignoring important return values |
| `modernize-use-nullptr` | Modernization | Use `nullptr` instead of `NULL` |
| `modernize-use-override` | Modernization | Mark virtual functions with `override` |
| `performance-unnecessary-copy-initialization` | Performance | Unnecessary copies that can be optimized |
| `readability-redundant-declaration` | Readability | Redundant declarations |

## Running Lint Without Reports

To run lint and see real-time filtered output:

```bash
make lint
```

Only real issues (not system header errors) will be displayed.

## Disabling Checks (If Needed)

To disable a specific check temporarily:

Edit `.clang-tidy` and comment out the check in the `Checks:` list:

```yaml
Checks: >
  bugprone-assert-side-effect,
  # bugprone-bool-pointer-implicit-conversion,  # Disabled for now
```

Once all issues are fixed, set `WarningsAsErrors: '*'` in `.clang-tidy` to make all warnings hard failures in CI.

## Integration with CI/CD

The linting report is automatically generated in CI and uploaded as an artifact. Download it from GitHub Actions to track progress over time.
