# CI/CD and Release Process

This document describes the automated build and release process for the `mar` project using GitHub Actions.

## Overview

The CI/CD pipeline is designed to ensure code quality through automated testing and to provide a streamlined, cost-effective way to distribute binaries.

### Workflow Stages

1.  **Continuous Integration (CI)**: Triggered on every Pull Request and push to `main`.
    *   Builds and tests on **Linux** (Ubuntu 22.04, 24.04) and **macOS** (latest).
    *   Runs unit tests (`make test`) and CLI integration tests (`tests/integration_test.sh`).
    *   Ensures that no broken code is merged into the `main` branch.
    *   Generates a linting report (categorized by check type and file).

2.  **Static Distribution Build (Linux only)**: Triggered on every push to `main` (including merges).
    *   Builds portable static musl binaries for x86_64 and ARM64 using Alpine Linux.
    *   Uploads these binaries as GitHub Actions Artifacts.
    *   **Retention Policy**: Artifacts from `main` are kept for **1 day** to minimize storage costs while ensuring the latest build is always available.

3.  **Automated Release**: Triggered when a git tag starting with `v` (e.g., `v1.0.0`) is pushed.
    *   Downloads the static Linux binaries produced in the build stage.
    *   Creates a formal GitHub Release and attaches the binaries.
    *   Automatically generates release notes based on commit history.
    *   **Automatically updates the Homebrew formula** with the correct SHA256 hash and commits it back.

## How to Make a Release

### Step 1: Prepare Your Code

Ensure all desired changes are merged into the `main` branch and all CI checks pass:

```bash
git checkout main
git pull origin main
# Verify that all CI checks have passed on GitHub
```

### Step 2: Create a Version Tag

Create a semantic version tag for your release:

```bash
git tag -a v0.1.2 -m "Release version 0.1.2

This release includes:
- Bug fix for issue #123
- Performance improvements for large archives
- Better documentation"
```

### Step 3: Push the Tag to Trigger Release

```bash
git push origin v0.1.2
```

This will:
1. Trigger the CI/CD pipeline to build and test
2. Create a GitHub Release with Linux static binaries attached
3. **Automatically update `Formula/mar.rb`** with the latest SHA256 and commit it

### Step 4: Verify the Release

Visit the [Releases page](https://github.com/earthframe/mar/releases) to confirm:
- The release was created
- Static binaries are attached
- Release notes were generated correctly

## Distribution Methods

### Linux

Users can download static musl binaries directly from GitHub Releases:

```bash
# Download and extract (example for x86_64)
curl -L https://github.com/earthframe/mar/releases/download/v0.1.2/mar-linux-x86_64-musl -o mar
chmod +x mar
./mar --version
```

Or use the static binary in a Docker container for maximum portability.

### macOS

macOS users install via Homebrew. The formula is automatically maintained in `Formula/mar.rb`:

```bash
brew install earthframe/mar  # Once the tap is registered
# Or clone and install locally:
brew install --build-from-source ./Formula/mar.rb
```

**How the Homebrew Formula is Maintained:**

When you create a release:
1. GitHub Actions downloads the source tarball for the tagged version
2. Computes the SHA256 hash of the tarball
3. Updates `Formula/mar.rb` with the new URL and hash
4. Commits the updated formula back to the repository

This ensures the formula is always in sync with the latest release and users always get the correct version.

## Security Considerations

### Secrets Management

The CI/CD workflow does **not** store or leak any secrets:

- **GitHub Token**: Used only in Actions for git operations. GitHub Actions securely injects tokens that are scoped to the current workflow run only.
- **Formula Updates**: The SHA256 hash is computed from the **public** source tarball on GitHub. No credentials or secrets are embedded in the formula.
- **Git Commits**: The automatic formula update uses a temporary token that expires after the workflow completes.

### Best Practices

- **Never commit** `.env`, `credentials.json`, or other secret files
- **Always use** GitHub Actions secrets for any sensitive configuration
- **Review** pull requests before merging to avoid introducing secrets
- **Never push** credentials or tokens to the repository

## Performance Regression Testing

The CI/CD pipeline includes a performance regression check that compares the current build's performance against hard baselines for both Linux and macOS.

*   **Baselines**: Stored in `benchmarks/baselines/`.
*   **Check Script**: `scripts/perf_check.sh`.
*   **Behavior**: If a regression is detected (default threshold is 10%), a warning is printed in the CI logs, but the build continues. This allows for flexibility across different runner environments.

To update the baselines, simply edit the files in `benchmarks/baselines/` with the new target values in milliseconds.

## Emergency Management

*   **Replacing an Artifact**: If a binary needs to be replaced, manually delete the asset from the GitHub Release page and upload a new one.
*   **Deleting a Release**: If a release was made in error, delete it from the GitHub UI and create a new one with the correct tag.
*   **Fixing a Bad Formula**: If the Homebrew formula needs to be corrected, manually edit `Formula/mar.rb` and push the change. The formula will be updated on the next release.

## Manual Triggers

The workflow can also be triggered manually via the **Actions** tab on GitHub using the `workflow_dispatch` event. This is useful for testing the build process without creating a new tag or commit.

## Linting Report

A detailed linting report is generated on every commit:

- **Markdown Report**: Categorized by check type and file for easy review
- **JSON Report**: Machine-readable format for automated tooling
- **Available as Artifact**: Download from GitHub Actions for offline analysis

Use this to prioritize which linting issues to fix based on frequency and severity.
