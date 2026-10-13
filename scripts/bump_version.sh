#!/usr/bin/env bash
# scripts/bump_version.sh - Updates MAR tool version across all implementations and configurations
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(dirname "$SCRIPT_DIR")"

if [ $# -ne 1 ]; then
    echo "Usage: $0 <new-version>"
    echo "Example: $0 0.3.0"
    exit 1
fi

NEW_VERSION="$1"

# Validate semver pattern (X.Y.Z)
if ! [[ "$NEW_VERSION" =~ ^[0-9]+\.[0-9]+\.[0-9]+$ ]]; then
    echo "Error: Version '$NEW_VERSION' does not conform to semantic versioning (X.Y.Z)"
    exit 1
fi

IFS='.' read -r MAJOR MINOR PATCH <<< "$NEW_VERSION"

echo "=== Bumping MAR Tool Version to $NEW_VERSION (Major=$MAJOR, Minor=$MINOR, Patch=$PATCH) ==="

# 1. Root VERSION file
echo "$NEW_VERSION" > "$REPO_ROOT/VERSION"
echo "✓ Updated VERSION"

# 2. C++ Constants (cpp/include/mar/constants.hpp)
sed -i.bak \
    -e "s/constexpr u8 TOOL_VERSION_MAJOR = [0-9]*;/constexpr u8 TOOL_VERSION_MAJOR = $MAJOR;/" \
    -e "s/constexpr u8 TOOL_VERSION_MINOR = [0-9]*;/constexpr u8 TOOL_VERSION_MINOR = $MINOR;/" \
    -e "s/constexpr u8 TOOL_VERSION_PATCH = [0-9]*;/constexpr u8 TOOL_VERSION_PATCH = $PATCH;/" \
    "$REPO_ROOT/cpp/include/mar/constants.hpp"
rm -f "$REPO_ROOT/cpp/include/mar/constants.hpp.bak"
echo "✓ Updated cpp/include/mar/constants.hpp"

# 3. Rust Format Constants (rust/src/format.rs)
sed -i.bak \
    -e "s/pub const TOOL_VERSION_MAJOR: u8 = [0-9]*;/pub const TOOL_VERSION_MAJOR: u8 = $MAJOR;/" \
    -e "s/pub const TOOL_VERSION_MINOR: u8 = [0-9]*;/pub const TOOL_VERSION_MINOR: u8 = $MINOR;/" \
    -e "s/pub const TOOL_VERSION_PATCH: u8 = [0-9]*;/pub const TOOL_VERSION_PATCH: u8 = $PATCH;/" \
    "$REPO_ROOT/rust/src/format.rs"
rm -f "$REPO_ROOT/rust/src/format.rs.bak"
echo "✓ Updated rust/src/format.rs"

# 4. Rust CLI Version (rust/src/bin/main.rs and pymar/src/bin/main.rs)
sed -i.bak "s/const TOOL_VERSION: &str = \"[0-9]*\.[0-9]*\.[0-9]*\";/const TOOL_VERSION: &str = \"$NEW_VERSION\";/" "$REPO_ROOT/rust/src/bin/main.rs"
rm -f "$REPO_ROOT/rust/src/bin/main.rs.bak"
if [ -f "$REPO_ROOT/pymar/src/bin/main.rs" ]; then
    sed -i.bak "s/const TOOL_VERSION: &str = \"[0-9]*\.[0-9]*\.[0-9]*\";/const TOOL_VERSION: &str = \"$NEW_VERSION\";/" "$REPO_ROOT/pymar/src/bin/main.rs"
    rm -f "$REPO_ROOT/pymar/src/bin/main.rs.bak"
fi
echo "✓ Updated Rust main.rs binaries"

# 5. Cargo crates (rust/Cargo.toml, python/Cargo.toml, pymar/Cargo.toml)
sed -i.bak "s/^version = \"[0-9]*\.[0-9]*\.[0-9]*\"/version = \"$NEW_VERSION\"/" "$REPO_ROOT/rust/Cargo.toml"
rm -f "$REPO_ROOT/rust/Cargo.toml.bak"
sed -i.bak "s/^version = \"[0-9]*\.[0-9]*\.[0-9]*\"/version = \"$NEW_VERSION\"/" "$REPO_ROOT/python/Cargo.toml"
rm -f "$REPO_ROOT/python/Cargo.toml.bak"
if [ -f "$REPO_ROOT/pymar/Cargo.toml" ]; then
    sed -i.bak "s/^version = \"[0-9]*\.[0-9]*\.[0-9]*\"/version = \"$NEW_VERSION\"/" "$REPO_ROOT/pymar/Cargo.toml"
    rm -f "$REPO_ROOT/pymar/Cargo.toml.bak"
fi
echo "✓ Updated Cargo.toml files"

# 6. Python packaging (python/pyproject.toml, pymar/pyproject.toml)
sed -i.bak "s/^version = \"[0-9]*\.[0-9]*\.[0-9]*\"/version = \"$NEW_VERSION\"/" "$REPO_ROOT/python/pyproject.toml"
rm -f "$REPO_ROOT/python/pyproject.toml.bak"
if [ -f "$REPO_ROOT/pymar/pyproject.toml" ]; then
    sed -i.bak "s/^version = \"[0-9]*\.[0-9]*\.[0-9]*\"/version = \"$NEW_VERSION\"/" "$REPO_ROOT/pymar/pyproject.toml"
    rm -f "$REPO_ROOT/pymar/pyproject.toml.bak"
fi
echo "✓ Updated pyproject.toml files"

# 7. Dockerfile (docker/Dockerfile)
sed -i.bak "s/^ARG VERSION=[0-9]*\.[0-9]*\.[0-9]*/ARG VERSION=$NEW_VERSION/" "$REPO_ROOT/docker/Dockerfile"
rm -f "$REPO_ROOT/docker/Dockerfile.bak"
echo "✓ Updated docker/Dockerfile"

# 8. Tests and Scripts
sed -i.bak "s/# Version: [0-9]*\.[0-9]*\.[0-9]*/# Version: $NEW_VERSION/" "$REPO_ROOT/tests/integration_test.sh"
sed -i.bak "s/MAR CLI Integration Tests v[0-9]*\.[0-9]*\.[0-9]*/MAR CLI Integration Tests v$NEW_VERSION/" "$REPO_ROOT/tests/integration_test.sh"
rm -f "$REPO_ROOT/tests/integration_test.sh.bak"
echo "✓ Updated tests/integration_test.sh"

# 9. Homebrew formula (.homebrew/mar.rb)
sed -i.bak "s|archive/refs/tags/v[0-9]*\.[0-9]*\.[0-9]*\.tar\.gz|archive/refs/tags/v$NEW_VERSION.tar.gz|" "$REPO_ROOT/.homebrew/mar.rb"
rm -f "$REPO_ROOT/.homebrew/mar.rb.bak"
echo "✓ Updated .homebrew/mar.rb"

echo ""
echo "Version successfully bumped to $NEW_VERSION across all targets!"
echo "Next steps:"
echo "  1. Update debian/changelog (if building Debian packages)."
echo "  2. Run 'make test && make test-cpp' to verify builds and test suites."
