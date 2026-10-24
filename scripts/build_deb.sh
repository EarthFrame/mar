#!/usr/bin/env bash
# scripts/build_deb.sh - Build production-ready Debian package (.deb) for MAR
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(dirname "$SCRIPT_DIR")"

# Usage: scripts/build_deb.sh <binary_path> [arch] [version] [output_dir]
# Examples:
#   scripts/build_deb.sh ./mar amd64 0.3.0 dist
#   scripts/build_deb.sh dist/mar-linux-arm64 arm64 0.3.0 dist

BINARY_PATH="${1:-}"
if [ -z "$BINARY_PATH" ] || [ ! -f "$BINARY_PATH" ]; then
    echo "Error: Binary path must be provided and exist as a regular file."
    echo "Usage: $0 <binary_path> [arch] [version] [output_dir]"
    exit 1
fi

HOST_DPKG_ARCH=$(dpkg --print-architecture 2>/dev/null || echo "amd64")
TARGET_ARCH="${2:-$HOST_DPKG_ARCH}"

VERSION="${3:-}"
if [ -z "$VERSION" ]; then
    if [ -f "$REPO_ROOT/VERSION" ]; then
        VERSION="$(cat "$REPO_ROOT/VERSION" | tr -d '[:space:]')"
    else
        VERSION="0.3.0"
    fi
fi
VERSION="${VERSION#v}"
PKG_VERSION="${VERSION}-1"

OUTPUT_DIR="${4:-$REPO_ROOT/dist}"
mkdir -p "$OUTPUT_DIR"

STAGE_DIR=$(mktemp -d "/tmp/mar_deb_${TARGET_ARCH}_XXXXXX")
chmod 755 "$STAGE_DIR"
cleanup() {
    rm -rf "$STAGE_DIR"
}
trap cleanup EXIT

echo "=== Building Debian package for MAR v${PKG_VERSION} (${TARGET_ARCH}) ==="
echo "  Source binary: $BINARY_PATH"
echo "  Output target: $OUTPUT_DIR"

# 1. Structure directories
BIN_DEST="$STAGE_DIR/usr/bin"
DOC_DEST="$STAGE_DIR/usr/share/doc/mar"
DEBIAN_DEST="$STAGE_DIR/DEBIAN"

mkdir -p "$BIN_DEST" "$DOC_DEST" "$DEBIAN_DEST"

# 2. Copy and strip binary
cp "$BINARY_PATH" "$BIN_DEST/mar"
chmod 755 "$BIN_DEST/mar"

# Strip symbols if possible
STRIP_TOOL="strip"
if [ "$TARGET_ARCH" = "arm64" ] && [ "$HOST_DPKG_ARCH" != "arm64" ]; then
    if command -v aarch64-linux-gnu-strip >/dev/null 2>&1; then
        STRIP_TOOL="aarch64-linux-gnu-strip"
    elif command -v llvm-strip >/dev/null 2>&1; then
        STRIP_TOOL="llvm-strip"
    else
        STRIP_TOOL=""
    fi
fi

if [ -n "$STRIP_TOOL" ] && command -v "$STRIP_TOOL" >/dev/null 2>&1; then
    echo "  Stripping binary with $STRIP_TOOL..."
    "$STRIP_TOOL" --strip-unneeded "$BIN_DEST/mar" 2>/dev/null || true
else
    echo "  Notice: Skipping strip (no compatible cross-strip found for $TARGET_ARCH)"
fi

# 3. Documentation & Copyright
if [ -f "$REPO_ROOT/debian/copyright" ]; then
    cp "$REPO_ROOT/debian/copyright" "$DOC_DEST/copyright"
elif [ -f "$REPO_ROOT/LICENSE.md" ]; then
    cp "$REPO_ROOT/LICENSE.md" "$DOC_DEST/copyright"
fi
chmod 644 "$DOC_DEST/copyright"

if [ -f "$REPO_ROOT/debian/changelog" ]; then
    gzip -9cn "$REPO_ROOT/debian/changelog" > "$DOC_DEST/changelog.Debian.gz"
    chmod 644 "$DOC_DEST/changelog.Debian.gz"
fi

# Man page (Debian Policy §12.1)
if [ -f "$REPO_ROOT/man/mar.1" ]; then
    MAN_DEST="$STAGE_DIR/usr/share/man/man1"
    mkdir -p "$MAN_DEST"
    gzip -9cn "$REPO_ROOT/man/mar.1" > "$MAN_DEST/mar.1.gz"
    chmod 644 "$MAN_DEST/mar.1.gz"
fi

# Shell completions
if [ -f "$REPO_ROOT/completions/bash/mar" ]; then
    BASH_DEST="$STAGE_DIR/usr/share/bash-completion/completions"
    mkdir -p "$BASH_DEST"
    cp "$REPO_ROOT/completions/bash/mar" "$BASH_DEST/mar"
    chmod 644 "$BASH_DEST/mar"
fi

if [ -f "$REPO_ROOT/completions/zsh/_mar" ]; then
    ZSH_DEST="$STAGE_DIR/usr/share/zsh/site-functions"
    mkdir -p "$ZSH_DEST"
    cp "$REPO_ROOT/completions/zsh/_mar" "$ZSH_DEST/_mar"
    chmod 644 "$ZSH_DEST/_mar"
fi

# 4. Installed-Size calculation (KiB)
INSTALLED_SIZE=$(du -sk "$STAGE_DIR/usr" | cut -f1)

# 5. Generate DEBIAN/control
cat <<EOF > "$DEBIAN_DEST/control"
Package: mar
Version: ${PKG_VERSION}
Section: utils
Priority: optional
Architecture: ${TARGET_ARCH}
Installed-Size: ${INSTALLED_SIZE}
Depends: libc6, libgcc-s1
Maintainer: EarthFrame Corporation <support@earthframe.com>
Homepage: https://github.com/earthframe/mar
Description: Universal Data Container and high-performance archival utility
 MAR is a high-performance archive format designed for efficient storage
 and retrieval of large datasets, with support for various compression
 algorithms and indexing.
EOF
chmod 644 "$DEBIAN_DEST/control"

# 6. Generate DEBIAN/md5sums
(
    cd "$STAGE_DIR"
    find usr -type f -exec md5sum {} + > "$DEBIAN_DEST/md5sums"
)
chmod 644 "$DEBIAN_DEST/md5sums"

# 7. Package build with root owner/group normalization
DEB_FILE="$OUTPUT_DIR/mar_${PKG_VERSION}_${TARGET_ARCH}.deb"
dpkg-deb --root-owner-group --build "$STAGE_DIR" "$DEB_FILE"

echo "  Successfully built: $DEB_FILE"
dpkg -I "$DEB_FILE"
echo "=== Done ==="
