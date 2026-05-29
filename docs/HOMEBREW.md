# Homebrew Installation Guide

This document explains how to install `mar` via Homebrew and how the package is maintained.

## Installation

### Option 1: From the Official Repository (when tap is available)

Once the Homebrew tap is set up, users can install with:

```bash
brew tap earthframe/tap
brew install mar
```

Or as a one-liner:

```bash
brew install earthframe/tap/mar
```

### Option 2: From Local Repository

To install directly from this repository:

```bash
brew install --build-from-source ./.homebrew/mar.rb
```

### Option 3: From GitHub Release

If you prefer to build from a specific release tag:

```bash
git clone https://github.com/earthframe/mar.git
cd mar
git checkout v0.1.2  # Replace with desired version
brew install --build-from-source ./.homebrew/mar.rb
```

## How the Homebrew Package is Maintained

### Formula Location

The Homebrew formula is stored in `.homebrew/mar.rb` in this repository.

### Automatic Updates on Release

When you create a release by pushing a tag:

1. **GitHub Actions** builds and tests the code
2. **Automatically** downloads the source tarball from GitHub Releases
3. **Computes** the SHA256 hash of the tarball (from public sources only)
4. **Updates** `.homebrew/mar.rb` with:
   - The correct GitHub release URL
   - The SHA256 hash for package integrity verification
5. **Commits** the updated formula back to the repository

This ensures the formula is always in sync and users get the correct version.

### Formula Content

The formula includes:

- **Description**: Brief explanation of what `mar` does
- **Homepage**: Link to the project repository
- **Source URL**: Direct link to the GitHub release tarball
- **SHA256**: Hash for verifying package integrity
- **License**: MIT
- **Build Dependencies**: `pkg-config` (only needed during build)
- **Runtime Dependencies**:
  - `libzstd`: Zstandard compression
  - `lz4`: LZ4 compression
  - `libdeflate`: DEFLATE compression
  - `bzip2`: BZIP2 compression
  - `macfuse`: FUSE support (optional, required for `mar mount`)
- **Build Instructions**: `make release`
- **Smoke Test**: Verifies installation by running `mar --version`

## Setting Up a Official Homebrew Tap

If you want to make `mar` available through Homebrew's main registry, you have two options:

### Option 1: Submit to Homebrew Core (Official Channel)

For widely-used projects, submit to the official Homebrew repository:

1. Fork [Homebrew/homebrew-core](https://github.com/Homebrew/homebrew-core)
2. Add your formula to `Formula/mar.rb`
3. Submit a pull request with:
   - Clear description of the project
   - Reason why it should be in core
   - Verification that it builds on macOS

See: [Homebrew Contributing Guide](https://docs.brew.sh/Formula-Cookbook)

### Option 2: Create a Custom Tap (Easier for Smaller Projects)

Create a separate repository for your tap:

```bash
# Create a new repository named homebrew-tap
git clone https://github.com/earthframe/homebrew-tap.git
cd homebrew-tap
mkdir -p Formula
cp ../mar/.homebrew/mar.rb Formula/
git add Formula/mar.rb
git commit -m "Add mar formula"
git push
```

Users can then install with:

```bash
brew tap earthframe/tap
brew install mar
```

## Security Notes

### SHA256 Verification

The SHA256 hash in the formula ensures package integrity:

- Computed from the **public** GitHub release tarball
- Users' `brew` command verifies the downloaded file matches this hash
- Prevents tampering or man-in-the-middle attacks

### No Secrets in Formula

The formula contains:
- ✅ Public URLs and hashes
- ✅ Package metadata (version, license, description)
- ❌ NO credentials, API keys, or private information

Any credentials needed for building are requested through standard `brew` prompts.

## Troubleshooting

### Formula won't install

1. Check that the SHA256 hash is correct:
   ```bash
   curl -sL https://github.com/earthframe/mar/archive/refs/tags/v0.1.2.tar.gz | sha256sum
   ```

2. Verify dependencies are available:
   ```bash
   brew install libzstd lz4 libdeflate bzip2
   ```

3. Try building manually to see detailed errors:
   ```bash
   brew install -vvv --build-from-source ./.homebrew/mar.rb
   ```

### Build fails on macOS

Ensure you have Xcode command line tools installed:

```bash
xcode-select --install
```

### Testing locally

Before committing to a tap, test the formula locally:

```bash
brew install --build-from-source ./.homebrew/mar.rb
brew test mar
brew uninstall mar
```
